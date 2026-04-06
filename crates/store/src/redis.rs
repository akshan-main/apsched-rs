use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, Script};

use apsched_core::error::StoreError;
use apsched_core::model::{JobLease, ScheduleSpec};
use apsched_core::traits::JobStore;

/// Default lease duration for acquired jobs (30 seconds).
const DEFAULT_LEASE_DURATION_SECS: i64 = 30;

/// Redis-backed job store using sorted sets for efficient due-job retrieval.
///
/// Data layout:
/// - `{prefix}jobs:{id}` -- Hash containing serialized job state and metadata
/// - `{prefix}next_run_times` -- Sorted set: score = unix timestamp, member = job_id
/// - `{prefix}running_counts:{id}` -- String counter for running instance count
/// - `{prefix}leases:{id}` -- Hash with acquired_by, acquired_at, lease_expires_at
///
/// Distributed locking is achieved via a Lua script that atomically checks
/// the job version and lease status before acquiring.
#[derive(Clone)]
pub struct RedisJobStore {
    #[allow(dead_code)] // Retained for future use (e.g. pub_sub, new connections)
    client: Client,
    connection: ConnectionManager,
    prefix: String,
}

impl std::fmt::Debug for RedisJobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisJobStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl RedisJobStore {
    /// Create a new Redis job store.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL (e.g. `redis://localhost:6379/0`)
    /// * `prefix` - Optional key prefix; defaults to `"apscheduler:"`
    pub async fn new(url: &str, prefix: Option<&str>) -> Result<Self, StoreError> {
        let client = Client::open(url).map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        let connection = ConnectionManager::new(client.clone())
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            client,
            connection,
            prefix: prefix.unwrap_or("apscheduler:").to_string(),
        })
    }

    /// Build a Redis key for a job hash.
    fn job_key(&self, job_id: &str) -> String {
        format!("{}jobs:{}", self.prefix, job_id)
    }

    /// Build the Redis key for the next_run_times sorted set.
    fn nrt_key(&self) -> String {
        format!("{}next_run_times", self.prefix)
    }

    /// Build a Redis key for a running count.
    fn running_count_key(&self, job_id: &str) -> String {
        format!("{}running_counts:{}", self.prefix, job_id)
    }

    /// Build a Redis key for a lease hash.
    fn lease_key(&self, job_id: &str) -> String {
        format!("{}leases:{}", self.prefix, job_id)
    }

    /// Build a Redis key for the job index set (tracks all job IDs).
    fn index_key(&self) -> String {
        format!("{}job_index", self.prefix)
    }

    /// Serialize a ScheduleSpec to JSON string for storage.
    fn serialize_spec(spec: &ScheduleSpec) -> Result<String, StoreError> {
        serde_json::to_string(spec).map_err(|e| StoreError::SerializationFailed(e.to_string()))
    }

    /// Deserialize a ScheduleSpec from a JSON string.
    fn deserialize_spec(json: &str) -> Result<ScheduleSpec, StoreError> {
        serde_json::from_str(json).map_err(|e| StoreError::DeserializationFailed(e.to_string()))
    }

    /// Convert a DateTime to an ISO 8601 string.
    fn dt_to_string(dt: &DateTime<Utc>) -> String {
        dt.to_rfc3339()
    }

    /// Parse an ISO 8601 string back to DateTime<Utc>.
    fn string_to_dt(s: &str) -> Result<DateTime<Utc>, StoreError> {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                StoreError::DeserializationFailed(format!("invalid datetime '{}': {}", s, e))
            })
    }

    /// Convert DateTime to a Unix timestamp as f64 (for sorted set scores).
    fn dt_to_score(dt: &DateTime<Utc>) -> f64 {
        dt.timestamp() as f64 + (dt.timestamp_subsec_millis() as f64 / 1000.0)
    }

    /// Read a job from Redis and reconstruct the ScheduleSpec.
    async fn read_job(&self, job_id: &str) -> Result<Option<ScheduleSpec>, StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);

        let data: redis::RedisResult<std::collections::HashMap<String, String>> =
            conn.hgetall(&job_key).await;

        match data {
            Ok(map) if map.is_empty() => Ok(None),
            Ok(map) => {
                let job_state = map
                    .get("job_state")
                    .ok_or_else(|| StoreError::CorruptedData {
                        job_id: job_id.to_string(),
                        detail: "missing job_state field".to_string(),
                    })?;
                let mut spec = Self::deserialize_spec(job_state)?;

                // Overlay authoritative fields
                if let Some(nrt_str) = map.get("next_run_time") {
                    if !nrt_str.is_empty() {
                        spec.next_run_time = Some(Self::string_to_dt(nrt_str)?);
                    } else {
                        spec.next_run_time = None;
                    }
                }

                if let Some(paused_str) = map.get("paused") {
                    spec.paused = paused_str == "true" || paused_str == "1";
                }

                if let Some(version_str) = map.get("version") {
                    spec.version = version_str.parse::<u64>().unwrap_or(0);
                }

                if let Some(executor) = map.get("executor") {
                    spec.executor = executor.clone();
                }

                Ok(Some(spec))
            }
            Err(e) => Err(StoreError::QueryFailed(e.to_string())),
        }
    }

    /// Write a job's data to Redis (HSET + ZADD).
    async fn write_job(&self, spec: &ScheduleSpec, version: u64) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(&spec.id);
        let nrt_key = self.nrt_key();
        let index_key = self.index_key();
        let job_state = Self::serialize_spec(spec)?;
        let now_str = Self::dt_to_string(&Utc::now());

        let nrt_str = spec
            .next_run_time
            .as_ref()
            .map(Self::dt_to_string)
            .unwrap_or_default();

        let paused_str = if spec.paused { "true" } else { "false" };

        let trigger_type = match &spec.trigger_state {
            apsched_core::model::TriggerState::Date { .. } => "date",
            apsched_core::model::TriggerState::Interval { .. } => "interval",
            apsched_core::model::TriggerState::Cron { .. } => "cron",
            apsched_core::model::TriggerState::CalendarInterval { .. } => "calendar_interval",
            apsched_core::model::TriggerState::Plugin { .. } => "plugin",
        };

        // Use a pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic();

        pipe.cmd("HSET")
            .arg(&job_key)
            .arg("id")
            .arg(&spec.id)
            .arg("job_state")
            .arg(&job_state)
            .arg("trigger_type")
            .arg(trigger_type)
            .arg("executor")
            .arg(&spec.executor)
            .arg("paused")
            .arg(paused_str)
            .arg("version")
            .arg(version.to_string())
            .arg("next_run_time")
            .arg(&nrt_str)
            .arg("updated_at")
            .arg(&now_str)
            .ignore();

        // Add to job index
        pipe.cmd("SADD").arg(&index_key).arg(&spec.id).ignore();

        // Update sorted set for next_run_time
        if let Some(nrt) = &spec.next_run_time {
            if !spec.paused {
                let score = Self::dt_to_score(nrt);
                pipe.cmd("ZADD")
                    .arg(&nrt_key)
                    .arg(score)
                    .arg(&spec.id)
                    .ignore();
            } else {
                // Paused jobs should not appear in the sorted set
                pipe.cmd("ZREM").arg(&nrt_key).arg(&spec.id).ignore();
            }
        } else {
            // No next_run_time means remove from sorted set
            pipe.cmd("ZREM").arg(&nrt_key).arg(&spec.id).ignore();
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    /// Lua script for atomic job acquisition.
    /// Returns 1 if acquired, 0 if not.
    fn acquire_lua_script() -> Script {
        Script::new(
            r#"
            -- KEYS[1] = job hash key
            -- KEYS[2] = lease hash key
            -- ARGV[1] = scheduler_id
            -- ARGV[2] = current_time (ISO string)
            -- ARGV[3] = lease_expires_at (ISO string)
            -- ARGV[4] = expected_version (string)
            local version = redis.call('HGET', KEYS[1], 'version')
            if version == ARGV[4] then
                local acquired = redis.call('HGET', KEYS[2], 'acquired_by')
                local expires = redis.call('HGET', KEYS[2], 'lease_expires_at')
                if acquired == false or expires == false or expires < ARGV[2] then
                    redis.call('HSET', KEYS[2],
                        'acquired_by', ARGV[1],
                        'acquired_at', ARGV[2],
                        'lease_expires_at', ARGV[3])
                    redis.call('HINCRBY', KEYS[1], 'version', 1)
                    return 1
                end
            end
            return 0
            "#,
        )
    }
}

#[async_trait]
impl JobStore for RedisJobStore {
    async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(&schedule.id);

        // Check if job already exists
        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if exists {
            return Err(StoreError::DuplicateJob {
                job_id: schedule.id.clone(),
            });
        }

        // Write a created_at field as well
        let now_str = Self::dt_to_string(&Utc::now());
        {
            let _: () = conn
                .hset(&job_key, "created_at", &now_str)
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        }

        self.write_job(&schedule, schedule.version).await?;

        // Initialize running count
        let rc_key = self.running_count_key(&schedule.id);
        let _: () = conn
            .set(&rc_key, 0i32)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(&schedule.id);

        // Check existence
        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: schedule.id.clone(),
            });
        }

        // Increment version
        let version: u64 = conn
            .hincr(&job_key, "version", 1i64)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        self.write_job(&schedule, version).await?;
        Ok(())
    }

    async fn remove_job(&self, job_id: &str) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);

        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        let nrt_key = self.nrt_key();
        let rc_key = self.running_count_key(job_id);
        let lease_key = self.lease_key(job_id);
        let index_key = self.index_key();

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.del(&job_key).ignore();
        pipe.cmd("ZREM").arg(&nrt_key).arg(job_id).ignore();
        pipe.del(&rc_key).ignore();
        pipe.del(&lease_key).ignore();
        pipe.cmd("SREM").arg(&index_key).arg(job_id).ignore();

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    async fn remove_all_jobs(&self) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let index_key = self.index_key();

        // Get all job IDs from the index
        let job_ids: Vec<String> = conn
            .smembers(&index_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if job_ids.is_empty() {
            return Ok(());
        }

        let nrt_key = self.nrt_key();

        let mut pipe = redis::pipe();
        pipe.atomic();

        for job_id in &job_ids {
            pipe.del(self.job_key(job_id)).ignore();
            pipe.del(self.running_count_key(job_id)).ignore();
            pipe.del(self.lease_key(job_id)).ignore();
        }

        pipe.del(&nrt_key).ignore();
        pipe.del(&index_key).ignore();

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError> {
        match self.read_job(job_id).await? {
            Some(spec) => Ok(spec),
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
        let mut conn = self.connection.clone();
        let index_key = self.index_key();

        let job_ids: Vec<String> = conn
            .smembers(&index_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut jobs = Vec::with_capacity(job_ids.len());
        for job_id in &job_ids {
            if let Some(spec) = self.read_job(job_id).await? {
                jobs.push(spec);
            }
        }

        // Sort by next_run_time
        jobs.sort_by(|a, b| a.next_run_time.cmp(&b.next_run_time));
        Ok(jobs)
    }

    async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
        let mut conn = self.connection.clone();
        let nrt_key = self.nrt_key();
        let score = Self::dt_to_score(&now);

        // Get all job IDs with next_run_time <= now
        let job_ids: Vec<String> = conn
            .zrangebyscore(&nrt_key, f64::NEG_INFINITY, score)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut due_jobs = Vec::new();
        let now_str = Self::dt_to_string(&now);

        for job_id in &job_ids {
            if let Some(spec) = self.read_job(job_id).await? {
                // Skip paused jobs (shouldn't be in sorted set, but double-check)
                if spec.paused {
                    continue;
                }

                // Check lease: skip if acquired and lease not expired
                let lease_key = self.lease_key(job_id);
                let acquired_by: Option<String> = conn
                    .hget(&lease_key, "acquired_by")
                    .await
                    .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

                if let Some(_) = acquired_by {
                    let expires: Option<String> =
                        conn.hget(&lease_key, "lease_expires_at")
                            .await
                            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

                    if let Some(exp_str) = expires {
                        if exp_str >= now_str {
                            continue; // lease still valid, skip
                        }
                    }
                }

                due_jobs.push(spec);
            }
        }

        due_jobs.sort_by(|a, b| a.next_run_time.cmp(&b.next_run_time));
        Ok(due_jobs)
    }

    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        let mut conn = self.connection.clone();
        let nrt_key = self.nrt_key();

        // Get the first element (lowest score) from the sorted set
        let result: Vec<(String, f64)> = conn
            .zrangebyscore_limit_withscores(&nrt_key, f64::NEG_INFINITY, f64::INFINITY, 0, 1)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if let Some((_job_id, _score)) = result.first() {
            // Read the actual next_run_time from the job hash for precision
            if let Some(spec) = self.read_job(&_job_id).await? {
                return Ok(spec.next_run_time);
            }
        }

        Ok(None)
    }

    async fn acquire_jobs(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        let mut conn = self.connection.clone();
        let nrt_key = self.nrt_key();
        let now_str = Self::dt_to_string(&now);
        let lease_expires = now + chrono::Duration::seconds(DEFAULT_LEASE_DURATION_SECS);
        let lease_expires_str = Self::dt_to_string(&lease_expires);
        let score = Self::dt_to_score(&now);

        // Get candidate job IDs
        let job_ids: Vec<String> = conn
            .zrangebyscore(&nrt_key, f64::NEG_INFINITY, score)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut leases = Vec::new();
        let script = Self::acquire_lua_script();

        for job_id in &job_ids {
            if leases.len() >= max_jobs {
                break;
            }

            // Read current version
            let version: Option<String> = conn
                .hget(self.job_key(job_id), "version")
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

            let version_str = version.unwrap_or_else(|| "0".to_string());

            let job_key = self.job_key(job_id);
            let lease_key = self.lease_key(job_id);

            let result: i32 = script
                .key(&job_key)
                .key(&lease_key)
                .arg(scheduler_id)
                .arg(&now_str)
                .arg(&lease_expires_str)
                .arg(&version_str)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

            if result == 1 {
                let new_version: u64 = version_str.parse::<u64>().unwrap_or(0) + 1;
                leases.push(JobLease {
                    job_id: job_id.clone(),
                    scheduler_id: scheduler_id.to_string(),
                    acquired_at: now,
                    expires_at: lease_expires,
                    version: new_version,
                });
            }
        }

        Ok(leases)
    }

    async fn release_job(&self, job_id: &str, scheduler_id: &str) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);
        let lease_key = self.lease_key(job_id);

        // Check job exists
        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        // Check that the caller owns the lease
        let acquired_by: Option<String> = conn
            .hget(&lease_key, "acquired_by")
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        match acquired_by {
            Some(ref owner) if owner == scheduler_id => {
                // Delete the lease
                let _: () = conn
                    .del(&lease_key)
                    .await
                    .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
                Ok(())
            }
            _ => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn update_next_run_time(
        &self,
        job_id: &str,
        next: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);
        let nrt_key = self.nrt_key();

        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        let now_str = Self::dt_to_string(&Utc::now());
        let nrt_str = next.as_ref().map(Self::dt_to_string).unwrap_or_default();

        let mut pipe = redis::pipe();
        pipe.atomic();

        pipe.cmd("HSET")
            .arg(&job_key)
            .arg("next_run_time")
            .arg(&nrt_str)
            .arg("updated_at")
            .arg(&now_str)
            .ignore();

        if let Some(nrt) = &next {
            // Check if paused
            let paused: Option<String> = conn
                .hget(&job_key, "paused")
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

            let is_paused = paused.as_deref() == Some("true") || paused.as_deref() == Some("1");

            if !is_paused {
                let score = Self::dt_to_score(nrt);
                pipe.cmd("ZADD")
                    .arg(&nrt_key)
                    .arg(score)
                    .arg(job_id)
                    .ignore();
            }
        } else {
            pipe.cmd("ZREM").arg(&nrt_key).arg(job_id).ignore();
        }

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);
        let rc_key = self.running_count_key(job_id);

        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        let count: i64 = conn
            .incr(&rc_key, 1i64)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(count as u32)
    }

    async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);
        let rc_key = self.running_count_key(job_id);

        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        // Decrement but clamp to 0 using a Lua script
        let script = Script::new(
            r#"
            local val = redis.call('GET', KEYS[1])
            if val == false then val = 0 else val = tonumber(val) end
            if val > 0 then
                val = val - 1
                redis.call('SET', KEYS[1], val)
            end
            return val
            "#,
        );

        let count: i64 = script
            .key(&rc_key)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(count as u32)
    }

    async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let mut conn = self.connection.clone();
        let job_key = self.job_key(job_id);
        let rc_key = self.running_count_key(job_id);

        let exists: bool = conn
            .exists(&job_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if !exists {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        let count: Option<i64> = conn
            .get(&rc_key)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(count.unwrap_or(0) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apsched_core::model::{CallableRef, TaskSpec, TriggerState};

    fn sample_task() -> TaskSpec {
        TaskSpec::new(CallableRef::ImportPath("mymodule:func".to_string()))
    }

    fn sample_trigger() -> TriggerState {
        TriggerState::Date {
            run_date: Utc::now(),
            timezone: "UTC".to_string(),
        }
    }

    fn make_schedule(id: &str, next_run_time: Option<DateTime<Utc>>) -> ScheduleSpec {
        let mut spec = ScheduleSpec::new(id.to_string(), sample_task(), sample_trigger());
        spec.next_run_time = next_run_time;
        spec
    }

    /// Helper to create a Redis store for testing.
    /// Uses default localhost Redis on port 6379, database 15 (test database).
    async fn create_test_store() -> RedisJobStore {
        let store = RedisJobStore::new("redis://127.0.0.1:6379/15", Some("test_apsched:"))
            .await
            .expect("failed to connect to Redis (is it running?)");

        // Clean up test keys
        store.remove_all_jobs().await.ok();
        store
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored (requires Redis on localhost)
    async fn test_redis_add_get() {
        let store = create_test_store().await;
        let now = Utc::now();
        let spec = make_schedule("redis_job1", Some(now));

        store.add_job(spec.clone()).await.unwrap();

        let retrieved = store.get_job("redis_job1").await.unwrap();
        assert_eq!(retrieved.id, "redis_job1");
        assert_eq!(retrieved.executor, "default");
        assert!(!retrieved.paused);
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_duplicate_job() {
        let store = create_test_store().await;
        let spec = make_schedule("redis_dup", Some(Utc::now()));

        store.add_job(spec.clone()).await.unwrap();
        let result = store.add_job(spec).await;
        assert!(matches!(result, Err(StoreError::DuplicateJob { .. })));
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_remove_job() {
        let store = create_test_store().await;
        let spec = make_schedule("redis_rm", Some(Utc::now()));
        store.add_job(spec).await.unwrap();

        store.remove_job("redis_rm").await.unwrap();

        let result = store.get_job("redis_rm").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_due_jobs() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::hours(1);
        let future = now + chrono::Duration::hours(1);

        store
            .add_job(make_schedule("redis_past", Some(past)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("redis_future", Some(future)))
            .await
            .unwrap();

        let due = store.get_due_jobs(now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "redis_past");
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_acquire_jobs() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(5);
        store
            .add_job(make_schedule("redis_acq", Some(past)))
            .await
            .unwrap();

        let leases1 = store.acquire_jobs("sched-A", 10, now).await.unwrap();
        assert_eq!(leases1.len(), 1);

        // Second acquire should get nothing (lease not expired)
        let leases2 = store.acquire_jobs("sched-B", 10, now).await.unwrap();
        assert!(leases2.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_running_count() {
        let store = create_test_store().await;
        store
            .add_job(make_schedule("redis_rc", Some(Utc::now())))
            .await
            .unwrap();

        assert_eq!(store.get_running_count("redis_rc").await.unwrap(), 0);
        assert_eq!(store.increment_running_count("redis_rc").await.unwrap(), 1);
        assert_eq!(store.increment_running_count("redis_rc").await.unwrap(), 2);
        assert_eq!(store.decrement_running_count("redis_rc").await.unwrap(), 1);
        assert_eq!(store.decrement_running_count("redis_rc").await.unwrap(), 0);
        assert_eq!(store.decrement_running_count("redis_rc").await.unwrap(), 0);
    }
}
