//! MongoDB job store implementation.
//!
//! Schema:
//! - Database: configurable (default "apscheduler")
//! - Collection: configurable (default "jobs")
//! - Documents have fields:
//!   - `_id`: String (job_id)
//!   - `next_run_time`: Option<f64> (unix timestamp, indexed for due-job queries)
//!   - `job_state`: Document (serialized ScheduleSpec)
//!   - `paused`: bool
//!   - `acquired_by`: Option<String>
//!   - `acquired_at`: Option<f64>
//!   - `lease_expires_at`: Option<f64>
//!   - `version`: i64
//!   - `running_count`: i32
//!   - `created_at`: f64
//!   - `updated_at`: f64
//!
//! Indexes:
//! - `next_run_time` (sparse, ascending) -- for due-job queries
//! - `acquired_by`, `lease_expires_at` -- for lease cleanup

use apsched_core::error::StoreError;
use apsched_core::model::{JobLease, ScheduleSpec};
use apsched_core::traits::JobStore;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{FindOneAndUpdateOptions, IndexOptions, ReturnDocument};
use mongodb::{Client, Collection, IndexModel};

const DEFAULT_DATABASE: &str = "apscheduler";
const DEFAULT_COLLECTION: &str = "jobs";
const DEFAULT_LEASE_DURATION_SECS: i64 = 30;

/// MongoDB-backed job store.
///
/// Uses optimistic concurrency via per-document `version` checks during
/// `acquire_jobs` so multiple scheduler processes can safely race to acquire
/// due jobs without ever double-executing.
pub struct MongoJobStore {
    #[allow(dead_code)]
    client: Client,
    collection: Collection<Document>,
    database_name: String,
    collection_name: String,
    lease_duration_secs: i64,
}

impl std::fmt::Debug for MongoJobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoJobStore")
            .field("database", &self.database_name)
            .field("collection", &self.collection_name)
            .finish_non_exhaustive()
    }
}

impl MongoJobStore {
    /// Create a new MongoDB job store from a connection URI.
    ///
    /// # Example URIs
    /// - `mongodb://localhost:27017`
    /// - `mongodb://user:pass@host:27017/dbname`
    /// - `mongodb+srv://user:pass@cluster.mongodb.net/dbname`
    pub async fn new(
        uri: &str,
        database: Option<&str>,
        collection: Option<&str>,
    ) -> Result<Self, StoreError> {
        let client = Client::with_uri_str(uri)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        let db_name = database
            .map(|s| s.to_string())
            .unwrap_or_else(|| DEFAULT_DATABASE.to_string());
        let coll_name = collection
            .map(|s| s.to_string())
            .unwrap_or_else(|| DEFAULT_COLLECTION.to_string());

        let db = client.database(&db_name);
        let coll: Collection<Document> = db.collection(&coll_name);

        // Verify connectivity with a ping. Do this first so that a completely
        // unreachable server fails fast with a clear error.
        db.run_command(doc! { "ping": 1 })
            .await
            .map_err(|e| StoreError::ConnectionFailed(format!("ping failed: {}", e)))?;

        // Create indexes. These are idempotent -- MongoDB ignores duplicates.
        let next_run_index = IndexModel::builder()
            .keys(doc! { "next_run_time": 1 })
            .options(IndexOptions::builder().sparse(true).build())
            .build();
        let lease_index = IndexModel::builder()
            .keys(doc! { "acquired_by": 1, "lease_expires_at": 1 })
            .build();

        coll.create_index(next_run_index)
            .await
            .map_err(|e| StoreError::QueryFailed(format!("create index next_run_time: {}", e)))?;
        coll.create_index(lease_index)
            .await
            .map_err(|e| StoreError::QueryFailed(format!("create index lease: {}", e)))?;

        Ok(Self {
            client,
            collection: coll,
            database_name: db_name,
            collection_name: coll_name,
            lease_duration_secs: DEFAULT_LEASE_DURATION_SECS,
        })
    }

    /// Convert a DateTime to a Unix timestamp as f64 with microsecond precision.
    fn ts_to_f64(t: DateTime<Utc>) -> f64 {
        t.timestamp_micros() as f64 / 1_000_000.0
    }

    fn f64_to_dt(ts: f64) -> Option<DateTime<Utc>> {
        let secs = ts.trunc() as i64;
        let nanos = (ts.fract().abs() * 1_000_000_000.0) as u32;
        DateTime::from_timestamp(secs, nanos)
    }

    fn trigger_type(state: &apsched_core::model::TriggerState) -> &'static str {
        use apsched_core::model::TriggerState;
        match state {
            TriggerState::Date { .. } => "date",
            TriggerState::Interval { .. } => "interval",
            TriggerState::Cron { .. } => "cron",
            TriggerState::CalendarInterval { .. } => "calendar_interval",
            TriggerState::Plugin { .. } => "plugin",
        }
    }

    /// Serialize a ScheduleSpec into a MongoDB document.
    fn spec_to_doc(spec: &ScheduleSpec) -> Result<Document, StoreError> {
        let job_state_json = serde_json::to_value(spec)
            .map_err(|e| StoreError::SerializationFailed(e.to_string()))?;
        let job_state = bson::to_bson(&job_state_json)
            .map_err(|e| StoreError::SerializationFailed(e.to_string()))?;

        let now_ts = Self::ts_to_f64(Utc::now());

        let mut d = doc! {
            "_id": &spec.id,
            "job_state": job_state,
            "paused": spec.paused,
            "version": spec.version as i64,
            "running_count": 0i32,
            "created_at": now_ts,
            "updated_at": now_ts,
            "trigger_type": Self::trigger_type(&spec.trigger_state),
            "executor": &spec.executor,
        };
        if let Some(nrt) = spec.next_run_time {
            d.insert("next_run_time", Self::ts_to_f64(nrt));
        }
        Ok(d)
    }

    /// Deserialize a MongoDB document back into a ScheduleSpec.
    fn doc_to_spec(d: &Document) -> Result<ScheduleSpec, StoreError> {
        let job_state = d
            .get("job_state")
            .ok_or_else(|| StoreError::CorruptedData {
                job_id: d.get_str("_id").unwrap_or("?").to_string(),
                detail: "missing job_state field".to_string(),
            })?;
        let json = bson::from_bson::<serde_json::Value>(job_state.clone())
            .map_err(|e| StoreError::DeserializationFailed(e.to_string()))?;
        let mut spec: ScheduleSpec = serde_json::from_value(json)
            .map_err(|e| StoreError::DeserializationFailed(e.to_string()))?;

        // Overlay authoritative top-level fields from columns.
        if let Ok(paused) = d.get_bool("paused") {
            spec.paused = paused;
        }
        if let Ok(version) = d.get_i64("version") {
            spec.version = version as u64;
        }
        if let Ok(executor) = d.get_str("executor") {
            spec.executor = executor.to_string();
        }
        match d.get("next_run_time") {
            Some(Bson::Double(ts)) => {
                spec.next_run_time = Self::f64_to_dt(*ts);
            }
            Some(Bson::Int64(i)) => {
                spec.next_run_time = Self::f64_to_dt(*i as f64);
            }
            Some(Bson::Int32(i)) => {
                spec.next_run_time = Self::f64_to_dt(*i as f64);
            }
            _ => {
                spec.next_run_time = None;
            }
        }
        Ok(spec)
    }
}

#[async_trait]
impl JobStore for MongoJobStore {
    async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let doc = Self::spec_to_doc(&schedule)?;
        match self.collection.insert_one(doc).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let s = e.to_string();
                if s.contains("E11000") || s.contains("duplicate key") {
                    Err(StoreError::DuplicateJob {
                        job_id: schedule.id.clone(),
                    })
                } else {
                    Err(StoreError::QueryFailed(s))
                }
            }
        }
    }

    async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let doc = Self::spec_to_doc(&schedule)?;
        let result = self
            .collection
            .replace_one(doc! { "_id": &schedule.id }, doc)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if result.matched_count == 0 {
            return Err(StoreError::JobNotFound {
                job_id: schedule.id.clone(),
            });
        }
        Ok(())
    }

    async fn remove_job(&self, job_id: &str) -> Result<(), StoreError> {
        let result = self
            .collection
            .delete_one(doc! { "_id": job_id })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if result.deleted_count == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }
        Ok(())
    }

    async fn remove_all_jobs(&self) -> Result<(), StoreError> {
        self.collection
            .delete_many(doc! {})
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError> {
        let result = self
            .collection
            .find_one(doc! { "_id": job_id })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        match result {
            Some(d) => Self::doc_to_spec(&d),
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
        use futures::TryStreamExt;
        let mut cursor = self
            .collection
            .find(doc! {})
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?
        {
            out.push(Self::doc_to_spec(&d)?);
        }
        // Sort by next_run_time ascending; None last.
        out.sort_by(|a, b| match (a.next_run_time, b.next_run_time) {
            (Some(x), Some(y)) => x.cmp(&y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        });
        Ok(out)
    }

    async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
        use futures::TryStreamExt;
        let now_ts = Self::ts_to_f64(now);
        let filter = doc! {
            "next_run_time": { "$lte": now_ts },
            "paused": false,
            "$or": [
                { "acquired_by": { "$exists": false } },
                { "acquired_by": Bson::Null },
                { "lease_expires_at": { "$lt": now_ts } },
            ],
        };
        let mut cursor = self
            .collection
            .find(filter)
            .sort(doc! { "next_run_time": 1 })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        let mut out = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?
        {
            out.push(Self::doc_to_spec(&d)?);
        }
        Ok(out)
    }

    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        let filter = doc! {
            "next_run_time": { "$exists": true, "$ne": Bson::Null },
            "paused": false,
        };
        let result = self
            .collection
            .find_one(filter)
            .sort(doc! { "next_run_time": 1 })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        match result {
            Some(d) => match d.get("next_run_time") {
                Some(Bson::Double(ts)) => Ok(Self::f64_to_dt(*ts)),
                Some(Bson::Int64(i)) => Ok(Self::f64_to_dt(*i as f64)),
                Some(Bson::Int32(i)) => Ok(Self::f64_to_dt(*i as f64)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
    }

    async fn acquire_jobs(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        use futures::TryStreamExt;

        let now_ts = Self::ts_to_f64(now);
        let lease_expires_ts = now_ts + self.lease_duration_secs as f64;

        // Find candidate due jobs.
        let filter = doc! {
            "next_run_time": { "$lte": now_ts },
            "paused": false,
            "$or": [
                { "acquired_by": { "$exists": false } },
                { "acquired_by": Bson::Null },
                { "lease_expires_at": { "$lt": now_ts } },
            ],
        };

        let mut cursor = self
            .collection
            .find(filter)
            .sort(doc! { "next_run_time": 1 })
            .limit(max_jobs as i64)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut candidates: Vec<(String, i64)> = Vec::new();
        while let Some(d) = cursor
            .try_next()
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?
        {
            let id = d.get_str("_id").unwrap_or("").to_string();
            let v = d.get_i64("version").unwrap_or(0);
            if !id.is_empty() {
                candidates.push((id, v));
            }
        }

        // Per-job atomic acquire via findOneAndUpdate with optimistic version
        // check. If a concurrent scheduler won the race the filter matches
        // nothing and we silently skip the job.
        let mut leases = Vec::new();
        for (job_id, version) in candidates {
            let filter = doc! {
                "_id": &job_id,
                "version": version,
                "$or": [
                    { "acquired_by": { "$exists": false } },
                    { "acquired_by": Bson::Null },
                    { "lease_expires_at": { "$lt": now_ts } },
                ],
            };
            let update = doc! {
                "$set": {
                    "acquired_by": scheduler_id,
                    "acquired_at": now_ts,
                    "lease_expires_at": lease_expires_ts,
                },
                "$inc": { "version": 1i64 },
            };
            let opts = FindOneAndUpdateOptions::builder()
                .return_document(ReturnDocument::After)
                .build();
            let result = self
                .collection
                .find_one_and_update(filter, update)
                .with_options(opts)
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
            if let Some(d) = result {
                let new_version = d.get_i64("version").unwrap_or(version + 1);
                leases.push(JobLease {
                    job_id: job_id.clone(),
                    scheduler_id: scheduler_id.to_string(),
                    acquired_at: now,
                    expires_at: now + chrono::Duration::seconds(self.lease_duration_secs),
                    version: new_version as u64,
                });
            }
        }
        Ok(leases)
    }

    async fn release_job(&self, job_id: &str, scheduler_id: &str) -> Result<(), StoreError> {
        let result = self
            .collection
            .update_one(
                doc! { "_id": job_id, "acquired_by": scheduler_id },
                doc! {
                    "$set": {
                        "acquired_by": Bson::Null,
                        "acquired_at": Bson::Null,
                        "lease_expires_at": Bson::Null,
                    },
                },
            )
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if result.matched_count == 0 {
            // Match SQL/Redis semantics: if nothing released, surface a
            // JobNotFound so upstream can distinguish from success.
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }
        Ok(())
    }

    async fn update_next_run_time(
        &self,
        job_id: &str,
        next: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        let now_ts = Self::ts_to_f64(Utc::now());
        let update = match next {
            Some(t) => doc! {
                "$set": {
                    "next_run_time": Self::ts_to_f64(t),
                    "updated_at": now_ts,
                },
            },
            None => doc! {
                "$unset": { "next_run_time": "" },
                "$set": { "updated_at": now_ts },
            },
        };
        let result = self
            .collection
            .update_one(doc! { "_id": job_id }, update)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if result.matched_count == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }
        Ok(())
    }

    async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let opts = FindOneAndUpdateOptions::builder()
            .return_document(ReturnDocument::After)
            .build();
        let result = self
            .collection
            .find_one_and_update(
                doc! { "_id": job_id },
                doc! { "$inc": { "running_count": 1i32 } },
            )
            .with_options(opts)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        match result {
            Some(d) => Ok(d.get_i32("running_count").unwrap_or(0) as u32),
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        // First ensure the job exists so we can return a meaningful error.
        let exists = self
            .collection
            .find_one(doc! { "_id": job_id })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if exists.is_none() {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        let opts = FindOneAndUpdateOptions::builder()
            .return_document(ReturnDocument::After)
            .build();
        // Only decrement when >0; otherwise leave alone and re-read.
        let result = self
            .collection
            .find_one_and_update(
                doc! { "_id": job_id, "running_count": { "$gt": 0i32 } },
                doc! { "$inc": { "running_count": -1i32 } },
            )
            .with_options(opts)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        if let Some(d) = result {
            return Ok(d.get_i32("running_count").unwrap_or(0) as u32);
        }
        // Already at zero -- return 0 without error.
        Ok(0)
    }

    async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let result = self
            .collection
            .find_one(doc! { "_id": job_id })
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        match result {
            Some(d) => Ok(d.get_i32("running_count").unwrap_or(0) as u32),
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn cleanup_stale_leases(&self, now: DateTime<Utc>) -> Result<u32, StoreError> {
        let now_ts = Self::ts_to_f64(now);
        let result = self
            .collection
            .update_many(
                doc! {
                    "acquired_by": { "$ne": Bson::Null },
                    "lease_expires_at": { "$lt": now_ts },
                },
                doc! {
                    "$set": {
                        "acquired_by": Bson::Null,
                        "acquired_at": Bson::Null,
                        "lease_expires_at": Bson::Null,
                    },
                },
            )
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        Ok(result.modified_count as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apsched_core::model::{CallableRef, ScheduleSpec, TaskSpec, TriggerState};

    fn make_spec(id: &str) -> ScheduleSpec {
        let mut s = ScheduleSpec::new(
            id.to_string(),
            TaskSpec::new(CallableRef::ImportPath("test:func".to_string())),
            TriggerState::Date {
                run_date: Utc::now() + chrono::Duration::seconds(60),
                timezone: "UTC".to_string(),
            },
        );
        s.next_run_time = Some(Utc::now() + chrono::Duration::seconds(60));
        s
    }

    async fn create_test_store() -> MongoJobStore {
        MongoJobStore::new(
            "mongodb://localhost:27017",
            Some("apsched_test"),
            Some("jobs"),
        )
        .await
        .expect("failed to connect to MongoDB (is it running?)")
    }

    #[tokio::test]
    #[ignore]
    async fn test_mongo_add_get() {
        let store = create_test_store().await;
        let _ = store.remove_all_jobs().await;
        let spec = make_spec("test1");
        store.add_job(spec).await.unwrap();
        let got = store.get_job("test1").await.unwrap();
        assert_eq!(got.id, "test1");
        store.remove_job("test1").await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_mongo_duplicate_job() {
        let store = create_test_store().await;
        let _ = store.remove_all_jobs().await;
        let spec = make_spec("dup1");
        store.add_job(spec.clone()).await.unwrap();
        let result = store.add_job(spec).await;
        assert!(matches!(result, Err(StoreError::DuplicateJob { .. })));
        store.remove_all_jobs().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_mongo_due_jobs() {
        let store = create_test_store().await;
        let _ = store.remove_all_jobs().await;
        let mut s = make_spec("due1");
        s.next_run_time = Some(Utc::now() - chrono::Duration::seconds(1));
        store.add_job(s).await.unwrap();
        let due = store.get_due_jobs(Utc::now()).await.unwrap();
        assert_eq!(due.len(), 1);
        store.remove_all_jobs().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_mongo_acquire_optimistic_locking() {
        let store = create_test_store().await;
        let _ = store.remove_all_jobs().await;
        let mut s = make_spec("acq1");
        s.next_run_time = Some(Utc::now() - chrono::Duration::seconds(1));
        store.add_job(s).await.unwrap();

        let now = Utc::now();
        let leases1 = store.acquire_jobs("scheduler1", 10, now).await.unwrap();
        let leases2 = store.acquire_jobs("scheduler2", 10, now).await.unwrap();
        assert_eq!(leases1.len(), 1);
        assert_eq!(leases2.len(), 0);
        store.remove_all_jobs().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_mongo_running_count() {
        let store = create_test_store().await;
        let _ = store.remove_all_jobs().await;
        store.add_job(make_spec("rc1")).await.unwrap();
        assert_eq!(store.get_running_count("rc1").await.unwrap(), 0);
        assert_eq!(store.increment_running_count("rc1").await.unwrap(), 1);
        assert_eq!(store.increment_running_count("rc1").await.unwrap(), 2);
        assert_eq!(store.decrement_running_count("rc1").await.unwrap(), 1);
        assert_eq!(store.decrement_running_count("rc1").await.unwrap(), 0);
        assert_eq!(store.decrement_running_count("rc1").await.unwrap(), 0);
        store.remove_all_jobs().await.unwrap();
    }
}
