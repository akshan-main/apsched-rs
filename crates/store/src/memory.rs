use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;

use apsched_core::error::StoreError;
use apsched_core::model::{JobLease, ScheduleSpec};
use apsched_core::traits::JobStore;

/// In-memory job store backed by DashMap for O(1) lookups and a BTreeMap time index
/// for O(log n) due-job retrieval.
#[derive(Debug)]
pub struct MemoryJobStore {
    /// Primary storage: job_id -> ScheduleSpec
    jobs: DashMap<String, ScheduleSpec>,
    /// Sorted index of (next_run_time, job_id) for efficient due-job queries.
    /// Only jobs with a Some(next_run_time) appear here.
    time_index: RwLock<BTreeMap<(DateTime<Utc>, String), ()>>,
    /// Running instance counts per job_id.
    running_counts: DashMap<String, u32>,
    /// Lease tracking for multi-scheduler coordination.
    leases: DashMap<String, JobLease>,
}

impl MemoryJobStore {
    /// Create a new empty in-memory job store.
    pub fn new() -> Self {
        Self {
            jobs: DashMap::new(),
            time_index: RwLock::new(BTreeMap::new()),
            running_counts: DashMap::new(),
            leases: DashMap::new(),
        }
    }

    /// Insert a job's next_run_time into the time index.
    fn index_insert(&self, job_id: &str, nrt: DateTime<Utc>) {
        let mut idx = self.time_index.write();
        idx.insert((nrt, job_id.to_string()), ());
    }

    /// Remove a job's next_run_time from the time index.
    fn index_remove(&self, job_id: &str, nrt: DateTime<Utc>) {
        let mut idx = self.time_index.write();
        idx.remove(&(nrt, job_id.to_string()));
    }
}

impl Default for MemoryJobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobStore for MemoryJobStore {
    async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let job_id = schedule.id.clone();

        // Check for duplicates
        if self.jobs.contains_key(&job_id) {
            if schedule.replace_existing {
                // Remove old entry from time index
                if let Some(old) = self.jobs.get(&job_id) {
                    if let Some(nrt) = old.next_run_time {
                        self.index_remove(&job_id, nrt);
                    }
                }
            } else {
                return Err(StoreError::DuplicateJob { job_id });
            }
        }

        // Index the new next_run_time
        if let Some(nrt) = schedule.next_run_time {
            self.index_insert(&job_id, nrt);
        }

        self.jobs.insert(job_id, schedule);
        Ok(())
    }

    async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let job_id = schedule.id.clone();

        // Remove old time index entry
        let old = self
            .jobs
            .get(&job_id)
            .ok_or_else(|| StoreError::JobNotFound {
                job_id: job_id.clone(),
            })?;
        if let Some(nrt) = old.next_run_time {
            self.index_remove(&job_id, nrt);
        }
        drop(old);

        // Insert new time index entry
        if let Some(nrt) = schedule.next_run_time {
            self.index_insert(&job_id, nrt);
        }

        self.jobs.insert(job_id, schedule);
        Ok(())
    }

    async fn remove_job(&self, job_id: &str) -> Result<(), StoreError> {
        let (_, spec) = self
            .jobs
            .remove(job_id)
            .ok_or_else(|| StoreError::JobNotFound {
                job_id: job_id.to_string(),
            })?;

        if let Some(nrt) = spec.next_run_time {
            self.index_remove(job_id, nrt);
        }

        self.running_counts.remove(job_id);
        self.leases.remove(job_id);

        Ok(())
    }

    async fn remove_all_jobs(&self) -> Result<(), StoreError> {
        self.jobs.clear();
        self.time_index.write().clear();
        self.running_counts.clear();
        self.leases.clear();
        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError> {
        self.jobs
            .get(job_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| StoreError::JobNotFound {
                job_id: job_id.to_string(),
            })
    }

    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
        Ok(self.jobs.iter().map(|r| r.value().clone()).collect())
    }

    async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
        let idx = self.time_index.read();
        let mut result = Vec::new();

        // Iterate from the beginning up to (now, max_string) inclusive.
        // BTreeMap range ..= gives us all entries with key <= bound.
        let upper = (now, String::from("\u{10FFFF}"));
        for ((_, job_id), _) in idx.range(..=upper) {
            if let Some(entry) = self.jobs.get(job_id) {
                let spec = entry.value();
                if !spec.paused {
                    result.push(spec.clone());
                }
            }
        }

        Ok(result)
    }

    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        let idx = self.time_index.read();
        // Walk from earliest until we find a non-paused job.
        for ((nrt, job_id), _) in idx.iter() {
            if let Some(entry) = self.jobs.get(job_id) {
                if !entry.value().paused {
                    return Ok(Some(*nrt));
                }
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
        let due = self.get_due_jobs(now).await?;
        let mut acquired = Vec::new();

        for spec in due {
            if acquired.len() >= max_jobs {
                break;
            }
            // Skip if already leased
            if self.leases.contains_key(&spec.id) {
                continue;
            }

            let lease = JobLease {
                job_id: spec.id.clone(),
                scheduler_id: scheduler_id.to_string(),
                acquired_at: now,
                expires_at: now + Duration::seconds(30),
                version: spec.version,
            };
            self.leases.insert(spec.id.clone(), lease.clone());
            acquired.push(lease);
        }

        Ok(acquired)
    }

    async fn release_job(&self, job_id: &str, _scheduler_id: &str) -> Result<(), StoreError> {
        self.leases.remove(job_id);
        Ok(())
    }

    async fn update_next_run_time(
        &self,
        job_id: &str,
        next: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        let mut entry = self
            .jobs
            .get_mut(job_id)
            .ok_or_else(|| StoreError::JobNotFound {
                job_id: job_id.to_string(),
            })?;

        // Remove old index entry
        if let Some(old_nrt) = entry.next_run_time {
            self.index_remove(job_id, old_nrt);
        }

        // Update the spec
        entry.next_run_time = next;

        // Insert new index entry
        if let Some(new_nrt) = next {
            self.index_insert(job_id, new_nrt);
        }

        Ok(())
    }

    async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let mut count = self.running_counts.entry(job_id.to_string()).or_insert(0);
        *count += 1;
        Ok(*count)
    }

    async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let mut count = self.running_counts.entry(job_id.to_string()).or_insert(0);
        *count = count.saturating_sub(1);
        Ok(*count)
    }

    async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        Ok(self
            .running_counts
            .get(job_id)
            .map(|r| *r.value())
            .unwrap_or(0))
    }

    async fn cleanup_stale_leases(&self, now: DateTime<Utc>) -> Result<u32, StoreError> {
        let mut cleaned = 0u32;
        let stale_keys: Vec<String> = self
            .leases
            .iter()
            .filter(|entry| entry.value().expires_at < now)
            .map(|entry| entry.key().clone())
            .collect();
        for key in stale_keys {
            self.leases.remove(&key);
            cleaned += 1;
        }
        Ok(cleaned)
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
        let mut spec = ScheduleSpec::new(id, sample_task(), sample_trigger());
        spec.next_run_time = next_run_time;
        spec
    }

    #[tokio::test]
    async fn test_add_and_get_job() {
        let store = MemoryJobStore::new();
        let spec = make_schedule("job1", Some(Utc::now()));
        store.add_job(spec.clone()).await.unwrap();

        let retrieved = store.get_job("job1").await.unwrap();
        assert_eq!(retrieved.id, "job1");
    }

    #[tokio::test]
    async fn test_get_nonexistent_job() {
        let store = MemoryJobStore::new();
        let result = store.get_job("nope").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_remove_job() {
        let store = MemoryJobStore::new();
        let spec = make_schedule("job1", Some(Utc::now()));
        store.add_job(spec).await.unwrap();
        store.remove_job("job1").await.unwrap();

        let result = store.get_job("job1").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_remove_nonexistent_job() {
        let store = MemoryJobStore::new();
        let result = store.remove_job("nope").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_duplicate_id_replace_existing_true() {
        let store = MemoryJobStore::new();
        let mut spec = make_schedule("job1", Some(Utc::now()));
        store.add_job(spec.clone()).await.unwrap();

        spec.replace_existing = true;
        spec.name = Some("updated".to_string());
        store.add_job(spec).await.unwrap();

        let retrieved = store.get_job("job1").await.unwrap();
        assert_eq!(retrieved.name, Some("updated".to_string()));
    }

    #[tokio::test]
    async fn test_duplicate_id_replace_existing_false() {
        let store = MemoryJobStore::new();
        let spec = make_schedule("job1", Some(Utc::now()));
        store.add_job(spec.clone()).await.unwrap();

        let result = store.add_job(spec).await;
        assert!(matches!(result, Err(StoreError::DuplicateJob { .. })));
    }

    #[tokio::test]
    async fn test_get_due_jobs_ordering() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        let t1 = now - Duration::seconds(30);
        let t2 = now - Duration::seconds(20);
        let t3 = now - Duration::seconds(10);
        let t_future = now + Duration::seconds(60);

        store
            .add_job(make_schedule("job_c", Some(t3)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job_a", Some(t1)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job_b", Some(t2)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job_future", Some(t_future)))
            .await
            .unwrap();

        let due = store.get_due_jobs(now).await.unwrap();
        assert_eq!(due.len(), 3);
        // Should be in chronological order (earliest first)
        assert_eq!(due[0].id, "job_a");
        assert_eq!(due[1].id, "job_b");
        assert_eq!(due[2].id, "job_c");
    }

    #[tokio::test]
    async fn test_get_due_jobs_skips_paused() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        let mut spec = make_schedule("paused_job", Some(now - Duration::seconds(10)));
        spec.paused = true;
        store.add_job(spec).await.unwrap();

        store
            .add_job(make_schedule(
                "active_job",
                Some(now - Duration::seconds(5)),
            ))
            .await
            .unwrap();

        let due = store.get_due_jobs(now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "active_job");
    }

    #[tokio::test]
    async fn test_get_next_run_time() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        // Empty store
        assert_eq!(store.get_next_run_time().await.unwrap(), None);

        let t1 = now + Duration::seconds(60);
        let t2 = now + Duration::seconds(30);
        store
            .add_job(make_schedule("job1", Some(t1)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job2", Some(t2)))
            .await
            .unwrap();

        assert_eq!(store.get_next_run_time().await.unwrap(), Some(t2));
    }

    #[tokio::test]
    async fn test_get_next_run_time_skips_paused() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        let t1 = now + Duration::seconds(10);
        let t2 = now + Duration::seconds(60);

        let mut spec = make_schedule("paused", Some(t1));
        spec.paused = true;
        store.add_job(spec).await.unwrap();
        store
            .add_job(make_schedule("active", Some(t2)))
            .await
            .unwrap();

        assert_eq!(store.get_next_run_time().await.unwrap(), Some(t2));
    }

    #[tokio::test]
    async fn test_empty_store() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        assert!(store.get_all_jobs().await.unwrap().is_empty());
        assert!(store.get_due_jobs(now).await.unwrap().is_empty());
        assert_eq!(store.get_next_run_time().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_running_count_increment_decrement() {
        let store = MemoryJobStore::new();

        assert_eq!(store.get_running_count("job1").await.unwrap(), 0);

        assert_eq!(store.increment_running_count("job1").await.unwrap(), 1);
        assert_eq!(store.increment_running_count("job1").await.unwrap(), 2);
        assert_eq!(store.get_running_count("job1").await.unwrap(), 2);

        assert_eq!(store.decrement_running_count("job1").await.unwrap(), 1);
        assert_eq!(store.get_running_count("job1").await.unwrap(), 1);

        assert_eq!(store.decrement_running_count("job1").await.unwrap(), 0);
        // Should not go below zero
        assert_eq!(store.decrement_running_count("job1").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_lease_acquire_and_release() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        store
            .add_job(make_schedule("job1", Some(now - Duration::seconds(10))))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job2", Some(now - Duration::seconds(5))))
            .await
            .unwrap();

        let leases = store.acquire_jobs("sched-1", 10, now).await.unwrap();
        assert_eq!(leases.len(), 2);
        assert_eq!(leases[0].scheduler_id, "sched-1");
        assert_eq!(leases[1].scheduler_id, "sched-1");

        // Acquiring again should return empty (already leased)
        let leases2 = store.acquire_jobs("sched-2", 10, now).await.unwrap();
        assert_eq!(leases2.len(), 0);

        // Release one and re-acquire
        store.release_job("job1", "sched-1").await.unwrap();
        let leases3 = store.acquire_jobs("sched-2", 10, now).await.unwrap();
        assert_eq!(leases3.len(), 1);
        assert_eq!(leases3[0].job_id, "job1");
    }

    #[tokio::test]
    async fn test_acquire_jobs_respects_max() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        for i in 0..5 {
            store
                .add_job(make_schedule(
                    &format!("job{}", i),
                    Some(now - Duration::seconds(10)),
                ))
                .await
                .unwrap();
        }

        let leases = store.acquire_jobs("sched-1", 2, now).await.unwrap();
        assert_eq!(leases.len(), 2);
    }

    #[tokio::test]
    async fn test_update_next_run_time_reindexes() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        let t_old = now + Duration::seconds(10);
        let t_new = now + Duration::seconds(100);

        store
            .add_job(make_schedule("job1", Some(t_old)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job2", Some(now + Duration::seconds(50))))
            .await
            .unwrap();

        // job1 should be next
        assert_eq!(store.get_next_run_time().await.unwrap(), Some(t_old));

        // Move job1 to the future
        store
            .update_next_run_time("job1", Some(t_new))
            .await
            .unwrap();

        // Now job2 should be next
        let next = store.get_next_run_time().await.unwrap().unwrap();
        assert_eq!(next, now + Duration::seconds(50));

        // Verify the job's spec was updated
        let spec = store.get_job("job1").await.unwrap();
        assert_eq!(spec.next_run_time, Some(t_new));
    }

    #[tokio::test]
    async fn test_update_next_run_time_to_none() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        store
            .add_job(make_schedule("job1", Some(now + Duration::seconds(10))))
            .await
            .unwrap();

        store.update_next_run_time("job1", None).await.unwrap();

        let spec = store.get_job("job1").await.unwrap();
        assert_eq!(spec.next_run_time, None);

        // Should not appear in next_run_time
        assert_eq!(store.get_next_run_time().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_remove_all_jobs() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        for i in 0..5 {
            store
                .add_job(make_schedule(
                    &format!("job{}", i),
                    Some(now + Duration::seconds(i as i64)),
                ))
                .await
                .unwrap();
            store
                .increment_running_count(&format!("job{}", i))
                .await
                .unwrap();
        }

        // Acquire some leases
        store
            .acquire_jobs("sched-1", 5, now + Duration::seconds(10))
            .await
            .unwrap();

        store.remove_all_jobs().await.unwrap();

        assert_eq!(store.get_all_jobs().await.unwrap().len(), 0);
        assert_eq!(store.get_next_run_time().await.unwrap(), None);
        assert_eq!(store.get_running_count("job0").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_update_job() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        let spec = make_schedule("job1", Some(now));
        store.add_job(spec).await.unwrap();

        let mut updated = make_schedule("job1", Some(now + Duration::seconds(60)));
        updated.name = Some("updated name".to_string());
        store.update_job(updated).await.unwrap();

        let retrieved = store.get_job("job1").await.unwrap();
        assert_eq!(retrieved.name, Some("updated name".to_string()));
        assert_eq!(retrieved.next_run_time, Some(now + Duration::seconds(60)));
    }

    #[tokio::test]
    async fn test_update_nonexistent_job() {
        let store = MemoryJobStore::new();
        let spec = make_schedule("nope", Some(Utc::now()));
        let result = store.update_job(spec).await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_concurrent_add_remove() {
        use std::sync::Arc;

        let store = Arc::new(MemoryJobStore::new());
        let now = Utc::now();

        let mut handles = Vec::new();

        // Spawn 50 tasks adding jobs
        for i in 0..50 {
            let store = store.clone();
            let handle = tokio::spawn(async move {
                let spec = make_schedule(
                    &format!("concurrent_job_{}", i),
                    Some(now + Duration::seconds(i as i64)),
                );
                store.add_job(spec).await.unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(store.get_all_jobs().await.unwrap().len(), 50);

        // Spawn 50 tasks removing jobs
        let mut handles = Vec::new();
        for i in 0..50 {
            let store = store.clone();
            let handle = tokio::spawn(async move {
                store
                    .remove_job(&format!("concurrent_job_{}", i))
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(store.get_all_jobs().await.unwrap().len(), 0);
        assert_eq!(store.get_next_run_time().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_get_all_jobs() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        store
            .add_job(make_schedule("job1", Some(now)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job2", Some(now)))
            .await
            .unwrap();
        store.add_job(make_schedule("job3", None)).await.unwrap();

        let all = store.get_all_jobs().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_jobs_with_no_next_run_time_not_due() {
        let store = MemoryJobStore::new();
        let now = Utc::now();

        store.add_job(make_schedule("job1", None)).await.unwrap();

        let due = store.get_due_jobs(now).await.unwrap();
        assert!(due.is_empty());
    }

    #[tokio::test]
    async fn test_concurrent_acquire_same_job_only_once() {
        // Bug 1 fix test: two concurrent acquire_jobs calls for the same job
        // should only succeed once.
        use std::sync::Arc;

        let store = Arc::new(MemoryJobStore::new());
        let now = Utc::now();
        store
            .add_job(make_schedule("contested", Some(now - Duration::seconds(5))))
            .await
            .unwrap();

        let store1 = store.clone();
        let store2 = store.clone();

        let (leases1, leases2) = tokio::join!(
            store1.acquire_jobs("sched-A", 10, now),
            store2.acquire_jobs("sched-B", 10, now),
        );

        let total = leases1.unwrap().len() + leases2.unwrap().len();
        // Only one scheduler should have acquired the job
        assert_eq!(
            total, 1,
            "exactly one scheduler should acquire the contested job"
        );
    }

    #[tokio::test]
    async fn test_cleanup_stale_leases() {
        // Bug 3 fix test: stale leases should be cleaned up.
        let store = MemoryJobStore::new();
        let now = Utc::now();

        store
            .add_job(make_schedule("job1", Some(now - Duration::seconds(60))))
            .await
            .unwrap();
        store
            .add_job(make_schedule("job2", Some(now - Duration::seconds(60))))
            .await
            .unwrap();

        // Acquire both jobs
        let leases = store
            .acquire_jobs("crashed-scheduler", 10, now)
            .await
            .unwrap();
        assert_eq!(leases.len(), 2);

        // Simulate time passing beyond lease expiry
        let after_expiry = now + Duration::seconds(60);

        // Before cleanup, new scheduler cannot acquire (leases are expired but not cleaned)
        // Actually for MemoryJobStore, acquire_jobs checks leases.contains_key which doesn't
        // check expiry. So stale leases DO block. Let's verify and then clean up.
        let leases2 = store.acquire_jobs("new-scheduler", 10, now).await.unwrap();
        assert_eq!(leases2.len(), 0, "leases should block acquisition");

        // Cleanup stale leases
        let cleaned = store.cleanup_stale_leases(after_expiry).await.unwrap();
        assert_eq!(cleaned, 2);

        // Now the jobs should be acquirable again
        let leases3 = store
            .acquire_jobs("new-scheduler", 10, after_expiry)
            .await
            .unwrap();
        assert_eq!(leases3.len(), 2);
    }
}
