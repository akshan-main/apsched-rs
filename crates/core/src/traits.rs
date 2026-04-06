use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::{ExecutorError, StoreError};
use crate::model::{JobLease, JobResultEnvelope, JobSpec, ScheduleSpec, TriggerState};

/// Trait that all trigger implementations must satisfy.
///
/// A trigger determines the schedule on which a job fires. Each call to
/// `get_next_fire_time` returns the next point in time at which the job
/// should execute, or `None` if the trigger is exhausted.
pub trait Trigger: Send + Sync + std::fmt::Debug {
    /// Compute the next fire time.
    ///
    /// * `previous_fire_time` - The last time this trigger fired, or `None`
    ///   if the job has never run.
    /// * `now` - The current wall-clock time (UTC).
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>>;

    /// Human-readable description of the trigger, e.g. `"cron[* * * * *]"`.
    fn describe(&self) -> String;

    /// Serialize this trigger's configuration into a transport-safe enum.
    fn serialize_state(&self) -> TriggerState;

    /// A static string identifying the trigger type, e.g. `"cron"`, `"date"`.
    fn trigger_type(&self) -> &'static str;
}

/// Trait for job persistence backends.
///
/// Implementations must be safe to share across threads and support async operations.
/// All methods that can fail return `Result<_, StoreError>`.
#[async_trait]
pub trait JobStore: Send + Sync + std::fmt::Debug {
    /// Add a new job schedule to the store.
    async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError>;

    /// Update an existing job schedule. The job must already exist.
    async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError>;

    /// Remove a job by its ID. Returns an error if the job does not exist.
    async fn remove_job(&self, job_id: &str) -> Result<(), StoreError>;

    /// Remove all jobs from the store.
    async fn remove_all_jobs(&self) -> Result<(), StoreError>;

    /// Get a single job by ID.
    async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError>;

    /// Get all jobs in the store.
    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError>;

    /// Get all jobs that are due to run at or before `now`.
    async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError>;

    /// Get the earliest next_run_time across all jobs, or None if no jobs are scheduled.
    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError>;

    /// Acquire leases on up to `max_jobs` due jobs for the given scheduler.
    async fn acquire_jobs(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError>;

    /// Release a lease on a job.
    async fn release_job(&self, job_id: &str, scheduler_id: &str) -> Result<(), StoreError>;

    /// Update the next_run_time for a job.
    async fn update_next_run_time(
        &self,
        job_id: &str,
        next: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError>;

    /// Increment the running instance count for a job. Returns the new count.
    async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError>;

    /// Decrement the running instance count for a job. Returns the new count.
    async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError>;

    /// Get the current running instance count for a job.
    async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError>;

    /// Clean up stale leases: release any job whose lease has expired.
    ///
    /// This should be called during scheduler startup to recover jobs that were
    /// held by a scheduler that crashed or was killed without releasing its leases.
    /// Returns the number of leases that were cleaned up.
    async fn cleanup_stale_leases(&self, now: DateTime<Utc>) -> Result<u32, StoreError>;
}

/// Trait for job execution backends.
///
/// An executor receives job instances and runs them, reporting results back through
/// the provided channel.
#[async_trait]
pub trait Executor: Send + Sync + std::fmt::Debug {
    /// Start the executor (initialize thread pools, etc.).
    async fn start(&self) -> Result<(), ExecutorError>;

    /// Shut down the executor.
    ///
    /// If `wait` is true, block until all currently running jobs complete.
    async fn shutdown(&self, wait: bool) -> Result<(), ExecutorError>;

    /// Submit a job for execution. Results should be sent to `result_tx`.
    async fn submit_job(
        &self,
        job: JobSpec,
        result_tx: tokio::sync::mpsc::Sender<JobResultEnvelope>,
    ) -> Result<(), ExecutorError>;

    /// Returns the number of currently running jobs in this executor.
    async fn running_job_count(&self) -> usize;

    /// Returns a static string identifying the executor type (e.g. "threadpool", "tokio").
    fn executor_type(&self) -> &'static str;
}
