use async_trait::async_trait;
use chrono::{DateTime, Utc};

use apsched_core::error::StoreError;
use apsched_core::model::{JobLease, ScheduleSpec};
use apsched_core::traits::JobStore;

/// SQL-backed job store using sqlx.
///
/// This is a scaffold that will be fully implemented in Phase 4.
/// All trait methods currently return `StoreError::ConnectionFailed` unless
/// a real connection pool is established.
#[derive(Debug)]
pub struct SqlJobStore {
    #[allow(dead_code)]
    pool: sqlx::AnyPool,
    table_name: String,
    schema: Option<String>,
}

impl SqlJobStore {
    /// Create a new SQL job store by connecting to the given database URL.
    ///
    /// # Arguments
    /// * `url` - A database connection string (e.g., `sqlite://jobs.db`, `postgres://...`).
    /// * `table_name` - The table name to use for job storage.
    pub async fn new(url: &str, table_name: &str) -> Result<Self, StoreError> {
        let pool = sqlx::AnyPool::connect(url)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            pool,
            table_name: table_name.to_string(),
            schema: None,
        })
    }

    /// Set an optional schema prefix (e.g., for PostgreSQL schemas).
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Returns the fully qualified table name (schema.table or just table).
    pub fn qualified_table_name(&self) -> String {
        match &self.schema {
            Some(s) => format!("{}.{}", s, self.table_name),
            None => self.table_name.clone(),
        }
    }

    fn not_implemented() -> StoreError {
        StoreError::ConnectionFailed("SQL store not yet implemented".to_string())
    }
}

#[async_trait]
impl JobStore for SqlJobStore {
    async fn add_job(&self, _schedule: ScheduleSpec) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn update_job(&self, _schedule: ScheduleSpec) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn remove_job(&self, _job_id: &str) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn remove_all_jobs(&self) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn get_job(&self, _job_id: &str) -> Result<ScheduleSpec, StoreError> {
        Err(Self::not_implemented())
    }

    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
        Err(Self::not_implemented())
    }

    async fn get_due_jobs(&self, _now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
        Err(Self::not_implemented())
    }

    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        Err(Self::not_implemented())
    }

    async fn acquire_jobs(
        &self,
        _scheduler_id: &str,
        _max_jobs: usize,
        _now: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        Err(Self::not_implemented())
    }

    async fn release_job(&self, _job_id: &str, _scheduler_id: &str) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn update_next_run_time(
        &self,
        _job_id: &str,
        _next: Option<DateTime<Utc>>,
    ) -> Result<(), StoreError> {
        Err(Self::not_implemented())
    }

    async fn increment_running_count(&self, _job_id: &str) -> Result<u32, StoreError> {
        Err(Self::not_implemented())
    }

    async fn decrement_running_count(&self, _job_id: &str) -> Result<u32, StoreError> {
        Err(Self::not_implemented())
    }

    async fn get_running_count(&self, _job_id: &str) -> Result<u32, StoreError> {
        Err(Self::not_implemented())
    }
}
