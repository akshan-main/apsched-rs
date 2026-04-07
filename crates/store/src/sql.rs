use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{AnyPool, Row};

use apsched_core::error::StoreError;
use apsched_core::model::{JobLease, ScheduleSpec};
use apsched_core::traits::JobStore;

/// Default lease duration for acquired jobs (30 seconds).
const DEFAULT_LEASE_DURATION_SECS: i64 = 30;

/// Database backend detected from the connection URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbBackend {
    Sqlite,
    Postgres,
}

/// SQL-backed job store using sqlx with AnyPool.
///
/// Supports both SQLite and PostgreSQL, auto-detected from the connection URL.
///
/// Key differences by backend:
/// - **SQLite**: uses `INTEGER` for booleans, optimistic locking for `acquire_jobs`
/// - **PostgreSQL**: uses `BOOLEAN` columns, `FOR UPDATE SKIP LOCKED` for
///   lock-free distributed job acquisition
#[derive(Debug, Clone)]
pub struct SqlJobStore {
    pool: AnyPool,
    table_name: String,
    backend: DbBackend,
}

impl SqlJobStore {
    /// Create a new SQL job store by connecting to the given database URL.
    ///
    /// # Arguments
    /// * `database_url` - A connection string. Prefixes:
    ///   - `sqlite:` for SQLite (e.g. `sqlite::memory:`, `sqlite://jobs.db`)
    ///   - `postgres:` or `postgresql:` for PostgreSQL
    /// * `table_name` - Optional table name override; defaults to `"apscheduler_jobs"`.
    pub async fn new(database_url: &str, table_name: Option<&str>) -> Result<Self, StoreError> {
        // Install AnyPool drivers
        sqlx::any::install_default_drivers();

        let backend = Self::detect_backend(database_url)?;

        // Normalize URL for SQLAlchemy compatibility. SQLAlchemy uses
        // `sqlite:///relative.db` (3 slashes, relative) and
        // `sqlite:////absolute.db` (4 slashes, absolute). sqlx expects
        // `sqlite:filename` or `sqlite://absolute/path`. Translate.
        //
        // Also map SQLAlchemy in-memory forms (`sqlite:///:memory:`,
        // `sqlite://:memory:`) to sqlx's canonical `sqlite::memory:`.
        let normalized_url: String =
            if backend == DbBackend::Sqlite && database_url.contains(":memory:") {
                "sqlite::memory:".to_string()
            } else if backend == DbBackend::Sqlite && !database_url.contains(":memory:") {
                let rest = database_url.trim_start_matches("sqlite:");
                // Count leading slashes
                let mut slashes = 0usize;
                for c in rest.chars() {
                    if c == '/' {
                        slashes += 1;
                    } else {
                        break;
                    }
                }
                let path_part = &rest[slashes..];
                match slashes {
                    // `sqlite:foo.db` - relative, pass through
                    0 => database_url.to_string(),
                    // `sqlite:/foo.db` - absolute? treat as relative-rooted path
                    1 => format!("sqlite:///{}", path_part),
                    // `sqlite://foo.db` - relative (no authority); pass through
                    2 => database_url.to_string(),
                    // `sqlite:///foo/bar` - SQLAlchemy absolute form; sqlx expects this
                    3 => database_url.to_string(),
                    // `sqlite:////foo/bar` - SQLAlchemy extra-slash absolute;
                    // collapse to 3 slashes for sqlx.
                    _ => format!("sqlite:///{}", path_part),
                }
            } else {
                database_url.to_string()
            };
        // Ensure sqlite creates the database file if it does not exist.
        // sqlx defaults to read-only open, which errors with "unable to open
        // database file" when the file is absent. Append ?mode=rwc.
        let normalized_url = if backend == DbBackend::Sqlite
            && !normalized_url.contains(":memory:")
            && !normalized_url.contains("mode=")
        {
            if normalized_url.contains('?') {
                format!("{}&mode=rwc", normalized_url)
            } else {
                format!("{}?mode=rwc", normalized_url)
            }
        } else {
            normalized_url
        };
        let database_url = normalized_url.as_str();

        // For in-memory SQLite, limit to 1 connection so all queries
        // share the same in-memory database.
        let max_conns = if database_url.contains(":memory:") {
            1
        } else {
            5
        };

        let pool = AnyPoolOptions::new()
            .max_connections(max_conns)
            .connect(database_url)
            .await
            .map_err(|e| StoreError::ConnectionFailed(e.to_string()))?;

        let store = Self {
            pool,
            table_name: table_name.unwrap_or("apscheduler_jobs").to_string(),
            backend,
        };

        store.create_schema().await?;
        Ok(store)
    }

    /// Detect the database backend from the URL prefix.
    fn detect_backend(url: &str) -> Result<DbBackend, StoreError> {
        if url.starts_with("sqlite:") {
            Ok(DbBackend::Sqlite)
        } else if url.starts_with("postgres:") || url.starts_with("postgresql:") {
            Ok(DbBackend::Postgres)
        } else {
            Err(StoreError::ConnectionFailed(format!(
                "Unsupported database URL prefix. Expected 'sqlite:', 'postgres:', or 'postgresql:'. Got: {}",
                url.split(':').next().unwrap_or("(empty)")
            )))
        }
    }

    /// Get the SQL placeholder for the given 1-based parameter index.
    /// SQLite uses `?`, Postgres uses `$1`, `$2`, etc.
    fn placeholder(&self, index: usize) -> String {
        match self.backend {
            DbBackend::Sqlite => "?".to_string(),
            DbBackend::Postgres => format!("${}", index),
        }
    }

    /// Create the jobs table and index if they don't exist.
    async fn create_schema(&self) -> Result<(), StoreError> {
        let (paused_type, paused_default) = match self.backend {
            DbBackend::Sqlite => ("INTEGER", "0"),
            DbBackend::Postgres => ("BOOLEAN", "FALSE"),
        };

        let create_table = format!(
            r#"CREATE TABLE IF NOT EXISTS {table} (
                id TEXT NOT NULL PRIMARY KEY,
                next_run_time TEXT,
                job_state TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                executor TEXT NOT NULL DEFAULT 'default',
                paused {paused_type} NOT NULL DEFAULT {paused_default},
                acquired_by TEXT,
                acquired_at TEXT,
                lease_expires_at TEXT,
                version INTEGER NOT NULL DEFAULT 0,
                running_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"#,
            table = self.table_name,
            paused_type = paused_type,
            paused_default = paused_default,
        );

        // Partial indexes: Postgres supports WHERE clauses on indexes.
        // SQLite also supports partial indexes.
        let paused_cond = match self.backend {
            DbBackend::Sqlite => "paused = 0",
            DbBackend::Postgres => "paused = FALSE",
        };

        let create_index = format!(
            r#"CREATE INDEX IF NOT EXISTS idx_{table}_next_run_time
                ON {table} (next_run_time)
                WHERE next_run_time IS NOT NULL AND {paused_cond}"#,
            table = self.table_name,
            paused_cond = paused_cond,
        );

        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        sqlx::query(&create_index)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    /// Serialize a ScheduleSpec to JSON string for storage.
    fn serialize_spec(spec: &ScheduleSpec) -> Result<String, StoreError> {
        serde_json::to_string(spec).map_err(|e| StoreError::SerializationFailed(e.to_string()))
    }

    /// Deserialize a ScheduleSpec from a JSON string.
    fn deserialize_spec(json: &str) -> Result<ScheduleSpec, StoreError> {
        serde_json::from_str(json).map_err(|e| StoreError::DeserializationFailed(e.to_string()))
    }

    /// Extract the trigger type string from a ScheduleSpec.
    fn trigger_type(spec: &ScheduleSpec) -> &'static str {
        match &spec.trigger_state {
            apsched_core::model::TriggerState::Date { .. } => "date",
            apsched_core::model::TriggerState::Interval { .. } => "interval",
            apsched_core::model::TriggerState::Cron { .. } => "cron",
            apsched_core::model::TriggerState::CalendarInterval { .. } => "calendar_interval",
            apsched_core::model::TriggerState::Plugin { .. } => "plugin",
        }
    }

    /// Convert a DateTime to an ISO 8601 string for storage.
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

    /// Parse a ScheduleSpec from a database row.
    fn row_to_spec(row: &AnyRow, backend: DbBackend) -> Result<ScheduleSpec, StoreError> {
        let job_state: String = row
            .try_get("job_state")
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        let mut spec = Self::deserialize_spec(&job_state)?;

        // Overlay database-authoritative fields onto the deserialized spec
        let next_run_time_str: Option<String> = row
            .try_get("next_run_time")
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        spec.next_run_time = match next_run_time_str {
            Some(s) => Some(Self::string_to_dt(&s)?),
            None => None,
        };

        // Read paused field - Postgres stores BOOLEAN, SQLite stores INTEGER
        let paused: bool = match backend {
            DbBackend::Sqlite => {
                let val: i32 = row
                    .try_get("paused")
                    .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
                val != 0
            }
            DbBackend::Postgres => row
                .try_get("paused")
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?,
        };
        spec.paused = paused;

        let version: i64 = row
            .try_get("version")
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        spec.version = version as u64;

        let executor: String = row
            .try_get("executor")
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
        spec.executor = executor;

        Ok(spec)
    }

    /// Build the paused value for bind parameters.
    /// SQLite expects i32, Postgres expects bool. With AnyPool we bind as i32
    /// for SQLite and bool for Postgres — but AnyPool requires a uniform type.
    /// We use a helper that returns the right SQL literal or bind approach.
    fn paused_false_condition(&self) -> &'static str {
        match self.backend {
            DbBackend::Sqlite => "paused = 0",
            DbBackend::Postgres => "paused = FALSE",
        }
    }
}

#[async_trait]
impl JobStore for SqlJobStore {
    async fn add_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let now = Utc::now();
        let now_str = Self::dt_to_string(&now);
        let job_state = Self::serialize_spec(&schedule)?;
        let trigger_type = Self::trigger_type(&schedule);
        let next_run_time_str = schedule.next_run_time.as_ref().map(Self::dt_to_string);

        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);
        let p4 = self.placeholder(4);
        let p5 = self.placeholder(5);
        let p6 = self.placeholder(6);
        let p7 = self.placeholder(7);
        let p8 = self.placeholder(8);
        let p9 = self.placeholder(9);

        // For paused, use integer in query for both backends (Postgres will cast)
        let paused_val: i32 = if schedule.paused { 1 } else { 0 };

        let query = match self.backend {
            DbBackend::Sqlite => format!(
                r#"INSERT INTO {table} (id, next_run_time, job_state, trigger_type, executor, paused, version, running_count, created_at, updated_at)
                   VALUES ({p1}, {p2}, {p3}, {p4}, {p5}, {p6}, {p7}, 0, {p8}, {p9})"#,
                table = self.table_name,
            ),
            DbBackend::Postgres => format!(
                r#"INSERT INTO {table} (id, next_run_time, job_state, trigger_type, executor, paused, version, running_count, created_at, updated_at)
                   VALUES ({p1}, {p2}, {p3}, {p4}, {p5}, {p6}::integer != 0, {p7}, 0, {p8}, {p9})"#,
                table = self.table_name,
            ),
        };

        sqlx::query(&query)
            .bind(&schedule.id)
            .bind(&next_run_time_str)
            .bind(&job_state)
            .bind(trigger_type)
            .bind(&schedule.executor)
            .bind(paused_val)
            .bind(schedule.version as i64)
            .bind(&now_str)
            .bind(&now_str)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("UNIQUE") || msg.contains("duplicate key") {
                    StoreError::DuplicateJob {
                        job_id: schedule.id.clone(),
                    }
                } else {
                    StoreError::QueryFailed(msg)
                }
            })?;

        Ok(())
    }

    async fn update_job(&self, schedule: ScheduleSpec) -> Result<(), StoreError> {
        let now = Utc::now();
        let now_str = Self::dt_to_string(&now);
        let job_state = Self::serialize_spec(&schedule)?;
        let trigger_type = Self::trigger_type(&schedule);
        let next_run_time_str = schedule.next_run_time.as_ref().map(Self::dt_to_string);
        let paused_val: i32 = if schedule.paused { 1 } else { 0 };

        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);
        let p4 = self.placeholder(4);
        let p5 = self.placeholder(5);
        let p6 = self.placeholder(6);
        let p7 = self.placeholder(7);

        let paused_expr = match self.backend {
            DbBackend::Sqlite => format!("paused = {p5}"),
            DbBackend::Postgres => format!("paused = {p5}::integer != 0"),
        };

        let query = format!(
            r#"UPDATE {table} SET next_run_time = {p1}, job_state = {p2}, trigger_type = {p3}, executor = {p4}, {paused_expr}, version = version + 1, updated_at = {p6}
               WHERE id = {p7}"#,
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&next_run_time_str)
            .bind(&job_state)
            .bind(trigger_type)
            .bind(&schedule.executor)
            .bind(paused_val)
            .bind(&now_str)
            .bind(&schedule.id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::JobNotFound {
                job_id: schedule.id,
            });
        }

        Ok(())
    }

    async fn remove_job(&self, job_id: &str) -> Result<(), StoreError> {
        let p1 = self.placeholder(1);
        let query = format!("DELETE FROM {} WHERE id = {}", self.table_name, p1);

        let result = sqlx::query(&query)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        Ok(())
    }

    async fn remove_all_jobs(&self) -> Result<(), StoreError> {
        let query = format!("DELETE FROM {}", self.table_name);

        sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(())
    }

    async fn get_job(&self, job_id: &str) -> Result<ScheduleSpec, StoreError> {
        let p1 = self.placeholder(1);
        let query = format!("SELECT * FROM {} WHERE id = {}", self.table_name, p1);

        let row = sqlx::query(&query)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        match row {
            Some(row) => Self::row_to_spec(&row, self.backend),
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn get_all_jobs(&self) -> Result<Vec<ScheduleSpec>, StoreError> {
        let query = format!("SELECT * FROM {} ORDER BY next_run_time", self.table_name);

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let backend = self.backend;
        rows.iter().map(|r| Self::row_to_spec(r, backend)).collect()
    }

    async fn get_due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ScheduleSpec>, StoreError> {
        let now_str = Self::dt_to_string(&now);
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let paused_cond = self.paused_false_condition();

        let query = format!(
            r#"SELECT * FROM {table}
               WHERE next_run_time <= {p1} AND {paused_cond}
               AND (acquired_by IS NULL OR lease_expires_at < {p2})
               ORDER BY next_run_time"#,
            table = self.table_name,
        );

        let rows = sqlx::query(&query)
            .bind(&now_str)
            .bind(&now_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let backend = self.backend;
        rows.iter().map(|r| Self::row_to_spec(r, backend)).collect()
    }

    async fn get_next_run_time(&self) -> Result<Option<DateTime<Utc>>, StoreError> {
        let paused_cond = self.paused_false_condition();

        let query = format!(
            "SELECT next_run_time FROM {} WHERE next_run_time IS NOT NULL AND {} ORDER BY next_run_time LIMIT 1",
            self.table_name, paused_cond,
        );

        let row: Option<AnyRow> = sqlx::query(&query)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        match row {
            Some(row) => {
                let nrt_str: String = row
                    .try_get("next_run_time")
                    .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
                let dt = Self::string_to_dt(&nrt_str)?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }

    async fn acquire_jobs(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        let now_str = Self::dt_to_string(&now);
        let lease_expires = now + chrono::Duration::seconds(DEFAULT_LEASE_DURATION_SECS);
        let lease_expires_str = Self::dt_to_string(&lease_expires);

        match self.backend {
            DbBackend::Postgres => {
                self.acquire_jobs_postgres(
                    scheduler_id,
                    max_jobs,
                    &now_str,
                    &lease_expires_str,
                    now,
                    lease_expires,
                )
                .await
            }
            DbBackend::Sqlite => {
                self.acquire_jobs_sqlite(
                    scheduler_id,
                    max_jobs,
                    &now_str,
                    &lease_expires_str,
                    now,
                    lease_expires,
                )
                .await
            }
        }
    }

    async fn release_job(&self, job_id: &str, scheduler_id: &str) -> Result<(), StoreError> {
        let now_str = Self::dt_to_string(&Utc::now());
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);

        let query = format!(
            r#"UPDATE {table} SET acquired_by = NULL, acquired_at = NULL, lease_expires_at = NULL, updated_at = {p1}
               WHERE id = {p2} AND acquired_by = {p3}"#,
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&now_str)
            .bind(job_id)
            .bind(scheduler_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
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
        let now_str = Self::dt_to_string(&Utc::now());
        let next_str = next.as_ref().map(Self::dt_to_string);
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);
        let p3 = self.placeholder(3);

        let query = format!(
            "UPDATE {table} SET next_run_time = {p1}, updated_at = {p2} WHERE id = {p3}",
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&next_str)
            .bind(&now_str)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        Ok(())
    }

    async fn increment_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let now_str = Self::dt_to_string(&Utc::now());
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);

        let query = format!(
            "UPDATE {table} SET running_count = running_count + 1, updated_at = {p1} WHERE id = {p2}",
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&now_str)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        self.get_running_count(job_id).await
    }

    async fn decrement_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let now_str = Self::dt_to_string(&Utc::now());
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);

        let query = format!(
            "UPDATE {table} SET running_count = CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END, updated_at = {p1} WHERE id = {p2}",
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&now_str)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            });
        }

        self.get_running_count(job_id).await
    }

    async fn get_running_count(&self, job_id: &str) -> Result<u32, StoreError> {
        let p1 = self.placeholder(1);
        let query = format!(
            "SELECT running_count FROM {} WHERE id = {}",
            self.table_name, p1
        );

        let row: Option<AnyRow> = sqlx::query(&query)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        match row {
            Some(row) => {
                let count: i32 = row
                    .try_get("running_count")
                    .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
                Ok(count as u32)
            }
            None => Err(StoreError::JobNotFound {
                job_id: job_id.to_string(),
            }),
        }
    }

    async fn cleanup_stale_leases(&self, now: DateTime<Utc>) -> Result<u32, StoreError> {
        let now_str = Self::dt_to_string(&now);
        let p1 = self.placeholder(1);
        let p2 = self.placeholder(2);

        let query = format!(
            r#"UPDATE {table} SET acquired_by = NULL, acquired_at = NULL, lease_expires_at = NULL, updated_at = {p1}
               WHERE lease_expires_at IS NOT NULL AND lease_expires_at < {p2}"#,
            table = self.table_name,
        );

        let result = sqlx::query(&query)
            .bind(&now_str)
            .bind(&now_str)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        Ok(result.rows_affected() as u32)
    }
}

// ---------------------------------------------------------------------------
// Backend-specific acquire_jobs implementations
// ---------------------------------------------------------------------------

impl SqlJobStore {
    /// PostgreSQL acquire: uses `FOR UPDATE SKIP LOCKED` for lock-free
    /// distributed job acquisition. This is the killer feature -- multiple
    /// scheduler instances will never deadlock or double-execute jobs.
    async fn acquire_jobs_postgres(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now_str: &str,
        lease_expires_str: &str,
        now: DateTime<Utc>,
        lease_expires: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        // Single atomic query: SELECT ... FOR UPDATE SKIP LOCKED, then UPDATE
        // We use a CTE (Common Table Expression) to atomically select and update.
        let query = format!(
            r#"WITH candidates AS (
                SELECT id, version FROM {table}
                WHERE next_run_time <= $1
                  AND paused = FALSE
                  AND (acquired_by IS NULL OR lease_expires_at < $2)
                ORDER BY next_run_time
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {table} SET
                acquired_by = $4,
                acquired_at = $5,
                lease_expires_at = $6,
                version = {table}.version + 1
            FROM candidates
            WHERE {table}.id = candidates.id
            RETURNING {table}.id, candidates.version + 1 AS new_version"#,
            table = self.table_name,
        );

        let rows = sqlx::query(&query)
            .bind(now_str)
            .bind(now_str)
            .bind(max_jobs as i64)
            .bind(scheduler_id)
            .bind(now_str)
            .bind(lease_expires_str)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut leases = Vec::with_capacity(rows.len());
        for row in &rows {
            let job_id: String = row
                .try_get("id")
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
            let new_version: i64 = row
                .try_get("new_version")
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
            leases.push(JobLease {
                job_id,
                scheduler_id: scheduler_id.to_string(),
                acquired_at: now,
                expires_at: lease_expires,
                version: new_version as u64,
            });
        }

        Ok(leases)
    }

    /// SQLite acquire: uses optimistic locking (SELECT then UPDATE with version check).
    async fn acquire_jobs_sqlite(
        &self,
        scheduler_id: &str,
        max_jobs: usize,
        now_str: &str,
        lease_expires_str: &str,
        now: DateTime<Utc>,
        lease_expires: DateTime<Utc>,
    ) -> Result<Vec<JobLease>, StoreError> {
        // First, get due jobs that are available for acquisition
        let select_query = format!(
            r#"SELECT id, version FROM {table}
               WHERE next_run_time <= ? AND paused = 0
               AND (acquired_by IS NULL OR lease_expires_at < ?)
               ORDER BY next_run_time
               LIMIT ?"#,
            table = self.table_name,
        );

        let candidates = sqlx::query(&select_query)
            .bind(now_str)
            .bind(now_str)
            .bind(max_jobs as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

        let mut leases = Vec::new();

        // Try to acquire each candidate with optimistic locking
        let update_query = format!(
            r#"UPDATE {table} SET acquired_by = ?, acquired_at = ?, lease_expires_at = ?, version = version + 1
               WHERE id = ? AND version = ? AND (acquired_by IS NULL OR lease_expires_at < ?)"#,
            table = self.table_name,
        );

        for candidate in &candidates {
            let job_id: String = candidate
                .try_get("id")
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;
            let version: i64 = candidate
                .try_get("version")
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

            let result = sqlx::query(&update_query)
                .bind(scheduler_id)
                .bind(now_str)
                .bind(lease_expires_str)
                .bind(&job_id)
                .bind(version)
                .bind(now_str)
                .execute(&self.pool)
                .await
                .map_err(|e| StoreError::QueryFailed(e.to_string()))?;

            if result.rows_affected() > 0 {
                leases.push(JobLease {
                    job_id,
                    scheduler_id: scheduler_id.to_string(),
                    acquired_at: now,
                    expires_at: lease_expires,
                    version: (version + 1) as u64,
                });
            }
            // If rows_affected == 0, another worker acquired it first; skip silently
        }

        Ok(leases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apsched_core::model::{CallableRef, ScheduleSpec, TaskSpec, TriggerState};

    /// Helper to create an in-memory SQLite store for testing.
    async fn create_test_store() -> SqlJobStore {
        // Use a shared in-memory database so the pool connections share state.
        SqlJobStore::new("sqlite::memory:", None)
            .await
            .expect("failed to create test store")
    }

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

    // ---- Test 1: Add and get a job ----

    #[tokio::test]
    async fn test_add_and_get_job() {
        let store = create_test_store().await;
        let now = Utc::now();
        let spec = make_schedule("job1", Some(now));

        store.add_job(spec.clone()).await.unwrap();

        let retrieved = store.get_job("job1").await.unwrap();
        assert_eq!(retrieved.id, "job1");
        assert_eq!(retrieved.executor, "default");
        assert!(!retrieved.paused);
    }

    #[tokio::test]
    async fn test_add_duplicate_job() {
        let store = create_test_store().await;
        let spec = make_schedule("dup1", Some(Utc::now()));

        store.add_job(spec.clone()).await.unwrap();
        let result = store.add_job(spec).await;
        assert!(matches!(result, Err(StoreError::DuplicateJob { .. })));
    }

    // ---- Test 2: Remove a job ----

    #[tokio::test]
    async fn test_remove_job() {
        let store = create_test_store().await;
        let spec = make_schedule("job2", Some(Utc::now()));
        store.add_job(spec).await.unwrap();

        store.remove_job("job2").await.unwrap();

        let result = store.get_job("job2").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_remove_nonexistent_job() {
        let store = create_test_store().await;
        let result = store.remove_job("nonexistent").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_remove_all_jobs() {
        let store = create_test_store().await;
        store
            .add_job(make_schedule("a", Some(Utc::now())))
            .await
            .unwrap();
        store
            .add_job(make_schedule("b", Some(Utc::now())))
            .await
            .unwrap();

        store.remove_all_jobs().await.unwrap();

        let all = store.get_all_jobs().await.unwrap();
        assert!(all.is_empty());
    }

    // ---- Test 3: Due job retrieval ----

    #[tokio::test]
    async fn test_get_due_jobs() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::hours(1);
        let future = now + chrono::Duration::hours(1);

        // Past job - should be due
        store
            .add_job(make_schedule("past", Some(past)))
            .await
            .unwrap();
        // Future job - should NOT be due
        store
            .add_job(make_schedule("future", Some(future)))
            .await
            .unwrap();
        // Paused job with past time - should NOT be due
        let mut paused = make_schedule("paused", Some(past));
        paused.paused = true;
        store.add_job(paused).await.unwrap();

        let due = store.get_due_jobs(now).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, "past");
    }

    #[tokio::test]
    async fn test_get_due_jobs_no_next_run_time() {
        let store = create_test_store().await;
        // Job with no next_run_time should NOT be due
        store.add_job(make_schedule("none", None)).await.unwrap();

        let due = store.get_due_jobs(Utc::now()).await.unwrap();
        assert!(due.is_empty());
    }

    // ---- Test 4: Optimistic locking (acquire same job twice) ----

    #[tokio::test]
    async fn test_acquire_jobs_optimistic_locking() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(5);
        store
            .add_job(make_schedule("contested", Some(past)))
            .await
            .unwrap();

        // First scheduler acquires the job
        let leases1 = store.acquire_jobs("scheduler-A", 10, now).await.unwrap();
        assert_eq!(leases1.len(), 1);
        assert_eq!(leases1[0].job_id, "contested");
        assert_eq!(leases1[0].scheduler_id, "scheduler-A");

        // Second scheduler tries to acquire the same job - should get nothing
        // because the lease hasn't expired yet
        let leases2 = store.acquire_jobs("scheduler-B", 10, now).await.unwrap();
        assert!(leases2.is_empty());
    }

    // ---- Test 5: Lease expiry ----

    #[tokio::test]
    async fn test_lease_expiry() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(5);
        store
            .add_job(make_schedule("expiring", Some(past)))
            .await
            .unwrap();

        // First scheduler acquires the job
        let leases = store.acquire_jobs("scheduler-A", 10, now).await.unwrap();
        assert_eq!(leases.len(), 1);

        // Simulate time passing beyond the lease expiry
        let after_expiry = now + chrono::Duration::seconds(DEFAULT_LEASE_DURATION_SECS + 10);

        // Second scheduler should now be able to acquire the job
        // because the lease has expired (lease_expires_at < after_expiry)
        let leases2 = store
            .acquire_jobs("scheduler-B", 10, after_expiry)
            .await
            .unwrap();
        assert_eq!(leases2.len(), 1);
        assert_eq!(leases2[0].scheduler_id, "scheduler-B");
    }

    // ---- Test 6: Update next_run_time ----

    #[tokio::test]
    async fn test_update_next_run_time() {
        let store = create_test_store().await;
        let now = Utc::now();
        let new_time = now + chrono::Duration::hours(2);
        store
            .add_job(make_schedule("update_nrt", Some(now)))
            .await
            .unwrap();

        store
            .update_next_run_time("update_nrt", Some(new_time))
            .await
            .unwrap();

        let spec = store.get_job("update_nrt").await.unwrap();
        assert_eq!(
            spec.next_run_time.unwrap().timestamp(),
            new_time.timestamp()
        );
    }

    #[tokio::test]
    async fn test_update_next_run_time_to_none() {
        let store = create_test_store().await;
        store
            .add_job(make_schedule("clear_nrt", Some(Utc::now())))
            .await
            .unwrap();

        store.update_next_run_time("clear_nrt", None).await.unwrap();

        let spec = store.get_job("clear_nrt").await.unwrap();
        assert!(spec.next_run_time.is_none());
    }

    #[tokio::test]
    async fn test_update_next_run_time_nonexistent() {
        let store = create_test_store().await;
        let result = store.update_next_run_time("ghost", Some(Utc::now())).await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    // ---- Test 7: Running count management ----

    #[tokio::test]
    async fn test_running_count() {
        let store = create_test_store().await;
        store
            .add_job(make_schedule("rc_job", Some(Utc::now())))
            .await
            .unwrap();

        // Initial count is 0
        let count = store.get_running_count("rc_job").await.unwrap();
        assert_eq!(count, 0);

        // Increment
        let count = store.increment_running_count("rc_job").await.unwrap();
        assert_eq!(count, 1);

        let count = store.increment_running_count("rc_job").await.unwrap();
        assert_eq!(count, 2);

        // Decrement
        let count = store.decrement_running_count("rc_job").await.unwrap();
        assert_eq!(count, 1);

        let count = store.decrement_running_count("rc_job").await.unwrap();
        assert_eq!(count, 0);

        // Decrement below zero should clamp at 0
        let count = store.decrement_running_count("rc_job").await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_running_count_nonexistent() {
        let store = create_test_store().await;
        let result = store.get_running_count("ghost").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));

        let result = store.increment_running_count("ghost").await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    // ---- Additional tests ----

    #[tokio::test]
    async fn test_get_all_jobs() {
        let store = create_test_store().await;
        let now = Utc::now();
        store.add_job(make_schedule("j1", Some(now))).await.unwrap();
        store
            .add_job(make_schedule("j2", Some(now + chrono::Duration::hours(1))))
            .await
            .unwrap();
        store.add_job(make_schedule("j3", None)).await.unwrap();

        let all = store.get_all_jobs().await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_get_next_run_time() {
        let store = create_test_store().await;
        let now = Utc::now();
        let early = now + chrono::Duration::hours(1);
        let late = now + chrono::Duration::hours(5);

        // No jobs -> None
        let nrt = store.get_next_run_time().await.unwrap();
        assert!(nrt.is_none());

        store
            .add_job(make_schedule("late_job", Some(late)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("early_job", Some(early)))
            .await
            .unwrap();

        let nrt = store.get_next_run_time().await.unwrap();
        assert!(nrt.is_some());
        assert_eq!(nrt.unwrap().timestamp(), early.timestamp());
    }

    #[tokio::test]
    async fn test_update_job() {
        let store = create_test_store().await;
        let mut spec = make_schedule("upd1", Some(Utc::now()));
        store.add_job(spec.clone()).await.unwrap();

        spec.executor = "custom".to_string();
        store.update_job(spec).await.unwrap();

        let retrieved = store.get_job("upd1").await.unwrap();
        assert_eq!(retrieved.executor, "custom");
    }

    #[tokio::test]
    async fn test_update_nonexistent_job() {
        let store = create_test_store().await;
        let spec = make_schedule("ghost", Some(Utc::now()));
        let result = store.update_job(spec).await;
        assert!(matches!(result, Err(StoreError::JobNotFound { .. })));
    }

    #[tokio::test]
    async fn test_release_job() {
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(1);
        store
            .add_job(make_schedule("rel_job", Some(past)))
            .await
            .unwrap();

        // Acquire
        let leases = store.acquire_jobs("sched-1", 10, now).await.unwrap();
        assert_eq!(leases.len(), 1);

        // Release
        store.release_job("rel_job", "sched-1").await.unwrap();

        // Should be acquirable again
        let leases2 = store.acquire_jobs("sched-2", 10, now).await.unwrap();
        assert_eq!(leases2.len(), 1);
        assert_eq!(leases2[0].scheduler_id, "sched-2");
    }

    #[tokio::test]
    async fn test_get_next_run_time_skips_paused() {
        let store = create_test_store().await;
        let now = Utc::now();

        let mut paused_spec = make_schedule("paused_early", Some(now));
        paused_spec.paused = true;
        store.add_job(paused_spec).await.unwrap();

        let late = now + chrono::Duration::hours(3);
        store
            .add_job(make_schedule("active_late", Some(late)))
            .await
            .unwrap();

        let nrt = store.get_next_run_time().await.unwrap();
        assert!(nrt.is_some());
        assert_eq!(nrt.unwrap().timestamp(), late.timestamp());
    }

    #[tokio::test]
    async fn test_serialization_roundtrip_interval_trigger() {
        let store = create_test_store().await;
        let now = Utc::now();

        let trigger = TriggerState::Interval {
            weeks: 0,
            days: 1,
            hours: 2,
            minutes: 30,
            seconds: 0,
            start_date: Some(now),
            end_date: None,
            timezone: "UTC".to_string(),
            jitter: Some(5.0),
            interval_micros: None,
        };

        let mut spec = ScheduleSpec::new("interval_job", sample_task(), trigger);
        spec.next_run_time = Some(now);
        store.add_job(spec).await.unwrap();

        let retrieved = store.get_job("interval_job").await.unwrap();
        match &retrieved.trigger_state {
            TriggerState::Interval {
                days,
                hours,
                minutes,
                jitter,
                ..
            } => {
                assert_eq!(*days, 1);
                assert_eq!(*hours, 2);
                assert_eq!(*minutes, 30);
                assert_eq!(*jitter, Some(5.0));
            }
            _ => panic!("expected interval trigger"),
        }
    }

    // ---- Backend detection tests ----

    #[test]
    fn test_detect_backend_sqlite() {
        assert_eq!(
            SqlJobStore::detect_backend("sqlite::memory:").unwrap(),
            DbBackend::Sqlite
        );
        assert_eq!(
            SqlJobStore::detect_backend("sqlite://test.db").unwrap(),
            DbBackend::Sqlite
        );
    }

    #[test]
    fn test_detect_backend_postgres() {
        assert_eq!(
            SqlJobStore::detect_backend("postgres://localhost/test").unwrap(),
            DbBackend::Postgres
        );
        assert_eq!(
            SqlJobStore::detect_backend("postgresql://user:pass@localhost/db").unwrap(),
            DbBackend::Postgres
        );
    }

    #[test]
    fn test_detect_backend_unsupported() {
        assert!(SqlJobStore::detect_backend("mysql://localhost/test").is_err());
    }

    // ---- Bug fix tests ----

    #[tokio::test]
    async fn test_concurrent_acquire_same_job_sqlite() {
        // Bug 1 fix test: two acquire_jobs calls for the same job should only succeed once.
        // SQLite uses optimistic locking with version check.
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(5);
        store
            .add_job(make_schedule("contested", Some(past)))
            .await
            .unwrap();

        // First acquire
        let leases1 = store.acquire_jobs("scheduler-A", 10, now).await.unwrap();
        assert_eq!(leases1.len(), 1);

        // Second acquire - same time, different scheduler
        let leases2 = store.acquire_jobs("scheduler-B", 10, now).await.unwrap();
        assert_eq!(
            leases2.len(),
            0,
            "second scheduler should not acquire an already-leased job"
        );
    }

    #[tokio::test]
    async fn test_cleanup_stale_leases_sql() {
        // Bug 3 fix test: stale leases should be cleaned up on startup.
        let store = create_test_store().await;
        let now = Utc::now();
        let past = now - chrono::Duration::minutes(5);

        store
            .add_job(make_schedule("stale1", Some(past)))
            .await
            .unwrap();
        store
            .add_job(make_schedule("stale2", Some(past)))
            .await
            .unwrap();

        // Simulate a crashed scheduler that acquired both jobs
        let leases = store.acquire_jobs("crashed-sched", 10, now).await.unwrap();
        assert_eq!(leases.len(), 2);

        // Another scheduler cannot acquire them
        let leases2 = store.acquire_jobs("new-sched", 10, now).await.unwrap();
        assert_eq!(leases2.len(), 0);

        // After lease expiry, clean up stale leases
        let after_expiry = now + chrono::Duration::seconds(DEFAULT_LEASE_DURATION_SECS + 10);
        let cleaned = store.cleanup_stale_leases(after_expiry).await.unwrap();
        assert_eq!(cleaned, 2);

        // Now the new scheduler can acquire the jobs
        let leases3 = store
            .acquire_jobs("new-sched", 10, after_expiry)
            .await
            .unwrap();
        assert_eq!(leases3.len(), 2);
    }
}
