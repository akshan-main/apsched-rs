use std::sync::Arc;

use pyo3::prelude::*;

use apsched_store::MemoryJobStore;

#[pyclass(name = "MemoryJobStore")]
pub struct PyMemoryJobStore {
    pub(crate) inner: Arc<MemoryJobStore>,
}

#[pymethods]
impl PyMemoryJobStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(MemoryJobStore::new()),
        }
    }

    fn __repr__(&self) -> String {
        "MemoryJobStore()".to_string()
    }

    fn __str__(&self) -> String {
        "MemoryJobStore()".to_string()
    }
}

/// Configuration for a SQL job store that will be created lazily.
/// The actual SqlJobStore must be created within the scheduler's tokio runtime
/// to avoid runtime-binding issues with sqlx connection pools.
///
/// Supports both SQLite and PostgreSQL URLs. The backend is auto-detected
/// from the URL prefix (`sqlite:`, `postgres:`, `postgresql:`).
#[pyclass(name = "SqlJobStore")]
pub struct PySqlJobStore {
    pub(crate) url: String,
    pub(crate) tablename: String,
}

#[pymethods]
impl PySqlJobStore {
    #[new]
    #[pyo3(signature = (url, tablename=None))]
    fn new(url: &str, tablename: Option<&str>) -> PyResult<Self> {
        let table = tablename.unwrap_or("apscheduler_jobs");
        Ok(Self {
            url: url.to_string(),
            tablename: table.to_string(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "SqlJobStore(url={:?}, tablename={:?})",
            self.url, self.tablename
        )
    }

    fn __str__(&self) -> String {
        format!(
            "SqlJobStore(url={:?}, tablename={:?})",
            self.url, self.tablename
        )
    }
}

/// Configuration for a Redis job store that will be created lazily.
/// The actual RedisJobStore must be created within the scheduler's tokio runtime.
#[pyclass(name = "RedisJobStore")]
pub struct PyRedisJobStore {
    pub(crate) url: String,
    pub(crate) prefix: String,
}

#[pymethods]
impl PyRedisJobStore {
    #[new]
    #[pyo3(signature = (url, prefix=None))]
    fn new(url: &str, prefix: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            url: url.to_string(),
            prefix: prefix.unwrap_or("apscheduler:").to_string(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "RedisJobStore(url={:?}, prefix={:?})",
            self.url, self.prefix
        )
    }

    fn __str__(&self) -> String {
        format!(
            "RedisJobStore(url={:?}, prefix={:?})",
            self.url, self.prefix
        )
    }
}
