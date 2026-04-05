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
