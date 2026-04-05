use pyo3::prelude::*;

#[pyclass(name = "ThreadPoolExecutor")]
pub struct PyThreadPoolExecutor {
    pub(crate) max_workers: usize,
}

#[pymethods]
impl PyThreadPoolExecutor {
    #[new]
    #[pyo3(signature = (max_workers=10))]
    fn new(max_workers: usize) -> Self {
        Self { max_workers }
    }

    fn __repr__(&self) -> String {
        format!("ThreadPoolExecutor(max_workers={})", self.max_workers)
    }

    fn __str__(&self) -> String {
        format!("ThreadPoolExecutor(max_workers={})", self.max_workers)
    }
}

#[pyclass(name = "ProcessPoolExecutor")]
pub struct PyProcessPoolExecutor {
    pub(crate) max_workers: usize,
}

#[pymethods]
impl PyProcessPoolExecutor {
    #[new]
    #[pyo3(signature = (max_workers=5))]
    fn new(max_workers: usize) -> Self {
        Self { max_workers }
    }

    fn __repr__(&self) -> String {
        format!("ProcessPoolExecutor(max_workers={})", self.max_workers)
    }

    fn __str__(&self) -> String {
        format!("ProcessPoolExecutor(max_workers={})", self.max_workers)
    }
}
