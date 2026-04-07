//! Python-facing job context object.
//!
//! Exposes per-job memory, DAG output passing, artifact directories, and a
//! per-run log via the `JobContext` Python class. Created on demand by the
//! Python-aware executor when a job declares `wants_context=True` (or auto-
//! detected via signature inspection).

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use apsched_core::SchedulerEngine;

/// Convert a Python value to `serde_json::Value` via `json.dumps(..., default=str)`,
/// so that non-JSON-native types (datetime, bytes, custom objects, ...) round-trip
/// as best-effort strings rather than crashing.
pub(crate) fn py_to_json(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    let json_mod = py.import("json")?;
    let builtins = py.import("builtins")?;
    let str_type = builtins.getattr("str")?;
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("default", str_type)?;
    let dumped = json_mod
        .call_method("dumps", (value,), Some(&kwargs))?
        .extract::<String>()?;
    serde_json::from_str(&dumped)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("json encode error: {}", e)))
}

/// Convert a `serde_json::Value` back into a Python object via `json.loads`.
pub(crate) fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<PyObject> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let obj = py.import("json")?.call_method1("loads", (json_str,))?;
    Ok(obj.unbind())
}

#[pyclass(name = "JobContext", module = "apscheduler._rust")]
pub struct PyJobContext {
    #[pyo3(get)]
    pub job_id: String,
    #[pyo3(get)]
    pub scheduled_time: PyObject,
    #[pyo3(get)]
    pub attempt: u32,
    #[pyo3(get)]
    pub run_id: String,
    pub(crate) engine: Arc<SchedulerEngine>,
}

#[pymethods]
impl PyJobContext {
    fn set(&self, py: Python<'_>, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let json_val = py_to_json(py, value)?;
        self.engine.set_job_state(&self.job_id, key, json_val);
        Ok(())
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        match self.engine.get_job_state(&self.job_id, key) {
            Some(val) => json_to_py(py, &val),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn set_output(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let json_val = py_to_json(py, value)?;
        self.engine.set_job_output(&self.job_id, json_val);
        Ok(())
    }

    #[pyo3(signature = (job_id, key=None))]
    fn upstream(&self, py: Python<'_>, job_id: &str, key: Option<&str>) -> PyResult<PyObject> {
        let output = self.engine.get_job_output(job_id);
        match output {
            None => Ok(py.None()),
            Some(val) => {
                let target = if let Some(k) = key {
                    val.get(k).cloned().unwrap_or(serde_json::Value::Null)
                } else {
                    val
                };
                json_to_py(py, &target)
            }
        }
    }

    #[getter]
    fn artifact_dir(&self, py: Python<'_>) -> PyResult<PyObject> {
        let path = self.engine.artifact_dir_for(&self.job_id);
        let pathlib = py.import("pathlib")?;
        let path_obj = pathlib.call_method1("Path", (path.to_string_lossy().to_string(),))?;
        Ok(path_obj.unbind())
    }

    fn log(&self, msg: &str) -> PyResult<()> {
        self.engine.append_job_log(&self.job_id, &self.run_id, msg);
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "<JobContext job_id='{}' attempt={}>",
            self.job_id, self.attempt
        )
    }
}
