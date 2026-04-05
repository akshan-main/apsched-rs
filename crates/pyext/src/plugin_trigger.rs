use chrono::{DateTime, Utc};
use pyo3::prelude::*;

use apsched_core::model::TriggerState;
use apsched_core::traits::Trigger;

use crate::convert::{datetime_to_py, py_to_datetime};

/// A trigger whose scheduling logic is implemented by a Python object.
///
/// The Python object must have a `get_next_fire_time(previous_fire_time, now)`
/// method that returns a `datetime` (or `None` to signal exhaustion).
pub struct PythonPluginTrigger {
    /// The Python trigger object.  Stored as a `PyObject` so it can live
    /// outside the GIL.
    py_trigger: PyObject,
    /// Cached class name for `describe()` / `Debug`.
    class_name: String,
}

// Safety: PyObject is Send+Sync when we only touch it while holding the GIL.
unsafe impl Send for PythonPluginTrigger {}
unsafe impl Sync for PythonPluginTrigger {}

impl PythonPluginTrigger {
    /// Create a new plugin trigger wrapping a Python object.
    ///
    /// The caller must ensure that `py_trigger` has a `get_next_fire_time`
    /// method.
    pub fn new(py: Python<'_>, py_trigger: &Bound<'_, PyAny>) -> PyResult<Self> {
        let class_name = py_trigger
            .getattr("__class__")?
            .getattr("__name__")?
            .extract::<String>()?;
        Ok(Self {
            py_trigger: py_trigger.clone().unbind(),
            class_name,
        })
    }
}

impl std::fmt::Debug for PythonPluginTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PythonPluginTrigger({})", self.class_name)
    }
}

impl Trigger for PythonPluginTrigger {
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        Python::with_gil(|py| {
            let prev_py: PyObject = match previous_fire_time {
                Some(dt) => datetime_to_py(py, dt).ok()?,
                None => py.None(),
            };
            let now_py = datetime_to_py(py, now).ok()?;

            let result = self
                .py_trigger
                .bind(py)
                .call_method1("get_next_fire_time", (prev_py, now_py))
                .ok()?;

            if result.is_none() {
                return None;
            }

            py_to_datetime(&result).ok()
        })
    }

    fn describe(&self) -> String {
        format!("plugin[{}]", self.class_name)
    }

    fn serialize_state(&self) -> TriggerState {
        TriggerState::Plugin {
            description: self.describe(),
        }
    }

    fn trigger_type(&self) -> &'static str {
        "plugin"
    }
}
