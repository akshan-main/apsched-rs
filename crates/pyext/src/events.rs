use pyo3::prelude::*;

use apsched_core::event::{self, SchedulerEvent};

/// Construct the appropriate Python event object for a given core
/// `SchedulerEvent`. Returns `JobExecutionEvent` for execution-related events,
/// `JobEvent` for job lifecycle events, and `SchedulerEvent` otherwise — so
/// listener callbacks can read fields like `.job_id`, `.exception`, etc.
pub fn build_python_event(py: Python<'_>, event: &SchedulerEvent) -> PyResult<PyObject> {
    let code = event.event_mask();
    use SchedulerEvent::*;
    match event {
        JobExecuted {
            schedule_id,
            jobstore,
            scheduled_run_time,
            ..
        } => {
            let dt_obj = crate::convert::datetime_to_py(py, *scheduled_run_time)?;
            let ev = PyJobExecutionEvent {
                code,
                alias: None,
                job_id: schedule_id.clone(),
                jobstore: jobstore.clone(),
                scheduled_run_time: dt_obj,
                retval: None,
                exception: None,
                traceback: None,
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        JobError {
            schedule_id,
            jobstore,
            scheduled_run_time,
            exception,
            ..
        } => {
            let dt_obj = crate::convert::datetime_to_py(py, *scheduled_run_time)?;
            let exc_str = pyo3::types::PyString::new(py, exception)
                .into_any()
                .unbind();
            let ev = PyJobExecutionEvent {
                code,
                alias: None,
                job_id: schedule_id.clone(),
                jobstore: jobstore.clone(),
                scheduled_run_time: dt_obj,
                retval: None,
                exception: Some(exc_str),
                traceback: Some(exception.clone()),
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        JobMissed {
            job_id,
            jobstore,
            scheduled_run_time,
        } => {
            let dt_obj = crate::convert::datetime_to_py(py, *scheduled_run_time)?;
            let ev = PyJobExecutionEvent {
                code,
                alias: None,
                job_id: job_id.clone(),
                jobstore: jobstore.clone(),
                scheduled_run_time: dt_obj,
                retval: None,
                exception: None,
                traceback: None,
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        JobSubmitted {
            schedule_id,
            jobstore,
            scheduled_run_time,
            ..
        } => {
            let dt_obj = crate::convert::datetime_to_py(py, *scheduled_run_time)?;
            let ev = PyJobExecutionEvent {
                code,
                alias: None,
                job_id: schedule_id.clone(),
                jobstore: jobstore.clone(),
                scheduled_run_time: dt_obj,
                retval: None,
                exception: None,
                traceback: None,
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        JobAdded { job_id, jobstore }
        | JobRemoved { job_id, jobstore }
        | JobModified { job_id, jobstore }
        | JobMaxInstances { job_id, jobstore } => {
            let ev = PyJobEvent {
                code,
                alias: None,
                job_id: job_id.clone(),
                jobstore: jobstore.clone(),
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        ExecutorAdded { alias }
        | ExecutorRemoved { alias }
        | JobStoreAdded { alias }
        | JobStoreRemoved { alias } => {
            let ev = PySchedulerEvent {
                code,
                alias: Some(alias.clone()),
            };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
        _ => {
            let ev = PySchedulerEvent { code, alias: None };
            Ok(ev.into_pyobject(py)?.into_any().unbind())
        }
    }
}

// ---------------------------------------------------------------------------
// Event classes
// ---------------------------------------------------------------------------

#[pyclass(name = "SchedulerEvent")]
#[derive(Clone)]
pub struct PySchedulerEvent {
    #[pyo3(get)]
    pub code: u32,
    #[pyo3(get)]
    pub alias: Option<String>,
}

#[pymethods]
impl PySchedulerEvent {
    #[new]
    #[pyo3(signature = (code, alias=None))]
    pub fn new(code: u32, alias: Option<String>) -> Self {
        Self { code, alias }
    }

    fn __repr__(&self) -> String {
        format!("SchedulerEvent(code={})", self.code)
    }
}

#[pyclass(name = "JobEvent")]
pub struct PyJobEvent {
    #[pyo3(get)]
    pub code: u32,
    #[pyo3(get)]
    pub alias: Option<String>,
    #[pyo3(get)]
    pub job_id: String,
    #[pyo3(get)]
    pub jobstore: String,
}

#[pymethods]
impl PyJobEvent {
    #[new]
    #[pyo3(signature = (code, job_id, jobstore, alias=None))]
    fn new(code: u32, job_id: String, jobstore: String, alias: Option<String>) -> Self {
        Self {
            code,
            alias,
            job_id,
            jobstore,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "JobEvent(code={}, job_id='{}', jobstore='{}')",
            self.code, self.job_id, self.jobstore
        )
    }
}

#[pyclass(name = "JobExecutionEvent")]
pub struct PyJobExecutionEvent {
    #[pyo3(get)]
    pub code: u32,
    #[pyo3(get)]
    pub alias: Option<String>,
    #[pyo3(get)]
    pub job_id: String,
    #[pyo3(get)]
    pub jobstore: String,
    #[pyo3(get)]
    pub scheduled_run_time: PyObject,
    #[pyo3(get)]
    pub retval: Option<PyObject>,
    #[pyo3(get)]
    pub exception: Option<PyObject>,
    #[pyo3(get)]
    pub traceback: Option<String>,
}

#[pymethods]
impl PyJobExecutionEvent {
    #[new]
    #[pyo3(signature = (code, job_id, jobstore, scheduled_run_time, retval=None, exception=None, traceback=None, alias=None))]
    fn new(
        code: u32,
        job_id: String,
        jobstore: String,
        scheduled_run_time: PyObject,
        retval: Option<PyObject>,
        exception: Option<PyObject>,
        traceback: Option<String>,
        alias: Option<String>,
    ) -> Self {
        Self {
            code,
            alias,
            job_id,
            jobstore,
            scheduled_run_time,
            retval,
            exception,
            traceback,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "JobExecutionEvent(code={}, job_id='{}', jobstore='{}')",
            self.code, self.job_id, self.jobstore
        )
    }
}

// ---------------------------------------------------------------------------
// Register event constants and classes
// ---------------------------------------------------------------------------

pub fn register_events(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("EVENT_SCHEDULER_STARTED", event::EVENT_SCHEDULER_STARTED)?;
    m.add("EVENT_SCHEDULER_SHUTDOWN", event::EVENT_SCHEDULER_SHUTDOWN)?;
    m.add("EVENT_SCHEDULER_PAUSED", event::EVENT_SCHEDULER_PAUSED)?;
    m.add("EVENT_SCHEDULER_RESUMED", event::EVENT_SCHEDULER_RESUMED)?;
    m.add("EVENT_EXECUTOR_ADDED", event::EVENT_EXECUTOR_ADDED)?;
    m.add("EVENT_EXECUTOR_REMOVED", event::EVENT_EXECUTOR_REMOVED)?;
    m.add("EVENT_JOBSTORE_ADDED", event::EVENT_JOBSTORE_ADDED)?;
    m.add("EVENT_JOBSTORE_REMOVED", event::EVENT_JOBSTORE_REMOVED)?;
    m.add("EVENT_ALL_JOBS_REMOVED", event::EVENT_ALL_JOBS_REMOVED)?;
    m.add("EVENT_JOB_ADDED", event::EVENT_JOB_ADDED)?;
    m.add("EVENT_JOB_REMOVED", event::EVENT_JOB_REMOVED)?;
    m.add("EVENT_JOB_MODIFIED", event::EVENT_JOB_MODIFIED)?;
    m.add("EVENT_JOB_EXECUTED", event::EVENT_JOB_EXECUTED)?;
    m.add("EVENT_JOB_ERROR", event::EVENT_JOB_ERROR)?;
    m.add("EVENT_JOB_MISSED", event::EVENT_JOB_MISSED)?;
    m.add("EVENT_JOB_SUBMITTED", event::EVENT_JOB_SUBMITTED)?;
    m.add("EVENT_JOB_MAX_INSTANCES", event::EVENT_JOB_MAX_INSTANCES)?;
    m.add("EVENT_ALL", event::EVENT_ALL)?;

    m.add_class::<PySchedulerEvent>()?;
    m.add_class::<PyJobEvent>()?;
    m.add_class::<PyJobExecutionEvent>()?;

    Ok(())
}
