use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use apsched_core::error::ExecutorError;
use apsched_core::event::{SchedulerEvent, EVENT_ALL};
use apsched_core::model::{
    CallableRef, CoalescePolicy, ScheduleSpec, SchedulerConfig, SerializedValue, TaskSpec,
    TriggerState,
};
use apsched_core::model::{JobOutcome, JobResultEnvelope, JobSpec};
use apsched_core::traits::{Executor, JobStore, Trigger};
use apsched_core::SchedulerEngine;
use apsched_store::MemoryJobStore;
use apsched_store::SqlJobStore;

use crate::convert::{
    datetime_to_py, deserialize_py_args, py_to_datetime, resolve_callable,
    scheduler_error_to_pyerr, serialize_py_args,
};
use crate::triggers::{PyCalendarIntervalTrigger, PyCronTrigger, PyDateTrigger, PyIntervalTrigger};

// ---------------------------------------------------------------------------
// Pending store: holds store config to be materialized at scheduler start
// ---------------------------------------------------------------------------

/// A store that has been configured but not yet bound to a tokio runtime.
/// SQL stores must be created within the scheduler's runtime to avoid
/// connection pool runtime-binding issues.
enum PendingStore {
    Memory(Arc<MemoryJobStore>),
    Sql { url: String, tablename: String },
}

impl PendingStore {
    /// Materialize the store within the given tokio runtime.
    fn into_store(self, rt: &tokio::runtime::Runtime) -> PyResult<Arc<dyn JobStore>> {
        match self {
            PendingStore::Memory(store) => Ok(store as Arc<dyn JobStore>),
            PendingStore::Sql { url, tablename } => {
                let store = rt
                    .block_on(SqlJobStore::new(&url, Some(&tablename)))
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to create SQL store: {}", e))
                    })?;
                Ok(Arc::new(store) as Arc<dyn JobStore>)
            }
        }
    }
}

/// Parse a jobstore argument from Python into a PendingStore.
///
/// Supports:
/// - `PyMemoryJobStore` instances
/// - `PySqlJobStore` instances (url/tablename config)
/// - String aliases: `"memory"`, `"sqlite"`, `"sqlalchemy"` (the latter two
///   require a `url` kwarg)
fn parse_jobstore(
    jobstore: &Bound<'_, PyAny>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<PendingStore> {
    // Try PyMemoryJobStore
    if let Ok(py_store) = jobstore.extract::<PyRef<'_, crate::stores::PyMemoryJobStore>>() {
        return Ok(PendingStore::Memory(Arc::clone(&py_store.inner)));
    }
    // Try PySqlJobStore (holds url/tablename config)
    if let Ok(py_store) = jobstore.extract::<PyRef<'_, crate::stores::PySqlJobStore>>() {
        return Ok(PendingStore::Sql {
            url: py_store.url.clone(),
            tablename: py_store.tablename.clone(),
        });
    }
    // Try string alias
    if let Ok(alias_str) = jobstore.extract::<String>() {
        match alias_str.as_str() {
            "memory" => {
                return Ok(PendingStore::Memory(Arc::new(MemoryJobStore::new())));
            }
            "sqlite" | "sqlalchemy" => {
                let url = kwargs
                    .and_then(|kw| kw.get_item("url").ok().flatten())
                    .and_then(|v| v.extract::<String>().ok())
                    .ok_or_else(|| {
                        PyValueError::new_err(
                            "A 'url' keyword argument is required for sqlite/sqlalchemy stores",
                        )
                    })?;
                let tablename = kwargs
                    .and_then(|kw| kw.get_item("tablename").ok().flatten())
                    .and_then(|v| v.extract::<String>().ok())
                    .unwrap_or_else(|| "apscheduler_jobs".to_string());
                return Ok(PendingStore::Sql { url, tablename });
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown jobstore type: '{}'. Use 'memory', 'sqlite', or 'sqlalchemy'",
                    other
                )));
            }
        }
    }
    Err(PyTypeError::new_err(
        "jobstore must be a MemoryJobStore, SqlJobStore, or a string alias ('memory', 'sqlite', 'sqlalchemy')",
    ))
}

// ---------------------------------------------------------------------------
// Callable store: holds Python callable objects that can't be serialized
// ---------------------------------------------------------------------------

struct CallableStore {
    callables: RwLock<HashMap<u64, PyObject>>,
    next_id: AtomicU64,
}

impl CallableStore {
    fn new() -> Self {
        Self {
            callables: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    fn store(&self, obj: PyObject) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.callables.write().insert(id, obj);
        id
    }

    fn get(&self, id: u64) -> Option<PyObject> {
        self.callables.read().get(&id).map(|obj| obj.clone())
    }

    #[allow(dead_code)]
    fn remove(&self, id: u64) -> Option<PyObject> {
        self.callables.write().remove(&id)
    }
}

// ---------------------------------------------------------------------------
// Python-aware executor: actually calls Python callables via the GIL
// ---------------------------------------------------------------------------

struct PythonAwareExecutor {
    callable_store: Arc<CallableStore>,
    max_workers: usize,
    running_count: Arc<std::sync::atomic::AtomicU32>,
    started: Arc<std::sync::atomic::AtomicBool>,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Optional reference to the Python asyncio event loop.
    /// When set, coroutines returned by async callables are scheduled on this
    /// loop via `asyncio.run_coroutine_threadsafe`.
    event_loop: Option<PyObject>,
}

impl std::fmt::Debug for PythonAwareExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PythonAwareExecutor")
            .field("max_workers", &self.max_workers)
            .finish()
    }
}

impl PythonAwareExecutor {
    fn new(callable_store: Arc<CallableStore>, max_workers: usize) -> Self {
        Self {
            callable_store,
            max_workers,
            running_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shutdown_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            event_loop: None,
        }
    }

    fn with_event_loop(mut self, loop_obj: PyObject) -> Self {
        self.event_loop = Some(loop_obj);
        self
    }
}

#[async_trait::async_trait]
impl Executor for PythonAwareExecutor {
    async fn start(&self) -> Result<(), ExecutorError> {
        self.started
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn shutdown(&self, _wait: bool) -> Result<(), ExecutorError> {
        self.shutdown_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.started
            .store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn submit_job(
        &self,
        job: JobSpec,
        result_tx: tokio::sync::mpsc::Sender<JobResultEnvelope>,
    ) -> Result<(), ExecutorError> {
        if self
            .shutdown_flag
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(ExecutorError::ShutdownInProgress);
        }
        if !self.started.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(ExecutorError::NotStarted);
        }

        let current = self
            .running_count
            .load(std::sync::atomic::Ordering::Relaxed) as usize;
        if current >= self.max_workers {
            return Err(ExecutorError::PoolExhausted);
        }

        self.running_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let running_count = Arc::clone(&self.running_count);
        let callable_store = Arc::clone(&self.callable_store);
        // Wrap the event loop in Arc so we can move it into the spawned task
        // without needing to clone the PyObject (which requires the GIL).
        let event_loop: Option<Arc<PyObject>> = self
            .event_loop
            .as_ref()
            .map(|l| Python::with_gil(|py| Arc::new(l.clone_ref(py))));
        let job_id = job.id;
        let schedule_id = job.schedule_id.clone();
        let task = job.task.clone();

        tokio::spawn(async move {
            let outcome = Python::with_gil(|py| {
                let loop_ref = event_loop.as_ref().map(|arc| arc.as_ref());
                match run_python_callable(
                    py,
                    &task.callable_ref,
                    &callable_store,
                    &task.args,
                    &task.kwargs,
                    loop_ref,
                ) {
                    Ok(_) => JobOutcome::Success,
                    Err(e) => JobOutcome::Error(format!("{}", e)),
                }
            });

            let envelope = JobResultEnvelope {
                job_id,
                schedule_id,
                outcome,
                completed_at: chrono::Utc::now(),
            };

            let _ = result_tx.send(envelope).await;
            running_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }

    async fn running_job_count(&self) -> usize {
        self.running_count
            .load(std::sync::atomic::Ordering::Relaxed) as usize
    }

    fn executor_type(&self) -> &'static str {
        "python_threadpool"
    }
}

// ---------------------------------------------------------------------------
// Pending job (queued before scheduler starts)
// ---------------------------------------------------------------------------

struct PendingJob {
    spec: ScheduleSpec,
}

// ---------------------------------------------------------------------------
// PyJob wrapper
// ---------------------------------------------------------------------------

#[pyclass(name = "Job")]
pub struct PyJob {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    name: Option<String>,
    #[pyo3(get)]
    next_run_time: Option<PyObject>,
    #[pyo3(get)]
    trigger: Option<String>,
    #[pyo3(get)]
    executor: String,
    #[pyo3(get)]
    jobstore: String,
}

#[pymethods]
impl PyJob {
    fn __repr__(&self) -> String {
        let name = self.name.as_deref().unwrap_or(&self.id);
        match &self.next_run_time {
            Some(_) => format!("<Job (id={} name='{}')>", self.id, name),
            None => format!("<Job (id={} name='{}' paused)>", self.id, name),
        }
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    #[getter]
    fn pending(&self) -> bool {
        self.next_run_time.is_some()
    }
}

impl PyJob {
    fn from_spec(py: Python<'_>, spec: &ScheduleSpec) -> PyResult<Self> {
        let nrt = match spec.next_run_time {
            Some(dt) => Some(datetime_to_py(py, dt)?),
            None => None,
        };
        let trigger_desc = match &spec.trigger_state {
            TriggerState::Date { .. } => Some("date".to_string()),
            TriggerState::Interval { .. } => Some("interval".to_string()),
            TriggerState::Cron { .. } => Some("cron".to_string()),
            TriggerState::CalendarInterval { .. } => Some("calendarinterval".to_string()),
            TriggerState::Plugin { description } => Some(description.clone()),
        };
        Ok(Self {
            id: spec.id.clone(),
            name: spec.name.clone(),
            next_run_time: nrt,
            trigger: trigger_desc,
            executor: spec.executor.clone(),
            jobstore: spec.jobstore.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Parse a trigger from Python arguments
// ---------------------------------------------------------------------------

fn parse_trigger(
    py: Python<'_>,
    trigger: &Bound<'_, PyAny>,
    trigger_args: Option<&Bound<'_, PyDict>>,
) -> PyResult<Box<dyn Trigger>> {
    // If it's a string, create the appropriate trigger from trigger_args
    if let Ok(trigger_str) = trigger.extract::<String>() {
        let args = trigger_args;
        return match trigger_str.as_str() {
            "date" => {
                let run_date = args.and_then(|a| a.get_item("run_date").ok().flatten());
                let timezone = args.and_then(|a| {
                    a.get_item("timezone")
                        .ok()
                        .flatten()
                        .and_then(|v| v.extract::<String>().ok())
                });
                let tz = timezone.as_deref().unwrap_or("UTC");
                let dt = match run_date {
                    Some(ref obj) => py_to_datetime(obj)?,
                    None => Utc::now(),
                };
                let t = apsched_triggers::DateTrigger::new(dt, tz.to_string())
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok(Box::new(t))
            }
            "interval" => {
                let get_i64 = |key: &str, default: i64| -> i64 {
                    args.and_then(|a| {
                        a.get_item(key)
                            .ok()
                            .flatten()
                            .and_then(|v| v.extract::<i64>().ok())
                    })
                    .unwrap_or(default)
                };
                let weeks = get_i64("weeks", 0);
                let days = get_i64("days", 0);
                let hours = get_i64("hours", 0);
                let minutes = get_i64("minutes", 0);
                let seconds = get_i64("seconds", 0);
                let start_date = args
                    .and_then(|a| a.get_item("start_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let end_date = args
                    .and_then(|a| a.get_item("end_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let timezone = args
                    .and_then(|a| {
                        a.get_item("timezone")
                            .ok()
                            .flatten()
                            .and_then(|v| v.extract::<String>().ok())
                    })
                    .unwrap_or_else(|| "UTC".to_string());
                let jitter = args.and_then(|a| {
                    a.get_item("jitter")
                        .ok()
                        .flatten()
                        .and_then(|v| v.extract::<f64>().ok())
                });

                let t = apsched_triggers::IntervalTrigger::new(
                    weeks, days, hours, minutes, seconds, start_date, end_date, timezone, jitter,
                )
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok(Box::new(t))
            }
            "cron" => {
                let get_str = |key: &str| -> Option<String> {
                    args.and_then(|a| {
                        a.get_item(key).ok().flatten().map(|v| {
                            if let Ok(s) = v.extract::<String>() {
                                s
                            } else if let Ok(i) = v.extract::<i64>() {
                                i.to_string()
                            } else {
                                v.str().map(|s| s.to_string()).unwrap_or_default()
                            }
                        })
                    })
                };
                let year = get_str("year");
                let month = get_str("month");
                let day = get_str("day");
                let week = get_str("week");
                let day_of_week = get_str("day_of_week");
                let hour = get_str("hour");
                let minute = get_str("minute");
                let second = get_str("second");
                let start_date = args
                    .and_then(|a| a.get_item("start_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let end_date = args
                    .and_then(|a| a.get_item("end_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let timezone = get_str("timezone").unwrap_or_else(|| "UTC".to_string());
                let jitter = args.and_then(|a| {
                    a.get_item("jitter")
                        .ok()
                        .flatten()
                        .and_then(|v| v.extract::<f64>().ok())
                });

                let t = apsched_triggers::CronTrigger::new(
                    year.as_deref(),
                    month.as_deref(),
                    day.as_deref(),
                    week.as_deref(),
                    day_of_week.as_deref(),
                    hour.as_deref(),
                    minute.as_deref(),
                    second.as_deref(),
                    start_date,
                    end_date,
                    timezone,
                    jitter,
                )
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok(Box::new(t))
            }
            "calendarinterval" => {
                let get_i32 = |key: &str, default: i32| -> i32 {
                    args.and_then(|a| {
                        a.get_item(key)
                            .ok()
                            .flatten()
                            .and_then(|v| v.extract::<i32>().ok())
                    })
                    .unwrap_or(default)
                };
                let get_u32 = |key: &str, default: u32| -> u32 {
                    args.and_then(|a| {
                        a.get_item(key)
                            .ok()
                            .flatten()
                            .and_then(|v| v.extract::<u32>().ok())
                    })
                    .unwrap_or(default)
                };
                let years = get_i32("years", 0);
                let months = get_i32("months", 0);
                let weeks = get_i32("weeks", 0);
                let days = get_i32("days", 0);
                let hour = get_u32("hour", 0);
                let minute = get_u32("minute", 0);
                let second = get_u32("second", 0);
                let start_date = args
                    .and_then(|a| a.get_item("start_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let end_date = args
                    .and_then(|a| a.get_item("end_date").ok().flatten())
                    .map(|obj| py_to_datetime(&obj))
                    .transpose()?;
                let timezone = args
                    .and_then(|a| {
                        a.get_item("timezone")
                            .ok()
                            .flatten()
                            .and_then(|v| v.extract::<String>().ok())
                    })
                    .unwrap_or_else(|| "UTC".to_string());

                let t = apsched_triggers::CalendarIntervalTrigger::new(
                    years, months, weeks, days, hour, minute, second, start_date, end_date,
                    timezone,
                )
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
                Ok(Box::new(t))
            }
            other => Err(PyValueError::new_err(format!(
                "unknown trigger type: '{}'",
                other
            ))),
        };
    }

    // If it's one of our trigger objects, extract the inner
    if let Ok(t) = trigger.extract::<PyRef<'_, PyDateTrigger>>() {
        return Ok(Box::new(t.inner.clone()));
    }
    if let Ok(t) = trigger.extract::<PyRef<'_, PyIntervalTrigger>>() {
        return Ok(Box::new(t.inner.clone()));
    }
    if let Ok(t) = trigger.extract::<PyRef<'_, PyCronTrigger>>() {
        return Ok(Box::new(t.inner.clone()));
    }
    if let Ok(t) = trigger.extract::<PyRef<'_, PyCalendarIntervalTrigger>>() {
        return Ok(Box::new(t.inner.clone()));
    }

    // Check if the object is a Python plugin trigger with a get_next_fire_time method
    if trigger.hasattr("get_next_fire_time")? {
        let plugin = crate::plugin_trigger::PythonPluginTrigger::new(py, trigger)?;
        return Ok(Box::new(plugin));
    }

    Err(PyTypeError::new_err(
        "trigger must be a string name, a trigger object, or an object with a get_next_fire_time() method",
    ))
}

// ---------------------------------------------------------------------------
// Build a ScheduleSpec from Python arguments
// ---------------------------------------------------------------------------

fn build_schedule_spec(
    py: Python<'_>,
    func: &Bound<'_, PyAny>,
    trigger: Option<&Bound<'_, PyAny>>,
    trigger_args: Option<&Bound<'_, PyDict>>,
    kwargs: Option<&Bound<'_, PyDict>>,
    callable_store: &CallableStore,
    config: &SchedulerConfig,
) -> PyResult<ScheduleSpec> {
    // Resolve the callable
    let (mut callable_ref, maybe_obj) = resolve_callable(py, func)?;
    if let Some(obj) = maybe_obj {
        let handle_id = callable_store.store(obj);
        callable_ref = CallableRef::InMemoryHandle(handle_id);
    }

    // Build task spec with args/kwargs from the `args` and `kwargs` keys in kwargs
    let mut task = TaskSpec::new(callable_ref);

    if let Some(kw) = kwargs {
        if let Some(args_obj) = kw.get_item("args")?.map(|v| v.unbind()) {
            let args_bound = args_obj.bind(py);
            if let Ok(tup) = args_bound.downcast::<PyTuple>() {
                let (ser_args, _) = serialize_py_args(py, tup, None)?;
                task.args = ser_args;
            } else {
                // Try to convert iterable to tuple
                let tup = PyTuple::new(py, args_bound.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
                let (ser_args, _) = serialize_py_args(py, &tup, None)?;
                task.args = ser_args;
            }
        }
        if let Some(kwargs_obj) = kw.get_item("kwargs")?.map(|v| v.unbind()) {
            let kwargs_bound = kwargs_obj.bind(py);
            if let Ok(d) = kwargs_bound.downcast::<PyDict>() {
                let (_, ser_kwargs) = serialize_py_args(py, &PyTuple::empty(py), Some(d))?;
                task.kwargs = ser_kwargs;
            }
        }
    }

    // Parse trigger
    // APScheduler compatibility: when trigger is a string and trigger_args is
    // not provided, the trigger parameters (seconds, minutes, hour, etc.) are
    // passed as **kwargs alongside other schedule params like id/name.  Merge
    // kwargs into trigger_args so parse_trigger can find them.
    let effective_trigger_args: Option<Bound<'_, PyDict>> = if trigger_args.is_some() {
        trigger_args.cloned()
    } else {
        kwargs.cloned()
    };
    let rust_trigger: Box<dyn Trigger> = if let Some(trig) = trigger {
        parse_trigger(py, trig, effective_trigger_args.as_ref())?
    } else {
        // Default: date trigger (run now)
        let dt = Utc::now();
        Box::new(
            apsched_triggers::DateTrigger::new(dt, config.timezone.clone())
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        )
    };

    let trigger_state = rust_trigger.serialize_state();
    let now = Utc::now();
    let next_run_time = rust_trigger.get_next_fire_time(None, now);

    // Extract optional schedule parameters from kwargs
    let job_id = kwargs
        .and_then(|kw| {
            kw.get_item("id")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<String>().ok())
        })
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let name = kwargs.and_then(|kw| {
        kw.get_item("name")
            .ok()
            .flatten()
            .and_then(|v| v.extract::<String>().ok())
    });

    let executor = kwargs
        .and_then(|kw| {
            kw.get_item("executor")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<String>().ok())
        })
        .unwrap_or_else(|| "default".to_string());

    let jobstore = kwargs
        .and_then(|kw| {
            kw.get_item("jobstore")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<String>().ok())
        })
        .unwrap_or_else(|| "default".to_string());

    let max_instances = kwargs
        .and_then(|kw| {
            kw.get_item("max_instances")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<u32>().ok())
        })
        .unwrap_or(config.job_defaults.max_instances);

    let replace_existing = kwargs
        .and_then(|kw| {
            kw.get_item("replace_existing")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<bool>().ok())
        })
        .unwrap_or(false);

    let misfire_grace_time = kwargs.and_then(|kw| {
        kw.get_item("misfire_grace_time")
            .ok()
            .flatten()
            .and_then(|v| v.extract::<f64>().ok())
            .map(std::time::Duration::from_secs_f64)
    });

    let coalesce = kwargs
        .and_then(|kw| {
            kw.get_item("coalesce")
                .ok()
                .flatten()
                .and_then(|v| v.extract::<bool>().ok())
        })
        .map(|b| {
            if b {
                CoalescePolicy::On
            } else {
                CoalescePolicy::Off
            }
        })
        .unwrap_or(config.job_defaults.coalesce);

    let mut spec = ScheduleSpec::new(job_id, task, trigger_state);
    spec.name = name;
    spec.executor = executor;
    spec.jobstore = jobstore;
    spec.max_instances = max_instances;
    spec.replace_existing = replace_existing;
    spec.coalesce = coalesce;
    spec.next_run_time = next_run_time;
    if let Some(grace) = misfire_grace_time {
        spec.misfire_grace_time = Some(grace);
    }

    Ok(spec)
}

// ---------------------------------------------------------------------------
// Run a Python callable
// ---------------------------------------------------------------------------

fn run_python_callable(
    py: Python<'_>,
    callable_ref: &CallableRef,
    callable_store: &CallableStore,
    args: &[SerializedValue],
    kwargs: &HashMap<String, SerializedValue>,
    event_loop: Option<&PyObject>,
) -> PyResult<PyObject> {
    // Resolve the callable
    let func: PyObject = match callable_ref {
        CallableRef::ImportPath(path) => {
            // path is "module:function"
            let parts: Vec<&str> = path.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(PyValueError::new_err(format!(
                    "invalid import path: '{}'",
                    path
                )));
            }
            let module = py.import(parts[0])?;
            module.getattr(parts[1])?.into()
        }
        CallableRef::InMemoryHandle(id) => callable_store
            .get(*id)
            .ok_or_else(|| PyRuntimeError::new_err(format!("callable handle {} not found", id)))?,
    };

    // Deserialize args/kwargs
    let (py_args, py_kwargs) = deserialize_py_args(py, args, kwargs)?;

    // Call the function
    let args_tuple = py_args.bind(py);
    let kwargs_dict = py_kwargs.bind(py);

    let args_tuple = args_tuple.downcast::<PyTuple>()?;
    let kwargs_dict = kwargs_dict.downcast::<PyDict>()?;

    let result = func.bind(py).call(args_tuple, Some(kwargs_dict))?;

    // If the result is a coroutine (async function), we need to await it.
    let inspect = py.import("inspect")?;
    let is_coro: bool = inspect.call_method1("iscoroutine", (&result,))?.extract()?;

    if is_coro {
        let asyncio = py.import("asyncio")?;

        if let Some(loop_obj) = event_loop {
            // We have a captured event loop (AsyncIOScheduler case).
            // Schedule the coroutine on that loop from this worker thread.
            let future =
                asyncio.call_method1("run_coroutine_threadsafe", (&result, loop_obj.bind(py)))?;
            // Block this worker thread until the coroutine completes.
            let awaited_result = future.call_method0("result")?;
            Ok(awaited_result.into())
        } else {
            // No event loop provided (BlockingScheduler / BackgroundScheduler).
            // Run the coroutine synchronously in a new event loop.
            let new_result = asyncio.call_method1("run", (&result,))?;
            Ok(new_result.into())
        }
    } else {
        Ok(result.into())
    }
}

// ---------------------------------------------------------------------------
// Shared introspection helpers
// ---------------------------------------------------------------------------

/// Shared implementation for `simulate_trigger`.
fn simulate_trigger_impl(
    py: Python<'_>,
    trigger: &Bound<'_, PyAny>,
    n: Option<usize>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<Vec<PyObject>> {
    let n = n.unwrap_or(10);
    let rust_trigger = parse_trigger(py, trigger, kwargs)?;
    let mut times = Vec::with_capacity(n);
    let mut prev: Option<chrono::DateTime<Utc>> = None;
    let mut now = Utc::now();
    for _ in 0..n {
        match rust_trigger.get_next_fire_time(prev, now) {
            Some(dt) => {
                times.push(datetime_to_py(py, dt)?);
                prev = Some(dt);
                // Advance now to just after the fire time so the trigger
                // progresses even for triggers that only look at `now`.
                now = dt + chrono::Duration::milliseconds(1);
            }
            None => break,
        }
    }
    Ok(times)
}

/// Helper to get or create a tokio runtime.
fn get_or_create_runtime(
    runtime: &Option<Arc<tokio::runtime::Runtime>>,
) -> PyResult<Arc<tokio::runtime::Runtime>> {
    if let Some(rt) = runtime {
        Ok(Arc::clone(rt))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
        Ok(Arc::new(rt))
    }
}

/// Shared implementation for `explain_job`.
fn explain_job_impl(
    py: Python<'_>,
    job_id: &str,
    jobstore: Option<&str>,
    engine: &Option<Arc<SchedulerEngine>>,
    runtime: &Option<Arc<tokio::runtime::Runtime>>,
) -> PyResult<PyObject> {
    if let Some(ref engine) = engine {
        let engine = Arc::clone(engine);
        let rt = get_or_create_runtime(runtime)?;
        let job_id_owned = job_id.to_string();
        let jobstore_owned = jobstore.map(|s| s.to_string());

        let spec = rt.block_on(async {
            engine
                .get_job(&job_id_owned, jobstore_owned.as_deref())
                .await
                .map_err(scheduler_error_to_pyerr)
        })?;

        let running_count = engine.running_instance_count(&spec.id);

        let dict = PyDict::new(py);
        dict.set_item("id", &spec.id)?;
        dict.set_item("name", spec.name.as_deref().unwrap_or(&spec.id))?;
        let trigger_desc = match &spec.trigger_state {
            TriggerState::Date { run_date, .. } => format!("date[{}]", run_date),
            TriggerState::Interval {
                weeks,
                days,
                hours,
                minutes,
                seconds,
                ..
            } => {
                let total =
                    weeks * 7 * 86400 + days * 86400 + hours * 3600 + minutes * 60 + seconds;
                format!("interval[{}s]", total)
            }
            TriggerState::Cron {
                hour,
                minute,
                second,
                ..
            } => {
                format!(
                    "cron[{}:{}:{}]",
                    hour.as_deref().unwrap_or("*"),
                    minute.as_deref().unwrap_or("*"),
                    second.as_deref().unwrap_or("*")
                )
            }
            TriggerState::CalendarInterval {
                years,
                months,
                weeks,
                days,
                ..
            } => {
                format!("calendarinterval[{}y{}m{}w{}d]", years, months, weeks, days)
            }
            TriggerState::Plugin { description } => description.clone(),
        };
        dict.set_item("trigger", trigger_desc)?;
        match spec.next_run_time {
            Some(dt) => dict.set_item("next_run_time", datetime_to_py(py, dt)?)?,
            None => dict.set_item("next_run_time", py.None())?,
        }
        dict.set_item("paused", spec.paused)?;
        dict.set_item("max_instances", spec.max_instances)?;
        dict.set_item("running_instances", running_count)?;
        dict.set_item("coalesce", matches!(spec.coalesce, CoalescePolicy::On))?;
        dict.set_item(
            "misfire_grace_time",
            spec.misfire_grace_time
                .map(|d| d.as_secs_f64())
                .unwrap_or(1.0),
        )?;
        dict.set_item("executor", &spec.executor)?;

        Ok(dict.into_any().unbind())
    } else {
        Err(PyRuntimeError::new_err("scheduler is not running"))
    }
}

/// Shared implementation for `get_scheduler_info`.
fn get_scheduler_info_impl(
    py: Python<'_>,
    engine: &Option<Arc<SchedulerEngine>>,
    runtime: &Option<Arc<tokio::runtime::Runtime>>,
    started: bool,
    start_time: Option<std::time::Instant>,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);

    if let Some(ref engine) = engine {
        let rt = get_or_create_runtime(runtime)?;
        let state = engine.state();
        let state_str = match state {
            apsched_core::model::SchedulerState::Stopped => "stopped",
            apsched_core::model::SchedulerState::Starting => "starting",
            apsched_core::model::SchedulerState::Running => "running",
            apsched_core::model::SchedulerState::Paused => "paused",
            apsched_core::model::SchedulerState::ShuttingDown => "shutting_down",
        };
        dict.set_item("state", state_str)?;
        dict.set_item("scheduler_id", engine.scheduler_id())?;

        let job_count = rt
            .block_on(async { engine.get_jobs(None).await })
            .map(|jobs| jobs.len())
            .unwrap_or(0);
        dict.set_item("job_count", job_count)?;

        let uptime = start_time.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
        dict.set_item("uptime_seconds", uptime)?;

        let stores: Vec<String> = engine.store_aliases();
        dict.set_item("stores", stores)?;

        let executors: Vec<String> = engine.executor_aliases();
        dict.set_item("executors", executors)?;
    } else {
        dict.set_item("state", "stopped")?;
        dict.set_item("scheduler_id", py.None())?;
        dict.set_item("job_count", 0)?;
        dict.set_item("uptime_seconds", 0.0)?;
        let empty: Vec<String> = Vec::new();
        dict.set_item("stores", empty.clone())?;
        dict.set_item("executors", empty)?;
    }

    Ok(dict.into_any().unbind())
}

// ---------------------------------------------------------------------------
// PyBlockingScheduler
// ---------------------------------------------------------------------------

#[pyclass(name = "BlockingScheduler")]
pub struct PyBlockingScheduler {
    engine: Option<Arc<SchedulerEngine>>,
    callable_store: Arc<CallableStore>,
    config: SchedulerConfig,
    pending_jobs: Vec<PendingJob>,
    stores: HashMap<String, PyObject>,
    pending_stores: HashMap<String, PendingStore>,
    executors: HashMap<String, PyObject>,
    listeners: Vec<(PyObject, u32)>,
    started: bool,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    start_time: Option<std::time::Instant>,
}

#[pymethods]
impl PyBlockingScheduler {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut config = SchedulerConfig::default();

        if let Some(kw) = kwargs {
            if let Some(tz) = kw
                .get_item("timezone")?
                .and_then(|v| v.extract::<String>().ok())
            {
                config.timezone = tz;
            }
            if let Some(daemon) = kw
                .get_item("daemon")?
                .and_then(|v| v.extract::<bool>().ok())
            {
                config.daemon = daemon;
            }
            // Parse job_defaults
            if let Some(jd) = kw.get_item("job_defaults")? {
                if let Ok(d) = jd.downcast::<PyDict>() {
                    if let Some(grace) = d
                        .get_item("misfire_grace_time")?
                        .and_then(|v| v.extract::<f64>().ok())
                    {
                        config.job_defaults.misfire_grace_time =
                            std::time::Duration::from_secs_f64(grace);
                    }
                    if let Some(coalesce) = d
                        .get_item("coalesce")?
                        .and_then(|v| v.extract::<bool>().ok())
                    {
                        config.job_defaults.coalesce = if coalesce {
                            CoalescePolicy::On
                        } else {
                            CoalescePolicy::Off
                        };
                    }
                    if let Some(max) = d
                        .get_item("max_instances")?
                        .and_then(|v| v.extract::<u32>().ok())
                    {
                        config.job_defaults.max_instances = max;
                    }
                }
            }
        }

        Ok(Self {
            engine: None,
            callable_store: Arc::new(CallableStore::new()),
            config,
            pending_jobs: Vec::new(),
            stores: HashMap::new(),
            pending_stores: HashMap::new(),
            executors: HashMap::new(),
            listeners: Vec::new(),
            started: false,
            shutdown_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            start_time: None,
        })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.started {
            return Err(PyRuntimeError::new_err("scheduler is already running"));
        }

        let callable_store = Arc::clone(&self.callable_store);
        let config = self.config.clone();
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        shutdown_flag.store(false, Ordering::Relaxed);
        self.start_time = Some(std::time::Instant::now());

        // Build a tokio runtime and start the engine
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;

        let engine = Arc::new(SchedulerEngine::new(
            config,
            Arc::new(apsched_core::WallClock),
        ));

        // Add default store if none configured
        let default_store = Arc::new(MemoryJobStore::new());
        engine
            .add_jobstore(default_store, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Add default executor (Python-aware so it can invoke Python callables)
        let default_executor = Arc::new(PythonAwareExecutor::new(Arc::clone(&callable_store), 10));
        engine
            .add_executor(default_executor, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Add user-configured stores (pending stores, materialized with this runtime)
        for (alias, pending) in self.pending_stores.drain() {
            let store = pending.into_store(&rt)?;
            engine
                .add_jobstore(store, &alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        // Also try stores registered as Python objects
        for (alias, store_obj) in &self.stores {
            let store_obj_bound = store_obj.bind(py);
            if let Ok(pending) = parse_jobstore(&store_obj_bound, None) {
                let store = pending.into_store(&rt)?;
                engine
                    .add_jobstore(store, alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        // Add user-configured executors
        for (alias, exec_obj) in &self.executors {
            let exec_obj_bound = exec_obj.bind(py);
            if let Ok(py_exec) =
                exec_obj_bound.extract::<PyRef<'_, crate::executors::PyThreadPoolExecutor>>()
            {
                let executor = Arc::new(PythonAwareExecutor::new(
                    Arc::clone(&callable_store),
                    py_exec.max_workers,
                ));
                engine
                    .add_executor(executor, alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        // Register listeners
        for (callback, mask) in &self.listeners {
            let cb = callback.clone();
            let listener_mask = *mask;
            // We wrap the Python callback in a Rust closure that acquires the GIL
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                listener_mask,
            );
        }

        // Start the engine
        let engine_clone = Arc::clone(&engine);
        rt.block_on(async { engine_clone.start().await.map_err(scheduler_error_to_pyerr) })?;

        // Add pending jobs
        let pending: Vec<PendingJob> = self.pending_jobs.drain(..).collect();
        for pj in pending {
            let engine_ref = Arc::clone(&engine);
            rt.block_on(async {
                engine_ref
                    .add_job(pj.spec)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
        }

        self.engine = Some(Arc::clone(&engine));
        self.started = true;

        // Block the current thread, releasing the GIL
        py.allow_threads(move || {
            // The scheduler loop is running in the tokio runtime.
            // We just park here until shutdown is signaled.
            while !shutdown_flag.load(Ordering::Relaxed) {
                std::thread::park_timeout(std::time::Duration::from_millis(100));
            }

            // Shut down the engine
            rt.block_on(async {
                let _ = engine.shutdown(true).await;
            });
        });

        self.started = false;
        self.engine = None;
        Ok(())
    }

    #[pyo3(signature = (wait=None))]
    fn shutdown(&mut self, py: Python<'_>, wait: Option<bool>) -> PyResult<()> {
        let _wait = wait.unwrap_or(true);
        self.shutdown_flag.store(true, Ordering::Relaxed);

        // If we have an engine and it's running from a background context,
        // trigger the shutdown
        if let Some(ref engine) = self.engine {
            // Engine shutdown is handled by the blocking loop detecting the flag
            // For safety, also signal the engine's internal wakeup
            engine.wakeup();
        }
        Ok(())
    }

    #[pyo3(signature = (func, trigger=None, trigger_args=None, **kwargs))]
    fn add_job(
        &mut self,
        py: Python<'_>,
        func: &Bound<'_, PyAny>,
        trigger: Option<&Bound<'_, PyAny>>,
        trigger_args: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let spec = build_schedule_spec(
            py,
            func,
            trigger,
            trigger_args,
            kwargs,
            &self.callable_store,
            &self.config,
        )?;

        let py_job = PyJob::from_spec(py, &spec)?;

        if self.started {
            if let Some(ref engine) = self.engine {
                // We need a tokio runtime to call async methods
                // Use tokio's Handle to block on the async call
                let engine = Arc::clone(engine);
                let spec_clone = spec;
                // Use a temporary runtime for this call
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("failed to create runtime: {}", e))
                    })?;
                rt.block_on(async {
                    engine
                        .add_job(spec_clone)
                        .await
                        .map_err(scheduler_error_to_pyerr)
                })?;
            }
        } else {
            self.pending_jobs.push(PendingJob { spec });
        }

        Ok(py_job.into_pyobject(py)?.into_any().unbind())
    }

    #[pyo3(signature = (trigger, trigger_args=None, **kwargs))]
    fn scheduled_job<'py>(
        &self,
        py: Python<'py>,
        trigger: &Bound<'py, PyAny>,
        trigger_args: Option<&Bound<'py, PyDict>>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        // The scheduled_job decorator requires holding a reference to `self`
        // across a Python closure boundary, which is not straightforward from
        // the Rust side. This is best handled in the Python wrapper layer.
        Err(PyRuntimeError::new_err(
            "scheduled_job() decorator is not yet supported from Rust extension. \
             Use add_job() directly instead.",
        ))
    }

    #[pyo3(signature = (job_id, jobstore=None, **kwargs))]
    fn modify_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        if let Some(ref engine) = self.engine {
            let mut changes = apsched_core::model::JobChanges::default();
            if let Some(kw) = kwargs {
                if let Some(name) = kw
                    .get_item("name")?
                    .and_then(|v| v.extract::<String>().ok())
                {
                    changes.name = Some(name);
                }
                if let Some(max) = kw
                    .get_item("max_instances")?
                    .and_then(|v| v.extract::<u32>().ok())
                {
                    changes.max_instances = Some(max);
                }
            }

            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            let spec = rt.block_on(async {
                engine
                    .modify_job(&job_id_owned, jobstore_owned.as_deref(), changes)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None, trigger=None, **kwargs))]
    fn reschedule_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
        trigger: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        if let Some(ref engine) = self.engine {
            // First, if a trigger is given, compute new trigger_state and next_run_time
            let mut changes = apsched_core::model::JobChanges::default();

            if let Some(trig) = trigger {
                let rust_trigger = parse_trigger(py, trig, None)?;
                let now = Utc::now();
                let nrt = rust_trigger.get_next_fire_time(None, now);
                changes.trigger_state = Some(rust_trigger.serialize_state());
                changes.next_run_time = Some(nrt);
            }

            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            let spec = rt.block_on(async {
                engine
                    .modify_job(&job_id_owned, jobstore_owned.as_deref(), changes)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn remove_job(&mut self, py: Python<'_>, job_id: &str, jobstore: Option<&str>) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            rt.block_on(async {
                engine
                    .remove_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })
        } else {
            // Remove from pending
            let initial_len = self.pending_jobs.len();
            self.pending_jobs.retain(|pj| pj.spec.id != job_id);
            if self.pending_jobs.len() == initial_len {
                Err(pyo3::exceptions::PyKeyError::new_err(format!(
                    "job '{}' not found",
                    job_id
                )))
            } else {
                Ok(())
            }
        }
    }

    #[pyo3(signature = (jobstore=None))]
    fn remove_all_jobs(&mut self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            // Use bulk remove_all_jobs instead of iterating one-by-one
            rt.block_on(async {
                engine
                    .remove_all_jobs(jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })
        } else {
            if let Some(_store) = jobstore {
                self.pending_jobs.retain(|pj| pj.spec.jobstore != _store);
            } else {
                self.pending_jobs.clear();
            }
            Ok(())
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn get_job(
        &self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<Option<PyObject>> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            match rt.block_on(async {
                engine
                    .get_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
            }) {
                Ok(spec) => {
                    let py_job = PyJob::from_spec(py, &spec)?;
                    Ok(Some(py_job.into_pyobject(py)?.into_any().unbind()))
                }
                Err(_) => Ok(None),
            }
        } else {
            // Search pending
            for pj in &self.pending_jobs {
                if pj.spec.id == job_id {
                    let py_job = PyJob::from_spec(py, &pj.spec)?;
                    return Ok(Some(py_job.into_pyobject(py)?.into_any().unbind()));
                }
            }
            Ok(None)
        }
    }

    #[pyo3(signature = (jobstore=None))]
    fn get_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<Vec<PyObject>> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            let specs = rt.block_on(async {
                engine
                    .get_jobs(jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let mut result = Vec::new();
            for spec in &specs {
                let py_job = PyJob::from_spec(py, spec)?;
                result.push(py_job.into_pyobject(py)?.into_any().unbind());
            }
            Ok(result)
        } else {
            let mut result = Vec::new();
            for pj in &self.pending_jobs {
                if jobstore.is_none() || Some(pj.spec.jobstore.as_str()) == jobstore {
                    let py_job = PyJob::from_spec(py, &pj.spec)?;
                    result.push(py_job.into_pyobject(py)?.into_any().unbind());
                }
            }
            Ok(result)
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn pause_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            let spec = rt.block_on(async {
                engine
                    .pause_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn resume_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        if let Some(ref engine) = self.engine {
            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?;
            let spec = rt.block_on(async {
                engine
                    .resume_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn pause(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.pause().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn resume(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.resume().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn wakeup(&self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.wakeup();
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (callback, mask=None))]
    fn add_listener(
        &mut self,
        py: Python<'_>,
        callback: PyObject,
        mask: Option<u32>,
    ) -> PyResult<()> {
        let mask = mask.unwrap_or(EVENT_ALL);
        self.listeners.push((callback.clone(), mask));

        // If already running, add to engine too
        if let Some(ref engine) = self.engine {
            let cb = callback;
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                mask,
            );
        }

        Ok(())
    }

    fn remove_listener(&mut self, py: Python<'_>, callback: PyObject) -> PyResult<()> {
        // Remove from our list (compare by identity)
        let cb_ptr = callback.as_ptr();
        self.listeners.retain(|(cb, _)| cb.as_ptr() != cb_ptr);
        Ok(())
    }

    #[pyo3(signature = (jobstore, alias=None, **kwargs))]
    fn add_jobstore(
        &mut self,
        py: Python<'_>,
        jobstore: &Bound<'_, PyAny>,
        alias: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let alias = alias.unwrap_or("default").to_string();
        let pending = parse_jobstore(jobstore, kwargs)?;

        if let Some(ref engine) = self.engine {
            // Engine is running; we need a runtime to materialize SQL stores.
            // Use a temporary runtime for immediate registration.
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;
            let store = pending.into_store(&rt)?;
            engine
                .add_jobstore(store, &alias)
                .map_err(scheduler_error_to_pyerr)?;
        } else {
            // Store for deferred creation at start time
            self.pending_stores.insert(alias.clone(), pending);
        }

        Ok(())
    }

    fn remove_jobstore(&mut self, alias: &str) -> PyResult<()> {
        self.stores.remove(alias);
        self.pending_stores.remove(alias);
        if let Some(ref engine) = self.engine {
            engine
                .remove_jobstore(alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        Ok(())
    }

    #[pyo3(signature = (executor, alias=None, **kwargs))]
    fn add_executor(
        &mut self,
        py: Python<'_>,
        executor: &Bound<'_, PyAny>,
        alias: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let alias = alias.unwrap_or("default").to_string();
        self.executors
            .insert(alias.clone(), executor.clone().unbind());

        if let Some(ref engine) = self.engine {
            if let Ok(py_exec) =
                executor.extract::<PyRef<'_, crate::executors::PyThreadPoolExecutor>>()
            {
                let exec = Arc::new(PythonAwareExecutor::new(
                    Arc::clone(&self.callable_store),
                    py_exec.max_workers,
                ));
                engine
                    .add_executor(exec, &alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        Ok(())
    }

    fn remove_executor(&mut self, alias: &str) -> PyResult<()> {
        self.executors.remove(alias);
        if let Some(ref engine) = self.engine {
            engine
                .remove_executor(alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        Ok(())
    }

    #[pyo3(signature = (jobstore=None))]
    fn print_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<()> {
        let jobs = self.get_jobs(py, jobstore)?;
        let builtins = py.import("builtins")?;

        if jobs.is_empty() {
            builtins.call_method1("print", ("No pending jobs",))?;
        } else {
            for job_obj in &jobs {
                let repr = job_obj.bind(py).repr()?;
                builtins.call_method1("print", (repr,))?;
            }
        }
        Ok(())
    }

    /// Simulate a trigger by computing the next N fire times without adding a job.
    #[pyo3(signature = (trigger, n=None, **kwargs))]
    fn simulate_trigger(
        &self,
        py: Python<'_>,
        trigger: &Bound<'_, PyAny>,
        n: Option<usize>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyObject>> {
        simulate_trigger_impl(py, trigger, n, kwargs)
    }

    /// Return a dict explaining the current state of a job.
    #[pyo3(signature = (job_id, jobstore=None))]
    fn explain_job(
        &self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        explain_job_impl(py, job_id, jobstore, &self.engine, &None)
    }

    /// Return a dict describing the scheduler state.
    fn get_scheduler_info(&self, py: Python<'_>) -> PyResult<PyObject> {
        get_scheduler_info_impl(py, &self.engine, &None, self.started, self.start_time)
    }

    fn __repr__(&self) -> String {
        if self.started {
            "BlockingScheduler(running=True)".to_string()
        } else {
            "BlockingScheduler(running=False)".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// PyBackgroundScheduler
// ---------------------------------------------------------------------------

#[pyclass(name = "BackgroundScheduler")]
pub struct PyBackgroundScheduler {
    engine: Option<Arc<SchedulerEngine>>,
    callable_store: Arc<CallableStore>,
    config: SchedulerConfig,
    pending_jobs: Vec<PendingJob>,
    stores: HashMap<String, PyObject>,
    pending_stores: HashMap<String, PendingStore>,
    executors: HashMap<String, PyObject>,
    listeners: Vec<(PyObject, u32)>,
    started: bool,
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    background_thread: Option<std::thread::JoinHandle<()>>,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    start_time: Option<std::time::Instant>,
    /// Cache of PyJob objects keyed by job ID, so get_jobs() returns cached
    /// objects without round-tripping through the Rust engine.
    jobs_cache: HashMap<String, PyObject>,
}

#[pymethods]
impl PyBackgroundScheduler {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut config = SchedulerConfig::default();

        if let Some(kw) = kwargs {
            if let Some(tz) = kw
                .get_item("timezone")?
                .and_then(|v| v.extract::<String>().ok())
            {
                config.timezone = tz;
            }
            if let Some(daemon) = kw
                .get_item("daemon")?
                .and_then(|v| v.extract::<bool>().ok())
            {
                config.daemon = daemon;
            }
            if let Some(jd) = kw.get_item("job_defaults")? {
                if let Ok(d) = jd.downcast::<PyDict>() {
                    if let Some(grace) = d
                        .get_item("misfire_grace_time")?
                        .and_then(|v| v.extract::<f64>().ok())
                    {
                        config.job_defaults.misfire_grace_time =
                            std::time::Duration::from_secs_f64(grace);
                    }
                    if let Some(coalesce) = d
                        .get_item("coalesce")?
                        .and_then(|v| v.extract::<bool>().ok())
                    {
                        config.job_defaults.coalesce = if coalesce {
                            CoalescePolicy::On
                        } else {
                            CoalescePolicy::Off
                        };
                    }
                    if let Some(max) = d
                        .get_item("max_instances")?
                        .and_then(|v| v.extract::<u32>().ok())
                    {
                        config.job_defaults.max_instances = max;
                    }
                }
            }
        }

        Ok(Self {
            engine: None,
            callable_store: Arc::new(CallableStore::new()),
            config,
            pending_jobs: Vec::new(),
            stores: HashMap::new(),
            pending_stores: HashMap::new(),
            executors: HashMap::new(),
            listeners: Vec::new(),
            started: false,
            shutdown_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            background_thread: None,
            runtime: None,
            start_time: None,
            jobs_cache: HashMap::new(),
        })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.started {
            return Err(PyRuntimeError::new_err("scheduler is already running"));
        }

        let config = self.config.clone();
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        shutdown_flag.store(false, Ordering::Relaxed);
        self.start_time = Some(std::time::Instant::now());

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?,
        );

        let engine = Arc::new(SchedulerEngine::new(
            config,
            Arc::new(apsched_core::WallClock),
        ));

        // Add default store
        let default_store = Arc::new(MemoryJobStore::new());
        engine
            .add_jobstore(default_store, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Add default executor (Python-aware so it can invoke Python callables)
        let callable_store = Arc::clone(&self.callable_store);
        let default_executor = Arc::new(PythonAwareExecutor::new(Arc::clone(&callable_store), 10));
        engine
            .add_executor(default_executor, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Add user stores (pending stores, materialized with this runtime)
        for (alias, pending) in self.pending_stores.drain() {
            let store = pending.into_store(&rt)?;
            engine
                .add_jobstore(store, &alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        // Also try stores registered as Python objects
        for (alias, store_obj) in &self.stores {
            let store_obj_bound = store_obj.bind(py);
            if let Ok(pending) = parse_jobstore(&store_obj_bound, None) {
                let store = pending.into_store(&rt)?;
                engine
                    .add_jobstore(store, alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        // Add user executors
        for (alias, exec_obj) in &self.executors {
            let exec_obj_bound = exec_obj.bind(py);
            if let Ok(py_exec) =
                exec_obj_bound.extract::<PyRef<'_, crate::executors::PyThreadPoolExecutor>>()
            {
                let executor = Arc::new(PythonAwareExecutor::new(
                    Arc::clone(&callable_store),
                    py_exec.max_workers,
                ));
                engine
                    .add_executor(executor, alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        // Register listeners
        for (callback, mask) in &self.listeners {
            let cb = callback.clone();
            let listener_mask = *mask;
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                listener_mask,
            );
        }

        // Start the engine
        let engine_clone = Arc::clone(&engine);
        let rt_clone = Arc::clone(&rt);
        rt.block_on(async { engine_clone.start().await.map_err(scheduler_error_to_pyerr) })?;

        // Add pending jobs
        let pending: Vec<PendingJob> = self.pending_jobs.drain(..).collect();
        for pj in pending {
            let engine_ref = Arc::clone(&engine);
            rt.block_on(async {
                engine_ref
                    .add_job(pj.spec)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
        }

        self.engine = Some(Arc::clone(&engine));
        self.runtime = Some(Arc::clone(&rt));
        self.started = true;

        // Spawn a background thread that keeps the runtime alive
        let engine_bg = Arc::clone(&engine);
        let shutdown_bg = Arc::clone(&shutdown_flag);
        let rt_bg = Arc::clone(&rt);
        let handle = std::thread::Builder::new()
            .name("apsched-background".to_string())
            .spawn(move || {
                while !shutdown_bg.load(Ordering::Relaxed) {
                    std::thread::park_timeout(std::time::Duration::from_millis(100));
                }
                rt_bg.block_on(async {
                    let _ = engine_bg.shutdown(true).await;
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("failed to spawn thread: {}", e)))?;

        self.background_thread = Some(handle);

        Ok(())
    }

    #[pyo3(signature = (wait=None))]
    fn shutdown(&mut self, py: Python<'_>, wait: Option<bool>) -> PyResult<()> {
        let wait = wait.unwrap_or(true);
        self.shutdown_flag.store(true, Ordering::Relaxed);

        if let Some(handle) = self.background_thread.take() {
            handle.thread().unpark();
            if wait {
                py.allow_threads(|| {
                    let _ = handle.join();
                });
            }
        }

        self.started = false;
        self.engine = None;
        self.runtime = None;
        Ok(())
    }

    #[pyo3(signature = (func, trigger=None, trigger_args=None, **kwargs))]
    fn add_job(
        &mut self,
        py: Python<'_>,
        func: &Bound<'_, PyAny>,
        trigger: Option<&Bound<'_, PyAny>>,
        trigger_args: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let spec = build_schedule_spec(
            py,
            func,
            trigger,
            trigger_args,
            kwargs,
            &self.callable_store,
            &self.config,
        )?;

        let job_id = spec.id.clone();
        let py_job = PyJob::from_spec(py, &spec)?;
        let py_job_obj = py_job.into_pyobject(py)?.into_any().unbind();

        // Cache the PyJob object and invalidate list cache
        self.jobs_cache.insert(job_id, py_job_obj.clone_ref(py));

        if self.started {
            if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
                let engine = Arc::clone(engine);
                let rt = Arc::clone(rt);
                let spec_clone = spec;
                rt.block_on(async {
                    engine
                        .add_job(spec_clone)
                        .await
                        .map_err(scheduler_error_to_pyerr)
                })?;
            }
        } else {
            self.pending_jobs.push(PendingJob { spec });
        }

        Ok(py_job_obj)
    }

    #[pyo3(signature = (trigger, trigger_args=None, **kwargs))]
    fn scheduled_job<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        trigger: &Bound<'py, PyAny>,
        trigger_args: Option<&Bound<'py, PyDict>>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<PyObject> {
        let scheduler = slf.clone().unbind();
        let trigger_obj = trigger.clone().unbind();
        let trigger_args_obj = trigger_args.map(|d| d.clone().unbind());
        let kwargs_obj = kwargs.map(|d| d.clone().unbind());

        let decorator = pyo3::types::PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, PyTuple>,
                  _kw: Option<&Bound<'_, PyDict>>|
                  -> PyResult<PyObject> {
                let py = args.py();
                let func = args.get_item(0)?;

                let trigger_bound = trigger_obj.bind(py);
                let trigger_args_bound = trigger_args_obj.as_ref().map(|d| d.bind(py));
                let kwargs_bound = kwargs_obj.as_ref().map(|d| d.bind(py));

                let mut sched = scheduler.bind(py).borrow_mut();
                let trigger_args_ref = trigger_args_bound.map(|b| b.downcast::<PyDict>().unwrap());
                let kwargs_ref = kwargs_bound.map(|b| b.downcast::<PyDict>().unwrap());

                sched.add_job(
                    py,
                    &func,
                    Some(trigger_bound),
                    trigger_args_ref.as_deref(),
                    kwargs_ref.as_deref(),
                )?;

                Ok(func.clone().unbind())
            },
        )?;

        Ok(decorator.into())
    }

    #[pyo3(signature = (job_id, jobstore=None, trigger=None, **kwargs))]
    fn reschedule_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
        trigger: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let mut changes = apsched_core::model::JobChanges::default();

            // Build trigger from string or trigger object, plus any kwargs
            let actual_trigger = if let Some(trig) = trigger {
                Some(parse_trigger(py, trig, kwargs)?)
            } else {
                None
            };

            if let Some(rust_trigger) = actual_trigger {
                let now = Utc::now();
                let nrt = rust_trigger.get_next_fire_time(None, now);
                changes.trigger_state = Some(rust_trigger.serialize_state());
                changes.next_run_time = Some(nrt);
            }

            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let spec = rt.block_on(async {
                engine
                    .modify_job(&job_id_owned, jobstore_owned.as_deref(), changes)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None, **kwargs))]
    fn modify_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let mut changes = apsched_core::model::JobChanges::default();
            if let Some(kw) = kwargs {
                if let Some(name) = kw
                    .get_item("name")?
                    .and_then(|v| v.extract::<String>().ok())
                {
                    changes.name = Some(name);
                }
                if let Some(max) = kw
                    .get_item("max_instances")?
                    .and_then(|v| v.extract::<u32>().ok())
                {
                    changes.max_instances = Some(max);
                }
            }

            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let spec = rt.block_on(async {
                engine
                    .modify_job(&job_id_owned, jobstore_owned.as_deref(), changes)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let py_job = PyJob::from_spec(py, &spec)?;
            let py_job_obj = py_job.into_pyobject(py)?.into_any().unbind();
            // Update caches
            self.jobs_cache
                .insert(job_id.to_string(), py_job_obj.clone_ref(py));

            Ok(py_job_obj)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn remove_job(&mut self, py: Python<'_>, job_id: &str, jobstore: Option<&str>) -> PyResult<()> {
        self.jobs_cache.remove(job_id);

        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            rt.block_on(async {
                engine
                    .remove_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })
        } else {
            let initial_len = self.pending_jobs.len();
            self.pending_jobs.retain(|pj| pj.spec.id != job_id);
            if self.pending_jobs.len() == initial_len {
                Err(pyo3::exceptions::PyKeyError::new_err(format!(
                    "job '{}' not found",
                    job_id
                )))
            } else {
                Ok(())
            }
        }
    }

    #[pyo3(signature = (jobstore=None))]
    fn remove_all_jobs(&mut self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<()> {
        // Clear caches
        self.jobs_cache.clear();

        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let jobstore_owned = jobstore.map(|s| s.to_string());

            // Use bulk remove_all_jobs instead of iterating one-by-one
            rt.block_on(async {
                engine
                    .remove_all_jobs(jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })
        } else {
            if let Some(store) = jobstore {
                self.pending_jobs.retain(|pj| pj.spec.jobstore != store);
            } else {
                self.pending_jobs.clear();
            }
            Ok(())
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn get_job(
        &self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<Option<PyObject>> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            match rt.block_on(async {
                engine
                    .get_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
            }) {
                Ok(spec) => {
                    let py_job = PyJob::from_spec(py, &spec)?;
                    Ok(Some(py_job.into_pyobject(py)?.into_any().unbind()))
                }
                Err(_) => Ok(None),
            }
        } else {
            for pj in &self.pending_jobs {
                if pj.spec.id == job_id {
                    let py_job = PyJob::from_spec(py, &pj.spec)?;
                    return Ok(Some(py_job.into_pyobject(py)?.into_any().unbind()));
                }
            }
            Ok(None)
        }
    }

    #[pyo3(signature = (jobstore=None))]
    fn get_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<Vec<PyObject>> {
        // Fast path: build from cached PyJob objects without touching the engine.
        if !self.jobs_cache.is_empty() {
            let result: Vec<PyObject> = self
                .jobs_cache
                .values()
                .map(|obj| obj.clone_ref(py))
                .collect();
            return Ok(result);
        }
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let specs = rt.block_on(async {
                engine
                    .get_jobs(jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let mut result = Vec::new();
            for spec in &specs {
                let py_job = PyJob::from_spec(py, spec)?;
                result.push(py_job.into_pyobject(py)?.into_any().unbind());
            }
            Ok(result)
        } else {
            let mut result = Vec::new();
            for pj in &self.pending_jobs {
                if jobstore.is_none() || Some(pj.spec.jobstore.as_str()) == jobstore {
                    let py_job = PyJob::from_spec(py, &pj.spec)?;
                    result.push(py_job.into_pyobject(py)?.into_any().unbind());
                }
            }
            Ok(result)
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn pause_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let spec = rt.block_on(async {
                engine
                    .pause_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn resume_job(
        &mut self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let rt = Arc::clone(rt);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let spec = rt.block_on(async {
                engine
                    .resume_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
            let py_job = PyJob::from_spec(py, &spec)?;
            Ok(py_job.into_pyobject(py)?.into_any().unbind())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn pause(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.pause().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn resume(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.resume().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn wakeup(&self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.wakeup();
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (callback, mask=None))]
    fn add_listener(
        &mut self,
        py: Python<'_>,
        callback: PyObject,
        mask: Option<u32>,
    ) -> PyResult<()> {
        let mask = mask.unwrap_or(EVENT_ALL);
        self.listeners.push((callback.clone(), mask));

        if let Some(ref engine) = self.engine {
            let cb = callback;
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                mask,
            );
        }

        Ok(())
    }

    fn remove_listener(&mut self, py: Python<'_>, callback: PyObject) -> PyResult<()> {
        let cb_ptr = callback.as_ptr();
        self.listeners.retain(|(cb, _)| cb.as_ptr() != cb_ptr);
        Ok(())
    }

    #[pyo3(signature = (jobstore, alias=None, **kwargs))]
    fn add_jobstore(
        &mut self,
        py: Python<'_>,
        jobstore: &Bound<'_, PyAny>,
        alias: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let alias = alias.unwrap_or("default").to_string();
        let pending = parse_jobstore(jobstore, kwargs)?;

        if let Some(ref engine) = self.engine {
            // Engine is running; use the scheduler's own runtime
            let rt_ref = self
                .runtime
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("Scheduler runtime not available"))?;
            let store = pending.into_store(rt_ref)?;
            engine
                .add_jobstore(store, &alias)
                .map_err(scheduler_error_to_pyerr)?;
        } else {
            // Store for deferred creation at start time
            self.pending_stores.insert(alias.clone(), pending);
        }

        Ok(())
    }

    fn remove_jobstore(&mut self, alias: &str) -> PyResult<()> {
        self.stores.remove(alias);
        self.pending_stores.remove(alias);
        if let Some(ref engine) = self.engine {
            engine
                .remove_jobstore(alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        Ok(())
    }

    #[pyo3(signature = (executor, alias=None, **kwargs))]
    fn add_executor(
        &mut self,
        py: Python<'_>,
        executor: &Bound<'_, PyAny>,
        alias: Option<&str>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
        let alias = alias.unwrap_or("default").to_string();
        self.executors
            .insert(alias.clone(), executor.clone().unbind());

        if let Some(ref engine) = self.engine {
            if let Ok(py_exec) =
                executor.extract::<PyRef<'_, crate::executors::PyThreadPoolExecutor>>()
            {
                let exec = Arc::new(PythonAwareExecutor::new(
                    Arc::clone(&self.callable_store),
                    py_exec.max_workers,
                ));
                engine
                    .add_executor(exec, &alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        Ok(())
    }

    fn remove_executor(&mut self, alias: &str) -> PyResult<()> {
        self.executors.remove(alias);
        if let Some(ref engine) = self.engine {
            engine
                .remove_executor(alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        Ok(())
    }

    #[pyo3(signature = (jobstore=None))]
    fn print_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<()> {
        let jobs = self.get_jobs(py, jobstore)?;
        let builtins = py.import("builtins")?;

        if jobs.is_empty() {
            builtins.call_method1("print", ("No pending jobs",))?;
        } else {
            for job_obj in &jobs {
                let repr = job_obj.bind(py).repr()?;
                builtins.call_method1("print", (repr,))?;
            }
        }
        Ok(())
    }

    /// Simulate a trigger by computing the next N fire times without adding a job.
    #[pyo3(signature = (trigger, n=None, **kwargs))]
    fn simulate_trigger(
        &self,
        py: Python<'_>,
        trigger: &Bound<'_, PyAny>,
        n: Option<usize>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyObject>> {
        simulate_trigger_impl(py, trigger, n, kwargs)
    }

    /// Return a dict explaining the current state of a job.
    #[pyo3(signature = (job_id, jobstore=None))]
    fn explain_job(
        &self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        explain_job_impl(py, job_id, jobstore, &self.engine, &self.runtime)
    }

    /// Return a dict describing the scheduler state.
    fn get_scheduler_info(&self, py: Python<'_>) -> PyResult<PyObject> {
        get_scheduler_info_impl(
            py,
            &self.engine,
            &self.runtime,
            self.started,
            self.start_time,
        )
    }

    fn __repr__(&self) -> String {
        if self.started {
            "BackgroundScheduler(running=True)".to_string()
        } else {
            "BackgroundScheduler(running=False)".to_string()
        }
    }
}

// ---------------------------------------------------------------------------
// PyAsyncIOScheduler
// ---------------------------------------------------------------------------

#[pyclass(name = "AsyncIOScheduler")]
pub struct PyAsyncIOScheduler {
    engine: Option<Arc<SchedulerEngine>>,
    callable_store: Arc<CallableStore>,
    config: SchedulerConfig,
    pending_jobs: Vec<PendingJob>,
    stores: HashMap<String, PyObject>,
    pending_stores: HashMap<String, PendingStore>,
    executors: HashMap<String, PyObject>,
    listeners: Vec<(PyObject, u32)>,
    started: bool,
    runtime: Option<Arc<tokio::runtime::Runtime>>,
    start_time: Option<std::time::Instant>,
}

#[pymethods]
impl PyAsyncIOScheduler {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut config = SchedulerConfig::default();

        if let Some(kw) = kwargs {
            if let Some(tz) = kw
                .get_item("timezone")?
                .and_then(|v| v.extract::<String>().ok())
            {
                config.timezone = tz;
            }
            if let Some(jd) = kw.get_item("job_defaults")? {
                if let Ok(d) = jd.downcast::<PyDict>() {
                    if let Some(grace) = d
                        .get_item("misfire_grace_time")?
                        .and_then(|v| v.extract::<f64>().ok())
                    {
                        config.job_defaults.misfire_grace_time =
                            std::time::Duration::from_secs_f64(grace);
                    }
                    if let Some(coalesce) = d
                        .get_item("coalesce")?
                        .and_then(|v| v.extract::<bool>().ok())
                    {
                        config.job_defaults.coalesce = if coalesce {
                            CoalescePolicy::On
                        } else {
                            CoalescePolicy::Off
                        };
                    }
                    if let Some(max) = d
                        .get_item("max_instances")?
                        .and_then(|v| v.extract::<u32>().ok())
                    {
                        config.job_defaults.max_instances = max;
                    }
                }
            }
        }

        Ok(Self {
            engine: None,
            callable_store: Arc::new(CallableStore::new()),
            config,
            pending_jobs: Vec::new(),
            stores: HashMap::new(),
            pending_stores: HashMap::new(),
            executors: HashMap::new(),
            listeners: Vec::new(),
            started: false,
            runtime: None,
            start_time: None,
        })
    }

    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.started {
            return Err(PyRuntimeError::new_err("scheduler is already running"));
        }

        self.start_time = Some(std::time::Instant::now());
        let config = self.config.clone();

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| PyRuntimeError::new_err(format!("failed to create runtime: {}", e)))?,
        );

        let engine = Arc::new(SchedulerEngine::new(
            config,
            Arc::new(apsched_core::WallClock),
        ));

        // Add default store
        let default_store = Arc::new(MemoryJobStore::new());
        engine
            .add_jobstore(default_store, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Add user stores (pending stores, materialized with this runtime)
        for (alias, pending) in self.pending_stores.drain() {
            let store = pending.into_store(&rt)?;
            engine
                .add_jobstore(store, &alias)
                .map_err(scheduler_error_to_pyerr)?;
        }
        // Also try stores registered as Python objects
        for (alias, store_obj) in &self.stores {
            let store_obj_bound = store_obj.bind(py);
            if let Ok(pending) = parse_jobstore(&store_obj_bound, None) {
                let store = pending.into_store(&rt)?;
                engine
                    .add_jobstore(store, alias)
                    .map_err(scheduler_error_to_pyerr)?;
            }
        }

        // Capture the running asyncio event loop so async callables can be
        // scheduled on it from worker threads.
        let asyncio = py.import("asyncio")?;
        let event_loop: PyObject = asyncio.call_method0("get_event_loop")?.into();

        // Add default executor (Python-aware, with event loop for coroutines)
        let callable_store = Arc::clone(&self.callable_store);
        let default_executor = Arc::new(
            PythonAwareExecutor::new(Arc::clone(&callable_store), 10).with_event_loop(event_loop),
        );
        engine
            .add_executor(default_executor, "default")
            .map_err(scheduler_error_to_pyerr)?;

        // Register listeners
        for (callback, mask) in &self.listeners {
            let cb = callback.clone();
            let listener_mask = *mask;
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                listener_mask,
            );
        }

        // Start the engine
        let engine_clone = Arc::clone(&engine);
        rt.block_on(async { engine_clone.start().await.map_err(scheduler_error_to_pyerr) })?;

        // Add pending jobs
        let pending: Vec<PendingJob> = self.pending_jobs.drain(..).collect();
        for pj in pending {
            let engine_ref = Arc::clone(&engine);
            rt.block_on(async {
                engine_ref
                    .add_job(pj.spec)
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;
        }

        self.engine = Some(engine);
        self.runtime = Some(rt);
        self.started = true;

        Ok(())
    }

    #[pyo3(signature = (wait=None))]
    fn shutdown(&mut self, py: Python<'_>, wait: Option<bool>) -> PyResult<()> {
        let wait = wait.unwrap_or(true);
        if let (Some(engine), Some(rt)) = (self.engine.take(), self.runtime.take()) {
            rt.block_on(async {
                let _ = engine.shutdown(wait).await;
            });
        }
        self.started = false;
        Ok(())
    }

    #[pyo3(signature = (func, trigger=None, trigger_args=None, **kwargs))]
    fn add_job(
        &mut self,
        py: Python<'_>,
        func: &Bound<'_, PyAny>,
        trigger: Option<&Bound<'_, PyAny>>,
        trigger_args: Option<&Bound<'_, PyDict>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyObject> {
        let spec = build_schedule_spec(
            py,
            func,
            trigger,
            trigger_args,
            kwargs,
            &self.callable_store,
            &self.config,
        )?;

        let py_job = PyJob::from_spec(py, &spec)?;

        if self.started {
            if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
                let engine = Arc::clone(engine);
                let spec_clone = spec;
                rt.block_on(async {
                    engine
                        .add_job(spec_clone)
                        .await
                        .map_err(scheduler_error_to_pyerr)
                })?;
            }
        } else {
            self.pending_jobs.push(PendingJob { spec });
        }

        Ok(py_job.into_pyobject(py)?.into_any().unbind())
    }

    #[pyo3(signature = (job_id, jobstore=None))]
    fn remove_job(&mut self, py: Python<'_>, job_id: &str, jobstore: Option<&str>) -> PyResult<()> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let job_id_owned = job_id.to_string();
            let jobstore_owned = jobstore.map(|s| s.to_string());

            rt.block_on(async {
                engine
                    .remove_job(&job_id_owned, jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })
        } else {
            let initial_len = self.pending_jobs.len();
            self.pending_jobs.retain(|pj| pj.spec.id != job_id);
            if self.pending_jobs.len() == initial_len {
                Err(pyo3::exceptions::PyKeyError::new_err(format!(
                    "job '{}' not found",
                    job_id
                )))
            } else {
                Ok(())
            }
        }
    }

    #[pyo3(signature = (jobstore=None))]
    fn get_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<Vec<PyObject>> {
        if let (Some(ref engine), Some(ref rt)) = (&self.engine, &self.runtime) {
            let engine = Arc::clone(engine);
            let jobstore_owned = jobstore.map(|s| s.to_string());

            let specs = rt.block_on(async {
                engine
                    .get_jobs(jobstore_owned.as_deref())
                    .await
                    .map_err(scheduler_error_to_pyerr)
            })?;

            let mut result = Vec::new();
            for spec in &specs {
                let py_job = PyJob::from_spec(py, spec)?;
                result.push(py_job.into_pyobject(py)?.into_any().unbind());
            }
            Ok(result)
        } else {
            let mut result = Vec::new();
            for pj in &self.pending_jobs {
                if jobstore.is_none() || Some(pj.spec.jobstore.as_str()) == jobstore {
                    let py_job = PyJob::from_spec(py, &pj.spec)?;
                    result.push(py_job.into_pyobject(py)?.into_any().unbind());
                }
            }
            Ok(result)
        }
    }

    fn pause(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.pause().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn resume(&mut self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.resume().map_err(scheduler_error_to_pyerr)
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    fn wakeup(&self) -> PyResult<()> {
        if let Some(ref engine) = self.engine {
            engine.wakeup();
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("scheduler is not running"))
        }
    }

    #[pyo3(signature = (callback, mask=None))]
    fn add_listener(
        &mut self,
        py: Python<'_>,
        callback: PyObject,
        mask: Option<u32>,
    ) -> PyResult<()> {
        let mask = mask.unwrap_or(EVENT_ALL);
        self.listeners.push((callback.clone(), mask));

        if let Some(ref engine) = self.engine {
            let cb = callback;
            engine.add_listener(
                Arc::new(move |event: &SchedulerEvent| {
                    Python::with_gil(|py| {
                        let code = event.event_mask();
                        let py_event = crate::events::PySchedulerEvent::new(code, None);
                        let _ = cb.bind(py).call1((py_event,));
                    });
                }),
                mask,
            );
        }

        Ok(())
    }

    fn remove_listener(&mut self, py: Python<'_>, callback: PyObject) -> PyResult<()> {
        let cb_ptr = callback.as_ptr();
        self.listeners.retain(|(cb, _)| cb.as_ptr() != cb_ptr);
        Ok(())
    }

    #[pyo3(signature = (jobstore=None))]
    fn print_jobs(&self, py: Python<'_>, jobstore: Option<&str>) -> PyResult<()> {
        let jobs = self.get_jobs(py, jobstore)?;
        let builtins = py.import("builtins")?;

        if jobs.is_empty() {
            builtins.call_method1("print", ("No pending jobs",))?;
        } else {
            for job_obj in &jobs {
                let repr = job_obj.bind(py).repr()?;
                builtins.call_method1("print", (repr,))?;
            }
        }
        Ok(())
    }

    /// Simulate a trigger by computing the next N fire times without adding a job.
    #[pyo3(signature = (trigger, n=None, **kwargs))]
    fn simulate_trigger(
        &self,
        py: Python<'_>,
        trigger: &Bound<'_, PyAny>,
        n: Option<usize>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Vec<PyObject>> {
        simulate_trigger_impl(py, trigger, n, kwargs)
    }

    /// Return a dict explaining the current state of a job.
    #[pyo3(signature = (job_id, jobstore=None))]
    fn explain_job(
        &self,
        py: Python<'_>,
        job_id: &str,
        jobstore: Option<&str>,
    ) -> PyResult<PyObject> {
        explain_job_impl(py, job_id, jobstore, &self.engine, &self.runtime)
    }

    /// Return a dict describing the scheduler state.
    fn get_scheduler_info(&self, py: Python<'_>) -> PyResult<PyObject> {
        get_scheduler_info_impl(
            py,
            &self.engine,
            &self.runtime,
            self.started,
            self.start_time,
        )
    }

    fn __repr__(&self) -> String {
        if self.started {
            "AsyncIOScheduler(running=True)".to_string()
        } else {
            "AsyncIOScheduler(running=False)".to_string()
        }
    }
}
