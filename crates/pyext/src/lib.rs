#![allow(unused_variables, dead_code, deprecated)]

use pyo3::prelude::*;

mod convert;
mod events;
mod executors;
mod plugin_trigger;
mod scheduler;
mod stores;
mod triggers;

#[pyfunction]
#[pyo3(signature = (otel_endpoint=None))]
fn init_tracing(otel_endpoint: Option<&str>) {
    apsched_core::init_tracing(otel_endpoint);
}

#[pymodule]
fn _rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register trigger classes
    m.add_class::<triggers::PyDateTrigger>()?;
    m.add_class::<triggers::PyIntervalTrigger>()?;
    m.add_class::<triggers::PyCronTrigger>()?;
    m.add_class::<triggers::PyCalendarIntervalTrigger>()?;

    // Register store classes
    m.add_class::<stores::PyMemoryJobStore>()?;
    m.add_class::<stores::PySqlJobStore>()?;
    m.add_class::<stores::PyRedisJobStore>()?;
    m.add_class::<stores::PyMongoJobStore>()?;

    // Register executor classes
    m.add_class::<executors::PyThreadPoolExecutor>()?;
    m.add_class::<executors::PyProcessPoolExecutor>()?;

    // Register scheduler classes
    m.add_class::<scheduler::PyBlockingScheduler>()?;
    m.add_class::<scheduler::PyBackgroundScheduler>()?;
    m.add_class::<scheduler::PyAsyncIOScheduler>()?;

    // Register Job class
    m.add_class::<scheduler::PyJob>()?;

    // Register event constants and classes
    events::register_events(m)?;

    // Register init_tracing function
    m.add_function(wrap_pyfunction!(init_tracing, m)?)?;

    Ok(())
}
