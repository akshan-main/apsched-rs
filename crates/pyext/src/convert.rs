use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};

use apsched_core::error::SchedulerError;
use apsched_core::model::{CallableRef, SerializedValue};

// ---------------------------------------------------------------------------
// Python datetime <-> chrono
// ---------------------------------------------------------------------------

/// Convert a Python `datetime.datetime` to `chrono::DateTime<Utc>`.
///
/// Handles both timezone-aware and naive datetimes.  Naive datetimes are
/// assumed to be in UTC.
pub fn py_to_datetime(obj: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    let py = obj.py();

    // Try to extract year/month/day/hour/minute/second/microsecond
    let year: i32 = obj.getattr("year")?.extract()?;
    let month: u32 = obj.getattr("month")?.extract()?;
    let day: u32 = obj.getattr("day")?.extract()?;
    let hour: u32 = obj.getattr("hour")?.extract()?;
    let minute: u32 = obj.getattr("minute")?.extract()?;
    let second: u32 = obj.getattr("second")?.extract()?;
    let microsecond: u32 = obj.getattr("microsecond")?.extract()?;

    let naive = NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| PyValueError::new_err("invalid date"))?,
        chrono::NaiveTime::from_hms_micro_opt(hour, minute, second, microsecond)
            .ok_or_else(|| PyValueError::new_err("invalid time"))?,
    );

    // Check if the datetime has tzinfo
    let tzinfo = obj.getattr("tzinfo")?;
    if tzinfo.is_none() {
        // Naive datetime — assume UTC
        Ok(Utc.from_utc_datetime(&naive))
    } else {
        // Timezone-aware: compute UTC offset and convert
        let utcoffset = obj.call_method0("utcoffset")?;
        if utcoffset.is_none() {
            Ok(Utc.from_utc_datetime(&naive))
        } else {
            let total_seconds: f64 = utcoffset.call_method0("total_seconds")?.extract()?;
            let offset_secs = total_seconds as i64;
            let utc_naive = naive - chrono::Duration::seconds(offset_secs);
            Ok(Utc.from_utc_datetime(&utc_naive))
        }
    }
}

/// Convert a `chrono::DateTime<Utc>` to a Python `datetime.datetime` (UTC).
pub fn datetime_to_py(py: Python<'_>, dt: DateTime<Utc>) -> PyResult<PyObject> {
    let datetime_mod = py.import("datetime")?;
    let datetime_cls = datetime_mod.getattr("datetime")?;
    let timezone_cls = datetime_mod.getattr("timezone")?;
    let utc = timezone_cls.getattr("utc")?;

    let naive = dt.naive_utc();
    let result = datetime_cls.call1((
        naive.date().year(),
        naive.date().month(),
        naive.date().day(),
        naive.time().hour(),
        naive.time().minute(),
        naive.time().second(),
        naive.time().nanosecond() / 1000, // microseconds
        &utc,
    ))?;

    Ok(result.into())
}

use chrono::Datelike;
use chrono::Timelike;

// ---------------------------------------------------------------------------
// Python timedelta / number -> Duration
// ---------------------------------------------------------------------------

/// Convert a Python `timedelta` or numeric (seconds) to `std::time::Duration`.
pub fn py_to_duration(obj: &Bound<'_, PyAny>) -> PyResult<std::time::Duration> {
    // Try as number first
    if let Ok(secs) = obj.extract::<f64>() {
        if secs < 0.0 {
            return Err(PyValueError::new_err("duration must be non-negative"));
        }
        return Ok(std::time::Duration::from_secs_f64(secs));
    }

    // Try as timedelta
    if obj.hasattr("total_seconds")? {
        let total_secs: f64 = obj.call_method0("total_seconds")?.extract()?;
        if total_secs < 0.0 {
            return Err(PyValueError::new_err("duration must be non-negative"));
        }
        return Ok(std::time::Duration::from_secs_f64(total_secs));
    }

    Err(PyTypeError::new_err(
        "expected a number (seconds) or datetime.timedelta",
    ))
}

// ---------------------------------------------------------------------------
// Timezone extraction
// ---------------------------------------------------------------------------

/// Extract a timezone string from a Python object.
/// Accepts a string directly, or a tzinfo/ZoneInfo object.
pub fn py_to_timezone(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    // String
    if let Ok(s) = obj.extract::<String>() {
        return Ok(s);
    }

    // Try .key attribute (ZoneInfo)
    if let Ok(key) = obj.getattr("key") {
        if let Ok(s) = key.extract::<String>() {
            return Ok(s);
        }
    }

    // Try .zone attribute (pytz)
    if let Ok(zone) = obj.getattr("zone") {
        if let Ok(s) = zone.extract::<String>() {
            return Ok(s);
        }
    }

    // Try str(obj)
    let s = obj.str()?.to_string();
    Ok(s)
}

// ---------------------------------------------------------------------------
// Pickle serialization
// ---------------------------------------------------------------------------

pub fn pickle_object(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let pickle = py.import("pickle")?;
    let bytes = pickle.call_method1("dumps", (obj, 2))?; // protocol 2 for broad compat
    bytes.extract::<Vec<u8>>()
}

pub fn unpickle_object(py: Python<'_>, data: &[u8]) -> PyResult<PyObject> {
    let pickle = py.import("pickle")?;
    let bytes = PyBytes::new(py, data);
    pickle.call_method1("loads", (bytes,)).map(|o| o.into())
}

// ---------------------------------------------------------------------------
// Serialize / deserialize Python args/kwargs
// ---------------------------------------------------------------------------

/// Pickle Python args/kwargs into SerializedValue vectors for storage.
pub fn serialize_py_args(
    py: Python<'_>,
    args: &Bound<'_, PyTuple>,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyResult<(Vec<SerializedValue>, HashMap<String, SerializedValue>)> {
    let mut ser_args = Vec::new();
    for item in args.iter() {
        let data = pickle_object(py, &item)?;
        ser_args.push(SerializedValue::Pickle(data));
    }

    let mut ser_kwargs = HashMap::new();
    if let Some(kw) = kwargs {
        for (key, val) in kw.iter() {
            let key_str: String = key.extract()?;
            let data = pickle_object(py, &val)?;
            ser_kwargs.insert(key_str, SerializedValue::Pickle(data));
        }
    }

    Ok((ser_args, ser_kwargs))
}

/// Unpickle SerializedValue vectors back into Python args and kwargs.
pub fn deserialize_py_args(
    py: Python<'_>,
    args: &[SerializedValue],
    kwargs: &HashMap<String, SerializedValue>,
) -> PyResult<(PyObject, PyObject)> {
    let mut py_args_vec: Vec<PyObject> = Vec::new();
    for arg in args {
        match arg {
            SerializedValue::Pickle(data) => {
                py_args_vec.push(unpickle_object(py, data)?);
            }
            SerializedValue::None => {
                py_args_vec.push(py.None());
            }
            SerializedValue::Json(v) => {
                // Convert JSON value to Python via json module
                let json_mod = py.import("json")?;
                let s = serde_json::to_string(v)
                    .map_err(|e| PyValueError::new_err(format!("json serialize error: {}", e)))?;
                let obj = json_mod.call_method1("loads", (s,))?;
                py_args_vec.push(obj.into());
            }
            SerializedValue::Cbor(data) => {
                // Fallback: try pickle
                py_args_vec.push(unpickle_object(py, data)?);
            }
        }
    }

    let py_args = PyTuple::new(py, &py_args_vec)?.into_any().unbind();

    let py_kwargs = PyDict::new(py);
    for (key, val) in kwargs {
        match val {
            SerializedValue::Pickle(data) => {
                py_kwargs.set_item(key, unpickle_object(py, data)?)?;
            }
            SerializedValue::None => {
                py_kwargs.set_item(key, py.None())?;
            }
            SerializedValue::Json(v) => {
                let json_mod = py.import("json")?;
                let s = serde_json::to_string(v)
                    .map_err(|e| PyValueError::new_err(format!("json serialize error: {}", e)))?;
                let obj = json_mod.call_method1("loads", (s,))?;
                py_kwargs.set_item(key, obj)?;
            }
            SerializedValue::Cbor(data) => {
                py_kwargs.set_item(key, unpickle_object(py, data)?)?;
            }
        }
    }

    Ok((py_args, py_kwargs.into_any().unbind()))
}

// ---------------------------------------------------------------------------
// Resolve a Python callable to a CallableRef
// ---------------------------------------------------------------------------

/// Determine if a Python callable can be referenced by import path, or if it
/// must be stored as an in-memory handle.
///
/// Returns `(CallableRef, Option<PyObject>)`: if the callable is importable, the
/// second element is None. Otherwise it holds the PyObject that must be stored
/// in the callable store.
pub fn resolve_callable(
    _py: Python<'_>,
    func: &Bound<'_, PyAny>,
) -> PyResult<(CallableRef, Option<PyObject>)> {
    // Try to get __module__ and __qualname__
    let module = func.getattr("__module__");
    let qualname = func.getattr("__qualname__");

    if let (Ok(module_obj), Ok(qualname_obj)) = (module, qualname) {
        if let (Ok(module_str), Ok(qualname_str)) =
            (module_obj.extract::<String>(), qualname_obj.extract::<String>())
        {
            // Check if it's a simple top-level function (no "<" in qualname,
            // no nested class methods like "Foo.<locals>.bar")
            if !qualname_str.contains('<')
                && !qualname_str.contains('.')
                && module_str != "__main__"
            {
                let import_path = format!("{}:{}", module_str, qualname_str);
                return Ok((CallableRef::ImportPath(import_path), None));
            }
        }
    }

    // Not importable — must be stored in-memory
    Ok((
        CallableRef::InMemoryHandle(0), // placeholder; caller assigns real ID
        Some(func.clone().unbind()),
    ))
}

// ---------------------------------------------------------------------------
// SchedulerError -> PyErr conversion
// ---------------------------------------------------------------------------

pub fn scheduler_error_to_pyerr(err: SchedulerError) -> PyErr {
    match &err {
        SchedulerError::AlreadyRunning => PyRuntimeError::new_err(err.to_string()),
        SchedulerError::NotRunning => PyRuntimeError::new_err(err.to_string()),
        SchedulerError::JobNotFound { .. } => {
            pyo3::exceptions::PyKeyError::new_err(err.to_string())
        }
        SchedulerError::JobStoreNotFound { .. } => {
            pyo3::exceptions::PyKeyError::new_err(err.to_string())
        }
        SchedulerError::ExecutorNotFound { .. } => {
            pyo3::exceptions::PyKeyError::new_err(err.to_string())
        }
        SchedulerError::DuplicateJobId { .. } => PyValueError::new_err(err.to_string()),
        SchedulerError::DuplicateJobStore { .. } => PyValueError::new_err(err.to_string()),
        SchedulerError::DuplicateExecutor { .. } => PyValueError::new_err(err.to_string()),
        SchedulerError::InvalidConfiguration(_) => PyValueError::new_err(err.to_string()),
        _ => PyRuntimeError::new_err(err.to_string()),
    }
}
