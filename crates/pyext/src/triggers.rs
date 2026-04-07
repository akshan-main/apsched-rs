use chrono::Utc;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use apsched_core::traits::Trigger;
use apsched_triggers::{CalendarIntervalTrigger, CronTrigger, DateTrigger, IntervalTrigger};

use crate::convert::{datetime_to_py, py_to_datetime};

/// Extract a timezone string from a Python value that may be a string,
/// `zoneinfo.ZoneInfo`, `pytz` timezone, `datetime.timezone`, or any object
/// with a `key`/`zone` attribute or sensible `str()` representation.
pub(crate) fn extract_timezone(tz: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let Some(tz) = tz else {
        return Ok("UTC".to_string());
    };
    if tz.is_none() {
        return Ok("UTC".to_string());
    }
    if let Ok(s) = tz.extract::<String>() {
        return Ok(s);
    }
    // zoneinfo.ZoneInfo has a `.key` attribute
    if let Ok(key_attr) = tz.getattr("key") {
        if let Ok(s) = key_attr.extract::<String>() {
            return Ok(s);
        }
    }
    // pytz timezones expose a `.zone` attribute
    if let Ok(zone_attr) = tz.getattr("zone") {
        if let Ok(s) = zone_attr.extract::<String>() {
            return Ok(s);
        }
    }
    // datetime.timezone (fixed offsets) and others — fall back to str()
    if let Ok(s) = tz.str() {
        let val = s.to_string();
        // datetime.timezone.utc → "UTC"
        if val == "UTC" || val.starts_with("UTC") {
            // For fixed UTC offsets like "UTC+05:00" we cannot easily map to a
            // chrono-tz name; default to UTC. Users should pass named zones.
            return Ok("UTC".to_string());
        }
        return Ok(val);
    }
    Ok("UTC".to_string())
}

// ---------------------------------------------------------------------------
// DateTrigger
// ---------------------------------------------------------------------------

#[pyclass(name = "DateTrigger")]
pub struct PyDateTrigger {
    pub(crate) inner: DateTrigger,
}

#[pymethods]
impl PyDateTrigger {
    #[new]
    #[pyo3(signature = (run_date=None, timezone=None))]
    fn new(
        run_date: Option<&Bound<'_, PyAny>>,
        timezone: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let tz = extract_timezone(timezone)?;
        let dt = match run_date {
            Some(obj) => py_to_datetime(obj)?,
            None => Utc::now(),
        };
        let trigger = DateTrigger::new(dt, tz).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: trigger })
    }

    #[pyo3(signature = (previous_fire_time=None, now=None))]
    fn get_next_fire_time(
        &self,
        py: Python<'_>,
        previous_fire_time: Option<&Bound<'_, PyAny>>,
        now: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<PyObject>> {
        let prev = match previous_fire_time {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let now_dt = match now {
            Some(obj) => py_to_datetime(obj)?,
            None => Utc::now(),
        };
        match self.inner.get_next_fire_time(prev, now_dt) {
            Some(dt) => Ok(Some(datetime_to_py(py, dt)?)),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        self.inner.describe()
    }

    fn __str__(&self) -> String {
        self.inner.describe()
    }
}

// ---------------------------------------------------------------------------
// IntervalTrigger
// ---------------------------------------------------------------------------

#[pyclass(name = "IntervalTrigger")]
pub struct PyIntervalTrigger {
    pub(crate) inner: IntervalTrigger,
    /// Cached interval in seconds for fast-path computation
    interval_secs: f64,
    /// Cached Python timedelta for fast prev + interval computation
    py_timedelta: PyObject,
}

#[pymethods]
impl PyIntervalTrigger {
    #[new]
    #[pyo3(signature = (weeks=0.0, days=0.0, hours=0.0, minutes=0.0, seconds=0.0, start_date=None, end_date=None, timezone=None, jitter=None))]
    fn new(
        py: Python<'_>,
        weeks: f64,
        days: f64,
        hours: f64,
        minutes: f64,
        seconds: f64,
        start_date: Option<&Bound<'_, PyAny>>,
        end_date: Option<&Bound<'_, PyAny>>,
        timezone: Option<&Bound<'_, PyAny>>,
        jitter: Option<f64>,
    ) -> PyResult<Self> {
        let tz = extract_timezone(timezone)?;
        let sd = match start_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let ed = match end_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let interval_secs =
            weeks * 7.0 * 86400.0 + days * 86400.0 + hours * 3600.0 + minutes * 60.0 + seconds;
        if interval_secs <= 0.0 {
            return Err(PyValueError::new_err(
                "interval must be positive and non-zero",
            ));
        }
        // Pre-create a Python timedelta for fast prev + interval in get_next_fire_time
        let datetime_mod = py.import("datetime")?;
        let timedelta_cls = datetime_mod.getattr("timedelta")?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("seconds", interval_secs)?;
        let py_timedelta = timedelta_cls.call((), Some(&kwargs))?.unbind();
        // If sub-second precision is needed, use the from_micros constructor.
        let total_micros = (interval_secs * 1_000_000.0).round() as i64;
        let has_fractional = (interval_secs.fract() != 0.0)
            || weeks.fract() != 0.0
            || days.fract() != 0.0
            || hours.fract() != 0.0
            || minutes.fract() != 0.0;
        let trigger = if has_fractional {
            IntervalTrigger::from_micros(total_micros, sd, ed, tz, jitter)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        } else {
            IntervalTrigger::new(
                weeks as i64,
                days as i64,
                hours as i64,
                minutes as i64,
                seconds as i64,
                sd,
                ed,
                tz,
                jitter,
            )
            .map_err(|e| PyValueError::new_err(e.to_string()))?
        };
        Ok(Self {
            inner: trigger,
            interval_secs,
            py_timedelta,
        })
    }

    #[pyo3(signature = (previous_fire_time=None, now=None))]
    fn get_next_fire_time(
        &self,
        py: Python<'_>,
        previous_fire_time: Option<&Bound<'_, PyAny>>,
        now: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<PyObject>> {
        // Fast path: when we have a previous_fire_time, just do prev + timedelta
        // using Python's native datetime arithmetic (single __add__ call).
        if let Some(prev_obj) = previous_fire_time {
            let result = prev_obj.call_method1("__add__", (self.py_timedelta.bind(py),))?;
            return Ok(Some(result.unbind()));
        }
        // Slow path: first fire needs full logic (start_date handling etc.)
        let now_dt = match now {
            Some(obj) => py_to_datetime(obj)?,
            None => Utc::now(),
        };
        match self.inner.get_next_fire_time(None, now_dt) {
            Some(dt) => Ok(Some(datetime_to_py(py, dt)?)),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        self.inner.describe()
    }

    fn __str__(&self) -> String {
        self.inner.describe()
    }
}

// ---------------------------------------------------------------------------
// CronTrigger
// ---------------------------------------------------------------------------

#[pyclass(name = "CronTrigger")]
pub struct PyCronTrigger {
    pub(crate) inner: CronTrigger,
}

#[pymethods]
impl PyCronTrigger {
    #[new]
    #[pyo3(signature = (year=None, month=None, day=None, week=None, day_of_week=None, hour=None, minute=None, second=None, start_date=None, end_date=None, timezone=None, jitter=None))]
    fn new(
        year: Option<&Bound<'_, PyAny>>,
        month: Option<&Bound<'_, PyAny>>,
        day: Option<&Bound<'_, PyAny>>,
        week: Option<&Bound<'_, PyAny>>,
        day_of_week: Option<&Bound<'_, PyAny>>,
        hour: Option<&Bound<'_, PyAny>>,
        minute: Option<&Bound<'_, PyAny>>,
        second: Option<&Bound<'_, PyAny>>,
        start_date: Option<&Bound<'_, PyAny>>,
        end_date: Option<&Bound<'_, PyAny>>,
        timezone: Option<&Bound<'_, PyAny>>,
        jitter: Option<f64>,
    ) -> PyResult<Self> {
        // Helper: accept both str and int for cron fields, converting int to str
        fn pyany_to_cron_str(obj: Option<&Bound<'_, PyAny>>) -> Option<String> {
            obj.map(|v| {
                if let Ok(s) = v.extract::<String>() {
                    s
                } else if let Ok(i) = v.extract::<i64>() {
                    i.to_string()
                } else {
                    v.str().map(|s| s.to_string()).unwrap_or_default()
                }
            })
        }
        let year_s = pyany_to_cron_str(year);
        let month_s = pyany_to_cron_str(month);
        let day_s = pyany_to_cron_str(day);
        let week_s = pyany_to_cron_str(week);
        let day_of_week_s = pyany_to_cron_str(day_of_week);
        let hour_s = pyany_to_cron_str(hour);
        let minute_s = pyany_to_cron_str(minute);
        let second_s = pyany_to_cron_str(second);
        let tz = extract_timezone(timezone)?;
        let sd = match start_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let ed = match end_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let trigger = CronTrigger::new(
            year_s.as_deref(),
            month_s.as_deref(),
            day_s.as_deref(),
            week_s.as_deref(),
            day_of_week_s.as_deref(),
            hour_s.as_deref(),
            minute_s.as_deref(),
            second_s.as_deref(),
            sd,
            ed,
            tz,
            jitter,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: trigger })
    }

    #[pyo3(signature = (previous_fire_time=None, now=None))]
    fn get_next_fire_time(
        &self,
        py: Python<'_>,
        previous_fire_time: Option<&Bound<'_, PyAny>>,
        now: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<PyObject>> {
        let prev = match previous_fire_time {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let now_dt = match now {
            Some(obj) => py_to_datetime(obj)?,
            None => Utc::now(),
        };
        match self.inner.get_next_fire_time(prev, now_dt) {
            Some(dt) => Ok(Some(datetime_to_py(py, dt)?)),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        self.inner.describe()
    }

    fn __str__(&self) -> String {
        self.inner.describe()
    }
}

// ---------------------------------------------------------------------------
// CalendarIntervalTrigger
// ---------------------------------------------------------------------------

#[pyclass(name = "CalendarIntervalTrigger")]
pub struct PyCalendarIntervalTrigger {
    pub(crate) inner: CalendarIntervalTrigger,
}

#[pymethods]
impl PyCalendarIntervalTrigger {
    #[new]
    #[pyo3(signature = (years=0, months=0, weeks=0, days=0, hour=0, minute=0, second=0, start_date=None, end_date=None, timezone=None))]
    fn new(
        years: i32,
        months: i32,
        weeks: i32,
        days: i32,
        hour: u32,
        minute: u32,
        second: u32,
        start_date: Option<&Bound<'_, PyAny>>,
        end_date: Option<&Bound<'_, PyAny>>,
        timezone: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let tz = extract_timezone(timezone)?;
        let sd = match start_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let ed = match end_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let trigger = CalendarIntervalTrigger::new(
            years, months, weeks, days, hour, minute, second, sd, ed, tz,
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: trigger })
    }

    #[pyo3(signature = (previous_fire_time=None, now=None))]
    fn get_next_fire_time(
        &self,
        py: Python<'_>,
        previous_fire_time: Option<&Bound<'_, PyAny>>,
        now: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Option<PyObject>> {
        let prev = match previous_fire_time {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let now_dt = match now {
            Some(obj) => py_to_datetime(obj)?,
            None => Utc::now(),
        };
        match self.inner.get_next_fire_time(prev, now_dt) {
            Some(dt) => Ok(Some(datetime_to_py(py, dt)?)),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        self.inner.describe()
    }

    fn __str__(&self) -> String {
        self.inner.describe()
    }
}
