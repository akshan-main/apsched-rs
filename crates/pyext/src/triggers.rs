use chrono::Utc;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use apsched_core::traits::Trigger;
use apsched_triggers::{CalendarIntervalTrigger, CronTrigger, DateTrigger, IntervalTrigger};

use crate::convert::{datetime_to_py, py_to_datetime};

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
    fn new(run_date: Option<&Bound<'_, PyAny>>, timezone: Option<&str>) -> PyResult<Self> {
        let tz = timezone.unwrap_or("UTC").to_string();
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
    #[pyo3(signature = (weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None, end_date=None, timezone=None, jitter=None))]
    fn new(
        py: Python<'_>,
        weeks: i64,
        days: i64,
        hours: i64,
        minutes: i64,
        seconds: i64,
        start_date: Option<&Bound<'_, PyAny>>,
        end_date: Option<&Bound<'_, PyAny>>,
        timezone: Option<&str>,
        jitter: Option<f64>,
    ) -> PyResult<Self> {
        let tz = timezone.unwrap_or("UTC").to_string();
        let sd = match start_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let ed = match end_date {
            Some(obj) => Some(py_to_datetime(obj)?),
            None => None,
        };
        let interval_secs =
            (weeks * 7 * 86400 + days * 86400 + hours * 3600 + minutes * 60 + seconds) as f64;
        // Pre-create a Python timedelta for fast prev + interval in get_next_fire_time
        let datetime_mod = py.import("datetime")?;
        let timedelta_cls = datetime_mod.getattr("timedelta")?;
        let kwargs = pyo3::types::PyDict::new(py);
        kwargs.set_item("seconds", interval_secs)?;
        let py_timedelta = timedelta_cls.call((), Some(&kwargs))?.unbind();
        let trigger =
            IntervalTrigger::new(weeks, days, hours, minutes, seconds, sd, ed, tz, jitter)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
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
        timezone: Option<&str>,
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
        let tz = timezone.unwrap_or("UTC").to_string();
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
        timezone: Option<&str>,
    ) -> PyResult<Self> {
        let tz = timezone.unwrap_or("UTC").to_string();
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
