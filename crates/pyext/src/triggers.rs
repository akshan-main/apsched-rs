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
        let trigger = DateTrigger::new(dt, tz)
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
// IntervalTrigger
// ---------------------------------------------------------------------------

#[pyclass(name = "IntervalTrigger")]
pub struct PyIntervalTrigger {
    pub(crate) inner: IntervalTrigger,
}

#[pymethods]
impl PyIntervalTrigger {
    #[new]
    #[pyo3(signature = (weeks=0, days=0, hours=0, minutes=0, seconds=0, start_date=None, end_date=None, timezone=None, jitter=None))]
    fn new(
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
        let trigger = IntervalTrigger::new(weeks, days, hours, minutes, seconds, sd, ed, tz, jitter)
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
        year: Option<&str>,
        month: Option<&str>,
        day: Option<&str>,
        week: Option<&str>,
        day_of_week: Option<&str>,
        hour: Option<&str>,
        minute: Option<&str>,
        second: Option<&str>,
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
        let trigger = CronTrigger::new(
            year, month, day, week, day_of_week, hour, minute, second, sd, ed, tz, jitter,
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
        let trigger =
            CalendarIntervalTrigger::new(years, months, weeks, days, hour, minute, second, sd, ed, tz)
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
