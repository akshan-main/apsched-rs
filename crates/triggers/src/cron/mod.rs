mod expr;
mod field;
mod parser;

pub use self::expr::CompiledCronExpr;
pub use self::field::FieldMatcher;
pub use self::parser::parse_cron_field;

use chrono::{DateTime, Utc};
use rand::thread_rng;
use serde::{Deserialize, Serialize};

use apsched_core::error::TriggerError;
use apsched_core::model::TriggerState;
use apsched_core::traits::Trigger;

use crate::base::apply_jitter;

/// A cron-style trigger supporting seconds through years.
///
/// Mirrors APScheduler 3.x `CronTrigger` with fields: year, month, day,
/// week, day_of_week, hour, minute, second.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronTrigger {
    expr: CompiledCronExpr,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    timezone: String,
    jitter_secs: Option<f64>,
    // Original field strings for serialization / describe.
    year: Option<String>,
    month: Option<String>,
    day: Option<String>,
    week: Option<String>,
    day_of_week: Option<String>,
    hour: Option<String>,
    minute: Option<String>,
    second: Option<String>,
}

impl CronTrigger {
    /// Create a new `CronTrigger`.
    ///
    /// All field parameters are `Option<&str>` — `None` is treated as `"*"`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        year: Option<&str>,
        month: Option<&str>,
        day: Option<&str>,
        week: Option<&str>,
        day_of_week: Option<&str>,
        hour: Option<&str>,
        minute: Option<&str>,
        second: Option<&str>,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
        jitter: Option<f64>,
    ) -> Result<Self, TriggerError> {
        // Validate timezone.
        timezone.parse::<chrono_tz::Tz>().map_err(|_| {
            TriggerError::InvalidCronExpression(format!("invalid timezone: {}", timezone))
        })?;

        let compiled =
            CompiledCronExpr::compile(year, month, day, week, day_of_week, hour, minute, second)?;

        Ok(Self {
            expr: compiled,
            start_date,
            end_date,
            timezone,
            jitter_secs: jitter,
            year: year.map(String::from),
            month: month.map(String::from),
            day: day.map(String::from),
            week: week.map(String::from),
            day_of_week: day_of_week.map(String::from),
            hour: hour.map(String::from),
            minute: minute.map(String::from),
            second: second.map(String::from),
        })
    }
}

impl Trigger for CronTrigger {
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        // Determine the base time to search from.
        let base = match previous_fire_time {
            Some(prev) => prev,
            None => {
                // If start_date is set and in the future, search from just before it.
                match self.start_date {
                    Some(start) if start > now => start - chrono::Duration::seconds(1),
                    _ => now,
                }
            }
        };

        let candidate = self.expr.get_next_fire_time(base, &self.timezone)?;

        // Enforce start_date: result must be >= start_date.
        if let Some(start) = self.start_date {
            if candidate < start {
                // Search again from start.
                let candidate2 = self
                    .expr
                    .get_next_fire_time(start - chrono::Duration::seconds(1), &self.timezone)?;
                if let Some(end) = self.end_date {
                    if candidate2 > end {
                        return None;
                    }
                }
                let jittered = self.maybe_apply_jitter(candidate2);
                return Some(jittered);
            }
        }

        // Enforce end_date.
        if let Some(end) = self.end_date {
            if candidate > end {
                return None;
            }
        }

        Some(self.maybe_apply_jitter(candidate))
    }

    fn describe(&self) -> String {
        let minute = self.minute.as_deref().unwrap_or("*");
        let hour = self.hour.as_deref().unwrap_or("*");
        let day = self.day.as_deref().unwrap_or("*");
        let month = self.month.as_deref().unwrap_or("*");
        let dow = self.day_of_week.as_deref().unwrap_or("*");
        format!("cron[{} {} {} {} {}]", minute, hour, day, month, dow)
    }

    fn serialize_state(&self) -> TriggerState {
        TriggerState::Cron {
            year: self.year.clone(),
            month: self.month.clone(),
            day: self.day.clone(),
            week: self.week.clone(),
            day_of_week: self.day_of_week.clone(),
            hour: self.hour.clone(),
            minute: self.minute.clone(),
            second: self.second.clone(),
            start_date: self.start_date,
            end_date: self.end_date,
            timezone: self.timezone.clone(),
            jitter: self.jitter_secs,
        }
    }

    fn trigger_type(&self) -> &'static str {
        "cron"
    }
}

impl CronTrigger {
    fn maybe_apply_jitter(&self, fire_time: DateTime<Utc>) -> DateTime<Utc> {
        match self.jitter_secs {
            Some(j) if j > 0.0 => apply_jitter(fire_time, j, &mut thread_rng()),
            _ => fire_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, s)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn test_basic_cron() {
        // Every minute at :00 seconds.
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("0"),
            None,
            None,
            "UTC".to_string(),
            None,
        )
        .unwrap();

        let now = utc(2024, 6, 15, 10, 30, 0);
        let next = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(next, utc(2024, 6, 15, 10, 31, 0));
    }

    #[test]
    fn test_cron_with_start_date() {
        let start = utc(2024, 7, 1, 0, 0, 0);
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
            Some(start),
            None,
            "UTC".to_string(),
            None,
        )
        .unwrap();

        let now = utc(2024, 6, 15, 0, 0, 0);
        let next = trigger.get_next_fire_time(None, now).unwrap();
        assert!(next >= start);
        assert_eq!(next, utc(2024, 7, 1, 12, 0, 0));
    }

    #[test]
    fn test_cron_with_end_date() {
        let end = utc(2024, 6, 15, 12, 0, 0);
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
            None,
            Some(end),
            "UTC".to_string(),
            None,
        )
        .unwrap();

        let now = utc(2024, 6, 15, 11, 0, 0);
        let next = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(next, utc(2024, 6, 15, 12, 0, 0));

        // After the end_date, should return None.
        let next2 = trigger.get_next_fire_time(Some(next), now);
        assert!(next2.is_none());
    }

    #[test]
    fn test_cron_describe() {
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            Some("MON-FRI"),
            Some("9"),
            Some("0"),
            Some("0"),
            None,
            None,
            "UTC".to_string(),
            None,
        )
        .unwrap();
        assert_eq!(trigger.describe(), "cron[0 9 * * MON-FRI]");
    }

    #[test]
    fn test_cron_serialize_state() {
        let trigger = CronTrigger::new(
            Some("2024"),
            Some("1-6"),
            None,
            None,
            None,
            Some("*/2"),
            Some("0"),
            Some("0"),
            None,
            None,
            "UTC".to_string(),
            Some(2.0),
        )
        .unwrap();
        match trigger.serialize_state() {
            TriggerState::Cron {
                year,
                month,
                hour,
                jitter,
                ..
            } => {
                assert_eq!(year.as_deref(), Some("2024"));
                assert_eq!(month.as_deref(), Some("1-6"));
                assert_eq!(hour.as_deref(), Some("*/2"));
                assert_eq!(jitter, Some(2.0));
            }
            _ => panic!("expected Cron variant"),
        }
    }

    #[test]
    fn test_cron_trigger_type() {
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "UTC".to_string(),
            None,
        )
        .unwrap();
        assert_eq!(trigger.trigger_type(), "cron");
    }

    #[test]
    fn test_cron_jitter() {
        let trigger = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("0"),
            None,
            None,
            "UTC".to_string(),
            Some(5.0),
        )
        .unwrap();

        let now = utc(2024, 6, 15, 10, 30, 0);
        let base_expected = utc(2024, 6, 15, 10, 31, 0);

        for _ in 0..20 {
            let next = trigger.get_next_fire_time(None, now).unwrap();
            assert!(next >= base_expected);
            assert!(next <= base_expected + chrono::Duration::seconds(5));
        }
    }

    #[test]
    fn test_cron_invalid_timezone() {
        let result = CronTrigger::new(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "Invalid/TZ".to_string(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_cron_invalid_field() {
        let result = CronTrigger::new(
            None,
            Some("13"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "UTC".to_string(),
            None,
        );
        assert!(result.is_err());
    }
}
