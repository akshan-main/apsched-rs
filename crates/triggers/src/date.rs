use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use apsched_core::error::TriggerError;
use apsched_core::model::TriggerState;
use apsched_core::traits::Trigger;

/// A trigger that fires exactly once at a specific date/time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateTrigger {
    run_date: DateTime<Utc>,
    timezone: String,
}

impl DateTrigger {
    /// Create a new `DateTrigger` that fires at `run_date`.
    ///
    /// The `timezone` string is stored for serialization purposes; the
    /// `run_date` is already in UTC.
    pub fn new(run_date: DateTime<Utc>, timezone: String) -> Result<Self, TriggerError> {
        // Validate timezone string
        timezone
            .parse::<chrono_tz::Tz>()
            .map_err(|_| TriggerError::InvalidDate(format!("invalid timezone: {}", timezone)))?;
        Ok(Self {
            run_date,
            timezone,
        })
    }

    /// Returns the configured run date.
    pub fn run_date(&self) -> DateTime<Utc> {
        self.run_date
    }
}

impl Trigger for DateTrigger {
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        // Only fire once: if it has already fired, return None.
        if previous_fire_time.is_some() {
            return None;
        }
        // Only fire if the run_date hasn't passed yet (allow exact match).
        if now <= self.run_date {
            Some(self.run_date)
        } else {
            None
        }
    }

    fn describe(&self) -> String {
        format!("date[{}]", self.run_date)
    }

    fn serialize_state(&self) -> TriggerState {
        TriggerState::Date {
            run_date: self.run_date,
            timezone: self.timezone.clone(),
        }
    }

    fn trigger_type(&self) -> &'static str {
        "date"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_basic_fire() {
        let now = Utc::now();
        let run_date = now + Duration::hours(1);
        let trigger = DateTrigger::new(run_date, "UTC".to_string()).unwrap();

        let next = trigger.get_next_fire_time(None, now);
        assert_eq!(next, Some(run_date));
    }

    #[test]
    fn test_no_repeat_after_fire() {
        let now = Utc::now();
        let run_date = now + Duration::hours(1);
        let trigger = DateTrigger::new(run_date, "UTC".to_string()).unwrap();

        let next = trigger.get_next_fire_time(Some(run_date), now);
        assert_eq!(next, None);
    }

    #[test]
    fn test_past_date() {
        let now = Utc::now();
        let run_date = now - Duration::hours(1);
        let trigger = DateTrigger::new(run_date, "UTC".to_string()).unwrap();

        let next = trigger.get_next_fire_time(None, now);
        assert_eq!(next, None);
    }

    #[test]
    fn test_exact_boundary() {
        let now = Utc::now();
        let trigger = DateTrigger::new(now, "UTC".to_string()).unwrap();

        // At exact boundary, should still fire.
        let next = trigger.get_next_fire_time(None, now);
        assert_eq!(next, Some(now));
    }

    #[test]
    fn test_describe() {
        let run_date = Utc::now();
        let trigger = DateTrigger::new(run_date, "UTC".to_string()).unwrap();
        let desc = trigger.describe();
        assert!(desc.starts_with("date["));
        assert!(desc.ends_with(']'));
    }

    #[test]
    fn test_serialize_state() {
        let run_date = Utc::now();
        let trigger = DateTrigger::new(run_date, "US/Eastern".to_string()).unwrap();
        match trigger.serialize_state() {
            TriggerState::Date { run_date: rd, timezone } => {
                assert_eq!(rd, run_date);
                assert_eq!(timezone, "US/Eastern");
            }
            _ => panic!("expected Date variant"),
        }
    }

    #[test]
    fn test_trigger_type() {
        let trigger = DateTrigger::new(Utc::now(), "UTC".to_string()).unwrap();
        assert_eq!(trigger.trigger_type(), "date");
    }

    #[test]
    fn test_invalid_timezone() {
        let result = DateTrigger::new(Utc::now(), "Not/A/Timezone".to_string());
        assert!(result.is_err());
    }
}
