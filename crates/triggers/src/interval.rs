use chrono::{DateTime, Duration, Utc};
use rand::thread_rng;
use serde::{Deserialize, Serialize};

use apsched_core::error::TriggerError;
use apsched_core::model::TriggerState;
use apsched_core::traits::Trigger;

use crate::base::apply_jitter;

/// A trigger that fires at fixed time intervals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntervalTrigger {
    /// Total interval as a chrono Duration.
    #[serde(with = "duration_serde")]
    interval: Duration,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    timezone: String,
    jitter_secs: Option<f64>,
    // Store original components for serialization
    weeks: i64,
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
}

mod duration_serde {
    use chrono::Duration;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.num_seconds().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = i64::deserialize(d)?;
        Ok(Duration::seconds(secs))
    }
}

impl IntervalTrigger {
    /// Create a new `IntervalTrigger`.
    ///
    /// The total interval is computed from the sum of all components. At least
    /// one component must be non-zero.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        weeks: i64,
        days: i64,
        hours: i64,
        minutes: i64,
        seconds: i64,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
        jitter: Option<f64>,
    ) -> Result<Self, TriggerError> {
        // Validate timezone
        timezone.parse::<chrono_tz::Tz>().map_err(|_| {
            TriggerError::InvalidInterval(format!("invalid timezone: {}", timezone))
        })?;

        let total_secs = weeks * 7 * 86400 + days * 86400 + hours * 3600 + minutes * 60 + seconds;
        if total_secs <= 0 {
            return Err(TriggerError::InvalidInterval(
                "interval must be positive and non-zero".to_string(),
            ));
        }

        Ok(Self {
            interval: Duration::seconds(total_secs),
            start_date,
            end_date,
            timezone,
            jitter_secs: jitter,
            weeks,
            days,
            hours,
            minutes,
            seconds,
        })
    }

    /// Check whether a candidate fire time is before the end_date (if set).
    fn check_end_date(&self, candidate: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if let Some(end) = self.end_date {
            if candidate > end {
                return None;
            }
        }
        Some(candidate)
    }

    /// Optionally apply jitter using thread-local RNG.
    fn maybe_apply_jitter(&self, fire_time: DateTime<Utc>) -> DateTime<Utc> {
        match self.jitter_secs {
            Some(j) if j > 0.0 => apply_jitter(fire_time, j, &mut thread_rng()),
            _ => fire_time,
        }
    }
}

impl Trigger for IntervalTrigger {
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let candidate = match previous_fire_time {
            Some(prev) => prev + self.interval,
            None => {
                match self.start_date {
                    Some(start) if start > now => {
                        // start_date is in the future: fire at start_date
                        start
                    }
                    Some(start) => {
                        // start_date is in the past: catch up by computing the next
                        // interval boundary after now.
                        let elapsed = now - start;
                        let elapsed_secs = elapsed.num_seconds();
                        let interval_secs = self.interval.num_seconds();
                        let periods = elapsed_secs / interval_secs;
                        let candidate = start + self.interval * (periods as i32);
                        if candidate <= now {
                            candidate + self.interval
                        } else {
                            candidate
                        }
                    }
                    None => {
                        // No start_date: first fire is now.
                        now
                    }
                }
            }
        };

        let candidate = self.check_end_date(candidate)?;
        Some(self.maybe_apply_jitter(candidate))
    }

    fn describe(&self) -> String {
        let secs = self.interval.num_seconds();
        if secs % 86400 == 0 {
            format!("interval[{} days]", secs / 86400)
        } else if secs % 3600 == 0 {
            format!("interval[{} hours]", secs / 3600)
        } else if secs % 60 == 0 {
            format!("interval[{} minutes]", secs / 60)
        } else {
            format!("interval[{} seconds]", secs)
        }
    }

    fn serialize_state(&self) -> TriggerState {
        TriggerState::Interval {
            weeks: self.weeks,
            days: self.days,
            hours: self.hours,
            minutes: self.minutes,
            seconds: self.seconds,
            start_date: self.start_date,
            end_date: self.end_date,
            timezone: self.timezone.clone(),
            jitter: self.jitter_secs,
        }
    }

    fn trigger_type(&self) -> &'static str {
        "interval"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_trigger(secs: i64) -> IntervalTrigger {
        IntervalTrigger::new(0, 0, 0, 0, secs, None, None, "UTC".to_string(), None).unwrap()
    }

    #[test]
    fn test_basic_interval() {
        let trigger = make_trigger(60);
        let now = Utc::now();

        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, now);

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        assert_eq!(second, first + Duration::seconds(60));
    }

    #[test]
    fn test_start_date_in_future() {
        let now = Utc::now();
        let start = now + Duration::hours(2);
        let trigger =
            IntervalTrigger::new(0, 0, 0, 0, 60, Some(start), None, "UTC".to_string(), None)
                .unwrap();

        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, start);
    }

    #[test]
    fn test_start_date_in_past() {
        let now = Utc::now();
        let start = now - Duration::seconds(150); // 150s ago, interval=60s
        let trigger =
            IntervalTrigger::new(0, 0, 0, 0, 60, Some(start), None, "UTC".to_string(), None)
                .unwrap();

        let first = trigger.get_next_fire_time(None, now).unwrap();
        // Should be at start + 3*60 = start + 180s which is 30s in the future
        assert!(first > now);
        assert!(first <= now + Duration::seconds(60));
    }

    #[test]
    fn test_end_date_boundary() {
        let now = Utc::now();
        let end = now + Duration::seconds(90);
        let trigger =
            IntervalTrigger::new(0, 0, 0, 0, 60, None, Some(end), "UTC".to_string(), None).unwrap();

        let first = trigger.get_next_fire_time(None, now).unwrap();
        let second = trigger.get_next_fire_time(Some(first), now);
        assert!(second.is_some()); // first + 60 = now + 60, which is < end
        let third = trigger.get_next_fire_time(Some(second.unwrap()), now);
        // second + 60 = now + 120, which is > end
        assert!(third.is_none());
    }

    #[test]
    fn test_jitter_bounds() {
        let trigger =
            IntervalTrigger::new(0, 0, 0, 0, 60, None, None, "UTC".to_string(), Some(5.0)).unwrap();
        let now = Utc::now();

        for _ in 0..50 {
            let fire = trigger.get_next_fire_time(None, now).unwrap();
            assert!(fire >= now);
            assert!(fire <= now + Duration::seconds(5));
        }
    }

    #[test]
    fn test_zero_interval_rejection() {
        let result = IntervalTrigger::new(0, 0, 0, 0, 0, None, None, "UTC".to_string(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_negative_interval_rejection() {
        let result = IntervalTrigger::new(0, 0, 0, 0, -10, None, None, "UTC".to_string(), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_describe() {
        let trigger =
            IntervalTrigger::new(0, 0, 2, 0, 0, None, None, "UTC".to_string(), None).unwrap();
        assert_eq!(trigger.describe(), "interval[2 hours]");

        let trigger = make_trigger(90);
        assert_eq!(trigger.describe(), "interval[90 seconds]");
    }

    #[test]
    fn test_serialize_state() {
        let trigger =
            IntervalTrigger::new(1, 2, 3, 4, 5, None, None, "UTC".to_string(), Some(1.0)).unwrap();
        match trigger.serialize_state() {
            TriggerState::Interval {
                weeks,
                days,
                hours,
                minutes,
                seconds,
                jitter,
                ..
            } => {
                assert_eq!(weeks, 1);
                assert_eq!(days, 2);
                assert_eq!(hours, 3);
                assert_eq!(minutes, 4);
                assert_eq!(seconds, 5);
                assert_eq!(jitter, Some(1.0));
            }
            _ => panic!("expected Interval variant"),
        }
    }

    #[test]
    fn test_trigger_type() {
        let trigger = make_trigger(10);
        assert_eq!(trigger.trigger_type(), "interval");
    }

    #[test]
    fn test_weeks_and_days() {
        let trigger =
            IntervalTrigger::new(1, 0, 0, 0, 0, None, None, "UTC".to_string(), None).unwrap();
        assert_eq!(trigger.describe(), "interval[7 days]");
    }
}
