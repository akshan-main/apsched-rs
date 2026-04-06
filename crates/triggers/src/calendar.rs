use chrono::{DateTime, Datelike, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use apsched_core::error::TriggerError;
use apsched_core::model::TriggerState;
use apsched_core::traits::Trigger;

/// A trigger that fires at calendar-based intervals.
///
/// Unlike `IntervalTrigger` (which uses fixed durations), this trigger
/// understands calendar semantics: adding 1 month to January 31 yields
/// February 28 (or 29), and adding 1 year to February 29 yields February 28.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalendarIntervalTrigger {
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hour: u32,
    minute: u32,
    second: u32,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    timezone: String,
}

impl CalendarIntervalTrigger {
    /// Create a new `CalendarIntervalTrigger`.
    ///
    /// At least one of `years`, `months`, `weeks`, `days` must be non-zero.
    /// `hour`, `minute`, `second` specify the time of day to fire (in the
    /// target timezone).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        years: i32,
        months: i32,
        weeks: i32,
        days: i32,
        hour: u32,
        minute: u32,
        second: u32,
        start_date: Option<DateTime<Utc>>,
        end_date: Option<DateTime<Utc>>,
        timezone: String,
    ) -> Result<Self, TriggerError> {
        // Validate timezone.
        timezone.parse::<chrono_tz::Tz>().map_err(|_| {
            TriggerError::InvalidCalendarInterval(format!("invalid timezone: {}", timezone))
        })?;

        if years == 0 && months == 0 && weeks == 0 && days == 0 {
            return Err(TriggerError::InvalidCalendarInterval(
                "at least one interval component must be non-zero".to_string(),
            ));
        }

        if hour > 23 || minute > 59 || second > 59 {
            return Err(TriggerError::InvalidCalendarInterval(
                "invalid time components".to_string(),
            ));
        }

        Ok(Self {
            years,
            months,
            weeks,
            days,
            hour,
            minute,
            second,
            start_date,
            end_date,
            timezone,
        })
    }

    /// Add the calendar interval to a date, clamping the day to the end of
    /// the target month if necessary.
    fn add_interval(&self, date: NaiveDate) -> Option<NaiveDate> {
        let total_months = self.years * 12 + self.months;
        let total_days = self.weeks * 7 + self.days;

        let mut new_date = date;

        if total_months != 0 {
            let target_month_0 = (new_date.year() * 12 + new_date.month0() as i32) + total_months;
            let target_year = target_month_0.div_euclid(12);
            let target_month = (target_month_0.rem_euclid(12) + 1) as u32;

            // Clamp day to last day of target month.
            let max_day = last_day_of_month(target_year, target_month);
            let day = new_date.day().min(max_day);
            new_date = NaiveDate::from_ymd_opt(target_year, target_month, day)?;
        }

        if total_days != 0 {
            new_date = new_date.checked_add_signed(chrono::Duration::days(total_days as i64))?;
        }

        Some(new_date)
    }

    /// Resolve a naive local datetime to UTC, handling DST.
    fn resolve_to_utc(&self, dt: NaiveDateTime) -> Option<DateTime<Utc>> {
        let tz: Tz = self.timezone.parse().ok()?;
        match tz.from_local_datetime(&dt) {
            LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
            LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
            LocalResult::None => {
                // DST gap: advance until valid.
                let mut candidate = dt;
                for _ in 0..7200 {
                    candidate += chrono::Duration::seconds(1);
                    match tz.from_local_datetime(&candidate) {
                        LocalResult::Single(t) => return Some(t.with_timezone(&Utc)),
                        LocalResult::Ambiguous(t, _) => return Some(t.with_timezone(&Utc)),
                        LocalResult::None => continue,
                    }
                }
                None
            }
        }
    }

    /// Build a NaiveDateTime from a date and the trigger's time-of-day.
    fn with_time(&self, date: NaiveDate) -> Option<NaiveDateTime> {
        let time = NaiveTime::from_hms_opt(self.hour, self.minute, self.second)?;
        Some(NaiveDateTime::new(date, time))
    }
}

impl Trigger for CalendarIntervalTrigger {
    fn get_next_fire_time(
        &self,
        previous_fire_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let tz: Tz = self.timezone.parse().ok()?;

        match previous_fire_time {
            Some(prev) => {
                let prev_local = prev.with_timezone(&tz).naive_local();
                let new_date = self.add_interval(prev_local.date())?;
                let dt = self.with_time(new_date)?;
                let result = self.resolve_to_utc(dt)?;
                self.check_end_date(result)
            }
            None => {
                let base = match self.start_date {
                    Some(start) => {
                        let start_local = start.with_timezone(&tz).naive_local();
                        start_local.date()
                    }
                    None => {
                        let now_local = now.with_timezone(&tz).naive_local();
                        now_local.date()
                    }
                };

                let dt = self.with_time(base)?;
                let candidate = self.resolve_to_utc(dt)?;

                if candidate > now {
                    self.check_end_date(candidate)
                } else {
                    // Time today has already passed; add one interval.
                    let new_date = self.add_interval(base)?;
                    let dt = self.with_time(new_date)?;
                    let result = self.resolve_to_utc(dt)?;
                    self.check_end_date(result)
                }
            }
        }
    }

    fn describe(&self) -> String {
        let mut parts = Vec::new();
        if self.years != 0 {
            parts.push(format!("{} year(s)", self.years));
        }
        if self.months != 0 {
            parts.push(format!("{} month(s)", self.months));
        }
        if self.weeks != 0 {
            parts.push(format!("{} week(s)", self.weeks));
        }
        if self.days != 0 {
            parts.push(format!("{} day(s)", self.days));
        }
        format!(
            "calendarinterval[{} at {:02}:{:02}:{:02}]",
            parts.join(", "),
            self.hour,
            self.minute,
            self.second
        )
    }

    fn serialize_state(&self) -> TriggerState {
        TriggerState::CalendarInterval {
            years: self.years,
            months: self.months,
            weeks: self.weeks,
            days: self.days,
            hour: self.hour,
            minute: self.minute,
            second: self.second,
            start_date: self.start_date,
            end_date: self.end_date,
            timezone: self.timezone.clone(),
        }
    }

    fn trigger_type(&self) -> &'static str {
        "calendarinterval"
    }
}

impl CalendarIntervalTrigger {
    fn check_end_date(&self, candidate: DateTime<Utc>) -> Option<DateTime<Utc>> {
        if let Some(end) = self.end_date {
            if candidate > end {
                return None;
            }
        }
        Some(candidate)
    }
}

/// Get the last day of a given month.
fn last_day_of_month(year: i32, month: u32) -> u32 {
    if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    }
    .unwrap()
    .pred_opt()
    .unwrap()
    .day()
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
    fn test_basic_monthly() {
        let trigger =
            CalendarIntervalTrigger::new(0, 1, 0, 0, 10, 0, 0, None, None, "UTC".to_string())
                .unwrap();

        let now = utc(2024, 1, 15, 8, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, utc(2024, 1, 15, 10, 0, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        assert_eq!(second, utc(2024, 2, 15, 10, 0, 0));
    }

    #[test]
    fn test_monthly_day_clamping() {
        // Start on Jan 31, monthly interval.
        let start = utc(2024, 1, 31, 10, 0, 0);
        let trigger = CalendarIntervalTrigger::new(
            0,
            1,
            0,
            0,
            10,
            0,
            0,
            Some(start),
            None,
            "UTC".to_string(),
        )
        .unwrap();

        let now = utc(2024, 1, 31, 8, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, utc(2024, 1, 31, 10, 0, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        // Feb 2024 has 29 days (leap year). Jan 31 clamped to Feb 29.
        assert_eq!(second, utc(2024, 2, 29, 10, 0, 0));

        let third = trigger.get_next_fire_time(Some(second), now).unwrap();
        // Feb 29 + 1 month = Mar 29.
        assert_eq!(third, utc(2024, 3, 29, 10, 0, 0));
    }

    #[test]
    fn test_yearly_leap_year() {
        // Start on Feb 29, 2024. Yearly interval.
        let start = utc(2024, 2, 29, 12, 0, 0);
        let trigger = CalendarIntervalTrigger::new(
            1,
            0,
            0,
            0,
            12,
            0,
            0,
            Some(start),
            None,
            "UTC".to_string(),
        )
        .unwrap();

        let now = utc(2024, 2, 29, 10, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, utc(2024, 2, 29, 12, 0, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        // 2025 is not a leap year, so Feb 29 clamps to Feb 28.
        assert_eq!(second, utc(2025, 2, 28, 12, 0, 0));
    }

    #[test]
    fn test_weekly() {
        let trigger =
            CalendarIntervalTrigger::new(0, 0, 1, 0, 9, 0, 0, None, None, "UTC".to_string())
                .unwrap();

        let now = utc(2024, 3, 1, 8, 0, 0); // Friday
        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, utc(2024, 3, 1, 9, 0, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        assert_eq!(second, utc(2024, 3, 8, 9, 0, 0));
    }

    #[test]
    fn test_end_date() {
        let end = utc(2024, 3, 15, 23, 59, 59);
        let trigger =
            CalendarIntervalTrigger::new(0, 0, 1, 0, 9, 0, 0, None, Some(end), "UTC".to_string())
                .unwrap();

        let now = utc(2024, 3, 1, 8, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        assert_eq!(second, utc(2024, 3, 8, 9, 0, 0));

        let third = trigger.get_next_fire_time(Some(second), now).unwrap();
        assert_eq!(third, utc(2024, 3, 15, 9, 0, 0));

        let fourth = trigger.get_next_fire_time(Some(third), now);
        // 2024-03-22 > end_date.
        assert!(fourth.is_none());
    }

    #[test]
    fn test_past_time_today() {
        // If now is past the fire time for today, should advance to next interval.
        let trigger =
            CalendarIntervalTrigger::new(0, 0, 0, 1, 9, 0, 0, None, None, "UTC".to_string())
                .unwrap();

        let now = utc(2024, 3, 1, 10, 0, 0); // already past 9:00
        let first = trigger.get_next_fire_time(None, now).unwrap();
        assert_eq!(first, utc(2024, 3, 2, 9, 0, 0));
    }

    #[test]
    fn test_dst_spring_forward() {
        // Fire at 2:30 AM ET on March 10, 2024 (spring forward day).
        // 2:30 AM doesn't exist.
        let start = utc(2024, 3, 3, 7, 30, 0); // Mar 3 at 2:30 AM ET
        let trigger = CalendarIntervalTrigger::new(
            0,
            0,
            1,
            0,
            2,
            30,
            0,
            Some(start),
            None,
            "America/New_York".to_string(),
        )
        .unwrap();

        let now = utc(2024, 3, 3, 6, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        // Mar 3 at 2:30 AM ET = 7:30 UTC
        assert_eq!(first, utc(2024, 3, 3, 7, 30, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        // Mar 10 at 2:30 AM ET doesn't exist (spring forward).
        // Should advance to 3:00 AM ET = 7:00 AM UTC.
        assert!(second >= utc(2024, 3, 10, 7, 0, 0));
    }

    #[test]
    fn test_dst_fall_back() {
        // Fire at 1:30 AM ET on Nov 3, 2024 (fall back day).
        // 1:30 AM exists twice; should use first occurrence.
        let start = utc(2024, 10, 27, 5, 30, 0); // Oct 27 at 1:30 AM ET (EDT)
        let trigger = CalendarIntervalTrigger::new(
            0,
            0,
            1,
            0,
            1,
            30,
            0,
            Some(start),
            None,
            "America/New_York".to_string(),
        )
        .unwrap();

        let now = utc(2024, 10, 27, 4, 0, 0);
        let first = trigger.get_next_fire_time(None, now).unwrap();
        // Oct 27 at 1:30 AM EDT = 5:30 UTC
        assert_eq!(first, utc(2024, 10, 27, 5, 30, 0));

        let second = trigger.get_next_fire_time(Some(first), now).unwrap();
        // Nov 3 at 1:30 AM - ambiguous. First occurrence (EDT) = 5:30 UTC.
        assert_eq!(second, utc(2024, 11, 3, 5, 30, 0));
    }

    #[test]
    fn test_zero_interval_rejection() {
        let result =
            CalendarIntervalTrigger::new(0, 0, 0, 0, 9, 0, 0, None, None, "UTC".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_time() {
        let result =
            CalendarIntervalTrigger::new(0, 0, 0, 1, 25, 0, 0, None, None, "UTC".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_describe() {
        let trigger =
            CalendarIntervalTrigger::new(1, 2, 0, 0, 10, 30, 0, None, None, "UTC".to_string())
                .unwrap();
        assert_eq!(
            trigger.describe(),
            "calendarinterval[1 year(s), 2 month(s) at 10:30:00]"
        );
    }

    #[test]
    fn test_serialize_state() {
        let trigger =
            CalendarIntervalTrigger::new(0, 1, 0, 0, 9, 0, 0, None, None, "US/Eastern".to_string())
                .unwrap();
        match trigger.serialize_state() {
            TriggerState::CalendarInterval {
                months,
                hour,
                timezone,
                ..
            } => {
                assert_eq!(months, 1);
                assert_eq!(hour, 9);
                assert_eq!(timezone, "US/Eastern");
            }
            _ => panic!("expected CalendarInterval variant"),
        }
    }

    #[test]
    fn test_trigger_type() {
        let trigger =
            CalendarIntervalTrigger::new(0, 0, 0, 1, 0, 0, 0, None, None, "UTC".to_string())
                .unwrap();
        assert_eq!(trigger.trigger_type(), "calendarinterval");
    }
}
