use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

use apsched_core::error::TriggerError;

use super::field::FieldMatcher;
use super::parser::parse_cron_field;

/// Month names for parsing (index 1 = JAN).
const MONTH_NAMES: &[&str] = &[
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];

/// Day-of-week names. APScheduler convention: MON=0 .. SUN=6.
const DOW_NAMES: &[&str] = &["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"];

/// Special markers extracted from the `day` field.
///
/// APScheduler supports:
/// - `last` / `L` — last day of the month
/// - `last <weekday>` (e.g. `last sun`) — last occurrence of `weekday` in the month
/// - `<n>W` — nearest weekday (Mon-Fri) to day `n`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DaySpecial {
    /// `last` or `L` — last day of the month.
    LastDay,
    /// `last <weekday>` — last occurrence of weekday (0=MON..6=SUN) in the month.
    LastWeekday(u32),
    /// `<n>W` — nearest weekday to day `n` in the month (stays within month).
    NearestWeekday(u32),
}

/// Special markers extracted from the `day_of_week` field.
///
/// APScheduler supports `<weekday>#<n>` — the nth occurrence of `weekday` in the
/// month (e.g. `6#3` = 3rd Saturday).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NthWeekday {
    /// Weekday (0=MON..6=SUN).
    pub weekday: u32,
    /// Occurrence number in the month (1..=5).
    pub nth: u32,
}

/// A fully compiled cron expression with matchers for every field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledCronExpr {
    pub seconds: FieldMatcher,
    pub minutes: FieldMatcher,
    pub hours: FieldMatcher,
    pub days: FieldMatcher,
    pub months: FieldMatcher,
    pub weekdays: FieldMatcher,
    pub years: Option<FieldMatcher>,
    /// Special day-of-month markers (`last`, `last sun`, `15W`).
    pub day_special: Option<DaySpecial>,
    /// Special day-of-week marker (`6#3`).
    pub nth_weekday: Option<NthWeekday>,
}

impl CompiledCronExpr {
    /// Compile individual cron field expressions into a `CompiledCronExpr`.
    ///
    /// Each parameter is `Option<&str>` — `None` means `"*"` (match all).
    #[allow(clippy::too_many_arguments)]
    pub fn compile(
        year: Option<&str>,
        month: Option<&str>,
        day: Option<&str>,
        _week: Option<&str>,
        day_of_week: Option<&str>,
        hour: Option<&str>,
        minute: Option<&str>,
        second: Option<&str>,
    ) -> Result<Self, TriggerError> {
        let seconds = parse_field(second, 0, 59, None, "second")?;
        let minutes = parse_field(minute, 0, 59, None, "minute")?;
        let hours = parse_field(hour, 0, 23, None, "hour")?;

        // Extract day special markers (`last`, `last sun`, `15W`) before
        // delegating to the generic parser, which does not understand them.
        let (days, day_special) = parse_day_field(day)?;

        // For months, names map: JAN=0 in the parser, but we need JAN=1.
        // We handle this by creating a padded names array where index corresponds
        // to value-1, and add 1 to the result. Instead, let's use a custom approach:
        // parse with min=1,max=12 and use a names list offset by 1.
        let months = parse_month_field(month)?;
        let (weekdays, nth_weekday) = parse_dow_field_with_nth(day_of_week)?;

        let years = match year {
            Some(y) if !y.trim().is_empty() && y.trim() != "*" => {
                Some(parse_field(Some(y), 1970, 2099, None, "year")?)
            }
            _ => None,
        };

        Ok(Self {
            seconds,
            minutes,
            hours,
            days,
            months,
            weekdays,
            years,
            day_special,
            nth_weekday,
        })
    }

    /// Find the next fire time strictly after `after`, in the given timezone.
    ///
    /// Returns `None` if no valid time exists within a 4-year search window.
    pub fn get_next_fire_time(
        &self,
        after: DateTime<Utc>,
        timezone: &str,
    ) -> Option<DateTime<Utc>> {
        let tz: Tz = timezone.parse().ok()?;

        // Convert to local time and start from after + 1 second
        let local = after.with_timezone(&tz);
        let mut dt = local.naive_local() + chrono::Duration::seconds(1);

        // Set a bail-out limit: search up to 4 years.
        let max_year = dt.year() + 4;

        loop {
            // --- Year ---
            if let Some(ref year_matcher) = self.years {
                match year_matcher.next_match(dt.year() as u32) {
                    Some(y) if y as i32 == dt.year() => { /* ok */ }
                    Some(y) => {
                        dt = NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(y as i32, 1, 1)?,
                            NaiveTime::from_hms_opt(0, 0, 0)?,
                        );
                        continue;
                    }
                    None => return None,
                }
            }
            if dt.year() > max_year {
                return None;
            }

            // --- Month ---
            match self.months.next_match(dt.month()) {
                Some(m) if m == dt.month() => { /* ok */ }
                Some(m) => {
                    dt = NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(dt.year(), m, 1)?,
                        NaiveTime::from_hms_opt(0, 0, 0)?,
                    );
                    continue;
                }
                None => {
                    // Roll to next year, January.
                    dt = NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1)?,
                        NaiveTime::from_hms_opt(0, 0, 0)?,
                    );
                    continue;
                }
            }

            // --- Day (of month AND/OR day of week) ---
            // Special markers constrain the corresponding field even if the
            // base matcher is wildcard.
            let days_constrained = !self.days.is_all() || self.day_special.is_some();
            let dow_constrained = !self.weekdays.is_all() || self.nth_weekday.is_some();

            let day_matched = {
                let max_day = last_day_of_month(dt.year(), dt.month());
                match self.find_matching_day(
                    dt.day(),
                    max_day,
                    dt.year(),
                    dt.month(),
                    days_constrained,
                    dow_constrained,
                ) {
                    Some(d) if d == dt.day() => true,
                    Some(d) => {
                        dt = NaiveDateTime::new(
                            NaiveDate::from_ymd_opt(dt.year(), dt.month(), d)?,
                            NaiveTime::from_hms_opt(0, 0, 0)?,
                        );
                        true
                    }
                    None => false,
                }
            };

            if !day_matched {
                if dt.month() == 12 {
                    dt = NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1)?,
                        NaiveTime::from_hms_opt(0, 0, 0)?,
                    );
                } else {
                    dt = NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(dt.year(), dt.month() + 1, 1)?,
                        NaiveTime::from_hms_opt(0, 0, 0)?,
                    );
                }
                continue;
            }

            // --- Hour ---
            match self.hours.next_match(dt.hour()) {
                Some(h) if h == dt.hour() => { /* ok */ }
                Some(h) => {
                    dt = NaiveDateTime::new(dt.date(), NaiveTime::from_hms_opt(h, 0, 0)?);
                    continue;
                }
                None => {
                    // Advance to next day.
                    dt = advance_day(dt)?;
                    continue;
                }
            }

            // --- Minute ---
            match self.minutes.next_match(dt.minute()) {
                Some(m) if m == dt.minute() => { /* ok */ }
                Some(m) => {
                    dt = NaiveDateTime::new(dt.date(), NaiveTime::from_hms_opt(dt.hour(), m, 0)?);
                    continue;
                }
                None => {
                    // Advance to next hour.
                    dt = advance_hour(dt)?;
                    continue;
                }
            }

            // --- Second ---
            match self.seconds.next_match(dt.second()) {
                Some(s) if s == dt.second() => { /* found it */ }
                Some(s) => {
                    dt = NaiveDateTime::new(
                        dt.date(),
                        NaiveTime::from_hms_opt(dt.hour(), dt.minute(), s)?,
                    );
                    continue;
                }
                None => {
                    // Advance to next minute.
                    dt = advance_minute(dt)?;
                    continue;
                }
            }

            // We have a fully matching time. Convert back to UTC, handling DST.
            return naive_to_utc(dt, tz);
        }
    }

    /// Find the next day >= `from_day` in the given month that satisfies the
    /// day-of-month and day-of-week constraints.
    ///
    /// APScheduler/cron semantics: if both are constrained, match EITHER
    /// (union). If only one is constrained, match that one.
    fn find_matching_day(
        &self,
        from_day: u32,
        max_day: u32,
        year: i32,
        month: u32,
        days_constrained: bool,
        dow_constrained: bool,
    ) -> Option<u32> {
        // Pre-compute the day(s) selected by day-of-month specials.
        // `day_special` either resolves to a single day within the month or
        // to no day at all (e.g. `last fri` when no Friday exists — impossible
        // for a real month, but the helper returns None gracefully).
        let special_day = match self.day_special {
            Some(DaySpecial::LastDay) => Some(max_day),
            Some(DaySpecial::LastWeekday(wd)) => last_weekday_of_month(year, month, wd),
            Some(DaySpecial::NearestWeekday(n)) => nearest_weekday(year, month, n, max_day),
            None => None,
        };
        let has_dom_bits = !self.days.is_all();

        // Pre-compute the nth weekday day, if configured.
        let nth_day = self
            .nth_weekday
            .as_ref()
            .and_then(|nw| nth_weekday_of_month(year, month, nw.weekday, nw.nth));
        let has_dow_bits = !self.weekdays.is_all();

        for d in from_day..=max_day {
            let date = NaiveDate::from_ymd_opt(year, month, d)?;
            let dow = apscheduler_weekday(date);

            // Day-of-month match: either from the explicit bit matcher or
            // the special marker. If both are present they union.
            let dom_ok =
                (has_dom_bits && self.days.matches(d)) || special_day.is_some_and(|sd| sd == d);

            // Day-of-week match: either from the explicit bit matcher or
            // the nth-weekday special.
            let dow_ok =
                (has_dow_bits && self.weekdays.matches(dow)) || nth_day.is_some_and(|nd| nd == d);

            let matched = if days_constrained && dow_constrained {
                // Union semantics (standard cron behavior).
                dom_ok || dow_ok
            } else if days_constrained {
                dom_ok
            } else if dow_constrained {
                dow_ok
            } else {
                true // both are wildcard
            };

            if matched {
                return Some(d);
            }
        }
        None
    }
}

/// Find the last occurrence of `weekday` (0=MON..6=SUN) in `year`/`month`.
fn last_weekday_of_month(year: i32, month: u32, weekday: u32) -> Option<u32> {
    let max_day = last_day_of_month(year, month);
    for d in (1..=max_day).rev() {
        let date = NaiveDate::from_ymd_opt(year, month, d)?;
        if apscheduler_weekday(date) == weekday {
            return Some(d);
        }
    }
    None
}

/// Find the nth occurrence (1..=5) of `weekday` (0=MON..6=SUN) in `year`/`month`.
fn nth_weekday_of_month(year: i32, month: u32, weekday: u32, nth: u32) -> Option<u32> {
    let max_day = last_day_of_month(year, month);
    let mut count = 0u32;
    for d in 1..=max_day {
        let date = NaiveDate::from_ymd_opt(year, month, d)?;
        if apscheduler_weekday(date) == weekday {
            count += 1;
            if count == nth {
                return Some(d);
            }
        }
    }
    None
}

/// Find the nearest weekday (Mon-Fri) to day `n` in `year`/`month`.
///
/// APScheduler/quartz semantics: if `n` falls on a weekday, return `n`. If it
/// falls on Saturday, return Friday `n-1` (or the next Monday if that would
/// cross into the previous month). If Sunday, return Monday `n+1` (or the
/// previous Friday if that would cross into the next month).
fn nearest_weekday(year: i32, month: u32, n: u32, max_day: u32) -> Option<u32> {
    if n < 1 || n > max_day {
        return None;
    }
    let date = NaiveDate::from_ymd_opt(year, month, n)?;
    let dow = apscheduler_weekday(date);
    match dow {
        0..=4 => Some(n), // Mon..Fri
        5 => {
            // Saturday: step back to Friday, unless that leaves the month.
            if n >= 2 {
                Some(n - 1)
            } else {
                // n == 1 Saturday: jump forward to Monday (n + 2).
                Some((n + 2).min(max_day))
            }
        }
        6 => {
            // Sunday: step forward to Monday, unless that leaves the month.
            if n < max_day {
                Some(n + 1)
            } else {
                // n is the last day and it's a Sunday: step back to Friday.
                Some(n - 2)
            }
        }
        _ => None,
    }
}

/// Convert APScheduler weekday convention: MON=0, TUE=1, ..., SUN=6.
fn apscheduler_weekday(date: NaiveDate) -> u32 {
    date.weekday().num_days_from_monday()
}

/// Get the last day of a month.
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

/// Advance a NaiveDateTime by one day (reset time to 00:00:00).
fn advance_day(dt: NaiveDateTime) -> Option<NaiveDateTime> {
    let next = dt.date().succ_opt()?;
    Some(NaiveDateTime::new(next, NaiveTime::from_hms_opt(0, 0, 0)?))
}

/// Advance a NaiveDateTime by one hour (reset minutes/seconds to 0).
fn advance_hour(dt: NaiveDateTime) -> Option<NaiveDateTime> {
    if dt.hour() == 23 {
        advance_day(dt)
    } else {
        Some(NaiveDateTime::new(
            dt.date(),
            NaiveTime::from_hms_opt(dt.hour() + 1, 0, 0)?,
        ))
    }
}

/// Advance a NaiveDateTime by one minute (reset seconds to 0).
fn advance_minute(dt: NaiveDateTime) -> Option<NaiveDateTime> {
    if dt.minute() == 59 {
        advance_hour(dt)
    } else {
        Some(NaiveDateTime::new(
            dt.date(),
            NaiveTime::from_hms_opt(dt.hour(), dt.minute() + 1, 0)?,
        ))
    }
}

/// Convert a naive local time to UTC, handling DST gaps and overlaps.
fn naive_to_utc(dt: NaiveDateTime, tz: Tz) -> Option<DateTime<Utc>> {
    use chrono::LocalResult;
    match tz.from_local_datetime(&dt) {
        LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
        LocalResult::Ambiguous(earliest, _latest) => {
            // Fall-back: use the first (earlier) occurrence.
            Some(earliest.with_timezone(&Utc))
        }
        LocalResult::None => {
            // Spring-forward gap: advance to the next valid time.
            // Walk forward second-by-second up to 2 hours.
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

/// Parse a month field, handling JAN-DEC names. Months: 1-12.
fn parse_month_field(expr: Option<&str>) -> Result<FieldMatcher, TriggerError> {
    let expr = expr.unwrap_or("*");
    // Create a names array where index 0 = "JAN" maps to value 1, etc.
    // We achieve this by putting a dummy at the start and using min=1.
    // But parse_int returns the index, so JAN=0, FEB=1...
    // We need JAN=1, so we offset the result.
    // Simplest: don't use the name mapping directly for months, instead
    // pre-process the expression to replace names with numbers.
    let replaced = replace_names(expr, MONTH_NAMES, 1);
    parse_cron_field(&replaced, 1, 12, None)
}

/// Parse a day-of-week field, handling MON-SUN names. DOW: 0-6.
#[allow(dead_code)]
fn parse_dow_field(expr: Option<&str>) -> Result<FieldMatcher, TriggerError> {
    let expr = expr.unwrap_or("*");
    let replaced = replace_names(expr, DOW_NAMES, 0);
    parse_cron_field(&replaced, 0, 6, None)
}

/// Parse a day-of-week field, extracting any `<weekday>#<n>` marker.
///
/// Returns a matcher and an optional `NthWeekday`. If `#` is present the
/// matcher is an empty bitset and the special marker drives selection.
fn parse_dow_field_with_nth(
    expr: Option<&str>,
) -> Result<(FieldMatcher, Option<NthWeekday>), TriggerError> {
    let raw = expr.unwrap_or("*").trim();

    // Detect the `#` marker. APScheduler only supports a single term, not a
    // comma list, for the nth-weekday form.
    if raw.contains('#') {
        let parts: Vec<&str> = raw.splitn(2, '#').collect();
        if parts.len() != 2 {
            return Err(TriggerError::InvalidCronExpression(format!(
                "invalid nth-weekday expression: {}",
                raw
            )));
        }
        let wd_token = replace_names(parts[0].trim(), DOW_NAMES, 0);
        let weekday = wd_token.parse::<u32>().map_err(|_| {
            TriggerError::InvalidCronExpression(format!("invalid weekday in nth: {}", parts[0]))
        })?;
        if weekday > 6 {
            return Err(TriggerError::InvalidCronExpression(format!(
                "weekday out of range in nth: {}",
                weekday
            )));
        }
        let nth = parts[1].trim().parse::<u32>().map_err(|_| {
            TriggerError::InvalidCronExpression(format!(
                "invalid occurrence in nth-weekday: {}",
                parts[1]
            ))
        })?;
        if !(1..=5).contains(&nth) {
            return Err(TriggerError::InvalidCronExpression(format!(
                "nth occurrence must be in 1..=5, got {}",
                nth
            )));
        }
        return Ok((FieldMatcher::new(0, 6), Some(NthWeekday { weekday, nth })));
    }

    let replaced = replace_names(raw, DOW_NAMES, 0);
    let matcher = parse_cron_field(&replaced, 0, 6, None)?;
    Ok((matcher, None))
}

/// Parse a day-of-month field, extracting any special markers
/// (`last`, `L`, `last <weekday>`, `<n>W`).
///
/// Returns a matcher and an optional `DaySpecial`. The matcher is empty when
/// only a special marker is present.
fn parse_day_field(expr: Option<&str>) -> Result<(FieldMatcher, Option<DaySpecial>), TriggerError> {
    let raw = expr.unwrap_or("*").trim();
    let lower = raw.to_ascii_lowercase();

    // `last` or `L` — last day of the month.
    if lower == "last" || lower == "l" {
        return Ok((FieldMatcher::new(1, 31), Some(DaySpecial::LastDay)));
    }

    // `last <weekday>` — e.g. `last sun`, `last 5`, `last FRI`.
    if let Some(rest) = lower.strip_prefix("last ") {
        let token = rest.trim();
        let replaced = replace_names(token, DOW_NAMES, 0);
        let wd = replaced.parse::<u32>().map_err(|_| {
            TriggerError::InvalidCronExpression(format!("invalid weekday in `last`: {}", token))
        })?;
        if wd > 6 {
            return Err(TriggerError::InvalidCronExpression(format!(
                "weekday out of range in `last`: {}",
                wd
            )));
        }
        return Ok((FieldMatcher::new(1, 31), Some(DaySpecial::LastWeekday(wd))));
    }

    // `<n>W` — nearest weekday to day `n`.
    if let Some(num_part) = lower.strip_suffix('w') {
        let n = num_part.trim().parse::<u32>().map_err(|_| {
            TriggerError::InvalidCronExpression(format!("invalid day in `<n>W`: {}", num_part))
        })?;
        if !(1..=31).contains(&n) {
            return Err(TriggerError::InvalidCronExpression(format!(
                "day out of range in `<n>W`: {}",
                n
            )));
        }
        return Ok((
            FieldMatcher::new(1, 31),
            Some(DaySpecial::NearestWeekday(n)),
        ));
    }

    let matcher = parse_cron_field(raw, 1, 31, None)?;
    Ok((matcher, None))
}

/// Parse a generic field.
fn parse_field(
    expr: Option<&str>,
    min: u32,
    max: u32,
    names: Option<&[&str]>,
    _field_name: &str,
) -> Result<FieldMatcher, TriggerError> {
    let expr = expr.unwrap_or("*");
    parse_cron_field(expr, min, max, names)
}

/// Replace all occurrences of name tokens (case-insensitive) with their
/// numeric equivalents. `base` is the numeric value of the first name.
fn replace_names(expr: &str, names: &[&str], base: u32) -> String {
    let mut result = expr.to_string();
    for (i, name) in names.iter().enumerate() {
        let val = base + i as u32;
        // Replace case-insensitively, but only whole tokens.
        // Simple approach: replace uppercase and original case.
        let upper = name.to_ascii_uppercase();
        let lower = name.to_ascii_lowercase();
        // Title case (first upper, rest lower).
        let title: String = {
            let mut c = upper.chars();
            match c.next() {
                Some(first) => first.to_string() + &lower[1..],
                None => String::new(),
            }
        };

        let val_str = val.to_string();
        result = result.replace(&upper, &val_str);
        result = result.replace(&lower, &val_str);
        result = result.replace(&title, &val_str);
    }
    result
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
    fn test_compile_defaults() {
        let expr =
            CompiledCronExpr::compile(None, None, None, None, None, None, None, None).unwrap();
        // Everything should be "all".
        assert!(expr.seconds.is_all());
        assert!(expr.minutes.is_all());
        assert!(expr.hours.is_all());
        assert!(expr.days.is_all());
        assert!(expr.months.is_all());
        assert!(expr.weekdays.is_all());
        assert!(expr.years.is_none());
    }

    #[test]
    fn test_every_minute() {
        let expr =
            CompiledCronExpr::compile(None, None, None, None, None, None, None, Some("0")).unwrap();
        let after = utc(2024, 1, 1, 12, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2024, 1, 1, 12, 1, 0));
    }

    #[test]
    fn test_specific_time() {
        // Every day at 10:30:00
        let expr = CompiledCronExpr::compile(
            None,
            None,
            None,
            None,
            None,
            Some("10"),
            Some("30"),
            Some("0"),
        )
        .unwrap();

        let after = utc(2024, 3, 15, 8, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2024, 3, 15, 10, 30, 0));

        // After 10:30, should go to next day.
        let after2 = utc(2024, 3, 15, 11, 0, 0);
        let next2 = expr.get_next_fire_time(after2, "UTC").unwrap();
        assert_eq!(next2, utc(2024, 3, 16, 10, 30, 0));
    }

    #[test]
    fn test_month_rollover() {
        // January only.
        let expr = CompiledCronExpr::compile(
            None,
            Some("1"),
            Some("1"),
            None,
            None,
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();

        let after = utc(2024, 2, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 0, 0, 0));
    }

    #[test]
    fn test_day_of_week() {
        // Every Monday at 09:00:00.
        let expr = CompiledCronExpr::compile(
            None,
            None,
            None,
            None,
            Some("0"), // MON=0
            Some("9"),
            Some("0"),
            Some("0"),
        )
        .unwrap();

        // 2024-01-01 is a Monday.
        let after = utc(2024, 1, 1, 10, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        // Next Monday is 2024-01-08.
        assert_eq!(next, utc(2024, 1, 8, 9, 0, 0));
    }

    #[test]
    fn test_named_months() {
        let expr = CompiledCronExpr::compile(
            None,
            Some("JAN"),
            Some("1"),
            None,
            None,
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 2, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 1, 1, 0, 0, 0));
    }

    #[test]
    fn test_named_dow() {
        let expr = CompiledCronExpr::compile(
            None,
            None,
            None,
            None,
            Some("FRI"),
            Some("17"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        // 2024-01-05 is a Friday.
        let after = utc(2024, 1, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2024, 1, 5, 17, 0, 0));
    }

    #[test]
    fn test_last_day_of_month_fn() {
        assert_eq!(last_day_of_month(2024, 2), 29); // leap year
        assert_eq!(last_day_of_month(2023, 2), 28);
        assert_eq!(last_day_of_month(2024, 1), 31);
        assert_eq!(last_day_of_month(2024, 4), 30);
        assert_eq!(last_day_of_month(2024, 12), 31);
    }

    #[test]
    fn test_year_constraint() {
        let expr = CompiledCronExpr::compile(
            Some("2025"),
            Some("6"),
            Some("15"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 1, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2025, 6, 15, 12, 0, 0));
    }

    #[test]
    fn test_dst_spring_forward() {
        // US Eastern: spring forward on 2024-03-10 at 2:00 AM -> 3:00 AM
        // If cron is scheduled for 2:30 AM, it should advance to 3:00 AM.
        let expr = CompiledCronExpr::compile(
            None,
            Some("3"),
            Some("10"),
            None,
            None,
            Some("2"),
            Some("30"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 3, 10, 6, 0, 0); // 1:00 AM ET
        let next = expr.get_next_fire_time(after, "America/New_York");
        // 2:30 AM doesn't exist, should advance to 3:00 AM ET = 7:00 AM UTC.
        assert!(next.is_some());
        let next = next.unwrap();
        // The result should be at or after 3:00 AM ET (7:00 AM UTC).
        assert!(next >= utc(2024, 3, 10, 7, 0, 0));
    }

    #[test]
    fn test_dst_fall_back() {
        // US Eastern: fall back on 2024-11-03 at 2:00 AM -> 1:00 AM.
        // 1:30 AM exists twice. Should use the first occurrence.
        let expr = CompiledCronExpr::compile(
            None,
            Some("11"),
            Some("3"),
            None,
            None,
            Some("1"),
            Some("30"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 11, 3, 4, 0, 0); // midnight ET
        let next = expr.get_next_fire_time(after, "America/New_York");
        assert!(next.is_some());
        let next = next.unwrap();
        // First 1:30 AM ET (EDT) = 5:30 AM UTC.
        assert_eq!(next, utc(2024, 11, 3, 5, 30, 0));
    }

    #[test]
    fn test_no_match_returns_none() {
        // Year 2020 only, but we're past 2020.
        let expr =
            CompiledCronExpr::compile(Some("2020"), None, None, None, None, None, None, None)
                .unwrap();
        let after = utc(2024, 1, 1, 0, 0, 0);
        assert!(expr.get_next_fire_time(after, "UTC").is_none());
    }

    #[test]
    fn test_dst_fall_back_2026_no_infinite_loop() {
        // Bug fix test: DST fall-back on 2026-11-01 at 2:00 AM America/New_York
        // Clocks go back from 2:00 AM EDT to 1:00 AM EST.
        // Cron: second=0, minute=0, hour=1 should fire at 1:00 AM (the first occurrence).
        let expr = CompiledCronExpr::compile(
            None,
            Some("11"),
            Some("1"),
            None,
            None,
            Some("1"),
            Some("0"),
            Some("0"),
        )
        .unwrap();

        // Start from before 1:00 AM on the fall-back day.
        // 2026-11-01 04:00:00 UTC = 00:00:00 EDT (midnight).
        let after = utc(2026, 11, 1, 4, 0, 0);
        let next = expr.get_next_fire_time(after, "America/New_York");
        assert!(next.is_some(), "should find a fire time during fall-back");
        let next = next.unwrap();
        // 1:00 AM EDT = 05:00 UTC (first occurrence, before fall-back).
        assert_eq!(next, utc(2026, 11, 1, 5, 0, 0));

        // Now call again from just after the first 1:00 AM occurrence.
        // This tests that we don't get stuck in an infinite loop returning
        // the same time. The next fire should be next year (2027-11-01).
        let next2 = expr.get_next_fire_time(next, "America/New_York");
        assert!(next2.is_some(), "should find next year's fire time");
        let next2 = next2.unwrap();
        assert!(
            next2 > next,
            "must strictly advance past the ambiguous time"
        );
        // Should be 2027-11-07 (first Sunday of November 2027... actually Nov 1 2027
        // is a Monday. Fall-back is first Sunday of November.)
        // In 2027, fall-back is Nov 7. But our cron matches day=1, so it's 2027-11-01.
        // 2027-11-01 is before fall-back, so 1:00 AM EDT = 05:00 UTC.
        assert_eq!(next2, utc(2027, 11, 1, 5, 0, 0));
    }

    #[test]
    fn test_get_next_fire_time_always_strictly_advances() {
        // Bug fix test: ensure get_next_fire_time never returns the same time as `after`.
        let expr = CompiledCronExpr::compile(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("0"), // every minute at second=0
        )
        .unwrap();

        let t = utc(2024, 6, 15, 12, 0, 0);
        let next = expr.get_next_fire_time(t, "UTC").unwrap();
        assert!(next > t, "next fire time must be strictly after `after`");
    }

    // ---------- L / W / # special markers ----------

    #[test]
    fn test_last_day_of_month_feb_leap() {
        // `day="last"` in Feb 2024 (leap year) -> Feb 29.
        let expr = CompiledCronExpr::compile(
            None,
            Some("2"),
            Some("last"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 2, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2024, 2, 29, 12, 0, 0));
    }

    #[test]
    fn test_last_day_of_month_feb_non_leap() {
        // `day="L"` in Feb 2023 -> Feb 28.
        let expr = CompiledCronExpr::compile(
            None,
            Some("2"),
            Some("L"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2023, 2, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2023, 2, 28, 12, 0, 0));
    }

    #[test]
    fn test_last_day_rolls_across_months() {
        // Every month last day. After Feb 28 2023 -> Mar 31.
        let expr = CompiledCronExpr::compile(
            None,
            None,
            Some("last"),
            None,
            None,
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2023, 2, 28, 12, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2023, 3, 31, 0, 0, 0));
    }

    #[test]
    fn test_last_weekday_last_sunday() {
        // `day="last sun"` in June 2024.
        // 2024-06-30 is a Sunday, so it's the last Sunday.
        let expr = CompiledCronExpr::compile(
            None,
            Some("6"),
            Some("last sun"),
            None,
            None,
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2024, 6, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2024, 6, 30, 0, 0, 0));
    }

    #[test]
    fn test_last_weekday_last_friday_named() {
        // Last Friday of January 2026. Jan 30 2026 is a Friday.
        let expr = CompiledCronExpr::compile(
            None,
            Some("1"),
            Some("last fri"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 1, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 1, 30, 12, 0, 0));
    }

    #[test]
    fn test_nearest_weekday_15w_saturday() {
        // 15W in August 2026: Aug 15 2026 is a Saturday -> Friday Aug 14.
        let expr = CompiledCronExpr::compile(
            None,
            Some("8"),
            Some("15W"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 8, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 8, 14, 12, 0, 0));
    }

    #[test]
    fn test_nearest_weekday_15w_sunday() {
        // 15W in February 2026: Feb 15 2026 is a Sunday -> Monday Feb 16.
        let expr = CompiledCronExpr::compile(
            None,
            Some("2"),
            Some("15W"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 2, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 2, 16, 12, 0, 0));
    }

    #[test]
    fn test_nearest_weekday_15w_weekday() {
        // 15W in Sept 2026: Sept 15 2026 is a Tuesday -> Sept 15 itself.
        let expr = CompiledCronExpr::compile(
            None,
            Some("9"),
            Some("15W"),
            None,
            None,
            Some("12"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 9, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 9, 15, 12, 0, 0));
    }

    #[test]
    fn test_nearest_weekday_1w_saturday_jumps_forward() {
        // 1W: if day 1 is Saturday, normally we'd step back to Friday, but
        // that would cross into the previous month; jump forward to Monday.
        // August 2026: Aug 1 is a Saturday -> Monday Aug 3.
        let expr = CompiledCronExpr::compile(
            None,
            Some("8"),
            Some("1W"),
            None,
            None,
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 7, 31, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 8, 3, 0, 0, 0));
    }

    #[test]
    fn test_nth_weekday_third_saturday() {
        // APScheduler DOW: MON=0..SUN=6, so Saturday = 5.
        // `day_of_week="5#3"` — third Saturday of January 2026.
        // Jan 2026 Saturdays: 3, 10, 17, 24, 31 -> third = Jan 17.
        let expr = CompiledCronExpr::compile(
            None,
            Some("1"),
            None,
            None,
            Some("5#3"),
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 1, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 1, 17, 0, 0, 0));
    }

    #[test]
    fn test_nth_weekday_third_sunday_via_6() {
        // `6#3` — third Sunday of January 2026.
        // Jan 2026 Sundays: 4, 11, 18, 25 -> third = Jan 18.
        let expr = CompiledCronExpr::compile(
            None,
            Some("1"),
            None,
            None,
            Some("6#3"),
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 1, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 1, 18, 0, 0, 0));
    }

    #[test]
    fn test_nth_weekday_rolls_to_next_month() {
        // `sat#3` starting after the third Saturday should roll to next month.
        let expr = CompiledCronExpr::compile(
            None,
            None,
            None,
            None,
            Some("sat#3"),
            Some("0"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        // Start after third Sat of Jan 2026 (Jan 17).
        let after = utc(2026, 1, 18, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        // Feb 2026 Saturdays: 7, 14, 21, 28 -> third = Feb 21.
        assert_eq!(next, utc(2026, 2, 21, 0, 0, 0));
    }

    #[test]
    fn test_nth_weekday_second_monday() {
        // `0#2` — second Monday of March 2026. Mondays: 2, 9, 16, 23, 30 -> Mar 9.
        let expr = CompiledCronExpr::compile(
            None,
            Some("3"),
            None,
            None,
            Some("0#2"),
            Some("9"),
            Some("0"),
            Some("0"),
        )
        .unwrap();
        let after = utc(2026, 3, 1, 0, 0, 0);
        let next = expr.get_next_fire_time(after, "UTC").unwrap();
        assert_eq!(next, utc(2026, 3, 9, 9, 0, 0));
    }

    #[test]
    fn test_invalid_nth_range() {
        // #6 is out of range (1..=5).
        let result =
            CompiledCronExpr::compile(None, None, None, None, Some("0#6"), None, None, Some("0"));
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_w_range() {
        let result =
            CompiledCronExpr::compile(None, None, Some("32W"), None, None, None, None, Some("0"));
        assert!(result.is_err());
    }

    #[test]
    fn test_last_weekday_helper_fn() {
        // 2024-06-30 is a Sunday.
        assert_eq!(last_weekday_of_month(2024, 6, 6), Some(30));
        // 2024-06-28 is a Friday (last Friday in June 2024).
        assert_eq!(last_weekday_of_month(2024, 6, 4), Some(28));
    }

    #[test]
    fn test_nth_weekday_helper_fn() {
        // 3rd Saturday of Jan 2026 = Jan 17.
        assert_eq!(nth_weekday_of_month(2026, 1, 5, 3), Some(17));
        // 5th Saturday of Jan 2026 = Jan 31.
        assert_eq!(nth_weekday_of_month(2026, 1, 5, 5), Some(31));
        // 6th Saturday doesn't exist.
        assert_eq!(nth_weekday_of_month(2026, 1, 5, 6), None);
    }

    #[test]
    fn test_dst_fall_back_repeated_calls_advance() {
        // Bug fix test: repeated calls during DST fall-back should always advance.
        let expr = CompiledCronExpr::compile(
            None,
            Some("11"),
            Some("3"),
            None,
            None,
            Some("1"),
            Some("30"),
            Some("0"),
        )
        .unwrap();

        // 2024-11-03 fall-back at 2:00 AM. 1:30 AM exists twice.
        let t1 = utc(2024, 11, 3, 4, 0, 0); // midnight ET
        let next1 = expr.get_next_fire_time(t1, "America/New_York").unwrap();
        // Call again with the result -- should NOT return the same time.
        let next2 = expr.get_next_fire_time(next1, "America/New_York");
        // next2 should either be None (date trigger) or a future time
        if let Some(n2) = next2 {
            assert!(n2 > next1, "repeated call must strictly advance");
        }
    }
}
