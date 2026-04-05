use apsched_core::error::TriggerError;

use super::field::FieldMatcher;

/// Parse a single cron field expression into a [`FieldMatcher`].
///
/// Supports:
/// - `*` — all values
/// - `*/n` — every n starting from `min`
/// - `n` — exact value
/// - `n-m` — inclusive range
/// - `n-m/s` — range with step
/// - `a,b,c` — comma-separated (each element may be a value or range)
/// - Named values via the `names` slice (case-insensitive)
///
/// All values must lie within `[min, max]`.
pub fn parse_cron_field(
    expr: &str,
    min: u32,
    max: u32,
    names: Option<&[&str]>,
) -> Result<FieldMatcher, TriggerError> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Err(TriggerError::InvalidCronExpression(
            "empty field expression".to_string(),
        ));
    }

    // Comma-separated list: split and merge.
    if expr.contains(',') {
        let mut matcher = FieldMatcher::new(min, max);
        for part in expr.split(',') {
            let sub = parse_single(part.trim(), min, max, names)?;
            // Merge bits
            for v in min..=max {
                if sub.matches(v) {
                    matcher.set(v);
                }
            }
        }
        return Ok(matcher);
    }

    parse_single(expr, min, max, names)
}

/// Parse a single (non-comma-containing) cron expression.
fn parse_single(
    expr: &str,
    min: u32,
    max: u32,
    names: Option<&[&str]>,
) -> Result<FieldMatcher, TriggerError> {
    // Wildcard
    if expr == "*" {
        return Ok(FieldMatcher::all(min, max));
    }

    // */step
    if let Some(step_str) = expr.strip_prefix("*/") {
        let step = parse_int(step_str, names)?;
        if step == 0 {
            return Err(TriggerError::InvalidCronExpression(
                "step cannot be zero".to_string(),
            ));
        }
        return Ok(FieldMatcher::from_range(min, max, min, max, step));
    }

    // range or range/step: "n-m" or "n-m/s"
    if expr.contains('-') {
        let (range_part, step) = if expr.contains('/') {
            let parts: Vec<&str> = expr.splitn(2, '/').collect();
            let s = parse_int(parts[1], names)?;
            if s == 0 {
                return Err(TriggerError::InvalidCronExpression(
                    "step cannot be zero".to_string(),
                ));
            }
            (parts[0], s)
        } else {
            (expr, 1)
        };

        let range_parts: Vec<&str> = range_part.splitn(2, '-').collect();
        if range_parts.len() != 2 {
            return Err(TriggerError::InvalidCronExpression(format!(
                "invalid range: {}",
                expr
            )));
        }
        let start = parse_int(range_parts[0], names)?;
        let end = parse_int(range_parts[1], names)?;
        validate_range(start, end, min, max, expr)?;
        return Ok(FieldMatcher::from_range(min, max, start, end, step));
    }

    // Single value
    let val = parse_int(expr, names)?;
    if val < min || val > max {
        return Err(TriggerError::InvalidCronExpression(format!(
            "value {} out of range [{}, {}]",
            val, min, max
        )));
    }
    Ok(FieldMatcher::from_values(min, max, &[val]))
}

/// Resolve a token to a u32, checking named values first.
fn parse_int(token: &str, names: Option<&[&str]>) -> Result<u32, TriggerError> {
    let token = token.trim();

    // Try named lookup (case-insensitive).
    if let Some(names) = names {
        let upper = token.to_ascii_uppercase();
        for (i, name) in names.iter().enumerate() {
            if name.to_ascii_uppercase() == upper {
                return Ok(i as u32);
            }
        }
    }

    token
        .parse::<u32>()
        .map_err(|_| TriggerError::InvalidCronExpression(format!("invalid value: {}", token)))
}

fn validate_range(
    start: u32,
    end: u32,
    min: u32,
    max: u32,
    expr: &str,
) -> Result<(), TriggerError> {
    if start > end {
        return Err(TriggerError::InvalidCronExpression(format!(
            "range start > end: {}",
            expr
        )));
    }
    if start < min || end > max {
        return Err(TriggerError::InvalidCronExpression(format!(
            "range [{}, {}] out of bounds [{}, {}]",
            start, end, min, max
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard() {
        let m = parse_cron_field("*", 0, 59, None).unwrap();
        assert!(m.is_all());
    }

    #[test]
    fn test_step() {
        let m = parse_cron_field("*/15", 0, 59, None).unwrap();
        assert!(m.matches(0));
        assert!(m.matches(15));
        assert!(m.matches(30));
        assert!(m.matches(45));
        assert!(!m.matches(1));
    }

    #[test]
    fn test_single_value() {
        let m = parse_cron_field("5", 0, 59, None).unwrap();
        assert!(m.matches(5));
        assert!(!m.matches(0));
        assert!(!m.matches(6));
    }

    #[test]
    fn test_range() {
        let m = parse_cron_field("10-20", 0, 59, None).unwrap();
        for v in 10..=20 {
            assert!(m.matches(v));
        }
        assert!(!m.matches(9));
        assert!(!m.matches(21));
    }

    #[test]
    fn test_range_with_step() {
        let m = parse_cron_field("0-30/10", 0, 59, None).unwrap();
        assert!(m.matches(0));
        assert!(m.matches(10));
        assert!(m.matches(20));
        assert!(m.matches(30));
        assert!(!m.matches(5));
    }

    #[test]
    fn test_comma_list() {
        let m = parse_cron_field("1,5,10-15", 0, 59, None).unwrap();
        assert!(m.matches(1));
        assert!(m.matches(5));
        for v in 10..=15 {
            assert!(m.matches(v));
        }
        assert!(!m.matches(2));
        assert!(!m.matches(16));
    }

    #[test]
    fn test_named_months() {
        // In the actual cron expr, month names are replaced with numbers
        // before parsing (see expr.rs replace_names). Here we test that the
        // parser's name-lookup works for day-of-week style usage where
        // the index directly maps to the value.
        let days = &["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"];
        let m = parse_cron_field("MON-FRI", 0, 6, Some(days)).unwrap();
        for v in 0..=4 {
            assert!(m.matches(v), "expected {} to match", v);
        }
        assert!(!m.matches(5)); // SAT
        assert!(!m.matches(6)); // SUN
    }

    #[test]
    fn test_named_case_insensitive() {
        let days = &["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"];
        let m = parse_cron_field("mon", 0, 6, Some(days)).unwrap();
        assert!(m.matches(0));
        assert!(!m.matches(1));
    }

    #[test]
    fn test_out_of_range() {
        let result = parse_cron_field("60", 0, 59, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_range() {
        let result = parse_cron_field("20-10", 0, 59, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_step() {
        let result = parse_cron_field("*/0", 0, 59, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty() {
        let result = parse_cron_field("", 0, 59, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_token() {
        let result = parse_cron_field("abc", 0, 59, None);
        assert!(result.is_err());
    }
}
