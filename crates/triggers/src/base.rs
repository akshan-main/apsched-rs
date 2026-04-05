pub use apsched_core::traits::Trigger;

use chrono::{DateTime, Duration, Utc};
use rand::Rng;

/// Apply a random jitter offset to a fire time.
///
/// The offset is drawn uniformly from `[0, jitter_secs]` and added to the base
/// time, so the result is never *before* `fire_time`.
pub fn apply_jitter(
    fire_time: DateTime<Utc>,
    jitter_secs: f64,
    rng: &mut impl Rng,
) -> DateTime<Utc> {
    if jitter_secs <= 0.0 {
        return fire_time;
    }
    let offset_ms = rng.gen_range(0..=(jitter_secs * 1000.0) as i64);
    fire_time + Duration::milliseconds(offset_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_apply_jitter_within_bounds() {
        let base = Utc::now();
        let jitter = 10.0;
        let mut rng = StdRng::seed_from_u64(42);

        for _ in 0..100 {
            let result = apply_jitter(base, jitter, &mut rng);
            assert!(result >= base);
            assert!(result <= base + Duration::milliseconds((jitter * 1000.0) as i64));
        }
    }

    #[test]
    fn test_apply_jitter_zero() {
        let base = Utc::now();
        let mut rng = StdRng::seed_from_u64(0);
        let result = apply_jitter(base, 0.0, &mut rng);
        assert_eq!(result, base);
    }

    #[test]
    fn test_apply_jitter_negative() {
        let base = Utc::now();
        let mut rng = StdRng::seed_from_u64(0);
        let result = apply_jitter(base, -5.0, &mut rng);
        assert_eq!(result, base);
    }
}
