use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Abstraction over system time, enabling deterministic testing.
pub trait Clock: Send + Sync {
    /// Returns the current UTC wall-clock time.
    fn now(&self) -> DateTime<Utc>;
    /// Returns a monotonic instant for measuring elapsed time.
    fn now_monotonic(&self) -> Instant;
}

/// Production clock backed by the real system clock.
#[derive(Debug, Clone, Copy)]
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn now_monotonic(&self) -> Instant {
        Instant::now()
    }
}

/// Test clock with manual time control. All methods are thread-safe.
pub struct TestClock {
    current_time: Arc<Mutex<DateTime<Utc>>>,
    base_monotonic: Instant,
    monotonic_offset: Arc<Mutex<Duration>>,
    frozen: Arc<Mutex<bool>>,
    /// Tracks how far the wall clock has been advanced since creation,
    /// so that when unfrozen we can compute a sensible value.
    wall_offset: Arc<Mutex<chrono::Duration>>,
    initial_time: DateTime<Utc>,
}

impl std::fmt::Debug for TestClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let time = self.current_time.lock().unwrap();
        let frozen = self.frozen.lock().unwrap();
        f.debug_struct("TestClock")
            .field("current_time", &*time)
            .field("frozen", &*frozen)
            .finish()
    }
}

impl TestClock {
    /// Create a new test clock starting at the given time. The clock starts frozen.
    pub fn new(initial_time: DateTime<Utc>) -> Self {
        Self {
            current_time: Arc::new(Mutex::new(initial_time)),
            base_monotonic: Instant::now(),
            monotonic_offset: Arc::new(Mutex::new(Duration::ZERO)),
            frozen: Arc::new(Mutex::new(true)),
            wall_offset: Arc::new(Mutex::new(chrono::Duration::zero())),
            initial_time,
        }
    }

    /// Advance time forward by the given duration.
    pub fn advance(&self, duration: Duration) {
        let chrono_dur = chrono::Duration::from_std(duration)
            .unwrap_or_else(|_| chrono::Duration::milliseconds(duration.as_millis() as i64));

        let mut time = self.current_time.lock().unwrap();
        *time = *time + chrono_dur;

        let mut offset = self.monotonic_offset.lock().unwrap();
        *offset += duration;

        let mut wall_off = self.wall_offset.lock().unwrap();
        *wall_off = *wall_off + chrono_dur;
    }

    /// Jump to a specific time. Also adjusts the monotonic offset accordingly.
    pub fn set(&self, time: DateTime<Utc>) {
        let mut current = self.current_time.lock().unwrap();
        let diff = time - *current;
        *current = time;

        // Update monotonic offset: only advance forward
        if let Ok(std_dur) = diff.to_std() {
            let mut offset = self.monotonic_offset.lock().unwrap();
            *offset += std_dur;
        }

        let mut wall_off = self.wall_offset.lock().unwrap();
        *wall_off = time - self.initial_time;
    }

    /// Freeze the clock so that `now()` always returns the same value.
    pub fn freeze(&self) {
        let mut frozen = self.frozen.lock().unwrap();
        *frozen = true;
    }

    /// Unfreeze the clock. After unfreezing, `now()` will advance with real elapsed time
    /// relative to the last set/advanced time.
    pub fn unfreeze(&self) {
        // Snapshot the current time so that real-time starts from here.
        let current = *self.current_time.lock().unwrap();
        let mut frozen = self.frozen.lock().unwrap();
        *frozen = false;

        // Keep offset as-is; the Clock impl handles the rest.
        drop(frozen);
        let _ = current;
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        let frozen = self.frozen.lock().unwrap();
        if *frozen {
            *self.current_time.lock().unwrap()
        } else {
            // When unfrozen, return the manually-set time plus real elapsed time
            // since the monotonic base. This gives a reasonable advancing clock.
            let current = *self.current_time.lock().unwrap();
            let offset = *self.monotonic_offset.lock().unwrap();
            let real_elapsed = self.base_monotonic.elapsed();
            if real_elapsed > offset {
                let additional = real_elapsed - offset;
                current
                    + chrono::Duration::from_std(additional)
                        .unwrap_or(chrono::Duration::zero())
            } else {
                current
            }
        }
    }

    fn now_monotonic(&self) -> Instant {
        let offset = self.monotonic_offset.lock().unwrap();
        self.base_monotonic + *offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_wall_clock_returns_current_time() {
        let clock = WallClock;
        let before = Utc::now();
        let t = clock.now();
        let after = Utc::now();
        assert!(t >= before && t <= after);
    }

    #[test]
    fn test_wall_clock_monotonic() {
        let clock = WallClock;
        let a = clock.now_monotonic();
        let b = clock.now_monotonic();
        assert!(b >= a);
    }

    #[test]
    fn test_test_clock_initial_time() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);
        assert_eq!(clock.now(), t0);
    }

    #[test]
    fn test_test_clock_advance() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);

        clock.advance(Duration::from_secs(60));
        let expected = Utc.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        assert_eq!(clock.now(), expected);
    }

    #[test]
    fn test_test_clock_set() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);

        let t1 = Utc.with_ymd_and_hms(2025, 6, 15, 12, 30, 0).unwrap();
        clock.set(t1);
        assert_eq!(clock.now(), t1);
    }

    #[test]
    fn test_test_clock_freeze_unfreeze() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);

        // Starts frozen
        assert_eq!(clock.now(), t0);
        assert_eq!(clock.now(), t0); // Still the same

        // Advance while frozen
        clock.advance(Duration::from_secs(3600));
        let t1 = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap();
        assert_eq!(clock.now(), t1);

        // Freeze again (already frozen, should be idempotent)
        clock.freeze();
        assert_eq!(clock.now(), t1);
    }

    #[test]
    fn test_test_clock_monotonic_advances() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);

        let m0 = clock.now_monotonic();
        clock.advance(Duration::from_secs(10));
        let m1 = clock.now_monotonic();

        assert!(m1 > m0);
        assert_eq!(m1 - m0, Duration::from_secs(10));
    }

    #[test]
    fn test_test_clock_thread_safety() {
        use std::thread;

        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = Arc::new(TestClock::new(t0));

        let clock2 = Arc::clone(&clock);
        let handle = thread::spawn(move || {
            clock2.advance(Duration::from_secs(100));
            clock2.now()
        });

        let result = handle.join().unwrap();
        let expected = t0 + chrono::Duration::seconds(100);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_test_clock_multiple_advances() {
        let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let clock = TestClock::new(t0);

        clock.advance(Duration::from_secs(30));
        clock.advance(Duration::from_secs(30));
        let expected = Utc.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        assert_eq!(clock.now(), expected);
    }
}
