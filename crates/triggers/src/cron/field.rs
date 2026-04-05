use serde::{Deserialize, Serialize};

/// Maximum number of bits (supports year range 1970-2099 = 130 values).
const WORDS: usize = 3; // 3 * 64 = 192 bits

/// Bitfield-based matcher for a single cron field.
///
/// Stores allowed values as bits in a fixed-size array, giving O(1)
/// membership checks. `min` and `max` define the valid range for this
/// field (e.g. 0..=59 for seconds, 1970..=2099 for years).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldMatcher {
    bits: [u64; WORDS],
    pub min: u32,
    pub max: u32,
}

impl FieldMatcher {
    /// Create a matcher with no values set.
    pub fn new(min: u32, max: u32) -> Self {
        debug_assert!(max >= min);
        debug_assert!((max - min + 1) as usize <= WORDS * 64);
        Self {
            bits: [0; WORDS],
            min,
            max,
        }
    }

    /// Create a matcher where every value in `[min, max]` is allowed.
    pub fn all(min: u32, max: u32) -> Self {
        let mut m = Self::new(min, max);
        let count = (max - min + 1) as usize;
        // Fill complete words.
        let full_words = count / 64;
        for w in m.bits.iter_mut().take(full_words) {
            *w = u64::MAX;
        }
        // Remaining bits in the last partial word.
        let remaining = count % 64;
        if remaining > 0 && full_words < WORDS {
            m.bits[full_words] = (1u64 << remaining) - 1;
        }
        m
    }

    /// Set a single value as allowed.
    pub fn set(&mut self, value: u32) {
        debug_assert!(value >= self.min && value <= self.max);
        let offset = (value - self.min) as usize;
        let word = offset / 64;
        let bit = offset % 64;
        self.bits[word] |= 1u64 << bit;
    }

    /// Check whether `value` is in the allowed set.
    pub fn matches(&self, value: u32) -> bool {
        if value < self.min || value > self.max {
            return false;
        }
        let offset = (value - self.min) as usize;
        let word = offset / 64;
        let bit = offset % 64;
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Find the smallest allowed value `>= from`. Returns `None` if no match
    /// exists in `[from, max]`.
    pub fn next_match(&self, from: u32) -> Option<u32> {
        if from > self.max {
            return None;
        }
        let start = from.max(self.min);
        let offset = (start - self.min) as usize;
        let start_word = offset / 64;
        let start_bit = offset % 64;

        // Check the first (partial) word.
        let masked = self.bits[start_word] >> start_bit;
        if masked != 0 {
            let result = start + masked.trailing_zeros();
            if result <= self.max {
                return Some(result);
            }
            return None;
        }

        // Check subsequent full words.
        let base = self.min + ((start_word + 1) * 64) as u32;
        for w in (start_word + 1)..WORDS {
            if self.bits[w] != 0 {
                let result =
                    base + ((w - start_word - 1) * 64) as u32 + self.bits[w].trailing_zeros();
                if result <= self.max {
                    return Some(result);
                }
                return None;
            }
        }
        None
    }

    /// The smallest allowed value, or `None` if no values are set.
    pub fn first_match(&self) -> Option<u32> {
        for (w, &word) in self.bits.iter().enumerate() {
            if word != 0 {
                let result = self.min + (w * 64) as u32 + word.trailing_zeros();
                if result <= self.max {
                    return Some(result);
                }
                return None;
            }
        }
        None
    }

    /// True if every value in `[min, max]` is allowed.
    pub fn is_all(&self) -> bool {
        *self == Self::all(self.min, self.max)
    }

    /// Build a matcher from a range `[start, end]` with a step.
    pub fn from_range(min: u32, max: u32, start: u32, end: u32, step: u32) -> Self {
        let mut m = Self::new(min, max);
        let mut v = start;
        while v <= end {
            m.set(v);
            if step == 0 {
                break;
            }
            v += step;
        }
        m
    }

    /// Build a matcher from an explicit set of values.
    pub fn from_values(min: u32, max: u32, values: &[u32]) -> Self {
        let mut m = Self::new(min, max);
        for &v in values {
            m.set(v);
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_empty() {
        let m = FieldMatcher::new(0, 59);
        assert!(!m.matches(0));
        assert!(!m.matches(30));
        assert_eq!(m.first_match(), None);
    }

    #[test]
    fn test_all() {
        let m = FieldMatcher::all(1, 12);
        for v in 1..=12 {
            assert!(m.matches(v));
        }
        assert!(!m.matches(0));
        assert!(!m.matches(13));
        assert!(m.is_all());
    }

    #[test]
    fn test_set_and_matches() {
        let mut m = FieldMatcher::new(0, 59);
        m.set(0);
        m.set(15);
        m.set(30);
        m.set(59);
        assert!(m.matches(0));
        assert!(m.matches(15));
        assert!(m.matches(30));
        assert!(m.matches(59));
        assert!(!m.matches(1));
        assert!(!m.matches(58));
    }

    #[test]
    fn test_next_match() {
        let mut m = FieldMatcher::new(0, 59);
        m.set(5);
        m.set(15);
        m.set(45);

        assert_eq!(m.next_match(0), Some(5));
        assert_eq!(m.next_match(5), Some(5));
        assert_eq!(m.next_match(6), Some(15));
        assert_eq!(m.next_match(16), Some(45));
        assert_eq!(m.next_match(46), None);
    }

    #[test]
    fn test_first_match() {
        let m = FieldMatcher::from_values(0, 59, &[10, 20, 30]);
        assert_eq!(m.first_match(), Some(10));
    }

    #[test]
    fn test_from_range() {
        let m = FieldMatcher::from_range(0, 59, 0, 59, 15);
        assert!(m.matches(0));
        assert!(m.matches(15));
        assert!(m.matches(30));
        assert!(m.matches(45));
        assert!(!m.matches(1));
        assert!(!m.matches(59));
    }

    #[test]
    fn test_from_values() {
        let m = FieldMatcher::from_values(1, 31, &[1, 15, 31]);
        assert!(m.matches(1));
        assert!(m.matches(15));
        assert!(m.matches(31));
        assert!(!m.matches(2));
    }

    #[test]
    fn test_is_all_false() {
        let m = FieldMatcher::from_values(0, 59, &[0, 30]);
        assert!(!m.is_all());
    }

    #[test]
    fn test_next_match_from_below_min() {
        let m = FieldMatcher::from_values(5, 10, &[7, 9]);
        assert_eq!(m.next_match(0), Some(7));
        assert_eq!(m.next_match(5), Some(7));
        assert_eq!(m.next_match(8), Some(9));
        assert_eq!(m.next_match(10), None);
    }

    #[test]
    fn test_wide_range_years() {
        // Year range: 1970-2099 (130 values).
        let m = FieldMatcher::all(1970, 2099);
        assert!(m.matches(1970));
        assert!(m.matches(2024));
        assert!(m.matches(2099));
        assert!(!m.matches(1969));
        assert!(!m.matches(2100));
        assert!(m.is_all());

        let m2 = FieldMatcher::from_values(1970, 2099, &[2025]);
        assert!(m2.matches(2025));
        assert!(!m2.matches(2024));
        assert_eq!(m2.next_match(2020), Some(2025));
        assert_eq!(m2.next_match(2026), None);
    }
}
