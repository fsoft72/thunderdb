// LIKE operator support - Step 2.4
//
// Pattern matching for SQL LIKE queries

use crate::error::Result;
use crate::storage::Value;

/// LIKE pattern type
#[derive(Debug)]
pub enum LikePattern {
    /// Prefix match: 'abc%' matches strings starting with "abc"
    Prefix(String),

    /// Suffix match: '%abc' matches strings ending with "abc"
    Suffix(String),

    /// Contains: '%abc%' matches strings containing "abc"
    /// Pre-builds a memchr::memmem::Finder for SIMD-accelerated search.
    Contains {
        needle: String,
        finder: memchr::memmem::Finder<'static>,
    },

    /// Exact match: 'abc' (no wildcards)
    Exact(String),

    /// Complex pattern with multiple % and _ wildcards
    Complex(String),
}

impl PartialEq for LikePattern {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Prefix(a), Self::Prefix(b)) => a == b,
            (Self::Suffix(a), Self::Suffix(b)) => a == b,
            (Self::Contains { needle: a, .. }, Self::Contains { needle: b, .. }) => a == b,
            (Self::Exact(a), Self::Exact(b)) => a == b,
            (Self::Complex(a), Self::Complex(b)) => a == b,
            _ => false,
        }
    }
}

impl Clone for LikePattern {
    fn clone(&self) -> Self {
        match self {
            Self::Prefix(s) => Self::Prefix(s.clone()),
            Self::Suffix(s) => Self::Suffix(s.clone()),
            Self::Contains { needle, .. } => Self::new_contains(needle.clone()),
            Self::Exact(s) => Self::Exact(s.clone()),
            Self::Complex(s) => Self::Complex(s.clone()),
        }
    }
}

impl LikePattern {
    /// Build a Contains variant with a pre-constructed SIMD Finder.
    ///
    /// # Safety
    /// The Finder borrows from the needle's heap buffer. String heap data
    /// has a stable address -- moving the struct does not move the heap
    /// allocation -- so the Finder's internal pointer stays valid for the
    /// lifetime of this enum variant.
    fn new_contains(needle: String) -> Self {
        let ptr = needle.as_bytes().as_ptr();
        let len = needle.as_bytes().len();
        // SAFETY: needle is heap-allocated, lives alongside finder in the
        // same enum variant, and is never mutated after construction.
        let static_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
        let finder = memchr::memmem::Finder::new(static_bytes);
        LikePattern::Contains { needle, finder }
    }

    /// Parse a LIKE pattern string
    ///
    /// # Arguments
    /// * `pattern` - SQL LIKE pattern (may contain % and _ wildcards)
    ///
    /// # Returns
    /// Parsed LikePattern variant
    pub fn parse(pattern: &str) -> Result<Self> {
        if pattern.is_empty() {
            return Ok(LikePattern::Exact(String::new()));
        }

        let has_leading_percent = pattern.starts_with('%');
        let has_trailing_percent = pattern.ends_with('%');
        let has_underscore = pattern.contains('_');
        let percent_count = pattern.matches('%').count();

        // Check for complex patterns (multiple % or any _)
        if has_underscore || percent_count > 2 || (percent_count == 2 && (!has_leading_percent || !has_trailing_percent)) {
            return Ok(LikePattern::Complex(pattern.to_string()));
        }

        match (has_leading_percent, has_trailing_percent, percent_count) {
            (true, true, 2) => {
                // %abc%
                let content = &pattern[1..pattern.len() - 1];
                Ok(LikePattern::new_contains(content.to_string()))
            }
            (true, false, 1) => {
                // %abc
                let content = &pattern[1..];
                Ok(LikePattern::Suffix(content.to_string()))
            }
            (false, true, 1) => {
                // abc%
                let content = &pattern[..pattern.len() - 1];
                Ok(LikePattern::Prefix(content.to_string()))
            }
            (false, false, 0) => {
                // abc (no wildcards)
                Ok(LikePattern::Exact(pattern.to_string()))
            }
            _ => Ok(LikePattern::Complex(pattern.to_string())),
        }
    }

    /// Check if a value matches this pattern
    ///
    /// # Arguments
    /// * `value` - Value to test
    ///
    /// # Returns
    /// true if value matches the pattern
    pub fn matches(&self, value: &Value) -> bool {
        match value {
            Value::Varchar(s) => self.matches_string(s.as_str()),
            _ => false, // LIKE only applies to strings
        }
    }

    /// Check if a string matches this pattern
    fn matches_string(&self, s: &str) -> bool {
        match self {
            LikePattern::Exact(pattern) => s.as_bytes() == pattern.as_bytes(),
            LikePattern::Prefix(prefix) => s.as_bytes().starts_with(prefix.as_bytes()),
            LikePattern::Suffix(suffix) => s.as_bytes().ends_with(suffix.as_bytes()),
            LikePattern::Contains { finder, .. } => finder.find(s.as_bytes()).is_some(),
            LikePattern::Complex(pattern) => self.matches_complex(s, pattern),
        }
    }

    /// Match a complex pattern with % and _ wildcards
    ///
    /// % matches zero or more characters
    /// _ matches exactly one character
    fn matches_complex(&self, text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();

        self.match_recursive(&text_chars, 0, &pattern_chars, 0)
    }

    /// Recursive pattern matching algorithm
    fn match_recursive(
        &self,
        text: &[char],
        t_idx: usize,
        pattern: &[char],
        p_idx: usize,
    ) -> bool {
        // If both exhausted, match
        if p_idx >= pattern.len() && t_idx >= text.len() {
            return true;
        }

        // If pattern exhausted but text remains, no match
        if p_idx >= pattern.len() {
            return false;
        }

        let current_pattern = pattern[p_idx];

        match current_pattern {
            '%' => {
                // % can match zero or more characters
                // Try matching zero characters (skip %)
                if self.match_recursive(text, t_idx, pattern, p_idx + 1) {
                    return true;
                }

                // Try matching one or more characters
                for i in t_idx..text.len() {
                    if self.match_recursive(text, i + 1, pattern, p_idx + 1) {
                        return true;
                    }
                }

                false
            }
            '_' => {
                // _ must match exactly one character
                if t_idx >= text.len() {
                    return false;
                }

                self.match_recursive(text, t_idx + 1, pattern, p_idx + 1)
            }
            c => {
                // Regular character - must match exactly
                if t_idx >= text.len() || text[t_idx] != c {
                    return false;
                }

                self.match_recursive(text, t_idx + 1, pattern, p_idx + 1)
            }
        }
    }

    /// Check if this pattern can use B-Tree index optimization
    ///
    /// Returns true for exact match and prefix patterns
    pub fn can_use_index(&self) -> bool {
        matches!(self, LikePattern::Exact(_) | LikePattern::Prefix(_))
    }

    /// Get the prefix for index-based range scan
    ///
    /// Returns Some(prefix) for prefix patterns, None otherwise
    pub fn get_prefix(&self) -> Option<&str> {
        match self {
            LikePattern::Prefix(prefix) => Some(prefix),
            LikePattern::Exact(exact) => Some(exact),
            _ => None,
        }
    }

    /// Get the range bounds for index scanning
    ///
    /// For prefix "abc", returns ("abc", "abd") to scan all strings starting with "abc"
    pub fn get_range_bounds(&self) -> Option<(String, String)> {
        if let Some(prefix) = self.get_prefix() {
            if prefix.is_empty() {
                return None;
            }

            // Calculate the next prefix by incrementing the last character
            let mut end_prefix = prefix.to_string();
            if let Some(last_char) = end_prefix.pop() {
                if let Some(next_char) = char::from_u32(last_char as u32 + 1) {
                    end_prefix.push(next_char);
                    return Some((prefix.to_string(), end_prefix));
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exact() {
        let pattern = LikePattern::parse("hello").unwrap();
        assert_eq!(pattern, LikePattern::Exact("hello".to_string()));
    }

    #[test]
    fn test_parse_prefix() {
        let pattern = LikePattern::parse("hello%").unwrap();
        assert_eq!(pattern, LikePattern::Prefix("hello".to_string()));
    }

    #[test]
    fn test_parse_suffix() {
        let pattern = LikePattern::parse("%world").unwrap();
        assert_eq!(pattern, LikePattern::Suffix("world".to_string()));
    }

    #[test]
    fn test_parse_contains() {
        let pattern = LikePattern::parse("%test%").unwrap();
        assert_eq!(pattern, LikePattern::new_contains("test".to_string()));
    }

    #[test]
    fn test_parse_complex() {
        let pattern = LikePattern::parse("h_llo").unwrap();
        assert_eq!(pattern, LikePattern::Complex("h_llo".to_string()));

        let pattern = LikePattern::parse("a%b%c").unwrap();
        assert_eq!(pattern, LikePattern::Complex("a%b%c".to_string()));
    }

    #[test]
    fn test_match_exact() {
        let pattern = LikePattern::parse("hello").unwrap();

        assert!(pattern.matches(&Value::varchar("hello".to_string())));
        assert!(!pattern.matches(&Value::varchar("hello world".to_string())));
        assert!(!pattern.matches(&Value::varchar("goodbye".to_string())));
    }

    #[test]
    fn test_match_prefix() {
        let pattern = LikePattern::parse("hello%").unwrap();

        assert!(pattern.matches(&Value::varchar("hello".to_string())));
        assert!(pattern.matches(&Value::varchar("hello world".to_string())));
        assert!(pattern.matches(&Value::varchar("hello123".to_string())));
        assert!(!pattern.matches(&Value::varchar("hi hello".to_string())));
        assert!(!pattern.matches(&Value::varchar("goodbye".to_string())));
    }

    #[test]
    fn test_match_suffix() {
        let pattern = LikePattern::parse("%world").unwrap();

        assert!(pattern.matches(&Value::varchar("world".to_string())));
        assert!(pattern.matches(&Value::varchar("hello world".to_string())));
        assert!(pattern.matches(&Value::varchar("my world".to_string())));
        assert!(!pattern.matches(&Value::varchar("world!".to_string())));
        assert!(!pattern.matches(&Value::varchar("goodbye".to_string())));
    }

    #[test]
    fn test_match_contains() {
        let pattern = LikePattern::parse("%test%").unwrap();

        assert!(pattern.matches(&Value::varchar("test".to_string())));
        assert!(pattern.matches(&Value::varchar("this is a test".to_string())));
        assert!(pattern.matches(&Value::varchar("testing 123".to_string())));
        assert!(pattern.matches(&Value::varchar("contest".to_string())));
        assert!(!pattern.matches(&Value::varchar("hello world".to_string())));
    }

    #[test]
    fn test_match_underscore() {
        let pattern = LikePattern::parse("h_llo").unwrap();

        assert!(pattern.matches(&Value::varchar("hello".to_string())));
        assert!(pattern.matches(&Value::varchar("hallo".to_string())));
        assert!(pattern.matches(&Value::varchar("hxllo".to_string())));
        assert!(!pattern.matches(&Value::varchar("hllo".to_string())));
        assert!(!pattern.matches(&Value::varchar("heello".to_string())));
    }

    #[test]
    fn test_match_complex() {
        let pattern = LikePattern::parse("a%b%c").unwrap();

        assert!(pattern.matches(&Value::varchar("abc".to_string())));
        assert!(pattern.matches(&Value::varchar("aXbYc".to_string())));
        assert!(pattern.matches(&Value::varchar("aXXXbYYYc".to_string())));
        assert!(!pattern.matches(&Value::varchar("ab".to_string())));
        assert!(!pattern.matches(&Value::varchar("bc".to_string())));

        let pattern = LikePattern::parse("_a_").unwrap();
        assert!(pattern.matches(&Value::varchar("xay".to_string())));
        assert!(pattern.matches(&Value::varchar("bac".to_string())));
        assert!(!pattern.matches(&Value::varchar("ay".to_string())));
        assert!(!pattern.matches(&Value::varchar("xayz".to_string())));
    }

    #[test]
    fn test_match_non_string() {
        let pattern = LikePattern::parse("hello%").unwrap();

        assert!(!pattern.matches(&Value::Int32(42)));
        assert!(!pattern.matches(&Value::Float64(3.14)));
        assert!(!pattern.matches(&Value::Null));
    }

    #[test]
    fn test_can_use_index() {
        assert!(LikePattern::parse("hello").unwrap().can_use_index());
        assert!(LikePattern::parse("hello%").unwrap().can_use_index());
        assert!(!LikePattern::parse("%hello").unwrap().can_use_index());
        assert!(!LikePattern::parse("%hello%").unwrap().can_use_index());
        assert!(!LikePattern::parse("h_llo").unwrap().can_use_index());
    }

    #[test]
    fn test_get_prefix() {
        let pattern = LikePattern::parse("hello%").unwrap();
        assert_eq!(pattern.get_prefix(), Some("hello"));

        let pattern = LikePattern::parse("hello").unwrap();
        assert_eq!(pattern.get_prefix(), Some("hello"));

        let pattern = LikePattern::parse("%hello").unwrap();
        assert_eq!(pattern.get_prefix(), None);
    }

    #[test]
    fn test_get_range_bounds() {
        let pattern = LikePattern::parse("abc%").unwrap();
        let bounds = pattern.get_range_bounds();
        assert_eq!(bounds, Some(("abc".to_string(), "abd".to_string())));

        let pattern = LikePattern::parse("test%").unwrap();
        let bounds = pattern.get_range_bounds();
        assert_eq!(bounds, Some(("test".to_string(), "tesu".to_string())));

        let pattern = LikePattern::parse("%abc").unwrap();
        assert_eq!(pattern.get_range_bounds(), None);
    }

    #[test]
    fn test_empty_pattern() {
        let pattern = LikePattern::parse("").unwrap();
        assert_eq!(pattern, LikePattern::Exact(String::new()));
        assert!(pattern.matches(&Value::varchar(String::new())));
    }

    #[test]
    fn test_edge_cases() {
        let pattern = LikePattern::parse("%").unwrap();
        assert!(pattern.matches(&Value::varchar("anything".to_string())));
        assert!(pattern.matches(&Value::varchar("".to_string())));

        let pattern = LikePattern::parse("%%").unwrap();
        assert!(pattern.matches(&Value::varchar("anything".to_string())));
    }
}
