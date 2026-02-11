// SmallString optimization - P2.3
//
// Inline storage for strings <= 23 bytes, avoiding heap allocation.
// Most database column values (names, statuses, short IDs) fit inline.

use std::fmt;

/// Maximum inline capacity in bytes
const INLINE_CAP: usize = 23;

/// A string type that stores short strings inline (on the stack) and
/// falls back to heap allocation for longer strings.
///
/// Strings up to 23 bytes are stored in a fixed-size array with zero
/// heap allocation. Longer strings use a standard `String`.
#[derive(Clone)]
pub enum SmallString {
    /// Inline storage for strings <= 23 bytes
    Inline {
        data: [u8; INLINE_CAP],
        len: u8,
    },
    /// Heap-allocated fallback for longer strings
    Heap(String),
}

impl SmallString {
    /// Create from a string slice
    pub fn new(s: &str) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            SmallString::Inline {
                data,
                len: s.len() as u8,
            }
        } else {
            SmallString::Heap(s.to_string())
        }
    }

    /// Create from an owned String (avoids copy if heap-allocated)
    pub fn from_string(s: String) -> Self {
        if s.len() <= INLINE_CAP {
            let mut data = [0u8; INLINE_CAP];
            data[..s.len()].copy_from_slice(s.as_bytes());
            SmallString::Inline {
                data,
                len: s.len() as u8,
            }
        } else {
            SmallString::Heap(s)
        }
    }

    /// Create from a UTF-8 byte slice
    pub fn from_utf8(bytes: &[u8]) -> Result<Self, std::str::Utf8Error> {
        let s = std::str::from_utf8(bytes)?;
        Ok(Self::new(s))
    }

    /// Get as string slice
    pub fn as_str(&self) -> &str {
        match self {
            SmallString::Inline { data, len } => {
                // Safety: we only store valid UTF-8
                unsafe { std::str::from_utf8_unchecked(&data[..*len as usize]) }
            }
            SmallString::Heap(s) => s.as_str(),
        }
    }

    /// Get length in bytes
    pub fn len(&self) -> usize {
        match self {
            SmallString::Inline { len, .. } => *len as usize,
            SmallString::Heap(s) => s.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get as byte slice
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SmallString::Inline { data, len } => &data[..*len as usize],
            SmallString::Heap(s) => s.as_bytes(),
        }
    }

    /// Check if stored inline
    pub fn is_inline(&self) -> bool {
        matches!(self, SmallString::Inline { .. })
    }
}

impl fmt::Debug for SmallString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SmallString({:?})", self.as_str())
    }
}

impl fmt::Display for SmallString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq for SmallString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for SmallString {}

impl PartialOrd for SmallString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SmallString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for SmallString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl From<String> for SmallString {
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

impl From<&str> for SmallString {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_short() {
        let s = SmallString::new("hello");
        assert!(s.is_inline());
        assert_eq!(s.as_str(), "hello");
        assert_eq!(s.len(), 5);
        assert!(!s.is_empty());
    }

    #[test]
    fn test_inline_exactly_23() {
        let s = SmallString::new("12345678901234567890123"); // 23 bytes
        assert!(s.is_inline());
        assert_eq!(s.len(), 23);
        assert_eq!(s.as_str(), "12345678901234567890123");
    }

    #[test]
    fn test_heap_24_bytes() {
        let s = SmallString::new("123456789012345678901234"); // 24 bytes
        assert!(!s.is_inline());
        assert_eq!(s.len(), 24);
        assert_eq!(s.as_str(), "123456789012345678901234");
    }

    #[test]
    fn test_empty() {
        let s = SmallString::new("");
        assert!(s.is_inline());
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.as_str(), "");
    }

    #[test]
    fn test_from_string() {
        let s = SmallString::from_string("test".to_string());
        assert!(s.is_inline());
        assert_eq!(s.as_str(), "test");

        let long = SmallString::from_string("x".repeat(100));
        assert!(!long.is_inline());
        assert_eq!(long.len(), 100);
    }

    #[test]
    fn test_from_utf8_valid() {
        let s = SmallString::from_utf8(b"hello").unwrap();
        assert_eq!(s.as_str(), "hello");
    }

    #[test]
    fn test_from_utf8_invalid() {
        let result = SmallString::from_utf8(&[0xff, 0xfe]);
        assert!(result.is_err());
    }

    #[test]
    fn test_as_bytes() {
        let s = SmallString::new("abc");
        assert_eq!(s.as_bytes(), b"abc");
    }

    #[test]
    fn test_clone() {
        let s1 = SmallString::new("hello");
        let s2 = s1.clone();
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_equality() {
        let a = SmallString::new("hello");
        let b = SmallString::new("hello");
        let c = SmallString::new("world");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_ordering() {
        let a = SmallString::new("alice");
        let b = SmallString::new("bob");
        assert!(a < b);
        assert!(b > a);
    }

    #[test]
    fn test_display() {
        let s = SmallString::new("test");
        assert_eq!(format!("{}", s), "test");
    }

    #[test]
    fn test_from_trait() {
        let s: SmallString = "hello".into();
        assert_eq!(s.as_str(), "hello");

        let s: SmallString = String::from("world").into();
        assert_eq!(s.as_str(), "world");
    }

    #[test]
    fn test_unicode() {
        // Japanese "konnichiwa" — 15 bytes UTF-8 (fits inline)
        let s = SmallString::new("こんにちは");
        assert!(s.is_inline());
        assert_eq!(s.as_str(), "こんにちは");

        // Emoji string that exceeds 23 bytes
        let long_emoji = "🌍🌎🌏🌍🌎🌏"; // 6 * 4 = 24 bytes
        let s = SmallString::new(long_emoji);
        assert!(!s.is_inline());
        assert_eq!(s.as_str(), long_emoji);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(SmallString::new("hello"));
        set.insert(SmallString::new("hello"));
        set.insert(SmallString::new("world"));
        assert_eq!(set.len(), 2);
    }
}
