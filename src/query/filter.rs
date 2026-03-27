// Filter and Operator types for query layer
//
// Type-safe query construction without SQL parsing

use crate::index::LikePattern;
use crate::storage::Value;
use std::fmt;

/// Query filter for a single column
///
/// Represents a condition like "age > 18" or "name LIKE 'John%'"
#[derive(Debug, Clone)]
pub struct Filter {
    /// Column name to filter on
    pub column: String,
    /// Comparison operator and value(s)
    pub operator: Operator,
    /// Cached parsed LikePattern for Like/NotLike operators
    cached_like: Option<LikePattern>,
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        self.column == other.column && self.operator == other.operator
    }
}

impl Filter {
    /// Create a new filter, pre-sorting In/NotIn lists and caching LikePatterns
    pub fn new(column: impl Into<String>, operator: Operator) -> Self {
        let operator = match operator {
            Operator::In(mut values) => {
                values.sort();
                Operator::In(values)
            }
            Operator::NotIn(mut values) => {
                values.sort();
                Operator::NotIn(values)
            }
            other => other,
        };
        // Pre-parse LIKE/NOT LIKE patterns
        let cached_like = match &operator {
            Operator::Like(pattern) | Operator::NotLike(pattern) => {
                LikePattern::parse(pattern).ok()
            }
            _ => None,
        };
        Self {
            column: column.into(),
            operator,
            cached_like,
        }
    }

    /// Estimated evaluation cost for filter reordering.
    ///
    /// Lower cost = cheaper to evaluate. Filters are sorted by cost so that
    /// cheap checks (null, integer equality) short-circuit before expensive
    /// ones (LIKE, IN with large lists).
    pub fn estimated_cost(&self) -> u8 {
        match &self.operator {
            Operator::IsNull | Operator::IsNotNull => 1,
            Operator::Equals(v) | Operator::NotEquals(v) => {
                if matches!(v, Value::Varchar(_)) { 6 } else { 2 }
            }
            Operator::GreaterThan(_)
            | Operator::GreaterThanOrEqual(_)
            | Operator::LessThan(_)
            | Operator::LessThanOrEqual(_) => 3,
            Operator::Between(_, _) => 4,
            Operator::In(_) | Operator::NotIn(_) => 5,
            Operator::Like(_) | Operator::NotLike(_) => {
                if let Some(ref pat) = self.cached_like {
                    if pat.can_use_index() { 7 } else { 8 }
                } else {
                    8
                }
            }
        }
    }

    /// Evaluate filter against a value
    ///
    /// Uses cached LikePattern for Like/NotLike operators to avoid re-parsing
    pub fn matches(&self, value: &Value) -> bool {
        match &self.operator {
            Operator::Like(_) => {
                if let Some(ref pattern) = self.cached_like {
                    pattern.matches(value)
                } else {
                    false
                }
            }
            Operator::NotLike(_) => {
                if let Some(ref pattern) = self.cached_like {
                    !pattern.matches(value)
                } else {
                    false
                }
            }
            _ => self.operator.matches(value),
        }
    }
}

/// Comparison operators for filters
#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    /// Equals (=)
    Equals(Value),

    /// Not equals (!=)
    NotEquals(Value),

    /// Greater than (>)
    GreaterThan(Value),

    /// Greater than or equal (>=)
    GreaterThanOrEqual(Value),

    /// Less than (<)
    LessThan(Value),

    /// Less than or equal (<=)
    LessThanOrEqual(Value),

    /// Between two values (inclusive)
    Between(Value, Value),

    /// In a list of values
    In(Vec<Value>),

    /// Not in a list of values
    NotIn(Vec<Value>),

    /// LIKE pattern match
    Like(String),

    /// NOT LIKE pattern match
    NotLike(String),

    /// IS NULL
    IsNull,

    /// IS NOT NULL
    IsNotNull,
}

impl Operator {
    /// Check if a value matches this operator
    pub fn matches(&self, value: &Value) -> bool {
        match self {
            Operator::Equals(v) => value == v,
            Operator::NotEquals(v) => value != v,
            Operator::GreaterThan(v) => {
                value.partial_cmp(v).map_or(false, |ord| ord == std::cmp::Ordering::Greater)
            }
            Operator::GreaterThanOrEqual(v) => {
                value.partial_cmp(v).map_or(false, |ord| ord != std::cmp::Ordering::Less)
            }
            Operator::LessThan(v) => {
                value.partial_cmp(v).map_or(false, |ord| ord == std::cmp::Ordering::Less)
            }
            Operator::LessThanOrEqual(v) => {
                value.partial_cmp(v).map_or(false, |ord| ord != std::cmp::Ordering::Greater)
            }
            Operator::Between(start, end) => {
                let ge_start = value.partial_cmp(start).map_or(false, |ord| ord != std::cmp::Ordering::Less);
                let le_end = value.partial_cmp(end).map_or(false, |ord| ord != std::cmp::Ordering::Greater);
                ge_start && le_end
            }
            Operator::In(values) => values.binary_search_by(|v| v.cmp(value)).is_ok(),
            Operator::NotIn(values) => values.binary_search_by(|v| v.cmp(value)).is_err(),
            Operator::Like(pattern) => {
                if let Value::Varchar(_) = value {
                    // Use simple pattern matching
                    use crate::index::LikePattern;
                    if let Ok(like_pattern) = LikePattern::parse(pattern) {
                        like_pattern.matches(value)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Operator::NotLike(pattern) => {
                if let Value::Varchar(_) = value {
                    use crate::index::LikePattern;
                    if let Ok(like_pattern) = LikePattern::parse(pattern) {
                        !like_pattern.matches(value)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Operator::IsNull => value.is_null(),
            Operator::IsNotNull => !value.is_null(),
        }
    }

    /// Check if this operator can use an index for optimization
    pub fn can_use_index(&self) -> bool {
        match self {
            Operator::Equals(_)
            | Operator::GreaterThan(_)
            | Operator::GreaterThanOrEqual(_)
            | Operator::LessThan(_)
            | Operator::LessThanOrEqual(_)
            | Operator::Between(_, _) => true,
            Operator::Like(pattern) => {
                if let Ok(lp) = LikePattern::parse(pattern) {
                    lp.can_use_index()
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Equals(v) => write!(f, "= {}", v),
            Operator::NotEquals(v) => write!(f, "!= {}", v),
            Operator::GreaterThan(v) => write!(f, "> {}", v),
            Operator::GreaterThanOrEqual(v) => write!(f, ">= {}", v),
            Operator::LessThan(v) => write!(f, "< {}", v),
            Operator::LessThanOrEqual(v) => write!(f, "<= {}", v),
            Operator::Between(start, end) => write!(f, "BETWEEN {} AND {}", start, end),
            Operator::In(values) => {
                write!(f, "IN (")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
            Operator::NotIn(values) => {
                write!(f, "NOT IN (")?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
            Operator::Like(pattern) => write!(f, "LIKE '{}'", pattern),
            Operator::NotLike(pattern) => write!(f, "NOT LIKE '{}'", pattern),
            Operator::IsNull => write!(f, "IS NULL"),
            Operator::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.column, self.operator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equals_operator() {
        let op = Operator::Equals(Value::Int32(42));

        assert!(op.matches(&Value::Int32(42)));
        assert!(!op.matches(&Value::Int32(43)));
        assert!(!op.matches(&Value::Int64(42)));
    }

    #[test]
    fn test_not_equals_operator() {
        let op = Operator::NotEquals(Value::Int32(42));

        assert!(!op.matches(&Value::Int32(42)));
        assert!(op.matches(&Value::Int32(43)));
    }

    #[test]
    fn test_greater_than_operator() {
        let op = Operator::GreaterThan(Value::Int32(10));

        assert!(op.matches(&Value::Int32(20)));
        assert!(!op.matches(&Value::Int32(10)));
        assert!(!op.matches(&Value::Int32(5)));
    }

    #[test]
    fn test_less_than_operator() {
        let op = Operator::LessThan(Value::Int32(10));

        assert!(op.matches(&Value::Int32(5)));
        assert!(!op.matches(&Value::Int32(10)));
        assert!(!op.matches(&Value::Int32(20)));
    }

    #[test]
    fn test_between_operator() {
        let op = Operator::Between(Value::Int32(10), Value::Int32(20));

        assert!(op.matches(&Value::Int32(10)));
        assert!(op.matches(&Value::Int32(15)));
        assert!(op.matches(&Value::Int32(20)));
        assert!(!op.matches(&Value::Int32(9)));
        assert!(!op.matches(&Value::Int32(21)));
    }

    #[test]
    fn test_in_operator() {
        let op = Operator::In(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
        ]);

        assert!(op.matches(&Value::Int32(1)));
        assert!(op.matches(&Value::Int32(2)));
        assert!(op.matches(&Value::Int32(3)));
        assert!(!op.matches(&Value::Int32(4)));
    }

    #[test]
    fn test_not_in_operator() {
        let op = Operator::NotIn(vec![
            Value::Int32(1),
            Value::Int32(2),
        ]);

        assert!(!op.matches(&Value::Int32(1)));
        assert!(!op.matches(&Value::Int32(2)));
        assert!(op.matches(&Value::Int32(3)));
    }

    #[test]
    fn test_like_operator() {
        let op = Operator::Like("test%".to_string());

        assert!(op.matches(&Value::varchar("test".to_string())));
        assert!(op.matches(&Value::varchar("testing".to_string())));
        assert!(!op.matches(&Value::varchar("best".to_string())));
        assert!(!op.matches(&Value::Int32(42)));
    }

    #[test]
    fn test_not_like_operator() {
        let op = Operator::NotLike("test%".to_string());

        assert!(!op.matches(&Value::varchar("test".to_string())));
        assert!(op.matches(&Value::varchar("best".to_string())));
    }

    #[test]
    fn test_is_null_operator() {
        let op = Operator::IsNull;

        assert!(op.matches(&Value::Null));
        assert!(!op.matches(&Value::Int32(42)));
    }

    #[test]
    fn test_is_not_null_operator() {
        let op = Operator::IsNotNull;

        assert!(!op.matches(&Value::Null));
        assert!(op.matches(&Value::Int32(42)));
    }

    #[test]
    fn test_filter_creation() {
        let filter = Filter::new("age", Operator::GreaterThan(Value::Int32(18)));

        assert_eq!(filter.column, "age");
        assert_eq!(filter.operator, Operator::GreaterThan(Value::Int32(18)));
    }

    #[test]
    fn test_filter_matches() {
        let filter = Filter::new("age", Operator::GreaterThan(Value::Int32(18)));

        assert!(filter.matches(&Value::Int32(25)));
        assert!(!filter.matches(&Value::Int32(15)));
    }

    #[test]
    fn test_can_use_index() {
        assert!(Operator::Equals(Value::Int32(1)).can_use_index());
        assert!(Operator::GreaterThan(Value::Int32(1)).can_use_index());
        assert!(Operator::Between(Value::Int32(1), Value::Int32(10)).can_use_index());
        // Prefix LIKE can use an index
        assert!(Operator::Like("test%".to_string()).can_use_index());
        // Non-prefix LIKE cannot
        assert!(!Operator::Like("%test%".to_string()).can_use_index());
        assert!(!Operator::IsNull.can_use_index());
    }

    #[test]
    fn test_display_filter() {
        let filter = Filter::new("age", Operator::GreaterThan(Value::Int32(18)));
        assert_eq!(format!("{}", filter), "age > 18");

        let filter = Filter::new("name", Operator::Like("John%".to_string()));
        assert_eq!(format!("{}", filter), "name LIKE 'John%'");
    }

    #[test]
    fn test_varchar_comparisons() {
        let op = Operator::GreaterThan(Value::varchar("banana".to_string()));

        assert!(op.matches(&Value::varchar("cherry".to_string())));
        assert!(!op.matches(&Value::varchar("apple".to_string())));
    }

    #[test]
    fn test_float_comparisons() {
        let op = Operator::Between(Value::Float64(1.0), Value::Float64(10.0));

        assert!(op.matches(&Value::Float64(5.0)));
        assert!(op.matches(&Value::Float64(1.0)));
        assert!(op.matches(&Value::Float64(10.0)));
        assert!(!op.matches(&Value::Float64(0.5)));
        assert!(!op.matches(&Value::Float64(10.5)));
    }
}
