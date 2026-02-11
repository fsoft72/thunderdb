// Index statistics - Step 2.5
//
// Basic statistics for query optimization

use crate::index::BTree;
use crate::query::Operator;
use crate::storage::Value;

/// Statistics for an index
///
/// Used for query optimization decisions
#[derive(Debug, Clone)]
pub struct IndexStatistics {
    /// Number of unique values (cardinality)
    pub cardinality: usize,

    /// Total number of entries (including duplicates)
    pub total_entries: usize,

    /// Minimum value in the index
    pub min_value: Option<Value>,

    /// Maximum value in the index
    pub max_value: Option<Value>,

    /// Average number of duplicates per key
    pub avg_duplicates: f64,
}

impl IndexStatistics {
    /// Compute statistics from a B-Tree index
    ///
    /// # Arguments
    /// * `tree` - B-Tree to analyze
    pub fn from_btree(tree: &BTree<Value, u64>) -> Self {
        let all_entries = tree.scan_all();

        if all_entries.is_empty() {
            return Self {
                cardinality: 0,
                total_entries: 0,
                min_value: None,
                max_value: None,
                avg_duplicates: 0.0,
            };
        }

        // Count unique keys
        let mut unique_keys = std::collections::HashSet::new();
        for (key, _) in &all_entries {
            // Use a string representation for hashing since Value doesn't implement Hash
            unique_keys.insert(format!("{:?}", key));
        }

        let cardinality = unique_keys.len();
        let total_entries = all_entries.len();

        let min_value = all_entries.first().map(|(k, _)| k.clone());
        let max_value = all_entries.last().map(|(k, _)| k.clone());

        let avg_duplicates = if cardinality > 0 {
            total_entries as f64 / cardinality as f64
        } else {
            0.0
        };

        Self {
            cardinality,
            total_entries,
            min_value,
            max_value,
            avg_duplicates,
        }
    }

    /// Check if this is a unique index (no duplicates)
    pub fn is_unique(&self) -> bool {
        self.avg_duplicates <= 1.0
    }

    /// Get selectivity (ratio of unique values to total entries)
    ///
    /// Higher selectivity means the index is more useful for filtering
    pub fn selectivity(&self) -> f64 {
        if self.total_entries == 0 {
            return 0.0;
        }

        self.cardinality as f64 / self.total_entries as f64
    }

    /// Create zero-valued stats for a newly created index
    pub fn empty() -> Self {
        Self {
            cardinality: 0,
            total_entries: 0,
            min_value: None,
            max_value: None,
            avg_duplicates: 0.0,
        }
    }

    /// Update stats after a value is inserted
    pub fn record_insert(&mut self, value: &Value) {
        self.total_entries += 1;

        // Update min/max
        match &self.min_value {
            None => self.min_value = Some(value.clone()),
            Some(min) => {
                if value.partial_cmp(min) == Some(std::cmp::Ordering::Less) {
                    self.min_value = Some(value.clone());
                }
            }
        }
        match &self.max_value {
            None => self.max_value = Some(value.clone()),
            Some(max) => {
                if value.partial_cmp(max) == Some(std::cmp::Ordering::Greater) {
                    self.max_value = Some(value.clone());
                }
            }
        }

        // Recompute avg_duplicates (approximation — cardinality can't be cheaply maintained)
        if self.cardinality > 0 {
            self.avg_duplicates = self.total_entries as f64 / self.cardinality as f64;
        }
    }

    /// Update stats after a row is deleted
    pub fn record_delete(&mut self) {
        if self.total_entries > 0 {
            self.total_entries -= 1;
        }
        if self.cardinality > 0 {
            self.avg_duplicates = self.total_entries as f64 / self.cardinality as f64;
        }
    }

    /// Estimate the number of rows an operator would return
    ///
    /// Used by the query optimizer to pick the most selective index.
    pub fn estimate_rows(&self, operator: &Operator) -> usize {
        if self.total_entries == 0 {
            return 0;
        }

        match operator {
            Operator::Equals(_) => {
                // Uniform assumption: total / cardinality
                self.total_entries / self.cardinality.max(1)
            }
            Operator::Between(low, high) => {
                self.estimate_range_rows(low, high)
            }
            Operator::GreaterThan(val) | Operator::GreaterThanOrEqual(val) => {
                if let Some(max) = &self.max_value {
                    self.estimate_range_rows(val, max)
                } else {
                    self.total_entries / 4
                }
            }
            Operator::LessThan(val) | Operator::LessThanOrEqual(val) => {
                if let Some(min) = &self.min_value {
                    self.estimate_range_rows(min, val)
                } else {
                    self.total_entries / 4
                }
            }
            Operator::Like(_) => {
                // Rough estimate for prefix-based LIKE
                self.total_entries / self.cardinality.max(1)
            }
            // NotEquals, In, NotIn, IsNull, IsNotNull, NotLike — worst case
            _ => self.total_entries,
        }
    }

    /// Estimate fraction of rows in a range using linear interpolation on numeric types
    fn estimate_range_rows(&self, low: &Value, high: &Value) -> usize {
        let (min, max) = match (&self.min_value, &self.max_value) {
            (Some(min), Some(max)) => (min, max),
            _ => return self.total_entries / 4,
        };

        let to_f64 = |v: &Value| -> Option<f64> {
            match v {
                Value::Int32(n) => Some(*n as f64),
                Value::Int64(n) => Some(*n as f64),
                Value::Float32(n) => Some(*n as f64),
                Value::Float64(n) => Some(*n),
                Value::Timestamp(n) => Some(*n as f64),
                _ => None,
            }
        };

        if let (Some(min_f), Some(max_f), Some(low_f), Some(high_f)) =
            (to_f64(min), to_f64(max), to_f64(low), to_f64(high))
        {
            let total_range = max_f - min_f;
            if total_range <= 0.0 {
                return self.total_entries;
            }
            let query_range = (high_f.min(max_f) - low_f.max(min_f)).max(0.0);
            let fraction = query_range / total_range;
            ((self.total_entries as f64) * fraction).ceil() as usize
        } else {
            // Non-numeric fallback
            self.total_entries / 4
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::BTree;

    #[test]
    fn test_empty_tree_stats() {
        let tree: BTree<Value, u64> = BTree::new(5).unwrap();
        let stats = IndexStatistics::from_btree(&tree);

        assert_eq!(stats.cardinality, 0);
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
        assert_eq!(stats.avg_duplicates, 0.0);
    }

    #[test]
    fn test_unique_index_stats() {
        let mut tree = BTree::new(5).unwrap();

        for i in 1..=10 {
            tree.insert(Value::Int32(i), i as u64).unwrap();
        }

        let stats = IndexStatistics::from_btree(&tree);

        assert_eq!(stats.cardinality, 10);
        assert_eq!(stats.total_entries, 10);
        assert_eq!(stats.min_value, Some(Value::Int32(1)));
        assert_eq!(stats.max_value, Some(Value::Int32(10)));
        assert_eq!(stats.avg_duplicates, 1.0);
        assert!(stats.is_unique());
        assert_eq!(stats.selectivity(), 1.0);
    }

    #[test]
    fn test_duplicate_index_stats() {
        let mut tree = BTree::new(5).unwrap();

        // Insert duplicates: 5 entries for each of 3 keys
        for i in 1..=3 {
            for j in 1..=5 {
                tree.insert(Value::Int32(i), (i * 10 + j) as u64).unwrap();
            }
        }

        let stats = IndexStatistics::from_btree(&tree);

        assert_eq!(stats.cardinality, 3);
        assert_eq!(stats.total_entries, 15);
        assert_eq!(stats.avg_duplicates, 5.0);
        assert!(!stats.is_unique());
        assert_eq!(stats.selectivity(), 0.2); // 3/15 = 0.2
    }

    #[test]
    fn test_varchar_index_stats() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(Value::varchar("alice".to_string()), 1).unwrap();
        tree.insert(Value::varchar("bob".to_string()), 2).unwrap();
        tree.insert(Value::varchar("charlie".to_string()), 3).unwrap();

        let stats = IndexStatistics::from_btree(&tree);

        assert_eq!(stats.cardinality, 3);
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.min_value, Some(Value::varchar("alice".to_string())));
        assert_eq!(stats.max_value, Some(Value::varchar("charlie".to_string())));
    }

    #[test]
    fn test_empty_stats() {
        let stats = IndexStatistics::empty();
        assert_eq!(stats.cardinality, 0);
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[test]
    fn test_record_insert() {
        let mut stats = IndexStatistics::empty();
        stats.cardinality = 1; // Manually set since record_insert doesn't track unique values

        stats.record_insert(&Value::Int32(10));
        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.min_value, Some(Value::Int32(10)));
        assert_eq!(stats.max_value, Some(Value::Int32(10)));

        stats.cardinality = 2;
        stats.record_insert(&Value::Int32(5));
        assert_eq!(stats.total_entries, 2);
        assert_eq!(stats.min_value, Some(Value::Int32(5)));
        assert_eq!(stats.max_value, Some(Value::Int32(10)));

        stats.cardinality = 3;
        stats.record_insert(&Value::Int32(20));
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.max_value, Some(Value::Int32(20)));
    }

    #[test]
    fn test_record_delete() {
        let mut stats = IndexStatistics {
            cardinality: 5,
            total_entries: 10,
            min_value: Some(Value::Int32(1)),
            max_value: Some(Value::Int32(100)),
            avg_duplicates: 2.0,
        };

        stats.record_delete();
        assert_eq!(stats.total_entries, 9);
    }

    #[test]
    fn test_estimate_rows_equals() {
        use crate::query::Operator;
        let stats = IndexStatistics {
            cardinality: 10,
            total_entries: 100,
            min_value: Some(Value::Int32(1)),
            max_value: Some(Value::Int32(100)),
            avg_duplicates: 10.0,
        };

        // Equals: 100 / 10 = 10
        let est = stats.estimate_rows(&Operator::Equals(Value::Int32(50)));
        assert_eq!(est, 10);
    }

    #[test]
    fn test_estimate_rows_range() {
        use crate::query::Operator;
        let stats = IndexStatistics {
            cardinality: 100,
            total_entries: 100,
            min_value: Some(Value::Int32(0)),
            max_value: Some(Value::Int32(100)),
            avg_duplicates: 1.0,
        };

        // Between 25..75 = 50% of range → ~50 rows
        let est = stats.estimate_rows(&Operator::Between(Value::Int32(25), Value::Int32(75)));
        assert_eq!(est, 50);
    }

    #[test]
    fn test_estimate_rows_empty() {
        use crate::query::Operator;
        let stats = IndexStatistics::empty();
        assert_eq!(stats.estimate_rows(&Operator::Equals(Value::Int32(1))), 0);
    }

    #[test]
    fn test_selectivity() {
        let mut tree = BTree::new(5).unwrap();

        // High selectivity: all unique
        for i in 1..=100 {
            tree.insert(Value::Int32(i), i as u64).unwrap();
        }

        let stats = IndexStatistics::from_btree(&tree);
        assert_eq!(stats.selectivity(), 1.0);

        // Low selectivity: 100 entries but only 10 unique
        let mut tree2 = BTree::new(5).unwrap();
        for i in 1..=10 {
            for _ in 1..=10 {
                tree2.insert(Value::Int32(i), i as u64).unwrap();
            }
        }

        let stats2 = IndexStatistics::from_btree(&tree2);
        assert_eq!(stats2.selectivity(), 0.1); // 10/100
    }
}
