// Index statistics - Step 2.5
//
// Basic statistics for query optimization

use crate::index::BTree;
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

        tree.insert(Value::Varchar("alice".to_string()), 1).unwrap();
        tree.insert(Value::Varchar("bob".to_string()), 2).unwrap();
        tree.insert(Value::Varchar("charlie".to_string()), 3).unwrap();

        let stats = IndexStatistics::from_btree(&tree);

        assert_eq!(stats.cardinality, 3);
        assert_eq!(stats.total_entries, 3);
        assert_eq!(stats.min_value, Some(Value::Varchar("alice".to_string())));
        assert_eq!(stats.max_value, Some(Value::Varchar("charlie".to_string())));
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
