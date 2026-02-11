// Direct CRUD API - Step 3.2
//
// Type-safe data access operations

use crate::error::Result;
use crate::index::stats::IndexStatistics;
use crate::index::IndexManager;
use crate::query::{Filter, Operator};
use crate::storage::{Row, Value};
use std::collections::HashMap;

/// Direct data access trait for type-safe CRUD operations
///
/// Bypasses SQL parsing for maximum performance
pub trait DirectDataAccess {
    /// Insert a new row into a table
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `values` - Column values in order
    ///
    /// # Returns
    /// Auto-generated row ID
    fn insert_row(&mut self, table: &str, values: Vec<Value>) -> Result<u64>;

    /// Insert multiple rows in batch
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `rows` - Vector of value vectors to insert
    ///
    /// # Returns
    /// Vector of auto-generated row IDs
    fn insert_batch(&mut self, table: &str, rows: Vec<Vec<Value>>) -> Result<Vec<u64>>;

    /// Get a row by ID
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `row_id` - Row ID to retrieve
    ///
    /// # Returns
    /// Row if found and not deleted
    fn get_by_id(&mut self, table: &str, row_id: u64) -> Result<Option<Row>>;

    /// Scan table with filters
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `filters` - List of filter conditions (AND combined)
    ///
    /// # Returns
    /// Vector of matching rows
    fn scan(&mut self, table: &str, filters: Vec<Filter>) -> Result<Vec<Row>>;

    /// Scan table with filters and limits
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `filters` - List of filter conditions
    /// * `limit` - Maximum rows to return
    /// * `offset` - Number of rows to skip
    ///
    /// # Returns
    /// Vector of matching rows
    fn scan_with_limit(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Row>>;

    /// Update rows matching filters
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `filters` - Conditions to select rows
    /// * `updates` - Column name and new value pairs
    ///
    /// # Returns
    /// Number of rows updated
    fn update(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        updates: Vec<(String, Value)>,
    ) -> Result<usize>;

    /// Delete rows matching filters
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `filters` - Conditions to select rows
    ///
    /// # Returns
    /// Number of rows deleted
    fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize>;

    /// Count rows matching filters
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `filters` - Conditions to select rows
    ///
    /// # Returns
    /// Number of matching rows
    fn count(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize>;
}

/// Query execution context
///
/// Holds statistics about query execution
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
    /// Number of rows scanned
    pub rows_scanned: usize,

    /// Number of rows that matched filters
    pub rows_matched: usize,

    /// Whether an index was used
    pub index_used: bool,

    /// Index column if used
    pub index_column: Option<String>,

    /// Execution time in microseconds
    pub execution_time_us: u64,
}

impl QueryContext {
    /// Create a new query context
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark that an index was used
    pub fn used_index(&mut self, column: String) {
        self.index_used = true;
        self.index_column = Some(column);
    }
}

/// Helper function to apply filters to a row
///
/// # Arguments
/// * `row` - Row to check
/// * `filters` - List of filters (AND combined)
/// * `column_mapping` - Maps column names to positions
///
/// # Returns
/// true if row matches all filters
pub fn apply_filters(
    row: &Row,
    filters: &[Filter],
    column_mapping: &std::collections::HashMap<String, usize>,
) -> bool {
    for filter in filters {
        let col_idx = if let Some(&idx) = column_mapping.get(&filter.column) {
            Some(idx)
        } else if filter.column.starts_with("col") {
            filter.column[3..].parse::<usize>().ok()
        } else {
            None
        };

        if let Some(idx) = col_idx {
            if let Some(value) = row.values.get(idx) {
                if !filter.matches(value) {
                    return false;
                }
            } else {
                return false; // Column index out of bounds for row
            }
        } else {
            return false; // Column doesn't exist in mapping and not a valid colN
        }
    }

    true
}

/// Helper function to choose the best index for a query
///
/// When stats are available, estimates result set sizes and picks the most
/// selective index. Without stats, falls back to operator-priority heuristics
/// (Equals first, then ranges/LIKE).
///
/// # Arguments
/// * `filters` - List of filters
/// * `available_indices` - List of indexed column names
/// * `stats` - Optional per-column index statistics for cost estimation
///
/// # Returns
/// Best column to use for index lookup, if any
pub fn choose_index(
    filters: &[Filter],
    available_indices: &[String],
    stats: Option<&HashMap<String, IndexStatistics>>,
) -> Option<(String, Operator)> {
    // Collect all indexable candidates
    let mut candidates: Vec<(String, Operator, bool)> = Vec::new();

    for filter in filters {
        if !available_indices.contains(&filter.column) {
            continue;
        }
        let indexable = match &filter.operator {
            Operator::Equals(_)
            | Operator::GreaterThan(_)
            | Operator::GreaterThanOrEqual(_)
            | Operator::LessThan(_)
            | Operator::LessThanOrEqual(_)
            | Operator::Between(_, _) => true,
            Operator::Like(pattern) => {
                use crate::index::LikePattern;
                if let Ok(lp) = LikePattern::parse(pattern) {
                    lp.can_use_index()
                } else {
                    false
                }
            }
            _ => false,
        };
        if indexable {
            candidates.push((filter.column.clone(), filter.operator.clone(), matches!(filter.operator, Operator::Equals(_))));
        }
    }

    if candidates.is_empty() {
        return None;
    }

    // If stats are available, pick the candidate with the smallest estimated result set
    if let Some(stats_map) = stats {
        let mut best: Option<(String, Operator, usize)> = None;
        for (col, op, _) in &candidates {
            let estimate = if let Some(col_stats) = stats_map.get(col) {
                col_stats.estimate_rows(op)
            } else {
                usize::MAX
            };
            if best.as_ref().is_none_or(|b| estimate < b.2) {
                best = Some((col.clone(), op.clone(), estimate));
            }
        }
        return best.map(|(col, op, _)| (col, op));
    }

    // Fallback: operator-priority heuristics (no stats)
    // Priority 1: Equals
    for (col, op, is_eq) in &candidates {
        if *is_eq {
            return Some((col.clone(), op.clone()));
        }
    }

    // Priority 2: anything else that's indexable
    let (col, op, _) = candidates.into_iter().next().unwrap();
    Some((col, op))
}

/// Merge-based O(n+m) intersection of two sorted vectors
pub fn sorted_intersect(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// Try to use multiple indices for a query, intersecting row ID sets
///
/// For each filter, attempts to get row IDs from the index. Filters that
/// can't use an index are pushed to `remaining_filters` for post-filtering.
/// If fewer than 2 index sets are found, returns None to fall back to single-index.
///
/// # Arguments
/// * `filters` - All WHERE filters
/// * `index_mgr` - The table's index manager
/// * `stats` - Optional statistics for ordering intersection by estimated size
/// * `remaining_filters` - Output: filters that couldn't use an index
///
/// # Returns
/// Intersected row IDs if at least 2 indices matched, None otherwise
pub fn multi_index_scan(
    filters: &[Filter],
    index_mgr: &IndexManager,
    stats: Option<&HashMap<String, IndexStatistics>>,
    remaining_filters: &mut Vec<Filter>,
) -> Option<Vec<u64>> {
    let mut indexed_sets: Vec<(Vec<u64>, usize)> = Vec::new();

    for filter in filters {
        if let Some(row_ids) = index_mgr.query_row_ids(&filter.column, &filter.operator) {
            let estimate = stats
                .and_then(|s| s.get(&filter.column))
                .map(|s| s.estimate_rows(&filter.operator))
                .unwrap_or(row_ids.len());
            indexed_sets.push((row_ids, estimate));
        } else {
            remaining_filters.push(filter.clone());
        }
    }

    if indexed_sets.len() < 2 {
        return None;
    }

    // Sort by estimated size (smallest first) for progressive short-circuit intersection
    indexed_sets.sort_by_key(|(_, est)| *est);

    let mut result = indexed_sets.remove(0).0;
    result.sort_unstable();

    for (mut set, _) in indexed_sets {
        if result.is_empty() {
            return Some(Vec::new());
        }
        set.sort_unstable();
        result = sorted_intersect(&result, &set);
    }

    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_row(row_id: u64, id: i32, name: &str, age: i32) -> Row {
        Row::new(
            row_id,
            vec![
                Value::Int32(id),
                Value::varchar(name.to_string()),
                Value::Int32(age),
            ],
        )
    }

    fn create_column_mapping() -> HashMap<String, usize> {
        let mut map = HashMap::new();
        map.insert("id".to_string(), 0);
        map.insert("name".to_string(), 1);
        map.insert("age".to_string(), 2);
        map
    }

    #[test]
    fn test_apply_filters_single() {
        let row = create_test_row(1, 100, "Alice", 25);
        let mapping = create_column_mapping();

        let filters = vec![Filter::new("age", Operator::GreaterThan(Value::Int32(20)))];

        assert!(apply_filters(&row, &filters, &mapping));

        let filters = vec![Filter::new("age", Operator::GreaterThan(Value::Int32(30)))];
        assert!(!apply_filters(&row, &filters, &mapping));
    }

    #[test]
    fn test_apply_filters_multiple() {
        let row = create_test_row(1, 100, "Alice", 25);
        let mapping = create_column_mapping();

        let filters = vec![
            Filter::new("age", Operator::GreaterThan(Value::Int32(20))),
            Filter::new("name", Operator::Like("A%".to_string())),
        ];

        assert!(apply_filters(&row, &filters, &mapping));

        let filters = vec![
            Filter::new("age", Operator::GreaterThan(Value::Int32(20))),
            Filter::new("name", Operator::Like("B%".to_string())),
        ];

        assert!(!apply_filters(&row, &filters, &mapping));
    }

    #[test]
    fn test_apply_filters_nonexistent_column() {
        let row = create_test_row(1, 100, "Alice", 25);
        let mapping = create_column_mapping();

        let filters = vec![Filter::new("salary", Operator::GreaterThan(Value::Int32(1000)))];

        assert!(!apply_filters(&row, &filters, &mapping));
    }

    #[test]
    fn test_apply_filters_empty() {
        let row = create_test_row(1, 100, "Alice", 25);
        let mapping = create_column_mapping();

        let filters = vec![];

        assert!(apply_filters(&row, &filters, &mapping));
    }

    #[test]
    fn test_choose_index_equals() {
        let filters = vec![
            Filter::new("age", Operator::GreaterThan(Value::Int32(20))),
            Filter::new("id", Operator::Equals(Value::Int32(100))),
        ];

        let indices = vec!["id".to_string(), "age".to_string()];

        let result = choose_index(&filters, &indices, None);
        assert!(result.is_some());

        let (column, _) = result.unwrap();
        assert_eq!(column, "id"); // Equals has priority
    }

    #[test]
    fn test_choose_index_range() {
        let filters = vec![
            Filter::new("age", Operator::Between(Value::Int32(20), Value::Int32(30))),
            Filter::new("name", Operator::Like("A%".to_string())),
        ];

        let indices = vec!["age".to_string()];

        let result = choose_index(&filters, &indices, None);
        assert!(result.is_some());

        let (column, _) = result.unwrap();
        assert_eq!(column, "age");
    }

    #[test]
    fn test_choose_index_none() {
        let filters = vec![Filter::new("name", Operator::Like("%test%".to_string()))];

        let indices = vec!["id".to_string()];

        let result = choose_index(&filters, &indices, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_choose_index_with_stats() {
        let filters = vec![
            Filter::new("age", Operator::Equals(Value::Int32(25))),
            Filter::new("city", Operator::Equals(Value::varchar("Rome".to_string()))),
        ];

        let indices = vec!["age".to_string(), "city".to_string()];

        // city has higher cardinality (more selective for Equals)
        let mut stats_map = HashMap::new();
        stats_map.insert("age".to_string(), IndexStatistics {
            cardinality: 10,
            total_entries: 1000,
            min_value: Some(Value::Int32(1)),
            max_value: Some(Value::Int32(100)),
            avg_duplicates: 100.0,
        });
        stats_map.insert("city".to_string(), IndexStatistics {
            cardinality: 500,
            total_entries: 1000,
            min_value: None,
            max_value: None,
            avg_duplicates: 2.0,
        });

        let result = choose_index(&filters, &indices, Some(&stats_map));
        assert!(result.is_some());
        let (column, _) = result.unwrap();
        // city is more selective: 1000/500=2 vs age 1000/10=100
        assert_eq!(column, "city");
    }

    #[test]
    fn test_query_context() {
        let mut ctx = QueryContext::new();

        assert!(!ctx.index_used);
        assert_eq!(ctx.rows_scanned, 0);

        ctx.used_index("id".to_string());
        assert!(ctx.index_used);
        assert_eq!(ctx.index_column, Some("id".to_string()));

        ctx.rows_scanned = 100;
        ctx.rows_matched = 10;
        assert_eq!(ctx.rows_scanned, 100);
        assert_eq!(ctx.rows_matched, 10);
    }

    #[test]
    fn test_sorted_intersect_overlap() {
        let a = vec![1, 3, 5, 7, 9];
        let b = vec![2, 3, 5, 8, 9, 10];
        assert_eq!(sorted_intersect(&a, &b), vec![3, 5, 9]);
    }

    #[test]
    fn test_sorted_intersect_disjoint() {
        let a = vec![1, 3, 5];
        let b = vec![2, 4, 6];
        assert_eq!(sorted_intersect(&a, &b), Vec::<u64>::new());
    }

    #[test]
    fn test_sorted_intersect_empty() {
        let a: Vec<u64> = vec![];
        let b = vec![1, 2, 3];
        assert_eq!(sorted_intersect(&a, &b), Vec::<u64>::new());
        assert_eq!(sorted_intersect(&b, &a), Vec::<u64>::new());
    }

    #[test]
    fn test_sorted_intersect_identical() {
        let a = vec![1, 2, 3];
        assert_eq!(sorted_intersect(&a, &a), vec![1, 2, 3]);
    }
}
