// Direct CRUD API - Step 3.2
//
// Type-safe data access operations

use crate::error::Result;
use crate::query::{Filter, Operator};
use crate::storage::{Row, Value};

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
/// # Arguments
/// * `filters` - List of filters
/// * `available_indices` - List of indexed column names
///
/// # Returns
/// Best column to use for index lookup, if any
pub fn choose_index(filters: &[Filter], available_indices: &[String]) -> Option<(String, Operator)> {
    // Priority 1: Equals
    for filter in filters {
        if available_indices.contains(&filter.column) {
            if matches!(filter.operator, Operator::Equals(_)) {
                return Some((filter.column.clone(), filter.operator.clone()));
            }
        }
    }

    // Priority 2: Range operators and Prefix LIKE
    for filter in filters {
        if available_indices.contains(&filter.column) {
            match &filter.operator {
                Operator::GreaterThan(_) | Operator::GreaterThanOrEqual(_) |
                Operator::LessThan(_) | Operator::LessThanOrEqual(_) |
                Operator::Between(_, _) => {
                    return Some((filter.column.clone(), filter.operator.clone()));
                }
                Operator::Like(pattern) => {
                    use crate::index::LikePattern;
                    if let Ok(lp) = LikePattern::parse(pattern) {
                        if lp.can_use_index() {
                            return Some((filter.column.clone(), filter.operator.clone()));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_row(row_id: u64, id: i32, name: &str, age: i32) -> Row {
        Row::new(
            row_id,
            vec![
                Value::Int32(id),
                Value::Varchar(name.to_string()),
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

        let result = choose_index(&filters, &indices);
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

        let result = choose_index(&filters, &indices);
        assert!(result.is_some());

        let (column, _) = result.unwrap();
        assert_eq!(column, "age");
    }

    #[test]
    fn test_choose_index_none() {
        let filters = vec![Filter::new("name", Operator::Like("%test%".to_string()))];

        let indices = vec!["id".to_string()];

        let result = choose_index(&filters, &indices);
        assert!(result.is_none());
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
}
