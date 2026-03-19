// QueryBuilder pattern - Step 3.3
//
// Fluent API for building queries

use crate::query::{Filter, Operator};
use crate::storage::{Row, Value};
use std::collections::HashMap;

/// Query builder for fluent query construction
///
/// Provides a chainable API for building queries without SQL
///
/// # Example
/// ```ignore
/// let results = QueryBuilder::from("users")
///     .filter("age", Operator::GreaterThan(Value::Int32(18)))
///     .filter("name", Operator::Like("John%".to_string()))
///     .limit(10)
///     .offset(5)
///     .execute(&mut db)?;
/// ```
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    /// Table to query
    table: String,

    /// Filter conditions (AND combined)
    filters: Vec<Filter>,

    /// Maximum rows to return
    limit: Option<usize>,

    /// Number of rows to skip
    offset: Option<usize>,

    /// Columns to select (None = all columns)
    columns: Option<Vec<String>>,

    /// ORDER BY column and direction
    order_by: Option<(String, OrderDirection)>,
}

/// Sort order direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    /// Ascending order (A-Z, 0-9)
    Asc,
    /// Descending order (Z-A, 9-0)
    Desc,
}

impl QueryBuilder {
    /// Create a new query builder for a table
    ///
    /// # Arguments
    /// * `table` - Table name to query
    pub fn from(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            filters: Vec::new(),
            limit: None,
            offset: None,
            columns: None,
            order_by: None,
        }
    }

    /// Add a filter condition
    ///
    /// Multiple filters are AND combined
    ///
    /// # Arguments
    /// * `column` - Column name
    /// * `operator` - Comparison operator
    pub fn filter(mut self, column: impl Into<String>, operator: Operator) -> Self {
        self.filters.push(Filter::new(column, operator));
        self
    }

    /// Add a filter using a Filter object
    pub fn add_filter(mut self, filter: Filter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add multiple filters
    pub fn filters(mut self, filters: Vec<Filter>) -> Self {
        self.filters.extend(filters);
        self
    }

    /// Set LIMIT (maximum rows to return)
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set OFFSET (rows to skip)
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Select specific columns (default: all columns)
    pub fn select(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }

    /// Add ORDER BY clause
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.order_by = Some((column.into(), direction));
        self
    }

    /// Shorthand for ORDER BY ASC
    pub fn order_by_asc(self, column: impl Into<String>) -> Self {
        self.order_by(column, OrderDirection::Asc)
    }

    /// Shorthand for ORDER BY DESC
    pub fn order_by_desc(self, column: impl Into<String>) -> Self {
        self.order_by(column, OrderDirection::Desc)
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get the filters
    pub fn get_filters(&self) -> &[Filter] {
        &self.filters
    }

    /// Get the limit
    pub fn get_limit(&self) -> Option<usize> {
        self.limit
    }

    /// Get the offset
    pub fn get_offset(&self) -> Option<usize> {
        self.offset
    }

    /// Get selected columns
    pub fn get_columns(&self) -> Option<&[String]> {
        self.columns.as_deref()
    }

    /// Get ORDER BY clause
    pub fn get_order_by(&self) -> Option<&(String, OrderDirection)> {
        self.order_by.as_ref()
    }

    /// Build the query plan (extract components)
    pub fn build(self) -> QueryPlan {
        QueryPlan {
            table: self.table,
            filters: self.filters,
            limit: self.limit,
            offset: self.offset,
            columns: self.columns,
            order_by: self.order_by,
        }
    }
}

/// Query execution plan
///
/// Represents a parsed query ready for execution
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub table: String,
    pub filters: Vec<Filter>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub columns: Option<Vec<String>>,
    pub order_by: Option<(String, OrderDirection)>,
}

impl QueryPlan {
    /// Apply limit and offset to a result set
    pub fn apply_pagination(&self, mut rows: Vec<Row>) -> Vec<Row> {
        // Apply offset
        if let Some(offset) = self.offset {
            if offset < rows.len() {
                rows = rows.into_iter().skip(offset).collect();
            } else {
                return Vec::new();
            }
        }

        // Apply limit
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }

        rows
    }

    /// Apply ORDER BY to a result set
    pub fn apply_ordering(
        &self,
        mut rows: Vec<Row>,
        column_mapping: &HashMap<String, usize>,
    ) -> Vec<Row> {
        if let Some((column, direction)) = &self.order_by {
            if let Some(&col_idx) = column_mapping.get(column) {
                rows.sort_by(|a, b| {
                    let val_a = a.values.get(col_idx);
                    let val_b = b.values.get(col_idx);

                    let cmp = match (val_a, val_b) {
                        (Some(a), Some(b)) => a.cmp(b),
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (None, None) => std::cmp::Ordering::Equal,
                    };

                    match direction {
                        OrderDirection::Asc => cmp,
                        OrderDirection::Desc => cmp.reverse(),
                    }
                });
            }
        }

        rows
    }

    /// Apply column projection to rows
    pub fn apply_projection(
        &self,
        rows: Vec<Row>,
        column_mapping: &HashMap<String, usize>,
    ) -> Vec<Row> {
        if let Some(columns) = &self.columns {
            let col_indices: Vec<usize> = columns
                .iter()
                .filter_map(|col| column_mapping.get(col).copied())
                .collect();

            rows.into_iter()
                .map(|mut row| {
                    let new_values: Vec<Value> = col_indices
                        .iter()
                        .filter_map(|&idx| row.values.get(idx).cloned())
                        .collect();
                    row.values = new_values;
                    row
                })
                .collect()
        } else {
            rows
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_query_builder_basic() {
        let query = QueryBuilder::from("users")
            .filter("age", Operator::GreaterThan(Value::Int32(18)))
            .limit(10)
            .build();

        assert_eq!(query.table, "users");
        assert_eq!(query.filters.len(), 1);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, None);
    }

    #[test]
    fn test_query_builder_multiple_filters() {
        let query = QueryBuilder::from("users")
            .filter("age", Operator::GreaterThan(Value::Int32(18)))
            .filter("name", Operator::Like("John%".to_string()))
            .build();

        assert_eq!(query.filters.len(), 2);
    }

    #[test]
    fn test_query_builder_limit_offset() {
        let query = QueryBuilder::from("users")
            .limit(10)
            .offset(5)
            .build();

        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(5));
    }

    #[test]
    fn test_query_builder_select() {
        let query = QueryBuilder::from("users")
            .select(vec!["id".to_string(), "name".to_string()])
            .build();

        assert_eq!(query.columns, Some(vec!["id".to_string(), "name".to_string()]));
    }

    #[test]
    fn test_query_builder_order_by() {
        let query = QueryBuilder::from("users")
            .order_by_asc("name")
            .build();

        assert_eq!(query.order_by, Some(("name".to_string(), OrderDirection::Asc)));

        let query = QueryBuilder::from("users")
            .order_by_desc("age")
            .build();

        assert_eq!(query.order_by, Some(("age".to_string(), OrderDirection::Desc)));
    }

    #[test]
    fn test_query_plan_pagination() {
        let plan = QueryPlan {
            table: "users".to_string(),
            filters: vec![],
            limit: Some(3),
            offset: Some(2),
            columns: None,
            order_by: None,
        };

        let rows = vec![
            Row::new(1, vec![Value::Int32(1)]),
            Row::new(2, vec![Value::Int32(2)]),
            Row::new(3, vec![Value::Int32(3)]),
            Row::new(4, vec![Value::Int32(4)]),
            Row::new(5, vec![Value::Int32(5)]),
        ];

        let result = plan.apply_pagination(rows);

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].row_id, 3);
        assert_eq!(result[1].row_id, 4);
        assert_eq!(result[2].row_id, 5);
    }

    #[test]
    fn test_query_plan_ordering() {
        let mut mapping = HashMap::new();
        mapping.insert("value".to_string(), 0);

        let plan = QueryPlan {
            table: "test".to_string(),
            filters: vec![],
            limit: None,
            offset: None,
            columns: None,
            order_by: Some(("value".to_string(), OrderDirection::Asc)),
        };

        let rows = vec![
            Row::new(1, vec![Value::Int32(5)]),
            Row::new(2, vec![Value::Int32(2)]),
            Row::new(3, vec![Value::Int32(8)]),
            Row::new(4, vec![Value::Int32(1)]),
        ];

        let result = plan.apply_ordering(rows, &mapping);

        assert_eq!(result[0].row_id, 4); // value = 1
        assert_eq!(result[1].row_id, 2); // value = 2
        assert_eq!(result[2].row_id, 1); // value = 5
        assert_eq!(result[3].row_id, 3); // value = 8
    }

    #[test]
    fn test_query_plan_ordering_desc() {
        let mut mapping = HashMap::new();
        mapping.insert("value".to_string(), 0);

        let plan = QueryPlan {
            table: "test".to_string(),
            filters: vec![],
            limit: None,
            offset: None,
            columns: None,
            order_by: Some(("value".to_string(), OrderDirection::Desc)),
        };

        let rows = vec![
            Row::new(1, vec![Value::Int32(5)]),
            Row::new(2, vec![Value::Int32(2)]),
            Row::new(3, vec![Value::Int32(8)]),
        ];

        let result = plan.apply_ordering(rows, &mapping);

        assert_eq!(result[0].row_id, 3); // value = 8
        assert_eq!(result[1].row_id, 1); // value = 5
        assert_eq!(result[2].row_id, 2); // value = 2
    }

    #[test]
    fn test_query_plan_projection() {
        let mut mapping = HashMap::new();
        mapping.insert("id".to_string(), 0);
        mapping.insert("name".to_string(), 1);
        mapping.insert("age".to_string(), 2);

        let plan = QueryPlan {
            table: "users".to_string(),
            filters: vec![],
            limit: None,
            offset: None,
            columns: Some(vec!["id".to_string(), "name".to_string()]),
            order_by: None,
        };

        let rows = vec![Row::new(
            1,
            vec![
                Value::Int32(100),
                Value::varchar("Alice".to_string()),
                Value::Int32(25),
            ],
        )];

        let result = plan.apply_projection(rows, &mapping);

        assert_eq!(result[0].values.len(), 2);
        assert_eq!(result[0].values[0], Value::Int32(100));
        assert_eq!(result[0].values[1], Value::varchar("Alice".to_string()));
    }

    #[test]
    fn test_builder_chaining() {
        let builder = QueryBuilder::from("users");

        // Test that methods return Self and can be chained
        let query = builder
            .filter("age", Operator::GreaterThan(Value::Int32(18)))
            .filter("name", Operator::Like("J%".to_string()))
            .limit(10)
            .offset(5)
            .order_by_asc("name")
            .build();

        assert_eq!(query.table, "users");
        assert_eq!(query.filters.len(), 2);
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.offset, Some(5));
        assert!(query.order_by.is_some());
    }
}
