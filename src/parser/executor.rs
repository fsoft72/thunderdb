// Query executor - Step 4.4
//
// Executes parsed SQL statements using the Direct API

use crate::error::{Error, Result};
use crate::parser::ast::*;
use crate::query::{Filter, Operator, QueryBuilder, OrderDirection as QueryOrderDirection};
use crate::storage::Value;

/// Execute a SQL statement
///
/// This is a simplified executor that demonstrates how to convert AST to Direct API calls.
/// A full implementation would integrate with Database/TableEngine.
///
/// For now, this module provides the conversion logic from AST to query operations.
pub struct Executor;

impl Executor {
    /// Convert a SELECT statement to a QueryBuilder
    pub fn select_to_query(stmt: &SelectStatement) -> QueryBuilder {
        let mut query = QueryBuilder::from(stmt.from.base_table_name());

        // Apply WHERE clause
        if let Some(ref where_expr) = stmt.where_clause {
            if let Ok(filters) = Self::expression_to_filters(where_expr) {
                query = query.filters(filters);
            }
        }

        // Apply ORDER BY
        if let Some(ref order_by) = stmt.order_by {
            let direction = match order_by.direction {
                OrderDirection::Asc => QueryOrderDirection::Asc,
                OrderDirection::Desc => QueryOrderDirection::Desc,
            };
            query = query.order_by(&order_by.column, direction);
        }

        // Apply LIMIT
        if let Some(limit) = stmt.limit {
            query = query.limit(limit);
        }

        // Apply OFFSET
        if let Some(offset) = stmt.offset {
            query = query.offset(offset);
        }

        // Apply column selection
        if !stmt.is_select_star() {
            let columns = stmt.get_column_names();
            query = query.select(columns);
        }

        query
    }

    /// Convert an Expression to Filter list
    ///
    /// Returns all filters that should be AND-combined
    pub fn expression_to_filters(expr: &Expression) -> Result<Vec<Filter>> {
        let mut filters = Vec::new();
        Self::collect_filters(expr, &mut filters)?;
        Ok(filters)
    }

    /// Recursively collect filters from an expression
    fn collect_filters(expr: &Expression, filters: &mut Vec<Filter>) -> Result<()> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        // AND - collect from both sides
                        Self::collect_filters(left, filters)?;
                        Self::collect_filters(right, filters)?;
                    }
                    BinaryOperator::Or => {
                        // OR is not directly supported in our filter system
                        // Would need more complex query planning
                        return Err(Error::Query(
                            "OR operator requires full table scan".to_string(),
                        ));
                    }
                    _ => {
                        // Comparison operator - create a filter
                        let col_name = match left.as_ref() {
                            Expression::Column(name) => name.clone(),
                            Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                            _ => {
                                return Err(Error::Query(
                                    "Left side of comparison must be a column".to_string(),
                                ));
                            }
                        };
                        let value = Self::expression_to_value(right)?;
                        let operator = Self::binary_op_to_operator(op, value)?;
                        filters.push(Filter::new(col_name, operator));
                    }
                }
            }
            Expression::Like { expr, pattern } => {
                let col_name = match expr.as_ref() {
                    Expression::Column(name) => name.clone(),
                    Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                    _ => return Err(Error::Query("LIKE requires a column reference".to_string())),
                };
                filters.push(Filter::new(col_name, Operator::Like(pattern.clone())));
            }
            Expression::In { expr, list } => {
                let col_name = match expr.as_ref() {
                    Expression::Column(name) => name.clone(),
                    Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                    _ => return Err(Error::Query("IN requires a column reference".to_string())),
                };
                let values: Result<Vec<Value>> =
                    list.iter().map(|e| Self::expression_to_value(e)).collect();
                filters.push(Filter::new(col_name, Operator::In(values?)));
            }
            Expression::Between { expr, low, high } => {
                let col_name = match expr.as_ref() {
                    Expression::Column(name) => name.clone(),
                    Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                    _ => return Err(Error::Query("BETWEEN requires a column reference".to_string())),
                };
                let low_val = Self::expression_to_value(low)?;
                let high_val = Self::expression_to_value(high)?;
                filters.push(Filter::new(col_name, Operator::Between(low_val, high_val)));
            }
            Expression::IsNull(expr) => {
                let col_name = match expr.as_ref() {
                    Expression::Column(name) => name.clone(),
                    Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                    _ => return Err(Error::Query("IS NULL requires a column reference".to_string())),
                };
                filters.push(Filter::new(col_name, Operator::IsNull));
            }
            Expression::IsNotNull(expr) => {
                let col_name = match expr.as_ref() {
                    Expression::Column(name) => name.clone(),
                    Expression::QualifiedColumn(table, col) => format!("{}.{}", table, col),
                    _ => return Err(Error::Query("IS NOT NULL requires a column reference".to_string())),
                };
                filters.push(Filter::new(col_name, Operator::IsNotNull));
            }
            _ => {
                return Err(Error::Query(format!(
                    "Unsupported expression in WHERE: {:?}",
                    expr
                )));
            }
        }

        Ok(())
    }

    /// Convert Expression to Value
    fn expression_to_value(expr: &Expression) -> Result<Value> {
        match expr {
            Expression::Literal(val) => Ok(val.clone()),
            _ => Err(Error::Query("Expected literal value".to_string())),
        }
    }

    /// Convert BinaryOperator to Operator
    fn binary_op_to_operator(op: &BinaryOperator, value: Value) -> Result<Operator> {
        Ok(match op {
            BinaryOperator::Equals => Operator::Equals(value),
            BinaryOperator::NotEquals => Operator::NotEquals(value),
            BinaryOperator::LessThan => Operator::LessThan(value),
            BinaryOperator::LessThanOrEqual => Operator::LessThanOrEqual(value),
            BinaryOperator::GreaterThan => Operator::GreaterThan(value),
            BinaryOperator::GreaterThanOrEqual => Operator::GreaterThanOrEqual(value),
            _ => {
                return Err(Error::Query(format!(
                    "Unsupported operator in WHERE: {:?}",
                    op
                )));
            }
        })
    }

    /// Get values from INSERT statement
    pub fn get_insert_values(stmt: &InsertStatement) -> Vec<Value> {
        stmt.values.clone()
    }

    /// Convert UPDATE assignments to (column, value) pairs
    pub fn get_update_assignments(stmt: &UpdateStatement) -> Result<Vec<(String, Value)>> {
        let mut assignments = Vec::new();

        for assignment in &stmt.assignments {
            let value = Self::expression_to_value(&assignment.value)?;
            assignments.push((assignment.column.clone(), value));
        }

        Ok(assignments)
    }

    /// Get WHERE filters for UPDATE/DELETE
    pub fn get_where_filters(where_clause: &Option<Expression>) -> Result<Vec<Filter>> {
        if let Some(expr) = where_clause {
            Self::expression_to_filters(expr)
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;

    #[test]
    fn test_select_to_query_simple() {
        let sql = "SELECT * FROM users";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            let query = Executor::select_to_query(&select);
            assert_eq!(query.table(), "users");
            assert_eq!(query.get_filters().len(), 0);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_to_query_with_where() {
        let sql = "SELECT * FROM users WHERE age > 18";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            let query = Executor::select_to_query(&select);
            assert_eq!(query.get_filters().len(), 1);
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_select_to_query_with_limit() {
        let sql = "SELECT * FROM users LIMIT 10 OFFSET 5";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            let query = Executor::select_to_query(&select);
            assert_eq!(query.get_limit(), Some(10));
            assert_eq!(query.get_offset(), Some(5));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_expression_to_filters_simple() {
        let sql = "SELECT * FROM users WHERE age = 25";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(expr) = select.where_clause {
                let filters = Executor::expression_to_filters(&expr).unwrap();
                assert_eq!(filters.len(), 1);
                assert_eq!(filters[0].column, "age");
            }
        }
    }

    #[test]
    fn test_expression_to_filters_and() {
        let sql = "SELECT * FROM users WHERE age > 18 AND status = 'active'";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(expr) = select.where_clause {
                let filters = Executor::expression_to_filters(&expr).unwrap();
                assert_eq!(filters.len(), 2);
            }
        }
    }

    #[test]
    fn test_expression_to_filters_like() {
        let sql = "SELECT * FROM users WHERE name LIKE 'John%'";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(expr) = select.where_clause {
                let filters = Executor::expression_to_filters(&expr).unwrap();
                assert_eq!(filters.len(), 1);
                assert!(matches!(filters[0].operator, Operator::Like(_)));
            }
        }
    }

    #[test]
    fn test_expression_to_filters_in() {
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(expr) = select.where_clause {
                let filters = Executor::expression_to_filters(&expr).unwrap();
                assert_eq!(filters.len(), 1);
                assert!(matches!(filters[0].operator, Operator::In(_)));
            }
        }
    }

    #[test]
    fn test_expression_to_filters_between() {
        let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Select(select) = stmt {
            if let Some(expr) = select.where_clause {
                let filters = Executor::expression_to_filters(&expr).unwrap();
                assert_eq!(filters.len(), 1);
                assert!(matches!(filters[0].operator, Operator::Between(_, _)));
            }
        }
    }

    #[test]
    fn test_get_insert_values() {
        let sql = "INSERT INTO users VALUES (1, 'Alice', 25)";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Insert(insert) = stmt {
            let values = Executor::get_insert_values(&insert);
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected INSERT statement");
        }
    }

    #[test]
    fn test_get_update_assignments() {
        let sql = "UPDATE users SET age = 26, status = 'active'";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Update(update) = stmt {
            let assignments = Executor::get_update_assignments(&update).unwrap();
            assert_eq!(assignments.len(), 2);
            assert_eq!(assignments[0].0, "age");
            assert_eq!(assignments[1].0, "status");
        } else {
            panic!("Expected UPDATE statement");
        }
    }

    #[test]
    fn test_get_where_filters() {
        let sql = "DELETE FROM users WHERE age < 18";
        let stmt = parse_sql(sql).unwrap();

        if let Statement::Delete(delete) = stmt {
            let filters = Executor::get_where_filters(&delete.where_clause).unwrap();
            assert_eq!(filters.len(), 1);
        } else {
            panic!("Expected DELETE statement");
        }
    }
}
