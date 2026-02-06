// Statement validator - Step 4.3
//
// Validates SQL statements for correctness

use crate::error::{Error, Result};
use crate::parser::ast::*;
use std::collections::HashSet;

/// SQL statement validator
///
/// Performs semantic validation of parsed SQL statements.
/// For the MVP, this provides basic validation. Full schema validation
/// will be added when table metadata is implemented.
pub struct Validator {
    /// Known tables (for MVP, this would come from catalog)
    known_tables: HashSet<String>,
}

impl Validator {
    /// Create a new validator
    pub fn new() -> Self {
        Self {
            known_tables: HashSet::new(),
        }
    }

    /// Register a table as valid
    pub fn add_table(&mut self, table: impl Into<String>) {
        self.known_tables.insert(table.into());
    }

    /// Validate a statement
    pub fn validate(&self, stmt: &Statement) -> Result<()> {
        match stmt {
            Statement::Select(s) => self.validate_select(s),
            Statement::Insert(i) => self.validate_insert(i),
            Statement::Update(u) => self.validate_update(u),
            Statement::Delete(d) => self.validate_delete(d),
            Statement::ShowTables => Ok(()), // No validation needed
            Statement::ShowDatabases => Ok(()), // No validation needed
            Statement::Use(_) => Ok(()), // No validation needed - database path is checked at runtime
            Statement::CreateTable(_) => Ok(()), // Basic structural validation done by parser
            Statement::DropTable(_) => Ok(()), // Basic structural validation done by parser
        }
    }

    /// Validate SELECT statement
    fn validate_select(&self, stmt: &SelectStatement) -> Result<()> {
        // Check table exists
        if !self.known_tables.is_empty() && !self.known_tables.contains(&stmt.from) {
            return Err(Error::Parser(format!("Table not found: {}", stmt.from)));
        }

        // Validate WHERE clause
        if let Some(ref where_expr) = stmt.where_clause {
            self.validate_expression(where_expr)?;
        }

        // Validate ORDER BY column exists (basic check)
        if let Some(ref order_by) = stmt.order_by {
            if order_by.column.is_empty() {
                return Err(Error::Parser("ORDER BY column cannot be empty".to_string()));
            }
        }

        // Validate LIMIT/OFFSET
        if let Some(limit) = stmt.limit {
            if limit == 0 {
                return Err(Error::Parser("LIMIT must be greater than 0".to_string()));
            }
        }

        Ok(())
    }

    /// Validate INSERT statement
    fn validate_insert(&self, stmt: &InsertStatement) -> Result<()> {
        // Check table exists
        if !self.known_tables.is_empty() && !self.known_tables.contains(&stmt.table) {
            return Err(Error::Parser(format!("Table not found: {}", stmt.table)));
        }

        // Check we have values
        if stmt.values.is_empty() {
            return Err(Error::Parser("INSERT must have at least one value".to_string()));
        }

        // If columns specified, check counts match
        if let Some(ref columns) = stmt.columns {
            if columns.len() != stmt.values.len() {
                return Err(Error::Parser(format!(
                    "Column count ({}) does not match value count ({})",
                    columns.len(),
                    stmt.values.len()
                )));
            }
        }

        Ok(())
    }

    /// Validate UPDATE statement
    fn validate_update(&self, stmt: &UpdateStatement) -> Result<()> {
        // Check table exists
        if !self.known_tables.is_empty() && !self.known_tables.contains(&stmt.table) {
            return Err(Error::Parser(format!("Table not found: {}", stmt.table)));
        }

        // Check we have assignments
        if stmt.assignments.is_empty() {
            return Err(Error::Parser(
                "UPDATE must have at least one assignment".to_string(),
            ));
        }

        // Validate assignment expressions
        for assignment in &stmt.assignments {
            if assignment.column.is_empty() {
                return Err(Error::Parser("Assignment column cannot be empty".to_string()));
            }
            self.validate_expression(&assignment.value)?;
        }

        // Validate WHERE clause
        if let Some(ref where_expr) = stmt.where_clause {
            self.validate_expression(where_expr)?;
        }

        Ok(())
    }

    /// Validate DELETE statement
    fn validate_delete(&self, stmt: &DeleteStatement) -> Result<()> {
        // Check table exists
        if !self.known_tables.is_empty() && !self.known_tables.contains(&stmt.table) {
            return Err(Error::Parser(format!("Table not found: {}", stmt.table)));
        }

        // Validate WHERE clause
        if let Some(ref where_expr) = stmt.where_clause {
            self.validate_expression(where_expr)?;
        } else {
            // Warn about DELETE without WHERE (would delete all rows)
            // In production, you might want to require explicit confirmation
        }

        Ok(())
    }

    /// Validate an expression
    fn validate_expression(&self, expr: &Expression) -> Result<()> {
        match expr {
            Expression::Literal(_) => Ok(()),
            Expression::Column(name) => {
                if name.is_empty() {
                    Err(Error::Parser("Column name cannot be empty".to_string()))
                } else {
                    Ok(())
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.validate_expression(left)?;
                self.validate_expression(right)?;
                Ok(())
            }
            Expression::UnaryOp { operand, .. } => self.validate_expression(operand),
            Expression::In { expr, list } => {
                self.validate_expression(expr)?;
                if list.is_empty() {
                    return Err(Error::Parser("IN list cannot be empty".to_string()));
                }
                for item in list {
                    self.validate_expression(item)?;
                }
                Ok(())
            }
            Expression::Between { expr, low, high } => {
                self.validate_expression(expr)?;
                self.validate_expression(low)?;
                self.validate_expression(high)?;
                Ok(())
            }
            Expression::IsNull(expr) | Expression::IsNotNull(expr) => {
                self.validate_expression(expr)
            }
            Expression::Like { expr, pattern } => {
                self.validate_expression(expr)?;
                if pattern.is_empty() {
                    return Err(Error::Parser("LIKE pattern cannot be empty".to_string()));
                }
                Ok(())
            }
        }
    }
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a parsed statement
///
/// This is a convenience function that creates a validator and runs validation.
/// For MVP without schema, this does basic structural validation.
pub fn validate_statement(stmt: &Statement) -> Result<()> {
    let validator = Validator::new();
    validator.validate(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql;

    #[test]
    fn test_validate_select_basic() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_select_with_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE age > 18").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_select_zero_limit() {
        let stmt = parse_sql("SELECT * FROM users LIMIT 0").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_insert_basic() {
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_update_basic() {
        let stmt = parse_sql("UPDATE users SET age = 26 WHERE id = 1").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_delete_basic() {
        let stmt = parse_sql("DELETE FROM users WHERE age < 18").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_with_known_tables() {
        let mut validator = Validator::new();
        validator.add_table("users");
        validator.add_table("orders");

        let stmt = parse_sql("SELECT * FROM users").unwrap();
        assert!(validator.validate(&stmt).is_ok());

        let stmt = parse_sql("SELECT * FROM unknown").unwrap();
        assert!(validator.validate(&stmt).is_err());
    }

    #[test]
    fn test_validate_in_empty_list() {
        // Empty IN lists fail at parse time, which is correct
        let result = parse_sql("SELECT * FROM users WHERE id IN ()");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_complex_where() {
        let stmt =
            parse_sql("SELECT * FROM users WHERE age > 18 AND status = 'active'").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_between() {
        let stmt = parse_sql("SELECT * FROM users WHERE age BETWEEN 18 AND 65").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_like() {
        let stmt = parse_sql("SELECT * FROM users WHERE name LIKE 'John%'").unwrap();
        let result = validate_statement(&stmt);
        assert!(result.is_ok());
    }
}
