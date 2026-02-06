// AST definitions - Step 4.2
//
// Abstract Syntax Tree for SQL statements

use crate::storage::Value;

/// SQL Statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Columns to select (None = *)
    pub columns: Vec<SelectColumn>,
    /// Table name
    pub from: String,
    /// WHERE clause (optional)
    pub where_clause: Option<Expression>,
    /// ORDER BY clause (optional)
    pub order_by: Option<OrderByClause>,
    /// LIMIT (optional)
    pub limit: Option<usize>,
    /// OFFSET (optional)
    pub offset: Option<usize>,
}

/// Column in SELECT
#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    /// * (all columns)
    Star,
    /// Specific column name
    Column(String),
    /// Column with alias (column AS alias)
    ColumnWithAlias(String, String),
}

/// ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub column: String,
    pub direction: OrderDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Table name
    pub table: String,
    /// Column names (optional, if specified)
    pub columns: Option<Vec<String>>,
    /// Values to insert
    pub values: Vec<Value>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Table name
    pub table: String,
    /// SET clause (column = value pairs)
    pub assignments: Vec<Assignment>,
    /// WHERE clause (optional)
    pub where_clause: Option<Expression>,
}

/// Assignment in UPDATE SET clause
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Table name
    pub table: String,
    /// WHERE clause (optional)
    pub where_clause: Option<Expression>,
}

/// Expression in WHERE clause or values
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Literal value
    Literal(Value),
    /// Column reference
    Column(String),
    /// Binary operation (e.g., age > 18)
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    /// Unary operation (e.g., NOT active)
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
    /// IN operation (e.g., id IN (1, 2, 3))
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
    },
    /// BETWEEN operation (e.g., age BETWEEN 18 AND 65)
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
    },
    /// IS NULL
    IsNull(Box<Expression>),
    /// IS NOT NULL
    IsNotNull(Box<Expression>),
    /// LIKE pattern (e.g., name LIKE 'John%')
    Like {
        expr: Box<Expression>,
        pattern: String,
    },
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
}

impl Statement {
    /// Get a description of the statement type
    pub fn statement_type(&self) -> &str {
        match self {
            Statement::Select(_) => "SELECT",
            Statement::Insert(_) => "INSERT",
            Statement::Update(_) => "UPDATE",
            Statement::Delete(_) => "DELETE",
        }
    }
}

impl SelectStatement {
    /// Check if this is SELECT *
    pub fn is_select_star(&self) -> bool {
        self.columns.len() == 1 && matches!(self.columns[0], SelectColumn::Star)
    }

    /// Get column names (excluding *)
    pub fn get_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|col| match col {
                SelectColumn::Column(name) => Some(name.clone()),
                SelectColumn::ColumnWithAlias(name, _) => Some(name.clone()),
                SelectColumn::Star => None,
            })
            .collect()
    }
}

impl Expression {
    /// Create a simple literal expression
    pub fn literal(value: Value) -> Self {
        Expression::Literal(value)
    }

    /// Create a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(name.into())
    }

    /// Create a binary operation
    pub fn binary_op(left: Expression, op: BinaryOperator, right: Expression) -> Self {
        Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_statement_basic() {
        let stmt = SelectStatement {
            columns: vec![SelectColumn::Star],
            from: "users".to_string(),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        assert!(stmt.is_select_star());
        assert_eq!(stmt.get_column_names().len(), 0);
    }

    #[test]
    fn test_select_statement_columns() {
        let stmt = SelectStatement {
            columns: vec![
                SelectColumn::Column("id".to_string()),
                SelectColumn::Column("name".to_string()),
            ],
            from: "users".to_string(),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        };

        assert!(!stmt.is_select_star());
        assert_eq!(stmt.get_column_names(), vec!["id", "name"]);
    }

    #[test]
    fn test_expression_creation() {
        let expr = Expression::literal(Value::Int32(42));
        assert!(matches!(expr, Expression::Literal(_)));

        let expr = Expression::column("age");
        assert!(matches!(expr, Expression::Column(_)));

        let expr = Expression::binary_op(
            Expression::column("age"),
            BinaryOperator::GreaterThan,
            Expression::literal(Value::Int32(18)),
        );
        assert!(matches!(expr, Expression::BinaryOp { .. }));
    }

    #[test]
    fn test_statement_type() {
        let select = Statement::Select(SelectStatement {
            columns: vec![SelectColumn::Star],
            from: "users".to_string(),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        });

        assert_eq!(select.statement_type(), "SELECT");
    }

    #[test]
    fn test_insert_statement() {
        let stmt = InsertStatement {
            table: "users".to_string(),
            columns: None,
            values: vec![
                Value::Int32(1),
                Value::Varchar("Alice".to_string()),
                Value::Int32(25),
            ],
        };

        assert_eq!(stmt.table, "users");
        assert_eq!(stmt.values.len(), 3);
    }

    #[test]
    fn test_update_statement() {
        let stmt = UpdateStatement {
            table: "users".to_string(),
            assignments: vec![Assignment {
                column: "age".to_string(),
                value: Expression::literal(Value::Int32(26)),
            }],
            where_clause: Some(Expression::binary_op(
                Expression::column("id"),
                BinaryOperator::Equals,
                Expression::literal(Value::Int32(1)),
            )),
        };

        assert_eq!(stmt.table, "users");
        assert_eq!(stmt.assignments.len(), 1);
        assert!(stmt.where_clause.is_some());
    }

    #[test]
    fn test_delete_statement() {
        let stmt = DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(Expression::binary_op(
                Expression::column("age"),
                BinaryOperator::LessThan,
                Expression::literal(Value::Int32(18)),
            )),
        };

        assert_eq!(stmt.table, "users");
        assert!(stmt.where_clause.is_some());
    }
}
