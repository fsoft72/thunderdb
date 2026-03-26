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
    ShowTables,
    ShowDatabases,
    Use(String),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropTable(String),
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table: String,
    pub column: String,
}

/// Table reference with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

/// Column reference, optionally qualified with table/alias
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

/// JOIN type
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// FROM clause — single table or chain of joins
#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    /// Single table: FROM users u
    Table(TableRef),
    /// Join: FROM users u JOIN posts p ON u.id = p.author_id
    Join {
        left: Box<FromClause>,
        join_type: JoinType,
        right: TableRef,
        on_left: ColumnRef,
        on_right: ColumnRef,
    },
}

impl FromClause {
    /// Get the base table name (leftmost table in join chain)
    pub fn base_table_name(&self) -> &str {
        match self {
            FromClause::Table(t) => &t.name,
            FromClause::Join { left, .. } => left.base_table_name(),
        }
    }

    /// Check if this is a simple single-table FROM (no joins)
    pub fn is_single_table(&self) -> bool {
        matches!(self, FromClause::Table(_))
    }
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
}

/// Data types supported by the engine
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Varchar,
    Float,
    Timestamp,
    Boolean,
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// Columns to select (None = *)
    pub columns: Vec<SelectColumn>,
    /// FROM clause (table or join)
    pub from: FromClause,
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
    /// COUNT(*)
    CountStar,
    /// Qualified column reference (table.column)
    QualifiedColumn(String, String),
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
    /// Qualified column reference (table.column or alias.column)
    QualifiedColumn(String, String),
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
            Statement::ShowTables => "SHOW TABLES",
            Statement::ShowDatabases => "SHOW DATABASES",
            Statement::Use(_) => "USE",
            Statement::CreateTable(_) => "CREATE TABLE",
            Statement::CreateIndex(_) => "CREATE INDEX",
            Statement::DropTable(_) => "DROP TABLE",
        }
    }
}

impl SelectStatement {
    /// Check if this is SELECT *
    pub fn is_select_star(&self) -> bool {
        self.columns.len() == 1 && matches!(self.columns[0], SelectColumn::Star)
    }

    /// Check if this is SELECT COUNT(*)
    pub fn is_count_star(&self) -> bool {
        self.columns.len() == 1 && matches!(self.columns[0], SelectColumn::CountStar)
    }

    /// Get column names (excluding * and COUNT(*))
    pub fn get_column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter_map(|col| match col {
                SelectColumn::Column(name) => Some(name.clone()),
                SelectColumn::ColumnWithAlias(name, _) => Some(name.clone()),
                SelectColumn::QualifiedColumn(table, col) => Some(format!("{}.{}", table, col)),
                SelectColumn::Star | SelectColumn::CountStar => None,
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
            from: FromClause::Table(TableRef { name: "users".to_string(), alias: None }),
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
            from: FromClause::Table(TableRef { name: "users".to_string(), alias: None }),
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
            from: FromClause::Table(TableRef { name: "users".to_string(), alias: None }),
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
                Value::varchar("Alice".to_string()),
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
