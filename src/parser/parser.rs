// Recursive descent parser - Step 4.2
//
// Converts tokens into AST

use crate::error::{Error, Result};
use crate::parser::ast::*;
use crate::parser::token::{Token, Tokenizer};
use crate::storage::Value;

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Create a new parser from SQL text
    pub fn new(sql: &str) -> Result<Self> {
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize()?;

        Ok(Self {
            tokens,
            position: 0,
        })
    }

    /// Parse SQL into a Statement
    pub fn parse(&mut self) -> Result<Statement> {
        if self.is_at_end() {
            return Err(Error::Parser("Empty input".to_string()));
        }

        let stmt = match self.current() {
            Token::Select => self.parse_select()?,
            Token::Insert => self.parse_insert()?,
            Token::Update => self.parse_update()?,
            Token::Delete => self.parse_delete()?,
            Token::Show => self.parse_show()?,
            Token::Use => self.parse_use()?,
            Token::Create => self.parse_create()?,
            Token::Drop => self.parse_drop()?,
            _ => {
                return Err(Error::Parser(format!(
                    "Expected SELECT, INSERT, UPDATE, DELETE, SHOW, USE, CREATE, or DROP, got {:?}",
                    self.current()
                )));
            }
        };

        // Expect EOF or semicolon
        if !self.is_at_end() && !matches!(self.current(), Token::Eof | Token::Semicolon) {
            return Err(Error::Parser(format!(
                "Unexpected token after statement: {:?}",
                self.current()
            )));
        }

        Ok(stmt)
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(Token::Select)?;

        // Parse columns
        let columns = self.parse_select_columns()?;

        // FROM clause
        self.expect(Token::From)?;
        let from = self.expect_identifier()?;

        // WHERE clause (optional)
        let where_clause = if self.match_token(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // ORDER BY (optional)
        let order_by = if self.match_token(&Token::OrderBy) {
            let column = self.expect_identifier()?;
            let direction = if self.match_token(&Token::Asc) {
                OrderDirection::Asc
            } else if self.match_token(&Token::Desc) {
                OrderDirection::Desc
            } else {
                OrderDirection::Asc // Default
            };

            Some(OrderByClause { column, direction })
        } else {
            None
        };

        // LIMIT (optional)
        let limit = if self.match_token(&Token::Limit) {
            Some(self.expect_number()? as usize)
        } else {
            None
        };

        // OFFSET (optional)
        let offset = if self.match_token(&Token::Offset) {
            Some(self.expect_number()? as usize)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            columns,
            from,
            where_clause,
            order_by,
            limit,
            offset,
        }))
    }

    /// Parse SELECT columns
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        if self.match_token(&Token::Star) {
            columns.push(SelectColumn::Star);
            return Ok(columns);
        }

        loop {
            let col_name = self.expect_identifier()?;

            // Check for AS alias
            if self.match_token(&Token::As) {
                let alias = self.expect_identifier()?;
                columns.push(SelectColumn::ColumnWithAlias(col_name, alias));
            } else {
                columns.push(SelectColumn::Column(col_name));
            }

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table = self.expect_identifier()?;

        // Optional column list
        let columns = if self.match_token(&Token::LeftParen) {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_identifier()?);
                if !self.match_token(&Token::Comma) {
                    break;
                }
            }
            self.expect(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // VALUES
        self.expect(Token::Values)?;
        self.expect(Token::LeftParen)?;

        let mut values = Vec::new();
        loop {
            values.push(self.parse_value()?);
            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(Token::Update)?;

        let table = self.expect_identifier()?;

        self.expect(Token::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect(Token::Equals)?;
            let value = self.parse_expression()?;

            assignments.push(Assignment { column, value });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        // WHERE clause (optional)
        let where_clause = if self.match_token(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table = self.expect_identifier()?;

        // WHERE clause (optional)
        let where_clause = if self.match_token(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    /// Parse SHOW statement
    fn parse_show(&mut self) -> Result<Statement> {
        self.expect(Token::Show)?;

        match self.current() {
            Token::Tables => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            Token::Databases => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            _ => Err(Error::Parser(format!(
                "Expected TABLES or DATABASES after SHOW, got {:?}",
                self.current()
            ))),
        }
    }

    /// Parse USE statement
    fn parse_use(&mut self) -> Result<Statement> {
        self.expect(Token::Use)?;
        let db_name = self.expect_identifier()?;
        Ok(Statement::Use(db_name))
    }

    /// Parse CREATE statement
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;

        match self.current() {
            Token::Table => self.parse_create_table(),
            _ => Err(Error::Parser(format!(
                "Expected TABLE after CREATE, got {:?}",
                self.current()
            ))),
        }
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(Token::Table)?;

        let name = self.expect_identifier()?;

        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let col_name = self.expect_identifier()?;
            let data_type = self.parse_data_type()?;

            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
            });

            if !self.match_token(&Token::Comma) {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStatement { name, columns }))
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> Result<DataType> {
        let type_name = self.expect_identifier()?.to_uppercase();

        match type_name.as_str() {
            "INT" | "INTEGER" => Ok(DataType::Int),
            "VARCHAR" | "TEXT" | "STRING" => Ok(DataType::Varchar),
            "FLOAT" | "DOUBLE" | "REAL" => Ok(DataType::Float),
            "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp),
            "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
            _ => Err(Error::Parser(format!("Unsupported data type: {}", type_name))),
        }
    }

    /// Parse DROP statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(Token::Drop)?;

        match self.current() {
            Token::Table => self.parse_drop_table(),
            _ => Err(Error::Parser(format!(
                "Expected TABLE after DROP, got {:?}",
                self.current()
            ))),
        }
    }

    /// Parse DROP TABLE statement
    fn parse_drop_table(&mut self) -> Result<Statement> {
        self.expect(Token::Table)?;
        let name = self.expect_identifier()?;
        Ok(Statement::DropTable(name))
    }

    /// Parse expression (handles precedence)
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or()
    }

    /// Parse OR expression
    fn parse_or(&mut self) -> Result<Expression> {
        let mut left = self.parse_and()?;

        while self.match_token(&Token::Or) {
            let right = self.parse_and()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and(&mut self) -> Result<Expression> {
        let mut left = self.parse_comparison()?;

        while self.match_token(&Token::And) {
            let right = self.parse_comparison()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse comparison expression
    fn parse_comparison(&mut self) -> Result<Expression> {
        let left = self.parse_term()?;

        // Check for comparison operators
        let op = match self.current() {
            Token::Equals => {
                self.advance();
                BinaryOperator::Equals
            }
            Token::NotEquals => {
                self.advance();
                BinaryOperator::NotEquals
            }
            Token::LessThan => {
                self.advance();
                BinaryOperator::LessThan
            }
            Token::LessThanOrEqual => {
                self.advance();
                BinaryOperator::LessThanOrEqual
            }
            Token::GreaterThan => {
                self.advance();
                BinaryOperator::GreaterThan
            }
            Token::GreaterThanOrEqual => {
                self.advance();
                BinaryOperator::GreaterThanOrEqual
            }
            Token::Like => {
                self.advance();
                let pattern = self.expect_string()?;
                return Ok(Expression::Like {
                    expr: Box::new(left),
                    pattern,
                });
            }
            Token::In => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let mut list = Vec::new();
                loop {
                    list.push(self.parse_expression()?);
                    if !self.match_token(&Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::RightParen)?;
                return Ok(Expression::In {
                    expr: Box::new(left),
                    list,
                });
            }
            Token::Between => {
                self.advance();
                let low = self.parse_term()?;
                self.expect(Token::And)?;
                let high = self.parse_term()?;
                return Ok(Expression::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                });
            }
            Token::Is => {
                self.advance();
                if self.match_token(&Token::Not) {
                    self.expect(Token::Null)?;
                    return Ok(Expression::IsNotNull(Box::new(left)));
                } else {
                    self.expect(Token::Null)?;
                    return Ok(Expression::IsNull(Box::new(left)));
                }
            }
            _ => return Ok(left),
        };

        let right = self.parse_term()?;

        Ok(Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    /// Parse term (addition/subtraction)
    fn parse_term(&mut self) -> Result<Expression> {
        let mut left = self.parse_factor()?;

        while matches!(self.current(), Token::Plus | Token::Minus) {
            let op = match self.current() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Subtract,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_factor()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse factor (multiplication/division)
    fn parse_factor(&mut self) -> Result<Expression> {
        let mut left = self.parse_unary()?;

        while matches!(self.current(), Token::Star | Token::Slash) {
            let op = match self.current() {
                Token::Star => BinaryOperator::Multiply,
                Token::Slash => BinaryOperator::Divide,
                _ => unreachable!(),
            };
            self.advance();

            let right = self.parse_unary()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse unary expression
    fn parse_unary(&mut self) -> Result<Expression> {
        if self.match_token(&Token::Not) {
            let operand = self.parse_unary()?;
            return Ok(Expression::UnaryOp {
                op: UnaryOperator::Not,
                operand: Box::new(operand),
            });
        }

        if self.match_token(&Token::Minus) {
            let operand = self.parse_unary()?;
            return Ok(Expression::UnaryOp {
                op: UnaryOperator::Minus,
                operand: Box::new(operand),
            });
        }

        self.parse_primary()
    }

    /// Parse primary expression
    fn parse_primary(&mut self) -> Result<Expression> {
        match self.current() {
            Token::Number(n) => {
                let num = *n;
                self.advance();
                // Determine if int or float
                if num.fract() == 0.0 && num >= i32::MIN as f64 && num <= i32::MAX as f64 {
                    Ok(Expression::Literal(Value::Int32(num as i32)))
                } else {
                    Ok(Expression::Literal(Value::Float64(num)))
                }
            }
            Token::String(s) => {
                let string = s.clone();
                self.advance();
                Ok(Expression::Literal(Value::varchar(string)))
            }
            Token::Identifier(name) => {
                let identifier = name.clone();
                self.advance();
                Ok(Expression::Column(identifier))
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Literal(Value::Null))
            }
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(expr)
            }
            _ => Err(Error::Parser(format!(
                "Unexpected token in expression: {:?}",
                self.current()
            ))),
        }
    }

    /// Parse a value (for INSERT VALUES)
    fn parse_value(&mut self) -> Result<Value> {
        match self.current() {
            Token::Number(n) => {
                let num = *n;
                self.advance();
                if num.fract() == 0.0 && num >= i32::MIN as f64 && num <= i32::MAX as f64 {
                    Ok(Value::Int32(num as i32))
                } else {
                    Ok(Value::Float64(num))
                }
            }
            Token::String(s) => {
                let string = s.clone();
                self.advance();
                Ok(Value::varchar(string))
            }
            Token::Null => {
                self.advance();
                Ok(Value::Null)
            }
            _ => Err(Error::Parser(format!(
                "Expected value, got {:?}",
                self.current()
            ))),
        }
    }

    /// Expect a specific token
    fn expect(&mut self, expected: Token) -> Result<()> {
        if self.current() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(Error::Parser(format!(
                "Expected {:?}, got {:?}",
                expected,
                self.current()
            )))
        }
    }

    /// Expect an identifier
    fn expect_identifier(&mut self) -> Result<String> {
        if let Token::Identifier(name) = self.current() {
            let identifier = name.clone();
            self.advance();
            Ok(identifier)
        } else {
            Err(Error::Parser(format!(
                "Expected identifier, got {:?}",
                self.current()
            )))
        }
    }

    /// Expect a string
    fn expect_string(&mut self) -> Result<String> {
        if let Token::String(s) = self.current() {
            let string = s.clone();
            self.advance();
            Ok(string)
        } else {
            Err(Error::Parser(format!(
                "Expected string, got {:?}",
                self.current()
            )))
        }
    }

    /// Expect a number
    fn expect_number(&mut self) -> Result<f64> {
        if let Token::Number(n) = self.current() {
            let num = *n;
            self.advance();
            Ok(num)
        } else {
            Err(Error::Parser(format!(
                "Expected number, got {:?}",
                self.current()
            )))
        }
    }

    /// Match and consume a token
    fn match_token(&mut self, expected: &Token) -> bool {
        if self.current() == expected {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Get current token
    fn current(&self) -> &Token {
        &self.tokens[self.position]
    }

    /// Advance to next token
    fn advance(&mut self) {
        if !self.is_at_end() {
            self.position += 1;
        }
    }

    /// Check if at end
    fn is_at_end(&self) -> bool {
        matches!(self.current(), Token::Eof)
    }
}

/// Parse SQL text into a Statement
pub fn parse_sql(sql: &str) -> Result<Statement> {
    let mut parser = Parser::new(sql)?;
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select_star() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.is_select_star());
                assert_eq!(s.from, "users");
                assert!(s.where_clause.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_columns() {
        let stmt = parse_sql("SELECT id, name FROM users").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(!s.is_select_star());
                assert_eq!(s.get_column_names(), vec!["id", "name"]);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE age > 18").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_order_by() {
        let stmt = parse_sql("SELECT * FROM users ORDER BY name ASC").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.order_by.is_some());
                let order = s.order_by.unwrap();
                assert_eq!(order.column, "name");
                assert_eq!(order.direction, OrderDirection::Asc);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_limit_offset() {
        let stmt = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 5").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.limit, Some(10));
                assert_eq!(s.offset, Some(5));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO users VALUES (1, 'Alice', 25)").unwrap();

        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.table, "users");
                assert_eq!(i.values.len(), 3);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET age = 26 WHERE id = 1").unwrap();

        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.table, "users");
                assert_eq!(u.assignments.len(), 1);
                assert!(u.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE age < 18").unwrap();

        match stmt {
            Statement::Delete(d) => {
                assert_eq!(d.table, "users");
                assert!(d.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_complex_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE age > 18 AND name LIKE 'John%'").unwrap();

        match stmt {
            Statement::Select(s) => {
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_error() {
        let result = parse_sql("INVALID SQL");
        assert!(result.is_err());
    }
}
