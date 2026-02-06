// SQL Tokenizer/Lexer - Step 4.1
//
// Converts SQL text into tokens for parsing

use crate::error::{Error, Result};
use std::fmt;

/// SQL Token types
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Where,
    Into,
    Values,
    Set,
    And,
    Or,
    Not,
    Like,
    In,
    Between,
    Is,
    Null,
    As,
    OrderBy,
    Limit,
    Offset,
    Asc,
    Desc,

    // Operators
    Equals,           // =
    NotEquals,        // !=, <>
    LessThan,         // <
    LessThanOrEqual,  // <=
    GreaterThan,      // >
    GreaterThanOrEqual, // >=
    Plus,             // +
    Minus,            // -
    Star,             // *
    Slash,            // /

    // Literals
    Number(f64),
    String(String),
    Identifier(String),

    // Delimiters
    LeftParen,    // (
    RightParen,   // )
    Comma,        // ,
    Semicolon,    // ;

    // Special
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Select => write!(f, "SELECT"),
            Token::Insert => write!(f, "INSERT"),
            Token::Update => write!(f, "UPDATE"),
            Token::Delete => write!(f, "DELETE"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Number(n) => write!(f, "{}", n),
            Token::String(s) => write!(f, "'{}'", s),
            Token::Identifier(s) => write!(f, "{}", s),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// SQL Tokenizer
pub struct Tokenizer {
    input: Vec<char>,
    position: usize,
}

impl Tokenizer {
    /// Create a new tokenizer from SQL text
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }

        Ok(tokens)
    }

    /// Get the next token
    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();
        self.skip_comments()?;
        self.skip_whitespace();

        if self.is_at_end() {
            return Ok(Token::Eof);
        }

        let ch = self.current_char();

        // Single character tokens
        let token = match ch {
            '(' => {
                self.advance();
                Token::LeftParen
            }
            ')' => {
                self.advance();
                Token::RightParen
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            ';' => {
                self.advance();
                Token::Semicolon
            }
            '+' => {
                self.advance();
                Token::Plus
            }
            '-' => {
                self.advance();
                // Check for -- comment
                if self.peek() == Some('-') {
                    self.skip_line_comment();
                    return self.next_token();
                }
                Token::Minus
            }
            '*' => {
                self.advance();
                Token::Star
            }
            '/' => {
                self.advance();
                // Check for /* */ comment
                if self.peek() == Some('*') {
                    self.skip_block_comment()?;
                    return self.next_token();
                }
                Token::Slash
            }
            '=' => {
                self.advance();
                Token::Equals
            }
            '!' => {
                if self.peek() == Some('=') {
                    self.advance(); // Skip !
                    self.advance(); // Skip =
                    Token::NotEquals
                } else {
                    return Err(Error::Parser("Unexpected character: !".to_string()));
                }
            }
            '<' => {
                let next = self.peek();
                self.advance();
                if next == Some('=') {
                    self.advance();
                    Token::LessThanOrEqual
                } else if next == Some('>') {
                    self.advance();
                    Token::NotEquals
                } else {
                    Token::LessThan
                }
            }
            '>' => {
                let next = self.peek();
                self.advance();
                if next == Some('=') {
                    self.advance();
                    Token::GreaterThanOrEqual
                } else {
                    Token::GreaterThan
                }
            }
            '\'' | '"' => self.read_string()?,
            _ if ch.is_ascii_digit() => self.read_number()?,
            _ if ch.is_alphabetic() || ch == '_' => self.read_identifier_or_keyword(),
            _ => {
                return Err(Error::Parser(format!("Unexpected character: {}", ch)));
            }
        };

        Ok(token)
    }

    /// Read a string literal
    fn read_string(&mut self) -> Result<Token> {
        let quote = self.current_char();
        self.advance(); // Skip opening quote

        let mut value = String::new();

        while !self.is_at_end() && self.current_char() != quote {
            let ch = self.current_char();

            if ch == '\\' {
                self.advance();
                if !self.is_at_end() {
                    let escaped = self.current_char();
                    value.push(match escaped {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '\\' => '\\',
                        '\'' => '\'',
                        '"' => '"',
                        _ => escaped,
                    });
                    self.advance();
                }
            } else {
                value.push(ch);
                self.advance();
            }
        }

        if self.is_at_end() {
            return Err(Error::Parser("Unterminated string".to_string()));
        }

        self.advance(); // Skip closing quote

        Ok(Token::String(value))
    }

    /// Read a number literal
    fn read_number(&mut self) -> Result<Token> {
        let mut value = String::new();

        while !self.is_at_end() && (self.current_char().is_ascii_digit() || self.current_char() == '.') {
            value.push(self.current_char());
            self.advance();
        }

        let num = value.parse::<f64>()
            .map_err(|_| Error::Parser(format!("Invalid number: {}", value)))?;

        Ok(Token::Number(num))
    }

    /// Read an identifier or keyword
    fn read_identifier_or_keyword(&mut self) -> Token {
        let mut value = String::new();

        while !self.is_at_end() {
            let ch = self.current_char();
            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Check if it's a keyword (case-insensitive)
        let upper = value.to_uppercase();
        match upper.as_str() {
            "SELECT" => Token::Select,
            "INSERT" => Token::Insert,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SET" => Token::Set,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "LIKE" => Token::Like,
            "IN" => Token::In,
            "BETWEEN" => Token::Between,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "AS" => Token::As,
            "ORDER" => {
                // Check for "ORDER BY"
                let saved_pos = self.position;
                self.skip_whitespace();

                // Try to read next word
                let mut next_word = String::new();
                while !self.is_at_end() && (self.current_char().is_alphabetic() || self.current_char() == '_') {
                    next_word.push(self.current_char());
                    self.advance();
                }

                if next_word.to_uppercase() == "BY" {
                    Token::OrderBy
                } else {
                    // Not "ORDER BY", restore position and return identifier
                    self.position = saved_pos;
                    Token::Identifier(value)
                }
            }
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            _ => Token::Identifier(value),
        }
    }

    /// Skip whitespace
    fn skip_whitespace(&mut self) {
        while !self.is_at_end() && self.current_char().is_whitespace() {
            self.advance();
        }
    }

    /// Skip line comment (--)
    fn skip_line_comment(&mut self) {
        while !self.is_at_end() && self.current_char() != '\n' {
            self.advance();
        }
    }

    /// Skip block comment (/* */)
    fn skip_block_comment(&mut self) -> Result<()> {
        self.advance(); // Skip *

        while !self.is_at_end() {
            if self.current_char() == '*' && self.peek() == Some('/') {
                self.advance(); // Skip *
                self.advance(); // Skip /
                return Ok(());
            }
            self.advance();
        }

        Err(Error::Parser("Unterminated block comment".to_string()))
    }

    /// Skip comments
    fn skip_comments(&mut self) -> Result<()> {
        loop {
            if self.is_at_end() {
                break;
            }

            if self.current_char() == '-' && self.peek() == Some('-') {
                self.skip_line_comment();
                self.skip_whitespace();
            } else if self.current_char() == '/' && self.peek() == Some('*') {
                self.advance(); // Skip /
                self.skip_block_comment()?;
                self.skip_whitespace();
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Get current character
    fn current_char(&self) -> char {
        self.input[self.position]
    }

    /// Peek at next character
    fn peek(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            Some(self.input[self.position + 1])
        } else {
            None
        }
    }

    /// Advance position
    fn advance(&mut self) {
        if !self.is_at_end() {
            self.position += 1;
        }
    }

    /// Check if at end of input
    fn is_at_end(&self) -> bool {
        self.position >= self.input.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let mut tokenizer = Tokenizer::new("SELECT * FROM users");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
        assert_eq!(tokens[4], Token::Eof);
    }

    #[test]
    fn test_tokenize_where() {
        let mut tokenizer = Tokenizer::new("WHERE age > 18 AND name = 'John'");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Where);
        assert_eq!(tokens[1], Token::Identifier("age".to_string()));
        assert_eq!(tokens[2], Token::GreaterThan);
        assert_eq!(tokens[3], Token::Number(18.0));
        assert_eq!(tokens[4], Token::And);
        assert_eq!(tokens[5], Token::Identifier("name".to_string()));
        assert_eq!(tokens[6], Token::Equals);
        assert_eq!(tokens[7], Token::String("John".to_string()));
    }

    #[test]
    fn test_tokenize_operators() {
        let mut tokenizer = Tokenizer::new("= != < <= > >= <>");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Equals);
        assert_eq!(tokens[1], Token::NotEquals);
        assert_eq!(tokens[2], Token::LessThan);
        assert_eq!(tokens[3], Token::LessThanOrEqual);
        assert_eq!(tokens[4], Token::GreaterThan);
        assert_eq!(tokens[5], Token::GreaterThanOrEqual);
        assert_eq!(tokens[6], Token::NotEquals);
    }

    #[test]
    fn test_tokenize_string_escapes() {
        let mut tokenizer = Tokenizer::new(r#"'Hello\nWorld\t!'"#);
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::String("Hello\nWorld\t!".to_string()));
    }

    #[test]
    fn test_tokenize_numbers() {
        let mut tokenizer = Tokenizer::new("42 3.14 0.5");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Number(42.0));
        assert_eq!(tokens[1], Token::Number(3.14));
        assert_eq!(tokens[2], Token::Number(0.5));
    }

    #[test]
    fn test_tokenize_insert() {
        let mut tokenizer = Tokenizer::new("INSERT INTO users VALUES (1, 'Alice', 25)");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Insert);
        assert_eq!(tokens[1], Token::Into);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
        assert_eq!(tokens[3], Token::Values);
        assert_eq!(tokens[4], Token::LeftParen);
        assert_eq!(tokens[5], Token::Number(1.0));
    }

    #[test]
    fn test_tokenize_comments() {
        let mut tokenizer = Tokenizer::new("SELECT * -- this is a comment\nFROM users");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".to_string()));
    }

    #[test]
    fn test_tokenize_block_comment() {
        let mut tokenizer = Tokenizer::new("SELECT /* comment */ * FROM users");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
    }

    #[test]
    fn test_tokenize_case_insensitive() {
        let mut tokenizer = Tokenizer::new("select FROM where");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::From);
        assert_eq!(tokens[2], Token::Where);
    }

    #[test]
    fn test_tokenize_order_by() {
        let mut tokenizer = Tokenizer::new("ORDER BY name ASC");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::OrderBy);
        assert_eq!(tokens[1], Token::Identifier("name".to_string()));
        assert_eq!(tokens[2], Token::Asc);
    }

    #[test]
    fn test_tokenize_limit_offset() {
        let mut tokenizer = Tokenizer::new("LIMIT 10 OFFSET 5");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Limit);
        assert_eq!(tokens[1], Token::Number(10.0));
        assert_eq!(tokens[2], Token::Offset);
        assert_eq!(tokens[3], Token::Number(5.0));
    }

    #[test]
    fn test_unterminated_string() {
        let mut tokenizer = Tokenizer::new("'unterminated");
        let result = tokenizer.tokenize();
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_block_comment() {
        let mut tokenizer = Tokenizer::new("/* unterminated");
        let result = tokenizer.tokenize();
        assert!(result.is_err());
    }
}
