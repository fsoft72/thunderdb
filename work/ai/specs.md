# 📄 Specifiche Complete: Custom Database Engine (Rust) - AGGIORNATO

## 1. File di Configurazione (JSON)

### 1.1 `config.json` (Global)

```json
{
  "database": {
    "root_path": "/var/lib/customdb",
    "max_connections": 100,
    "auto_create": true
  },
  "indexing": {
    "update_delay_ms": 50,
    "max_pending_buffer": 1000,
    "thread_count": 2,
    "batch_size": 500
  },
  "storage": {
    "page_size": 4096,
    "cache_size_mb": 512,
    "fsync_mode": "periodic",
    "fsync_interval_ms": 1000
  },
  "btree": {
    "order": 128,
    "node_cache_size": 1000,
    "preload_on_startup": false
  },
  "api": {
    "rest": {
      "enabled": true,
      "port": 8080,
      "host": "0.0.0.0",
      "max_body_size_mb": 10
    },
    "repl": {
      "enabled": true,
      "history_file": ".customdb_history",
      "prompt": "customdb> "
    }
  },
  "logging": {
    "level": "INFO",
    "output": "stdout",
    "file_path": "/var/log/customdb.log",
    "rotation": {
      "enabled": true,
      "max_size_mb": 100,
      "max_files": 10
    }
  },
  "metrics": {
    "enabled": true,
    "retention_hours": 24,
    "export_prometheus": false,
    "prometheus_port": 9090
  },
  "recovery": {
    "auto_recover_on_startup": true,
    "backup_corrupted_files": true,
    "max_recovery_attempts": 3
  }
}
```

### 1.2 Loading Configuration

```rust
// src/config.rs
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub database: DatabaseConfig,
    pub indexing: IndexingConfig,
    pub storage: StorageConfig,
    pub btree: BTreeConfig,
    pub api: ApiConfig,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub recovery: RecoveryConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub root_path: String,
    pub max_connections: usize,
    pub auto_create: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexingConfig {
    pub update_delay_ms: u64,
    pub max_pending_buffer: usize,
    pub thread_count: usize,
    pub batch_size: usize,
}

// ... altri struct per le sezioni

impl Config {
    /// Carica configurazione da file JSON
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let content = fs::read_to_string(path)
            .map_err(|e| ConfigError::FileRead(e.to_string()))?;
        
        let config: Config = serde_json::from_str(&content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;
        
        config.validate()?;
        Ok(config)
    }
    
    /// Genera configurazione di default
    pub fn default() -> Self {
        Self {
            database: DatabaseConfig {
                root_path: "./customdb_data".to_string(),
                max_connections: 100,
                auto_create: true,
            },
            indexing: IndexingConfig {
                update_delay_ms: 50,
                max_pending_buffer: 1000,
                thread_count: 2,
                batch_size: 500,
            },
            // ... altri default
        }
    }
    
    /// Salva configurazione su file
    pub fn save(&self, path: &str) -> Result<(), ConfigError> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ConfigError::SerializeError(e.to_string()))?;
        
        fs::write(path, json)
            .map_err(|e| ConfigError::FileWrite(e.to_string()))?;
        
        Ok(())
    }
    
    /// Valida configurazione
    fn validate(&self) -> Result<(), ConfigError> {
        if self.indexing.update_delay_ms == 0 {
            return Err(ConfigError::InvalidValue(
                "indexing.update_delay_ms must be > 0".into()
            ));
        }
        
        if self.btree.order < 3 {
            return Err(ConfigError::InvalidValue(
                "btree.order must be >= 3".into()
            ));
        }
        
        // ... altre validazioni
        
        Ok(())
    }
}

#[derive(Debug)]
pub enum ConfigError {
    FileRead(String),
    FileWrite(String),
    ParseError(String),
    SerializeError(String),
    InvalidValue(String),
}
```

---

## 2. API di Accesso Diretto ai Dati (CRUD)

### 2.1 Direct Data Access API (Rust)

Oltre al parser SQL, il database espone un'API **type-safe** per operazioni CRUD dirette, utile per:
- Performance critiche (zero overhead di parsing)
- Embedded use cases
- Batch operations
- Testing e debugging

```rust
// src/api/direct.rs

use crate::{Row, Value, Filter, Operator};

/// API diretta per accesso ai dati (bypassa SQL parser)
pub trait DirectDataAccess {
    /// Inserisce una singola riga
    fn insert_row(&mut self, table: &str, values: Vec<Value>) -> Result<u64>;
    
    /// Inserisce più righe in batch (più veloce)
    fn insert_batch(&mut self, table: &str, rows: Vec<Vec<Value>>) -> Result<Vec<u64>>;
    
    /// Legge una riga per PRIMARY KEY
    fn get_by_id(&self, table: &str, row_id: u64) -> Result<Option<Row>>;
    
    /// Legge più righe per PRIMARY KEY
    fn get_by_ids(&self, table: &str, row_ids: &[u64]) -> Result<Vec<Row>>;
    
    /// Scan con filtri personalizzati
    fn scan(&self, table: &str, filters: Vec<Filter>) -> Result<Vec<Row>>;
    
    /// Scan con limite e offset (paginazione)
    fn scan_paginated(
        &self, 
        table: &str, 
        filters: Vec<Filter>,
        limit: usize,
        offset: usize
    ) -> Result<Vec<Row>>;
    
    /// Aggiorna righe che matchano i filtri
    fn update(
        &mut self, 
        table: &str, 
        filters: Vec<Filter>,
        updates: Vec<(String, Value)>  // (column_name, new_value)
    ) -> Result<usize>;  // Ritorna numero righe modificate
    
    /// Aggiorna una singola riga per ID
    fn update_by_id(
        &mut self,
        table: &str,
        row_id: u64,
        updates: Vec<(String, Value)>
    ) -> Result<bool>;  // true se trovata e aggiornata
    
    /// Elimina righe che matchano i filtri
    fn delete(
        &mut self,
        table: &str,
        filters: Vec<Filter>
    ) -> Result<usize>;  // Ritorna numero righe eliminate
    
    /// Elimina una singola riga per ID
    fn delete_by_id(&mut self, table: &str, row_id: u64) -> Result<bool>;
    
    /// Conta righe che matchano i filtri
    fn count(&self, table: &str, filters: Vec<Filter>) -> Result<usize>;
    
    /// Verifica esistenza di una riga per ID
    fn exists(&self, table: &str, row_id: u64) -> Result<bool>;
}

// Struct per costruire filtri in modo type-safe
#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub operator: Operator,
}

#[derive(Debug, Clone)]
pub enum Operator {
    Equals(Value),
    NotEquals(Value),
    GreaterThan(Value),
    GreaterThanOrEqual(Value),
    LessThan(Value),
    LessThanOrEqual(Value),
    Between(Value, Value),
    In(Vec<Value>),
    Like(String),  // Pattern con % e _
    IsNull,
    IsNotNull,
}

// Builder pattern per costruire query complesse
pub struct QueryBuilder {
    table: String,
    filters: Vec<Filter>,
    limit: Option<usize>,
    offset: Option<usize>,
    order_by: Option<(String, SortOrder)>,
}

#[derive(Debug, Clone)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl QueryBuilder {
    pub fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            filters: Vec::new(),
            limit: None,
            offset: None,
            order_by: None,
        }
    }
    
    pub fn filter(mut self, column: &str, operator: Operator) -> Self {
        self.filters.push(Filter {
            column: column.to_string(),
            operator,
        });
        self
    }
    
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
    
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
    
    pub fn order_by(mut self, column: &str, order: SortOrder) -> Self {
        self.order_by = Some((column.to_string(), order));
        self
    }
    
    pub fn execute(&self, db: &Database) -> Result<Vec<Row>> {
        db.scan_paginated(
            &self.table,
            self.filters.clone(),
            self.limit.unwrap_or(usize::MAX),
            self.offset.unwrap_or(0),
        )
    }
}
```

### 2.2 Esempi di Utilizzo Direct API

```rust
use custom_db::{Database, Value, Operator, QueryBuilder, SortOrder};

fn main() -> Result<()> {
    let mut db = Database::open("./mydb")?;
    
    // === INSERT ===
    
    // Insert singolo
    let row_id = db.insert_row(
        "users",
        vec![
            Value::Int64(1),
            Value::Varchar("alice@example.com".into()),
            Value::Int32(25),
            Value::Timestamp(current_timestamp()),
        ]
    )?;
    println!("Inserted row ID: {}", row_id);
    
    // Insert batch (più veloce per molte righe)
    let rows = vec![
        vec![Value::Int64(2), Value::Varchar("bob@example.com".into()), Value::Int32(30)],
        vec![Value::Int64(3), Value::Varchar("charlie@example.com".into()), Value::Int32(35)],
    ];
    let ids = db.insert_batch("users", rows)?;
    println!("Inserted {} rows", ids.len());
    
    // === READ ===
    
    // Get by ID (velocissimo - usa RAT direttamente)
    if let Some(row) = db.get_by_id("users", 1)? {
        println!("User: {:?}", row);
    }
    
    // Get multiple IDs
    let rows = db.get_by_ids("users", &[1, 2, 3])?;
    println!("Found {} users", rows.len());
    
    // Scan con filtri
    let rows = db.scan(
        "users",
        vec![
            Filter {
                column: "age".into(),
                operator: Operator::GreaterThan(Value::Int32(25)),
            },
        ]
    )?;
    
    // Query complessa con builder
    let results = QueryBuilder::new("users")
        .filter("age", Operator::Between(Value::Int32(20), Value::Int32(40)))
        .filter("email", Operator::Like("%.com".into()))
        .order_by("age", SortOrder::Descending)
        .limit(10)
        .offset(0)
        .execute(&db)?;
    
    // === UPDATE ===
    
    // Update by ID
    db.update_by_id(
        "users",
        1,
        vec![
            ("age".into(), Value::Int32(26)),
        ]
    )?;
    
    // Update con filtri (più righe)
    let updated = db.update(
        "users",
        vec![
            Filter {
                column: "age".into(),
                operator: Operator::LessThan(Value::Int32(20)),
            },
        ],
        vec![
            ("age".into(), Value::Int32(20)),  // Set minimo a 20
        ]
    )?;
    println!("Updated {} rows", updated);
    
    // === DELETE ===
    
    // Delete by ID
    db.delete_by_id("users", 999)?;
    
    // Delete con filtri
    let deleted = db.delete(
        "users",
        vec![
            Filter {
                column: "age".into(),
                operator: Operator::GreaterThan(Value::Int32(100)),
            },
        ]
    )?;
    println!("Deleted {} rows", deleted);
    
    // === COUNT & EXISTS ===
    
    let count = db.count("users", vec![])?;  // Conta tutte le righe
    println!("Total users: {}", count);
    
    if db.exists("users", 1)? {
        println!("User 1 exists");
    }
    
    Ok(())
}
```

### 2.3 REST API per Direct Access

Oltre all'endpoint SQL, esponi endpoint REST per le operazioni dirette:

```http
# INSERT
POST /api/tables/{table}/rows
Content-Type: application/json
{
  "values": [1, "alice@example.com", 25, 1738617600000]
}
Response: {"row_id": 1, "success": true}

# INSERT BATCH
POST /api/tables/{table}/rows/batch
{
  "rows": [
    [1, "alice@example.com", 25],
    [2, "bob@example.com", 30]
  ]
}
Response: {"row_ids": [1, 2], "count": 2}

# GET BY ID
GET /api/tables/{table}/rows/{row_id}
Response: {
  "row_id": 1,
  "values": [1, "alice@example.com", 25, 1738617600000]
}

# GET BY IDs (batch)
GET /api/tables/{table}/rows?ids=1,2,3
Response: {
  "rows": [...]
}

# SCAN (con filtri)
POST /api/tables/{table}/scan
{
  "filters": [
    {
      "column": "age",
      "operator": "GreaterThan",
      "value": 25
    }
  ],
  "limit": 10,
  "offset": 0
}
Response: {
  "rows": [...],
  "count": 10,
  "has_more": true
}

# UPDATE BY ID
PUT /api/tables/{table}/rows/{row_id}
{
  "updates": {
    "age": 26,
    "email": "newemail@example.com"
  }
}
Response: {"success": true, "updated": true}

# UPDATE (bulk con filtri)
PUT /api/tables/{table}/rows
{
  "filters": [...],
  "updates": {"age": 30}
}
Response: {"success": true, "updated_count": 15}

# DELETE BY ID
DELETE /api/tables/{table}/rows/{row_id}
Response: {"success": true, "deleted": true}

# DELETE (bulk con filtri)
DELETE /api/tables/{table}/rows
{
  "filters": [...]
}
Response: {"success": true, "deleted_count": 5}

# COUNT
GET /api/tables/{table}/count?filters=[...]
Response: {"count": 150000}

# EXISTS
HEAD /api/tables/{table}/rows/{row_id}
Response: 200 OK (exists) / 404 Not Found
```

---

## 3. Parser SQL - Architettura e Implementazione

### 3.1 Scelta dell'Approccio

**Strategia**: Parser custom **hand-written** con tokenizer + recursive descent parser.

**Motivazioni**:
- ✅ Zero dependencies (requisito del progetto)
- ✅ Controllo completo su errori e diagnostica
- ✅ Performance ottimali per subset SQL limitato
- ✅ Facilità di estensione
- ❌ Parser generator come `pest` o `nom` aggiungerebbero dipendenze

### 3.2 Architettura del Parser

```text
SQL String
    ↓
┌─────────────────┐
│   TOKENIZER     │  Converte stringa in stream di token
│   (Lexer)       │  "SELECT * FROM users WHERE id = 1"
└────────┬────────┘    ↓
         │         [SELECT, *, FROM, IDENT(users), WHERE, IDENT(id), =, NUMBER(1)]
         ↓
┌─────────────────┐
│     PARSER      │  Costruisce AST (Abstract Syntax Tree)
│ (Recursive      │  
│  Descent)       │
└────────┬────────┘
         │
         ↓
    AST (Statement)
         │
         ↓
┌─────────────────┐
│   VALIDATOR     │  Valida semantica (tabelle esistono, tipi compatibili)
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│   EXECUTOR      │  Esegue query o modifica schema
└─────────────────┘
```

### 3.3 Tokenizer (Lexer)

```rust
// src/parser/token.rs

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    From,
    Where,
    And,
    Or,
    Not,
    Like,
    Between,
    In,
    Is,
    Null,
    OrderBy,
    GroupBy,
    Limit,
    Offset,
    Join,
    Inner,
    Left,
    Right,
    On,
    Create,
    Drop,
    Table,
    Index,
    Primary,
    Key,
    
    // Types
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    VarcharType,
    TimestampType,
    
    // Operators
    Equals,           // =
    NotEquals,        // != or <>
    LessThan,         // 
    LessThanOrEqual,  // <=
    GreaterThan,      // >
    GreaterThanOrEqual, // >=
    Plus,             // +
    Minus,            // -
    Star,             // *
    Slash,            // /
    Percent,          // %
    
    // Delimiters
    LeftParen,        // (
    RightParen,       // )
    Comma,            // ,
    Semicolon,        // ;
    Dot,              // .
    
    // Literals
    Number(f64),
    String(String),
    Identifier(String),
    
    // Special
    Eof,
}

pub struct Tokenizer {
    input: Vec<char>,
    position: usize,
    current_char: Option<char>,
}

impl Tokenizer {
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let current = chars.get(0).copied();
        
        Self {
            input: chars,
            position: 0,
            current_char: current,
        }
    }
    
    pub fn tokenize(&mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();
        
        loop {
            self.skip_whitespace();
            
            match self.current_char {
                None => {
                    tokens.push(Token::Eof);
                    break;
                }
                Some('(') => {
                    tokens.push(Token::LeftParen);
                    self.advance();
                }
                Some(')') => {
                    tokens.push(Token::RightParen);
                    self.advance();
                }
                Some(',') => {
                    tokens.push(Token::Comma);
                    self.advance();
                }
                Some(';') => {
                    tokens.push(Token::Semicolon);
                    self.advance();
                }
                Some('.') => {
                    tokens.push(Token::Dot);
                    self.advance();
                }
                Some('*') => {
                    tokens.push(Token::Star);
                    self.advance();
                }
                Some('+') => {
                    tokens.push(Token::Plus);
                    self.advance();
                }
                Some('-') => {
                    // Potrebbe essere -- (commento) o - (minus)
                    if self.peek() == Some('-') {
                        self.skip_line_comment();
                    } else {
                        tokens.push(Token::Minus);
                        self.advance();
                    }
                }
                Some('/') => {
                    tokens.push(Token::Slash);
                    self.advance();
                }
                Some('%') => {
                    tokens.push(Token::Percent);
                    self.advance();
                }
                Some('=') => {
                    tokens.push(Token::Equals);
                    self.advance();
                }
                Some('!') => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::NotEquals);
                        self.advance();
                        self.advance();
                    } else {
                        return Err(ParseError::UnexpectedChar('!'));
                    }
                }
                Some('<') => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::LessThanOrEqual);
                        self.advance();
                        self.advance();
                    } else if self.peek() == Some('>') {
                        tokens.push(Token::NotEquals);
                        self.advance();
                        self.advance();
                    } else {
                        tokens.push(Token::LessThan);
                        self.advance();
                    }
                }
                Some('>') => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::GreaterThanOrEqual);
                        self.advance();
                        self.advance();
                    } else {
                        tokens.push(Token::GreaterThan);
                        self.advance();
                    }
                }
                Some('\'') | Some('"') => {
                    tokens.push(self.read_string()?);
                }
                Some(c) if c.is_ascii_digit() => {
                    tokens.push(self.read_number()?);
                }
                Some(c) if c.is_ascii_alphabetic() || c == '_' => {
                    tokens.push(self.read_identifier_or_keyword());
                }
                Some(c) => {
                    return Err(ParseError::UnexpectedChar(c));
                }
            }
        }
        
        Ok(tokens)
    }
    
    fn read_identifier_or_keyword(&mut self) -> Token {
        let mut ident = String::new();
        
        while let Some(c) = self.current_char {
            if c.is_ascii_alphanumeric() || c == '_' {
                ident.push(c);
                self.advance();
            } else {
                break;
            }
        }
        
        // Check se è keyword (case-insensitive)
        match ident.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "INSERT" => Token::Insert,
            "UPDATE" => Token::Update,
            "DELETE" => Token::Delete,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "LIKE" => Token::Like,
            "BETWEEN" => Token::Between,
            "IN" => Token::In,
            "IS" => Token::Is,
            "NULL" => Token::Null,
            "ORDER" => Token::OrderBy,
            "GROUP" => Token::GroupBy,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "ON" => Token::On,
            "CREATE" => Token::Create,
            "DROP" => Token::Drop,
            "TABLE" => Token::Table,
            "INDEX" => Token::Index,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "INT32" => Token::Int32Type,
            "INT64" => Token::Int64Type,
            "FLOAT32" => Token::Float32Type,
            "FLOAT64" => Token::Float64Type,
            "VARCHAR" => Token::VarcharType,
            "TIMESTAMP" => Token::TimestampType,
            _ => Token::Identifier(ident),
        }
    }
    
    fn read_number(&mut self) -> Result<Token, ParseError> {
        let mut num_str = String::new();
        let mut has_dot = false;
        
        while let Some(c) = self.current_char {
            if c.is_ascii_digit() {
                num_str.push(c);
                self.advance();
            } else if c == '.' && !has_dot && self.peek().map_or(false, |p| p.is_ascii_digit()) {
                has_dot = true;
                num_str.push(c);
                self.advance();
            } else {
                break;
            }
        }
        
        let num = num_str.parse::<f64>()
            .map_err(|_| ParseError::InvalidNumber(num_str))?;
        
        Ok(Token::Number(num))
    }
    
    fn read_string(&mut self) -> Result<Token, ParseError> {
        let quote_char = self.current_char.unwrap();
        self.advance();  // Skip opening quote
        
        let mut string = String::new();
        
        while let Some(c) = self.current_char {
            if c == quote_char {
                self.advance();  // Skip closing quote
                return Ok(Token::String(string));
            } else if c == '\\' {
                // Escape sequence
                self.advance();
                match self.current_char {
                    Some('n') => string.push('\n'),
                    Some('t') => string.push('\t'),
                    Some('r') => string.push('\r'),
                    Some('\\') => string.push('\\'),
                    Some(q) if q == quote_char => string.push(quote_char),
                    _ => return Err(ParseError::InvalidEscapeSequence),
                }
                self.advance();
            } else {
                string.push(c);
                self.advance();
            }
        }
        
        Err(ParseError::UnterminatedString)
    }
    
    fn skip_whitespace(&mut self) {
        while let Some(c) = self.current_char {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }
    
    fn skip_line_comment(&mut self) {
        while let Some(c) = self.current_char {
            if c == '\n' {
                self.advance();
                break;
            }
            self.advance();
        }
    }
    
    fn advance(&mut self) {
        self.position += 1;
        self.current_char = self.input.get(self.position).copied();
    }
    
    fn peek(&self) -> Option<char> {
        self.input.get(self.position + 1).copied()
    }
}
```

### 3.4 Parser (AST Builder)

```rust
// src/parser/ast.rs

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    DropTable(DropTableStatement),
    DropIndex(DropIndexStatement),
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>,  // * ou lista di colonne
    pub from: String,                // Nome tabella
    pub where_clause: Option<Expression>,
    pub order_by: Option<(String, SortOrder)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SelectColumn {
    All,                  // *
    Column(String),       // nome_colonna
    Aliased(String, String),  // nome_colonna AS alias
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,  // None = tutte le colonne in ordine
    pub values: Vec<Vec<Expression>>,  // Supporta multi-row insert
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, Expression)>,  // colonna = valore
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Value),
    Column(String),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        operand: Box<Expression>,
    },
    Function {
        name: String,
        args: Vec<Expression>,
    },
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
    },
    In {
        expr: Box<Expression>,
        list: Vec<Expression>,
    },
    Like {
        expr: Box<Expression>,
        pattern: String,
    },
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Not,
    Minus,
}

// src/parser/parser.rs

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, position: 0 }
    }
    
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        match self.current_token() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            _ => Err(ParseError::UnexpectedToken(self.current_token().clone())),
        }
    }
    
    fn parse_select(&mut self) -> Result<Statement, ParseError> {
        self.consume(Token::Select)?;
        
        // Parse columns
        let columns = self.parse_select_columns()?;
        
        // FROM
        self.consume(Token::From)?;
        let table = self.expect_identifier()?;
        
        // WHERE (optional)
        let where_clause = if self.match_token(&Token::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };
        
        // ORDER BY (optional)
        let order_by = if self.match_keyword("ORDER") {
            self.consume_keyword("BY")?;
            let column = self.expect_identifier()?;
            let order = if self.match_keyword("DESC") {
                SortOrder::Descending
            } else {
                self.match_keyword("ASC"); // Optional
                SortOrder::Ascending
            };
            Some((column, order))
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
            from: table,
            where_clause,
            order_by,
            limit,
            offset,
        }))
    }
    
    fn parse_expression(&mut self) -> Result<Expression, ParseError> {
        self.parse_or_expression()
    }
    
    fn parse_or_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_and_expression()?;
        
        while self.match_token(&Token::Or) {
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        
        Ok(left)
    }
    
    fn parse_and_expression(&mut self) -> Result<Expression, ParseError> {
        let mut left = self.parse_comparison_expression()?;
        
        while self.match_token(&Token::And) {
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        
        Ok(left)
    }
    
    fn parse_comparison_expression(&mut self) -> Result<Expression, ParseError> {
        let left = self.parse_additive_expression()?;
        
        // Check for comparison operators
        if let Some(op) = self.match_comparison_op() {
            let right = self.parse_additive_expression()?;
            return Ok(Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
        
        // Check for BETWEEN
        if self.match_token(&Token::Between) {
            let low = self.parse_additive_expression()?;
            self.consume_keyword("AND")?;
            let high = self.parse_additive_expression()?;
            return Ok(Expression::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
            });
        }
        
        // Check for LIKE
        if self.match_token(&Token::Like) {
            let pattern = self.expect_string()?;
            return Ok(Expression::Like {
                expr: Box::new(left),
                pattern,
            });
        }
        
        // Check for IS NULL / IS NOT NULL
        if self.match_token(&Token::Is) {
            if self.match_token(&Token::Not) {
                self.consume(Token::Null)?;
                return Ok(Expression::IsNotNull(Box::new(left)));
            } else {
                self.consume(Token::Null)?;
                return Ok(Expression::IsNull(Box::new(left)));
            }
        }
        
        Ok(left)
    }
    
    // ... altri metodi di parsing
    
    fn current_token(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }
    
    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }
    
    fn consume(&mut self, expected: Token) -> Result<(), ParseError> {
        if self.current_token() == &expected {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::ExpectedToken {
                expected,
                found: self.current_token().clone(),
            })
        }
    }
    
    fn match_token(&mut self, token: &Token) -> bool {
        if self.current_token() == token {
            self.advance();
            true
        } else {
            false
        }
    }
}
```

### 3.5 Validator & Executor

```rust
// src/parser/validator.rs

pub struct Validator<'a> {
    db: &'a Database,
}

impl<'a> Validator<'a> {
    pub fn validate(&self, stmt: &Statement) -> Result<(), ValidationError> {
        match stmt {
            Statement::Select(s) => self.validate_select(s),
            Statement::Insert(i) => self.validate_insert(i),
            // ... altri statement
        }
    }
    
    fn validate_select(&self, stmt: &SelectStatement) -> Result<(), ValidationError> {
        // 1. Verifica che la tabella esista
        if !self.db.table_exists(&stmt.from) {
            return Err(ValidationError::TableNotFound(stmt.from.clone()));
        }
        
        // 2. Verifica che le colonne esistano
        let schema = self.db.get_table_schema(&stmt.from)?;
        
        for col in &stmt.columns {
            match col {
                SelectColumn::All => {}  // OK
                SelectColumn::Column(name) | SelectColumn::Aliased(name, _) => {
                    if !schema.has_column(name) {
                        return Err(ValidationError::ColumnNotFound {
                            table: stmt.from.clone(),
                            column: name.clone(),
                        });
                    }
                }
            }
        }
        
        // 3. Valida WHERE clause (tipi compatibili)
        if let Some(expr) = &stmt.where_clause {
            self.validate_expression(expr, &schema)?;
        }
        
        Ok(())
    }
    
    fn validate_expression(
        &self, 
        expr: &Expression, 
        schema: &TableSchema
    ) -> Result<(), ValidationError> {
        // Verifica che colonne esistano e tipi siano compatibili
        match expr {
            Expression::Column(name) => {
                if !schema.has_column(name) {
                    return Err(ValidationError::ColumnNotFound {
                        table: schema.name.clone(),
                        column: name.clone(),
                    });
                }
            }
            Expression::BinaryOp { left, op, right } => {
                self.validate_expression(left, schema)?;
                self.validate_expression(right, schema)?;
                
                // Check type compatibility
                // ...
            }
            // ... altri casi
        }
        Ok(())
    }
}

// src/executor.rs

pub struct Executor<'a> {
    db: &'a mut Database,
}

impl<'a> Executor<'a> {
    pub fn execute(&mut self, stmt: Statement) -> Result<ExecutionResult, ExecutionError> {
        match stmt {
            Statement::Select(s) => self.execute_select(s),
            Statement::Insert(i) => self.execute_insert(i),
            Statement::Update(u) => self.execute_update(u),
            Statement::Delete(d) => self.execute_delete(d),
            // ... DDL statements
        }
    }
    
    fn execute_select(&self, stmt: SelectStatement) -> Result<ExecutionResult, ExecutionError> {
        // Converte AST in query plan e esegue
        let filters = if let Some(expr) = stmt.where_clause {
            self.expression_to_filters(expr)?
        } else {
            vec![]
        };
        
        let rows = self.db.scan_paginated(
            &stmt.from,
            filters,
            stmt.limit.unwrap_or(usize::MAX),
            stmt.offset.unwrap_or(0),
        )?;
        
        Ok(ExecutionResult::Select { rows })
    }
    
    fn expression_to_filters(&self, expr: Expression) -> Result<Vec<Filter>, ExecutionError> {
        // Converte Expression AST in Filter struct
        match expr {
            Expression::BinaryOp { left, op, right } => {
                // Gestisce AND/OR combinando filtri
                // ...
            }
            // ... altri casi
        }
    }
}
```

### 3.6 API Completa del Parser

```rust
// src/parser/mod.rs

pub struct SqlParser {
    tokenizer: Tokenizer,
    parser: Parser,
    validator: Validator,
}

impl SqlParser {
    pub fn parse_and_validate(
        sql: &str,
        db: &Database
    ) -> Result<Statement, SqlError> {
        // 1. Tokenize
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize()
            .map_err(SqlError::ParseError)?;
        
        // 2. Parse to AST
        let mut parser = Parser::new(tokens);
        let stmt = parser.parse()
            .map_err(SqlError::ParseError)?;
        
        // 3. Validate
        let validator = Validator::new(db);
        validator.validate(&stmt)
            .map_err(SqlError::ValidationError)?;
        
        Ok(stmt)
    }
}

// Uso:
let stmt = SqlParser::parse_and_validate(
    "SELECT * FROM users WHERE age > 25",
    &db
)?;

let result = executor.execute(stmt)?;
```

---

## 4. Roadmap di Sviluppo AGGIORNATA

### Fase 1: Storage Layer (2 settimane)
- [ ] **Day 1-2**: Setup progetto e struttura base
  - Inizializza progetto Cargo
  - Definisci struttura moduli (`src/storage`, `src/index`, `src/parser`, `src/api`)
  - Crea `Config` struct e loading da JSON
  
- [ ] **Day 3-5**: Value & Row serialization
  - Implementa `Value` enum con tutti i tipi
  - Implementa serializzazione/deserializzazione binaria
  - Test: round-trip serialize/deserialize
  
- [ ] **Day 6-8**: RecordAddressTable (RAT)
  - Implementa struttura RAT in memoria
  - Implementa persistenza su `rat.bin`
  - Binary search per lookup veloce
  - Test: insert 100k entries, benchmark lookup
  
- [ ] **Day 9-12**: File data.bin management
  - Implementa append-only writer
  - Implementa reader con offset
  - Gestione tombstone (DELETE marker)
  - Test: write/read 1M records
  
- [ ] **Day 13-14**: TableEngine base
  - Integra data.bin + RAT
  - Implementa `insert_row`, `get_by_id`
  - Test: insert/read/delete operations

### Fase 2: B-Tree Index (2 settimane)
- [ ] **Day 1-3**: BTreeNode in memoria
  - Implementa struttura nodo (keys + children)
  - Insert algoritmo
  - Lookup e range scan
  - Test: build tree con 10k keys
  
- [ ] **Day 4-6**: Serializzazione B-Tree
  - Formato binario per nodi
  - Scrittura/lettura da `.idx` file
  - Node cache LRU
  - Test: persist/load index, verify integrity
  
- [ ] **Day 7-9**: IndexManager
  - Gestisce multipli indici per tabella
  - Collega B-Tree a RAT (row_id → offset)
  - Update indici su INSERT/UPDATE/DELETE
  - Test: multi-index operations
  
- [ ] **Day 10-12**: Operatori stringa (LIKE)
  - Implementa prefix scan (StartsWith)
  - Implementa full scan per EndsWith/Contains
  - Test: LIKE queries con vari pattern
  
- [ ] **Day 13-14**: Statistics & Optimizer
  - Calcolo cardinalità, min/max
  - Stima selettività
  - Filter reordering
  - Test: compare query plans

### Fase 3: Concurrency & Lazy Indexing (1.5 settimane)
- [ ] **Day 1-3**: Thread communication
  - Setup MPSC channel per index updates
  - Implementa buffer `pending_updates`
  - Test: send/receive messages
  
- [ ] **Day 4-6**: Debouncing logic
  - Timer con reset su nuove scritture
  - Batch flush quando timer scade
  - Flush forzato su buffer pieno
  - Test: verify debouncing behavior
  
- [ ] **Day 7-9**: Table-level locking
  - Implementa RwLock su TableEngine
  - Blocca letture durante flush indici
  - Test: concurrent insert/select, race conditions
  
- [ ] **Day 10-11**: Integration test
  - Test completo: concurrent writes + lazy index update
  - Measure performance impact

### Fase 4: SQL Parser (2 settimane)
- [ ] **Day 1-3**: Tokenizer
  - Implementa lexer completo
  - Gestione keywords, operators, literals
  - Gestione stringhe con escape
  - Test: tokenize varie query SQL
  
- [ ] **Day 4-7**: Parser (AST)
  - Recursive descent parser per SELECT
  - Parser per INSERT, UPDATE, DELETE
  - Parser per CREATE/DROP TABLE/INDEX
  - Test: parse query complesse
  
- [ ] **Day 8-10**: Validator
  - Verifica esistenza tabelle/colonne
  - Verifica compatibilità tipi
  - Test: catch validation errors
  
- [ ] **Day 11-14**: Executor
  - Converte AST → execution plan
  - Integra con TableEngine
  - Test: end-to-end SQL execution

### Fase 5: Direct Data Access API (1 settimana)
- [ ] **Day 1-2**: Filter & Operator structs
  - Implementa `Filter`, `Operator` enums
  - QueryBuilder pattern
  - Test: build complex filters
  
- [ ] **Day 3-5**: CRUD operations
  - Implementa `insert_row`, `insert_batch`
  - Implementa `scan`, `scan_paginated`
  - Implementa `update`, `delete`
  - Test: all CRUD operations
  
- [ ] **Day 6-7**: Count & Exists
  - Implementa `count`, `exists`
  - Ottimizza per performance
  - Test: benchmark vs full scan

### Fase 6: DDL & Management (1 settimana)
- [ ] **Day 1-2**: CREATE TABLE
  - Parse DDL statement
  - Crea directory + file iniziali
  - Salva schema in `config.meta`
  - Test: create tables con vari schemi
  
- [ ] **Day 3-4**: CREATE/DROP INDEX
  - Implementa creazione indice su colonna esistente
  - Implementa DROP INDEX
  - Test: dynamic index management
  
- [ ] **Day 5-6**: Compress (Garbage Collection)
  - Implementa compaction algorithm
  - Ricostruzione RAT e indici
  - Test: compact table con 50% deleted
  
- [ ] **Day 7**: Recovery da crash
  - Implementa `recover_from_crash`
  - Validazione e troncamento `data.bin`
  - Test: simulate crash, verify recovery

### Fase 7: REST API & REPL (1.5 settimane)
- [ ] **Day 1-3**: HTTP Server base
  - Setup server HTTP (tiny_http o custom)
  - Routing per endpoint principali
  - Request/response serialization JSON
  - Test: basic HTTP requests
  
- [ ] **Day 4-6**: REST endpoints
  - `/api/execute` (SQL)
  - `/api/query`
  - `/api/tables/{table}/rows` (CRUD)
  - `/api/tables/{table}/compress`
  - `/api/metrics`
  - Test: all endpoints
  
- [ ] **Day 7-9**: REPL interface
  - Input loop con readline
  - Comando `.help`, `.tables`, `.stats`, etc.
  - Pretty-print risultati tabellari
  - Test: interactive usage
  
- [ ] **Day 10-11**: Authentication & Security (opzionale)
  - API key authentication
  - Rate limiting
  - SQL injection prevention

### Fase 8: WebAssembly Support (1.5 settimane)
- [ ] **Day 1-3**: Storage adapter per IndexedDB
  - Implementa `WasmStorage` trait
  - Mapping file → object stores
  - Test: read/write su IndexedDB
  
- [ ] **Day 4-6**: Threading con Web Workers
  - Setup message passing
  - Index update in background worker
  - Test: concurrent operations in browser
  
- [ ] **Day 7-9**: Wasm bindings
  - Esporta API JavaScript-friendly
  - Gestione Promise/async
  - Test: browser integration
  
- [ ] **Day 10-11**: Example web app
  - Simple TODO app o dashboard
  - Dimostra tutte le features

### Fase 9: Monitoring & Observability (1 settimana)
- [ ] **Day 1-3**: Metrics collection
  - Implementa `TableMetrics` struct
  - Tracking query latency (P50, P95, P99)
  - Tracking index usage
  - Test: verify metrics accuracy
  
- [ ] **Day 4-5**: Metrics API
  - `/api/metrics` endpoint
  - Formato JSON + Prometheus (opzionale)
  - Test: consume metrics
  
- [ ] **Day 6-7**: Logging system
  - Integra `log` facade
  - Configurabile via config.json
  - File rotation
  - Test: various log levels

### Fase 10: Testing & Optimization (2 settimane)
- [ ] **Day 1-4**: Correctness tests
  - Unit test per ogni modulo
  - Integration test end-to-end
  - Concurrency test (race conditions)
  - Recovery test
  - Test coverage > 80%
  
- [ ] **Day 5-8**: Performance benchmarks
  - Benchmark INSERT (single & batch)
  - Benchmark SELECT (index vs scan)
  - Benchmark LIKE queries
  - Benchmark compress
  - Target: >100k insert/s, <1ms SELECT by ID
  
- [ ] **Day 9-12**: Profiling & optimization
  - CPU profiling (flamegraph)
  - Memory profiling
  - Identify bottlenecks
  - Optimize hot paths
  
- [ ] **Day 13-14**: Documentation
  - API reference (rustdoc)
  - Architecture documentation
  - Tutorial & examples
  - Performance tuning guide

### Fase 11: Polish & Release (1 settimana)
- [ ] **Day 1-2**: Error handling review
  - Consistent error types
  - Helpful error messages
  - Error recovery strategies
  
- [ ] **Day 3-4**: CI/CD setup
  - GitHub Actions o equivalente
  - Automated testing
  - Cross-platform builds (Linux, macOS, Wasm)
  
- [ ] **Day 5-7**: Release preparation
  - Version tagging
  - Changelog
  - Release notes
  - Crate publication (crates.io)

---

## Stima Totale Aggiornata

**Totale**: ~14 settimane (3.5 mesi) per un developer full-time

**Breakdown**:
- Core Engine (Storage + Index + Concurrency): 5.5 settimane (39%)
- Query Layer (Parser + Direct API): 3 settimane (21%)
- Management (DDL + Recovery): 1 settimana (7%)
- Interfaces (REST + REPL + Wasm): 3 settimane (21%)
- Quality (Testing + Optimization): 2 settimane (14%)

**Critical Path**: Storage → Index → Parser → Integration

**Opzionale** (non bloccante):
- WebAssembly support
- Authentication/Security
- Prometheus metrics export
- Advanced optimizer (join planning)

---

## Priorità per MVP (Minimum Viable Product)

Se vuoi un **MVP funzionante in 6 settimane**:

### Week 1-2: Storage + RAT
- Value serialization
- data.bin management
- RAT implementation
- Basic TableEngine (insert, get_by_id)

### Week 3-4: B-Tree Index
- In-memory B-Tree
- Persistence
- Single index per table
- Basic range queries

### Week 5: Direct API + Simple Parser
- Direct CRUD API
- Tokenizer + Parser base (SELECT, INSERT only)
- No DDL (schema hardcoded per testing)

### Week 6: Testing + REPL
- Correctness tests
- Basic REPL
- Documentation essenziale

**Risultato MVP**: Database funzionante con API Rust, query semplici, REPL basic.

**Dopo MVP**: Aggiungi fase per fase le features avanzate (DDL, REST API, Wasm, optimizer avanzato).

