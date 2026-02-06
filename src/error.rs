use std::fmt;
use std::io;

/// Main error type for ThunderDB operations
#[derive(Debug)]
pub enum Error {
    /// I/O errors (file operations, etc.)
    Io(io::Error),

    /// Configuration errors
    Config(String),

    /// Storage layer errors
    Storage(String),

    /// Index errors
    Index(String),

    /// Query errors
    Query(String),

    /// Parser errors
    Parser(String),

    /// Serialization/deserialization errors
    Serialization(String),

    /// Table not found
    TableNotFound(String),

    /// Column not found
    ColumnNotFound(String),

    /// Type mismatch
    TypeMismatch { expected: String, found: String },

    /// Invalid operation
    InvalidOperation(String),

    /// Row not found
    RowNotFound(u64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::Config(msg) => write!(f, "Configuration error: {}", msg),
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Index(msg) => write!(f, "Index error: {}", msg),
            Error::Query(msg) => write!(f, "Query error: {}", msg),
            Error::Parser(msg) => write!(f, "Parser error: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::TableNotFound(name) => write!(f, "Table not found: {}", name),
            Error::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
            Error::TypeMismatch { expected, found } => {
                write!(f, "Type mismatch: expected {}, found {}", expected, found)
            }
            Error::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            Error::RowNotFound(id) => write!(f, "Row not found: {}", id),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

/// Result type for ThunderDB operations
pub type Result<T> = std::result::Result<T, Error>;
