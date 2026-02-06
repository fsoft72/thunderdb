use crate::error::{Error, Result};
use std::fmt;

/// Supported data types in ThunderDB
///
/// Each value type has a specific binary representation for efficient storage
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 32-bit floating point
    Float32(f32),
    /// 64-bit floating point
    Float64(f64),
    /// Variable-length string (UTF-8)
    Varchar(String),
    /// Unix timestamp (milliseconds since epoch)
    Timestamp(i64),
    /// Null value
    Null,
}

// Type tags for binary serialization (1 byte)
const TYPE_INT32: u8 = 1;
const TYPE_INT64: u8 = 2;
const TYPE_FLOAT32: u8 = 3;
const TYPE_FLOAT64: u8 = 4;
const TYPE_VARCHAR: u8 = 5;
const TYPE_TIMESTAMP: u8 = 6;
const TYPE_NULL: u8 = 0;

impl Value {
    /// Write value directly to a writer to avoid allocations
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<usize> {
        match self {
            Value::Int32(v) => {
                writer.write_all(&[TYPE_INT32])?;
                writer.write_all(&v.to_le_bytes())?;
                Ok(5)
            }
            Value::Int64(v) => {
                writer.write_all(&[TYPE_INT64])?;
                writer.write_all(&v.to_le_bytes())?;
                Ok(9)
            }
            Value::Float32(v) => {
                writer.write_all(&[TYPE_FLOAT32])?;
                writer.write_all(&v.to_le_bytes())?;
                Ok(5)
            }
            Value::Float64(v) => {
                writer.write_all(&[TYPE_FLOAT64])?;
                writer.write_all(&v.to_le_bytes())?;
                Ok(9)
            }
            Value::Varchar(s) => {
                writer.write_all(&[TYPE_VARCHAR])?;
                let str_bytes = s.as_bytes();
                let len = str_bytes.len() as u32;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(str_bytes)?;
                Ok(1 + 4 + str_bytes.len())
            }
            Value::Timestamp(v) => {
                writer.write_all(&[TYPE_TIMESTAMP])?;
                writer.write_all(&v.to_le_bytes())?;
                Ok(9)
            }
            Value::Null => {
                writer.write_all(&[TYPE_NULL])?;
                Ok(1)
            }
        }
    }

    /// Serialize value to bytes
    ///
    /// Format: [type_tag: u8] [data: variable length]
    /// - Int32: [1] [4 bytes]
    /// - Int64: [2] [8 bytes]
    /// - Float32: [3] [4 bytes]
    /// - Float64: [4] [8 bytes]
    /// - Varchar: [5] [length: u32] [utf8 bytes]
    /// - Timestamp: [6] [8 bytes]
    /// - Null: [0]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9); // Pre-allocate for common types
        match self {
            Value::Int32(v) => {
                bytes.push(TYPE_INT32);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Int64(v) => {
                bytes.push(TYPE_INT64);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float32(v) => {
                bytes.push(TYPE_FLOAT32);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float64(v) => {
                bytes.push(TYPE_FLOAT64);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Varchar(s) => {
                bytes.push(TYPE_VARCHAR);
                let str_bytes = s.as_bytes();
                let len = str_bytes.len() as u32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(str_bytes);
            }
            Value::Timestamp(v) => {
                bytes.push(TYPE_TIMESTAMP);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Null => {
                bytes.push(TYPE_NULL);
            }
        }
        bytes
    }

    /// Deserialize value from bytes
    ///
    /// Returns (Value, bytes_consumed)
    pub fn from_bytes(bytes: &[u8]) -> Result<(Self, usize)> {
        if bytes.is_empty() {
            return Err(Error::Serialization("Empty byte array".to_string()));
        }

        let type_tag = bytes[0];
        let mut consumed = 1;

        let value = match type_tag {
            TYPE_INT32 => {
                if bytes.len() < 5 {
                    return Err(Error::Serialization("Insufficient bytes for Int32".to_string()));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&bytes[1..5]);
                consumed += 4;
                Value::Int32(i32::from_le_bytes(buf))
            }
            TYPE_INT64 => {
                if bytes.len() < 9 {
                    return Err(Error::Serialization("Insufficient bytes for Int64".to_string()));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[1..9]);
                consumed += 8;
                Value::Int64(i64::from_le_bytes(buf))
            }
            TYPE_FLOAT32 => {
                if bytes.len() < 5 {
                    return Err(Error::Serialization("Insufficient bytes for Float32".to_string()));
                }
                let mut buf = [0u8; 4];
                buf.copy_from_slice(&bytes[1..5]);
                consumed += 4;
                Value::Float32(f32::from_le_bytes(buf))
            }
            TYPE_FLOAT64 => {
                if bytes.len() < 9 {
                    return Err(Error::Serialization("Insufficient bytes for Float64".to_string()));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[1..9]);
                consumed += 8;
                Value::Float64(f64::from_le_bytes(buf))
            }
            TYPE_VARCHAR => {
                if bytes.len() < 5 {
                    return Err(Error::Serialization("Insufficient bytes for Varchar length".to_string()));
                }
                let mut len_buf = [0u8; 4];
                len_buf.copy_from_slice(&bytes[1..5]);
                let len = u32::from_le_bytes(len_buf) as usize;
                consumed += 4;

                if bytes.len() < 5 + len {
                    return Err(Error::Serialization(format!(
                        "Insufficient bytes for Varchar data: expected {}, got {}",
                        len,
                        bytes.len() - 5
                    )));
                }

                let str_bytes = &bytes[5..5 + len];
                let s = String::from_utf8(str_bytes.to_vec())
                    .map_err(|e| Error::Serialization(format!("Invalid UTF-8: {}", e)))?;
                consumed += len;
                Value::Varchar(s)
            }
            TYPE_TIMESTAMP => {
                if bytes.len() < 9 {
                    return Err(Error::Serialization("Insufficient bytes for Timestamp".to_string()));
                }
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes[1..9]);
                consumed += 8;
                Value::Timestamp(i64::from_le_bytes(buf))
            }
            TYPE_NULL => Value::Null,
            _ => {
                return Err(Error::Serialization(format!("Unknown type tag: {}", type_tag)));
            }
        };

        Ok((value, consumed))
    }

    /// Get the type name as a string
    pub fn type_name(&self) -> &str {
        match self {
            Value::Int32(_) => "INT32",
            Value::Int64(_) => "INT64",
            Value::Float32(_) => "FLOAT32",
            Value::Float64(_) => "FLOAT64",
            Value::Varchar(_) => "VARCHAR",
            Value::Timestamp(_) => "TIMESTAMP",
            Value::Null => "NULL",
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float32(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
            Value::Timestamp(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int32_serialization() {
        let val = Value::Int32(42);
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_int64_serialization() {
        let val = Value::Int64(9223372036854775807);
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_float32_serialization() {
        let val = Value::Float32(3.14159);
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_float64_serialization() {
        let val = Value::Float64(2.718281828459045);
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_varchar_serialization() {
        let val = Value::Varchar("Hello, ThunderDB!".to_string());
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_varchar_empty() {
        let val = Value::Varchar(String::new());
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_varchar_unicode() {
        let val = Value::Varchar("こんにちは世界 🌍".to_string());
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_timestamp_serialization() {
        let val = Value::Timestamp(1609459200000); // 2021-01-01 00:00:00 UTC
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn test_null_serialization() {
        let val = Value::Null;
        let bytes = val.to_bytes();
        let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
        assert_eq!(val, decoded);
        assert_eq!(consumed, bytes.len());
        assert_eq!(bytes.len(), 1);
    }

    #[test]
    fn test_all_types_round_trip() {
        let values = vec![
            Value::Int32(-42),
            Value::Int64(123456789),
            Value::Float32(1.5),
            Value::Float64(-2.5),
            Value::Varchar("test".to_string()),
            Value::Timestamp(0),
            Value::Null,
        ];

        for val in values {
            let bytes = val.to_bytes();
            let (decoded, consumed) = Value::from_bytes(&bytes).unwrap();
            assert_eq!(val, decoded);
            assert_eq!(consumed, bytes.len());
        }
    }

    #[test]
    fn test_type_name() {
        assert_eq!(Value::Int32(1).type_name(), "INT32");
        assert_eq!(Value::Int64(1).type_name(), "INT64");
        assert_eq!(Value::Float32(1.0).type_name(), "FLOAT32");
        assert_eq!(Value::Float64(1.0).type_name(), "FLOAT64");
        assert_eq!(Value::Varchar("x".to_string()).type_name(), "VARCHAR");
        assert_eq!(Value::Timestamp(0).type_name(), "TIMESTAMP");
        assert_eq!(Value::Null.type_name(), "NULL");
    }

    #[test]
    fn test_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn test_insufficient_bytes() {
        // Int32 with not enough bytes
        let result = Value::from_bytes(&[TYPE_INT32, 0, 0]);
        assert!(result.is_err());

        // Varchar with truncated data
        let result = Value::from_bytes(&[TYPE_VARCHAR, 10, 0, 0, 0, b'x']);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_type_tag() {
        let result = Value::from_bytes(&[99]);
        assert!(result.is_err());
    }
}
