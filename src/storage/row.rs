use crate::error::{Error, Result};
use crate::storage::value::Value;

/// Represents a database row with a unique ID and values
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Unique row identifier (auto-generated, monotonic)
    pub row_id: u64,
    /// Column values in order
    pub values: Vec<Value>,
}

impl Row {
    /// Create a new row with the given ID and values
    pub fn new(row_id: u64, values: Vec<Value>) -> Self {
        Self { row_id, values }
    }

    /// Serialize row to bytes
    ///
    /// Format:
    /// - Row ID: [8 bytes, u64 little-endian]
    /// - Value count: [4 bytes, u32 little-endian]
    /// - Values: [serialized values concatenated]
    ///
    /// Total length prefix is NOT included (managed by DataFile)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Write row ID
        bytes.extend_from_slice(&self.row_id.to_le_bytes());

        // Write value count
        let value_count = self.values.len() as u32;
        bytes.extend_from_slice(&value_count.to_le_bytes());

        // Write each value
        for value in &self.values {
            bytes.extend_from_slice(&value.to_bytes());
        }

        bytes
    }

    /// Deserialize row from bytes
    ///
    /// Returns the deserialized Row
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::Serialization(
                "Insufficient bytes for row header".to_string(),
            ));
        }

        // Read row ID
        let mut row_id_buf = [0u8; 8];
        row_id_buf.copy_from_slice(&bytes[0..8]);
        let row_id = u64::from_le_bytes(row_id_buf);

        // Read value count
        let mut count_buf = [0u8; 4];
        count_buf.copy_from_slice(&bytes[8..12]);
        let value_count = u32::from_le_bytes(count_buf) as usize;

        // Read values
        let mut values = Vec::with_capacity(value_count);
        let mut offset = 12;

        for i in 0..value_count {
            if offset >= bytes.len() {
                return Err(Error::Serialization(format!(
                    "Unexpected end of data while reading value {} of {}",
                    i + 1,
                    value_count
                )));
            }

            let (value, consumed) = Value::from_bytes(&bytes[offset..])?;
            values.push(value);
            offset += consumed;
        }

        Ok(Self { row_id, values })
    }

    /// Get the number of columns in this row
    pub fn column_count(&self) -> usize {
        self.values.len()
    }

    /// Get a value by column index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a mutable reference to a value by column index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_serialization_simple() {
        let row = Row::new(
            42,
            vec![
                Value::Int32(100),
                Value::Varchar("test".to_string()),
                Value::Null,
            ],
        );

        let bytes = row.to_bytes();
        let decoded = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_row_serialization_all_types() {
        let row = Row::new(
            12345,
            vec![
                Value::Int32(-42),
                Value::Int64(9223372036854775807),
                Value::Float32(3.14),
                Value::Float64(2.718281828),
                Value::Varchar("Hello, World!".to_string()),
                Value::Timestamp(1609459200000),
                Value::Null,
            ],
        );

        let bytes = row.to_bytes();
        let decoded = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row, decoded);
    }

    #[test]
    fn test_row_empty_values() {
        let row = Row::new(1, vec![]);

        let bytes = row.to_bytes();
        let decoded = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row, decoded);
        assert_eq!(decoded.column_count(), 0);
    }

    #[test]
    fn test_row_large_varchar() {
        let large_string = "x".repeat(10000);
        let row = Row::new(99, vec![Value::Varchar(large_string.clone())]);

        let bytes = row.to_bytes();
        let decoded = Row::from_bytes(&bytes).unwrap();

        assert_eq!(row, decoded);
        if let Value::Varchar(s) = &decoded.values[0] {
            assert_eq!(s.len(), 10000);
        } else {
            panic!("Expected Varchar");
        }
    }

    #[test]
    fn test_row_get() {
        let row = Row::new(
            1,
            vec![Value::Int32(10), Value::Varchar("test".to_string())],
        );

        assert_eq!(row.get(0), Some(&Value::Int32(10)));
        assert_eq!(row.get(1), Some(&Value::Varchar("test".to_string())));
        assert_eq!(row.get(2), None);
    }

    #[test]
    fn test_row_get_mut() {
        let mut row = Row::new(1, vec![Value::Int32(10)]);

        if let Some(val) = row.get_mut(0) {
            *val = Value::Int32(20);
        }

        assert_eq!(row.get(0), Some(&Value::Int32(20)));
    }

    #[test]
    fn test_row_column_count() {
        let row = Row::new(1, vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);
        assert_eq!(row.column_count(), 3);
    }

    #[test]
    fn test_row_insufficient_bytes() {
        // Not enough bytes for header
        let result = Row::from_bytes(&[0, 0, 0, 0]);
        assert!(result.is_err());

        // Valid header but missing values
        let mut bytes = vec![0u8; 8]; // row_id
        bytes.extend_from_slice(&2u32.to_le_bytes()); // claim 2 values
        let result = Row::from_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_row_round_trip_many_rows() {
        for i in 0..100 {
            let row = Row::new(
                i,
                vec![
                    Value::Int64(i as i64),
                    Value::Varchar(format!("row_{}", i)),
                    Value::Float64(i as f64 * 1.5),
                ],
            );

            let bytes = row.to_bytes();
            let decoded = Row::from_bytes(&bytes).unwrap();
            assert_eq!(row, decoded);
        }
    }
}
