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

    /// Write row to a writer using the column-offset format.
    ///
    /// Format: [row_id:8][col_count:4][off0:2]...[offN-1:2][val0]...[valN-1]
    ///
    /// Each offset is a u16 LE relative to the start of the values area.
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> Result<usize> {
        let col_count = self.values.len();

        // Serialize all values into a temporary buffer to compute offsets
        let mut values_buf = Vec::with_capacity(col_count * 8);
        let mut offsets: Vec<u16> = Vec::with_capacity(col_count);

        for value in &self.values {
            offsets.push(values_buf.len() as u16);
            value.write_to(&mut values_buf)?;
        }

        if values_buf.len() > u16::MAX as usize {
            return Err(Error::Serialization(format!(
                "Row values area ({} bytes) exceeds u16 offset limit (65535)",
                values_buf.len()
            )));
        }

        // Write header
        writer.write_all(&self.row_id.to_le_bytes())?;          // 8 bytes
        writer.write_all(&(col_count as u32).to_le_bytes())?;   // 4 bytes

        // Write offset array
        for off in &offsets {
            writer.write_all(&off.to_le_bytes())?;               // 2 bytes each
        }

        // Write values
        writer.write_all(&values_buf)?;

        Ok(8 + 4 + col_count * 2 + values_buf.len())
    }

    /// Serialize row to bytes
    ///
    /// Format:
    /// - Row ID: [8 bytes, u64 little-endian]
    /// - Value count: [4 bytes, u32 little-endian]
    /// - Offset array: [col_count x 2 bytes, u16 little-endian each]
    /// - Values: [serialized values concatenated]
    ///
    /// Total length prefix is NOT included (managed by DataFile)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.values.len() * 10);
        self.write_to(&mut bytes).unwrap();
        bytes
    }

    /// Deserialize row from bytes (column-offset format)
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::Serialization(
                "Insufficient bytes for row header".to_string(),
            ));
        }

        let row_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let value_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;

        let offsets_end = 12 + value_count * 2;
        if bytes.len() < offsets_end {
            return Err(Error::Serialization(
                "Insufficient bytes for offset array".to_string(),
            ));
        }

        // Skip past the offset array to the values area
        let values_start = offsets_end;

        let mut values = Vec::with_capacity(value_count);
        let mut cursor = values_start;

        for i in 0..value_count {
            if cursor >= bytes.len() {
                return Err(Error::Serialization(format!(
                    "Unexpected end of data while reading value {} of {}",
                    i + 1,
                    value_count
                )));
            }
            let (value, consumed) = Value::from_bytes(&bytes[cursor..])?;
            values.push(value);
            cursor += consumed;
        }

        Ok(Self { row_id, values })
    }

    /// Extract a single column value from raw row bytes without full deserialization.
    ///
    /// Uses the column-offset array to jump directly to the target column.
    /// Does not allocate a Row or deserialize other columns.
    pub fn value_at(bytes: &[u8], col_idx: usize) -> Result<Value> {
        if bytes.len() < 12 {
            return Err(Error::Serialization(
                "Insufficient bytes for row header".to_string(),
            ));
        }

        let value_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;

        if col_idx >= value_count {
            return Err(Error::Serialization(format!(
                "Column index {} out of bounds (row has {} columns)",
                col_idx, value_count
            )));
        }

        let offsets_start = 12;
        let values_area_start = offsets_start + value_count * 2;

        // Read the offset for the target column
        let off_pos = offsets_start + col_idx * 2;
        if bytes.len() < off_pos + 2 {
            return Err(Error::Serialization(
                "Insufficient bytes for offset entry".to_string(),
            ));
        }
        let col_offset =
            u16::from_le_bytes(bytes[off_pos..off_pos + 2].try_into().unwrap()) as usize;

        let value_pos = values_area_start + col_offset;
        if value_pos >= bytes.len() {
            return Err(Error::Serialization(
                "Column offset points past end of row data".to_string(),
            ));
        }

        let (value, _) = Value::from_bytes(&bytes[value_pos..])?;
        Ok(value)
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
                Value::varchar("test".to_string()),
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
                Value::varchar("Hello, World!".to_string()),
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
        let row = Row::new(99, vec![Value::varchar(large_string.clone())]);

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
            vec![Value::Int32(10), Value::varchar("test".to_string())],
        );

        assert_eq!(row.get(0), Some(&Value::Int32(10)));
        assert_eq!(row.get(1), Some(&Value::varchar("test".to_string())));
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
    fn test_row_format_with_offsets() {
        // 4-column row similar to blog_posts: id, author_id, title, content
        let row = Row::new(
            42,
            vec![
                Value::Int32(1),
                Value::Int32(5),
                Value::varchar("Post about rust #1".to_string()),
                Value::varchar("This is a long content field with lots of text".to_string()),
            ],
        );

        let bytes = row.to_bytes();

        // Verify header: row_id(8) + col_count(4) + 4 offsets(8) = 20 bytes before values
        let col_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        assert_eq!(col_count, 4);

        // Verify round-trip
        let decoded = Row::from_bytes(&bytes).unwrap();
        assert_eq!(row, decoded);
    }

    #[test]
    fn test_value_at_single_column() {
        let row = Row::new(
            1,
            vec![
                Value::Int32(100),
                Value::varchar("hello".to_string()),
                Value::Int64(999),
            ],
        );

        let bytes = row.to_bytes();

        // Extract each column individually
        let v0 = Row::value_at(&bytes, 0).unwrap();
        assert_eq!(v0, Value::Int32(100));

        let v1 = Row::value_at(&bytes, 1).unwrap();
        assert_eq!(v1, Value::varchar("hello".to_string()));

        let v2 = Row::value_at(&bytes, 2).unwrap();
        assert_eq!(v2, Value::Int64(999));

        // Out of bounds
        assert!(Row::value_at(&bytes, 3).is_err());
    }

    #[test]
    fn test_row_round_trip_many_rows() {
        for i in 0..100 {
            let row = Row::new(
                i,
                vec![
                    Value::Int64(i as i64),
                    Value::varchar(format!("row_{}", i)),
                    Value::Float64(i as f64 * 1.5),
                ],
            );

            let bytes = row.to_bytes();
            let decoded = Row::from_bytes(&bytes).unwrap();
            assert_eq!(row, decoded);
        }
    }
}
