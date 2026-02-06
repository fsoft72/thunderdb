use crate::error::{Error, Result};
use crate::storage::row::Row;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Tombstone marker for deleted rows (single byte prefix)
const TOMBSTONE_MARKER: u8 = 0xFF;
const ACTIVE_MARKER: u8 = 0x00;

/// Manages the append-only data.bin file
///
/// Format for each row:
/// - Status marker: [1 byte] (0x00 = active, 0xFF = deleted)
/// - Length: [4 bytes, u32 little-endian] (length of row data)
/// - Row data: [variable length, serialized Row]
pub struct DataFile {
    path: PathBuf,
    file: File,
    current_offset: u64,
    fsync_on_write: bool,
    write_buffer: Vec<u8>,
    read_buffer: Vec<u8>,
}

impl DataFile {
    /// Open or create a data file
    ///
    /// # Arguments
    /// * `path` - Path to the data.bin file
    /// * `fsync_on_write` - Whether to call fsync after each write
    pub fn open<P: AsRef<Path>>(path: P, fsync_on_write: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Get current file size to know where to append
        let current_offset = file.metadata()?.len();

        Ok(Self {
            path,
            file,
            current_offset,
            fsync_on_write,
            write_buffer: Vec::with_capacity(1024),
            read_buffer: Vec::with_capacity(1024),
        })
    }

    /// Append a row to the data file
    ///
    /// # Arguments
    /// * `row` - Row to append
    ///
    /// # Returns
    /// (offset, length) tuple indicating where the row was written
    pub fn append_row(&mut self, row: &Row) -> Result<(u64, u32)> {
        // We still need to know the length before writing to put it in the header
        self.write_buffer.clear();
        row.write_to(&mut self.write_buffer)?;
        let length = self.write_buffer.len() as u32;

        // Seek to end of file
        self.file.seek(SeekFrom::End(0))?;
        let offset = self.current_offset;

        // Write status marker (active)
        self.file.write_all(&[ACTIVE_MARKER])?;

        // Write length prefix
        self.file.write_all(&length.to_le_bytes())?;

        // Write row data
        self.file.write_all(&self.write_buffer)?;

        if self.fsync_on_write {
            self.file.sync_all()?;
        }

        // Update current offset (1 byte marker + 4 bytes length + row data)
        self.current_offset += 1 + 4 + length as u64;

        Ok((offset, length))
    }

    /// Read a row from the data file
    ///
    /// # Arguments
    /// * `offset` - Byte offset where the row starts
    /// * `length` - Length of the row data (from RAT)
    ///
    /// # Returns
    /// The deserialized Row, or None if row is deleted
    pub fn read_row(&mut self, offset: u64, length: u32) -> Result<Option<Row>> {
        // Seek to offset
        self.file.seek(SeekFrom::Start(offset))?;

        // Read marker (1) + length (4) + row data (length) in one go
        let total_to_read = 1 + 4 + length as usize;
        if self.read_buffer.len() < total_to_read {
            self.read_buffer.resize(total_to_read, 0);
        }
        
        self.file.read_exact(&mut self.read_buffer[..total_to_read])?;

        // Check status marker
        if self.read_buffer[0] == TOMBSTONE_MARKER {
            return Ok(None);
        }

        // Verify length prefix matches what we expect from RAT
        let mut length_buf = [0u8; 4];
        length_buf.copy_from_slice(&self.read_buffer[1..5]);
        let stored_length = u32::from_le_bytes(length_buf);

        if stored_length != length {
            return Err(Error::Storage(format!(
                "Length mismatch at offset {}: expected {}, found {}",
                offset, length, stored_length
            )));
        }

        // Deserialize row from the remaining buffer
        let row = Row::from_bytes(&self.read_buffer[5..total_to_read])?;

        Ok(Some(row))
    }

    /// Mark a row as deleted by writing tombstone marker
    ///
    /// # Arguments
    /// * `offset` - Byte offset where the row starts
    pub fn mark_deleted(&mut self, offset: u64) -> Result<()> {
        // Seek to offset
        self.file.seek(SeekFrom::Start(offset))?;

        // Write tombstone marker
        self.file.write_all(&[TOMBSTONE_MARKER])?;

        if self.fsync_on_write {
            self.file.sync_all()?;
        }

        Ok(())
    }

    /// Force synchronize file to disk
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Get current file size
    pub fn size(&self) -> u64 {
        self.current_offset
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Scan all active rows in the file (more efficient than individual reads)
    pub fn scan_rows(&mut self) -> Result<Vec<Row>> {
        let mut results = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        let mut row_buffer = Vec::with_capacity(1024);
        
        loop {
            // Read status marker
            let mut marker = [0u8; 1];
            match self.file.read_exact(&mut marker) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Read length
            let mut length_buf = [0u8; 4];
            self.file.read_exact(&mut length_buf)?;
            let length = u32::from_le_bytes(length_buf) as usize;

            if marker[0] == TOMBSTONE_MARKER {
                // Skip deleted row
                self.file.seek(SeekFrom::Current(length as i64))?;
            } else {
                // Read and deserialize active row
                if row_buffer.len() < length {
                    row_buffer.resize(length, 0);
                }
                self.file.read_exact(&mut row_buffer[..length])?;
                results.push(Row::from_bytes(&row_buffer[..length])?);
            }
        }

        Ok(results)
    }

    /// Scan all rows in the file (for recovery/rebuild)
    ///
    /// Returns vector of (offset, length, row_id, deleted) tuples
    pub fn scan_all(&mut self) -> Result<Vec<(u64, u32, u64, bool)>> {
        let mut results = Vec::new();

        self.file.seek(SeekFrom::Start(0))?;
        let mut offset = 0u64;

        loop {
            // Try to read status marker
            let mut marker = [0u8; 1];
            match self.file.read_exact(&mut marker) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let deleted = marker[0] == TOMBSTONE_MARKER;

            // Read length
            let mut length_buf = [0u8; 4];
            self.file.read_exact(&mut length_buf)?;
            let length = u32::from_le_bytes(length_buf);

            // Read only row_id (first 8 bytes of row data) to save I/O and allocations
            let mut row_id_buf = [0u8; 8];
            self.file.read_exact(&mut row_id_buf)?;
            let row_id = u64::from_le_bytes(row_id_buf);

            // Seek past the rest of the row data
            if length > 8 {
                self.file.seek(SeekFrom::Current((length - 8) as i64))?;
            }

            results.push((offset, length, row_id, deleted));

            offset += 1 + 4 + length as u64;
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::value::Value;
    use std::fs;

    fn create_test_row(row_id: u64) -> Row {
        Row::new(
            row_id,
            vec![
                Value::Int64(row_id as i64),
                Value::Varchar(format!("row_{}", row_id)),
                Value::Float64(row_id as f64 * 1.5),
            ],
        )
    }

    #[test]
    fn test_datafile_create() {
        let path = "/tmp/test_data_create.bin";
        let _ = fs::remove_file(path);

        let df = DataFile::open(path, false).unwrap();
        assert_eq!(df.size(), 0);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_append_and_read() {
        let path = "/tmp/test_data_append.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        let row = create_test_row(1);
        let (offset, length) = df.append_row(&row).unwrap();

        assert_eq!(offset, 0);
        assert!(length > 0);

        let read_row = df.read_row(offset, length).unwrap();
        assert_eq!(read_row, Some(row));

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_multiple_rows() {
        let path = "/tmp/test_data_multiple.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        let mut positions = Vec::new();

        // Append 10 rows
        for i in 1..=10 {
            let row = create_test_row(i);
            let (offset, length) = df.append_row(&row).unwrap();
            positions.push((offset, length, row));
        }

        // Read them back
        for (offset, length, expected_row) in positions {
            let row = df.read_row(offset, length).unwrap();
            assert_eq!(row, Some(expected_row));
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_mark_deleted() {
        let path = "/tmp/test_data_delete.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        let row = create_test_row(1);
        let (offset, length) = df.append_row(&row).unwrap();

        // Verify it's readable
        let read_row = df.read_row(offset, length).unwrap();
        assert_eq!(read_row, Some(row));

        // Mark as deleted
        df.mark_deleted(offset).unwrap();

        // Should return None now
        let read_row = df.read_row(offset, length).unwrap();
        assert_eq!(read_row, None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_persistence() {
        let path = "/tmp/test_data_persist.bin";
        let _ = fs::remove_file(path);

        let positions = {
            let mut df = DataFile::open(path, true).unwrap();

            let mut positions = Vec::new();
            for i in 1..=5 {
                let row = create_test_row(i);
                let (offset, length) = df.append_row(&row).unwrap();
                positions.push((offset, length));
            }
            positions
        };

        // Reopen and read
        let mut df = DataFile::open(path, false).unwrap();
        assert!(df.size() > 0);

        for (offset, length) in positions {
            let row = df.read_row(offset, length).unwrap();
            assert!(row.is_some());
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_scan_all() {
        let path = "/tmp/test_data_scan.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        // Append some rows
        for i in 1..=5 {
            let row = create_test_row(i);
            df.append_row(&row).unwrap();
        }

        // Scan all
        let entries = df.scan_all().unwrap();
        assert_eq!(entries.len(), 5);

        for (i, (_offset, length, row_id, deleted)) in entries.iter().enumerate() {
            assert_eq!(*row_id, (i + 1) as u64);
            assert!(!deleted);
            assert!(*length > 0);
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_scan_with_deletes() {
        let path = "/tmp/test_data_scan_delete.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        let mut positions = Vec::new();
        for i in 1..=5 {
            let row = create_test_row(i);
            let (offset, length) = df.append_row(&row).unwrap();
            positions.push((offset, length));
        }

        // Delete row 2 and 4
        df.mark_deleted(positions[1].0).unwrap();
        df.mark_deleted(positions[3].0).unwrap();

        // Scan all
        let entries = df.scan_all().unwrap();
        assert_eq!(entries.len(), 5);

        assert!(!entries[0].3); // row 1 - not deleted
        assert!(entries[1].3);  // row 2 - deleted
        assert!(!entries[2].3); // row 3 - not deleted
        assert!(entries[3].3);  // row 4 - deleted
        assert!(!entries[4].3); // row 5 - not deleted

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_datafile_large_rows() {
        let path = "/tmp/test_data_large.bin";
        let _ = fs::remove_file(path);

        let mut df = DataFile::open(path, false).unwrap();

        // Create a row with large varchar
        let large_string = "x".repeat(100_000);
        let row = Row::new(1, vec![Value::Varchar(large_string.clone())]);

        let (offset, length) = df.append_row(&row).unwrap();
        assert!(length > 100_000);

        let read_row = df.read_row(offset, length).unwrap().unwrap();
        if let Value::Varchar(s) = &read_row.values[0] {
            assert_eq!(s.len(), 100_000);
        } else {
            panic!("Expected Varchar");
        }

        fs::remove_file(path).ok();
    }
}
