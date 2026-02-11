use crate::error::{Error, Result};
use crate::storage::row::Row;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Tombstone marker for deleted rows (single byte prefix)
const TOMBSTONE_MARKER: u8 = 0xFF;
const ACTIVE_MARKER: u8 = 0x00;

enum DataFileBackend {
    #[allow(dead_code)]
    File(File),
    Memory(Vec<u8>),
}

/// Manages the append-only data.bin file
///
/// Format for each row:
/// - Status marker: [1 byte] (0x00 = active, 0xFF = deleted)
/// - Length: [4 bytes, u32 little-endian] (length of row data)
/// - Row data: [variable length, serialized Row]
pub struct DataFile {
    path: PathBuf,
    backend: DataFileBackend,
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
        let _path_buf = path.as_ref().to_path_buf();
        let _fsync = fsync_on_write;

        #[cfg(not(target_arch = "wasm32"))]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&_path_buf)?;

            // Get current file size to know where to append
            let current_offset = file.metadata()?.len();

            Ok(Self {
                path: _path_buf,
                backend: DataFileBackend::File(file),
                current_offset,
                fsync_on_write: _fsync,
                write_buffer: Vec::with_capacity(1024),
                read_buffer: Vec::with_capacity(1024),
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::open_in_memory()
        }
    }

    /// Open an in-memory data file
    pub fn open_in_memory() -> Result<Self> {
        Ok(Self {
            path: PathBuf::from(":memory:"),
            backend: DataFileBackend::Memory(Vec::with_capacity(1024)),
            current_offset: 0,
            fsync_on_write: false,
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

        let offset = self.current_offset;

        match &mut self.backend {
            DataFileBackend::File(file) => {
                // Seek to end of file
                file.seek(SeekFrom::End(0))?;

                // Write status marker (active)
                file.write_all(&[ACTIVE_MARKER])?;

                // Write length prefix
                file.write_all(&length.to_le_bytes())?;

                // Write row data
                file.write_all(&self.write_buffer)?;

                if self.fsync_on_write {
                    file.sync_all()?;
                }
            }
            DataFileBackend::Memory(data) => {
                data.push(ACTIVE_MARKER);
                data.extend_from_slice(&length.to_le_bytes());
                data.extend_from_slice(&self.write_buffer);
            }
        }

        // Update current offset (1 byte marker + 4 bytes length + row data)
        self.current_offset += 1 + 4 + length as u64;

        Ok((offset, length))
    }

    /// Append multiple rows in a single batch I/O operation
    ///
    /// Serializes all rows into the write buffer and writes them in one go,
    /// with a single optional fsync at the end.
    ///
    /// # Arguments
    /// * `rows` - Rows to append
    ///
    /// # Returns
    /// Vector of (offset, length) tuples for each row
    pub fn append_rows_batch(&mut self, rows: &[Row]) -> Result<Vec<(u64, u32)>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize all rows into the write buffer
        self.write_buffer.clear();
        let mut positions = Vec::with_capacity(rows.len());

        for row in rows {
            let row_start = self.write_buffer.len();

            // Reserve space for marker + length prefix
            self.write_buffer.push(ACTIVE_MARKER);
            self.write_buffer.extend_from_slice(&[0u8; 4]); // placeholder length

            // Serialize row data
            let data_start = self.write_buffer.len();
            row.write_to(&mut self.write_buffer)?;
            let data_len = (self.write_buffer.len() - data_start) as u32;

            // Patch the length prefix
            self.write_buffer[row_start + 1..row_start + 5]
                .copy_from_slice(&data_len.to_le_bytes());

            let offset = self.current_offset + row_start as u64;
            positions.push((offset, data_len));
        }

        // Single I/O write for the entire batch
        let total_bytes = self.write_buffer.len() as u64;
        match &mut self.backend {
            DataFileBackend::File(file) => {
                file.seek(SeekFrom::End(0))?;
                file.write_all(&self.write_buffer)?;

                if self.fsync_on_write {
                    file.sync_all()?;
                }
            }
            DataFileBackend::Memory(data) => {
                data.extend_from_slice(&self.write_buffer);
            }
        }

        self.current_offset += total_bytes;

        Ok(positions)
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
        let total_to_read = 1 + 4 + length as usize;

        match &mut self.backend {
            DataFileBackend::File(file) => {
                // Seek to offset
                file.seek(SeekFrom::Start(offset))?;

                // Read marker (1) + length (4) + row data (length) in one go
                if self.read_buffer.len() < total_to_read {
                    self.read_buffer.resize(total_to_read, 0);
                }
                
                file.read_exact(&mut self.read_buffer[..total_to_read])?;

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
            DataFileBackend::Memory(data) => {
                let start = offset as usize;
                let end = start + total_to_read;
                
                if end > data.len() {
                    return Err(Error::Storage(format!("Read out of bounds: {} > {}", end, data.len())));
                }

                let slice = &data[start..end];

                // Check status marker
                if slice[0] == TOMBSTONE_MARKER {
                    return Ok(None);
                }

                // Verify length
                let mut length_buf = [0u8; 4];
                length_buf.copy_from_slice(&slice[1..5]);
                let stored_length = u32::from_le_bytes(length_buf);

                if stored_length != length {
                    return Err(Error::Storage(format!(
                        "Length mismatch at offset {}: expected {}, found {}",
                        offset, length, stored_length
                    )));
                }

                let row = Row::from_bytes(&slice[5..])?;
                Ok(Some(row))
            }
        }
    }

    /// Mark a row as deleted by writing tombstone marker
    ///
    /// # Arguments
    /// * `offset` - Byte offset where the row starts
    pub fn mark_deleted(&mut self, offset: u64) -> Result<()> {
        match &mut self.backend {
            DataFileBackend::File(file) => {
                // Seek to offset
                file.seek(SeekFrom::Start(offset))?;

                // Write tombstone marker
                file.write_all(&[TOMBSTONE_MARKER])?;

                if self.fsync_on_write {
                    file.sync_all()?;
                }
            }
            DataFileBackend::Memory(data) => {
                let idx = offset as usize;
                if idx < data.len() {
                    data[idx] = TOMBSTONE_MARKER;
                } else {
                    return Err(Error::Storage(format!("Mark deleted out of bounds: {} >= {}", idx, data.len())));
                }
            }
        }

        Ok(())
    }

    /// Force synchronize file to disk
    pub fn sync(&mut self) -> Result<()> {
        if let DataFileBackend::File(file) = &mut self.backend {
            file.sync_all()?;
        }
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
        let mut row_buffer = Vec::with_capacity(1024);
        
        match &mut self.backend {
            DataFileBackend::File(file) => {
                file.seek(SeekFrom::Start(0))?;
                loop {
                    let mut marker = [0u8; 1];
                    match file.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let mut length_buf = [0u8; 4];
                    file.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf) as usize;

                    if marker[0] == TOMBSTONE_MARKER {
                        file.seek(SeekFrom::Current(length as i64))?;
                    } else {
                        if row_buffer.len() < length {
                            row_buffer.resize(length, 0);
                        }
                        file.read_exact(&mut row_buffer[..length])?;
                        results.push(Row::from_bytes(&row_buffer[..length])?);
                    }
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
                    let marker = data[cursor];
                    cursor += 1;
                    
                    let mut length_buf = [0u8; 4];
                    length_buf.copy_from_slice(&data[cursor..cursor+4]);
                    let length = u32::from_le_bytes(length_buf) as usize;
                    cursor += 4;

                    if marker != TOMBSTONE_MARKER {
                        results.push(Row::from_bytes(&data[cursor..cursor+length])?);
                    }
                    cursor += length;
                }
            }
        }

        Ok(results)
    }

    /// Scan all rows in the file (for recovery/rebuild)
    ///
    /// Returns vector of (offset, length, row_id, deleted) tuples
    pub fn scan_all(&mut self) -> Result<Vec<(u64, u32, u64, bool)>> {
        let mut results = Vec::new();

        match &mut self.backend {
            DataFileBackend::File(file) => {
                file.seek(SeekFrom::Start(0))?;
                let mut offset = 0u64;
                loop {
                    let mut marker = [0u8; 1];
                    match file.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let deleted = marker[0] == TOMBSTONE_MARKER;
                    let mut length_buf = [0u8; 4];
                    file.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf);

                    let mut row_id_buf = [0u8; 8];
                    file.read_exact(&mut row_id_buf)?;
                    let row_id = u64::from_le_bytes(row_id_buf);

                    if length > 8 {
                        file.seek(SeekFrom::Current((length - 8) as i64))?;
                    }

                    results.push((offset, length, row_id, deleted));
                    offset += 1 + 4 + length as u64;
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
                    let offset = cursor as u64;
                    let marker = data[cursor];
                    cursor += 1;
                    
                    let mut length_buf = [0u8; 4];
                    length_buf.copy_from_slice(&data[cursor..cursor+4]);
                    let length = u32::from_le_bytes(length_buf);
                    cursor += 4;

                    let mut row_id_buf = [0u8; 8];
                    row_id_buf.copy_from_slice(&data[cursor..cursor+8]);
                    let row_id = u64::from_le_bytes(row_id_buf);

                    results.push((offset, length, row_id, marker == TOMBSTONE_MARKER));
                    cursor += length as usize;
                }
            }
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
