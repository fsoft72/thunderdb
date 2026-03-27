use crate::error::{Error, Result};
use crate::storage::row::Row;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Tombstone marker for deleted rows (single byte prefix)
const TOMBSTONE_MARKER: u8 = 0xFF;
const ACTIVE_MARKER: u8 = 0x00;

/// BufWriter capacity: 256 KB
const BUF_WRITER_CAPACITY: usize = 256 * 1024;

enum DataFileBackend {
    #[allow(dead_code)]
    File(BufWriter<File>),
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
    /// Group commit: time of last sync (None = never synced)
    last_sync: Option<std::time::Instant>,
    /// Group commit interval in ms (0 = disabled)
    group_commit_ms: u64,
    /// Whether the BufWriter has unflushed data
    dirty: bool,
}

impl DataFile {
    /// Open or create a data file
    ///
    /// # Arguments
    /// * `path` - Path to the data.bin file
    /// * `fsync_on_write` - Whether to call fsync after each write
    pub fn open<P: AsRef<Path>>(path: P, fsync_on_write: bool) -> Result<Self> {
        Self::open_with_group_commit(path, fsync_on_write, 0)
    }

    /// Open or create a data file with group commit support
    ///
    /// # Arguments
    /// * `path` - Path to the data.bin file
    /// * `fsync_on_write` - Whether to call fsync after each write
    /// * `group_commit_ms` - Group commit interval in ms (0 = disabled)
    pub fn open_with_group_commit<P: AsRef<Path>>(
        path: P,
        fsync_on_write: bool,
        group_commit_ms: u64,
    ) -> Result<Self> {
        let _path_buf = path.as_ref().to_path_buf();
        let _fsync = fsync_on_write;
        let _group_ms = group_commit_ms;

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
                backend: DataFileBackend::File(BufWriter::with_capacity(BUF_WRITER_CAPACITY, file)),
                current_offset,
                fsync_on_write: _fsync,
                write_buffer: Vec::with_capacity(1024),
                read_buffer: Vec::with_capacity(1024),
                last_sync: None,
                group_commit_ms: _group_ms,
                dirty: false,
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
            last_sync: None,
            group_commit_ms: 0,
            dirty: false,
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
            DataFileBackend::File(writer) => {
                // Seek to end of file
                writer.seek(SeekFrom::End(0))?;

                // Write status marker (active)
                writer.write_all(&[ACTIVE_MARKER])?;

                // Write length prefix
                writer.write_all(&length.to_le_bytes())?;

                // Write row data
                writer.write_all(&self.write_buffer)?;

                self.dirty = true;
            }
            DataFileBackend::Memory(data) => {
                data.push(ACTIVE_MARKER);
                data.extend_from_slice(&length.to_le_bytes());
                data.extend_from_slice(&self.write_buffer);
            }
        }

        // Update current offset (1 byte marker + 4 bytes length + row data)
        self.current_offset += 1 + 4 + length as u64;

        // Group-commit-aware sync
        self.maybe_sync()?;

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
            DataFileBackend::File(writer) => {
                writer.seek(SeekFrom::End(0))?;
                writer.write_all(&self.write_buffer)?;
                self.dirty = true;
            }
            DataFileBackend::Memory(data) => {
                data.extend_from_slice(&self.write_buffer);
            }
        }

        self.current_offset += total_bytes;

        // Group-commit-aware sync
        self.maybe_sync()?;

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
            DataFileBackend::File(writer) => {
                // Only flush buffered writes if there are pending writes
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();

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
            DataFileBackend::File(writer) => {
                // Flush buffered writes before random-access write
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();

                // Seek to offset
                file.seek(SeekFrom::Start(offset))?;

                // Write tombstone marker
                file.write_all(&[TOMBSTONE_MARKER])?;
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

        // Group-commit-aware sync
        self.maybe_sync()?;

        Ok(())
    }

    /// Force synchronize file to disk (always syncs regardless of group commit timer)
    pub fn sync(&mut self) -> Result<()> {
        if let DataFileBackend::File(writer) = &mut self.backend {
            writer.flush()?;
            writer.get_mut().sync_all()?;
            self.last_sync = Some(std::time::Instant::now());
            self.dirty = false;
        }
        Ok(())
    }

    /// Conditionally sync based on group commit interval
    ///
    /// If group_commit_ms == 0: immediate sync (current behavior).
    /// If group_commit_ms > 0: only sync if enough time has elapsed.
    fn maybe_sync(&mut self) -> Result<()> {
        if !self.fsync_on_write {
            return Ok(());
        }

        if let DataFileBackend::File(writer) = &mut self.backend {
            if self.group_commit_ms == 0 {
                // Immediate sync
                writer.flush()?;
                writer.get_mut().sync_all()?;
                self.last_sync = Some(std::time::Instant::now());
                self.dirty = false;
            } else {
                // Group commit: only sync if threshold exceeded
                let should_sync = match self.last_sync {
                    None => true,
                    Some(last) => last.elapsed().as_millis() >= self.group_commit_ms as u128,
                };
                if should_sync {
                    writer.flush()?;
                    writer.get_mut().sync_all()?;
                    self.last_sync = Some(std::time::Instant::now());
                    self.dirty = false;
                }
            }
        }
        Ok(())
    }

    /// Scan active rows with an optional limit for early termination
    pub fn scan_rows_limited(&mut self, limit: Option<usize>) -> Result<Vec<Row>> {
        let max_rows = limit.unwrap_or(usize::MAX);
        let mut results = Vec::new();
        let mut row_buffer = Vec::with_capacity(1024);

        match &mut self.backend {
            DataFileBackend::File(writer) => {
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();
                file.seek(SeekFrom::Start(0))?;

                // Wrap in BufReader to reduce per-row syscalls
                let mut reader = BufReader::with_capacity(256 * 1024, &*file);
                loop {
                    if results.len() >= max_rows {
                        break;
                    }
                    let mut marker = [0u8; 1];
                    match reader.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let mut length_buf = [0u8; 4];
                    reader.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf) as usize;

                    if marker[0] == TOMBSTONE_MARKER {
                        // Skip deleted row data
                        std::io::copy(&mut reader.by_ref().take(length as u64), &mut std::io::sink())?;
                    } else {
                        if row_buffer.len() < length {
                            row_buffer.resize(length, 0);
                        }
                        reader.read_exact(&mut row_buffer[..length])?;
                        results.push(Row::from_bytes(&row_buffer[..length])?);
                    }
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
                    if results.len() >= max_rows {
                        break;
                    }
                    if cursor + 1 + 4 > data.len() {
                        break; // Truncated record header
                    }
                    let marker = data[cursor];
                    cursor += 1;

                    let mut length_buf = [0u8; 4];
                    length_buf.copy_from_slice(&data[cursor..cursor+4]);
                    let length = u32::from_le_bytes(length_buf) as usize;
                    cursor += 4;

                    if cursor + length > data.len() {
                        break; // Truncated record body
                    }
                    if marker != TOMBSTONE_MARKER {
                        results.push(Row::from_bytes(&data[cursor..cursor+length])?);
                    }
                    cursor += length;
                }
            }
        }

        Ok(results)
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
        self.scan_rows_limited(None)
    }

    /// Scan active rows, applying a callback filter on raw bytes.
    ///
    /// For each active row, passes the raw byte buffer to `predicate`.
    /// If it returns `true`, the row is fully deserialized and included.
    /// If `false`, the row is skipped without deserialization.
    pub fn scan_rows_callback<F>(
        &mut self,
        limit: Option<usize>,
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let max_rows = limit.unwrap_or(usize::MAX);
        let mut results = Vec::new();
        let mut row_buffer = Vec::with_capacity(1024);

        match &mut self.backend {
            DataFileBackend::File(writer) => {
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();
                file.seek(SeekFrom::Start(0))?;

                let mut reader = BufReader::with_capacity(256 * 1024, &*file);
                loop {
                    if results.len() >= max_rows {
                        break;
                    }
                    let mut marker = [0u8; 1];
                    match reader.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let mut length_buf = [0u8; 4];
                    reader.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf) as usize;

                    if marker[0] == TOMBSTONE_MARKER {
                        std::io::copy(
                            &mut reader.by_ref().take(length as u64),
                            &mut std::io::sink(),
                        )?;
                    } else {
                        if row_buffer.len() < length {
                            row_buffer.resize(length, 0);
                        }
                        reader.read_exact(&mut row_buffer[..length])?;
                        if predicate(&row_buffer[..length]) {
                            results.push(Row::from_bytes(&row_buffer[..length])?);
                        }
                    }
                }
            }
            DataFileBackend::Memory(data) => {
                let mut cursor = 0usize;
                while cursor < data.len() {
                    if results.len() >= max_rows {
                        break;
                    }
                    if cursor + 1 + 4 > data.len() {
                        break;
                    }
                    let marker = data[cursor];
                    cursor += 1;

                    let length = u32::from_le_bytes(
                        data[cursor..cursor + 4].try_into().unwrap(),
                    ) as usize;
                    cursor += 4;

                    if cursor + length > data.len() {
                        break;
                    }
                    if marker != TOMBSTONE_MARKER {
                        if predicate(&data[cursor..cursor + length]) {
                            results.push(Row::from_bytes(&data[cursor..cursor + length])?);
                        }
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
            DataFileBackend::File(writer) => {
                // Only flush if there are pending writes
                if self.dirty {
                    writer.flush()?;
                    self.dirty = false;
                }
                let file = writer.get_mut();

                file.seek(SeekFrom::Start(0))?;
                let mut reader = BufReader::with_capacity(256 * 1024, &*file);
                let mut offset = 0u64;
                loop {
                    let mut marker = [0u8; 1];
                    match reader.read_exact(&mut marker) {
                        Ok(_) => {}
                        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e.into()),
                    }

                    let deleted = marker[0] == TOMBSTONE_MARKER;
                    let mut length_buf = [0u8; 4];
                    reader.read_exact(&mut length_buf)?;
                    let length = u32::from_le_bytes(length_buf);

                    let mut row_id_buf = [0u8; 8];
                    reader.read_exact(&mut row_id_buf)?;
                    let row_id = u64::from_le_bytes(row_id_buf);

                    // Skip remaining row data past the row_id
                    if length > 8 {
                        std::io::copy(&mut reader.by_ref().take((length - 8) as u64), &mut std::io::sink())?;
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
                Value::varchar(format!("row_{}", row_id)),
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

        // Create a row with large varchar (must fit within u16 offset limit)
        let large_string = "x".repeat(60_000);
        let row = Row::new(1, vec![Value::varchar(large_string.clone())]);

        let (offset, length) = df.append_row(&row).unwrap();
        assert!(length > 60_000);

        let read_row = df.read_row(offset, length).unwrap().unwrap();
        if let Value::Varchar(s) = &read_row.values[0] {
            assert_eq!(s.len(), 60_000);
        } else {
            panic!("Expected Varchar");
        }

        fs::remove_file(path).ok();
    }
}
