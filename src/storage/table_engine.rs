use crate::config::StorageConfig;
use crate::error::{Error, Result};
use crate::storage::{DataFile, RecordAddressTable, Row, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// TableEngine coordinates storage operations for a single table
///
/// Manages:
/// - data.bin (append-only row storage)
/// - rat.bin (Record Address Table)
/// - Auto-generated row IDs
pub struct TableEngine {
    name: String,
    table_dir: PathBuf,
    data_file: DataFile,
    rat: RecordAddressTable,
    next_row_id: AtomicU64,
    config: StorageConfig,
}

impl TableEngine {
    /// Open or create a table
    ///
    /// # Arguments
    /// * `name` - Table name
    /// * `base_dir` - Base directory for database
    /// * `config` - Storage configuration
    pub fn open<P: AsRef<Path>>(name: &str, base_dir: P, config: StorageConfig) -> Result<Self> {
        let base_dir = base_dir.as_ref();

        // Create table directory
        let table_dir = base_dir.join(name);
        if ! table_dir.exists() {
            std::fs::create_dir_all(&table_dir)?;
        }

        // Open data file
        let data_path = table_dir.join("data.bin");
        let data_file = DataFile::open(&data_path, config.fsync_on_write)?;

        // Load RAT
        let rat_path = table_dir.join("rat.bin");
        let rat = RecordAddressTable::load(&rat_path)?;

        // Determine next row ID
        let max_row_id = rat.row_ids().into_iter().max().unwrap_or(0);
        let next_row_id = AtomicU64::new(max_row_id + 1);

        Ok(Self {
            name: name.to_string(),
            table_dir,
            data_file,
            rat,
            next_row_id,
            config,
        })
    }

    /// Insert a new row
    ///
    /// # Arguments
    /// * `values` - Column values for the row
    ///
    /// # Returns
    /// The auto-generated row_id
    pub fn insert_row(&mut self, values: Vec<Value>) -> Result<u64> {
        // Generate new row ID
        let row_id = self.next_row_id.fetch_add(1, Ordering::SeqCst);

        // Create row
        let row = Row::new(row_id, values);

        // Append to data file
        let (offset, length) = self.data_file.append_row(&row)?;

        // Insert into RAT
        self.rat.insert(row_id, offset, length)?;

        Ok(row_id)
    }

    /// Insert multiple rows in batch
    ///
    /// # Arguments
    /// * `rows` - Vector of value vectors to insert
    ///
    /// # Returns
    /// Vector of auto-generated row_ids
    pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) -> Result<Vec<u64>> {
        let mut row_ids = Vec::with_capacity(rows.len());

        for values in rows {
            let row_id = self.insert_row(values)?;
            row_ids.push(row_id);
        }

        Ok(row_ids)
    }

    /// Get a row by ID
    ///
    /// # Arguments
    /// * `row_id` - Row ID to retrieve
    ///
    /// # Returns
    /// The row if found and not deleted, None otherwise
    pub fn get_by_id(&mut self, row_id: u64) -> Result<Option<Row>> {
        // Look up in RAT
        if let Some((offset, length)) = self.rat.get(row_id) {
            // Read from data file
            self.data_file.read_row(offset, length)
        } else {
            Ok(None)
        }
    }

    /// Delete a row by ID
    ///
    /// # Arguments
    /// * `row_id` - Row ID to delete
    ///
    /// # Returns
    /// true if row was found and deleted, false otherwise
    pub fn delete_by_id(&mut self, row_id: u64) -> Result<bool> {
        if let Some((offset, _length)) = self.rat.get(row_id) {
            // Mark as deleted in data file
            self.data_file.mark_deleted(offset)?;

            // Mark as deleted in RAT
            self.rat.delete(row_id);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get all active row IDs
    pub fn active_row_ids(&self) -> Vec<u64> {
        self.rat.active_row_ids()
    }

    /// Scan all active rows
    ///
    /// Returns all non-deleted rows
    pub fn scan_all(&mut self) -> Result<Vec<Row>> {
        let row_ids = self.active_row_ids();
        let mut rows = Vec::with_capacity(row_ids.len());

        for row_id in row_ids {
            if let Some(row) = self.get_by_id(row_id)? {
                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Get table statistics
    pub fn stats(&self) -> TableStats {
        TableStats {
            name: self.name.clone(),
            total_rows: self.rat.len(),
            active_rows: self.rat.active_count(),
            data_file_size: self.data_file.size(),
        }
    }

    /// Flush RAT to disk
    pub fn flush(&mut self) -> Result<()> {
        if self.rat.is_dirty() {
            let rat_path = self.table_dir.join("rat.bin");
            self.rat.save(rat_path)?;
        }

        if ! self.config.fsync_on_write {
            self.data_file.sync()?;
        }

        Ok(())
    }

    /// Compact the table by removing deleted entries from RAT
    ///
    /// This does NOT rewrite data.bin (garbage collection is a future feature)
    pub fn compact(&mut self) -> Result<()> {
        self.rat.compact();
        self.flush()?;
        Ok(())
    }

    /// Rebuild RAT from data file
    ///
    /// Useful for recovery or after corruption
    pub fn rebuild_rat(&mut self) -> Result<()> {
        let entries = self.data_file.scan_all()?;

        self.rat = RecordAddressTable::new();
        let mut max_row_id = 0u64;

        for (offset, length, row_id, deleted) in entries {
            self.rat.insert(row_id, offset, length)?;
            if deleted {
                self.rat.delete(row_id);
            }
            max_row_id = max_row_id.max(row_id);
        }

        self.next_row_id.store(max_row_id + 1, Ordering::SeqCst);

        self.flush()?;

        Ok(())
    }

    /// Get table name
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStats {
    pub name: String,
    pub total_rows: usize,
    pub active_rows: usize,
    pub data_file_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_table(name: &str) -> TableEngine {
        let base_dir = format!("/tmp/thunderdb_test_{}", name);
        let _ = fs::remove_dir_all(&base_dir);

        let config = StorageConfig {
            data_dir: base_dir.clone(),
            fsync_on_write: false,
            fsync_interval_ms: 1000,
            max_data_file_size_mb: 1024,
        };

        TableEngine::open(name, &base_dir, config).unwrap()
    }

    #[test]
    fn test_table_create() {
        let table = create_test_table("test_create");
        assert_eq!(table.name(), "test_create");
        assert_eq!(table.stats().active_rows, 0);
    }

    #[test]
    fn test_table_insert_and_get() {
        let mut table = create_test_table("test_insert");

        let values = vec![
            Value::Int32(42),
            Value::Varchar("Alice".to_string()),
            Value::Float64(3.14),
        ];

        let row_id = table.insert_row(values.clone()).unwrap();
        assert_eq!(row_id, 1);

        let row = table.get_by_id(row_id).unwrap().unwrap();
        assert_eq!(row.row_id, row_id);
        assert_eq!(row.values, values);
    }

    #[test]
    fn test_table_insert_multiple() {
        let mut table = create_test_table("test_insert_multiple");

        let row_id1 = table
            .insert_row(vec![Value::Int32(1), Value::Varchar("A".to_string())])
            .unwrap();
        let row_id2 = table
            .insert_row(vec![Value::Int32(2), Value::Varchar("B".to_string())])
            .unwrap();
        let row_id3 = table
            .insert_row(vec![Value::Int32(3), Value::Varchar("C".to_string())])
            .unwrap();

        assert_eq!(row_id1, 1);
        assert_eq!(row_id2, 2);
        assert_eq!(row_id3, 3);

        assert_eq!(table.stats().active_rows, 3);
    }

    #[test]
    fn test_table_insert_batch() {
        let mut table = create_test_table("test_batch");

        let rows = vec![
            vec![Value::Int32(1), Value::Varchar("A".to_string())],
            vec![Value::Int32(2), Value::Varchar("B".to_string())],
            vec![Value::Int32(3), Value::Varchar("C".to_string())],
        ];

        let row_ids = table.insert_batch(rows).unwrap();
        assert_eq!(row_ids, vec![1, 2, 3]);
        assert_eq!(table.stats().active_rows, 3);
    }

    #[test]
    fn test_table_delete() {
        let mut table = create_test_table("test_delete");

        let row_id = table
            .insert_row(vec![Value::Int32(42)])
            .unwrap();

        assert!(table.get_by_id(row_id).unwrap().is_some());

        let deleted = table.delete_by_id(row_id).unwrap();
        assert!(deleted);

        assert!(table.get_by_id(row_id).unwrap().is_none());

        // Try deleting again
        let deleted = table.delete_by_id(row_id).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_table_scan_all() {
        let mut table = create_test_table("test_scan");

        for i in 1..=10 {
            table
                .insert_row(vec![Value::Int32(i), Value::Varchar(format!("row_{}", i))])
                .unwrap();
        }

        let rows = table.scan_all().unwrap();
        assert_eq!(rows.len(), 10);

        // Delete some rows
        table.delete_by_id(3).unwrap();
        table.delete_by_id(7).unwrap();

        let rows = table.scan_all().unwrap();
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn test_table_persistence() {
        let base_dir = "/tmp/thunderdb_test_persist";
        let _ = fs::remove_dir_all(base_dir);

        let config = StorageConfig {
            data_dir: base_dir.to_string(),
            fsync_on_write: false,
            fsync_interval_ms: 1000,
            max_data_file_size_mb: 1024,
        };

        // Create table and insert data
        {
            let mut table = TableEngine::open("users", base_dir, config.clone()).unwrap();

            for i in 1..=5 {
                table
                    .insert_row(vec![
                        Value::Int64(i),
                        Value::Varchar(format!("user_{}", i)),
                    ])
                    .unwrap();
            }

            table.delete_by_id(3).unwrap();
            table.flush().unwrap();
        }

        // Reopen and verify
        {
            let mut table = TableEngine::open("users", base_dir, config).unwrap();

            assert_eq!(table.stats().total_rows, 5);
            assert_eq!(table.stats().active_rows, 4);

            assert!(table.get_by_id(1).unwrap().is_some());
            assert!(table.get_by_id(2).unwrap().is_some());
            assert!(table.get_by_id(3).unwrap().is_none()); // deleted
            assert!(table.get_by_id(4).unwrap().is_some());
            assert!(table.get_by_id(5).unwrap().is_some());
        }

        fs::remove_dir_all(base_dir).ok();
    }

    #[test]
    fn test_table_compact() {
        let mut table = create_test_table("test_compact");

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        // Delete half
        for i in (1..=10).step_by(2) {
            table.delete_by_id(i).unwrap();
        }

        assert_eq!(table.stats().total_rows, 10);
        assert_eq!(table.stats().active_rows, 5);

        table.compact().unwrap();

        assert_eq!(table.stats().total_rows, 5);
        assert_eq!(table.stats().active_rows, 5);
    }

    #[test]
    fn test_table_rebuild_rat() {
        let base_dir = "/tmp/thunderdb_test_rebuild";
        let _ = fs::remove_dir_all(base_dir);

        let config = StorageConfig {
            data_dir: base_dir.to_string(),
            fsync_on_write: false,
            fsync_interval_ms: 1000,
            max_data_file_size_mb: 1024,
        };

        // Create table and insert data
        {
            let mut table = TableEngine::open("test", base_dir, config.clone()).unwrap();

            for i in 1..=5 {
                table.insert_row(vec![Value::Int32(i)]).unwrap();
            }

            table.flush().unwrap();
        }

        // Delete RAT file to simulate corruption
        let rat_path = PathBuf::from(base_dir).join("test/rat.bin");
        fs::remove_file(&rat_path).unwrap();

        // Reopen and rebuild
        {
            let mut table = TableEngine::open("test", base_dir, config).unwrap();

            // RAT should be empty
            assert_eq!(table.stats().total_rows, 0);

            // Rebuild
            table.rebuild_rat().unwrap();

            // Should have recovered all rows
            assert_eq!(table.stats().active_rows, 5);

            for i in 1..=5 {
                assert!(table.get_by_id(i).unwrap().is_some());
            }
        }

        fs::remove_dir_all(base_dir).ok();
    }

    #[test]
    fn test_table_active_row_ids() {
        let mut table = create_test_table("test_active_ids");

        for i in 1..=5 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        table.delete_by_id(2).unwrap();
        table.delete_by_id(4).unwrap();

        let active_ids = table.active_row_ids();
        assert_eq!(active_ids, vec![1, 3, 5]);
    }

    #[test]
    fn test_table_stats() {
        let mut table = create_test_table("test_stats");

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        table.delete_by_id(5).unwrap();

        let stats = table.stats();
        assert_eq!(stats.name, "test_stats");
        assert_eq!(stats.total_rows, 10);
        assert_eq!(stats.active_rows, 9);
        assert!(stats.data_file_size > 0);
    }
}
