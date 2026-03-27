use crate::config::StorageConfig;
use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::Error;
use crate::storage::{DataFile, RecordAddressTable, Row, Value};
use crate::index::IndexManager;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use serde::{Serialize, Deserialize};

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Table schema metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
}

/// TableEngine coordinates storage operations for a single table
///
/// Manages:
/// - data.bin (append-only row storage)
/// - rat.bin (Record Address Table)
/// - schema.json (Table metadata)
/// - indices/ directory
/// - Auto-generated row IDs
pub struct TableEngine {
    name: String,
    table_dir: PathBuf,
    data_file: DataFile,
    rat: RecordAddressTable,
    index_manager: IndexManager,
    schema: Option<TableSchema>,
    next_row_id: u64,
    config: StorageConfig,
    /// Cached column name -> position mapping, invalidated on schema change
    column_mapping_cache: Option<Arc<HashMap<String, usize>>>,
}

impl TableEngine {
    /// Open an existing table
    ///
    /// # Arguments
    /// * `name` - Table name
    /// * `base_dir` - Base directory for database
    /// * `config` - Storage configuration
    ///
    /// # Returns
    /// A Result with the TableEngine if it exists, or Error::TableNotFound
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open<P: AsRef<Path>>(name: &str, base_dir: P, config: StorageConfig, btree_order: usize) -> Result<Self> {
        let base_dir = base_dir.as_ref();

        // Check if table directory exists
        let table_dir = base_dir.join(name);
        if ! table_dir.exists() {
            return Err(Error::TableNotFound(name.to_string()));
        }

        // Open data file
        let data_path = table_dir.join("data.bin");
        let data_file = DataFile::open_with_group_commit(
            &data_path,
            config.fsync_on_write,
            config.group_commit_interval_ms,
        )?;

        // Load RAT
        let rat_path = table_dir.join("rat.bin");
        let rat = RecordAddressTable::load(&rat_path)?;

        // Initialize Index Manager
        let index_dir = table_dir.join("indices");
        let mut index_manager = IndexManager::new(name, &index_dir, btree_order)?;
        index_manager.load()?;

        // Load Schema
        let schema_path = table_dir.join("schema.json");
        let schema = if schema_path.exists() {
            let content = std::fs::read_to_string(&schema_path)?;
            Some(serde_json::from_str(&content)?)
        } else {
            None
        };

        // Determine next row ID — O(log n) via BTreeMap last key
        let max_row_id = rat.max_row_id().unwrap_or(0);
        let next_row_id = max_row_id + 1;

        Ok(Self {
            name: name.to_string(),
            table_dir,
            data_file,
            rat,
            index_manager,
            schema,
            next_row_id,
            config,
            column_mapping_cache: None,
        })
    }

    /// Open an in-memory table
    pub fn open_in_memory(name: &str, config: StorageConfig, btree_order: usize) -> Result<Self> {
        let data_file = DataFile::open_in_memory()?;
        let rat = RecordAddressTable::new();
        let index_manager = IndexManager::new(name, ":memory:", btree_order)?;

        Ok(Self {
            name: name.to_string(),
            table_dir: PathBuf::from(":memory:"),
            data_file,
            rat,
            index_manager,
            schema: None,
            next_row_id: 1,
            config,
            column_mapping_cache: None,
        })
    }

    /// Load a table from disk into memory
    pub fn load_to_memory<P: AsRef<Path>>(name: &str, base_dir: P, mut config: StorageConfig, btree_order: usize) -> Result<Self> {
        let base_dir = base_dir.as_ref();
        let table_dir = base_dir.join(name);

        if ! table_dir.exists() || base_dir.to_string_lossy() == ":memory:" {
            config.in_memory = true;
            return Self::open_in_memory(name, config, btree_order);
        }

        // Open disk-based table first to read its data
        let mut disk_table = Self::open(name, base_dir, config.clone(), btree_order)?;
        
        // Create in-memory table
        config.in_memory = true;
        let mut mem_table = Self::open_in_memory(name, config, btree_order)?;

        // Copy schema
        if let Some(schema) = disk_table.schema() {
            mem_table.schema = Some(schema.clone());
        }

        // Copy all rows
        let rows = disk_table.data_file.scan_rows()?;
        for row in rows {
            mem_table.insert_row(row.values)?;
        }

        // Recreate indices with backfill from the rows we just copied
        let indexed_columns = disk_table.index_manager.indexed_columns().to_vec();
        for col in indexed_columns {
            mem_table.create_index(&col)?;
        }

        Ok(mem_table)
    }

    /// Set table schema
    pub fn set_schema(&mut self, schema: TableSchema) -> Result<()> {
        if ! self.config.in_memory {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let schema_path = self.table_dir.join("schema.json");
                let content = serde_json::to_string_pretty(&schema)?;
                std::fs::write(schema_path, content)?;
            }
        }
        self.schema = Some(schema);
        self.column_mapping_cache = None; // Invalidate cache on schema change
        Ok(())
    }

    /// Get table schema
    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    /// Create a new table or open it if it already exists
    ///
    /// # Arguments
    /// * `name` - Table name
    /// * `base_dir` - Base directory for database
    /// * `config` - Storage configuration
    pub fn create<P: AsRef<Path>>(name: &str, base_dir: P, config: StorageConfig, btree_order: usize) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let base_dir = base_dir.as_ref();

            // Create table directory if it doesn't exist
            let table_dir = base_dir.join(name);
            if ! table_dir.exists() {
                std::fs::create_dir_all(&table_dir)?;
                // Also create indices directory
                std::fs::create_dir_all(table_dir.join("indices"))?;
            }

            Self::open(name, base_dir, config, btree_order)
        }
        #[cfg(target_arch = "wasm32")]
        {
            let _ = base_dir;
            Self::open_in_memory(name, config, btree_order)
        }
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
        let row_id = self.next_row_id;
        self.next_row_id += 1;

        // Create row
        let row = Row::new(row_id, values);

        // Append to data file
        let (offset, length) = self.data_file.append_row(&row)?;

        // Insert into RAT
        self.rat.insert(row_id, offset, length)?;

        // Update indices
        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.insert_row(&row, &mapping)?;
        }

        Ok(row_id)
    }

    /// Insert multiple rows in batch
    ///
    /// Optimized for throughput: single I/O write, bulk RAT update,
    /// column mapping computed once, and batched index updates.
    ///
    /// # Arguments
    /// * `rows` - Vector of value vectors to insert
    ///
    /// # Returns
    /// Vector of auto-generated row_ids
    pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) -> Result<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Generate all row IDs upfront
        let start_id = self.next_row_id;
        self.next_row_id += rows.len() as u64;
        let row_ids: Vec<u64> = (start_id..start_id + rows.len() as u64).collect();

        // 2. Create all Row objects
        let row_objects: Vec<Row> = row_ids
            .iter()
            .zip(rows.into_iter())
            .map(|(&row_id, values)| Row::new(row_id, values))
            .collect();

        // 3. Batch data file write — single I/O
        let positions = self.data_file.append_rows_batch(&row_objects)?;

        // 4. Bulk RAT update
        let rat_entries: Vec<(u64, u64, u32)> = row_ids
            .iter()
            .zip(positions.iter())
            .map(|(&row_id, &(offset, length))| (row_id, offset, length))
            .collect();
        self.rat.bulk_insert(rat_entries)?;

        // 5. Batch index updates (sorted insertion for better cache locality)
        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.insert_rows_batch(&row_objects, &mapping)?;
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

    /// Update a row by ID
    ///
    /// Reads old row to remove stale index entries, then appends new row.
    ///
    /// # Arguments
    /// * `row_id` - Row ID to update
    /// * `values` - New column values
    ///
    /// # Returns
    /// true if row was found and updated, false otherwise
    pub fn update_row(&mut self, row_id: u64, values: Vec<Value>) -> Result<bool> {
        if let Some((offset, length)) = self.rat.get(row_id) {
            // Read old row for index deletion
            let old_values = if !self.index_manager.indexed_columns().is_empty() {
                self.data_file.read_row(offset, length)?
                    .map(|row| row.values)
            } else {
                None
            };

            // Mark old row as deleted in data file
            self.data_file.mark_deleted(offset)?;

            // Mark as deleted in RAT so insert can overwrite it
            self.rat.delete(row_id);

            // Create new row with same ID
            let row = Row::new(row_id, values);

            // Append to data file
            let (new_offset, new_length) = self.data_file.append_row(&row)?;

            // Update RAT with new position (now allowed because it's deleted)
            self.rat.insert(row_id, new_offset, new_length)?;

            // Update indices: remove old entries, insert new ones
            if !self.index_manager.indexed_columns().is_empty() {
                let mapping = self.build_column_mapping();
                if let Some(old_vals) = old_values {
                    self.index_manager.delete_row(row_id, &old_vals, &mapping)?;
                }
                self.index_manager.insert_row(&row, &mapping)?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete a row by ID
    ///
    /// Reads the row before marking it deleted so we can remove it from indices.
    ///
    /// # Arguments
    /// * `row_id` - Row ID to delete
    ///
    /// # Returns
    /// true if row was found and deleted, false otherwise
    pub fn delete_by_id(&mut self, row_id: u64) -> Result<bool> {
        if let Some((offset, length)) = self.rat.get(row_id) {
            // Read the row before deleting so we can remove from indices
            let old_values = if !self.index_manager.indexed_columns().is_empty() {
                self.data_file.read_row(offset, length)?
                    .map(|row| row.values)
            } else {
                None
            };

            // Mark as deleted in data file
            self.data_file.mark_deleted(offset)?;

            // Mark as deleted in RAT
            self.rat.delete(row_id);

            // Remove from indices using old values
            if let Some(values) = old_values {
                let mapping = self.build_column_mapping();
                self.index_manager.delete_row(row_id, &values, &mapping)?;
            }

            // Auto-compact if threshold exceeded
            if self.config.auto_compact && self.config.compaction_threshold > 0.0 {
                let total = self.rat.len();
                let active = self.rat.active_count();
                if total > 0 && (total - active) as f64 / total as f64 >= self.config.compaction_threshold {
                    self.full_compact()?;
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get multiple rows by their IDs
    ///
    /// Sorts reads by file offset for sequential I/O access pattern.
    /// Skips missing/deleted rows.
    pub fn get_by_ids(&mut self, row_ids: &[u64]) -> Result<Vec<Row>> {
        self.fetch_rows_sorted_by_offset(row_ids)
    }

    /// Get multiple rows by ID, filtering on raw bytes before deserializing.
    ///
    /// Like `get_by_ids()` but applies a predicate to the raw row bytes.
    /// Only rows where `predicate` returns `true` are deserialized.
    /// Use `Row::value_at()` inside the predicate for column-level checks.
    pub fn get_by_ids_filtered<F>(
        &mut self,
        row_ids: &[u64],
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if let Some((offset, length)) = self.rat.get(row_id) {
                entries.push((offset, length));
            }
        }

        entries.sort_unstable_by_key(|&(offset, _)| offset);

        let mut rows = Vec::with_capacity(entries.len());
        for (offset, length) in entries {
            if let Some(raw) = self.data_file.read_raw(offset, length)? {
                if predicate(&raw) {
                    rows.push(Row::from_bytes(&raw)?);
                }
            }
        }
        Ok(rows)
    }

    /// Resolve RAT entries for the given row_ids, sort by on-disk offset,
    /// and read rows sequentially to avoid scattered seeks.
    fn fetch_rows_sorted_by_offset(&mut self, row_ids: &[u64]) -> Result<Vec<Row>> {
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if let Some((offset, length)) = self.rat.get(row_id) {
                entries.push((offset, length));
            }
        }

        entries.sort_unstable_by_key(|&(offset, _)| offset);

        let mut rows = Vec::with_capacity(entries.len());
        for (offset, length) in entries {
            if let Some(row) = self.data_file.read_row(offset, length)? {
                rows.push(row);
            }
        }
        Ok(rows)
    }

    /// Get the number of active rows without scanning (O(1))
    pub fn active_row_count(&self) -> usize {
        self.rat.active_count()
    }

    /// Get all active row IDs
    pub fn active_row_ids(&self) -> Vec<u64> {
        self.rat.active_row_ids()
    }

    /// Scan all active rows
    ///
    /// Returns all non-deleted rows
    pub fn scan_all(&mut self) -> Result<Vec<Row>> {
        // Sequential scan of the data file is much faster than random access
        // because it avoids seeks and benefits from OS prefetching.
        self.data_file.scan_rows()
    }

    /// Scan active rows with an optional limit for early termination
    pub fn scan_all_limited(&mut self, limit: Option<usize>) -> Result<Vec<Row>> {
        self.data_file.scan_rows_limited(limit)
    }

    /// Scan active rows with a callback predicate on raw bytes.
    ///
    /// The predicate receives the raw serialized row bytes (without the
    /// data-file marker/length envelope). Use `Row::value_at()` inside the
    /// predicate to extract individual columns for filtering.
    pub fn scan_all_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.data_file.scan_rows_callback(None, predicate)
    }

    /// Count active rows matching a predicate without deserializing.
    pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.data_file.count_rows_callback(predicate)
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
        if self.config.in_memory {
            return Ok(());
        }

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

    /// Full compaction: rewrite data.bin to reclaim space from tombstones
    ///
    /// Scans all active rows, writes them to a new data file, rebuilds
    /// the RAT and all indices. Works for both disk and in-memory tables.
    pub fn full_compact(&mut self) -> Result<()> {
        let active_rows = self.data_file.scan_rows()?;

        if self.config.in_memory {
            // In-memory path: create new DataFile, append all rows, swap
            let mut new_data_file = DataFile::open_in_memory()?;
            let mut new_rat = RecordAddressTable::new();

            for row in &active_rows {
                let (offset, length) = new_data_file.append_row(row)?;
                new_rat.insert(row.row_id, offset, length)?;
            }

            self.data_file = new_data_file;
            self.rat = new_rat;
        } else {
            #[cfg(not(target_arch = "wasm32"))]
            {
                // Disk path: write to tmp file, atomic rename, reopen
                let tmp_path = self.table_dir.join("data.bin.tmp");
                let data_path = self.table_dir.join("data.bin");

                let mut tmp_file = DataFile::open(&tmp_path, true)?;
                let mut new_rat = RecordAddressTable::new();

                for row in &active_rows {
                    let (offset, length) = tmp_file.append_row(row)?;
                    new_rat.insert(row.row_id, offset, length)?;
                }
                tmp_file.sync()?;

                // Atomic rename
                std::fs::rename(&tmp_path, &data_path)?;

                // Reopen the data file
                self.data_file = DataFile::open_with_group_commit(
                    &data_path,
                    self.config.fsync_on_write,
                    self.config.group_commit_interval_ms,
                )?;
                self.rat = new_rat;
            }
        }

        // Rebuild all indices from the compacted rows
        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            for col in self.index_manager.indexed_columns().to_vec() {
                self.index_manager.rebuild_index(&col, &active_rows, &mapping)?;
            }
        }

        // Persist RAT
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

        self.next_row_id = max_row_id + 1;

        self.flush()?;

        Ok(())
    }

    /// Greater than search using an index
    pub fn greater_than_by_index(
        &mut self,
        column_name: &str,
        value: &Value,
        inclusive: bool,
    ) -> Result<Vec<Row>> {
        let row_ids = self.index_manager.greater_than(column_name, value, inclusive)?;
        self.get_by_ids(&row_ids)
    }

    /// Less than search using an index
    pub fn less_than_by_index(
        &mut self,
        column_name: &str,
        value: &Value,
        inclusive: bool,
    ) -> Result<Vec<Row>> {
        let row_ids = self.index_manager.less_than(column_name, value, inclusive)?;
        self.get_by_ids(&row_ids)
    }

    /// Prefix search using an index
    pub fn prefix_search_by_index(&mut self, column_name: &str, prefix: &str) -> Result<Vec<Row>> {
        let row_ids = self.index_manager.prefix_search(column_name, prefix)?;
        self.get_by_ids(&row_ids)
    }

    /// Search for rows matching a value using an index
    pub fn search_by_index(&mut self, column_name: &str, value: &Value) -> Result<Vec<Row>> {
        let row_ids = self.index_manager.search(column_name, value)?;
        self.get_by_ids(&row_ids)
    }

    /// Range search using an index
    pub fn range_search_by_index(
        &mut self,
        column_name: &str,
        start_value: &Value,
        end_value: &Value,
    ) -> Result<Vec<Row>> {
        let row_ids = self.index_manager.range_query(column_name, start_value, end_value)?;
        self.get_by_ids(&row_ids)
    }

    /// Build a column name -> position mapping from the schema (cached)
    pub fn build_column_mapping(&mut self) -> Arc<HashMap<String, usize>> {
        if let Some(ref cached) = self.column_mapping_cache {
            return Arc::clone(cached);
        }
        let mut mapping = HashMap::new();
        if let Some(schema) = &self.schema {
            for (i, col) in schema.columns.iter().enumerate() {
                mapping.insert(col.name.clone(), i);
            }
        }
        let arc = Arc::new(mapping);
        self.column_mapping_cache = Some(Arc::clone(&arc));
        arc
    }

    /// Create an index on a column and backfill it from existing rows
    ///
    /// Unlike calling `index_manager_mut().create_index()` directly, this
    /// method populates the new index with all existing rows so that indexed
    /// queries return correct results immediately.
    pub fn create_index(&mut self, column_name: &str) -> Result<()> {
        self.index_manager.create_index(column_name)?;

        let rows = self.scan_all()?;
        if !rows.is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.rebuild_index(column_name, &rows, &mapping)?;
        }

        Ok(())
    }

    /// Get index manager
    pub fn index_manager(&self) -> &IndexManager {
        &self.index_manager
    }

    /// Get index manager (mutable)
    pub fn index_manager_mut(&mut self) -> &mut IndexManager {
        &mut self.index_manager
    }

    /// Get table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Save in-memory table to disk
    pub fn save_to_disk<P: AsRef<Path>>(&mut self, base_dir: P) -> Result<()> {
        let base_dir = base_dir.as_ref();
        
        // Create table directory if it doesn't exist
        let table_dir = base_dir.join(&self.name);
        if ! table_dir.exists() {
            std::fs::create_dir_all(&table_dir)?;
            std::fs::create_dir_all(table_dir.join("indices"))?;
        }

        // Create a temporary disk-based table engine to perform the save
        let mut disk_config = self.config.clone();
        disk_config.in_memory = false;
        disk_config.data_dir = base_dir.to_string_lossy().to_string();

        // We can't easily use TableEngine::create here because it might try to open if it exists
        // and we want to overwrite/update it.
        
        // 1. Save Schema
        if let Some(schema) = &self.schema {
            let schema_path = table_dir.join("schema.json");
            let content = serde_json::to_string_pretty(schema)?;
            std::fs::write(schema_path, content)?;
        }

        // 2. Save Data (simplest is to just overwrite data.bin and rat.bin)
        let data_path = table_dir.join("data.bin");
        let mut disk_data_file = DataFile::open(&data_path, true)?;
        
        // We need to clear it if it exists
        if data_path.exists() {
            std::fs::write(&data_path, [])?; // Truncate
            disk_data_file = DataFile::open(&data_path, true)?;
        }

        let mut disk_rat = RecordAddressTable::new();
        let rows = self.scan_all()?;
        for row in rows {
            let (offset, length) = disk_data_file.append_row(&row)?;
            disk_rat.insert(row.row_id, offset, length)?;
        }

        // 3. Save RAT
        let rat_path = table_dir.join("rat.bin");
        disk_rat.save(rat_path)?;

        // 4. Save Indices
        let index_dir = table_dir.join("indices");
        self.index_manager.save_to(index_dir)?;

        Ok(())
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
            in_memory: false,
            group_commit_interval_ms: 0,
            compaction_threshold: 0.5,
            auto_compact: false,
        };

        TableEngine::create(name, &base_dir, config, 100).unwrap()
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
            Value::varchar("Alice".to_string()),
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
            .insert_row(vec![Value::Int32(1), Value::varchar("A".to_string())])
            .unwrap();
        let row_id2 = table
            .insert_row(vec![Value::Int32(2), Value::varchar("B".to_string())])
            .unwrap();
        let row_id3 = table
            .insert_row(vec![Value::Int32(3), Value::varchar("C".to_string())])
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
            vec![Value::Int32(1), Value::varchar("A".to_string())],
            vec![Value::Int32(2), Value::varchar("B".to_string())],
            vec![Value::Int32(3), Value::varchar("C".to_string())],
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
                .insert_row(vec![Value::Int32(i), Value::varchar(format!("row_{}", i))])
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
            in_memory: false,
            group_commit_interval_ms: 0,
            compaction_threshold: 0.5,
            auto_compact: false,
        };

        // Create table and insert data
        {
            let mut table = TableEngine::create("users", base_dir, config.clone(), 100).unwrap();

            for i in 1..=5 {
                table
                    .insert_row(vec![
                        Value::Int64(i),
                        Value::varchar(format!("user_{}", i)),
                    ])
                    .unwrap();
            }

            table.delete_by_id(3).unwrap();
            table.flush().unwrap();
        }

        // Reopen and verify
        {
            let mut table = TableEngine::open("users", base_dir, config, 100).unwrap();

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
            in_memory: false,
            group_commit_interval_ms: 0,
            compaction_threshold: 0.5,
            auto_compact: false,
        };

        // Create table and insert data
        {
            let mut table = TableEngine::create("test", base_dir, config.clone(), 100).unwrap();

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
            let mut table = TableEngine::open("test", base_dir, config, 100).unwrap();

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

    #[test]
    fn test_full_compact_disk() {
        let mut table = create_test_table("test_full_compact");

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i), Value::varchar(format!("row_{}", i))]).unwrap();
        }

        // Delete half the rows
        for i in (1..=10).step_by(2) {
            table.delete_by_id(i).unwrap();
        }

        let size_before = table.stats().data_file_size;
        assert_eq!(table.stats().active_rows, 5);

        table.full_compact().unwrap();

        // After compaction: data file should be smaller, all active rows intact
        assert!(table.stats().data_file_size < size_before);
        assert_eq!(table.stats().active_rows, 5);
        assert_eq!(table.stats().total_rows, 5);

        // Verify rows are still readable
        for i in (2..=10).step_by(2) {
            let row = table.get_by_id(i).unwrap();
            assert!(row.is_some(), "Row {} should exist", i);
        }
    }

    #[test]
    fn test_full_compact_in_memory() {
        let config = StorageConfig {
            data_dir: ":memory:".to_string(),
            fsync_on_write: false,
            fsync_interval_ms: 1000,
            max_data_file_size_mb: 1024,
            in_memory: true,
            group_commit_interval_ms: 0,
            compaction_threshold: 0.5,
            auto_compact: false,
        };

        let mut table = TableEngine::open_in_memory("test_mem_compact", config, 100).unwrap();

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        for i in 1..=5 {
            table.delete_by_id(i).unwrap();
        }

        table.full_compact().unwrap();

        assert_eq!(table.stats().active_rows, 5);
        assert_eq!(table.stats().total_rows, 5);

        for i in 6..=10 {
            assert!(table.get_by_id(i).unwrap().is_some());
        }
    }

    #[test]
    fn test_auto_compact_triggers() {
        let base_dir = "/tmp/thunderdb_test_auto_compact";
        let _ = fs::remove_dir_all(base_dir);

        let config = StorageConfig {
            data_dir: base_dir.to_string(),
            fsync_on_write: false,
            fsync_interval_ms: 1000,
            max_data_file_size_mb: 1024,
            in_memory: false,
            group_commit_interval_ms: 0,
            compaction_threshold: 0.5,
            auto_compact: true,
        };

        let mut table = TableEngine::create("test", base_dir, config, 100).unwrap();

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        // Delete 5 of 10 → 50% dead → should trigger auto-compact
        for i in 1..=5 {
            table.delete_by_id(i).unwrap();
        }

        // After auto-compact, RAT should have only 5 entries
        assert_eq!(table.stats().total_rows, 5);
        assert_eq!(table.stats().active_rows, 5);

        fs::remove_dir_all(base_dir).ok();
    }

    #[test]
    fn test_auto_compact_disabled() {
        let mut table = create_test_table("test_auto_compact_off");

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        // Delete 8 of 10 → 80% dead, but auto_compact is false
        for i in 1..=8 {
            table.delete_by_id(i).unwrap();
        }

        // RAT should still have all 10 entries (not compacted)
        assert_eq!(table.stats().total_rows, 10);
        assert_eq!(table.stats().active_rows, 2);
    }

    #[test]
    fn test_full_compact_with_indices() {
        let mut table = create_test_table("test_compact_idx");

        table.set_schema(TableSchema {
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
                ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
            ],
        }).unwrap();

        table.index_manager_mut().create_index("id").unwrap();

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i), Value::varchar(format!("n{}", i))]).unwrap();
        }

        // Rebuild index after inserts since insert_row doesn't auto-index without mapping
        let rows = table.scan_all().unwrap();
        let mut mapping = HashMap::new();
        mapping.insert("id".to_string(), 0);
        mapping.insert("name".to_string(), 1);
        table.index_manager_mut().rebuild_index("id", &rows, &mapping).unwrap();

        for i in 1..=5 {
            table.delete_by_id(i).unwrap();
        }

        table.full_compact().unwrap();

        // Verify index still works after compaction
        let results = table.index_manager().search("id", &Value::Int32(7)).unwrap();
        assert_eq!(results.len(), 1);

        // Row data is correct
        let row = table.get_by_id(results[0]).unwrap().unwrap();
        assert_eq!(row.values[0], Value::Int32(7));
    }
}
