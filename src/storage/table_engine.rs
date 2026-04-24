use crate::config::StorageConfig;
use crate::error::Result;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::Error;
use crate::storage::{Row, Value};
use crate::storage::paged_table::PagedTable;
use crate::storage::page::Ctid;
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
/// - pages.bin (slotted page storage with TOAST support)
/// - schema.json (Table metadata)
/// - indices/ directory
pub struct TableEngine {
    name: String,
    table_dir: PathBuf,
    paged_table: PagedTable,
    index_manager: IndexManager,
    schema: Option<TableSchema>,
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
    /// * `btree_order` - B-tree order for index manager
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

        // Open paged table
        let pages_path = table_dir.join("pages.bin");
        let paged_table = PagedTable::open(&pages_path)?;

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

        Ok(Self {
            name: name.to_string(),
            table_dir,
            paged_table,
            index_manager,
            schema,
            config,
            column_mapping_cache: None,
        })
    }

    /// Open an in-memory table backed by a temporary file
    pub fn open_in_memory(name: &str, config: StorageConfig, btree_order: usize) -> Result<Self> {
        let tmp_dir = std::env::temp_dir().join(format!("thunderdb_mem_{}", name));
        std::fs::create_dir_all(&tmp_dir).ok();
        let pages_path = tmp_dir.join("pages.bin");
        let paged_table = PagedTable::open(&pages_path)?;

        let index_manager = IndexManager::new(name, ":memory:", btree_order)?;

        Ok(Self {
            name: name.to_string(),
            table_dir: PathBuf::from(":memory:"),
            paged_table,
            index_manager,
            schema: None,
            config,
            column_mapping_cache: None,
        })
    }

    /// Load a table from disk into memory
    ///
    /// Opens the disk-based table, copies all rows and indices into a
    /// new in-memory table instance.
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
        let rows = disk_table.scan_all()?;
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
    /// * `btree_order` - B-tree order for index manager
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
    /// Inserts values into the paged table and updates indices.
    /// Returns the row_id derived from the ctid.
    ///
    /// # Arguments
    /// * `values` - Column values for the row
    ///
    /// # Returns
    /// The row_id (ctid packed as u64)
    pub fn insert_row(&mut self, values: Vec<Value>) -> Result<u64> {
        let ctid = self.paged_table.insert_row(&values)?;
        let row_id = ctid.to_u64();

        if !self.index_manager.indexed_columns().is_empty() {
            let row = Row::new(row_id, values);
            let mapping = self.build_column_mapping();
            self.index_manager.insert_row(&row, &mapping)?;
        }

        Ok(row_id)
    }

    /// Insert multiple rows in batch
    ///
    /// Uses PagedTable::insert_batch for efficient page-level batching
    /// (single read-write per page instead of per row), then batches
    /// index updates.
    ///
    /// # Arguments
    /// * `rows` - Vector of value vectors to insert
    ///
    /// # Returns
    /// Vector of row_ids (ctids packed as u64)
    pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) -> Result<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let ctids = self.paged_table.insert_batch(&rows)?;
        let row_ids: Vec<u64> = ctids.iter().map(|c| c.to_u64()).collect();

        if !self.index_manager.indexed_columns().is_empty() {
            let row_objects: Vec<Row> = row_ids
                .iter()
                .zip(rows.into_iter())
                .map(|(&id, values)| Row::new(id, values))
                .collect();
            let mapping = self.build_column_mapping();
            self.index_manager.insert_rows_batch(&row_objects, &mapping)?;
        }

        Ok(row_ids)
    }

    /// Get a row by ID
    ///
    /// # Arguments
    /// * `row_id` - Row ID (ctid packed as u64) to retrieve
    ///
    /// # Returns
    /// The row if found and not deleted, None otherwise
    pub fn get_by_id(&mut self, row_id: u64) -> Result<Option<Row>> {
        let ctid = Ctid::from_u64(row_id);
        self.paged_table.get_row(ctid)
    }

    /// Update a row by ID
    ///
    /// Reads old row to remove stale index entries, then deletes old
    /// and inserts new values via PagedTable.
    ///
    /// # Arguments
    /// * `row_id` - Row ID (ctid packed as u64) to update
    /// * `values` - New column values
    ///
    /// # Returns
    /// true if row was found and updated, false otherwise
    pub fn update_row(&mut self, row_id: u64, values: Vec<Value>) -> Result<bool> {
        let old_ctid = Ctid::from_u64(row_id);

        let old_values = if !self.index_manager.indexed_columns().is_empty() {
            self.paged_table.get_row(old_ctid)?.map(|r| r.values)
        } else {
            None
        };

        let new_ctid = self.paged_table.update_row(old_ctid, &values)?;
        let new_row_id = new_ctid.to_u64();

        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            if let Some(old_vals) = old_values {
                self.index_manager.delete_row(row_id, &old_vals, &mapping)?;
            }
            let new_row = Row::new(new_row_id, values);
            self.index_manager.insert_row(&new_row, &mapping)?;
        }

        Ok(true)
    }

    /// Update a row, using caller-supplied old values for index maintenance.
    ///
    /// Identical to `update_row` but skips the redundant `get_row` read
    /// because the caller already holds the old row from a prior scan.
    /// This avoids triggering an mmap-remap + sync_all between the scan
    /// and the write when the table has indices.
    ///
    /// # Arguments
    /// * `row_id`     - Row ID (ctid packed as u64) to update
    /// * `old_values` - Previous column values (for index removal)
    /// * `values`     - New column values to write
    ///
    /// # Returns
    /// true if the row was updated
    pub(crate) fn _update_row_with_old(
        &mut self,
        row_id: u64,
        old_values: Vec<Value>,
        values: Vec<Value>,
    ) -> Result<bool> {
        let old_ctid = Ctid::from_u64(row_id);
        let new_ctid = self.paged_table.update_row(old_ctid, &values)?;
        let new_row_id = new_ctid.to_u64();

        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.delete_row(row_id, &old_values, &mapping)?;
            let new_row = Row::new(new_row_id, values);
            self.index_manager.insert_row(&new_row, &mapping)?;
        }

        Ok(true)
    }

    /// Delete a row, using caller-supplied old values for index maintenance.
    ///
    /// Identical to `delete_by_id` but skips the redundant `get_row` read
    /// because the caller already holds the old row from a prior scan.
    ///
    /// # Arguments
    /// * `row_id`     - Row ID (ctid packed as u64) to delete
    /// * `old_values` - Previous column values (for index removal)
    ///
    /// # Returns
    /// true if the row was deleted
    pub(crate) fn _delete_with_old_values(
        &mut self,
        row_id: u64,
        old_values: Vec<Value>,
    ) -> Result<bool> {
        let ctid = Ctid::from_u64(row_id);
        let deleted = self.paged_table.delete_row(ctid)?;
        if !deleted {
            return Ok(false);
        }

        if !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.delete_row(row_id, &old_values, &mapping)?;
        }

        Ok(true)
    }

    /// Delete multiple rows in batch.
    ///
    /// Uses `PagedTable::delete_batch` for page-level I/O efficiency, then
    /// removes all index entries via `IndexManager::delete_rows_batch`.
    ///
    /// # Arguments
    /// * `deletions` - (row_id, old_values) pairs; old_values are used for index cleanup
    ///
    /// # Returns
    /// Number of rows actually deleted
    pub fn delete_batch(&mut self, deletions: &[(u64, Vec<Value>)]) -> Result<usize> {
        if deletions.is_empty() {
            return Ok(0);
        }

        let ctids: Vec<Ctid> = deletions.iter()
            .map(|(row_id, _)| Ctid::from_u64(*row_id))
            .collect();

        let deleted = self.paged_table.delete_batch(&ctids)?;

        if deleted > 0 && !self.index_manager.indexed_columns().is_empty() {
            let mapping = self.build_column_mapping();
            self.index_manager.delete_rows_batch(deletions, &mapping)?;
        }

        Ok(deleted)
    }

    /// Delete a row by ID
    ///
    /// Reads the row before deleting so we can remove it from indices.
    ///
    /// # Arguments
    /// * `row_id` - Row ID (ctid packed as u64) to delete
    ///
    /// # Returns
    /// true if row was found and deleted, false otherwise
    pub fn delete_by_id(&mut self, row_id: u64) -> Result<bool> {
        let ctid = Ctid::from_u64(row_id);

        let old_values = if !self.index_manager.indexed_columns().is_empty() {
            self.paged_table.get_row(ctid)?.map(|r| r.values)
        } else {
            None
        };

        let deleted = self.paged_table.delete_row(ctid)?;
        if !deleted {
            return Ok(false);
        }

        if let Some(values) = old_values {
            let mapping = self.build_column_mapping();
            self.index_manager.delete_row(row_id, &values, &mapping)?;
        }

        Ok(true)
    }

    /// Get multiple rows by their IDs
    ///
    /// Groups reads by page_id for efficient I/O. Skips missing/deleted rows.
    pub fn get_by_ids(&mut self, row_ids: &[u64]) -> Result<Vec<Row>> {
        let ctids: Vec<Ctid> = row_ids.iter().map(|&id| Ctid::from_u64(id)).collect();
        self.paged_table.get_rows_by_ctids(&ctids)
    }

    /// Get multiple rows by ID, filtering on raw bytes before deserializing.
    ///
    /// Like `get_by_ids()` but applies a predicate to the raw row bytes.
    /// Only rows where `predicate` returns `true` are deserialized.
    pub fn get_by_ids_filtered<F>(
        &mut self,
        row_ids: &[u64],
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let ctids: Vec<Ctid> = row_ids.iter().map(|&id| Ctid::from_u64(id)).collect();
        self.paged_table.get_rows_by_ctids_filtered(&ctids, predicate)
    }

    /// Get the number of active rows without scanning (O(1))
    pub fn active_row_count(&self) -> usize {
        self.paged_table.active_row_count()
    }

    /// Scan all active rows
    ///
    /// Returns all non-deleted rows from the paged table.
    pub fn scan_all(&mut self) -> Result<Vec<Row>> {
        self.paged_table.scan_all()
    }

    /// Scan all active rows, returning only projected columns.
    pub fn scan_all_projected(&mut self, columns: &[usize]) -> Result<Vec<Row>> {
        self.paged_table.scan_all_projected(columns)
    }

    /// Get multiple rows by ID, returning only projected columns.
    pub fn get_by_ids_projected(&mut self, row_ids: &[u64], columns: &[usize]) -> Result<Vec<Row>> {
        let ctids: Vec<Ctid> = row_ids.iter().map(|&id| Ctid::from_u64(id)).collect();
        self.paged_table.get_rows_by_ctids_projected(&ctids, columns)
    }

    /// Scan active rows with an optional limit for early termination
    pub fn scan_all_limited(&mut self, limit: Option<usize>) -> Result<Vec<Row>> {
        let mut rows = self.paged_table.scan_all()?;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    /// Scan active rows with a callback predicate on raw bytes.
    ///
    /// The predicate receives the raw serialized row bytes (slot-level).
    /// Only rows where the predicate returns true are deserialized.
    pub fn scan_all_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.paged_table.scan_filtered(predicate)
    }

    /// Count active rows matching a predicate without deserializing.
    pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        self.paged_table.count_filtered(predicate)
    }

    /// Stream active rows through a callback with projected columns.
    /// Thin pass-through to `PagedTable::for_each_row_projected`.
    pub fn for_each_row_projected<F: FnMut(&[Value])>(
        &mut self,
        columns: &[usize],
        callback: F,
    ) -> Result<usize> {
        self.paged_table.for_each_row_projected(columns, callback)
    }

    /// Get mutable access to the underlying paged table.
    pub fn paged_table_mut(&mut self) -> &mut PagedTable {
        &mut self.paged_table
    }

    /// Get table statistics
    pub fn stats(&self) -> TableStats {
        TableStats {
            name: self.name.clone(),
            total_rows: self.paged_table.active_row_count(),
            active_rows: self.paged_table.active_row_count(),
            data_file_size: 0, // PageFile doesn't expose size yet
        }
    }

    /// Flush data to disk
    pub fn flush(&mut self) -> Result<()> {
        // PageFile uses mmap — no explicit flush needed
        Ok(())
    }

    /// Compact the table (no-op for page-based storage)
    pub fn compact(&mut self) -> Result<()> {
        // Page-based storage doesn't need RAT compaction
        Ok(())
    }

    /// Full compaction (no-op for page-based storage)
    pub fn full_compact(&mut self) -> Result<()> {
        // Page compaction is future work
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
    ///
    /// Scans all rows from the in-memory table and writes them into
    /// a new disk-based PagedTable.
    pub fn save_to_disk<P: AsRef<Path>>(&mut self, base_dir: P) -> Result<()> {
        let base_dir = base_dir.as_ref();

        // Create table directory if it doesn't exist
        let table_dir = base_dir.join(&self.name);
        if ! table_dir.exists() {
            std::fs::create_dir_all(&table_dir)?;
            std::fs::create_dir_all(table_dir.join("indices"))?;
        }

        // Save schema
        if let Some(schema) = &self.schema {
            let schema_path = table_dir.join("schema.json");
            let content = serde_json::to_string_pretty(schema)?;
            std::fs::write(schema_path, content)?;
        }

        // Save data: scan all rows from in-memory table, insert into disk-based PagedTable
        let pages_path = table_dir.join("pages.bin");
        let mut disk_table = PagedTable::open(&pages_path)?;
        let rows = self.scan_all()?;
        for row in rows {
            disk_table.insert_row(&row.values)?;
        }

        // Save indices
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

    /// Create a test table backed by disk in a temp directory
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
        let row = table.get_by_id(row_id).unwrap().unwrap();
        assert_eq!(row.row_id, row_id);
        assert_eq!(row.values, values);
    }

    #[test]
    fn test_table_insert_multiple() {
        let mut table = create_test_table("test_insert_multiple");

        let id1 = table
            .insert_row(vec![Value::Int32(1), Value::varchar("A".to_string())])
            .unwrap();
        let id2 = table
            .insert_row(vec![Value::Int32(2), Value::varchar("B".to_string())])
            .unwrap();
        let id3 = table
            .insert_row(vec![Value::Int32(3), Value::varchar("C".to_string())])
            .unwrap();

        // Verify all rows are retrievable
        assert!(table.get_by_id(id1).unwrap().is_some());
        assert!(table.get_by_id(id2).unwrap().is_some());
        assert!(table.get_by_id(id3).unwrap().is_some());
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
        assert_eq!(row_ids.len(), 3);
        assert_eq!(table.stats().active_rows, 3);

        // Verify each inserted row is retrievable
        for &id in &row_ids {
            assert!(table.get_by_id(id).unwrap().is_some());
        }
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

        let mut ids = Vec::new();
        for i in 1..=10 {
            let id = table
                .insert_row(vec![Value::Int32(i), Value::varchar(format!("row_{}", i))])
                .unwrap();
            ids.push(id);
        }

        let rows = table.scan_all().unwrap();
        assert_eq!(rows.len(), 10);

        // Delete some rows using actual row_ids
        table.delete_by_id(ids[2]).unwrap(); // was row 3
        table.delete_by_id(ids[6]).unwrap(); // was row 7

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

            let mut ids = Vec::new();
            for i in 1..=5 {
                let id = table
                    .insert_row(vec![
                        Value::Int64(i),
                        Value::varchar(format!("user_{}", i)),
                    ])
                    .unwrap();
                ids.push(id);
            }

            table.delete_by_id(ids[2]).unwrap(); // delete 3rd row
            table.flush().unwrap();
        }

        // Reopen and verify
        {
            let mut table = TableEngine::open("users", base_dir, config, 100).unwrap();

            assert_eq!(table.active_row_count(), 4);
            let rows = table.scan_all().unwrap();
            assert_eq!(rows.len(), 4);
        }

        fs::remove_dir_all(base_dir).ok();
    }

    #[test]
    fn test_table_compact_noop() {
        let mut table = create_test_table("test_compact_noop");

        for i in 1..=10 {
            table.insert_row(vec![Value::Int32(i)]).unwrap();
        }

        // compact is a no-op, should not error
        table.compact().unwrap();
        assert_eq!(table.stats().active_rows, 10);
    }

    #[test]
    fn test_table_stats() {
        let mut table = create_test_table("test_stats");

        let mut ids = Vec::new();
        for i in 1..=10 {
            let id = table.insert_row(vec![Value::Int32(i)]).unwrap();
            ids.push(id);
        }

        table.delete_by_id(ids[4]).unwrap();

        let stats = table.stats();
        assert_eq!(stats.name, "test_stats");
        assert_eq!(stats.active_rows, 9);
    }

    #[test]
    fn test_engine_delete_batch_no_index() {
        let mut table = create_test_table("te_delete_batch_noidx");
        let mut ids = Vec::new();
        for i in 0..10i32 {
            ids.push(table.insert_row(vec![Value::Int32(i)]).unwrap());
        }

        let deletions: Vec<(u64, Vec<Value>)> = ids[..5].iter()
            .enumerate()
            .map(|(i, &id)| (id, vec![Value::Int32(i as i32)]))
            .collect();
        let count = table.delete_batch(&deletions).unwrap();

        assert_eq!(count, 5);
        assert_eq!(table.active_row_count(), 5);
        for &id in &ids[..5] {
            assert!(table.get_by_id(id).unwrap().is_none());
        }
    }

    #[test]
    fn test_engine_delete_batch_with_index() {
        let mut table = create_test_table("te_delete_batch_idx");

        // Set schema so IndexManager can find columns by name
        table.set_schema(TableSchema {
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
                ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
            ],
        }).unwrap();
        table.create_index("id").unwrap();

        let mut ids = Vec::new();
        let mut values_map = Vec::new();
        for i in 0..5i32 {
            let vals = vec![Value::Int32(i), Value::varchar(format!("user_{}", i))];
            ids.push(table.insert_row(vals.clone()).unwrap());
            values_map.push(vals);
        }

        // Delete rows 1 and 3
        let deletions = vec![
            (ids[1], values_map[1].clone()),
            (ids[3], values_map[3].clone()),
        ];
        let count = table.delete_batch(&deletions).unwrap();
        assert_eq!(count, 2);
        assert_eq!(table.active_row_count(), 3);

        // Index must no longer return deleted rows
        let found = table.search_by_index("id", &Value::Int32(1)).unwrap();
        assert!(found.is_empty());
        let found = table.search_by_index("id", &Value::Int32(3)).unwrap();
        assert!(found.is_empty());

        // Surviving rows still in index
        let found = table.search_by_index("id", &Value::Int32(0)).unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn test_engine_delete_batch_empty() {
        let mut table = create_test_table("te_delete_batch_empty");
        let count = table.delete_batch(&[]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn for_each_row_projected_delegates_to_paged_table() {
        let mut table = create_test_table("for_each_delegate");
        table.insert_row(vec![Value::Int32(1), Value::varchar("a"), Value::Int32(10)]).unwrap();
        table.insert_row(vec![Value::Int32(2), Value::varchar("b"), Value::Int32(20)]).unwrap();

        let mut ids: Vec<i32> = Vec::new();
        let count = table.for_each_row_projected(&[0], |vals| {
            if let Value::Int32(n) = vals[0] { ids.push(n); }
        }).unwrap();

        assert_eq!(count, 2);
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }
}
