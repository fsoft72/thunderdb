// Index manager - Step 2.3
//
// Manages multiple indices per table and coordinates index updates

use crate::error::{Error, Result};
use crate::index::BTree;
use crate::index::stats::IndexStatistics;
#[cfg(not(target_arch = "wasm32"))]
use crate::index::{load_index, save_index};
use crate::storage::{Row, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Manages multiple B-Tree indices for a single table
///
/// Each index is associated with a column and provides fast lookups
pub struct IndexManager {
    /// Table name
    table_name: String,

    /// Directory where index files are stored
    #[allow(dead_code)]
    index_dir: PathBuf,

    /// Active indices: column_name -> BTree
    indices: HashMap<String, BTree<Value, u64>>,

    /// Column names that have indices
    indexed_columns: Vec<String>,

    /// B-Tree order for new indices
    btree_order: usize,

    /// Cached statistics per indexed column
    stats_cache: HashMap<String, IndexStatistics>,
}

impl IndexManager {
    /// Create a new index manager
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `index_dir` - Directory for storing .idx files
    /// * `btree_order` - Order for B-Tree indices
    pub fn new<P: AsRef<Path>>(table_name: &str, index_dir: P, btree_order: usize) -> Result<Self> {
        let index_dir = index_dir.as_ref().to_path_buf();

        // Create index directory if it doesn't exist
        #[cfg(not(target_arch = "wasm32"))]
        {
            if ! index_dir.exists() {
                std::fs::create_dir_all(&index_dir)?;
            }
        }

        Ok(Self {
            table_name: table_name.to_string(),
            index_dir,
            indices: HashMap::new(),
            indexed_columns: Vec::new(),
            btree_order,
            stats_cache: HashMap::new(),
        })
    }

    /// Create an index on a column
    ///
    /// # Arguments
    /// * `column_name` - Name of the column to index
    pub fn create_index(&mut self, column_name: &str) -> Result<()> {
        if self.indices.contains_key(column_name) {
            return Err(Error::Index(format!(
                "Index already exists on column: {}",
                column_name
            )));
        }

        let tree = BTree::new(self.btree_order)?;
        self.indices.insert(column_name.to_string(), tree);
        self.indexed_columns.push(column_name.to_string());
        self.stats_cache.insert(column_name.to_string(), IndexStatistics::empty());

        Ok(())
    }

    /// Drop an index
    ///
    /// # Arguments
    /// * `column_name` - Name of the indexed column
    pub fn drop_index(&mut self, column_name: &str) -> Result<()> {
        if ! self.indices.contains_key(column_name) {
            return Err(Error::Index(format!(
                "No index exists on column: {}",
                column_name
            )));
        }

        self.indices.remove(column_name);
        self.indexed_columns.retain(|col| col != column_name);
        self.stats_cache.remove(column_name);

        // Remove index file
        #[cfg(not(target_arch = "wasm32"))]
        {
            let index_path = self.get_index_path(column_name);
            if index_path.exists() {
                std::fs::remove_file(index_path)?;
            }
        }

        Ok(())
    }

    /// Insert a row into all relevant indices
    ///
    /// # Arguments
    /// * `row` - Row to index
    /// * `column_mapping` - Maps column names to positions in row.values
    pub fn insert_row(&mut self, row: &Row, column_mapping: &HashMap<String, usize>) -> Result<()> {
        for column_name in &self.indexed_columns {
            if let Some(&col_idx) = column_mapping.get(column_name) {
                if let Some(value) = row.values.get(col_idx) {
                    if let Some(index) = self.indices.get_mut(column_name) {
                        index.insert(value.clone(), row.row_id)?;
                    }
                    if let Some(stats) = self.stats_cache.get_mut(column_name) {
                        stats.record_insert(value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Insert multiple rows into all relevant indices
    ///
    /// Batches entries per column, sorts them, and inserts in sorted order
    /// for better B-Tree cache locality.
    pub fn insert_rows_batch(
        &mut self,
        rows: &[Row],
        column_mapping: &HashMap<String, usize>,
    ) -> Result<()> {
        for column_name in &self.indexed_columns {
            if let Some(&col_idx) = column_mapping.get(column_name) {
                // Collect all (value, row_id) pairs for this column
                let mut entries: Vec<(Value, u64)> = Vec::with_capacity(rows.len());
                for row in rows {
                    if let Some(value) = row.values.get(col_idx) {
                        entries.push((value.clone(), row.row_id));
                    }
                }

                // Sort by key for better B-Tree insertion locality
                entries.sort_by(|a, b| a.0.cmp(&b.0));

                if let Some(index) = self.indices.get_mut(column_name) {
                    for (value, row_id) in &entries {
                        index.insert(value.clone(), *row_id)?;
                    }
                }

                if let Some(stats) = self.stats_cache.get_mut(column_name) {
                    for (value, _) in &entries {
                        stats.record_insert(value);
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove a row from all indices
    ///
    /// For each indexed column, extracts the value from the row's values
    /// and calls btree.delete(value, row_id) to remove the entry.
    ///
    /// # Arguments
    /// * `row_id` - Row ID to remove
    /// * `values` - The row's column values
    /// * `column_mapping` - Maps column names to positions in values
    pub fn delete_row(
        &mut self,
        row_id: u64,
        values: &[Value],
        column_mapping: &HashMap<String, usize>,
    ) -> Result<()> {
        for column_name in &self.indexed_columns {
            if let Some(&col_idx) = column_mapping.get(column_name) {
                if let Some(value) = values.get(col_idx) {
                    if let Some(index) = self.indices.get_mut(column_name) {
                        index.delete(value, &row_id);
                    }
                    if let Some(stats) = self.stats_cache.get_mut(column_name) {
                        stats.record_delete();
                    }
                }
            }
        }
        Ok(())
    }

    /// Search for rows where column > value or column >= value
    pub fn greater_than(&self, column_name: &str, value: &Value, inclusive: bool) -> Result<Vec<u64>> {
        if let Some(index) = self.indices.get(column_name) {
            let results = index.scan_from(value);
            let mut row_ids = Vec::new();
            for (key, row_id) in results {
                if inclusive || key.cmp(value) > std::cmp::Ordering::Equal {
                    row_ids.push(row_id);
                }
            }
            Ok(row_ids)
        } else {
            Err(Error::Index(format!("No index on column: {}", column_name)))
        }
    }

    /// Search for rows where column < value or column <= value
    pub fn less_than(&self, column_name: &str, value: &Value, inclusive: bool) -> Result<Vec<u64>> {
        if let Some(index) = self.indices.get(column_name) {
            let results = index.scan_to(value);
            let mut row_ids = Vec::new();
            for (key, row_id) in results {
                if inclusive || key.cmp(value) < std::cmp::Ordering::Equal {
                    row_ids.push(row_id);
                }
            }
            Ok(row_ids)
        } else {
            Err(Error::Index(format!("No index on column: {}", column_name)))
        }
    }

    /// Search for rows matching a prefix (LIKE 'abc%')
    pub fn prefix_search(&self, column_name: &str, prefix: &str) -> Result<Vec<u64>> {
        use crate::index::LikePattern;
        let pattern = LikePattern::Prefix(prefix.to_string());
        if let Some((start, end)) = pattern.get_range_bounds() {
            let start_val = Value::varchar(start);
            let end_val = Value::varchar(end);
            if let Some(index) = self.indices.get(column_name) {
                let results = index.range_scan(&start_val, &end_val);
                // Filter out the 'end' boundary which is exclusive for prefix matches
                Ok(results.into_iter()
                    .filter(|(k, _)| k.cmp(&end_val) < std::cmp::Ordering::Equal)
                    .map(|(_, row_id)| row_id)
                    .collect())
            } else {
                Err(Error::Index(format!("No index on column: {}", column_name)))
            }
        } else {
            Ok(Vec::new())
        }
    }

    /// Search for rows matching a value in a specific column
    ///
    /// # Arguments
    /// * `column_name` - Column to search
    /// * `value` - Value to find
    ///
    /// # Returns
    /// Vector of row IDs
    pub fn search(&self, column_name: &str, value: &Value) -> Result<Vec<u64>> {
        if let Some(index) = self.indices.get(column_name) {
            Ok(index.search(value))
        } else {
            Err(Error::Index(format!(
                "No index on column: {}",
                column_name
            )))
        }
    }

    /// Range query on an indexed column
    ///
    /// # Arguments
    /// * `column_name` - Column to query
    /// * `start_value` - Start of range (inclusive)
    /// * `end_value` - End of range (inclusive)
    ///
    /// # Returns
    /// Vector of row IDs in sorted order
    pub fn range_query(
        &self,
        column_name: &str,
        start_value: &Value,
        end_value: &Value,
    ) -> Result<Vec<u64>> {
        if let Some(index) = self.indices.get(column_name) {
            let results = index.range_scan(start_value, end_value);
            Ok(results.into_iter().map(|(_, row_id)| row_id).collect())
        } else {
            Err(Error::Index(format!(
                "No index on column: {}",
                column_name
            )))
        }
    }

    /// Check if a column is indexed
    pub fn has_index(&self, column_name: &str) -> bool {
        self.indices.contains_key(column_name)
    }

    /// Get all indexed columns
    pub fn indexed_columns(&self) -> &[String] {
        &self.indexed_columns
    }

    /// Save all indices to disk
    pub fn flush(&self) -> Result<()> {
        self.save_to(&self.index_dir)
    }

    /// Save all indices to a specific directory
    pub fn save_to<P: AsRef<Path>>(&self, index_dir: P) -> Result<()> {
        let index_dir = index_dir.as_ref();
        
        #[cfg(not(target_arch = "wasm32"))]
        {
            if ! index_dir.exists() {
                std::fs::create_dir_all(index_dir)?;
            }

            for (column_name, index) in &self.indices {
                let path = index_dir.join(format!("{}_{}.idx", self.table_name, column_name));
                save_index(index, &path)?;
            }
        }
        Ok(())
    }

    /// Load all indices from disk by scanning the index directory
    pub fn load(&mut self) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if ! self.index_dir.exists() {
                return Ok(());
            }

            let prefix = format!("{}_", self.table_name);
            let entries = std::fs::read_dir(&self.index_dir)?;

            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                    if filename.starts_with(&prefix) && filename.ends_with(".idx") {
                        // Extract column name: {table_name}_{column_name}.idx
                        let col_part = &filename[prefix.len()..filename.len() - 4];
                        let column_name = col_part.to_string();

                        if ! self.indices.contains_key(&column_name) {
                            let index = load_index(&path)?;
                            let stats = IndexStatistics::from_btree(&index);
                            self.indices.insert(column_name.clone(), index);
                            self.stats_cache.insert(column_name.clone(), stats);
                            if ! self.indexed_columns.contains(&column_name) {
                                self.indexed_columns.push(column_name);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Rebuild an index from rows
    ///
    /// # Arguments
    /// * `column_name` - Column to rebuild index for
    /// * `rows` - All rows in the table
    /// * `column_mapping` - Maps column names to positions
    pub fn rebuild_index(
        &mut self,
        column_name: &str,
        rows: &[Row],
        column_mapping: &HashMap<String, usize>,
    ) -> Result<()> {
        // Create new index
        let mut index = BTree::new(self.btree_order)?;

        // Get column position
        let col_idx = column_mapping.get(column_name).ok_or_else(|| {
            Error::Index(format!("Column not found in mapping: {}", column_name))
        })?;

        // Index all rows
        for row in rows {
            if let Some(value) = row.values.get(*col_idx) {
                index.insert(value.clone(), row.row_id)?;
            }
        }

        // Recompute stats from rebuilt index
        let stats = IndexStatistics::from_btree(&index);

        // Replace old index
        self.indices.insert(column_name.to_string(), index);
        self.stats_cache.insert(column_name.to_string(), stats);

        Ok(())
    }

    /// Query row IDs from an index using an operator
    ///
    /// Returns None if the column is not indexed or the operator is not supported.
    pub fn query_row_ids(&self, column: &str, operator: &crate::query::Operator) -> Option<Vec<u64>> {
        if !self.indices.contains_key(column) {
            return None;
        }

        use crate::query::Operator;
        match operator {
            Operator::Equals(val) => self.search(column, val).ok(),
            Operator::Between(start, end) => self.range_query(column, start, end).ok(),
            Operator::GreaterThan(val) => self.greater_than(column, val, false).ok(),
            Operator::GreaterThanOrEqual(val) => self.greater_than(column, val, true).ok(),
            Operator::LessThan(val) => self.less_than(column, val, false).ok(),
            Operator::LessThanOrEqual(val) => self.less_than(column, val, true).ok(),
            Operator::Like(pattern) => {
                use crate::index::LikePattern;
                if let Ok(lp) = LikePattern::parse(pattern) {
                    if let Some(prefix) = lp.get_prefix() {
                        self.prefix_search(column, prefix).ok()
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            Operator::In(values) => {
                let index = self.indices.get(column)?;
                let mut all_ids = Vec::new();
                for val in values {
                    all_ids.extend(index.search(val));
                }
                all_ids.sort_unstable();
                all_ids.dedup();
                Some(all_ids)
            }
            _ => None,
        }
    }

    /// Get cached statistics for a specific column's index
    pub fn column_stats(&self, col: &str) -> Option<&IndexStatistics> {
        self.stats_cache.get(col)
    }

    /// Get all cached index statistics
    pub fn all_stats(&self) -> &HashMap<String, IndexStatistics> {
        &self.stats_cache
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexManagerStats {
        let mut total_keys = 0;
        let mut index_details = HashMap::new();

        for (column_name, index) in &self.indices {
            let tree_stats = index.stats();
            total_keys += tree_stats.total_keys;
            index_details.insert(
                column_name.clone(),
                IndexInfo {
                    keys: tree_stats.total_keys,
                    height: tree_stats.height,
                    nodes: tree_stats.node_count,
                },
            );
        }

        IndexManagerStats {
            table_name: self.table_name.clone(),
            index_count: self.indices.len(),
            total_keys,
            index_details,
        }
    }

    /// Get path for an index file
    #[allow(dead_code)]
    fn get_index_path(&self, column_name: &str) -> PathBuf {
        self.index_dir
            .join(format!("{}_{}.idx", self.table_name, column_name))
    }
}

/// Statistics for the index manager
#[derive(Debug, Clone)]
pub struct IndexManagerStats {
    pub table_name: String,
    pub index_count: usize,
    pub total_keys: usize,
    pub index_details: HashMap<String, IndexInfo>,
}

/// Information about a single index
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub keys: usize,
    pub height: usize,
    pub nodes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_manager(name: &str) -> IndexManager {
        let dir = format!("/tmp/thunderdb_idx_test_{}", name);
        let _ = fs::remove_dir_all(&dir);
        IndexManager::new("test_table", &dir, 5).unwrap()
    }

    fn create_test_row(row_id: u64, id_val: i32, name: &str, age: i32) -> Row {
        Row::new(
            row_id,
            vec![
                Value::Int32(id_val),
                Value::varchar(name.to_string()),
                Value::Int32(age),
            ],
        )
    }

    fn create_column_mapping() -> HashMap<String, usize> {
        let mut mapping = HashMap::new();
        mapping.insert("id".to_string(), 0);
        mapping.insert("name".to_string(), 1);
        mapping.insert("age".to_string(), 2);
        mapping
    }

    #[test]
    fn test_create_index() {
        let mut mgr = create_test_manager("create");

        mgr.create_index("id").unwrap();
        assert!(mgr.has_index("id"));
        assert!(!mgr.has_index("name"));

        // Try creating duplicate
        assert!(mgr.create_index("id").is_err());
    }

    #[test]
    fn test_drop_index() {
        let mut mgr = create_test_manager("drop");

        mgr.create_index("id").unwrap();
        assert!(mgr.has_index("id"));

        mgr.drop_index("id").unwrap();
        assert!(!mgr.has_index("id"));

        // Try dropping non-existent
        assert!(mgr.drop_index("id").is_err());
    }

    #[test]
    fn test_insert_and_search() {
        let mut mgr = create_test_manager("insert");
        let mapping = create_column_mapping();

        mgr.create_index("id").unwrap();

        let row1 = create_test_row(1, 100, "Alice", 25);
        let row2 = create_test_row(2, 200, "Bob", 30);

        mgr.insert_row(&row1, &mapping).unwrap();
        mgr.insert_row(&row2, &mapping).unwrap();

        let results = mgr.search("id", &Value::Int32(100)).unwrap();
        assert_eq!(results, vec![1]);

        let results = mgr.search("id", &Value::Int32(200)).unwrap();
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn test_multiple_indices() {
        let mut mgr = create_test_manager("multiple");
        let mapping = create_column_mapping();

        mgr.create_index("id").unwrap();
        mgr.create_index("age").unwrap();

        let row = create_test_row(1, 100, "Alice", 25);
        mgr.insert_row(&row, &mapping).unwrap();

        assert_eq!(mgr.search("id", &Value::Int32(100)).unwrap(), vec![1]);
        assert_eq!(mgr.search("age", &Value::Int32(25)).unwrap(), vec![1]);
    }

    #[test]
    fn test_range_query() {
        let mut mgr = create_test_manager("range");
        let mapping = create_column_mapping();

        mgr.create_index("age").unwrap();

        for i in 1..=10 {
            let row = create_test_row(i, i as i32 * 10, "User", i as i32 * 5);
            mgr.insert_row(&row, &mapping).unwrap();
        }

        // Query ages 10-30 (rows 2-6)
        let results = mgr
            .range_query("age", &Value::Int32(10), &Value::Int32(30))
            .unwrap();

        assert_eq!(results, vec![2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_rebuild_index() {
        let mut mgr = create_test_manager("rebuild");
        let mapping = create_column_mapping();

        let rows = vec![
            create_test_row(1, 100, "Alice", 25),
            create_test_row(2, 200, "Bob", 30),
            create_test_row(3, 300, "Charlie", 35),
        ];

        mgr.create_index("id").unwrap();
        mgr.rebuild_index("id", &rows, &mapping).unwrap();

        assert_eq!(mgr.search("id", &Value::Int32(100)).unwrap(), vec![1]);
        assert_eq!(mgr.search("id", &Value::Int32(200)).unwrap(), vec![2]);
        assert_eq!(mgr.search("id", &Value::Int32(300)).unwrap(), vec![3]);
    }

    #[test]
    fn test_persistence() {
        let dir = "/tmp/thunderdb_idx_test_persist";
        let _ = fs::remove_dir_all(dir);

        let mapping = create_column_mapping();

        // Create and populate
        {
            let mut mgr = IndexManager::new("users", dir, 5).unwrap();
            mgr.create_index("id").unwrap();

            for i in 1..=5 {
                let row = create_test_row(i, i as i32 * 10, "User", 20);
                mgr.insert_row(&row, &mapping).unwrap();
            }

            mgr.flush().unwrap();
        }

        // Reload
        {
            let mut mgr = IndexManager::new("users", dir, 5).unwrap();
            mgr.indexed_columns.push("id".to_string());
            mgr.load().unwrap();

            assert_eq!(mgr.search("id", &Value::Int32(30)).unwrap(), vec![3]);
        }

        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn test_stats() {
        let mut mgr = create_test_manager("stats");
        let mapping = create_column_mapping();

        mgr.create_index("id").unwrap();
        mgr.create_index("age").unwrap();

        for i in 1..=10 {
            let row = create_test_row(i, i as i32, "User", i as i32);
            mgr.insert_row(&row, &mapping).unwrap();
        }

        let stats = mgr.stats();
        assert_eq!(stats.index_count, 2);
        assert_eq!(stats.total_keys, 20); // 10 keys in each index
    }

    #[test]
    fn test_indexed_columns() {
        let mut mgr = create_test_manager("columns");

        mgr.create_index("id").unwrap();
        mgr.create_index("age").unwrap();

        let cols = mgr.indexed_columns();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"age".to_string()));
    }
}
