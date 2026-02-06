// ThunderDB - A custom database engine with minimal dependencies
//
// Features:
// - Append-only storage with lazy B-Tree indexing
// - Dual interface: SQL parser + direct type-safe API
// - Zero heavy dependencies (only serde/serde_json)
// - Designed for embeddability and future WebAssembly support

pub mod config;
pub mod error;
pub mod storage;
pub mod index;
pub mod query;
pub mod parser;
pub mod wasm;

#[cfg(feature = "repl")]
pub mod repl;

// Re-export commonly used types
pub use config::{DatabaseConfig, load_config, save_config};
pub use error::{Error, Result};
pub use storage::{Value, Row, TableEngine};
pub use index::{IndexManager};
pub use query::{Filter, Operator, DirectDataAccess, QueryBuilder, choose_index, apply_filters};
pub use parser::{parse_sql, Statement};

/// ThunderDB version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::collections::HashMap;
use std::path::PathBuf;

/// Main database handle
///
/// This is the primary entry point for interacting with ThunderDB.
/// It coordinates storage, indexing, and query execution.
pub struct Database {
    config: DatabaseConfig,
    data_dir: PathBuf,
    tables: HashMap<String, TableEngine>,
}

impl Database {
    /// Open or create a database at the specified path
    ///
    /// # Arguments
    /// * `data_dir` - Directory where database files will be stored
    ///
    /// # Returns
    /// A Database instance ready for operations
    pub fn open<P: AsRef<std::path::Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref();

        // Create data directory if it doesn't exist
        if ! data_dir.exists() {
            std::fs::create_dir_all(data_dir)?;
        }

        // Load or create configuration
        let config_path = data_dir.join("config.json");
        let mut config = if config_path.exists() {
            load_config(&config_path)?
        } else {
            let mut config = DatabaseConfig::default();
            config.storage.data_dir = data_dir.to_string_lossy().to_string();
            save_config(&config, &config_path)?;
            config
        };

        // Update data_dir to absolute path
        config.storage.data_dir = data_dir.to_string_lossy().to_string();

        Ok(Self {
            config,
            data_dir: data_dir.to_path_buf(),
            tables: HashMap::new(),
        })
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get a table, loading it if necessary. Fails if table doesn't exist.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let table = TableEngine::open(name, &self.data_dir, self.config.storage.clone())?;
            self.tables.insert(name.to_string(), table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// Create or get a table.
    pub fn get_or_create_table(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let table = TableEngine::create(name, &self.data_dir, self.config.storage.clone())?;
            self.tables.insert(name.to_string(), table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// Get a table (read-only)
    pub fn get_table(&self, name: &str) -> Option<&TableEngine> {
        self.tables.get(name)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        let mut table_names = std::collections::HashSet::new();

        // Add already loaded tables
        for name in self.tables.keys() {
            table_names.insert(name.clone());
        }

        // Scan data directory for other tables
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            // Check if it's a valid table directory (contains data.bin)
                            if entry.path().join("data.bin").exists() {
                                table_names.insert(name.to_string());
                            }
                        }
                    }
                }
            }
        }

        let mut result: Vec<String> = table_names.into_iter().collect();
        result.sort();
        result
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        // Remove from memory
        self.tables.remove(name);

        // Remove from disk
        let table_dir = self.data_dir.join(name);
        if table_dir.exists() {
            std::fs::remove_dir_all(table_dir)?;
            Ok(())
        } else {
            Err(Error::TableNotFound(name.to_string()))
        }
    }
}

// Implement DirectDataAccess for Database
impl DirectDataAccess for Database {
    fn insert_row(&mut self, table: &str, values: Vec<Value>) -> Result<u64> {
        let table_engine = self.get_or_create_table(table)?;
        table_engine.insert_row(values)
    }

    fn insert_batch(&mut self, table: &str, rows: Vec<Vec<Value>>) -> Result<Vec<u64>> {
        let table_engine = self.get_or_create_table(table)?;
        table_engine.insert_batch(rows)
    }

    fn get_by_id(&mut self, table: &str, row_id: u64) -> Result<Option<Row>> {
        let table_engine = self.get_table_mut(table)?;
        table_engine.get_by_id(row_id)
    }

    fn scan(&mut self, table: &str, filters: Vec<Filter>) -> Result<Vec<Row>> {
        self.scan_with_limit(table, filters, None, None)
    }

    fn scan_with_limit(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Row>> {
        let table_engine = self.get_table_mut(table)?;
        
        // Build column mapping from schema if available
        let mut column_mapping = std::collections::HashMap::new();
        if let Some(schema) = table_engine.schema() {
            for (i, col) in schema.columns.iter().enumerate() {
                column_mapping.insert(col.name.clone(), i);
            }
        }

        // Try to use an index
        let indexed_columns: Vec<String> = table_engine.index_manager().indexed_columns().to_vec();
        let mut rows = if let Some((col, op)) = choose_index(&filters, &indexed_columns) {
            match op {
                Operator::Equals(val) => table_engine.search_by_index(&col, &val)?,
                Operator::Between(start, end) => table_engine.range_search_by_index(&col, &start, &end)?,
                Operator::GreaterThan(val) => table_engine.greater_than_by_index(&col, &val, false)?,
                Operator::GreaterThanOrEqual(val) => table_engine.greater_than_by_index(&col, &val, true)?,
                Operator::LessThan(val) => table_engine.less_than_by_index(&col, &val, false)?,
                Operator::LessThanOrEqual(val) => table_engine.less_than_by_index(&col, &val, true)?,
                Operator::Like(pattern) => {
                    use crate::index::LikePattern;
                    if let Ok(lp) = LikePattern::parse(&pattern) {
                        if let Some(prefix) = lp.get_prefix() {
                            table_engine.prefix_search_by_index(&col, prefix)?
                        } else {
                            table_engine.scan_all()?
                        }
                    } else {
                        table_engine.scan_all()?
                    }
                }
                _ => table_engine.scan_all()?, // Fallback
            }
        } else {
            table_engine.scan_all()?
        };

        // Apply remaining filters (the ones not handled by index)
        if !filters.is_empty() {
            rows.retain(|row| {
                apply_filters(row, &filters, &column_mapping)
            });
        }

        // Apply offset
        if let Some(offset_val) = offset {
            if offset_val < rows.len() {
                rows = rows.into_iter().skip(offset_val).collect();
            } else {
                rows.clear();
            }
        }

        // Apply limit
        if let Some(limit_val) = limit {
            rows.truncate(limit_val);
        }

        Ok(rows)
    }

    fn update(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        updates: Vec<(String, Value)>,
    ) -> Result<usize> {
        // Get rows to update
        let rows = self.scan(table, filters)?;
        let count = rows.len();

        if count == 0 {
            return Ok(0);
        }

        let table_engine = self.get_table_mut(table)?;
        
        // Build column mapping from schema if available
        let mut column_mapping = std::collections::HashMap::new();
        if let Some(schema) = table_engine.schema() {
            for (i, col) in schema.columns.iter().enumerate() {
                column_mapping.insert(col.name.clone(), i);
            }
        }
        
        for mut row in rows {
            let row_id = row.row_id;
            
            // Apply updates to row.values
            for (col_name, new_val) in &updates {
                let col_idx = if let Some(&idx) = column_mapping.get(col_name) {
                    Some(idx)
                } else if col_name.starts_with("col") {
                    col_name[3..].parse::<usize>().ok()
                } else {
                    None
                };

                if let Some(idx) = col_idx {
                    if idx < row.values.len() {
                        row.values[idx] = new_val.clone();
                    }
                }
            }
            
            table_engine.update_row(row_id, row.values)?;
        }

        Ok(count)
    }

    fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        // Get rows to delete
        let rows = self.scan(table, filters)?;
        let count = rows.len();

        // Delete each row
        let table_engine = self.get_table_mut(table)?;
        for row in rows {
            table_engine.delete_by_id(row.row_id)?;
        }

        Ok(count)
    }

    fn count(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        let rows = self.scan(table, filters)?;
        Ok(rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_open() {
        let temp_dir = "/tmp/thunderdb_test";
        let _ = std::fs::remove_dir_all(temp_dir);

        let db = Database::open(temp_dir).unwrap();
        assert_eq!(db.config().storage.data_dir, temp_dir);

        std::fs::remove_dir_all(temp_dir).ok();
    }
}
