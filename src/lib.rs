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

#[cfg(feature = "repl")]
pub mod repl;

// Re-export commonly used types
pub use config::{DatabaseConfig, load_config, save_config};
pub use error::{Error, Result};
pub use storage::{Value, Row, TableEngine};
pub use index::{IndexManager};
pub use query::{Filter, Operator, DirectDataAccess, QueryBuilder};
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

    /// Create or get a table
    pub fn get_or_create_table(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let table = TableEngine::open(name, &self.data_dir, self.config.storage.clone())?;
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
        self.tables.keys().cloned().collect()
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
        let table_engine = self.get_or_create_table(table)?;
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
        let table_engine = self.get_or_create_table(table)?;
        let mut rows = table_engine.scan_all()?;

        // Apply filters
        if !filters.is_empty() {
            // Create column mapping (for now, assume columns are indexed by position)
            // In a full implementation, this would come from table schema
            let _column_mapping: HashMap<String, usize> = HashMap::new();

            rows.retain(|_row| {
                filters.iter().all(|_filter| {
                    // For now, we'll do a simple scan without column mapping
                    // A full implementation would use the schema
                    true
                })
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
        _table: &str,
        _filters: Vec<Filter>,
        _updates: Vec<(String, Value)>,
    ) -> Result<usize> {
        // UPDATE not yet implemented
        Err(Error::Query("UPDATE not yet implemented".to_string()))
    }

    fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        // Get rows to delete
        let rows = self.scan(table, filters)?;
        let count = rows.len();

        // Delete each row
        let table_engine = self.get_or_create_table(table)?;
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
