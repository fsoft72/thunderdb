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
#[cfg(feature = "wasm")]
pub mod wasm;

#[cfg(feature = "repl")]
pub mod repl;

// Re-export commonly used types
pub use config::{DatabaseConfig, load_config, save_config};
pub use error::{Error, Result};
pub use storage::{Value, Row, TableEngine};
pub use index::{IndexManager};
pub use query::{Filter, Operator, DirectDataAccess, QueryBuilder, choose_index, apply_filters, multi_index_scan};
pub use parser::{parse_sql, Statement, PreparedCache};

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
    #[allow(dead_code)]
    data_dir: PathBuf,
    tables: HashMap<String, TableEngine>,
    statement_cache: PreparedCache,
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

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Create data directory if it doesn't exist
            if ! data_dir.exists() {
                std::fs::create_dir_all(data_dir)?;
            }
        }

        // Load or create configuration
        let config_path = data_dir.join("config.json");
        let mut config = if config_path.exists() {
            load_config(&config_path)?
        } else {
            let mut config = DatabaseConfig::default();
            config.storage.data_dir = data_dir.to_string_lossy().to_string();
            #[cfg(not(target_arch = "wasm32"))]
            save_config(&config, &config_path)?;
            config
        };

        // Update data_dir to absolute path
        config.storage.data_dir = data_dir.to_string_lossy().to_string();

        Ok(Self {
            config,
            data_dir: data_dir.to_path_buf(),
            tables: HashMap::new(),
            statement_cache: PreparedCache::default(),
        })
    }

    /// Open an in-memory database
    pub fn open_in_memory() -> Result<Self> {
        let mut config = DatabaseConfig::default();
        config.storage.in_memory = true;
        config.storage.data_dir = ":memory:".to_string();

        Ok(Self {
            config,
            data_dir: PathBuf::from(":memory:"),
            tables: HashMap::new(),
            statement_cache: PreparedCache::default(),
        })
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Get database configuration (mutable)
    pub fn config_mut(&mut self) -> &mut DatabaseConfig {
        &mut self.config
    }

    /// Get a table, loading it if necessary. Fails if table doesn't exist.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let table = if self.config.storage.in_memory {
                TableEngine::load_to_memory(name, &self.data_dir, self.config.storage.clone())?
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                { TableEngine::open(name, &self.data_dir, self.config.storage.clone())? }
                #[cfg(target_arch = "wasm32")]
                { TableEngine::open_in_memory(name, self.config.storage.clone())? }
            };
            
            self.tables.insert(name.to_string(), table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// Create or get a table.
    pub fn get_or_create_table(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let table = if self.config.storage.in_memory {
                TableEngine::load_to_memory(name, &self.data_dir, self.config.storage.clone())?
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                { TableEngine::create(name, &self.data_dir, self.config.storage.clone())? }
                #[cfg(target_arch = "wasm32")]
                { TableEngine::open_in_memory(name, self.config.storage.clone())? }
            };

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
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.data_dir.exists() && self.data_dir.to_string_lossy() != ":memory:" {
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
            }
        }

        let mut result: Vec<String> = table_names.into_iter().collect();
        result.sort();
        result
    }

    /// Save the database to disk (if it has a disk path)
    pub fn save(&mut self) -> Result<()> {
        if self.data_dir.to_string_lossy() == ":memory:" {
            return Err(Error::Config("Cannot save a purely in-memory database without a data directory. Use open(path) with --memory instead.".to_string()));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Ensure data directory exists
            if ! self.data_dir.exists() {
                std::fs::create_dir_all(&self.data_dir)?;
            }

            // Save configuration
            let config_path = self.data_dir.join("config.json");
            save_config(&self.config, &config_path)?;

            let all_tables = self.list_tables();
            let base_dir = self.data_dir.clone();
            for table_name in all_tables {
                if ! self.tables.contains_key(&table_name) {
                    // Load and save (effectively a no-op if it was already on disk, 
                    // but ensures it's consistent if we changed something)
                    let table = self.get_table_mut(&table_name)?;
                    table.save_to_disk(&base_dir)?;
                } else {
                    let table = self.tables.get_mut(&table_name).unwrap();
                    table.save_to_disk(&base_dir)?;
                }
            }
        }

        Ok(())
    }

    /// Parse SQL with caching — returns a cached Statement on hit, parses on miss
    pub fn parse_sql_cached(&mut self, sql: &str) -> Result<Statement> {
        if let Some(stmt) = self.statement_cache.get(sql) {
            return Ok(stmt);
        }
        let stmt = parse_sql(sql)?;
        self.statement_cache.insert(sql, stmt.clone());
        Ok(stmt)
    }

    /// Clear the prepared statement cache (call after DDL operations)
    pub fn clear_statement_cache(&mut self) {
        self.statement_cache.clear();
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        // Remove from memory
        let removed = self.tables.remove(name).is_some();

        // Remove from disk
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.data_dir.to_string_lossy() == ":memory:" {
                if removed {
                    return Ok(());
                } else {
                    return Err(Error::TableNotFound(name.to_string()));
                }
            }

            let table_dir = self.data_dir.join(name);
            if table_dir.exists() {
                std::fs::remove_dir_all(table_dir)?;
                Ok(())
            } else if !removed {
                Err(Error::TableNotFound(name.to_string()))
            } else {
                Ok(())
            }
        }

        #[cfg(target_arch = "wasm32")]
        Ok(())
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

        // Collect stats for index selection
        let all_stats = table_engine.index_manager().all_stats();
        let stats_ref = if all_stats.is_empty() { None } else { Some(all_stats) };

        // Strategy 1: Try multi-index intersection (needs >=2 indexed filters)
        let mut remaining_filters = Vec::new();
        let multi_result = multi_index_scan(
            &filters,
            table_engine.index_manager(),
            stats_ref,
            &mut remaining_filters,
        );

        let (source_rows, active_filters) = if let Some(row_ids) = multi_result {
            let rows = table_engine.get_by_ids(&row_ids)?;
            (rows, remaining_filters)
        } else {
            // Strategy 2: Single best index
            let indexed_columns: Vec<String> = table_engine.index_manager().indexed_columns().to_vec();
            let source = if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
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
                    _ => table_engine.scan_all()?,
                }
            } else {
                table_engine.scan_all()?
            };
            (source, filters)
        };

        // Single-pass: filter + offset + limit with early termination
        let offset_val = offset.unwrap_or(0);
        let limit_val = limit.unwrap_or(usize::MAX);
        let mut skipped = 0usize;
        let mut result = Vec::new();

        for row in source_rows {
            if !active_filters.is_empty() && !apply_filters(&row, &active_filters, &column_mapping) {
                continue;
            }
            if skipped < offset_val {
                skipped += 1;
                continue;
            }
            result.push(row);
            if result.len() >= limit_val {
                break;
            }
        }

        Ok(result)
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
