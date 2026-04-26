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
use storage::page::value_at_page_bytes;
pub use index::{IndexManager};
pub use query::{Filter, Operator, DirectDataAccess, QueryBuilder, Aggregate, AggRow, choose_index, apply_filters, multi_index_scan};
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
    /// Cached table names, invalidated on create/drop
    table_names_cache: Option<Vec<String>>,
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
            table_names_cache: None,
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
            table_names_cache: None,
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

    /// Load or create a table depending on the `create_if_missing` flag.
    ///
    /// When `create_if_missing` is false, returns Error::TableNotFound if the
    /// table doesn't exist. When true, creates it on the fly.
    fn load_table(&mut self, name: &str, create_if_missing: bool) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            let btree_order = self.config.index.btree_order;
            let table = if self.config.storage.in_memory {
                TableEngine::load_to_memory(name, &self.data_dir, self.config.storage.clone(), btree_order)?
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    if create_if_missing {
                        TableEngine::create(name, &self.data_dir, self.config.storage.clone(), btree_order)?
                    } else {
                        TableEngine::open(name, &self.data_dir, self.config.storage.clone(), btree_order)?
                    }
                }
                #[cfg(target_arch = "wasm32")]
                { TableEngine::open_in_memory(name, self.config.storage.clone(), btree_order)? }
            };
            self.tables.insert(name.to_string(), table);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// Get a table, loading it if necessary. Fails if table doesn't exist.
    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableEngine> {
        self.load_table(name, false)
    }

    /// Return up to `k` rows from `table`, sorted by the indexed column `col`.
    /// `desc` selects descending order.
    /// Returns an error if the table is missing or the column has no index.
    pub fn scan_indexed_top_k(
        &mut self,
        table: &str,
        col: &str,
        k: usize,
        desc: bool,
    ) -> Result<Vec<Row>> {
        let table_engine = self.get_table_mut(table)?;
        let row_ids = table_engine
            .index_manager()
            .indexed_top_k_row_ids(col, k, desc)
            .ok_or_else(|| Error::Index(format!("No index on column: {}", col)))?;
        table_engine.get_by_ids(&row_ids)
    }

    /// Create or get a table.
    pub fn get_or_create_table(&mut self, name: &str) -> Result<&mut TableEngine> {
        if !self.tables.contains_key(name) {
            self.table_names_cache = None; // Invalidate on potential creation
        }
        self.load_table(name, true)
    }

    /// Get a table (read-only)
    pub fn get_table(&self, name: &str) -> Option<&TableEngine> {
        self.tables.get(name)
    }

    /// List all table names (cached, invalidated on create/drop)
    pub fn list_tables(&mut self) -> Vec<String> {
        if let Some(ref cached) = self.table_names_cache {
            return cached.clone();
        }

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
                                    // Check if it's a valid table directory (contains pages.bin)
                                    if entry.path().join("pages.bin").exists() {
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
        self.table_names_cache = Some(result.clone());
        result
    }

    /// Save the database to disk (if it has a disk path)
    ///
    /// Only saves tables that are already loaded — avoids loading unmodified
    /// on-disk tables just to re-save them.
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

            // Only save tables that are already loaded in memory
            let base_dir = self.data_dir.clone();
            for table in self.tables.values_mut() {
                table.save_to_disk(&base_dir)?;
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
        self.table_names_cache = None; // Invalidate cache

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

        // Build column mapping from schema (uses cached Arc)
        let column_mapping = table_engine.build_column_mapping();

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
            if remaining_filters.is_empty() {
                let rows = table_engine.get_by_ids(&row_ids)?;
                (rows, remaining_filters)
            } else {
                let mut remaining_filters = remaining_filters;
                remaining_filters.sort_by_key(|f| f.estimated_cost());
                let rem_col_indices: Vec<Option<usize>> = remaining_filters
                    .iter()
                    .map(|f| {
                        if let Some(&idx) = column_mapping.get(&f.column) {
                            Some(idx)
                        } else if f.column.starts_with("col") {
                            f.column[3..].parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .collect();
                let rows = table_engine.get_by_ids_filtered(&row_ids, |raw_bytes| {
                    for (filter, col_idx) in remaining_filters.iter().zip(rem_col_indices.iter()) {
                        if let Some(idx) = col_idx {
                            match value_at_page_bytes(raw_bytes, *idx) {
                                Ok(val) => {
                                    if !filter.matches(&val) {
                                        return false;
                                    }
                                }
                                Err(_) => return false,
                            }
                        } else {
                            return false;
                        }
                    }
                    true
                })?;
                (rows, vec![])
            }
        } else {
            // Strategy 2: Single best index
            let indexed_columns: Vec<String> = table_engine.index_manager().indexed_columns().to_vec();
            if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
                // Check selectivity: if index returns >25% of table,
                // a sequential scan with predicate is faster.
                let total = table_engine.active_row_count();
                let use_index = if let Some(row_ids) = table_engine.index_manager().query_row_ids(&col, &op) {
                    if total > 0 && row_ids.len() * 4 > total {
                        None // too many results — sequential scan wins
                    } else {
                        Some(row_ids)
                    }
                } else {
                    None
                };

                if let Some(row_ids) = use_index {
                    // Collect remaining filters (everything except the indexed one)
                    let remaining: Vec<Filter> = filters
                        .iter()
                        .filter(|f| f.column != col || f.operator != op)
                        .cloned()
                        .collect();

                    if remaining.is_empty() {
                        (table_engine.get_by_ids(&row_ids)?, vec![])
                    } else {
                        let mut remaining = remaining;
                        remaining.sort_by_key(|f| f.estimated_cost());
                        let rem_col_indices: Vec<Option<usize>> = remaining
                            .iter()
                            .map(|f| {
                                if let Some(&idx) = column_mapping.get(&f.column) {
                                    Some(idx)
                                } else if f.column.starts_with("col") {
                                    f.column[3..].parse::<usize>().ok()
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let rows = table_engine.get_by_ids_filtered(&row_ids, |raw_bytes| {
                            for (filter, col_idx) in remaining.iter().zip(rem_col_indices.iter()) {
                                if let Some(idx) = col_idx {
                                    match value_at_page_bytes(raw_bytes, *idx) {
                                        Ok(val) => {
                                            if !filter.matches(&val) {
                                                return false;
                                            }
                                        }
                                        Err(_) => return false,
                                    }
                                } else {
                                    return false;
                                }
                            }
                            true
                        })?;
                        (rows, vec![])
                    }
                } else {
                    // Index not usable or low selectivity — fall through
                    // to the filtered scan path below.
                    let mut filters = filters;
                    filters.sort_by_key(|f| f.estimated_cost());
                    let filter_col_indices: Vec<Option<usize>> = filters
                        .iter()
                        .map(|f| {
                            if let Some(&idx) = column_mapping.get(&f.column) {
                                Some(idx)
                            } else if f.column.starts_with("col") {
                                f.column[3..].parse::<usize>().ok()
                            } else {
                                None
                            }
                        })
                        .collect();
                    let rows = table_engine.scan_all_filtered(|raw_bytes| {
                        for (filter, col_idx) in filters.iter().zip(filter_col_indices.iter()) {
                            if let Some(idx) = col_idx {
                                match value_at_page_bytes(raw_bytes, *idx) {
                                    Ok(val) => {
                                        if !filter.matches(&val) { return false; }
                                    }
                                    Err(_) => return false,
                                }
                            } else { return false; }
                        }
                        true
                    })?;
                    return Ok(apply_pagination(rows, limit, offset));
                }
            } else if filters.is_empty() {
                // No filters: push limit into scan
                let scan_limit = limit.map(|l| l + offset.unwrap_or(0));
                (table_engine.scan_all_limited(scan_limit)?, filters)
            } else {
                // Filtered scan: use callback to filter on raw bytes
                // before full deserialization
                let mut filters = filters;
                filters.sort_by_key(|f| f.estimated_cost());
                let filter_col_indices: Vec<Option<usize>> = filters
                    .iter()
                    .map(|f| {
                        if let Some(&idx) = column_mapping.get(&f.column) {
                            Some(idx)
                        } else if f.column.starts_with("col") {
                            f.column[3..].parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                let rows = table_engine.scan_all_filtered(|raw_bytes| {
                    for (filter, col_idx) in filters.iter().zip(filter_col_indices.iter()) {
                        if let Some(idx) = col_idx {
                            match value_at_page_bytes(raw_bytes, *idx) {
                                Ok(val) => {
                                    if !filter.matches(&val) {
                                        return false;
                                    }
                                }
                                Err(_) => return false,
                            }
                        } else {
                            return false;
                        }
                    }
                    true
                })?;
                return Ok(apply_pagination(rows, limit, offset));
            }
        };

        let mut active_filters = active_filters;
        active_filters.sort_by_key(|f| f.estimated_cost());

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

    fn scan_with_projection(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<usize>>,
    ) -> Result<Vec<Row>> {
        let cols = match projection {
            None => return self.scan_with_limit(table, filters, limit, offset),
            Some(c) => c,
        };

        let table_engine = self.get_table_mut(table)?;
        let column_mapping = table_engine.build_column_mapping();
        let all_stats = table_engine.index_manager().all_stats();
        let stats_ref = if all_stats.is_empty() { None } else { Some(all_stats) };

        // Try index path
        let indexed_columns: Vec<String> = table_engine.index_manager().indexed_columns().to_vec();
        let index_choice = choose_index(&filters, &indexed_columns, stats_ref);

        let source_rows = if filters.is_empty() {
            // No filters: projected scan
            table_engine.scan_all_projected(&cols)?
        } else if let Some((col, ref op)) = index_choice {
            if let Some(row_ids) = table_engine.index_manager().query_row_ids(&col, op) {
                let remaining: Vec<Filter> = filters.iter()
                    .filter(|f| f.column != col || f.operator != *op)
                    .cloned().collect();

                if remaining.is_empty() {
                    table_engine.get_by_ids_projected(&row_ids, &cols)?
                } else {
                    let rem_col_indices: Vec<Option<usize>> = remaining.iter()
                        .map(|f| column_mapping.get(&f.column).copied()
                            .or_else(|| f.column.strip_prefix("col").and_then(|s| s.parse().ok())))
                        .collect();
                    // Filter on raw bytes, then project only requested columns
                    let ctids: Vec<_> = row_ids.iter()
                        .map(|&id| crate::storage::page::Ctid::from_u64(id)).collect();
                    table_engine.paged_table_mut().get_rows_by_ctids_filtered(&ctids, |raw_bytes| {
                        for (filter, col_idx) in remaining.iter().zip(rem_col_indices.iter()) {
                            if let Some(idx) = col_idx {
                                match value_at_page_bytes(raw_bytes, *idx) {
                                    Ok(val) => if !filter.matches(&val) { return false; },
                                    Err(_) => return false,
                                }
                            } else { return false; }
                        }
                        true
                    })?
                }
            } else {
                // Fallback: full scan + filter + project post-hoc
                let rows = self.scan_with_limit(table, filters, limit, offset)?;
                return Ok(_project_rows(rows, &cols));
            }
        } else {
            // No index: filtered scan with projection
            let mut filters = filters;
            filters.sort_by_key(|f| f.estimated_cost());
            let filter_col_indices: Vec<Option<usize>> = filters.iter()
                .map(|f| column_mapping.get(&f.column).copied()
                    .or_else(|| f.column.strip_prefix("col").and_then(|s| s.parse().ok())))
                .collect();
            let rows = table_engine.scan_all_filtered(|raw_bytes| {
                for (filter, col_idx) in filters.iter().zip(filter_col_indices.iter()) {
                    if let Some(idx) = col_idx {
                        match value_at_page_bytes(raw_bytes, *idx) {
                            Ok(val) => if !filter.matches(&val) { return false; },
                            Err(_) => return false,
                        }
                    } else { return false; }
                }
                true
            })?;
            return Ok(apply_pagination(_project_rows(rows, &cols), limit, offset));
        };

        Ok(apply_pagination(source_rows, limit, offset))
    }

    fn update(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        updates: Vec<(String, Value)>,
    ) -> Result<usize> {
        // Collect matching rows in a single scan pass (keep full Row data).
        // Rows already carry old values — build the full batch before handing
        // off to update_batch, avoiding redundant per-row mmap sync_all calls.
        let rows = self.scan(table, filters)?;

        if rows.is_empty() {
            return Ok(0);
        }

        let table_engine = self.get_table_mut(table)?;
        let column_mapping = table_engine.build_column_mapping();

        let batch: Vec<(u64, Vec<Value>, Vec<Value>)> = rows.into_iter().map(|row| {
            let old_values = row.values.clone();
            let mut new_values = row.values;

            // Apply updates to new_values
            for (col_name, new_val) in &updates {
                let col_idx = if let Some(&idx) = column_mapping.get(col_name) {
                    Some(idx)
                } else if col_name.starts_with("col") {
                    col_name[3..].parse::<usize>().ok()
                } else {
                    None
                };

                if let Some(idx) = col_idx {
                    if idx < new_values.len() {
                        new_values[idx] = new_val.clone();
                    }
                }
            }

            (row.row_id, old_values, new_values)
        }).collect();

        let count = batch.len();
        table_engine.update_batch(&batch)?;
        Ok(count)
    }

    fn delete(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        // Collect matching rows in a single scan pass.
        // Rows already carry old values — build the full batch before handing
        // off to delete_batch, avoiding redundant per-row mmap sync_all calls.
        let rows = self.scan(table, filters)?;

        if rows.is_empty() {
            return Ok(0);
        }

        let count = rows.len();
        let table_engine = self.get_table_mut(table)?;

        let deletions: Vec<(u64, Vec<Value>)> = rows.into_iter()
            .map(|r| (r.row_id, r.values))
            .collect();

        table_engine.delete_batch(&deletions)?;
        Ok(count)
    }

    /// Count rows matching the given filters, using index-only or callback paths
    /// to avoid materializing full Row objects.
    fn count(&mut self, table: &str, filters: Vec<Filter>) -> Result<usize> {
        let table_engine = self.get_table_mut(table)?;

        // Fast path: no filters → O(1) from active count
        if filters.is_empty() {
            return Ok(table_engine.active_row_count());
        }

        let column_mapping = table_engine.build_column_mapping();
        let all_stats = table_engine.index_manager().all_stats();
        let stats_ref = if all_stats.is_empty() { None } else { Some(all_stats) };

        // Single-filter fast path: count via index without collecting row IDs
        if filters.len() == 1 {
            let indexed_columns = table_engine.index_manager().indexed_columns().to_vec();
            if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
                if let Some(count) = table_engine.index_manager().count_row_ids(&col, &op) {
                    return Ok(count);
                }
            }
        } else {
            // Multi-filter: try index intersection
            let mut remaining = Vec::new();
            let multi = multi_index_scan(
                &filters,
                table_engine.index_manager(),
                stats_ref,
                &mut remaining,
            );
            if let Some(row_ids) = multi {
                if remaining.is_empty() {
                    return Ok(row_ids.len());
                }
            }

            // Fallback to single index
            let indexed_columns = table_engine.index_manager().indexed_columns().to_vec();
            if let Some((col, op)) = choose_index(&filters, &indexed_columns, stats_ref) {
                if let Some(row_ids) = table_engine.index_manager().query_row_ids(&col, &op) {
                    if filters.len() == 1 {
                        return Ok(row_ids.len());
                    }
                }
            }
        }

        // Path B: callback count (scan but no Row allocation)
        let filter_col_indices: Vec<Option<usize>> = filters
            .iter()
            .map(|f| {
                if let Some(&idx) = column_mapping.get(&f.column) {
                    Some(idx)
                } else if f.column.starts_with("col") {
                    f.column[3..].parse::<usize>().ok()
                } else {
                    None
                }
            })
            .collect();

        table_engine.count_filtered(|raw_bytes| {
            for (filter, col_idx) in filters.iter().zip(filter_col_indices.iter()) {
                if let Some(idx) = col_idx {
                    match value_at_page_bytes(raw_bytes, *idx) {
                        Ok(val) => {
                            if !filter.matches(&val) {
                                return false;
                            }
                        }
                        Err(_) => return false,
                    }
                } else {
                    return false;
                }
            }
            true
        })
    }

    /// Stream rows through a callback, projecting to the requested columns.
    ///
    /// # Hot path (no filters)
    /// Delegates directly to `TableEngine::for_each_row_projected` — zero
    /// per-row heap allocations for inline-sized values.
    ///
    /// # Filter path
    /// Wraps `scan_with_projection` as a correctness-preserving fallback.
    /// Streaming filter optimisation is deferred (spec §3.5 Plan B).
    ///
    /// # Arguments
    /// * `table`      - Table name
    /// * `filters`    - Row predicates; empty = no filter (hot path)
    /// * `projection` - Column indices to materialise; `None` = all columns
    ///                  (requires schema to be set on the table)
    /// * `callback`   - Called once per matching row with a slice of values
    ///
    /// # Returns
    /// Number of rows passed to the callback.
    fn for_each_row<F: FnMut(&[Value])>(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        projection: Option<Vec<usize>>,
        mut callback: F,
    ) -> Result<usize> {
        // Resolve projection (explicit, or all-columns from schema).
        // The borrow of `table_engine` is dropped at the end of this block.
        let cols: Vec<usize> = {
            let table_engine = self.get_table_mut(table)?;
            match projection {
                Some(c) => c,
                None => {
                    let schema = table_engine.schema().ok_or_else(|| {
                        Error::InvalidOperation(
                            "for_each_row with projection=None requires table schema to be set".into()
                        )
                    })?;
                    (0..schema.columns.len()).collect()
                }
            }
        };

        // Hot path: no filters → direct streaming scan (zero per-row allocs)
        if filters.is_empty() {
            let table_engine = self.get_table_mut(table)?;
            return table_engine.for_each_row_projected(&cols, callback);
        }

        // Filter path: wrap scan_with_projection as fallback. Correctness
        // first; streaming filter optimisation is deferred (spec §3.5 Plan B).
        let rows = self.scan_with_projection(table, filters, None, None, Some(cols))?;
        let n = rows.len();
        for row in rows {
            callback(&row.values);
        }
        Ok(n)
    }

    /// GROUP BY + aggregate over `table`, with optional WHERE `filters`.
    ///
    /// Default code path: full streaming scan via `for_each_row` (with
    /// minimal projection covering only the columns referenced by
    /// `group_by` and `aggs`), folding rows into a hash-grouped
    /// accumulator. Indexed / cached fast paths will dispatch in front
    /// of this in later tasks.
    fn aggregate(
        &mut self,
        table: &str,
        group_by: Vec<String>,
        aggs: Vec<Aggregate>,
        filters: Vec<Filter>,
    ) -> Result<Vec<AggRow>> {
        use crate::query::aggregate as aggm;

        // Fast path: global COUNT(*) (with optional filters) → route to
        // existing count(), which already uses index/callback fast paths.
        if group_by.is_empty()
            && aggs.len() == 1
            && matches!(aggs[0], Aggregate::Count)
        {
            let n = self.count(table, filters)?;
            let n_i64 = i64::try_from(n).map_err(|_| {
                crate::error::Error::Query(
                    "aggregate: COUNT exceeds i64::MAX".into()
                )
            })?;
            return Ok(vec![AggRow { keys: vec![], aggs: vec![Value::Int64(n_i64)] }]);
        }

        // Fast path: global MIN/MAX over indexed columns, no filters.
        // Each MIN/MAX collapses to a single B-tree endpoint lookup.
        if group_by.is_empty()
            && filters.is_empty()
            && !aggs.is_empty()
            && aggs.iter().all(|a| matches!(a, Aggregate::Min(_) | Aggregate::Max(_)))
        {
            // Verify all referenced columns are indexed.
            let all_indexed = {
                let tbl = self.get_table_mut(table)?;
                aggs.iter().all(|a| {
                    let col = match a {
                        Aggregate::Min(c) | Aggregate::Max(c) => c,
                        _ => unreachable!(),
                    };
                    tbl.index_manager().has_index(col)
                })
            };
            if all_indexed {
                // Snapshot schema column ordering once for index lookup.
                let schema_cols: Vec<String> = {
                    let tbl = self.get_table_mut(table)?;
                    let s = tbl.schema().ok_or_else(|| {
                        Error::InvalidOperation(
                            format!("aggregate: table `{}` has no schema", table)
                        )
                    })?;
                    s.columns.iter().map(|c| c.name.clone()).collect()
                };
                let mut out = Vec::with_capacity(aggs.len());
                for a in &aggs {
                    let (col, want_max) = match a {
                        Aggregate::Min(c) => (c, false),
                        Aggregate::Max(c) => (c, true),
                        _ => unreachable!(),
                    };
                    let rows = self.scan_indexed_top_k(table, col, 1, want_max)?;
                    let v = rows.first().and_then(|r| {
                        let idx = schema_cols.iter().position(|c| c == col)?;
                        r.values.get(idx).cloned()
                    }).unwrap_or(Value::Null);
                    out.push(v);
                }
                return Ok(vec![AggRow { keys: vec![], aggs: out }]);
            }
        }

        // Snapshot the schema column names + types (drops the &mut borrow
        // before re-entering `for_each_row`, which also borrows mutably).
        let (schema_cols, schema_types): (Vec<String>, Vec<String>) = {
            let table_engine = self.get_table_mut(table)?;
            let schema = table_engine.schema().ok_or_else(|| {
                Error::InvalidOperation(
                    "aggregate requires table schema to be set".into()
                )
            })?;
            let cols = schema.columns.iter().map(|c| c.name.clone()).collect();
            let types = schema.columns.iter().map(|c| c.data_type.clone()).collect();
            (cols, types)
        };

        let plan = aggm::plan(&schema_cols, &schema_types, &group_by, &aggs)?;
        let projection = Some(plan.projection.clone());

        let mut agg = aggm::Aggregator::new(&plan);
        self.for_each_row(table, filters, projection, |row| agg.feed(row))?;

        agg.into_rows()
    }

    /// `SELECT DISTINCT cols FROM table WHERE filters`.
    ///
    /// Default code path: streaming scan with projection limited to the
    /// selected columns, deduplicating by hashing each projected row tuple
    /// into a `HashSet<Vec<Value>>`. Row order in the result is unspecified.
    fn distinct(
        &mut self,
        table: &str,
        cols: Vec<String>,
        filters: Vec<Filter>,
    ) -> Result<Vec<Vec<Value>>> {
        use std::collections::HashSet;

        // Fast path: single column, no filters, column is indexed →
        // walk the B-tree leaves and emit each unique key once.
        if cols.len() == 1 && filters.is_empty() {
            let col = &cols[0];
            if let Ok(tbl) = self.get_table_mut(table) {
                if tbl.index_manager().has_index(col) {
                    if let Some(keys) = tbl.index_manager().distinct_indexed_keys(col) {
                        return Ok(keys.into_iter().map(|v| vec![v]).collect());
                    }
                }
            }
        }

        // Snapshot schema column names (drops the &mut borrow before
        // re-entering `for_each_row`, which also borrows mutably).
        let schema_cols: Vec<String> = {
            let table_engine = self.get_table_mut(table)?;
            let schema = table_engine.schema().ok_or_else(|| {
                Error::InvalidOperation(
                    "distinct requires table schema to be set".into()
                )
            })?;
            schema.columns.iter().map(|c| c.name.clone()).collect()
        };

        // Resolve each requested column name to its position in the schema.
        let proj_idxs: Vec<usize> = cols.iter().map(|name| {
            schema_cols.iter().position(|c| c == name).ok_or_else(|| {
                Error::Query(format!("distinct: unknown column `{}`", name))
            })
        }).collect::<Result<_>>()?;

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        let projection = Some(proj_idxs.clone());
        let proj_len = proj_idxs.len();

        self.for_each_row(table, filters, projection, |row| {
            let key: Vec<Value> = (0..proj_len).map(|i| row[i].clone()).collect();
            seen.insert(key);
        })?;

        Ok(seen.into_iter().collect())
    }
}

/// Apply offset and limit to a pre-filtered result set.
/// Project rows to keep only selected column indices.
fn _project_rows(rows: Vec<Row>, cols: &[usize]) -> Vec<Row> {
    rows.into_iter()
        .map(|row| {
            let values = cols.iter()
                .filter_map(|&i| row.values.get(i).cloned())
                .collect();
            Row::new(row.row_id, values)
        })
        .collect()
}

fn apply_pagination(rows: Vec<Row>, limit: Option<usize>, offset: Option<usize>) -> Vec<Row> {
    let offset_val = offset.unwrap_or(0);
    let limit_val = limit.unwrap_or(usize::MAX);

    rows.into_iter()
        .skip(offset_val)
        .take(limit_val)
        .collect()
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

    #[test]
    fn for_each_row_streams_no_filter() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};

        let dir = std::env::temp_dir().join("thunderdb_db_for_each_nofilter");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();

        db.insert_batch("t", vec![
            vec![Value::Int32(1), Value::varchar("alice".to_string())],
            vec![Value::Int32(2), Value::varchar("bob".to_string())],
            vec![Value::Int32(3), Value::varchar("charlie".to_string())],
        ]).unwrap();
        {
            let tbl = db.get_table_mut("t").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ]}).unwrap();
        }

        let mut ids: Vec<i32> = Vec::new();
        let count = db.for_each_row("t", vec![], Some(vec![0]), |vals| {
            if let Value::Int32(n) = vals[0] { ids.push(n); }
        }).unwrap();

        assert_eq!(count, 3);
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_row_with_filter_uses_fallback() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};
        use crate::query::{Filter, Operator};

        let dir = std::env::temp_dir().join("thunderdb_db_for_each_filter");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();

        db.insert_batch("t", vec![
            vec![Value::Int32(1), Value::varchar("a".to_string())],
            vec![Value::Int32(2), Value::varchar("b".to_string())],
            vec![Value::Int32(3), Value::varchar("c".to_string())],
        ]).unwrap();
        {
            let tbl = db.get_table_mut("t").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ]}).unwrap();
        }

        let mut matched: Vec<i32> = Vec::new();
        let count = db.for_each_row(
            "t",
            vec![Filter::new("id", Operator::GreaterThan(Value::Int32(1)))],
            Some(vec![0]),
            |vals| {
                if let Value::Int32(n) = vals[0] { matched.push(n); }
            },
        ).unwrap();

        assert_eq!(count, 2);
        matched.sort();
        assert_eq!(matched, vec![2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_row_projection_none_requires_schema() {
        let dir = std::env::temp_dir().join("thunderdb_db_for_each_noschema");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();
        db.insert_batch("t", vec![vec![Value::Int32(1)]]).unwrap();

        let r = db.for_each_row("t", vec![], None, |_| {});
        assert!(matches!(r, Err(Error::InvalidOperation(_))));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_batch_update_via_sql() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};

        // Clean up any leftover temp state from prior runs (open_in_memory uses tmp dir by table name)
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("thunderdb_mem_batch_upd_t"));

        let mut db = Database::open_in_memory().unwrap();
        // Insert 20 rows: (i, 0)
        let rows_data: Vec<Vec<Value>> = (0..20i32)
            .map(|i| vec![Value::Int32(i), Value::Int32(0)])
            .collect();
        db.insert_batch("batch_upd_t", rows_data).unwrap();
        {
            let tbl = db.get_table_mut("batch_upd_t").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT".into() },
                ColumnInfo { name: "val".into(), data_type: "INT".into() },
            ]}).unwrap();
        }
        // Update all rows: SET val = 99
        let updated = db.update("batch_upd_t", vec![], vec![("val".to_string(), Value::Int32(99))]).unwrap();
        assert_eq!(updated, 20);
        let rows = db.scan("batch_upd_t", vec![]).unwrap();
        assert_eq!(rows.len(), 20);
        assert!(rows.iter().all(|r| r.values[1] == Value::Int32(99)));

        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("thunderdb_mem_batch_upd_t"));
    }

    #[test]
    fn test_batch_delete_via_sql() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};

        // Clean up any leftover temp state from prior runs (open_in_memory uses tmp dir by table name)
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("thunderdb_mem_batch_del_t2"));

        let mut db = Database::open_in_memory().unwrap();
        // Insert 20 rows: (i, i)
        let rows_data: Vec<Vec<Value>> = (0..20i32)
            .map(|i| vec![Value::Int32(i), Value::Int32(i)])
            .collect();
        db.insert_batch("batch_del_t2", rows_data).unwrap();
        {
            let tbl = db.get_table_mut("batch_del_t2").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT".into() },
                ColumnInfo { name: "val".into(), data_type: "INT".into() },
            ]}).unwrap();
        }
        // DELETE FROM batch_del_t2 WHERE val >= 10
        let deleted = db.delete("batch_del_t2", vec![
            Filter::new("val", Operator::GreaterThanOrEqual(Value::Int32(10))),
        ]).unwrap();
        assert_eq!(deleted, 10);
        let rows = db.scan("batch_del_t2", vec![]).unwrap();
        assert_eq!(rows.len(), 10);

        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("thunderdb_mem_batch_del_t2"));
    }

    #[test]
    fn timing_w5_disk_with_persisted_index() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};
        let dir = std::env::temp_dir().join("thunderdb_w5_disk_idx");
        let _ = std::fs::remove_dir_all(&dir);

        {
            let mut db = Database::open(&dir).unwrap();
            let n = 10_000i32;
            let posts: Vec<Vec<Value>> = (1..=n).map(|i| vec![
                Value::Int32(i),
                Value::Int32((i % 50) + 1),
                Value::varchar(format!("Post about Rust #{}", i)),
                Value::varchar(format!("This is post {} discussing Rust in depth. ThunderDB makes Rust easy.", i)),
            ]).collect();
            db.insert_batch("blog_posts", posts).unwrap();
            {
                let tbl = db.get_table_mut("blog_posts").unwrap();
                tbl.set_schema(TableSchema { columns: vec![
                    ColumnInfo { name: "id".into(),        data_type: "INT32".into() },
                    ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "title".into(),     data_type: "VARCHAR".into() },
                    ColumnInfo { name: "content".into(),   data_type: "VARCHAR".into() },
                ]}).unwrap();
                tbl.create_index("id").unwrap();
                tbl.create_index("author_id").unwrap();
                tbl.create_index("title").unwrap();
            }
        } // drop db — index files should be persisted

        // Verify index files exist
        let idx_dir = dir.join("blog_posts").join("indices");
        let idx_count = std::fs::read_dir(&idx_dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("idx"))
            .count();
        assert_eq!(idx_count, 3, "3 index files must be persisted");

        // Reopen like restore_all does — indices should load
        let mut db = Database::open(&dir).unwrap();
        let _ = db.count("blog_posts", vec![]).unwrap();

        let n = 10_000i32;
        let t0 = std::time::Instant::now();
        for i in 1..=n {
            db.update("blog_posts",
                vec![Filter::new("id", Operator::Equals(Value::Int32(i)))],
                vec![("title".into(), Value::varchar(format!("Updated #{}", i)))]).unwrap();
        }
        let elapsed = t0.elapsed();
        let ms_per_op = elapsed.as_millis() as f64 / n as f64;
        eprintln!("{} W5 disk+persisted-index: {:?} = {:.3}ms/op", n, elapsed, ms_per_op);
        assert!(ms_per_op < 1.0, "W5 with persisted index should be < 1ms/op, got {:.3}", ms_per_op);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
