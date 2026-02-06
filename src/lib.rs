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

// Storage types (will be implemented in Phase 1)
// pub use storage::{Value, Row, TableEngine};

// Index types (will be implemented in Phase 2)
// pub use index::{BTree, IndexManager};

// Query types (will be implemented in Phase 3)
// pub use query::{Filter, Operator, DirectDataAccess};

// Parser types (will be implemented in Phase 4)
// pub use parser::{parse_sql, Statement};

/// ThunderDB version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Main database handle
///
/// This is the primary entry point for interacting with ThunderDB.
/// It coordinates storage, indexing, and query execution.
pub struct Database {
    config: DatabaseConfig,
    // tables: HashMap<String, TableEngine>,  // Will be added in Phase 1
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
            // tables: HashMap::new(),
        })
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
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
