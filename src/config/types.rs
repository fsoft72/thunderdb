use serde::{Deserialize, Serialize};

/// Main database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub storage: StorageConfig,
    pub index: IndexConfig,
    pub query: QueryConfig,
    #[serde(default)]
    pub repl: ReplConfig,
}

/// Storage layer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory for database files
    pub data_dir: String,

    /// Whether to fsync after every write
    #[serde(default)]
    pub fsync_on_write: bool,

    /// Interval in milliseconds for periodic fsync (if fsync_on_write is false)
    #[serde(default = "default_fsync_interval")]
    pub fsync_interval_ms: u64,

    /// Maximum size of data file in MB before rotation
    #[serde(default = "default_max_file_size")]
    pub max_data_file_size_mb: usize,
}

/// Index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// B-Tree order (max children per node)
    #[serde(default = "default_btree_order")]
    pub btree_order: usize,

    /// Maximum number of nodes to keep in cache
    #[serde(default = "default_node_cache_size")]
    pub node_cache_size: usize,

    /// Number of operations before triggering lazy index update
    #[serde(default = "default_lazy_threshold")]
    pub lazy_update_threshold: usize,

    /// Number of deletes before triggering index rebuild
    #[serde(default = "default_rebuild_threshold")]
    pub rebuild_threshold: usize,
}

/// Query execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Default LIMIT if not specified
    #[serde(default = "default_limit")]
    pub default_limit: usize,

    /// Maximum allowed LIMIT
    #[serde(default = "default_max_limit")]
    pub max_limit: usize,
}

/// REPL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplConfig {
    /// File to store command history
    #[serde(default = "default_history_file")]
    pub history_file: String,

    /// Maximum number of history entries
    #[serde(default = "default_max_history")]
    pub max_history_size: usize,

    /// REPL prompt string
    #[serde(default = "default_prompt")]
    pub prompt: String,
}

// Default value functions
const fn default_fsync_interval() -> u64 {
    1000
}

const fn default_max_file_size() -> usize {
    1024
}

const fn default_btree_order() -> usize {
    100
}

const fn default_node_cache_size() -> usize {
    1000
}

const fn default_lazy_threshold() -> usize {
    100
}

const fn default_rebuild_threshold() -> usize {
    10000
}

const fn default_limit() -> usize {
    1000
}

const fn default_max_limit() -> usize {
    100000
}

fn default_history_file() -> String {
    ".thunderdb_history".to_string()
}

const fn default_max_history() -> usize {
    1000
}

fn default_prompt() -> String {
    "thunderdb> ".to_string()
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            history_file: default_history_file(),
            max_history_size: default_max_history(),
            prompt: default_prompt(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                data_dir: "./data".to_string(),
                fsync_on_write: false,
                fsync_interval_ms: default_fsync_interval(),
                max_data_file_size_mb: default_max_file_size(),
            },
            index: IndexConfig {
                btree_order: default_btree_order(),
                node_cache_size: default_node_cache_size(),
                lazy_update_threshold: default_lazy_threshold(),
                rebuild_threshold: default_rebuild_threshold(),
            },
            query: QueryConfig {
                default_limit: default_limit(),
                max_limit: default_max_limit(),
            },
            repl: ReplConfig::default(),
        }
    }
}
