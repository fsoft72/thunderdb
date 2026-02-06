pub mod types;

use crate::error::{Error, Result};
use std::fs;
use std::path::Path;
pub use types::*;

/// Load configuration from a JSON file
///
/// # Arguments
/// * `path` - Path to the configuration file
///
/// # Returns
/// Parsed DatabaseConfig or default if file doesn't exist
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<DatabaseConfig> {
    let path = path.as_ref();

    if ! path.exists() {
        return Ok(DatabaseConfig::default());
    }

    let content = fs::read_to_string(path)?;
    let config: DatabaseConfig = serde_json::from_str(&content)?;

    validate_config(&config)?;

    Ok(config)
}

/// Save configuration to a JSON file
///
/// # Arguments
/// * `config` - Configuration to save
/// * `path` - Path where to save the configuration
pub fn save_config<P: AsRef<Path>>(config: &DatabaseConfig, path: P) -> Result<()> {
    validate_config(config)?;

    let content = serde_json::to_string_pretty(config)?;
    fs::write(path, content)?;

    Ok(())
}

/// Validate configuration values
///
/// # Arguments
/// * `config` - Configuration to validate
///
/// # Returns
/// Ok if valid, Error otherwise
fn validate_config(config: &DatabaseConfig) -> Result<()> {
    if config.storage.data_dir.is_empty() {
        return Err(Error::Config("data_dir cannot be empty".to_string()));
    }

    if config.index.btree_order < 3 {
        return Err(Error::Config("btree_order must be at least 3".to_string()));
    }

    if config.index.node_cache_size == 0 {
        return Err(Error::Config("node_cache_size must be greater than 0".to_string()));
    }

    if config.query.max_limit < config.query.default_limit {
        return Err(Error::Config("max_limit must be >= default_limit".to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DatabaseConfig::default();
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.index.btree_order, 100);
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_load_save_config() {
        let config = DatabaseConfig::default();
        let temp_file = "/tmp/test_config.json";

        save_config(&config, temp_file).unwrap();
        let loaded = load_config(temp_file).unwrap();

        assert_eq!(config.storage.data_dir, loaded.storage.data_dir);
        assert_eq!(config.index.btree_order, loaded.index.btree_order);

        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_validation() {
        let mut config = DatabaseConfig::default();

        // Invalid btree_order
        config.index.btree_order = 2;
        assert!(validate_config(&config).is_err());

        config.index.btree_order = 100;

        // Invalid node_cache_size
        config.index.node_cache_size = 0;
        assert!(validate_config(&config).is_err());
    }
}
