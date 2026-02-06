// B-Tree persistence - Step 2.2
//
// Binary serialization for B-Tree nodes with LRU cache

use crate::error::{Error, Result};
use crate::index::btree::BTree;
use crate::index::node::BTreeNode;
use crate::storage::Value;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Magic number for index file format: "TIDX" (ThunderDB Index)
const INDEX_MAGIC: [u8; 4] = [b'T', b'I', b'D', b'X'];
const INDEX_VERSION: u32 = 1;

/// Serialize a B-Tree to binary format and save to file
///
/// Format:
/// - Header: [magic: 4 bytes][version: 4 bytes][order: 4 bytes][root_id: 8 bytes][node_count: 8 bytes]
/// - For each node: [serialized node]
///
/// # Arguments
/// * `tree` - BTree to serialize
/// * `path` - Path to .idx file
pub fn save_index<P: AsRef<Path>>(tree: &BTree<Value, u64>, path: P) -> Result<()> {
    let path = path.as_ref();
    let mut file = File::create(path)?;

    let stats = tree.stats();

    // Write header
    file.write_all(&INDEX_MAGIC)?;
    file.write_all(&INDEX_VERSION.to_le_bytes())?;
    file.write_all(&(stats.order as u32).to_le_bytes())?;

    // For now, we'll save a simplified version without all internal state
    // In a full implementation, we'd need access to tree internals
    // This is a placeholder that saves the data in a way we can rebuild
    file.write_all(&0u64.to_le_bytes())?; // root_id placeholder
    file.write_all(&(stats.total_keys as u64).to_le_bytes())?;

    // Write all key-value pairs (we'll rebuild the tree structure on load)
    for (key, value) in tree.scan_all() {
        write_value(&mut file, &key)?;
        file.write_all(&value.to_le_bytes())?;
    }

    file.sync_all()?;
    Ok(())
}

/// Load a B-Tree from binary format
///
/// # Arguments
/// * `path` - Path to .idx file
///
/// # Returns
/// Reconstructed BTree
pub fn load_index<P: AsRef<Path>>(path: P) -> Result<BTree<Value, u64>> {
    let path = path.as_ref();

    if ! path.exists() {
        return Err(Error::Index(format!("Index file not found: {:?}", path)));
    }

    let mut file = File::open(path)?;

    // Read and verify header
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if magic != INDEX_MAGIC {
        return Err(Error::Index("Invalid index file magic number".to_string()));
    }

    let mut version_buf = [0u8; 4];
    file.read_exact(&mut version_buf)?;
    let version = u32::from_le_bytes(version_buf);
    if version != INDEX_VERSION {
        return Err(Error::Index(format!("Unsupported index version: {}", version)));
    }

    let mut order_buf = [0u8; 4];
    file.read_exact(&mut order_buf)?;
    let order = u32::from_le_bytes(order_buf) as usize;

    // Skip root_id placeholder
    file.seek(SeekFrom::Current(8))?;

    let mut count_buf = [0u8; 8];
    file.read_exact(&mut count_buf)?;
    let count = u64::from_le_bytes(count_buf) as usize;

    // Create new tree
    let mut tree = BTree::new(order)?;

    // Read and insert all key-value pairs
    for _ in 0..count {
        let key = read_value(&mut file)?;

        let mut value_buf = [0u8; 8];
        file.read_exact(&mut value_buf)?;
        let value = u64::from_le_bytes(value_buf);

        tree.insert(key, value)?;
    }

    Ok(tree)
}

/// Write a Value to the file
fn write_value(file: &mut File, value: &Value) -> Result<()> {
    let bytes = value.to_bytes();
    file.write_all(&(bytes.len() as u32).to_le_bytes())?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Read a Value from the file
fn read_value(file: &mut File) -> Result<Value> {
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut value_bytes = vec![0u8; len];
    file.read_exact(&mut value_bytes)?;

    let (value, _) = Value::from_bytes(&value_bytes)?;
    Ok(value)
}

/// LRU cache for B-Tree nodes
///
/// Keeps frequently accessed nodes in memory to avoid disk I/O
pub struct NodeCache<K, V>
where
    K: Clone + PartialOrd + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
{
    cache: HashMap<u64, BTreeNode<K, V>>,
    lru_list: VecDeque<u64>,
    capacity: usize,
}

impl<K, V> NodeCache<K, V>
where
    K: Clone + PartialOrd + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
{
    /// Create a new node cache with given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: HashMap::new(),
            lru_list: VecDeque::new(),
            capacity,
        }
    }

    /// Get a node from cache
    ///
    /// Returns Some(node) if found, updates LRU order
    pub fn get(&mut self, node_id: u64) -> Option<&BTreeNode<K, V>> {
        if self.cache.contains_key(&node_id) {
            // Update LRU - move to back
            self.lru_list.retain(|&id| id != node_id);
            self.lru_list.push_back(node_id);
            self.cache.get(&node_id)
        } else {
            None
        }
    }

    /// Insert a node into cache
    ///
    /// Evicts least recently used node if at capacity
    pub fn insert(&mut self, node_id: u64, node: BTreeNode<K, V>) {
        // If already exists, remove from LRU list
        if self.cache.contains_key(&node_id) {
            self.lru_list.retain(|&id| id != node_id);
        }

        // Check if we need to evict
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&node_id) {
            if let Some(evict_id) = self.lru_list.pop_front() {
                self.cache.remove(&evict_id);
            }
        }

        // Insert node
        self.cache.insert(node_id, node);
        self.lru_list.push_back(node_id);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            capacity: self.capacity,
            hit_rate: 0.0, // Would need hit/miss tracking
        }
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru_list.clear();
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_save_and_load_index() {
        let path = "/tmp/test_index.idx";
        let _ = fs::remove_file(path);

        // Create and populate tree
        let mut tree = BTree::new(5).unwrap();
        for i in vec![10, 5, 15, 3, 7, 12, 18, 1, 20] {
            tree.insert(Value::Int32(i), i as u64).unwrap();
        }

        // Save to file
        save_index(&tree, path).unwrap();

        // Load from file
        let loaded_tree = load_index(path).unwrap();

        // Verify data
        let original_data = tree.scan_all();
        let loaded_data = loaded_tree.scan_all();

        assert_eq!(original_data.len(), loaded_data.len());
        for (orig, loaded) in original_data.iter().zip(loaded_data.iter()) {
            assert_eq!(orig.0, loaded.0);
            assert_eq!(orig.1, loaded.1);
        }

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_empty_tree() {
        let path = "/tmp/test_empty_index.idx";
        let _ = fs::remove_file(path);

        let tree: BTree<Value, u64> = BTree::new(5).unwrap();
        save_index(&tree, path).unwrap();

        let loaded = load_index(path).unwrap();
        assert_eq!(loaded.len(), 0);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_save_large_tree() {
        let path = "/tmp/test_large_index.idx";
        let _ = fs::remove_file(path);

        let mut tree = BTree::new(100).unwrap();
        for i in 0..1000 {
            tree.insert(Value::Int64(i), i as u64).unwrap();
        }

        save_index(&tree, path).unwrap();
        let loaded = load_index(path).unwrap();

        assert_eq!(loaded.len(), 1000);

        // Spot check some values
        assert_eq!(loaded.search(&Value::Int64(500)), vec![500]);
        assert_eq!(loaded.search(&Value::Int64(0)), vec![0]);
        assert_eq!(loaded.search(&Value::Int64(999)), vec![999]);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_load_nonexistent() {
        let result = load_index("/tmp/nonexistent_index.idx");
        assert!(result.is_err());
    }

    #[test]
    fn test_node_cache_basic() {
        let mut cache: NodeCache<i32, String> = NodeCache::new(3);

        let node1 = BTreeNode::new_leaf(1);
        let node2 = BTreeNode::new_leaf(2);
        let node3 = BTreeNode::new_leaf(3);

        cache.insert(1, node1);
        cache.insert(2, node2);
        cache.insert(3, node3);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn test_node_cache_eviction() {
        let mut cache: NodeCache<i32, String> = NodeCache::new(2);

        cache.insert(1, BTreeNode::new_leaf(1));
        cache.insert(2, BTreeNode::new_leaf(2));

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());

        // Insert third node, should evict least recently used (1)
        cache.insert(3, BTreeNode::new_leaf(3));

        assert!(cache.get(1).is_none()); // Evicted
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn test_node_cache_lru_update() {
        let mut cache: NodeCache<i32, String> = NodeCache::new(2);

        cache.insert(1, BTreeNode::new_leaf(1));
        cache.insert(2, BTreeNode::new_leaf(2));

        // Access node 1, making it more recent
        cache.get(1);

        // Insert node 3, should evict node 2 (least recent)
        cache.insert(3, BTreeNode::new_leaf(3));

        assert!(cache.get(1).is_some()); // Still there
        assert!(cache.get(2).is_none()); // Evicted
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn test_cache_stats() {
        let mut cache: NodeCache<i32, String> = NodeCache::new(5);

        cache.insert(1, BTreeNode::new_leaf(1));
        cache.insert(2, BTreeNode::new_leaf(2));

        let stats = cache.stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.capacity, 5);
    }

    #[test]
    fn test_cache_clear() {
        let mut cache: NodeCache<i32, String> = NodeCache::new(5);

        cache.insert(1, BTreeNode::new_leaf(1));
        cache.insert(2, BTreeNode::new_leaf(2));

        cache.clear();

        assert_eq!(cache.stats().size, 0);
        assert!(cache.get(1).is_none());
        assert!(cache.get(2).is_none());
    }
}
