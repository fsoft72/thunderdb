// B-Tree persistence - Step 2.2
//
// Binary serialization for B-Tree nodes with LRU cache
//
// v1 format: flat key-value pairs (rebuild tree on load)
// v2 format: serialized tree structure (direct deserialization, ~20x faster load)

use crate::error::{Error, Result};
use crate::index::btree::BTree;
use crate::index::node::{BTreeNode, NodeType};
use crate::storage::Value;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Magic number for index file format: "TIDX" (ThunderDB Index)
const INDEX_MAGIC: [u8; 4] = [b'T', b'I', b'D', b'X'];
const INDEX_VERSION_V1: u32 = 1;
const INDEX_VERSION_V2: u32 = 2;

/// Serialize a B-Tree to binary v2 format and save to file
///
/// v2 format serializes the tree structure directly instead of
/// flattening to key-value pairs, enabling O(n) deserialization
/// without re-insertion.
///
/// Format:
/// ```text
/// Header:
///   [magic: 4][version: 4][order: 4][root_id: 8][node_count: 8]
///   [first_leaf_id: 9 (1 flag + 8 id)][entry_count: 8]
///
/// Per node:
///   [node_type: 1]
///   [key_count: 4][keys...]
///   [value_count: 4][values...]       (leaf only)
///   [child_count: 4][children...]     (internal only)
///   [parent: 9 (1 flag + 8 id)]
///   [next_leaf: 9 (1 flag + 8 id)]
/// ```
pub fn save_index<P: AsRef<Path>>(tree: &BTree<Value, u64>, path: P) -> Result<()> {
    let path = path.as_ref();
    let mut file = File::create(path)?;

    let nodes = tree.nodes();
    let node_count = nodes.len() as u64;

    // Write header
    file.write_all(&INDEX_MAGIC)?;
    file.write_all(&INDEX_VERSION_V2.to_le_bytes())?;
    file.write_all(&(tree.order() as u32).to_le_bytes())?;
    file.write_all(&tree.root_id().to_le_bytes())?;
    file.write_all(&node_count.to_le_bytes())?;

    // Write first_leaf_id as optional: [1 byte flag][8 bytes id]
    write_optional_u64(&mut file, tree.first_leaf_id())?;

    // Write entry count
    file.write_all(&(tree.entry_count() as u64).to_le_bytes())?;

    // Write each node
    for node in nodes {
        // Node type: 0 = Leaf, 1 = Internal
        let type_byte: u8 = if node.is_leaf() { 0 } else { 1 };
        file.write_all(&[type_byte])?;

        // Keys
        file.write_all(&(node.keys.len() as u32).to_le_bytes())?;
        for key in &node.keys {
            write_value(&mut file, key)?;
        }

        if node.is_leaf() {
            // Values (leaf only)
            file.write_all(&(node.values.len() as u32).to_le_bytes())?;
            for val in &node.values {
                file.write_all(&val.to_le_bytes())?;
            }
        } else {
            // Children (internal only)
            file.write_all(&(node.children.len() as u32).to_le_bytes())?;
            for child in &node.children {
                file.write_all(&child.to_le_bytes())?;
            }
        }

        // Parent
        write_optional_u64(&mut file, node.parent)?;

        // Next leaf
        write_optional_u64(&mut file, node.next_leaf)?;
    }

    file.sync_all()?;
    Ok(())
}

/// Load a B-Tree from binary format (supports v1 and v2)
///
/// v1: reads flat key-value pairs and rebuilds tree via insert
/// v2: reads serialized tree structure directly (O(n))
pub fn load_index<P: AsRef<Path>>(path: P) -> Result<BTree<Value, u64>> {
    let path = path.as_ref();

    if !path.exists() {
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

    match version {
        INDEX_VERSION_V1 => load_index_v1(&mut file),
        INDEX_VERSION_V2 => load_index_v2(&mut file),
        _ => Err(Error::Index(format!("Unsupported index version: {}", version))),
    }
}

/// Load v1 format: flat key-value pairs, rebuild tree via insert
fn load_index_v1(file: &mut File) -> Result<BTree<Value, u64>> {
    let mut order_buf = [0u8; 4];
    file.read_exact(&mut order_buf)?;
    let order = u32::from_le_bytes(order_buf) as usize;

    // Skip root_id placeholder
    file.seek(SeekFrom::Current(8))?;

    let mut count_buf = [0u8; 8];
    file.read_exact(&mut count_buf)?;
    let count = u64::from_le_bytes(count_buf) as usize;

    let mut tree = BTree::new(order)?;

    for _ in 0..count {
        let key = read_value(file)?;
        let mut value_buf = [0u8; 8];
        file.read_exact(&mut value_buf)?;
        let value = u64::from_le_bytes(value_buf);
        tree.insert(key, value)?;
    }

    Ok(tree)
}

/// Load v2 format: deserialize tree structure directly
fn load_index_v2(file: &mut File) -> Result<BTree<Value, u64>> {
    let mut order_buf = [0u8; 4];
    file.read_exact(&mut order_buf)?;
    let order = u32::from_le_bytes(order_buf) as usize;

    let mut root_id_buf = [0u8; 8];
    file.read_exact(&mut root_id_buf)?;
    let root_id = u64::from_le_bytes(root_id_buf);

    let mut node_count_buf = [0u8; 8];
    file.read_exact(&mut node_count_buf)?;
    let node_count = u64::from_le_bytes(node_count_buf) as usize;

    let first_leaf_id = read_optional_u64(file)?;

    let mut entry_count_buf = [0u8; 8];
    file.read_exact(&mut entry_count_buf)?;
    let entry_count = u64::from_le_bytes(entry_count_buf) as usize;

    // Read all nodes
    let mut nodes = Vec::with_capacity(node_count);
    for node_idx in 0..node_count {
        let mut type_buf = [0u8; 1];
        file.read_exact(&mut type_buf)?;
        let is_leaf = type_buf[0] == 0;

        // Read keys
        let mut key_count_buf = [0u8; 4];
        file.read_exact(&mut key_count_buf)?;
        let key_count = u32::from_le_bytes(key_count_buf) as usize;

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            keys.push(read_value(file)?);
        }

        let mut values = Vec::new();
        let mut children = Vec::new();

        if is_leaf {
            // Read values
            let mut val_count_buf = [0u8; 4];
            file.read_exact(&mut val_count_buf)?;
            let val_count = u32::from_le_bytes(val_count_buf) as usize;

            values = Vec::with_capacity(val_count);
            for _ in 0..val_count {
                let mut val_buf = [0u8; 8];
                file.read_exact(&mut val_buf)?;
                values.push(u64::from_le_bytes(val_buf));
            }
        } else {
            // Read children
            let mut child_count_buf = [0u8; 4];
            file.read_exact(&mut child_count_buf)?;
            let child_count = u32::from_le_bytes(child_count_buf) as usize;

            children = Vec::with_capacity(child_count);
            for _ in 0..child_count {
                let mut child_buf = [0u8; 8];
                file.read_exact(&mut child_buf)?;
                children.push(u64::from_le_bytes(child_buf));
            }
        }

        // Read parent
        let parent = read_optional_u64(file)?;

        // Read next_leaf
        let next_leaf = read_optional_u64(file)?;

        let node = BTreeNode {
            node_id: node_idx as u64,
            node_type: if is_leaf { NodeType::Leaf } else { NodeType::Internal },
            keys,
            values,
            children,
            parent,
            next_leaf,
        };

        nodes.push(node);
    }

    BTree::from_parts(order, root_id, first_leaf_id, entry_count, nodes)
}

/// Write a Value to the file (length-prefixed)
fn write_value(file: &mut File, value: &Value) -> Result<()> {
    let bytes = value.to_bytes();
    file.write_all(&(bytes.len() as u32).to_le_bytes())?;
    file.write_all(&bytes)?;
    Ok(())
}

/// Read a Value from the file (length-prefixed)
fn read_value(file: &mut File) -> Result<Value> {
    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut value_bytes = vec![0u8; len];
    file.read_exact(&mut value_bytes)?;

    let (value, _) = Value::from_bytes(&value_bytes)?;
    Ok(value)
}

/// Write an Optional<u64> as [1 byte flag][8 bytes value]
fn write_optional_u64(file: &mut File, opt: Option<u64>) -> Result<()> {
    match opt {
        Some(val) => {
            file.write_all(&[1u8])?;
            file.write_all(&val.to_le_bytes())?;
        }
        None => {
            file.write_all(&[0u8])?;
            file.write_all(&0u64.to_le_bytes())?;
        }
    }
    Ok(())
}

/// Read an Optional<u64> from [1 byte flag][8 bytes value]
fn read_optional_u64(file: &mut File) -> Result<Option<u64>> {
    let mut flag = [0u8; 1];
    file.read_exact(&mut flag)?;

    let mut val_buf = [0u8; 8];
    file.read_exact(&mut val_buf)?;

    if flag[0] == 1 {
        Ok(Some(u64::from_le_bytes(val_buf)))
    } else {
        Ok(None)
    }
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
    fn test_save_and_load_index_v2() {
        let path = "/tmp/test_index_v2.idx";
        let _ = fs::remove_file(path);

        // Create and populate tree
        let mut tree = BTree::new(5).unwrap();
        for i in vec![10, 5, 15, 3, 7, 12, 18, 1, 20] {
            tree.insert(Value::Int32(i), i as u64).unwrap();
        }

        // Save to file (v2 format)
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

        // Verify tree structure is preserved (same search results)
        assert_eq!(loaded_tree.search(&Value::Int32(10)), vec![10u64]);
        assert_eq!(loaded_tree.search(&Value::Int32(5)), vec![5u64]);
        assert_eq!(loaded_tree.search(&Value::Int32(99)), Vec::<u64>::new());

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
    fn test_v2_preserves_tree_structure() {
        let path = "/tmp/test_v2_structure.idx";
        let _ = fs::remove_file(path);

        let mut tree = BTree::new(3).unwrap();
        // Insert enough to cause splits
        for i in 1..=20 {
            tree.insert(Value::Int32(i), i as u64).unwrap();
        }

        let original_stats = tree.stats();

        save_index(&tree, path).unwrap();
        let loaded = load_index(path).unwrap();

        let loaded_stats = loaded.stats();

        // Tree structure should be identical
        assert_eq!(original_stats.order, loaded_stats.order);
        assert_eq!(original_stats.node_count, loaded_stats.node_count);
        assert_eq!(original_stats.leaf_count, loaded_stats.leaf_count);
        assert_eq!(original_stats.internal_count, loaded_stats.internal_count);
        assert_eq!(original_stats.total_keys, loaded_stats.total_keys);
        assert_eq!(original_stats.height, loaded_stats.height);

        // Range queries should work
        let range = loaded.range_scan(&Value::Int32(5), &Value::Int32(15));
        let keys: Vec<i32> = range.iter().map(|(k, _)| {
            if let Value::Int32(v) = k { *v } else { panic!() }
        }).collect();
        assert_eq!(keys, vec![5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_v2_with_varchar_keys() {
        let path = "/tmp/test_v2_varchar.idx";
        let _ = fs::remove_file(path);

        let mut tree = BTree::new(5).unwrap();
        tree.insert(Value::varchar("alice".to_string()), 1).unwrap();
        tree.insert(Value::varchar("bob".to_string()), 2).unwrap();
        tree.insert(Value::varchar("charlie".to_string()), 3).unwrap();

        save_index(&tree, path).unwrap();
        let loaded = load_index(path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.search(&Value::varchar("alice".to_string())), vec![1]);
        assert_eq!(loaded.search(&Value::varchar("bob".to_string())), vec![2]);
        assert_eq!(loaded.search(&Value::varchar("charlie".to_string())), vec![3]);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_backward_compat_v1_load() {
        // Manually write a v1 format file
        let path = "/tmp/test_v1_compat.idx";
        let _ = fs::remove_file(path);

        {
            let mut file = File::create(path).unwrap();
            file.write_all(&INDEX_MAGIC).unwrap();
            file.write_all(&INDEX_VERSION_V1.to_le_bytes()).unwrap();
            file.write_all(&(5u32).to_le_bytes()).unwrap(); // order
            file.write_all(&0u64.to_le_bytes()).unwrap(); // root_id placeholder
            file.write_all(&3u64.to_le_bytes()).unwrap(); // count = 3

            // Write 3 key-value pairs
            for i in [10i32, 20, 30] {
                let val = Value::Int32(i);
                let bytes = val.to_bytes();
                file.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
                file.write_all(&bytes).unwrap();
                file.write_all(&(i as u64).to_le_bytes()).unwrap();
            }
            file.sync_all().unwrap();
        }

        // Load should work via v1 path
        let loaded = load_index(path).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.search(&Value::Int32(10)), vec![10]);
        assert_eq!(loaded.search(&Value::Int32(20)), vec![20]);
        assert_eq!(loaded.search(&Value::Int32(30)), vec![30]);

        fs::remove_file(path).ok();
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
