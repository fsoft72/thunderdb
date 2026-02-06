use std::fmt::Debug;

/// B-Tree node type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    /// Leaf node (contains data)
    Leaf,
    /// Internal node (contains only keys for navigation)
    Internal,
}

/// B-Tree node structure
///
/// Generic over key type K and value type V.
/// For our use case: K = Value (column value), V = u64 (row_id)
#[derive(Debug, Clone)]
pub struct BTreeNode<K, V>
where
    K: Clone + PartialOrd + Debug,
    V: Clone + Debug,
{
    /// Unique node identifier
    pub node_id: u64,

    /// Node type (leaf or internal)
    pub node_type: NodeType,

    /// Keys stored in this node (sorted)
    pub keys: Vec<K>,

    /// Values for leaf nodes (parallel to keys)
    /// For leaf: each key maps to a value
    /// For internal: empty
    pub values: Vec<V>,

    /// Child node IDs for internal nodes
    /// For internal: len(children) = len(keys) + 1
    /// For leaf: empty
    pub children: Vec<u64>,

    /// Parent node ID (None for root)
    pub parent: Option<u64>,

    /// Next leaf node (for leaf nodes, enables range scans)
    pub next_leaf: Option<u64>,
}

impl<K, V> BTreeNode<K, V>
where
    K: Clone + PartialOrd + Debug,
    V: Clone + Debug,
{
    /// Create a new leaf node
    pub fn new_leaf(node_id: u64) -> Self {
        Self {
            node_id,
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            parent: None,
            next_leaf: None,
        }
    }

    /// Create a new internal node
    pub fn new_internal(node_id: u64) -> Self {
        Self {
            node_id,
            node_type: NodeType::Internal,
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            parent: None,
            next_leaf: None,
        }
    }

    /// Check if node is a leaf
    pub fn is_leaf(&self) -> bool {
        self.node_type == NodeType::Leaf
    }

    /// Check if node is full (ready to split)
    ///
    /// # Arguments
    /// * `order` - Maximum number of children per node
    pub fn is_full(&self, order: usize) -> bool {
        if self.is_leaf() {
            self.keys.len() >= order
        } else {
            self.children.len() >= order
        }
    }

    /// Get the number of keys in this node
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Find the position where a key should be inserted (binary search)
    ///
    /// Returns the index where key should go
    pub fn find_position(&self, key: &K) -> usize {
        match self.keys.binary_search_by(|k| k.partial_cmp(key).unwrap()) {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
    }

    /// Insert a key-value pair into a leaf node
    ///
    /// Returns true if successful, false if node is full
    pub fn insert_leaf(&mut self, key: K, value: V, order: usize) -> bool {
        if self.is_full(order) {
            return false;
        }

        let pos = self.find_position(&key);
        self.keys.insert(pos, key);
        self.values.insert(pos, value);
        true
    }

    /// Insert a key and child pointer into an internal node
    ///
    /// Returns true if successful, false if node is full
    pub fn insert_internal(&mut self, key: K, child_id: u64, order: usize) -> bool {
        if self.is_full(order) {
            return false;
        }

        let pos = self.find_position(&key);
        self.keys.insert(pos, key);
        self.children.insert(pos + 1, child_id);
        true
    }

    /// Split a leaf node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split_leaf(&mut self, new_node_id: u64) -> (K, Self) {
        let mid = self.keys.len() / 2;

        // Create new right node
        let mut right_node = BTreeNode::new_leaf(new_node_id);
        right_node.parent = self.parent;

        // Move second half to right node
        right_node.keys = self.keys.split_off(mid);
        right_node.values = self.values.split_off(mid);

        // Update leaf pointers
        right_node.next_leaf = self.next_leaf;
        self.next_leaf = Some(new_node_id);

        let middle_key = right_node.keys[0].clone();

        (middle_key, right_node)
    }

    /// Split an internal node into two nodes
    ///
    /// Returns (middle_key, new_right_node)
    pub fn split_internal(&mut self, new_node_id: u64) -> (K, Self) {
        let mid = self.keys.len() / 2;

        // Create new right node
        let mut right_node = BTreeNode::new_internal(new_node_id);
        right_node.parent = self.parent;

        // Extract middle key (it moves up to parent)
        let middle_key = self.keys[mid].clone();

        // Split keys and children
        right_node.keys = self.keys.split_off(mid + 1);
        self.keys.truncate(mid); // Remove middle key from left node

        right_node.children = self.children.split_off(mid + 1);

        (middle_key, right_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Value;

    #[test]
    fn test_node_creation() {
        let leaf: BTreeNode<i32, String> = BTreeNode::new_leaf(1);
        assert!(leaf.is_leaf());
        assert_eq!(leaf.node_id, 1);
        assert_eq!(leaf.key_count(), 0);

        let internal: BTreeNode<i32, String> = BTreeNode::new_internal(2);
        assert!(!internal.is_leaf());
        assert_eq!(internal.node_id, 2);
    }

    #[test]
    fn test_leaf_insert() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_leaf(1);
        let order = 5;

        assert!(node.insert_leaf(10, "ten".to_string(), order));
        assert!(node.insert_leaf(5, "five".to_string(), order));
        assert!(node.insert_leaf(15, "fifteen".to_string(), order));

        assert_eq!(node.keys, vec![5, 10, 15]);
        assert_eq!(node.values, vec!["five".to_string(), "ten".to_string(), "fifteen".to_string()]);
    }

    #[test]
    fn test_leaf_full() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_leaf(1);
        let order = 3;

        assert!(node.insert_leaf(1, "one".to_string(), order));
        assert!(node.insert_leaf(2, "two".to_string(), order));
        assert!(node.insert_leaf(3, "three".to_string(), order));

        assert!(node.is_full(order));
        assert!(!node.insert_leaf(4, "four".to_string(), order));
    }

    #[test]
    fn test_find_position() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_leaf(1);
        node.keys = vec![10, 20, 30, 40];

        assert_eq!(node.find_position(&5), 0);
        assert_eq!(node.find_position(&15), 1);
        assert_eq!(node.find_position(&25), 2);
        assert_eq!(node.find_position(&45), 4);
        assert_eq!(node.find_position(&20), 1); // Exact match
    }

    #[test]
    fn test_leaf_split() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_leaf(1);
        let order = 5;

        for i in 1..=5 {
            node.insert_leaf(i * 10, format!("val{}", i), order);
        }

        let (middle_key, right_node) = node.split_leaf(2);

        assert_eq!(middle_key, 30);
        assert_eq!(node.keys, vec![10, 20]);
        assert_eq!(right_node.keys, vec![30, 40, 50]);
        assert_eq!(node.next_leaf, Some(2));
        assert_eq!(right_node.next_leaf, None);
    }

    #[test]
    fn test_internal_insert() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_internal(1);
        let order = 5;

        node.children.push(10); // Initial child

        assert!(node.insert_internal(20, 11, order));
        assert!(node.insert_internal(10, 12, order));

        assert_eq!(node.keys, vec![10, 20]);
        assert_eq!(node.children, vec![10, 12, 11]);
    }

    #[test]
    fn test_internal_split() {
        let mut node: BTreeNode<i32, String> = BTreeNode::new_internal(1);
        node.keys = vec![10, 20, 30, 40, 50];
        node.children = vec![1, 2, 3, 4, 5, 6];

        let (middle_key, right_node) = node.split_internal(2);

        assert_eq!(middle_key, 30);
        assert_eq!(node.keys, vec![10, 20]);
        assert_eq!(right_node.keys, vec![40, 50]);
        assert_eq!(node.children, vec![1, 2, 3]);
        assert_eq!(right_node.children, vec![4, 5, 6]);
    }

    #[test]
    fn test_node_with_value_type() {
        let mut node: BTreeNode<Value, u64> = BTreeNode::new_leaf(1);
        let order = 5;

        node.insert_leaf(Value::Int32(10), 100, order);
        node.insert_leaf(Value::Int32(5), 50, order);
        node.insert_leaf(Value::Int32(15), 150, order);

        assert_eq!(node.keys.len(), 3);
        assert_eq!(node.values, vec![50, 100, 150]);
    }
}
