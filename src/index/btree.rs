use crate::error::{Error, Result};
use crate::index::node::BTreeNode;
use std::collections::HashMap;
use std::fmt::Debug;

/// B-Tree index structure
///
/// Generic in-memory B-Tree implementation supporting:
/// - Insert with automatic node splitting
/// - Exact search
/// - Range queries
/// - Duplicate key handling
///
/// For ThunderDB: K = Value (column value), V = u64 (row_id)
#[derive(Debug, Clone)]
pub struct BTree<K, V>
where
    K: Clone + PartialOrd + Debug,
    V: Clone + Debug,
{
    /// Root node ID
    root_id: u64,

    /// B-Tree order (maximum number of children per node)
    order: usize,

    /// All nodes indexed by node_id
    nodes: HashMap<u64, BTreeNode<K, V>>,

    /// Next available node ID
    next_node_id: u64,

    /// First leaf node (for sequential scans)
    first_leaf_id: Option<u64>,
}

impl<K, V> BTree<K, V>
where
    K: Clone + PartialOrd + Debug,
    V: Clone + Debug,
{
    /// Create a new empty B-Tree
    ///
    /// # Arguments
    /// * `order` - Maximum number of children per node (must be >= 3)
    pub fn new(order: usize) -> Result<Self> {
        if order < 3 {
            return Err(Error::Index("B-Tree order must be at least 3".to_string()));
        }

        let root_id = 0;
        let root = BTreeNode::new_leaf(root_id);

        let mut nodes = HashMap::new();
        nodes.insert(root_id, root);

        Ok(Self {
            root_id,
            order,
            nodes,
            next_node_id: 1,
            first_leaf_id: Some(root_id),
        })
    }

    /// Insert a key-value pair into the B-Tree
    ///
    /// Supports duplicate keys by storing multiple values
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        // Find the leaf node where key should go
        let leaf_id = self.find_leaf(&key);

        // Try to insert into the leaf
        let should_split = {
            let leaf = self.nodes.get_mut(&leaf_id).unwrap();
            if leaf.is_full(self.order) {
                true
            } else {
                leaf.insert_leaf(key.clone(), value.clone(), self.order);
                false
            }
        };

        if should_split {
            self.split_and_insert_leaf(leaf_id, key, value)?;
        }

        Ok(())
    }

    /// Search for all values matching a key
    ///
    /// Returns vector of values (supports duplicates)
    pub fn search(&self, key: &K) -> Vec<V> {
        let leaf_id = self.find_leaf(key);
        let leaf = self.nodes.get(&leaf_id).unwrap();

        let mut results = Vec::new();

        // Find first occurrence (binary_search might return any match for duplicates)
        let pos = leaf.find_position(key);

        // Backtrack to find the first occurrence
        let mut start_pos = pos;
        while start_pos > 0 {
            if leaf.keys[start_pos - 1].partial_cmp(key).unwrap() == std::cmp::Ordering::Equal {
                start_pos -= 1;
            } else {
                break;
            }
        }

        // Collect all matching values
        for i in start_pos..leaf.keys.len() {
            if leaf.keys[i].partial_cmp(key).unwrap() == std::cmp::Ordering::Equal {
                results.push(leaf.values[i].clone());
            } else {
                break;
            }
        }

        results
    }

    /// Range scan: find all values where start_key <= key <= end_key
    ///
    /// # Arguments
    /// * `start_key` - Inclusive start of range
    /// * `end_key` - Inclusive end of range
    ///
    /// # Returns
    /// Vector of (key, value) pairs in sorted order
    pub fn range_scan(&self, start_key: &K, end_key: &K) -> Vec<(K, V)> {
        let mut results = Vec::new();

        // Find starting leaf
        let mut current_leaf_id = self.find_leaf(start_key);

        loop {
            let leaf = match self.nodes.get(&current_leaf_id) {
                Some(node) => node,
                None => break,
            };

            // Scan this leaf
            for i in 0..leaf.keys.len() {
                let key = &leaf.keys[i];

                // Check if key is in range
                if key.partial_cmp(start_key).unwrap() >= std::cmp::Ordering::Equal
                    && key.partial_cmp(end_key).unwrap() <= std::cmp::Ordering::Equal
                {
                    results.push((key.clone(), leaf.values[i].clone()));
                } else if key.partial_cmp(end_key).unwrap() > std::cmp::Ordering::Equal {
                    // Past the end, we're done
                    return results;
                }
            }

            // Move to next leaf
            match leaf.next_leaf {
                Some(next_id) => current_leaf_id = next_id,
                None => break,
            }
        }

        results
    }

    /// Get all entries in sorted order
    pub fn scan_all(&self) -> Vec<(K, V)> {
        let mut results = Vec::new();

        if let Some(first_id) = self.first_leaf_id {
            let mut current_id = first_id;

            loop {
                let leaf = match self.nodes.get(&current_id) {
                    Some(node) => node,
                    None => break,
                };

                for i in 0..leaf.keys.len() {
                    results.push((leaf.keys[i].clone(), leaf.values[i].clone()));
                }

                match leaf.next_leaf {
                    Some(next_id) => current_id = next_id,
                    None => break,
                }
            }
        }

        results
    }

    /// Get the number of entries in the tree
    pub fn len(&self) -> usize {
        self.scan_all().len()
    }

    /// Check if tree is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Find the leaf node where a key should be located
    fn find_leaf(&self, key: &K) -> u64 {
        let mut current_id = self.root_id;

        loop {
            let node = self.nodes.get(&current_id).unwrap();

            if node.is_leaf() {
                return current_id;
            }

            // Internal node - find which child to follow
            // For internal nodes: if key >= keys[i], we go to the right child
            let mut child_idx = 0;
            for (i, node_key) in node.keys.iter().enumerate() {
                match key.partial_cmp(node_key).unwrap() {
                    std::cmp::Ordering::Less => {
                        child_idx = i;
                        break;
                    }
                    _ => {
                        child_idx = i + 1;
                    }
                }
            }

            current_id = node.children[child_idx];
        }
    }

    /// Split a full leaf node and insert the key-value pair
    fn split_and_insert_leaf(&mut self, leaf_id: u64, key: K, value: V) -> Result<()> {
        let new_node_id = self.next_node_id;
        self.next_node_id += 1;

        // Split the leaf
        let (middle_key, right_node) = {
            let leaf = self.nodes.get_mut(&leaf_id).unwrap();
            leaf.split_leaf(new_node_id)
        };

        self.nodes.insert(new_node_id, right_node);

        // Insert the new key-value pair into appropriate node
        {
            let target_id = match key.partial_cmp(&middle_key).unwrap() {
                std::cmp::Ordering::Less => leaf_id,
                _ => new_node_id,
            };

            let target = self.nodes.get_mut(&target_id).unwrap();
            target.insert_leaf(key, value, self.order);
        }

        // Propagate middle key up to parent
        let parent_id = self.nodes.get(&leaf_id).unwrap().parent;
        match parent_id {
            Some(parent) => self.insert_into_parent(parent, middle_key, new_node_id)?,
            None => self.create_new_root(leaf_id, middle_key, new_node_id)?,
        }

        Ok(())
    }

    /// Insert a key into a parent internal node
    fn insert_into_parent(&mut self, parent_id: u64, key: K, right_child_id: u64) -> Result<()> {
        let should_split = {
            let parent = self.nodes.get_mut(&parent_id).unwrap();
            if parent.is_full(self.order) {
                true
            } else {
                parent.insert_internal(key.clone(), right_child_id, self.order);
                self.nodes.get_mut(&right_child_id).unwrap().parent = Some(parent_id);
                false
            }
        };

        if should_split {
            self.split_and_insert_internal(parent_id, key, right_child_id)?;
        }

        Ok(())
    }

    /// Split a full internal node and insert the key
    fn split_and_insert_internal(&mut self, node_id: u64, key: K, right_child_id: u64) -> Result<()> {
        let new_node_id = self.next_node_id;
        self.next_node_id += 1;

        // Split the internal node
        let (middle_key, right_node) = {
            let node = self.nodes.get_mut(&node_id).unwrap();
            node.split_internal(new_node_id)
        };

        // Update parent pointers for children of right node
        for child_id in &right_node.children {
            if let Some(child) = self.nodes.get_mut(child_id) {
                child.parent = Some(new_node_id);
            }
        }

        self.nodes.insert(new_node_id, right_node);

        // Insert the new key into appropriate node
        {
            let target_id = match key.partial_cmp(&middle_key).unwrap() {
                std::cmp::Ordering::Less => node_id,
                _ => new_node_id,
            };

            let target = self.nodes.get_mut(&target_id).unwrap();
            target.insert_internal(key, right_child_id, self.order);
            self.nodes.get_mut(&right_child_id).unwrap().parent = Some(target_id);
        }

        // Propagate middle key up to parent
        let grandparent_id = self.nodes.get(&node_id).unwrap().parent;
        match grandparent_id {
            Some(gp) => self.insert_into_parent(gp, middle_key, new_node_id)?,
            None => self.create_new_root(node_id, middle_key, new_node_id)?,
        }

        Ok(())
    }

    /// Create a new root node when the old root splits
    fn create_new_root(&mut self, left_id: u64, key: K, right_id: u64) -> Result<()> {
        let new_root_id = self.next_node_id;
        self.next_node_id += 1;

        let mut new_root = BTreeNode::new_internal(new_root_id);
        new_root.keys.push(key);
        new_root.children.push(left_id);
        new_root.children.push(right_id);

        // Update parent pointers
        self.nodes.get_mut(&left_id).unwrap().parent = Some(new_root_id);
        self.nodes.get_mut(&right_id).unwrap().parent = Some(new_root_id);

        self.nodes.insert(new_root_id, new_root);
        self.root_id = new_root_id;

        Ok(())
    }

    /// Get tree statistics for debugging
    pub fn stats(&self) -> BTreeStats {
        let mut leaf_count = 0;
        let mut internal_count = 0;
        let mut total_keys = 0;

        for node in self.nodes.values() {
            if node.is_leaf() {
                leaf_count += 1;
                total_keys += node.key_count();
            } else {
                internal_count += 1;
            }
        }

        BTreeStats {
            order: self.order,
            node_count: self.nodes.len(),
            leaf_count,
            internal_count,
            total_keys,
            height: self.calculate_height(),
        }
    }

    /// Calculate tree height
    fn calculate_height(&self) -> usize {
        let mut height = 0;
        let mut current_id = self.root_id;

        loop {
            let node = self.nodes.get(&current_id).unwrap();
            height += 1;

            if node.is_leaf() {
                break;
            }

            current_id = node.children[0];
        }

        height
    }
}

/// B-Tree statistics for debugging and optimization
#[derive(Debug, Clone)]
pub struct BTreeStats {
    pub order: usize,
    pub node_count: usize,
    pub leaf_count: usize,
    pub internal_count: usize,
    pub total_keys: usize,
    pub height: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_create() {
        let tree: BTree<i32, String> = BTree::new(5).unwrap();
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_btree_invalid_order() {
        let result: Result<BTree<i32, String>> = BTree::new(2);
        assert!(result.is_err());
    }

    #[test]
    fn test_btree_insert_and_search() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(10, "ten".to_string()).unwrap();
        tree.insert(5, "five".to_string()).unwrap();
        tree.insert(15, "fifteen".to_string()).unwrap();

        assert_eq!(tree.search(&10), vec!["ten".to_string()]);
        assert_eq!(tree.search(&5), vec!["five".to_string()]);
        assert_eq!(tree.search(&15), vec!["fifteen".to_string()]);
        assert_eq!(tree.search(&99), Vec::<String>::new());
    }

    #[test]
    fn test_btree_duplicates() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(10, "first".to_string()).unwrap();
        tree.insert(10, "second".to_string()).unwrap();
        tree.insert(10, "third".to_string()).unwrap();

        let results = tree.search(&10);
        assert_eq!(results.len(), 3);
        assert!(results.contains(&"first".to_string()));
        assert!(results.contains(&"second".to_string()));
        assert!(results.contains(&"third".to_string()));
    }

    #[test]
    fn test_btree_range_scan() {
        let mut tree = BTree::new(5).unwrap();

        for i in vec![10, 5, 15, 3, 7, 12, 18, 1, 20] {
            tree.insert(i, format!("val{}", i)).unwrap();
        }

        let results = tree.range_scan(&5, &15);
        let keys: Vec<i32> = results.iter().map(|(k, _)| *k).collect();

        assert_eq!(keys, vec![5, 7, 10, 12, 15]);
    }

    #[test]
    fn test_btree_scan_all() {
        let mut tree = BTree::new(5).unwrap();

        let values = vec![10, 5, 15, 3, 7, 12, 18];
        for val in values {
            tree.insert(val, format!("val{}", val)).unwrap();
        }

        let results = tree.scan_all();
        let keys: Vec<i32> = results.iter().map(|(k, _)| *k).collect();

        assert_eq!(keys, vec![3, 5, 7, 10, 12, 15, 18]);
    }

    #[test]
    fn test_btree_split() {
        let mut tree = BTree::new(3).unwrap(); // Small order to force splits

        // Insert enough values to cause splits
        for i in 1..=10 {
            tree.insert(i, format!("val{}", i)).unwrap();
        }

        // Verify all values are still searchable
        for i in 1..=10 {
            let results = tree.search(&i);
            assert_eq!(results, vec![format!("val{}", i)]);
        }

        let stats = tree.stats();
        assert!(stats.height > 1); // Should have split
        assert_eq!(stats.total_keys, 10);
    }

    #[test]
    fn test_btree_large_dataset() {
        let mut tree = BTree::new(100).unwrap();

        // Insert 1000 values
        for i in 0..1000 {
            tree.insert(i, i as u64).unwrap();
        }

        assert_eq!(tree.len(), 1000);

        // Random lookups
        assert_eq!(tree.search(&500), vec![500u64]);
        assert_eq!(tree.search(&999), vec![999u64]);
        assert_eq!(tree.search(&0), vec![0u64]);

        // Range query
        let results = tree.range_scan(&100, &110);
        assert_eq!(results.len(), 11); // 100 to 110 inclusive
    }

    #[test]
    fn test_btree_stats() {
        let mut tree = BTree::new(5).unwrap();

        for i in 1..=20 {
            tree.insert(i, i).unwrap();
        }

        let stats = tree.stats();
        assert_eq!(stats.order, 5);
        assert_eq!(stats.total_keys, 20);
        assert!(stats.node_count > 0);
        assert!(stats.leaf_count > 0);
    }

    #[test]
    fn test_btree_empty_range() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(10, "ten".to_string()).unwrap();
        tree.insert(20, "twenty".to_string()).unwrap();

        // Range with no matching values
        let results = tree.range_scan(&12, &18);
        assert_eq!(results.len(), 0);
    }
}
