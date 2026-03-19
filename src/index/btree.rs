use crate::error::{Error, Result};
use crate::index::node::BTreeNode;
use std::fmt::Debug;

/// B-Tree index structure
///
/// Generic in-memory B-Tree implementation supporting:
/// - Insert with automatic node splitting
/// - Exact search
/// - Range queries
/// - Duplicate key handling
///
/// Nodes are stored in a Vec-based arena for cache-friendly access.
/// For ThunderDB: K = Value (column value), V = u64 (row_id)
#[derive(Debug, Clone)]
pub struct BTree<K, V>
where
    K: Clone + Ord + Debug,
    V: Clone + Debug,
{
    /// Root node ID
    root_id: u64,

    /// B-Tree order (maximum number of children per node)
    order: usize,

    /// All nodes stored in a Vec arena (index = node_id)
    nodes: Vec<BTreeNode<K, V>>,

    /// Next available node ID
    next_node_id: u64,

    /// First leaf node (for sequential scans)
    first_leaf_id: Option<u64>,

    /// Number of key-value entries in the tree
    entry_count: usize,
}

impl<K, V> BTree<K, V>
where
    K: Clone + Ord + Debug,
    V: Clone + Debug + PartialEq,
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

        let mut nodes = Vec::new();
        nodes.push(root);

        Ok(Self {
            root_id,
            order,
            nodes,
            next_node_id: 1,
            first_leaf_id: Some(root_id),
            entry_count: 0,
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
            let leaf = &mut self.nodes[leaf_id as usize];
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

        self.entry_count += 1;

        Ok(())
    }

    /// Search for all values matching a key
    ///
    /// Returns vector of values (supports duplicates)
    pub fn search(&self, key: &K) -> Vec<V> {
        let leaf_id = self.find_leaf(key);
        let leaf = &self.nodes[leaf_id as usize];

        let mut results = Vec::new();

        // Find first occurrence (binary_search might return any match for duplicates)
        let pos = leaf.find_position(key);

        // Backtrack to find the first occurrence
        let mut start_pos = pos;
        while start_pos > 0 {
            if leaf.keys[start_pos - 1].cmp(key) == std::cmp::Ordering::Equal {
                start_pos -= 1;
            } else {
                break;
            }
        }

        // Collect all matching values
        for i in start_pos..leaf.keys.len() {
            if leaf.keys[i].cmp(key) == std::cmp::Ordering::Equal {
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
            let leaf = match self.nodes.get(current_leaf_id as usize) {
                Some(node) => node,
                None => break,
            };

            // Scan this leaf
            for i in 0..leaf.keys.len() {
                let key = &leaf.keys[i];

                // Check if key is in range
                if key.cmp(start_key) >= std::cmp::Ordering::Equal
                    && key.cmp(end_key) <= std::cmp::Ordering::Equal
                {
                    results.push((key.clone(), leaf.values[i].clone()));
                } else if key.cmp(end_key) > std::cmp::Ordering::Equal {
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

    /// Scan from start_key to the end of the tree
    pub fn scan_from(&self, start_key: &K) -> Vec<(K, V)> {
        let mut results = Vec::new();
        let mut current_leaf_id = self.find_leaf(start_key);

        loop {
            let leaf = match self.nodes.get(current_leaf_id as usize) {
                Some(node) => node,
                None => break,
            };

            for i in 0..leaf.keys.len() {
                let key = &leaf.keys[i];
                if key.cmp(start_key) >= std::cmp::Ordering::Equal {
                    results.push((key.clone(), leaf.values[i].clone()));
                }
            }

            match leaf.next_leaf {
                Some(next_id) => current_leaf_id = next_id,
                None => break,
            }
        }
        results
    }

    /// Scan from the beginning of the tree up to end_key
    pub fn scan_to(&self, end_key: &K) -> Vec<(K, V)> {
        let mut results = Vec::new();
        let mut current_id = match self.first_leaf_id {
            Some(id) => id,
            None => return results,
        };

        loop {
            let leaf = match self.nodes.get(current_id as usize) {
                Some(node) => node,
                None => break,
            };

            for i in 0..leaf.keys.len() {
                let key = &leaf.keys[i];
                if key.cmp(end_key) <= std::cmp::Ordering::Equal {
                    results.push((key.clone(), leaf.values[i].clone()));
                } else {
                    return results; // Past the end
                }
            }

            match leaf.next_leaf {
                Some(next_id) => current_id = next_id,
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
                let leaf = match self.nodes.get(current_id as usize) {
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

    /// Get the number of entries in the tree — O(1)
    pub fn len(&self) -> usize {
        self.entry_count
    }

    /// Check if tree is empty — O(1)
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Find the leaf node where a key should be located
    fn find_leaf(&self, key: &K) -> u64 {
        let mut current_id = self.root_id;

        loop {
            let node = &self.nodes[current_id as usize];

            if node.is_leaf() {
                return current_id;
            }

            // Internal node - find which child to follow
            let pos = node.find_position(key);

            let mut child_idx = pos;
            if pos < node.keys.len() && node.keys[pos].cmp(key) == std::cmp::Ordering::Equal {
                child_idx = pos + 1;
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
            let leaf = &mut self.nodes[leaf_id as usize];
            leaf.split_leaf(new_node_id)
        };

        debug_assert_eq!(new_node_id as usize, self.nodes.len());
        self.nodes.push(right_node);

        // Insert the new key-value pair into appropriate node
        {
            let target_id = match key.cmp(&middle_key) {
                std::cmp::Ordering::Less => leaf_id,
                _ => new_node_id,
            };

            let target = &mut self.nodes[target_id as usize];
            target.insert_leaf(key, value, self.order);
        }

        // Propagate middle key up to parent
        let parent_id = self.nodes[leaf_id as usize].parent;
        match parent_id {
            Some(parent) => self.insert_into_parent(parent, middle_key, new_node_id)?,
            None => self.create_new_root(leaf_id, middle_key, new_node_id)?,
        }

        Ok(())
    }

    /// Insert a key into a parent internal node
    fn insert_into_parent(&mut self, parent_id: u64, key: K, right_child_id: u64) -> Result<()> {
        let should_split = {
            let parent = &mut self.nodes[parent_id as usize];
            if parent.is_full(self.order) {
                true
            } else {
                parent.insert_internal(key.clone(), right_child_id, self.order);
                self.nodes[right_child_id as usize].parent = Some(parent_id);
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
            let node = &mut self.nodes[node_id as usize];
            node.split_internal(new_node_id)
        };

        // Update parent pointers for children of right node
        for child_id in &right_node.children.clone() {
            self.nodes[*child_id as usize].parent = Some(new_node_id);
        }

        debug_assert_eq!(new_node_id as usize, self.nodes.len());
        self.nodes.push(right_node);

        // Insert the new key into appropriate node
        {
            let target_id = match key.cmp(&middle_key) {
                std::cmp::Ordering::Less => node_id,
                _ => new_node_id,
            };

            let target = &mut self.nodes[target_id as usize];
            target.insert_internal(key, right_child_id, self.order);
            self.nodes[right_child_id as usize].parent = Some(target_id);
        }

        // Propagate middle key up to parent
        let grandparent_id = self.nodes[node_id as usize].parent;
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
        self.nodes[left_id as usize].parent = Some(new_root_id);
        self.nodes[right_id as usize].parent = Some(new_root_id);

        debug_assert_eq!(new_root_id as usize, self.nodes.len());
        self.nodes.push(new_root);
        self.root_id = new_root_id;

        Ok(())
    }

    /// Delete a key-value pair from the tree (lazy — no rebalancing)
    ///
    /// Walks to the leaf via find_leaf, scans for the matching key+value pair,
    /// and removes it. Underflow is harmless since the tree is rebuilt on load.
    ///
    /// # Returns
    /// `true` if the pair was found and removed, `false` otherwise
    pub fn delete(&mut self, key: &K, value: &V) -> bool {
        let leaf_id = self.find_leaf(key);
        let leaf = &mut self.nodes[leaf_id as usize];

        // Scan for matching key+value pair
        for i in 0..leaf.keys.len() {
            if leaf.keys[i].cmp(key) == std::cmp::Ordering::Equal
                && leaf.values[i] == *value
            {
                leaf.keys.remove(i);
                leaf.values.remove(i);
                self.entry_count -= 1;
                return true;
            }
        }

        false
    }

    /// Get the root node ID
    pub fn root_id(&self) -> u64 {
        self.root_id
    }

    /// Get the tree order
    pub fn order(&self) -> usize {
        self.order
    }

    /// Get the first leaf node ID
    pub fn first_leaf_id(&self) -> Option<u64> {
        self.first_leaf_id
    }

    /// Get the entry count
    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    /// Get a reference to all nodes in the arena
    pub fn nodes(&self) -> &[BTreeNode<K, V>] {
        &self.nodes
    }

    /// Reconstruct a BTree from its constituent parts
    ///
    /// Used by the v2 persist format to directly deserialize tree structure.
    pub fn from_parts(
        order: usize,
        root_id: u64,
        first_leaf_id: Option<u64>,
        entry_count: usize,
        nodes: Vec<BTreeNode<K, V>>,
    ) -> Result<Self> {
        if order < 3 {
            return Err(Error::Index("B-Tree order must be at least 3".to_string()));
        }

        let next_node_id = nodes.len() as u64;

        Ok(Self {
            root_id,
            order,
            nodes,
            next_node_id,
            first_leaf_id,
            entry_count,
        })
    }

    /// Get tree statistics for debugging
    pub fn stats(&self) -> BTreeStats {
        let mut leaf_count = 0;
        let mut internal_count = 0;
        let mut total_keys = 0;

        for node in self.nodes.iter() {
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
            let node = &self.nodes[current_id as usize];
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

    #[test]
    fn test_btree_scan_from_to() {
        let mut tree = BTree::new(5).unwrap();
        for i in vec![10, 20, 30, 40, 50] {
            tree.insert(i, i).unwrap();
        }

        // scan_from(25) -> 30, 40, 50
        let results = tree.scan_from(&25);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, 30);

        // scan_to(35) -> 10, 20, 30
        let results = tree.scan_to(&35);
        assert_eq!(results.len(), 3);
        assert_eq!(results[2].0, 30);
    }

    #[test]
    fn test_btree_len_is_o1() {
        let mut tree = BTree::new(5).unwrap();
        assert_eq!(tree.len(), 0);

        for i in 0..100 {
            tree.insert(i, i as u64).unwrap();
            assert_eq!(tree.len(), (i + 1) as usize);
        }
    }

    #[test]
    fn test_btree_delete_basic() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(10, 1u64).unwrap();
        tree.insert(20, 2u64).unwrap();
        tree.insert(30, 3u64).unwrap();

        assert_eq!(tree.len(), 3);

        // Delete existing pair
        assert!(tree.delete(&20, &2u64));
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.search(&20), Vec::<u64>::new());

        // Delete non-existent value for existing key
        assert!(!tree.delete(&10, &999u64));
        assert_eq!(tree.len(), 2);

        // Delete non-existent key
        assert!(!tree.delete(&99, &1u64));
        assert_eq!(tree.len(), 2);
    }

    #[test]
    fn test_btree_delete_with_duplicates() {
        let mut tree = BTree::new(5).unwrap();

        tree.insert(10, 1u64).unwrap();
        tree.insert(10, 2u64).unwrap();
        tree.insert(10, 3u64).unwrap();

        assert_eq!(tree.len(), 3);

        // Delete one duplicate
        assert!(tree.delete(&10, &2u64));
        assert_eq!(tree.len(), 2);

        let results = tree.search(&10);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1u64));
        assert!(results.contains(&3u64));
    }

    #[test]
    fn test_btree_from_parts() {
        let mut tree = BTree::new(5).unwrap();
        for i in 1..=10 {
            tree.insert(i, i as u64).unwrap();
        }

        let root_id = tree.root_id();
        let order = tree.order();
        let first_leaf_id = tree.first_leaf_id();
        let entry_count = tree.entry_count();
        let nodes = tree.nodes().to_vec();

        let rebuilt = BTree::from_parts(order, root_id, first_leaf_id, entry_count, nodes).unwrap();

        assert_eq!(rebuilt.len(), 10);
        for i in 1..=10 {
            assert_eq!(rebuilt.search(&i), vec![i as u64]);
        }
    }
}
