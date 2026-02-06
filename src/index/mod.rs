// Index layer - Phase 2
//
// B-Tree indexing implementation for fast lookups and range queries

pub mod node;
pub mod btree;
pub mod persist;
pub mod manager;
pub mod lazy;

pub use btree::{BTree, BTreeStats};
pub use node::{BTreeNode, NodeType};
