// Index layer - Phase 2
//
// B-Tree indexing implementation for fast lookups and range queries

pub mod node;
pub mod btree;
pub mod persist;
pub mod manager;
pub mod lazy;
pub mod like;
pub mod stats;

pub use btree::{BTree, BTreeStats};
pub use node::{BTreeNode, NodeType};
pub use persist::{save_index, load_index, NodeCache, CacheStats};
pub use manager::{IndexManager, IndexManagerStats, IndexInfo};
pub use like::LikePattern;
pub use stats::IndexStatistics;
