// Prepared statement cache - P2.4
//
// LRU cache mapping SQL strings to parsed Statements.
// Avoids re-tokenizing/re-parsing identical queries.

use crate::parser::ast::Statement;
use std::collections::{HashMap, VecDeque};

/// Default maximum number of cached statements
const DEFAULT_CAPACITY: usize = 128;

/// FNV-1a hash for fast string hashing
fn fnv1a_hash(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// LRU cache for parsed SQL statements
///
/// Maps SQL strings (via FNV-1a hash) to parsed `Statement` ASTs.
/// On cache hit, the entry is promoted to the back of the LRU queue.
/// When capacity is reached, the least-recently-used entry is evicted.
pub struct PreparedCache {
    /// Cached statements keyed by SQL hash
    entries: HashMap<u64, Statement>,
    /// LRU order: front = oldest, back = newest
    lru_order: VecDeque<u64>,
    /// Maximum number of entries
    capacity: usize,
}

impl PreparedCache {
    /// Create a new cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            lru_order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Look up a cached statement by SQL string
    ///
    /// Returns a cloned Statement on hit and promotes the entry in the LRU.
    pub fn get(&mut self, sql: &str) -> Option<Statement> {
        let key = fnv1a_hash(sql);
        if self.entries.contains_key(&key) {
            // Promote in LRU order
            self.lru_order.retain(|&k| k != key);
            self.lru_order.push_back(key);
            Some(self.entries[&key].clone())
        } else {
            None
        }
    }

    /// Insert a parsed statement into the cache
    ///
    /// If the cache is at capacity, evicts the least-recently-used entry.
    pub fn insert(&mut self, sql: &str, stmt: Statement) {
        let key = fnv1a_hash(sql);

        if self.entries.contains_key(&key) {
            // Update existing entry and promote
            self.entries.insert(key, stmt);
            self.lru_order.retain(|&k| k != key);
            self.lru_order.push_back(key);
            return;
        }

        // Evict if at capacity
        if self.entries.len() >= self.capacity {
            if let Some(oldest_key) = self.lru_order.pop_front() {
                self.entries.remove(&oldest_key);
            }
        }

        self.entries.insert(key, stmt);
        self.lru_order.push_back(key);
    }

    /// Clear the entire cache
    ///
    /// Should be called after DDL statements (CREATE TABLE, DROP TABLE)
    /// to invalidate any cached statements that might reference changed schema.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.lru_order.clear();
    }

    /// Get the number of cached entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for PreparedCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::*;
    use crate::storage::Value;

    fn make_select(table: &str) -> Statement {
        Statement::Select(SelectStatement {
            columns: vec![SelectColumn::Star],
            from: table.to_string(),
            where_clause: None,
            order_by: None,
            limit: None,
            offset: None,
        })
    }

    fn make_insert(table: &str) -> Statement {
        Statement::Insert(InsertStatement {
            table: table.to_string(),
            columns: None,
            values: vec![Value::Int32(1)],
        })
    }

    #[test]
    fn test_cache_miss() {
        let mut cache = PreparedCache::new(4);
        assert!(cache.get("SELECT * FROM users").is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_hit() {
        let mut cache = PreparedCache::new(4);
        let sql = "SELECT * FROM users";
        let stmt = make_select("users");

        cache.insert(sql, stmt.clone());
        assert_eq!(cache.len(), 1);

        let cached = cache.get(sql);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), stmt);
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = PreparedCache::new(3);

        cache.insert("SELECT * FROM a", make_select("a"));
        cache.insert("SELECT * FROM b", make_select("b"));
        cache.insert("SELECT * FROM c", make_select("c"));
        assert_eq!(cache.len(), 3);

        // Insert a 4th entry — should evict "a" (the oldest)
        cache.insert("SELECT * FROM d", make_select("d"));
        assert_eq!(cache.len(), 3);

        assert!(cache.get("SELECT * FROM a").is_none());
        assert!(cache.get("SELECT * FROM b").is_some());
        assert!(cache.get("SELECT * FROM c").is_some());
        assert!(cache.get("SELECT * FROM d").is_some());
    }

    #[test]
    fn test_lru_promotion() {
        let mut cache = PreparedCache::new(3);

        cache.insert("SELECT * FROM a", make_select("a"));
        cache.insert("SELECT * FROM b", make_select("b"));
        cache.insert("SELECT * FROM c", make_select("c"));

        // Access "a" to promote it
        cache.get("SELECT * FROM a");

        // Now insert a 4th — should evict "b" (oldest after promotion)
        cache.insert("SELECT * FROM d", make_select("d"));

        assert!(cache.get("SELECT * FROM a").is_some());
        assert!(cache.get("SELECT * FROM b").is_none());
        assert!(cache.get("SELECT * FROM c").is_some());
        assert!(cache.get("SELECT * FROM d").is_some());
    }

    #[test]
    fn test_clear() {
        let mut cache = PreparedCache::new(4);

        cache.insert("SELECT * FROM a", make_select("a"));
        cache.insert("INSERT INTO b VALUES (1)", make_insert("b"));
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(cache.get("SELECT * FROM a").is_none());
    }

    #[test]
    fn test_update_existing() {
        let mut cache = PreparedCache::new(4);
        let sql = "SELECT * FROM users";

        cache.insert(sql, make_select("users"));
        cache.insert(sql, make_insert("users"));

        assert_eq!(cache.len(), 1);
        let cached = cache.get(sql).unwrap();
        assert!(matches!(cached, Statement::Insert(_)));
    }
}
