use crate::error::{Error, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// Entry in the Record Address Table
///
/// Each entry maps a row_id to its location in the data file
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RatEntry {
    /// Unique row identifier
    row_id: u64,
    /// Byte offset in data.bin
    offset: u64,
    /// Length of serialized row in bytes
    length: u32,
    /// Whether this row has been deleted (tombstone)
    deleted: bool,
}

impl RatEntry {
    /// Size of a RAT entry in bytes (fixed size for efficient storage)
    const SIZE: usize = 8 + 8 + 4 + 1; // 21 bytes

    /// Serialize entry to bytes
    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.row_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.length.to_le_bytes());
        bytes[20] = if self.deleted { 1 } else { 0 };
        bytes
    }

    /// Deserialize entry from bytes
    fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        let mut row_id_buf = [0u8; 8];
        row_id_buf.copy_from_slice(&bytes[0..8]);
        let row_id = u64::from_le_bytes(row_id_buf);

        let mut offset_buf = [0u8; 8];
        offset_buf.copy_from_slice(&bytes[8..16]);
        let offset = u64::from_le_bytes(offset_buf);

        let mut length_buf = [0u8; 4];
        length_buf.copy_from_slice(&bytes[16..20]);
        let length = u32::from_le_bytes(length_buf);

        let deleted = bytes[20] != 0;

        Self {
            row_id,
            offset,
            length,
            deleted,
        }
    }
}

/// Record Address Table (RAT)
///
/// In-memory index mapping row_id to physical location in data file.
/// Maintained as a sorted vector for O(log n) binary search.
pub struct RecordAddressTable {
    /// Entries sorted by row_id
    entries: Vec<RatEntry>,
    /// Whether the RAT has been modified since last save
    dirty: bool,
}

// Magic number for RAT file format: "RAT\0"
const RAT_MAGIC: [u8; 4] = [b'R', b'A', b'T', 0];
const RAT_VERSION: u32 = 1;

impl RecordAddressTable {
    /// Create a new empty RAT
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            dirty: false,
        }
    }

    /// Load RAT from file
    ///
    /// # Arguments
    /// * `path` - Path to the rat.bin file
    ///
    /// # Returns
    /// Loaded RAT or empty RAT if file doesn't exist
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        if ! path.exists() {
            return Ok(Self::new());
        }

        let mut file = File::open(path)?;

        // Read and verify header
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != RAT_MAGIC {
            return Err(Error::Storage("Invalid RAT file magic number".to_string()));
        }

        let mut version_buf = [0u8; 4];
        file.read_exact(&mut version_buf)?;
        let version = u32::from_le_bytes(version_buf);
        if version != RAT_VERSION {
            return Err(Error::Storage(format!(
                "Unsupported RAT version: {}",
                version
            )));
        }

        let mut count_buf = [0u8; 8];
        file.read_exact(&mut count_buf)?;
        let count = u64::from_le_bytes(count_buf) as usize;

        // Read entries
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let mut entry_bytes = [0u8; RatEntry::SIZE];
            file.read_exact(&mut entry_bytes)?;
            entries.push(RatEntry::from_bytes(&entry_bytes));
        }

        Ok(Self {
            entries,
            dirty: false,
        })
    }

    /// Save RAT to file
    ///
    /// # Arguments
    /// * `path` - Path where to save rat.bin
    pub fn save<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();

        let mut file = File::create(path)?;

        // Write header
        file.write_all(&RAT_MAGIC)?;
        file.write_all(&RAT_VERSION.to_le_bytes())?;
        file.write_all(&(self.entries.len() as u64).to_le_bytes())?;

        // Write entries
        for entry in &self.entries {
            file.write_all(&entry.to_bytes())?;
        }

        file.sync_all()?;
        self.dirty = false;

        Ok(())
    }

    /// Insert a new entry into the RAT
    ///
    /// # Arguments
    /// * `row_id` - Unique row identifier
    /// * `offset` - Byte offset in data file
    /// * `length` - Length of serialized row
    ///
    /// # Returns
    /// Error if row_id already exists
    pub fn insert(&mut self, row_id: u64, offset: u64, length: u32) -> Result<()> {
        // Binary search to find insertion point
        match self.entries.binary_search_by_key(&row_id, |e| e.row_id) {
            Ok(_) => Err(Error::Storage(format!("Row ID {} already exists", row_id))),
            Err(pos) => {
                self.entries.insert(
                    pos,
                    RatEntry {
                        row_id,
                        offset,
                        length,
                        deleted: false,
                    },
                );
                self.dirty = true;
                Ok(())
            }
        }
    }

    /// Look up a row by ID
    ///
    /// # Arguments
    /// * `row_id` - Row to look up
    ///
    /// # Returns
    /// Some((offset, length)) if found and not deleted, None otherwise
    pub fn get(&self, row_id: u64) -> Option<(u64, u32)> {
        self.entries
            .binary_search_by_key(&row_id, |e| e.row_id)
            .ok()
            .and_then(|idx| {
                let entry = &self.entries[idx];
                if entry.deleted {
                    None
                } else {
                    Some((entry.offset, entry.length))
                }
            })
    }

    /// Mark a row as deleted (tombstone)
    ///
    /// # Arguments
    /// * `row_id` - Row to delete
    ///
    /// # Returns
    /// true if row was found and marked deleted, false otherwise
    pub fn delete(&mut self, row_id: u64) -> bool {
        if let Ok(idx) = self.entries.binary_search_by_key(&row_id, |e| e.row_id) {
            if ! self.entries[idx].deleted {
                self.entries[idx].deleted = true;
                self.dirty = true;
                return true;
            }
        }
        false
    }

    /// Get the number of entries (including deleted)
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if RAT is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of active (non-deleted) entries
    pub fn active_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.deleted).count()
    }

    /// Check if RAT has been modified
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Get all row IDs (including deleted)
    pub fn row_ids(&self) -> Vec<u64> {
        self.entries.iter().map(|e| e.row_id).collect()
    }

    /// Get all active row IDs (excluding deleted)
    pub fn active_row_ids(&self) -> Vec<u64> {
        self.entries
            .iter()
            .filter(|e| !e.deleted)
            .map(|e| e.row_id)
            .collect()
    }

    /// Compact the RAT by removing deleted entries
    ///
    /// This should be called periodically to reclaim memory
    pub fn compact(&mut self) {
        let old_len = self.entries.len();
        self.entries.retain(|e| !e.deleted);
        if self.entries.len() != old_len {
            self.dirty = true;
        }
    }
}

impl Default for RecordAddressTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_rat_entry_serialization() {
        let entry = RatEntry {
            row_id: 42,
            offset: 1024,
            length: 128,
            deleted: false,
        };

        let bytes = entry.to_bytes();
        let decoded = RatEntry::from_bytes(&bytes);

        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_rat_insert_and_get() {
        let mut rat = RecordAddressTable::new();

        rat.insert(1, 0, 100).unwrap();
        rat.insert(2, 100, 200).unwrap();
        rat.insert(3, 300, 150).unwrap();

        assert_eq!(rat.get(1), Some((0, 100)));
        assert_eq!(rat.get(2), Some((100, 200)));
        assert_eq!(rat.get(3), Some((300, 150)));
        assert_eq!(rat.get(999), None);
    }

    #[test]
    fn test_rat_insert_duplicate() {
        let mut rat = RecordAddressTable::new();

        rat.insert(1, 0, 100).unwrap();
        let result = rat.insert(1, 200, 100);

        assert!(result.is_err());
    }

    #[test]
    fn test_rat_delete() {
        let mut rat = RecordAddressTable::new();

        rat.insert(1, 0, 100).unwrap();
        assert_eq!(rat.get(1), Some((0, 100)));

        assert!(rat.delete(1));
        assert_eq!(rat.get(1), None);

        // Delete again should return false
        assert!(!rat.delete(1));
    }

    #[test]
    fn test_rat_unordered_insert() {
        let mut rat = RecordAddressTable::new();

        // Insert in random order - should still maintain sorted
        rat.insert(5, 500, 50).unwrap();
        rat.insert(1, 0, 100).unwrap();
        rat.insert(3, 300, 75).unwrap();
        rat.insert(2, 200, 80).unwrap();

        assert_eq!(rat.get(1), Some((0, 100)));
        assert_eq!(rat.get(2), Some((200, 80)));
        assert_eq!(rat.get(3), Some((300, 75)));
        assert_eq!(rat.get(5), Some((500, 50)));
    }

    #[test]
    fn test_rat_persistence() {
        let temp_file = "/tmp/test_rat.bin";
        let _ = fs::remove_file(temp_file);

        // Create and save
        let mut rat = RecordAddressTable::new();
        rat.insert(1, 0, 100).unwrap();
        rat.insert(2, 100, 200).unwrap();
        rat.delete(1);
        rat.save(temp_file).unwrap();

        // Load and verify
        let loaded = RecordAddressTable::load(temp_file).unwrap();
        assert_eq!(loaded.get(1), None); // deleted
        assert_eq!(loaded.get(2), Some((100, 200)));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.active_count(), 1);

        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_rat_dirty_flag() {
        let mut rat = RecordAddressTable::new();
        assert!(!rat.is_dirty());

        rat.insert(1, 0, 100).unwrap();
        assert!(rat.is_dirty());

        let temp_file = "/tmp/test_rat_dirty.bin";
        rat.save(temp_file).unwrap();
        assert!(!rat.is_dirty());

        rat.delete(1);
        assert!(rat.is_dirty());

        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_rat_counts() {
        let mut rat = RecordAddressTable::new();

        rat.insert(1, 0, 100).unwrap();
        rat.insert(2, 100, 100).unwrap();
        rat.insert(3, 200, 100).unwrap();

        assert_eq!(rat.len(), 3);
        assert_eq!(rat.active_count(), 3);
        assert!(!rat.is_empty());

        rat.delete(2);
        assert_eq!(rat.len(), 3);
        assert_eq!(rat.active_count(), 2);
    }

    #[test]
    fn test_rat_row_ids() {
        let mut rat = RecordAddressTable::new();

        rat.insert(5, 0, 100).unwrap();
        rat.insert(2, 100, 100).unwrap();
        rat.insert(8, 200, 100).unwrap();

        let all_ids = rat.row_ids();
        assert_eq!(all_ids, vec![2, 5, 8]); // Sorted

        rat.delete(5);
        let active_ids = rat.active_row_ids();
        assert_eq!(active_ids, vec![2, 8]);
    }

    #[test]
    fn test_rat_compact() {
        let mut rat = RecordAddressTable::new();

        rat.insert(1, 0, 100).unwrap();
        rat.insert(2, 100, 100).unwrap();
        rat.insert(3, 200, 100).unwrap();

        rat.delete(1);
        rat.delete(3);

        assert_eq!(rat.len(), 3);
        assert_eq!(rat.active_count(), 1);

        rat.compact();

        assert_eq!(rat.len(), 1);
        assert_eq!(rat.active_count(), 1);
        assert_eq!(rat.get(2), Some((100, 100)));
    }

    #[test]
    fn test_rat_large_dataset() {
        let mut rat = RecordAddressTable::new();

        // Insert 100k entries
        for i in 0..100_000 {
            rat.insert(i, i * 100, 100).unwrap();
        }

        // Verify lookup is fast (binary search)
        assert_eq!(rat.get(50_000), Some((5_000_000, 100)));
        assert_eq!(rat.get(99_999), Some((9_999_900, 100)));
        assert_eq!(rat.get(100_000), None);

        // Delete some entries
        for i in (0..100_000).step_by(2) {
            rat.delete(i);
        }

        assert_eq!(rat.active_count(), 50_000);
    }

    #[test]
    fn test_rat_load_nonexistent() {
        let result = RecordAddressTable::load("/tmp/nonexistent_rat.bin");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
