//! Slotted page storage format.
//!
//! An 8KB page with a slot directory growing forward and row data growing
//! backward. Ctid (page_id, slot_index) provides O(1) physical addressing.

use crate::error::{Error, Result};
use crate::storage::value::Value;

/// Page size in bytes (8 KB).
pub const PAGE_SIZE: usize = 8192;

/// Page header size in bytes.
const PAGE_HEADER_SIZE: usize = 24;

/// Slot entry size in bytes (offset: u16 + length: u16).
const SLOT_SIZE: usize = 4;

/// Sentinel value for empty free-slot list or freed slot offset.
const INVALID_SLOT: u16 = 0xFFFF;

/// Page type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Data = 0,
    Overflow = 1,
    FreeSpaceMap = 2,
}

impl PageType {
    fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(PageType::Data),
            1 => Ok(PageType::Overflow),
            2 => Ok(PageType::FreeSpaceMap),
            _ => Err(Error::Storage(format!("Unknown page type: {}", v))),
        }
    }
}

/// Physical row address: (page_id, slot_index).
///
/// Packs into a u64 for B-tree index compatibility:
/// upper 48 bits = page_id, lower 16 bits = slot_index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ctid {
    pub page_id: u32,
    pub slot_index: u16,
}

impl Ctid {
    /// Create a new Ctid.
    pub fn new(page_id: u32, slot_index: u16) -> Self {
        Self { page_id, slot_index }
    }

    /// Pack into a u64 for B-tree storage.
    pub fn to_u64(self) -> u64 {
        (self.page_id as u64) << 16 | self.slot_index as u64
    }

    /// Unpack from a u64.
    pub fn from_u64(val: u64) -> Self {
        Self {
            page_id: (val >> 16) as u32,
            slot_index: (val & 0xFFFF) as u16,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ctid_roundtrip() {
        let ctid = Ctid::new(12345, 42);
        let packed = ctid.to_u64();
        let unpacked = Ctid::from_u64(packed);
        assert_eq!(ctid, unpacked);
    }

    #[test]
    fn test_ctid_zero() {
        let ctid = Ctid::new(0, 0);
        assert_eq!(ctid.to_u64(), 0);
        assert_eq!(Ctid::from_u64(0), ctid);
    }

    #[test]
    fn test_ctid_max_values() {
        let ctid = Ctid::new(u32::MAX, u16::MAX);
        let unpacked = Ctid::from_u64(ctid.to_u64());
        assert_eq!(ctid, unpacked);
    }

    #[test]
    fn test_page_type_roundtrip() {
        assert_eq!(PageType::from_u8(0).unwrap(), PageType::Data);
        assert_eq!(PageType::from_u8(1).unwrap(), PageType::Overflow);
        assert_eq!(PageType::from_u8(2).unwrap(), PageType::FreeSpaceMap);
        assert!(PageType::from_u8(99).is_err());
    }
}
