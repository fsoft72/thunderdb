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

/// Parsed view of the 24-byte page header.
#[derive(Debug, Clone)]
struct PageHeader {
    page_id: u32,
    page_type: PageType,
    flags: u8,
    slot_count: u16,
    free_space_start: u16,
    free_space_end: u16,
    first_free_slot: u16,
    active_count: u16,
}

impl PageHeader {
    fn from_bytes(data: &[u8; PAGE_SIZE]) -> Result<Self> {
        Ok(Self {
            page_id: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            page_type: PageType::from_u8(data[4])?,
            flags: data[5],
            slot_count: u16::from_le_bytes(data[6..8].try_into().unwrap()),
            free_space_start: u16::from_le_bytes(data[8..10].try_into().unwrap()),
            free_space_end: u16::from_le_bytes(data[10..12].try_into().unwrap()),
            first_free_slot: u16::from_le_bytes(data[12..14].try_into().unwrap()),
            active_count: u16::from_le_bytes(data[14..16].try_into().unwrap()),
        })
    }

    fn write_to(&self, data: &mut [u8; PAGE_SIZE]) {
        data[0..4].copy_from_slice(&self.page_id.to_le_bytes());
        data[4] = self.page_type as u8;
        data[5] = self.flags;
        data[6..8].copy_from_slice(&self.slot_count.to_le_bytes());
        data[8..10].copy_from_slice(&self.free_space_start.to_le_bytes());
        data[10..12].copy_from_slice(&self.free_space_end.to_le_bytes());
        data[12..14].copy_from_slice(&self.first_free_slot.to_le_bytes());
        data[14..16].copy_from_slice(&self.active_count.to_le_bytes());
    }
}

/// An 8KB slotted page.
///
/// Slot directory grows forward from byte 24; row data grows backward
/// from byte 8191. Free space is the gap between them.
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new empty data page.
    pub fn new(page_id: u32) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        let header = PageHeader {
            page_id,
            page_type: PageType::Data,
            flags: 0,
            slot_count: 0,
            free_space_start: PAGE_HEADER_SIZE as u16,
            free_space_end: PAGE_SIZE as u16,
            first_free_slot: INVALID_SLOT,
            active_count: 0,
        };
        header.write_to(&mut data);
        Self { header, data }
    }

    /// Parse a page from raw bytes.
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Result<Self> {
        let header = PageHeader::from_bytes(&data)?;
        Ok(Self { header, data })
    }

    /// Serialize the page to raw bytes.
    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut data = self.data;
        self.header.write_to(&mut data);
        data
    }

    /// Page identifier.
    pub fn page_id(&self) -> u32 {
        self.header.page_id
    }

    /// Page type.
    pub fn page_type(&self) -> PageType {
        self.header.page_type
    }

    /// Total number of slots (active + free).
    pub fn slot_count(&self) -> u16 {
        self.header.slot_count
    }

    /// Number of active (non-free) rows.
    pub fn active_count(&self) -> u16 {
        self.header.active_count
    }

    /// Contiguous free space available in bytes.
    pub fn free_space(&self) -> usize {
        self.header.free_space_end as usize - self.header.free_space_start as usize
    }

    /// Check if a row of the given size can be inserted.
    pub fn can_fit(&self, row_size: usize) -> bool {
        if self.header.first_free_slot != INVALID_SLOT {
            self.free_space() >= row_size
        } else {
            self.free_space() >= row_size + SLOT_SIZE
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

    #[test]
    fn test_new_page() {
        let page = Page::new(42);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.page_type(), PageType::Data);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.active_count(), 0);
        assert_eq!(page.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_page_serialization_roundtrip() {
        let page = Page::new(99);
        let bytes = page.to_bytes();
        assert_eq!(bytes.len(), PAGE_SIZE);
        let restored = Page::from_bytes(bytes).unwrap();
        assert_eq!(restored.page_id(), 99);
        assert_eq!(restored.page_type(), PageType::Data);
        assert_eq!(restored.slot_count(), 0);
        assert_eq!(restored.active_count(), 0);
    }
}
