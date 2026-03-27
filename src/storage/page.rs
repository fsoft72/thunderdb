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

/// Serialize row values for page storage (no row_id, u16 col_count).
///
/// Format: [col_count: u16][off0: u16]...[offN-1: u16][val0]...[valN-1]
pub fn serialize_row_for_page(values: &[Value]) -> Vec<u8> {
    let col_count = values.len();
    let mut values_buf = Vec::with_capacity(col_count * 8);
    let mut offsets: Vec<u16> = Vec::with_capacity(col_count);

    for value in values {
        offsets.push(values_buf.len() as u16);
        value.write_to(&mut values_buf).unwrap();
    }

    let total = 2 + col_count * 2 + values_buf.len();
    let mut row = Vec::with_capacity(total);
    row.extend_from_slice(&(col_count as u16).to_le_bytes());
    for off in &offsets {
        row.extend_from_slice(&off.to_le_bytes());
    }
    row.extend_from_slice(&values_buf);
    row
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

    /// Get the free_space_start value from the header.
    ///
    /// For overflow pages, this serves as the write cursor.
    pub fn header_free_space_start(&self) -> u16 {
        self.header.free_space_start
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

    /// Insert a row into the page. Returns the slot index.
    pub fn insert_row(&mut self, row_data: &[u8]) -> Result<u16> {
        let row_len = row_data.len();
        if !self.can_fit(row_len) {
            return Err(Error::Storage(format!(
                "Page {} is full: need {} bytes, have {} free",
                self.header.page_id, row_len, self.free_space()
            )));
        }

        // Allocate row space (grows backward)
        let new_end = self.header.free_space_end as usize - row_len;
        self.data[new_end..new_end + row_len].copy_from_slice(row_data);
        self.header.free_space_end = new_end as u16;

        // Allocate slot
        let slot_index = if self.header.first_free_slot != INVALID_SLOT {
            let slot = self.header.first_free_slot;
            let slot_pos = PAGE_HEADER_SIZE + slot as usize * SLOT_SIZE;
            let next_free = u16::from_le_bytes(
                self.data[slot_pos + 2..slot_pos + 4].try_into().unwrap(),
            );
            self.header.first_free_slot = next_free;
            slot
        } else {
            let slot = self.header.slot_count;
            self.header.slot_count += 1;
            self.header.free_space_start += SLOT_SIZE as u16;
            slot
        };

        // Write slot entry
        let slot_pos = PAGE_HEADER_SIZE + slot_index as usize * SLOT_SIZE;
        self.data[slot_pos..slot_pos + 2].copy_from_slice(&(new_end as u16).to_le_bytes());
        self.data[slot_pos + 2..slot_pos + 4].copy_from_slice(&(row_len as u16).to_le_bytes());

        self.header.active_count += 1;
        Ok(slot_index)
    }

    /// Get the raw row data for a slot. Returns None if the slot is free or out of bounds.
    pub fn get_row(&self, slot_index: u16) -> Option<&[u8]> {
        if slot_index >= self.header.slot_count {
            return None;
        }

        let slot_pos = PAGE_HEADER_SIZE + slot_index as usize * SLOT_SIZE;
        let offset = u16::from_le_bytes(
            self.data[slot_pos..slot_pos + 2].try_into().unwrap(),
        );

        if offset == INVALID_SLOT {
            return None;
        }

        let length = u16::from_le_bytes(
            self.data[slot_pos + 2..slot_pos + 4].try_into().unwrap(),
        );

        let start = offset as usize;
        let end = start + length as usize;
        Some(&self.data[start..end])
    }

    /// Compact the page by rewriting active rows contiguously.
    ///
    /// Eliminates gaps left by deleted rows, reclaiming space into the
    /// contiguous free area. Slot offsets are updated to point to the
    /// new positions. Free slot list is preserved.
    pub fn compact(&mut self) {
        // Collect active rows: (slot_index, data)
        let mut active_rows: Vec<(u16, Vec<u8>)> = Vec::new();
        for i in 0..self.header.slot_count {
            if let Some(row_data) = self.get_row(i) {
                active_rows.push((i, row_data.to_vec()));
            }
        }

        // Rewrite rows from the end of the page
        let mut write_pos = PAGE_SIZE;
        for (slot_index, row_data) in &active_rows {
            write_pos -= row_data.len();
            self.data[write_pos..write_pos + row_data.len()].copy_from_slice(row_data);

            // Update slot offset
            let slot_pos = PAGE_HEADER_SIZE + *slot_index as usize * SLOT_SIZE;
            self.data[slot_pos..slot_pos + 2].copy_from_slice(&(write_pos as u16).to_le_bytes());
        }

        self.header.free_space_end = write_pos as u16;
    }

    /// Delete a row by slot index. Returns false if the slot is already
    /// free or out of bounds.
    pub fn delete_row(&mut self, slot_index: u16) -> bool {
        if slot_index >= self.header.slot_count {
            return false;
        }

        let slot_pos = PAGE_HEADER_SIZE + slot_index as usize * SLOT_SIZE;
        let offset = u16::from_le_bytes(
            self.data[slot_pos..slot_pos + 2].try_into().unwrap(),
        );

        if offset == INVALID_SLOT {
            return false;
        }

        self.data[slot_pos..slot_pos + 2].copy_from_slice(&INVALID_SLOT.to_le_bytes());
        self.data[slot_pos + 2..slot_pos + 4]
            .copy_from_slice(&self.header.first_free_slot.to_le_bytes());
        self.header.first_free_slot = slot_index;
        self.header.active_count -= 1;

        true
    }

    /// Extract a single column value from a row without full deserialization.
    ///
    /// Uses the column-offset array in the page row format (u16 col_count).
    pub fn value_at(&self, slot_index: u16, col_idx: usize) -> Result<Value> {
        let row_data = self.get_row(slot_index).ok_or_else(|| {
            Error::Storage(format!("Slot {} is free or out of bounds", slot_index))
        })?;

        if row_data.len() < 2 {
            return Err(Error::Serialization("Row too short for col_count".to_string()));
        }

        let col_count = u16::from_le_bytes(row_data[0..2].try_into().unwrap()) as usize;

        if col_idx >= col_count {
            return Err(Error::Serialization(format!(
                "Column index {} out of bounds (row has {} columns)",
                col_idx, col_count
            )));
        }

        let offsets_start = 2usize;
        let values_area_start = offsets_start + col_count * 2;

        let off_pos = offsets_start + col_idx * 2;
        let col_offset = u16::from_le_bytes(
            row_data[off_pos..off_pos + 2].try_into().unwrap(),
        ) as usize;

        let value_pos = values_area_start + col_offset;
        if value_pos >= row_data.len() {
            return Err(Error::Serialization(
                "Column offset points past end of row data".to_string(),
            ));
        }

        let (value, _) = Value::from_bytes(&row_data[value_pos..])?;
        Ok(value)
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

    #[test]
    fn test_serialize_row_for_page() {
        let values = vec![Value::Int32(42), Value::varchar("hello".to_string())];
        let data = serialize_row_for_page(&values);
        let col_count = u16::from_le_bytes(data[0..2].try_into().unwrap());
        assert_eq!(col_count, 2);
    }

    #[test]
    fn test_insert_and_get_row() {
        let mut page = Page::new(0);
        let values = vec![Value::Int32(42), Value::varchar("hello".to_string())];
        let row_data = serialize_row_for_page(&values);

        let slot = page.insert_row(&row_data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.active_count(), 1);
        assert_eq!(page.slot_count(), 1);

        let retrieved = page.get_row(slot).unwrap();
        assert_eq!(retrieved, &row_data[..]);
    }

    #[test]
    fn test_insert_multiple_rows() {
        let mut page = Page::new(0);
        for i in 0..50 {
            let values = vec![Value::Int32(i), Value::varchar(format!("row_{}", i))];
            let data = serialize_row_for_page(&values);
            page.insert_row(&data).unwrap();
        }
        assert_eq!(page.active_count(), 50);
        assert_eq!(page.slot_count(), 50);
    }

    #[test]
    fn test_insert_full_page_error() {
        let mut page = Page::new(0);
        let big_value = Value::varchar("x".repeat(200));
        let data = serialize_row_for_page(&vec![big_value]);
        let mut count = 0;
        while page.can_fit(data.len()) {
            page.insert_row(&data).unwrap();
            count += 1;
        }
        assert!(count > 0);
        assert!(page.insert_row(&data).is_err());
    }

    #[test]
    fn test_get_invalid_slot() {
        let page = Page::new(0);
        assert!(page.get_row(0).is_none());
        assert!(page.get_row(100).is_none());
    }

    #[test]
    fn test_delete_row() {
        let mut page = Page::new(0);
        let data = serialize_row_for_page(&vec![Value::Int32(1)]);
        let slot = page.insert_row(&data).unwrap();
        assert_eq!(page.active_count(), 1);

        assert!(page.delete_row(slot));
        assert_eq!(page.active_count(), 0);
        assert!(page.get_row(slot).is_none());
        assert!(!page.delete_row(slot)); // double delete
    }

    #[test]
    fn test_slot_reuse_after_delete() {
        let mut page = Page::new(0);
        let data1 = serialize_row_for_page(&vec![Value::Int32(1)]);
        let data2 = serialize_row_for_page(&vec![Value::Int32(2)]);
        let data3 = serialize_row_for_page(&vec![Value::Int32(3)]);

        let s0 = page.insert_row(&data1).unwrap();
        let s1 = page.insert_row(&data2).unwrap();
        let _s2 = page.insert_row(&data3).unwrap();

        page.delete_row(s0);
        page.delete_row(s1);

        // LIFO: next insert reuses s1, then s0
        let data4 = serialize_row_for_page(&vec![Value::Int32(4)]);
        let s_new = page.insert_row(&data4).unwrap();
        assert_eq!(s_new, s1);

        let data5 = serialize_row_for_page(&vec![Value::Int32(5)]);
        let s_new2 = page.insert_row(&data5).unwrap();
        assert_eq!(s_new2, s0);
    }

    #[test]
    fn test_delete_invalid_slot() {
        let mut page = Page::new(0);
        assert!(!page.delete_row(0));
        assert!(!page.delete_row(999));
    }

    #[test]
    fn test_compact() {
        let mut page = Page::new(0);

        let mut slots = Vec::new();
        for i in 0..5 {
            let data = serialize_row_for_page(&vec![
                Value::Int32(i),
                Value::varchar(format!("row_{}", i)),
            ]);
            slots.push(page.insert_row(&data).unwrap());
        }

        let space_before_delete = page.free_space();

        // Delete rows 1 and 3 (creates gaps)
        page.delete_row(slots[1]);
        page.delete_row(slots[3]);
        assert_eq!(page.active_count(), 3);

        // Free space doesn't increase after delete (data gaps not reclaimed)
        let space_after_delete = page.free_space();
        assert_eq!(space_after_delete, space_before_delete);

        // Compact reclaims the gaps
        page.compact();
        let space_after_compact = page.free_space();
        assert!(space_after_compact > space_after_delete);
        assert_eq!(page.active_count(), 3);

        // Remaining rows still readable
        assert!(page.get_row(slots[0]).is_some());
        assert!(page.get_row(slots[2]).is_some());
        assert!(page.get_row(slots[4]).is_some());

        // Deleted slots still gone
        assert!(page.get_row(slots[1]).is_none());
        assert!(page.get_row(slots[3]).is_none());
    }

    #[test]
    fn test_value_at() {
        let mut page = Page::new(0);
        let values = vec![
            Value::Int32(42),
            Value::varchar("hello world".to_string()),
            Value::Int64(999),
        ];
        let data = serialize_row_for_page(&values);
        let slot = page.insert_row(&data).unwrap();

        let v0 = page.value_at(slot, 0).unwrap();
        assert_eq!(v0, Value::Int32(42));

        let v1 = page.value_at(slot, 1).unwrap();
        assert_eq!(v1, Value::varchar("hello world".to_string()));

        let v2 = page.value_at(slot, 2).unwrap();
        assert_eq!(v2, Value::Int64(999));

        // Out of bounds column
        assert!(page.value_at(slot, 3).is_err());

        // Invalid slot
        assert!(page.value_at(99, 0).is_err());
    }

    #[test]
    fn test_full_roundtrip_with_data() {
        let mut page = Page::new(7);

        for i in 0..10 {
            let data = serialize_row_for_page(&vec![
                Value::Int32(i),
                Value::varchar(format!("text_{}", i)),
            ]);
            page.insert_row(&data).unwrap();
        }

        page.delete_row(3);
        page.delete_row(7);

        let bytes = page.to_bytes();
        let restored = Page::from_bytes(bytes).unwrap();

        assert_eq!(restored.page_id(), 7);
        assert_eq!(restored.active_count(), 8);
        assert_eq!(restored.slot_count(), 10);

        for i in 0..10u16 {
            if i == 3 || i == 7 {
                assert!(restored.get_row(i).is_none());
            } else {
                let val = restored.value_at(i, 0).unwrap();
                assert_eq!(val, Value::Int32(i as i32));
            }
        }
    }

    #[test]
    fn test_empty_row() {
        let mut page = Page::new(0);
        let data = serialize_row_for_page(&vec![]);
        let slot = page.insert_row(&data).unwrap();
        let row = page.get_row(slot).unwrap();
        let col_count = u16::from_le_bytes(row[0..2].try_into().unwrap());
        assert_eq!(col_count, 0);
    }
}
