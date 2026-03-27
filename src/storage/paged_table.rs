//! PagedTable: CRUD layer over PageFile + Page + TOAST.
//!
//! Wraps a `PageFile` and provides row-level operations (insert, get,
//! delete, update, scan) using slotted pages with automatic TOAST for
//! oversized rows.

use crate::error::Result;
use crate::storage::page::{Ctid, Page, PageType, PAGE_SIZE, PAGE_HEADER_SIZE, SLOT_SIZE, INVALID_SLOT, serialize_row_for_page, value_at_page_bytes};
use crate::storage::page_file::PageFile;
use crate::storage::toast;
use crate::storage::value::Value;
use crate::storage::row::Row;
use std::collections::HashMap;
use std::path::Path;

/// Rows larger than this (serialized bytes) trigger TOAST.
const TOAST_THRESHOLD: usize = 2000;

/// Type tag for a TOAST pointer (must match toast.rs).
const TOAST_TAG: u8 = 0x07;

/// Page-based table storage with automatic TOAST support.
///
/// Manages rows across slotted pages, tracking active row count and
/// delegating free-space management to the underlying `PageFile`.
pub struct PagedTable {
    page_file: PageFile,
    active_count: u64,
}

impl PagedTable {
    /// Open or create a PagedTable backed by the given file path.
    ///
    /// On open, scans all data pages to count active rows.
    pub fn open(path: &Path) -> Result<Self> {
        let mut page_file = PageFile::open(path)?;

        // Scan data pages (starting at page 1) to count active rows
        let mut active_count: u64 = 0;
        for page_id in 1..page_file.page_count() {
            let page = page_file.read_page(page_id)?;
            if page.page_type() == PageType::Data {
                active_count += page.active_count() as u64;
            }
        }

        Ok(Self {
            page_file,
            active_count,
        })
    }

    /// Insert a row into the table.
    ///
    /// Serializes the values, toasts if the row exceeds TOAST_THRESHOLD,
    /// finds a page with enough space, inserts, and updates the FSM.
    pub fn insert_row(&mut self, values: &[Value]) -> Result<Ctid> {
        let row_bytes = serialize_row_for_page(values);

        let data = if row_bytes.len() > TOAST_THRESHOLD {
            toast::toast_row(values, &mut self.page_file)?
        } else {
            row_bytes
        };

        let page_id = self.page_file.find_page_with_space(data.len())?;
        let mut page = self.page_file.read_page(page_id)?;
        let slot_index = page.insert_row(&data)?;
        self.page_file.write_page(&page)?;
        self.page_file.update_fsm(page_id, page.free_space())?;

        self.active_count += 1;

        Ok(Ctid::new(page_id, slot_index))
    }

    /// Insert multiple rows in a single batch.
    ///
    /// Keeps a hot page in memory and fills it before writing. When the
    /// page is full it is flushed once and a new page is allocated.
    /// This avoids the per-row read-modify-write cycle and the implicit
    /// `sync_all()` between each write and read that plagues single inserts.
    pub fn insert_batch(&mut self, rows_values: &[Vec<Value>]) -> Result<Vec<Ctid>> {
        if rows_values.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Serialize all rows (with TOAST if needed)
        let mut serialized = Vec::with_capacity(rows_values.len());
        for values in rows_values {
            let row_bytes = serialize_row_for_page(values);
            let data = if row_bytes.len() > TOAST_THRESHOLD {
                toast::toast_row(values, &mut self.page_file)?
            } else {
                row_bytes
            };
            serialized.push(data);
        }

        // 2. Load the first page with space (single FSM scan + page read)
        let mut ctids = Vec::with_capacity(serialized.len());
        let mut cur_page_id = self.page_file.find_page_with_space(serialized[0].len())?;
        let mut cur_page = self.page_file.read_page(cur_page_id)?;

        for data in &serialized {
            if !cur_page.can_fit(data.len()) {
                // Flush full page: one write + one FSM update
                self.page_file.write_page(&cur_page)?;
                self.page_file.update_fsm(cur_page_id, cur_page.free_space())?;

                // Allocate a fresh page (no read needed — Page::new is in-memory)
                cur_page_id = self.page_file.allocate_page()?;
                cur_page = Page::new(cur_page_id);
            }

            let slot = cur_page.insert_row(data)?;
            ctids.push(Ctid::new(cur_page_id, slot));
        }

        // 3. Flush the last page
        self.page_file.write_page(&cur_page)?;
        self.page_file.update_fsm(cur_page_id, cur_page.free_space())?;

        self.active_count += ctids.len() as u64;

        Ok(ctids)
    }

    /// Get a row by its ctid.
    ///
    /// Reads the page, extracts the raw slot bytes, detoasts if needed,
    /// then parses the u16-col_count page row format into a `Row`.
    pub fn get_row(&mut self, ctid: Ctid) -> Result<Option<Row>> {
        if ctid.page_id >= self.page_file.page_count() {
            return Ok(None);
        }

        let page = self.page_file.read_page(ctid.page_id)?;
        let raw = match page.get_row(ctid.slot_index) {
            Some(bytes) => bytes.to_vec(),
            None => return Ok(None),
        };

        let detoasted = toast::detoast_row_bytes(&raw, &mut self.page_file)?;
        let values = _parse_page_row_values(&detoasted)?;

        Ok(Some(Row::new(ctid.to_u64(), values)))
    }

    /// Delete a row by ctid.
    ///
    /// Reads the page, frees any TOAST data, marks the slot as deleted,
    /// writes the page back, and updates the FSM.
    /// Returns `false` if the slot was already free or out of bounds.
    pub fn delete_row(&mut self, ctid: Ctid) -> Result<bool> {
        if ctid.page_id >= self.page_file.page_count() {
            return Ok(false);
        }

        let mut page = self.page_file.read_page(ctid.page_id)?;

        let raw = match page.get_row(ctid.slot_index) {
            Some(bytes) => bytes.to_vec(),
            None => return Ok(false),
        };

        // Free any overflow data referenced by TOAST pointers
        toast::free_toast_data(&raw, &mut self.page_file)?;

        if !page.delete_row(ctid.slot_index) {
            return Ok(false);
        }

        self.page_file.write_page(&page)?;
        self.page_file.update_fsm(ctid.page_id, page.free_space())?;
        self.active_count -= 1;

        Ok(true)
    }

    /// Update a row by deleting the old one and inserting new values.
    ///
    /// Returns the ctid of the newly inserted row.
    pub fn update_row(&mut self, ctid: Ctid, values: &[Value]) -> Result<Ctid> {
        self.delete_row(ctid)?;
        self.insert_row(values)
    }

    /// Scan all active rows in the table.
    ///
    /// Uses direct mmap access to avoid per-page 8KB copies and per-row
    /// allocations. Only rows with TOAST pointers incur a copy.
    pub fn scan_all(&mut self) -> Result<Vec<Row>> {
        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::new();

        for page_id in 1..page_count {
            let pd = _mmap_page(mmap_ptr, page_id);
            if pd[4] != PageType::Data as u8 { continue; }

            let slot_count = u16::from_le_bytes(pd[6..8].try_into().unwrap());
            for slot in 0..slot_count {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                let row_id = Ctid::new(page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_page_row_values(&detoasted)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_page_row_values(raw)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Scan all active rows, returning only projected columns.
    ///
    /// Like `scan_all` but deserializes only the requested column indices,
    /// skipping expensive VARCHAR parsing for columns not in `columns`.
    pub fn scan_all_projected(&mut self, columns: &[usize]) -> Result<Vec<Row>> {
        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::new();

        for page_id in 1..page_count {
            let pd = _mmap_page(mmap_ptr, page_id);
            if pd[4] != PageType::Data as u8 { continue; }

            let slot_count = u16::from_le_bytes(pd[6..8].try_into().unwrap());
            for slot in 0..slot_count {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                let row_id = Ctid::new(page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_projected(&detoasted, columns)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_projected(raw, columns)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Scan rows that pass a raw-bytes predicate.
    ///
    /// The predicate receives the raw (possibly toasted) slot bytes.
    /// Only matching rows are detoasted and deserialized.
    pub fn scan_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::new();

        for page_id in 1..page_count {
            let pd = _mmap_page(mmap_ptr, page_id);
            if pd[4] != PageType::Data as u8 { continue; }

            let slot_count = u16::from_le_bytes(pd[6..8].try_into().unwrap());
            for slot in 0..slot_count {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                if !predicate(raw) { continue; }

                let row_id = Ctid::new(page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_page_row_values(&detoasted)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_page_row_values(raw)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Count rows that pass a raw-bytes predicate without collecting them.
    pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut count = 0;

        for page_id in 1..page_count {
            let pd = _mmap_page(mmap_ptr, page_id);
            if pd[4] != PageType::Data as u8 { continue; }

            let slot_count = u16::from_le_bytes(pd[6..8].try_into().unwrap());
            for slot in 0..slot_count {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                if predicate(raw) {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    /// Fetch multiple rows by ctid, grouping reads by page_id.
    ///
    /// Uses direct mmap access to avoid per-page 8KB copies.
    /// Results are returned in arbitrary order.
    pub fn get_rows_by_ctids(&mut self, ctids: &[Ctid]) -> Result<Vec<Row>> {
        let mut by_page: HashMap<u32, Vec<u16>> = HashMap::new();
        for ctid in ctids {
            by_page.entry(ctid.page_id).or_default().push(ctid.slot_index);
        }

        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::with_capacity(ctids.len());

        for (page_id, slots) in &by_page {
            if *page_id >= page_count { continue; }

            let pd = _mmap_page(mmap_ptr, *page_id);

            for &slot in slots {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                let row_id = Ctid::new(*page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_page_row_values(&detoasted)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_page_row_values(raw)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Fetch multiple rows by ctid, returning only projected columns.
    pub fn get_rows_by_ctids_projected(&mut self, ctids: &[Ctid], columns: &[usize]) -> Result<Vec<Row>> {
        let mut by_page: HashMap<u32, Vec<u16>> = HashMap::new();
        for ctid in ctids {
            by_page.entry(ctid.page_id).or_default().push(ctid.slot_index);
        }

        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::with_capacity(ctids.len());

        for (page_id, slots) in &by_page {
            if *page_id >= page_count { continue; }

            let pd = _mmap_page(mmap_ptr, *page_id);

            for &slot in slots {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                let row_id = Ctid::new(*page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_projected(&detoasted, columns)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_projected(raw, columns)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Fetch multiple rows by ctid with a raw-bytes predicate filter.
    ///
    /// Same page-grouping strategy as `get_rows_by_ctids`, but only
    /// detoasts and deserializes rows whose raw bytes pass the predicate.
    pub fn get_rows_by_ctids_filtered<F>(
        &mut self,
        ctids: &[Ctid],
        predicate: F,
    ) -> Result<Vec<Row>>
    where
        F: Fn(&[u8]) -> bool,
    {
        let mut by_page: HashMap<u32, Vec<u16>> = HashMap::new();
        for ctid in ctids {
            by_page.entry(ctid.page_id).or_default().push(ctid.slot_index);
        }

        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut rows = Vec::with_capacity(ctids.len());

        for (page_id, slots) in &by_page {
            if *page_id >= page_count { continue; }

            let pd = _mmap_page(mmap_ptr, *page_id);

            for &slot in slots {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                if !predicate(raw) { continue; }

                let row_id = Ctid::new(*page_id, slot).to_u64();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    let values = _parse_page_row_values(&detoasted)?;
                    rows.push(Row::new(row_id, values));
                } else {
                    let values = _parse_page_row_values(raw)?;
                    rows.push(Row::new(row_id, values));
                }
            }
        }

        Ok(rows)
    }

    /// Return the number of active (non-deleted) rows.
    pub fn active_row_count(&self) -> usize {
        self.active_count as usize
    }
}

/// Get a page-sized slice from the mmap pointer.
///
/// # Safety
/// Internally uses `unsafe` to create a slice from the raw pointer.
/// The caller must ensure `mmap_ptr` is valid for at least
/// `(page_id + 1) * PAGE_SIZE` bytes and that no concurrent writes
/// invalidate the mapping (i.e., `ensure_mmap_and_ptr` was called
/// and no writes have occurred since).
#[inline]
fn _mmap_page<'a>(mmap_ptr: *const u8, page_id: u32) -> &'a [u8] {
    let offset = page_id as usize * PAGE_SIZE;
    // SAFETY: mmap_ptr was obtained from ensure_mmap_and_ptr() which
    // remaps if stale. No writes occur during scan, so the mapping
    // remains valid for the duration of the scan loop.
    unsafe { std::slice::from_raw_parts(mmap_ptr.add(offset), PAGE_SIZE) }
}

/// Extract the raw row bytes for a slot from a page data slice.
///
/// Returns `None` if the slot is out of bounds or has been freed.
#[inline]
fn _slot_bytes<'a>(page_data: &'a [u8], slot: u16) -> Option<&'a [u8]> {
    // Check against slot_count (bytes 6..8 in page header)
    let slot_count = u16::from_le_bytes(page_data[6..8].try_into().unwrap());
    if slot >= slot_count { return None; }

    let slot_pos = PAGE_HEADER_SIZE + slot as usize * SLOT_SIZE;
    if slot_pos + SLOT_SIZE > page_data.len() { return None; }

    let offset = u16::from_le_bytes(page_data[slot_pos..slot_pos + 2].try_into().unwrap());
    if offset == INVALID_SLOT { return None; }

    let length = u16::from_le_bytes(page_data[slot_pos + 2..slot_pos + 4].try_into().unwrap());
    let start = offset as usize;
    let end = start + length as usize;
    Some(&page_data[start..end])
}

/// Quick inline check for TOAST pointers in raw row bytes.
#[inline]
fn _has_toast(raw: &[u8]) -> bool {
    if raw.len() < 2 { return false; }
    let col_count = u16::from_le_bytes(raw[0..2].try_into().unwrap()) as usize;
    let values_area = 2 + col_count * 2;
    values_area < raw.len() && raw[values_area..].contains(&TOAST_TAG)
}

/// Parse only selected columns from page-row bytes.
///
/// Uses `value_at_page_bytes` to jump directly to each requested column
/// offset, skipping deserialization of unused columns.
fn _parse_projected(bytes: &[u8], columns: &[usize]) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(columns.len());
    for &col in columns {
        values.push(value_at_page_bytes(bytes, col)?);
    }
    Ok(values)
}

/// Parse values from detoasted page-row bytes (u16 col_count format).
///
/// Format: [col_count: u16][off0: u16]...[offN-1: u16][val0]...[valN-1]
fn _parse_page_row_values(bytes: &[u8]) -> Result<Vec<Value>> {
    if bytes.len() < 2 {
        return Ok(Vec::new());
    }

    let col_count = u16::from_le_bytes(bytes[0..2].try_into().unwrap()) as usize;
    let offsets_start = 2;
    let values_area_start = offsets_start + col_count * 2;
    let mut values = Vec::with_capacity(col_count);
    let mut cursor = values_area_start;

    for _ in 0..col_count {
        let (value, consumed) = Value::from_bytes(&bytes[cursor..])?;
        values.push(value);
        cursor += consumed;
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::path::PathBuf::from("/tmp/thunderdb_paged_table_tests");
        let _ = std::fs::create_dir_all(&dir);
        dir.join(name)
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_insert_and_get_roundtrip() {
        let path = temp_path("test_insert_get.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        let values = vec![
            Value::Int32(42),
            Value::varchar("hello world".to_string()),
            Value::Int64(999),
        ];

        let ctid = pt.insert_row(&values).unwrap();
        let row = pt.get_row(ctid).unwrap().expect("row should exist");

        assert_eq!(row.row_id, ctid.to_u64());
        assert_eq!(row.values, values);

        cleanup(&path);
    }

    #[test]
    fn test_insert_multiple_and_get() {
        let path = temp_path("test_insert_multi.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        let mut ctids = Vec::new();
        for i in 0..10 {
            let values = vec![Value::Int32(i), Value::varchar(format!("row_{}", i))];
            let ctid = pt.insert_row(&values).unwrap();
            ctids.push(ctid);
        }

        for (i, ctid) in ctids.iter().enumerate() {
            let row = pt.get_row(*ctid).unwrap().expect("row should exist");
            assert_eq!(row.values[0], Value::Int32(i as i32));
            assert_eq!(row.values[1], Value::varchar(format!("row_{}", i)));
        }

        cleanup(&path);
    }

    #[test]
    fn test_active_row_count() {
        let path = temp_path("test_active_count.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        assert_eq!(pt.active_row_count(), 0);

        for i in 0..5 {
            pt.insert_row(&[Value::Int32(i)]).unwrap();
        }
        assert_eq!(pt.active_row_count(), 5);

        cleanup(&path);
    }

    #[test]
    fn test_get_nonexistent_row() {
        let path = temp_path("test_get_none.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        // Out-of-bounds page
        let result = pt.get_row(Ctid::new(999, 0)).unwrap();
        assert!(result.is_none());

        // Valid page but no rows
        pt.insert_row(&[Value::Int32(1)]).unwrap();
        let result = pt.get_row(Ctid::new(1, 99)).unwrap();
        assert!(result.is_none());

        cleanup(&path);
    }

    #[test]
    fn test_delete_then_get_none() {
        let path = temp_path("test_delete.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        let ctid = pt.insert_row(&[Value::Int32(1)]).unwrap();
        assert_eq!(pt.active_row_count(), 1);

        let deleted = pt.delete_row(ctid).unwrap();
        assert!(deleted);
        assert_eq!(pt.active_row_count(), 0);

        let row = pt.get_row(ctid).unwrap();
        assert!(row.is_none());

        // Double delete returns false
        let deleted_again = pt.delete_row(ctid).unwrap();
        assert!(!deleted_again);

        cleanup(&path);
    }

    #[test]
    fn test_update_row() {
        let path = temp_path("test_update.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        // Insert two rows so slot reuse does not overwrite the one we check
        pt.insert_row(&[Value::Int32(0)]).unwrap();
        let old_ctid = pt.insert_row(&[Value::Int32(1), Value::varchar("old".to_string())]).unwrap();

        let new_ctid = pt.update_row(old_ctid, &[Value::Int32(2), Value::varchar("new".to_string())]).unwrap();

        // New ctid has the updated data
        let row = pt.get_row(new_ctid).unwrap().expect("updated row should exist");
        assert_eq!(row.values[0], Value::Int32(2));
        assert_eq!(row.values[1], Value::varchar("new".to_string()));

        // Active count: started with 2, update deletes 1 + inserts 1 = still 2
        assert_eq!(pt.active_row_count(), 2);

        cleanup(&path);
    }

    #[test]
    fn test_scan_all() {
        let path = temp_path("test_scan_all.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        for i in 0..10 {
            pt.insert_row(&[Value::Int32(i)]).unwrap();
        }

        let rows = pt.scan_all().unwrap();
        assert_eq!(rows.len(), 10);

        // Verify all values present (order may vary by page layout)
        let mut found: Vec<i32> = rows.iter().map(|r| {
            if let Value::Int32(v) = r.values[0] { v } else { panic!("expected Int32") }
        }).collect();
        found.sort();
        assert_eq!(found, (0..10).collect::<Vec<_>>());

        cleanup(&path);
    }

    #[test]
    fn test_scan_all_after_delete() {
        let path = temp_path("test_scan_del.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        let mut ctids = Vec::new();
        for i in 0..5 {
            ctids.push(pt.insert_row(&[Value::Int32(i)]).unwrap());
        }

        // Delete row 1 and 3
        pt.delete_row(ctids[1]).unwrap();
        pt.delete_row(ctids[3]).unwrap();

        let rows = pt.scan_all().unwrap();
        assert_eq!(rows.len(), 3);

        let mut found: Vec<i32> = rows.iter().map(|r| {
            if let Value::Int32(v) = r.values[0] { v } else { panic!("expected Int32") }
        }).collect();
        found.sort();
        assert_eq!(found, vec![0, 2, 4]);

        cleanup(&path);
    }

    #[test]
    fn test_scan_filtered() {
        let path = temp_path("test_scan_filter.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        for i in 0..10 {
            pt.insert_row(&[Value::Int32(i)]).unwrap();
        }

        // Filter: accept all rows (always true)
        let all = pt.scan_filtered(|_bytes| true).unwrap();
        assert_eq!(all.len(), 10);

        // Filter: reject all rows
        let none = pt.scan_filtered(|_bytes| false).unwrap();
        assert_eq!(none.len(), 0);

        cleanup(&path);
    }

    #[test]
    fn test_count_filtered() {
        let path = temp_path("test_count_filter.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        for i in 0..10 {
            pt.insert_row(&[Value::Int32(i)]).unwrap();
        }

        let count_all = pt.count_filtered(|_bytes| true).unwrap();
        assert_eq!(count_all, 10);

        let count_none = pt.count_filtered(|_bytes| false).unwrap();
        assert_eq!(count_none, 0);

        cleanup(&path);
    }

    #[test]
    fn test_get_rows_by_ctids() {
        let path = temp_path("test_batch_ctids.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        let mut ctids = Vec::new();
        for i in 0..20 {
            let ctid = pt.insert_row(&[Value::Int32(i), Value::varchar(format!("row_{}", i))]).unwrap();
            ctids.push(ctid);
        }

        // Fetch 5 specific rows
        let fetch = vec![ctids[2], ctids[7], ctids[11], ctids[15], ctids[19]];
        let rows = pt.get_rows_by_ctids(&fetch).unwrap();
        assert_eq!(rows.len(), 5);

        let mut found: Vec<i32> = rows.iter().map(|r| {
            if let Value::Int32(v) = r.values[0] { v } else { panic!("expected Int32") }
        }).collect();
        found.sort();
        assert_eq!(found, vec![2, 7, 11, 15, 19]);

        cleanup(&path);
    }

    #[test]
    fn test_get_rows_by_ctids_with_missing() {
        let path = temp_path("test_batch_missing.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();
        let ctid = pt.insert_row(&[Value::Int32(1)]).unwrap();

        // Request existing + non-existing ctids
        let fetch = vec![ctid, Ctid::new(999, 0), Ctid::new(1, 99)];
        let rows = pt.get_rows_by_ctids(&fetch).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Int32(1));

        cleanup(&path);
    }

    #[test]
    fn test_get_rows_by_ctids_filtered() {
        let path = temp_path("test_batch_filter.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        let mut ctids = Vec::new();
        for i in 0..20 {
            let ctid = pt.insert_row(&[Value::Int32(i)]).unwrap();
            ctids.push(ctid);
        }

        // Fetch all 20 ctids but filter: accept all
        let all = pt.get_rows_by_ctids_filtered(&ctids, |_| true).unwrap();
        assert_eq!(all.len(), 20);

        // Fetch all 20 ctids but filter: reject all
        let none = pt.get_rows_by_ctids_filtered(&ctids, |_| false).unwrap();
        assert_eq!(none.len(), 0);

        cleanup(&path);
    }

    #[test]
    fn test_insert_batch() {
        let path = temp_path("test_insert_batch.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        let rows: Vec<Vec<Value>> = (0..200)
            .map(|i| vec![Value::Int32(i), Value::varchar(format!("batch_row_{}", i))])
            .collect();

        let ctids = pt.insert_batch(&rows).unwrap();
        assert_eq!(ctids.len(), 200);
        assert_eq!(pt.active_row_count(), 200);

        // Verify every row is retrievable
        for (i, ctid) in ctids.iter().enumerate() {
            let row = pt.get_row(*ctid).unwrap().expect("row should exist");
            assert_eq!(row.values[0], Value::Int32(i as i32));
        }

        // scan_all should return all 200
        let all = pt.scan_all().unwrap();
        assert_eq!(all.len(), 200);

        cleanup(&path);
    }

    #[test]
    fn test_insert_batch_spans_pages() {
        let path = temp_path("test_batch_pages.pages");
        cleanup(&path);

        let mut pt = PagedTable::open(&path).unwrap();

        // Each row ~120 bytes → ~60 rows per 8KB page → 500 rows = ~8 pages
        let rows: Vec<Vec<Value>> = (0..500)
            .map(|i| vec![
                Value::Int64(i),
                Value::varchar(format!("value_{:0>80}", i)),
            ])
            .collect();

        let ctids = pt.insert_batch(&rows).unwrap();
        assert_eq!(ctids.len(), 500);
        assert_eq!(pt.active_row_count(), 500);

        // Verify data integrity at boundaries
        let first = pt.get_row(ctids[0]).unwrap().unwrap();
        assert_eq!(first.values[0], Value::Int64(0));

        let last = pt.get_row(ctids[499]).unwrap().unwrap();
        assert_eq!(last.values[0], Value::Int64(499));

        cleanup(&path);
    }

    #[test]
    fn test_reopen_preserves_data() {
        let path = temp_path("test_reopen.pages");
        cleanup(&path);

        let ctid;
        {
            let mut pt = PagedTable::open(&path).unwrap();
            ctid = pt.insert_row(&[Value::Int32(42), Value::varchar("persist".to_string())]).unwrap();
            assert_eq!(pt.active_row_count(), 1);
        }

        // Reopen
        {
            let mut pt = PagedTable::open(&path).unwrap();
            assert_eq!(pt.active_row_count(), 1);
            let row = pt.get_row(ctid).unwrap().expect("row should survive reopen");
            assert_eq!(row.values[0], Value::Int32(42));
            assert_eq!(row.values[1], Value::varchar("persist".to_string()));
        }

        cleanup(&path);
    }
}
