//! PagedTable: CRUD layer over PageFile + Page + TOAST.
//!
//! Wraps a `PageFile` and provides row-level operations (insert, get,
//! delete, update, scan) using slotted pages with automatic TOAST for
//! oversized rows.

use crate::error::Result;
use crate::storage::page::{Ctid, Page, PageType, PAGE_SIZE, serialize_row_for_page};
use crate::storage::page_file::PageFile;
use crate::storage::toast;
use crate::storage::value::Value;
use crate::storage::row::Row;
use std::path::Path;

/// Rows larger than this (serialized bytes) trigger TOAST.
const TOAST_THRESHOLD: usize = 2000;

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

    /// Return the number of active (non-deleted) rows.
    pub fn active_row_count(&self) -> usize {
        self.active_count as usize
    }
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
