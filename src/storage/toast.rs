//! TOAST (The Oversized-Attribute Storage Technique) for large rows.
//!
//! Moves large VARCHAR fields to overflow pages when a row exceeds
//! TOAST_THRESHOLD. Value stays clean -- TOAST interception happens
//! at the raw bytes level.

use crate::error::{Error, Result};
use crate::storage::page::{Page, PageType, PAGE_SIZE};
use crate::storage::page_file::PageFile;
use crate::storage::value::Value;

/// Rows larger than this (in serialized bytes) trigger toasting.
const TOAST_THRESHOLD: usize = 2000;

/// Type tag for a TOAST pointer in serialized row data.
const TOAST_TAG: u8 = 0x07;

/// Size of a TOAST pointer: tag(1) + page_id(4) + offset(2) + length(4).
const TOAST_POINTER_SIZE: usize = 11;

/// Maximum data that fits in an overflow page (PAGE_SIZE minus 24-byte header).
const MAX_OVERFLOW_DATA: usize = PAGE_SIZE - 24;

/// Page header size (same as in page.rs).
const PAGE_HEADER_SIZE: usize = 24;

/// Encode a TOAST pointer as 11 bytes.
fn encode_toast_pointer(page_id: u32, offset: u16, length: u32) -> [u8; TOAST_POINTER_SIZE] {
    let mut buf = [0u8; TOAST_POINTER_SIZE];
    buf[0] = TOAST_TAG;
    buf[1..5].copy_from_slice(&page_id.to_le_bytes());
    buf[5..7].copy_from_slice(&offset.to_le_bytes());
    buf[7..11].copy_from_slice(&length.to_le_bytes());
    buf
}

/// Decode a TOAST pointer from bytes (must start with TOAST_TAG).
fn decode_toast_pointer(bytes: &[u8]) -> Result<(u32, u16, u32)> {
    if bytes.len() < TOAST_POINTER_SIZE || bytes[0] != TOAST_TAG {
        return Err(Error::Storage("Invalid TOAST pointer".to_string()));
    }
    let page_id = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
    let offset = u16::from_le_bytes(bytes[5..7].try_into().unwrap());
    let length = u32::from_le_bytes(bytes[7..11].try_into().unwrap());
    Ok((page_id, offset, length))
}

/// Write data to an overflow page, allocating a new one if needed.
///
/// Returns (page_id, offset_within_data_area).
fn _write_to_overflow(data: &[u8], page_file: &mut PageFile) -> Result<(u32, u16)> {
    if data.len() > MAX_OVERFLOW_DATA {
        return Err(Error::Storage(format!(
            "TOAST data ({} bytes) exceeds max overflow page capacity ({} bytes)",
            data.len(),
            MAX_OVERFLOW_DATA
        )));
    }

    // Try to find an existing overflow page with enough space
    // Scan pages looking for Overflow type with enough room
    let needed = data.len();
    let mut target_page_id: Option<u32> = None;
    let mut target_write_pos: u16 = 0;

    for pid in 1..page_file.page_count() {
        let page = page_file.read_page(pid)?;
        if page.page_type() != PageType::Overflow {
            continue;
        }
        // For overflow pages, free_space_start is the write cursor
        let used = page.header_free_space_start();
        let available = PAGE_SIZE - used as usize;
        if available >= needed {
            target_page_id = Some(pid);
            target_write_pos = used;
            break;
        }
    }

    let (page_id, write_pos) = if let Some(pid) = target_page_id {
        (pid, target_write_pos)
    } else {
        // Allocate a new overflow page
        let pid = page_file.allocate_page()?;
        // Rewrite it as an overflow page
        let mut raw = [0u8; PAGE_SIZE];
        raw[0..4].copy_from_slice(&pid.to_le_bytes());
        raw[4] = PageType::Overflow as u8;
        // free_space_start = PAGE_HEADER_SIZE (write cursor at start of data area)
        raw[8..10].copy_from_slice(&(PAGE_HEADER_SIZE as u16).to_le_bytes());
        // free_space_end = PAGE_SIZE
        raw[10..12].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        let page = Page::from_bytes(raw)?;
        page_file.write_page(&page)?;
        (pid, PAGE_HEADER_SIZE as u16)
    };

    // Write the data
    let page = page_file.read_page(page_id)?;
    let page_bytes = page.to_bytes();
    let mut new_bytes = page_bytes;
    let wp = write_pos as usize;
    new_bytes[wp..wp + data.len()].copy_from_slice(data);

    // Update write cursor (free_space_start)
    let new_cursor = (wp + data.len()) as u16;
    new_bytes[8..10].copy_from_slice(&new_cursor.to_le_bytes());

    let updated_page = Page::from_bytes(new_bytes)?;
    page_file.write_page(&updated_page)?;

    // Offset is relative to data area start (after header)
    let offset = write_pos - PAGE_HEADER_SIZE as u16;

    Ok((page_id, offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toast_pointer_roundtrip() {
        let encoded = encode_toast_pointer(42, 100, 5000);
        assert_eq!(encoded[0], TOAST_TAG);
        let (pid, off, len) = decode_toast_pointer(&encoded).unwrap();
        assert_eq!(pid, 42);
        assert_eq!(off, 100);
        assert_eq!(len, 5000);
    }

    #[test]
    fn test_decode_invalid_pointer() {
        assert!(decode_toast_pointer(&[0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
        assert!(decode_toast_pointer(&[TOAST_TAG, 0, 0]).is_err()); // too short
    }
}
