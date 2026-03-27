//! Multi-page file manager with Free Space Map.
//!
//! Manages a file as a sequence of 8KB pages. Page 0 is the FSM.
//! Data pages start at page_id 1.

use crate::error::{Error, Result};
use crate::storage::page::{Page, PageType, PAGE_SIZE};
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Free space is encoded in 32-byte increments.
const FSM_GRANULARITY: usize = 32;

/// Byte offset of FSM data within page 0 (after 24-byte page header).
const FSM_DATA_OFFSET: usize = 24;

/// Maximum data pages tracked by a single FSM page.
const FSM_CAPACITY: usize = PAGE_SIZE - FSM_DATA_OFFSET;

/// Multi-page file manager with Free Space Map.
pub struct PageFile {
    path: PathBuf,
    file: File,
    mmap: Option<Mmap>,
    page_count: u32,
    stale_mmap: bool,
}

impl PageFile {
    /// Open or create a page file.
    ///
    /// If the file is empty, initializes it with an empty FSM page (page 0).
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let mut pf = Self {
            path: path.to_path_buf(),
            file,
            mmap: None,
            page_count: (file_len / PAGE_SIZE as u64) as u32,
            stale_mmap: false,
        };

        if file_len == 0 {
            // Initialize with FSM page
            pf.init_fsm_page()?;
        }

        // Set up mmap
        if pf.page_count > 0 {
            pf.setup_mmap()?;
        }

        Ok(pf)
    }

    /// Initialize page 0 as an empty FSM page.
    fn init_fsm_page(&mut self) -> Result<()> {
        let mut data = [0u8; PAGE_SIZE];
        // Write page header: page_id=0, page_type=FreeSpaceMap
        data[0..4].copy_from_slice(&0u32.to_le_bytes()); // page_id
        data[4] = PageType::FreeSpaceMap as u8;           // page_type
        // rest is zeros (flags, slot_count, free_space_start, etc.)

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&data)?;
        self.file.sync_all()?;
        self.page_count = 1;
        self.stale_mmap = true;
        Ok(())
    }

    /// Set up or refresh the memory map.
    fn setup_mmap(&mut self) -> Result<()> {
        let file_len = self.file.metadata()?.len();
        if file_len > 0 {
            self.mmap = Some(unsafe { Mmap::map(&self.file)? });
            self.stale_mmap = false;
        }
        Ok(())
    }

    /// Remap if stale (called lazily before reads).
    fn remap_if_needed(&mut self) -> Result<()> {
        if self.stale_mmap {
            self.file.sync_all()?;
            self.setup_mmap()?;
        }
        Ok(())
    }

    /// Read a page by ID.
    ///
    /// Uses mmap when available for zero-copy access.
    pub fn read_page(&mut self, page_id: u32) -> Result<Page> {
        if page_id >= self.page_count {
            return Err(Error::Storage(format!(
                "Page {} out of bounds (file has {} pages)",
                page_id, self.page_count
            )));
        }

        self.remap_if_needed()?;

        let offset = page_id as usize * PAGE_SIZE;

        if let Some(ref mmap) = self.mmap {
            let end = offset + PAGE_SIZE;
            if end > mmap.len() {
                return Err(Error::Storage(format!(
                    "mmap read out of bounds: {} > {}",
                    end,
                    mmap.len()
                )));
            }
            let mut buf = [0u8; PAGE_SIZE];
            buf.copy_from_slice(&mmap[offset..end]);
            Page::from_bytes(buf)
        } else {
            let mut buf = [0u8; PAGE_SIZE];
            self.file.seek(SeekFrom::Start(offset as u64))?;
            self.file.read_exact(&mut buf)?;
            Page::from_bytes(buf)
        }
    }

    /// Write a page to the file.
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let page_id = page.page_id();
        if page_id >= self.page_count {
            return Err(Error::Storage(format!(
                "Page {} out of bounds (file has {} pages)",
                page_id, self.page_count
            )));
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        let bytes = page.to_bytes();

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&bytes)?;
        self.stale_mmap = true;

        Ok(())
    }

    /// Allocate a new empty data page at the end of the file.
    ///
    /// Returns the new page_id.
    pub fn allocate_page(&mut self) -> Result<u32> {
        let page_id = self.page_count;
        let page = Page::new(page_id);
        let bytes = page.to_bytes();

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&bytes)?;

        self.page_count += 1;
        self.stale_mmap = true;

        Ok(page_id)
    }

    /// Total number of pages (including FSM page 0).
    pub fn page_count(&self) -> u32 {
        self.page_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_path(name: &str) -> PathBuf {
        let dir = PathBuf::from("/tmp/thunderdb_page_file_tests");
        let _ = fs::create_dir_all(&dir);
        dir.join(name)
    }

    fn cleanup(path: &Path) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_open_creates_fsm_page() {
        let path = temp_path("test_open.pages");
        cleanup(&path);

        let pf = PageFile::open(&path).unwrap();
        assert_eq!(pf.page_count(), 1); // just the FSM page

        // File should be exactly 1 page
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), PAGE_SIZE as u64);

        cleanup(&path);
    }

    #[test]
    fn test_write_and_read_page() {
        let path = temp_path("test_rw.pages");
        cleanup(&path);

        let mut pf = PageFile::open(&path).unwrap();

        // Manually extend file for page 1
        let page_id = pf.allocate_page().unwrap();
        assert_eq!(page_id, 1);

        // Create a page with data
        let mut page = Page::new(page_id);
        let row_data = crate::storage::page::serialize_row_for_page(
            &vec![crate::storage::Value::Int32(42)],
        );
        page.insert_row(&row_data).unwrap();

        // Write and read back
        pf.write_page(&page).unwrap();
        let restored = pf.read_page(page_id).unwrap();
        assert_eq!(restored.page_id(), page_id);
        assert_eq!(restored.active_count(), 1);

        let row = restored.get_row(0).unwrap();
        assert_eq!(row, &row_data[..]);

        cleanup(&path);
    }

    #[test]
    fn test_reopen_existing_file() {
        let path = temp_path("test_reopen.pages");
        cleanup(&path);

        {
            let _pf = PageFile::open(&path).unwrap();
        }

        // Reopen
        let pf = PageFile::open(&path).unwrap();
        assert_eq!(pf.page_count(), 1);

        cleanup(&path);
    }
}
