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
