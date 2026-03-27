# PageFile + FSM Design — Sub-project 2

Second phase of the storage format migration. Implements multi-page file
management with free space tracking and mmap support, building on the
Page struct from sub-project 1.

## File Layout

```
[Page 0: FSM][Page 1: data][Page 2: data]...[Page N: data]
```

Page 0 is reserved for the Free Space Map. Data pages start at page_id 1.
The file size is always a multiple of `PAGE_SIZE` (8192 bytes).

## Free Space Map (FSM)

The FSM occupies page 0. After the 24-byte page header (page_type =
`FreeSpaceMap`), the remaining 8168 bytes each represent one data page.

Each byte encodes the available free space of the corresponding page in
32-byte increments:

```
fsm_value = page.free_space() / 32
```

- `0` = page full (< 32 bytes free)
- `255` = page empty (~8KB free)

This supports up to 8168 data pages = ~64MB of data. For larger files,
additional FSM pages can be added later — this is sufficient for the
current scope.

### FSM Operations

**Find page with space:** Linear scan of the FSM array for the first byte
`>= ceil(needed_bytes / 32)`. Returns the corresponding page_id
(fsm_index + 1, since page 0 is the FSM itself). If no page found,
allocate a new page.

**Update after insert/delete:** After modifying a page, compute
`page.free_space() / 32` and write it to `fsm[page_id - 1]`.

## PageFile Struct

```rust
pub struct PageFile {
    path: PathBuf,
    file: File,
    mmap: Option<Mmap>,
    page_count: u32,
    stale_mmap: bool,
}
```

- `file`: opened read+write, used for all writes
- `mmap`: optional memory map for zero-copy reads
- `page_count`: cached count of pages in the file (file_size / PAGE_SIZE)
- `stale_mmap`: set to `true` after writes, cleared after remap

## API

### Construction

```rust
PageFile::open(path: &Path) -> Result<Self>
```

Opens or creates the file. If the file is empty, initializes it with
page 0 (empty FSM page). Sets up mmap if the file is non-empty.
Computes `page_count` from file size.

### Page I/O

```rust
PageFile::read_page(&mut self, page_id: u32) -> Result<Page>
```

Reads an 8KB page. Uses mmap when available (remaps if stale), falls
back to seek+read.

```rust
PageFile::write_page(&mut self, page: &Page) -> Result<()>
```

Writes an 8KB page to the file at `page_id * PAGE_SIZE`. Marks mmap
as stale.

### Allocation

```rust
PageFile::allocate_page(&mut self) -> Result<u32>
```

Extends the file by 8KB, writes an empty data page, increments
`page_count`, returns the new page_id.

### FSM Operations

```rust
PageFile::find_page_with_space(&mut self, needed: usize) -> Result<u32>
```

Scans the FSM for a page with enough free space. If none found,
allocates a new page. Returns the page_id.

```rust
PageFile::update_fsm(&mut self, page_id: u32, free_space: usize) -> Result<()>
```

Updates the FSM entry for the given page_id. Computes
`free_space / 32` and writes to the FSM page at the appropriate
offset.

```rust
PageFile::page_count(&self) -> u32
```

Returns the total number of pages (including page 0).

### mmap

```rust
PageFile::remap(&mut self) -> Result<()>
```

Private method. Remaps the mmap if stale. Called lazily before reads.

With mmap, `read_page` accesses `&mmap[page_id * PAGE_SIZE .. (page_id + 1) * PAGE_SIZE]`
directly — zero syscalls, zero copies. The bytes are parsed into a
`Page` via `Page::from_bytes()`.

Without mmap (fallback), `read_page` does `seek + read_exact` into a
buffer.

Writes always go through the `File` handle (not the mmap). After any
write, `stale_mmap = true`. Before the next read, `remap()` is called.

## File Structure

New file: `src/storage/page_file.rs`

Depends on:
- `crate::storage::page::{Page, PageType, PAGE_SIZE}`
- `crate::error`
- `memmap2::Mmap`

Does NOT depend on DataFile, RAT, TableEngine, or any query layer code.

Register in `src/storage/mod.rs`:
```rust
pub mod page_file;
pub use page_file::PageFile;
```

## Testing

Unit tests cover:
- Create new file: FSM page initialized, page_count = 1
- Allocate page: page_count increments, page is readable
- Write and read page round-trip
- FSM update and find_page_with_space
- Allocate multiple pages, fill some, find space correctly
- mmap read path (verify data matches write)
- Reopen existing file: page_count restored, data intact

Integration test: insert rows via Page API into pages managed by
PageFile, read them back.

## Out of Scope

- TOAST overflow pages (sub-project 3)
- Integration with TableEngine (sub-project 4)
- Multiple FSM pages for files > 64MB
- Concurrent access / locking
