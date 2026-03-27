# Slotted Page Storage — Sub-project 1: Page Struct + Ctid

First phase of the storage format migration from append-only to slotted
page format. This sub-project implements the core Page data structure and
Ctid addressing — no file I/O, no integration with TableEngine.

## Architecture Overview (full project)

The full migration spans 4 sub-projects:

1. **Page struct + Ctid** (this spec) — page format, slot management, CRUD
   on a single in-memory page
2. **PageFile + FSM** — multi-page file management, free space map, mmap
3. **TOAST overflow** — overflow pages for rows exceeding page capacity
4. **PagedTable + Integration** — replaces DataFile + RAT, wires into
   TableEngine and indexes

Each sub-project gets its own spec → plan → implementation cycle.

## Constants

```rust
const PAGE_SIZE: usize = 8192;        // 8 KB
const PAGE_HEADER_SIZE: usize = 24;
const SLOT_SIZE: usize = 4;           // 2 bytes offset + 2 bytes length
const INVALID_SLOT: u16 = 0xFFFF;     // sentinel for empty free list
```

## Page Layout

```
Offset  Size   Field
──────  ─────  ──────────────────────────────
0       4      page_id: u32 LE
4       1      page_type: u8 (DATA=0, OVERFLOW=1, FSM=2)
5       1      flags: u8 (reserved)
6       2      slot_count: u16 LE — total slots (active + free)
8       2      free_space_start: u16 LE — byte offset after last slot entry
10      2      free_space_end: u16 LE — byte offset before first row data
12      2      first_free_slot: u16 LE — head of free slot list (INVALID_SLOT = none)
14      2      active_count: u16 LE — number of live rows
16      8      reserved: [0u8; 8]
24      ...    slot directory (grows →)
...     ...    free space gap
...     ...    row data (grows ←, from byte 8191 backward)
```

**Slot directory** starts at byte 24, each entry is 4 bytes:
- `offset: u16` — byte offset of row data within the page.
  For free slots, set to `INVALID_SLOT`.
- `length: u16` — byte length of row data.
  For free slots, stores the index of the next free slot
  (`INVALID_SLOT` = end of free list).

**Row data** grows backward from byte 8191. The first inserted row
occupies `[8192 - row_len .. 8192]`. Rows are not ordered — freed space
creates gaps that can be reclaimed by compaction.

**Free space** is the gap between `free_space_start` (end of slot
directory) and `free_space_end` (start of row data area). A row of size
`N` can be inserted if `free_space_end - free_space_start >= N + SLOT_SIZE`
(when a new slot must be allocated) or `>= N` (when reusing a free slot).

## Row Format Inside Page

```
[col_count: u16 LE][off0: u16 LE]...[offN-1: u16 LE][val0]...[valN-1]
```

No `row_id` stored — the row's identity is its ctid `(page_id, slot_index)`,
reconstructed at read time. `col_count` is `u16` (max 65535 columns).
Column offsets are `u16` relative to the start of the values area (same
semantics as the current row format, minus the 8-byte row_id header).

## Ctid

Physical row address: `(page_id: u32, slot_index: u16)`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Ctid {
    pub page_id: u32,
    pub slot_index: u16,
}
```

Packed into `u64` for B-tree index compatibility:

```rust
impl Ctid {
    pub fn to_u64(self) -> u64 {
        (self.page_id as u64) << 16 | self.slot_index as u64
    }

    pub fn from_u64(val: u64) -> Self {
        Self {
            page_id: (val >> 16) as u32,
            slot_index: (val & 0xFFFF) as u16,
        }
    }
}
```

Supports 2^32 pages × 2^16 slots = files up to 32 TB.

## Page Type

```rust
#[repr(u8)]
pub enum PageType {
    Data = 0,
    Overflow = 1,
    FreeSpaceMap = 2,
}
```

This sub-project only implements `Data` pages. `Overflow` and
`FreeSpaceMap` are added in sub-projects 2 and 3.

## Page API

```rust
pub struct Page {
    header: PageHeader,
    data: [u8; PAGE_SIZE],
}
```

The `data` array holds the entire page including the header. The `header`
struct is a parsed view of `data[0..24]` kept in sync.

### Construction

```rust
Page::new(page_id: u32) -> Self
```

Creates an empty data page. `free_space_start = PAGE_HEADER_SIZE`,
`free_space_end = PAGE_SIZE`, `slot_count = 0`, `active_count = 0`,
`first_free_slot = INVALID_SLOT`.

```rust
Page::from_bytes(bytes: [u8; PAGE_SIZE]) -> Result<Self>
```

Parses the header from the byte array.

### Serialization

```rust
Page::to_bytes(&self) -> [u8; PAGE_SIZE]
```

Returns the raw page bytes with the header written back.

### Row Operations

```rust
Page::insert_row(&mut self, row_data: &[u8]) -> Result<u16>
```

Inserts a row. Returns the slot index. Steps:
1. Check if row fits: `row_data.len()` must not exceed available space.
2. If `first_free_slot != INVALID_SLOT`, reuse that slot. Otherwise
   allocate a new slot at `slot_count` and increment `slot_count`,
   advancing `free_space_start` by `SLOT_SIZE`.
3. Write `row_data` at `free_space_end - row_data.len()`. Update
   `free_space_end`.
4. Set slot's `offset` and `length`.
5. Increment `active_count`.

```rust
Page::get_row(&self, slot_index: u16) -> Option<&[u8]>
```

Returns a slice of the row data, or `None` if the slot is free or out
of bounds.

```rust
Page::delete_row(&mut self, slot_index: u16) -> bool
```

Marks a slot as free. Adds it to the head of the free slot list.
Decrements `active_count`. Returns `false` if slot was already free
or out of bounds. Does NOT reclaim the row data space (requires
compaction).

```rust
Page::free_space(&self) -> usize
```

Returns `free_space_end - free_space_start` — the contiguous free
space available. Note: after deletes, there may be additional
reclaimable space in gaps left by deleted rows, but this is only
available after compaction.

```rust
Page::compact(&mut self)
```

Rewrites all active rows contiguously from the end of the page,
eliminating gaps from deleted rows. Updates all slot offsets.
Reclaims fragmented space into contiguous free space.

```rust
Page::active_count(&self) -> u16
```

Returns the number of live rows.

### Value extraction

```rust
Page::value_at(&self, slot_index: u16, col_idx: usize) -> Result<Value>
```

Extracts a single column value from a row without deserializing the
full row. Uses the column-offset array in the row data. Equivalent
to `Row::value_at()` but adjusted for the page row format (no row_id
in the header, `col_count` is `u16` not `u32`).

## File Structure

New file: `src/storage/page.rs`

Contains:
- `Ctid` struct
- `PageType` enum
- `Page` struct with all methods
- Constants (`PAGE_SIZE`, `PAGE_HEADER_SIZE`, `SLOT_SIZE`, `INVALID_SLOT`)
- Unit tests

No dependencies on DataFile, RAT, TableEngine, or any query layer code.
Only depends on `crate::error` and `crate::storage::value::Value` (for
`value_at`).

## Serialization Compatibility

The row format inside pages uses `u16` for `col_count` instead of the
current `u32`. A new serialization function is needed:

```rust
/// Serialize row values for page storage (no row_id, u16 col_count).
pub fn serialize_row_for_page(values: &[Value]) -> Vec<u8>
```

This is a standalone function in `page.rs`, not a method on `Row`. It
produces the page row format directly. For reading, `Page::get_row()`
returns the raw bytes which can be parsed with a page-specific
deserialization function or the existing `Value::from_bytes()`.

## Testing

Unit tests cover:
- Empty page creation and serialization round-trip
- Insert single row, read it back
- Insert multiple rows until page is full
- Delete a row, verify slot is freed
- Reuse freed slot on next insert
- Compact after deletes, verify space reclaimed
- Ctid pack/unpack round-trip
- value_at on page rows
- Edge cases: insert into full page (error), delete invalid slot, empty row

## Out of Scope (this sub-project)

- File I/O / PageFile (sub-project 2)
- Free Space Map (sub-project 2)
- TOAST overflow pages (sub-project 3)
- Integration with TableEngine / indexes (sub-project 4)
- Forwarding pointers for moved rows (sub-project 4)
