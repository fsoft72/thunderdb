# TOAST Overflow Design — Sub-project 3

Third phase of the storage format migration. Implements overflow pages
for rows that exceed the page capacity, building on Page (sub-project 1)
and PageFile (sub-project 2).

## When TOAST Activates

When a serialized row exceeds `TOAST_THRESHOLD` (2000 bytes, ~1/4 of an
8KB page), the largest VARCHAR fields are moved to overflow pages one at
a time until the row fits within the threshold.

The toasting logic lives in a standalone module — `Page` and `PageFile`
remain unaware of TOAST. The sub-project 4 (PagedTable) will call the
toast functions before inserting into pages.

## Constants

```rust
const TOAST_THRESHOLD: usize = 2000;
const TOAST_TAG: u8 = 0x07;
const TOAST_POINTER_SIZE: usize = 11;
const MAX_OVERFLOW_DATA: usize = PAGE_SIZE - 24; // 8168 bytes
```

## TOAST Pointer Format

When a VARCHAR field is moved to overflow, its serialized bytes in the
row are replaced with a TOAST pointer:

```
[TOAST_TAG: u8 = 0x07][page_id: u32 LE][offset: u16 LE][length: u32 LE]
```

Total: 11 bytes. `TOAST_TAG` (7) does not conflict with existing Value
type tags (0-6).

- `page_id`: the overflow page containing the data
- `offset`: byte offset within the overflow page (after 24-byte header)
- `length`: byte length of the original serialized VARCHAR value
  (including its type tag + length prefix + string bytes)

## Overflow Page Layout

Overflow pages use `page_type = Overflow` (1). No slot directory — data
is written sequentially after the 24-byte header.

```
[Page Header: 24 bytes, page_type=1]
[overflow data: up to 8168 bytes]
```

Multiple toasted fields can share the same overflow page if they fit.
A field that exceeds 8168 bytes is an error in this version (future
work: chain multiple overflow pages).

The overflow page tracks how much data it contains via the
`free_space_start` header field (repurposed as write cursor for overflow
pages, since there is no slot directory): starts at 24 (header size),
advances as data is appended. Available space =
`PAGE_SIZE - free_space_start`.

## Design Principle: Value Stays Clean

`Value` knows nothing about TOAST. The toast module intercepts raw
row bytes and replaces TOAST pointers with the real data before
`Value::from_bytes()` sees them. From Value's perspective, it only
ever sees normal type tags (0-6).

## API

New file: `src/storage/toast.rs`

```rust
/// Serialize row values, toasting large VARCHARs to overflow pages.
///
/// If the serialized row exceeds TOAST_THRESHOLD, the largest VARCHAR
/// fields are moved to overflow pages one at a time (largest first)
/// until the row fits.
///
/// Returns the (possibly toasted) row bytes ready for page insertion.
pub fn toast_row(
    values: &[Value],
    page_file: &mut PageFile,
) -> Result<Vec<u8>>
```

Steps:
1. Serialize all values with `serialize_row_for_page()`.
2. If result <= `TOAST_THRESHOLD`, return as-is.
3. Collect `(col_index, serialized_size)` for all VARCHAR columns.
4. Sort by size descending.
5. For the largest VARCHAR: allocate space in an overflow page, write
   the original serialized bytes there, replace the field in the row
   with a TOAST pointer.
6. Repeat until the row fits or no more VARCHAR fields can be toasted.
7. If still too large, return error.

```rust
/// Resolve TOAST pointers in raw row bytes.
///
/// Scans the row for TOAST_TAG (0x07) type tags. For each one, reads
/// the overflow page and replaces the pointer with the original data.
/// Returns the detoasted row bytes.
pub fn detoast_row_bytes(
    row_bytes: &[u8],
    page_file: &mut PageFile,
) -> Result<Vec<u8>>
```

Steps:
1. Parse `col_count` and offsets from the row header.
2. Walk through each value's type tag.
3. If tag == `TOAST_TAG`: read `(page_id, offset, length)`, fetch the
   overflow data from the page, write the original bytes into the
   output buffer.
4. If tag != `TOAST_TAG`: copy the value bytes as-is.
5. Rebuild the offset array for the detoasted row.

```rust
/// Free overflow pages referenced by TOAST pointers in a row.
///
/// Called when a row is deleted. Scans for TOAST_TAG entries and
/// marks the corresponding overflow space as available.
pub fn free_toast_data(
    row_bytes: &[u8],
    page_file: &mut PageFile,
) -> Result<()>
```

For this first version, freeing simply updates the FSM to reflect
the freed space. Full overflow page garbage collection is future work.

### Helper: Overflow Page Management

```rust
/// Find or allocate an overflow page with enough space, write data,
/// return (page_id, offset).
fn write_to_overflow(
    data: &[u8],
    page_file: &mut PageFile,
) -> Result<(u32, u16)>
```

Steps:
1. Scan existing overflow pages for one with enough free space.
2. If none found, allocate a new overflow page.
3. Append data after the existing content in the overflow page.
4. Update the page's `free_space_end` (used as write cursor).
5. Return `(page_id, offset_within_page)`.

## File Structure

New file: `src/storage/toast.rs`

Depends on:
- `crate::storage::page::{Page, PageType, PAGE_SIZE, serialize_row_for_page}`
- `crate::storage::page_file::PageFile`
- `crate::storage::value::Value`
- `crate::error`

Register in `src/storage/mod.rs`:
```rust
pub mod toast;
```

## Testing

Unit tests cover:
- Small row (< threshold): toast_row returns unchanged bytes
- Large row (> threshold): toast_row moves largest VARCHAR to overflow,
  row bytes contain TOAST pointer
- detoast_row_bytes resolves pointer back to original data
- Round-trip: toast → detoast produces identical values
- Multiple toasted fields in one row
- Row at exactly the threshold boundary
- free_toast_data marks overflow space as available
- Error on field too large for a single overflow page (> 8168 bytes)

## Out of Scope

- Chained overflow pages for fields > 8168 bytes
- Compression of toasted data
- Integration with PagedTable (sub-project 4)
- Overflow page garbage collection / compaction
