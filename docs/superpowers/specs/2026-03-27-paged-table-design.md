# PagedTable + Integration Design — Sub-project 4

Final phase of the storage format migration. Implements `PagedTable` as the
CRUD layer over `PageFile` + `Page` + TOAST, and rewires `TableEngine` to
use it instead of `DataFile` + `RAT`.

## PagedTable

### Struct

```rust
pub struct PagedTable {
    page_file: PageFile,
    active_count: u64,
}
```

- `page_file`: manages the page-based file
- `active_count`: cached count of live rows (incremented on insert,
  decremented on delete)

### Construction

```rust
PagedTable::open(path: &Path) -> Result<Self>
```

Opens or creates a page file. On open, scans all data pages to compute
`active_count` (sum of `page.active_count()` across all data pages).

### Row Operations

```rust
PagedTable::insert_row(&mut self, values: &[Value]) -> Result<Ctid>
```

1. Serialize with `serialize_row_for_page(values)`.
2. If too large, `toast_row(values, &mut self.page_file)`.
3. `page_file.find_page_with_space(bytes.len())` → page_id.
4. Read page, insert row, write page, update FSM.
5. Increment `active_count`.
6. Return `Ctid { page_id, slot_index }`.

```rust
PagedTable::get_row(&mut self, ctid: Ctid) -> Result<Option<Row>>
```

1. Read page at `ctid.page_id`.
2. `page.get_row(ctid.slot_index)` → raw bytes (None if free).
3. `detoast_row_bytes(raw, &mut self.page_file)` → detoasted.
4. Deserialize values from detoasted bytes.
5. Return `Row { row_id: ctid.to_u64(), values }`.

```rust
PagedTable::delete_row(&mut self, ctid: Ctid) -> Result<bool>
```

1. Read page, get raw bytes for the slot.
2. `free_toast_data(raw, &mut self.page_file)`.
3. `page.delete_row(ctid.slot_index)`.
4. Write page, update FSM.
5. Decrement `active_count`.
6. Return true (false if slot was already free).

```rust
PagedTable::update_row(&mut self, ctid: Ctid, values: &[Value]) -> Result<Ctid>
```

1. Delete old row at ctid.
2. Insert new values → new_ctid.
3. Return new_ctid. The caller (TableEngine) updates B-tree indexes.

### Scan Operations

```rust
PagedTable::scan_all(&mut self) -> Result<Vec<Row>>
```

Iterate all data pages (page_id 1..page_count). For each page, iterate
all active slots. Detoast and deserialize each row. Row ID =
`Ctid::new(page_id, slot_index).to_u64()`.

```rust
PagedTable::scan_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool
```

Same as `scan_all` but applies predicate on raw bytes before
deserializing. Rows that fail the predicate are skipped without
deserialization. TOAST pointers in predicate columns are NOT resolved
during filtering — the predicate sees the toasted row bytes. For
non-toasted columns (the common case for filter targets like integer
IDs), this works correctly.

```rust
PagedTable::count_filtered<F>(&mut self, predicate: F) -> Result<usize>
where F: Fn(&[u8]) -> bool
```

Same as `scan_filtered` but counts matches instead of collecting rows.

```rust
PagedTable::active_row_count(&self) -> usize
```

Returns cached `active_count`.

### Indexed Fetch

```rust
PagedTable::get_rows_by_ctids(&mut self, ctids: &[Ctid]) -> Result<Vec<Row>>
```

Reads rows by ctid. Groups ctids by page_id to minimize page reads —
reads each page once, extracts all requested slots, detoasts and
deserializes.

```rust
PagedTable::get_rows_by_ctids_filtered<F>(
    &mut self,
    ctids: &[Ctid],
    predicate: F,
) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool
```

Same but applies predicate before deserialization.

## TableEngine Integration

### Fields Replaced

```rust
// Before:
pub struct TableEngine {
    data_file: DataFile,
    rat: RecordAddressTable,
    index_manager: IndexManager,
    // ...
}

// After:
pub struct TableEngine {
    paged_table: PagedTable,
    index_manager: IndexManager,
    // ...
}
```

`DataFile` and `RecordAddressTable` are removed from TableEngine. The
files `data_file.rs` and `rat.rs` remain in the codebase (not deleted)
but are no longer used by TableEngine.

### Method Mapping

| TableEngine method | Before (DataFile+RAT) | After (PagedTable) |
|---|---|---|
| `insert_row(values)` | RAT assign ID, DataFile append | `paged_table.insert_row(values)` → ctid, return `ctid.to_u64()` |
| `insert_batch(rows)` | Bulk RAT + DataFile batch | Loop `insert_row` per row |
| `get_by_id(row_id)` | RAT lookup → DataFile read | `Ctid::from_u64(row_id)` → `paged_table.get_row(ctid)` |
| `delete_by_id(row_id)` | RAT + DataFile tombstone | `paged_table.delete_row(Ctid::from_u64(row_id))` |
| `update_row(row_id, values)` | Delete + re-insert | `paged_table.update_row(ctid, values)` → new_ctid, update indexes |
| `get_by_ids(row_ids)` | Sorted offset fetch | Convert to ctids → `paged_table.get_rows_by_ctids()` |
| `get_by_ids_filtered(row_ids, pred)` | read_raw_with + pred | Convert to ctids → `paged_table.get_rows_by_ctids_filtered()` |
| `scan_all()` | DataFile sequential scan | `paged_table.scan_all()` |
| `scan_all_filtered(pred)` | DataFile callback scan | `paged_table.scan_filtered(pred)` |
| `count_filtered(pred)` | DataFile count callback | `paged_table.count_filtered(pred)` |
| `active_row_count()` | RAT active_count | `paged_table.active_row_count()` |
| `flush()` | RAT save + DataFile sync | `page_file` sync (mmap handles reads) |
| `compact()` | RAT compact | Per-page `page.compact()` for fragmented pages |

### Index Updates

B-tree indexes store `u64` values which are now packed ctids. The flow
is unchanged — `IndexManager::insert_row` and `delete_row` accept u64
row_ids.

On `update_row`: if the ctid changes (row moved to different
page/slot), the old ctid is removed from all indexes and the new ctid
is inserted. This is already the pattern in the current `update_row`.

### Constructor Changes

```rust
TableEngine::open(name, base_dir, config, btree_order) -> Result<Self>
```

Opens `PagedTable` from `base_dir/name/pages.bin` instead of
`data.bin` + `rat.bin`. Index files (`.idx`) remain in the same
location.

`open_in_memory` creates a `PagedTable` backed by an in-memory
`PageFile` (future work — for now, uses a temp file).

### What Gets Removed from TableEngine

- `data_file: DataFile` field
- `rat: RecordAddressTable` field
- `next_row_id: u64` field (ctids are generated by PagedTable)
- `fetch_rows_sorted_by_offset()` private method
- `read_batch_sequential` usage (page-based I/O replaces it)
- All `DataFile::*` method calls
- All `RAT::*` method calls

## File Structure

- New: `src/storage/paged_table.rs` — PagedTable struct with CRUD + scan
- Modify: `src/storage/table_engine.rs` — replace DataFile+RAT with PagedTable
- Modify: `src/storage/mod.rs` — register paged_table module

## Testing

### PagedTable unit tests (in paged_table.rs)

- Insert and get single row
- Insert and get multiple rows
- Delete row, verify get returns None
- Update row, verify new data at new ctid
- scan_all returns all active rows
- scan_filtered skips non-matching rows
- count_filtered counts correctly
- get_rows_by_ctids batch fetch
- TOAST integration: insert large row, get it back
- active_row_count accuracy after inserts and deletes

### Integration tests

- All existing integration tests must pass (blog_benchmark,
  thunderdb_vs_sqlite_bench, join_test, etc.)
- The benchmark should show improvement from page-based I/O

## Out of Scope

- Forwarding pointers for moved rows
- In-memory PageFile backend
- WAL (Write-Ahead Log)
- MVCC / transactions
