# SP3b: Thunder Write-Path Optimization

**Date:** 2026-04-24
**Status:** Approved
**Motivation:** SP3 benchmarks revealed Thunder loses W5 (UPDATE by PK, 139x), W7 (DELETE by PK, 94x), W6 (UPDATE by indexed col, 3.4x), W8 (DELETE by range, 2.9x), W9 (Mixed, 98x). Root cause: every update is delete+insert (2 page cycles per row), every delete is a standalone page round-trip, index maintenance fires per-row with no batching or short-circuit.

---

## Goals

1. Collapse per-row page I/O to per-page I/O for bulk UPDATE and DELETE.
2. Enable in-place slot overwrite when new row fits existing slot (eliminates FSM scan + reallocation).
3. Skip B-tree index ops when the indexed column value is unchanged.
4. Batch B-tree deletions (sort by value) for cache locality, mirroring existing `insert_rows_batch`.

## Non-Goals

- Page compaction / dead-space reclamation (future work).
- WAL / durability changes.
- Query planner changes.
- Any read-path changes.

---

## Architecture

Five layers touched top-to-bottom:

```
lib.rs::update() / delete()
    ↓  collects rows first (already done), builds batch input
TableEngine::update_batch / delete_batch
    ↓  smart index skip; routes to page-level batch
PagedTable::update_batch / delete_batch
    ↓  groups by page_id, one read+write per page; in-place where new_len ≤ old_len
Page::update_row_inplace
    ↓  overwrites slot bytes, updates slot length field
IndexManager::delete_rows_batch
    ↓  sorts (old_val, row_id) per column, batch B-tree deletes
```

---

## Layer Designs

### 1. `Page::update_row_inplace`

```rust
/// Overwrite slot data in-place when new_data fits within the existing slot length.
/// Returns true if updated; false if new_data.len() > old slot length (caller must relocate).
/// Does NOT change active_count. Dead bytes at tail of slot are abandoned.
pub fn update_row_inplace(&mut self, slot: u16, new_data: &[u8]) -> bool
```

Algorithm:
1. Read `(offset, old_len)` from slot directory at `PAGE_HEADER_SIZE + slot * SLOT_SIZE`.
2. If `offset == INVALID_SLOT` → return `false`.
3. If `new_data.len() > old_len as usize` → return `false`.
4. `self.data[offset..offset + new_data.len()].copy_from_slice(new_data)`.
5. Write `new_data.len() as u16` into `self.data[slot_pos + 2..slot_pos + 4]`.
6. Return `true`.

FSM is not updated (freed tail bytes are dead space, not reclaimed until future compaction).
`active_count` is not changed (row remains active).

---

### 2. `PagedTable::update_batch` and `delete_batch`

```rust
pub enum BatchUpdateOutcome {
    InPlace(Ctid),    // same ctid, overwritten in slot
    Relocated(Ctid),  // old deleted, new inserted, new ctid
}

pub fn update_batch(
    &mut self,
    mutations: &[(Ctid, &[Value])],  // (old_ctid, new_values)
) -> Result<Vec<BatchUpdateOutcome>>

pub fn delete_batch(&mut self, ctids: &[Ctid]) -> Result<usize>
```

**`update_batch` algorithm:**

1. Serialize all `new_values` upfront (no page I/O).
2. Group mutations by `page_id`.
3. For each group: read page once.
   - For each mutation: call `page.update_row_inplace(slot, new_bytes)`.
     - `true` → record `InPlace(ctid)`.
     - `false` (oversized or TOAST) → call `page.delete_row(slot)`, record ctid as needing relocation; free TOAST if present.
   - Write page once; update FSM once.
4. Batch-insert all relocated rows via existing `insert_batch`, record `Relocated(new_ctid)`.
5. `active_count` net change is zero for updates (each relocated row: `delete_row` decrements, `insert_batch` increments; in-place rows are unchanged). No explicit adjustment needed.

**`delete_batch` algorithm:**

1. Sort ctids by `page_id` (sequential access pattern).
2. Group by `page_id`. For each group: read page once.
   - For each ctid: free TOAST if present; call `page.delete_row(slot)`.
   - Write page once; update FSM once.
3. Decrement `active_count` by successful delete count.

**I/O comparison (10k rows, ~150 pages):**

| Operation | Current page reads | Current page writes | Batch page reads | Batch page writes |
|---|---|---|---|---|
| UPDATE 10k rows | ~10k | ~20k | ~150 | ~150 |
| DELETE 10k rows | ~10k | ~10k | ~150 | ~150 |

---

### 3. `IndexManager::delete_rows_batch`

```rust
pub fn delete_rows_batch(
    &mut self,
    deletions: &[(u64, &[Value])],  // (row_id, old_values)
    column_mapping: &HashMap<String, usize>,
) -> Result<()>
```

Mirrors `insert_rows_batch`: per indexed column, collect `(value, row_id)` pairs, sort by value, delete in sorted order for B-tree traversal locality.

---

### 4. `TableEngine::update_batch` and `delete_batch`

```rust
pub fn update_batch(
    &mut self,
    updates: &[(u64, Vec<Value>, Vec<Value>)],  // (row_id, old_values, new_values)
) -> Result<usize>

pub fn delete_batch(
    &mut self,
    deletions: &[(u64, Vec<Value>)],  // (row_id, old_values)
) -> Result<usize>
```

**`update_batch` internals:**

1. Convert row_ids to ctids; call `PagedTable::update_batch`.
2. Collect outcomes; resolve new row_ids for relocated rows.
3. Smart index skip: for each indexed column at `col_idx`:
   - If `old_values[col_idx] == new_values[col_idx]` AND row stayed in-place → skip entirely.
   - If value changed → add `(old_val, old_row_id)` to delete list; add `(new_val, new_row_id)` to insert list.
   - If row relocated → always delete old + insert new regardless of value equality.
4. `index_manager.delete_rows_batch(delete_list)`.
5. `index_manager.insert_rows_batch(insert_list)`.

**`delete_batch` internals:**

1. Convert row_ids to ctids; call `PagedTable::delete_batch`.
2. For all indexed cols, collect `(old_val, row_id)` pairs → `delete_rows_batch`.

**B-tree op comparison (10k updates, 3 indexes, W5 updates non-indexed `content` column):**

| Path | B-tree ops |
|---|---|
| Current per-row (no skip) | 60k |
| Batch + smart skip (non-indexed col changed) | 0 |
| Batch + smart skip (indexed col changed) | 2 × N × affected_indexes |

---

### 5. `lib.rs` changes

Replace per-row loops in `update()` and `delete()` with batch calls.

**`update()` (lines ~629-651):**
```rust
// Build batch input from already-collected rows
let updates: Vec<(u64, Vec<Value>, Vec<Value>)> = rows.into_iter().map(|row| {
    let old = row.values.clone();
    let mut new_vals = row.values;
    for (col_name, new_val) in &updates_assignments {
        if let Some(idx) = resolve_col_idx(&column_mapping, col_name) {
            if idx < new_vals.len() { new_vals[idx] = new_val.clone(); }
        }
    }
    (row.row_id, old, new_vals)
}).collect();
let count = table_engine.update_batch(&updates)?;
```

**`delete()` (lines ~669-671):**
```rust
let deletions: Vec<(u64, Vec<Value>)> = rows.into_iter()
    .map(|r| (r.row_id, r.values))
    .collect();
let count = table_engine.delete_batch(&deletions)?;
```

The existing `_update_row_with_old` and `_delete_with_old_values` methods on `TableEngine` are retained for any callers outside the main `lib.rs` update/delete paths.

---

## Testing

### Unit tests (per layer)

- **`Page::update_row_inplace`**: overwrite fits, overwrite too large, freed-slot guard, slot length updated correctly, active_count unchanged.
- **`PagedTable::update_batch`**: all-in-place (single page), all-relocate, mixed, multi-page boundary, active_count invariant, TOAST rows fall through to relocate.
- **`PagedTable::delete_batch`**: sequential pages, sparse ctids across many pages, TOAST rows freed, active_count decremented correctly.
- **`IndexManager::delete_rows_batch`**: single index, multi-index, sorted deletion verified, no crash on empty input.
- **`TableEngine::update_batch`**: non-indexed column update (0 index ops), indexed column update (index entries updated), relocation triggers index update.
- **`TableEngine::delete_batch`**: rows removed from index, non-existent rows ignored.

### Integration / benchmark gate

- All existing tests in `vs_sqlite_read` and `vs_sqlite_write` must continue to pass.
- W5/W7/W9 ratios must improve over SP3 baseline (`perf/baseline-write.json`).
- Target: W5 ratio < 10x (from 139x), W7 ratio < 5x (from 94x), W9 ratio < 10x (from 98x). These are aspirational; exact targets confirmed after first run.

---

## Risks

| Risk | Mitigation |
|---|---|
| In-place dead space causes page fill-up over time | Acceptable for now; compaction is future work. W5 same-size updates produce zero dead space. |
| Batch relocation of oversized rows interleaves page writes | Relocated inserts use existing `insert_batch` path; no new concurrency concern (single-writer). |
| Smart index skip incorrectly skips when it shouldn't | Covered by unit test: update indexed col, verify old entry removed and new entry present. |
| `delete_rows_batch` sort changes deletion order vs B-tree expectations | B-tree delete is value+row_id keyed; order-invariant for correctness. Sort is optimization only. |

---

## File Checklist

| File | Change |
|---|---|
| `src/storage/page.rs` | Add `Page::update_row_inplace` |
| `src/storage/paged_table.rs` | Add `BatchUpdateOutcome`, `update_batch`, `delete_batch` |
| `src/index/manager.rs` | Add `delete_rows_batch` |
| `src/storage/table_engine.rs` | Add `update_batch`, `delete_batch` |
| `src/lib.rs` | Replace per-row loops in `update()` / `delete()` |
| `tests/perf/vs_sqlite_write.rs` | Rerun, rebaseline W5/W7/W8/W9 |
| `CHANGES.md` | SP3b entry |
