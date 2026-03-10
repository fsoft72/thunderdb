# CHANGES

## 2026-03-10: Fix correctness bugs (#1-#5 from IMPROVEMENTS.md)

### 1. ORDER BY and column projection now execute correctly (issue #1)
- `Repl::execute_select` now builds a `QueryPlan` and applies `apply_ordering()`, `apply_pagination()`, and `apply_projection()` to returned rows.
- When ORDER BY is present, LIMIT/OFFSET are deferred until after sorting so that pagination operates on the sorted result set.
- Files changed: `src/repl/mod.rs`

### 2. Creating an index on a populated table now backfills it (issue #2)
- Added `TableEngine::create_index()` which creates the index and rebuilds it from all existing rows.
- This prevents empty-index bugs where the planner trusts a newly created but unpopulated index.
- Files changed: `src/storage/table_engine.rs`

### 3. Float NaN values no longer panic in index operations (issue #3)
- Implemented `Eq`, `Ord`, and `PartialOrd` manually for `Value`, using `f32::total_cmp` / `f64::total_cmp` for float variants.
- Changed B-tree generic bound from `PartialOrd` to `Ord` throughout `btree.rs`, `node.rs`, `persist.rs`.
- Replaced all `.partial_cmp().unwrap()` calls with `.cmp()` in `btree.rs`, `node.rs`, `manager.rs`, `builder.rs`.
- Files changed: `src/storage/value.rs`, `src/index/btree.rs`, `src/index/node.rs`, `src/index/persist.rs`, `src/index/manager.rs`, `src/query/builder.rs`

### 4. SmallString internal representation is now private (issue #4)
- Converted `SmallString` from a public enum to a public struct wrapping a private `SmallStringRepr` enum.
- External code can no longer construct an `Inline` variant with invalid UTF-8 bytes, eliminating the UB risk from `from_utf8_unchecked`.
- Files changed: `src/storage/small_string.rs`

### 5. Index config (btree_order) is now wired through the runtime (issue #5)
- `TableEngine::open`, `open_in_memory`, `create`, and `load_to_memory` now accept a `btree_order` parameter.
- `Database` passes `config.index.btree_order` when creating table engines, replacing the hardcoded `100`.
- Files changed: `src/storage/table_engine.rs`, `src/lib.rs`

## 2026-03-10: Performance improvements (#6-#11 from IMPROVEMENTS.md)

### 6. O(1) startup: next_row_id from BTreeMap last key (issue #6)
- Added `RecordAddressTable::max_row_id()` using `BTreeMap::keys().next_back()`.
- `TableEngine::open` uses `rat.max_row_id()` instead of `rat.row_ids().into_iter().max()`.
- Files changed: `src/storage/rat.rs`, `src/storage/table_engine.rs`

### 7. O(1) active_count via cached counter (issue #7)
- Added `cached_active` field to `RecordAddressTable`, maintained incrementally on insert/delete/bulk_insert/compact/load.
- `active_count()` is now O(1) instead of O(n).
- Files changed: `src/storage/rat.rs`

### 8. Reduce per-row syscalls in full scans (issue #8)
- Wrapped `scan_rows()` and `scan_all()` File branches in `BufReader::with_capacity(256KB)`.
- Batches many small `read_exact()` calls into large sequential reads, reducing syscall overhead.
- Files changed: `src/storage/data_file.rs`

### 9. Reorder indexed row fetches by on-disk offset (issue #9)
- Added `fetch_rows_sorted_by_offset()` which resolves RAT entries, sorts by disk offset, and reads sequentially.
- Applied to `get_by_ids()`, `search_by_index()`, `greater/less_than_by_index()`, `prefix_search`, and `range_search`.
- Files changed: `src/storage/table_engine.rs`

### 10. Replace HashSet+format! cardinality counting (issue #10)
- `IndexStatistics::from_btree()` now extracts key references, sorts them, and counts adjacent changes.
- Eliminates `HashSet<String>` and `format!("{:?}", key)` heap allocation per entry.
- Also replaced `partial_cmp()` with direct comparison operators in `record_insert()`.
- Files changed: `src/index/stats.rs`

### 11. Count-only execution path (issue #11)
- `count()` without filters returns `rat.active_count()` in O(1).
- With filters, counts matches without collecting into `Vec<Row>`.
- Multi-index intersection with no remaining filters returns `row_ids.len()` directly.
- Files changed: `src/lib.rs`
