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
