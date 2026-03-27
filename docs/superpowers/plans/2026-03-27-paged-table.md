# PagedTable + Integration Implementation Plan (Sub-project 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace DataFile + RAT with page-based storage by implementing PagedTable and rewiring TableEngine, completing the slotted page migration.

**Architecture:** `PagedTable` wraps `PageFile` and provides CRUD + scan operations using slotted pages. TOAST is integrated for large rows. `TableEngine` replaces its `data_file` + `rat` fields with a single `paged_table` field. B-tree indexes continue to store u64 row_ids which are now packed ctids. All existing tests must continue to pass.

**Tech Stack:** Rust, existing Page/PageFile/TOAST modules from sub-projects 1-3.

---

### Task 1: Create paged_table.rs with struct, open, insert_row, get_row

**Files:**
- Create: `src/storage/paged_table.rs`
- Modify: `src/storage/mod.rs`

- [ ] **Step 1: Register the module**

In `src/storage/mod.rs`, add after `pub mod toast;`:

```rust
pub mod paged_table;
pub use paged_table::PagedTable;
```

- [ ] **Step 2: Create paged_table.rs with struct and open**

Create `src/storage/paged_table.rs`. The file should contain:

- `PagedTable` struct with `page_file: PageFile` and `active_count: u64`
- `open(path: &Path)` constructor that opens a PageFile and scans data pages to count active rows
- `insert_row(values: &[Value]) -> Result<Ctid>` that serializes, toasts if needed, finds a page with space, inserts, updates FSM
- `get_row(ctid: Ctid) -> Result<Option<Row>>` that reads the page, gets the slot, detoasts, and returns a Row with `row_id = ctid.to_u64()`
- `active_row_count() -> usize` returns cached count
- Unit tests for insert+get roundtrip and active_row_count

Key imports:
```rust
use crate::error::Result;
use crate::storage::page::{Page, PageType, Ctid, PAGE_SIZE, serialize_row_for_page};
use crate::storage::page_file::PageFile;
use crate::storage::toast::{toast_row, detoast_row_bytes, TOAST_THRESHOLD};
use crate::storage::value::Value;
use crate::storage::row::Row;
use std::path::Path;
```

For `get_row`, the detoasted row bytes use the page row format (u16 col_count, no row_id). Parse values by walking col_count + offsets + values with `Value::from_bytes`, then construct `Row::new(ctid.to_u64(), values)`.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib paged_table::tests 2>&1 | tail -10`

- [ ] **Step 4: Commit**

```bash
git add src/storage/mod.rs src/storage/paged_table.rs
git commit -m "Add PagedTable with open, insert_row, get_row"
```

---

### Task 2: Add delete_row, update_row, scan operations

**Files:**
- Modify: `src/storage/paged_table.rs`

- [ ] **Step 1: Add delete_row**

```rust
pub fn delete_row(&mut self, ctid: Ctid) -> Result<bool>
```

Read page, get raw bytes from slot, call `free_toast_data`, call `page.delete_row`, write page, update FSM, decrement `active_count`. Return false if slot was already free.

- [ ] **Step 2: Add update_row**

```rust
pub fn update_row(&mut self, ctid: Ctid, values: &[Value]) -> Result<Ctid>
```

Delete old row, insert new values, return new ctid.

- [ ] **Step 3: Add scan_all**

```rust
pub fn scan_all(&mut self) -> Result<Vec<Row>>
```

Iterate pages 1..page_count. For each data page, iterate slots 0..slot_count. For active slots, detoast + deserialize. Row ID = `Ctid::new(page_id, slot_index).to_u64()`.

- [ ] **Step 4: Add scan_filtered and count_filtered**

```rust
pub fn scan_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool

pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
where F: Fn(&[u8]) -> bool
```

Same as scan_all but apply predicate on raw slot bytes before deserializing. count_filtered counts without collecting.

- [ ] **Step 5: Add tests**

Tests for: delete then get returns None, update returns new ctid with correct data, scan_all returns all active rows, scan_filtered skips non-matching, count_filtered accuracy.

- [ ] **Step 6: Run tests and commit**

Run: `cargo test --lib paged_table::tests 2>&1 | tail -10`

```bash
git add src/storage/paged_table.rs
git commit -m "Add delete_row, update_row, scan_all, scan_filtered, count_filtered"
```

---

### Task 3: Add batch ctid fetch

**Files:**
- Modify: `src/storage/paged_table.rs`

- [ ] **Step 1: Add get_rows_by_ctids**

```rust
pub fn get_rows_by_ctids(&mut self, ctids: &[Ctid]) -> Result<Vec<Row>>
```

Group ctids by page_id (HashMap<u32, Vec<u16>>). For each page, read it once and extract all requested slots. Detoast and deserialize each.

- [ ] **Step 2: Add get_rows_by_ctids_filtered**

```rust
pub fn get_rows_by_ctids_filtered<F>(&mut self, ctids: &[Ctid], predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool
```

Same but apply predicate before deserializing.

- [ ] **Step 3: Add tests**

Test: insert 20 rows, collect ctids, fetch 5 specific ctids, verify correct data. Test filtered variant.

- [ ] **Step 4: Run tests and commit**

Run: `cargo test --lib paged_table::tests 2>&1 | tail -10`

```bash
git add src/storage/paged_table.rs
git commit -m "Add get_rows_by_ctids and filtered variant for indexed fetch"
```

---

### Task 4: Rewire TableEngine struct and constructors

**Files:**
- Modify: `src/storage/table_engine.rs`

This is the critical integration task. The implementer must read the current TableEngine carefully before making changes.

- [ ] **Step 1: Replace struct fields**

Change the TableEngine struct from:

```rust
pub struct TableEngine {
    name: String,
    table_dir: PathBuf,
    data_file: DataFile,
    rat: RecordAddressTable,
    index_manager: IndexManager,
    schema: Option<TableSchema>,
    next_row_id: u64,
    config: StorageConfig,
    column_mapping_cache: Option<Arc<HashMap<String, usize>>>,
}
```

to:

```rust
pub struct TableEngine {
    name: String,
    table_dir: PathBuf,
    paged_table: PagedTable,
    index_manager: IndexManager,
    schema: Option<TableSchema>,
    config: StorageConfig,
    column_mapping_cache: Option<Arc<HashMap<String, usize>>>,
}
```

Remove `data_file`, `rat`, `next_row_id`. Add `paged_table`.

Update imports: add `use crate::storage::paged_table::PagedTable;` and `use crate::storage::page::Ctid;`. Remove `DataFile` and `RecordAddressTable` from imports.

- [ ] **Step 2: Rewrite `open()` constructor**

Replace the existing `open()` body. Instead of opening `data.bin` + `rat.bin`, open `pages.bin`:

```rust
let pages_path = table_dir.join("pages.bin");
let paged_table = PagedTable::open(&pages_path)?;
```

Load schema and indexes as before. Remove `next_row_id` computation (no longer needed).

- [ ] **Step 3: Rewrite `create()` constructor**

Similar to `open()` but creates the directory structure. Open PagedTable on new `pages.bin`.

- [ ] **Step 4: Rewrite `open_in_memory()` and `load_to_memory()`**

For `open_in_memory`, use a temp file path for PagedTable (PageFile doesn't have an in-memory mode yet). For `load_to_memory`, open PagedTable from the actual path.

- [ ] **Step 5: Verify it compiles**

Run: `cargo check 2>&1 | head -30`
Expected: many errors from methods still referencing `self.data_file` and `self.rat`. This is expected — Tasks 5 and 6 will fix them.

- [ ] **Step 6: Commit (WIP — will not compile fully yet)**

```bash
git add src/storage/table_engine.rs
git commit -m "WIP: Replace DataFile+RAT with PagedTable in TableEngine struct"
```

---

### Task 5: Rewire TableEngine CRUD methods

**Files:**
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Rewrite insert_row**

```rust
pub fn insert_row(&mut self, values: Vec<Value>) -> Result<u64> {
    let ctid = self.paged_table.insert_row(&values)?;
    let row_id = ctid.to_u64();

    if !self.index_manager.indexed_columns().is_empty() {
        let row = Row::new(row_id, values);
        let mapping = self.build_column_mapping();
        self.index_manager.insert_row(&row, &mapping)?;
    }

    Ok(row_id)
}
```

- [ ] **Step 2: Rewrite insert_batch**

```rust
pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) -> Result<Vec<u64>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut row_ids = Vec::with_capacity(rows.len());
    let mut row_objects = Vec::with_capacity(rows.len());

    for values in rows {
        let ctid = self.paged_table.insert_row(&values)?;
        let row_id = ctid.to_u64();
        row_objects.push(Row::new(row_id, values));
        row_ids.push(row_id);
    }

    if !self.index_manager.indexed_columns().is_empty() {
        let mapping = self.build_column_mapping();
        self.index_manager.insert_rows_batch(&row_objects, &mapping)?;
    }

    Ok(row_ids)
}
```

- [ ] **Step 3: Rewrite get_by_id**

```rust
pub fn get_by_id(&mut self, row_id: u64) -> Result<Option<Row>> {
    let ctid = Ctid::from_u64(row_id);
    self.paged_table.get_row(ctid)
}
```

- [ ] **Step 4: Rewrite update_row**

```rust
pub fn update_row(&mut self, row_id: u64, values: Vec<Value>) -> Result<bool> {
    let old_ctid = Ctid::from_u64(row_id);

    // Read old row for index deletion
    let old_values = if !self.index_manager.indexed_columns().is_empty() {
        self.paged_table.get_row(old_ctid)?.map(|r| r.values)
    } else {
        None
    };

    let new_ctid = self.paged_table.update_row(old_ctid, &values)?;
    let new_row_id = new_ctid.to_u64();

    // Update indices
    if !self.index_manager.indexed_columns().is_empty() {
        let mapping = self.build_column_mapping();
        if let Some(old_vals) = old_values {
            self.index_manager.delete_row(row_id, &old_vals, &mapping)?;
        }
        let new_row = Row::new(new_row_id, values);
        self.index_manager.insert_row(&new_row, &mapping)?;
    }

    Ok(true)
}
```

- [ ] **Step 5: Rewrite delete_by_id**

```rust
pub fn delete_by_id(&mut self, row_id: u64) -> Result<bool> {
    let ctid = Ctid::from_u64(row_id);

    let old_values = if !self.index_manager.indexed_columns().is_empty() {
        self.paged_table.get_row(ctid)?.map(|r| r.values)
    } else {
        None
    };

    let deleted = self.paged_table.delete_row(ctid)?;
    if !deleted {
        return Ok(false);
    }

    if let Some(values) = old_values {
        let mapping = self.build_column_mapping();
        self.index_manager.delete_row(row_id, &values, &mapping)?;
    }

    Ok(true)
}
```

- [ ] **Step 6: Commit (still may not compile — scan methods pending)**

```bash
git add src/storage/table_engine.rs
git commit -m "WIP: Rewire TableEngine CRUD methods to use PagedTable"
```

---

### Task 6: Rewire TableEngine scan, count, index, and utility methods

**Files:**
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Rewrite scan and count methods**

```rust
pub fn get_by_ids(&mut self, row_ids: &[u64]) -> Result<Vec<Row>> {
    let ctids: Vec<Ctid> = row_ids.iter().map(|&id| Ctid::from_u64(id)).collect();
    self.paged_table.get_rows_by_ctids(&ctids)
}

pub fn get_by_ids_filtered<F>(&mut self, row_ids: &[u64], predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool {
    let ctids: Vec<Ctid> = row_ids.iter().map(|&id| Ctid::from_u64(id)).collect();
    self.paged_table.get_rows_by_ctids_filtered(&ctids, predicate)
}

pub fn active_row_count(&self) -> usize {
    self.paged_table.active_row_count()
}

pub fn scan_all(&mut self) -> Result<Vec<Row>> {
    self.paged_table.scan_all()
}

pub fn scan_all_limited(&mut self, limit: Option<usize>) -> Result<Vec<Row>> {
    // PagedTable doesn't have a limited scan yet — scan all and truncate
    let mut rows = self.paged_table.scan_all()?;
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

pub fn scan_all_filtered<F>(&mut self, predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool {
    self.paged_table.scan_filtered(predicate)
}

pub fn count_filtered<F>(&mut self, predicate: F) -> Result<usize>
where F: Fn(&[u8]) -> bool {
    self.paged_table.count_filtered(predicate)
}
```

- [ ] **Step 2: Remove dead methods**

Remove `fetch_rows_sorted_by_offset()`, `active_row_ids()` (if unused or update to scan pages). Remove `rebuild_rat()`.

- [ ] **Step 3: Rewrite flush and compact**

```rust
pub fn flush(&mut self) -> Result<()> {
    // PageFile handles sync internally
    Ok(())
}

pub fn compact(&mut self) -> Result<()> {
    // Compact fragmented pages
    // For now, no-op — page compaction is an optimization
    Ok(())
}

pub fn full_compact(&mut self) -> Result<()> {
    // No-op for now
    Ok(())
}
```

- [ ] **Step 4: Rewrite save_to_disk**

Update `save_to_disk` to flush the PageFile and save indexes. Remove RAT save logic.

- [ ] **Step 5: Update index methods**

The `*_by_index` methods (`search_by_index`, `range_search_by_index`, etc.) currently call `self.get_by_ids()`. They should continue to work since `get_by_ids` now delegates to `paged_table.get_rows_by_ctids`. Verify they compile.

- [ ] **Step 6: Update create_index**

`create_index` currently calls `self.scan_all()` to backfill. This should still work since `scan_all` is rewired. Verify it compiles.

- [ ] **Step 7: Verify compilation**

Run: `cargo check 2>&1 | tail -20`
Expected: should compile (possibly with warnings about unused imports).

- [ ] **Step 8: Commit**

```bash
git add src/storage/table_engine.rs
git commit -m "Rewire TableEngine scan, count, index methods to PagedTable"
```

---

### Task 7: Fix tests, update CHANGES.md, run benchmark

**Files:**
- Modify: `src/storage/table_engine.rs` (test fixes)
- Modify: `CHANGES.md`

- [ ] **Step 1: Run full test suite and fix failures**

Run: `cargo test 2>&1 | tail -30`

Expected failures: tests that reference `data.bin`, `rat.bin`, or use `DataFile`/`RAT` APIs directly. Fix each:
- TableEngine tests in `table_engine.rs` that construct with old fields
- Integration tests that rely on old file structure
- The `table_existence` test that checks for `data.bin`

For each failure, update to use the new PagedTable-based API.

- [ ] **Step 2: Run blog benchmark test**

Run: `cargo test --test blog_benchmark_test -- --nocapture 2>&1 | tail -20`
Expected: should pass with paged storage.

- [ ] **Step 3: Run ThunderDB vs SQLite benchmark**

Run: `cargo test --test thunderdb_vs_sqlite_bench --release -- --nocapture 2>&1 | tail -20`
Report the full timing table.

- [ ] **Step 4: Update CHANGES.md**

Add at the top of `CHANGES.md`:

```markdown
## 2026-03-27 - Slotted page storage migration (sub-project 4: PagedTable + Integration)

- **PagedTable**: New CRUD layer over PageFile+Page+TOAST, replaces DataFile+RAT
- **TableEngine rewrite**: All storage operations now use 8KB slotted pages with ctid addressing
- **Batch ctid fetch**: `get_rows_by_ctids()` groups reads by page_id for minimal page I/O
- **Filtered ctid fetch**: `get_rows_by_ctids_filtered()` applies predicates on raw page bytes
- **Automatic TOAST**: Insert automatically toasts rows > 2000 bytes; get automatically detoasts
- Removed DataFile and RAT dependency from TableEngine
- New file: `src/storage/paged_table.rs`
```

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Complete slotted page migration: all tests pass, benchmark updated"
```
