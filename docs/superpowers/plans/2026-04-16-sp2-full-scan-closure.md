# SP2: Full-scan closure & COLD fairness fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the FAST/WARM Full-table-scan Loss (2.04x) by adding a streaming `for_each_row` API, and fix harness fairness by fadvising SQLite's WAL/SHM companions.

**Architecture:** New callback-driven scan method on `DirectDataAccess` that reuses a single `Vec<Value>` buffer across rows — zero heap allocations per row. Full-scan hot path streams directly from `PagedTable`; filter/index paths wrap existing `scan_with_projection` as a fallback (API completeness without requiring a full streaming filter machinery). Harness extension adds WAL/SHM file fadvising in `reopen_handles`.

**Tech Stack:** Rust 2021. No new dependencies. Existing `memmap2`, `rusqlite`, `libc` (unix) already present.

**Spec:** `docs/superpowers/specs/2026-04-16-sp2-full-scan-closure-design.md`

---

## File Structure

```
thunderdb/
├── perf/
│   └── baseline.json                   # MODIFIED (Task 7) — refreshed after SP2 lands
├── src/
│   ├── lib.rs                          # MODIFIED (Tasks 3, 4) — Database::for_each_row impl
│   ├── query/
│   │   └── direct.rs                   # MODIFIED (Task 3) — trait method + tests
│   └── storage/
│       ├── paged_table.rs              # MODIFIED (Task 1) — for_each_row_projected + tests
│       └── table_engine.rs             # MODIFIED (Task 2) — pass-through + tests
├── tests/
│   └── perf/
│       ├── common/
│       │   └── fixtures.rs             # MODIFIED (Task 6) — reopen_handles WAL/SHM
│       ├── harness_selftest.rs         # MODIFIED (Task 6) — new fadvise self-test
│       └── vs_sqlite_read.rs           # MODIFIED (Tasks 4, 5) — scenario 10 + hard assertion
└── CHANGES.md                          # MODIFIED (Task 7) — SP2 entry
```

No new files. Every change extends an existing module.

---

## Task 1: `PagedTable::for_each_row_projected`

**Files:**
- Modify: `src/storage/paged_table.rs`

- [ ] **Step 1: Write failing test**

Add to the `#[cfg(test)] mod tests` block in `src/storage/paged_table.rs`. First find where existing tests live (near the bottom of the file) and append:

```rust
    #[test]
    fn for_each_row_projected_visits_all_rows() {
        let dir = std::env::temp_dir().join("thunderdb_for_each_test_basic");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pages.bin");
        let mut pt = PagedTable::open(&path).unwrap();

        // Insert 5 rows with Int32 id + Varchar name
        for i in 0..5i32 {
            pt.insert_row(&[Value::Int32(i), Value::varchar(format!("user_{}", i))]).unwrap();
        }

        // Project column 0 only (id)
        let mut seen: Vec<i32> = Vec::new();
        let count = pt.for_each_row_projected(&[0], |vals| {
            assert_eq!(vals.len(), 1);
            if let Value::Int32(n) = vals[0] { seen.push(n); }
        }).unwrap();

        assert_eq!(count, 5);
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2, 3, 4]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_row_projected_with_toasted_rows() {
        let dir = std::env::temp_dir().join("thunderdb_for_each_test_toast");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pages.bin");
        let mut pt = PagedTable::open(&path).unwrap();

        // Insert one small and one large (TOAST-triggering) row
        let large = "x".repeat(3000); // > TOAST_THRESHOLD (2000)
        pt.insert_row(&[Value::Int32(1), Value::varchar("small".into())]).unwrap();
        pt.insert_row(&[Value::Int32(2), Value::varchar(large.clone())]).unwrap();

        let mut seen: Vec<(i32, String)> = Vec::new();
        pt.for_each_row_projected(&[0, 1], |vals| {
            let id = if let Value::Int32(n) = vals[0] { n } else { panic!() };
            let s = if let Value::Varchar(s) = &vals[1] { s.as_str().to_string() } else { panic!() };
            seen.push((id, s));
        }).unwrap();

        seen.sort_by_key(|(id, _)| *id);
        assert_eq!(seen[0], (1, "small".to_string()));
        assert_eq!(seen[1], (2, large));

        let _ = std::fs::remove_dir_all(&dir);
    }
```

- [ ] **Step 2: Run tests — expect compile failure (method doesn't exist)**

```bash
cargo test --lib paged_table::tests::for_each_row 2>&1 | tail -15
```

Expected: compile error — `no method named 'for_each_row_projected' found`.

- [ ] **Step 3: Implement `for_each_row_projected`**

Add to `src/storage/paged_table.rs`, just after the existing `scan_all_projected` method (around line 258):

```rust
    /// Stream all active rows through a callback with projected columns.
    ///
    /// Uses a reused buffer — zero heap allocations per row when projected
    /// values are inline-sized. The callback's slice is valid only during
    /// the invocation.
    ///
    /// Returns the number of rows passed to the callback.
    pub fn for_each_row_projected<F: FnMut(&[Value])>(
        &mut self,
        columns: &[usize],
        mut callback: F,
    ) -> Result<usize> {
        let mmap_ptr = self.page_file.ensure_mmap_and_ptr()?;
        let page_count = self.page_file.page_count();
        let mut buf: Vec<Value> = Vec::with_capacity(columns.len());
        let mut count = 0usize;

        for page_id in 1..page_count {
            let pd = _mmap_page(mmap_ptr, page_id);
            if pd[4] != PageType::Data as u8 { continue; }

            let slot_count = u16::from_le_bytes(pd[6..8].try_into().unwrap());
            for slot in 0..slot_count {
                let raw = match _slot_bytes(pd, slot) {
                    Some(b) => b,
                    None => continue,
                };

                buf.clear();
                if _has_toast(raw) {
                    let raw_owned = raw.to_vec();
                    let detoasted = toast::detoast_row_bytes(&raw_owned, &mut self.page_file)?;
                    for &col in columns {
                        buf.push(value_at_page_bytes(&detoasted, col)?);
                    }
                } else {
                    for &col in columns {
                        buf.push(value_at_page_bytes(raw, col)?);
                    }
                }
                callback(&buf);
                count += 1;
            }
        }
        Ok(count)
    }
```

- [ ] **Step 4: Run tests — expect pass**

```bash
cargo test --lib paged_table::tests::for_each_row 2>&1 | tail -15
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/storage/paged_table.rs
git commit -m "Add PagedTable::for_each_row_projected streaming scan (SP2 Task 1)

Zero-allocation per-row callback API. Reuses a single Vec<Value>
buffer across all slots. Tests cover basic projection and TOAST rows."
```

---

## Task 2: `TableEngine::for_each_row_projected`

**Files:**
- Modify: `src/storage/table_engine.rs`

- [ ] **Step 1: Write failing test**

Append to the existing `#[cfg(test)] mod tests` block in `src/storage/table_engine.rs`:

```rust
    #[test]
    fn for_each_row_projected_delegates_to_paged_table() {
        let mut table = create_test_table("for_each_delegate");
        table.insert_row(vec![Value::Int32(1), Value::varchar("a".into()), Value::Int32(10)]).unwrap();
        table.insert_row(vec![Value::Int32(2), Value::varchar("b".into()), Value::Int32(20)]).unwrap();

        let mut ids: Vec<i32> = Vec::new();
        let count = table.for_each_row_projected(&[0], |vals| {
            if let Value::Int32(n) = vals[0] { ids.push(n); }
        }).unwrap();

        assert_eq!(count, 2);
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }
```

(The helper `create_test_table` already exists in `table_engine.rs`'s test module — it builds a test table with disk storage in a temp dir.)

- [ ] **Step 2: Run test — expect compile failure**

```bash
cargo test --lib table_engine::tests::for_each_row_projected_delegates 2>&1 | tail -15
```

Expected: compile error — `no method named 'for_each_row_projected'`.

- [ ] **Step 3: Implement pass-through**

Add to `src/storage/table_engine.rs`, just after the existing `scan_all_projected` method (around line 374):

```rust
    /// Stream active rows through a callback with projected columns.
    /// Thin pass-through to `PagedTable::for_each_row_projected`.
    pub fn for_each_row_projected<F: FnMut(&[Value])>(
        &mut self,
        columns: &[usize],
        callback: F,
    ) -> Result<usize> {
        self.paged_table.for_each_row_projected(columns, callback)
    }
```

- [ ] **Step 4: Run test — expect pass**

```bash
cargo test --lib table_engine::tests::for_each_row_projected_delegates 2>&1 | tail -10
```

Expected: 1 test passes.

- [ ] **Step 5: Commit**

```bash
git add src/storage/table_engine.rs
git commit -m "Add TableEngine::for_each_row_projected pass-through (SP2 Task 2)"
```

---

## Task 3: `DirectDataAccess::for_each_row` trait method

**Files:**
- Modify: `src/query/direct.rs`

- [ ] **Step 1: Add trait method**

In `src/query/direct.rs`, add to the `DirectDataAccess` trait definition. Find the existing methods (after `count`, around line 121) and append before the closing `}` of the trait:

```rust
    /// Stream rows through a callback. Zero per-row heap allocations
    /// when projected values are inline-sized — the callback's `&[Value]`
    /// slice lives in a reused buffer.
    ///
    /// The slice is valid only during the callback invocation; clone
    /// values you need to retain.
    ///
    /// # Arguments
    /// * `table`      - Table name
    /// * `filters`    - WHERE conditions (AND-combined); `vec![]` for full scan
    /// * `projection` - Column indices to materialize; `None` = all columns
    /// * `callback`   - Invoked once per matching row
    ///
    /// # Returns
    /// Number of rows processed.
    fn for_each_row<F: FnMut(&[Value])>(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        projection: Option<Vec<usize>>,
        callback: F,
    ) -> Result<usize>;
```

- [ ] **Step 2: Verify trait compiles**

```bash
cargo check --lib 2>&1 | tail -10
```

Expected: compile error — `not all trait items implemented ... for_each_row` in `impl DirectDataAccess for Database` block in `src/lib.rs`. That's the expected next-step gap.

- [ ] **Step 3: Commit (trait-only)**

```bash
git add src/query/direct.rs
git commit -m "Add for_each_row to DirectDataAccess trait (SP2 Task 3)"
```

---

## Task 4: `Database::for_each_row` impl (no-filter hot path + filter fallback)

**Files:**
- Modify: `src/lib.rs`

- [ ] **Step 1: Write failing integration-style test**

Add to the existing `#[cfg(test)] mod tests` block in `src/lib.rs` (or the end of the file if the tests are inline). Find the existing tests section (look for `#[cfg(test)]` at the bottom of the file) and append:

```rust
    #[test]
    fn for_each_row_streams_no_filter() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};

        let dir = std::env::temp_dir().join("thunderdb_db_for_each_nofilter");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();

        db.insert_batch("t", vec![
            vec![Value::Int32(1), Value::varchar("alice".into())],
            vec![Value::Int32(2), Value::varchar("bob".into())],
            vec![Value::Int32(3), Value::varchar("charlie".into())],
        ]).unwrap();
        {
            let tbl = db.get_table_mut("t").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ]}).unwrap();
        }

        let mut ids: Vec<i32> = Vec::new();
        let count = db.for_each_row("t", vec![], Some(vec![0]), |vals| {
            if let Value::Int32(n) = vals[0] { ids.push(n); }
        }).unwrap();

        assert_eq!(count, 3);
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_row_with_filter_uses_fallback() {
        use crate::storage::table_engine::{ColumnInfo, TableSchema};
        use crate::query::{Filter, Operator};

        let dir = std::env::temp_dir().join("thunderdb_db_for_each_filter");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();

        db.insert_batch("t", vec![
            vec![Value::Int32(1), Value::varchar("a".into())],
            vec![Value::Int32(2), Value::varchar("b".into())],
            vec![Value::Int32(3), Value::varchar("c".into())],
        ]).unwrap();
        {
            let tbl = db.get_table_mut("t").unwrap();
            tbl.set_schema(TableSchema { columns: vec![
                ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ]}).unwrap();
        }

        let mut matched: Vec<i32> = Vec::new();
        let count = db.for_each_row(
            "t",
            vec![Filter::new("id", Operator::GreaterThan(Value::Int32(1)))],
            Some(vec![0]),
            |vals| {
                if let Value::Int32(n) = vals[0] { matched.push(n); }
            },
        ).unwrap();

        assert_eq!(count, 2);
        matched.sort();
        assert_eq!(matched, vec![2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_row_projection_none_requires_schema() {
        let dir = std::env::temp_dir().join("thunderdb_db_for_each_noschema");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Database::open(&dir).unwrap();
        db.insert_batch("t", vec![vec![Value::Int32(1)]]).unwrap();

        let r = db.for_each_row("t", vec![], None, |_| {});
        assert!(matches!(r, Err(Error::InvalidOperation(_))));

        let _ = std::fs::remove_dir_all(&dir);
    }
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cargo test --lib for_each_row 2>&1 | tail -15
```

Expected: compile error — `for_each_row` not implemented for `Database`.

- [ ] **Step 3: Implement `Database::for_each_row`**

In `src/lib.rs`, find the `impl DirectDataAccess for Database` block (search for that exact string). Add the new method implementation at the end of that impl block, just before the closing `}`:

```rust
    fn for_each_row<F: FnMut(&[Value])>(
        &mut self,
        table: &str,
        filters: Vec<Filter>,
        projection: Option<Vec<usize>>,
        mut callback: F,
    ) -> Result<usize> {
        let table_engine = self.get_table_mut(table)?;

        // Resolve projection (explicit or all-columns from schema)
        let cols: Vec<usize> = match projection {
            Some(c) => c,
            None => {
                let schema = table_engine.schema().ok_or_else(|| {
                    Error::InvalidOperation(
                        "for_each_row with projection=None requires table schema to be set".into()
                    )
                })?;
                (0..schema.columns.len()).collect()
            }
        };

        // Hot path: no filters → direct streaming scan
        if filters.is_empty() {
            return table_engine.for_each_row_projected(&cols, callback);
        }

        // Filter path: wrap scan_with_projection as fallback. Correctness first;
        // streaming filter optimization is deferred (§3.5 Plan B in the spec).
        let rows = self.scan_with_projection(table, filters, None, None, Some(cols))?;
        let n = rows.len();
        for row in rows {
            callback(&row.values);
        }
        Ok(n)
    }
```

Note: the filter-path fallback calls `self.scan_with_projection` which internally re-resolves the table + cols. That's fine — filter-path performance is not the Phase 1 target.

- [ ] **Step 4: Run tests — expect pass**

```bash
cargo test --lib for_each_row 2>&1 | tail -15
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs
git commit -m "Add Database::for_each_row streaming impl (SP2 Task 4)

No-filter hot path streams directly via for_each_row_projected with
zero per-row allocations. Filter path wraps scan_with_projection as
a correctness-preserving fallback."
```

---

## Task 5: Migrate scenario 10 + restore hard assertion

**Files:**
- Modify: `tests/perf/vs_sqlite_read.rs`

- [ ] **Step 1: Update scenario 10**

In `tests/perf/vs_sqlite_read.rs`, find the block for scenario 10:

```rust
        // 10. Full scan 10k posts
        Scenario::new("10. Full table scan (10k posts)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_projection("blog_posts", vec![], None, None, Some(vec![0])).unwrap();
            })
```

Replace ONLY the `.thunder(...)` closure body with the streaming call:

```rust
        // 10. Full scan 10k posts
        Scenario::new("10. Full table scan (10k posts)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().for_each_row("blog_posts", vec![], Some(vec![0]), |_| {}).unwrap();
            })
```

Leave the `.sqlite(...)` and `.assert(...)` blocks unchanged.

- [ ] **Step 2: Restore hard assertion**

At the bottom of the same file, find the `#[test] fn vs_sqlite_read` function. Replace this block:

```rust
    // Parent-goal assertion — tightened as SP2 closes gaps.
    // SP1 acceptance: no Failures. Known Losses (full scan, IN) are tracked and
    // become Wins/Ties in SP2.
    assert!(report.summary.failure == 0, "read scenarios have {} failure(s)", report.summary.failure);
    eprintln!("SP1 acceptance: known Losses remaining = {}; SP2 closes them.", report.summary.loss);
```

With:

```rust
    // Parent-goal assertion (SP2 and beyond).
    assert!(report.summary.failure == 0, "read scenarios have {} failure(s)", report.summary.failure);
    assert!(report.summary.loss == 0, "read scenarios have {} loss(es)", report.summary.loss);
```

- [ ] **Step 3: Run the bench**

```bash
cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read 2>&1 | tail -25
```

Expected: the scoreboard shows scenario 10 with ratio < 1.05 (Win or Tie). The test passes (both asserts hold).

If scenario 10 still shows Loss (ratio > 1.05), STOP and report back — Plan B from §3.5 of the spec needs activation (inline fast-path `value_at_page_bytes` for Int32, or pass raw bytes to callback).

- [ ] **Step 4: Commit**

```bash
git add tests/perf/vs_sqlite_read.rs
git commit -m "Migrate scenario 10 to streaming API; restore loss==0 assertion (SP2 Task 5)"
```

---

## Task 6: Harness fairness fix — fadvise WAL/SHM companions

**Files:**
- Modify: `tests/perf/common/fixtures.rs`
- Modify: `tests/perf/harness_selftest.rs`

- [ ] **Step 1: Write failing self-test**

Append to `tests/perf/harness_selftest.rs`, at the end of the file (after the existing tests):

```rust
#[test]
#[cfg(unix)]
fn cold_fadvises_sqlite_wal_companions() {
    // FAST mode uses WAL → sqlite.db-wal companion file is created during
    // the inserts. Verify reopen_handles reaches fadvise on it without error.
    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);

    let wal_path = {
        let mut s = f.sqlite_path.clone().into_os_string();
        s.push("-wal");
        std::path::PathBuf::from(s)
    };
    assert!(wal_path.exists(),
        "FAST mode should have created the WAL file {}", wal_path.display());

    // Call the reopen path — must not error.
    common::fixtures::reopen_handles(&mut f).expect("reopen should succeed");

    drop_fixtures(f);
}
```

- [ ] **Step 2: Run — expect pass (spec path), or fail (new behavior needed)**

```bash
cargo test --test harness_selftest cold_fadvises_sqlite_wal 2>&1 | tail -15
```

Two possible outcomes:
- Passes already: the existing `reopen_handles` tolerates missing fadvise targets (since fadvise of the main `.db` works). The test only verifies no error. Still valid — but the actual fadvise of WAL isn't happening. Step 3 adds it.
- Fails because `reopen_handles` is `pub(crate)` (not reachable from `tests/perf/harness_selftest.rs` directly): the test file accesses it via the `common::fixtures::reopen_handles` path in Step 1. Since the test binary is a separate crate compilation that includes `common/` as a module via `mod common;`, `pub(crate)` resolves to "visible within this test crate" — so the call should work. If compile fails, change `pub(crate)` to `pub` on `reopen_handles`.

- [ ] **Step 3: Extend `reopen_handles` to fadvise WAL/SHM**

In `tests/perf/common/fixtures.rs`, find the existing `reopen_handles` function. It currently looks like:

```rust
pub(crate) fn reopen_handles(f: &mut Fixtures) -> std::io::Result<()> {
    let (_t, _s) = f.take_handles();
    drop(_t);
    drop(_s);
    for p in crate::common::cache::collect_data_files(&f.thunder_dir) {
        let _ = crate::common::cache::posix_fadvise_dontneed(&p);
    }
    let _ = crate::common::cache::posix_fadvise_dontneed(&f.sqlite_path);
    let t = Database::open(&f.thunder_dir).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    let s = Connection::open(&f.sqlite_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    f.set_handles(t, s);
    Ok(())
}
```

Replace with:

```rust
pub(crate) fn reopen_handles(f: &mut Fixtures) -> std::io::Result<()> {
    let (_t, _s) = f.take_handles();
    drop(_t);
    drop(_s);

    // Thunder: all *.bin data files.
    for p in crate::common::cache::collect_data_files(&f.thunder_dir) {
        let _ = crate::common::cache::posix_fadvise_dontneed(&p);
    }

    // SQLite: main db + WAL/SHM companions (fair COLD measurement).
    // Missing files (non-WAL journal mode, DELETE mode post-commit) skip silently.
    let _ = crate::common::cache::posix_fadvise_dontneed(&f.sqlite_path);
    for suffix in &["-wal", "-shm"] {
        let companion = {
            let mut s = f.sqlite_path.clone().into_os_string();
            s.push(suffix);
            std::path::PathBuf::from(s)
        };
        if companion.exists() {
            let _ = crate::common::cache::posix_fadvise_dontneed(&companion);
        }
    }

    let t = Database::open(&f.thunder_dir).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    let s = Connection::open(&f.sqlite_path).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    f.set_handles(t, s);
    Ok(())
}
```

- [ ] **Step 4: Run the self-test — confirm it still passes**

```bash
cargo test --test harness_selftest cold_fadvises_sqlite_wal 2>&1 | tail -10
```

Expected: 1 test passes.

Also run the full harness_selftest suite to confirm no regression:

```bash
cargo test --test harness_selftest 2>&1 | tail -5
```

Expected: all tests pass (55+).

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/fixtures.rs tests/perf/harness_selftest.rs
git commit -m "Fadvise SQLite WAL/SHM companions in COLD reopen (SP2 Task 6)

COLD measurements previously left SQLite's -wal and -shm files hot
in the OS page cache, skewing comparisons. Now fadvises them when
they exist; missing companions skip silently."
```

---

## Task 7: Rebaseline + CHANGES.md + final verification

**Files:**
- Modify: `perf/baseline.json`
- Modify: `CHANGES.md`

- [ ] **Step 1: Re-run full vs_sqlite_read to confirm state**

```bash
cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read 2>&1 | tail -25
```

Expected: `OVERALL: 10 Win or better, 0 Loss, 0 Failure → PASS`.

- [ ] **Step 2: Promote new baseline**

```bash
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read 2>&1 | tail -10
```

Expected: stderr includes `Baseline promoted: perf/baseline.json`.

- [ ] **Step 3: Confirm vs Base column reads as small delta**

```bash
cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read 2>&1 | grep -E "Full table scan|Summary|OVERALL"
```

Expected: scenario 10's `vs Base` column shows a small percentage (±5%, timing jitter).

- [ ] **Step 4: Add CHANGES.md entry**

Open `CHANGES.md` and prepend immediately below the top `# ThunderDB Changes` header:

```markdown
## 2026-04-16 - SP2: Full-scan closure & COLD fairness fix

Second sub-project in the "faster than SQLite in all benchmarks" program.

- **Streaming scan API**: new `DirectDataAccess::for_each_row` callback-driven method on `Database`. Zero per-row heap allocations on the hot no-filter path — `PagedTable::for_each_row_projected` reuses a single `Vec<Value>` buffer across all slots. Filter path wraps `scan_with_projection` for API completeness.
- **Full-scan benchmark migrated**: scenario 10 (Full table scan 10k posts) now uses `for_each_row`. Closes the last FAST/WARM Loss remaining from SP1.
- **Scoreboard at SMALL/FAST/WARM**: 10 Win or Tie, 0 Loss, 0 Failure. Hard `report.summary.loss == 0` assertion restored in `vs_sqlite_read`.
- **COLD fairness fix**: `reopen_handles` now fadvises SQLite's `-wal` and `-shm` companion files (when they exist) alongside the main `.db`. Previously SQLite retained its WAL in the OS cache across "COLD" samples, giving it an unfair advantage. COLD measurements are now apples-to-apples.
- **New baseline committed** to `perf/baseline.json`.

Spec: `docs/superpowers/specs/2026-04-16-sp2-full-scan-closure-design.md`
Plan: `docs/superpowers/plans/2026-04-16-sp2-full-scan-closure.md`
```

- [ ] **Step 5: Run full test suite to confirm no regressions**

```bash
cargo test --release 2>&1 | grep -E "^test result" | head -20
```

Expected: all test binaries pass (every line starts `test result: ok`).

- [ ] **Step 6: Commit**

```bash
git add perf/baseline.json CHANGES.md
git commit -m "Rebaseline + CHANGES for SP2 (SP2 Task 7)

SP2 complete. Scoreboard at SMALL/FAST/WARM is 10 Win-or-Tie /
0 Loss / 0 Failure."
```

---

## Acceptance

All spec §7 deliverables met when plan is complete:

- [ ] `DirectDataAccess::for_each_row` method defined (Task 3)
- [ ] `PagedTable::for_each_row_projected<F>` implemented (Task 1)
- [ ] `TableEngine::for_each_row_projected<F>` delegates (Task 2)
- [ ] `Database::for_each_row<F>` full impl (Task 4)
- [ ] Unit tests for Phase 1 paths (Tasks 1, 2, 4)
- [ ] Scenario 10 migrated to new API (Task 5)
- [ ] `vs_sqlite_read` hard assertion restored to `loss == 0` (Task 5)
- [ ] `reopen_handles` fadvises WAL/SHM companions (Task 6)
- [ ] Phase 2 self-test added (Task 6)
- [ ] `perf/baseline.json` refreshed and committed (Task 7)
- [ ] `CHANGES.md` entry for SP2 (Task 7)

**Parent-goal progress after SP2:** SMALL/FAST/WARM cell fully closed (no Losses). COLD measurement now honest — future sub-project addresses any remaining COLD regressions based on this data.

**Next sub-project:** SP3 — Write-path benchmarks (single INSERT, UPDATE, DELETE, UPDATE-of-indexed).
