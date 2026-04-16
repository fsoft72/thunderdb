# SP2 — Full-scan closure & COLD fairness fix

**Date:** 2026-04-16
**Sub-project:** 2 of 7 in the "Thunder becomes faster than SQLite in all benchmarks" program.
**Status:** Design approved; ready for implementation plan.
**Spec for SP1 (prerequisite):** `docs/superpowers/specs/2026-04-16-benchmark-harness-design.md`

---

## 1. Context

### 1.1 Starting state

SP1 delivered the benchmark harness and migrated the 11 read-path scenarios onto it. Current scoreboard at `SMALL/FAST/WARM`:

| Result        | Count |
|---------------|------:|
| Win           | 9     |
| Tie           | 1     |
| Loss          | 1     |
| Failure       | 0     |

The single Loss is **scenario 10 — Full table scan (10k posts)**: Thunder 566µs vs SQLite 277µs, ratio **2.04x**.

Additionally, the full-matrix run (`THUNDERDB_DURABILITY=both THUNDERDB_CACHE=both`) exposed a **harness fairness bug**: `reopen_handles` calls `posix_fadvise(DONTNEED)` on `thunder/*.bin` and `sqlite.db`, but not on `sqlite.db-wal` and `sqlite.db-shm`. SQLite in WAL mode keeps its WAL cached across "COLD" samples while Thunder is fully evicted, skewing COLD comparisons toward SQLite.

### 1.2 Root-cause analysis of the Loss

The full-scan Thunder path:

```
Database::scan_with_projection("blog_posts", [], None, None, Some([0]))
  → TableEngine::scan_all_projected(&[0])
    → PagedTable::scan_all_projected(&[0])
      for each of N slots across pages:
        _parse_projected(bytes, &[0])            // Vec::with_capacity(1) + value_at_page_bytes
        Row::new(row_id, values)                 // another struct
        result.push(Row)                         // possibly realloc
```

For 10k rows, that's **10k allocations of small `Vec<Value>` buffers** plus 10k `Row` struct constructions, plus periodic `Vec<Row>` reallocs. The per-row gap vs SQLite (~29ns) matches this allocation profile.

SQLite's equivalent path — `stmt.query_map(|r| r.get::<_, i32>(0)).count()` — is an iterator that reads each row, invokes the closure (which returns an inline `i32`), and drains via `.count()`. **Zero per-row heap allocations.**

The API gap: Thunder only exposes materializing scans. To match SQLite's iteration cost, we need a streaming primitive.

### 1.3 COLD measurement artifact

`reopen_handles` at `tests/perf/common/fixtures.rs:238-253`:

```rust
for p in crate::common::cache::collect_data_files(&f.thunder_dir) {
    let _ = crate::common::cache::posix_fadvise_dontneed(&p);
}
let _ = crate::common::cache::posix_fadvise_dontneed(&f.sqlite_path);  // .db only
```

`collect_data_files` recurses `*.bin` → catches all Thunder data.
For SQLite, only `sqlite.db` is fadvised. `sqlite.db-wal` and `sqlite.db-shm` (WAL mode sibling files) stay hot in the OS page cache, giving SQLite a free warm cache in the "COLD" column.

Consequence: the 148x COLD regression on scenario 7, and similar outliers on scenarios 4–9, are partly artifacts of the fairness bug — not true Thunder weaknesses.

---

## 2. Scope

### 2.1 In scope

**Phase 1 — Streaming scan API.** A new callback-driven `for_each_row` method on `DirectDataAccess` that yields each row's projected values through a callback with a reused buffer. Target: close the FAST/WARM full-scan Loss.

**Phase 2 — Harness fairness fix.** Extend `reopen_handles` to fadvise SQLite's `-wal` and `-shm` companion files when they exist. Produce honest COLD numbers. Re-baseline.

### 2.2 Explicitly out of scope

- **COLD-specific Thunder optimizations** (lazy index loading, faster index deserialization, etc.) — deferred. Only addressed if post-fairness-fix measurements still show Thunder losing. In that case, a follow-up sub-project handles it.
- **MEDIUM/LARGE tier verification** — SP5's job.
- **Other read-path scenarios** — already Win or Tie at FAST/WARM.
- **Write-path optimizations** — SP3.
- **New query features** — SP4a/4b.

### 2.3 Success criteria

1. `cargo test --test vs_sqlite_read --release` passes at default (SMALL/FAST/WARM) with `report.summary.loss == 0`. The hard `loss == 0` assertion replaces the SP1-era relaxed `failure == 0` version.
2. Scenario 10 (Full table scan) verdict = **Win** or **Tie** (ratio ≤ 1.05).
3. `reopen_handles` fadvises `sqlite.db-wal` and `sqlite.db-shm` when they exist.
4. A new baseline (`perf/baseline.json`) captures the post-SP2 state.
5. Phase 2 harness self-test (`cold_fadvises_sqlite_wal`) passes on Linux, no-ops on other platforms.

### 2.4 Acceptance matrix

| Cell              | Phase 1                       | Phase 2                                                          |
|-------------------|-------------------------------|------------------------------------------------------------------|
| SMALL/FAST/WARM   | 10 Win-or-better, 0 Loss      | unchanged                                                         |
| SMALL/FAST/COLD   | unchanged from SP1            | re-measured with fair fadvise; documented (may still show losses) |
| DURABLE cells     | unchanged (Unsupported)       | unchanged                                                         |
| MEDIUM/LARGE      | spot-check only; not required | not required                                                      |

---

## 3. Phase 1 design: streaming scan API

### 3.1 New trait method

Add to `DirectDataAccess` in `src/query/direct.rs`:

```rust
/// Stream all rows through a callback. Zero per-row heap allocations
/// when projected values are inline-sized; the callback's `&[Value]`
/// slice lives in a buffer reused across rows.
///
/// The callback slice is only valid for the duration of the callback
/// invocation — clone values you need to retain.
///
/// # Arguments
/// * `table`      - Table name
/// * `filters`    - WHERE conditions (AND-combined); `vec![]` for full scan
/// * `projection` - Column indices to materialize; `None` = all columns
/// * `callback`   - Invoked once per matching row
///
/// # Returns
/// Number of rows invoked for.
fn for_each_row<F: FnMut(&[Value])>(
    &mut self,
    table: &str,
    filters: Vec<Filter>,
    projection: Option<Vec<usize>>,
    callback: F,
) -> Result<usize>;
```

### 3.2 Implementation layers

**`PagedTable::for_each_row_projected<F>`** (`src/storage/paged_table.rs`):

```rust
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
                let detoasted = toast::detoast_row_bytes(
                    &raw.to_vec(), &mut self.page_file
                )?;
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

**`TableEngine::for_each_row_projected<F>`** (`src/storage/table_engine.rs`):

Thin pass-through. Delegates to `PagedTable::for_each_row_projected`. No filter logic at this layer (filters handled in `Database` per 3.3).

**`Database::for_each_row<F>`** (`src/lib.rs`): Full-featured with index path + raw-bytes predicate. Structure mirrors existing `scan_with_projection`:

```rust
fn for_each_row<F: FnMut(&[Value])>(
    &mut self,
    table: &str,
    filters: Vec<Filter>,
    projection: Option<Vec<usize>>,
    mut callback: F,
) -> Result<usize> {
    let table_engine = self.get_table_mut(table)?;
    let cols: Vec<usize> = match projection {
        Some(c) => c,
        None => {
            let schema = table_engine.schema()
                .ok_or_else(|| Error::InvalidOperation(
                    "for_each_row with projection=None requires table schema to be set".into()
                ))?;
            (0..schema.columns.len()).collect()
        }
    };

    if filters.is_empty() {
        // Hot path: no filter, just scan + project
        return table_engine.for_each_row_projected(&cols, callback);
    }

    // Filter path: reuse index-choice + raw-bytes-predicate logic from
    // scan_with_projection, but emit via callback instead of Vec<Row>.
    // The plan details the exact translation step-by-step.
    unimplemented!("filter path — see implementation plan")
}
```

The filter path is a mechanical translation of `scan_with_projection`'s logic: index-chosen row IDs feed into a callback-shaped fetch-by-ctids-projected; filtered scan uses `get_rows_by_ctids_filtered` + callback invocation on match.

### 3.3 Why not extend existing methods?

- `scan_with_projection` returns `Vec<Row>`. Changing its signature breaks every caller (SQL layer, integration tests, examples). The trait method is new, existing method stays.
- Conditional materialization (return iterator vs Vec depending on caller) is unidiomatic in Rust without a trait-level iterator abstraction. Two methods is the clean path.

### 3.4 Benchmark migration

Scenario 10 in `tests/perf/vs_sqlite_read.rs` changes from:

```rust
.thunder(|f| {
    let _ = f.thunder_mut().scan_with_projection("blog_posts", vec![], None, None, Some(vec![0])).unwrap();
})
```

to:

```rust
.thunder(|f| {
    let _ = f.thunder_mut().for_each_row("blog_posts", vec![], Some(vec![0]), |_| {}).unwrap();
})
```

The assert closure stays on `scan_with_projection` — it runs once per scenario for correctness, not in the hot loop.

### 3.5 Expected outcome

Per-row Thunder cost drops from ~56ns to ~20-27ns (comparable to SQLite's ~28ns/row). Gap at 10k rows: ~30-40µs Thunder advantage OR dead-even. Verdict: **Win** or **Tie**.

If the result is still Loss, investigate: possibly `value_at_page_bytes` itself has constant overhead we can shave (offset lookup, `Value::from_bytes` tag match for Int32). Fallback ideas (not implemented unless needed):

- Inline a fast-path `Value::int32_at_page_bytes` that bypasses the tag match when the caller knows the column type.
- Skip the `Vec<Value>` buffer entirely and pass `&[u8]` raw bytes to the callback. Callers extract what they want.

These are held as Plan B; Plan A is enough on paper.

---

## 4. Phase 2 design: harness fairness fix

### 4.1 The change

In `tests/perf/common/fixtures.rs`, extend `reopen_handles`:

```rust
pub(crate) fn reopen_handles(f: &mut Fixtures) -> std::io::Result<()> {
    let (_t, _s) = f.take_handles();
    drop(_t);
    drop(_s);

    for p in crate::common::cache::collect_data_files(&f.thunder_dir) {
        let _ = crate::common::cache::posix_fadvise_dontneed(&p);
    }
    // SQLite: main db + WAL companions (best-effort; missing files ignored).
    let _ = crate::common::cache::posix_fadvise_dontneed(&f.sqlite_path);
    for suffix in &["-wal", "-shm"] {
        let mut companion = f.sqlite_path.clone().into_os_string();
        companion.push(suffix);
        let path = std::path::PathBuf::from(companion);
        if path.exists() {
            let _ = crate::common::cache::posix_fadvise_dontneed(&path);
        }
    }

    // Reopens use the same error conversion pattern as SP1's existing
    // reopen_handles (see tests/perf/common/fixtures.rs) — unchanged here.
    let t = Database::open(&f.thunder_dir)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    let s = Connection::open(&f.sqlite_path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
    f.set_handles(t, s);
    Ok(())
}
```

### 4.2 Safety & portability

- Missing companion files (e.g., non-WAL journal mode): `path.exists()` guard skips silently.
- Non-Linux: existing `posix_fadvise_dontneed` already is a no-op (verifies file exists); extension preserves that.
- `.into_os_string()` + `push(suffix)` appends byte-level suffix without requiring the path to be UTF-8.

### 4.3 Self-test

New test in `tests/perf/harness_selftest.rs`:

```rust
#[test]
#[cfg(unix)]
fn cold_fadvises_sqlite_wal_companions() {
    // Build a FAST-mode fixture (WAL journal → creates sqlite.db-wal),
    // verify -wal exists, call reopen_handles, verify no error.
    // fadvise doesn't expose observable cache state, so we assert the
    // code path is reachable and non-erroring; that plus the code
    // inspection (path.exists() guard → posix_fadvise call) is the
    // contract we're validating.
    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);

    let wal_path = {
        let mut s = f.sqlite_path.clone().into_os_string();
        s.push("-wal");
        std::path::PathBuf::from(s)
    };
    assert!(wal_path.exists(), "FAST mode should have created {}", wal_path.display());

    crate::common::fixtures::reopen_handles(&mut f).unwrap();
    drop_fixtures(f);
}
```

This is coarse by design — fadvise doesn't expose observable cache state, so "cache was dropped" isn't directly testable. The assertion is: the code path reaches fadvise for the WAL file without panicking or erroring.

### 4.4 Expected outcome

Re-running the full matrix after Phase 2 produces a new COLD column that may still show some Thunder losses (reopen cost + index reload are legitimate Thunder work), but narrower than the 148x extremes — those were amplified by SQLite's cache-retention advantage.

Whether Thunder wins or loses honest COLD is data for the future COLD-optimization sub-project. SP2 establishes the measurement.

---

## 5. Rebaseline

After Phase 1 + Phase 2 land and tests pass:

1. Run `THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_read --release` at default (SMALL/FAST/WARM).
2. Commit the updated `perf/baseline.json`.
3. Future regressions against this new baseline are caught by the `vs Base` column.

We do NOT rebaseline with `DURABILITY=both CACHE=both` — that would bloat baseline.json with matrix cells the CI doesn't currently gate on. Each cell gets baselined when its sub-project closes.

---

## 6. Testing summary

| Test                                         | Location                                     | Covers                       |
|----------------------------------------------|----------------------------------------------|------------------------------|
| `for_each_row_projected_visits_all_rows`     | `src/storage/paged_table.rs`                 | Phase 1: row count correctness |
| `for_each_row_projected_with_toasted_rows`   | `src/storage/paged_table.rs`                 | Phase 1: TOAST handling        |
| `for_each_row_with_filter_callback`          | `src/storage/table_engine.rs`                | Phase 1: filter integration (via Database layer tests) |
| `for_each_row_database_trait_impl`           | `src/lib.rs` inline or `tests/integration/`  | Phase 1: `Database::for_each_row` wires up correctly |
| Scenario 10 (migrated)                       | `tests/perf/vs_sqlite_read.rs`               | Phase 1: WIN/TIE verdict      |
| `vs_sqlite_read` hard assertion              | `tests/perf/vs_sqlite_read.rs`               | Phase 1: `loss == 0`          |
| `cold_fadvises_sqlite_wal_companions`        | `tests/perf/harness_selftest.rs`             | Phase 2: WAL/SHM fadvise path |

Implementation plan will structure the work as TDD commits.

---

## 7. Deliverables checklist

- [ ] `DirectDataAccess::for_each_row` method defined
- [ ] `PagedTable::for_each_row_projected<F>` implemented
- [ ] `TableEngine::for_each_row_projected<F>` delegates
- [ ] `Database::for_each_row<F>` full impl (filter + index path)
- [ ] Unit tests for Phase 1 paths
- [ ] Scenario 10 migrated to new API
- [ ] `vs_sqlite_read` hard assertion restored to `loss == 0`
- [ ] `reopen_handles` fadvises WAL/SHM companions
- [ ] Phase 2 self-test added
- [ ] `perf/baseline.json` refreshed and committed
- [ ] `CHANGES.md` entry for SP2

---

## 8. Out-of-scope for SP2 — documented follow-ups

- **COLD-specific Thunder optimization.** If post-Phase-2 measurements show significant Thunder COLD regressions vs (fairly measured) SQLite, a follow-up sub-project investigates — likely lazy index loading, mmap-faulting patterns, or eager-work-on-open reduction.
- **Filter-path streaming.** This spec implements `Database::for_each_row` with filter support end-to-end. If the filter path shows any Phase-1-equivalent performance gap vs `scan_with_projection`, SP2's plan may need adjustment — but no filtered-scan benchmark currently demands it, so this risk is low.
- **Alternative fast paths in Plan B (§3.5).** Only triggered if Plan A falls short.
