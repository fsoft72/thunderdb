# SP4b — Query features II — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land eleven new ThunderDB-vs-SQLite benchmark scenarios that cover GROUP BY, scalar aggregates (COUNT/SUM/AVG/MIN/MAX), and DISTINCT; ship a native `aggregate()` + `distinct()` API on `DirectDataAccess`; opportunistically close losses with five indexed/cached fast paths.

**Architecture:** New `tests/perf/vs_sqlite_query2.rs` bench binary, reuses the existing `blog_posts_q` fixture from SP4a, new committed baseline `perf/baseline-query2.json`. Soft FAST/WARM loss gate by default, strict via `SP4B_STRICT_LOSS_GATE=1`. New API in `src/query/direct.rs` (trait + types), implementation in a new `src/query/aggregate.rs`, wired in `src/lib.rs`. Closure work touches `src/index/btree.rs` and `src/storage/table_engine.rs` only when their scenarios show Loss.

**Tech Stack:** Rust 2021, ThunderDB workspace crate, `rusqlite` for the SQLite reference, existing SP1 harness in `tests/perf/common/`.

**Branch:** `sp4b-query-features-ii` (already created from `master`; spec already committed).

**Spec:** `docs/superpowers/specs/2026-04-26-sp4b-query-features-2-design.md`

---

## File Structure

| Path | Role | Disposition |
|---|---|---|
| `src/query/direct.rs` | Add `Aggregate` enum, `AggRow` struct, two trait methods (`aggregate`, `distinct`). | Modify |
| `src/query/aggregate.rs` | New module: hash-grouping aggregator + per-aggregate accumulator + entry helper. | Create |
| `src/query/mod.rs` | Re-export `Aggregate`, `AggRow` from the module. | Modify |
| `src/lib.rs` | Implement `aggregate` + `distinct` on `Database` (impl block at line ~298 already implements `DirectDataAccess`); re-export `Aggregate`, `AggRow` from crate root. | Modify |
| `tests/perf/vs_sqlite_query2.rs` | New bench binary; eleven scenarios; soft loss gate. | Create |
| `Cargo.toml` | Register the new `[[test]]` entry next to `vs_sqlite_query`. | Modify |
| `perf/baseline-query2.json` | Committed baseline for SMALL/FAST/WARM. | Create |
| `src/index/btree.rs` | Optional `scan_distinct_keys` iterator (Task 12 closure 4 only). | Modify (conditional) |
| `src/storage/table_engine.rs` | Optional `row_count` cache (Task 12 closure 2 only); fallback path stays in `aggregate.rs`. | Modify (conditional) |
| `CHANGES.md` | SP4b entry with ratio table. | Modify |

The bench binary owns scenarios + the `vs_sqlite_query2()` test. The aggregator module owns grouping/accumulators. No new harness primitives are introduced — `Scenario`, `Harness`, `Fixtures::snapshot_all/restore_all`, the `reset` hook, and the `Baseline` writer all already exist from SP1/SP3. The fixture is unchanged from SP4a (`build_blog_posts_q_fixtures` in `tests/perf/common/fixtures.rs`).

---

## Task 0: Verify branch and clean tree

**Files:** _none_ — sanity check only.

- [ ] **Step 1: Confirm branch and clean working tree**

Run: `git status && git rev-parse --abbrev-ref HEAD`
Expected: branch `sp4b-query-features-ii`; only `.claude/`, `AGENTS.md`, `CLAUDE.md` untracked from earlier sessions; modified `.gitignore` carry-over is acceptable. No other modifications.

If on the wrong branch, run `git checkout sp4b-query-features-ii`.

---

## Task 1: Add `Aggregate` enum + `AggRow` struct + trait method signatures

**Files:**
- Modify: `src/query/direct.rs` (add types; add two methods to `DirectDataAccess`)
- Modify: `src/query/mod.rs` (re-export types)
- Modify: `src/lib.rs` (re-export from crate root)

This task only adds types and trait method *signatures*. Default implementations come in Task 2/3. The crate will not build cleanly until Task 2 finishes (the `Database` impl will be missing the two methods); that's expected.

- [ ] **Step 1: Append types and trait methods to `src/query/direct.rs`**

Insert immediately after the existing `DirectDataAccess` trait definition (right after the `for_each_row` method, before the `QueryContext` struct), and add the supporting types just above the trait:

```rust
/// Aggregate function over a column (or, for `Count`, over rows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Aggregate {
    /// `COUNT(*)` — counts rows regardless of NULLs.
    Count,
    /// `COUNT(col)` — counts rows where `col` is non-NULL.
    CountCol(String),
    /// `SUM(col)` — INT64 columns only; returns `Value::Null` if all inputs NULL.
    Sum(String),
    /// `AVG(col)` — INT64 columns only; returns `Value::Float64` (or `Value::Null`).
    Avg(String),
    /// `MIN(col)` — typed-as-input; NULLs skipped.
    Min(String),
    /// `MAX(col)` — typed-as-input; NULLs skipped.
    Max(String),
}

/// Result row from `aggregate()`. `keys` is empty for global aggregates
/// (i.e., when `group_by` was empty); otherwise it parallels `group_by`.
/// `aggs` parallels the `aggs` argument in order.
#[derive(Debug, Clone, PartialEq)]
pub struct AggRow {
    pub keys: Vec<Value>,
    pub aggs: Vec<Value>,
}
```

Then add these two method declarations *inside* the `DirectDataAccess` trait (after `for_each_row`, before the closing brace):

```rust
    /// GROUP BY `group_by`, computing `aggs`, with optional WHERE `filters`.
    /// Empty `group_by` = global aggregate; returns exactly one row.
    /// SQLite-matched semantics: NULLs are skipped by SUM/AVG/MIN/MAX/CountCol;
    /// SUM of empty input returns `Value::Null` (not zero); AVG returns `Value::Null`;
    /// MIN/MAX over empty input return `Value::Null`.
    /// Group ordering is implementation-defined.
    fn aggregate(
        &mut self,
        table: &str,
        group_by: Vec<String>,
        aggs: Vec<Aggregate>,
        filters: Vec<Filter>,
    ) -> Result<Vec<AggRow>>;

    /// `SELECT DISTINCT cols FROM table WHERE filters`.
    /// Single-column queries still return `Vec<Vec<Value>>` for shape uniformity.
    /// Row ordering is implementation-defined.
    fn distinct(
        &mut self,
        table: &str,
        cols: Vec<String>,
        filters: Vec<Filter>,
    ) -> Result<Vec<Vec<Value>>>;
```

- [ ] **Step 2: Re-export from `src/query/mod.rs`**

Find the existing `pub use direct::{...}` line. Add `Aggregate` and `AggRow` to the import list. If the file currently re-exports e.g. `pub use direct::{DirectDataAccess, QueryContext, apply_filters};`, change it to `pub use direct::{Aggregate, AggRow, DirectDataAccess, QueryContext, apply_filters};`.

- [ ] **Step 3: Re-export from crate root in `src/lib.rs`**

Find the existing `pub use crate::query::{...}` (or similar) re-export at the top of `src/lib.rs`. Add `Aggregate` and `AggRow` to that list. The `use crate::query::{Filter, Operator, DirectDataAccess, QueryBuilder, choose_index, apply_filters, ...}` import inside the file (line ~27) does not need updating; only the public re-export does.

- [ ] **Step 4: Verify the file parses (build will still fail on missing impls)**

Run: `cargo check 2>&1 | head -30`
Expected: error `not all trait items implemented for `Database`: `aggregate`, `distinct``. Other crates/modules compile clean. If you see a parse error or an unrelated compilation error, fix it before moving on.

- [ ] **Step 5: Do NOT commit yet**

This task leaves the build broken. Task 2 will fix it; commit happens at the end of Task 2.

---

## Task 2: Implement `aggregate()` (default hash-group path)

**Files:**
- Create: `src/query/aggregate.rs`
- Modify: `src/query/mod.rs` (add `pub mod aggregate;`)
- Modify: `src/lib.rs` (add `aggregate()` impl on `Database` inside the `impl DirectDataAccess for Database` block at line ~299)

The default path: scan the table via `for_each_row` (zero per-row alloc, projection limited to needed cols), accumulate into a `HashMap<Vec<Value>, AggState>`, then emit `AggRow`s. No fast paths in this task.

- [ ] **Step 1: Create `src/query/aggregate.rs`**

```rust
//! Hash-grouping aggregator used by `Database::aggregate`.
//!
//! Default code path. Indexed / cached fast paths dispatch in
//! `Database::aggregate` *before* falling through here.

use crate::error::{Error, Result};
use crate::query::direct::{Aggregate, AggRow};
use crate::storage::Value;
use std::collections::HashMap;

/// Per-aggregate accumulator. One slot per element of the input `aggs`.
#[derive(Debug, Clone)]
enum AggSlot {
    Count(u64),
    CountCol { non_null: u64 },
    Sum    { acc: i128, has_value: bool },
    Avg    { acc: i128, n: u64 },
    Min    { v: Option<Value> },
    Max    { v: Option<Value> },
}

impl AggSlot {
    fn from_spec(a: &Aggregate) -> Self {
        match a {
            Aggregate::Count        => AggSlot::Count(0),
            Aggregate::CountCol(_)  => AggSlot::CountCol { non_null: 0 },
            Aggregate::Sum(_)       => AggSlot::Sum { acc: 0, has_value: false },
            Aggregate::Avg(_)       => AggSlot::Avg { acc: 0, n: 0 },
            Aggregate::Min(_)       => AggSlot::Min { v: None },
            Aggregate::Max(_)       => AggSlot::Max { v: None },
        }
    }

    fn finalize(self) -> Value {
        match self {
            AggSlot::Count(n)              => Value::Int64(n as i64),
            AggSlot::CountCol { non_null } => Value::Int64(non_null as i64),
            AggSlot::Sum { has_value: false, .. } => Value::Null,
            AggSlot::Sum { acc, .. }       => Value::Int64(acc as i64),
            AggSlot::Avg { n: 0, .. }      => Value::Null,
            AggSlot::Avg { acc, n }        => Value::Float64(acc as f64 / n as f64),
            AggSlot::Min { v: None }       => Value::Null,
            AggSlot::Min { v: Some(x) }    => x,
            AggSlot::Max { v: None }       => Value::Null,
            AggSlot::Max { v: Some(x) }    => x,
        }
    }
}

/// Per-group state: one `AggSlot` per element of the input `aggs`, in order.
type GroupState = Vec<AggSlot>;

/// Pre-resolved input plan: which column indexes hold the group keys and
/// the column read by each aggregate (None for `Count`).
pub(crate) struct AggPlan {
    pub key_idxs: Vec<usize>,
    pub agg_specs: Vec<Aggregate>,
    pub agg_col_idxs: Vec<Option<usize>>,
    /// Combined projection (group keys ∪ agg cols, deduplicated, in scan order).
    pub projection: Vec<usize>,
    /// Position of each `key_idx` inside `projection`.
    pub key_proj_pos: Vec<usize>,
    /// Position of each `agg_col_idxs[i]` inside `projection`, or None.
    pub agg_proj_pos: Vec<Option<usize>>,
}

/// Build an `AggPlan` from string column names + the column→index mapping
/// of the table's schema.
pub(crate) fn plan(
    schema_cols: &[String],
    group_by: &[String],
    aggs: &[Aggregate],
) -> Result<AggPlan> {
    let lookup = |name: &str| -> Result<usize> {
        schema_cols.iter().position(|c| c == name).ok_or_else(|| {
            Error::Other(format!("aggregate: unknown column `{}`", name))
        })
    };

    let key_idxs: Vec<usize> = group_by.iter()
        .map(|n| lookup(n)).collect::<Result<_>>()?;

    let agg_col_idxs: Vec<Option<usize>> = aggs.iter().map(|a| match a {
        Aggregate::Count                  => Ok(None),
        Aggregate::CountCol(c)
        | Aggregate::Sum(c)
        | Aggregate::Avg(c)
        | Aggregate::Min(c)
        | Aggregate::Max(c)               => lookup(c).map(Some),
    }).collect::<Result<_>>()?;

    let mut projection: Vec<usize> = Vec::new();
    for &k in &key_idxs {
        if !projection.contains(&k) { projection.push(k); }
    }
    for slot in &agg_col_idxs {
        if let Some(idx) = slot {
            if !projection.contains(idx) { projection.push(*idx); }
        }
    }

    let key_proj_pos: Vec<usize> = key_idxs.iter()
        .map(|k| projection.iter().position(|p| p == k).unwrap()).collect();
    let agg_proj_pos: Vec<Option<usize>> = agg_col_idxs.iter()
        .map(|opt| opt.map(|c| projection.iter().position(|p| *p == c).unwrap()))
        .collect();

    Ok(AggPlan {
        key_idxs, agg_specs: aggs.to_vec(),
        agg_col_idxs, projection, key_proj_pos, agg_proj_pos,
    })
}

/// Apply one row (already projected per `plan.projection`) to the accumulator.
pub(crate) fn fold_row(
    plan: &AggPlan,
    groups: &mut HashMap<Vec<Value>, GroupState>,
    row: &[Value],
) {
    let key: Vec<Value> = plan.key_proj_pos.iter()
        .map(|&p| row[p].clone()).collect();

    let entry = groups.entry(key).or_insert_with(|| {
        plan.agg_specs.iter().map(AggSlot::from_spec).collect()
    });

    for (i, spec) in plan.agg_specs.iter().enumerate() {
        let v: Option<&Value> = plan.agg_proj_pos[i].map(|p| &row[p]);
        match (&mut entry[i], spec, v) {
            (AggSlot::Count(n), Aggregate::Count, _) => { *n += 1; }
            (AggSlot::CountCol { non_null }, Aggregate::CountCol(_), Some(val)) => {
                if !matches!(val, Value::Null) { *non_null += 1; }
            }
            (AggSlot::Sum { acc, has_value }, Aggregate::Sum(_), Some(val)) => {
                if let Value::Int64(x) = val {
                    *acc += *x as i128; *has_value = true;
                }
            }
            (AggSlot::Avg { acc, n }, Aggregate::Avg(_), Some(val)) => {
                if let Value::Int64(x) = val { *acc += *x as i128; *n += 1; }
            }
            (AggSlot::Min { v }, Aggregate::Min(_), Some(val)) => {
                if matches!(val, Value::Null) { /* skip */ }
                else if v.as_ref().map_or(true, |cur| val < cur) { *v = Some(val.clone()); }
            }
            (AggSlot::Max { v }, Aggregate::Max(_), Some(val)) => {
                if matches!(val, Value::Null) { /* skip */ }
                else if v.as_ref().map_or(true, |cur| val > cur) { *v = Some(val.clone()); }
            }
            _ => {}
        }
    }
}

/// Drain `groups` into `Vec<AggRow>`. For empty `group_by` and zero rows
/// scanned, emit a single `AggRow { keys: vec![], aggs: <finalized zero-state> }`
/// so `SELECT COUNT(*)` over an empty filter still returns one row.
pub(crate) fn finalize(
    plan: &AggPlan,
    groups: HashMap<Vec<Value>, GroupState>,
) -> Vec<AggRow> {
    if plan.key_idxs.is_empty() && groups.is_empty() {
        let zero_state: GroupState = plan.agg_specs.iter().map(AggSlot::from_spec).collect();
        return vec![AggRow {
            keys: vec![],
            aggs: zero_state.into_iter().map(AggSlot::finalize).collect(),
        }];
    }
    groups.into_iter().map(|(keys, state)| AggRow {
        keys,
        aggs: state.into_iter().map(AggSlot::finalize).collect(),
    }).collect()
}
```

- [ ] **Step 2: Wire the module in `src/query/mod.rs`**

Add a line `pub mod aggregate;` in `src/query/mod.rs` next to the existing module declarations. Re-exports from Task 1 step 2 are unchanged.

- [ ] **Step 3: Implement `aggregate()` on `Database` in `src/lib.rs`**

Inside the existing `impl DirectDataAccess for Database` block (starts at line ~299), after the existing `count` method (line ~697) and before the trait closing brace, add:

```rust
    fn aggregate(
        &mut self,
        table: &str,
        group_by: Vec<String>,
        aggs: Vec<Aggregate>,
        filters: Vec<Filter>,
    ) -> Result<Vec<AggRow>> {
        use crate::query::aggregate as aggm;
        use std::collections::HashMap;

        // Resolve schema column names for the table.
        let schema_cols: Vec<String> = {
            let tbl = self.get_table(table).ok_or_else(|| {
                crate::error::Error::Other(format!("aggregate: unknown table `{}`", table))
            })?;
            tbl.schema().columns.iter().map(|c| c.name.clone()).collect()
        };

        let plan = aggm::plan(&schema_cols, &group_by, &aggs)?;
        let projection = Some(plan.projection.clone());

        let mut groups: HashMap<Vec<Value>, Vec<aggm::AggSlot>> = HashMap::new();
        // Borrow `plan` and `groups` into the closure.
        let plan_ref = &plan;
        self.for_each_row(table, filters, projection, |row| {
            aggm::fold_row(plan_ref, &mut groups, row);
        })?;

        Ok(aggm::finalize(&plan, groups))
    }
```

If `AggSlot` is private to the module (it is — it's `enum AggSlot`, not `pub enum`), the `HashMap<Vec<Value>, Vec<aggm::AggSlot>>` line above won't compile. To avoid leaking `AggSlot`, change the helper signatures so the `HashMap` is owned by the aggregator module:

Replace `fold_row` + `finalize` calls in the impl with a single helper that owns the map. Update `src/query/aggregate.rs` to expose:

```rust
/// Stateful aggregator. Caller calls `feed` for each projected row,
/// then `into_rows` to drain.
pub(crate) struct Aggregator<'a> {
    plan: &'a AggPlan,
    groups: HashMap<Vec<Value>, GroupState>,
}

impl<'a> Aggregator<'a> {
    pub fn new(plan: &'a AggPlan) -> Self {
        Self { plan, groups: HashMap::new() }
    }
    pub fn feed(&mut self, row: &[Value]) { fold_row(self.plan, &mut self.groups, row); }
    pub fn into_rows(self) -> Vec<AggRow> { finalize(self.plan, self.groups) }
}
```

Then the `Database::aggregate` impl becomes:

```rust
    fn aggregate(
        &mut self,
        table: &str,
        group_by: Vec<String>,
        aggs: Vec<Aggregate>,
        filters: Vec<Filter>,
    ) -> Result<Vec<AggRow>> {
        use crate::query::aggregate as aggm;

        let schema_cols: Vec<String> = {
            let tbl = self.get_table(table).ok_or_else(|| {
                crate::error::Error::Other(format!("aggregate: unknown table `{}`", table))
            })?;
            tbl.schema().columns.iter().map(|c| c.name.clone()).collect()
        };

        let plan = aggm::plan(&schema_cols, &group_by, &aggs)?;
        let projection = Some(plan.projection.clone());

        let mut agg = aggm::Aggregator::new(&plan);
        self.for_each_row(table, filters, projection, |row| agg.feed(row))?;

        Ok(agg.into_rows())
    }
```

Notes:
- `Database::get_table` and `TableEngine::schema()` exist (used elsewhere in this file). If the actual method names differ, search `src/lib.rs` for how `count()` reaches the schema and mirror that — `count()` is at line ~697. Do not introduce a new accessor.
- `crate::error::Error::Other` is the catch-all error variant; if the codebase uses a different variant name, follow `count()`'s error pattern.

- [ ] **Step 4: Build and confirm clean compile**

Run: `cargo build --tests 2>&1 | tail -20`
Expected: clean build. If `Aggregate`/`AggRow` aren't visible inside the impl, add `use crate::query::{Aggregate, AggRow};` at the top of `src/lib.rs`.

- [ ] **Step 5: Commit**

```bash
git add src/query/direct.rs src/query/mod.rs src/query/aggregate.rs src/lib.rs
git commit -m "feat(query): add Aggregate/AggRow types + Database::aggregate (hash-group default path)"
```

---

## Task 3: Implement `distinct()`

**Files:**
- Modify: `src/lib.rs` (add `distinct` impl in the same `impl DirectDataAccess for Database` block)

Default path: scan with projection limited to `cols`, push each row tuple into a `HashSet<Vec<Value>>`, drain to `Vec<Vec<Value>>`.

- [ ] **Step 1: Add the impl**

Inside `impl DirectDataAccess for Database`, just below the new `aggregate` method:

```rust
    fn distinct(
        &mut self,
        table: &str,
        cols: Vec<String>,
        filters: Vec<Filter>,
    ) -> Result<Vec<Vec<Value>>> {
        use std::collections::HashSet;

        let schema_cols: Vec<String> = {
            let tbl = self.get_table(table).ok_or_else(|| {
                crate::error::Error::Other(format!("distinct: unknown table `{}`", table))
            })?;
            tbl.schema().columns.iter().map(|c| c.name.clone()).collect()
        };

        let proj_idxs: Vec<usize> = cols.iter().map(|name| {
            schema_cols.iter().position(|c| c == name).ok_or_else(|| {
                crate::error::Error::Other(format!("distinct: unknown column `{}`", name))
            })
        }).collect::<Result<_>>()?;

        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        let projection = Some(proj_idxs.clone());
        let proj_len = proj_idxs.len();

        self.for_each_row(table, filters, projection, |row| {
            let key: Vec<Value> = (0..proj_len).map(|i| row[i].clone()).collect();
            seen.insert(key);
        })?;

        Ok(seen.into_iter().collect())
    }
```

`Value` must implement `Eq + Hash` for `HashSet` to work. Verify by grepping `src/storage/value.rs` for `impl Hash for Value` / `derive(Hash, Eq)`. If `Value` does not implement `Hash`, fall back to a `BTreeMap<Vec<Value>, ()>` keyed on byte-serialized values:

```rust
// fallback: byte-key dedup if Value isn't Hash
let mut seen: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
let mut buf: Vec<Vec<Value>> = Vec::new();
self.for_each_row(table, filters, projection, |row| {
    let key: Vec<Value> = (0..proj_len).map(|i| row[i].clone()).collect();
    let bytes = bincode::serialize(&key).unwrap();  // Value already implements Serialize
    if seen.insert(bytes) { buf.push(key); }
})?;
Ok(buf)
```

Choose whichever path matches the existing `Value` derives. Do NOT add `derive(Hash)` to `Value` for this SP — that is a wider change.

- [ ] **Step 2: Build**

Run: `cargo build --tests 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add src/lib.rs
git commit -m "feat(query): add Database::distinct (hash-set default path)"
```

---

## Task 4: Correctness unit tests for `aggregate` + `distinct`

**Files:**
- Create: `tests/aggregate_distinct.rs` (top-level integration test, sibling of existing tests)

Cover the SQLite-matched semantics that scenarios alone may not catch (empty table, all-NULL column, GROUP BY with a NULL key).

- [ ] **Step 1: Write the failing tests**

```rust
//! Correctness tests for Database::aggregate and Database::distinct.

use thunderdb::{
    Aggregate, Database, DirectDataAccess, Filter, Operator, Value,
};
use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};

fn open_with_schema(cols: Vec<(&str, &str)>) -> Database {
    let dir = std::env::temp_dir().join(format!(
        "thunderdb_aggdist_{}_{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = Database::open(&dir).unwrap();
    // Insert one dummy row to force table creation, then overwrite schema.
    let mut placeholder: Vec<Value> = (0..cols.len()).map(|_| Value::Int64(0)).collect();
    placeholder[0] = Value::Int64(1);
    db.insert_batch("t", vec![placeholder]).unwrap();
    {
        let tbl = db.get_table_mut("t").unwrap();
        tbl.set_schema(TableSchema {
            columns: cols.iter().map(|(n, ty)| ColumnInfo {
                name: (*n).into(), data_type: (*ty).into(),
            }).collect(),
        }).unwrap();
    }
    db.delete("t", vec![]).unwrap();   // empty the table
    db
}

#[test]
fn aggregate_count_star_empty() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    let r = db.aggregate("t", vec![], vec![Aggregate::Count], vec![]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].keys, vec![]);
    assert_eq!(r[0].aggs, vec![Value::Int64(0)]);
}

#[test]
fn aggregate_sum_empty_is_null_not_zero() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    let r = db.aggregate("t", vec![], vec![Aggregate::Sum("v".into())], vec![]).unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].aggs, vec![Value::Null], "SUM of empty must be NULL (SQLite parity)");
}

#[test]
fn aggregate_avg_min_max_skip_nulls() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("v", "INT64")]);
    db.insert_batch("t", vec![
        vec![Value::Int64(1), Value::Int64(10)],
        vec![Value::Int64(2), Value::Null],
        vec![Value::Int64(3), Value::Int64(30)],
    ]).unwrap();

    let r = db.aggregate("t", vec![], vec![
        Aggregate::Avg("v".into()),
        Aggregate::Min("v".into()),
        Aggregate::Max("v".into()),
    ], vec![]).unwrap();

    assert_eq!(r.len(), 1);
    assert_eq!(r[0].aggs[0], Value::Float64(20.0));
    assert_eq!(r[0].aggs[1], Value::Int64(10));
    assert_eq!(r[0].aggs[2], Value::Int64(30));
}

#[test]
fn aggregate_group_by_with_null_key() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("k", "VARCHAR")]);
    db.insert_batch("t", vec![
        vec![Value::Int64(1), Value::varchar("a".into())],
        vec![Value::Int64(2), Value::varchar("a".into())],
        vec![Value::Int64(3), Value::Null],
        vec![Value::Int64(4), Value::varchar("b".into())],
    ]).unwrap();

    let mut r = db.aggregate("t", vec!["k".into()],
        vec![Aggregate::Count], vec![]).unwrap();
    // Multiset compare — sort by debug repr of key.
    r.sort_by_key(|row| format!("{:?}", row.keys));

    assert_eq!(r.len(), 3, "two non-null groups + one NULL group");
    let total: i64 = r.iter().map(|row| match row.aggs[0] {
        Value::Int64(n) => n, _ => panic!("expected Int64 count"),
    }).sum();
    assert_eq!(total, 4);
}

#[test]
fn distinct_low_card_with_filter() {
    let mut db = open_with_schema(vec![("id", "INT64"), ("k", "VARCHAR")]);
    db.insert_batch("t", vec![
        vec![Value::Int64(1), Value::varchar("a".into())],
        vec![Value::Int64(2), Value::varchar("a".into())],
        vec![Value::Int64(3), Value::varchar("b".into())],
        vec![Value::Int64(4), Value::varchar("c".into())],
    ]).unwrap();

    let mut d = db.distinct("t", vec!["k".into()],
        vec![Filter::new("id", Operator::GreaterThan(Value::Int64(1)))]).unwrap();
    d.sort();
    assert_eq!(d.len(), 3);
    assert_eq!(d[0], vec![Value::varchar("a".into())]);
    assert_eq!(d[1], vec![Value::varchar("b".into())]);
    assert_eq!(d[2], vec![Value::varchar("c".into())]);
}
```

If `Operator::GreaterThan` has a different name in the codebase, replace with whatever's used in `src/query/filter.rs` (search for the existing variants — SP4a's bench uses `Operator::Equals`, `Operator::IsNull`, `Operator::IsNotNull`, so `GreaterThan` may need to be substituted with `Operator::Equals(Value::Int64(2)) | Operator::Equals(...)` style — easier: change the filter to `Filter::new("k", Operator::IsNotNull)` if `GreaterThan` isn't available, and adjust the assertion accordingly).

- [ ] **Step 2: Run the tests**

Run: `cargo test --test aggregate_distinct --release -- --nocapture`
Expected: PASS. If any test fails, fix the implementation in `src/query/aggregate.rs` (Task 2) or `src/lib.rs::distinct` (Task 3) — the spec semantics are pinned, the tests are the source of truth here.

- [ ] **Step 3: Commit**

```bash
git add tests/aggregate_distinct.rs
git commit -m "test(query): correctness tests for aggregate + distinct (NULL/empty/group)"
```

---

## Task 5: Skeleton bench binary

**Files:**
- Create: `tests/perf/vs_sqlite_query2.rs`
- Modify: `Cargo.toml` (register the new `[[test]]`)

Empty scenario list at first; just enough wiring to build, run, and produce an empty report against `perf/baseline-query2.json`.

- [ ] **Step 1: Register the bench in Cargo.toml**

Add immediately after the `vs_sqlite_query` entry:

```toml
[[test]]
name = "vs_sqlite_query2"
path = "tests/perf/vs_sqlite_query2.rs"
```

- [ ] **Step 2: Create the bench file**

```rust
//! ThunderDB vs SQLite — query-features II scenarios (SP4b).
//! Covers GROUP BY, scalar aggregates (COUNT/SUM/AVG/MIN/MAX), and DISTINCT.

mod common;

use common::*;
#[allow(unused_imports)]
use thunderdb::{Aggregate, DirectDataAccess, Filter, Operator, Value};
use std::path::PathBuf;

fn scenarios() -> Vec<Scenario> {
    vec![
        // populated in Tasks 6..10
    ]
}

#[test]
fn vs_sqlite_query2() {
    let h = Harness::from_env();
    let baseline_path = PathBuf::from("perf/baseline-query2.json");
    let artifact_dir = PathBuf::from("target/perf");
    let report = h.run(&scenarios(), &baseline_path, &artifact_dir);

    // Hard correctness gate (always on).
    assert!(
        report.summary.failure == 0,
        "query2 scenarios have {} failure(s)", report.summary.failure
    );

    // Soft loss gate by default. Strict mode opted in via env var.
    if std::env::var("SP4B_STRICT_LOSS_GATE").as_deref() == Ok("1") {
        assert!(
            report.summary.loss == 0,
            "query2 scenarios have {} loss(es) (strict gate)", report.summary.loss
        );
    } else if report.summary.loss > 0 {
        eprintln!(
            "warn: {} loss(es) under soft loss gate; set SP4B_STRICT_LOSS_GATE=1 to fail",
            report.summary.loss
        );
    }
}
```

- [ ] **Step 3: Build and run with the empty scenario list**

Run: `cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: PASS, no scenarios reported, summary all zeroes. The harness writes an empty report to `target/perf/`. `perf/baseline-query2.json` is not created yet (no scenarios to baseline).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): scaffold vs_sqlite_query2 bench binary (SP4b)"
```

---

## Task 6: Q10 + Q11 — COUNT(*) full and COUNT(*) WHERE indexed

**Files:**
- Modify: `tests/perf/vs_sqlite_query2.rs`

Q10: `SELECT COUNT(*) FROM blog_posts_q`
Q11: `SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7`

Thunder closures use the new `aggregate()` API. Asserts compare directly against SQLite.

- [ ] **Step 1: Add Q10 + Q11 to the `scenarios()` vec**

Replace the empty body:

```rust
fn scenarios() -> Vec<Scenario> {
    vec![
        // Q10. COUNT(*) full table
        Scenario::new("Q10. COUNT(*) full table", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q10 count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q11. COUNT(*) WHERE author_id = 7  (indexed)
        Scenario::new("Q11. COUNT(*) WHERE indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count],
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count],
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q11 count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
    ]
}
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: 2 scenarios printed, `failure == 0`. Loss verdicts allowed under the soft gate.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): SP4b Q10+Q11 COUNT(*) full + WHERE indexed"
```

---

## Task 7: Q12 + Q13 — SUM and AVG over non-indexed int

**Files:**
- Modify: `tests/perf/vs_sqlite_query2.rs`

Q12: `SELECT SUM(views) FROM blog_posts_q`
Q13: `SELECT AVG(views) FROM blog_posts_q`

- [ ] **Step 1: Append Q12 + Q13**

Add inside the `vec![ ... ]` after Q11:

```rust
        // Q12. SUM(views)  — non-indexed full scan
        Scenario::new("Q12. SUM int non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Sum("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Sum("views".into())], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q12 sum: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q13. AVG(views) — non-indexed full scan
        Scenario::new("Q13. AVG int non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Avg("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: f64 = f.sqlite().query_row(
                    "SELECT AVG(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Avg("views".into())], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Float64(x) => x, _ => f64::NAN };
                let s: f64 = f.sqlite().query_row(
                    "SELECT AVG(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if (t - s).abs() > 1e-6 {
                    Err(format!("Q13 avg: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: 4 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): SP4b Q12+Q13 SUM and AVG over non-indexed int"
```

---

## Task 8: Q14 + Q15 — MIN/MAX indexed and non-indexed

**Files:**
- Modify: `tests/perf/vs_sqlite_query2.rs`

Q14: `SELECT MIN(id), MAX(id) FROM blog_posts_q` (indexed PK)
Q15: `SELECT MIN(views), MAX(views) FROM blog_posts_q` (non-indexed)

- [ ] **Step 1: Append Q14 + Q15**

```rust
        // Q14. MIN/MAX over indexed PK
        Scenario::new("Q14. MIN/MAX indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("id".into()), Aggregate::Max("id".into())],
                    vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(id), MAX(id) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("id".into()), Aggregate::Max("id".into())], vec![]).unwrap();
                let (tmin, tmax) = match (&r[0].aggs[0], &r[0].aggs[1]) {
                    (Value::Int64(a), Value::Int64(b)) => (*a, *b), _ => (-1, -1),
                };
                let (smin, smax): (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(id), MAX(id) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                if (tmin, tmax) != (smin, smax) {
                    Err(format!("Q14 minmax: thunder=({},{}), sqlite=({},{})", tmin, tmax, smin, smax))
                } else { Ok(()) }
            })
            .build(),

        // Q15. MIN/MAX over non-indexed views
        Scenario::new("Q15. MIN/MAX non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("views".into()), Aggregate::Max("views".into())],
                    vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(views), MAX(views) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("views".into()), Aggregate::Max("views".into())], vec![]).unwrap();
                let (tmin, tmax) = match (&r[0].aggs[0], &r[0].aggs[1]) {
                    (Value::Int64(a), Value::Int64(b)) => (*a, *b), _ => (-1, -1),
                };
                let (smin, smax): (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(views), MAX(views) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                if (tmin, tmax) != (smin, smax) {
                    Err(format!("Q15 minmax: thunder=({},{}), sqlite=({},{})", tmin, tmax, smin, smax))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: 6 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): SP4b Q14+Q15 MIN/MAX indexed and non-indexed"
```

---

## Task 9: Q16 + Q17 + Q18 — GROUP BY scenarios

**Files:**
- Modify: `tests/perf/vs_sqlite_query2.rs`

Q16: `SELECT author_id, COUNT(*) … GROUP BY author_id` (50 groups, indexed key)
Q17: `SELECT category, COUNT(*) … GROUP BY category` (5 + NULL groups, non-indexed key)
Q18: `SELECT author_id, SUM(views) … GROUP BY author_id`

Asserts compare result-set sizes and the global sum-over-groups against SQLite for parity.

- [ ] **Step 1: Append Q16, Q17, Q18**

```rust
        // Q16. GROUP BY indexed low-card
        Scenario::new("Q16. GROUP BY indexed low-card", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT author_id, COUNT(*) FROM blog_posts_q GROUP BY author_id").unwrap();
                let _: Vec<(i64, i64)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i64 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT author_id FROM blog_posts_q GROUP BY author_id)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total {
                    Err(format!("Q16 groups: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),

        // Q17. GROUP BY non-indexed low-card (category, includes NULL group)
        Scenario::new("Q17. GROUP BY non-indexed low-card", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["category".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT category, COUNT(*) FROM blog_posts_q GROUP BY category").unwrap();
                let _: Vec<(Option<String>, i64)> = st.query_map([],
                    |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["category".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i64 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT category FROM blog_posts_q GROUP BY category)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total {
                    Err(format!("Q17 groups: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),

        // Q18. GROUP BY indexed + SUM
        Scenario::new("Q18. GROUP BY + SUM", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Sum("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT author_id, SUM(views) FROM blog_posts_q GROUP BY author_id").unwrap();
                let _: Vec<(i64, i64)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Sum("views".into())], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i128 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n as i128, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT author_id FROM blog_posts_q GROUP BY author_id)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total as i128 {
                    Err(format!("Q18 groups+sum: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: 9 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): SP4b Q16+Q17+Q18 GROUP BY scenarios"
```

---

## Task 10: Q19 + Q20 — DISTINCT high-card and low-card

**Files:**
- Modify: `tests/perf/vs_sqlite_query2.rs`

Q19: `SELECT DISTINCT slug FROM blog_posts_q` (10 000 distinct, indexed)
Q20: `SELECT DISTINCT category FROM blog_posts_q` (5 + NULL, non-indexed)

- [ ] **Step 1: Append Q19 + Q20**

```rust
        // Q19. DISTINCT high-card indexed (slug)
        Scenario::new("Q19. DISTINCT high-card indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["slug".into()], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT DISTINCT slug FROM blog_posts_q").unwrap();
                let _: Vec<String> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["slug".into()], vec![]).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT DISTINCT slug FROM blog_posts_q)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q19 distinct: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q20. DISTINCT low-card non-indexed (category)
        Scenario::new("Q20. DISTINCT low-card non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["category".into()], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT DISTINCT category FROM blog_posts_q").unwrap();
                let _: Vec<Option<String>> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["category".into()], vec![]).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT DISTINCT category FROM blog_posts_q)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q20 distinct: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Full run (no QUICK, baseline-quality)**

Run: `cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: 11 scenarios, `failure == 0`. Loss verdicts expected and accepted under the soft gate; record per-scenario ratios from the printed report for use in Task 11/12 and the CHANGES entry.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query2.rs
git commit -m "feat(perf): SP4b Q19+Q20 DISTINCT high-card and low-card"
```

---

## Task 11: Promote baseline + commit

**Files:**
- Create: `perf/baseline-query2.json`

- [ ] **Step 1: Promote the current run as the baseline**

Run: `THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: PASS, harness writes `perf/baseline-query2.json`.

- [ ] **Step 2: Re-run without the env var to confirm the baseline reads back cleanly**

Run: `cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: PASS. Each scenario's ratio is now compared against the just-promoted baseline.

- [ ] **Step 3: Commit**

```bash
git add perf/baseline-query2.json
git commit -m "perf(sp4b): promote baseline-query2.json (SMALL/FAST/WARM)"
```

---

## Task 12: Quick-win perf pass (CONDITIONAL)

This task is a single umbrella; the work inside is **opt-in per-fix**. Only attempt a fix if its scenario shows a Loss in the baseline from Task 11. Stop on any fix that grows past a focused patch — defer the rest to a follow-up SP. Do NOT chase Q12, Q13, Q15, Q17, Q20: those are full-scan-bound on the row-decoder hotspot and are explicitly deferred per the spec.

For each fix, follow the loop below.

### Closure 1 — MIN/MAX indexed → O(1) (closes Q14)

**Files:**
- Modify: `src/lib.rs` (`Database::aggregate`)

- [ ] **Step 1: Add a unit test**

Append to `tests/aggregate_distinct.rs`:

```rust
#[test]
fn aggregate_min_max_indexed_uses_btree_endpoints() {
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
    let dir = std::env::temp_dir().join(format!(
        "thunderdb_minmax_idx_{}_{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut db = thunderdb::Database::open(&dir).unwrap();

    db.insert_batch("t", (1..=1000).map(|i| vec![
        thunderdb::Value::Int64(i), thunderdb::Value::Int64(i * 10),
    ]).collect()).unwrap();
    {
        let tbl = db.get_table_mut("t").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT64".into() },
            ColumnInfo { name: "v".into(),  data_type: "INT64".into() },
        ]}).unwrap();
        tbl.create_index("id").unwrap();
    }

    let r = db.aggregate("t", vec![], vec![
        thunderdb::Aggregate::Min("id".into()),
        thunderdb::Aggregate::Max("id".into()),
    ], vec![]).unwrap();
    assert_eq!(r[0].aggs, vec![thunderdb::Value::Int64(1), thunderdb::Value::Int64(1000)]);

    // Negative test: non-indexed column still works (falls through to scan path).
    let r = db.aggregate("t", vec![], vec![
        thunderdb::Aggregate::Min("v".into()),
        thunderdb::Aggregate::Max("v".into()),
    ], vec![]).unwrap();
    assert_eq!(r[0].aggs, vec![thunderdb::Value::Int64(10), thunderdb::Value::Int64(10000)]);
}
```

Run: `cargo test --test aggregate_distinct --release aggregate_min_max_indexed_uses_btree_endpoints -- --nocapture`
Expected: PASS (correctness already covered by default path); the win is perf, not behavior.

- [ ] **Step 2: Implement dispatch in `Database::aggregate`**

At the top of `Database::aggregate` in `src/lib.rs`, *before* calling `aggm::plan(...)`, dispatch:

```rust
        // Fast path: global MIN/MAX over indexed columns, no filters.
        if group_by.is_empty() && filters.is_empty()
            && aggs.iter().all(|a| matches!(a, Aggregate::Min(_) | Aggregate::Max(_)))
        {
            let tbl = self.get_table_mut(table).ok_or_else(|| {
                crate::error::Error::Other(format!("aggregate: unknown table `{}`", table))
            })?;
            let mut all_indexed = true;
            for a in &aggs {
                let col = match a { Aggregate::Min(c) | Aggregate::Max(c) => c, _ => unreachable!() };
                if !tbl.has_index(col) { all_indexed = false; break; }
            }
            if all_indexed {
                let mut out = Vec::with_capacity(aggs.len());
                for a in &aggs {
                    let (col, want_max) = match a {
                        Aggregate::Min(c) => (c, false),
                        Aggregate::Max(c) => (c, true),
                        _ => unreachable!(),
                    };
                    // scan_first_k(1) for MIN, scan_last_k(1) for MAX, then read the column.
                    let row_ids = if want_max {
                        tbl.indexed_top_k_row_ids(col, 1, true)?
                    } else {
                        tbl.indexed_top_k_row_ids(col, 1, false)?
                    };
                    let v = if let Some(rid) = row_ids.first() {
                        tbl.value_at(*rid, col)?  // existing accessor — name may differ
                    } else { Value::Null };
                    out.push(v);
                }
                return Ok(vec![AggRow { keys: vec![], aggs: out }]);
            }
        }
```

Note: the exact accessor names (`has_index`, `indexed_top_k_row_ids`, `value_at`) must match what's already on `TableEngine` / `IndexManager`. SP4a added `IndexManager::indexed_top_k_row_ids` and `Database::scan_indexed_top_k`; reuse those — `Database::scan_indexed_top_k(table, col, 1, want_max)` already returns `Vec<Row>`, which is even simpler:

```rust
                let rows = self.scan_indexed_top_k(table, col, 1, want_max)?;
                let v = rows.first()
                    .and_then(|r| {
                        // find column index in schema, then read
                        let cols = self.get_table(table).unwrap().schema().columns.clone();
                        let idx = cols.iter().position(|c| &c.name == col)?;
                        r.values.get(idx).cloned()
                    })
                    .unwrap_or(Value::Null);
                out.push(v);
```

Use whichever variant compiles cleanly with the existing API. The full row decode for one row is already cheap; the win is avoiding the 10 000-row scan.

- [ ] **Step 3: Run the unit test + bench**

Run: `cargo test --test aggregate_distinct --release -- --nocapture`
Then: `cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: unit tests PASS; Q14's verdict moves toward Tie or Win.

- [ ] **Step 4: Re-promote baseline + commit**

```bash
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
git add src/lib.rs tests/aggregate_distinct.rs perf/baseline-query2.json
git commit -m "perf(query): MIN/MAX over indexed cols use B-tree endpoints (closes Q14)"
```

### Closure 2 — COUNT(*) full → O(1) cached row count (closes Q10)

**Files:**
- Modify: `src/storage/table_engine.rs` (add `row_count` accessor + counter)
- Modify: `src/lib.rs` (`Database::aggregate` dispatch)

If adding a persisted counter is too invasive, ship the **fallback fast scan-only** path instead: dispatch on the same shape (`group_by.is_empty() && aggs == [Count] && filters.is_empty()`) and use `for_each_row` with an empty projection (`Some(vec![])`) so no values are decoded — only the row presence is counted.

- [ ] **Step 1: Decide — full closure or fallback**

Decision criteria: if `TableEngine` already tracks an internal count (search `pub fn row_count\|pub fn len`), wire it through. If not, ship the fallback. Do not add persistence in this SP unless the patch is one or two lines.

- [ ] **Step 2 (fallback path): Dispatch in `Database::aggregate`**

```rust
        // Fast path: global COUNT(*) with no filters → scan with empty projection.
        if group_by.is_empty() && filters.is_empty()
            && aggs.len() == 1 && matches!(aggs[0], Aggregate::Count)
        {
            let mut n: u64 = 0;
            self.for_each_row(table, vec![], Some(vec![]), |_| { n += 1; })?;
            return Ok(vec![AggRow { keys: vec![], aggs: vec![Value::Int64(n as i64)] }]);
        }
```

If the existing `count(table, vec![])` already takes this path internally, just call it and wrap:

```rust
            let n = self.count(table, vec![])?;
            return Ok(vec![AggRow { keys: vec![], aggs: vec![Value::Int64(n as i64)] }]);
```

- [ ] **Step 3: Bench + promote + commit**

Run: `cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2`
Expected: Q10 verdict improves.

```bash
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
git add src/lib.rs perf/baseline-query2.json
git commit -m "perf(query): COUNT(*) global uses count() fast path (closes Q10)"
```

### Closure 3 — COUNT(*) WHERE indexed → O(K) probe (closes Q11)

**Files:**
- Modify: `src/lib.rs` (`Database::aggregate` dispatch)

`count(table, filters)` already exists and routes to the index lookup for `author_id = 7`. The work is dispatch:

```rust
        // Fast path: COUNT(*) with single equality filter on an indexed column.
        if group_by.is_empty()
            && aggs.len() == 1 && matches!(aggs[0], Aggregate::Count)
        {
            let n = self.count(table, filters)?;
            return Ok(vec![AggRow { keys: vec![], aggs: vec![Value::Int64(n as i64)] }]);
        }
```

This subsumes Closure 2's fallback — keep one combined dispatch.

- [ ] **Step 1: Replace closures 2+3 with the combined dispatch above**

- [ ] **Step 2: Bench + promote + commit**

```bash
cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
git add src/lib.rs perf/baseline-query2.json
git commit -m "perf(query): COUNT(*) routes through count() fast path (closes Q10+Q11)"
```

### Closure 4 — DISTINCT over single indexed col → leaf-walk (closes Q19)

**Files:**
- Modify: `src/index/btree.rs` (add `scan_distinct_keys`)
- Modify: `src/lib.rs` (`Database::distinct` dispatch)

- [ ] **Step 1: Add `BTree::scan_distinct_keys`**

In `src/index/btree.rs`, find the existing `scan_first_k` / `scan_last_k` implementations added by SP4a. Add a sibling that walks the leaf chain forward and skips consecutive duplicate keys. Return either an `impl Iterator<Item = Value>` or a `Vec<Value>` — pick whichever matches the existing helpers' style.

```rust
/// Walk the leaf chain forward, emitting each unique key exactly once.
/// Assumes the leaf chain is already sorted.
pub fn scan_distinct_keys(&self) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    // Reuse leaf iteration from scan_first_k. The exact API is the existing
    // `walk_leaves_forward` (or whatever scan_first_k uses).
    self.for_each_key_in_order(|k| {
        match out.last() {
            Some(prev) if prev == k => {}
            _ => out.push(k.clone()),
        }
    });
    out
}
```

If `for_each_key_in_order` does not already exist, factor the leaf-chain walk out of `scan_first_k` into a private helper, then reuse it from both `scan_first_k` and `scan_distinct_keys`. If that grows beyond a focused refactor, defer this closure.

- [ ] **Step 2: Add a unit test**

Append to whatever `#[cfg(test)] mod tests` lives next to `BTree`:

```rust
#[test]
fn btree_scan_distinct_keys_dedups() {
    let mut t = BTree::new();
    for k in [1i64, 1, 2, 2, 2, 3, 4, 4, 5] {
        t.insert(Value::Int64(k), 0);
    }
    let keys: Vec<i64> = t.scan_distinct_keys().into_iter().filter_map(|v| match v {
        Value::Int64(n) => Some(n), _ => None,
    }).collect();
    assert_eq!(keys, vec![1, 2, 3, 4, 5]);
}
```

If the `BTree::insert` signature requires different args, mirror the existing tests in that file.

- [ ] **Step 3: Dispatch in `Database::distinct`**

At the top of `Database::distinct` in `src/lib.rs`, before the default scan path:

```rust
        // Fast path: single indexed column, no filters.
        if cols.len() == 1 && filters.is_empty() {
            let col = &cols[0];
            let tbl = self.get_table(table).ok_or_else(|| {
                crate::error::Error::Other(format!("distinct: unknown table `{}`", table))
            })?;
            if tbl.has_index(col) {
                let keys = tbl.index_distinct_keys(col)?;  // wrapper around BTree::scan_distinct_keys
                return Ok(keys.into_iter().map(|v| vec![v]).collect());
            }
        }
```

`TableEngine::index_distinct_keys` is a thin wrapper: look up the column's `BTree` via `IndexManager`, call `scan_distinct_keys`. If introducing this wrapper grows the patch, call into `IndexManager` directly from `Database::distinct`.

- [ ] **Step 4: Bench + promote + commit**

```bash
cargo test --release
cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query2 --release -- --nocapture vs_sqlite_query2
git add src/index/btree.rs src/lib.rs perf/baseline-query2.json
git commit -m "perf(query): DISTINCT single indexed col uses B-tree leaf walk (closes Q19)"
```

### Closure 5 — GROUP BY single indexed col → ordered chunk-aggregate (closes Q16/Q18)

**Files:**
- Modify: `src/lib.rs` (`Database::aggregate` dispatch)
- Possibly: `src/index/btree.rs` (expose ordered key+rowid iteration; may already exist)

Walk the index in key order; emit an `AggRow` each time the key changes. Avoids the hashmap; allocations are O(distinct_keys) instead of O(rows).

- [ ] **Step 1: Sketch the dispatch**

```rust
        // Fast path: GROUP BY single indexed col, no filters.
        if group_by.len() == 1 && filters.is_empty() {
            let key_col = &group_by[0];
            let tbl_has_index = self.get_table(table)
                .map(|t| t.has_index(key_col)).unwrap_or(false);
            if tbl_has_index {
                // Walk the index in order; for each key, look up matching rows
                // and feed them through Aggregator::feed for that single group.
                // This is meaningfully faster only when distinct_keys ≪ rows.
                // ... implementation details specific to IndexManager surface ...
            }
        }
```

The detailed implementation depends on the existing `IndexManager` API. If walking the index leaf chain by key + collecting rowids is straightforward, land it. If it requires plumbing a new iterator through `BTree` or `IndexManager`, defer this closure.

- [ ] **Step 2: Bench + promote + commit (only if landed)**

Same pattern as previous closures. Skip the entire closure if Step 1 grows beyond a focused patch.

---

## Task 13: Run full ThunderDB test suite

**Files:** _none_ — regression check.

- [ ] **Step 1: Run the full test suite**

Run: `cargo test --release`
Expected: PASS. Pay special attention to `vs_sqlite_read`, `vs_sqlite_write`, and `vs_sqlite_query` — none of them must regress.

If any earlier suite loses scenarios, revisit the most recent perf patch from Task 12. Either fix the regression or revert the patch. Do not gate read/write losses through the soft gate; their hard gate is intentional. SP4a's `vs_sqlite_query` baseline is also hard-comparison territory now.

---

## Task 14: CHANGES.md entry

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Append the SP4b entry directly under the document title**

The new section goes immediately after the leading `# ThunderDB Changes` line and before the existing `## 2026-04-26 - SP4a: Query features I` block, so entries stay in reverse-chronological order. Use the ratio table from the final Task 11 / Task 12 baseline run.

```markdown
## 2026-04-26 - SP4b: Query features II (GROUP BY, aggregates, DISTINCT)

Sixth sub-project in the "faster than SQLite in all benchmarks" program. Adds query-shape coverage for the SQL features deferred from SP4a — `GROUP BY`, scalar aggregates, and `DISTINCT` — and ships native `DirectDataAccess::aggregate` and `DirectDataAccess::distinct` APIs to compute them.

- **11 new query scenarios** in `tests/perf/vs_sqlite_query2.rs`:
  - Q10 COUNT(*) full table
  - Q11 COUNT(*) WHERE author_id = 7 (indexed)
  - Q12 SUM(views) — non-indexed full scan
  - Q13 AVG(views) — non-indexed full scan
  - Q14 MIN/MAX(id) — indexed PK
  - Q15 MIN/MAX(views) — non-indexed
  - Q16 GROUP BY author_id, COUNT(*) — indexed key
  - Q17 GROUP BY category, COUNT(*) — non-indexed key (includes NULL group)
  - Q18 GROUP BY author_id, SUM(views) — indexed key
  - Q19 SELECT DISTINCT slug — high-card indexed
  - Q20 SELECT DISTINCT category — low-card non-indexed
- **New API**: `Aggregate` enum (`Count`, `CountCol`, `Sum`, `Avg`, `Min`, `Max`), `AggRow` struct, and two `DirectDataAccess` methods `aggregate(table, group_by, aggs, filters)` and `distinct(table, cols, filters)`. SQLite-matched semantics: SUM/AVG/MIN/MAX skip NULLs; SUM of empty input returns NULL, not zero.
- **Shared fixture**: SP4a's `blog_posts_q` (10 000 rows) reused unchanged.
- **Soft FAST/WARM loss gate by default**, strict via `SP4B_STRICT_LOSS_GATE=1`. Mirrors SP3 / SP4a policy.
- **Closures landed in this SP** (filled in based on Task 12 outcome):
  - <fill in: MIN/MAX indexed B-tree endpoints / COUNT(*) fast path / DISTINCT leaf walk / GROUP BY ordered chunk-aggregate / etc.>
- **FAST/WARM ratios (SMALL tier, 11 samples)** — fill in from the final baseline:

  | Scenario | Thunder | SQLite | Ratio | Verdict |
  |---|---|---|---|---|
  | Q10. COUNT(*) full table | … | … | …x | … |
  | Q11. COUNT(*) WHERE indexed | … | … | …x | … |
  | Q12. SUM int non-indexed | … | … | …x | … |
  | Q13. AVG int non-indexed | … | … | …x | … |
  | Q14. MIN/MAX indexed | … | … | …x | … |
  | Q15. MIN/MAX non-indexed | … | … | …x | … |
  | Q16. GROUP BY indexed low-card | … | … | …x | … |
  | Q17. GROUP BY non-indexed low-card | … | … | …x | … |
  | Q18. GROUP BY + SUM | … | … | …x | … |
  | Q19. DISTINCT high-card indexed | … | … | …x | … |
  | Q20. DISTINCT low-card non-indexed | … | … | …x | … |

- **Separate baseline file**: query II scenarios are committed to `perf/baseline-query2.json`; SP4a's query I scenarios stay on `perf/baseline-query.json`. Same per-binary baseline pattern from SP3.

Spec: `docs/superpowers/specs/2026-04-26-sp4b-query-features-2-design.md`
Plan: `docs/superpowers/plans/2026-04-26-sp4b-query-features-2.md`
```

When filling in the ratio table, copy numbers exactly from the harness's printed report after the final Task 11 (or Task 12) baseline run.

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs(sp4b): CHANGES entry with ratio table"
```

---

## Task 15: Update the program-tracker memory note

**Files:** _none_ — update the user's auto-memory file directly.

- [ ] **Step 1: Update the SP4b row + scoreboard in `project_faster_than_sqlite_program.md`**

Open `~/.claude/projects/-home-fabio-dev-projects-thunderdb/memory/project_faster_than_sqlite_program.md` and:
- Mark SP4b status as ✅ merged with the date.
- Refresh the "Current scoreboard" line to include the new query2 suite (Win/Tie/Loss/Failure) so the next session starts with accurate state.
- Update the "How to apply" sentence to point at the next default sub-project (SP5 — large-scale stress).
- Add a brief deliverables paragraph for SP4b mirroring the SP4a one (new bench binary, new API, fixture reused, baseline file, scenario count, headline ratio improvements).

(No commit — this is a memory-system file, not a tracked repo artifact.)

---

## Self-review notes (already applied)

- **Spec coverage:** every spec section maps to a task — API + types (Tasks 1–3), correctness tests (Task 4), bench skeleton (Task 5), eleven scenarios (Tasks 6–10), committed baseline (Task 11), all five spec closures (Task 12 sub-sections), regression sweep (Task 13), CHANGES (Task 14), memory note (Task 15).
- **Placeholder scan:** the only deliberately-unfilled content is the ratio table in Task 14 step 1 (filled from the final baseline run) and the closure-list bullet (filled from the actual outcome of Task 12). Task 12 sub-sections are intentionally a menu, not a fixed plan, because they are conditional on the Task 11 baseline. Closures 2 and 3 are folded into a single combined dispatch in Closure 3's commit.
- **Type consistency:** `Aggregate`, `AggRow`, and the two trait methods use the same names everywhere they appear (Tasks 1, 2, 3, 4, 6–10, 12). The `Value::Int64` / `Value::Float64` / `Value::varchar` / `Value::Null` constructors mirror SP4a usage. Filter shape `Filter::new(<col>, <Operator>)` mirrors the existing read/write/query suites. The `for_each_row` callback type matches the trait declaration (`F: FnMut(&[Value])`).
- **Closure-2/3 dedup:** Closure 2's fallback dispatch is a strict subset of Closure 3's combined dispatch, so the plan explicitly notes "subsumes Closure 2" in Closure 3's first step to avoid double-implementation.
