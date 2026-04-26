# SP4a — Query features I — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land nine new ThunderDB-vs-SQLite benchmark scenarios that cover ORDER BY, IS NULL, multi-filter AND, OFFSET, and string equality, plus opportunistic quick-win perf patches where the new baseline shows a Loss.

**Architecture:** New `tests/perf/vs_sqlite_query.rs` bench binary, new `blog_posts_q` fixture in `tests/perf/common/fixtures.rs`, new committed baseline `perf/baseline-query.json`. Soft FAST/WARM loss gate by default, strict via `SP4A_STRICT_LOSS_GATE=1`. Perf patches scoped to `src/query/direct.rs`, `src/parser/executor.rs`, B-tree leaf-traversal helpers if needed.

**Tech Stack:** Rust 2021, ThunderDB workspace crate, `rusqlite` for the SQLite reference, existing SP1 harness in `tests/perf/common/`.

**Branch:** `sp4a-query-features-1` (already created from `master`).

**Spec:** `docs/superpowers/specs/2026-04-26-sp4a-query-features-1-design.md`

---

## File Structure

| Path | Role | Disposition |
|---|---|---|
| `tests/perf/common/fixtures.rs` | Add `build_blog_posts_q_fixtures` next to `build_blog_fixtures` and `build_empty_fixtures`. Adds NULL-bearing schema + extra indexed string column. | Modify |
| `tests/perf/vs_sqlite_query.rs` | New bench binary; nine scenarios; soft loss gate. | Create |
| `Cargo.toml` | Register the new `[[test]]` entry next to `vs_sqlite_read` / `vs_sqlite_write`. | Modify |
| `perf/baseline-query.json` | Committed baseline for SMALL/FAST/WARM. | Create |
| `src/query/direct.rs` | Optional ORDER BY indexed pushdown helper (Task 13 only). | Modify (conditional) |
| `src/parser/executor.rs` | Optional wiring for ORDER BY pushdown when index matches order column (Task 13 only). | Modify (conditional) |
| `src/index/btree.rs` (or wherever the B-tree lives) | Optional `find_last_leaf` + leaf-chain reverse traversal (Task 13 only). | Modify (conditional) |
| `CHANGES.md` | SP4a entry with ratio table. | Modify |

The bench binary owns scenarios + the `vs_sqlite_query()` test. The fixture builder owns dataset + schema. No new harness primitives are introduced — `Scenario`, `Harness`, `Fixtures::snapshot_all/restore_all`, the `reset` hook, and the `Baseline` writer all already exist from SP1/SP3.

---

## Task 0: Verify branch and clean tree

**Files:** _none_ — sanity check only.

- [ ] **Step 1: Confirm branch and clean working tree**

Run: `git status && git rev-parse --abbrev-ref HEAD`
Expected: branch `sp4a-query-features-1`; only `.claude/` untracked from earlier sessions; no other modifications.

If on the wrong branch, run `git checkout sp4a-query-features-1`.

---

## Task 1: Fixture builder for `blog_posts_q`

**Files:**
- Modify: `tests/perf/common/fixtures.rs` (append a new public function `build_blog_posts_q_fixtures`)

The new fixture mirrors `build_blog_fixtures` but writes a single richer table. Schema and counts come from the spec.

- [ ] **Step 1: Add the fixture builder**

Append to `tests/perf/common/fixtures.rs`:

```rust
/// Build a single-table dataset (`blog_posts_q`) tuned for SP4a query
/// scenarios: nullable `category` (~10% NULL), nullable `published_at`
/// (~30% NULL), indexed `slug`, non-indexed `body`, non-indexed `views`.
///
/// Both Thunder and SQLite are populated with identical row contents so
/// per-scenario `.assert` callbacks can compare row counts byte-for-byte.
///
/// Determinism: row `i` (1-based) chooses
/// - `author_id = (i % 50) + 1`
/// - `category = if i % 10 == 0 { NULL } else { CATEGORIES[i % 5] }`
/// - `published_at = if i % 10 < 3 { NULL } else { 1_700_000_000 + i as i64 }`
/// - `views = ((i * 2654435761) % 1_000_000) as i64`  (Knuth multiplicative hash)
/// - `slug = format!("post-{:08x}", i)`              (unique, lowercase ascii)
pub fn build_blog_posts_q_fixtures(tier: Tier, mode: Durability) -> Fixtures {
    use rusqlite::params;
    use thunderdb::{DirectDataAccess, Value};
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};

    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}_{}_{}_q_{}",
        std::process::id(), tier.label(), mode.label(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    );
    let base = std::env::temp_dir().join(format!("thunderdb_perf_{}", unique));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let thunder_dir = base.join("thunder");
    let sqlite_path = base.join("sqlite.db");

    const CATEGORIES: [&str; 5] = ["news", "review", "tutorial", "opinion", "guide"];
    let post_count = tier.post_count();

    // ── Thunder ──
    let mut tdb = Database::open(&thunder_dir).expect("open thunderdb");

    let posts: Vec<Vec<Value>> = (1..=post_count).map(|i| {
        let author_id = (i as i64 % 50) + 1;
        let category: Value = if i % 10 == 0 {
            Value::Null
        } else {
            Value::varchar(CATEGORIES[i % CATEGORIES.len()].to_string())
        };
        let published_at: Value = if i % 10 < 3 {
            Value::Null
        } else {
            Value::Int64(1_700_000_000 + i as i64)
        };
        let views = ((i as i64).wrapping_mul(2654435761)).rem_euclid(1_000_000);
        vec![
            Value::Int64(i as i64),
            Value::Int64(author_id),
            Value::varchar(format!("Post about topic #{}", i)),
            Value::varchar(format!("post-{:08x}", i)),
            Value::varchar(format!("This is the body of post {}.  Topic discussion follows for several sentences.", i)),
            category,
            published_at,
            Value::Int64(views),
        ]
    }).collect();

    tdb.insert_batch("blog_posts_q", posts).unwrap();
    {
        let tbl = tdb.get_table_mut("blog_posts_q").unwrap();
        tbl.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(),           data_type: "INT64".into() },
            ColumnInfo { name: "author_id".into(),    data_type: "INT64".into() },
            ColumnInfo { name: "title".into(),        data_type: "VARCHAR".into() },
            ColumnInfo { name: "slug".into(),         data_type: "VARCHAR".into() },
            ColumnInfo { name: "body".into(),         data_type: "VARCHAR".into() },
            ColumnInfo { name: "category".into(),     data_type: "VARCHAR".into() },
            ColumnInfo { name: "published_at".into(), data_type: "INT64".into() },
            ColumnInfo { name: "views".into(),        data_type: "INT64".into() },
        ]}).unwrap();
        tbl.create_index("id").unwrap();
        tbl.create_index("author_id").unwrap();
        tbl.create_index("title").unwrap();
        tbl.create_index("slug").unwrap();
    }

    // ── SQLite ──
    let sdb = Connection::open(&sqlite_path).unwrap();
    match mode {
        Durability::Fast => {
            sdb.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        }
        Durability::Durable => {
            sdb.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;").unwrap();
        }
    }
    sdb.execute_batch(
        "CREATE TABLE blog_posts_q (
            id INTEGER PRIMARY KEY,
            author_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            slug TEXT NOT NULL,
            body TEXT NOT NULL,
            category TEXT,
            published_at INTEGER,
            views INTEGER NOT NULL
         );
         CREATE INDEX idx_q_author ON blog_posts_q(author_id);
         CREATE INDEX idx_q_title  ON blog_posts_q(title);
         CREATE INDEX idx_q_slug   ON blog_posts_q(slug);"
    ).unwrap();

    {
        let tx = sdb.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare(
                "INSERT INTO blog_posts_q (id, author_id, title, slug, body, category, published_at, views)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)").unwrap();
            for i in 1..=post_count {
                let author_id = (i as i64 % 50) + 1;
                let category: Option<String> = if i % 10 == 0 {
                    None
                } else {
                    Some(CATEGORIES[i % CATEGORIES.len()].to_string())
                };
                let published_at: Option<i64> = if i % 10 < 3 {
                    None
                } else {
                    Some(1_700_000_000 + i as i64)
                };
                let views = ((i as i64).wrapping_mul(2654435761)).rem_euclid(1_000_000);
                st.execute(params![
                    i as i64, author_id,
                    format!("Post about topic #{}", i),
                    format!("post-{:08x}", i),
                    format!("This is the body of post {}.  Topic discussion follows for several sentences.", i),
                    category, published_at, views,
                ]).unwrap();
            }
        }
        tx.commit().unwrap();
    }

    make_fixtures(tier, mode, thunder_dir, sqlite_path, tdb, sdb)
}
```

Notes:
- Uses `Value::Int64` to match the SQLite `INTEGER` column width and avoid signed/unsigned mismatch on `id` over 10 000 rows.
- The constants `CATEGORIES` and the deterministic seed-free generator make the dataset reproducible without a PRNG dependency.

- [ ] **Step 2: Build (no test yet) to verify it compiles**

Run: `cargo build --tests`
Expected: clean build. Warning about an unused function is OK at this point.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/common/fixtures.rs
git commit -m "feat(perf): add blog_posts_q fixture for SP4a query benchmarks"
```

---

## Task 2: Fixture self-test

**Files:**
- Modify: `tests/perf/harness_selftest.rs` (append new test)

Add a smoke test that the fixture builds, has the expected row count, the expected NULL ratio, and the expected indices.

- [ ] **Step 1: Write the failing test**

Append to `tests/perf/harness_selftest.rs`:

```rust
#[test]
fn blog_posts_q_fixture_shape() {
    use thunderdb::{DirectDataAccess, Filter, Operator};
    use crate::common::fairness::{Tier, Durability};
    use crate::common::fixtures::{build_blog_posts_q_fixtures, drop_fixtures};

    let mut f = build_blog_posts_q_fixtures(Tier::Small, Durability::Fast);

    let total = f.thunder_mut().count("blog_posts_q", vec![]).unwrap() as usize;
    assert_eq!(total, Tier::Small.post_count(), "row count mismatch");

    let nulls = f.thunder_mut().count(
        "blog_posts_q",
        vec![Filter::new("category", Operator::IsNull)],
    ).unwrap();
    let expected_nulls = (Tier::Small.post_count() / 10) as u64;
    assert_eq!(nulls, expected_nulls,
        "category NULL count: got {}, want {}", nulls, expected_nulls);

    // SQLite must agree.
    let s_nulls: i64 = f.sqlite().query_row(
        "SELECT COUNT(*) FROM blog_posts_q WHERE category IS NULL", [], |r| r.get(0)).unwrap();
    assert_eq!(s_nulls as u64, nulls, "thunder/sqlite NULL count disagree");

    drop_fixtures(f);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test --test harness_selftest --release blog_posts_q_fixture_shape -- --nocapture`
Expected: PASS. If it fails on the NULL count, re-check the `i % 10 == 0` rule in Task 1 against `Tier::Small::post_count()`.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/harness_selftest.rs
git commit -m "test(perf): smoke test for blog_posts_q fixture"
```

---

## Task 3: Skeleton bench binary

**Files:**
- Create: `tests/perf/vs_sqlite_query.rs`
- Modify: `Cargo.toml` (register the new `[[test]]`)

Empty scenario list at first; just enough wiring to build, run, and produce an empty report against `perf/baseline-query.json`.

- [ ] **Step 1: Register the bench in Cargo.toml**

Add immediately after the `vs_sqlite_write` entry:

```toml
[[test]]
name = "vs_sqlite_query"
path = "tests/perf/vs_sqlite_query.rs"
```

- [ ] **Step 2: Create the bench file**

```rust
//! ThunderDB vs SQLite — query-features I scenarios (SP4a).
//! Covers ORDER BY, IS NULL, multi-filter AND, OFFSET, and string EQ.

mod common;

use common::*;
use thunderdb::{DirectDataAccess, Filter, Operator, Value};
use std::path::PathBuf;

fn scenarios() -> Vec<Scenario> {
    vec![
        // populated in Tasks 5..11
    ]
}

#[test]
fn vs_sqlite_query() {
    let h = Harness::from_env();
    let baseline_path = PathBuf::from("perf/baseline-query.json");
    let artifact_dir = PathBuf::from("target/perf");
    let report = h.run(&scenarios(), &baseline_path, &artifact_dir);

    // Hard correctness gate (always on).
    assert!(
        report.summary.failure == 0,
        "query scenarios have {} failure(s)", report.summary.failure
    );

    // Soft loss gate by default. Strict mode opted in via env var.
    if std::env::var("SP4A_STRICT_LOSS_GATE").as_deref() == Ok("1") {
        assert!(
            report.summary.loss == 0,
            "query scenarios have {} loss(es) (strict gate)", report.summary.loss
        );
    } else if report.summary.loss > 0 {
        eprintln!(
            "warn: {} loss(es) under soft loss gate; set SP4A_STRICT_LOSS_GATE=1 to fail",
            report.summary.loss
        );
    }
}
```

- [ ] **Step 3: Build and run with the empty scenario list**

Run: `cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: PASS, no scenarios reported, summary all zeroes. The harness writes an empty report to `target/perf/`. `perf/baseline-query.json` is not created yet (no scenarios to baseline).

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): scaffold vs_sqlite_query bench binary (SP4a)"
```

---

## Task 4: Helper closures shared by scenarios

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

ORDER BY scenarios need to sort + paginate after `scan_with_limit`, mirroring what `Executor::select_to_query` + `QueryPlan::apply_ordering` would do. To avoid copy-paste, add small private helpers in the bench file.

- [ ] **Step 1: Add helpers above `scenarios()`**

```rust
use thunderdb::storage::Row;

/// Sort `rows` by the integer column at index `col_idx`. Stable sort, NULLs first.
fn sort_rows_by_int(mut rows: Vec<Row>, col_idx: usize, desc: bool) -> Vec<Row> {
    rows.sort_by(|a, b| {
        let av = a.values.get(col_idx);
        let bv = b.values.get(col_idx);
        let ord = match (av, bv) {
            (Some(Value::Null), Some(Value::Null)) => std::cmp::Ordering::Equal,
            (Some(Value::Null), _) => std::cmp::Ordering::Less,
            (_, Some(Value::Null)) => std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        };
        if desc { ord.reverse() } else { ord }
    });
    rows
}

/// Take the first `n` rows after a sort.
fn take_n(mut rows: Vec<Row>, n: usize) -> Vec<Row> {
    rows.truncate(n);
    rows
}
```

- [ ] **Step 2: Build**

Run: `cargo build --tests`
Expected: clean build.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): add sort + take helpers for SP4a scenarios"
```

---

## Task 5: Q1 + Q2 — ORDER BY indexed ASC/DESC + LIMIT

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q1: `SELECT * FROM blog_posts_q ORDER BY id LIMIT 100`
Q2: `SELECT * FROM blog_posts_q ORDER BY id DESC LIMIT 100`

Both Thunder closures perform a full scan + sort + take, exactly the way the existing executor pipeline would. SQLite uses native SQL.

- [ ] **Step 1: Add Q1 + Q2 to the `scenarios()` vec**

Replace the empty body:

```rust
fn scenarios() -> Vec<Scenario> {
    vec![
        // Q1. ORDER BY indexed ASC + LIMIT 100
        Scenario::new("Q1. ORDER BY indexed ASC + LIMIT 100", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let _ = take_n(sort_rows_by_int(rows, 0, false), 100);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY id LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 0, false), 100).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY id LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q1 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q2. ORDER BY indexed DESC + LIMIT 100
        Scenario::new("Q2. ORDER BY indexed DESC + LIMIT 100", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let _ = take_n(sort_rows_by_int(rows, 0, true), 100);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY id DESC LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 0, true), 100).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY id DESC LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q2 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
    ]
}
```

- [ ] **Step 2: Run quick mode and confirm scenarios appear, no Failure**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 2 scenarios printed in the report; `summary.failure == 0`. Loss is allowed under the soft gate.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q1+Q2 ORDER BY indexed ASC/DESC + LIMIT"
```

---

## Task 6: Q3 — ORDER BY non-indexed full sort

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q3: `SELECT * FROM blog_posts_q ORDER BY views LIMIT 100` — `views` has no index, so this is the honest full-scan + full-sort path.

- [ ] **Step 1: Append Q3 to `scenarios()`**

Add inside the `vec![ ... ]` after Q2:

```rust
        // Q3. ORDER BY non-indexed (views) full sort + LIMIT 100
        Scenario::new("Q3. ORDER BY non-indexed full sort", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                // views is column index 7 (0-based) per the schema in Task 1.
                let _ = take_n(sort_rows_by_int(rows, 7, false), 100);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY views LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 7, false), 100).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY views LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q3 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 3 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q3 ORDER BY non-indexed full sort"
```

---

## Task 7: Q4 — Top-K with WHERE + ORDER BY index

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q4: `SELECT * FROM blog_posts_q WHERE author_id = 7 ORDER BY id DESC LIMIT 10`

`author_id` is indexed; `id` is indexed. Thunder filters via index, sorts the 200-ish hits, takes 10.

- [ ] **Step 1: Append Q4**

```rust
        // Q4. WHERE author_id = ? ORDER BY id DESC LIMIT 10  (top-K with index)
        Scenario::new("Q4. Top-K via ORDER BY indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))],
                    None, None).unwrap();
                let _ = take_n(sort_rows_by_int(rows, 0, true), 10);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE author_id = 7 ORDER BY id DESC LIMIT 10").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))],
                    None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 0, true), 10).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q WHERE author_id = 7 ORDER BY id DESC LIMIT 10)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q4 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 4 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q4 top-K with WHERE + ORDER BY index"
```

---

## Task 8: Q5 — OFFSET deep skip

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q5: `SELECT * FROM blog_posts_q LIMIT 50 OFFSET 9000` — `scan_with_limit` already supports the `offset` argument and pushes it into the scan loop; Thunder closure passes it through.

- [ ] **Step 1: Append Q5**

```rust
        // Q5. LIMIT 50 OFFSET 9000  (deep skip)
        Scenario::new("Q5. OFFSET deep skip", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], Some(50), Some(9000)).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q LIMIT 50 OFFSET 9000").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], Some(50), Some(9000)).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q LIMIT 50 OFFSET 9000)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q5 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 5 scenarios, no failure.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q5 OFFSET deep skip"
```

---

## Task 9: Q6 — IS NULL filter

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q6: `SELECT * FROM blog_posts_q WHERE category IS NULL` — uses `Operator::IsNull` (already in `src/query/filter.rs`).

- [ ] **Step 1: Append Q6**

```rust
        // Q6. WHERE category IS NULL
        Scenario::new("Q6. IS NULL filter", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("category", Operator::IsNull)],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE category IS NULL").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("category", Operator::IsNull)]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE category IS NULL",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q6 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 6 scenarios. If Q6 emits a Failure with mismatched counts, recheck the NULL frequency in Task 1 (Tier::Small post_count must be a multiple of 10 for the assertion above to be exact — if it isn't, leave the assertion as `>= 0` and tighten it after confirming counts).

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q6 IS NULL filter"
```

---

## Task 10: Q7 + Q8 — String EQ indexed and non-indexed

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q7: `WHERE slug = 'post-00001234'` (slug is indexed).
Q8: `WHERE body = '<full body string for row 1234>'` (body is not indexed).

- [ ] **Step 1: Add a body-string helper next to `take_n`**

```rust
fn body_for(i: i64) -> String {
    format!("This is the body of post {}.  Topic discussion follows for several sentences.", i)
}
fn slug_for(i: i64) -> String { format!("post-{:08x}", i) }
```

- [ ] **Step 2: Append Q7 + Q8**

```rust
        // Q7. WHERE slug = ? (indexed string EQ)
        Scenario::new("Q7. string EQ indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("slug", Operator::Equals(Value::varchar(slug_for(1234))))],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let target = slug_for(1234);
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE slug = ?1").unwrap();
                let _: Vec<i64> = st.query_map([&target], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("slug", Operator::Equals(Value::varchar(slug_for(1234))))]).unwrap();
                if t != 1 { Err(format!("Q7 row count: thunder={}, want 1", t)) } else { Ok(()) }
            })
            .build(),

        // Q8. WHERE body = ? (non-indexed string EQ)
        Scenario::new("Q8. string EQ non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("body", Operator::Equals(Value::varchar(body_for(1234))))],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let target = body_for(1234);
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE body = ?1").unwrap();
                let _: Vec<i64> = st.query_map([&target], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("body", Operator::Equals(Value::varchar(body_for(1234))))]).unwrap();
                if t != 1 { Err(format!("Q8 row count: thunder={}, want 1", t)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 3: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 8 scenarios, no failure.

- [ ] **Step 4: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q7+Q8 string EQ indexed and non-indexed"
```

---

## Task 11: Q9 — Multi-filter AND, mixed indexed/non-indexed

**Files:**
- Modify: `tests/perf/vs_sqlite_query.rs`

Q9: `WHERE author_id = 7 AND category = 'review' AND published_at IS NOT NULL`.

- [ ] **Step 1: Append Q9**

```rust
        // Q9. multi-filter AND mixed
        Scenario::new("Q9. multi-filter AND mixed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![
                        Filter::new("author_id",    Operator::Equals(Value::Int64(7))),
                        Filter::new("category",     Operator::Equals(Value::varchar("review".to_string()))),
                        Filter::new("published_at", Operator::IsNotNull),
                    ],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q
                     WHERE author_id = 7 AND category = 'review' AND published_at IS NOT NULL").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![
                        Filter::new("author_id",    Operator::Equals(Value::Int64(7))),
                        Filter::new("category",     Operator::Equals(Value::varchar("review".to_string()))),
                        Filter::new("published_at", Operator::IsNotNull),
                    ]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q
                     WHERE author_id = 7 AND category = 'review' AND published_at IS NOT NULL",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q9 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Full run (no QUICK, baseline-quality)**

Run: `cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: 9 scenarios, `failure == 0`. Some Loss verdicts are expected and accepted under the soft gate; record the per-scenario ratios from the printed report for use in Task 12 and the CHANGES.md entry.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_query.rs
git commit -m "feat(perf): SP4a Q9 multi-filter AND mixed"
```

---

## Task 12: Promote baseline + commit

**Files:**
- Create: `perf/baseline-query.json`

- [ ] **Step 1: Promote the current run as the baseline**

Run: `THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: PASS, harness writes `perf/baseline-query.json`.

- [ ] **Step 2: Re-run without the env var to confirm the baseline reads back cleanly**

Run: `cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: PASS. Each scenario's ratio is now compared against the just-promoted baseline (which means the report's own ratio is ~1.00x for thunder-vs-baseline-thunder).

- [ ] **Step 3: Commit**

```bash
git add perf/baseline-query.json
git commit -m "perf(sp4a): promote baseline-query.json (SMALL/FAST/WARM)"
```

---

## Task 13: Quick-win perf pass (CONDITIONAL)

This task is a single umbrella; the work inside is **opt-in per-fix**. Only attempt a fix if its scenario shows a Loss in the baseline from Task 12. Stop on any fix that grows past a focused patch — defer the rest to a follow-up SP. Do NOT chase Q3 (non-indexed sort) or the non-indexed members of Q9: those are planner-shaped and are explicitly deferred per the spec.

**Files (any subset; only those needed):**
- Modify: `src/query/direct.rs`
- Modify: `src/parser/executor.rs`
- Modify: `src/index/btree.rs` (or whichever file holds the leaf chain)
- Modify: `tests/perf/vs_sqlite_query.rs` (only if the fix changes how the bench should call into Thunder — usually it does not; Thunder uses the same API and just runs faster)

For each fix you attempt, follow the loop below. The exact steps differ per fix; the loop does not.

- [ ] **Step 1 (per-fix): Add or update a unit test in the most relevant `#[cfg(test)] mod tests`**

Choose a focused unit test that captures the new behaviour. Examples:

  *(a) ORDER BY indexed pushdown (covers Q1, Q2, Q4)*
  ```rust
  #[test]
  fn order_by_indexed_does_not_full_scan() {
      // arrange a small table with `id` index
      // run scan with limit=10 + order_by=id
      // assert: number of rows visited == 10 (instrument via QueryContext counter)
  }
  ```

  *(b) OFFSET pushdown (covers Q5)*
  Already in place per `src/lib.rs:504-521`; if the bench shows a Loss anyway, the cost is in scan iteration, not OFFSET. Skip the fix.

  *(c) IS NULL fast path (covers Q6)*
  ```rust
  #[test]
  fn is_null_filter_skips_decoding_non_null_rows() {
      // assert no `value_at_page_bytes` call beyond the null bitmap check
      // when filter is Operator::IsNull
  }
  ```

  *(d) String EQ indexed dispatch (covers Q7)*
  ```rust
  #[test]
  fn string_eq_uses_index_lookup() {
      // a table with an indexed string column
      // QueryContext::used_index should record "slug"
      // when filter is Equals(Value::varchar("..."))
  }
  ```

- [ ] **Step 2 (per-fix): Run the test and confirm it fails**

Run: `cargo test --release <test_name> -- --nocapture`
Expected: FAIL, with output that pins the bug.

- [ ] **Step 3 (per-fix): Implement the minimal change**

The implementation lives in one of the files listed above. Keep the patch surgical. If it grows past a single function or two, stop and defer.

- [ ] **Step 4 (per-fix): Re-run the unit test**

Run: `cargo test --release <test_name> -- --nocapture`
Expected: PASS.

- [ ] **Step 5 (per-fix): Re-run the bench**

Run: `cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query`
Expected: the targeted scenario's verdict moves from Loss toward Tie or Win. `failure == 0` still holds. Other scenarios should not regress.

- [ ] **Step 6 (per-fix): Re-promote the baseline and commit**

```bash
THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_query --release -- --nocapture vs_sqlite_query
git add src/<changed-files> tests/<changed-tests> perf/baseline-query.json
git commit -m "perf(query): <one-line description of the fix>"
```

If no fix was attempted (or none landed), this task collapses to a no-op and the next task starts.

---

## Task 14: Run full ThunderDB test suite

**Files:** _none_ — regression check.

- [ ] **Step 1: Run the full test suite**

Run: `cargo test --release`
Expected: PASS. Pay special attention to `vs_sqlite_read` and `vs_sqlite_write` — they MUST NOT regress.

If `vs_sqlite_read` or `vs_sqlite_write` lose scenarios that previously passed, revisit the most recent perf patch from Task 13. Either fix the regression or revert the patch. Do not gate read/write losses through the soft gate; their hard gate is intentional.

---

## Task 15: CHANGES.md entry

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Append the SP4a entry directly under the document title**

The new section goes immediately after the leading `# ThunderDB Changes` line and before the existing `## 2026-04-24 - SP3b: Write-path optimization` block, so entries stay in reverse-chronological order. Use the ratio table from the final Task 12 / Task 13 baseline run.

```markdown
## 2026-04-26 - SP4a: Query features I (ORDER BY, IS NULL, multi-filter, OFFSET, string EQ)

Fifth sub-project in the "faster than SQLite in all benchmarks" program. Adds query-shape coverage for features that already exist in ThunderDB but were not measured against SQLite, plus opportunistic perf fixes where the new baseline showed a Loss.

- **9 new query scenarios** in `tests/perf/vs_sqlite_query.rs`:
  - Q1 ORDER BY indexed ASC + LIMIT 100
  - Q2 ORDER BY indexed DESC + LIMIT 100
  - Q3 ORDER BY non-indexed full sort
  - Q4 Top-K via WHERE + ORDER BY indexed
  - Q5 OFFSET deep skip (LIMIT 50 OFFSET 9000)
  - Q6 IS NULL filter
  - Q7 string EQ indexed (`slug`)
  - Q8 string EQ non-indexed (`body`)
  - Q9 multi-filter AND mixed (`author_id = ? AND category = ? AND published_at IS NOT NULL`)
- **`blog_posts_q` fixture** added to `tests/perf/common/fixtures.rs`. Single 10 000-row table with INT64 PK, indexed `author_id`/`title`/`slug`, non-indexed `body`/`views`, nullable `category` (~10% NULL) and `published_at` (~30% NULL).
- **Soft FAST/WARM loss gate by default**, strict via `SP4A_STRICT_LOSS_GATE=1`. Mirrors the SP3 policy.
- **Quick-win perf fixes** (only those that landed within scope; entries are filled in based on the actual outcome of Task 13):
  - <fill in: ORDER BY indexed pushdown / IS NULL fast path / string EQ indexed dispatch / etc.>
- **FAST/WARM ratios (SMALL tier, 11 samples)** — fill in from the final baseline:

  | Scenario | Thunder | SQLite | Ratio | Verdict |
  |---|---|---|---|---|
  | Q1. ORDER BY indexed ASC + LIMIT 100 | … | … | …x | … |
  | Q2. ORDER BY indexed DESC + LIMIT 100 | … | … | …x | … |
  | Q3. ORDER BY non-indexed full sort | … | … | …x | … |
  | Q4. Top-K via ORDER BY indexed | … | … | …x | … |
  | Q5. OFFSET deep skip | … | … | …x | … |
  | Q6. IS NULL filter | … | … | …x | … |
  | Q7. string EQ indexed | … | … | …x | … |
  | Q8. string EQ non-indexed | … | … | …x | … |
  | Q9. multi-filter AND mixed | … | … | …x | … |

- **Separate baseline file**: query scenarios are committed to `perf/baseline-query.json`; read suite stays on `perf/baseline.json`, write suite on `perf/baseline-write.json`. Same per-binary baseline pattern from SP3.

Spec: `docs/superpowers/specs/2026-04-26-sp4a-query-features-1-design.md`
Plan: `docs/superpowers/plans/2026-04-26-sp4a-query-features-1.md`
```

When filling in the ratio table, copy numbers exactly from the harness's printed report after the final Task 12 (or Task 13) baseline run.

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs(sp4a): CHANGES entry with ratio table"
```

---

## Task 16: Update the program-tracker memory note

**Files:** _none_ — update the user's auto-memory file directly.

- [ ] **Step 1: Update the SP4a row + scoreboard in `project_faster_than_sqlite_program.md`**

Open `~/.claude/projects/-home-fabio-dev-projects-thunderdb/memory/project_faster_than_sqlite_program.md` and:
- Mark SP4a status as ✅ merged with the date.
- Refresh the "Current scoreboard" line(s) to include the new query suite (Win/Tie/Loss/Failure) so the next session starts with accurate state.
- Update the "How to apply" sentence to point at SP4b (or SP3b follow-up) as the next default.

(No commit — this is a memory-system file, not a tracked repo artifact.)

---

## Self-review notes (already applied)

- **Spec coverage:** every spec section maps to a task — fixture (Task 1), bench skeleton (Task 3), nine scenarios (Tasks 5–11), committed baseline (Task 12), quick-win perf pass (Task 13), regression sweep (Task 14), CHANGES (Task 15).
- **Placeholder scan:** the only deliberately-unfilled content is the ratio table in Task 15 step 1, which is meant to be filled from the final baseline run; the surrounding text marks this clearly. The Task 13 sub-fix list is intentionally a menu, not a fixed plan, because it is conditional on the Task 12 baseline.
- **Type consistency:** `Value::Int64` everywhere for INT columns; `Value::varchar` everywhere for TEXT; `Filter::new(<col>, <Operator>)` consistent with the existing read/write suites; column index `7` for `views` matches the schema declared in Task 1 (`id, author_id, title, slug, body, category, published_at, views`).
