# SP4b — Query features II (design)

Date: 2026-04-26
Program: "Faster than SQLite" (sub-project 4b of 8)
Status: design approved, plan pending
Branch: `sp4b-query-features-ii`

## Goal

Add benchmark coverage for the SQL query shapes deferred from SP4a — `GROUP BY`, scalar aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), and `DISTINCT` — and land native ThunderDB APIs to compute them, with opportunistic closures wherever the new baseline shows a Loss.

Acceptance: bench binary green under default soft loss gate, committed baseline at SMALL/FAST/WARM, all closures listed below either landed or explicitly deferred with rationale, scoreboard updated in `CHANGES.md`.

## Non-goals

- Multi-column composite indexes — out of scope.
- Cost-based planner / statistics — out of scope.
- Multi-key GROUP BY indexed pushdown — single-key only.
- HAVING clause — out of scope (no scenarios use it).
- MEDIUM / LARGE tier coverage — SP5.
- COLD-state optimization — SP2b.
- Concurrent reads — SP7.
- Backwards compatibility for the new public types — not required (per program memo).

## Context

ThunderDB currently exposes only `count(table, filters)` as an aggregate via `DirectDataAccess`. There is no public API for `SUM`/`AVG`/`MIN`/`MAX`, no `GROUP BY`, and no `DISTINCT`. Bench scenarios on these shapes must therefore either compute the result in user code over `scan_with_limit` (slow, unfair) or use new native APIs (this SP). SP4a left this gap intentionally and pinned the work here.

Per SP3 gotcha, baseline files are per-binary. SP4b uses `perf/baseline-query2.json`. SP4a's `perf/baseline-query.json`, the read suite, and the write suite are untouched.

The `blog_posts_q` fixture from SP4a (10 000 rows, 50 distinct `author_id`, 5 distinct `category` + ~10% NULL, indexed `id`/`author_id`/`title`/`slug`, non-indexed `views`/`category`/`published_at`/`body`) is reused unchanged. No new fixture required.

## Architecture

New bench binary `tests/perf/vs_sqlite_query2.rs`. Sibling of `vs_sqlite_query.rs`. Reuses the SP1 harness in `tests/perf/common/` (`Scenario`, `Runner`, `Verdict`, `Fairness`, `CacheState`, `Report`, `Baseline`).

Loss-gate policy mirrors SP4a: soft warn by default, strict via `SP4B_STRICT_LOSS_GATE=1`. The hard correctness gate (`report.summary.failure == 0`) is always on.

New API surface lives in `src/query/direct.rs` (trait additions) and is implemented for `Database` in `src/lib.rs`. Internal hash-grouping aggregator is a private module under `src/query/` (e.g., `src/query/aggregate.rs`). Indexed fast paths dispatch in the entry function before the scan loop.

## API surface

```rust
// src/query/direct.rs (re-exported from crate root)

/// Aggregate function over a column (or, for Count, over rows).
#[derive(Debug, Clone)]
pub enum Aggregate {
    Count,                  // COUNT(*) — no column reference
    CountCol(String),       // COUNT(col) — non-NULL only
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

/// Result row from `aggregate()`. `keys` is empty for global aggregates.
#[derive(Debug, Clone)]
pub struct AggRow {
    pub keys: Vec<Value>,
    pub aggs: Vec<Value>,   // parallel to the `aggs` argument, in order
}

pub trait DirectDataAccess {
    // ...existing methods unchanged...

    /// GROUP BY `group_by`, computing `aggs`, with optional WHERE `filters`.
    /// Empty `group_by` = global aggregate; returns exactly one row.
    fn aggregate(
        &mut self,
        table: &str,
        group_by: Vec<String>,
        aggs: Vec<Aggregate>,
        filters: Vec<Filter>,
    ) -> Result<Vec<AggRow>>;

    /// SELECT DISTINCT cols. Single-column queries still return Vec<Vec<Value>>
    /// for shape uniformity.
    fn distinct(
        &mut self,
        table: &str,
        cols: Vec<String>,
        filters: Vec<Filter>,
    ) -> Result<Vec<Vec<Value>>>;
}
```

### Semantics (SQLite-matched)

- `Sum`, `Avg`, `Min`, `Max` skip NULLs.
- Empty input (after filters):
  - `Count` → `Int64(0)`
  - `CountCol` → `Int64(0)`
  - `Sum` → `Null`     (matches SQLite, **not** `Int64(0)`)
  - `Avg` → `Null`
  - `Min`/`Max` → `Null`
- Type rules:
  - `Sum(INT64 col)` → `Int64`. Overflow not handled — caller's responsibility (matches SQLite behavior of integer overflow on SUM, which itself is implementation-defined).
  - `Avg(INT64 col)` → `Float64`.
  - `Min(col)`/`Max(col)` → typed-as-input.
  - `Count`/`CountCol` → `Int64`.
- Group-key ordering: implementation-defined (HashMap iteration order). Bench `.assert` callbacks compare result sets as multisets (sort both sides by key bytes, or compare on counts only).
- `cols` in `distinct()` may have arity ≥ 1; single-col is still wrapped in `Vec<Vec<Value>>` for uniformity.
- `Aggregate` and `AggRow` are re-exported from the crate root.

### Internals

Default path: forward scan via existing scan iterator + filter application + `HashMap<Vec<Value>, AggState>` accumulator. `AggState` is a parallel `Vec<AggSlot>` matching the input `aggs` order, where each `AggSlot` is one of `{ count, sum_i64, sum_f64+n, min, max }`.

Indexed fast paths dispatch at the top of `aggregate()` and `distinct()` before falling through to the scan path. Each fast path is keyed on `(group_by, aggs, filters)` shape and the indexedness of the involved columns.

## Scenarios

Eleven scenarios on `blog_posts_q` (10 000 rows). `reset` is a no-op (no fixture mutation between samples).

| #   | Name                              | SQL shape                                                                       |
|-----|-----------------------------------|---------------------------------------------------------------------------------|
| Q10 | COUNT(*) full table               | `SELECT COUNT(*) FROM blog_posts_q`                                             |
| Q11 | COUNT(*) WHERE indexed            | `SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7`                         |
| Q12 | SUM int non-indexed               | `SELECT SUM(views) FROM blog_posts_q`                                           |
| Q13 | AVG int non-indexed               | `SELECT AVG(views) FROM blog_posts_q`                                           |
| Q14 | MIN/MAX indexed                   | `SELECT MIN(id), MAX(id) FROM blog_posts_q`                                     |
| Q15 | MIN/MAX non-indexed               | `SELECT MIN(views), MAX(views) FROM blog_posts_q`                               |
| Q16 | GROUP BY indexed low-card         | `SELECT author_id, COUNT(*) FROM blog_posts_q GROUP BY author_id` (50 groups)   |
| Q17 | GROUP BY non-indexed low-card     | `SELECT category, COUNT(*) FROM blog_posts_q GROUP BY category` (5 + NULL)      |
| Q18 | GROUP BY + SUM                    | `SELECT author_id, SUM(views) FROM blog_posts_q GROUP BY author_id`             |
| Q19 | DISTINCT high-card indexed        | `SELECT DISTINCT slug FROM blog_posts_q` (10 000 distinct)                      |
| Q20 | DISTINCT low-card non-indexed     | `SELECT DISTINCT category FROM blog_posts_q`                                    |

Tier / cache / durability matrix: SMALL × FAST × WARM committed. COLD runs but is informational only and not gated. MEDIUM/LARGE are SP5's job.

## Closures (in-scope, opportunistic)

After the honest baseline lands, attempt the following only where the corresponding scenario shows a Loss. None of these are mandatory; each one drops out of scope if it grows beyond a focused patch.

1. **MIN/MAX over indexed col → O(1).** Reuse `BTree::scan_first_k(1)` (MIN) and `BTree::scan_last_k(1)` (MAX) added in SP4a. Wire into the `aggregate()` dispatch when `group_by.is_empty()`, every agg is `Min(c)`/`Max(c)` on an indexed `c`, and there are no filters. Closes Q14.
2. **COUNT(\*) full → O(1) via cached row count.** Add `TableEngine::row_count` (atomic / `u64` field), increment in `insert`/`insert_batch`, decrement in `delete`, persist alongside table metadata so reopens don't require a recount. Dispatch when `group_by.is_empty()`, single `Count` agg, no filters. Closes Q10. **Fallback if persistence is invasive:** drop to "fast scan-only" path that walks pages without decoding rows. Still a Win-shaped closure.
3. **COUNT(\*) WHERE indexed → O(K) probe.** Existing `count()` already supports filtered count; verify `aggregate()` dispatches to it (or to the same code path) for `group_by.is_empty()`, single `Count`, single equality filter on an indexed column. Closes Q11.
4. **DISTINCT over single indexed col → leaf-walk unique keys.** New `BTree::scan_distinct_keys() -> impl Iterator<Item = Value>` that walks the leaf chain once and skips duplicate consecutive keys. Wire into `distinct()` dispatch when `cols.len() == 1`, the column is indexed, and `filters.is_empty()`. Closes Q19.
5. **GROUP BY single indexed col → ordered chunk-aggregate.** Walk the index in key order, emitting an `AggRow` each time the key changes. Avoids the hashmap allocation entirely. Dispatch when `group_by.len() == 1`, group key is indexed, and `filters.is_empty()`. Closes Q16/Q18 if Loss.

Q12, Q13, Q15, Q17, Q20 are expected to remain Loss-candidates after this SP — they are full-scan-bound and the gap is dominated by ThunderDB's row-decoder cost (same hotspot called out in SP4a follow-up). Documented as known.

## Deliverables

- `tests/perf/vs_sqlite_query2.rs` with the eleven scenarios listed above.
- `Aggregate`, `AggRow` types in `src/query/direct.rs`, re-exported from the crate root.
- `aggregate()` and `distinct()` methods on the `DirectDataAccess` trait, implemented for `Database`.
- Internal hash-grouping aggregator (e.g., `src/query/aggregate.rs`).
- Indexed fast-path dispatch in the entry functions for the closures listed above.
- New `BTree::scan_distinct_keys()` iterator if the Q19 closure lands.
- Optional `TableEngine::row_count` cache + persistence if the Q10 full closure lands; otherwise the fast scan-only fallback.
- `perf/baseline-query2.json` committed (SMALL/FAST/WARM).
- Selftest coverage for any new harness-shared helpers (none expected; reset is no-op).
- `CHANGES.md` entry dated 2026-04-26 with the SMALL/FAST/WARM ratio table, per-scenario verdict, and updated combined scoreboard.
- Implementation plan at `docs/superpowers/plans/2026-04-26-sp4b-query-features-2.md`.

## Risks

- **Cached row-count invalidation.** Adding `TableEngine::row_count` requires touching every insert/delete path and persisting the counter so reopens stay correct. A miss here = wrong `COUNT(*)`. Mitigation: keep the fallback "fast scan-only" path on the table; if the persistence work grows beyond a focused patch, ship the fallback and defer the true O(1) path to a follow-up.
- **NULL semantics divergence.** SQLite's `SUM` of empty/all-NULL is `NULL`, not `0`; `AVG` is `NULL`. The spec pins ThunderDB to SQLite-match behavior. Mitigation: bench `.assert` callbacks compare directly against SQLite per scenario, so a divergence fails the run.
- **Group ordering non-determinism.** Hash-based grouping returns rows in arbitrary order. Bench `.assert` callbacks must compare as multisets (e.g., by sorting both sides by key bytes or by comparing aggregated counts). Spec calls this out explicitly so plan-level test code is unsurprised.
- **Public API churn.** New `Aggregate`, `AggRow`, and trait additions are breaking changes for any external user. Acceptable per program memo ("no backward compatibility required").
- **B-tree distinct-key iteration.** SP4a added `scan_first_k`/`scan_last_k`. A general `scan_distinct_keys` iterator over the leaf chain is new shape. If extracting it cleanly grows beyond a focused patch, defer the Q19 closure to a follow-up and leave Q19 as Loss.

## Open questions

None at design time.

## References

- SP1 spec: `docs/superpowers/specs/2026-04-16-benchmark-harness-design.md`
- SP3 spec: `docs/superpowers/specs/2026-04-23-sp3-write-path-design.md`
- SP3b spec: `docs/superpowers/specs/2026-04-24-sp3b-write-path-optimization-design.md`
- SP4a spec: `docs/superpowers/specs/2026-04-26-sp4a-query-features-1-design.md`
- Harness: `tests/perf/common/`
- Read suite: `tests/perf/vs_sqlite_read.rs`
- Write suite: `tests/perf/vs_sqlite_write.rs`
- Query suite I (SP4a): `tests/perf/vs_sqlite_query.rs`
