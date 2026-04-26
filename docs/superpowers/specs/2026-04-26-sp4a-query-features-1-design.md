# SP4a — Query features I (design)

Date: 2026-04-26
Program: "Faster than SQLite" (sub-project 4a of 8)
Status: design approved, plan pending
Branch: `sp4a-query-features-1`

## Goal

Add benchmark coverage for five SQL query shapes that already exist in ThunderDB but are not measured against SQLite, then close any cheap perf gaps surfaced by the new baseline. Shapes: `ORDER BY`, `IS NULL`, multi-column `AND` filter, `OFFSET`, string equality.

Acceptance: bench binary green under default soft loss gate, committed baseline at SMALL/FAST/WARM, all quick-win perf patches landed where they apply, scoreboard updated in `CHANGES.md`.

## Non-goals

- `GROUP BY`, aggregates, `DISTINCT` — deferred to SP4b.
- Multi-column composite indexes — out of scope.
- Cost-based planner / statistics — out of scope.
- MEDIUM / LARGE tier coverage — SP5.
- COLD-state optimization — SP2b.
- Concurrent reads — SP7.

## Context

`src/parser/ast.rs` and `src/parser/parser.rs` already model `ORDER BY`, `OFFSET`, and `IsNull`. `src/query/builder.rs` exposes `order_by`, `order_by_asc`, `order_by_desc`, `offset`. `src/parser/executor.rs` wires SQL → builder for all five features. SP1 read suite (`vs_sqlite_read.rs`) does not exercise them. SP4a fills the measurement gap and follows up with cheap fixes.

Per SP3 gotcha, baseline files are per-binary. SP4a uses `perf/baseline-query.json`. Read and write suites untouched.

## Architecture

New bench binary `tests/perf/vs_sqlite_query.rs`. Sibling of `vs_sqlite_read.rs` and `vs_sqlite_write.rs`. Reuses the SP1 harness in `tests/perf/common/` (`Scenario`, `Runner`, `Verdict`, `Fairness`, `CacheState`, `Report`, `Baseline`).

Loss-gate policy mirrors SP3: soft warn by default, strict via `SP4A_STRICT_LOSS_GATE=1`. The hard `report.summary.loss == 0` assertion stays out for now; SP4a is allowed to land with honest losses pending the quick-win pass.

## Fixture

New `blog_posts_q` builder lives in `tests/perf/common/fixtures.rs` (or a sibling module if file size justifies a split). 10 000 rows. Same shape on Thunder and SQLite. Schema:

| Column        | Type          | Indexed | Notes                                      |
|---------------|---------------|---------|--------------------------------------------|
| `id`          | `i64` PK      | yes     | dense `1..=10_000`                         |
| `author_id`   | `i64`         | yes     | ~50 distinct, dense distribution            |
| `title`       | `varchar`     | yes     | near-unique                                |
| `slug`        | `varchar`     | yes     | unique, lowercase ascii, deterministic     |
| `body`        | `varchar`     | no      | longer text, ~200 bytes                    |
| `category`    | `varchar?`    | no      | nullable; ~10% NULL; 5 distinct non-NULL    |
| `published_at`| `i64?`        | no      | nullable; ~30% NULL                         |
| `views`       | `i64`         | no      | for ORDER BY non-indexed                    |

Determinism: seeded RNG, fixed seed. Fixture writers go through the same `Database::insert_batch` path as the SP3 fixtures.

## Scenarios

Nine scenarios. Each implements the `Scenario` trait. `reset` is a no-op (no fixture mutation between samples).

| #  | Name                              | SQL shape                                                                       |
|----|-----------------------------------|---------------------------------------------------------------------------------|
| Q1 | ORDER BY indexed ASC + LIMIT      | `SELECT * FROM blog_posts_q ORDER BY id LIMIT 100`                              |
| Q2 | ORDER BY indexed DESC + LIMIT     | `SELECT * FROM blog_posts_q ORDER BY id DESC LIMIT 100`                         |
| Q3 | ORDER BY non-indexed full sort    | `SELECT * FROM blog_posts_q ORDER BY views LIMIT 100`                           |
| Q4 | Top-K with WHERE + ORDER BY index | `SELECT * FROM blog_posts_q WHERE author_id = ? ORDER BY id DESC LIMIT 10`      |
| Q5 | OFFSET deep skip                  | `SELECT * FROM blog_posts_q LIMIT 50 OFFSET 9000`                               |
| Q6 | IS NULL filter                    | `SELECT * FROM blog_posts_q WHERE category IS NULL`                             |
| Q7 | String EQ indexed                 | `SELECT * FROM blog_posts_q WHERE slug = ?`                                     |
| Q8 | String EQ non-indexed             | `SELECT * FROM blog_posts_q WHERE body = ?` (point-ish, parameter chosen so the value matches a known row) |
| Q9 | Multi-filter AND, mixed           | `SELECT * FROM blog_posts_q WHERE author_id = ? AND category = ? AND published_at IS NOT NULL` |

Tier / cache / durability matrix: SMALL × FAST × WARM committed. COLD runs but is informational only and not gated. MEDIUM/LARGE are SP5's job.

## Quick-win perf fixes (in-scope, opportunistic)

After the honest baseline lands, attempt the following only where the corresponding scenario shows a Loss. None of these are mandatory; each one drops out of scope if it grows beyond a focused patch.

1. **ORDER BY indexed pushdown.** When `order_by` column matches an existing index and there is no other ORDER BY constraint, walk the B-tree leaf chain forward (ASC) or reverse (DESC) instead of full scan + sort. Top-K (Q1, Q2, Q4) becomes O(K) reads instead of O(N) reads + O(N log N) sort. Implementation lives in `src/query/direct.rs` and/or `src/parser/executor.rs`. Reverse traversal needs `find_last_leaf` + back-pointer walk on the B-tree leaf chain; if missing, add it.
2. **OFFSET pushdown.** Verify the scan loop applies a skip counter before materializing rows. If the current path materializes all then drops, push the counter into the scan iterator. Closes Q5 if Loss.
3. **IS NULL fast path.** Full scan with a null check on the requested column, no row decode beyond the null bitmap. If the column is indexed, consider a NULL sentinel key in the B-tree; otherwise scan-only. Closes Q6 if Loss.
4. **String EQ indexed dispatch.** Confirm `WHERE slug = '...'` hits the index lookup path, not a full scan. If lookup dispatch ignores string indexes, fix it. Closes Q7 if Loss.

Q3 (ORDER BY non-indexed) and Q9 (multi-filter AND with non-indexed members) are expected to remain Loss until a planner exists; those are deferred without further work.

## Deliverables

- `tests/perf/vs_sqlite_query.rs` with the nine scenarios listed above.
- Fixture builder for `blog_posts_q` reachable from the bench binary.
- `perf/baseline-query.json` committed (SMALL/FAST/WARM).
- Selftest coverage for any new harness-shared helpers (none expected; reset is no-op).
- Quick-win perf patches where they land cleanly per the rules above.
- `CHANGES.md` entry dated 2026-04-26 with the SMALL/FAST/WARM ratio table and the per-scenario verdict.
- Implementation plan at `docs/superpowers/plans/2026-04-26-sp4a-query-features-1.md`.

## Risks

- **Reverse leaf traversal may not exist** in the current B-tree. If `find_last_leaf` and prev-pointer chains are absent, adding them is non-trivial. Fallback: forward traversal + reverse the K-element buffer for DESC, still O(K) net for top-K but full scan if no LIMIT. Acceptable for SP4a.
- **String index lookup gap** (Q7). If the index path treats string keys as a full-scan probe, the fix may touch `IndexManager` lookup dispatch. If the patch grows past a focused change, defer Q7 quick-win to a follow-up.
- **Fixture growth.** Adding a new fixture next to `blog_posts` doubles per-binary setup time. Acceptable: setup is outside the timed loop and harness samples are bounded.

## Open questions

None at design time.

## References

- SP1 spec: `docs/superpowers/specs/2026-04-16-benchmark-harness-design.md`
- SP3 spec: `docs/superpowers/specs/2026-04-23-sp3-write-path-design.md`
- SP3b spec: `docs/superpowers/specs/2026-04-24-sp3b-write-path-optimization-design.md`
- Harness: `tests/perf/common/`
- Read suite: `tests/perf/vs_sqlite_read.rs`
- Write suite: `tests/perf/vs_sqlite_write.rs`
