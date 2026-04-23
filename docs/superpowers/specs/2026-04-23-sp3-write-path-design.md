# SP3 — Write-path benchmarks vs SQLite

**Date:** 2026-04-23
**Program:** "Faster than SQLite on every benchmark" (sub-project 3 of 7)
**Status:** Design approved, pending implementation plan
**Predecessors:** SP1 (harness), SP2 (full-scan closure + COLD fairness)

## Goal

Extend the head-to-head benchmark suite to cover write-path operations: INSERT, UPDATE, DELETE, and transaction-boundary variants. At the end of SP3, Thunder must Win or Tie every write scenario at SMALL/FAST/WARM. COLD and DURABLE results reported but not gated.

## Scope

**In scope:**
- INSERT, UPDATE, DELETE scenarios with varied commit cadence (per-row, single txn, batched)
- Index-maintenance variants
- Mixed-mutation scenario within a single transaction
- Filesystem-level snapshot/restore primitive for deterministic baselines across samples
- FAST durability measurements (Thunder default, SQLite synchronous=OFF / journal WAL + no fsync)
- Baseline promotion for new scenarios at SMALL/FAST/WARM
- COLD results for information only
- DURABLE cells declared as `Unsupported` until SP6

**Out of scope:**
- New batched write APIs on `Database` (add only if benchmarks expose an API-shaped gap; deferred to follow-up)
- DURABLE fsync-on-commit benchmarks (SP6)
- Concurrent writer benchmarks (SP7)
- MEDIUM / LARGE tier write measurements (SP5)
- COLD-mode write-path optimization (future SP2b)

## Acceptance Criteria

1. All 9 write scenarios run at SMALL/FAST/WARM and produce a verdict of `Win` or `Tie` (ratio ≤ 1.05).
2. Hard assertion `report.summary.loss == 0` at SMALL/FAST/WARM in `vs_sqlite_write`, matching SP2 rigor.
3. COLD cells execute, report verdicts, but are excluded from the loss gate.
4. DURABLE cells declared `Unsupported`; SP6 will flip this.
5. `perf/baseline.json` updated to include W1–W9 at SMALL/FAST/WARM.
6. `CHANGES.md` has an `SP3` entry summarizing ratios and any honest COLD regressions found.
7. Project memory "Faster than SQLite program status" scoreboard updated; SP3 marked merged.
8. `harness_selftest.rs` includes a snapshot-restore roundtrip test.

## Scenario Matrix

All scenarios at SMALL tier (baseline 10k rows where applicable), FAST durability, WARM+COLD cache.

| ID  | Operation                              | Commit cadence | Baseline       | Thunder assert              | SQLite oracle |
|-----|----------------------------------------|----------------|----------------|-----------------------------|---------------|
| W1  | INSERT 10k rows                        | per-row commit | empty          | row_count == 10_000         | same          |
| W2  | INSERT 10k rows                        | single txn     | empty          | row_count == 10_000         | same          |
| W3  | INSERT 10k rows                        | batch 1 000    | empty          | row_count == 10_000         | same          |
| W4  | INSERT 10k rows with secondary index   | single txn     | empty + index  | row_count == 10_000         | same          |
| W5  | UPDATE 10k rows by primary key         | single txn     | 10k baseline   | all rows mutated            | same          |
| W6  | UPDATE by indexed column (range)       | single txn     | 10k + index    | N rows mutated              | same          |
| W7  | DELETE 10k rows by primary key         | single txn     | 10k baseline   | row_count == 0              | same          |
| W8  | DELETE by range predicate              | single txn     | 10k baseline   | row_count == expected       | same          |
| W9  | Mixed INSERT + UPDATE + DELETE         | single txn     | 10k baseline   | row_count + checksum match  | same          |

Row schema mirrors the existing blog-fixture table used by the read suite (see SP1 fixtures) to reuse column definitions and enable comparable reads if needed later.

## Architecture

### New test file: `tests/perf/vs_sqlite_write.rs`

Mirrors `tests/perf/vs_sqlite_read.rs` structurally:
- Entry function `vs_sqlite_write` registers scenarios via `ScenarioBuilder` against a `Harness`.
- Runs under `cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`.
- Respects all existing env vars: `THUNDERDB_TIER`, `THUNDERDB_DURABILITY`, `THUNDERDB_CACHE`, `THUNDERDB_UPDATE_BASELINE`, `THUNDERDB_QUICK`.
- Enforces `report.summary.loss == 0` only over the FAST/WARM projection of the report.

### New primitive: `Snapshot` in `tests/perf/common/fixtures.rs`

```rust
pub struct Snapshot {
    snapshot_dir: PathBuf,   // temp dir holding pristine copies
    engine: Engine,          // Thunder or SQLite (file set differs)
}

impl Fixtures {
    pub fn snapshot(&self, engine: Engine, live_path: &Path) -> Snapshot;
}

impl Snapshot {
    pub fn restore(&self, live_path: &Path) -> Result<()>;
}
```

Semantics:
- `snapshot` closes/flushes the live DB, then byte-copies all engine files to a temp directory owned by the `Snapshot`:
  - Thunder: every file in the Thunder data directory.
  - SQLite: main `.db`, `-wal`, `-shm` if present.
- `restore` deletes the live files and copies the snapshot back. Called in scenario `pre_sample`.
- `Drop` cleans the temp dir.
- Behavior on partial copy failure: propagate `io::Error`; harness converts to `Failure`.

### Scenario lifecycle for writes

Extension of the existing `ScenarioBuilder` hooks — no new hook types needed:

```
setup (once per scenario per engine):
    build baseline DB (or empty) → snapshot = fixtures.snapshot(engine, live_path)
pre_sample (N times):
    snapshot.restore(live_path)
    reopen handle honoring current cache-state (COLD fadvise path already exists)
measure:
    execute write op
    assert post-condition vs oracle
```

`setup` runs exactly once even across WARM/COLD variants because the snapshot is immutable. `pre_sample` is the per-sample reset point.

### Harness changes

Minimal. A single `pre_sample` hook already exists for `ScenarioBuilder`; confirm during plan writing and extend only if required. No changes to `Verdict`, `Baseline`, `Report`.

## Data Flow

```
┌──────────────┐   setup once   ┌─────────────┐
│ build DB     │───────────────▶│ Snapshot    │
│ (baseline or │                │ (temp dir)  │
│  empty)      │                └──────┬──────┘
└──────────────┘                       │
                                       │ per sample
                 ┌─────────────────────▼──────────────────────┐
                 │ restore → reopen (COLD fadvise if needed)   │
                 └─────────────────────┬──────────────────────┘
                                       │
                                       ▼
                         ┌──────────────────────┐
                         │ measure: write op    │
                         │ timer wraps only op  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ post-op assert       │
                         │ row count / checksum │
                         └──────────────────────┘
```

Timer wraps only the measured write operation, not setup, snapshot, restore, reopen, or post-assert — same discipline as the read suite.

## Error Handling

- `Snapshot::restore` I/O failure → propagated; harness records `Failure`.
- Write operation panic → `Failure` (existing catch_unwind path in `Harness`).
- Post-op assertion mismatch → `Failure` via the harness's assert-mismatch reporting.
- Oracle divergence (Thunder result ≠ SQLite result) → `Failure`.
- Missing COLD support for an engine → `Unsupported`, not `Failure`.

No silent fallbacks. Any snapshot corruption fails loudly.

## Testing

### `tests/perf/harness_selftest.rs` additions

- `snapshot_restore_roundtrip`: build small DB, snapshot, mutate (INSERT N rows), restore, assert row count matches pre-mutation.
- `snapshot_restore_cold_path`: after restore, verify COLD reopen still fadvises companion files (regression guard for SP2 fix).
- `snapshot_drop_cleans_tempdir`: verify temp dir removed on `Drop`.

### Scenario-level correctness

Each write scenario asserts expected post-state vs an oracle. A Thunder scenario that writes the correct number of rows but the wrong content must still fail — use row-count + per-row checksum for W9 mixed case.

### Quick run

`THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write` reduces to 3 samples for development iteration.

## Deliverables

- `tests/perf/common/fixtures.rs` — `Snapshot` type, `Fixtures::snapshot` method.
- `tests/perf/vs_sqlite_write.rs` — 9 scenarios.
- `tests/perf/harness_selftest.rs` — 3 new tests.
- `perf/baseline.json` — promoted with W1–W9 at SMALL/FAST/WARM.
- `docs/superpowers/plans/2026-04-23-sp3-write-path.md` — implementation plan (next step).
- `CHANGES.md` — SP3 entry at top.
- Project memory update: scoreboard, SP3 → ✅ merged.

## Evidence Plan

Two runs recorded in CHANGES.md:
1. Pre-optimization baseline: all 9 scenarios against the current write path as-is. Honest ratios — any Loss indicates work to do before merge.
2. Post-optimization: if any Loss, fix in-scope code (no new APIs unless absolutely necessary — see Q6 in brainstorming) and re-run.

If a scenario Loss is caused by a missing API shape rather than implementation inefficiency, defer the API work to a follow-up SP and document in CHANGES.md.

## Risks

- **Snapshot cost dominates runtime.** Large baseline copies may slow iteration. Mitigation: `THUNDERDB_QUICK=1` for dev; snapshot lives on same filesystem so copy is a reflink/hardlink candidate if perf becomes painful (optimization deferred).
- **Baseline promotion leaks partial runs.** Only promote when full SMALL/FAST/WARM run is Win-or-Tie clean; existing `THUNDERDB_UPDATE_BASELINE=1` gate applies.
- **Thunder write API gaps.** If the existing API forces Thunder into a per-row path where SQLite bulk-inserts natively, W2/W3/W4 may show losses not fixable within SP3. Escalate as a new SP rather than inflating scope.

## Open Questions

None blocking. API-shape decisions deferred to evidence (Q6 = C).
