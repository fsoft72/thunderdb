# Benchmark Harness & Fairness Protocol — Design

**Date:** 2026-04-16
**Sub-project:** 1 of 7 in the "Thunder becomes faster than SQLite in all benchmarks" program.
**Status:** Design approved; ready for implementation plan.

---

## 1. Context

### 1.1 Parent program

Goal: **ThunderDB is faster than (or tied with) SQLite in every benchmark we measure, across every fairness dimension we care about.** Fairness dimensions:

1. Scale tier — small, medium, large datasets.
2. Durability mode — fast defaults and strict fsync.
3. Cache state — cold start and warm steady-state.
4. Feature breadth — write path, read path, aggregates, concurrency.

Current state (2026-04-16): the existing `tests/integration/thunderdb_vs_sqlite_bench.rs` covers 11 read-oriented scenarios at a single tier / fast-durability / warm-cache cell. Thunder wins 8, loses 3 (Setup 1.13×, IN 1.13×, Full scan 1.93×).

### 1.2 Sub-project decomposition

| SP  | Name                                 | Summary                                                          |
|-----|--------------------------------------|------------------------------------------------------------------|
| 1   | Benchmark harness & fairness protocol (**this doc**) | Foundation: runner, fixtures, scoreboard, baseline diff. |
| 2   | Current-suite closure                | Fix the 3 known read-path losses using the new harness.          |
| 3   | Write-path benchmarks                | Single INSERT, UPDATE, DELETE, UPDATE-of-indexed.                |
| 4a  | Query features I                     | ORDER BY / LIMIT / OFFSET, IS NULL, multi-filter WHERE, string EQ. |
| 4b  | Query features II                    | GROUP BY + aggregates (COUNT/SUM/AVG/MIN/MAX), DISTINCT.          |
| 5   | Large-scale stress                   | Re-run everything at MEDIUM/LARGE tiers; fix algorithmic issues. |
| 6   | Durability-matched fairness          | Thunder fsync-on-commit; compete against SQLite FULL.             |
| 7   | Concurrent reads                     | Multi-reader API design + benchmarks.                             |

Each sub-project is 3 days to 2 weeks. SP1 is the gate: no meaningful measurement of SP2–SP7 is possible without it.

### 1.3 Constraints

- **No backward compatibility constraint on Thunder internals.** On-disk formats, APIs, index layouts, and TOAST encoding may all change freely in SP2–SP7. The harness treats Thunder as a black box via its public `Database` API.
- **Project philosophy** (from `CLAUDE.md`): incremental progress, composition over inheritance, explicit over implicit, test-driven where possible.
- **Tradeoff envelope** (per brainstorming Q2): SIMD, parallelism, new crates all acceptable in downstream sub-projects. SP1 itself introduces no new runtime dependencies.

---

## 2. Scope of this sub-project

### 2.1 In scope

- Build a shared harness library under `tests/perf/common/`.
- Migrate the existing `thunderdb_vs_sqlite_bench.rs` onto the new harness as `tests/perf/vs_sqlite_read.rs`.
- Support 3 × 2 × 2 measurement matrix: tier (SMALL/MEDIUM/LARGE) × durability (FAST/DURABLE) × cache (WARM/COLD).
- Terminal scoreboard, JSON artifact per run, baseline-diff against a committed `baseline.json`.
- Band-based verdicts (Win / Tie / Loss / Unsupported / Failure) with Tie acceptable toward parent goal.
- Self-tests proving the harness itself works.

### 2.2 Explicitly out of scope for SP1

- Any Thunder performance optimization (that's SP2 onward).
- Any new query features in Thunder (that's SP4).
- Multi-reader Thunder API (SP7).
- Thunder `fsync_on_commit` durability mode (SP6). SP1 defines the `DURABLE` cell shape; scenarios in it return `Unsupported` until SP6.

### 2.3 Success criteria

- `cargo test --test vs_sqlite_read` runs on the new harness and reproduces the current 8-Win-3-Loss scoreboard at `SMALL/FAST/WARM`.
- All 12 cells (3 tiers × 2 durabilities × 2 caches) can be requested via env vars; `DURABLE` cells show Thunder results as `Unsupported`.
- Harness self-tests (`tests/perf/harness_selftest.rs`) pass.
- JSON artifact is written to `target/perf/<ISO-timestamp>.json` with schema version 1.
- `cargo test --test vs_sqlite_read -- --update-baseline` promotes a run to `baseline.json` and the next run shows a `vs Base` column.

---

## 3. Architecture

### 3.1 Module layout

```
thunderdb/
├── benches/                          (existing, unchanged)
├── tests/
│   ├── perf/
│   │   ├── common/                   (NEW — the harness library)
│   │   │   ├── mod.rs                Public API re-exports
│   │   │   ├── scenario.rs           Scenario struct + registration
│   │   │   ├── runner.rs             Driver loop: warmup, samples, catch_unwind
│   │   │   ├── fairness.rs           Tier/Durability/CacheState enums + env-var parsing
│   │   │   ├── fixtures.rs           build_blog_fixtures, seeded generators
│   │   │   ├── cache.rs              posix_fadvise wiring for COLD cache
│   │   │   ├── verdict.rs            classify_ratio, aggregation
│   │   │   ├── report.rs             Terminal table + JSON writer
│   │   │   └── baseline.rs           Load/save baseline.json, diff logic
│   │   ├── vs_sqlite_read.rs         (migrated from integration/)
│   │   └── harness_selftest.rs       (NEW — harness self-tests)
│   └── integration/
│       └── thunderdb_vs_sqlite_bench.rs  (DELETED at end of SP1)
└── target/perf/                      (gitignored; runtime only)
    ├── <ISO-timestamp>.json
    └── baseline.json                 (committed to repo)
```

`tests/perf/common/` is included as `mod common;` from each test file. This is the standard Rust integration-test shared-code pattern. No new crate.

### 3.2 Cargo.toml changes

Add to `[[test]]` list:

```toml
[[test]]
name = "vs_sqlite_read"
path = "tests/perf/vs_sqlite_read.rs"

[[test]]
name = "harness_selftest"
path = "tests/perf/harness_selftest.rs"
```

Remove the old `thunderdb_vs_sqlite_bench` entry at end of SP1.

Add `libc` as a unix-only dev-dep for `posix_fadvise`:

```toml
[target.'cfg(unix)'.dev-dependencies]
libc = "0.2"
```

No other new dependencies. `rusqlite` and `memmap2` are already present. On non-unix platforms the `cache.rs` module compiles a no-op `posix_fadvise_dontneed` stub.

### 3.3 Public API surface

```rust
// tests/perf/common/mod.rs

pub use scenario::{Scenario, ScenarioBuilder};
pub use fairness::{Tier, Durability, CacheState, HarnessConfig};
pub use fixtures::{Fixtures, build_blog_fixtures, drop_fixtures};
pub use runner::Harness;
pub use verdict::Verdict;
pub use report::BenchResult;
```

---

## 4. Core abstractions

### 4.1 Scenario

A `Scenario` describes a single benchmark end-to-end:

```rust
pub struct Scenario {
    pub name: &'static str,
    pub group: &'static str,
    pub setup: Box<dyn Fn(Tier, Durability) -> Fixtures + Send + Sync>,
    pub thunder: Box<dyn Fn(&mut Fixtures) + Send + Sync + UnwindSafe>,
    pub sqlite:  Box<dyn Fn(&Fixtures) + Send + Sync + UnwindSafe>,
    pub assert:  Box<dyn Fn(&Fixtures) -> Result<(), String> + Send + Sync>,
}
```

Registration is via a `ScenarioBuilder` to keep call sites readable:

```rust
Scenario::new("IN (1, 3) on author_id", "read")
    .setup(blog_fixture)
    .thunder(|f| { let _ = f.thunder_mut().count("blog_posts",
        vec![Filter::new("author_id", Operator::In(vec![Value::Int32(1), Value::Int32(3)]))]
    ).unwrap(); })
    .sqlite(|f| { let _: i64 = f.sqlite().query_row(
        "SELECT COUNT(*) FROM blog_posts WHERE author_id IN (1, 3)", [], |r| r.get(0)
    ).unwrap(); })
    .assert(|f| { /* both engines agree on count */ Ok(()) })
    .build()
```

### 4.2 Harness driver

```rust
pub struct Harness { config: HarnessConfig }

impl Harness {
    pub fn from_env() -> Self;                    // reads THUNDERDB_TIER/DURABILITY/CACHE
    pub fn run_scenarios(&self, s: &[Scenario]) -> HarnessReport;
    pub fn run_parallel_scenarios(&self, s: &[Scenario]) -> HarnessReport; // SP7 placeholder
}
```

`run_scenarios` loops:

```
for each (tier, durability, cache) in requested cells:
    for each scenario:
        fixtures = scenario.setup(tier, durability)
        warmup(&fixtures, scenario)                         # 3 iterations, results discarded
        if cache == COLD:
            for each of 11 samples:
                reopen + fadvise(DONTNEED)
                time(scenario.thunder(&mut fixtures))
                time(scenario.sqlite(&fixtures))
        else:  # WARM
            for each of 11 samples:
                time(scenario.thunder(&mut fixtures))
                time(scenario.sqlite(&fixtures))
        correctness = scenario.assert(&fixtures)
        record_result(...)
        drop_fixtures(fixtures)
```

Each timed block is wrapped in `std::panic::catch_unwind`; panics record as `Verdict::Failure`, keep the run going.

### 4.3 HarnessReport

```rust
pub struct HarnessReport {
    pub cells: Vec<CellReport>,
    pub git_sha: String,
    pub started_at: String,
}

pub struct CellReport {
    pub tier: Tier, pub mode: Durability, pub cache: CacheState,
    pub results: Vec<BenchResult>,
}

impl HarnessReport {
    pub fn to_terminal(&self) -> String;
    pub fn to_json(&self) -> String;
    pub fn write_to(&self, dir: &Path) -> io::Result<PathBuf>;
    pub fn exit_with_verdict(self) -> !;      // exits process 0/1 per aggregate verdict
}
```

---

## 5. Fairness matrix

### 5.1 Scale tier (3 values)

| Tier     | Posts     | Comments (avg 3/post) | Users | Env-var value |
|----------|----------:|----------------------:|------:|---------------|
| SMALL    | 10 000    | ~30 000               | 5     | `small` (default) |
| MEDIUM   | 100 000   | ~300 000              | 5     | `medium`      |
| LARGE    | 1 000 000 | ~3 000 000            | 5     | `large`       |

`THUNDERDB_TIER=all` runs all three sequentially. Users stay at 5 intentionally — joins exercise many-to-one patterns, not user-scale work.

Dataset generator:

```rust
fn generate_posts(tier: Tier) -> Vec<Vec<Value>> {
    let count = tier.post_count();
    (1..=count)
        .map(|i| {
            let author_id = (i % USER_COUNT) + 1;
            let topic = TOPICS[i % TOPICS.len()];
            vec![
                Value::Int32(i as i32),
                Value::Int32(author_id as i32),
                Value::varchar(format!("Post about {} #{}", topic, i)),
                Value::varchar(format!("This is post {} discussing {}...", i, topic)),
            ]
        })
        .collect()
}
```

Content is derived from the row index — no RNG needed for the base blog dataset. `FIXTURE_SEED` (see §7.2) is reserved for future fixtures that need randomness. Changing either the generation rule or `FIXTURE_SEED` requires re-baselining.

### 5.2 Durability mode (2 values)

| Mode    | SQLite pragmas                                | Thunder config                   |
|---------|-----------------------------------------------|----------------------------------|
| FAST    | `journal_mode=WAL` + `synchronous=NORMAL`     | mmap, `fsync_on_write=false`     |
| DURABLE | `journal_mode=DELETE` + `synchronous=FULL`    | `fsync_on_commit=true` (SP6)     |

Env-var: `THUNDERDB_DURABILITY=fast|durable|both`, default `fast`.

**SP6 dependency:** until SP6 lands, every scenario in the DURABLE cell records `Verdict::Unsupported` for Thunder. The harness does not set `fsync_on_commit` (the config field doesn't exist yet); it records the SQLite timing, marks the Thunder side unsupported, and moves on. CI passes.

### 5.3 Cache state (2 values)

| State | Preparation                                                                                |
|-------|--------------------------------------------------------------------------------------------|
| WARM  | After setup + warmup; same handle reused across samples. Current semantics.                 |
| COLD  | Between samples: close handles, `posix_fadvise(DONTNEED)` on data files, reopen handles.    |

Env-var: `THUNDERDB_CACHE=warm|cold|both`, default `warm`.

**COLD implementation:**

```rust
fn cold_prep(f: &mut Fixtures) -> io::Result<()> {
    drop(f.thunder.take());        // close handle
    drop(f.sqlite.take());
    // Thunder stores multiple files under thunder_dir; walk the dir
    for path in collect_data_files(&f.thunder_dir) {
        posix_fadvise_dontneed(&path)?;
    }
    posix_fadvise_dontneed(&f.sqlite_path)?;
    f.thunder = Some(Database::open(&f.thunder_dir)?);
    f.sqlite = Some(Connection::open(&f.sqlite_path)?);
    Ok(())
}
```

`posix_fadvise(POSIX_FADV_DONTNEED)` is best-effort and doesn't require root. On Linux it typically succeeds for pages not currently dirty. COLD measurements are therefore "cache-evicted as best we can" — documented as such in the spec, not claimed as "truly cold." This is still strictly more honest than WARM-only.

On non-Linux platforms (Windows, mac) the call is a no-op and COLD degrades to "reopen-only." Documented in the harness comment.

### 5.4 Execution budget

| Cell configuration             | Approx. runtime for 11 read benches |
|--------------------------------|------------------------------------:|
| SMALL × FAST × WARM (default)  | ~3 s                                |
| SMALL × both × both            | ~30 s                               |
| MEDIUM × both × both           | ~5 min                              |
| LARGE × both × both            | ~30 min                             |

After SP3–SP7 land, scenarios count grows to ~40. Runtime scales linearly. Budgets remain within reason for local (SMALL default), PR (MEDIUM), and nightly (LARGE).

Escape hatch: `--quick` flag sets sample count to 3 (instead of 11). Cuts runtime by ~3×, widens confidence intervals. Intended for exploratory work only; not for regression detection.

---

## 6. Results, verdicts, and baseline

### 6.1 BenchResult

```rust
pub struct BenchResult {
    pub scenario: String,
    pub group:    String,
    pub tier:     Tier,
    pub mode:     Durability,
    pub cache:    CacheState,
    pub thunder:  EngineTiming,
    pub sqlite:   EngineTiming,
    pub ratio:    f64,
    pub verdict:  Verdict,
}

pub struct EngineTiming {
    pub median: Duration,
    pub p95:    Duration,
    pub sample_count: usize,
    pub dropped_outliers: usize,
}

pub enum Verdict { Win, Tie, Loss, Unsupported, Failure(String) }
```

### 6.2 Verdict rule

```rust
fn classify_ratio(ratio: f64) -> Verdict {
    if ratio < 0.95 { Verdict::Win }
    else if ratio <= 1.05 { Verdict::Tie }
    else { Verdict::Loss }
}
```

### 6.3 Sample reduction

11 samples per engine per scenario. Drop the min and max. Median of remaining 9. p95 is computed on the full 11 (no dropping). `dropped_outliers` field records how many samples deviated >5× from the median — advisory only.

### 6.4 Aggregate pass/fail

Harness exit code:

| Condition                               | Exit |
|-----------------------------------------|-----:|
| Any `Failure`                           | 1    |
| Any `Loss`                              | 1    |
| Only `Win` / `Tie` / `Unsupported`      | 0    |

`Unsupported` is explicitly distinct from `Loss`. CI does not fail because SP6 hasn't landed.

### 6.5 Terminal output

Per-cell table (one block per `(tier, mode, cache)` combination actually run):

```
=== vs SQLite: read (tier=SMALL, mode=FAST, cache=WARM) ===

 Scenario                             Thunder    SQLite    Ratio   vs Base   Verdict
 ------------------------------------ --------  --------  -------  -------   ----------
 1. COUNT(*) all three tables              1µs    182µs    0.01x    +0%     Win
 8. IN (1, 3) on author_id                93µs     82µs    1.13x    +0%     Loss
 ...
 Summary: 9 Win, 0 Tie, 2 Loss, 0 Unsupported
```

Final aggregate across all cells:

```
=== Aggregate across all cells ===
 read:     44 Win, 0 Tie,  4 Loss, 0 Unsupported
 OVERALL:  44 Win, 0 Tie,  4 Loss, 0 Unsupported   → FAIL
```

`vs Base` column rules:
- `< −5%` improvement (advisory).
- `[−5%, +5%]` neutral.
- `> +5%` regression (advisory; doesn't change Verdict).
- `new` — scenario not in baseline.
- `removed` — scenario in baseline but not in current run.

### 6.6 JSON artifact

Path: `target/perf/<ISO-timestamp>.json`.

```json
{
  "schema_version": 1,
  "git_sha": "55237db...",
  "rustc_version": "1.78.0",
  "started_at": "2026-04-16T14:22:00Z",
  "host": { "os": "linux", "cores": 8 },
  "cells": [
    {
      "tier": "SMALL", "mode": "FAST", "cache": "WARM",
      "results": [
        {
          "scenario": "IN (1, 3) on author_id", "group": "read",
          "thunder": { "median_ns": 93000, "p95_ns": 98000, "sample_count": 11, "dropped_outliers": 0 },
          "sqlite":  { "median_ns": 82000, "p95_ns": 87000, "sample_count": 11, "dropped_outliers": 0 },
          "ratio": 1.134,
          "verdict": "Loss"
        }
      ]
    }
  ],
  "summary": { "win": 9, "tie": 0, "loss": 2, "unsupported": 0, "failure": 0 }
}
```

`schema_version` is 1. Any breaking change bumps it; old baselines are not migrated — we re-baseline from the new run. Regression history is lost across schema bumps, which is acceptable because schema bumps are rare and the "vs Base" column is advisory, not a gate.

### 6.7 Baseline

File: `target/perf/baseline.json` (committed to repo).

Format is the same JSON schema as a run artifact. Comparison is per `(scenario, tier, mode, cache)` key. Entries missing from baseline print `new`; entries missing from current run print `removed`.

`cargo test --test vs_sqlite_read -- --update-baseline` promotes the current run to `baseline.json`. Implementation reads `std::env::args()` after `--` to detect the flag (cargo-test standard arg handling).

Partial runs (SIGINT, `Failure`) are written to `<timestamp>-partial.json` and `--update-baseline` refuses to promote them.

---

## 7. Fixture builders

### 7.1 Shared blog dataset

```rust
pub struct Fixtures {
    pub tier:         Tier,
    pub mode:         Durability,
    pub thunder_dir:  PathBuf,
    pub sqlite_path:  PathBuf,
    thunder:          Option<Database>,           // Option to support COLD reopen
    sqlite:           Option<Connection>,
}

impl Fixtures {
    pub fn thunder(&self) -> &Database { self.thunder.as_ref().expect("thunder handle closed") }
    pub fn thunder_mut(&mut self) -> &mut Database { self.thunder.as_mut().expect("thunder handle closed") }
    pub fn sqlite(&self) -> &Connection { self.sqlite.as_ref().expect("sqlite handle closed") }
}

pub fn build_blog_fixtures(tier: Tier, mode: Durability) -> Fixtures;
pub fn drop_fixtures(f: Fixtures);
```

The handle fields are private so callers go through the helpers. The harness is the only code that calls `thunder.take()` during COLD reopen.

The builder:
1. Creates tmp paths `/tmp/thunderdb_perf_<pid>_<tier>_<mode>_<rand>/`.
2. Opens Thunder with config matching `mode`.
3. Opens SQLite; applies pragmas matching `mode`.
4. Generates posts/comments deterministically (seeded RNG).
5. Inserts into both engines. Creates the same indices on both.
6. Returns `Fixtures` with both handles `Some(_)`.

### 7.2 Determinism

- All string data in the base fixture is derived from row index — no RNG in the base builder.
- Reserved seed for fixtures that do need randomness: `const FIXTURE_SEED: u64 = 0xD811_1DB5_EED5_5EED;`.
- Both engines insert in the same order. Row IDs match between engines by construction (both use sequential INTEGER PRIMARY KEY semantics).

### 7.3 Per-category extensions

- SP3 (write-path): adds `with_unindexed_target_row(fixture)` helper. No new dataset.
- SP4b (aggregates): adds `build_blog_with_tags_fixtures` — same blog + a many-to-many `tags` table.
- SP5 (stress): reuses `build_blog_fixtures` at `LARGE` tier; no new builder.
- SP7 (concurrent): fixture returns Thunder handle wrapped in whatever multi-reader type SP7 introduces.

Each extension is additive. Old fixtures don't change signature.

### 7.4 Assert closures

Every scenario ships an `assert` closure. It runs once per `(tier, mode, cache)` cell after samples are collected. Example:

```rust
.assert(|f| {
    let t = f.thunder().count("blog_posts", vec![...]).unwrap();
    let s: i64 = f.sqlite().query_row("SELECT COUNT(*) ...", [], |r| r.get(0)).unwrap();
    if t as i64 != s { Err(format!("Thunder={}, SQLite={}", t, s)) } else { Ok(()) }
})
```

Assert failures produce `Verdict::Failure` regardless of timing. A fast-but-wrong Thunder is **worse** than a slow-correct Thunder.

---

## 8. Error handling & failure modes

| Mode                      | Cause                                           | Handling                                                      |
|---------------------------|-------------------------------------------------|---------------------------------------------------------------|
| Scenario panic            | Closure panics mid-sample                       | `catch_unwind`; record `Verdict::Failure(msg)`; continue.     |
| Correctness mismatch      | `assert` closure returns `Err`                  | `Verdict::Failure`. Timing results discarded. Continue.       |
| Fixture build failure     | `build_blog_fixtures` errors or panics          | Abort current cell only. Remaining scenarios in cell skip.    |
| Unsupported               | Thunder path `unimplemented!()` for this cell   | `Verdict::Unsupported`. CI passes. Continue.                  |
| Timing outlier            | One sample >5× median of rest                   | Keep sample; increment `dropped_outliers` counter; continue.  |
| SIGINT                    | User interrupts                                 | Write `<timestamp>-partial.json`, exit 130.                   |

Catch boundary: each sample execution wrapped in `catch_unwind`. Closures marked `UnwindSafe`. Fixtures are passed `&mut` to Thunder closures; a panic poisons the fixture and the whole cell restarts its setup.

All harness-internal logging goes to stderr. The measured closure is silent (no logging inside the timed block — would distort measurements).

---

## 9. Self-tests

### 9.1 Unit tests in `common/`

- `verdict::tests` — boundary cases at 0.95, 1.0, 1.05; classification correctness.
- `baseline::tests` — missing file → empty; version mismatch → error; round-trip JSON.
- `runner::tests` — synthetic sleep-based closures produce expected verdicts under generous tolerance.
- `report::tests` — JSON round-trip equality; terminal output against golden string (rendered in `target/perf/golden/`).

### 9.2 `tests/perf/harness_selftest.rs`

Integration tests for the harness itself, using synthetic scenarios that don't touch Thunder or SQLite:

| Test                                  | Setup                                          | Expected outcome |
|---------------------------------------|------------------------------------------------|------------------|
| `both_engines_tie`                    | Both closures sleep 1 ms                       | Verdict::Tie     |
| `thunder_wins`                        | Thunder sleeps 1 ms; SQLite sleeps 10 ms       | Verdict::Win     |
| `thunder_loses`                       | Thunder sleeps 10 ms; SQLite sleeps 1 ms       | exit code 1 (expected; wrapped in `#[should_panic]` + runs harness in subprocess) |
| `assert_fails`                        | Closures are fine; assert returns Err          | Verdict::Failure |
| `thunder_panics`                      | Thunder closure `panic!()`s                    | Verdict::Failure, no process crash |
| `cold_cache_reopen`                   | Scenario opens a temp file; verifies handle was reopened between samples | Pass |
| `unsupported_scenario`                | Thunder closure is `unimplemented!()`          | Verdict::Unsupported |

### 9.3 Non-goals

- Not testing that measured timings match sleep durations precisely. Scheduler noise makes that flaky. Tolerance bands (≥5× gap for Win detection) absorb noise.
- Not testing Thunder correctness via the harness. That remains the existing integration test suite's job. Assert closures are last-line-of-defense, not primary coverage.

---

## 10. Open questions for the implementation plan

These are explicitly deferred to SP1's implementation plan, not answered in this spec:

- Exact `Scenario` builder ergonomics (macro vs. function chain).
- Whether `HarnessConfig::from_env` supports a dotenv file as fallback.
- Color output detection (`is-terminal` crate vs. hand-rolled).
- Rustc/git version collection: shell out to `rustc --version` and `git rev-parse HEAD`, or depend on `git2` / `rustc_version` crates. (Prefer shell-out — no new deps.)

The implementation plan (next deliverable after this spec is approved) resolves these.

---

## 11. Deliverables checklist

- [ ] `tests/perf/common/` module with all submodules from §3.1.
- [ ] Migrated `tests/perf/vs_sqlite_read.rs` reproducing current 11 scenarios.
- [ ] `tests/perf/harness_selftest.rs` passing all self-tests from §9.2.
- [ ] `target/perf/` directory created automatically; `baseline.json` committed.
- [ ] `Cargo.toml` updated with new `[[test]]` entries; old integration bench removed.
- [ ] `CHANGES.md` entry describing the harness.
- [ ] `cargo test --test vs_sqlite_read` passes (reproduces current scoreboard).
- [ ] `THUNDERDB_TIER=medium THUNDERDB_DURABILITY=both THUNDERDB_CACHE=both cargo test ...` runs the full matrix; DURABLE rows show `Unsupported`.
- [ ] `cargo test ... -- --update-baseline` promotes baseline and the next run shows a `vs Base` column.
