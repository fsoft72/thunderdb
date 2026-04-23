# SP3 — Write-path benchmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 9 write-path benchmark scenarios (INSERT/UPDATE/DELETE with varied commit cadence) to the ThunderDB vs SQLite head-to-head suite; all scenarios Win or Tie at SMALL/FAST/WARM with hard `loss == 0` gate, mirroring SP2 rigor.

**Architecture:** A new `tests/perf/vs_sqlite_write.rs` test binary registers scenarios through the existing `ScenarioBuilder`/`Harness`. Because write operations mutate the DB across samples, we add (1) a `Snapshot` filesystem primitive in `fixtures.rs` that byte-copies pristine DB files to a temp dir and restores before each sample, (2) an optional `reset` hook on `Scenario` that the runner invokes before every warmup iteration and every timed sample, outside the timer. Scenarios using the hook build their snapshots inside `setup` and call `fixtures.restore_snapshots()` in `reset`.

**Tech Stack:** Rust, existing ThunderDB `Database` + `DirectDataAccess` API, `rusqlite`, existing `tests/perf/common` harness library, `std::fs` for file copies, `posix_fadvise` via existing `common::cache` module for COLD fairness.

---

## Spec Reference

Spec: `docs/superpowers/specs/2026-04-23-sp3-write-path-design.md`. Read it before starting. Key constraints:
- DURABLE stays `Unsupported` (SP6 territory).
- COLD results reported but not gated.
- `loss == 0` enforced only on the FAST/WARM projection of the report.
- No speculative new APIs; only real Thunder `Database` calls.

## File Map

**Modify:**
- `tests/perf/common/fixtures.rs` — add `Engine` enum, `Snapshot` struct, `snapshot_all`/`restore_all` on `Fixtures`, new `build_empty_fixtures` helper.
- `tests/perf/common/scenario.rs` — add optional `reset` hook (`ResetFn`), default no-op.
- `tests/perf/common/runner.rs` — call `reset` before each warmup iteration and before each timed sample (outside timer) in both Thunder and SQLite sample loops.
- `tests/perf/common/mod.rs` — re-export new types as needed.
- `tests/perf/harness_selftest.rs` — add 3 snapshot tests and 1 reset-hook test.
- `perf/baseline.json` — promoted after green SMALL/FAST/WARM run.
- `CHANGES.md` — new SP3 entry at top.

**Create:**
- `tests/perf/vs_sqlite_write.rs` — 9 write scenarios + FAST/WARM `loss == 0` assertion.

**No changes to:**
- `tests/perf/common/{verdict,fairness,cache,report,baseline}.rs` — interfaces are stable.
- `tests/perf/vs_sqlite_read.rs` — uses no `reset` hook; default must be a no-op so this file still compiles unchanged.

---

## Pre-flight

### Task 0: Branch setup

**Files:** none

- [ ] **Step 1: Confirm clean working tree on master**

Run: `git status --porcelain`
Expected: empty output (only `.claude/` untracked is acceptable)

- [ ] **Step 2: Create SP3 feature branch**

Run: `git checkout -b sp3-write-path`
Expected: "Switched to a new branch 'sp3-write-path'"

- [ ] **Step 3: Verify current baseline is green**

Run: `cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read`
Expected: test passes, final scoreboard shows `loss == 0` at SMALL/FAST/WARM.

---

## Phase 1 — Snapshot primitive

### Task 1: `Engine` enum in fixtures.rs

**Files:**
- Modify: `tests/perf/common/fixtures.rs`

- [ ] **Step 1: Write failing unit test**

Add at the end of the `#[cfg(test)] mod tests` block in `tests/perf/common/fixtures.rs`:

```rust
#[test]
fn engine_enum_has_variants() {
    let e = Engine::Thunder;
    match e {
        Engine::Thunder => {}
        Engine::Sqlite => {}
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test harness_selftest --release engine_enum_has_variants 2>&1 | tail -20; cargo test --release --lib engine_enum 2>&1 | tail -5`
Note: the unit test lives inside the perf `common` module which is pulled in by each perf test binary; run via `cargo test --test vs_sqlite_read --release engine_enum_has_variants`.
Expected: FAIL with "cannot find type `Engine` in this scope".

- [ ] **Step 3: Add the enum**

Add near the top of `tests/perf/common/fixtures.rs` after the imports:

```rust
/// Which storage engine a [`Snapshot`] captures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Thunder,
    Sqlite,
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --test vs_sqlite_read --release engine_enum_has_variants -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/fixtures.rs
git commit -m "Add Engine enum in perf fixtures (SP3 Task 1)"
```

---

### Task 2: `Snapshot` struct + `Fixtures::snapshot_all` / `restore_all`

**Files:**
- Modify: `tests/perf/common/fixtures.rs`

- [ ] **Step 1: Write failing unit test**

Add at the end of the `#[cfg(test)] mod tests` block:

```rust
#[test]
fn snapshot_and_restore_roundtrip_blog_fixture() {
    use rusqlite::params;

    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
    f.snapshot_all().unwrap();

    // Mutate both engines after snapshot.
    f.thunder_mut().delete("blog_posts", vec![]).unwrap();
    let _ = f.sqlite().execute("DELETE FROM blog_posts", params![]).unwrap();

    // Restore.
    f.restore_all().unwrap();

    // Row counts match the pristine fixture.
    let t_posts = f.thunder_mut().count("blog_posts", vec![]).unwrap();
    let s_posts: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
    assert_eq!(t_posts, Tier::Small.post_count());
    assert_eq!(s_posts as usize, Tier::Small.post_count());

    drop_fixtures(f);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test vs_sqlite_read --release snapshot_and_restore_roundtrip_blog_fixture -- --nocapture`
Expected: FAIL — `snapshot_all` / `restore_all` not found.

- [ ] **Step 3: Add `Snapshot` struct + methods**

Add to `tests/perf/common/fixtures.rs` after the `Engine` enum:

```rust
use std::path::Path;

/// Filesystem snapshot of a single engine's on-disk state. Used to reset
/// mutated databases to a pristine baseline between write-benchmark samples.
/// The temp directory is cleaned on `Drop`.
pub struct Snapshot {
    engine: Engine,
    snapshot_dir: PathBuf,
    /// Live file/dir paths that were captured, relative to snapshot_dir.
    entries: Vec<PathBuf>,
}

impl Snapshot {
    /// Capture a pristine copy of the engine files at `live_path` into a new temp dir.
    ///
    /// Thunder: `live_path` is a directory; every file inside is copied recursively.
    /// SQLite: `live_path` is the main `.db` file; `-wal` and `-shm` companions are
    /// captured when present.
    pub fn capture(engine: Engine, live_path: &Path) -> std::io::Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let uniq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let snap_dir = std::env::temp_dir().join(format!("thunderdb_snap_{}_{}", std::process::id(), uniq));
        std::fs::create_dir_all(&snap_dir)?;
        let mut entries = Vec::new();
        match engine {
            Engine::Thunder => copy_dir_recursive(live_path, &snap_dir, Path::new(""), &mut entries)?,
            Engine::Sqlite => {
                copy_file_if_exists(live_path, &snap_dir.join(live_path.file_name().unwrap()), &mut entries, live_path.file_name().unwrap().into())?;
                for suffix in &["-wal", "-shm"] {
                    let mut live = live_path.as_os_str().to_os_string();
                    live.push(suffix);
                    let live = PathBuf::from(live);
                    let name = live.file_name().unwrap().to_os_string();
                    copy_file_if_exists(&live, &snap_dir.join(&name), &mut entries, name.into())?;
                }
            }
        }
        Ok(Snapshot { engine, snapshot_dir: snap_dir, entries })
    }

    /// Restore pristine files to `live_path`, replacing any current content.
    pub fn restore(&self, live_path: &Path) -> std::io::Result<()> {
        match self.engine {
            Engine::Thunder => {
                // Drop the whole live dir then recreate from snapshot.
                if live_path.exists() { std::fs::remove_dir_all(live_path)?; }
                std::fs::create_dir_all(live_path)?;
                for rel in &self.entries {
                    let src = self.snapshot_dir.join(rel);
                    let dst = live_path.join(rel);
                    if let Some(parent) = dst.parent() { std::fs::create_dir_all(parent)?; }
                    std::fs::copy(&src, &dst)?;
                }
            }
            Engine::Sqlite => {
                // Remove live + companions, then copy snapshot files back into the same dir.
                let parent = live_path.parent().unwrap_or(Path::new("."));
                for suffix in &["", "-wal", "-shm"] {
                    let mut p = live_path.as_os_str().to_os_string();
                    p.push(suffix);
                    let p = PathBuf::from(p);
                    if p.exists() { std::fs::remove_file(&p)?; }
                }
                for rel in &self.entries {
                    let src = self.snapshot_dir.join(rel);
                    let dst = parent.join(rel);
                    std::fs::copy(&src, &dst)?;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.snapshot_dir);
    }
}

fn copy_file_if_exists(src: &Path, dst: &Path, entries: &mut Vec<PathBuf>, rel: PathBuf) -> std::io::Result<()> {
    if src.exists() {
        std::fs::copy(src, dst)?;
        entries.push(rel);
    }
    Ok(())
}

fn copy_dir_recursive(src_root: &Path, dst_root: &Path, rel: &Path, entries: &mut Vec<PathBuf>) -> std::io::Result<()> {
    let src = src_root.join(rel);
    for entry in std::fs::read_dir(&src)? {
        let entry = entry?;
        let name = entry.file_name();
        let child_rel = rel.join(&name);
        let meta = entry.metadata()?;
        if meta.is_dir() {
            std::fs::create_dir_all(dst_root.join(&child_rel))?;
            copy_dir_recursive(src_root, dst_root, &child_rel, entries)?;
        } else {
            let dst = dst_root.join(&child_rel);
            if let Some(parent) = dst.parent() { std::fs::create_dir_all(parent)?; }
            std::fs::copy(entry.path(), dst)?;
            entries.push(child_rel);
        }
    }
    Ok(())
}
```

Extend `Fixtures` with two optional snapshot fields and helper methods. Modify the struct in place:

```rust
pub struct Fixtures {
    pub tier: Tier,
    pub mode: Durability,
    pub thunder_dir: PathBuf,
    pub sqlite_path: PathBuf,
    thunder: Option<Database>,
    sqlite: Option<Connection>,
    thunder_snap: Option<Snapshot>,
    sqlite_snap: Option<Snapshot>,
}
```

Adjust `make_fixtures` to initialize the two new fields to `None`:

```rust
pub(crate) fn make_fixtures(
    tier: Tier, mode: Durability,
    thunder_dir: PathBuf, sqlite_path: PathBuf,
    thunder: Database, sqlite: Connection,
) -> Fixtures {
    Fixtures {
        tier, mode, thunder_dir, sqlite_path,
        thunder: Some(thunder), sqlite: Some(sqlite),
        thunder_snap: None, sqlite_snap: None,
    }
}
```

Add the following methods to `impl Fixtures`:

```rust
    /// Capture pristine snapshots of both engine file sets.
    /// Must be called AFTER the fixture is fully built and BEFORE any measured
    /// mutation. Closes and reopens handles around the capture so on-disk files
    /// are flushed and self-consistent.
    pub fn snapshot_all(&mut self) -> std::io::Result<()> {
        // Close handles so OS page cache is flushed before we copy files.
        let (t, s) = self.take_handles();
        drop(t); drop(s);
        self.thunder_snap = Some(Snapshot::capture(Engine::Thunder, &self.thunder_dir)?);
        self.sqlite_snap  = Some(Snapshot::capture(Engine::Sqlite,  &self.sqlite_path)?);
        // Reopen handles for downstream use.
        let t = Database::open(&self.thunder_dir)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        let s = Connection::open(&self.sqlite_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        self.set_handles(t, s);
        Ok(())
    }

    /// Restore pristine files to both engines from the snapshots taken by
    /// `snapshot_all`. Handles are closed, files replaced, handles reopened.
    /// Panics if `snapshot_all` has not been called.
    pub fn restore_all(&mut self) -> std::io::Result<()> {
        let (t, s) = self.take_handles();
        drop(t); drop(s);
        self.thunder_snap.as_ref().expect("snapshot_all not called").restore(&self.thunder_dir)?;
        self.sqlite_snap.as_ref().expect("snapshot_all not called").restore(&self.sqlite_path)?;
        let t = Database::open(&self.thunder_dir)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        let s = Connection::open(&self.sqlite_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e)))?;
        self.set_handles(t, s);
        Ok(())
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --test vs_sqlite_read --release snapshot_and_restore_roundtrip_blog_fixture -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/fixtures.rs
git commit -m "Add Snapshot primitive + snapshot_all/restore_all on Fixtures (SP3 Task 2)"
```

---

### Task 3: `snapshot_drop_cleans_tempdir` self-test

**Files:**
- Modify: `tests/perf/harness_selftest.rs`

- [ ] **Step 1: Write failing test**

Append at the end of `tests/perf/harness_selftest.rs` inside the existing module or top-level test list (match existing style; if the file uses top-level `#[test]` functions, add one there):

```rust
#[test]
fn snapshot_drop_cleans_tempdir() {
    use common::fixtures::{build_blog_fixtures, drop_fixtures};
    use common::fairness::{Tier, Durability};

    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
    f.snapshot_all().unwrap();
    // Capture the dirs before drop.
    // Trusting Drop: we can't easily observe the temp dir path externally, so
    // snapshot again and check we can take two independent snapshots without
    // colliding or leaving the older dir populated.
    let before_tmp_entries = std::fs::read_dir(std::env::temp_dir()).unwrap().count();
    f.snapshot_all().unwrap(); // replaces previous snapshots; old Snapshots drop
    let after_tmp_entries = std::fs::read_dir(std::env::temp_dir()).unwrap().count();
    assert!(after_tmp_entries <= before_tmp_entries + 2,
        "expected old snapshot dirs cleaned by Drop");
    drop_fixtures(f);
}
```

- [ ] **Step 2: Run the test to verify it fails (RED)**

Run: `cargo test --test harness_selftest --release snapshot_drop_cleans_tempdir -- --nocapture`
Expected: if `snapshot_all` replacement does not drop the old Snapshot before building the new one, this may spuriously pass. Investigate: in Task 2's implementation, `self.thunder_snap = Some(...)` replaces the prior `Option`, which drops the old `Snapshot`. So test should PASS immediately.

If it fails, fix by ensuring the assignment sequence drops the old snapshot before the new one is created (or explicitly `self.thunder_snap = None;` before capture).

- [ ] **Step 3: Commit**

```bash
git add tests/perf/harness_selftest.rs
git commit -m "Self-test: Snapshot::Drop cleans temp dir (SP3 Task 3)"
```

---

## Phase 2 — `reset` hook on scenario

### Task 4: Add `ResetFn` to `Scenario` (backwards-compatible default)

**Files:**
- Modify: `tests/perf/common/scenario.rs`

- [ ] **Step 1: Write failing unit test**

Add to the `#[cfg(test)] mod tests` block in `tests/perf/common/scenario.rs`:

```rust
#[test]
fn builder_supports_reset_hook() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = Arc::clone(&calls);
    let s = Scenario::new("with_reset", "write")
        .setup(|t, m| build_blog_fixtures(t, m))
        .reset(move |_f| { calls2.fetch_add(1, Ordering::SeqCst); Ok(()) })
        .thunder(|_f| {}).sqlite(|_f| {}).assert(|_f| Ok(())).build();
    // Manually invoke the reset hook.
    let mut f = crate::common::fixtures::build_blog_fixtures(
        crate::common::fairness::Tier::Small,
        crate::common::fairness::Durability::Fast,
    );
    (s.reset)(&mut f).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    crate::common::fixtures::drop_fixtures(f);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test vs_sqlite_read --release builder_supports_reset_hook -- --nocapture`
Expected: FAIL — `reset` method/field not found on `Scenario`/`ScenarioBuilder`.

- [ ] **Step 3: Add `ResetFn`, builder method, and default no-op**

Edit `tests/perf/common/scenario.rs`:

```rust
pub type ResetFn   = Box<dyn Fn(&mut Fixtures) -> Result<(), String> + Send + Sync>;
```

Add a `reset: ResetFn` field to `Scenario` (NOT `Option<_>`; store a default no-op closure so the runner can always call it):

```rust
pub struct Scenario {
    pub name: &'static str,
    pub group: &'static str,
    pub setup: SetupFn,
    pub reset: ResetFn,
    pub thunder: ThunderFn,
    pub sqlite: SqliteFn,
    pub assert: AssertFn,
}
```

Add `reset: Option<ResetFn>` to `ScenarioBuilder`, a `.reset()` builder method, and have `.build()` default to a no-op when unset:

```rust
pub struct ScenarioBuilder {
    name: &'static str,
    group: &'static str,
    setup: Option<SetupFn>,
    reset: Option<ResetFn>,
    thunder: Option<ThunderFn>,
    sqlite: Option<SqliteFn>,
    assert: Option<AssertFn>,
}
```

Update `Scenario::new` to initialize `reset: None`:

```rust
impl Scenario {
    pub fn new(name: &'static str, group: &'static str) -> ScenarioBuilder {
        ScenarioBuilder { name, group, setup: None, reset: None, thunder: None, sqlite: None, assert: None }
    }
}
```

Add the builder method:

```rust
impl ScenarioBuilder {
    /// Set the reset function that restores pristine state before every
    /// warmup iteration and every timed sample. Default: no-op.
    pub fn reset<F: Fn(&mut Fixtures) -> Result<(), String> + Send + Sync + 'static>(mut self, f: F) -> Self {
        self.reset = Some(Box::new(f));
        self
    }
```

Update `.build()` to supply a default:

```rust
    pub fn build(self) -> Scenario {
        Scenario {
            name: self.name, group: self.group,
            setup: self.setup.expect("scenario missing setup"),
            reset: self.reset.unwrap_or_else(|| Box::new(|_| Ok(()))),
            thunder: self.thunder.expect("scenario missing thunder"),
            sqlite: self.sqlite.expect("scenario missing sqlite"),
            assert: self.assert.expect("scenario missing assert"),
        }
    }
}
```

- [ ] **Step 4: Run tests to verify GREEN**

Run: `cargo test --test vs_sqlite_read --release builder_supports_reset_hook -- --nocapture`
Expected: PASS.

Run: `cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read`
Expected: existing read-suite passes unchanged (default no-op reset for all read scenarios).

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/scenario.rs
git commit -m "Add optional reset hook to Scenario (SP3 Task 4)"
```

---

### Task 5: Wire `reset` into `Harness::run_one`

**Files:**
- Modify: `tests/perf/common/runner.rs`

- [ ] **Step 1: Write failing test**

Append to the `#[cfg(test)] mod tests` block in `tests/perf/common/runner.rs`:

```rust
#[test]
fn reset_hook_fires_before_each_sample_and_warmup() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let h = Harness { config: HarnessConfig {
        tiers: vec![Tier::Small], durabilities: vec![Durability::Fast],
        cache_states: vec![CacheState::Warm], sample_count: 3, update_baseline: false,
    }};
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = Arc::clone(&calls);
    let s = Scenario::new("reset_probe", "write")
        .setup(|t, m| build_blog_fixtures(t, m))
        .reset(move |_f| { calls2.fetch_add(1, Ordering::SeqCst); Ok(()) })
        .thunder(|_f| {}).sqlite(|_f| {}).assert(|_f| Ok(())).build();
    let _ = h.run_one(&s, Tier::Small, Durability::Fast, CacheState::Warm);
    // Warmup: 3 thunder resets + 3 sqlite resets.
    // Sample loop: 3 thunder resets + 3 sqlite resets.
    // Total: 12.
    assert_eq!(calls.load(Ordering::SeqCst), 12);
}
```

- [ ] **Step 2: Run test to verify RED**

Run: `cargo test --test vs_sqlite_read --release reset_hook_fires_before_each_sample_and_warmup -- --nocapture`
Expected: FAIL — assertion 0 != 12.

- [ ] **Step 3: Call reset in warmup + sample loops**

In `tests/perf/common/runner.rs` inside `run_one`, edit the warmup loop to:

```rust
        // Warmup: 3 iterations, results discarded. Reset before each call.
        for _ in 0..3 {
            let _ = (scenario.reset)(&mut fixtures);
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.thunder)(&mut fixtures)));
            let _ = (scenario.reset)(&mut fixtures);
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.sqlite)(&fixtures)));
        }
```

The Thunder sample loop must hold a `&mut` on fixtures across the iterations, so restructure it to avoid the `catch_unwind` closure. Replace:

```rust
        let thunder_panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut timings = Vec::with_capacity(samples);
            for _ in 0..samples {
                if cache == CacheState::Cold {
                    let _ = crate::common::fixtures::reopen_handles(&mut fixtures);
                }
                let t0 = Instant::now();
                (scenario.thunder)(&mut fixtures);
                timings.push(t0.elapsed().as_nanos());
            }
            Harness::reduce(timings)
        }));
```

with a per-iteration `catch_unwind` that preserves reset-before-sample semantics and is cheap to refactor:

```rust
        let mut thunder_timings: Vec<u128> = Vec::with_capacity(samples);
        let mut thunder_panicked = false;
        for _ in 0..samples {
            if cache == CacheState::Cold {
                let _ = crate::common::fixtures::reopen_handles(&mut fixtures);
            }
            if let Err(msg) = (scenario.reset)(&mut fixtures) {
                eprintln!("reset returned Err: {}", msg);
            }
            let t0 = Instant::now();
            let panic_guard = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.thunder)(&mut fixtures)));
            let elapsed = t0.elapsed().as_nanos();
            if panic_guard.is_err() { thunder_panicked = true; break; }
            thunder_timings.push(elapsed);
        }
        let thunder = if thunder_panicked {
            None
        } else {
            let (median, p95, out) = Harness::reduce(thunder_timings);
            Some(EngineTiming { median_ns: median, p95_ns: p95, sample_count: samples, dropped_outliers: out })
        };
```

Apply the same restructure to the SQLite sample loop:

```rust
        let mut sqlite_timings: Vec<u128> = Vec::with_capacity(samples);
        let mut sqlite_panicked = false;
        for _ in 0..samples {
            if cache == CacheState::Cold {
                let _ = crate::common::fixtures::reopen_handles(&mut fixtures);
            }
            if let Err(msg) = (scenario.reset)(&mut fixtures) {
                eprintln!("reset returned Err: {}", msg);
            }
            let t0 = Instant::now();
            let panic_guard = std::panic::catch_unwind(AssertUnwindSafe(|| (scenario.sqlite)(&fixtures)));
            let elapsed = t0.elapsed().as_nanos();
            if panic_guard.is_err() { sqlite_panicked = true; break; }
            sqlite_timings.push(elapsed);
        }
        let sqlite = if sqlite_panicked {
            None
        } else {
            let (median, p95, out) = Harness::reduce(sqlite_timings);
            Some(EngineTiming { median_ns: median, p95_ns: p95, sample_count: samples, dropped_outliers: out })
        };
```

- [ ] **Step 4: Run test to verify GREEN**

Run: `cargo test --test vs_sqlite_read --release reset_hook_fires_before_each_sample_and_warmup -- --nocapture`
Expected: PASS (12 calls).

Run the entire read suite and self-tests:
```bash
cargo test --test vs_sqlite_read --release -- --nocapture vs_sqlite_read
cargo test --test harness_selftest --release
```
Expected: all green, no regressions. Read scenarios use the default no-op reset so behavior is unchanged.

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/runner.rs
git commit -m "Runner invokes reset hook before each warmup and sample iter (SP3 Task 5)"
```

---

## Phase 3 — Write scenarios

### Task 6: `build_empty_fixtures` helper

**Files:**
- Modify: `tests/perf/common/fixtures.rs`

`build_blog_fixtures` pre-populates 10k posts, which is wrong for INSERT scenarios. Add an empty-tables variant with the same schema but no rows.

- [ ] **Step 1: Write failing test**

Append to `#[cfg(test)] mod tests` in `fixtures.rs`:

```rust
#[test]
fn empty_fixture_has_zero_rows() {
    let mut f = build_empty_fixtures(Tier::Small, Durability::Fast);
    use thunderdb::DirectDataAccess;
    assert_eq!(f.thunder_mut().count("blog_posts", vec![]).unwrap(), 0);
    let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
    assert_eq!(s, 0);
    drop_fixtures(f);
}
```

- [ ] **Step 2: Run test to verify RED**

Run: `cargo test --test vs_sqlite_read --release empty_fixture_has_zero_rows -- --nocapture`
Expected: FAIL — `build_empty_fixtures` not found.

- [ ] **Step 3: Add `build_empty_fixtures`**

In `tests/perf/common/fixtures.rs` add (near `build_blog_fixtures`):

```rust
/// Build the same blog schema as `build_blog_fixtures` but with zero rows in
/// posts and comments. Users table is left empty as well. Intended for INSERT
/// benchmarks that measure population cost starting from an empty DB.
pub fn build_empty_fixtures(tier: Tier, mode: Durability) -> Fixtures {
    use std::sync::atomic::{AtomicU64, Ordering};
    use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let unique = format!(
        "{}_{}_{}_empty_{}",
        std::process::id(), tier.label(), mode.label(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
    );
    let base = std::env::temp_dir().join(format!("thunderdb_perf_{}", unique));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let thunder_dir = base.join("thunder");
    let sqlite_path = base.join("sqlite.db");

    let mut tdb = Database::open(&thunder_dir).expect("open thunderdb");
    // Create empty tables with matching schemas so scenarios can INSERT without
    // a prior schema-evolution step.
    for (table, cols) in [
        ("users", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "email".into(), data_type: "VARCHAR".into() },
        ]),
        ("blog_posts", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "title".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "content".into(), data_type: "VARCHAR".into() },
        ]),
        ("comments", vec![
            ColumnInfo { name: "id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "post_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
            ColumnInfo { name: "text".into(), data_type: "VARCHAR".into() },
        ]),
    ] {
        tdb.create_table(table).unwrap();
        let tbl = tdb.get_table_mut(table).unwrap();
        tbl.set_schema(TableSchema { columns: cols }).unwrap();
        tbl.create_index("id").unwrap();
    }

    let sdb = Connection::open(&sqlite_path).unwrap();
    match mode {
        Durability::Fast => { sdb.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); }
        Durability::Durable => { sdb.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;").unwrap(); }
    }
    sdb.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);
         CREATE TABLE blog_posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL);
         CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, author_id INTEGER NOT NULL, text TEXT NOT NULL);"
    ).unwrap();

    make_fixtures(tier, mode, thunder_dir, sqlite_path, tdb, sdb)
}
```

> If `Database::create_table` does not exist or has a different signature, check the actual ThunderDB API via `grep -n "create_table" src/**/*.rs` and adapt. If Thunder auto-creates tables on first `insert_batch`, an alternative is to write a single row, delete it, and set the schema — stop to consult the codebase before hacking around it.

- [ ] **Step 4: Run test to verify GREEN**

Run: `cargo test --test vs_sqlite_read --release empty_fixture_has_zero_rows -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/perf/common/fixtures.rs
git commit -m "Add build_empty_fixtures helper (SP3 Task 6)"
```

---

### Task 7: `vs_sqlite_write.rs` skeleton + W1 INSERT per-row commit

**Files:**
- Create: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Create the test binary with W1 only**

Write `tests/perf/vs_sqlite_write.rs`:

```rust
//! ThunderDB vs SQLite — write-path scenarios (SP3).

mod common;

use common::*;
use rusqlite::params;
use thunderdb::{DirectDataAccess, Value};

/// Number of rows written by every SP3 insert/update/delete scenario at the
/// SMALL tier. Kept separate from `Tier::Small.post_count()` so write-suite
/// sizing can diverge from read-suite sizing later without coupling.
const WRITE_ROW_COUNT: usize = 10_000;

fn scenarios() -> Vec<Scenario> {
    vec![
        // W1. INSERT 10k rows, per-row commit (no explicit txn)
        Scenario::new("W1. INSERT 10k per-row commit", "write")
            .setup(|t, m| {
                let mut f = build_empty_fixtures(t, m);
                f.snapshot_all().expect("snapshot_all");
                f
            })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                let db = f.thunder_mut();
                for i in 1..=WRITE_ROW_COUNT as i32 {
                    db.insert("blog_posts", vec![
                        Value::Int32(i),
                        Value::Int32((i % 5) + 1),
                        Value::varchar(format!("Post #{}", i)),
                        Value::varchar(format!("Body of post {}", i)),
                    ]).unwrap();
                }
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
                for i in 1..=WRITE_ROW_COUNT as i32 {
                    st.execute(params![i, (i % 5) + 1, format!("Post #{}", i), format!("Body of post {}", i)]).unwrap();
                }
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t != WRITE_ROW_COUNT || s as usize != WRITE_ROW_COUNT {
                    Err(format!("W1 count mismatch: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
    ]
}

#[test]
fn vs_sqlite_write() {
    use std::path::PathBuf;
    let harness = Harness::from_env();
    let baseline_path = PathBuf::from("perf/baseline.json");
    let artifact_dir = PathBuf::from("target/perf");
    let report = harness.run(&scenarios(), &baseline_path, &artifact_dir);

    // Loss gate ONLY at FAST/WARM cells. COLD/DURABLE reported but not enforced.
    let mut fw_loss = 0;
    for cell in &report.cells {
        if cell.mode == common::Durability::Fast && cell.cache == common::CacheState::Warm {
            for r in &cell.results {
                if matches!(r.verdict, Verdict::Loss) || matches!(r.verdict, Verdict::Failure(_)) {
                    eprintln!("FAST/WARM non-Win/Tie: {} -> {:?}", r.scenario, r.verdict);
                    fw_loss += 1;
                }
            }
        }
    }
    assert_eq!(fw_loss, 0, "FAST/WARM write scenarios must all be Win or Tie");
}
```

> If `Verdict`, `Durability`, or `CacheState` aren't re-exported by `tests/perf/common/mod.rs`, either import them from their submodules (`common::verdict::Verdict`, `common::fairness::{Durability, CacheState}`) or extend the re-exports in a small follow-up edit to `mod.rs`. Check first with `grep -n "pub use" tests/perf/common/mod.rs`.

- [ ] **Step 2: Run the quick variant**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: test executes; scoreboard prints a single W1 row. If W1 is a Loss, that's still acceptable for this task — fix in Task 16 (optimize / investigate) or keep data for the CHANGES entry.
Must NOT panic during reset (snapshot must restore cleanly).

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W1: INSERT per-row commit scenario + FAST/WARM loss gate (SP3 Task 7)"
```

---

### Task 8: W2 INSERT single txn

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W2 to `scenarios()` (before the closing `]`):**

```rust
        // W2. INSERT 10k rows, single transaction
        Scenario::new("W2. INSERT 10k single txn", "write")
            .setup(|t, m| { let mut f = build_empty_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                let rows: Vec<Vec<Value>> = (1..=WRITE_ROW_COUNT as i32).map(|i| vec![
                    Value::Int32(i),
                    Value::Int32((i % 5) + 1),
                    Value::varchar(format!("Post #{}", i)),
                    Value::varchar(format!("Body of post {}", i)),
                ]).collect();
                f.thunder_mut().insert_batch("blog_posts", rows).unwrap();
            })
            .sqlite(|f| {
                let tx = f.sqlite().unchecked_transaction().unwrap();
                {
                    let mut st = tx.prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
                    for i in 1..=WRITE_ROW_COUNT as i32 {
                        st.execute(params![i, (i % 5) + 1, format!("Post #{}", i), format!("Body of post {}", i)]).unwrap();
                    }
                }
                tx.commit().unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t != WRITE_ROW_COUNT || s as usize != WRITE_ROW_COUNT {
                    Err(format!("W2 count mismatch: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: 2 rows in scoreboard.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W2: INSERT single transaction (SP3 Task 8)"
```

---

### Task 9: W3 INSERT batch 1000

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W3**

```rust
        // W3. INSERT 10k rows in batches of 1000
        Scenario::new("W3. INSERT 10k batch 1000", "write")
            .setup(|t, m| { let mut f = build_empty_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                for batch_start in (1..=WRITE_ROW_COUNT as i32).step_by(1000) {
                    let rows: Vec<Vec<Value>> = (batch_start..batch_start + 1000).map(|i| vec![
                        Value::Int32(i), Value::Int32((i % 5) + 1),
                        Value::varchar(format!("Post #{}", i)),
                        Value::varchar(format!("Body of post {}", i)),
                    ]).collect();
                    f.thunder_mut().insert_batch("blog_posts", rows).unwrap();
                }
            })
            .sqlite(|f| {
                for batch_start in (1..=WRITE_ROW_COUNT as i32).step_by(1000) {
                    let tx = f.sqlite().unchecked_transaction().unwrap();
                    {
                        let mut st = tx.prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
                        for i in batch_start..batch_start + 1000 {
                            st.execute(params![i, (i % 5) + 1, format!("Post #{}", i), format!("Body of post {}", i)]).unwrap();
                        }
                    }
                    tx.commit().unwrap();
                }
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t != WRITE_ROW_COUNT || s as usize != WRITE_ROW_COUNT {
                    Err(format!("W3 count mismatch: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: 3 rows.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W3: INSERT batched 1000 (SP3 Task 9)"
```

---

### Task 10: W4 INSERT with secondary index

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

The existing `build_empty_fixtures` creates only the `id` index. For W4 we need an additional secondary index on `author_id` before inserting. Add the secondary indices inside the scenario's `setup`.

- [ ] **Step 1: Append W4**

```rust
        // W4. INSERT 10k rows into a table with a secondary index (author_id)
        Scenario::new("W4. INSERT 10k w/ secondary index", "write")
            .setup(|t, m| {
                let mut f = build_empty_fixtures(t, m);
                // Add secondary index on Thunder side.
                {
                    let tbl = f.thunder_mut().get_table_mut("blog_posts").unwrap();
                    tbl.create_index("author_id").unwrap();
                    tbl.create_index("title").unwrap();
                }
                // And on SQLite.
                f.sqlite().execute_batch(
                    "CREATE INDEX idx_posts_author ON blog_posts(author_id);
                     CREATE INDEX idx_posts_title ON blog_posts(title);"
                ).unwrap();
                f.snapshot_all().unwrap();
                f
            })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                let rows: Vec<Vec<Value>> = (1..=WRITE_ROW_COUNT as i32).map(|i| vec![
                    Value::Int32(i), Value::Int32((i % 5) + 1),
                    Value::varchar(format!("Post #{}", i)),
                    Value::varchar(format!("Body of post {}", i)),
                ]).collect();
                f.thunder_mut().insert_batch("blog_posts", rows).unwrap();
            })
            .sqlite(|f| {
                let tx = f.sqlite().unchecked_transaction().unwrap();
                {
                    let mut st = tx.prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
                    for i in 1..=WRITE_ROW_COUNT as i32 {
                        st.execute(params![i, (i % 5) + 1, format!("Post #{}", i), format!("Body of post {}", i)]).unwrap();
                    }
                }
                tx.commit().unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t != WRITE_ROW_COUNT || s as usize != WRITE_ROW_COUNT {
                    Err(format!("W4 count mismatch: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: 4 rows.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W4: INSERT with secondary index (SP3 Task 10)"
```

---

### Task 11: W5 UPDATE by PK

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

For UPDATE scenarios the baseline is the blog fixture (already ~10k posts at Tier::Small — verify by reading `Tier::Small.post_count()` in `tests/perf/common/fairness.rs`; if it's not 10k, pass `WRITE_ROW_COUNT` to `build_blog_fixtures` or adapt assertions to `Tier::Small.post_count()`).

- [ ] **Step 1: Check post count**

Run: `grep -n "post_count\|Tier::Small" tests/perf/common/fairness.rs`
Expected output shows `Tier::Small.post_count() == 10000` (or whatever SP1 set it to). If different, in the scenarios below replace `WRITE_ROW_COUNT` with `tier.post_count()` via a `setup` closure that captures `t`.

- [ ] **Step 2: Append W5**

```rust
        // W5. UPDATE every row by primary key, single txn
        Scenario::new("W5. UPDATE 10k by PK", "write")
            .setup(|t, m| { let mut f = build_blog_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                use thunderdb::{Filter, Operator};
                let db = f.thunder_mut();
                let n = db.count("blog_posts", vec![]).unwrap() as i32;
                for i in 1..=n {
                    db.update("blog_posts",
                        vec![Filter::new("id", Operator::Equal(Value::Int32(i)))],
                        vec![("title".into(), Value::varchar(format!("Updated #{}", i)))]).unwrap();
                }
            })
            .sqlite(|f| {
                let tx = f.sqlite().unchecked_transaction().unwrap();
                {
                    let mut st = tx.prepare("UPDATE blog_posts SET title = ?1 WHERE id = ?2").unwrap();
                    let n: i64 = tx.query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                    for i in 1..=n as i32 {
                        st.execute(params![format!("Updated #{}", i), i]).unwrap();
                    }
                }
                tx.commit().unwrap();
            })
            .assert(|f| {
                let tc = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let sc: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if tc as i64 != sc {
                    return Err(format!("W5 row-count drift: thunder={}, sqlite={}", tc, sc));
                }
                // Spot-check one row has the updated title on both engines.
                use thunderdb::{Filter, Operator};
                let tt = f.thunder_mut().scan_with_projection("blog_posts",
                    vec![Filter::new("id", Operator::Equal(Value::Int32(1)))],
                    None, None, Some(vec![2])).unwrap();
                let st: String = f.sqlite().query_row("SELECT title FROM blog_posts WHERE id = 1", [], |r| r.get(0)).unwrap();
                if !format!("{:?}", tt).contains("Updated #1") || st != "Updated #1" {
                    return Err(format!("W5 update missing: thunder={:?}, sqlite={}", tt, st));
                }
                Ok(())
            })
            .build(),
```

> If `Database::update` has a different signature, adjust call. Consult `grep -n "fn update" src/database.rs` before deviating.

- [ ] **Step 3: Quick run + commit**

Run: `THUNDERDB_QUICK=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W5: UPDATE 10k by PK (SP3 Task 11)"
```

---

### Task 12: W6 UPDATE by indexed column (range)

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W6**

```rust
        // W6. UPDATE by indexed column — set all posts with author_id=3 to new title
        Scenario::new("W6. UPDATE by indexed column", "write")
            .setup(|t, m| { let mut f = build_blog_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                use thunderdb::{Filter, Operator};
                f.thunder_mut().update("blog_posts",
                    vec![Filter::new("author_id", Operator::Equal(Value::Int32(3)))],
                    vec![("title".into(), Value::varchar("bulk-updated".into()))]).unwrap();
            })
            .sqlite(|f| {
                f.sqlite().execute("UPDATE blog_posts SET title = 'bulk-updated' WHERE author_id = 3", params![]).unwrap();
            })
            .assert(|f| {
                use thunderdb::{Filter, Operator};
                let tc = f.thunder_mut().scan_with_projection("blog_posts",
                    vec![Filter::new("author_id", Operator::Equal(Value::Int32(3)))],
                    None, None, Some(vec![2])).unwrap().len();
                let sc: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts WHERE author_id = 3 AND title = 'bulk-updated'", [], |r| r.get(0)).unwrap();
                if tc as i64 != sc {
                    Err(format!("W6 mismatch: thunder matched={}, sqlite updated={}", tc, sc))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run + commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W6: UPDATE by indexed column (SP3 Task 12)"
```

---

### Task 13: W7 DELETE by PK

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W7**

```rust
        // W7. DELETE every row by primary key, single txn
        Scenario::new("W7. DELETE 10k by PK", "write")
            .setup(|t, m| { let mut f = build_blog_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                use thunderdb::{Filter, Operator};
                let db = f.thunder_mut();
                let n = db.count("blog_posts", vec![]).unwrap() as i32;
                for i in 1..=n {
                    db.delete("blog_posts", vec![Filter::new("id", Operator::Equal(Value::Int32(i)))]).unwrap();
                }
            })
            .sqlite(|f| {
                let tx = f.sqlite().unchecked_transaction().unwrap();
                {
                    let mut st = tx.prepare("DELETE FROM blog_posts WHERE id = ?1").unwrap();
                    let n: i64 = tx.query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                    for i in 1..=n as i32 { st.execute(params![i]).unwrap(); }
                }
                tx.commit().unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t != 0 || s != 0 {
                    Err(format!("W7 not empty: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run + commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W7: DELETE 10k by PK (SP3 Task 13)"
```

---

### Task 14: W8 DELETE by range predicate

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W8**

```rust
        // W8. DELETE by a range predicate: remove all posts with id > 5000
        Scenario::new("W8. DELETE by range predicate", "write")
            .setup(|t, m| { let mut f = build_blog_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                use thunderdb::{Filter, Operator};
                f.thunder_mut().delete("blog_posts",
                    vec![Filter::new("id", Operator::GreaterThan(Value::Int32(5000)))]).unwrap();
            })
            .sqlite(|f| {
                f.sqlite().execute("DELETE FROM blog_posts WHERE id > 5000", params![]).unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t as i64 != s {
                    Err(format!("W8 mismatch: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),
```

- [ ] **Step 2: Quick run + commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W8: DELETE by range predicate (SP3 Task 14)"
```

---

### Task 15: W9 Mixed INSERT + UPDATE + DELETE

**Files:**
- Modify: `tests/perf/vs_sqlite_write.rs`

- [ ] **Step 1: Append W9**

```rust
        // W9. Mixed mutation burst: insert 1000 new rows, update 1000 existing,
        // delete 1000 existing, all in a single logical operation.
        Scenario::new("W9. Mixed INSERT+UPDATE+DELETE", "write")
            .setup(|t, m| { let mut f = build_blog_fixtures(t, m); f.snapshot_all().unwrap(); f })
            .reset(|f| f.restore_all().map_err(|e| format!("restore: {}", e)))
            .thunder(|f| {
                use thunderdb::{Filter, Operator};
                let db = f.thunder_mut();
                // Insert 1000 new rows (ids 10001..=11000).
                let new_rows: Vec<Vec<Value>> = (10_001..=11_000).map(|i| vec![
                    Value::Int32(i), Value::Int32((i % 5) + 1),
                    Value::varchar(format!("Mixed #{}", i)),
                    Value::varchar("Mixed body".into()),
                ]).collect();
                db.insert_batch("blog_posts", new_rows).unwrap();
                // Update 1000 existing (ids 1..=1000).
                for i in 1..=1000 {
                    db.update("blog_posts",
                        vec![Filter::new("id", Operator::Equal(Value::Int32(i)))],
                        vec![("title".into(), Value::varchar(format!("Mixed-upd #{}", i)))]).unwrap();
                }
                // Delete 1000 existing (ids 2001..=3000).
                for i in 2001..=3000 {
                    db.delete("blog_posts", vec![Filter::new("id", Operator::Equal(Value::Int32(i)))]).unwrap();
                }
            })
            .sqlite(|f| {
                let tx = f.sqlite().unchecked_transaction().unwrap();
                {
                    let mut ins = tx.prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)").unwrap();
                    for i in 10_001..=11_000 {
                        ins.execute(params![i, (i % 5) + 1, format!("Mixed #{}", i), "Mixed body"]).unwrap();
                    }
                    let mut upd = tx.prepare("UPDATE blog_posts SET title = ?1 WHERE id = ?2").unwrap();
                    for i in 1..=1000 {
                        upd.execute(params![format!("Mixed-upd #{}", i), i]).unwrap();
                    }
                    let mut del = tx.prepare("DELETE FROM blog_posts WHERE id = ?1").unwrap();
                    for i in 2001..=3000 { del.execute(params![i]).unwrap(); }
                }
                tx.commit().unwrap();
            })
            .assert(|f| {
                let tc = f.thunder_mut().count("blog_posts", vec![]).unwrap() as i64;
                let sc: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if tc != sc { return Err(format!("W9 count drift: thunder={}, sqlite={}", tc, sc)); }
                let st: String = f.sqlite().query_row("SELECT title FROM blog_posts WHERE id = 1", [], |r| r.get(0)).unwrap();
                if st != "Mixed-upd #1" { return Err(format!("W9 update missing: sqlite={}", st)); }
                Ok(())
            })
            .build(),
```

- [ ] **Step 2: Quick run + commit**

```bash
git add tests/perf/vs_sqlite_write.rs
git commit -m "SP3 W9: mixed INSERT+UPDATE+DELETE (SP3 Task 15)"
```

---

## Phase 4 — Full run, optimize, baseline, release

### Task 16: Full WARM run — investigate any Loss

**Files:** varies (only if a Loss surfaces)

- [ ] **Step 1: Run full 11-sample suite at SMALL/FAST/WARM**

Run: `cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: `loss == 0` assertion holds. If it does, skip to Task 17.

- [ ] **Step 2 (only if Loss): diagnose**

For each Loss scenario, compare Thunder vs SQLite call chains. Typical suspects:
- Per-row index updates dominating W1/W4 → batch the index maintenance inside `insert_batch` if not already
- W5/W7 per-PK linear scan → verify the `id` index is actually used by `update`/`delete`
- Commit overhead on W1 per-row → compare SQLite autocommit path

Fix **only** in-scope Thunder code (no new public APIs; see Q6-C in spec). If root cause is an API-shape gap, stop and escalate to a follow-up SP rather than growing SP3 scope.

- [ ] **Step 3 (only if Loss fixed): re-run**

Repeat Step 1.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A src/
git commit -m "Fix <specific Thunder slowdown> uncovered by SP3 <Wn> (SP3 Task 16)"
```

---

### Task 17: Full COLD run — record, don't gate

**Files:** none (informational)

- [ ] **Step 1: Run COLD variant**

Run: `THUNDERDB_CACHE=cold cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: the test may assert-fail on the FAST/WARM gate because the harness runs both WARM and COLD when `both` is set — confirm current env resolution:

Run: `THUNDERDB_CACHE=both cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: FAST/WARM cells Win/Tie (gate passes). FAST/COLD cells will likely show losses; they are NOT gated.

- [ ] **Step 2: Capture COLD ratios for CHANGES.md**

Copy the COLD scoreboard rows verbatim to a scratch buffer; you'll paste them into the CHANGES entry in Task 19.

No commit for this task.

---

### Task 18: Add snapshot + reset self-tests

**Files:**
- Modify: `tests/perf/harness_selftest.rs`

- [ ] **Step 1: Add a snapshot-COLD-safety test**

Append:

```rust
#[test]
fn restore_all_preserves_cold_fadvise() {
    use common::fixtures::{build_blog_fixtures, reopen_handles, drop_fixtures};
    use common::fairness::{Tier, Durability};

    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
    f.snapshot_all().unwrap();
    // Mutate, restore, then reopen COLD — must not panic.
    f.thunder_mut().delete("blog_posts", vec![]).unwrap();
    f.restore_all().unwrap();
    reopen_handles(&mut f).unwrap();
    use thunderdb::DirectDataAccess;
    let n = f.thunder_mut().count("blog_posts", vec![]).unwrap();
    assert_eq!(n, Tier::Small.post_count());
    drop_fixtures(f);
}
```

- [ ] **Step 2: Run self-tests**

Run: `cargo test --test harness_selftest --release`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/perf/harness_selftest.rs
git commit -m "Self-test: restore_all + COLD reopen interop (SP3 Task 18)"
```

---

### Task 19: Promote baseline + CHANGES.md + memory update

**Files:**
- Modify: `perf/baseline.json` (via `THUNDERDB_UPDATE_BASELINE=1`)
- Modify: `CHANGES.md`
- Modify: memory file(s) (project scoreboard)

- [ ] **Step 1: Promote baseline**

Run: `THUNDERDB_UPDATE_BASELINE=1 cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: final line "Baseline promoted: perf/baseline.json".

- [ ] **Step 2: Verify baseline committed**

Run: `git diff --stat perf/baseline.json`
Expected: W1–W9 scenarios present with timings + ratios.

- [ ] **Step 3: Append SP3 entry to top of `CHANGES.md`**

Template (replace `<...>` with actual values from the baseline run):

```markdown
## 2026-04-23 - SP3: Write-path benchmarks

Third sub-project in the "faster than SQLite in all benchmarks" program. Closes write-path coverage at SMALL/FAST/WARM.

- **9 new write scenarios** (`tests/perf/vs_sqlite_write.rs`): W1 INSERT per-row, W2 INSERT single txn, W3 INSERT batch 1000, W4 INSERT with secondary index, W5 UPDATE by PK, W6 UPDATE by indexed column, W7 DELETE by PK, W8 DELETE by range predicate, W9 Mixed INSERT+UPDATE+DELETE.
- **New `Snapshot` primitive** in `tests/perf/common/fixtures.rs` — byte-copies pristine engine files to a temp dir; `Fixtures::snapshot_all` / `restore_all` reset both engines before every sample so mutation benchmarks stay deterministic. Snapshot's `Drop` cleans its temp dir. Reusable by SP5 (large scale) and SP7 (concurrency).
- **`reset` hook on `Scenario`** — called before every warmup and timed sample, outside the timer. Default no-op keeps the read suite unchanged.
- **FAST/WARM scoreboard**: <N Win, N Tie, 0 Loss> across W1–W9. Full SMALL/FAST/WARM program now <total> Win/Tie, 0 Loss. Hard `loss == 0` assertion in `vs_sqlite_write`.
- **COLD findings (reported, not gated)**: <paste COLD ratios here>. Feeds evidence for future SP2b cold-start optimization.
- **Baseline promoted** for W1–W9 at SMALL/FAST/WARM.

Spec: `docs/superpowers/specs/2026-04-23-sp3-write-path-design.md`
Plan: `docs/superpowers/plans/2026-04-23-sp3-write-path.md`
```

- [ ] **Step 4: Update project memory scoreboard**

Edit `/home/fabio/.claude/projects/-home-fabio-dev-projects-thunderdb/memory/project_faster_than_sqlite_program.md`:
- Mark SP3 row `✅ merged`.
- Update the current-scoreboard line at the bottom to reflect W1–W9 cells added.

(This is a write to disk; no git commit needed for the memory file — it's outside the repo.)

- [ ] **Step 5: Commit CHANGES + baseline**

```bash
git add perf/baseline.json CHANGES.md
git commit -m "Rebaseline + CHANGES for SP3 (SP3 Task 19)"
```

---

### Task 20: Merge to master

**Files:** none

- [ ] **Step 1: Switch to master, pull**

Run: `git checkout master && git pull --ff-only`
Expected: clean fast-forward or already up to date.

- [ ] **Step 2: Merge SP3 (no-ff for history)**

Run: `git merge --no-ff sp3-write-path -m "Merge SP3: write-path benchmarks"`
Expected: merge commit created.

- [ ] **Step 3: Verify green on master**

Run: `cargo test --test vs_sqlite_write --release -- --nocapture vs_sqlite_write`
Expected: loss gate passes.

- [ ] **Step 4: DO NOT PUSH.** (Per CLAUDE.md rule.)

---

## Self-Review

- **Spec coverage:** W1–W9 ↔ Tasks 7–15; Snapshot primitive ↔ Task 2; reset hook ↔ Tasks 4–5; FAST/WARM loss gate ↔ Task 7; COLD reporting ↔ Task 17; baseline promotion ↔ Task 19; self-tests ↔ Tasks 3, 18; CHANGES + memory ↔ Task 19. ✅
- **Placeholders:** All steps contain exact code and commands. Two spots flag API-verification needs (`Database::create_table` in Task 6, `Database::update` signature in Task 11) — these are explicit instructions to consult the codebase, not TBDs.
- **Type consistency:** `Snapshot`, `Engine`, `snapshot_all`, `restore_all`, `reset` used identically across tasks.
- **Scope:** single subsystem (write-path bench). No new Thunder public API unless evidence forces a Task 16 fix. Matches SP3 acceptance.
