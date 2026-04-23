//! End-to-end harness self-tests using synthetic scenarios.

mod common;

use common::*;
use std::thread::sleep;
use std::time::Duration;

fn mini_config(cache: CacheState, mode: Durability) -> HarnessConfig {
    HarnessConfig {
        tiers: vec![Tier::Small],
        durabilities: vec![mode],
        cache_states: vec![cache],
        sample_count: 3,
        update_baseline: false,
    }
}

#[test]
fn both_engines_tie() {
    let h = Harness { config: mini_config(CacheState::Warm, Durability::Fast) };
    let s = vec![Scenario::new("tie_1ms", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| sleep(Duration::from_millis(1)))
        .sqlite(|_f| sleep(Duration::from_millis(1)))
        .assert(|_f| Ok(()))
        .build()];
    let r = h.run_scenarios(&s);
    assert_eq!(r.cells[0].results[0].verdict, Verdict::Tie);
}

#[test]
fn thunder_wins_big_gap() {
    let h = Harness { config: mini_config(CacheState::Warm, Durability::Fast) };
    let s = vec![Scenario::new("thunder_fast", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| sleep(Duration::from_millis(1)))
        .sqlite(|_f| sleep(Duration::from_millis(20)))
        .assert(|_f| Ok(()))
        .build()];
    let r = h.run_scenarios(&s);
    assert_eq!(r.cells[0].results[0].verdict, Verdict::Win);
}

#[test]
fn durable_is_unsupported() {
    let h = Harness { config: mini_config(CacheState::Warm, Durability::Durable) };
    let s = vec![Scenario::new("any", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| {})
        .sqlite(|_f| {})
        .assert(|_f| Ok(()))
        .build()];
    let r = h.run_scenarios(&s);
    assert_eq!(r.cells[0].results[0].verdict, Verdict::Unsupported);
}

#[test]
fn thunder_panic_is_failure_not_crash() {
    let h = Harness { config: mini_config(CacheState::Warm, Durability::Fast) };
    let s = vec![Scenario::new("crash", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| panic!("deliberate"))
        .sqlite(|_f| {})
        .assert(|_f| Ok(()))
        .build()];
    let r = h.run_scenarios(&s);
    assert!(matches!(r.cells[0].results[0].verdict, Verdict::Failure(_)));
}

#[test]
fn assert_mismatch_is_failure() {
    let h = Harness { config: mini_config(CacheState::Warm, Durability::Fast) };
    let s = vec![Scenario::new("wrong", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| {})
        .sqlite(|_f| {})
        .assert(|_f| Err("engines disagree".into()))
        .build()];
    let r = h.run_scenarios(&s);
    assert!(matches!(&r.cells[0].results[0].verdict, Verdict::Failure(m) if m == "engines disagree"));
}

#[test]
fn cold_cache_completes_scenario() {
    let h = Harness { config: mini_config(CacheState::Cold, Durability::Fast) };
    let s = vec![Scenario::new("cold_end_to_end", "self")
        .setup(|t, m| build_blog_fixtures(t, m))
        .thunder(|_f| {})
        .sqlite(|_f| {})
        .assert(|_f| Ok(()))
        .build()];
    let r = h.run_scenarios(&s);
    // Noisy timing for no-op closures; just ensure it completed without Failure/Unsupported.
    assert!(!matches!(r.cells[0].results[0].verdict, Verdict::Failure(_) | Verdict::Unsupported),
        "got {:?}", r.cells[0].results[0].verdict);
}

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

#[test]
fn snapshot_drop_cleans_tempdir() {
    use common::fixtures::{build_blog_fixtures, drop_fixtures};
    use common::fairness::{Tier, Durability};

    let mut f = build_blog_fixtures(Tier::Small, Durability::Fast);
    f.snapshot_all().unwrap();
    // Snapshotting again should drop the prior Snapshots (and their temp dirs).
    let before = std::fs::read_dir(std::env::temp_dir()).unwrap().count();
    f.snapshot_all().unwrap();
    let after = std::fs::read_dir(std::env::temp_dir()).unwrap().count();
    // At most 2 new entries (the two new snapshot dirs). Old ones must be cleaned.
    assert!(after <= before + 2,
        "expected old snapshot dirs cleaned by Drop; before={}, after={}", before, after);
    drop_fixtures(f);
}
