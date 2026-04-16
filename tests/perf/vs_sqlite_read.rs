//! ThunderDB vs SQLite — read-path scenarios, running through the harness.
//! Migrated from tests/integration/thunderdb_vs_sqlite_bench.rs.

mod common;

use common::*;
use thunderdb::DirectDataAccess;
use std::path::PathBuf;

fn scenarios() -> Vec<Scenario> {
    vec![
        // 1. COUNT(*) all three tables
        Scenario::new("1. COUNT(*) all three tables", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().count("users", vec![]).unwrap();
                let _ = f.thunder_mut().count("blog_posts", vec![]).unwrap();
                let _ = f.thunder_mut().count("comments", vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
                let _: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                let _: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM comments", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let tu = f.thunder_mut().count("users", vec![]).unwrap();
                let su: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
                if tu as i64 != su { Err(format!("users mismatch: thunder={}, sqlite={}", tu, su)) } else { Ok(()) }
            })
            .build(),
    ]
}

#[test]
fn vs_sqlite_read() {
    let h = Harness::from_env();
    let baseline_path = PathBuf::from("perf/baseline.json");
    let artifact_dir = PathBuf::from("target/perf");
    let report = h.run(&scenarios(), &baseline_path, &artifact_dir);
    assert!(report.summary.loss == 0, "read scenarios have {} loss(es)", report.summary.loss);
    assert!(report.summary.failure == 0, "read scenarios have {} failure(s)", report.summary.failure);
}
