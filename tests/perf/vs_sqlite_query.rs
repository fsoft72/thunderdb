//! ThunderDB vs SQLite — query-features I scenarios (SP4a).
//! Covers ORDER BY, IS NULL, multi-filter AND, OFFSET, and string EQ.

mod common;

use common::*;
#[allow(unused_imports)]
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
