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
