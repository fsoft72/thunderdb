//! ThunderDB vs SQLite — query-features I scenarios (SP4a).
//! Covers ORDER BY, IS NULL, multi-filter AND, OFFSET, and string EQ.

mod common;

use common::*;
#[allow(unused_imports)]
use thunderdb::{DirectDataAccess, Filter, Operator, Value};
use std::path::PathBuf;

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

/// Body string for post `i` — must match `build_blog_posts_q_fixtures`.
fn body_for(i: i64) -> String {
    format!("This is the body of post {}.  Topic discussion follows for several sentences.", i)
}

/// Slug string for post `i` — must match `build_blog_posts_q_fixtures`.
fn slug_for(i: i64) -> String { format!("post-{:08x}", i) }

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
