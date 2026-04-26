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
        // Q1. ORDER BY indexed ASC + LIMIT 100
        Scenario::new("Q1. ORDER BY indexed ASC + LIMIT 100", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_indexed_top_k("blog_posts_q", "id", 100, false).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY id LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_indexed_top_k("blog_posts_q", "id", 100, false).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY id LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q1 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q2. ORDER BY indexed DESC + LIMIT 100
        Scenario::new("Q2. ORDER BY indexed DESC + LIMIT 100", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_indexed_top_k("blog_posts_q", "id", 100, true).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY id DESC LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_indexed_top_k("blog_posts_q", "id", 100, true).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY id DESC LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q2 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q3. ORDER BY non-indexed (views) full sort + LIMIT 100
        Scenario::new("Q3. ORDER BY non-indexed full sort", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let _ = take_n(sort_rows_by_int(rows, 7, false), 100);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q ORDER BY views LIMIT 100").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 7, false), 100).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q ORDER BY views LIMIT 100)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q3 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q4. WHERE author_id = ? ORDER BY id DESC LIMIT 10
        Scenario::new("Q4. Top-K via ORDER BY indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))],
                    None, None).unwrap();
                let _ = take_n(sort_rows_by_int(rows, 0, true), 10);
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE author_id = 7 ORDER BY id DESC LIMIT 10").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let rows = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))],
                    None, None).unwrap();
                let t = take_n(sort_rows_by_int(rows, 0, true), 10).len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q WHERE author_id = 7 ORDER BY id DESC LIMIT 10)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q4 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q5. LIMIT 50 OFFSET 9000  (deep skip)
        Scenario::new("Q5. OFFSET deep skip", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], Some(50), Some(9000)).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q LIMIT 50 OFFSET 9000").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_with_limit(
                    "blog_posts_q", vec![], Some(50), Some(9000)).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT id FROM blog_posts_q LIMIT 50 OFFSET 9000)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q5 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q6. WHERE category IS NULL
        Scenario::new("Q6. IS NULL filter", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("category", Operator::IsNull)],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE category IS NULL").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("category", Operator::IsNull)]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE category IS NULL",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q6 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q7. WHERE slug = ? (indexed string EQ)
        Scenario::new("Q7. string EQ indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("slug", Operator::Equals(Value::varchar(slug_for(1234))))],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let target = slug_for(1234);
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE slug = ?1").unwrap();
                let _: Vec<i64> = st.query_map([&target], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("slug", Operator::Equals(Value::varchar(slug_for(1234))))]).unwrap();
                if t != 1 { Err(format!("Q7 row count: thunder={}, want 1", t)) } else { Ok(()) }
            })
            .build(),

        // Q8. WHERE body = ? (non-indexed string EQ)
        Scenario::new("Q8. string EQ non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![Filter::new("body", Operator::Equals(Value::varchar(body_for(1234))))],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let target = body_for(1234);
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q WHERE body = ?1").unwrap();
                let _: Vec<i64> = st.query_map([&target], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![Filter::new("body", Operator::Equals(Value::varchar(body_for(1234))))]).unwrap();
                if t != 1 { Err(format!("Q8 row count: thunder={}, want 1", t)) } else { Ok(()) }
            })
            .build(),

        // Q9. multi-filter AND mixed
        Scenario::new("Q9. multi-filter AND mixed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_limit(
                    "blog_posts_q",
                    vec![
                        Filter::new("author_id",    Operator::Equals(Value::Int64(7))),
                        Filter::new("category",     Operator::Equals(Value::varchar("review".to_string()))),
                        Filter::new("published_at", Operator::IsNotNull),
                    ],
                    None, None).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT * FROM blog_posts_q
                     WHERE author_id = 7 AND category = 'review' AND published_at IS NOT NULL").unwrap();
                let _: Vec<i64> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().count(
                    "blog_posts_q",
                    vec![
                        Filter::new("author_id",    Operator::Equals(Value::Int64(7))),
                        Filter::new("category",     Operator::Equals(Value::varchar("review".to_string()))),
                        Filter::new("published_at", Operator::IsNotNull),
                    ]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q
                     WHERE author_id = 7 AND category = 'review' AND published_at IS NOT NULL",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q9 row count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
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
