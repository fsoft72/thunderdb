//! ThunderDB vs SQLite — query-features II scenarios (SP4b).
//! Covers GROUP BY, scalar aggregates (COUNT/SUM/AVG/MIN/MAX), and DISTINCT.

mod common;

use common::*;
#[allow(unused_imports)]
use thunderdb::{Aggregate, DirectDataAccess, Filter, Operator, Value};
use std::path::PathBuf;

fn scenarios() -> Vec<Scenario> {
    vec![
        // Q10. COUNT(*) full table
        Scenario::new("Q10. COUNT(*) full table", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q10 count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q11. COUNT(*) WHERE author_id = 7  (indexed)
        Scenario::new("Q11. COUNT(*) WHERE indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count],
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Count],
                    vec![Filter::new("author_id", Operator::Equals(Value::Int64(7)))]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q WHERE author_id = 7", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q11 count: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q12. SUM(views)  — non-indexed full scan
        Scenario::new("Q12. SUM int non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Sum("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Sum("views".into())], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Int64(n) => n, _ => -1 };
                let s: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t != s { Err(format!("Q12 sum: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q13. AVG(views) — non-indexed full scan
        Scenario::new("Q13. AVG int non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Avg("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: f64 = f.sqlite().query_row(
                    "SELECT AVG(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![], vec![Aggregate::Avg("views".into())], vec![]).unwrap();
                let t = match r[0].aggs[0] { Value::Float64(x) => x, _ => f64::NAN };
                let s: f64 = f.sqlite().query_row(
                    "SELECT AVG(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if (t - s).abs() > 1e-6 {
                    Err(format!("Q13 avg: thunder={}, sqlite={}", t, s))
                } else { Ok(()) }
            })
            .build(),

        // Q14. MIN/MAX over indexed PK
        Scenario::new("Q14. MIN/MAX indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("id".into()), Aggregate::Max("id".into())],
                    vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(id), MAX(id) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("id".into()), Aggregate::Max("id".into())], vec![]).unwrap();
                let (tmin, tmax) = match (&r[0].aggs[0], &r[0].aggs[1]) {
                    (Value::Int64(a), Value::Int64(b)) => (*a, *b), _ => (-1, -1),
                };
                let (smin, smax): (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(id), MAX(id) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                if (tmin, tmax) != (smin, smax) {
                    Err(format!("Q14 minmax: thunder=({},{}), sqlite=({},{})", tmin, tmax, smin, smax))
                } else { Ok(()) }
            })
            .build(),

        // Q15. MIN/MAX over non-indexed views
        Scenario::new("Q15. MIN/MAX non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("views".into()), Aggregate::Max("views".into())],
                    vec![]).unwrap();
            })
            .sqlite(|f| {
                let _: (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(views), MAX(views) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec![],
                    vec![Aggregate::Min("views".into()), Aggregate::Max("views".into())], vec![]).unwrap();
                let (tmin, tmax) = match (&r[0].aggs[0], &r[0].aggs[1]) {
                    (Value::Int64(a), Value::Int64(b)) => (*a, *b), _ => (-1, -1),
                };
                let (smin, smax): (i64, i64) = f.sqlite().query_row(
                    "SELECT MIN(views), MAX(views) FROM blog_posts_q", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                if (tmin, tmax) != (smin, smax) {
                    Err(format!("Q15 minmax: thunder=({},{}), sqlite=({},{})", tmin, tmax, smin, smax))
                } else { Ok(()) }
            })
            .build(),

        // Q16. GROUP BY indexed low-card
        Scenario::new("Q16. GROUP BY indexed low-card", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT author_id, COUNT(*) FROM blog_posts_q GROUP BY author_id").unwrap();
                let _: Vec<(i64, i64)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i64 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT author_id FROM blog_posts_q GROUP BY author_id)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total {
                    Err(format!("Q16 groups: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),

        // Q17. GROUP BY non-indexed low-card (category, includes NULL group)
        Scenario::new("Q17. GROUP BY non-indexed low-card", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["category".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT category, COUNT(*) FROM blog_posts_q GROUP BY category").unwrap();
                let _: Vec<(Option<String>, i64)> = st.query_map([],
                    |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["category".into()],
                    vec![Aggregate::Count], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i64 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT category FROM blog_posts_q GROUP BY category)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total {
                    Err(format!("Q17 groups: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),

        // Q18. GROUP BY indexed + SUM
        Scenario::new("Q18. GROUP BY + SUM", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Sum("views".into())], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT author_id, SUM(views) FROM blog_posts_q GROUP BY author_id").unwrap();
                let _: Vec<(i64, i64)> = st.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap()
                    .map(|x| x.unwrap()).collect();
            })
            .assert(|f| {
                let r = f.thunder_mut().aggregate(
                    "blog_posts_q", vec!["author_id".into()],
                    vec![Aggregate::Sum("views".into())], vec![]).unwrap();
                let t_groups = r.len();
                let t_total: i128 = r.iter().map(|row| match row.aggs[0] {
                    Value::Int64(n) => n as i128, _ => 0,
                }).sum();
                let s_groups: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT author_id FROM blog_posts_q GROUP BY author_id)",
                    [], |r| r.get(0)).unwrap();
                let s_total: i64 = f.sqlite().query_row(
                    "SELECT SUM(views) FROM blog_posts_q", [], |r| r.get(0)).unwrap();
                if t_groups as i64 != s_groups || t_total != s_total as i128 {
                    Err(format!("Q18 groups+sum: thunder=({},{}), sqlite=({},{})",
                        t_groups, t_total, s_groups, s_total))
                } else { Ok(()) }
            })
            .build(),

        // Q19. DISTINCT high-card indexed (slug)
        Scenario::new("Q19. DISTINCT high-card indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["slug".into()], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT DISTINCT slug FROM blog_posts_q").unwrap();
                let _: Vec<String> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["slug".into()], vec![]).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT DISTINCT slug FROM blog_posts_q)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q19 distinct: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // Q20. DISTINCT low-card non-indexed (category)
        Scenario::new("Q20. DISTINCT low-card non-indexed", "query")
            .setup(|t, m| build_blog_posts_q_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["category".into()], vec![]).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare(
                    "SELECT DISTINCT category FROM blog_posts_q").unwrap();
                let _: Vec<Option<String>> = st.query_map([], |r| r.get(0)).unwrap()
                    .map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().distinct(
                    "blog_posts_q", vec!["category".into()], vec![]).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM (SELECT DISTINCT category FROM blog_posts_q)",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("Q20 distinct: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),
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
