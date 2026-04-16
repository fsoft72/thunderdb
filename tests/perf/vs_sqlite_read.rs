//! ThunderDB vs SQLite — read-path scenarios, running through the harness.
//! Migrated from tests/integration/thunderdb_vs_sqlite_bench.rs.

mod common;

use common::*;
use thunderdb::{DirectDataAccess, Filter, Operator, Value};
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

        // 2. LIKE prefix on title (2000 hits)
        Scenario::new("2. LIKE prefix on title (2000 hits)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_projection(
                    "blog_posts",
                    vec![Filter::new("title", Operator::Like("Post about rust%".into()))],
                    None, None, Some(vec![0])).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare("SELECT id FROM blog_posts WHERE title LIKE 'Post about rust%'").unwrap();
                let _: Vec<i32> = st.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_with_projection(
                    "blog_posts",
                    vec![Filter::new("title", Operator::Like("Post about rust%".into()))],
                    None, None, Some(vec![0])).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE title LIKE 'Post about rust%'",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("LIKE prefix title: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 3. LIKE prefix on content (1 hit)
        Scenario::new("3. LIKE prefix on content (1 hit)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan_with_projection(
                    "blog_posts",
                    vec![Filter::new("content", Operator::Like("This is post 42 %".into()))],
                    None, None, Some(vec![0])).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare("SELECT id FROM blog_posts WHERE content LIKE 'This is post 42 %'").unwrap();
                let _: Vec<i32> = st.query_map([], |r| r.get(0)).unwrap().map(|r| r.unwrap()).collect();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_with_projection(
                    "blog_posts",
                    vec![Filter::new("content", Operator::Like("This is post 42 %".into()))],
                    None, None, Some(vec![0])).unwrap().len();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE content LIKE 'This is post 42 %'",
                    [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("LIKE prefix content: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 4. Indexed EQ: posts by author_id=1
        Scenario::new("4. Indexed EQ: posts by author_id=1", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int32(1)))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE author_id = 1", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int32(1)))]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE author_id = 1", [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("EQ author_id: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 5. Post + comments (indexed)
        Scenario::new("5. Post + comments (indexed)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().scan("blog_posts",
                    vec![Filter::new("id", Operator::Equals(Value::Int32(500)))]).unwrap();
                let _ = f.thunder_mut().scan("comments",
                    vec![Filter::new("post_id", Operator::Equals(Value::Int32(500)))]).unwrap();
            })
            .sqlite(|f| {
                let _: String = f.sqlite().query_row(
                    "SELECT title FROM blog_posts WHERE id = ?1", [500], |r| r.get(0)).unwrap();
                let mut st = f.sqlite().prepare("SELECT id FROM comments WHERE post_id = ?1").unwrap();
                let _: usize = st.query_map([500], |r| r.get::<_, i32>(0)).unwrap().count();
            })
            .assert(|_f| Ok(()))
            .build(),

        // 6. 3-table join
        Scenario::new("6. 3-table join (post+comments+users)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let posts = f.thunder_mut().scan("blog_posts",
                    vec![Filter::new("id", Operator::Equals(Value::Int32(1234)))]).unwrap();
                let post = &posts[0];
                let author_id = post.values[1].clone();
                let _ = f.thunder_mut().scan("users",
                    vec![Filter::new("id", Operator::Equals(author_id))]).unwrap();
                let comments = f.thunder_mut().scan("comments",
                    vec![Filter::new("post_id", Operator::Equals(Value::Int32(1234)))]).unwrap();
                let ids: Vec<Value> = comments.iter().map(|c| c.values[2].clone()).collect();
                let unique: Vec<Value> = {
                    let mut seen = std::collections::HashSet::new();
                    ids.iter().filter(|v| seen.insert(format!("{:?}", v))).cloned().collect()
                };
                let _ = f.thunder_mut().scan("users", vec![Filter::new("id", Operator::In(unique))]).unwrap();
            })
            .sqlite(|f| {
                let _: (String, String) = f.sqlite().query_row(
                    "SELECT bp.title, u.name FROM blog_posts bp JOIN users u ON u.id = bp.author_id WHERE bp.id = ?1",
                    [1234], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                let mut st = f.sqlite().prepare(
                    "SELECT c.text, u.name FROM comments c JOIN users u ON u.id = c.author_id WHERE c.post_id = ?1").unwrap();
                let _: Vec<(String, String)> = st.query_map([1234], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect();
            })
            .assert(|_f| Ok(()))
            .build(),

        // 7. Recent 20 posts + comment counts
        Scenario::new("7. Recent 20 posts + comment counts", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let post_count = f.tier.post_count();
                let threshold = (post_count - 20) as i32;
                let recent = f.thunder_mut().scan("blog_posts",
                    vec![Filter::new("id", Operator::GreaterThan(Value::Int32(threshold)))]).unwrap();
                for post in &recent {
                    let pid = post.values[0].clone();
                    let _ = f.thunder_mut().count("comments",
                        vec![Filter::new("post_id", Operator::Equals(pid))]).unwrap();
                }
            })
            .sqlite(|f| {
                let post_count = f.tier.post_count();
                let threshold = (post_count - 20) as i32;
                let mut st = f.sqlite().prepare(
                    "SELECT bp.id, COUNT(c.id) FROM blog_posts bp LEFT JOIN comments c ON c.post_id = bp.id WHERE bp.id > ?1 GROUP BY bp.id").unwrap();
                let _: Vec<(i32, i64)> = st.query_map([threshold], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect();
            })
            .assert(|_f| Ok(()))
            .build(),

        // 8. IN (1, 3) on author_id
        Scenario::new("8. IN (1, 3) on author_id", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("author_id", Operator::In(vec![Value::Int32(1), Value::Int32(3)]))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE author_id IN (1, 3)", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("author_id", Operator::In(vec![Value::Int32(1), Value::Int32(3)]))]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE author_id IN (1, 3)", [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("IN: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 9. BETWEEN 5000..5100 on id
        Scenario::new("9. BETWEEN 5000..5100 on id (indexed)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("id", Operator::Between(Value::Int32(5000), Value::Int32(5100)))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE id BETWEEN 5000 AND 5100", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("blog_posts",
                    vec![Filter::new("id", Operator::Between(Value::Int32(5000), Value::Int32(5100)))]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM blog_posts WHERE id BETWEEN 5000 AND 5100", [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("BETWEEN: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 10. Full scan 10k posts
        Scenario::new("10. Full table scan (10k posts)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().for_each_row("blog_posts", vec![], Some(vec![0]), |_| {}).unwrap();
            })
            .sqlite(|f| {
                let mut st = f.sqlite().prepare("SELECT id FROM blog_posts").unwrap();
                let _: usize = st.query_map([], |r| r.get::<_, i32>(0)).unwrap().count();
            })
            .assert(|f| {
                let t = f.thunder_mut().scan_with_projection("blog_posts", vec![], None, None, Some(vec![0])).unwrap().len();
                let s: i64 = f.sqlite().query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("full scan: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
            })
            .build(),

        // 11. COUNT WHERE author_id=2 (indexed)
        Scenario::new("11. COUNT WHERE author_id=2 (indexed)", "read")
            .setup(|t, m| build_blog_fixtures(t, m))
            .thunder(|f| {
                let _ = f.thunder_mut().count("comments",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int32(2)))]).unwrap();
            })
            .sqlite(|f| {
                let _: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM comments WHERE author_id = 2", [], |r| r.get(0)).unwrap();
            })
            .assert(|f| {
                let t = f.thunder_mut().count("comments",
                    vec![Filter::new("author_id", Operator::Equals(Value::Int32(2)))]).unwrap();
                let s: i64 = f.sqlite().query_row(
                    "SELECT COUNT(*) FROM comments WHERE author_id = 2", [], |r| r.get(0)).unwrap();
                if t as i64 != s { Err(format!("COUNT WHERE: thunder={}, sqlite={}", t, s)) } else { Ok(()) }
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
    // Parent-goal assertion (SP2 and beyond).
    assert!(report.summary.failure == 0, "read scenarios have {} failure(s)", report.summary.failure);
    assert!(report.summary.loss == 0, "read scenarios have {} loss(es)", report.summary.loss);
}
