/// ThunderDB vs SQLite3 performance comparison benchmark.
///
/// Mirrors the blog benchmark suite (5 users, 10 000 posts, ~30 000 comments)
/// and runs the same operations on both engines, printing a side-by-side
/// timing table at the end.

use rusqlite::Connection;
use thunderdb::{Database, DirectDataAccess, Filter, Operator, Value};
use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};

use std::fs;
use std::time::{Duration, Instant};

const THUNDER_DIR: &str = "/tmp/thunderdb_vs_sqlite_bench";
const SQLITE_PATH: &str = "/tmp/thunderdb_vs_sqlite_bench.db";
const USER_COUNT: usize = 5;
const POST_COUNT: usize = 10_000;

/// Deterministic 2–4 comments per post based on post index.
fn comments_for_post(post_idx: usize) -> usize {
    2 + (post_idx % 3)
}

/// Total expected comments across all posts.
fn total_comments() -> usize {
    (1..=POST_COUNT).map(comments_for_post).sum()
}

const TOPICS: [&str; 5] = ["rust", "database", "performance", "testing", "design"];

// ─── Result collector ───────────────────────────────────────────────────────

struct BenchResult {
    name: String,
    thunder_dur: Duration,
    sqlite_dur: Duration,
}

impl BenchResult {
    fn new(name: &str, thunder: Duration, sqlite: Duration) -> Self {
        Self {
            name: name.to_string(),
            thunder_dur: thunder,
            sqlite_dur: sqlite,
        }
    }
}

/// Print a formatted comparison table.
fn print_results(results: &[BenchResult]) {
    println!();
    println!("{:<45} {:>14} {:>14} {:>10}", "Benchmark", "ThunderDB", "SQLite3", "Ratio");
    println!("{}", "-".repeat(87));
    for r in results {
        let thunder_us = r.thunder_dur.as_micros();
        let sqlite_us = r.sqlite_dur.as_micros();
        let ratio = if sqlite_us > 0 {
            thunder_us as f64 / sqlite_us as f64
        } else {
            f64::INFINITY
        };
        let indicator = if ratio < 1.0 { " <-- Thunder wins" } else { "" };
        println!(
            "{:<45} {:>10} µs {:>10} µs {:>9.2}x{}",
            r.name, thunder_us, sqlite_us, ratio, indicator
        );
    }
    println!("{}", "-".repeat(87));
    println!();
}

// ─── ThunderDB setup ────────────────────────────────────────────────────────

fn setup_thunderdb() -> Database {
    let _ = fs::remove_dir_all(THUNDER_DIR);
    let mut db = Database::open(THUNDER_DIR).expect("open thunderdb");

    // Users
    let users: Vec<Vec<Value>> = (1..=USER_COUNT)
        .map(|i| {
            vec![
                Value::Int32(i as i32),
                Value::varchar(format!("user_{}", i)),
                Value::varchar(format!("user_{}@example.com", i)),
            ]
        })
        .collect();
    db.insert_batch("users", users).unwrap();

    {
        let table = db.get_table_mut("users").unwrap();
        table
            .set_schema(TableSchema {
                columns: vec![
                    ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
                    ColumnInfo { name: "email".into(), data_type: "VARCHAR".into() },
                ],
            })
            .unwrap();
        table.create_index("id").unwrap();
    }

    // Blog posts
    let posts: Vec<Vec<Value>> = (1..=POST_COUNT)
        .map(|i| {
            let author_id = (i % USER_COUNT) + 1;
            let topic = TOPICS[i % TOPICS.len()];
            vec![
                Value::Int32(i as i32),
                Value::Int32(author_id as i32),
                Value::varchar(format!("Post about {} #{}", topic, i)),
                Value::varchar(format!(
                    "This is post {} discussing {} in depth. ThunderDB makes {} easy.",
                    i, topic, topic
                )),
            ]
        })
        .collect();
    db.insert_batch("blog_posts", posts).unwrap();

    {
        let table = db.get_table_mut("blog_posts").unwrap();
        table
            .set_schema(TableSchema {
                columns: vec![
                    ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "title".into(), data_type: "VARCHAR".into() },
                    ColumnInfo { name: "content".into(), data_type: "VARCHAR".into() },
                ],
            })
            .unwrap();
        table.create_index("id").unwrap();
        table.create_index("author_id").unwrap();
        table.create_index("title").unwrap();
    }

    // Comments
    let mut comment_rows: Vec<Vec<Value>> = Vec::new();
    let mut comment_id = 1i32;
    for post_idx in 1..=POST_COUNT {
        let n = comments_for_post(post_idx);
        for c in 0..n {
            let commenter_id = ((post_idx + c) % USER_COUNT) + 1;
            comment_rows.push(vec![
                Value::Int32(comment_id),
                Value::Int32(post_idx as i32),
                Value::Int32(commenter_id as i32),
                Value::varchar(format!("Comment {} on post {}", c + 1, post_idx)),
            ]);
            comment_id += 1;
        }
    }
    db.insert_batch("comments", comment_rows).unwrap();

    {
        let table = db.get_table_mut("comments").unwrap();
        table
            .set_schema(TableSchema {
                columns: vec![
                    ColumnInfo { name: "id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "post_id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "author_id".into(), data_type: "INT32".into() },
                    ColumnInfo { name: "text".into(), data_type: "VARCHAR".into() },
                ],
            })
            .unwrap();
        table.create_index("post_id").unwrap();
        table.create_index("author_id").unwrap();
    }

    db
}

// ─── SQLite setup ───────────────────────────────────────────────────────────

fn setup_sqlite() -> Connection {
    let _ = fs::remove_file(SQLITE_PATH);
    let conn = Connection::open(SQLITE_PATH).expect("open sqlite");

    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
        .unwrap();

    conn.execute_batch(
        "CREATE TABLE users (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            email TEXT NOT NULL
         );
         CREATE TABLE blog_posts (
            id        INTEGER PRIMARY KEY,
            author_id INTEGER NOT NULL,
            title     TEXT NOT NULL,
            content   TEXT NOT NULL
         );
         CREATE INDEX idx_posts_author ON blog_posts(author_id);
         CREATE TABLE comments (
            id        INTEGER PRIMARY KEY,
            post_id   INTEGER NOT NULL,
            author_id INTEGER NOT NULL,
            text      TEXT NOT NULL
         );
         CREATE INDEX idx_comments_post   ON comments(post_id);
         CREATE INDEX idx_comments_author ON comments(author_id);
         CREATE INDEX idx_posts_title ON blog_posts(title);",
    )
    .unwrap();

    // Users
    {
        let mut stmt = conn
            .prepare("INSERT INTO users (id, name, email) VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 1..=USER_COUNT {
            stmt.execute(rusqlite::params![
                i as i32,
                format!("user_{}", i),
                format!("user_{}@example.com", i),
            ])
            .unwrap();
        }
    }

    // Blog posts (batch in a transaction)
    {
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO blog_posts (id, author_id, title, content) VALUES (?1, ?2, ?3, ?4)")
                .unwrap();
            for i in 1..=POST_COUNT {
                let author_id = (i % USER_COUNT) + 1;
                let topic = TOPICS[i % TOPICS.len()];
                stmt.execute(rusqlite::params![
                    i as i32,
                    author_id as i32,
                    format!("Post about {} #{}", topic, i),
                    format!(
                        "This is post {} discussing {} in depth. ThunderDB makes {} easy.",
                        i, topic, topic
                    ),
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    // Comments (batch in a transaction)
    {
        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx
                .prepare("INSERT INTO comments (id, post_id, author_id, text) VALUES (?1, ?2, ?3, ?4)")
                .unwrap();
            let mut comment_id = 1i32;
            for post_idx in 1..=POST_COUNT {
                let n = comments_for_post(post_idx);
                for c in 0..n {
                    let commenter_id = ((post_idx + c) % USER_COUNT) + 1;
                    stmt.execute(rusqlite::params![
                        comment_id,
                        post_idx as i32,
                        commenter_id as i32,
                        format!("Comment {} on post {}", c + 1, post_idx),
                    ])
                    .unwrap();
                    comment_id += 1;
                }
            }
        }
        tx.commit().unwrap();
    }

    conn
}

// ─── Benchmark helpers ──────────────────────────────────────────────────────

/// Time a closure, returning (duration, result).
fn timed<F, R>(f: F) -> (Duration, R)
where
    F: FnOnce() -> R,
{
    let t = Instant::now();
    let r = f();
    (t.elapsed(), r)
}

// ─── Main test ──────────────────────────────────────────────────────────────

#[test]
fn thunderdb_vs_sqlite_benchmark() {
    println!("\n========== Setting up databases ==========\n");

    let (thunder_setup, mut tdb) = timed(setup_thunderdb);
    let (sqlite_setup, sdb) = timed(setup_sqlite);

    let mut results: Vec<BenchResult> = Vec::new();
    results.push(BenchResult::new(
        "Setup (schema + insert + index)",
        thunder_setup,
        sqlite_setup,
    ));

    // ── 1. COUNT(*) ─────────────────────────────────────────────────────
    {
        let (td, tc) = timed(|| {
            let u = tdb.count("users", vec![]).unwrap();
            let p = tdb.count("blog_posts", vec![]).unwrap();
            let c = tdb.count("comments", vec![]).unwrap();
            (u, p, c)
        });
        assert_eq!(tc, (USER_COUNT, POST_COUNT, total_comments()));

        let (sd, sc) = timed(|| {
            let u: i64 = sdb.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap();
            let p: i64 = sdb.query_row("SELECT COUNT(*) FROM blog_posts", [], |r| r.get(0)).unwrap();
            let c: i64 = sdb.query_row("SELECT COUNT(*) FROM comments", [], |r| r.get(0)).unwrap();
            (u as usize, p as usize, c as usize)
        });
        assert_eq!(sc, (USER_COUNT, POST_COUNT, total_comments()));

        results.push(BenchResult::new("1. COUNT(*) all three tables", td, sd));
    }

    // ── 2. Full-text search: LIKE prefix on title ───────────────────────
    {
        let (td, tcount) = timed(|| {
            let rows = tdb
                .scan_with_projection(
                    "blog_posts",
                    vec![Filter::new("title", Operator::Like("Post about rust%".into()))],
                    None, None,
                    Some(vec![0]),
                )
                .unwrap();
            rows.len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb
                .prepare("SELECT id FROM blog_posts WHERE title LIKE 'Post about rust%'")
                .unwrap();
            let rows: Vec<i32> = stmt
                .query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            rows.len()
        });

        assert_eq!(tcount, POST_COUNT / TOPICS.len());
        assert_eq!(scount, POST_COUNT / TOPICS.len());
        results.push(BenchResult::new("2. LIKE prefix on title (2000 hits)", td, sd));
    }

    // ── 3. LIKE prefix single hit on content ────────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.scan_with_projection(
                "blog_posts",
                vec![Filter::new("content", Operator::Like("This is post 42 %".into()))],
                None, None,
                Some(vec![0]),
            )
            .unwrap()
            .len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb
                .prepare("SELECT id FROM blog_posts WHERE content LIKE 'This is post 42 %'")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, i32>(0))
                .unwrap()
                .count()
        });

        assert_eq!(tcount, 1);
        assert_eq!(scount, 1);
        results.push(BenchResult::new("3. LIKE prefix on content (1 hit)", td, sd));
    }

    // ── 4. Indexed equality: posts by author_id ─────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.scan_with_projection(
                "blog_posts",
                vec![Filter::new("author_id", Operator::Equals(Value::Int32(1)))],
                None, None,
                Some(vec![0]),
            )
            .unwrap()
            .len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb
                .prepare("SELECT id FROM blog_posts WHERE author_id = 1")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, i32>(0))
                .unwrap()
                .count()
        });

        let expected = POST_COUNT / USER_COUNT;
        assert_eq!(tcount, expected);
        assert_eq!(scount, expected);
        results.push(BenchResult::new("4. Indexed EQ: posts by author_id=1", td, sd));
    }

    // ── 5. Single post + comments (indexed join) ────────────────────────
    {
        let post_id = 500i32;

        let (td, _) = timed(|| {
            let posts = tdb
                .scan(
                    "blog_posts",
                    vec![Filter::new("id", Operator::Equals(Value::Int32(post_id)))],
                )
                .unwrap();
            assert_eq!(posts.len(), 1);

            let comments = tdb
                .scan(
                    "comments",
                    vec![Filter::new("post_id", Operator::Equals(Value::Int32(post_id)))],
                )
                .unwrap();
            assert_eq!(comments.len(), comments_for_post(post_id as usize));
        });

        let (sd, _) = timed(|| {
            let title: String = sdb
                .query_row(
                    "SELECT title FROM blog_posts WHERE id = ?1",
                    [post_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(!title.is_empty());

            let mut stmt = sdb
                .prepare("SELECT id FROM comments WHERE post_id = ?1")
                .unwrap();
            let n: usize = stmt.query_map([post_id], |r| r.get::<_, i32>(0)).unwrap().count();
            assert_eq!(n, comments_for_post(post_id as usize));
        });

        results.push(BenchResult::new("5. Post + comments join (indexed)", td, sd));
    }

    // ── 6. 3-table join: post + comments + user names ───────────────────
    {
        let post_id = 1234i32;

        let (td, _) = timed(|| {
            let posts = tdb
                .scan(
                    "blog_posts",
                    vec![Filter::new("id", Operator::Equals(Value::Int32(post_id)))],
                )
                .unwrap();
            let post = &posts[0];
            let post_author_id = &post.values[1];

            let authors = tdb
                .scan(
                    "users",
                    vec![Filter::new("id", Operator::Equals(post_author_id.clone()))],
                )
                .unwrap();
            assert_eq!(authors.len(), 1);

            let comments = tdb
                .scan(
                    "comments",
                    vec![Filter::new("post_id", Operator::Equals(Value::Int32(post_id)))],
                )
                .unwrap();

            let commenter_ids: Vec<Value> = comments.iter().map(|c| c.values[2].clone()).collect();
            let unique_ids: Vec<Value> = {
                let mut seen = std::collections::HashSet::new();
                commenter_ids
                    .iter()
                    .filter(|v| seen.insert(format!("{:?}", v)))
                    .cloned()
                    .collect()
            };
            let _commenters = tdb
                .scan(
                    "users",
                    vec![Filter::new("id", Operator::In(unique_ids))],
                )
                .unwrap();
        });

        let (sd, _) = timed(|| {
            let _: (String, String) = sdb
                .query_row(
                    "SELECT bp.title, u.name
                     FROM blog_posts bp
                     JOIN users u ON u.id = bp.author_id
                     WHERE bp.id = ?1",
                    [post_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();

            let mut stmt = sdb
                .prepare(
                    "SELECT c.text, u.name
                     FROM comments c
                     JOIN users u ON u.id = c.author_id
                     WHERE c.post_id = ?1",
                )
                .unwrap();
            let rows: Vec<(String, String)> = stmt
                .query_map([post_id], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(rows.len(), comments_for_post(post_id as usize));
        });

        results.push(BenchResult::new("6. 3-table join (post+comments+users)", td, sd));
    }

    // ── 7. Recent posts page + comment counts ───────────────────────────
    {
        let threshold = (POST_COUNT - 20) as i32;

        let (td, _) = timed(|| {
            let recent = tdb
                .scan(
                    "blog_posts",
                    vec![Filter::new("id", Operator::GreaterThan(Value::Int32(threshold)))],
                )
                .unwrap();
            assert_eq!(recent.len(), 20);

            let mut total = 0usize;
            for post in &recent {
                let pid = post.values[0].clone();
                total += tdb
                    .count("comments", vec![Filter::new("post_id", Operator::Equals(pid))])
                    .unwrap();
            }
            assert!(total >= 20 * 2 && total <= 20 * 4);
        });

        let (sd, _) = timed(|| {
            let mut stmt = sdb
                .prepare(
                    "SELECT bp.id, COUNT(c.id)
                     FROM blog_posts bp
                     LEFT JOIN comments c ON c.post_id = bp.id
                     WHERE bp.id > ?1
                     GROUP BY bp.id",
                )
                .unwrap();
            let rows: Vec<(i32, i64)> = stmt
                .query_map([threshold], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(rows.len(), 20);
        });

        results.push(BenchResult::new("7. Recent 20 posts + comment counts", td, sd));
    }

    // ── 8. IN operator: posts by authors (1, 3) ─────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.scan_with_projection(
                "blog_posts",
                vec![Filter::new(
                    "author_id",
                    Operator::In(vec![Value::Int32(1), Value::Int32(3)]),
                )],
                None, None,
                Some(vec![0]),
            )
            .unwrap()
            .len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb
                .prepare("SELECT id FROM blog_posts WHERE author_id IN (1, 3)")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, i32>(0)).unwrap().count()
        });

        let expected = 2 * (POST_COUNT / USER_COUNT);
        assert_eq!(tcount, expected);
        assert_eq!(scount, expected);
        results.push(BenchResult::new("8. IN (1, 3) on author_id", td, sd));
    }

    // ── 9. Range scan: BETWEEN on id ────────────────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.scan_with_projection(
                "blog_posts",
                vec![Filter::new(
                    "id",
                    Operator::Between(Value::Int32(5000), Value::Int32(5100)),
                )],
                None, None,
                Some(vec![0]),
            )
            .unwrap()
            .len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb
                .prepare("SELECT id FROM blog_posts WHERE id BETWEEN 5000 AND 5100")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, i32>(0)).unwrap().count()
        });

        assert_eq!(tcount, 101);
        assert_eq!(scount, 101);
        results.push(BenchResult::new("9. BETWEEN 5000..5100 on id (indexed)", td, sd));
    }

    // ── 10. Full table scan (all posts) ─────────────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.scan_with_projection("blog_posts", vec![], None, None, Some(vec![0]))
                .unwrap().len()
        });

        let (sd, scount) = timed(|| {
            let mut stmt = sdb.prepare("SELECT id FROM blog_posts").unwrap();
            stmt.query_map([], |r| r.get::<_, i32>(0)).unwrap().count()
        });

        assert_eq!(tcount, POST_COUNT);
        assert_eq!(scount, POST_COUNT);
        results.push(BenchResult::new("10. Full table scan (10k posts)", td, sd));
    }

    // ── 11. Aggregation: COUNT with WHERE ───────────────────────────────
    {
        let (td, tcount) = timed(|| {
            tdb.count(
                "comments",
                vec![Filter::new("author_id", Operator::Equals(Value::Int32(2)))],
            )
            .unwrap()
        });

        let (sd, scount) = timed(|| {
            sdb.query_row(
                "SELECT COUNT(*) FROM comments WHERE author_id = 2",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap() as usize
        });

        assert_eq!(tcount, scount);
        results.push(BenchResult::new("11. COUNT WHERE author_id=2 (indexed)", td, sd));
    }

    // ── Print results ───────────────────────────────────────────────────
    print_results(&results);

    // ── Cleanup ─────────────────────────────────────────────────────────
    let _ = fs::remove_dir_all(THUNDER_DIR);
    let _ = fs::remove_file(SQLITE_PATH);

    println!("=== ThunderDB vs SQLite benchmark complete ===\n");
}
