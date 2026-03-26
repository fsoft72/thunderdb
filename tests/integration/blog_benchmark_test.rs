/// Blog benchmark suite: 5 users, 10 000 posts, 2–4 comments per post.
///
/// Exercises bulk inserts, full-text search (LIKE), index-accelerated
/// lookups, and manual joins across the three tables.
///
/// All foreign-key and primary-key columns are indexed to exercise
/// the B-tree with both unique and high-duplicate keys.

use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
use std::collections::HashMap;
use std::fs;
use std::time::Instant;

const DATA_DIR: &str = "/tmp/thunderdb_blog_benchmark";
const USER_COUNT: usize = 5;
const POST_COUNT: usize = 10_000;

/// Deterministic 2–4 comments per post based on post index.
fn comments_for_post(post_idx: usize) -> usize {
    2 + (post_idx % 3) // yields 2, 3, 4, 2, 3, 4, …
}

/// Build the database with all three tables populated.
fn setup_db() -> Database {
    let _ = fs::remove_dir_all(DATA_DIR);
    let mut db = Database::open(DATA_DIR).expect("open db");

    // ── Users ──────────────────────────────────────────────────────────
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
                    ColumnInfo { name: "id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
                    ColumnInfo { name: "email".to_string(), data_type: "VARCHAR".to_string() },
                ],
            })
            .unwrap();
        table.create_index("id").unwrap();
    }

    // ── Blog posts ─────────────────────────────────────────────────────
    let topics = ["rust", "database", "performance", "testing", "design"];

    let posts: Vec<Vec<Value>> = (1..=POST_COUNT)
        .map(|i| {
            let author_id = (i % USER_COUNT) + 1;
            let topic = topics[i % topics.len()];
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
                    ColumnInfo { name: "id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "author_id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "title".to_string(), data_type: "VARCHAR".to_string() },
                    ColumnInfo { name: "content".to_string(), data_type: "VARCHAR".to_string() },
                ],
            })
            .unwrap();
        table.create_index("id").unwrap();
        table.create_index("author_id").unwrap();
    }

    // ── Comments ───────────────────────────────────────────────────────
    let mut comment_rows: Vec<Vec<Value>> = Vec::new();
    let mut comment_id = 1i32;
    for post_idx in 1..=POST_COUNT {
        let n_comments = comments_for_post(post_idx);
        for c in 0..n_comments {
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
                    ColumnInfo { name: "id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "post_id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "author_id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "text".to_string(), data_type: "VARCHAR".to_string() },
                ],
            })
            .unwrap();
        table.create_index("post_id").unwrap();
        table.create_index("author_id").unwrap();
    }

    db
}

#[test]
fn test_blog_benchmark_suite() {
    let mut db = setup_db();

    // ── 1. Table counts ────────────────────────────────────────────────
    println!("\n=== 1. Table counts ===");

    let user_count = db.count("users", vec![]).unwrap();
    assert_eq!(user_count, USER_COUNT);

    let post_count = db.count("blog_posts", vec![]).unwrap();
    assert_eq!(post_count, POST_COUNT);

    let expected_comments: usize = (1..=POST_COUNT).map(comments_for_post).sum();
    let comment_count = db.count("comments", vec![]).unwrap();
    assert_eq!(comment_count, expected_comments);
    println!("users: {}, posts: {}, comments: {}", user_count, post_count, comment_count);

    // ── 2. Full-text search: title prefix ──────────────────────────────
    println!("\n=== 2. Full-text search: title LIKE prefix ===");

    let t = Instant::now();
    let rust_posts = db.scan(
        "blog_posts",
        vec![Filter::new("title", Operator::Like("Post about rust%".to_string()))],
    ).unwrap();
    let elapsed = t.elapsed();

    let expected = POST_COUNT / topics_count();
    assert_eq!(rust_posts.len(), expected);
    for row in &rust_posts {
        let title = row.values[2].to_string();
        assert!(title.starts_with("Post about rust"), "bad match: {}", title);
    }
    println!("{} hits in {:?}", rust_posts.len(), elapsed);

    // ── 3. Full-text search: content prefix (single match) ─────────────
    println!("\n=== 3. Full-text search: content LIKE prefix (single hit) ===");

    let t = Instant::now();
    let results = db.scan(
        "blog_posts",
        vec![Filter::new("content", Operator::Like("This is post 42 %".to_string()))],
    ).unwrap();
    let elapsed = t.elapsed();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], Value::Int32(42));
    println!("{} hit in {:?}", results.len(), elapsed);

    // ── 4. Posts by author (indexed, high-duplicate key) ──────────────
    println!("\n=== 4. Posts by author_id=1 (indexed) ===");

    let t = Instant::now();
    let user1_posts = db.scan(
        "blog_posts",
        vec![Filter::new("author_id", Operator::Equals(Value::Int32(1)))],
    ).unwrap();
    let elapsed = t.elapsed();

    let expected = POST_COUNT / USER_COUNT;
    assert_eq!(user1_posts.len(), expected);
    for row in &user1_posts {
        assert_eq!(row.values[1], Value::Int32(1));
    }
    println!("{} rows in {:?}", user1_posts.len(), elapsed);

    // ── 5. Single post + comments (indexed join) ───────────────────────
    println!("\n=== 5. Join: post 500 + comments (indexed) ===");

    let post_id = 500;
    let t = Instant::now();

    let posts = db.scan(
        "blog_posts",
        vec![Filter::new("id", Operator::Equals(Value::Int32(post_id)))],
    ).unwrap();
    assert_eq!(posts.len(), 1);

    let comments = db.scan(
        "comments",
        vec![Filter::new("post_id", Operator::Equals(Value::Int32(post_id)))],
    ).unwrap();
    let elapsed = t.elapsed();

    let expected_comments = comments_for_post(post_id as usize);
    assert_eq!(comments.len(), expected_comments);
    assert_eq!(posts[0].values[0], Value::Int32(post_id));
    for c in &comments {
        assert_eq!(c.values[1], Value::Int32(post_id));
    }
    println!("post {} + {} comments in {:?}", post_id, comments.len(), elapsed);

    // ── 6. 3-table join: post + comments + commenter names ─────────────
    println!("\n=== 6. 3-table join: post 1234 + comments + user names ===");

    let post_id = 1234;
    let t = Instant::now();

    // Fetch post
    let posts = db.scan(
        "blog_posts",
        vec![Filter::new("id", Operator::Equals(Value::Int32(post_id)))],
    ).unwrap();
    assert_eq!(posts.len(), 1);
    let post = &posts[0];

    // Fetch post author
    let post_author_id = &post.values[1];
    let authors = db.scan(
        "users",
        vec![Filter::new("id", Operator::Equals(post_author_id.clone()))],
    ).unwrap();
    assert_eq!(authors.len(), 1);
    let post_author_name = authors[0].values[1].to_string();

    // Fetch comments for this post
    let comments = db.scan(
        "comments",
        vec![Filter::new("post_id", Operator::Equals(Value::Int32(post_id)))],
    ).unwrap();

    // Batch-fetch unique commenters with IN
    let commenter_ids: Vec<Value> = comments.iter().map(|c| c.values[2].clone()).collect();
    let unique_ids: Vec<Value> = {
        let mut seen = std::collections::HashSet::new();
        commenter_ids
            .iter()
            .filter(|v| seen.insert(format!("{:?}", v)))
            .cloned()
            .collect()
    };
    let commenters = db.scan(
        "users",
        vec![Filter::new("id", Operator::In(unique_ids.clone()))],
    ).unwrap();
    let elapsed = t.elapsed();

    // Build user_id → name lookup
    let name_map: HashMap<String, String> = commenters
        .iter()
        .map(|u| (format!("{:?}", u.values[0]), u.values[1].to_string()))
        .collect();

    let expected_comments = comments_for_post(post_id as usize);
    assert_eq!(comments.len(), expected_comments);
    assert_eq!(commenters.len(), unique_ids.len());
    assert!(!post_author_name.is_empty());
    for cid in &commenter_ids {
        assert!(name_map.contains_key(&format!("{:?}", cid)), "missing user for {:?}", cid);
    }
    println!(
        "post {} by {}: {} comments, {} commenters in {:?}",
        post_id, post_author_name, comments.len(), commenters.len(), elapsed
    );

    // ── 7. Recent posts page + comment counts ──────────────────────────
    println!("\n=== 7. Recent posts page: last 20 + comment counts ===");

    let t = Instant::now();
    let recent_posts = db.scan(
        "blog_posts",
        vec![Filter::new("id", Operator::GreaterThan(Value::Int32((POST_COUNT - 20) as i32)))],
    ).unwrap();
    assert_eq!(recent_posts.len(), 20);

    let mut total_comments = 0usize;
    for post in &recent_posts {
        let pid = post.values[0].clone();
        let count = db.count(
            "comments",
            vec![Filter::new("post_id", Operator::Equals(pid))],
        ).unwrap();
        total_comments += count;
    }
    let elapsed = t.elapsed();

    assert!(total_comments >= 20 * 2);
    assert!(total_comments <= 20 * 4);
    println!("20 posts + {} comments in {:?}", total_comments, elapsed);

    // ── 8. Posts by multiple authors (IN operator) ─────────────────────
    println!("\n=== 8. Posts by authors IN (1, 3) ===");

    let t = Instant::now();
    let results = db.scan(
        "blog_posts",
        vec![Filter::new(
            "author_id",
            Operator::In(vec![Value::Int32(1), Value::Int32(3)]),
        )],
    ).unwrap();
    let elapsed = t.elapsed();

    let expected = 2 * (POST_COUNT / USER_COUNT);
    assert_eq!(results.len(), expected);
    for row in &results {
        let aid = &row.values[1];
        assert!(*aid == Value::Int32(1) || *aid == Value::Int32(3));
    }
    println!("{} rows in {:?}", results.len(), elapsed);

    // ── 9. Range scan on post ID (indexed BETWEEN) ─────────────────────
    println!("\n=== 9. Range scan: id BETWEEN 5000 AND 5100 ===");

    let t = Instant::now();
    let results = db.scan(
        "blog_posts",
        vec![Filter::new("id", Operator::Between(Value::Int32(5000), Value::Int32(5100)))],
    ).unwrap();
    let elapsed = t.elapsed();

    assert_eq!(results.len(), 101);
    println!("{} rows in {:?}", results.len(), elapsed);

    // ── Cleanup ────────────────────────────────────────────────────────
    let _ = fs::remove_dir_all(DATA_DIR);
    println!("\n=== All blog benchmark tests passed ===\n");
}

/// Number of topic categories (must match setup_db)
const fn topics_count() -> usize {
    5
}
