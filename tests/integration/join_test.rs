use thunderdb::{Database, Value, DirectDataAccess};
use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
use std::fs;

const DATA_DIR: &str = "/tmp/thunderdb_join_test";

/// Sets up a fresh database with users, posts, and comments tables for JOIN testing.
fn setup_db() -> Database {
    let _ = fs::remove_dir_all(DATA_DIR);
    let mut db = Database::open(DATA_DIR).expect("open db");

    // Users: id, name, age
    db.insert_batch("users", vec![
        vec![Value::Int32(1), Value::varchar("Alice"), Value::Int32(30)],
        vec![Value::Int32(2), Value::varchar("Bob"), Value::Int32(25)],
        vec![Value::Int32(3), Value::varchar("Charlie"), Value::Int32(35)],
    ]).unwrap();
    {
        let t = db.get_table_mut("users").unwrap();
        t.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT".into() },
            ColumnInfo { name: "name".into(), data_type: "VARCHAR".into() },
            ColumnInfo { name: "age".into(), data_type: "INT".into() },
        ]}).unwrap();
        t.create_index("id").unwrap();
    }

    // Posts: id, author_id, title — only Alice and Bob have posts
    db.insert_batch("posts", vec![
        vec![Value::Int32(1), Value::Int32(1), Value::varchar("Post A")],
        vec![Value::Int32(2), Value::Int32(1), Value::varchar("Post B")],
        vec![Value::Int32(3), Value::Int32(2), Value::varchar("Post C")],
    ]).unwrap();
    {
        let t = db.get_table_mut("posts").unwrap();
        t.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT".into() },
            ColumnInfo { name: "author_id".into(), data_type: "INT".into() },
            ColumnInfo { name: "title".into(), data_type: "VARCHAR".into() },
        ]}).unwrap();
        t.create_index("author_id").unwrap();
        t.create_index("id").unwrap();
    }

    // Comments: id, post_id, text — only on posts 1 and 3
    db.insert_batch("comments", vec![
        vec![Value::Int32(1), Value::Int32(1), Value::varchar("Nice!")],
        vec![Value::Int32(2), Value::Int32(1), Value::varchar("Great!")],
        vec![Value::Int32(3), Value::Int32(3), Value::varchar("Cool!")],
    ]).unwrap();
    {
        let t = db.get_table_mut("comments").unwrap();
        t.set_schema(TableSchema { columns: vec![
            ColumnInfo { name: "id".into(), data_type: "INT".into() },
            ColumnInfo { name: "post_id".into(), data_type: "INT".into() },
            ColumnInfo { name: "text".into(), data_type: "VARCHAR".into() },
        ]}).unwrap();
    }

    db
}

/// Comprehensive JOIN integration test covering all join types, aliases, dot-qualified
/// columns, WHERE pushdown, COUNT, SELECT *, ORDER BY, and multi-table chaining.
#[test]
fn test_join_suite() {
    let mut db = setup_db();

    {
        let mut repl = thunderdb::repl::Repl::new(&mut db).unwrap();

        // INNER JOIN: users with posts (3 rows — Alice x2, Bob x1)
        repl.execute_sql("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id");

        // LEFT JOIN: all users including Charlie who has no posts (4 rows)
        repl.execute_sql("SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.author_id");

        // RIGHT JOIN: all posts even if author somehow missing (3 rows, same as INNER here)
        repl.execute_sql("SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.author_id");

        // Multi-table: users -> posts -> comments (3 rows)
        repl.execute_sql("SELECT u.name, p.title, c.text FROM users u JOIN posts p ON u.id = p.author_id JOIN comments c ON p.id = c.post_id");

        // WHERE pushdown: only Alice's posts (age > 28)
        repl.execute_sql("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id WHERE u.age > 28");

        // COUNT with JOIN
        repl.execute_sql("SELECT COUNT(*) FROM users u JOIN posts p ON u.id = p.author_id");

        // SELECT * from JOIN
        repl.execute_sql("SELECT * FROM users u JOIN posts p ON u.id = p.author_id");

        // INNER JOIN with ORDER BY
        repl.execute_sql("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id ORDER BY p.title");

        // LEFT OUTER JOIN (OUTER keyword)
        repl.execute_sql("SELECT u.name, p.title FROM users u LEFT OUTER JOIN posts p ON u.id = p.author_id");
    }

    let _ = fs::remove_dir_all(DATA_DIR);
}
