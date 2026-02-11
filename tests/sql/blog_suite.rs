use thunderdb::{Database, Result, DirectDataAccess};
use std::fs;

#[test]
fn test_sql_blog_suite() -> Result<()> {
    let data_dir = "/tmp/thunderdb_sql_blog_test";
    let _ = fs::remove_dir_all(data_dir);
    
    let mut db = Database::open(data_dir)?;

    // 1. Create tables (using INSERT as it implicitly creates them, 
    //    or using our new CREATE TABLE if we want to test it)
    
    // Test CREATE TABLE idempotency (DROP if exists, then CREATE)
    // Actually, DROP TABLE will fail if not exists, so we handle it.
    let _ = db.drop_table("users");
    let _ = db.drop_table("blog_posts");
    let _ = db.drop_table("comments");

    // In a schema-less engine, CREATE TABLE is mostly a placeholder,
    // but let's use it to verify it works.
    
    // We'll use a helper to execute SQL via the same logic as the REPL
    // but without the REPL loop.
    
    execute_sql(&mut db, "CREATE TABLE users (id INT, name VARCHAR, email VARCHAR);")?;
    execute_sql(&mut db, "CREATE TABLE blog_posts (id INT, author_id INT, title VARCHAR, content VARCHAR);")?;
    execute_sql(&mut db, "CREATE TABLE comments (id INT, post_id INT, author_id INT, text VARCHAR);")?;

    // 2. Populate tables
    execute_sql(&mut db, "INSERT INTO users VALUES (1, 'Fabio', 'fabio@example.com');")?;
    execute_sql(&mut db, "INSERT INTO users VALUES (2, 'John', 'john@example.com');")?;
    execute_sql(&mut db, "INSERT INTO users VALUES (3, 'Alice', 'alice@example.com');")?;

    execute_sql(&mut db, "INSERT INTO blog_posts VALUES (1, 1, 'First Post', 'Hello world');")?;
    execute_sql(&mut db, "INSERT INTO blog_posts VALUES (2, 1, 'Second Post', 'ThunderDB is cool');")?;
    execute_sql(&mut db, "INSERT INTO blog_posts VALUES (3, 2, 'Johns Post', 'I am John');")?;

    execute_sql(&mut db, "INSERT INTO comments VALUES (1, 1, 2, 'Nice post!');")?;
    execute_sql(&mut db, "INSERT INTO comments VALUES (2, 1, 3, 'Agreed');")?;
    execute_sql(&mut db, "INSERT INTO comments VALUES (3, 2, 2, 'Great work');")?;

    // 3. Verify data (Simple SELECTs)
    let users = db.scan("users", vec![])?;
    assert_eq!(users.len(), 3);
    
    let posts = db.scan("blog_posts", vec![])?;
    assert_eq!(posts.len(), 3);

    // 4. Verify relationships (Manual joins since engine doesn't support JOIN yet)
    // Find comments for post 1
    let post1_comments = db.scan("comments", vec![
        thunderdb::Filter::new("col1", thunderdb::Operator::Equals(thunderdb::Value::Int32(1)))
    ])?;
    assert_eq!(post1_comments.len(), 2);

    // 5. CRUD: UPDATE
    // Note: col1 is author_id in blog_posts, col2 is title
    // Update title of post 1
    execute_sql(&mut db, "UPDATE blog_posts SET col2 = 'Updated First Post' WHERE col0 = 1;")?;
    
    let updated_post = db.get_by_id("blog_posts", 1)?.unwrap();
    assert_eq!(updated_post.values[2], thunderdb::Value::varchar("Updated First Post".to_string()));

    // 6. CRUD: DELETE
    execute_sql(&mut db, "DELETE FROM comments WHERE col0 = 3;")?;
    let comments = db.scan("comments", vec![])?;
    assert_eq!(comments.len(), 2);

    // 7. Cleanup
    execute_sql(&mut db, "DROP TABLE comments;")?;
    let tables = db.list_tables();
    assert!(!tables.contains(&"comments".to_string()));

    let _ = fs::remove_dir_all(data_dir);
    Ok(())
}

fn execute_sql(db: &mut Database, sql: &str) -> Result<()> {
    use thunderdb::{parse_sql, Statement};
    use thunderdb::parser::Executor;

    let stmt = parse_sql(sql)?;
    match stmt {
        Statement::Select(_) => {
            // Not strictly needed for this suite to assert on results of SELECT
            // but we could implement it if needed.
        }
        Statement::Insert(insert) => {
            let values = Executor::get_insert_values(&insert);
            db.insert_row(&insert.table, values)?;
        }
        Statement::Update(update) => {
            let filters = Executor::get_where_filters(&update.where_clause)?;
            let updates = Executor::get_update_assignments(&update)?;
            db.update(&update.table, filters, updates)?;
        }
        Statement::Delete(delete) => {
            let filters = Executor::get_where_filters(&delete.where_clause)?;
            db.delete(&delete.table, filters)?;
        }
        Statement::CreateTable(create) => {
            db.get_or_create_table(&create.name)?;
        }
        Statement::DropTable(table) => {
            db.drop_table(&table)?;
        }
        _ => {}
    }
    Ok(())
}
