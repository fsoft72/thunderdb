use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator, Result};
use std::fs;

const DATA_DIR: &str = "/tmp/thunderdb_stress_1m";
const ROW_COUNT: usize = 1_000_000;
const BATCH_SIZE: usize = 10_000;

/// Set up a database pre-populated with 1M rows.
/// Schema: (id: Int32, name: Varchar, score: Int32)
fn setup_1m_db() -> Database {
    let _ = fs::remove_dir_all(DATA_DIR);
    let mut db = Database::open(DATA_DIR).expect("Failed to open database");

    for chunk_start in (0..ROW_COUNT).step_by(BATCH_SIZE) {
        let chunk_end = (chunk_start + BATCH_SIZE).min(ROW_COUNT);
        let batch: Vec<Vec<Value>> = (chunk_start..chunk_end)
            .map(|i| {
                vec![
                    Value::Int32(i as i32),
                    Value::varchar(format!("user_{}", i)),
                    Value::Int32((i % 1000) as i32),
                ]
            })
            .collect();
        db.insert_batch("stress", batch).unwrap();
    }

    // Set schema so column names work
    {
        use thunderdb::storage::table_engine::{ColumnInfo, TableSchema};
        let table = db.get_table_mut("stress").unwrap();
        table
            .set_schema(TableSchema {
                columns: vec![
                    ColumnInfo { name: "id".to_string(), data_type: "INT32".to_string() },
                    ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
                    ColumnInfo { name: "score".to_string(), data_type: "INT32".to_string() },
                ],
            })
            .unwrap();
    }

    db
}

fn cleanup() {
    let _ = fs::remove_dir_all(DATA_DIR);
}

// ── Bulk insert ──────────────────────────────────────────────────────

#[test]
#[ignore]
fn stress_bulk_insert_1m() -> Result<()> {
    let _ = fs::remove_dir_all(DATA_DIR);
    let mut db = Database::open(DATA_DIR)?;

    let start = std::time::Instant::now();
    for chunk_start in (0..ROW_COUNT).step_by(BATCH_SIZE) {
        let chunk_end = (chunk_start + BATCH_SIZE).min(ROW_COUNT);
        let batch: Vec<Vec<Value>> = (chunk_start..chunk_end)
            .map(|i| {
                vec![
                    Value::Int32(i as i32),
                    Value::varchar(format!("user_{}", i)),
                    Value::Int32((i % 1000) as i32),
                ]
            })
            .collect();
        db.insert_batch("stress", batch)?;
    }
    let elapsed = start.elapsed();

    let count = db.count("stress", vec![])?;
    assert_eq!(count, ROW_COUNT);
    println!(
        "Inserted {} rows in {:.2}s ({:.0} rows/s)",
        ROW_COUNT,
        elapsed.as_secs_f64(),
        ROW_COUNT as f64 / elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── Full table scan ──────────────────────────────────────────────────

#[test]
#[ignore]
fn stress_full_scan_1m() -> Result<()> {
    let mut db = setup_1m_db();

    let start = std::time::Instant::now();
    let rows = db.scan("stress", vec![])?;
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), ROW_COUNT);
    println!(
        "Full scan {} rows in {:.2}s ({:.0} rows/s)",
        rows.len(),
        elapsed.as_secs_f64(),
        rows.len() as f64 / elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── Filtered scan (no index) ────────────────────────────────────────

#[test]
#[ignore]
fn stress_filtered_scan_no_index_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // score = 42 → matches 0.1% of rows (every 1000th)
    let start = std::time::Instant::now();
    let rows = db.scan(
        "stress",
        vec![Filter::new("score", Operator::Equals(Value::Int32(42)))],
    )?;
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), ROW_COUNT / 1000);
    println!(
        "Filtered scan (no index): {} matches in {:.2}s",
        rows.len(),
        elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── Index creation on 1M rows ───────────────────────────────────────

#[test]
#[ignore]
fn stress_index_creation_1m() -> Result<()> {
    let mut db = setup_1m_db();

    let start = std::time::Instant::now();
    {
        let table = db.get_table_mut("stress")?;
        table.index_manager_mut().create_index("score")?;

        // Rebuild index from existing rows
        let rows = table.scan_all()?;
        let mapping = table.build_column_mapping();
        for row in &rows {
            table.index_manager_mut().insert_row(row, &mapping)?;
        }
    }
    let elapsed = start.elapsed();

    println!("Index created on {} rows in {:.2}s", ROW_COUNT, elapsed.as_secs_f64());

    // Verify indexed lookup works
    let rows = db.scan(
        "stress",
        vec![Filter::new("score", Operator::Equals(Value::Int32(42)))],
    )?;
    assert_eq!(rows.len(), ROW_COUNT / 1000);

    cleanup();
    Ok(())
}

// ── Filtered scan with index ────────────────────────────────────────

#[test]
#[ignore]
fn stress_filtered_scan_with_index_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Create and populate index
    {
        let table = db.get_table_mut("stress")?;
        table.index_manager_mut().create_index("score")?;
        let rows = table.scan_all()?;
        let mapping = table.build_column_mapping();
        for row in &rows {
            table.index_manager_mut().insert_row(row, &mapping)?;
        }
    }

    let start = std::time::Instant::now();
    let rows = db.scan(
        "stress",
        vec![Filter::new("score", Operator::Equals(Value::Int32(42)))],
    )?;
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), ROW_COUNT / 1000);
    println!(
        "Indexed scan: {} matches in {:.2}s",
        rows.len(),
        elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── Update performance ──────────────────────────────────────────────

#[test]
#[ignore]
fn stress_update_10_percent_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Update rows where score < 100 (10% of rows)
    let start = std::time::Instant::now();
    let updated = db.update(
        "stress",
        vec![Filter::new("score", Operator::LessThan(Value::Int32(100)))],
        vec![("score".to_string(), Value::Int32(9999))],
    )?;
    let elapsed = start.elapsed();

    assert_eq!(updated, ROW_COUNT / 10);
    println!(
        "Updated {} rows (10%) in {:.2}s ({:.0} rows/s)",
        updated,
        elapsed.as_secs_f64(),
        updated as f64 / elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── Delete performance ──────────────────────────────────────────────

#[test]
#[ignore]
fn stress_delete_10_percent_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Delete rows where score < 100 (10% of rows)
    let start = std::time::Instant::now();
    let deleted = db.delete(
        "stress",
        vec![Filter::new("score", Operator::LessThan(Value::Int32(100)))],
    )?;
    let elapsed = start.elapsed();

    assert_eq!(deleted, ROW_COUNT / 10);

    let remaining = db.count("stress", vec![])?;
    assert_eq!(remaining, ROW_COUNT - ROW_COUNT / 10);

    println!(
        "Deleted {} rows (10%) in {:.2}s, {} remaining",
        deleted,
        elapsed.as_secs_f64(),
        remaining
    );

    cleanup();
    Ok(())
}

// ── Count performance (O(1) fast path) ──────────────────────────────

#[test]
#[ignore]
fn stress_count_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Unfiltered count should be O(1)
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let c = db.count("stress", vec![])?;
        assert_eq!(c, ROW_COUNT);
    }
    let elapsed = start.elapsed();

    println!(
        "1000x unfiltered COUNT on {} rows in {:.2}ms ({:.0}ns/call)",
        ROW_COUNT,
        elapsed.as_secs_f64() * 1000.0,
        elapsed.as_nanos() as f64 / 1000.0
    );

    cleanup();
    Ok(())
}

// ── Range queries ───────────────────────────────────────────────────

#[test]
#[ignore]
fn stress_range_query_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Range: score BETWEEN 100 AND 199 (10% of rows)
    let start = std::time::Instant::now();
    let rows = db.scan(
        "stress",
        vec![
            Filter::new("score", Operator::GreaterThanOrEqual(Value::Int32(100))),
            Filter::new("score", Operator::LessThan(Value::Int32(200))),
        ],
    )?;
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), ROW_COUNT / 10);
    println!(
        "Range scan: {} matches in {:.2}s",
        rows.len(),
        elapsed.as_secs_f64()
    );

    cleanup();
    Ok(())
}

// ── LIMIT queries ───────────────────────────────────────────────────

#[test]
#[ignore]
fn stress_limit_query_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // LIMIT 10 on 1M rows — should be near-instant with limit push-down
    let start = std::time::Instant::now();
    let rows = db.scan_with_limit("stress", vec![], Some(10), None)?;
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), 10);
    println!(
        "LIMIT 10 on {} rows in {:.2}ms",
        ROW_COUNT,
        elapsed.as_secs_f64() * 1000.0
    );

    // Should be well under 100ms with limit push-down
    assert!(
        elapsed.as_millis() < 500,
        "LIMIT 10 took {}ms — expected <500ms with limit push-down",
        elapsed.as_millis()
    );

    cleanup();
    Ok(())
}

// ── Compaction after deletes ────────────────────────────────────────

#[test]
#[ignore]
fn stress_compaction_1m() -> Result<()> {
    let mut db = setup_1m_db();

    // Delete 50% of rows
    let deleted = db.delete(
        "stress",
        vec![Filter::new("score", Operator::LessThan(Value::Int32(500)))],
    )?;
    assert_eq!(deleted, ROW_COUNT / 2);

    // Run full compaction
    let start = std::time::Instant::now();
    {
        let table = db.get_table_mut("stress")?;
        table.full_compact()?;
    }
    let elapsed = start.elapsed();

    let remaining = db.count("stress", vec![])?;
    assert_eq!(remaining, ROW_COUNT / 2);

    println!(
        "Compacted after deleting {} rows in {:.2}s, {} remaining",
        deleted,
        elapsed.as_secs_f64(),
        remaining
    );

    // Verify data integrity after compaction
    let rows = db.scan("stress", vec![])?;
    assert_eq!(rows.len(), ROW_COUNT / 2);
    for row in &rows {
        if let Value::Int32(score) = &row.values[2] {
            assert!(*score >= 500, "Found row with score {} after deleting <500", score);
        }
    }

    cleanup();
    Ok(())
}
