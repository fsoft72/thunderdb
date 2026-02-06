use criterion::{criterion_group, criterion_main, Criterion};
use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use std::fs;

fn setup_populated_db(name: &str, row_count: usize) -> Database {
    let path = format!("/tmp/thunderdb_bench_{}", name);
    let _ = fs::remove_dir_all(&path);
    let mut db = Database::open(&path).expect("Failed to open database");
    
    let mut batch = Vec::with_capacity(1000);
    for i in 0..row_count {
        batch.push(vec![
            Value::Int32(i as i32),
            Value::Varchar(format!("User {}", i)),
            Value::Int32((i % 100) as i32) // Age between 0-99
        ]);
        
        if batch.len() == 1000 {
            db.insert_batch("users", batch.drain(..).collect()).unwrap();
        }
    }
    if !batch.is_empty() {
        db.insert_batch("users", batch).unwrap();
    }
    
    db
}

fn benchmark_selects(c: &mut Criterion) {
    let row_count = 10_000;
    let mut db = setup_populated_db("select", row_count);

    // Benchmark reading by ID
    c.bench_function("get_by_id", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % row_count;
            db.get_by_id("users", (i + 1) as u64).unwrap();
        });
    });

    // Benchmark full table scan
    c.bench_function("scan_all_10k", |b| {
        b.iter(|| {
            db.scan("users", vec![]).unwrap();
        });
    });

    // Benchmark search with filter (currently scan-based)
    c.bench_function("search_with_filter_match_1_percent", |b| {
        b.iter(|| {
            // Search for age = 42 (should be 1% of 10k rows = 100 rows)
            db.scan("users", vec![
                Filter::new("col2", Operator::Equals(Value::Int32(42)))
            ]).unwrap();
        });
    });
    
    // Benchmark search with column name (uses schema)
    // First we need to set schema for the table
    {
        use thunderdb::storage::table_engine::{TableSchema, ColumnInfo};
        let table = db.get_table_mut("users").unwrap();
        table.set_schema(TableSchema {
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
                ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
                ColumnInfo { name: "age".to_string(), data_type: "INT".to_string() },
            ]
        }).unwrap();
    }

    c.bench_function("search_with_named_column_filter", |b| {
        b.iter(|| {
            db.scan("users", vec![
                Filter::new("age", Operator::Equals(Value::Int32(42)))
            ]).unwrap();
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_selects
);
criterion_main!(benches);