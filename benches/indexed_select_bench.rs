use criterion::{criterion_group, criterion_main, Criterion};
use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use std::fs;

fn setup_indexed_db(name: &str, row_count: usize) -> Database {
    let path = format!("/tmp/thunderdb_bench_{}", name);
    let _ = fs::remove_dir_all(&path);
    let mut db = Database::open(&path).expect("Failed to open database");
    
    // Set schema and create index
    {
        use thunderdb::storage::table_engine::{TableSchema, ColumnInfo};
        let table = db.get_or_create_table("users").unwrap();
        table.set_schema(TableSchema {
            columns: vec![
                ColumnInfo { name: "id".to_string(), data_type: "INT".to_string() },
                ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string() },
                ColumnInfo { name: "age".to_string(), data_type: "INT".to_string() },
            ]
        }).unwrap();
        table.index_manager_mut().create_index("age").unwrap();
    }

    let mut batch = Vec::with_capacity(1000);
    for i in 0..row_count {
        batch.push(vec![
            Value::Int32(i as i32),
            Value::varchar(format!("User {}", i)),
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

fn benchmark_indexed_selects(c: &mut Criterion) {
    let row_count = 10_000;
    let mut db = setup_indexed_db("indexed_select", row_count);

    // Benchmark search with index
    c.bench_function("indexed_search_age_equals", |b| {
        b.iter(|| {
            // Search for age = 42 (should use index)
            db.scan("users", vec![
                Filter::new("age", Operator::Equals(Value::Int32(42)))
            ]).unwrap();
        });
    });

    // Benchmark range search with index
    c.bench_function("indexed_range_search_age", |b| {
        b.iter(|| {
            // Search for age between 40 and 45
            db.scan("users", vec![
                Filter::new("age", Operator::Between(Value::Int32(40), Value::Int32(45)))
            ]).unwrap();
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_indexed_selects
);
criterion_main!(benches);
