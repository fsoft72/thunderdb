use criterion::{criterion_group, criterion_main, Criterion};
use thunderdb::{Database, Value, DirectDataAccess};
use std::fs;

fn setup_db(name: &str) -> Database {
    let path = format!("/tmp/thunderdb_bench_{}", name);
    let _ = fs::remove_dir_all(&path);
    Database::open(&path).expect("Failed to open database")
}

fn benchmark_inserts(c: &mut Criterion) {
    let mut db = setup_db("insert");
    
    // Create a table first to avoid measuring table creation time
    db.insert_row("bench_table", vec![Value::Int32(0)]).unwrap();

    c.bench_function("insert_single_row", |b| {
        let mut count = 0;
        b.iter(|| {
            count += 1;
            db.insert_row("bench_table", vec![
                Value::Int32(count),
                Value::varchar("Benchmarking is fun"),
                Value::Float64(3.14)
            ]).unwrap();
        });
    });

    let mut db_batch = setup_db("insert_batch");
    c.bench_function("insert_batch_100", |b| {
        let mut count = 0;
        b.iter(|| {
            count += 1;
            let mut batch = Vec::with_capacity(100);
            for i in 0..100 {
                batch.push(vec![
                    Value::Int32(count * 100 + i),
                    Value::varchar(format!("Row {}", i)),
                    Value::Float64(i as f64)
                ]);
            }
            db_batch.insert_batch("bench_table", batch).unwrap();
        });
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_inserts
);
criterion_main!(benches);