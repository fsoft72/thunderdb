use criterion::{criterion_group, criterion_main, Criterion};
use thunderdb::{Database, Value, DirectDataAccess, Filter, Operator};
use std::fs;

const ROW_COUNT: usize = 1_000_000;
const BATCH_SIZE: usize = 10_000;

/// Set up a database pre-populated with 1M rows.
/// Schema: (id: Int32, name: Varchar, score: Int32)
fn setup_1m_db(suffix: &str) -> Database {
    let path = format!("/tmp/thunderdb_stress_bench_{}", suffix);
    let _ = fs::remove_dir_all(&path);
    let mut db = Database::open(&path).expect("Failed to open database");

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

    // Set schema
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

/// Benchmark: bulk insert 1M rows
fn bench_insert_1m(c: &mut Criterion) {
    c.bench_function("insert_1m_rows", |b| {
        b.iter(|| {
            let path = "/tmp/thunderdb_stress_bench_insert";
            let _ = fs::remove_dir_all(path);
            let mut db = Database::open(path).unwrap();

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

            let _ = fs::remove_dir_all(path);
        });
    });
}

/// Benchmark: full table scan on 1M rows
fn bench_scan_1m(c: &mut Criterion) {
    let mut db = setup_1m_db("scan");

    c.bench_function("scan_all_1m", |b| {
        b.iter(|| {
            db.scan("stress", vec![]).unwrap();
        });
    });

    let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_scan");
}

/// Benchmark: indexed point lookup on 1M rows
fn bench_indexed_lookup_1m(c: &mut Criterion) {
    let mut db = setup_1m_db("idx_lookup");

    // Create and populate index on score column
    {
        let table = db.get_table_mut("stress").unwrap();
        table.index_manager_mut().create_index("score").unwrap();
        let rows = table.scan_all().unwrap();
        let mapping = table.build_column_mapping();
        for row in &rows {
            table.index_manager_mut().insert_row(row, &mapping).unwrap();
        }
    }

    c.bench_function("indexed_lookup_1m", |b| {
        let mut val = 0i32;
        b.iter(|| {
            val = (val + 1) % 1000;
            db.scan(
                "stress",
                vec![Filter::new("score", Operator::Equals(Value::Int32(val)))],
            )
            .unwrap();
        });
    });

    let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_idx_lookup");
}

/// Benchmark: range scan on 1M rows
fn bench_range_scan_1m(c: &mut Criterion) {
    let mut db = setup_1m_db("range");

    c.bench_function("range_scan_1m_10pct", |b| {
        b.iter(|| {
            db.scan(
                "stress",
                vec![
                    Filter::new("score", Operator::GreaterThanOrEqual(Value::Int32(100))),
                    Filter::new("score", Operator::LessThan(Value::Int32(200))),
                ],
            )
            .unwrap();
        });
    });

    let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_range");
}

/// Benchmark: COUNT on 1M rows (should be O(1) with fast-path)
fn bench_count_1m(c: &mut Criterion) {
    let mut db = setup_1m_db("count");

    c.bench_function("count_1m_no_filter", |b| {
        b.iter(|| {
            db.count("stress", vec![]).unwrap();
        });
    });

    let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_count");
}

/// Benchmark: filtered update on 1M rows
fn bench_update_1m(c: &mut Criterion) {
    // Each iteration needs a fresh DB since updates are destructive
    c.bench_function("update_1m_10pct", |b| {
        b.iter_with_setup(
            || setup_1m_db("update"),
            |mut db| {
                db.update(
                    "stress",
                    vec![Filter::new("score", Operator::LessThan(Value::Int32(100)))],
                    vec![("score".to_string(), Value::Int32(9999))],
                )
                .unwrap();
                let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_update");
            },
        );
    });
}

/// Benchmark: LIMIT query on 1M rows
fn bench_limit_1m(c: &mut Criterion) {
    let mut db = setup_1m_db("limit");

    c.bench_function("limit_10_on_1m", |b| {
        b.iter(|| {
            db.scan_with_limit("stress", vec![], Some(10), None).unwrap();
        });
    });

    let _ = fs::remove_dir_all("/tmp/thunderdb_stress_bench_limit");
}

criterion_group!(
    name = stress_benches;
    config = Criterion::default().sample_size(10);
    targets = bench_insert_1m,
              bench_scan_1m,
              bench_indexed_lookup_1m,
              bench_range_scan_1m,
              bench_count_1m,
              bench_update_1m,
              bench_limit_1m
);
criterion_main!(stress_benches);
