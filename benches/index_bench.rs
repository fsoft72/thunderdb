use criterion::{criterion_group, criterion_main, Criterion, black_box};
use thunderdb::{Value};
use thunderdb::index::BTree;
use std::fs;

fn benchmark_index_ops(c: &mut Criterion) {
    let path = "/tmp/thunderdb_bench_index";
    let _ = fs::remove_dir_all(path);
    fs::create_dir_all(path).unwrap();

    // Benchmark B-Tree insertion
    c.bench_function("btree_insert_1000", |b| {
        b.iter(|| {
            let mut btree = BTree::new(black_box(100)).unwrap(); // order 100
            for i in 0..1000 {
                btree.insert(Value::Int32(i), i as u64).unwrap();
            }
        });
    });

    // Setup a larger tree for search benchmarks
    let mut btree = BTree::new(100).unwrap();
    for i in 0..10_000 {
        btree.insert(Value::Int32(i), i as u64).unwrap();
    }

    c.bench_function("btree_search_match", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % 10_000;
            black_box(btree.search(&Value::Int32(i)));
        });
    });

    c.bench_function("btree_range_scan_100", |b| {
        let mut i = 0;
        b.iter(|| {
            i = (i + 1) % 9_900;
            let start = Value::Int32(i);
            let end = Value::Int32(i + 100);
            black_box(btree.range_scan(&start, &end));
        });
    });

    let _ = fs::remove_dir_all(path);
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_index_ops
);
criterion_main!(benches);
