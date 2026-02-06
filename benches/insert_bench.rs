// Insert benchmark - will be implemented in Phase 6
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_inserts(c: &mut Criterion) {
    c.bench_function("insert_single", |b| {
        b.iter(|| {
            black_box(42)
        });
    });
}

criterion_group!(benches, benchmark_inserts);
criterion_main!(benches);
