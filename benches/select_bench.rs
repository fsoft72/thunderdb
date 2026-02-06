// Select benchmark - will be implemented in Phase 6
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_selects(c: &mut Criterion) {
    c.bench_function("select_by_id", |b| {
        b.iter(|| {
            black_box(42)
        });
    });
}

criterion_group!(benches, benchmark_selects);
criterion_main!(benches);
