//! Contended same-key `gauge_avg` / `count_add` on the shared collector.
//!
//! The averaged-gauge sum is an `i128` guarded by a CAS spinlock and the
//! saturating counter add is a CAS-retry loop, so threads recording the same
//! metric/tags key spin against each other instead of each issuing a single
//! `fetch_add`. These benches keep that cost visible: every thread hammers
//! one key, which is the worst case for the spinlock (distinct keys never
//! contend).

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rylv_metrics::{
    MetricCollectorTrait, RylvStr, RylvTag, SharedCollector, SharedCollectorOptions,
};
use std::time::Instant;

const THREADS: usize = 4;

type BenchHasher = ahash::RandomState;

fn make_shared() -> SharedCollector<BenchHasher> {
    SharedCollector::new(SharedCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    })
}

fn run_parallel_gauge_avg(collector: &SharedCollector<BenchHasher>, iters: u64, threads: usize) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / threads;
    let remainder = total_iters % threads;

    std::thread::scope(|scope| {
        for index in 0..threads {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                let mut tags = [RylvTag::from_static("env:bench")];
                for _ in 0..work {
                    collector.gauge_avg(
                        RylvStr::from_static("bench.shared.gauge_avg"),
                        1,
                        &mut tags,
                    );
                }
            });
        }
    });
}

fn run_parallel_count_add<C: MetricCollectorTrait + Sync>(
    collector: &C,
    iters: u64,
    threads: usize,
) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / threads;
    let remainder = total_iters % threads;

    std::thread::scope(|scope| {
        for index in 0..threads {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                let mut tags = [RylvTag::from_static("env:bench")];
                for _ in 0..work {
                    collector.count_add(RylvStr::from_static("bench.shared.count"), 1, &mut tags);
                }
            });
        }
    });
}

fn benchmark_shared_gauge_avg_same_key_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_gauge_avg_same_key_contention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("gauge_avg_same_key_contended", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_gauge_avg(&collector, black_box(iters), THREADS);
            start.elapsed()
        });
    });

    group.bench_function("gauge_avg_same_key_single_thread", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_gauge_avg(&collector, black_box(iters), 1);
            start.elapsed()
        });
    });
    group.finish();
}

fn benchmark_shared_count_add_same_key_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_count_add_same_key_contention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("count_add_same_key_contended", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, black_box(iters), THREADS);
            start.elapsed()
        });
    });

    group.bench_function("count_add_same_key_single_thread", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, black_box(iters), 1);
            start.elapsed()
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    benchmark_shared_gauge_avg_same_key_contention,
    benchmark_shared_count_add_same_key_contention
);
criterion_main!(benches);
