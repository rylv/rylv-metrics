use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rylv_metrics::{
    MetricCollectorTrait, PreparedMetric, RylvStr, SharedCollector, SharedCollectorOptions,
    TLSCollector, TLSCollectorOptions,
};
use std::time::Instant;

type BenchHasher = ahash::RandomState;

fn make_shared() -> SharedCollector<BenchHasher> {
    SharedCollector::new(SharedCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    })
}

fn make_tls() -> TLSCollector<BenchHasher> {
    TLSCollector::new(TLSCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    })
}

// ---------------------------------------------------------------------------
// Single-threaded: histogram — regular vs sorted vs prepared, shared vs tls
// ---------------------------------------------------------------------------

fn benchmark_histogram_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("histogram_single_thread");
    group.throughput(Throughput::Elements(1));

    // --- SharedCollector ---
    group.bench_function("shared_regular", |b| {
        let collector = make_shared();
        let mut tags = [
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ];
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&mut tags),
            );
        });
    });

    group.bench_function("shared_sorted", |b| {
        let collector = make_shared();
        let sorted = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        b.iter(|| {
            collector.histogram_sorted(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&sorted),
            );
        });
    });

    group.bench_function("shared_prepared", |b| {
        let collector = make_shared();
        let sorted = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        let prepared = collector.prepare_metric(RylvStr::from_static("bench.histogram"), sorted);
        b.iter(|| {
            collector.histogram_prepared(black_box(&prepared), black_box(42));
        });
    });

    // --- TLSCollector ---
    group.bench_function("tls_regular", |b| {
        let collector = make_tls();
        let mut tags = [
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ];
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&mut tags),
            );
        });
    });

    group.bench_function("tls_sorted", |b| {
        let collector = make_tls();
        let sorted = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        b.iter(|| {
            collector.histogram_sorted(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&sorted),
            );
        });
    });

    group.bench_function("tls_prepared", |b| {
        let collector = make_tls();
        let sorted = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        let prepared = collector.prepare_metric(RylvStr::from_static("bench.histogram"), sorted);
        b.iter(|| {
            collector.histogram_prepared(black_box(&prepared), black_box(42));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Parallel helpers
// ---------------------------------------------------------------------------

fn run_parallel_histogram_regular<C: MetricCollectorTrait + Sync>(
    collector: &C,
    iters: u64,
    thread_count: usize,
) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / thread_count;
    let remainder = total_iters % thread_count;

    std::thread::scope(|scope| {
        for index in 0..thread_count {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                let mut tags = [
                    RylvStr::from_static("service:api"),
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("region:us-east-1"),
                ];
                for _ in 0..work {
                    collector.histogram(
                        RylvStr::from_static("bench.parallel.histogram"),
                        42,
                        &mut tags,
                    );
                }
            });
        }
    });
}

fn run_parallel_histogram_sorted<C: MetricCollectorTrait + Sync>(
    collector: &C,
    iters: u64,
    thread_count: usize,
) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / thread_count;
    let remainder = total_iters % thread_count;

    std::thread::scope(|scope| {
        for index in 0..thread_count {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                let sorted = collector.prepare_sorted_tags([
                    RylvStr::from_static("service:api"),
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("region:us-east-1"),
                ]);
                for _ in 0..work {
                    collector.histogram_sorted(
                        RylvStr::from_static("bench.parallel.histogram"),
                        42,
                        &sorted,
                    );
                }
            });
        }
    });
}

fn run_parallel_histogram_prepared_shared(
    collector: &SharedCollector<BenchHasher>,
    prepared: &PreparedMetric<BenchHasher>,
    iters: u64,
    thread_count: usize,
) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / thread_count;
    let remainder = total_iters % thread_count;

    std::thread::scope(|scope| {
        for index in 0..thread_count {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                for _ in 0..work {
                    collector.histogram_prepared(prepared, 42);
                }
            });
        }
    });
}

fn run_parallel_histogram_prepared_tls(
    collector: &TLSCollector<BenchHasher>,
    prepared: &PreparedMetric<BenchHasher>,
    iters: u64,
    thread_count: usize,
) {
    let total_iters = match usize::try_from(iters) {
        Ok(v) => v,
        Err(_) => usize::MAX / 2,
    };
    let base = total_iters / thread_count;
    let remainder = total_iters % thread_count;

    std::thread::scope(|scope| {
        for index in 0..thread_count {
            let work = base + usize::from(index < remainder);
            scope.spawn(move || {
                for _ in 0..work {
                    collector.histogram_prepared(prepared, 42);
                }
            });
        }
    });
}

// ---------------------------------------------------------------------------
// Multi-threaded: histogram — regular vs sorted vs prepared, shared vs tls
// ---------------------------------------------------------------------------

fn benchmark_histogram_parallel(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut group = c.benchmark_group("histogram_parallel_shared");
    group.throughput(Throughput::Elements(1));

    // --- SharedCollector ---
    group.bench_function("shared_regular", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_regular(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("shared_sorted", |b| {
        let collector = make_shared();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("shared_prepared", |b| {
        let collector = make_shared();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.histogram"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("service:api"),
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("region:us-east-1"),
            ]),
        );
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_prepared_shared(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });

    // --- TLSCollector ---
    group.bench_function("tls_regular", |b| {
        let collector = make_tls();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_regular(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("tls_sorted", |b| {
        let collector = make_tls();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("tls_prepared", |b| {
        let collector = make_tls();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.histogram"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("service:api"),
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("region:us-east-1"),
            ]),
        );
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_prepared_tls(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_histogram_single_thread,
    benchmark_histogram_parallel
);
criterion_main!(benches);
