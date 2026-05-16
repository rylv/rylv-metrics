use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, PreparedMetric, RylvStr, SharedCollector,
    SharedCollectorOptions, TLSCollector,
};
use std::time::Instant;

type BenchHasher = ahash::RandomState;

fn make_collector() -> SharedCollector<BenchHasher> {
    let options = SharedCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    };

    SharedCollector::new(options)
}

#[cfg(feature = "tls-collector")]
fn make_tls_collector() -> TLSCollector<BenchHasher> {
    use rylv_metrics::{TLSCollector, TLSCollectorOptions};

    let options = TLSCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    };

    TLSCollector::new(options)
}

#[cfg(feature = "tls-collector")]
fn benchmark_count_add_tls_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_add_tls_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("count_add_tls_on", |b| {
        let collector = make_tls_collector();
        let mut tags = [
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("kind:count"),
        ];
        b.iter(|| {
            collector.count_add(
                black_box(RylvStr::from_static("bench.counter")),
                black_box(1),
                black_box(&mut tags),
            );
        });
    });

    group.bench_function("count_add_tls_off", |b| {
        let collector = make_collector();
        let mut tags = [
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("kind:count"),
        ];
        b.iter(|| {
            collector.count_add(
                black_box(RylvStr::from_static("bench.counter")),
                black_box(1),
                black_box(&mut tags),
            );
        });
    });
    group.finish();
}

#[cfg(feature = "tls-collector")]
fn benchmark_histogram_tls_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("histogram_tls_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("histogram_tls_on", |b| {
        let collector = make_tls_collector();
        let mut tags = [
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("kind:histogram"),
        ];
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&mut tags),
            );
        });
    });

    group.bench_function("histogram_tls_off", |b| {
        let collector = make_collector();
        let mut tags = [
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("kind:histogram"),
        ];
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("bench.histogram")),
                black_box(42),
                black_box(&mut tags),
            );
        });
    });
    group.finish();
}

fn run_parallel_count_add<C: MetricCollectorTrait + Sync>(
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
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("kind:parallel"),
                ];
                for _ in 0..work {
                    collector.count_add(
                        RylvStr::from_static("bench.parallel.counter"),
                        1,
                        &mut tags,
                    );
                }
            });
        }
    });
}

fn run_parallel_histogram<C: MetricCollectorTrait + Sync>(
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
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("kind:parallel-h"),
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
                let tags = collector.prepare_sorted_tags([
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("kind:parallel-h"),
                ]);
                for _ in 0..work {
                    collector.histogram_sorted(
                        RylvStr::from_static("bench.parallel.histogram"),
                        42,
                        &tags,
                    );
                }
            });
        }
    });
}

fn run_parallel_count_add_sorted<C: MetricCollectorTrait + Sync>(
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
                let tags = collector.prepare_sorted_tags([
                    RylvStr::from_static("env:bench"),
                    RylvStr::from_static("kind:parallel"),
                ]);
                for _ in 0..work {
                    collector.count_add_sorted(
                        RylvStr::from_static("bench.parallel.counter"),
                        1,
                        &tags,
                    );
                }
            });
        }
    });
}

fn run_parallel_count_add_prepared_udp<C: MetricCollectorTrait<Hasher = BenchHasher> + Sync>(
    collector: &C,
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
                    collector.count_add_prepared(prepared, 1);
                }
            });
        }
    });
}

fn run_parallel_histogram_prepared_udp<C: MetricCollectorTrait<Hasher = BenchHasher> + Sync>(
    collector: &C,
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

#[cfg(feature = "tls-collector")]
fn run_parallel_count_add_prepared_tls<C: MetricCollectorTrait<Hasher = BenchHasher> + Sync>(
    collector: &C,
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
                    collector.count_add_prepared(prepared, 1);
                }
            });
        }
    });
}

#[cfg(feature = "tls-collector")]
fn run_parallel_histogram_prepared_tls<C: MetricCollectorTrait<Hasher = BenchHasher> + Sync>(
    collector: &C,
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

fn benchmark_sorted_tags_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_tags_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("count_add_regular_tags", |b| {
        let collector = make_collector();
        let mut tags_unsorted = [
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ];
        b.iter(|| {
            collector.count_add(
                black_box(RylvStr::from_static("bench.sorted.count")),
                black_box(1),
                black_box(&mut tags_unsorted),
            );
        });
    });

    group.bench_function("count_add_sorted_tags", |b| {
        let collector = make_collector();
        let sorted_tags = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        b.iter(|| {
            collector.count_add_sorted(
                black_box(RylvStr::from_static("bench.sorted.count")),
                black_box(1),
                black_box(&sorted_tags),
            );
        });
    });

    group.bench_function("count_add_prepared_metric", |b| {
        let collector = make_collector();
        let sorted_tags = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        let prepared =
            collector.prepare_metric(RylvStr::from_static("bench.sorted.count"), sorted_tags);
        b.iter(|| {
            collector.count_add_prepared(black_box(&prepared), black_box(1));
        });
    });
    group.finish();
}

fn benchmark_histogram_sorted_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("histogram_sorted_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("histogram_regular_tags", |b| {
        let collector = make_collector();
        let mut tags_unsorted = [
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ];
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("bench.sorted.histogram")),
                black_box(42),
                black_box(&mut tags_unsorted),
            );
        });
    });

    group.bench_function("histogram_sorted_tags", |b| {
        let collector = make_collector();
        let sorted_tags = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        b.iter(|| {
            collector.histogram_sorted(
                black_box(RylvStr::from_static("bench.sorted.histogram")),
                black_box(42),
                black_box(&sorted_tags),
            );
        });
    });

    group.bench_function("histogram_prepared_metric", |b| {
        let collector = make_collector();
        let sorted_tags = collector.prepare_sorted_tags([
            RylvStr::from_static("service:api"),
            RylvStr::from_static("env:bench"),
            RylvStr::from_static("region:us-east-1"),
        ]);
        let prepared =
            collector.prepare_metric(RylvStr::from_static("bench.sorted.histogram"), sorted_tags);
        b.iter(|| {
            collector.histogram_prepared(black_box(&prepared), black_box(42));
        });
    });
    group.finish();
}

#[cfg(feature = "tls-collector")]
fn benchmark_sorted_tags_parallel_compare(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut group = c.benchmark_group("sorted_tags_parallel_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("udp_regular_parallel", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("udp_sorted_parallel", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("udp_prepared_parallel", |b| {
        let collector = make_collector();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.counter"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("kind:parallel"),
            ]),
        );
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add_prepared_udp(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("tls_regular_parallel", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("tls_sorted_parallel", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("tls_prepared_parallel", |b| {
        let collector = make_tls_collector();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.counter"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("kind:parallel"),
            ]),
        );
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add_prepared_tls(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });
    group.finish();
}

#[cfg(feature = "tls-collector")]
fn benchmark_histogram_sorted_parallel_compare(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut group = c.benchmark_group("histogram_sorted_parallel_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("udp_regular_parallel", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("udp_sorted_parallel", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("udp_prepared_parallel", |b| {
        let collector = make_collector();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.histogram"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("kind:parallel-h"),
            ]),
        );
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_prepared_udp(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("tls_regular_parallel", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("tls_sorted_parallel", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.bench_function("tls_prepared_parallel", |b| {
        let collector = make_tls_collector();
        let prepared = collector.prepare_metric(
            RylvStr::from_static("bench.parallel.histogram"),
            collector.prepare_sorted_tags([
                RylvStr::from_static("env:bench"),
                RylvStr::from_static("kind:parallel-h"),
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

#[cfg(feature = "tls-collector")]
fn benchmark_count_add_tls_parallel_contention(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut group = c.benchmark_group("count_add_tls_parallel_contention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("count_add_tls_on_parallel_contention", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("count_add_tls_off_parallel_contention", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_count_add(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.finish();
}

#[cfg(feature = "tls-collector")]
fn benchmark_histogram_tls_parallel_contention(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let mut group = c.benchmark_group("histogram_tls_parallel_contention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("histogram_tls_on_parallel_contention", |b| {
        let collector = make_tls_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram(&collector, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("histogram_tls_off_parallel_contention", |b| {
        let collector = make_collector();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram(&collector, iters, thread_count);
            start.elapsed()
        });
    });
    group.finish();
}

#[cfg(feature = "tls-collector")]
fn benchmark_parallel_histogram_merge_and_iterate(c: &mut Criterion) {
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let ops_per_thread = 2048usize;
    let total_ops = (thread_count * ops_per_thread) as u64;
    let mut group = c.benchmark_group("parallel_histogram_merge_iterate");
    group.throughput(Throughput::Elements(total_ops));

    group.bench_function("tls_on", |b| {
        let collector = make_tls_collector();
        let collector = &collector;
        b.iter(|| {
            run_parallel_histogram(collector, total_ops, thread_count);
            black_box(collector.try_begin_drain().into_iter().flatten().count());
        });
    });

    group.bench_function("tls_off", |b| {
        let collector = make_collector();
        let collector = &collector;
        b.iter(|| {
            run_parallel_histogram(&collector, total_ops, thread_count);
            black_box(bench_flush_and_drain_frame_count(collector));
        });
    });

    group.finish();
}

pub fn bench_flush_and_drain_frame_count(collector: impl DrainMetricCollectorTrait) -> usize {
    if let Some(drain) = collector.try_begin_drain() {
        return drain.count();
    }
    0
}

criterion_group!(
    benches,
    benchmark_sorted_tags_compare,
    benchmark_histogram_sorted_compare,
    benchmark_sorted_tags_parallel_compare,
    benchmark_histogram_sorted_parallel_compare,
    benchmark_count_add_tls_compare,
    benchmark_histogram_tls_compare,
    benchmark_count_add_tls_parallel_contention,
    benchmark_histogram_tls_parallel_contention,
    benchmark_parallel_histogram_merge_and_iterate
);
criterion_main!(benches);
