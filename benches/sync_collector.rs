use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, PreparedMetric, RylvStr,
    SharedCollector, SharedCollectorOptions, SortedTags, DEFAULT_STATS_WRITER_TYPE,
};
#[cfg(all(feature = "udp", feature = "tls-collector"))]
use rylv_metrics::{TLSCollector, TLSCollectorOptions};
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use std::time::{Duration, Instant};

#[cfg(all(feature = "dhat-heap", not(feature = "allocationcounter")))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

type BenchHasher = ahash::RandomState;
const DYNAMIC_POOL_SIZE: usize = 16_384;
const HISTOGRAM_VALUE: u64 = 42;
const TAG_CONST_1: &str = "tag:value";
const TAG_CONST_2: &str = "tag2:value2";

fn benchmark_record_histogram(c: &mut Criterion) {
    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let datadog_addr = socket.local_addr().unwrap();

    let finish = Arc::new(AtomicBool::new(false));
    let finish2 = finish.clone();
    let join = spawn(move || {
        socket
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let mut buf = [0; 14000];
        let mut received: usize = 0;
        loop {
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                received += size;
            }
            if finish2.load(Ordering::SeqCst) {
                println!("received {}", received);
                break;
            }
        }
    });

    let options = MetricCollectorOptions {
        max_udp_batch_size: 20000,
        max_udp_packet_size: 1400,
        flush_interval: Duration::from_millis(10000),
        writer_type: DEFAULT_STATS_WRITER_TYPE,
    };
    let inner_options = SharedCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    };

    let inner = SharedCollector::new(inner_options);
    let collector = MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector");

    let mut count_millis: u64 = 0;
    let n = 1024 * 1024;
    let mut vec_metrics = Vec::<&'static str>::with_capacity(n);
    let mut tags_metrics = Vec::<&'static str>::with_capacity(n);

    for i in 0..n {
        vec_metrics.push(format!("some.long.metric.by.some.criteria{i}").leak());
        tags_metrics.push(format!("sometag:somevaluefromcriteria{i}").leak());
    }

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    c.bench_function("histogram_allocations", |b| {
        let mut i = 0;
        let internal = Instant::now();
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static(vec_metrics[i])),
                black_box(1),
                black_box([
                    RylvStr::from_static(tags_metrics[i]),
                    RylvStr::from_static("tag:value"),
                    RylvStr::from_static("tag2:value2"),
                ]),
            );
            i = (i + 1) % n;
        });
        count_millis += internal.elapsed().as_millis() as u64;
    });
    let internal = Instant::now();
    drop(collector);
    count_millis += internal.elapsed().as_millis() as u64;
    println!("elapsed: {:?}", count_millis);

    let now = Instant::now();
    finish.store(true, Ordering::SeqCst);
    join.join().unwrap();
    println!("finish reader in :{} ms", now.elapsed().as_millis());
}

fn benchmark_record_histogram_single(c: &mut Criterion) {
    let (datadog_addr, finish, join) = spawn_udp_receiver();
    let collector = make_udp_collector(datadog_addr);

    let mut count_millis: u64 = 0;
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    c.bench_function("histogram", |b| {
        let internal = Instant::now();
        b.iter(|| {
            collector.histogram(
                black_box(RylvStr::from_static("some.metric")),
                black_box(1),
                black_box([
                    RylvStr::from_static("tag:value"),
                    RylvStr::from_static("tag2:value2"),
                ]),
            );
        });
        count_millis += internal.elapsed().as_millis() as u64;
    });
    let internal = Instant::now();
    drop(collector);
    count_millis += internal.elapsed().as_millis() as u64;
    println!("elapsed: {:?}", count_millis);

    let now = Instant::now();
    finish.store(true, Ordering::SeqCst);
    join.join().unwrap();
    println!("finish reader in :{} ms", now.elapsed().as_millis());
}

fn spawn_udp_receiver() -> (SocketAddr, Arc<AtomicBool>, JoinHandle<()>) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let datadog_addr = socket.local_addr().unwrap();
    let finish = Arc::new(AtomicBool::new(false));
    let finish2 = Arc::clone(&finish);
    let join = spawn(move || {
        socket
            .set_read_timeout(Some(Duration::from_millis(250)))
            .unwrap();
        let mut buf = [0; 14000];
        let mut received: usize = 0;
        loop {
            if let Ok((size, _)) = socket.recv_from(&mut buf) {
                received += size;
            }
            if finish2.load(Ordering::SeqCst) {
                println!("received {}", received);
                return;
            }
        }
    });
    (datadog_addr, finish, join)
}

fn make_udp_collector(
    datadog_addr: SocketAddr,
) -> MetricCollector<rylv_metrics::SharedCollector<BenchHasher>> {
    let options = MetricCollectorOptions {
        max_udp_batch_size: 20000,
        max_udp_packet_size: 1400,
        flush_interval: Duration::from_millis(10000),
        writer_type: DEFAULT_STATS_WRITER_TYPE,
    };
    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let inner_options = SharedCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    };
    let inner = SharedCollector::new(inner_options);
    MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector")
}

#[cfg(all(feature = "udp", feature = "tls-collector"))]
fn make_tls_collector(
    datadog_addr: SocketAddr,
) -> MetricCollector<rylv_metrics::TLSCollector<BenchHasher>> {
    let options = MetricCollectorOptions {
        max_udp_batch_size: 20000,
        max_udp_packet_size: 1400,
        flush_interval: Duration::from_millis(10000),
        writer_type: DEFAULT_STATS_WRITER_TYPE,
    };
    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let inner_options = TLSCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: ahash::RandomState::new(),
    };
    let inner = TLSCollector::new(inner_options);
    MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector")
}

fn make_dynamic_metric_and_tag_pool() -> (Vec<RylvStr<'static>>, Vec<RylvStr<'static>>) {
    let mut metrics = Vec::with_capacity(DYNAMIC_POOL_SIZE);
    let mut tags = Vec::with_capacity(DYNAMIC_POOL_SIZE);
    for i in 0..DYNAMIC_POOL_SIZE {
        let metric = format!("bench.sync.histogram.metric{i}");
        let tag = format!("benchsync:criterion{i}");
        metrics.push(RylvStr::from_static(metric.leak()));
        tags.push(RylvStr::from_static(tag.leak()));
    }
    (metrics, tags)
}

fn build_sorted_tags_pool<C: MetricCollectorTrait>(
    dynamic_tags: &[RylvStr<'static>],
    collector: &C,
) -> Vec<SortedTags<C::Hasher>> {
    dynamic_tags
        .iter()
        .map(|tag| {
            collector.prepare_sorted_tags([
                tag.clone(),
                RylvStr::from_static(TAG_CONST_1),
                RylvStr::from_static(TAG_CONST_2),
            ])
        })
        .collect()
}

fn run_parallel_histogram_regular<C: MetricCollectorTrait + Sync>(
    collector: &C,
    metrics: &[RylvStr<'static>],
    dynamic_tags: &[RylvStr<'static>],
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
                let len = metrics.len();
                for iter in 0..work {
                    let idx = (iter + index) % len;
                    let mut tags = [
                        dynamic_tags[idx].clone(),
                        RylvStr::from_static(TAG_CONST_1),
                        RylvStr::from_static(TAG_CONST_2),
                    ];
                    collector.histogram(metrics[idx].clone(), HISTOGRAM_VALUE, &mut tags);
                }
            });
        }
    });
}

fn run_parallel_histogram_sorted<C: MetricCollectorTrait + Sync>(
    collector: &C,
    metrics: &[RylvStr<'static>],
    sorted_tags: &[SortedTags<C::Hasher>],
    iters: u64,
    thread_count: usize,
) where
    C::Hasher: Sync,
{
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
                let len = metrics.len();
                for iter in 0..work {
                    let idx = (iter + index) % len;
                    collector.histogram_sorted(
                        metrics[idx].clone(),
                        HISTOGRAM_VALUE,
                        &sorted_tags[idx],
                    );
                }
            });
        }
    });
}

fn run_parallel_histogram_prepared_udp(
    collector: &MetricCollector<rylv_metrics::SharedCollector<BenchHasher>>,
    prepared: &[PreparedMetric<BenchHasher>],
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
                let len = prepared.len();
                for iter in 0..work {
                    let idx = (iter + index) % len;
                    collector.histogram_prepared(&prepared[idx], HISTOGRAM_VALUE);
                }
            });
        }
    });
}

#[cfg(all(feature = "udp", feature = "tls-collector"))]
fn run_parallel_histogram_prepared_tls(
    collector: &MetricCollector<rylv_metrics::TLSCollector<BenchHasher>>,
    prepared: &[PreparedMetric<BenchHasher>],
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
                let len = prepared.len();
                for iter in 0..work {
                    let idx = (iter + index) % len;
                    collector.histogram_prepared(&prepared[idx], HISTOGRAM_VALUE);
                }
            });
        }
    });
}

fn benchmark_histogram_sync_single_compare(c: &mut Criterion) {
    let (metrics, dynamic_tags) = make_dynamic_metric_and_tag_pool();

    let mut group = c.benchmark_group("sync_histogram_single_compare");
    group.throughput(Throughput::Elements(1));

    let (datadog_addr, finish, join) = spawn_udp_receiver();
    let collector = make_udp_collector(datadog_addr);
    group.bench_function("regular_tags", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            let mut tags = [
                dynamic_tags[idx].clone(),
                RylvStr::from_static(TAG_CONST_1),
                RylvStr::from_static(TAG_CONST_2),
            ];
            collector.histogram(
                black_box(metrics[idx].clone()),
                black_box(HISTOGRAM_VALUE),
                black_box(&mut tags),
            );
            idx = (idx + 1) % metrics.len();
        });
    });

    group.bench_function("sorted_tags", |b| {
        let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
        let mut idx = 0usize;
        b.iter(|| {
            collector.histogram_sorted(
                black_box(metrics[idx].clone()),
                black_box(HISTOGRAM_VALUE),
                black_box(&sorted_tags[idx]),
            );
            idx = (idx + 1) % metrics.len();
        });
    });

    group.bench_function("prepared_metric", |b| {
        let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
        let prepared: Vec<PreparedMetric<BenchHasher>> = metrics
            .iter()
            .zip(sorted_tags)
            .map(|(metric, tags)| collector.prepare_metric(metric.clone(), tags))
            .collect();
        let mut idx = 0usize;
        b.iter(|| {
            collector.histogram_prepared(black_box(&prepared[idx]), black_box(HISTOGRAM_VALUE));
            idx = (idx + 1) % prepared.len();
        });
    });
    group.finish();
    drop(collector);
    finish.store(true, Ordering::SeqCst);
    join.join().unwrap();
}

fn benchmark_histogram_sync_parallel_udp_compare(c: &mut Criterion) {
    let (metrics, dynamic_tags) = make_dynamic_metric_and_tag_pool();
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());

    let mut group = c.benchmark_group("sync_histogram_parallel_udp_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("regular_parallel", |b| {
        let (datadog_addr, finish, join) = spawn_udp_receiver();
        let collector = make_udp_collector(datadog_addr);
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_regular(
                &collector,
                &metrics,
                &dynamic_tags,
                iters,
                thread_count,
            );
            start.elapsed()
        });
        drop(collector);
        finish.store(true, Ordering::SeqCst);
        join.join().unwrap();
    });

    group.bench_function("sorted_parallel", |b| {
        let (datadog_addr, finish, join) = spawn_udp_receiver();
        let collector = make_udp_collector(datadog_addr);
        let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, &metrics, &sorted_tags, iters, thread_count);
            start.elapsed()
        });
        drop(collector);
        finish.store(true, Ordering::SeqCst);
        join.join().unwrap();
    });

    let (datadog_addr, finish, join) = spawn_udp_receiver();
    finish.store(true, Ordering::SeqCst);
    let collector = make_udp_collector(datadog_addr);
    let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
    group.bench_function("prepared_parallel", |b| {
        let prepared: Vec<PreparedMetric<BenchHasher>> = metrics
            .iter()
            .zip(sorted_tags.iter())
            .map(|(metric, tags)| collector.prepare_metric(metric.clone(), tags.clone()))
            .collect();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_prepared_udp(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });
    group.finish();

    drop(collector);
    join.join().unwrap();
}

#[cfg(all(feature = "udp", feature = "tls-collector"))]
fn benchmark_histogram_sync_parallel_tls_compare(c: &mut Criterion) {
    let (metrics, dynamic_tags) = make_dynamic_metric_and_tag_pool();
    let thread_count = std::thread::available_parallelism().map_or(1, |n| n.get());
    let (datadog_addr, finish, join) = spawn_udp_receiver();
    let collector = make_tls_collector(datadog_addr);

    let mut group = c.benchmark_group("sync_histogram_parallel_tls_compare");
    group.throughput(Throughput::Elements(1));

    group.bench_function("regular_parallel", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_regular(
                &collector,
                &metrics,
                &dynamic_tags,
                iters,
                thread_count,
            );
            start.elapsed()
        });
    });

    group.bench_function("sorted_parallel", |b| {
        let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_sorted(&collector, &metrics, &sorted_tags, iters, thread_count);
            start.elapsed()
        });
    });

    group.bench_function("prepared_parallel", |b| {
        let sorted_tags = build_sorted_tags_pool(&dynamic_tags, &collector);
        let prepared: Vec<PreparedMetric<BenchHasher>> = metrics
            .iter()
            .zip(sorted_tags)
            .map(|(metric, tags)| collector.prepare_metric(metric.clone(), tags))
            .collect();
        b.iter_custom(|iters| {
            let start = Instant::now();
            run_parallel_histogram_prepared_tls(&collector, &prepared, iters, thread_count);
            start.elapsed()
        });
    });
    group.finish();

    drop(collector);
    finish.store(true, Ordering::SeqCst);
    join.join().unwrap();
}

// Criterion group and main function
#[cfg(all(feature = "udp", feature = "tls-collector"))]
criterion_group!(
    benches,
    benchmark_record_histogram,
    benchmark_record_histogram_single,
    benchmark_histogram_sync_single_compare,
    benchmark_histogram_sync_parallel_udp_compare,
    benchmark_histogram_sync_parallel_tls_compare
);

#[cfg(not(all(feature = "udp", feature = "tls-collector")))]
criterion_group!(
    benches,
    benchmark_record_histogram,
    benchmark_record_histogram_single,
    benchmark_histogram_sync_single_compare,
    benchmark_histogram_sync_parallel_udp_compare,
);

criterion_main!(benches);
