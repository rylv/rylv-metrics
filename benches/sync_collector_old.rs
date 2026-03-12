use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    SigFig, TLSCollector, TLSCollectorOptions, DEFAULT_STATS_WRITER_TYPE,
};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::spawn;
use std::time::{Duration, Instant};

#[cfg(all(feature = "dhat-heap", not(feature = "allocationcounter")))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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

    let inner = TLSCollector::new(TLSCollectorOptions {
        stats_prefix: String::new(),
        histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
        default_histogram_config: HistogramConfig::new(SigFig::new(0).unwrap(), vec![])
            .unwrap()
            .with_bounds(100, 2_000_000)
            .unwrap(),
        hasher_builder: ahash::RandomState::new(),
    });

    // let inner = SharedCollector::new(SharedCollectorOptions {
    //     stats_prefix: String::new(),
    //     histogram_configs: std::collections::HashMap::with_hasher(ahash::RandomState::new()),
    //     default_histogram_config: HistogramConfig::new(SigFig::new(0).unwrap(), vec![])
    //         .unwrap()
    //         .with_bounds(100, 2_000_000)
    //         .unwrap(),
    //     hasher_builder: ahash::RandomState::new(),
    // });
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

criterion_group!(
    benches,
    benchmark_record_histogram,
    // benchmark_record_histogram_single
);
criterion_main!(benches);
