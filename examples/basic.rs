//! Basic usage of all four metric types using the direct API.
//!
//! Run with: `cargo run --example basic`

use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr, StatsWriterType,
};
use std::time::Duration;

fn main() {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_secs(10),
        stats_prefix: "myapp.".to_string(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Histogram — tracks distribution of values (emits min, avg, max, 99th percentile, count)
    collector.histogram(
        RylvStr::from_static("request.latency"),
        42,
        &mut [
            RylvStr::from_static("endpoint:api"),
            RylvStr::from_static("method:get"),
        ],
    );

    // Counter — increments by one
    collector.count(
        RylvStr::from_static("request.count"),
        &mut [RylvStr::from_static("endpoint:api")],
    );

    // Counter add — increments by an arbitrary value
    collector.count_add(
        RylvStr::from_static("bytes.sent"),
        1024,
        &mut [RylvStr::from_static("endpoint:api")],
    );

    // Gauge — records a point-in-time value (averaged when multiple values per flush)
    collector.gauge(
        RylvStr::from_static("connections.active"),
        100,
        &mut [RylvStr::from_static("pool:main")],
    );

    // Metrics without tags
    collector.histogram(RylvStr::from_static("memory.usage"), 512, &mut []);
    collector.count(RylvStr::from_static("heartbeat"), &mut []);

    // Shutdown flushes pending metrics before exiting
    collector.shutdown();

    println!("All metric types recorded and flushed.");
}
