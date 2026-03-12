//! Gauge examples: point-in-time measurements averaged on flush.
//!
//! Run with: `cargo run --example gauges`

use rylv_metrics::{
    gauge, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr, SharedCollector,
    SharedCollectorOptions, StatsWriterType,
};
use std::time::Duration;

fn main() {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_secs(10),
        writer_type: StatsWriterType::Simple,
    };
    let inner_options = SharedCollectorOptions {
        stats_prefix: "myapp.".to_string(),
        ..Default::default()
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();
    let inner = SharedCollector::new(inner_options);
    let collector = MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector");

    // --- Direct API ---

    collector.gauge(
        RylvStr::from_static("connections.active"),
        42,
        &mut [RylvStr::from_static("pool:main")],
    );

    collector.gauge(
        RylvStr::from_static("queue.depth"),
        150,
        &mut [
            RylvStr::from_static("queue:jobs"),
            RylvStr::from_static("priority:high"),
        ],
    );

    // Gauge without tags
    collector.gauge(RylvStr::from_static("cpu.usage_percent"), 73, &mut []);

    // --- Macros ---

    gauge!(collector, "connections.active", 38, "pool:main");
    gauge!(collector, "disk.free_mb", 20480, "volume:/data");
    gauge!(collector, "threads.running", 8);

    // Multiple gauge values for the same metric/tags are averaged on flush.
    // Here, (42 + 38) / 2 = 40 will be sent for connections.active|pool:main
    // if both calls happen within the same flush interval.

    drop(collector);
    println!("Gauge metrics recorded and flushed.");
}
