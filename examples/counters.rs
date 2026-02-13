//! Counter examples: `count` (increment by one) and `count_add` (increment by value).
//!
//! Run with: `cargo run --example counters`

use rylv_metrics::{
    count, count_add, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
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
        default_sig_fig: rylv_metrics::SigFig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // --- Direct API (zero-copy for static strings) ---

    // Increment by one
    collector.count(
        RylvStr::from_static("http.requests"),
        &mut [
            RylvStr::from_static("endpoint:/users"),
            RylvStr::from_static("method:get"),
        ],
    );

    // Increment by a specific value
    collector.count_add(
        RylvStr::from_static("bytes.received"),
        4096,
        &mut [RylvStr::from_static("endpoint:/upload")],
    );

    // Counter without tags
    collector.count(RylvStr::from_static("events.processed"), &mut []);

    // --- Macros (more ergonomic, allocates on first key insertion) ---

    count!(collector, "http.requests", "endpoint:/users", "method:post");
    count_add!(collector, "bytes.received", 2048, "endpoint:/upload");
    count!(collector, "events.processed");

    // Dynamic tags via format!
    let endpoint = "/api/v2/data";
    count!(collector, "http.requests", format!("endpoint:{endpoint}"));

    // Multiple calls aggregate: the flush will send the total
    for _ in 0..100 {
        count!(collector, "loop.iterations", "loop:demo");
    }

    collector.shutdown();
    println!("Counter metrics recorded and flushed.");
}
