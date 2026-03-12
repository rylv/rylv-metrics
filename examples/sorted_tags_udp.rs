//! `SortedTags` and `PreparedMetric` with `MetricCollector`.
//!
//! Run with: `cargo run --example sorted_tags_udp --features udp`
//!
//! Note:
//! - `PreparedMetric` is great for single-threaded hot paths.
//! - For heavily shared multi-threaded UDP collectors, prefer `*_sorted`.

use rylv_metrics::{
    count_add_sorted, histogram_sorted, sorted_tags, MetricCollector, MetricCollectorOptions,
    MetricCollectorTrait, RylvStr, SharedCollector, SharedCollectorOptions, StatsWriterType,
};
use std::time::Duration;

fn main() {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_secs(2),
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

    let tags = sorted_tags!(collector, "service:web", "route:/users", "env:prod");

    count_add_sorted!(collector, "requests.total", 1, &tags);
    histogram_sorted!(collector, "requests.latency_ms", 42, &tags);

    // Single-thread fast path.
    let prepared = collector.prepare_metric(RylvStr::from_static("requests.total"), tags);
    collector.count_add_prepared(&prepared, 10);

    drop(collector);
}
