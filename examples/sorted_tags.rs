//! Reusing pre-sorted tags with `SortedTags` in a collector.
//!
//! Run with: `cargo run --example sorted_tags`

use rylv_metrics::{
    count_add_sorted, gauge_sorted, histogram_sorted, sorted_tags, DrainMetricCollectorTrait,
    MetricCollectorTrait, RylvStr, SharedCollector,
};

fn main() {
    let collector = SharedCollector::default();

    // Build once, reuse in hot-path metric calls.
    let request_tags = sorted_tags!(collector, "service:web", "route:/users", "env:prod");

    count_add_sorted!(collector, "requests.total", 1, &request_tags);
    gauge_sorted!(collector, "requests.inflight", 4, &request_tags);
    histogram_sorted!(collector, "requests.latency_ms", 37, &request_tags);

    // Optional: precompute metric + tags hash once for an even faster hot path.
    let prepared = collector.prepare_metric(RylvStr::from_static("requests.total"), request_tags);
    collector.count_add_prepared(&prepared, 1);

    loop {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.by_ref() {
                println!("{:?}", frame);
            }
            break;
        }
    }
}
