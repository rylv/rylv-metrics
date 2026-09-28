//! Reusing pre-sorted tags with `SortedTags` in a collector.
//!
//! Run with: `cargo run --example sorted_tags`

use rylv_metrics::{
    count_add_sorted, gauge_avg_sorted, histogram_sorted, DrainMetricCollectorTrait,
    MetricCollectorTrait, MetricDrain, RylvStr, RylvTag, SharedCollector,
};

fn main() {
    let collector = SharedCollector::default();

    // Build once, reuse in hot-path metric calls. Compound keeps the key and
    // dynamic value separate until the tags are prepared.
    let route = String::from("/users");
    let request_tags = collector.prepare_sorted_tags([
        RylvTag::from_static("service:web"),
        RylvTag::Compound(RylvStr::from_static("route"), RylvStr::from(route)),
        RylvTag::from_static_compound("env", "prod"),
    ]);

    count_add_sorted!(collector, "requests.total", 1, &request_tags);
    gauge_avg_sorted!(collector, "requests.inflight", 4, &request_tags);
    histogram_sorted!(collector, "requests.latency_ms", 37, &request_tags);

    // Optional: precompute metric + tags hash once for an even faster hot path.
    let prepared = collector.prepare_metric(RylvStr::from_static("requests.total"), request_tags);
    collector.count_add_prepared(&prepared, 1);

    loop {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.frames() {
                println!("{:?}", frame);
            }
            break;
        }
    }
}
