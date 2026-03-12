//! Thread-local aggregation with `TLSCollector` (no UDP transport).
//!
//! Run with: `cargo run --example sorted_tags_tls --features "tls-collector shared-collector"`

use rylv_metrics::{
    count_add_sorted, gauge_sorted, histogram_sorted, sorted_tags, DrainMetricCollectorTrait,
    MetricCollectorTrait, RylvStr, TLSCollector, TLSCollectorOptions,
};

fn main() {
    let collector = TLSCollector::new(TLSCollectorOptions::default());

    let request_tags = sorted_tags!(collector, "service:web", "route:/users", "env:prod");

    count_add_sorted!(collector, "requests.total", 1, &request_tags);
    gauge_sorted!(collector, "requests.inflight", 4, &request_tags);
    histogram_sorted!(collector, "requests.latency_ms", 37, &request_tags);

    let prepared = collector.prepare_metric(RylvStr::from_static("requests.total"), request_tags);
    collector.count_add_prepared(&prepared, 1);

    let drain = collector.try_begin_drain();
    if let Some(mut drain) = drain {
        for frame in drain.by_ref() {
            println!("{:?}", frame);
        }
    }
}
