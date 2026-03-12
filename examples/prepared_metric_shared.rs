//! Pure shared-collector example using `PreparedMetric`.
//!
//! Run with: `cargo run --example prepared_metric_shared`

use rylv_metrics::{DrainMetricCollectorTrait, MetricCollectorTrait, RylvStr, SharedCollector};

fn main() {
    let collector = SharedCollector::default();
    let tags = collector.prepare_sorted_tags([
        RylvStr::from_static("service:api"),
        RylvStr::from_static("env:prod"),
    ]);

    let prepared_count = collector.prepare_metric(RylvStr::from_static("requests.total"), tags);
    for _ in 0..5 {
        collector.count_add_prepared(&prepared_count, 1);
    }

    loop {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.by_ref() {
                println!("{:?}", frame);
            }
            break;
        }
    }
}
