//! Basic usage with external drain polling.
//!
//! Run with: `cargo run --example shared_basic`

use rylv_metrics::{DrainMetricCollectorTrait, MetricCollectorTrait, RylvStr, SharedCollector};

fn main() {
    let collector = SharedCollector::default();

    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvStr::from_static("service:web")],
    );
    collector.gauge(
        RylvStr::from_static("memory_mb"),
        256,
        &mut [RylvStr::from_static("service:web")],
    );
    collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvStr::from_static("service:web")],
    );

    // Drain is non-blocking; first call usually schedules a generation swap.
    // Poll until ownership is available, then consume borrowed frames.
    loop {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.by_ref() {
                println!("{:?}", frame);
            }
            break;
        }
    }
}
