//! Example runtime loop that retries drain after a short delay.
//!
//! Run with: `cargo run --example shared_runtime_retry`

use rylv_metrics::{DrainMetricCollectorTrait, MetricCollectorTrait, RylvStr, SharedCollector};
use std::thread;
use std::time::Duration;

fn main() {
    let collector = SharedCollector::default();

    for value in [10_u64, 20, 30, 40] {
        collector.histogram(
            RylvStr::from_static("request.latency_ms"),
            value,
            &mut [RylvStr::from_static("route:/users")],
        );
    }

    // External runtime policy: retry later if drain cannot get full ownership yet.
    let retry_delay = Duration::from_millis(25);
    let mut attempts = 0_u32;

    loop {
        attempts += 1;

        if let Some(mut drain) = collector.try_begin_drain() {
            println!("drain acquired after {attempts} attempt(s)");
            for frame in drain.by_ref() {
                println!("{:?}", frame);
            }
            break;
        }

        println!("drain unavailable (attempt {attempts}), retrying...");
        thread::sleep(retry_delay);
    }
}
