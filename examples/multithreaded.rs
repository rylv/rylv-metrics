//! Sharing a MetricCollector across multiple threads using Arc.
//!
//! Run with: `cargo run --example multithreaded`

use rylv_metrics::{
    count, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr, StatsWriterType,
};
use std::sync::Arc;
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
    let collector = Arc::new(MetricCollector::new(bind_addr, datadog_addr, options));

    let mut handles = Vec::new();

    // Spawn worker threads that record metrics concurrently
    for thread_id in 0..4 {
        let collector = collector.clone();
        let handle = std::thread::spawn(move || {
            let tag = format!("thread:{thread_id}");

            for i in 0..100 {
                // Direct API
                collector.histogram(
                    RylvStr::from_static("task.duration"),
                    i * 10,
                    &mut [RylvStr::from_static("pool:workers"), RylvStr::from(&*tag)],
                );

                // Macros work too
                count!(collector, "tasks.completed", format!("thread:{thread_id}"));
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to finish
    for handle in handles {
        handle.join().unwrap();
    }

    // shutdown() requires ownership — unwrap the Arc
    // (only succeeds when no other Arc references remain)
    match Arc::try_unwrap(collector) {
        Ok(c) => c.shutdown(),
        Err(_) => eprintln!("Warning: other Arc references still alive, cannot shutdown cleanly"),
    }

    println!("All threads finished. Metrics flushed.");
}
