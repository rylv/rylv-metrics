#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
use std::time::Duration;

// Fuzz target focusing on edge cases in tags
fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let options = MetricCollectorOptions {
        max_udp_packet_size: 2048,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Split the data into chunks to create multiple tags
    let mut tags: Vec<String> = Vec::new();
    let mut start = 0;
    let chunk_size = if data.len() > 10 { data.len() / 10 } else { 1 };

    for _ in 0..10 {
        if start >= data.len() {
            break;
        }
        let end = std::cmp::min(start + chunk_size, data.len());
        let tag = String::from_utf8_lossy(&data[start..end]).to_string();
        if !tag.is_empty() {
            tags.push(tag);
        }
        start = end;
    }

    // Test with various tag combinations
    collector.increment_by_one("fuzz.metric", &mut tags);
    collector.gauge("fuzz.gauge", 42, &mut tags);
    collector.histogram("fuzz.histogram", 100, &mut tags);

    drop(collector);
});
