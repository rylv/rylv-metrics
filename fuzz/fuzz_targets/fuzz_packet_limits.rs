#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType};
use std::time::Duration;

// Fuzz target focusing on packet size limits and edge cases
fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    // Use first 2 bytes to determine packet size (within reasonable limits)
    let packet_size = u16::from_le_bytes([data[0], data[1]]);
    let packet_size = (packet_size % 8192).max(64); // 64 to 8192 bytes

    // Use next byte to determine writer type
    let writer_type = match data[2] % 3 {
        0 => StatsWriterType::Simple,
        #[cfg(target_os = "linux")]
        1 => StatsWriterType::LinuxBatch,
        #[cfg(target_vendor = "apple")]
        2 => StatsWriterType::AppleBatch,
        _ => StatsWriterType::Simple,
    };

    let options = MetricCollectorOptions {
        max_udp_packet_size: packet_size,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type,
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Create a large metric name and many tags to test packet limits
    let metric_base = String::from_utf8_lossy(&data[3..]);
    let metric_name = format!("test.metric.{}", metric_base);

    // Create many tags
    let mut tags: Vec<String> = (0..20)
        .map(|i| format!("tag{}:value{}", i, metric_base))
        .collect();

    // Send multiple metrics to test batching and packet limits
    for i in 0..10 {
        collector.increment_by_one(format!("{}.{}", metric_name, i), &mut tags);
        collector.gauge(format!("{}.gauge.{}", metric_name, i), i as u64, &mut tags);
        collector.histogram(format!("{}.hist.{}", metric_name, i), i as u64 * 100, &mut tags);
    }

    drop(collector);
});
