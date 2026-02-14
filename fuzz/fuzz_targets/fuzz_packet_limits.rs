#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
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
        default_histogram_config: HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Create a large metric name and many tags to test packet limits
    let metric_base = String::from_utf8_lossy(&data[3..]);
    let metric_name = format!("test.metric.{}", metric_base);

    // Create many tags
    let tags: Vec<String> = (0..20)
        .map(|i| format!("tag{}:value{}", i, metric_base))
        .collect();

    // Send multiple metrics to test batching and packet limits
    for i in 0..10 {
        let metric_count = format!("{}.{}", metric_name, i);
        let metric_gauge = format!("{}.gauge.{}", metric_name, i);
        let metric_hist = format!("{}.hist.{}", metric_name, i);
        let mut tag_refs: Vec<RylvStr<'_>> = tags.iter().map(|t| RylvStr::from(t.as_str())).collect();
        collector.count(RylvStr::from(metric_count.as_str()), &mut tag_refs);
        collector.gauge(RylvStr::from(metric_gauge.as_str()), i as u64, &mut tag_refs);
        collector.histogram(
            RylvStr::from(metric_hist.as_str()),
            i as u64 * 100,
            &mut tag_refs,
        );
    }

    drop(collector);
});
