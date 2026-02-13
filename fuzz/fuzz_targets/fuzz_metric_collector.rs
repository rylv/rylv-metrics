#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
use std::time::Duration;

// Fuzz target that tests the metric collector with various random inputs
fuzz_target!(|data: &[u8]| {
    if data.len() < 10 {
        return;
    }

    // Create a collector instance (using a throwaway UDP address)
    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
        default_histogram_config: HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Parse the fuzz input
    let op = data[0] % 4; // Operation type
    let value = u64::from_le_bytes([
        data[1], data[2], data[3], data[4],
        data[5], data[6], data[7], data[8],
    ]);

    // Use remaining bytes as metric name and tags
    let remaining = &data[9..];
    let split_point = remaining.len() / 2;

    // Convert bytes to strings (lossy conversion is OK for fuzzing)
    let metric_name = String::from_utf8_lossy(&remaining[..split_point]);
    let tags_str = String::from_utf8_lossy(&remaining[split_point..]);

    // Split tags by common delimiters
    let tags: Vec<String> = tags_str
        .split(|c: char| c == ',' || c == ';' || c == ' ')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Execute different operations based on fuzz input
    let metric = RylvStr::from(metric_name.as_ref());
    let mut tag_refs: Vec<RylvStr<'_>> = tags.iter().map(|t| RylvStr::from(t.as_str())).collect();

    match op {
        0 => collector.count(metric, &mut tag_refs),
        1 => collector.count_add(metric, value, &mut tag_refs),
        2 => collector.gauge(metric, value, &mut tag_refs),
        3 => collector.histogram(metric, value, &mut tag_refs),
        _ => unreachable!(),
    }

    // Cleanup
    drop(collector);
});
