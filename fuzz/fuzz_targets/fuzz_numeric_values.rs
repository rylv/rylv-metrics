#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
use std::time::Duration;

// Fuzz target focusing on numeric edge cases
fuzz_target!(|data: &[u8]| {
    if data.len() < 16 {
        return;
    }

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1024,
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

    // Extract two u64 values from the fuzz input
    let value1 = u64::from_le_bytes([
        data[0], data[1], data[2], data[3],
        data[4], data[5], data[6], data[7],
    ]);
    let value2 = u64::from_le_bytes([
        data[8], data[9], data[10], data[11],
        data[12], data[13], data[14], data[15],
    ]);

    let tags = vec!["fuzz:test".to_string()];
    let mut tag_refs: Vec<RylvStr<'_>> = tags.iter().map(|t| RylvStr::from(t.as_str())).collect();

    // Test with potentially extreme values
    collector.count_add(RylvStr::from_static("fuzz.counter"), value1, &mut tag_refs);
    collector.gauge(RylvStr::from_static("fuzz.gauge"), value2, &mut tag_refs);
    collector.histogram(RylvStr::from_static("fuzz.histogram"), value1, &mut tag_refs);

    // Test common edge cases
    collector.count_add(RylvStr::from_static("fuzz.zero"), 0, &mut tag_refs);
    collector.count_add(RylvStr::from_static("fuzz.max"), u64::MAX, &mut tag_refs);
    collector.gauge(RylvStr::from_static("fuzz.one"), 1, &mut tag_refs);
    collector.histogram(RylvStr::from_static("fuzz.edge"), u64::MAX / 2, &mut tag_refs);

    // Test rapid updates to the same metric (aggregation stress test)
    for _ in 0..100 {
        collector.count_add(RylvStr::from_static("fuzz.rapid"), value1, &mut tag_refs);
    }

    drop(collector);
});
