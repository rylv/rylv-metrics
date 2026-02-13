#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
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
        default_histogram_config: HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
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
    let mut tag_refs: Vec<RylvStr<'_>> = tags.iter().map(|t| RylvStr::from(t.as_str())).collect();
    collector.count(RylvStr::from_static("fuzz.metric"), &mut tag_refs);
    collector.gauge(RylvStr::from_static("fuzz.gauge"), 42, &mut tag_refs);
    collector.histogram(RylvStr::from_static("fuzz.histogram"), 100, &mut tag_refs);

    drop(collector);
});
