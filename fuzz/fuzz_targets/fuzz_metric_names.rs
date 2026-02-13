#![no_main]

use libfuzzer_sys::fuzz_target;
use rylv_metrics::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
use std::time::Duration;

// Fuzz target focusing on edge cases in metric names
fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
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

    // Convert bytes to string (testing various encodings)
    let metric_name = String::from_utf8_lossy(data);

    // Test with empty tags
    let mut empty_tags: [RylvStr<'static>; 0] = [];
    let metric = RylvStr::from(metric_name.as_ref());

    // Try all operations with this potentially malformed metric name
    collector.count(metric.clone(), &mut empty_tags);
    collector.count_add(metric.clone(), 42, &mut empty_tags);
    collector.gauge(metric.clone(), 100, &mut empty_tags);
    collector.histogram(metric, 250, &mut empty_tags);

    drop(collector);
});
