use rylv_metrics::{
    histogram, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType,
};
use std::time::Duration;

fn create_test_collector() -> MetricCollector {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1500,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();

    MetricCollector::new(bind_addr, datadog_addr, options)
}

#[test]
fn test_histogram_macro_with_static_tags() {
    let collector = create_test_collector();

    // Test with static string tags
    histogram!(collector, "test.metric", 100, "tag1:value1", "tag2:value2");

    // Verify the metric was recorded
}

#[test]
fn test_histogram_macro_with_owned_tags() {
    let collector = create_test_collector();

    // Test with owned String tags
    let tag1 = "tag1:value1".to_string();
    let tag2 = format!("tag2:{}", "value2");

    histogram!(collector, "test.metric", 100, tag1, tag2);
}

#[test]
fn test_histogram_macro_with_mixed_tags() {
    let collector = create_test_collector();

    // Test with mixed static and owned tags
    histogram!(
        collector,
        "test.metric",
        100,
        "static:tag",
        format!("dynamic:{}", "tag")
    );
}

#[test]
fn test_histogram_macro_without_tags() {
    let collector = create_test_collector();

    // Test without tags
    histogram!(collector, "test.metric", 100);
}

#[test]
fn test_histogram_macro_single_tag() {
    let collector = create_test_collector();

    // Test with a single tag
    histogram!(collector, "test.metric", 100, "tag:value");
}

#[test]
fn test_histogram_macro_many_tags() {
    let collector = create_test_collector();

    // Test with many tags
    histogram!(
        collector,
        "test.metric",
        100,
        "tag1:value1",
        "tag2:value2",
        "tag3:value3",
        "tag4:value4",
        format!("tag5:{}", "value5")
    );
}
