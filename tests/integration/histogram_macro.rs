use rylv_metrics::{
    histogram, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, SharedCollector,
    StatsWriterType,
};
use std::time::Duration;

fn random_datadog_addr() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

fn create_test_collector() -> MetricCollector<SharedCollector> {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1500,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        writer_type: StatsWriterType::Simple,
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = random_datadog_addr();

    let inner = SharedCollector::default();
    MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector")
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
