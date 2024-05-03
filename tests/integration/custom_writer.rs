use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, MetricResult, RylvStr,
    StatsWriterTrait, StatsWriterType,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A custom writer that collects metrics in datadog wire format for testing
#[derive(Clone)]
pub struct TestStatsWriter {
    /// Stores metrics in datadog wire format
    metrics: Arc<Mutex<Vec<String>>>,
    max_udp_packet_size: u16,
    stats_prefix: String,
    current_buffer: Arc<Mutex<String>>,
}

impl TestStatsWriter {
    pub fn new(max_udp_packet_size: u16, stats_prefix: String) -> Self {
        Self {
            metrics: Arc::new(Mutex::new(Vec::new())),
            max_udp_packet_size,
            stats_prefix,
            current_buffer: Arc::new(Mutex::new(String::with_capacity(
                max_udp_packet_size as usize,
            ))),
        }
    }

    pub fn get_all_metrics_as_text(&self) -> String {
        self.metrics.lock().unwrap().join("")
    }
}

impl StatsWriterTrait for TestStatsWriter {
    fn metric_copied(&self) -> bool {
        true
    }

    fn write(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: &str,
    ) -> MetricResult<()> {
        let mut buffer = self.current_buffer.lock().unwrap();

        // Build the metric in datadog wire format
        // Format: prefix + metric_name:value|type|#tags\n
        // or prefix + metric_name:value|type\n (when no tags)

        let mut metric_line = String::new();
        metric_line.push_str(&self.stats_prefix);

        for metric in metrics {
            metric_line.push_str(metric);
        }

        metric_line.push(':');
        metric_line.push_str(value);
        metric_line.push('|');
        metric_line.push_str(metric_type);

        if !tags.is_empty() {
            metric_line.push_str("|#");
            metric_line.push_str(tags);
        }

        metric_line.push('\n');

        let metric_len = metric_line.len();

        // Check if metric is too large
        if metric_len > self.max_udp_packet_size as usize {
            return Err(format!("Metric is larger than {}", self.max_udp_packet_size).into());
        }

        // If buffer + new metric exceeds max size, flush current buffer
        if buffer.len() + metric_len > self.max_udp_packet_size as usize && !buffer.is_empty() {
            self.metrics.lock().unwrap().push(buffer.clone());
            buffer.clear();
        }

        buffer.push_str(&metric_line);

        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        let mut buffer = self.current_buffer.lock().unwrap();

        if !buffer.is_empty() {
            self.metrics.lock().unwrap().push(buffer.clone());
            let size = buffer.len();
            buffer.clear();
            return Ok(size);
        }

        Ok(0)
    }

    fn reset(&mut self) {
        let mut buffer = self.current_buffer.lock().unwrap();
        buffer.clear();
    }
}

// ============================================================================
// Tests for Custom Writer
// ============================================================================

#[test]
fn test_custom_writer_basic() -> std::io::Result<()> {
    let writer = TestStatsWriter::new(512, String::new());
    let writer_clone = writer.clone();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Test counter
    collector.count(
        RylvStr::from_static("custom.counter"),
        &mut [RylvStr::from_static("env:test")],
    );
    collector.count_add(
        RylvStr::from_static("custom.counter.value"),
        42,
        &mut [RylvStr::from_static("env:prod")],
    );

    // Test gauge
    collector.gauge(
        RylvStr::from_static("custom.gauge"),
        100,
        &mut [RylvStr::from_static("host:server1")],
    );

    // Test histogram
    collector.histogram(
        RylvStr::from_static("custom.histogram"),
        250,
        &mut [RylvStr::from_static("endpoint:/api")],
    );

    // Wait for flush
    std::thread::sleep(Duration::from_millis(150));

    let metrics = writer_clone.get_all_metrics_as_text();

    // Verify datadog wire format
    assert!(
        metrics.contains("custom.counter:1|c|#env:test\n"),
        "Should contain counter metric in correct format"
    );
    assert!(
        metrics.contains("custom.counter.value:42|c|#env:prod\n"),
        "Should contain counter with value in correct format"
    );
    assert!(
        metrics.contains("custom.gauge:100|g|#host:server1\n"),
        "Should contain gauge metric in correct format"
    );
    assert!(
        metrics.contains("custom.histogram.count:1|c|#endpoint:/api\n"),
        "Should contain histogram count in correct format"
    );
    assert!(
        metrics.contains("custom.histogram.min:250|g|#endpoint:/api\n"),
        "Should contain histogram min in correct format"
    );
    assert!(
        metrics.contains("custom.histogram.max:250|g|#endpoint:/api\n"),
        "Should contain histogram max in correct format"
    );
    assert!(
        metrics.contains("custom.histogram.avg:") && metrics.contains("|g|#endpoint:/api\n"),
        "Should contain histogram avg in correct format"
    );
    assert!(
        metrics.contains("custom.histogram.99percentile:")
            && metrics.contains("|g|#endpoint:/api\n"),
        "Should contain histogram 99percentile in correct format"
    );

    Ok(())
}

#[test]
fn test_custom_writer_no_tags() -> std::io::Result<()> {
    let writer = TestStatsWriter::new(512, String::new());
    let writer_clone = writer.clone();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    let mut empty_tags: [RylvStr<'_>; 0] = [];
    collector.count(RylvStr::from_static("notags.counter"), &mut empty_tags);
    collector.gauge(RylvStr::from_static("notags.gauge"), 100, &mut empty_tags);
    collector.histogram(
        RylvStr::from_static("notags.histogram"),
        250,
        &mut empty_tags,
    );

    std::thread::sleep(Duration::from_millis(150));

    let metrics = writer_clone.get_all_metrics_as_text();

    // Verify datadog wire format without tags (no |# separator)
    assert!(
        metrics.contains("notags.counter:1|c\n"),
        "Should contain counter without tags"
    );
    assert!(
        metrics.contains("notags.gauge:100|g\n"),
        "Should contain gauge without tags"
    );
    assert!(
        metrics.contains("notags.histogram.count:1|c\n"),
        "Should contain histogram count without tags"
    );

    // Verify no tags are present
    assert!(
        !metrics.contains("notags.counter:1|c|#"),
        "Counter should not have tag separator"
    );
    assert!(
        !metrics.contains("notags.gauge:100|g|#"),
        "Gauge should not have tag separator"
    );

    Ok(())
}

#[test]
fn test_custom_writer_with_prefix() -> std::io::Result<()> {
    let writer = TestStatsWriter::new(1024, "app.".to_string());
    let writer_clone = writer.clone();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1024,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: "app.".to_string(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvStr::from_static("method:GET")],
    );
    collector.gauge(
        RylvStr::from_static("memory"),
        512,
        &mut [RylvStr::from_static("unit:mb")],
    );
    collector.histogram(
        RylvStr::from_static("latency"),
        150,
        &mut [RylvStr::from_static("service:api")],
    );

    std::thread::sleep(Duration::from_millis(150));

    let metrics = writer_clone.get_all_metrics_as_text();

    // Verify prefix is included in wire format
    assert!(
        metrics.contains("app.requests:1|c|#method:GET\n"),
        "Should contain prefix in counter metric"
    );
    assert!(
        metrics.contains("app.memory:512|g|#unit:mb\n"),
        "Should contain prefix in gauge metric"
    );
    assert!(
        metrics.contains("app.latency.count:1|c|#service:api\n"),
        "Should contain prefix in histogram count"
    );
    assert!(
        metrics.contains("app.latency.min:150|g|#service:api\n"),
        "Should contain prefix in histogram min"
    );

    Ok(())
}

#[test]
fn test_custom_writer_aggregation() -> std::io::Result<()> {
    let writer = TestStatsWriter::new(1024, String::new());
    let writer_clone = writer.clone();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1024,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Test counter aggregation
    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );
    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );
    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );

    // Test gauge aggregation
    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        50,
        &mut [RylvStr::from_static("host:web1")],
    );
    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        75,
        &mut [RylvStr::from_static("host:web1")],
    );
    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        90,
        &mut [RylvStr::from_static("host:web1")],
    );

    // Test histogram aggregation
    collector.histogram(
        RylvStr::from_static("request.duration"),
        100,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        200,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        150,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        300,
        &mut [RylvStr::from_static("endpoint:/users")],
    );

    std::thread::sleep(Duration::from_millis(150));

    let metrics = writer_clone.get_all_metrics_as_text();

    // Verify counter aggregation
    assert!(
        metrics.contains("page.views:3|c|#page:home\n"),
        "Counter should aggregate to 3"
    );

    // Verify gauge aggregation (should have gauge metric)
    assert!(
        metrics.contains("cpu.usage:") && metrics.contains("|g|#host:web1\n"),
        "Gauge should be present with correct format"
    );

    // Verify histogram aggregation
    assert!(
        metrics.contains("request.duration.count:4|c|#endpoint:/users\n"),
        "Histogram count should be 4"
    );
    assert!(
        metrics.contains("request.duration.min:100|g|#endpoint:/users\n"),
        "Histogram min should be 100"
    );
    assert!(
        metrics.contains("request.duration.max:300|g|#endpoint:/users\n"),
        "Histogram max should be 300"
    );

    Ok(())
}

#[test]
fn test_custom_writer_multiple_tags() -> std::io::Result<()> {
    let writer = TestStatsWriter::new(1024, String::new());
    let writer_clone = writer.clone();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1024,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:9999".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Test with multiple tags (they should be sorted)
    collector.count(
        RylvStr::from_static("multi.tag.metric"),
        &mut [
            RylvStr::from_static("tag3:value3"),
            RylvStr::from_static("tag1:value1"),
            RylvStr::from_static("tag2:value2"),
        ],
    );

    std::thread::sleep(Duration::from_millis(150));

    let metrics = writer_clone.get_all_metrics_as_text();

    // RylvStrs should be sorted alphabetically
    assert!(
        metrics.contains("multi.tag.metric:1|c|#tag1:value1,tag2:value2,tag3:value3\n"),
        "RylvStrs should be sorted alphabetically in wire format"
    );

    Ok(())
}
