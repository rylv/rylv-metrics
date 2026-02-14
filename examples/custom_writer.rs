//! Using a custom writer to capture metrics instead of sending them via UDP.
//!
//! Run with: `cargo run --example custom_writer`

use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, MetricResult, RylvStr,
    StatsWriterTrait, StatsWriterType,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A custom writer that stores metrics in a shared Vec instead of sending UDP.
struct InMemoryWriter {
    lines: Arc<Mutex<Vec<String>>>,
}

impl StatsWriterTrait for InMemoryWriter {
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
        let metric_name: String = metrics.iter().copied().collect();
        let line = if tags.is_empty() {
            format!("{metric_name}:{value}|{metric_type}")
        } else {
            format!("{metric_name}:{value}|{metric_type}|#{tags}")
        };
        self.lines.lock().unwrap().push(line);
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        Ok(0)
    }

    fn reset(&mut self) {}
}

fn main() {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let writer = InMemoryWriter {
        lines: lines.clone(),
    };

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_millis(50),
        stats_prefix: "app.".to_string(),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
        histogram_configs: std::collections::HashMap::new(),
        default_histogram_config: rylv_metrics::HistogramConfig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Record some metrics
    collector.count(
        RylvStr::from_static("request.count"),
        &mut [RylvStr::from_static("endpoint:api")],
    );
    collector.gauge(
        RylvStr::from_static("connections"),
        10,
        &mut [RylvStr::from_static("pool:main")],
    );

    // Shutdown triggers a final flush
    collector.shutdown();

    // Inspect captured metrics
    let captured = lines.lock().unwrap();
    println!("Captured {} metric lines:", captured.len());
    for line in captured.iter() {
        println!("  {line}");
    }
}
