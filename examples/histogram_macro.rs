use rylv_metrics::{
    histogram, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, StatsWriterType,
};
use std::time::Duration;

fn main() {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1500,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
        default_sig_fig: rylv_metrics::SigFig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    // Example 1: Static string tags
    histogram!(
        collector,
        "request.duration",
        150,
        "endpoint:api",
        "method:get"
    );

    // Example 2: Mixed static and owned string tags
    let status_code = 200;
    histogram!(
        collector,
        "response.size",
        1024,
        "service:web",
        format!("status:{}", status_code)
    );

    // Example 3: Without tags
    histogram!(collector, "memory.usage", 512);

    // Example 4: Many tags
    histogram!(
        collector,
        "database.query",
        45,
        "db:postgres",
        "table:users",
        "operation:select",
        format!("rows:{}", 100)
    );

    println!("Histogram metrics recorded successfully!");
    println!("The macro automatically handles both static and owned string tags.");
}
