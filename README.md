# rylv-metrics

A high-performance DogStatsD metrics client for Rust with support for client-side aggregation.

## Features

- **High Performance**: Lock-free data structures and optimized UDP batching
- **Client-Side Aggregation**: Reduces network overhead by aggregating metrics before sending
- **Multiple Writer Backends**:
  - `Simple`: Standard UDP writer (all platforms)
  - `LinuxBatch`: Uses `sendmmsg` for batch UDP writes (Linux only)
  - `AppleBatch`: Uses `sendmsg_x` for batch UDP writes (macOS only)
  - `Custom`: Bring your own writer implementation
- **Metric Types**: Histograms, Counters, and Gauges
- **Flexible Tags**: Support for static and owned string tags
- **Configurable Histograms**: Adjustable significant figures for histogram precision

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rylv-metrics = "0.1"
```

## Quick Start

```rust
use rylv_metrics::{
    count, count_add, gauge, histogram, MetricCollector, MetricCollectorOptions,
    MetricCollectorTrait, RylvStr,
};
use std::net::SocketAddr;
use std::time::Duration;

fn main() {
    // Configure the collector
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_secs(10),
        stats_prefix: "myapp.".to_string(),
        writer_type: rylv_metrics::DEFAULT_STATS_WRITER_TYPE,
        histogram_configs: Default::default(),
        default_sig_fig: rylv_metrics::SigFig::default(),
        hasher_builder: std::hash::RandomState::new(),
    };

    // Create the collector
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let dst_addr: SocketAddr = "127.0.0.1:8125".parse().unwrap();
    let collector = MetricCollector::new(bind_addr, dst_addr, options);

    // Record metrics
    collector.histogram(
        RylvStr::from_static("request.latency"),
        42,
        &mut [RylvStr::from_static("endpoint:api"), RylvStr::from_static("method:GET")],
    );
    collector.count(
        RylvStr::from_static("request.count"),
        &mut [RylvStr::from_static("endpoint:api")],
    );
    collector.gauge(
        RylvStr::from_static("connections.active"),
        100,
        &mut [RylvStr::from_static("pool:main")],
    );

    // Or use convenience macros with string literals
    histogram!(collector, "request.latency", 42, "endpoint:api", "method:GET");
    count!(collector, "request.count", "endpoint:api");
    count_add!(collector, "bytes.sent", 1024, "endpoint:api");
    gauge!(collector, "connections.active", 100, "pool:main");

    // Collector flushes automatically on drop
}
```

## Metric Types

### Histogram

Records distribution of values with configurable precision:

```rust
histogram!(collector, "response.time", 150, "service:api");
```

### Counter

Increments a counter, aggregated client-side:

```rust
count!(collector, "requests.total", "status:200");
count_add!(collector, "bytes.sent", 1024, "endpoint:upload");
```

### Gauge

Records point-in-time values:

```rust
gauge!(collector, "memory.used", 1024000, "host:server1");
```

## Custom Writer

Implement `StatsWriterTrait` for custom metric destinations:

```rust
use rylv_metrics::{StatsWriterTrait, MetricResult, StatsWriterType};

struct MyWriter { /* ... */ }

impl StatsWriterTrait for MyWriter {
    fn metric_copied(&self) -> bool { false }

    fn write(&mut self, metrics: &[&str], tags: &str, value: &str, metric_type: &str) -> MetricResult<()> {
        // Your implementation
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        Ok(0)
    }

    fn reset(&mut self) {}
}

// Use with StatsWriterType::Custom(Box::new(MyWriter { ... }))
```

## Feature Flags

- `dhat-heap`: Enables heap profiling support via `dhat`
- `allocationcounter`: Enables allocation counting instrumentation

## Platform Support

- Linux (x86_64, aarch64)
- macOS (x86_64, aarch64)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
