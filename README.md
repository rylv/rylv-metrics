[![CI](https://github.com/rylv/rylv-metrics/actions/workflows/ci.yml/badge.svg)](https://github.com/rylv/rylv-metrics/actions/workflows/ci.yml)

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
- **Configurable Histograms**: Adjustable significant figures, custom percentile lists, and optional base metrics (`count`, `min`, `avg`, `max`)
- **Shared Collector Mode**: Use `SharedCollector` to aggregate and drain metrics without background threads or network I/O
- **Thread-Local Collector Mode**: Use `TLSCollector` for per-thread aggregation that scales linearly under contention

## Installation

Requires Rust 1.90 or newer.

Add to your `Cargo.toml`:

```toml
[dependencies]
rylv-metrics = "0.6.1"
```

Default build enables no transport or collector backend features.
Enable the APIs you want explicitly. For example, UDP sending with a inner collector requires:

```toml
[dependencies]
rylv-metrics = { version = "0.6.1", features = ["udp", "shared-collector"] }
```

## Quick Start

```rust
use rylv_metrics::{
    count, count_add, gauge_avg, histogram, MetricCollector, MetricCollectorOptions,
    MetricCollectorTrait, RylvStr, RylvTag, SharedCollector,
};
use std::net::SocketAddr;
use std::time::Duration;

fn main() {
    // Configure the collector
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_secs(10),
        writer_type: rylv_metrics::DEFAULT_STATS_WRITER_TYPE,
        ..Default::default()
    };
    // Create the collector
    let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let dst_addr: SocketAddr = "127.0.0.1:8125".parse().unwrap();
    let inner = SharedCollector::default();
    let collector = MetricCollector::new(bind_addr, dst_addr, options, inner).unwrap();

    // Record metrics
    collector.histogram(
        RylvStr::from_static("request.latency"),
        42,
        &mut [RylvTag::from_static("endpoint:api"), RylvTag::from_static("method:GET")],
    );
    collector.count(
        RylvStr::from_static("request.count"),
        &mut [RylvTag::from_static("endpoint:api")],
    );
    collector.gauge_avg(
        RylvStr::from_static("connections.active"),
        100,
        &mut [RylvTag::from_static("pool:main")],
    );

    // Or use convenience macros with string literals
    histogram!(collector, "request.latency", 42, "endpoint:api", "method:GET");
    count!(collector, "request.count", "endpoint:api");
    count_add!(collector, "bytes.sent", 1024, "endpoint:api");
    gauge_avg!(collector, "connections.active", 100, "pool:main");

    // Drop triggers a final best-effort flush
}
```

## Tags

Use `RylvTag::from_parts` when the tag key is static and its value is generated at runtime. It serializes the same way as a full `"route:/users"` tag:

```rust
use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTag, SharedCollector};

let collector = SharedCollector::default();
let route = String::from("/users");
let tags = collector.prepare_sorted_tags([
    RylvTag::from_static("service:web"),
    RylvTag::from_parts(RylvStr::from_static("route"), route),
]);
collector.count_add_sorted(RylvStr::from_static("requests.total"), 1, &tags);
```

`prepare_sorted_tags` resolves and sorts the tags once, so `tags` can be reused across metric calls. A `(key, value)` tuple also converts to `RylvTag` when both parts convert to `RylvStr`. For two static parts, use `RylvTag::from_static_compound("env", "prod")`.

### Typed tags

Enable the optional `derive` feature alongside a collector feature:

```toml
rylv-metrics = { version = "0.6", features = ["derive", "shared-collector"] }
```

```rust
use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTags, SharedCollector};

#[derive(RylvTags)]
struct RequestTags<'a> {
    env: RylvStr<'a>,
    #[rylv(rename = "http.route")]
    route: RylvStr<'a>,
    status: RylvStr<'a>,
}

let collector = SharedCollector::default();
let tags = RequestTags {
    env: RylvStr::from_static("prod"),
    route: RylvStr::from_static("/users"),
    status: RylvStr::from_static("200"),
};
collector.count_add_typed(RylvStr::from_static("requests.total"), 1, &tags);
```

Each included field must be `RylvStr` or `Option<RylvStr>`; `None` omits a tag.
Use `from_static` for known constants,
`Borrowed` for temporary values, and `Owned` or `OwnedStr` for owned values.
The derive computes canonical key order at compile time. `sorted_tags()` returns
a stack-backed `RylvTagsGuard` that owns the temporary tag array. `*_typed`
uses this guard without sorting on each call, while hashing the current values.
Use the slice API for keys or tag counts chosen at runtime. Use
`SortedTags` when the entire set can be prepared and reused, since it also
caches the tags hash. Use `PreparedMetric` when the metric name and tag values
are both reusable; it also caches the metric identity.

For an inline tag set, use `rylv_tags!` with the same `derive` feature:

```rust
collector.count_typed(
    RylvStr::from_static("requests.total"),
    &rylv_metrics::rylv_tags! {
        env: RylvStr::from_static("prod"),
        #[rylv(rename = "http-route")]
        route: RylvStr::from_static("/users"),
        region?: None,
    },
);
```

The macro creates a local struct and its `RylvTags` implementation. Append `?`
to mark an optional field. Expressions run once in declaration order; tag keys
are ordered at compile time.

## Metric Types

### Histogram

Records distribution of values with configurable precision and percentiles:

```rust
histogram!(collector, "response.time", 150, "service:api");
```

You can configure per-metric or default histogram behavior with `HistogramConfig`
(significant figures, percentile list, and base metric toggles).

### Counter

Increments a counter, aggregated client-side. The window's net total is sent on
flush; negative deltas are supported, so the net can be negative (a zero net is
skipped):

```rust
count!(collector, "requests.total", "status:200");
count_add!(collector, "bytes.sent", 1024, "endpoint:upload");
count_add!(collector, "inflight.requests", -1, "endpoint:upload");
```

### Gauge

Records point-in-time values. `gauge_avg` averages the recorded values over the
window and emits the real mean (negative values included); `gauge` is
last-write-wins:

```rust
gauge_avg!(collector, "memory.used", 1024000, "host:server1");
gauge!(collector, "queue.depth", 42, "host:server1");
```

## Custom Writer

Implement `StatsWriterTrait` for custom metric destinations:

```rust
use rylv_metrics::{MetricKind, MetricResult, StatsWriterTrait, StatsWriterType};

struct MyWriter { /* ... */ }

impl StatsWriterTrait for MyWriter {
    fn metric_copied(&self) -> bool { false }

    fn write(&mut self, metrics: &[&str], tags: &str, value: &str, metric_type: MetricKind) -> MetricResult<()> {
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

## Shared Collector

Use `SharedCollector` when you want to own scheduling and transport externally:

```rust
use rylv_metrics::{DrainMetricCollectorTrait, MetricDrain, MetricCollectorTrait, RylvStr, RylvTag, SharedCollector};

let collector = SharedCollector::default();
collector.count(RylvStr::from_static("requests"), &mut [RylvTag::from_static("env:test")]);

loop {
    if let Some(mut drain) = collector.try_begin_drain() {
        for frame in drain.frames() {
            // send frame to UDP/HTTP/queue/etc
            println!("{:?}", frame);
        }
        break;
    }
}
```

The drain owns the detached aggregator; `drain.frames()` returns a separate
iterator borrowing it. Frames can outlive the iterator, for example while a
transport batches them, but must be released before the drain is dropped or
borrowed again. Names and tags reference the existing storage. Empty entries
are removed when the drain is dropped, after all frame borrows have ended.

## TLS Collector

Use `TLSCollector` when you want the lowest possible write latency under
multi-threaded contention. Each thread aggregates into its own local state
backed by `hashbrown::HashTable` and `parking_lot`, and drains merge the
per-thread frames on demand:

```rust
use rylv_metrics::{
    DrainMetricCollectorTrait, MetricDrain, MetricCollectorTrait, RylvStr, RylvTag,
    TLSCollector, TLSCollectorOptions,
};

let collector = TLSCollector::new(TLSCollectorOptions::default());
collector.count(RylvStr::from_static("requests"), &mut [RylvTag::from_static("env:test")]);

if let Some(mut drain) = collector.try_begin_drain() {
    for frame in drain.frames() {
        // send frame to UDP/HTTP/queue/etc
        println!("{:?}", frame);
    }
}
```

`TLSCollector` plugs into `MetricCollector` as an inner collector just like
`SharedCollector`, so it can be combined with any of the UDP writer backends.
Enable it via the `tls-collector` feature.

## SortedTags And PreparedMetric

For hot paths, you can precompute tag handling:

- `SortedTags`: sorts and joins tags once, then reuse with `*_sorted`.
- `PreparedMetric`: precomputes metric+tags identity, then reuse with `*_prepared`.

Guidance for concurrency:

- Single-thread / low contention: `PreparedMetric` is usually fastest.
- Multi-thread with `TLSCollector`: `PreparedMetric` tends to scale well.
- Multi-thread with shared `MetricCollector`: `PreparedMetric` can add contention;
  prefer `SortedTags` + `*_sorted`.

See examples:

- `examples/sorted_tags.rs`
- `examples/prepared_metric_shared.rs`
- `examples/sorted_tags_udp.rs`

### Benchmark Snapshot (Lower Is Better)

Measured on the current machine with `thread_local_compare` (`histogram_*` path):

| Scenario | Variant | Time (ns) | Throughput (M ops/s) |
|---|---|---:|---:|
| Single-thread | regular | 43.36 | 23.07 |
| Single-thread | sorted | 35.56 | 28.13 |
| Single-thread | prepared | 14.44 | 69.27 |
| Multi-thread shared collector | regular | 73.47 | 13.61 |
| Multi-thread shared collector | sorted | 44.92 | 22.26 |
| Multi-thread shared collector | prepared | 25.60 | 39.07 |
| Multi-thread TLS collector | regular | 7.53 | 132.87 |
| Multi-thread TLS collector | sorted | 4.73 | 211.38 |
| Multi-thread TLS collector | prepared | 3.02 | 330.65 |

Reproduce this snapshot with:

```bash
cargo bench --bench thread_local_compare --features "udp tls-collector custom_writer shared-collector" -- 'histogram_sorted_compare/(histogram_regular_tags|histogram_sorted_tags|histogram_prepared_metric)$'
cargo bench --bench thread_local_compare --features "udp tls-collector custom_writer shared-collector" -- 'histogram_sorted_parallel_compare/(udp_regular_parallel|udp_sorted_parallel|udp_prepared_parallel|tls_regular_parallel|tls_sorted_parallel|tls_prepared_parallel)$'
```

### Tradeoffs

- TLS improves write throughput under contention, but uses more memory.
- With TLS, each active thread keeps a local aggregator per collector.
- This duplicates in-memory series state (keys, counters, gauges, histograms) until merge/flush.
- Approximate memory growth is proportional to `active_threads * active_series_per_thread`.
- `PreparedMetric` avoids repeated metric/tag preparation work, but each aggregator still stores
  its own series state for prepared entries.

## Feature Flags

- `udp`: Enables `MetricCollector`, `MetricCollectorOptions`, and built-in UDP writer types (`Simple`, `LinuxBatch`, `AppleBatch`)
- `custom_writer`: Enables `StatsWriterTrait` export and `StatsWriterType::Custom`
- `shared-collector`: Enables `SharedCollector`, `SharedCollectorOptions`, and shared in-memory aggregation APIs
- `tls-collector`: Enables `TLSCollector` for thread-local aggregation
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

## Release Workflow

Before publishing, run:

```bash
make prepare-publish
```

This requires Rust 1.90 or newer. It verifies the workspace, lists both packages, runs release tests, and dry-runs publishing the entire workspace with all features. Cargo validates both packages before publishing them in dependency order.
