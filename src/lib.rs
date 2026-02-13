//! # rylv-metrics
//!
//! A high-performance `DogStatsD` metrics client for Rust with client-side aggregation.
//!
//! ## Features
//!
//! - **High Performance**: Lock-free data structures and optimized UDP batching
//! - **Client-Side Aggregation**: Reduces network overhead by aggregating metrics before sending
//! - **Multiple Writer Backends**: Simple, `LinuxBatch`, `AppleBatch`, and Custom writers
//! - **Metric Types**: Histograms, Counters, and Gauges
//!
//! ## Quick Start
//!
//! ```no_run
//! use rylv_metrics::{MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr};
//! use rylv_metrics::{histogram, count, count_add, gauge};
//! use std::net::SocketAddr;
//! use std::time::Duration;
//!
//! let options = MetricCollectorOptions {
//!     max_udp_packet_size: 1432,
//!     max_udp_batch_size: 10,
//!     flush_interval: Duration::from_secs(10),
//!     stats_prefix: "myapp.".to_string(),
//!     writer_type: rylv_metrics::DEFAULT_STATS_WRITER_TYPE,
//!     histogram_configs: Default::default(),
//!     default_histogram_config: rylv_metrics::HistogramConfig::default(),
//!     hasher_builder: std::hash::RandomState::new(),
//! };
//!
//! let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
//! let dst_addr: SocketAddr = "127.0.0.1:8125".parse().unwrap();
//! let collector = MetricCollector::new(bind_addr, dst_addr, options);
//!
//! // Direct API — use RylvStr::from_static() for zero-copy aggregation keys
//! collector.histogram(RylvStr::from_static("request.latency"), 42, &mut [RylvStr::from_static("endpoint:api")]);
//! collector.count(RylvStr::from_static("request.count"), &mut [RylvStr::from_static("endpoint:api")]);
//! collector.count_add(RylvStr::from_static("bytes.sent"), 1024, &mut [RylvStr::from_static("endpoint:api")]);
//! collector.gauge(RylvStr::from_static("connections.active"), 100, &mut [RylvStr::from_static("pool:main")]);
//!
//! // Convenience macros — allocate on first key insertion, but more ergonomic
//! histogram!(collector, "request.latency", 42, "endpoint:api");
//! count!(collector, "request.count", "endpoint:api");
//! count_add!(collector, "bytes.sent", 1024, "endpoint:api");
//! gauge!(collector, "connections.active", 100, "pool:main");
//! ```

// #![deny(unsafe_code)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
//#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
// Disabled because it reports false duplicate-crate errors from dev-dependencies
//#![warn(clippy::cargo)]
#![warn(missing_docs)]
#![warn(clippy::missing_errors_doc)]
#![warn(clippy::missing_panics_doc)]
#![allow(clippy::module_name_repetitions)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::panic))]

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
mod dogstats;
mod error;

pub use dogstats::collector::{
    HistogramConfig, MetricCollector, MetricCollectorOptions, MetricCollectorTrait,
    StatsWriterType, DEFAULT_STATS_WRITER_TYPE,
};
pub use dogstats::writer::StatsWriterTrait;
pub use dogstats::{RylvStr, SigFig};
pub use error::MetricsError;

/// Result type for metric operations.
///
/// Wraps errors that can occur during metric collection and transmission.
pub type MetricResult<T> = Result<T, MetricsError>;

/// Default hasher builder used by metric aggregation maps.
pub(crate) type DefaultMetricHasher = std::hash::RandomState;
