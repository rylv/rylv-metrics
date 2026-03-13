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
//! # #[cfg(feature = "udp")] {
//! use rylv_metrics::{
//!     MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr, SharedCollector,
//! };
//! use rylv_metrics::{histogram, count, count_add, gauge};
//! use std::net::SocketAddr;
//! use std::time::Duration;
//!
//! let options = MetricCollectorOptions {
//!     max_udp_packet_size: 1432,
//!     max_udp_batch_size: 10,
//!     flush_interval: Duration::from_secs(10),
//!     writer_type: rylv_metrics::DEFAULT_STATS_WRITER_TYPE,
//!     ..Default::default()
//! };
//!
//! let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
//! let dst_addr: SocketAddr = "127.0.0.1:8125".parse().unwrap();
//! let inner = SharedCollector::default();
//! let collector = MetricCollector::new(bind_addr, dst_addr, options, inner).unwrap();
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
//! # }
//! ```

// #![deny(unsafe_code)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
//#![deny(clippy::unreachable)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::pedantic)]
#![deny(clippy::nursery)]
// Disabled because it reports false duplicate-crate errors from dev-dependencies
#![deny(clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]
#![deny(missing_docs)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]
#![allow(clippy::module_name_repetitions)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::expect_used))]
#![cfg_attr(test, allow(clippy::panic))]

// https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
mod dogstats;
mod error;

#[cfg(all(feature = "custom_writer", feature = "udp"))]
pub use dogstats::writer::StatsWriterTrait;
pub use dogstats::{
    DrainMetricCollectorTrait, HistogramBaseMetric, HistogramConfig, MetricCollectorTrait,
    MetricFrameRef, MetricKind, MetricSuffix, PreparedMetric, SortedTags,
};
#[cfg(feature = "udp")]
pub use dogstats::{
    MetricCollector, MetricCollectorOptions, StatsWriterType, DEFAULT_STATS_WRITER_TYPE,
};
pub use dogstats::{RylvStr, SigFig};
#[cfg(feature = "shared-collector")]
pub use dogstats::{SharedCollector, SharedCollectorOptions};
#[cfg(feature = "tls-collector")]
pub use dogstats::{TLSCollector, TLSCollectorOptions};
pub use error::MetricsError;

/// Result type for metric operations.
///
/// Wraps errors that can occur during metric collection and transmission.
pub type MetricResult<T> = Result<T, MetricsError>;

/// Default hasher builder used by metric aggregation maps.
pub(crate) type DefaultMetricHasher = std::hash::RandomState;

/// Internal benchmark hook for measuring lookup-key comparison behavior.
#[cfg(feature = "__bench-internals")]
#[doc(hidden)]
#[must_use]
pub fn benchmark_lookup_compare(
    metric: &str,
    lookup_tags: &[&str],
    entry_tags: &[&str],
    hash: u64,
) -> bool {
    use crate::dogstats::{AggregatorEntryKey, LookupKey};

    let lookup_tags_owned: Box<[RylvStr<'static>]> = lookup_tags
        .iter()
        .map(|tag| RylvStr::from((*tag).to_owned()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let entry_tags_owned: Vec<RylvStr<'static>> = entry_tags
        .iter()
        .map(|tag| RylvStr::from((*tag).to_owned()))
        .collect();

    let entry = AggregatorEntryKey {
        metric: RylvStr::from(metric.to_owned()),
        tags: SortedTags::new(entry_tags_owned, &std::hash::RandomState::default()),
        hash,
        fingerprint: 0,
        id: 1,
    };

    let lookup = LookupKey {
        metric: RylvStr::from(metric.to_owned()),
        tags: &lookup_tags_owned,
        tags_hash: 0,
        hash,
    };
    lookup.compare(&entry)
}
