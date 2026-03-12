use std::{borrow::Cow, cmp::Ordering, sync::Arc};

#[cfg(feature = "shared-collector")]
pub use aggregator::Aggregator;

mod aggregator;
pub mod collector;
#[cfg(feature = "udp")]
mod collector_udp;
mod histogram_config;
#[cfg(feature = "udp")]
mod job;
pub mod macros;
#[cfg(feature = "udp")]
mod net;
mod slice_utils;
mod sorted_tags;
#[cfg(feature = "udp")]
pub mod writer;
#[cfg(feature = "udp")]
mod writer_utils;
pub use aggregator::SigFig;
#[cfg(feature = "__bench-internals")]
pub use aggregator::{AggregatorEntryKey, LookupKey};
pub use collector::DrainMetricCollectorTrait;
pub use collector::MetricCollectorTrait;
pub use collector::{MetricFrameRef, MetricKind, MetricSuffix};
#[cfg(feature = "shared-collector")]
pub use collector::{SharedCollector, SharedCollectorOptions};
#[cfg(feature = "tls-collector")]
pub use collector::{TLSCollector, TLSCollectorOptions};
#[cfg(feature = "udp")]
pub use collector_udp::{
    MetricCollector, MetricCollectorOptions, StatsWriterType, DEFAULT_STATS_WRITER_TYPE,
};
pub use histogram_config::{Bounds, HistogramBaseMetric, HistogramConfig};
pub use sorted_tags::{PreparedMetric, SortedTags};

/// A flexible string type that can hold static references, borrowed references, or owned values.
/// Used for metric names and tags.
///
/// # Choosing the Right Variant
///
/// | Variant | When to use | `to_cow()` cost |
/// |---------|-------------|-----------------|
/// | `RylvStr::Static` | Compile-time string literals (`from_static("...")`) | Zero-copy (`Cow::Borrowed`) |
/// | `RylvStr::Borrowed` | Short-lived `&str` references (via `From<&str>`) | Allocates (`Cow::Owned`) |
/// | `RylvStr::Owned` | Runtime-generated strings (via `From<String>`) | Allocates (`Cow::Owned`) |
///
/// For best performance, use `RylvStr::from_static()` whenever the string is known
/// at compile time. This avoids heap allocation when the aggregator stores a new metric key.
#[derive(Debug, Clone)]
pub enum RylvStr<'a> {
    /// A borrowed `&'static str`. Zero-copy on `to_cow()`.
    Static(&'static str),
    /// A borrowed non-static `&str`. Clones on `to_cow()`.
    Borrowed(&'a str),
    /// An owned string stored in an `Arc` for cheap cloning.
    Owned(Arc<str>),
}

impl RylvStr<'_> {
    /// Creates a `RylvStr::Static` from a `&'static str` for zero-copy conversion.
    #[must_use]
    pub const fn from_static(s: &'static str) -> RylvStr<'static> {
        RylvStr::Static(s)
    }
}

impl AsRef<str> for RylvStr<'_> {
    fn as_ref(&self) -> &str {
        match self {
            RylvStr::Static(s) | RylvStr::Borrowed(s) => s,
            RylvStr::Owned(s) => s.as_ref(),
        }
    }
}

impl PartialEq for RylvStr<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for RylvStr<'_> {}

impl PartialOrd for RylvStr<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RylvStr<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl<'a> From<&'a str> for RylvStr<'a> {
    fn from(s: &'a str) -> Self {
        RylvStr::Borrowed(s)
    }
}

impl From<String> for RylvStr<'_> {
    fn from(s: String) -> Self {
        RylvStr::Owned(Arc::from(s))
    }
}

impl From<Arc<str>> for RylvStr<'_> {
    fn from(s: Arc<str>) -> Self {
        RylvStr::Owned(s)
    }
}

impl<'a> From<Cow<'a, str>> for RylvStr<'a> {
    fn from(cow: Cow<'a, str>) -> Self {
        match cow {
            Cow::Borrowed(s) => RylvStr::Borrowed(s),
            Cow::Owned(s) => RylvStr::Owned(Arc::from(s)),
        }
    }
}
