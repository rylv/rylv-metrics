use std::hash::BuildHasher;

use crate::dogstats::RylvStr;
use crate::dogstats::{PreparedMetric, SortedTags};

#[cfg(feature = "shared-collector")]
mod shared_collector;
#[cfg(feature = "tls-collector")]
mod tls_collector;

#[cfg(feature = "shared-collector")]
pub(super) use shared_collector::GaugeState;
#[cfg(feature = "shared-collector")]
pub use shared_collector::{SharedCollector, SharedCollectorOptions};
#[cfg(feature = "tls-collector")]
pub use tls_collector::{TLSCollector, TLSCollectorOptions};

/// Trait defining the interface for metric collection.
///
/// Implementations of this trait can record histograms, counters, and gauges
/// with associated tags.
pub trait MetricCollectorTrait {
    /// The hasher used to produce [`PreparedMetric`] keys.
    type Hasher: BuildHasher + Clone;

    /// Records a histogram value for distribution tracking.
    ///
    /// Histograms are aggregated client-side and percentiles are computed
    /// before being sent to the server.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>;

    /// Increments a counter by one.
    ///
    /// Counters are aggregated client-side and the total is sent on flush.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>;

    /// Increments a counter by the specified value.
    ///
    /// Counters are aggregated client-side and the total is sent on flush.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>;

    /// Records a gauge value representing a point-in-time measurement.
    ///
    /// Multiple gauge values for the same metric/tags are averaged on flush.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>;

    /// Records a histogram using pre-sorted tags.
    fn histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>);

    /// Increments a counter by one using pre-sorted tags.
    fn count_sorted(&self, metric: RylvStr<'_>, tags: &SortedTags<Self::Hasher>) {
        self.count_add_sorted(metric, 1, tags);
    }

    /// Increments a counter by value using pre-sorted tags.
    fn count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>);

    /// Records a gauge using pre-sorted tags.
    fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>);

    /// Builds a [`SortedTags`] bound to this collector's hasher.
    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<Self::Hasher>;

    /// Precomputes a collector-bound metric key for hot paths.
    ///
    /// The returned [`PreparedMetric`] caches the metric name, pre-sorted tags,
    /// and a pre-computed hash, making subsequent `*_prepared` calls faster.
    fn prepare_metric(
        &self,
        metric: RylvStr<'_>,
        tags: SortedTags<Self::Hasher>,
    ) -> PreparedMetric<Self::Hasher>;

    /// Records a histogram using a prepared metric key.
    fn histogram_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64);

    /// Increments a counter by one using a prepared metric key.
    fn count_prepared(&self, prepared: &PreparedMetric<Self::Hasher>) {
        self.count_add_prepared(prepared, 1);
    }

    /// Increments a counter by value using a prepared metric key.
    fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64);

    /// Records a gauge using a prepared metric key.
    fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64);
}

/// Trait for collectors that support draining aggregated metrics.
pub trait DrainMetricCollectorTrait: MetricCollectorTrait {
    /// Drain iterator returned by this collector.
    type Drain<'a>: Iterator<Item = MetricFrameRef<'a>>
    where
        Self: 'a;

    /// Tries to begin a drain cycle, returning a handle to iterate over
    /// aggregated metric frames.
    fn try_begin_drain(&self) -> Option<Self::Drain<'_>>;
}

/// Borrowed representation of a drained metric frame.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFrameRef<'a> {
    /// Prefix configured in collector options.
    pub prefix: &'a str,
    /// Base metric name without prefix or suffix.
    pub metric: &'a str,
    /// Optional metric suffix.
    pub suffix: MetricSuffix<'a>,
    /// Tags in joined `DogStatsD` format.
    pub tags: &'a str,
    /// Numeric value.
    pub value: u64,
    /// Metric kind.
    pub kind: MetricKind,
}

/// Suffix descriptor for a borrowed metric frame.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricSuffix<'a> {
    /// No metric suffix.
    None,
    /// Static suffix (e.g. `.count`, `.min`, `.max`).
    Static(&'a str),
    /// Percentile suffix represented as a quantile in `[0, 1)`.
    Percentile(f64),
}

/// Metric kind emitted by the drain APIs.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetricKind {
    /// Counter metric (`|c`).
    Count,
    /// Gauge metric (`|g`).
    Gauge,
}
