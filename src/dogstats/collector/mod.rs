use std::hash::BuildHasher;

use crate::MetricResult;

use crate::dogstats::RylvStr;
use crate::dogstats::RylvTag;
use crate::dogstats::{PreparedMetric, SortedTags};

#[cfg(feature = "shared-collector")]
mod shared_collector;
#[cfg(feature = "tls-collector")]
mod tls_collector;

#[cfg(feature = "shared-collector")]
pub(super) use shared_collector::{GaugeLastState, GaugeState};
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
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    /// The aggregation state is unchanged; the caller decides how to report it.
    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT) -> MetricResult<()>
    where
        TT: AsMut<[RylvTag<'t>]>;

    /// Increments a counter by one.
    ///
    /// Counters are aggregated client-side and the total is sent on flush.
    /// Negative values are supported: the window's net total is emitted, and it
    /// may be negative.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, tags: TT)
    where
        TT: AsMut<[RylvTag<'t>]>,
    {
        self.count_add(metric, 1, tags);
    }

    /// Increments a counter by the specified value.
    ///
    /// Counters are aggregated client-side and the window's net total is sent
    /// on flush. Negative values are supported: the net total may be negative.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: i64, tags: TT)
    where
        TT: AsMut<[RylvTag<'t>]>;

    /// Records a gauge value representing a point-in-time measurement.
    ///
    /// Multiple gauge values for the same metric/tags are averaged on flush.
    /// Negative values are supported and enter the average; the emitted mean
    /// is a floating-point value.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn gauge_avg<'m, 't, TT>(&self, metric: RylvStr<'m>, value: i64, tags: TT)
    where
        TT: AsMut<[RylvTag<'t>]>;

    /// Records a gauge value representing a point-in-time measurement.
    ///
    /// Only the last value recorded before flush is emitted (last-write-wins).
    /// Negative values are supported.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: i64, tags: TT)
    where
        TT: AsMut<[RylvTag<'t>]>;

    /// Records a timing value for duration tracking.
    ///
    /// Timings are aggregated client-side identically to histograms, but emitted
    /// with the `DogStatsD` `ms` metric type instead of gauge.
    ///
    /// **Note:** The `tags` slice is sorted in-place for consistent aggregation keys.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    /// The aggregation state is unchanged; the caller decides how to report it.
    fn timing<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT) -> MetricResult<()>
    where
        TT: AsMut<[RylvTag<'t>]>;

    /// Records a histogram using pre-sorted tags.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    fn histogram_sorted(
        &self,
        metric: RylvStr<'_>,
        value: u64,
        tags: &SortedTags<Self::Hasher>,
    ) -> MetricResult<()>;

    /// Increments a counter by one using pre-sorted tags.
    fn count_sorted(&self, metric: RylvStr<'_>, tags: &SortedTags<Self::Hasher>) {
        self.count_add_sorted(metric, 1, tags);
    }

    /// Increments a counter by value using pre-sorted tags.
    ///
    /// Negative values are supported: the window's net total may be negative.
    fn count_add_sorted(&self, metric: RylvStr<'_>, value: i64, tags: &SortedTags<Self::Hasher>);

    /// Records an averaged gauge using pre-sorted tags.
    ///
    /// Negative values are supported and enter the average.
    fn gauge_avg_sorted(&self, metric: RylvStr<'_>, value: i64, tags: &SortedTags<Self::Hasher>);

    /// Records a last-write-wins gauge using pre-sorted tags.
    ///
    /// Negative values are supported.
    fn gauge_sorted(&self, metric: RylvStr<'_>, value: i64, tags: &SortedTags<Self::Hasher>);

    /// Records a timing using pre-sorted tags.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    fn timing_sorted(
        &self,
        metric: RylvStr<'_>,
        value: u64,
        tags: &SortedTags<Self::Hasher>,
    ) -> MetricResult<()>;

    /// Builds a [`SortedTags`] bound to this collector's hasher.
    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvTag<'a>>,
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
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    fn histogram_prepared(
        &self,
        prepared: &PreparedMetric<Self::Hasher>,
        value: u64,
    ) -> MetricResult<()>;

    /// Increments a counter by one using a prepared metric key.
    fn count_prepared(&self, prepared: &PreparedMetric<Self::Hasher>) {
        self.count_add_prepared(prepared, 1);
    }

    /// Increments a counter by value using a prepared metric key.
    ///
    /// Negative values are supported: the window's net total may be negative.
    fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: i64);

    /// Records an averaged gauge using a prepared metric key.
    ///
    /// Negative values are supported and enter the average.
    fn gauge_avg_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: i64);

    /// Records a last-write-wins gauge using a prepared metric key.
    ///
    /// Negative values are supported.
    fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: i64);

    /// Records a timing using a prepared metric key.
    ///
    /// # Errors
    /// Returns `MetricResult::Err` when the value cannot be recorded into the
    /// metric's histogram (for example a value above the configured maximum).
    fn timing_prepared(
        &self,
        prepared: &PreparedMetric<Self::Hasher>,
        value: u64,
    ) -> MetricResult<()>;
}

/// Owns a drained generation and lends a separate frame cursor.
///
/// Frames borrow this owner, so they may outlive the cursor and be queued for
/// batch I/O. The owner cannot be recycled while any of those frames are in use.
/// Implementors store frame data in the owner; the cursor only traverses it.
/// A cursor cannot yield references into a scratch buffer it reuses on `next()`:
///
/// ```compile_fail
/// use rylv_metrics::{MetricFrameRef, MetricKind, MetricSuffix, MetricValue};
/// struct ScratchCursor<'a> { name: &'a mut String }
/// impl<'a> Iterator for ScratchCursor<'a> {
///     type Item = MetricFrameRef<'a>;
///     fn next(&mut self) -> Option<Self::Item> {
///         self.name.clear();
///         self.name.push_str("other");
///         Some(MetricFrameRef {
///             prefix: "", metric: &self.name, tags: "", value: MetricValue::Int(1),
///             kind: MetricKind::Count, suffix: MetricSuffix::None,
///         })
///     }
/// }
/// ```
pub trait MetricDrain {
    /// Cursor borrowing this drained generation.
    type Cursor<'a>: Iterator<Item = MetricFrameRef<'a>>
    where
        Self: 'a;

    /// Borrows the generation to traverse its pending metrics.
    ///
    /// ```compile_fail
    /// # use rylv_metrics::MetricDrain;
    /// fn recycle_too_early(mut drain: impl MetricDrain) {
    /// let frame = drain.frames().next().unwrap();
    /// drop(drain);
    /// println!("{}", frame.metric);
    /// }
    /// ```
    fn frames(&mut self) -> Self::Cursor<'_>;

    /// Visits the remaining metrics through a borrowed cursor.
    fn for_each_frame<'a>(&'a mut self, visit: impl FnMut(MetricFrameRef<'a>)) {
        self.frames().for_each(visit);
    }

    /// Consumes and counts the remaining frames.
    fn count_frames(&mut self) -> usize {
        self.frames().count()
    }
}

/// Trait for collectors that provide owned drained generations.
pub trait DrainMetricCollectorTrait: MetricCollectorTrait {
    /// Owner of a drained generation, recycled when dropped.
    type Drain<'a>: MetricDrain
    where
        Self: 'a;

    /// Tries to detach a generation for draining through its borrowed cursor.
    fn try_begin_drain(&self) -> Option<Self::Drain<'_>>;
}

/// Numeric value of a drained metric frame.
///
/// Integer metric values are `i64` (the domain of the reference `DogStatsD`
/// clients): counters may emit negative net totals, and last-write-wins gauges
/// may hold negative values. Averaged gauges and histogram `.avg` series are
/// real means and are emitted as floating-point values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricValue {
    /// An integer value (counters, last-write-wins gauges, histogram
    /// `.count`/`.min`/`.max` and percentiles).
    Int(i64),
    /// A floating-point value (averaged gauges and histogram `.avg`).
    Float(f64),
}

impl MetricValue {
    /// Returns the integer value for [`MetricValue::Int`], `None` for
    /// [`MetricValue::Float`].
    #[must_use]
    pub const fn as_int(self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(value),
            Self::Float(_) => None,
        }
    }

    /// Returns the floating-point value (an [`MetricValue::Int`] is widened).
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // widening an integer metric value is the point
    pub const fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }
}

impl std::fmt::Display for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(value) => write!(f, "{value}"),
            // Same plain-decimal formatting the `DogStatsD` writer uses.
            Self::Float(value) => {
                let mut out = String::new();
                f.write_str(crate::dogstats::format::format_float(*value, &mut out))
            }
        }
    }
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
    pub value: MetricValue,
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
    /// Gauge metric (`|g`), averaged over the flush window.
    GaugeAvg,
    /// Gauge metric (`|g`), last recorded value wins.
    Gauge,
    /// Timing metric (`|ms`).
    Timing,
}
