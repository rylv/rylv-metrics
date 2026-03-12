use std::collections::HashMap;
use std::hash::BuildHasher;
use std::iter::FromIterator;
use std::sync::Arc;

use crate::dogstats::aggregator::SigFig;
use crate::DefaultMetricHasher;
use crate::MetricResult;

/// Base histogram metrics that can be emitted alongside configured percentiles.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum HistogramBaseMetric {
    /// Emit the `.count` metric.
    Count,
    /// Emit the `.min` metric.
    Min,
    /// Emit the `.avg` metric.
    Avg,
    /// Emit the `.max` metric.
    Max,
}

impl HistogramBaseMetric {
    const fn mask(self) -> u8 {
        match self {
            Self::Count => 1 << 0,
            Self::Min => 1 << 1,
            Self::Avg => 1 << 2,
            Self::Max => 1 << 3,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct HistogramBaseMetrics(u8);

impl HistogramBaseMetrics {
    pub(crate) const NONE: Self = Self(0);
    pub(crate) const ALL: Self = Self(
        HistogramBaseMetric::Count.mask()
            | HistogramBaseMetric::Min.mask()
            | HistogramBaseMetric::Avg.mask()
            | HistogramBaseMetric::Max.mask(),
    );

    pub(crate) const fn only(metric: HistogramBaseMetric) -> Self {
        Self(metric.mask())
    }

    pub(crate) const fn contains(self, metric: HistogramBaseMetric) -> bool {
        self.0 & metric.mask() != 0
    }

    pub(crate) const fn with(self, metric: HistogramBaseMetric) -> Self {
        Self(self.0 | metric.mask())
    }

    pub(crate) const fn without(self, metric: HistogramBaseMetric) -> Self {
        Self(self.0 & !metric.mask())
    }
}

impl From<HistogramBaseMetric> for HistogramBaseMetrics {
    fn from(metric: HistogramBaseMetric) -> Self {
        Self::only(metric)
    }
}

impl<const N: usize> From<[HistogramBaseMetric; N]> for HistogramBaseMetrics {
    fn from(metrics: [HistogramBaseMetric; N]) -> Self {
        Self::from_iter(metrics)
    }
}

impl FromIterator<HistogramBaseMetric> for HistogramBaseMetrics {
    fn from_iter<T: IntoIterator<Item = HistogramBaseMetric>>(iter: T) -> Self {
        iter.into_iter().fold(Self::NONE, Self::with)
    }
}

/// Configuration for histogram precision.
///
/// Controls the number of significant figures used when recording
/// histogram values, affecting both precision and memory usage.
///
/// # Example
///
/// ```ignore
/// use rylv_metrics::{HistogramConfig, SigFig};
/// let config = HistogramConfig::new(SigFig::new(2).unwrap(), vec![0.95, 0.99]).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct HistogramConfig {
    sig_fig: SigFig,
    bounds: Bounds,
    percentiles: Arc<[f64]>,
    emit_base_metrics: HistogramBaseMetrics,
}

/// Inclusive lower and upper bounds for recorded histogram values.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Bounds {
    min: u64,
    max: u64,
}

impl Bounds {
    /// Creates histogram bounds.
    ///
    /// # Errors
    /// Returns an error if `min < 1` or `max < min`.
    pub fn new(min: u64, max: u64) -> MetricResult<Self> {
        if min < 1 {
            return Err("Invalid histogram bounds: min must be >= 1".into());
        }
        if max < min {
            return Err("Invalid histogram bounds: max must be >= min".into());
        }

        Ok(Self { min, max })
    }

    /// Returns the inclusive lower bound.
    #[must_use]
    pub const fn min(self) -> u64 {
        self.min
    }

    /// Returns the inclusive upper bound.
    #[must_use]
    pub const fn max(self) -> u64 {
        self.max
    }
}

impl Default for Bounds {
    fn default() -> Self {
        Self {
            min: 1,
            max: u64::MAX,
        }
    }
}

impl HistogramConfig {
    const fn set_emit_base_metric(&mut self, metric: HistogramBaseMetric, emit: bool) {
        self.emit_base_metrics = if emit {
            self.emit_base_metrics.with(metric)
        } else {
            self.emit_base_metrics.without(metric)
        };
    }

    /// Creates a new histogram configuration with the given significant figures and percentiles.
    ///
    /// # Errors
    /// Returns an error if any percentile is NaN/inf or outside `[0.0, 1.0)`.
    pub fn new(sig_fig: SigFig, percentiles: Vec<f64>) -> MetricResult<Self> {
        for percentile in &percentiles {
            if !percentile.is_finite() || *percentile < 0.0 || *percentile >= 1.0 {
                return Err("Invalid percentile: must be finite and in range [0.0, 1.0)".into());
            }
        }

        Ok(Self {
            sig_fig,
            bounds: Bounds::default(),
            percentiles: percentiles.into(),
            emit_base_metrics: HistogramBaseMetrics::ALL,
        })
    }

    /// Replaces the set of emitted base histogram metrics.
    #[must_use]
    pub fn with_base_metrics(
        mut self,
        emit_base_metrics: impl IntoIterator<Item = HistogramBaseMetric>,
    ) -> Self {
        self.emit_base_metrics = HistogramBaseMetrics::from_iter(emit_base_metrics);
        self
    }

    /// Enables or disables the `.count` histogram metric.
    #[must_use]
    pub const fn with_count(mut self, emit: bool) -> Self {
        self.set_emit_base_metric(HistogramBaseMetric::Count, emit);
        self
    }

    /// Enables or disables the `.min` histogram metric.
    #[must_use]
    pub const fn with_min(mut self, emit: bool) -> Self {
        self.set_emit_base_metric(HistogramBaseMetric::Min, emit);
        self
    }

    /// Enables or disables the `.avg` histogram metric.
    ///
    /// Note: `.avg` currently reflects p50 behavior for compatibility.
    #[must_use]
    pub const fn with_avg(mut self, emit: bool) -> Self {
        self.set_emit_base_metric(HistogramBaseMetric::Avg, emit);
        self
    }

    /// Enables or disables the `.max` histogram metric.
    #[must_use]
    pub const fn with_max(mut self, emit: bool) -> Self {
        self.set_emit_base_metric(HistogramBaseMetric::Max, emit);
        self
    }

    pub(crate) const fn sig_fig(&self) -> SigFig {
        self.sig_fig
    }

    pub(crate) fn percentiles(&self) -> Arc<[f64]> {
        self.percentiles.clone()
    }

    pub(crate) const fn bounds(&self) -> Bounds {
        self.bounds
    }

    pub(crate) const fn emit_base_metrics(&self) -> HistogramBaseMetrics {
        self.emit_base_metrics
    }

    /// Sets histogram recording bounds.
    ///
    /// These bounds determine the compatible pool and the histogram allocation shape.
    #[must_use]
    pub const fn with_bounds(mut self, bounds: Bounds) -> Self {
        self.bounds = bounds;
        self
    }
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            sig_fig: SigFig::default(),
            bounds: Bounds::default(),
            percentiles: vec![0.95, 0.99].into(),
            emit_base_metrics: HistogramBaseMetrics::ALL,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct HistogramPoolSpec {
    pub sig_fig: SigFig,
    pub bounds: Bounds,
}

impl HistogramPoolSpec {
    pub(crate) const fn from_config(config: &HistogramConfig) -> Self {
        Self {
            sig_fig: config.sig_fig,
            bounds: config.bounds,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedHistogramConfig {
    config: HistogramConfig,
    pool_id: usize,
}

impl ResolvedHistogramConfig {
    pub const fn from_config(config: HistogramConfig, pool_id: usize) -> Self {
        Self { config, pool_id }
    }

    pub(crate) const fn pool_id(&self) -> usize {
        self.pool_id
    }

    pub(crate) const fn sig_fig(&self) -> SigFig {
        self.config.sig_fig()
    }

    pub(crate) const fn bounds(&self) -> Bounds {
        self.config.bounds()
    }

    pub(crate) fn percentiles(&self) -> Arc<[f64]> {
        self.config.percentiles()
    }

    pub(crate) const fn emit_base_metrics(&self) -> HistogramBaseMetrics {
        self.config.emit_base_metrics()
    }
}

pub struct ResolvedHistogramConfigs<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    pub default_histogram_config: ResolvedHistogramConfig,
    pub histogram_configs: HashMap<String, ResolvedHistogramConfig, S>,
    pub pool_specs: Arc<[HistogramPoolSpec]>,
    pub pool_count: usize,
}

pub fn resolve_histogram_configs<S>(
    default_histogram_config: HistogramConfig,
    histogram_configs: HashMap<String, HistogramConfig, S>,
    hasher_builder: &S,
) -> ResolvedHistogramConfigs<S>
where
    S: BuildHasher + Clone,
{
    let register_pool = |config: &HistogramConfig,
                         pool_ids: &mut HashMap<HistogramPoolSpec, usize, S>,
                         next_pool_id: &mut usize| {
        let pool_spec = HistogramPoolSpec::from_config(config);
        *pool_ids.entry(pool_spec).or_insert_with(|| {
            let id = *next_pool_id;
            *next_pool_id += 1;
            id
        })
    };

    let mut pool_ids = HashMap::with_hasher(hasher_builder.clone());
    let mut next_pool_id = 0;
    let default_pool_id =
        register_pool(&default_histogram_config, &mut pool_ids, &mut next_pool_id);

    let mut resolved_histogram_configs =
        HashMap::with_capacity_and_hasher(histogram_configs.len(), hasher_builder.clone());
    for (metric, config) in histogram_configs {
        let pool_id = register_pool(&config, &mut pool_ids, &mut next_pool_id);
        resolved_histogram_configs.insert(
            metric,
            ResolvedHistogramConfig::from_config(config, pool_id),
        );
    }

    let mut pool_specs =
        vec![HistogramPoolSpec::from_config(&default_histogram_config); next_pool_id];
    for (pool_spec, pool_id) in pool_ids {
        pool_specs[pool_id] = pool_spec;
    }

    ResolvedHistogramConfigs {
        default_histogram_config: ResolvedHistogramConfig::from_config(
            default_histogram_config,
            default_pool_id,
        ),
        histogram_configs: resolved_histogram_configs,
        pool_specs: pool_specs.into(),
        pool_count: next_pool_id,
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_histogram_configs, Bounds, HistogramBaseMetric, HistogramConfig};
    use crate::dogstats::aggregator::SigFig;
    use std::collections::HashMap;

    #[test]
    fn histogram_config_allows_empty_percentiles() {
        let config = HistogramConfig::new(SigFig::default(), vec![]);
        assert!(config.is_ok());
    }

    #[test]
    fn histogram_config_rejects_invalid_percentiles() {
        let invalid = [f64::NAN, f64::INFINITY, -0.1, 1.0];
        for percentile in invalid {
            let config = HistogramConfig::new(SigFig::default(), vec![percentile]);
            assert!(config.is_err());
        }
    }

    #[test]
    fn bounds_reject_invalid_values() {
        assert!(Bounds::new(0, 10).is_err());
        assert!(Bounds::new(10, 9).is_err());
    }

    #[test]
    fn histogram_base_metrics_builds_typed_sets() {
        let metrics = super::HistogramBaseMetrics::from([
            HistogramBaseMetric::Count,
            HistogramBaseMetric::Max,
        ]);

        assert!(metrics.contains(HistogramBaseMetric::Count));
        assert!(!metrics.contains(HistogramBaseMetric::Min));
        assert!(metrics.contains(HistogramBaseMetric::Max));
    }

    #[test]
    fn histogram_config_with_base_metrics_replaces_selection() {
        let config = HistogramConfig::new(SigFig::default(), Vec::new())
            .unwrap()
            .with_base_metrics([HistogramBaseMetric::Count, HistogramBaseMetric::Max]);

        let metrics = config.emit_base_metrics();
        assert!(metrics.contains(HistogramBaseMetric::Count));
        assert!(!metrics.contains(HistogramBaseMetric::Min));
        assert!(!metrics.contains(HistogramBaseMetric::Avg));
        assert!(metrics.contains(HistogramBaseMetric::Max));
    }

    #[test]
    fn resolve_histogram_configs_reuses_pool_ids_for_matching_specs() {
        let default_config = HistogramConfig::default();
        let shared = HistogramConfig::new(SigFig::default(), vec![0.95]).unwrap();
        let distinct = HistogramConfig::new(SigFig::new(2).unwrap(), vec![0.95]).unwrap();
        let mut configs = HashMap::new();
        configs.insert("metric.a".to_string(), shared.clone());
        configs.insert("metric.b".to_string(), shared);
        configs.insert("metric.c".to_string(), distinct);

        let resolved =
            resolve_histogram_configs(default_config, configs, &std::hash::RandomState::new());

        assert_eq!(resolved.pool_count, 2);
        assert_eq!(
            resolved.histogram_configs["metric.a"].pool_id(),
            resolved.histogram_configs["metric.b"].pool_id()
        );
        assert_ne!(
            resolved.histogram_configs["metric.a"].pool_id(),
            resolved.histogram_configs["metric.c"].pool_id()
        );
        assert_eq!(resolved.pool_specs.len(), resolved.pool_count);
    }
}
