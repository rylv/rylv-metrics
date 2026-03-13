use super::{AggregatorEntryKey, HistogramWrapper};
use crate::dogstats::collector::GaugeState;
use crate::dogstats::histogram_config::ResolvedHistogramConfig;
use crate::DefaultMetricHasher;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use hdrhistogram::Histogram;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::AtomicU64;

impl<S: BuildHasher + Clone> Hash for AggregatorEntryKey<S> {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        // SAFETY: DashMap uses the raw hash stored in `AggregatorEntryKey::hash`
        // via `determine_shard`; this `Hash` impl is never called.
        unsafe { std::hint::unreachable_unchecked() }
    }
}

pub struct Aggregator<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    pub histograms: DashMap<AggregatorEntryKey<S>, HistogramWrapper, S>,
    pub count: DashMap<AggregatorEntryKey<S>, AtomicU64, S>,
    pub gauge: DashMap<AggregatorEntryKey<S>, GaugeState, S>,
    pub pool_histograms: Vec<SegQueue<HistogramWrapper>>,
}

impl<S> Aggregator<S>
where
    S: BuildHasher + Clone,
{
    pub(crate) fn with_hasher_builder(hasher_builder: &S, pool_count: usize) -> Self {
        Self {
            histograms: DashMap::with_hasher(hasher_builder.clone()),
            count: DashMap::with_hasher(hasher_builder.clone()),
            gauge: DashMap::with_hasher(hasher_builder.clone()),
            pool_histograms: (0..pool_count).map(|_| SegQueue::new()).collect(),
        }
    }

    pub(crate) fn get_histogram(
        &self,
        pool_id: usize,
        config: &ResolvedHistogramConfig,
    ) -> Option<HistogramWrapper> {
        debug_assert!(pool_id < self.pool_histograms.len());
        // SAFETY: pool_id is produced by `resolve_histogram_configs` which assigns
        // sequential IDs matching `pool_histograms.len()`.
        if let Some(mut h) = unsafe { self.pool_histograms.get_unchecked(pool_id) }.pop() {
            h.percentiles = config.percentiles().clone();
            h.emit_base_metrics = config.emit_base_metrics();
            return Some(h);
        }

        let bounds = config.bounds();
        if let Ok(histo) =
            Histogram::new_with_bounds(bounds.min(), bounds.max(), config.sig_fig().value())
        {
            return Some(HistogramWrapper {
                pool_id,
                histogram: histo,
                min: u64::MAX,
                max: u64::MIN,
                percentiles: config.percentiles().clone(),
                emit_base_metrics: config.emit_base_metrics(),
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::Aggregator;
    use crate::dogstats::aggregator::HistogramWrapper;
    use crate::dogstats::histogram_config::HistogramBaseMetrics;
    use crate::dogstats::histogram_config::{HistogramConfig, ResolvedHistogramConfig};
    use crate::{HistogramBaseMetric, SigFig};
    use hdrhistogram::Histogram;
    use std::sync::Arc;

    type TestHasher = std::hash::RandomState;

    #[test]
    fn with_hasher_builder_creates_requested_pool_count() {
        let aggregator = Aggregator::<TestHasher>::with_hasher_builder(&TestHasher::new(), 3);

        assert_eq!(aggregator.pool_histograms.len(), 3);
        assert!(aggregator.histograms.is_empty());
        assert!(aggregator.count.is_empty());
        assert!(aggregator.gauge.is_empty());
    }

    #[test]
    fn get_histogram_creates_new_wrapper_when_pool_is_empty() {
        let aggregator = Aggregator::<TestHasher>::with_hasher_builder(&TestHasher::new(), 1);
        let config = ResolvedHistogramConfig::from_config(HistogramConfig::default(), 0);

        let wrapper = aggregator.get_histogram(0, &config).unwrap();

        assert_eq!(wrapper.pool_id, 0);
        assert_eq!(wrapper.min, u64::MAX);
        assert_eq!(wrapper.max, u64::MIN);
        assert_eq!(wrapper.percentiles.as_ref(), &[0.95, 0.99]);
    }

    #[test]
    fn get_histogram_reuses_pool_entry_and_refreshes_metadata() {
        let aggregator = Aggregator::<TestHasher>::with_hasher_builder(&TestHasher::new(), 1);
        let config = ResolvedHistogramConfig::from_config(
            HistogramConfig::new(SigFig::default(), vec![0.5])
                .unwrap()
                .with_base_metrics([
                    HistogramBaseMetric::Min,
                    HistogramBaseMetric::Avg,
                    HistogramBaseMetric::Max,
                ]),
            0,
        );

        aggregator.pool_histograms[0].push(HistogramWrapper {
            pool_id: 0,
            min: 1,
            max: 2,
            histogram: Histogram::new_with_bounds(1, u64::MAX, 3).unwrap(),
            percentiles: Arc::from([0.95_f64, 0.99_f64]),
            emit_base_metrics: HistogramBaseMetrics::ALL,
        });

        let wrapper = aggregator.get_histogram(0, &config).unwrap();

        assert_eq!(wrapper.pool_id, 0);
        assert_eq!(wrapper.percentiles.as_ref(), &[0.5]);
        assert_eq!(wrapper.emit_base_metrics, config.emit_base_metrics());
    }
}
