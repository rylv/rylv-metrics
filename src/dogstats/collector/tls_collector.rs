use crate::dogstats::collector::{MetricFrameRef, MetricKind, MetricSuffix};
use crate::dogstats::histogram_config::{
    resolve_histogram_configs, Bounds, HistogramBaseMetric, HistogramBaseMetrics, HistogramConfig,
    HistogramPoolSpec, ResolvedHistogramConfig, ResolvedHistogramConfigs,
};
use crate::dogstats::sorted_tags::{
    combine_metric_tags_hash, hash_tags, to_static_metric, PreparedMetric,
};
use crate::dogstats::{
    aggregator::{
        to_agg_entry_key, AggregatorEntryKey, HistogramWrapper, LookupKey, LookupKeySorted,
        RemoveKey,
    },
    RylvStr, SortedTags,
};
use crate::DefaultMetricHasher;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::ptr::addr_of_mut;
use std::sync::Arc;

use super::{DrainMetricCollectorTrait, MetricCollectorTrait};
use crossbeam::utils::CachePadded;
use hashbrown::hash_table::Entry::{Occupied, Vacant};
use hashbrown::HashTable;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use thread_local::ThreadLocal;
use tracing::error;

#[derive(Clone, Copy, Default)]
struct GaugeStateHb {
    sum: u64,
    count: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum DrainStage {
    Count,
    Gauge,
    Histogram,
    Done,
}

struct LocalAggregatorHb<S>
where
    S: BuildHasher + Clone,
{
    histograms: HashTable<(AggregatorEntryKey<S>, HistogramWrapper)>,
    count: HashTable<(AggregatorEntryKey<S>, u64)>,
    gauge: HashTable<(AggregatorEntryKey<S>, GaugeStateHb)>,
    pool_histograms: Vec<Vec<HistogramWrapper>>,
}

struct AggregatorSplitBorrow<'a, S: BuildHasher + Clone> {
    histograms: &'a mut HashTable<(AggregatorEntryKey<S>, HistogramWrapper)>,
    pool_histograms: &'a mut [Vec<HistogramWrapper>],
}

impl<S> LocalAggregatorHb<S>
where
    S: BuildHasher + Clone,
{
    fn with_pool_count(hasher_builder: &S, pool_count: usize) -> Self {
        let _ = hasher_builder;
        Self {
            histograms: HashTable::new(),
            count: HashTable::new(),
            gauge: HashTable::new(),
            pool_histograms: (0..pool_count).map(|_| Vec::new()).collect(),
        }
    }

    fn empty_like(&self) -> Self {
        Self {
            histograms: HashTable::with_capacity(self.histograms.len()),
            count: HashTable::with_capacity(self.count.len()),
            gauge: HashTable::with_capacity(self.gauge.len()),
            pool_histograms: self
                .pool_histograms
                .iter()
                .map(|pool| Vec::with_capacity(pool.len()))
                .collect(),
        }
    }

    fn split_borrow(&mut self) -> AggregatorSplitBorrow<'_, S> {
        AggregatorSplitBorrow {
            histograms: &mut self.histograms,
            pool_histograms: &mut self.pool_histograms,
        }
    }

    const fn swap_with(&mut self, fresh: Self) -> Self {
        std::mem::replace(self, fresh)
    }
}

fn get_histogram_from_pool(
    pool_histograms: &mut [Vec<HistogramWrapper>],
    pool_id: usize,
    sig_fig: crate::dogstats::aggregator::SigFig,
    bounds: Bounds,
    percentiles: Arc<[f64]>,
    emit_base_metrics: HistogramBaseMetrics,
) -> Option<HistogramWrapper> {
    if let Some(mut histogram) = pool_histograms[pool_id].pop() {
        histogram.percentiles = percentiles;
        histogram.emit_base_metrics = emit_base_metrics;
        return Some(histogram);
    }

    if let Ok(histogram) = Histogram::new_with_bounds(bounds.min(), bounds.max(), sig_fig.value()) {
        return Some(HistogramWrapper {
            pool_id,
            histogram,
            min: u64::MAX,
            max: u64::MIN,
            percentiles,
            emit_base_metrics,
        });
    }

    None
}

fn get_histogram_from_pool_config(
    pool_histograms: &mut [Vec<HistogramWrapper>],
    config: &ResolvedHistogramConfig,
) -> Option<HistogramWrapper> {
    get_histogram_from_pool(
        pool_histograms,
        config.pool_id(),
        config.sig_fig(),
        config.bounds(),
        config.percentiles().clone(),
        config.emit_base_metrics(),
    )
}

struct GlobalAggregatorHb<S>
where
    S: BuildHasher + Clone,
{
    histograms: HashTable<(AggregatorEntryKey<S>, HistogramWrapper)>,
    count: HashTable<(AggregatorEntryKey<S>, u64)>,
    gauge: HashTable<(AggregatorEntryKey<S>, GaugeStateHb)>,
    pool_histograms: Vec<Vec<HistogramWrapper>>,
    key_to_remove: Vec<RemoveKey>,
}

impl<S> GlobalAggregatorHb<S>
where
    S: BuildHasher + Clone,
{
    fn with_pool_count(hasher_builder: &S, pool_count: usize) -> Self {
        let _ = hasher_builder;
        Self {
            histograms: HashTable::new(),
            count: HashTable::new(),
            gauge: HashTable::new(),
            pool_histograms: (0..pool_count).map(|_| Vec::new()).collect(),
            key_to_remove: Vec::new(),
        }
    }

    fn empty_like(&self) -> Self {
        Self {
            histograms: HashTable::with_capacity(self.histograms.len()),
            count: HashTable::with_capacity(self.count.len()),
            gauge: HashTable::with_capacity(self.gauge.len()),
            pool_histograms: self
                .pool_histograms
                .iter()
                .map(|pool| Vec::with_capacity(pool.len()))
                .collect(),
            key_to_remove: Vec::with_capacity(self.key_to_remove.capacity()),
        }
    }
}

/// Thread-local collector using `Mutex<hashbrown::HashTable<..>>` for hot-path aggregation.
///
/// This type keeps one local map set per thread and merges ownership into a global
/// map set during flush, avoiding concurrent map access in the write hot path.
pub struct TLSCollector<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone + Send,
{
    stats_prefix: String,
    buffers: ThreadLocal<CachePadded<Mutex<LocalAggregatorHb<S>>>>,
    hasher_builder: S,
    pool_count: usize,
    histogram_configs: HashMap<String, ResolvedHistogramConfig, S>,
    pool_specs: Arc<[HistogramPoolSpec]>,
    default_histogram_config: ResolvedHistogramConfig,
    global_aggregator: Mutex<GlobalAggregatorHb<S>>,
    recycled_global_aggregators: Mutex<Vec<GlobalAggregatorHb<S>>>,
    recycled_local_aggregators: Mutex<Vec<LocalAggregatorHb<S>>>,
    recycled_remove_keys: Mutex<Vec<Vec<RemoveKey>>>,
}

impl<S> TLSCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Creates a hashbrown-based TLS collector from the provided options.
    #[must_use]
    pub fn new(options: TLSCollectorOptions<S>) -> Self {
        let ResolvedHistogramConfigs {
            default_histogram_config,
            histogram_configs,
            pool_specs,
            pool_count,
        } = resolve_histogram_configs(
            options.default_histogram_config,
            options.histogram_configs,
            &options.hasher_builder,
        );
        Self {
            stats_prefix: options.stats_prefix,
            buffers: ThreadLocal::new(),
            pool_count,
            pool_specs,
            global_aggregator: Mutex::new(GlobalAggregatorHb::with_pool_count(
                &options.hasher_builder,
                pool_count,
            )),
            histogram_configs,
            default_histogram_config,
            hasher_builder: options.hasher_builder,
            recycled_global_aggregators: Mutex::new(Vec::new()),
            recycled_local_aggregators: Mutex::new(Vec::new()),
            recycled_remove_keys: Mutex::new(Vec::new()),
        }
    }

    fn get_or_create_thread_local_aggregator(&self) -> &CachePadded<Mutex<LocalAggregatorHb<S>>> {
        self.buffers.get_or(|| {
            CachePadded::new(Mutex::new(LocalAggregatorHb::with_pool_count(
                &self.hasher_builder,
                self.pool_count,
            )))
        })
    }

    fn flush_all_to_global(&self) -> GlobalAggregatorHb<S> {
        // Lock global only to swap the active generation with a fresh recycled one.
        let mut global_to_merge = {
            let mut global_guard = self.global_aggregator.lock();
            let fresh_global = self
                .recycled_global_aggregators
                .lock()
                .pop()
                .unwrap_or_else(|| global_guard.empty_like());
            std::mem::replace(&mut *global_guard, fresh_global)
        };

        // Swap each local quickly and merge into exclusive global.
        for buffer in &self.buffers {
            let mut local_guard = buffer.lock();
            let fresh = self
                .recycled_local_aggregators
                .lock()
                .pop()
                .unwrap_or_else(|| local_guard.empty_like());
            let mut local = local_guard.swap_with(fresh);
            drop(local_guard);
            let mut to_remove = self.recycled_remove_keys.lock().pop().unwrap_or_default();
            merge_local_aggregator_into_global_hashbrown(
                &mut local,
                &mut global_to_merge,
                &self.pool_specs,
                &mut to_remove,
            );
            to_remove.clear();
            self.recycled_remove_keys.lock().push(to_remove);
            self.recycle(local);
        }

        global_to_merge
    }

    fn recycle(&self, local: LocalAggregatorHb<S>) {
        self.recycled_local_aggregators.lock().push(local);
    }

    fn recycle_global(&self, global: GlobalAggregatorHb<S>) {
        self.recycled_global_aggregators.lock().push(global);
    }

    fn begin_drain(&self) -> TLSDrain<'_, S> {
        let global = self.flush_all_to_global();
        TLSDrain::new(self, global)
    }

    fn record_histogram(&self, metric: RylvStr<'_>, value: u64, tags: &mut [RylvStr<'_>]) {
        if tags.len() > 1 {
            tags.sort_unstable();
        }
        let lookup = build_lookup_key(metric, tags, &self.hasher_builder);
        let buffer = self.get_or_create_thread_local_aggregator();

        {
            let mut aggregator = buffer.lock();
            let split = aggregator.split_borrow();
            match split.histograms.entry(
                lookup.hash,
                |(key, _)| lookup.compare(key),
                |(key, _)| key.hash,
            ) {
                Occupied(mut entry) => {
                    if let Err(err) = entry.get_mut().1.record(value) {
                        error!("Fail to record: {err}");
                    }
                }
                Vacant(entry) => {
                    let histogram_config = self
                        .histogram_configs
                        .get(lookup.metric.as_ref())
                        .unwrap_or(&self.default_histogram_config);
                    if let Some(mut histogram) =
                        get_histogram_from_pool_config(split.pool_histograms, histogram_config)
                    {
                        if let Err(err) = histogram.record(value) {
                            error!("Fail to record: {err}");
                        }
                        entry.insert((lookup.into_key(), histogram));
                    }
                }
            }
            drop(aggregator);
        }
    }

    fn record_histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let hash =
            combine_metric_tags_hash(&self.hasher_builder, metric.as_ref(), tags.tags_hash());
        let lookup = LookupKeySorted {
            metric,
            sorted_tags: tags,
            hash,
        };

        let buffer = self.get_or_create_thread_local_aggregator();

        {
            let mut aggregator = buffer.lock();
            let split = aggregator.split_borrow();
            match split.histograms.entry(
                lookup.hash,
                |(key, _)| lookup.compare(key),
                |(key, _)| key.hash,
            ) {
                Occupied(mut entry) => {
                    if let Err(err) = entry.get_mut().1.record(value) {
                        error!("Fail to record: {err}");
                    }
                }
                Vacant(entry) => {
                    let histogram_config = self
                        .histogram_configs
                        .get(lookup.metric.as_ref())
                        .unwrap_or(&self.default_histogram_config);
                    if let Some(mut histogram) =
                        get_histogram_from_pool_config(split.pool_histograms, histogram_config)
                    {
                        if let Err(err) = histogram.record(value) {
                            error!("Fail to record: {err}");
                        }
                        entry.insert((lookup.into_key(), histogram));
                    }
                }
            }
            drop(aggregator);
        }
    }

    fn record_histogram_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();
        let split = aggregator.split_borrow();
        let entry_id = prepared.prepared_id();
        if let Some((_, histogram)) = split
            .histograms
            .find_mut(prepared.hash(), |(key, _)| key.id == entry_id)
        {
            if let Err(err) = histogram.record(value) {
                error!("Fail to record: {err}");
            }
            return;
        }

        match split.histograms.entry(
            prepared.hash(),
            |(key, _)| match_prepared_agg_key(key, prepared),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                if let Err(err) = entry.get_mut().1.record(value) {
                    error!("Fail to record: {err}");
                }
            }
            Vacant(entry) => {
                let histogram_config = self
                    .histogram_configs
                    .get(prepared.metric().as_ref())
                    .unwrap_or(&self.default_histogram_config);

                if let Some(mut histogram) =
                    get_histogram_from_pool_config(split.pool_histograms, histogram_config)
                {
                    if let Err(err) = histogram.record(value) {
                        error!("Fail to record: {err}");
                    }
                    entry.insert((to_agg_entry_key(prepared), histogram));
                }
            }
        }
        drop(aggregator);
    }

    fn record_count_add(&self, metric: RylvStr<'_>, value: u64, tags: &mut [RylvStr<'_>]) {
        if tags.len() > 1 {
            tags.sort_unstable();
        }
        let lookup = build_lookup_key(metric, tags, &self.hasher_builder);
        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();

        match aggregator.count.entry(
            lookup.hash,
            |(key, _)| lookup.compare(key),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                entry.get_mut().1 += value;
            }
            Vacant(entry) => {
                entry.insert((lookup.into_key(), value));
            }
        }
    }

    fn record_count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let hash =
            combine_metric_tags_hash(&self.hasher_builder, metric.as_ref(), tags.tags_hash());
        let lookup = LookupKeySorted {
            metric,
            sorted_tags: tags,
            hash,
        };

        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();

        match aggregator.count.entry(
            lookup.hash,
            |(key, _)| lookup.compare(key),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                entry.get_mut().1 += value;
            }
            Vacant(entry) => {
                entry.insert((lookup.into_key(), value));
            }
        }
    }

    fn record_count_add_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();
        let entry_id = prepared.prepared_id();
        if let Some((_, existing)) = aggregator
            .count
            .find_mut(prepared.hash(), |(key, _)| key.id == entry_id)
        {
            *existing += value;
            return;
        }
        match aggregator.count.entry(
            prepared.hash(),
            |(key, _)| match_prepared_agg_key(key, prepared),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                entry.get_mut().1 += value;
            }
            Vacant(entry) => {
                entry.insert((to_agg_entry_key(prepared), value));
            }
        }
        drop(aggregator);
    }

    fn record_gauge(&self, metric: RylvStr<'_>, value: u64, tags: &mut [RylvStr<'_>]) {
        if tags.len() > 1 {
            tags.sort_unstable();
        }
        let lookup = build_lookup_key(metric, tags, &self.hasher_builder);
        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();

        match aggregator.gauge.entry(
            lookup.hash,
            |(key, _)| lookup.compare(key),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                let gauge = &mut entry.get_mut().1;
                gauge.count += 1;
                gauge.sum += value;
            }
            Vacant(entry) => {
                entry.insert((
                    lookup.into_key(),
                    GaugeStateHb {
                        sum: value,
                        count: 1,
                    },
                ));
            }
        }
    }

    fn record_gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let hash =
            combine_metric_tags_hash(&self.hasher_builder, metric.as_ref(), tags.tags_hash());
        let lookup = LookupKeySorted {
            metric,
            sorted_tags: tags,
            hash,
        };

        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();

        match aggregator.gauge.entry(
            lookup.hash,
            |(key, _)| lookup.compare(key),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                let gauge = &mut entry.get_mut().1;
                gauge.count += 1;
                gauge.sum += value;
            }
            Vacant(entry) => {
                entry.insert((
                    lookup.into_key(),
                    GaugeStateHb {
                        sum: value,
                        count: 1,
                    },
                ));
            }
        }
    }

    fn record_gauge_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let buffer = self.get_or_create_thread_local_aggregator();
        let mut aggregator = buffer.lock();

        let entry_id = prepared.prepared_id();
        if let Some((_, gauge)) = aggregator
            .gauge
            .find_mut(prepared.hash(), |(key, _)| key.id == entry_id)
        {
            gauge.count += 1;
            gauge.sum += value;
            return;
        }
        match aggregator.gauge.entry(
            prepared.hash(),
            |(key, _)| match_prepared_agg_key(key, prepared),
            |(key, _)| key.hash,
        ) {
            Occupied(mut entry) => {
                let gauge = &mut entry.get_mut().1;
                gauge.count += 1;
                gauge.sum += value;
            }
            Vacant(entry) => {
                entry.insert((
                    to_agg_entry_key(prepared),
                    GaugeStateHb {
                        sum: value,
                        count: 1,
                    },
                ));
            }
        }
        drop(aggregator);
    }
}

/// Configuration options for the hashbrown + mutex TLS collector.
#[derive(Debug)]
pub struct TLSCollectorOptions<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    /// Prefix prepended verbatim to all metric names.
    pub stats_prefix: String,
    /// Per-metric histogram configuration for custom precision settings.
    pub histogram_configs: HashMap<String, HistogramConfig, S>,
    /// Default histogram configuration when metric-specific config is absent.
    pub default_histogram_config: HistogramConfig,
    /// Hasher builder used by internal aggregation maps.
    pub hasher_builder: S,
}

impl Default for TLSCollectorOptions<DefaultMetricHasher> {
    fn default() -> Self {
        Self {
            stats_prefix: String::new(),
            histogram_configs: HashMap::new(),
            default_histogram_config: HistogramConfig::default(),
            hasher_builder: DefaultMetricHasher::new(),
        }
    }
}

impl<S> MetricCollectorTrait for TLSCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Hasher = S;

    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.record_histogram(metric, value, tags.as_mut());
    }

    fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.count_add(metric, 1, tags);
    }

    fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.record_count_add(metric, value, tags.as_mut());
    }

    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.record_gauge(metric, value, tags.as_mut());
    }

    fn histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        self.record_histogram_sorted(metric, value, tags);
    }

    fn count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        self.record_count_add_sorted(metric, value, tags);
    }

    fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        self.record_gauge_sorted(metric, value, tags);
    }

    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<Self::Hasher> {
        SortedTags::new(tags, &self.hasher_builder)
    }

    fn prepare_metric(
        &self,
        metric: RylvStr<'_>,
        tags: SortedTags<Self::Hasher>,
    ) -> PreparedMetric<Self::Hasher> {
        let metric = to_static_metric(metric);
        let hash =
            combine_metric_tags_hash(&self.hasher_builder, metric.as_ref(), tags.tags_hash());
        PreparedMetric::new(metric, tags, hash)
    }

    fn histogram_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.record_histogram_prepared(prepared, value);
    }

    fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.record_count_add_prepared(prepared, value);
    }

    fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.record_gauge_prepared(prepared, value);
    }
}

impl<S> MetricCollectorTrait for &TLSCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Hasher = S;

    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        (*self).histogram(metric, value, tags);
    }

    fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        (*self).count(metric, tags);
    }

    fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        (*self).count_add(metric, value, tags);
    }

    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        (*self).gauge(metric, value, tags);
    }

    fn histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        (*self).histogram_sorted(metric, value, tags);
    }

    fn count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        (*self).count_add_sorted(metric, value, tags);
    }

    fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        (*self).gauge_sorted(metric, value, tags);
    }

    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<Self::Hasher> {
        (*self).prepare_sorted_tags(tags)
    }

    fn prepare_metric(
        &self,
        metric: RylvStr<'_>,
        tags: SortedTags<Self::Hasher>,
    ) -> PreparedMetric<Self::Hasher> {
        (*self).prepare_metric(metric, tags)
    }

    fn histogram_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        (*self).histogram_prepared(prepared, value);
    }

    fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        (*self).count_add_prepared(prepared, value);
    }

    fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        (*self).gauge_prepared(prepared, value);
    }
}

impl<S> DrainMetricCollectorTrait for TLSCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Drain<'a>
        = TLSDrain<'a, S>
    where
        Self: 'a;

    fn try_begin_drain(&self) -> Option<Self::Drain<'_>> {
        Some(self.begin_drain())
    }
}

impl<S> DrainMetricCollectorTrait for &TLSCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Drain<'a>
        = TLSDrain<'a, S>
    where
        Self: 'a;

    fn try_begin_drain(&self) -> Option<Self::Drain<'_>> {
        Some((*self).begin_drain())
    }
}

fn build_lookup_key<'a, S>(
    metric: RylvStr<'a>,
    tags: &'a [RylvStr<'a>],
    hasher_builder: &S,
) -> LookupKey<'a>
where
    S: BuildHasher,
{
    let tags_hash = hash_tags(hasher_builder, tags);
    let hash = combine_metric_tags_hash(hasher_builder, metric.as_ref(), tags_hash);
    LookupKey {
        metric,
        tags,
        tags_hash,
        hash,
    }
}

fn match_prepared_agg_key<S>(key: &AggregatorEntryKey<S>, prepared: &PreparedMetric<S>) -> bool
where
    S: BuildHasher + Clone,
{
    if key.fingerprint != prepared.fingerprint() {
        return false;
    }
    key.metric.as_ref() == prepared.metric().as_ref() && &key.tags == prepared.tags()
}

fn merge_local_aggregator_into_global_hashbrown<S>(
    local: &mut LocalAggregatorHb<S>,
    global: &mut GlobalAggregatorHb<S>,
    pool_specs: &[HistogramPoolSpec],
    to_remove: &mut Vec<RemoveKey>,
) where
    S: BuildHasher + Clone,
{
    to_remove.clear();
    for (key, value) in &mut local.count {
        let val = *value;
        if val == 0 {
            to_remove.push(key.remove_key());
            continue;
        }

        *value = 0;
        match global
            .count
            .entry(key.hash, |(existing, _)| existing == key, |(k, _)| k.hash)
        {
            Occupied(mut entry) => {
                entry.get_mut().1 += val;
            }
            Vacant(entry) => {
                entry.insert((key.clone(), val));
            }
        }
    }

    remove_from_table(&mut local.count, to_remove);

    for (key, value) in &mut local.gauge {
        if value.count == 0 {
            to_remove.push(key.remove_key());
            continue;
        }

        match global
            .gauge
            .entry(key.hash, |(existing, _)| existing == key, |(k, _)| k.hash)
        {
            Occupied(mut entry) => {
                let existing = &mut entry.get_mut().1;
                existing.sum += value.sum;
                existing.count += value.count;
            }
            Vacant(entry) => {
                entry.insert((key.clone(), *value));
            }
        }

        value.count = 0;
        value.sum = 0;
    }

    remove_from_table(&mut local.gauge, to_remove);

    to_remove.clear();
    for (key, local_histogram) in &mut local.histograms {
        if local_histogram.histogram.is_empty() {
            to_remove.push(key.remove_key());
            continue;
        }

        match global
            .histograms
            .entry(key.hash, |(existing, _)| key == existing, |(k, _)| k.hash)
        {
            Occupied(mut entry) => {
                let global_histogram = &mut entry.get_mut().1;
                global_histogram.min = min(global_histogram.min, local_histogram.min);
                global_histogram.max = max(global_histogram.max, local_histogram.max);
                if let Err(err) = global_histogram.histogram.add(&local_histogram.histogram) {
                    error!("Fail to merge histogram: {err}");
                }
            }
            Vacant(entry) => {
                if let Some(fresh_histogram) = get_histogram_from_pool(
                    &mut local.pool_histograms,
                    local_histogram.pool_id,
                    pool_specs[local_histogram.pool_id].sig_fig,
                    pool_specs[local_histogram.pool_id].bounds,
                    local_histogram.percentiles.clone(),
                    local_histogram.emit_base_metrics,
                ) {
                    let owned_histogram = std::mem::replace(local_histogram, fresh_histogram);
                    entry.insert((key.clone(), owned_histogram));
                } else {
                    error!("Fail to allocate histogram while merging local aggregator");
                    continue;
                }
            }
        }
        local_histogram.reset();
    }

    remove_from_table_callback(&mut local.histograms, to_remove, |histogram_wrapper| {
        let index = histogram_wrapper.pool_id;
        debug_assert!(index < local.pool_histograms.len());
        unsafe { local.pool_histograms.get_unchecked_mut(index) }.push(histogram_wrapper);
    });
}

fn remove_from_table<S: BuildHasher + Clone, V>(
    table: &mut HashTable<(AggregatorEntryKey<S>, V)>,
    to_remove: &mut Vec<RemoveKey>,
) {
    remove_from_table_callback(table, to_remove, |_| ());
}

fn remove_from_table_callback<S: BuildHasher + Clone, V>(
    table: &mut HashTable<(AggregatorEntryKey<S>, V)>,
    to_remove: &mut Vec<RemoveKey>,
    mut on_removed: impl FnMut(V),
) {
    for key in to_remove.iter() {
        if let Occupied(entry) = table.entry(key.hash, |k| key.id == k.0.id, |k| k.0.hash) {
            let (value, _) = entry.remove();
            on_removed(value.1);
        }
    }
    to_remove.clear();
}

pub struct TLSDrain<'a, S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    collector: &'a TLSCollector<S>,
    prefix: &'a str,
    stage: DrainStage,
    count_iter: Option<MyIterMut<'a, (AggregatorEntryKey<S>, u64)>>,
    gauge_iter: Option<MyIterMut<'a, (AggregatorEntryKey<S>, GaugeStateHb)>>,
    histogram_iter: Option<MyIterMut<'a, (AggregatorEntryKey<S>, HistogramWrapper)>>,

    pool_histograms: &'a mut [Vec<HistogramWrapper>],
    keys_to_remove: &'a mut Vec<RemoveKey>,
    pending_histogram: Option<PendingHistogram<'a, S>>,

    // SAFETY:
    // `TLSDrain` is self-referential: the iterators and borrowed slices above point into this
    // heap-allocated `GlobalAggregatorHb`. Keeping the raw pointer as the final field ensures all
    // other fields are dropped before we reconstruct and free the box in `Drop`, so those borrows
    // never outlive the backing aggregator allocation.
    aggregator: Option<*mut GlobalAggregatorHb<S>>,
}

impl<'a, S> TLSDrain<'a, S>
where
    S: BuildHasher + Clone + Send + Sync,
{
    fn new(collector: &'a TLSCollector<S>, aggregator: GlobalAggregatorHb<S>) -> Self {
        let global = Box::new(aggregator);
        let global_ptr = Box::into_raw(global);
        Self {
            collector,
            prefix: collector.stats_prefix.as_str(),
            stage: DrainStage::Count,
            count_iter: Some(MyIterMut::new(unsafe { addr_of_mut!((*global_ptr).count) })),
            gauge_iter: Some(MyIterMut::new(unsafe { addr_of_mut!((*global_ptr).gauge) })),
            histogram_iter: Some(MyIterMut::new(unsafe {
                addr_of_mut!((*global_ptr).histograms)
            })),
            pool_histograms: unsafe { &mut *addr_of_mut!((*global_ptr).pool_histograms) },
            keys_to_remove: unsafe { &mut *addr_of_mut!((*global_ptr).key_to_remove) },
            pending_histogram: None,

            aggregator: Some(global_ptr),
        }
    }

    fn emit_count_metric(&mut self) -> Option<MetricFrameRef<'a>> {
        if let Some(iter) = self.count_iter.as_mut() {
            for entry in iter.by_ref() {
                let key = &mut entry.0;
                let value = entry.1;
                if value == 0 {
                    self.keys_to_remove.push(key.remove_key());
                    continue;
                }

                // SAFETY: `AggregatorEntryKey` stores owned `'static` metric/tag data.
                // During drain we borrow those strings for `'a`, where `'a` is bounded by the
                // lifetime of `TLSDrain`. Non-empty count entries are not removed until the
                // current count stage finishes, and the backing `GlobalAggregatorHb` is owned by
                // `TLSDrain`, so the borrowed strings remain valid for the yielded frame.
                let (metric, tags) = unsafe {
                    (
                        std::mem::transmute::<&str, &'a str>(key.metric.as_ref()),
                        std::mem::transmute::<&str, &'a str>(key.tags.joined_tags()),
                    )
                };

                return Some(MetricFrameRef {
                    prefix: self.prefix,
                    metric,
                    suffix: MetricSuffix::None,
                    tags,
                    value,
                    kind: MetricKind::Count,
                });
            }
        }

        if let Some(table) = self.count_iter.take().map(|iter| iter.table) {
            let table = unsafe { &mut *table };
            remove_from_table(table, self.keys_to_remove);
        }
        self.stage = DrainStage::Gauge;
        None
    }

    fn emit_gauge_metric(&mut self) -> Option<MetricFrameRef<'a>> {
        if let Some(iter) = self.gauge_iter.as_mut() {
            for entry in iter.by_ref() {
                let key = &mut entry.0;
                let gauge = &mut entry.1;
                let count = gauge.count;
                if count == 0 {
                    self.keys_to_remove.push(key.remove_key());
                    continue;
                }

                let value = gauge.sum / count;
                // SAFETY: `AggregatorEntryKey` stores owned `'static` metric/tag data.
                // During drain we borrow those strings for `'a`, where `'a` is bounded by the
                // lifetime of `TLSDrain`. Non-empty gauge entries are reset in place but are not
                // removed until after the gauge stage, and the backing aggregator allocation stays
                // owned by `TLSDrain`, so the borrowed strings remain valid for the yielded frame.
                let (metric, tags) = unsafe {
                    (
                        std::mem::transmute::<&str, &'a str>(key.metric.as_ref()),
                        std::mem::transmute::<&str, &'a str>(key.tags.joined_tags()),
                    )
                };
                gauge.sum = 0;
                gauge.count = 0;

                return Some(MetricFrameRef {
                    prefix: self.prefix,
                    metric,
                    suffix: MetricSuffix::None,
                    tags,
                    value,
                    kind: MetricKind::Gauge,
                });
            }
        }

        if let Some(table) = self.gauge_iter.take().map(|iter| iter.table) {
            let table = unsafe { &mut *table };
            remove_from_table(table, self.keys_to_remove);
        }

        self.stage = DrainStage::Histogram;
        None
    }

    fn load_next_histogram(&mut self) -> bool {
        if let Some(iter) = self.histogram_iter.as_mut() {
            for histogram_entry in iter.by_ref() {
                let key = &mut histogram_entry.0;
                let histo_wrapper = &mut histogram_entry.1;
                if histo_wrapper.histogram.is_empty() {
                    self.keys_to_remove.push(key.remove_key());
                    continue;
                }

                // SAFETY: `AggregatorEntryKey` stores owned `'static` metric/tag data.
                // During drain we borrow those strings for `'a`, where `'a` is bounded by the
                // lifetime of `TLSDrain`. Non-empty histogram entries are not removed until the
                // histogram stage completes, and the backing `GlobalAggregatorHb` is owned by
                // `TLSDrain`, so the borrowed strings remain valid across all percentile/base
                // metric frames emitted from `pending_histogram`. The Miri TLS-drain test covers
                // this invariant by reading borrowed frame fields across iteration.
                let (metric, tags) = unsafe {
                    (
                        std::mem::transmute::<&str, &'a str>(key.metric.as_ref()),
                        std::mem::transmute::<&str, &'a str>(key.tags.joined_tags()),
                    )
                };

                let pending = PendingHistogram {
                    metric,
                    tags,
                    entry: histogram_entry,
                    step: 0,
                };
                self.pending_histogram = Some(pending);
                return true;
            }
        }

        false
    }

    fn emit_pending_histogram(&mut self) -> Option<MetricFrameRef<'a>> {
        let mut pending = self.pending_histogram.take()?;
        loop {
            let histo_wrapper = &mut pending.entry.1;
            let percentile_count = histo_wrapper.percentiles.len();
            let frame = match pending.step {
                0 => {
                    pending.step += 1;
                    if histo_wrapper.emits(HistogramBaseMetric::Count) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".count"),
                            tags: pending.tags,
                            value: histo_wrapper.histogram.len(),
                            kind: MetricKind::Count,
                        })
                    } else {
                        None
                    }
                }
                1 => {
                    pending.step += 1;
                    if histo_wrapper.emits(HistogramBaseMetric::Min) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".min"),
                            tags: pending.tags,
                            value: histo_wrapper.min,
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                2 => {
                    pending.step += 1;
                    if histo_wrapper.emits(HistogramBaseMetric::Avg) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".avg"),
                            tags: pending.tags,
                            value: histo_wrapper.histogram.value_at_quantile(0.50),
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                index if index < 3 + percentile_count => {
                    pending.step += 1;
                    let percentile_index = index - 3;
                    let percentile = histo_wrapper.percentiles[percentile_index];
                    Some(MetricFrameRef {
                        prefix: self.prefix,
                        metric: pending.metric,
                        suffix: MetricSuffix::Percentile(percentile),
                        tags: pending.tags,
                        value: histo_wrapper.histogram.value_at_quantile(percentile),
                        kind: MetricKind::Gauge,
                    })
                }
                index if index == 3 + percentile_count => {
                    pending.step += 1;
                    if histo_wrapper.emits(HistogramBaseMetric::Max) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".max"),
                            tags: pending.tags,
                            value: histo_wrapper.max,
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                _ => {
                    histo_wrapper.reset();
                    return None;
                }
            };

            if let Some(frame) = frame {
                self.pending_histogram = Some(pending);
                return Some(frame);
            }
        }
    }
}

impl<S> Drop for TLSDrain<'_, S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.count_iter = None;
        self.gauge_iter = None;
        self.histogram_iter = None;

        if let Some(aggregator) = self.aggregator.take() {
            let agg = *unsafe { Box::from_raw(aggregator) };
            self.collector.recycle_global(agg);
        }
    }
}

struct PendingHistogram<'a, S>
where
    S: BuildHasher + Clone,
{
    metric: &'a str,
    tags: &'a str,
    entry: &'a mut (AggregatorEntryKey<S>, HistogramWrapper),
    step: usize,
}

struct MyIterMut<'a, T>
where
    T: 'a,
{
    table: *mut hashbrown::HashTable<T>,
    iter_mut: hashbrown::hash_table::IterMut<'a, T>,
}

impl<T> MyIterMut<'_, T> {
    fn new(table_ptr: *mut HashTable<T>) -> Self {
        let iter_mut = unsafe { (*table_ptr).iter_mut() };
        let static_iter = unsafe {
            // SAFETY: `iter_mut` only gets stored inside `MyIterMut`, which itself is embedded in
            // `TLSDrain<'a, _>`. `TLSDrain` owns the pointed-to table allocation through
            // `aggregator`, so the table outlives the iterator for `'a`. `Drop` clears all
            // iterators before freeing the backing `GlobalAggregatorHb`.
            std::mem::transmute::<
                hashbrown::hash_table::IterMut<'_, T>,
                hashbrown::hash_table::IterMut<'_, T>,
            >(iter_mut)
        };

        Self {
            table: table_ptr,
            iter_mut: static_iter,
        }
    }
}

impl<'a, T> Iterator for MyIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_mut.next()
    }
}

impl<'a, S> Iterator for TLSDrain<'a, S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Item = MetricFrameRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.stage {
                DrainStage::Count => {
                    if let Some(frame) = self.emit_count_metric() {
                        return Some(frame);
                    }
                }
                DrainStage::Gauge => {
                    if let Some(frame) = self.emit_gauge_metric() {
                        return Some(frame);
                    }
                }
                DrainStage::Histogram => {
                    if self.pending_histogram.is_none() && !self.load_next_histogram() {
                        if let Some(table) = self.histogram_iter.take().map(|iter| iter.table) {
                            let table = unsafe { &mut *table };
                            remove_from_table_callback(
                                table,
                                self.keys_to_remove,
                                |v: HistogramWrapper| {
                                    let index = v.pool_id;
                                    debug_assert!(index < self.pool_histograms.len());
                                    unsafe { self.pool_histograms.get_unchecked_mut(index) }
                                        .push(v);
                                },
                            );
                        }
                        self.stage = DrainStage::Done;
                        continue;
                    }

                    if let Some(frame) = self.emit_pending_histogram() {
                        return Some(frame);
                    }
                }
                DrainStage::Done => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_lookup_key, get_histogram_from_pool, get_histogram_from_pool_config,
        merge_local_aggregator_into_global_hashbrown, GaugeStateHb, GlobalAggregatorHb,
        LocalAggregatorHb, TLSCollector, TLSCollectorOptions,
    };
    use crate::dogstats::aggregator::HistogramWrapper;
    use crate::dogstats::collector::{DrainMetricCollectorTrait, MetricKind, MetricSuffix};
    use crate::dogstats::histogram_config::{
        resolve_histogram_configs, Bounds, HistogramBaseMetric, HistogramBaseMetrics,
        HistogramConfig,
    };
    use crate::{MetricCollectorTrait, RylvStr};
    use hdrhistogram::Histogram;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn percentile_suffix(percentile: f64) -> String {
        let mut percentile_number = (percentile * 100.0).to_string();
        if percentile_number.contains('.') {
            while percentile_number.ends_with('0') {
                percentile_number.pop();
            }
            if percentile_number.ends_with('.') {
                percentile_number.pop();
            }
        }
        format!(".{percentile_number}percentile")
    }

    fn format_drained_lines<S, I>(drain: I) -> Vec<String>
    where
        S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
        I: IntoIterator<Item = crate::dogstats::collector::MetricFrameRef<'static>>,
    {
        let _ = std::marker::PhantomData::<S>;
        let mut lines = Vec::new();
        for frame in drain {
            let mut metric = String::new();
            metric.push_str(frame.prefix);
            metric.push_str(frame.metric);
            match frame.suffix {
                MetricSuffix::None => {}
                MetricSuffix::Static(suffix) => metric.push_str(suffix),
                MetricSuffix::Percentile(percentile) => {
                    metric.push_str(percentile_suffix(percentile).as_str());
                }
            }
            let metric_type = match frame.kind {
                MetricKind::Count => "c",
                MetricKind::Gauge => "g",
            };
            if frame.tags.is_empty() {
                lines.push(format!("{metric}:{}|{metric_type}\n", frame.value));
            } else {
                lines.push(format!(
                    "{metric}:{}|{metric_type}|#{}\n",
                    frame.value, frame.tags
                ));
            }
        }
        lines.sort_unstable();
        lines
    }

    fn drain_metrics_now<S>(collector: &TLSCollector<S>) -> Vec<String>
    where
        S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
    {
        let drain = collector.try_begin_drain().into_iter().flatten().map(|frame| unsafe {
            std::mem::transmute::<
                crate::dogstats::collector::MetricFrameRef<'_>,
                crate::dogstats::collector::MetricFrameRef<'static>,
            >(frame)
        });
        format_drained_lines::<S, _>(drain)
    }

    fn assert_regular_reference_lines(lines: &[String]) {
        assert_eq!(
            lines,
            &[
                "ref.latency.95percentile:60|g|#a:1,b:2\n".to_string(),
                "ref.latency.99percentile:60|g|#a:1,b:2\n".to_string(),
                "ref.latency.avg:40|g|#a:1,b:2\n".to_string(),
                "ref.latency.count:2|c|#a:1,b:2\n".to_string(),
                "ref.latency.max:60|g|#a:1,b:2\n".to_string(),
                "ref.latency.min:40|g|#a:1,b:2\n".to_string(),
                "ref.load:15|g|#a:1,b:2\n".to_string(),
                "ref.requests:5|c|#a:1,b:2\n".to_string(),
            ]
        );
    }

    fn insert_local_entries(
        local: &mut LocalAggregatorHb<crate::DefaultMetricHasher>,
        resolved: &crate::dogstats::histogram_config::ResolvedHistogramConfigs<
            crate::DefaultMetricHasher,
        >,
        hasher: &crate::DefaultMetricHasher,
    ) {
        let count_key = build_lookup_key(
            RylvStr::from_static("requests"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(10);
        local
            .count
            .entry(
                count_key.hash,
                |(key, _)| key == &count_key,
                |(key, _)| key.hash,
            )
            .insert((count_key, 5));

        let zero_count_key = build_lookup_key(
            RylvStr::from_static("empty_requests"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(11);
        local
            .count
            .entry(
                zero_count_key.hash,
                |(key, _)| key == &zero_count_key,
                |(key, _)| key.hash,
            )
            .insert((zero_count_key, 0));

        let gauge_key = build_lookup_key(
            RylvStr::from_static("load"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(12);
        local
            .gauge
            .entry(
                gauge_key.hash,
                |(key, _)| key == &gauge_key,
                |(key, _)| key.hash,
            )
            .insert((gauge_key, GaugeStateHb { sum: 30, count: 2 }));

        let zero_gauge_key = build_lookup_key(
            RylvStr::from_static("empty_load"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(13);
        local
            .gauge
            .entry(
                zero_gauge_key.hash,
                |(key, _)| key == &zero_gauge_key,
                |(key, _)| key.hash,
            )
            .insert((zero_gauge_key, GaugeStateHb { sum: 0, count: 0 }));

        let hist_key = build_lookup_key(
            RylvStr::from_static("latency"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(14);
        let mut histogram = get_histogram_from_pool_config(
            &mut local.pool_histograms,
            &resolved.default_histogram_config,
        )
        .unwrap();
        histogram.record(40).unwrap();
        histogram.record(60).unwrap();
        local
            .histograms
            .entry(
                hist_key.hash,
                |(key, _)| key == &hist_key,
                |(key, _)| key.hash,
            )
            .insert((hist_key, histogram));

        let empty_hist_key = build_lookup_key(
            RylvStr::from_static("empty_latency"),
            &[RylvStr::from_static("a:1")],
            hasher,
        )
        .into_key_with_id(15);
        let empty_histogram = get_histogram_from_pool_config(
            &mut local.pool_histograms,
            &resolved.default_histogram_config,
        )
        .unwrap();
        local
            .histograms
            .entry(
                empty_hist_key.hash,
                |(key, _)| key == &empty_hist_key,
                |(key, _)| key.hash,
            )
            .insert((empty_hist_key, empty_histogram));
    }

    #[test]
    fn get_histogram_from_pool_reuses_available_entry() {
        let mut pool_histograms = vec![vec![HistogramWrapper {
            pool_id: 0,
            min: 10,
            max: 20,
            histogram: Histogram::new_with_bounds(1, u64::MAX, 3).unwrap(),
            percentiles: Arc::from([0.99_f64]),
            emit_base_metrics: HistogramBaseMetrics::NONE,
        }]];

        let wrapper = get_histogram_from_pool(
            &mut pool_histograms,
            0,
            crate::SigFig::default(),
            Bounds::default(),
            Arc::from([0.5_f64, 0.95_f64]),
            HistogramBaseMetrics::only(HistogramBaseMetric::Count),
        )
        .unwrap();

        assert!(pool_histograms[0].is_empty());
        assert_eq!(wrapper.pool_id, 0);
        assert_eq!(wrapper.min, 10);
        assert_eq!(wrapper.max, 20);
        assert_eq!(wrapper.percentiles.as_ref(), &[0.5, 0.95]);
        assert!(wrapper.emits(HistogramBaseMetric::Count));
    }

    #[test]
    fn tls_collector_drains_sorted_and_prepared_metrics() {
        let collector = TLSCollector::new(TLSCollectorOptions {
            stats_prefix: "tls.".to_string(),
            ..Default::default()
        });

        let sorted = collector
            .prepare_sorted_tags([RylvStr::from_static("b:2"), RylvStr::from_static("a:1")]);
        let prepared_count =
            collector.prepare_metric(RylvStr::from_static("requests"), sorted.clone());
        let prepared_gauge = collector.prepare_metric(RylvStr::from_static("load"), sorted.clone());
        let prepared_hist =
            collector.prepare_metric(RylvStr::from_static("latency"), sorted.clone());

        collector.count_add_sorted(RylvStr::from_static("requests"), 2, &sorted);
        collector.count_add_prepared(&prepared_count, 3);
        collector.gauge_sorted(RylvStr::from_static("load"), 10, &sorted);
        collector.gauge_prepared(&prepared_gauge, 20);
        collector.histogram_sorted(RylvStr::from_static("latency"), 40, &sorted);
        collector.histogram_prepared(&prepared_hist, 60);

        assert_eq!(
            drain_metrics_now(&collector),
            vec![
                "tls.latency.95percentile:60|g|#a:1,b:2\n".to_string(),
                "tls.latency.99percentile:60|g|#a:1,b:2\n".to_string(),
                "tls.latency.avg:40|g|#a:1,b:2\n".to_string(),
                "tls.latency.count:2|c|#a:1,b:2\n".to_string(),
                "tls.latency.max:60|g|#a:1,b:2\n".to_string(),
                "tls.latency.min:40|g|#a:1,b:2\n".to_string(),
                "tls.load:15|g|#a:1,b:2\n".to_string(),
                "tls.requests:5|c|#a:1,b:2\n".to_string(),
            ]
        );
        assert!(drain_metrics_now(&collector).is_empty());
    }

    #[test]
    fn tls_reference_trait_impls_cover_regular_paths() {
        let collector = TLSCollector::new(TLSCollectorOptions {
            stats_prefix: "ref.".to_string(),
            ..Default::default()
        });
        let collector_ref = &collector;

        collector_ref.count(
            RylvStr::from_static("requests"),
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        collector_ref.count_add(
            RylvStr::from_static("requests"),
            4,
            &mut [RylvStr::from_static("a:1"), RylvStr::from_static("b:2")],
        );
        collector_ref.gauge(
            RylvStr::from_static("load"),
            10,
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        collector_ref.gauge(
            RylvStr::from_static("load"),
            20,
            &mut [RylvStr::from_static("a:1"), RylvStr::from_static("b:2")],
        );
        collector_ref.histogram(
            RylvStr::from_static("latency"),
            40,
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        collector_ref.histogram(
            RylvStr::from_static("latency"),
            60,
            &mut [RylvStr::from_static("a:1"), RylvStr::from_static("b:2")],
        );

        let drain = <&TLSCollector as DrainMetricCollectorTrait>::try_begin_drain(&collector_ref)
            .unwrap()
            .map(|frame| unsafe {
                std::mem::transmute::<
                    crate::dogstats::collector::MetricFrameRef<'_>,
                    crate::dogstats::collector::MetricFrameRef<'static>,
                >(frame)
            });
        let lines = format_drained_lines::<crate::DefaultMetricHasher, _>(drain);
        assert_regular_reference_lines(&lines);
    }

    #[test]
    fn merge_local_aggregator_moves_values_and_removes_empty_entries() {
        let hasher = crate::DefaultMetricHasher::new();
        let resolved = resolve_histogram_configs(
            HistogramConfig::default(),
            HashMap::with_hasher(hasher.clone()),
            &hasher,
        );
        let mut local = LocalAggregatorHb::with_pool_count(&hasher, resolved.pool_count);
        let mut global = GlobalAggregatorHb::with_pool_count(&hasher, resolved.pool_count);
        let mut to_remove = Vec::new();

        insert_local_entries(&mut local, &resolved, &hasher);
        merge_local_aggregator_into_global_hashbrown(
            &mut local,
            &mut global,
            &resolved.pool_specs,
            &mut to_remove,
        );

        assert_eq!(global.count.len(), 1);
        assert_eq!(global.gauge.len(), 1);
        assert_eq!(global.histograms.len(), 1);
        assert_eq!(local.count.len(), 1);
        assert_eq!(local.gauge.len(), 1);
        assert_eq!(local.histograms.len(), 1);
        assert_eq!(local.pool_histograms[0].len(), 1);
        assert_eq!(local.count.iter().next().unwrap().1, 0);
        assert_eq!(local.gauge.iter().next().unwrap().1.count, 0);
        assert_eq!(local.gauge.iter().next().unwrap().1.sum, 0);
        assert!(local
            .histograms
            .iter()
            .next()
            .unwrap()
            .1
            .histogram
            .is_empty());

        merge_local_aggregator_into_global_hashbrown(
            &mut local,
            &mut global,
            &resolved.pool_specs,
            &mut to_remove,
        );

        assert_eq!(global.count.iter().next().unwrap().1, 5);
        let global_gauge = &global.gauge.iter().next().unwrap().1;
        assert_eq!(global_gauge.sum, 30);
        assert_eq!(global_gauge.count, 2);
        assert_eq!(
            global.histograms.iter().next().unwrap().1.histogram.len(),
            2
        );
        assert!(local.count.is_empty());
        assert!(local.gauge.is_empty());
        assert!(local.histograms.is_empty());
        assert_eq!(local.pool_histograms[0].len(), 2);
    }
}
