use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{cmp::Ordering as CmpOrdering, collections::HashMap};

use super::{DrainMetricCollectorTrait, MetricCollectorTrait};
use crate::dogstats::aggregator::{
    to_agg_entry_key, AggregatorEntryKey, HistogramWrapper, LookupKey, LookupKeySorted, RemoveKey,
};
use crate::dogstats::collector::{MetricFrameRef, MetricKind, MetricSuffix};
use crate::dogstats::histogram_config::{
    resolve_histogram_configs, HistogramBaseMetric, HistogramConfig, ResolvedHistogramConfig,
    ResolvedHistogramConfigs,
};
use crate::dogstats::sorted_tags::{combine_metric_tags_hash, hash_tags, PreparedMetric};
use crate::dogstats::{Aggregator, RylvStr, SortedTags};
use crate::DefaultMetricHasher;
use arc_swap::ArcSwap;
use dashmap::{DashMap, SharedValue};
use tracing::error;

pub struct GaugeState {
    pub sum: AtomicU64,
    pub count: AtomicU64,
}

/// Configuration options for the metric collector.
///
/// This variant does not spawn a background job and does not perform network I/O.
/// Collected metrics can be drained by calling [`SharedCollector::try_begin_drain`].
#[derive(Debug)]
pub struct SharedCollectorOptions<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    /// Prefix prepended verbatim to all metric names.
    pub stats_prefix: String,
    /// Per-metric histogram configuration for custom precision settings.
    pub histogram_configs: std::collections::HashMap<String, HistogramConfig, S>,
    /// Default histogram configuration when metric-specific config is absent.
    pub default_histogram_config: HistogramConfig,
    /// Hasher builder used by internal aggregation maps.
    pub hasher_builder: S,
}

impl Default for SharedCollectorOptions<DefaultMetricHasher> {
    fn default() -> Self {
        Self {
            stats_prefix: String::new(),
            histogram_configs: std::collections::HashMap::new(),
            default_histogram_config: HistogramConfig::default(),
            hasher_builder: DefaultMetricHasher::new(),
        }
    }
}
/// A shared metrics collector.
///
/// Unlike [`MetricCollector`], this type does not spawn a background thread and does not
/// perform any network I/O. Metrics are aggregated in-memory and can be emitted by calling
/// [`SharedCollector::try_begin_drain`].
pub struct SharedCollector<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    current_aggregator: Arc<ArcSwap<Aggregator<S>>>,
    pending_to_process_aggregator: Mutex<Option<Arc<Aggregator<S>>>>,
    available_aggregator: Mutex<Option<Aggregator<S>>>,
    hasher_builder: S,
    pool_count: usize,
    default_histogram_config: ResolvedHistogramConfig,
    histogram_configs: std::collections::HashMap<String, ResolvedHistogramConfig, S>,
    stats_prefix: String,
}

impl Default for SharedCollector {
    fn default() -> Self {
        Self::new(SharedCollectorOptions::default())
    }
}

/// Owned drain handle for collection.
///
/// This handle owns one drained aggregator generation and yields borrowed
/// frames via its `Iterator` implementation. Dropping the handle recycles the
/// aggregator for future collection rounds.
pub struct SharedDrain<'a, S>
where
    S: BuildHasher + Clone,
{
    collector: &'a SharedCollector<S>,
    aggregator: Option<*mut Aggregator<S>>,
    frames: Frames<'a, S>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DrainStage {
    Count,
    Gauge,
    Histogram,
    Done,
}

type CountDrainIter<'a, S> = dashmap::iter::Iter<
    'a,
    AggregatorEntryKey<S>,
    AtomicU64,
    S,
    DashMap<AggregatorEntryKey<S>, AtomicU64, S>,
>;
type GaugeDrainIter<'a, S> = dashmap::iter::Iter<
    'a,
    AggregatorEntryKey<S>,
    GaugeState,
    S,
    DashMap<AggregatorEntryKey<S>, GaugeState, S>,
>;
type HistogramDrainIter<'a, S> = dashmap::iter::IterMut<
    'a,
    AggregatorEntryKey<S>,
    HistogramWrapper,
    S,
    DashMap<AggregatorEntryKey<S>, HistogramWrapper, S>,
>;

/// Stateful borrowed frame drainer.
pub struct Frames<'a, S>
where
    S: BuildHasher + Clone,
{
    prefix: &'a str,
    stage: DrainStage,
    count_iter: Option<CountDrainIter<'a, S>>,
    gauge_iter: Option<GaugeDrainIter<'a, S>>,
    histogram_iter: Option<HistogramDrainIter<'a, S>>,
    count: &'a DashMap<AggregatorEntryKey<S>, AtomicU64, S>,
    gauge: &'a DashMap<AggregatorEntryKey<S>, GaugeState, S>,
    histogram: &'a DashMap<AggregatorEntryKey<S>, HistogramWrapper, S>,
    pool_histograms: &'a [crossbeam::queue::SegQueue<HistogramWrapper>],
    keys_to_remove: Vec<RemoveKey>,
    pending_histogram: Option<PendingHistogram<'a, S>>,
}

struct PendingHistogram<'a, S>
where
    S: BuildHasher + Clone,
{
    metric: &'a str,
    tags: &'a str,
    entry: dashmap::mapref::multiple::RefMutMulti<'a, AggregatorEntryKey<S>, HistogramWrapper>,
    step: usize,
}

impl<S> SharedCollector<S>
where
    S: BuildHasher + Clone,
{
    /// Creates a shared in-memory collector from the provided options.
    #[must_use]
    pub fn new(options: SharedCollectorOptions<S>) -> Self {
        let hasher_builder = options.hasher_builder.clone();
        let ResolvedHistogramConfigs {
            default_histogram_config,
            histogram_configs,
            pool_specs: _,
            pool_count,
        } = resolve_histogram_configs(
            options.default_histogram_config,
            options.histogram_configs,
            &hasher_builder,
        );
        Self {
            current_aggregator: Arc::new(ArcSwap::new(Arc::new(Aggregator::with_hasher_builder(
                &hasher_builder,
                pool_count,
            )))),
            pending_to_process_aggregator: Mutex::new(None),
            available_aggregator: Mutex::new(None),
            hasher_builder,
            pool_count,
            default_histogram_config,
            histogram_configs,
            stats_prefix: options.stats_prefix,
        }
    }

    fn begin_drain(&self) -> Option<SharedDrain<'_, S>> {
        let mut pending = self.pending_to_process_aggregator.try_lock().ok()?;
        let alloc_agg = if let Some(alloc_agg) = pending.take() {
            alloc_agg
        } else {
            let aggregator = self
                .available_aggregator
                .try_lock()
                .ok()?
                .take()
                .unwrap_or_else(|| {
                    Aggregator::with_hasher_builder(&self.hasher_builder, self.pool_count)
                });
            self.current_aggregator.swap(Arc::new(aggregator))
        };
        match Arc::try_unwrap(alloc_agg) {
            Ok(aggregator) => {
                let agg_ptr = Box::into_raw(Box::new(aggregator));
                Some(SharedDrain {
                    collector: self,
                    frames: drain_aggregator_frames(
                        unsafe { &*agg_ptr },
                        self.stats_prefix.as_str(),
                    ),
                    aggregator: Some(agg_ptr),
                })
            }
            Err(alloc_agg) => {
                *pending = Some(alloc_agg);
                None
            }
        }
    }
}

impl<'a, S> Iterator for SharedDrain<'a, S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Item = MetricFrameRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.frames.next_frame()
    }
}

impl<S> Drop for SharedDrain<'_, S>
where
    S: BuildHasher + Clone,
{
    fn drop(&mut self) {
        // SAFETY: because we only add not mutable alias,
        // There is no order in drop issues here
        if let Some(aggregator) = self.aggregator.take() {
            if let Ok(mut available) = self.collector.available_aggregator.try_lock() {
                *available = Some(unsafe { *Box::from_raw(aggregator) });
            }
        }
    }
}

fn add_or_insert_entry_read_first<V, S>(
    metric: RylvStr<'_>,
    tags: &[RylvStr<'_>],
    value: u64,
    hashmap: &DashMap<AggregatorEntryKey<S>, V, impl BuildHasher + Clone>,
    record_fn: impl FnOnce(&V, u64) -> Result<(), String>,
    new_fn: impl FnOnce() -> Option<V>,
) where
    S: BuildHasher + Clone,
{
    let lookup_key = build_lookup_key(metric, tags, hashmap);

    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(lookup_key.hash as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };

    // fast path using read lock only
    {
        let search_result = shard_lock
            .read()
            .find(lookup_key.hash, |(k, _)| lookup_key.compare(k));
        if let Some(bucket) = search_result {
            // SAFETY: because we have a shard_lock with read access, there are no concurrent writer in the shard
            let x = unsafe { bucket.as_ref() }.1.get();
            if let Err(err) = record_fn(x, value) {
                error!("Fail to record: {err}");
            }
            return;
        }
    }

    let mut guard = shard_lock.write();

    // lookup again
    let search_result = guard.find_or_find_insert_slot(
        lookup_key.hash,
        |(k, _)| lookup_key.compare(k),
        |(k, _)| k.hash,
    );

    match search_result {
        // fast path using write lock
        Ok(bucket) => {
            // SAFETY: because we have a shard_lock with write access, there are no concurrent writer in the shard
            if let Err(err) = record_fn(unsafe { bucket.as_ref() }.1.get(), value) {
                error!("Fail to record: {err}");
            }
        }

        // slow path using write lock, initializing the value
        Err(insert_slot) => {
            if let Some(v) = new_fn() {
                if let Err(err) = record_fn(&v, value) {
                    error!("Fail to record: {err}");
                }

                let agg_key = lookup_key.into_key();

                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
            }
        }
    }
}

fn add_or_insert_entry_read_first_sorted<V, S>(
    metric: RylvStr<'_>,
    sorted_tags: &SortedTags<S>,
    value: u64,
    hashmap: &DashMap<AggregatorEntryKey<S>, V, S>,
    record_fn: impl FnOnce(&V, u64) -> Result<(), String>,
    new_fn: impl FnOnce() -> Option<V>,
) where
    S: BuildHasher + Clone,
{
    let hash = combine_metric_tags_hash(hashmap.hasher(), metric.as_ref(), sorted_tags.tags_hash());
    let lookup_key = LookupKeySorted {
        metric,
        sorted_tags,
        hash,
    };

    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(lookup_key.hash as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };

    {
        let search_result = shard_lock
            .read()
            .find(lookup_key.hash, |(k, _)| lookup_key.compare(k));
        if let Some(bucket) = search_result {
            let x = unsafe { bucket.as_ref() }.1.get();
            if let Err(err) = record_fn(x, value) {
                error!("Fail to record: {err}");
            }
            return;
        }
    }

    let mut guard = shard_lock.write();
    let search_result = guard.find_or_find_insert_slot(
        lookup_key.hash,
        |(k, _)| lookup_key.compare(k),
        |(k, _)| k.hash,
    );

    match search_result {
        Ok(bucket) => {
            if let Err(err) = record_fn(unsafe { bucket.as_ref() }.1.get(), value) {
                error!("Fail to record: {err}");
            }
        }
        Err(insert_slot) => {
            if let Some(v) = new_fn() {
                if let Err(err) = record_fn(&v, value) {
                    error!("Fail to record: {err}");
                }
                let agg_key = lookup_key.into_key();
                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
            }
        }
    }
}

fn add_or_insert_entry_read_first_prepared<V, S>(
    prepared: &PreparedMetric<S>,
    value: u64,
    hashmap: &DashMap<AggregatorEntryKey<S>, V, S>,
    record_fn: impl FnOnce(&V, u64) -> Result<(), String>,
    new_fn: impl FnOnce() -> Option<V>,
) where
    S: BuildHasher + Clone,
{
    let entry_id = prepared.prepared_id();
    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(prepared.hash() as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };

    {
        let read_guard = shard_lock.read();
        let search_result = read_guard.find(prepared.hash(), |(k, _)| k.id == entry_id);
        if let Some(bucket) = search_result {
            let x = unsafe { bucket.as_ref() }.1.get();
            if let Err(err) = record_fn(x, value) {
                error!("Fail to record: {err}");
            }
            return;
        }

        let search_by_signature = read_guard.find(prepared.hash(), |(k, _)| {
            match_prepared_signature(k, prepared)
        });
        if let Some(bucket) = search_by_signature {
            let (_, val) = unsafe { bucket.as_ref() };
            if let Err(err) = record_fn(val.get(), value) {
                error!("Fail to record: {err}");
            }
            return;
        }
        drop(read_guard);
    }

    let mut guard = shard_lock.write();
    let search_result =
        guard.find_or_find_insert_slot(prepared.hash(), |(k, _)| k.id == entry_id, |(k, _)| k.hash);

    match search_result {
        Ok(bucket) => {
            if let Err(err) = record_fn(unsafe { bucket.as_ref() }.1.get(), value) {
                error!("Fail to record: {err}");
            }
        }
        Err(insert_slot) => {
            if let Some(bucket) = guard.find(prepared.hash(), |(k, _)| {
                match_prepared_signature(k, prepared)
            }) {
                let (_, val) = unsafe { bucket.as_ref() };
                if let Err(err) = record_fn(val.get(), value) {
                    error!("Fail to record: {err}");
                }
                return;
            }
            if let Some(v) = new_fn() {
                if let Err(err) = record_fn(&v, value) {
                    error!("Fail to record: {err}");
                }
                let agg_key = to_agg_entry_key(prepared);
                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
                drop(guard);
            }
        }
    }
}

fn build_lookup_key<'a, V, S>(
    metric: RylvStr<'a>,
    tags: &'a [RylvStr<'a>],
    hashmap: &DashMap<AggregatorEntryKey<S>, V, impl BuildHasher + Clone>,
) -> LookupKey<'a>
where
    S: BuildHasher + Clone,
{
    let hasher_builder = hashmap.hasher();
    let tags_hash = hash_tags(hasher_builder, tags);
    let hash = combine_metric_tags_hash(hasher_builder, metric.as_ref(), tags_hash);

    LookupKey {
        metric,
        tags,
        tags_hash,
        hash,
    }
}

fn match_prepared_signature<S: BuildHasher + Clone>(
    key: &AggregatorEntryKey<S>,
    prepared: &PreparedMetric<S>,
) -> bool {
    if key.fingerprint != prepared.fingerprint() {
        return false;
    }
    key.metric.as_ref() == prepared.metric().as_ref() && &key.tags == prepared.tags()
}

pub fn remove_from_map<V, SH, S>(
    map: &DashMap<AggregatorEntryKey<S>, V, SH>,
    key: &RemoveKey,
    mut on_removed: impl FnMut(V),
) where
    S: BuildHasher + Clone,
    SH: BuildHasher + Clone,
{
    #[allow(clippy::cast_possible_truncation)]
    let shard = map.determine_shard(key.hash as usize);
    let shard_lock = unsafe { map.shards().get_unchecked(shard) };
    let mut guard = shard_lock.write();
    if let Some(bucket) = guard.find(key.hash, |(k, _v)| k.id == key.id) {
        let entry = unsafe { guard.remove(bucket) };
        on_removed(entry.0 .1.into_inner());
    }
}

impl<'a, S> Frames<'a, S>
where
    S: BuildHasher + Clone,
{
    fn emit_count_metric(&mut self) -> Option<MetricFrameRef<'a>> {
        if let Some(iter) = self.count_iter.as_mut() {
            for entry in iter.by_ref() {
                let value = entry.value().load(Ordering::SeqCst);
                if value == 0 {
                    self.keys_to_remove.push(entry.key().remove_key());
                    continue;
                }

                let key = entry.key();
                // SAFETY: key metric/tags are stored in `Cow<'static, str>`. Entries with
                // value > 0 are not removed in this drain cycle, so references remain valid.
                let (metric, tags) = unsafe {
                    (
                        std::mem::transmute::<&str, &'a str>(key.metric.as_ref()),
                        std::mem::transmute::<&str, &'a str>(key.tags.joined_tags()),
                    )
                };
                entry.value().store(0, Ordering::SeqCst);
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

        self.count_iter = None;
        for key in &self.keys_to_remove {
            remove_from_map(self.count, key, |_| ());
        }
        self.keys_to_remove.clear();
        self.stage = DrainStage::Gauge;
        None
    }

    fn emit_gauge_metric(&mut self) -> Option<MetricFrameRef<'a>> {
        if let Some(iter) = self.gauge_iter.as_mut() {
            for entry in iter.by_ref() {
                let count = entry.count.load(Ordering::SeqCst);
                if count == 0 {
                    self.keys_to_remove.push(entry.key().remove_key());
                    continue;
                }

                let value = entry.sum.load(Ordering::SeqCst) / count;
                let key = entry.key();
                // SAFETY: key metric/tags are stored in `Cow<'static, str>`. Entries with
                // count > 0 are not removed in this drain cycle, so references remain valid.
                let (metric, tags) = unsafe {
                    (
                        std::mem::transmute::<&str, &'a str>(key.metric.as_ref()),
                        std::mem::transmute::<&str, &'a str>(key.tags.joined_tags()),
                    )
                };
                entry.sum.store(0, Ordering::SeqCst);
                entry.count.store(0, Ordering::SeqCst);
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

        self.gauge_iter = None;
        for key in &self.keys_to_remove {
            remove_from_map(self.gauge, key, |_k| ());
        }
        self.keys_to_remove.clear();
        self.stage = DrainStage::Histogram;
        None
    }

    fn load_next_histogram(&mut self) -> bool {
        if let Some(iter) = self.histogram_iter.as_mut() {
            for histogram_entry in iter.by_ref() {
                if histogram_entry.value().histogram.is_empty() {
                    self.keys_to_remove.push(histogram_entry.key().remove_key());
                    continue;
                }

                let key = histogram_entry.key();

                // SAFETY: `AggregatorEntryKey` stores owned `'static` metric/tag data.
                // During drain we borrow those strings for `'a`, where `'a` is bounded by
                // the lifetime of `SharedDrain`. Non-empty histogram entries are not removed
                // until the drain completes, and the backing aggregator itself is owned by
                // `SharedDrain`, so the borrowed strings remain valid for the entire iterator.
                // The Miri shared-drain test exercises this invariant by reading frame fields
                // across iteration before the drain is dropped.
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
            let entry = pending.entry.value_mut();
            let percentile_count = entry.percentiles.len();
            let frame = match pending.step {
                0 => {
                    pending.step += 1;
                    if entry.emits(HistogramBaseMetric::Count) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".count"),
                            tags: pending.tags,
                            value: entry.histogram.len(),
                            kind: MetricKind::Count,
                        })
                    } else {
                        None
                    }
                }
                1 => {
                    pending.step += 1;
                    if entry.emits(HistogramBaseMetric::Min) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".min"),
                            tags: pending.tags,
                            value: entry.min,
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                2 => {
                    pending.step += 1;
                    if entry.emits(HistogramBaseMetric::Avg) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".avg"),
                            tags: pending.tags,
                            value: entry.histogram.value_at_quantile(0.50),
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                index if index < 3 + percentile_count => {
                    pending.step += 1;
                    let percentile_index = index - 3;
                    let percentile = entry.percentiles[percentile_index];
                    Some(MetricFrameRef {
                        prefix: self.prefix,
                        metric: pending.metric,
                        suffix: MetricSuffix::Percentile(percentile),
                        tags: pending.tags,
                        value: entry.histogram.value_at_quantile(percentile),
                        kind: MetricKind::Gauge,
                    })
                }
                index if index == 3 + percentile_count => {
                    pending.step += 1;
                    if entry.emits(HistogramBaseMetric::Max) {
                        Some(MetricFrameRef {
                            prefix: self.prefix,
                            metric: pending.metric,
                            suffix: MetricSuffix::Static(".max"),
                            tags: pending.tags,
                            value: entry.max,
                            kind: MetricKind::Gauge,
                        })
                    } else {
                        None
                    }
                }
                _ => {
                    entry.reset();
                    return None;
                }
            };

            if let Some(frame) = frame {
                self.pending_histogram = Some(pending);
                return Some(frame);
            }
        }
    }

    /// Returns next drained metric frame.
    pub fn next_frame(&mut self) -> Option<MetricFrameRef<'a>> {
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
                        self.histogram_iter = None;
                        for key in &self.keys_to_remove {
                            remove_from_map(self.histogram, key, |v: HistogramWrapper| {
                                let index = v.pool_id;
                                unsafe { self.pool_histograms.get_unchecked(index) }.push(v);
                            });
                        }
                        self.keys_to_remove.clear();
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

pub fn drain_aggregator_frames<'a, S>(
    aggregator: &'a Aggregator<S>,
    prefix: &'a str,
) -> Frames<'a, S>
where
    S: BuildHasher + Clone,
{
    Frames {
        prefix,
        stage: DrainStage::Count,
        count_iter: Some(aggregator.count.iter()),
        gauge_iter: Some(aggregator.gauge.iter()),
        histogram_iter: Some(aggregator.histograms.iter_mut()),
        count: &aggregator.count,
        gauge: &aggregator.gauge,
        histogram: &aggregator.histograms,
        pool_histograms: &aggregator.pool_histograms,
        keys_to_remove: Vec::new(),
        pending_histogram: None,
    }
}

impl<S> MetricCollectorTrait for SharedCollector<S>
where
    S: BuildHasher + Clone,
{
    type Hasher = S;
    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        let mut_tags = tags.as_mut();
        let aggregator = self.current_aggregator.load();
        record_histogram_in_aggregator(
            &aggregator,
            &self.histogram_configs,
            &self.default_histogram_config,
            metric,
            value,
            mut_tags,
        );
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
        let mut_tags = tags.as_mut();
        let aggregator = self.current_aggregator.load();
        record_count_add_in_aggregator(&aggregator, metric, value, mut_tags);
    }

    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        let mut_tags = tags.as_mut();
        let aggregator = self.current_aggregator.load();
        record_gauge_in_aggregator(&aggregator, metric, value, mut_tags);
    }

    fn histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let aggregator = self.current_aggregator.load();
        record_histogram_in_aggregator_sorted(
            &aggregator,
            &self.histogram_configs,
            &self.default_histogram_config,
            metric,
            value,
            tags,
        );
    }

    fn count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let aggregator = self.current_aggregator.load();
        record_count_add_in_aggregator_sorted(&aggregator, metric, value, tags);
    }

    fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<S>) {
        let aggregator = self.current_aggregator.load();
        record_gauge_in_aggregator_sorted(&aggregator, metric, value, tags);
    }

    fn prepare_metric(&self, metric: RylvStr<'_>, tags: SortedTags<S>) -> PreparedMetric<S> {
        let metric = crate::dogstats::sorted_tags::to_static_metric(metric);
        let hash =
            combine_metric_tags_hash(&self.hasher_builder, metric.as_ref(), tags.tags_hash());
        PreparedMetric::new(metric, tags, hash)
    }

    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<S> {
        SortedTags::new(tags, &self.hasher_builder)
    }

    fn histogram_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let aggregator = self.current_aggregator.load();
        record_histogram_in_aggregator_prepared(
            &aggregator,
            &self.histogram_configs,
            &self.default_histogram_config,
            prepared,
            value,
        );
    }

    fn count_add_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let aggregator = self.current_aggregator.load();
        record_count_add_in_aggregator_prepared(&aggregator, prepared, value);
    }

    fn gauge_prepared(&self, prepared: &PreparedMetric<S>, value: u64) {
        let aggregator = self.current_aggregator.load();
        record_gauge_in_aggregator_prepared(&aggregator, prepared, value);
    }
}

impl<S> MetricCollectorTrait for &SharedCollector<S>
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

    fn prepare_metric(
        &self,
        metric: RylvStr<'_>,
        tags: SortedTags<Self::Hasher>,
    ) -> PreparedMetric<Self::Hasher> {
        (*self).prepare_metric(metric, tags)
    }

    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<Self::Hasher> {
        (*self).prepare_sorted_tags(tags)
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

impl<S> DrainMetricCollectorTrait for &SharedCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Drain<'a>
        = SharedDrain<'a, S>
    where
        Self: 'a;

    fn try_begin_drain(&self) -> Option<Self::Drain<'_>> {
        (*self).begin_drain()
    }
}

impl<S> DrainMetricCollectorTrait for SharedCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    type Drain<'a>
        = SharedDrain<'a, S>
    where
        Self: 'a;

    fn try_begin_drain(&self) -> Option<Self::Drain<'_>> {
        self.begin_drain()
    }
}

pub fn record_histogram_in_aggregator<S>(
    aggregator: &Aggregator<S>,
    histogram_configs: &HashMap<String, ResolvedHistogramConfig, S>,
    default_histogram_config: &ResolvedHistogramConfig,
    metric: RylvStr<'_>,
    value: u64,
    mut_tags: &mut [RylvStr<'_>],
) where
    S: BuildHasher + Clone,
{
    mut_tags.sort_unstable_by(|a, b| {
        if a == b {
            CmpOrdering::Equal
        } else {
            a.as_ref().cmp(b.as_ref())
        }
    });

    let hashmap = &aggregator.histograms;
    let lookup_key = build_lookup_key(metric, mut_tags, hashmap);

    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(lookup_key.hash as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };
    let mut guard = shard_lock.write();
    let search_result = guard.find_or_find_insert_slot(
        lookup_key.hash,
        |(k, _)| lookup_key.compare(k),
        |(k, _)| k.hash,
    );

    match search_result {
        Ok(bucket) => {
            if let Err(err) = unsafe { bucket.as_mut() }
                .1
                .get_mut()
                .record(value)
                .map_err(|err| err.to_string())
            {
                error!("Fail to record: {err}");
            }
        }
        Err(insert_slot) => {
            let histogram_config = histogram_configs
                .get(lookup_key.metric.as_ref())
                .unwrap_or(default_histogram_config);
            if let Some(mut v) =
                aggregator.get_histogram(histogram_config.pool_id(), histogram_config)
            {
                if let Err(err) = v.record(value).map_err(|err| err.to_string()) {
                    error!("Fail to record: {err}");
                }

                let agg_key = lookup_key.into_key();
                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
            }
        }
    }
}

pub fn record_histogram_in_aggregator_sorted<S>(
    aggregator: &Aggregator<S>,
    histogram_configs: &HashMap<String, ResolvedHistogramConfig, S>,
    default_histogram_config: &ResolvedHistogramConfig,
    metric: RylvStr<'_>,
    value: u64,
    sorted_tags: &SortedTags<S>,
) where
    S: BuildHasher + Clone,
{
    let hashmap = &aggregator.histograms;
    let hash = combine_metric_tags_hash(hashmap.hasher(), metric.as_ref(), sorted_tags.tags_hash());
    let lookup_key = LookupKeySorted {
        metric,
        sorted_tags,
        hash,
    };

    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(lookup_key.hash as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };
    let mut guard = shard_lock.write();
    let search_result = guard.find_or_find_insert_slot(
        lookup_key.hash,
        |(k, _)| lookup_key.compare(k),
        |(k, _)| k.hash,
    );

    match search_result {
        Ok(bucket) => {
            if let Err(err) = unsafe { bucket.as_mut() }
                .1
                .get_mut()
                .record(value)
                .map_err(|err| err.to_string())
            {
                error!("Fail to record: {err}");
            }
        }
        Err(insert_slot) => {
            let histogram_config = histogram_configs
                .get(lookup_key.metric.as_ref())
                .unwrap_or(default_histogram_config);
            if let Some(mut v) =
                aggregator.get_histogram(histogram_config.pool_id(), histogram_config)
            {
                if let Err(err) = v.record(value).map_err(|err| err.to_string()) {
                    error!("Fail to record: {err}");
                }

                let agg_key = lookup_key.into_key();
                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
            }
        }
    }
}

pub fn record_histogram_in_aggregator_prepared<S>(
    aggregator: &Aggregator<S>,
    histogram_configs: &HashMap<String, ResolvedHistogramConfig, S>,
    default_histogram_config: &ResolvedHistogramConfig,
    prepared: &PreparedMetric<S>,
    value: u64,
) where
    S: BuildHasher + Clone,
{
    let hashmap = &aggregator.histograms;
    let entry_id = prepared.prepared_id();
    #[allow(clippy::cast_possible_truncation)]
    let shard = hashmap.determine_shard(prepared.hash() as usize);
    let shard_lock = unsafe { hashmap.shards().get_unchecked(shard) };
    let mut guard = shard_lock.write();
    let search_result =
        guard.find_or_find_insert_slot(prepared.hash(), |(k, _)| k.id == entry_id, |(k, _)| k.hash);

    match search_result {
        Ok(bucket) => {
            if let Err(err) = unsafe { bucket.as_mut() }
                .1
                .get_mut()
                .record(value)
                .map_err(|err| err.to_string())
            {
                error!("Fail to record: {err}");
            }
        }
        Err(insert_slot) => {
            if let Some(bucket) = guard.find(prepared.hash(), |(k, _)| {
                match_prepared_signature(k, prepared)
            }) {
                let (_, hist) = unsafe { bucket.as_mut() };
                if let Err(err) = hist.get_mut().record(value).map_err(|err| err.to_string()) {
                    error!("Fail to record: {err}");
                }
                return;
            }

            let histogram_config = histogram_configs
                .get(prepared.metric().as_ref())
                .unwrap_or(default_histogram_config);
            if let Some(mut v) =
                aggregator.get_histogram(histogram_config.pool_id(), histogram_config)
            {
                if let Err(err) = v.record(value).map_err(|err| err.to_string()) {
                    error!("Fail to record: {err}");
                }
                let agg_key = to_agg_entry_key(prepared);
                unsafe {
                    guard.insert_in_slot(agg_key.hash, insert_slot, (agg_key, SharedValue::new(v)));
                }
                drop(guard);
            }
        }
    }
}

pub fn record_count_add_in_aggregator<S>(
    aggregator: &Aggregator<S>,
    metric: RylvStr<'_>,
    value: u64,
    mut_tags: &mut [RylvStr<'_>],
) where
    S: BuildHasher + Clone,
{
    mut_tags.sort_unstable();
    add_or_insert_entry_read_first(
        metric,
        mut_tags,
        value,
        &aggregator.count,
        |v, value| {
            v.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || Some(AtomicU64::new(0)),
    );
}

pub fn record_count_add_in_aggregator_sorted<S>(
    aggregator: &Aggregator<S>,
    metric: RylvStr<'_>,
    value: u64,
    sorted_tags: &SortedTags<S>,
) where
    S: BuildHasher + Clone,
{
    add_or_insert_entry_read_first_sorted(
        metric,
        sorted_tags,
        value,
        &aggregator.count,
        |v, value| {
            v.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || Some(AtomicU64::new(0)),
    );
}

pub fn record_count_add_in_aggregator_prepared<S>(
    aggregator: &Aggregator<S>,
    prepared: &PreparedMetric<S>,
    value: u64,
) where
    S: BuildHasher + Clone,
{
    add_or_insert_entry_read_first_prepared(
        prepared,
        value,
        &aggregator.count,
        |v, value| {
            v.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || Some(AtomicU64::new(0)),
    );
}

pub fn record_gauge_in_aggregator<S>(
    aggregator: &Aggregator<S>,
    metric: RylvStr<'_>,
    value: u64,
    mut_tags: &mut [RylvStr<'_>],
) where
    S: BuildHasher + Clone,
{
    mut_tags.sort_unstable();
    add_or_insert_entry_read_first(
        metric,
        mut_tags,
        value,
        &aggregator.gauge,
        |v, value| {
            v.count.fetch_add(1, Ordering::Relaxed);
            v.sum.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || {
            Some(GaugeState {
                count: AtomicU64::new(0),
                sum: AtomicU64::new(0),
            })
        },
    );
}

pub fn record_gauge_in_aggregator_sorted<S>(
    aggregator: &Aggregator<S>,
    metric: RylvStr<'_>,
    value: u64,
    sorted_tags: &SortedTags<S>,
) where
    S: BuildHasher + Clone,
{
    add_or_insert_entry_read_first_sorted(
        metric,
        sorted_tags,
        value,
        &aggregator.gauge,
        |v, value| {
            v.count.fetch_add(1, Ordering::Relaxed);
            v.sum.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || {
            Some(GaugeState {
                count: AtomicU64::new(0),
                sum: AtomicU64::new(0),
            })
        },
    );
}

pub fn record_gauge_in_aggregator_prepared<S>(
    aggregator: &Aggregator<S>,
    prepared: &PreparedMetric<S>,
    value: u64,
) where
    S: BuildHasher + Clone,
{
    add_or_insert_entry_read_first_prepared(
        prepared,
        value,
        &aggregator.gauge,
        |v, value| {
            v.count.fetch_add(1, Ordering::Relaxed);
            v.sum.fetch_add(value, Ordering::Relaxed);
            Ok(())
        },
        || {
            Some(GaugeState {
                count: AtomicU64::new(0),
                sum: AtomicU64::new(0),
            })
        },
    );
}

#[cfg(test)]
mod tests {
    use super::{
        drain_aggregator_frames, record_count_add_in_aggregator,
        record_count_add_in_aggregator_prepared, record_count_add_in_aggregator_sorted,
        record_gauge_in_aggregator, record_gauge_in_aggregator_prepared,
        record_gauge_in_aggregator_sorted, record_histogram_in_aggregator,
        record_histogram_in_aggregator_prepared, record_histogram_in_aggregator_sorted,
        remove_from_map, SharedCollector, SharedCollectorOptions,
    };
    use crate::dogstats::aggregator::Aggregator;
    use crate::dogstats::collector::{DrainMetricCollectorTrait, MetricKind, MetricSuffix};
    use crate::dogstats::histogram_config::{resolve_histogram_configs, HistogramConfig};
    use crate::{MetricCollectorTrait, RylvStr};
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

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

    fn drain_to_lines<'a>(
        drain: impl Iterator<Item = crate::dogstats::collector::MetricFrameRef<'a>>,
    ) -> Vec<String> {
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

    fn frames_to_lines<S>(mut frames: super::Frames<'_, S>) -> Vec<String>
    where
        S: std::hash::BuildHasher + Clone,
    {
        let mut lines = Vec::new();
        while let Some(frame) = frames.next_frame() {
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

    fn drain_metrics_now<S>(collector: &SharedCollector<S>) -> Vec<String>
    where
        S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
    {
        for _ in 0..8 {
            if let Some(drain) = collector.try_begin_drain() {
                return drain_to_lines(drain);
            }
        }
        panic!("unable to acquire drain ownership");
    }

    fn assert_reference_lines(lines: &[String]) {
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

    fn record_all_helper_variants(collector: &SharedCollector, aggregator: &Aggregator) {
        let sorted = collector
            .prepare_sorted_tags([RylvStr::from_static("b:2"), RylvStr::from_static("a:1")]);
        let prepared_count_a =
            collector.prepare_metric(RylvStr::from_static("requests_prepared"), sorted.clone());
        let prepared_count_b =
            collector.prepare_metric(RylvStr::from_static("requests_prepared"), sorted.clone());
        let prepared_gauge_a =
            collector.prepare_metric(RylvStr::from_static("load_prepared"), sorted.clone());
        let prepared_gauge_b =
            collector.prepare_metric(RylvStr::from_static("load_prepared"), sorted.clone());
        let prepared_hist_a =
            collector.prepare_metric(RylvStr::from_static("latency_prepared"), sorted.clone());
        let prepared_hist_b =
            collector.prepare_metric(RylvStr::from_static("latency_prepared"), sorted);

        record_count_add_in_aggregator(
            aggregator,
            RylvStr::from_static("requests"),
            2,
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        record_count_add_in_aggregator_sorted(
            aggregator,
            RylvStr::from_static("requests_sorted"),
            3,
            prepared_count_a.tags(),
        );
        record_count_add_in_aggregator_prepared(aggregator, &prepared_count_a, 4);
        record_count_add_in_aggregator_prepared(aggregator, &prepared_count_b, 5);

        record_gauge_in_aggregator(
            aggregator,
            RylvStr::from_static("load"),
            10,
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        record_gauge_in_aggregator_sorted(
            aggregator,
            RylvStr::from_static("load_sorted"),
            20,
            prepared_gauge_a.tags(),
        );
        record_gauge_in_aggregator_prepared(aggregator, &prepared_gauge_a, 30);
        record_gauge_in_aggregator_prepared(aggregator, &prepared_gauge_b, 50);

        record_histogram_in_aggregator(
            aggregator,
            &collector.histogram_configs,
            &collector.default_histogram_config,
            RylvStr::from_static("latency"),
            40,
            &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
        );
        record_histogram_in_aggregator_sorted(
            aggregator,
            &collector.histogram_configs,
            &collector.default_histogram_config,
            RylvStr::from_static("latency_sorted"),
            50,
            prepared_hist_a.tags(),
        );
        record_histogram_in_aggregator_prepared(
            aggregator,
            &collector.histogram_configs,
            &collector.default_histogram_config,
            &prepared_hist_a,
            60,
        );
        record_histogram_in_aggregator_prepared(
            aggregator,
            &collector.histogram_configs,
            &collector.default_histogram_config,
            &prepared_hist_b,
            70,
        );
    }

    #[test]
    fn shared_collector_drains_sorted_and_prepared_metrics() {
        let collector = SharedCollector::new(SharedCollectorOptions {
            stats_prefix: "sans.".to_string(),
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
                "sans.latency.95percentile:60|g|#a:1,b:2\n".to_string(),
                "sans.latency.99percentile:60|g|#a:1,b:2\n".to_string(),
                "sans.latency.avg:40|g|#a:1,b:2\n".to_string(),
                "sans.latency.count:2|c|#a:1,b:2\n".to_string(),
                "sans.latency.max:60|g|#a:1,b:2\n".to_string(),
                "sans.latency.min:40|g|#a:1,b:2\n".to_string(),
                "sans.load:15|g|#a:1,b:2\n".to_string(),
                "sans.requests:5|c|#a:1,b:2\n".to_string(),
            ]
        );
        assert!(drain_metrics_now(&collector).is_empty());
    }

    #[test]
    fn shared_try_begin_drain_returns_none_while_previous_arc_is_held() {
        let collector = SharedCollector::new(SharedCollectorOptions::default());
        collector.count(
            RylvStr::from_static("requests"),
            &mut [RylvStr::from_static("scope:test")],
        );

        let held = collector.current_aggregator.load_full();
        assert!(collector.try_begin_drain().is_none());
        drop(held);

        assert_eq!(
            drain_metrics_now(&collector),
            vec!["requests:1|c|#scope:test\n".to_string()]
        );
    }

    #[test]
    fn shared_reference_trait_impls_cover_regular_paths() {
        let collector = SharedCollector::new(SharedCollectorOptions {
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

        let drain =
            <&SharedCollector as DrainMetricCollectorTrait>::try_begin_drain(&collector_ref)
                .unwrap();
        assert_reference_lines(&drain_to_lines(drain));
    }

    #[test]
    fn raw_aggregator_record_helpers_cover_regular_sorted_and_prepared_paths() {
        let collector = SharedCollector::new(SharedCollectorOptions::default());
        let aggregator =
            Aggregator::with_hasher_builder(&collector.hasher_builder, collector.pool_count);

        record_all_helper_variants(&collector, &aggregator);

        let lines = frames_to_lines(drain_aggregator_frames(&aggregator, "agg."));
        assert!(lines.contains(&"agg.requests:2|c|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.requests_sorted:3|c|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.requests_prepared:9|c|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.load:10|g|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.load_sorted:20|g|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.load_prepared:40|g|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.latency.count:1|c|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.latency_sorted.count:1|c|#a:1,b:2\n".to_string()));
        assert!(lines.contains(&"agg.latency_prepared.count:2|c|#a:1,b:2\n".to_string()));
    }

    #[test]
    fn drain_frames_remove_empty_entries_and_recycle_histograms() {
        let hasher = crate::DefaultMetricHasher::new();
        let resolved = resolve_histogram_configs(
            HistogramConfig::default(),
            HashMap::with_hasher(hasher.clone()),
            &hasher,
        );
        let aggregator = Aggregator::with_hasher_builder(&hasher, resolved.pool_count);
        let empty_configs = HashMap::with_hasher(hasher);

        record_count_add_in_aggregator(
            &aggregator,
            RylvStr::from_static("to_remove"),
            1,
            &mut [RylvStr::from_static("a:1")],
        );
        let remove_key = aggregator
            .count
            .iter()
            .find(|entry| entry.key().metric.as_ref() == "to_remove")
            .unwrap()
            .key()
            .remove_key();
        remove_from_map(&aggregator.count, &remove_key, |_| ());
        assert!(aggregator.count.is_empty());

        record_count_add_in_aggregator(
            &aggregator,
            RylvStr::from_static("requests"),
            1,
            &mut [RylvStr::from_static("a:1")],
        );
        aggregator
            .count
            .iter()
            .find(|entry| entry.key().metric.as_ref() == "requests")
            .unwrap()
            .value()
            .store(0, Ordering::SeqCst);

        record_gauge_in_aggregator(
            &aggregator,
            RylvStr::from_static("load"),
            10,
            &mut [RylvStr::from_static("a:1")],
        );
        let gauge = aggregator.gauge.iter().next().unwrap();
        gauge.sum.store(0, Ordering::SeqCst);
        gauge.count.store(0, Ordering::SeqCst);
        drop(gauge);

        record_histogram_in_aggregator(
            &aggregator,
            &empty_configs,
            &resolved.default_histogram_config,
            RylvStr::from_static("latency"),
            10,
            &mut [RylvStr::from_static("a:1")],
        );
        aggregator
            .histograms
            .iter_mut()
            .find(|entry| entry.key().metric.as_ref() == "latency")
            .unwrap()
            .value_mut()
            .reset();

        let mut frames = drain_aggregator_frames(&aggregator, "");
        assert!(frames.next_frame().is_none());
        drop(frames);

        assert!(aggregator.count.is_empty());
        assert!(aggregator.gauge.is_empty());
        assert!(aggregator.histograms.is_empty());
        assert!(
            aggregator.pool_histograms[resolved.default_histogram_config.pool_id()]
                .pop()
                .is_some()
        );
    }
}
