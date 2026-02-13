use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};
use std::{mem, net::SocketAddr, time::Duration};

use super::job::initialize_job;
use crate::dogstats::aggregator::{AggregatorEntryKey, LookupKey, SigFig, DEFAULT_SIG_FIG};
use crate::dogstats::writer::StatsWriterTrait;
use crate::dogstats::{Aggregator, GaugeState, RylvStr};
use crate::{DefaultMetricHasher, MetricResult};
use arc_swap::ArcSwap;
use crossbeam::channel::{unbounded, Sender};
use dashmap::{DashMap, SharedValue};
use tracing::error;

/// Trait defining the interface for metric collection.
///
/// Implementations of this trait can record histograms, counters, and gauges
/// with associated tags.
pub trait MetricCollectorTrait {
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

    /// Shuts down the collector, flushing any pending metrics.
    fn shutdown(self);
}

/// Configuration for histogram precision.
///
/// Controls the number of significant figures used when recording
/// histogram values, affecting both precision and memory usage.
///
/// # Example
///
/// ```ignore
/// use rylv_metrics::collector::HistogramConfig;
/// let config = HistogramConfig::new(SigFig::new(2).unwrap());
/// ```
#[derive(Debug, Clone, Copy)]
pub struct HistogramConfig {
    sig_fig: SigFig,
    // TODO: add bounds configs
}

impl HistogramConfig {
    /// Creates a new histogram configuration with the given significant figures.
    #[must_use]
    pub const fn new(sig_fig: SigFig) -> Self {
        Self { sig_fig }
    }
}

/// The main metrics collector that aggregates and sends metrics via UDP.
///
/// `MetricCollector` spawns a background thread that periodically flushes
/// aggregated metrics to a DogStatsD-compatible server. Metrics are automatically
/// flushed when the collector is dropped.
///
/// This type is `Send + Sync` and can be shared across threads via `Arc<MetricCollector>`.
///
/// # Example
///
/// ```no_run
/// use rylv_metrics::{MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr};
/// use rylv_metrics::{histogram, count, count_add, gauge};
/// use std::net::SocketAddr;
/// use std::time::Duration;
///
/// let options = MetricCollectorOptions {
///     max_udp_packet_size: 1432,
///     max_udp_batch_size: 10,
///     flush_interval: Duration::from_secs(10),
///     stats_prefix: "myapp.".to_string(),
///     writer_type: rylv_metrics::DEFAULT_STATS_WRITER_TYPE,
///     histogram_configs: Default::default(),
///     default_sig_fig: rylv_metrics::SigFig::default(),
///     hasher_builder: std::hash::RandomState::new(),
/// };
///
/// let bind_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
/// let dst_addr: SocketAddr = "127.0.0.1:8125".parse().unwrap();
/// let collector = MetricCollector::new(bind_addr, dst_addr, options);
///
/// // Direct API (zero-copy for static strings)
/// collector.histogram(RylvStr::from_static("latency"), 42, &mut [RylvStr::from_static("endpoint:api")]);
/// collector.count(RylvStr::from_static("request.count"), &mut [RylvStr::from_static("endpoint:api")]);
/// collector.count_add(RylvStr::from_static("bytes.sent"), 1024, &mut [RylvStr::from_static("endpoint:api")]);
/// collector.gauge(RylvStr::from_static("connections"), 100, &mut [RylvStr::from_static("pool:main")]);
///
/// // Convenience macros (allocate on first key insertion)
/// histogram!(collector, "latency", 42, "endpoint:api");
/// count!(collector, "request.count", "endpoint:api");
/// count_add!(collector, "bytes.sent", 1024, "endpoint:api");
/// gauge!(collector, "connections", 100, "pool:main");
/// ```
pub struct MetricCollector<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    aggregator: Arc<ArcSwap<Aggregator<S>>>,
    _hasher_builder: S,
    default_sig_fig: SigFig,
    sender: Option<Sender<()>>,
    histogram_configs: std::collections::HashMap<String, HistogramConfig>,
    // only used in cold path
    job_handle: Option<JoinHandle<MetricResult<()>>>,
}

impl std::fmt::Debug for StatsWriterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(target_os = "linux")]
            Self::LinuxBatch => write!(f, "LinuxBatch"),
            Self::Simple => write!(f, "Simple"),
            #[cfg(target_vendor = "apple")]
            Self::AppleBatch => write!(f, "AppleBatch"),
            Self::Custom(_) => write!(f, "Custom(...)"),
        }
    }
}

/// Specifies the UDP writer backend to use for sending metrics.
///
/// Different backends offer varying performance characteristics depending
/// on the platform.
pub enum StatsWriterType {
    /// Uses `sendmmsg` for batch UDP writes. Linux only.
    #[cfg(target_os = "linux")]
    LinuxBatch,
    /// Standard UDP writer using individual `send_to` calls. Works on all platforms.
    Simple,
    /// Uses `sendmsg_x` for batch UDP writes. macOS only.
    #[cfg(target_vendor = "apple")]
    AppleBatch,
    /// User-provided writer implementation.
    Custom(Box<dyn StatsWriterTrait + Send + Sync + 'static>),
}

/// The default writer type (Simple) that works on all platforms.
pub const DEFAULT_STATS_WRITER_TYPE: StatsWriterType = StatsWriterType::Simple;

/// Configuration options for the metric collector.
///
/// Controls UDP packet sizes, flush intervals, and writer backend selection.
#[derive(Debug)]
pub struct MetricCollectorOptions<S = DefaultMetricHasher>
where
    S: BuildHasher + Clone,
{
    // TODO: add support for this metric, if value = 1 -> no aggregation at all -> queue of MetricLines
    // pub max_metrics_per_packet: u16,
    /// Maximum size of a single UDP packet in bytes. Recommended: 1432 for safe MTU.
    pub max_udp_packet_size: u16,
    /// Maximum number of messages to batch in a single `sendmmsg`/`sendmsg_x` call.
    pub max_udp_batch_size: u32,
    /// How often to flush aggregated metrics to the server.
    pub flush_interval: Duration,
    /// Prefix prepended verbatim to all metric names. Include a trailing dot if desired (e.g., `"myapp."` results in `"myapp.metric"`).
    pub stats_prefix: String,
    /// The UDP writer backend to use.
    pub writer_type: StatsWriterType,
    /// Per-metric histogram configuration for custom precision settings.
    pub histogram_configs: std::collections::HashMap<String, HistogramConfig>,
    /// Default histogram significant figures when metric-specific config is absent.
    pub default_sig_fig: SigFig,
    /// Hasher builder used by internal aggregation maps.
    pub hasher_builder: S,
}

impl Default for MetricCollectorOptions<DefaultMetricHasher> {
    fn default() -> Self {
        Self {
            max_udp_packet_size: 1432,
            max_udp_batch_size: 10,
            flush_interval: Duration::from_secs(10),
            stats_prefix: String::new(),
            writer_type: DEFAULT_STATS_WRITER_TYPE,
            histogram_configs: std::collections::HashMap::new(),
            default_sig_fig: DEFAULT_SIG_FIG,
            hasher_builder: DefaultMetricHasher::new(),
        }
    }
}

impl<S> MetricCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Creates a new metric collector that sends aggregated metrics to `dst_addr`.
    ///
    /// Binds a UDP socket to `bind_addr` and spawns a background flush thread.
    #[must_use]
    pub fn new(
        bind_addr: SocketAddr,
        dst_addr: SocketAddr,
        mut options: MetricCollectorOptions<S>,
    ) -> Self {
        let (sender, receiver) = unbounded::<()>();
        let hasher_builder = options.hasher_builder.clone();
        let default_sig_fig = options.default_sig_fig;

        let alloc_aggregator = Arc::new(ArcSwap::new(Arc::new(Aggregator::with_hasher_builder(
            hasher_builder.clone(),
        ))));
        let alloc_clone = alloc_aggregator.clone();
        let mut histogram_configs = std::collections::HashMap::new();
        mem::swap(&mut options.histogram_configs, &mut histogram_configs);
        let job_handle =
            spawn(move || initialize_job(bind_addr, dst_addr, options, &receiver, alloc_clone));

        Self {
            aggregator: alloc_aggregator,
            _hasher_builder: hasher_builder,
            default_sig_fig,
            sender: Some(sender),
            job_handle: Some(job_handle),
            histogram_configs,
        }
    }
}

impl<S> MetricCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn add_or_insert_entry_write<V>(
        &self,
        metric: RylvStr<'_>,
        tags: &[RylvStr<'_>],
        value: u64,
        hashmap: &DashMap<AggregatorEntryKey, V, S>,
        record_fn: impl FnOnce(&mut V, u64) -> Result<(), String>,
        new_fn: impl FnOnce(SigFig) -> Option<V>,
    ) {
        let lookup_key = build_lookup_key(metric, tags, hashmap);

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
                if let Err(err) = record_fn(unsafe { bucket.as_mut() }.1.get_mut(), value) {
                    error!("Fail to record: {err}");
                }
            }
            Err(insert_slot) => {
                let sig_fig = self
                    .histogram_configs
                    .get(lookup_key.metric.as_ref())
                    .map_or(self.default_sig_fig, |config| config.sig_fig);
                if let Some(mut v) = new_fn(sig_fig) {
                    if let Err(err) = record_fn(&mut v, value) {
                        error!("Fail to record: {err}");
                    }

                    let agg_key = lookup_key.into_key();
                    unsafe {
                        guard.insert_in_slot(
                            agg_key.hash,
                            insert_slot,
                            (agg_key, SharedValue::new(v)),
                        );
                    }
                }
            }
        }
    }
}

fn add_or_insert_entry_read_first<V>(
    metric: RylvStr<'_>,
    tags: &[RylvStr<'_>],
    value: u64,
    hashmap: &DashMap<AggregatorEntryKey, V, impl BuildHasher + Clone>,
    record_fn: impl FnOnce(&V, u64) -> Result<(), String>,
    new_fn: impl FnOnce() -> Option<V>,
) {
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

fn build_lookup_key<'a, V>(
    metric: RylvStr<'a>,
    tags: &'a [RylvStr<'a>],
    hashmap: &DashMap<AggregatorEntryKey, V, impl BuildHasher + Clone>,
) -> LookupKey<'a> {
    let mut hasher = hashmap.hasher().build_hasher();
    metric.as_ref().hash(&mut hasher);
    for tag in tags {
        tag.as_ref().hash(&mut hasher);
    }

    LookupKey {
        metric,
        tags,
        hash: hasher.finish(),
    }
}
impl<S> MetricCollectorTrait for MetricCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        let mut_tags = tags.as_mut();
        mut_tags.sort_unstable();

        let aggregator = self.aggregator.load();
        let hashmap = &aggregator.histograms;

        self.add_or_insert_entry_write(
            metric,
            mut_tags,
            value,
            hashmap,
            |v, value| v.record(value).map_err(|err| err.to_string()),
            |sig_fig| aggregator.get_histogram(sig_fig),
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
        mut_tags.sort_unstable();

        let aggregator = self.aggregator.load();
        let hashmap = &aggregator.count;

        add_or_insert_entry_read_first(
            metric,
            mut_tags,
            value,
            hashmap,
            |v, value| {
                v.fetch_add(value, Ordering::Relaxed);
                Ok(())
            },
            || Some(AtomicU64::new(0)),
        );
    }

    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, mut tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        let mut_tags = tags.as_mut();
        mut_tags.sort_unstable();

        let aggregator = self.aggregator.load();
        let hashmap = &aggregator.gauge;

        add_or_insert_entry_read_first(
            metric,
            mut_tags,
            value,
            hashmap,
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

    fn shutdown(self) {
        drop(self);
    }
}

impl<S> Drop for MetricCollector<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // Drop the sender to signal the background job to stop.
        // When the sender is dropped, the receiver in the background job
        // will detect the channel closure and initiate shutdown.
        drop(self.sender.take());

        // Wait for the background job to finish gracefully
        if let Some(handle) = self.job_handle.take() {
            let _ = handle.join();
        }
    }
}
