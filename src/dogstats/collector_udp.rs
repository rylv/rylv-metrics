use std::{
    hash::BuildHasher,
    net::SocketAddr,
    sync::Arc,
    thread::{spawn, JoinHandle},
    time::Duration,
};

use crate::{
    dogstats::writer::StatsWriterHolder, MetricCollectorTrait, PreparedMetric, RylvStr, SortedTags,
};

#[cfg(feature = "custom_writer")]
use crate::StatsWriterTrait;

use super::collector::DrainMetricCollectorTrait;
use super::job::initialize_job;
use super::writer::UdpSocketWriter;
use crate::MetricResult;
use crossbeam::channel::{unbounded, Sender};
#[cfg(target_os = "linux")]
use rustix::net::SocketAddrAny;
use std::net::UdpSocket;
use tracing::error;

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
    #[cfg(feature = "custom_writer")]
    Custom(Box<dyn StatsWriterTrait + Send + Sync + 'static>),
}

impl std::fmt::Debug for StatsWriterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(target_os = "linux")]
            Self::LinuxBatch => write!(f, "LinuxBatch"),
            Self::Simple => write!(f, "Simple"),
            #[cfg(target_vendor = "apple")]
            Self::AppleBatch => write!(f, "AppleBatch"),
            #[cfg(feature = "custom_writer")]
            Self::Custom(_) => write!(f, "Custom(...)"),
        }
    }
}

/// The default writer type (Simple) that works on all platforms.
pub const DEFAULT_STATS_WRITER_TYPE: StatsWriterType = StatsWriterType::Simple;

/// Configuration options for the metric collector.
///
/// Controls UDP packet sizes, flush intervals, and writer backend selection.
#[derive(Debug)]
pub struct MetricCollectorOptions {
    /// Maximum size of a single UDP packet in bytes. Recommended: 1432 for safe MTU.
    pub max_udp_packet_size: u16,
    /// Maximum number of messages to batch in a single `sendmmsg`/`sendmsg_x` call.
    pub max_udp_batch_size: u32,
    /// How often to flush aggregated metrics to the server.
    pub flush_interval: Duration,
    /// The UDP writer backend to use.
    pub writer_type: StatsWriterType,
}

impl Default for MetricCollectorOptions {
    fn default() -> Self {
        Self {
            max_udp_packet_size: 1432,
            max_udp_batch_size: 10,
            flush_interval: Duration::from_secs(10),
            writer_type: DEFAULT_STATS_WRITER_TYPE,
        }
    }
}
/// UDP-backed collector that composes an inner drainable metric collector with
/// a background flush/runtime layer.
///
/// Construction validates UDP startup synchronously, so socket bind/setup
/// failures are returned from [`MetricCollector::new`] immediately.
///
/// Delivery remains best-effort:
/// - individual send failures are logged by the worker thread
/// - drop waits for the worker to finish a final drain attempt
/// - worker failures are logged on drop but are not returned to the caller
pub struct MetricCollector<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    inner: Arc<MC>,
    sender: Option<Sender<()>>,
    job_handle: Option<JoinHandle<MetricResult<()>>>,
}

impl<MC> MetricCollector<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Builds a UDP collector around an existing drainable inner collector.
    ///
    /// # Errors
    /// Returns an error if the UDP socket cannot be created or the runtime
    /// worker cannot be started successfully.
    #[cold]
    pub fn new(
        bind_addr: SocketAddr,
        dst_addr: SocketAddr,
        options: MetricCollectorOptions,
        inner: MC,
    ) -> MetricResult<Self> {
        let flush_interval = options.flush_interval;
        let writer = UdpSocketWriter {
            sock: UdpSocket::bind(bind_addr)?,
            destination_addr: dst_addr,
            #[cfg(target_os = "linux")]
            destination: SocketAddrAny::from(dst_addr),
        };
        let writer_type = options.writer_type;
        let max_udp_packet_size = options.max_udp_packet_size;
        let max_udp_batch_size = options.max_udp_batch_size;
        let inner = Arc::new(inner);
        let (sender, receiver) = unbounded::<()>();
        let runtime_inner = Arc::clone(&inner);
        let job_handle = spawn(move || {
            let holder = StatsWriterHolder::new(
                writer,
                writer_type,
                max_udp_packet_size,
                max_udp_batch_size,
            );

            initialize_job(flush_interval, &receiver, runtime_inner, holder)
        });
        Ok(Self {
            inner,
            sender: Some(sender),
            job_handle: Some(job_handle),
        })
    }
}

impl<MC> Drop for MetricCollector<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    #[cold]
    fn drop(&mut self) {
        drop(self.sender.take());

        if let Some(handle) = self.job_handle.take() {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => error!("metric collector worker exited with error: {err}"),
                Err(_) => error!("metric collector worker panicked during shutdown"),
            }
        }
    }
}

impl<MC> MetricCollectorTrait for MetricCollector<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    type Hasher = MC::Hasher;

    fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.inner.histogram(metric, value, tags);
    }

    fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.inner.count(metric, tags);
    }

    fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.inner.count_add(metric, value, tags);
    }

    fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, tags: TT)
    where
        TT: AsMut<[RylvStr<'t>]>,
    {
        self.inner.gauge(metric, value, tags);
    }

    fn histogram_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>) {
        self.inner.histogram_sorted(metric, value, tags);
    }

    fn count_add_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>) {
        self.inner.count_add_sorted(metric, value, tags);
    }

    fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, tags: &SortedTags<Self::Hasher>) {
        self.inner.gauge_sorted(metric, value, tags);
    }

    #[cold]
    fn prepare_sorted_tags<'a>(
        &self,
        tags: impl IntoIterator<Item = RylvStr<'a>>,
    ) -> SortedTags<Self::Hasher> {
        self.inner.prepare_sorted_tags(tags)
    }

    #[cold]
    fn prepare_metric(
        &self,
        metric: RylvStr<'_>,
        tags: SortedTags<Self::Hasher>,
    ) -> PreparedMetric<Self::Hasher> {
        self.inner.prepare_metric(metric, tags)
    }

    fn histogram_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.inner.histogram_prepared(prepared, value);
    }

    fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.inner.count_add_prepared(prepared, value);
    }

    fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
        self.inner.gauge_prepared(prepared, value);
    }
}

#[cfg(test)]
mod tests {
    use super::{MetricCollector, MetricCollectorOptions, StatsWriterType};
    use crate::dogstats::collector::{DrainMetricCollectorTrait, MetricFrameRef};
    use crate::{MetricCollectorTrait, PreparedMetric, RylvStr, SortedTags};
    use crossbeam::channel::unbounded;
    use std::hash::BuildHasher;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[derive(Default)]
    struct FakeInner {
        calls: Mutex<Vec<String>>,
    }

    impl FakeInner {
        fn record(&self, value: String) {
            self.calls.lock().unwrap().push(value);
        }

        fn take_calls(&self) -> Vec<String> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }

    impl MetricCollectorTrait for FakeInner {
        type Hasher = std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

        fn histogram<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, _tags: TT)
        where
            TT: AsMut<[RylvStr<'t>]>,
        {
            self.record(format!("histogram:{}:{value}", metric.as_ref()));
        }

        fn count<'m, 't, TT>(&self, metric: RylvStr<'m>, _tags: TT)
        where
            TT: AsMut<[RylvStr<'t>]>,
        {
            self.record(format!("count:{}", metric.as_ref()));
        }

        fn count_add<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, _tags: TT)
        where
            TT: AsMut<[RylvStr<'t>]>,
        {
            self.record(format!("count_add:{}:{value}", metric.as_ref()));
        }

        fn gauge<'m, 't, TT>(&self, metric: RylvStr<'m>, value: u64, _tags: TT)
        where
            TT: AsMut<[RylvStr<'t>]>,
        {
            self.record(format!("gauge:{}:{value}", metric.as_ref()));
        }

        fn histogram_sorted(
            &self,
            metric: RylvStr<'_>,
            value: u64,
            _tags: &SortedTags<Self::Hasher>,
        ) {
            self.record(format!("histogram_sorted:{}:{value}", metric.as_ref()));
        }

        fn count_add_sorted(
            &self,
            metric: RylvStr<'_>,
            value: u64,
            _tags: &SortedTags<Self::Hasher>,
        ) {
            self.record(format!("count_add_sorted:{}:{value}", metric.as_ref()));
        }

        fn gauge_sorted(&self, metric: RylvStr<'_>, value: u64, _tags: &SortedTags<Self::Hasher>) {
            self.record(format!("gauge_sorted:{}:{value}", metric.as_ref()));
        }

        fn prepare_sorted_tags<'a>(
            &self,
            tags: impl IntoIterator<Item = RylvStr<'a>>,
        ) -> SortedTags<Self::Hasher> {
            SortedTags::new(tags, &std::hash::BuildHasherDefault::default())
        }

        fn prepare_metric(
            &self,
            metric: RylvStr<'_>,
            tags: SortedTags<Self::Hasher>,
        ) -> PreparedMetric<Self::Hasher> {
            let metric = match metric {
                RylvStr::Static(s) => RylvStr::Static(s),
                RylvStr::Borrowed(s) => RylvStr::from(s.to_owned()),
                RylvStr::Owned(s) => RylvStr::Owned(s),
            };
            let mut hasher = <Self::Hasher as Default>::default().build_hasher();
            std::hash::Hash::hash(&metric.as_ref(), &mut hasher);
            std::hash::Hash::hash(&tags.tags_hash(), &mut hasher);
            PreparedMetric::new(metric, tags, std::hash::Hasher::finish(&hasher))
        }

        fn histogram_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
            self.record(format!(
                "histogram_prepared:{}:{value}",
                prepared.metric().as_ref()
            ));
        }

        fn count_add_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
            self.record(format!(
                "count_add_prepared:{}:{value}",
                prepared.metric().as_ref()
            ));
        }

        fn gauge_prepared(&self, prepared: &PreparedMetric<Self::Hasher>, value: u64) {
            self.record(format!(
                "gauge_prepared:{}:{value}",
                prepared.metric().as_ref()
            ));
        }
    }

    impl DrainMetricCollectorTrait for FakeInner {
        type Drain<'a> = std::vec::IntoIter<MetricFrameRef<'a>>;

        fn try_begin_drain(&self) -> Option<Self::Drain<'_>> {
            Some(Vec::new().into_iter())
        }
    }

    fn collector_with_inner(inner: Arc<FakeInner>) -> MetricCollector<FakeInner> {
        let (sender, _receiver) = unbounded();
        MetricCollector {
            inner,
            sender: Some(sender),
            job_handle: Some(thread::spawn(|| Ok(()))),
        }
    }

    #[test]
    fn metric_collector_options_default_values_match_documented_defaults() {
        let options = MetricCollectorOptions::default();

        assert_eq!(options.max_udp_packet_size, 1432);
        assert_eq!(options.max_udp_batch_size, 10);
        assert_eq!(options.flush_interval, Duration::from_secs(10));
        assert!(matches!(options.writer_type, StatsWriterType::Simple));
    }

    #[test]
    fn stats_writer_type_debug_matches_variant_name() {
        assert_eq!(format!("{:?}", StatsWriterType::Simple), "Simple");
    }

    #[test]
    fn metric_collector_trait_methods_forward_to_inner_collector() {
        let inner = Arc::new(FakeInner::default());
        let collector = collector_with_inner(Arc::clone(&inner));
        let sorted = collector
            .prepare_sorted_tags([RylvStr::from_static("b:2"), RylvStr::from_static("a:1")]);
        let prepared = collector.prepare_metric(RylvStr::from_static("prepared"), sorted.clone());

        collector.count(
            RylvStr::from_static("requests"),
            &mut [RylvStr::from_static("tag:test")],
        );
        collector.count_add(
            RylvStr::from_static("requests"),
            3,
            &mut [RylvStr::from_static("tag:test")],
        );
        collector.gauge(
            RylvStr::from_static("load"),
            9,
            &mut [RylvStr::from_static("tag:test")],
        );
        collector.histogram(
            RylvStr::from_static("latency"),
            7,
            &mut [RylvStr::from_static("tag:test")],
        );
        collector.count_add_sorted(RylvStr::from_static("sorted_count"), 2, &sorted);
        collector.gauge_sorted(RylvStr::from_static("sorted_gauge"), 5, &sorted);
        collector.histogram_sorted(RylvStr::from_static("sorted_hist"), 11, &sorted);
        collector.count_add_prepared(&prepared, 4);
        collector.gauge_prepared(&prepared, 6);
        collector.histogram_prepared(&prepared, 8);

        let calls = inner.take_calls();
        assert_eq!(
            calls,
            vec![
                "count:requests".to_string(),
                "count_add:requests:3".to_string(),
                "gauge:load:9".to_string(),
                "histogram:latency:7".to_string(),
                "count_add_sorted:sorted_count:2".to_string(),
                "gauge_sorted:sorted_gauge:5".to_string(),
                "histogram_sorted:sorted_hist:11".to_string(),
                "count_add_prepared:prepared:4".to_string(),
                "gauge_prepared:prepared:6".to_string(),
                "histogram_prepared:prepared:8".to_string(),
            ]
        );
    }
}
