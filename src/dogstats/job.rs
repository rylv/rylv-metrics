use crate::dogstats::writer::{StatsWriterHolder, StatsWriterTrait, UdpSocketWriter};
use crate::{HashMap, MetricResult};

use super::aggregator::{AggregatorEntryKey, HistogramWrapper, POOL_COUNT};
use super::collector::MetricCollectorOptions;
use super::{Aggregator, GaugeState, MetricType};
use arc_swap::ArcSwap;
use bumpalo::Bump;
use crossbeam::channel::{tick, Receiver};
use crossbeam::queue::SegQueue;
use crossbeam::select;
use itoa::Buffer;
#[cfg(target_os = "linux")]
use rustix::net::SocketAddrAny;
use std::mem::transmute;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

use super::aggregator::RemoveKey;

struct MetricCollectorJob {
    current_aggregator: Arc<ArcSwap<Aggregator>>,
    pending_to_process_aggregator: Option<Arc<Aggregator>>,
    available_aggregator: Option<Aggregator>,

    buffer: Buffer,
    keys: Vec<RemoveKey>,
    bump: Bump,

    stats_writer: StatsWriterHolder,
}
enum SendResult {
    Ok,
    WouldBlock,
}

impl MetricCollectorJob {
    fn send_metrics(&mut self) -> SendResult {
        let mut alloc_agg = if let Some(alloc_agg) = self.pending_to_process_aggregator.take() {
            match Arc::try_unwrap(alloc_agg) {
                Ok(alloc_agg) => alloc_agg,
                Err(alloc_agg) => {
                    self.pending_to_process_aggregator = Some(alloc_agg);
                    return SendResult::WouldBlock;
                }
            }
        } else {
            let agg = self.available_aggregator.take().unwrap_or_default();
            self.pending_to_process_aggregator = Some(self.current_aggregator.swap(Arc::new(agg)));
            return SendResult::WouldBlock;
        };

        self.process_data(&mut alloc_agg);

        self.available_aggregator = Some(alloc_agg);
        SendResult::Ok
    }

    fn process_data(&mut self, aggregator: &mut Aggregator) {
        let buffer = &mut self.buffer;
        let keys_to_remove = &mut self.keys;
        let values = &self.bump;

        // get a guard to stats_writer
        // The guard will be dropped at the end of the function and the drop implementation
        // will reset the stats_writer internal state.
        let mut stats_writer = self.stats_writer.acquire();

        Self::process_count(
            &mut stats_writer,
            buffer,
            keys_to_remove,
            values,
            &mut aggregator.count,
        );
        Self::process_gauge(
            &mut stats_writer,
            buffer,
            keys_to_remove,
            values,
            &mut aggregator.gauge,
        );
        Self::process_histogram(
            &mut stats_writer,
            keys_to_remove,
            buffer,
            values,
            &mut aggregator.histograms,
            &aggregator.pool_histograms,
        );

        if let Err(err) = stats_writer.flush() {
            error!("Error sending metrics: {err}");
        }

        self.bump.reset();
    }

    fn get_value<'a>(value: u64, bump: &'a Bump, buffer: &mut Buffer) -> &'a str {
        let value = buffer.format(value);
        bump.alloc_str(value)
    }

    fn process_histogram<'data, 'bump: 'data, 'w>(
        stats_writer: &'w mut dyn StatsWriterTrait,
        keys_to_remove: &mut Vec<RemoveKey>,
        buffer: &mut Buffer,
        bump: &'bump Bump,
        map: &'data mut HashMap<AggregatorEntryKey, HistogramWrapper>,
        pool_histograms: &[SegQueue<HistogramWrapper>; POOL_COUNT],
    ) {
        let can_use_stack = stats_writer.metric_copied();
        for mut histogram_entry in map.iter_mut() {
            let key = histogram_entry.key();
            let count = histogram_entry.histogram.len();
            if count > 0 {
                let metric_str = key.metric.as_ref();
                // SAFETY: the metric and tags belong to a key that is not removed, so their lifetime is larger than the IoSlice
                let (metric_str, joined_tags) = unsafe {
                    (
                        transmute::<&str, &str>(metric_str),
                        transmute::<&str, &str>(key.tags.joined_tags.as_ref()),
                    )
                };

                let min = histogram_entry.min;
                let p50 = histogram_entry.histogram.value_at_quantile(0.50);
                let p99 = histogram_entry.histogram.value_at_quantile(0.99);
                let max = histogram_entry.max;

                Self::send_metric(
                    stats_writer,
                    &[metric_str, ".count"],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(count)
                    } else {
                        Self::get_value(count, bump, buffer)
                    },
                    MetricType::Count,
                );

                Self::send_metric(
                    stats_writer,
                    &[metric_str, ".min"],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(min)
                    } else {
                        Self::get_value(min, bump, buffer)
                    },
                    MetricType::Gauge,
                );

                Self::send_metric(
                    stats_writer,
                    &[metric_str, ".avg"],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(p50)
                    } else {
                        Self::get_value(p50, bump, buffer)
                    },
                    MetricType::Gauge,
                );

                Self::send_metric(
                    stats_writer,
                    &[metric_str, ".99percentile"],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(p99)
                    } else {
                        Self::get_value(p99, bump, buffer)
                    },
                    MetricType::Gauge,
                );

                Self::send_metric(
                    stats_writer,
                    &[metric_str, ".max"],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(max)
                    } else {
                        Self::get_value(max, bump, buffer)
                    },
                    MetricType::Gauge,
                );

                histogram_entry.reset();
            } else {
                keys_to_remove.push(key.to_key());
            }
        }
        for key in keys_to_remove.iter() {
            Self::remove_from_map(map, key, |v| {
                pool_histograms[v.sig_fig.value() as usize].push(v);
            });
        }
        keys_to_remove.clear();
    }

    fn remove_from_map<V>(
        map: &mut HashMap<AggregatorEntryKey, V>,
        key: &RemoveKey,
        mut f: impl FnMut(V),
    ) {
        #[allow(clippy::cast_possible_truncation)]
        let shard = map.determine_shard(key.hash as usize);
        let shard_lock = unsafe { map.shards().get_unchecked(shard) };

        let mut guard = shard_lock.write();
        if let Some(bucket) = guard.find(key.hash, |(k, _v)| k.id == key.id) {
            let entry = unsafe { guard.remove(bucket) };
            f(entry.0 .1.into_inner());
        }
    }

    fn process_gauge<'data, 'bump: 'data, 'w>(
        stats_writer: &'w mut dyn StatsWriterTrait,
        buffer: &mut Buffer,
        keys_to_remove: &mut Vec<RemoveKey>,
        bump: &'bump Bump,
        map: &'data mut HashMap<AggregatorEntryKey, GaugeState>,
    ) {
        let can_use_stack = stats_writer.metric_copied();
        for entry in map.iter() {
            let key = entry.key();
            let count = entry.count.load(Ordering::SeqCst);
            if count > 0 {
                let value = entry.sum.load(Ordering::SeqCst) / count;
                let metric_str = key.metric.as_ref();
                // SAFETY: the metric and tags belong to a key that is not removed, so their lifetime is larger than the IoSlice
                let (metric_str, joined_tags) = unsafe {
                    (
                        transmute::<&str, &str>(metric_str),
                        transmute::<&str, &str>(key.tags.joined_tags.as_ref()),
                    )
                };
                Self::send_metric(
                    stats_writer,
                    &[metric_str],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(value)
                    } else {
                        Self::get_value(value, bump, buffer)
                    },
                    MetricType::Gauge,
                );
                entry.sum.store(0, Ordering::SeqCst);
                entry.count.store(0, Ordering::SeqCst);
            } else {
                keys_to_remove.push(key.to_key());
            }
        }
        for key in keys_to_remove.iter() {
            Self::remove_from_map(map, key, |_k| ());
        }
        keys_to_remove.clear();
    }

    fn process_count<'data, 'bump: 'data, 'w>(
        stats_writer: &'w mut dyn StatsWriterTrait,
        buffer: &mut Buffer,
        keys_to_remove: &mut Vec<RemoveKey>,
        bump: &'bump Bump,
        map: &'data mut HashMap<AggregatorEntryKey, AtomicU64>,
    ) {
        let can_use_stack = stats_writer.metric_copied();
        for entry in map.iter() {
            let key = entry.key();
            let value = entry.value().load(Ordering::SeqCst);
            if value > 0 {
                let metric_str = key.metric.as_ref();
                // SAFETY: the metric and tags belong to a key that is not removed, so their lifetime is larger than the IoSlice
                let (metric_str, joined_tags) = unsafe {
                    (
                        transmute::<&str, &str>(metric_str),
                        transmute::<&str, &str>(key.tags.joined_tags.as_ref()),
                    )
                };
                Self::send_metric(
                    stats_writer,
                    &[metric_str],
                    joined_tags,
                    if can_use_stack {
                        buffer.format(value)
                    } else {
                        Self::get_value(value, bump, buffer)
                    },
                    MetricType::Count,
                );
                entry.value().store(0, Ordering::SeqCst);
            } else {
                keys_to_remove.push(entry.key().to_key());
            }
        }
        for key in keys_to_remove.iter() {
            Self::remove_from_map(map, key, |_k| ());
        }
        keys_to_remove.clear();
    }

    // TODO: move this to stats writer directly
    fn send_metric<'data>(
        stats_writer: &mut dyn StatsWriterTrait,
        metric: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: MetricType,
    ) {
        let metric_type_str = match metric_type {
            MetricType::Count => "c",
            MetricType::Gauge => "g",
        };

        match stats_writer.write(metric, tags, value, metric_type_str) {
            Ok(()) => {}
            Err(err) => warn!("Error sending metrics. Error {err}"),
        }
    }
}

pub fn initialize_job(
    bind_addr: SocketAddr,
    stats_dst: SocketAddr,
    options: MetricCollectorOptions,
    receiver: &Receiver<()>,
    aggregtor: Arc<ArcSwap<Aggregator>>,
) -> MetricResult<()> {
    let flush_interval = options.flush_interval;
    let writer = UdpSocketWriter {
        sock: UdpSocket::bind(bind_addr)?,
        destination_addr: stats_dst,
        #[cfg(target_os = "linux")]
        destination: SocketAddrAny::from(stats_dst),
    };

    let mut job = MetricCollectorJob {
        stats_writer: StatsWriterHolder::new(
            writer,
            options.writer_type,
            options.stats_prefix.clone(),
            options.max_udp_packet_size,
            options.max_udp_batch_size,
        ),

        current_aggregator: aggregtor,
        // When send_metrics is activated, the current aggregator is moved from current_aggregator
        // is replaced with the available aggregator or with a new one if none is available.
        // After an aggregator is processed, it is moved to available_aggregator.
        // Only 2 aggregator should be created maximum, current and processed one.
        available_aggregator: None,

        // when send_metrics is activated, the current aggregator is moved to this field
        // until no more reference to it is held, when no reference found then the aggregator is processed.
        // This is to avoid concurrent access to the aggregator during processing time.
        pending_to_process_aggregator: None,

        buffer: Buffer::new(),
        keys: Vec::new(),

        // TODO only use this in batch mode apple/linux to hold values
        bump: Bump::with_capacity(20 * 1024),
    };

    let large_tick = tick(flush_interval);
    let shorter_tick = tick(Duration::from_millis(10));

    let mut finish = false;

    loop {
        // wait for time to flush or shutdown signal
        select! {
            // wait timeout
            recv(large_tick) -> _ => (),
            // wait signal
            recv(receiver) -> _ => {
                finish = true;
            },
        }

        loop {
            // try to send metrics, if would block (because the Aggregator is held by many)
            // then wait for shorter tick until the aggregator only is referenced one
            // TODO: use a timeout to avoid waiting for ever in shorter_tick if a bug is introduced
            match job.send_metrics() {
                SendResult::WouldBlock => {
                    if let Err(err) = shorter_tick.recv() {
                        error!("Error awaiting shorter tick: {err}");
                    }
                }
                SendResult::Ok => {
                    if finish {
                        return Ok(());
                    }
                    break;
                }
            }
        }
    }
}
