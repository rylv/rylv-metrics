use crate::dogstats::collector::{DrainMetricCollectorTrait, MetricKind, MetricSuffix};
use crate::dogstats::writer::{StatsWriterHolder, StatsWriterTrait};
use crate::MetricResult;

use bumpalo::Bump;
use crossbeam::channel::{tick, Receiver};
use crossbeam::select;
use itoa::Buffer;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

struct MetricCollectorJob<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    collector: Arc<MC>,

    buffer: Buffer,
    bump: Bump,

    stats_writer: StatsWriterHolder,
}

enum SendResult {
    Ok,
    WouldBlock,
}

impl<MC> MetricCollectorJob<MC>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    fn send_metrics(&mut self) -> SendResult {
        let Some(drain) = self.collector.try_begin_drain() else {
            return SendResult::WouldBlock;
        };

        let mut percentile_suffix_cache = HashMap::<u64, &str>::new();
        let mut stats_writer = self.stats_writer.acquire();
        let can_use_stack = stats_writer.metric_copied();
        for metric in drain {
            let value = if can_use_stack {
                self.buffer.format(metric.value)
            } else {
                Self::get_value(metric.value, &self.bump, &mut self.buffer)
            };

            let mut metric_parts = ["", "", ""];
            let mut part_count = 0usize;
            if !metric.prefix.is_empty() {
                metric_parts[part_count] = metric.prefix;
                part_count += 1;
            }
            metric_parts[part_count] = metric.metric;
            part_count += 1;
            match metric.suffix {
                MetricSuffix::None => {}
                MetricSuffix::Static(suffix) => {
                    metric_parts[part_count] = suffix;
                    part_count += 1;
                }
                MetricSuffix::Percentile(percentile) => {
                    let key = percentile.to_bits();
                    let suffix = match percentile_suffix_cache.entry(key) {
                        Occupied(occupied_entry) => *occupied_entry.get(),
                        Vacant(vacant_entry) => {
                            vacant_entry.insert(Self::get_percentile_suffix(percentile, &self.bump))
                        }
                    };
                    metric_parts[part_count] = suffix;
                    part_count += 1;
                }
            }

            Self::send_metric(
                &mut stats_writer,
                &metric_parts[..part_count],
                metric.tags,
                value,
                metric.kind,
            );
        }

        if let Err(err) = stats_writer.flush() {
            error!("Error sending metrics: {err}");
        }

        drop(percentile_suffix_cache);
        self.bump.reset();
        SendResult::Ok
    }

    fn get_value<'a>(value: u64, bump: &'a Bump, buffer: &mut Buffer) -> &'a str {
        let value = buffer.format(value);
        bump.alloc_str(value)
    }

    fn get_percentile_suffix(percentile: f64, bump: &Bump) -> &str {
        let mut percentile_number = (percentile * 100.0).to_string();
        if percentile_number.contains('.') {
            while percentile_number.ends_with('0') {
                percentile_number.pop();
            }
            if percentile_number.ends_with('.') {
                percentile_number.pop();
            }
        }

        let suffix = format!(".{percentile_number}percentile");
        bump.alloc_str(&suffix)
    }

    fn send_metric<'data>(
        stats_writer: &mut dyn StatsWriterTrait,
        metric: &[&'data str],
        tags: &'data str,
        value: &'data str,
        metric_type: MetricKind,
    ) {
        match stats_writer.write(metric, tags, value, metric_type) {
            Ok(()) => {}
            Err(err) => error!("Error sending metrics. Error {err}"),
        }
    }
}

pub fn initialize_job<MC>(
    flush_interval: Duration,
    receiver: &Receiver<()>,
    collector: Arc<MC>,
    stats_writer: StatsWriterHolder,
) -> MetricResult<()>
where
    MC: DrainMetricCollectorTrait + Send + Sync + 'static,
    MC::Hasher: BuildHasher + Clone + Send + Sync + 'static,
{
    let mut job = MetricCollectorJob {
        stats_writer,
        collector,
        buffer: Buffer::new(),
        bump: Bump::with_capacity(20 * 1024),
    };

    let large_tick = tick(flush_interval);
    let shorter_tick = tick(Duration::from_millis(10));
    let mut finish = false;

    loop {
        select! {
            recv(large_tick) -> _ => (),
            recv(receiver) -> _ => finish = true,
        }

        loop {
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
