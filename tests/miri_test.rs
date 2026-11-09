#[cfg(feature = "shared-collector")]
use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, MetricKind as FrameMetricKind,
    MetricSuffix, MetricValue, RylvStr, RylvTag, SharedCollector,
};
#[cfg(all(feature = "custom_writer", feature = "udp"))]
use rylv_metrics::{MetricKind, MetricResult, StatsWriterTrait, StatsWriterType};
#[cfg(feature = "tls-collector")]
use rylv_metrics::{TLSCollector, TLSCollectorOptions};

#[cfg(all(feature = "custom_writer", feature = "udp"))]
#[derive(Default)]
struct MiriCustomWriter {
    chunks: Vec<String>,
    current: String,
}

#[cfg(all(feature = "custom_writer", feature = "udp"))]
impl MiriCustomWriter {
    fn all_text(&self) -> String {
        self.chunks.concat()
    }
}

#[cfg(all(feature = "custom_writer", feature = "udp"))]
impl StatsWriterTrait for MiriCustomWriter {
    fn metric_copied(&self) -> bool {
        true
    }

    fn write(
        &mut self,
        metrics: &[&str],
        tags: &str,
        value: &str,
        metric_type: MetricKind,
    ) -> MetricResult<()> {
        let metric_type = match metric_type {
            MetricKind::Count => "c",
            MetricKind::GaugeAvg | MetricKind::Gauge => "g",
            MetricKind::Timing => "ms",
        };
        for metric in metrics {
            self.current.push_str(metric);
        }
        self.current.push(':');
        self.current.push_str(value);
        self.current.push('|');
        self.current.push_str(metric_type);
        if !tags.is_empty() {
            self.current.push_str("|#");
            self.current.push_str(tags);
        }
        self.current.push('\n');
        Ok(())
    }

    fn flush(&mut self) -> MetricResult<usize> {
        if self.current.is_empty() {
            return Ok(0);
        }

        let size = self.current.len();
        self.chunks.push(std::mem::take(&mut self.current));
        Ok(size)
    }

    fn reset(&mut self) {
        self.current.clear();
    }
}

#[cfg(all(feature = "custom_writer", feature = "udp"))]
#[test]
fn miri_custom_writer_formats_and_flushes() {
    let mut writer = MiriCustomWriter::default();

    writer
        .write(&["custom.metric"], "env:test", "42", MetricKind::Count)
        .expect("write should succeed");
    writer
        .write(&["another.metric"], "", "1", MetricKind::Gauge)
        .expect("write should succeed");

    let flushed = writer.flush().expect("flush should succeed");
    assert!(flushed > 0);

    let text = writer.all_text();
    assert!(text.contains("custom.metric:42|c|#env:test\n"));
    assert!(text.contains("another.metric:1|g\n"));
}

#[cfg(all(feature = "custom_writer", feature = "udp"))]
#[test]
fn miri_custom_writer_reset_clears_pending_buffer() {
    let mut writer = MiriCustomWriter::default();

    writer
        .write(&["pending.metric"], "scope:miri", "7", MetricKind::Gauge)
        .expect("write should succeed");
    writer.reset();

    let flushed = writer.flush().expect("flush should succeed");
    assert_eq!(flushed, 0);
    assert!(writer.all_text().is_empty());
}

#[cfg(all(feature = "custom_writer", feature = "udp"))]
#[test]
fn miri_custom_writer_can_be_wrapped_in_stats_writer_type() {
    let custom: Box<dyn StatsWriterTrait + Send + Sync> = Box::new(MiriCustomWriter::default());
    let writer_type = StatsWriterType::Custom(custom);

    assert!(matches!(writer_type, StatsWriterType::Custom(_)));
}

#[cfg(feature = "shared-collector")]
#[test]
fn miri_shared_drain_keeps_borrowed_frame_fields_valid() {
    let collector = SharedCollector::default();
    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvTag::from_static("env:test")],
    );
    collector.gauge_avg(
        RylvStr::from_static("memory_mb"),
        256,
        &mut [RylvTag::from_static("env:test")],
    );
    let _ = collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvTag::from_static("env:test")],
    );
    let _ = collector.timing(
        RylvStr::from_static("duration_ms"),
        55,
        &mut [RylvTag::from_static("env:test")],
    );

    let mut acquired = None;
    for _ in 0..8 {
        if let Some(drain) = collector.try_begin_drain() {
            acquired = Some(drain);
            break;
        }
    }

    let mut drain = acquired.expect("drain should become available");
    let mut saw_count = false;
    let mut saw_gauge = false;
    let mut saw_histogram = false;
    let mut saw_timing = false;

    for frame in drain.frames() {
        assert!(!frame.metric.is_empty());
        let rendered = match frame.suffix {
            MetricSuffix::None => frame.metric.to_string(),
            MetricSuffix::Static(suffix) => format!("{}{}", frame.metric, suffix),
            MetricSuffix::Percentile(percentile) => format!("{}@{percentile}", frame.metric),
        };
        assert!(!rendered.is_empty());

        match frame.kind {
            FrameMetricKind::Count => saw_count = true,
            FrameMetricKind::GaugeAvg | FrameMetricKind::Gauge => {
                saw_gauge = true;
                if frame.metric == "latency_ms" {
                    saw_histogram = true;
                }
            }
            FrameMetricKind::Timing => {
                if frame.metric == "duration_ms" {
                    saw_timing = true;
                }
            }
        }
    }

    assert!(saw_count);
    assert!(saw_gauge);
    assert!(saw_histogram);
    assert!(saw_timing);
}

#[cfg(feature = "shared-collector")]
#[test]
fn miri_shared_drain_can_be_dropped_before_exhaustion() {
    let collector = SharedCollector::default();
    let _ = collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvTag::from_static("env:test")],
    );

    let mut drain = collector
        .try_begin_drain()
        .expect("shared drain should become available");
    assert!(drain.frames().next().is_some());
    drop(drain);

    collector.count(RylvStr::from_static("after_drop"), &mut []);
    assert!(collector.try_begin_drain().is_some());
}

#[cfg(feature = "shared-collector")]
#[test]
fn miri_shared_gauge_state_spinlock_under_concurrent_record_and_drain() {
    // The averaged-gauge sum is an `UnsafeCell<i128>` guarded by a manual CAS
    // spinlock; the invariant "the sum is accessed only while holding
    // `sum_lock`" is a concurrent claim, so recorders and a drainer must run
    // together under Miri.
    //
    // Two schedule-dependent paths of the dependencies perform
    // integer-to-pointer casts, which Miri rejects under strict provenance, so
    // each round is arranged to stay off both:
    // - dashmap's shard lock parks through `parking_lot_core` when a thread
    //   waits on another thread's write lock, and a write lock is taken to
    //   insert a missing entry. The recorders therefore only ever find
    //   existing entries: this thread inserts the keys into the current
    //   generation before they start, and the drain installs the generation
    //   recycled by the previous drain, which kept its entries because it was
    //   recorded into before being drained.
    // - arc-swap's helping fallback runs when a `load` races the `swap` that
    //   begins a drain. The drain therefore begins (swaps generations) before
    //   the recorders start; they then record concurrently, contending with
    //   each other on the same gauge's spinlock, while this thread drains the
    //   outgoing generation (`take_sum`) and recycles it.
    const ROUNDS: i64 = 4;
    const RECORDERS: i64 = 2;
    const RECORDS_PER_ROUND: i64 = 16;

    fn record(collector: &SharedCollector, value: i64) {
        collector.gauge_avg(
            RylvStr::from_static("miri.gauge"),
            value,
            &mut [RylvTag::from_static("env:miri")],
        );
        collector.count_add(
            RylvStr::from_static("miri.gauge"),
            1,
            &mut [RylvTag::from_static("env:miri")],
        );
    }

    // Traverses a drained generation and returns its count total.
    fn drained_count(drain: &mut impl MetricDrain) -> i64 {
        let mut counted = 0;
        for frame in drain.frames() {
            assert_eq!(frame.metric, "miri.gauge");
            match (frame.kind, frame.value) {
                (FrameMetricKind::Count, MetricValue::Int(count)) => counted += count,
                (FrameMetricKind::GaugeAvg, MetricValue::Float(mean)) => {
                    assert!((-32.0..=31.0).contains(&mean), "mean {mean} out of range");
                }
                other => panic!("unexpected frame {other:?}"),
            }
        }
        counted
    }

    fn begin_drain(collector: &SharedCollector) -> impl MetricDrain + '_ {
        collector
            .try_begin_drain()
            .expect("no other thread holds the outgoing generation")
    }

    let collector = SharedCollector::default();

    // Leave a recycled generation holding the keys for the first round.
    record(&collector, 0);
    let mut drained = drained_count(&mut begin_drain(&collector));

    for _ in 0..ROUNDS {
        record(&collector, 0);
        let mut drain = begin_drain(&collector);
        std::thread::scope(|scope| {
            for _ in 0..RECORDERS {
                let collector = &collector;
                scope.spawn(move || {
                    for value in 0..RECORDS_PER_ROUND {
                        record(collector, value * 4 - 32);
                    }
                });
            }

            // Drain and recycle the outgoing generation while the recorders
            // run: its `take_sum` follows `record` calls made on other
            // threads in the previous round.
            drained += drained_count(&mut drain);
            drop(drain);
        });
    }

    // Every count recorded so far is drained exactly once.
    drained += drained_count(&mut begin_drain(&collector));
    assert_eq!(drained, 1 + ROUNDS * (1 + RECORDERS * RECORDS_PER_ROUND));

    // After the threads join, the collector still accepts records and drains.
    collector.gauge_avg(RylvStr::from_static("miri.gauge"), 7, &mut []);
    let mut drain = collector
        .try_begin_drain()
        .expect("drain should become available after the threads join");
    assert!(drain.frames().next().is_some());
}

#[cfg(feature = "tls-collector")]
#[test]
fn miri_tls_drain_keeps_borrowed_frame_fields_valid() {
    let collector = TLSCollector::new(TLSCollectorOptions::default());
    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvTag::from_static("env:test")],
    );
    collector.gauge_avg(
        RylvStr::from_static("memory_mb"),
        256,
        &mut [RylvTag::from_static("env:test")],
    );
    let _ = collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvTag::from_static("env:test")],
    );
    let _ = collector.timing(
        RylvStr::from_static("duration_ms"),
        55,
        &mut [RylvTag::from_static("env:test")],
    );

    let drain = collector.try_begin_drain();
    let mut drain = drain.expect("tls drain should be immediately available");
    let mut saw_count = false;
    let mut saw_gauge = false;
    let mut saw_histogram = false;
    let mut saw_timing = false;

    for frame in drain.frames() {
        assert!(!frame.metric.is_empty());
        let rendered = match frame.suffix {
            MetricSuffix::None => frame.metric.to_string(),
            MetricSuffix::Static(suffix) => format!("{}{}", frame.metric, suffix),
            MetricSuffix::Percentile(percentile) => format!("{}@{percentile}", frame.metric),
        };
        assert!(!rendered.is_empty());

        match frame.kind {
            FrameMetricKind::Count => saw_count = true,
            FrameMetricKind::GaugeAvg | FrameMetricKind::Gauge => {
                saw_gauge = true;
                if frame.metric == "latency_ms" {
                    saw_histogram = true;
                }
            }
            FrameMetricKind::Timing => {
                if frame.metric == "duration_ms" {
                    saw_timing = true;
                }
            }
        }
    }

    assert!(saw_count);
    assert!(saw_gauge);
    assert!(saw_histogram);
    assert!(saw_timing);
}

#[cfg(feature = "tls-collector")]
#[test]
fn miri_tls_drain_can_be_dropped_before_exhaustion() {
    let collector = TLSCollector::new(TLSCollectorOptions::default());
    let _ = collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvTag::from_static("env:test")],
    );

    let mut drain = collector
        .try_begin_drain()
        .expect("tls drain should be immediately available");
    assert!(drain.frames().next().is_some());
    drop(drain);

    collector.count(RylvStr::from_static("after_drop"), &mut []);
    assert!(collector.try_begin_drain().is_some());
}
