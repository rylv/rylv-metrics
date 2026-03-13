#[cfg(feature = "shared-collector")]
use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricKind as FrameMetricKind, MetricSuffix,
    RylvStr, SharedCollector,
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
            MetricKind::Gauge => "g",
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
        &mut [RylvStr::from_static("env:test")],
    );
    collector.gauge(
        RylvStr::from_static("memory_mb"),
        256,
        &mut [RylvStr::from_static("env:test")],
    );
    collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvStr::from_static("env:test")],
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

    for frame in drain.by_ref() {
        assert!(!frame.metric.is_empty());
        let rendered = match frame.suffix {
            MetricSuffix::None => frame.metric.to_string(),
            MetricSuffix::Static(suffix) => format!("{}{}", frame.metric, suffix),
            MetricSuffix::Percentile(percentile) => format!("{}@{percentile}", frame.metric),
        };
        assert!(!rendered.is_empty());

        match frame.kind {
            FrameMetricKind::Count => saw_count = true,
            FrameMetricKind::Gauge => {
                saw_gauge = true;
                if frame.metric == "latency_ms" {
                    saw_histogram = true;
                }
            }
        }
    }

    assert!(saw_count);
    assert!(saw_gauge);
    assert!(saw_histogram);
}

#[cfg(feature = "tls-collector")]
#[test]
fn miri_tls_drain_keeps_borrowed_frame_fields_valid() {
    let collector = TLSCollector::new(TLSCollectorOptions::default());
    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvStr::from_static("env:test")],
    );
    collector.gauge(
        RylvStr::from_static("memory_mb"),
        256,
        &mut [RylvStr::from_static("env:test")],
    );
    collector.histogram(
        RylvStr::from_static("latency_ms"),
        42,
        &mut [RylvStr::from_static("env:test")],
    );

    let drain = collector.try_begin_drain();
    let mut drain = drain.expect("tls drain should be immediately available");
    let mut saw_count = false;
    let mut saw_gauge = false;
    let mut saw_histogram = false;

    for frame in drain.by_ref() {
        assert!(!frame.metric.is_empty());
        let rendered = match frame.suffix {
            MetricSuffix::None => frame.metric.to_string(),
            MetricSuffix::Static(suffix) => format!("{}{}", frame.metric, suffix),
            MetricSuffix::Percentile(percentile) => format!("{}@{percentile}", frame.metric),
        };
        assert!(!rendered.is_empty());

        match frame.kind {
            FrameMetricKind::Count => saw_count = true,
            FrameMetricKind::Gauge => {
                saw_gauge = true;
                if frame.metric == "latency_ms" {
                    saw_histogram = true;
                }
            }
        }
    }

    assert!(saw_count);
    assert!(saw_gauge);
    assert!(saw_histogram);
}
