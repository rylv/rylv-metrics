//! Signed counters/gauges, real (floating-point) means, and over-max errors.
//!
//! Contract:
//! - a counter window's net is emitted as `|c` whenever it is non-zero,
//!   negative included — drop rules (if any) belong to the caller;
//! - a last-write-wins gauge emits its last recorded value, negative included;
//! - an averaged gauge emits the real mean as a double, formatted with at most
//!   6 fraction digits;
//! - a histogram `.avg` is the real mean of the recorded values, not the median;
//! - a value above the histogram maximum surfaces as an `Err` and leaves the
//!   aggregation state unchanged.

use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, MetricFrameRef, MetricKind,
    MetricSuffix, RylvStr, SharedCollector, SharedCollectorOptions, SigFig,
};

fn drain_lines(collector: &SharedCollector) -> Vec<String> {
    let mut lines = Vec::new();
    let mut drain = collector.try_begin_drain().unwrap();
    for frame in drain.frames() {
        let suffix = match frame.suffix {
            rylv_metrics::MetricSuffix::None => String::new(),
            MetricSuffix::Static(suffix) => suffix.to_owned(),
            MetricSuffix::Percentile(percentile) => format!(".{percentile}"),
        };
        lines.push(format!(
            "{}{suffix}:{},{:?}",
            frame.metric, frame.value, frame.kind
        ));
    }
    lines.sort();
    lines
}

#[test]
fn negative_counter_net_is_emitted() {
    let collector = SharedCollector::default();
    collector.count_add(RylvStr::from_static("inflight"), 5, &mut []);
    collector.count_add(RylvStr::from_static("inflight"), -7, &mut []);

    assert_eq!(
        drain_lines(&collector),
        vec!["inflight:-2,Count".to_owned()],
        "the window's net total is emitted, negative included"
    );
}

#[test]
fn zero_counter_net_is_skipped() {
    let collector = SharedCollector::default();
    collector.count_add(RylvStr::from_static("net.zero"), 3, &mut []);
    collector.count_add(RylvStr::from_static("net.zero"), -3, &mut []);

    assert!(
        drain_lines(&collector).is_empty(),
        "a zero net has nothing to send"
    );
}

#[test]
fn negative_gauge_last_value_is_emitted() {
    let collector = SharedCollector::default();
    collector.gauge(RylvStr::from_static("depth"), 10, &mut []);
    collector.gauge(RylvStr::from_static("depth"), -4, &mut []);

    assert_eq!(
        drain_lines(&collector),
        vec!["depth:-4,Gauge".to_owned()],
        "last-write-wins gauge emits its last value, negative included"
    );
}

#[test]
fn gauge_avg_emits_fractional_mean() {
    let collector = SharedCollector::default();
    collector.gauge_avg(RylvStr::from_static("ratio"), 1, &mut []);
    collector.gauge_avg(RylvStr::from_static("ratio"), 2, &mut []);

    assert_eq!(
        drain_lines(&collector),
        vec!["ratio:1.5,GaugeAvg".to_owned()],
        "the mean is a real f64 mean, not an integer division"
    );
}

#[test]
fn gauge_avg_accepts_negative_values() {
    let collector = SharedCollector::default();
    collector.gauge_avg(RylvStr::from_static("delta"), -5, &mut []);
    collector.gauge_avg(RylvStr::from_static("delta"), 2, &mut []);

    assert_eq!(
        drain_lines(&collector),
        vec!["delta:-1.5,GaugeAvg".to_owned()],
        "negative values enter the average and a negative mean is emitted"
    );
}

#[test]
fn histogram_avg_is_the_real_mean() {
    let collector = SharedCollector::default();
    let _ = collector.histogram(RylvStr::from_static("latency"), 40, &mut []);
    let _ = collector.histogram(RylvStr::from_static("latency"), 60, &mut []);

    let lines = drain_lines(&collector);
    // The mean of 40 and 60 is 50, emitted as a floating-point value.
    assert!(
        lines.contains(&"latency.avg:50,Gauge".to_owned()),
        "histogram .avg is the real mean: {lines:?}"
    );
}

#[test]
fn histogram_avg_can_be_fractional() {
    let collector = SharedCollector::default();
    let _ = collector.histogram(RylvStr::from_static("small"), 1, &mut []);
    let _ = collector.histogram(RylvStr::from_static("small"), 2, &mut []);

    let lines = drain_lines(&collector);
    assert!(
        lines.contains(&"small.avg:1.5,Gauge".to_owned()),
        "fractional mean is emitted: {lines:?}"
    );
}

#[test]
fn gauge_avg_kind_is_distinguishable_from_gauge_kind() {
    let collector = SharedCollector::default();
    collector.gauge_avg(RylvStr::from_static("avg.metric"), 7, &mut []);
    collector.gauge(RylvStr::from_static("last.metric"), 7, &mut []);

    let mut drain = collector.try_begin_drain().unwrap();
    let mut avg_kind = None;
    let mut last_kind = None;
    drain.for_each_frame(|frame: MetricFrameRef<'_>| {
        if frame.metric == "avg.metric" {
            avg_kind = Some(frame.kind);
        }
        if frame.metric == "last.metric" {
            last_kind = Some(frame.kind);
        }
    });
    assert_eq!(avg_kind, Some(MetricKind::GaugeAvg));
    assert_eq!(last_kind, Some(MetricKind::Gauge));
}

#[test]
fn over_max_histogram_value_returns_error() {
    let options = SharedCollectorOptions {
        default_histogram_config: rylv_metrics::HistogramConfig::new(SigFig::ONE, Vec::new())
            .unwrap()
            .with_bounds(1, 100)
            .unwrap(),
        ..SharedCollectorOptions::default()
    };
    let collector = SharedCollector::new(options);

    // A value above the configured maximum is rejected; the caller decides how
    // to report it.
    let result = collector.histogram(RylvStr::from_static("bounded"), 200, &mut []);
    assert!(result.is_err(), "a value above the maximum surfaces as Err");

    assert!(
        drain_lines(&collector).is_empty(),
        "the rejected value is not aggregated"
    );

    // The histogram stays usable for in-range values.
    let _ = collector.histogram(RylvStr::from_static("bounded"), 100, &mut []);
    let lines = drain_lines(&collector);
    assert!(
        lines.contains(&"bounded.count:1,Count".to_owned()),
        "{lines:?}"
    );
}
