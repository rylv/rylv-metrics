use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, MetricKind, MetricSuffix,
    RylvStr, RylvTag, TLSCollector, TLSCollectorOptions,
};

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

fn drain_metrics_now<S>(collector: &TLSCollector<S>) -> Vec<String>
where
    S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
{
    let mut drain = collector
        .try_begin_drain()
        .expect("tls drain should be available");
    let mut lines = Vec::new();
    for frame in drain.frames() {
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
            MetricKind::GaugeAvg | MetricKind::Gauge => "g",
            MetricKind::Timing => "ms",
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
    lines
}

fn sorted_lines(mut lines: Vec<String>) -> Vec<String> {
    lines.sort_unstable();
    lines
}

#[test]
fn test_tls_hashbrown_drain_consumes_prepared_metrics() {
    let collector = TLSCollector::new(TLSCollectorOptions {
        stats_prefix: "tls.".to_string(),
        ..Default::default()
    });

    let sorted =
        collector.prepare_sorted_tags([RylvTag::from_static("b:2"), RylvTag::from_static("a:1")]);
    let prepared = collector.prepare_metric(RylvStr::from_static("requests"), sorted);

    let _ = collector.histogram_prepared(&prepared, 42);

    let lines = sorted_lines(drain_metrics_now(&collector));
    let expected_first = sorted_lines(vec![
        "tls.requests.count:1|c|#a:1,b:2\n".to_string(),
        "tls.requests.min:42|g|#a:1,b:2\n".to_string(),
        "tls.requests.avg:42|g|#a:1,b:2\n".to_string(),
        "tls.requests.95percentile:42|g|#a:1,b:2\n".to_string(),
        "tls.requests.99percentile:42|g|#a:1,b:2\n".to_string(),
        "tls.requests.max:42|g|#a:1,b:2\n".to_string(),
    ]);
    assert_eq!(lines, expected_first);

    let drained_again = drain_metrics_now(&collector);
    assert!(
        drained_again.is_empty(),
        "drain must consume current aggregated state"
    );

    let _ = collector.histogram_prepared(&prepared, 43);
    let drained_third = sorted_lines(drain_metrics_now(&collector));
    let expected_third = sorted_lines(vec![
        "tls.requests.count:1|c|#a:1,b:2\n".to_string(),
        "tls.requests.min:43|g|#a:1,b:2\n".to_string(),
        "tls.requests.avg:43|g|#a:1,b:2\n".to_string(),
        "tls.requests.95percentile:43|g|#a:1,b:2\n".to_string(),
        "tls.requests.99percentile:43|g|#a:1,b:2\n".to_string(),
        "tls.requests.max:43|g|#a:1,b:2\n".to_string(),
    ]);
    assert_eq!(drained_third, expected_third);
}

#[test]
fn test_tls_rejected_histogram_value_leaves_extremes_untouched() {
    // Review repro (PR #18): with limit 100, a rejected 200 must not leave
    // its extremes behind for the next successful record.
    let collector = TLSCollector::new(TLSCollectorOptions {
        default_histogram_config: rylv_metrics::HistogramConfig::new(
            rylv_metrics::SigFig::ONE,
            Vec::new(),
        )
        .unwrap()
        .with_bounds(1, 100)
        .unwrap(),
        ..Default::default()
    });

    assert!(collector
        .histogram(RylvStr::from_static("h"), 200, &mut [])
        .is_err());
    assert!(collector
        .histogram(RylvStr::from_static("h"), 50, &mut [])
        .is_ok());

    let lines = sorted_lines(drain_metrics_now(&collector));
    assert!(lines.contains(&"h.count:1|c\n".to_string()), "{lines:?}");
    assert!(lines.contains(&"h.min:50|g\n".to_string()), "{lines:?}");
    assert!(lines.contains(&"h.max:50|g\n".to_string()), "{lines:?}");
    assert!(
        !lines.iter().any(|line| line.contains(":200")),
        "the rejected value must not appear in the extremes: {lines:?}"
    );
}

#[test]
fn test_tls_histogram_value_above_i64_max_is_rejected() {
    // Emitted histogram series are integers (`i64`), so the default upper
    // bound is `i64::MAX`: a value above it is rejected as over-max instead of
    // being reinterpreted as a negative integer on drain.
    let collector = TLSCollector::new(TLSCollectorOptions::default());

    assert!(
        collector
            .histogram(RylvStr::from_static("wide"), 1 << 63, &mut [])
            .is_err(),
        "a value above i64::MAX is rejected, not emitted as negative"
    );
    assert!(
        drain_metrics_now(&collector).is_empty(),
        "the rejected value is not aggregated"
    );
}

#[test]
fn test_tls_gauge_avg_mean_of_two_i64_max_samples_is_exact() {
    // Review repro (PR #18 review): two `i64::MAX` samples must average to
    // `i64::MAX`; a narrower accumulator overflows (panic in debug builds).
    let collector = TLSCollector::new(TLSCollectorOptions::default());
    collector.gauge_avg(RylvStr::from_static("big"), i64::MAX, &mut []);
    collector.gauge_avg(RylvStr::from_static("big"), i64::MAX, &mut []);

    let lines = drain_metrics_now(&collector);
    assert!(
        lines.contains(&"big:9223372036854775808|g\n".to_string()),
        "the mean of two i64::MAX samples is exact: {lines:?}"
    );
}
