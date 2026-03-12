use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollectorTrait, MetricKind, MetricSuffix, RylvStr,
    TLSCollector, TLSCollectorOptions,
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
    let drain = collector.try_begin_drain().into_iter().flatten();
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
        collector.prepare_sorted_tags([RylvStr::from_static("b:2"), RylvStr::from_static("a:1")]);
    let prepared = collector.prepare_metric(RylvStr::from_static("requests"), sorted);

    collector.histogram_prepared(&prepared, 42);

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

    collector.histogram_prepared(&prepared, 43);
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
