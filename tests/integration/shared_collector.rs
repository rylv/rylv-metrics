use rylv_metrics::{
    DrainMetricCollectorTrait, HistogramConfig, MetricCollector, MetricCollectorOptions,
    MetricCollectorTrait, RylvStr, SharedCollector, SharedCollectorOptions, SigFig,
    StatsWriterType,
};
use std::time::Duration;

use super::custom_writer::TestStatsWriter;

fn drain_metrics_now<S>(collector: &SharedCollector<S>) -> Vec<String>
where
    S: std::hash::BuildHasher + Clone + Send + Sync + 'static,
{
    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            let mut lines = Vec::new();
            for frame in drain.by_ref() {
                let mut metric = String::new();
                metric.push_str(frame.prefix);
                metric.push_str(frame.metric);
                match frame.suffix {
                    rylv_metrics::MetricSuffix::None => {}
                    rylv_metrics::MetricSuffix::Static(suffix) => metric.push_str(suffix),
                    rylv_metrics::MetricSuffix::Percentile(percentile) => {
                        metric.push_str(percentile_suffix(percentile).as_str());
                    }
                }

                let metric_type = match frame.kind {
                    rylv_metrics::MetricKind::Count => "c",
                    rylv_metrics::MetricKind::Gauge => "g",
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
            return lines;
        }
    }
    panic!("unable to acquire drain ownership");
}

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

#[test]
fn test_shared_drain_basic() {
    let options = SharedCollectorOptions {
        stats_prefix: "app.".to_string(),
        ..Default::default()
    };
    let collector = SharedCollector::new(options);

    collector.count(
        RylvStr::from_static("requests"),
        &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
    );
    collector.count_add(
        RylvStr::from_static("requests"),
        2,
        &mut [RylvStr::from_static("a:1"), RylvStr::from_static("b:2")],
    );

    collector.gauge(
        RylvStr::from_static("memory"),
        100,
        &mut [RylvStr::from_static("host:web")],
    );
    collector.gauge(
        RylvStr::from_static("memory"),
        300,
        &mut [RylvStr::from_static("host:web")],
    );

    collector.histogram(
        RylvStr::from_static("latency"),
        42,
        &mut [RylvStr::from_static("endpoint:/")],
    );

    let lines = drain_metrics_now(&collector);
    let joined = lines.concat();

    assert!(joined.contains("app.requests:3|c|#a:1,b:2\n"));
    assert!(joined.contains("app.memory:200|g|#host:web\n"));
    assert!(joined.contains("app.latency.count:1|c|#endpoint:/\n"));
    assert!(joined.contains("app.latency.min:42|g|#endpoint:/\n"));
    assert!(joined.contains("app.latency.avg:42|g|#endpoint:/\n"));
    assert!(joined.contains("app.latency.95percentile:42|g|#endpoint:/\n"));
    assert!(joined.contains("app.latency.99percentile:42|g|#endpoint:/\n"));
    assert!(joined.contains("app.latency.max:42|g|#endpoint:/\n"));

    let drained_again = drain_metrics_now(&collector);
    assert!(drained_again.is_empty());
}

#[test]
fn test_shared_custom_histogram_config() {
    let options = SharedCollectorOptions {
        stats_prefix: String::new(),
        default_histogram_config: HistogramConfig::new(SigFig::default(), vec![0.75])
            .unwrap()
            .with_count(false)
            .with_min(false),
        ..Default::default()
    };

    let collector = SharedCollector::new(options);
    collector.histogram(
        RylvStr::from_static("custom.hist"),
        100,
        &mut [RylvStr::from_static("scope:test")],
    );

    let lines: String = drain_metrics_now(&collector).concat();

    assert!(!lines.contains("custom.hist.count:"));
    assert!(!lines.contains("custom.hist.min:"));
    assert!(lines.contains("custom.hist.avg:100|g|#scope:test\n"));
    assert!(lines.contains("custom.hist.75percentile:100|g|#scope:test\n"));
    assert!(lines.contains("custom.hist.max:100|g|#scope:test\n"));
}

#[test]
fn test_shared_drain_frames_borrowed_output() {
    let options = SharedCollectorOptions::default();
    let collector = SharedCollector::new(options);

    collector.count(
        RylvStr::from_static("frames.count"),
        &mut [RylvStr::from_static("scope:test")],
    );

    let mut seen = 0usize;
    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.by_ref() {
                seen += 1;
                assert_eq!(frame.prefix, "");
                assert_eq!(frame.metric, "frames.count");
                assert_eq!(frame.tags, "scope:test");
                assert_eq!(frame.value, 1);
                assert_eq!(frame.kind, rylv_metrics::MetricKind::Count);
            }
            break;
        }
    }
    assert_eq!(seen, 1);
}

fn random_datadog_addr() -> std::net::SocketAddr {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

fn sorted_lines(text: &str) -> Vec<String> {
    let mut lines = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| format!("{line}\n"))
        .collect::<Vec<_>>();
    lines.sort_unstable();
    lines
}

#[test]
fn test_shared_parity_with_metric_collector_custom_writer() {
    // Empty prefix: the SharedCollector applies the prefix via drain frames,
    // so the writer should not duplicate it.
    let writer = TestStatsWriter::new(512);
    let writer_clone = writer.clone();
    let default_histogram_config =
        HistogramConfig::new(SigFig::default(), vec![0.9, 0.99]).unwrap();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(50),
        writer_type: StatsWriterType::Custom(Box::new(writer)),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = random_datadog_addr();
    let inner = SharedCollector::new(SharedCollectorOptions {
        stats_prefix: "parity.".to_string(),
        default_histogram_config: default_histogram_config.clone(),
        ..Default::default()
    });
    let collector = MetricCollector::new(bind_addr, datadog_addr, options, inner)
        .expect("failed to create collector");

    let shared_collector = SharedCollector::new(SharedCollectorOptions {
        stats_prefix: "parity.".to_string(),
        default_histogram_config,
        ..Default::default()
    });

    collector.count_add(
        RylvStr::from_static("requests"),
        3,
        &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
    );
    shared_collector.count_add(
        RylvStr::from_static("requests"),
        3,
        &mut [RylvStr::from_static("b:2"), RylvStr::from_static("a:1")],
    );

    collector.gauge(
        RylvStr::from_static("memory"),
        100,
        &mut [RylvStr::from_static("host:web")],
    );
    collector.gauge(
        RylvStr::from_static("memory"),
        300,
        &mut [RylvStr::from_static("host:web")],
    );
    shared_collector.gauge(
        RylvStr::from_static("memory"),
        100,
        &mut [RylvStr::from_static("host:web")],
    );
    shared_collector.gauge(
        RylvStr::from_static("memory"),
        300,
        &mut [RylvStr::from_static("host:web")],
    );

    collector.histogram(
        RylvStr::from_static("latency"),
        42,
        &mut [RylvStr::from_static("endpoint:/")],
    );
    collector.histogram(
        RylvStr::from_static("latency"),
        100,
        &mut [RylvStr::from_static("endpoint:/")],
    );
    shared_collector.histogram(
        RylvStr::from_static("latency"),
        42,
        &mut [RylvStr::from_static("endpoint:/")],
    );
    shared_collector.histogram(
        RylvStr::from_static("latency"),
        100,
        &mut [RylvStr::from_static("endpoint:/")],
    );

    drop(collector);

    let with_job = sorted_lines(&writer_clone.get_all_metrics_as_text());
    let shared_lines = drain_metrics_now(&shared_collector);
    let shared_text = shared_lines.concat();
    let shared_sorted = sorted_lines(&shared_text);

    assert_eq!(with_job, shared_sorted);
}
