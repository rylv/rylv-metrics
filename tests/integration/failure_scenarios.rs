use rylv_metrics::{
    DrainMetricCollectorTrait, MetricCollector, MetricCollectorOptions, MetricCollectorTrait,
    RylvStr, SharedCollector, SharedCollectorOptions, StatsWriterType,
};
use std::net::UdpSocket;
use std::time::Duration;

// ============================================================================
// Closed / unreachable destination
// ============================================================================

/// The collector must not panic when the destination is unreachable.
/// UDP is fire-and-forget, so metrics are silently lost — the important
/// thing is that the collector continues to accept new metrics and shuts
/// down cleanly.
#[test]
fn test_collector_survives_unreachable_destination() {
    // Bind a socket to grab a port, then drop it so nothing is listening.
    let tmp = UdpSocket::bind("127.0.0.1:0").unwrap();
    let dead_addr = tmp.local_addr().unwrap();
    drop(tmp);

    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_millis(50),
        writer_type: StatsWriterType::Simple,
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let inner = SharedCollector::default();
    let collector =
        MetricCollector::new(bind_addr, dead_addr, options, inner).expect("constructor succeeds");

    // Record metrics — these will be sent to a port no one listens on.
    for i in 0..100 {
        collector.count(
            RylvStr::from_static("dead.target.count"),
            &mut [RylvStr::from_static("scenario:unreachable")],
        );
        collector.histogram(
            RylvStr::from_static("dead.target.hist"),
            i,
            &mut [RylvStr::from_static("scenario:unreachable")],
        );
    }

    // Wait for at least one flush cycle
    std::thread::sleep(Duration::from_millis(120));

    // Recording must still work after failed sends
    collector.gauge(
        RylvStr::from_static("dead.target.gauge"),
        42,
        &mut [RylvStr::from_static("scenario:unreachable")],
    );

    // Graceful drop — must not panic or hang
    drop(collector);
}

/// When the destination socket is closed mid-stream the collector should
/// continue operating and shut down cleanly.
#[test]
fn test_collector_survives_destination_closed_midstream() {
    let listener = UdpSocket::bind("127.0.0.1:0").unwrap();
    let dest_addr = listener.local_addr().unwrap();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 512,
        max_udp_batch_size: 10,
        flush_interval: Duration::from_millis(50),
        writer_type: StatsWriterType::Simple,
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let inner = SharedCollector::default();
    let collector =
        MetricCollector::new(bind_addr, dest_addr, options, inner).expect("constructor succeeds");

    collector.count(
        RylvStr::from_static("midstream.count"),
        &mut [RylvStr::from_static("phase:before")],
    );

    // Let the first flush go through
    std::thread::sleep(Duration::from_millis(80));

    // Close the receiver — subsequent sends hit a closed port
    drop(listener);

    // Keep recording — the collector must not panic
    for _ in 0..50 {
        collector.count(
            RylvStr::from_static("midstream.count"),
            &mut [RylvStr::from_static("phase:after")],
        );
    }

    std::thread::sleep(Duration::from_millis(80));
    drop(collector);
}

// ============================================================================
// Cardinality explosion
// ============================================================================

/// High-cardinality tags should not cause OOM or panic.
/// The collector will use more memory, but it must remain stable.
#[test]
fn test_shared_collector_handles_high_cardinality_tags() {
    let collector = SharedCollector::new(SharedCollectorOptions {
        stats_prefix: String::new(),
        ..Default::default()
    });

    // Record 1000 unique tag combinations
    for i in 0..1_000 {
        let tag = format!("id:{i}");
        collector.count(
            RylvStr::from_static("high_cardinality.requests"),
            &mut [RylvStr::from(tag)],
        );
    }

    let mut frame_count = 0;
    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            for frame in drain.by_ref() {
                assert_eq!(frame.metric, "high_cardinality.requests");
                assert_eq!(frame.value, 1);
                frame_count += 1;
            }
            break;
        }
    }

    // Each unique tag set produces one counter frame
    assert_eq!(frame_count, 1_000);
}

/// High-cardinality metric names should also be handled gracefully.
#[test]
fn test_shared_collector_handles_high_cardinality_metrics() {
    let collector = SharedCollector::default();

    for i in 0..500 {
        let metric = format!("metric.unique.{i}");
        collector.count(
            RylvStr::from(metric),
            &mut [RylvStr::from_static("env:test")],
        );
    }

    let mut frame_count = 0;
    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            for _frame in drain.by_ref() {
                frame_count += 1;
            }
            break;
        }
    }

    assert_eq!(frame_count, 500);
}

// ============================================================================
// Graceful shutdown under load
// ============================================================================

/// The collector must drain all pending metrics on drop, even when
/// metrics are being recorded up until the very last moment.
#[test]
fn test_graceful_shutdown_flushes_pending_metrics() {
    let receiver_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let dest_addr = receiver_sock.local_addr().unwrap();
    receiver_sock
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1432,
        max_udp_batch_size: 10,
        // Long flush interval so the only flush happens on drop
        flush_interval: Duration::from_secs(60),
        writer_type: StatsWriterType::Simple,
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let inner = SharedCollector::default();
    let collector =
        MetricCollector::new(bind_addr, dest_addr, options, inner).expect("constructor succeeds");

    collector.count(
        RylvStr::from_static("shutdown.pending"),
        &mut [RylvStr::from_static("test:flush")],
    );

    // Drop triggers final drain
    drop(collector);

    // The receiver should get the metric from the final flush
    let mut buf = [0u8; 4096];
    let result = receiver_sock.recv_from(&mut buf);
    assert!(result.is_ok(), "should receive metrics from final flush");
    let (n, _) = result.unwrap();
    let text = String::from_utf8_lossy(&buf[..n]);
    assert!(
        text.contains("shutdown.pending"),
        "final flush should contain pending metrics, got: {text}"
    );
}

// ============================================================================
// Empty drain
// ============================================================================

/// Draining an empty collector should return an empty iterator, not None.
#[test]
fn test_shared_collector_drain_when_empty() {
    let collector = SharedCollector::default();

    let mut acquired = false;
    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            acquired = true;
            assert!(drain.next().is_none(), "empty collector should yield no frames");
            break;
        }
    }
    assert!(acquired, "should be able to acquire drain on empty collector");
}

// ============================================================================
// Consecutive drains
// ============================================================================

/// After a successful drain, the aggregated state should be reset.
/// A second drain should see only metrics recorded after the first drain.
#[test]
fn test_shared_collector_consecutive_drains_reset_state() {
    let collector = SharedCollector::default();

    collector.count(
        RylvStr::from_static("consecutive.count"),
        &mut [RylvStr::from_static("round:1")],
    );

    // First drain
    let mut first_count = 0;
    for _ in 0..8 {
        if let Some(drain) = collector.try_begin_drain() {
            first_count = drain.count();
            break;
        }
    }
    assert_eq!(first_count, 1);

    // Record new metrics
    collector.count(
        RylvStr::from_static("consecutive.count"),
        &mut [RylvStr::from_static("round:2")],
    );

    // Second drain should only see the new metric
    let mut second_count = 0;
    for _ in 0..8 {
        if let Some(drain) = collector.try_begin_drain() {
            second_count = drain.count();
            break;
        }
    }
    assert_eq!(second_count, 1);
}
