use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr, StatsWriterType,
    DEFAULT_STATS_WRITER_TYPE,
};
use std::collections::HashSet;
use std::net::UdpSocket;
use std::thread::JoinHandle;
use std::time::Duration;

// ============================================================================
// Helper functions to reduce test code duplication
// ============================================================================

/// Creates a UDP receiver thread that collects all messages until timeout
fn spawn_udp_receiver(port: u16) -> JoinHandle<Vec<String>> {
    std::thread::spawn(move || {
        let socket =
            UdpSocket::bind(format!("127.0.0.1:{}", port)).expect("couldn't bind to address");
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set_read_timeout failed");
        let mut buf = [0; 10000];
        let mut received = Vec::<String>::new();

        while let Ok((number_of_bytes, _)) = socket.recv_from(&mut buf) {
            let filled_buf = &buf[..number_of_bytes];
            let text = String::from_utf8(filled_buf.to_vec()).unwrap();
            received.push(text);
        }
        received
    })
}

/// Creates a UDP receiver thread that expects exactly N messages (for deterministic tests)
fn spawn_udp_receiver_exact(port: u16, expected_count: usize) -> JoinHandle<HashSet<String>> {
    std::thread::spawn(move || {
        let socket =
            UdpSocket::bind(format!("127.0.0.1:{}", port)).expect("couldn't bind to address");
        let mut buf = [0; 10000];
        let mut received = HashSet::<String>::new();
        let mut counter = 0;
        while counter < expected_count {
            let (number_of_bytes, _) = socket.recv_from(&mut buf).expect("Expected udp message");
            let filled_buf = &mut buf[..number_of_bytes];
            let text = String::from_utf8(filled_buf.to_vec()).unwrap();
            received.insert(text);
            counter += 1;
        }
        received
    })
}

/// Creates a MetricCollector with the given configuration
fn create_collector(
    port: u16,
    writer_type: StatsWriterType,
    stats_prefix: String,
    max_udp_packet_size: u16,
) -> MetricCollector {
    let options = MetricCollectorOptions {
        max_udp_packet_size,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix,
        writer_type,
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = format!("127.0.0.1:{}", port).parse().unwrap();

    MetricCollector::new(bind_addr, datadog_addr, options)
}

/// Waits for metrics to be flushed and collects results
fn wait_and_collect(receiver: JoinHandle<Vec<String>>) -> String {
    std::thread::sleep(Duration::from_millis(150));
    let received = receiver.join().unwrap();
    assert!(!received.is_empty(), "Should receive at least one message");
    received.join("\n")
}

/// Waits for metrics to be flushed and collects exact results
fn wait_and_collect_exact(receiver: JoinHandle<HashSet<String>>) -> HashSet<String> {
    receiver.join().unwrap()
}

// ============================================================================
// Basic writer type tests
// ============================================================================

#[test]
fn test_build_collector() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver_exact(9090, 2);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9090, DEFAULT_STATS_WRITER_TYPE, String::new(), 261);

    collector.histogram(
        RylvStr::from_static("some.metric"),
        1,
        &mut [
            RylvStr::from_static("tag:value"),
            RylvStr::from_static("tag2:value2"),
        ],
    );
    collector.histogram(
        RylvStr::from_static("some.metric"),
        1,
        &mut [
            RylvStr::from_static("tag:value"),
            RylvStr::from_static("tag2:value3"),
        ],
    );

    let expected1 = "some.metric.count:1|c|#tag2:value2,tag:value\nsome.metric.min:1|g|#tag2:value2,tag:value\nsome.metric.avg:1|g|#tag2:value2,tag:value\nsome.metric.99percentile:1|g|#tag2:value2,tag:value\nsome.metric.max:1|g|#tag2:value2,tag:value\n";
    let expected2 = "some.metric.count:1|c|#tag2:value3,tag:value\nsome.metric.min:1|g|#tag2:value3,tag:value\nsome.metric.avg:1|g|#tag2:value3,tag:value\nsome.metric.99percentile:1|g|#tag2:value3,tag:value\nsome.metric.max:1|g|#tag2:value3,tag:value\n";

    let mut expected_set = HashSet::new();
    expected_set.insert(expected1.to_owned());
    expected_set.insert(expected2.to_owned());

    let received = wait_and_collect_exact(receiver);
    assert_eq!(received, expected_set);

    Ok(())
}

#[test]
fn test_simple_writer() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9091);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9091, StatsWriterType::Simple, String::new(), 261);

    collector.histogram(
        RylvStr::from_static("test.simple.writer"),
        10,
        &mut [RylvStr::from_static("env:test")],
    );
    collector.histogram(
        RylvStr::from_static("test.simple.writer"),
        20,
        &mut [RylvStr::from_static("env:prod")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("test.simple.writer.count:1|c|#env:test"));
    assert!(all_text.contains("test.simple.writer.count:1|c|#env:prod"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_writer() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9092);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9092, StatsWriterType::LinuxBatch, String::new(), 261);

    collector.histogram(
        RylvStr::from_static("test.linux.batch"),
        100,
        &mut [RylvStr::from_static("batch:enabled")],
    );
    collector.histogram(
        RylvStr::from_static("test.linux.batch"),
        200,
        &mut [RylvStr::from_static("batch:test")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("test.linux.batch.count:1|c|#batch:enabled"));
    assert!(all_text.contains("test.linux.batch.count:1|c|#batch:test"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_writer() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9093);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9093, StatsWriterType::AppleBatch, String::new(), 261);

    collector.histogram(
        RylvStr::from_static("test.apple.batch"),
        50,
        &mut [RylvStr::from_static("platform:macos")],
    );
    collector.histogram(
        RylvStr::from_static("test.apple.batch"),
        75,
        &mut [RylvStr::from_static("platform:ios")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("test.apple.batch.count:1|c|#platform:macos"));
    assert!(all_text.contains("test.apple.batch.count:1|c|#platform:ios"));

    Ok(())
}

// ============================================================================
// Counter and Gauge method tests
// ============================================================================

#[test]
fn test_simple_writer_counter_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9094);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9094, StatsWriterType::Simple, String::new(), 512);

    collector.count(
        RylvStr::from_static("requests.total"),
        &mut [RylvStr::from_static("service:api")],
    );
    collector.count_add(
        RylvStr::from_static("requests.bytes"),
        1024,
        &mut [RylvStr::from_static("service:api")],
    );
    collector.gauge(
        RylvStr::from_static("memory.usage"),
        75,
        &mut [RylvStr::from_static("host:server1")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("requests.total:1|c|#service:api"));
    assert!(all_text.contains("requests.bytes:1024|c|#service:api"));
    assert!(all_text.contains("memory.usage:75|g|#host:server1"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_counter_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9095);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9095, StatsWriterType::LinuxBatch, String::new(), 512);

    collector.count(
        RylvStr::from_static("linux.requests"),
        &mut [RylvStr::from_static("batch:sendmmsg")],
    );
    collector.count_add(
        RylvStr::from_static("linux.packets"),
        500,
        &mut [RylvStr::from_static("batch:sendmmsg")],
    );
    collector.gauge(
        RylvStr::from_static("linux.cpu"),
        85,
        &mut [RylvStr::from_static("core:0")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.requests:1|c|#batch:sendmmsg"));
    assert!(all_text.contains("linux.packets:500|c|#batch:sendmmsg"));
    assert!(all_text.contains("linux.cpu:85|g|#core:0"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_counter_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9096);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9096, StatsWriterType::AppleBatch, String::new(), 512);

    collector.count(
        RylvStr::from_static("macos.events"),
        &mut [RylvStr::from_static("batch:sendmsg_x")],
    );
    collector.count_add(
        RylvStr::from_static("macos.transfers"),
        2048,
        &mut [RylvStr::from_static("batch:sendmsg_x")],
    );
    collector.gauge(
        RylvStr::from_static("macos.temp"),
        65,
        &mut [RylvStr::from_static("sensor:cpu")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.events:1|c|#batch:sendmsg_x"));
    assert!(all_text.contains("macos.transfers:2048|c|#batch:sendmsg_x"));
    assert!(all_text.contains("macos.temp:65|g|#sensor:cpu"));

    Ok(())
}

// ============================================================================
// Mixed method tests
// ============================================================================

#[test]
fn test_simple_writer_mixed_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9097);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9097, StatsWriterType::Simple, "app.".to_string(), 1024);

    collector.histogram(
        RylvStr::from_static("latency"),
        150,
        &mut [RylvStr::from_static("endpoint:/api")],
    );
    collector.count(
        RylvStr::from_static("errors"),
        &mut [RylvStr::from_static("type:500")],
    );
    collector.count_add(
        RylvStr::from_static("throughput"),
        10000,
        &mut [RylvStr::from_static("unit:bytes")],
    );
    collector.gauge(
        RylvStr::from_static("connections"),
        42,
        &mut [RylvStr::from_static("pool:main")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("app.latency.count:1|c|#endpoint:/api"));
    assert!(all_text.contains("app.errors:1|c|#type:500"));
    assert!(all_text.contains("app.throughput:10000|c|#unit:bytes"));
    assert!(all_text.contains("app.connections:42|g|#pool:main"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_mixed_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9098);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(
        9098,
        StatsWriterType::LinuxBatch,
        "linux.".to_string(),
        1024,
    );

    collector.histogram(
        RylvStr::from_static("response.time"),
        250,
        &mut [RylvStr::from_static("service:web")],
    );
    collector.count(
        RylvStr::from_static("requests.count"),
        &mut [RylvStr::from_static("method:GET")],
    );
    collector.count_add(
        RylvStr::from_static("bytes.sent"),
        5120,
        &mut [RylvStr::from_static("proto:http")],
    );
    collector.gauge(
        RylvStr::from_static("active.users"),
        123,
        &mut [RylvStr::from_static("region:us-east")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.response.time.count:1|c|#service:web"));
    assert!(all_text.contains("linux.requests.count:1|c|#method:GET"));
    assert!(all_text.contains("linux.bytes.sent:5120|c|#proto:http"));
    assert!(all_text.contains("linux.active.users:123|g|#region:us-east"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_mixed_methods() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9099);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(
        9099,
        StatsWriterType::AppleBatch,
        "macos.".to_string(),
        1024,
    );

    collector.histogram(
        RylvStr::from_static("render.time"),
        16,
        &mut [RylvStr::from_static("view:main")],
    );
    collector.count(
        RylvStr::from_static("gestures.tap"),
        &mut [RylvStr::from_static("screen:home")],
    );
    collector.count_add(
        RylvStr::from_static("pixels.drawn"),
        1920000,
        &mut [RylvStr::from_static("resolution:1080p")],
    );
    collector.gauge(
        RylvStr::from_static("battery.level"),
        87,
        &mut [RylvStr::from_static("device:iphone")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.render.time.count:1|c|#view:main"));
    assert!(all_text.contains("macos.gestures.tap:1|c|#screen:home"));
    assert!(all_text.contains("macos.pixels.drawn:1920000|c|#resolution:1080p"));
    assert!(all_text.contains("macos.battery.level:87|g|#device:iphone"));

    Ok(())
}

// ============================================================================
// Aggregation tests - counter
// ============================================================================

#[test]
fn test_simple_writer_counter_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9100);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9100, StatsWriterType::Simple, String::new(), 512);

    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );
    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );
    collector.count(
        RylvStr::from_static("page.views"),
        &mut [RylvStr::from_static("page:home")],
    );

    collector.count_add(
        RylvStr::from_static("data.transferred"),
        100,
        &mut [RylvStr::from_static("protocol:tcp")],
    );
    collector.count_add(
        RylvStr::from_static("data.transferred"),
        200,
        &mut [RylvStr::from_static("protocol:tcp")],
    );
    collector.count_add(
        RylvStr::from_static("data.transferred"),
        150,
        &mut [RylvStr::from_static("protocol:tcp")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("page.views:3|c|#page:home"));
    assert!(all_text.contains("data.transferred:450|c|#protocol:tcp"));

    Ok(())
}

#[test]
fn test_simple_writer_gauge_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9101);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9101, StatsWriterType::Simple, String::new(), 512);

    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        50,
        &mut [RylvStr::from_static("host:web1")],
    );
    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        75,
        &mut [RylvStr::from_static("host:web1")],
    );
    collector.gauge(
        RylvStr::from_static("cpu.usage"),
        90,
        &mut [RylvStr::from_static("host:web1")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(
        all_text.contains("cpu.usage:") && all_text.contains("|g|#host:web1"),
        "Gauge metric should be present"
    );

    Ok(())
}

#[test]
fn test_simple_writer_histogram_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9102);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9102, StatsWriterType::Simple, String::new(), 512);

    collector.histogram(
        RylvStr::from_static("request.duration"),
        100,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        200,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        150,
        &mut [RylvStr::from_static("endpoint:/users")],
    );
    collector.histogram(
        RylvStr::from_static("request.duration"),
        300,
        &mut [RylvStr::from_static("endpoint:/users")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("request.duration.count:4|c|#endpoint:/users"));
    assert!(all_text.contains("request.duration.min:100|g|#endpoint:/users"));
    assert!(all_text.contains("request.duration.max:300|g|#endpoint:/users"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_counter_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9103);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9103, StatsWriterType::LinuxBatch, String::new(), 512);

    for _ in 0..5 {
        collector.count(
            RylvStr::from_static("linux.network.packets"),
            &mut [RylvStr::from_static("iface:eth0")],
        );
    }

    collector.count_add(
        RylvStr::from_static("linux.disk.writes"),
        512,
        &mut [RylvStr::from_static("mount:/")],
    );
    collector.count_add(
        RylvStr::from_static("linux.disk.writes"),
        1024,
        &mut [RylvStr::from_static("mount:/")],
    );
    collector.count_add(
        RylvStr::from_static("linux.disk.writes"),
        768,
        &mut [RylvStr::from_static("mount:/")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.network.packets:5|c|#iface:eth0"));
    assert!(all_text.contains("linux.disk.writes:2304|c|#mount:/"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_histogram_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9104);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9104, StatsWriterType::LinuxBatch, String::new(), 1024);

    let values = [50, 75, 125, 200, 100];
    for &val in &values {
        collector.histogram(
            RylvStr::from_static("linux.query.time"),
            val,
            &mut [RylvStr::from_static("db:postgres")],
        );
    }

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.query.time.count:5|c|#db:postgres"));
    assert!(all_text.contains("linux.query.time.min:50|g|#db:postgres"));
    assert!(all_text.contains("linux.query.time.max:200|g|#db:postgres"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_counter_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9105);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9105, StatsWriterType::AppleBatch, String::new(), 512);

    for _ in 0..6 {
        collector.count(
            RylvStr::from_static("macos.ui.clicks"),
            &mut [RylvStr::from_static("button:submit")],
        );
    }

    collector.count_add(
        RylvStr::from_static("macos.file.size"),
        256,
        &mut [RylvStr::from_static("type:image")],
    );
    collector.count_add(
        RylvStr::from_static("macos.file.size"),
        512,
        &mut [RylvStr::from_static("type:image")],
    );
    collector.count_add(
        RylvStr::from_static("macos.file.size"),
        1024,
        &mut [RylvStr::from_static("type:image")],
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.ui.clicks:6|c|#button:submit"));
    assert!(all_text.contains("macos.file.size:1792|c|#type:image"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_histogram_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9106);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9106, StatsWriterType::AppleBatch, String::new(), 1024);

    let values = [30, 60, 45, 55, 58, 62];
    for &val in &values {
        collector.histogram(
            RylvStr::from_static("macos.animation.fps"),
            val,
            &mut [RylvStr::from_static("scene:menu")],
        );
    }

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.animation.fps.count:6|c|#scene:menu"));
    assert!(all_text.contains("macos.animation.fps.min:30|g|#scene:menu"));
    assert!(all_text.contains("macos.animation.fps.max:62|g|#scene:menu"));

    Ok(())
}

// ============================================================================
// Heavy aggregation tests
// ============================================================================

#[test]
fn test_simple_writer_heavy_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9107);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9107, StatsWriterType::Simple, String::new(), 1024);

    for i in 1..=50 {
        collector.count(
            RylvStr::from_static("heavy.counter"),
            &mut [RylvStr::from_static("load:test")],
        );
        collector.histogram(
            RylvStr::from_static("heavy.histogram"),
            i * 10,
            &mut [RylvStr::from_static("load:test")],
        );
    }

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("heavy.counter:50|c|#load:test"));
    assert!(all_text.contains("heavy.histogram.count:50|c|#load:test"));
    assert!(all_text.contains("heavy.histogram.min:10|g|#load:test"));
    assert!(all_text.contains("heavy.histogram.max:500|g|#load:test"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_heavy_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9108);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9108, StatsWriterType::LinuxBatch, String::new(), 1024);

    for i in 1..=100 {
        collector.count(
            RylvStr::from_static("linux.heavy.requests"),
            &mut [RylvStr::from_static("test:sendmmsg")],
        );
        if i % 2 == 0 {
            collector.histogram(
                RylvStr::from_static("linux.heavy.latency"),
                i,
                &mut [RylvStr::from_static("test:sendmmsg")],
            );
        }
    }

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.heavy.requests:100|c|#test:sendmmsg"));
    assert!(all_text.contains("linux.heavy.latency.count:50|c|#test:sendmmsg"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_heavy_aggregation() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9109);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9109, StatsWriterType::AppleBatch, String::new(), 1024);

    for i in 1..=100 {
        collector.count(
            RylvStr::from_static("macos.heavy.interactions"),
            &mut [RylvStr::from_static("test:sendmsg_x")],
        );
        if i % 3 == 0 {
            collector.histogram(
                RylvStr::from_static("macos.heavy.frametime"),
                i,
                &mut [RylvStr::from_static("test:sendmsg_x")],
            );
        }
    }

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.heavy.interactions:100|c|#test:sendmsg_x"));
    assert!(all_text.contains("macos.heavy.frametime.count:33|c|#test:sendmsg_x"));

    Ok(())
}

// ============================================================================
// Tests without tags
// ============================================================================

#[test]
fn test_simple_writer_no_tags() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9110);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9110, StatsWriterType::Simple, String::new(), 512);

    let mut empty_tags: [RylvStr<'_>; 0] = [];
    collector.count(RylvStr::from_static("notags.counter"), &mut empty_tags);
    collector.count_add(
        RylvStr::from_static("notags.counter.value"),
        42,
        &mut empty_tags,
    );
    collector.gauge(RylvStr::from_static("notags.gauge"), 100, &mut empty_tags);
    collector.histogram(
        RylvStr::from_static("notags.histogram"),
        250,
        &mut empty_tags,
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("notags.counter:1|c"));
    assert!(all_text.contains("notags.counter.value:42|c"));
    assert!(all_text.contains("notags.gauge:100|g"));
    assert!(all_text.contains("notags.histogram.count:1|c"));
    assert!(all_text.contains("notags.histogram.min:250|g"));
    assert!(all_text.contains("notags.histogram.max:250|g"));
    // Verify no tags are present (no |# separator)
    assert!(!all_text.contains("notags.counter:1|c|#"));
    assert!(!all_text.contains("notags.gauge:100|g|#"));

    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn test_linux_batch_no_tags() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9111);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9111, StatsWriterType::LinuxBatch, String::new(), 512);

    let mut empty_tags: [RylvStr<'_>; 0] = [];
    collector.count(
        RylvStr::from_static("linux.notags.requests"),
        &mut empty_tags,
    );
    collector.count_add(
        RylvStr::from_static("linux.notags.bytes"),
        1024,
        &mut empty_tags,
    );
    collector.gauge(
        RylvStr::from_static("linux.notags.cpu"),
        45,
        &mut empty_tags,
    );
    collector.histogram(
        RylvStr::from_static("linux.notags.latency"),
        75,
        &mut empty_tags,
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("linux.notags.requests:1|c"));
    assert!(all_text.contains("linux.notags.bytes:1024|c"));
    assert!(all_text.contains("linux.notags.cpu:45|g"));
    assert!(all_text.contains("linux.notags.latency.count:1|c"));
    // Verify no tags are present
    assert!(!all_text.contains("linux.notags.requests:1|c|#"));
    assert!(!all_text.contains("linux.notags.cpu:45|g|#"));

    Ok(())
}

#[test]
#[cfg(target_vendor = "apple")]
fn test_apple_batch_no_tags() -> std::io::Result<()> {
    let receiver = spawn_udp_receiver(9112);
    std::thread::sleep(Duration::from_millis(100));

    let collector = create_collector(9112, StatsWriterType::AppleBatch, String::new(), 512);

    let mut empty_tags: [RylvStr<'_>; 0] = [];
    collector.count(RylvStr::from_static("macos.notags.events"), &mut empty_tags);
    collector.count_add(
        RylvStr::from_static("macos.notags.transfers"),
        2048,
        &mut empty_tags,
    );
    collector.gauge(
        RylvStr::from_static("macos.notags.memory"),
        512,
        &mut empty_tags,
    );
    collector.histogram(
        RylvStr::from_static("macos.notags.response"),
        125,
        &mut empty_tags,
    );

    let all_text = wait_and_collect(receiver);
    assert!(all_text.contains("macos.notags.events:1|c"));
    assert!(all_text.contains("macos.notags.transfers:2048|c"));
    assert!(all_text.contains("macos.notags.memory:512|g"));
    assert!(all_text.contains("macos.notags.response.count:1|c"));
    // Verify no tags are present
    assert!(!all_text.contains("macos.notags.events:1|c|#"));
    assert!(!all_text.contains("macos.notags.memory:512|g|#"));

    Ok(())
}
