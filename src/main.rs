use rylv_metrics::{
    MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    DEFAULT_STATS_WRITER_TYPE,
};
use std::time::{Duration, Instant};

const SERVER_ADDRESS: &str = "127.0.0.1:9090";

fn main() {
    let block = || {
        sync_main_heavy();
    };

    // #[cfg(feature = "allocationcounter")]
    // {
    //     let info = allocation_counter::measure(block);
    //     println!("{:?}", info);
    // }
    // #[cfg(not(feature = "allocationcounter"))]
    {
        block();
    }
}

#[allow(dead_code)]
fn sync_main() {
    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = std::net::ToSocketAddrs::to_socket_addrs(SERVER_ADDRESS)
        .unwrap()
        .next()
        .unwrap();

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1400,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_secs(30),
        stats_prefix: String::new(),
        writer_type: DEFAULT_STATS_WRITER_TYPE,
        histogram_configs: std::collections::HashMap::new(),
    };

    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    let instant = Instant::now();
    for _ in 0..50000000 {
        collector.histogram(
            RylvStr::from_static("some.metric"),
            1,
            [
                RylvStr::from_static("tag:value"),
                RylvStr::from_static("tag2:value2"),
            ],
        );
    }

    collector.shutdown();
    println!("elapsed: {:?}ms", instant.elapsed().as_millis());
}

fn sync_main_heavy() {
    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = std::net::ToSocketAddrs::to_socket_addrs(SERVER_ADDRESS)
        .unwrap()
        .next()
        .unwrap();

    let n = 1024 * 1024;
    let mut vec_metrics = Vec::<&'static str>::with_capacity(n);
    let mut tags_metrics = Vec::<&'static str>::with_capacity(n);

    for i in 0..n {
        vec_metrics.push(format!("some.long.metric.by.some.criteria{i}").leak());
        tags_metrics.push(format!("sometag:somevaluefromcriteria{i}").leak());
    }

    let options = MetricCollectorOptions {
        max_udp_packet_size: 1400,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: DEFAULT_STATS_WRITER_TYPE,
        histogram_configs: std::collections::HashMap::new(),
    };

    #[allow(unused_variables)]
    let collector = MetricCollector::new(bind_addr, datadog_addr, options);

    let instant = Instant::now();
    #[allow(unused_mut, unused_variables)]
    let mut i = 0;
    #[cfg(feature = "allocationcounter")]
    {
        let info = allocation_counter::measure(|| {
            for _ in 0..50000000 {
                collector.histogram(
                    RylvStr::from_static(vec_metrics[i]),
                    1,
                    [
                        RylvStr::from_static(tags_metrics[i]),
                        RylvStr::from_static("tag:value"),
                        RylvStr::from_static("tag2:value2"),
                    ],
                );
                i = (i + 1) % n;
            }

            collector.shutdown();
        });
        println!("{:?}", info)
    }

    println!("elapsed: {:?}ms", instant.elapsed().as_millis());
}
