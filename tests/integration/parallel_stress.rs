use rylv_metrics::{
    histogram, MetricCollector, MetricCollectorOptions, MetricCollectorTrait, RylvStr,
    StatsWriterType,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn create_test_collector() -> MetricCollector {
    let options = MetricCollectorOptions {
        max_udp_packet_size: 1500,
        max_udp_batch_size: 100,
        flush_interval: Duration::from_millis(100),
        stats_prefix: String::new(),
        writer_type: StatsWriterType::Simple,
        histogram_configs: std::collections::HashMap::new(),
    };

    let bind_addr = "0.0.0.0:0".parse().unwrap();
    let datadog_addr = "127.0.0.1:8125".parse().unwrap();

    MetricCollector::new(bind_addr, datadog_addr, options)
}

#[test]
fn test_parallel_histogram_stress() {
    let collector = Arc::new(create_test_collector());
    let num_threads = 8;
    let iterations_per_thread = 10_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let value = (thread_id * iterations_per_thread + i) as u64;

                    // Test histogram with static tags
                    collector.histogram(
                        RylvStr::from_static("parallel.histogram.static"),
                        value,
                        [
                            RylvStr::from_static("thread:static"),
                            RylvStr::from_static("test:parallel"),
                        ],
                    );

                    // Test histogram with dynamic tags
                    collector.histogram(
                        RylvStr::from_static("parallel.histogram.dynamic"),
                        value,
                        [
                            RylvStr::from(format!("thread:{}", thread_id)),
                            RylvStr::from(format!("iteration:{}", i)),
                        ],
                    );

                    // Test histogram with mixed tags
                    collector.histogram(
                        RylvStr::from_static("parallel.histogram.mixed"),
                        value,
                        [
                            RylvStr::from(String::from("static:tag")),
                            RylvStr::from(format!("thread:{}", thread_id)),
                        ],
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

fn wait_and_shutdown(collector: Arc<MetricCollector>) {
    // Wait for all Arc clones to be dropped, then call shutdown
    let mut holder = Some(collector);
    loop {
        match Arc::try_unwrap(holder.take().unwrap()) {
            Ok(collector) => {
                collector.shutdown();
                break;
            }
            Err(c) => {
                let _ = holder.insert(c);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn test_parallel_mixed_metrics() {
    let collector = Arc::new(create_test_collector());
    let num_threads = 8;
    let iterations_per_thread = 5_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let value = (thread_id * iterations_per_thread + i) as u64;
                    let tag = format!("thread:{}", thread_id);

                    match i % 4 {
                        0 => {
                            // Histogram
                            collector.histogram(
                                RylvStr::from_static("parallel.mixed.histogram"),
                                value,
                                [RylvStr::from(tag.clone())],
                            );
                        }
                        1 => {
                            // Counter increment by one
                            collector.count(
                                RylvStr::from_static("parallel.mixed.counter"),
                                [RylvStr::from(tag.clone())],
                            );
                        }
                        2 => {
                            // Counter increment by value
                            collector.count_add(
                                RylvStr::from_static("parallel.mixed.counter_value"),
                                value,
                                [RylvStr::from(tag.clone())],
                            );
                        }
                        3 => {
                            // Gauge
                            collector.gauge(
                                RylvStr::from_static("parallel.mixed.gauge"),
                                value,
                                [RylvStr::from(tag.clone())],
                            );
                        }
                        _ => unreachable!(),
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_high_contention() {
    // Test high contention on the same metric name across threads
    let collector = Arc::new(create_test_collector());
    let num_threads = 16;
    let iterations_per_thread = 5_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    // All threads write to the same metric with same tags
                    // This creates high contention on the same hashmap entry
                    collector.histogram(
                        RylvStr::from_static("parallel.contention.same_metric"),
                        (thread_id * iterations_per_thread + i) as u64,
                        [
                            RylvStr::from_static("contention:high"),
                            RylvStr::from_static("test:stress"),
                        ],
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_many_unique_metrics() {
    // Test many unique metric names to stress the hashmap
    let collector = Arc::new(create_test_collector());
    let num_threads = 8;
    let unique_metrics_per_thread = 1_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..unique_metrics_per_thread {
                    let metric_name = format!("parallel.unique.metric.{}.{}", thread_id, i);
                    collector.histogram(
                        RylvStr::from(metric_name),
                        i as u64,
                        [RylvStr::from_static("unique:metric")],
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_with_macro() {
    // Test the histogram! macro under parallel stress
    let collector = Arc::new(create_test_collector());
    let num_threads = 8;
    let iterations_per_thread = 10_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let value = (thread_id * iterations_per_thread + i) as u64;

                    // Use macro with static tags
                    histogram!(
                        collector.as_ref(),
                        "macro.parallel.static",
                        value,
                        "thread:macro",
                        "test:parallel"
                    );

                    // Use macro with dynamic tags
                    histogram!(
                        collector.as_ref(),
                        "macro.parallel.dynamic",
                        value,
                        format!("thread:{}", thread_id),
                        format!("iter:{}", i % 100)
                    );

                    // Use macro with mixed tags
                    histogram!(
                        collector.as_ref(),
                        "macro.parallel.mixed",
                        value,
                        "static:tag",
                        format!("thread:{}", thread_id)
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_rapid_fire() {
    // Extremely fast, short bursts from many threads
    let collector = Arc::new(create_test_collector());
    let num_threads = 32;
    let iterations_per_thread = 1_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    collector.histogram(
                        RylvStr::from_static("parallel.rapid"),
                        i as u64,
                        [RylvStr::from(format!("thread:{}", thread_id))],
                    );
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_varying_tag_counts() {
    // Test with varying numbers of tags
    let collector = Arc::new(create_test_collector());
    let num_threads = 8;
    let iterations_per_thread = 5_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let value = i as u64;

                    match i % 5 {
                        0 => {
                            // No tags
                            let mut tags: [RylvStr<'_>; 0] = [];
                            collector.histogram(
                                RylvStr::from_static("parallel.tags.none"),
                                value,
                                &mut tags,
                            );
                        }
                        1 => {
                            // One tag
                            collector.histogram(
                                RylvStr::from_static("parallel.tags.one"),
                                value,
                                [RylvStr::from_static("tag:one")],
                            );
                        }
                        2 => {
                            // Two tags
                            collector.histogram(
                                RylvStr::from_static("parallel.tags.two"),
                                value,
                                [
                                    RylvStr::from_static("tag:one"),
                                    RylvStr::from_static("tag:two"),
                                ],
                            );
                        }
                        3 => {
                            // Three tags with mix
                            collector.histogram(
                                RylvStr::from_static("parallel.tags.three"),
                                value,
                                [
                                    RylvStr::from(String::from("tag:static")),
                                    RylvStr::from(format!("tag:dynamic{}", thread_id)),
                                    RylvStr::from(String::from("tag:mixed")),
                                ],
                            );
                        }
                        4 => {
                            // Many tags
                            collector.histogram(
                                RylvStr::from_static("parallel.tags.many"),
                                value,
                                [
                                    RylvStr::from(String::from("tag1:value1")),
                                    RylvStr::from(String::from("tag2:value2")),
                                    RylvStr::from(String::from("tag3:value3")),
                                    RylvStr::from(format!("tag4:thread{}", thread_id)),
                                    RylvStr::from(format!("tag5:iter{}", i)),
                                ],
                            );
                        }
                        _ => unreachable!(),
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}

#[test]
fn test_parallel_all_operations() {
    // Stress test all operations together
    let collector = Arc::new(create_test_collector());
    let num_threads = 12;
    let iterations_per_thread = 5_000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let collector = Arc::clone(&collector);
            thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let value = i as u64;
                    let thread_tag = format!("thread:{}", thread_id);

                    // Histogram
                    collector.histogram(
                        RylvStr::from_static("stress.histogram"),
                        value,
                        [RylvStr::from(thread_tag.clone())],
                    );

                    // Counter increment by one
                    collector.count(
                        RylvStr::from_static("stress.counter.one"),
                        [RylvStr::from(thread_tag.clone())],
                    );

                    // Counter increment by value
                    collector.count_add(
                        RylvStr::from_static("stress.counter.value"),
                        value,
                        [RylvStr::from(thread_tag.clone())],
                    );

                    // Gauge
                    collector.gauge(
                        RylvStr::from_static("stress.gauge"),
                        value,
                        [RylvStr::from(thread_tag.clone())],
                    );

                    // Use macro
                    histogram!(collector.as_ref(), "stress.macro", value, thread_tag);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    wait_and_shutdown(collector);
}
