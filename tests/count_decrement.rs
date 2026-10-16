#![cfg(any(feature = "shared-collector", feature = "tls-collector"))]

#[cfg(feature = "tls-collector")]
use rylv_metrics::MetricCollectorTrait;
use rylv_metrics::{
    count_dec, count_dec_prepared, count_dec_sorted, count_sub, count_sub_prepared,
    count_sub_sorted, DrainMetricCollectorTrait, MetricDrain, MetricKind, RylvStr, RylvTag,
};

fn check_decrements(collector: &impl DrainMetricCollectorTrait) {
    let name = RylvStr::from_static("balance");
    collector.count_add(
        name.clone(),
        3,
        &mut [RylvTag::from_static("b:2"), RylvTag::from_static("a:1")],
    );
    collector.count_sub(
        name.clone(),
        5,
        &mut [RylvTag::from_static("a:1"), RylvTag::from_static("b:2")],
    );
    collector.count_dec(
        name.clone(),
        &mut [RylvTag::from_static("a:1"), RylvTag::from_static("b:2")],
    );

    let sorted =
        collector.prepare_sorted_tags([RylvTag::from_static("b:2"), RylvTag::from_static("a:1")]);
    collector.count_sorted(name.clone(), &sorted);
    count_sub_sorted!(collector, "balance", 2, &sorted);
    count_dec_sorted!(collector, "balance", &sorted);

    let prepared = collector.prepare_metric(name, sorted);
    collector.count_add_prepared(&prepared, 8);
    count_sub_prepared!(collector, &prepared, 2);
    count_dec_prepared!(collector, &prepared);

    // Each subtraction stops at zero: 3 -> 0 -> 0 -> 1 -> 0 -> 0 -> 8 -> 6 -> 5.

    count_dec!(collector, "negative");
    count_sub!(collector, "negative", 4);
    count_sub!(collector, "zero", u64::MAX);
    collector.count_add(RylvStr::from_static("negative"), 2, &mut []);
    count_dec!(collector, "tagged", "b:2", "a:1");
    count_sub!(collector, "tagged", 2, "a:1", "b:2");
    collector.count_add(
        RylvStr::from_static("tagged"),
        3,
        &mut [RylvTag::from_static("b:2"), RylvTag::from_static("a:1")],
    );
    count_sub!(collector, "tagged", 2, "a:1", "b:2");
    collector.count_sub(RylvStr::from_static("large"), u64::MAX, &mut []);
    collector.count_add(RylvStr::from_static("large"), u64::MAX, &mut []);

    let mut drain = collector.try_begin_drain().unwrap();
    let mut frames: Vec<_> = drain
        .frames()
        .filter(|f| f.kind == MetricKind::Count)
        .collect();
    frames.sort_by_key(|f| f.metric);
    assert_eq!(frames.len(), 4);
    assert_eq!((frames[0].metric, frames[0].value), ("balance", 5));
    assert_eq!(
        (frames[1].metric, frames[1].value, frames[1].tags),
        ("large", u64::MAX, "")
    );
    assert_eq!(
        (frames[2].metric, frames[2].value, frames[2].tags),
        ("negative", 2, "")
    );
    assert_eq!(
        (frames[3].metric, frames[3].value, frames[3].tags),
        ("tagged", 1, "a:1,b:2")
    );
    drop(frames);
    drop(drain);
    assert_eq!(collector.try_begin_drain().unwrap().count_frames(), 0);
}

#[cfg(feature = "shared-collector")]
#[test]
fn shared_decrements_aggregate_and_drain() {
    check_decrements(&rylv_metrics::SharedCollector::default());
}

#[cfg(feature = "tls-collector")]
#[test]
fn tls_decrements_aggregate_and_drain() {
    check_decrements(&rylv_metrics::TLSCollector::new(
        rylv_metrics::TLSCollectorOptions::default(),
    ));
}

#[cfg(feature = "tls-collector")]
#[test]
fn tls_decrement_consumes_balance_recorded_by_another_thread() {
    let collector = std::sync::Arc::new(rylv_metrics::TLSCollector::new(
        rylv_metrics::TLSCollectorOptions::default(),
    ));
    let writer = std::sync::Arc::clone(&collector);
    std::thread::spawn(move || {
        writer.count_add(
            RylvStr::from_static("shared.balance"),
            5,
            &mut [RylvTag::from_static("a:1")],
        );
    })
    .join()
    .unwrap();

    let subtractor = std::sync::Arc::clone(&collector);
    std::thread::spawn(move || {
        subtractor.count_sub(
            RylvStr::from_static("shared.balance"),
            3,
            &mut [RylvTag::from_static("a:1")],
        );
    })
    .join()
    .unwrap();

    let mut drain = collector.try_begin_drain().unwrap();
    let frames: Vec<_> = drain.frames().collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].value, 2);
}
