#![cfg(any(feature = "shared-collector", feature = "tls-collector"))]

use rylv_metrics::{
    DrainMetricCollectorTrait, MetricDrain, MetricKind, MetricSuffix, RylvStr, RylvTag,
};

fn assert_compound_and_full_aggregate<C: DrainMetricCollectorTrait>(collector: C) {
    {
        let route = String::from("/users");
        collector.count_add(
            RylvStr::from_static("requests"),
            1,
            &mut [
                RylvTag::Compound(RylvStr::from_static("route"), RylvStr::from(route.as_str())),
                RylvTag::from_static("service:web"),
            ],
        );
    }

    collector.count_add(
        RylvStr::from_static("requests"),
        2,
        &mut [
            RylvTag::from_static("service:web"),
            RylvTag::from_static("route:/users"),
        ],
    );

    let tags = collector.prepare_sorted_tags([
        RylvTag::from_static("service:web"),
        RylvTag::Compound(
            RylvStr::from_static("route"),
            RylvStr::from(String::from("/users")),
        ),
    ]);
    let prepared = collector.prepare_metric(RylvStr::from_static("requests"), tags);
    collector.count_add_prepared(&prepared, 3);

    for _ in 0..8 {
        if let Some(mut drain) = collector.try_begin_drain() {
            let mut frames = drain.frames();
            let frame = frames.next().expect("one aggregated counter");
            assert_eq!(frame.metric, "requests");
            assert_eq!(frame.tags, "route:/users,service:web");
            assert_eq!(frame.value, 6);
            assert_eq!(frame.kind, MetricKind::Count);
            assert_eq!(frame.suffix, MetricSuffix::None);
            assert!(
                frames.next().is_none(),
                "equivalent tags must share one key"
            );
            return;
        }
    }
    panic!("unable to acquire drain ownership");
}

#[cfg(feature = "shared-collector")]
#[test]
fn shared_compound_and_full_tags_share_aggregate() {
    assert_compound_and_full_aggregate(rylv_metrics::SharedCollector::default());
}

#[cfg(feature = "tls-collector")]
#[test]
fn tls_compound_and_full_tags_share_aggregate() {
    assert_compound_and_full_aggregate(rylv_metrics::TLSCollector::new(
        rylv_metrics::TLSCollectorOptions::default(),
    ));
}
