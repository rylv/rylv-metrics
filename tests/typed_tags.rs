#![cfg(feature = "derive")]

use rylv_metrics::{RylvStr, RylvTag, RylvTags, RylvValue};
use std::{borrow::Cow, sync::Arc};

#[derive(RylvTags)]
struct Ordered<'a> {
    host: &'a str,
    host2: &'a str,
    a: &'a str,
    a_b: &'a str,
    #[rylv(rename = "a-b")]
    dash: &'a str,
    #[rylv(rename = "a.b")]
    dot: &'a str,
    #[rylv(rename = "a/b")]
    slash: &'a str,
}

#[test]
fn derived_order_matches_rylv_tag_ord_for_dynamic_values() {
    for value in ["", "x:y", "long-value"] {
        let tags = Ordered {
            host: value,
            host2: value,
            a: value,
            a_b: value,
            dash: value,
            dot: value,
            slash: value,
        };
        tags.__with_sorted_tags(|actual| {
            let mut expected = [
                ("host", value).into(),
                ("host2", value).into(),
                ("a", value).into(),
                ("a_b", value).into(),
                ("a-b", value).into(),
                ("a.b", value).into(),
                ("a/b", value).into(),
            ];
            expected.sort_unstable();
            assert_eq!(actual.len(), expected.len());
            for (a, b) in actual.iter().zip(expected.iter()) {
                assert_eq!(a, b);
            }
            assert_eq!(actual[0], RylvTag::from_parts("a-b", value));
            assert_eq!(actual[5], RylvTag::from_parts("host2", value));
        });
    }
}

#[derive(RylvTags)]
struct Generic<'a, T> {
    r#type: &'a str,
    value: T,
    #[rylv(skip)]
    ignored: usize,
}

#[derive(RylvTags)]
struct Empty {}

#[test]
fn generics_raw_identifiers_skipped_fields_and_empty_structs_work() {
    let tags = Generic {
        r#type: "kind",
        value: String::from("hello"),
        ignored: 123,
    };
    assert_eq!(tags.ignored, 123);
    tags.__with_sorted_tags(|slice| {
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0], RylvTag::from_static("type:kind"));
        assert_eq!(slice[1], RylvTag::from_static("value:hello"));
    });
    Empty {}.__with_sorted_tags(|slice| assert!(slice.is_empty()));
}

#[test]
fn all_supported_value_types_borrow_and_static_stays_static() {
    let borrowed = String::from("borrowed");
    let arc_str: Arc<str> = Arc::from("arc-str");
    let arc_string = Arc::new(String::from("arc-string"));
    let cow: Cow<'_, str> = Cow::Owned(String::from("cow"));
    assert!(matches!(
        RylvStr::from_static("static").__as_rylv_str(),
        RylvStr::Static("static")
    ));
    for value in [
        RylvStr::Borrowed("borrowed"),
        RylvStr::from(String::from("borrowed")),
        RylvStr::from(Arc::<str>::from("borrowed")),
    ] {
        assert!(matches!(
            value.__as_rylv_str(),
            RylvStr::Borrowed("borrowed")
        ));
    }
    assert_eq!((&"str").__as_rylv_str().as_ref(), "str");
    assert_eq!(borrowed.__as_rylv_str().as_ref(), "borrowed");
    assert_eq!(arc_str.__as_rylv_str().as_ref(), "arc-str");
    assert_eq!(arc_string.__as_rylv_str().as_ref(), "arc-string");
    assert_eq!(cow.__as_rylv_str().as_ref(), "cow");
    let reference = &borrowed;
    assert_eq!(RylvValue::__as_rylv_str(&reference).as_ref(), "borrowed");
}

#[cfg(any(feature = "shared-collector", feature = "tls-collector"))]
mod aggregation {
    use super::*;
    use rylv_metrics::{
        DrainMetricCollectorTrait, MetricDrain, MetricKind, MetricSuffix, MetricValue,
    };

    #[derive(RylvTags)]
    struct RequestTags<'a> {
        env: &'a str,
        #[rylv(rename = "http.route")]
        route: &'a str,
    }

    fn check<C: DrainMetricCollectorTrait>(collector: C) {
        let tags = RequestTags {
            env: "prod",
            route: "/users",
        };
        let full = || {
            [
                RylvTag::from_static("http.route:/users"),
                RylvTag::from_static("env:prod"),
            ]
        };
        let compound = || {
            [
                RylvTag::from_static_compound("env", "prod"),
                RylvTag::from_static_compound("http.route", "/users"),
            ]
        };
        let metric = RylvStr::from_static;

        collector.count_add(metric("count"), 1, &mut full());
        collector.count_add(metric("count"), 2, &mut compound());
        collector.count_add_typed(metric("count"), 3, &tags);
        collector.count_typed(metric("one"), &tags);

        collector.gauge_avg(metric("average"), 10, &mut full());
        collector.gauge_avg(metric("average"), 20, &mut compound());
        collector.gauge_avg_typed(metric("average"), 30, &tags);

        collector.gauge(metric("last"), 10, &mut full());
        collector.gauge(metric("last"), 20, &mut compound());
        collector.gauge_typed(metric("last"), 30, &tags);

        collector
            .histogram(metric("histogram"), 10, &mut full())
            .unwrap();
        collector
            .histogram(metric("histogram"), 20, &mut compound())
            .unwrap();
        collector
            .histogram_typed(metric("histogram"), 30, &tags)
            .unwrap();

        collector.timing(metric("timing"), 10, &mut full()).unwrap();
        collector
            .timing(metric("timing"), 20, &mut compound())
            .unwrap();
        collector.timing_typed(metric("timing"), 30, &tags).unwrap();

        let mut drain = collector.try_begin_drain().expect("drain available");
        let frames: Vec<_> = drain.frames().collect();
        assert!(frames
            .iter()
            .all(|frame| frame.tags == "env:prod,http.route:/users"));
        let exact = |name, suffix, kind, value| {
            let matching: Vec<_> = frames
                .iter()
                .filter(|frame| frame.metric == name && frame.suffix == suffix)
                .collect();
            assert_eq!(matching.len(), 1, "split series for {name}");
            assert_eq!(matching[0].kind, kind);
            assert_eq!(matching[0].value, value);
        };
        exact(
            "count",
            MetricSuffix::None,
            MetricKind::Count,
            MetricValue::Int(6),
        );
        exact(
            "one",
            MetricSuffix::None,
            MetricKind::Count,
            MetricValue::Int(1),
        );
        exact(
            "average",
            MetricSuffix::None,
            MetricKind::GaugeAvg,
            MetricValue::Float(20.0),
        );
        exact(
            "last",
            MetricSuffix::None,
            MetricKind::Gauge,
            MetricValue::Int(30),
        );
        exact(
            "histogram",
            MetricSuffix::Static(".count"),
            MetricKind::Count,
            MetricValue::Int(3),
        );
        exact(
            "timing",
            MetricSuffix::Static(".count"),
            MetricKind::Count,
            MetricValue::Int(3),
        );
    }

    fn rejected_values_preserve_error_semantics<C: DrainMetricCollectorTrait>(collector: C) {
        let tags = RequestTags {
            env: "prod",
            route: "/users",
        };
        assert!(collector
            .histogram_typed(RylvStr::from_static("histogram.error"), u64::MAX, &tags)
            .is_err());
        assert!(collector
            .timing_typed(RylvStr::from_static("timing.error"), u64::MAX, &tags)
            .is_err());
        let mut drain = collector.try_begin_drain().expect("drain available");
        assert!(drain.frames().next().is_none());
    }

    #[cfg(feature = "shared-collector")]
    #[test]
    fn shared_paths_aggregate_together() {
        check(rylv_metrics::SharedCollector::default());
    }

    #[cfg(feature = "shared-collector")]
    #[test]
    fn shared_reference_path_aggregates_together() {
        let collector = rylv_metrics::SharedCollector::default();
        check(&collector);
    }

    #[cfg(feature = "shared-collector")]
    #[test]
    fn shared_typed_histogram_errors_leave_no_series() {
        rejected_values_preserve_error_semantics(rylv_metrics::SharedCollector::default());
    }

    #[cfg(feature = "tls-collector")]
    #[test]
    fn tls_paths_aggregate_together() {
        check(rylv_metrics::TLSCollector::new(
            rylv_metrics::TLSCollectorOptions::default(),
        ));
    }

    #[cfg(feature = "tls-collector")]
    #[test]
    fn tls_reference_path_aggregates_together() {
        let collector =
            rylv_metrics::TLSCollector::new(rylv_metrics::TLSCollectorOptions::default());
        check(&collector);
    }

    #[cfg(feature = "tls-collector")]
    #[test]
    fn tls_typed_histogram_errors_leave_no_series() {
        rejected_values_preserve_error_semantics(rylv_metrics::TLSCollector::new(
            rylv_metrics::TLSCollectorOptions::default(),
        ));
    }
}
