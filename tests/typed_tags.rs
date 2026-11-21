#![cfg(feature = "derive")]

use rylv_metrics::{RylvStr, RylvTag, RylvTags};

#[derive(RylvTags)]
struct Ordered<'a> {
    host: RylvStr<'a>,
    host2: RylvStr<'a>,
    a: RylvStr<'a>,
    a_b: RylvStr<'a>,
    #[rylv(rename = "a-b")]
    dash: RylvStr<'a>,
    #[rylv(rename = "a.b")]
    dot: RylvStr<'a>,
    #[rylv(rename = "a/b")]
    slash: RylvStr<'a>,
}

#[test]
fn derived_order_matches_rylv_tag_ord_for_dynamic_values() {
    for value in ["", "x:y", "long-value"] {
        let tags = Ordered {
            host: RylvStr::Borrowed(value),
            host2: RylvStr::Borrowed(value),
            a: RylvStr::Borrowed(value),
            a_b: RylvStr::Borrowed(value),
            dash: RylvStr::Borrowed(value),
            dot: RylvStr::Borrowed(value),
            slash: RylvStr::Borrowed(value),
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
    r#type: RylvStr<'a>,
    value: RylvStr<'a>,
    #[rylv(skip)]
    ignored: T,
}

#[derive(RylvTags)]
struct Empty {}

#[test]
fn generics_raw_identifiers_skipped_fields_and_empty_structs_work() {
    let tags = Generic {
        r#type: RylvStr::from_static("kind"),
        value: RylvStr::from(String::from("hello")),
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
fn derived_values_keep_static_and_borrow_dynamic() {
    #[derive(RylvTags)]
    struct Values<'a> {
        static_value: RylvStr<'a>,
        borrowed_value: RylvStr<'a>,
        owned_value: RylvStr<'a>,
    }
    let dynamic = String::from("dynamic");
    let tags = Values {
        static_value: RylvStr::from_static("prod"),
        borrowed_value: RylvStr::Borrowed(&dynamic),
        owned_value: RylvStr::from(String::from("owned")),
    };
    tags.__with_sorted_tags(|tags| {
        assert!(matches!(
            tags[0],
            RylvTag::Compound(_, RylvStr::Borrowed("dynamic"))
        ));
        assert!(matches!(
            tags[1],
            RylvTag::Compound(_, RylvStr::Borrowed("owned"))
        ));
        assert!(matches!(
            tags[2],
            RylvTag::Compound(_, RylvStr::Static("prod"))
        ));
    });
}

#[cfg(any(feature = "shared-collector", feature = "tls-collector"))]
mod aggregation {
    use super::*;
    use rylv_metrics::{
        DrainMetricCollectorTrait, MetricDrain, MetricKind, MetricSuffix, MetricValue,
    };

    #[derive(RylvTags)]
    struct RequestTags<'a> {
        env: RylvStr<'a>,
        #[rylv(rename = "http.route")]
        route: RylvStr<'a>,
    }

    fn check<C: DrainMetricCollectorTrait>(collector: C) {
        let tags = RequestTags {
            env: RylvStr::from_static("prod"),
            route: RylvStr::from_static("/users"),
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
            env: RylvStr::from_static("prod"),
            route: RylvStr::from_static("/users"),
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
