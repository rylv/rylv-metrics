// Target-specific exceptions to the shared library lint policy.
#![allow(
    clippy::elidable_lifetime_names,
    clippy::expect_used,
    clippy::needless_pass_by_value,
    clippy::struct_field_names,
    clippy::unwrap_used,
    missing_docs
)]
#![cfg(feature = "derive")]

use proptest::prelude::*;
use rylv_metrics::{RylvStr, RylvTag, RylvTags, RylvTagsGuard};
use std::sync::Arc;

fn arbitrary_value<'a>(variant: u8, value: &'a str) -> RylvStr<'a> {
    match variant {
        0 => RylvStr::from_static("prod"),
        1 => RylvStr::Borrowed(value),
        2 => RylvStr::from(value.to_owned()),
        _ => RylvStr::from(Arc::<str>::from(value)),
    }
}

fn arbitrary_values() -> impl Strategy<Value = Vec<(u8, String)>> {
    proptest::collection::vec(
        (
            0_u8..4,
            proptest::collection::vec(any::<char>(), 0..96)
                .prop_map(|chars| chars.into_iter().collect()),
        ),
        7,
    )
}

struct Manual<'a> {
    env: &'a str,
}

impl RylvTags for Manual<'_> {
    type Guard<'a>
        = RylvTagsGuard<'a, 1>
    where
        Self: 'a;

    fn sorted_tags(&self) -> Self::Guard<'_> {
        RylvTagsGuard::new([RylvTag::from_parts(RylvStr::from_static("env"), self.env)])
    }
}

#[test]
fn manual_implementation_returns_guard() {
    let tags = Manual { env: "prod" };
    assert_eq!(
        tags.sorted_tags().as_ref()[0],
        RylvTag::from_static("env:prod")
    );
}

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
        let guard = tags.sorted_tags();
        let actual = guard.as_ref();
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
    }
}

proptest! {
    #[test]
    fn derived_order_matches_runtime_sort_for_independent_values(fields in arbitrary_values()) {
        let values: Vec<_> = fields.iter().map(|(variant, value)| {
            if *variant == 0 { "prod" } else { value.as_str() }
        }).collect();
        let tags = Ordered {
            host: arbitrary_value(fields[0].0, &fields[0].1),
            host2: arbitrary_value(fields[1].0, &fields[1].1),
            a: arbitrary_value(fields[2].0, &fields[2].1),
            a_b: arbitrary_value(fields[3].0, &fields[3].1),
            dash: arbitrary_value(fields[4].0, &fields[4].1),
            dot: arbitrary_value(fields[5].0, &fields[5].1),
            slash: arbitrary_value(fields[6].0, &fields[6].1),
        };
        let guard = tags.sorted_tags();
        let mut expected: Vec<_> = [
            ("host", values[0]),
            ("host2", values[1]),
            ("a", values[2]),
            ("a_b", values[3]),
            ("a-b", values[4]),
            ("a.b", values[5]),
            ("a/b", values[6]),
        ]
        .into_iter()
        .map(|(key, value)| RylvTag::from(format!("{key}:{value}")))
        .collect();
        expected.sort_unstable();
        prop_assert_eq!(guard.as_ref(), expected.as_slice());
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
fn generated_guard_lifetime_avoids_existing_parameters() {
    #[derive(RylvTags)]
    struct Tags<'__rylv_guard, '__rylv_guard_, '__rylv_guard__> {
        first: RylvStr<'__rylv_guard>,
        second: RylvStr<'__rylv_guard_>,
        third: RylvStr<'__rylv_guard__>,
    }
    let value = String::from("dynamic");
    let tags = Tags {
        first: RylvStr::Borrowed(&value),
        second: RylvStr::Borrowed(&value),
        third: RylvStr::Borrowed(&value),
    };
    let guard = tags.sorted_tags();
    assert_eq!(
        guard.as_ref(),
        &[
            RylvTag::from_static("first:dynamic"),
            RylvTag::from_static("second:dynamic"),
            RylvTag::from_static("third:dynamic"),
        ]
    );
}

#[test]
fn generics_raw_identifiers_skipped_fields_and_empty_structs_work() {
    let tags = Generic {
        r#type: RylvStr::from_static("kind"),
        value: RylvStr::from(String::from("hello")),
        ignored: 123,
    };
    assert_eq!(tags.ignored, 123);
    let guard = tags.sorted_tags();
    let slice = guard.as_ref();
    assert_eq!(slice.len(), 2);
    assert_eq!(slice[0], RylvTag::from_static("type:kind"));
    assert_eq!(slice[1], RylvTag::from_static("value:hello"));
    assert!(Empty {}.sorted_tags().as_ref().is_empty());
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
    let guard = tags.sorted_tags();
    let tags = guard.as_ref();
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
