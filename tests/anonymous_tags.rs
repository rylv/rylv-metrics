#![allow(missing_docs, clippy::expect_used)]
#![cfg(feature = "derive")]

use rylv_metrics::{rylv_tags, RylvStr, RylvTag, RylvTags};

#[cfg(feature = "shared-collector")]
#[test]
fn anonymous_and_named_tags_aggregate_together() {
    use rylv_metrics::{
        DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, MetricValue, SharedCollector,
    };

    #[derive(RylvTags)]
    struct Named<'a> {
        env: RylvStr<'a>,
    }
    let collector = SharedCollector::default();
    collector.count_typed(
        RylvStr::from_static("requests"),
        &Named {
            env: RylvStr::from_static("prod"),
        },
    );
    collector.count_typed(
        RylvStr::from_static("requests"),
        &rylv_tags! {
            region?: None,
            env: RylvStr::from_static("prod"),
        },
    );
    let mut drain = collector.try_begin_drain().expect("drain available");
    let frames: Vec<_> = drain.frames().collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].tags, "env:prod");
    assert_eq!(frames[0].value, MetricValue::Int(2));
}

#[test]
fn anonymous_tags_sort_and_omit_optional_values() {
    let route = String::from("/users");
    let tags = rylv_tags! {
        z: RylvStr::from_static("last"),
        region?: None,
        #[rylv(rename = "http-route")]
        route: RylvStr::Borrowed(&route),
        empty?: Some(RylvStr::from_static("")),
    };
    assert_eq!(
        tags.sorted_tags().as_ref(),
        &[
            RylvTag::from_static("empty:"),
            RylvTag::from_static("http-route:/users"),
            RylvTag::from_static("z:last"),
        ]
    );
}

#[test]
fn expressions_are_evaluated_once_in_declaration_order() {
    let mut seen = Vec::new();
    let tags = rylv_tags! {
        z: { seen.push(1); RylvStr::from_static("z") },
        a?: { seen.push(2); Some(RylvStr::from_static("a")) },
        b: { seen.push(3); RylvStr::from(String::from("b")) },
    };
    assert_eq!(seen, [1, 2, 3]);
    assert_eq!(tags.sorted_tags().as_ref().len(), 3);
}

#[test]
fn empty_repeated_and_raw_identifier_sets_work() {
    assert!(rylv_tags! {}.sorted_tags().as_ref().is_empty());
    let first = rylv_tags! { r#type: RylvStr::from_static("one") };
    let second = rylv_tags! { r#type: RylvStr::from_static("two") };
    assert_eq!(
        first.sorted_tags().as_ref(),
        &[RylvTag::from_static("type:one")]
    );
    assert_eq!(
        second.sorted_tags().as_ref(),
        &[RylvTag::from_static("type:two")]
    );
}

#[test]
fn owned_values_and_static_variants_survive_the_block() {
    let tags = rylv_tags! {
        owned: RylvStr::from(String::from("dynamic")),
        static_value: RylvStr::from_static("prod"),
    };
    let guard = tags.sorted_tags();
    assert!(matches!(
        guard.as_ref()[0],
        RylvTag::Compound(_, RylvStr::Borrowed("dynamic"))
    ));
    assert!(matches!(
        guard.as_ref()[1],
        RylvTag::Compound(_, RylvStr::Static("prod"))
    ));
}

#[test]
fn generated_names_do_not_capture_caller_expressions() {
    #[allow(non_snake_case)]
    let __RylvAnonymousTags = RylvStr::from_static("prod");
    let __rylv_field_0 = RylvStr::from_static("us");
    let tags = rylv_tags! {
        env: __RylvAnonymousTags,
        region: __rylv_field_0,
    };
    assert_eq!(
        tags.sorted_tags().as_ref(),
        &[
            RylvTag::from_static("env:prod"),
            RylvTag::from_static("region:us"),
        ]
    );
}

#[test]
fn caller_constants_do_not_capture_generated_bindings() {
    #[allow(non_upper_case_globals)]
    const __rylv_field_0: u8 = 1;
    #[allow(non_upper_case_globals)]
    const __rylv_values: u8 = 2;
    let tags = rylv_tags! {
        env: RylvStr::from_static("prod"),
        region?: Some(RylvStr::from_static("us")),
    };
    assert_eq!(__rylv_field_0 + __rylv_values, 3);
    assert_eq!(tags.sorted_tags().as_ref().len(), 2);
}

#[cfg(feature = "shared-collector")]
#[test]
fn borrowed_temporaries_survive_the_collector_call() {
    use rylv_metrics::{
        DrainMetricCollectorTrait, MetricCollectorTrait, MetricDrain, SharedCollector,
    };
    let collector = SharedCollector::default();
    collector.count_typed(
        RylvStr::from_static("requests"),
        &rylv_tags! {
            env: RylvStr::Borrowed(&format!("env{}", 1)),
            region?: Some(RylvStr::Borrowed(&format!("region{}", 2))),
        },
    );
    let mut drain = collector.try_begin_drain().expect("drain available");
    let frames: Vec<_> = drain.frames().collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].tags, "env:env1,region:region2");
}

#[test]
fn caller_scope_and_control_flow_are_preserved() -> Result<(), &'static str> {
    struct Caller;
    impl Caller {
        fn value(fail: bool) -> Result<RylvStr<'static>, &'static str> {
            if fail {
                return Err("unavailable");
            }
            Ok(RylvStr::from_static("prod"))
        }
        fn tags(fail: bool) -> Result<impl RylvTags, &'static str> {
            Ok(rylv_tags! { env: Self::value(fail)? })
        }
    }
    assert_eq!(Caller::tags(false)?.sorted_tags().as_ref().len(), 1);
    assert_eq!(Caller::tags(true).err(), Some("unavailable"));
    Ok(())
}
