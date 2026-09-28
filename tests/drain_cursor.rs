#![cfg(any(feature = "shared-collector", feature = "tls-collector"))]

use rylv_metrics::{
    DrainMetricCollectorTrait, MetricDrain, MetricFrameRef, MetricKind, MetricSuffix, RylvStr,
    RylvTag,
};

fn retained_frames_share_storage(collector: &impl DrainMetricCollectorTrait) {
    let names = ["counter", "average", "latest", "histogram", "timing"];
    let prepared = names.map(|name| {
        collector.prepare_metric(
            RylvStr::from(name.to_owned()),
            collector.prepare_sorted_tags([RylvTag::from("env:owned".to_owned())]),
        )
    });
    let pointers = prepared.each_ref().map(|key| {
        (
            key.metric().as_ref().as_ptr(),
            key.tags().joined_tags().as_ptr(),
        )
    });
    collector.count_add_prepared(&prepared[0], 1);
    collector.gauge_avg_prepared(&prepared[1], 2);
    collector.gauge_prepared(&prepared[2], 3);
    collector.histogram_prepared(&prepared[3], 4);
    collector.timing_prepared(&prepared[4], 5);
    drop(prepared); // The aggregator must now keep the owned strings alive.

    let mut drain = collector.try_begin_drain().unwrap();
    let frames: Vec<_> = drain.frames().collect(); // Cursor is destroyed here.

    // A separate generation can be recorded and recycled while these frames live.
    collector.count(RylvStr::from("next_generation".to_owned()), &mut []);
    let mut next = collector.try_begin_drain().unwrap();
    assert!(next.frames().any(|frame| frame.metric == "next_generation"));
    drop(next);

    for (index, name) in names.iter().enumerate() {
        let matching: Vec<_> = frames
            .iter()
            .filter(|frame| frame.metric == *name)
            .collect();
        assert!(!matching.is_empty(), "missing {name}");
        for frame in matching {
            assert_eq!(frame.tags, "env:owned");
            assert_eq!(frame.metric.as_ptr(), pointers[index].0, "name was copied");
            assert_eq!(frame.tags.as_ptr(), pointers[index].1, "tags were copied");
        }
    }
    assert_eq!(
        frames.iter().find(|f| f.metric == "counter").unwrap().value,
        1
    );
    assert_eq!(
        frames.iter().find(|f| f.metric == "average").unwrap().value,
        2
    );
    assert_eq!(
        frames.iter().find(|f| f.metric == "latest").unwrap().value,
        3
    );
    drop(frames);

    // Reborrowing after frames are released can remove exhausted keys on Drop.
    assert_eq!(drain.count_frames(), 0);
    drop(drain);
    for _ in 0..3 {
        assert_eq!(collector.try_begin_drain().unwrap().count_frames(), 0);
    }
}

fn retained_frame_survives_partial_cursor(collector: &impl DrainMetricCollectorTrait) {
    collector.histogram(
        RylvStr::from("owned_histogram".to_owned()),
        42,
        &mut [RylvTag::from("owned:tag".to_owned())],
    );
    let mut drain = collector.try_begin_drain().unwrap();
    let first = drain.frames().next().unwrap(); // Pending histogram guard is dropped.
    assert_eq!(first.metric, "owned_histogram");
    assert_eq!(first.tags, "owned:tag");
    assert_eq!(first.value, 1); // Histogram count is emitted first.
    drop(drain);
    collector.count(RylvStr::from_static("after_drop"), &mut []);
    assert!(collector
        .try_begin_drain()
        .unwrap()
        .frames()
        .any(|f| f.metric == "after_drop"));
}

#[cfg(feature = "shared-collector")]
#[test]
fn shared_frames_outlive_cursor_without_copying_strings() {
    retained_frames_share_storage(&rylv_metrics::SharedCollector::default());
}

#[cfg(feature = "tls-collector")]
#[test]
fn tls_frames_outlive_cursor_without_copying_strings() {
    retained_frames_share_storage(&rylv_metrics::TLSCollector::new(
        rylv_metrics::TLSCollectorOptions::default(),
    ));
}

#[cfg(feature = "shared-collector")]
#[test]
fn shared_frame_outlives_partial_cursor() {
    retained_frame_survives_partial_cursor(&rylv_metrics::SharedCollector::default());
}

#[cfg(feature = "tls-collector")]
#[test]
fn tls_frame_outlives_partial_cursor() {
    retained_frame_survives_partial_cursor(&rylv_metrics::TLSCollector::new(
        rylv_metrics::TLSCollectorOptions::default(),
    ));
}

// A safe external implementation can use existing owned storage and a standard
// borrowed iterator. No temporary String is needed in the cursor.
struct ExternalDrain([String; 2]);

fn frame_from_string(name: &String) -> MetricFrameRef<'_> {
    MetricFrameRef {
        prefix: "",
        metric: name,
        tags: "",
        value: 1,
        kind: MetricKind::Count,
        suffix: MetricSuffix::None,
    }
}

impl MetricDrain for ExternalDrain {
    type Cursor<'a> =
        std::iter::Map<std::slice::Iter<'a, String>, fn(&'a String) -> MetricFrameRef<'a>>;

    fn frames(&mut self) -> Self::Cursor<'_> {
        self.0.iter().map(frame_from_string)
    }
}

#[test]
fn external_drain_keeps_first_frame_valid_after_advancing_cursor() {
    let mut drain = ExternalDrain(["first".to_owned(), "other".to_owned()]);
    let mut cursor = drain.frames();
    let first = cursor.next().unwrap();
    let other = cursor.next().unwrap();
    assert!(cursor.next().is_none());
    drop(cursor);
    assert_eq!(first.metric, "first");
    assert_eq!(other.metric, "other");
}

#[cfg(all(feature = "tls-collector", feature = "allocationcounter"))]
#[test]
fn tls_cursor_over_live_entries_does_not_allocate() {
    let collector = rylv_metrics::TLSCollector::new(rylv_metrics::TLSCollectorOptions::default());
    use rylv_metrics::MetricCollectorTrait;
    collector.count(RylvStr::from("owned_counter".to_owned()), &mut []);
    let mut drain = collector.try_begin_drain().unwrap();
    let measured = allocation_counter::measure(|| {
        let first = {
            let mut cursor = drain.frames();
            let first = cursor.next().unwrap();
            assert!(cursor.next().is_none());
            first
        };
        assert_eq!(first.metric, "owned_counter");
    });
    assert_eq!(measured.count_total, 0);
}
