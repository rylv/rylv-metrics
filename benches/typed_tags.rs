// Target-specific exceptions to the shared library lint policy.
#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rylv_metrics::{
    MetricCollectorTrait, RylvStr, RylvTag, RylvTags, SharedCollector, TLSCollector,
    TLSCollectorOptions,
};
use std::time::Duration;

#[derive(RylvTags)]
struct Tags4<'a> {
    a: RylvStr<'a>,
    b: RylvStr<'a>,
    c: RylvStr<'a>,
    d: RylvStr<'a>,
}

#[derive(RylvTags)]
struct OptionalTags4<'a> {
    a: Option<RylvStr<'a>>,
    b: Option<RylvStr<'a>>,
    c: Option<RylvStr<'a>>,
    d: Option<RylvStr<'a>>,
}

#[derive(RylvTags)]
struct Tags8<'a> {
    a: RylvStr<'a>,
    b: RylvStr<'a>,
    c: RylvStr<'a>,
    d: RylvStr<'a>,
    e: RylvStr<'a>,
    f: RylvStr<'a>,
    g: RylvStr<'a>,
    h: RylvStr<'a>,
}

fn make_tags<'a, const N: usize>(keys: &[&'static str; N], value: &'a str) -> [RylvTag<'a>; N] {
    std::array::from_fn(|index| RylvTag::from_parts(RylvStr::from_static(keys[index]), value))
}

fn bench_set<C: MetricCollectorTrait, T: RylvTags, const N: usize>(
    criterion: &mut Criterion,
    label: &str,
    collector: &C,
    ordered: &[&'static str; N],
    reversed: &[&'static str; N],
    typed: &T,
    value: &str,
) {
    let mut group = criterion.benchmark_group(label);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));
    group.sample_size(20);
    let sorted = collector.prepare_sorted_tags(
        ordered
            .iter()
            .map(|key| RylvTag::from_parts(RylvStr::from_static(key), value.to_owned())),
    );
    group.bench_function("slice_unsorted", |bench| {
        bench.iter(|| {
            let mut tags = make_tags(reversed, black_box(value));
            collector.count_add(
                RylvStr::from_static("bench.typed"),
                black_box(1),
                black_box(&mut tags),
            );
        });
    });
    group.bench_function("slice_ordered", |bench| {
        bench.iter(|| {
            let mut tags = make_tags(ordered, black_box(value));
            collector.count_add(
                RylvStr::from_static("bench.typed"),
                black_box(1),
                black_box(&mut tags),
            );
        });
    });
    group.bench_function("typed", |bench| {
        bench.iter(|| {
            collector.count_add_typed(
                RylvStr::from_static("bench.typed"),
                black_box(1),
                black_box(typed),
            );
        });
    });
    group.bench_function("sorted", |bench| {
        bench.iter(|| {
            collector.count_add_sorted(
                RylvStr::from_static("bench.typed"),
                black_box(1),
                black_box(&sorted),
            );
        });
    });
    group.finish();
}

fn bench_optional_set<C: MetricCollectorTrait>(
    criterion: &mut Criterion,
    label: &str,
    collector: &C,
    required: &Tags4<'_>,
    optional_all: &OptionalTags4<'_>,
    optional_half: &OptionalTags4<'_>,
) {
    let mut group = criterion.benchmark_group(label);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));
    group.sample_size(20);
    group.bench_function("required", |bench| {
        bench.iter(|| {
            collector.count_add_typed(
                RylvStr::from_static("bench.optional"),
                black_box(1),
                black_box(required),
            );
        });
    });
    group.bench_function("optional_all", |bench| {
        bench.iter(|| {
            collector.count_add_typed(
                RylvStr::from_static("bench.optional"),
                black_box(1),
                black_box(optional_all),
            );
        });
    });
    group.bench_function("optional_half", |bench| {
        bench.iter(|| {
            collector.count_add_typed(
                RylvStr::from_static("bench.optional"),
                black_box(1),
                black_box(optional_half),
            );
        });
    });
    group.finish();
}

fn benchmarks(criterion: &mut Criterion) {
    let value = String::from("dynamic-value");
    let tags4 = Tags4 {
        a: RylvStr::Borrowed(&value),
        b: RylvStr::Borrowed(&value),
        c: RylvStr::Borrowed(&value),
        d: RylvStr::Borrowed(&value),
    };
    let optional_all = OptionalTags4 {
        a: Some(RylvStr::Borrowed(&value)),
        b: Some(RylvStr::Borrowed(&value)),
        c: Some(RylvStr::Borrowed(&value)),
        d: Some(RylvStr::Borrowed(&value)),
    };
    let optional_half = OptionalTags4 {
        a: Some(RylvStr::Borrowed(&value)),
        b: None,
        c: Some(RylvStr::Borrowed(&value)),
        d: None,
    };
    {
        let mut guard_group = criterion.benchmark_group("guard/4");
        guard_group.warm_up_time(Duration::from_millis(100));
        guard_group.measurement_time(Duration::from_millis(500));
        guard_group.sample_size(20);
        guard_group.bench_function("required", |bench| {
            bench.iter(|| black_box(black_box(&tags4).sorted_tags()));
        });
        guard_group.bench_function("optional_all", |bench| {
            bench.iter(|| black_box(black_box(&optional_all).sorted_tags()));
        });
        guard_group.bench_function("optional_half", |bench| {
            bench.iter(|| black_box(black_box(&optional_half).sorted_tags()));
        });
        guard_group.finish();
    }
    let tags8 = Tags8 {
        a: RylvStr::Borrowed(&value),
        b: RylvStr::Borrowed(&value),
        c: RylvStr::Borrowed(&value),
        d: RylvStr::Borrowed(&value),
        e: RylvStr::Borrowed(&value),
        f: RylvStr::Borrowed(&value),
        g: RylvStr::Borrowed(&value),
        h: RylvStr::Borrowed(&value),
    };
    let shared = SharedCollector::default();
    let tls = TLSCollector::new(TLSCollectorOptions::default());
    let ordered4 = ["a", "b", "c", "d"];
    let reverse4 = ["d", "c", "b", "a"];
    let ordered8 = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let reverse8 = ["h", "g", "f", "e", "d", "c", "b", "a"];
    bench_set(
        criterion, "shared/4", &shared, &ordered4, &reverse4, &tags4, &value,
    );
    bench_set(
        criterion, "shared/8", &shared, &ordered8, &reverse8, &tags8, &value,
    );
    bench_set(
        criterion, "tls/4", &tls, &ordered4, &reverse4, &tags4, &value,
    );
    bench_set(
        criterion, "tls/8", &tls, &ordered8, &reverse8, &tags8, &value,
    );
    bench_optional_set(
        criterion,
        "shared/optional4",
        &shared,
        &tags4,
        &optional_all,
        &optional_half,
    );
    bench_optional_set(
        criterion,
        "tls/optional4",
        &tls,
        &tags4,
        &optional_all,
        &optional_half,
    );
}

criterion_group!(typed_tags, benchmarks);
criterion_main!(typed_tags);
