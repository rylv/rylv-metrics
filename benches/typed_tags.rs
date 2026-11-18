use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rylv_metrics::{
    MetricCollectorTrait, RylvStr, RylvTag, RylvTags, SharedCollector, TLSCollector,
    TLSCollectorOptions,
};
use std::time::Duration;

#[derive(RylvTags)]
struct Tags4<'a> {
    a: &'a str,
    b: &'a str,
    c: &'a str,
    d: &'a str,
}

#[derive(RylvTags)]
struct Tags8<'a> {
    a: &'a str,
    b: &'a str,
    c: &'a str,
    d: &'a str,
    e: &'a str,
    f: &'a str,
    g: &'a str,
    h: &'a str,
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

fn benchmarks(criterion: &mut Criterion) {
    let value = String::from("dynamic-value");
    let tags4 = Tags4 {
        a: &value,
        b: &value,
        c: &value,
        d: &value,
    };
    let tags8 = Tags8 {
        a: &value,
        b: &value,
        c: &value,
        d: &value,
        e: &value,
        f: &value,
        g: &value,
        h: &value,
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
}

criterion_group!(typed_tags, benchmarks);
criterion_main!(typed_tags);
