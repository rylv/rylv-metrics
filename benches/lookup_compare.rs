use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rylv_metrics::__bench::CompareFixture;

fn compare_tags_old(compare: &[&str], joined_tags: &str, tag_count: usize) -> bool {
    if tag_count != compare.len() {
        return false;
    }
    if compare.is_empty() {
        return joined_tags.is_empty();
    }

    let joined = joined_tags.as_bytes();
    let mut offset = 0usize;
    let last_index = compare.len() - 1;
    for (index, tag) in compare.iter().enumerate() {
        let tag_bytes = tag.as_bytes();
        let next_offset = offset + tag_bytes.len();
        if next_offset > joined.len() || joined[offset..next_offset] != *tag_bytes {
            return false;
        }
        offset = next_offset;

        if index < last_index {
            if offset >= joined.len() || joined[offset] != b',' {
                return false;
            }
            offset += 1;
        }
    }

    offset == joined.len()
}

fn expected_joined_len(tags: &[&str]) -> usize {
    if tags.is_empty() {
        return 0;
    }
    tags.iter().map(|tag| tag.len()).sum::<usize>() + tags.len() - 1
}

fn compare_tags_new(compare: &[&str], joined_tags: &str, tag_count: usize) -> bool {
    if tag_count != compare.len() {
        return false;
    }
    if joined_tags.len() != expected_joined_len(compare) {
        return false;
    }
    compare_tags_old(compare, joined_tags, tag_count)
}

fn benchmark_compare_algorithms(c: &mut Criterion) {
    let tags = ["tag:value", "env:prod", "service:api"];
    let joined_match = "tag:value,env:prod,service:api";
    let joined_len_miss = "tag:value,env:prodx,service:api";
    let joined_content_miss_same_len = "tag:value,env:stag,service:api";

    c.bench_function("alg_old_match", |b| {
        b.iter(|| black_box(compare_tags_old(black_box(&tags), black_box(joined_match), 3)));
    });
    c.bench_function("alg_new_match", |b| {
        b.iter(|| black_box(compare_tags_new(black_box(&tags), black_box(joined_match), 3)));
    });

    c.bench_function("alg_old_miss_joined_len", |b| {
        b.iter(|| black_box(compare_tags_old(black_box(&tags), black_box(joined_len_miss), 3)));
    });
    c.bench_function("alg_new_miss_joined_len", |b| {
        b.iter(|| black_box(compare_tags_new(black_box(&tags), black_box(joined_len_miss), 3)));
    });

    c.bench_function("alg_old_miss_same_len_diff_content", |b| {
        b.iter(|| {
            black_box(compare_tags_old(
                black_box(&tags),
                black_box(joined_content_miss_same_len),
                3,
            ))
        });
    });
    c.bench_function("alg_new_miss_same_len_diff_content", |b| {
        b.iter(|| {
            black_box(compare_tags_new(
                black_box(&tags),
                black_box(joined_content_miss_same_len),
                3,
            ))
        });
    });
}

fn benchmark_lookup_compare(c: &mut Criterion) {
    let hash = 42;

    let match_fixture = CompareFixture::new(
        "metric.lookup",
        &["tag:value", "env:prod"],
        &["tag:value", "env:prod"],
        hash,
    );
    c.bench_function("lookup_compare_match_2_tags", |b| {
        b.iter(|| black_box(match_fixture.compare()));
    });

    let miss_count_fixture = CompareFixture::new(
        "metric.lookup",
        &["tag:value", "env:prod"],
        &["tag:value"],
        hash,
    );
    c.bench_function("lookup_compare_miss_tag_count", |b| {
        b.iter(|| black_box(miss_count_fixture.compare()));
    });

    let miss_len_fixture = CompareFixture::new(
        "metric.lookup",
        &["tag:value", "env:prod"],
        &["tag:value", "env:prodxx"],
        hash,
    );
    c.bench_function("lookup_compare_miss_joined_len", |b| {
        b.iter(|| black_box(miss_len_fixture.compare()));
    });

    let miss_content_fixture = CompareFixture::new(
        "metric.lookup",
        &["tag:value", "env:prod"],
        &["tag:value", "env:stag"],
        hash,
    );
    c.bench_function("lookup_compare_miss_same_len_diff_content", |b| {
        b.iter(|| black_box(miss_content_fixture.compare()));
    });
}

criterion_group!(benches, benchmark_lookup_compare, benchmark_compare_algorithms);
criterion_main!(benches);
