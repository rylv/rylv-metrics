use crate::dogstats::slice_utils::equal_slice;
use crate::dogstats::{resolve_tags, RylvStr, RylvTag};
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Fast secondary fingerprint over metric + joined tags.
#[must_use]
#[inline]
pub fn metric_tags_fingerprint(metric: &str, joined_tags: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let mut hash = OFFSET;
    for b in metric.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    // Delimiter between metric and tags to avoid ambiguity.
    hash ^= u64::from(b'|');
    hash = hash.wrapping_mul(PRIME);
    for b in joined_tags.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Fast secondary fingerprint over metric + sorted tags slice.
#[must_use]
#[inline]
pub fn metric_tags_fingerprint_from_tags(metric: &str, tags: &[RylvTag<'_>]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let mut hash = OFFSET;
    for b in metric.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash ^= u64::from(b'|');
    hash = hash.wrapping_mul(PRIME);

    let mut iter = tags.iter();
    if let Some(tag) = iter.next() {
        hash = hash_tag::<PRIME>(hash, tag);
    }
    for tag in iter {
        hash ^= u64::from(b',');
        hash = hash.wrapping_mul(PRIME);

        hash = hash_tag::<PRIME>(hash, tag);
    }
    hash
}

fn hash_tag<const PRIME: u64>(mut hash: u64, tag: &RylvTag) -> u64 {
    match tag {
        RylvTag::Full(tag) => {
            for b in tag.as_ref().as_bytes() {
                hash ^= u64::from(*b);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        RylvTag::Compound(k, v) => {
            for b in k.as_ref().as_bytes() {
                hash ^= u64::from(*b);
                hash = hash.wrapping_mul(PRIME);
            }

            hash ^= u64::from(':');
            hash = hash.wrapping_mul(PRIME);

            for b in v.as_ref().as_bytes() {
                hash ^= u64::from(*b);
                hash = hash.wrapping_mul(PRIME);
            }
        }
    }
    hash
}

/// Compute a hash over a sorted tag slice.
#[must_use]
#[inline]
pub fn hash_tags<S: BuildHasher>(hasher_builder: &S, tags: &[RylvTag<'_>]) -> u64 {
    let mut hasher = hasher_builder.build_hasher();
    for tag in tags {
        match tag {
            RylvTag::Full(t) => {
                t.as_ref().hash(&mut hasher);
            }
            RylvTag::Compound(key, value) => {
                key.as_ref().hash(&mut hasher);
                ':'.hash(&mut hasher);
                value.as_ref().hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Combine a metric name with a precomputed tags hash into a single lookup hash.
#[must_use]
#[inline]
pub fn combine_metric_tags_hash<S: BuildHasher>(
    hasher_builder: &S,
    metric: &str,
    tags_hash: u64,
) -> u64 {
    let mut hasher = hasher_builder.build_hasher();
    metric.hash(&mut hasher);
    tags_hash.hash(&mut hasher);
    hasher.finish()
}

/// Pre-sorted reusable tag set for hot paths.
///
/// Build once and reuse across many metric calls to avoid per-call tag sorting
/// and hashing. The tags hash is precomputed at construction time.
#[derive(Clone, Debug)]
pub struct SortedTags<S: BuildHasher + Clone> {
    tags: Box<[RylvTag<'static>]>,
    joined_tags: Arc<str>,
    tags_hash: u64,
    id: u64,
    _hasher: PhantomData<S>,
}

impl<S: BuildHasher + Clone> Eq for SortedTags<S> {}
impl<S: BuildHasher + Clone> PartialEq for SortedTags<S> {
    fn eq(&self, other: &Self) -> bool {
        // Can exists mutiples equals sorted tags but with different ids
        self.id == other.id
            || self.tags_hash == other.tags_hash
                && equal_slice(
                    self.joined_tags().as_bytes(),
                    other.joined_tags().as_bytes(),
                )
    }
}

impl<S: BuildHasher + Clone> SortedTags<S> {
    /// Builds a `SortedTags` from any tag iterator.
    ///
    /// Tags are resolved (compound tags are joined), converted to owned/static form,
    /// sorted, joined, and hashed once.
    #[must_use]
    pub fn new<'a, I>(tags: I, hasher_builder: &S) -> Self
    where
        I: IntoIterator<Item = RylvTag<'a>>,
    {
        let mut tags_vec: Vec<RylvTag<'static>> = resolve_tags(tags);
        tags_vec.sort_unstable();

        let joined_tags = build_joined_tags(&tags_vec);

        let tags_hash = hash_tags(hasher_builder, &tags_vec);

        Self {
            // TODO: Why not leave the vec here, for size reduction?
            tags: tags_vec.into_boxed_slice(),
            joined_tags,
            tags_hash,
            id: next_sorted_tag_id(),
            _hasher: PhantomData,
        }
    }

    /// Builds `SortedTags` from already-sorted tags and a precomputed tags hash.
    pub(crate) fn from_sorted_tags_with_hash(tags: &[RylvTag<'_>], tags_hash: u64) -> Self {
        let tags_vec: Vec<RylvTag<'static>> = resolve_tags(tags.iter().cloned());
        let joined_tags = build_joined_tags(&tags_vec);

        Self {
            // TODO: Why not leave the vec here, for size reduction?
            tags: tags_vec.into_boxed_slice(),
            joined_tags,
            tags_hash,
            id: next_sorted_tag_id(),
            _hasher: PhantomData,
        }
    }

    /// Returns the precomputed tags hash.
    #[must_use]
    pub const fn tags_hash(&self) -> u64 {
        self.tags_hash
    }

    /// Returns sorted tags.
    #[must_use]
    pub fn tags(&self) -> &[RylvTag<'static>] {
        &self.tags
    }

    /// Returns pre-joined tags in `DogStatsD` format.
    #[must_use]
    pub fn joined_tags(&self) -> &str {
        &self.joined_tags
    }

    /// Number of tags.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Returns `true` when no tags are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }
}

fn build_joined_tags(tags_vec: &[RylvTag<'static>]) -> Arc<str> {
    if tags_vec.is_empty() {
        return Arc::<str>::from("");
    }

    let joined_len = tags_vec.iter().map(super::RylvTag::len).sum::<usize>() + tags_vec.len() - 1;
    let mut buffer = String::with_capacity(joined_len);
    let mut iter = tags_vec.iter();
    if let Some(tag) = iter.next() {
        tag.push_tag(&mut buffer);
    }
    for tag in iter {
        buffer.push(',');
        tag.push_tag(&mut buffer);
    }
    Arc::<str>::from(buffer)
}

/// Collector-bound precomputed metric key for hot paths.
///
/// Use collector `prepare_metric(...)` APIs to build this once and then call
/// `*_prepared` methods to skip per-call hash recomputation.
///
/// In heavily concurrent workloads:
/// - `MetricCollector::new(..., TLSCollector::new(TLSCollectorOptions::default()))`: `*_prepared` is typically beneficial.
/// - `MetricCollector::new(..., SharedCollector::new(SharedCollectorOptions::default()))`: `*_prepared` may add contention on shared state; prefer
///   `SortedTags` + `*_sorted` for cross-thread shared collectors.
pub struct PreparedMetric<S: BuildHasher + Clone> {
    metric: RylvStr<'static>,
    tags: SortedTags<S>,
    hash: u64,
    fingerprint: u64,
    prepared_id: u64,
}

impl<S: BuildHasher + Clone> PreparedMetric<S> {
    /// Creates a prepared metric key.
    #[must_use]
    pub fn new(metric: RylvStr<'static>, tags: SortedTags<S>, hash: u64) -> Self {
        let prepared_id = next_metric_id();
        let fingerprint = metric_tags_fingerprint(metric.as_ref(), tags.joined_tags());
        Self {
            metric,
            tags,
            hash,
            fingerprint,
            prepared_id,
        }
    }

    /// Returns metric name.
    #[must_use]
    pub const fn metric(&self) -> &RylvStr<'static> {
        &self.metric
    }

    /// Returns sorted tags.
    #[must_use]
    pub const fn tags(&self) -> &SortedTags<S> {
        &self.tags
    }

    /// Returns precomputed hash.
    #[must_use]
    pub const fn hash(&self) -> u64 {
        self.hash
    }

    /// Returns the secondary precomputed fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Returns stable prepared id.
    #[must_use]
    pub const fn prepared_id(&self) -> u64 {
        self.prepared_id
    }
}

static NEXT_SORTED_TAG_ID: AtomicU64 = AtomicU64::new(0);
pub fn next_sorted_tag_id() -> u64 {
    NEXT_SORTED_TAG_ID.fetch_add(1, Ordering::Relaxed)
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
pub fn next_metric_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::SortedTags;
    use crate::{DefaultMetricHasher, RylvStr, RylvTag};

    fn default_hasher() -> DefaultMetricHasher {
        DefaultMetricHasher::default()
    }

    #[test]
    fn sorted_tags_builds_sorted_joined() {
        let tags = SortedTags::new(
            [
                RylvTag::from("service:api".to_string()),
                RylvTag::from_static("env:prod"),
                RylvTag::from_static("az:use1"),
            ],
            &default_hasher(),
        );
        assert_eq!(tags.joined_tags(), "az:use1,env:prod,service:api");
        assert_eq!(tags.len(), 3);
    }

    #[test]
    fn sorted_tags_with_compound_tags() {
        let tags = SortedTags::new(
            [
                RylvTag::from_static_compound("service", "api"),
                RylvTag::from_static_compound("env", "prod"),
                RylvTag::from_static("az:use1"),
            ],
            &default_hasher(),
        );
        assert_eq!(tags.joined_tags(), "az:use1,env:prod,service:api");
        assert_eq!(tags.len(), 3);
        assert!(!tags.is_empty());
    }

    #[test]
    fn sorted_tags_empty() {
        let tags: SortedTags<DefaultMetricHasher> =
            SortedTags::new(std::iter::empty::<RylvTag<'_>>(), &default_hasher());
        assert_eq!(tags.joined_tags(), "");
        assert_eq!(tags.len(), 0);
        assert!(tags.is_empty());
    }

    #[test]
    fn sorted_tags_equality_same_tags() {
        let hasher = default_hasher();
        let a = SortedTags::new([RylvTag::from_static("env:prod")], &hasher);
        let b = SortedTags::new([RylvTag::from_static("env:prod")], &hasher);
        assert_eq!(a, b);
    }

    #[test]
    fn sorted_tags_equality_full_vs_compound() {
        let hasher = default_hasher();
        let a = SortedTags::new([RylvTag::from_static("env:prod")], &hasher);
        let b = SortedTags::new(
            [RylvTag::Compound(
                RylvStr::from_static("env"),
                RylvStr::from_static("prod"),
            )],
            &hasher,
        );
        // joined_tags should be identical
        assert_eq!(a.joined_tags(), b.joined_tags());
    }

    #[test]
    fn fingerprint_consistent_full_vs_compound() {
        use super::{metric_tags_fingerprint, metric_tags_fingerprint_from_tags};

        let full_tags = [RylvTag::from_static("env:prod")];
        let compound_tags = [RylvTag::Compound(
            RylvStr::from_static("env"),
            RylvStr::from_static("prod"),
        )];

        let fp_full = metric_tags_fingerprint_from_tags("my.metric", &full_tags);
        let fp_compound = metric_tags_fingerprint_from_tags("my.metric", &compound_tags);
        assert_eq!(fp_full, fp_compound);

        let fp_joined = metric_tags_fingerprint("my.metric", "env:prod");
        assert_eq!(fp_full, fp_joined);
    }

    #[test]
    fn hash_tags_deterministic() {
        use super::hash_tags;

        let hasher = default_hasher();
        let tags = [
            RylvTag::from_static("env:prod"),
            RylvTag::from_static("az:use1"),
        ];
        let h1 = hash_tags(&hasher, &tags);
        let h2 = hash_tags(&hasher, &tags);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_tags_compound() {
        use super::hash_tags;

        let hasher = default_hasher();
        let compound_tags = [RylvTag::Compound(
            RylvStr::from_static("env"),
            RylvStr::from_static("prod"),
        )];
        // Just verify it produces a non-zero hash without panicking
        let h = hash_tags(&hasher, &compound_tags);
        assert_ne!(h, 0);
    }

    #[test]
    fn sorted_tags_accessor_returns_sorted_slice() {
        let tags = SortedTags::new(
            [RylvTag::from_static("z:3"), RylvTag::from_static("a:1")],
            &default_hasher(),
        );
        let slice = tags.tags();
        assert_eq!(slice.len(), 2);
        // First tag should be "a:1" (sorted)
        let mut buf = String::new();
        slice[0].push_tag(&mut buf);
        assert_eq!(buf, "a:1");
    }

    #[test]
    fn fingerprint_multi_tags() {
        use super::{metric_tags_fingerprint, metric_tags_fingerprint_from_tags};

        let tags = [RylvTag::from_static("a:1"), RylvTag::from_static("b:2")];
        let fp = metric_tags_fingerprint_from_tags("m", &tags);
        let fp_joined = metric_tags_fingerprint("m", "a:1,b:2");
        assert_eq!(fp, fp_joined);
    }

    #[test]
    fn combine_metric_tags_hash_works() {
        use super::combine_metric_tags_hash;

        let hasher = default_hasher();
        let h = combine_metric_tags_hash(&hasher, "my.metric", 12345);
        assert_ne!(h, 0);
    }

    #[test]
    fn prepared_metric_accessors() {
        use super::PreparedMetric;

        let hasher = default_hasher();
        let tags = SortedTags::new([RylvTag::from_static("env:prod")], &hasher);
        let hash = super::combine_metric_tags_hash(&hasher, "my.metric", tags.tags_hash());
        let prepared = PreparedMetric::new(RylvStr::from_static("my.metric"), tags, hash);

        assert_eq!(prepared.metric().as_ref(), "my.metric");
        assert_eq!(prepared.tags().len(), 1);
        assert_eq!(prepared.hash(), hash);
        assert_ne!(prepared.fingerprint(), 0);
        // prepared_id should be unique
        let _ = prepared.prepared_id();
    }

    #[test]
    fn next_metric_id_increments() {
        let a = super::next_metric_id();
        let b = super::next_metric_id();
        assert_eq!(b, a + 1);
    }

    #[test]
    fn from_sorted_tags_with_hash() {
        let hasher = default_hasher();
        let tags = [RylvTag::from_static("a:1"), RylvTag::from_static("b:2")];
        let tags_hash = super::hash_tags(&hasher, &tags);
        let sorted: SortedTags<DefaultMetricHasher> =
            SortedTags::from_sorted_tags_with_hash(&tags, tags_hash);
        assert_eq!(sorted.joined_tags(), "a:1,b:2");
        assert_eq!(sorted.tags_hash(), tags_hash);
    }
}
