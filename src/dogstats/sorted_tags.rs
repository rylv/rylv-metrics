use crate::dogstats::slice_utils::equal_slice;
use crate::dogstats::RylvStr;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Fast secondary fingerprint over metric + joined tags.
#[must_use]
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
pub fn metric_tags_fingerprint_from_tags(metric: &str, tags: &[RylvStr<'_>]) -> u64 {
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
        for b in tag.as_ref().as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    for tag in iter {
        hash ^= u64::from(b',');
        hash = hash.wrapping_mul(PRIME);
        for b in tag.as_ref().as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

/// Compute a hash over a sorted tag slice.
#[must_use]
pub fn hash_tags<S: BuildHasher>(hasher_builder: &S, tags: &[RylvStr<'_>]) -> u64 {
    let mut hasher = hasher_builder.build_hasher();
    for tag in tags {
        tag.as_ref().hash(&mut hasher);
    }
    hasher.finish()
}

/// Combine a metric name with a precomputed tags hash into a single lookup hash.
#[must_use]
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
    tags: Box<[RylvStr<'static>]>,
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
    /// Tags are converted to owned/static form, sorted, joined, and hashed once.
    #[must_use]
    pub fn new<'a, I>(tags: I, hasher_builder: &S) -> Self
    where
        I: IntoIterator<Item = RylvStr<'a>>,
    {
        let mut tags_vec: Vec<RylvStr<'static>> = tags.into_iter().map(to_static_tag).collect();
        tags_vec.sort_unstable();

        // TODO: route this through `build_joined_tags` too, so joined-tag construction lives in
        // one place before we do any further hot-path tuning here.
        let joined_tags = if tags_vec.is_empty() {
            Arc::<str>::from("")
        } else {
            let joined_len =
                tags_vec.iter().map(|tag| tag.as_ref().len()).sum::<usize>() + tags_vec.len() - 1;
            let mut buffer = String::with_capacity(joined_len);
            let mut iter = tags_vec.iter();
            if let Some(tag) = iter.next() {
                buffer.push_str(tag.as_ref());
            }
            for tag in iter {
                buffer.push(',');
                buffer.push_str(tag.as_ref());
            }
            Arc::<str>::from(buffer)
        };

        let tags_hash = hash_tags(hasher_builder, &tags_vec);

        Self {
            tags: tags_vec.into_boxed_slice(),
            joined_tags,
            tags_hash,
            id: next_sorted_tag_id(),
            _hasher: PhantomData,
        }
    }

    /// Builds `SortedTags` from already-sorted tags and a precomputed tags hash.
    pub(crate) fn from_sorted_tags_with_hash(tags: &[RylvStr<'_>], tags_hash: u64) -> Self {
        let tags_vec: Vec<RylvStr<'static>> = tags.iter().cloned().map(to_static_tag).collect();
        let joined_tags = build_joined_tags(&tags_vec);

        Self {
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
    pub fn tags(&self) -> &[RylvStr<'static>] {
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

fn to_static_tag(tag: RylvStr<'_>) -> RylvStr<'static> {
    match tag {
        RylvStr::Static(s) => RylvStr::Static(s),
        RylvStr::Borrowed(s) => RylvStr::Owned(Arc::from(s)),
        RylvStr::Owned(s) => RylvStr::Owned(s),
    }
}

fn build_joined_tags(tags_vec: &[RylvStr<'static>]) -> Arc<str> {
    if tags_vec.is_empty() {
        return Arc::<str>::from("");
    }

    let joined_len =
        tags_vec.iter().map(|tag| tag.as_ref().len()).sum::<usize>() + tags_vec.len() - 1;
    let mut buffer = String::with_capacity(joined_len);
    let mut iter = tags_vec.iter();
    if let Some(tag) = iter.next() {
        buffer.push_str(tag.as_ref());
    }
    for tag in iter {
        buffer.push(',');
        buffer.push_str(tag.as_ref());
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

/// Converts a metric name into a `'static` representation.
#[must_use]
pub fn to_static_metric(metric: RylvStr<'_>) -> RylvStr<'static> {
    match metric {
        RylvStr::Static(s) => RylvStr::Static(s),
        RylvStr::Borrowed(s) => RylvStr::Owned(Arc::from(s)),
        RylvStr::Owned(s) => RylvStr::Owned(s),
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
    use crate::{DefaultMetricHasher, RylvStr};

    fn default_hasher() -> DefaultMetricHasher {
        DefaultMetricHasher::default()
    }

    #[test]
    fn sorted_tags_builds_sorted_joined() {
        let tags = SortedTags::new(
            [
                RylvStr::from("service:api".to_string()),
                RylvStr::from_static("env:prod"),
                RylvStr::from_static("az:use1"),
            ],
            &default_hasher(),
        );
        assert_eq!(tags.joined_tags(), "az:use1,env:prod,service:api");
        assert_eq!(tags.len(), 3);
    }
}
