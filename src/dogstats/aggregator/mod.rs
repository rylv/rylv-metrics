use super::histogram_config::{HistogramBaseMetric, HistogramBaseMetrics};
use super::slice_utils::equal_slice;
use super::sorted_tags::{
    metric_tags_fingerprint, metric_tags_fingerprint_from_tags, next_metric_id, to_static_metric,
    SortedTags,
};
use super::RylvStr;
use crate::{DefaultMetricHasher, MetricsError, PreparedMetric};
use hdrhistogram::Histogram;
use std::cmp::{max, min};
use std::hash::BuildHasher;
use std::sync::Arc;

#[cfg(feature = "shared-collector")]
mod shared;

#[cfg(feature = "shared-collector")]
pub use shared::Aggregator;

#[derive(Clone)]
/// Internal benchmark-facing representation of an aggregated metric key.
pub struct AggregatorEntryKey<S: BuildHasher + Clone = DefaultMetricHasher> {
    /// Metric name.
    pub metric: RylvStr<'static>,
    /// Pre-sorted tags bound to the collector hasher.
    pub tags: SortedTags<S>,
    /// Combined metric-and-tags hash.
    pub hash: u64,
    /// Secondary fingerprint used to reduce hash collision checks.
    pub fingerprint: u64,
    /// Prepared metric identifier.
    pub id: u64,
}

pub fn to_agg_entry_key<S: BuildHasher + Clone>(
    prepared_metric: &PreparedMetric<S>,
) -> AggregatorEntryKey<S> {
    AggregatorEntryKey {
        metric: prepared_metric.metric().clone(),
        tags: prepared_metric.tags().clone(),
        hash: prepared_metric.hash(),
        fingerprint: prepared_metric.fingerprint(),
        id: prepared_metric.prepared_id(),
    }
}

pub struct RemoveKey {
    pub hash: u64,
    pub id: u64,
}

impl<S: BuildHasher + Clone> AggregatorEntryKey<S> {
    /// Converts the key into the minimal identifier used for removals.
    #[must_use]
    pub const fn remove_key(&self) -> RemoveKey {
        RemoveKey {
            hash: self.hash,
            id: self.id,
        }
    }
}

impl<S: BuildHasher + Clone> Eq for AggregatorEntryKey<S> {}

impl<S: BuildHasher + Clone> PartialEq for AggregatorEntryKey<S> {
    fn eq(&self, other: &Self) -> bool {
        let m1 = self.metric.as_ref().as_bytes();
        let m2 = other.metric.as_ref().as_bytes();

        self.id == other.id
            || self.hash == other.hash
                && self.fingerprint == other.fingerprint
                && equal_slice(m1, m2)
                && self.tags == other.tags
    }
}

/// Internal benchmark-facing borrowed lookup key used for comparisons.
pub struct LookupKey<'a> {
    /// Metric name.
    pub metric: RylvStr<'a>,
    /// Sorted tag slice.
    pub tags: &'a [RylvStr<'a>],
    /// Hash of the sorted tag set.
    pub tags_hash: u64,
    /// Combined metric-and-tags hash.
    pub hash: u64,
}

impl LookupKey<'_> {
    pub(crate) fn into_key<S: BuildHasher + Clone>(self) -> AggregatorEntryKey<S> {
        self.into_key_with_id(next_metric_id())
    }

    pub(crate) fn into_key_with_id<S: BuildHasher + Clone>(self, id: u64) -> AggregatorEntryKey<S> {
        let fingerprint = metric_tags_fingerprint_from_tags(self.metric.as_ref(), self.tags);
        AggregatorEntryKey {
            metric: to_static_metric(self.metric),
            tags: SortedTags::from_sorted_tags_with_hash(self.tags, self.tags_hash),
            hash: self.hash,
            fingerprint,
            id,
        }
    }

    /// Compares the borrowed lookup key against an owned aggregated key.
    #[must_use]
    pub fn compare<S: BuildHasher + Clone>(&self, c: &AggregatorEntryKey<S>) -> bool {
        let m1 = self.metric.as_ref();
        let m2 = c.metric.as_ref();
        c.hash == self.hash
            && m1.len() == m2.len()
            && equal_slice(m1.as_bytes(), m2.as_bytes())
            && self.compare_tags_joined(c.tags.joined_tags(), c.tags.len())
    }

    fn compare_tags_joined(&self, joined_tags: &str, tag_count: usize) -> bool {
        let compare = self.tags;
        if tag_count != compare.len() {
            return false;
        }
        if joined_tags.len() != Self::joined_tags_len(compare) {
            return false;
        }
        if compare.is_empty() {
            return joined_tags.is_empty();
        }

        let joined = joined_tags.as_bytes();
        let mut offset = 0usize;
        let last_index = compare.len() - 1;
        for (index, tag) in compare.iter().enumerate() {
            let tag_bytes = tag.as_ref().as_bytes();
            let next_offset = offset + tag_bytes.len();
            if next_offset > joined.len() || !equal_slice(&joined[offset..next_offset], tag_bytes) {
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

    fn joined_tags_len(tags: &[RylvStr<'_>]) -> usize {
        if tags.is_empty() {
            return 0;
        }
        tags.iter().map(|tag| tag.as_ref().len()).sum::<usize>() + tags.len() - 1
    }
}

pub struct LookupKeySorted<'a, S: BuildHasher + Clone> {
    pub metric: RylvStr<'a>,
    pub sorted_tags: &'a SortedTags<S>,
    pub hash: u64,
}

impl<S: BuildHasher + Clone> LookupKeySorted<'_, S> {
    pub fn compare(&self, c: &AggregatorEntryKey<S>) -> bool {
        let m1 = c.metric.as_ref().as_bytes();
        let m2 = self.metric.as_ref().as_bytes();
        c.hash == self.hash && equal_slice(m1, m2) && self.sorted_tags == &c.tags
    }

    pub(crate) fn into_key(self) -> AggregatorEntryKey<S> {
        self.into_key_with_id(next_metric_id())
    }

    pub(crate) fn into_key_with_id(self, id: u64) -> AggregatorEntryKey<S> {
        let fingerprint =
            metric_tags_fingerprint(self.metric.as_ref(), self.sorted_tags.joined_tags());
        AggregatorEntryKey {
            metric: to_static_metric(self.metric),
            tags: self.sorted_tags.clone(),
            hash: self.hash,
            fingerprint,
            id,
        }
    }
}

#[derive(Clone)]
pub struct HistogramWrapper {
    pub pool_id: usize,
    pub min: u64,
    pub max: u64,
    pub histogram: Histogram<u64>,
    pub percentiles: Arc<[f64]>,
    pub emit_base_metrics: HistogramBaseMetrics,
}

impl HistogramWrapper {
    pub const fn emits(&self, metric: HistogramBaseMetric) -> bool {
        self.emit_base_metrics.contains(metric)
    }

    pub fn reset(&mut self) {
        self.min = u64::MAX;
        self.max = u64::MIN;
        self.histogram.reset();
    }

    pub fn record(&mut self, value: u64) -> Result<(), hdrhistogram::RecordError> {
        self.min = min(self.min, value);
        self.max = max(self.max, value);
        self.histogram.record(value)
    }
}

pub const SIG_FIG_MAX: u8 = 5;
pub const SIG_FIG_DEF: u8 = 3;
const _: () = assert!(SIG_FIG_DEF <= SIG_FIG_MAX);

/// Number of significant figures for histogram precision (0..=5).
///
/// Higher values increase precision but also memory usage.
/// Use [`SigFig::default()`] for the default value of 3.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SigFig {
    value: u8,
}

impl SigFig {
    /// Creates a new `SigFig` with the given number of significant figures (0..=5).
    ///
    /// # Errors
    /// Returns [`MetricsError`] if `value` exceeds 5.
    pub fn new(value: u8) -> Result<Self, MetricsError> {
        if value > SIG_FIG_MAX {
            return Err(MetricsError::from(
                "Invalid sig fig: must be 0, 1, 2, 3, 4 or 5",
            ));
        }
        Ok(Self { value })
    }

    /// Returns the number of significant figures.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.value
    }
}

impl Default for SigFig {
    fn default() -> Self {
        Self { value: SIG_FIG_DEF }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        to_agg_entry_key, AggregatorEntryKey, HistogramWrapper, LookupKey, LookupKeySorted, SigFig,
    };
    use crate::dogstats::histogram_config::{HistogramBaseMetric, HistogramBaseMetrics};
    use crate::dogstats::sorted_tags::{combine_metric_tags_hash, hash_tags, SortedTags};
    use crate::{PreparedMetric, RylvStr};
    use hdrhistogram::Histogram;
    use std::sync::Arc;

    type TestHasher = std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

    fn sorted_tags(tags: &[&str]) -> SortedTags<TestHasher> {
        SortedTags::new(tags.iter().copied().map(RylvStr::from), &TestHasher::new())
    }

    #[test]
    fn to_agg_entry_key_and_remove_key_follow_prepared_metric() {
        let tags = sorted_tags(&["env:test", "service:api"]);
        let hash = combine_metric_tags_hash(&TestHasher::new(), "bench.metric", tags.tags_hash());
        let prepared =
            PreparedMetric::new(RylvStr::from_static("bench.metric"), tags.clone(), hash);

        let entry = to_agg_entry_key(&prepared);
        let remove = entry.remove_key();

        assert_eq!(entry.metric.as_ref(), "bench.metric");
        assert_eq!(entry.tags, tags);
        assert_eq!(entry.hash, prepared.hash());
        assert_eq!(entry.fingerprint, prepared.fingerprint());
        assert_eq!(entry.id, prepared.prepared_id());
        assert_eq!(remove.hash, prepared.hash());
        assert_eq!(remove.id, prepared.prepared_id());
    }

    #[test]
    fn aggregator_entry_key_equality_uses_id_or_full_signature() {
        let same_id_left = AggregatorEntryKey {
            metric: RylvStr::from_static("left"),
            tags: sorted_tags(&["a:1"]),
            hash: 1,
            fingerprint: 10,
            id: 99,
        };
        let same_id_right = AggregatorEntryKey {
            metric: RylvStr::from_static("right"),
            tags: sorted_tags(&["b:2"]),
            hash: 2,
            fingerprint: 11,
            id: 99,
        };
        assert!(same_id_left == same_id_right);

        let signature_left = AggregatorEntryKey {
            metric: RylvStr::from_static("metric"),
            tags: sorted_tags(&["env:test"]),
            hash: 44,
            fingerprint: 55,
            id: 1,
        };
        let signature_right = AggregatorEntryKey {
            metric: RylvStr::from_static("metric"),
            tags: sorted_tags(&["env:test"]),
            hash: 44,
            fingerprint: 55,
            id: 2,
        };
        assert!(signature_left == signature_right);
    }

    #[test]
    fn lookup_key_into_key_and_compare_cover_match_and_mismatch_paths() {
        let hasher = TestHasher::new();
        let tags = [
            RylvStr::from_static("env:test"),
            RylvStr::from_static("service:api"),
        ];
        let tags_hash = hash_tags(&hasher, &tags);
        let hash = combine_metric_tags_hash(&hasher, "bench.metric", tags_hash);
        let lookup = LookupKey {
            metric: RylvStr::from_static("bench.metric"),
            tags: &tags,
            tags_hash,
            hash,
        };

        let entry = lookup.into_key_with_id::<TestHasher>(7);
        assert_eq!(entry.metric.as_ref(), "bench.metric");
        assert_eq!(entry.tags.joined_tags(), "env:test,service:api");
        assert_eq!(entry.id, 7);

        let matching_lookup = LookupKey {
            metric: RylvStr::from_static("bench.metric"),
            tags: &tags,
            tags_hash,
            hash,
        };
        assert!(matching_lookup.compare(&entry));

        let mismatched_metric = LookupKey {
            metric: RylvStr::from_static("other.metric"),
            tags: &tags,
            tags_hash,
            hash,
        };
        assert!(!mismatched_metric.compare(&entry));

        let short_tags = [RylvStr::from_static("env:test")];
        let mismatched_tag_count = LookupKey {
            metric: RylvStr::from_static("bench.metric"),
            tags: &short_tags,
            tags_hash: hash_tags(&hasher, &short_tags),
            hash,
        };
        assert!(!mismatched_tag_count.compare(&entry));

        let bad_separator_entry = AggregatorEntryKey {
            metric: RylvStr::from_static("bench.metric"),
            tags: SortedTags::new([RylvStr::from_static("env:test,service:api")], &hasher),
            hash,
            fingerprint: entry.fingerprint,
            id: entry.id + 1,
        };
        let lookup_for_separator = LookupKey {
            metric: RylvStr::from_static("bench.metric"),
            tags: &tags,
            tags_hash,
            hash,
        };
        assert!(!lookup_for_separator.compare(&bad_separator_entry));
    }

    #[test]
    fn lookup_key_sorted_into_key_and_compare_work() {
        let hasher = TestHasher::new();
        let tags = sorted_tags(&["env:test", "service:api"]);
        let hash = combine_metric_tags_hash(&hasher, "bench.metric", tags.tags_hash());
        let lookup = LookupKeySorted {
            metric: RylvStr::from_static("bench.metric"),
            sorted_tags: &tags,
            hash,
        };

        let entry = lookup.into_key_with_id(55);
        assert_eq!(entry.id, 55);
        assert_eq!(entry.tags, tags);

        let matching_lookup = LookupKeySorted {
            metric: RylvStr::from_static("bench.metric"),
            sorted_tags: &entry.tags,
            hash,
        };
        assert!(matching_lookup.compare(&entry));

        let other_tags = sorted_tags(&["env:prod"]);
        let mismatched_lookup = LookupKeySorted {
            metric: RylvStr::from_static("bench.metric"),
            sorted_tags: &other_tags,
            hash,
        };
        assert!(!mismatched_lookup.compare(&entry));
    }

    #[test]
    fn histogram_wrapper_record_reset_and_emit_bits_work() {
        let mut wrapper = HistogramWrapper {
            pool_id: 0,
            min: u64::MAX,
            max: u64::MIN,
            histogram: Histogram::new_with_bounds(1, 1_000, 3).unwrap(),
            percentiles: Arc::from([0.95_f64]),
            emit_base_metrics: HistogramBaseMetrics::from([
                HistogramBaseMetric::Count,
                HistogramBaseMetric::Max,
            ]),
        };

        assert!(wrapper.emits(HistogramBaseMetric::Count));
        assert!(!wrapper.emits(HistogramBaseMetric::Min));
        wrapper.record(42).unwrap();
        wrapper.record(84).unwrap();
        assert_eq!(wrapper.min, 42);
        assert_eq!(wrapper.max, 84);
        assert_eq!(wrapper.histogram.len(), 2);

        wrapper.reset();
        assert_eq!(wrapper.min, u64::MAX);
        assert_eq!(wrapper.max, u64::MIN);
        assert_eq!(wrapper.histogram.len(), 0);
    }

    #[test]
    fn sig_fig_validates_range() {
        assert_eq!(SigFig::new(0).unwrap().value(), 0);
        assert_eq!(SigFig::new(5).unwrap().value(), 5);
        assert!(SigFig::new(6).is_err());
    }
}
