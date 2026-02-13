use super::Tags;
use super::{materialize_tags, GaugeState, RylvStr};
use crate::{DefaultMetricHasher, MetricsError};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use hdrhistogram::Histogram;
use std::borrow::Cow;
use std::cmp::{max, min};
use std::hash::BuildHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub struct AggregatorEntryKey {
    pub metric: Cow<'static, str>,
    pub tags: Tags,
    pub hash: u64,
    pub id: u64,
}

pub struct RemoveKey {
    pub hash: u64,
    pub id: u64,
}

impl AggregatorEntryKey {
    pub const fn to_key(&self) -> RemoveKey {
        RemoveKey {
            hash: self.hash,
            id: self.id,
        }
    }
}

impl Hash for AggregatorEntryKey {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        // unused, because the hash is calculated in the constructor
        unreachable!();
    }
}

impl Eq for AggregatorEntryKey {}
impl PartialEq for AggregatorEntryKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
            && self.metric.as_ref().eq(other.metric.as_ref())
            && self.tags == other.tags
    }
}

pub struct LookupKey<'a> {
    pub metric: RylvStr<'a>,
    pub tags: &'a [RylvStr<'a>],
    pub hash: u64,
}

impl LookupKey<'_> {
    pub(crate) fn into_key(self) -> AggregatorEntryKey {
        AggregatorEntryKey {
            metric: self.metric.to_cow(),
            tags: materialize_tags(self.tags),
            hash: self.hash,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub fn compare(&self, c: &AggregatorEntryKey) -> bool {
        c.hash == self.hash
            && c.metric.as_ref().eq(self.metric.as_ref())
            && self.compare_tags(c.tags.tags.as_slice())
    }

    fn compare_tags(&self, tags: &'_ [Cow<'_, str>]) -> bool {
        let compare = self.tags;
        if tags.len() != compare.len() {
            return false;
        }
        if tags.is_empty() {
            return true;
        }

        for i in 0..tags.len() {
            let cow = &tags[i];

            let tag = cow.as_ref();
            let cmp = compare[i].as_ref();
            if tag != cmp {
                return false;
            }
        }

        true
    }
}

pub struct HistogramWrapper {
    pub min: u64,
    pub max: u64,
    pub histogram: Histogram<u64>,
    pub sig_fig: SigFig,
}

impl HistogramWrapper {
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
/// Number of pool buckets: one per valid `SigFig` value (0..=5).
pub const POOL_COUNT: usize = SIG_FIG_MAX as usize + 1;

/// Default number of significant figures (3) for histogram recording.
pub(crate) const DEFAULT_SIG_FIG: SigFig = SigFig { value: SIG_FIG_DEF };

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

pub struct Aggregator<S = DefaultMetricHasher> {
    pub histograms: DashMap<AggregatorEntryKey, HistogramWrapper, S>,
    pub count: DashMap<AggregatorEntryKey, AtomicU64, S>,
    pub gauge: DashMap<AggregatorEntryKey, GaugeState, S>,

    // TODO: reuse cross Aggregators
    pub pool_histograms: [SegQueue<HistogramWrapper>; POOL_COUNT],
}

impl<S> Aggregator<S>
where
    S: BuildHasher + Clone,
{
    pub(crate) fn with_hasher_builder(hasher_builder: S) -> Self {
        Self {
            histograms: DashMap::with_hasher(hasher_builder.clone()),
            count: DashMap::with_hasher(hasher_builder.clone()),
            gauge: DashMap::with_hasher(hasher_builder),
            pool_histograms: [
                SegQueue::new(),
                SegQueue::new(),
                SegQueue::new(),
                SegQueue::new(),
                SegQueue::new(),
                SegQueue::new(),
            ],
        }
    }

    pub(crate) fn get_histogram(&self, sig_fig: SigFig) -> Option<HistogramWrapper> {
        if let Some(h) =
            unsafe { self.pool_histograms.get_unchecked(sig_fig.value() as usize) }.pop()
        {
            return Some(h);
        }

        // TODO: parameterize bounds
        if let Ok(histo) = Histogram::new_with_bounds(1, u64::MAX, sig_fig.value()) {
            return Some(HistogramWrapper {
                histogram: histo,
                min: u64::MAX,
                max: u64::MIN,
                sig_fig,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sig_fig_new_valid() {
        for v in 0..=5 {
            let sig_fig = SigFig::new(v).expect("should be valid");
            assert_eq!(sig_fig.value(), v);
        }
    }

    #[test]
    fn test_sig_fig_new_invalid() {
        for v in 6..=255 {
            assert!(SigFig::new(v).is_err());
        }
    }
}
