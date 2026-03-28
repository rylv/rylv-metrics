#[cfg(feature = "shared-collector")]
pub use aggregator::Aggregator;
use std::cmp::min;
use std::{borrow::Cow, cmp::Ordering, sync::Arc};

mod aggregator;
pub mod collector;
#[cfg(feature = "udp")]
mod collector_udp;
mod histogram_config;
#[cfg(feature = "udp")]
mod job;
pub mod macros;
#[cfg(feature = "udp")]
mod net;
mod slice_utils;
mod sorted_tags;
#[cfg(feature = "udp")]
pub mod writer;
#[cfg(feature = "udp")]
mod writer_utils;
pub use aggregator::SigFig;
#[cfg(feature = "__bench-internals")]
pub use aggregator::{AggregatorEntryKey, LookupKey};
pub use collector::DrainMetricCollectorTrait;
pub use collector::MetricCollectorTrait;
pub use collector::{MetricFrameRef, MetricKind, MetricSuffix};
#[cfg(feature = "shared-collector")]
pub use collector::{SharedCollector, SharedCollectorOptions};
#[cfg(feature = "tls-collector")]
pub use collector::{TLSCollector, TLSCollectorOptions};
#[cfg(feature = "udp")]
pub use collector_udp::{
    MetricCollector, MetricCollectorOptions, StatsWriterType, DEFAULT_STATS_WRITER_TYPE,
};
pub use histogram_config::{HistogramBaseMetric, HistogramConfig};
pub use sorted_tags::{PreparedMetric, SortedTags};

/// A flexible string type that can hold static references, borrowed references, or owned values.
/// Used for metric names and tags.
///
/// # Choosing the Right Variant
///
/// | Variant | When to use | `to_cow()` cost |
/// |---------|-------------|-----------------|
/// | `RylvStr::Static` | Compile-time string literals (`from_static("...")`) | Zero-copy (`Cow::Borrowed`) |
/// | `RylvStr::Borrowed` | Short-lived `&str` references (via `From<&str>`) | Allocates (`Cow::Owned`) |
/// | `RylvStr::Owned` | Runtime-generated strings (via `From<String>`) | Allocates (`Cow::Owned`) |
///
/// For best performance, use `RylvStr::from_static()` whenever the string is known
/// at compile time. This avoids heap allocation when the aggregator stores a new metric key.
#[derive(Debug, Clone)]
pub enum RylvStr<'a> {
    /// A borrowed `&'static str`. Zero-copy on `to_cow()`.
    Static(&'static str),
    /// A borrowed non-static `&str`. Clones on `to_cow()`.
    Borrowed(&'a str),
    /// An owned String stored in an `Arc` for cheap cloning.
    Owned(Arc<String>),
    /// An owned str stored in an `Arc` for cheap cloning.
    OwnedStr(Arc<str>),
}

impl RylvStr<'_> {
    /// Creates a `RylvStr::Static` from a `&'static str` for zero-copy conversion.
    #[inline]
    #[must_use]
    pub const fn from_static(s: &'static str) -> RylvStr<'static> {
        RylvStr::Static(s)
    }

    /// Converts this `RylvStr` into a `'static` variant, cloning borrowed data.
    #[inline]
    #[must_use]
    pub fn into_static(self) -> RylvStr<'static> {
        match self {
            RylvStr::Static(s) => RylvStr::Static(s),
            RylvStr::Borrowed(s) => RylvStr::OwnedStr(Arc::from(s)),
            RylvStr::Owned(s) => RylvStr::Owned(s),
            RylvStr::OwnedStr(s) => RylvStr::OwnedStr(s),
        }
    }
}

impl AsRef<str> for RylvStr<'_> {
    #[inline]
    fn as_ref(&self) -> &str {
        match self {
            RylvStr::Static(s) | RylvStr::Borrowed(s) => s,
            RylvStr::Owned(s) => s,
            RylvStr::OwnedStr(s) => s,
        }
    }
}

impl PartialEq for RylvStr<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for RylvStr<'_> {}

impl PartialOrd for RylvStr<'_> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RylvStr<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl<'a> From<&'a str> for RylvStr<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        RylvStr::Borrowed(s)
    }
}

impl From<String> for RylvStr<'_> {
    #[inline]
    fn from(s: String) -> Self {
        RylvStr::Owned(Arc::from(s))
    }
}

impl From<Arc<String>> for RylvStr<'_> {
    #[inline]
    fn from(s: Arc<String>) -> Self {
        RylvStr::Owned(s)
    }
}

impl From<Arc<str>> for RylvStr<'_> {
    #[inline]
    fn from(s: Arc<str>) -> Self {
        RylvStr::OwnedStr(s)
    }
}

impl<'a> From<Cow<'a, str>> for RylvStr<'a> {
    #[inline]
    fn from(cow: Cow<'a, str>) -> Self {
        match cow {
            Cow::Borrowed(s) => RylvStr::Borrowed(s),
            Cow::Owned(s) => RylvStr::Owned(Arc::new(s)),
        }
    }
}

/// A tag that is either a pre-formatted full tag string or a key-value compound pair.
///
/// # Variants
///
/// | Variant | Example | Resolved form |
/// |---------|---------|---------------|
/// | `Full` | `RylvTag::Full(RylvStr::from_static("env:prod"))` | `"env:prod"` |
/// | `Compound` | `RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"))` | `"env:prod"` |
///
/// `Full` is zero-cost when the tag is already in `key:value` format.
/// `Compound` joins key and value with `:` at resolution time (allocates).
#[derive(Debug, Clone)]
pub enum RylvTag<'a> {
    /// A pre-formatted tag string (e.g. `"env:prod"`).
    Full(RylvStr<'a>),
    /// A key-value pair joined with `:` on resolution.
    Compound(RylvStr<'a>, RylvStr<'a>),
}

impl<'a> From<&'a str> for RylvTag<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        RylvTag::Full(RylvStr::from(s))
    }
}

impl From<String> for RylvTag<'_> {
    #[inline]
    fn from(value: String) -> Self {
        RylvTag::Full(RylvStr::from(value))
    }
}

impl From<Arc<String>> for RylvTag<'_> {
    #[inline]
    fn from(value: Arc<String>) -> Self {
        RylvTag::Full(RylvStr::from(value))
    }
}

impl From<Arc<str>> for RylvTag<'_> {
    #[inline]
    fn from(value: Arc<str>) -> Self {
        RylvTag::Full(RylvStr::from(value))
    }
}

impl<'a> From<RylvStr<'a>> for RylvTag<'a> {
    #[inline]
    fn from(s: RylvStr<'a>) -> Self {
        RylvTag::Full(s)
    }
}

impl<'a> From<Cow<'a, str>> for RylvTag<'a> {
    #[inline]
    fn from(cow: Cow<'a, str>) -> Self {
        RylvTag::Full(RylvStr::from(cow))
    }
}

impl RylvTag<'_> {
    /// Returns the length of the resolved tag string in bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            RylvTag::Full(t) => t.as_ref().len(),
            RylvTag::Compound(k, v) => k.as_ref().len() + v.as_ref().len() + 1,
        }
    }

    /// Returns `true` when the resolved tag string is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn push_tag(&self, buffer: &mut String) {
        match self {
            RylvTag::Full(tag) => {
                buffer.push_str(tag.as_ref());
            }
            RylvTag::Compound(k, v) => {
                buffer.push_str(k.as_ref());
                buffer.push(':');
                buffer.push_str(v.as_ref());
            }
        }
    }

    /// Converts this tag into a `'static` variant, cloning borrowed data.
    #[inline]
    #[must_use]
    pub fn into_static_tag(self) -> RylvTag<'static> {
        match self {
            RylvTag::Compound(key, value) => {
                RylvTag::Compound(key.into_static(), value.into_static())
            }
            RylvTag::Full(s) => RylvTag::Full(s.into_static()),
        }
    }

    /// Creates a `RylvTag::Full` wrapping a `RylvStr::Static` for zero-copy usage.
    #[inline]
    #[must_use]
    pub const fn from_static(s: &'static str) -> RylvTag<'static> {
        RylvTag::Full(RylvStr::from_static(s))
    }
}

fn resolve_tags<'a, I>(tags: I) -> Vec<RylvTag<'static>>
where
    I: IntoIterator<Item = RylvTag<'a>>,
{
    tags.into_iter().map(RylvTag::into_static_tag).collect()
}

impl Eq for RylvTag<'_> {}
impl PartialEq for RylvTag<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        match self {
            RylvTag::Full(r1) => match other {
                RylvTag::Full(r2) => r1.eq(r2),
                RylvTag::Compound(k2, v2) => eq(r1, k2, v2),
            },
            RylvTag::Compound(k1, v1) => match other {
                RylvTag::Full(r2) => eq(r2, k1, v1),
                RylvTag::Compound(k2, v2) => k1.eq(k2) && v1.eq(v2),
            },
        }
    }
}

impl Ord for RylvTag<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match self {
            RylvTag::Full(r1) => match other {
                RylvTag::Full(r2) => r1.cmp(r2),
                RylvTag::Compound(k2, v2) => cmp(r1, k2, v2),
            },
            RylvTag::Compound(k1, v1) => match other {
                RylvTag::Full(r2) => cmp(r2, k1, v1).reverse(),
                RylvTag::Compound(k2, v2) => {
                    let ordering = k1.cmp(k2);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                    v1.cmp(v2)
                }
            },
        }
    }
}

impl PartialOrd for RylvTag<'_> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn cmp(r: &RylvStr, k: &RylvStr, v: &RylvStr) -> Ordering {
    let r = r.as_ref();
    let k = k.as_ref();
    let v = v.as_ref();

    let m = min(r.len(), k.len());
    let ordering = r[0..m].cmp(k);
    if ordering != Ordering::Equal {
        return ordering;
    }

    let Some(sep) = r.as_bytes().get(m) else {
        return Ordering::Less;
    };

    let ordering = sep.cmp(&b':');
    if ordering != Ordering::Equal {
        return ordering;
    }

    let Some(rest) = r.get(m + 1..) else {
        return if v.is_empty() {
            Ordering::Equal
        } else {
            Ordering::Less
        };
    };

    rest.cmp(v)
}

#[inline]
fn eq(r: &RylvStr, k: &RylvStr, v: &RylvStr) -> bool {
    let r = r.as_ref();
    let k = k.as_ref();
    let v = v.as_ref();
    (r.len() == k.len() + v.len() + 1)
        && r[0..k.len()].eq(k)
        && r.as_bytes()[k.len()] == b':'
        && r[k.len() + 1..].eq(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use std::sync::Arc;

    // --- RylvStr tests ---

    #[test]
    fn rylv_str_partial_eq() {
        let a = RylvStr::from_static("hello");
        let b = RylvStr::from("hello".to_string());
        let c = RylvStr::Borrowed("hello");
        let d = RylvStr::OwnedStr(Arc::from("hello"));
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(a, d);
        assert_eq!(b, d);
        assert_ne!(a, RylvStr::from_static("world"));
    }

    #[test]
    fn rylv_str_partial_ord() {
        let a = RylvStr::from_static("abc");
        let b = RylvStr::from_static("def");
        assert!(a < b);
        assert_eq!(a.partial_cmp(&a), Some(Ordering::Equal));
    }

    #[test]
    fn rylv_str_from_arc_string() {
        let arc = Arc::new("test".to_string());
        let s = RylvStr::from(arc);
        assert_eq!(s.as_ref(), "test");
    }

    #[test]
    fn rylv_str_from_arc_str() {
        let arc: Arc<str> = Arc::from("test");
        let s = RylvStr::from(arc);
        assert_eq!(s.as_ref(), "test");
    }

    #[test]
    fn rylv_str_from_cow_borrowed() {
        let cow = Cow::Borrowed("hello");
        let s = RylvStr::from(cow);
        assert_eq!(s.as_ref(), "hello");
    }

    #[test]
    fn rylv_str_from_cow_owned() {
        let cow: Cow<'_, str> = Cow::Owned("hello".to_string());
        let s = RylvStr::from(cow);
        assert_eq!(s.as_ref(), "hello");
    }

    #[test]
    fn rylv_str_into_static_owned_str() {
        let s = RylvStr::OwnedStr(Arc::from("test"));
        let st = s.into_static();
        assert_eq!(st.as_ref(), "test");
    }

    // --- RylvTag tests ---

    #[test]
    fn rylv_tag_from_str() {
        let tag = RylvTag::from("env:prod");
        assert_eq!(tag.len(), 8);
    }

    #[test]
    fn rylv_tag_from_arc_string() {
        let arc = Arc::new("env:prod".to_string());
        let tag = RylvTag::from(arc);
        assert_eq!(tag.len(), 8);
    }

    #[test]
    fn rylv_tag_from_arc_str() {
        let arc: Arc<str> = Arc::from("env:prod");
        let tag = RylvTag::from(arc);
        assert_eq!(tag.len(), 8);
    }

    #[test]
    fn rylv_tag_from_cow() {
        let cow: Cow<'_, str> = Cow::Owned("env:prod".to_string());
        let tag = RylvTag::from(cow);
        assert_eq!(tag.len(), 8);
    }

    #[test]
    fn rylv_tag_from_static() {
        let tag = RylvTag::from_static("env:prod");
        assert_eq!(tag.len(), 8);
    }

    #[test]
    fn rylv_tag_compound_len() {
        let tag = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert_eq!(tag.len(), 8); // "env" + ":" + "prod"
    }

    #[test]
    fn rylv_tag_is_empty() {
        let tag = RylvTag::Full(RylvStr::from_static(""));
        assert!(tag.is_empty());

        let tag = RylvTag::from_static("env:prod");
        assert!(!tag.is_empty());
    }

    #[test]
    fn rylv_tag_push_tag_compound() {
        let tag = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        let mut buf = String::new();
        tag.push_tag(&mut buf);
        assert_eq!(buf, "env:prod");
    }

    #[test]
    fn rylv_tag_push_tag_full() {
        let tag = RylvTag::Full(RylvStr::from_static("env:prod"));
        let mut buf = String::new();
        tag.push_tag(&mut buf);
        assert_eq!(buf, "env:prod");
    }

    #[test]
    fn rylv_tag_into_static_tag_compound() {
        let tag = RylvTag::Compound(RylvStr::Borrowed("env"), RylvStr::Borrowed("prod"));
        let st = tag.into_static_tag();
        let mut buf = String::new();
        st.push_tag(&mut buf);
        assert_eq!(buf, "env:prod");
    }

    // --- Cross-variant PartialEq ---

    #[test]
    fn rylv_tag_eq_full_full() {
        let a = RylvTag::Full(RylvStr::from_static("env:prod"));
        let b = RylvTag::Full(RylvStr::from_static("env:prod"));
        assert_eq!(a, b);
    }

    #[test]
    fn rylv_tag_eq_full_compound() {
        let full = RylvTag::Full(RylvStr::from_static("env:prod"));
        let compound =
            RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert_eq!(full, compound);
        assert_eq!(compound, full);
    }

    #[test]
    fn rylv_tag_eq_compound_compound() {
        let a = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        let b = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert_eq!(a, b);
    }

    #[test]
    fn rylv_tag_neq_full_compound_different_value() {
        let full = RylvTag::Full(RylvStr::from_static("env:staging"));
        let compound =
            RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert_ne!(full, compound);
    }

    // --- Cross-variant Ord ---

    #[test]
    fn rylv_tag_ord_full_full() {
        let a = RylvTag::Full(RylvStr::from_static("az:use1"));
        let b = RylvTag::Full(RylvStr::from_static("env:prod"));
        assert!(a < b);
    }

    #[test]
    fn rylv_tag_ord_full_compound_equal() {
        let full = RylvTag::Full(RylvStr::from_static("env:prod"));
        let compound =
            RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert_eq!(full.cmp(&compound), Ordering::Equal);
    }

    #[test]
    fn rylv_tag_ord_compound_full_less() {
        let compound =
            RylvTag::Compound(RylvStr::from_static("az"), RylvStr::from_static("use1"));
        let full = RylvTag::Full(RylvStr::from_static("env:prod"));
        assert!(compound < full);
    }

    #[test]
    fn rylv_tag_ord_compound_compound() {
        let a = RylvTag::Compound(RylvStr::from_static("az"), RylvStr::from_static("use1"));
        let b = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("prod"));
        assert!(a < b);

        let c = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("alpha"));
        let d = RylvTag::Compound(RylvStr::from_static("env"), RylvStr::from_static("beta"));
        assert!(c < d);
    }

    #[test]
    fn rylv_tag_partial_ord() {
        let a = RylvTag::from_static("az:use1");
        let b = RylvTag::from_static("env:prod");
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
    }

    // --- cmp/eq helper function tests ---

    #[test]
    fn helper_eq_matching() {
        let r = RylvStr::from_static("env:prod");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert!(eq(&r, &k, &v));
    }

    #[test]
    fn helper_eq_different_key() {
        let r = RylvStr::from_static("env:prod");
        let k = RylvStr::from_static("az");
        let v = RylvStr::from_static("prod");
        assert!(!eq(&r, &k, &v));
    }

    #[test]
    fn helper_eq_different_value() {
        let r = RylvStr::from_static("env:prod");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("staging");
        assert!(!eq(&r, &k, &v));
    }

    #[test]
    fn helper_cmp_equal() {
        let r = RylvStr::from_static("env:prod");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert_eq!(cmp(&r, &k, &v), Ordering::Equal);
    }

    #[test]
    fn helper_cmp_key_less() {
        let r = RylvStr::from_static("az:use1");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert_eq!(cmp(&r, &k, &v), Ordering::Less);
    }

    #[test]
    fn helper_cmp_key_greater() {
        let r = RylvStr::from_static("zzz:val");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert_eq!(cmp(&r, &k, &v), Ordering::Greater);
    }

    #[test]
    fn helper_cmp_r_shorter_than_key() {
        let r = RylvStr::from_static("en");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert_eq!(cmp(&r, &k, &v), Ordering::Less);
    }

    #[test]
    fn helper_cmp_separator_mismatch() {
        // 'e' < ':' is false, 'e' > ':' in ASCII
        let r = RylvStr::from_static("enveprod");
        let k = RylvStr::from_static("env");
        let v = RylvStr::from_static("prod");
        assert_eq!(cmp(&r, &k, &v), Ordering::Greater);
    }

    // --- resolve_tags ---

    #[test]
    fn resolve_tags_converts_to_static() {
        let tags = vec![
            RylvTag::Full(RylvStr::Borrowed("env:prod")),
            RylvTag::Compound(RylvStr::Borrowed("az"), RylvStr::Borrowed("use1")),
        ];
        let resolved = resolve_tags(tags);
        assert_eq!(resolved.len(), 2);
        let mut buf = String::new();
        resolved[1].push_tag(&mut buf);
        assert_eq!(buf, "az:use1");
    }
}
