use std::{
    borrow::Cow,
    cmp::Ordering,
    sync::{atomic::AtomicU64, Arc},
};

use crate::dogstats::aggregator::Aggregator;

mod aggregator;
pub mod collector;
mod job;
pub mod macros;
mod net;
pub mod writer;
mod writer_utils;

pub use aggregator::SigFig;

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
    /// An owned string stored in an `Arc` for cheap cloning.
    Owned(Arc<str>),
}

impl RylvStr<'_> {
    /// Creates a `RylvStr::Static` from a `&'static str` for zero-copy conversion.
    #[must_use]
    pub const fn from_static(s: &'static str) -> RylvStr<'static> {
        RylvStr::Static(s)
    }

    /// Converts this value into a `Cow<'static, str>`.
    #[must_use]
    pub fn to_cow(&self) -> Cow<'static, str> {
        match self {
            RylvStr::Static(s) => Cow::Borrowed(s),
            RylvStr::Borrowed(s) => Cow::Owned((*s).to_owned()),
            RylvStr::Owned(s) => Cow::Owned(s.as_ref().to_owned()),
        }
    }
}

impl AsRef<str> for RylvStr<'_> {
    fn as_ref(&self) -> &str {
        match self {
            RylvStr::Static(s) | RylvStr::Borrowed(s) => s,
            RylvStr::Owned(s) => s.as_ref(),
        }
    }
}

impl PartialEq for RylvStr<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for RylvStr<'_> {}

impl PartialOrd for RylvStr<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RylvStr<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl<'a> From<&'a str> for RylvStr<'a> {
    fn from(s: &'a str) -> Self {
        RylvStr::Borrowed(s)
    }
}

impl From<String> for RylvStr<'_> {
    fn from(s: String) -> Self {
        RylvStr::Owned(Arc::from(s))
    }
}

impl From<Arc<str>> for RylvStr<'_> {
    fn from(s: Arc<str>) -> Self {
        RylvStr::Owned(s)
    }
}

impl<'a> From<Cow<'a, str>> for RylvStr<'a> {
    fn from(cow: Cow<'a, str>) -> Self {
        match cow {
            Cow::Borrowed(s) => RylvStr::Borrowed(s),
            Cow::Owned(s) => RylvStr::Owned(Arc::from(s)),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Tags {
    pub tags: Vec<Cow<'static, str>>,

    // TODO: check if needed this and can be replaced with iovec with separators with self reference
    pub joined_tags: Cow<'static, str>,
}

#[derive(Copy, Clone)]
pub enum MetricType {
    Count,
    Gauge,
}

pub struct GaugeState {
    pub sum: AtomicU64,
    pub count: AtomicU64,
}

pub fn materialize_tags(tags: &[RylvStr<'_>]) -> Tags {
    if tags.is_empty() {
        return Tags {
            tags: Vec::new(),
            joined_tags: Cow::Borrowed(""),
        };
    }

    if tags.len() == 1 {
        let tag = tags[0].to_cow();
        return Tags {
            joined_tags: tag.clone(),
            tags: vec![tag],
        };
    }

    let mut vec = Vec::with_capacity(tags.len());
    let mut tags_len_bytes = 0;
    for tag in tags {
        let cow_tag = tag.to_cow();
        tags_len_bytes += cow_tag.len();
        vec.push(cow_tag);
    }

    let vec_len = vec.len();
    let joined_tags_len = tags_len_bytes + vec_len - 1;
    let mut buffer = String::with_capacity(joined_tags_len);

    let mut iter = vec.iter();
    if let Some(tag) = iter.next() {
        buffer.push_str(tag.as_ref());
    }

    for tag in iter {
        buffer.push(',');
        buffer.push_str(tag.as_ref());
    }

    Tags {
        tags: vec,
        joined_tags: Cow::Owned(buffer),
    }
}
