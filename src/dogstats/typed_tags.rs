//! Borrowed tag values and compile-time ordered tag sets.

use std::{borrow::Cow, sync::Arc};

use crate::{RylvStr, RylvTag, __private::Sealed};

/// A value that can be borrowed as a tag value by [`RylvTags`].
///
/// Implementations are supplied by this crate; downstream implementations are
/// unsupported. Dynamic values are borrowed without cloning on the hot path.
/// `RylvStr::Static` stays static, avoiding a copy when a new key is inserted.
///
/// ```
/// use rylv_metrics::{RylvStr, RylvValue};
/// let value = RylvStr::from_static("prod");
/// assert!(matches!(value.__as_rylv_str(), RylvStr::Static("prod")));
/// ```
pub trait RylvValue: Sealed {
    /// Borrows the value for one metric recording call.
    #[doc(hidden)]
    fn __as_rylv_str(&self) -> RylvStr<'_>;
}

impl Sealed for RylvStr<'_> {}
impl RylvValue for RylvStr<'_> {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        match self {
            RylvStr::Static(value) => RylvStr::Static(value),
            _ => RylvStr::Borrowed(self.as_ref()),
        }
    }
}

impl Sealed for &str {}
impl RylvValue for &str {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        RylvStr::Borrowed(self)
    }
}

impl Sealed for String {}
impl RylvValue for String {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        RylvStr::Borrowed(self)
    }
}

impl Sealed for Arc<str> {}
impl RylvValue for Arc<str> {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        RylvStr::Borrowed(self)
    }
}

impl Sealed for Arc<String> {}
impl RylvValue for Arc<String> {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        RylvStr::Borrowed(self)
    }
}

impl Sealed for Cow<'_, str> {}
impl RylvValue for Cow<'_, str> {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        RylvStr::Borrowed(self)
    }
}

impl<T: RylvValue + ?Sized> Sealed for &T {}
impl<T: RylvValue + ?Sized> RylvValue for &T {
    #[inline]
    fn __as_rylv_str(&self) -> RylvStr<'_> {
        (*self).__as_rylv_str()
    }
}

/// A set of tags generated in canonical order by `#[derive(RylvTags)]`.
///
/// The callback receives unique keys sorted according to [`RylvTag::cmp`].
/// Implement this trait only through the derive macro. The `__private` module
/// used by generated code is not a stable API. An invalid manual implementation
/// can split one metric into multiple aggregation series.
///
/// ```
/// # #[cfg(feature = "derive")] {
/// use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTags, SharedCollector};
/// #[derive(RylvTags)]
/// struct Tags<'a> { env: &'a str }
/// let collector = SharedCollector::default();
/// collector.count_typed(RylvStr::from_static("requests"), &Tags { env: "prod" });
/// # }
/// ```
pub trait RylvTags: Sealed {
    /// Supplies a temporary, canonically sorted tag slice to the callback.
    #[doc(hidden)]
    fn __with_sorted_tags<R>(&self, f: impl FnOnce(&mut [RylvTag<'_>]) -> R) -> R;
}
