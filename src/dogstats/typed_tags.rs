//! Compile-time ordered tag sets.

use crate::RylvTag;

/// A set of tags generated in canonical order by `#[derive(RylvTags)]`.
///
/// The callback receives unique keys sorted according to [`RylvTag::cmp`].
/// Prefer the derive macro. Manual implementations must provide unique keys in
/// canonical order; violating this contract can split one metric into multiple
/// aggregation series.
///
/// ```
/// # #[cfg(feature = "derive")] {
/// use rylv_metrics::{MetricCollectorTrait, RylvStr, RylvTags, SharedCollector};
/// #[derive(RylvTags)]
/// struct Tags<'a> { env: RylvStr<'a> }
/// let collector = SharedCollector::default();
/// collector.count_typed(RylvStr::from_static("requests"), &Tags { env: RylvStr::from_static("prod") });
/// # }
/// ```
pub trait RylvTags {
    /// Supplies a temporary, canonically sorted tag slice to the callback.
    #[doc(hidden)]
    fn __with_sorted_tags<R>(&self, f: impl FnOnce(&mut [RylvTag<'_>]) -> R) -> R;
}
