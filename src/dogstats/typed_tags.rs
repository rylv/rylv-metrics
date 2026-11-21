//! Compile-time ordered tag sets.

use crate::RylvTag;

/// A set of tags supplied in canonical order to `*_typed` collector methods.
///
/// Prefer `#[derive(RylvTags)]`, which enforces these rules for structs with
/// `RylvStr` fields. Manual implementations must uphold the following contract
/// on every call to [`RylvTags::__with_sorted_tags`]:
///
/// - Call the callback exactly once with the complete tag slice and return its
///   result. Keep the tags and any borrowed values alive until it returns.
/// - Each tag must represent a `key:value` pair with a nonempty, unique key.
///   Keys must not contain `:`, `,`, `|`, `#`, or whitespace. Values may be
///   empty and may contain `:`.
/// - Sort the tags in ascending [`RylvTag::cmp`] order by their complete
///   resolved `key:value` form. Field declaration order or sorting by value
///   alone is insufficient.
///
/// The typed collector paths trust this ordering and do not sort or validate
/// the slice. Breaking the contract can split one metric into multiple
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
    /// Supplies the tag slice according to the trait contract above.
    #[doc(hidden)]
    fn __with_sorted_tags<R>(&self, f: impl FnOnce(&mut [RylvTag<'_>]) -> R) -> R;
}
