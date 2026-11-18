# rylv-metrics-derive

Optional `#[derive(RylvTags)]` support for `rylv-metrics`. Enable the `derive`
feature on `rylv-metrics` to use it. The macro creates a stack-allocated tag
slice in canonical order, allowing `*_typed` collector calls to skip sorting.
