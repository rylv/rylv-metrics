# rylv-metrics-derive

Optional `#[derive(RylvTags)]` support for `rylv-metrics`. Enable the `derive`
feature on `rylv-metrics` to use it. The macro creates a stack-allocated tag
slice in canonical order, allowing `*_typed` collector calls to skip sorting.

Fields can use `RylvStr<'_>` or `Option<RylvStr<'_>>`. An optional field with
`None` omits its tag; `Some(RylvStr::from_static(""))` includes an empty value.
Optional tags are compacted on the stack without a heap allocation.
Type aliases and renamed imports of these types are supported. Structs whose
fields are all required use the fixed-length guard constructor, selected at
compile time.

`rylv_metrics::rylv_tags!` creates the same typed tag set inline:

```rust
use rylv_metrics::{rylv_tags, RylvStr};

let tags = rylv_tags! {
    env: RylvStr::from_static("prod"),
    #[rylv(rename = "http-route")]
    route: RylvStr::from_static("/users"),
    region?: None,
};
```

Append `?` to a field name for an optional value. Each expression is evaluated
once in declaration order. This macro supports `rename`; omit unwanted fields
directly rather than using `skip`.
