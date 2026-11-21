// Target-specific exceptions to the shared library lint policy.
#![allow(missing_docs)]
#![cfg(feature = "derive")]

#[test]
fn invalid_derive_inputs_are_rejected() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/typed_tags_*.rs");
}
