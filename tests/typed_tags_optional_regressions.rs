#![allow(missing_docs)]
#![cfg(feature = "derive")]

use rylv_metrics::{RylvStr, RylvTag, RylvTags};

#[test]
fn local_option_name_does_not_affect_qualified_optional_fields() {
    struct Option;

    #[derive(RylvTags)]
    struct Tags {
        env: ::core::option::Option<RylvStr<'static>>,
    }

    let _ = Option;
    let tags = Tags {
        env: Some(RylvStr::from_static("prod")),
    };
    assert_eq!(
        tags.sorted_tags().as_ref(),
        &[RylvTag::from_static("env:prod")]
    );
    assert!(Tags { env: None }.sorted_tags().as_ref().is_empty());
}

#[test]
fn local_some_function_cannot_remove_required_tags() {
    #[allow(non_snake_case, clippy::unnecessary_wraps)]
    fn Some<T>(_: T) -> Option<T> {
        None
    }

    #[derive(RylvTags)]
    struct Tags {
        env: RylvStr<'static>,
        region: Option<RylvStr<'static>>,
    }

    assert!(Some(()).is_none());
    for region in [
        None,
        ::core::option::Option::Some(RylvStr::from_static("us")),
    ] {
        let tags = Tags {
            env: RylvStr::from_static("prod"),
            region,
        };
        let guard = tags.sorted_tags();
        let mut expected = vec![RylvTag::from_static("env:prod")];
        if tags.region.is_some() {
            expected.push(RylvTag::from_static("region:us"));
        }
        assert_eq!(guard.as_ref(), expected.as_slice());
    }
}

#[test]
fn forwarded_optional_type_is_recognized() {
    macro_rules! define_tags {
        ($ty:ty) => {
            #[derive(RylvTags)]
            struct Tags {
                env: $ty,
            }
        };
    }
    macro_rules! forward_type {
        ($ty:ty) => {
            define_tags!($ty);
        };
    }
    forward_type!(Option<RylvStr<'static>>);

    let tags = Tags {
        env: Some(RylvStr::from_static("prod")),
    };
    assert_eq!(
        tags.sorted_tags().as_ref(),
        &[RylvTag::from_static("env:prod")]
    );
    assert!(Tags { env: None }.sorted_tags().as_ref().is_empty());
}

#[test]
#[allow(unused_parens)]
fn nested_parentheses_preserve_optional_type() {
    #[derive(RylvTags)]
    struct Tags {
        env: ((::core::option::Option<RylvStr<'static>>)),
    }

    let tags = Tags {
        env: Some(RylvStr::from_static("prod")),
    };
    assert_eq!(
        tags.sorted_tags().as_ref(),
        &[RylvTag::from_static("env:prod")]
    );
    assert!(Tags { env: None }.sorted_tags().as_ref().is_empty());
}
