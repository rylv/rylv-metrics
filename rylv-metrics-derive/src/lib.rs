//! Derive support for compile-time ordered `rylv-metrics` tags.

use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, parse_quote, Attribute, Data, DeriveInput, Expr, Fields, Ident, LitStr,
    Path, Token,
};

struct AnonymousField {
    attrs: Vec<Attribute>,
    ident: Ident,
    optional: bool,
    value: Expr,
}

struct AnonymousTags {
    path: proc_macro2::TokenStream,
    fields: Vec<AnonymousField>,
}

impl Parse for AnonymousTags {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let path;
        syn::parenthesized!(path in input);
        let path = path.parse()?;
        let mut fields = Vec::new();
        while !input.is_empty() {
            let attrs = input.call(Attribute::parse_outer)?;
            let ident = input.parse()?;
            let optional = input.parse::<Option<Token![?]>>()?.is_some();
            input.parse::<Token![:]>()?;
            let value = input.parse()?;
            fields.push(AnonymousField {
                attrs,
                ident,
                optional,
                value,
            });
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(Self { path, fields })
    }
}

/// Implementation detail of `rylv_metrics::rylv_tags!`.
#[doc(hidden)]
#[proc_macro]
pub fn __rylv_tags(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as AnonymousTags);
    expand_anonymous(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_anonymous(input: &AnonymousTags) -> syn::Result<proc_macro2::TokenStream> {
    let path = &input.path;
    let lifetime = (!input.fields.is_empty()).then(|| quote!(<'__rylv_value>));
    let mut declarations = Vec::new();
    let mut definitions = Vec::new();
    let mut values = Vec::new();
    for field in &input.fields {
        let AnonymousField {
            attrs,
            ident,
            optional,
            value,
        } = field;
        for attr in attrs {
            if !attr.path().is_ident("rylv") {
                return Err(syn::Error::new_spanned(
                    attr,
                    "only `rylv` attributes are supported in rylv_tags!",
                ));
            }
        }
        let (_, skip) = field_options(attrs)?;
        if skip {
            return Err(syn::Error::new_spanned(
                ident,
                "`skip` is not supported in rylv_tags!",
            ));
        }
        let ty = if *optional {
            quote!(::core::option::Option<#path::RylvStr<'__rylv_value>>)
        } else {
            quote!(#path::RylvStr<'__rylv_value>)
        };
        declarations.push(quote!(#(#attrs)* #ident: #ty));
        definitions.push(quote!(pub #ident: #ty));
        values.push(quote!(#ident: #value));
    }
    let derived: DeriveInput = syn::parse2(quote! {
        struct __RylvAnonymousTags #lifetime { #(#declarations),* }
    })?;
    let implementation = expand_with_path(&derived, path)?;
    // Keep the expressions in the final struct literal so Rust extends the
    // lifetime of borrowed temporaries in a surrounding `let` initializer.
    Ok(quote! {{
        mod __rylv_anonymous {
            pub struct __RylvAnonymousTags #lifetime { #(#definitions),* }
            #implementation
        }
        __rylv_anonymous::__RylvAnonymousTags { #(#values),* }
    }})
}

/// Derives a tag set with one tag per non-skipped named field.
///
/// Field names become keys unless `#[rylv(rename = "key")]` is present.
/// `#[rylv(skip)]` omits a field. `#[rylv(crate = "path")]` on the struct
/// overrides the default `::rylv_metrics` path, for renamed dependencies.
/// Keys must be unique and cannot be empty or contain `:`, `,`, `|`, `#`,
/// whitespace, or line breaks. Every non-skipped field must have type `RylvStr`
/// or `Option<RylvStr>`. `None` omits the tag; `Some("")` includes an empty value.
///
/// ```ignore
/// #[derive(rylv_metrics::RylvTags)]
/// struct Tags<'a> {
///     env: rylv_metrics::RylvStr<'a>,
///     #[rylv(rename = "http.route")]
///     route: rylv_metrics::RylvStr<'a>,
/// }
/// ```
#[proc_macro_derive(RylvTags, attributes(rylv))]
pub fn derive_rylv_tags(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn crate_path(attrs: &[Attribute]) -> syn::Result<Path> {
    let mut path = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("rylv")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                if path.is_some() {
                    return Err(meta.error("duplicate `crate` attribute"));
                }
                let value: LitStr = meta.value()?.parse()?;
                path = Some(value.parse()?);
                Ok(())
            } else {
                Err(meta.error("unknown struct-level `rylv` attribute"))
            }
        })?;
    }
    Ok(path.unwrap_or_else(|| parse_quote!(::rylv_metrics)))
}

fn field_options(attrs: &[Attribute]) -> syn::Result<(Option<LitStr>, bool)> {
    let mut rename = None;
    let mut skip = false;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("rylv")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if rename.is_some() {
                    return Err(meta.error("duplicate `rename` attribute"));
                }
                rename = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("skip") {
                if skip {
                    return Err(meta.error("duplicate `skip` attribute"));
                }
                skip = true;
                Ok(())
            } else {
                Err(meta.error("unknown field-level `rylv` attribute"))
            }
        })?;
    }
    if skip && rename.is_some() {
        return Err(syn::Error::new_spanned(
            rename,
            "`skip` and `rename` cannot be combined",
        ));
    }
    Ok((rename, skip))
}

fn validate_key(key: &str, span: proc_macro2::Span) -> syn::Result<()> {
    if key.is_empty() {
        return Err(syn::Error::new(span, "tag key cannot be empty"));
    }
    for (character, label) in [(':', "colon"), (',', "comma"), ('|', "pipe"), ('#', "hash")] {
        if key.contains(character) {
            return Err(syn::Error::new(
                span,
                format!("tag key cannot contain {label} (`{character}`)"),
            ));
        }
    }
    if key.contains(['\n', '\r']) {
        return Err(syn::Error::new(span, "tag key cannot contain line breaks"));
    }
    if key.contains(' ') {
        return Err(syn::Error::new(span, "tag key cannot contain spaces"));
    }
    if key.contains(char::is_whitespace) {
        return Err(syn::Error::new(span, "tag key cannot contain whitespace"));
    }
    Ok(())
}

fn tag_tokens(
    path: &proc_macro2::TokenStream,
    ident: &syn::Ident,
    key: &LitStr,
) -> proc_macro2::TokenStream {
    quote! {
        #path::__RylvTagValue::optional_tag(&self.#ident, #key)
    }
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let path = crate_path(&input.attrs)?;
    expand_with_path(input, &quote!(#path))
}

fn expand_with_path(
    input: &DeriveInput,
    path: &proc_macro2::TokenStream,
) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "RylvTags can only be derived for structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "RylvTags requires named fields",
        ));
    };

    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    for field in &fields.named {
        let (rename, skip) = field_options(&field.attrs)?;
        if skip {
            continue;
        }
        let ident = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "RylvTags requires named fields"))?;
        let key = rename.as_ref().map_or_else(
            || ident.to_string().trim_start_matches("r#").to_owned(),
            LitStr::value,
        );
        let span = rename.as_ref().map_or_else(|| ident.span(), LitStr::span);
        validate_key(&key, span)?;
        if !seen.insert(key.clone()) {
            return Err(syn::Error::new(span, format!("duplicate tag key `{key}`")));
        }
        let key_literal = LitStr::new(&key, span);
        entries.push((format!("{key}:"), ident.clone(), key_literal, &field.ty));
    }
    entries.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let tags: Vec<_> = entries
        .iter()
        .map(|(_, ident, key, _)| tag_tokens(path, ident, key))
        .collect();
    let types = entries.iter().map(|entry| entry.3);
    let required_tags = entries.iter().map(|(_, ident, key, _)| {
        quote! {
            #path::__RylvTagValue::required_tag(&self.#ident, #key)
        }
    });
    let count = entries.len();
    let name = &input.ident;
    let mut guard_lifetime = syn::Lifetime::new("'__rylv_guard", proc_macro2::Span::call_site());
    while input
        .generics
        .lifetimes()
        .any(|parameter| parameter.lifetime.ident == guard_lifetime.ident)
    {
        guard_lifetime.ident = quote::format_ident!("{}_", guard_lifetime.ident);
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #path::RylvTags for #name #ty_generics #where_clause {
            type Guard<#guard_lifetime> = #path::RylvTagsGuard<#guard_lifetime, #count>
                where Self: #guard_lifetime;

            #[inline]
            fn sorted_tags(&self) -> Self::Guard<'_> {
                if false #(|| <#types as #path::__RylvTagValue>::OPTIONAL)* {
                    #path::RylvTagsGuard::from_optional([#(#tags),*])
                } else {
                    #path::RylvTagsGuard::new([#(#required_tags),*])
                }
            }
        }
    })
}
