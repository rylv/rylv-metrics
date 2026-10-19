//! Plain-decimal float formatting for `DogStatsD` values.

use std::fmt::Write as _;

/// Formats a float as a plain decimal: never scientific notation, no grouping
/// separators, at most 6 fraction digits with ties rounded to even (over the
/// exact binary value), and trailing fraction zeros omitted. This matches the
/// double formatting of the established `DogStatsD` client libraries.
///
/// Non-finite values format as `NaN` / `inf` / `-inf` (means of non-empty
/// windows are always finite). Values with more than 40 integer digits are
/// outside the metric domain and are printed in full.
pub fn format_float(value: f64, out: &mut String) -> &str {
    out.clear();
    // `{value:.6}` rounds the exact binary value half-to-even.
    let _ = write!(out, "{value:.6}");
    if out.contains('.') {
        while out.ends_with('0') {
            out.pop();
        }
        if out.ends_with('.') {
            out.pop();
        }
    }
    out.as_str()
}

#[cfg(test)]
mod tests {
    use super::format_float;

    fn fmt(value: f64) -> String {
        format_float(value, &mut String::new()).to_owned()
    }

    #[test]
    fn formats_plain_decimal_without_trailing_zeros() {
        assert_eq!(fmt(1.0), "1");
        assert_eq!(fmt(1.5), "1.5");
        assert_eq!(fmt(100.0), "100");
        assert_eq!(fmt(0.5), "0.5");
        assert_eq!(fmt(-0.25), "-0.25");
        assert_eq!(fmt(1.000_000_1), "1");
        assert_eq!(fmt(0.000_000_4), "0");
        assert_eq!(fmt(-0.0), "-0");
        assert_eq!(fmt(0.0), "0");
        assert_eq!(fmt(123_456_789.123_456_8), "123456789.123457");
        // Ties over the exact binary value round half-to-even.
        assert_eq!(fmt(1.000_000_5), "1.000001");
        assert_eq!(fmt(2.000_000_5), "2.000001");
        assert_eq!(fmt(0.000_000_5), "0");
        assert_eq!(fmt(0.000_001_5), "0.000002");
    }
}
