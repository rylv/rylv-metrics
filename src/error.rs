use hdrhistogram::errors::{CreationError, RecordError};
#[cfg(feature = "udp")]
use rustix::io::Errno;
use thiserror::Error;

/// Errors that can occur during metric collection and transmission.
#[derive(Error, Debug)]
pub enum MetricsError {
    /// A custom error with a free-form message.
    #[error("Custom error: {0}")]
    Custom(String),

    /// An I/O error from the standard library.
    #[error("Std Io error: {0}")]
    StdIo(#[from] std::io::Error),

    /// A low-level system errno.
    #[cfg(feature = "udp")]
    #[error("Errno error: {0}")]
    Errno(#[from] Errno),

    /// Failed to record a value into a histogram.
    #[error("Histogram record error: {0}")]
    Histogram(#[from] RecordError),

    /// Failed to create a histogram.
    #[error("Histogram creation error: {0}")]
    HistogramCreation(#[from] CreationError),
}

impl From<String> for MetricsError {
    fn from(value: String) -> Self {
        Self::Custom(value)
    }
}

impl From<&str> for MetricsError {
    fn from(value: &str) -> Self {
        Self::Custom(value.to_string())
    }
}

/// Wraps a rejected histogram value into a [`MetricsError`].
///
/// The collectors record into histograms through functions that return the
/// one-byte [`RecordError`] and convert it here, so the success path never
/// builds or drops the larger `MetricsError` result and the conversion stays
/// out of the hot code.
#[cfg(any(feature = "tls-collector", feature = "shared-collector"))]
#[cold]
#[inline(never)]
pub const fn histogram_record_error(err: RecordError) -> MetricsError {
    MetricsError::Histogram(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_string() {
        let err = MetricsError::from("something went wrong".to_string());
        assert!(err.to_string().contains("something went wrong"));
    }

    #[test]
    fn from_str() {
        let err = MetricsError::from("oops");
        assert!(err.to_string().contains("oops"));
    }
}
