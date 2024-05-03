use hdrhistogram::errors::{CreationError, RecordError};
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
