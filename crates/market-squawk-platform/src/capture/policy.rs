//! Bounded durability policy for the supervised capture writer.

use std::{num::NonZeroUsize, time::Duration};

use thiserror::Error;

const MAX_FLUSH_INTERVAL: Duration = Duration::from_secs(60);

/// Invalid background writer policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureWriterPolicyError {
    /// Capture flush intervals must be positive.
    #[error("capture flush interval must be greater than zero")]
    ZeroFlushInterval,
    /// A long flush interval defeats bounded durability and shutdown behavior.
    #[error("capture flush interval exceeds the maximum of 60 seconds")]
    FlushIntervalTooLong,
}

/// Background capture-writer flush policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureWriterPolicy {
    pub(super) flush_every_records: NonZeroUsize,
    pub(super) flush_interval: Duration,
}

impl CaptureWriterPolicy {
    /// Constructs a bounded flush policy.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureWriterPolicyError`] when the interval is zero or exceeds 60 seconds.
    pub fn try_new(
        flush_every_records: NonZeroUsize,
        flush_interval: Duration,
    ) -> Result<Self, CaptureWriterPolicyError> {
        if flush_interval.is_zero() {
            return Err(CaptureWriterPolicyError::ZeroFlushInterval);
        }
        if flush_interval > MAX_FLUSH_INTERVAL {
            return Err(CaptureWriterPolicyError::FlushIntervalTooLong);
        }
        Ok(Self {
            flush_every_records,
            flush_interval,
        })
    }
}

impl Default for CaptureWriterPolicy {
    fn default() -> Self {
        let flush_every_records = match NonZeroUsize::new(256) {
            Some(value) => value,
            None => NonZeroUsize::MIN,
        };
        Self {
            flush_every_records,
            flush_interval: Duration::from_secs(1),
        }
    }
}
