//! Frozen comparable capture benchmark endpoint identities.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Endpoint families measured by both the standard and candidate backends.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Endpoint {
    /// Nonblocking producer-side queue installation.
    QueuePush,
    /// Consumer-side removal from a ready queue.
    QueuePop,
    /// Validation plus nonblocking capture admission.
    CaptureAdmission,
    /// One bounded writer append without an explicit flush.
    WriterAppend,
    /// One bounded writer append followed by flush.
    FlushInclusiveWriter,
}

impl Endpoint {
    pub(crate) const ALL: [Self; 5] = [
        Self::QueuePush,
        Self::QueuePop,
        Self::CaptureAdmission,
        Self::WriterAppend,
        Self::FlushInclusiveWriter,
    ];

    pub(crate) const fn has_deferred_writer_samples(self) -> bool {
        matches!(self, Self::WriterAppend | Self::FlushInclusiveWriter)
    }

    pub(crate) const fn has_deferred_samples(self) -> bool {
        matches!(
            self,
            Self::QueuePop | Self::WriterAppend | Self::FlushInclusiveWriter
        )
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::QueuePush => "queue_push",
            Self::QueuePop => "queue_pop",
            Self::CaptureAdmission => "capture_admission",
            Self::WriterAppend => "writer_append",
            Self::FlushInclusiveWriter => "flush_inclusive_writer",
        };
        formatter.write_str(name)
    }
}
