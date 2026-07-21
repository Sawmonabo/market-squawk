//! Validated live provenance and point-in-time research metadata.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{DigestAlgorithm, SchemaVersion, SchemaVersionError, SourceIdentifier};

#[path = "provenance/live.rs"]
mod live;
#[path = "provenance/research.rs"]
mod research;

pub use live::{
    DecodedLiveProvenanceInput, LiveProvenance, LiveRecordState, RecordedLiveProvenanceInput,
};
pub use research::{
    AvailabilityEvidence, ResearchContext, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTemporalPrecision, ResearchTime, RevisionNumber,
};

/// An algorithm-qualified 256-bit content digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadHash {
    algorithm: DigestAlgorithm,
    digest: [u8; 32],
}

impl PayloadHash {
    /// Constructs an algorithm-qualified digest.
    pub const fn new(algorithm: DigestAlgorithm, digest: [u8; 32]) -> Self {
        Self { algorithm, digest }
    }

    /// Returns the digest algorithm.
    pub const fn algorithm(self) -> DigestAlgorithm {
        self.algorithm
    }

    /// Returns the digest bytes.
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// A content digest or opaque source-side record locator retained for provenance.
///
/// A [`Self::SourceReference`] is only a bounded identity supplied by its caller. It carries no
/// inherent existence, immutability, or retrievability guarantee.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "value",
    rename_all = "snake_case"
)]
pub enum PayloadReference {
    /// Algorithm-qualified content digest.
    ContentHash(PayloadHash),
    /// Opaque provider, file-manifest, or capture-record identity.
    SourceReference(SourceIdentifier),
}

/// A live/research provenance or point-in-time invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceError {
    /// Local receive time is later than local ingestion time.
    ReceivedAfterIngested,
    /// Local availability precedes local receipt.
    AvailabilityBeforeReceived,
    /// Payload content hash does not match the complete live binding.
    PayloadDigestMismatch,
    /// Availability evidence is later than local ingestion.
    AvailabilityAfterIngested,
    /// A source claims availability before its known publication time.
    AvailabilityBeforePublished,
    /// A superseding revision is not strictly later than publication.
    SupersededNotAfterPublished,
    /// A superseding revision is not strictly later than conservative availability.
    SupersededNotAfterAvailable,
    /// Revision numbers are one-based.
    ZeroRevision,
    /// Decoder output cannot claim `DirectVerified`; a recorded label is only a caller-supplied
    /// archival assertion paired with an opaque reference and does not prove successful
    /// qualification.
    UnqualifiedDirectVerified,
    /// A recorded direct-verified label lacks its required assessment reference.
    MissingAssessmentReference,
    /// Serialized input uses an unsupported schema version.
    SchemaVersion(SchemaVersionError),
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceivedAfterIngested => {
                formatter.write_str("receive time must not be later than ingestion time")
            }
            Self::AvailabilityBeforeReceived => {
                formatter.write_str("availability time must not precede receive time")
            }
            Self::PayloadDigestMismatch => {
                formatter.write_str("payload reference digest does not match live binding")
            }
            Self::AvailabilityAfterIngested => {
                formatter.write_str("availability evidence must not be later than ingestion time")
            }
            Self::AvailabilityBeforePublished => {
                formatter.write_str("availability evidence must not precede publication time")
            }
            Self::SupersededNotAfterPublished => {
                formatter.write_str("superseded time must be later than publication time")
            }
            Self::SupersededNotAfterAvailable => {
                formatter.write_str("superseded time must be later than conservative availability")
            }
            Self::ZeroRevision => formatter.write_str("revision number must be nonzero"),
            Self::UnqualifiedDirectVerified => {
                formatter.write_str("decoded provenance cannot claim direct-verified quality")
            }
            Self::MissingAssessmentReference => formatter
                .write_str("recorded direct-verified provenance requires an assessment reference"),
            Self::SchemaVersion(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProvenanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SchemaVersion(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SchemaVersionError> for ProvenanceError {
    fn from(value: SchemaVersionError) -> Self {
        Self::SchemaVersion(value)
    }
}

pub(super) fn ensure_current_schema(schema_version: SchemaVersion) -> Result<(), ProvenanceError> {
    schema_version
        .ensure_supported()
        .map(|_| ())
        .map_err(Into::into)
}
