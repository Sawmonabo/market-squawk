//! Validated live provenance and point-in-time research metadata.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{SchemaVersion, SchemaVersionError, SourceIdentifier};

#[path = "provenance/live.rs"]
mod live;
#[path = "provenance/research.rs"]
mod research;

pub use live::{LiveProvenance, LiveVerificationState};
pub use research::{
    AvailabilityEvidence, ResearchContext, ResearchProvenance, ResearchTime, RevisionNumber,
};

/// Hash algorithm identifying how a retained payload digest was produced.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadHashAlgorithm {
    /// SHA-256 digest.
    Sha256,
    /// BLAKE3 digest.
    Blake3,
}

/// An algorithm-qualified 256-bit content digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadHash {
    algorithm: PayloadHashAlgorithm,
    digest: [u8; 32],
}

impl PayloadHash {
    /// Constructs an algorithm-qualified digest.
    pub const fn new(algorithm: PayloadHashAlgorithm, digest: [u8; 32]) -> Self {
        Self { algorithm, digest }
    }

    /// Returns the digest algorithm.
    pub const fn algorithm(self) -> PayloadHashAlgorithm {
        self.algorithm
    }

    /// Returns the digest bytes.
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// Durable evidence identifying the exact source payload behind a canonical record.
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
    /// Bounded provider, file-manifest, or capture-record reference.
    SourceReference(SourceIdentifier),
}

/// A live/research provenance or point-in-time invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceError {
    /// Local receive time is later than local ingestion time.
    ReceivedAfterIngested,
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
    /// Decoding cannot author `DirectVerified`; it requires a successful qualification.
    UnqualifiedDirectVerified,
    /// Qualification evidence was not eligible and direct verified.
    QualificationNotEligible,
    /// Qualification evidence describes a different source, venue, instrument, or generation.
    QualificationIdentityMismatch,
    /// Qualification timing does not describe the decoded market event.
    QualificationTimingMismatch,
    /// Qualification coverage does not match decoded provenance.
    QualificationCoverageMismatch,
    /// A recorded direct-verified label lacks its required evidence identity.
    MissingQualificationEvidenceId,
    /// Serialized input uses an unsupported schema version.
    SchemaVersion(SchemaVersionError),
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReceivedAfterIngested => {
                formatter.write_str("receive time must not be later than ingestion time")
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
            Self::QualificationNotEligible => formatter
                .write_str("direct-verified provenance requires successful qualification evidence"),
            Self::QualificationIdentityMismatch => formatter.write_str(
                "qualification source, venue, instrument, or generation does not match provenance",
            ),
            Self::QualificationTimingMismatch => {
                formatter.write_str("qualification timing does not match provenance")
            }
            Self::QualificationCoverageMismatch => {
                formatter.write_str("qualification coverage does not match provenance")
            }
            Self::MissingQualificationEvidenceId => formatter.write_str(
                "recorded direct-verified provenance requires a qualification evidence identity",
            ),
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
