//! Bounded catalog evidence and exact analytical artifact verification.

use std::fmt;

use thiserror::Error;

mod canonical;
mod catalog;
pub(crate) mod fs;

pub(super) const MAX_PARQUET_METADATA_BYTES: u64 = 64 * 1024 * 1024;

/// Opaque identity of current relationship-bearing analytical catalog content.
///
/// This is intentionally not interchangeable with authority-transition evidence. Authority
/// evidence binds an immutable endpoint transition, while this digest binds one point-in-time
/// catalog snapshot whose content changes through ordinary ingestion.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CatalogContentEvidenceDigest([u8; 32]);

impl CatalogContentEvidenceDigest {
    /// Reconstructs a non-reserved digest from exact SHA-256 bytes.
    pub fn try_from_bytes(bytes: [u8; 32]) -> Option<Self> {
        if bytes == [0; 32] {
            None
        } else {
            Some(Self(bytes))
        }
    }

    pub(crate) fn try_new(bytes: [u8; 32]) -> Option<Self> {
        Self::try_from_bytes(bytes)
    }

    /// Returns the exact SHA-256 bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for CatalogContentEvidenceDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogContentEvidenceDigest([SHA-256])")
    }
}

/// Bounded analytical evidence validation failure.
#[derive(Debug, Error)]
pub(crate) enum EvidenceError {
    #[error("analytical evidence limits are invalid")]
    InvalidLimits,
    #[error("analytical catalog evidence is invalid")]
    InvalidCatalogEvidence,
    #[error("analytical generation evidence is semantically inconsistent")]
    GenerationSemanticMismatch,
    #[error("analytical evidence resource limit was exceeded")]
    ResourceLimitExceeded,
    #[error("analytical evidence verification was cancelled")]
    Cancelled,
    #[error("analytical artifact is not a private, single-link regular file")]
    UnsafeArtifact,
    #[error("analytical artifact bytes or Parquet metadata differ from catalog evidence")]
    ArtifactMetadataMismatch,
    #[error("analytical artifact path is invalid")]
    ArtifactPath(#[from] market_squawk_platform::ArtifactPathError),
    #[error("analytical artifact filesystem operation failed")]
    Io(#[from] std::io::Error),
    #[error("analytical artifact Parquet metadata is invalid")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("source and destination name the same retained artifact root")]
    SameRootRestore,
    #[error("analytical restore destination is not fresh")]
    DestinationNotFresh,
    #[error("analytical restore destination conflicts with the exact verified bundle subset")]
    DestinationConflict,
    #[error("analytical artifact materialization may be partially durable")]
    DestinationMaterializationIndeterminate,
}
pub(crate) use catalog::{
    ArtifactEvidenceRow, CatalogEvidenceSnapshot, EvidenceLimits, EvidenceSnapshotRequest,
    GenerationEvidenceRow, GenerationObjectEvidenceRow, GenerationParentEvidenceRow,
    ManifestEvidenceRow, QueryArtifactEvidenceRow,
};
