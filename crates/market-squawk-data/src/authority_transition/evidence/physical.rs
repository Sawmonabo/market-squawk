//! One independently verified physical row. Archives never acquire fictitious artifact IDs.
use crate::{DatasetSchemaRef, Sha256Digest};

#[derive(Debug)]
pub(crate) enum PhysicalArtifactEvidence {
    Artifact {
        relative_reference: Box<str>,
        content_hash: Sha256Digest,
        size_bytes: u64,
        expected_row_count: Option<u64>,
    },
    QueryArtifact {
        relative_reference: Box<str>,
        content_hash: Sha256Digest,
        size_bytes: u64,
        expected_row_count: Option<u64>,
    },
    MarketEventArchive {
        relative_reference: Box<str>,
        content_hash: Sha256Digest,
        size_bytes: u64,
        expected_row_count: u64,
        schema: DatasetSchemaRef,
    },
}
impl PhysicalArtifactEvidence {
    pub(crate) fn relative_reference(&self) -> &str {
        match self {
            Self::Artifact {
                relative_reference, ..
            }
            | Self::QueryArtifact {
                relative_reference, ..
            }
            | Self::MarketEventArchive {
                relative_reference, ..
            } => relative_reference,
        }
    }
    pub(crate) fn content_hash(&self) -> Sha256Digest {
        match self {
            Self::Artifact { content_hash, .. }
            | Self::QueryArtifact { content_hash, .. }
            | Self::MarketEventArchive { content_hash, .. } => *content_hash,
        }
    }
    pub(crate) fn size_bytes(&self) -> u64 {
        match self {
            Self::Artifact { size_bytes, .. }
            | Self::QueryArtifact { size_bytes, .. }
            | Self::MarketEventArchive { size_bytes, .. } => *size_bytes,
        }
    }
    pub(crate) fn expected_row_count(&self) -> Option<u64> {
        match self {
            Self::Artifact {
                expected_row_count, ..
            }
            | Self::QueryArtifact {
                expected_row_count, ..
            } => *expected_row_count,
            Self::MarketEventArchive {
                expected_row_count, ..
            } => Some(*expected_row_count),
        }
    }
    pub(crate) fn schema(&self) -> Option<&DatasetSchemaRef> {
        match self {
            Self::MarketEventArchive { schema, .. } => Some(schema),
            _ => None,
        }
    }
}
