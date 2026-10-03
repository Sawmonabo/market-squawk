//! Logical market-event publication identities, independent of hot or archived placement.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};

use crate::{CatalogError, DatasetId, DatasetSchemaRef, DatasetSchemaRegistry, Sha256Digest};

/// A committed event publication and the immutable dataset horizon it establishes.
///
/// Successful ingest returns this identity after rows, evidence and run completion commit together.
/// Reconstructing an identity does not authorize access: readers validate it against the catalog.
/// Physical archival never changes its identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketEventCommitRef {
    dataset_id: DatasetId,
    sequence: u64,
    schema: DatasetSchemaRef,
    content_hash: Sha256Digest,
    available_at: Timestamp,
    publication_digest: EvidenceDigest,
    row_count: u64,
}

impl MarketEventCommitRef {
    /// Reconstructs checked coordinates; the catalog independently verifies their membership.
    #[allow(
        clippy::too_many_arguments,
        reason = "the catalog reconstructs all immutable commit coordinates together"
    )]
    pub fn try_new(
        dataset_id: DatasetId,
        sequence: u64,
        schema: DatasetSchemaRef,
        content_hash: Sha256Digest,
        available_at: Timestamp,
        publication_digest: EvidenceDigest,
        row_count: u64,
    ) -> Result<Self, CatalogError> {
        if sequence == 0
            || i64::try_from(sequence).is_err()
            || row_count == 0
            || i64::try_from(row_count).is_err()
            || content_hash.bytes() == [0; 32]
            || publication_digest.algorithm() != DigestAlgorithm::Sha256
            || publication_digest.bytes() == [0; 32]
            || DatasetSchemaRegistry::local()
                .canonical_market_events()
                .map_err(|_| CatalogError::InvalidRecord)?
                != schema
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            dataset_id,
            sequence,
            schema,
            content_hash,
            available_at,
            publication_digest,
            row_count,
        })
    }

    /// Returns the logical event collection.
    pub const fn dataset_id(&self) -> &DatasetId {
        &self.dataset_id
    }

    /// Returns the committed collection horizon, including this publication.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the canonical event schema identity.
    pub const fn schema(&self) -> &DatasetSchemaRef {
        &self.schema
    }

    /// Returns the logical commit-chain digest, independent of file layout.
    pub const fn content_hash(&self) -> Sha256Digest {
        self.content_hash
    }

    /// Returns the original catalog publication clock.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the source publication that established this horizon.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    /// Returns this publication's row count, not the cumulative collection size.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }
}
