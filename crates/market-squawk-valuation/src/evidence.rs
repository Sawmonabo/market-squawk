//! Immutable source, venue, dataset, analytical, and portfolio evidence bindings.

use std::mem::size_of;

use market_squawk_analytics::{FeatureKey, FeatureSemanticDigest};
use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_portfolio::PortfolioRevisionToken;

use crate::{CanonicalHasher, FairValueError, checked_add};

digest_id!(
    /// SHA-256 commitment to one complete fair-value evidence binding.
    FairValueEvidenceHash
);

/// Whether the source-specific admission boundary verified the retained evidence binding.
///
/// This analytical verification is independent of live [`market_squawk_domain::DataQuality`]
/// and does not grant execution authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EvidenceVerification {
    /// Source identity, content identity, and referenced immutable origin were verified.
    Verified,
    /// One or more fair-value evidence checks remain unresolved.
    Unverified,
}

/// Immutable producer identity behind one valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvidenceOrigin {
    /// Direct market observation on a named venue.
    Market {
        /// Venue supplying the quote or transaction.
        venue_id: VenueId,
    },
    /// Exact immutable research dataset generation.
    Research {
        /// Manifest-pinned source generation.
        manifest: DatasetManifestRef,
    },
    /// Exact feature output and its immutable research input generation.
    Analytics {
        /// Versioned feature identity.
        feature_key: FeatureKey,
        /// Complete semantic feature identity.
        semantic_digest: FeatureSemanticDigest,
        /// Manifest-pinned analytical input generation.
        manifest: DatasetManifestRef,
    },
    /// Exact immutable portfolio revision.
    Portfolio {
        /// Opaque immutable revision precondition.
        revision: PortfolioRevisionToken,
    },
}

impl EvidenceOrigin {
    pub(crate) fn hash_into(&self, hash: &mut CanonicalHasher) {
        match self {
            Self::Market { venue_id } => {
                hash.u8(1);
                hash.bytes(venue_id.as_str().as_bytes());
            }
            Self::Research { manifest } => {
                hash.u8(2);
                hash_manifest(hash, manifest);
            }
            Self::Analytics {
                feature_key,
                semantic_digest,
                manifest,
            } => {
                hash.u8(3);
                hash.bytes(feature_key.name().as_bytes());
                hash.u32(feature_key.version().get());
                hash.fixed(semantic_digest.as_bytes());
                hash_manifest(hash, manifest);
            }
            Self::Portfolio { revision } => {
                hash.u8(4);
                hash.fixed(revision.bytes());
            }
        }
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize, FairValueError> {
        match self {
            Self::Market { venue_id } => Ok(venue_id.retained_bytes()),
            Self::Research { manifest } => manifest_retained_bytes(manifest),
            Self::Analytics {
                feature_key,
                manifest,
                ..
            } => checked_add(feature_key.name().len(), manifest_retained_bytes(manifest)?),
            Self::Portfolio { .. } => Ok(0),
        }
    }

    pub(crate) const fn venue_id(&self) -> Option<&VenueId> {
        match self {
            Self::Market { venue_id } => Some(venue_id),
            Self::Research { .. } | Self::Analytics { .. } | Self::Portfolio { .. } => None,
        }
    }
}

/// Untrusted fields used to construct an immutable evidence binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueEvidenceInput {
    /// Stable source identity.
    pub source_id: SourceId,
    /// Source-defined record identity.
    pub source_identifier: SourceIdentifier,
    /// Exact source payload content.
    pub payload_digest: EvidenceDigest,
    /// Immutable producer origin.
    pub origin: EvidenceOrigin,
    /// Source-authored observation time.
    pub source_timestamp: Timestamp,
    /// Time the evidence became available for use.
    pub available_at: Timestamp,
    /// Time Market Squawk durably ingested the evidence.
    pub ingested_at: Timestamp,
    /// Result of source-specific fair-value evidence admission.
    pub verification: EvidenceVerification,
}

/// Complete immutable evidence for one valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueEvidence {
    source_id: SourceId,
    source_identifier: SourceIdentifier,
    payload_digest: EvidenceDigest,
    origin: EvidenceOrigin,
    source_timestamp: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    verification: EvidenceVerification,
    hash: FairValueEvidenceHash,
    retained_bytes: usize,
}

impl FairValueEvidence {
    /// Validates provenance ordering and derives a deterministic content identity.
    ///
    /// # Errors
    ///
    /// Rejects zero payload identities or non-monotonic source/availability/ingestion times.
    pub fn try_new(input: FairValueEvidenceInput) -> Result<Self, FairValueError> {
        if input.payload_digest.bytes() == [0; 32] {
            return Err(FairValueError::InvalidEvidenceDigest);
        }
        if input.source_timestamp > input.available_at || input.available_at > input.ingested_at {
            return Err(FairValueError::InvalidTime);
        }
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(
                input.source_id.retained_bytes(),
                checked_add(
                    input.source_identifier.retained_bytes(),
                    input.origin.retained_bytes()?,
                )?,
            )?,
        )?;
        let mut hash = CanonicalHasher::new(b"market-squawk/fair-value-evidence/v1");
        hash.bytes(input.source_id.as_str().as_bytes());
        hash.bytes(input.source_identifier.as_str().as_bytes());
        hash.u8(match input.payload_digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        });
        hash.fixed(input.payload_digest.bytes());
        input.origin.hash_into(&mut hash);
        hash.i64(input.source_timestamp.unix_nanos());
        hash.i64(input.available_at.unix_nanos());
        hash.i64(input.ingested_at.unix_nanos());
        hash.u8(match input.verification {
            EvidenceVerification::Verified => 1,
            EvidenceVerification::Unverified => 2,
        });
        Ok(Self {
            source_id: input.source_id,
            source_identifier: input.source_identifier,
            payload_digest: input.payload_digest,
            origin: input.origin,
            source_timestamp: input.source_timestamp,
            available_at: input.available_at,
            ingested_at: input.ingested_at,
            verification: input.verification,
            hash: FairValueEvidenceHash(hash.finish()),
            retained_bytes,
        })
    }

    /// Returns the source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the source-defined record identity.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    /// Returns the exact source payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the immutable producer origin.
    pub const fn origin(&self) -> &EvidenceOrigin {
        &self.origin
    }

    /// Returns the source-authored observation time.
    pub const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns the evidence availability time.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the local ingestion time.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the source-specific admission result.
    pub const fn verification(&self) -> EvidenceVerification {
        self.verification
    }

    /// Returns the complete deterministic evidence identity.
    pub const fn hash(&self) -> FairValueEvidenceHash {
        self.hash
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn hash_manifest(hash: &mut CanonicalHasher, manifest: &DatasetManifestRef) {
    hash.bytes(manifest.dataset_id().as_str().as_bytes());
    hash.u64(manifest.manifest_version());
    hash.bytes(manifest.schema().name().as_bytes());
    hash.u32(u32::from(manifest.schema_version().get()));
    hash.fixed(manifest.schema().fingerprint());
    hash.fixed(manifest.content_hash().bytes());
}

fn manifest_retained_bytes(manifest: &DatasetManifestRef) -> Result<usize, FairValueError> {
    checked_add(
        manifest.dataset_id().as_str().len(),
        manifest.schema().name().len(),
    )
}
