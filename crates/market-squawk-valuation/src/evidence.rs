//! Producer-derived source, market, dataset, analytical, and portfolio evidence bindings.

use std::mem::size_of;

use market_squawk_analytics::FeatureKey;
use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::{
    AccountId, DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use rust_decimal::Decimal;

use crate::{CanonicalHasher, FairValueError, checked_add};

digest_id!(
    /// SHA-256 commitment to one complete fair-value evidence binding.
    FairValueEvidenceHash
);

/// Whether the producer-specific admission boundary established a complete usable binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EvidenceVerification {
    /// The producer receipt and required provenance/time fields are complete.
    Verified,
    /// One or more required producer fields were explicitly unavailable.
    Unverified,
}

/// Immutable producer identity behind one valuation input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvidenceOrigin {
    /// Post-commit directly verified market observation and activity evidence set.
    Market {
        /// Exact venue supplying the selected observation.
        venue_id: VenueId,
        /// Producer qualification assessment identity.
        assessment_id: SourceIdentifier,
        /// Complete live binding identity.
        binding_digest: [u8; 32],
        /// Exact canonical state identity.
        canonical_state_digest: EvidenceDigest,
        /// Instrument-owned state revision published by commit.
        committed_state_revision: u64,
        /// Instrument-definition revision used for normalization.
        definition_revision: u64,
        /// Complete market-activity policy identity.
        activity_policy_hash: [u8; 32],
        /// Canonical set of genuine receipts evaluated by that policy.
        activity_set_hash: [u8; 32],
    },
    /// Exact selected cell from a manifest-pinned query output.
    Research {
        /// Manifest-pinned source generation.
        manifest: DatasetManifestRef,
        /// Complete catalog-resolved generation/object graph.
        object_graph_digest: EvidenceDigest,
        /// Manifest, SQL, and execution-limit identity.
        query_identity: EvidenceDigest,
        /// Exact query result identity.
        result_digest: EvidenceDigest,
        /// Selected result row.
        row: usize,
        /// Source revision retained by the selected row.
        revision: u32,
    },
    /// Exact analytical feature identity derived from a manifest-pinned query output.
    Analytics {
        /// Versioned feature identity.
        feature_key: FeatureKey,
        /// Complete semantic feature identity.
        semantic_digest: [u8; 32],
        /// Manifest-pinned analytical input generation.
        manifest: DatasetManifestRef,
        /// Complete catalog-resolved generation/object graph.
        object_graph_digest: EvidenceDigest,
        /// Manifest, SQL, and execution-limit identity.
        query_identity: EvidenceDigest,
        /// Exact query result identity.
        result_digest: EvidenceDigest,
        /// Selected result row.
        row: usize,
        /// Source revision retained by the selected row.
        revision: u32,
    },
    /// Exact immutable portfolio revision and selected position.
    Portfolio {
        /// Opaque immutable revision identity derived from the actual revision object.
        revision: [u8; 32],
        /// Exact account owning the revision.
        account_id: AccountId,
        /// Exact selected position quantity.
        position_quantity: Decimal,
        /// Complete point-in-time portfolio evidence identity.
        point_in_time_digest: [u8; 32],
    },
}

impl EvidenceOrigin {
    pub(crate) fn hash_into(&self, hash: &mut CanonicalHasher) {
        match self {
            Self::Market {
                venue_id,
                assessment_id,
                binding_digest,
                canonical_state_digest,
                committed_state_revision,
                definition_revision,
                activity_policy_hash,
                activity_set_hash,
            } => {
                hash.u8(1);
                hash.bytes(venue_id.as_str().as_bytes());
                hash.bytes(assessment_id.as_str().as_bytes());
                hash.fixed(*binding_digest);
                hash_digest(hash, *canonical_state_digest);
                hash.u64(*committed_state_revision);
                hash.u64(*definition_revision);
                hash.fixed(*activity_policy_hash);
                hash.fixed(*activity_set_hash);
            }
            Self::Research {
                manifest,
                object_graph_digest,
                query_identity,
                result_digest,
                row,
                revision,
            } => {
                hash.u8(2);
                hash_manifest(hash, manifest);
                hash_digest(hash, *object_graph_digest);
                hash_digest(hash, *query_identity);
                hash_digest(hash, *result_digest);
                hash.u64(u64::try_from(*row).unwrap_or(u64::MAX));
                hash.u32(*revision);
            }
            Self::Analytics {
                feature_key,
                semantic_digest,
                manifest,
                object_graph_digest,
                query_identity,
                result_digest,
                row,
                revision,
            } => {
                hash.u8(3);
                hash.bytes(feature_key.name().as_bytes());
                hash.u32(feature_key.version().get());
                hash.fixed(*semantic_digest);
                hash_manifest(hash, manifest);
                hash_digest(hash, *object_graph_digest);
                hash_digest(hash, *query_identity);
                hash_digest(hash, *result_digest);
                hash.u64(u64::try_from(*row).unwrap_or(u64::MAX));
                hash.u32(*revision);
            }
            Self::Portfolio {
                revision,
                account_id,
                position_quantity,
                point_in_time_digest,
            } => {
                hash.u8(4);
                hash.fixed(*revision);
                hash.bytes(account_id.as_uuid().as_bytes());
                hash.bytes(&position_quantity.mantissa().to_be_bytes());
                hash.u32(position_quantity.scale());
                hash.fixed(*point_in_time_digest);
            }
        }
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize, FairValueError> {
        match self {
            Self::Market {
                venue_id,
                assessment_id,
                ..
            } => checked_add(venue_id.retained_bytes(), assessment_id.retained_bytes()),
            Self::Research { manifest, .. } => manifest_retained_bytes(manifest),
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
            Self::Market { venue_id, .. } => Some(venue_id),
            Self::Research { .. } | Self::Analytics { .. } | Self::Portfolio { .. } => None,
        }
    }

    pub(crate) const fn is_market(&self) -> bool {
        matches!(self, Self::Market { .. })
    }

    pub(crate) const fn is_research(&self) -> bool {
        matches!(self, Self::Research { .. })
    }

    pub(crate) const fn market_activity_policy_hash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Market {
                activity_policy_hash,
                ..
            } => Some(*activity_policy_hash),
            Self::Research { .. } | Self::Analytics { .. } | Self::Portfolio { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FairValueEvidenceParts {
    pub(crate) source_id: SourceId,
    pub(crate) source_identifier: SourceIdentifier,
    pub(crate) payload_digest: EvidenceDigest,
    pub(crate) origin: EvidenceOrigin,
    pub(crate) source_timestamp: Option<Timestamp>,
    pub(crate) effective_at: Option<Timestamp>,
    pub(crate) published_at: Option<Timestamp>,
    pub(crate) available_at: Option<Timestamp>,
    pub(crate) received_at: Option<Timestamp>,
    pub(crate) ingested_at: Timestamp,
    pub(crate) verification: EvidenceVerification,
}

/// Complete immutable evidence derived from an admitted producer object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FairValueEvidence {
    source_id: SourceId,
    source_identifier: SourceIdentifier,
    payload_digest: EvidenceDigest,
    origin: EvidenceOrigin,
    source_timestamp: Option<Timestamp>,
    effective_at: Option<Timestamp>,
    published_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    received_at: Option<Timestamp>,
    ingested_at: Timestamp,
    verification: EvidenceVerification,
    hash: FairValueEvidenceHash,
    retained_bytes: usize,
}

impl FairValueEvidence {
    pub(crate) fn try_from_parts(parts: FairValueEvidenceParts) -> Result<Self, FairValueError> {
        let available = parts.available_at;
        let observed_times = [parts.source_timestamp, parts.published_at];
        let origin_order_invalid = available.is_some_and(|available_at| {
            (parts.origin.is_market()
                && parts
                    .source_timestamp
                    .is_some_and(|value| value > available_at))
                || (parts.origin.is_research()
                    && parts.published_at.is_some_and(|value| value > available_at))
        });
        let receive_order_invalid =
            parts
                .received_at
                .zip(available)
                .is_some_and(|(received, available_at)| {
                    if parts.origin.is_market() {
                        received > available_at
                    } else {
                        available_at > received
                    }
                });
        let verified_incomplete = parts.verification == EvidenceVerification::Verified
            && (available.is_none()
                || (parts.source_timestamp.is_none() && parts.effective_at.is_none()));
        if parts.payload_digest.bytes() == [0; 32]
            || origin_order_invalid
            || receive_order_invalid
            || observed_times
                .into_iter()
                .flatten()
                .any(|value| value > parts.ingested_at)
            || available.is_some_and(|value| value > parts.ingested_at)
            || parts
                .received_at
                .is_some_and(|value| value > parts.ingested_at)
            || verified_incomplete
        {
            return Err(FairValueError::InvalidTime);
        }
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(
                parts.source_id.retained_bytes(),
                checked_add(
                    parts.source_identifier.retained_bytes(),
                    parts.origin.retained_bytes()?,
                )?,
            )?,
        )?;
        let mut hash = CanonicalHasher::new(b"market-squawk/fair-value-evidence/v2");
        hash.bytes(parts.source_id.as_str().as_bytes());
        hash.bytes(parts.source_identifier.as_str().as_bytes());
        hash_digest(&mut hash, parts.payload_digest);
        parts.origin.hash_into(&mut hash);
        hash_optional_time(&mut hash, parts.source_timestamp);
        hash_optional_time(&mut hash, parts.effective_at);
        hash_optional_time(&mut hash, parts.published_at);
        hash_optional_time(&mut hash, parts.available_at);
        hash_optional_time(&mut hash, parts.received_at);
        hash.i64(parts.ingested_at.unix_nanos());
        hash.u8(match parts.verification {
            EvidenceVerification::Verified => 1,
            EvidenceVerification::Unverified => 2,
        });
        Ok(Self {
            source_id: parts.source_id,
            source_identifier: parts.source_identifier,
            payload_digest: parts.payload_digest,
            origin: parts.origin,
            source_timestamp: parts.source_timestamp,
            effective_at: parts.effective_at,
            published_at: parts.published_at,
            available_at: parts.available_at,
            received_at: parts.received_at,
            ingested_at: parts.ingested_at,
            verification: parts.verification,
            hash: FairValueEvidenceHash(hash.finish()),
            retained_bytes,
        })
    }

    /// Returns the producer-owned source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the producer-owned record identity.
    pub const fn source_identifier(&self) -> &SourceIdentifier {
        &self.source_identifier
    }

    /// Returns the exact producer payload identity.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the immutable producer origin.
    pub const fn origin(&self) -> &EvidenceOrigin {
        &self.origin
    }

    /// Returns the source-authored observation time when available.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        self.source_timestamp
    }

    /// Returns the effective timestamp when available at timestamp precision.
    pub const fn effective_at(&self) -> Option<Timestamp> {
        self.effective_at
    }

    /// Returns the publication timestamp when supplied.
    pub const fn published_at(&self) -> Option<Timestamp> {
        self.published_at
    }

    /// Returns the conservative availability timestamp when established.
    pub const fn available_at(&self) -> Option<Timestamp> {
        self.available_at
    }

    /// Returns the trusted local receive timestamp when the producer retained one.
    pub const fn received_at(&self) -> Option<Timestamp> {
        self.received_at
    }

    /// Returns the producer ingestion or immutable-publication timestamp.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns the producer-specific admission result.
    pub const fn verification(&self) -> EvidenceVerification {
        self.verification
    }

    /// Returns the complete deterministic evidence identity.
    pub const fn hash(&self) -> FairValueEvidenceHash {
        self.hash
    }

    pub(crate) const fn relevance_timestamp(&self) -> Option<Timestamp> {
        match self.source_timestamp {
            Some(value) => Some(value),
            None => self.effective_at,
        }
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn hash_optional_time(hash: &mut CanonicalHasher, value: Option<Timestamp>) {
    match value {
        Some(value) => {
            hash.u8(1);
            hash.i64(value.unix_nanos());
        }
        None => hash.u8(0),
    }
}

fn hash_digest(hash: &mut CanonicalHasher, digest: EvidenceDigest) {
    hash.u8(match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    });
    hash.fixed(digest.bytes());
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
