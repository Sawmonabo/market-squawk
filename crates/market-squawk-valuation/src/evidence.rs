//! Producer-derived source, market, dataset, analytical, and portfolio evidence bindings.

mod forecast;
pub use forecast::{
    ForecastValuationEvidence, ForecastValuationOriginIdentity, ForecastValuationReference,
    ForecastValuationResolver, ForecastValuationSource, ForecastValuationValueSelection,
};

use std::{io, mem::size_of};

use market_squawk_analytics::FeatureKey;
use market_squawk_data::{
    CompanySecurityIdentityDisposition, CompanySecurityIdentitySelectionReceipt,
    DatasetManifestRef, MarketEventCommitRef, ProviderMarketEventPointInTimeSelection,
    SecResearchDisposition, SecResearchIdentityOutcome, SecResearchIdentitySelection,
};
use market_squawk_domain::{
    AccountId, CompanyIdentityObservation, CompanyIdentitySurface, Currency, DataQuality,
    DigestAlgorithm, EvidenceDigest, MarketEvent, Money, PayloadReference, ResearchObservation,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};

use crate::{
    AutomaticValuationCalculation, AutomaticValuationMethodReceipt, CanonicalHasher,
    FairValueError, InputInstrumentRelation, InputObservability, InputSignificance, MarketAccess,
    MarketActivity, PriceAdjustment, ValuationAmount, ValuationAmountBasis, ValuationInput,
    checked_add,
};

const MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES: usize = 1_048_576;

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

/// Exact durable publication joined to a genuine qualified market input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketValuationPublication {
    pub(crate) qualified_input_id: crate::InputId,
    pub(crate) qualified_amount: ValuationAmount,
    pub(crate) commit: MarketEventCommitRef,
    pub(crate) selection_digest: EvidenceDigest,
    pub(crate) publication_digest: EvidenceDigest,
    pub(crate) publication_row: u32,
    pub(crate) coordinate_digest: EvidenceDigest,
    pub(crate) canonical_event_digest: EvidenceDigest,
    pub(crate) canonical_event: Box<str>,
    pub(crate) knowledge_at: Timestamp,
    pub(crate) commit_available_at: Timestamp,
    pub(crate) origin_committed_at: Timestamp,
}

impl MarketValuationPublication {
    /// Returns the exact logical catalog horizon containing the selected event.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }
    /// Returns the complete source-qualified PIT selection identity.
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }
    /// Returns the source knowledge ceiling retained by selection.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
}

/// Closed original authority for either tick interpretation or a native monetary price.
/// The canonical full market definition is retained as bounded bytes, avoiding a second owned
/// reference graph while allowing recovery to replay every source assertion.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PublishedMarketPriceAuthority {
    ExecutionTerms {
        terms: market_squawk_domain::InstrumentExecutionTerms,
    },
    MarketData {
        canonical_definition: Box<str>,
        published_at: Timestamp,
    },
}

/// Archived source price and the exact catalog authority that establishes its financial meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedMarketValuationEvidence {
    pub(crate) commit: MarketEventCommitRef,
    pub(crate) selection_digest: EvidenceDigest,
    pub(crate) publication_digest: EvidenceDigest,
    pub(crate) publication_row: u32,
    pub(crate) canonical_event_digest: EvidenceDigest,
    pub(crate) canonical_event: Box<str>,
    pub(crate) canonical_price_authority: Box<str>,
    pub(crate) definition_content: EvidenceDigest,
    pub(crate) definition_audit: EvidenceDigest,
    pub(crate) knowledge_at: Timestamp,
    pub(crate) commit_available_at: Timestamp,
    pub(crate) origin_committed_at: Timestamp,
}

impl PublishedMarketValuationEvidence {
    /// Returns the exact price's logical catalog horizon and event-rights parent.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }
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
        /// Exact catalog publication, required before this input participates in automatic valuation.
        publication: Option<Box<MarketValuationPublication>>,
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
    /// One exact company fact selected through canonical security identity and all PIT clocks.
    Fundamental {
        /// Immutable source generation containing the selected fact.
        manifest: DatasetManifestRef,
        /// Exact source object/publication identity.
        origin_digest: EvidenceDigest,
        /// Complete bounded PIT read request identity.
        request_digest: EvidenceDigest,
        /// Complete selected/excluded/conflict row identity.
        selection_digest: EvidenceDigest,
        /// Complete source and selection result identity.
        result_digest: EvidenceDigest,
        /// Exact company/security relationship selection identity.
        company_security_digest: EvidenceDigest,
        /// Complete canonical relationship receipt, including selected stock and exact parents.
        canonical_company_security: Box<str>,
        /// Canonical issuer parent retained by the source selection.
        canonical_company_observation: Box<str>,
        /// Exact issuer parent digest bound by both source and relationship selections.
        company_observation_digest: EvidenceDigest,
        /// Canonical source row ordinal.
        row: u32,
        /// Exact provider-native extraction record identity.
        canonical_row_digest: EvidenceDigest,
        /// Source knowledge cutoff, distinct from the fact's economic period.
        knowledge_at: Timestamp,
        /// Actual completion of the retained source generation.
        generation_completed_at: Timestamp,
        /// Complete canonical observation JSON, retaining calendar dates, units and filing context.
        canonical_observation: Box<str>,
    },
    /// A completed automatic calculation; its full inputs and assumptions remain recoverable.
    AutomaticValuation {
        /// Genuine calculator-issued immutable receipt, never caller-supplied money.
        receipt: Box<AutomaticValuationMethodReceipt>,
    },
    /// An exact archived trade or quote midpoint interpreted with a genuine historical definition.
    PublishedMarket {
        /// Source publication and definition proof, with no live qualification.
        evidence: Box<PublishedMarketValuationEvidence>,
    },
    /// One explicit outcome from an artifact-authenticated estimator distribution.
    ForecastDistribution {
        /// Genuine selected model outcome and its exact causal source selection.
        evidence: Box<ForecastValuationEvidence>,
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
                publication,
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
                if let Some(value) = publication {
                    hash.u8(1);
                    hash.fixed(value.qualified_input_id.bytes());
                    value.qualified_amount.hash_into(hash);
                    hash_market_event_commit(hash, &value.commit);
                    for digest in [
                        value.selection_digest,
                        value.publication_digest,
                        value.coordinate_digest,
                        value.canonical_event_digest,
                    ] {
                        hash_digest(hash, digest);
                    }
                    hash.u32(value.publication_row);
                    hash.bytes(value.canonical_event.as_bytes());
                    hash.i64(value.knowledge_at.unix_nanos());
                    hash.i64(value.commit_available_at.unix_nanos());
                    hash.i64(value.origin_committed_at.unix_nanos());
                }
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
            Self::Fundamental {
                manifest,
                origin_digest,
                request_digest,
                selection_digest,
                result_digest,
                company_security_digest,
                canonical_company_security,
                canonical_company_observation,
                company_observation_digest,
                row,
                canonical_row_digest,
                knowledge_at,
                generation_completed_at,
                canonical_observation,
            } => {
                hash.u8(5);
                hash_manifest(hash, manifest);
                for digest in [
                    origin_digest,
                    request_digest,
                    selection_digest,
                    result_digest,
                    company_security_digest,
                    company_observation_digest,
                    canonical_row_digest,
                ] {
                    hash_digest(hash, *digest);
                }
                hash.u32(*row);
                hash.i64(knowledge_at.unix_nanos());
                hash.i64(generation_completed_at.unix_nanos());
                hash.bytes(canonical_observation.as_bytes());
                hash.bytes(canonical_company_security.as_bytes());
                hash.bytes(canonical_company_observation.as_bytes());
            }
            Self::AutomaticValuation { receipt } => {
                hash.u8(6);
                hash.fixed(receipt.id().bytes());
            }
            Self::PublishedMarket { evidence } => {
                hash.u8(7);
                hash_market_event_commit(hash, &evidence.commit);
                for digest in [
                    evidence.selection_digest,
                    evidence.publication_digest,
                    evidence.canonical_event_digest,
                    evidence.definition_content,
                    evidence.definition_audit,
                ] {
                    hash_digest(hash, digest);
                }
                hash.u32(evidence.publication_row);
                hash.bytes(evidence.canonical_event.as_bytes());
                hash.bytes(evidence.canonical_price_authority.as_bytes());
                hash.i64(evidence.knowledge_at.unix_nanos());
                hash.i64(evidence.commit_available_at.unix_nanos());
                hash.i64(evidence.origin_committed_at.unix_nanos());
            }
            Self::ForecastDistribution { evidence } => {
                hash.u8(8);
                hash_digest(hash, evidence.source().reference().identity());
                hash.u64(evidence.selection().digest_ordinal());
            }
        }
    }

    pub(crate) fn retained_bytes(&self) -> Result<usize, FairValueError> {
        match self {
            Self::Market {
                venue_id,
                assessment_id,
                publication,
                ..
            } => {
                let base = checked_add(venue_id.retained_bytes(), assessment_id.retained_bytes())?;
                match publication {
                    Some(value) => checked_add(
                        base,
                        checked_add(
                            size_of::<MarketValuationPublication>(),
                            checked_add(
                                market_event_commit_retained_bytes(&value.commit)?,
                                value.canonical_event.len(),
                            )?,
                        )?,
                    ),
                    None => Ok(base),
                }
            }
            Self::Research { manifest, .. } => manifest_retained_bytes(manifest),
            Self::Analytics {
                feature_key,
                manifest,
                ..
            } => checked_add(feature_key.name().len(), manifest_retained_bytes(manifest)?),
            Self::Portfolio { .. } => Ok(0),
            Self::Fundamental {
                manifest,
                canonical_observation,
                canonical_company_security,
                canonical_company_observation,
                ..
            } => checked_add(
                manifest_retained_bytes(manifest)?,
                checked_add(
                    canonical_observation.len(),
                    checked_add(
                        canonical_company_security.len(),
                        canonical_company_observation.len(),
                    )?,
                )?,
            ),
            Self::AutomaticValuation { receipt } => receipt.retained_bytes(),
            Self::ForecastDistribution { evidence } => checked_add(
                size_of::<ForecastValuationEvidence>(),
                evidence.source().retained_bytes(),
            ),
            Self::PublishedMarket { evidence } => checked_add(
                size_of::<PublishedMarketValuationEvidence>(),
                checked_add(
                    market_event_commit_retained_bytes(&evidence.commit)?,
                    checked_add(
                        evidence.canonical_event.len(),
                        evidence.canonical_price_authority.len(),
                    )?,
                )?,
            ),
        }
    }

    pub(crate) const fn venue_id(&self) -> Option<&VenueId> {
        match self {
            Self::Market { venue_id, .. } => Some(venue_id),
            Self::Research { .. }
            | Self::Analytics { .. }
            | Self::Portfolio { .. }
            | Self::Fundamental { .. }
            | Self::AutomaticValuation { .. }
            | Self::ForecastDistribution { .. }
            | Self::PublishedMarket { .. } => None,
        }
    }

    pub(crate) const fn is_market(&self) -> bool {
        matches!(self, Self::Market { .. } | Self::PublishedMarket { .. })
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
            Self::Research { .. }
            | Self::Analytics { .. }
            | Self::Portfolio { .. }
            | Self::Fundamental { .. }
            | Self::AutomaticValuation { .. }
            | Self::ForecastDistribution { .. }
            | Self::PublishedMarket { .. } => None,
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
    pub(crate) qualification_evaluated_at: Option<Timestamp>,
    pub(crate) qualification_valid_until: Option<Timestamp>,
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
    qualification_evaluated_at: Option<Timestamp>,
    qualification_valid_until: Option<Timestamp>,
    ingested_at: Timestamp,
    verification: EvidenceVerification,
    hash: FairValueEvidenceHash,
    retained_bytes: usize,
}

impl FairValueEvidence {
    fn parts_with_origin(&self, origin: EvidenceOrigin) -> FairValueEvidenceParts {
        FairValueEvidenceParts {
            source_id: self.source_id.clone(),
            source_identifier: self.source_identifier.clone(),
            payload_digest: self.payload_digest,
            origin,
            source_timestamp: self.source_timestamp,
            effective_at: self.effective_at,
            published_at: self.published_at,
            available_at: self.available_at,
            received_at: self.received_at,
            qualification_evaluated_at: self.qualification_evaluated_at,
            qualification_valid_until: self.qualification_valid_until,
            ingested_at: self.ingested_at,
            verification: self.verification,
        }
    }
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
                || (parts.source_timestamp.is_none()
                    && parts.effective_at.is_none()
                    && !matches!(parts.origin, EvidenceOrigin::Fundamental { .. })
                    && !matches!(&parts.origin, EvidenceOrigin::ForecastDistribution { evidence } if evidence.source().financial_epoch().is_some())));
        let qualification_incomplete =
            parts.qualification_evaluated_at.is_some() != parts.qualification_valid_until.is_some();
        let qualification_invalid = (!parts.origin.is_market()
            && parts.qualification_evaluated_at.is_some())
            || parts
                .qualification_evaluated_at
                .zip(parts.qualification_valid_until)
                .is_some_and(|(evaluated_at, valid_until)| evaluated_at > valid_until);
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
            || qualification_incomplete
            || qualification_invalid
        {
            return Err(FairValueError::InvalidTime);
        }
        validate_derived_origin(&parts)?;
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
        if let (Some(evaluated_at), Some(valid_until)) = (
            parts.qualification_evaluated_at,
            parts.qualification_valid_until,
        ) {
            hash.u8(1);
            hash.i64(evaluated_at.unix_nanos());
            hash.i64(valid_until.unix_nanos());
        }
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
            qualification_evaluated_at: parts.qualification_evaluated_at,
            qualification_valid_until: parts.qualification_valid_until,
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

    /// Returns exact source-selection coordinates admitted for automatic valuation.
    ///
    /// Uncatalogued live values and generic analytical cells carry no automatic authority.
    pub fn automatic_selection_binding(&self) -> Option<(EvidenceDigest, Timestamp)> {
        match &self.origin {
            EvidenceOrigin::Market {
                publication: Some(value),
                ..
            } => Some((value.selection_digest, value.knowledge_at)),
            EvidenceOrigin::Fundamental {
                selection_digest,
                knowledge_at,
                ..
            } => Some((*selection_digest, *knowledge_at)),
            EvidenceOrigin::PublishedMarket { evidence } => {
                Some((evidence.selection_digest, evidence.knowledge_at))
            }
            EvidenceOrigin::ForecastDistribution { evidence } => Some((
                evidence.source().reference().identity(),
                evidence.source().reference().knowledge_at(),
            )),
            _ => None,
        }
    }

    /// Whether an exact observed market selection, rather than a modeled amount, backs this input.
    pub fn has_automatic_market_binding(&self) -> bool {
        matches!(
            self.origin,
            EvidenceOrigin::Market {
                publication: Some(_),
                ..
            } | EvidenceOrigin::PublishedMarket { .. }
        )
    }

    pub(crate) fn derived_exclusive_expiry(&self) -> Option<Timestamp> {
        match &self.origin {
            EvidenceOrigin::AutomaticValuation { receipt } => Some(receipt.expires_at()),
            EvidenceOrigin::ForecastDistribution { evidence } => {
                Some(evidence.source().distribution().expires_at())
            }
            _ => None,
        }
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

    /// Returns when the selected market observation's complete qualification was evaluated.
    pub const fn qualification_evaluated_at(&self) -> Option<Timestamp> {
        self.qualification_evaluated_at
    }

    /// Returns the selected market observation's inclusive qualification expiry.
    pub const fn qualification_valid_until(&self) -> Option<Timestamp> {
        self.qualification_valid_until
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

    pub(crate) fn producer_verification_is_current_at(&self, at: Timestamp) -> bool {
        if self.verification != EvidenceVerification::Verified {
            return false;
        }
        if !matches!(self.origin, EvidenceOrigin::Market { .. }) {
            return true;
        }
        self.qualification_evaluated_at
            .zip(self.qualification_valid_until)
            .is_some_and(|(evaluated_at, valid_until)| evaluated_at <= at && at <= valid_until)
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn validate_input_binding(
        &self,
        spec: &crate::measurement::ValuationInputSpec,
    ) -> Result<(), FairValueError> {
        match &self.origin {
            EvidenceOrigin::ForecastDistribution { evidence } => {
                let from_source =
                    evidence.selection() == ForecastValuationValueSelection::FinancialOrigin;
                if spec.amount != evidence.source().selected_amount(evidence.selection())?
                    || spec.subject_instrument_id != evidence.source().reference().instrument_id()
                    || spec.reference_instrument_id != spec.subject_instrument_id
                    || spec.relationship != InputInstrumentRelation::Identical
                    || spec.observability
                        != if from_source {
                            InputObservability::Observable
                        } else {
                            InputObservability::Unobservable
                        }
                    || spec.adjustment
                        != if from_source {
                            PriceAdjustment::None
                        } else {
                            PriceAdjustment::Unobservable
                        }
                    || spec.market_access != MarketAccess::NotAssessed
                    || spec.market_activity != MarketActivity::NotAssessed
                    || spec.market_access_assessment.is_some()
                    || spec.use_assessment.is_some()
                    || spec.data_quality
                        != if from_source {
                            DataQuality::Aggregated
                        } else {
                            DataQuality::Modeled
                        }
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
            }
            EvidenceOrigin::PublishedMarket { evidence } => {
                let event = decode_market_event(&evidence.canonical_event)?;
                let (amount, quality) = published_market_amount(&event, evidence)?;
                if spec.amount != amount
                    || Some(spec.subject_instrument_id)
                        != market_event_provenance(&event)?.instrument_id()
                    || spec.reference_instrument_id != spec.subject_instrument_id
                    || spec.relationship != InputInstrumentRelation::Identical
                    || spec.observability != InputObservability::Observable
                    || spec.adjustment != PriceAdjustment::None
                    || spec.market_access != MarketAccess::NotAssessed
                    || spec.market_activity != MarketActivity::NotAssessed
                    || spec.market_access_assessment.is_some()
                    || spec.use_assessment.is_some()
                    || spec.data_quality != quality
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
            }
            EvidenceOrigin::Market {
                publication: Some(publication),
                ..
            } => {
                let event = decode_market_event(&publication.canonical_event)?;
                let provenance = market_event_provenance(&event)?;
                if provenance.instrument_id() != Some(spec.reference_instrument_id)
                    || spec.amount != publication.qualified_amount
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
                let mut origin = self.origin.clone();
                if let EvidenceOrigin::Market { publication, .. } = &mut origin {
                    *publication = None;
                }
                let evidence = Self::try_from_parts(self.parts_with_origin(origin))?;
                let mut original = spec.clone();
                original.evidence = evidence;
                if ValuationInput::try_from_spec(original)?.id() != publication.qualified_input_id {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
            }
            EvidenceOrigin::Fundamental {
                canonical_observation,
                canonical_company_security,
                ..
            } => {
                let observation = decode_fundamental(canonical_observation)?;
                let receipt = decode_company_security(canonical_company_security)?;
                let [selected] = receipt.ordered_candidates() else {
                    return Err(FairValueError::InvalidInstrumentRelationship);
                };
                let (currency, basis) = fundamental_amount_unit(&observation)?;
                if selected.instrument_id() != spec.subject_instrument_id
                    || spec.subject_instrument_id != spec.reference_instrument_id
                    || spec.relationship != InputInstrumentRelation::Identical
                    || spec.amount.money() != Money::new(observation.value(), currency)
                    || spec.amount.basis() != basis
                    || spec.observability != InputObservability::Observable
                    || spec.adjustment != PriceAdjustment::None
                    || spec.data_quality != observation.context().provenance().quality()
                    || spec.market_access != MarketAccess::NotAssessed
                    || spec.market_activity != MarketActivity::NotAssessed
                    || spec.use_assessment.is_some()
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
            }
            EvidenceOrigin::AutomaticValuation { receipt } => {
                if receipt.instrument_id() != spec.subject_instrument_id
                    || spec.subject_instrument_id != spec.reference_instrument_id
                    || spec.relationship != InputInstrumentRelation::Identical
                    || spec.amount != receipt.range().central()
                    || spec.observability != InputObservability::Unobservable
                    || spec.adjustment != PriceAdjustment::Unobservable
                    || spec.data_quality != DataQuality::Modeled
                    || spec.use_assessment.is_some()
                    || spec.market_access != MarketAccess::NotAssessed
                    || spec.market_activity != MarketActivity::NotAssessed
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
            }
            EvidenceOrigin::Market { .. }
            | EvidenceOrigin::Research { .. }
            | EvidenceOrigin::Analytics { .. }
            | EvidenceOrigin::Portfolio { .. } => {}
        }
        Ok(())
    }
}

impl ValuationInput {
    /// Derives a research price from exact archived events and catalog-pinned price authority.
    ///
    /// Uses the actual trade or coherent bid/ask midpoint and retains all definition/publication
    /// identities. It neither recreates a live qualification nor changes the source's availability.
    pub fn from_published_market_selection(
        selected: &ProviderMarketEventPointInTimeSelection,
        market_definitions: &market_squawk_data::MarketDataInstrumentPopulationSelection,
        execution_definitions: Option<&market_squawk_data::PinnedInstrumentDefinitions>,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let [source] = selected.sources() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let [candidate] = source.tied_candidates() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let provenance = market_event_provenance(candidate.event())?;
        let source_at = provenance
            .source_timestamp()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let knowledge_at = selected.request().knowledge_cutoff();
        let instrument_id = selected
            .request()
            .instrument_id()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let [record] = market_definitions.records() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        if selected.completeness()
            != market_squawk_data::ProviderMarketEventSelectionCompleteness::Complete
            || market_definitions.disposition()
                != market_squawk_data::MarketDataInstrumentPopulationDisposition::Complete
            || !market_definitions.exclusions().is_empty()
            || market_definitions.query().instrument_ids() != [instrument_id]
            || market_definitions.query().knowledge_at() != knowledge_at
            || market_definitions.query().effective_at() != knowledge_at
            || record.definition().instrument_id() != instrument_id
            || record.published_at() > knowledge_at
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let (authority, definition_content, definition_audit) = match candidate.event() {
            MarketEvent::MarketDataQuote(_) | MarketEvent::MarketDataTrade(_) => (
                PublishedMarketPriceAuthority::MarketData {
                    canonical_definition: encode_source_record(record.definition())?,
                    published_at: record.published_at(),
                },
                record.revision_digest(),
                market_definitions.receipt_digest(),
            ),
            MarketEvent::Trade(_) | MarketEvent::Quote(_) => {
                let definitions =
                    execution_definitions.ok_or(FairValueError::InvalidProducerEvidence)?;
                let terms = definitions
                    .execution_terms_at(instrument_id, source_at)
                    .ok_or(FairValueError::InvalidProducerEvidence)?;
                if definitions.as_of() != knowledge_at
                    || definitions.execution_terms_at(instrument_id, knowledge_at) != Some(terms)
                    || terms.quote_currency() != record.definition().quote_currency()
                {
                    return Err(FairValueError::InvalidProducerEvidence);
                }
                (
                    PublishedMarketPriceAuthority::ExecutionTerms { terms },
                    EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        definitions.content_identity().bytes(),
                    ),
                    EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        definitions.audit_identity().bytes(),
                    ),
                )
            }
            _ => return Err(FairValueError::InvalidProducerEvidence),
        };
        let published = PublishedMarketValuationEvidence {
            commit: selected.commit().clone(),
            selection_digest: selected.selection_digest(),
            publication_digest: candidate.coordinate().publication().digest(),
            publication_row: candidate.coordinate().publication_row_ordinal(),
            canonical_event_digest: candidate.coordinate().canonical_event_digest(),
            canonical_event: encode_source_record(candidate.event())?,
            canonical_price_authority: encode_source_record(&authority)?,
            definition_content,
            definition_audit,
            knowledge_at,
            commit_available_at: selected.commit_available_at(),
            origin_committed_at: candidate.coordinate().origin_committed_at(),
        };
        let (amount, quality) = published_market_amount(candidate.event(), &published)?;
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: provenance.source_id().clone(),
            source_identifier: provenance.source_identifier().clone(),
            payload_digest: provenance.binding().payload_digest(),
            origin: EvidenceOrigin::PublishedMarket {
                evidence: Box::new(published),
            },
            source_timestamp: Some(source_at),
            effective_at: Some(source_at),
            published_at: None,
            available_at: Some(provenance.available_at()),
            received_at: Some(provenance.received_at()),
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: provenance.ingested_at(),
            verification: EvidenceVerification::Verified,
        })?;
        Self::try_from_spec(crate::measurement::ValuationInputSpec {
            subject_instrument_id: instrument_id,
            reference_instrument_id: instrument_id,
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Observable,
            adjustment: PriceAdjustment::None,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: quality,
            evidence,
            use_assessment: None,
        })
    }
    /// Joins a genuine qualified price to one exact, raw-verified catalog publication.
    ///
    /// Every newest tie is preserved by the data authority; ambiguous source or row selections
    /// cannot enter this constructor. Matching the complete live state commitment prevents a
    /// same-source publication from being substituted as this price's rights parent.
    pub fn bind_selected_market_publication(
        self,
        selected: &ProviderMarketEventPointInTimeSelection,
    ) -> Result<Self, FairValueError> {
        let [source] = selected.sources() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let [candidate] = source.tied_candidates() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let coordinate = candidate.coordinate();
        let canonical_event = encode_source_record(candidate.event())?;
        if <[u8; 32]>::from(Sha256::digest(canonical_event.as_bytes()))
            != coordinate.canonical_event_digest().bytes()
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let evidence = self.evidence();
        let mut origin = evidence.origin().clone();
        let EvidenceOrigin::Market { publication, .. } = &mut origin else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        if publication.is_some()
            || selected.request().instrument_id() != Some(self.reference_instrument_id())
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        *publication = Some(Box::new(MarketValuationPublication {
            qualified_input_id: self.id(),
            qualified_amount: self.amount(),
            commit: selected.commit().clone(),
            selection_digest: selected.selection_digest(),
            publication_digest: coordinate.publication().digest(),
            publication_row: coordinate.publication_row_ordinal(),
            coordinate_digest: coordinate.coordinate_digest(),
            canonical_event_digest: coordinate.canonical_event_digest(),
            canonical_event,
            knowledge_at: selected.request().knowledge_cutoff(),
            commit_available_at: selected.commit_available_at(),
            origin_committed_at: coordinate.origin_committed_at(),
        }));
        let evidence = FairValueEvidence::try_from_parts(evidence.parts_with_origin(origin))?;
        Self::try_from_spec(self.spec_with_evidence(evidence))
    }

    fn spec_with_evidence(
        &self,
        evidence: FairValueEvidence,
    ) -> crate::measurement::ValuationInputSpec {
        crate::measurement::ValuationInputSpec {
            subject_instrument_id: self.subject_instrument_id(),
            reference_instrument_id: self.reference_instrument_id(),
            relationship: self.relationship(),
            amount: self.amount(),
            significance: self.significance(),
            observability: self.observability(),
            adjustment: self.adjustment(),
            market_activity: self.market_activity(),
            market_access: self.market_access(),
            market_access_assessment: self.market_access_assessment().cloned(),
            data_quality: self.data_quality(),
            evidence,
            use_assessment: self.use_assessment().cloned(),
        }
    }
    /// Derives one observed monetary company fact from an actual security-resolved PIT selection.
    ///
    /// The exact source concept and unit determine common-total, entity-total or per-share basis. Calendar periods,
    /// filing context, revisions and generation-completion time remain in the evidence. No date is
    /// promoted to an invented timestamp, and excluded rows cannot be selected.
    pub fn from_selected_fundamental(
        selection: &SecResearchIdentitySelection,
        row: u32,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let SecResearchIdentityOutcome::Exact(selected) = selection.outcome() else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        if selection.identity().disposition() != CompanySecurityIdentityDisposition::Complete
            || selected.disposition() != SecResearchDisposition::Selected
            || selection.identity().candidates().len() != 1
            || selected.request().knowledge_at() != selection.request().knowledge_at()
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let selected_row = selected
            .selected()
            .iter()
            .find(|value| value.row().row_ordinal() == row)
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let source = selected
            .decoded_rows()
            .get(usize::try_from(row).map_err(|_| FairValueError::Arithmetic)?)
            .map_err(|_| FairValueError::InvalidProducerEvidence)?
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let ResearchObservation::Fundamental(observation) = &source else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let provenance = observation.context().provenance();
        let company = selected.company_identity().observation();
        let company_digest = selected.receipt().company_observation_digest();
        selection
            .identity()
            .receipt()
            .validate_selected_company(
                selection.request().instrument_id(),
                company,
                company_digest,
                selection.request().knowledge_at(),
            )
            .map_err(|_| FairValueError::InvalidInstrumentRelationship)?;
        validate_fundamental_company(observation, company)?;
        let canonical_company_security = String::from_utf8(
            selection
                .identity()
                .receipt()
                .canonical_bytes()
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
        )
        .map_err(|_| FairValueError::InvalidProducerEvidence)?
        .into_boxed_str();
        let canonical_observation = encode_source_record(&source)?;
        let payload_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(canonical_observation.as_bytes()).into(),
        );
        if payload_digest != selected_row.row().observation_digest() {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let (currency, basis) = fundamental_amount_unit(observation)?;
        let amount = ValuationAmount::try_new(
            Money::new(observation.value(), currency),
            u8::try_from(observation.value().scale()).map_err(|_| FairValueError::InvalidAmount)?,
            basis,
        )?;
        let receipt = selected.receipt();
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: provenance.source_id().clone(),
            source_identifier: provenance.source_identifier().clone(),
            payload_digest,
            origin: EvidenceOrigin::Fundamental {
                manifest: selected.origin().manifest().clone(),
                origin_digest: receipt.origin_digest(),
                request_digest: receipt.request_digest(),
                selection_digest: receipt.selection_digest(),
                result_digest: receipt.result_digest(),
                company_security_digest: selection.identity().receipt().receipt_digest(),
                canonical_company_security,
                canonical_company_observation: encode_source_record(company)?,
                company_observation_digest: company_digest,
                row,
                canonical_row_digest: selected_row.row().canonical_row_digest(),
                knowledge_at: selection.request().knowledge_at(),
                generation_completed_at: selected.origin().generation_completed_at(),
                canonical_observation,
            },
            source_timestamp: provenance.source_timestamp(),
            effective_at: observation.context().time().effective().exact_timestamp(),
            published_at: observation
                .context()
                .time()
                .published()
                .and_then(|value| value.exact_timestamp()),
            available_at: provenance.availability().conservative_available_at(),
            received_at: Some(provenance.received_at()),
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: provenance
                .ingested_at()
                .max(selected.origin().generation_completed_at()),
            verification: EvidenceVerification::Verified,
        })?;
        Self::try_from_spec(crate::measurement::ValuationInputSpec {
            subject_instrument_id: selection.request().instrument_id(),
            reference_instrument_id: selection.request().instrument_id(),
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Observable,
            adjustment: PriceAdjustment::None,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: provenance.quality(),
            evidence,
            use_assessment: None,
        })
    }

    /// Consumes a genuine completed calculation into a derived, unobservable valuation input.
    ///
    /// The full method receipt is retained in the existing fair-value evidence chain. This grants
    /// no approval and cannot create a quoted-price or execution-quality input.
    pub fn from_automatic_calculation(
        calculation: AutomaticValuationCalculation,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let receipt = calculation.into_receipt();
        let instrument_id = receipt.instrument_id();
        let amount = receipt.range().central();
        let calculated_at = receipt.calculated_at();
        let effective_at = receipt.measurement_at();
        let payload_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.id().bytes());
        let source_identifier = SourceIdentifier::try_from(receipt.id().to_string())
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: SourceId::try_from("market-squawk.valuation")
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            source_identifier,
            payload_digest,
            origin: EvidenceOrigin::AutomaticValuation {
                receipt: Box::new(receipt),
            },
            source_timestamp: Some(calculated_at),
            effective_at: Some(effective_at),
            published_at: Some(calculated_at),
            available_at: Some(calculated_at),
            received_at: None,
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: calculated_at,
            verification: EvidenceVerification::Verified,
        })?;
        Self::try_from_spec(crate::measurement::ValuationInputSpec {
            subject_instrument_id: instrument_id,
            reference_instrument_id: instrument_id,
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Unobservable,
            adjustment: PriceAdjustment::Unobservable,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: DataQuality::Modeled,
            evidence,
            use_assessment: None,
        })
    }
}

fn validate_derived_origin(parts: &FairValueEvidenceParts) -> Result<(), FairValueError> {
    match &parts.origin {
        EvidenceOrigin::ForecastDistribution { evidence } => {
            let source = evidence.source();
            let distribution = source.distribution();
            source.selected_amount(evidence.selection())?;
            if parts.payload_digest != source.reference().identity()
                || parts.source_id.as_str() != "market-squawk.forecast"
                || parts.source_identifier.as_str()
                    != forecast::forecast_value_identifier(evidence.selection())
                || parts.source_timestamp != distribution.observed_through()
                || parts.effective_at != distribution.target_at()
                || parts.published_at != Some(distribution.published_at())
                || parts.available_at != Some(distribution.published_at())
                || parts.received_at != Some(distribution.published_at())
                || parts.ingested_at != distribution.published_at()
                || parts.qualification_evaluated_at.is_some()
                || parts.qualification_valid_until.is_some()
                || parts.verification != EvidenceVerification::Verified
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        }
        EvidenceOrigin::PublishedMarket { evidence } => {
            if evidence.commit.content_hash().bytes() == [0; 32]
                || [
                    evidence.selection_digest,
                    evidence.publication_digest,
                    evidence.canonical_event_digest,
                    evidence.definition_content,
                    evidence.definition_audit,
                ]
                .iter()
                .any(|digest| {
                    digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
                })
                || evidence.canonical_event_digest.bytes()
                    != <[u8; 32]>::from(Sha256::digest(evidence.canonical_event.as_bytes()))
                || evidence.commit_available_at != evidence.commit.available_at()
                || evidence.origin_committed_at > evidence.commit_available_at
                || evidence.commit_available_at > evidence.knowledge_at
                || evidence.origin_committed_at > evidence.knowledge_at
                || parts.ingested_at > evidence.knowledge_at
                || parts
                    .available_at
                    .is_none_or(|time| time > evidence.knowledge_at)
                || parts
                    .source_timestamp
                    .is_none_or(|time| time > evidence.knowledge_at)
                || parts.qualification_evaluated_at.is_some()
                || parts.qualification_valid_until.is_some()
                || parts.verification != EvidenceVerification::Verified
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let event = decode_market_event(&evidence.canonical_event)?;
            let provenance = market_event_provenance(&event)?;
            published_market_amount(&event, evidence)?;
            if provenance.source_id() != &parts.source_id
                || provenance.source_identifier() != &parts.source_identifier
                || provenance.binding().payload_digest() != parts.payload_digest
                || provenance.source_timestamp() != parts.source_timestamp
                || parts.effective_at != parts.source_timestamp
                || Some(provenance.available_at()) != parts.available_at
                || Some(provenance.received_at()) != parts.received_at
                || provenance.ingested_at() != parts.ingested_at
                || parts.published_at.is_some()
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        }
        EvidenceOrigin::Market {
            venue_id,
            canonical_state_digest,
            publication: Some(value),
            ..
        } => {
            if value.canonical_event.is_empty()
                || value.canonical_event.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES
                || value.commit.content_hash().bytes() == [0; 32]
                || [
                    value.selection_digest,
                    value.publication_digest,
                    value.coordinate_digest,
                    value.canonical_event_digest,
                ]
                .iter()
                .any(|digest| {
                    digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
                })
                || value.canonical_event_digest.bytes()
                    != <[u8; 32]>::from(Sha256::digest(value.canonical_event.as_bytes()))
                || value.commit_available_at != value.commit.available_at()
                || value.origin_committed_at > value.commit_available_at
                || value.commit_available_at > value.knowledge_at
                || value.origin_committed_at > value.knowledge_at
                || parts.ingested_at > value.knowledge_at
                || parts
                    .available_at
                    .is_none_or(|time| time > value.knowledge_at)
                || parts
                    .source_timestamp
                    .is_none_or(|time| time > value.knowledge_at)
                || parts.verification != EvidenceVerification::Verified
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let event = decode_market_event(&value.canonical_event)?;
            let provenance = market_event_provenance(&event)?;
            if provenance.source_id() != &parts.source_id
                || provenance.source_identifier() != &parts.source_identifier
                || provenance.venue_id() != Some(venue_id)
                || provenance.binding().payload_digest() != parts.payload_digest
                || provenance.binding().canonical_state_digest().digest() != *canonical_state_digest
                || provenance.source_timestamp() != parts.source_timestamp
                || Some(provenance.received_at()) != parts.received_at
                || Some(provenance.available_at()) != parts.available_at
                || provenance.ingested_at() != parts.ingested_at
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        }
        EvidenceOrigin::Fundamental {
            manifest,
            origin_digest,
            request_digest,
            selection_digest,
            result_digest,
            company_security_digest,
            canonical_company_security,
            canonical_company_observation,
            company_observation_digest,
            canonical_row_digest,
            knowledge_at,
            generation_completed_at,
            canonical_observation,
            ..
        } => {
            if manifest.content_hash().bytes() == [0; 32]
                || [
                    origin_digest,
                    request_digest,
                    selection_digest,
                    result_digest,
                    company_security_digest,
                    company_observation_digest,
                    canonical_row_digest,
                ]
                .iter()
                .any(|digest| {
                    digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
                })
                || parts.payload_digest.algorithm() != DigestAlgorithm::Sha256
                || parts.payload_digest.bytes()
                    != <[u8; 32]>::from(Sha256::digest(canonical_observation.as_bytes()))
                || *generation_completed_at > *knowledge_at
                || parts.ingested_at > *knowledge_at
                || parts.available_at.is_none_or(|value| value > *knowledge_at)
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let observation = decode_fundamental(canonical_observation)?;
            let company = decode_company(canonical_company_observation)?;
            let identity = decode_company_security(canonical_company_security)?;
            let [entry] = identity.ordered_candidates() else {
                return Err(FairValueError::InvalidProducerEvidence);
            };
            identity
                .validate_selected_company(
                    entry.instrument_id(),
                    &company,
                    *company_observation_digest,
                    *knowledge_at,
                )
                .map_err(|_| FairValueError::InvalidProducerEvidence)?;
            if identity.receipt_digest() != *company_security_digest {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            validate_fundamental_company(&observation, &company)?;
            let provenance = observation.context().provenance();
            if provenance.source_id() != &parts.source_id
                || provenance.source_identifier() != &parts.source_identifier
                || provenance.source_timestamp() != parts.source_timestamp
                || provenance.availability().conservative_available_at() != parts.available_at
                || Some(provenance.received_at()) != parts.received_at
                || provenance.ingested_at().max(*generation_completed_at) != parts.ingested_at
                || observation.context().time().effective().exact_timestamp() != parts.effective_at
                || observation
                    .context()
                    .time()
                    .published()
                    .and_then(|value| value.exact_timestamp())
                    != parts.published_at
                || parts.verification != EvidenceVerification::Verified
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        }
        EvidenceOrigin::AutomaticValuation { receipt } => {
            if parts.source_id.as_str() != "market-squawk.valuation"
                || parts.source_identifier.as_str() != receipt.id().to_string()
                || parts.payload_digest
                    != EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.id().bytes())
                || parts.source_timestamp != Some(receipt.calculated_at())
                || parts.effective_at != Some(receipt.measurement_at())
                || parts.published_at != Some(receipt.calculated_at())
                || parts.available_at != Some(receipt.calculated_at())
                || parts.ingested_at != receipt.calculated_at()
                || parts.received_at.is_some()
                || parts.verification != EvidenceVerification::Verified
                || receipt.inputs().iter().any(|value| {
                    matches!(
                        value.input().evidence().origin(),
                        EvidenceOrigin::AutomaticValuation { .. }
                    )
                })
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        }
        EvidenceOrigin::Market { .. }
        | EvidenceOrigin::Research { .. }
        | EvidenceOrigin::Analytics { .. }
        | EvidenceOrigin::Portfolio { .. } => {}
    }
    Ok(())
}

fn decode_market_event(value: &str) -> Result<MarketEvent, FairValueError> {
    if value.is_empty() || value.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    serde_json::from_str(value).map_err(|_| FairValueError::InvalidProducerEvidence)
}

fn published_market_amount(
    event: &MarketEvent,
    evidence: &PublishedMarketValuationEvidence,
) -> Result<(ValuationAmount, DataQuality), FairValueError> {
    let provenance = market_event_provenance(event)?;
    if evidence.canonical_price_authority.is_empty()
        || evidence.canonical_price_authority.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES
    {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    let authority: PublishedMarketPriceAuthority =
        serde_json::from_str(&evidence.canonical_price_authority)
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
    if encode_source_record(&authority)? != evidence.canonical_price_authority {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    let midpoint = |bid: Decimal, ask: Decimal| -> Result<Decimal, FairValueError> {
        if bid <= Decimal::ZERO || bid > ask {
            return Err(FairValueError::InvalidAmount);
        }
        bid.checked_add(ask)
            .and_then(|sum| sum.checked_div(Decimal::TWO))
            .ok_or(FairValueError::InvalidAmount)
    };
    let (value, currency, source_scale) = match authority {
        PublishedMarketPriceAuthority::ExecutionTerms { terms } => {
            if provenance.instrument_id() != Some(terms.instrument_id()) {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let price = |ticks: market_squawk_domain::PriceTicks| {
                ticks
                    .checked_to_decimal(terms.price_tick())
                    .map_err(|_| FairValueError::InvalidAmount)
            };
            let value = match event {
                MarketEvent::Trade(value) => price(value.price())?,
                MarketEvent::Quote(value) => {
                    let (Some(bid), Some(ask)) = (value.bid(), value.ask()) else {
                        return Err(FairValueError::InvalidAmount);
                    };
                    midpoint(price(bid.price())?, price(ask.price())?)?
                }
                _ => return Err(FairValueError::InvalidProducerEvidence),
            };
            (
                value,
                terms.quote_currency(),
                terms.price_tick().as_decimal().scale(),
            )
        }
        PublishedMarketPriceAuthority::MarketData {
            canonical_definition,
            published_at,
        } => {
            if canonical_definition.is_empty()
                || canonical_definition.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES
                || published_at > evidence.knowledge_at
                || evidence.definition_content.bytes()
                    != <[u8; 32]>::from(Sha256::digest(canonical_definition.as_bytes()))
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let definition: market_squawk_domain::MarketDataInstrumentDefinition =
                serde_json::from_str(&canonical_definition)
                    .map_err(|_| FairValueError::InvalidProducerEvidence)?;
            if encode_source_record(&definition)? != canonical_definition {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            let (reference, value, source_scale) = match event {
                MarketEvent::MarketDataTrade(trade) => (
                    trade.reference(),
                    trade.price().amount(),
                    trade.price().amount().scale(),
                ),
                MarketEvent::MarketDataQuote(quote) => {
                    let (Some(bid), Some(ask)) = (quote.bid(), quote.ask()) else {
                        return Err(FairValueError::InvalidAmount);
                    };
                    let (bid, ask) = (bid.price().amount(), ask.price().amount());
                    (
                        quote.reference(),
                        midpoint(bid, ask)?,
                        bid.scale().max(ask.scale()),
                    )
                }
                _ => return Err(FairValueError::InvalidProducerEvidence),
            };
            if reference.definition_digest() != evidence.definition_content
                || provenance.instrument_id() != Some(reference.instrument_id())
            {
                return Err(FairValueError::InvalidProducerEvidence);
            }
            for at in [
                provenance
                    .source_timestamp()
                    .ok_or(FairValueError::InvalidProducerEvidence)?,
                provenance.received_at(),
                evidence.knowledge_at,
            ] {
                reference
                    .validate_definition_at(&definition, at)
                    .map_err(|_| FairValueError::InvalidProducerEvidence)?;
            }
            (value, reference.currency(), source_scale)
        }
    };
    if value <= Decimal::ZERO {
        return Err(FairValueError::InvalidAmount);
    }
    let quality = match provenance.recorded_quality() {
        DataQuality::DirectVerified => DataQuality::DirectUnverified,
        DataQuality::Modeled
        | DataQuality::Estimated
        | DataQuality::Stale
        | DataQuality::Quarantined => {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        quality => quality,
    };
    Ok((
        ValuationAmount::try_new(
            Money::new(value, currency),
            u8::try_from(value.scale().max(source_scale))
                .map_err(|_| FairValueError::InvalidAmount)?,
            ValuationAmountBasis::PerInstrumentUnit,
        )?,
        quality,
    ))
}

fn market_event_provenance(
    event: &MarketEvent,
) -> Result<&market_squawk_domain::LiveProvenance, FairValueError> {
    match event {
        MarketEvent::Trade(value) => Ok(value.provenance()),
        MarketEvent::Quote(value) => Ok(value.provenance()),
        MarketEvent::MarketDataQuote(value) => Ok(value.provenance()),
        MarketEvent::MarketDataTrade(value) => Ok(value.provenance()),
        _ => Err(FairValueError::InvalidProducerEvidence),
    }
}

fn fundamental_amount_unit(
    observation: &market_squawk_domain::FundamentalObservation,
) -> Result<(Currency, ValuationAmountBasis), FairValueError> {
    if let Some(xbrl) = observation.xbrl_evidence() {
        let (currency, basis) = if let Some(currency) = xbrl.unit().measure_name() {
            let common_total = xbrl.concept().local_name().as_str()
                == "NetIncomeLossAvailableToCommonStockholdersBasic"
                && xbrl.concept().namespace_uri().is_some_and(|namespace| {
                    namespace.as_str().starts_with("http://fasb.org/us-gaap/")
                });
            (
                currency,
                if common_total {
                    ValuationAmountBasis::TotalCommonEquity
                } else {
                    ValuationAmountBasis::ReportingEntityTotal
                },
            )
        } else {
            let (numerator, denominator) = xbrl
                .unit()
                .divide_parts()
                .ok_or(FairValueError::InvalidAmount)?;
            let ([currency], [shares]) = (numerator, denominator) else {
                return Err(FairValueError::InvalidAmount);
            };
            if shares.namespace_uri().map(|namespace| namespace.as_str())
                != Some("http://www.xbrl.org/2003/instance")
                || shares.local_name().as_str() != "shares"
            {
                return Err(FairValueError::InvalidAmount);
            }
            (currency, ValuationAmountBasis::PerInstrumentUnit)
        };
        if currency.namespace_uri().map(|namespace| namespace.as_str())
            != Some("http://www.xbrl.org/2003/iso4217")
        {
            return Err(FairValueError::InvalidAmount);
        }
        let parsed = Currency::try_from(currency.local_name().as_str())
            .map_err(|_| FairValueError::InvalidAmount)?;
        if parsed.as_str() != currency.local_name().as_str() {
            return Err(FairValueError::InvalidAmount);
        }
        return Ok((parsed, basis));
    }
    let unit = observation.unit().as_str();
    let (currency_text, basis) = match unit.strip_suffix("/shares") {
        Some(currency) => (currency, ValuationAmountBasis::PerInstrumentUnit),
        None if observation.concept().as_str()
            == "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic" =>
        {
            (unit, ValuationAmountBasis::TotalCommonEquity)
        }
        None => (unit, ValuationAmountBasis::ReportingEntityTotal),
    };
    let currency = Currency::try_from(currency_text).map_err(|_| FairValueError::InvalidAmount)?;
    if currency.as_str() != currency_text {
        return Err(FairValueError::InvalidAmount);
    }
    Ok((currency, basis))
}

/// Joins an original issuer-owned fact to its exact source parent without inventing a stock ID.
pub(crate) fn validate_fundamental_company(
    observation: &market_squawk_domain::FundamentalObservation,
    company: &CompanyIdentityObservation,
) -> Result<(), FairValueError> {
    let provenance = observation.context().provenance();
    let payload = match (company.surface(), observation.xbrl_evidence()) {
        (CompanyIdentitySurface::SecCompanyFacts, None) => {
            company.identity_payload_evidence().content_digest()
        }
        (CompanyIdentitySurface::SecFilingXbrl, Some(xbrl))
            if xbrl.entity().scheme().as_str() == "http://www.sec.gov/CIK"
                && xbrl.entity().value().as_str() == company.provider_company_id().as_str()
                && xbrl.accession() == observation.fact_context().accession() =>
        {
            xbrl.source_payload().content_digest()
        }
        _ => return Err(FairValueError::InvalidProducerEvidence),
    };
    if provenance.instrument_id().is_some()
        || observation.subject().issuer_id() != Some(company.provider_company_id())
        || provenance.source_id() != company.source_id()
        || !matches!(provenance.payload_reference(), PayloadReference::ContentHash(hash)
            if hash.algorithm() == payload.algorithm() && hash.digest() == payload.bytes())
    {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    Ok(())
}

fn decode_company_security(
    value: &str,
) -> Result<CompanySecurityIdentitySelectionReceipt, FairValueError> {
    CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(value.as_bytes())
        .map_err(|_| FairValueError::InvalidProducerEvidence)
}

fn decode_company(value: &str) -> Result<CompanyIdentityObservation, FairValueError> {
    if value.is_empty() || value.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    let company =
        serde_json::from_str(value).map_err(|_| FairValueError::InvalidProducerEvidence)?;
    if encode_source_record(&company)?.as_ref() != value {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    Ok(company)
}

fn decode_fundamental(
    value: &str,
) -> Result<market_squawk_domain::FundamentalObservation, FairValueError> {
    if value.is_empty() || value.len() > MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    match serde_json::from_str::<ResearchObservation>(value)
        .map_err(|_| FairValueError::InvalidProducerEvidence)?
    {
        ResearchObservation::Fundamental(observation) => Ok(observation),
        _ => Err(FairValueError::InvalidProducerEvidence),
    }
}

struct EvidenceEncodingSize(usize);

impl io::Write for EvidenceEncodingSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|value| *value <= MAXIMUM_FUNDAMENTAL_EVIDENCE_BYTES)
            .ok_or_else(|| io::Error::other("fundamental evidence byte limit"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_source_record(value: &impl serde::Serialize) -> Result<Box<str>, FairValueError> {
    let mut size = EvidenceEncodingSize(0);
    serde_json::to_writer(&mut size, value).map_err(|_| FairValueError::InvalidProducerEvidence)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size.0)
        .map_err(|_| FairValueError::Arithmetic)?;
    serde_json::to_writer(&mut bytes, value)
        .map_err(|_| FairValueError::InvalidProducerEvidence)?;
    if bytes.len() != size.0 {
        return Err(FairValueError::InvalidProducerEvidence);
    }
    String::from_utf8(bytes)
        .map(String::into_boxed_str)
        .map_err(|_| FairValueError::InvalidProducerEvidence)
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

pub(crate) fn hash_manifest(hash: &mut CanonicalHasher, manifest: &DatasetManifestRef) {
    hash.bytes(manifest.dataset_id().as_str().as_bytes());
    hash.u64(manifest.manifest_version());
    hash.bytes(manifest.schema().name().as_bytes());
    hash.u32(u32::from(manifest.schema_version().get()));
    hash.fixed(manifest.schema().fingerprint());
    hash.fixed(manifest.content_hash().bytes());
}

pub(crate) fn manifest_retained_bytes(
    manifest: &DatasetManifestRef,
) -> Result<usize, FairValueError> {
    checked_add(
        manifest.dataset_id().as_str().len(),
        manifest.schema().name().len(),
    )
}

/// Hashes logical event identity without incorporating hot/archive placement.
pub(crate) fn hash_market_event_commit(hash: &mut CanonicalHasher, commit: &MarketEventCommitRef) {
    hash.bytes(b"market-squawk/market-event-commit/v1");
    hash.bytes(commit.dataset_id().as_str().as_bytes());
    hash.u64(commit.sequence());
    hash.bytes(commit.schema().name().as_bytes());
    hash.u32(u32::from(commit.schema().version().get()));
    hash.fixed(commit.schema().fingerprint());
    hash.fixed(commit.content_hash().bytes());
    hash.i64(commit.available_at().unix_nanos());
    hash_digest(hash, commit.publication_digest());
    hash.u64(commit.row_count());
}

pub(crate) fn market_event_commit_retained_bytes(
    commit: &MarketEventCommitRef,
) -> Result<usize, FairValueError> {
    checked_add(
        commit.dataset_id().as_str().len(),
        commit.schema().name().len(),
    )
}
