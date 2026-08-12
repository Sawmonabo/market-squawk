//! Evidence-derived identity approval for one caller-selected market reference.
//!
//! This kernel resolves an exact provider instrument and venue. It does not select a benchmark,
//! construct an investment universe, recommend an instrument, or grant execution authority.

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_openfigi::{
    OpenFigiAccess, OpenFigiIdentityCandidate, OpenFigiMappingOutcome, OpenFigiMappingReceipt,
};
use market_squawk_data::{
    MarketDataInstrumentCatalogError, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord, market_data_instrument_id,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, Currency, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier, Figi,
    IdentifierEntitlement, IdentifierRightsPolicyReference, InstrumentId, MetadataRevision,
    ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId, Timestamp, VenueId,
};
use market_squawk_sources::{AuthorizationMode, SourceMetadata};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::nasdaq_reference::{
    NasdaqCurrentListing, NasdaqListingKey, NasdaqReferenceUniverseError,
    NasdaqReferenceUniverseService,
};
use super::openfigi_identity::{
    OpenFigiCatalogDisposition, OpenFigiIdentityPublicationError,
    OpenFigiIdentityPublicationStatus, OpenFigiIdentityPublisher,
};

const APPROVAL_DIGEST_DOMAIN: &[u8] = b"market-squawk/market-reference-identity-approval/v1\0";
const RATE_LIMIT_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/openfigi-rate-limit-evidence/v1\0";

/// One exact caller-selected provider instrument and venue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketReferenceIdentityRequest {
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
}

impl MarketReferenceIdentityRequest {
    pub(crate) const fn new(
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
    ) -> Self {
        Self {
            provider_instrument_id,
            venue_id,
        }
    }

    pub(crate) const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }
}

/// Expected, fail-closed reason that an exact reference identity cannot currently be approved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketReferenceIdentityUnavailable {
    ListingNotCurrent,
    MappingNotFound,
    MappingAmbiguous,
    MappingConflict,
    MappingProviderError,
    IdentityConflict,
    QuoteCurrencyUnavailable,
    CanonicalDefinitionUnavailable,
    EvidenceExpired,
}

/// Exact result of one bounded identity-resolution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MarketReferenceIdentityResolution {
    Available(MarketReferenceIdentityApprovalV1),
    Unavailable(MarketReferenceIdentityUnavailable),
}

/// Non-forgeable reference-only approval backed by exact source and catalog evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketReferenceIdentityApprovalV1 {
    request: MarketReferenceIdentityRequest,
    instrument_id: InstrumentId,
    permanent_figi: Figi,
    composite_figi: Option<Figi>,
    share_class_figi: Option<Figi>,
    asset_class: AssetClass,
    quote_currency: Currency,
    listing_source_id: SourceId,
    listing_metadata_revision: MetadataRevision,
    listing_source_contract: RevisionBoundPayloadEvidence,
    listing_authorization_basis: AuthorizationBasis,
    listing_authorization_evidence: ExactPayloadEvidence,
    listing_authorization_effective: EffectiveInterval,
    listing_coverage_evidence: ExactPayloadEvidence,
    listing_coverage_effective: EffectiveInterval,
    listing_payload_evidence: ExactPayloadEvidence,
    listing_source_timestamp: Timestamp,
    listing_observed_at: Timestamp,
    listing_max_source_age_nanos: u64,
    listing_max_transport_age_nanos: u64,
    listing_max_market_age_nanos: u64,
    mapping_source_id: SourceId,
    mapping_metadata_revision: MetadataRevision,
    mapping_source_contract: RevisionBoundPayloadEvidence,
    mapping_authorization_basis: AuthorizationBasis,
    mapping_terms_evidence: ExactPayloadEvidence,
    mapping_terms_effective: EffectiveInterval,
    mapping_coverage_evidence: ExactPayloadEvidence,
    mapping_coverage_effective: EffectiveInterval,
    mapping_access: OpenFigiAccess,
    mapping_requested_at: Timestamp,
    mapping_received_at: Timestamp,
    mapping_request_evidence: ExactPayloadEvidence,
    mapping_response_evidence: ExactPayloadEvidence,
    mapping_rate_limit_evidence: ExactPayloadEvidence,
    mapping_max_source_age_nanos: u64,
    mapping_max_transport_age_nanos: u64,
    mapping_max_market_age_nanos: u64,
    catalog_batch_digest: EvidenceDigest,
    catalog_inserted: u32,
    catalog_replayed: u32,
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_published_at: Timestamp,
    definition_reference_evidence: RevisionBoundPayloadEvidence,
    definition_effective: EffectiveInterval,
    quote_currency_evidence: ExactPayloadEvidence,
    figi_source_id: SourceId,
    figi_source_evidence: ExactPayloadEvidence,
    figi_observed_at: Timestamp,
    figi_validity: EffectiveInterval,
    figi_rights_policy: IdentifierRightsPolicyReference,
    evaluated_at: Timestamp,
    expires_at: Timestamp,
    digest: EvidenceDigest,
}

impl MarketReferenceIdentityApprovalV1 {
    pub(crate) const fn request(&self) -> &MarketReferenceIdentityRequest {
        &self.request
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn permanent_figi(&self) -> &Figi {
        &self.permanent_figi
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }

    pub(crate) const fn listing_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.listing_payload_evidence
    }

    pub(crate) const fn listing_source_timestamp(&self) -> Timestamp {
        self.listing_source_timestamp
    }

    pub(crate) const fn listing_observed_at(&self) -> Timestamp {
        self.listing_observed_at
    }

    pub(crate) const fn mapping_terms_evidence(&self) -> &ExactPayloadEvidence {
        &self.mapping_terms_evidence
    }

    pub(crate) const fn mapping_response_evidence(&self) -> &ExactPayloadEvidence {
        &self.mapping_response_evidence
    }

    pub(crate) const fn mapping_received_at(&self) -> Timestamp {
        self.mapping_received_at
    }

    pub(crate) const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    pub(crate) const fn definition_reference_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.definition_reference_evidence
    }

    pub(crate) const fn quote_currency_evidence(&self) -> &ExactPayloadEvidence {
        &self.quote_currency_evidence
    }

    pub(crate) const fn figi_rights_policy(&self) -> &IdentifierRightsPolicyReference {
        &self.figi_rights_policy
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the exclusive expiry bounded by registered source, terms, and definition evidence.
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Generic source-kernel authority for one exact reference identity.
#[derive(Clone)]
pub(crate) struct MarketReferenceIdentityAuthority {
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    openfigi: Arc<OpenFigiIdentityPublisher>,
    catalog: MarketDataInstrumentReadCapability,
}

impl std::fmt::Debug for MarketReferenceIdentityAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MarketReferenceIdentityAuthority")
            .field("nasdaq", &self.nasdaq)
            .field("openfigi", &self.openfigi)
            .field("catalog", &self.catalog)
            .finish()
    }
}

impl MarketReferenceIdentityAuthority {
    pub(crate) const fn new(
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        openfigi: Arc<OpenFigiIdentityPublisher>,
        catalog: MarketDataInstrumentReadCapability,
    ) -> Self {
        Self {
            nasdaq,
            openfigi,
            catalog,
        }
    }

    /// Resolves one exact listing, publishes an exact FIGI, and re-reads its canonical definition.
    pub(crate) async fn resolve(
        &self,
        request: MarketReferenceIdentityRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketReferenceIdentityResolution, MarketReferenceIdentityError> {
        check_operation(deadline, cancellation)?;
        let key = NasdaqListingKey::new(
            request.provider_instrument_id.clone(),
            request.venue_id.clone(),
        );
        let mut listings = self
            .nasdaq
            .selected_current_listings(std::slice::from_ref(&key), deadline, cancellation)
            .await
            .map_err(map_nasdaq_error)?;
        if listings.is_empty() {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::ListingNotCurrent,
            ));
        }
        if listings.len() != 1 {
            return Err(MarketReferenceIdentityError::InvalidEvidence);
        }
        let listing = listings
            .pop()
            .ok_or(MarketReferenceIdentityError::InvalidEvidence)?;

        let publication = self
            .openfigi
            .resolve_selected_and_publish(vec![key.clone()], deadline, cancellation)
            .await
            .map_err(map_openfigi_error)?;
        let [result] = publication.results() else {
            return Err(MarketReferenceIdentityError::InvalidEvidence);
        };
        if result.listing() != &key {
            return Err(MarketReferenceIdentityError::InvalidEvidence);
        }
        let (candidate, instrument_id, catalog_disposition) = match result.status() {
            OpenFigiIdentityPublicationStatus::ListingNotCurrent => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::ListingNotCurrent,
                ));
            }
            OpenFigiIdentityPublicationStatus::NoMatch => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::MappingNotFound,
                ));
            }
            OpenFigiIdentityPublicationStatus::Ambiguous { .. } => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::MappingAmbiguous,
                ));
            }
            OpenFigiIdentityPublicationStatus::ProviderConflict { .. } => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::MappingConflict,
                ));
            }
            OpenFigiIdentityPublicationStatus::ProviderError { .. } => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::MappingProviderError,
                ));
            }
            OpenFigiIdentityPublicationStatus::IdentityConflict { .. } => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::IdentityConflict,
                ));
            }
            OpenFigiIdentityPublicationStatus::QuoteCurrencyUnavailable => {
                return Ok(MarketReferenceIdentityResolution::Unavailable(
                    MarketReferenceIdentityUnavailable::QuoteCurrencyUnavailable,
                ));
            }
            OpenFigiIdentityPublicationStatus::Exact {
                candidate,
                instrument_id,
                catalog_disposition,
            } => (candidate.clone(), *instrument_id, *catalog_disposition),
        };
        let derived_instrument_id = market_data_instrument_id(candidate.exchange_figi())
            .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)?;
        if instrument_id != derived_instrument_id {
            return Err(MarketReferenceIdentityError::InvalidEvidence);
        }

        let [mapping] = publication.provider_receipts() else {
            return Err(MarketReferenceIdentityError::InvalidEvidence);
        };
        validate_mapping(mapping, &listing, &candidate)?;
        let catalog_receipt = publication
            .catalog_receipt()
            .ok_or(MarketReferenceIdentityError::InvalidEvidence)?;
        validate_catalog_receipt(catalog_receipt, catalog_disposition)?;
        let record = self
            .catalog
            .latest(instrument_id, deadline, cancellation)
            .map_err(map_catalog_error)?;
        let Some(record) = record else {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::CanonicalDefinitionUnavailable,
            ));
        };
        check_operation(deadline, cancellation)?;
        let evaluated_at = system_timestamp()?;
        let nasdaq_metadata = self.nasdaq.reference_identity_metadata();
        let openfigi_metadata = self.openfigi.reference_identity_metadata();
        match build_approval(
            request,
            listing,
            candidate,
            mapping,
            catalog_receipt,
            &record,
            nasdaq_metadata,
            openfigi_metadata,
            evaluated_at,
        )? {
            Some(approval) => Ok(MarketReferenceIdentityResolution::Available(approval)),
            None => Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::EvidenceExpired,
            )),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "each independent authority receipt is validated before one approval is minted"
)]
fn build_approval(
    request: MarketReferenceIdentityRequest,
    listing: NasdaqCurrentListing,
    candidate: OpenFigiIdentityCandidate,
    mapping: &OpenFigiMappingReceipt,
    catalog_receipt: &market_squawk_data::MarketDataInstrumentSynchronizationReceipt,
    record: &MarketDataInstrumentRecord,
    nasdaq_metadata: &SourceMetadata,
    openfigi_metadata: &SourceMetadata,
    evaluated_at: Timestamp,
) -> Result<Option<MarketReferenceIdentityApprovalV1>, MarketReferenceIdentityError> {
    let definition = record.definition();
    let expected_instrument_id = market_data_instrument_id(candidate.exchange_figi())
        .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)?;
    let [figi_record] = definition.identifiers() else {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    };
    let ExternalIdentifier::Figi(identifier_figi) = figi_record.identifier() else {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    };
    if listing.key().symbol() != request.provider_instrument_id()
        || listing.key().mic() != request.venue_id()
        || !nasdaq_metadata
            .coverage()
            .asset_classes()
            .contains(&listing.asset_class())
        || !openfigi_metadata
            .coverage()
            .asset_classes()
            .contains(&listing.asset_class())
        || definition.instrument_id() != expected_instrument_id
        || definition.permanent_figi() != candidate.exchange_figi()
        || identifier_figi != candidate.exchange_figi()
        || definition.asset_class() != listing.asset_class()
        || !evidence_is_nonzero(definition.quote_currency_evidence())
        || figi_record.assignment_verification() != AssignmentVerification::VerifiedAssigned
        || figi_record.source_id() != mapping.source_id()
        || figi_record.source_evidence() != mapping.response().evidence()
        || figi_record.source_timestamp().is_some()
        || figi_record.observed_at() > mapping.received_at()
        || figi_record.validity() != definition.effective_interval()
        || figi_record.rights_policy().entitlement() != IdentifierEntitlement::PublicDomain
        || !evidence_is_nonzero(definition.reference_payload_evidence())
        || !evidence_is_nonzero(listing.source_payload_evidence())
        || !evidence_is_nonzero(nasdaq_metadata.revision_evidence().payload_evidence())
        || !evidence_is_nonzero(nasdaq_metadata.authorization().evidence())
        || !evidence_is_nonzero(nasdaq_metadata.coverage().evidence())
        || !evidence_is_nonzero(openfigi_metadata.revision_evidence().payload_evidence())
        || !evidence_is_nonzero(openfigi_metadata.authorization().evidence())
        || !evidence_is_nonzero(openfigi_metadata.coverage().evidence())
        || !evidence_is_nonzero(mapping.request().evidence())
        || !evidence_is_nonzero(mapping.response().evidence())
        || record.revision_digest().bytes() == [0; 32]
        || record.revision_sequence() == 0
        || listing.source_id() != nasdaq_metadata.source_id()
        || listing.metadata_revision() != nasdaq_metadata.revision()
        || mapping.source_id() != openfigi_metadata.source_id()
        || mapping.metadata_revision() != openfigi_metadata.revision()
        || mapping.coverage_evidence() != openfigi_metadata.coverage().evidence()
        || mapping.access() != OpenFigiAccess::Public
        || nasdaq_metadata.authorization().mode() != AuthorizationMode::PublicInterface
        || openfigi_metadata.authorization().mode() != AuthorizationMode::PublicInterface
    {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    let terms_locator_matches = openfigi_metadata
        .authorization()
        .evidence()
        .version_pinned_locator()
        .is_some_and(|locator| {
            locator.reference().as_str() == figi_record.rights_policy().terms_reference().as_str()
        });
    if !terms_locator_matches {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    if listing.source_timestamp() > listing.observed_at()
        || listing.observed_at() > evaluated_at
        || mapping.requested_at() > mapping.received_at()
        || mapping.received_at() > evaluated_at
        || record.published_at() > evaluated_at
    {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    if !nasdaq_metadata.is_effective_at(evaluated_at)
        || !openfigi_metadata.is_effective_at(evaluated_at)
        || !interval_contains(definition.effective_interval(), evaluated_at)
        || !interval_contains(figi_record.validity(), evaluated_at)
    {
        return Ok(None);
    }

    let mut expires_at = fresh_until(
        listing.source_timestamp(),
        nasdaq_metadata.freshness_policy().max_source_age_nanos(),
    )?;
    minimize_expiry(
        &mut expires_at,
        fresh_until(
            listing.observed_at(),
            nasdaq_metadata.freshness_policy().max_transport_age_nanos(),
        )?,
    );
    minimize_expiry(
        &mut expires_at,
        fresh_until(
            listing.observed_at(),
            nasdaq_metadata.freshness_policy().max_market_age_nanos(),
        )?,
    );
    minimize_expiry(
        &mut expires_at,
        fresh_until(
            mapping.received_at(),
            openfigi_metadata
                .freshness_policy()
                .max_transport_age_nanos(),
        )?,
    );
    minimize_expiry(
        &mut expires_at,
        fresh_until(
            mapping.received_at(),
            openfigi_metadata.freshness_policy().max_source_age_nanos(),
        )?,
    );
    minimize_expiry(
        &mut expires_at,
        fresh_until(
            mapping.received_at(),
            openfigi_metadata.freshness_policy().max_market_age_nanos(),
        )?,
    );
    minimize_optional_expiry(
        &mut expires_at,
        nasdaq_metadata
            .authorization()
            .effective_interval()
            .ends_at(),
    );
    minimize_optional_expiry(
        &mut expires_at,
        nasdaq_metadata.coverage().effective_interval().ends_at(),
    );
    minimize_optional_expiry(
        &mut expires_at,
        openfigi_metadata
            .authorization()
            .effective_interval()
            .ends_at(),
    );
    minimize_optional_expiry(
        &mut expires_at,
        openfigi_metadata.coverage().effective_interval().ends_at(),
    );
    minimize_optional_expiry(&mut expires_at, definition.effective_interval().ends_at());
    minimize_optional_expiry(&mut expires_at, figi_record.validity().ends_at());
    if evaluated_at >= expires_at {
        return Ok(None);
    }

    let catalog_inserted = u32::try_from(catalog_receipt.inserted())
        .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)?;
    let catalog_replayed = u32::try_from(catalog_receipt.replayed())
        .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)?;
    let mut approval = MarketReferenceIdentityApprovalV1 {
        request,
        instrument_id: expected_instrument_id,
        permanent_figi: candidate.exchange_figi().clone(),
        composite_figi: candidate.composite_figi().cloned(),
        share_class_figi: candidate.share_class_figi().cloned(),
        asset_class: listing.asset_class(),
        quote_currency: definition.quote_currency(),
        listing_source_id: listing.source_id().clone(),
        listing_metadata_revision: listing.metadata_revision().clone(),
        listing_source_contract: nasdaq_metadata.revision_evidence().clone(),
        listing_authorization_basis: nasdaq_metadata.authorization().basis().clone(),
        listing_authorization_evidence: nasdaq_metadata.authorization().evidence().clone(),
        listing_authorization_effective: nasdaq_metadata.authorization().effective_interval(),
        listing_coverage_evidence: nasdaq_metadata.coverage().evidence().clone(),
        listing_coverage_effective: nasdaq_metadata.coverage().effective_interval(),
        listing_payload_evidence: listing.source_payload_evidence().clone(),
        listing_source_timestamp: listing.source_timestamp(),
        listing_observed_at: listing.observed_at(),
        listing_max_source_age_nanos: nasdaq_metadata.freshness_policy().max_source_age_nanos(),
        listing_max_transport_age_nanos: nasdaq_metadata
            .freshness_policy()
            .max_transport_age_nanos(),
        listing_max_market_age_nanos: nasdaq_metadata.freshness_policy().max_market_age_nanos(),
        mapping_source_id: mapping.source_id().clone(),
        mapping_metadata_revision: mapping.metadata_revision().clone(),
        mapping_source_contract: openfigi_metadata.revision_evidence().clone(),
        mapping_authorization_basis: openfigi_metadata.authorization().basis().clone(),
        mapping_terms_evidence: openfigi_metadata.authorization().evidence().clone(),
        mapping_terms_effective: openfigi_metadata.authorization().effective_interval(),
        mapping_coverage_evidence: mapping.coverage_evidence().clone(),
        mapping_coverage_effective: openfigi_metadata.coverage().effective_interval(),
        mapping_access: mapping.access(),
        mapping_requested_at: mapping.requested_at(),
        mapping_received_at: mapping.received_at(),
        mapping_request_evidence: mapping.request().evidence().clone(),
        mapping_response_evidence: mapping.response().evidence().clone(),
        mapping_rate_limit_evidence: rate_limit_evidence(mapping),
        mapping_max_source_age_nanos: openfigi_metadata.freshness_policy().max_source_age_nanos(),
        mapping_max_transport_age_nanos: openfigi_metadata
            .freshness_policy()
            .max_transport_age_nanos(),
        mapping_max_market_age_nanos: openfigi_metadata.freshness_policy().max_market_age_nanos(),
        catalog_batch_digest: catalog_receipt.batch_digest(),
        catalog_inserted,
        catalog_replayed,
        definition_revision_digest: record.revision_digest(),
        definition_revision_sequence: record.revision_sequence(),
        definition_published_at: record.published_at(),
        definition_reference_evidence: definition.reference_evidence().clone(),
        definition_effective: definition.effective_interval(),
        quote_currency_evidence: definition.quote_currency_evidence().clone(),
        figi_source_id: figi_record.source_id().clone(),
        figi_source_evidence: figi_record.source_evidence().clone(),
        figi_observed_at: figi_record.observed_at(),
        figi_validity: figi_record.validity(),
        figi_rights_policy: figi_record.rights_policy().clone(),
        evaluated_at,
        expires_at,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    approval.digest = approval_digest(&approval);
    Ok(Some(approval))
}

fn validate_mapping(
    mapping: &OpenFigiMappingReceipt,
    listing: &NasdaqCurrentListing,
    candidate: &OpenFigiIdentityCandidate,
) -> Result<(), MarketReferenceIdentityError> {
    let [result] = mapping.results() else {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    };
    let job = result.job();
    if job.symbol() != listing.key().symbol()
        || job.mic() != listing.key().mic()
        || job.listing_source_id() != listing.source_id()
        || job.listing_metadata_revision() != listing.metadata_revision()
        || job.listing_payload_evidence() != listing.source_payload_evidence()
        || job.listing_source_timestamp() != listing.source_timestamp()
        || job.listing_observed_at() != listing.observed_at()
        || !matches!(result.outcome(), OpenFigiMappingOutcome::Exact(value) if value == candidate)
    {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    Ok(())
}

fn validate_catalog_receipt(
    receipt: &market_squawk_data::MarketDataInstrumentSynchronizationReceipt,
    disposition: OpenFigiCatalogDisposition,
) -> Result<(), MarketReferenceIdentityError> {
    let counts_match = match disposition {
        OpenFigiCatalogDisposition::InsertedOrAdvanced => {
            receipt.submitted() == 1 && receipt.inserted() == 1 && receipt.replayed() == 0
        }
        OpenFigiCatalogDisposition::Replayed => {
            receipt.submitted() == 1 && receipt.inserted() == 0 && receipt.replayed() == 1
        }
    };
    if !counts_match || receipt.batch_digest().bytes() == [0; 32] {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    Ok(())
}

fn rate_limit_evidence(mapping: &OpenFigiMappingReceipt) -> ExactPayloadEvidence {
    let rate = mapping.rate_limit();
    let mut hasher = Sha256::new();
    hasher.update(RATE_LIMIT_EVIDENCE_DOMAIN);
    update_bytes(&mut hasher, rate.raw_limit());
    update_bytes(&mut hasher, rate.raw_remaining());
    update_bytes(&mut hasher, rate.raw_reset());
    hasher.update(rate.limit().to_be_bytes());
    hasher.update(rate.remaining().to_be_bytes());
    hasher.update(rate.reset_after_seconds().to_be_bytes());
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn approval_digest(approval: &MarketReferenceIdentityApprovalV1) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(APPROVAL_DIGEST_DOMAIN);
    update_text(
        &mut hasher,
        approval.request.provider_instrument_id.as_str(),
    );
    update_text(&mut hasher, approval.request.venue_id.as_str());
    hasher.update(approval.instrument_id.as_uuid().as_bytes());
    update_text(&mut hasher, approval.permanent_figi.as_str());
    update_optional_figi(&mut hasher, approval.composite_figi.as_ref());
    update_optional_figi(&mut hasher, approval.share_class_figi.as_ref());
    hasher.update([asset_class_code(approval.asset_class)]);
    update_text(&mut hasher, approval.quote_currency.as_str());
    update_text(&mut hasher, approval.listing_source_id.as_str());
    update_revision(&mut hasher, &approval.listing_metadata_revision);
    update_revision_evidence(&mut hasher, &approval.listing_source_contract);
    update_text(
        &mut hasher,
        approval
            .listing_authorization_basis
            .as_source_identifier()
            .as_str(),
    );
    update_evidence(&mut hasher, &approval.listing_authorization_evidence);
    update_interval(&mut hasher, approval.listing_authorization_effective);
    update_evidence(&mut hasher, &approval.listing_coverage_evidence);
    update_interval(&mut hasher, approval.listing_coverage_effective);
    update_evidence(&mut hasher, &approval.listing_payload_evidence);
    update_timestamp(&mut hasher, approval.listing_source_timestamp);
    update_timestamp(&mut hasher, approval.listing_observed_at);
    hasher.update(approval.listing_max_source_age_nanos.to_be_bytes());
    hasher.update(approval.listing_max_transport_age_nanos.to_be_bytes());
    hasher.update(approval.listing_max_market_age_nanos.to_be_bytes());
    update_text(&mut hasher, approval.mapping_source_id.as_str());
    update_revision(&mut hasher, &approval.mapping_metadata_revision);
    update_revision_evidence(&mut hasher, &approval.mapping_source_contract);
    update_text(
        &mut hasher,
        approval
            .mapping_authorization_basis
            .as_source_identifier()
            .as_str(),
    );
    update_evidence(&mut hasher, &approval.mapping_terms_evidence);
    update_interval(&mut hasher, approval.mapping_terms_effective);
    update_evidence(&mut hasher, &approval.mapping_coverage_evidence);
    update_interval(&mut hasher, approval.mapping_coverage_effective);
    hasher.update([match approval.mapping_access {
        OpenFigiAccess::Public => 1,
        OpenFigiAccess::ApiKey => 2,
    }]);
    update_timestamp(&mut hasher, approval.mapping_requested_at);
    update_timestamp(&mut hasher, approval.mapping_received_at);
    update_evidence(&mut hasher, &approval.mapping_request_evidence);
    update_evidence(&mut hasher, &approval.mapping_response_evidence);
    update_evidence(&mut hasher, &approval.mapping_rate_limit_evidence);
    hasher.update(approval.mapping_max_source_age_nanos.to_be_bytes());
    hasher.update(approval.mapping_max_transport_age_nanos.to_be_bytes());
    hasher.update(approval.mapping_max_market_age_nanos.to_be_bytes());
    update_digest(&mut hasher, approval.catalog_batch_digest);
    hasher.update(approval.catalog_inserted.to_be_bytes());
    hasher.update(approval.catalog_replayed.to_be_bytes());
    update_digest(&mut hasher, approval.definition_revision_digest);
    hasher.update(approval.definition_revision_sequence.to_be_bytes());
    update_timestamp(&mut hasher, approval.definition_published_at);
    update_revision_evidence(&mut hasher, &approval.definition_reference_evidence);
    update_interval(&mut hasher, approval.definition_effective);
    update_evidence(&mut hasher, &approval.quote_currency_evidence);
    update_text(&mut hasher, approval.figi_source_id.as_str());
    update_evidence(&mut hasher, &approval.figi_source_evidence);
    update_timestamp(&mut hasher, approval.figi_observed_at);
    update_interval(&mut hasher, approval.figi_validity);
    update_text(
        &mut hasher,
        approval.figi_rights_policy.policy_id().as_str(),
    );
    hasher.update([entitlement_code(approval.figi_rights_policy.entitlement())]);
    update_text(
        &mut hasher,
        approval.figi_rights_policy.terms_reference().as_str(),
    );
    update_timestamp(&mut hasher, approval.evaluated_at);
    update_timestamp(&mut hasher, approval.expires_at);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn update_revision(hasher: &mut Sha256, revision: &MetadataRevision) {
    update_text(hasher, revision.as_source_identifier().as_str());
}

fn update_revision_evidence(hasher: &mut Sha256, evidence: &RevisionBoundPayloadEvidence) {
    update_revision(hasher, evidence.metadata_revision());
    update_evidence(hasher, evidence.payload_evidence());
}

fn update_evidence(hasher: &mut Sha256, evidence: &ExactPayloadEvidence) {
    update_digest(hasher, evidence.content_digest());
    match evidence.version_pinned_locator() {
        Some(locator) => {
            hasher.update([1]);
            update_text(hasher, locator.reference().as_str());
            update_text(hasher, locator.version().as_str());
        }
        None => hasher.update([0]),
    }
}

fn evidence_is_nonzero(evidence: &ExactPayloadEvidence) -> bool {
    evidence.content_digest().bytes() != [0; 32]
}

fn update_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(digest.bytes());
}

fn update_optional_figi(hasher: &mut Sha256, figi: Option<&Figi>) {
    match figi {
        Some(figi) => {
            hasher.update([1]);
            update_text(hasher, figi.as_str());
        }
        None => hasher.update([0]),
    }
}

fn update_interval(hasher: &mut Sha256, interval: EffectiveInterval) {
    update_timestamp(hasher, interval.starts_at());
    match interval.ends_at() {
        Some(end) => {
            hasher.update([1]);
            update_timestamp(hasher, end);
        }
        None => hasher.update([0]),
    }
}

fn update_timestamp(hasher: &mut Sha256, timestamp: Timestamp) {
    hasher.update(timestamp.unix_nanos().to_be_bytes());
}

fn update_text(hasher: &mut Sha256, value: &str) {
    update_bytes(hasher, value.as_bytes());
}

fn update_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    at >= interval.starts_at() && interval.ends_at().is_none_or(|end| at < end)
}

fn fresh_until(
    observed_at: Timestamp,
    maximum_age_nanos: u64,
) -> Result<Timestamp, MarketReferenceIdentityError> {
    let nanos = i64::try_from(maximum_age_nanos)
        .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)?;
    observed_at
        .checked_add_nanos(nanos)
        .map_err(|_| MarketReferenceIdentityError::InvalidEvidence)
}

fn minimize_expiry(current: &mut Timestamp, candidate: Timestamp) {
    if candidate < *current {
        *current = candidate;
    }
}

fn minimize_optional_expiry(current: &mut Timestamp, candidate: Option<Timestamp>) {
    if let Some(candidate) = candidate {
        minimize_expiry(current, candidate);
    }
}

const fn asset_class_code(asset_class: AssetClass) -> u8 {
    match asset_class {
        AssetClass::Equity => 1,
        AssetClass::Fund => 2,
        AssetClass::FixedIncome => 3,
        AssetClass::Option => 4,
        AssetClass::Future => 5,
        AssetClass::ForeignExchange => 6,
        AssetClass::Crypto => 7,
        AssetClass::Commodity => 8,
        AssetClass::Index => 9,
        AssetClass::Cash => 10,
    }
}

const fn entitlement_code(entitlement: IdentifierEntitlement) -> u8 {
    match entitlement {
        IdentifierEntitlement::UnknownOrRestricted => 1,
        IdentifierEntitlement::PublicDomain => 2,
        IdentifierEntitlement::UserOwned => 3,
        IdentifierEntitlement::LicensedInternalUse => 4,
        IdentifierEntitlement::LicensedRedistribution => 5,
    }
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), MarketReferenceIdentityError> {
    if cancellation.is_cancelled() {
        Err(MarketReferenceIdentityError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(MarketReferenceIdentityError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn system_timestamp() -> Result<Timestamp, MarketReferenceIdentityError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MarketReferenceIdentityError::Clock)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(MarketReferenceIdentityError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_nasdaq_error(error: NasdaqReferenceUniverseError) -> MarketReferenceIdentityError {
    match error {
        NasdaqReferenceUniverseError::Cancelled => MarketReferenceIdentityError::Cancelled,
        NasdaqReferenceUniverseError::DeadlineExceeded => {
            MarketReferenceIdentityError::DeadlineExceeded
        }
        NasdaqReferenceUniverseError::Clock => MarketReferenceIdentityError::Clock,
        NasdaqReferenceUniverseError::Capacity => MarketReferenceIdentityError::ResourceExhausted,
        _ => MarketReferenceIdentityError::Unavailable,
    }
}

fn map_openfigi_error(error: OpenFigiIdentityPublicationError) -> MarketReferenceIdentityError {
    match error {
        OpenFigiIdentityPublicationError::Cancelled => MarketReferenceIdentityError::Cancelled,
        OpenFigiIdentityPublicationError::DeadlineExceeded => {
            MarketReferenceIdentityError::DeadlineExceeded
        }
        OpenFigiIdentityPublicationError::Clock => MarketReferenceIdentityError::Clock,
        OpenFigiIdentityPublicationError::Capacity => {
            MarketReferenceIdentityError::ResourceExhausted
        }
        _ => MarketReferenceIdentityError::Unavailable,
    }
}

fn map_catalog_error(error: MarketDataInstrumentCatalogError) -> MarketReferenceIdentityError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => MarketReferenceIdentityError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => {
            MarketReferenceIdentityError::DeadlineExceeded
        }
        MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            MarketReferenceIdentityError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::CorruptCatalog => {
            MarketReferenceIdentityError::InvalidEvidence
        }
        _ => MarketReferenceIdentityError::Unavailable,
    }
}

/// Operational or structurally invalid failure distinct from an expected unavailable result.
#[derive(Debug, Error)]
pub(crate) enum MarketReferenceIdentityError {
    #[error("market reference identity resolution was cancelled")]
    Cancelled,
    #[error("market reference identity resolution deadline elapsed")]
    DeadlineExceeded,
    #[error("market reference identity authority is unavailable")]
    Unavailable,
    #[error("market reference identity evidence is internally inconsistent")]
    InvalidEvidence,
    #[error("market reference identity capacity is unavailable")]
    ResourceExhausted,
    #[error("market reference identity wall clock is unavailable")]
    Clock,
}
