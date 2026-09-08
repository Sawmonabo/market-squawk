//! Evidence-bound canonical identity approval for one caller-selected market reference.
//!
//! This kernel corroborates an exact official listing against the repository-owned market-data
//! definition catalog. It never mints an [`InstrumentId`], derives one from an external
//! identifier, or promotes an optional provider identifier into canonical authority.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS, MarketDataInstrumentCatalogError,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
};
use market_squawk_domain::{
    AssetClass, AuthorizationBasis, Currency, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, InstrumentId, MetadataRevision, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceId, Timestamp, VenueId,
};
use market_squawk_sources::{AuthorizationMode, SourceMetadata};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::nasdaq_reference::{
    NasdaqCurrentListing, NasdaqListingKey, NasdaqReferenceUniverseError,
    NasdaqReferenceUniverseService,
};

const APPROVAL_DIGEST_DOMAIN: &[u8] = b"market-squawk/market-reference-identity-approval/v2\0";

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

/// Expected, fail-closed reason that an exact canonical identity cannot currently be approved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketReferenceIdentityUnavailable {
    /// The official directory has no current row for the exact symbol and venue.
    ListingNotCurrent,
    /// No repository-owned definition has the exact official venue-symbol mapping.
    CanonicalInstrumentUnresolved,
    /// More than one repository-owned identity claims the exact official venue-symbol mapping.
    CanonicalInstrumentAmbiguous,
    /// A selected repository-owned identity no longer has a current immutable definition.
    CanonicalDefinitionUnavailable,
    /// One of the exact source or definition authorities is no longer effective or fresh.
    EvidenceExpired,
}

/// Exact result of one bounded identity-resolution request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MarketReferenceIdentityResolution {
    Available(MarketReferenceIdentityApprovalV1),
    Unavailable(MarketReferenceIdentityUnavailable),
}

/// Non-forgeable reference-only approval backed by official listing and immutable catalog evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketReferenceIdentityApprovalV1 {
    request: MarketReferenceIdentityRequest,
    instrument_id: InstrumentId,
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
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_published_at: Timestamp,
    definition_reference_evidence: RevisionBoundPayloadEvidence,
    definition_effective: EffectiveInterval,
    quote_currency_evidence: ExactPayloadEvidence,
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

    pub(crate) const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    pub(crate) const fn definition_reference_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.definition_reference_evidence
    }

    pub(crate) const fn quote_currency_evidence(&self) -> &ExactPayloadEvidence {
        &self.quote_currency_evidence
    }

    pub(crate) const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    /// Returns the exclusive expiry bounded by official source and definition evidence.
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Generic source-kernel authority for one exact repository-owned reference identity.
#[derive(Clone)]
pub(crate) struct MarketReferenceIdentityAuthority {
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    catalog: MarketDataInstrumentReadCapability,
}

impl std::fmt::Debug for MarketReferenceIdentityAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MarketReferenceIdentityAuthority")
            .field("nasdaq", &self.nasdaq)
            .field("catalog", &self.catalog)
            .finish()
    }
}

impl MarketReferenceIdentityAuthority {
    pub(crate) const fn new(
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        catalog: MarketDataInstrumentReadCapability,
    ) -> Self {
        Self { nasdaq, catalog }
    }

    /// Corroborates one exact current listing with a unique repository-owned definition.
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

        let search = self
            .catalog
            .search(
                request.provider_instrument_id().as_str(),
                MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS,
                deadline,
                cancellation,
            )
            .map_err(map_catalog_error)?;
        let mut exact = BTreeMap::new();
        for matched in search.matches() {
            let record = matched.record();
            if definition_matches_listing(record, &listing, &request) {
                exact.insert(record.definition().instrument_id(), record.clone());
            }
        }
        if search.has_more() || exact.len() > 1 {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::CanonicalInstrumentAmbiguous,
            ));
        }
        let Some((instrument_id, _searched_record)) = exact.pop_first() else {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::CanonicalInstrumentUnresolved,
            ));
        };
        let Some(record) = self
            .catalog
            .latest(instrument_id, deadline, cancellation)
            .map_err(map_catalog_error)?
        else {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::CanonicalDefinitionUnavailable,
            ));
        };
        if !definition_matches_listing(&record, &listing, &request) {
            return Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::CanonicalDefinitionUnavailable,
            ));
        }

        check_operation(deadline, cancellation)?;
        let evaluated_at = system_timestamp()?;
        match build_approval(
            request,
            listing,
            &record,
            self.nasdaq.reference_identity_metadata(),
            evaluated_at,
        )? {
            Some(approval) => Ok(MarketReferenceIdentityResolution::Available(approval)),
            None => Ok(MarketReferenceIdentityResolution::Unavailable(
                MarketReferenceIdentityUnavailable::EvidenceExpired,
            )),
        }
    }
}

fn definition_matches_listing(
    record: &MarketDataInstrumentRecord,
    listing: &NasdaqCurrentListing,
    request: &MarketReferenceIdentityRequest,
) -> bool {
    let definition = record.definition();
    listing.key().symbol() == request.provider_instrument_id()
        && listing.key().mic() == request.venue_id()
        && definition.asset_class() == listing.asset_class()
        && definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id() == request.venue_id()
                && mapping.venue_symbol().as_str() == request.provider_instrument_id().as_str()
        })
}

fn build_approval(
    request: MarketReferenceIdentityRequest,
    listing: NasdaqCurrentListing,
    record: &MarketDataInstrumentRecord,
    nasdaq_metadata: &SourceMetadata,
    evaluated_at: Timestamp,
) -> Result<Option<MarketReferenceIdentityApprovalV1>, MarketReferenceIdentityError> {
    let definition = record.definition();
    if !definition_matches_listing(record, &listing, &request)
        || !nasdaq_metadata
            .coverage()
            .asset_classes()
            .contains(&listing.asset_class())
        || !evidence_is_nonzero(definition.reference_payload_evidence())
        || !evidence_is_nonzero(definition.quote_currency_evidence())
        || !evidence_is_nonzero(listing.source_payload_evidence())
        || !evidence_is_nonzero(nasdaq_metadata.revision_evidence().payload_evidence())
        || !evidence_is_nonzero(nasdaq_metadata.authorization().evidence())
        || !evidence_is_nonzero(nasdaq_metadata.coverage().evidence())
        || record.revision_digest().bytes() == [0; 32]
        || record.revision_sequence() == 0
        || listing.source_id() != nasdaq_metadata.source_id()
        || listing.metadata_revision() != nasdaq_metadata.revision()
        || nasdaq_metadata.authorization().mode() != AuthorizationMode::PublicInterface
        || listing.source_timestamp() > listing.observed_at()
        || listing.observed_at() > evaluated_at
        || record.published_at() > evaluated_at
    {
        return Err(MarketReferenceIdentityError::InvalidEvidence);
    }
    if !nasdaq_metadata.is_effective_at(evaluated_at)
        || !interval_contains(definition.effective_interval(), evaluated_at)
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
    minimize_optional_expiry(&mut expires_at, definition.effective_interval().ends_at());
    if evaluated_at >= expires_at {
        return Ok(None);
    }

    let mut approval = MarketReferenceIdentityApprovalV1 {
        request,
        instrument_id: definition.instrument_id(),
        asset_class: definition.asset_class(),
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
        definition_revision_digest: record.revision_digest(),
        definition_revision_sequence: record.revision_sequence(),
        definition_published_at: record.published_at(),
        definition_reference_evidence: definition.reference_evidence().clone(),
        definition_effective: definition.effective_interval(),
        quote_currency_evidence: definition.quote_currency_evidence().clone(),
        evaluated_at,
        expires_at,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
    };
    approval.digest = approval_digest(&approval);
    Ok(Some(approval))
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
    update_digest(&mut hasher, approval.definition_revision_digest);
    hasher.update(approval.definition_revision_sequence.to_be_bytes());
    update_timestamp(&mut hasher, approval.definition_published_at);
    update_revision_evidence(&mut hasher, &approval.definition_reference_evidence);
    update_interval(&mut hasher, approval.definition_effective);
    update_evidence(&mut hasher, &approval.quote_currency_evidence);
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
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
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
