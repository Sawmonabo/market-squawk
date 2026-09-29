//! Coinbase Exchange Direct identity extracted from its original product response.

use market_squawk_adapter_coinbase::{
    CoinbaseDirectConfig, CoinbaseDirectProductReferenceEvidence,
};
use market_squawk_domain::{
    AssetClass, Currency, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, IdentifierError,
    IdentityError, InstrumentDefinition, InstrumentError, InstrumentExecutionTerms, InstrumentId,
    MetadataRevision, ProviderIdentityEvidence, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp,
    TradingStatus, VenueId, VenueSymbol,
};
use market_squawk_sources::ProviderNativeIdentityRequest;
use thiserror::Error;

use crate::live_source::crypto_reference::AcceptedCatalogReference;

const DIRECT_IDENTITY_NAMESPACE: &str = "coinbase-exchange-direct";

/// Checked native assertion for one original Coinbase Exchange product response.
///
/// This is source evidence for the catalog writer. It is not a selected current identity:
/// the registry must obtain that separately from the catalog after atomic publication.
#[derive(Clone, Debug)]
pub(super) struct DirectProductCatalogAssertion {
    expected: InstrumentDefinition,
    provider_identity: ProviderIdentityRecord,
    instrument: InstrumentId,
    venue: VenueId,
    venue_symbol: VenueSymbol,
    quote_currency: Currency,
    execution_terms: InstrumentExecutionTerms,
    observed_at: Timestamp,
    body_digest: EvidenceDigest,
}

impl DirectProductCatalogAssertion {
    /// Constructs the distinct Direct identity from the already captured product response.
    /// The caller must retain its original capture for bootstrap; this path performs no GET.
    pub(super) fn try_new(
        config: &CoinbaseDirectConfig,
        evidence: &CoinbaseDirectProductReferenceEvidence,
        expected: &InstrumentDefinition,
    ) -> Result<Self, DirectProductReferenceError> {
        let product = evidence.product().as_source_identifier().as_str();
        let capture = evidence.capture_receipt().capture();
        let [page] = capture.pages() else {
            return Err(DirectProductReferenceError::ProductCaptureMismatch);
        };
        if evidence.product() != config.product()
            || capture.source_id() != config.product_reference_profile().metadata().source_id()
            || capture.metadata_revision()
                != config.product_reference_profile().metadata().revision()
            || capture.dataset().as_str() != config.product_url()
            || page.http_status() != 200
            || page.body_bytes() == 0
            || page.body_digest().bytes() == [0; 32]
        {
            return Err(DirectProductReferenceError::ProductCaptureMismatch);
        }
        if evidence.trading_status() != TradingStatus::Active
            || evidence.trading_disabled()
            || evidence.cancel_only()
            || evidence.post_only()
            || evidence.limit_only()
            || evidence.auction_mode()
        {
            return Err(DirectProductReferenceError::ProductUnavailable);
        }

        // Exchange identifies a product as BASE-QUOTE. These independent response fields
        // prevent an ID-only or configured mapping from supplying catalog reference evidence.
        let base = evidence
            .base_currency()
            .ok_or(DirectProductReferenceError::MissingCurrency)?;
        let quote = evidence
            .quote_currency()
            .ok_or(DirectProductReferenceError::MissingCurrency)?;
        if format!("{base}-{quote}") != product
            || quote.as_str() != expected.quote_currency().as_str()
        {
            return Err(DirectProductReferenceError::ProductCurrencyMismatch);
        }

        let namespace = SourceId::try_from(DIRECT_IDENTITY_NAMESPACE)?;
        let provider_instrument_id = ProviderInstrumentId::try_from(product)?;
        let venue_symbol = VenueSymbol::try_from(product)?;
        if expected.instrument_id() != config.instrument()
            || expected.asset_class() != AssetClass::Crypto
            || expected.execution_terms() != config.execution_terms()
            || !expected.venue_mappings().iter().any(|mapping| {
                mapping.venue_id() == config.venue() && mapping.venue_symbol() == &venue_symbol
            })
        {
            return Err(DirectProductReferenceError::RouteMismatch);
        }
        let body_digest = page.body_digest();
        let revision = MetadataRevision::new(SourceIdentifier::try_from(format!(
            "coinbase-exchange-direct-product-{}",
            hex_digest(body_digest)
        ))?);
        let observed_at = evidence.observed_at();
        let authorization = config.metadata().authorization().effective_interval();
        if observed_at < authorization.starts_at()
            || authorization
                .ends_at()
                .is_some_and(|end| observed_at >= end)
        {
            return Err(DirectProductReferenceError::ProductCaptureMismatch);
        }
        let provider_identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id: config.instrument(),
            source_id: namespace,
            provider_instrument_id,
            evidence: ProviderIdentityEvidence::from_content_digest(body_digest),
            source_timestamp: None,
            observed_at,
            metadata_revision: revision,
            validity: EffectiveInterval::new(observed_at, authorization.ends_at())?,
            supersedes: None,
        });
        Ok(Self {
            expected: expected.clone(),
            provider_identity,
            instrument: config.instrument(),
            venue: config.venue().clone(),
            venue_symbol,
            quote_currency: expected.quote_currency(),
            execution_terms: config.execution_terms(),
            observed_at,
            body_digest,
        })
    }

    /// Returns the first assertion for an absent Direct identity key.
    /// A changed accepted assertion needs real supersession evidence; callers may replay the
    /// current accepted identity only after checking its native route and quote currency.
    pub(super) const fn provider_identity(&self) -> &ProviderIdentityRecord {
        &self.provider_identity
    }

    pub(super) const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub(super) const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub(super) const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }

    pub(super) const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }

    pub(super) const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    pub(super) const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Supplies the shared atomic catalog merger with only sealed, cross-checked Direct facts.
    pub(super) fn accepted_catalog_reference(&self) -> AcceptedCatalogReference {
        AcceptedCatalogReference {
            expected: self.expected.clone(),
            namespace: self.provider_identity.source_id().clone(),
            native_id: self.provider_identity.provider_instrument_id().clone(),
            canonical: self.instrument,
            venue: self.venue.clone(),
            symbol: self.venue_symbol.clone(),
            quote: self.quote_currency,
            evidence: ExactPayloadEvidence::from_content_digest(self.body_digest),
            validity: self.provider_identity.validity(),
            observed_at: self.observed_at,
        }
    }

    /// Builds the exact A1 request after the catalog writer has published this assertion.
    /// The knowledge cutoff must be captured after publication, not at HTTP receipt time.
    pub(super) fn request_at(
        &self,
        knowledge_at: Timestamp,
    ) -> Result<ProviderNativeIdentityRequest, DirectProductReferenceError> {
        if knowledge_at.unix_nanos() < self.observed_at.unix_nanos() {
            return Err(DirectProductReferenceError::InvalidKnowledgeCutoff);
        }
        Ok(ProviderNativeIdentityRequest {
            namespace: self.provider_identity.source_id().clone(),
            provider_instrument_id: self.provider_identity.provider_instrument_id().clone(),
            instrument: self.instrument,
            venue: self.venue.clone(),
            venue_symbol: self.venue_symbol.clone(),
            knowledge_at,
            effective_at: knowledge_at,
        })
    }
}

fn hex_digest(digest: EvidenceDigest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in digest.bytes() {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

/// The Direct response cannot establish the requested native route.
#[derive(Debug, Error)]
pub(super) enum DirectProductReferenceError {
    #[error("Coinbase Direct product capture does not match the configured route")]
    ProductCaptureMismatch,
    #[error("Coinbase Direct product response lacks currency identity")]
    MissingCurrency,
    #[error("Coinbase Direct product currency does not match the configured route")]
    ProductCurrencyMismatch,
    #[error("Coinbase Direct product does not match the admitted canonical route")]
    RouteMismatch,
    #[error("Coinbase Direct product is unavailable for the configured live route")]
    ProductUnavailable,
    #[error("Coinbase Direct catalog knowledge cutoff predates the product observation")]
    InvalidKnowledgeCutoff,
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Instrument(#[from] InstrumentError),
}
