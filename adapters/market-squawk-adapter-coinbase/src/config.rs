use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    MarketDepth, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile, CoverageTopology,
    FreshnessPolicy, HistoricalCapability, InstrumentCoverage, LiveCoverageDeclaration,
    LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, ProviderBudgetPolicy,
    ProviderNativeInstrumentAttestation, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataError, SourceMetadataInput, SourceProtocolProfile,
};
use serde::Serialize;
use thiserror::Error;

/// Sole public Advanced Trade market-data endpoint accepted by this protocol profile.
pub const COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT: &str =
    "wss://advanced-trade-ws.coinbase.com";
const COINBASE_VENUE: &str = "coinbase-exchange";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const CONFIGURED_PRODUCTS: &str = "coinbase-advanced-trade-configured-products-v1";
const CONFIGURED_CHANNELS: &str = "level2+market_trades+heartbeats";
// Coinbase recommends distributing high-volume products across connections. The live application
// also owns one deterministic route per source generation, so this public profile is deliberately
// one product per connection rather than overstating multi-product routing support.
const MAX_PRODUCTS: usize = 1;
const MAX_PRODUCT_BYTES: usize = 64;
const MAX_SUBSCRIPTION_BYTES: usize = 16 * 1024;
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Closed public channel set supported by the pinned Advanced Trade adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CoinbaseChannel {
    /// Public price-level snapshot followed by absolute-size updates.
    Level2,
    /// Public batched market-trade messages.
    MarketTrades,
    /// Feed-health heartbeats; never market-price freshness.
    Heartbeats,
}

impl CoinbaseChannel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Level2 => "level2",
            Self::MarketTrades => "market_trades",
            Self::Heartbeats => "heartbeats",
        }
    }
}

/// Explicit provider-product to stable internal-instrument mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseProductMapping {
    product: ProviderProduct,
    instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
}

impl CoinbaseProductMapping {
    /// Constructs a syntactically valid Coinbase product mapping.
    ///
    /// # Errors
    ///
    /// Rejects product identifiers outside the bounded Exchange grammar.
    pub fn try_new(
        product: ProviderProduct,
        instrument_attestation: ProviderNativeInstrumentAttestation,
    ) -> Result<Self, CoinbaseConfigError> {
        let product_value = product.as_source_identifier().as_str();
        validate_product(product_value)?;
        if instrument_attestation.venue_mapping().venue_id().as_str() != COINBASE_VENUE
            || instrument_attestation
                .provider_key()
                .provider_instrument_id()
                .as_str()
                != product_value
            || instrument_attestation
                .venue_mapping()
                .venue_symbol()
                .as_str()
                != product_value
            || instrument_attestation
                .validate_at(instrument_attestation.selected_at())
                .is_err()
        {
            return Err(CoinbaseConfigError::InvalidNativeProductAttestation);
        }
        Ok(Self {
            product,
            instrument_attestation: Arc::new(instrument_attestation),
        })
    }

    /// Returns the exact provider product identity.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the mapped stable internal instrument.
    pub fn instrument(&self) -> InstrumentId {
        self.instrument_attestation.instrument_id()
    }

    /// Returns the exact durable provider/canonical identity selected before session construction.
    pub fn instrument_attestation(&self) -> &ProviderNativeInstrumentAttestation {
        self.instrument_attestation.as_ref()
    }

    pub(crate) const fn shared_instrument_attestation(
        &self,
    ) -> &Arc<ProviderNativeInstrumentAttestation> {
        &self.instrument_attestation
    }

    pub(crate) fn validate_source_scope(
        &self,
        source_id: &SourceId,
        effective: &EffectiveInterval,
    ) -> Result<(), CoinbaseConfigError> {
        let attestation = self.instrument_attestation();
        let validity_contains_profile = match (effective.ends_at(), attestation.valid_until()) {
            (Some(profile_end), Some(identity_end)) => profile_end <= identity_end,
            (Some(_), None) | (None, None) => true,
            (None, Some(_)) => false,
        };
        if attestation.provider_key().source_id() != source_id
            || attestation.validate_at(effective.starts_at()).is_err()
            || !validity_contains_profile
        {
            return Err(CoinbaseConfigError::InvalidNativeProductAttestation);
        }
        Ok(())
    }
}

/// Count and deadline limits for one exact connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseTransportLimits {
    max_frame_bytes: usize,
    connect_timeout: Duration,
    io_timeout: Duration,
}

impl CoinbaseTransportLimits {
    /// Constructs nonzero bounded transport limits.
    ///
    /// # Errors
    ///
    /// Rejects a zero/oversized frame bound or a zero/excessive operation timeout.
    pub fn try_new(
        max_frame_bytes: usize,
        connect_timeout: Duration,
        io_timeout: Duration,
    ) -> Result<Self, CoinbaseConfigError> {
        if max_frame_bytes == 0
            || max_frame_bytes > market_squawk_sources::MAX_RAW_FRAME_BYTES
            || connect_timeout.is_zero()
            || io_timeout.is_zero()
            || connect_timeout > MAX_OPERATION_TIMEOUT
            || io_timeout > MAX_OPERATION_TIMEOUT
        {
            return Err(CoinbaseConfigError::InvalidTransportLimits);
        }
        Ok(Self {
            max_frame_bytes,
            connect_timeout,
            io_timeout,
        })
    }

    /// Returns the exact incoming frame and message ceiling.
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    /// Returns the connect/handshake deadline.
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the subscription-write, read, pong, and close-response deadline.
    pub const fn io_timeout(self) -> Duration {
        self.io_timeout
    }
}

/// Immutable configuration and exact metadata for one Coinbase source profile.
#[derive(Clone, Debug)]
pub struct CoinbaseExchangeConfig {
    metadata: SourceMetadata,
    mappings: Box<[CoinbaseProductMapping]>,
    channels: Box<[CoinbaseChannel]>,
    limits: CoinbaseTransportLimits,
    subscriptions: Box<[Box<str>]>,
}

impl CoinbaseExchangeConfig {
    /// Builds the exact public Advanced Trade source profile.
    ///
    /// # Errors
    ///
    /// Rejects non-public authorization, missing/duplicate channels or mappings, invalid product
    /// syntax, excessive subscription state, incompatible budget authority, or metadata that could
    /// overstate the selected protocol.
    #[allow(
        clippy::too_many_arguments,
        reason = "source metadata evidence and runtime bounds remain explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        mappings: Vec<CoinbaseProductMapping>,
        channels: Vec<CoinbaseChannel>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: CoinbaseTransportLimits,
    ) -> Result<Self, CoinbaseConfigError> {
        if authorization.mode() != AuthorizationMode::PublicInterface {
            return Err(CoinbaseConfigError::InvalidAuthorization);
        }
        validate_mappings(&mappings, &source_id, &effective)?;
        validate_channels(&channels)?;

        let venue = mappings
            .first()
            .ok_or(CoinbaseConfigError::InvalidMappingCount)?
            .instrument_attestation()
            .venue_mapping()
            .venue_id()
            .clone();
        let decoder_rule = rule("coinbase-advanced-trade-v1-decoder")?;
        let timestamp_rule = rule("coinbase-advanced-trade-rfc3339-timestamp")?;
        let sequence_rule = rule("coinbase-advanced-trade-envelope-sequence-unbound")?;
        let checksum_rule = rule("coinbase-advanced-trade-checksum-unsupported")?;
        let no_snapshot_rule = rule("coinbase-advanced-trade-trade-snapshot-not-applicable")?;
        let live = LiveCoverageDeclaration::try_new(
            ProviderProduct::new(SourceIdentifier::try_from(CONFIGURED_PRODUCTS)?),
            ProviderChannel::new(SourceIdentifier::try_from(CONFIGURED_CHANNELS)?),
            vec![
                LiveCoverageRule::try_new(
                    LiveEventClass::BookSnapshot,
                    Some(MarketDepth::PriceLevel),
                    SnapshotApplicability::Required,
                )?,
                LiveCoverageRule::try_new(
                    LiveEventClass::BookDelta,
                    Some(MarketDepth::PriceLevel),
                    SnapshotApplicability::Required,
                )?,
                LiveCoverageRule::try_new(
                    LiveEventClass::Trade,
                    None,
                    SnapshotApplicability::NotApplicable {
                        metadata_rule: no_snapshot_rule,
                    },
                )?,
            ],
        )?;
        let instruments = mappings
            .iter()
            .map(CoinbaseProductMapping::instrument)
            .collect::<Vec<_>>();
        let coverage = SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(venue),
            InstrumentCoverage::enumerated(instruments)?,
            Some(live),
            CoverageDelay::RealTime,
            DeliveryEvidence::Unknown,
        )?;
        let provider = SourceIdentifier::try_from(COINBASE_PROVIDER)?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id,
            revision_evidence,
            SourceClass::Exchange,
            provider,
            authorization,
            coverage,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(market_squawk_sources::EndpointPolicy::try_new([
                COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT,
            ])?),
            freshness,
            Some(budget),
            SourceCapabilities::new(
                true,
                false,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
                decoder_rule,
                SemanticInterpretationProfile::new(
                    rule("coinbase-advanced-trade-maker-side-aggressor")?,
                    rule("coinbase-advanced-trade-auction-unused")?,
                    rule("coinbase-advanced-trade-status-unused")?,
                    rule("coinbase-advanced-trade-corporate-action-unused")?,
                ),
                timestamp_rule,
                SequenceValidationProfile::Unsupported {
                    rule: sequence_rule,
                },
                ChecksumValidationProfile::Unsupported {
                    rule: checksum_rule,
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ))),
        ))?;
        let subscriptions = subscription_payloads(&mappings, &channels)?;
        Ok(Self {
            metadata,
            mappings: mappings.into_boxed_slice(),
            channels: channels.into_boxed_slice(),
            limits,
            subscriptions,
        })
    }

    /// Returns the immutable exact source metadata.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the only production endpoint.
    pub const fn endpoint(&self) -> &'static str {
        COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT
    }

    /// Returns configured product mappings in subscription order.
    pub fn mappings(&self) -> &[CoinbaseProductMapping] {
        &self.mappings
    }

    /// Returns the exact channel profile in subscription order.
    pub fn channels(&self) -> &[CoinbaseChannel] {
        &self.channels
    }

    /// Returns immutable transport limits.
    pub const fn transport_limits(&self) -> CoinbaseTransportLimits {
        self.limits
    }

    pub(crate) fn subscriptions(&self) -> &[Box<str>] {
        &self.subscriptions
    }
}

#[derive(Serialize)]
struct Subscription<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    product_ids: Option<Vec<&'a str>>,
    channel: &'static str,
}

fn subscription_payloads(
    mappings: &[CoinbaseProductMapping],
    channels: &[CoinbaseChannel],
) -> Result<Box<[Box<str>]>, CoinbaseConfigError> {
    let products = mappings
        .iter()
        .map(|mapping| mapping.product.as_source_identifier().as_str())
        .collect::<Vec<_>>();
    let mut payloads = Vec::new();
    payloads
        .try_reserve_exact(channels.len())
        .map_err(|_error| CoinbaseConfigError::AllocationFailed)?;
    let mut total_bytes = 0_usize;
    for channel in channels {
        let subscription = Subscription {
            kind: "subscribe",
            product_ids: if *channel == CoinbaseChannel::Heartbeats {
                None
            } else {
                Some(products.clone())
            },
            channel: channel.as_str(),
        };
        let payload =
            serde_json::to_string(&subscription).map_err(|_| CoinbaseConfigError::Serialization)?;
        total_bytes = total_bytes
            .checked_add(payload.len())
            .ok_or(CoinbaseConfigError::SubscriptionTooLarge)?;
        if payload.len() > MAX_SUBSCRIPTION_BYTES || total_bytes > MAX_SUBSCRIPTION_BYTES {
            return Err(CoinbaseConfigError::SubscriptionTooLarge);
        }
        payloads.push(payload.into_boxed_str());
    }
    Ok(payloads.into_boxed_slice())
}

fn validate_product(value: &str) -> Result<(), CoinbaseConfigError> {
    if value.is_empty()
        || value.len() > MAX_PRODUCT_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CoinbaseConfigError::InvalidProduct);
    }
    Ok(())
}

fn validate_mappings(
    mappings: &[CoinbaseProductMapping],
    source_id: &SourceId,
    effective: &EffectiveInterval,
) -> Result<(), CoinbaseConfigError> {
    if mappings.is_empty() || mappings.len() > MAX_PRODUCTS {
        return Err(CoinbaseConfigError::InvalidMappingCount);
    }
    let mut products = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    for mapping in mappings {
        mapping.validate_source_scope(source_id, effective)?;
        if !products.insert(mapping.product.as_source_identifier().as_str()) {
            return Err(CoinbaseConfigError::DuplicateProduct);
        }
        if !instruments.insert(mapping.instrument()) {
            return Err(CoinbaseConfigError::DuplicateInstrument);
        }
    }
    Ok(())
}

fn validate_channels(channels: &[CoinbaseChannel]) -> Result<(), CoinbaseConfigError> {
    let actual = channels.iter().copied().collect::<BTreeSet<_>>();
    let required = BTreeSet::from([
        CoinbaseChannel::Level2,
        CoinbaseChannel::MarketTrades,
        CoinbaseChannel::Heartbeats,
    ]);
    if actual != required || channels.len() != required.len() {
        return Err(CoinbaseConfigError::InvalidChannelProfile);
    }
    Ok(())
}

fn rule(value: &str) -> Result<IntegrityRule, CoinbaseConfigError> {
    let version = RuleVersion::new(1).map_err(|_| CoinbaseConfigError::InvalidRule)?;
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(value)?,
        version,
    ))
}

#[cfg(test)]
pub(crate) fn fixture_product_mapping(
    source: &str,
    instrument: InstrumentId,
) -> Result<CoinbaseProductMapping, Box<dyn std::error::Error>> {
    use market_squawk_domain::Timestamp;

    fixture_product_mapping_with_effective(
        source,
        instrument,
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
    )
}

#[cfg(test)]
fn fixture_product_mapping_with_effective(
    source: &str,
    instrument: InstrumentId,
    effective: EffectiveInterval,
) -> Result<CoinbaseProductMapping, Box<dyn std::error::Error>> {
    use market_squawk_domain::{
        Currency, EvidenceDigest, MarketDataInstrumentDefinition,
        MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
        ProviderIdentityKey, ProviderIdentityRecord, ProviderIdentityRecordInput,
        ProviderInstrumentId, Timestamp, VenueId, VenueMapping, VenueSymbol,
    };
    use market_squawk_sources::ProviderNativeInstrumentAttestationInput;

    let source_id = SourceId::try_from(source)?;
    let provider_instrument_id = ProviderInstrumentId::try_from("BTC-USD")?;
    let venue_mapping = VenueMapping::new(
        VenueId::try_from(COINBASE_VENUE)?,
        VenueSymbol::try_from("BTC-USD")?,
    );
    let digest =
        |byte| EvidenceDigest::new(market_squawk_domain::DigestAlgorithm::Sha256, [byte; 32]);
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: instrument,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("coinbase-test-reference-v1")?),
                ExactPayloadEvidence::from_content_digest(digest(41)),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Crypto,
            display_name: None,
            quote_currency: Currency::try_from("USD")?,
            quote_currency_evidence: ExactPayloadEvidence::from_content_digest(digest(42)),
            venue_mappings: vec![venue_mapping.clone()],
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: instrument,
                source_id: source_id.clone(),
                provider_instrument_id: provider_instrument_id.clone(),
                evidence: ProviderIdentityEvidence::from_content_digest(digest(43)),
                source_timestamp: None,
                observed_at: Timestamp::from_unix_nanos(0),
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                    "coinbase-test-provider-identity-v1",
                )?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: Vec::new(),
        })?;
    let attestation = ProviderNativeInstrumentAttestation::try_select(
        ProviderNativeInstrumentAttestationInput {
            definition: &definition,
            definition_revision_digest: digest(44),
            definition_published_at: Timestamp::from_unix_nanos(0),
            provider_key: ProviderIdentityKey::new(source_id, provider_instrument_id),
            venue_mapping,
            selected_at: Timestamp::from_unix_nanos(0),
        },
    )?;
    Ok(CoinbaseProductMapping::try_new(
        ProviderProduct::new(SourceIdentifier::try_from("BTC-USD")?),
        attestation,
    )?)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use market_squawk_domain::Timestamp;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn native_attestation_scope_must_cover_the_exact_source_profile() -> TestResult {
        let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let identity_end = Timestamp::from_unix_nanos(10);
        let mapping = fixture_product_mapping_with_effective(
            "coinbase-exchange-public",
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), Some(identity_end))?,
        )?;
        let source = SourceId::try_from("coinbase-exchange-public")?;
        assert_eq!(
            mapping.validate_source_scope(
                &source,
                &EffectiveInterval::new(Timestamp::from_unix_nanos(0), Some(identity_end))?,
            ),
            Ok(())
        );
        assert_eq!(
            mapping.validate_source_scope(
                &source,
                &EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            ),
            Err(CoinbaseConfigError::InvalidNativeProductAttestation)
        );
        assert_eq!(
            mapping.validate_source_scope(
                &SourceId::try_from("coinbase-other-source")?,
                &EffectiveInterval::new(Timestamp::from_unix_nanos(0), Some(identity_end),)?,
            ),
            Err(CoinbaseConfigError::InvalidNativeProductAttestation)
        );
        Ok(())
    }
}

/// Coinbase configuration invariant failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseConfigError {
    /// An identity or rule version was outside its bounded grammar.
    #[error("Coinbase configuration contains an invalid bounded identity")]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// Metadata declarations were internally inconsistent.
    #[error("Coinbase source metadata is invalid: {0}")]
    Metadata(#[from] SourceMetadataError),
    /// Network or budget policy was not valid for the pinned endpoint/authorization.
    #[error("Coinbase source policy is invalid: {0}")]
    Network(#[from] market_squawk_sources::NetworkPolicyError),
    /// A static provider validation rule could not be represented.
    #[error("Coinbase validation rule is invalid")]
    InvalidRule,
    /// Only the public-interface profile is supported by this adapter.
    #[error("Coinbase Exchange public adapter requires public-interface authorization")]
    InvalidAuthorization,
    /// Product identifier violated the Exchange grammar.
    #[error("Coinbase product identifier is invalid")]
    InvalidProduct,
    /// Durable provider identity, canonical instrument, venue, and provider product diverged.
    #[error("Coinbase provider-native instrument attestation is inconsistent")]
    InvalidNativeProductAttestation,
    /// Product mapping count was empty or exceeded the connection ceiling.
    #[error("Coinbase product mapping count is invalid")]
    InvalidMappingCount,
    /// The same provider product was mapped more than once.
    #[error("Coinbase provider product is duplicated")]
    DuplicateProduct,
    /// One internal instrument was ambiguously mapped to multiple provider products.
    #[error("Coinbase internal instrument mapping is duplicated")]
    DuplicateInstrument,
    /// The required exact channel profile was missing, duplicated, or extended.
    #[error("Coinbase channel profile must be exactly level2, market_trades, and heartbeats")]
    InvalidChannelProfile,
    /// Frame or operation timeout bounds were invalid.
    #[error("Coinbase transport limits are invalid")]
    InvalidTransportLimits,
    /// Subscription JSON could not be constructed.
    #[error("Coinbase subscription serialization failed")]
    Serialization,
    /// Subscription bytes exceeded the fixed outbound ceiling.
    #[error("Coinbase subscription exceeds its byte ceiling")]
    SubscriptionTooLarge,
    /// Subscription payload storage could not be reserved within its fixed channel count.
    #[error("Coinbase subscription allocation failed")]
    AllocationFailed,
    /// Validated source metadata did not expose the exact live protocol invariants.
    #[error("Coinbase source metadata is missing its validated live protocol profile")]
    InvalidProtocolProfile,
    /// Direct profile requires explicit user-authorized read-only market-data evidence.
    #[error("Coinbase Direct profile requires user-authorized credentials")]
    InvalidDirectAuthorization,
    /// Direct execution terms belong to a different mapped instrument.
    #[error("Coinbase Direct execution terms do not match the mapped instrument")]
    InvalidDirectInstrumentTerms,
    /// Direct provider budget cannot fund the concurrent WebSocket and REST bootstrap.
    #[error("Coinbase Direct provider budget cannot fund transport bootstrap")]
    InvalidDirectBudget,
    /// Direct snapshot, segmentation, replay, or level-3 owner limits are invalid.
    #[error("Coinbase Direct bounds are invalid")]
    InvalidDirectLimits,
}
