use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId, IntegrityRule,
    LiveEventClass, MarketDepth, MetadataRevision, ProviderChannel, ProviderIdentityKey,
    ProviderInstrumentId, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier, VenueId,
    VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile, CoverageTopology,
    FreshnessPolicy, HistoricalCapability, InstrumentCoverage, LiveCoverageDeclaration,
    LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, ProviderBudgetPolicy,
    ProviderNumericPolicy, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataError,
    SourceMetadataInput, SourceProtocolProfile,
};
use serde::Serialize;
use thiserror::Error;

/// Sole public Advanced Trade market-data endpoint accepted by this protocol profile.
pub const COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT: &str =
    "wss://advanced-trade-ws.coinbase.com";
const COINBASE_VENUE: &str = "coinbase-exchange";
pub(crate) const COINBASE_PROVIDER: &str = "coinbase-exchange";
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
    provider_instrument_id: ProviderInstrumentId,
    venue_symbol: VenueSymbol,
    instrument: InstrumentId,
}

impl CoinbaseProductMapping {
    /// Constructs a syntactically valid Coinbase product mapping.
    ///
    /// # Errors
    ///
    /// Rejects product identifiers outside the bounded Exchange grammar.
    pub fn try_new(
        product: ProviderProduct,
        instrument: InstrumentId,
    ) -> Result<Self, CoinbaseConfigError> {
        let (provider_instrument_id, venue_symbol) = native_product_identity(&product)?;
        Ok(Self {
            product,
            provider_instrument_id,
            venue_symbol,
            instrument,
        })
    }

    /// Returns the exact provider product identity.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the Coinbase-native product identity within a source namespace.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the independent Coinbase Exchange venue symbol.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }

    /// Returns the mapped stable internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
}

/// Exact profile revision and provider-native coordinate selected for one configured product.
///
/// This adapter-local value carries no registry or canonical-selection authority. It keeps the
/// provider key, provider-profile revision and digest, venue symbol, and canonical instrument
/// together so public and Direct decoders cannot reconstruct any coordinate from a diagnostic
/// message identity.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CoinbaseNativeProductCoordinate {
    mapping: CoinbaseProductMapping,
    identity_key: ProviderIdentityKey,
    identity_revision: MetadataRevision,
    identity_digest: EvidenceDigest,
    venue: VenueId,
}

impl CoinbaseNativeProductCoordinate {
    pub(crate) fn try_new(
        mapping: CoinbaseProductMapping,
        source_id: SourceId,
        revision_evidence: &RevisionBoundPayloadEvidence,
    ) -> Result<Self, CoinbaseConfigError> {
        let venue = VenueId::try_from(COINBASE_VENUE)?;
        let coordinate = Self {
            identity_key: ProviderIdentityKey::new(
                source_id,
                mapping.provider_instrument_id().clone(),
            ),
            mapping,
            identity_revision: revision_evidence.metadata_revision().clone(),
            identity_digest: revision_evidence.payload_evidence().content_digest(),
            venue,
        };
        coordinate.validate_static()?;
        Ok(coordinate)
    }

    pub(crate) const fn product(&self) -> &ProviderProduct {
        self.mapping.product()
    }

    pub(crate) const fn mapping(&self) -> &CoinbaseProductMapping {
        &self.mapping
    }

    pub(crate) const fn provider_identity_key(&self) -> &ProviderIdentityKey {
        &self.identity_key
    }

    pub(crate) const fn identity_revision(&self) -> &MetadataRevision {
        &self.identity_revision
    }

    pub(crate) const fn identity_digest(&self) -> EvidenceDigest {
        self.identity_digest
    }

    pub(crate) const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub(crate) const fn venue_symbol(&self) -> &VenueSymbol {
        self.mapping.venue_symbol()
    }

    pub(crate) const fn instrument(&self) -> InstrumentId {
        self.mapping.instrument()
    }

    pub(crate) fn validate_metadata(
        &self,
        metadata: &SourceMetadata,
    ) -> Result<(), CoinbaseConfigError> {
        self.validate_static()?;
        if metadata.source_id() != self.provider_identity_key().source_id()
            || metadata.provider().as_str() != COINBASE_PROVIDER
            || metadata.revision() != self.identity_revision()
            || metadata
                .revision_evidence()
                .payload_evidence()
                .content_digest()
                != self.identity_digest()
        {
            return Err(CoinbaseConfigError::InvalidNativeProductCoordinate);
        }
        Ok(())
    }

    pub(crate) fn validates_wire_product(&self, product: &str) -> bool {
        self.product().as_source_identifier().as_str() == product
            && self
                .provider_identity_key()
                .provider_instrument_id()
                .as_str()
                == product
            && self.venue_symbol().as_str() == product
    }

    fn validate_static(&self) -> Result<(), CoinbaseConfigError> {
        if self.venue().as_str() != COINBASE_VENUE
            || !self.validates_wire_product(self.product().as_source_identifier().as_str())
        {
            return Err(CoinbaseConfigError::InvalidNativeProductCoordinate);
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
    coordinate: Arc<CoinbaseNativeProductCoordinate>,
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
        validate_mappings(&mappings)?;
        validate_channels(&channels)?;

        let coordinate = Arc::new(CoinbaseNativeProductCoordinate::try_new(
            mappings
                .first()
                .ok_or(CoinbaseConfigError::InvalidMappingCount)?
                .clone(),
            source_id.clone(),
            &revision_evidence,
        )?);

        let venue = coordinate.venue().clone();
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
        let coverage = SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(venue),
            InstrumentCoverage::enumerated(vec![coordinate.instrument()])?,
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
        coordinate.validate_metadata(&metadata)?;
        let subscriptions = subscription_payloads(&mappings, &channels)?;
        Ok(Self {
            metadata,
            coordinate,
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
        std::slice::from_ref(self.coordinate.mapping())
    }

    pub(crate) const fn native_coordinate(&self) -> &Arc<CoinbaseNativeProductCoordinate> {
        &self.coordinate
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

fn native_product_identity(
    product: &ProviderProduct,
) -> Result<(ProviderInstrumentId, VenueSymbol), CoinbaseConfigError> {
    let value = product.as_source_identifier().as_str();
    if value.is_empty()
        || value.len() > MAX_PRODUCT_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CoinbaseConfigError::InvalidProduct);
    }
    let provider_instrument_id = ProviderInstrumentId::try_from(value)?;
    let venue_symbol =
        VenueSymbol::try_from(value).map_err(|_error| CoinbaseConfigError::InvalidProduct)?;
    if provider_instrument_id.as_str() != value || venue_symbol.as_str() != value {
        return Err(CoinbaseConfigError::InvalidNativeProductCoordinate);
    }
    Ok((provider_instrument_id, venue_symbol))
}

fn validate_mappings(mappings: &[CoinbaseProductMapping]) -> Result<(), CoinbaseConfigError> {
    if mappings.is_empty() || mappings.len() > MAX_PRODUCTS {
        return Err(CoinbaseConfigError::InvalidMappingCount);
    }
    let mut products = BTreeSet::new();
    let mut instruments = BTreeSet::new();
    for mapping in mappings {
        if !products.insert(mapping.product.as_source_identifier().as_str()) {
            return Err(CoinbaseConfigError::DuplicateProduct);
        }
        if !instruments.insert(mapping.instrument) {
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
    /// Product, provider identity, profile revision/digest, venue, and venue symbol diverged.
    #[error("Coinbase provider-native product coordinate is inconsistent")]
    InvalidNativeProductCoordinate,
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
