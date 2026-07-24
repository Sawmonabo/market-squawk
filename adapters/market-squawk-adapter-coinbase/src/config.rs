use std::collections::BTreeSet;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    MarketDepth, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier, VenueId,
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

/// Sole production endpoint accepted by this protocol profile.
pub const COINBASE_EXCHANGE_ENDPOINT: &str = "wss://ws-feed.exchange.coinbase.com";
const COINBASE_VENUE: &str = "coinbase-exchange";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const CONFIGURED_PRODUCTS: &str = "coinbase-exchange-configured-products-v1";
const CONFIGURED_CHANNELS: &str = "level2_batch+matches+heartbeat";
const MAX_PRODUCTS: usize = 100;
const MAX_PRODUCT_BYTES: usize = 64;
const MAX_SUBSCRIPTION_BYTES: usize = 16 * 1024;
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Closed channel set supported by the pinned Exchange v1 adapter.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CoinbaseChannel {
    /// Unauthenticated batched price-level snapshot followed by absolute-size updates.
    Level2,
    /// Match and initial `last_match` trade messages.
    Matches,
    /// Feed-health heartbeat; never market-price freshness.
    Heartbeat,
}

impl CoinbaseChannel {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Level2 => "level2_batch",
            Self::Matches => "matches",
            Self::Heartbeat => "heartbeat",
        }
    }
}

/// Explicit provider-product to stable internal-instrument mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseProductMapping {
    product: ProviderProduct,
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
        validate_product(product.as_source_identifier().as_str())?;
        Ok(Self {
            product,
            instrument,
        })
    }

    /// Returns the exact provider product identity.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the mapped stable internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
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
    subscription: Box<str>,
}

impl CoinbaseExchangeConfig {
    /// Builds the exact Exchange v1 source profile.
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

        let venue = VenueId::try_from(COINBASE_VENUE)?;
        let decoder_rule = rule("coinbase-exchange-v1-decoder")?;
        let timestamp_rule = rule("coinbase-exchange-rfc3339-timestamp")?;
        let sequence_rule = rule("coinbase-exchange-level2-sequence-unsupported")?;
        let checksum_rule = rule("coinbase-exchange-checksum-unsupported")?;
        let no_snapshot_rule = rule("coinbase-exchange-trade-snapshot-not-applicable")?;
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
                COINBASE_EXCHANGE_ENDPOINT,
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
                    rule("coinbase-exchange-maker-side-aggressor")?,
                    rule("coinbase-exchange-auction-unused")?,
                    rule("coinbase-exchange-status-unused")?,
                    rule("coinbase-exchange-corporate-action-unused")?,
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
        let subscription = subscription_payload(&mappings, &channels)?;
        Ok(Self {
            metadata,
            mappings: mappings.into_boxed_slice(),
            channels: channels.into_boxed_slice(),
            limits,
            subscription,
        })
    }

    /// Returns the immutable exact source metadata.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the only production endpoint.
    pub const fn endpoint(&self) -> &'static str {
        COINBASE_EXCHANGE_ENDPOINT
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

    pub(crate) fn subscription(&self) -> &str {
        &self.subscription
    }
}

#[derive(Serialize)]
struct Subscription<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    product_ids: Vec<&'a str>,
    channels: Vec<&'static str>,
}

fn subscription_payload(
    mappings: &[CoinbaseProductMapping],
    channels: &[CoinbaseChannel],
) -> Result<Box<str>, CoinbaseConfigError> {
    let subscription = Subscription {
        kind: "subscribe",
        product_ids: mappings
            .iter()
            .map(|mapping| mapping.product.as_source_identifier().as_str())
            .collect(),
        channels: channels
            .iter()
            .copied()
            .map(CoinbaseChannel::as_str)
            .collect(),
    };
    let payload =
        serde_json::to_string(&subscription).map_err(|_| CoinbaseConfigError::Serialization)?;
    if payload.len() > MAX_SUBSCRIPTION_BYTES {
        return Err(CoinbaseConfigError::SubscriptionTooLarge);
    }
    Ok(payload.into_boxed_str())
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
        CoinbaseChannel::Matches,
        CoinbaseChannel::Heartbeat,
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
    #[error("Coinbase channel profile must be exactly level2_batch, matches, and heartbeat")]
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
    /// Validated source metadata did not expose the exact live protocol invariants.
    #[error("Coinbase source metadata is missing its validated live protocol profile")]
    InvalidProtocolProfile,
    /// Direct profile requires explicit user-authorized read-only market-data evidence.
    #[error("Coinbase Direct profile requires user-authorized credentials")]
    InvalidDirectAuthorization,
    /// Direct snapshot, segmentation, replay, or level-3 owner limits are invalid.
    #[error("Coinbase Direct bounds are invalid")]
    InvalidDirectLimits,
}
