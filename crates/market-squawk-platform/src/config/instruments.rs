//! Strict production instrument and subscription configuration.

use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    num::NonZeroUsize,
    time::Duration,
};

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, Currency, Denomination, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentDefinitionRevision, InstrumentId, LiveEventClass, LotSize,
    MAX_LIVE_CAPTURE_PAYLOAD_BYTES, MarketDepth, SourceIdentifier, TickSize, Timestamp,
    TradingStatus, VenueId, VenueMapping, VenueSymbol, VersionPinnedSourceLocator,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use super::ConfigError;

/// Pinned public Coinbase Exchange WebSocket endpoint accepted by production configuration.
pub const COINBASE_EXCHANGE_ENDPOINT: &str = "wss://ws-feed.exchange.coinbase.com";

const COINBASE_VENUE: &str = "coinbase-exchange";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const MAX_INSTRUMENTS: usize = 100;
const MAX_COINBASE_PRODUCT_BYTES: usize = 64;
const MAX_SUBSCRIPTION_BYTES: usize = 16 * 1024;
const MAX_ENVIRONMENT_PROFILE_BYTES: usize = 128 * 1024;
const MAX_LEGACY_PRODUCTS: usize = 128;
const MAX_LEGACY_PRODUCT_BYTES: usize = 128;
const MIN_FRESHNESS_MS: u64 = 250;
const MAX_FRESHNESS_MS: u64 = 600_000;
const MAX_ACK_TIMEOUT_MS: u64 = 60_000;
const MAX_CONTROL_MESSAGES: usize = 4_096;
const MAX_CONTROL_BYTES: usize = 4 * 1024 * 1024;

const REQUIRED_EVENT_CLASSES: [LiveEventClass; 3] = [
    LiveEventClass::BookSnapshot,
    LiveEventClass::BookDelta,
    LiveEventClass::Trade,
];

/// Explicit Coinbase product binding to one invariant-preserving internal instrument definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseInstrumentMapping {
    product: Box<str>,
    definition: InstrumentDefinition,
}

impl CoinbaseInstrumentMapping {
    /// Returns the exact provider product identifier.
    pub fn product(&self) -> &str {
        &self.product
    }

    /// Returns the validated canonical instrument definition.
    pub const fn definition(&self) -> &InstrumentDefinition {
        &self.definition
    }
}

/// Bounded retained control audit state for one source generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseControlLimits {
    message_capacity: NonZeroUsize,
    byte_capacity: NonZeroUsize,
}

impl CoinbaseControlLimits {
    /// Returns the maximum retained control/audit item count.
    pub const fn message_capacity(self) -> NonZeroUsize {
        self.message_capacity
    }

    /// Returns the maximum retained control/audit bytes.
    pub const fn byte_capacity(self) -> NonZeroUsize {
        self.byte_capacity
    }
}

/// Complete validated Coinbase production source configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseSourceConfig {
    authorization: CoinbaseAuthorizationAttestation,
    instruments: Box<[CoinbaseInstrumentMapping]>,
    freshness: Duration,
    max_frame_bytes: NonZeroUsize,
    subscription_ack_timeout: Duration,
    control_limits: CoinbaseControlLimits,
    subscription_bytes: NonZeroUsize,
}

impl CoinbaseSourceConfig {
    /// Returns the sole permitted production endpoint.
    pub const fn endpoint(&self) -> &'static str {
        COINBASE_EXCHANGE_ENDPOINT
    }

    /// Returns the explicit locally admitted public-interface authorization attestation.
    pub const fn authorization(&self) -> &CoinbaseAuthorizationAttestation {
        &self.authorization
    }

    /// Returns provider products and their explicit internal instrument definitions.
    pub fn instruments(&self) -> &[CoinbaseInstrumentMapping] {
        &self.instruments
    }

    /// Returns the exact live event profile supported by the pinned adapter.
    pub const fn event_classes(&self) -> &[LiveEventClass] {
        &REQUIRED_EVENT_CLASSES
    }

    /// Returns the exact aggregated order-book depth supplied by Coinbase level2.
    pub const fn depth(&self) -> MarketDepth {
        MarketDepth::PriceLevel
    }

    /// Returns the configured market-price freshness threshold.
    pub const fn freshness(&self) -> Duration {
        self.freshness
    }

    /// Returns the exact incoming frame ceiling.
    pub const fn max_frame_bytes(&self) -> NonZeroUsize {
        self.max_frame_bytes
    }

    /// Returns the deadline for the exact subscription acknowledgement.
    pub const fn subscription_ack_timeout(&self) -> Duration {
        self.subscription_ack_timeout
    }

    /// Returns bounded one-generation control state limits.
    pub const fn control_limits(&self) -> CoinbaseControlLimits {
        self.control_limits
    }

    /// Returns the checked encoded subscription request size.
    pub const fn subscription_bytes(&self) -> NonZeroUsize {
        self.subscription_bytes
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoinbaseSourceConfigWire {
    endpoint: String,
    authorization: CoinbaseAuthorizationAttestationWire,
    event_classes: Vec<LiveEventClass>,
    depth: MarketDepth,
    freshness_ms: u64,
    max_frame_bytes: usize,
    subscription_ack_timeout_ms: u64,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    instruments: Vec<CoinbaseInstrumentWire>,
}

/// Explicit user-admitted authorization evidence for the Coinbase public interface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoinbaseAuthorizationAttestation {
    provider: SourceIdentifier,
    basis: AuthorizationBasis,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
}

impl CoinbaseAuthorizationAttestation {
    /// Returns the exact provider namespace authorized by the local attestation.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the user-supplied audited authorization basis.
    pub const fn basis(&self) -> &AuthorizationBasis {
        &self.basis
    }

    /// Returns content-hashed, version-pinned evidence of the reviewed authorization material.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the required finite authorization validity interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective
    }

    /// Returns whether the attestation explicitly covers the supplied instant.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        at >= self.effective.starts_at() && self.effective.ends_at().is_some_and(|until| at < until)
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CoinbaseAuthorizationModeWire {
    PublicInterface,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoinbaseAuthorizationAttestationWire {
    mode: CoinbaseAuthorizationModeWire,
    provider: String,
    basis: String,
    evidence_sha256: String,
    evidence_reference: String,
    evidence_version: String,
    effective_from_unix_nanos: i64,
    effective_until_unix_nanos: i64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoinbaseInstrumentWire {
    product: String,
    instrument_id: InstrumentId,
    definition_revision: InstrumentDefinitionRevision,
    asset_class: AssetClass,
    primary_currency: Option<Currency>,
    primary_asset: Option<InstrumentId>,
    quote_currency: Currency,
    tick_size: TickSize,
    lot_size: LotSize,
    contract_multiplier: Decimal,
    venue: VenueId,
    trading_status: TradingStatus,
}

impl<'de> Deserialize<'de> for CoinbaseSourceConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CoinbaseSourceConfigWire::deserialize(deserializer)?;
        Self::try_from(wire).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<CoinbaseSourceConfigWire> for CoinbaseSourceConfig {
    type Error = CoinbaseConfigurationError;

    fn try_from(wire: CoinbaseSourceConfigWire) -> Result<Self, Self::Error> {
        validate_source_profile(&wire)?;
        let authorization = CoinbaseAuthorizationAttestation::try_from(wire.authorization)?;
        let mut products = BTreeSet::new();
        let mut instrument_ids = BTreeSet::new();
        let mut instruments = Vec::with_capacity(wire.instruments.len());
        for mapping in wire.instruments {
            validate_product(&mapping.product)?;
            if !products.insert(mapping.product.clone()) {
                return Err(CoinbaseConfigurationError::DuplicateProduct);
            }
            if !instrument_ids.insert(mapping.instrument_id) {
                return Err(CoinbaseConfigurationError::DuplicateInstrument);
            }
            instruments.push(mapping.try_into_mapping()?);
        }
        let subscription_bytes = encoded_subscription_bytes(&products)?;
        let freshness = Duration::from_millis(wire.freshness_ms);
        let subscription_ack_timeout = Duration::from_millis(wire.subscription_ack_timeout_ms);
        let max_frame_bytes = NonZeroUsize::new(wire.max_frame_bytes)
            .ok_or(CoinbaseConfigurationError::InvalidFrameLimit)?;
        let control_limits = CoinbaseControlLimits {
            message_capacity: NonZeroUsize::new(wire.control_message_capacity)
                .ok_or(CoinbaseConfigurationError::InvalidControlLimits)?,
            byte_capacity: NonZeroUsize::new(wire.control_byte_capacity)
                .ok_or(CoinbaseConfigurationError::InvalidControlLimits)?,
        };
        Ok(Self {
            authorization,
            instruments: instruments.into_boxed_slice(),
            freshness,
            max_frame_bytes,
            subscription_ack_timeout,
            control_limits,
            subscription_bytes,
        })
    }
}

impl TryFrom<CoinbaseAuthorizationAttestationWire> for CoinbaseAuthorizationAttestation {
    type Error = CoinbaseConfigurationError;

    fn try_from(wire: CoinbaseAuthorizationAttestationWire) -> Result<Self, Self::Error> {
        let CoinbaseAuthorizationModeWire::PublicInterface = wire.mode;
        if wire.provider != COINBASE_PROVIDER {
            return Err(CoinbaseConfigurationError::AuthorizationProviderMismatch);
        }
        let provider = SourceIdentifier::try_from(wire.provider)
            .map_err(|_error| CoinbaseConfigurationError::InvalidAuthorizationAttestation)?;
        let basis = AuthorizationBasis::new(
            SourceIdentifier::try_from(wire.basis)
                .map_err(|_error| CoinbaseConfigurationError::InvalidAuthorizationAttestation)?,
        );
        let content_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            decode_sha256(&wire.evidence_sha256)?,
        );
        let locator = VersionPinnedSourceLocator::new(
            SourceIdentifier::try_from(wire.evidence_reference)
                .map_err(|_error| CoinbaseConfigurationError::InvalidAuthorizationAttestation)?,
            SourceIdentifier::try_from(wire.evidence_version)
                .map_err(|_error| CoinbaseConfigurationError::InvalidAuthorizationAttestation)?,
        );
        let effective = EffectiveInterval::new(
            Timestamp::from_unix_nanos(wire.effective_from_unix_nanos),
            Some(Timestamp::from_unix_nanos(wire.effective_until_unix_nanos)),
        )
        .map_err(|_error| CoinbaseConfigurationError::InvalidAuthorizationAttestation)?;
        Ok(Self {
            provider,
            basis,
            evidence: ExactPayloadEvidence::with_version_pinned_locator(content_digest, locator),
            effective,
        })
    }
}

impl CoinbaseInstrumentWire {
    fn try_into_mapping(self) -> Result<CoinbaseInstrumentMapping, CoinbaseConfigurationError> {
        if self.venue.as_str() != COINBASE_VENUE {
            return Err(CoinbaseConfigurationError::WrongVenue);
        }
        if self.asset_class != AssetClass::Crypto {
            return Err(CoinbaseConfigurationError::UnsupportedAssetClass);
        }
        let primary_denomination = match (self.primary_currency, self.primary_asset) {
            (Some(currency), None) => Denomination::Currency(currency),
            (None, Some(instrument)) => Denomination::Asset(instrument),
            _ => return Err(CoinbaseConfigurationError::InvalidDenomination),
        };
        let venue_symbol = VenueSymbol::try_from(self.product.as_str())
            .map_err(|_error| CoinbaseConfigurationError::InvalidProduct)?;
        let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
            instrument_id: self.instrument_id,
            definition_revision: self.definition_revision,
            asset_class: self.asset_class,
            primary_denomination,
            quote_currency: self.quote_currency,
            tick_size: self.tick_size,
            lot_size: self.lot_size,
            contract_multiplier: self.contract_multiplier,
            venue_mappings: vec![VenueMapping::new(self.venue, venue_symbol)],
            provider_identities: Vec::new(),
            identifiers: Vec::new(),
            trading_status: self.trading_status,
        })
        .map_err(|_error| CoinbaseConfigurationError::InvalidInstrumentDefinition)?;
        Ok(CoinbaseInstrumentMapping {
            product: self.product.into_boxed_str(),
            definition,
        })
    }
}

fn validate_source_profile(
    wire: &CoinbaseSourceConfigWire,
) -> Result<(), CoinbaseConfigurationError> {
    if wire.endpoint != COINBASE_EXCHANGE_ENDPOINT {
        return Err(CoinbaseConfigurationError::InvalidEndpoint);
    }
    if wire.instruments.is_empty() || wire.instruments.len() > MAX_INSTRUMENTS {
        return Err(CoinbaseConfigurationError::InvalidInstrumentCount);
    }
    let event_classes = wire.event_classes.iter().copied().collect::<HashSet<_>>();
    if event_classes != REQUIRED_EVENT_CLASSES.into_iter().collect()
        || wire.event_classes.len() != REQUIRED_EVENT_CLASSES.len()
    {
        return Err(CoinbaseConfigurationError::InvalidEventClasses);
    }
    if wire.depth != MarketDepth::PriceLevel {
        return Err(CoinbaseConfigurationError::InvalidDepth);
    }
    if !(MIN_FRESHNESS_MS..=MAX_FRESHNESS_MS).contains(&wire.freshness_ms) {
        return Err(CoinbaseConfigurationError::InvalidFreshness);
    }
    if wire.max_frame_bytes == 0 || wire.max_frame_bytes > MAX_LIVE_CAPTURE_PAYLOAD_BYTES {
        return Err(CoinbaseConfigurationError::InvalidFrameLimit);
    }
    if wire.subscription_ack_timeout_ms == 0
        || wire.subscription_ack_timeout_ms > MAX_ACK_TIMEOUT_MS
    {
        return Err(CoinbaseConfigurationError::InvalidAcknowledgementTimeout);
    }
    if wire.control_message_capacity == 0
        || wire.control_message_capacity > MAX_CONTROL_MESSAGES
        || wire.control_byte_capacity == 0
        || wire.control_byte_capacity > MAX_CONTROL_BYTES
    {
        return Err(CoinbaseConfigurationError::InvalidControlLimits);
    }
    Ok(())
}

fn validate_product(product: &str) -> Result<(), CoinbaseConfigurationError> {
    if product.is_empty()
        || product.len() > MAX_COINBASE_PRODUCT_BYTES
        || !product
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(CoinbaseConfigurationError::InvalidProduct);
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], CoinbaseConfigurationError> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 {
        return Err(CoinbaseConfigurationError::InvalidAuthorizationAttestation);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(pair[0])?;
        let low = decode_hex_nibble(pair[1])?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

fn decode_hex_nibble(value: u8) -> Result<u8, CoinbaseConfigurationError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(CoinbaseConfigurationError::InvalidAuthorizationAttestation),
    }
}

pub(super) fn parse_environment_profile(value: &str) -> Result<CoinbaseSourceConfig, ConfigError> {
    if value.len() > MAX_ENVIRONMENT_PROFILE_BYTES {
        return Err(ConfigError::InvalidEnvironmentValue);
    }
    serde_json::from_str(value).map_err(|_error| ConfigError::InvalidEnvironmentValue)
}

pub(super) fn validate_product_list(products: &[String]) -> Result<(), ConfigError> {
    if products.is_empty() || products.len() > MAX_LEGACY_PRODUCTS {
        return Err(ConfigError::InvalidProducts);
    }
    let mut unique = BTreeSet::new();
    for product in products {
        if product.is_empty()
            || product.len() > MAX_LEGACY_PRODUCT_BYTES
            || !product
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-._/".contains(character))
            || !unique.insert(product)
        {
            return Err(ConfigError::InvalidProducts);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct Subscription<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    product_ids: Vec<&'a str>,
    channels: [&'static str; 3],
}

fn encoded_subscription_bytes(
    products: &BTreeSet<String>,
) -> Result<NonZeroUsize, CoinbaseConfigurationError> {
    let subscription = Subscription {
        kind: "subscribe",
        product_ids: products.iter().map(String::as_str).collect(),
        channels: ["level2", "matches", "heartbeat"],
    };
    let bytes = serde_json::to_vec(&subscription)
        .map_err(|_error| CoinbaseConfigurationError::SubscriptionSerialization)?
        .len();
    NonZeroUsize::new(bytes)
        .filter(|size| size.get() <= MAX_SUBSCRIPTION_BYTES)
        .ok_or(CoinbaseConfigurationError::SubscriptionTooLarge)
}

/// Strict Coinbase configuration failure without rendering source input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseConfigurationError {
    #[error("Coinbase production endpoint is invalid")]
    InvalidEndpoint,
    #[error("Coinbase authorization attestation is invalid or unbounded")]
    InvalidAuthorizationAttestation,
    #[error("Coinbase authorization attestation names another provider")]
    AuthorizationProviderMismatch,
    #[error("Coinbase instrument mapping count is invalid")]
    InvalidInstrumentCount,
    #[error("Coinbase product identifier is invalid")]
    InvalidProduct,
    #[error("Coinbase product mapping is duplicated")]
    DuplicateProduct,
    #[error("Coinbase internal instrument mapping is duplicated")]
    DuplicateInstrument,
    #[error("Coinbase instrument venue is invalid")]
    WrongVenue,
    #[error("Coinbase instrument asset class is unsupported")]
    UnsupportedAssetClass,
    #[error("Coinbase instrument denomination is invalid")]
    InvalidDenomination,
    #[error("Coinbase instrument definition is invalid")]
    InvalidInstrumentDefinition,
    #[error("Coinbase event-class profile is invalid")]
    InvalidEventClasses,
    #[error("Coinbase market depth is invalid")]
    InvalidDepth,
    #[error("Coinbase freshness limit is invalid")]
    InvalidFreshness,
    #[error("Coinbase frame limit is invalid")]
    InvalidFrameLimit,
    #[error("Coinbase subscription acknowledgement timeout is invalid")]
    InvalidAcknowledgementTimeout,
    #[error("Coinbase control-state limits are invalid")]
    InvalidControlLimits,
    #[error("Coinbase subscription serialization failed")]
    SubscriptionSerialization,
    #[error("Coinbase subscription exceeds its byte ceiling")]
    SubscriptionTooLarge,
}

impl fmt::Display for CoinbaseInstrumentMapping {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}",
            self.product,
            self.definition.instrument_id()
        )
    }
}
