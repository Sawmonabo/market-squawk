//! Strict production instrument and subscription configuration.

use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    num::NonZeroUsize,
    str::FromStr,
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

/// Pinned public Coinbase Advanced Trade market-data endpoint accepted by production configuration.
pub const COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT: &str =
    "wss://advanced-trade-ws.coinbase.com";
/// Pinned public Kraken Spot WebSocket v2 endpoint accepted by production configuration.
pub const KRAKEN_WEBSOCKET_V2_ENDPOINT: &str = "wss://ws.kraken.com/v2";

const COINBASE_VENUE: &str = "coinbase-exchange";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const KRAKEN_VENUE: &str = "kraken";
const KRAKEN_PROVIDER: &str = "kraken";
const MAX_INSTRUMENTS: usize = 1;
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
const RECOMMENDED_PROFILE_EFFECTIVE_FROM_UNIX_NANOS: i64 = 1_784_779_200_000_000_000;
const RECOMMENDED_PROFILE_EFFECTIVE_UNTIL_UNIX_NANOS: i64 = 1_816_315_200_000_000_000;
const RECOMMENDED_INSTRUMENT_ID: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";
const RECOMMENDED_PRIMARY_ASSET_ID: &str = "b9f6d14f-9140-4ca3-a412-9bd59b3b5e67";
const COINBASE_REVIEW_EVIDENCE_SHA256: &str =
    "18e2c5d1c52a32b3bf734415a579ec99aea8ef2cb8d3c34a38f4fea577ab73bb";
const KRAKEN_PUBLIC_FEED_REVIEW_EVIDENCE_SHA256: &str =
    "10ad4be02cbc6d2047e67e4703a604c104991d10a9f0ece531e070309715cbe7";

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
        COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT
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

pub(super) fn recommended_coinbase_public_config()
-> Result<CoinbaseSourceConfig, CoinbaseConfigurationError> {
    let wire = format!(
        r#"{{
          "endpoint":"{COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT}",
          "event_classes":["book_snapshot","book_delta","trade"],
          "depth":"price_level",
          "freshness_ms":5000,
          "max_frame_bytes":16777216,
          "subscription_ack_timeout_ms":5000,
          "control_message_capacity":64,
          "control_byte_capacity":65536,
          "authorization":{{
            "mode":"public_interface",
            "provider":"coinbase-exchange",
            "basis":"market-squawk-reviewed-coinbase-public-interface",
            "evidence_sha256":"{COINBASE_REVIEW_EVIDENCE_SHA256}",
            "evidence_reference":"https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview",
            "evidence_version":"reviewed-2026-08-08",
            "effective_from_unix_nanos":{RECOMMENDED_PROFILE_EFFECTIVE_FROM_UNIX_NANOS},
            "effective_until_unix_nanos":{RECOMMENDED_PROFILE_EFFECTIVE_UNTIL_UNIX_NANOS}
          }},
          "instruments":[{{
            "product":"BTC-USD",
            "instrument_id":"{RECOMMENDED_INSTRUMENT_ID}",
            "definition_revision":1,
            "asset_class":"crypto",
            "primary_asset":"{RECOMMENDED_PRIMARY_ASSET_ID}",
            "quote_currency":"USD",
            "tick_size":"0.01",
            "lot_size":"0.00000001",
            "contract_multiplier":"1",
            "venue":"coinbase-exchange",
            "trading_status":"active"
          }}]
        }}"#
    );
    let mut config: CoinbaseSourceConfig = serde_json::from_str(&wire)
        .map_err(|_error| CoinbaseConfigurationError::InvalidEmbeddedRecommendedProfile)?;
    let definition = recommended_public_btc_usd_definition()
        .map_err(|_error| CoinbaseConfigurationError::InvalidEmbeddedRecommendedProfile)?;
    let mapping = config
        .instruments
        .first_mut()
        .ok_or(CoinbaseConfigurationError::InvalidEmbeddedRecommendedProfile)?;
    mapping.definition = definition;
    Ok(config)
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

/// Explicit locally admitted authorization evidence for the Coinbase public interface.
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

    /// Returns the locally admitted audited authorization basis.
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
    if wire.endpoint != COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT {
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

/// Explicit Kraken symbol binding to one invariant-preserving internal instrument definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenInstrumentMapping {
    symbol: Box<str>,
    definition: InstrumentDefinition,
}

impl KrakenInstrumentMapping {
    /// Returns the exact Kraken v2 symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the validated canonical instrument definition.
    pub const fn definition(&self) -> &InstrumentDefinition {
        &self.definition
    }
}

/// Explicit local authorization evidence for the Kraken public interface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct KrakenAuthorizationAttestation {
    provider: SourceIdentifier,
    basis: AuthorizationBasis,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
}

impl KrakenAuthorizationAttestation {
    /// Returns the exact provider namespace admitted by this attestation.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the reviewed authorization basis.
    pub const fn basis(&self) -> &AuthorizationBasis {
        &self.basis
    }

    /// Returns the content-hashed, version-pinned authorization evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the finite authorization validity interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective
    }

    /// Returns whether this attestation covers the supplied instant.
    pub fn is_effective_at(&self, at: Timestamp) -> bool {
        at >= self.effective.starts_at() && self.effective.ends_at().is_some_and(|end| at < end)
    }
}

/// Complete validated Kraken public book-and-trade production configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenSourceConfig {
    authorization: KrakenAuthorizationAttestation,
    instrument: KrakenInstrumentMapping,
    freshness: Duration,
    max_frame_bytes: NonZeroUsize,
    subscription_ack_timeout: Duration,
    control_limits: CoinbaseControlLimits,
}

impl KrakenSourceConfig {
    /// Returns the sole permitted production endpoint.
    pub const fn endpoint(&self) -> &'static str {
        KRAKEN_WEBSOCKET_V2_ENDPOINT
    }

    /// Returns explicit locally admitted public-interface authorization evidence.
    pub const fn authorization(&self) -> &KrakenAuthorizationAttestation {
        &self.authorization
    }

    /// Returns the exact Kraken v2 symbol.
    pub fn symbol(&self) -> &str {
        self.instrument.symbol()
    }

    /// Returns the validated canonical instrument definition.
    pub const fn definition(&self) -> &InstrumentDefinition {
        self.instrument.definition()
    }

    /// Returns the only checksum scope currently admitted by the reviewed adapter policy.
    pub const fn depth(&self) -> usize {
        10
    }

    /// Returns the exact public event profile authorized by the composite configuration.
    pub const fn event_classes(&self) -> &[LiveEventClass] {
        &REQUIRED_EVENT_CLASSES
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
}

pub(super) fn recommended_kraken_public_config()
-> Result<KrakenSourceConfig, KrakenConfigurationError> {
    let wire = format!(
        r#"{{
          "endpoint":"{KRAKEN_WEBSOCKET_V2_ENDPOINT}",
          "channels":["book","trade"],
          "depth":10,
          "freshness_ms":5000,
          "max_frame_bytes":1048576,
          "subscription_ack_timeout_ms":5000,
          "control_message_capacity":64,
          "control_byte_capacity":65536,
          "authorization":{{
            "mode":"public_interface",
            "provider":"kraken",
            "basis":"market-squawk-reviewed-kraken-public-interface",
            "evidence_sha256":"{KRAKEN_PUBLIC_FEED_REVIEW_EVIDENCE_SHA256}",
            "evidence_reference":"https://github.com/Sawmonabo/market-squawk/blob/main/docs/research/2026-07-16-kraken-websocket-v2-checksum.md",
            "evidence_version":"reviewed-2026-08-14",
            "effective_from_unix_nanos":{RECOMMENDED_PROFILE_EFFECTIVE_FROM_UNIX_NANOS},
            "effective_until_unix_nanos":{RECOMMENDED_PROFILE_EFFECTIVE_UNTIL_UNIX_NANOS}
          }},
          "instrument":{{
            "symbol":"BTC/USD",
            "instrument_id":"{RECOMMENDED_INSTRUMENT_ID}",
            "definition_revision":1,
            "asset_class":"crypto",
            "primary_asset":"{RECOMMENDED_PRIMARY_ASSET_ID}",
            "quote_currency":"USD",
            "tick_size":"0.01",
            "lot_size":"0.00000001",
            "contract_multiplier":"1",
            "venue":"kraken",
            "trading_status":"active"
          }}
        }}"#
    );
    let mut config: KrakenSourceConfig = serde_json::from_str(&wire)
        .map_err(|_error| KrakenConfigurationError::InvalidEmbeddedRecommendedProfile)?;
    config.instrument.definition = recommended_public_btc_usd_definition()
        .map_err(|_error| KrakenConfigurationError::InvalidEmbeddedRecommendedProfile)?;
    Ok(config)
}

#[derive(Clone, Copy, Debug)]
struct EmbeddedRecommendedProfileError;

fn recommended_public_btc_usd_definition()
-> Result<InstrumentDefinition, EmbeddedRecommendedProfileError> {
    let instrument_id =
        InstrumentId::from_str(RECOMMENDED_INSTRUMENT_ID).map_err(invalid_recommended_profile)?;
    let primary_asset = InstrumentId::from_str(RECOMMENDED_PRIMARY_ASSET_ID)
        .map_err(invalid_recommended_profile)?;
    let coinbase_venue = VenueId::try_from(COINBASE_VENUE).map_err(invalid_recommended_profile)?;
    let kraken_venue = VenueId::try_from(KRAKEN_VENUE).map_err(invalid_recommended_profile)?;
    InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id,
        definition_revision: InstrumentDefinitionRevision::try_from(1_u64)
            .map_err(invalid_recommended_profile)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Asset(primary_asset),
        quote_currency: Currency::try_from("USD").map_err(invalid_recommended_profile)?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))
            .map_err(invalid_recommended_profile)?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 8))
            .map_err(invalid_recommended_profile)?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![
            VenueMapping::new(
                coinbase_venue,
                VenueSymbol::try_from("BTC-USD").map_err(invalid_recommended_profile)?,
            ),
            VenueMapping::new(
                kraken_venue,
                VenueSymbol::try_from("BTC/USD").map_err(invalid_recommended_profile)?,
            ),
        ],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })
    .map_err(invalid_recommended_profile)
}

fn invalid_recommended_profile<T>(_error: T) -> EmbeddedRecommendedProfileError {
    EmbeddedRecommendedProfileError
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum KrakenChannelWire {
    Book,
    Trade,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KrakenSourceConfigWire {
    endpoint: String,
    authorization: KrakenAuthorizationAttestationWire,
    channels: [KrakenChannelWire; 2],
    depth: usize,
    freshness_ms: u64,
    max_frame_bytes: usize,
    subscription_ack_timeout_ms: u64,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    instrument: KrakenInstrumentWire,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KrakenAuthorizationAttestationWire {
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
struct KrakenInstrumentWire {
    symbol: String,
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

impl<'de> Deserialize<'de> for KrakenSourceConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = KrakenSourceConfigWire::deserialize(deserializer)?;
        Self::try_from(wire).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<KrakenSourceConfigWire> for KrakenSourceConfig {
    type Error = KrakenConfigurationError;

    fn try_from(wire: KrakenSourceConfigWire) -> Result<Self, Self::Error> {
        if wire.endpoint != KRAKEN_WEBSOCKET_V2_ENDPOINT
            || wire.channels != [KrakenChannelWire::Book, KrakenChannelWire::Trade]
            || wire.depth != 10
        {
            return Err(KrakenConfigurationError::InvalidProtocolProfile);
        }
        if !(MIN_FRESHNESS_MS..=MAX_FRESHNESS_MS).contains(&wire.freshness_ms) {
            return Err(KrakenConfigurationError::InvalidFreshness);
        }
        let max_frame_bytes = NonZeroUsize::new(wire.max_frame_bytes)
            .filter(|bound| bound.get() <= MAX_LIVE_CAPTURE_PAYLOAD_BYTES)
            .ok_or(KrakenConfigurationError::InvalidFrameLimit)?;
        if wire.subscription_ack_timeout_ms == 0
            || wire.subscription_ack_timeout_ms > MAX_ACK_TIMEOUT_MS
        {
            return Err(KrakenConfigurationError::InvalidAcknowledgementTimeout);
        }
        let control_limits = CoinbaseControlLimits {
            message_capacity: NonZeroUsize::new(wire.control_message_capacity)
                .filter(|bound| bound.get() <= MAX_CONTROL_MESSAGES)
                .ok_or(KrakenConfigurationError::InvalidControlLimits)?,
            byte_capacity: NonZeroUsize::new(wire.control_byte_capacity)
                .filter(|bound| bound.get() <= MAX_CONTROL_BYTES)
                .ok_or(KrakenConfigurationError::InvalidControlLimits)?,
        };
        Ok(Self {
            authorization: wire.authorization.try_into()?,
            instrument: wire.instrument.try_into()?,
            freshness: Duration::from_millis(wire.freshness_ms),
            max_frame_bytes,
            subscription_ack_timeout: Duration::from_millis(wire.subscription_ack_timeout_ms),
            control_limits,
        })
    }
}

impl TryFrom<KrakenAuthorizationAttestationWire> for KrakenAuthorizationAttestation {
    type Error = KrakenConfigurationError;

    fn try_from(wire: KrakenAuthorizationAttestationWire) -> Result<Self, Self::Error> {
        let CoinbaseAuthorizationModeWire::PublicInterface = wire.mode;
        if wire.provider != KRAKEN_PROVIDER {
            return Err(KrakenConfigurationError::AuthorizationProviderMismatch);
        }
        let identifier = |value: String| {
            SourceIdentifier::try_from(value)
                .map_err(|_error| KrakenConfigurationError::InvalidAuthorizationAttestation)
        };
        let digest = decode_kraken_sha256(&wire.evidence_sha256)?;
        let effective = EffectiveInterval::new(
            Timestamp::from_unix_nanos(wire.effective_from_unix_nanos),
            Some(Timestamp::from_unix_nanos(wire.effective_until_unix_nanos)),
        )
        .map_err(|_error| KrakenConfigurationError::InvalidAuthorizationAttestation)?;
        Ok(Self {
            provider: identifier(wire.provider)?,
            basis: AuthorizationBasis::new(identifier(wire.basis)?),
            evidence: ExactPayloadEvidence::with_version_pinned_locator(
                EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
                VersionPinnedSourceLocator::new(
                    identifier(wire.evidence_reference)?,
                    identifier(wire.evidence_version)?,
                ),
            ),
            effective,
        })
    }
}

impl TryFrom<KrakenInstrumentWire> for KrakenInstrumentMapping {
    type Error = KrakenConfigurationError;

    fn try_from(wire: KrakenInstrumentWire) -> Result<Self, Self::Error> {
        if wire.symbol.is_empty()
            || wire.symbol.len() > MAX_COINBASE_PRODUCT_BYTES
            || !wire.symbol.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.')
            })
        {
            return Err(KrakenConfigurationError::InvalidSymbol);
        }
        if wire.venue.as_str() != KRAKEN_VENUE || wire.asset_class != AssetClass::Crypto {
            return Err(KrakenConfigurationError::InvalidInstrumentDefinition);
        }
        let primary_denomination = match (wire.primary_currency, wire.primary_asset) {
            (Some(currency), None) => Denomination::Currency(currency),
            (None, Some(instrument)) => Denomination::Asset(instrument),
            _ => return Err(KrakenConfigurationError::InvalidInstrumentDefinition),
        };
        let venue_symbol = VenueSymbol::try_from(wire.symbol.as_str())
            .map_err(|_error| KrakenConfigurationError::InvalidSymbol)?;
        let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
            instrument_id: wire.instrument_id,
            definition_revision: wire.definition_revision,
            asset_class: wire.asset_class,
            primary_denomination,
            quote_currency: wire.quote_currency,
            tick_size: wire.tick_size,
            lot_size: wire.lot_size,
            contract_multiplier: wire.contract_multiplier,
            venue_mappings: vec![VenueMapping::new(wire.venue, venue_symbol)],
            provider_identities: Vec::new(),
            identifiers: Vec::new(),
            trading_status: wire.trading_status,
        })
        .map_err(|_error| KrakenConfigurationError::InvalidInstrumentDefinition)?;
        Ok(Self {
            symbol: wire.symbol.into_boxed_str(),
            definition,
        })
    }
}

fn decode_kraken_sha256(value: &str) -> Result<[u8; 32], KrakenConfigurationError> {
    let bytes = value.as_bytes();
    if bytes.len() != 64 {
        return Err(KrakenConfigurationError::InvalidAuthorizationAttestation);
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        let nibble = |value: u8| match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            b'A'..=b'F' => Some(value - b'A' + 10),
            _ => None,
        };
        let high =
            nibble(pair[0]).ok_or(KrakenConfigurationError::InvalidAuthorizationAttestation)?;
        let low =
            nibble(pair[1]).ok_or(KrakenConfigurationError::InvalidAuthorizationAttestation)?;
        digest[index] = (high << 4) | low;
    }
    Ok(digest)
}

pub(super) fn parse_kraken_environment_profile(
    value: &str,
) -> Result<KrakenSourceConfig, ConfigError> {
    if value.len() > MAX_ENVIRONMENT_PROFILE_BYTES {
        return Err(ConfigError::InvalidEnvironmentValue);
    }
    serde_json::from_str(value).map_err(|_error| ConfigError::InvalidEnvironmentValue)
}

/// Strict Kraken configuration failure without rendering source input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenConfigurationError {
    #[error("embedded recommended Kraken public profile is invalid")]
    InvalidEmbeddedRecommendedProfile,
    #[error("Kraken production protocol profile is invalid")]
    InvalidProtocolProfile,
    #[error("Kraken authorization attestation is invalid or unbounded")]
    InvalidAuthorizationAttestation,
    #[error("Kraken authorization attestation names another provider")]
    AuthorizationProviderMismatch,
    #[error("Kraken symbol is invalid")]
    InvalidSymbol,
    #[error("Kraken instrument definition is invalid")]
    InvalidInstrumentDefinition,
    #[error("Kraken freshness limit is invalid")]
    InvalidFreshness,
    #[error("Kraken frame limit is invalid")]
    InvalidFrameLimit,
    #[error("Kraken subscription acknowledgement timeout is invalid")]
    InvalidAcknowledgementTimeout,
    #[error("Kraken control-state limits are invalid")]
    InvalidControlLimits,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    product_ids: Option<Vec<&'a str>>,
    channel: &'static str,
}

fn encoded_subscription_bytes(
    products: &BTreeSet<String>,
) -> Result<NonZeroUsize, CoinbaseConfigurationError> {
    let product_ids = products.iter().map(String::as_str).collect::<Vec<_>>();
    let mut bytes = 0_usize;
    for (channel, carries_products) in [
        ("level2", true),
        ("market_trades", true),
        ("heartbeats", false),
    ] {
        let subscription = Subscription {
            kind: "subscribe",
            product_ids: if carries_products {
                Some(product_ids.clone())
            } else {
                None
            },
            channel,
        };
        bytes = bytes
            .checked_add(
                serde_json::to_vec(&subscription)
                    .map_err(|_error| CoinbaseConfigurationError::SubscriptionSerialization)?
                    .len(),
            )
            .ok_or(CoinbaseConfigurationError::SubscriptionTooLarge)?;
    }
    NonZeroUsize::new(bytes)
        .filter(|size| size.get() <= MAX_SUBSCRIPTION_BYTES)
        .ok_or(CoinbaseConfigurationError::SubscriptionTooLarge)
}

/// Strict Coinbase configuration failure without rendering source input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseConfigurationError {
    #[error("embedded recommended Coinbase public profile is invalid")]
    InvalidEmbeddedRecommendedProfile,
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
