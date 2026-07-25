//! Authenticated Coinbase Exchange Direct Market Data profile and level-3 decoders.

use std::fmt;
use std::num::NonZeroU64;

use chrono::DateTime;
use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentExecutionTerms, IntegrityRule,
    LiveEventClass, MarketDepth, PriceTicks, ProviderChannel, ProviderProduct, QuantityLots,
    RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability, SequenceNumber,
    SequenceValidationRule, SnapshotApplicability, SourceId, SourceIdentifier, Timestamp,
    TradingStatus, VenueId,
};
use market_squawk_live::{
    DirectBookLimits, DirectOrderBook, DirectOrderBookError, normalize_delta_quantity,
    normalize_positive_quantity, normalize_price,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile,
    CoverageTopology, DecoderEvidence, EndpointPolicy, FreshnessPolicy, HistoricalCapability,
    HttpCaptureMethod, HttpRequestBounds, InstrumentCoverage, LiveCoverageDeclaration,
    LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy, PathScope, ProviderBookSide,
    ProviderBudgetPolicy, ProviderCursorOnlyReason, ProviderDecimalLexeme, ProviderNumericPolicy,
    ProviderOrderChangeReason, ProviderOrderEvent, ProviderOrderEventKind, ProviderOrderRecord,
    ProviderPrice, ProviderQuantity, QueryParameterRule, SegmentedHttpResponseCapture,
    SegmentedHttpResponseReceipt, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile, TransportFrameKind, ValidatedRawMarketFrame,
};
use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use zeroize::Zeroizing;

use crate::{CoinbaseConfigError, CoinbaseProductMapping, CoinbaseTransportLimits};

/// Authenticated Direct Market Data WebSocket endpoint.
pub const COINBASE_DIRECT_WEBSOCKET_ENDPOINT: &str = "wss://ws-direct.exchange.coinbase.com";
const COINBASE_REST_ORIGIN: &str = "https://api.exchange.coinbase.com";
const COINBASE_VENUE: &str = "coinbase-exchange";
const COINBASE_PROVIDER: &str = "coinbase-exchange";
const DIRECT_CHANNEL: &str = "full";
const WEBSOCKET_AUTH_PATH: &str = "/users/self/verify";
const MAX_SIGNING_FIELD_BYTES: usize = 1_024;
const MAX_SIGNED_SUBSCRIPTION_BYTES: usize = 16 * 1024;
const MAX_DIRECT_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DIRECT_SNAPSHOT_SEGMENTS: usize = 64;
const MIN_DIRECT_CONCURRENT_REQUESTS: u16 = 2;
const MIN_DIRECT_BOOTSTRAP_REQUESTS_PER_WINDOW: u32 = 3;

/// Complete transport, snapshot, queue, and level-3 ownership limits for one product generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectLimits {
    websocket: CoinbaseTransportLimits,
    max_snapshot_bytes: u64,
    max_snapshot_segments: usize,
    book: DirectBookLimits,
}

impl CoinbaseDirectLimits {
    /// Constructs bounded direct-feed limits.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive snapshot limits or a byte limit impossible under the segment count.
    pub fn try_new(
        websocket: CoinbaseTransportLimits,
        max_snapshot_bytes: u64,
        max_snapshot_segments: usize,
        book: DirectBookLimits,
    ) -> Result<Self, CoinbaseConfigError> {
        let segment_capacity = max_snapshot_segments
            .checked_mul(market_squawk_sources::MAX_RAW_FRAME_BYTES)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(CoinbaseConfigError::InvalidDirectLimits)?;
        if max_snapshot_bytes == 0
            || max_snapshot_bytes > MAX_DIRECT_SNAPSHOT_BYTES
            || max_snapshot_segments == 0
            || max_snapshot_segments > MAX_DIRECT_SNAPSHOT_SEGMENTS
            || max_snapshot_bytes > segment_capacity
        {
            return Err(CoinbaseConfigError::InvalidDirectLimits);
        }
        Ok(Self {
            websocket,
            max_snapshot_bytes,
            max_snapshot_segments,
            book,
        })
    }

    /// Returns the bounded WebSocket transport profile.
    pub const fn websocket(self) -> CoinbaseTransportLimits {
        self.websocket
    }

    /// Returns the complete HTTP snapshot byte ceiling.
    pub const fn max_snapshot_bytes(self) -> u64 {
        self.max_snapshot_bytes
    }

    /// Returns the maximum number of exact snapshot capture segments.
    pub const fn max_snapshot_segments(self) -> usize {
        self.max_snapshot_segments
    }

    /// Returns the instrument-owned order-map, replay, and publication limits.
    pub const fn book(self) -> DirectBookLimits {
        self.book
    }
}

/// Immutable metadata and endpoint profile for one product per Direct connection.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectConfig {
    metadata: SourceMetadata,
    mapping: CoinbaseProductMapping,
    terms: InstrumentExecutionTerms,
    limits: CoinbaseDirectLimits,
    snapshot_url: Box<str>,
    product_url: Box<str>,
}

impl CoinbaseDirectConfig {
    /// Builds a distinct authenticated `ws-direct`/`full` plus REST level-3 profile.
    ///
    /// `DirectVerified` is only a metadata ceiling. This constructor cannot create current
    /// authorization, capture, snapshot consistency, status, precision, or healthy-stream
    /// evidence and therefore cannot mint execution authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "authorization, coverage, budget, and every runtime bound remain explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        mapping: CoinbaseProductMapping,
        terms: InstrumentExecutionTerms,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: CoinbaseDirectLimits,
    ) -> Result<Self, CoinbaseConfigError> {
        if authorization.mode() != AuthorizationMode::UserAuthorized {
            return Err(CoinbaseConfigError::InvalidDirectAuthorization);
        }
        if terms.instrument_id() != mapping.instrument() {
            return Err(CoinbaseConfigError::InvalidDirectInstrumentTerms);
        }
        validate_direct_budget(&budget)?;
        let product = mapping.product().as_source_identifier().as_str();
        let snapshot_base = format!("{COINBASE_REST_ORIGIN}/products/{product}/book");
        let snapshot_url = format!("{snapshot_base}?level=3");
        let product_url = format!("{COINBASE_REST_ORIGIN}/products/{product}");
        let request_bounds = direct_request_bounds(limits)?;
        let level_rule = QueryParameterRule::try_new_exact_public(
            SourceIdentifier::try_from("level")?,
            SourceIdentifier::try_from("3")?,
        )?;
        let snapshot_rule =
            ApiEndpointRule::try_new(&snapshot_base, PathScope::Exact, vec![level_rule], 1, 7)?;
        let product_rule =
            ApiEndpointRule::try_new(&product_url, PathScope::Exact, Vec::new(), 1, 1)?;
        let endpoints = EndpointPolicy::try_new_combined(
            [COINBASE_DIRECT_WEBSOCKET_ENDPOINT],
            vec![snapshot_rule, product_rule],
            request_bounds,
        )?;
        endpoints.authorize(&snapshot_url)?;
        endpoints.authorize(&product_url)?;

        let decoder_rule = direct_rule("coinbase-exchange-direct-full-v1-decoder")?;
        let timestamp_rule = direct_rule("coinbase-exchange-direct-rfc3339-time")?;
        let sequence_rule = direct_rule("coinbase-exchange-direct-product-sequence")?;
        let checksum_rule = direct_rule("coinbase-exchange-direct-checksum-unsupported")?;
        let live = LiveCoverageDeclaration::try_new(
            mapping.product().clone(),
            ProviderChannel::new(SourceIdentifier::try_from(DIRECT_CHANNEL)?),
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
            ],
        )?;
        let coverage = SourceCoverage::try_instrument(
            coverage_evidence,
            effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(VenueId::try_from(COINBASE_VENUE)?),
            InstrumentCoverage::enumerated(vec![mapping.instrument()])?,
            Some(live),
            CoverageDelay::RealTime,
            DeliveryEvidence::DirectVenue,
        )?;
        let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id,
            revision_evidence,
            SourceClass::Exchange,
            SourceIdentifier::try_from(COINBASE_PROVIDER)?,
            authorization,
            coverage,
            DataQuality::DirectVerified,
            NetworkAccessPolicy::Allowlisted(endpoints),
            freshness,
            Some(budget),
            SourceCapabilities::new(
                true,
                false,
                SequenceCapability::Provided,
                ChecksumCapability::Unsupported,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
                decoder_rule,
                SemanticInterpretationProfile::new(
                    direct_rule("coinbase-exchange-direct-maker-side")?,
                    direct_rule("coinbase-exchange-direct-auction-mode-v1")?,
                    direct_rule("coinbase-exchange-direct-product-status")?,
                    direct_rule("coinbase-exchange-direct-corporate-action-unused")?,
                ),
                timestamp_rule,
                SequenceValidationProfile::Provided {
                    rule: sequence_rule,
                    progression: SequenceValidationRule::Consecutive,
                },
                ChecksumValidationProfile::Unsupported {
                    rule: checksum_rule,
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ))),
        ))?;
        Ok(Self {
            metadata,
            mapping,
            terms,
            limits,
            snapshot_url: snapshot_url.into_boxed_str(),
            product_url: product_url.into_boxed_str(),
        })
    }

    /// Returns immutable metadata. It remains a ceiling declaration, not current authority.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the authenticated Direct WebSocket endpoint.
    pub const fn websocket_endpoint(&self) -> &'static str {
        COINBASE_DIRECT_WEBSOCKET_ENDPOINT
    }

    /// Returns the exact level-3 snapshot URL.
    pub fn snapshot_url(&self) -> &str {
        &self.snapshot_url
    }

    /// Returns the exact current-product evidence URL.
    pub fn product_url(&self) -> &str {
        &self.product_url
    }

    /// Returns the sole product on this bounded connection.
    pub const fn product(&self) -> &ProviderProduct {
        self.mapping.product()
    }

    /// Returns the stable mapped instrument.
    pub const fn instrument(&self) -> market_squawk_domain::InstrumentId {
        self.mapping.instrument()
    }

    /// Returns the immutable instrument terms used for exact Direct normalization.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.terms
    }

    /// Returns all direct transport and ownership limits.
    pub const fn limits(&self) -> CoinbaseDirectLimits {
        self.limits
    }

    /// Constructs one redacted authenticated `full` subscription.
    pub fn try_signed_subscription(
        &self,
        unix_seconds: u64,
        signer: &dyn CoinbaseDirectSigningCapability,
    ) -> Result<CoinbaseSignedSubscription, CoinbaseDirectSigningError> {
        if unix_seconds == 0 {
            return Err(CoinbaseDirectSigningError::InvalidTimestamp);
        }
        let timestamp = unix_seconds.to_string();
        let request = CoinbaseDirectSigningRequest {
            timestamp: &timestamp,
        };
        let authentication = signer.sign(request)?;
        let wire = SignedSubscriptionWire {
            kind: "subscribe",
            product_ids: [self.product().as_source_identifier().as_str()],
            channels: [DIRECT_CHANNEL],
            signature: authentication.signature(),
            key: authentication.key(),
            passphrase: authentication.passphrase(),
            timestamp: &timestamp,
        };
        let payload = Zeroizing::new(
            serde_json::to_string(&wire).map_err(|_| CoinbaseDirectSigningError::Serialization)?,
        );
        if payload.len() > MAX_SIGNED_SUBSCRIPTION_BYTES {
            return Err(CoinbaseDirectSigningError::SubscriptionTooLarge);
        }
        Ok(CoinbaseSignedSubscription(payload))
    }

    /// Decodes current product status and increments from an exact captured REST response.
    pub fn decode_product_evidence(
        &self,
        capture: &SegmentedHttpResponseCapture,
    ) -> Result<CoinbaseDirectProductEvidence, CoinbaseDirectProductError> {
        validate_http_capture(
            capture,
            self.product_url(),
            self.metadata.source_id(),
            self.metadata.revision(),
            self.limits.max_snapshot_bytes,
            self.limits.max_snapshot_segments,
        )
        .map_err(CoinbaseDirectProductError::Capture)?;
        let wire: ProductWire = serde_json::from_reader(capture.reader())
            .map_err(|_| CoinbaseDirectProductError::Schema)?;
        if wire.id != self.product().as_source_identifier().as_str() {
            return Err(CoinbaseDirectProductError::WrongProduct);
        }
        let base_increment = parse_direct_quantity(&wire.base_increment)
            .map_err(|_| CoinbaseDirectProductError::Increment)?;
        let quote_increment = parse_direct_quantity(&wire.quote_increment)
            .map_err(|_| CoinbaseDirectProductError::Increment)?;
        if base_increment.value().decimal() != self.terms.lot_size().as_decimal()
            || quote_increment.value().decimal() != self.terms.price_tick().as_decimal()
        {
            return Err(CoinbaseDirectProductError::Increment);
        }
        let status = SourceIdentifier::try_from(wire.status.as_str())
            .map_err(|_| CoinbaseDirectProductError::Status)?;
        let trading_status = if wire.status == "online"
            && !wire.trading_disabled
            && !wire.cancel_only
            && !wire.post_only
            && !wire.limit_only
            && !wire.auction_mode
        {
            TradingStatus::Active
        } else if wire.status == "delisted" {
            TradingStatus::Delisted
        } else {
            TradingStatus::Inactive
        };
        Ok(CoinbaseDirectProductEvidence {
            product: self.product().clone(),
            provider_status: status,
            trading_status,
            base_increment,
            quote_increment,
            trading_disabled: wire.trading_disabled,
            cancel_only: wire.cancel_only,
            post_only: wire.post_only,
            limit_only: wire.limit_only,
            auction_mode: wire.auction_mode,
            capture: capture.receipt().clone(),
        })
    }
}

fn validate_direct_budget(budget: &ProviderBudgetPolicy) -> Result<(), CoinbaseConfigError> {
    if budget.max_concurrent() < MIN_DIRECT_CONCURRENT_REQUESTS {
        return Err(CoinbaseConfigError::InvalidDirectBudget);
    }
    for index in 0..budget.window_count() {
        let window = budget
            .window(index)
            .ok_or(CoinbaseConfigError::InvalidDirectBudget)?;
        if window.requests_per_window() < MIN_DIRECT_BOOTSTRAP_REQUESTS_PER_WINDOW {
            return Err(CoinbaseConfigError::InvalidDirectBudget);
        }
    }
    Ok(())
}

fn direct_request_bounds(
    limits: CoinbaseDirectLimits,
) -> Result<HttpRequestBounds, CoinbaseConfigError> {
    let websocket = limits.websocket();
    let connect = u64::try_from(websocket.connect_timeout().as_nanos())
        .map_err(|_| CoinbaseConfigError::InvalidDirectLimits)?;
    let read = u64::try_from(websocket.io_timeout().as_nanos())
        .map_err(|_| CoinbaseConfigError::InvalidDirectLimits)?;
    let total = connect
        .checked_add(read)
        .ok_or(CoinbaseConfigError::InvalidDirectLimits)?;
    Ok(HttpRequestBounds::try_new(
        NonZeroU64::new(connect).ok_or(CoinbaseConfigError::InvalidDirectLimits)?,
        NonZeroU64::new(read).ok_or(CoinbaseConfigError::InvalidDirectLimits)?,
        NonZeroU64::new(total).ok_or(CoinbaseConfigError::InvalidDirectLimits)?,
        0,
        NonZeroU64::new(limits.max_snapshot_bytes)
            .ok_or(CoinbaseConfigError::InvalidDirectLimits)?,
    )?)
}

fn direct_rule(value: &str) -> Result<IntegrityRule, CoinbaseConfigError> {
    Ok(IntegrityRule::new(
        SourceIdentifier::try_from(value)?,
        RuleVersion::new(1).map_err(|_| CoinbaseConfigError::InvalidRule)?,
    ))
}

/// Exact prehash coordinates presented to a local signing capability.
#[derive(Clone, Copy, Debug)]
pub struct CoinbaseDirectSigningRequest<'a> {
    timestamp: &'a str,
}

impl CoinbaseDirectSigningRequest<'_> {
    /// Returns the decimal Unix-seconds timestamp.
    pub const fn timestamp(&self) -> &str {
        self.timestamp
    }

    /// Returns the exact authentication method.
    pub const fn method(self) -> &'static str {
        "GET"
    }

    /// Returns the exact authentication request path.
    pub const fn path(self) -> &'static str {
        WEBSOCKET_AUTH_PATH
    }
}

/// Least-authority local signing boundary. Implementations own and zeroize secret material.
pub trait CoinbaseDirectSigningCapability: fmt::Debug + Send + Sync {
    /// Signs `timestamp + GET + /users/self/verify` and returns bounded redacted credentials.
    fn sign(
        &self,
        request: CoinbaseDirectSigningRequest<'_>,
    ) -> Result<CoinbaseDirectAuthentication, CoinbaseDirectSigningError>;
}

/// Bounded authentication fields. Debug output never reveals any field.
pub struct CoinbaseDirectAuthentication {
    key: Zeroizing<String>,
    passphrase: Zeroizing<String>,
    signature: Zeroizing<String>,
}

impl CoinbaseDirectAuthentication {
    /// Constructs bounded authentication output from the trusted signing boundary.
    pub fn try_new(
        key: String,
        passphrase: String,
        signature: String,
    ) -> Result<Self, CoinbaseDirectSigningError> {
        let key = Zeroizing::new(key);
        let passphrase = Zeroizing::new(passphrase);
        let signature = Zeroizing::new(signature);
        for value in [key.as_str(), passphrase.as_str(), signature.as_str()] {
            if value.is_empty()
                || value.len() > MAX_SIGNING_FIELD_BYTES
                || value.chars().any(char::is_control)
            {
                return Err(CoinbaseDirectSigningError::InvalidAuthentication);
            }
        }
        Ok(Self {
            key,
            passphrase,
            signature,
        })
    }

    fn key(&self) -> &str {
        &self.key
    }

    fn passphrase(&self) -> &str {
        &self.passphrase
    }

    fn signature(&self) -> &str {
        &self.signature
    }
}

impl fmt::Debug for CoinbaseDirectAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoinbaseDirectAuthentication([REDACTED])")
    }
}

/// Serialized authenticated subscription with redacted diagnostics.
pub struct CoinbaseSignedSubscription(Zeroizing<String>);

impl CoinbaseSignedSubscription {
    /// Returns exact bytes for the immediate WebSocket send.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CoinbaseSignedSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoinbaseSignedSubscription([REDACTED])")
    }
}

#[derive(Serialize)]
struct SignedSubscriptionWire<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    product_ids: [&'a str; 1],
    channels: [&'static str; 1],
    signature: &'a str,
    key: &'a str,
    passphrase: &'a str,
    timestamp: &'a str,
}

/// Signing or authenticated-subscription construction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectSigningError {
    /// Timestamp zero cannot satisfy the authentication window.
    #[error("Coinbase Direct signing timestamp is invalid")]
    InvalidTimestamp,
    /// A signing result was empty, oversized, or contained control characters.
    #[error("Coinbase Direct authentication output is invalid")]
    InvalidAuthentication,
    /// Signed subscription serialization failed.
    #[error("Coinbase Direct subscription serialization failed")]
    Serialization,
    /// Signed subscription exceeded its outbound byte ceiling.
    #[error("Coinbase Direct subscription exceeds its byte ceiling")]
    SubscriptionTooLarge,
    /// The local secret/signing capability failed without exposing secret diagnostics.
    #[error("Coinbase Direct signing capability failed")]
    Capability,
}

/// One decoded Coinbase Direct frame classified by its actual cursor semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoinbaseDirectDecodeOutcome {
    /// A proven public product-sequence event eligible for contiguous book processing.
    Sequenced(ProviderOrderEvent),
    /// A validated lifecycle control that carries no public cursor or book authority.
    NonBook(CoinbaseDirectNonBookEvent),
}

/// A validated Coinbase lifecycle control that cannot advance public state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectNonBookEvent {
    kind: CoinbaseDirectNonBookKind,
    evidence: DecoderEvidence,
}

impl CoinbaseDirectNonBookEvent {
    /// Returns the typed lifecycle-control payload.
    pub const fn kind(&self) -> &CoinbaseDirectNonBookKind {
        &self.kind
    }

    /// Returns exact captured-frame evidence without granting cursor authority.
    pub const fn evidence(&self) -> &DecoderEvidence {
        &self.evidence
    }
}

/// Closed non-book lifecycle controls supported by the pinned Direct grammar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoinbaseDirectNonBookKind {
    /// A private stop activation notification; it never advances public sequence or freshness.
    Activate(CoinbaseDirectActivation),
    /// A private received acknowledgement that has not opened a public-book order.
    Received(CoinbaseDirectReceivedLifecycle),
    /// An owner-only TPSL trigger notification with no public cursor or book authority.
    TpslTriggered(CoinbaseDirectTpslTriggeredLifecycle),
}

/// Provider stop classification retained by an `activate` lifecycle control.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoinbaseDirectStopType {
    /// The provider emitted an entry-stop activation.
    Entry,
}

/// Fully typed, unsequenced Coinbase stop activation evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectActivation {
    product: ProviderProduct,
    provider_timestamp: ProviderDecimalLexeme,
    user_id: SourceIdentifier,
    profile_id: SourceIdentifier,
    order_id: SourceIdentifier,
    stop_type: CoinbaseDirectStopType,
    side: ProviderBookSide,
    stop_price: PriceTicks,
    size: QuantityLots,
    funds: ProviderDecimalLexeme,
}

/// Typed private received acknowledgement with no public cursor or freshness authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectReceivedLifecycle {
    product: ProviderProduct,
    order_id: SourceIdentifier,
}

/// Typed owner-only TPSL repricing lifecycle with no public cursor or freshness authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectTpslTriggeredLifecycle {
    order_id: SourceIdentifier,
    side: ProviderBookSide,
    old_price: PriceTicks,
    new_price: PriceTicks,
}

impl CoinbaseDirectReceivedLifecycle {
    /// Returns the exact provider product without creating a sequence domain.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the acknowledged provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }
}

impl CoinbaseDirectTpslTriggeredLifecycle {
    /// Returns the owner order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the owner order side reported by Coinbase.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the instrument-scaled pre-trigger limit price.
    pub const fn old_price(&self) -> PriceTicks {
        self.old_price
    }

    /// Returns the instrument-scaled post-trigger limit price.
    pub const fn new_price(&self) -> PriceTicks {
        self.new_price
    }
}

impl CoinbaseDirectActivation {
    /// Returns the exact provider product without creating a sequence domain.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the provider decimal timestamp lexeme; it is not public price freshness.
    pub const fn provider_timestamp(&self) -> &ProviderDecimalLexeme {
        &self.provider_timestamp
    }

    /// Returns the authenticated provider user identity.
    pub const fn user_id(&self) -> &SourceIdentifier {
        &self.user_id
    }

    /// Returns the authenticated provider profile identity.
    pub const fn profile_id(&self) -> &SourceIdentifier {
        &self.profile_id
    }

    /// Returns the stop-order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the provider stop classification.
    pub const fn stop_type(&self) -> CoinbaseDirectStopType {
        self.stop_type
    }

    /// Returns the stop's book side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the instrument-scaled stop price.
    pub const fn stop_price(&self) -> PriceTicks {
        self.stop_price
    }

    /// Returns the instrument-scaled stop size.
    pub const fn size(&self) -> QuantityLots {
        self.size
    }

    /// Returns the exact nonnegative provider funds lexeme.
    pub const fn funds(&self) -> &ProviderDecimalLexeme {
        &self.funds
    }
}

/// Exact classifier for cursor-bearing and documented non-book Coinbase `full` messages.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectDecoder {
    source_id: SourceId,
    metadata_revision: market_squawk_domain::MetadataRevision,
    product: ProviderProduct,
    terms: InstrumentExecutionTerms,
    decoder_rule: IntegrityRule,
    max_frame_bytes: usize,
}

impl CoinbaseDirectDecoder {
    /// Binds the decoder to one immutable Direct product profile.
    pub fn try_new(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseConfigError> {
        let live = match config.metadata().protocol_profile() {
            SourceProtocolProfile::Live(profile) => profile,
            SourceProtocolProfile::NotLive => {
                return Err(CoinbaseConfigError::InvalidProtocolProfile);
            }
        };
        Ok(Self {
            source_id: config.metadata().source_id().clone(),
            metadata_revision: config.metadata().revision().clone(),
            product: config.product().clone(),
            terms: config.execution_terms(),
            decoder_rule: live.decoder_rule().clone(),
            max_frame_bytes: config.limits.websocket().max_frame_bytes(),
        })
    }

    /// Decodes one already-captured text frame by its actual public-cursor semantics.
    ///
    /// Unknown sequenced types and every schema/invariant violation return an error that requires
    /// a completely fresh snapshot/generation.
    pub fn decode(
        &self,
        validated: &ValidatedRawMarketFrame<'_>,
    ) -> Result<CoinbaseDirectDecodeOutcome, CoinbaseDirectDecodeError> {
        let frame = validated.frame();
        if frame.source_id() != &self.source_id
            || frame.metadata_revision() != &self.metadata_revision
            || frame.transport() != TransportFrameKind::Text
        {
            return Err(CoinbaseDirectDecodeError::FrameAuthority);
        }
        let payload = frame.payload();
        if payload.is_empty() || payload.len() > self.max_frame_bytes {
            return Err(CoinbaseDirectDecodeError::FrameTooLarge);
        }
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| CoinbaseDirectDecodeError::Schema)?;
        let object = value.as_object().ok_or(CoinbaseDirectDecodeError::Schema)?;
        let kind = required_text(object, "type")?;
        if !object.contains_key("sequence") {
            return self.decode_unsequenced(validated, object, kind);
        }
        if kind == "change"
            && object.get("reason").and_then(Value::as_str) == Some("tpsl_triggered")
        {
            return Err(CoinbaseDirectDecodeError::PrivateLifecycleSequence);
        }
        if kind == "activate" {
            return Err(CoinbaseDirectDecodeError::UnknownSequencedMessage);
        }
        let event_kind = match kind {
            "received" => {
                validate_fields(
                    object,
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "order_type",
                        "size",
                        "price",
                        "side",
                        "funds",
                        "client_oid",
                        "user_id",
                        "profile_id",
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "order_type",
                    ],
                )?;
                let _order_id = parse_order_id(object, "order_id")?;
                validate_optional_enum(object, "order_type", &["limit", "market"])?;
                validate_optional_price(object, "price", self.terms)?;
                validate_optional_quantity(object, "size", self.terms, false)?;
                validate_optional_side(object, "side")?;
                validate_optional_nonnegative_decimal(object, "funds")?;
                validate_optional_identifier(object, "client_oid")?;
                validate_authenticated_identity(object)?;
                ProviderOrderEventKind::CursorOnly(ProviderCursorOnlyReason::Received)
            }
            "open" => {
                validate_fields(
                    object,
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "price",
                        "remaining_size",
                        "side",
                        "user_id",
                        "profile_id",
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "price",
                        "remaining_size",
                        "side",
                    ],
                )?;
                validate_authenticated_identity(object)?;
                ProviderOrderEventKind::Open(ProviderOrderRecord::new(
                    parse_order_id(object, "order_id")?,
                    parse_direct_side(required_text(object, "side")?)?,
                    normalize_direct_price(required_text(object, "price")?, self.terms)?,
                    normalize_direct_quantity(
                        required_text(object, "remaining_size")?,
                        self.terms,
                    )?,
                    self.terms,
                ))
            }
            "match" => {
                validate_fields(
                    object,
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "trade_id",
                        "maker_order_id",
                        "taker_order_id",
                        "size",
                        "price",
                        "side",
                        "taker_user_id",
                        "user_id",
                        "taker_profile_id",
                        "profile_id",
                        "taker_fee_rate",
                        "maker_user_id",
                        "maker_profile_id",
                        "maker_fee_rate",
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "trade_id",
                        "maker_order_id",
                        "taker_order_id",
                        "size",
                        "price",
                        "side",
                    ],
                )?;
                required_u64(object, "trade_id")?;
                let _taker_order_id = parse_order_id(object, "taker_order_id")?;
                normalize_direct_price(required_text(object, "price")?, self.terms)?;
                parse_direct_side(required_text(object, "side")?)?;
                for field in [
                    "taker_user_id",
                    "user_id",
                    "taker_profile_id",
                    "profile_id",
                    "maker_user_id",
                    "maker_profile_id",
                ] {
                    validate_optional_identifier(object, field)?;
                }
                validate_optional_nonnegative_decimal(object, "taker_fee_rate")?;
                validate_optional_nonnegative_decimal(object, "maker_fee_rate")?;
                ProviderOrderEventKind::Match {
                    maker_order_id: parse_order_id(object, "maker_order_id")?,
                    maker_side: parse_direct_side(required_text(object, "side")?)?,
                    maker_price: normalize_direct_price(
                        required_text(object, "price")?,
                        self.terms,
                    )?,
                    quantity: normalize_direct_quantity(
                        required_text(object, "size")?,
                        self.terms,
                    )?,
                }
            }
            "done" => {
                validate_fields(
                    object,
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "reason",
                        "price",
                        "remaining_size",
                        "side",
                        "order_type",
                        "user_id",
                        "profile_id",
                        "cancel_reason",
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "reason",
                    ],
                )?;
                let reason = required_text(object, "reason")?;
                if !matches!(reason, "filled" | "canceled") {
                    return Err(CoinbaseDirectDecodeError::Schema);
                }
                validate_optional_price(object, "price", self.terms)?;
                validate_optional_quantity(object, "remaining_size", self.terms, true)?;
                validate_optional_side(object, "side")?;
                validate_optional_enum(object, "order_type", &["limit", "market"])?;
                validate_authenticated_identity(object)?;
                validate_optional_cancel_reason(object)?;
                if reason != "canceled" && object.contains_key("cancel_reason") {
                    return Err(CoinbaseDirectDecodeError::Schema);
                }
                let is_market = object
                    .get("order_type")
                    .and_then(Value::as_str)
                    .is_some_and(|order_type| order_type == "market");
                let has_price = object.contains_key("price");
                let has_remaining = object.contains_key("remaining_size");
                let has_side = object.contains_key("side");
                if (is_market && (has_price || has_remaining))
                    || (has_price != has_remaining)
                    || ((has_price || has_remaining) && !has_side)
                {
                    return Err(CoinbaseDirectDecodeError::Schema);
                }
                ProviderOrderEventKind::Done {
                    order_id: parse_order_id(object, "order_id")?,
                    side: object
                        .get("side")
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or(CoinbaseDirectDecodeError::Schema)
                                .and_then(parse_direct_side)
                        })
                        .transpose()?,
                    price: object
                        .get("price")
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or(CoinbaseDirectDecodeError::Schema)
                                .and_then(|value| normalize_direct_price(value, self.terms))
                        })
                        .transpose()?,
                    remaining_quantity: object
                        .get("remaining_size")
                        .map(|value| {
                            value
                                .as_str()
                                .ok_or(CoinbaseDirectDecodeError::Schema)
                                .and_then(|value| {
                                    normalize_direct_delta_quantity(value, self.terms)
                                })
                        })
                        .transpose()?,
                }
            }
            "change" => {
                validate_fields(
                    object,
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "price",
                        "side",
                        "new_size",
                        "old_size",
                        "new_funds",
                        "old_funds",
                        "reason",
                        "old_price",
                        "new_price",
                        "user_id",
                        "profile_id",
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "order_id",
                        "reason",
                        "side",
                    ],
                )?;
                let side = parse_direct_side(required_text(object, "side")?)?;
                validate_authenticated_identity(object)?;
                let (reason, previous_price, previous_quantity, new_price, new_quantity) =
                    match required_text(object, "reason")? {
                        "modify_order" => {
                            if object.contains_key("price")
                                || object.contains_key("new_funds")
                                || object.contains_key("old_funds")
                            {
                                return Err(CoinbaseDirectDecodeError::Schema);
                            }
                            let previous_price = normalize_direct_price(
                                required_text(object, "old_price")?,
                                self.terms,
                            )?;
                            let new_price = normalize_direct_price(
                                required_text(object, "new_price")?,
                                self.terms,
                            )?;
                            let previous_quantity = normalize_direct_quantity(
                                required_text(object, "old_size")?,
                                self.terms,
                            )?;
                            let new_quantity = normalize_direct_quantity(
                                required_text(object, "new_size")?,
                                self.terms,
                            )?;
                            (
                                ProviderOrderChangeReason::ModifyOrder,
                                Some(previous_price),
                                Some(previous_quantity),
                                Some(new_price),
                                Some(new_quantity),
                            )
                        }
                        "STP" => match (
                            object.get("old_size"),
                            object.get("new_size"),
                            object.get("old_funds"),
                            object.get("new_funds"),
                        ) {
                            (Some(old_size), Some(new_size), None, None)
                                if !object.contains_key("old_price")
                                    && !object.contains_key("new_price") =>
                            {
                                let previous_price = normalize_direct_price(
                                    required_text(object, "price")?,
                                    self.terms,
                                )?;
                                let previous_quantity = normalize_direct_quantity(
                                    old_size.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?,
                                    self.terms,
                                )?;
                                let new_quantity = normalize_direct_quantity(
                                    new_size.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?,
                                    self.terms,
                                )?;
                                if new_quantity > previous_quantity {
                                    return Err(CoinbaseDirectDecodeError::Numeric);
                                }
                                (
                                    ProviderOrderChangeReason::SelfTradePrevention,
                                    Some(previous_price),
                                    Some(previous_quantity),
                                    None,
                                    Some(new_quantity),
                                )
                            }
                            (None, None, Some(old_funds), Some(new_funds))
                                if !object.contains_key("old_price")
                                    && !object.contains_key("new_price")
                                    && object.get("price").is_none_or(Value::is_null) =>
                            {
                                parse_nonnegative_decimal_value(old_funds)?;
                                parse_nonnegative_decimal_value(new_funds)?;
                                (
                                    ProviderOrderChangeReason::SelfTradePrevention,
                                    None,
                                    None,
                                    None,
                                    None,
                                )
                            }
                            _ => return Err(CoinbaseDirectDecodeError::Schema),
                        },
                        _ => return Err(CoinbaseDirectDecodeError::Schema),
                    };
                ProviderOrderEventKind::Change {
                    order_id: parse_order_id(object, "order_id")?,
                    reason,
                    side,
                    previous_price,
                    previous_quantity,
                    new_price,
                    new_quantity,
                }
            }
            _ if object.contains_key("sequence") => {
                return Err(CoinbaseDirectDecodeError::UnknownSequencedMessage);
            }
            _ => return Err(CoinbaseDirectDecodeError::UnsupportedMessage),
        };
        let product = required_text(object, "product_id")?;
        if product != self.product.as_source_identifier().as_str() {
            return Err(CoinbaseDirectDecodeError::WrongProduct);
        }
        let sequence = object
            .get("sequence")
            .and_then(Value::as_u64)
            .map(SequenceNumber::new)
            .ok_or(CoinbaseDirectDecodeError::Schema)?;
        let timestamp = parse_direct_timestamp(required_text(object, "time")?)?;
        ProviderOrderEvent::try_new(
            self.product.clone(),
            sequence,
            timestamp,
            event_kind,
            self.terms,
            DecoderEvidence::from_validated_frame(validated, self.decoder_rule.clone()),
        )
        .map(CoinbaseDirectDecodeOutcome::Sequenced)
        .map_err(|_| CoinbaseDirectDecodeError::FrameTooLarge)
    }

    fn decode_unsequenced(
        &self,
        validated: &ValidatedRawMarketFrame<'_>,
        object: &Map<String, Value>,
        kind: &str,
    ) -> Result<CoinbaseDirectDecodeOutcome, CoinbaseDirectDecodeError> {
        let evidence = DecoderEvidence::from_validated_frame(validated, self.decoder_rule.clone());
        let kind = match kind {
            "activate" => CoinbaseDirectNonBookKind::Activate(self.decode_activate(object)?),
            "received" => {
                CoinbaseDirectNonBookKind::Received(self.decode_private_received(object)?)
            }
            "change" if object.get("reason").and_then(Value::as_str) == Some("tpsl_triggered") => {
                CoinbaseDirectNonBookKind::TpslTriggered(self.decode_tpsl_triggered(object)?)
            }
            "open" | "match" | "done" | "change" => {
                return Err(CoinbaseDirectDecodeError::UnsequencedBookMutation);
            }
            _ => return Err(CoinbaseDirectDecodeError::UnsupportedMessage),
        };
        Ok(CoinbaseDirectDecodeOutcome::NonBook(
            CoinbaseDirectNonBookEvent { kind, evidence },
        ))
    }

    fn decode_activate(
        &self,
        object: &Map<String, Value>,
    ) -> Result<CoinbaseDirectActivation, CoinbaseDirectDecodeError> {
        validate_fields(
            object,
            &[
                "type",
                "product_id",
                "timestamp",
                "user_id",
                "profile_id",
                "order_id",
                "stop_type",
                "side",
                "stop_price",
                "size",
                "funds",
                "private",
            ],
            &[
                "type",
                "product_id",
                "timestamp",
                "user_id",
                "profile_id",
                "order_id",
                "stop_type",
                "side",
                "stop_price",
                "size",
                "funds",
                "private",
            ],
        )?;
        self.validate_product_text(object)?;
        let provider_timestamp =
            ProviderDecimalLexeme::try_new(required_text(object, "timestamp")?)
                .map_err(|_| CoinbaseDirectDecodeError::Timestamp)?;
        if provider_timestamp.decimal().is_sign_negative()
            || object.get("private").and_then(Value::as_bool) != Some(true)
        {
            return Err(CoinbaseDirectDecodeError::Schema);
        }
        let stop_type = match required_text(object, "stop_type")? {
            "entry" => CoinbaseDirectStopType::Entry,
            _ => return Err(CoinbaseDirectDecodeError::Schema),
        };
        let funds = ProviderDecimalLexeme::try_new(required_text(object, "funds")?)
            .map_err(|_| CoinbaseDirectDecodeError::Numeric)?;
        if funds.decimal().is_sign_negative() {
            return Err(CoinbaseDirectDecodeError::Numeric);
        }
        Ok(CoinbaseDirectActivation {
            product: self.product.clone(),
            provider_timestamp,
            user_id: parse_order_id(object, "user_id")?,
            profile_id: parse_order_id(object, "profile_id")?,
            order_id: parse_order_id(object, "order_id")?,
            stop_type,
            side: parse_direct_side(required_text(object, "side")?)?,
            stop_price: normalize_direct_price(required_text(object, "stop_price")?, self.terms)?,
            size: normalize_direct_quantity(required_text(object, "size")?, self.terms)?,
            funds,
        })
    }

    fn decode_private_received(
        &self,
        object: &Map<String, Value>,
    ) -> Result<CoinbaseDirectReceivedLifecycle, CoinbaseDirectDecodeError> {
        validate_fields(
            object,
            &[
                "type",
                "time",
                "product_id",
                "order_id",
                "order_type",
                "size",
                "price",
                "side",
                "funds",
                "client_oid",
                "user_id",
                "profile_id",
            ],
            &["type", "time", "product_id", "order_id", "order_type"],
        )?;
        if !object.contains_key("user_id") && !object.contains_key("profile_id") {
            return Err(CoinbaseDirectDecodeError::UnsequencedBookMutation);
        }
        self.validate_product_text(object)?;
        let _provider_time = parse_direct_timestamp(required_text(object, "time")?)?;
        validate_optional_enum(object, "order_type", &["limit", "market"])?;
        validate_optional_price(object, "price", self.terms)?;
        validate_optional_quantity(object, "size", self.terms, false)?;
        validate_optional_side(object, "side")?;
        validate_optional_nonnegative_decimal(object, "funds")?;
        validate_optional_identifier(object, "client_oid")?;
        validate_authenticated_identity(object)?;
        Ok(CoinbaseDirectReceivedLifecycle {
            product: self.product.clone(),
            order_id: parse_order_id(object, "order_id")?,
        })
    }

    fn decode_tpsl_triggered(
        &self,
        object: &Map<String, Value>,
    ) -> Result<CoinbaseDirectTpslTriggeredLifecycle, CoinbaseDirectDecodeError> {
        validate_fields(
            object,
            &[
                "type",
                "reason",
                "order_id",
                "side",
                "old_price",
                "new_price",
            ],
            &[
                "type",
                "reason",
                "order_id",
                "side",
                "old_price",
                "new_price",
            ],
        )?;
        if required_text(object, "reason")? != "tpsl_triggered" {
            return Err(CoinbaseDirectDecodeError::Schema);
        }
        Ok(CoinbaseDirectTpslTriggeredLifecycle {
            order_id: parse_order_id(object, "order_id")?,
            side: parse_direct_side(required_text(object, "side")?)?,
            old_price: normalize_direct_price(required_text(object, "old_price")?, self.terms)?,
            new_price: normalize_direct_price(required_text(object, "new_price")?, self.terms)?,
        })
    }

    fn validate_product_text(
        &self,
        object: &Map<String, Value>,
    ) -> Result<(), CoinbaseDirectDecodeError> {
        if required_text(object, "product_id")? == self.product.as_source_identifier().as_str() {
            Ok(())
        } else {
            Err(CoinbaseDirectDecodeError::WrongProduct)
        }
    }
}

fn required_text<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, CoinbaseDirectDecodeError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(CoinbaseDirectDecodeError::Schema)
}

fn validate_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    required: &[&str],
) -> Result<(), CoinbaseDirectDecodeError> {
    if object
        .keys()
        .any(|field| !allowed.contains(&field.as_str()))
        || required.iter().any(|field| !object.contains_key(*field))
    {
        Err(CoinbaseDirectDecodeError::Schema)
    } else {
        Ok(())
    }
}

fn required_u64(
    object: &Map<String, Value>,
    field: &str,
) -> Result<u64, CoinbaseDirectDecodeError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(CoinbaseDirectDecodeError::Schema)
}

fn validate_authenticated_identity(
    object: &Map<String, Value>,
) -> Result<(), CoinbaseDirectDecodeError> {
    validate_optional_identifier(object, "user_id")?;
    validate_optional_identifier(object, "profile_id")
}

fn validate_optional_identifier(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    let value = value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?;
    SourceIdentifier::try_from(value)
        .map(|_identifier| ())
        .map_err(|_| CoinbaseDirectDecodeError::Schema)
}

fn validate_optional_cancel_reason(
    object: &Map<String, Value>,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get("cancel_reason") else {
        return Ok(());
    };
    if matches!(
        value,
        Value::String(code)
            if matches!(code.as_str(), "101" | "102" | "103" | "104" | "105" | "106" | "107")
    ) || matches!(value, Value::Number(code) if matches!(code.as_u64(), Some(101..=107)))
    {
        Ok(())
    } else {
        Err(CoinbaseDirectDecodeError::Schema)
    }
}

fn validate_optional_enum(
    object: &Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    let value = value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?;
    if allowed.contains(&value) {
        Ok(())
    } else {
        Err(CoinbaseDirectDecodeError::Schema)
    }
}

fn validate_optional_side(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    parse_direct_side(value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?).map(|_side| ())
}

fn validate_optional_price(
    object: &Map<String, Value>,
    field: &str,
    terms: InstrumentExecutionTerms,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    normalize_direct_price(
        value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?,
        terms,
    )
    .map(|_price| ())
}

fn validate_optional_quantity(
    object: &Map<String, Value>,
    field: &str,
    terms: InstrumentExecutionTerms,
    allow_zero: bool,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    let value = value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?;
    if allow_zero {
        normalize_direct_delta_quantity(value, terms).map(|_quantity| ())
    } else {
        normalize_direct_quantity(value, terms).map(|_quantity| ())
    }
}

fn validate_optional_nonnegative_decimal(
    object: &Map<String, Value>,
    field: &str,
) -> Result<(), CoinbaseDirectDecodeError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    parse_nonnegative_decimal_value(value)
}

fn parse_nonnegative_decimal_value(value: &Value) -> Result<(), CoinbaseDirectDecodeError> {
    let value = value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?;
    let lexeme =
        ProviderDecimalLexeme::try_new(value).map_err(|_| CoinbaseDirectDecodeError::Numeric)?;
    if lexeme.decimal().is_sign_negative() {
        Err(CoinbaseDirectDecodeError::Numeric)
    } else {
        Ok(())
    }
}

fn parse_order_id(
    object: &Map<String, Value>,
    field: &str,
) -> Result<SourceIdentifier, CoinbaseDirectDecodeError> {
    SourceIdentifier::try_from(required_text(object, field)?)
        .map_err(|_| CoinbaseDirectDecodeError::OrderIdentity)
}

fn parse_direct_side(value: &str) -> Result<ProviderBookSide, CoinbaseDirectDecodeError> {
    match value {
        "buy" => Ok(ProviderBookSide::Bid),
        "sell" => Ok(ProviderBookSide::Ask),
        _ => Err(CoinbaseDirectDecodeError::Schema),
    }
}

fn parse_direct_timestamp(value: &str) -> Result<Timestamp, CoinbaseDirectDecodeError> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(Timestamp::from_unix_nanos)
        .ok_or(CoinbaseDirectDecodeError::Timestamp)
}

fn parse_direct_price(value: &str) -> Result<ProviderPrice, CoinbaseDirectDecodeError> {
    let lexeme =
        ProviderDecimalLexeme::try_new(value).map_err(|_| CoinbaseDirectDecodeError::Numeric)?;
    if lexeme.decimal().is_zero() || lexeme.decimal().is_sign_negative() {
        return Err(CoinbaseDirectDecodeError::Numeric);
    }
    Ok(ProviderPrice::new(lexeme))
}

fn parse_direct_quantity(value: &str) -> Result<ProviderQuantity, CoinbaseDirectDecodeError> {
    let lexeme =
        ProviderDecimalLexeme::try_new(value).map_err(|_| CoinbaseDirectDecodeError::Numeric)?;
    if lexeme.decimal().is_zero() || lexeme.decimal().is_sign_negative() {
        return Err(CoinbaseDirectDecodeError::Numeric);
    }
    Ok(ProviderQuantity::new(lexeme))
}

fn normalize_direct_price(
    value: &str,
    terms: InstrumentExecutionTerms,
) -> Result<PriceTicks, CoinbaseDirectDecodeError> {
    normalize_price(&parse_direct_price(value)?, terms.price_tick())
        .map_err(|_| CoinbaseDirectDecodeError::Numeric)
}

fn normalize_direct_quantity(
    value: &str,
    terms: InstrumentExecutionTerms,
) -> Result<QuantityLots, CoinbaseDirectDecodeError> {
    normalize_positive_quantity(&parse_direct_quantity(value)?, terms.lot_size())
        .map_err(|_| CoinbaseDirectDecodeError::Numeric)
}

fn normalize_direct_delta_quantity(
    value: &str,
    terms: InstrumentExecutionTerms,
) -> Result<QuantityLots, CoinbaseDirectDecodeError> {
    let lexeme =
        ProviderDecimalLexeme::try_new(value).map_err(|_| CoinbaseDirectDecodeError::Numeric)?;
    let provider = ProviderQuantity::new(lexeme);
    normalize_delta_quantity(&provider, terms.lot_size())
        .map_err(|_| CoinbaseDirectDecodeError::Numeric)
}

/// A `full` frame that cannot safely advance the maintained product cursor.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectDecodeError {
    /// The validated frame belongs to another configured source/revision or transport.
    #[error("Coinbase Direct frame authority does not match the decoder")]
    FrameAuthority,
    /// The captured frame is empty or exceeds its configured WebSocket bound.
    #[error("Coinbase Direct frame size is invalid")]
    FrameTooLarge,
    /// Known-message fields are missing, duplicated, wrong-typed, or newly introduced.
    #[error("Coinbase Direct message schema is invalid")]
    Schema,
    /// The frame belongs to another product.
    #[error("Coinbase Direct message belongs to the wrong product")]
    WrongProduct,
    /// Venue event time is missing or invalid.
    #[error("Coinbase Direct message time is invalid")]
    Timestamp,
    /// Exact price or quantity evidence is invalid.
    #[error("Coinbase Direct numeric evidence is invalid")]
    Numeric,
    /// An order identity exceeds the bounded provider identity grammar.
    #[error("Coinbase Direct order identity is invalid")]
    OrderIdentity,
    /// A new sequenced type may mutate state and forces a fresh snapshot.
    #[error("Coinbase Direct sequenced message type is unknown")]
    UnknownSequencedMessage,
    /// An owner-only private lifecycle was combined with a public product sequence.
    #[error("Coinbase Direct private lifecycle cannot carry a public sequence")]
    PrivateLifecycleSequence,
    /// A lifecycle frame could mutate the public book but carries no advancing public cursor.
    #[error("Coinbase Direct book mutation has no provable public sequence")]
    UnsequencedBookMutation,
    /// An unsequenced message is outside this order-event decoder.
    #[error("Coinbase Direct message type is unsupported")]
    UnsupportedMessage,
}

/// Streaming level-3 snapshot decoder bound to one exact Direct profile.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectSnapshotDecoder {
    source_id: SourceId,
    metadata_revision: market_squawk_domain::MetadataRevision,
    product: ProviderProduct,
    terms: InstrumentExecutionTerms,
    snapshot_url: Box<str>,
    limits: CoinbaseDirectLimits,
}

impl CoinbaseDirectSnapshotDecoder {
    /// Constructs the snapshot decoder from immutable direct configuration.
    pub fn try_new(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseConfigError> {
        Ok(Self {
            source_id: config.metadata().source_id().clone(),
            metadata_revision: config.metadata().revision().clone(),
            product: config.product().clone(),
            terms: config.execution_terms(),
            snapshot_url: config.snapshot_url().to_owned().into_boxed_str(),
            limits: config.limits(),
        })
    }

    /// Streams an exact segmented response into the instrument-owned unpublished order map.
    ///
    /// Metadata is scanned without retaining rows, then a second bounded streaming pass inserts
    /// each order directly. Any capture, schema, count, numeric, or owner error invalidates the
    /// generation.
    pub fn decode_into(
        &self,
        capture: &SegmentedHttpResponseCapture,
        owner: &mut DirectOrderBook,
    ) -> Result<(), CoinbaseDirectSnapshotError> {
        if owner.product() != &self.product {
            owner.invalidate_generation();
            return Err(CoinbaseDirectSnapshotError::WrongProduct);
        }
        if owner.execution_terms() != self.terms {
            owner.invalidate_generation();
            return Err(CoinbaseDirectSnapshotError::Owner(
                DirectOrderBookError::InstrumentTermsMismatch,
            ));
        }
        if let Err(error) = validate_http_capture(
            capture,
            &self.snapshot_url,
            &self.source_id,
            &self.metadata_revision,
            self.limits.max_snapshot_bytes,
            self.limits.max_snapshot_segments,
        ) {
            owner.invalidate_generation();
            return Err(CoinbaseDirectSnapshotError::Capture(error));
        }
        let metadata: SnapshotMetadataWire = match serde_json::from_reader(capture.reader()) {
            Ok(value) => value,
            Err(_) => {
                owner.invalidate_generation();
                return Err(CoinbaseDirectSnapshotError::Schema);
            }
        };
        let timestamp = match parse_direct_timestamp(&metadata.time) {
            Ok(value) => value,
            Err(_) => {
                owner.invalidate_generation();
                return Err(CoinbaseDirectSnapshotError::Timestamp);
            }
        };
        if let Err(error) = validate_snapshot_auction(&metadata, self.terms) {
            owner.invalidate_generation();
            return Err(error);
        }
        owner.begin_snapshot(SequenceNumber::new(metadata.sequence))?;
        let parsed = {
            let mut deserializer = serde_json::Deserializer::from_reader(capture.reader());
            let decoded = SnapshotRowsSeed {
                owner,
                terms: self.terms,
            }
            .deserialize(&mut deserializer);
            decoded.and_then(|count| {
                deserializer.end()?;
                if count == 0 {
                    return Err(serde_json::Error::custom("empty Coinbase snapshot"));
                }
                Ok(())
            })
        };
        if parsed.is_err() {
            owner.invalidate_generation();
            return Err(CoinbaseDirectSnapshotError::Schema);
        }
        owner.bind_snapshot_receipt(capture.receipt().clone())?;
        owner.finish_snapshot(timestamp)?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotMetadataWire {
    sequence: u64,
    time: String,
    auction_mode: Option<bool>,
    auction: Option<SnapshotAuctionWire>,
    #[serde(rename = "bids")]
    _bids: IgnoredAny,
    #[serde(rename = "asks")]
    _asks: IgnoredAny,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotAuctionWire {
    indicative_open_price: String,
    indicative_open_size: String,
    indicative_bid_price: String,
    indicative_bid_size: String,
    indicative_ask_price: String,
    indicative_ask_size: String,
    auction_status: String,
}

fn validate_snapshot_auction(
    metadata: &SnapshotMetadataWire,
    terms: InstrumentExecutionTerms,
) -> Result<(), CoinbaseDirectSnapshotError> {
    match (metadata.auction_mode, metadata.auction.as_ref()) {
        (Some(true), Some(auction)) => {
            normalize_direct_price(&auction.indicative_open_price, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            normalize_direct_quantity(&auction.indicative_open_size, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            normalize_direct_price(&auction.indicative_bid_price, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            normalize_direct_quantity(&auction.indicative_bid_size, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            normalize_direct_price(&auction.indicative_ask_price, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            normalize_direct_quantity(&auction.indicative_ask_size, terms)
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            SourceIdentifier::try_from(auction.auction_status.as_str())
                .map_err(|_| CoinbaseDirectSnapshotError::Schema)?;
            Err(CoinbaseDirectSnapshotError::AuctionMode)
        }
        (Some(true), None) | (Some(false), Some(_)) | (None, Some(_)) => {
            Err(CoinbaseDirectSnapshotError::Schema)
        }
        (Some(false), None) | (None, None) => Ok(()),
    }
}

struct SnapshotRowsSeed<'a> {
    owner: &'a mut DirectOrderBook,
    terms: InstrumentExecutionTerms,
}

impl<'de> DeserializeSeed<'de> for SnapshotRowsSeed<'_> {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SnapshotRowsVisitor {
            owner: self.owner,
            terms: self.terms,
        })
    }
}

struct SnapshotRowsVisitor<'a> {
    owner: &'a mut DirectOrderBook,
    terms: InstrumentExecutionTerms,
}

impl<'de> Visitor<'de> for SnapshotRowsVisitor<'_> {
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Coinbase level-3 snapshot object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen_sequence = false;
        let mut seen_time = false;
        let mut seen_bids = false;
        let mut seen_asks = false;
        let mut seen_auction_mode = false;
        let mut seen_auction = false;
        let mut count = 0_usize;
        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                "sequence" if !seen_sequence => {
                    let _value = map.next_value::<u64>()?;
                    seen_sequence = true;
                }
                "time" if !seen_time => {
                    let _value = map.next_value::<String>()?;
                    seen_time = true;
                }
                "bids" if !seen_bids => {
                    count = count
                        .checked_add(map.next_value_seed(SnapshotSideSeed {
                            owner: &mut *self.owner,
                            side: ProviderBookSide::Bid,
                            terms: self.terms,
                        })?)
                        .ok_or_else(|| A::Error::custom("snapshot order count overflow"))?;
                    seen_bids = true;
                }
                "asks" if !seen_asks => {
                    count = count
                        .checked_add(map.next_value_seed(SnapshotSideSeed {
                            owner: &mut *self.owner,
                            side: ProviderBookSide::Ask,
                            terms: self.terms,
                        })?)
                        .ok_or_else(|| A::Error::custom("snapshot order count overflow"))?;
                    seen_asks = true;
                }
                "auction_mode" if !seen_auction_mode => {
                    let _value = map.next_value::<bool>()?;
                    seen_auction_mode = true;
                }
                "auction" if !seen_auction => {
                    let _value = map.next_value::<Option<SnapshotAuctionWire>>()?;
                    seen_auction = true;
                }
                _ => return Err(A::Error::custom("unknown or duplicate snapshot field")),
            }
        }
        if !(seen_sequence && seen_time && seen_bids && seen_asks) {
            return Err(A::Error::custom("incomplete snapshot"));
        }
        Ok(count)
    }
}

struct SnapshotSideSeed<'a> {
    owner: &'a mut DirectOrderBook,
    side: ProviderBookSide,
    terms: InstrumentExecutionTerms,
}

impl<'de> DeserializeSeed<'de> for SnapshotSideSeed<'_> {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(SnapshotSideVisitor {
            owner: self.owner,
            side: self.side,
            terms: self.terms,
        })
    }
}

struct SnapshotSideVisitor<'a> {
    owner: &'a mut DirectOrderBook,
    side: ProviderBookSide,
    terms: InstrumentExecutionTerms,
}

impl<'de> Visitor<'de> for SnapshotSideVisitor<'_> {
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a sequence of Coinbase [price,size,order_id] rows")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some([price, quantity, order_id]) = sequence.next_element::<[String; 3]>()? {
            let order_id = SourceIdentifier::try_from(order_id)
                .map_err(|_| A::Error::custom("invalid snapshot order id"))?;
            let price = normalize_direct_price(&price, self.terms)
                .map_err(|_| A::Error::custom("invalid snapshot price"))?;
            let quantity = normalize_direct_quantity(&quantity, self.terms)
                .map_err(|_| A::Error::custom("invalid snapshot quantity"))?;
            self.owner
                .try_push_snapshot_order(ProviderOrderRecord::new(
                    order_id, self.side, price, quantity, self.terms,
                ))
                .map_err(A::Error::custom)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| A::Error::custom("snapshot order count overflow"))?;
        }
        Ok(count)
    }
}

fn validate_http_capture(
    capture: &SegmentedHttpResponseCapture,
    expected_url: &str,
    expected_source: &SourceId,
    expected_revision: &market_squawk_domain::MetadataRevision,
    max_body_bytes: u64,
    max_segments: usize,
) -> Result<(), CoinbaseDirectCaptureError> {
    let receipt = capture.receipt();
    if receipt.currentness_lease().validate_current().is_err()
        || receipt.source_id() != expected_source
        || receipt.metadata_revision() != expected_revision
        || receipt.method() != HttpCaptureMethod::Get
        || receipt.status() != 200
        || receipt.final_url() != expected_url
        || receipt.body_length() == 0
        || receipt.body_length() > max_body_bytes
        || receipt.segments().is_empty()
        || receipt.segments().len() > max_segments
        || receipt
            .declared_body_length()
            .is_some_and(|declared| declared != receipt.body_length())
    {
        return Err(CoinbaseDirectCaptureError::InvalidReceipt);
    }
    Ok(())
}

/// Snapshot capture or decode failure; every variant requires a fresh generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectSnapshotError {
    /// HTTP capture metadata or bounds are inconsistent.
    #[error("Coinbase Direct snapshot capture is invalid: {0}")]
    Capture(#[from] CoinbaseDirectCaptureError),
    /// Snapshot belongs to another configured product.
    #[error("Coinbase Direct snapshot belongs to the wrong product")]
    WrongProduct,
    /// Snapshot JSON shape or order row is invalid.
    #[error("Coinbase Direct snapshot schema is invalid")]
    Schema,
    /// Required provider source time is invalid.
    #[error("Coinbase Direct snapshot time is invalid")]
    Timestamp,
    /// Auction indicative books are retained provider evidence, not execution authority.
    #[error("Coinbase Direct snapshot is in auction mode")]
    AuctionMode,
    /// Instrument-owned lifecycle, sequence, map, count, byte, or invariant failure.
    #[error("Coinbase Direct snapshot owner rejected state: {0}")]
    Owner(#[from] DirectOrderBookError),
}

/// Segmented response receipt mismatch.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectCaptureError {
    /// Method, final URL, status, length, segment count, or configured bound is inconsistent.
    #[error("captured HTTP response receipt is inconsistent")]
    InvalidReceipt,
}

#[derive(Deserialize)]
struct ProductWire {
    id: String,
    status: String,
    base_increment: String,
    quote_increment: String,
    trading_disabled: bool,
    cancel_only: bool,
    post_only: bool,
    limit_only: bool,
    auction_mode: bool,
}

/// Current provider-authored product status and precision evidence.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectProductEvidence {
    product: ProviderProduct,
    provider_status: SourceIdentifier,
    trading_status: TradingStatus,
    base_increment: ProviderQuantity,
    quote_increment: ProviderQuantity,
    trading_disabled: bool,
    cancel_only: bool,
    post_only: bool,
    limit_only: bool,
    auction_mode: bool,
    capture: SegmentedHttpResponseReceipt,
}

impl CoinbaseDirectProductEvidence {
    /// Returns the exact provider product.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the exact provider status token.
    pub const fn provider_status(&self) -> &SourceIdentifier {
        &self.provider_status
    }

    /// Returns the conservatively interpreted current trading status.
    pub const fn trading_status(&self) -> TradingStatus {
        self.trading_status
    }

    /// Returns exact base-size increment evidence.
    pub const fn base_increment(&self) -> &ProviderQuantity {
        &self.base_increment
    }

    /// Returns exact quote-price increment evidence.
    pub const fn quote_increment(&self) -> &ProviderQuantity {
        &self.quote_increment
    }

    /// Returns whether the product is provider-disabled for trading.
    pub const fn trading_disabled(&self) -> bool {
        self.trading_disabled
    }

    /// Returns whether only cancellations are currently accepted.
    pub const fn cancel_only(&self) -> bool {
        self.cancel_only
    }

    /// Returns whether only post-only orders are currently accepted.
    pub const fn post_only(&self) -> bool {
        self.post_only
    }

    /// Returns whether only limit orders are currently accepted.
    pub const fn limit_only(&self) -> bool {
        self.limit_only
    }

    /// Returns whether the product is in provider auction mode.
    pub const fn auction_mode(&self) -> bool {
        self.auction_mode
    }

    /// Returns the registry-trusted effective observation coordinate for this product response.
    pub const fn observed_at(&self) -> Timestamp {
        self.capture.received_at()
    }

    /// Returns the exact HTTP capture receipt.
    pub const fn capture_receipt(&self) -> &SegmentedHttpResponseReceipt {
        &self.capture
    }
}

/// Current-product response failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectProductError {
    /// HTTP capture metadata or bounds are inconsistent.
    #[error("Coinbase Direct product capture is invalid: {0}")]
    Capture(CoinbaseDirectCaptureError),
    /// Product JSON is missing a required typed field.
    #[error("Coinbase Direct product response schema is invalid")]
    Schema,
    /// Response belongs to another product.
    #[error("Coinbase Direct product response belongs to the wrong product")]
    WrongProduct,
    /// Product status cannot fit the bounded provider identity.
    #[error("Coinbase Direct product status is invalid")]
    Status,
    /// Base or quote increment is nonpositive or inexact.
    #[error("Coinbase Direct product increment is invalid")]
    Increment,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::str::FromStr as _;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, ConnectionGeneration, Currency, Denomination,
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
        InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize,
        MetadataRevision, PriceTicks, ProviderProduct, QuantityLots, RevisionBoundPayloadEvidence,
        SequenceCapability, SourceId, SourceIdentifier, TickSize, Timestamp, TradingStatus,
    };
    use market_squawk_live::{DirectBookLimits, DirectOrderBook, DirectSyncPhase};
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode,
        AuthorizationSubjectResolutionError, AuthorizationSubjectResolver, BackoffPolicy,
        BudgetScope, BudgetWindowSemantics, CurrentSourceSession, FreshnessPolicy,
        HttpCaptureMethod, ProviderBookSide, ProviderBudgetPolicy, ProviderBudgetWindow,
        ProviderDecimalLexeme, ProviderOrderChangeReason, ProviderOrderEventKind, RawFrameFactory,
        SessionId, TransportFrameKind,
    };
    use sha2::Digest as _;

    use crate::{
        COINBASE_DIRECT_WEBSOCKET_ENDPOINT, CoinbaseConfigError, CoinbaseDirectAuthentication,
        CoinbaseDirectConfig, CoinbaseDirectDecodeError, CoinbaseDirectDecodeOutcome,
        CoinbaseDirectDecoder, CoinbaseDirectLimits, CoinbaseDirectNonBookKind,
        CoinbaseDirectSigningCapability, CoinbaseDirectSigningError, CoinbaseDirectSigningRequest,
        CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError, CoinbaseDirectStopType,
        CoinbaseProductMapping, CoinbaseTransportLimits,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    static_assertions::assert_not_impl_any!(CoinbaseDirectAuthentication: Clone);
    static_assertions::assert_not_impl_any!(super::CoinbaseSignedSubscription: Clone);

    fn id(value: &str) -> TestResult<SourceIdentifier> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    fn evidence(byte: u8) -> ExactPayloadEvidence {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        ))
    }

    fn config() -> TestResult<CoinbaseDirectConfig> {
        Ok(config_with_budget(2, &[(8, 1_000_000_000)])??)
    }

    fn config_with_budget(
        max_concurrent: u16,
        windows: &[(u32, u64)],
    ) -> TestResult<Result<CoinbaseDirectConfig, CoinbaseConfigError>> {
        let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let terms = InstrumentExecutionTerms::try_new(
            instrument,
            InstrumentDefinitionRevision::try_from(1)?,
            TickSize::try_from_decimal(ProviderDecimalLexeme::try_new("0.01")?.decimal())?,
            LotSize::try_from_decimal(ProviderDecimalLexeme::try_new("0.00000001")?.decimal())?,
            Currency::try_from("USD")?,
            Denomination::Currency(Currency::try_from("BTC")?),
            ProviderDecimalLexeme::try_new("1")?.decimal(),
        )?;
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(id("coinbase-read-only-market-data-account")?),
            evidence(2),
            effective,
        );
        let windows = windows
            .iter()
            .map(|&(requests_per_window, window_nanos)| {
                Ok(ProviderBudgetWindow::try_new(
                    NonZeroU32::new(requests_per_window).ok_or("zero request budget")?,
                    NonZeroU64::new(window_nanos).ok_or("zero budget window")?,
                    BudgetWindowSemantics::Tumbling,
                )?)
            })
            .collect::<TestResult<Vec<_>>>()?;
        let budget = ProviderBudgetPolicy::try_new_conjunctive(
            BudgetScope::for_authorization(id("coinbase-exchange")?, &authorization)?,
            &windows,
            NonZeroU16::new(max_concurrent).ok_or("zero concurrency")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("zero initial backoff")?,
                NonZeroU64::new(1_000_000_000).ok_or("zero maximum backoff")?,
                1_000,
            )?,
        )?;
        Ok(CoinbaseDirectConfig::try_new(
            SourceId::try_from("coinbase-exchange-direct")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(id("coinbase-direct-2026-07-24")?),
                evidence(3),
            ),
            authorization,
            evidence(4),
            effective,
            CoinbaseProductMapping::try_new(ProviderProduct::new(id("BTC-USD")?), instrument)?,
            terms,
            FreshnessPolicy::try_new(
                5_000_000_000,
                1_000_000_000,
                2_000_000_000,
                1_000_000_000,
                100_000_000,
            )?,
            budget,
            CoinbaseDirectLimits::try_new(
                CoinbaseTransportLimits::try_new(
                    256 * 1024,
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                )?,
                16 * 1024 * 1024,
                8,
                DirectBookLimits::try_new(128, 64, 32, 512 * 1024, 8)?,
            )?,
        ))
    }

    fn capture_authority(
        config: &CoinbaseDirectConfig,
        generation: u64,
    ) -> TestResult<(
        AuthoritativeSourceRegistry,
        CurrentSourceSession,
        RawFrameFactory,
    )> {
        let mut registry =
            AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
                Arc::new(FixtureAuthorizationSubjectResolver),
            )?;
        let registered =
            registry.register(config.metadata().clone(), Timestamp::from_unix_nanos(1))?;
        let session = registry.begin_session(
            &registered,
            SessionId::new(id("coinbase-direct-test-session")?),
            ConnectionGeneration::new(generation)?,
            Timestamp::from_unix_nanos(1),
        )?;
        let frames = registry.take_raw_frame_factory(&session)?;
        Ok((registry, session, frames))
    }

    #[derive(Debug)]
    struct FixtureAuthorizationSubjectResolver;

    impl AuthorizationSubjectResolver for FixtureAuthorizationSubjectResolver {
        fn resolve_subject_record(
            &self,
            mode: AuthorizationMode,
            _evidence: EvidenceDigest,
        ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
            if mode != AuthorizationMode::UserAuthorized {
                return Err(AuthorizationSubjectResolutionError::UnsupportedMode);
            }
            SourceIdentifier::try_from("coinbase-direct-fixture-credential")
                .map_err(|_| AuthorizationSubjectResolutionError::EvidenceUnresolved)
        }
    }

    fn capture(
        frames: &mut RawFrameFactory,
        url: &str,
        body: &[u8],
    ) -> TestResult<market_squawk_sources::SegmentedHttpResponseCapture> {
        let mut builder = frames.try_http_response_builder(
            HttpCaptureMethod::Get,
            url,
            200,
            Some(u64::try_from(body.len())?),
            16 * 1024 * 1024,
            8,
        )?;
        let split = body.len().saturating_div(2).max(1).min(body.len());
        builder.try_push_segment(Bytes::copy_from_slice(&body[..split]))?;
        if split < body.len() {
            builder.try_push_segment(Bytes::copy_from_slice(&body[split..]))?;
        }
        Ok(builder.finish()?)
    }

    fn decode_event(
        decoder: &CoinbaseDirectDecoder,
        frames: &mut RawFrameFactory,
        session: &CurrentSourceSession,
        payload: &'static [u8],
    ) -> TestResult<market_squawk_sources::ProviderOrderEvent> {
        let frame = frames.try_frame(TransportFrameKind::Text, Bytes::from_static(payload))?;
        match decoder.decode(&session.validate_live_frame(&frame)?)? {
            CoinbaseDirectDecodeOutcome::Sequenced(event) => Ok(event),
            CoinbaseDirectDecodeOutcome::NonBook(_) => {
                Err("expected a sequenced direct event".into())
            }
        }
    }

    #[derive(Debug)]
    struct FixtureSigner;

    impl CoinbaseDirectSigningCapability for FixtureSigner {
        fn sign(
            &self,
            request: CoinbaseDirectSigningRequest<'_>,
        ) -> Result<CoinbaseDirectAuthentication, CoinbaseDirectSigningError> {
            assert_eq!(request.method(), "GET");
            assert_eq!(request.path(), "/users/self/verify");
            CoinbaseDirectAuthentication::try_new(
                "fixture-key".to_owned(),
                "fixture-pass".to_owned(),
                "fixture-signature".to_owned(),
            )
        }
    }

    #[test]
    fn direct_profile_is_distinct_authenticated_sequenced_and_checksum_truthful() -> TestResult {
        let config = config()?;
        assert_eq!(
            config.websocket_endpoint(),
            COINBASE_DIRECT_WEBSOCKET_ENDPOINT
        );
        assert_eq!(
            config.metadata().authorization().mode(),
            AuthorizationMode::UserAuthorized
        );
        assert_eq!(
            config.metadata().capabilities().sequence(),
            SequenceCapability::Provided
        );
        assert_eq!(
            config.metadata().capabilities().checksum(),
            ChecksumCapability::Unsupported
        );
        assert!(
            config
                .metadata()
                .network_policy()
                .authorize(config.snapshot_url())
                .is_ok()
        );
        assert!(
            config
                .metadata()
                .network_policy()
                .authorize(config.product_url())
                .is_ok()
        );
        assert!(
            config
                .metadata()
                .network_policy()
                .authorize("https://api.exchange.coinbase.com/products/BTC-USD/book?level=2")
                .is_err()
        );
        assert!(
            config
                .metadata()
                .network_policy()
                .authorize("https://api.exchange.coinbase.com/products/BTC-USD/book")
                .is_err()
        );
        assert!(
            config
                .metadata()
                .network_policy()
                .authorize("wss://ws-feed.exchange.coinbase.com")
                .is_err()
        );
        let subscription = config.try_signed_subscription(1_721_847_600, &FixtureSigner)?;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(subscription.as_str())?["channels"][0],
            "full"
        );
        assert!(!format!("{subscription:?}").contains("fixture-pass"));
        for (case, max_concurrent, short_window_requests, long_window_requests) in [
            ("concurrency", 1, 3, 3),
            ("primary window", 2, 2, 3),
            ("additional window", 2, 3, 2),
        ] {
            let outcome = config_with_budget(
                max_concurrent,
                &[
                    (short_window_requests, 1_000_000_000),
                    (long_window_requests, 2_000_000_000),
                ],
            )?;
            assert!(
                matches!(outcome, Err(CoinbaseConfigError::InvalidDirectBudget)),
                "{case} unexpectedly admitted an unusable Direct budget: {outcome:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn full_decoder_classifies_cursor_and_rejects_unknown_sequenced_types() -> TestResult {
        let config = config()?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let (_registry, session, mut frames) = capture_authority(&config, 1)?;
        let received_frame = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"received","time":"2026-07-24T21:34:10.600Z","product_id":"BTC-USD","sequence":11,"order_id":"order-a","order_type":"limit","size":"1.00","price":"100.00","side":"buy"}"#,
            ),
        )?;
        let validated = session.validate_live_frame(&received_frame)?;
        let received = match decoder.decode(&validated)? {
            CoinbaseDirectDecodeOutcome::Sequenced(event) => event,
            CoinbaseDirectDecodeOutcome::NonBook(_) => {
                return Err("received message was not sequenced".into());
            }
        };
        assert!(matches!(
            received.kind(),
            ProviderOrderEventKind::CursorOnly(_)
        ));
        assert_eq!(received.sequence().get(), 11);
        assert_eq!(received.wire_bytes(), received_frame.payload().len());
        assert_eq!(received.evidence().frame_id(), received_frame.frame_id());
        assert_eq!(
            received.evidence().payload_digest().bytes(),
            <[u8; 32]>::from(sha2::Sha256::digest(received_frame.payload()))
        );
        assert!(
            received
                .evidence()
                .binding()
                .shares_allocation_with(received_frame.binding())
        );
        let mut byte_bounded_owner = DirectOrderBook::try_new(
            session.generation(),
            config.product().clone(),
            config.execution_terms(),
            DirectBookLimits::try_new(4, 4, 2, received.wire_bytes() - 1, 2)?,
        )?;
        assert_eq!(
            byte_bounded_owner.try_queue(received),
            Err(market_squawk_live::DirectOrderBookError::QueueBytesExceeded)
        );
        assert_eq!(byte_bounded_owner.phase(), DirectSyncPhase::Quarantined);

        let unknown_frame = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"new_state_changing_message","time":"2026-07-24T21:34:10.601Z","product_id":"BTC-USD","sequence":12}"#,
            ),
        )?;
        assert_eq!(
            decoder.decode(&session.validate_live_frame(&unknown_frame)?),
            Err(CoinbaseDirectDecodeError::UnknownSequencedMessage)
        );

        let activate_frame = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"activate","product_id":"BTC-USD","timestamp":"1483736448.299000","user_id":"user-a","profile_id":"profile-a","order_id":"stop-a","stop_type":"entry","side":"buy","stop_price":"80.00","size":"2.00","funds":"50.00","private":true}"#,
            ),
        )?;
        let CoinbaseDirectDecodeOutcome::NonBook(activate) =
            decoder.decode(&session.validate_live_frame(&activate_frame)?)?
        else {
            return Err("activate message entered the sequenced path".into());
        };
        let CoinbaseDirectNonBookKind::Activate(activation) = activate.kind() else {
            return Err("activate message was misclassified".into());
        };
        assert_eq!(activation.stop_type(), CoinbaseDirectStopType::Entry);
        assert_eq!(activation.side(), ProviderBookSide::Bid);
        assert_eq!(activation.stop_price(), PriceTicks::new(8_000));
        assert_eq!(activation.size(), QuantityLots::new(200_000_000)?);
        assert_eq!(
            activation.provider_timestamp().as_str(),
            "1483736448.299000"
        );
        assert_eq!(activate.evidence().frame_id(), activate_frame.frame_id());

        let private_received_frame = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"received","time":"2026-07-24T21:34:10.602Z","product_id":"BTC-USD","order_id":"private-order","order_type":"limit","size":"1.00","price":"100.00","side":"buy","user_id":"user-a","profile_id":"profile-a"}"#,
            ),
        )?;
        assert!(matches!(
            decoder.decode(&session.validate_live_frame(&private_received_frame)?)?,
            CoinbaseDirectDecodeOutcome::NonBook(super::CoinbaseDirectNonBookEvent {
                kind: CoinbaseDirectNonBookKind::Received(_),
                ..
            })
        ));

        let tpsl_frame = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"new_price":"8245","order_id":"tpsl-a","type":"change","side":"sell","old_price":"9785","reason":"tpsl_triggered"}"#,
            ),
        )?;
        let CoinbaseDirectDecodeOutcome::NonBook(tpsl_event) =
            decoder.decode(&session.validate_live_frame(&tpsl_frame)?)?
        else {
            return Err("TPSL owner lifecycle entered the public sequence path".into());
        };
        let CoinbaseDirectNonBookKind::TpslTriggered(tpsl) = tpsl_event.kind() else {
            return Err("TPSL owner lifecycle was misclassified".into());
        };
        assert_eq!(tpsl.order_id(), &id("tpsl-a")?);
        assert_eq!(tpsl.side(), ProviderBookSide::Ask);
        assert_eq!(tpsl.old_price(), PriceTicks::new(978_500));
        assert_eq!(tpsl.new_price(), PriceTicks::new(824_500));
        assert_eq!(tpsl_event.evidence().frame_id(), tpsl_frame.frame_id());

        let sequenced_tpsl = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"new_price":"8245","order_id":"tpsl-a","type":"change","side":"sell","old_price":"9785","reason":"tpsl_triggered","sequence":13}"#,
            ),
        )?;
        assert_eq!(
            decoder.decode(&session.validate_live_frame(&sequenced_tpsl)?),
            Err(CoinbaseDirectDecodeError::PrivateLifecycleSequence)
        );

        for payload in [
            br#"{"type":"change","reason":"modify_order","time":"2026-07-24T21:34:10.603Z","order_id":"private-order","side":"buy","product_id":"BTC-USD","old_size":"1.00","new_size":"1.00","old_price":"100.00","new_price":"99.00","user_id":"user-a","profile_id":"profile-a"}"#
                .as_slice(),
            br#"{"type":"change","reason":"STP","time":"2026-07-24T21:34:10.604Z","order_id":"private-order","side":"buy","product_id":"BTC-USD","old_size":"1.00","new_size":"0.50","price":"100.00"}"#
                .as_slice(),
            br#"{"type":"open","time":"2026-07-24T21:34:10.605Z","product_id":"BTC-USD","order_id":"private-order","price":"100.00","remaining_size":"1.00","side":"buy"}"#
                .as_slice(),
            br#"{"type":"match","trade_id":12,"maker_order_id":"private-order","taker_order_id":"taker-a","time":"2026-07-24T21:34:10.606Z","product_id":"BTC-USD","size":"0.50","price":"100.00","side":"buy"}"#
                .as_slice(),
            br#"{"type":"done","time":"2026-07-24T21:34:10.607Z","product_id":"BTC-USD","order_id":"private-order","reason":"canceled","price":"100.00","remaining_size":"0.50","side":"buy"}"#
                .as_slice(),
        ] {
            let frame = frames.try_frame(
                TransportFrameKind::Text,
                Bytes::from_static(payload),
            )?;
            assert_eq!(
                decoder.decode(&session.validate_live_frame(&frame)?),
                Err(CoinbaseDirectDecodeError::UnsequencedBookMutation)
            );
        }
        Ok(())
    }

    #[test]
    fn snapshot_streams_orders_and_required_time_into_non_authoritative_owner() -> TestResult {
        let config = config()?;
        let (mut registry, session, mut frames) = capture_authority(&config, 1)?;
        let crossed_capture = capture(
            &mut frames,
            config.snapshot_url(),
            br#"{"sequence":9,"bids":[["101.00","1.00","crossed-bid"]],"asks":[["101.00","1.00","crossed-ask"]],"time":"2026-07-24T21:34:10.596119497Z","auction_mode":false}"#,
        )?;
        let mut crossed_owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        assert_eq!(
            CoinbaseDirectSnapshotDecoder::try_new(&config)?
                .decode_into(&crossed_capture, &mut crossed_owner),
            Err(CoinbaseDirectSnapshotError::Owner(
                market_squawk_live::DirectOrderBookError::CrossedBook
            ))
        );
        assert_eq!(crossed_owner.phase(), DirectSyncPhase::Quarantined);

        let body = br#"{"sequence":10,"bids":[["100.00","5.00","bid-a"]],"asks":[["101.00","4.00","ask-a"]],"time":"2026-07-24T21:34:10.596119498Z","auction_mode":false}"#;
        let capture = capture(&mut frames, config.snapshot_url(), body)?;
        let mut owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        CoinbaseDirectSnapshotDecoder::try_new(&config)?.decode_into(&capture, &mut owner)?;
        assert_eq!(owner.phase(), DirectSyncPhase::SnapshotLoaded);
        assert!(owner.published_book().is_none());
        assert_eq!(
            owner.candidate_sequence().map(|value| value.get()),
            Some(10)
        );
        assert_eq!(
            owner
                .snapshot_receipt()
                .map(|receipt| receipt.body_digest()),
            Some(capture.receipt().body_digest())
        );
        assert_eq!(
            capture.receipt().connection_generation(),
            session.generation()
        );
        assert!(capture.receipt().received_at() >= session.started_at());
        owner.begin_replay()?;
        owner.finish_replay()?;
        assert_eq!(owner.phase(), DirectSyncPhase::Healthy);
        assert!(owner.published_book().is_some());

        let mut mutation_owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        CoinbaseDirectSnapshotDecoder::try_new(&config)?
            .decode_into(&capture, &mut mutation_owner)?;
        mutation_owner.begin_replay()?;
        mutation_owner.finish_replay()?;
        let crossing_open = decode_event(
            &CoinbaseDirectDecoder::try_new(&config)?,
            &mut frames,
            &session,
            br#"{"type":"open","time":"2026-07-24T21:34:10.600Z","product_id":"BTC-USD","sequence":11,"order_id":"crossing-open","price":"101.00","remaining_size":"1.00","side":"buy"}"#,
        )?;
        assert_eq!(
            mutation_owner.try_apply_live(crossing_open),
            Err(market_squawk_live::DirectOrderBookError::CrossedBook)
        );
        assert_eq!(mutation_owner.phase(), DirectSyncPhase::Quarantined);

        registry.end_session(&session, Timestamp::from_unix_nanos(2))?;
        assert!(owner.published_book().is_none());
        assert_eq!(owner.phase(), DirectSyncPhase::Quarantined);
        Ok(())
    }

    #[test]
    fn product_response_supplies_actual_status_and_increment_evidence() -> TestResult {
        let config = config()?;
        let (_registry, session, mut frames) = capture_authority(&config, 1)?;
        let capture = capture(
            &mut frames,
            config.product_url(),
            br#"{"id":"BTC-USD","status":"online","base_increment":"0.00000001","quote_increment":"0.01","trading_disabled":false,"cancel_only":false,"post_only":false,"limit_only":false,"auction_mode":false}"#,
        )?;
        let evidence = config.decode_product_evidence(&capture)?;
        assert_eq!(evidence.trading_status(), TradingStatus::Active);
        assert_eq!(evidence.base_increment().value().as_str(), "0.00000001");
        assert_eq!(evidence.quote_increment().value().as_str(), "0.01");
        assert_eq!(
            evidence.capture_receipt().body_digest(),
            capture.receipt().body_digest()
        );
        assert_eq!(evidence.observed_at(), capture.receipt().received_at());
        assert_eq!(
            evidence.capture_receipt().connection_generation(),
            session.generation()
        );
        assert_eq!(evidence.capture_receipt().final_url(), config.product_url());
        Ok(())
    }

    #[test]
    fn auction_evidence_is_retained_but_cannot_establish_execution_authority() -> TestResult {
        let config = config()?;
        let (_registry, _session, mut frames) = capture_authority(&config, 1)?;
        let product_capture = capture(
            &mut frames,
            config.product_url(),
            br#"{"id":"BTC-USD","status":"online","base_increment":"0.00000001","quote_increment":"0.01","trading_disabled":false,"cancel_only":false,"post_only":false,"limit_only":false,"auction_mode":true}"#,
        )?;
        let product = config.decode_product_evidence(&product_capture)?;
        assert!(product.auction_mode());
        assert_eq!(product.trading_status(), TradingStatus::Inactive);

        let snapshot_capture = capture(
            &mut frames,
            config.snapshot_url(),
            br#"{"sequence":10,"bids":[["100.00","5.00","bid-a"]],"asks":[["101.00","4.00","ask-a"]],"time":"2026-07-24T21:34:10.596119498Z","auction_mode":true,"auction":{"indicative_open_price":"100.50","indicative_open_size":"1.25","indicative_bid_price":"100.00","indicative_bid_size":"5.00","indicative_ask_price":"101.00","indicative_ask_size":"4.00","auction_status":"CAN_OPEN"}}"#,
        )?;
        let mut owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        assert_eq!(
            CoinbaseDirectSnapshotDecoder::try_new(&config)?
                .decode_into(&snapshot_capture, &mut owner),
            Err(CoinbaseDirectSnapshotError::AuctionMode)
        );
        assert_eq!(owner.phase(), DirectSyncPhase::Quarantined);
        assert!(owner.published_book().is_none());
        Ok(())
    }

    #[test]
    fn snapshot_receipt_from_another_generation_cannot_advance_health() -> TestResult {
        let config = config()?;
        let (_registry, _session, mut frames) = capture_authority(&config, 1)?;
        let capture = capture(
            &mut frames,
            config.snapshot_url(),
            br#"{"sequence":10,"bids":[["100.00","5.00","bid-a"]],"asks":[["101.00","4.00","ask-a"]],"time":"2026-07-24T21:34:10.596119498Z","auction_mode":false}"#,
        )?;
        let mut owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(2)?,
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        assert_eq!(
            CoinbaseDirectSnapshotDecoder::try_new(&config)?.decode_into(&capture, &mut owner),
            Err(CoinbaseDirectSnapshotError::Owner(
                market_squawk_live::DirectOrderBookError::SnapshotGenerationMismatch
            ))
        );
        assert_eq!(owner.phase(), DirectSyncPhase::Quarantined);
        assert!(owner.published_book().is_none());
        Ok(())
    }

    #[test]
    fn modify_order_reprices_atomically_and_authenticated_additions_are_typed() -> TestResult {
        let config = config()?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let (_registry, session, mut frames) = capture_authority(&config, 1)?;
        let modified = decode_event(
            &decoder,
            &mut frames,
            &session,
            br#"{"type":"change","reason":"modify_order","time":"2026-07-24T21:34:10.600Z","sequence":11,"order_id":"bid-a","side":"buy","product_id":"BTC-USD","old_size":"5.00","new_size":"4.00","old_price":"100.00","new_price":"99.50","user_id":"user-a","profile_id":"profile-a"}"#,
        )?;
        let mut owner = DirectOrderBook::try_new(
            session.generation(),
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        owner.try_queue(modified)?;
        let snapshot = capture(
            &mut frames,
            config.snapshot_url(),
            br#"{"sequence":10,"bids":[["100.00","5.00","bid-a"]],"asks":[["101.00","4.00","ask-a"]],"time":"2026-07-24T21:34:10.596119498Z","auction_mode":false}"#,
        )?;
        CoinbaseDirectSnapshotDecoder::try_new(&config)?.decode_into(&snapshot, &mut owner)?;
        owner.begin_replay()?;
        assert!(owner.replay_next()?);
        assert!(!owner.replay_next()?);
        owner.finish_replay()?;
        assert_eq!(owner.phase(), DirectSyncPhase::Healthy);
        let published = owner.published_book().ok_or("missing healthy book")?;
        let best_bid = published.bids().next().ok_or("missing best bid")?;
        assert_eq!(best_bid.price(), PriceTicks::new(9_950));
        assert_eq!(best_bid.quantity(), QuantityLots::new(400_000_000)?);

        let matched = decode_event(
            &decoder,
            &mut frames,
            &session,
            br#"{"type":"match","trade_id":12,"sequence":12,"maker_order_id":"bid-a","taker_order_id":"taker-a","time":"2026-07-24T21:34:10.601Z","product_id":"BTC-USD","size":"1.00","price":"99.50","side":"buy","maker_user_id":"maker-user","user_id":"maker-user","maker_profile_id":"maker-profile","profile_id":"maker-profile","maker_fee_rate":"0.001"}"#,
        )?;
        assert!(matches!(
            matched.kind(),
            ProviderOrderEventKind::Match { .. }
        ));
        owner.try_apply_live(matched)?;
        assert_eq!(
            owner
                .published_book()
                .and_then(|book| book.bids().next())
                .map(|level| level.quantity()),
            Some(QuantityLots::new(300_000_000)?)
        );

        let public_modify = decode_event(
            &decoder,
            &mut frames,
            &session,
            br#"{"type":"change","reason":"modify_order","time":"2026-07-24T21:34:10.602Z","sequence":13,"order_id":"bid-a","side":"buy","product_id":"BTC-USD","old_size":"3.00","new_size":"3.00","old_price":"99.50","new_price":"99.25"}"#,
        )?;
        assert!(matches!(
            public_modify.kind(),
            ProviderOrderEventKind::Change {
                reason: ProviderOrderChangeReason::ModifyOrder,
                ..
            }
        ));
        owner.try_apply_live(public_modify)?;
        assert_eq!(
            owner
                .published_book()
                .and_then(|book| book.bids().next())
                .map(|level| level.price()),
            Some(PriceTicks::new(9_925))
        );

        let done = decode_event(
            &decoder,
            &mut frames,
            &session,
            br#"{"type":"done","time":"2026-07-24T21:34:10.603Z","product_id":"BTC-USD","sequence":14,"order_id":"bid-a","reason":"canceled","price":"99.25","remaining_size":"3.00","side":"buy","user_id":"user-a","profile_id":"profile-a","cancel_reason":"101"}"#,
        )?;
        assert!(matches!(done.kind(), ProviderOrderEventKind::Done { .. }));
        owner.try_apply_live(done)?;
        assert!(
            owner
                .published_book()
                .is_some_and(|book| book.bids().next().is_none())
        );

        let invalid_match = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"match","trade_id":15,"sequence":15,"maker_order_id":"bid-a","taker_order_id":"taker-a","time":"2026-07-24T21:34:10.604Z","product_id":"BTC-USD","size":"1.00","price":"99.50","side":"buy","maker_fee_rate":[]}"#,
            ),
        )?;
        assert_eq!(
            decoder.decode(&session.validate_live_frame(&invalid_match)?),
            Err(CoinbaseDirectDecodeError::Schema)
        );

        let invalid_stp = frames.try_frame(
            TransportFrameKind::Text,
            Bytes::from_static(
                br#"{"type":"change","reason":"STP","time":"2026-07-24T21:34:10.605Z","sequence":16,"order_id":"bid-a","side":"buy","product_id":"BTC-USD","price":"99.25","old_size":"3.00","new_size":"4.00"}"#,
            ),
        )?;
        assert_eq!(
            decoder.decode(&session.validate_live_frame(&invalid_stp)?),
            Err(CoinbaseDirectDecodeError::Numeric)
        );
        Ok(())
    }
}
