//! Authenticated Coinbase Exchange Direct Market Data profile and level-3 decoders.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU64;

use chrono::DateTime;
use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, IntegrityRule, LiveEventClass, MarketDepth,
    ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion,
    SequenceCapability, SequenceNumber, SequenceValidationRule, SnapshotApplicability, SourceId,
    SourceIdentifier, Timestamp, TradingStatus, VenueId,
};
use market_squawk_live::{DirectBookLimits, DirectOrderBook, DirectOrderBookError};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile,
    CoverageTopology, EndpointPolicy, FreshnessPolicy, HistoricalCapability, HttpCaptureMethod,
    HttpRequestBounds, InstrumentCoverage, LiveCoverageDeclaration, LiveCoverageRule,
    LiveProtocolProfile, NetworkAccessPolicy, PathScope, ProviderBookLevel, ProviderBookSide,
    ProviderBudgetPolicy, ProviderCursorOnlyReason, ProviderDecimalLexeme, ProviderNumericPolicy,
    ProviderOrderEvent, ProviderOrderEventKind, ProviderOrderRecord, ProviderPrice,
    ProviderQuantity, QueryParameterRule, SegmentedHttpResponseCapture,
    SegmentedHttpResponseReceipt, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

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
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
        limits: CoinbaseDirectLimits,
    ) -> Result<Self, CoinbaseConfigError> {
        if authorization.mode() != AuthorizationMode::UserAuthorized {
            return Err(CoinbaseConfigError::InvalidDirectAuthorization);
        }
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
                    direct_rule("coinbase-exchange-direct-auction-unused")?,
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
        let payload =
            serde_json::to_string(&wire).map_err(|_| CoinbaseDirectSigningError::Serialization)?;
        if payload.len() > MAX_SIGNED_SUBSCRIPTION_BYTES {
            return Err(CoinbaseDirectSigningError::SubscriptionTooLarge);
        }
        Ok(CoinbaseSignedSubscription(payload.into_boxed_str()))
    }

    /// Decodes current product status and increments from an exact captured REST response.
    pub fn decode_product_evidence(
        &self,
        capture: &SegmentedHttpResponseCapture,
    ) -> Result<CoinbaseDirectProductEvidence, CoinbaseDirectProductError> {
        validate_http_capture(
            capture,
            self.product_url(),
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
        let status = SourceIdentifier::try_from(wire.status.as_str())
            .map_err(|_| CoinbaseDirectProductError::Status)?;
        let trading_status = if wire.status == "online"
            && !wire.trading_disabled
            && !wire.cancel_only
            && !wire.post_only
            && !wire.limit_only
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
            capture: capture.receipt().clone(),
        })
    }
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
#[derive(Clone)]
pub struct CoinbaseDirectAuthentication {
    key: Box<str>,
    passphrase: Box<str>,
    signature: Box<str>,
}

impl CoinbaseDirectAuthentication {
    /// Constructs bounded authentication output from the trusted signing boundary.
    pub fn try_new(
        key: &str,
        passphrase: &str,
        signature: &str,
    ) -> Result<Self, CoinbaseDirectSigningError> {
        for value in [key, passphrase, signature] {
            if value.is_empty()
                || value.len() > MAX_SIGNING_FIELD_BYTES
                || value.chars().any(char::is_control)
            {
                return Err(CoinbaseDirectSigningError::InvalidAuthentication);
            }
        }
        Ok(Self {
            key: key.to_owned().into_boxed_str(),
            passphrase: passphrase.to_owned().into_boxed_str(),
            signature: signature.to_owned().into_boxed_str(),
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
#[derive(Clone)]
pub struct CoinbaseSignedSubscription(Box<str>);

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

/// Exact classifier for sequenced Coinbase `full` messages.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectDecoder {
    product: ProviderProduct,
    max_frame_bytes: usize,
}

impl CoinbaseDirectDecoder {
    /// Binds the decoder to one immutable Direct product profile.
    pub fn try_new(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseConfigError> {
        Ok(Self {
            product: config.product().clone(),
            max_frame_bytes: config.limits.websocket().max_frame_bytes(),
        })
    }

    /// Decodes one already-captured text frame into a sequenced level-3 event.
    ///
    /// Unknown sequenced types and every schema/invariant violation return an error that requires
    /// a completely fresh snapshot/generation.
    pub fn decode_captured_text(
        &self,
        payload: &[u8],
    ) -> Result<ProviderOrderEvent, CoinbaseDirectDecodeError> {
        if payload.is_empty() || payload.len() > self.max_frame_bytes {
            return Err(CoinbaseDirectDecodeError::FrameTooLarge);
        }
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| CoinbaseDirectDecodeError::Schema)?;
        let object = value.as_object().ok_or(CoinbaseDirectDecodeError::Schema)?;
        let kind = required_text(object, "type")?;
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
                ProviderOrderEventKind::Open(ProviderOrderRecord::new(
                    parse_order_id(object, "order_id")?,
                    parse_direct_side(required_text(object, "side")?)?,
                    ProviderBookLevel::new(
                        parse_direct_price(required_text(object, "price")?)?,
                        parse_direct_quantity(required_text(object, "remaining_size")?)?,
                    ),
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
                    ],
                    &[
                        "type",
                        "time",
                        "product_id",
                        "sequence",
                        "maker_order_id",
                        "size",
                    ],
                )?;
                ProviderOrderEventKind::Match {
                    maker_order_id: parse_order_id(object, "maker_order_id")?,
                    quantity: parse_direct_quantity(required_text(object, "size")?)?,
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
                ProviderOrderEventKind::Done {
                    order_id: parse_order_id(object, "order_id")?,
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
                    ],
                    &["type", "time", "product_id", "sequence", "order_id"],
                )?;
                let new_quantity = match (object.get("new_size"), object.get("new_funds")) {
                    (Some(value), None) => Some(parse_direct_quantity(
                        value.as_str().ok_or(CoinbaseDirectDecodeError::Schema)?,
                    )?),
                    (None, Some(value)) if value.is_string() => None,
                    _ => return Err(CoinbaseDirectDecodeError::Schema),
                };
                ProviderOrderEventKind::Change {
                    order_id: parse_order_id(object, "order_id")?,
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
            payload.len(),
        )
        .map_err(|_| CoinbaseDirectDecodeError::FrameTooLarge)
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
    let allowed = allowed.iter().copied().collect::<BTreeSet<_>>();
    if object.keys().any(|field| !allowed.contains(field.as_str()))
        || required.iter().any(|field| !object.contains_key(*field))
    {
        Err(CoinbaseDirectDecodeError::Schema)
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

/// A `full` frame that cannot safely advance the maintained product cursor.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectDecodeError {
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
    /// An unsequenced message is outside this order-event decoder.
    #[error("Coinbase Direct message type is unsupported")]
    UnsupportedMessage,
}

/// Streaming level-3 snapshot decoder bound to one exact Direct profile.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectSnapshotDecoder {
    product: ProviderProduct,
    snapshot_url: Box<str>,
    limits: CoinbaseDirectLimits,
}

impl CoinbaseDirectSnapshotDecoder {
    /// Constructs the snapshot decoder from immutable direct configuration.
    pub fn try_new(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseConfigError> {
        Ok(Self {
            product: config.product().clone(),
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
        if let Err(error) = validate_http_capture(
            capture,
            &self.snapshot_url,
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
        owner.begin_snapshot(SequenceNumber::new(metadata.sequence))?;
        let parsed = {
            let mut deserializer = serde_json::Deserializer::from_reader(capture.reader());
            let decoded = SnapshotRowsSeed { owner }.deserialize(&mut deserializer);
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
    #[serde(rename = "bids")]
    _bids: IgnoredAny,
    #[serde(rename = "asks")]
    _asks: IgnoredAny,
}

struct SnapshotRowsSeed<'a> {
    owner: &'a mut DirectOrderBook,
}

impl<'de> DeserializeSeed<'de> for SnapshotRowsSeed<'_> {
    type Value = usize;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SnapshotRowsVisitor { owner: self.owner })
    }
}

struct SnapshotRowsVisitor<'a> {
    owner: &'a mut DirectOrderBook,
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
                        })?)
                        .ok_or_else(|| A::Error::custom("snapshot order count overflow"))?;
                    seen_bids = true;
                }
                "asks" if !seen_asks => {
                    count = count
                        .checked_add(map.next_value_seed(SnapshotSideSeed {
                            owner: &mut *self.owner,
                            side: ProviderBookSide::Ask,
                        })?)
                        .ok_or_else(|| A::Error::custom("snapshot order count overflow"))?;
                    seen_asks = true;
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
        })
    }
}

struct SnapshotSideVisitor<'a> {
    owner: &'a mut DirectOrderBook,
    side: ProviderBookSide,
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
            let price = parse_direct_price(&price)
                .map_err(|_| A::Error::custom("invalid snapshot price"))?;
            let quantity = parse_direct_quantity(&quantity)
                .map_err(|_| A::Error::custom("invalid snapshot quantity"))?;
            self.owner
                .try_push_snapshot_order(ProviderOrderRecord::new(
                    order_id,
                    self.side,
                    ProviderBookLevel::new(price, quantity),
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
    max_body_bytes: u64,
    max_segments: usize,
) -> Result<(), CoinbaseDirectCaptureError> {
    let receipt = capture.receipt();
    if receipt.method() != HttpCaptureMethod::Get
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
    use std::time::Duration;

    use bytes::Bytes;
    use market_squawk_domain::{
        AuthorizationBasis, ChecksumCapability, ConnectionGeneration, DigestAlgorithm,
        EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId, MetadataRevision,
        ProviderProduct, RevisionBoundPayloadEvidence, SequenceCapability, SourceId,
        SourceIdentifier, Timestamp, TradingStatus,
    };
    use market_squawk_live::{DirectBookLimits, DirectOrderBook, DirectSyncPhase};
    use market_squawk_sources::{
        AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
        HttpCaptureMethod, ProviderBudgetPolicy, ProviderOrderEventKind,
        SegmentedHttpResponseBuilder,
    };

    use crate::{
        COINBASE_DIRECT_WEBSOCKET_ENDPOINT, CoinbaseDirectAuthentication, CoinbaseDirectConfig,
        CoinbaseDirectDecodeError, CoinbaseDirectDecoder, CoinbaseDirectLimits,
        CoinbaseDirectSigningCapability, CoinbaseDirectSigningError, CoinbaseDirectSigningRequest,
        CoinbaseDirectSnapshotDecoder, CoinbaseProductMapping, CoinbaseTransportLimits,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

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
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(id("coinbase-read-only-market-data-account")?),
            evidence(2),
            effective,
        );
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(id("coinbase-exchange")?, &authorization)?,
            NonZeroU32::new(8).ok_or("zero request budget")?,
            NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
            NonZeroU16::new(1).ok_or("zero concurrency")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("zero initial backoff")?,
                NonZeroU64::new(1_000_000_000).ok_or("zero maximum backoff")?,
                1_000,
            )?,
        )?;
        CoinbaseDirectConfig::try_new(
            SourceId::try_from("coinbase-exchange-direct")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(id("coinbase-direct-2026-07-24")?),
                evidence(3),
            ),
            authorization,
            evidence(4),
            effective,
            CoinbaseProductMapping::try_new(
                ProviderProduct::new(id("BTC-USD")?),
                InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            )?,
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
        )
        .map_err(Into::into)
    }

    fn capture(
        url: &str,
        body: &[u8],
    ) -> TestResult<market_squawk_sources::SegmentedHttpResponseCapture> {
        let mut builder = SegmentedHttpResponseBuilder::try_new(
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
                "fixture-key",
                "fixture-pass",
                "fixture-signature",
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
        Ok(())
    }

    #[test]
    fn full_decoder_classifies_cursor_and_rejects_unknown_sequenced_types() -> TestResult {
        let config = config()?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let received = decoder.decode_captured_text(
            br#"{"type":"received","time":"2026-07-24T21:34:10.600Z","product_id":"BTC-USD","sequence":11,"order_id":"order-a","order_type":"limit","size":"1.00","price":"100.00","side":"buy"}"#,
        )?;
        assert!(matches!(
            received.kind(),
            ProviderOrderEventKind::CursorOnly(_)
        ));
        assert_eq!(received.sequence().get(), 11);
        assert_eq!(
            decoder.decode_captured_text(
                br#"{"type":"new_state_changing_message","time":"2026-07-24T21:34:10.601Z","product_id":"BTC-USD","sequence":12}"#
            ),
            Err(CoinbaseDirectDecodeError::UnknownSequencedMessage)
        );
        Ok(())
    }

    #[test]
    fn snapshot_streams_orders_and_required_time_into_non_authoritative_owner() -> TestResult {
        let config = config()?;
        let body = br#"{"sequence":10,"bids":[["100.00","5.00","bid-a"]],"asks":[["101.00","4.00","ask-a"]],"time":"2026-07-24T21:34:10.596119498Z"}"#;
        let capture = capture(config.snapshot_url(), body)?;
        let mut owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            config.product().clone(),
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
        owner.begin_replay()?;
        owner.finish_replay()?;
        assert_eq!(owner.phase(), DirectSyncPhase::Healthy);
        assert!(owner.published_book().is_some());
        Ok(())
    }

    #[test]
    fn product_response_supplies_actual_status_and_increment_evidence() -> TestResult {
        let config = config()?;
        let capture = capture(
            config.product_url(),
            br#"{"id":"BTC-USD","status":"online","base_increment":"0.00000001","quote_increment":"0.01","trading_disabled":false,"cancel_only":false,"post_only":false,"limit_only":false}"#,
        )?;
        let evidence = config.decode_product_evidence(&capture)?;
        assert_eq!(evidence.trading_status(), TradingStatus::Active);
        assert_eq!(evidence.base_increment().value().as_str(), "0.00000001");
        assert_eq!(evidence.quote_increment().value().as_str(), "0.01");
        assert_eq!(
            evidence.capture_receipt().body_digest(),
            capture.receipt().body_digest()
        );
        Ok(())
    }
}
