//! Evidence-bound authenticated level-3 configuration and secret-safe subscription encoding.

use std::fmt;
use std::num::{NonZeroU16, NonZeroUsize};

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    MarketDepth, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion,
    SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId, SourceIdentifier, VenueId,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ChecksumAlgorithm, ChecksumBookScope,
    ChecksumValidationProfile, CoverageTopology, FreshnessPolicy, HistoricalCapability,
    InstrumentCoverage, InstrumentCoverageMembership, LiveCoverageDeclaration, LiveCoverageRule,
    LiveProtocolProfile, MAX_RAW_FRAME_BYTES, NetworkAccessPolicy, ProviderBudgetPolicy,
    ProviderNumericPolicy, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataError,
    SourceMetadataInput, SourceMetadataProvider, SourceProtocolProfile,
};
use serde::Serialize;
use thiserror::Error;
use url::Url;

/// Authenticated Kraken Spot WebSocket v2 endpoint used by the `level3` channel.
pub const KRAKEN_L3_WEBSOCKET_ENDPOINT: &str = "wss://ws.kraken.com/v2";
/// Private REST endpoint from which the central credential authority obtains a short-lived token.
pub const KRAKEN_L3_GET_TOKEN_ENDPOINT: &str =
    "https://api.kraken.com/0/private/GetWebSocketsToken";
/// Closed identity for Kraken's individual-order checksum canonicalization.
pub const KRAKEN_L3_CHECKSUM_CANONICALIZATION_ID: &str = "kraken-ws-v2-level3-checksum-v1";
/// Closed identity for the top-ten-level, order-queue checksum scope.
pub const KRAKEN_L3_CHECKSUM_SCOPE_ID: &str =
    "asks-low-to-high-bids-high-to-low-top-10-levels-order-queue";
/// Reviewed L3 qualification-policy revision.
pub const KRAKEN_L3_QUALIFICATION_POLICY_VERSION: u32 = 1;
/// SHA-256 of the canonical authenticated L3 qualification decision in the fixture manifest.
pub const KRAKEN_L3_QUALIFICATION_POLICY_DIGEST: &str =
    "91ae39a8cdbc24cefa77c926479b99f15991de0020cbb27727df1fd40228df29";

const MAX_SYMBOL_BYTES: usize = 64;
const MAX_PRODUCTS_PER_CONNECTION: usize = 200;
const MAX_TOKEN_BYTES: usize = 2_048;
const MAX_SUBSCRIPTION_BYTES: usize = 64 * 1024;

/// Kraken-supported authenticated order-book depths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenL3Depth {
    /// Ten price levels per side.
    Ten,
    /// One hundred price levels per side.
    OneHundred,
    /// One thousand price levels per side.
    OneThousand,
}

impl KrakenL3Depth {
    /// Returns the provider depth value.
    pub const fn get(self) -> usize {
        match self {
            Self::Ten => 10,
            Self::OneHundred => 100,
            Self::OneThousand => 1_000,
        }
    }

    /// Returns Kraken's subscription-rate counter increase per symbol.
    pub const fn rate_counter_cost(self) -> usize {
        match self {
            Self::Ten => 5,
            Self::OneHundred => 25,
            Self::OneThousand => 100,
        }
    }
}

/// Kraken account tier used only to enforce the documented subscription-rate boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenL3ClientTier {
    /// Standard account rate-counter limit.
    Standard,
    /// Pro account rate-counter limit.
    Pro,
}

impl KrakenL3ClientTier {
    const fn rate_counter_limit(self) -> usize {
        match self {
            Self::Standard => 200,
            Self::Pro => 500,
        }
    }
}

/// Stable provider-symbol to internal-instrument mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenL3ProductMapping {
    symbol: String,
    instrument: InstrumentId,
}

impl KrakenL3ProductMapping {
    /// Constructs a bounded exact Kraken symbol mapping.
    ///
    /// # Errors
    ///
    /// Rejects an empty, oversized, non-ASCII, whitespace-bearing symbol.
    pub fn try_new(
        symbol: impl Into<String>,
        instrument: InstrumentId,
    ) -> Result<Self, KrakenL3ConfigError> {
        let symbol = symbol.into();
        if symbol.is_empty()
            || symbol.len() > MAX_SYMBOL_BYTES
            || !symbol.is_ascii()
            || symbol.chars().any(char::is_whitespace)
        {
            return Err(KrakenL3ConfigError::InvalidSymbol);
        }
        Ok(Self { symbol, instrument })
    }

    /// Returns the exact Kraken symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the mapped internal instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }
}

/// Borrowed short-lived Kraken WebSocket token.
///
/// Token ownership remains with the central secret/token authority. This wrapper is intentionally
/// non-serializable and its debug representation never contains the token.
#[derive(Clone, Copy)]
pub struct KrakenL3WebSocketToken<'a>(&'a str);

impl<'a> KrakenL3WebSocketToken<'a> {
    /// Validates one ephemeral token returned by `GetWebSocketsToken`.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, non-ASCII, whitespace-bearing, or control-bearing material.
    pub fn try_new(token: &'a str) -> Result<Self, KrakenL3ConfigError> {
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
            || !token.is_ascii()
            || token
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(KrakenL3ConfigError::InvalidToken);
        }
        Ok(Self(token))
    }

    fn expose(self) -> &'a str {
        self.0
    }
}

impl fmt::Debug for KrakenL3WebSocketToken<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KrakenL3WebSocketToken([REDACTED])")
    }
}

/// Redacted, zeroed-on-drop authenticated subscription payload.
pub struct KrakenL3SecretPayload(Vec<u8>);

impl KrakenL3SecretPayload {
    /// Returns payload bytes for the immediate WebSocket write.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for KrakenL3SecretPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KrakenL3SecretPayload([REDACTED])")
    }
}

impl Drop for KrakenL3SecretPayload {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Immutable authenticated level-3 configuration for one bounded WebSocket connection.
#[derive(Clone, Debug)]
pub struct KrakenL3Config {
    metadata: SourceMetadata,
    endpoint: Url,
    products: Vec<KrakenL3ProductMapping>,
    depth: KrakenL3Depth,
    tier: KrakenL3ClientTier,
    credential_record_id: SourceIdentifier,
    max_message_bytes: NonZeroUsize,
}

impl KrakenL3Config {
    /// Constructs an authenticated, order-level Kraken profile.
    ///
    /// `credential_record_id` is a non-secret stable local record identity. API keys, signing
    /// secrets, and WebSocket tokens must remain behind the central secret/token authority.
    ///
    /// # Errors
    ///
    /// Rejects metadata that overstates source quality or coverage, public authorization, an
    /// unapproved endpoint, duplicate/unbounded mappings, or an invalid message bound.
    pub fn try_new(
        metadata: SourceMetadata,
        products: Vec<KrakenL3ProductMapping>,
        depth: KrakenL3Depth,
        tier: KrakenL3ClientTier,
        credential_record_id: SourceIdentifier,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenL3ConfigError> {
        validate_products(&products, depth, tier)?;
        if max_message_bytes.get() > MAX_RAW_FRAME_BYTES {
            return Err(KrakenL3ConfigError::MessageBound);
        }
        if metadata.source_class() != SourceClass::Exchange
            || metadata.provider().as_str() != "kraken"
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.authorization().basis().as_source_identifier() != &credential_record_id
            || metadata.quality_ceiling() != DataQuality::DirectUnverified
            || metadata.capabilities().sequence() != SequenceCapability::Unsupported
            || metadata.capabilities().checksum() != ChecksumCapability::Provided
            || !metadata.capabilities().source_timestamps()
        {
            return Err(KrakenL3ConfigError::InvalidMetadata);
        }
        for mapping in &products {
            if metadata
                .coverage()
                .instruments()
                .membership(mapping.instrument())
                != InstrumentCoverageMembership::Enumerated
            {
                return Err(KrakenL3ConfigError::InvalidMetadata);
            }
        }
        let coverage = metadata
            .coverage()
            .live()
            .ok_or(KrakenL3ConfigError::InvalidMetadata)?;
        let venue =
            VenueId::try_from("kraken").map_err(|_| KrakenL3ConfigError::InvalidMetadata)?;
        if !metadata.coverage().topology().is_single_venue()
            || !metadata.coverage().topology().contains_venue(&venue)
        {
            return Err(KrakenL3ConfigError::InvalidMetadata);
        }
        if coverage
            .rule_for(LiveEventClass::BookSnapshot, Some(MarketDepth::OrderLevel))
            .is_none()
            || coverage
                .rule_for(LiveEventClass::BookDelta, Some(MarketDepth::OrderLevel))
                .is_none()
        {
            return Err(KrakenL3ConfigError::InvalidMetadata);
        }
        let SourceProtocolProfile::Live(protocol) = metadata.protocol_profile() else {
            return Err(KrakenL3ConfigError::InvalidMetadata);
        };
        validate_checksum_profile(protocol.checksum())?;
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(KrakenL3ConfigError::InvalidMetadata);
        };
        endpoint_policy
            .authorize(KRAKEN_L3_WEBSOCKET_ENDPOINT)
            .map_err(|_| KrakenL3ConfigError::Endpoint)?;
        let endpoint =
            Url::parse(KRAKEN_L3_WEBSOCKET_ENDPOINT).map_err(|_| KrakenL3ConfigError::Endpoint)?;
        Ok(Self {
            metadata,
            endpoint,
            products,
            depth,
            tier,
            credential_record_id,
            max_message_bytes,
        })
    }

    /// Returns immutable source metadata. It remains a quality ceiling, not current authority.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the exact allowlisted WebSocket endpoint.
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the bounded product mappings carried by this connection.
    pub fn products(&self) -> &[KrakenL3ProductMapping] {
        &self.products
    }

    /// Returns the configured price-level retention surrounding the order-level book.
    pub const fn retained_price_levels(&self) -> KrakenL3Depth {
        self.depth
    }

    /// Returns the explicit provider depth classification.
    pub const fn market_depth(&self) -> MarketDepth {
        MarketDepth::OrderLevel
    }

    /// Returns the configured provider tier used for subscription admission.
    pub const fn client_tier(&self) -> KrakenL3ClientTier {
        self.tier
    }

    /// Returns the maximum symbols admitted in one subscription-rate window.
    pub const fn max_symbols_per_subscription_batch(&self) -> usize {
        self.tier.rate_counter_limit() / self.depth.rate_counter_cost()
    }

    /// Returns the number of rate-window batches needed to subscribe every configured product.
    pub fn subscription_batch_count(&self) -> usize {
        self.products
            .len()
            .div_ceil(self.max_symbols_per_subscription_batch())
    }

    /// Returns the stable non-secret credential-record identity.
    pub const fn credential_record_id(&self) -> &SourceIdentifier {
        &self.credential_record_id
    }

    /// Returns the maximum accepted WebSocket message size.
    pub const fn max_message_bytes(&self) -> usize {
        self.max_message_bytes.get()
    }

    /// Finds the exact configured mapping for one provider symbol.
    pub fn mapping(&self, symbol: &str) -> Option<&KrakenL3ProductMapping> {
        self.products
            .iter()
            .find(|mapping| mapping.symbol == symbol)
    }

    /// Encodes one bounded authenticated snapshot subscription batch.
    ///
    /// The returned payload is redacted in debug output and overwritten on drop. Callers should
    /// write it immediately and must not persist or log its bytes. Batches are ordered by the
    /// configured product list; the connection supervisor must admit at most one batch in each
    /// documented one-second subscription-rate window.
    ///
    /// # Errors
    ///
    /// Returns an error if bounded serialization cannot be completed.
    pub fn try_subscription_payload(
        &self,
        token: KrakenL3WebSocketToken<'_>,
        batch_index: usize,
        request_id: Option<u64>,
    ) -> Result<KrakenL3SecretPayload, KrakenL3ConfigError> {
        if request_id == Some(0) {
            return Err(KrakenL3ConfigError::InvalidRequestId);
        }
        let batch_size = self.max_symbols_per_subscription_batch();
        let start = batch_index
            .checked_mul(batch_size)
            .filter(|start| *start < self.products.len())
            .ok_or(KrakenL3ConfigError::InvalidSubscriptionBatch)?;
        let end = start.saturating_add(batch_size).min(self.products.len());
        let symbols = self.products[start..end]
            .iter()
            .map(KrakenL3ProductMapping::symbol)
            .collect::<Vec<_>>();
        let request = SubscriptionRequest {
            method: "subscribe",
            params: SubscriptionParams {
                channel: "level3",
                symbols,
                depth: self.depth.get(),
                snapshot: true,
                token: token.expose(),
            },
            request_id,
        };
        let payload = serde_json::to_vec(&request)
            .map_err(|_| KrakenL3ConfigError::SubscriptionSerialization)?;
        if payload.len() > MAX_SUBSCRIPTION_BYTES {
            return Err(KrakenL3ConfigError::SubscriptionSerialization);
        }
        Ok(KrakenL3SecretPayload(payload))
    }
}

impl SourceMetadataProvider for KrakenL3Config {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    method: &'static str,
    params: SubscriptionParams<'a>,
    #[serde(rename = "req_id", skip_serializing_if = "Option::is_none")]
    request_id: Option<u64>,
}

#[derive(Serialize)]
struct SubscriptionParams<'a> {
    channel: &'static str,
    #[serde(rename = "symbol")]
    symbols: Vec<&'a str>,
    depth: usize,
    snapshot: bool,
    token: &'a str,
}

fn validate_products(
    products: &[KrakenL3ProductMapping],
    depth: KrakenL3Depth,
    tier: KrakenL3ClientTier,
) -> Result<(), KrakenL3ConfigError> {
    if products.is_empty() || products.len() > MAX_PRODUCTS_PER_CONNECTION {
        return Err(KrakenL3ConfigError::ProductBound);
    }
    if depth.rate_counter_cost() > tier.rate_counter_limit() {
        return Err(KrakenL3ConfigError::RateCounterBound);
    }
    for (index, mapping) in products.iter().enumerate() {
        if products[..index]
            .iter()
            .any(|prior| prior.symbol == mapping.symbol || prior.instrument == mapping.instrument)
        {
            return Err(KrakenL3ConfigError::DuplicateProduct);
        }
    }
    Ok(())
}

fn validate_checksum_profile(
    checksum: &ChecksumValidationProfile,
) -> Result<(), KrakenL3ConfigError> {
    let ChecksumValidationProfile::Provided {
        algorithm,
        canonicalization,
        scope,
        book_scope: Some(book_scope),
        ..
    } = checksum
    else {
        return Err(KrakenL3ConfigError::InvalidMetadata);
    };
    if *algorithm != ChecksumAlgorithm::Crc32IsoHdlc
        || canonicalization.as_str() != KRAKEN_L3_CHECKSUM_CANONICALIZATION_ID
        || scope.as_str() != KRAKEN_L3_CHECKSUM_SCOPE_ID
        || book_scope.depth() != MarketDepth::OrderLevel
        || book_scope.level_count().map(NonZeroU16::get) != Some(10)
    {
        return Err(KrakenL3ConfigError::InvalidMetadata);
    }
    Ok(())
}

/// Caller-owned evidence for authenticated Kraken order-level source metadata.
#[derive(Clone, Debug)]
pub struct KrakenL3MetadataInput {
    source_id: SourceId,
    revision_evidence: RevisionBoundPayloadEvidence,
    authorization: AuthorizationGrant,
    coverage_evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    instruments: Vec<InstrumentId>,
    freshness: FreshnessPolicy,
    budget: ProviderBudgetPolicy,
}

impl KrakenL3MetadataInput {
    /// Collects rights, coverage, timing, and budget evidence for a bounded instrument set.
    #[allow(
        clippy::too_many_arguments,
        reason = "source identity, rights, coverage, timing, and budget evidence stay explicit"
    )]
    pub const fn new(
        source_id: SourceId,
        revision_evidence: RevisionBoundPayloadEvidence,
        authorization: AuthorizationGrant,
        coverage_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        instruments: Vec<InstrumentId>,
        freshness: FreshnessPolicy,
        budget: ProviderBudgetPolicy,
    ) -> Self {
        Self {
            source_id,
            revision_evidence,
            authorization,
            coverage_evidence,
            effective,
            instruments,
            freshness,
            budget,
        }
    }

    /// Builds metadata capped at `DirectUnverified` with explicit order-level coverage.
    ///
    /// # Errors
    ///
    /// Rejects public authorization, invalid evidence relationships, duplicate/unbounded
    /// instruments, or incompatible source-framework policy.
    pub fn try_build(self) -> Result<SourceMetadata, KrakenL3MetadataError> {
        if self.authorization.mode() != AuthorizationMode::UserAuthorized {
            return Err(KrakenL3MetadataError::Authorization);
        }
        if self.instruments.is_empty()
            || self.instruments.len() > MAX_PRODUCTS_PER_CONNECTION
            || self
                .instruments
                .iter()
                .enumerate()
                .any(|(index, instrument)| self.instruments[..index].contains(instrument))
        {
            return Err(KrakenL3MetadataError::Instruments);
        }
        let version = RuleVersion::new(KRAKEN_L3_QUALIFICATION_POLICY_VERSION)
            .map_err(|_| KrakenL3MetadataError::Rule)?;
        let make_rule = |name: &'static str| -> Result<IntegrityRule, KrakenL3MetadataError> {
            Ok(IntegrityRule::new(
                SourceIdentifier::try_from(name)?,
                version,
            ))
        };
        let rules = vec![
            LiveCoverageRule::try_new(
                LiveEventClass::BookSnapshot,
                Some(MarketDepth::OrderLevel),
                SnapshotApplicability::Required,
            )?,
            LiveCoverageRule::try_new(
                LiveEventClass::BookDelta,
                Some(MarketDepth::OrderLevel),
                SnapshotApplicability::Required,
            )?,
        ];
        let live = LiveCoverageDeclaration::try_new(
            ProviderProduct::new(SourceIdentifier::try_from("kraken-spot")?),
            ProviderChannel::new(SourceIdentifier::try_from("level3-v2")?),
            rules,
        )?;
        let coverage = SourceCoverage::try_instrument(
            self.coverage_evidence,
            self.effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(VenueId::try_from("kraken")?),
            InstrumentCoverage::enumerated(self.instruments)?,
            Some(live),
            CoverageDelay::RealTime,
            DeliveryEvidence::DirectVenue,
        )?;
        let checksum = ChecksumValidationProfile::Provided {
            rule: make_rule("kraken-ws-v2-level3-checksum-v1")?,
            algorithm: ChecksumAlgorithm::Crc32IsoHdlc,
            canonicalization: SourceIdentifier::try_from(KRAKEN_L3_CHECKSUM_CANONICALIZATION_ID)?,
            scope: SourceIdentifier::try_from(KRAKEN_L3_CHECKSUM_SCOPE_ID)?,
            book_scope: Some(ChecksumBookScope::new(
                MarketDepth::OrderLevel,
                NonZeroU16::new(10),
            )),
        };
        let protocol = LiveProtocolProfile::new(
            make_rule("kraken-ws-v2-level3-decoder-policy-v1")?,
            SemanticInterpretationProfile::new(
                make_rule("kraken-ws-v2-level3-side-policy-v1")?,
                make_rule("kraken-ws-v2-level3-auction-unsupported-v1")?,
                make_rule("kraken-ws-v2-level3-system-status-v1")?,
                make_rule("kraken-ws-v2-level3-corporate-action-unsupported-v1")?,
            ),
            make_rule("kraken-ws-v2-level3-rfc3339-timestamp-v1")?,
            SequenceValidationProfile::Unsupported {
                rule: make_rule("kraken-ws-v2-level3-sequence-unsupported-v1")?,
            },
            checksum,
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        );
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            self.source_id,
            self.revision_evidence,
            SourceClass::Exchange,
            SourceIdentifier::try_from("kraken")?,
            self.authorization,
            coverage,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(market_squawk_sources::EndpointPolicy::try_new([
                KRAKEN_L3_WEBSOCKET_ENDPOINT,
            ])?),
            self.freshness,
            Some(self.budget),
            SourceCapabilities::new(
                true,
                false,
                SequenceCapability::Unsupported,
                ChecksumCapability::Provided,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(protocol)),
        ))?)
    }
}

/// Authenticated Kraken level-3 configuration error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenL3ConfigError {
    /// Source metadata is inconsistent with the authenticated order-level profile.
    #[error("Kraken level-3 metadata is inconsistent with adapter capabilities")]
    InvalidMetadata,
    /// A provider symbol is malformed or oversized.
    #[error("Kraken level-3 symbol is invalid")]
    InvalidSymbol,
    /// The product set is empty or exceeds the per-connection ceiling.
    #[error("Kraken level-3 product count is outside the supported bound")]
    ProductBound,
    /// A product symbol or internal instrument appears more than once.
    #[error("Kraken level-3 product mapping is duplicated")]
    DuplicateProduct,
    /// The subscription exceeds the selected account tier's rate-counter limit.
    #[error("Kraken level-3 subscription exceeds the selected rate-counter limit")]
    RateCounterBound,
    /// The WebSocket endpoint is not the exact allowlisted production authority.
    #[error("Kraken level-3 endpoint is not allowlisted")]
    Endpoint,
    /// The message bound exceeds the global raw-frame ceiling.
    #[error("Kraken level-3 message bound is invalid")]
    MessageBound,
    /// The ephemeral provider token is malformed or oversized.
    #[error("Kraken level-3 WebSocket token is invalid")]
    InvalidToken,
    /// Kraken reserves zero from the accepted client request-identity domain.
    #[error("Kraken level-3 request identifier is invalid")]
    InvalidRequestId,
    /// The requested rate-window subscription batch does not exist.
    #[error("Kraken level-3 subscription batch is invalid")]
    InvalidSubscriptionBatch,
    /// The authenticated subscription could not be encoded inside its bound.
    #[error("Kraken level-3 subscription serialization failed")]
    SubscriptionSerialization,
}

/// Authenticated Kraken level-3 metadata construction error.
#[derive(Debug, Error)]
pub enum KrakenL3MetadataError {
    /// A source-framework relationship was invalid.
    #[error(transparent)]
    Metadata(#[from] SourceMetadataError),
    /// A provider/network policy was invalid.
    #[error(transparent)]
    Network(#[from] market_squawk_sources::NetworkPolicyError),
    /// A bounded domain identity was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    /// Authenticated level-3 access requires user-authorized credentials.
    #[error("Kraken level-3 authorization must be user-authorized")]
    Authorization,
    /// The instrument set is empty, duplicated, or outside the connection bound.
    #[error("Kraken level-3 instrument coverage is invalid")]
    Instruments,
    /// A compiled provider rule identity was invalid.
    #[error("compiled Kraken level-3 rule identity is invalid")]
    Rule,
}
