//! Evidence-bound authenticated level-3 configuration and secret-safe subscription encoding.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    EffectiveInterval, ExactPayloadEvidence, InstrumentId, IntegrityRule, LiveEventClass,
    MarketDepth, MetadataRevision, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence,
    RuleVersion, SchemaVersion, SequenceCapability, SnapshotApplicability, SourceId,
    SourceIdentifier, VenueId,
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

use crate::handoff::{KrakenInstrumentBinding, instrument_binding};

/// Authenticated Kraken Spot WebSocket v2 endpoint used by the `level3` channel.
pub const KRAKEN_L3_WEBSOCKET_ENDPOINT: &str = "wss://ws-l3.kraken.com/v2";
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

/// Process-local protected authority for one exact Kraken credential authorization generation.
///
/// This non-cloneable, non-serializable allocation is established before any short-lived provider
/// token exists. It binds configuration and later token capabilities to the same protected
/// credential record and authorization generation.
pub struct KrakenL3CredentialAuthority {
    binding: Arc<KrakenL3CredentialAuthorityBinding>,
}

impl KrakenL3CredentialAuthority {
    /// Establishes one exact protected credential authority.
    ///
    /// The coordinates are secret-free. Allocation identity, not reconstructable values alone,
    /// binds every capability minted by this authority.
    pub(crate) fn new(
        credential_record_id: SourceIdentifier,
        authorization_generation: NonZeroU64,
    ) -> Self {
        Self {
            binding: Arc::new(KrakenL3CredentialAuthorityBinding {
                credential_record_id,
                authorization_generation,
            }),
        }
    }

    /// Admits one short-lived provider token into a non-forgeable one-use capability.
    ///
    /// The caller's `String` allocation moves into adapter ownership without copying. The
    /// capability is neither cloneable nor serializable and zeroes the token on rejection and
    /// drop.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, non-ASCII, whitespace-bearing, or control-bearing material.
    pub fn try_mint_subscription_capability(
        &self,
        token: String,
    ) -> Result<KrakenL3TokenCapability, KrakenL3ConfigError> {
        let mut token = token.into_bytes();
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES
            || !token.is_ascii()
            || token
                .iter()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            zeroize_token_buffer(&mut token);
            return Err(KrakenL3ConfigError::InvalidToken);
        }
        Ok(KrakenL3TokenCapability {
            binding: Arc::clone(&self.binding),
            token,
        })
    }

    /// Returns the secret-free credential record identity.
    pub fn credential_record_id(&self) -> &SourceIdentifier {
        &self.binding.credential_record_id
    }

    /// Returns the exact nonzero authorization generation.
    pub fn authorization_generation(&self) -> NonZeroU64 {
        self.binding.authorization_generation
    }
}

impl fmt::Debug for KrakenL3CredentialAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KrakenL3CredentialAuthority")
            .field("credential_record_id", &self.binding.credential_record_id)
            .field(
                "authorization_generation",
                &self.binding.authorization_generation,
            )
            .finish()
    }
}

/// Non-forgeable one-use authorization to encode one Kraken L3 subscription.
///
/// The capability cannot be constructed, cloned, copied, or serialized outside the credential
/// authority. It owns and zeroes the sole adapter token allocation admitted for this request. Its
/// secret-free credential coordinates and allocation identity follow the resulting request through
/// the send receipt, decoder registration, and handoff.
pub struct KrakenL3TokenCapability {
    binding: Arc<KrakenL3CredentialAuthorityBinding>,
    token: Vec<u8>,
}

impl KrakenL3TokenCapability {
    /// Returns the secret-free credential record identity.
    pub fn credential_record_id(&self) -> &SourceIdentifier {
        &self.binding.credential_record_id
    }

    /// Returns the exact nonzero authorization generation.
    pub fn authorization_generation(&self) -> NonZeroU64 {
        self.binding.authorization_generation
    }
}

impl fmt::Debug for KrakenL3TokenCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KrakenL3TokenCapability")
            .field("credential_record_id", &self.binding.credential_record_id)
            .field(
                "authorization_generation",
                &self.binding.authorization_generation,
            )
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl Drop for KrakenL3TokenCapability {
    fn drop(&mut self) {
        zeroize_token_buffer(&mut self.token);
    }
}

#[derive(Debug)]
pub(crate) struct KrakenL3CredentialAuthorityBinding {
    credential_record_id: SourceIdentifier,
    authorization_generation: NonZeroU64,
}

impl KrakenL3CredentialAuthorityBinding {
    pub(crate) const fn credential_record_id(&self) -> &SourceIdentifier {
        &self.credential_record_id
    }

    pub(crate) const fn authorization_generation(&self) -> NonZeroU64 {
        self.authorization_generation
    }
}

/// Exact secret-free contract encoded beside one authenticated subscription payload.
///
/// This value never retains or hashes the short-lived WebSocket token. It is created in the same
/// operation as the secret-bearing bytes, so its batch, request identifier, depth, snapshot
/// semantic, and ordered provider symbols cannot drift from what was encoded.
#[derive(Debug)]
pub struct KrakenL3SubscriptionRequestEvidence {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    batch_index: usize,
    request_id: Option<u64>,
    depth: KrakenL3Depth,
    snapshot: bool,
    instrument_bindings: Vec<Arc<KrakenInstrumentBinding>>,
    credential_authority: Arc<KrakenL3CredentialAuthorityBinding>,
}

impl KrakenL3SubscriptionRequestEvidence {
    /// Returns the immutable source identity used to encode the request.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact metadata revision used to encode the request.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the zero-based configured subscription batch.
    pub const fn batch_index(&self) -> usize {
        self.batch_index
    }

    /// Returns the exact provider request identifier encoded on the wire.
    pub const fn request_id(&self) -> Option<u64> {
        self.request_id
    }

    /// Returns the exact provider depth encoded on the wire.
    pub const fn depth(&self) -> KrakenL3Depth {
        self.depth
    }

    /// Returns whether the exact request asked for initializing snapshots.
    pub const fn snapshot(&self) -> bool {
        self.snapshot
    }

    /// Returns the ordered native-symbol to external-instrument bindings encoded in this batch.
    pub fn instrument_bindings(&self) -> &[Arc<KrakenInstrumentBinding>] {
        &self.instrument_bindings
    }

    /// Returns the exact secret-free credential record identity used by the request.
    pub fn credential_record_id(&self) -> &SourceIdentifier {
        &self.credential_authority.credential_record_id
    }

    /// Returns the exact protected authorization generation used by the request.
    pub fn authorization_generation(&self) -> NonZeroU64 {
        self.credential_authority.authorization_generation
    }

    pub(crate) fn shares_credential_authority_with(
        &self,
        binding: &Arc<KrakenL3CredentialAuthorityBinding>,
    ) -> bool {
        Arc::ptr_eq(&self.credential_authority, binding)
    }
}

impl PartialEq for KrakenL3SubscriptionRequestEvidence {
    fn eq(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.metadata_revision == other.metadata_revision
            && self.batch_index == other.batch_index
            && self.request_id == other.request_id
            && self.depth == other.depth
            && self.snapshot == other.snapshot
            && self.instrument_bindings == other.instrument_bindings
            && Arc::ptr_eq(&self.credential_authority, &other.credential_authority)
    }
}

impl Eq for KrakenL3SubscriptionRequestEvidence {}

/// Redacted, zeroed-on-drop authenticated subscription payload and its secret-free contract.
pub struct KrakenL3SecretPayload {
    bytes: Vec<u8>,
    request_evidence: Option<KrakenL3SubscriptionRequestEvidence>,
}

impl KrakenL3SecretPayload {
    #[allow(
        dead_code,
        reason = "the selected authenticated L3 session foundation consumes this opaque payload"
    )]
    pub(crate) fn into_transport_parts(
        mut self,
    ) -> Result<(String, KrakenL3SubscriptionRequestEvidence), KrakenL3ConfigError> {
        let evidence = self
            .request_evidence
            .take()
            .ok_or(KrakenL3ConfigError::SubscriptionSerialization)?;
        let bytes = std::mem::take(&mut self.bytes);
        String::from_utf8(bytes)
            .map(|wire| (wire, evidence))
            .map_err(|error| {
                let mut bytes = error.into_bytes();
                zeroize_token_buffer(&mut bytes);
                KrakenL3ConfigError::SubscriptionSerialization
            })
    }
}

impl fmt::Debug for KrakenL3SecretPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KrakenL3SecretPayload([REDACTED])")
    }
}

impl Drop for KrakenL3SecretPayload {
    fn drop(&mut self) {
        zeroize_token_buffer(&mut self.bytes);
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
    credential_authority: Arc<KrakenL3CredentialAuthorityBinding>,
    max_message_bytes: NonZeroUsize,
}

impl KrakenL3Config {
    /// Constructs an authenticated, order-level Kraken profile.
    ///
    /// `credential_authority` owns the non-secret credential record and authorization-generation
    /// allocation that must later mint each short-lived token capability. API keys and signing
    /// secrets remain outside this adapter.
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
        credential_authority: &KrakenL3CredentialAuthority,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenL3ConfigError> {
        validate_products(&products, depth, tier)?;
        if max_message_bytes.get() > MAX_RAW_FRAME_BYTES {
            return Err(KrakenL3ConfigError::MessageBound);
        }
        if metadata.source_class() != SourceClass::Exchange
            || metadata.provider().as_str() != "kraken"
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.authorization().basis().as_source_identifier()
                != credential_authority.credential_record_id()
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
            credential_authority: Arc::clone(&credential_authority.binding),
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
    pub fn credential_record_id(&self) -> &SourceIdentifier {
        &self.credential_authority.credential_record_id
    }

    /// Returns the exact protected authorization generation required by this configuration.
    pub fn authorization_generation(&self) -> NonZeroU64 {
        self.credential_authority.authorization_generation
    }

    pub(crate) fn credential_authority_binding(&self) -> Arc<KrakenL3CredentialAuthorityBinding> {
        Arc::clone(&self.credential_authority)
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
        token: KrakenL3TokenCapability,
        batch_index: usize,
        request_id: Option<u64>,
    ) -> Result<KrakenL3SecretPayload, KrakenL3ConfigError> {
        if !Arc::ptr_eq(&self.credential_authority, &token.binding) {
            return Err(KrakenL3ConfigError::CredentialAuthorityMismatch);
        }
        if request_id == Some(0) || (request_id.is_none() && self.subscription_batch_count() > 1) {
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
        let instrument_bindings = self.products[start..end]
            .iter()
            .map(|mapping| instrument_binding(mapping.symbol(), mapping.instrument()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| KrakenL3ConfigError::SubscriptionSerialization)?;
        let token =
            std::str::from_utf8(&token.token).map_err(|_| KrakenL3ConfigError::InvalidToken)?;
        let request = SubscriptionRequest {
            method: "subscribe",
            params: SubscriptionParams {
                channel: "level3",
                symbols,
                depth: self.depth.get(),
                snapshot: true,
                token,
            },
            request_id,
        };
        let mut payload = Vec::with_capacity(MAX_TOKEN_BYTES.saturating_add(512));
        if serde_json::to_writer(&mut payload, &request).is_err() {
            zeroize_token_buffer(&mut payload);
            return Err(KrakenL3ConfigError::SubscriptionSerialization);
        }
        if payload.len() > MAX_SUBSCRIPTION_BYTES {
            zeroize_token_buffer(&mut payload);
            return Err(KrakenL3ConfigError::SubscriptionSerialization);
        }
        Ok(KrakenL3SecretPayload {
            bytes: payload,
            request_evidence: Some(KrakenL3SubscriptionRequestEvidence {
                source_id: self.metadata.source_id().clone(),
                metadata_revision: self.metadata.revision().clone(),
                batch_index,
                request_id,
                depth: self.depth,
                snapshot: true,
                instrument_bindings,
                credential_authority: Arc::clone(&self.credential_authority),
            }),
        })
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

fn zeroize_token_buffer(bytes: &mut [u8]) {
    bytes.fill(0);
    let _ = std::hint::black_box(bytes);
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
    /// The token capability was minted by a different protected authority allocation.
    #[error("Kraken level-3 token authority does not match configuration authority")]
    CredentialAuthorityMismatch,
    /// Kraken reserves zero, and multi-batch subscriptions require explicit request identity.
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
