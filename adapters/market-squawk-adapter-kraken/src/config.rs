//! Checked adapter configuration.

use std::num::NonZeroUsize;

use market_squawk_domain::{
    DataQuality, EvidenceDigest, InstrumentId, LiveEventClass, MarketDepth, MetadataRevision,
    ProviderChannel, ProviderIdentityKey, ProviderIdentityRecord, ProviderIdentityRegistry,
    ProviderProduct, SequenceCapability, SourceId, VenueId, VenueSymbol,
};
use market_squawk_sources::{
    ChecksumValidationProfile, InstrumentCoverageMembership, MAX_RAW_FRAME_BYTES,
    NetworkAccessPolicy, ResolvedChecksumValidator, SourceClass, SourceMetadata,
    SourceMetadataProvider,
};
use thiserror::Error;
use url::Url;

use crate::messages::PUBLIC_SUBSCRIPTION_REQUEST_ID;

const KRAKEN_ENDPOINT: &str = "wss://ws.kraken.com/v2";
const MAX_SYMBOL_BYTES: usize = 64;
const KRAKEN_PROVIDER: &str = "kraken";
const KRAKEN_PRODUCT: &str = "kraken-spot";
const KRAKEN_BOOK_CHANNEL: &str = "book-v2";
const KRAKEN_TRADE_CHANNEL: &str = "trade-v2";

/// Kraken-supported retained book depths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenDepth {
    /// Ten price levels per side.
    Ten,
    /// Twenty-five price levels per side.
    TwentyFive,
    /// One hundred price levels per side.
    OneHundred,
    /// Five hundred price levels per side.
    FiveHundred,
    /// One thousand price levels per side.
    OneThousand,
}

impl KrakenDepth {
    /// Returns the provider depth value.
    pub const fn get(self) -> usize {
        match self {
            Self::Ten => 10,
            Self::TwentyFive => 25,
            Self::OneHundred => 100,
            Self::FiveHundred => 500,
            Self::OneThousand => 1_000,
        }
    }
}

/// Independently registered Kraken channel and its integrity capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenChannel {
    /// Price-level book with the selected retained depth and CRC32 validation.
    Book(KrakenDepth),
    /// Trade stream; Kraken supplies trade IDs but no book-style checksum.
    Trades,
}

impl KrakenChannel {
    pub(crate) const fn provider_channel(self) -> &'static str {
        match self {
            Self::Book(_) => KRAKEN_BOOK_CHANNEL,
            Self::Trades => KRAKEN_TRADE_CHANNEL,
        }
    }
}

/// Exact Kraken-native instrument and public market-surface coordinates.
///
/// This value preserves an accepted provider identity assertion independently from Kraken's
/// venue-native WebSocket symbol. It carries no current-source or publication authority: the
/// shared source registry must still attest the selected identity and venue mapping for the exact
/// application session before these coordinates may enter publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenNativeMarketCoordinates {
    source_id: SourceId,
    source_metadata_revision: MetadataRevision,
    source_metadata_digest: EvidenceDigest,
    provider_identity_key: ProviderIdentityKey,
    provider_identity_revision: MetadataRevision,
    provider_identity_digest: EvidenceDigest,
    venue: VenueId,
    venue_symbol: VenueSymbol,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    instrument: InstrumentId,
    channel: KrakenChannel,
}

impl KrakenNativeMarketCoordinates {
    /// Returns the registered live-source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact registered source-metadata revision.
    pub const fn source_metadata_revision(&self) -> &MetadataRevision {
        &self.source_metadata_revision
    }

    /// Returns the digest of the exact registered source-metadata payload.
    pub const fn source_metadata_digest(&self) -> EvidenceDigest {
        self.source_metadata_digest
    }

    /// Returns the independent provider-native identity key.
    pub const fn provider_identity_key(&self) -> &ProviderIdentityKey {
        &self.provider_identity_key
    }

    /// Returns the exact accepted provider-identity revision.
    pub const fn provider_identity_revision(&self) -> &MetadataRevision {
        &self.provider_identity_revision
    }

    /// Returns the exact accepted provider-identity content digest.
    pub const fn provider_identity_digest(&self) -> EvidenceDigest {
        self.provider_identity_digest
    }

    /// Returns the exact venue namespace.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the venue-native WebSocket symbol independently from the provider identity key.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }

    /// Returns the exact provider product declared by source metadata.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    /// Returns the exact provider channel declared by source metadata.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    /// Returns the externally resolved canonical instrument.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the independently configured Kraken book or trade surface.
    pub const fn channel(&self) -> KrakenChannel {
        self.channel
    }

    pub(crate) fn matches_surface(
        &self,
        metadata: &SourceMetadata,
        symbol: &str,
        instrument: InstrumentId,
        channel: KrakenChannel,
    ) -> bool {
        let Ok(surface) = validate_public_surface(metadata, symbol, instrument, channel) else {
            return false;
        };
        self.source_id == *metadata.source_id()
            && self.source_metadata_revision == *metadata.revision()
            && self.source_metadata_digest
                == metadata
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest()
            && self.venue == surface.venue
            && self.venue_symbol.as_str() == symbol
            && self.provider_product == surface.provider_product
            && self.provider_channel == surface.provider_channel
            && self.instrument == instrument
            && self.channel == channel
    }
}

#[derive(Debug)]
pub(crate) struct ValidatedKrakenSurface {
    venue: VenueId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
}

/// Immutable configuration for one Kraken symbol and one connection generation.
#[derive(Clone, Debug)]
pub struct KrakenConfig {
    metadata: SourceMetadata,
    endpoint: Url,
    symbol: String,
    instrument: InstrumentId,
    channel: KrakenChannel,
    max_message_bytes: NonZeroUsize,
}

impl KrakenConfig {
    /// Constructs a configuration bound to authoritative Kraken metadata.
    ///
    /// # Errors
    ///
    /// Rejects metadata that overstates Kraken's capabilities, an unapproved endpoint, a malformed
    /// symbol, an unsupported checksum profile, or a message bound outside the global capture
    /// ceiling.
    pub fn try_new(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        depth: KrakenDepth,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            symbol,
            instrument,
            KrakenChannel::Book(depth),
            max_message_bytes,
        )
    }

    /// Constructs a trade-channel configuration with checksum-unsupported metadata.
    pub fn try_trades(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            symbol,
            instrument,
            KrakenChannel::Trades,
            max_message_bytes,
        )
    }

    fn try_for_channel(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        channel: KrakenChannel,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        let symbol = symbol.into();
        validate_public_surface(&metadata, &symbol, instrument, channel)?;
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(KrakenConfigError::InvalidMetadata);
        };
        let endpoint = Url::parse(KRAKEN_ENDPOINT).map_err(|_| KrakenConfigError::Endpoint)?;
        endpoint_policy
            .authorize(KRAKEN_ENDPOINT)
            .map_err(|_| KrakenConfigError::Endpoint)?;
        if max_message_bytes.get() > MAX_RAW_FRAME_BYTES {
            return Err(KrakenConfigError::MessageBound);
        }
        Ok(Self {
            metadata,
            endpoint,
            symbol,
            instrument,
            channel,
            max_message_bytes,
        })
    }

    /// Returns the exact allowlisted endpoint.
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the provider symbol handled by this source instance.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the internal instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the independently registered provider channel.
    pub const fn channel(&self) -> KrakenChannel {
        self.channel
    }

    /// Returns the maximum exact frame size.
    pub const fn max_message_bytes(&self) -> usize {
        self.max_message_bytes.get()
    }

    /// Validates one accepted provider identity against this exact Kraken market surface.
    ///
    /// The returned coordinates carry no current-source or publication authority. That authority
    /// remains a serialized shared-registry responsibility.
    ///
    /// # Errors
    ///
    /// Rejects a provider-identity record absent from the supplied accepted registry, any
    /// conflict for its natural key, a provider namespace or canonical-instrument mismatch,
    /// zero identity evidence, non-overlapping identity/source validity, or any venue/product/
    /// channel mismatch in this configuration.
    pub fn try_native_coordinates(
        &self,
        record: &ProviderIdentityRecord,
        accepted_identities: &ProviderIdentityRegistry,
    ) -> Result<KrakenNativeMarketCoordinates, KrakenConfigError> {
        native_market_coordinates(
            &self.metadata,
            &self.symbol,
            self.instrument,
            self.channel,
            record,
            accepted_identities,
        )
    }

    pub(crate) fn authorize_endpoint(&self) -> Result<(), KrakenConfigError> {
        if self.endpoint.as_str() == KRAKEN_ENDPOINT {
            return self
                .metadata
                .network_policy()
                .authorize(KRAKEN_ENDPOINT)
                .map_err(|_| KrakenConfigError::Endpoint);
        }
        #[cfg(all(feature = "loopback-fixture", debug_assertions))]
        if is_local_test_endpoint(&self.endpoint) {
            return Ok(());
        }
        Err(KrakenConfigError::Endpoint)
    }

    /// Replaces the sealed endpoint with a loopback-only deterministic test connector.
    ///
    /// This API does not exist unless both the explicit loopback-fixture feature and debug
    /// assertions are enabled. Production and release all-features builds therefore have no
    /// endpoint override.
    #[cfg(all(feature = "loopback-fixture", debug_assertions))]
    pub fn with_local_endpoint_for_test(
        mut self,
        endpoint: &str,
    ) -> Result<Self, KrakenConfigError> {
        let endpoint = Url::parse(endpoint).map_err(|_| KrakenConfigError::Endpoint)?;
        if !is_local_test_endpoint(&endpoint) {
            return Err(KrakenConfigError::Endpoint);
        }
        self.endpoint = endpoint;
        Ok(self)
    }
}

#[cfg(all(feature = "loopback-fixture", debug_assertions))]
fn is_local_test_endpoint(endpoint: &Url) -> bool {
    let loopback = match endpoint.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(_)) | None => false,
    };
    endpoint.scheme() == "ws"
        && loopback
        && endpoint.port().is_some()
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && endpoint.path() == "/"
}

impl SourceMetadataProvider for KrakenConfig {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

pub(crate) fn public_subscription_payload(
    symbol: &str,
    channel: KrakenChannel,
) -> Result<String, serde_json::Error> {
    let (channel, depth) = match channel {
        KrakenChannel::Book(depth) => ("book", Some(depth.get())),
        KrakenChannel::Trades => ("trade", None),
    };
    let mut params = serde_json::Map::new();
    params.insert(
        "channel".to_owned(),
        serde_json::Value::String(channel.to_owned()),
    );
    params.insert(
        "symbol".to_owned(),
        serde_json::Value::Array(vec![serde_json::Value::String(symbol.to_owned())]),
    );
    params.insert("snapshot".to_owned(), serde_json::Value::Bool(true));
    if let Some(depth) = depth {
        params.insert("depth".to_owned(), serde_json::Value::from(depth));
    }
    serde_json::to_string(&serde_json::json!({
        "method": "subscribe",
        "params": params,
        "req_id": PUBLIC_SUBSCRIPTION_REQUEST_ID,
    }))
}

pub(crate) fn validate_public_surface(
    metadata: &SourceMetadata,
    symbol: &str,
    instrument: InstrumentId,
    channel: KrakenChannel,
) -> Result<ValidatedKrakenSurface, KrakenConfigError> {
    if symbol.is_empty()
        || symbol.len() > MAX_SYMBOL_BYTES
        || !symbol.is_ascii()
        || symbol.chars().any(char::is_whitespace)
    {
        return Err(KrakenConfigError::InvalidSymbol);
    }
    if metadata.source_class() != SourceClass::Exchange
        || metadata.provider().as_str() != KRAKEN_PROVIDER
        || metadata.quality_ceiling() != DataQuality::DirectUnverified
        || metadata.capabilities().sequence() != SequenceCapability::Unsupported
        || !metadata.capabilities().source_timestamps()
        || metadata.coverage().instruments().membership(instrument)
            != InstrumentCoverageMembership::Enumerated
    {
        return Err(KrakenConfigError::InvalidMetadata);
    }
    let venue =
        VenueId::try_from(KRAKEN_PROVIDER).map_err(|_| KrakenConfigError::InvalidMetadata)?;
    if !metadata.coverage().topology().is_single_venue()
        || !metadata.coverage().topology().contains_venue(&venue)
    {
        return Err(KrakenConfigError::InvalidMetadata);
    }
    let live = metadata
        .coverage()
        .live()
        .ok_or(KrakenConfigError::InvalidMetadata)?;
    if live.provider_product().as_source_identifier().as_str() != KRAKEN_PRODUCT
        || live.provider_channel().as_source_identifier().as_str() != channel.provider_channel()
    {
        return Err(KrakenConfigError::InvalidMetadata);
    }
    let market_squawk_sources::SourceProtocolProfile::Live(protocol) = metadata.protocol_profile()
    else {
        return Err(KrakenConfigError::InvalidMetadata);
    };
    match channel {
        KrakenChannel::Book(depth) => {
            if live
                .rule_for(LiveEventClass::BookSnapshot, Some(MarketDepth::PriceLevel))
                .is_none()
                || live
                    .rule_for(LiveEventClass::BookDelta, Some(MarketDepth::PriceLevel))
                    .is_none()
            {
                return Err(KrakenConfigError::InvalidMetadata);
            }
            ResolvedChecksumValidator::resolve(protocol.checksum(), depth.get())
                .map_err(|_| KrakenConfigError::InvalidMetadata)?;
        }
        KrakenChannel::Trades => {
            if live.rule_for(LiveEventClass::Trade, None).is_none()
                || !matches!(
                    protocol.checksum(),
                    ChecksumValidationProfile::Unsupported { .. }
                )
            {
                return Err(KrakenConfigError::InvalidMetadata);
            }
        }
    }
    Ok(ValidatedKrakenSurface {
        venue,
        provider_product: live.provider_product().clone(),
        provider_channel: live.provider_channel().clone(),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "native identity, venue, canonical, and registered source axes remain explicit"
)]
fn native_market_coordinates(
    metadata: &SourceMetadata,
    symbol: &str,
    instrument: InstrumentId,
    channel: KrakenChannel,
    record: &ProviderIdentityRecord,
    accepted_identities: &ProviderIdentityRegistry,
) -> Result<KrakenNativeMarketCoordinates, KrakenConfigError> {
    let surface = validate_public_surface(metadata, symbol, instrument, channel)?;
    let record_is_accepted = accepted_identities
        .accepted()
        .iter()
        .any(|accepted| accepted == record);
    let key_is_conflicted = accepted_identities
        .conflicts()
        .iter()
        .any(|conflict| conflict.key() == &record.key());
    let provider_namespace =
        SourceId::try_from(KRAKEN_PROVIDER).map_err(|_| KrakenConfigError::NativeIdentity)?;
    if !record_is_accepted
        || key_is_conflicted
        || record.source_id() != &provider_namespace
        || record.instrument_id() != instrument
        || record.evidence().content_digest().bytes() == [0; 32]
        || !intervals_overlap(record.validity(), metadata.coverage().effective_interval())
    {
        return Err(KrakenConfigError::NativeIdentity);
    }
    let venue_symbol =
        VenueSymbol::try_from(symbol).map_err(|_| KrakenConfigError::InvalidSymbol)?;
    Ok(KrakenNativeMarketCoordinates {
        source_id: metadata.source_id().clone(),
        source_metadata_revision: metadata.revision().clone(),
        source_metadata_digest: metadata
            .revision_evidence()
            .payload_evidence()
            .content_digest(),
        provider_identity_key: record.key(),
        provider_identity_revision: record.metadata_revision().clone(),
        provider_identity_digest: record.evidence().content_digest(),
        venue: surface.venue,
        venue_symbol,
        provider_product: surface.provider_product,
        provider_channel: surface.provider_channel,
        instrument,
        channel,
    })
}

fn intervals_overlap(
    left: market_squawk_domain::EffectiveInterval,
    right: market_squawk_domain::EffectiveInterval,
) -> bool {
    left.ends_at().is_none_or(|end| right.starts_at() < end)
        && right.ends_at().is_none_or(|end| left.starts_at() < end)
}

/// Kraken configuration error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenConfigError {
    /// Metadata is inconsistent with the reviewed Kraken policy.
    #[error("Kraken source metadata is inconsistent with adapter capabilities")]
    InvalidMetadata,
    /// The configured symbol is invalid or unbounded.
    #[error("Kraken symbol is invalid")]
    InvalidSymbol,
    /// Provider-native identity evidence is absent, conflicted, or relationally inconsistent.
    #[error("Kraken provider-native identity is invalid")]
    NativeIdentity,
    /// The endpoint is not the exact approved production authority.
    #[error("Kraken endpoint is not allowlisted")]
    Endpoint,
    /// The per-message bound exceeds global capture limits.
    #[error("Kraken message bound is invalid")]
    MessageBound,
    /// The exact bounded subscription request could not be encoded.
    #[error("Kraken subscription request could not be encoded")]
    SubscriptionSerialization,
}
