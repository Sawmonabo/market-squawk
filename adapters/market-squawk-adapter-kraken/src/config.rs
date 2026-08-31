//! Checked adapter configuration.

use std::num::NonZeroUsize;

use market_squawk_domain::{
    DataQuality, EffectiveInterval, EvidenceDigest, InstrumentDefinition, InstrumentId,
    LiveEventClass, MarketDataInstrumentDefinition, MarketDepth, MetadataRevision, ProviderChannel,
    ProviderIdentityKey, ProviderProduct, SequenceCapability, SourceId, Timestamp, VenueId,
    VenueMapping, VenueSymbol,
};
use market_squawk_sources::{
    ChecksumValidationProfile, InstrumentCoverageMembership, MAX_RAW_FRAME_BYTES,
    NetworkAccessPolicy, ResolvedChecksumValidator, SourceClass, SourceMetadata,
    SourceMetadataProvider,
};
use thiserror::Error;
use url::Url;

use crate::messages::PUBLIC_SUBSCRIPTION_REQUEST_ID;

pub(crate) const KRAKEN_ENDPOINT: &str = "wss://ws.kraken.com/v2";
const MAX_SYMBOL_BYTES: usize = 64;
pub(crate) const KRAKEN_PROVIDER: &str = "kraken";
pub(crate) const KRAKEN_PRODUCT: &str = "kraken-spot";
pub(crate) const KRAKEN_BOOK_CHANNEL: &str = "book-v2";
pub(crate) const KRAKEN_TRADE_CHANNEL: &str = "trade-v2";

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

/// Exact durable reference-selection value evidence supplied by application composition.
///
/// This value is deliberately not reference authority. The installed application must construct
/// it from a digest-verified catalog selection receipt; the adapter checks and carries every slot
/// but cannot mint or query repository-owned reference records itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenReferenceSelectionEvidence {
    reference_revision: MetadataRevision,
    reference_payload_digest: EvidenceDigest,
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_published_at: Timestamp,
    definition_validity: EffectiveInterval,
    selection_receipt_digest: EvidenceDigest,
}

impl KrakenReferenceSelectionEvidence {
    /// Constructs checked value evidence copied from one exact durable catalog selection.
    ///
    /// This constructor cannot grant reference authority. Application composition must require the
    /// opaque repository record whose private fields produced these values before calling it.
    pub fn try_new(
        reference_revision: MetadataRevision,
        reference_payload_digest: EvidenceDigest,
        definition_revision_digest: EvidenceDigest,
        definition_revision_sequence: u32,
        definition_published_at: Timestamp,
        definition_validity: EffectiveInterval,
        selection_receipt_digest: EvidenceDigest,
    ) -> Result<Self, KrakenConfigError> {
        if reference_payload_digest.bytes() == [0; 32]
            || definition_revision_digest.bytes() == [0; 32]
            || definition_revision_sequence == 0
            || selection_receipt_digest.bytes() == [0; 32]
        {
            return Err(KrakenConfigError::NativeIdentity);
        }
        Ok(Self {
            reference_revision,
            reference_payload_digest,
            definition_revision_digest,
            definition_revision_sequence,
            definition_published_at,
            definition_validity,
            selection_receipt_digest,
        })
    }

    /// Returns the source-authored reference revision bound to the selected definition.
    pub const fn reference_revision(&self) -> &MetadataRevision {
        &self.reference_revision
    }

    /// Returns the exact reference-source payload digest.
    pub const fn reference_payload_digest(&self) -> EvidenceDigest {
        self.reference_payload_digest
    }

    /// Returns the digest of the complete immutable catalog definition revision.
    pub const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    /// Returns the repository-owned monotonic definition revision position.
    pub const fn definition_revision_sequence(&self) -> u32 {
        self.definition_revision_sequence
    }

    /// Returns when the selected immutable definition first became durable locally.
    pub const fn definition_published_at(&self) -> Timestamp {
        self.definition_published_at
    }

    /// Returns the full half-open effective interval of the selected definition.
    pub const fn definition_validity(&self) -> EffectiveInterval {
        self.definition_validity
    }

    /// Returns the digest of the exact catalog-selection receipt.
    pub const fn selection_receipt_digest(&self) -> EvidenceDigest {
        self.selection_receipt_digest
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
    venue_mapping: VenueMapping,
    reference_revision: MetadataRevision,
    reference_payload_digest: EvidenceDigest,
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_published_at: Timestamp,
    reference_selection_digest: EvidenceDigest,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    instrument: InstrumentId,
    channel: KrakenChannel,
    selected_at: Timestamp,
    valid_from: Timestamp,
    valid_until: Option<Timestamp>,
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
        self.venue_mapping.venue_id()
    }

    /// Returns the venue-native WebSocket symbol independently from the provider identity key.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        self.venue_mapping.venue_symbol()
    }

    /// Returns the exact current venue mapping selected from the canonical instrument definition.
    pub const fn venue_mapping(&self) -> &VenueMapping {
        &self.venue_mapping
    }

    /// Returns the source-authored reference revision bound to the catalog selection.
    pub const fn reference_revision(&self) -> &MetadataRevision {
        &self.reference_revision
    }

    /// Returns the exact reference-source payload digest.
    pub const fn reference_payload_digest(&self) -> EvidenceDigest {
        self.reference_payload_digest
    }

    /// Returns the digest of the complete immutable catalog definition revision.
    pub const fn definition_revision_digest(&self) -> EvidenceDigest {
        self.definition_revision_digest
    }

    /// Returns the repository-owned monotonic definition revision position.
    pub const fn definition_revision_sequence(&self) -> u32 {
        self.definition_revision_sequence
    }

    /// Returns when the immutable selected definition first became durable locally.
    pub const fn definition_published_at(&self) -> Timestamp {
        self.definition_published_at
    }

    /// Returns the digest of the exact application catalog-selection receipt.
    pub const fn reference_selection_digest(&self) -> EvidenceDigest {
        self.reference_selection_digest
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

    /// Returns the explicit instant at which registry and source validity were selected.
    pub const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }

    /// Returns the inclusive start of the common definition/source/identity validity interval.
    pub const fn valid_from(&self) -> Timestamp {
        self.valid_from
    }

    /// Returns the exclusive end of the common source/identity validity window, when finite.
    pub const fn valid_until(&self) -> Option<Timestamp> {
        self.valid_until
    }

    /// Returns whether these coordinates remain valid at an event or session instant.
    pub fn is_valid_at(&self, at: Timestamp) -> bool {
        self.valid_from <= at && self.valid_until.is_none_or(|end| at < end)
    }

    /// Returns whether the reference selection was already known and valid at local receipt time.
    pub fn is_selected_at(&self, at: Timestamp) -> bool {
        self.selected_at <= at && self.is_valid_at(at)
    }

    pub(crate) fn matches_surface(
        &self,
        metadata: &SourceMetadata,
        channel: KrakenChannel,
    ) -> bool {
        let Ok(surface) = validate_public_surface(
            metadata,
            self.venue_symbol().as_str(),
            self.instrument,
            channel,
        ) else {
            return false;
        };
        self.source_id == *metadata.source_id()
            && self.source_metadata_revision == *metadata.revision()
            && self.source_metadata_digest
                == metadata
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest()
            && self.venue() == &surface.venue
            && self.provider_product == surface.provider_product
            && self.provider_channel == surface.provider_channel
            && self.channel == channel
            && metadata.is_effective_at(self.selected_at)
            && self.is_selected_at(self.selected_at)
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
    coordinates: KrakenNativeMarketCoordinates,
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
        definition: &InstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        depth: KrakenDepth,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            KrakenChannel::Book(depth),
            max_message_bytes,
        )
    }

    /// Constructs a trade-channel configuration with checksum-unsupported metadata.
    pub fn try_trades(
        metadata: SourceMetadata,
        definition: &InstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            KrakenChannel::Trades,
            max_message_bytes,
        )
    }

    /// Constructs a book configuration from a catalog-selected non-execution definition.
    pub fn try_new_selected(
        metadata: SourceMetadata,
        definition: &MarketDataInstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        depth: KrakenDepth,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_selected_channel(
            metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            KrakenChannel::Book(depth),
            max_message_bytes,
        )
    }

    /// Constructs a trade configuration from a catalog-selected non-execution definition.
    pub fn try_trades_selected(
        metadata: SourceMetadata,
        definition: &MarketDataInstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_selected_channel(
            metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            KrakenChannel::Trades,
            max_message_bytes,
        )
    }

    fn try_for_selected_channel(
        metadata: SourceMetadata,
        definition: &MarketDataInstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        channel: KrakenChannel,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        let coordinates = native_selected_market_coordinates(
            &metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            channel,
        )?;
        Self::from_coordinates(metadata, coordinates, max_message_bytes)
    }

    fn try_for_channel(
        metadata: SourceMetadata,
        definition: &InstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        selected_at: Timestamp,
        channel: KrakenChannel,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        let coordinates = native_market_coordinates(
            &metadata,
            definition,
            provider_identity_key,
            reference_selection,
            selected_at,
            channel,
        )?;
        Self::from_coordinates(metadata, coordinates, max_message_bytes)
    }

    fn from_coordinates(
        metadata: SourceMetadata,
        coordinates: KrakenNativeMarketCoordinates,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
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
            coordinates,
            max_message_bytes,
        })
    }

    /// Returns the exact allowlisted endpoint.
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the provider symbol handled by this source instance.
    pub fn symbol(&self) -> &str {
        self.coordinates.venue_symbol().as_str()
    }

    /// Returns the internal instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.coordinates.instrument()
    }

    /// Returns the independently registered provider channel.
    pub const fn channel(&self) -> KrakenChannel {
        self.coordinates.channel()
    }

    /// Returns the maximum exact frame size.
    pub const fn max_message_bytes(&self) -> usize {
        self.max_message_bytes.get()
    }

    /// Returns the mandatory exact provider-native coordinates for this generation.
    pub const fn native_coordinates(&self) -> &KrakenNativeMarketCoordinates {
        &self.coordinates
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

fn native_selected_market_coordinates(
    metadata: &SourceMetadata,
    definition: &MarketDataInstrumentDefinition,
    provider_identity_key: &ProviderIdentityKey,
    reference_selection: &KrakenReferenceSelectionEvidence,
    selected_at: Timestamp,
    channel: KrakenChannel,
) -> Result<KrakenNativeMarketCoordinates, KrakenConfigError> {
    if definition.reference_revision() != reference_selection.reference_revision()
        || definition.reference_payload_evidence().content_digest()
            != reference_selection.reference_payload_digest()
        || definition.effective_interval() != reference_selection.definition_validity()
    {
        return Err(KrakenConfigError::NativeIdentity);
    }
    native_market_coordinates_parts(
        metadata,
        definition.instrument_id(),
        definition.effective_interval(),
        definition.venue_mappings(),
        definition.provider_identity_at(
            provider_identity_key.source_id(),
            provider_identity_key.provider_instrument_id(),
            selected_at,
        ),
        provider_identity_key,
        reference_selection,
        selected_at,
        channel,
    )
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

fn native_market_coordinates(
    metadata: &SourceMetadata,
    definition: &InstrumentDefinition,
    provider_identity_key: &ProviderIdentityKey,
    reference_selection: &KrakenReferenceSelectionEvidence,
    selected_at: Timestamp,
    channel: KrakenChannel,
) -> Result<KrakenNativeMarketCoordinates, KrakenConfigError> {
    native_market_coordinates_parts(
        metadata,
        definition.instrument_id(),
        reference_selection.definition_validity(),
        definition.venue_mappings(),
        definition.provider_identity_at(
            provider_identity_key.source_id(),
            provider_identity_key.provider_instrument_id(),
            selected_at,
        ),
        provider_identity_key,
        reference_selection,
        selected_at,
        channel,
    )
}

#[allow(clippy::too_many_arguments)]
fn native_market_coordinates_parts(
    metadata: &SourceMetadata,
    instrument: InstrumentId,
    definition_validity: EffectiveInterval,
    venue_mappings: &[VenueMapping],
    record: Option<&market_squawk_domain::ProviderIdentityRecord>,
    provider_identity_key: &ProviderIdentityKey,
    reference_selection: &KrakenReferenceSelectionEvidence,
    selected_at: Timestamp,
    channel: KrakenChannel,
) -> Result<KrakenNativeMarketCoordinates, KrakenConfigError> {
    let provider_namespace =
        SourceId::try_from(KRAKEN_PROVIDER).map_err(|_| KrakenConfigError::NativeIdentity)?;
    if provider_identity_key.source_id() != &provider_namespace {
        return Err(KrakenConfigError::NativeIdentity);
    }
    let record = record.ok_or(KrakenConfigError::NativeIdentity)?;
    if record.key() != *provider_identity_key
        || record.instrument_id() != instrument
        || record.evidence().content_digest().bytes() == [0; 32]
    {
        return Err(KrakenConfigError::NativeIdentity);
    }
    let venue =
        VenueId::try_from(KRAKEN_PROVIDER).map_err(|_| KrakenConfigError::InvalidMetadata)?;
    let venue_mapping = venue_mappings
        .iter()
        .find(|mapping| mapping.venue_id() == &venue)
        .cloned()
        .ok_or(KrakenConfigError::VenueMapping)?;
    let surface = validate_public_surface(
        metadata,
        venue_mapping.venue_symbol().as_str(),
        instrument,
        channel,
    )?;
    if surface.venue != *venue_mapping.venue_id()
        || !metadata.is_effective_at(selected_at)
        || reference_selection.definition_published_at() > selected_at
    {
        return Err(KrakenConfigError::NativeIdentity);
    }
    let (valid_from, valid_until) = common_validity_interval(
        metadata,
        definition_validity,
        record.validity(),
        selected_at,
    )?;
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
        venue_mapping,
        reference_revision: reference_selection.reference_revision().clone(),
        reference_payload_digest: reference_selection.reference_payload_digest(),
        definition_revision_digest: reference_selection.definition_revision_digest(),
        definition_revision_sequence: reference_selection.definition_revision_sequence(),
        definition_published_at: reference_selection.definition_published_at(),
        reference_selection_digest: reference_selection.selection_receipt_digest(),
        provider_product: surface.provider_product,
        provider_channel: surface.provider_channel,
        instrument,
        channel,
        selected_at,
        valid_from,
        valid_until,
    })
}

fn common_validity_interval(
    metadata: &SourceMetadata,
    definition: market_squawk_domain::EffectiveInterval,
    identity: market_squawk_domain::EffectiveInterval,
    selected_at: Timestamp,
) -> Result<(Timestamp, Option<Timestamp>), KrakenConfigError> {
    let authorization = metadata.authorization().effective_interval();
    let coverage = metadata.coverage().effective_interval();
    let common_start = authorization
        .starts_at()
        .max(coverage.starts_at())
        .max(definition.starts_at())
        .max(identity.starts_at());
    let valid_until = [
        authorization.ends_at(),
        coverage.ends_at(),
        definition.ends_at(),
        identity.ends_at(),
    ]
    .into_iter()
    .flatten()
    .min();
    if selected_at < common_start || valid_until.is_some_and(|end| selected_at >= end) {
        return Err(KrakenConfigError::NativeIdentity);
    }
    Ok((common_start, valid_until))
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
    /// The current canonical definition has no exact Kraken venue-symbol mapping.
    #[error("Kraken venue mapping is absent from the canonical instrument definition")]
    VenueMapping,
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
