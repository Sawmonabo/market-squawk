//! Sealed platform-to-Kraken production profile.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use market_squawk_adapter_kraken::{
    KrakenChannel, KrakenConfig, KrakenConfigError, KrakenDepth, KrakenMetadataError,
    KrakenMetadataInput, KrakenReferenceSelectionEvidence, KrakenSocketHandoffConsumer,
    KrakenSource,
};
use market_squawk_data::{
    MarketDataInstrumentCatalogError, MarketDataInstrumentRecord,
    MarketDataProviderIdentitySelection,
};
use market_squawk_domain::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentDefinition,
    MarketDataInstrumentDefinition, MetadataRevision, ProviderIdentityKey, ProviderIdentityRecord,
    RevisionBoundPayloadEvidence, VenueId, VenueMapping,
};
use market_squawk_domain::{IdentityError, SourceId, SourceIdentifier, Timestamp};
#[cfg(test)]
use market_squawk_domain::{
    InstrumentDefinitionInput, ProviderIdentityEvidence, ProviderIdentityRecordInput,
    ProviderInstrumentId,
};
use market_squawk_live::{
    LiveSnapshotReader, RouteSnapshot, ShardKey, SnapshotCompleteness, StreamPhaseSnapshot,
    StreamSnapshot,
};
use market_squawk_platform::{KrakenAuthorizationAttestation, KrakenSourceConfig};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, FreshnessPolicy,
    LiveSourceGeneration, ProviderBudgetPolicy, SourceError, SourceMetadata,
    SourceMetadataProvider,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::supervisor::{ProductionSourceSupervisor, ProductionSupervisorError};

const BOOK_SOURCE_ID: &str = "kraken-public-book-v2";
const TRADE_SOURCE_ID: &str = "kraken-public-trades-v2";
const BOOK_IMPLEMENTATION_PROFILE_VERSION: &str = "kraken-book-v2-profile-2026-08-14";
const TRADE_IMPLEMENTATION_PROFILE_VERSION: &str = "kraken-trade-v2-profile-2026-08-14";
const PROFILE_EVIDENCE_DOMAIN: &[u8] = b"market-squawk/kraken-production-profile/v1\0";
const REQUESTS_PER_WINDOW: u32 = 8;
const REQUEST_WINDOW_NANOS: u64 = 1_000_000_000;
// Book and trade are separate exact WebSocket supervisors and may connect/subscribe concurrently.
// The shared provider scope therefore reserves capacity for both required channel operations.
const MAX_CONCURRENT_REQUESTS: u16 = 2;
const INITIAL_BACKOFF_NANOS: u64 = 250_000_000;
const MAXIMUM_BACKOFF_NANOS: u64 = 30_000_000_000;
const BACKOFF_JITTER_BASIS_POINTS: u16 = 2_000;
const MAX_CLOCK_SKEW_NANOS: u64 = 1_000_000_000;
const CURRENTNESS_OBSERVATION_INTERVAL: Duration = Duration::from_millis(10);

/// Complete immutable Kraken provider profile derived from strict local configuration.
#[derive(Debug)]
pub(super) struct ProductionKrakenProfile {
    adapter_config: KrakenConfig,
}

/// Exact pair of independently governed public Kraken channels used by the installed runtime.
///
/// Book and trade traffic cannot share one [`SourceMetadata`] value: Kraken supplies CRC32
/// integrity evidence for the L2 book and no corresponding checksum for trades. Keeping two
/// profiles preserves that distinction while allowing the application owner to start and stop
/// both channels as one product surface.
#[derive(Debug)]
pub(super) struct ProductionKrakenProfileSet {
    book: ProductionKrakenProfile,
    trades: ProductionKrakenProfile,
}

#[derive(Clone, Copy)]
enum KrakenProfileDefinition<'a> {
    Legacy(&'a InstrumentDefinition),
    Selected(&'a MarketDataInstrumentDefinition),
}

impl KrakenProfileDefinition<'_> {
    const fn instrument_id(self) -> market_squawk_domain::InstrumentId {
        match self {
            Self::Legacy(definition) => definition.instrument_id(),
            Self::Selected(definition) => definition.instrument_id(),
        }
    }
}

impl ProductionKrakenProfileSet {
    pub(super) fn try_from_selection(
        config: &KrakenSourceConfig,
        record: &MarketDataInstrumentRecord,
        selection: &MarketDataProviderIdentitySelection,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let selected = SelectedKrakenReference::try_new(config, record, selection)?;
        Ok(Self {
            book: ProductionKrakenProfile::try_for_selected_channel(
                config,
                &selected,
                KrakenChannel::Book(KrakenDepth::Ten),
            )?,
            trades: ProductionKrakenProfile::try_for_selected_channel(
                config,
                &selected,
                KrakenChannel::Trades,
            )?,
        })
    }

    #[cfg(test)]
    pub(super) fn try_from_at(
        config: &KrakenSourceConfig,
        at: Timestamp,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let (definition, provider_identity_key, reference_selection) =
            test_reference_selection(config, at)?;
        Ok(Self {
            book: ProductionKrakenProfile::try_for_channel_with_reference(
                config,
                &definition,
                &provider_identity_key,
                &reference_selection,
                at,
                KrakenChannel::Book(KrakenDepth::Ten),
            )?,
            trades: ProductionKrakenProfile::try_for_channel_with_reference(
                config,
                &definition,
                &provider_identity_key,
                &reference_selection,
                at,
                KrakenChannel::Trades,
            )?,
        })
    }

    pub(super) fn try_from_config(
        _config: &KrakenSourceConfig,
    ) -> Result<Self, ProductionKrakenProfileError> {
        // Root composition must query one digest-verified immutable reference record and pass its
        // opaque selection receipt into the provider-owned constructor. Configuration text and
        // process start time cannot mint provider identity or reference currentness.
        Err(ProductionKrakenProfileError::ReferenceSelectionRequired)
    }

    pub(super) fn into_channels(self) -> [ProductionKrakenProfile; 2] {
        [self.book, self.trades]
    }

    #[cfg(test)]
    pub(super) const fn book(&self) -> &ProductionKrakenProfile {
        &self.book
    }

    #[cfg(test)]
    pub(super) const fn trades(&self) -> &ProductionKrakenProfile {
        &self.trades
    }
}

impl ProductionKrakenProfile {
    pub(super) fn publication_config(&self) -> KrakenConfig {
        self.adapter_config.clone()
    }

    #[cfg(test)]
    pub(super) fn try_from_at(
        config: &KrakenSourceConfig,
        at: Timestamp,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let (definition, provider_identity_key, reference_selection) =
            test_reference_selection(config, at)?;
        Self::try_for_channel_with_reference(
            config,
            &definition,
            &provider_identity_key,
            &reference_selection,
            at,
            KrakenChannel::Book(KrakenDepth::Ten),
        )
    }

    fn try_for_selected_channel(
        config: &KrakenSourceConfig,
        selected: &SelectedKrakenReference<'_>,
        channel: KrakenChannel,
    ) -> Result<Self, ProductionKrakenProfileError> {
        Self::try_for_channel_parts(
            config,
            KrakenProfileDefinition::Selected(selected.definition),
            selected.provider_identity,
            selected.venue_mapping,
            &selected.provider_identity_key,
            &selected.reference_evidence,
            selected.selected_at,
            channel,
        )
    }

    #[cfg(test)]
    fn try_for_channel_with_reference(
        config: &KrakenSourceConfig,
        definition: &InstrumentDefinition,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        at: Timestamp,
        channel: KrakenChannel,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let attestation = config.authorization();
        if attestation.provider().as_str() != "kraken" {
            return Err(ProductionKrakenProfileError::AuthorizationMismatch);
        }
        if !attestation.is_effective_at(at) {
            return Err(ProductionKrakenProfileError::AuthorizationNotEffective);
        }
        let provider_identity = definition
            .provider_identity_at(
                provider_identity_key.source_id(),
                provider_identity_key.provider_instrument_id(),
                at,
            )
            .ok_or(ProductionKrakenProfileError::NativeIdentity)?;
        let venue = VenueId::try_from("kraken")?;
        let venue_mapping = definition
            .venue_mappings()
            .iter()
            .find(|mapping| mapping.venue_id() == &venue)
            .ok_or(ProductionKrakenProfileError::NativeIdentity)?;
        Self::try_for_channel_parts(
            config,
            KrakenProfileDefinition::Legacy(definition),
            provider_identity,
            venue_mapping,
            provider_identity_key,
            reference_selection,
            at,
            channel,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_for_channel_parts(
        config: &KrakenSourceConfig,
        definition: KrakenProfileDefinition<'_>,
        provider_identity: &ProviderIdentityRecord,
        venue_mapping: &VenueMapping,
        provider_identity_key: &ProviderIdentityKey,
        reference_selection: &KrakenReferenceSelectionEvidence,
        at: Timestamp,
        channel: KrakenChannel,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let attestation = config.authorization();
        if attestation.provider().as_str() != "kraken" {
            return Err(ProductionKrakenProfileError::AuthorizationMismatch);
        }
        if !attestation.is_effective_at(at) {
            return Err(ProductionKrakenProfileError::AuthorizationNotEffective);
        }
        let evidence_input = KrakenProfileEvidence::try_for_channel(
            config,
            definition.instrument_id(),
            provider_identity,
            venue_mapping,
            reference_selection,
            channel,
        )?;
        let encoded = serde_json::to_vec(&evidence_input)
            .map_err(|_error| ProductionKrakenProfileError::EvidenceSerialization)?;
        let mut hasher = Sha256::new();
        hasher.update(PROFILE_EVIDENCE_DOMAIN);
        hasher.update(encoded);
        let digest: [u8; 32] = hasher.finalize().into();
        let profile_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest,
        ));
        let effective = attestation.effective_interval();
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::PublicInterface,
            attestation.basis().clone(),
            attestation.evidence().clone(),
            effective,
        );
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(attestation.provider().clone(), &authorization)?,
            nonzero_u32(REQUESTS_PER_WINDOW)?,
            nonzero_u64(REQUEST_WINDOW_NANOS)?,
            nonzero_u16(MAX_CONCURRENT_REQUESTS)?,
            BackoffPolicy::try_new(
                nonzero_u64(INITIAL_BACKOFF_NANOS)?,
                nonzero_u64(MAXIMUM_BACKOFF_NANOS)?,
                BACKOFF_JITTER_BASIS_POINTS,
            )?,
        )?;
        let freshness_nanos = duration_nanos(config.freshness())?;
        let freshness = FreshnessPolicy::try_new(
            freshness_nanos,
            freshness_nanos,
            freshness_nanos,
            freshness_nanos,
            MAX_CLOCK_SKEW_NANOS,
        )?;
        let revision = RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(content_addressed_revision(digest)?),
            profile_evidence.clone(),
        );
        let metadata_input = match channel {
            KrakenChannel::Book(_) => KrakenMetadataInput::new(
                SourceId::try_from(BOOK_SOURCE_ID)?,
                revision,
                authorization,
                profile_evidence,
                effective,
                definition.instrument_id(),
                freshness,
                budget,
            ),
            KrakenChannel::Trades => KrakenMetadataInput::new_trades(
                SourceId::try_from(TRADE_SOURCE_ID)?,
                revision,
                authorization,
                profile_evidence,
                effective,
                definition.instrument_id(),
                freshness,
                budget,
            ),
        };
        let metadata = metadata_input.try_build()?;
        let adapter_config = match (channel, definition) {
            (KrakenChannel::Book(depth), KrakenProfileDefinition::Legacy(definition)) => {
                KrakenConfig::try_new(
                    metadata,
                    definition,
                    provider_identity_key,
                    reference_selection,
                    at,
                    depth,
                    config.max_frame_bytes(),
                )?
            }
            (KrakenChannel::Trades, KrakenProfileDefinition::Legacy(definition)) => {
                KrakenConfig::try_trades(
                    metadata,
                    definition,
                    provider_identity_key,
                    reference_selection,
                    at,
                    config.max_frame_bytes(),
                )?
            }
            (KrakenChannel::Book(depth), KrakenProfileDefinition::Selected(definition)) => {
                KrakenConfig::try_new_selected(
                    metadata,
                    definition,
                    provider_identity_key,
                    reference_selection,
                    at,
                    depth,
                    config.max_frame_bytes(),
                )?
            }
            (KrakenChannel::Trades, KrakenProfileDefinition::Selected(definition)) => {
                KrakenConfig::try_trades_selected(
                    metadata,
                    definition,
                    provider_identity_key,
                    reference_selection,
                    at,
                    config.max_frame_bytes(),
                )?
            }
        };
        Ok(Self { adapter_config })
    }

    pub(super) fn metadata(&self) -> &SourceMetadata {
        self.adapter_config.metadata()
    }

    pub(super) fn endpoint(&self) -> &str {
        self.adapter_config.endpoint().as_str()
    }

    pub(super) const fn channel(&self) -> KrakenChannel {
        self.adapter_config.channel()
    }

    pub(super) const fn source_key(&self) -> &'static str {
        match self.adapter_config.channel() {
            KrakenChannel::Book(_) => BOOK_SOURCE_ID,
            KrakenChannel::Trades => TRADE_SOURCE_ID,
        }
    }

    pub(super) fn try_source_with_publication_handoff(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<(KrakenSource, KrakenSocketHandoffConsumer), SourceError> {
        KrakenSource::try_new_with_publication_handoff(self.adapter_config.clone(), generation)
    }

    #[cfg(all(test, debug_assertions))]
    pub(super) fn with_local_endpoint_for_test(
        self,
        endpoint: &str,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let Self { adapter_config } = self;
        Ok(Self {
            adapter_config: adapter_config.with_local_endpoint_for_test(endpoint)?,
        })
    }
}

impl TryFrom<&KrakenSourceConfig> for ProductionKrakenProfile {
    type Error = ProductionKrakenProfileError;

    fn try_from(_config: &KrakenSourceConfig) -> Result<Self, Self::Error> {
        Err(ProductionKrakenProfileError::ReferenceSelectionRequired)
    }
}

struct SelectedKrakenReference<'a> {
    definition: &'a MarketDataInstrumentDefinition,
    provider_identity: &'a ProviderIdentityRecord,
    venue_mapping: &'a VenueMapping,
    provider_identity_key: ProviderIdentityKey,
    reference_evidence: KrakenReferenceSelectionEvidence,
    selected_at: Timestamp,
}

impl<'a> SelectedKrakenReference<'a> {
    fn try_new(
        config: &KrakenSourceConfig,
        record: &'a MarketDataInstrumentRecord,
        selection: &MarketDataProviderIdentitySelection,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let query = selection.query();
        let exact = selection.exact_receipt()?;
        let provider = SourceId::try_from("kraken")?;
        let venue = VenueId::try_from("kraken")?;
        let definition = record.definition();

        if query.source_id() != &provider
            || query.provider_instrument_id().as_str() != config.symbol()
            || exact.instrument_id() != definition.instrument_id()
            || exact.instrument_id() != config.definition().instrument_id()
            || record.revision_digest() != exact.definition_revision_digest()
            || record.revision_sequence() != exact.definition_revision_sequence()
            || record.published_at() != exact.definition_published_at()
            || definition.reference_revision() != exact.definition_reference_revision()
            || definition.reference_payload_evidence().content_digest()
                != exact.definition_reference_payload_digest()
            || !interval_contains(definition.effective_interval(), query.effective_at())
            || !exact.matching_venues().contains(&venue)
        {
            return Err(ProductionKrakenProfileError::NativeIdentity);
        }

        let provider_identity = definition
            .provider_identity_at(
                query.source_id(),
                query.provider_instrument_id(),
                query.effective_at(),
            )
            .ok_or(ProductionKrakenProfileError::NativeIdentity)?;
        if provider_identity.metadata_revision() != exact.provider_identity_revision()
            || provider_identity.evidence().content_digest()
                != exact.provider_identity_payload_digest()
            || provider_identity.validity() != exact.provider_identity_validity()
        {
            return Err(ProductionKrakenProfileError::NativeIdentity);
        }

        let venue_mapping = definition
            .venue_mappings()
            .iter()
            .find(|mapping| mapping.venue_id() == &venue)
            .filter(|mapping| mapping.venue_symbol().as_str() == config.symbol())
            .ok_or(ProductionKrakenProfileError::NativeIdentity)?;
        let provider_identity_key = ProviderIdentityKey::new(
            query.source_id().clone(),
            query.provider_instrument_id().clone(),
        );
        let reference_evidence = KrakenReferenceSelectionEvidence::try_new(
            exact.definition_reference_revision().clone(),
            exact.definition_reference_payload_digest(),
            exact.definition_revision_digest(),
            exact.definition_revision_sequence(),
            exact.definition_published_at(),
            definition.effective_interval(),
            selection.selection_digest(),
        )?;
        Ok(Self {
            definition,
            provider_identity,
            venue_mapping,
            provider_identity_key,
            reference_evidence,
            selected_at: query.effective_at(),
        })
    }
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    at >= interval.starts_at() && interval.ends_at().is_none_or(|end| at < end)
}

#[derive(Serialize)]
struct KrakenProfileEvidence<'a> {
    implementation_profile_version: &'static str,
    channel: &'static str,
    endpoint: &'a str,
    symbol: &'a str,
    instrument_id: market_squawk_domain::InstrumentId,
    provider_identity_source: &'a str,
    provider_instrument_id: &'a str,
    provider_identity_revision: &'a str,
    provider_identity_digest: EvidenceDigest,
    provider_identity_validity: EffectiveInterval,
    venue: &'a str,
    venue_symbol: &'a str,
    reference_revision: &'a str,
    reference_payload_digest: EvidenceDigest,
    definition_revision_digest: EvidenceDigest,
    definition_revision_sequence: u32,
    definition_published_at: Timestamp,
    definition_validity: EffectiveInterval,
    reference_selection_digest: EvidenceDigest,
    depth: Option<usize>,
    snapshot: bool,
    freshness_nanos: u64,
    max_frame_bytes: usize,
    subscription_ack_timeout_nanos: u64,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    authorization: &'a KrakenAuthorizationAttestation,
}

impl<'a> KrakenProfileEvidence<'a> {
    fn try_for_channel(
        config: &'a KrakenSourceConfig,
        instrument_id: market_squawk_domain::InstrumentId,
        provider_identity: &'a ProviderIdentityRecord,
        venue_mapping: &'a market_squawk_domain::VenueMapping,
        reference_selection: &'a KrakenReferenceSelectionEvidence,
        channel: KrakenChannel,
    ) -> Result<Self, ProductionKrakenProfileError> {
        let controls = config.control_limits();
        let (implementation_profile_version, channel_name, depth) = match channel {
            KrakenChannel::Book(depth) => (
                BOOK_IMPLEMENTATION_PROFILE_VERSION,
                "book",
                Some(depth.get()),
            ),
            KrakenChannel::Trades => (TRADE_IMPLEMENTATION_PROFILE_VERSION, "trade", None),
        };
        Ok(Self {
            implementation_profile_version,
            channel: channel_name,
            endpoint: config.endpoint(),
            symbol: config.symbol(),
            instrument_id,
            provider_identity_source: provider_identity.source_id().as_str(),
            provider_instrument_id: provider_identity.provider_instrument_id().as_str(),
            provider_identity_revision: provider_identity
                .metadata_revision()
                .as_source_identifier()
                .as_str(),
            provider_identity_digest: provider_identity.evidence().content_digest(),
            provider_identity_validity: provider_identity.validity(),
            venue: venue_mapping.venue_id().as_str(),
            venue_symbol: venue_mapping.venue_symbol().as_str(),
            reference_revision: reference_selection
                .reference_revision()
                .as_source_identifier()
                .as_str(),
            reference_payload_digest: reference_selection.reference_payload_digest(),
            definition_revision_digest: reference_selection.definition_revision_digest(),
            definition_revision_sequence: reference_selection.definition_revision_sequence(),
            definition_published_at: reference_selection.definition_published_at(),
            definition_validity: reference_selection.definition_validity(),
            reference_selection_digest: reference_selection.selection_receipt_digest(),
            depth,
            snapshot: true,
            freshness_nanos: duration_nanos(config.freshness())?,
            max_frame_bytes: config.max_frame_bytes().get(),
            subscription_ack_timeout_nanos: duration_nanos(config.subscription_ack_timeout())?,
            control_message_capacity: controls.message_capacity().get(),
            control_byte_capacity: controls.byte_capacity().get(),
            authorization: config.authorization(),
        })
    }
}

#[cfg(test)]
fn test_reference_selection(
    config: &KrakenSourceConfig,
    selected_at: Timestamp,
) -> Result<
    (
        InstrumentDefinition,
        ProviderIdentityKey,
        KrakenReferenceSelectionEvidence,
    ),
    ProductionKrakenProfileError,
> {
    let provider = SourceId::try_from("kraken")?;
    let provider_instrument = ProviderInstrumentId::try_from(config.symbol())?;
    let key = ProviderIdentityKey::new(provider.clone(), provider_instrument.clone());
    let definition = config.definition();
    let venue = VenueId::try_from("kraken")?;
    let venue_mapping = definition
        .venue_mappings()
        .iter()
        .find(|mapping| mapping.venue_id() == &venue)
        .filter(|mapping| mapping.venue_symbol().as_str() == config.symbol())
        .ok_or(ProductionKrakenProfileError::NativeIdentity)?;

    if definition
        .provider_identity_conflicts()
        .iter()
        .any(|conflict| conflict.key() == &key)
    {
        return Err(ProductionKrakenProfileError::NativeIdentity);
    }
    if definition
        .provider_identity_at(&provider, &provider_instrument, selected_at)
        .is_some()
    {
        return Ok((
            definition.clone(),
            key,
            test_reference_selection_evidence(config)?,
        ));
    }
    if definition
        .provider_identities()
        .iter()
        .any(|record| record.key() == key)
    {
        // A configured assertion exists but is not effective now. Silently inventing a successor
        // would discard the provider-authored revision edge required by the identity registry.
        return Err(ProductionKrakenProfileError::NativeIdentity);
    }

    let validity = config.authorization().effective_interval();
    let record = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: definition.instrument_id(),
        source_id: provider,
        provider_instrument_id: provider_instrument,
        evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [0x91; 32],
        )),
        source_timestamp: None,
        observed_at: validity.starts_at(),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
            "kraken-test-reference-identity-v1",
        )?),
        validity,
        supersedes: None,
    });
    let mut provider_identities = Vec::new();
    provider_identities
        .try_reserve_exact(definition.provider_identities().len().saturating_add(1))
        .map_err(|_error| ProductionKrakenProfileError::Allocation)?;
    provider_identities.extend(definition.provider_identities().iter().cloned());
    provider_identities.push(record);
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: definition.instrument_id(),
        definition_revision: definition.definition_revision(),
        asset_class: definition.asset_class(),
        primary_denomination: definition.primary_denomination(),
        quote_currency: definition.quote_currency(),
        tick_size: definition.tick_size(),
        lot_size: definition.lot_size(),
        contract_multiplier: definition.contract_multiplier(),
        venue_mappings: definition.venue_mappings().to_vec(),
        provider_identities,
        identifiers: definition.identifiers().to_vec(),
        trading_status: definition.trading_status(),
    })?;
    Ok((definition, key, test_reference_selection_evidence(config)?))
}

#[cfg(test)]
fn test_reference_selection_evidence(
    config: &KrakenSourceConfig,
) -> Result<KrakenReferenceSelectionEvidence, ProductionKrakenProfileError> {
    let validity = config.authorization().effective_interval();
    Ok(KrakenReferenceSelectionEvidence::try_new(
        MetadataRevision::new(SourceIdentifier::try_from("kraken-test-reference-v1")?),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [0xa1; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [0xa2; 32]),
        1,
        validity.starts_at(),
        validity,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [0xa3; 32]),
    )?)
}

fn content_addressed_revision(
    digest: [u8; 32],
) -> Result<SourceIdentifier, ProductionKrakenProfileError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut revision = String::with_capacity(74);
    revision.push_str("kraken-v2-");
    for byte in digest {
        revision.push(char::from(HEX[usize::from(byte >> 4)]));
        revision.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(SourceIdentifier::try_from(revision)?)
}

/// Channel label used by the atomic two-supervisor owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum KrakenPublicChannel {
    Book,
    Trades,
}

/// Bounded currentness check over Kraken's exact native snapshot topology.
///
/// The observer owns no publication authority and retains only a cloneable bounded reader plus the
/// closed route/source identities admitted before network access. A read failure or incomplete
/// snapshot fails closed. Transient resynchronization does not cancel either supervisor; it only
/// withdraws composite currentness until both exact channel generations are healthy again.
#[derive(Debug)]
pub(super) struct KrakenPublicCurrentnessObserver {
    snapshots: LiveSnapshotReader,
    routes: Arc<[ShardKey]>,
    book_source: SourceId,
    trade_source: SourceId,
}

impl KrakenPublicCurrentnessObserver {
    pub(super) fn try_new(
        snapshots: LiveSnapshotReader,
        routes: &[ShardKey],
        book_source: SourceId,
        trade_source: SourceId,
    ) -> Result<Self, KrakenPublicSupervisorSetError> {
        if routes.is_empty()
            || book_source == trade_source
            || routes
                .iter()
                .enumerate()
                .any(|(index, route)| routes[index.saturating_add(1)..].contains(route))
        {
            return Err(KrakenPublicSupervisorSetError::InvalidCurrentnessTopology);
        }
        let mut retained_routes = Vec::new();
        retained_routes
            .try_reserve_exact(routes.len())
            .map_err(|_error| KrakenPublicSupervisorSetError::Allocation)?;
        retained_routes.extend(routes.iter().cloned());
        Ok(Self {
            snapshots,
            routes: retained_routes.into(),
            book_source,
            trade_source,
        })
    }

    fn is_current(&self) -> bool {
        let Ok(at) = system_timestamp() else {
            return false;
        };
        let Ok(lease) = self.snapshots.try_load_all() else {
            return false;
        };
        let mut observed_routes = 0_usize;
        for shard in lease.snapshots() {
            if shard.route_dimension().completeness() != SnapshotCompleteness::Complete {
                return false;
            }
            let Some(next_count) = observed_routes.checked_add(shard.routes().len()) else {
                return false;
            };
            observed_routes = next_count;
        }
        if observed_routes != self.routes.len() {
            return false;
        }
        for expected in self.routes.iter() {
            let mut matched = None;
            for shard in lease.snapshots() {
                for route in shard.routes() {
                    if route.route() == expected {
                        if matched.is_some() {
                            return false;
                        }
                        matched = Some(route);
                    }
                }
            }
            let Some(route) = matched else {
                return false;
            };
            if !self.route_is_current(expected, route, at) {
                return false;
            }
        }
        true
    }

    fn route_is_current(&self, expected: &ShardKey, route: &RouteSnapshot, at: Timestamp) -> bool {
        if route.stream_dimension().completeness() != SnapshotCompleteness::Complete
            || route.status_dimension().completeness() != SnapshotCompleteness::Complete
            || route.streams().len() != 2
        {
            return false;
        }
        let mut book = None;
        let mut trades = None;
        for stream in route.streams() {
            if stream.source() == &self.book_source {
                if book.replace(stream).is_some() {
                    return false;
                }
            } else if stream.source() == &self.trade_source {
                if trades.replace(stream).is_some() {
                    return false;
                }
            } else {
                return false;
            }
        }
        book.is_some_and(|stream| book_stream_is_current(expected, stream, at))
            && trades.is_some_and(|stream| trade_stream_is_current(expected, stream, at))
    }
}

fn base_stream_is_current(
    expected: &ShardKey,
    stream: &StreamSnapshot,
    channel: &str,
    at: Timestamp,
) -> bool {
    stream.venue() == expected.venue()
        && stream.instrument() == expected.instrument()
        && stream.provider_channel().as_source_identifier().as_str() == channel
        && stream.generation_current()
        && stream.phase() == StreamPhaseSnapshot::Healthy
        && stream.source_valid_until() >= at
}

fn book_stream_is_current(expected: &ShardKey, stream: &StreamSnapshot, at: Timestamp) -> bool {
    base_stream_is_current(expected, stream, "book-v2", at)
        && stream.snapshot_initialized()
        && stream.bid_dimension().completeness() == SnapshotCompleteness::Complete
        && stream.ask_dimension().completeness() == SnapshotCompleteness::Complete
}

fn trade_stream_is_current(expected: &ShardKey, stream: &StreamSnapshot, at: Timestamp) -> bool {
    base_stream_is_current(expected, stream, "trade-v2", at)
        && stream.last_trade().is_some_and(|trade| {
            trade.connection_generation() == stream.connection_generation()
                && stream.trading_status() == Some(trade.trading_status())
                && trade.qualification_valid_until() >= at
        })
}

#[derive(Debug)]
struct KrakenPublicSupervisorTask {
    channel: KrakenPublicChannel,
    task: JoinHandle<Result<(), ProductionSupervisorError>>,
}

/// Atomic owner for the independently governed public Kraken book and trade supervisors.
///
/// Both sources must publish startup readiness before this owner is returned. Any early failure
/// cancels and reaps the sibling, and normal shutdown applies one shared deadline to the pair.
#[derive(Debug)]
pub(super) struct KrakenPublicSupervisorSet {
    cancellation: CancellationToken,
    tasks: Vec<KrakenPublicSupervisorTask>,
    currentness: KrakenPublicCurrentnessObserver,
}

impl KrakenPublicSupervisorSet {
    pub(super) async fn start(
        book: ProductionSourceSupervisor,
        trades: ProductionSourceSupervisor,
        parent_cancellation: CancellationToken,
        cleanup_timeout: Duration,
        currentness: KrakenPublicCurrentnessObserver,
    ) -> Result<Self, KrakenPublicSupervisorSetError> {
        if parent_cancellation.is_cancelled() {
            return Err(KrakenPublicSupervisorSetError::Cancelled);
        }
        if cleanup_timeout.is_zero() {
            return Err(KrakenPublicSupervisorSetError::InvalidCleanupTimeout);
        }
        // Reject an unrepresentable bound before either durable supervisor is spawned. The
        // failure paths still handle a later clock-range edge without detaching either task.
        cleanup_deadline(cleanup_timeout)?;
        let mut tasks = Vec::new();
        tasks
            .try_reserve_exact(2)
            .map_err(|_error| KrakenPublicSupervisorSetError::Allocation)?;
        let mut readiness = FuturesUnordered::new();
        let cancellation = parent_cancellation.child_token();

        for (channel, supervisor) in [
            (KrakenPublicChannel::Book, book),
            (KrakenPublicChannel::Trades, trades),
        ] {
            let (ready, receiver) = oneshot::channel();
            let run_cancellation = cancellation.clone();
            let terminal_cancellation = cancellation.clone();
            tasks.push(KrakenPublicSupervisorTask {
                channel,
                task: tokio::spawn(async move {
                    let outcome = supervisor.run(run_cancellation, ready).await;
                    // Either channel leaving its run loop makes the pair non-current. Cancel the
                    // sibling immediately; the owner reaps and reports both bounded outcomes.
                    terminal_cancellation.cancel();
                    outcome
                }),
            });
            readiness.push(async move { (channel, receiver.await) });
        }

        let mut ready_count = 0_usize;
        while let Some((channel, result)) = readiness.next().await {
            match result {
                Ok(()) => {
                    ready_count += 1;
                }
                Err(_closed) => {
                    cancellation.cancel();
                    // `Vec::pop` reaps the last entry first. Prefer the channel whose readiness
                    // sender closed so its terminal supervisor error remains the primary cause.
                    tasks.sort_by_key(|task| task.channel == channel);
                    let deadline = match cleanup_deadline(cleanup_timeout) {
                        Ok(deadline) => deadline,
                        Err(error) => {
                            abort_tasks(&mut tasks).await;
                            return Err(error);
                        }
                    };
                    return Err(match reap_tasks(&mut tasks, deadline).await {
                        Some(error) => error,
                        None => KrakenPublicSupervisorSetError::ExitedBeforeReadiness { channel },
                    });
                }
            }
        }
        if ready_count != 2
            || cancellation.is_cancelled()
            || tasks.iter().any(|task| task.task.is_finished())
        {
            cancellation.cancel();
            let deadline = match cleanup_deadline(cleanup_timeout) {
                Ok(deadline) => deadline,
                Err(error) => {
                    abort_tasks(&mut tasks).await;
                    return Err(error);
                }
            };
            return Err(reap_tasks(&mut tasks, deadline).await.unwrap_or(
                KrakenPublicSupervisorSetError::ExitedBeforeReadiness {
                    channel: KrakenPublicChannel::Book,
                },
            ));
        }
        let currentness_deadline = match cleanup_deadline(cleanup_timeout) {
            Ok(deadline) => deadline,
            Err(error) => {
                cancellation.cancel();
                abort_tasks(&mut tasks).await;
                return Err(error);
            }
        };
        while !currentness.is_current() {
            if parent_cancellation.is_cancelled() {
                cancellation.cancel();
                return Err(reap_tasks(&mut tasks, currentness_deadline)
                    .await
                    .unwrap_or(KrakenPublicSupervisorSetError::Cancelled));
            }
            if cancellation.is_cancelled() || tasks.iter().any(|task| task.task.is_finished()) {
                cancellation.cancel();
                return Err(reap_tasks(&mut tasks, currentness_deadline)
                    .await
                    .unwrap_or(KrakenPublicSupervisorSetError::ExitedBeforeReadiness {
                        channel: KrakenPublicChannel::Book,
                    }));
            }
            let now = Instant::now();
            if now >= currentness_deadline {
                cancellation.cancel();
                let _cleanup_error = reap_tasks(&mut tasks, currentness_deadline).await;
                return Err(KrakenPublicSupervisorSetError::CurrentnessDeadline);
            }
            let observation_at = now
                .checked_add(CURRENTNESS_OBSERVATION_INTERVAL)
                .map_or(currentness_deadline, |candidate| {
                    candidate.min(currentness_deadline)
                });
            tokio::time::sleep_until(tokio::time::Instant::from_std(observation_at)).await;
        }
        if parent_cancellation.is_cancelled() {
            cancellation.cancel();
            return Err(reap_tasks(&mut tasks, currentness_deadline)
                .await
                .unwrap_or(KrakenPublicSupervisorSetError::Cancelled));
        }
        if cancellation.is_cancelled() || tasks.iter().any(|task| task.task.is_finished()) {
            cancellation.cancel();
            return Err(reap_tasks(&mut tasks, currentness_deadline)
                .await
                .unwrap_or(KrakenPublicSupervisorSetError::ExitedBeforeReadiness {
                    channel: KrakenPublicChannel::Book,
                }));
        }
        Ok(Self {
            cancellation,
            tasks,
            currentness,
        })
    }

    pub(super) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.tasks.iter().all(|task| !task.task.is_finished())
            && self.currentness.is_current()
    }

    pub(super) async fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<(), KrakenPublicSupervisorSetError> {
        self.cancellation.cancel();
        match reap_tasks(&mut self.tasks, deadline).await {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for KrakenPublicSupervisorSet {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

async fn reap_tasks(
    tasks: &mut Vec<KrakenPublicSupervisorTask>,
    deadline: Instant,
) -> Option<KrakenPublicSupervisorSetError> {
    let mut first_error = None;
    while let Some(mut owned) = tasks.pop() {
        let outcome = tokio::select! {
            biased;
            outcome = &mut owned.task => Some(outcome),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => None,
        };
        let error = match outcome {
            Some(Ok(Ok(()))) => None,
            Some(Ok(Err(source))) => Some(KrakenPublicSupervisorSetError::Supervisor {
                channel: owned.channel,
                source,
            }),
            Some(Err(source)) => Some(KrakenPublicSupervisorSetError::Task {
                channel: owned.channel,
                source,
            }),
            None => {
                owned.task.abort();
                let _aborted = owned.task.await;
                Some(KrakenPublicSupervisorSetError::ShutdownDeadline)
            }
        };
        if first_error.is_none() {
            first_error = error;
        }
    }
    first_error
}

async fn abort_tasks(tasks: &mut Vec<KrakenPublicSupervisorTask>) {
    while let Some(owned) = tasks.pop() {
        owned.task.abort();
        let _aborted = owned.task.await;
    }
}

fn cleanup_deadline(cleanup_timeout: Duration) -> Result<Instant, KrakenPublicSupervisorSetError> {
    Instant::now()
        .checked_add(cleanup_timeout)
        .ok_or(KrakenPublicSupervisorSetError::DeadlineRange)
}

/// Atomic public-Kraken supervisor-set startup or shutdown failure.
#[derive(Debug, Error)]
pub(super) enum KrakenPublicSupervisorSetError {
    #[error("public Kraken supervisor-set allocation failed")]
    Allocation,
    #[error("public Kraken supervisor-set operation was cancelled")]
    Cancelled,
    #[error("public Kraken supervisor-set cleanup timeout must be non-zero")]
    InvalidCleanupTimeout,
    #[error("public Kraken supervisor-set deadline cannot be represented")]
    DeadlineRange,
    #[error("public Kraken currentness observer topology is invalid")]
    InvalidCurrentnessTopology,
    #[error("public Kraken channels did not become atomically current before the startup deadline")]
    CurrentnessDeadline,
    #[error("public Kraken {channel:?} supervisor exited before startup readiness")]
    ExitedBeforeReadiness { channel: KrakenPublicChannel },
    #[error("public Kraken {channel:?} supervisor failed: {source}")]
    Supervisor {
        channel: KrakenPublicChannel,
        #[source]
        source: ProductionSupervisorError,
    },
    #[error("public Kraken {channel:?} supervisor task failed: {source}")]
    Task {
        channel: KrakenPublicChannel,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("public Kraken supervisors exceeded their shared shutdown deadline")]
    ShutdownDeadline,
}

fn duration_nanos(value: Duration) -> Result<u64, ProductionKrakenProfileError> {
    u64::try_from(value.as_nanos()).map_err(|_error| ProductionKrakenProfileError::DurationRange)
}

fn system_timestamp() -> Result<Timestamp, ProductionKrakenProfileError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ProductionKrakenProfileError::ClockRange)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| ProductionKrakenProfileError::ClockRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn nonzero_u16(value: u16) -> Result<NonZeroU16, ProductionKrakenProfileError> {
    NonZeroU16::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

fn nonzero_u32(value: u32) -> Result<NonZeroU32, ProductionKrakenProfileError> {
    NonZeroU32::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, ProductionKrakenProfileError> {
    NonZeroU64::new(value).ok_or(ProductionKrakenProfileError::InvalidStaticPolicy)
}

/// Kraken production-profile validation failure.
#[derive(Debug, Error)]
pub enum ProductionKrakenProfileError {
    #[error("a digest-verified durable Kraken reference selection is required")]
    ReferenceSelectionRequired,
    #[error("Kraken catalog identity selection is invalid")]
    Catalog(#[from] MarketDataInstrumentCatalogError),
    #[error("Kraken production profile allocation failed")]
    Allocation,
    #[error("Kraken production profile identity is invalid")]
    Identity(#[from] IdentityError),
    #[error("Kraken production native identity assertion is invalid")]
    NativeIdentity,
    #[error("Kraken production instrument definition is invalid")]
    Instrument(#[from] market_squawk_domain::InstrumentError),
    #[error("Kraken source metadata is invalid")]
    Metadata(#[from] KrakenMetadataError),
    #[error("Kraken adapter configuration is invalid")]
    Adapter(#[from] KrakenConfigError),
    #[error("Kraken production profile evidence could not be encoded")]
    EvidenceSerialization,
    #[error("Kraken production duration exceeds the supported nanosecond range")]
    DurationRange,
    #[error("Kraken authorization attestation names another provider")]
    AuthorizationMismatch,
    #[error("Kraken authorization attestation is not effective at composition time")]
    AuthorizationNotEffective,
    #[error("Kraken production static policy contains a zero bound")]
    InvalidStaticPolicy,
    #[error("Kraken production system wall clock is invalid")]
    ClockRange,
    #[error("Kraken source network/budget policy is invalid")]
    NetworkPolicy(#[from] market_squawk_sources::NetworkPolicyError),
    #[error("Kraken source policy is invalid")]
    SourcePolicy(#[from] market_squawk_sources::SourceMetadataError),
}
