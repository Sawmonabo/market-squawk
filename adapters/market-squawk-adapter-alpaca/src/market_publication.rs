//! Provider-local preparation for durable Alpaca current-market publications.
//!
//! The only public producer consumes a validated frame through the stateful Alpaca decoder. Exact
//! configured mappings, provider-normalized typed fields, reference-master price/quantity terms,
//! canonical events, and native-lineage bytes therefore stay joined without accepting a
//! caller-created [`MarketEvent`] or open JSON value.

use std::collections::BTreeMap;

use bytes::Bytes;
use market_squawk_domain::{
    AggressorSide, AssetClass, BookLevel, CanonicalStateDigest, CanonicalizationRule,
    CoverageStatus, DataQuality, DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest,
    HaltTransition, InstrumentDefinition, LiveEventClass, LiveEvidenceBinding, LiveProvenance,
    MarketEvent, PayloadHash, PayloadReference, PriceTicks, QuantityLots, QuoteEvent, RuleVersion,
    SourceIdentifier, Timestamp, TradeEvent, TradeTakerOrderType, TradingHaltEvent,
};
use market_squawk_sources::{
    DecoderEvidence, ProviderBookLevel, ProviderEventMicrobatchToken, ProviderMarketEventBatch,
    ProviderMarketEventNativeLineageBatch, ProviderNativeLineageImplementation,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderTimestampEvidence,
    ProviderWholeCaptureToken, SealedProviderEventMicrobatchBinding,
    SealedProviderResponseMarketEventBinding, SourceMetadata,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::AlpacaError;
use crate::config::{
    ALPACA_PROVIDER, AlpacaProviderInstrumentCoordinate, IEX_VENUE, INDICATIVE_OPTIONS_VENUE,
};

const IEX_DATASET_PREFIX: &str = "alpaca:iex-market-events:v1:";
const INDICATIVE_OPTIONS_DATASET_PREFIX: &str = "alpaca:indicative-option-market-events:v1:";
const CANONICALIZATION_RULE: &str = "alpaca-decoded-market-event-v1";

/// Exact Alpaca current-data surface represented by one immutable publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlpacaMarketEventSurface {
    /// The mandatory authenticated REST snapshot preceding one IEX stream generation.
    IexBootSnapshot,
    /// Real-time IEX WebSocket events under the free Basic account.
    IexStream,
    /// Modified indicative option quotes and delayed option trades.
    IndicativeOptionsStream,
}

impl AlpacaMarketEventSurface {
    const fn name(self) -> &'static str {
        match self {
            Self::IexBootSnapshot => "iex_boot_snapshot",
            Self::IexStream => "iex_stream",
            Self::IndicativeOptionsStream => "indicative_options_stream",
        }
    }

    const fn quality(self) -> DataQuality {
        match self {
            Self::IexBootSnapshot | Self::IexStream => DataQuality::DirectUnverified,
            Self::IndicativeOptionsStream => DataQuality::Indicative,
        }
    }

    const fn venue(self) -> &'static str {
        match self {
            Self::IexBootSnapshot | Self::IexStream => IEX_VENUE,
            Self::IndicativeOptionsStream => INDICATIVE_OPTIONS_VENUE,
        }
    }

    const fn feed(self) -> &'static str {
        match self {
            Self::IexBootSnapshot | Self::IexStream => "iex",
            Self::IndicativeOptionsStream => "indicative",
        }
    }

    const fn source_identifier_prefix(self) -> &'static str {
        match self {
            Self::IexBootSnapshot | Self::IexStream => "alpaca:iex:",
            Self::IndicativeOptionsStream => "alpaca:indicative-options:",
        }
    }

    const fn dataset_prefix(self) -> &'static str {
        match self {
            Self::IexBootSnapshot | Self::IexStream => IEX_DATASET_PREFIX,
            Self::IndicativeOptionsStream => INDICATIVE_OPTIONS_DATASET_PREFIX,
        }
    }

    const fn dataset_surface(self) -> &'static str {
        match self {
            Self::IexBootSnapshot | Self::IexStream => "iex",
            Self::IndicativeOptionsStream => "indicative_options",
        }
    }

    fn expected_asset_class(self, actual: AssetClass) -> bool {
        match self {
            Self::IexBootSnapshot | Self::IexStream => {
                matches!(actual, AssetClass::Equity | AssetClass::Fund)
            }
            Self::IndicativeOptionsStream => actual == AssetClass::Option,
        }
    }

    const fn admits(self, class: LiveEventClass) -> bool {
        match self {
            Self::IexBootSnapshot => matches!(class, LiveEventClass::Trade | LiveEventClass::Quote),
            Self::IexStream => matches!(
                class,
                LiveEventClass::Trade | LiveEventClass::Quote | LiveEventClass::TradingHalt
            ),
            Self::IndicativeOptionsStream => {
                matches!(class, LiveEventClass::Trade | LiveEventClass::Quote)
            }
        }
    }
}

/// Complete decoder-owned current-event material for one closed shared implementation tag.
///
/// There is deliberately no public constructor. [`crate::AlpacaIexDecoder`] and
/// [`crate::AlpacaOptionsDecoder`] are the only producers, after parsing a validated raw frame and
/// matching every typed observation to the exact configured mapping and instrument definition.
#[derive(Debug)]
pub struct AlpacaPreparedMarketEventPublication {
    surface: AlpacaMarketEventSurface,
    parts: AlpacaMarketEventPublicationParts,
}

impl AlpacaPreparedMarketEventPublication {
    pub(crate) fn try_from_decoded(
        metadata: &SourceMetadata,
        surface: AlpacaMarketEventSurface,
        configured: &BTreeMap<String, std::sync::Arc<AlpacaProviderInstrumentCoordinate>>,
        decoded: &market_squawk_sources::DecodedProviderBatch,
        definitions: &[InstrumentDefinition],
        ingested_at: Timestamp,
    ) -> Result<Self, AlpacaError> {
        let evidence = decoded.evidence();
        let live = metadata.coverage().live().ok_or(AlpacaError::Protocol)?;
        if metadata.provider().as_str() != ALPACA_PROVIDER
            || metadata.quality_ceiling() != surface.quality()
            || evidence.binding().source_id() != metadata.source_id()
            || evidence.binding().metadata_revision() != metadata.revision()
            || evidence.received_at() > ingested_at
            || decoded.observations().is_empty()
        {
            return Err(AlpacaError::Protocol);
        }
        let dataset = publication_dataset(metadata, surface)?;
        let mut events = Vec::new();
        let mut native_rows = Vec::new();
        let mut capture_ordinals = Vec::new();
        events
            .try_reserve_exact(decoded.observations().len())
            .map_err(|_| AlpacaError::Allocation)?;
        native_rows
            .try_reserve_exact(decoded.observations().len())
            .map_err(|_| AlpacaError::Allocation)?;
        capture_ordinals
            .try_reserve_exact(decoded.observations().len())
            .map_err(|_| AlpacaError::Allocation)?;

        for (ordinal, observation) in decoded.observations().iter().enumerate() {
            let coordinate = exact_coordinate(configured, observation)?;
            let definition = exact_definition(definitions, coordinate, observation, surface)?;
            let event = canonical_event(
                metadata,
                live,
                surface,
                evidence,
                observation,
                definition,
                ingested_at,
            )?;
            native_rows.push(encode_native_row(
                ordinal,
                surface,
                coordinate,
                observation,
            )?);
            capture_ordinals.push(0);
            events.push(event);
        }
        let batch = ProviderMarketEventBatch::try_new(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            dataset.clone(),
            events,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        let sidecar = serde_json::to_vec(&AlpacaMarketBatchNativeV1 {
            version: 1,
            surface: surface.name(),
            dataset: dataset.as_str(),
            quality: surface.quality(),
            feed: surface.feed(),
            provider_product: live.provider_product().as_source_identifier().as_str(),
            provider_channel: live.provider_channel().as_source_identifier().as_str(),
            venue: surface.venue(),
            indicative_not_opra: surface == AlpacaMarketEventSurface::IndicativeOptionsStream,
            delayed_trade_nanos: if surface == AlpacaMarketEventSurface::IndicativeOptionsStream {
                Some(900_000_000_000_u64)
            } else {
                None
            },
            event_count: batch.events().len(),
        })
        .map(Bytes::from)
        .map_err(|_| AlpacaError::Serialization)?;
        Ok(Self {
            surface,
            parts: AlpacaMarketEventPublicationParts {
                batch,
                native_rows,
                native_sidecar: sidecar,
                capture_ordinals,
            },
        })
    }

    /// Returns the exact provider-local surface selected by the decoder state.
    pub const fn surface(&self) -> AlpacaMarketEventSurface {
        self.surface
    }

    /// Consumes a sealed boot-response token into the common immutable response-event binding.
    pub fn try_into_response_binding(
        self,
        authority: ProviderWholeCaptureToken,
    ) -> Result<SealedProviderResponseMarketEventBinding, AlpacaError> {
        if self.surface != AlpacaMarketEventSurface::IexBootSnapshot {
            return Err(AlpacaError::Protocol);
        }
        let AlpacaMarketEventPublicationParts {
            batch,
            native_rows,
            native_sidecar,
            capture_ordinals,
        } = self.parts;
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::AlpacaIexMarketDataV1,
            &batch,
            native_rows,
            Some(native_sidecar),
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        SealedProviderResponseMarketEventBinding::try_new(
            authority,
            batch,
            native,
            capture_ordinals,
        )
        .map_err(|_| AlpacaError::CaptureMaterial)
    }

    /// Consumes a sealed IEX or indicative-options frame into the common stream binding.
    pub fn try_into_event_microbatch_binding(
        self,
        authority: ProviderEventMicrobatchToken,
    ) -> Result<SealedProviderEventMicrobatchBinding, AlpacaError> {
        let implementation = match self.surface {
            AlpacaMarketEventSurface::IexStream => {
                ProviderNativeLineageImplementation::AlpacaIexMarketDataV1
            }
            AlpacaMarketEventSurface::IndicativeOptionsStream => {
                ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1
            }
            AlpacaMarketEventSurface::IexBootSnapshot => return Err(AlpacaError::Protocol),
        };
        let AlpacaMarketEventPublicationParts {
            batch,
            native_rows,
            native_sidecar,
            capture_ordinals,
        } = self.parts;
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            implementation,
            &batch,
            native_rows,
            Some(native_sidecar),
        )
        .map_err(|_| AlpacaError::CaptureMaterial)?;
        SealedProviderEventMicrobatchBinding::try_new(authority, batch, native, capture_ordinals)
            .map_err(|_| AlpacaError::CaptureMaterial)
    }
}

#[derive(Debug)]
struct AlpacaMarketEventPublicationParts {
    batch: ProviderMarketEventBatch,
    native_rows: Vec<Bytes>,
    native_sidecar: Bytes,
    capture_ordinals: Vec<u16>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaMarketEventNativeV1<'a> {
    version: u16,
    surface: &'static str,
    canonical_row_ordinal: u32,
    provider_event_id: &'a str,
    event_class: LiveEventClass,
    source_timestamp: Timestamp,
    provider_identity_source: &'a str,
    provider_instrument_id: &'a str,
    provider_identity_revision: &'a str,
    provider_identity_evidence: EvidenceDigest,
    provider_identity_valid_from: Timestamp,
    provider_identity_valid_until: Option<Timestamp>,
    venue: &'a str,
    venue_symbol: &'a str,
    canonical_instrument_id: market_squawk_domain::InstrumentId,
    coordinate_digest: EvidenceDigest,
    provider_semantics: AlpacaProviderSemanticsV1<'a>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaMarketBatchNativeV1<'a> {
    version: u16,
    surface: &'static str,
    dataset: &'a str,
    quality: DataQuality,
    feed: &'a str,
    provider_product: &'a str,
    provider_channel: &'a str,
    venue: &'static str,
    indicative_not_opra: bool,
    delayed_trade_nanos: Option<u64>,
    event_count: usize,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum AlpacaProviderSemanticsV1<'a> {
    Trade {
        trade_id: &'a str,
        price: &'a str,
        quantity: &'a str,
        aggressor_side: AggressorSide,
        provider_aggressor_code: Option<&'a str>,
        aggressor_rule: &'a str,
        taker_order_type: Option<TradeTakerOrderType>,
    },
    Quote {
        bid: Option<AlpacaProviderBookLevelV1<'a>>,
        ask: Option<AlpacaProviderBookLevelV1<'a>>,
    },
    TradingHalt {
        status: &'a str,
        status_rule: &'a str,
        transition: HaltTransition,
        reason: &'a str,
    },
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaProviderBookLevelV1<'a> {
    price: &'a str,
    quantity: &'a str,
}

fn exact_coordinate<'a>(
    configured: &'a BTreeMap<String, std::sync::Arc<AlpacaProviderInstrumentCoordinate>>,
    observation: &ProviderNormalizedObservation,
) -> Result<&'a AlpacaProviderInstrumentCoordinate, AlpacaError> {
    let mut matches = configured
        .values()
        .filter(|coordinate| {
            coordinate.instrument() == observation.instrument()
                && coordinate.venue() == observation.venue()
        })
        .map(std::sync::Arc::as_ref);
    let coordinate = matches.next().ok_or(AlpacaError::Protocol)?;
    if matches.next().is_some() {
        return Err(AlpacaError::Protocol);
    }
    Ok(coordinate)
}

fn exact_definition<'a>(
    definitions: &'a [InstrumentDefinition],
    coordinate: &AlpacaProviderInstrumentCoordinate,
    observation: &ProviderNormalizedObservation,
    surface: AlpacaMarketEventSurface,
) -> Result<&'a InstrumentDefinition, AlpacaError> {
    let mut matches = definitions
        .iter()
        .filter(|definition| definition.instrument_id() == coordinate.instrument());
    let definition = matches.next().ok_or(AlpacaError::Protocol)?;
    let provider_identity = definition.provider_identities().iter().find(|identity| {
        identity.key() == *coordinate.identity_key()
            && identity.metadata_revision() == coordinate.provider_identity_revision()
            && identity.evidence().content_digest() == coordinate.provider_identity_digest()
            && identity.validity() == coordinate.provider_identity_validity()
    });
    if matches.next().is_some()
        || !surface.expected_asset_class(definition.asset_class())
        || observation.instrument() != definition.instrument_id()
        || !definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id() == coordinate.venue()
                && mapping.venue_symbol() == coordinate.venue_symbol()
        })
        || provider_identity.is_none()
    {
        return Err(AlpacaError::Protocol);
    }
    Ok(definition)
}

fn canonical_event(
    metadata: &SourceMetadata,
    live: &market_squawk_sources::LiveCoverageDeclaration,
    surface: AlpacaMarketEventSurface,
    evidence: &DecoderEvidence,
    observation: &ProviderNormalizedObservation,
    definition: &InstrumentDefinition,
    ingested_at: Timestamp,
) -> Result<MarketEvent, AlpacaError> {
    let source_timestamp = observation_timestamp(observation)?;
    if !surface.admits(observation.event_class())
        || observation.venue().as_str() != surface.venue()
        || !observation
            .source_identifier()
            .as_str()
            .starts_with(surface.source_identifier_prefix())
        || !metadata.is_effective_at(evidence.received_at())
        || !metadata
            .authorization()
            .is_effective_at(evidence.received_at())
    {
        return Err(AlpacaError::Protocol);
    }
    let payload = AlpacaCanonicalPayload::try_from_observation(observation, definition)?;
    let canonical_state_digest = payload.canonical_digest()?;
    let binding = LiveEvidenceBinding::new(
        metadata.source_id().clone(),
        evidence
            .binding()
            .session_id()
            .as_source_identifier()
            .clone(),
        metadata.revision().clone(),
        metadata.authorization().basis().clone(),
        observation.venue().clone(),
        observation.instrument(),
        evidence.binding().connection_generation(),
        live.provider_product().clone(),
        live.provider_channel().clone(),
        observation.event_class(),
        observation.source_identifier().clone(),
        evidence.payload_digest(),
        canonical_state_digest,
        None,
    )
    .map_err(|_| AlpacaError::Protocol)?;
    let provenance = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        Some(source_timestamp),
        evidence.received_at(),
        evidence.received_at(),
        ingested_at,
        surface.quality(),
        CoverageStatus::Insufficient,
        PayloadReference::ContentHash(PayloadHash::new(
            evidence.payload_digest().algorithm(),
            evidence.payload_digest().bytes(),
        )),
    ))
    .map_err(|_| AlpacaError::Protocol)?;
    payload.into_event(provenance)
}

enum AlpacaCanonicalPayload {
    Trade {
        price: PriceTicks,
        quantity: QuantityLots,
        aggressor: AggressorSide,
        taker_order_type: Option<TradeTakerOrderType>,
    },
    Quote {
        bid: Option<BookLevel>,
        ask: Option<BookLevel>,
    },
    TradingHalt {
        transition: HaltTransition,
        reason: SourceIdentifier,
    },
}

impl AlpacaCanonicalPayload {
    fn try_from_observation(
        observation: &ProviderNormalizedObservation,
        definition: &InstrumentDefinition,
    ) -> Result<Self, AlpacaError> {
        match observation.payload() {
            ProviderObservationPayload::Trade {
                price,
                quantity,
                aggressor,
                taker_order_type,
                ..
            } => Ok(Self::Trade {
                price: PriceTicks::try_from_decimal(
                    price.value().decimal(),
                    definition.tick_size(),
                )
                .map_err(|_| AlpacaError::Protocol)?,
                quantity: QuantityLots::try_from_decimal(
                    quantity.value().decimal(),
                    definition.lot_size(),
                )
                .map_err(|_| AlpacaError::Protocol)?,
                aggressor: aggressor.side(),
                taker_order_type: *taker_order_type,
            }),
            ProviderObservationPayload::Quote { bid, ask } => Ok(Self::Quote {
                bid: bid
                    .as_ref()
                    .map(|level| canonical_level(level, definition))
                    .transpose()?,
                ask: ask
                    .as_ref()
                    .map(|level| canonical_level(level, definition))
                    .transpose()?,
            }),
            ProviderObservationPayload::TradingHalt {
                transition, reason, ..
            } => Ok(Self::TradingHalt {
                transition: *transition,
                reason: reason.clone(),
            }),
            ProviderObservationPayload::BookSnapshot(_)
            | ProviderObservationPayload::BookDelta(_)
            | ProviderObservationPayload::Auction { .. }
            | ProviderObservationPayload::InstrumentStatus { .. }
            | ProviderObservationPayload::CorporateAction { .. } => Err(AlpacaError::Protocol),
        }
    }

    fn canonical_digest(&self) -> Result<CanonicalStateDigest, AlpacaError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-decoded-market-event/v1\0");
        match self {
            Self::Trade {
                price,
                quantity,
                aggressor,
                taker_order_type,
            } => {
                digest.update([1]);
                digest.update(price.get().to_be_bytes());
                digest.update(quantity.get().to_be_bytes());
                digest.update([match aggressor {
                    AggressorSide::Buy => 1,
                    AggressorSide::Sell => 2,
                    AggressorSide::Unknown => 3,
                }]);
                digest.update([match taker_order_type {
                    None => 0,
                    Some(TradeTakerOrderType::Limit) => 1,
                    Some(TradeTakerOrderType::Market) => 2,
                }]);
            }
            Self::Quote { bid, ask } => {
                digest.update([2]);
                hash_level(&mut digest, *bid);
                hash_level(&mut digest, *ask);
            }
            Self::TradingHalt { transition, reason } => {
                digest.update([3]);
                digest.update([match transition {
                    HaltTransition::Halted => 1,
                    HaltTransition::Resumed => 2,
                }]);
                hash_text(&mut digest, reason.as_str())?;
            }
        }
        Ok(CanonicalStateDigest::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
            CanonicalizationRule::new(
                SourceIdentifier::try_from(CANONICALIZATION_RULE)?,
                RuleVersion::new(1).map_err(|_| AlpacaError::Protocol)?,
            ),
        ))
    }

    fn into_event(self, provenance: LiveProvenance) -> Result<MarketEvent, AlpacaError> {
        match self {
            Self::Trade {
                price,
                quantity,
                aggressor,
                taker_order_type,
            } => TradeEvent::new(provenance, price, quantity, aggressor, taker_order_type)
                .map(MarketEvent::Trade)
                .map_err(|_| AlpacaError::Protocol),
            Self::Quote { bid, ask } => QuoteEvent::new(provenance, bid, ask)
                .map(MarketEvent::Quote)
                .map_err(|_| AlpacaError::Protocol),
            Self::TradingHalt { transition, reason } => {
                TradingHaltEvent::new(provenance, transition, reason)
                    .map(MarketEvent::TradingHalt)
                    .map_err(|_| AlpacaError::Protocol)
            }
        }
    }
}

fn canonical_level(
    level: &ProviderBookLevel,
    definition: &InstrumentDefinition,
) -> Result<BookLevel, AlpacaError> {
    let price =
        PriceTicks::try_from_decimal(level.price().value().decimal(), definition.tick_size())
            .map_err(|_| AlpacaError::Protocol)?;
    let quantity =
        QuantityLots::try_from_decimal(level.quantity().value().decimal(), definition.lot_size())
            .map_err(|_| AlpacaError::Protocol)?;
    BookLevel::new(price, quantity).map_err(|_| AlpacaError::Protocol)
}

fn encode_native_row(
    ordinal: usize,
    surface: AlpacaMarketEventSurface,
    coordinate: &AlpacaProviderInstrumentCoordinate,
    observation: &ProviderNormalizedObservation,
) -> Result<Bytes, AlpacaError> {
    serde_json::to_vec(&AlpacaMarketEventNativeV1 {
        version: 1,
        surface: surface.name(),
        canonical_row_ordinal: u32::try_from(ordinal).map_err(|_| AlpacaError::Protocol)?,
        provider_event_id: observation.source_identifier().as_str(),
        event_class: observation.event_class(),
        source_timestamp: observation_timestamp(observation)?,
        provider_identity_source: coordinate.identity_key().source_id().as_str(),
        provider_instrument_id: coordinate.identity_key().provider_instrument_id().as_str(),
        provider_identity_revision: coordinate
            .provider_identity_revision()
            .as_source_identifier()
            .as_str(),
        provider_identity_evidence: coordinate.provider_identity_digest(),
        provider_identity_valid_from: coordinate.provider_identity_validity().starts_at(),
        provider_identity_valid_until: coordinate.provider_identity_validity().ends_at(),
        venue: coordinate.venue().as_str(),
        venue_symbol: coordinate.venue_symbol().as_str(),
        canonical_instrument_id: coordinate.instrument(),
        coordinate_digest: coordinate.binding_digest(),
        provider_semantics: native_semantics(observation)?,
    })
    .map(Bytes::from)
    .map_err(|_| AlpacaError::Serialization)
}

fn native_semantics(
    observation: &ProviderNormalizedObservation,
) -> Result<AlpacaProviderSemanticsV1<'_>, AlpacaError> {
    match observation.payload() {
        ProviderObservationPayload::Trade {
            trade_id,
            price,
            quantity,
            aggressor,
            taker_order_type,
        } => Ok(AlpacaProviderSemanticsV1::Trade {
            trade_id: trade_id.as_str(),
            price: price.value().as_str(),
            quantity: quantity.value().as_str(),
            aggressor_side: aggressor.side(),
            provider_aggressor_code: aggressor.provider_code().map(SourceIdentifier::as_str),
            aggressor_rule: aggressor.rule().provider_rule().as_str(),
            taker_order_type: *taker_order_type,
        }),
        ProviderObservationPayload::Quote { bid, ask } => Ok(AlpacaProviderSemanticsV1::Quote {
            bid: bid.as_ref().map(native_level),
            ask: ask.as_ref().map(native_level),
        }),
        ProviderObservationPayload::TradingHalt {
            status,
            transition,
            reason,
        } => Ok(AlpacaProviderSemanticsV1::TradingHalt {
            status: status.status().as_str(),
            status_rule: status.rule().provider_rule().as_str(),
            transition: *transition,
            reason: reason.as_str(),
        }),
        ProviderObservationPayload::BookSnapshot(_)
        | ProviderObservationPayload::BookDelta(_)
        | ProviderObservationPayload::Auction { .. }
        | ProviderObservationPayload::InstrumentStatus { .. }
        | ProviderObservationPayload::CorporateAction { .. } => Err(AlpacaError::Protocol),
    }
}

fn native_level(level: &ProviderBookLevel) -> AlpacaProviderBookLevelV1<'_> {
    AlpacaProviderBookLevelV1 {
        price: level.price().value().as_str(),
        quantity: level.quantity().value().as_str(),
    }
}

fn observation_timestamp(
    observation: &ProviderNormalizedObservation,
) -> Result<Timestamp, AlpacaError> {
    match observation.timestamp() {
        ProviderTimestampEvidence::Provided { value, .. } => Ok(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => Err(AlpacaError::Protocol),
    }
}

fn publication_dataset(
    metadata: &SourceMetadata,
    surface: AlpacaMarketEventSurface,
) -> Result<SourceIdentifier, AlpacaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-current-provider-dataset/v1\0");
    hash_text(&mut digest, metadata.source_id().as_str())?;
    hash_text(
        &mut digest,
        metadata.revision().as_source_identifier().as_str(),
    )?;
    hash_text(&mut digest, surface.dataset_surface())?;
    hash_text(&mut digest, surface.venue())?;
    SourceIdentifier::try_from(format!(
        "{}{}",
        surface.dataset_prefix(),
        lower_hex(digest.finalize().into())
    ))
    .map_err(Into::into)
}

fn hash_level(digest: &mut Sha256, level: Option<BookLevel>) {
    match level {
        Some(level) => {
            digest.update([1]);
            digest.update(level.price().get().to_be_bytes());
            digest.update(level.quantity().get().to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), AlpacaError> {
    digest.update(
        u32::try_from(value.len())
            .map_err(|_| AlpacaError::Protocol)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
