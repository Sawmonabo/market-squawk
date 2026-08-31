//! Provider-local preparation for durable Alpaca current-market publications.
//!
//! This module deliberately stops immediately before the shared native-lineage implementation
//! tag. Alpaca IEX and Alpaca indicative options need distinct closed tags in
//! `market-squawk-sources`; using the existing historical-bar tag would make persisted evidence
//! lie. The returned parts are otherwise complete and non-cloneable: canonical events, exact
//! provider-native row semantics, batch semantics, and raw-frame/page ordinals stay joined until
//! the application consumes them into the shared sealed publication contract.

use bytes::Bytes;
use market_squawk_domain::{
    DataQuality, EvidenceDigest, LiveEventClass, LiveProvenance, MarketEvent, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{ProviderMarketEventBatch, SourceMetadata};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::config::{
    ALPACA_PROVIDER, AlpacaProviderInstrumentCoordinate, IEX_VENUE, INDICATIVE_OPTIONS_VENUE,
};
use crate::{
    AlpacaError, AlpacaIexLiveConfig, AlpacaInstrumentMapping, AlpacaOptionMapping,
    AlpacaOptionsLiveConfig,
};

const IEX_DATASET_PREFIX: &str = "alpaca:iex-market-events:v1:";
const INDICATIVE_OPTIONS_DATASET_PREFIX: &str = "alpaca:indicative-option-market-events:v1:";

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

    fn admits(self, class: LiveEventClass) -> bool {
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

/// One exact canonical event, provider-native semantic projection, and raw-frame/page coordinate.
#[derive(Clone, Debug)]
pub struct AlpacaMarketEventRecord {
    event: MarketEvent,
    native_semantics: Value,
    capture_ordinal: u16,
    coordinate: AlpacaProviderInstrumentCoordinate,
    surface: AlpacaMarketEventSurface,
}

impl AlpacaMarketEventRecord {
    /// Binds one IEX event to the exact provider identity and venue mapping that decoded it.
    pub fn try_iex(
        surface: AlpacaMarketEventSurface,
        mapping: &AlpacaInstrumentMapping,
        event: MarketEvent,
        native_semantics: Value,
        capture_ordinal: u16,
    ) -> Result<Self, AlpacaError> {
        if !matches!(
            surface,
            AlpacaMarketEventSurface::IexBootSnapshot | AlpacaMarketEventSurface::IexStream
        ) || records.iter().any(|record| {
            !config
                .mappings()
                .iter()
                .any(|mapping| mapping.provider_coordinate() == &record.coordinate)
        }) {
            return Err(AlpacaError::Protocol);
        }
        Self::try_new(
            surface,
            mapping.provider_coordinate(),
            event,
            native_semantics,
            capture_ordinal,
        )
    }

    /// Binds one indicative option event to its exact provider identity and venue mapping.
    pub fn try_indicative_option(
        mapping: &AlpacaOptionMapping,
        event: MarketEvent,
        native_semantics: Value,
        capture_ordinal: u16,
    ) -> Result<Self, AlpacaError> {
        Self::try_new(
            AlpacaMarketEventSurface::IndicativeOptionsStream,
            mapping.provider_coordinate(),
            event,
            native_semantics,
            capture_ordinal,
        )
    }

    fn try_new(
        surface: AlpacaMarketEventSurface,
        coordinate: &AlpacaProviderInstrumentCoordinate,
        event: MarketEvent,
        native_semantics: Value,
        capture_ordinal: u16,
    ) -> Result<Self, AlpacaError> {
        let provenance = event_provenance(&event);
        let class = event_class(&event);
        let effective_at = provenance
            .source_timestamp()
            .unwrap_or(provenance.received_at());
        if !native_semantics.is_object()
            || !surface.admits(class)
            || coordinate.venue().as_str() != surface.venue()
            || provenance.instrument_id() != Some(coordinate.instrument())
            || provenance.venue_id() != Some(coordinate.venue())
            || provenance.quality() != surface.quality()
            || !provenance
                .source_identifier()
                .as_str()
                .starts_with(surface.source_identifier_prefix())
            || !coordinate.is_effective_at(effective_at)
        {
            return Err(AlpacaError::Protocol);
        }
        Ok(Self {
            event,
            native_semantics,
            capture_ordinal,
            coordinate: coordinate.clone(),
            surface,
        })
    }
}

/// Complete provider-local current-event material awaiting only its shared implementation tag.
#[derive(Debug)]
pub struct AlpacaPreparedMarketEventPublication {
    surface: AlpacaMarketEventSurface,
    parts: AlpacaMarketEventPublicationParts,
}

impl AlpacaPreparedMarketEventPublication {
    /// Prepares an exact IEX snapshot or stream publication under the selected free-IEX profile.
    pub fn try_iex(
        config: &AlpacaIexLiveConfig,
        surface: AlpacaMarketEventSurface,
        records: Vec<AlpacaMarketEventRecord>,
    ) -> Result<Self, AlpacaError> {
        if !matches!(
            surface,
            AlpacaMarketEventSurface::IexBootSnapshot | AlpacaMarketEventSurface::IexStream
        ) {
            return Err(AlpacaError::Protocol);
        }
        Self::try_new(config.metadata(), surface, records)
    }

    /// Prepares a modified/delayed indicative-options stream publication without OPRA claims.
    pub fn try_indicative_options(
        config: &AlpacaOptionsLiveConfig,
        records: Vec<AlpacaMarketEventRecord>,
    ) -> Result<Self, AlpacaError> {
        if records.iter().any(|record| {
            !config
                .mappings()
                .iter()
                .any(|mapping| mapping.provider_coordinate() == &record.coordinate)
        }) {
            return Err(AlpacaError::Protocol);
        }
        Self::try_new(
            config.metadata(),
            AlpacaMarketEventSurface::IndicativeOptionsStream,
            records,
        )
    }

    fn try_new(
        metadata: &SourceMetadata,
        surface: AlpacaMarketEventSurface,
        records: Vec<AlpacaMarketEventRecord>,
    ) -> Result<Self, AlpacaError> {
        if metadata.provider().as_str() != ALPACA_PROVIDER
            || metadata.quality_ceiling() != surface.quality()
            || records.is_empty()
            || records.iter().any(|record| record.surface != surface)
        {
            return Err(AlpacaError::Protocol);
        }
        let live = metadata.coverage().live().ok_or(AlpacaError::Protocol)?;
        let dataset = publication_dataset(metadata, surface)?;
        let mut events = Vec::new();
        let mut native_rows = Vec::new();
        let mut capture_ordinals = Vec::new();
        events
            .try_reserve_exact(records.len())
            .map_err(|_| AlpacaError::Allocation)?;
        native_rows
            .try_reserve_exact(records.len())
            .map_err(|_| AlpacaError::Allocation)?;
        capture_ordinals
            .try_reserve_exact(records.len())
            .map_err(|_| AlpacaError::Allocation)?;
        for (ordinal, record) in records.into_iter().enumerate() {
            let provenance = event_provenance(&record.event);
            let class = event_class(&record.event);
            if provenance.source_id() != metadata.source_id()
                || provenance.binding().metadata_revision() != metadata.revision()
                || provenance.binding().provider_product() != live.provider_product()
                || provenance.binding().provider_channel() != live.provider_channel()
                || provenance.binding().event_class() != class
                || live.rule_for(class, None).is_none()
                || !metadata.is_effective_at(provenance.received_at())
            {
                return Err(AlpacaError::Protocol);
            }
            native_rows.push(encode_native_row(ordinal, &record)?);
            capture_ordinals.push(record.capture_ordinal);
            events.push(record.event);
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

    /// Returns the exact provider-local surface.
    pub const fn surface(&self) -> AlpacaMarketEventSurface {
        self.surface
    }

    /// Consumes the non-cloneable provider-local material.
    ///
    /// Integration must select the matching closed shared native-lineage implementation and then
    /// construct either `SealedProviderResponseMarketEventBinding` (boot snapshot) or
    /// `SealedProviderEventMicrobatchBinding` (stream). No other semantic input is missing.
    pub fn into_parts(self) -> AlpacaMarketEventPublicationParts {
        self.parts
    }
}

/// Non-cloneable handoff to the common sealed current-event publication boundary.
#[derive(Debug)]
pub struct AlpacaMarketEventPublicationParts {
    batch: ProviderMarketEventBatch,
    native_rows: Vec<Bytes>,
    native_sidecar: Bytes,
    capture_ordinals: Vec<u16>,
}

impl AlpacaMarketEventPublicationParts {
    /// Consumes all provider-local publication inputs without cloning authority-bearing state.
    pub fn into_parts(self) -> (ProviderMarketEventBatch, Vec<Bytes>, Bytes, Vec<u16>) {
        (
            self.batch,
            self.native_rows,
            self.native_sidecar,
            self.capture_ordinals,
        )
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct AlpacaMarketEventNativeV1<'a> {
    version: u16,
    surface: &'static str,
    canonical_row_ordinal: u32,
    provider_event_id: &'a str,
    event_class: LiveEventClass,
    source_timestamp: Option<Timestamp>,
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
    provider_semantics: &'a Value,
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

fn encode_native_row(
    ordinal: usize,
    record: &AlpacaMarketEventRecord,
) -> Result<Bytes, AlpacaError> {
    let provenance = event_provenance(&record.event);
    let coordinate = &record.coordinate;
    serde_json::to_vec(&AlpacaMarketEventNativeV1 {
        version: 1,
        surface: record.surface.name(),
        canonical_row_ordinal: u32::try_from(ordinal).map_err(|_| AlpacaError::Protocol)?,
        provider_event_id: provenance.source_identifier().as_str(),
        event_class: event_class(&record.event),
        source_timestamp: provenance.source_timestamp(),
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
        provider_semantics: &record.native_semantics,
    })
    .map(Bytes::from)
    .map_err(|_| AlpacaError::Serialization)
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

fn event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(event) => event.provenance(),
        MarketEvent::Quote(event) => event.provenance(),
        MarketEvent::BookSnapshot(event) => event.provenance(),
        MarketEvent::BookDelta(event) => event.provenance(),
        MarketEvent::Auction(event) => event.provenance(),
        MarketEvent::TradingHalt(event) => event.provenance(),
        MarketEvent::InstrumentStatus(event) => event.provenance(),
        MarketEvent::CorporateAction(event) => event.provenance(),
    }
}

const fn event_class(event: &MarketEvent) -> LiveEventClass {
    match event {
        MarketEvent::Trade(_) => LiveEventClass::Trade,
        MarketEvent::Quote(_) => LiveEventClass::Quote,
        MarketEvent::BookSnapshot(_) => LiveEventClass::BookSnapshot,
        MarketEvent::BookDelta(_) => LiveEventClass::BookDelta,
        MarketEvent::Auction(_) => LiveEventClass::Auction,
        MarketEvent::TradingHalt(_) => LiveEventClass::TradingHalt,
        MarketEvent::InstrumentStatus(_) => LiveEventClass::InstrumentStatus,
        MarketEvent::CorporateAction(_) => LiveEventClass::CorporateAction,
    }
}
