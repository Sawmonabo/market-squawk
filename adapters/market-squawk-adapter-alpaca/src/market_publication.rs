//! Provider-local preparation for durable Alpaca current-market publications.
//!
//! The only public producer consumes a validated frame through the stateful Alpaca decoder. Exact
//! configured mappings, provider-normalized typed fields, independent reference-master identity and currency,
//! canonical events, and native-lineage bytes therefore stay joined without accepting a
//! caller-created [`MarketEvent`] or open JSON value.

use std::collections::BTreeMap;

use bytes::Bytes;
use market_squawk_domain::{
    AggressorSide, AssetClass, CanonicalStateDigest, CanonicalizationRule, CoverageStatus,
    DataQuality, DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest, HaltTransition,
    InstrumentId, LiveEventClass, LiveEvidenceBinding, LiveProvenance, MarketDataQuoteEvent,
    MarketDataQuoteSide, MarketDataQuoteSize, MarketDataReference, MarketDataTradeEvent,
    MarketDataTradeQuantityUnit, MarketEvent, Money, PayloadHash, PayloadReference, RuleVersion,
    SourceIdentifier, Timestamp, TradeTakerOrderType, TradingHaltEvent,
};
use market_squawk_sources::{
    DecoderEvidence, ProviderBookLevel, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSealExpectation, ProviderCaptureSealRequest, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderEventMicrobatchMaterial,
    ProviderEventMicrobatchSealExpectation, ProviderEventMicrobatchToken, ProviderMarketEventBatch,
    ProviderMarketEventNativeLineageBatch, ProviderNativeLineageImplementation,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderTimestampEvidence,
    ProviderWholeCaptureToken, SealedProviderCaptureMaterial, SealedProviderEventMicrobatchBinding,
    SealedProviderPublicationBinding, SealedProviderResponseMarketEventBinding, SourceMetadata,
    ValidatedRawMarketFrame,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::AlpacaError;
use crate::boot_snapshot::AlpacaIexBootSnapshotEvidence;
use crate::config::{ALPACA_PROVIDER, IEX_VENUE, INDICATIVE_OPTIONS_VENUE};
use crate::error::AlpacaCaptureRejoinStage;

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
    evidence: DecoderEvidence,
    stream_identity: SourceIdentifier,
    bootstrap: Option<AlpacaIexBootSnapshotEvidence>,
    parts: AlpacaMarketEventPublicationParts,
}

impl AlpacaPreparedMarketEventPublication {
    pub(crate) fn try_from_decoded(
        metadata: &SourceMetadata,
        surface: AlpacaMarketEventSurface,
        configured: &BTreeMap<String, InstrumentId>,
        decoded: &market_squawk_sources::DecodedProviderBatch,
        references: &[MarketDataReference],
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
            || references.len() != configured.len()
        {
            return Err(AlpacaError::Protocol);
        }
        for (symbol, instrument) in configured {
            let mut selected = references.iter().filter(|reference| {
                reference.source_symbol().as_str() == symbol
                    && reference.instrument_id() == *instrument
            });
            let reference = selected.next().ok_or(AlpacaError::Protocol)?;
            if selected.next().is_some() || !surface.expected_asset_class(reference.asset_class()) {
                return Err(AlpacaError::Protocol);
            }
            reference
                .validate_at(evidence.received_at())
                .map_err(|_| AlpacaError::Protocol)?;
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
            let reference = exact_reference(configured, references, observation, surface)?;
            let event = canonical_event(
                metadata,
                live,
                surface,
                evidence,
                observation,
                reference,
                ingested_at,
            )?;
            native_rows.push(encode_native_row(ordinal, surface, observation, reference)?);
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
        .map(Vec::into_boxed_slice)
        .map_err(|_| AlpacaError::Serialization)?;
        Ok(Self {
            surface,
            evidence: evidence.clone(),
            stream_identity: live.provider_channel().as_source_identifier().clone(),
            bootstrap: None,
            parts: AlpacaMarketEventPublicationParts {
                batch: AlpacaQueuedCanonicalBatch::try_from_batch(batch)?,
                native_rows: native_rows.into_boxed_slice(),
                native_sidecar: sidecar,
                capture_ordinals,
            },
        })
    }

    /// Returns the exact provider-local surface selected by the decoder state.
    pub const fn surface(&self) -> AlpacaMarketEventSurface {
        self.surface
    }

    pub(crate) fn bind_boot_snapshot(&mut self, bootstrap: Option<AlpacaIexBootSnapshotEvidence>) {
        self.bootstrap = bootstrap;
    }

    /// Returns the exact decoder-owned dataset used when issuing capture material.
    pub fn dataset(&self) -> &SourceIdentifier {
        self.parts.batch.dataset()
    }

    /// Returns the source-owned channel used when issuing capture material.
    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    /// Joins this decode to the exact capture-admitted frame and splits the one-use physical seal.
    ///
    /// A bootstrap also requires the real transport response supplied by
    /// [`crate::AlpacaIexLiveSource::try_new_with_publication_handoff`]. A parser-created body
    /// cannot supply HTTP response authority. Stream frames retain absent transport-level
    /// sequence/exchange time, independently of their decoded event times.
    pub fn into_pending_publication(
        mut self,
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
    ) -> Result<(AlpacaMarketSealRejoin, ProviderCaptureSealRequest), AlpacaError> {
        self.validate_capture(frame, &capture)?;
        let (expectation, request) = if self.surface == AlpacaMarketEventSurface::IexBootSnapshot {
            let response = self
                .bootstrap
                .as_ref()
                .ok_or(AlpacaError::CaptureMaterial)?;
            if response.frame != *frame.frame() || response.http_status != 200 {
                return Err(AlpacaError::CaptureMaterial);
            }
            let [record] = capture.records() else {
                return Err(AlpacaError::CaptureMaterial);
            };
            let receipt = ProviderCaptureSetReceipt::try_new(
                self.evidence.binding().source_id().clone(),
                self.evidence.binding().metadata_revision().clone(),
                self.dataset().clone(),
                response.request_identity,
                ProviderCaptureTerminalDisposition::StandaloneResponse,
                vec![
                    ProviderCapturePageReceipt::try_new(
                        0,
                        response.request_identity,
                        None,
                        None,
                        response.http_status,
                        u64::try_from(self.evidence.frame_bytes())
                            .map_err(|_| AlpacaError::CaptureMaterial)?,
                        self.evidence.payload_digest(),
                        self.evidence.received_at(),
                    )
                    .map_err(|_| AlpacaError::CaptureMaterial)?,
                ],
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?;
            let response_record = market_squawk_platform::RawCaptureRecord::try_new_live(
                record.event_id(),
                std::sync::Arc::from(record.source()),
                record.connection_id(),
                Some(0),
                None,
                record.received_at(),
                Bytes::copy_from_slice(record.payload()),
            )
            .map_err(|_| AlpacaError::CaptureMaterial)?;
            let material = ProviderCaptureMaterial::try_new(receipt, vec![response_record])
                .map_err(|_| AlpacaError::CaptureMaterial)?;
            let (expectation, request) = material.into_whole_seal_parts();
            (AlpacaMarketSealExpectation::Response(expectation), request)
        } else {
            let (expectation, request) = capture.into_sealing_parts();
            (AlpacaMarketSealExpectation::Stream(expectation), request)
        };
        self.bootstrap = None;
        Ok((
            AlpacaMarketSealRejoin {
                publication: self,
                expectation,
            },
            request,
        ))
    }

    fn validate_capture(
        &self,
        validated: &ValidatedRawMarketFrame<'_>,
        capture: &ProviderEventMicrobatchMaterial,
    ) -> Result<(), AlpacaError> {
        self.evidence
            .currentness_lease()
            .validate_current()
            .map_err(|_| AlpacaError::CaptureMaterial)?;
        let frame = validated.frame();
        let receipt = capture.receipt();
        let [physical] = receipt.frames() else {
            return Err(AlpacaError::CaptureMaterial);
        };
        let [record] = capture.records() else {
            return Err(AlpacaError::CaptureMaterial);
        };
        if frame.binding() != self.evidence.binding()
            || frame.frame_id() != self.evidence.frame_id()
            || frame.received_at() != self.evidence.received_at()
            || frame.payload().len() != self.evidence.frame_bytes()
            || EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(frame.payload()).into(),
            ) != self.evidence.payload_digest()
            || receipt.source_id() != frame.source_id()
            || receipt.metadata_revision() != frame.metadata_revision()
            || receipt.dataset() != self.dataset()
            || receipt.stream_identity() != self.stream_identity()
            || physical.ordinal() != 0
            || physical.source_sequence().is_some()
            || physical.exchange_at().is_some()
            || physical.received_at() != frame.received_at()
            || physical.payload_digest() != self.evidence.payload_digest()
            || usize::try_from(physical.payload_bytes()).ok() != Some(self.evidence.frame_bytes())
            || physical.event_id() != *record.event_id().as_bytes()
            || physical.connection_id() != *record.connection_id().as_bytes()
            || record.source_sequence().is_some()
            || record.exchange_at().is_some()
            || record.payload() != frame.payload()
        {
            return Err(AlpacaError::CaptureMaterial);
        }
        Ok(())
    }

    /// Consumes a sealed boot-response token into the common immutable response-event binding.
    fn try_into_response_binding(
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
        let batch = batch.into_batch()?;
        let native_rows = native_rows
            .into_vec()
            .into_iter()
            .map(Bytes::from)
            .collect();
        let native_sidecar = Bytes::from(native_sidecar);
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::AlpacaIexMarketDataV1,
            &batch,
            native_rows,
            Some(native_sidecar),
        )
        .map_err(|source| AlpacaError::CaptureRejoin {
            stage: AlpacaCaptureRejoinStage::ResponseNativeLineage,
            source,
        })?;
        SealedProviderResponseMarketEventBinding::try_new(
            authority,
            batch,
            native,
            capture_ordinals,
        )
        .map_err(|source| AlpacaError::CaptureRejoin {
            stage: AlpacaCaptureRejoinStage::ResponseBinding,
            source,
        })
    }

    /// Consumes a sealed IEX or indicative-options frame into the common stream binding.
    fn try_into_event_microbatch_binding(
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
        let batch = batch.into_batch()?;
        let native_rows = native_rows
            .into_vec()
            .into_iter()
            .map(Bytes::from)
            .collect();
        let native_sidecar = Bytes::from(native_sidecar);
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            implementation,
            &batch,
            native_rows,
            Some(native_sidecar),
        )
        .map_err(|source| AlpacaError::CaptureRejoin {
            stage: AlpacaCaptureRejoinStage::StreamNativeLineage,
            source,
        })?;
        SealedProviderEventMicrobatchBinding::try_new(authority, batch, native, capture_ordinals)
            .map_err(|source| AlpacaError::CaptureRejoin {
                stage: AlpacaCaptureRejoinStage::StreamBinding,
                source,
            })
    }
}

/// Opaque adapter continuation accepting only the physical result of its exact seal request.
#[derive(Debug)]
pub struct AlpacaMarketSealRejoin {
    publication: AlpacaPreparedMarketEventPublication,
    expectation: AlpacaMarketSealExpectation,
}

#[derive(Debug)]
enum AlpacaMarketSealExpectation {
    Response(ProviderCaptureSealExpectation),
    Stream(ProviderEventMicrobatchSealExpectation),
}

impl AlpacaMarketSealRejoin {
    /// Exact owned byte buffers and checked shared decoder authority charge for queue admission.
    /// The raw seal request is separately charged by its owner; shared witness overhead is
    /// conservatively charged in full by both sides of the split.
    pub fn retained_bytes(&self) -> Result<usize, AlpacaError> {
        let publication = &self.publication;
        if publication.bootstrap.is_some() {
            return Err(AlpacaError::CaptureMaterial);
        }
        let parts = &publication.parts;
        let native_slots = std::mem::size_of::<Box<[u8]>>()
            .checked_mul(parts.native_rows.len())
            .ok_or(AlpacaError::Allocation)?;
        let native = parts
            .native_rows
            .iter()
            .try_fold(native_slots, |sum, row| sum.checked_add(row.len()))
            .ok_or(AlpacaError::Allocation)?;
        let canonical = parts
            .batch
            .dynamic_retained_bytes()
            .ok_or(AlpacaError::Allocation)?;
        let decoder = publication
            .evidence
            .dynamic_retained_bytes()
            .map_err(|_| AlpacaError::Allocation)?;
        std::mem::size_of::<Self>()
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|size| size.checked_add(native))
            .and_then(|size| size.checked_add(canonical))
            .and_then(|size| size.checked_add(decoder))
            .and_then(|size| size.checked_add(parts.native_sidecar.len()))
            .and_then(|size| {
                size.checked_add(
                    parts
                        .capture_ordinals
                        .capacity()
                        .checked_mul(std::mem::size_of::<u16>())?,
                )
            })
            .and_then(|size| size.checked_add(publication.stream_identity.retained_bytes()))
            .ok_or(AlpacaError::Allocation)
    }
    /// Consumes the exact one-use seal result into the existing common publication binding.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SealedProviderPublicationBinding, AlpacaError> {
        self.publication
            .evidence
            .currentness_lease()
            .validate_current()
            .map_err(|_| AlpacaError::PublicationSessionNotCurrent)?;
        match self.expectation {
            AlpacaMarketSealExpectation::Response(expectation) => {
                let authority = expectation
                    .try_rejoin(sealed)
                    .and_then(market_squawk_sources::RejoinedProviderCapture::try_into_whole)
                    .map_err(|source| AlpacaError::CaptureRejoin {
                        stage: AlpacaCaptureRejoinStage::ResponseSeal,
                        source,
                    })?;
                self.publication
                    .try_into_response_binding(authority)
                    .map(Into::into)
            }
            AlpacaMarketSealExpectation::Stream(expectation) => {
                let authority = expectation
                    .try_rejoin(sealed)
                    .map_err(|source| AlpacaError::CaptureRejoin {
                        stage: AlpacaCaptureRejoinStage::StreamSeal,
                        source,
                    })?;
                self.publication
                    .try_into_event_microbatch_binding(authority)
                    .map(Into::into)
            }
        }
    }
}

#[derive(Debug)]
struct AlpacaMarketEventPublicationParts {
    batch: AlpacaQueuedCanonicalBatch,
    native_rows: Box<[Box<[u8]>]>,
    native_sidecar: Box<[u8]>,
    capture_ordinals: Vec<u16>,
}

/// Private exact serialized canonical material keeps queue storage directly measurable.
/// Rehydration occurs only after the same raw seal rejoins; no public JSON constructor exists.
#[derive(Debug)]
struct AlpacaQueuedCanonicalBatch {
    source: market_squawk_domain::SourceId,
    revision: market_squawk_domain::MetadataRevision,
    dataset: SourceIdentifier,
    events: Box<[Box<[u8]>]>,
}
impl AlpacaQueuedCanonicalBatch {
    fn try_from_batch(batch: ProviderMarketEventBatch) -> Result<Self, AlpacaError> {
        let events = batch
            .events()
            .iter()
            .map(|event| {
                serde_json::to_vec(event)
                    .map(Vec::into_boxed_slice)
                    .map_err(|_| AlpacaError::Serialization)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        Ok(Self {
            source: batch.source_id().clone(),
            revision: batch.metadata_revision().clone(),
            dataset: batch.dataset().clone(),
            events,
        })
    }
    fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }
    fn into_batch(self) -> Result<ProviderMarketEventBatch, AlpacaError> {
        let events = self
            .events
            .into_vec()
            .into_iter()
            .map(|bytes| {
                serde_json::from_slice::<MarketEvent>(&bytes)
                    .map_err(|_| AlpacaError::Serialization)
            })
            .collect::<Result<Vec<_>, _>>()?;
        ProviderMarketEventBatch::try_new(self.source, self.revision, self.dataset, events)
            .map_err(|source| AlpacaError::CaptureRejoin {
                stage: AlpacaCaptureRejoinStage::CanonicalBatch,
                source,
            })
    }
    fn dynamic_retained_bytes(&self) -> Option<usize> {
        let slots = std::mem::size_of::<Box<[u8]>>().checked_mul(self.events.len())?;
        self.events
            .iter()
            .try_fold(slots, |sum, value| sum.checked_add(value.len()))?
            .checked_add(self.source.retained_bytes())?
            .checked_add(self.revision.as_source_identifier().retained_bytes())?
            .checked_add(self.dataset.retained_bytes())
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
    source_timestamp: Timestamp,
    reference: &'a MarketDataReference,
    venue: &'a str,
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

fn exact_reference<'a>(
    configured: &BTreeMap<String, InstrumentId>,
    references: &'a [MarketDataReference],
    observation: &ProviderNormalizedObservation,
    surface: AlpacaMarketEventSurface,
) -> Result<&'a MarketDataReference, AlpacaError> {
    let mut found = references.iter().filter(|reference| {
        reference.instrument_id() == observation.instrument()
            && configured.get(reference.source_symbol().as_str())
                == Some(&reference.instrument_id())
            && surface.expected_asset_class(reference.asset_class())
    });
    let reference = found.next().ok_or(AlpacaError::Protocol)?;
    if found.next().is_some() || observation.venue().as_str() != surface.venue() {
        return Err(AlpacaError::Protocol);
    }
    Ok(reference)
}

fn canonical_event(
    metadata: &SourceMetadata,
    live: &market_squawk_sources::LiveCoverageDeclaration,
    surface: AlpacaMarketEventSurface,
    evidence: &DecoderEvidence,
    observation: &ProviderNormalizedObservation,
    reference: &MarketDataReference,
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
    let payload = AlpacaCanonicalPayload::try_from_observation(observation, reference, surface)?;
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
        SourceIdentifier::try_from(reference.source_symbol().as_str())?,
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
    payload.into_event(provenance, reference.clone())
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AlpacaCanonicalPayload {
    Trade {
        trade_id: SourceIdentifier,
        price: Money,
        quantity: rust_decimal::Decimal,
        unit: MarketDataTradeQuantityUnit,
        aggressor: AggressorSide,
        taker_order_type: Option<TradeTakerOrderType>,
    },
    Quote {
        bid: Option<MarketDataQuoteSide>,
        ask: Option<MarketDataQuoteSide>,
    },
    TradingHalt {
        transition: HaltTransition,
        reason: SourceIdentifier,
    },
}

impl AlpacaCanonicalPayload {
    fn try_from_observation(
        observation: &ProviderNormalizedObservation,
        reference: &MarketDataReference,
        surface: AlpacaMarketEventSurface,
    ) -> Result<Self, AlpacaError> {
        match observation.payload() {
            ProviderObservationPayload::Trade {
                trade_id,
                price,
                quantity,
                aggressor,
                taker_order_type,
            } => {
                if quantity.value().decimal().fract() != rust_decimal::Decimal::ZERO {
                    return Err(AlpacaError::Protocol);
                }
                // Alpaca passes source trade volume. IEX equities count shares; option trades
                // count contracts independently of deliverable size or premium multiplier.
                let unit = match surface {
                    AlpacaMarketEventSurface::IexBootSnapshot
                    | AlpacaMarketEventSurface::IexStream => MarketDataTradeQuantityUnit::Shares,
                    AlpacaMarketEventSurface::IndicativeOptionsStream => {
                        MarketDataTradeQuantityUnit::Contracts
                    }
                };
                Ok(Self::Trade {
                    trade_id: trade_id.clone(),
                    price: Money::new(price.value().decimal(), reference.currency()),
                    quantity: quantity.value().decimal(),
                    unit,
                    aggressor: aggressor.side(),
                    taker_order_type: *taker_order_type,
                })
            }
            ProviderObservationPayload::Quote { bid, ask } => Ok(Self::Quote {
                bid: bid.as_ref().map(|level| canonical_level(level, reference)),
                ask: ask.as_ref().map(|level| canonical_level(level, reference)),
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
        let bytes = serde_json::to_vec(self).map_err(|_| AlpacaError::Serialization)?;
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/alpaca-source-market-event/v1\0");
        digest.update(bytes);
        Ok(CanonicalStateDigest::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
            CanonicalizationRule::new(
                SourceIdentifier::try_from(CANONICALIZATION_RULE)?,
                RuleVersion::new(1).map_err(|_| AlpacaError::Protocol)?,
            ),
        ))
    }

    fn into_event(
        self,
        provenance: LiveProvenance,
        reference: MarketDataReference,
    ) -> Result<MarketEvent, AlpacaError> {
        match self {
            Self::Trade {
                trade_id,
                price,
                quantity,
                unit,
                aggressor,
                taker_order_type,
            } => MarketDataTradeEvent::try_new(
                provenance,
                reference,
                trade_id,
                price,
                quantity,
                unit,
                aggressor,
                taker_order_type,
            )
            .map(MarketEvent::MarketDataTrade)
            .map_err(|_| AlpacaError::Protocol),
            Self::Quote { bid, ask } => {
                MarketDataQuoteEvent::try_new(provenance, reference, bid, ask)
                    .map(MarketEvent::MarketDataQuote)
                    .map_err(|_| AlpacaError::Protocol)
            }
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
    reference: &MarketDataReference,
) -> MarketDataQuoteSide {
    MarketDataQuoteSide::new(
        Money::new(level.price().value().decimal(), reference.currency()),
        MarketDataQuoteSize::UnresolvedUnit(level.quantity().value().decimal()),
    )
}

fn encode_native_row(
    ordinal: usize,
    surface: AlpacaMarketEventSurface,
    observation: &ProviderNormalizedObservation,
    reference: &MarketDataReference,
) -> Result<Box<[u8]>, AlpacaError> {
    serde_json::to_vec(&AlpacaMarketEventNativeV1 {
        version: 1,
        surface: surface.name(),
        canonical_row_ordinal: u32::try_from(ordinal).map_err(|_| AlpacaError::Protocol)?,
        provider_event_id: observation.source_identifier().as_str(),
        event_class: observation.event_class(),
        source_timestamp: observation_timestamp(observation)?,
        reference,
        venue: observation.venue().as_str(),
        provider_semantics: native_semantics(observation)?,
    })
    .map(Vec::into_boxed_slice)
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
