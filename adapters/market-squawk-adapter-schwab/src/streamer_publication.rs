//! Seal-first Schwab Streamer Level-One quote publication.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, LiveEventClass, LiveProvenance,
    LotSize, MarketDepth, ProviderChannel, ProviderProduct, SourceIdentifier, TickSize, Timestamp,
    VenueId,
};
use market_squawk_sources::{
    ProviderCaptureError, ProviderMarketEventBatch, ProviderMarketEventNativeLineageBatch,
    ProviderNativeLineageImplementation, SealedProviderEventMicrobatchBinding,
};
use serde::Serialize;
use thiserror::Error;

use crate::streamer::StreamerContent;
use crate::{
    MarketDataService, NativeScalar, SchwabCanonicalError, SchwabMarketDataDelay,
    SchwabMarketDataQualification, SchwabQuoteAbstention, SchwabQuoteCanonicalOutcome,
    SchwabResolvedProviderIdentity, SchwabSealedStreamerCapture, SchwabStreamerFamilyDoctorHandoff,
    SchwabStreamerFieldDictionary, SchwabStreamerFrameSealEvidence, StreamerNativeValue,
    StreamerNestedField, canonicalize_streamer_batch, canonicalize_streamer_quote_record,
};

/// Provider/feed/venue/depth/quality evidence for one Level-One Streamer quote record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerQuoteMarketDataEvidence {
    service: MarketDataService,
    reference_venue_id: VenueId,
    qualification: SchwabMarketDataQualification,
}

impl SchwabStreamerQuoteMarketDataEvidence {
    /// Constructs exact market evidence for one selected Level-One service.
    pub fn try_new(
        reference_venue_id: VenueId,
        qualification: SchwabMarketDataQualification,
    ) -> Result<Self, SchwabStreamerPublicationError> {
        let service = qualification
            .streamer_service()
            .ok_or(SchwabStreamerPublicationError::InvalidEvidence)?;
        if !is_level_one(service)
            || qualification.depth().canonical() != Some(MarketDepth::TopOfBook)
        {
            return Err(SchwabStreamerPublicationError::InvalidEvidence);
        }
        Ok(Self {
            service,
            reference_venue_id,
            qualification,
        })
    }

    /// Exact provider Streamer service.
    pub const fn service(&self) -> MarketDataService {
        self.service
    }

    /// Exact qualified provider feed label.
    pub const fn feed(&self) -> &SourceIdentifier {
        self.qualification.feed()
    }

    /// Exact venue represented by this record.
    pub const fn venue_id(&self) -> &VenueId {
        &self.reference_venue_id
    }

    /// Exact admitted depth; Level-One quote publication requires top-of-book.
    pub const fn depth(&self) -> MarketDepth {
        MarketDepth::TopOfBook
    }

    /// Explicit delay evidence.
    pub const fn delay(&self) -> SchwabMarketDataDelay {
        self.qualification.delay()
    }

    /// Exact archival quality retained by the canonical event.
    pub const fn quality(&self) -> DataQuality {
        self.qualification.quality()
    }

    /// Exact provider-product binding retained by canonical provenance.
    pub const fn provider_product(&self) -> &ProviderProduct {
        self.qualification.provider_product()
    }

    /// Exact provider-channel binding retained by canonical provenance.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        self.qualification.provider_channel()
    }

    /// Nonzero SHA-256 qualification evidence for the supplied market semantics.
    pub const fn qualification_evidence(&self) -> EvidenceDigest {
        self.qualification.observation_evidence()
    }
}

/// Exact logical frame/data/content coordinate and semantic inputs for one quote record.
#[derive(Debug)]
pub struct SchwabStreamerQuoteRecordRequest {
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_ordinal: u16,
    dictionary: SchwabStreamerFieldDictionary,
    identity: SchwabResolvedProviderIdentity,
    instrument_id: InstrumentId,
    source_identifier: SourceIdentifier,
    provenance: LiveProvenance,
    tick_size: TickSize,
    lot_size: LotSize,
    market_data: SchwabStreamerQuoteMarketDataEvidence,
}

impl SchwabStreamerQuoteRecordRequest {
    /// Constructs one exact mapping input. Validation occurs against the sealed native record.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, economics, and frame coordinates remain explicit"
    )]
    pub fn new(
        frame_ordinal: u16,
        data_batch_ordinal: u16,
        content_ordinal: u16,
        dictionary: SchwabStreamerFieldDictionary,
        identity: SchwabResolvedProviderIdentity,
        instrument_id: InstrumentId,
        source_identifier: SourceIdentifier,
        provenance: LiveProvenance,
        tick_size: TickSize,
        lot_size: LotSize,
        market_data: SchwabStreamerQuoteMarketDataEvidence,
    ) -> Self {
        Self {
            frame_ordinal,
            data_batch_ordinal,
            content_ordinal,
            dictionary,
            identity,
            instrument_id,
            source_identifier,
            provenance,
            tick_size,
            lot_size,
            market_data,
        }
    }
}

/// Complete caller-owned mapping inputs for one already sealed Streamer microbatch.
#[derive(Debug)]
pub struct SchwabStreamerQuotePublicationRequest<'a> {
    doctor_handoffs: Vec<&'a SchwabStreamerFamilyDoctorHandoff>,
    records: Vec<SchwabStreamerQuoteRecordRequest>,
}

impl<'a> SchwabStreamerQuotePublicationRequest<'a> {
    /// Constructs a bounded record mapping request; duplicate logical coordinates are rejected
    /// when the sealed capture is consumed.
    pub fn new(
        doctor_handoffs: Vec<&'a SchwabStreamerFamilyDoctorHandoff>,
        records: Vec<SchwabStreamerQuoteRecordRequest>,
    ) -> Self {
        Self {
            doctor_handoffs,
            records,
        }
    }
}

/// Why one exact Streamer content record did not become a canonical Level-One quote.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchwabStreamerRecordDispositionReason {
    /// This selected read-only service needs another provider-neutral canonical family.
    UnsupportedCanonicalFamily,
    /// No exact mapping input was supplied for this Level-One content coordinate.
    MissingMappingInput,
    /// Provider data was validly retained but could not produce a complete quote.
    QuoteAbstention(SchwabQuoteAbstention),
    /// The frozen dictionary or exact numeric mapping rejected the provider record.
    CanonicalMappingRejected,
}

/// Explicit outcome for one provider content coordinate omitted from canonical quote rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerRecordDisposition {
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_ordinal: u16,
    service: MarketDataService,
    provider_identifier: crate::ProviderIdentifier,
    reason: SchwabStreamerRecordDispositionReason,
}

impl SchwabStreamerRecordDisposition {
    /// Zero-based logical event-frame ordinal inside the sealed microbatch.
    pub const fn frame_ordinal(&self) -> u16 {
        self.frame_ordinal
    }

    /// Zero-based data-envelope ordinal inside the frame.
    pub const fn data_batch_ordinal(&self) -> u16 {
        self.data_batch_ordinal
    }

    /// Zero-based content-record ordinal inside the data envelope.
    pub const fn content_ordinal(&self) -> u16 {
        self.content_ordinal
    }

    /// Exact selected Schwab service.
    pub const fn service(&self) -> MarketDataService {
        self.service
    }

    /// Exact provider-native record key.
    pub const fn provider_identifier(&self) -> &crate::ProviderIdentifier {
        &self.provider_identifier
    }

    /// Explicit non-publication reason.
    pub const fn reason(&self) -> SchwabStreamerRecordDispositionReason {
        self.reason
    }
}

/// Result of attempting typed quote publication from an already sealed Streamer microbatch.
#[derive(Debug)]
pub enum SchwabStreamerQuotePublicationOutcome {
    /// At least one canonical quote row was bound to exact logical and physical frames.
    Published(Box<SchwabSealedStreamerQuotePublication>),
    /// No canonical quote row existed; the exact sealed raw authority remains recoverable.
    SealedRaw(Box<SchwabSealedRawStreamerPublication>),
}

/// Sealed raw Streamer evidence with explicit reasons why no typed quote batch was minted.
#[derive(Debug)]
pub struct SchwabSealedRawStreamerPublication {
    capture: SchwabSealedStreamerCapture,
    dispositions: Box<[SchwabStreamerRecordDisposition]>,
}

impl SchwabSealedRawStreamerPublication {
    /// Explicit provider-record dispositions. This can be empty for control-only frames.
    pub const fn dispositions(&self) -> &[SchwabStreamerRecordDisposition] {
        &self.dispositions
    }

    /// Returns the still non-cloneable sealed raw capture for another typed family.
    pub fn into_capture(self) -> SchwabSealedStreamerCapture {
        self.capture
    }
}

/// Non-cloneable canonical/native/physical Level-One quote publication authority.
#[derive(Debug)]
pub struct SchwabSealedStreamerQuotePublication {
    binding: SealedProviderEventMicrobatchBinding,
    dispositions: Box<[SchwabStreamerRecordDisposition]>,
}

impl SchwabSealedStreamerQuotePublication {
    /// Sole shared typed event publication authority.
    pub const fn binding(&self) -> &SealedProviderEventMicrobatchBinding {
        &self.binding
    }

    /// Explicit provider records retained raw but omitted from canonical quote rows.
    pub const fn dispositions(&self) -> &[SchwabStreamerRecordDisposition] {
        &self.dispositions
    }

    /// Consumes the adapter handoff into the shared one-shot event publication binding.
    pub fn into_binding(self) -> SealedProviderEventMicrobatchBinding {
        self.binding
    }
}

impl SchwabSealedStreamerCapture {
    /// Maps Level-One content only after the exact raw frames have crossed the physical seal.
    pub fn into_level_one_quote_publication(
        self,
        request: SchwabStreamerQuotePublicationRequest<'_>,
    ) -> Result<SchwabStreamerQuotePublicationOutcome, SchwabStreamerPublicationError> {
        let mut doctor_handoffs = BTreeMap::new();
        for handoff in request.doctor_handoffs {
            if doctor_handoffs.insert(handoff.service(), handoff).is_some() {
                return Err(SchwabStreamerPublicationError::InvalidEvidence);
            }
        }
        let mut inputs = BTreeMap::new();
        for input in request.records {
            let coordinate = (
                input.frame_ordinal,
                input.data_batch_ordinal,
                input.content_ordinal,
            );
            if inputs.insert(coordinate, input).is_some() {
                return Err(SchwabStreamerPublicationError::MappingMismatch);
            }
        }
        let mut qualifications = BTreeMap::new();
        for (&(frame_ordinal, data_batch_ordinal, content_ordinal), input) in &inputs {
            let service = input.market_data.service;
            let handoff = *doctor_handoffs
                .get(&service)
                .ok_or(SchwabStreamerPublicationError::InvalidEvidence)?;
            if !input
                .market_data
                .qualification
                .validates_streamer_publication_coordinate(
                    service,
                    handoff,
                    &self,
                    frame_ordinal,
                    data_batch_ordinal,
                    content_ordinal,
                )
            {
                return Err(SchwabStreamerPublicationError::InvalidEvidence);
            }
            if let Some((existing, _)) =
                qualifications.insert(service, (&input.market_data.qualification, handoff))
            {
                if existing != &input.market_data.qualification {
                    return Err(SchwabStreamerPublicationError::InvalidEvidence);
                }
            }
        }
        if qualifications.len() != doctor_handoffs.len() {
            return Err(SchwabStreamerPublicationError::InvalidEvidence);
        }

        let mut events = Vec::new();
        let mut native_rows = Vec::new();
        let mut row_event_frame_ordinals = Vec::new();
        let mut dispositions = Vec::new();
        let mut used = BTreeSet::new();
        for (frame_index, parsed) in self.parsed_frames().iter().enumerate() {
            let Some(parsed) = parsed else {
                return Err(SchwabStreamerPublicationError::InvalidEvidence);
            };
            let frame_ordinal = u16::try_from(frame_index)
                .map_err(|_| SchwabStreamerPublicationError::InvalidEvidence)?;
            let frame = self
                .frames()
                .get(frame_index)
                .ok_or(SchwabStreamerPublicationError::InvalidEvidence)?;
            if parsed.raw_sha256() != frame.payload_digest().bytes() {
                return Err(SchwabStreamerPublicationError::InvalidEvidence);
            }
            for (data_index, batch) in parsed.value().data.iter().enumerate() {
                let data_batch_ordinal = u16::try_from(data_index)
                    .map_err(|_| SchwabStreamerPublicationError::InvalidEvidence)?;
                if !is_level_one(batch.service) {
                    for (content_index, content) in batch.content.iter().enumerate() {
                        dispositions.push(disposition(
                            frame_ordinal,
                            data_batch_ordinal,
                            content_index,
                            batch.service,
                            content,
                            SchwabStreamerRecordDispositionReason::UnsupportedCanonicalFamily,
                        )?);
                    }
                    continue;
                }

                let dictionary = batch
                    .content
                    .iter()
                    .enumerate()
                    .find_map(|(content_index, _)| {
                        u16::try_from(content_index)
                            .ok()
                            .and_then(|content_ordinal| {
                                inputs
                                    .get(&(frame_ordinal, data_batch_ordinal, content_ordinal))
                                    .map(|input| &input.dictionary)
                            })
                    });
                let mapped_batch = dictionary
                    .map(|dictionary| canonicalize_streamer_batch(batch, dictionary))
                    .transpose();

                for (content_index, content) in batch.content.iter().enumerate() {
                    let content_ordinal = u16::try_from(content_index)
                        .map_err(|_| SchwabStreamerPublicationError::InvalidEvidence)?;
                    let coordinate = (frame_ordinal, data_batch_ordinal, content_ordinal);
                    let Some(input) = inputs.get(&coordinate) else {
                        dispositions.push(disposition(
                            frame_ordinal,
                            data_batch_ordinal,
                            content_index,
                            batch.service,
                            content,
                            SchwabStreamerRecordDispositionReason::MissingMappingInput,
                        )?);
                        continue;
                    };
                    used.insert(coordinate);
                    if input.dictionary.service() != batch.service
                        || dictionary != Some(&input.dictionary)
                    {
                        return Err(SchwabStreamerPublicationError::MappingMismatch);
                    }
                    let canonical_record = match &mapped_batch {
                        Ok(Some(records)) => records.get(content_index),
                        Ok(None) | Err(_) => None,
                    };
                    let Some(canonical_record) = canonical_record else {
                        dispositions.push(disposition(
                            frame_ordinal,
                            data_batch_ordinal,
                            content_index,
                            batch.service,
                            content,
                            SchwabStreamerRecordDispositionReason::CanonicalMappingRejected,
                        )?);
                        continue;
                    };
                    validate_mapping(
                        self.coordinates(),
                        self.stream_identity(),
                        frame,
                        canonical_record,
                        input,
                    )?;
                    match canonicalize_streamer_quote_record(
                        canonical_record,
                        input.identity.clone(),
                        input.provenance.clone(),
                        input.tick_size,
                        input.lot_size,
                    ) {
                        Ok(SchwabQuoteCanonicalOutcome::Mapped {
                            provider_instrument_id,
                            resolution_evidence,
                            event,
                        }) => {
                            if &provider_instrument_id != input.identity.provider_instrument_id()
                                || resolution_evidence != input.identity.resolution_evidence()
                            {
                                return Err(SchwabStreamerPublicationError::InvalidEvidence);
                            }
                            native_rows.push(encode_native_row(
                                frame,
                                frame_ordinal,
                                data_batch_ordinal,
                                content_ordinal,
                                canonical_record,
                                content,
                                input,
                                qualifications
                                    .get(&input.market_data.service)
                                    .map(|(_, handoff)| handoff.capture_set_sha256())
                                    .ok_or(SchwabStreamerPublicationError::InvalidEvidence)?,
                            )?);
                            events.push(*event);
                            row_event_frame_ordinals.push(frame_ordinal);
                        }
                        Ok(SchwabQuoteCanonicalOutcome::Abstained { reason, .. }) => {
                            dispositions.push(disposition(
                                frame_ordinal,
                                data_batch_ordinal,
                                content_index,
                                batch.service,
                                content,
                                SchwabStreamerRecordDispositionReason::QuoteAbstention(reason),
                            )?);
                        }
                        Err(_) => dispositions.push(disposition(
                            frame_ordinal,
                            data_batch_ordinal,
                            content_index,
                            batch.service,
                            content,
                            SchwabStreamerRecordDispositionReason::CanonicalMappingRejected,
                        )?),
                    }
                }
            }
        }
        if used.len() != inputs.len() {
            return Err(SchwabStreamerPublicationError::MappingMismatch);
        }
        if events.is_empty() {
            return Ok(SchwabStreamerQuotePublicationOutcome::SealedRaw(Box::new(
                SchwabSealedRawStreamerPublication {
                    capture: self,
                    dispositions: dispositions.into_boxed_slice(),
                },
            )));
        }

        let sidecar = encode_sidecar(&self, &qualifications, &dispositions)?;
        let crate::transport::SchwabSealedStreamerCaptureParts {
            token, coordinates, ..
        } = self.into_parts();
        let batch = ProviderMarketEventBatch::try_new(
            coordinates.source_id().clone(),
            coordinates.metadata_revision().clone(),
            coordinates.dataset().clone(),
            events,
        )?;
        let native_lineage = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1,
            &batch,
            native_rows,
            Some(sidecar),
        )?;
        let binding = SealedProviderEventMicrobatchBinding::try_new(
            token,
            batch,
            native_lineage,
            row_event_frame_ordinals,
        )?;
        binding.validate()?;
        Ok(SchwabStreamerQuotePublicationOutcome::Published(Box::new(
            SchwabSealedStreamerQuotePublication {
                binding,
                dispositions: dispositions.into_boxed_slice(),
            },
        )))
    }
}

fn validate_mapping(
    coordinates: &crate::SchwabCaptureCoordinates,
    stream_identity: &SourceIdentifier,
    frame: &SchwabStreamerFrameSealEvidence,
    record: &crate::SchwabCanonicalStreamerRecord,
    input: &SchwabStreamerQuoteRecordRequest,
) -> Result<(), SchwabStreamerPublicationError> {
    let binding = input.provenance.binding();
    let received_at = timestamp_from_millis(frame.received_at_unix_millis())?;
    let connection_generation =
        market_squawk_domain::ConnectionGeneration::new(frame.generation().get())
            .map_err(|_| SchwabStreamerPublicationError::InvalidEvidence)?;
    if record.service != input.market_data.service
        || record.provider_identifier != *input.identity.provider_symbol()
        || input.market_data.depth() != MarketDepth::TopOfBook
        || input.identity.resolution_evidence().algorithm() != DigestAlgorithm::Sha256
        || input.identity.resolution_evidence().bytes() == [0; 32]
        || binding.source_id() != coordinates.source_id()
        || binding.metadata_revision() != coordinates.metadata_revision()
        || binding.session_id() != stream_identity
        || binding.venue_id() != input.market_data.venue_id()
        || binding.instrument_id() != input.instrument_id
        || binding.connection_generation() != connection_generation
        || binding.provider_product() != input.market_data.provider_product()
        || binding.provider_channel() != input.market_data.provider_channel()
        || binding.event_class() != LiveEventClass::Quote
        || binding.source_identifier() != &input.source_identifier
        || binding.payload_digest() != frame.payload_digest()
        || input.provenance.source_timestamp() != record.provider_envelope_timestamp
        || input.provenance.received_at() != received_at
        || input.provenance.recorded_quality() != input.market_data.quality()
    {
        return Err(SchwabStreamerPublicationError::InvalidEvidence);
    }
    Ok(())
}

fn disposition(
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_index: usize,
    service: MarketDataService,
    content: &StreamerContent,
    reason: SchwabStreamerRecordDispositionReason,
) -> Result<SchwabStreamerRecordDisposition, SchwabStreamerPublicationError> {
    Ok(SchwabStreamerRecordDisposition {
        frame_ordinal,
        data_batch_ordinal,
        content_ordinal: u16::try_from(content_index)
            .map_err(|_| SchwabStreamerPublicationError::InvalidEvidence)?,
        service,
        provider_identifier: content.key.clone(),
        reason,
    })
}

fn is_level_one(service: MarketDataService) -> bool {
    matches!(
        service,
        MarketDataService::LevelOneEquities
            | MarketDataService::LevelOneOptions
            | MarketDataService::LevelOneFutures
            | MarketDataService::LevelOneFuturesOptions
            | MarketDataService::LevelOneForex
    )
}

fn timestamp_from_millis(value: u64) -> Result<Timestamp, SchwabStreamerPublicationError> {
    let nanos = i64::try_from(value)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(SchwabStreamerPublicationError::InvalidEvidence)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerQuoteNativeRowV1<'a> {
    version: u16,
    family: &'static str,
    frame_ordinal: u16,
    transport_ordinal: u64,
    data_batch_ordinal: u16,
    content_ordinal: u16,
    service: &'static str,
    command: &'a str,
    provider_envelope_timestamp: Option<Timestamp>,
    provider_identifier: &'a str,
    fields: Vec<SchwabStreamerFieldV1<'a>>,
    dictionary_version: &'a str,
    dictionary_evidence: EvidenceDigest,
    instrument_id: InstrumentId,
    provider_instrument_id: &'a str,
    provider_source_identifier: &'a str,
    resolution_evidence: EvidenceDigest,
    feed: &'a str,
    reference_venue: &'a str,
    provider_reported_venue: Option<&'a str>,
    depth: MarketDepth,
    delay: SchwabMarketDataDelay,
    quality: DataQuality,
    provider_product: &'a str,
    provider_channel: &'a str,
    qualification_evidence: EvidenceDigest,
    qualification_receipt_evidence: EvidenceDigest,
    qualification_family: market_squawk_sources::SchwabMarketDataFamily,
    qualification_observed_at: Timestamp,
    qualification_response_observed_at: Timestamp,
    streamer_doctor_capture_set_evidence: EvidenceDigest,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerFieldV1<'a> {
    field_id: u16,
    value: SchwabStreamerNativeValueV1<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
enum SchwabStreamerNativeValueV1<'a> {
    Null,
    Bool(bool),
    Number(&'a str),
    Text(&'a str),
    Sequence(Vec<SchwabStreamerNativeValueV1<'a>>),
    Fields(Vec<SchwabStreamerNestedFieldV1<'a>>),
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerNestedFieldV1<'a> {
    field_id: u16,
    value: SchwabStreamerNativeValueV1<'a>,
}

fn encode_native_row(
    frame: &SchwabStreamerFrameSealEvidence,
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_ordinal: u16,
    record: &crate::SchwabCanonicalStreamerRecord,
    content: &StreamerContent,
    input: &SchwabStreamerQuoteRecordRequest,
    streamer_doctor_capture_set_evidence: EvidenceDigest,
) -> Result<Bytes, SchwabStreamerPublicationError> {
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(content.fields.len())
        .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)?;
    for field in &content.fields {
        fields.push(SchwabStreamerFieldV1 {
            field_id: field.field_id,
            value: native_value(&field.value)?,
        });
    }
    serde_json::to_vec(&SchwabStreamerQuoteNativeRowV1 {
        version: 1,
        family: "schwab.streamer.level-one-quote",
        frame_ordinal,
        transport_ordinal: frame.transport_ordinal().get(),
        data_batch_ordinal,
        content_ordinal,
        service: record.service.as_str(),
        command: &record.command,
        provider_envelope_timestamp: record.provider_envelope_timestamp,
        provider_identifier: record.provider_identifier.as_str(),
        fields,
        dictionary_version: record.dictionary_version.as_str(),
        dictionary_evidence: record.dictionary_evidence,
        instrument_id: input.instrument_id,
        provider_instrument_id: input.identity.provider_instrument_id().as_str(),
        provider_source_identifier: input.source_identifier.as_str(),
        resolution_evidence: input.identity.resolution_evidence(),
        feed: input.market_data.feed().as_str(),
        reference_venue: input.market_data.venue_id().as_str(),
        provider_reported_venue: None,
        depth: input.market_data.depth(),
        delay: input.market_data.delay(),
        quality: input.market_data.quality(),
        provider_product: input
            .market_data
            .provider_product()
            .as_source_identifier()
            .as_str(),
        provider_channel: input
            .market_data
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        qualification_evidence: input.market_data.qualification_evidence(),
        qualification_receipt_evidence: input.market_data.qualification.receipt_evidence(),
        qualification_family: input.market_data.qualification.family(),
        qualification_observed_at: input.market_data.qualification.family_observed_at(),
        qualification_response_observed_at: input.market_data.qualification.response_observed_at(),
        streamer_doctor_capture_set_evidence,
    })
    .map(Bytes::from)
    .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)
}

fn native_value(
    value: &StreamerNativeValue,
) -> Result<SchwabStreamerNativeValueV1<'_>, SchwabStreamerPublicationError> {
    match value {
        StreamerNativeValue::Scalar(NativeScalar::Null) => Ok(SchwabStreamerNativeValueV1::Null),
        StreamerNativeValue::Scalar(NativeScalar::Bool(value)) => {
            Ok(SchwabStreamerNativeValueV1::Bool(*value))
        }
        StreamerNativeValue::Scalar(NativeScalar::Number(value)) => {
            Ok(SchwabStreamerNativeValueV1::Number(value.as_str()))
        }
        StreamerNativeValue::Scalar(NativeScalar::Text(value)) => {
            Ok(SchwabStreamerNativeValueV1::Text(value))
        }
        StreamerNativeValue::Sequence(values) => {
            let mut encoded = Vec::new();
            encoded
                .try_reserve_exact(values.len())
                .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)?;
            for value in values {
                encoded.push(native_value(value)?);
            }
            Ok(SchwabStreamerNativeValueV1::Sequence(encoded))
        }
        StreamerNativeValue::Fields(fields) => {
            let mut encoded = Vec::new();
            encoded
                .try_reserve_exact(fields.len())
                .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)?;
            for StreamerNestedField { field_id, value } in fields {
                encoded.push(SchwabStreamerNestedFieldV1 {
                    field_id: *field_id,
                    value: native_value(value)?,
                });
            }
            Ok(SchwabStreamerNativeValueV1::Fields(encoded))
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerNativeSidecarV1<'a> {
    version: u16,
    family: &'static str,
    stream_identity: &'a str,
    connection_generation: u64,
    token_generation: u64,
    first_transport_ordinal: u64,
    last_transport_ordinal: u64,
    frame_count: u64,
    payload_bytes: u64,
    first_received_at_unix_millis: u64,
    last_received_at_unix_millis: u64,
    content_sha256: [u8; 32],
    observation_sha256: [u8; 32],
    qualifications: Vec<SchwabStreamerQualificationSidecarV1>,
    frames: Vec<SchwabStreamerFrameNativeSidecarV1<'a>>,
    dispositions: Vec<SchwabStreamerDispositionSidecarV1<'a>>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerQualificationSidecarV1 {
    service: &'static str,
    receipt_evidence: EvidenceDigest,
    family: market_squawk_sources::SchwabMarketDataFamily,
    observation_evidence: EvidenceDigest,
    observed_at: Timestamp,
    response_observed_at: Timestamp,
    streamer_doctor_capture_set_evidence: EvidenceDigest,
    streamer_doctor_request_evidence: EvidenceDigest,
    streamer_doctor_provider_records: u64,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerFrameNativeSidecarV1<'a> {
    frame_ordinal: u16,
    event_id: uuid::Uuid,
    transport_ordinal: u64,
    received_at_unix_millis: u64,
    payload_bytes: u64,
    payload_digest: EvidenceDigest,
    provider_schema: &'static str,
    provider_schema_version: u16,
    unknown_field_count: usize,
    unknown_field_bytes: usize,
    unknown_field_paths: &'a [Box<str>],
    unknown_field_digest: [u8; 32],
    response_count: usize,
    data_batch_count: usize,
    notification_count: usize,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabStreamerDispositionSidecarV1<'a> {
    frame_ordinal: u16,
    data_batch_ordinal: u16,
    content_ordinal: u16,
    service: &'static str,
    provider_identifier: &'a str,
    reason: &'static str,
}

fn encode_sidecar(
    capture: &SchwabSealedStreamerCapture,
    qualifications: &BTreeMap<
        MarketDataService,
        (
            &SchwabMarketDataQualification,
            &SchwabStreamerFamilyDoctorHandoff,
        ),
    >,
    dispositions: &[SchwabStreamerRecordDisposition],
) -> Result<Bytes, SchwabStreamerPublicationError> {
    let receipt = capture.streamer_receipt();
    let mut frames = Vec::new();
    frames
        .try_reserve_exact(capture.frames().len())
        .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)?;
    for (frame_index, (frame, parsed)) in capture
        .frames()
        .iter()
        .zip(capture.parsed_frames())
        .enumerate()
    {
        let Some(parsed) = parsed else {
            return Err(SchwabStreamerPublicationError::InvalidEvidence);
        };
        let unknown = parsed.unknown_fields();
        frames.push(SchwabStreamerFrameNativeSidecarV1 {
            frame_ordinal: u16::try_from(frame_index)
                .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)?,
            event_id: frame.event_id(),
            transport_ordinal: frame.transport_ordinal().get(),
            received_at_unix_millis: frame.received_at_unix_millis(),
            payload_bytes: frame.payload_bytes(),
            payload_digest: frame.payload_digest(),
            provider_schema: parsed.schema_name(),
            provider_schema_version: parsed.schema_version(),
            unknown_field_count: unknown.field_count(),
            unknown_field_bytes: unknown.encoded_bytes(),
            unknown_field_paths: unknown.paths(),
            unknown_field_digest: unknown.digest(),
            response_count: parsed.value().responses.len(),
            data_batch_count: parsed.value().data.len(),
            notification_count: parsed.value().notifications.len(),
        });
    }
    let disposition_rows = dispositions
        .iter()
        .map(|disposition| SchwabStreamerDispositionSidecarV1 {
            frame_ordinal: disposition.frame_ordinal,
            data_batch_ordinal: disposition.data_batch_ordinal,
            content_ordinal: disposition.content_ordinal,
            service: disposition.service.as_str(),
            provider_identifier: disposition.provider_identifier.as_str(),
            reason: disposition_reason(disposition.reason),
        })
        .collect();
    let qualification_rows = qualifications
        .iter()
        .map(
            |(service, (qualification, handoff))| SchwabStreamerQualificationSidecarV1 {
                service: service.as_str(),
                receipt_evidence: qualification.receipt_evidence(),
                family: qualification.family(),
                observation_evidence: qualification.observation_evidence(),
                observed_at: qualification.family_observed_at(),
                response_observed_at: qualification.response_observed_at(),
                streamer_doctor_capture_set_evidence: handoff.capture_set_sha256(),
                streamer_doctor_request_evidence: handoff.request_payload_sha256(),
                streamer_doctor_provider_records: handoff.provider_records(),
            },
        )
        .collect();
    serde_json::to_vec(&SchwabStreamerNativeSidecarV1 {
        version: 1,
        family: "schwab.streamer.market-data",
        stream_identity: capture.stream_identity().as_str(),
        connection_generation: receipt.generation().get(),
        token_generation: receipt.token_generation().get(),
        first_transport_ordinal: receipt.first_ordinal().get(),
        last_transport_ordinal: receipt.last_ordinal().get(),
        frame_count: receipt.frame_count(),
        payload_bytes: receipt.payload_bytes(),
        first_received_at_unix_millis: receipt.first_received_at_unix_millis(),
        last_received_at_unix_millis: receipt.last_received_at_unix_millis(),
        content_sha256: receipt.content_sha256(),
        observation_sha256: receipt.observation_sha256(),
        qualifications: qualification_rows,
        frames,
        dispositions: disposition_rows,
    })
    .map(Bytes::from)
    .map_err(|_| SchwabStreamerPublicationError::NativeEncoding)
}

const fn disposition_reason(reason: SchwabStreamerRecordDispositionReason) -> &'static str {
    match reason {
        SchwabStreamerRecordDispositionReason::UnsupportedCanonicalFamily => {
            "unsupported_canonical_family"
        }
        SchwabStreamerRecordDispositionReason::MissingMappingInput => "missing_mapping_input",
        SchwabStreamerRecordDispositionReason::QuoteAbstention(
            SchwabQuoteAbstention::NoQuotedSide,
        ) => "no_quoted_side",
        SchwabStreamerRecordDispositionReason::QuoteAbstention(
            SchwabQuoteAbstention::IncompleteBid,
        ) => "incomplete_bid",
        SchwabStreamerRecordDispositionReason::QuoteAbstention(
            SchwabQuoteAbstention::IncompleteAsk,
        ) => "incomplete_ask",
        SchwabStreamerRecordDispositionReason::QuoteAbstention(SchwabQuoteAbstention::ZeroSize) => {
            "zero_size"
        }
        SchwabStreamerRecordDispositionReason::CanonicalMappingRejected => {
            "canonical_mapping_rejected"
        }
    }
}

/// Secret-free Schwab Streamer seal/publication failure.
#[derive(Debug, Error)]
pub enum SchwabStreamerPublicationError {
    /// Source, identity, frame, clock, feed, venue, depth, or qualification evidence differs.
    #[error("Schwab Streamer publication evidence is invalid")]
    InvalidEvidence,
    /// Caller mapping coordinates or frozen dictionaries do not align one-for-one.
    #[error("Schwab Streamer mapping inputs do not match the sealed provider records")]
    MappingMismatch,
    /// Provider-native semantic evidence could not be encoded within the closed boundary.
    #[error("Schwab Streamer provider-native evidence could not be encoded")]
    NativeEncoding,
    /// Common typed publication binding failed closed.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
}

impl From<SchwabCanonicalError> for SchwabStreamerPublicationError {
    fn from(_error: SchwabCanonicalError) -> Self {
        Self::InvalidEvidence
    }
}
