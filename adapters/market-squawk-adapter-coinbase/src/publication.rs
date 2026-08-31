//! Consuming Coinbase raw-seal and qualified canonical-event publication handoffs.
//!
//! Physical storage remains application owned. This module moves exact provider material across a
//! narrow boundary, then accepts only common one-use seal tokens whose persisted receipts rejoin
//! that material. Canonical events must already have been qualified by the live plane; this module
//! validates their source, venue, product, channel, generation, clocks, payload, and raw coordinate
//! before minting a common durable publication binding.

use std::io::Read as _;
use std::{
    collections::BTreeSet,
    mem::{size_of, size_of_val},
};

use bytes::Bytes;
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, EvidenceDigest, LiveEventClass, LiveProvenance, MarketDepth,
    MarketEvent, MetadataRevision, ProviderChannel, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_live::CommittedResearchMarketObservation;
use market_squawk_sources::{
    DecodedProviderBatch, HttpCaptureMethod, MAX_PROVIDER_CAPTURE_PAGE_BYTES,
    ProviderCaptureSealRequest, ProviderCaptureTerminalDisposition,
    ProviderEventMicrobatchMaterial, ProviderEventMicrobatchSealExpectation,
    ProviderEventMicrobatchToken, ProviderMarketEventBatch, ProviderMarketEventNativeLineageBatch,
    ProviderMarketEventNativeLineageRow, ProviderNativeLineageImplementation,
    ProviderObservationPayload, ProviderOrderChangeReason, ProviderOrderEventKind,
    ProviderPublicationBindingKind, ProviderSequenceEvidence, ProviderTimestampEvidence,
    ProviderWholeCaptureToken, SealedProviderCaptureMaterial,
    SealedProviderCompositeResponseEventBinding, SealedProviderEventMicrobatchBinding,
    SealedProviderEventMicrobatchReceipt, SealedProviderPublicationBinding,
    SealedProviderResponseMarketEventBinding, TransportFrameKind, ValidatedRawMarketFrame,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    CoinbaseDirectTradeEvidence, CoinbaseMarketChannel, CoinbaseMarketContinuity,
    CoinbaseMarketFeed, CoinbaseMarketHandoff, CoinbaseMarketHandoffEvidence,
    CoinbaseMarketRawLineage,
};

const PUBLIC_CHANNEL: &str = "level2+market_trades+heartbeats";
const DIRECT_CHANNEL: &str = "full";

/// Application-supplied capture-owned physical identities for one Coinbase raw handoff.
///
/// The first event identity belongs to the public frame or Direct REST snapshot. Remaining Direct
/// identities map one-for-one to replay frames. The adapter validates identity shape but never
/// creates or substitutes a UUID or connection authority.
#[derive(Debug)]
pub struct CoinbaseMarketPhysicalCaptureIdentity {
    connection_id: [u8; 16],
    event_ids: Vec<[u8; 16]>,
}

impl CoinbaseMarketPhysicalCaptureIdentity {
    /// Admits explicit capture-owned physical identities at the adapter boundary.
    pub fn try_new(
        connection_id: [u8; 16],
        event_ids: Vec<[u8; 16]>,
    ) -> Result<Self, CoinbaseMarketPublicationError> {
        if connection_id == [0; 16]
            || event_ids.is_empty()
            || event_ids.iter().any(|identity| *identity == [0; 16])
            || event_ids.iter().collect::<BTreeSet<_>>().len() != event_ids.len()
        {
            return Err(CoinbaseMarketPublicationError::InvalidPhysicalIdentity);
        }
        Ok(Self {
            connection_id,
            event_ids,
        })
    }

    /// Returns the capture-owned connection identity bytes.
    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    /// Returns capture-owned event identities in exact raw-object order.
    pub fn event_ids(&self) -> &[[u8; 16]] {
        &self.event_ids
    }
}

/// Application-selected durable dataset and exact stream boundary.
#[derive(Debug)]
pub struct CoinbaseMarketPublicationContext {
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    physical: CoinbaseMarketPhysicalCaptureIdentity,
}

impl CoinbaseMarketPublicationContext {
    /// Creates one explicit common publication context without inventing provider or venue data.
    pub const fn new(
        dataset: SourceIdentifier,
        stream_identity: SourceIdentifier,
        physical: CoinbaseMarketPhysicalCaptureIdentity,
    ) -> Self {
        Self {
            dataset,
            stream_identity,
            physical,
        }
    }

    /// Returns the application-selected durable dataset.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact application-selected stream boundary identity.
    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    /// Returns the application-owned physical identities.
    pub const fn physical(&self) -> &CoinbaseMarketPhysicalCaptureIdentity {
        &self.physical
    }
}

/// One exact source frame ready for application-owned `RawCaptureRecord` construction.
#[derive(Debug)]
pub struct CoinbaseMarketRawSealFrame {
    event_id: [u8; 16],
    connection_id: [u8; 16],
    source_sequence: Option<u64>,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    payload: Bytes,
}

impl CoinbaseMarketRawSealFrame {
    pub const fn event_id(&self) -> [u8; 16] {
        self.event_id
    }

    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    pub const fn exchange_at(&self) -> Option<Timestamp> {
        self.exchange_at
    }

    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_payload(self) -> Bytes {
        self.payload
    }
}

/// Exact live-event microbatch material consumed by the application physical-seal boundary.
#[derive(Debug)]
pub struct CoinbaseEventMicrobatchSealMaterial {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    frames: Box<[CoinbaseMarketRawSealFrame]>,
}

impl CoinbaseEventMicrobatchSealMaterial {
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    pub const fn frames(&self) -> &[CoinbaseMarketRawSealFrame] {
        &self.frames
    }

    pub fn into_frames(self) -> Box<[CoinbaseMarketRawSealFrame]> {
        self.frames
    }
}

/// Original Coinbase Direct response-segment evidence retained beside the common response page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectSnapshotSegmentEvidence {
    ordinal: u32,
    body_length: u64,
    body_digest: EvidenceDigest,
}

impl CoinbaseDirectSnapshotSegmentEvidence {
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    pub const fn body_length(self) -> u64 {
        self.body_length
    }

    pub const fn body_digest(self) -> EvidenceDigest {
        self.body_digest
    }
}

/// One complete Direct REST snapshot ready for application-owned response-set sealing.
#[derive(Debug)]
pub struct CoinbaseDirectSnapshotSealMaterial {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    request_identity: EvidenceDigest,
    method: HttpCaptureMethod,
    final_url: Box<str>,
    status: u16,
    declared_body_length: Option<u64>,
    received_at: Timestamp,
    body_digest: EvidenceDigest,
    segments: Box<[CoinbaseDirectSnapshotSegmentEvidence]>,
    frame: CoinbaseMarketRawSealFrame,
}

impl CoinbaseDirectSnapshotSealMaterial {
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    pub const fn method(&self) -> HttpCaptureMethod {
        self.method
    }

    pub fn final_url(&self) -> &str {
        &self.final_url
    }

    pub const fn status(&self) -> u16 {
        self.status
    }

    pub const fn declared_body_length(&self) -> Option<u64> {
        self.declared_body_length
    }

    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    pub const fn segments(&self) -> &[CoinbaseDirectSnapshotSegmentEvidence] {
        &self.segments
    }

    pub const fn frame(&self) -> &CoinbaseMarketRawSealFrame {
        &self.frame
    }

    pub fn into_frame(self) -> CoinbaseMarketRawSealFrame {
        self.frame
    }
}

/// Closed raw material shape for the two distinct Coinbase market-data surfaces.
#[derive(Debug)]
pub enum CoinbaseMarketSealMaterial {
    AdvancedTrade(CoinbaseEventMicrobatchSealMaterial),
    ExchangeDirect {
        snapshot: CoinbaseDirectSnapshotSealMaterial,
        replay: CoinbaseEventMicrobatchSealMaterial,
    },
}

/// Common one-use tokens returned after the application physically seals the exact material.
#[derive(Debug)]
pub enum CoinbaseMarketSealedTokens {
    AdvancedTrade(ProviderEventMicrobatchToken),
    ExchangeDirect {
        snapshot: ProviderWholeCaptureToken,
        replay: ProviderEventMicrobatchToken,
    },
}

/// One already-qualified public canonical row mapped to an exact decoded observation ordinal.
#[derive(Debug)]
pub struct CoinbaseQualifiedPublicRow {
    observation_ordinal: u16,
    event: MarketEvent,
}

impl CoinbaseQualifiedPublicRow {
    pub const fn new(observation_ordinal: u16, event: MarketEvent) -> Self {
        Self {
            observation_ordinal,
            event,
        }
    }
}

/// One already-qualified Direct canonical row mapped to an exact replay-frame ordinal.
#[derive(Debug)]
pub struct CoinbaseQualifiedDirectReplayRow {
    replay_frame_ordinal: u16,
    event: MarketEvent,
}

impl CoinbaseQualifiedDirectReplayRow {
    pub const fn new(replay_frame_ordinal: u16, event: MarketEvent) -> Self {
        Self {
            replay_frame_ordinal,
            event,
        }
    }
}

/// Explicit reason an otherwise valid provider coordinate produced no canonical row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoinbaseMarketOmissionReason {
    CanonicalAbstention,
    UnsupportedCanonicalFamily,
    IncompleteEconomicFields,
}

/// One omitted decoded-observation or replay-frame ordinal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseMarketOmission {
    ordinal: u16,
    reason: CoinbaseMarketOmissionReason,
}

impl CoinbaseMarketOmission {
    pub const fn new(ordinal: u16, reason: CoinbaseMarketOmissionReason) -> Self {
        Self { ordinal, reason }
    }

    pub const fn ordinal(self) -> u16 {
        self.ordinal
    }

    pub const fn reason(self) -> CoinbaseMarketOmissionReason {
        self.reason
    }
}

/// Already-qualified canonical events for one exact Coinbase surface.
#[derive(Debug)]
pub enum CoinbaseQualifiedMarketPublication {
    AdvancedTrade {
        rows: Vec<CoinbaseQualifiedPublicRow>,
        omissions: Vec<CoinbaseMarketOmission>,
    },
    ExchangeDirect {
        initial_snapshot: MarketEvent,
        replay_rows: Vec<CoinbaseQualifiedDirectReplayRow>,
        replay_omissions: Vec<CoinbaseMarketOmission>,
    },
}

/// Explicit whole-handoff reason for retaining sealed raw evidence without typed publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoinbaseMarketNonPublicationReason {
    CanonicalQualificationAbstained,
    CanonicalQualificationUnavailable,
    InstrumentDefinitionUnavailable,
    LiveGenerationUnavailable,
    ApplicationBackpressure,
}

/// Qualification decision supplied only by the owning live/application layer.
#[derive(Debug)]
pub enum CoinbaseMarketQualificationOutcome {
    Qualified(CoinbaseQualifiedMarketPublication),
    Abstained(CoinbaseMarketNonPublicationReason),
    Unavailable(CoinbaseMarketNonPublicationReason),
}

/// Final result after exact physical rejoin and optional canonical qualification.
#[derive(Debug)]
pub enum CoinbaseSealedMarketPublication {
    Published(SealedProviderPublicationBinding),
    SealedRaw(CoinbaseSealedRawMarketPublication),
}

/// Exact common sealed tokens retained when canonical publication abstains or is unavailable.
#[derive(Debug)]
pub struct CoinbaseSealedRawMarketPublication {
    tokens: CoinbaseMarketSealedTokens,
    reason: CoinbaseMarketNonPublicationReason,
}

impl CoinbaseSealedRawMarketPublication {
    pub const fn reason(&self) -> CoinbaseMarketNonPublicationReason {
        self.reason
    }

    pub fn into_tokens(self) -> CoinbaseMarketSealedTokens {
        self.tokens
    }
}

/// Opaque provider continuation awaiting exact application-owned physical tokens.
#[derive(Debug)]
pub struct CoinbaseMarketSealRejoin {
    evidence: CoinbaseMarketHandoffEvidence,
    typed_batch: DecodedProviderBatch,
    expected_channel: ProviderChannel,
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    physical_connection_id: [u8; 16],
    physical_event_ids: Box<[[u8; 16]]>,
    raw: CoinbasePublicationRawEvidence,
    public_state: CoinbasePublicPublicationState,
}

#[derive(Debug)]
enum CoinbasePublicationRawEvidence {
    AdvancedTrade {
        source_sequence: Option<u64>,
        exchange_at: Option<Timestamp>,
    },
    ExchangeDirect {
        snapshot: CoinbaseDirectSnapshotPublicationEvidence,
        replay: Box<[CoinbaseDirectReplayPublicationEvidence]>,
    },
}

#[derive(Debug)]
enum CoinbasePublicPublicationState {
    NotApplicable,
    AwaitingSeal {
        expectation: ProviderEventMicrobatchSealExpectation,
        frame_id: market_squawk_sources::FrameId,
        available_at: Timestamp,
        decoded_retained_bytes: usize,
    },
    Sealed {
        token: ProviderEventMicrobatchToken,
        frame_id: market_squawk_sources::FrameId,
        available_at: Timestamp,
        decoded_retained_bytes: usize,
    },
}

impl CoinbaseMarketSealRejoin {
    /// Rejoins the one-use common physical seal for a public Advanced Trade frame.
    pub fn try_rejoin_public_seal(
        mut self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<Self, CoinbaseMarketPublicationError> {
        let state = std::mem::replace(
            &mut self.public_state,
            CoinbasePublicPublicationState::NotApplicable,
        );
        let CoinbasePublicPublicationState::AwaitingSeal {
            expectation,
            frame_id,
            available_at,
            decoded_retained_bytes,
        } = state
        else {
            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
        };
        let token = expectation.try_rejoin(sealed)?;
        let CoinbasePublicationRawEvidence::AdvancedTrade {
            source_sequence,
            exchange_at,
        } = &self.raw
        else {
            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
        };
        validate_event_token(
            &token,
            self.typed_batch.evidence().binding().source_id(),
            self.typed_batch.evidence().binding().metadata_revision(),
            &self.dataset,
            &self.stream_identity,
            self.physical_connection_id,
            &self.physical_event_ids,
            &[ExpectedFrame {
                sequence: *source_sequence,
                exchange_at: *exchange_at,
                received_at: self.typed_batch.evidence().received_at(),
                payload_digest: self.typed_batch.evidence().payload_digest(),
            }],
        )?;
        self.public_state = CoinbasePublicPublicationState::Sealed {
            token,
            frame_id,
            available_at,
            decoded_retained_bytes,
        };
        Ok(self)
    }

    /// Returns the exact registered source identity retained by the decoder and raw receipt.
    pub fn source_id(&self) -> &SourceId {
        self.typed_batch.evidence().binding().source_id()
    }

    /// Returns the exact metadata interpretation revision retained by decoder evidence.
    pub fn metadata_revision(&self) -> &MetadataRevision {
        self.typed_batch.evidence().binding().metadata_revision()
    }

    /// Returns the exact source connection generation that produced the physical frame.
    pub fn connection_generation(&self) -> ConnectionGeneration {
        self.typed_batch
            .evidence()
            .binding()
            .connection_generation()
    }

    /// Returns the nonzero generation-local physical frame identity.
    pub fn frame_id(
        &self,
    ) -> Result<market_squawk_sources::FrameId, CoinbaseMarketPublicationError> {
        match &self.public_state {
            CoinbasePublicPublicationState::Sealed { frame_id, .. } => Ok(*frame_id),
            CoinbasePublicPublicationState::NotApplicable
            | CoinbasePublicPublicationState::AwaitingSeal { .. } => {
                Err(CoinbaseMarketPublicationError::ProfileMismatch)
            }
        }
    }

    /// Returns SHA-256 of the exact provider frame retained by raw capture.
    pub const fn raw_payload_digest(&self) -> EvidenceDigest {
        self.typed_batch.evidence().payload_digest()
    }

    /// Returns the number of provider observations that must rejoin this exact frame.
    pub fn expected_row_count(&self) -> usize {
        self.typed_batch.observations().len()
    }

    /// Returns persisted logical and immutable physical raw receipt evidence.
    pub fn persisted_receipt(
        &self,
    ) -> Result<&SealedProviderEventMicrobatchReceipt, CoinbaseMarketPublicationError> {
        match &self.public_state {
            CoinbasePublicPublicationState::Sealed { token, .. } => Ok(token.persisted_receipt()),
            CoinbasePublicPublicationState::NotApplicable
            | CoinbasePublicPublicationState::AwaitingSeal { .. } => {
                Err(CoinbaseMarketPublicationError::ProfileMismatch)
            }
        }
    }

    /// Returns a checked conservative charge for the sealed continuation retained in rendezvous.
    pub fn conservative_retained_bytes(&self) -> Option<usize> {
        let (decoded_retained_bytes, receipt) = match &self.public_state {
            CoinbasePublicPublicationState::Sealed {
                token,
                decoded_retained_bytes,
                ..
            } => (*decoded_retained_bytes, token.persisted_receipt()),
            CoinbasePublicPublicationState::NotApplicable
            | CoinbasePublicPublicationState::AwaitingSeal { .. } => return None,
        };
        let event_ids = size_of::<[u8; 16]>().checked_mul(self.physical_event_ids.len())?;
        let capture = receipt.capture();
        let receipt_bytes = capture
            .source_id()
            .retained_bytes()
            .checked_add(
                capture
                    .metadata_revision()
                    .as_source_identifier()
                    .retained_bytes(),
            )?
            .checked_add(capture.dataset().retained_bytes())?
            .checked_add(capture.stream_identity().retained_bytes())?
            .checked_add(size_of_val(capture.frames()))?
            .checked_add(receipt.segment().relative_reference().len())?
            .checked_add(size_of_val(receipt.segment().frames()))?;
        size_of::<Self>()
            .checked_add(decoded_retained_bytes)?
            .checked_add(event_ids)?
            .checked_add(receipt_bytes)?
            .checked_add(self.dataset.retained_bytes())?
            .checked_add(self.stream_identity.retained_bytes())?
            .checked_add(
                self.expected_channel
                    .as_source_identifier()
                    .retained_bytes(),
            )?
            .checked_add(
                self.evidence
                    .product()
                    .as_source_identifier()
                    .retained_bytes(),
            )?
            .checked_add(self.evidence.venue().retained_bytes())
    }

    /// Consumes the sealed frame and exact post-commit live rows into the common immutable
    /// provider-event binding. No caller-constructed `MarketEvent` is accepted at this boundary.
    pub fn try_publish_committed(
        mut self,
        committed: Vec<CommittedResearchMarketObservation>,
    ) -> Result<SealedProviderPublicationBinding, CoinbaseMarketPublicationError> {
        let state = std::mem::replace(
            &mut self.public_state,
            CoinbasePublicPublicationState::NotApplicable,
        );
        let CoinbasePublicPublicationState::Sealed {
            token,
            frame_id,
            available_at,
            ..
        } = state
        else {
            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
        };
        let expected = self.typed_batch.observations().len();
        if expected == 0 || committed.len() != expected {
            return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
        }
        let mut rows = Vec::new();
        rows.try_reserve_exact(expected)
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        for (wire_ordinal, committed) in committed.into_iter().enumerate() {
            validate_committed_public_row(
                &committed,
                &self,
                frame_id,
                available_at,
                wire_ordinal,
                expected,
            )?;
            let ordinal = u16::try_from(wire_ordinal)
                .map_err(|_| CoinbaseMarketPublicationError::CanonicalAlignmentMismatch)?;
            rows.push(CoinbaseQualifiedPublicRow::new(
                ordinal,
                committed.into_parts().event,
            ));
        }
        let tokens = CoinbaseMarketSealedTokens::AdvancedTrade(token);
        self.validate_tokens(&tokens)?;
        match self.publish_qualified(
            tokens,
            CoinbaseQualifiedMarketPublication::AdvancedTrade {
                rows,
                omissions: Vec::new(),
            },
            Some(frame_id),
        )? {
            CoinbaseSealedMarketPublication::Published(binding) => Ok(binding),
            CoinbaseSealedMarketPublication::SealedRaw(_) => {
                Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch)
            }
        }
    }

    /// Consumes an already sealed public frame into an explicit raw-only terminal disposition.
    pub fn into_sealed_raw(
        mut self,
        reason: CoinbaseMarketNonPublicationReason,
    ) -> Result<CoinbaseSealedRawMarketPublication, CoinbaseMarketPublicationError> {
        let state = std::mem::replace(
            &mut self.public_state,
            CoinbasePublicPublicationState::NotApplicable,
        );
        let CoinbasePublicPublicationState::Sealed { token, .. } = state else {
            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
        };
        Ok(CoinbaseSealedRawMarketPublication {
            tokens: CoinbaseMarketSealedTokens::AdvancedTrade(token),
            reason,
        })
    }
}

#[derive(Debug)]
struct CoinbaseDirectSnapshotPublicationEvidence {
    status: u16,
    body_digest: EvidenceDigest,
    body_length: u64,
    received_at: Timestamp,
    initial_source_identifier: SourceIdentifier,
}

#[derive(Debug)]
struct CoinbaseDirectReplayPublicationEvidence {
    sequence: u64,
    provider_timestamp: Timestamp,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
    native_semantics: Bytes,
}

impl CoinbaseMarketHandoff {
    /// Splits one public Advanced Trade handoff into the generic live batch and a one-use durable
    /// publication continuation over the exact capture-owned physical frame.
    ///
    /// The cloned provider-normalized graph is bounded by the common decoder limit and remains
    /// inside the publication continuation. Canonical events are accepted later only from the
    /// live runtime's non-constructible post-commit export for this exact [`FrameId`](market_squawk_sources::FrameId).
    pub fn into_pending_publication(
        self,
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
        available_at: Timestamp,
    ) -> Result<
        (
            DecodedProviderBatch,
            CoinbaseMarketSealRejoin,
            ProviderCaptureSealRequest,
        ),
        CoinbaseMarketPublicationError,
    > {
        validate_public_capture(&self, frame, &capture, available_at)?;
        let decoded_retained_bytes = self
            .typed_batch()
            .retained_bytes()
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        let publication_batch = self.typed_batch().clone();
        let frame_id = frame.frame().frame_id();
        let receipt = capture.receipt();
        let [physical] = receipt.frames() else {
            return Err(CoinbaseMarketPublicationError::RawEvidenceMismatch);
        };
        let dataset = receipt.dataset().clone();
        let stream_identity = receipt.stream_identity().clone();
        let physical_connection_id = physical.connection_id();
        let physical_event_ids = vec![physical.event_id()].into_boxed_slice();
        let (evidence, raw, live_batch) = self.into_parts();
        let CoinbaseMarketRawLineage::AdvancedTrade(_payload) = raw else {
            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
        };
        let expected_channel = expected_provider_channel(evidence.feed())?;
        let (expectation, seal_request) = capture.into_sealing_parts();
        let rejoin = CoinbaseMarketSealRejoin {
            evidence,
            typed_batch: publication_batch,
            expected_channel,
            dataset,
            stream_identity,
            physical_connection_id,
            physical_event_ids,
            raw: CoinbasePublicationRawEvidence::AdvancedTrade {
                source_sequence: None,
                exchange_at: None,
            },
            public_state: CoinbasePublicPublicationState::AwaitingSeal {
                expectation,
                frame_id,
                available_at,
                decoded_retained_bytes,
            },
        };
        Ok((live_batch, rejoin, seal_request))
    }

    /// Consumes exact provider lineage into application-owned seal material plus opaque rejoin.
    pub fn into_publication_seal_handoff(
        self,
        context: CoinbaseMarketPublicationContext,
    ) -> Result<
        (CoinbaseMarketSealRejoin, CoinbaseMarketSealMaterial),
        CoinbaseMarketPublicationError,
    > {
        let (evidence, raw, typed_batch) = self.into_parts();
        let binding = typed_batch.evidence().binding();
        let expected_channel = expected_provider_channel(evidence.feed())?;
        let source_id = binding.source_id().clone();
        let metadata_revision = binding.metadata_revision().clone();
        let CoinbaseMarketPublicationContext {
            dataset,
            stream_identity,
            physical,
        } = context;
        let CoinbaseMarketPhysicalCaptureIdentity {
            connection_id,
            event_ids,
        } = physical;

        match raw {
            CoinbaseMarketRawLineage::AdvancedTrade(payload) => {
                if event_ids.len() != 1 {
                    return Err(CoinbaseMarketPublicationError::InvalidPhysicalIdentity);
                }
                let decoder = typed_batch.evidence();
                let frame = CoinbaseMarketRawSealFrame {
                    event_id: event_ids[0],
                    connection_id,
                    source_sequence: Some(evidence.continuity().terminal()),
                    exchange_at: Some(evidence.provider_published_at()),
                    received_at: decoder.received_at(),
                    payload: Bytes::copy_from_slice(payload.as_bytes()),
                };
                let material = CoinbaseEventMicrobatchSealMaterial {
                    source_id,
                    metadata_revision,
                    dataset: dataset.clone(),
                    stream_identity: stream_identity.clone(),
                    frames: vec![frame].into_boxed_slice(),
                };
                let source_sequence = Some(evidence.continuity().terminal());
                let exchange_at = Some(evidence.provider_published_at());
                Ok((
                    CoinbaseMarketSealRejoin {
                        evidence,
                        typed_batch,
                        expected_channel,
                        dataset,
                        stream_identity,
                        physical_connection_id: connection_id,
                        physical_event_ids: event_ids.into_boxed_slice(),
                        raw: CoinbasePublicationRawEvidence::AdvancedTrade {
                            source_sequence,
                            exchange_at,
                        },
                        public_state: CoinbasePublicPublicationState::NotApplicable,
                    },
                    CoinbaseMarketSealMaterial::AdvancedTrade(material),
                ))
            }
            CoinbaseMarketRawLineage::DirectInitial(lineage) => {
                let (snapshot, replay) = lineage.into_sealing_split();
                if event_ids.len() != replay.len().saturating_add(1) {
                    return Err(CoinbaseMarketPublicationError::InvalidPhysicalIdentity);
                }
                let snapshot_receipt = snapshot.receipt();
                if snapshot_receipt.body_length() > MAX_PROVIDER_CAPTURE_PAGE_BYTES {
                    return Err(
                        CoinbaseMarketPublicationError::SnapshotExceedsCommonSealFrame {
                            bytes: snapshot_receipt.body_length(),
                            max: MAX_PROVIDER_CAPTURE_PAGE_BYTES,
                        },
                    );
                }
                let mut body = Vec::new();
                body.try_reserve_exact(
                    usize::try_from(snapshot_receipt.body_length())
                        .map_err(|_| CoinbaseMarketPublicationError::Allocation)?,
                )
                .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
                snapshot
                    .reader()
                    .read_to_end(&mut body)
                    .map_err(|_| CoinbaseMarketPublicationError::RawEvidenceMismatch)?;
                if u64::try_from(body.len()).ok() != Some(snapshot_receipt.body_length())
                    || EvidenceDigest::new(
                        market_squawk_domain::DigestAlgorithm::Sha256,
                        Sha256::digest(&body).into(),
                    ) != snapshot_receipt.body_digest()
                {
                    return Err(CoinbaseMarketPublicationError::RawEvidenceMismatch);
                }
                let segments = snapshot_receipt
                    .segments()
                    .iter()
                    .map(|segment| CoinbaseDirectSnapshotSegmentEvidence {
                        ordinal: segment.ordinal(),
                        body_length: segment.body_length(),
                        body_digest: segment.body_digest(),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let snapshot_frame = CoinbaseMarketRawSealFrame {
                    event_id: event_ids[0],
                    connection_id,
                    source_sequence: Some(0),
                    exchange_at: evidence.snapshot_provider_at(),
                    received_at: snapshot_receipt.received_at(),
                    payload: Bytes::from(body),
                };
                let snapshot_material = CoinbaseDirectSnapshotSealMaterial {
                    source_id: source_id.clone(),
                    metadata_revision: metadata_revision.clone(),
                    dataset: dataset.clone(),
                    request_identity: evidence.request_set_digest(),
                    method: snapshot_receipt.method(),
                    final_url: snapshot_receipt.final_url().to_owned().into_boxed_str(),
                    status: snapshot_receipt.status(),
                    declared_body_length: snapshot_receipt.declared_body_length(),
                    received_at: snapshot_receipt.received_at(),
                    body_digest: snapshot_receipt.body_digest(),
                    segments,
                    frame: snapshot_frame,
                };
                let initial_source_identifier = direct_book_identifier(
                    match evidence.continuity() {
                        CoinbaseMarketContinuity::SnapshotContiguous { snapshot, .. } => {
                            snapshot.get()
                        }
                        CoinbaseMarketContinuity::ProviderCursorUnverified { .. } => {
                            return Err(CoinbaseMarketPublicationError::ProfileMismatch);
                        }
                    },
                    snapshot_receipt.body_digest(),
                )?;
                let snapshot_evidence = CoinbaseDirectSnapshotPublicationEvidence {
                    status: snapshot_receipt.status(),
                    body_digest: snapshot_receipt.body_digest(),
                    body_length: snapshot_receipt.body_length(),
                    received_at: snapshot_receipt.received_at(),
                    initial_source_identifier,
                };

                let mut raw_frames = Vec::new();
                let mut replay_evidence = Vec::new();
                raw_frames
                    .try_reserve_exact(replay.len())
                    .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
                replay_evidence
                    .try_reserve_exact(replay.len())
                    .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
                for (ordinal, frame) in replay.into_iter().enumerate() {
                    let (event, payload, native_trade) = frame.into_parts();
                    let decoder = event.evidence();
                    let payload_digest = decoder.payload_digest();
                    let received_at = decoder.received_at();
                    let provider_timestamp = event.timestamp();
                    let sequence = event.sequence().get();
                    replay_evidence.push(CoinbaseDirectReplayPublicationEvidence {
                        sequence,
                        provider_timestamp,
                        received_at,
                        payload_digest,
                        native_semantics: encode_direct_event(&event, native_trade.as_ref())?,
                    });
                    raw_frames.push(CoinbaseMarketRawSealFrame {
                        event_id: event_ids[ordinal + 1],
                        connection_id,
                        source_sequence: Some(sequence),
                        exchange_at: Some(provider_timestamp),
                        received_at,
                        payload: Bytes::copy_from_slice(payload.as_bytes()),
                    });
                }
                let replay_material = CoinbaseEventMicrobatchSealMaterial {
                    source_id,
                    metadata_revision,
                    dataset: dataset.clone(),
                    stream_identity: stream_identity.clone(),
                    frames: raw_frames.into_boxed_slice(),
                };
                Ok((
                    CoinbaseMarketSealRejoin {
                        evidence,
                        typed_batch,
                        expected_channel,
                        dataset,
                        stream_identity,
                        physical_connection_id: connection_id,
                        physical_event_ids: event_ids.into_boxed_slice(),
                        raw: CoinbasePublicationRawEvidence::ExchangeDirect {
                            snapshot: snapshot_evidence,
                            replay: replay_evidence.into_boxed_slice(),
                        },
                        public_state: CoinbasePublicPublicationState::NotApplicable,
                    },
                    CoinbaseMarketSealMaterial::ExchangeDirect {
                        snapshot: snapshot_material,
                        replay: replay_material,
                    },
                ))
            }
        }
    }
}

impl CoinbaseMarketSealRejoin {
    /// Rejoins exact common seal tokens and publishes only already-qualified canonical events.
    pub fn try_rejoin(
        self,
        tokens: CoinbaseMarketSealedTokens,
        qualification: CoinbaseMarketQualificationOutcome,
    ) -> Result<CoinbaseSealedMarketPublication, CoinbaseMarketPublicationError> {
        self.validate_tokens(&tokens)?;
        match qualification {
            CoinbaseMarketQualificationOutcome::Abstained(reason)
            | CoinbaseMarketQualificationOutcome::Unavailable(reason) => {
                Ok(CoinbaseSealedMarketPublication::SealedRaw(
                    CoinbaseSealedRawMarketPublication { tokens, reason },
                ))
            }
            CoinbaseMarketQualificationOutcome::Qualified(qualified) => {
                self.publish_qualified(tokens, qualified, None)
            }
        }
    }

    fn validate_tokens(
        &self,
        tokens: &CoinbaseMarketSealedTokens,
    ) -> Result<(), CoinbaseMarketPublicationError> {
        match (&self.raw, tokens) {
            (
                CoinbasePublicationRawEvidence::AdvancedTrade {
                    source_sequence,
                    exchange_at,
                },
                CoinbaseMarketSealedTokens::AdvancedTrade(token),
            ) => validate_event_token(
                token,
                self.typed_batch.evidence().binding().source_id(),
                self.typed_batch.evidence().binding().metadata_revision(),
                &self.dataset,
                &self.stream_identity,
                self.physical_connection_id,
                &self.physical_event_ids,
                &[ExpectedFrame {
                    sequence: *source_sequence,
                    exchange_at: *exchange_at,
                    received_at: self.typed_batch.evidence().received_at(),
                    payload_digest: self.typed_batch.evidence().payload_digest(),
                }],
            ),
            (
                CoinbasePublicationRawEvidence::ExchangeDirect { snapshot, replay },
                CoinbaseMarketSealedTokens::ExchangeDirect {
                    snapshot: snapshot_token,
                    replay: replay_token,
                },
            ) => {
                validate_snapshot_token(
                    snapshot_token,
                    self.typed_batch.evidence().binding().source_id(),
                    self.typed_batch.evidence().binding().metadata_revision(),
                    &self.dataset,
                    self.evidence.request_set_digest(),
                    self.physical_connection_id,
                    self.physical_event_ids[0],
                    snapshot,
                )?;
                let expected = replay
                    .iter()
                    .map(|frame| ExpectedFrame {
                        sequence: Some(frame.sequence),
                        exchange_at: Some(frame.provider_timestamp),
                        received_at: frame.received_at,
                        payload_digest: frame.payload_digest,
                    })
                    .collect::<Vec<_>>();
                validate_event_token(
                    replay_token,
                    self.typed_batch.evidence().binding().source_id(),
                    self.typed_batch.evidence().binding().metadata_revision(),
                    &self.dataset,
                    &self.stream_identity,
                    self.physical_connection_id,
                    &self.physical_event_ids[1..],
                    &expected,
                )
            }
            _ => Err(CoinbaseMarketPublicationError::ProfileMismatch),
        }
    }

    fn publish_qualified(
        self,
        tokens: CoinbaseMarketSealedTokens,
        qualified: CoinbaseQualifiedMarketPublication,
        physical_frame_id: Option<market_squawk_sources::FrameId>,
    ) -> Result<CoinbaseSealedMarketPublication, CoinbaseMarketPublicationError> {
        match (&self.raw, tokens, qualified) {
            (
                CoinbasePublicationRawEvidence::AdvancedTrade { .. },
                CoinbaseMarketSealedTokens::AdvancedTrade(token),
                CoinbaseQualifiedMarketPublication::AdvancedTrade { rows, omissions },
            ) => {
                let (events, native, ordinals) = self.public_rows(rows, omissions)?;
                let sidecar = self.encode_batch_sidecar(
                    ProviderPublicationBindingKind::EventMicrobatch,
                    physical_frame_id,
                )?;
                let batch = ProviderMarketEventBatch::try_new(
                    self.typed_batch.evidence().binding().source_id().clone(),
                    self.typed_batch
                        .evidence()
                        .binding()
                        .metadata_revision()
                        .clone(),
                    self.dataset,
                    events,
                )?;
                let native = ProviderMarketEventNativeLineageBatch::try_new(
                    ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1,
                    &batch,
                    native,
                    Some(sidecar),
                )?;
                let binding =
                    SealedProviderEventMicrobatchBinding::try_new(token, batch, native, ordinals)?;
                binding.validate()?;
                Ok(CoinbaseSealedMarketPublication::Published(binding.into()))
            }
            (
                CoinbasePublicationRawEvidence::ExchangeDirect { snapshot, replay },
                CoinbaseMarketSealedTokens::ExchangeDirect {
                    snapshot: snapshot_token,
                    replay: replay_token,
                },
                CoinbaseQualifiedMarketPublication::ExchangeDirect {
                    initial_snapshot,
                    replay_rows,
                    replay_omissions,
                },
            ) => {
                validate_direct_snapshot_event(&initial_snapshot, &self, &snapshot)?;
                let response_batch = ProviderMarketEventBatch::try_new(
                    self.typed_batch.evidence().binding().source_id().clone(),
                    self.typed_batch
                        .evidence()
                        .binding()
                        .metadata_revision()
                        .clone(),
                    self.dataset.clone(),
                    vec![initial_snapshot],
                )?;
                let response_native = ProviderMarketEventNativeLineageBatch::try_new(
                    ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1,
                    &response_batch,
                    vec![ProviderMarketEventNativeLineageRow::try_new(
                        self.evidence.instrument_attestation().clone(),
                        encode_snapshot_native(&snapshot, &self.evidence)?,
                    )?],
                    Some(self.encode_batch_sidecar(
                        ProviderPublicationBindingKind::CompositeResponseEvent,
                        None,
                    )?),
                )?;
                let response_binding = SealedProviderResponseMarketEventBinding::try_new(
                    snapshot_token,
                    response_batch,
                    response_native,
                    vec![0],
                )?;
                let (events, native, ordinals) =
                    self.direct_replay_rows(&replay, replay_rows, replay_omissions)?;
                let event_batch = ProviderMarketEventBatch::try_new(
                    self.typed_batch.evidence().binding().source_id().clone(),
                    self.typed_batch
                        .evidence()
                        .binding()
                        .metadata_revision()
                        .clone(),
                    self.dataset,
                    events,
                )?;
                let event_native = ProviderMarketEventNativeLineageBatch::try_new(
                    ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1,
                    &event_batch,
                    native,
                    None,
                )?;
                let event_binding = SealedProviderEventMicrobatchBinding::try_new(
                    replay_token,
                    event_batch,
                    event_native,
                    ordinals,
                )?;
                let composite = SealedProviderCompositeResponseEventBinding::try_new(
                    response_binding,
                    event_binding,
                )?;
                Ok(CoinbaseSealedMarketPublication::Published(composite.into()))
            }
            _ => Err(CoinbaseMarketPublicationError::ProfileMismatch),
        }
    }

    fn public_rows(
        &self,
        rows: Vec<CoinbaseQualifiedPublicRow>,
        omissions: Vec<CoinbaseMarketOmission>,
    ) -> Result<
        (
            Vec<MarketEvent>,
            Vec<ProviderMarketEventNativeLineageRow>,
            Vec<u16>,
        ),
        CoinbaseMarketPublicationError,
    > {
        let observations = self.typed_batch.observations();
        let mut covered = BTreeSet::new();
        let mut events = Vec::new();
        let mut native = Vec::new();
        let mut ordinals = Vec::new();
        events
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        native
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        ordinals
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        for row in rows {
            let index = usize::from(row.observation_ordinal);
            let observation = observations
                .get(index)
                .ok_or(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch)?;
            if !covered.insert(row.observation_ordinal) {
                return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
            }
            validate_public_event(&row.event, observation, self)?;
            native.push(ProviderMarketEventNativeLineageRow::try_new(
                observation.instrument_attestation().clone(),
                encode_public_native(observation, row.observation_ordinal, &row.event, self)?,
            )?);
            events.push(row.event);
            ordinals.push(0);
        }
        for omission in omissions {
            if usize::from(omission.ordinal) >= observations.len()
                || !covered.insert(omission.ordinal)
            {
                return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
            }
        }
        if covered.len() != observations.len() || events.is_empty() {
            return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
        }
        Ok((events, native, ordinals))
    }

    fn direct_replay_rows(
        &self,
        replay: &[CoinbaseDirectReplayPublicationEvidence],
        rows: Vec<CoinbaseQualifiedDirectReplayRow>,
        omissions: Vec<CoinbaseMarketOmission>,
    ) -> Result<
        (
            Vec<MarketEvent>,
            Vec<ProviderMarketEventNativeLineageRow>,
            Vec<u16>,
        ),
        CoinbaseMarketPublicationError,
    > {
        let mut covered = BTreeSet::new();
        let mut events = Vec::new();
        let mut native = Vec::new();
        let mut ordinals = Vec::new();
        events
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        native
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        ordinals
            .try_reserve_exact(rows.len())
            .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
        for row in rows {
            let index = usize::from(row.replay_frame_ordinal);
            let frame = replay
                .get(index)
                .ok_or(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch)?;
            if !covered.insert(row.replay_frame_ordinal) {
                return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
            }
            validate_direct_replay_event(&row.event, frame, self)?;
            native.push(ProviderMarketEventNativeLineageRow::try_new(
                self.evidence.instrument_attestation().clone(),
                encode_direct_native_row(frame, row.replay_frame_ordinal, &row.event)?,
            )?);
            events.push(row.event);
            ordinals.push(row.replay_frame_ordinal);
        }
        for omission in omissions {
            if usize::from(omission.ordinal) >= replay.len() || covered.contains(&omission.ordinal)
            {
                return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
            }
            covered.insert(omission.ordinal);
        }
        if covered.len() != replay.len() || events.is_empty() {
            return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
        }
        Ok((events, native, ordinals))
    }

    fn encode_batch_sidecar(
        &self,
        kind: ProviderPublicationBindingKind,
        physical_frame_id: Option<market_squawk_sources::FrameId>,
    ) -> Result<Bytes, CoinbaseMarketPublicationError> {
        #[derive(Serialize)]
        struct Sidecar<'a> {
            schema: &'static str,
            feed: &'static str,
            channel: &'static str,
            publication_kind: &'static str,
            provider_product: &'a str,
            venue: &'a str,
            instrument: String,
            native_input_depth: Option<&'static str>,
            output_depth: Option<&'static str>,
            request_set_digest: EvidenceDigest,
            subscription_digest: EvidenceDigest,
            subscription_acknowledgement: Option<EvidenceDigest>,
            continuity_snapshot: Option<u64>,
            continuity_terminal: u64,
            source_generation: u64,
            generation_frame_ordinal: Option<u64>,
            provider_published_at: i64,
            snapshot_provider_at: Option<i64>,
        }
        let (continuity_snapshot, continuity_terminal) = match self.evidence.continuity() {
            CoinbaseMarketContinuity::ProviderCursorUnverified { terminal } => (None, terminal),
            CoinbaseMarketContinuity::SnapshotContiguous { snapshot, terminal } => {
                (Some(snapshot.get()), terminal.get())
            }
        };
        let sidecar = Sidecar {
            schema: "market-squawk/coinbase-market-publication-sidecar/v1",
            feed: feed_name(self.evidence.feed()),
            channel: channel_name(self.evidence.channel()),
            publication_kind: publication_kind_name(kind),
            provider_product: self.evidence.product().as_source_identifier().as_str(),
            venue: self.evidence.venue().as_str(),
            instrument: self.evidence.configured_instrument().to_string(),
            native_input_depth: self.evidence.native_input_depth().map(depth_name),
            output_depth: self.evidence.output_depth().map(depth_name),
            request_set_digest: self.evidence.request_set_digest(),
            subscription_digest: self.evidence.subscription_digest(),
            subscription_acknowledgement: self
                .evidence
                .subscription_acknowledgement()
                .map(|value| value.content_digest()),
            continuity_snapshot,
            continuity_terminal,
            source_generation: self
                .typed_batch
                .evidence()
                .binding()
                .connection_generation()
                .get(),
            generation_frame_ordinal: physical_frame_id.map(|frame_id| frame_id.get()),
            provider_published_at: self.evidence.provider_published_at().unix_nanos(),
            snapshot_provider_at: self
                .evidence
                .snapshot_provider_at()
                .map(Timestamp::unix_nanos),
        };
        encode_json(&sidecar)
    }
}

#[derive(Clone, Copy)]
struct ExpectedFrame {
    sequence: Option<u64>,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
}

fn validate_public_capture(
    handoff: &CoinbaseMarketHandoff,
    validated: &ValidatedRawMarketFrame<'_>,
    capture: &ProviderEventMicrobatchMaterial,
    available_at: Timestamp,
) -> Result<(), CoinbaseMarketPublicationError> {
    if handoff.evidence().feed() != CoinbaseMarketFeed::AdvancedTradePublic
        || !matches!(
            handoff.raw_lineage(),
            CoinbaseMarketRawLineage::AdvancedTrade(_)
        )
    {
        return Err(CoinbaseMarketPublicationError::ProfileMismatch);
    }
    let frame = validated.frame();
    let receipt = capture.receipt();
    let [physical] = receipt.frames() else {
        return Err(CoinbaseMarketPublicationError::RawEvidenceMismatch);
    };
    let [record] = capture.records() else {
        return Err(CoinbaseMarketPublicationError::RawEvidenceMismatch);
    };
    let decoder = handoff.typed_batch().evidence();
    let binding = decoder.binding();
    let payload_bytes = u64::try_from(frame.payload().len())
        .map_err(|_| CoinbaseMarketPublicationError::RawEvidenceMismatch)?;
    if frame.transport() != TransportFrameKind::Text
        || frame.source_id() != binding.source_id()
        || frame.metadata_revision() != binding.metadata_revision()
        || frame.session_id() != binding.session_id()
        || frame.connection_generation() != binding.connection_generation()
        || frame.frame_id() != decoder.frame_id()
        || frame.received_at() != decoder.received_at()
        || frame.payload() != handoff.raw_payload().as_bytes()
        || handoff.raw_payload_digest() != decoder.payload_digest()
        || receipt.source_id() != frame.source_id()
        || receipt.metadata_revision() != frame.metadata_revision()
        || physical.ordinal() != 0
        || physical.event_id() == [0; 16]
        || physical.connection_id() == [0; 16]
        || physical.source_sequence().is_some()
        || physical.exchange_at().is_some()
        || physical.received_at() != frame.received_at()
        || physical.payload_bytes() != payload_bytes
        || physical.payload_digest() != decoder.payload_digest()
        || record.source() != frame.source_id().as_str()
        || record.source_sequence().is_some()
        || record.exchange_at().is_some()
        || record.payload() != frame.payload()
        || *record.event_id().as_bytes() != physical.event_id()
        || *record.connection_id().as_bytes() != physical.connection_id()
        || available_at < frame.received_at()
    {
        return Err(CoinbaseMarketPublicationError::RawEvidenceMismatch);
    }
    Ok(())
}

fn validate_event_token(
    token: &ProviderEventMicrobatchToken,
    source_id: &SourceId,
    revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    stream_identity: &SourceIdentifier,
    connection_id: [u8; 16],
    event_ids: &[[u8; 16]],
    expected: &[ExpectedFrame],
) -> Result<(), CoinbaseMarketPublicationError> {
    let capture = token.persisted_receipt().capture();
    if capture.source_id() != source_id
        || capture.metadata_revision() != revision
        || capture.dataset() != dataset
        || capture.stream_identity() != stream_identity
        || capture.frames().len() != expected.len()
        || event_ids.len() != expected.len()
    {
        return Err(CoinbaseMarketPublicationError::SealedReceiptMismatch);
    }
    for ((frame, expected), event_id) in capture.frames().iter().zip(expected).zip(event_ids) {
        if frame.event_id() != *event_id
            || frame.connection_id() != connection_id
            || frame.source_sequence() != expected.sequence
            || frame.exchange_at() != expected.exchange_at
            || frame.received_at() != expected.received_at
            || frame.payload_digest() != expected.payload_digest
        {
            return Err(CoinbaseMarketPublicationError::SealedReceiptMismatch);
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "exact response seal evidence stays explicit"
)]
fn validate_snapshot_token(
    token: &ProviderWholeCaptureToken,
    source_id: &SourceId,
    revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    request_identity: EvidenceDigest,
    connection_id: [u8; 16],
    event_id: [u8; 16],
    expected: &CoinbaseDirectSnapshotPublicationEvidence,
) -> Result<(), CoinbaseMarketPublicationError> {
    let receipt = token.persisted_receipt();
    let capture = receipt.capture();
    let page = capture
        .pages()
        .first()
        .ok_or(CoinbaseMarketPublicationError::SealedReceiptMismatch)?;
    let frame = receipt
        .segment()
        .frames()
        .first()
        .ok_or(CoinbaseMarketPublicationError::SealedReceiptMismatch)?;
    if capture.source_id() != source_id
        || capture.metadata_revision() != revision
        || capture.dataset() != dataset
        || capture.request_set_identity() != request_identity
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.pages().len() != 1
        || page.request_identity() != request_identity
        || page.request_page_token_digest().is_some()
        || page.response_next_page_token_digest().is_some()
        || page.http_status() != expected.status
        || page.body_bytes() != expected.body_length
        || page.body_digest() != expected.body_digest
        || page.received_at() != expected.received_at
        || frame.provider_payload_digest() != expected.body_digest
        || frame.provider_payload_bytes() != expected.body_length
        || frame.source_sequence() != Some(0)
        || receipt.segment().claim().frames().len() != 1
        || receipt.segment().frames().len() != 1
    {
        return Err(CoinbaseMarketPublicationError::SealedReceiptMismatch);
    }
    let _application_owned_ids = (connection_id, event_id);
    Ok(())
}

fn validate_public_event(
    event: &MarketEvent,
    observation: &market_squawk_sources::ProviderNormalizedObservation,
    handoff: &CoinbaseMarketSealRejoin,
) -> Result<(), CoinbaseMarketPublicationError> {
    let provenance = event_provenance(event);
    validate_common_event(
        event,
        provenance,
        handoff,
        observation.source_identifier(),
        handoff.typed_batch.evidence().payload_digest(),
        handoff.typed_batch.evidence().received_at(),
        expected_timestamp(observation.timestamp()),
        observation.event_class(),
        observation.depth(),
    )?;
    if canonical_sequence(event) != expected_sequence(observation.sequence()) {
        return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "committed physical, wire, canonical, and clock coordinates remain explicit"
)]
fn validate_committed_public_row(
    committed: &CommittedResearchMarketObservation,
    handoff: &CoinbaseMarketSealRejoin,
    frame_id: market_squawk_sources::FrameId,
    available_at: Timestamp,
    wire_ordinal: usize,
    row_count: usize,
) -> Result<(), CoinbaseMarketPublicationError> {
    let observation = handoff
        .typed_batch
        .observations()
        .get(wire_ordinal)
        .ok_or(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch)?;
    let event = committed.event();
    let provenance = event_provenance(event);
    validate_public_event(event, observation, handoff)?;
    if committed.qualification().recorded_quality() != DataQuality::DirectUnverified
        || committed.qualification().binding() != provenance.binding()
        || committed.connection_generation()
            != handoff
                .typed_batch
                .evidence()
                .binding()
                .connection_generation()
        || committed.frame_id() != Some(frame_id)
        || committed.wire_ordinal() != wire_ordinal
        || committed.row_count() != row_count
        || provenance.available_at() < available_at
        || !stable_public_trade_identity_matches(observation, committed)
    {
        return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
    }
    Ok(())
}

fn stable_public_trade_identity_matches(
    observation: &market_squawk_sources::ProviderNormalizedObservation,
    committed: &CommittedResearchMarketObservation,
) -> bool {
    match observation.payload() {
        ProviderObservationPayload::Trade { trade_id, .. } => {
            committed.stable_trade_id() == Some(trade_id)
        }
        ProviderObservationPayload::BookSnapshot(_) | ProviderObservationPayload::BookDelta(_) => {
            committed.stable_trade_id().is_none()
        }
        ProviderObservationPayload::Quote { .. }
        | ProviderObservationPayload::Auction { .. }
        | ProviderObservationPayload::TradingHalt { .. }
        | ProviderObservationPayload::InstrumentStatus { .. }
        | ProviderObservationPayload::CorporateAction { .. } => false,
    }
}

fn validate_direct_snapshot_event(
    event: &MarketEvent,
    handoff: &CoinbaseMarketSealRejoin,
    snapshot: &CoinbaseDirectSnapshotPublicationEvidence,
) -> Result<(), CoinbaseMarketPublicationError> {
    validate_common_event(
        event,
        event_provenance(event),
        handoff,
        &snapshot.initial_source_identifier,
        snapshot.body_digest,
        snapshot.received_at,
        handoff.evidence.snapshot_provider_at(),
        LiveEventClass::BookSnapshot,
        Some(MarketDepth::PriceLevel),
    )?;
    let expected = match handoff.evidence.continuity() {
        CoinbaseMarketContinuity::SnapshotContiguous { snapshot, .. } => Some(snapshot.get()),
        CoinbaseMarketContinuity::ProviderCursorUnverified { .. } => None,
    };
    if canonical_sequence(event) != expected {
        return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
    }
    Ok(())
}

fn validate_direct_replay_event(
    event: &MarketEvent,
    frame: &CoinbaseDirectReplayPublicationEvidence,
    handoff: &CoinbaseMarketSealRejoin,
) -> Result<(), CoinbaseMarketPublicationError> {
    let provenance = event_provenance(event);
    let event_class = event_class_of(event);
    let expected_sequence = match event_class {
        LiveEventClass::BookSnapshot | LiveEventClass::BookDelta => Some(frame.sequence),
        LiveEventClass::Trade
        | LiveEventClass::Quote
        | LiveEventClass::Auction
        | LiveEventClass::TradingHalt
        | LiveEventClass::InstrumentStatus
        | LiveEventClass::CorporateAction => None,
    };
    if provenance.source_id() != handoff.typed_batch.evidence().binding().source_id()
        || provenance.binding().metadata_revision()
            != handoff.typed_batch.evidence().binding().metadata_revision()
        || provenance.binding().session_id()
            != handoff
                .typed_batch
                .evidence()
                .binding()
                .session_id()
                .as_source_identifier()
        || provenance.binding().provider_product() != handoff.evidence.product()
        || provenance.binding().provider_channel() != &handoff.expected_channel
        || provenance.binding().venue_id() != handoff.evidence.venue()
        || provenance.binding().instrument_id() != handoff.evidence.configured_instrument()
        || provenance.binding().connection_generation()
            != handoff
                .typed_batch
                .evidence()
                .binding()
                .connection_generation()
        || provenance.binding().event_class() != event_class
        || provenance.binding().payload_digest() != frame.payload_digest
        || provenance.received_at() != frame.received_at
        || provenance.source_timestamp() != Some(frame.provider_timestamp)
        || provenance.recorded_quality() != DataQuality::DirectUnverified
        || provenance.execution_eligibility()
            != market_squawk_domain::ExecutionEligibility::Ineligible
        || provenance.available_at() < provenance.received_at()
        || provenance.ingested_at() < provenance.available_at()
        || canonical_sequence(event) != expected_sequence
    {
        return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "canonical/raw alignment stays explicit"
)]
fn validate_common_event(
    event: &MarketEvent,
    provenance: &LiveProvenance,
    handoff: &CoinbaseMarketSealRejoin,
    source_identifier: &SourceIdentifier,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    source_timestamp: Option<Timestamp>,
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
) -> Result<(), CoinbaseMarketPublicationError> {
    let binding = handoff.typed_batch.evidence().binding();
    if provenance.source_id() != binding.source_id()
        || provenance.binding().metadata_revision() != binding.metadata_revision()
        || provenance.binding().session_id() != binding.session_id().as_source_identifier()
        || provenance.binding().connection_generation() != binding.connection_generation()
        || provenance.binding().provider_product() != handoff.evidence.product()
        || provenance.binding().provider_channel() != &handoff.expected_channel
        || provenance.binding().venue_id() != handoff.evidence.venue()
        || provenance.binding().instrument_id() != handoff.evidence.configured_instrument()
        || provenance.binding().source_identifier() != source_identifier
        || provenance.binding().payload_digest() != payload_digest
        || provenance.binding().event_class() != event_class
        || provenance.received_at() != received_at
        || provenance.source_timestamp() != source_timestamp
        || event_class_of(event) != event_class
        || event_depth(event) != depth
        || provenance.recorded_quality() != DataQuality::DirectUnverified
        || provenance.execution_eligibility()
            != market_squawk_domain::ExecutionEligibility::Ineligible
        || provenance.available_at() < provenance.received_at()
        || provenance.ingested_at() < provenance.available_at()
    {
        return Err(CoinbaseMarketPublicationError::CanonicalAlignmentMismatch);
    }
    Ok(())
}

#[derive(Serialize)]
struct CanonicalNativeEnvelope<'a> {
    schema: &'static str,
    coordinate_kind: &'static str,
    coordinate_ordinal: u16,
    source_identifier: &'a str,
    event_class: &'static str,
    depth: Option<&'static str>,
    raw_payload_digest: EvidenceDigest,
    source_generation: u64,
    source_timestamp: Option<i64>,
    received_at: i64,
    available_at: i64,
    ingested_at: i64,
    provider_native: Option<&'a [u8]>,
}

fn encode_public_native(
    observation: &market_squawk_sources::ProviderNormalizedObservation,
    ordinal: u16,
    event: &MarketEvent,
    handoff: &CoinbaseMarketSealRejoin,
) -> Result<Bytes, CoinbaseMarketPublicationError> {
    let provenance = event_provenance(event);
    let envelope = CanonicalNativeEnvelope {
        schema: "market-squawk/coinbase-advanced-trade/native-row/v1",
        coordinate_kind: "decoded_observation",
        coordinate_ordinal: ordinal,
        source_identifier: observation.source_identifier().as_str(),
        event_class: event_class_name(observation.event_class()),
        depth: observation.depth().map(depth_name),
        raw_payload_digest: handoff.typed_batch.evidence().payload_digest(),
        source_generation: provenance.connection_generation().get(),
        source_timestamp: provenance.source_timestamp().map(Timestamp::unix_nanos),
        received_at: provenance.received_at().unix_nanos(),
        available_at: provenance.available_at().unix_nanos(),
        ingested_at: provenance.ingested_at().unix_nanos(),
        provider_native: None,
    };
    encode_json(&envelope)
}

fn encode_snapshot_native(
    snapshot: &CoinbaseDirectSnapshotPublicationEvidence,
    evidence: &CoinbaseMarketHandoffEvidence,
) -> Result<Bytes, CoinbaseMarketPublicationError> {
    #[derive(Serialize)]
    struct SnapshotRow<'a> {
        schema: &'static str,
        source_identifier: &'a str,
        body_digest: EvidenceDigest,
        body_length: u64,
        received_at: i64,
        provider_at: Option<i64>,
        snapshot_sequence: u64,
        terminal_sequence: u64,
    }
    let CoinbaseMarketContinuity::SnapshotContiguous {
        snapshot: start,
        terminal,
    } = evidence.continuity()
    else {
        return Err(CoinbaseMarketPublicationError::ProfileMismatch);
    };
    encode_json(&SnapshotRow {
        schema: "market-squawk/coinbase-exchange-direct/snapshot-native-row/v1",
        source_identifier: snapshot.initial_source_identifier.as_str(),
        body_digest: snapshot.body_digest,
        body_length: snapshot.body_length,
        received_at: snapshot.received_at.unix_nanos(),
        provider_at: evidence.snapshot_provider_at().map(Timestamp::unix_nanos),
        snapshot_sequence: start.get(),
        terminal_sequence: terminal.get(),
    })
}

fn encode_direct_native_row(
    frame: &CoinbaseDirectReplayPublicationEvidence,
    ordinal: u16,
    event: &MarketEvent,
) -> Result<Bytes, CoinbaseMarketPublicationError> {
    let provenance = event_provenance(event);
    let envelope = CanonicalNativeEnvelope {
        schema: "market-squawk/coinbase-exchange-direct/replay-native-row/v1",
        coordinate_kind: "replay_frame",
        coordinate_ordinal: ordinal,
        source_identifier: provenance.source_identifier().as_str(),
        event_class: event_class_name(event_class_of(event)),
        depth: event_depth(event).map(depth_name),
        raw_payload_digest: frame.payload_digest,
        source_generation: provenance.connection_generation().get(),
        source_timestamp: provenance.source_timestamp().map(Timestamp::unix_nanos),
        received_at: provenance.received_at().unix_nanos(),
        available_at: provenance.available_at().unix_nanos(),
        ingested_at: provenance.ingested_at().unix_nanos(),
        provider_native: Some(&frame.native_semantics),
    };
    encode_json(&envelope)
}

fn encode_direct_event(
    event: &market_squawk_sources::ProviderOrderEvent,
    trade: Option<&CoinbaseDirectTradeEvidence>,
) -> Result<Bytes, CoinbaseMarketPublicationError> {
    #[derive(Serialize)]
    struct NativeEvent<'a> {
        schema: &'static str,
        sequence: u64,
        provider_timestamp: i64,
        kind: &'static str,
        order_id: Option<&'a str>,
        maker_order_id: Option<&'a str>,
        trade_id: Option<u64>,
        taker_order_id: Option<&'a str>,
    }
    let (kind, order_id, maker_order_id) = match event.kind() {
        ProviderOrderEventKind::CursorOnly(_) => ("cursor_only", None, None),
        ProviderOrderEventKind::Open(order) => ("open", Some(order.order_id().as_str()), None),
        ProviderOrderEventKind::Match { maker_order_id, .. } => {
            ("match", None, Some(maker_order_id.as_str()))
        }
        ProviderOrderEventKind::Done { order_id, .. } => ("done", Some(order_id.as_str()), None),
        ProviderOrderEventKind::Change {
            order_id, reason, ..
        } => (
            match reason {
                ProviderOrderChangeReason::SelfTradePrevention => "change_stp",
                ProviderOrderChangeReason::ModifyOrder => "change_modify",
            },
            Some(order_id.as_str()),
            None,
        ),
    };
    encode_json(&NativeEvent {
        schema: "market-squawk/coinbase-exchange-direct/provider-order-event/v1",
        sequence: event.sequence().get(),
        provider_timestamp: event.timestamp().unix_nanos(),
        kind,
        order_id,
        maker_order_id,
        trade_id: trade.map(CoinbaseDirectTradeEvidence::trade_id),
        taker_order_id: trade.map(|value| value.taker_order_id().as_str()),
    })
}

fn expected_provider_channel(
    feed: CoinbaseMarketFeed,
) -> Result<ProviderChannel, CoinbaseMarketPublicationError> {
    Ok(ProviderChannel::new(
        SourceIdentifier::try_from(match feed {
            CoinbaseMarketFeed::AdvancedTradePublic => PUBLIC_CHANNEL,
            CoinbaseMarketFeed::ExchangeDirectFull => DIRECT_CHANNEL,
        })
        .map_err(|_| CoinbaseMarketPublicationError::ProfileMismatch)?,
    ))
}

fn direct_book_identifier(
    sequence: u64,
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, CoinbaseMarketPublicationError> {
    let mut value = format!("coinbase-direct-book-{sequence}-");
    value
        .try_reserve_exact(64)
        .map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
    for byte in digest.bytes() {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").map_err(|_| CoinbaseMarketPublicationError::Allocation)?;
    }
    SourceIdentifier::try_from(value).map_err(|_| CoinbaseMarketPublicationError::ProfileMismatch)
}

fn encode_json(value: &impl Serialize) -> Result<Bytes, CoinbaseMarketPublicationError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|_| CoinbaseMarketPublicationError::NativeLineage)
}

fn expected_timestamp(evidence: &ProviderTimestampEvidence) -> Option<Timestamp> {
    match evidence {
        ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
    }
}

fn expected_sequence(evidence: &ProviderSequenceEvidence) -> Option<u64> {
    match evidence {
        ProviderSequenceEvidence::Provided { value, .. } => Some(value.get()),
        ProviderSequenceEvidence::Unsupported { .. } => None,
    }
}

fn canonical_sequence(event: &MarketEvent) -> Option<u64> {
    match event {
        MarketEvent::BookSnapshot(value) => value.sequence().map(|value| value.get()),
        MarketEvent::BookDelta(value) => value.sequence().map(|value| value.get()),
        _ => None,
    }
}

fn event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(value) => value.provenance(),
        MarketEvent::Quote(value) => value.provenance(),
        MarketEvent::BookSnapshot(value) => value.provenance(),
        MarketEvent::BookDelta(value) => value.provenance(),
        MarketEvent::Auction(value) => value.provenance(),
        MarketEvent::TradingHalt(value) => value.provenance(),
        MarketEvent::InstrumentStatus(value) => value.provenance(),
        MarketEvent::CorporateAction(value) => value.provenance(),
    }
}

fn event_class_of(event: &MarketEvent) -> LiveEventClass {
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

fn event_depth(event: &MarketEvent) -> Option<MarketDepth> {
    match event {
        MarketEvent::BookSnapshot(value) => Some(value.depth()),
        MarketEvent::BookDelta(value) => Some(value.depth()),
        _ => None,
    }
}

const fn event_class_name(value: LiveEventClass) -> &'static str {
    match value {
        LiveEventClass::Trade => "trade",
        LiveEventClass::Quote => "quote",
        LiveEventClass::BookSnapshot => "book_snapshot",
        LiveEventClass::BookDelta => "book_delta",
        LiveEventClass::Auction => "auction",
        LiveEventClass::TradingHalt => "trading_halt",
        LiveEventClass::InstrumentStatus => "instrument_status",
        LiveEventClass::CorporateAction => "corporate_action",
    }
}

const fn depth_name(value: MarketDepth) -> &'static str {
    match value {
        MarketDepth::TopOfBook => "top_of_book",
        MarketDepth::PriceLevel => "price_level",
        MarketDepth::OrderLevel => "order_level",
    }
}

const fn feed_name(value: CoinbaseMarketFeed) -> &'static str {
    match value {
        CoinbaseMarketFeed::AdvancedTradePublic => "advanced_trade_public",
        CoinbaseMarketFeed::ExchangeDirectFull => "exchange_direct_full",
    }
}

const fn channel_name(value: CoinbaseMarketChannel) -> &'static str {
    match value {
        CoinbaseMarketChannel::Level2 => "level2",
        CoinbaseMarketChannel::MarketTrades => "market_trades",
        CoinbaseMarketChannel::Full => "full",
    }
}

const fn publication_kind_name(value: ProviderPublicationBindingKind) -> &'static str {
    match value {
        ProviderPublicationBindingKind::ResponseSet => "response_set",
        ProviderPublicationBindingKind::ResponseMarketEvent => "response_market_event",
        ProviderPublicationBindingKind::EventMicrobatch => "event_microbatch",
        ProviderPublicationBindingKind::CompositeResponseEvent => "composite_response_event",
    }
}

/// Coinbase raw seal, rejoin, qualification, or canonical publication failure.
#[derive(Debug, Error)]
pub enum CoinbaseMarketPublicationError {
    #[error("Coinbase physical capture identities are invalid")]
    InvalidPhysicalIdentity,
    #[error("Coinbase market handoff profile does not match the selected publication shape")]
    ProfileMismatch,
    #[error("Coinbase raw provider evidence is inconsistent")]
    RawEvidenceMismatch,
    #[error("Coinbase Direct snapshot has {bytes} bytes, above common one-frame seal limit {max}")]
    SnapshotExceedsCommonSealFrame { bytes: u64, max: u64 },
    #[error("Coinbase sealed receipt does not rejoin the exact raw handoff")]
    SealedReceiptMismatch,
    #[error("Coinbase qualified canonical rows do not align to exact provider coordinates")]
    CanonicalAlignmentMismatch,
    #[error("Coinbase provider-native lineage could not be encoded")]
    NativeLineage,
    #[error("Coinbase bounded publication allocation failed")]
    Allocation,
    #[error("common sealed provider publication rejected Coinbase evidence")]
    Common(#[from] market_squawk_sources::ProviderCaptureError),
}
