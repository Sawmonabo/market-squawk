//! Seal-first Kraken Spot public market-data publication material.
//!
//! Kraken decoding produces provider-normalized observations, while instrument-owned state in
//! `market-squawk-live` owns checksum/state qualification and canonical market-event construction.
//! This module rejoins the exact sealed frame only with the live plane's one-use committed research
//! observations and then constructs the common provider-event publication binding.

use std::mem::size_of;

use bytes::Bytes;
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, EvidenceDigest, InstrumentId, LiveEventClass,
    LiveProvenance, MarketDepth, MarketEvent, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_live::CommittedResearchMarketObservation;
use market_squawk_sources::{
    ProviderCaptureError, ProviderCaptureSealRequest, ProviderEventMicrobatchMaterial,
    ProviderEventMicrobatchSealExpectation, ProviderEventMicrobatchToken, ProviderMarketEventBatch,
    ProviderMarketEventNativeLineageBatch, ProviderNativeLineageImplementation,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderTimestampEvidence, SealedProviderCaptureMaterial,
    SealedProviderEventMicrobatchBinding, SealedProviderEventMicrobatchReceipt,
    SealedProviderPublicationBinding, SourceMetadataProvider, TransportFrameKind,
    ValidatedRawMarketFrame,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    KrakenChannel, KrakenConfig, KrakenDecodeOutcome, KrakenDepth, KrakenPublicControl,
    KrakenPublicationDecodeOutcome,
};

const KRAKEN_VENUE: &str = "kraken";

/// Explicit reason a sealed Kraken frame produced no market observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenPublicationAbstention {
    /// Connection liveness only.
    Heartbeat,
    /// Application-level ping response only.
    Pong,
    /// Provider engine status only.
    Online,
    /// Book subscription acknowledgement only.
    BookSubscribed,
    /// Trade subscription acknowledgement only.
    TradeSubscribed,
}

/// Explicit reason a sealed Kraken frame cannot yield a canonical publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenPublicationUnavailable {
    /// The provider refused the exact public subscription.
    SubscriptionRefused,
    /// The decoder rejected the provider payload.
    DecodeRejected,
    /// A fresh provider snapshot or generation is required.
    ResynchronizationRequired,
    /// The source generation was quarantined.
    Quarantined,
    /// The qualified canonical output is not available at the current live/application boundary.
    QualifiedCanonicalOutputUnavailable,
    /// The bounded application publication lane could not admit the observation.
    ApplicationBackpressure,
}

/// Immutable value evidence shared by every state of one Kraken publication handoff.
#[derive(Debug)]
pub struct KrakenPublicationEvidence {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    provider_product: SourceIdentifier,
    feed: SourceIdentifier,
    venue: VenueId,
    provider_symbol: String,
    instrument_id: InstrumentId,
    retained_depth: Option<KrakenDepth>,
    session_id: SourceIdentifier,
    connection_generation: ConnectionGeneration,
    generation_frame_ordinal: u64,
    microbatch_frame_ordinal: u16,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    raw_payload_digest: EvidenceDigest,
    received_at: Timestamp,
    available_at: Timestamp,
}

impl KrakenPublicationEvidence {
    /// Returns the exact source authority identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact metadata interpretation revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the common provider dataset selected by application composition.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact application-defined stream boundary identity.
    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    /// Returns the code-owned Kraken Spot product identity.
    pub const fn provider_product(&self) -> &SourceIdentifier {
        &self.provider_product
    }

    /// Returns the code-owned Kraken public feed/channel identity.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the venue-qualified Kraken identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the exact provider symbol handled by the source generation.
    pub fn provider_symbol(&self) -> &str {
        &self.provider_symbol
    }

    /// Returns the already-resolved internal instrument identity; this module never mints one.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the provider-retained price-level depth for book feeds.
    pub const fn retained_depth(&self) -> Option<KrakenDepth> {
        self.retained_depth
    }

    /// Returns the exact registry session identity.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    /// Returns the exact source connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    /// Returns the nonzero generation-local raw frame ordinal.
    pub const fn generation_frame_ordinal(&self) -> u64 {
        self.generation_frame_ordinal
    }

    /// Returns the exact frame ordinal inside the sealed event microbatch.
    pub const fn microbatch_frame_ordinal(&self) -> u16 {
        self.microbatch_frame_ordinal
    }

    /// Returns the locally assigned raw-event UUID bytes.
    pub const fn event_id(&self) -> [u8; 16] {
        self.event_id
    }

    /// Returns the application capture-generation UUID bytes.
    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    /// Returns SHA-256 of the exact provider frame.
    pub const fn raw_payload_digest(&self) -> EvidenceDigest {
        self.raw_payload_digest
    }

    /// Returns the trusted socket-boundary receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when this exact decoded disposition became available to local consumers.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }
}

#[derive(Debug)]
enum PendingDisposition {
    Market {
        observations: Vec<ProviderNormalizedObservation>,
        decoded_retained_bytes: usize,
    },
    Abstained(KrakenPublicationAbstention),
    Unavailable(KrakenPublicationUnavailable),
}

/// Non-cloneable raw-plus-typed Kraken frame awaiting the application-owned physical seal.
#[derive(Debug)]
pub struct KrakenPendingPublication {
    expectation: ProviderEventMicrobatchSealExpectation,
    seal_request: ProviderCaptureSealRequest,
    evidence: KrakenPublicationEvidence,
    disposition: PendingDisposition,
}

impl KrakenPendingPublication {
    /// Builds an explicit unavailable handoff for a captured frame whose typed decode could not be
    /// admitted. It carries no canonical row and cannot become a common event binding.
    pub fn try_unavailable(
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
        config: &KrakenConfig,
        available_at: Timestamp,
        reason: KrakenPublicationUnavailable,
    ) -> Result<Self, KrakenPublicationError> {
        Self::try_from_disposition(
            frame,
            capture,
            config,
            available_at,
            PendingDisposition::Unavailable(reason),
        )
    }

    fn try_from_disposition(
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
        config: &KrakenConfig,
        available_at: Timestamp,
        disposition: PendingDisposition,
    ) -> Result<Self, KrakenPublicationError> {
        let evidence = validate_capture(frame, &capture, config, available_at)?;
        validate_disposition(&disposition, config)?;
        let (expectation, seal_request) = capture.into_sealing_parts();
        Ok(Self {
            expectation,
            seal_request,
            evidence,
            disposition,
        })
    }

    /// Splits the one-shot handoff into its opaque continuation and application-sealed request.
    pub fn into_sealing_parts(self) -> (KrakenPublicationSealRejoin, ProviderCaptureSealRequest) {
        (
            KrakenPublicationSealRejoin {
                expectation: self.expectation,
                evidence: self.evidence,
                disposition: self.disposition,
            },
            self.seal_request,
        )
    }
}

impl KrakenPublicationDecodeOutcome {
    /// Consumes one decoded Kraken frame and its exact application capture material into the sole
    /// seal request and a provider-typed rejoin continuation.
    pub fn into_pending_publication(
        self,
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
        config: &KrakenConfig,
        available_at: Timestamp,
    ) -> Result<KrakenPendingPublication, KrakenPublicationError> {
        let (outcome, decoded_retained_bytes) = self.into_parts();
        let disposition = match outcome {
            KrakenDecodeOutcome::Market(observations) => PendingDisposition::Market {
                observations,
                decoded_retained_bytes,
            },
            KrakenDecodeOutcome::Control(control) => match control {
                KrakenPublicControl::Heartbeat => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Heartbeat)
                }
                KrakenPublicControl::Pong { .. } => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Pong)
                }
                KrakenPublicControl::Online => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Online)
                }
                KrakenPublicControl::Subscribed {
                    channel: KrakenChannel::Book(_),
                    ..
                } => PendingDisposition::Abstained(KrakenPublicationAbstention::BookSubscribed),
                KrakenPublicControl::Subscribed {
                    channel: KrakenChannel::Trades,
                    ..
                } => PendingDisposition::Abstained(KrakenPublicationAbstention::TradeSubscribed),
                KrakenPublicControl::SubscriptionRefused { .. } => PendingDisposition::Unavailable(
                    KrakenPublicationUnavailable::SubscriptionRefused,
                ),
                KrakenPublicControl::ProviderReset { .. } => PendingDisposition::Unavailable(
                    KrakenPublicationUnavailable::ResynchronizationRequired,
                ),
            },
        };
        KrakenPendingPublication::try_from_disposition(
            frame,
            capture,
            config,
            available_at,
            disposition,
        )
    }
}

/// Opaque non-cloneable continuation held across the application-owned physical seal.
#[derive(Debug)]
pub struct KrakenPublicationSealRejoin {
    expectation: ProviderEventMicrobatchSealExpectation,
    evidence: KrakenPublicationEvidence,
    disposition: PendingDisposition,
}

impl KrakenPublicationSealRejoin {
    /// Rejoins only the physical result split from this exact Kraken handoff.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<KrakenSealedPublication, KrakenPublicationError> {
        let token = self.expectation.try_rejoin(sealed)?;
        let publication = match self.disposition {
            PendingDisposition::Market {
                observations,
                decoded_retained_bytes,
            } => {
                let (native_rows, native_sidecar) = native_material(&observations, &self.evidence)?;
                KrakenSealedPublication::Market(KrakenSealedMarketPublicationMaterial {
                    token,
                    evidence: self.evidence,
                    observations,
                    decoded_retained_bytes,
                    native_rows,
                    native_sidecar,
                })
            }
            PendingDisposition::Abstained(reason) => {
                KrakenSealedPublication::Abstained(KrakenSealedNonMarketPublication {
                    token,
                    evidence: self.evidence,
                    reason: KrakenNonMarketReason::Abstained(reason),
                })
            }
            PendingDisposition::Unavailable(reason) => {
                KrakenSealedPublication::Unavailable(KrakenSealedNonMarketPublication {
                    token,
                    evidence: self.evidence,
                    reason: KrakenNonMarketReason::Unavailable(reason),
                })
            }
        };
        Ok(publication)
    }
}

/// Truthful post-seal disposition for one exact Kraken public frame.
#[derive(Debug)]
pub enum KrakenSealedPublication {
    /// Provider-normalized market material that still requires live canonical qualification.
    Market(KrakenSealedMarketPublicationMaterial),
    /// A valid control frame for which publication intentionally abstained.
    Abstained(KrakenSealedNonMarketPublication),
    /// A sealed frame with an explicit unavailable cause and no canonical row.
    Unavailable(KrakenSealedNonMarketPublication),
}

/// Typed non-market disposition retained beside its exact sealed raw receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenNonMarketReason {
    /// Valid provider control traffic intentionally omitted from market data.
    Abstained(KrakenPublicationAbstention),
    /// The frame could not produce an eligible canonical publication.
    Unavailable(KrakenPublicationUnavailable),
}

/// Non-cloneable sealed control/unavailable frame. It cannot become a market-event binding.
#[derive(Debug)]
pub struct KrakenSealedNonMarketPublication {
    token: ProviderEventMicrobatchToken,
    evidence: KrakenPublicationEvidence,
    reason: KrakenNonMarketReason,
}

impl KrakenSealedNonMarketPublication {
    /// Returns exact Kraken and source-generation value evidence.
    pub const fn evidence(&self) -> &KrakenPublicationEvidence {
        &self.evidence
    }

    /// Returns the explicit abstention or unavailable cause.
    pub const fn reason(&self) -> KrakenNonMarketReason {
        self.reason
    }

    /// Returns persisted logical and immutable physical raw evidence.
    pub const fn persisted_receipt(&self) -> &SealedProviderEventMicrobatchReceipt {
        self.token.persisted_receipt()
    }
}

/// Non-cloneable sealed Kraken observations awaiting already-qualified canonical events.
#[derive(Debug)]
pub struct KrakenSealedMarketPublicationMaterial {
    token: ProviderEventMicrobatchToken,
    evidence: KrakenPublicationEvidence,
    observations: Vec<ProviderNormalizedObservation>,
    decoded_retained_bytes: usize,
    native_rows: Box<[Bytes]>,
    native_sidecar: Bytes,
}

/// One-use committed canonical rows supplied only by the instrument-owned live runtime.
#[derive(Debug)]
pub struct KrakenQualifiedMarketPublication {
    rows: Vec<CommittedResearchMarketObservation>,
}

impl KrakenQualifiedMarketPublication {
    /// Retains committed rows in exact provider wire order without cloning qualification authority.
    pub fn try_new(
        rows: Vec<CommittedResearchMarketObservation>,
    ) -> Result<Self, KrakenPublicationError> {
        if rows.is_empty() {
            return Err(KrakenPublicationError::InvalidQualifiedPublication);
        }
        Ok(Self { rows })
    }
}

impl KrakenSealedMarketPublicationMaterial {
    /// Returns exact Kraken and source-generation value evidence.
    pub const fn evidence(&self) -> &KrakenPublicationEvidence {
        &self.evidence
    }

    /// Returns provider-normalized pre-state observations in Kraken wire order.
    pub fn observations(&self) -> &[ProviderNormalizedObservation] {
        &self.observations
    }

    /// Returns the closed common-schema implementation required once qualified canonical events
    /// are exported by the live plane.
    pub const fn native_implementation(&self) -> ProviderNativeLineageImplementation {
        ProviderNativeLineageImplementation::KrakenSpotV1
    }

    /// Returns bounded Kraken-native row semantics in exact provider observation order.
    pub fn native_rows(&self) -> &[Bytes] {
        &self.native_rows
    }

    /// Returns batch-level feed, depth, receipt, availability, and generation semantics. This is
    /// evidence only and cannot authorize publication.
    pub const fn native_sidecar(&self) -> &Bytes {
        &self.native_sidecar
    }

    /// Returns persisted logical and immutable physical raw evidence.
    pub const fn persisted_receipt(&self) -> &SealedProviderEventMicrobatchReceipt {
        self.token.persisted_receipt()
    }

    /// Returns a conservative checked memory charge for one retained sealed-frame handoff.
    ///
    /// The common decoder supplies the exact closed decoded-graph charge. Provider-native rows,
    /// sidecar bytes, and application evidence retained after sealing are added here.
    pub fn conservative_retained_bytes(&self) -> Option<usize> {
        let native_slots = size_of::<Bytes>().checked_mul(self.native_rows.len())?;
        let native_bytes = self
            .native_rows
            .iter()
            .try_fold(self.native_sidecar.len(), |total, row| {
                total.checked_add(row.len())
            })?;
        let evidence_strings = [
            self.evidence.source_id.retained_bytes(),
            self.evidence
                .metadata_revision
                .as_source_identifier()
                .retained_bytes(),
            self.evidence.dataset.retained_bytes(),
            self.evidence.stream_identity.retained_bytes(),
            self.evidence.provider_product.retained_bytes(),
            self.evidence.feed.retained_bytes(),
            self.evidence.venue.retained_bytes(),
            self.evidence.provider_symbol.capacity(),
            self.evidence.session_id.retained_bytes(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)?;
        size_of::<Self>()
            .checked_add(self.decoded_retained_bytes)?
            .checked_add(native_slots)?
            .checked_add(native_bytes)?
            .checked_add(evidence_strings)
    }

    /// Consumes sealed Kraken material and committed live rows into the common durable binding.
    ///
    /// Rows remain single-venue Kraken evidence. This join neither combines venues nor grants
    /// execution authority.
    pub fn try_publish_qualified(
        self,
        qualified: KrakenQualifiedMarketPublication,
    ) -> Result<SealedProviderPublicationBinding, KrakenPublicationError> {
        if qualified.rows.len() != self.observations.len()
            || self.native_rows.len() != self.observations.len()
        {
            return Err(KrakenPublicationError::InvalidQualifiedPublication);
        }
        let mut events = Vec::new();
        events
            .try_reserve_exact(qualified.rows.len())
            .map_err(|_| KrakenPublicationError::Allocation)?;
        let row_count = qualified.rows.len();
        for (wire_ordinal, (normalized, committed)) in
            self.observations.iter().zip(qualified.rows).enumerate()
        {
            validate_committed_row(
                normalized,
                &committed,
                &self.evidence,
                wire_ordinal,
                row_count,
            )?;
            events.push(committed.into_parts().event);
        }
        let batch = ProviderMarketEventBatch::try_new(
            self.evidence.source_id.clone(),
            self.evidence.metadata_revision.clone(),
            self.evidence.dataset.clone(),
            events,
        )?;
        let native = ProviderMarketEventNativeLineageBatch::try_new(
            ProviderNativeLineageImplementation::KrakenSpotV1,
            &batch,
            self.native_rows.into_vec(),
            Some(self.native_sidecar),
        )?;
        let mut row_frame_ordinals = Vec::new();
        row_frame_ordinals
            .try_reserve_exact(batch.events().len())
            .map_err(|_| KrakenPublicationError::Allocation)?;
        row_frame_ordinals.resize(batch.events().len(), 0);
        let binding = SealedProviderEventMicrobatchBinding::try_new(
            self.token,
            batch,
            native,
            row_frame_ordinals,
        )?;
        binding.validate()?;
        Ok(binding.into())
    }

    /// Consumes a sealed market frame into an explicit unavailable state without minting a
    /// canonical row.
    pub fn into_unavailable(
        self,
        reason: KrakenPublicationUnavailable,
    ) -> KrakenSealedNonMarketPublication {
        KrakenSealedNonMarketPublication {
            token: self.token,
            evidence: self.evidence,
            reason: KrakenNonMarketReason::Unavailable(reason),
        }
    }
}

fn validate_committed_row(
    normalized: &ProviderNormalizedObservation,
    committed: &CommittedResearchMarketObservation,
    evidence: &KrakenPublicationEvidence,
    wire_ordinal: usize,
    row_count: usize,
) -> Result<(), KrakenPublicationError> {
    let event = committed.event();
    let provenance = event_provenance(event);
    let binding = committed.qualification().binding();
    let source_timestamp = observation_timestamp(normalized);
    if committed.qualification().recorded_quality() != DataQuality::DirectUnverified
        || provenance.recorded_quality() != DataQuality::DirectUnverified
        || provenance.execution_eligibility()
            != market_squawk_domain::ExecutionEligibility::Ineligible
        || committed.wire_ordinal() != wire_ordinal
        || committed.row_count() != row_count
        || provenance.source_id() != &evidence.source_id
        || provenance.connection_generation() != evidence.connection_generation
        || binding.source_id() != &evidence.source_id
        || binding.session_id() != &evidence.session_id
        || binding.metadata_revision() != &evidence.metadata_revision
        || binding.provider_product().as_source_identifier() != &evidence.provider_product
        || binding.provider_channel().as_source_identifier() != &evidence.feed
        || binding.venue_id() != &evidence.venue
        || binding.instrument_id() != evidence.instrument_id
        || binding.connection_generation() != evidence.connection_generation
        || committed.connection_generation() != evidence.connection_generation
        || committed
            .frame_id()
            .map(market_squawk_sources::FrameId::get)
            != Some(evidence.generation_frame_ordinal)
        || binding.payload_digest() != evidence.raw_payload_digest
        || binding.source_identifier() != normalized.source_identifier()
        || binding.event_class() != normalized.event_class()
        || provenance.source_identifier() != normalized.source_identifier()
        || provenance.source_timestamp() != source_timestamp
        || provenance.received_at() != evidence.received_at
        || provenance.available_at() < evidence.available_at
        || provenance.ingested_at() < provenance.available_at()
        || normalized.venue() != &evidence.venue
        || normalized.instrument() != evidence.instrument_id
        || event_class(event) != normalized.event_class()
        || event_depth(event) != normalized.depth()
        || event_sequence(event).is_some()
        || !stable_trade_identity_matches(normalized, committed)
    {
        return Err(KrakenPublicationError::InvalidQualifiedPublication);
    }
    Ok(())
}

fn stable_trade_identity_matches(
    normalized: &ProviderNormalizedObservation,
    committed: &CommittedResearchMarketObservation,
) -> bool {
    match normalized.payload() {
        ProviderObservationPayload::Trade { trade_id, .. } => {
            committed.stable_trade_id() == Some(trade_id)
        }
        ProviderObservationPayload::BookSnapshot(_) | ProviderObservationPayload::BookDelta(_) => {
            committed.stable_trade_id().is_none()
        }
        _ => false,
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

fn event_class(event: &MarketEvent) -> LiveEventClass {
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

fn event_sequence(event: &MarketEvent) -> Option<market_squawk_domain::SequenceNumber> {
    match event {
        MarketEvent::BookSnapshot(value) => value.sequence(),
        MarketEvent::BookDelta(value) => value.sequence(),
        _ => None,
    }
}

fn validate_capture(
    frame: &ValidatedRawMarketFrame<'_>,
    capture: &ProviderEventMicrobatchMaterial,
    config: &KrakenConfig,
    available_at: Timestamp,
) -> Result<KrakenPublicationEvidence, KrakenPublicationError> {
    let raw = frame.frame();
    let metadata = config.metadata();
    let receipt = capture.receipt();
    let raw_receipt = receipt
        .frames()
        .first()
        .ok_or(KrakenPublicationError::InvalidCapture)?;
    let raw_record = capture
        .records()
        .first()
        .ok_or(KrakenPublicationError::InvalidCapture)?;
    let live = metadata
        .coverage()
        .live()
        .ok_or(KrakenPublicationError::InvalidMarketEvidence)?;
    if capture.receipt().frames().len() != 1
        || capture.records().len() != 1
        || raw.transport() != TransportFrameKind::Text
        || raw.source_id() != metadata.source_id()
        || raw.metadata_revision() != metadata.revision()
        || receipt.source_id() != raw.source_id()
        || receipt.metadata_revision() != raw.metadata_revision()
        || raw_receipt.ordinal() != 0
        || raw_receipt.received_at() != raw.received_at()
        || raw_receipt.exchange_at().is_some()
        || raw_receipt.source_sequence().is_some()
        || raw_record.payload() != raw.payload()
        || available_at < raw.received_at()
        || live.provider_product().as_source_identifier().as_str() != "kraken-spot"
        || !matches!(
            live.provider_channel().as_source_identifier().as_str(),
            "book-v2" | "trade-v2"
        )
    {
        return Err(KrakenPublicationError::InvalidCapture);
    }
    let (retained_depth, expected_feed) = match config.channel() {
        KrakenChannel::Book(depth) => (Some(depth), "book-v2"),
        KrakenChannel::Trades => (None, "trade-v2"),
    };
    if live.provider_channel().as_source_identifier().as_str() != expected_feed {
        return Err(KrakenPublicationError::InvalidMarketEvidence);
    }
    Ok(KrakenPublicationEvidence {
        source_id: raw.source_id().clone(),
        metadata_revision: raw.metadata_revision().clone(),
        dataset: receipt.dataset().clone(),
        stream_identity: receipt.stream_identity().clone(),
        provider_product: live.provider_product().as_source_identifier().clone(),
        feed: live.provider_channel().as_source_identifier().clone(),
        venue: VenueId::try_from(KRAKEN_VENUE)
            .map_err(|_| KrakenPublicationError::InvalidMarketEvidence)?,
        provider_symbol: config.symbol().to_owned(),
        instrument_id: config.instrument(),
        retained_depth,
        session_id: raw.session_id().as_source_identifier().clone(),
        connection_generation: raw.connection_generation(),
        generation_frame_ordinal: raw.frame_id().get(),
        microbatch_frame_ordinal: raw_receipt.ordinal(),
        event_id: raw_receipt.event_id(),
        connection_id: raw_receipt.connection_id(),
        raw_payload_digest: raw_receipt.payload_digest(),
        received_at: raw.received_at(),
        available_at,
    })
}

fn validate_disposition(
    disposition: &PendingDisposition,
    config: &KrakenConfig,
) -> Result<(), KrakenPublicationError> {
    let PendingDisposition::Market { observations, .. } = disposition else {
        return Ok(());
    };
    if observations.is_empty() {
        return Err(KrakenPublicationError::InvalidMarketEvidence);
    }
    for observation in observations {
        if observation.venue().as_str() != KRAKEN_VENUE
            || observation.instrument() != config.instrument()
        {
            return Err(KrakenPublicationError::InvalidMarketEvidence);
        }
        match (config.channel(), observation.payload()) {
            (KrakenChannel::Book(_), ProviderObservationPayload::BookSnapshot(value))
                if value.depth() == MarketDepth::PriceLevel => {}
            (KrakenChannel::Book(_), ProviderObservationPayload::BookDelta(value))
                if value.depth() == MarketDepth::PriceLevel => {}
            (KrakenChannel::Trades, ProviderObservationPayload::Trade { .. }) => {}
            _ => return Err(KrakenPublicationError::InvalidMarketEvidence),
        }
    }
    Ok(())
}

fn observation_timestamp(observation: &ProviderNormalizedObservation) -> Option<Timestamp> {
    match observation.timestamp() {
        ProviderTimestampEvidence::Provided { value, .. } => Some(*value),
        ProviderTimestampEvidence::AuthoritativelyAbsent(_) => None,
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct KrakenNativeRowV1<'a> {
    version: u16,
    provider: &'static str,
    provider_product: &'a str,
    feed: &'a str,
    provider_symbol: &'a str,
    venue: &'a str,
    instrument_id: InstrumentId,
    generation_frame_ordinal: u64,
    microbatch_frame_ordinal: u16,
    provider_row_ordinal: u32,
    raw_payload_digest: EvidenceDigest,
    source_identifier: &'a str,
    source_timestamp: Option<Timestamp>,
    event_class: LiveEventClass,
    depth: Option<MarketDepth>,
    retained_depth: Option<usize>,
    sequence: KrakenNativeSequenceV1<'a>,
    snapshot: KrakenNativeSnapshotV1<'a>,
    checksum: KrakenNativeChecksumV1<'a>,
    payload: KrakenNativePayloadV1<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum KrakenNativeSequenceV1<'a> {
    Unsupported { rule: &'a str, rule_version: u32 },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum KrakenNativeSnapshotV1<'a> {
    Initializing { provider_reference: Option<&'a str> },
    Delta { provider_reference: Option<&'a str> },
    NotApplicable { rule: &'a str, rule_version: u32 },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum KrakenNativeChecksumV1<'a> {
    Provided {
        value: &'a str,
        rule: &'a str,
        rule_version: u32,
    },
    Unsupported {
        rule: &'a str,
        rule_version: u32,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum KrakenNativePayloadV1<'a> {
    Trade {
        trade_id: &'a str,
        price: &'a str,
        quantity: &'a str,
        aggressor_side: market_squawk_domain::AggressorSide,
        aggressor_provider_code: Option<&'a str>,
        aggressor_rule: &'a str,
        aggressor_rule_version: u32,
        taker_order_type: Option<market_squawk_domain::TradeTakerOrderType>,
    },
    BookSnapshot {
        bid_count: usize,
        ask_count: usize,
    },
    BookDelta {
        change_count: usize,
    },
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct KrakenNativeBatchSidecarV1<'a> {
    version: u16,
    family: &'static str,
    source_id: &'a str,
    metadata_revision: &'a str,
    dataset: &'a str,
    stream_identity: &'a str,
    provider_product: &'a str,
    feed: &'a str,
    provider_symbol: &'a str,
    venue: &'a str,
    retained_depth: Option<usize>,
    session_id: &'a str,
    connection_generation: u64,
    generation_frame_ordinal: u64,
    microbatch_frame_ordinal: u16,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    raw_payload_digest: EvidenceDigest,
    received_at: Timestamp,
    available_at: Timestamp,
    provider_row_count: usize,
    quality_ceiling: DataQuality,
    execution_authority: &'static str,
}

fn native_material(
    observations: &[ProviderNormalizedObservation],
    evidence: &KrakenPublicationEvidence,
) -> Result<(Box<[Bytes]>, Bytes), KrakenPublicationError> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(observations.len())
        .map_err(|_| KrakenPublicationError::NativeEncoding)?;
    for (ordinal, observation) in observations.iter().enumerate() {
        let row = KrakenNativeRowV1 {
            version: 1,
            provider: KRAKEN_VENUE,
            provider_product: evidence.provider_product.as_str(),
            feed: evidence.feed.as_str(),
            provider_symbol: &evidence.provider_symbol,
            venue: evidence.venue.as_str(),
            instrument_id: evidence.instrument_id,
            generation_frame_ordinal: evidence.generation_frame_ordinal,
            microbatch_frame_ordinal: evidence.microbatch_frame_ordinal,
            provider_row_ordinal: u32::try_from(ordinal)
                .map_err(|_| KrakenPublicationError::NativeEncoding)?,
            raw_payload_digest: evidence.raw_payload_digest,
            source_identifier: observation.source_identifier().as_str(),
            source_timestamp: observation_timestamp(observation),
            event_class: observation.event_class(),
            depth: observation.depth(),
            retained_depth: evidence.retained_depth.map(KrakenDepth::get),
            sequence: native_sequence(observation)?,
            snapshot: native_snapshot(observation),
            checksum: native_checksum(observation),
            payload: native_payload(observation)?,
        };
        rows.push(Bytes::from(
            serde_json::to_vec(&row).map_err(|_| KrakenPublicationError::NativeEncoding)?,
        ));
    }
    let sidecar = KrakenNativeBatchSidecarV1 {
        version: 1,
        family: "kraken.spot.public-market-event",
        source_id: evidence.source_id.as_str(),
        metadata_revision: evidence.metadata_revision.as_source_identifier().as_str(),
        dataset: evidence.dataset.as_str(),
        stream_identity: evidence.stream_identity.as_str(),
        provider_product: evidence.provider_product.as_str(),
        feed: evidence.feed.as_str(),
        provider_symbol: &evidence.provider_symbol,
        venue: evidence.venue.as_str(),
        retained_depth: evidence.retained_depth.map(KrakenDepth::get),
        session_id: evidence.session_id.as_str(),
        connection_generation: evidence.connection_generation.get(),
        generation_frame_ordinal: evidence.generation_frame_ordinal,
        microbatch_frame_ordinal: evidence.microbatch_frame_ordinal,
        event_id: evidence.event_id,
        connection_id: evidence.connection_id,
        raw_payload_digest: evidence.raw_payload_digest,
        received_at: evidence.received_at,
        available_at: evidence.available_at,
        provider_row_count: observations.len(),
        quality_ceiling: DataQuality::DirectUnverified,
        execution_authority: "none",
    };
    let sidecar = Bytes::from(
        serde_json::to_vec(&sidecar).map_err(|_| KrakenPublicationError::NativeEncoding)?,
    );
    Ok((rows.into_boxed_slice(), sidecar))
}

fn native_sequence(
    observation: &ProviderNormalizedObservation,
) -> Result<KrakenNativeSequenceV1<'_>, KrakenPublicationError> {
    match observation.sequence() {
        ProviderSequenceEvidence::Unsupported { rule } => Ok(KrakenNativeSequenceV1::Unsupported {
            rule: rule.provider_rule().as_str(),
            rule_version: rule.version().get(),
        }),
        ProviderSequenceEvidence::Provided { .. } => {
            Err(KrakenPublicationError::InvalidMarketEvidence)
        }
    }
}

fn native_snapshot(observation: &ProviderNormalizedObservation) -> KrakenNativeSnapshotV1<'_> {
    match observation.snapshot() {
        ProviderSnapshotEvidence::InitializingSnapshot { provider_reference } => {
            KrakenNativeSnapshotV1::Initializing {
                provider_reference: provider_reference.as_ref().map(SourceIdentifier::as_str),
            }
        }
        ProviderSnapshotEvidence::Delta {
            provider_snapshot_reference,
        } => KrakenNativeSnapshotV1::Delta {
            provider_reference: provider_snapshot_reference
                .as_ref()
                .map(SourceIdentifier::as_str),
        },
        ProviderSnapshotEvidence::NotApplicable(rule) => KrakenNativeSnapshotV1::NotApplicable {
            rule: rule.provider_rule().as_str(),
            rule_version: rule.version().get(),
        },
    }
}

fn native_checksum(observation: &ProviderNormalizedObservation) -> KrakenNativeChecksumV1<'_> {
    match observation.checksum() {
        market_squawk_sources::ProviderChecksumEvidence::Provided { value, rule } => {
            KrakenNativeChecksumV1::Provided {
                value: value.as_str(),
                rule: rule.provider_rule().as_str(),
                rule_version: rule.version().get(),
            }
        }
        market_squawk_sources::ProviderChecksumEvidence::Unsupported { rule } => {
            KrakenNativeChecksumV1::Unsupported {
                rule: rule.provider_rule().as_str(),
                rule_version: rule.version().get(),
            }
        }
    }
}

fn native_payload(
    observation: &ProviderNormalizedObservation,
) -> Result<KrakenNativePayloadV1<'_>, KrakenPublicationError> {
    match observation.payload() {
        ProviderObservationPayload::Trade {
            trade_id,
            price,
            quantity,
            aggressor,
            taker_order_type,
        } => Ok(KrakenNativePayloadV1::Trade {
            trade_id: trade_id.as_str(),
            price: price.value().as_str(),
            quantity: quantity.value().as_str(),
            aggressor_side: aggressor.side(),
            aggressor_provider_code: aggressor.provider_code().map(SourceIdentifier::as_str),
            aggressor_rule: aggressor.rule().provider_rule().as_str(),
            aggressor_rule_version: aggressor.rule().version().get(),
            taker_order_type: *taker_order_type,
        }),
        ProviderObservationPayload::BookSnapshot(value) => {
            Ok(KrakenNativePayloadV1::BookSnapshot {
                bid_count: value.bids().len(),
                ask_count: value.asks().len(),
            })
        }
        ProviderObservationPayload::BookDelta(value) => Ok(KrakenNativePayloadV1::BookDelta {
            change_count: value.changes().len(),
        }),
        ProviderObservationPayload::Quote { .. }
        | ProviderObservationPayload::Auction { .. }
        | ProviderObservationPayload::TradingHalt { .. }
        | ProviderObservationPayload::InstrumentStatus { .. }
        | ProviderObservationPayload::CorporateAction { .. } => {
            Err(KrakenPublicationError::InvalidMarketEvidence)
        }
    }
}

/// Closed Kraken publication bridge failure.
#[derive(Debug, Error)]
pub enum KrakenPublicationError {
    /// Raw frame, logical receipt, or immutable capture material did not match exactly.
    #[error("Kraken raw capture evidence is inconsistent")]
    InvalidCapture,
    /// Venue, feed, depth, instrument, or decoded family evidence was inconsistent.
    #[error("Kraken market evidence is inconsistent")]
    InvalidMarketEvidence,
    /// Bounded provider-native semantics could not be encoded.
    #[error("Kraken native lineage encoding failed")]
    NativeEncoding,
    /// Committed live rows did not match the sealed Kraken frame exactly.
    #[error("Kraken committed canonical publication is inconsistent")]
    InvalidQualifiedPublication,
    /// A bounded publication allocation failed.
    #[error("Kraken canonical publication allocation failed")]
    Allocation,
    /// The shared consuming raw/publication binding rejected the handoff.
    #[error("Kraken common publication binding failed")]
    Capture(#[from] ProviderCaptureError),
}
