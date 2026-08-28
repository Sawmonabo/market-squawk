//! Seal-first Kraken Spot public market-data publication material.
//!
//! This module deliberately stops below canonical live qualification. Kraken decoding produces
//! provider-normalized observations, while instrument-owned state in `market-squawk-live` owns
//! checksum/state qualification and canonical market-event construction. Until that live plane
//! exports recorded `DirectUnverified` events, this module retains narrow sealed qualification
//! material and never constructs or accepts caller-made canonical events.

use bytes::Bytes;
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, EvidenceDigest, InstrumentId, LiveEventClass, MarketDepth,
    MetadataRevision, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ProviderCaptureError, ProviderCaptureSealRequest, ProviderEventMicrobatchMaterial,
    ProviderEventMicrobatchSealExpectation, ProviderEventMicrobatchToken,
    ProviderNativeLineageImplementation, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderTimestampEvidence,
    SealedProviderCaptureMaterial, SealedProviderEventMicrobatchReceipt, SourceMetadataProvider,
    TransportFrameKind, ValidatedRawMarketFrame,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    KrakenChannel, KrakenConfig, KrakenControl, KrakenDecodeOutcome, KrakenDepth,
    KrakenSubscription,
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
    Market(Vec<ProviderNormalizedObservation>),
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

impl KrakenDecodeOutcome {
    /// Consumes one decoded Kraken frame and its exact application capture material into the sole
    /// seal request and a provider-typed rejoin continuation.
    pub fn into_pending_publication(
        self,
        frame: &ValidatedRawMarketFrame<'_>,
        capture: ProviderEventMicrobatchMaterial,
        config: &KrakenConfig,
        available_at: Timestamp,
    ) -> Result<KrakenPendingPublication, KrakenPublicationError> {
        let disposition = match self {
            Self::Market(observations) => PendingDisposition::Market(observations),
            Self::Control(control) => match control {
                KrakenControl::Heartbeat => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Heartbeat)
                }
                KrakenControl::Pong => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Pong)
                }
                KrakenControl::Online => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::Online)
                }
                KrakenControl::Subscribed(KrakenSubscription::Book) => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::BookSubscribed)
                }
                KrakenControl::Subscribed(KrakenSubscription::Trade) => {
                    PendingDisposition::Abstained(KrakenPublicationAbstention::TradeSubscribed)
                }
                KrakenControl::SubscriptionRefused => PendingDisposition::Unavailable(
                    KrakenPublicationUnavailable::SubscriptionRefused,
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
            PendingDisposition::Market(observations) => {
                let (native_rows, native_sidecar) = native_material(&observations, &self.evidence)?;
                KrakenSealedPublication::Market(KrakenSealedMarketPublicationMaterial {
                    token,
                    evidence: self.evidence,
                    observations,
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
    native_rows: Box<[Bytes]>,
    native_sidecar: Bytes,
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

    /// Consumes a sealed market frame into an explicit unavailable state without minting a
    /// canonical row. This is the truthful current path while live canonical export is absent.
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
    let PendingDisposition::Market(observations) = disposition else {
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
    /// The shared consuming raw/publication binding rejected the handoff.
    #[error("Kraken common publication binding failed")]
    Capture(#[from] ProviderCaptureError),
}
