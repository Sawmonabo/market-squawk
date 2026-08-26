//! Authenticated Coinbase Direct snapshot/bootstrap and live transport.
//!
//! This is intentionally not a [`market_squawk_sources::LiveMarketSource`]. Direct bootstrap owns
//! a REST product response, a segmented level-3 snapshot, a bounded replay queue, and one
//! instrument book across the atomic snapshot-to-live handoff. The generic raw-only source
//! contract cannot express that ownership without hiding synchronization state.

mod http;

use std::cmp::Ordering;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _, future::BoxFuture};
use market_squawk_domain::{
    CapturePayload, ConnectionGeneration, DataQuality, DigestAlgorithm, EvidenceDigest,
    ExactPayloadEvidence, InstrumentExecutionTerms, LiveEventClass, MarketDepth, PriceTicks,
    QuantityLots, RawCaptureFrameView as _, SequenceNumber, SnapshotApplicability,
    SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetReservationDecision, ChecksumValidationProfile, ControlFrameKind, DecodedControlFrame,
    DecodedProviderBatch, DecoderEvidence, DirectOrderBook, DirectOrderBookError,
    DirectPublishedBook, DirectPublishedLevel, HttpCaptureMethod, LiveSourceGeneration,
    NetworkAccessPolicy, ProviderBookChange, ProviderBookLevel, ProviderBookSide,
    ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
    ProviderObservationPayload, ProviderOrderEvent, ProviderOrderRecord, ProviderPrice,
    ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, RawMarketSink, SegmentedHttpCaptureError,
    SegmentedHttpResponseCapture, SegmentedHttpResponseReceipt, SequenceValidationProfile,
    SharedProviderBudget, SinkError, SourceError, SourceMetadata, SourceMetadataProvider,
    SourceProtocolProfile, TlsProviderCapability, TransportFrameKind, apply_http_retry_after,
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig};
use tokio_tungstenite::{WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use self::http::{
    CoinbaseDirectHttpRequest, CoinbaseDirectHttpResponse, CoinbaseDirectHttpTransport,
    CoinbaseDirectHttpTransportError, ReqwestCoinbaseDirectHttpTransport,
};
use crate::direct::CoinbaseDirectSnapshotCoordinates;
use crate::market_handoff::{
    CoinbaseDirectInitialMarketLineage, CoinbaseDirectReplayFrame, CoinbaseMarketHandoffInput,
    CoinbaseMarketRawLineage, direct_request_set_digest, exact_digest,
};
use crate::{
    CoinbaseConfigError, CoinbaseDirectConfig, CoinbaseDirectDecodeError,
    CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder, CoinbaseDirectNonBookEvent,
    CoinbaseDirectProductError, CoinbaseDirectProductEvidence, CoinbaseDirectSigningCapability,
    CoinbaseDirectSigningError, CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError,
    CoinbaseDirectTradeEvidence, CoinbaseMarketChannel, CoinbaseMarketContinuity,
    CoinbaseMarketFeed, CoinbaseMarketHandoff, CoinbaseMarketHandoffError,
    CoinbaseSignedSubscription,
};

/// Borrowed, read-only healthy book evidence emitted by one Direct owner.
///
/// This update is unqualified provider evidence. It does not mint `DirectVerified`, canonical
/// market events, order authority, or execution eligibility.
#[derive(Debug)]
struct CoinbaseDirectBookUpdate<'a> {
    config: &'a CoinbaseDirectConfig,
    sequence: SequenceNumber,
    source_timestamp: Timestamp,
    request_set_digest: EvidenceDigest,
    subscription_request_digest: EvidenceDigest,
    subscription_evidence: &'a ExactPayloadEvidence,
    snapshot_capture: SegmentedHttpResponseCapture,
    replay_frames: Vec<SequencedFrameEvidence>,
    snapshot_coordinates: CoinbaseDirectSnapshotCoordinates,
    previous_published_sequence: Option<SequenceNumber>,
    previous: Option<&'a PublishedBookState>,
    current: &'a PublishedBookState,
    publication: CoinbaseDirectPublicationKind,
}

impl<'a> CoinbaseDirectBookUpdate<'a> {
    fn try_market_handoff(self) -> Result<CoinbaseMarketHandoff, CoinbaseDirectPublicationError> {
        let typed_batch = self.try_typed_batch()?;
        let continuity = match self.previous_published_sequence {
            None if self.publication == CoinbaseDirectPublicationKind::Snapshot => {
                CoinbaseMarketContinuity::SnapshotContiguous {
                    snapshot: self.snapshot_coordinates.sequence,
                    terminal: self.sequence,
                }
            }
            Some(_) => return Err(CoinbaseDirectPublicationError::SnapshotClaimRequired),
            _ => return Err(CoinbaseDirectPublicationError::EvidenceMismatch),
        };
        let mut replay = Vec::new();
        replay
            .try_reserve_exact(self.replay_frames.len())
            .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?;
        if replay.capacity()
            > self
                .config
                .limits()
                .checked_replay_container_slots()
                .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?
        {
            return Err(CoinbaseDirectPublicationError::Allocation);
        }
        for frame in self.replay_frames {
            replay.push(
                CoinbaseDirectReplayFrame::try_new(
                    frame.event,
                    frame.raw_payload,
                    frame.native_trade,
                )
                .map_err(CoinbaseDirectPublicationError::Handoff)?,
            );
        }
        let raw_lineage = CoinbaseMarketRawLineage::DirectInitial(
            CoinbaseDirectInitialMarketLineage::try_new(self.snapshot_capture, replay)
                .map_err(CoinbaseDirectPublicationError::Handoff)?,
        );
        CoinbaseMarketHandoff::try_new(
            CoinbaseMarketHandoffInput {
                feed: CoinbaseMarketFeed::ExchangeDirectFull,
                channel: CoinbaseMarketChannel::Full,
                native_input_depth: Some(MarketDepth::OrderLevel),
                product: self.config.product().clone(),
                configured_instrument: self.config.instrument(),
                venue: self.config.venue().clone(),
                request_set_digest: self.request_set_digest,
                subscription_digest: self.subscription_request_digest,
                subscription_acknowledgement: Some(self.subscription_evidence.clone()),
                continuity,
                provider_published_at: self.source_timestamp,
                snapshot_provider_at: Some(self.snapshot_coordinates.timestamp),
            },
            raw_lineage,
            typed_batch,
        )
        .map_err(CoinbaseDirectPublicationError::Handoff)
    }

    fn try_typed_batch(&self) -> Result<DecodedProviderBatch, CoinbaseDirectPublicationError> {
        let terminal = self
            .replay_frames
            .last()
            .ok_or(CoinbaseDirectPublicationError::EvidenceMismatch)?;
        let decoder_evidence = terminal.event.evidence();
        let snapshot_receipt = self.snapshot_capture.receipt();
        decoder_evidence
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationError::StaleAuthority)?;
        snapshot_receipt
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationError::StaleAuthority)?;
        if !decoder_evidence
            .binding()
            .shares_allocation_with(snapshot_receipt.binding())
            || !decoder_evidence
                .currentness_lease()
                .shares_authority_with(snapshot_receipt.currentness_lease())
            || decoder_evidence.binding().source_id() != self.config.metadata().source_id()
            || decoder_evidence.binding().metadata_revision() != self.config.metadata().revision()
            || snapshot_receipt.connection_generation()
                != decoder_evidence.binding().connection_generation()
        {
            return Err(CoinbaseDirectPublicationError::EvidenceMismatch);
        }
        let SourceProtocolProfile::Live(protocol) = self.config.metadata().protocol_profile()
        else {
            return Err(CoinbaseDirectPublicationError::ProfileMismatch);
        };
        let sequence_rule = match protocol.sequence() {
            SequenceValidationProfile::Provided { rule, .. } => rule.clone(),
            SequenceValidationProfile::Unsupported { .. } => {
                return Err(CoinbaseDirectPublicationError::ProfileMismatch);
            }
        };
        let checksum_rule = match protocol.checksum() {
            ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
            ChecksumValidationProfile::Provided { .. } => {
                return Err(CoinbaseDirectPublicationError::ProfileMismatch);
            }
        };
        let (event_class, depth) = match self.publication {
            CoinbaseDirectPublicationKind::Snapshot => {
                (LiveEventClass::BookSnapshot, Some(MarketDepth::PriceLevel))
            }
            CoinbaseDirectPublicationKind::Delta => {
                (LiveEventClass::BookDelta, Some(MarketDepth::PriceLevel))
            }
            CoinbaseDirectPublicationKind::Quote => (LiveEventClass::Quote, None),
        };
        let coverage_rule = self
            .config
            .metadata()
            .coverage()
            .live()
            .and_then(|coverage| coverage.rule_for(event_class, depth))
            .ok_or(CoinbaseDirectPublicationError::ProfileMismatch)?;
        let snapshot = match (self.publication, coverage_rule.snapshot_applicability()) {
            (CoinbaseDirectPublicationKind::Snapshot, SnapshotApplicability::Required)
                if self.previous.is_none() =>
            {
                ProviderSnapshotEvidence::InitializingSnapshot {
                    provider_reference: None,
                }
            }
            (CoinbaseDirectPublicationKind::Delta, SnapshotApplicability::Required)
                if self.previous.is_some() =>
            {
                ProviderSnapshotEvidence::Delta {
                    provider_snapshot_reference: None,
                }
            }
            (
                CoinbaseDirectPublicationKind::Quote,
                SnapshotApplicability::NotApplicable { metadata_rule },
            ) if self.previous.is_some() => {
                ProviderSnapshotEvidence::NotApplicable(metadata_rule.clone())
            }
            _ => return Err(CoinbaseDirectPublicationError::ProfileMismatch),
        };
        let terms = self.config.execution_terms();
        match (self.publication, self.previous) {
            (CoinbaseDirectPublicationKind::Snapshot, None) => {}
            (CoinbaseDirectPublicationKind::Delta, Some(previous)) if previous != self.current => {}
            (CoinbaseDirectPublicationKind::Quote, Some(previous)) if previous == self.current => {}
            _ => return Err(CoinbaseDirectPublicationError::EvidenceMismatch),
        }
        let payload = match self.publication {
            CoinbaseDirectPublicationKind::Snapshot => ProviderObservationPayload::book_snapshot(
                MarketDepth::PriceLevel,
                provider_levels(&self.current.bids, terms)?,
                provider_levels(&self.current.asks, terms)?,
            ),
            CoinbaseDirectPublicationKind::Delta => {
                let previous = self
                    .previous
                    .ok_or(CoinbaseDirectPublicationError::ProfileMismatch)?;
                ProviderObservationPayload::book_delta(
                    MarketDepth::PriceLevel,
                    provider_changes(previous, self.current, terms)?,
                )
            }
            CoinbaseDirectPublicationKind::Quote => {
                let bid = self
                    .current
                    .bids
                    .first()
                    .copied()
                    .map(|level| provider_level(level, terms))
                    .transpose()?;
                let ask = self
                    .current
                    .asks
                    .first()
                    .copied()
                    .map(|level| provider_level(level, terms))
                    .transpose()?;
                ProviderObservationPayload::quote(bid, ask)
            }
        }
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
        let source_identifier = direct_book_identifier(self.sequence, snapshot_receipt)?;
        let observation = ProviderNormalizedObservation::try_new(
            source_identifier,
            self.config.venue().clone(),
            self.config.instrument(),
            ProviderTimestampEvidence::Provided {
                value: self.source_timestamp,
                rule: protocol.timestamp_rule().clone(),
            },
            ProviderSequenceEvidence::Provided {
                value: self.sequence,
                rule: sequence_rule,
            },
            snapshot,
            ProviderChecksumEvidence::Unsupported {
                rule: checksum_rule,
            },
            payload,
        )
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
        DecodedProviderBatch::try_new(decoder_evidence.clone(), vec![observation])
            .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)
    }
}

/// Exact order-level bootstrap or sequenced successor emitted by one Direct generation.
///
/// The payload retains Coinbase order identities and remains unqualified provider evidence. The
/// central live authority must validate its current generation, source receipt, coverage, timing,
/// and sequence before constructing any product read model or execution-eligible state.
#[derive(Clone, Copy, Debug)]
pub struct CoinbaseDirectOrderLevelUpdate<'a> {
    config: &'a CoinbaseDirectConfig,
    subscription_evidence: &'a ExactPayloadEvidence,
    snapshot_receipt: &'a SegmentedHttpResponseReceipt,
    decoder_evidence: &'a DecoderEvidence,
    payload: CoinbaseDirectOrderLevelPayload<'a>,
}

impl<'a> CoinbaseDirectOrderLevelUpdate<'a> {
    /// Returns the explicit order-level depth contract.
    pub const fn market_depth(self) -> MarketDepth {
        MarketDepth::OrderLevel
    }

    /// Returns the immutable configured source metadata.
    pub const fn metadata(self) -> &'a SourceMetadata {
        self.config.metadata()
    }

    /// Returns the exact mapped provider product.
    pub const fn product(self) -> &'a market_squawk_domain::ProviderProduct {
        self.config.product()
    }

    /// Returns the exact connection generation shared by snapshot and current frame.
    pub fn connection_generation(self) -> ConnectionGeneration {
        self.snapshot_receipt.connection_generation()
    }

    /// Returns exact authenticated-subscription acknowledgement evidence.
    pub const fn subscription_evidence(self) -> &'a ExactPayloadEvidence {
        self.subscription_evidence
    }

    /// Returns the complete level-3 REST snapshot receipt anchoring this generation.
    pub const fn snapshot_receipt(self) -> &'a SegmentedHttpResponseReceipt {
        self.snapshot_receipt
    }

    /// Returns exact raw-frame and decoder evidence backing the current cursor.
    pub const fn decoder_evidence(self) -> &'a DecoderEvidence {
        self.decoder_evidence
    }

    /// Returns the order-identity-preserving payload.
    pub const fn payload(self) -> CoinbaseDirectOrderLevelPayload<'a> {
        self.payload
    }

    /// Revalidates the complete borrowed order-level evidence before downstream admission.
    ///
    /// # Errors
    ///
    /// Rejects revoked or cross-generation evidence, a mislabeled quality/depth profile, an
    /// incomplete snapshot, or a non-contiguous replay suffix.
    pub fn validate_current(self) -> Result<(), CoinbaseDirectOrderLevelPublicationError> {
        self.decoder_evidence
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectOrderLevelPublicationError::StaleAuthority)?;
        self.snapshot_receipt
            .currentness_lease()
            .validate_current()
            .map_err(|_error| CoinbaseDirectOrderLevelPublicationError::StaleAuthority)?;
        if self.config.publication_depth() != MarketDepth::OrderLevel
            || self.config.metadata().quality_ceiling() != DataQuality::DirectUnverified
            || !self
                .decoder_evidence
                .binding()
                .shares_allocation_with(self.snapshot_receipt.binding())
            || !self
                .decoder_evidence
                .currentness_lease()
                .shares_authority_with(self.snapshot_receipt.currentness_lease())
            || self.decoder_evidence.binding().source_id() != self.config.metadata().source_id()
            || self.decoder_evidence.binding().metadata_revision()
                != self.config.metadata().revision()
            || self.snapshot_receipt.connection_generation()
                != self.decoder_evidence.binding().connection_generation()
        {
            return Err(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch);
        }
        let coverage = self
            .config
            .metadata()
            .coverage()
            .live()
            .ok_or(CoinbaseDirectOrderLevelPublicationError::ProfileMismatch)?;
        for event_class in [LiveEventClass::BookSnapshot, LiveEventClass::BookDelta] {
            if coverage
                .rule_for(event_class, Some(MarketDepth::OrderLevel))
                .is_none()
            {
                return Err(CoinbaseDirectOrderLevelPublicationError::ProfileMismatch);
            }
        }
        match self.payload {
            CoinbaseDirectOrderLevelPayload::Snapshot {
                snapshot_sequence,
                snapshot_timestamp: _,
                orders,
                replay,
            } => {
                if orders.is_empty()
                    || orders.len() > self.config.limits().book().max_orders()
                    || replay.len() > self.config.limits().book().max_queue_events()
                {
                    return Err(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch);
                }
                let mut cursor = snapshot_sequence;
                let mut replay_bytes = 0_usize;
                for event in replay {
                    validate_order_level_event(self.config, event)?;
                    validate_sequenced_progression(cursor, event.sequence()).map_err(|_error| {
                        CoinbaseDirectOrderLevelPublicationError::SequenceMismatch
                    })?;
                    cursor = event.sequence();
                    replay_bytes = replay_bytes
                        .checked_add(event.wire_bytes())
                        .ok_or(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch)?;
                }
                if replay_bytes > self.config.limits().book().max_queue_bytes()
                    || replay.last().is_some_and(|event| {
                        event.evidence().frame_id() != self.decoder_evidence.frame_id()
                    })
                {
                    return Err(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch);
                }
            }
            CoinbaseDirectOrderLevelPayload::Event(event) => {
                validate_order_level_event(self.config, event)?;
                if event.evidence().frame_id() != self.decoder_evidence.frame_id() {
                    return Err(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch);
                }
            }
        }
        Ok(())
    }
}

fn validate_order_level_event(
    config: &CoinbaseDirectConfig,
    event: &ProviderOrderEvent,
) -> Result<(), CoinbaseDirectOrderLevelPublicationError> {
    if event.product() != config.product()
        || event.execution_terms() != config.execution_terms()
        || event.evidence().binding().source_id() != config.metadata().source_id()
        || event.evidence().binding().metadata_revision() != config.metadata().revision()
    {
        Err(CoinbaseDirectOrderLevelPublicationError::EvidenceMismatch)
    } else {
        Ok(())
    }
}

/// Borrowed order-level state anchored to one exact REST snapshot generation.
#[derive(Clone, Copy, Debug)]
pub enum CoinbaseDirectOrderLevelPayload<'a> {
    /// Complete non-aggregated snapshot plus its exact post-snapshot replay suffix.
    Snapshot {
        /// Provider cursor returned by the REST level-3 snapshot.
        snapshot_sequence: SequenceNumber,
        /// Provider time returned by the REST level-3 snapshot.
        snapshot_timestamp: Timestamp,
        /// Every non-aggregated `[price, size, order_id]` row in provider response order.
        orders: &'a [ProviderOrderRecord],
        /// Complete contiguous WebSocket suffix applied before atomic handoff.
        replay: &'a [ProviderOrderEvent],
    },
    /// One exact contiguous `full` successor, including cursor-only messages.
    Event(&'a ProviderOrderEvent),
}

/// Invalid order-level publication evidence.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectOrderLevelPublicationError {
    /// Snapshot or frame currentness was revoked before downstream admission.
    #[error("Coinbase Direct order-level publication authority is stale")]
    StaleAuthority,
    /// Snapshot, frame, product, source, revision, or terms evidence is inconsistent.
    #[error("Coinbase Direct order-level publication evidence is inconsistent")]
    EvidenceMismatch,
    /// Immutable metadata does not declare the order-level Direct profile.
    #[error("Coinbase Direct order-level publication profile is inconsistent")]
    ProfileMismatch,
    /// Replay events are not an exact contiguous successor chain.
    #[error("Coinbase Direct order-level replay sequence is inconsistent")]
    SequenceMismatch,
}

/// Exact bounded publication selected by the single-writer Direct owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoinbaseDirectPublicationKind {
    /// First complete bounded price-level image for the connection generation.
    Snapshot,
    /// Exact changed price levels relative to the preceding accepted publication.
    Delta,
    /// Cursor-only successor whose bounded price-level image did not change.
    Quote,
}

/// Failure to bind one synchronized Direct book to the ordinary captured live-ingress contract.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectPublicationError {
    /// Snapshot or frame currentness was revoked before publication.
    #[error("Coinbase Direct publication authority is stale")]
    StaleAuthority,
    /// Snapshot and last-frame evidence do not share one exact source generation.
    #[error("Coinbase Direct publication evidence is inconsistent")]
    EvidenceMismatch,
    /// Immutable metadata no longer exposes the Direct quote profile.
    #[error("Coinbase Direct publication profile is inconsistent")]
    ProfileMismatch,
    /// A synchronized best level cannot be converted exactly using the configured terms.
    #[error("Coinbase Direct publication numeric evidence is invalid")]
    InvalidNumeric,
    /// A bounded publication buffer could not be allocated.
    #[error("Coinbase Direct publication allocation failed")]
    Allocation,
    /// The bounded normalized observation could not be constructed.
    #[error("Coinbase Direct publication observation is invalid")]
    InvalidObservation,
    /// The consuming raw/typed handoff could not bind exact provider evidence.
    #[error("Coinbase Direct market handoff is invalid: {0}")]
    Handoff(CoinbaseMarketHandoffError),
    /// A successor cannot publish until common orchestration returns a material-bound snapshot
    /// claim.
    #[error("Coinbase Direct successor requires an accepted immutable snapshot claim")]
    SnapshotClaimRequired,
}

#[derive(Debug, Eq, PartialEq)]
struct PublishedBookState {
    bids: Vec<DirectPublishedLevel>,
    asks: Vec<DirectPublishedLevel>,
}

impl PublishedBookState {
    fn try_new(depth: usize) -> Result<Self, CoinbaseDirectSessionError> {
        let mut bids = Vec::new();
        bids.try_reserve_exact(depth)
            .map_err(|_error| CoinbaseDirectSessionError::PublicationAllocation)?;
        let mut asks = Vec::new();
        asks.try_reserve_exact(depth)
            .map_err(|_error| CoinbaseDirectSessionError::PublicationAllocation)?;
        Ok(Self { bids, asks })
    }

    fn replace_from(&mut self, book: DirectPublishedBook<'_>) {
        self.bids.clear();
        self.bids.extend(book.bids());
        self.asks.clear();
        self.asks.extend(book.asks());
    }

    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
}

#[derive(Debug)]
struct OrderLevelPublicationState {
    snapshot: Option<CoinbaseDirectSnapshotCoordinates>,
    snapshot_orders: Vec<ProviderOrderRecord>,
    replay_events: Vec<ProviderOrderEvent>,
    replay_bytes: usize,
}

impl OrderLevelPublicationState {
    fn try_new(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseDirectSessionError> {
        let limits = config.limits().book();
        let mut snapshot_orders = Vec::new();
        snapshot_orders
            .try_reserve_exact(limits.max_orders())
            .map_err(|_error| CoinbaseDirectSessionError::OrderLevelAllocation)?;
        let mut replay_events = Vec::new();
        replay_events
            .try_reserve_exact(limits.max_queue_events())
            .map_err(|_error| CoinbaseDirectSessionError::OrderLevelAllocation)?;
        if replay_events.capacity() > config.limits().checked_replay_container_slots()? {
            return Err(CoinbaseDirectSessionError::OrderLevelAllocation);
        }
        Ok(Self {
            snapshot: None,
            snapshot_orders,
            replay_events,
            replay_bytes: 0,
        })
    }

    fn try_queue(
        &mut self,
        config: &CoinbaseDirectConfig,
        event: ProviderOrderEvent,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if self.replay_events.len() == config.limits().book().max_queue_events() {
            return Err(CoinbaseDirectSessionError::OrderLevelState);
        }
        validate_order_level_event(config, &event)
            .map_err(|_error| CoinbaseDirectSessionError::OrderLevelState)?;
        let next_bytes = self
            .replay_bytes
            .checked_add(event.wire_bytes())
            .ok_or(CoinbaseDirectSessionError::OrderLevelState)?;
        if next_bytes > config.limits().book().max_queue_bytes() {
            return Err(CoinbaseDirectSessionError::OrderLevelState);
        }
        self.replay_events.push(event);
        self.replay_bytes = next_bytes;
        Ok(())
    }

    fn bind_snapshot(
        &mut self,
        config: &CoinbaseDirectConfig,
        coordinates: CoinbaseDirectSnapshotCoordinates,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if self.snapshot.is_some()
            || self.snapshot_orders.is_empty()
            || self.snapshot_orders.len() > config.limits().book().max_orders()
        {
            return Err(CoinbaseDirectSessionError::OrderLevelState);
        }
        self.snapshot = Some(coordinates);
        self.replay_events
            .retain(|event| event.sequence() > coordinates.sequence);
        self.replay_bytes = self
            .replay_events
            .iter()
            .try_fold(0_usize, |total, event| {
                total
                    .checked_add(event.wire_bytes())
                    .ok_or(CoinbaseDirectSessionError::OrderLevelState)
            })?;
        if self.replay_bytes > config.limits().book().max_queue_bytes() {
            return Err(CoinbaseDirectSessionError::OrderLevelState);
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.snapshot = None;
        self.snapshot_orders.clear();
        self.replay_events.clear();
        self.replay_bytes = 0;
    }
}

fn provider_level(
    level: DirectPublishedLevel,
    terms: InstrumentExecutionTerms,
) -> Result<ProviderBookLevel, CoinbaseDirectPublicationError> {
    provider_level_parts(level.price(), level.quantity(), terms)
}

fn provider_level_parts(
    price: PriceTicks,
    quantity: QuantityLots,
    terms: InstrumentExecutionTerms,
) -> Result<ProviderBookLevel, CoinbaseDirectPublicationError> {
    let mut price = price
        .checked_to_decimal(terms.price_tick())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    price.rescale(terms.price_tick().as_decimal().scale());
    let mut quantity = quantity
        .checked_to_decimal(terms.lot_size())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    quantity.rescale(terms.lot_size().as_decimal().scale());
    let price = ProviderDecimalLexeme::try_new(&price.to_string())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    let quantity = ProviderDecimalLexeme::try_new(&quantity.to_string())
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?;
    Ok(ProviderBookLevel::new(
        ProviderPrice::new(price),
        ProviderQuantity::new(quantity),
    ))
}

fn provider_levels(
    levels: &[DirectPublishedLevel],
    terms: InstrumentExecutionTerms,
) -> Result<Vec<ProviderBookLevel>, CoinbaseDirectPublicationError> {
    let mut normalized = Vec::new();
    normalized
        .try_reserve_exact(levels.len())
        .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?;
    for level in levels {
        normalized.push(provider_level(*level, terms)?);
    }
    Ok(normalized)
}

fn provider_changes(
    previous: &PublishedBookState,
    current: &PublishedBookState,
    terms: InstrumentExecutionTerms,
) -> Result<Vec<ProviderBookChange>, CoinbaseDirectPublicationError> {
    let capacity = previous
        .bids
        .len()
        .checked_add(previous.asks.len())
        .and_then(|value| value.checked_add(current.bids.len()))
        .and_then(|value| value.checked_add(current.asks.len()))
        .ok_or(CoinbaseDirectPublicationError::InvalidObservation)?;
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(capacity)
        .map_err(|_error| CoinbaseDirectPublicationError::Allocation)?;
    append_side_changes(
        &previous.bids,
        &current.bids,
        ProviderBookSide::Bid,
        terms,
        &mut changes,
    )?;
    append_side_changes(
        &previous.asks,
        &current.asks,
        ProviderBookSide::Ask,
        terms,
        &mut changes,
    )?;
    Ok(changes)
}

fn append_side_changes(
    previous: &[DirectPublishedLevel],
    current: &[DirectPublishedLevel],
    side: ProviderBookSide,
    terms: InstrumentExecutionTerms,
    changes: &mut Vec<ProviderBookChange>,
) -> Result<(), CoinbaseDirectPublicationError> {
    let mut previous_index = 0_usize;
    let mut current_index = 0_usize;
    while previous_index < previous.len() || current_index < current.len() {
        match (
            previous.get(previous_index).copied(),
            current.get(current_index).copied(),
        ) {
            (Some(old), Some(new)) if old.price() == new.price() => {
                if old.quantity() != new.quantity() {
                    changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                }
                previous_index = previous_index.saturating_add(1);
                current_index = current_index.saturating_add(1);
            }
            (Some(old), Some(new)) if level_precedes(old, new, side) => {
                changes.push(ProviderBookChange::new(
                    side,
                    provider_level_parts(
                        old.price(),
                        QuantityLots::new(0)
                            .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?,
                        terms,
                    )?,
                ));
                previous_index = previous_index.saturating_add(1);
            }
            (Some(_old), Some(new)) => {
                changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                current_index = current_index.saturating_add(1);
            }
            (Some(old), None) => {
                changes.push(ProviderBookChange::new(
                    side,
                    provider_level_parts(
                        old.price(),
                        QuantityLots::new(0)
                            .map_err(|_error| CoinbaseDirectPublicationError::InvalidNumeric)?,
                        terms,
                    )?,
                ));
                previous_index = previous_index.saturating_add(1);
            }
            (None, Some(new)) => {
                changes.push(ProviderBookChange::new(side, provider_level(new, terms)?));
                current_index = current_index.saturating_add(1);
            }
            (None, None) => break,
        }
    }
    Ok(())
}

fn level_precedes(
    left: DirectPublishedLevel,
    right: DirectPublishedLevel,
    side: ProviderBookSide,
) -> bool {
    match side {
        ProviderBookSide::Bid => left.price().cmp(&right.price()) == Ordering::Greater,
        ProviderBookSide::Ask => left.price().cmp(&right.price()) == Ordering::Less,
    }
}

fn direct_book_identifier(
    sequence: SequenceNumber,
    snapshot: &SegmentedHttpResponseReceipt,
) -> Result<SourceIdentifier, CoinbaseDirectPublicationError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = snapshot.body_digest().bytes();
    let mut identifier = format!("coinbase-direct-book-{}-", sequence.get());
    identifier
        .try_reserve_exact(digest.len().saturating_mul(2))
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)?;
    for byte in digest {
        identifier.push(char::from(HEX[usize::from(byte >> 4)]));
        identifier.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(identifier)
        .map_err(|_error| CoinbaseDirectPublicationError::InvalidObservation)
}

/// Exact pre-network replay/output admission derived from one immutable Direct configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoinbaseDirectOutputAdmission {
    maximum_events: usize,
    maximum_raw_bytes: usize,
    maximum_container_slots: usize,
    complete_retained_bytes: u64,
}

impl CoinbaseDirectOutputAdmission {
    fn try_from_config(config: &CoinbaseDirectConfig) -> Result<Self, CoinbaseConfigError> {
        Ok(Self {
            maximum_events: config.limits().book().max_queue_events(),
            maximum_raw_bytes: config.limits().book().max_queue_bytes(),
            maximum_container_slots: config.limits().checked_replay_container_slots()?,
            complete_retained_bytes: config.checked_maximum_retained_bytes()?,
        })
    }

    /// Returns the maximum number of unsettled sequenced capture receipts.
    pub const fn maximum_events(self) -> usize {
        self.maximum_events
    }

    /// Returns the maximum summed raw bytes represented by unsettled sequenced receipts.
    pub const fn maximum_raw_bytes(self) -> usize {
        self.maximum_raw_bytes
    }

    /// Returns the allocator-capacity ceiling already included in the complete byte admission.
    pub const fn maximum_container_slots(self) -> usize {
        self.maximum_container_slots
    }

    /// Returns the complete adapter/session/output peak admitted by application composition.
    pub const fn complete_retained_bytes(self) -> u64 {
        self.complete_retained_bytes
    }
}

/// Nonblocking application boundary for one Coinbase Direct generation.
///
/// Every `try_*` callback is synchronous. Implementations must not hide an unbounded queue or
/// await downstream work. Raw frames are accepted through [`RawMarketSink`] before any decoded
/// outcome mutates the session.
pub trait CoinbaseDirectOutput: RawMarketSink {
    /// Admits and preallocates the complete bounded replay-receipt owner before network activity.
    fn try_admit_replay(
        &mut self,
        admission: CoinbaseDirectOutputAdmission,
    ) -> Result<(), SinkError>;

    /// Accepts the exact captured and validated signed `full` acknowledgement.
    ///
    /// The adapter invokes this only after [`RawMarketSink::try_publish`] accepted the same raw
    /// frame. Implementations must pair `evidence` with that capture receipt before admitting
    /// coverage; a payload digest without the receipt is insufficient.
    fn try_publish_subscription_acknowledgement(
        &mut self,
        acknowledgement: DecodedControlFrame,
    ) -> Result<(), SinkError>;

    /// Accepts current provider product/status/tick/lot evidence without qualifying it.
    fn try_publish_product(
        &mut self,
        evidence: CoinbaseDirectProductEvidence,
    ) -> Result<(), SinkError>;

    /// Accepts one private lifecycle event that carries no public cursor or book authority.
    fn try_publish_non_book(&mut self, event: CoinbaseDirectNonBookEvent) -> Result<(), SinkError>;

    /// Retains capture admission for every exact sequenced frame admitted by the bounded replay
    /// owner. Implementations must preserve those receipts in order until the consuming handoff.
    fn try_retain_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError>;

    /// Consumes capture admission for one sequenced frame proven to be at or before the exact
    /// REST snapshot cutoff. The raw capture remains sealed; only its ineligible replay claim is
    /// discarded. Implementations must accept either the just-captured frame or the ordered front
    /// of the retained pre-snapshot queue and must reject every other identity.
    fn try_discard_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError>;

    /// Accepts the explicit order-level snapshot/replay handoff or one contiguous successor.
    ///
    /// Existing price-level consumers remain source-compatible. The default rejects an activated
    /// order-level profile so order identities can never disappear silently; central composition
    /// must implement this callback before selecting [`CoinbaseDirectConfig::try_new_order_level`].
    fn try_publish_order_level(
        &mut self,
        _update: CoinbaseDirectOrderLevelUpdate<'_>,
    ) -> Result<(), SinkError> {
        Err(SinkError::CaptureIncomplete)
    }

    /// Consumes the sole raw-plus-typed market handoff. The initial Direct value owns the exact
    /// segmented snapshot and every surviving replay frame; returning an error must not discard
    /// that pending graph at the product-orchestration boundary.
    fn try_publish_book(&mut self, handoff: CoinbaseMarketHandoff) -> Result<(), SinkError>;
}

/// Terminal construction, transport, synchronization, or output failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CoinbaseDirectSessionError {
    /// Immutable Coinbase configuration could not produce the pinned runtime profile.
    #[error("Coinbase Direct session configuration is invalid: {0}")]
    Configuration(#[from] CoinbaseConfigError),
    /// Registry generation, provider budget, network, cancellation, or raw sink failure.
    #[error("Coinbase Direct source authority or transport failed: {0}")]
    Source(#[from] SourceError),
    /// Application-owned signing capability or signed subscription construction failed.
    #[error("Coinbase Direct signing failed: {0}")]
    Signing(#[from] CoinbaseDirectSigningError),
    /// A captured order lifecycle frame failed the pinned decoder.
    #[error("Coinbase Direct frame decode failed: {0}")]
    Decode(#[from] CoinbaseDirectDecodeError),
    /// A complete captured product response failed typed evidence decoding.
    #[error("Coinbase Direct product evidence failed: {0}")]
    Product(#[from] CoinbaseDirectProductError),
    /// A complete captured level-3 response failed snapshot decoding.
    #[error("Coinbase Direct snapshot failed: {0}")]
    Snapshot(#[from] CoinbaseDirectSnapshotError),
    /// Same-owner queue, replay, or live mutation failed.
    #[error("Coinbase Direct book synchronization failed: {0}")]
    Book(#[from] DirectOrderBookError),
    /// Generation-bound segmented HTTP capture failed.
    #[error("Coinbase Direct HTTP capture failed: {0}")]
    Capture(#[from] SegmentedHttpCaptureError),
    /// The exact signed `full` subscription acknowledgement was absent, malformed, or duplicated.
    #[error("Coinbase Direct subscription acknowledgement is invalid")]
    Subscription,
    /// A WebSocket payload or internal protocol frame was not accepted by the pinned profile.
    #[error("Coinbase Direct WebSocket protocol is invalid")]
    WebSocketProtocol,
    /// HTTP response metadata, headers, media type, or final URL was not admitted.
    #[error("Coinbase Direct HTTP response is invalid")]
    HttpResponse,
    /// HTTP acquisition exceeded its configured total deadline.
    #[error("Coinbase Direct HTTP response deadline elapsed")]
    HttpDeadline,
    /// HTTP response bytes exceeded the configured complete-body ceiling.
    #[error("Coinbase Direct HTTP response exceeded its byte ceiling")]
    HttpBodyTooLarge,
    /// HTTP response segmentation exceeded the configured count ceiling.
    #[error("Coinbase Direct HTTP response exceeded its segment ceiling")]
    HttpSegmentLimit,
    /// The bounded product/status refresh deadline could not be represented.
    #[error("Coinbase Direct product refresh deadline is invalid")]
    ProductRefreshDeadline,
    /// A cancellation close handshake failed or exceeded its bound.
    #[error("Coinbase Direct WebSocket shutdown failed")]
    Shutdown,
    /// The bounded publication-state buffers could not be reserved before transport starts.
    #[error("Coinbase Direct publication state allocation failed")]
    PublicationAllocation,
    /// The bounded order-level bootstrap buffers could not be reserved before transport starts.
    #[error("Coinbase Direct order-level publication allocation failed")]
    OrderLevelAllocation,
    /// Order-level snapshot/replay/event state was incomplete, excessive, or out of phase.
    #[error("Coinbase Direct order-level publication state is invalid")]
    OrderLevelState,
    /// The retained post-snapshot replay graph exceeded its admitted count or raw-byte ceiling.
    #[error("Coinbase Direct replay lineage exceeded its admitted bounds")]
    ReplayLineageLimit,
    /// Common product orchestration has not returned a material-bound immutable snapshot claim.
    #[error("Coinbase Direct immutable snapshot claim is unavailable")]
    SnapshotClaimRequired,
}

#[derive(Debug)]
struct CoinbaseDirectDispatchedHttpResponse {
    response: CoinbaseDirectHttpResponse,
    permit: BudgetPermit,
}

#[derive(Debug)]
struct SequencedFrameEvidence {
    event: ProviderOrderEvent,
    raw_payload: CapturePayload,
    native_trade: Option<CoinbaseDirectTradeEvidence>,
}

/// Production one-generation Coinbase Direct session.
#[derive(Debug)]
pub struct CoinbaseDirectSession {
    config: CoinbaseDirectConfig,
    authority: ActiveLiveSourceGeneration,
    budget: SharedProviderBudget,
    decoder: CoinbaseDirectDecoder,
    snapshot_decoder: CoinbaseDirectSnapshotDecoder,
    http: Arc<dyn CoinbaseDirectHttpTransport>,
    http_timeout: Duration,
    book: DirectOrderBook,
    acknowledgement_evidence: Option<ExactPayloadEvidence>,
    request_set_digest: EvidenceDigest,
    subscription_request_digest: Option<EvidenceDigest>,
    replay_frames: Vec<SequencedFrameEvidence>,
    replay_bytes: usize,
    last_observed_sequence: Option<SequenceNumber>,
    snapshot_capture: Option<SegmentedHttpResponseCapture>,
    snapshot_coordinates: Option<CoinbaseDirectSnapshotCoordinates>,
    published_state: PublishedBookState,
    next_published_state: PublishedBookState,
    order_level_state: Option<OrderLevelPublicationState>,
    has_published_book: bool,
    last_published_sequence: Option<SequenceNumber>,
    next_product_refresh: Option<Instant>,
    generation_started: bool,
}

impl CoinbaseDirectSession {
    /// Consumes one registry-minted generation and the project-installed TLS capability before
    /// creating the hardened production HTTP client.
    ///
    /// No credentials are read or retained. The signing capability is supplied only to
    /// [`Self::run`] at first use.
    pub fn try_new(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        tls_provider: TlsProviderCapability,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let bounds = direct_http_bounds(&config)?;
        let http = Arc::new(
            ReqwestCoinbaseDirectHttpTransport::try_new(bounds, tls_provider)
                .map_err(|_error| CoinbaseDirectSessionError::HttpResponse)?,
        );
        Self::try_new_inner(config, generation, http)
    }

    #[cfg(test)]
    fn try_new_with_transport(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        Self::try_new_inner(config, generation, http)
    }

    fn try_new_inner(
        config: CoinbaseDirectConfig,
        generation: LiveSourceGeneration,
        http: Arc<dyn CoinbaseDirectHttpTransport>,
    ) -> Result<Self, CoinbaseDirectSessionError> {
        let authority = generation.try_start(config.metadata())?;
        let budget = authority
            .budget()?
            .cloned()
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        let decoder = CoinbaseDirectDecoder::try_new(&config)?;
        let snapshot_decoder = CoinbaseDirectSnapshotDecoder::try_new(&config)?;
        let book = DirectOrderBook::try_new(
            authority.generation(),
            config.product().clone(),
            config.execution_terms(),
            config.limits().book(),
        )?;
        let bounds = direct_http_bounds(&config)?;
        let published_depth = config.limits().book().published_depth();
        let order_level_state = if config.publication_depth() == MarketDepth::OrderLevel {
            Some(OrderLevelPublicationState::try_new(&config)?)
        } else {
            None
        };
        let request_set_digest = direct_request_set_digest(&config);
        let mut replay_frames = Vec::new();
        replay_frames
            .try_reserve_exact(config.limits().book().max_queue_events())
            .map_err(|_error| CoinbaseDirectSessionError::PublicationAllocation)?;
        if replay_frames.capacity() > config.limits().checked_replay_container_slots()? {
            return Err(CoinbaseDirectSessionError::PublicationAllocation);
        }
        Ok(Self {
            config,
            authority,
            budget,
            decoder,
            snapshot_decoder,
            http,
            http_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            book,
            acknowledgement_evidence: None,
            request_set_digest,
            subscription_request_digest: None,
            replay_frames,
            replay_bytes: 0,
            last_observed_sequence: None,
            snapshot_capture: None,
            snapshot_coordinates: None,
            published_state: PublishedBookState::try_new(published_depth)?,
            next_published_state: PublishedBookState::try_new(published_depth)?,
            order_level_state,
            has_published_book: false,
            last_published_sequence: None,
            next_product_refresh: None,
            generation_started: false,
        })
    }

    /// Returns immutable source metadata. Its quality is a ceiling declaration, not current
    /// qualification minted by this session.
    pub const fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }

    /// Runs one production connection until cancellation or a typed fail-closed terminal defect.
    pub async fn run(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        self.try_admit_output(output)?;
        let outcome = self.run_production(signer, output, cancellation).await;
        self.finish_generation(outcome)
    }

    async fn run_production(
        &mut self,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError> {
        self.begin_generation()?;
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled.into());
        }
        self.validate_generation()?;
        self.authorize_endpoint(self.config.websocket_endpoint())?;
        let limits = self.config.limits().websocket();
        let websocket_config = WebSocketConfig::default()
            .read_buffer_size(limits.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(32 * 1024)
            .max_message_size(Some(limits.max_frame_bytes()))
            .max_frame_size(Some(limits.max_frame_bytes()));
        let connect = connect_async_with_config(
            self.config.websocket_endpoint(),
            Some(websocket_config),
            true,
        );
        let reservation = self.reserve_budget()?;
        let permit = self.commit_budget(reservation)?;
        let (socket, _response) =
            await_websocket(&cancellation, limits.connect_timeout(), connect, |error| {
                map_connect_error(error, &self.budget)
            })
            .await?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        permit.release();
        self.run_connected(socket, signer, None, output, cancellation)
            .await
    }

    #[cfg(test)]
    async fn run_with_socket_for_test<S>(
        &mut self,
        socket: WebSocketStream<S>,
        signer: &dyn CoinbaseDirectSigningCapability,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
        unix_seconds: u64,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.try_admit_output(output)?;
        let outcome = async {
            self.begin_generation()?;
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            self.validate_generation()?;
            let reservation = self.reserve_budget()?;
            let permit = self.commit_budget(reservation)?;
            permit.release();
            self.run_connected(socket, signer, Some(unix_seconds), output, cancellation)
                .await
        }
        .await;
        self.finish_generation(outcome)
    }

    fn try_admit_output(
        &self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        output
            .try_admit_replay(CoinbaseDirectOutputAdmission::try_from_config(
                &self.config,
            )?)
            .map_err(SourceError::Sink)?;
        Ok(())
    }

    async fn run_connected<S>(
        &mut self,
        socket: WebSocketStream<S>,
        signer: &dyn CoinbaseDirectSigningCapability,
        fixed_unix_seconds: Option<u64>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        let (outcome, shutdown) = {
            let mut socket = socket;
            let outcome = async {
                let unix_seconds = match fixed_unix_seconds {
                    Some(unix_seconds) => unix_seconds,
                    None => current_unix_seconds()?,
                };
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.validate_generation()?;
                let subscription = self.config.try_signed_subscription(unix_seconds, signer)?;
                self.validate_generation()?;
                if cancellation.is_cancelled() {
                    return Err(SourceError::Cancelled.into());
                }
                self.run_connected_inner(&mut socket, subscription, output, &cancellation)
                    .await
            };
            let outcome = outcome.await;
            let shutdown = if matches!(
                outcome,
                Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled))
            ) {
                self.shutdown_socket(&mut socket, output, limits.io_timeout())
                    .await
            } else {
                Ok(())
            };
            (outcome, shutdown)
        };
        shutdown?;
        outcome
    }

    async fn run_connected_inner<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        subscription: CoinbaseSignedSubscription,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let limits = self.config.limits().websocket();
        self.validate_generation()?;
        let subscription_reservation = self.reserve_budget()?;
        let subscription_permit = self.commit_budget(subscription_reservation)?;
        let subscription_digest = exact_digest(subscription.as_str().as_bytes());
        send_with_deadline(
            socket,
            Message::Text(subscription.as_str().into()),
            cancellation,
            limits.io_timeout(),
        )
        .await?;
        self.subscription_request_digest = Some(subscription_digest);
        drop(subscription);
        self.await_subscription(socket, output, cancellation)
            .await?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        subscription_permit.release();

        self.bootstrap_and_run_live(socket, output, cancellation)
            .await
    }

    async fn await_subscription<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let acknowledgement = async {
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if self
                    .handle_message(
                        socket,
                        message,
                        output,
                        cancellation,
                        InboundMode::AwaitingAck,
                    )
                    .await?
                    == MessageDisposition::Acknowledged
                {
                    return Ok(());
                }
            }
        };
        match tokio::time::timeout(io_timeout, acknowledgement).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn bootstrap_and_run_live<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let product_url = self.config.product_url().to_owned();
        let product_response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &product_url,
                InboundMode::Queueing,
            )
            .await?;
        let product_capture = self.finish_http_capture(&product_url, product_response)?;
        let product_evidence = self.config.decode_product_evidence(&product_capture)?;
        output
            .try_publish_product(product_evidence)
            .map_err(SourceError::Sink)?;
        self.schedule_product_refresh()?;

        let snapshot_url = self.config.snapshot_url().to_owned();
        let snapshot_response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &snapshot_url,
                InboundMode::Queueing,
            )
            .await?;
        let snapshot_capture = self.finish_http_capture(&snapshot_url, snapshot_response)?;
        let frontier =
            handoff_frontier_payload(snapshot_capture.receipt(), self.authority.generation());
        self.await_handoff_frontier(socket, output, cancellation, frontier)
            .await?;
        let snapshot_coordinates = if let Some(order_level) = self.order_level_state.as_mut() {
            let coordinates = self.snapshot_decoder.decode_into_order_level(
                &snapshot_capture,
                &mut self.book,
                &mut order_level.snapshot_orders,
            )?;
            order_level.bind_snapshot(&self.config, coordinates)?;
            coordinates
        } else {
            self.snapshot_decoder
                .decode_into_coordinates(&snapshot_capture, &mut self.book)?
        };
        self.discard_replay_through(snapshot_coordinates.sequence, output)?;
        self.book.begin_replay()?;
        loop {
            if cancellation.is_cancelled() {
                return Err(SourceError::Cancelled.into());
            }
            if !self.book.replay_next()? {
                break;
            }
        }
        self.book.finish_replay()?;
        self.validate_generation()?;
        self.snapshot_capture = Some(snapshot_capture);
        self.snapshot_coordinates = Some(snapshot_coordinates);
        self.publish_initial_book_if_exact(output)?;

        loop {
            if !self.replay_frames.is_empty() {
                let snapshot_receipt = self
                    .snapshot_capture
                    .as_ref()
                    .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?
                    .receipt();
                let frontier =
                    publication_frontier_payload(snapshot_receipt, self.authority.generation());
                self.await_publication_frontier(socket, output, cancellation, frontier)
                    .await?;
                self.publish_initial_book_if_exact(output)?;
            }
            let refresh_at = self
                .next_product_refresh
                .ok_or(CoinbaseDirectSessionError::ProductRefreshDeadline)?;
            match read_with_refresh_deadline(
                socket,
                output,
                cancellation,
                self.config.limits().websocket().io_timeout(),
                refresh_at,
            )
            .await?
            {
                LiveRead::Message(message) => {
                    let _disposition = self
                        .handle_message(socket, message, output, cancellation, InboundMode::Live)
                        .await?;
                }
                LiveRead::RefreshProduct => {
                    self.refresh_product_while_live(socket, output, cancellation)
                        .await?;
                }
            }
        }
    }

    async fn refresh_product_while_live<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let product_url = self.config.product_url().to_owned();
        let response = self
            .fetch_while_processing(
                socket,
                output,
                cancellation,
                &product_url,
                InboundMode::Live,
            )
            .await?;
        let capture = self.finish_http_capture(&product_url, response)?;
        let evidence = self.config.decode_product_evidence(&capture)?;
        output
            .try_publish_product(evidence)
            .map_err(SourceError::Sink)?;
        self.schedule_product_refresh()
    }

    async fn await_handoff_frontier<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        frontier: Bytes,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let io_timeout = self.config.limits().websocket().io_timeout();
        let operation = async {
            self.validate_generation()?;
            send_with_deadline(
                socket,
                Message::Ping(frontier.clone()),
                cancellation,
                io_timeout,
            )
            .await?;
            let mut matching_pong_seen = false;
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if matches!(&message, Message::Pong(payload) if payload == &frontier) {
                    self.validate_generation()?;
                    // The matching Pong is ordered after every preceding data frame. Once any
                    // product cursor exists, extending the queue past this marker would make the
                    // snapshot boundary depend on scheduler timing.
                    if self.last_observed_sequence.is_some() {
                        return Ok(());
                    }
                    matching_pong_seen = true;
                    continue;
                }
                let disposition = self
                    .handle_message(socket, message, output, cancellation, InboundMode::Queueing)
                    .await?;
                if matching_pong_seen && disposition == MessageDisposition::Sequenced {
                    self.validate_generation()?;
                    return Ok(());
                }
            }
        };
        match tokio::time::timeout(io_timeout, operation).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn await_publication_frontier<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        frontier: Bytes,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if self.replay_frames.is_empty() {
            return Err(CoinbaseDirectSessionError::WebSocketProtocol);
        }
        let io_timeout = self.config.limits().websocket().io_timeout();
        let operation = async {
            self.validate_generation()?;
            send_with_deadline(
                socket,
                Message::Ping(frontier.clone()),
                cancellation,
                io_timeout,
            )
            .await?;
            loop {
                let message = read_with_deadline(socket, output, cancellation, io_timeout).await?;
                if matches!(&message, Message::Pong(payload) if payload == &frontier) {
                    self.validate_generation()?;
                    return Ok(());
                }
                let _disposition = self
                    .handle_message(
                        socket,
                        message,
                        output,
                        cancellation,
                        InboundMode::PublicationFrontier,
                    )
                    .await?;
            }
        };
        match tokio::time::timeout(io_timeout, operation).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(SourceError::ConnectionIdle.into()),
        }
    }

    async fn fetch_while_processing<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        url: &str,
        mode: InboundMode,
    ) -> Result<CoinbaseDirectDispatchedHttpResponse, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut response = self.start_http_request(url, cancellation.clone())?;
        loop {
            let deadline =
                ReceiveDeadline::strictest(output, self.config.limits().websocket().io_timeout())?;
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(SourceError::Cancelled.into());
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
                    if deadline.sink_owned {
                        output
                            .poll_deadline(Instant::now())
                            .map_err(SourceError::Sink)?;
                        return Err(SourceError::InvalidProtocolState.into());
                    }
                    return Err(SourceError::ConnectionIdle.into());
                }
                completed = &mut response => {
                    return completed;
                }
                message = socket.next() => {
                    let message = map_next_message(message)?;
                    let _disposition = self
                        .handle_message(
                            socket,
                            message,
                            output,
                            cancellation,
                            mode,
                        )
                        .await?;
                }
            }
        }
    }

    fn start_http_request(
        &self,
        url: &str,
        cancellation: CancellationToken,
    ) -> Result<
        BoxFuture<
            'static,
            Result<CoinbaseDirectDispatchedHttpResponse, CoinbaseDirectSessionError>,
        >,
        CoinbaseDirectSessionError,
    > {
        self.validate_generation()?;
        self.authorize_endpoint(url)?;
        let limits = self.config.limits();
        let request = CoinbaseDirectHttpRequest::new(
            url,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
            self.http_timeout,
            cancellation,
        );
        let transport = Arc::clone(&self.http);
        let reservation = self.reserve_budget()?;
        Ok(Box::pin(async move {
            let permit = match reservation.commit_dispatch() {
                BudgetDispatchDecision::Ready(permit) => permit,
                BudgetDispatchDecision::WaitUntil(deadline) => {
                    return Err(SourceError::BudgetWaitUntil { deadline }.into());
                }
                BudgetDispatchDecision::Unavailable(reason) => {
                    return Err(SourceError::BudgetUnavailable { reason }.into());
                }
            };
            let response = transport
                .get(request)
                .await
                .map_err(map_http_transport_error)?;
            Ok(CoinbaseDirectDispatchedHttpResponse { response, permit })
        }))
    }

    fn finish_http_capture(
        &mut self,
        expected_url: &str,
        dispatched: CoinbaseDirectDispatchedHttpResponse,
    ) -> Result<SegmentedHttpResponseCapture, CoinbaseDirectSessionError> {
        let CoinbaseDirectDispatchedHttpResponse { response, permit } = dispatched;
        self.validate_generation()?;
        self.authorize_endpoint(response.final_url.as_ref())?;
        if response.final_url.as_ref() != expected_url {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        if matches!(response.status, 401 | 403) {
            return Err(SourceError::Unauthorized.into());
        }
        if response.status == 429 || (500..=599).contains(&response.status) {
            return Err(
                SourceError::from_applied_budget_refusal(apply_http_retry_after(
                    &self.budget,
                    response.retry_after.as_deref(),
                    1_000,
                ))
                .into(),
            );
        }
        if response.status != 200
            || !content_type_is_json(response.content_type.as_deref())
            || response
                .content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(CoinbaseDirectSessionError::HttpResponse);
        }
        let limits = self.config.limits();
        let mut builder = self.authority.frames_mut()?.try_http_response_builder(
            HttpCaptureMethod::Get,
            response.final_url.as_ref(),
            response.status,
            response.declared_body_length,
            limits.max_snapshot_bytes(),
            limits.max_snapshot_segments(),
        )?;
        for segment in response.segments {
            builder.try_push_segment(segment)?;
        }
        let capture = builder.finish()?;
        self.validate_generation()?;
        self.budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        permit.release();
        Ok(capture)
    }

    async fn handle_message<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        message: Message,
        output: &mut dyn CoinbaseDirectOutput,
        cancellation: &CancellationToken,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match message {
            Message::Text(text) => self.capture_decode_commit(
                TransportFrameKind::Text,
                Bytes::copy_from_slice(text.as_bytes()),
                output,
                mode,
            ),
            Message::Binary(payload) => {
                self.capture_rejected_protocol_frame(TransportFrameKind::Binary, payload, output)?;
                Err(if mode == InboundMode::AwaitingAck {
                    CoinbaseDirectSessionError::Subscription
                } else {
                    CoinbaseDirectSessionError::WebSocketProtocol
                })
            }
            Message::Ping(payload) => {
                send_with_deadline(
                    socket,
                    Message::Pong(payload),
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Ok(MessageDisposition::Control)
            }
            Message::Pong(_) => Ok(MessageDisposition::Control),
            Message::Close(_frame) => {
                flush_with_deadline(
                    socket,
                    cancellation,
                    self.config.limits().websocket().io_timeout(),
                )
                .await?;
                Err(SourceError::ProviderUnavailable.into())
            }
            Message::Frame(_) => Err(CoinbaseDirectSessionError::WebSocketProtocol),
        }
    }

    fn capture_decode_commit(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
        mode: InboundMode,
    ) -> Result<MessageDisposition, CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let retained_payload = frame.capture_payload().clone();
        let acknowledgement_evidence = if mode == InboundMode::AwaitingAck {
            let digest: [u8; 32] = Sha256::digest(frame.payload()).into();
            Some(ExactPayloadEvidence::from_content_digest(
                EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            ))
        } else {
            None
        };
        let decoded = (|| {
            let validated = self.authority.validate_live_frame(&frame)?;
            if mode == InboundMode::AwaitingAck {
                validate_subscription_ack(validated.frame().payload(), self.config.product())?;
                let SourceProtocolProfile::Live(protocol) =
                    self.config.metadata().protocol_profile()
                else {
                    return Err(CoinbaseDirectSessionError::Subscription);
                };
                Ok((
                    None,
                    Some(DecoderEvidence::from_validated_frame(
                        &validated,
                        protocol.decoder_rule().clone(),
                    )),
                ))
            } else {
                match self.decoder.decode(&validated) {
                    Ok(outcome) => Ok((Some(outcome), None)),
                    Err(CoinbaseDirectDecodeError::UnsupportedMessage)
                        if validate_subscription_ack(
                            validated.frame().payload(),
                            self.config.product(),
                        )
                        .is_ok() =>
                    {
                        Err(CoinbaseDirectSessionError::Subscription)
                    }
                    Err(error) => Err(error.into()),
                }
            }
        })();
        output.try_publish(frame).map_err(SourceError::Sink)?;
        let (decoded, validated_acknowledgement) = decoded?;
        if let Some(evidence) = validated_acknowledgement {
            output
                .try_publish_subscription_acknowledgement(DecodedControlFrame::new(
                    evidence,
                    ControlFrameKind::SubscriptionAcknowledgement,
                    None,
                ))
                .map_err(SourceError::Sink)?;
            self.acknowledgement_evidence = acknowledgement_evidence;
            return Ok(MessageDisposition::Acknowledged);
        }
        let decoded = decoded.ok_or(CoinbaseDirectSessionError::Subscription)?;
        match decoded {
            CoinbaseDirectDecodeOutcome::Sequenced(decoded_event) => {
                let (event, native_trade) = decoded_event.into_parts();
                let sequence = event.sequence();
                let evidence = event.evidence().clone();
                let retained_event = event.clone();
                let order_level_event = self.order_level_state.as_ref().map(|_state| event.clone());
                if let Some(previous) = self.last_observed_sequence {
                    validate_sequenced_progression(previous, sequence)?;
                }
                let retain_for_replay = match mode {
                    InboundMode::Queueing => {
                        self.book.try_queue(event)?;
                        if let (Some(state), Some(order_level_event)) =
                            (self.order_level_state.as_mut(), order_level_event)
                        {
                            state.try_queue(&self.config, order_level_event)?;
                        }
                        true
                    }
                    InboundMode::Live | InboundMode::PublicationFrontier => {
                        if self.has_published_book {
                            return Err(CoinbaseDirectSessionError::SnapshotClaimRequired);
                        }
                        let book_sequence = self
                            .book
                            .last_sequence()
                            .ok_or(DirectOrderBookError::WrongPhase)?;
                        if sequence > book_sequence {
                            self.book.try_apply_live(event)?;
                            if let (Some(state), Some(order_level_event)) =
                                (self.order_level_state.as_mut(), order_level_event)
                            {
                                state.try_queue(&self.config, order_level_event)?;
                            }
                            true
                        } else {
                            false
                        }
                    }
                    InboundMode::AwaitingAck => {
                        return Err(CoinbaseDirectSessionError::Subscription);
                    }
                };
                if retain_for_replay {
                    self.try_push_replay_frame(SequencedFrameEvidence {
                        event: retained_event,
                        raw_payload: retained_payload,
                        native_trade,
                    })?;
                    output
                        .try_retain_sequenced_frame(&evidence)
                        .map_err(SourceError::Sink)?;
                } else {
                    output
                        .try_discard_sequenced_frame(&evidence)
                        .map_err(SourceError::Sink)?;
                }
                self.last_observed_sequence = Some(sequence);
                Ok(MessageDisposition::Sequenced)
            }
            CoinbaseDirectDecodeOutcome::NonBook(event) => {
                output
                    .try_publish_non_book(event)
                    .map_err(SourceError::Sink)?;
                Ok(MessageDisposition::NonBook)
            }
        }
    }

    fn try_push_replay_frame(
        &mut self,
        frame: SequencedFrameEvidence,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let limits = self.config.limits().book();
        if self.replay_frames.len() >= limits.max_queue_events()
            || frame.event.wire_bytes() != frame.raw_payload.as_bytes().len()
            || frame.event.evidence().payload_digest() != exact_digest(frame.raw_payload.as_bytes())
        {
            return Err(CoinbaseDirectSessionError::ReplayLineageLimit);
        }
        let next_bytes = self
            .replay_bytes
            .checked_add(frame.event.wire_bytes())
            .ok_or(CoinbaseDirectSessionError::ReplayLineageLimit)?;
        if next_bytes > limits.max_queue_bytes() {
            return Err(CoinbaseDirectSessionError::ReplayLineageLimit);
        }
        self.replay_frames.push(frame);
        self.replay_bytes = next_bytes;
        Ok(())
    }

    fn discard_replay_through(
        &mut self,
        cutoff: SequenceNumber,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let discarded_count = self
            .replay_frames
            .iter()
            .take_while(|frame| frame.event.sequence() <= cutoff)
            .count();
        let discarded_bytes =
            self.replay_frames[..discarded_count]
                .iter()
                .try_fold(0_usize, |total, frame| {
                    output
                        .try_discard_sequenced_frame(frame.event.evidence())
                        .map_err(SourceError::Sink)?;
                    total
                        .checked_add(frame.event.wire_bytes())
                        .ok_or(CoinbaseDirectSessionError::ReplayLineageLimit)
                })?;
        self.replay_frames.drain(..discarded_count);
        self.replay_bytes = self
            .replay_bytes
            .checked_sub(discarded_bytes)
            .ok_or(CoinbaseDirectSessionError::ReplayLineageLimit)?;
        if self
            .replay_frames
            .first()
            .is_some_and(|frame| frame.event.sequence() <= cutoff)
        {
            return Err(CoinbaseDirectSessionError::WebSocketProtocol);
        }
        Ok(())
    }

    fn capture_rejected_protocol_frame(
        &mut self,
        transport: TransportFrameKind,
        payload: Bytes,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        ensure_frame_bound(
            payload.len(),
            self.config.limits().websocket().max_frame_bytes(),
        )?;
        let frame = self.authority.frames_mut()?.try_frame(transport, payload)?;
        let _validated = self.authority.validate_live_frame(&frame)?;
        output.try_publish(frame).map_err(SourceError::Sink)?;
        Ok(())
    }

    fn publish_book(
        &mut self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if self.has_published_book {
            return Err(CoinbaseDirectSessionError::SnapshotClaimRequired);
        }
        let sequence = self
            .book
            .last_sequence()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        let source_timestamp = self
            .book
            .source_timestamp()
            .ok_or(DirectOrderBookError::SnapshotTimestampRequired)?;
        if self
            .replay_frames
            .last()
            .is_none_or(|frame| frame.event.sequence() != sequence)
        {
            return Err(CoinbaseDirectSessionError::WebSocketProtocol);
        }
        let subscription_evidence = self
            .acknowledgement_evidence
            .as_ref()
            .ok_or(CoinbaseDirectSessionError::Subscription)?;
        let subscription_request_digest = self
            .subscription_request_digest
            .ok_or(CoinbaseDirectSessionError::Subscription)?;
        let snapshot_coordinates = self
            .snapshot_coordinates
            .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?;
        let book = self
            .book
            .published_book()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        self.next_published_state.replace_from(book);
        let snapshot_capture = self
            .snapshot_capture
            .take()
            .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?;
        let replay_frames = std::mem::take(&mut self.replay_frames);
        self.replay_bytes = 0;
        let handoff = CoinbaseDirectBookUpdate {
            config: &self.config,
            sequence,
            source_timestamp,
            request_set_digest: self.request_set_digest,
            subscription_request_digest,
            subscription_evidence,
            snapshot_capture,
            replay_frames,
            snapshot_coordinates,
            previous_published_sequence: None,
            previous: None,
            current: &self.next_published_state,
            publication: CoinbaseDirectPublicationKind::Snapshot,
        }
        .try_market_handoff()
        .map_err(|_error| CoinbaseDirectSessionError::WebSocketProtocol)?;
        output
            .try_publish_book(handoff)
            .map_err(SourceError::Sink)?;
        Err(CoinbaseDirectSessionError::SnapshotClaimRequired)
    }

    fn publish_initial_book_if_exact(
        &mut self,
        output: &mut dyn CoinbaseDirectOutput,
    ) -> Result<(), CoinbaseDirectSessionError> {
        let sequence = self
            .book
            .last_sequence()
            .ok_or(DirectOrderBookError::WrongPhase)?;
        let Some(observed) = self
            .replay_frames
            .last()
            .map(|frame| frame.event.sequence())
        else {
            return Ok(());
        };
        match observed.cmp(&sequence) {
            Ordering::Equal => self.publish_book(output),
            Ordering::Less => Ok(()),
            Ordering::Greater => Err(CoinbaseDirectSessionError::WebSocketProtocol),
        }
    }

    fn validate_generation(&self) -> Result<(), SourceError> {
        self.authority.validate_current()?;
        let issued = self
            .authority
            .budget()?
            .ok_or(SourceError::GenerationAuthorityMismatch)?;
        if !self.budget.shares_allocation_with(issued) {
            return Err(SourceError::GenerationAuthorityMismatch);
        }
        Ok(())
    }

    fn authorize_endpoint(&self, target: &str) -> Result<(), SourceError> {
        self.config
            .metadata()
            .network_policy()
            .authorize(target)
            .map_err(|_error| SourceError::InvalidProtocolState)
    }

    fn reserve_budget(&self) -> Result<BudgetReservation, SourceError> {
        self.validate_generation()?;
        match self.budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => Ok(reservation),
            BudgetReservationDecision::WaitUntil(deadline) => {
                Err(SourceError::BudgetWaitUntil { deadline })
            }
            BudgetReservationDecision::Unavailable(reason) => {
                Err(SourceError::BudgetUnavailable { reason })
            }
        }
    }

    fn commit_budget(&self, reservation: BudgetReservation) -> Result<BudgetPermit, SourceError> {
        self.validate_generation()?;
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => Ok(permit),
            BudgetDispatchDecision::WaitUntil(deadline) => {
                Err(SourceError::BudgetWaitUntil { deadline })
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                Err(SourceError::BudgetUnavailable { reason })
            }
        }
    }

    fn schedule_product_refresh(&mut self) -> Result<(), CoinbaseDirectSessionError> {
        self.next_product_refresh = Some(
            Instant::now()
                .checked_add(self.config.limits().product_refresh_interval())
                .ok_or(CoinbaseDirectSessionError::ProductRefreshDeadline)?,
        );
        Ok(())
    }

    fn begin_generation(&mut self) -> Result<(), SourceError> {
        if self.generation_started {
            return Err(SourceError::InvalidProtocolState);
        }
        self.generation_started = true;
        Ok(())
    }

    fn finish_generation(
        &mut self,
        outcome: Result<(), CoinbaseDirectSessionError>,
    ) -> Result<(), CoinbaseDirectSessionError> {
        if outcome.is_err() {
            self.book.invalidate_generation();
            self.acknowledgement_evidence = None;
            self.subscription_request_digest = None;
            self.replay_frames.clear();
            self.replay_bytes = 0;
            self.last_observed_sequence = None;
            self.snapshot_capture = None;
            self.snapshot_coordinates = None;
            self.published_state.clear();
            self.next_published_state.clear();
            if let Some(order_level) = self.order_level_state.as_mut() {
                order_level.clear();
            }
            self.has_published_book = false;
            self.last_published_sequence = None;
            self.next_product_refresh = None;
        }
        outcome
    }

    async fn shutdown_socket<S>(
        &mut self,
        socket: &mut WebSocketStream<S>,
        output: &mut dyn CoinbaseDirectOutput,
        deadline: Duration,
    ) -> Result<(), CoinbaseDirectSessionError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let operation = async {
            socket
                .send(Message::Close(None))
                .await
                .map_err(|_error| CoinbaseDirectSessionError::Shutdown)?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Text,
                            Bytes::copy_from_slice(text.as_bytes()),
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Binary(payload))) => {
                        self.capture_rejected_protocol_frame(
                            TransportFrameKind::Binary,
                            payload,
                            output,
                        )?;
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                    Some(Ok(Message::Ping(_) | Message::Frame(_))) | Some(Err(_)) => {
                        return Err(CoinbaseDirectSessionError::Shutdown);
                    }
                }
            }
        };
        tokio::time::timeout(deadline, operation)
            .await
            .map_err(|_elapsed| CoinbaseDirectSessionError::Shutdown)?
    }
}

impl SourceMetadataProvider for CoinbaseDirectSession {
    fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

fn direct_http_bounds(
    config: &CoinbaseDirectConfig,
) -> Result<market_squawk_sources::HttpRequestBounds, CoinbaseDirectSessionError> {
    match config.metadata().network_policy() {
        NetworkAccessPolicy::Allowlisted(policy) => Ok(policy.request_bounds()),
        NetworkAccessPolicy::Denied => Err(SourceError::InvalidProtocolState.into()),
    }
}

fn current_unix_seconds() -> Result<u64, CoinbaseDirectSigningError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .filter(|seconds| *seconds > 0)
        .ok_or(CoinbaseDirectSigningError::InvalidTimestamp)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundMode {
    AwaitingAck,
    Queueing,
    Live,
    PublicationFrontier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MessageDisposition {
    Acknowledged,
    Control,
    NonBook,
    Sequenced,
}

#[derive(Clone, Debug)]
enum LiveRead {
    Message(Message),
    RefreshProduct,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAck {
    #[serde(rename = "type")]
    kind: String,
    channels: [SubscriptionAckChannel; 1],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionAckChannel {
    name: String,
    product_ids: [String; 1],
}

fn validate_subscription_ack(
    payload: &[u8],
    product: &market_squawk_domain::ProviderProduct,
) -> Result<(), CoinbaseDirectSessionError> {
    let ack: SubscriptionAck = serde_json::from_slice(payload)
        .map_err(|_error| CoinbaseDirectSessionError::Subscription)?;
    let channel = &ack.channels[0];
    if ack.kind != "subscriptions"
        || channel.name != "full"
        || channel.product_ids[0] != product.as_source_identifier().as_str()
    {
        return Err(CoinbaseDirectSessionError::Subscription);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct ReceiveDeadline {
    at: Instant,
    sink_owned: bool,
}

impl ReceiveDeadline {
    fn strictest(
        output: &dyn CoinbaseDirectOutput,
        transport_timeout: Duration,
    ) -> Result<Self, SourceError> {
        let transport = Instant::now()
            .checked_add(transport_timeout)
            .ok_or(SourceError::InvalidProtocolState)?;
        match output.next_deadline() {
            Some(sink_deadline) if sink_deadline <= transport => Ok(Self {
                at: sink_deadline,
                sink_owned: true,
            }),
            _ => Ok(Self {
                at: transport,
                sink_owned: false,
            }),
        }
    }
}

async fn read_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    output: &mut dyn CoinbaseDirectOutput,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
) -> Result<Message, CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = ReceiveDeadline::strictest(output, transport_timeout)?;
    let next = tokio::select! {
        biased;
        () = cancellation.cancelled() => return Err(SourceError::Cancelled.into()),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline.at)) => {
            if deadline.sink_owned {
                output
                    .poll_deadline(Instant::now())
                    .map_err(SourceError::Sink)?;
                return Err(SourceError::InvalidProtocolState.into());
            }
            return Err(SourceError::ConnectionIdle.into());
        }
        result = socket.next() => result,
    };
    map_next_message(next)
}

async fn read_with_refresh_deadline<S>(
    socket: &mut WebSocketStream<S>,
    output: &mut dyn CoinbaseDirectOutput,
    cancellation: &CancellationToken,
    transport_timeout: Duration,
    refresh_at: Instant,
) -> Result<LiveRead, CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let receive_deadline = ReceiveDeadline::strictest(output, transport_timeout)?;
    if refresh_at >= receive_deadline.at {
        return read_with_deadline(socket, output, cancellation, transport_timeout)
            .await
            .map(LiveRead::Message);
    }

    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled.into()),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(refresh_at)) => {
            Ok(LiveRead::RefreshProduct)
        }
        message = socket.next() => map_next_message(message).map(LiveRead::Message),
    }
}

fn map_next_message(
    message: Option<Result<Message, WebSocketError>>,
) -> Result<Message, CoinbaseDirectSessionError> {
    match message {
        Some(Ok(message)) => Ok(message),
        Some(Err(_error)) => Err(SourceError::Network.into()),
        None => Err(SourceError::ProviderUnavailable.into()),
    }
}

async fn await_websocket<T, E, F>(
    cancellation: &CancellationToken,
    deadline: Duration,
    operation: impl Future<Output = Result<T, E>>,
    map_error: F,
) -> Result<T, CoinbaseDirectSessionError>
where
    F: FnOnce(E) -> SourceError,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SourceError::Cancelled.into()),
        result = tokio::time::timeout(deadline, operation) => match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(map_error(error).into()),
            Err(_elapsed) => Err(SourceError::Network.into()),
        }
    }
}

async fn send_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.send(message), |_error| {
        SourceError::Network
    })
    .await
}

async fn flush_with_deadline<S>(
    socket: &mut WebSocketStream<S>,
    cancellation: &CancellationToken,
    deadline: Duration,
) -> Result<(), CoinbaseDirectSessionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    await_websocket(cancellation, deadline, socket.flush(), |_error| {
        SourceError::Network
    })
    .await
}

fn ensure_frame_bound(actual: usize, maximum: usize) -> Result<(), CoinbaseDirectSessionError> {
    if actual > maximum {
        Err(SourceError::FrameTooLarge { max: maximum }.into())
    } else {
        Ok(())
    }
}

fn validate_sequenced_progression(
    previous: SequenceNumber,
    observed: SequenceNumber,
) -> Result<(), CoinbaseDirectSessionError> {
    if observed == previous {
        return Err(DirectOrderBookError::DuplicateSequence.into());
    }
    if observed < previous {
        return Err(DirectOrderBookError::SequenceRegression.into());
    }
    let expected = previous
        .checked_next()
        .map_err(|_error| DirectOrderBookError::SequenceExhausted)?;
    if observed != expected {
        return Err(DirectOrderBookError::SequenceGap.into());
    }
    Ok(())
}

fn handoff_frontier_payload(
    receipt: &SegmentedHttpResponseReceipt,
    generation: ConnectionGeneration,
) -> Bytes {
    frontier_payload(receipt, generation, *b"MSQCBF01")
}

fn publication_frontier_payload(
    receipt: &SegmentedHttpResponseReceipt,
    generation: ConnectionGeneration,
) -> Bytes {
    frontier_payload(receipt, generation, *b"MSQCBF02")
}

fn frontier_payload(
    receipt: &SegmentedHttpResponseReceipt,
    generation: ConnectionGeneration,
    domain: [u8; 8],
) -> Bytes {
    let mut payload = [0_u8; 56];
    payload[..8].copy_from_slice(&domain);
    payload[8..40].copy_from_slice(&receipt.body_digest().bytes());
    payload[40..48].copy_from_slice(&receipt.received_at().unix_nanos().to_be_bytes());
    payload[48..].copy_from_slice(&generation.get().to_be_bytes());
    Bytes::copy_from_slice(&payload)
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn map_http_transport_error(error: CoinbaseDirectHttpTransportError) -> CoinbaseDirectSessionError {
    match error {
        CoinbaseDirectHttpTransportError::Network => SourceError::Network.into(),
        CoinbaseDirectHttpTransportError::Deadline => CoinbaseDirectSessionError::HttpDeadline,
        CoinbaseDirectHttpTransportError::Cancelled => SourceError::Cancelled.into(),
        CoinbaseDirectHttpTransportError::BodyTooLarge => {
            CoinbaseDirectSessionError::HttpBodyTooLarge
        }
        CoinbaseDirectHttpTransportError::SegmentLimit => {
            CoinbaseDirectSessionError::HttpSegmentLimit
        }
        CoinbaseDirectHttpTransportError::Protocol
        | CoinbaseDirectHttpTransportError::Allocation => CoinbaseDirectSessionError::HttpResponse,
    }
}

fn map_connect_error(error: WebSocketError, budget: &SharedProviderBudget) -> SourceError {
    if let WebSocketError::Http(response) = &error {
        let status = response.status();
        if matches!(status.as_u16(), 401 | 403) {
            return SourceError::Unauthorized;
        }
        if status.as_u16() == 429 || status.is_server_error() {
            return SourceError::from_applied_budget_refusal(apply_http_retry_after(
                budget,
                response
                    .headers()
                    .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                    .map(|value| value.as_bytes()),
                1_000,
            ));
        }
    }
    SourceError::Network
}

#[cfg(test)]
#[path = "direct_transport/tests.rs"]
mod tests;
