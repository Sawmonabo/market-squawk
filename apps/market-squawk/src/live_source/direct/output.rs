//! Capture-receipt pairing and current-product qualification for one Direct product owner.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use market_squawk_adapter_coinbase::{
    CoinbaseDirectNonBookEvent, CoinbaseDirectOrderLevelPayload, CoinbaseDirectOrderLevelUpdate,
    CoinbaseDirectOutput, CoinbaseDirectOutputAdmission, CoinbaseDirectProductEvidence,
    CoinbaseMarketFeed, CoinbaseMarketHandoff, CoinbaseMarketPhysicalCaptureIdentity,
    CoinbaseMarketPublicationContext, CoinbaseMarketRawLineage,
};
use market_squawk_domain::{
    ChecksumEvidence, DataQuality, ProviderProduct, SequenceCapability, SequenceEvidence,
    SequenceNumber, SourceIdentifier, TradingStatus,
};
use market_squawk_live::{
    OrderLevelBatch, OrderLevelBatchInput, OrderLevelBatchPayload, OrderLevelRoute,
    provider_snapshot_orders, sequenced_provider_event,
};
use market_squawk_sources::{
    CaptureAdmissionReceipt, ChecksumValidationProfile, DecodeOutcome, DecodedControlFrame,
    DecoderEvidence, FrameId, MarketFreshness, RawMarketFrame, RawMarketSink,
    SequenceValidationProfile, SinkError, SourceProtocolProfile,
};
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

use super::super::{
    composition::system_timestamp,
    order_level::OrderLevelIngress,
    sink::{CoinbaseCapturedPublicationIngress, ProductionRawMarketSink},
};

#[derive(Debug)]
struct CapturedFrame {
    frame_id: FrameId,
    wire_bytes: usize,
    receipt: CaptureAdmissionReceipt,
    frame: RawMarketFrame,
}

/// Fail-closed Direct-specific output qualification failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CoinbaseDirectOutputFailure {
    /// A callback was absent, duplicated, reordered, or paired with another captured frame.
    #[error("Coinbase Direct output evidence does not match the captured frame")]
    EvidenceMismatch,
    /// Product evidence belongs to another configured product.
    #[error("Coinbase Direct product evidence does not match the configured product")]
    ProductMismatch,
    /// Current product evidence does not permit active trading.
    #[error("Coinbase Direct product is not currently active for trading")]
    ProductUnavailable,
    /// The order-identity-preserving publication could not enter the exact generation actor.
    #[error("Coinbase Direct order-level publication is invalid")]
    OrderLevelPublication,
    /// The application output could not honor the adapter's complete pre-network replay bound.
    #[error("Coinbase Direct output replay admission is invalid")]
    ReplayAdmission,
}

/// Same-task output owner that pairs adapter evidence with registry capture receipts.
#[derive(Debug)]
pub(super) struct CoinbaseDirectProductOutput<'sink, 'registry> {
    sink: &'sink mut ProductionRawMarketSink<'registry>,
    product: ProviderProduct,
    last_capture: Option<CapturedFrame>,
    sequenced_captures: VecDeque<CapturedFrame>,
    sequenced_capture_bytes: usize,
    replay_admission: Option<CoinbaseDirectOutputAdmission>,
    publication: CoinbaseCapturedPublicationIngress,
    publication_dataset: SourceIdentifier,
    publication_stream: SourceIdentifier,
    product_active: bool,
    bootstrap_permit: Option<OwnedSemaphorePermit>,
    order_level: Option<OrderLevelIngress>,
    order_level_publish_timeout: Option<Duration>,
    order_level_snapshot_sequence: Option<SequenceNumber>,
    order_level_last_sequence: Option<SequenceNumber>,
    terminal: Option<CoinbaseDirectOutputFailure>,
}

impl<'sink, 'registry> CoinbaseDirectProductOutput<'sink, 'registry> {
    pub(super) fn new(
        sink: &'sink mut ProductionRawMarketSink<'registry>,
        product: ProviderProduct,
        bootstrap_permit: OwnedSemaphorePermit,
        order_level: Option<OrderLevelIngress>,
        order_level_publish_timeout: Option<Duration>,
        publication: CoinbaseCapturedPublicationIngress,
        publication_dataset: SourceIdentifier,
        publication_stream: SourceIdentifier,
    ) -> Self {
        Self {
            sink,
            product,
            last_capture: None,
            sequenced_captures: VecDeque::new(),
            sequenced_capture_bytes: 0,
            replay_admission: None,
            publication,
            publication_dataset,
            publication_stream,
            product_active: false,
            bootstrap_permit: Some(bootstrap_permit),
            order_level,
            order_level_publish_timeout,
            order_level_snapshot_sequence: None,
            order_level_last_sequence: None,
            terminal: None,
        }
    }

    pub(super) const fn terminal_failure(&self) -> Option<CoinbaseDirectOutputFailure> {
        self.terminal
    }

    fn fail(&mut self, failure: CoinbaseDirectOutputFailure) -> SinkError {
        if self.terminal.is_none() {
            self.terminal = Some(failure);
        }
        SinkError::CaptureIncomplete
    }

    fn take_last_capture(
        &mut self,
        evidence: &DecoderEvidence,
    ) -> Result<CapturedFrame, SinkError> {
        let captured = self
            .last_capture
            .take()
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        if captured.frame_id != evidence.frame_id() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        Ok(captured)
    }
}

impl RawMarketSink for CoinbaseDirectProductOutput<'_, '_> {
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError> {
        if self.terminal.is_some() || self.last_capture.is_some() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let frame_id = frame.frame_id();
        let wire_bytes = frame.payload().len();
        let receipt = self.sink.try_capture_predecoded(&frame)?;
        self.last_capture = Some(CapturedFrame {
            frame_id,
            wire_bytes,
            receipt,
            frame,
        });
        Ok(())
    }

    fn next_deadline(&self) -> Option<Instant> {
        RawMarketSink::next_deadline(self.sink)
    }

    fn poll_deadline(&mut self, now: Instant) -> Result<(), SinkError> {
        RawMarketSink::poll_deadline(self.sink, now)
    }
}

impl CoinbaseDirectOutput for CoinbaseDirectProductOutput<'_, '_> {
    fn try_admit_replay(
        &mut self,
        admission: CoinbaseDirectOutputAdmission,
    ) -> Result<(), SinkError> {
        if self.terminal.is_some()
            || self.replay_admission.is_some()
            || self.last_capture.is_some()
            || !self.sequenced_captures.is_empty()
            || admission.maximum_events() == 0
            || admission.maximum_raw_bytes() == 0
            || admission.complete_retained_bytes() == 0
        {
            return Err(self.fail(CoinbaseDirectOutputFailure::ReplayAdmission));
        }
        self.sequenced_captures
            .try_reserve_exact(admission.maximum_events())
            .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
        if self.sequenced_captures.capacity() > admission.maximum_container_slots() {
            return Err(self.fail(CoinbaseDirectOutputFailure::ReplayAdmission));
        }
        self.replay_admission = Some(admission);
        Ok(())
    }

    fn try_publish_subscription_acknowledgement(
        &mut self,
        acknowledgement: DecodedControlFrame,
    ) -> Result<(), SinkError> {
        let captured = self.take_last_capture(acknowledgement.evidence())?;
        self.sink
            .try_process_captured_outcome(DecodeOutcome::Control(acknowledgement), captured.receipt)
    }

    fn try_publish_product(
        &mut self,
        evidence: CoinbaseDirectProductEvidence,
    ) -> Result<(), SinkError> {
        if evidence.product() != &self.product {
            return Err(self.fail(CoinbaseDirectOutputFailure::ProductMismatch));
        }
        if evidence.trading_status() != TradingStatus::Active
            || evidence.trading_disabled()
            || evidence.cancel_only()
            || evidence.post_only()
            || evidence.limit_only()
            || evidence.auction_mode()
        {
            return Err(self.fail(CoinbaseDirectOutputFailure::ProductUnavailable));
        }
        self.product_active = true;
        Ok(())
    }

    fn try_publish_non_book(&mut self, event: CoinbaseDirectNonBookEvent) -> Result<(), SinkError> {
        let _captured = self.take_last_capture(event.evidence())?;
        Ok(())
    }

    fn try_retain_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError> {
        if self.terminal.is_some() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let admission = self
            .replay_admission
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
        let captured = self.take_last_capture(evidence)?;
        if captured.wire_bytes != evidence.frame_bytes()
            || self.sequenced_captures.len() >= admission.maximum_events()
        {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let next_bytes = self
            .sequenced_capture_bytes
            .checked_add(captured.wire_bytes)
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
        if next_bytes > admission.maximum_raw_bytes() {
            return Err(self.fail(CoinbaseDirectOutputFailure::ReplayAdmission));
        }
        self.sequenced_captures.push_back(captured);
        self.sequenced_capture_bytes = next_bytes;
        Ok(())
    }

    fn try_discard_sequenced_frame(&mut self, evidence: &DecoderEvidence) -> Result<(), SinkError> {
        if self.terminal.is_some() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        if self.last_capture.is_some() {
            let _discarded = self.take_last_capture(evidence)?;
            return Ok(());
        }
        let discarded = self
            .sequenced_captures
            .pop_front()
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        if discarded.frame_id != evidence.frame_id() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        self.sequenced_capture_bytes = self
            .sequenced_capture_bytes
            .checked_sub(discarded.wire_bytes)
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        Ok(())
    }

    fn try_publish_order_level(
        &mut self,
        update: CoinbaseDirectOrderLevelUpdate<'_>,
    ) -> Result<(), SinkError> {
        if !self.product_active || update.product() != &self.product {
            return Err(self.fail(CoinbaseDirectOutputFailure::ProductUnavailable));
        }
        if update.validate_current().is_err() {
            return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
        }
        let Some(captured) = self.sequenced_captures.back() else {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        };
        if captured.frame_id != update.decoder_evidence().frame_id() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        if matches!(
            update.payload(),
            CoinbaseDirectOrderLevelPayload::Snapshot { .. }
        ) {
            let batch = update
                .try_snapshot_http_batch()
                .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication))?;
            self.sink.try_process_http_response_batch(batch)?;
        }
        let Some(ingress) = self.order_level.as_ref() else {
            return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
        };
        let Some(publish_timeout) = self.order_level_publish_timeout else {
            return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
        };
        let available_at = match system_timestamp() {
            Ok(available_at) => available_at,
            Err(_error) => {
                return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
            }
        };
        let built = match build_order_level_batch(
            update,
            ingress,
            self.order_level_snapshot_sequence,
            self.order_level_last_sequence,
            available_at,
        ) {
            Ok(built) => built,
            Err(()) => {
                return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
            }
        };
        let deadline = match Instant::now().checked_add(publish_timeout) {
            Some(deadline) => deadline,
            None => return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication)),
        };
        if ingress.try_publish(built.batch, deadline).is_err() {
            return Err(self.fail(CoinbaseDirectOutputFailure::OrderLevelPublication));
        }
        self.order_level_snapshot_sequence = Some(built.snapshot_sequence);
        self.order_level_last_sequence = Some(built.terminal_sequence);
        Ok(())
    }

    fn try_publish_book(&mut self, handoff: CoinbaseMarketHandoff) -> Result<(), SinkError> {
        if !self.product_active {
            return Err(self.fail(CoinbaseDirectOutputFailure::ProductUnavailable));
        }
        if handoff.evidence().feed() != CoinbaseMarketFeed::ExchangeDirectFull {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let CoinbaseMarketRawLineage::DirectInitial(lineage) = handoff.raw_lineage() else {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        };
        let admission = self
            .replay_admission
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
        let mut captures = std::mem::take(&mut self.sequenced_captures);
        let captured_bytes = std::mem::replace(&mut self.sequenced_capture_bytes, 0);
        if captures.len() != lineage.replay().len() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let mut event_ids = Vec::new();
        event_ids
            .try_reserve_exact(lineage.replay().len().saturating_add(1))
            .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        if event_ids.capacity() > admission.maximum_container_slots() {
            return Err(self.fail(CoinbaseDirectOutputFailure::ReplayAdmission));
        }
        let mut claimed_bytes = 0_usize;
        let mut connection_id = None;
        let mut terminal = None;
        for replay in lineage.replay() {
            let mut matched = captures
                .pop_front()
                .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
            if matched.frame_id != replay.decoder_evidence().frame_id()
                || matched.wire_bytes != replay.decoder_evidence().frame_bytes()
            {
                return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
            }
            claimed_bytes = claimed_bytes
                .checked_add(matched.wire_bytes)
                .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
            let claim = matched
                .receipt
                .try_issue_provider_event_identity_claim(&matched.frame)
                .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
            if claim.frame_id() != matched.frame_id
                || claim.payload_digest() != replay.decoder_evidence().payload_digest()
                || claim.received_at() != replay.decoder_evidence().received_at()
                || !claim
                    .binding()
                    .shares_allocation_with(replay.decoder_evidence().binding())
            {
                return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
            }
            match connection_id {
                None => connection_id = Some(claim.connection_id()),
                Some(expected) if expected == claim.connection_id() => {}
                Some(_) => {
                    return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
                }
            }
            event_ids.push(*claim.event_id().as_bytes());
            terminal = Some(matched);
        }
        if !captures.is_empty()
            || claimed_bytes != captured_bytes
            || terminal
                .as_ref()
                .is_none_or(|claim| claim.frame_id != handoff.typed_batch().evidence().frame_id())
        {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        let terminal_batch = handoff.typed_batch().clone();
        let terminal =
            terminal.ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        let connection_id = connection_id
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        event_ids.insert(0, *lineage.snapshot().receipt().event_id().as_bytes());
        let physical =
            CoinbaseMarketPhysicalCaptureIdentity::try_new(*connection_id.as_bytes(), event_ids)
                .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        let context = CoinbaseMarketPublicationContext::new(
            self.publication_dataset.clone(),
            self.publication_stream.clone(),
            physical,
        );
        let observed_at = system_timestamp()
            .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        self.publication
            .try_submit_direct(handoff, context, observed_at)
            .map_err(|_input| self.fail(CoinbaseDirectOutputFailure::ReplayAdmission))?;
        self.sink
            .try_process_captured_outcome(DecodeOutcome::Data(terminal_batch), terminal.receipt)
    }
}

struct BuiltOrderLevelBatch {
    batch: OrderLevelBatch,
    snapshot_sequence: SequenceNumber,
    terminal_sequence: SequenceNumber,
}

fn build_order_level_batch(
    update: CoinbaseDirectOrderLevelUpdate<'_>,
    ingress: &OrderLevelIngress,
    retained_snapshot: Option<SequenceNumber>,
    retained_previous: Option<SequenceNumber>,
    available_at: market_squawk_domain::Timestamp,
) -> Result<BuiltOrderLevelBatch, ()> {
    let SourceProtocolProfile::Live(protocol) = update.metadata().protocol_profile() else {
        return Err(());
    };
    let SequenceValidationProfile::Provided { rule, progression } = protocol.sequence() else {
        return Err(());
    };
    if !matches!(
        protocol.checksum(),
        ChecksumValidationProfile::Unsupported { .. }
    ) {
        return Err(());
    }
    let route = OrderLevelRoute::new(
        ingress.key().source_id().clone(),
        ingress.key().venue_id().clone(),
        ingress.key().instrument_id(),
        update.product().as_source_identifier().clone(),
        ingress.key().generation(),
    );
    let generation = update.connection_generation();
    if generation != ingress.key().generation() {
        return Err(());
    }
    let (
        payload,
        snapshot_sequence,
        terminal_sequence,
        previous_sequence,
        source_timestamp,
        received_at,
    ) = match update.payload() {
        CoinbaseDirectOrderLevelPayload::Snapshot {
            snapshot_sequence,
            snapshot_timestamp,
            orders,
            replay,
        } => {
            if retained_snapshot.is_some() || retained_previous.is_some() {
                return Err(());
            }
            let orders = provider_snapshot_orders(orders).map_err(|_error| ())?;
            let mut events = Vec::new();
            events
                .try_reserve_exact(replay.len())
                .map_err(|_error| ())?;
            for event in replay {
                events.push(sequenced_provider_event(event).map_err(|_error| ())?);
            }
            let terminal_sequence = replay
                .last()
                .map_or(snapshot_sequence, |event| event.sequence());
            let previous_sequence = match replay.len() {
                0 => None,
                1 => Some(snapshot_sequence),
                length => Some(replay[length - 2].sequence()),
            };
            let source_timestamp = replay
                .last()
                .map_or(snapshot_timestamp, |event| event.timestamp());
            let received_at = replay
                .last()
                .map_or(update.snapshot_receipt().received_at(), |event| {
                    event.evidence().received_at()
                });
            (
                OrderLevelBatchPayload::Snapshot {
                    snapshot_source_timestamp: snapshot_timestamp,
                    snapshot_received_at: update.snapshot_receipt().received_at(),
                    orders,
                    replay: events,
                },
                snapshot_sequence,
                terminal_sequence,
                previous_sequence,
                source_timestamp,
                received_at,
            )
        }
        CoinbaseDirectOrderLevelPayload::Event(event) => {
            let snapshot_sequence = retained_snapshot.ok_or(())?;
            let previous_sequence = retained_previous.ok_or(())?;
            let terminal_sequence = event.sequence();
            let mut events = Vec::new();
            events.try_reserve_exact(1).map_err(|_error| ())?;
            events.push(sequenced_provider_event(event).map_err(|_error| ())?);
            (
                OrderLevelBatchPayload::Update { events },
                snapshot_sequence,
                terminal_sequence,
                Some(previous_sequence),
                event.timestamp(),
                event.evidence().received_at(),
            )
        }
    };
    let sequence = SequenceEvidence::validate(
        SequenceCapability::Provided,
        Some(rule.clone()),
        *progression,
        generation,
        Some(snapshot_sequence),
        previous_sequence,
        Some(terminal_sequence),
    )
    .map_err(|_error| ())?;
    let batch_identifier = SourceIdentifier::try_from(format!(
        "coinbase-l3-g{}-f{}",
        generation.get(),
        update.decoder_evidence().frame_id().get()
    ))
    .map_err(|_error| ())?;
    let batch = OrderLevelBatch::try_new(OrderLevelBatchInput::new(
        route,
        batch_identifier,
        source_timestamp,
        received_at,
        available_at,
        DataQuality::DirectUnverified,
        MarketFreshness::Fresh {
            last_market_at: received_at,
        },
        Some(*progression),
        sequence,
        ChecksumEvidence::unsupported(generation),
        None,
        payload,
    ))
    .map_err(|_error| ())?;
    Ok(BuiltOrderLevelBatch {
        batch,
        snapshot_sequence,
        terminal_sequence,
    })
}
