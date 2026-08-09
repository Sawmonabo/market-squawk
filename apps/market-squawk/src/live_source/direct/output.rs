//! Capture-receipt pairing and current-product qualification for one Direct product owner.

use std::time::{Duration, Instant};

use market_squawk_adapter_coinbase::{
    CoinbaseDirectBookUpdate, CoinbaseDirectNonBookEvent, CoinbaseDirectOrderLevelPayload,
    CoinbaseDirectOrderLevelUpdate, CoinbaseDirectOutput, CoinbaseDirectProductEvidence,
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
    composition::system_timestamp, order_level::OrderLevelIngress, sink::ProductionRawMarketSink,
};

#[derive(Debug)]
struct CapturedFrame {
    frame_id: FrameId,
    receipt: CaptureAdmissionReceipt,
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
    /// The synchronized book could not produce one bounded canonical publication.
    #[error("Coinbase Direct book publication is invalid")]
    Publication,
    /// The order-identity-preserving publication could not enter the exact generation actor.
    #[error("Coinbase Direct order-level publication is invalid")]
    OrderLevelPublication,
}

/// Same-task output owner that pairs adapter evidence with registry capture receipts.
#[derive(Debug)]
pub(super) struct CoinbaseDirectProductOutput<'sink, 'registry> {
    sink: &'sink mut ProductionRawMarketSink<'registry>,
    product: ProviderProduct,
    last_capture: Option<CapturedFrame>,
    sequenced_capture: Option<CapturedFrame>,
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
    ) -> Self {
        Self {
            sink,
            product,
            last_capture: None,
            sequenced_capture: None,
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
        let receipt = self.sink.try_capture_predecoded(&frame)?;
        self.last_capture = Some(CapturedFrame { frame_id, receipt });
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
        let captured = self.take_last_capture(evidence)?;
        self.sequenced_capture = Some(captured);
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
        let Some(captured) = self.sequenced_capture.as_ref() else {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        };
        if captured.frame_id != update.decoder_evidence().frame_id() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
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

    fn try_publish_book(&mut self, update: CoinbaseDirectBookUpdate<'_>) -> Result<(), SinkError> {
        if !self.product_active {
            return Err(self.fail(CoinbaseDirectOutputFailure::ProductUnavailable));
        }
        let batch = update
            .try_publication_batch()
            .map_err(|_error| self.fail(CoinbaseDirectOutputFailure::Publication))?;
        let captured = self
            .sequenced_capture
            .take()
            .ok_or_else(|| self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch))?;
        if captured.frame_id != batch.evidence().frame_id() {
            return Err(self.fail(CoinbaseDirectOutputFailure::EvidenceMismatch));
        }
        self.sink
            .try_process_captured_outcome(DecodeOutcome::Data(batch), captured.receipt)?;
        self.bootstrap_permit.take();
        Ok(())
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
