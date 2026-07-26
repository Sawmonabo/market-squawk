//! Capture-receipt pairing and current-product qualification for one Direct product owner.

use std::time::Instant;

use market_squawk_adapter_coinbase::{
    CoinbaseDirectBookUpdate, CoinbaseDirectNonBookEvent, CoinbaseDirectOutput,
    CoinbaseDirectProductEvidence,
};
use market_squawk_domain::{ProviderProduct, TradingStatus};
use market_squawk_sources::{
    CaptureAdmissionReceipt, DecodeOutcome, DecodedControlFrame, DecoderEvidence, FrameId,
    RawMarketFrame, RawMarketSink, SinkError,
};
use thiserror::Error;
use tokio::sync::OwnedSemaphorePermit;

use super::super::sink::ProductionRawMarketSink;

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
    terminal: Option<CoinbaseDirectOutputFailure>,
}

impl<'sink, 'registry> CoinbaseDirectProductOutput<'sink, 'registry> {
    pub(super) fn new(
        sink: &'sink mut ProductionRawMarketSink<'registry>,
        product: ProviderProduct,
        bootstrap_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            sink,
            product,
            last_capture: None,
            sequenced_capture: None,
            product_active: false,
            bootstrap_permit: Some(bootstrap_permit),
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
