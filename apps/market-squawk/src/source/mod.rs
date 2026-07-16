pub mod coinbase;
pub mod mock;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_platform::{
    CaptureAdmissionReceipt, CaptureGenerationKey, RawCapturePublisher, RawCaptureRecord,
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::domain::MarketEvent;

/// Exact source binding and nonblocking raw-capture publisher supplied by application composition.
#[derive(Clone, Debug)]
pub struct CaptureContext {
    publisher: RawCapturePublisher,
    key: CaptureGenerationKey,
}

impl CaptureContext {
    /// Binds one source session/generation to its supervised capture publisher.
    pub const fn new(publisher: RawCapturePublisher, key: CaptureGenerationKey) -> Self {
        Self { publisher, key }
    }

    /// Returns the raw-wire connection identity coupled to the active generation.
    pub const fn connection_id(&self) -> Uuid {
        self.key.connection_id()
    }

    /// Publishes exact frame bytes synchronously before source decode.
    pub fn publish(
        &self,
        event_id: Uuid,
        source: std::sync::Arc<str>,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: Bytes,
    ) -> anyhow::Result<CaptureAdmissionReceipt> {
        let record = RawCaptureRecord::try_new_live(
            event_id,
            source,
            self.key.connection_id(),
            source_sequence,
            exchange_at,
            received_at,
            payload,
        )?;
        Ok(self.publisher.try_publish(&self.key, record)?)
    }
}

/// Typed single-session result consumed only by the app-owned reconnect supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceRunOutcome {
    /// The finite source completed normally.
    Completed,
    /// Cancellation ended the source session.
    Cancelled,
    /// Transport/session failure requires a new generation and capture allocation.
    ReconnectRequired,
}

#[async_trait]
pub trait MarketSource: Send {
    async fn run_session(
        &mut self,
        capture: CaptureContext,
        events: mpsc::Sender<MarketEvent>,
        cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<SourceRunOutcome>;
}
