pub mod coinbase;
pub mod mock;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{CaptureAuthorityIdentity, Timestamp};
use market_squawk_platform::{
    DiagnosticCaptureBundle, DiagnosticCaptureFrame, DiagnosticCaptureReceipt, RawCapturePublisher,
    RawCaptureRecord,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::MarketEvent;

/// Exact source binding and nonblocking raw-capture publisher supplied by application composition.
#[derive(Debug)]
pub struct CaptureContext {
    publisher: RawCapturePublisher<DiagnosticCaptureBundle>,
    identity: CaptureAuthorityIdentity,
    connection_id: Uuid,
    next_ordinal: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl CaptureContext {
    /// Binds one source session/generation to its supervised capture publisher.
    pub fn new(
        publisher: RawCapturePublisher<DiagnosticCaptureBundle>,
        identity: CaptureAuthorityIdentity,
        connection_id: Uuid,
    ) -> Self {
        Self {
            publisher,
            identity,
            connection_id,
            next_ordinal: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Returns the raw-wire connection identity coupled to the active generation.
    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
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
    ) -> anyhow::Result<DiagnosticCaptureReceipt> {
        let record = RawCaptureRecord::try_new_live(
            event_id,
            source,
            self.connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
        )?;
        if record.source() != self.identity.source_id().as_str() {
            anyhow::bail!("diagnostic capture source differs from the supervised source identity");
        }
        let previous = self
            .next_ordinal
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |current| current.checked_add(1),
            )
            .map_err(|_exhausted| anyhow::anyhow!("diagnostic frame ordinal exhausted"))?;
        let ordinal = std::num::NonZeroU64::new(
            previous
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("diagnostic frame ordinal exhausted"))?,
        )
        .ok_or_else(|| anyhow::anyhow!("diagnostic frame ordinal must be nonzero"))?;
        let received_nanos = record
            .received_at()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("receive timestamp is outside domain range"))?;
        let frame = DiagnosticCaptureFrame::try_new(
            self.identity.clone(),
            ordinal,
            Timestamp::from_unix_nanos(received_nanos),
            Bytes::copy_from_slice(record.payload()),
        )?;
        Ok(self.publisher.try_publish(&frame)?)
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
        cancellation: CancellationToken,
    ) -> anyhow::Result<SourceRunOutcome>;
}

pub(crate) async fn send_event_until_cancelled(
    events: &mpsc::Sender<MarketEvent>,
    event: MarketEvent,
    cancellation: &CancellationToken,
) -> anyhow::Result<bool> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(false),
        result = events.send(event) => {
            result?;
            Ok(true)
        }
    }
}
