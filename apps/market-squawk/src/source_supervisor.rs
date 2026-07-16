//! App-owned source-session and capture-generation supervision.

use std::time::Duration;

use market_squawk_domain::CaptureAuthorityIdentity;
use market_squawk_platform::{DiagnosticCaptureBundle, RawCaptureControl, RawCapturePublisher};
use tokio::sync::{mpsc, watch};

use crate::{
    domain::MarketEvent,
    source::{CaptureContext, MarketSource, SourceRunOutcome},
};

/// Sole application owner of positive capture-allocation transitions.
#[derive(Debug)]
pub struct SourceSupervisor {
    publisher: RawCapturePublisher<DiagnosticCaptureBundle>,
    control: RawCaptureControl<DiagnosticCaptureBundle>,
    identity: CaptureAuthorityIdentity,
    connection_id: uuid::Uuid,
    maximum_backoff: Duration,
}

impl SourceSupervisor {
    /// Binds an already activated initial allocation to source-session supervision.
    pub const fn new(
        publisher: RawCapturePublisher<DiagnosticCaptureBundle>,
        control: RawCaptureControl<DiagnosticCaptureBundle>,
        identity: CaptureAuthorityIdentity,
        connection_id: uuid::Uuid,
    ) -> Self {
        Self {
            publisher,
            control,
            identity,
            connection_id,
            maximum_backoff: Duration::from_secs(30),
        }
    }

    /// Runs sessions, and only on a typed reconnect outcome rotates to a fresh allocation.
    pub async fn run(
        mut self,
        mut source: Box<dyn MarketSource>,
        events: mpsc::Sender<MarketEvent>,
        mut cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            let context = CaptureContext::new(
                self.publisher.clone(),
                self.identity.clone(),
                self.connection_id,
            );
            match source
                .run_session(context, events.clone(), cancel.clone())
                .await?
            {
                SourceRunOutcome::Completed | SourceRunOutcome::Cancelled => return Ok(()),
                SourceRunOutcome::ReconnectRequired => {}
            }
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = backoff.saturating_mul(2).min(self.maximum_backoff);
            let generation = self.identity.connection_generation().checked_next()?;
            let next = CaptureAuthorityIdentity::new(
                self.identity.source_id().clone(),
                self.identity.metadata_revision().clone(),
                self.identity.session_identifier().clone(),
                generation,
            );
            self.control
                .rotate_generation(DiagnosticCaptureBundle::new(next.clone()))?;
            self.identity = next;
            self.connection_id = uuid::Uuid::new_v4();
        }
    }
}
