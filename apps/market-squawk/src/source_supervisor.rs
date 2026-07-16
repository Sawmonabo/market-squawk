//! App-owned source-session and capture-generation supervision.

use std::time::Duration;

use market_squawk_platform::{CaptureGenerationKey, RawCaptureControl, RawCapturePublisher};
use tokio::sync::{mpsc, watch};

use crate::{
    MarketEvent,
    source::{CaptureContext, MarketSource, SourceRunOutcome},
};

/// Sole application owner of positive capture-allocation transitions.
#[derive(Debug)]
pub struct SourceSupervisor {
    publisher: RawCapturePublisher,
    control: RawCaptureControl,
    key: CaptureGenerationKey,
    maximum_backoff: Duration,
}

impl SourceSupervisor {
    /// Binds an already activated initial allocation to source-session supervision.
    pub const fn new(
        publisher: RawCapturePublisher,
        control: RawCaptureControl,
        key: CaptureGenerationKey,
    ) -> Self {
        Self {
            publisher,
            control,
            key,
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
            let context = CaptureContext::new(self.publisher.clone(), self.key.clone());
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
            let generation = self.key.generation().checked_next()?;
            let next = CaptureGenerationKey::new(
                self.key.source_id().clone(),
                self.key.metadata_revision().clone(),
                self.key.session_id().clone(),
                generation,
                uuid::Uuid::new_v4(),
            );
            self.control.rotate_generation(next.clone())?;
            self.key = next;
        }
    }
}
