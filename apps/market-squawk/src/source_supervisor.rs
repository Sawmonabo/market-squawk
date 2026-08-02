//! App-owned source-session and capture-generation supervision.

use std::{future::Future, time::Duration};

use market_squawk_domain::CaptureAuthorityIdentity;
use market_squawk_platform::{DiagnosticCaptureBundle, RawCaptureControl, RawCapturePublisher};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

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
        cancellation: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            let context = CaptureContext::new(
                self.publisher.try_clone()?,
                self.identity.clone(),
                self.connection_id,
            );
            let session = source.run_session(context, events.clone(), cancellation.child_token());
            let outcome = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
                result = session => result?,
            };
            match outcome {
                SourceRunOutcome::Completed | SourceRunOutcome::Cancelled => return Ok(()),
                SourceRunOutcome::ReconnectRequired => {}
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(()),
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

/// Stable classification for a source task failure retained after its join handle is reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceTaskFailureKind {
    /// The supervised source or its supervisor returned a typed error.
    Source,
    /// The source task panicked or was unexpectedly cancelled outside the shutdown owner.
    Join,
    /// The task owner's internal lifecycle invariant was violated.
    Lifecycle,
}

/// Bounded diagnostic retained after a failed source task is reaped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTaskFailure {
    kind: SourceTaskFailureKind,
    detail: &'static str,
}

impl SourceTaskFailure {
    const fn new(kind: SourceTaskFailureKind, detail: &'static str) -> Self {
        Self { kind, detail }
    }

    /// Returns the typed failure boundary.
    pub const fn kind(&self) -> SourceTaskFailureKind {
        self.kind
    }

    /// Returns bounded, UTF-8 diagnostic detail.
    pub const fn detail(&self) -> &'static str {
        self.detail
    }
}

/// Final, owned source-task disposition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourceShutdownOutcome {
    /// The source stopped cooperatively and its task was joined.
    Graceful,
    /// The deadline elapsed; the source task was aborted and then joined.
    AbortedAtDeadline,
    /// The task was joined after returning or encountering a failure.
    TaskFailed(SourceTaskFailure),
}

/// Source-task shutdown rejected an invalid lifecycle request.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SourceShutdownError {
    /// Shutdown deadlines must be nonzero.
    #[error("source shutdown deadline must be nonzero")]
    InvalidDeadline,
}

/// Sole owner of a spawned source task and its cancellation capability.
///
/// The normal application path always calls [`Self::shutdown`] or [`Self::wait`]. Dropping an
/// unfinished owner aborts the task as a final no-detach backstop.
#[derive(Debug)]
#[must_use = "a supervised source task must be joined or shut down"]
pub struct SupervisedSourceTask {
    cancellation: CancellationToken,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    completion: Option<SourceShutdownOutcome>,
}

impl SupervisedSourceTask {
    /// Spawns one source supervisor and retains exclusive task ownership.
    pub fn spawn(
        supervisor: SourceSupervisor,
        source: Box<dyn MarketSource>,
        events: mpsc::Sender<MarketEvent>,
    ) -> Self {
        Self::spawn_with_cancellation(move |cancellation| {
            supervisor.run(source, events, cancellation)
        })
    }

    /// Spawns an application-owned source future with one cancellation capability.
    ///
    /// This constructor exists so application composition can retain the same shutdown ownership
    /// contract around alternate supervised source services. The callback must not detach work.
    fn spawn_with_cancellation<F, Fut>(task: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let cancellation = CancellationToken::new();
        let handle = tokio::spawn(task(cancellation.child_token()));
        Self {
            cancellation,
            handle: Some(handle),
            completion: None,
        }
    }

    /// Waits for natural completion and reaps the join handle.
    pub async fn wait(&mut self) -> SourceShutdownOutcome {
        if let Some(completion) = &self.completion {
            return completion.clone();
        }
        let result = match self.handle.as_mut() {
            Some(handle) => handle.await,
            None => {
                let outcome = SourceShutdownOutcome::TaskFailed(SourceTaskFailure::new(
                    SourceTaskFailureKind::Lifecycle,
                    "source task handle was absent before completion",
                ));
                self.completion = Some(outcome.clone());
                return outcome;
            }
        };
        self.handle = None;
        let outcome = classify_join_result(result);
        self.completion = Some(outcome.clone());
        outcome
    }

    /// Cancels the source, waits through the deadline, then aborts and joins if necessary.
    pub async fn shutdown(
        &mut self,
        deadline: Duration,
    ) -> Result<SourceShutdownOutcome, SourceShutdownError> {
        if let Some(completion) = &self.completion {
            return Ok(completion.clone());
        }
        if deadline.is_zero() {
            self.cancellation.cancel();
            let _outcome = self.abort_and_await().await;
            return Err(SourceShutdownError::InvalidDeadline);
        }

        self.cancellation.cancel();
        if let Ok(outcome) = tokio::time::timeout(deadline, self.wait()).await {
            return Ok(outcome);
        }

        Ok(self.abort_and_await().await)
    }

    async fn abort_and_await(&mut self) -> SourceShutdownOutcome {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
        let result = match self.handle.as_mut() {
            Some(handle) => handle.await,
            None => {
                let outcome = SourceShutdownOutcome::TaskFailed(SourceTaskFailure::new(
                    SourceTaskFailureKind::Lifecycle,
                    "source task handle disappeared at the shutdown deadline",
                ));
                self.completion = Some(outcome.clone());
                return outcome;
            }
        };
        self.handle = None;
        let outcome = match result {
            Err(error) if error.is_cancelled() => SourceShutdownOutcome::AbortedAtDeadline,
            other => classify_join_result(other),
        };
        self.completion = Some(outcome.clone());
        outcome
    }

    /// Returns whether the owned join handle has been consumed.
    pub const fn is_reaped(&self) -> bool {
        self.handle.is_none()
    }
}

impl Drop for SupervisedSourceTask {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

fn classify_join_result(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> SourceShutdownOutcome {
    match result {
        Ok(Ok(())) => SourceShutdownOutcome::Graceful,
        Ok(Err(_error)) => SourceShutdownOutcome::TaskFailed(SourceTaskFailure::new(
            SourceTaskFailureKind::Source,
            "supervised source returned an error",
        )),
        Err(_error) => SourceShutdownOutcome::TaskFailed(SourceTaskFailure::new(
            SourceTaskFailureKind::Join,
            "source task join failed",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deadline_aborts_and_awaits_an_owned_task_that_ignores_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        });
        let mut task = SupervisedSourceTask {
            cancellation,
            handle: Some(handle),
            completion: None,
        };

        let outcome = task.shutdown(Duration::from_millis(10)).await?;

        assert_eq!(outcome, SourceShutdownOutcome::AbortedAtDeadline);
        assert!(task.is_reaped());
        Ok(())
    }

    #[tokio::test]
    async fn zero_shutdown_deadline_is_rejected_after_abort_and_await()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let handle: JoinHandle<anyhow::Result<()>> =
            tokio::spawn(async { std::future::pending().await });
        let mut task = SupervisedSourceTask {
            cancellation,
            handle: Some(handle),
            completion: None,
        };

        assert_eq!(
            task.shutdown(Duration::ZERO).await,
            Err(SourceShutdownError::InvalidDeadline)
        );
        assert!(task.is_reaped());
        Ok(())
    }

    #[test]
    fn raw_source_error_text_is_not_retained_in_typed_or_debug_diagnostics()
    -> Result<(), Box<dyn std::error::Error>> {
        const SENTINEL_SECRET: &str = "sentinel-secret-must-not-escape";
        let outcome = classify_join_result(Ok(Err(anyhow::anyhow!(SENTINEL_SECRET))));
        let rendered = format!("{outcome:?}");
        let failure = match outcome {
            SourceShutdownOutcome::TaskFailed(failure) => failure,
            other => return Err(format!("unexpected test outcome: {other:?}").into()),
        };

        assert_eq!(failure.kind(), SourceTaskFailureKind::Source);
        assert_eq!(failure.detail(), "supervised source returned an error");
        assert!(!failure.detail().contains(SENTINEL_SECRET));
        assert!(!rendered.contains(SENTINEL_SECRET));
        Ok(())
    }
}
