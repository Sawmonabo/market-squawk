//! Sole-account native Streamer -> physical capture -> canonical store -> current Markets.
mod families;
mod publication;
use crate::application::SchwabStreamerApplicationOutcome;
use crate::live_source::{SchwabRestQuoteCurrentSessionInput, SchwabStreamerCurrentEvidence};
use crate::provider_activation::PreparedSchwabStreamerMarketRuntimeStart;
use market_squawk_adapter_schwab::*;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_services::ServiceError;
use std::{
    collections::BTreeSet,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub(super) enum SchwabCurrentRuntime {
    Rest(super::schwab_sink::SchwabRestQuoteCurrentRuntime),
    Streamer(SchwabStreamerCurrentRuntime),
}
impl SchwabCurrentRuntime {
    pub(super) fn is_healthy(&self) -> bool {
        match self {
            Self::Rest(value) => value.is_healthy(),
            Self::Streamer(value) => value.is_healthy(),
        }
    }
    pub(super) async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        match self {
            Self::Rest(value) => value.finish_shutdown_before(deadline, cancellation).await,
            Self::Streamer(value) => value.finish_shutdown_before(deadline, cancellation).await,
        }
    }
    pub(super) async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        match self {
            Self::Rest(value) => value.finish_retained_shutdown().await,
            Self::Streamer(value) => value.finish_retained_shutdown().await,
        }
    }
}

/// Original source outcome and actual physical cleanup are separate evidence.
#[derive(Clone, Copy, Debug)]
struct StreamerWorkerOutcome {
    run: Result<(), ServiceError>,
    cleanup: Result<(), ServiceError>,
}

pub(super) struct SchwabStreamerCurrentRuntime {
    lifecycle: CancellationToken,
    worker: tokio::task::JoinHandle<StreamerWorkerOutcome>,
    joined: Option<Result<StreamerWorkerOutcome, ServiceError>>,
}
impl SchwabStreamerCurrentRuntime {
    pub(super) async fn start(
        prepared: PreparedSchwabStreamerMarketRuntimeStart,
        current: SchwabRestQuoteCurrentSessionInput,
        lifecycle: CancellationToken,
        deadline: Instant,
    ) -> Result<Self, super::group::AccountRuntimeStartFailure> {
        let (ready, mut waiting) = oneshot::channel();
        let mut worker = tokio::spawn(run(prepared, current, lifecycle.clone(), ready));
        let requested = tokio::select! {
            biased;
            value = &mut worker => {
                return Err(start_failure(value, ServiceError::Unavailable));
            }
            () = lifecycle.cancelled() => Some(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => Some(ServiceError::DeadlineExceeded),
            value = &mut waiting => value.err().map(|_| ServiceError::Unavailable),
        };
        if requested.is_none() && !worker.is_finished() && !lifecycle.is_cancelled() {
            return Ok(Self {
                lifecycle,
                worker,
                joined: None,
            });
        }
        // The registry retains this exact constructor task. Its ordinary waiter is bounded;
        // this original owner observes actual cleanup before reporting a failed startup.
        lifecycle.cancel();
        let fallback = requested.unwrap_or(ServiceError::Unavailable);
        Err(start_failure(worker.await, fallback))
    }
    fn is_healthy(&self) -> bool {
        !self.lifecycle.is_cancelled() && self.joined.is_none() && !self.worker.is_finished()
    }
    async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.lifecycle.cancel();
        if let Some(joined) = self.joined {
            return cleanup_result(joined);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => Err(ServiceError::DeadlineExceeded),
            result = self.finish_retained_shutdown() => result,
        }
    }
    async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.lifecycle.cancel();
        if let Some(joined) = self.joined {
            return cleanup_result(joined);
        }
        let joined = (&mut self.worker).await.map_err(|error| {
            tracing::error!(%error, "Schwab Streamer owner join failed");
            ServiceError::Unavailable
        });
        // No await between observing the original task and caching its terminal outcome.
        self.joined = Some(joined);
        cleanup_result(joined)
    }
}
impl Drop for SchwabStreamerCurrentRuntime {
    fn drop(&mut self) {
        self.lifecycle.cancel();
    }
}
fn start_failure(
    joined: Result<StreamerWorkerOutcome, tokio::task::JoinError>,
    fallback: ServiceError,
) -> super::group::AccountRuntimeStartFailure {
    use super::group::AccountRuntimeStartFailure;
    match joined {
        Ok(outcome) => AccountRuntimeStartFailure::after_cleanup(
            outcome.run.err().unwrap_or(fallback),
            outcome.cleanup,
        ),
        Err(error) => {
            tracing::error!(%error, "Schwab Streamer startup owner join failed");
            AccountRuntimeStartFailure::after_cleanup(fallback, Err(ServiceError::Unavailable))
        }
    }
}
fn cleanup_result(joined: Result<StreamerWorkerOutcome, ServiceError>) -> Result<(), ServiceError> {
    joined.and_then(|outcome| outcome.cleanup)
}

#[derive(Debug)]
struct CurrentConnectionControl {
    control: Mutex<Option<SchwabStreamerConnectionControl>>,
}
impl SchwabStreamerConnectionControlSource for CurrentConnectionControl {
    fn mint(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SchwabStreamerConnectionControl, SchwabTransportError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.control
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .take()
                .ok_or(SchwabTransportError::Protocol)
        })
    }
}
struct CaptureSink {
    sender: mpsc::Sender<StreamerMicrobatch>,
    overflow: Arc<Mutex<Option<StreamerMicrobatch>>>,
}
impl StreamerCaptureSink for CaptureSink {
    fn publish(
        &mut self,
        batch: StreamerMicrobatch,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamerCaptureSinkError>> + Send + '_>> {
        Box::pin(async move {
            // This owner is joined, never aborted: cancellation stops further network reads
            // while the consumer keeps draining and sealing already received frames.
            // Do not select cancellation against send; dropping it would discard the batch.
            if let Err(error) = self.sender.send(batch).await {
                let mut overflow = self
                    .overflow
                    .lock()
                    .map_err(|_| StreamerCaptureSinkError::Integrity)?;
                if overflow.is_some() {
                    return Err(StreamerCaptureSinkError::Integrity);
                }
                *overflow = Some(error.0);
                return Err(StreamerCaptureSinkError::Closed);
            }
            Ok(())
        })
    }
}

async fn run(
    prepared: PreparedSchwabStreamerMarketRuntimeStart,
    mut current: SchwabRestQuoteCurrentSessionInput,
    lifecycle: CancellationToken,
    ready: oneshot::Sender<()>,
) -> StreamerWorkerOutcome {
    let authority = Arc::clone(&prepared.publication.authority);
    let mut native_cleanup = Ok(());
    let outcome = run_native(
        prepared,
        &mut current,
        &lifecycle,
        ready,
        &mut native_cleanup,
    )
    .await;
    // Disconnect invalidates continuous current authority. Reprepare must mint a fresh durable
    // registry generation and cannot reuse this one-use connection control or original proof.
    lifecycle.cancel();
    authority.begin_revocation();
    let source_drain = authority
        .finish_revocation_drain()
        .await
        .map_err(|_| ServiceError::Unavailable);
    let current_drain = current
        .shutdown()
        .await
        .map_err(|_| ServiceError::Unavailable);
    if let Err(error) = outcome {
        tracing::warn!(?error, "Schwab current stream disconnected");
    }
    let cleanup = native_cleanup.and(source_drain).and(current_drain);
    if cleanup.is_err() {
        tracing::warn!(
            ?native_cleanup,
            ?source_drain,
            ?current_drain,
            "Schwab Streamer cleanup incomplete"
        );
    }
    StreamerWorkerOutcome {
        run: outcome,
        cleanup,
    }
}
async fn run_native(
    prepared: PreparedSchwabStreamerMarketRuntimeStart,
    current: &mut SchwabRestQuoteCurrentSessionInput,
    lifecycle: &CancellationToken,
    ready: oneshot::Sender<()>,
    native_cleanup: &mut Result<(), ServiceError>,
) -> Result<(), ServiceError> {
    let nz = |value| NonZeroUsize::new(value).ok_or(ServiceError::Internal);
    let native_generation = ConnectionGeneration::new(
        NonZeroU64::new(current.connection_generation().get()).ok_or(ServiceError::Internal)?,
    );
    let stream_identity =
        SourceIdentifier::try_from(format!("schwab-current-stream-{}", native_generation.get()))
            .map_err(|_| ServiceError::Internal)?;
    let session = SourceIdentifier::try_from(prepared.generation.session_id().to_string())
        .map_err(|_| ServiceError::Internal)?;
    let control = Arc::new(CurrentConnectionControl {
        control: Mutex::new(Some(SchwabStreamerConnectionControl::new(
            native_generation,
            session,
            prepared.publication.authority.coordinates(),
            stream_identity,
        ))),
    });
    let admission = StreamerAdmission::new(
        RequestAdmission::new(nz(16 * 1024)?, nz(50)?),
        nz(12)?,
        nz(9)?,
    );
    let parse = ParseBounds::new(
        nz(1024 * 1024)?,
        nz(8192)?,
        nz(128 * 1024)?,
        nz(64)?,
        128,
        128 * 1024,
    );
    let bounds = StreamerTransportBounds::try_new(
        Duration::from_secs(5),
        Duration::from_secs(15),
        Duration::from_millis(250),
        0,
        nz(1024 * 1024)?,
        nz(1)?,
        nz(1024 * 1024)?,
        Duration::from_millis(250),
    )
    .map_err(|_| ServiceError::Internal)?;
    let selections = families::selections(&prepared.bindings)?;
    let mut native = crate::provider_rate::GovernedSchwabStreamer::try_new(
        Arc::clone(&prepared.activation),
        &prepared.provider_rate,
        selections.keys().copied().collect(),
        control,
        admission,
        bounds,
        parse,
        AccessTokenAdmission::new(nz(16 * 1024)?, Duration::from_secs(60)),
        SchwabTransportTelemetry::default(),
    )
    .await
    .map_err(|_| ServiceError::Unavailable)?;
    let dictionary = SchwabStreamerFieldDictionary::official(MarketDataService::LevelOneEquities)
        .map_err(|_| ServiceError::InvalidResult)?;
    for (service, keys) in selections {
        native
            .replace_desired(
                StreamerSubscription::try_new(
                    service,
                    keys,
                    SchwabStreamerFieldDictionary::official(service)
                        .map_err(|_| ServiceError::InvalidResult)?
                        .field_ids()
                        .collect(),
                    admission,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
            )
            .map_err(|_| ServiceError::Unavailable)?;
    }
    let (sender, mut receiver) = mpsc::channel(4);
    let overflow = Arc::new(Mutex::new(None));
    let mut sink = CaptureSink {
        sender,
        overflow: Arc::clone(&overflow),
    };
    let native_cancel = lifecycle.child_token();
    let run_cancel = native_cancel.clone();
    let bootstrap = prepared.bootstrap;
    let mut consumer = publication::Consumer::new(
        prepared.activation,
        prepared.generation,
        prepared.publication,
        prepared.bindings,
        prepared.canonical,
        prepared.listing,
        prepared.nasdaq_generation,
        prepared.venue,
        dictionary,
        ready,
    )?;
    let transport = tokio::spawn(async move {
        native
            .run(bootstrap.bootstrap().value(), &mut sink, run_cancel)
            .await
    });
    let mut failure = None;
    while let Some(batch) = receiver.recv().await {
        if let Err(error) = consumer
            .consume(batch, current, parse, &native_cancel)
            .await
        {
            if failure.is_none() {
                failure = Some(error);
            }
            native_cancel.cancel();
        }
    }
    let native_result = transport.await;
    if let Ok(Err(error)) = &native_result {
        // Closed secret-free enum; never log the native frame or bootstrap material.
        tracing::warn!(
            ?error,
            consumer_failed = failure.is_some(),
            generation = native_generation.get(),
            "Schwab Streamer native transport ended"
        );
    }
    if let Err(error) = &native_result {
        tracing::error!(%error, "Schwab native transport owner join failed");
        *native_cleanup = Err(ServiceError::Unavailable);
    }
    // The transport is joined before the final overflow slot is inspected. A poisoned slot
    // still yields its original batch for sealing, but cannot authorize clean retirement.
    let last = match overflow.lock() {
        Ok(mut retained) => retained.take(),
        Err(poisoned) => {
            *native_cleanup = Err(ServiceError::Unavailable);
            failure.get_or_insert(ServiceError::Unavailable);
            poisoned.into_inner().take()
        }
    };
    if let Some(batch) = last {
        if let Err(error) = consumer
            .consume(batch, current, parse, &native_cancel)
            .await
        {
            if failure.is_none() {
                failure = Some(error);
            }
        }
    }
    *native_cleanup = (*native_cleanup).and(consumer.capture_cleanup());
    if let Some(error) = failure {
        return Err(error);
    }
    native_result
        .map_err(|_| ServiceError::Unavailable)?
        .map(|_| ())
        .map_err(|_| ServiceError::Unavailable)
}
fn now() -> Result<Timestamp, ServiceError> {
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ServiceError::Unavailable)?
                .as_nanos(),
        )
        .map_err(|_| ServiceError::Unavailable)?,
    ))
}
