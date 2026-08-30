//! Seal-first Schwab REST quote handoff into durable application publication.
//!
//! Every completed provider response crosses the sole research capture sealer exactly once.
//! Accepted canonical quotes then continue through the exact Schwab research generation and its
//! revocable precommit lease. The live runtime supplies the original registered
//! capture/session/display capability, so an opaque same-response projection can enter the current
//! registry only after the archival seal and immutable canonical publication succeed. Current
//! authority is never reconstructed from sealed provenance, and a runtime cannot become ready
//! until one qualified quote reaches the provider-neutral display ingress.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_schwab::{
    CapturedRestResponse, NativeScalar, ProviderIdentifier, QuoteComponentField, ReadOnlyRoute,
    RestItemAccounting, SchwabOAuthAuthorityReceipt, SchwabResolvedProviderIdentity,
    SchwabRestDelayEvidence, SchwabRestFamily, SchwabRestPayload,
    SchwabRestQuoteMarketDataEvidence, SchwabRestQuotePublicationOutcome,
    SchwabRestQuotePublicationRequest, SchwabRestQuoteRecordRequest,
    SchwabSealedRestQuotePublication, SchwabSealedRestResponse, SchwabTransportTelemetry,
};
use market_squawk_data::ProviderMarketEventPublicationKind;
use market_squawk_domain::{
    CanonicalStateDigest, CanonicalizationRule, ConnectionGeneration, CoverageStatus, DataQuality,
    DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest, LiveEventClass,
    LiveEvidenceBinding, LiveProvenance, MarketDepth, MarketEvent, PayloadHash, PayloadReference,
    RecordedLiveProvenanceInput, RuleVersion, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    BudgetUnavailableReason, InstrumentCoverageMembership, ProviderNativeLineageImplementation,
    ProviderRateAuthority, SourceMetadata,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::schwab::{
    SchwabRestQuoteBatch, SchwabRestQuoteBatchOutcome, SchwabRestQuoteEventSink,
    SchwabRestQuoteInstrumentBinding, SchwabRestQuotePollOutcome, SchwabRestQuoteProducer,
    SchwabRestQuotePublicationReceipt, SchwabRestQuoteRuntimeBounds, SchwabRestQuoteRuntimeError,
    SchwabRestQuoteSinkError, SchwabRestQuoteSourceEvidence,
};
use crate::application::research::{
    MarketEventPublicationReceipt, MarketEventSealedReceiptEvidence,
};
use crate::application::{
    MarketEventDurableReadWriter, SchwabMarketPublicationError, SchwabRestQuoteGenerationAuthority,
    SchwabRestQuotePostSealFailure, SchwabRestQuoteSourceHealthOutcome,
};
use crate::live_source::{
    SchwabRestQuoteCurrentBridge, SchwabRestQuoteCurrentEvidence, SchwabRestQuoteCurrentInstrument,
    SchwabRestQuoteCurrentPublication, SchwabRestQuoteCurrentRequest,
    SchwabRestQuoteCurrentSessionBridge, SchwabRestQuoteCurrentSessionInput,
    SchwabRestQuoteCurrentUnavailable,
};
use crate::provider_activation::{MarketInstrumentBinding, SchwabMarketDataAccountActivation};
use crate::provider_onboarding::SchwabOAuthPublicationEpoch;

const QUOTE_CANONICAL_STATE_RULE: &str = "market-squawk-schwab-rest-quote-state";
const QUOTE_CANONICAL_STATE_RULE_VERSION: u32 = 1;

/// One-use application composition input for an exact Schwab REST quote generation.
///
/// The current-session input already owns the registry-minted session, frame, capture, health,
/// display, and cleanup capabilities. This value adds the protected account activation, shared
/// provider budget, canonical bindings, and durable research generation; none is reconstructed by
/// the factory.
pub(crate) struct SchwabRestQuoteCurrentRuntimeInput {
    activation: SchwabMarketDataAccountActivation,
    provider_rate: ProviderRateAuthority,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Vec<MarketInstrumentBinding>,
    bounds: SchwabRestQuoteRuntimeBounds,
    telemetry: SchwabTransportTelemetry,
    durable: Arc<SchwabRestQuoteGenerationAuthority>,
    durable_writer: MarketEventDurableReadWriter,
    current: SchwabRestQuoteCurrentSessionInput,
    request_timeout: Duration,
    poll_interval: Duration,
    lifecycle: CancellationToken,
}

impl SchwabRestQuoteCurrentRuntimeInput {
    #[allow(
        clippy::too_many_arguments,
        reason = "every account, rate, source, current, durable, and lifecycle authority remains explicit"
    )]
    pub(crate) fn new(
        activation: SchwabMarketDataAccountActivation,
        provider_rate: ProviderRateAuthority,
        evidence: SchwabRestQuoteSourceEvidence,
        bindings: Vec<MarketInstrumentBinding>,
        bounds: SchwabRestQuoteRuntimeBounds,
        telemetry: SchwabTransportTelemetry,
        durable: Arc<SchwabRestQuoteGenerationAuthority>,
        durable_writer: MarketEventDurableReadWriter,
        current: SchwabRestQuoteCurrentSessionInput,
        request_timeout: Duration,
        poll_interval: Duration,
        lifecycle: CancellationToken,
    ) -> Self {
        Self {
            activation,
            provider_rate,
            evidence,
            bindings,
            bounds,
            telemetry,
            durable,
            durable_writer,
            current,
            request_timeout,
            poll_interval,
            lifecycle,
        }
    }
}

/// Started quote runtime whose worker owns polling plus exact-generation cleanup.
pub(crate) struct SchwabRestQuoteCurrentRuntime {
    cancellation: SchwabRestQuoteRuntimeCancellation,
    worker: tokio::task::JoinHandle<Result<(), SchwabRestQuoteSessionRuntimeError>>,
}

impl std::fmt::Debug for SchwabRestQuoteCurrentRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabRestQuoteCurrentRuntime")
            .field("cancelled", &self.cancellation.token.is_cancelled())
            .field("worker_finished", &self.worker.is_finished())
            .finish_non_exhaustive()
    }
}

impl SchwabRestQuoteCurrentRuntime {
    /// Binds every exact authority, starts polling, and returns only after a qualified current
    /// quote has entered the provider-neutral display ingress.
    pub(crate) async fn start(
        input: SchwabRestQuoteCurrentRuntimeInput,
        deadline: Instant,
    ) -> Result<Self, SchwabRestQuoteSessionRuntimeError> {
        let SchwabRestQuoteCurrentRuntimeInput {
            activation,
            provider_rate,
            evidence,
            bindings,
            bounds,
            telemetry,
            durable,
            durable_writer,
            current,
            request_timeout,
            poll_interval,
            lifecycle,
        } = input;
        if request_timeout.is_zero()
            || poll_interval.is_zero()
            || lifecycle.is_cancelled()
            || Instant::now() >= deadline
            || evidence.metadata() != durable.metadata()
        {
            let source = if lifecycle.is_cancelled() {
                SchwabRestQuoteSessionRuntimeError::Cancelled
            } else if Instant::now() >= deadline {
                SchwabRestQuoteSessionRuntimeError::Deadline
            } else {
                SchwabRestQuoteSessionRuntimeError::InvalidConfiguration
            };
            return Err(with_cleanup(
                source,
                cleanup_unstarted(current, &durable).await,
            ));
        }
        if let Err(source) = current_oauth_receipt(&activation, &lifecycle, deadline).await {
            return Err(with_cleanup(
                source,
                cleanup_unstarted(current, &durable).await,
            ));
        }
        let generation = current.connection_generation();
        let maximum = bounds.request_admission.max_items();
        let qualified = match SchwabRestQuoteInstrumentBinding::try_all(
            bindings,
            evidence.metadata().source_id(),
            maximum,
        ) {
            Ok(qualified) => qualified,
            Err(error) => {
                return Err(with_cleanup(
                    error.into(),
                    cleanup_unstarted(current, &durable).await,
                ));
            }
        };
        let instruments = match current_instruments(&qualified, evidence.metadata()) {
            Ok(instruments) => instruments,
            Err(error) => {
                return Err(with_cleanup(
                    SchwabRestQuoteSessionRuntimeError::Current(error),
                    cleanup_unstarted(current, &durable).await,
                ));
            }
        };
        let current_bridge = match SchwabRestQuoteCurrentSessionBridge::try_new(
            current,
            durable.metadata(),
            durable.session_identifier(),
            generation,
            evidence.venue_id(),
            &instruments,
        )
        .await
        {
            Ok(bridge) => Arc::new(bridge),
            Err(error) => {
                return Err(with_cleanup(
                    SchwabRestQuoteSessionRuntimeError::Current(error),
                    drain_durable(&durable).await,
                ));
            }
        };
        let current_sink: Arc<dyn SchwabRestQuoteCurrentBridge> = current_bridge.clone();
        let sink: Arc<dyn SchwabRestQuoteEventSink> =
            Arc::new(SchwabRestQuoteSealFirstSink::production(
                Arc::clone(&durable),
                durable_writer,
                current_sink,
            ));
        let producer = match SchwabRestQuoteProducer::try_production(
            activation,
            &provider_rate,
            generation,
            evidence,
            qualified,
            bounds,
            telemetry,
            sink,
        ) {
            Ok(producer) => producer,
            Err(error) => {
                let cleanup = cleanup_bridge(current_bridge, &durable).await;
                return Err(with_cleanup(error.into(), cleanup));
            }
        };
        let (ready_sender, mut ready_receiver) = oneshot::channel();
        let worker_cancellation = lifecycle.clone();
        let mut worker = tokio::spawn(run_current_runtime(
            producer,
            current_bridge,
            Arc::clone(&durable),
            request_timeout,
            poll_interval,
            worker_cancellation,
            ready_sender,
        ));
        let startup = tokio::select! {
            biased;
            ready = &mut ready_receiver => SchwabRestQuoteStartup::Ready(ready),
            result = &mut worker => SchwabRestQuoteStartup::Worker(result),
            () = lifecycle.cancelled() => SchwabRestQuoteStartup::Cancelled,
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                SchwabRestQuoteStartup::Deadline
            }
        };
        match startup {
            SchwabRestQuoteStartup::Ready(Ok(())) => Ok(Self {
                cancellation: SchwabRestQuoteRuntimeCancellation::new(lifecycle),
                worker,
            }),
            SchwabRestQuoteStartup::Ready(Err(_closed)) => {
                Err(startup_worker_outcome(worker.await))
            }
            SchwabRestQuoteStartup::Worker(result) => Err(startup_worker_outcome(result)),
            SchwabRestQuoteStartup::Cancelled => {
                let cleanup = finish_cancelled_start(worker, &lifecycle).await;
                Err(with_cleanup(
                    SchwabRestQuoteSessionRuntimeError::Cancelled,
                    cleanup,
                ))
            }
            SchwabRestQuoteStartup::Deadline => {
                let cleanup = finish_cancelled_start(worker, &lifecycle).await;
                Err(with_cleanup(
                    SchwabRestQuoteSessionRuntimeError::Deadline,
                    cleanup,
                ))
            }
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        !self.cancellation.token.is_cancelled() && !self.worker.is_finished()
    }

    /// Cancels future work and waits for registry, display, capture, and durable drains.
    pub(crate) async fn shutdown(
        mut self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
        self.cancellation.cancel();
        let requested = tokio::select! {
            biased;
            result = &mut self.worker => return worker_outcome(result),
            () = cancellation.cancelled() => SchwabRestQuoteSessionRuntimeError::Cancelled,
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                SchwabRestQuoteSessionRuntimeError::Deadline
            }
        };
        let cleanup = worker_outcome(self.worker.await);
        match cleanup {
            Ok(()) => Err(requested),
            Err(cleanup) => Err(with_cleanup(requested, Err(cleanup))),
        }
    }
}

enum SchwabRestQuoteStartup {
    Ready(Result<(), oneshot::error::RecvError>),
    Worker(Result<Result<(), SchwabRestQuoteSessionRuntimeError>, tokio::task::JoinError>),
    Cancelled,
    Deadline,
}

#[derive(Debug)]
struct SchwabRestQuoteRuntimeCancellation {
    token: CancellationToken,
}

impl SchwabRestQuoteRuntimeCancellation {
    const fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    fn cancel(&self) {
        self.token.cancel();
    }
}

impl Drop for SchwabRestQuoteRuntimeCancellation {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Closed construction, run, and complete-drain failures for one current Schwab generation.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SchwabRestQuoteSessionRuntimeError {
    #[error("Schwab current quote runtime configuration is invalid")]
    InvalidConfiguration,
    #[error("Schwab current quote runtime ended before qualified display readiness")]
    EndedBeforeReady,
    #[error("Schwab current quote runtime was cancelled")]
    Cancelled,
    #[error("Schwab current quote runtime deadline elapsed")]
    Deadline,
    #[error("Schwab current quote runtime lost sole generation ownership")]
    Ownership,
    #[error("Schwab current quote session failed: {0:?}")]
    Current(SchwabRestQuoteCurrentUnavailable),
    #[error(transparent)]
    Producer(#[from] SchwabRestQuoteRuntimeError),
    #[error(transparent)]
    Durable(#[from] SchwabMarketPublicationError),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error("Schwab current quote run failed and exact-generation cleanup also failed")]
    RunCleanup {
        source: Box<SchwabRestQuoteSessionRuntimeError>,
        cleanup: Box<SchwabRestQuoteSessionRuntimeError>,
    },
}

async fn current_oauth_receipt(
    activation: &SchwabMarketDataAccountActivation,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<SchwabOAuthAuthorityReceipt, SchwabRestQuoteSessionRuntimeError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            return Err(SchwabRestQuoteSessionRuntimeError::Cancelled);
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            return Err(SchwabRestQuoteSessionRuntimeError::Deadline);
        }
        current = activation.require_current() => {
            current.map_err(SchwabRestQuoteRuntimeError::from)?;
        }
    }
    let oauth = activation.oauth_authority();
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabRestQuoteSessionRuntimeError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(SchwabRestQuoteSessionRuntimeError::Deadline)
        }
        receipt = oauth.current_receipt() => receipt
            .map_err(SchwabRestQuoteRuntimeError::from)
            .map_err(Into::into),
    }
}

async fn run_current_runtime(
    producer: SchwabRestQuoteProducer,
    current: Arc<SchwabRestQuoteCurrentSessionBridge>,
    durable: Arc<SchwabRestQuoteGenerationAuthority>,
    request_timeout: Duration,
    poll_interval: Duration,
    cancellation: CancellationToken,
    ready: oneshot::Sender<()>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    let mut ready = Some(ready);
    let run = loop {
        if cancellation.is_cancelled() {
            break Ok(());
        }
        let Some(deadline) = Instant::now().checked_add(request_timeout) else {
            break Err(SchwabRestQuoteSessionRuntimeError::InvalidConfiguration);
        };
        match producer.poll_once(&cancellation, deadline).await {
            Ok(SchwabRestQuotePollOutcome::Published { .. }) => {
                if let Some(sender) = ready.take() {
                    let _startup_receiver = sender.send(());
                }
                if !wait_runtime(poll_interval, &cancellation).await {
                    break Ok(());
                }
            }
            Ok(SchwabRestQuotePollOutcome::SealedWithoutPublication { current, .. }) => {
                match current {
                    SchwabRestQuoteCurrentPublication::NotApplicable
                    | SchwabRestQuoteCurrentPublication::NoPublishableQuotes
                    | SchwabRestQuoteCurrentPublication::Unavailable(
                        SchwabRestQuoteCurrentUnavailable::Deadline
                        | SchwabRestQuoteCurrentUnavailable::Busy,
                    ) => {
                        if !wait_runtime(poll_interval, &cancellation).await {
                            break Ok(());
                        }
                    }
                    SchwabRestQuoteCurrentPublication::Unavailable(reason) => {
                        break Err(SchwabRestQuoteSessionRuntimeError::Current(reason));
                    }
                    SchwabRestQuoteCurrentPublication::Published { .. } => {
                        break Err(SchwabRestQuoteSessionRuntimeError::InvalidConfiguration);
                    }
                }
            }
            Ok(SchwabRestQuotePollOutcome::Deferred(until)) => {
                let wait = match producer.remaining_budget_wait(until) {
                    Ok(wait) => wait,
                    Err(error) => break Err(error.into()),
                };
                if !wait_runtime(wait, &cancellation).await {
                    break Ok(());
                }
            }
            Err(SchwabRestQuoteRuntimeError::Cancelled) if cancellation.is_cancelled() => {
                break Ok(());
            }
            Err(
                SchwabRestQuoteRuntimeError::Deadline
                | SchwabRestQuoteRuntimeError::Sink(SchwabRestQuoteSinkError::Deadline)
                | SchwabRestQuoteRuntimeError::Budget(BudgetUnavailableReason::ConcurrencyExhausted),
            ) => {
                if !wait_runtime(poll_interval, &cancellation).await {
                    break Ok(());
                }
            }
            Err(error) => break Err(error.into()),
        }
    };

    drop(producer);
    let current_cleanup = match Arc::try_unwrap(current) {
        Ok(current) => current
            .shutdown()
            .await
            .map_err(SchwabRestQuoteSessionRuntimeError::Current),
        Err(_retained) => Err(SchwabRestQuoteSessionRuntimeError::Ownership),
    };
    let durable_cleanup = drain_durable(&durable).await;
    merge_run_cleanup(merge_run_cleanup(run, current_cleanup), durable_cleanup)
}

async fn wait_runtime(duration: Duration, cancellation: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

async fn cleanup_unstarted(
    current: SchwabRestQuoteCurrentSessionInput,
    durable: &Arc<SchwabRestQuoteGenerationAuthority>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    durable.begin_revocation();
    let current = current
        .shutdown()
        .await
        .map_err(SchwabRestQuoteSessionRuntimeError::Current);
    let durable = drain_durable(durable).await;
    merge_run_cleanup(current, durable)
}

async fn cleanup_bridge(
    current: Arc<SchwabRestQuoteCurrentSessionBridge>,
    durable: &Arc<SchwabRestQuoteGenerationAuthority>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    durable.begin_revocation();
    let current = match Arc::try_unwrap(current) {
        Ok(current) => current
            .shutdown()
            .await
            .map_err(SchwabRestQuoteSessionRuntimeError::Current),
        Err(_retained) => Err(SchwabRestQuoteSessionRuntimeError::Ownership),
    };
    let durable = drain_durable(durable).await;
    merge_run_cleanup(current, durable)
}

async fn drain_durable(
    durable: &Arc<SchwabRestQuoteGenerationAuthority>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    durable.begin_revocation();
    let cancellation = CancellationToken::new();
    tokio::select! {
        biased;
        result = durable.finish_revocation_drain(&cancellation) => result.map_err(Into::into),
        () = tokio::time::sleep(durable.operation_timeout()) => {
            cancellation.cancel();
            Err(SchwabRestQuoteSessionRuntimeError::Deadline)
        }
    }
}

async fn finish_cancelled_start(
    worker: tokio::task::JoinHandle<Result<(), SchwabRestQuoteSessionRuntimeError>>,
    cancellation: &CancellationToken,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    cancellation.cancel();
    worker_outcome(worker.await)
}

fn startup_worker_outcome(
    result: Result<Result<(), SchwabRestQuoteSessionRuntimeError>, tokio::task::JoinError>,
) -> SchwabRestQuoteSessionRuntimeError {
    match result {
        Ok(Ok(())) => SchwabRestQuoteSessionRuntimeError::EndedBeforeReady,
        Ok(Err(error)) => error,
        Err(error) => error.into(),
    }
}

fn worker_outcome(
    result: Result<Result<(), SchwabRestQuoteSessionRuntimeError>, tokio::task::JoinError>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    match result {
        Ok(result) => result,
        Err(error) => Err(error.into()),
    }
}

fn merge_run_cleanup(
    run: Result<(), SchwabRestQuoteSessionRuntimeError>,
    cleanup: Result<(), SchwabRestQuoteSessionRuntimeError>,
) -> Result<(), SchwabRestQuoteSessionRuntimeError> {
    match (run, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(source),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(source), Err(cleanup)) => Err(SchwabRestQuoteSessionRuntimeError::RunCleanup {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn with_cleanup(
    source: SchwabRestQuoteSessionRuntimeError,
    cleanup: Result<(), SchwabRestQuoteSessionRuntimeError>,
) -> SchwabRestQuoteSessionRuntimeError {
    match cleanup {
        Ok(()) => source,
        Err(cleanup) => SchwabRestQuoteSessionRuntimeError::RunCleanup {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        },
    }
}

/// Application-owned consumer for one exact Schwab research generation.
pub(crate) struct SchwabRestQuoteSealFirstSink {
    authority: Arc<SchwabRestQuoteGenerationAuthority>,
    durable_read: SchwabRestQuoteDurableReadInstall,
    current: Arc<dyn SchwabRestQuoteCurrentBridge>,
}

enum SchwabRestQuoteDurableReadInstall {
    Required(MarketEventDurableReadWriter),
    #[cfg(test)]
    AuthorityBoundaryOnly,
}

impl std::fmt::Debug for SchwabRestQuoteSealFirstSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabRestQuoteSealFirstSink")
            .field("authority", &self.authority)
            .field("durable_read", &"[SOURCE-BOUND PIT INSTALL]")
            .field("current", &"[GENERATION-BOUND CURRENT AUTHORITY]")
            .finish()
    }
}

impl SchwabRestQuoteSealFirstSink {
    fn production(
        authority: Arc<SchwabRestQuoteGenerationAuthority>,
        durable_writer: MarketEventDurableReadWriter,
        current: Arc<dyn SchwabRestQuoteCurrentBridge>,
    ) -> Self {
        Self {
            authority,
            durable_read: SchwabRestQuoteDurableReadInstall::Required(durable_writer),
            current,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(
        authority: Arc<SchwabRestQuoteGenerationAuthority>,
        current: Arc<dyn SchwabRestQuoteCurrentBridge>,
    ) -> Self {
        Self {
            authority,
            durable_read: SchwabRestQuoteDurableReadInstall::AuthorityBoundaryOnly,
            current,
        }
    }

    async fn publish_batch(
        &self,
        batch: SchwabRestQuoteBatch,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let (outcome, evidence, bindings, oauth_epoch, connection_generation, accounting) =
            batch.into_parts();
        let oauth = oauth_epoch.receipt();
        let deadline = Instant::now()
            .checked_add(self.authority.operation_timeout())
            .ok_or(SchwabRestQuoteSinkError::Deadline)?;
        let event_id = Uuid::new_v4();
        match outcome {
            SchwabRestQuoteBatchOutcome::Accepted(response) => {
                let payload_digest = evidence_digest(response.capture().receipt().body_sha256());
                // Retain the exact bounded response projection needed by the current registry,
                // but do not publish it until the archival seal and immutable canonical
                // publication have both succeeded.
                let current_evidence = SchwabRestQuoteCurrentEvidence::try_from_response(&response);
                let request = build_publication_request(
                    &response,
                    &evidence,
                    &bindings,
                    self.authority.session_identifier(),
                    oauth,
                    connection_generation,
                );
                let pending = response
                    .into_pending_capture(self.authority.coordinates(), event_id)
                    .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
                let (rejoin, seal_request) = pending.into_sealing_parts();
                let sealed = self
                    .authority
                    .seal_capture(seal_request, deadline)
                    .await
                    .map_err(map_publication_error)?;
                let sealed = match rejoin.try_rejoin(sealed) {
                    Ok(sealed) => sealed,
                    Err(_error) => {
                        return self.accepted_failure(
                            payload_digest,
                            None,
                            SchwabRestQuoteSinkError::InvalidReceipt,
                        );
                    }
                };
                self.publish_accepted(
                    sealed,
                    request,
                    &evidence,
                    &bindings,
                    oauth_epoch,
                    connection_generation,
                    accounting,
                    deadline,
                    current_evidence,
                )
                .await
            }
            SchwabRestQuoteBatchOutcome::ProviderRejected(capture) => {
                self.seal_noncanonical(
                    capture,
                    &evidence,
                    oauth_epoch,
                    connection_generation,
                    accounting,
                    event_id,
                    deadline,
                    NoncanonicalOutcome::ProviderRejected,
                )
                .await
            }
            SchwabRestQuoteBatchOutcome::InvalidPayload { capture, error } => {
                self.seal_noncanonical(
                    capture,
                    &evidence,
                    oauth_epoch,
                    connection_generation,
                    accounting,
                    event_id,
                    deadline,
                    NoncanonicalOutcome::InvalidPayload(error),
                )
                .await
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the physically sealed response retains every exact dispatch coordinate"
    )]
    async fn publish_accepted(
        &self,
        sealed: SchwabSealedRestResponse,
        request: Result<SchwabRestQuotePublicationRequest, SchwabRestQuoteSinkError>,
        evidence: &SchwabRestQuoteSourceEvidence,
        bindings: &[SchwabRestQuoteInstrumentBinding],
        oauth_epoch: SchwabOAuthPublicationEpoch,
        connection_generation: ConnectionGeneration,
        accounting: RestItemAccounting,
        deadline: Instant,
        current_evidence: Result<SchwabRestQuoteCurrentEvidence, SchwabRestQuoteCurrentUnavailable>,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let payload_digest = evidence_digest(sealed.receipt().body_sha256());
        let sealed_receipt_digest = sealed.persisted_receipt().receipt_digest();
        let oauth = oauth_epoch.receipt();
        let observed_at = match wall_timestamp() {
            Ok(observed_at) => observed_at,
            Err(error) => {
                return self.accepted_failure(payload_digest, Some(sealed_receipt_digest), error);
            }
        };
        if oauth_epoch.validate_current(oauth).is_err() {
            return self.accepted_failure(
                payload_digest,
                Some(sealed_receipt_digest),
                SchwabRestQuoteSinkError::InvalidReceipt,
            );
        }
        if let Err(error) = validate_dispatch(
            self.authority.metadata(),
            evidence,
            oauth,
            accounting,
            sealed.receipt().token_generation().get(),
            sealed.route(),
            sealed.accounting(),
        ) {
            return self.accepted_failure(payload_digest, Some(sealed_receipt_digest), error);
        }
        let request = match request {
            Ok(request) => request,
            Err(error) => {
                return self.accepted_failure(payload_digest, Some(sealed_receipt_digest), error);
            }
        };
        let outcome = match sealed.into_quote_publication(request) {
            Ok(outcome) => outcome,
            Err(_error) => {
                return self.accepted_failure(
                    payload_digest,
                    Some(sealed_receipt_digest),
                    SchwabRestQuoteSinkError::InvalidReceipt,
                );
            }
        };
        match outcome {
            SchwabRestQuotePublicationOutcome::SealedRaw(raw) => {
                self.authority
                    .record_source_health(SchwabRestQuoteSourceHealthOutcome::AllRowsAbstained {
                        payload_digest,
                        sealed_receipt_digest,
                        dispositions: raw.dispositions().to_vec().into_boxed_slice(),
                    })
                    .map_err(map_publication_error)?;
                raw_sealed_receipt(
                    self.authority.metadata(),
                    connection_generation,
                    accounting,
                    SchwabRestQuoteCurrentPublication::NoPublishableQuotes,
                )
            }
            SchwabRestQuotePublicationOutcome::Published(publication) => {
                if let Err(error) = validate_publication(
                    &publication,
                    self.authority.metadata(),
                    evidence,
                    bindings,
                    connection_generation,
                    payload_digest,
                    self.authority.provider_dataset(),
                ) {
                    return self.accepted_failure(
                        payload_digest,
                        Some(sealed_receipt_digest),
                        error,
                    );
                }
                self.publish_durable(
                    publication,
                    oauth_epoch,
                    observed_at,
                    connection_generation,
                    accounting,
                    payload_digest,
                    sealed_receipt_digest,
                    deadline,
                    current_evidence,
                    evidence,
                    bindings,
                )
                .await
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "durable publication checks every sealed/generation/accounting coordinate"
    )]
    async fn publish_durable(
        &self,
        publication: Box<SchwabSealedRestQuotePublication>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        observed_at: Timestamp,
        connection_generation: ConnectionGeneration,
        accounting: RestItemAccounting,
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: EvidenceDigest,
        deadline: Instant,
        current_evidence: Result<SchwabRestQuoteCurrentEvidence, SchwabRestQuoteCurrentUnavailable>,
        evidence: &SchwabRestQuoteSourceEvidence,
        bindings: &[SchwabRestQuoteInstrumentBinding],
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let oauth = oauth_epoch.receipt();
        let expected_count = publication.binding().record_count();
        let expected_digest = publication.binding().evidence_digest().evidence();
        let idempotency_key = publication_idempotency_key(expected_digest, oauth, payload_digest);
        let receipt = match self
            .authority
            .publish_sealed_rest_quotes(
                publication,
                oauth_epoch,
                observed_at,
                idempotency_key,
                deadline,
            )
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                let _health_result = self.authority.record_source_health(
                    SchwabRestQuoteSourceHealthOutcome::PostSealPublicationUnavailable {
                        payload_digest,
                        sealed_receipt_digest: Some(sealed_receipt_digest),
                        reason: post_seal_failure(&error),
                    },
                );
                return Err(map_publication_error(error));
            }
        };
        let generation = receipt.generation();
        let expected_current_count = u64::try_from(expected_count)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        if generation.publication_digest() != expected_digest
            || generation.sealed_receipt_digest() != sealed_receipt_digest
            || generation.provider_dataset() != self.authority.provider_dataset()
            || generation.event_count() != expected_count
            || generation.oauth_generation() != oauth.generation()
            || generation.restart_selector().manifest() != generation.committed().manifest()
            || generation.restart_selector().publication_digest() != expected_digest
            || generation.restart_selector().publication_kind()
                != ProviderMarketEventPublicationKind::ResponseMarketEvent
        {
            return self.accepted_failure(
                payload_digest,
                Some(sealed_receipt_digest),
                SchwabRestQuoteSinkError::InvalidReceipt,
            );
        }
        let durable_receipt = match MarketEventPublicationReceipt::try_new(
            generation.restart_selector().manifest().clone(),
            generation.restart_selector().publication_digest(),
            generation.restart_selector().publication_kind(),
            ProviderNativeLineageImplementation::SchwabRestMarketDataV1,
            self.authority.metadata().source_id().clone(),
            generation.provider_dataset().clone(),
            MarketEventSealedReceiptEvidence::Single(generation.sealed_receipt_digest()),
            generation.event_count(),
        ) {
            Ok(receipt) => receipt,
            Err(_error) => {
                return self.accepted_failure(
                    payload_digest,
                    Some(sealed_receipt_digest),
                    SchwabRestQuoteSinkError::InvalidReceipt,
                );
            }
        };
        match &self.durable_read {
            SchwabRestQuoteDurableReadInstall::Required(writer) => {
                if writer.retain(durable_receipt).await.is_err() {
                    return self.accepted_failure(
                        payload_digest,
                        Some(sealed_receipt_digest),
                        SchwabRestQuoteSinkError::Unavailable,
                    );
                }
            }
            #[cfg(test)]
            SchwabRestQuoteDurableReadInstall::AuthorityBoundaryOnly => {}
        }
        let current = match current_evidence {
            Ok(current_evidence) => {
                self.publish_current(&current_evidence, evidence, bindings, deadline)
            }
            Err(reason) => SchwabRestQuoteCurrentPublication::Unavailable(reason),
        };
        if matches!(current, SchwabRestQuoteCurrentPublication::NotApplicable)
            || current.published() != 0 && current.published() != expected_current_count
            || matches!(
                current,
                SchwabRestQuoteCurrentPublication::NoPublishableQuotes
            )
        {
            return self.accepted_failure(
                payload_digest,
                Some(sealed_receipt_digest),
                SchwabRestQuoteSinkError::InvalidReceipt,
            );
        }
        if !matches!(current, SchwabRestQuoteCurrentPublication::Published { .. }) {
            self.authority
                .record_source_health(
                    SchwabRestQuoteSourceHealthOutcome::DurablePublishedCurrentUnavailable {
                        publication_digest: expected_digest,
                        sealed_receipt_digest,
                        event_count: expected_count,
                        dispositions: receipt.dispositions().to_vec().into_boxed_slice(),
                    },
                )
                .map_err(map_publication_error)?;
        }

        raw_sealed_receipt(
            self.authority.metadata(),
            connection_generation,
            accounting,
            current,
        )
    }

    fn accepted_failure(
        &self,
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: Option<EvidenceDigest>,
        error: SchwabRestQuoteSinkError,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let reason = match error {
            SchwabRestQuoteSinkError::InvalidReceipt => {
                SchwabRestQuotePostSealFailure::AuthorityOrBinding
            }
            SchwabRestQuoteSinkError::Unavailable => {
                SchwabRestQuotePostSealFailure::StorageUnavailable
            }
            SchwabRestQuoteSinkError::Cancelled => {
                SchwabRestQuotePostSealFailure::ShutdownOrRevocation
            }
            SchwabRestQuoteSinkError::Deadline => SchwabRestQuotePostSealFailure::Deadline,
        };
        let _health_result = self.authority.record_source_health(
            SchwabRestQuoteSourceHealthOutcome::PostSealPublicationUnavailable {
                payload_digest,
                sealed_receipt_digest,
                reason,
            },
        );
        Err(error)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "raw rejection sealing retains every exact dispatch coordinate and typed cause"
    )]
    async fn seal_noncanonical(
        &self,
        capture: CapturedRestResponse,
        evidence: &SchwabRestQuoteSourceEvidence,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        connection_generation: ConnectionGeneration,
        accounting: RestItemAccounting,
        event_id: Uuid,
        deadline: Instant,
        outcome: NoncanonicalOutcome,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
        let oauth = oauth_epoch.receipt();
        let token_generation = capture.receipt().token_generation().get();
        let route = capture.receipt().route();
        let status = capture.receipt().status();
        let payload_digest = evidence_digest(capture.receipt().body_sha256());
        let captured_accounting = capture.accounting();
        let pending = capture
            .into_pending_capture(self.authority.coordinates(), event_id)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        let (rejoin, seal_request) = pending.into_sealing_parts();
        let sealed = self
            .authority
            .seal_capture(seal_request, deadline)
            .await
            .map_err(map_publication_error)?;
        let sealed = rejoin
            .try_rejoin(sealed)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        oauth_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        validate_dispatch(
            self.authority.metadata(),
            evidence,
            oauth,
            accounting,
            token_generation,
            route,
            captured_accounting,
        )?;
        if sealed.coordinates() != &self.authority.coordinates()
            || sealed.accounting() != accounting
        {
            return Err(SchwabRestQuoteSinkError::InvalidReceipt);
        }
        let sealed_receipt_digest = sealed.persisted_receipt().receipt_digest();
        let health = match outcome {
            NoncanonicalOutcome::ProviderRejected => {
                SchwabRestQuoteSourceHealthOutcome::ProviderRejected {
                    status,
                    payload_digest,
                    sealed_receipt_digest,
                }
            }
            NoncanonicalOutcome::InvalidPayload(error) => {
                SchwabRestQuoteSourceHealthOutcome::InvalidPayload {
                    status,
                    error,
                    payload_digest,
                    sealed_receipt_digest,
                }
            }
        };
        self.authority
            .record_source_health(health)
            .map_err(map_publication_error)?;
        raw_sealed_receipt(
            self.authority.metadata(),
            connection_generation,
            accounting,
            SchwabRestQuoteCurrentPublication::NotApplicable,
        )
    }

    fn publish_current(
        &self,
        response: &SchwabRestQuoteCurrentEvidence,
        evidence: &SchwabRestQuoteSourceEvidence,
        bindings: &[SchwabRestQuoteInstrumentBinding],
        deadline: Instant,
    ) -> SchwabRestQuoteCurrentPublication {
        let instruments = match current_instruments(bindings, evidence.metadata()) {
            Ok(instruments) => instruments,
            Err(reason) => return SchwabRestQuoteCurrentPublication::Unavailable(reason),
        };
        self.current
            .publish_current(SchwabRestQuoteCurrentRequest::new(
                response,
                evidence.metadata(),
                evidence.venue_id(),
                evidence.delay(),
                &instruments,
                deadline,
            ))
    }
}

impl SchwabRestQuoteEventSink for SchwabRestQuoteSealFirstSink {
    fn publish(
        &self,
        batch: SchwabRestQuoteBatch,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.publish_batch(batch))
    }
}

#[derive(Debug)]
enum NoncanonicalOutcome {
    ProviderRejected,
    InvalidPayload(market_squawk_adapter_schwab::SchwabAdapterError),
}

fn build_publication_request(
    response: &market_squawk_adapter_schwab::ExecutedRestResponse,
    evidence: &SchwabRestQuoteSourceEvidence,
    bindings: &[SchwabRestQuoteInstrumentBinding],
    session_id: &SourceIdentifier,
    oauth: SchwabOAuthAuthorityReceipt,
    connection_generation: ConnectionGeneration,
) -> Result<SchwabRestQuotePublicationRequest, SchwabRestQuoteSinkError> {
    if response.capture().receipt().route() != ReadOnlyRoute::Quotes
        || response.payload().family() != SchwabRestFamily::Quotes
        || response.capture().receipt().token_generation() != oauth.generation()
        || bindings.is_empty()
    {
        return Err(SchwabRestQuoteSinkError::InvalidReceipt);
    }
    let SchwabRestPayload::Quotes(quotes) = response.payload() else {
        return Err(SchwabRestQuoteSinkError::InvalidReceipt);
    };
    let received_at =
        timestamp_from_unix_millis(response.capture().receipt().received_at_unix_millis())?;
    let observed_at = wall_timestamp()?;
    if observed_at < received_at {
        return Err(SchwabRestQuoteSinkError::InvalidReceipt);
    }
    let payload_digest = evidence_digest(response.capture().receipt().body_sha256());
    let payload_reference = PayloadReference::ContentHash(PayloadHash::new(
        DigestAlgorithm::Sha256,
        response.capture().receipt().body_sha256(),
    ));
    let mut records = Vec::new();
    records
        .try_reserve_exact(quotes.value().quotes().len())
        .map_err(|_error| SchwabRestQuoteSinkError::Unavailable)?;
    for quote in quotes.value().quotes() {
        let Some(binding) = bindings
            .iter()
            .find(|binding| binding.provider_symbol() == quote.symbol().as_str())
        else {
            continue;
        };
        let provider_identity = binding
            .binding()
            .provider_identity()
            .ok_or(SchwabRestQuoteSinkError::InvalidReceipt)?;
        if provider_identity.source_id() != evidence.metadata().source_id()
            || provider_identity.instrument_id() != binding.instrument_id()
        {
            return Err(SchwabRestQuoteSinkError::InvalidReceipt);
        }
        let identity = SchwabResolvedProviderIdentity::try_new(
            quote.symbol().clone(),
            provider_identity.provider_instrument_id().clone(),
            provider_identity.evidence().content_digest(),
        )
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        let source_identifier =
            SourceIdentifier::try_from(provider_identity.provider_instrument_id().as_str())
                .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        let state = canonical_quote_state(
            payload_digest,
            binding.instrument_id(),
            identity.provider_symbol(),
            identity.resolution_evidence(),
        )?;
        let live_binding = LiveEvidenceBinding::new(
            evidence.metadata().source_id().clone(),
            session_id.clone(),
            evidence.metadata().revision().clone(),
            evidence.metadata().authorization().basis().clone(),
            evidence.venue_id().clone(),
            binding.instrument_id(),
            connection_generation,
            evidence.provider_product().clone(),
            evidence.provider_channel().clone(),
            LiveEventClass::Quote,
            source_identifier.clone(),
            payload_digest,
            state,
            None,
        )
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        let coverage = quote_coverage_status(evidence, binding.instrument_id());
        let provenance = if evidence.quality() == DataQuality::DirectVerified {
            LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
                live_binding,
                quote_source_timestamp(quote)?,
                received_at,
                observed_at,
                observed_at,
                evidence.quality(),
                coverage,
                payload_reference.clone(),
                qualification_reference(evidence.qualification_evidence())?,
            ))
        } else {
            LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
                live_binding,
                quote_source_timestamp(quote)?,
                received_at,
                observed_at,
                observed_at,
                evidence.quality(),
                coverage,
                payload_reference.clone(),
            ))
        }
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        let terms = binding.binding().execution_terms();
        let market_data = SchwabRestQuoteMarketDataEvidence::try_new(
            session_id.clone(),
            connection_generation,
            evidence.feed().clone(),
            evidence.venue_id().clone(),
            MarketDepth::TopOfBook,
            evidence.delay(),
            evidence.quality(),
            evidence.provider_product().clone(),
            evidence.provider_channel().clone(),
            evidence.qualification_evidence(),
        )
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
        records.push(SchwabRestQuoteRecordRequest::new(
            identity,
            binding.instrument_id(),
            source_identifier,
            provenance,
            terms.price_tick(),
            terms.lot_size(),
            market_data,
        ));
    }
    Ok(SchwabRestQuotePublicationRequest::new(records))
}

fn current_instruments(
    bindings: &[SchwabRestQuoteInstrumentBinding],
    metadata: &SourceMetadata,
) -> Result<Vec<SchwabRestQuoteCurrentInstrument>, SchwabRestQuoteCurrentUnavailable> {
    let mut instruments = Vec::new();
    instruments
        .try_reserve_exact(bindings.len())
        .map_err(|_error| SchwabRestQuoteCurrentUnavailable::Allocation)?;
    for binding in bindings {
        let provider_identity = binding
            .binding()
            .provider_identity()
            .ok_or(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        if provider_identity.source_id() != metadata.source_id()
            || provider_identity.instrument_id() != binding.instrument_id()
        {
            return Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth);
        }
        let provider_symbol = ProviderIdentifier::try_new(binding.provider_symbol().to_owned())
            .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        let source_identifier =
            SourceIdentifier::try_from(provider_identity.provider_instrument_id().as_str())
                .map_err(|_error| SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)?;
        instruments.push(SchwabRestQuoteCurrentInstrument::try_new(
            provider_symbol,
            source_identifier,
            binding.instrument_id(),
            binding.binding().execution_terms(),
        )?);
    }
    Ok(instruments)
}

#[allow(
    clippy::too_many_arguments,
    reason = "dispatch validation joins every producer/sink authority coordinate"
)]
fn validate_dispatch(
    metadata: &SourceMetadata,
    evidence: &SchwabRestQuoteSourceEvidence,
    oauth: SchwabOAuthAuthorityReceipt,
    accounting: RestItemAccounting,
    token_generation: u64,
    route: ReadOnlyRoute,
    captured_accounting: RestItemAccounting,
) -> Result<(), SchwabRestQuoteSinkError> {
    if evidence.metadata() != metadata
        || !metadata.is_effective_at(wall_timestamp()?)
        || token_generation != oauth.generation().get()
        || route != ReadOnlyRoute::Quotes
        || captured_accounting != accounting
        || accounting.requested == 0
        || accounting.returned.checked_add(accounting.missing) != Some(accounting.requested)
        || accounting.returned > accounting.provider_records
    {
        return Err(SchwabRestQuoteSinkError::InvalidReceipt);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "accepted publication validation retains exact source/OAuth/instrument lineage"
)]
fn validate_publication(
    publication: &SchwabSealedRestQuotePublication,
    metadata: &SourceMetadata,
    evidence: &SchwabRestQuoteSourceEvidence,
    bindings: &[SchwabRestQuoteInstrumentBinding],
    connection_generation: ConnectionGeneration,
    payload_digest: EvidenceDigest,
    provider_dataset: &SourceIdentifier,
) -> Result<(), SchwabRestQuoteSinkError> {
    let binding = publication.binding();
    binding
        .validate()
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?;
    if binding.batch().source_id() != metadata.source_id()
        || binding.batch().metadata_revision() != metadata.revision()
        || binding.batch().dataset() != provider_dataset
        || binding.native_lineage().implementation()
            != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
        || binding.native_lineage().rows().len() != binding.record_count()
        || binding.record_count() == 0
    {
        return Err(SchwabRestQuoteSinkError::InvalidReceipt);
    }
    for event in binding.batch().events() {
        let MarketEvent::Quote(quote) = event else {
            return Err(SchwabRestQuoteSinkError::InvalidReceipt);
        };
        let provenance = quote.provenance();
        let event_binding = provenance.binding();
        let exact_instrument = bindings.iter().any(|candidate| {
            candidate.instrument_id() == event_binding.instrument_id()
                && candidate
                    .binding()
                    .provider_identity()
                    .is_some_and(|provider| {
                        provider.source_id() == metadata.source_id()
                            && provider.provider_instrument_id().as_str()
                                == event_binding.source_identifier().as_str()
                    })
        });
        if !exact_instrument
            || event_binding.source_id() != metadata.source_id()
            || event_binding.metadata_revision() != metadata.revision()
            || event_binding.connection_generation() != connection_generation
            || event_binding.venue_id() != evidence.venue_id()
            || event_binding.provider_product() != evidence.provider_product()
            || event_binding.provider_channel() != evidence.provider_channel()
            || event_binding.event_class() != LiveEventClass::Quote
            || event_binding.payload_digest() != payload_digest
            || provenance.recorded_quality() != evidence.quality()
        {
            return Err(SchwabRestQuoteSinkError::InvalidReceipt);
        }
    }
    Ok(())
}

fn quote_coverage_status(
    evidence: &SchwabRestQuoteSourceEvidence,
    instrument_id: market_squawk_domain::InstrumentId,
) -> CoverageStatus {
    let coverage = evidence.metadata().coverage();
    let Some(live) = coverage.live() else {
        return CoverageStatus::Unknown;
    };
    let delay_matches = match (coverage.delay(), evidence.delay()) {
        (market_squawk_domain::CoverageDelay::RealTime, SchwabRestDelayEvidence::RealTime) => true,
        (
            market_squawk_domain::CoverageDelay::Delayed(declared),
            SchwabRestDelayEvidence::Delayed(observed),
        ) => declared == observed.get(),
        _ => false,
    };
    if live.provider_product() != evidence.provider_product()
        || live.provider_channel() != evidence.provider_channel()
        || live.rule_for(LiveEventClass::Quote, None).is_none()
        || !coverage.topology().contains_venue(evidence.venue_id())
        || !delay_matches
    {
        return CoverageStatus::Insufficient;
    }
    match coverage.instruments().membership(instrument_id) {
        InstrumentCoverageMembership::Enumerated
        | InstrumentCoverageMembership::EvidenceBackedUniverse => CoverageStatus::Sufficient,
        InstrumentCoverageMembership::PartialUnproven | InstrumentCoverageMembership::Outside => {
            CoverageStatus::Insufficient
        }
    }
}

fn canonical_quote_state(
    payload_digest: EvidenceDigest,
    instrument_id: market_squawk_domain::InstrumentId,
    provider_symbol: &ProviderIdentifier,
    resolution_evidence: EvidenceDigest,
) -> Result<CanonicalStateDigest, SchwabRestQuoteSinkError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/schwab-rest-quote-state/v1\0");
    digest.update(payload_digest.bytes());
    digest.update(instrument_id.as_uuid().as_bytes());
    hash_field(&mut digest, provider_symbol.as_str().as_bytes())?;
    digest.update(resolution_evidence.bytes());
    let rule = CanonicalizationRule::new(
        SourceIdentifier::try_from(QUOTE_CANONICAL_STATE_RULE)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?,
        RuleVersion::new(QUOTE_CANONICAL_STATE_RULE_VERSION)
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)?,
    );
    Ok(CanonicalStateDigest::new(
        evidence_digest(digest.finalize().into()),
        rule,
    ))
}

fn quote_source_timestamp(
    quote: &market_squawk_adapter_schwab::SchwabQuote,
) -> Result<Option<Timestamp>, SchwabRestQuoteSinkError> {
    match quote
        .quote_fields()
        .iter()
        .find(|field| field.name() == &QuoteComponentField::QuoteTime)
        .map(|field| field.value())
    {
        None | Some(NativeScalar::Null) => Ok(None),
        Some(NativeScalar::Number(value)) => value
            .as_str()
            .parse::<u64>()
            .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)
            .and_then(timestamp_from_unix_millis)
            .map(Some),
        Some(NativeScalar::Bool(_) | NativeScalar::Text(_)) => {
            Err(SchwabRestQuoteSinkError::InvalidReceipt)
        }
    }
}

fn qualification_reference(
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, SchwabRestQuoteSinkError> {
    SourceIdentifier::try_from(encode_hex(digest.bytes()).as_str())
        .map_err(|_error| SchwabRestQuoteSinkError::InvalidReceipt)
}

fn publication_idempotency_key(
    binding_digest: EvidenceDigest,
    oauth: SchwabOAuthAuthorityReceipt,
    payload_digest: EvidenceDigest,
) -> String {
    format!(
        "schwab-rest-quotes-{}-{}-{}",
        oauth.generation().get(),
        encode_hex(payload_digest.bytes()),
        encode_hex(binding_digest.bytes())
    )
}

fn raw_sealed_receipt(
    metadata: &SourceMetadata,
    connection_generation: ConnectionGeneration,
    accounting: RestItemAccounting,
    current: SchwabRestQuoteCurrentPublication,
) -> Result<SchwabRestQuotePublicationReceipt, SchwabRestQuoteSinkError> {
    SchwabRestQuotePublicationReceipt::try_new(
        metadata.source_id().clone(),
        connection_generation,
        accounting,
        true,
        current,
    )
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), SchwabRestQuoteSinkError> {
    let length =
        u64::try_from(value.len()).map_err(|_error| SchwabRestQuoteSinkError::Unavailable)?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn timestamp_from_unix_millis(value: u64) -> Result<Timestamp, SchwabRestQuoteSinkError> {
    let nanos = value
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabRestQuoteSinkError::InvalidReceipt)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn wall_timestamp() -> Result<Timestamp, SchwabRestQuoteSinkError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| SchwabRestQuoteSinkError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| SchwabRestQuoteSinkError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn map_publication_error(error: SchwabMarketPublicationError) -> SchwabRestQuoteSinkError {
    match error {
        SchwabMarketPublicationError::Cancelled => SchwabRestQuoteSinkError::Cancelled,
        SchwabMarketPublicationError::Deadline => SchwabRestQuoteSinkError::Deadline,
        SchwabMarketPublicationError::AuthorityInvalid
        | SchwabMarketPublicationError::FamilyMismatch
        | SchwabMarketPublicationError::Capture(_)
        | SchwabMarketPublicationError::Transport(_)
        | SchwabMarketPublicationError::Quote(_) => SchwabRestQuoteSinkError::InvalidReceipt,
        SchwabMarketPublicationError::AuthorityRevoked
        | SchwabMarketPublicationError::AuthorityExpired
        | SchwabMarketPublicationError::FamilyUnavailable
        | SchwabMarketPublicationError::SourceHealthUnavailable
        | SchwabMarketPublicationError::RestartInvalid
        | SchwabMarketPublicationError::Runtime(_)
        | SchwabMarketPublicationError::Research(_)
        | SchwabMarketPublicationError::Ingest(_)
        | SchwabMarketPublicationError::Rights(_)
        | SchwabMarketPublicationError::History(_)
        | SchwabMarketPublicationError::Option(_)
        | SchwabMarketPublicationError::Streamer(_) => SchwabRestQuoteSinkError::Unavailable,
    }
}

fn post_seal_failure(error: &SchwabMarketPublicationError) -> SchwabRestQuotePostSealFailure {
    match error {
        SchwabMarketPublicationError::Deadline => SchwabRestQuotePostSealFailure::Deadline,
        SchwabMarketPublicationError::Cancelled
        | SchwabMarketPublicationError::AuthorityRevoked
        | SchwabMarketPublicationError::AuthorityExpired => {
            SchwabRestQuotePostSealFailure::ShutdownOrRevocation
        }
        SchwabMarketPublicationError::AuthorityInvalid
        | SchwabMarketPublicationError::FamilyUnavailable
        | SchwabMarketPublicationError::FamilyMismatch
        | SchwabMarketPublicationError::Capture(_)
        | SchwabMarketPublicationError::Transport(_)
        | SchwabMarketPublicationError::Quote(_) => {
            SchwabRestQuotePostSealFailure::AuthorityOrBinding
        }
        SchwabMarketPublicationError::SourceHealthUnavailable
        | SchwabMarketPublicationError::RestartInvalid
        | SchwabMarketPublicationError::Runtime(_)
        | SchwabMarketPublicationError::Research(_)
        | SchwabMarketPublicationError::Ingest(_)
        | SchwabMarketPublicationError::Rights(_)
        | SchwabMarketPublicationError::History(_)
        | SchwabMarketPublicationError::Option(_)
        | SchwabMarketPublicationError::Streamer(_) => {
            SchwabRestQuotePostSealFailure::StorageUnavailable
        }
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}
