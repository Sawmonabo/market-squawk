//! One-product Direct registry, capture, synchronization, and reconnect owner.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use market_squawk_adapter_coinbase::{
    CoinbaseConfigError, CoinbaseDirectConfig, CoinbaseDirectHmacSigner, CoinbaseDirectSession,
    CoinbaseDirectSessionError,
};
use market_squawk_domain::{IdentityError, InstrumentError, SourceIdentifier};
use market_squawk_live::{
    BookError, DepthLimit, LiveIngressBindError, LiveRouteConfig, LiveRuntimeIngress,
    OrderLevelLimitError, OrderLevelLimits, OrderLevelRoute,
};
use market_squawk_platform::{
    AppConfig, CaptureChannelError, CaptureChannelLimits, CaptureGenerationError,
    CaptureProcessInfrastructure, CaptureShutdownStatus, CaptureWorkerReapError,
    CaptureWriterPolicy, CaptureWriterPolicyError, CaptureWriterSpawnError,
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths,
    MemoryCaptureSinkConstructionError, RawCaptureControl, RollingMemoryCaptureSink,
    raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, BudgetUnavailableReason,
    CaptureGenerationCapabilities, ProviderBackoffAuthority, ProviderBackoffDecision,
    ProviderBackoffError, ProviderRateAuthority, RegisteredSource, RegistryError, SessionId,
    SourceError, SourceMetadata, TlsProviderError, install_ring_tls_provider,
};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::provider_activation::CoinbaseDirectRuntimeAdmission;

use super::super::composition::ProductionCoinbaseProfileError;
use super::super::composition::system_timestamp;
use super::super::order_level::{
    MAX_ORDER_LEVEL_INGRESS_COMMANDS, OrderLevelActorLimits, OrderLevelActorShutdown,
    OrderLevelBookKey, OrderLevelDirectory, OrderLevelMonitorError, OrderLevelRegistration,
};
use super::super::route_actor::{RouteActorWorker, RouteBufferLimits, spawn_route_activation};
use super::super::sink::{
    CoinbaseCapturedPublicationIngress, ProductionPredecodedMarketSinkInput,
    ProductionRawMarketSink, ProductionSinkConstructionError, ProductionSinkFailure,
};
use super::super::subscription_state::{
    GenerationIdentity, SubscriptionConstructionError, SubscriptionLimits, SubscriptionStateMachine,
};
use super::output::{CoinbaseDirectOutputFailure, CoinbaseDirectProductOutput};

const CAPTURE_FLUSH_RECORDS: usize = 256;
const CONTROL_AUDIT_RECORDS: usize = 64;
const CONTROL_AUDIT_BYTES: usize = 64 * 1024;
const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;
const SOURCE_AUTHORITY_ROOT: &str = "coinbase-direct-account-authority";
const SOURCE_AUTHORITY_CHILD: &str = "sources";
const LOCAL_CONCURRENCY_RETRY: Duration = Duration::from_millis(25);
const ORDER_LEVEL_OUTSTANDING_READS: usize = 64;

/// One preflight-complete product notification retained by the account startup barrier.
#[derive(Clone, Copy, Debug)]
pub(super) struct ProductReady {
    pub(super) slot: usize,
}

/// Immutable one-product runtime configuration prepared before live-runtime startup.
#[derive(Clone, Debug)]
pub(super) struct ProductRuntimeSpec {
    slot: usize,
    config: CoinbaseDirectConfig,
    route: LiveRouteConfig,
}

impl ProductRuntimeSpec {
    pub(super) const fn new(
        slot: usize,
        config: CoinbaseDirectConfig,
        route: LiveRouteConfig,
    ) -> Self {
        Self {
            slot,
            config,
            route,
        }
    }

    pub(super) const fn slot(&self) -> usize {
        self.slot
    }

    pub(super) const fn route(&self) -> &LiveRouteConfig {
        &self.route
    }

    pub(super) const fn metadata(&self) -> &SourceMetadata {
        self.config.metadata()
    }
}

/// Runs one product until account cancellation or a terminal product defect.
#[allow(
    clippy::too_many_arguments,
    reason = "every product authority and bounded runtime capability remains explicit"
)]
pub(super) async fn run_product(
    spec: ProductRuntimeSpec,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    account_subject: SourceIdentifier,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: CoinbaseCapturedPublicationIngress,
    order_level: Option<OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: Arc<CoinbaseDirectHmacSigner>,
    ready: mpsc::Sender<ProductReady>,
    mut start: watch::Receiver<bool>,
    bootstrap_slots: Arc<Semaphore>,
    cancellation: CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let paths = LocalPaths::prepare(app_config.data_dir())?;
    let authority_store = LocalAuthorityStateStore::try_open(
        paths
            .control_root()?
            .root()
            .join(SOURCE_AUTHORITY_ROOT)
            .join(account_subject.as_str())
            .join(SOURCE_AUTHORITY_CHILD)
            .join(spec.config.metadata().source_id().as_str()),
    )?;
    let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(provider_rate.clone());
    let mut registry =
        AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            authority_store,
            resolver,
            provider_rate,
        )?;
    let registered =
        registry.register_or_resume_exact(spec.config.metadata().clone(), system_timestamp()?)?;
    let backoff = registry.provider_backoff_authority(&registered)?;
    let run = run_product_loop(
        &spec,
        &app_config,
        admission,
        capture_process,
        live_ingress,
        &publication,
        order_level.as_ref(),
        route_buffer_limits,
        signer.as_ref(),
        &ready,
        &mut start,
        &bootstrap_slots,
        &mut registry,
        &registered,
        &backoff,
        &cancellation,
    )
    .await;
    drop(backoff);
    drop(registered);
    let shutdown = registry.shutdown();
    match (run, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(source),
        (Ok(()), Err(shutdown)) => Err(shutdown.into()),
        (Err(source), Err(shutdown)) => Err(CoinbaseDirectProductRuntimeError::RunShutdown {
            source: Box::new(source),
            shutdown,
        }),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the sole product owner receives independent lifecycle authorities explicitly"
)]
async fn run_product_loop(
    spec: &ProductRuntimeSpec,
    app_config: &AppConfig,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: &CoinbaseCapturedPublicationIngress,
    order_level: Option<&OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: &CoinbaseDirectHmacSigner,
    ready: &mpsc::Sender<ProductReady>,
    start: &mut watch::Receiver<bool>,
    bootstrap_slots: &Arc<Semaphore>,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    backoff: &ProviderBackoffAuthority,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let mut ready_sent = false;
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let startup = (!ready_sent).then_some((ready, &mut *start));
        let outcome = run_generation(
            spec,
            app_config,
            admission,
            capture_process,
            live_ingress.clone(),
            publication.clone(),
            order_level,
            route_buffer_limits,
            signer,
            registry,
            registered,
            startup,
            bootstrap_slots,
            cancellation.child_token(),
        )
        .await;
        if !ready_sent {
            ready_sent = outcome.ready_sent;
        }
        match outcome.result {
            Ok(()) if cancellation.is_cancelled() => return Ok(()),
            Ok(()) => return Err(CoinbaseDirectProductRuntimeError::SourceExited),
            Err(CoinbaseDirectProductRuntimeError::Session(
                CoinbaseDirectSessionError::Source(SourceError::Cancelled),
            )) if cancellation.is_cancelled() => return Ok(()),
            Err(error) if !ready_sent || !error.recoverable() => return Err(error),
            Err(error) => {
                wait_after_failure(
                    error,
                    backoff,
                    spec.config.limits().product_refresh_interval(),
                    cancellation,
                )
                .await?;
            }
        }
    }
}

struct GenerationOutcome {
    ready_sent: bool,
    result: Result<(), CoinbaseDirectProductRuntimeError>,
}

async fn register_order_level_generation(
    directory: Option<&OrderLevelDirectory>,
    spec: &ProductRuntimeSpec,
    generation: market_squawk_domain::ConnectionGeneration,
    cancellation: &CancellationToken,
) -> Result<Option<OrderLevelRegistration>, CoinbaseDirectProductRuntimeError> {
    let Some(directory) = directory else {
        return Ok(None);
    };
    let book = spec.config.limits().book();
    let retained_bytes = u32::try_from(spec.config.checked_maximum_retained_bytes()?)
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let order_units = book
        .max_orders()
        .checked_add(book.max_queue_events())
        .and_then(|value| u32::try_from(value).ok())
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let read_order_units = u32::try_from(book.max_orders())
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let actor_limits = OrderLevelActorLimits::try_new(
        NonZeroUsize::new(
            book.max_queue_events()
                .min(MAX_ORDER_LEVEL_INGRESS_COMMANDS),
        )
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?,
        retained_bytes,
        order_units,
        NonZeroUsize::new(ORDER_LEVEL_OUTSTANDING_READS)
            .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?,
        retained_bytes,
        read_order_units,
    )
    .map_err(|error| {
        tracing::error!(%error, "Coinbase Direct order-level actor configuration failed");
        CoinbaseDirectProductRuntimeError::OrderLevelConfiguration
    })?;
    let route = OrderLevelRoute::new(
        spec.config.metadata().source_id().clone(),
        spec.config.venue().clone(),
        spec.config.instrument(),
        spec.config.product().as_source_identifier().clone(),
        generation,
    );
    let limits =
        OrderLevelLimits::new(book.max_orders(), DepthLimit::new(book.published_depth())?)?;
    let deadline = Instant::now()
        .checked_add(spec.config.limits().websocket().connect_timeout())
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    directory
        .register(route, limits, actor_limits, cancellation, deadline)
        .await
        .map(Some)
        .map_err(|error| {
            tracing::error!(%error, "Coinbase Direct order-level generation registration failed");
            CoinbaseDirectProductRuntimeError::OrderLevelDirectory
        })
}

async fn unregister_order_level_generation(
    directory: &OrderLevelDirectory,
    key: &OrderLevelBookKey,
    app_config: &AppConfig,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let deadline = Instant::now()
        .checked_add(app_config.source_shutdown())
        .ok_or(CoinbaseDirectProductRuntimeError::OrderLevelAccounting)?;
    let cleanup = CancellationToken::new();
    let result = directory
        .unregister(key, &cleanup, deadline)
        .await
        .map_err(|error| {
            tracing::error!(%error, "Coinbase Direct order-level generation cleanup failed");
            CoinbaseDirectProductRuntimeError::OrderLevelDirectory
        })?;
    if result == OrderLevelActorShutdown::Graceful {
        Ok(())
    } else {
        Err(CoinbaseDirectProductRuntimeError::OrderLevelShutdownIncomplete)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "generation construction keeps every authority and cleanup owner explicit"
)]
async fn run_generation(
    spec: &ProductRuntimeSpec,
    app_config: &AppConfig,
    admission: CoinbaseDirectRuntimeAdmission,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    publication: CoinbaseCapturedPublicationIngress,
    order_level: Option<&OrderLevelDirectory>,
    route_buffer_limits: RouteBufferLimits,
    signer: &CoinbaseDirectHmacSigner,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    startup: Option<(&mpsc::Sender<ProductReady>, &mut watch::Receiver<bool>)>,
    bootstrap_slots: &Arc<Semaphore>,
    cancellation: CancellationToken,
) -> GenerationOutcome {
    let started_at = match system_timestamp() {
        Ok(value) => value,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let session_id = match SourceIdentifier::try_from(format!(
        "{}-{}",
        spec.config.metadata().source_id(),
        uuid::Uuid::new_v4()
    )) {
        Ok(value) => SessionId::new(value),
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let session = match registry.begin_next_session(registered, session_id, started_at) {
        Ok(value) => value,
        Err(error) => {
            return GenerationOutcome {
                ready_sent: false,
                result: Err(error.into()),
            };
        }
    };
    let route_cancellation = cancellation.child_token();
    let mut capture_control: Option<RawCaptureControl<CaptureGenerationCapabilities>> = None;
    let mut capture_writer = None;
    let mut route_worker: Option<RouteActorWorker> = None;
    let mut order_level_key: Option<OrderLevelBookKey> = None;
    let mut ready_sent = false;

    let run = async {
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let health_reporter = registry.take_current_health_reporter(&session)?;
        let (publisher, control, writer) = raw_capture_channel(
            &capture_process,
            CaptureChannelLimits::new(
                admission.capture_queue_records_per_product(),
                admission.capture_queue_bytes_per_product(),
            ),
            capabilities,
        )?;
        let sink = RollingMemoryCaptureSink::try_new(
            admission.capture_queue_records_per_product(),
            admission.capture_queue_bytes_per_product(),
        )?;
        let flush_records = NonZeroUsize::new(
            admission
                .capture_queue_records_per_product()
                .get()
                .min(CAPTURE_FLUSH_RECORDS),
        )
        .ok_or(CoinbaseDirectProductRuntimeError::InvalidStaticPolicy)?;
        let policy =
            CaptureWriterPolicy::try_new(flush_records, app_config.capture_flush_interval())?;
        let writer = spawn_capture_writer(writer, sink, policy)?;
        capture_control = Some(control);
        capture_writer = Some(writer);
        capture_control
            .as_mut()
            .ok_or(CoinbaseDirectProductRuntimeError::CaptureOwnerMissing)?
            .activate_initial()?;

        let source_generation = registry.take_live_source_generation(&session)?;
        let order_level_registration = register_order_level_generation(
            order_level,
            spec,
            session.generation(),
            &cancellation,
        )
        .await?;
        let (order_level_ingress, mut order_level_monitor) = match order_level_registration {
            Some(registration) => {
                order_level_key = Some(registration.key().clone());
                let (ingress, monitor) = registration.into_parts();
                (Some(ingress), Some(monitor))
            }
            None => (None, None),
        };
        let dormant = live_ingress.reserve_route(spec.route.route().clone())?;
        let (route, worker) =
            spawn_route_activation(dormant, route_buffer_limits, route_cancellation.clone());
        route_worker = Some(worker);
        let subscription = SubscriptionStateMachine::try_new(
            GenerationIdentity::from_session(&session),
            [spec.config.product().as_source_identifier().as_str()],
            spec.config.limits().websocket().io_timeout(),
            Instant::now(),
            SubscriptionLimits::try_new(CONTROL_AUDIT_RECORDS, CONTROL_AUDIT_BYTES, 0, 0)?,
        )?;
        let mut source = CoinbaseDirectSession::try_new(
            spec.config.clone(),
            source_generation,
            install_ring_tls_provider()?,
        )?;
        let mut sink =
            ProductionRawMarketSink::try_new_predecoded(ProductionPredecodedMarketSinkInput {
                capture: publisher,
                registry,
                session: &session,
                health_reporter,
                metadata: spec.config.metadata().clone(),
                subscription,
                live_ingress,
                routes: vec![route],
            })?;
        if let Some((ready, start)) = startup {
            ready
                .try_send(ProductReady { slot: spec.slot })
                .map_err(|_error| CoinbaseDirectProductRuntimeError::SupervisorQueue)?;
            ready_sent = true;
            wait_for_account_start(start, &cancellation).await?;
        }

        let bootstrap_permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into());
            }
            permit = Arc::clone(bootstrap_slots).acquire_owned() => {
                permit.map_err(
                    |_error| CoinbaseDirectProductRuntimeError::BootstrapGateClosed,
                )?
            }
        };
        let order_level_publish_timeout = order_level_ingress
            .as_ref()
            .map(|_| spec.config.limits().websocket().io_timeout());
        let mut output = CoinbaseDirectProductOutput::new(
            &mut sink,
            spec.config.product().clone(),
            bootstrap_permit,
            order_level_ingress,
            order_level_publish_timeout,
            publication,
            spec.config
                .metadata()
                .coverage()
                .live()
                .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?
                .provider_product()
                .as_source_identifier()
                .clone(),
            spec.config
                .metadata()
                .coverage()
                .live()
                .ok_or(CoinbaseDirectProductRuntimeError::ActivationBinding)?
                .provider_channel()
                .as_source_identifier()
                .clone(),
        );
        let session_result = match order_level_monitor.as_mut() {
            Some(monitor) => tokio::select! {
                biased;
                terminal = monitor.wait_until_terminal(&cancellation) => match terminal {
                    Ok(failure) => {
                        tracing::error!(%failure, "Coinbase Direct order-level actor failed terminally");
                        Err(CoinbaseDirectProductRuntimeError::OrderLevelTerminal)
                    }
                    Err(OrderLevelMonitorError::Cancelled) if cancellation.is_cancelled() => {
                        Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into())
                    }
                    Err(error) => {
                        tracing::error!(%error, "Coinbase Direct order-level monitor failed");
                        Err(CoinbaseDirectProductRuntimeError::OrderLevelMonitor)
                    }
                },
                result = source.run(signer, &mut output, cancellation.clone()) => {
                    result.map_err(Into::into)
                }
            },
            None => source
                .run(signer, &mut output, cancellation.clone())
                .await
                .map_err(Into::into),
        };
        let output_failure = output.terminal_failure();
        drop(output);
        let sink_failure = sink.terminal_failure();
        drop(sink);
        if let Some(failure) = output_failure {
            return Err(failure.into());
        }
        if let Some(failure) = sink_failure {
            return Err(failure.into());
        }
        session_result
    }
    .await;

    route_cancellation.cancel();
    let mut cleanup = None;
    if let (Some(directory), Some(key)) = (order_level, order_level_key.as_ref()) {
        let result = unregister_order_level_generation(directory, key, app_config).await;
        retain_first_error(&mut cleanup, result);
    }
    if let Some(worker) = route_worker {
        let result = cleanup_route_worker(worker).await;
        retain_first_error(&mut cleanup, result);
    }
    let ended_at = system_timestamp().unwrap_or(started_at);
    retain_first_error(
        &mut cleanup,
        registry.end_session(&session, ended_at).map_err(Into::into),
    );
    if let Some(mut control) = capture_control {
        control.invalidate_current();
        drop(control);
    }
    if let Some(writer) = capture_writer {
        retain_first_error(
            &mut cleanup,
            shutdown_capture_writer(writer, app_config.capture_shutdown()).await,
        );
    }
    let result = match (run, cleanup) {
        (Ok(()), None) => Ok(()),
        (Err(source), None) => Err(source),
        (Ok(()), Some(cleanup)) => Err(cleanup),
        (Err(source), Some(cleanup)) => Err(CoinbaseDirectProductRuntimeError::RunCleanup {
            source: Box::new(source),
            cleanup: Box::new(cleanup),
        }),
    };
    GenerationOutcome { ready_sent, result }
}

async fn wait_for_account_start(
    start: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    while !*start.borrow() {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(CoinbaseDirectSessionError::Source(SourceError::Cancelled).into());
            }
            changed = start.changed() => {
                changed.map_err(|_error| CoinbaseDirectProductRuntimeError::StartupBarrier)?;
            }
        }
    }
    Ok(())
}

async fn cleanup_route_worker(
    worker: RouteActorWorker,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    match worker.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(failure)) => Err(ProductionSinkFailure::RouteActivation(failure).into()),
        Err(error) => Err(CoinbaseDirectProductRuntimeError::RouteTask(error)),
    }
}

async fn shutdown_capture_writer(
    writer: market_squawk_platform::CaptureWriterHandle<CaptureGenerationCapabilities>,
    deadline: Duration,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    let mut pending = writer.shutdown(deadline);
    let status = pending.wait_until_deadline().await;
    if status == CaptureShutdownStatus::DeadlineElapsed {
        pending.wait_until_terminated().await;
    }
    let termination = pending
        .try_reap()?
        .ok_or(CoinbaseDirectProductRuntimeError::CaptureOwnerMissing)?;
    if status == CaptureShutdownStatus::DeadlineElapsed
        || termination.shutdown_deadline_elapsed()
        || termination.outcome().is_incomplete()
    {
        return Err(CoinbaseDirectProductRuntimeError::CaptureShutdownIncomplete);
    }
    Ok(())
}

fn retain_first_error(
    retained: &mut Option<CoinbaseDirectProductRuntimeError>,
    candidate: Result<(), CoinbaseDirectProductRuntimeError>,
) {
    if retained.is_none() {
        *retained = candidate.err();
    }
}

async fn wait_after_failure(
    error: CoinbaseDirectProductRuntimeError,
    backoff: &ProviderBackoffAuthority,
    product_status_retry: Duration,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    if matches!(
        &error,
        CoinbaseDirectProductRuntimeError::Session(CoinbaseDirectSessionError::Source(
            SourceError::BudgetUnavailable {
                reason: BudgetUnavailableReason::ConcurrencyExhausted,
            },
        ))
    ) {
        return wait_for_local_retry(LOCAL_CONCURRENCY_RETRY, cancellation).await;
    }
    if matches!(
        &error,
        CoinbaseDirectProductRuntimeError::Output(CoinbaseDirectOutputFailure::ProductUnavailable,)
    ) {
        return wait_for_local_retry(product_status_retry, cancellation).await;
    }
    let deadline = match &error {
        CoinbaseDirectProductRuntimeError::Session(CoinbaseDirectSessionError::Source(
            SourceError::BudgetWaitUntil { deadline },
        )) => *deadline,
        _ => match backoff.apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS)? {
            ProviderBackoffDecision::WaitUntil(deadline) => deadline,
            ProviderBackoffDecision::Unavailable(reason) => {
                return Err(CoinbaseDirectProductRuntimeError::BudgetUnavailable(reason));
            }
        },
    };
    let wait = backoff.remaining_wait(deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

async fn wait_for_local_retry(
    duration: Duration,
    cancellation: &CancellationToken,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        () = tokio::time::sleep(duration) => Ok(()),
    }
}

/// Product construction, generation, capture, reconnect, or cleanup failure.
#[derive(Debug, Error)]
pub enum CoinbaseDirectProductRuntimeError {
    /// Activation evidence is incomplete or inconsistent with Direct runtime construction.
    #[error("Coinbase Direct activation evidence is incomplete")]
    ActivationBinding,
    /// Canonical metadata evidence could not be represented.
    #[error("Coinbase Direct metadata evidence encoding failed")]
    EvidenceEncoding,
    /// Checked order-level resource accounting could not be represented.
    #[error("Coinbase Direct order-level accounting is invalid")]
    OrderLevelAccounting,
    /// The exact generation-owned order-level actor did not shut down cleanly.
    #[error("Coinbase Direct order-level actor shutdown was incomplete")]
    OrderLevelShutdownIncomplete,
    /// The exact generation-owned order-level actor entered a terminal fail-closed state.
    #[error("Coinbase Direct order-level actor failed terminally")]
    OrderLevelTerminal,
    /// A static bounded policy unexpectedly produced zero.
    #[error("Coinbase Direct static runtime policy is invalid")]
    InvalidStaticPolicy,
    /// A generation lost an explicitly retained capture owner.
    #[error("Coinbase Direct capture ownership is incomplete")]
    CaptureOwnerMissing,
    /// The transient capture worker did not drain and terminate cleanly.
    #[error("Coinbase Direct capture shutdown was incomplete")]
    CaptureShutdownIncomplete,
    /// A product completed before account cancellation.
    #[error("Coinbase Direct product source exited unexpectedly")]
    SourceExited,
    /// The bounded account startup channel rejected a ready notification.
    #[error("Coinbase Direct account startup queue is unavailable")]
    SupervisorQueue,
    /// The account startup barrier closed before network release.
    #[error("Coinbase Direct account startup barrier closed")]
    StartupBarrier,
    /// The account-wide bootstrap admission owner closed unexpectedly.
    #[error("Coinbase Direct account bootstrap admission closed")]
    BootstrapGateClosed,
    /// Shared provider budget admission is terminally unavailable.
    #[error("Coinbase Direct provider budget is unavailable: {0:?}")]
    BudgetUnavailable(BudgetUnavailableReason),
    /// Product runtime and registry shutdown both failed.
    #[error("Coinbase Direct product runtime and registry shutdown both failed")]
    RunShutdown {
        /// Primary product failure.
        source: Box<Self>,
        /// Registry shutdown failure.
        shutdown: RegistryError,
    },
    /// Product generation and its bounded cleanup both failed.
    #[error("Coinbase Direct generation and bounded cleanup both failed")]
    RunCleanup {
        /// Primary generation failure.
        source: Box<Self>,
        /// Cleanup failure.
        cleanup: Box<Self>,
    },
    /// The Direct adapter rejected configuration or exact runtime bounds.
    #[error(transparent)]
    Configuration(#[from] CoinbaseConfigError),
    /// Trusted wall-clock conversion failed.
    #[error(transparent)]
    Clock(#[from] ProductionCoinbaseProfileError),
    /// Stable financial identity construction failed.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// Price-level projection depth could not be represented.
    #[error(transparent)]
    OrderLevelBook(#[from] BookError),
    /// Canonical order-level retained-state limits were invalid.
    #[error(transparent)]
    OrderLevelLimit(#[from] OrderLevelLimitError),
    /// Application actor limits were invalid.
    #[error("Coinbase Direct order-level actor configuration failed")]
    OrderLevelConfiguration,
    /// The process-wide order-level directory rejected this generation.
    #[error("Coinbase Direct order-level directory operation failed")]
    OrderLevelDirectory,
    /// The order-level supervisor monitor failed before the source exited.
    #[error("Coinbase Direct order-level supervisor monitor failed")]
    OrderLevelMonitor,
    /// Authorization or coverage interval construction failed.
    #[error(transparent)]
    Interval(#[from] InstrumentError),
    /// Local path preparation failed.
    #[error(transparent)]
    Paths(#[from] market_squawk_platform::PathError),
    /// Durable authority-store ownership failed.
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    /// Source registry authority failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Capture channel construction failed.
    #[error(transparent)]
    CaptureChannel(#[from] CaptureChannelError),
    /// Capture generation activation failed.
    #[error(transparent)]
    CaptureGeneration(#[from] CaptureGenerationError),
    /// Transient capture storage construction failed.
    #[error(transparent)]
    CaptureStorage(#[from] MemoryCaptureSinkConstructionError),
    /// Capture writer policy construction failed.
    #[error(transparent)]
    CapturePolicy(#[from] CaptureWriterPolicyError),
    /// Capture worker startup failed.
    #[error(transparent)]
    CaptureWriter(#[from] CaptureWriterSpawnError),
    /// Capture worker reap failed.
    #[error(transparent)]
    CaptureReap(#[from] CaptureWorkerReapError),
    /// Live-route reservation failed.
    #[error(transparent)]
    RouteBind(#[from] LiveIngressBindError),
    /// Route actor task failed.
    #[error("Coinbase Direct route actor task failed")]
    RouteTask(tokio::task::JoinError),
    /// Subscription state construction failed.
    #[error(transparent)]
    Subscription(#[from] SubscriptionConstructionError),
    /// Predecoded production sink construction failed.
    #[error(transparent)]
    SinkConstruction(#[from] ProductionSinkConstructionError),
    /// Production sink authority failed closed.
    #[error("Coinbase Direct production sink failed closed: {0}")]
    Sink(#[from] ProductionSinkFailure),
    /// Direct-specific capture or current-product qualification failed.
    #[error(transparent)]
    Output(#[from] CoinbaseDirectOutputFailure),
    /// Direct transport, synchronization, or signing failed.
    #[error(transparent)]
    Session(#[from] CoinbaseDirectSessionError),
    /// TLS provider installation failed.
    #[error(transparent)]
    Tls(#[from] TlsProviderError),
    /// Shared provider refusal backoff failed.
    #[error(transparent)]
    Backoff(#[from] ProviderBackoffError),
}

impl CoinbaseDirectProductRuntimeError {
    fn recoverable(&self) -> bool {
        match self {
            Self::Sink(failure) => failure.requires_generation_resynchronization(),
            Self::Output(CoinbaseDirectOutputFailure::ProductUnavailable) => true,
            Self::Output(CoinbaseDirectOutputFailure::OrderLevelPublication)
            | Self::OrderLevelTerminal
            | Self::OrderLevelMonitor => true,
            Self::Session(CoinbaseDirectSessionError::Source(source)) => matches!(
                source,
                SourceError::Network
                    | SourceError::ConnectionIdle
                    | SourceError::FrameTooLarge { .. }
                    | SourceError::GenerationResynchronizationRequired
                    | SourceError::ProviderUnavailable
                    | SourceError::BudgetWaitUntil { .. }
                    | SourceError::BudgetUnavailable {
                        reason: BudgetUnavailableReason::ConcurrencyExhausted,
                    }
            ),
            Self::Session(
                CoinbaseDirectSessionError::Decode(_)
                | CoinbaseDirectSessionError::Product(_)
                | CoinbaseDirectSessionError::Snapshot(_)
                | CoinbaseDirectSessionError::Book(_)
                | CoinbaseDirectSessionError::Capture(_)
                | CoinbaseDirectSessionError::Subscription
                | CoinbaseDirectSessionError::WebSocketProtocol
                | CoinbaseDirectSessionError::HttpResponse
                | CoinbaseDirectSessionError::HttpDeadline
                | CoinbaseDirectSessionError::HttpBodyTooLarge
                | CoinbaseDirectSessionError::HttpSegmentLimit,
            ) => true,
            _ => false,
        }
    }
}
