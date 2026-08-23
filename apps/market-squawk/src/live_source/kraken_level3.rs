//! Authenticated Kraken order-level runtime with generation-owned capture and read state.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use market_squawk_adapter_kraken::{
    KRAKEN_L3_CHECKSUM_SCOPE_ID, KRAKEN_L3_WEBSOCKET_ENDPOINT, KrakenL3BatchKind,
    KrakenL3BookBatch, KrakenL3Config, KrakenL3Control, KrakenL3DecodeError, KrakenL3DecodeOutcome,
    KrakenL3Decoder, KrakenL3OrderEventKind, KrakenL3ScaleError,
};
use market_squawk_domain::{
    ChecksumCapability, ChecksumEvidence, ChecksumScope, ChecksumValue, DataQuality, IdentityError,
    InstrumentExecutionTerms, InstrumentId, IntegrityEvidenceError, IntegrityRule, MarketDepth,
    SequenceEvidence, SourceIdentifier, Timestamp,
};
use market_squawk_live::{
    BookError, BookSide, DepthLimit, OrderLevelBatch, OrderLevelBatchInput, OrderLevelBatchPayload,
    OrderLevelEvent, OrderLevelLimitError, OrderLevelLimits, OrderLevelOperation,
    OrderLevelPriorityUpdate, OrderLevelQuarantineReason, OrderLevelRoute, OrderLevelVisibleOrder,
    UnknownOrderDisposition,
};
use market_squawk_platform::{
    AppConfig, CaptureChannelError, CaptureChannelLimits, CaptureGenerationError,
    CaptureProcessInfrastructure, CapturePublishError, CaptureShutdownStatus,
    CaptureWorkerReapError, CaptureWriterPolicy, CaptureWriterPolicyError, CaptureWriterSpawnError,
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths,
    MemoryCaptureSinkConstructionError, RawCaptureControl, RollingMemoryCaptureSink,
    raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, BudgetDispatchDecision,
    BudgetReservationDecision, BudgetUnavailableReason, CaptureGenerationCapabilities,
    ChecksumValidationProfile, DecoderEvidence, ProviderBackoffAuthority, ProviderBackoffDecision,
    ProviderBackoffError, ProviderOrderChangeReason, ProviderRateAuthority, RegistryError,
    SequenceValidationProfile, SessionId, SourceError, SourceProtocolProfile, TransportFrameKind,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{oneshot, watch},
    task::{JoinError, JoinSet},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderOnboardingError,
    provider_activation::{
        KrakenL3AccountActivation, KrakenL3ActivationError, MarketInstrumentBinding,
    },
};

use super::order_level::{
    MAX_ORDER_LEVEL_INGRESS_COMMANDS, OrderLevelActorLimits, OrderLevelActorShutdown,
    OrderLevelBookKey, OrderLevelDirectory, OrderLevelDirectoryError, OrderLevelIngress,
    OrderLevelMonitorError, OrderLevelReadError, OrderLevelRegistration, OrderLevelTerminalFailure,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const CURRENTNESS_INTERVAL: Duration = Duration::from_secs(1);
const SUBSCRIPTION_RATE_WINDOW: Duration = Duration::from_secs(1);
const LOCAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const CAPTURE_FLUSH_RECORDS: usize = 256;
const ACTOR_INGRESS_COMMANDS: usize = 256;
const ACTOR_OUTSTANDING_READS: usize = 32;
const ACTOR_MINIMUM_BYTES: usize = 1024 * 1024;
const READ_BUFFER_BYTES: usize = 64 * 1024;
const WRITE_BUFFER_BYTES: usize = 16 * 1024;
const MAX_WRITE_BUFFER_BYTES: usize = 128 * 1024;
const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;
const SOURCE_AUTHORITY_ROOT: &str = "kraken-level3-account-authority";

/// Running authenticated Kraken L3 account owner.
///
/// The runtime exposes read-only order-derived projections. It has no strategy hook, order
/// submission, risk bypass, or execution-quality promotion surface.
#[derive(Debug)]
pub(crate) struct KrakenLevel3LiveRuntime {
    cancellation: CancellationToken,
    healthy: Arc<AtomicBool>,
    current_keys: watch::Receiver<Arc<[OrderLevelBookKey]>>,
    supervisor: tokio::task::JoinHandle<Result<(), KrakenLevel3RuntimeError>>,
    published_shutdown: KrakenPublishedShutdownState,
    shutdown_deadline: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KrakenPublishedShutdownState {
    Running,
    Complete,
    Failed,
}

impl KrakenLevel3LiveRuntime {
    /// Reports fully acknowledged, checksum-synchronized account readiness.
    pub(crate) fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Returns the exact current order-level actor key for one configured instrument.
    ///
    /// The clone contains no mutable authority and lets the application release its registry
    /// ownership lock before issuing the bounded actor read. Callers must revalidate the owning
    /// group after the read before publishing the result.
    pub(crate) fn current_key(&self, instrument: InstrumentId) -> Option<OrderLevelBookKey> {
        self.current_keys
            .borrow()
            .iter()
            .find(|key| key.instrument_id() == instrument)
            .cloned()
    }

    /// Synchronously closes this published runtime to new work without consuming its task owner.
    pub(crate) fn begin_shutdown(&self) {
        self.healthy.store(false, Ordering::Release);
        self.cancellation.cancel();
    }

    /// Waits for published cleanup while retaining the supervisor for an exact retry.
    pub(crate) async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), KrakenLevel3RuntimeError> {
        self.begin_shutdown();
        match self.published_shutdown {
            KrakenPublishedShutdownState::Complete => return Ok(()),
            KrakenPublishedShutdownState::Failed => {
                return Err(KrakenLevel3RuntimeError::PublishedShutdownFailed);
            }
            KrakenPublishedShutdownState::Running => {}
        }
        if cancellation.is_cancelled() {
            return Err(KrakenLevel3RuntimeError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(KrakenLevel3RuntimeError::ShutdownDeadline);
        }
        let outcome = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(KrakenLevel3RuntimeError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(KrakenLevel3RuntimeError::ShutdownDeadline);
            }
            outcome = &mut self.supervisor => outcome,
        };
        match outcome {
            Ok(Ok(())) => {
                self.published_shutdown = KrakenPublishedShutdownState::Complete;
                Ok(())
            }
            Ok(Err(error)) => {
                tracing::error!(%error, "published Kraken level-3 supervisor failed terminally");
                self.published_shutdown = KrakenPublishedShutdownState::Failed;
                Err(KrakenLevel3RuntimeError::PublishedShutdownFailed)
            }
            Err(error) => {
                tracing::error!(%error, "published Kraken level-3 supervisor task failed terminally");
                self.published_shutdown = KrakenPublishedShutdownState::Failed;
                Err(KrakenLevel3RuntimeError::PublishedShutdownFailed)
            }
        }
    }

    /// Cancels the account owner and waits for exact-generation cleanup.
    pub(crate) async fn shutdown(mut self) -> Result<(), KrakenLevel3RuntimeError> {
        self.begin_shutdown();
        match self.published_shutdown {
            KrakenPublishedShutdownState::Complete => return Ok(()),
            KrakenPublishedShutdownState::Failed => {
                return Err(KrakenLevel3RuntimeError::PublishedShutdownFailed);
            }
            KrakenPublishedShutdownState::Running => {}
        }
        match tokio::time::timeout(self.shutdown_deadline, &mut self.supervisor).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(KrakenLevel3RuntimeError::SupervisorTask(error)),
            Err(_elapsed) => {
                self.supervisor.abort();
                let _aborted = (&mut self.supervisor).await;
                Err(KrakenLevel3RuntimeError::ShutdownDeadline)
            }
        }
    }
}

impl Drop for KrakenLevel3LiveRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

impl KrakenL3AccountActivation {
    /// Starts the authenticated multi-symbol L3 owner after exact binding preflight.
    #[allow(
        clippy::too_many_arguments,
        reason = "account, catalog, capture, rate, and actor authorities remain explicit"
    )]
    pub(crate) async fn start_order_level_runtime(
        mut self,
        app_config: AppConfig,
        provider_rate: ProviderRateAuthority,
        capture_process: CaptureProcessInfrastructure,
        instruments: Box<[MarketInstrumentBinding]>,
        directory: OrderLevelDirectory,
        cancellation: CancellationToken,
    ) -> Result<KrakenLevel3LiveRuntime, KrakenLevel3RuntimeError> {
        self.require_current().await?;
        let config = self
            .take_config()
            .ok_or(KrakenLevel3RuntimeError::ActivationTopology)?;
        let specs = validate_instruments(&config, instruments)?;
        let shutdown_deadline = app_config.source_shutdown();
        let (keys_sender, keys_receiver) = watch::channel(Arc::<[OrderLevelBookKey]>::from([]));
        let (startup_sender, startup_receiver) = oneshot::channel();
        let healthy = Arc::new(AtomicBool::new(false));
        let task_healthy = Arc::clone(&healthy);
        let task_cancellation = cancellation.clone();
        let task_directory = directory.clone();
        let mut supervisor = tokio::spawn(async move {
            let result = run_registry_owner(
                self,
                config,
                specs,
                app_config,
                provider_rate,
                capture_process,
                task_directory,
                keys_sender,
                task_healthy,
                task_cancellation.clone(),
                startup_sender,
            )
            .await;
            task_cancellation.cancel();
            result
        });
        tokio::select! {
            startup = startup_receiver => match startup {
                Ok(()) => Ok(KrakenLevel3LiveRuntime {
                    cancellation,
                    healthy,
                    current_keys: keys_receiver,
                    supervisor,
                    published_shutdown: KrakenPublishedShutdownState::Running,
                    shutdown_deadline,
                }),
                Err(_closed) => map_supervisor_outcome(supervisor.await),
            },
            outcome = &mut supervisor => map_supervisor_outcome(outcome),
        }
    }
}

#[derive(Clone, Debug)]
struct InstrumentSpec {
    symbol: SourceIdentifier,
    instrument: InstrumentId,
    terms: InstrumentExecutionTerms,
}

fn validate_instruments(
    config: &KrakenL3Config,
    instruments: Box<[MarketInstrumentBinding]>,
) -> Result<Arc<[InstrumentSpec]>, KrakenLevel3RuntimeError> {
    if instruments.len() != config.products().len() {
        return Err(KrakenLevel3RuntimeError::ActivationTopology);
    }
    let mut specs = Vec::new();
    specs
        .try_reserve_exact(instruments.len())
        .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
    for binding in instruments {
        let mapping = config
            .mapping(binding.provider_symbol())
            .filter(|mapping| mapping.instrument() == binding.instrument_id())
            .ok_or(KrakenLevel3RuntimeError::ActivationTopology)?;
        specs.push(InstrumentSpec {
            symbol: SourceIdentifier::try_from(mapping.symbol())?,
            instrument: binding.instrument_id(),
            terms: binding.execution_terms(),
        });
    }
    if config.products().iter().any(|mapping| {
        !specs.iter().any(|spec| {
            spec.symbol.as_str() == mapping.symbol() && spec.instrument == mapping.instrument()
        })
    }) {
        return Err(KrakenLevel3RuntimeError::ActivationTopology);
    }
    Ok(specs.into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the registry owner retains every exact authority through shutdown"
)]
async fn run_registry_owner(
    activation: KrakenL3AccountActivation,
    config: KrakenL3Config,
    specs: Arc<[InstrumentSpec]>,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    capture_process: CaptureProcessInfrastructure,
    directory: OrderLevelDirectory,
    keys: watch::Sender<Arc<[OrderLevelBookKey]>>,
    healthy: Arc<AtomicBool>,
    cancellation: CancellationToken,
    startup: oneshot::Sender<()>,
) -> Result<(), KrakenLevel3RuntimeError> {
    let paths = LocalPaths::prepare(app_config.data_dir())?;
    let authority_store = LocalAuthorityStateStore::try_open(
        paths
            .control_root()?
            .root()
            .join(SOURCE_AUTHORITY_ROOT)
            .join(activation.account_binding().subject().as_str())
            .join(config.metadata().source_id().as_str()),
    )?;
    let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(provider_rate.clone());
    let mut registry =
        AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            authority_store,
            resolver,
            provider_rate,
        )?;
    let registered =
        registry.register_or_resume_exact(config.metadata().clone(), system_timestamp()?)?;
    let backoff = registry.provider_backoff_authority(&registered)?;
    let run = run_generation_loop(
        &activation,
        &config,
        &specs,
        &app_config,
        capture_process,
        &directory,
        &keys,
        &healthy,
        &mut registry,
        &registered,
        &backoff,
        &cancellation,
        startup,
    )
    .await;
    drop(backoff);
    drop(registered);
    let shutdown = registry.shutdown();
    match (run, shutdown) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Err(primary), Err(shutdown)) => Err(KrakenLevel3RuntimeError::RunShutdown {
            primary: Box::new(primary),
            shutdown,
        }),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the generation loop retains immutable authorities without hiding globals"
)]
async fn run_generation_loop(
    activation: &KrakenL3AccountActivation,
    config: &KrakenL3Config,
    specs: &[InstrumentSpec],
    app_config: &AppConfig,
    capture_process: CaptureProcessInfrastructure,
    directory: &OrderLevelDirectory,
    keys: &watch::Sender<Arc<[OrderLevelBookKey]>>,
    healthy: &Arc<AtomicBool>,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &market_squawk_sources::RegisteredSource,
    backoff: &ProviderBackoffAuthority,
    cancellation: &CancellationToken,
    startup: oneshot::Sender<()>,
) -> Result<(), KrakenLevel3RuntimeError> {
    let mut startup = Some(startup);
    loop {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let result = run_generation(
            activation,
            config,
            specs,
            app_config,
            capture_process,
            directory,
            keys,
            healthy,
            registry,
            registered,
            cancellation.child_token(),
            &mut startup,
        )
        .await;
        healthy.store(false, Ordering::Release);
        keys.send_replace(Arc::from([]));
        match result {
            Ok(()) if cancellation.is_cancelled() => return Ok(()),
            Ok(()) => return Err(KrakenLevel3RuntimeError::SourceExited),
            Err(KrakenLevel3RuntimeError::Cancelled) if cancellation.is_cancelled() => {
                return Ok(());
            }
            Err(error) if startup.is_some() || !error.recoverable() => return Err(error),
            Err(error) => wait_after_failure(&error, backoff, cancellation).await?,
        }
    }
}

struct GenerationActors {
    keys: Vec<OrderLevelBookKey>,
    ingresses: Vec<OrderLevelIngress>,
    monitors: JoinSet<Result<OrderLevelTerminalFailure, OrderLevelMonitorError>>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "one generation owns capture, registry, token, socket, and actors atomically"
)]
async fn run_generation(
    activation: &KrakenL3AccountActivation,
    config: &KrakenL3Config,
    specs: &[InstrumentSpec],
    app_config: &AppConfig,
    capture_process: CaptureProcessInfrastructure,
    directory: &OrderLevelDirectory,
    published_keys: &watch::Sender<Arc<[OrderLevelBookKey]>>,
    healthy: &Arc<AtomicBool>,
    registry: &mut AuthoritativeSourceRegistry,
    registered: &market_squawk_sources::RegisteredSource,
    cancellation: CancellationToken,
    startup: &mut Option<oneshot::Sender<()>>,
) -> Result<(), KrakenLevel3RuntimeError> {
    let started_at = system_timestamp()?;
    let session = registry.begin_next_session(
        registered,
        SessionId::new(SourceIdentifier::try_from(format!(
            "kraken-l3-{}",
            uuid::Uuid::new_v4()
        ))?),
        started_at,
    )?;
    let mut actors: Option<GenerationActors> = None;
    let mut capture_control: Option<RawCaptureControl<CaptureGenerationCapabilities>> = None;
    let mut capture_writer = None;
    let run = async {
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (publisher, control, writer) = raw_capture_channel(
            &capture_process,
            CaptureChannelLimits::new(
                app_config.capture_queue_capacity(),
                app_config.capture_memory_ceiling_bytes(),
            ),
            capabilities,
        )?;
        let sink = RollingMemoryCaptureSink::try_new(
            app_config.capture_queue_capacity(),
            app_config.capture_memory_ceiling_bytes(),
        )?;
        let flush_records = NonZeroUsize::new(
            app_config
                .capture_queue_capacity()
                .get()
                .min(CAPTURE_FLUSH_RECORDS),
        )
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
        let policy =
            CaptureWriterPolicy::try_new(flush_records, app_config.capture_flush_interval())?;
        let writer = spawn_capture_writer(writer, sink, policy)?;
        capture_control = Some(control);
        capture_writer = Some(writer);
        capture_control
            .as_mut()
            .ok_or(KrakenLevel3RuntimeError::CaptureOwnerMissing)?
            .activate_initial()?;
        let mut source = registry
            .take_live_source_generation(&session)?
            .try_start(config.metadata())?;
        let generation = source.generation();
        actors = Some(
            register_generation_actors(directory, config, specs, generation, &cancellation).await?,
        );
        let token = activation
            .acquire_websocket_token(cancellation.clone())
            .await?;
        source.validate_current()?;
        let budget = source
            .budget()?
            .ok_or(KrakenLevel3RuntimeError::MissingProviderBudget)?;
        let socket_config = WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER_BYTES)
            .write_buffer_size(WRITE_BUFFER_BYTES)
            .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
            .max_message_size(Some(config.max_message_bytes()))
            .max_frame_size(Some(config.max_message_bytes()));
        let connect = connect_async_tls_with_config(
            KRAKEN_L3_WEBSOCKET_ENDPOINT,
            Some(socket_config),
            true,
            None,
        );
        let connection_permit = commit_connection_budget(budget, &cancellation).await?;
        let (mut socket, response) = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(KrakenLevel3RuntimeError::Cancelled),
            response = tokio::time::timeout(CONNECT_TIMEOUT, connect) => {
                response
                    .map_err(|_elapsed| KrakenLevel3RuntimeError::Transport)?
                    .map_err(|_error| KrakenLevel3RuntimeError::Transport)?
            }
        };
        if response.status().is_redirection() {
            return Err(KrakenLevel3RuntimeError::Transport);
        }
        budget
            .record_success()
            .map_err(KrakenLevel3RuntimeError::BudgetUnavailable)?;
        let profile = IntegrityProfile::try_from_config(config)?;
        let mut decoder = KrakenL3Decoder::try_new(config)?;
        let actors = actors
            .as_mut()
            .ok_or(KrakenLevel3RuntimeError::ActorTopology)?;
        let socket_result = run_socket(
            config,
            specs,
            &profile,
            &mut source,
            &publisher,
            &mut decoder,
            actors,
            directory,
            published_keys,
            healthy,
            &mut socket,
            token,
            startup,
            &cancellation,
        )
        .await;
        drop(connection_permit);
        socket_result
    }
    .await;

    healthy.store(false, Ordering::Release);
    published_keys.send_replace(Arc::from([]));
    let mut cleanup = None;
    if let Some(actors) = actors.as_mut() {
        if let Some(error) = run
            .as_ref()
            .err()
            .filter(|error| !matches!(error, KrakenLevel3RuntimeError::Cancelled))
        {
            quarantine_actors(
                &actors.ingresses,
                quarantine_reason(error),
                app_config.source_shutdown(),
            );
        }
        retain_cleanup(
            &mut cleanup,
            unregister_generation_actors(directory, actors, app_config.source_shutdown()).await,
        );
    }
    retain_cleanup(
        &mut cleanup,
        registry
            .end_session(&session, system_timestamp().unwrap_or(started_at))
            .map_err(Into::into),
    );
    if let Some(mut control) = capture_control {
        control.invalidate_current();
    }
    if let Some(writer) = capture_writer {
        retain_cleanup(
            &mut cleanup,
            shutdown_capture_writer(writer, app_config.capture_shutdown()).await,
        );
    }
    match (run, cleanup) {
        (Ok(()), None) => Ok(()),
        (Err(error), None) => Err(error),
        (Ok(()), Some(error)) => Err(error),
        (Err(primary), Some(cleanup)) => Err(KrakenLevel3RuntimeError::RunCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

#[derive(Clone, Debug)]
struct IntegrityProfile {
    decoder_rule: IntegrityRule,
    checksum_rule: IntegrityRule,
    checksum_scope: SourceIdentifier,
}

impl IntegrityProfile {
    fn try_from_config(config: &KrakenL3Config) -> Result<Self, KrakenLevel3RuntimeError> {
        let SourceProtocolProfile::Live(protocol) = config.metadata().protocol_profile() else {
            return Err(KrakenLevel3RuntimeError::ProtocolProfile);
        };
        if !matches!(
            protocol.sequence(),
            SequenceValidationProfile::Unsupported { .. }
        ) {
            return Err(KrakenLevel3RuntimeError::ProtocolProfile);
        }
        let ChecksumValidationProfile::Provided {
            rule,
            scope,
            book_scope: Some(book_scope),
            ..
        } = protocol.checksum()
        else {
            return Err(KrakenLevel3RuntimeError::ProtocolProfile);
        };
        if book_scope.depth() != MarketDepth::OrderLevel
            || book_scope.level_count().map(|value| value.get()) != Some(10)
            || scope.as_str() != KRAKEN_L3_CHECKSUM_SCOPE_ID
        {
            return Err(KrakenLevel3RuntimeError::ProtocolProfile);
        }
        Ok(Self {
            decoder_rule: protocol.decoder_rule().clone(),
            checksum_rule: rule.clone(),
            checksum_scope: scope.clone(),
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the established socket loop retains every generation-bound capability"
)]
async fn run_socket<S>(
    config: &KrakenL3Config,
    specs: &[InstrumentSpec],
    profile: &IntegrityProfile,
    source: &mut market_squawk_sources::ActiveLiveSourceGeneration,
    capture: &market_squawk_platform::RawCapturePublisher<CaptureGenerationCapabilities>,
    decoder: &mut KrakenL3Decoder,
    actors: &mut GenerationActors,
    directory: &OrderLevelDirectory,
    published_keys: &watch::Sender<Arc<[OrderLevelBookKey]>>,
    healthy: &Arc<AtomicBool>,
    socket: &mut WebSocketStream<S>,
    token: crate::provider_activation::KrakenL3WebSocketTokenMaterial,
    startup: &mut Option<oneshot::Sender<()>>,
    cancellation: &CancellationToken,
) -> Result<(), KrakenLevel3RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut acknowledged = vec![false; specs.len()];
    let mut market_at = vec![None; specs.len()];
    let mut batch_index = 0_usize;
    let mut next_subscription = tokio::time::Instant::now();
    let idle = Duration::from_nanos(
        config
            .metadata()
            .freshness_policy()
            .max_connection_idle_nanos(),
    );
    let market_age =
        Duration::from_nanos(config.metadata().freshness_policy().max_market_age_nanos());
    let mut last_transport = tokio::time::Instant::now();
    let mut currentness = tokio::time::interval(CURRENTNESS_INTERVAL);
    currentness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let idle_deadline = last_transport + idle;
        let stale_deadline = healthy
            .load(Ordering::Acquire)
            .then(|| market_deadline(&market_at, market_age))
            .flatten();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                close_socket(socket).await;
                return Err(KrakenLevel3RuntimeError::Cancelled);
            }
            monitor = actors.monitors.join_next() => {
                return match monitor {
                    Some(Ok(Ok(failure))) => Err(KrakenLevel3RuntimeError::ActorTerminal(failure)),
                    Some(Ok(Err(error))) => Err(KrakenLevel3RuntimeError::ActorMonitor(error)),
                    Some(Err(error)) => Err(KrakenLevel3RuntimeError::ActorTask(error)),
                    None => Err(KrakenLevel3RuntimeError::ActorTopology),
                };
            }
            _ = currentness.tick() => {
                source.validate_current()?;
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                return Err(KrakenLevel3RuntimeError::ConnectionIdle);
            }
            () = async {
                if let Some(deadline) = stale_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                return Err(KrakenLevel3RuntimeError::MarketStale);
            }
            () = tokio::time::sleep_until(next_subscription),
                if batch_index < config.subscription_batch_count() => {
                if system_timestamp()? >= token.expires_at() {
                    return Err(KrakenLevel3RuntimeError::TokenExpiredBeforeSubscription);
                }
                let request_id = u64::try_from(batch_index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
                let payload = config.try_subscription_payload(
                    token.token()?,
                    batch_index,
                    Some(request_id),
                )?;
                let text = std::str::from_utf8(payload.as_bytes())
                    .map_err(|_error| KrakenLevel3RuntimeError::Protocol)?;
                send_message(socket, Message::Text(text.into()), cancellation).await?;
                batch_index = batch_index
                    .checked_add(1)
                    .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
                next_subscription = tokio::time::Instant::now() + SUBSCRIPTION_RATE_WINDOW;
            }
            message = socket.next() => {
                let message = message
                    .ok_or(KrakenLevel3RuntimeError::Transport)?
                    .map_err(|_error| KrakenLevel3RuntimeError::Transport)?;
                last_transport = tokio::time::Instant::now();
                match message {
                    Message::Text(text) => {
                        let payload = Bytes::copy_from_slice(text.as_bytes());
                        let frame = source.frames_mut()?.try_frame(TransportFrameKind::Text, payload)?;
                        let receipt = capture.try_publish(&frame)?;
                        let validated = source.validate_live_frame(&frame)?;
                        let evidence = DecoderEvidence::from_validated_frame(
                            &validated,
                            profile.decoder_rule.clone(),
                        );
                        let outcome = decoder.decode_payload(frame.payload())?;
                        process_outcome(
                            config,
                            specs,
                            profile,
                            source.generation(),
                            decoder,
                            actors,
                            directory,
                            published_keys,
                            healthy,
                            &mut acknowledged,
                            &mut market_at,
                            outcome,
                            &evidence,
                            startup,
                            cancellation,
                        )
                        .await?;
                        drop(receipt);
                    }
                    Message::Binary(payload) => {
                        let frame = source.frames_mut()?.try_frame(
                            TransportFrameKind::Binary,
                            Bytes::copy_from_slice(payload.as_ref()),
                        )?;
                        let receipt = capture.try_publish(&frame)?;
                        source.validate_live_frame(&frame)?;
                        drop(receipt);
                        return Err(KrakenLevel3RuntimeError::Protocol);
                    }
                    Message::Ping(payload) => {
                        send_message(socket, Message::Pong(payload), cancellation).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(KrakenLevel3RuntimeError::Transport),
                    Message::Frame(_) => return Err(KrakenLevel3RuntimeError::Protocol),
                }
            }
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "capture evidence and every readiness owner must cross one atomic output boundary"
)]
async fn process_outcome(
    config: &KrakenL3Config,
    specs: &[InstrumentSpec],
    profile: &IntegrityProfile,
    generation: market_squawk_domain::ConnectionGeneration,
    decoder: &KrakenL3Decoder,
    actors: &GenerationActors,
    directory: &OrderLevelDirectory,
    published_keys: &watch::Sender<Arc<[OrderLevelBookKey]>>,
    healthy: &Arc<AtomicBool>,
    acknowledged: &mut [bool],
    market_at: &mut [Option<tokio::time::Instant>],
    outcome: KrakenL3DecodeOutcome,
    evidence: &DecoderEvidence,
    startup: &mut Option<oneshot::Sender<()>>,
    cancellation: &CancellationToken,
) -> Result<(), KrakenLevel3RuntimeError> {
    match outcome {
        KrakenL3DecodeOutcome::Control(KrakenL3Control::Subscribed { symbol, instrument }) => {
            let index = spec_index(specs, symbol.as_str(), instrument)?;
            if acknowledged[index] {
                return Err(KrakenLevel3RuntimeError::Protocol);
            }
            acknowledged[index] = true;
        }
        KrakenL3DecodeOutcome::Control(
            KrakenL3Control::Heartbeat | KrakenL3Control::Pong | KrakenL3Control::Online,
        ) => {}
        KrakenL3DecodeOutcome::Book(batch) => {
            let index = spec_index(specs, batch.symbol().as_str(), batch.instrument())?;
            if !acknowledged[index] || batch.quality_ceiling() != DataQuality::DirectUnverified {
                return Err(KrakenLevel3RuntimeError::Protocol);
            }
            validate_market_time(config, batch.timestamp(), evidence.received_at())?;
            let canonical = build_order_level_batch(
                &batch,
                specs[index].terms,
                &actors.ingresses[index],
                profile,
                generation,
                evidence,
            )?;
            let deadline = Instant::now()
                .checked_add(IO_TIMEOUT)
                .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
            actors.ingresses[index].try_publish(canonical, deadline)?;
            market_at[index] = Some(tokio::time::Instant::now());
        }
    }
    if !healthy.load(Ordering::Acquire)
        && acknowledged.iter().all(|value| *value)
        && specs.iter().all(|spec| {
            decoder.state(spec.symbol.as_str())
                == Some(market_squawk_adapter_kraken::KrakenL3DecoderState::Healthy)
        })
    {
        activation_readiness(directory, &actors.keys, cancellation).await?;
        published_keys.send_replace(clone_keys(&actors.keys)?);
        healthy.store(true, Ordering::Release);
        if let Some(sender) = startup.take() {
            sender
                .send(())
                .map_err(|()| KrakenLevel3RuntimeError::StartupObserverDropped)?;
        }
    }
    Ok(())
}

fn build_order_level_batch(
    batch: &KrakenL3BookBatch,
    terms: InstrumentExecutionTerms,
    ingress: &OrderLevelIngress,
    profile: &IntegrityProfile,
    generation: market_squawk_domain::ConnectionGeneration,
    evidence: &DecoderEvidence,
) -> Result<OrderLevelBatch, KrakenLevel3RuntimeError> {
    if batch.market_depth() != MarketDepth::OrderLevel || ingress.key().generation() != generation {
        return Err(KrakenLevel3RuntimeError::ActorTopology);
    }
    let scaled = batch.try_scaled_events(terms)?;
    batch.validate_price_projection(terms)?;
    let payload = match batch.kind() {
        KrakenL3BatchKind::Snapshot => {
            let mut orders = Vec::new();
            orders
                .try_reserve_exact(scaled.len())
                .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
            for event in scaled {
                if event.kind() != KrakenL3OrderEventKind::Snapshot {
                    return Err(KrakenLevel3RuntimeError::Protocol);
                }
                orders.push(visible_order(event.order())?);
            }
            OrderLevelBatchPayload::Snapshot {
                snapshot_source_timestamp: batch.timestamp(),
                snapshot_received_at: evidence.received_at(),
                orders,
                replay: Vec::new(),
            }
        }
        KrakenL3BatchKind::Update => {
            let mut operations = Vec::new();
            operations
                .try_reserve_exact(scaled.len())
                .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
            for event in scaled {
                let order = event.order();
                let operation = match event.kind() {
                    KrakenL3OrderEventKind::Snapshot => {
                        return Err(KrakenLevel3RuntimeError::Protocol);
                    }
                    KrakenL3OrderEventKind::Add => OrderLevelOperation::Open(visible_order(order)?),
                    KrakenL3OrderEventKind::Modify => OrderLevelOperation::Change {
                        order_id: SourceIdentifier::try_from(order.order_id().as_str())?,
                        reason: ProviderOrderChangeReason::ModifyOrder,
                        side: book_side(order.side()),
                        previous_price: None,
                        previous_quantity: None,
                        new_price: Some(order.price()),
                        new_quantity: Some(order.quantity()),
                        provider_order_timestamp: Some(order.provider_order_timestamp()),
                        priority: OrderLevelPriorityUpdate::Preserve,
                        unknown_order: UnknownOrderDisposition::Reject,
                    },
                    KrakenL3OrderEventKind::Delete => OrderLevelOperation::Done {
                        order_id: SourceIdentifier::try_from(order.order_id().as_str())?,
                        side: Some(book_side(order.side())),
                        price: Some(order.price()),
                        quantity: market_squawk_live::OrderLevelDeleteQuantity::ZeroMeansDelete,
                        provider_order_timestamp: Some(order.provider_order_timestamp()),
                        unknown_order: UnknownOrderDisposition::Reject,
                    },
                };
                operations.push(operation);
            }
            let event = OrderLevelEvent::try_new(
                None,
                Some(batch.local_generation_ordinal()),
                batch.timestamp(),
                evidence.received_at(),
                operations,
            )?;
            OrderLevelBatchPayload::Update {
                events: vec![event],
            }
        }
    };
    let checksum_scope = ChecksumScope::new(
        MarketDepth::OrderLevel,
        10,
        SourceIdentifier::try_from(profile.checksum_scope.as_str())?,
    )?;
    let value = ChecksumValue::new(u64::from(batch.checksum()));
    let checksum = ChecksumEvidence::validate_book(
        ChecksumCapability::Provided,
        Some(profile.checksum_rule.clone()),
        generation,
        Some(checksum_scope),
        Some(value),
        Some(value),
    )?;
    let route = OrderLevelRoute::new(
        ingress.key().source_id().clone(),
        ingress.key().venue_id().clone(),
        batch.instrument(),
        SourceIdentifier::try_from(batch.symbol().as_str())?,
        generation,
    );
    let identifier = SourceIdentifier::try_from(format!(
        "kraken-l3-g{}-f{}-{}",
        generation.get(),
        evidence.frame_id().get(),
        batch.instrument()
    ))?;
    let available_at = system_timestamp()?;
    OrderLevelBatch::try_new(OrderLevelBatchInput::new(
        route,
        identifier,
        batch.timestamp(),
        evidence.received_at(),
        available_at,
        DataQuality::DirectUnverified,
        market_squawk_sources::MarketFreshness::Fresh {
            last_market_at: evidence.received_at(),
        },
        None,
        SequenceEvidence::unsupported(generation),
        checksum,
        Some(batch.local_generation_ordinal()),
        payload,
    ))
    .map_err(Into::into)
}

fn visible_order(
    order: &market_squawk_adapter_kraken::KrakenL3ScaledOrder,
) -> Result<OrderLevelVisibleOrder, KrakenLevel3RuntimeError> {
    Ok(OrderLevelVisibleOrder::new(
        SourceIdentifier::try_from(order.order_id().as_str())?,
        book_side(order.side()),
        order.price(),
        order.quantity(),
        Some(order.provider_order_timestamp()),
        None,
    )?)
}

const fn book_side(side: market_squawk_sources::ProviderBookSide) -> BookSide {
    match side {
        market_squawk_sources::ProviderBookSide::Bid => BookSide::Bid,
        market_squawk_sources::ProviderBookSide::Ask => BookSide::Ask,
    }
}

async fn register_generation_actors(
    directory: &OrderLevelDirectory,
    config: &KrakenL3Config,
    specs: &[InstrumentSpec],
    generation: market_squawk_domain::ConnectionGeneration,
    cancellation: &CancellationToken,
) -> Result<GenerationActors, KrakenLevel3RuntimeError> {
    let mut registrations: Vec<OrderLevelRegistration> = Vec::new();
    registrations
        .try_reserve_exact(specs.len())
        .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
    let mut keys = Vec::new();
    let mut ingresses = Vec::new();
    keys.try_reserve_exact(specs.len())
        .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
    ingresses
        .try_reserve_exact(specs.len())
        .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
    let venue = market_squawk_domain::VenueId::try_from("kraken")?;
    let actor_limits = actor_limits(config)?;
    let book_limits = OrderLevelLimits::new(
        market_squawk_sources::MAX_DECODED_BOOK_ITEMS,
        DepthLimit::new(config.retained_price_levels().get())?,
    )?;
    let registration_deadline = Instant::now()
        .checked_add(CONNECT_TIMEOUT)
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
    for spec in specs {
        let route = OrderLevelRoute::new(
            config.metadata().source_id().clone(),
            venue.clone(),
            spec.instrument,
            spec.symbol.clone(),
            generation,
        );
        let registration = match directory
            .register(
                route,
                book_limits,
                actor_limits,
                cancellation,
                registration_deadline,
            )
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                let primary = KrakenLevel3RuntimeError::Directory(error);
                return match cleanup_registrations(directory, registrations).await {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(KrakenLevel3RuntimeError::RunCleanup {
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup),
                    }),
                };
            }
        };
        registrations.push(registration);
    }
    let mut monitors = JoinSet::new();
    for registration in registrations {
        keys.push(registration.key().clone());
        let (ingress, mut monitor) = registration.into_parts();
        let monitor_cancellation = cancellation.clone();
        monitors.spawn(async move { monitor.wait_until_terminal(&monitor_cancellation).await });
        ingresses.push(ingress);
    }
    Ok(GenerationActors {
        keys,
        ingresses,
        monitors,
    })
}

async fn cleanup_registrations(
    directory: &OrderLevelDirectory,
    registrations: Vec<OrderLevelRegistration>,
) -> Result<(), KrakenLevel3RuntimeError> {
    let now = Instant::now();
    let (deadline, mut first_error) = now.checked_add(IO_TIMEOUT).map_or_else(
        || (now, Some(KrakenLevel3RuntimeError::ResourceAccounting)),
        |deadline| (deadline, None),
    );
    let cleanup = CancellationToken::new();
    for registration in registrations {
        let key = registration.key().clone();
        drop(registration);
        let error = match directory.unregister(&key, &cleanup, deadline).await {
            Ok(OrderLevelActorShutdown::Graceful) => None,
            Ok(_) => Some(KrakenLevel3RuntimeError::ActorShutdownIncomplete),
            Err(error) => Some(KrakenLevel3RuntimeError::from(error)),
        };
        if first_error.is_none() {
            first_error = error;
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn actor_limits(
    config: &KrakenL3Config,
) -> Result<OrderLevelActorLimits, KrakenLevel3RuntimeError> {
    let maximum_bytes = config
        .max_message_bytes()
        .checked_mul(4)
        .map(|value| value.max(ACTOR_MINIMUM_BYTES))
        .and_then(|value| u32::try_from(value).ok())
        .and_then(NonZeroU32::new)
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
    let order_units = market_squawk_sources::MAX_DECODED_BOOK_ITEMS
        .checked_mul(2)
        .and_then(|value| u32::try_from(value).ok())
        .and_then(NonZeroU32::new)
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
    let read_units = config
        .retained_price_levels()
        .get()
        .checked_mul(2)
        .and_then(|value| u32::try_from(value).ok())
        .and_then(NonZeroU32::new)
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
    OrderLevelActorLimits::try_new(
        NonZeroUsize::new(ACTOR_INGRESS_COMMANDS.min(MAX_ORDER_LEVEL_INGRESS_COMMANDS))
            .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?,
        maximum_bytes,
        order_units,
        NonZeroUsize::new(ACTOR_OUTSTANDING_READS)
            .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?,
        maximum_bytes,
        read_units,
    )
    .map_err(Into::into)
}

async fn activation_readiness(
    directory: &OrderLevelDirectory,
    keys: &[OrderLevelBookKey],
    cancellation: &CancellationToken,
) -> Result<(), KrakenLevel3RuntimeError> {
    let deadline = Instant::now()
        .checked_add(IO_TIMEOUT)
        .ok_or(KrakenLevel3RuntimeError::ResourceAccounting)?;
    for key in keys {
        let read = directory
            .read_price_projection(key, cancellation, deadline)
            .await?;
        if read.projection().quality() != DataQuality::DirectUnverified
            || read.projection().route().generation() != key.generation()
        {
            return Err(KrakenLevel3RuntimeError::ActorTopology);
        }
    }
    Ok(())
}

async fn unregister_generation_actors(
    directory: &OrderLevelDirectory,
    actors: &mut GenerationActors,
    timeout: Duration,
) -> Result<(), KrakenLevel3RuntimeError> {
    let now = Instant::now();
    let (deadline, mut first_error) = now.checked_add(timeout).map_or_else(
        || (now, Some(KrakenLevel3RuntimeError::ResourceAccounting)),
        |deadline| (deadline, None),
    );
    let cleanup = CancellationToken::new();
    for key in &actors.keys {
        let error = match directory.unregister(key, &cleanup, deadline).await {
            Ok(OrderLevelActorShutdown::Graceful) => None,
            Ok(_) => Some(KrakenLevel3RuntimeError::ActorShutdownIncomplete),
            Err(error) => Some(KrakenLevel3RuntimeError::from(error)),
        };
        if first_error.is_none() {
            first_error = error;
        }
    }
    actors.monitors.abort_all();
    while actors.monitors.join_next().await.is_some() {}
    first_error.map_or(Ok(()), Err)
}

fn quarantine_actors(
    ingresses: &[OrderLevelIngress],
    reason: OrderLevelQuarantineReason,
    timeout: Duration,
) {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return;
    };
    for ingress in ingresses {
        let _result = ingress.request_quarantine(reason, deadline);
    }
}

fn validate_market_time(
    config: &KrakenL3Config,
    source_at: Timestamp,
    received_at: Timestamp,
) -> Result<(), KrakenLevel3RuntimeError> {
    let policy = config.metadata().freshness_policy();
    let difference = received_at
        .unix_nanos()
        .checked_sub(source_at.unix_nanos())
        .ok_or(KrakenLevel3RuntimeError::MarketTimestamp)?;
    if difference >= 0 {
        if u64::try_from(difference)
            .ok()
            .is_none_or(|age| age > policy.max_source_age_nanos())
        {
            return Err(KrakenLevel3RuntimeError::MarketTimestamp);
        }
    } else if difference
        .checked_neg()
        .and_then(|skew| u64::try_from(skew).ok())
        .is_none_or(|skew| skew > policy.max_clock_skew_nanos())
    {
        return Err(KrakenLevel3RuntimeError::MarketTimestamp);
    }
    Ok(())
}

fn spec_index(
    specs: &[InstrumentSpec],
    symbol: &str,
    instrument: InstrumentId,
) -> Result<usize, KrakenLevel3RuntimeError> {
    specs
        .iter()
        .position(|spec| spec.symbol.as_str() == symbol && spec.instrument == instrument)
        .ok_or(KrakenLevel3RuntimeError::ActorTopology)
}

fn clone_keys(
    keys: &[OrderLevelBookKey],
) -> Result<Arc<[OrderLevelBookKey]>, KrakenLevel3RuntimeError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(keys.len())
        .map_err(|_| KrakenLevel3RuntimeError::Allocation)?;
    cloned.extend(keys.iter().cloned());
    Ok(cloned.into())
}

fn market_deadline(
    market_at: &[Option<tokio::time::Instant>],
    maximum_age: Duration,
) -> Option<tokio::time::Instant> {
    market_at
        .iter()
        .flatten()
        .filter_map(|received| received.checked_add(maximum_age))
        .min()
}

async fn send_message<S>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    cancellation: &CancellationToken,
) -> Result<(), KrakenLevel3RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(KrakenLevel3RuntimeError::Cancelled),
        result = tokio::time::timeout(IO_TIMEOUT, socket.send(message)) => {
            result
                .map_err(|_elapsed| KrakenLevel3RuntimeError::Transport)?
                .map_err(|_error| KrakenLevel3RuntimeError::Transport)
        }
    }
}

async fn close_socket<S>(socket: &mut WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let _result = tokio::time::timeout(IO_TIMEOUT, socket.close(None)).await;
}

async fn commit_connection_budget(
    budget: &market_squawk_sources::SharedProviderBudget,
    cancellation: &CancellationToken,
) -> Result<market_squawk_sources::BudgetPermit, KrakenLevel3RuntimeError> {
    loop {
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(deadline) => {
                let wait = budget
                    .remaining_wait(deadline)
                    .map_err(KrakenLevel3RuntimeError::BudgetUnavailable)?;
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(KrakenLevel3RuntimeError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(KrakenLevel3RuntimeError::BudgetUnavailable(reason));
            }
        };
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(deadline) => {
                let wait = budget
                    .remaining_wait(deadline)
                    .map_err(KrakenLevel3RuntimeError::BudgetUnavailable)?;
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(KrakenLevel3RuntimeError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(KrakenLevel3RuntimeError::BudgetUnavailable(reason));
            }
        }
    }
}

async fn wait_after_failure(
    error: &KrakenLevel3RuntimeError,
    backoff: &ProviderBackoffAuthority,
    cancellation: &CancellationToken,
) -> Result<(), KrakenLevel3RuntimeError> {
    let wait = if error.provider_failure() {
        match backoff.apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS)? {
            ProviderBackoffDecision::WaitUntil(deadline) => backoff.remaining_wait(deadline)?,
            ProviderBackoffDecision::Unavailable(reason) => {
                return Err(KrakenLevel3RuntimeError::BudgetUnavailable(reason));
            }
        }
    } else {
        LOCAL_RECONNECT_DELAY
    };
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        () = tokio::time::sleep(wait) => Ok(()),
    }
}

async fn shutdown_capture_writer(
    writer: market_squawk_platform::CaptureWriterHandle<CaptureGenerationCapabilities>,
    timeout: Duration,
) -> Result<(), KrakenLevel3RuntimeError> {
    let mut pending = writer.shutdown(timeout);
    let status = pending.wait_until_deadline().await;
    if status == CaptureShutdownStatus::DeadlineElapsed {
        pending.wait_until_terminated().await;
    }
    let termination = pending
        .try_reap()?
        .ok_or(KrakenLevel3RuntimeError::CaptureOwnerMissing)?;
    if status == CaptureShutdownStatus::DeadlineElapsed
        || termination.shutdown_deadline_elapsed()
        || termination.outcome().is_incomplete()
    {
        return Err(KrakenLevel3RuntimeError::CaptureShutdownIncomplete);
    }
    Ok(())
}

fn quarantine_reason(error: &KrakenLevel3RuntimeError) -> OrderLevelQuarantineReason {
    match error {
        KrakenLevel3RuntimeError::Decode(KrakenL3DecodeError::ChecksumMismatch { .. }) => {
            OrderLevelQuarantineReason::Checksum
        }
        KrakenLevel3RuntimeError::Decode(
            KrakenL3DecodeError::Allocation | KrakenL3DecodeError::ProjectionOverflow,
        )
        | KrakenLevel3RuntimeError::CapturePublish(_)
        | KrakenLevel3RuntimeError::Ingress(_)
        | KrakenLevel3RuntimeError::ActorTerminal(_)
        | KrakenLevel3RuntimeError::ActorTask(_)
        | KrakenLevel3RuntimeError::ActorMonitor(_) => OrderLevelQuarantineReason::Resource,
        KrakenLevel3RuntimeError::Decode(
            KrakenL3DecodeError::DuplicateOrder
            | KrakenL3DecodeError::UnknownOrder
            | KrakenL3DecodeError::InvalidOrderTransition,
        ) => OrderLevelQuarantineReason::Mutation,
        KrakenLevel3RuntimeError::Decode(
            KrakenL3DecodeError::InvalidBook | KrakenL3DecodeError::CrossedBook,
        ) => OrderLevelQuarantineReason::Book,
        _ => OrderLevelQuarantineReason::Snapshot,
    }
}

fn retain_cleanup(
    retained: &mut Option<KrakenLevel3RuntimeError>,
    candidate: Result<(), KrakenLevel3RuntimeError>,
) {
    if retained.is_none() {
        *retained = candidate.err();
    }
}

fn map_supervisor_outcome(
    outcome: Result<Result<(), KrakenLevel3RuntimeError>, JoinError>,
) -> Result<KrakenLevel3LiveRuntime, KrakenLevel3RuntimeError> {
    match outcome {
        Ok(Ok(())) => Err(KrakenLevel3RuntimeError::SupervisorExitedBeforeStartup),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(KrakenLevel3RuntimeError::SupervisorTask(error)),
    }
}

fn system_timestamp() -> Result<Timestamp, KrakenLevel3RuntimeError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| KrakenLevel3RuntimeError::ClockRange)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| KrakenLevel3RuntimeError::ClockRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Authenticated Kraken L3 construction, transport, integrity, or cleanup failure.
#[derive(Debug, Error)]
pub(crate) enum KrakenLevel3RuntimeError {
    #[error("Kraken level-3 activation topology is inconsistent")]
    ActivationTopology,
    #[error("Kraken level-3 runtime allocation failed")]
    Allocation,
    #[error("Kraken level-3 bounded resource accounting failed")]
    ResourceAccounting,
    #[error("Kraken level-3 protocol metadata is inconsistent")]
    ProtocolProfile,
    #[error("Kraken level-3 actor topology is inconsistent")]
    ActorTopology,
    #[error("Kraken level-3 source exited unexpectedly")]
    SourceExited,
    #[error("Kraken level-3 runtime was cancelled")]
    Cancelled,
    #[error("Kraken level-3 WebSocket transport failed")]
    Transport,
    #[error("Kraken level-3 protocol state is invalid")]
    Protocol,
    #[error("Kraken level-3 connection exceeded its idle deadline")]
    ConnectionIdle,
    #[error("Kraken level-3 market state exceeded its freshness deadline")]
    MarketStale,
    #[error("Kraken level-3 market timestamp is invalid")]
    MarketTimestamp,
    #[error("Kraken level-3 system clock is outside the supported timestamp range")]
    ClockRange,
    #[error("Kraken level-3 token expired before all bounded subscriptions were sent")]
    TokenExpiredBeforeSubscription,
    #[error("Kraken level-3 registry source has no provider budget")]
    MissingProviderBudget,
    #[error("Kraken level-3 startup observer was dropped")]
    StartupObserverDropped,
    #[error("Kraken level-3 supervisor exited before startup")]
    SupervisorExitedBeforeStartup,
    #[error("Kraken level-3 supervisor shutdown deadline elapsed")]
    ShutdownDeadline,
    #[error("Kraken level-3 published supervisor failed terminally during shutdown")]
    PublishedShutdownFailed,
    #[error("Kraken level-3 capture owner is missing")]
    CaptureOwnerMissing,
    #[error("Kraken level-3 capture shutdown was incomplete")]
    CaptureShutdownIncomplete,
    #[error("Kraken level-3 actor shutdown was incomplete")]
    ActorShutdownIncomplete,
    #[error("Kraken level-3 actor failed terminally: {0}")]
    ActorTerminal(OrderLevelTerminalFailure),
    #[error("Kraken level-3 actor monitor failed: {0}")]
    ActorMonitor(OrderLevelMonitorError),
    #[error("Kraken level-3 actor task failed")]
    ActorTask(JoinError),
    #[error("Kraken level-3 supervisor task failed")]
    SupervisorTask(JoinError),
    #[error("Kraken level-3 provider budget is unavailable: {0:?}")]
    BudgetUnavailable(BudgetUnavailableReason),
    #[error("Kraken level-3 generation and cleanup both failed")]
    RunCleanup {
        primary: Box<Self>,
        cleanup: Box<Self>,
    },
    #[error("Kraken level-3 runtime and registry shutdown both failed")]
    RunShutdown {
        primary: Box<Self>,
        shutdown: RegistryError,
    },
    #[error(transparent)]
    Activation(#[from] KrakenL3ActivationError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Integrity(#[from] IntegrityEvidenceError),
    #[error(transparent)]
    Decode(#[from] KrakenL3DecodeError),
    #[error(transparent)]
    Scale(#[from] KrakenL3ScaleError),
    #[error(transparent)]
    OrderModel(#[from] market_squawk_live::OrderLevelModelError),
    #[error(transparent)]
    OrderBatch(#[from] market_squawk_live::OrderLevelBatchError),
    #[error(transparent)]
    OrderLimit(#[from] OrderLevelLimitError),
    #[error(transparent)]
    Book(#[from] BookError),
    #[error("Kraken level-3 actor configuration failed: {0}")]
    ActorConfiguration(#[from] super::order_level::OrderLevelConfigurationError),
    #[error(transparent)]
    Directory(#[from] OrderLevelDirectoryError),
    #[error(transparent)]
    Read(#[from] OrderLevelReadError),
    #[error("Kraken level-3 actor ingress failed: {0}")]
    Ingress(#[from] super::order_level::OrderLevelIngressError),
    #[error(transparent)]
    Paths(#[from] market_squawk_platform::PathError),
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    CaptureChannel(#[from] CaptureChannelError),
    #[error(transparent)]
    CaptureGeneration(#[from] CaptureGenerationError),
    #[error(transparent)]
    CapturePublish(#[from] CapturePublishError),
    #[error(transparent)]
    CaptureStorage(#[from] MemoryCaptureSinkConstructionError),
    #[error(transparent)]
    CapturePolicy(#[from] CaptureWriterPolicyError),
    #[error(transparent)]
    CaptureWriter(#[from] CaptureWriterSpawnError),
    #[error(transparent)]
    CaptureReap(#[from] CaptureWorkerReapError),
    #[error(transparent)]
    Backoff(#[from] ProviderBackoffError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    KrakenConfig(#[from] market_squawk_adapter_kraken::KrakenL3ConfigError),
}

impl KrakenLevel3RuntimeError {
    fn recoverable(&self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::Protocol
                | Self::ConnectionIdle
                | Self::MarketStale
                | Self::MarketTimestamp
                | Self::TokenExpiredBeforeSubscription
                | Self::Decode(_)
                | Self::Scale(_)
                | Self::OrderBatch(_)
                | Self::ActorTerminal(_)
                | Self::ActorMonitor(_)
                | Self::ActorTask(_)
                | Self::Ingress(_)
                | Self::CapturePublish(_)
                | Self::Activation(
                    KrakenL3ActivationError::Network
                        | KrakenL3ActivationError::Deadline
                        | KrakenL3ActivationError::RateLimited
                        | KrakenL3ActivationError::Response
                )
        )
    }

    fn provider_failure(&self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::ConnectionIdle
                | Self::Activation(
                    KrakenL3ActivationError::Network
                        | KrakenL3ActivationError::Deadline
                        | KrakenL3ActivationError::RateLimited
                        | KrakenL3ActivationError::Response
                )
        )
    }
}
