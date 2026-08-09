//! Bounded, on-demand Tradier REST ownership for one authenticated account.
//!
//! This runtime owns the mutable REST clients and their exact registry generations. It does not
//! poll, submit orders, promote provider quality, or duplicate the account or provider-rate
//! authority retained by application composition.

use std::{
    future::Future,
    mem::{size_of, size_of_val},
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_tradier::{
    TradierAccessSurface, TradierDerivedIndexBatch, TradierLogicalProfile, TradierQuoteBatch,
    TradierQuoteRequest, TradierRestError, TradierRestEvidence, TradierSnapshotClient,
    TradierSourceConfig,
};
use market_squawk_domain::{
    ConnectionGeneration, DataQuality, IdentityError, RawCaptureFrameView, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    AppConfig, CaptureChannelError, CaptureChannelLimits, CaptureGenerationError,
    CaptureProcessInfrastructureLimits, CapturePublishError, CaptureWriterPolicy,
    CaptureWriterPolicyError, DestinationFenceRegistryInitializationError,
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths,
    ProcessCaptureShutdownDisposition, ProcessCaptureShutdownOutcome, ProcessCaptureShutdownPolicy,
    ProcessCaptureShutdownPolicyError, ProcessCaptureWriterSpawnError, ProcessJournalCaptureConfig,
    ProcessJournalCaptureConfigError, ProcessJournalCaptureWriter, RawCaptureControl,
    RawCapturePublisher, initialize_capture_process_infrastructure, raw_capture_channel,
    spawn_process_journal_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, CaptureGenerationCapabilities,
    CurrentSourceSession, LiveSourceGeneration, ProviderRateAuthority, RegisteredSource,
    RegistryError, SessionId, SourceError, SourceMetadataProvider,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ProviderOnboardingError,
    provider_activation::{TradierMarketDataAccountActivation, TradierMarketDataActivationError},
};

const SOURCE_AUTHORITY_ROOT: &str = "tradier-rest-account-authority";
const CAPTURE_HELPER_STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const CAPTURE_FLUSH_RECORDS: usize = 256;
const MAX_COMMAND_CAPACITY: usize = 1_024;
const MAX_OUTSTANDING_RESPONSES: usize = 128;
const RESPONSE_ACCOUNTING_FIXED_SLACK: usize = 512;
const ARC_CONTROL_BLOCK_SLACK: usize = 64;

/// Explicit queue and retained-response limits for one account REST owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TradierRestRuntimeLimits {
    command_capacity: NonZeroUsize,
    outstanding_responses: NonZeroUsize,
    retained_response_bytes: NonZeroU32,
    maximum_response_bytes: NonZeroU32,
}

impl TradierRestRuntimeLimits {
    /// Constructs a fully bounded command and response-retention policy.
    pub(crate) fn try_new(
        command_capacity: NonZeroUsize,
        outstanding_responses: NonZeroUsize,
        retained_response_bytes: NonZeroU32,
        maximum_response_bytes: NonZeroU32,
    ) -> Result<Self, TradierRestRuntimeError> {
        if command_capacity.get() > MAX_COMMAND_CAPACITY
            || outstanding_responses.get() > MAX_OUTSTANDING_RESPONSES
            || maximum_response_bytes.get() > retained_response_bytes.get()
        {
            return Err(TradierRestRuntimeError::InvalidLimits);
        }
        Ok(Self {
            command_capacity,
            outstanding_responses,
            retained_response_bytes,
            maximum_response_bytes,
        })
    }

    pub(crate) const fn command_capacity(self) -> NonZeroUsize {
        self.command_capacity
    }

    pub(crate) const fn outstanding_responses(self) -> NonZeroUsize {
        self.outstanding_responses
    }

    pub(crate) const fn retained_response_bytes(self) -> NonZeroU32 {
        self.retained_response_bytes
    }

    pub(crate) const fn maximum_response_bytes(self) -> NonZeroU32 {
        self.maximum_response_bytes
    }
}

/// Terminal state that requires a fresh account REST runtime and source generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum TradierRestTerminalFailure {
    #[error("Tradier REST source generation failed terminally: {0}")]
    Source(SourceError),
    #[error("Tradier REST response identity did not match the active registry generation")]
    EvidenceMismatch,
    #[error("Tradier REST raw capture failed terminally: {0}")]
    Capture(CapturePublishError),
    #[error("Tradier REST normalized response exceeded its admitted retained-byte envelope")]
    ResponseAccounting,
    #[error("Tradier REST actor stopped unexpectedly")]
    WorkerStopped,
}

/// One running, account-scoped Tradier REST owner.
#[derive(Debug)]
pub(crate) struct TradierRestRuntime {
    client: TradierRestClient,
    consolidated_generation: ConnectionGeneration,
    derived_index_generation: Option<ConnectionGeneration>,
    cancellation: CancellationToken,
    worker: JoinHandle<Result<TradierRestShutdown, TradierRestRuntimeError>>,
    shutdown_deadline: Duration,
}

impl TradierRestRuntime {
    /// Starts one serialized REST owner from the account's one-shot clients.
    #[allow(
        clippy::too_many_arguments,
        reason = "account, source, rate, storage, response, and lifecycle authorities stay explicit"
    )]
    pub(crate) async fn start(
        activation: &mut TradierMarketDataAccountActivation,
        consolidated_config: TradierSourceConfig,
        derived_index_config: Option<TradierSourceConfig>,
        app_config: AppConfig,
        provider_rate: ProviderRateAuthority,
        limits: TradierRestRuntimeLimits,
        cancellation: CancellationToken,
    ) -> Result<Self, TradierRestRuntimeError> {
        let cancellation = cancellation.child_token();
        validate_configs(&consolidated_config, derived_index_config.as_ref(), limits)?;
        let quote_symbols = configured_quote_symbols(&consolidated_config)?;
        let quote_without_greeks = TradierQuoteRequest::try_new(quote_symbols.clone(), false)?;
        let quote_with_greeks = TradierQuoteRequest::try_new(quote_symbols, true)?;
        let response_byte_capacity = usize::try_from(limits.retained_response_bytes().get())
            .map_err(|_error| TradierRestRuntimeError::InvalidLimits)?;
        let shutdown_deadline = total_shutdown_deadline(&app_config)?;
        let registered_at = system_timestamp()?;
        require_runtime_open(&cancellation)?;
        activation.require_current().await?;

        let paths = LocalPaths::prepare(app_config.data_dir())?;
        let authority_store = LocalAuthorityStateStore::try_open(
            paths
                .control_root()?
                .root()
                .join(SOURCE_AUTHORITY_ROOT)
                .join(activation.account_binding().subject().as_str()),
        )?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                app_config.capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(provider_rate.clone());
        let mut registry =
            AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
                authority_store,
                resolver,
                provider_rate,
            )?;

        let consolidated_registration = match registry
            .register_or_resume_exact(consolidated_config.metadata().clone(), registered_at)
        {
            Ok(registered) => registered,
            Err(error) => {
                return Err(shutdown_registry_after_startup_error(
                    registry,
                    error.into(),
                ));
            }
        };
        let derived_registration = match derived_index_config.as_ref() {
            Some(config) => {
                match registry.register_or_resume_exact(config.metadata().clone(), registered_at) {
                    Ok(registered) => Some(registered),
                    Err(error) => {
                        return Err(shutdown_registry_after_startup_error(
                            registry,
                            error.into(),
                        ));
                    }
                }
            }
            None => None,
        };
        if !consolidated_registration.has_provider_budget()
            || derived_registration.as_ref().is_some_and(|derived| {
                consolidated_registration.shares_provider_budget_with(derived) != Some(true)
            })
        {
            return Err(shutdown_registry_after_startup_error(
                registry,
                TradierRestRuntimeError::BudgetTopology,
            ));
        }

        let mut consolidated_authority = None;
        let mut derived_authority = None;
        let mut consolidated_client = None;
        let mut derived_client = None;
        let setup = async {
            consolidated_authority = Some(
                prepare_surface_authority(
                    &mut registry,
                    &consolidated_registration,
                    &consolidated_config,
                    &app_config,
                    &paths,
                    capture_process,
                )
                .await?,
            );
            let generation = consolidated_authority
                .as_mut()
                .ok_or(TradierRestRuntimeError::StartupInvariant)?
                .take_generation()?;
            let client = activation.take_consolidated_snapshot_client(generation)?;
            if client.metadata() != consolidated_config.metadata() {
                return Err(TradierRestRuntimeError::SourceTopology);
            }
            consolidated_client = Some(client);

            if let (Some(config), Some(registered)) =
                (derived_index_config.as_ref(), derived_registration.as_ref())
            {
                derived_authority = Some(
                    prepare_surface_authority(
                        &mut registry,
                        registered,
                        config,
                        &app_config,
                        &paths,
                        capture_process,
                    )
                    .await?,
                );
                let generation = derived_authority
                    .as_mut()
                    .ok_or(TradierRestRuntimeError::StartupInvariant)?
                    .take_generation()?;
                let client = activation.take_derived_index_client(generation)?;
                if client.metadata() != config.metadata() {
                    return Err(TradierRestRuntimeError::SourceTopology);
                }
                derived_client = Some(client);
            }
            activation.require_current().await?;
            require_runtime_open(&cancellation)?;
            Ok::<(), TradierRestRuntimeError>(())
        }
        .await;

        if let Err(primary) = setup {
            drop(derived_client.take());
            drop(consolidated_client.take());
            let cleanup = cleanup_startup(
                &mut registry,
                &mut derived_authority,
                &mut consolidated_authority,
                &app_config,
            )
            .await;
            drop(derived_registration);
            drop(consolidated_registration);
            let registry_cleanup = registry.shutdown().map_err(Into::into);
            return Err(combine_startup_failure(primary, cleanup, registry_cleanup));
        }

        if consolidated_authority.is_none()
            || consolidated_client.is_none()
            || derived_index_config.is_some() != derived_registration.is_some()
            || derived_index_config.is_some() != derived_authority.is_some()
            || derived_index_config.is_some() != derived_client.is_some()
        {
            let primary = TradierRestRuntimeError::StartupInvariant;
            drop(derived_client.take());
            drop(consolidated_client.take());
            let cleanup = cleanup_startup(
                &mut registry,
                &mut derived_authority,
                &mut consolidated_authority,
                &app_config,
            )
            .await;
            drop(derived_registration);
            drop(consolidated_registration);
            let registry_cleanup = registry.shutdown().map_err(Into::into);
            return Err(combine_startup_failure(primary, cleanup, registry_cleanup));
        }
        let (consolidated_authority, consolidated_client) =
            match (consolidated_authority.take(), consolidated_client.take()) {
                (Some(authority), Some(client)) => (authority, client),
                (authority, client) => {
                    consolidated_authority = authority;
                    consolidated_client = client;
                    let primary = TradierRestRuntimeError::StartupInvariant;
                    drop(derived_client.take());
                    drop(consolidated_client.take());
                    let cleanup = cleanup_startup(
                        &mut registry,
                        &mut derived_authority,
                        &mut consolidated_authority,
                        &app_config,
                    )
                    .await;
                    drop(derived_registration);
                    drop(consolidated_registration);
                    let registry_cleanup = registry.shutdown().map_err(Into::into);
                    return Err(combine_startup_failure(primary, cleanup, registry_cleanup));
                }
            };
        let consolidated_generation = consolidated_authority.generation();
        let derived_index_generation = derived_authority
            .as_ref()
            .map(PreparedSurfaceAuthority::generation);

        let (commands, receiver) = mpsc::channel(limits.command_capacity().get());
        let response_count = Arc::new(Semaphore::new(limits.outstanding_responses().get()));
        let response_bytes = Arc::new(Semaphore::new(response_byte_capacity));
        let (terminal_sender, terminal_receiver) = watch::channel(None);
        let client = TradierRestClient {
            commands,
            response_count,
            response_bytes,
            maximum_response_bytes: limits.maximum_response_bytes().get(),
            derived_indexes_available: derived_authority.is_some(),
            cancellation: cancellation.clone(),
            terminal: terminal_receiver,
        };
        let derived = derived_index_config
            .zip(derived_registration)
            .zip(derived_authority)
            .zip(derived_client)
            .map(
                |(((config, registration), authority), client)| RestSurfaceOwner {
                    config,
                    registration: Some(registration),
                    authority,
                    client: Some(client),
                },
            );
        let actor = RestActorOwner {
            registry: Some(registry),
            consolidated: Some(RestSurfaceOwner {
                config: consolidated_config,
                registration: Some(consolidated_registration),
                authority: consolidated_authority,
                client: Some(consolidated_client),
            }),
            derived,
            quote_without_greeks,
            quote_with_greeks,
            app_config,
        };
        let actor_cancellation = cancellation.clone();
        let worker = tokio::spawn(async move {
            actor
                .run(receiver, terminal_sender, actor_cancellation)
                .await
        });
        Ok(Self {
            client,
            consolidated_generation,
            derived_index_generation,
            cancellation,
            worker,
            shutdown_deadline,
        })
    }

    pub(crate) fn client(&self) -> TradierRestClient {
        self.client.clone()
    }

    pub(crate) const fn consolidated_generation(&self) -> ConnectionGeneration {
        self.consolidated_generation
    }

    pub(crate) const fn derived_index_generation(&self) -> Option<ConnectionGeneration> {
        self.derived_index_generation
    }

    pub(crate) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && !self.worker.is_finished()
            && self.client.current_failure().is_none()
    }

    /// Cancels pending work and waits for both generations, capture helpers, and registry closure.
    pub(crate) async fn shutdown(mut self) -> Result<TradierRestShutdown, TradierRestRuntimeError> {
        self.cancellation.cancel();
        match tokio::time::timeout(self.shutdown_deadline, &mut self.worker).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(TradierRestRuntimeError::Worker(error)),
            Err(_elapsed) => {
                self.worker.abort();
                let _aborted = (&mut self.worker).await;
                Err(TradierRestRuntimeError::ShutdownDeadline)
            }
        }
    }
}

impl Drop for TradierRestRuntime {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Cloneable bounded request handle; it owns no provider client or account credential.
#[derive(Clone, Debug)]
pub(crate) struct TradierRestClient {
    commands: mpsc::Sender<RestCommand>,
    response_count: Arc<Semaphore>,
    response_bytes: Arc<Semaphore>,
    maximum_response_bytes: u32,
    derived_indexes_available: bool,
    cancellation: CancellationToken,
    terminal: watch::Receiver<Option<TradierRestTerminalFailure>>,
}

impl TradierRestClient {
    pub(crate) fn current_failure(&self) -> Option<TradierRestTerminalFailure> {
        *self.terminal.borrow()
    }

    pub(crate) async fn fetch_configured_quotes(
        &self,
        include_greeks: bool,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<TradierQuoteBootstrapLease, TradierRestRequestError> {
        match self
            .dispatch(
                RestOperation::ConfiguredQuotes { include_greeks },
                cancellation,
                deadline,
            )
            .await?
        {
            RestResponse::Quotes { batch, ticket } => Ok(TradierQuoteBootstrapLease {
                batch,
                _ticket: ticket,
            }),
            RestResponse::DerivedIndexes { .. } => {
                Err(TradierRestRequestError::ResponseKindMismatch)
            }
        }
    }

    pub(crate) async fn fetch_derived_indexes(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<TradierDerivedIndexLease, TradierRestRequestError> {
        if !self.derived_indexes_available {
            return Err(TradierRestRequestError::Unavailable);
        }
        match self
            .dispatch(RestOperation::DerivedIndexes, cancellation, deadline)
            .await?
        {
            RestResponse::DerivedIndexes { batch, ticket } => Ok(TradierDerivedIndexLease {
                batch,
                _ticket: ticket,
            }),
            RestResponse::Quotes { .. } => Err(TradierRestRequestError::ResponseKindMismatch),
        }
    }

    async fn dispatch(
        &self,
        operation: RestOperation,
        caller_cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<RestResponse, TradierRestRequestError> {
        require_request_open(self, caller_cancellation, deadline)?;
        let count = acquire_response_count(
            Arc::clone(&self.response_count),
            &self.cancellation,
            caller_cancellation,
            deadline,
        )
        .await?;
        let bytes = acquire_response_bytes(
            Arc::clone(&self.response_bytes),
            self.maximum_response_bytes,
            &self.cancellation,
            caller_cancellation,
            deadline,
        )
        .await?;
        let ticket = ResponseBudgetTicket {
            charged_bytes: self.maximum_response_bytes,
            _count: count,
            _bytes: bytes,
        };
        let operation_cancellation = self.cancellation.child_token();
        let cancellation_guard = OperationCancellation::new(operation_cancellation.clone());
        let (response, receiver) = oneshot::channel();
        let command = RestCommand {
            operation,
            deadline,
            cancellation: operation_cancellation,
            response,
            ticket,
        };
        tokio::select! {
            biased;
            () = caller_cancellation.cancelled() => {
                return Err(TradierRestRequestError::Cancelled);
            }
            () = self.cancellation.cancelled() => {
                return Err(current_terminal_or_closed(&self.terminal));
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(TradierRestRequestError::Deadline);
            }
            result = self.commands.send(command) => {
                result.map_err(|_error| current_terminal_or_closed(&self.terminal))?;
            }
        }
        let result = tokio::select! {
            biased;
            () = caller_cancellation.cancelled() => Err(TradierRestRequestError::Cancelled),
            () = self.cancellation.cancelled() => Err(current_terminal_or_closed(&self.terminal)),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(TradierRestRequestError::Deadline)
            }
            response = receiver => {
                response.map_err(|_closed| current_terminal_or_closed(&self.terminal))?
            }
        };
        drop(cancellation_guard);
        result
    }
}

/// Exact configured-set quote bootstrap plus retained response-capacity ownership.
#[derive(Debug)]
pub(crate) struct TradierQuoteBootstrapLease {
    batch: TradierQuoteBatch,
    _ticket: ResponseBudgetTicket,
}

impl TradierQuoteBootstrapLease {
    pub(crate) const fn batch(&self) -> &TradierQuoteBatch {
        &self.batch
    }
}

/// Exact derived-index batch plus retained response-capacity ownership.
#[derive(Debug)]
pub(crate) struct TradierDerivedIndexLease {
    batch: TradierDerivedIndexBatch,
    _ticket: ResponseBudgetTicket,
}

impl TradierDerivedIndexLease {
    pub(crate) const fn batch(&self) -> &TradierDerivedIndexBatch {
        &self.batch
    }
}

#[derive(Debug)]
struct ResponseBudgetTicket {
    charged_bytes: u32,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct OperationCancellation(CancellationToken);

impl OperationCancellation {
    const fn new(cancellation: CancellationToken) -> Self {
        Self(cancellation)
    }
}

impl Drop for OperationCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Debug)]
struct RestCommand {
    operation: RestOperation,
    deadline: Instant,
    cancellation: CancellationToken,
    response: oneshot::Sender<Result<RestResponse, TradierRestRequestError>>,
    ticket: ResponseBudgetTicket,
}

#[derive(Debug)]
enum RestOperation {
    ConfiguredQuotes { include_greeks: bool },
    DerivedIndexes,
}

#[derive(Debug)]
enum RestResponse {
    Quotes {
        batch: TradierQuoteBatch,
        ticket: ResponseBudgetTicket,
    },
    DerivedIndexes {
        batch: TradierDerivedIndexBatch,
        ticket: ResponseBudgetTicket,
    },
}

#[derive(Debug)]
struct PreparedSurfaceAuthority {
    session: Option<CurrentSourceSession>,
    generation_number: ConnectionGeneration,
    generation: Option<LiveSourceGeneration>,
    capture: RawCapturePublisher<CaptureGenerationCapabilities>,
    capture_control: Option<RawCaptureControl<CaptureGenerationCapabilities>>,
    capture_writer: Option<ProcessJournalCaptureWriter<CaptureGenerationCapabilities>>,
}

impl PreparedSurfaceAuthority {
    const fn generation(&self) -> ConnectionGeneration {
        self.generation_number
    }

    fn take_generation(&mut self) -> Result<LiveSourceGeneration, TradierRestRuntimeError> {
        self.generation
            .take()
            .ok_or(TradierRestRuntimeError::StartupInvariant)
    }
}

#[derive(Debug)]
struct RestSurfaceOwner {
    config: TradierSourceConfig,
    registration: Option<RegisteredSource>,
    authority: PreparedSurfaceAuthority,
    client: Option<TradierSnapshotClient>,
}

#[derive(Debug)]
struct RestActorOwner {
    registry: Option<AuthoritativeSourceRegistry>,
    consolidated: Option<RestSurfaceOwner>,
    derived: Option<RestSurfaceOwner>,
    quote_without_greeks: TradierQuoteRequest,
    quote_with_greeks: TradierQuoteRequest,
    app_config: AppConfig,
}

impl RestActorOwner {
    async fn run(
        mut self,
        mut commands: mpsc::Receiver<RestCommand>,
        terminal: watch::Sender<Option<TradierRestTerminalFailure>>,
        cancellation: CancellationToken,
    ) -> Result<TradierRestShutdown, TradierRestRuntimeError> {
        let mut terminal_failure = None;
        loop {
            let command = tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                command = commands.recv() => match command {
                    Some(command) => command,
                    None => break,
                },
            };
            let RestCommand {
                operation,
                deadline,
                cancellation: operation_cancellation,
                response,
                ticket,
            } = command;
            let result = self
                .execute(operation, deadline, operation_cancellation, ticket)
                .await;
            if let Err(error) = &result
                && let Some(failure) = error.terminal_failure()
            {
                terminal_failure = Some(failure);
                terminal.send_replace(Some(failure));
                cancellation.cancel();
            }
            let _ignored = response.send(result);
            if terminal_failure.is_some() {
                break;
            }
        }
        commands.close();
        while let Ok(command) = commands.try_recv() {
            let failure = terminal_failure.map_or(
                TradierRestRequestError::WorkerClosed,
                TradierRestRequestError::Terminal,
            );
            let _ignored = command.response.send(Err(failure));
        }
        let shutdown = self.cleanup().await;
        if terminal_failure.is_none() && !cancellation.is_cancelled() {
            terminal.send_replace(Some(TradierRestTerminalFailure::WorkerStopped));
        }
        shutdown
    }

    async fn execute(
        &mut self,
        operation: RestOperation,
        deadline: Instant,
        cancellation: CancellationToken,
        ticket: ResponseBudgetTicket,
    ) -> Result<RestResponse, TradierRestRequestError> {
        if Instant::now() >= deadline {
            return Err(TradierRestRequestError::Deadline);
        }
        let result = match operation {
            RestOperation::ConfiguredQuotes { include_greeks } => {
                let request = if include_greeks {
                    self.quote_with_greeks.clone()
                } else {
                    self.quote_without_greeks.clone()
                };
                let surface = self
                    .consolidated
                    .as_mut()
                    .ok_or(TradierRestRequestError::WorkerClosed)?;
                let client = surface
                    .client
                    .as_mut()
                    .ok_or(TradierRestRequestError::WorkerClosed)?;
                let response = run_with_deadline(
                    client.fetch_quotes(request, cancellation.clone()),
                    &cancellation,
                    deadline,
                )
                .await?;
                validate_and_capture(
                    &surface.config,
                    surface.authority.generation(),
                    response.evidence(),
                    &surface.authority.capture,
                )?;
                validate_response_size(
                    quote_batch_retained_bytes(&response)?,
                    ticket.charged_bytes,
                )?;
                RestResponse::Quotes {
                    batch: response,
                    ticket,
                }
            }
            RestOperation::DerivedIndexes => {
                let surface = self
                    .derived
                    .as_mut()
                    .ok_or(TradierRestRequestError::Unavailable)?;
                let client = surface
                    .client
                    .as_mut()
                    .ok_or(TradierRestRequestError::WorkerClosed)?;
                let response = run_with_deadline(
                    client.fetch_derived_indexes(cancellation.clone()),
                    &cancellation,
                    deadline,
                )
                .await?;
                validate_and_capture(
                    &surface.config,
                    surface.authority.generation(),
                    response.evidence(),
                    &surface.authority.capture,
                )?;
                validate_response_size(
                    derived_batch_retained_bytes(&response)?,
                    ticket.charged_bytes,
                )?;
                RestResponse::DerivedIndexes {
                    batch: response,
                    ticket,
                }
            }
        };
        Ok(result)
    }

    async fn cleanup(mut self) -> Result<TradierRestShutdown, TradierRestRuntimeError> {
        let consolidated_generation = self
            .consolidated
            .as_ref()
            .map(|surface| surface.authority.generation())
            .ok_or(TradierRestRuntimeError::StartupInvariant)?;
        let derived_index_generation = self
            .derived
            .as_ref()
            .map(|surface| surface.authority.generation());
        let mut cleanup_error = None;
        if let Some(mut registry) = self.registry.take() {
            if let Some(mut surface) = self.derived.take() {
                retain_first_error(
                    &mut cleanup_error,
                    cleanup_surface(&mut registry, &mut surface, &self.app_config).await,
                );
            }
            if let Some(mut surface) = self.consolidated.take() {
                retain_first_error(
                    &mut cleanup_error,
                    cleanup_surface(&mut registry, &mut surface, &self.app_config).await,
                );
            }
            retain_first_error(
                &mut cleanup_error,
                registry.shutdown().map_err(TradierRestRuntimeError::from),
            );
        } else {
            retain_first_error(
                &mut cleanup_error,
                Err(TradierRestRuntimeError::StartupInvariant),
            );
        }
        match cleanup_error {
            Some(error) => Err(error),
            None => Ok(TradierRestShutdown {
                consolidated_generation,
                derived_index_generation,
            }),
        }
    }
}

/// Clean terminal identity for the exact generations owned by the runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TradierRestShutdown {
    consolidated_generation: ConnectionGeneration,
    derived_index_generation: Option<ConnectionGeneration>,
}

impl TradierRestShutdown {
    pub(crate) const fn consolidated_generation(self) -> ConnectionGeneration {
        self.consolidated_generation
    }

    pub(crate) const fn derived_index_generation(self) -> Option<ConnectionGeneration> {
        self.derived_index_generation
    }
}

async fn prepare_surface_authority(
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    config: &TradierSourceConfig,
    app_config: &AppConfig,
    paths: &LocalPaths,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
) -> Result<PreparedSurfaceAuthority, TradierRestRuntimeError> {
    let started_at = system_timestamp()?;
    let session = registry.begin_next_session(
        registered,
        SessionId::new(SourceIdentifier::try_from(format!(
            "tradier-rest-{}",
            Uuid::new_v4()
        ))?),
        started_at,
    )?;
    let generation_number = session.generation();
    let mut capture_control = None;
    let mut capture_writer = None;
    let prepared = (|| {
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let (publisher, control, writer) = raw_capture_channel(
            &capture_process,
            CaptureChannelLimits::new(
                app_config.capture_queue_capacity(),
                app_config.capture_memory_ceiling_bytes(),
            ),
            capabilities,
        )?;
        let flush_records = NonZeroUsize::new(
            app_config
                .capture_queue_capacity()
                .get()
                .min(CAPTURE_FLUSH_RECORDS),
        )
        .ok_or(TradierRestRuntimeError::StartupInvariant)?;
        let policy =
            CaptureWriterPolicy::try_new(flush_records, app_config.capture_flush_interval())?;
        let process_config = ProcessJournalCaptureConfig::try_new(
            paths.root(),
            capture_source_key(config.metadata().source_id())?,
            CAPTURE_HELPER_STARTUP_DEADLINE,
        )?;
        let writer_handle = spawn_process_journal_capture_writer(writer, process_config, policy)?;
        capture_control = Some(control);
        capture_writer = Some(writer_handle);
        capture_control
            .as_mut()
            .ok_or(TradierRestRuntimeError::StartupInvariant)?
            .activate_initial()?;
        let generation = registry.take_live_source_generation(&session)?;
        Ok::<_, TradierRestRuntimeError>((publisher, generation))
    })();
    match prepared {
        Ok((capture, generation)) => Ok(PreparedSurfaceAuthority {
            session: Some(session),
            generation_number,
            generation: Some(generation),
            capture,
            capture_control,
            capture_writer,
        }),
        Err(primary) => {
            let cleanup = cleanup_partial_authority(
                registry,
                &session,
                &mut capture_control,
                &mut capture_writer,
                app_config,
                started_at,
            )
            .await;
            Err(combine_primary_cleanup(primary, cleanup))
        }
    }
}

async fn cleanup_startup(
    registry: &mut AuthoritativeSourceRegistry,
    derived: &mut Option<PreparedSurfaceAuthority>,
    consolidated: &mut Option<PreparedSurfaceAuthority>,
    app_config: &AppConfig,
) -> Result<(), TradierRestRuntimeError> {
    let mut error = None;
    if let Some(mut authority) = derived.take() {
        retain_first_error(
            &mut error,
            cleanup_prepared_authority(registry, &mut authority, app_config).await,
        );
    }
    if let Some(mut authority) = consolidated.take() {
        retain_first_error(
            &mut error,
            cleanup_prepared_authority(registry, &mut authority, app_config).await,
        );
    }
    error.map_or(Ok(()), Err)
}

async fn cleanup_surface(
    registry: &mut AuthoritativeSourceRegistry,
    surface: &mut RestSurfaceOwner,
    app_config: &AppConfig,
) -> Result<(), TradierRestRuntimeError> {
    drop(surface.client.take());
    let cleanup = cleanup_prepared_authority(registry, &mut surface.authority, app_config).await;
    drop(surface.registration.take());
    cleanup
}

async fn cleanup_prepared_authority(
    registry: &mut AuthoritativeSourceRegistry,
    authority: &mut PreparedSurfaceAuthority,
    app_config: &AppConfig,
) -> Result<(), TradierRestRuntimeError> {
    drop(authority.generation.take());
    let Some(session) = authority.session.take() else {
        return Err(TradierRestRuntimeError::StartupInvariant);
    };
    cleanup_partial_authority(
        registry,
        &session,
        &mut authority.capture_control,
        &mut authority.capture_writer,
        app_config,
        session.started_at(),
    )
    .await
}

async fn cleanup_partial_authority(
    registry: &mut AuthoritativeSourceRegistry,
    session: &CurrentSourceSession,
    capture_control: &mut Option<RawCaptureControl<CaptureGenerationCapabilities>>,
    capture_writer: &mut Option<ProcessJournalCaptureWriter<CaptureGenerationCapabilities>>,
    app_config: &AppConfig,
    fallback_time: Timestamp,
) -> Result<(), TradierRestRuntimeError> {
    let mut error = None;
    retain_first_error(
        &mut error,
        registry
            .end_session(session, system_timestamp().unwrap_or(fallback_time))
            .map_err(Into::into),
    );
    if let Some(mut control) = capture_control.take() {
        control.invalidate_current();
    }
    if let Some(writer) = capture_writer.take() {
        retain_first_error(
            &mut error,
            shutdown_capture_writer(writer, app_config.capture_shutdown()).await,
        );
    }
    error.map_or(Ok(()), Err)
}

async fn shutdown_capture_writer(
    writer: ProcessJournalCaptureWriter<CaptureGenerationCapabilities>,
    deadline: Duration,
) -> Result<(), TradierRestRuntimeError> {
    let policy = ProcessCaptureShutdownPolicy::try_new(deadline, deadline)?;
    let outcome = writer.shutdown(policy).await;
    if capture_shutdown_is_clean(&outcome) {
        Ok(())
    } else {
        Err(TradierRestRuntimeError::IncompleteCaptureShutdown(outcome))
    }
}

fn capture_shutdown_is_clean(outcome: &ProcessCaptureShutdownOutcome) -> bool {
    outcome.disposition() == ProcessCaptureShutdownDisposition::Complete
        && outcome.helper_reaped()
        && outcome.worker_termination().is_some_and(|termination| {
            !termination.shutdown_deadline_elapsed() && !termination.outcome().is_incomplete()
        })
}

async fn run_with_deadline<T>(
    operation: impl Future<Output = Result<T, TradierRestError>>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, TradierRestRequestError> {
    if Instant::now() >= deadline {
        return Err(TradierRestRequestError::Deadline);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(TradierRestRequestError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(TradierRestRequestError::Deadline)
        }
        result = operation => result.map_err(|error| {
            terminal_failure_for_rest_error(&error)
                .map_or(TradierRestRequestError::Provider(error), TradierRestRequestError::Terminal)
        }),
    }
}

fn terminal_failure_for_rest_error(error: &TradierRestError) -> Option<TradierRestTerminalFailure> {
    match error {
        TradierRestError::InvalidRequest
        | TradierRestError::UnknownSymbol
        | TradierRestError::Cancelled => None,
        TradierRestError::Source(
            source @ (SourceError::Network
            | SourceError::ProviderUnavailable
            | SourceError::BudgetWaitUntil { .. }),
        ) => {
            let _retryable = source;
            None
        }
        TradierRestError::Source(source) => Some(TradierRestTerminalFailure::Source(*source)),
        TradierRestError::InvalidProfile
        | TradierRestError::Url
        | TradierRestError::NetworkPolicy
        | TradierRestError::InvalidRateLimitEvidence
        | TradierRestError::InvalidResponse
        | TradierRestError::MissingObservation
        | TradierRestError::UnexpectedObservation
        | TradierRestError::DuplicateObservation
        | TradierRestError::InvalidDecimal
        | TradierRestError::InvalidTimestamp
        | TradierRestError::InvalidDate
        | TradierRestError::ResponseLimitExceeded
        | TradierRestError::Allocation => Some(TradierRestTerminalFailure::EvidenceMismatch),
    }
}

fn validate_and_capture(
    config: &TradierSourceConfig,
    generation: ConnectionGeneration,
    evidence: &TradierRestEvidence,
    capture: &RawCapturePublisher<CaptureGenerationCapabilities>,
) -> Result<(), TradierRestRequestError> {
    if evidence.source_id() != config.metadata().source_id()
        || evidence.metadata_revision() != config.metadata().revision()
        || evidence.connection_generation() != generation
        || evidence.payload().is_empty()
        || u64::try_from(evidence.payload().len()).map_or(true, |bytes| {
            bytes > config.transport_limits().http().max_response_bytes()
        })
    {
        return Err(TradierRestRequestError::Terminal(
            TradierRestTerminalFailure::EvidenceMismatch,
        ));
    }
    capture
        .try_publish(evidence.raw_frame())
        .map(|_receipt| ())
        .map_err(|error| {
            TradierRestRequestError::Terminal(TradierRestTerminalFailure::Capture(error))
        })
}

fn validate_response_size(
    retained_bytes: usize,
    charged_bytes: u32,
) -> Result<(), TradierRestRequestError> {
    if retained_bytes > charged_bytes as usize {
        Err(TradierRestRequestError::Terminal(
            TradierRestTerminalFailure::ResponseAccounting,
        ))
    } else {
        Ok(())
    }
}

fn quote_batch_retained_bytes(batch: &TradierQuoteBatch) -> Result<usize, TradierRestRequestError> {
    let mut bytes = evidence_retained_bytes(batch.evidence())?
        .checked_add(size_of::<TradierQuoteBatch>())
        .and_then(|bytes| bytes.checked_add(size_of_val(batch.observations())))
        .ok_or(response_accounting_error())?;
    for observation in batch.observations() {
        bytes = checked_add(bytes, observation.symbol().retained_bytes())?;
        bytes = checked_add(bytes, observation.venue().retained_bytes())?;
        if let Some(side) = observation.bid() {
            bytes = checked_add(bytes, side.exchange().retained_bytes())?;
        }
        if let Some(side) = observation.ask() {
            bytes = checked_add(bytes, side.exchange().retained_bytes())?;
        }
    }
    checked_add(bytes, RESPONSE_ACCOUNTING_FIXED_SLACK)
}

fn derived_batch_retained_bytes(
    batch: &TradierDerivedIndexBatch,
) -> Result<usize, TradierRestRequestError> {
    let mut bytes = evidence_retained_bytes(batch.evidence())?
        .checked_add(size_of::<TradierDerivedIndexBatch>())
        .and_then(|bytes| bytes.checked_add(size_of_val(batch.observations())))
        .ok_or(response_accounting_error())?;
    for observation in batch.observations() {
        bytes = checked_add(bytes, observation.symbol().retained_bytes())?;
        bytes = checked_add(bytes, observation.venue().retained_bytes())?;
    }
    checked_add(bytes, RESPONSE_ACCOUNTING_FIXED_SLACK)
}

fn evidence_retained_bytes(
    evidence: &TradierRestEvidence,
) -> Result<usize, TradierRestRequestError> {
    let frame = evidence
        .raw_frame()
        .checked_retained_footprint()
        .and_then(|footprint| footprint.checked_complete_bytes())
        .map_err(|_error| response_accounting_error())?;
    frame
        .checked_add(size_of::<TradierRestEvidence>())
        .and_then(|bytes| bytes.checked_add(evidence.request_url().len()))
        .and_then(|bytes| bytes.checked_add(ARC_CONTROL_BLOCK_SLACK))
        .ok_or(response_accounting_error())
}

fn checked_add(bytes: usize, additional: usize) -> Result<usize, TradierRestRequestError> {
    bytes
        .checked_add(additional)
        .ok_or(response_accounting_error())
}

const fn response_accounting_error() -> TradierRestRequestError {
    TradierRestRequestError::Terminal(TradierRestTerminalFailure::ResponseAccounting)
}

fn validate_configs(
    consolidated: &TradierSourceConfig,
    derived: Option<&TradierSourceConfig>,
    limits: TradierRestRuntimeLimits,
) -> Result<(), TradierRestRuntimeError> {
    if consolidated.profile() != TradierLogicalProfile::ConsolidatedSecurities
        || consolidated.access_surface() != TradierAccessSurface::RestSnapshots
        || consolidated.metadata().quality_ceiling() != DataQuality::Aggregated
        || consolidated.mappings().is_empty()
    {
        return Err(TradierRestRuntimeError::SourceTopology);
    }
    if let Some(derived) = derived {
        if derived.profile() != TradierLogicalProfile::DerivedIndexes
            || derived.access_surface() != TradierAccessSurface::RestSnapshots
            || derived.metadata().quality_ceiling() != DataQuality::Modeled
            || derived.mappings().is_empty()
            || derived.metadata().source_id() == consolidated.metadata().source_id()
        {
            return Err(TradierRestRuntimeError::SourceTopology);
        }
    }
    let response_maximum = u64::from(limits.maximum_response_bytes().get());
    if consolidated.transport_limits().http().max_response_bytes() > response_maximum
        || derived.is_some_and(|config| {
            config.transport_limits().http().max_response_bytes() > response_maximum
        })
    {
        return Err(TradierRestRuntimeError::InvalidLimits);
    }
    Ok(())
}

fn configured_quote_symbols(
    config: &TradierSourceConfig,
) -> Result<Vec<SourceIdentifier>, TradierRestRuntimeError> {
    let mut symbols = Vec::new();
    symbols
        .try_reserve_exact(config.mappings().len())
        .map_err(|_error| TradierRestRuntimeError::Allocation)?;
    symbols.extend(
        config
            .mappings()
            .iter()
            .map(|mapping| mapping.symbol().clone()),
    );
    Ok(symbols)
}

fn capture_source_key(source_id: &SourceId) -> Result<String, TradierRestRuntimeError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(source_id.as_str().as_bytes());
    let mut key = String::new();
    key.try_reserve_exact("tradier-rest-".len() + digest.len() * 2)
        .map_err(|_error| TradierRestRuntimeError::Allocation)?;
    key.push_str("tradier-rest-");
    for byte in digest {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(key)
}

fn total_shutdown_deadline(app_config: &AppConfig) -> Result<Duration, TradierRestRuntimeError> {
    app_config
        .capture_shutdown()
        .checked_mul(4)
        .and_then(|capture| capture.checked_add(app_config.source_shutdown()))
        .ok_or(TradierRestRuntimeError::DeadlineRange)
}

fn system_timestamp() -> Result<Timestamp, TradierRestRuntimeError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| TradierRestRuntimeError::SystemTime)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| TradierRestRuntimeError::SystemTime)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn shutdown_registry_after_startup_error(
    registry: AuthoritativeSourceRegistry,
    primary: TradierRestRuntimeError,
) -> TradierRestRuntimeError {
    match registry.shutdown() {
        Ok(()) => primary,
        Err(cleanup) => TradierRestRuntimeError::StartupCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup.into()),
        },
    }
}

fn combine_primary_cleanup(
    primary: TradierRestRuntimeError,
    cleanup: Result<(), TradierRestRuntimeError>,
) -> TradierRestRuntimeError {
    match cleanup {
        Ok(()) => primary,
        Err(cleanup) => TradierRestRuntimeError::StartupCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        },
    }
}

fn combine_startup_failure(
    primary: TradierRestRuntimeError,
    cleanup: Result<(), TradierRestRuntimeError>,
    registry_cleanup: Result<(), TradierRestRuntimeError>,
) -> TradierRestRuntimeError {
    let primary = combine_primary_cleanup(primary, cleanup);
    combine_primary_cleanup(primary, registry_cleanup)
}

fn retain_first_error(
    first: &mut Option<TradierRestRuntimeError>,
    result: Result<(), TradierRestRuntimeError>,
) {
    if first.is_none()
        && let Err(error) = result
    {
        *first = Some(error);
    }
}

fn require_request_open(
    client: &TradierRestClient,
    caller_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), TradierRestRequestError> {
    if caller_cancellation.is_cancelled() {
        return Err(TradierRestRequestError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(TradierRestRequestError::Deadline);
    }
    if let Some(failure) = client.current_failure() {
        return Err(TradierRestRequestError::Terminal(failure));
    }
    if client.cancellation.is_cancelled() || client.commands.is_closed() {
        return Err(TradierRestRequestError::WorkerClosed);
    }
    Ok(())
}

fn require_runtime_open(cancellation: &CancellationToken) -> Result<(), TradierRestRuntimeError> {
    if cancellation.is_cancelled() {
        Err(TradierRestRuntimeError::Cancelled)
    } else {
        Ok(())
    }
}

async fn acquire_response_count(
    budget: Arc<Semaphore>,
    runtime_cancellation: &CancellationToken,
    caller_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, TradierRestRequestError> {
    tokio::select! {
        biased;
        () = caller_cancellation.cancelled() => Err(TradierRestRequestError::Cancelled),
        () = runtime_cancellation.cancelled() => Err(TradierRestRequestError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(TradierRestRequestError::Deadline)
        }
        permit = budget.acquire_owned() => {
            permit.map_err(|_closed| TradierRestRequestError::WorkerClosed)
        }
    }
}

async fn acquire_response_bytes(
    budget: Arc<Semaphore>,
    bytes: u32,
    runtime_cancellation: &CancellationToken,
    caller_cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, TradierRestRequestError> {
    tokio::select! {
        biased;
        () = caller_cancellation.cancelled() => Err(TradierRestRequestError::Cancelled),
        () = runtime_cancellation.cancelled() => Err(TradierRestRequestError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(TradierRestRequestError::Deadline)
        }
        permit = budget.acquire_many_owned(bytes) => {
            permit.map_err(|_closed| TradierRestRequestError::WorkerClosed)
        }
    }
}

fn current_terminal_or_closed(
    terminal: &watch::Receiver<Option<TradierRestTerminalFailure>>,
) -> TradierRestRequestError {
    (*terminal.borrow()).map_or(
        TradierRestRequestError::WorkerClosed,
        TradierRestRequestError::Terminal,
    )
}

/// One bounded on-demand REST operation failed without returning partial data.
#[derive(Debug, Error)]
pub(crate) enum TradierRestRequestError {
    #[error("Tradier derived-index data is not configured for this account")]
    Unavailable,
    #[error("Tradier REST request was cancelled")]
    Cancelled,
    #[error("Tradier REST request deadline elapsed")]
    Deadline,
    #[error("Tradier REST actor is closed")]
    WorkerClosed,
    #[error("Tradier REST actor returned a response for another operation")]
    ResponseKindMismatch,
    #[error("Tradier REST runtime failed terminally: {0}")]
    Terminal(TradierRestTerminalFailure),
    #[error(transparent)]
    Provider(TradierRestError),
}

impl TradierRestRequestError {
    const fn terminal_failure(&self) -> Option<TradierRestTerminalFailure> {
        match self {
            Self::Terminal(failure) => Some(*failure),
            Self::Unavailable
            | Self::Cancelled
            | Self::Deadline
            | Self::WorkerClosed
            | Self::ResponseKindMismatch
            | Self::Provider(_) => None,
        }
    }
}

/// Startup, lifecycle, authority, or cleanup failure for the REST owner.
#[derive(Debug, Error)]
pub(crate) enum TradierRestRuntimeError {
    #[error("Tradier REST runtime limits are invalid")]
    InvalidLimits,
    #[error("Tradier REST logical source topology is invalid")]
    SourceTopology,
    #[error("Tradier REST sources do not share one account provider-budget allocation")]
    BudgetTopology,
    #[error("Tradier REST startup ownership invariant failed")]
    StartupInvariant,
    #[error("Tradier REST bounded allocation failed")]
    Allocation,
    #[error("Tradier REST startup was cancelled")]
    Cancelled,
    #[error("Tradier REST lifecycle deadline cannot be represented")]
    DeadlineRange,
    #[error("system time could not be represented as a Market Squawk timestamp")]
    SystemTime,
    #[error("Tradier REST shutdown exceeded its bounded deadline")]
    ShutdownDeadline,
    #[error("Tradier REST capture did not shut down cleanly: {0:?}")]
    IncompleteCaptureShutdown(ProcessCaptureShutdownOutcome),
    #[error("Tradier REST startup failed and cleanup also failed")]
    StartupCleanup {
        primary: Box<TradierRestRuntimeError>,
        cleanup: Box<TradierRestRuntimeError>,
    },
    #[error(transparent)]
    Activation(#[from] TradierMarketDataActivationError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    Paths(#[from] market_squawk_platform::PathError),
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    CaptureInfrastructure(#[from] DestinationFenceRegistryInitializationError),
    #[error(transparent)]
    CaptureChannel(#[from] CaptureChannelError),
    #[error(transparent)]
    CaptureGeneration(#[from] CaptureGenerationError),
    #[error(transparent)]
    CaptureWriterPolicy(#[from] CaptureWriterPolicyError),
    #[error(transparent)]
    CaptureConfig(#[from] ProcessJournalCaptureConfigError),
    #[error(transparent)]
    CaptureSpawn(#[from] ProcessCaptureWriterSpawnError),
    #[error(transparent)]
    CaptureShutdownPolicy(#[from] ProcessCaptureShutdownPolicyError),
    #[error(transparent)]
    Adapter(#[from] TradierRestError),
    #[error("Tradier REST actor task failed: {0}")]
    Worker(JoinError),
}
