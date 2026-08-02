//! Single per-user installed-service composition and lifecycle authority.

mod analysis;
mod decision;
mod dispatch;
mod governance;
mod governance_persistence;
mod jobs;
mod mcp_client;
mod mcp_control;
mod operations;
mod resources;
mod runtime;
mod tool_services;

use std::{sync::Arc, time::Duration};

use axum::{Router, extract::State, http::Request, response::Response, routing::any};
use dispatch::InstalledApplicationDispatcher;
use market_squawk_mcp::{
    AuditSink, HttpMcpConfig, McpHandlerFactory, McpHttpService, McpLimitSpec, McpLimits,
};
use market_squawk_platform::{
    LocalAuthorityStateStoreError, LocalPaths, PathError, PreferredSecretStore, SecretStore,
};
use market_squawk_runtime::{
    ApplicationClientError, ApplicationProtocolRange, ApplicationProtocolVersion,
    ApplicationRequestScope, CorrelationId, CredentialError, EventHub, EventHubLimits, InputStager,
    InputStagingLimits, LoopbackApplicationClient, MutationReplayGuard, NamedClient, OriginPolicy,
    RendezvousError, ReplayLimits, RuntimeContractError, RuntimeRouter, RuntimeRouterLimits,
};
use market_squawk_services::{JsonStructureLimits, ServiceLimits, ToolServices};
use resources::InstalledResourceProvider;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tool_services::InstalledToolServices;
use uuid::Uuid;

use mcp_client::InstalledMcpRelayTransport;

use crate::{AppConfig, LocalProduct, LocalProductError, jobs::InstalledJobAuthority};

use self::runtime::{PreparedRuntime, current_timestamp};
use self::{
    governance::{
        GovernedActionService, GovernedActionServiceLimits, InstalledGovernanceOperations,
    },
    governance_persistence::GovernancePersistence,
};
use crate::application::governance::{GovernanceAuthority, GovernanceLimits};

const RUNTIME_SECRET_DIRECTORY: &str = "secrets/installed-runtime";
const REQUEST_BODY_BYTES: usize = 1024 * 1024;
const RESPONSE_BODY_BYTES: usize = 64 * 1024 * 1024;
const EVENT_REQUEST_BYTES: usize = 64 * 1024;
const RUNTIME_CONCURRENCY: usize = 64;
const REPLAY_CAPACITY: usize = 4_096;
const RETAINED_EVENTS: usize = 4_096;
const MAXIMUM_EVENT_BYTES: usize = 1024 * 1024;
const MAXIMUM_STAGED_INPUTS: usize = 64;
const MAXIMUM_STAGED_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const CURSOR_LIFETIME: Duration = Duration::from_secs(5 * 60);
const INPUT_TICKET_LIFETIME: Duration = Duration::from_secs(5 * 60);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const MAXIMUM_CLIENT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const GRACEFUL_REQUEST_DRAIN: Duration = Duration::from_secs(5);
const FORCED_REQUEST_DRAIN: Duration = Duration::from_secs(2);
const JOB_RUNNER_DRAIN: Duration = Duration::from_secs(15);
const MCP_CLIENT_REQUESTS: usize = 4;
const TAURI_ORIGINS: [&str; 2] = ["tauri://localhost", "http://tauri.localhost"];

pub use runtime::SystemProcessIdentityVerifier;

/// Native-only connector for one named client of the already running installed service.
pub struct InstalledServiceConnector {
    paths: LocalPaths,
    secret_store: Arc<dyn SecretStore>,
}

impl std::fmt::Debug for InstalledServiceConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledServiceConnector")
            .field("paths", &"[LOCAL CAPABILITIES]")
            .field("secret_store", &"[SECRET AUTHORITY]")
            .finish()
    }
}

impl InstalledServiceConnector {
    /// Opens only discovery and native secret capabilities; it never constructs product domains.
    pub fn try_new(config: &AppConfig) -> Result<Self, InstalledServiceError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        let secret_store = runtime_secret_store(&paths)?;
        Ok(Self {
            paths,
            secret_store,
        })
    }

    /// Opens discovery with an already-owned native secret capability.
    ///
    /// This is intended for a foreground unlock/bootstrap host and deterministic integration
    /// verification. The capability remains native and is never serialized to presentation code.
    pub fn try_new_with_secret_store(
        config: &AppConfig,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, InstalledServiceError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        Ok(Self {
            paths,
            secret_store,
        })
    }

    /// Resolves one exact live generation and returns its authenticated native client.
    pub fn connect(
        &self,
        client: NamedClient,
        origin: Option<String>,
    ) -> Result<LoopbackApplicationClient, InstalledServiceError> {
        self.connect_with_timeout(client, origin, CLIENT_TIMEOUT)
    }

    /// Resolves one exact live generation with an explicitly bounded native-client timeout.
    pub fn connect_with_timeout(
        &self,
        client: NamedClient,
        origin: Option<String>,
        timeout: Duration,
    ) -> Result<LoopbackApplicationClient, InstalledServiceError> {
        if timeout.is_zero() || timeout > MAXIMUM_CLIENT_TIMEOUT {
            return Err(InstalledServiceError::InvalidComposition);
        }
        let structure = JsonStructureLimits::try_new(32, 64 * 1024, 10_000, 2_000)
            .map_err(|_error| InstalledServiceError::InvalidComposition)?;
        runtime::connect_client(
            &self.paths,
            Arc::clone(&self.secret_store),
            client,
            origin,
            structure,
            RESPONSE_BODY_BYTES,
            timeout,
        )
    }

    /// Resolves one current Claude Code or Codex registration into a credential-owning MCP relay
    /// transport. The endpoint and credential remain private to the native connector.
    pub fn connect_mcp_relay(
        &self,
        client: NamedClient,
    ) -> Result<Arc<dyn market_squawk_mcp::McpRelayTransport>, InstalledServiceError> {
        if !matches!(client, NamedClient::ClaudeCode | NamedClient::Codex) {
            return Err(InstalledServiceError::InvalidComposition);
        }
        let resolved =
            runtime::resolve_client_root(&self.paths, Arc::clone(&self.secret_store), client)?;
        let registration = mcp_control::resolve_registration(
            &self.paths,
            resolved.record.runtime(),
            &resolved.registration,
        )?;
        let credential = runtime::load_client_credential(&self.secret_store, &registration)?;
        let transport =
            InstalledMcpRelayTransport::try_new(&resolved.record, credential, CLIENT_TIMEOUT)
                .map_err(|_error| InstalledServiceError::InvalidComposition)?;
        Ok(Arc::new(transport))
    }
}

/// Sole per-user owner for the product, jobs, private runtime, and stateless MCP endpoint.
pub struct InstalledService {
    server: market_squawk_runtime::RuntimeServer,
    product: LocalProduct,
    jobs: InstalledJobAuthority,
    audit: Arc<crate::mcp::audit::DurableAuditSink>,
    runtime: PreparedRuntime,
}

impl std::fmt::Debug for InstalledService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledService")
            .field("runtime", &self.runtime)
            .field("product", &"[LOCAL PRODUCT AUTHORITY]")
            .field("jobs", &self.jobs)
            .field("audit", &"[DURABLE AUDIT AUTHORITY]")
            .field("server", &self.server)
            .finish_non_exhaustive()
    }
}

impl InstalledService {
    /// Composes every installed-product authority, proves the bound route is ready, then publishes
    /// the authenticated rendezvous as the final startup step.
    pub async fn start(config: AppConfig) -> Result<Self, InstalledServiceError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        let secret_store = runtime_secret_store(&paths)?;
        Self::start_prepared(config, paths, secret_store).await
    }

    /// Composes the installed service with an already-owned native secret capability.
    ///
    /// This is the foreground encrypted-fallback and deterministic verification boundary. The
    /// caller cannot replace any product authority, runtime identity, route, or lifecycle policy.
    pub async fn start_with_secret_store(
        config: AppConfig,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, InstalledServiceError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        Self::start_prepared(config, paths, secret_store).await
    }

    async fn start_prepared(
        config: AppConfig,
        paths: LocalPaths,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, InstalledServiceError> {
        let mut runtime = PreparedRuntime::prepare(&paths, secret_store).await?;
        let product = LocalProduct::try_new(config)?;
        let runners = match crate::jobs::InstalledJobRunners::try_new(&product) {
            Ok(runners) => Arc::new(runners),
            Err(error) => {
                shutdown_application(product.application()).await;
                return Err(error.into());
            }
        };
        let jobs =
            match InstalledJobAuthority::open(&paths, runners.registered(), current_timestamp()?)
                .await
            {
                Ok(jobs) => jobs,
                Err(error) => {
                    shutdown_application(product.application()).await;
                    return Err(error.into());
                }
            };
        let ComposedTransport {
            audit,
            server,
            readiness,
        } = match compose_transport(&paths, &mut runtime, &product, &jobs, runners) {
            Ok(composed) => composed,
            Err(error) => {
                cleanup_startup(&product, &jobs).await;
                return Err(error);
            }
        };
        if readiness
            .probe_ready(CancellationToken::new())
            .await
            .is_err()
            || server.is_finished()
        {
            drain_failed_server(server).await;
            cleanup_startup(&product, &jobs).await;
            return Err(InstalledServiceError::ReadinessFailed);
        }
        if let Err(error) = runtime.publish() {
            drain_failed_server(server).await;
            cleanup_startup(&product, &jobs).await;
            return Err(error);
        }
        Ok(Self {
            server,
            product,
            jobs,
            audit,
            runtime,
        })
    }

    /// Serves until cancellation, then executes every shutdown phase in authority order.
    pub async fn run(self, cancellation: CancellationToken) -> Result<(), InstalledServiceError> {
        let Self {
            server,
            product,
            jobs,
            audit,
            runtime,
        } = self;
        let transport = server
            .run_until(cancellation, GRACEFUL_REQUEST_DRAIN, FORCED_REQUEST_DRAIN)
            .await
            .is_ok();
        let jobs_stopped = if let Ok(at) = current_timestamp() {
            jobs.shutdown_authority(at, JOB_RUNNER_DRAIN).await.is_ok()
        } else {
            false
        };
        let application = shutdown_application(product.application()).await;
        let audit_flushed = audit.flush().is_ok();
        let jobs_closed = jobs.shutdown_repository().await.is_ok();
        let rendezvous_retired = runtime.retire().unwrap_or(false);
        let report = InstalledServiceShutdownReport {
            transport,
            jobs_stopped,
            application,
            audit_flushed,
            jobs_closed,
            rendezvous_retired,
        };
        if report.is_complete() {
            Ok(())
        } else {
            Err(InstalledServiceError::ShutdownIncomplete(report))
        }
    }
}

struct ComposedTransport {
    audit: Arc<crate::mcp::audit::DurableAuditSink>,
    server: market_squawk_runtime::RuntimeServer,
    readiness: LoopbackApplicationClient,
}

fn compose_transport(
    paths: &LocalPaths,
    runtime: &mut PreparedRuntime,
    product: &LocalProduct,
    jobs: &InstalledJobAuthority,
    runners: Arc<crate::jobs::InstalledJobRunners>,
) -> Result<ComposedTransport, InstalledServiceError> {
    let structure = JsonStructureLimits::try_new(32, 64 * 1024, 10_000, 2_000)
        .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let service_limits =
        ServiceLimits::try_new(64 * 1024, 1_000, RESPONSE_BODY_BYTES, 1_000_000, structure)
            .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let router_limits = RuntimeRouterLimits::try_new(
        REQUEST_BODY_BYTES,
        RESPONSE_BODY_BYTES,
        EVENT_REQUEST_BYTES,
        RUNTIME_CONCURRENCY,
        CURSOR_LIFETIME,
        INPUT_TICKET_LIFETIME,
        structure,
        service_limits,
    )
    .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let application = product.application();
    let inputs = Arc::new(InputStager::new(
        product.paths().artifacts()?.clone(),
        runtime.runtime(),
        InputStagingLimits::try_new(MAXIMUM_STAGED_INPUTS, MAXIMUM_STAGED_INPUT_BYTES)
            .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    ));
    let mcp_limit_spec = McpLimitSpec::default();
    let desktop_registration = runtime.registration(NamedClient::Desktop)?;
    let mcp_control = runtime.take_mcp_clients()?.activate(
        runtime.runtime(),
        desktop_registration.client_id(),
        runtime.secret_store(),
        runtime.credentials(),
        MCP_CLIENT_REQUESTS,
        mcp_limit_spec,
    )?;
    let governance_persistence = GovernancePersistence::try_open(paths)
        .map_err(|_error| InstalledServiceError::GovernanceState)?;
    let governance_actions = governance_persistence
        .load_registrations()
        .map_err(|_error| InstalledServiceError::GovernanceState)?
        .map(|registrations| {
            let authority = GovernanceAuthority::try_load(
                runtime.secret_store(),
                registrations,
                governance_persistence.audit_sink(),
                GovernanceLimits::standard()
                    .map_err(|_error| InstalledServiceError::GovernanceState)?,
            )
            .map_err(|_error| InstalledServiceError::GovernanceState)?;
            GovernedActionService::try_new(
                Arc::new(authority),
                product.decision_governance(),
                product.fair_value_governance(),
                GovernedActionServiceLimits::standard()
                    .map_err(|_error| InstalledServiceError::GovernanceState)?,
            )
            .map(Arc::new)
            .map_err(|_error| InstalledServiceError::GovernanceState)
        })
        .transpose()?;
    let governance = Arc::new(InstalledGovernanceOperations::new(
        governance_actions,
        runtime.runtime(),
        desktop_registration.client_id(),
    ));
    let services = Arc::new(
        InstalledToolServices::try_new(
            Arc::clone(&application),
            product,
            jobs,
            runners,
            Arc::clone(&inputs),
        )
        .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    );
    let dispatcher = Arc::new(
        InstalledApplicationDispatcher::try_new(
            Arc::clone(&services),
            product,
            runtime.runtime(),
            runtime.endpoint(),
            Arc::clone(&mcp_control),
            governance,
        )
        .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    );
    let replay = Arc::new(
        MutationReplayGuard::try_new(
            ReplayLimits::try_new(REPLAY_CAPACITY)
                .map_err(|_error| InstalledServiceError::InvalidComposition)?,
        )
        .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    );
    let events = Arc::new(
        EventHub::try_new(
            runtime.runtime().service_generation(),
            EventHubLimits::try_new(RETAINED_EVENTS, MAXIMUM_EVENT_BYTES)
                .map_err(|_error| InstalledServiceError::InvalidComposition)?,
        )
        .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    );
    let native = RuntimeRouter::try_new(
        runtime.runtime(),
        runtime.endpoint(),
        ApplicationProtocolRange::single(ApplicationProtocolVersion::V1),
        OriginPolicy::try_new(TAURI_ORIGINS.map(str::to_owned))
            .map_err(|_error| InstalledServiceError::InvalidComposition)?,
        router_limits,
        runtime.credentials(),
        dispatcher,
        replay,
        events,
        inputs,
    )
    .map_err(|_error| InstalledServiceError::InvalidComposition)?;

    let audit = Arc::new(crate::mcp::audit::DurableAuditSink::try_new(
        paths.control_root()?.try_clone_directory()?,
    )?);
    let resources = Arc::new(InstalledResourceProvider::new(
        runtime.runtime(),
        Arc::clone(&application),
        jobs.repository(),
        product.artifact_authority(),
    ));
    let limits = McpLimits::try_from(mcp_limit_spec)
        .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let services: Arc<dyn ToolServices> = services;
    let audit_sink: Arc<dyn AuditSink> = audit.clone();
    let factory = McpHandlerFactory::try_new(
        services,
        limits,
        audit_sink,
        product.artifacts(),
        resources,
        runtime.runtime().workspace_id(),
    )
    .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let authenticator: Arc<dyn market_squawk_mcp::McpHttpAuthenticator> = mcp_control;
    let endpoint = runtime.endpoint().to_string();
    let request_cancellation = native.request_cancellation();
    let mcp = Arc::new(McpHttpService::new(
        factory,
        authenticator,
        HttpMcpConfig::try_new(
            [endpoint],
            TAURI_ORIGINS.map(str::to_owned),
            request_cancellation,
        )
        .map_err(|_error| InstalledServiceError::InvalidComposition)?,
    ));
    let mcp_router = Router::new()
        .route("/mcp", any(mcp_request))
        .with_state(mcp);
    let server = native
        .start(runtime.take_listener()?, Some(mcp_router))
        .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    let registration = runtime.registration(NamedClient::Desktop)?;
    let scope = ApplicationRequestScope::try_new(
        runtime.runtime(),
        registration.client_id(),
        registration.generation(),
        CorrelationId::try_from_uuid(Uuid::new_v4())?,
        structure,
        REQUEST_BODY_BYTES,
    )?;
    let readiness = LoopbackApplicationClient::try_new(
        runtime.record(),
        scope,
        runtime.read_credential(NamedClient::Desktop)?,
        None,
        RESPONSE_BODY_BYTES,
        structure,
        CLIENT_TIMEOUT,
    )?;
    Ok(ComposedTransport {
        audit,
        server,
        readiness,
    })
}

async fn mcp_request(
    State(service): State<Arc<McpHttpService>>,
    request: Request<axum::body::Body>,
) -> Response<axum::body::Body> {
    service.handle(request).await
}

async fn drain_failed_server(server: market_squawk_runtime::RuntimeServer) {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let _drain = server
        .run_until(cancellation, GRACEFUL_REQUEST_DRAIN, FORCED_REQUEST_DRAIN)
        .await;
}

async fn cleanup_startup(product: &LocalProduct, jobs: &InstalledJobAuthority) {
    if let Ok(at) = current_timestamp() {
        let _jobs = jobs.shutdown_authority(at, JOB_RUNNER_DRAIN).await;
    }
    shutdown_application(product.application()).await;
    let _repository = jobs.shutdown_repository().await;
}

async fn shutdown_application(application: Arc<crate::application::Application>) -> bool {
    application.begin_shutdown();
    let Some(deadline) = std::time::Instant::now().checked_add(application.shutdown_timeout())
    else {
        return false;
    };
    application.shutdown(deadline).await.is_complete()
}

fn runtime_secret_store(paths: &LocalPaths) -> Result<Arc<dyn SecretStore>, InstalledServiceError> {
    Ok(Arc::new(
        PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
            "market-squawk-runtime",
            paths.control_root()?.root().join(RUNTIME_SECRET_DIRECTORY),
        )
        .map_err(|_error| InstalledServiceError::SecretStore)?,
    ))
}

/// Terminal evidence for every installed-service shutdown barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstalledServiceShutdownReport {
    transport: bool,
    jobs_stopped: bool,
    application: bool,
    audit_flushed: bool,
    jobs_closed: bool,
    rendezvous_retired: bool,
}

impl InstalledServiceShutdownReport {
    /// True only when every required barrier completed.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.transport
            && self.jobs_stopped
            && self.application
            && self.audit_flushed
            && self.jobs_closed
            && self.rendezvous_retired
    }
}

/// Closed installed-service composition and lifecycle failure.
#[derive(Debug, Error)]
pub enum InstalledServiceError {
    /// Another process owns the installed-service instance authority.
    #[error("the installed Market Squawk service is already running")]
    AlreadyRunning,
    /// Persistent service identity is invalid or unsupported.
    #[error("installed-service identity state is invalid")]
    InvalidRuntimeState,
    /// The stable loopback endpoint cannot be reacquired.
    #[error("the installed-service endpoint is unavailable")]
    EndpointUnavailable,
    /// The prepared listener has already transferred to the runtime server.
    #[error("the installed-service listener was already transferred")]
    ListenerAlreadyTaken,
    /// Secure local state cannot be opened, recovered, or persisted.
    #[error("installed-service state is unavailable")]
    State(#[source] LocalAuthorityStateStoreError),
    /// The local secret authority rejected the requested operation.
    #[error("installed-service secret storage is unavailable")]
    SecretStore,
    /// Durable governance registrations, audit, or authority state could not be composed.
    #[error("installed-service governance state is unavailable")]
    GovernanceState,
    /// Operating-system entropy is unavailable.
    #[error("installed-service entropy is unavailable")]
    EntropyUnavailable,
    /// System time cannot produce a valid canonical timestamp.
    #[error("installed-service wall clock is unavailable")]
    ClockUnavailable,
    /// The operating system cannot provide a stable process discriminator.
    #[error("installed-service process identity is unavailable")]
    ProcessIdentityUnavailable,
    /// A local path capability could not be opened.
    #[error(transparent)]
    Path(#[from] PathError),
    /// A runtime identity or bounded protocol contract is invalid.
    #[error(transparent)]
    Runtime(#[from] RuntimeContractError),
    /// Client credential custody or authentication failed.
    #[error(transparent)]
    Credential(#[from] CredentialError),
    /// Rendezvous publication or retirement failed.
    #[error(transparent)]
    Rendezvous(#[from] RendezvousError),
    /// The owned loopback listener failed.
    #[error("installed-service listener operation failed")]
    Io(#[from] std::io::Error),
    /// Whole-product composition failed before publication.
    #[error(transparent)]
    Product(#[from] LocalProductError),
    /// Durable job authority composition or shutdown failed.
    #[error(transparent)]
    Jobs(#[from] crate::jobs::InstalledJobError),
    /// Durable MCP audit construction failed.
    #[error(transparent)]
    Audit(#[from] crate::mcp::LocalAuditError),
    /// Installed MCP client authority construction or recovery failed.
    #[error("installed MCP client authority is unavailable")]
    McpControl,
    /// A code-owned service limit or component contract was invalid.
    #[error("installed-service composition is invalid")]
    InvalidComposition,
    /// The authenticated exact-generation self-probe did not prove readiness.
    #[error("installed-service readiness verification failed")]
    ReadinessFailed,
    /// No authenticated current installed-service generation is available.
    #[error("the installed Market Squawk service is unavailable")]
    ServiceUnavailable,
    /// A private application client could not be constructed or used.
    #[error(transparent)]
    Client(#[from] ApplicationClientError),
    /// One or more bounded shutdown barriers did not complete.
    #[error("installed-service shutdown was incomplete: {0:?}")]
    ShutdownIncomplete(InstalledServiceShutdownReport),
}

impl InstalledServiceError {
    fn instance(error: LocalAuthorityStateStoreError) -> Self {
        if matches!(error, LocalAuthorityStateStoreError::AlreadyLocked) {
            Self::AlreadyRunning
        } else {
            Self::State(error)
        }
    }
}

impl From<LocalAuthorityStateStoreError> for InstalledServiceError {
    fn from(error: LocalAuthorityStateStoreError) -> Self {
        Self::State(error)
    }
}

impl From<mcp_control::McpControlError> for InstalledServiceError {
    fn from(_error: mcp_control::McpControlError) -> Self {
        Self::McpControl
    }
}
