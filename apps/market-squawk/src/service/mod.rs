//! Single per-user installed-service composition and lifecycle authority.

mod analysis;
mod backtest_preparation;
mod bootstrap;
mod decision;
mod dispatch;
mod forecast_preparation;
mod governance;
mod governance_persistence;
mod jobs;
mod lifecycle;
mod logging;
mod mcp_client;
mod mcp_control;
mod operations;
mod operations_activity;
mod operations_activity_bindings;
mod operations_bootstrap;
mod operations_composition;
mod portfolio_import;
mod ready_admission;
mod research_dataset;
mod resources;
mod runtime;
mod tool_services;
mod update_package;
mod workspace_recovery;
mod workspace_selector;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{Router, extract::State, http::Request, response::Response, routing::any};
use dispatch::{InstalledApplicationDispatcher, InstalledDispatcherComposition};
use market_squawk_installer::{PlatformError, default_installation_data_root};
use market_squawk_mcp::{
    AuditSink, HttpMcpConfig, McpHandlerFactory, McpHttpService, McpLimitSpec, McpLimits,
};
use market_squawk_platform::{
    EncryptedFileFallbackStatus, InstalledServiceSelectedWorkspaceGuard,
    LocalAuthorityStateStoreError, LocalPaths, LocalSecretStoreError, PathError,
    PreferredSecretStore, SecretCancellation, SecretInteractionPolicy, SecretOperationControl,
    SecretStore, SecretValue,
};
use market_squawk_runtime::{
    ApplicationClientError, ApplicationProtocolRange, ApplicationProtocolVersion,
    ApplicationRequestScope, CorrelationId, CredentialError, EventHub, EventHubLimits, InputStager,
    InputStagingLimits, InstallationId, LoopbackApplicationClient, MutationReplayGuard,
    NamedClient, OriginPolicy, RendezvousError, ReplayLimits, RuntimeContractError, RuntimeRouter,
    RuntimeRouterLimits,
};
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, RequestOrigin, ServiceLimits, ToolServices,
};
use resources::InstalledResourceProvider;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tool_services::{
    InstalledToolServiceAuthorities, InstalledToolServiceRuntime, InstalledToolServices,
};
use uuid::Uuid;

pub use bootstrap::{
    BootstrapRequirement, InstalledServiceBootstrapState, InstalledServiceBootstrapStatus,
};

use mcp_client::InstalledMcpRelayTransport;

use lifecycle::InstalledServiceLifecycle;
pub use lifecycle::InstalledServiceRunOutcome;
pub use logging::{InstalledServiceLogging, InstalledServiceLoggingError, TerminalLogFormat};
use operations_bootstrap::{PreparedInstalledOperations, ReadyInstalledOperations};

use crate::{AppConfig, LocalProduct, LocalProductError, jobs::InstalledJobAuthority};

use self::portfolio_import::InstalledPortfolioImportOperations;
use self::runtime::{PreparedRuntime, current_timestamp};
use self::workspace_selector::{WorkspacePlacement, WorkspaceSelector, WorkspaceSelectorError};
use self::{
    governance::{
        GovernedActionService, GovernedActionServiceLimits, InstalledGovernanceComposition,
        InstalledGovernanceOperations,
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
const EPHEMERAL_CREDENTIAL_CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const MCP_CLIENT_REQUESTS: usize = 4;
const TAURI_ORIGINS: [&str; 2] = ["tauri://localhost", "http://tauri.localhost"];

pub use runtime::SystemProcessIdentityVerifier;

/// One fresh, absolute, non-default installation root authorized for destructive verification
/// credential cleanup.
pub struct EphemeralVerificationRoot {
    root: PathBuf,
}

impl std::fmt::Debug for EphemeralVerificationRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EphemeralVerificationRoot([CONTROLLED PATH])")
    }
}

impl EphemeralVerificationRoot {
    /// Validates the destructive verification boundary before logging or service setup mutates it.
    pub fn try_new(root: impl AsRef<Path>) -> Result<Self, InstalledServiceError> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(InstalledServiceError::InvalidEphemeralVerificationRoot);
        }
        let selected = canonical_candidate(root)?;
        let default = canonical_candidate(&default_installation_data_root()?)?;
        if selected == default {
            return Err(InstalledServiceError::InvalidEphemeralVerificationRoot);
        }
        match std::fs::symlink_metadata(root) {
            Ok(metadata)
                if metadata.file_type().is_dir() && std::fs::read_dir(root)?.next().is_none() => {}
            Ok(_) => return Err(InstalledServiceError::InvalidEphemeralVerificationRoot),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Returns the already validated root for colocated verification-only logging.
    pub fn as_path(&self) -> &Path {
        &self.root
    }
}

/// Native-only connector for one named client of the already running installed service.
pub struct InstalledServiceConnector {
    paths: LocalPaths,
}

impl std::fmt::Debug for InstalledServiceConnector {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledServiceConnector")
            .field("paths", &"[LOCAL CAPABILITIES]")
            .finish()
    }
}

impl InstalledServiceConnector {
    /// Opens only native discovery capabilities; it never constructs product or secret domains.
    pub fn try_new(_config: &AppConfig) -> Result<Self, InstalledServiceError> {
        let paths = LocalPaths::prepare(default_installation_data_root()?)?;
        Ok(Self { paths })
    }

    /// Opens native discovery at an explicitly selected absolute installation authority root.
    pub fn try_new_at_installation_root(
        _config: &AppConfig,
        installation_root: impl AsRef<Path>,
    ) -> Result<Self, InstalledServiceError> {
        let paths = prepare_installation_paths(installation_root.as_ref())?;
        Ok(Self { paths })
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
        let admitted = ready_admission::request(&self.paths, client, timeout)?;
        runtime::connect_admitted_client(admitted, origin, structure, RESPONSE_BODY_BYTES, timeout)
    }

    /// Reads the current short-lived, secret-free credential-bootstrap state.
    pub async fn bootstrap_status(
        &self,
    ) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
        bootstrap::status(&self.paths).await
    }

    /// Consumes one explicit encrypted-fallback unlock over the owner-authenticated channel.
    pub async fn bootstrap_unlock(
        &self,
        unlock: SecretValue,
    ) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
        bootstrap::unlock(&self.paths, unlock).await
    }

    /// Retries unchanged preparation after a foreground platform-keyring interaction.
    pub async fn bootstrap_retry_after_foreground_keyring(
        &self,
    ) -> Result<InstalledServiceBootstrapStatus, InstalledServiceError> {
        bootstrap::retry_after_foreground_keyring(&self.paths).await
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
        let admitted = ready_admission::request(&self.paths, client, CLIENT_TIMEOUT)?;
        let transport = InstalledMcpRelayTransport::try_new(
            &admitted.record,
            admitted.credential,
            CLIENT_TIMEOUT,
        )
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
    admission: ready_admission::ReadyAdmission,
    lifecycle: Arc<InstalledServiceLifecycle>,
    installation_paths: LocalPaths,
    ephemeral_verification_credentials: bool,
    _workspace_selector: Arc<WorkspaceSelector>,
    _selected_workspace_guard: InstalledServiceSelectedWorkspaceGuard,
}

impl std::fmt::Debug for InstalledService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledService")
            .field("runtime", &self.runtime)
            .field("admission", &self.admission)
            .field("product", &"[LOCAL PRODUCT AUTHORITY]")
            .field("jobs", &self.jobs)
            .field("audit", &"[DURABLE AUDIT AUTHORITY]")
            .field("server", &self.server)
            .field("lifecycle", &self.lifecycle)
            .field(
                "ephemeral_verification_credentials",
                &self.ephemeral_verification_credentials,
            )
            .field("workspace_selector", &"[INSTALLATION-GLOBAL AUTHORITY]")
            .field("instance_guard", &"[INSTALLATION-GLOBAL AUTHORITY]")
            .finish_non_exhaustive()
    }
}

impl InstalledService {
    /// Composes every installed-product authority, proves the bound route is ready, then publishes
    /// the authenticated rendezvous as the final startup step.
    pub async fn start(config: AppConfig) -> Result<Self, InstalledServiceError> {
        let logs = logging::open_installation_log_store()?;
        Self::start_with_logging_store(config, logs).await
    }

    /// Composes the service at an explicitly selected absolute installation authority root.
    pub async fn start_at_installation_root(
        config: AppConfig,
        installation_root: impl AsRef<Path>,
    ) -> Result<Self, InstalledServiceError> {
        let installation_paths = prepare_installation_paths(installation_root.as_ref())?;
        let logs = logging::open_log_store(&installation_paths)?;
        Self::start_with_logging_store_at_prepared_root(config, installation_paths, logs).await
    }

    /// Composes the installed service over the process-owned structured-log store.
    pub async fn start_with_logging_store(
        config: AppConfig,
        logs: Arc<crate::application::logs::StructuredLogStore>,
    ) -> Result<Self, InstalledServiceError> {
        let installation_paths = LocalPaths::prepare(default_installation_data_root()?)?;
        Self::start_with_logging_store_at_prepared_root(config, installation_paths, logs).await
    }

    /// Composes the service with process-owned logging at an explicit installation root.
    pub async fn start_with_logging_store_at_installation_root(
        config: AppConfig,
        installation_root: impl AsRef<Path>,
        logs: Arc<crate::application::logs::StructuredLogStore>,
    ) -> Result<Self, InstalledServiceError> {
        let installation_paths = prepare_installation_paths(installation_root.as_ref())?;
        Self::start_with_logging_store_at_prepared_root(config, installation_paths, logs).await
    }

    async fn start_with_logging_store_at_prepared_root(
        config: AppConfig,
        installation_paths: LocalPaths,
        logs: Arc<crate::application::logs::StructuredLogStore>,
    ) -> Result<Self, InstalledServiceError> {
        let workspace_paths = LocalPaths::prepare(config.data_dir())?;
        let secret_store = runtime_secret_store(&installation_paths)?;
        Self::start_prepared(
            config,
            installation_paths,
            workspace_paths,
            secret_store,
            logs,
            false,
        )
        .await
    }

    /// Composes the installed service with an already-owned native secret capability.
    ///
    /// This is the foreground encrypted-fallback and deterministic verification boundary. The
    /// caller cannot replace any product authority, runtime identity, route, or lifecycle policy.
    pub async fn start_with_secret_store(
        config: AppConfig,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, InstalledServiceError> {
        let workspace_paths = LocalPaths::prepare(config.data_dir())?;
        let installation_paths =
            LocalPaths::prepare(deterministic_installation_root(&workspace_paths)?)?;
        let logs = logging::open_log_store(&installation_paths)?;
        Self::start_prepared(
            config,
            installation_paths,
            workspace_paths,
            secret_store,
            logs,
            false,
        )
        .await
    }

    /// Composes a verification-only service whose exact credential generations are retired and
    /// proven absent on every graceful shutdown or startup unwind.
    ///
    pub async fn start_ephemeral_verification_with_logging_store_at_installation_root(
        config: AppConfig,
        installation_root: EphemeralVerificationRoot,
        logs: Arc<crate::application::logs::StructuredLogStore>,
    ) -> Result<Self, InstalledServiceError> {
        let installation_paths = prepare_installation_paths(installation_root.as_path())?;
        let workspace_paths = LocalPaths::prepare(config.data_dir())?;
        let secret_store = runtime_secret_store(&installation_paths)?;
        Self::start_prepared(
            config,
            installation_paths,
            workspace_paths,
            secret_store,
            logs,
            true,
        )
        .await
    }

    async fn start_prepared(
        config: AppConfig,
        installation_paths: LocalPaths,
        legacy_workspace_paths: LocalPaths,
        secret_store: Arc<dyn SecretStore>,
        logs: Arc<crate::application::logs::StructuredLogStore>,
        ephemeral_verification_credentials: bool,
    ) -> Result<Self, InstalledServiceError> {
        let instance_guard = runtime::acquire_instance(&installation_paths)?;
        let installation_id = runtime::installation_id(&installation_paths)?
            .map_or_else(|| InstallationId::try_from_uuid(Uuid::new_v4()), Ok)?;
        let workspace_selector = Arc::new(
            WorkspaceSelector::try_open_or_bootstrap(&installation_paths, &legacy_workspace_paths)
                .map_err(map_workspace_selector_startup)?,
        );
        let selection = workspace_selector
            .startup_selection()
            .map_err(|_error| InstalledServiceError::WorkspaceSelection)?;
        let failed_startup_selector = Arc::clone(&workspace_selector);
        let failed_startup_selection = selection.clone();
        let cleanup_paths = installation_paths.clone();
        let cleanup_store = Arc::clone(&secret_store);
        let result = async move {
            let mut runtime = loop {
                match PreparedRuntime::prepare(
                    &installation_paths,
                    Arc::clone(&secret_store),
                    selection.identity(),
                    installation_id,
                )
                .await
                {
                    Ok(runtime) => break runtime,
                    Err(error) => {
                        let Some(requirement) =
                            recoverable_bootstrap_requirement(secret_store.as_ref(), &error)?
                        else {
                            return Err(error);
                        };
                        let _action = bootstrap::wait_for_action(
                            &installation_paths,
                            Arc::clone(&secret_store),
                            installation_id,
                            requirement,
                        )
                        .await?;
                    }
                }
            };
            let lifecycle = Arc::new(InstalledServiceLifecycle::new(runtime.runtime()));
            let selected_workspace_guard = selection
                .bind_instance_guard(instance_guard)
                .map_err(map_workspace_selector_startup)?;
            let workspace_paths = selected_workspace_guard.workspace_paths().clone();
            let product = LocalProduct::try_new_at_selected_workspace(
                config.clone(),
                &selected_workspace_guard,
            )?;
            let prepared_operations = PreparedInstalledOperations::prepare(
                &config,
                &installation_paths,
                &workspace_paths,
                selection.clone(),
                &product,
                Arc::clone(&lifecycle),
                Arc::clone(&workspace_selector),
                logs,
            )
            .map_err(|error| composition_stage(error, "operations preparation"))?;
            let runners = match crate::jobs::InstalledJobRunners::try_new(
                &product,
                prepared_operations.application_for_job_runners(),
            ) {
                Ok(runners) => Arc::new(runners),
                Err(error) => {
                    shutdown_application(product.application()).await;
                    return Err(error.into());
                }
            };
            let jobs = match InstalledJobAuthority::open(
                &workspace_paths,
                runners.registered(),
                current_timestamp()?,
            )
            .await
            {
                Ok(jobs) => jobs,
                Err(error) => {
                    shutdown_application(product.application()).await;
                    return Err(error.into());
                }
            };
            let operations = match prepared_operations.bind(&product, &jobs, &runners).await {
                Ok(operations) => operations,
                Err(error) => {
                    cleanup_startup(&product, &jobs).await;
                    return Err(composition_stage(error, "operations binding"));
                }
            };
            if let Err(error) = operations.reconcile_settings_startup() {
                cleanup_startup(&product, &jobs).await;
                return Err(composition_stage(error, "settings startup reconciliation"));
            }
            let ComposedTransport {
                audit,
                server,
                readiness,
                mcp_control,
            } = match compose_transport(TransportComposition {
                paths: &workspace_paths,
                runtime: &mut runtime,
                product: &product,
                jobs: &jobs,
                runners,
                operations: &operations,
                workspace_selector: Arc::clone(&workspace_selector),
                workspace_placement: selection.placement(),
            }) {
                Ok(composed) => composed,
                Err(error) => {
                    cleanup_startup(&product, &jobs).await;
                    return Err(error);
                }
            };
            let native_readiness = readiness.probe_ready(CancellationToken::new()).await;
            if native_readiness.is_err() || server.is_finished() {
                drain_failed_server(server).await;
                cleanup_startup(&product, &jobs).await;
                return Err(InstalledServiceError::ReadinessFailed);
            }
            if operations
                .recovery_bridge()
                .finalize_startup(&selection)
                .is_err()
            {
                drain_failed_server(server).await;
                cleanup_startup(&product, &jobs).await;
                return Err(InstalledServiceError::WorkspaceSelection);
            }
            let (desktop_credential, cli_credential) = runtime.admission_credentials()?;
            let mut admission = match ready_admission::ReadyAdmission::start(
                &installation_paths,
                runtime.record().clone(),
                runtime.registration(NamedClient::Desktop)?,
                runtime.registration(NamedClient::Cli)?,
                desktop_credential,
                cli_credential,
                mcp_control,
            ) {
                Ok(admission) => admission,
                Err(error) => {
                    drain_failed_server(server).await;
                    cleanup_startup(&product, &jobs).await;
                    return Err(error);
                }
            };
            let admission_readiness = admission.probe().await;
            if admission_readiness.is_err() {
                let _retired = admission.shutdown().await;
                drain_failed_server(server).await;
                cleanup_startup(&product, &jobs).await;
                return Err(InstalledServiceError::ReadinessFailed);
            }
            if let Err(error) = runtime.publish() {
                let _retired = admission.shutdown().await;
                drain_failed_server(server).await;
                cleanup_startup(&product, &jobs).await;
                return Err(error);
            }
            if let Err(error) = admission.publish() {
                let _rendezvous = runtime.retire();
                let _retired = admission.shutdown().await;
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
                admission,
                lifecycle,
                installation_paths,
                ephemeral_verification_credentials,
                _workspace_selector: workspace_selector,
                _selected_workspace_guard: selected_workspace_guard,
            })
        }
        .await;
        let credential_cleanup = if ephemeral_verification_credentials && result.is_err() {
            retire_and_verify_ephemeral_credentials(&cleanup_paths, cleanup_store.as_ref())
        } else {
            Ok(())
        };
        if result.is_err()
            && recover_failed_workspace_startup(
                failed_startup_selector.as_ref(),
                &failed_startup_selection,
            )
            .is_err()
        {
            return Err(InstalledServiceError::WorkspaceSelection);
        }
        match (result, credential_cleanup) {
            (Ok(service), Ok(())) => Ok(service),
            (Err(error), Ok(())) => Err(error),
            (Ok(_service), Err(_cleanup_error)) => {
                Err(InstalledServiceError::EphemeralCredentialCleanup)
            }
            (Err(startup), Err(_cleanup_error)) => {
                Err(InstalledServiceError::EphemeralStartupCleanup {
                    startup: Box::new(startup),
                })
            }
        }
    }

    /// Serves until cancellation, then executes every shutdown phase in authority order.
    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<InstalledServiceRunOutcome, InstalledServiceError> {
        let Self {
            server,
            product,
            jobs,
            audit,
            runtime,
            mut admission,
            lifecycle,
            installation_paths,
            ephemeral_verification_credentials,
            _workspace_selector,
            _selected_workspace_guard,
        } = self;
        let transport_cancellation = CancellationToken::new();
        let mut serving = Box::pin(server.run_until(
            transport_cancellation.clone(),
            GRACEFUL_REQUEST_DRAIN,
            FORCED_REQUEST_DRAIN,
        ));
        let (
            expected_next,
            transport_stopped_unexpectedly,
            admission_stopped_unexpectedly,
            completed_transport,
        ) = tokio::select! {
            biased;
            expected_next = lifecycle.wait_for_restart() => {
                (Some(expected_next), false, false, None)
            }
            () = cancellation.cancelled() => {
                (None, false, false, None)
            }
            result = &mut serving => {
                (None, true, false, Some(result.is_ok()))
            }
            () = admission.failed() => {
                (None, false, true, None)
            }
        };
        let admission_retired = admission.shutdown().await;
        transport_cancellation.cancel();
        let transport = match completed_transport {
            Some(transport) => transport,
            None => (&mut serving).await.is_ok(),
        };
        drop(serving);
        let jobs_stopped = if let Ok(at) = current_timestamp() {
            jobs.shutdown_authority(at, JOB_RUNNER_DRAIN).await.is_ok()
        } else {
            false
        };
        let application = shutdown_application(product.application()).await;
        let audit_flushed = audit.flush().is_ok();
        let jobs_closed = jobs.shutdown_repository().await.is_ok();
        let rendezvous_retired = runtime.retire().unwrap_or(false);
        let credential_cleanup = if ephemeral_verification_credentials {
            let secret_store = runtime.secret_store();
            retire_and_verify_ephemeral_credentials(&installation_paths, secret_store.as_ref())
        } else {
            Ok(())
        };
        let report = InstalledServiceShutdownReport {
            transport,
            admission_retired,
            jobs_stopped,
            application,
            audit_flushed,
            jobs_closed,
            rendezvous_retired,
        };
        drop(runtime);
        drop(admission);
        drop(audit);
        drop(jobs);
        drop(product);
        drop(lifecycle);
        drop(installation_paths);
        drop(_workspace_selector);
        drop(_selected_workspace_guard);
        if credential_cleanup.is_err() {
            Err(InstalledServiceError::EphemeralCredentialCleanup)
        } else if report.is_complete() && admission_stopped_unexpectedly {
            Err(InstalledServiceError::AdmissionStopped)
        } else if report.is_complete() && transport_stopped_unexpectedly {
            Err(InstalledServiceError::TransportStopped)
        } else if report.is_complete() {
            Ok(
                expected_next.map_or(InstalledServiceRunOutcome::Stopped, |expected_next| {
                    InstalledServiceRunOutcome::RestartRequested { expected_next }
                }),
            )
        } else {
            Err(InstalledServiceError::ShutdownIncomplete(report))
        }
    }
}

fn composition_stage(error: InstalledServiceError, stage: &'static str) -> InstalledServiceError {
    if matches!(error, InstalledServiceError::InvalidComposition) {
        InstalledServiceError::CompositionStage(stage)
    } else {
        error
    }
}

fn map_workspace_selector_startup(error: WorkspaceSelectorError) -> InstalledServiceError {
    if matches!(
        error,
        WorkspaceSelectorError::Persistence(LocalAuthorityStateStoreError::AlreadyLocked)
    ) {
        InstalledServiceError::AlreadyRunning
    } else {
        InstalledServiceError::WorkspaceSelection
    }
}

fn recover_failed_workspace_startup(
    selector: &WorkspaceSelector,
    selection: &workspace_selector::WorkspaceStartupSelection,
) -> Result<(), InstalledServiceError> {
    let Some(handoff) = selection.handoff() else {
        return Ok(());
    };
    match handoff.phase() {
        workspace_selector::WorkspaceHandoffPhase::Activate => selector
            .stage_startup_rollback(handoff.handoff_id(), selection.identity())
            .map(|_rollback| ()),
        workspace_selector::WorkspaceHandoffPhase::Rollback => {
            selector.mark_rollback_failed(handoff.handoff_id(), selection.identity())
        }
    }
    .map_err(|_error| InstalledServiceError::WorkspaceSelection)
}

struct ComposedTransport {
    audit: Arc<crate::mcp::audit::DurableAuditSink>,
    server: market_squawk_runtime::RuntimeServer,
    readiness: LoopbackApplicationClient,
    mcp_control: Arc<mcp_control::InstalledMcpControl>,
}

struct TransportComposition<'a> {
    paths: &'a LocalPaths,
    runtime: &'a mut PreparedRuntime,
    product: &'a LocalProduct,
    jobs: &'a InstalledJobAuthority,
    runners: Arc<crate::jobs::InstalledJobRunners>,
    operations: &'a ReadyInstalledOperations,
    workspace_selector: Arc<WorkspaceSelector>,
    workspace_placement: WorkspacePlacement,
}

fn compose_transport(
    composition: TransportComposition<'_>,
) -> Result<ComposedTransport, InstalledServiceError> {
    let TransportComposition {
        paths,
        runtime,
        product,
        jobs,
        runners,
        operations,
        workspace_selector,
        workspace_placement,
    } = composition;
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
    let mcp_activation_authority = runtime.mcp_client_activation_authority()?;
    let mcp_control = runtime.take_mcp_clients()?.activate(
        mcp_activation_authority,
        MCP_CLIENT_REQUESTS,
        mcp_limit_spec,
    )?;
    let governance_persistence = Arc::new(
        GovernancePersistence::try_open(paths)
            .map_err(|_error| InstalledServiceError::GovernanceState)?,
    );
    governance_persistence
        .recover_pending(runtime.secret_store().as_ref())
        .map_err(|_error| InstalledServiceError::GovernanceState)?;
    let governance_limits =
        GovernanceLimits::standard().map_err(|_error| InstalledServiceError::GovernanceState)?;
    let governed_action_limits = GovernedActionServiceLimits::standard()
        .map_err(|_error| InstalledServiceError::GovernanceState)?;
    let decision_governance = product.decision_governance();
    let fair_value_governance = product.fair_value_governance();
    let governance_actions = governance_persistence
        .load_registrations()
        .map_err(|_error| InstalledServiceError::GovernanceState)?
        .map(|registrations| {
            let authority = GovernanceAuthority::try_load(
                runtime.secret_store(),
                registrations,
                governance_persistence.audit_sink(),
                governance_limits,
            )
            .map_err(|_error| InstalledServiceError::GovernanceState)?;
            GovernedActionService::try_new(
                Arc::new(authority),
                Arc::clone(&decision_governance),
                Arc::clone(&fair_value_governance),
                governed_action_limits,
            )
            .map(Arc::new)
            .map_err(|_error| InstalledServiceError::GovernanceState)
        })
        .transpose()?;
    let governance = Arc::new(InstalledGovernanceOperations::new(
        InstalledGovernanceComposition {
            actions: governance_actions,
            persistence: Arc::clone(&governance_persistence),
            secrets: runtime.secret_store(),
            decisions: decision_governance,
            fair_value: fair_value_governance,
            authority_limits: governance_limits,
            action_limits: governed_action_limits,
        },
        runtime.runtime(),
        desktop_registration.client_id(),
    ));
    let portfolio_import = InstalledPortfolioImportOperations::try_new(
        product.paths(),
        product.portfolio(),
        Arc::clone(&inputs),
        runtime.runtime(),
    )
    .map_err(|error| InstalledServiceError::PortfolioImportState(error.to_string()))?;
    let services = Arc::new(
        InstalledToolServices::try_new(
            InstalledToolServiceAuthorities::new(
                Arc::clone(&application),
                operations.application(),
                product,
                jobs,
            ),
            InstalledToolServiceRuntime::new(
                runners,
                Arc::clone(&inputs),
                runtime.runtime(),
                portfolio_import,
            ),
        )
        .map_err(|_error| InstalledServiceError::CompositionStage("installed tool services"))?,
    );
    let recovery_deadline = Instant::now()
        .checked_add(CLIENT_TIMEOUT)
        .ok_or(InstalledServiceError::InvalidComposition)?;
    let recovery_origin = RequestOrigin::try_new(
        runtime.runtime().workspace_id().as_uuid(),
        desktop_registration.client_id().as_uuid(),
    )
    .map_err(|_error| InstalledServiceError::InvalidComposition)?;
    services
        .recover_promoting_portfolio_imports(
            &RequestContext::new(
                RequestId::try_string("startup-portfolio-import-recovery")
                    .map_err(|_error| InstalledServiceError::InvalidComposition)?,
                CancellationToken::new(),
                recovery_deadline,
                service_limits,
            )
            .with_origin(recovery_origin),
        )
        .map_err(|error| InstalledServiceError::PortfolioImportState(error.to_string()))?;
    let dispatcher = Arc::new(
        InstalledApplicationDispatcher::try_new(
            InstalledDispatcherComposition {
                services: Arc::clone(&services),
                runtime: runtime.runtime(),
                workspace_generation: operations
                    .workspaces()
                    .active()
                    .map_err(|_error| InstalledServiceError::InvalidComposition)?
                    .generation()
                    .get(),
                workspace_placement: match workspace_placement {
                    WorkspacePlacement::Managed => "managed",
                    WorkspacePlacement::LegacyMigrationRequired => "legacy_migration_required",
                },
                endpoint: runtime.endpoint(),
                mcp: Arc::clone(&mcp_control),
                governance,
                settings: operations.settings_operations(),
            },
            product,
        )
        .map_err(|_error| InstalledServiceError::CompositionStage("application dispatcher"))?,
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
    .map_err(|_error| InstalledServiceError::CompositionStage("runtime router"))?;
    let activity_readers = operations_activity_bindings::build_runtime_activity_readers(
        jobs,
        product.source_lifecycle_authority(),
        product.paper_runtime_activity_authority(),
        native.client_activity_reader(),
        Arc::clone(&mcp_control),
        operations.workspaces(),
        workspace_selector,
    );
    operations
        .activity()
        .bind(activity_readers)
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
    .map_err(|_error| InstalledServiceError::CompositionStage("MCP handler factory"))?;
    let authenticator: Arc<dyn market_squawk_mcp::McpHttpAuthenticator> = mcp_control.clone();
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
        .map_err(|_error| InstalledServiceError::CompositionStage("runtime server"))?;
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
        mcp_control,
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
    let namespace = runtime_secret_namespace(paths)?;
    Ok(Arc::new(
        PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
            &namespace,
            paths.control_root()?.root().join(RUNTIME_SECRET_DIRECTORY),
        )
        .map_err(|_error| InstalledServiceError::SecretStore)?,
    ))
}

fn canonical_candidate(path: &Path) -> Result<PathBuf, InstalledServiceError> {
    match std::fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or(InstalledServiceError::InvalidEphemeralVerificationRoot)?;
            let name = path
                .file_name()
                .ok_or(InstalledServiceError::InvalidEphemeralVerificationRoot)?;
            Ok(std::fs::canonicalize(parent)?.join(name))
        }
        Err(error) => Err(error.into()),
    }
}

fn retire_and_verify_ephemeral_credentials(
    paths: &LocalPaths,
    secret_store: &dyn SecretStore,
) -> Result<(), InstalledServiceError> {
    let mut failed = false;
    let mut references = match runtime::credential_references(paths) {
        Ok(references) => references,
        Err(_error) => {
            failed = true;
            Vec::new()
        }
    };
    match mcp_control::credential_references(paths) {
        Ok(mut mcp_references) => references.append(&mut mcp_references),
        Err(_error) => failed = true,
    }
    references.sort();
    references.dedup();

    for reference in &references {
        let control = ephemeral_credential_control()?;
        match secret_store.delete(reference, &control) {
            Ok(()) | Err(LocalSecretStoreError::NotFound) => {}
            Err(_error) => failed = true,
        }
    }
    for reference in &references {
        let control = ephemeral_credential_control()?;
        match secret_store.read(reference, &control) {
            Err(LocalSecretStoreError::NotFound) => {}
            Ok(secret) => {
                drop(secret);
                failed = true;
            }
            Err(_error) => failed = true,
        }
    }
    if failed {
        Err(InstalledServiceError::SecretStore)
    } else {
        Ok(())
    }
}

fn ephemeral_credential_control() -> Result<SecretOperationControl, InstalledServiceError> {
    let deadline = Instant::now()
        .checked_add(EPHEMERAL_CREDENTIAL_CLEANUP_TIMEOUT)
        .ok_or(InstalledServiceError::SecretStore)?;
    SecretOperationControl::try_new(
        "installed-verification-credential-cleanup",
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(|_error| InstalledServiceError::SecretStore)
}

fn runtime_secret_namespace(paths: &LocalPaths) -> Result<String, InstalledServiceError> {
    use sha2::{Digest as _, Sha256};

    let selected = std::fs::canonicalize(paths.root())?;
    let default = default_installation_data_root()
        .ok()
        .and_then(|root| std::fs::canonicalize(root).ok());
    if default.as_ref() == Some(&selected) {
        return Ok("market-squawk-runtime".to_owned());
    }
    let digest = Sha256::digest(selected.as_os_str().as_encoded_bytes());
    let mut namespace = String::from("market-squawk-runtime-v1-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &digest[..16] {
        namespace.push(char::from(HEX[usize::from(byte >> 4)]));
        namespace.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(namespace)
}

fn prepare_installation_paths(root: &Path) -> Result<LocalPaths, InstalledServiceError> {
    if !root.is_absolute() {
        return Err(InstalledServiceError::InvalidInstallationRoot);
    }
    LocalPaths::prepare(root).map_err(Into::into)
}

fn recoverable_bootstrap_requirement(
    secret_store: &dyn SecretStore,
    error: &InstalledServiceError,
) -> Result<Option<BootstrapRequirement>, InstalledServiceError> {
    let credential_condition = matches!(
        error,
        InstalledServiceError::SecretStore
            | InstalledServiceError::Credential(CredentialError::SecretStore)
    );
    if !credential_condition {
        return Ok(None);
    }
    match secret_store
        .encrypted_file_fallback_status()
        .map_err(|_error| InstalledServiceError::SecretStore)?
    {
        EncryptedFileFallbackStatus::Locked => {
            Ok(Some(BootstrapRequirement::EncryptedFallbackLocked))
        }
        EncryptedFileFallbackStatus::Disabled | EncryptedFileFallbackStatus::Ready => Ok(None),
    }
}

fn deterministic_installation_root(
    workspace_paths: &LocalPaths,
) -> Result<PathBuf, InstalledServiceError> {
    workspace_paths
        .root()
        .parent()
        .map(|parent| parent.join(".market-squawk-installed-service"))
        .ok_or(InstalledServiceError::InvalidComposition)
}

/// Terminal evidence for every installed-service shutdown barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstalledServiceShutdownReport {
    transport: bool,
    admission_retired: bool,
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
            && self.admission_retired
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
    /// An explicitly selected installation authority root was not absolute.
    #[error("the installed-service authority root must be absolute")]
    InvalidInstallationRoot,
    /// A destructive verification root was not fresh, absolute, and distinct from the live root.
    #[error("ephemeral verification requires a fresh, absolute, non-default installation root")]
    InvalidEphemeralVerificationRoot,
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
    /// Durable governed portfolio-import approval or recovery state is unavailable.
    #[error("installed-service portfolio import state is unavailable: {0}")]
    PortfolioImportState(String),
    /// A concrete backup, recovery, or update authority rejected startup state.
    #[error("installed-service operations {stage} is unavailable")]
    OperationsAuthority {
        /// Exact closed construction or recovery stage.
        stage: &'static str,
        /// Closed service-level cause.
        #[source]
        source: market_squawk_services::ServiceError,
    },
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
    /// The operating system did not expose the per-user installation-data location.
    #[error(transparent)]
    Platform(#[from] PlatformError),
    /// Durable active-workspace selection or generation fencing failed.
    #[error("installed-service workspace selection is unavailable")]
    WorkspaceSelection,
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
    #[error("installed MCP client authority is unavailable: {0}")]
    McpControl(&'static str),
    /// A code-owned service limit or component contract was invalid.
    #[error("installed-service composition is invalid")]
    InvalidComposition,
    /// One named installed-service composition stage rejected its closed configuration.
    #[error("installed-service {0} composition is invalid")]
    CompositionStage(&'static str),
    /// The authenticated exact-generation self-probe did not prove readiness.
    #[error("installed-service readiness verification failed")]
    ReadinessFailed,
    /// The private runtime server stopped without an admitted lifecycle signal.
    #[error("installed-service transport stopped unexpectedly")]
    TransportStopped,
    /// The service-lifetime ready admission broker stopped without an admitted lifecycle signal.
    #[error("installed-service ready admission stopped unexpectedly")]
    AdmissionStopped,
    /// No authenticated current installed-service generation is available.
    #[error("the installed Market Squawk service is unavailable")]
    ServiceUnavailable,
    /// The short-lived bootstrap endpoint or its owner boundary is unavailable.
    #[error("installed-service bootstrap channel is unavailable")]
    BootstrapUnavailable,
    /// A bootstrap request violated the fixed binary protocol or exact binding.
    #[error("installed-service bootstrap request is invalid")]
    BootstrapProtocol,
    /// A bootstrap request or the total bootstrap lifetime elapsed.
    #[error("installed-service bootstrap deadline elapsed")]
    BootstrapDeadline,
    /// Owner-authenticated bootstrap input was rejected without exposing secret detail.
    #[error("installed-service bootstrap request was rejected")]
    BootstrapRejected,
    /// The ready-state native admission endpoint or its owner boundary is unavailable.
    #[error("installed-service ready admission is unavailable")]
    AdmissionUnavailable,
    /// A ready-state admission request violated the fixed exact-generation protocol.
    #[error("installed-service ready admission request is invalid")]
    AdmissionProtocol,
    /// A ready-state admission request elapsed its bounded monotonic deadline.
    #[error("installed-service ready admission deadline elapsed")]
    AdmissionDeadline,
    /// Owner-authenticated ready-state admission was rejected without secret detail.
    #[error("installed-service ready admission was rejected")]
    AdmissionRejected,
    /// A private application client could not be constructed or used.
    #[error(transparent)]
    Client(#[from] ApplicationClientError),
    /// One or more bounded shutdown barriers did not complete.
    #[error("installed-service shutdown was incomplete: {0:?}")]
    ShutdownIncomplete(InstalledServiceShutdownReport),
    /// A verification-only service could not retire and prove absence of every credential.
    #[error("ephemeral verification credential cleanup was incomplete")]
    EphemeralCredentialCleanup,
    /// Startup failed and cleanup of credentials created before that failure was also incomplete.
    #[error("ephemeral verification credential cleanup failed after service startup failed")]
    EphemeralStartupCleanup {
        /// The primary startup failure retained for diagnosis and audit.
        #[source]
        startup: Box<InstalledServiceError>,
    },
    /// Structured-log storage could not be opened for service composition.
    #[error(transparent)]
    Logging(#[from] InstalledServiceLoggingError),
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
    fn from(error: mcp_control::McpControlError) -> Self {
        use mcp_control::McpControlError;
        let reason = match error {
            McpControlError::Path(_) => "controlled-path-unavailable",
            McpControlError::AuthorityStore(_) => "authority-store-unavailable",
            McpControlError::Credential(_) => "credential-registry-unavailable",
            McpControlError::HttpAuthentication(_) => "authenticator-invalid",
            McpControlError::InvalidState => "authority-state-invalid",
            McpControlError::InvalidRequest => "request-invalid",
            McpControlError::Unauthorized => "request-unauthorized",
            McpControlError::Interrupted => "request-interrupted",
            McpControlError::RecoveryPending => "credential-recovery-pending",
            McpControlError::SecretStore => "secret-store-unavailable",
            McpControlError::Clock => "system-clock-invalid",
        };
        Self::McpControl(reason)
    }
}
