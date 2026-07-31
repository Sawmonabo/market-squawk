//! Least-privilege Tauri bridge over the existing local application authorities.

use std::sync::OnceLock;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk::{
    AppConfig, LocalProduct, OnboardingNextAction, OnboardingSessionView, ProviderOnboardingError,
    ProviderPortalActivationAuthority, ProviderPortalActivationError, StartOnboardingRequest,
};
use market_squawk_data::CatalogLimit;
use market_squawk_installer::{
    CommandError, InstallError, InstallStatus, RepairRequest, RollbackRequest, repair, rollback,
    status as installation_status, update_from_channel,
};
use market_squawk_installer::{ProgramName, active_program_path};
#[cfg(not(target_os = "windows"))]
use market_squawk_installer::{UninstallRequest, uninstall};
use market_squawk_platform::SecretValue;
use market_squawk_services::{
    JsonStructureLimits, RequestContext, RequestId, ServiceError, ServiceLimits,
    validate_json_contract,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tauri::State;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::contracts::{
    ApplicationInvocation, DesktopBootstrap, DesktopCommandError, InstallationControlCommand,
    McpClientInstruction, OperationSummary, ProviderOnboardingCommand, Readiness, ReadinessState,
    SetupStep, SetupStepAction, SetupStepState,
};

const MAXIMUM_APPLICATION_ARGUMENT_BYTES: usize = 256 * 1024;
const MAXIMUM_DESKTOP_RESULT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_DESKTOP_RESULT_ITEMS: u64 = 1_000;
const MAXIMUM_OPERATION_BYTES: usize = 128;
const MAXIMUM_PROVIDER_SESSIONS: usize = 32;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_SETUP_OPERATION: &str = "Source.Setup";
const LIVE_MARKET_SETUP_SURFACES: [&str; 3] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
];
const RESEARCH_SETUP_OPERATIONS: [&str; 5] = [
    "Research.ListDatasets",
    "Research.GetManifest",
    "Research.GetHistory",
    "Research.GetAlternativeData",
    "Research.IngestSource",
];
const PORTFOLIO_SETUP_OPERATIONS: [&str; 6] = [
    "Portfolio.Import",
    "Portfolio.GetHoldings",
    "Portfolio.GetTransactions",
    "Portfolio.GetPerformance",
    "Portfolio.GetExposure",
    "Portfolio.GetRisk",
];
const PAPER_SETUP_OPERATIONS: [&str; 8] = [
    "Bot.GetStatus",
    "Bot.Start",
    "Bot.Stop",
    "Execution.GetOrders",
    "Execution.GetFills",
    "Execution.Cancel",
    "Execution.Reconcile",
    "Risk.TriggerKillSwitch",
];

#[derive(Clone, Copy, Debug)]
enum InvocationAuthority {
    ReadOnly,
    ExactConfirmed(&'static str),
}

pub(crate) struct DesktopState {
    product: LocalProduct,
    config: AppConfig,
    config_path: Option<PathBuf>,
    installation_root: PathBuf,
    portal_activation: Arc<dyn ProviderPortalActivationAuthority>,
    cancellation: CancellationToken,
    restart_program: OnceLock<PathBuf>,
}

impl DesktopState {
    pub(crate) fn new(
        product: LocalProduct,
        config: AppConfig,
        config_path: Option<PathBuf>,
        installation_root: PathBuf,
    ) -> Self {
        let portal_activation = product.provider_portal_activation();
        Self {
            product,
            config,
            config_path,
            installation_root,
            portal_activation,
            cancellation: CancellationToken::new(),
            restart_program: OnceLock::new(),
        }
    }

    fn schedule_restart(&self, program: PathBuf) -> Result<(), DesktopCommandError> {
        self.restart_program.set(program).map_err(|_| {
            DesktopCommandError::new(
                "installation_restart_pending",
                "Market Squawk is already restarting into the selected release.",
            )
        })
    }

    pub(crate) fn scheduled_restart_program(&self) -> Option<PathBuf> {
        self.restart_program.get().cloned()
    }

    fn bootstrap(&self) -> Result<DesktopBootstrap, DesktopCommandError> {
        let onboarding = self.product.provider_onboarding();
        let session_limit = CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)
            .map_err(|_error| DesktopCommandError::internal())?;
        let profiles = serialize(onboarding.profiles())?;
        let session_views = onboarding
            .current_sessions(session_limit)
            .map_err(map_onboarding_error)?;
        let sessions = serialize(&session_views)?;
        let fallback = serialize(
            onboarding
                .encrypted_file_fallback_status()
                .map_err(map_onboarding_error)?,
        )?;
        let capabilities = self.product.application().capabilities();
        let research_service_available = RESEARCH_SETUP_OPERATIONS
            .iter()
            .all(|operation| capabilities.find(operation).is_some());
        let portfolio_service_available = PORTFOLIO_SETUP_OPERATIONS
            .iter()
            .all(|operation| capabilities.find(operation).is_some());
        let paper_service_available = PAPER_SETUP_OPERATIONS
            .iter()
            .all(|operation| capabilities.find(operation).is_some());
        let operations = capabilities
            .tools()
            .iter()
            .map(|tool| {
                let contract = tool.contract();
                let effects = tool.effects();
                OperationSummary::new(
                    tool.name().to_owned(),
                    tool.description().to_owned(),
                    contract.domain().as_str(),
                    contract.authorization().as_str(),
                    effects.read_only(),
                    effects.destructive(),
                    Value::Object(tool.input_schema().clone()),
                )
            })
            .collect();
        let model_runtime = if self.product.model_runtime().is_some() {
            Readiness::new(
                ReadinessState::Ready,
                "Verified",
                "The configured local training release and native inference runtime were admitted.",
            )
        } else {
            Readiness::new(
                ReadinessState::NotConfigured,
                "Not configured",
                "No verified local training release is configured for this workspace.",
            )
        };
        let mcp_client = self.mcp_client_instruction();
        let mcp_available = mcp_client.is_some();
        let setup_steps = setup_steps(
            &session_views,
            research_service_available,
            portfolio_service_available,
            paper_service_available,
            mcp_available,
        );
        let mcp = if mcp_available {
            Readiness::new(
                ReadinessState::Available,
                "Available",
                "The packaged CLI path and bounded local stdio MCP tool contract were verified. The service is not running.",
            )
        } else {
            Readiness::new(
                ReadinessState::Unverified,
                "Unavailable",
                "A complete local MCP client instruction could not be generated from verified installed state.",
            )
        };
        let installation = installation_status(&self.installation_root)
            .map_err(|_error| DesktopCommandError::internal())?;
        let installation = if installation.is_installed() && installation.is_healthy() {
            Readiness::new(
                ReadinessState::Ready,
                "Verified",
                "The complete installed release and every retained component passed verification.",
            )
        } else if installation.is_installed() {
            Readiness::new(
                ReadinessState::Unverified,
                "Repair required",
                "The installed release failed component verification and cannot be treated as ready.",
            )
        } else {
            Readiness::new(
                ReadinessState::NotConfigured,
                "Not installed",
                "No complete versioned Market Squawk release is active for this user.",
            )
        };
        Ok(DesktopBootstrap::new(
            env!("CARGO_PKG_VERSION"),
            if cfg!(debug_assertions) {
                "development"
            } else {
                "release"
            },
            self.product.paths().root().display().to_string(),
            Readiness::new(
                ReadinessState::Ready,
                "Ready",
                "The controlled local workspace and catalogs opened successfully.",
            ),
            installation,
            model_runtime,
            mcp,
            mcp_client,
            fallback,
            profiles,
            sessions,
            setup_steps,
            operations,
        ))
    }

    fn mcp_client_instruction(&self) -> Option<McpClientInstruction> {
        let cli_program = self.product.verified_local_mcp_program().ok()?;
        let appimage_launcher = crate::appimage_mcp_launcher(&cli_program).ok()?;
        let (program, appimage_dispatch) = match appimage_launcher {
            Some(launcher) => (launcher.program, true),
            None => (cli_program, false),
        };
        let mut arguments = Vec::with_capacity(9);
        if appimage_dispatch {
            arguments.push("--stdio-mcp".to_owned());
        }
        if let Some(config_path) = self.config_path.as_deref() {
            arguments.push("--config".to_owned());
            arguments.push(path_text(config_path)?);
        }
        arguments.push("--data-dir".to_owned());
        arguments.push(path_text(self.product.paths().root())?);
        if let Some(training_release_root) = self.config.training_release_root() {
            arguments.push("--training-release-root".to_owned());
            arguments.push(path_text(training_release_root)?);
        }
        if !appimage_dispatch {
            arguments.push("mcp".to_owned());
            arguments.push("serve".to_owned());
        }

        Some(McpClientInstruction::new(
            path_text(&program)?,
            arguments,
            BTreeMap::new(),
        ))
    }

    pub(crate) fn begin_shutdown(&self) {
        self.cancellation.cancel();
        self.product.application().begin_shutdown();
    }

    pub(crate) async fn finish_shutdown(&self) {
        self.begin_shutdown();
        let application = self.product.application();
        let Some(deadline) = Instant::now().checked_add(application.shutdown_timeout()) else {
            return;
        };
        let _report = application.shutdown(deadline).await;
    }
}

fn path_text(path: &Path) -> Option<String> {
    path.to_str().map(str::to_owned)
}

fn setup_steps(
    sessions: &[OnboardingSessionView],
    research_service_available: bool,
    portfolio_service_available: bool,
    paper_service_available: bool,
    mcp_available: bool,
) -> Vec<SetupStep> {
    let sources_ready = sessions.iter().any(|session| {
        LIVE_MARKET_SETUP_SURFACES.contains(&session.surface_id())
            && session.next_action() == OnboardingNextAction::Active
    });
    let files_imported = has_active_session(sessions, "local.files");
    let portfolio_imported = has_active_session(sessions, "local.portfolio-imports");
    let research_ready = research_service_available;
    let portfolio_ready = portfolio_service_available;
    let paper_ready = paper_service_available;
    let review_ready =
        sources_ready && research_ready && portfolio_ready && paper_ready && mcp_available;

    vec![
        SetupStep::new(
            "system",
            "System",
            SetupStepState::Complete,
            true,
            "Validated configuration, controlled paths, catalogs, and application services initialized successfully.",
            None,
            None,
            None,
        ),
        SetupStep::new(
            "storage",
            "Storage",
            SetupStepState::Complete,
            true,
            "The controlled workspace and catalog are open in the effective data directory.",
            None,
            None,
            None,
        ),
        SetupStep::new(
            "sources",
            "Sources",
            if sources_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            sources_ready,
            "Activate at least one supported live market-data source.",
            (!sources_ready)
                .then_some("No supported live market provider session currently holds active authority."),
            (!sources_ready).then_some(
                "Connect Coinbase public, Coinbase Exchange direct, or Kraken and complete its provider-specific verification.",
            ),
            Some(SetupStepAction::ConfigureSources),
        ),
        SetupStep::new(
            "research",
            "Research",
            if research_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            research_ready,
            if research_service_available && files_imported {
                "The complete Research contract is initialized and a local-file import authority is active; model-runtime admission remains separate."
            } else if research_service_available {
                "The complete Research contract is initialized; importing private local data is optional and no import has been recorded."
            } else {
                "The installed application is missing one or more required Research operations."
            },
            (!research_ready).then_some("The complete Research application contract is unavailable."),
            (!research_ready).then_some(
                "Repair or reinstall the complete native package, then refresh status.",
            ),
            Some(SetupStepAction::ConfigureResearch),
        ),
        SetupStep::new(
            "portfolio",
            "Portfolio",
            if portfolio_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            portfolio_ready,
            if portfolio_service_available && portfolio_imported {
                "The complete Portfolio contract is initialized and a private import authority is active."
            } else if portfolio_service_available {
                "The complete Portfolio contract is initialized; importing private holdings or transactions is optional."
            } else {
                "The installed application is missing one or more required Portfolio operations."
            },
            (!portfolio_ready)
                .then_some("The complete Portfolio application contract is unavailable."),
            (!portfolio_ready)
                .then_some("Repair or reinstall the complete native package, then refresh status."),
            Some(SetupStepAction::ConfigurePortfolio),
        ),
        SetupStep::new(
            "paper",
            "Paper",
            if paper_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::ActionRequired
            },
            paper_ready,
            if paper_service_available {
                "The complete local paper-only Bot and Execution contract is initialized under central risk authority."
            } else {
                "The installed application is missing one or more required paper bot or execution operations."
            },
            (!paper_ready).then_some("The complete paper application contract is unavailable."),
            (!paper_ready)
                .then_some("Repair or reinstall the complete native package, then refresh status."),
            Some(SetupStepAction::ConfigurePaper),
        ),
        SetupStep::new(
            "mcp",
            "MCP",
            if mcp_available {
                SetupStepState::Available
            } else {
                SetupStepState::Blocked
            },
            mcp_available,
            if mcp_available {
                "The verified packaged CLI and required workspace identity paths are available as a client configuration; the MCP service is not running."
            } else {
                "The local stdio MCP service requires a verified packaged CLI, representable workspace paths, and a valid tool contract."
            },
            (!mcp_available).then_some(
                "The installed CLI, effective paths, or bounded MCP tool contract is unavailable.",
            ),
            (!mcp_available).then_some(
                "Repair or reinstall the complete native package, then refresh setup status.",
            ),
            Some(SetupStepAction::ReviewMcp),
        ),
        SetupStep::new(
            "review",
            "Review",
            if review_ready {
                SetupStepState::Complete
            } else {
                SetupStepState::Blocked
            },
            review_ready,
            "Final readiness is derived from every required owning authority above.",
            (!review_ready).then_some("One or more required setup authorities remain incomplete."),
            (!review_ready)
                .then_some("Resolve each named blocker, refresh status, and review again."),
            Some(SetupStepAction::ReviewStatus),
        ),
    ]
}

fn has_active_session(sessions: &[OnboardingSessionView], surface_id: &str) -> bool {
    sessions.iter().any(|session| {
        session.surface_id() == surface_id && session.next_action() == OnboardingNextAction::Active
    })
}

#[tauri::command]
pub(crate) fn desktop_bootstrap(
    state: State<'_, DesktopState>,
) -> Result<DesktopBootstrap, DesktopCommandError> {
    state.bootstrap()
}

#[tauri::command]
pub(crate) async fn installation_control(
    request: InstallationControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    app: tauri::AppHandle,
) -> Result<Value, DesktopCommandError> {
    if request.requires_confirmation() && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the installation change before continuing.",
        ));
    }
    let root = state.installation_root.clone();
    match request {
        InstallationControlCommand::Status => {
            let current = blocking_installation(move || installation_status(&root)).await?;
            Ok(json!({
                "action": "status",
                "status": current,
                "receipt": null,
                "restartRequired": false,
            }))
        }
        InstallationControlCommand::Update => {
            let receipt = match update_from_channel(&root).await {
                Ok(receipt) => receipt,
                Err(CommandError::Lifecycle(InstallError::UpdateNotNewer)) => {
                    let current = blocking_installation(move || installation_status(&root)).await?;
                    return Ok(json!({
                        "action": "update",
                        "status": current,
                        "receipt": null,
                        "restartRequired": false,
                    }));
                }
                Err(error) => return Err(map_installation_error(error)),
            };
            let current = prepare_installation_restart(root, &state).await?;
            request_installation_restart(&app);
            Ok(json!({
                "action": "update",
                "status": current,
                "receipt": receipt,
                "restartRequired": true,
            }))
        }
        InstallationControlCommand::Repair => {
            let operation_root = root.clone();
            let receipt =
                blocking_installation(move || repair(RepairRequest::new(operation_root))).await?;
            let current = blocking_installation(move || installation_status(&root)).await?;
            Ok(json!({
                "action": "repair",
                "status": current,
                "receipt": receipt,
                "restartRequired": false,
            }))
        }
        InstallationControlCommand::Rollback => {
            let operation_root = root.clone();
            let receipt =
                blocking_installation(move || rollback(RollbackRequest::new(operation_root)))
                    .await?;
            let current = prepare_installation_restart(root, &state).await?;
            request_installation_restart(&app);
            Ok(json!({
                "action": "rollback",
                "status": current,
                "receipt": receipt,
                "restartRequired": true,
            }))
        }
        InstallationControlCommand::Uninstall => uninstall_programs(root, &app).await,
    }
}

#[cfg(target_os = "windows")]
async fn uninstall_programs(
    root: PathBuf,
    app: &tauri::AppHandle,
) -> Result<Value, DesktopCommandError> {
    let current = blocking_installation(move || installation_status(&root)).await?;
    tauri_plugin_opener::open_url("ms-settings:appsfeatures", None::<&str>).map_err(|_error| {
        DesktopCommandError::new(
            "native_uninstall_handoff_failed",
            "Windows Installed apps could not be opened. Close Market Squawk and remove it \
                 from Windows Settings.",
        )
    })?;
    app.exit(0);
    Ok(json!({
        "action": "uninstall",
        "status": current,
        "receipt": null,
        "restartRequired": false,
    }))
}

#[cfg(not(target_os = "windows"))]
async fn uninstall_programs(
    root: PathBuf,
    app: &tauri::AppHandle,
) -> Result<Value, DesktopCommandError> {
    let receipt =
        blocking_installation(move || uninstall(UninstallRequest::preserving_data(root))).await?;
    app.exit(0);
    Ok(json!({
        "action": "uninstall",
        "status": {
            "installed": false,
            "active_version": null,
            "previous_version": null,
            "target": null,
            "manifest_sha256": null,
            "channel_manifest_url": null,
            "healthy": false
        },
        "receipt": receipt,
        "restartRequired": false,
    }))
}

async fn prepare_installation_restart(
    root: PathBuf,
    state: &DesktopState,
) -> Result<InstallStatus, DesktopCommandError> {
    let status_root = root.clone();
    let current = blocking_installation(move || installation_status(&status_root)).await?;
    let program =
        blocking_installation(move || active_program_path(&root, ProgramName::Desktop)).await?;
    state.schedule_restart(program)?;
    Ok(current)
}

fn request_installation_restart(app: &tauri::AppHandle) {
    app.exit(0);
}

async fn blocking_installation<T>(
    operation: impl FnOnce() -> Result<T, market_squawk_installer::InstallError> + Send + 'static,
) -> Result<T, DesktopCommandError>
where
    T: Send + 'static,
{
    tauri::async_runtime::spawn_blocking(operation)
        .await
        .map_err(|_error| DesktopCommandError::internal())?
        .map_err(map_installation_error)
}

#[tauri::command]
pub(crate) async fn application_invoke(
    request: ApplicationInvocation,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    invoke_application(request, &state, InvocationAuthority::ReadOnly).await
}

async fn invoke_application(
    mut request: ApplicationInvocation,
    state: &DesktopState,
    authority: InvocationAuthority,
) -> Result<Value, DesktopCommandError> {
    if request.operation.is_empty()
        || request.operation.len() > MAXIMUM_OPERATION_BYTES
        || request.operation.chars().any(char::is_control)
    {
        return Err(DesktopCommandError::invalid_request(
            "The selected Market Squawk operation is invalid.",
        ));
    }
    let application = state.product.application();
    let capabilities = application.capabilities();
    let descriptor = capabilities
        .find(&request.operation)
        .ok_or_else(|| map_service_error(ServiceError::NotFound))?;
    let authorized = match authority {
        InvocationAuthority::ReadOnly => descriptor.effects().read_only(),
        InvocationAuthority::ExactConfirmed(operation) => request.operation == operation,
    };
    if !authorized {
        return Err(map_service_error(ServiceError::Unauthorized));
    }
    request.arguments.insert(
        "resultLimits".to_owned(),
        json!({
            "maximumItems": MAXIMUM_DESKTOP_RESULT_ITEMS,
            "maximumBytes": MAXIMUM_DESKTOP_RESULT_BYTES,
        }),
    );
    let input = Value::Object(request.arguments.clone());
    let input_structure = JsonStructureLimits::try_new(24, 64 * 1024, 4_096, 1_024)
        .map_err(|_error| DesktopCommandError::internal())?;
    validate_json_contract(&input, input_structure, MAXIMUM_APPLICATION_ARGUMENT_BYTES).map_err(
        |_error| {
            DesktopCommandError::invalid_request(
                "The operation input exceeds the desktop safety limits.",
            )
        },
    )?;

    let result_structure = JsonStructureLimits::try_new(32, 1024 * 1024, 10_000, 10_000)
        .map_err(|_error| DesktopCommandError::internal())?;
    let limits = ServiceLimits::try_new(
        1024 * 1024,
        1_000,
        4 * 1024 * 1024,
        10_000,
        result_structure,
    )
    .map_err(|_error| DesktopCommandError::internal())?;
    let deadline = Instant::now()
        .checked_add(APPLICATION_REQUEST_TIMEOUT)
        .ok_or_else(DesktopCommandError::internal)?;
    let cancellation = state.cancellation.child_token();
    let request_id = RequestId::try_string(format!("desktop-{}", Uuid::new_v4()))
        .map_err(|_error| DesktopCommandError::internal())?;
    let context = RequestContext::new(request_id, cancellation.clone(), deadline, limits);
    let operation = request.operation;
    let arguments = request.arguments;
    tokio::select! {
        biased;
        () = state.cancellation.cancelled() => {
            cancellation.cancel();
            Err(map_service_error(ServiceError::Cancelled))
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            cancellation.cancel();
            Err(map_service_error(ServiceError::DeadlineExceeded))
        }
        result = application.invoke(&operation, arguments, context) => {
            result
                .map(|result| result.into_envelope())
                .map_err(map_service_error)
        }
    }
}

#[tauri::command]
pub(crate) async fn provider_onboarding(
    request: ProviderOnboardingCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    if request.requires_confirmation() && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the provider change before continuing.",
        ));
    }
    let cancellation = state.cancellation.child_token();
    let operation_cancellation = cancellation.clone();
    let onboarding = state.product.provider_onboarding();
    let activation = Arc::clone(&state.portal_activation);
    let operation = async move {
        match request {
            ProviderOnboardingCommand::Bootstrap => provider_bootstrap(&onboarding),
            ProviderOnboardingCommand::Start {
                surface_id,
                organization,
                administrative_email,
            } => {
                let request =
                    StartOnboardingRequest::try_new(surface_id, organization, administrative_email)
                        .map_err(map_onboarding_error)?;
                serialize(
                    onboarding
                        .start(request, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::Resume { session_id } => serialize(
                onboarding
                    .resume(session_id)
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::UnlockFallback { secret } => {
                let secret = SecretValue::new(secret).map_err(|_error| {
                    DesktopCommandError::invalid_request(
                        "The encrypted-storage unlock value is invalid.",
                    )
                })?;
                serialize(
                    onboarding
                        .unlock_encrypted_file_fallback(secret, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::LockFallback => serialize(
                onboarding
                    .lock_encrypted_file_fallback(operation_cancellation)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::SubmitSecret { session_id, secret } => {
                let secret = SecretValue::new(secret).map_err(|_error| {
                    DesktopCommandError::invalid_request(
                        "The provider credential is empty or too large.",
                    )
                })?;
                serialize(
                    onboarding
                        .submit_secret(session_id, secret, operation_cancellation)
                        .await
                        .map_err(map_onboarding_error)?,
                )
            }
            ProviderOnboardingCommand::Activate {
                session_id,
                request,
            } => serialize(
                activation
                    .activate(session_id, request, operation_cancellation)
                    .await
                    .map_err(map_activation_error)?,
            ),
            ProviderOnboardingCommand::Renew { session_id } => serialize(
                onboarding
                    .begin_renewal(session_id)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::Cleanup { session_id } => serialize(
                onboarding
                    .reconcile_cleanup(session_id, operation_cancellation)
                    .await
                    .map_err(map_onboarding_error)?,
            ),
            ProviderOnboardingCommand::Cancel { session_id } => serialize(
                activation
                    .cancel(session_id, operation_cancellation)
                    .await
                    .map_err(map_activation_error)?,
            ),
        }
    };
    tokio::select! {
        biased;
        () = state.cancellation.cancelled() => {
            cancellation.cancel();
            Err(DesktopCommandError::new(
                "cancelled",
                "The desktop is shutting down.",
            ))
        }
        () = tokio::time::sleep(PROVIDER_REQUEST_TIMEOUT) => {
            cancellation.cancel();
            Err(DesktopCommandError::new(
                "deadline_exceeded",
                "The provider operation exceeded its local deadline.",
            ))
        }
        result = operation => result,
    }
}

#[tauri::command]
pub(crate) fn open_official_provider_page(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let profile = state
        .product
        .provider_onboarding()
        .profiles()
        .into_iter()
        .find(|profile| profile.id() == provider_id)
        .ok_or_else(|| {
            DesktopCommandError::invalid_request("The selected provider is not supported.")
        })?;
    tauri_plugin_opener::open_url(profile.official_handoff_url(), None::<&str>).map_err(|_error| {
        DesktopCommandError::new(
            "open_failed",
            "The official provider page could not be opened in the system browser.",
        )
    })
}

#[tauri::command]
pub(crate) async fn open_protected_provider_setup(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let supported = state
        .product
        .provider_onboarding()
        .profiles()
        .into_iter()
        .any(|profile| profile.id() == provider_id);
    if !supported {
        return Err(DesktopCommandError::invalid_request(
            "The selected provider is not supported.",
        ));
    }
    let mut arguments = Map::new();
    arguments.insert("provider".to_owned(), Value::String(provider_id));
    arguments.insert("confirm".to_owned(), Value::Bool(true));
    let result = invoke_application(
        ApplicationInvocation {
            operation: SOURCE_SETUP_OPERATION.to_owned(),
            arguments,
        },
        &state,
        InvocationAuthority::ExactConfirmed(SOURCE_SETUP_OPERATION),
    )
    .await?;
    let portal_url = result
        .pointer("/data/portal/url")
        .and_then(Value::as_str)
        .ok_or_else(DesktopCommandError::internal)?;
    let parsed = Url::parse(portal_url).map_err(|_error| DesktopCommandError::internal())?;
    if parsed.scheme() != "http"
        || parsed.host_str() != Some("127.0.0.1")
        || parsed.port().is_none()
        || parsed.username() != ""
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(DesktopCommandError::internal());
    }
    tauri_plugin_opener::open_url(parsed.as_str(), None::<&str>).map_err(|_error| {
        DesktopCommandError::new(
            "open_failed",
            "The protected provider setup could not be opened in the system browser.",
        )
    })
}

fn provider_bootstrap(
    onboarding: &market_squawk::ProviderOnboardingService,
) -> Result<Value, DesktopCommandError> {
    let limit = CatalogLimit::new(MAXIMUM_PROVIDER_SESSIONS)
        .map_err(|_error| DesktopCommandError::internal())?;
    Ok(json!({
        "profiles": onboarding.profiles(),
        "sessions": onboarding
            .current_sessions(limit)
            .map_err(map_onboarding_error)?,
        "encryptedFileFallback": onboarding
            .encrypted_file_fallback_status()
            .map_err(map_onboarding_error)?,
    }))
}

fn serialize(value: impl Serialize) -> Result<Value, DesktopCommandError> {
    serde_json::to_value(value).map_err(|_error| DesktopCommandError::internal())
}

fn map_service_error(error: ServiceError) -> DesktopCommandError {
    DesktopCommandError::new(
        match error {
            ServiceError::InvalidRequest => "invalid_request",
            ServiceError::NotFound => "not_found",
            ServiceError::Unauthorized => "unauthorized",
            ServiceError::ResourceExhausted => "resource_exhausted",
            ServiceError::Cancelled => "cancelled",
            ServiceError::DeadlineExceeded => "deadline_exceeded",
            ServiceError::Unavailable => "unavailable",
            ServiceError::InvalidResult => "invalid_result",
            ServiceError::Internal => "internal",
        },
        error.to_string(),
    )
}

fn map_onboarding_error(error: ProviderOnboardingError) -> DesktopCommandError {
    let code = match error {
        ProviderOnboardingError::UnknownProfile => "unknown_provider",
        ProviderOnboardingError::InvalidRequest | ProviderOnboardingError::InvalidSecretShape => {
            "invalid_request"
        }
        ProviderOnboardingError::AdministrativeContactRequired => "contact_required",
        ProviderOnboardingError::CredentialRejected => "credential_rejected",
        ProviderOnboardingError::ProbeRateLimited => "rate_limited",
        ProviderOnboardingError::ProbeDeadlineExceeded => "deadline_exceeded",
        ProviderOnboardingError::OperationCancelled => "cancelled",
        ProviderOnboardingError::EvidenceRefreshRequired => "evidence_refresh_required",
        ProviderOnboardingError::RightsBlocked => "rights_blocked",
        ProviderOnboardingError::ActivationExpired => "activation_expired",
        ProviderOnboardingError::SecretCleanupUnavailable
        | ProviderOnboardingError::RemoteReconciliationRequired => "reconciliation_required",
        ProviderOnboardingError::InvalidProfile
        | ProviderOnboardingError::SecretImportUnavailable
        | ProviderOnboardingError::RenewalUnavailable
        | ProviderOnboardingError::ActivationUnavailable
        | ProviderOnboardingError::SecretVerificationFailed
        | ProviderOnboardingError::InvalidSessionState
        | ProviderOnboardingError::ClientConfiguration
        | ProviderOnboardingError::ProbeUnavailable
        | ProviderOnboardingError::OfficialDocumentUnavailable
        | ProviderOnboardingError::SecretOperationUnavailable
        | ProviderOnboardingError::Clock
        | ProviderOnboardingError::Profile(_)
        | ProviderOnboardingError::Catalog(_)
        | ProviderOnboardingError::SecretStore(_)
        | ProviderOnboardingError::Identity(_)
        | ProviderOnboardingError::Network(_)
        | ProviderOnboardingError::Tls(_) => "provider_unavailable",
    };
    DesktopCommandError::new(code, error.to_string())
}

fn map_activation_error(error: ProviderPortalActivationError) -> DesktopCommandError {
    DesktopCommandError::new(
        match error {
            ProviderPortalActivationError::InvalidRequest => "invalid_request",
            ProviderPortalActivationError::Unavailable => "provider_unavailable",
            ProviderPortalActivationError::StateUnavailable => "state_unavailable",
            ProviderPortalActivationError::Cancelled => "cancelled",
        },
        error.to_string(),
    )
}

fn map_installation_error(error: impl std::fmt::Display) -> DesktopCommandError {
    DesktopCommandError::new("installation_failed", error.to_string())
}
