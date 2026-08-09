//! Least-privilege Tauri bridge over the existing local application authorities.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use market_squawk::service::BootstrapRequirement;
use market_squawk_installer::{
    CommandError, InstallError, InstallStatus, RepairRequest, RollbackRequest, repair, rollback,
    status as installation_status, update_from_channel,
};
use market_squawk_installer::{ProgramName, program_install_snapshot};
#[cfg(not(target_os = "windows"))]
use market_squawk_installer::{UninstallRequest, uninstall};
use market_squawk_platform::{LocalPaths, SecretValue};
use market_squawk_runtime::{ApplicationClientError, LoopbackApplicationClient, RuntimeIdentity};
use market_squawk_services::RequestId;
use serde_json::{Map, Value, json};
use tauri::{Manager as _, State};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::contracts::{
    ApplicationInvocation, DesktopBootstrap, DesktopCommandError, DesktopServiceBootstrapCommand,
    DesktopServiceBootstrapRequirement, DesktopServiceBootstrapStatus, DesktopStartup,
    InstallationControlCommand, OperationSummary, ProviderOnboardingCommand, Readiness,
    ReadinessState,
};
use crate::mcp_clients::DesktopMcpClientState;
use crate::service::{
    self, DesktopBootstrapAction, DesktopServiceBootstrap, DesktopServiceConnection,
    DesktopServiceStartup,
};

const MAXIMUM_APPLICATION_ARGUMENT_BYTES: usize = 256 * 1024;
const MAXIMUM_DESKTOP_RESULT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_DESKTOP_RESULT_ITEMS: u64 = 1_000;
const MAXIMUM_SAFE_JAVASCRIPT_INTEGER: i64 = 9_007_199_254_740_991;
const MAXIMUM_OPERATION_BYTES: usize = 128;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const GOVERNANCE_AUTHORIZATION_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAXIMUM_GOVERNANCE_AUTHORIZATIONS: usize = 256;
const SOURCE_SETUP_OPERATION: &str = "Source.Setup";
const SOURCE_STATUS_OPERATION: &str = "Source.GetStatus";

pub(crate) struct DesktopCompositionContext {
    configured_data_root: PathBuf,
    service_data_root: PathBuf,
    installation_root: PathBuf,
    installation_status: InstallStatus,
    relay_program: PathBuf,
}

impl DesktopCompositionContext {
    pub(crate) fn new(
        configured_data_root: PathBuf,
        service_data_root: PathBuf,
        installation_root: PathBuf,
        installation_status: InstallStatus,
        relay_program: PathBuf,
    ) -> Self {
        Self {
            configured_data_root,
            service_data_root,
            installation_root,
            installation_status,
            relay_program,
        }
    }
}

struct PendingDesktopBootstrap {
    service: DesktopServiceBootstrap,
    context: DesktopCompositionContext,
}

pub(crate) struct DesktopBootstrapState {
    pending: tokio::sync::Mutex<Option<PendingDesktopBootstrap>>,
}

impl DesktopBootstrapState {
    pub(crate) fn compose(
        app: &tauri::AppHandle,
        startup: DesktopServiceStartup,
        context: DesktopCompositionContext,
    ) -> Result<Self, DesktopCommandError> {
        let pending = match startup {
            DesktopServiceStartup::Ready(connection) => {
                manage_ready_desktop(app, *connection, &context)?;
                None
            }
            DesktopServiceStartup::BootstrapRequired(service) => {
                Some(PendingDesktopBootstrap { service, context })
            }
        };
        Ok(Self {
            pending: tokio::sync::Mutex::new(pending),
        })
    }

    async fn status(&self) -> Result<DesktopServiceBootstrapStatus, DesktopCommandError> {
        let pending = self.pending.lock().await;
        let pending = pending.as_ref().ok_or_else(DesktopCommandError::internal)?;
        Ok(DesktopServiceBootstrapStatus::required(
            match pending.service.requirement() {
                BootstrapRequirement::EncryptedFallbackLocked => {
                    DesktopServiceBootstrapRequirement::EncryptedFallbackLocked
                }
                BootstrapRequirement::ForegroundKeyringRetry => {
                    DesktopServiceBootstrapRequirement::ForegroundKeyringRetry
                }
            },
        ))
    }
}

fn manage_ready_desktop(
    app: &tauri::AppHandle,
    connection: DesktopServiceConnection,
    context: &DesktopCompositionContext,
) -> Result<(), DesktopCommandError> {
    if app.try_state::<DesktopState>().is_some()
        || app.try_state::<DesktopMcpClientState>().is_some()
    {
        return Err(DesktopCommandError::internal());
    }
    let state = DesktopState::try_new(
        connection.application,
        connection.bootstrap,
        &context.configured_data_root,
        &context.service_data_root,
        context.installation_root.clone(),
        context.installation_status.clone(),
    )?;
    let local_paths = LocalPaths::open_existing(state.data_root())
        .map_err(|_error| DesktopCommandError::internal())?;
    let (endpoint_identity, claude_credential_identity, codex_credential_identity) =
        state.mcp_authority_identities();
    let mcp_clients = DesktopMcpClientState::try_new(
        &local_paths,
        context.relay_program.clone(),
        &context.service_data_root,
        state.runtime(),
        endpoint_identity,
        claude_credential_identity,
        codex_credential_identity,
    )
    .map_err(|_error| DesktopCommandError::internal())?;
    if !app.manage(state) || !app.manage(mcp_clients) {
        return Err(DesktopCommandError::internal());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum InvocationAuthority {
    ReadOnly,
    ExactConfirmed(&'static str),
    RiskMediated(&'static str),
}

#[derive(Clone, Debug)]
struct ServiceOperation {
    name: String,
    description: String,
    domain: String,
    authorization: String,
    read_only: bool,
    destructive: bool,
    input_schema: Value,
}

impl ServiceOperation {
    fn into_summary(self) -> OperationSummary {
        OperationSummary::new(
            self.name,
            self.description,
            self.domain,
            self.authorization,
            self.read_only,
            self.destructive,
            self.input_schema,
        )
    }
}

#[derive(Debug)]
struct ServiceBootstrapSnapshot {
    runtime: RuntimeIdentity,
    workspace_placement: ServiceWorkspacePlacement,
    mcp_endpoint_identity: String,
    claude_code_credential_identity: String,
    codex_credential_identity: String,
    provider_profiles: Value,
    encrypted_file_fallback: Value,
    operations: Vec<ServiceOperation>,
    mcp_ready: bool,
    model_runtime_configured: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServiceWorkspacePlacement {
    Managed,
    LegacyMigrationRequired,
}

impl TryFrom<Value> for ServiceBootstrapSnapshot {
    type Error = DesktopCommandError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.get("schemaVersion").and_then(Value::as_u64) != Some(1)
            || value.pointer("/product/name").and_then(Value::as_str) != Some("Market Squawk")
            || value.pointer("/product/deployment").and_then(Value::as_str) != Some("self_hosted")
            || value.pointer("/product/version").and_then(Value::as_str)
                != Some(env!("CARGO_PKG_VERSION"))
            || value.pointer("/readiness/service").and_then(Value::as_bool) != Some(true)
            || value
                .pointer("/readiness/nativeApplication")
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Err(DesktopCommandError::new(
                "invalid_service_contract",
                "The installed Market Squawk service contract is incompatible with this dashboard.",
            ));
        }
        let runtime = serde_json::from_value::<RuntimeIdentity>(
            value
                .get("runtime")
                .cloned()
                .ok_or_else(DesktopCommandError::internal)?,
        )
        .map_err(|_error| DesktopCommandError::internal())?;
        if value.pointer("/workspace/id").and_then(Value::as_str)
            != Some(runtime.workspace_id().as_uuid().to_string().as_str())
            || value
                .pointer("/workspace/generation")
                .and_then(Value::as_u64)
                != Some(runtime.service_generation().get())
        {
            return Err(DesktopCommandError::internal());
        }
        let workspace_placement = match required_string(&value, "/workspace/placement")? {
            "managed" => ServiceWorkspacePlacement::Managed,
            "legacy_migration_required" => ServiceWorkspacePlacement::LegacyMigrationRequired,
            _ => return Err(DesktopCommandError::internal()),
        };
        let mcp_endpoint_identity =
            required_string(&value, "/mcpAuthority/endpointIdentity")?.to_owned();
        let claude_code_credential_identity =
            required_string(&value, "/mcpAuthority/claudeCodeCredentialIdentity")?.to_owned();
        let codex_credential_identity =
            required_string(&value, "/mcpAuthority/codexCredentialIdentity")?.to_owned();
        let provider_profiles = value
            .pointer("/sources/profiles")
            .filter(|profiles| profiles.is_array())
            .cloned()
            .ok_or_else(DesktopCommandError::internal)?;
        let encrypted_file_fallback = value
            .pointer("/sources/encryptedFileFallback")
            .cloned()
            .ok_or_else(DesktopCommandError::internal)?;
        let operations = value
            .pointer("/application/operations")
            .and_then(Value::as_array)
            .ok_or_else(DesktopCommandError::internal)?;
        let mut names = BTreeSet::new();
        let mut parsed_operations = Vec::new();
        parsed_operations
            .try_reserve(operations.len())
            .map_err(|_error| DesktopCommandError::internal())?;
        for operation in operations {
            let name = required_string(operation, "/name")?.to_owned();
            if !names.insert(name.clone()) {
                return Err(DesktopCommandError::internal());
            }
            let authorization = required_string(operation, "/contract/authorization")?.to_owned();
            let read_only = operation
                .pointer("/effects/readOnly")
                .and_then(Value::as_bool)
                .ok_or_else(DesktopCommandError::internal)?;
            if !matches!(
                authorization.as_str(),
                "read_only" | "local_confirmation" | "risk_mediated"
            ) || (authorization == "read_only") != read_only
            {
                return Err(DesktopCommandError::internal());
            }
            parsed_operations.push(ServiceOperation {
                name,
                description: required_string(operation, "/description")?.to_owned(),
                domain: required_string(operation, "/contract/domain")?.to_owned(),
                authorization,
                read_only,
                destructive: operation
                    .pointer("/effects/destructive")
                    .and_then(Value::as_bool)
                    .ok_or_else(DesktopCommandError::internal)?,
                input_schema: operation
                    .get("inputSchema")
                    .filter(|schema| schema.is_object())
                    .cloned()
                    .ok_or_else(DesktopCommandError::internal)?,
            });
        }
        let mcp_ready = value
            .pointer("/readiness/mcp")
            .and_then(Value::as_bool)
            .ok_or_else(DesktopCommandError::internal)?;
        let model_runtime_configured = value
            .pointer("/readiness/modelRuntimeConfigured")
            .and_then(Value::as_bool)
            .ok_or_else(DesktopCommandError::internal)?;
        Ok(Self {
            runtime,
            workspace_placement,
            mcp_endpoint_identity,
            claude_code_credential_identity,
            codex_credential_identity,
            provider_profiles,
            encrypted_file_fallback,
            operations: parsed_operations,
            mcp_ready,
            model_runtime_configured,
        })
    }
}

fn required_string<'a>(value: &'a Value, pointer: &str) -> Result<&'a str, DesktopCommandError> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)
}

pub(crate) struct DesktopState {
    application: Arc<LoopbackApplicationClient>,
    service_bootstrap: ServiceBootstrapSnapshot,
    data_root: PathBuf,
    installation_root: PathBuf,
    installation_status: InstallStatus,
    cancellation: CancellationToken,
    restart_program: OnceLock<PathBuf>,
    governance_authorizations: Mutex<HashMap<Uuid, NativeGovernanceAuthorization>>,
}

struct NativeGovernanceAuthorization {
    window_label: String,
    preview_id: Uuid,
    ticket_id: Uuid,
    retained_until: Instant,
}

impl DesktopState {
    pub(crate) fn try_new(
        application: LoopbackApplicationClient,
        service_bootstrap: Value,
        configured_data_root: &Path,
        service_data_root: &Path,
        installation_root: PathBuf,
        installation_status: InstallStatus,
    ) -> Result<Self, DesktopCommandError> {
        let service_bootstrap = ServiceBootstrapSnapshot::try_from(service_bootstrap)?;
        let data_root = resolve_workspace_data_root(
            &service_bootstrap,
            configured_data_root,
            service_data_root,
        )?;
        Ok(Self {
            application: Arc::new(application),
            service_bootstrap,
            data_root,
            installation_root,
            installation_status,
            cancellation: CancellationToken::new(),
            restart_program: OnceLock::new(),
            governance_authorizations: Mutex::new(HashMap::new()),
        })
    }

    fn data_root(&self) -> &Path {
        &self.data_root
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

    pub(crate) fn application(&self) -> Arc<LoopbackApplicationClient> {
        Arc::clone(&self.application)
    }

    pub(crate) const fn runtime(&self) -> RuntimeIdentity {
        self.service_bootstrap.runtime
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.child_token()
    }

    pub(crate) fn service_operation_index(&self) -> BTreeMap<String, String> {
        self.service_bootstrap
            .operations
            .iter()
            .filter(|descriptor| !descriptor.read_only && descriptor.authorization != "read_only")
            .map(|descriptor| (descriptor.name.clone(), descriptor.domain.clone()))
            .collect()
    }

    pub(crate) const fn mcp_ready(&self) -> bool {
        self.service_bootstrap.mcp_ready
    }

    pub(crate) fn mcp_authority_identities(&self) -> (&str, &str, &str) {
        (
            &self.service_bootstrap.mcp_endpoint_identity,
            &self.service_bootstrap.claude_code_credential_identity,
            &self.service_bootstrap.codex_credential_identity,
        )
    }

    async fn bootstrap(&self) -> Result<DesktopBootstrap, DesktopCommandError> {
        let sessions = self.provider_sessions().await?;
        let capabilities = &self.service_bootstrap.operations;
        let operations = capabilities
            .iter()
            .cloned()
            .map(ServiceOperation::into_summary)
            .collect();
        let model_runtime = if self.service_bootstrap.model_runtime_configured {
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
        let mcp_available = self.service_bootstrap.mcp_ready;
        let mcp = if mcp_available {
            Readiness::new(
                ReadinessState::Available,
                "Available",
                "The shared local MCP service is running. Client connection can be configured without exposing its credential to this window.",
            )
        } else {
            Readiness::new(
                ReadinessState::Unverified,
                "Unavailable",
                "The installed service did not report the complete local MCP contract as ready.",
            )
        };
        let installation = &self.installation_status;
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
            self.data_root.to_string_lossy().into_owned(),
            self.service_bootstrap.runtime,
            Readiness::new(
                ReadinessState::Ready,
                "Ready",
                "The installed service authenticated this window for the active workspace.",
            ),
            installation,
            model_runtime,
            mcp,
            self.service_bootstrap.encrypted_file_fallback.clone(),
            self.service_bootstrap.provider_profiles.clone(),
            Value::Array(sessions),
            operations,
        ))
    }

    async fn provider_sessions(&self) -> Result<Vec<Value>, DesktopCommandError> {
        let result = invoke_application(
            ApplicationInvocation {
                operation: SOURCE_STATUS_OPERATION.to_owned(),
                arguments: Map::new(),
            },
            self,
            InvocationAuthority::ReadOnly,
        )
        .await?;
        let rows = match result.get("data") {
            Some(Value::Array(rows)) => rows,
            Some(Value::Null) => return Ok(Vec::new()),
            _ => return Err(DesktopCommandError::internal()),
        };
        let mut identities = BTreeSet::new();
        let mut sessions = Vec::new();
        for row in rows {
            let Some(session) = row.get("currentSession") else {
                return Err(DesktopCommandError::internal());
            };
            if session.is_null() {
                continue;
            }
            let identity = session
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(DesktopCommandError::internal)?;
            if identities.insert(identity.to_owned()) {
                sessions.push(session.clone());
            }
        }
        Ok(sessions)
    }

    pub(crate) fn retain_governance_authorization(
        &self,
        window_label: &str,
        mut result: Value,
    ) -> Result<Value, DesktopCommandError> {
        let authorization = result
            .pointer("/data/authorization")
            .and_then(Value::as_object)
            .ok_or_else(DesktopCommandError::internal)?;
        let ticket_id = required_uuid(authorization, "ticketId")?;
        let preview_id = required_uuid(authorization, "previewId")?;
        let principal_id = required_uuid(authorization, "principalId")?;
        let expires_at = authorization
            .get("expiresAt")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(DesktopCommandError::internal)?
            .to_owned();
        let retained_until = Instant::now()
            .checked_add(GOVERNANCE_AUTHORIZATION_LIFETIME)
            .ok_or_else(DesktopCommandError::internal)?;
        let mut authorizations = self
            .governance_authorizations
            .lock()
            .map_err(|_| DesktopCommandError::internal())?;
        authorizations.retain(|_, entry| entry.retained_until > Instant::now());
        if authorizations.len() >= MAXIMUM_GOVERNANCE_AUTHORIZATIONS {
            return Err(DesktopCommandError::new(
                "resource_exhausted",
                "Too many pending governance authorizations are open. Finish or restart the current review.",
            ));
        }
        let handle = allocate_authorization_handle(&authorizations)?;
        authorizations.insert(
            handle,
            NativeGovernanceAuthorization {
                window_label: window_label.to_owned(),
                preview_id,
                ticket_id,
                retained_until,
            },
        );
        let data = result
            .get_mut("data")
            .and_then(Value::as_object_mut)
            .ok_or_else(DesktopCommandError::internal)?;
        data.insert(
            "authorization".to_owned(),
            json!({
                "authorizationHandle": handle,
                "previewId": preview_id,
                "principalId": principal_id,
                "expiresAt": expires_at,
            }),
        );
        Ok(result)
    }

    pub(crate) fn consume_governance_authorizations(
        &self,
        window_label: &str,
        preview_id: Uuid,
        handles: Vec<Uuid>,
    ) -> Result<Vec<Uuid>, DesktopCommandError> {
        if handles.is_empty() || handles.len() > 2 {
            return Err(DesktopCommandError::invalid_request(
                "The governance action requires one or two current authorizations.",
            ));
        }
        let mut unique = HashSet::new();
        if handles.iter().any(|handle| !unique.insert(*handle)) {
            return Err(DesktopCommandError::invalid_request(
                "A governance authorization can be used only once.",
            ));
        }
        let now = Instant::now();
        let mut authorizations = self
            .governance_authorizations
            .lock()
            .map_err(|_| DesktopCommandError::internal())?;
        authorizations.retain(|_, entry| entry.retained_until > now);
        let mut ticket_ids = Vec::with_capacity(handles.len());
        for handle in &handles {
            let entry = authorizations.get(handle).ok_or_else(|| {
                DesktopCommandError::new(
                    "authorization_unavailable",
                    "The governance authorization expired or is no longer available. Authenticate again.",
                )
            })?;
            if entry.window_label != window_label || entry.preview_id != preview_id {
                return Err(DesktopCommandError::new(
                    "unauthorized",
                    "The governance authorization does not belong to this window and preview.",
                ));
            }
            ticket_ids.push(entry.ticket_id);
        }
        for handle in handles {
            authorizations.remove(&handle);
        }
        Ok(ticket_ids)
    }

    pub(crate) fn begin_shutdown(&self) {
        self.cancellation.cancel();
        if let Ok(mut authorizations) = self.governance_authorizations.lock() {
            authorizations.clear();
        }
    }

    pub(crate) async fn finish_shutdown(&self) {
        self.begin_shutdown();
    }
}

fn resolve_workspace_data_root(
    bootstrap: &ServiceBootstrapSnapshot,
    configured_data_root: &Path,
    service_data_root: &Path,
) -> Result<PathBuf, DesktopCommandError> {
    let candidate = match bootstrap.workspace_placement {
        ServiceWorkspacePlacement::Managed => service_data_root
            .join("workspaces")
            .join(bootstrap.runtime.workspace_id().as_uuid().to_string()),
        ServiceWorkspacePlacement::LegacyMigrationRequired => configured_data_root.to_path_buf(),
    };
    if !candidate.is_absolute() {
        return Err(DesktopCommandError::internal());
    }
    let paths =
        LocalPaths::open_existing(candidate).map_err(|_error| DesktopCommandError::internal())?;
    Ok(paths.root().to_path_buf())
}

fn required_uuid(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<Uuid, DesktopCommandError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(DesktopCommandError::internal)
}

fn allocate_authorization_handle(
    authorizations: &HashMap<Uuid, NativeGovernanceAuthorization>,
) -> Result<Uuid, DesktopCommandError> {
    (0..16)
        .map(|_| Uuid::new_v4())
        .find(|candidate| !authorizations.contains_key(candidate))
        .ok_or_else(DesktopCommandError::internal)
}

#[tauri::command]
pub(crate) async fn desktop_bootstrap(
    app: tauri::AppHandle,
    bootstrap_state: State<'_, DesktopBootstrapState>,
) -> Result<DesktopStartup, DesktopCommandError> {
    if let Some(state) = app.try_state::<DesktopState>() {
        return state
            .bootstrap()
            .await
            .map(Box::new)
            .map(DesktopStartup::Ready);
    }
    bootstrap_state
        .status()
        .await
        .map(DesktopStartup::BootstrapRequired)
}

#[tauri::command]
pub(crate) async fn desktop_service_bootstrap(
    request: DesktopServiceBootstrapCommand,
    window: tauri::Window,
    app: tauri::AppHandle,
    bootstrap_state: State<'_, DesktopBootstrapState>,
) -> Result<(), DesktopCommandError> {
    if window.label() != "main" || app.try_state::<DesktopState>().is_some() {
        return Err(DesktopCommandError::new(
            "service_bootstrap_unavailable",
            "The local service bootstrap action is not available for this window.",
        ));
    }
    let action = match request {
        DesktopServiceBootstrapCommand::UnlockEncryptedFallback { unlock } => {
            DesktopBootstrapAction::Unlock(SecretValue::new(unlock).map_err(|_error| {
                DesktopCommandError::new(
                    "service_bootstrap_rejected",
                    "The local service rejected the bounded bootstrap action.",
                )
            })?)
        }
        DesktopServiceBootstrapCommand::RetryAfterForegroundKeyring => {
            DesktopBootstrapAction::RetryAfterForegroundKeyring
        }
    };
    let mut pending_guard = bootstrap_state.pending.lock().await;
    let pending = pending_guard.as_ref().ok_or_else(|| {
        DesktopCommandError::new(
            "service_bootstrap_unavailable",
            "The local service bootstrap action is no longer available.",
        )
    })?;
    let connection = service::complete_bootstrap(&pending.service, action)
        .await
        .map_err(|_error| {
            DesktopCommandError::new(
                "service_bootstrap_failed",
                "The local service could not complete credential bootstrap and reconnect.",
            )
        })?;
    manage_ready_desktop(&app, connection, &pending.context)?;
    *pending_guard = None;
    Ok(())
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
    let snapshot =
        blocking_installation(move || program_install_snapshot(&root, ProgramName::Desktop))
            .await?;
    let current = snapshot.status().clone();
    let program = snapshot
        .program_path()
        .ok_or_else(DesktopCommandError::internal)?
        .to_path_buf();
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

pub(crate) async fn invoke_application(
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
    if matches!(
        authority,
        InvocationAuthority::ExactConfirmed(_) | InvocationAuthority::RiskMediated(_)
    ) {
        request
            .arguments
            .insert("confirm".to_owned(), Value::Bool(true));
    }
    invoke_service_operation(
        state,
        &request.operation,
        request.arguments,
        authority,
        true,
    )
    .await
}

/// Invokes one private installed-client operation without copying transport bookkeeping fields into
/// its closed business input. The operation must still be present in the authenticated service
/// bootstrap and match the exact native authority supplied by the caller.
pub(crate) async fn invoke_private_application(
    operation: &'static str,
    mut arguments: Map<String, Value>,
    state: &DesktopState,
    authority: InvocationAuthority,
) -> Result<Value, DesktopCommandError> {
    if matches!(
        authority,
        InvocationAuthority::ExactConfirmed(_) | InvocationAuthority::RiskMediated(_)
    ) {
        arguments.insert("confirm".to_owned(), Value::Bool(true));
    }
    invoke_service_operation(state, operation, arguments, authority, false).await
}

async fn invoke_service_operation(
    state: &DesktopState,
    operation: &str,
    mut arguments: Map<String, Value>,
    authority: InvocationAuthority,
    apply_desktop_result_limits: bool,
) -> Result<Value, DesktopCommandError> {
    let descriptor = state
        .service_bootstrap
        .operations
        .iter()
        .find(|descriptor| descriptor.name == operation)
        .ok_or_else(|| DesktopCommandError::new("not_found", "The operation is unavailable."))?;
    let authorized = match authority {
        InvocationAuthority::ReadOnly => {
            descriptor.read_only && descriptor.authorization == "read_only"
        }
        InvocationAuthority::ExactConfirmed(expected) => {
            operation == expected
                && !descriptor.read_only
                && descriptor.authorization == "local_confirmation"
        }
        InvocationAuthority::RiskMediated(expected) => {
            operation == expected
                && !descriptor.read_only
                && descriptor.authorization == "risk_mediated"
        }
    };
    if !authorized {
        return Err(DesktopCommandError::new(
            "unauthorized",
            "The dashboard is not authorized to invoke this operation.",
        ));
    }
    if apply_desktop_result_limits
        && descriptor
            .input_schema
            .pointer("/properties/resultLimits")
            .is_some()
    {
        arguments.insert(
            "resultLimits".to_owned(),
            json!({
                "maximumItems": MAXIMUM_DESKTOP_RESULT_ITEMS,
                "maximumBytes": MAXIMUM_DESKTOP_RESULT_BYTES,
            }),
        );
    }
    let arguments = Value::Object(arguments);
    let argument_bytes =
        serde_json::to_vec(&arguments).map_err(|_error| DesktopCommandError::internal())?;
    if argument_bytes.len() > MAXIMUM_APPLICATION_ARGUMENT_BYTES {
        return Err(DesktopCommandError::invalid_request(
            "The operation input exceeds the desktop safety limits.",
        ));
    }
    let request_id = RequestId::try_string(format!("desktop-{}", Uuid::new_v4()))
        .map_err(|_error| DesktopCommandError::internal())?;
    let response = state
        .application
        .invoke_operation(
            request_id,
            operation,
            arguments,
            APPLICATION_REQUEST_TIMEOUT,
            state.cancellation.child_token(),
        )
        .await
        .map_err(map_application_client_error)?;
    let result = decode_application_result(response.result())?;
    let result_bytes =
        serde_json::to_vec(&result).map_err(|_error| DesktopCommandError::internal())?;
    let maximum_result_bytes = usize::try_from(MAXIMUM_DESKTOP_RESULT_BYTES)
        .map_err(|_error| DesktopCommandError::internal())?;
    if result_bytes.len() > maximum_result_bytes {
        return Err(DesktopCommandError::new(
            "resource_exhausted",
            "The operation result exceeds the dashboard safety limit.",
        ));
    }
    let result = lossless_webview_value(result);
    let webview_bytes =
        serde_json::to_vec(&result).map_err(|_error| DesktopCommandError::internal())?;
    if webview_bytes.len() > maximum_result_bytes {
        return Err(DesktopCommandError::new(
            "resource_exhausted",
            "The operation result exceeds the dashboard safety limit.",
        ));
    }
    Ok(result)
}

/// Preserves integers that JavaScript cannot represent exactly as decimal strings.
///
/// Tauri transports JSON values through a WebView. Nanosecond timestamps routinely exceed
/// JavaScript's safe-integer range, so allowing Serde to emit them as JSON numbers would silently
/// alter point-in-time evidence before the dashboard validates it.
pub(crate) fn lossless_webview_value(value: Value) -> Value {
    match value {
        Value::Number(number) => {
            let outside_safe_range = number.as_i64().is_some_and(|value| {
                !(-MAXIMUM_SAFE_JAVASCRIPT_INTEGER..=MAXIMUM_SAFE_JAVASCRIPT_INTEGER)
                    .contains(&value)
            }) || number
                .as_u64()
                .is_some_and(|value| value > MAXIMUM_SAFE_JAVASCRIPT_INTEGER as u64);
            if outside_safe_range {
                Value::String(number.to_string())
            } else {
                Value::Number(number)
            }
        }
        Value::Array(values) => {
            Value::Array(values.into_iter().map(lossless_webview_value).collect())
        }
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key, lossless_webview_value(value)))
                .collect(),
        ),
        scalar => scalar,
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
    match request {
        ProviderOnboardingCommand::Bootstrap => provider_bootstrap(&state).await,
        ProviderOnboardingCommand::Start {
            surface_id,
            organization,
            administrative_email,
        } => reject_protected_provider_action((surface_id, organization, administrative_email)),
        ProviderOnboardingCommand::Resume { session_id }
        | ProviderOnboardingCommand::Renew { session_id }
        | ProviderOnboardingCommand::Cleanup { session_id }
        | ProviderOnboardingCommand::Cancel { session_id } => {
            reject_protected_provider_action(session_id)
        }
        ProviderOnboardingCommand::UnlockFallback { secret } => {
            reject_protected_provider_action(secret)
        }
        ProviderOnboardingCommand::LockFallback => reject_protected_provider_action(()),
        ProviderOnboardingCommand::SubmitSecret { session_id, secret } => {
            reject_protected_provider_action((session_id, secret))
        }
        ProviderOnboardingCommand::Activate {
            session_id,
            request,
        } => reject_protected_provider_action((session_id, request)),
    }
}

fn reject_protected_provider_action<T>(request: T) -> Result<Value, DesktopCommandError> {
    drop(request);
    Err(DesktopCommandError::new(
        "protected_provider_setup_required",
        "Continue this provider change in Market Squawk's protected local setup window.",
    ))
}

#[tauri::command]
pub(crate) fn open_official_provider_page(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let profiles = state
        .service_bootstrap
        .provider_profiles
        .as_array()
        .ok_or_else(DesktopCommandError::internal)?;
    let official_url = profiles
        .iter()
        .find(|profile| profile.get("id").and_then(Value::as_str) == Some(provider_id.as_str()))
        .and_then(|profile| profile.get("official_handoff_url"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            DesktopCommandError::invalid_request("The selected provider is not supported.")
        })?;
    let parsed = Url::parse(official_url).map_err(|_error| DesktopCommandError::internal())?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(DesktopCommandError::internal());
    }
    tauri_plugin_opener::open_url(parsed.as_str(), None::<&str>).map_err(|_error| {
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
        .service_bootstrap
        .provider_profiles
        .as_array()
        .is_some_and(|profiles| {
            profiles.iter().any(|profile| {
                profile.get("id").and_then(Value::as_str) == Some(provider_id.as_str())
            })
        });
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

async fn provider_bootstrap(state: &DesktopState) -> Result<Value, DesktopCommandError> {
    Ok(json!({
        "profiles": state.service_bootstrap.provider_profiles,
        "sessions": state.provider_sessions().await?,
        "encryptedFileFallback": state.service_bootstrap.encrypted_file_fallback,
    }))
}

pub(crate) fn map_application_client_error(error: ApplicationClientError) -> DesktopCommandError {
    let (code, message) = match error {
        ApplicationClientError::Rejected => (
            "operation_rejected",
            "The installed service rejected the dashboard request.",
        ),
        ApplicationClientError::Unavailable => (
            "service_unavailable",
            "The installed Market Squawk service is unavailable.",
        ),
        ApplicationClientError::Interrupted => (
            "request_interrupted",
            "The dashboard request was cancelled or exceeded its deadline.",
        ),
        ApplicationClientError::InvalidResponse => (
            "invalid_service_response",
            "The installed service returned an invalid response.",
        ),
    };
    DesktopCommandError::new(code, message)
}

fn map_installation_error(error: impl std::fmt::Display) -> DesktopCommandError {
    DesktopCommandError::new("installation_failed", error.to_string())
}

pub(crate) fn decode_application_result(response: &Value) -> Result<Value, DesktopCommandError> {
    let object = response
        .as_object()
        .ok_or_else(DesktopCommandError::internal)?;
    if object.len() != 2 || object.get("ok") != Some(&Value::Bool(true)) {
        return Err(DesktopCommandError::new(
            "operation_failed",
            "The Market Squawk service rejected the operation.",
        ));
    }
    object
        .get("value")
        .cloned()
        .ok_or_else(DesktopCommandError::internal)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{decode_application_result, lossless_webview_value};

    #[test]
    fn native_success_returns_the_application_result_envelope() {
        let response = json!({
            "ok": true,
            "value": {
                "data": {"status": "ready"},
                "metadata": {
                    "completeness": "complete",
                    "returnedItems": 1,
                    "availableItems": 1,
                    "sourceCoverage": null,
                    "dataQuality": null
                }
            }
        });

        assert_eq!(
            decode_application_result(&response).ok(),
            Some(json!({
                "data": {"status": "ready"},
                "metadata": {
                    "completeness": "complete",
                    "returnedItems": 1,
                    "availableItems": 1,
                    "sourceCoverage": null,
                    "dataQuality": null
                }
            }))
        );
    }

    #[test]
    fn webview_transport_preserves_financial_time_integers_exactly() {
        assert_eq!(
            lossless_webview_value(json!({
                "timestamp": 1_800_000_000_000_000_001_i64,
                "safeCount": 42
            })),
            json!({
                "timestamp": "1800000000000000001",
                "safeCount": 42
            })
        );
    }
}
