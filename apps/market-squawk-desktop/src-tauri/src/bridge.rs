//! Least-privilege Tauri bridge over the existing local application authorities.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::{Duration, Instant},
};

use market_squawk::service::BootstrapRequirement;
use market_squawk::{
    SchwabOAuthInstallationCapabilityError, SchwabOAuthInstallationTrustAction,
    SchwabOAuthInstallationTrustState,
};
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

use crate::analytical_controller::DesktopAnalyticalController;
use crate::contracts::{
    ApplicationInvocation, DesktopBootstrap, DesktopCommandError, DesktopServiceBootstrapCommand,
    DesktopServiceBootstrapRequirement, DesktopServiceBootstrapStatus, DesktopServiceReconnect,
    DesktopStartup, InstallationControlCommand, ProductCapability, ProviderOnboardingCommand,
    Readiness, ReadinessState,
};
use crate::events::DesktopEventSubscriptions;
use crate::mcp_clients::{DesktopMcpClientState, DesktopMcpRuntimeBinding};
use crate::service::{
    self, DesktopBootstrapAction, DesktopServiceAuthority, DesktopServiceBootstrap,
    DesktopServiceConnection, DesktopServiceError, DesktopServiceStartup,
};

const MAXIMUM_APPLICATION_ARGUMENT_BYTES: usize = 256 * 1024;
const MAXIMUM_DESKTOP_RESULT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_DESKTOP_RESULT_ITEMS: u64 = 1_000;
const MAXIMUM_SAFE_JAVASCRIPT_INTEGER: i64 = 9_007_199_254_740_991;
const MAXIMUM_OPERATION_BYTES: usize = 128;
const MAXIMUM_RESEARCH_COLLECTION_TOKENS: usize = 4_096;
const MAXIMUM_RESEARCH_PRODUCT_TOKENS: usize = 4_096;
const MAXIMUM_RESEARCH_PREPARATION_RECEIPTS: usize = 256;
const APPLICATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const GOVERNANCE_AUTHORIZATION_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAXIMUM_GOVERNANCE_AUTHORIZATIONS: usize = 256;
const SOURCE_SETUP_OPERATION: &str = "Source.Setup";
const SOURCE_STATUS_OPERATION: &str = "Source.GetStatus";
const SCHWAB_PROVIDER_ID: &str = "schwab.trader-api-market-data";

#[derive(Clone)]
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

enum PendingDesktopBootstrap {
    Initial {
        service: DesktopServiceBootstrap,
        context: DesktopCompositionContext,
    },
    Reconnect {
        service: DesktopServiceBootstrap,
        expected_runtime: RuntimeIdentity,
    },
}

pub(crate) struct DesktopBootstrapState {
    pending: tokio::sync::Mutex<Option<PendingDesktopBootstrap>>,
    reconnect_gate: tokio::sync::Mutex<()>,
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
                Some(PendingDesktopBootstrap::Initial { service, context })
            }
        };
        Ok(Self {
            pending: tokio::sync::Mutex::new(pending),
            reconnect_gate: tokio::sync::Mutex::new(()),
        })
    }

    async fn status(&self) -> Result<DesktopServiceBootstrapStatus, DesktopCommandError> {
        let pending = self.pending.lock().await;
        let pending = pending.as_ref().ok_or_else(DesktopCommandError::internal)?;
        Ok(pending_bootstrap_status(pending))
    }

    async fn pending_status(&self) -> Option<DesktopServiceBootstrapStatus> {
        self.pending
            .lock()
            .await
            .as_ref()
            .map(pending_bootstrap_status)
    }
}

fn pending_bootstrap_status(pending: &PendingDesktopBootstrap) -> DesktopServiceBootstrapStatus {
    let service = match pending {
        PendingDesktopBootstrap::Initial { service, .. }
        | PendingDesktopBootstrap::Reconnect { service, .. } => service,
    };
    DesktopServiceBootstrapStatus::required(match service.requirement() {
        BootstrapRequirement::EncryptedFallbackLocked => {
            DesktopServiceBootstrapRequirement::EncryptedFallbackLocked
        }
        BootstrapRequirement::ForegroundKeyringRetry => {
            DesktopServiceBootstrapRequirement::ForegroundKeyringRetry
        }
    })
}

fn manage_ready_desktop(
    app: &tauri::AppHandle,
    connection: DesktopServiceConnection,
    context: &DesktopCompositionContext,
) -> Result<(), DesktopCommandError> {
    if app.try_state::<DesktopState>().is_some() {
        return Err(DesktopCommandError::internal());
    }
    let state = DesktopState::try_new(connection, context.clone())?;
    if !app.manage(state) {
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
    domain: String,
    authorization: String,
    read_only: bool,
    input_schema: Value,
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
            required_string(operation, "/description")?;
            operation
                .pointer("/effects/destructive")
                .and_then(Value::as_bool)
                .ok_or_else(DesktopCommandError::internal)?;
            parsed_operations.push(ServiceOperation {
                name,
                domain: required_string(operation, "/contract/domain")?.to_owned(),
                authorization,
                read_only,
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
    current: RwLock<Arc<DesktopGeneration>>,
    webview_admitted_runtime: RwLock<RuntimeIdentity>,
    service: Arc<DesktopServiceAuthority>,
    context: DesktopCompositionContext,
    restart_program: OnceLock<PathBuf>,
}

pub(crate) struct DesktopGeneration {
    application: Arc<LoopbackApplicationClient>,
    service_bootstrap: ServiceBootstrapSnapshot,
    data_root: PathBuf,
    cancellation: CancellationToken,
    governance_authorizations: Mutex<HashMap<Uuid, NativeGovernanceAuthorization>>,
    research_collections: Mutex<ResearchCollectionTokens>,
    research_preparation_choices: Mutex<StableProductTokens>,
    research_preparation_receipts: Mutex<OneUseProductTokens>,
    research_activities: Mutex<StableProductTokens>,
    mcp_clients: Arc<DesktopMcpClientState>,
    analytical_controller: Arc<DesktopAnalyticalController>,
    analytical_gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Default)]
struct ResearchCollectionTokens {
    by_dataset: HashMap<String, Uuid>,
    by_token: HashMap<Uuid, String>,
}

#[derive(Default)]
struct StableProductTokens {
    by_key: HashMap<String, Uuid>,
    by_token: HashMap<Uuid, Value>,
}

impl StableProductTokens {
    fn register(
        &mut self,
        key: String,
        authority: Value,
        capacity_message: &'static str,
    ) -> Result<Uuid, DesktopCommandError> {
        if let Some(token) = self.by_key.get(&key).copied() {
            self.by_token.insert(token, authority);
            return Ok(token);
        }
        if self.by_key.len() >= MAXIMUM_RESEARCH_PRODUCT_TOKENS {
            return Err(DesktopCommandError::new(
                "resource_exhausted",
                capacity_message,
            ));
        }
        self.by_key
            .try_reserve(1)
            .map_err(|_error| DesktopCommandError::internal())?;
        self.by_token
            .try_reserve(1)
            .map_err(|_error| DesktopCommandError::internal())?;
        let token = next_opaque_token(&self.by_token);
        self.by_key.insert(key, token);
        self.by_token.insert(token, authority);
        Ok(token)
    }

    fn resolve(
        &self,
        token: Uuid,
        missing_message: &'static str,
    ) -> Result<Value, DesktopCommandError> {
        self.by_token
            .get(&token)
            .cloned()
            .ok_or_else(|| DesktopCommandError::new("not_found", missing_message))
    }
}

#[derive(Default)]
struct OneUseProductTokens {
    by_token: HashMap<Uuid, Value>,
}

impl OneUseProductTokens {
    fn register(&mut self, authority: Value) -> Result<Uuid, DesktopCommandError> {
        if self.by_token.len() >= MAXIMUM_RESEARCH_PREPARATION_RECEIPTS {
            return Err(DesktopCommandError::new(
                "resource_exhausted",
                "Too many research preparations are waiting for review. Finish or reopen Research before preparing another.",
            ));
        }
        self.by_token
            .try_reserve(1)
            .map_err(|_error| DesktopCommandError::internal())?;
        let token = next_opaque_token(&self.by_token);
        self.by_token.insert(token, authority);
        Ok(token)
    }

    fn consume(&mut self, token: Uuid) -> Result<Value, DesktopCommandError> {
        self.by_token.remove(&token).ok_or_else(|| {
            DesktopCommandError::new(
                "not_found",
                "That research preparation is no longer available. Review the preparation again.",
            )
        })
    }
}

fn next_opaque_token<T>(entries: &HashMap<Uuid, T>) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if !entries.contains_key(&candidate) {
            return candidate;
        }
    }
}

impl ResearchCollectionTokens {
    fn register(&mut self, dataset: &str) -> Result<Uuid, DesktopCommandError> {
        if let Some(token) = self.by_dataset.get(dataset) {
            return Ok(*token);
        }
        if self.by_dataset.len() >= MAXIMUM_RESEARCH_COLLECTION_TOKENS {
            return Err(DesktopCommandError::new(
                "resource_exhausted",
                "Too many research collections are open. Reopen the workspace and narrow the collection list.",
            ));
        }
        self.by_dataset
            .try_reserve(1)
            .map_err(|_error| DesktopCommandError::internal())?;
        self.by_token
            .try_reserve(1)
            .map_err(|_error| DesktopCommandError::internal())?;
        let token = next_opaque_token(&self.by_token);
        self.by_dataset.insert(dataset.to_owned(), token);
        self.by_token.insert(token, dataset.to_owned());
        Ok(token)
    }

    fn resolve(&self, token: Uuid) -> Result<String, DesktopCommandError> {
        self.by_token.get(&token).cloned().ok_or_else(|| {
            DesktopCommandError::new(
                "not_found",
                "That research collection is no longer open. Refresh Research and try again.",
            )
        })
    }
}

struct PreparedDesktopReplacement {
    generation: Arc<DesktopGeneration>,
    mcp_binding: DesktopMcpRuntimeBinding,
    bootstrap: DesktopBootstrap,
}

struct NativeGovernanceAuthorization {
    window_label: String,
    preview_id: Uuid,
    ticket_id: Uuid,
    retained_until: Instant,
}

impl DesktopState {
    pub(crate) fn try_new(
        connection: DesktopServiceConnection,
        context: DesktopCompositionContext,
    ) -> Result<Self, DesktopCommandError> {
        let service = Arc::clone(&connection.authority);
        let generation = DesktopGeneration::try_new(connection, &context)?;
        let runtime = generation.runtime();
        Ok(Self {
            current: RwLock::new(Arc::new(generation)),
            webview_admitted_runtime: RwLock::new(runtime),
            service,
            context,
            restart_program: OnceLock::new(),
        })
    }

    pub(crate) fn generation(&self) -> Result<Arc<DesktopGeneration>, DesktopCommandError> {
        let generation = self.current_generation()?;
        let admitted = self
            .webview_admitted_runtime
            .read()
            .map_err(|_error| DesktopCommandError::internal())?;
        admit_webview_runtime(*admitted, generation.runtime())?;
        Ok(generation)
    }

    pub(crate) fn current_generation(&self) -> Result<Arc<DesktopGeneration>, DesktopCommandError> {
        self.current
            .read()
            .map(|generation| Arc::clone(&generation))
            .map_err(|_error| DesktopCommandError::internal())
    }

    pub(crate) fn admit_current(
        &self,
        generation: &Arc<DesktopGeneration>,
    ) -> Result<(), DesktopCommandError> {
        let current = self
            .current
            .read()
            .map_err(|_error| DesktopCommandError::internal())?;
        if Arc::ptr_eq(&current, generation) {
            Ok(())
        } else {
            Err(service_generation_changed())
        }
    }

    async fn prepare_replacement(
        &self,
        current: &Arc<DesktopGeneration>,
        expected: RuntimeIdentity,
        connection: DesktopServiceConnection,
    ) -> Result<PreparedDesktopReplacement, DesktopCommandError> {
        if !Arc::ptr_eq(&self.service, &connection.authority) {
            return Err(DesktopCommandError::internal());
        }
        let replacement =
            DesktopGeneration::try_replacement(connection, &self.context, current, expected)?;
        let mcp_binding = DesktopMcpClientState::prepare_runtime_binding(
            replacement.runtime(),
            replacement.service_bootstrap.mcp_endpoint_identity.clone(),
            replacement
                .service_bootstrap
                .claude_code_credential_identity
                .clone(),
            replacement
                .service_bootstrap
                .codex_credential_identity
                .clone(),
        )
        .map_err(|_error| DesktopCommandError::internal())?;
        let generation = Arc::new(replacement);
        let bootstrap = generation
            .prepare_bootstrap(&self.context.installation_status)
            .await?;
        self.admit_current(current)?;
        Ok(PreparedDesktopReplacement {
            generation,
            mcp_binding,
            bootstrap,
        })
    }

    fn replace_generation(
        &self,
        expected: RuntimeIdentity,
        replacement: Arc<DesktopGeneration>,
    ) -> Result<Arc<DesktopGeneration>, DesktopCommandError> {
        let mut current = self
            .current
            .write()
            .map_err(|_error| DesktopCommandError::internal())?;
        admit_replacement(
            current.runtime(),
            &current.data_root,
            expected,
            replacement.runtime(),
            &replacement.data_root,
        )?;
        Ok(std::mem::replace(&mut current, replacement))
    }

    pub(crate) fn acknowledge_webview_runtime(
        &self,
        generation: &Arc<DesktopGeneration>,
        runtime: RuntimeIdentity,
    ) -> Result<(), DesktopCommandError> {
        self.admit_current(generation)?;
        if generation.runtime() != runtime {
            return Err(service_generation_changed());
        }
        let mut admitted = self
            .webview_admitted_runtime
            .write()
            .map_err(|_error| DesktopCommandError::internal())?;
        self.admit_current(generation)?;
        *admitted = runtime;
        Ok(())
    }

    pub(crate) fn service_authority(&self) -> Arc<DesktopServiceAuthority> {
        Arc::clone(&self.service)
    }

    fn schedule_restart(&self, program: PathBuf) -> Result<(), DesktopCommandError> {
        self.restart_program.set(program).map_err(|_error| {
            DesktopCommandError::new(
                "installation_restart_pending",
                "Market Squawk is already restarting into the selected release.",
            )
        })
    }

    pub(crate) fn scheduled_restart_program(&self) -> Option<PathBuf> {
        self.restart_program.get().cloned()
    }

    pub(crate) fn begin_shutdown(&self) {
        if let Ok(generation) = self.current_generation() {
            generation.begin_shutdown();
        }
    }

    pub(crate) async fn finish_shutdown(&self) {
        self.begin_shutdown();
    }
}

fn admit_replacement(
    current: RuntimeIdentity,
    current_data_root: &Path,
    expected: RuntimeIdentity,
    replacement: RuntimeIdentity,
    replacement_data_root: &Path,
) -> Result<(), DesktopCommandError> {
    if current != expected
        || replacement.installation_id() != current.installation_id()
        || replacement.service_generation() <= current.service_generation()
    {
        Err(DesktopCommandError::new(
            "service_reconnect_rejected",
            "The installed service reconnect did not prove a newer runtime for this installation.",
        ))
    } else if replacement.workspace_id() != current.workspace_id()
        || replacement_data_root != current_data_root
    {
        Err(DesktopCommandError::new(
            "service_relaunch_required",
            "The installed service opened a different workspace. Relaunch Market Squawk to admit that workspace explicitly.",
        ))
    } else {
        Ok(())
    }
}

fn service_generation_changed() -> DesktopCommandError {
    DesktopCommandError::new(
        "service_generation_changed",
        "The installed service changed while this request was running. Review the refreshed workspace before retrying.",
    )
}

fn admit_webview_runtime(
    admitted: RuntimeIdentity,
    current: RuntimeIdentity,
) -> Result<(), DesktopCommandError> {
    if admitted == current {
        Ok(())
    } else {
        Err(service_generation_changed())
    }
}

impl DesktopGeneration {
    fn try_new(
        connection: DesktopServiceConnection,
        context: &DesktopCompositionContext,
    ) -> Result<Self, DesktopCommandError> {
        let service_bootstrap = ServiceBootstrapSnapshot::try_from(connection.bootstrap)?;
        let data_root = resolve_workspace_data_root(
            &service_bootstrap,
            &context.configured_data_root,
            &context.service_data_root,
        )?;
        let local_paths = LocalPaths::open_existing(&data_root)
            .map_err(|_error| DesktopCommandError::internal())?;
        let analytical_controller = DesktopAnalyticalController::try_open(
            &local_paths,
            service_bootstrap.runtime.workspace_id().as_uuid(),
        )?;
        let mcp_clients = DesktopMcpClientState::try_new(
            &local_paths,
            context.relay_program.clone(),
            &context.service_data_root,
            service_bootstrap.runtime,
            service_bootstrap.mcp_endpoint_identity.clone(),
            service_bootstrap.claude_code_credential_identity.clone(),
            service_bootstrap.codex_credential_identity.clone(),
        )
        .map_err(|_error| DesktopCommandError::internal())?;
        Ok(Self {
            application: Arc::new(connection.application),
            service_bootstrap,
            data_root,
            cancellation: CancellationToken::new(),
            governance_authorizations: Mutex::new(HashMap::new()),
            research_collections: Mutex::new(ResearchCollectionTokens::default()),
            research_preparation_choices: Mutex::new(StableProductTokens::default()),
            research_preparation_receipts: Mutex::new(OneUseProductTokens::default()),
            research_activities: Mutex::new(StableProductTokens::default()),
            mcp_clients: Arc::new(mcp_clients),
            analytical_controller: Arc::new(analytical_controller),
            analytical_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    fn try_replacement(
        connection: DesktopServiceConnection,
        context: &DesktopCompositionContext,
        current: &Arc<Self>,
        expected: RuntimeIdentity,
    ) -> Result<Self, DesktopCommandError> {
        let service_bootstrap = ServiceBootstrapSnapshot::try_from(connection.bootstrap)?;
        let data_root = resolve_workspace_data_root(
            &service_bootstrap,
            &context.configured_data_root,
            &context.service_data_root,
        )?;
        admit_replacement(
            current.runtime(),
            &current.data_root,
            expected,
            service_bootstrap.runtime,
            &data_root,
        )?;
        Ok(Self {
            application: Arc::new(connection.application),
            service_bootstrap,
            data_root,
            cancellation: CancellationToken::new(),
            governance_authorizations: Mutex::new(HashMap::new()),
            research_collections: Mutex::new(ResearchCollectionTokens::default()),
            research_preparation_choices: Mutex::new(StableProductTokens::default()),
            research_preparation_receipts: Mutex::new(OneUseProductTokens::default()),
            research_activities: Mutex::new(StableProductTokens::default()),
            mcp_clients: Arc::clone(&current.mcp_clients),
            analytical_controller: Arc::clone(&current.analytical_controller),
            analytical_gate: Arc::clone(&current.analytical_gate),
        })
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

    pub(crate) fn register_research_collection(
        &self,
        dataset: &str,
    ) -> Result<Uuid, DesktopCommandError> {
        self.research_collections
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .register(dataset)
    }

    pub(crate) fn resolve_research_collection(
        &self,
        collection: Uuid,
    ) -> Result<String, DesktopCommandError> {
        self.research_collections
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .resolve(collection)
    }

    pub(crate) fn register_research_preparation_choice(
        &self,
        key: String,
        authority: Value,
    ) -> Result<Uuid, DesktopCommandError> {
        self.research_preparation_choices
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .register(
                key,
                authority,
                "Too many research choices are open. Reopen Research and narrow the available choices.",
            )
    }

    pub(crate) fn resolve_research_preparation_choice(
        &self,
        choice: Uuid,
    ) -> Result<Value, DesktopCommandError> {
        self.research_preparation_choices
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .resolve(
                choice,
                "That research choice is no longer available. Refresh Research and try again.",
            )
    }

    pub(crate) fn register_research_preparation_receipt(
        &self,
        authority: Value,
    ) -> Result<Uuid, DesktopCommandError> {
        self.research_preparation_receipts
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .register(authority)
    }

    pub(crate) fn consume_research_preparation_receipt(
        &self,
        receipt: Uuid,
    ) -> Result<Value, DesktopCommandError> {
        self.research_preparation_receipts
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .consume(receipt)
    }

    pub(crate) fn register_research_activity(
        &self,
        key: String,
        authority: Value,
    ) -> Result<Uuid, DesktopCommandError> {
        self.research_activities
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .register(
                key,
                authority,
                "Too much background activity is open. Reopen Research to refresh the activity list.",
            )
    }

    pub(crate) fn resolve_research_activity(
        &self,
        activity: Uuid,
    ) -> Result<Value, DesktopCommandError> {
        self.research_activities
            .lock()
            .map_err(|_error| DesktopCommandError::internal())?
            .resolve(
                activity,
                "That background activity is no longer available. Refresh Research and try again.",
            )
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

    pub(crate) fn mcp_clients(&self) -> &DesktopMcpClientState {
        self.mcp_clients.as_ref()
    }

    pub(crate) fn analytical_controller(&self) -> &DesktopAnalyticalController {
        self.analytical_controller.as_ref()
    }

    pub(crate) async fn analytical_retirement_fence(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.analytical_gate).lock_owned().await
    }

    async fn bootstrap(
        self: &Arc<Self>,
        state: &DesktopState,
    ) -> Result<DesktopBootstrap, DesktopCommandError> {
        Ok(self.present_bootstrap(&state.context.installation_status))
    }

    async fn prepare_bootstrap(
        self: &Arc<Self>,
        installation_status: &InstallStatus,
    ) -> Result<DesktopBootstrap, DesktopCommandError> {
        Ok(self.present_bootstrap(installation_status))
    }

    fn present_bootstrap(&self, installation_status: &InstallStatus) -> DesktopBootstrap {
        let capabilities = self
            .service_bootstrap
            .operations
            .iter()
            .filter_map(|operation| ProductCapability::for_operation(&operation.name))
            .collect::<BTreeSet<_>>()
            .into_iter()
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
        let installation = if installation_status.is_installed() && installation_status.is_healthy()
        {
            Readiness::new(
                ReadinessState::Ready,
                "Verified",
                "The complete installed release and every retained component passed verification.",
            )
        } else if installation_status.is_installed() {
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
        DesktopBootstrap::new(
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
            capabilities,
        )
    }

    async fn provider_sessions(
        self: &Arc<Self>,
        state: &DesktopState,
    ) -> Result<Vec<Value>, DesktopCommandError> {
        let result = invoke_application(
            ApplicationInvocation {
                operation: SOURCE_STATUS_OPERATION.to_owned(),
                arguments: Map::new(),
            },
            state,
            self,
            InvocationAuthority::ReadOnly,
        )
        .await?;
        Self::parse_provider_sessions(result)
    }

    fn parse_provider_sessions(result: Value) -> Result<Vec<Value>, DesktopCommandError> {
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
    let _reconnect_guard = bootstrap_state.reconnect_gate.lock().await;
    if let Some(status) = bootstrap_state.pending_status().await {
        return Ok(DesktopStartup::BootstrapRequired(status));
    }
    if let Some(state) = app.try_state::<DesktopState>() {
        let generation = state.current_generation()?;
        let bootstrap = generation.bootstrap(&state).await?;
        state.admit_current(&generation)?;
        return Ok(DesktopStartup::Ready(Box::new(bootstrap)));
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
    subscriptions: State<'_, DesktopEventSubscriptions>,
) -> Result<(), DesktopCommandError> {
    if window.label() != "main" {
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
    let _reconnect_guard = bootstrap_state.reconnect_gate.lock().await;
    let mut pending_guard = bootstrap_state.pending.lock().await;
    let pending = pending_guard.as_ref().ok_or_else(|| {
        DesktopCommandError::new(
            "service_bootstrap_unavailable",
            "The local service bootstrap action is no longer available.",
        )
    })?;
    let service = match pending {
        PendingDesktopBootstrap::Initial { service, .. }
        | PendingDesktopBootstrap::Reconnect { service, .. } => service,
    };
    let connection = service::complete_bootstrap(service, action)
        .await
        .map_err(|_error| {
            DesktopCommandError::new(
                "service_bootstrap_failed",
                "The local service could not complete credential bootstrap and reconnect.",
            )
        })?;
    match pending_guard
        .as_ref()
        .ok_or_else(DesktopCommandError::internal)?
    {
        PendingDesktopBootstrap::Initial { context, .. } => {
            if app.try_state::<DesktopState>().is_some() {
                return Err(DesktopCommandError::new(
                    "service_bootstrap_unavailable",
                    "The initial local service bootstrap is no longer current.",
                ));
            }
            manage_ready_desktop(&app, connection, &context)?;
        }
        PendingDesktopBootstrap::Reconnect {
            expected_runtime, ..
        } => {
            let state = app.try_state::<DesktopState>().ok_or_else(|| {
                DesktopCommandError::new(
                    "service_bootstrap_unavailable",
                    "The reconnecting desktop service state is no longer available.",
                )
            })?;
            if let Err(error) =
                commit_reconnected_generation(&state, &subscriptions, *expected_runtime, connection)
                    .await
            {
                // Foreground recovery already produced a connection. The old generation remains
                // current on every preparation failure, so retaining a consumed bootstrap action
                // would be misleading; a later reconnect must start fresh.
                let _cleared = pending_guard.take();
                return Err(error);
            }
        }
    }
    let _completed = pending_guard
        .take()
        .ok_or_else(DesktopCommandError::internal)?;
    Ok(())
}

#[tauri::command]
pub(crate) async fn desktop_service_reconnect(
    request: DesktopServiceReconnect,
    window: tauri::Window,
    state: State<'_, DesktopState>,
    bootstrap_state: State<'_, DesktopBootstrapState>,
    subscriptions: State<'_, DesktopEventSubscriptions>,
) -> Result<DesktopStartup, DesktopCommandError> {
    if window.label() != "main" {
        return Err(DesktopCommandError::new(
            "service_reconnect_unavailable",
            "The installed service reconnect is not available for this window.",
        ));
    }
    let _reconnect_guard = bootstrap_state.reconnect_gate.lock().await;
    if bootstrap_state.pending.lock().await.is_some() {
        return Err(DesktopCommandError::new(
            "service_reconnect_pending",
            "The installed service is already waiting for foreground recovery.",
        ));
    }
    let generation = state.current_generation()?;
    if generation.runtime() != request.expected_runtime() {
        return Err(DesktopCommandError::new(
            "service_reconnect_stale",
            "The reconnect request belongs to an earlier desktop service generation.",
        ));
    }
    let expected_runtime = generation.runtime();
    let startup = service::reconnect_or_start(&state.service_authority())
        .await
        .map_err(|_error| {
            DesktopCommandError::new(
                "service_reconnect_failed",
                "The installed service could not reconnect or restart within the local deadline.",
            )
        })?;
    match startup {
        DesktopServiceStartup::Ready(connection) => {
            let bootstrap = commit_reconnected_generation(
                &state,
                &subscriptions,
                expected_runtime,
                *connection,
            )
            .await?;
            Ok(DesktopStartup::Ready(Box::new(bootstrap)))
        }
        DesktopServiceStartup::BootstrapRequired(service) => {
            let status = DesktopServiceBootstrapStatus::required(match service.requirement() {
                BootstrapRequirement::EncryptedFallbackLocked => {
                    DesktopServiceBootstrapRequirement::EncryptedFallbackLocked
                }
                BootstrapRequirement::ForegroundKeyringRetry => {
                    DesktopServiceBootstrapRequirement::ForegroundKeyringRetry
                }
            });
            *bootstrap_state.pending.lock().await = Some(PendingDesktopBootstrap::Reconnect {
                service,
                expected_runtime,
            });
            Ok(DesktopStartup::BootstrapRequired(status))
        }
    }
}

async fn commit_reconnected_generation(
    state: &DesktopState,
    subscriptions: &DesktopEventSubscriptions,
    expected_runtime: RuntimeIdentity,
    connection: DesktopServiceConnection,
) -> Result<DesktopBootstrap, DesktopCommandError> {
    let old = state.current_generation()?;
    let retirement_fence = old.mcp_clients().retirement_fence().await;
    state.admit_current(&old)?;
    let analytical_fence = old.analytical_retirement_fence().await;
    state.admit_current(&old)?;
    let prepared = state
        .prepare_replacement(&old, expected_runtime, connection)
        .await?;
    let retired = subscriptions
        .stop_clear_and_replace(|| {
            old.mcp_clients().commit_runtime_binding_while_fenced(
                &retirement_fence,
                prepared.mcp_binding,
                || state.replace_generation(expected_runtime, Arc::clone(&prepared.generation)),
            )
        })
        .await?;
    retired.begin_shutdown();
    drop(retirement_fence);
    drop(analytical_fence);
    drop(retired);
    Ok(prepared.bootstrap)
}

#[tauri::command]
pub(crate) async fn installation_control(
    request: InstallationControlCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    app: tauri::AppHandle,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    if request.requires_confirmation() && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the installation change before continuing.",
        ));
    }
    let root = state.context.installation_root.clone();
    match request {
        InstallationControlCommand::Status => {
            let current = blocking_installation(move || installation_status(&root)).await?;
            state.admit_current(&generation)?;
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
                    state.admit_current(&generation)?;
                    return Ok(json!({
                        "action": "update",
                        "status": current,
                        "receipt": null,
                        "restartRequired": false,
                    }));
                }
                Err(error) => return Err(map_installation_error(error)),
            };
            state.admit_current(&generation)?;
            let current = prepare_installation_restart(root, &state, &generation).await?;
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
            state.admit_current(&generation)?;
            let current = blocking_installation(move || installation_status(&root)).await?;
            state.admit_current(&generation)?;
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
            state.admit_current(&generation)?;
            let current = prepare_installation_restart(root, &state, &generation).await?;
            request_installation_restart(&app);
            Ok(json!({
                "action": "rollback",
                "status": current,
                "receipt": receipt,
                "restartRequired": true,
            }))
        }
        InstallationControlCommand::Uninstall => {
            let result = uninstall_programs(root, &app).await;
            state.admit_current(&generation)?;
            result
        }
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
    generation: &Arc<DesktopGeneration>,
) -> Result<InstallStatus, DesktopCommandError> {
    let snapshot =
        blocking_installation(move || program_install_snapshot(&root, ProgramName::Desktop))
            .await?;
    state.admit_current(generation)?;
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
    generation: &Arc<DesktopGeneration>,
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
        generation,
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
    generation: &Arc<DesktopGeneration>,
    authority: InvocationAuthority,
) -> Result<Value, DesktopCommandError> {
    if matches!(
        authority,
        InvocationAuthority::ExactConfirmed(_) | InvocationAuthority::RiskMediated(_)
    ) {
        arguments.insert("confirm".to_owned(), Value::Bool(true));
    }
    invoke_service_operation(state, generation, operation, arguments, authority, false).await
}

async fn invoke_service_operation(
    state: &DesktopState,
    generation: &Arc<DesktopGeneration>,
    operation: &str,
    arguments: Map<String, Value>,
    authority: InvocationAuthority,
    apply_desktop_result_limits: bool,
) -> Result<Value, DesktopCommandError> {
    state.admit_current(generation)?;
    let result = invoke_generation_operation(
        generation,
        operation,
        arguments,
        authority,
        apply_desktop_result_limits,
    )
    .await?;
    state.admit_current(generation)?;
    Ok(result)
}

async fn invoke_generation_operation(
    generation: &Arc<DesktopGeneration>,
    operation: &str,
    mut arguments: Map<String, Value>,
    authority: InvocationAuthority,
    apply_desktop_result_limits: bool,
) -> Result<Value, DesktopCommandError> {
    let descriptor = generation
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
    let response = generation
        .application
        .invoke_operation(
            request_id,
            operation,
            arguments,
            APPLICATION_REQUEST_TIMEOUT,
            generation.cancellation(),
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
    let generation = state.generation()?;
    if request.requires_confirmation() && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the provider change before continuing.",
        ));
    }
    match request {
        ProviderOnboardingCommand::Bootstrap => provider_bootstrap(&state, &generation).await,
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
    let generation = state.generation()?;
    let profiles = generation
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
    })?;
    state.admit_current(&generation)
}

#[tauri::command]
pub(crate) async fn open_protected_provider_setup(
    provider_id: String,
    state: State<'_, DesktopState>,
) -> Result<(), DesktopCommandError> {
    let generation = state.generation()?;
    let supported = generation
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
    if provider_id == SCHWAB_PROVIDER_ID {
        let authority = state.service_authority();
        let status = authority
            .schwab_oauth_installation_trust(
                SchwabOAuthInstallationTrustAction::Status,
                generation.cancellation(),
            )
            .await
            .map_err(map_schwab_callback_trust_error)?;
        let trust = match status {
            SchwabOAuthInstallationTrustState::Trusted => status,
            SchwabOAuthInstallationTrustState::SetupRequired => authority
                .schwab_oauth_installation_trust(
                    SchwabOAuthInstallationTrustAction::Enroll,
                    generation.cancellation(),
                )
                .await
                .map_err(map_schwab_callback_trust_error)?,
            SchwabOAuthInstallationTrustState::RepairRequired => {
                return Err(DesktopCommandError::new(
                    "schwab_callback_repair_required",
                    "Schwab's private local callback needs repair before setup can continue.",
                ));
            }
            SchwabOAuthInstallationTrustState::Unsupported => {
                return Err(DesktopCommandError::new(
                    "schwab_callback_unsupported",
                    "Secure Schwab browser setup is not available on this operating system yet.",
                ));
            }
        };
        if trust != SchwabOAuthInstallationTrustState::Trusted {
            return Err(DesktopCommandError::new(
                "schwab_callback_trust_required",
                "Schwab setup needs approval for Market Squawk's private local callback.",
            ));
        }
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
        &generation,
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
    })?;
    state.admit_current(&generation)
}

fn map_schwab_callback_trust_error(error: DesktopServiceError) -> DesktopCommandError {
    match error {
        DesktopServiceError::SchwabOAuthTrust(
            SchwabOAuthInstallationCapabilityError::TrustCancelled,
        ) => DesktopCommandError::new(
            "schwab_callback_cancelled",
            "Schwab setup was cancelled before the private local callback was approved.",
        ),
        DesktopServiceError::SchwabOAuthTrust(
            SchwabOAuthInstallationCapabilityError::TrustTimeout,
        ) => DesktopCommandError::new(
            "schwab_callback_timeout",
            "Schwab setup approval timed out. Try again when you are ready.",
        ),
        DesktopServiceError::SchwabOAuthTrust(
            SchwabOAuthInstallationCapabilityError::InvalidTlsIdentity,
        ) => DesktopCommandError::new(
            "schwab_callback_repair_required",
            "Schwab's private local callback needs repair before setup can continue.",
        ),
        DesktopServiceError::SchwabOAuthTrust(
            SchwabOAuthInstallationCapabilityError::UnsupportedPlatform,
        ) => DesktopCommandError::new(
            "schwab_callback_unsupported",
            "Secure Schwab browser setup is not available on this operating system yet.",
        ),
        DesktopServiceError::SchwabOAuthTrust(
            SchwabOAuthInstallationCapabilityError::TrustEnrollment,
        ) => DesktopCommandError::new(
            "schwab_callback_trust_required",
            "Schwab setup needs approval for Market Squawk's private local callback.",
        ),
        _ => DesktopCommandError::internal(),
    }
}

async fn provider_bootstrap(
    state: &DesktopState,
    generation: &Arc<DesktopGeneration>,
) -> Result<Value, DesktopCommandError> {
    let sessions = generation.provider_sessions(state).await?;
    let supports = |operation: &str| {
        generation
            .service_bootstrap
            .operations
            .iter()
            .any(|candidate| candidate.name == operation)
    };
    state.admit_current(generation)?;
    Ok(json!({
        "profiles": generation.service_bootstrap.provider_profiles,
        "sessions": sessions,
        "encryptedFileFallback": generation.service_bootstrap.encrypted_file_fallback,
        "capabilities": {
            "credentialImport": supports("Source.ImportCredentialBundle"),
            "health": supports("Source.GetHealth"),
            "manifestEvidence": supports("Research.GetManifest"),
            "researchIngestion": supports("Source.GetStatus")
                && supports("Source.ListObjects")
                && supports("Source.Discover")
                && supports("Research.StartIngestSource"),
            "status": supports("Source.GetStatus"),
            "coverage": supports("Source.GetCoverage"),
        },
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
    use std::path::Path;

    use market_squawk_runtime::{InstallationId, RuntimeIdentity, ServiceGeneration, WorkspaceId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        admit_replacement, admit_webview_runtime, decode_application_result, lossless_webview_value,
    };

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

    #[test]
    fn reconnect_requires_the_existing_workspace_root_and_a_new_webview_handoff()
    -> Result<(), Box<dyn std::error::Error>> {
        let current = runtime_identity(1, 2, 7)?;
        let replacement = runtime_identity(1, 2, 8)?;
        let other_workspace = runtime_identity(1, 3, 8)?;
        let root = Path::new("/canonical/workspace");

        assert!(admit_replacement(current, root, current, replacement, root).is_ok());
        assert!(admit_replacement(current, root, current, other_workspace, root).is_err());
        assert!(
            admit_replacement(
                current,
                root,
                current,
                replacement,
                Path::new("/other/workspace"),
            )
            .is_err()
        );
        assert!(admit_webview_runtime(current, replacement).is_err());
        assert!(admit_webview_runtime(replacement, replacement).is_ok());
        Ok(())
    }

    fn runtime_identity(
        installation: u128,
        workspace: u128,
        generation: u64,
    ) -> Result<RuntimeIdentity, Box<dyn std::error::Error>> {
        Ok(RuntimeIdentity::try_new(
            InstallationId::try_from_uuid(Uuid::from_u128(installation))?,
            WorkspaceId::try_from_uuid(Uuid::from_u128(workspace))?,
            ServiceGeneration::try_new(generation)?,
        )?)
    }
}
