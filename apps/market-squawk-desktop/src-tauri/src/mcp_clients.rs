//! Bounded native controls for owned Claude Code and Codex MCP registrations.
//!
//! Official client CLIs, relay paths, endpoint identities, and credential identities remain on
//! the Rust side of the Tauri boundary. The WebView receives only secret-free state, receipt
//! digests, available actions, and protocol-verification evidence.

use std::{
    collections::BTreeMap,
    fmt,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use market_squawk::mcp::clients::{
    MCP_PROTOCOL_VERSION, McpClientKind, McpClientRegistrationError, McpClientRegistrationManager,
    McpClientState, McpClientStatus, McpProtocolVerification, McpRegistrationAuthority,
};
use market_squawk_platform::LocalPaths;
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::RequestId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use tauri::State;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::{
    bridge::{DesktopState, decode_application_result, map_application_client_error},
    contracts::DesktopCommandError,
};

const MCP_STATUS_TIMEOUT: Duration = Duration::from_secs(120);
const MCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(180);
const MCP_REPAIR_TIMEOUT: Duration = Duration::from_secs(240);
const MCP_DISCONNECT_TIMEOUT: Duration = Duration::from_secs(180);
const MCP_VERIFY_TIMEOUT: Duration = Duration::from_secs(220);
const MCP_SERVICE_TIMEOUT: Duration = Duration::from_secs(30);
const MCP_TRANSPORT: &str = "stdio_relay";
const MCP_STATUS_OPERATION: &str = "Mcp.GetRuntimeStatus";
const MCP_ACTIVATE_OPERATION: &str = "Mcp.ActivateCredential";
const MCP_ROTATE_OPERATION: &str = "Mcp.RotateCredential";
const MCP_REVOKE_OPERATION: &str = "Mcp.RevokeCredential";

/// Sole native mutation authority for supported MCP client registrations.
pub(crate) struct DesktopMcpClientState {
    runtime: RuntimeIdentity,
    control_gate: AsyncMutex<()>,
    inner: Arc<Mutex<DesktopMcpClientAuthority>>,
}

impl fmt::Debug for DesktopMcpClientState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopMcpClientState")
            .field("runtime", &self.runtime)
            .field("inner", &"[MCP CLIENT REGISTRATION AUTHORITY]")
            .finish()
    }
}

impl DesktopMcpClientState {
    /// Opens the owned receipt authority and binds it to the exact installed service identities.
    #[allow(
        clippy::too_many_arguments,
        reason = "each client credential identity must remain independently bound to the exact service runtime"
    )]
    pub(crate) fn try_new(
        paths: &LocalPaths,
        relay_program: impl AsRef<Path>,
        installation_data_root: impl AsRef<Path>,
        runtime: RuntimeIdentity,
        endpoint_identity: impl Into<String>,
        claude_credential_identity: impl Into<String>,
        codex_credential_identity: impl Into<String>,
    ) -> Result<Self, McpClientRegistrationError> {
        let endpoint_identity = endpoint_identity.into();
        let authorities = BTreeMap::from([
            (
                McpClientKind::ClaudeCode,
                McpRegistrationAuthority::try_new(
                    runtime,
                    endpoint_identity.clone(),
                    claude_credential_identity,
                )?,
            ),
            (
                McpClientKind::Codex,
                McpRegistrationAuthority::try_new(
                    runtime,
                    endpoint_identity,
                    codex_credential_identity,
                )?,
            ),
        ]);
        Ok(Self {
            runtime,
            control_gate: AsyncMutex::new(()),
            inner: Arc::new(Mutex::new(DesktopMcpClientAuthority {
                manager: McpClientRegistrationManager::try_new(
                    paths,
                    relay_program,
                    installation_data_root,
                )?,
                authorities,
            })),
        })
    }
}

struct DesktopMcpClientAuthority {
    manager: McpClientRegistrationManager,
    authorities: BTreeMap<McpClientKind, McpRegistrationAuthority>,
}

impl DesktopMcpClientAuthority {
    fn authority(
        &self,
        client: McpClientKind,
    ) -> Result<McpRegistrationAuthority, McpClientRegistrationError> {
        self.authorities
            .get(&client)
            .cloned()
            .ok_or(McpClientRegistrationError::InvalidAuthorityIdentity)
    }

    fn synchronize_service_authority(
        &mut self,
        runtime: RuntimeIdentity,
        service: &McpServiceRuntimeStatus,
    ) -> Result<(), McpClientRegistrationError> {
        service.validate()?;
        for facts in &service.clients {
            let current = self.authority(facts.client)?;
            self.authorities.insert(
                facts.client,
                McpRegistrationAuthority::try_new(
                    runtime,
                    current.endpoint_identity(),
                    facts.credential_identity.clone(),
                )?,
            );
        }
        Ok(())
    }

    fn inspect_client(
        &self,
        client: McpClientKind,
        service: &McpServiceRuntimeStatus,
    ) -> Result<McpClientPresentation, McpClientRegistrationError> {
        let authority = self.authority(client)?;
        let status = self.manager.inspect_with_authority(client, &authority)?;
        let facts = service
            .client(client)
            .ok_or(McpClientRegistrationError::InvalidAuthorityIdentity)?;
        Ok(self.present(status, facts))
    }

    fn present(
        &self,
        status: McpClientStatus,
        service: &McpServiceClientStatus,
    ) -> McpClientPresentation {
        let client = status.client();
        let last_verification = if status.state() == McpClientState::Owned {
            status
                .receipt()
                .and_then(|receipt| receipt.last_verification())
                .map(McpVerificationPresentation::from)
        } else {
            None
        };
        let receipt = status.receipt().map(|receipt| McpOwnedReceiptPresentation {
            command_sha256: receipt.command_sha256().to_owned(),
            observed_at_unix_seconds: receipt.observed_at_unix_seconds(),
        });
        McpClientPresentation {
            client,
            label: client.label(),
            state: if service.access_revoked && status.state() == McpClientState::Owned {
                McpClientPresentationState::AccessRevoked
            } else {
                status.state().into()
            },
            client_version: status.client_version().map(str::to_owned),
            receipt,
            verification: last_verification,
            blocker: if service.access_revoked && status.state() == McpClientState::Owned {
                Some("This client's service credential is revoked until explicitly reconnected.")
            } else {
                status.blocker()
            },
            service: service.clone(),
        }
    }

    fn inspect_all(
        &self,
        service: &McpServiceRuntimeStatus,
    ) -> Result<Vec<McpClientPresentation>, McpClientRegistrationError> {
        [McpClientKind::ClaudeCode, McpClientKind::Codex]
            .into_iter()
            .map(|client| self.inspect_client(client, service))
            .collect()
    }

    fn verify_client_connections(
        &self,
        requested_client: McpClientKind,
        service: &McpServiceRuntimeStatus,
    ) -> Result<(), McpClientRegistrationError> {
        let requested_facts = service
            .client(requested_client)
            .ok_or(McpClientRegistrationError::InvalidAuthorityIdentity)?;
        if requested_facts.access_revoked {
            return Err(McpClientRegistrationError::OwnershipRequired {
                client: requested_client,
            });
        }
        let requested_authority = self.authority(requested_client)?;
        let requested_status = self
            .manager
            .inspect_with_authority(requested_client, &requested_authority)?;
        if requested_status.state() != McpClientState::Owned {
            return Err(McpClientRegistrationError::OwnershipRequired {
                client: requested_client,
            });
        }

        let other_client = match requested_client {
            McpClientKind::ClaudeCode => McpClientKind::Codex,
            McpClientKind::Codex => McpClientKind::ClaudeCode,
        };
        let other_facts = service
            .client(other_client)
            .ok_or(McpClientRegistrationError::InvalidAuthorityIdentity)?;
        let other_is_current =
            if other_facts.access_revoked || other_facts.credential_rotation_recovery_pending {
                false
            } else {
                let other_authority = self.authority(other_client)?;
                self.manager
                    .inspect_with_authority(other_client, &other_authority)?
                    .state()
                    == McpClientState::Owned
            };

        if other_is_current {
            self.manager.verify_concurrent_clients()?;
        } else {
            self.manager.verify_protocol(requested_client)?;
        }
        Ok(())
    }

    fn admit_control(
        &self,
        request: McpClientControlCommand,
        service: &McpServiceRuntimeStatus,
    ) -> Result<(), NativeMcpOperationError> {
        let client = request.client();
        let authority = self.authority(client)?;
        let status = self.manager.inspect_with_authority(client, &authority)?;
        let facts = service
            .client(client)
            .ok_or(McpClientRegistrationError::InvalidAuthorityIdentity)?;
        if request.service_operation().is_some() && facts.credential_rotation_recovery_pending {
            return Err(NativeMcpOperationError::CredentialRecoveryPending);
        }
        let admitted = match request {
            McpClientControlCommand::Connect { .. } => status.state() == McpClientState::Ready,
            McpClientControlCommand::Reconnect { .. } => {
                status.state() == McpClientState::Owned && facts.access_revoked
            }
            McpClientControlCommand::Repair { .. } => matches!(
                status.state(),
                McpClientState::Owned | McpClientState::RepairRequired
            ),
            McpClientControlCommand::RotateCredential { .. }
            | McpClientControlCommand::RevokeCredential { .. }
            | McpClientControlCommand::Verify { .. } => {
                status.state() == McpClientState::Owned && !facts.access_revoked
            }
            McpClientControlCommand::Disconnect { .. } => matches!(
                status.state(),
                McpClientState::Owned | McpClientState::RepairRequired
            ),
        };
        if admitted {
            return Ok(());
        }
        Err(match status.state() {
            McpClientState::Absent | McpClientState::Unsupported => {
                McpClientRegistrationError::ClientUnavailable { client }.into()
            }
            McpClientState::Conflict => {
                McpClientRegistrationError::UnownedConflict { client }.into()
            }
            McpClientState::Ready | McpClientState::Owned | McpClientState::RepairRequired => {
                McpClientRegistrationError::OwnershipRequired { client }.into()
            }
        })
    }
}

/// Secret-free shared-service and supported-client status for the MCP dashboard.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpStatus {
    service_ready: bool,
    shared_endpoint_ready: bool,
    workspace_id: String,
    service_generation: u64,
    protocol_version: &'static str,
    transport: &'static str,
    runtime: McpServiceRuntimeStatus,
    clients: Vec<McpClientPresentation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum McpClientPresentationState {
    Absent,
    Unsupported,
    Ready,
    Owned,
    RepairRequired,
    AccessRevoked,
    Conflict,
}

impl From<McpClientState> for McpClientPresentationState {
    fn from(state: McpClientState) -> Self {
        match state {
            McpClientState::Absent => Self::Absent,
            McpClientState::Unsupported => Self::Unsupported,
            McpClientState::Ready => Self::Ready,
            McpClientState::Owned => Self::Owned,
            McpClientState::RepairRequired => Self::RepairRequired,
            McpClientState::Conflict => Self::Conflict,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpClientPresentation {
    client: McpClientKind,
    label: &'static str,
    state: McpClientPresentationState,
    client_version: Option<String>,
    receipt: Option<McpOwnedReceiptPresentation>,
    verification: Option<McpVerificationPresentation>,
    blocker: Option<&'static str>,
    service: McpServiceClientStatus,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct McpServiceRuntimeStatus {
    session_model: String,
    active_clients: usize,
    active_requests: usize,
    admitted_requests: Option<u64>,
    rate_limited_requests: Option<u64>,
    rejected_credentials: u64,
    uptime_seconds: u64,
    process: McpProcessResources,
    limits: McpRuntimeLimits,
    clients: Vec<McpServiceClientStatus>,
}

impl McpServiceRuntimeStatus {
    fn client(&self, client: McpClientKind) -> Option<&McpServiceClientStatus> {
        self.clients.iter().find(|facts| facts.client == client)
    }

    fn validate(&self) -> Result<(), McpClientRegistrationError> {
        let claude = self.client(McpClientKind::ClaudeCode);
        let codex = self.client(McpClientKind::Codex);
        let active_requests = self.clients.iter().try_fold(0_usize, |total, facts| {
            total.checked_add(facts.active_requests)
        });
        let admitted_requests = self.clients.iter().try_fold(0_u64, |total, facts| {
            total.checked_add(facts.admitted_requests)
        });
        let rate_limited_requests = self.clients.iter().try_fold(0_u64, |total, facts| {
            total.checked_add(facts.rate_limited_requests)
        });
        let distinct_client_authorities = claude.zip(codex).is_some_and(|(claude, codex)| {
            claude.client_id != codex.client_id
                && claude.credential_identity != codex.credential_identity
        });
        if self.session_model != "stateless_request_scoped"
            || self.clients.len() != 2
            || claude.is_none()
            || codex.is_none()
            || !distinct_client_authorities
            || active_requests != Some(self.active_requests)
            || admitted_requests != self.admitted_requests
            || rate_limited_requests != self.rate_limited_requests
            || self.active_clients
                != self
                    .clients
                    .iter()
                    .filter(|facts| facts.active_requests > 0)
                    .count()
            || self.limits.has_zero()
            || self
                .clients
                .iter()
                .any(|facts| !facts.has_valid_identity_and_limits())
        {
            return Err(McpClientRegistrationError::InvalidAuthorityIdentity);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct McpServiceClientStatus {
    client: McpClientKind,
    client_id: String,
    credential_generation: u64,
    credential_identity: String,
    maximum_active_requests: usize,
    active_requests: usize,
    admitted_requests: u64,
    rate_limited_requests: u64,
    observed_relay_initializations: u64,
    last_activity_unix_seconds: Option<u64>,
    credential_rotation_recovery_pending: bool,
    prior_credential_cleanup_pending: bool,
    access_revoked: bool,
}

impl McpServiceClientStatus {
    fn has_valid_identity_and_limits(&self) -> bool {
        let Ok(client_id) = Uuid::parse_str(&self.client_id) else {
            return false;
        };
        self.credential_generation > 0
            && self.credential_identity == format!("{client_id}:{}", self.credential_generation)
            && self.maximum_active_requests > 0
            && self.active_requests <= self.maximum_active_requests
            && (!self.prior_credential_cleanup_pending || self.credential_rotation_recovery_pending)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct McpProcessResources {
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct McpRuntimeLimits {
    maximum_frame_bytes: usize,
    maximum_body_bytes: usize,
    maximum_active_requests: usize,
    maximum_inline_bytes: usize,
    maximum_inline_items: usize,
    maximum_result_bytes: usize,
    maximum_result_items: usize,
    request_timeout_milliseconds: u64,
}

impl McpRuntimeLimits {
    const fn has_zero(self) -> bool {
        self.maximum_frame_bytes == 0
            || self.maximum_body_bytes == 0
            || self.maximum_active_requests == 0
            || self.maximum_inline_bytes == 0
            || self.maximum_inline_items == 0
            || self.maximum_result_bytes == 0
            || self.maximum_result_items == 0
            || self.request_timeout_milliseconds == 0
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct McpCredentialMutationReceipt {
    client: McpClientKind,
    client_id: String,
    credential_generation: u64,
    credential_identity: String,
    prior_generation_revoked: bool,
    prior_credential_cleanup_pending: bool,
    access_revoked: bool,
    completed_at_unix_seconds: u64,
}

impl McpCredentialMutationReceipt {
    fn validate(
        &self,
        expected_client: McpClientKind,
        operation: &str,
    ) -> Result<(), DesktopCommandError> {
        let expected_revoked = operation == MCP_REVOKE_OPERATION;
        let expected_rotated = matches!(operation, MCP_ROTATE_OPERATION | MCP_REVOKE_OPERATION);
        let client_id =
            Uuid::parse_str(&self.client_id).map_err(|_error| DesktopCommandError::internal())?;
        let expected_identity = format!("{client_id}:{}", self.credential_generation);
        if self.client != expected_client
            || self.credential_generation == 0
            || self.credential_identity != expected_identity
            || self.prior_generation_revoked != expected_rotated
            || self.access_revoked != expected_revoked
            || self.completed_at_unix_seconds == 0
            || (self.prior_credential_cleanup_pending && !expected_rotated)
        {
            return Err(DesktopCommandError::new(
                "mcp_service_contract_invalid",
                "The shared MCP service returned an incompatible credential receipt.",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpOwnedReceiptPresentation {
    command_sha256: String,
    observed_at_unix_seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct McpVerificationPresentation {
    protocol_version: String,
    client_info_name: String,
    server_name: String,
    tool_count: usize,
    resource_count: usize,
    tool_domains: Box<[String]>,
    resource_names: Box<[String]>,
    safe_read_tool: String,
    verified_at_unix_seconds: u64,
}

impl From<&McpProtocolVerification> for McpVerificationPresentation {
    fn from(verification: &McpProtocolVerification) -> Self {
        Self {
            protocol_version: verification.protocol_version().to_owned(),
            client_info_name: verification.client_info_name().to_owned(),
            server_name: verification.server_name().to_owned(),
            tool_count: verification.tool_count(),
            resource_count: verification.resource_count(),
            tool_domains: verification.tool_domains().into(),
            resource_names: verification.resource_names().into(),
            safe_read_tool: verification.safe_read_tool().to_owned(),
            verified_at_unix_seconds: verification.verified_at_unix_seconds(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "action"
)]
pub(crate) enum McpClientControlCommand {
    Connect { client: McpClientKind },
    Reconnect { client: McpClientKind },
    Repair { client: McpClientKind },
    RotateCredential { client: McpClientKind },
    RevokeCredential { client: McpClientKind },
    Disconnect { client: McpClientKind },
    Verify { client: McpClientKind },
}

impl McpClientControlCommand {
    const fn timeout(self) -> Duration {
        match self {
            Self::Connect { .. } => MCP_CONNECT_TIMEOUT,
            Self::Reconnect { .. } => MCP_CONNECT_TIMEOUT,
            Self::Repair { .. } => MCP_REPAIR_TIMEOUT,
            Self::RotateCredential { .. } | Self::RevokeCredential { .. } => MCP_REPAIR_TIMEOUT,
            Self::Disconnect { .. } => MCP_DISCONNECT_TIMEOUT,
            Self::Verify { .. } => MCP_VERIFY_TIMEOUT,
        }
    }

    const fn client(self) -> McpClientKind {
        match self {
            Self::Connect { client }
            | Self::Reconnect { client }
            | Self::Repair { client }
            | Self::RotateCredential { client }
            | Self::RevokeCredential { client }
            | Self::Disconnect { client }
            | Self::Verify { client } => client,
        }
    }

    const fn service_operation(self) -> Option<&'static str> {
        match self {
            Self::Connect { .. } | Self::Reconnect { .. } => Some(MCP_ACTIVATE_OPERATION),
            Self::RotateCredential { .. } => Some(MCP_ROTATE_OPERATION),
            Self::RevokeCredential { .. } | Self::Disconnect { .. } => Some(MCP_REVOKE_OPERATION),
            Self::Repair { .. } | Self::Verify { .. } => None,
        }
    }
}

#[tauri::command]
pub(crate) async fn mcp_status(
    desktop: State<'_, DesktopState>,
    clients: State<'_, DesktopMcpClientState>,
) -> Result<McpStatus, DesktopCommandError> {
    admit_current_runtime(&desktop, &clients)?;
    desktop
        .application()
        .probe_ready(desktop.cancellation())
        .await
        .map_err(map_application_client_error)?;
    let service =
        invoke_mcp_service::<McpServiceRuntimeStatus>(&desktop, MCP_STATUS_OPERATION, json!({}))
            .await?;
    service.validate().map_err(map_mcp_client_error)?;
    let response_service = service.clone();
    let runtime = desktop.runtime();
    let authority = Arc::clone(&clients.inner);
    let client_statuses = run_blocking(MCP_STATUS_TIMEOUT, move || {
        let mut authority = lock_authority(&authority)?;
        authority.synchronize_service_authority(runtime, &service)?;
        authority.inspect_all(&service).map_err(Into::into)
    })
    .await?;
    Ok(status_response(&desktop, response_service, client_statuses))
}

#[tauri::command]
pub(crate) async fn mcp_client_control(
    request: McpClientControlCommand,
    confirmed: bool,
    desktop: State<'_, DesktopState>,
    clients: State<'_, DesktopMcpClientState>,
) -> Result<McpStatus, DesktopCommandError> {
    if !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm this MCP client change before continuing.",
        ));
    }
    admit_current_runtime(&desktop, &clients)?;
    let _control_guard = clients.control_gate.lock().await;
    if !desktop.mcp_ready() {
        return Err(DesktopCommandError::new(
            "mcp_endpoint_unavailable",
            "The shared Market Squawk MCP endpoint is not ready.",
        ));
    }
    desktop
        .application()
        .probe_ready(desktop.cancellation())
        .await
        .map_err(map_application_client_error)?;

    let initial_service =
        invoke_mcp_service::<McpServiceRuntimeStatus>(&desktop, MCP_STATUS_OPERATION, json!({}))
            .await?;
    initial_service.validate().map_err(map_mcp_client_error)?;
    let runtime = desktop.runtime();
    let preflight_service = initial_service.clone();
    let authority = Arc::clone(&clients.inner);
    run_blocking(MCP_STATUS_TIMEOUT, move || {
        let mut authority = lock_authority(&authority)?;
        authority.synchronize_service_authority(runtime, &preflight_service)?;
        authority.admit_control(request, &preflight_service)
    })
    .await?;

    if let Some(operation) = request.service_operation() {
        let mutation = invoke_mcp_service::<McpCredentialMutationReceipt>(
            &desktop,
            operation,
            json!({"client": request.client()}),
        )
        .await?;
        mutation.validate(request.client(), operation)?;
    }

    let effective_service = if request.service_operation().is_some() {
        invoke_mcp_service::<McpServiceRuntimeStatus>(&desktop, MCP_STATUS_OPERATION, json!({}))
            .await?
    } else {
        initial_service
    };
    effective_service.validate().map_err(map_mcp_client_error)?;
    let runtime = desktop.runtime();
    let timeout = request.timeout();
    let authority = Arc::clone(&clients.inner);
    run_blocking(timeout, move || {
        let mut authority = lock_authority(&authority)?;
        authority.synchronize_service_authority(runtime, &effective_service)?;
        match request {
            McpClientControlCommand::Connect { client } => {
                let registration_authority = authority.authority(client)?;
                authority.manager.connect(client, registration_authority)?;
            }
            McpClientControlCommand::Reconnect { client }
            | McpClientControlCommand::Repair { client }
            | McpClientControlCommand::RotateCredential { client }
            | McpClientControlCommand::RevokeCredential { client } => {
                let registration_authority = authority.authority(client)?;
                authority.manager.repair(client, registration_authority)?;
            }
            McpClientControlCommand::Disconnect { client } => {
                authority.manager.disconnect(client)?;
            }
            McpClientControlCommand::Verify { client } => {
                authority.verify_client_connections(client, &effective_service)?;
            }
        }
        Ok(())
    })
    .await?;

    let final_service =
        invoke_mcp_service::<McpServiceRuntimeStatus>(&desktop, MCP_STATUS_OPERATION, json!({}))
            .await?;
    final_service.validate().map_err(map_mcp_client_error)?;
    let response_service = final_service.clone();
    let runtime = desktop.runtime();
    let authority = Arc::clone(&clients.inner);
    let client_statuses = run_blocking(MCP_STATUS_TIMEOUT, move || {
        let mut authority = lock_authority(&authority)?;
        authority.synchronize_service_authority(runtime, &final_service)?;
        authority.inspect_all(&final_service).map_err(Into::into)
    })
    .await?;
    Ok(status_response(&desktop, response_service, client_statuses))
}

fn status_response(
    desktop: &DesktopState,
    runtime_status: McpServiceRuntimeStatus,
    clients: Vec<McpClientPresentation>,
) -> McpStatus {
    let runtime = desktop.runtime();
    McpStatus {
        service_ready: true,
        shared_endpoint_ready: desktop.mcp_ready(),
        workspace_id: runtime.workspace_id().as_uuid().to_string(),
        service_generation: runtime.service_generation().get(),
        protocol_version: MCP_PROTOCOL_VERSION,
        transport: MCP_TRANSPORT,
        runtime: runtime_status,
        clients,
    }
}

async fn invoke_mcp_service<T: DeserializeOwned>(
    desktop: &DesktopState,
    operation: &str,
    arguments: serde_json::Value,
) -> Result<T, DesktopCommandError> {
    let request_id = RequestId::try_string(format!("desktop-mcp-{}", Uuid::new_v4()))
        .map_err(|_error| DesktopCommandError::internal())?;
    let response = desktop
        .application()
        .invoke_operation(
            request_id,
            operation,
            arguments,
            MCP_SERVICE_TIMEOUT,
            desktop.cancellation(),
        )
        .await
        .map_err(map_application_client_error)?;
    let result = decode_application_result(response.result())?;
    serde_json::from_value(result).map_err(|_error| {
        DesktopCommandError::new(
            "mcp_service_contract_invalid",
            "The shared MCP service returned an incompatible runtime contract.",
        )
    })
}

fn admit_current_runtime(
    desktop: &DesktopState,
    clients: &DesktopMcpClientState,
) -> Result<(), DesktopCommandError> {
    if clients.runtime != desktop.runtime() {
        return Err(DesktopCommandError::new(
            "mcp_authority_stale",
            "The MCP client authority belongs to an earlier service generation. Restart Market Squawk and retry.",
        ));
    }
    Ok(())
}

fn lock_authority(
    authority: &Arc<Mutex<DesktopMcpClientAuthority>>,
) -> Result<std::sync::MutexGuard<'_, DesktopMcpClientAuthority>, NativeMcpOperationError> {
    authority
        .lock()
        .map_err(|_error| NativeMcpOperationError::AuthorityUnavailable)
}

async fn run_blocking<T>(
    timeout: Duration,
    operation: impl FnOnce() -> Result<T, NativeMcpOperationError> + Send + 'static,
) -> Result<T, DesktopCommandError>
where
    T: Send + 'static,
{
    tokio::time::timeout(timeout, tauri::async_runtime::spawn_blocking(operation))
        .await
        .map_err(|_elapsed| {
            DesktopCommandError::new(
                "mcp_client_timeout",
                "The installed MCP client did not complete before the local deadline.",
            )
        })?
        .map_err(|_join_error| DesktopCommandError::internal())?
        .map_err(|error| match error {
            NativeMcpOperationError::Registration(error) => map_mcp_client_error(error),
            NativeMcpOperationError::AuthorityUnavailable => DesktopCommandError::internal(),
            NativeMcpOperationError::CredentialRecoveryPending => DesktopCommandError::new(
                "mcp_credential_recovery_pending",
                "Restart Market Squawk to finish the interrupted credential change before making another client change.",
            ),
        })
}

#[derive(Debug)]
enum NativeMcpOperationError {
    Registration(McpClientRegistrationError),
    AuthorityUnavailable,
    CredentialRecoveryPending,
}

impl From<McpClientRegistrationError> for NativeMcpOperationError {
    fn from(error: McpClientRegistrationError) -> Self {
        Self::Registration(error)
    }
}

fn map_mcp_client_error(error: McpClientRegistrationError) -> DesktopCommandError {
    match error {
        McpClientRegistrationError::ClientUnavailable { .. } => DesktopCommandError::new(
            "mcp_client_unavailable",
            "The selected MCP client is absent or does not support the required official commands.",
        ),
        McpClientRegistrationError::UnownedConflict { .. } => DesktopCommandError::new(
            "mcp_client_conflict",
            "A same-name MCP entry is not owned by Market Squawk and was not changed.",
        ),
        McpClientRegistrationError::OwnershipRequired { .. } => DesktopCommandError::new(
            "mcp_client_not_owned",
            "This action requires an exact Market Squawk-owned MCP registration receipt.",
        ),
        McpClientRegistrationError::ClientDeadline => DesktopCommandError::new(
            "mcp_client_timeout",
            "The installed MCP client did not complete before the local deadline.",
        ),
        McpClientRegistrationError::RegistrationVerification { .. }
        | McpClientRegistrationError::Protocol => DesktopCommandError::new(
            "mcp_client_verification_failed",
            "The installed MCP client did not reproduce the verified Market Squawk registration or protocol session.",
        ),
        McpClientRegistrationError::InvalidReceipt => DesktopCommandError::new(
            "mcp_client_receipt_invalid",
            "The owned MCP registration receipt is corrupt or incompatible and requires repair.",
        ),
        McpClientRegistrationError::InvalidRelayProgram
        | McpClientRegistrationError::InvalidInstallationRoot
        | McpClientRegistrationError::UnsafeExecutable => DesktopCommandError::new(
            "mcp_relay_unavailable",
            "The verified Market Squawk MCP relay or installed service location is unavailable.",
        ),
        McpClientRegistrationError::InvalidAuthorityIdentity => DesktopCommandError::new(
            "mcp_authority_unavailable",
            "The installed service did not provide a valid MCP client authority identity.",
        ),
        McpClientRegistrationError::ClientInspection { .. }
        | McpClientRegistrationError::ClientCommand { .. }
        | McpClientRegistrationError::InvalidClientOutput { .. }
        | McpClientRegistrationError::ClientLaunch
        | McpClientRegistrationError::ClientOutput
        | McpClientRegistrationError::ReceiptMutation
        | McpClientRegistrationError::ReceiptEncoding
        | McpClientRegistrationError::Clock
        | McpClientRegistrationError::Path(_)
        | McpClientRegistrationError::ReceiptStore(_) => DesktopCommandError::new(
            "mcp_client_operation_failed",
            "Market Squawk could not complete the installed MCP client operation.",
        ),
    }
}
