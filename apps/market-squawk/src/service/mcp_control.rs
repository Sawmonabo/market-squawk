//! Desktop-only authority for installed MCP credentials and runtime evidence.
//!
//! This module owns no presentation state. It retains the two MCP client registrations in a
//! crash-recoverable journal, resolves the current generation before the runtime credential
//! registry is loaded, authenticates each client through a dynamically replaceable identity, and
//! exposes bounded, secret-free facts to the native desktop bridge.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_mcp::{
    AuthenticatedMcpClient, McpHttpAuthError, McpHttpAuthenticator, McpLimitSpec,
};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, LocalSecretStoreError,
    PathError, SecretCancellation, SecretInteractionPolicy, SecretOperationControl, SecretStore,
};
use market_squawk_runtime::{
    AppRequestEnvelope, ClientCredentialRegistration, ClientCredentialRotationPlan, ClientId,
    CredentialError, CredentialRegistry, InstallationId, NamedClient, OperationEffect,
    RuntimeIdentity, WorkspaceId,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use thiserror::Error;

pub(super) const STATUS_OPERATION: &str = "Mcp.GetRuntimeStatus";
pub(super) const ACTIVATE_OPERATION: &str = "Mcp.ActivateCredential";
pub(super) const ROTATE_OPERATION: &str = "Mcp.RotateCredential";
pub(super) const REVOKE_OPERATION: &str = "Mcp.RevokeCredential";

const FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "installed-service/mcp-client-authority";
const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Resolves the current generation for a newly launched MCP relay without reading secret bytes.
pub(super) fn resolve_registration(
    paths: &LocalPaths,
    runtime: RuntimeIdentity,
    root: &ClientCredentialRegistration,
) -> Result<ClientCredentialRegistration, McpControlError> {
    ensure_mcp_client(root.client())?;
    let store =
        LocalAuthorityStateStore::try_open(paths.control_root()?.root().join(AUTHORITY_DIRECTORY))?;
    let encoded = store.load()?.ok_or(McpControlError::InvalidState)?;
    let document = serde_json::from_slice::<AuthorityDocument>(&encoded)
        .map_err(|_error| McpControlError::InvalidState)?
        .validate(runtime.installation_id(), runtime.workspace_id())?;
    document.validate_runtime_root(root)?;
    document
        .effective_registrations()?
        .into_iter()
        .find(|registration| registration.client() == root.client())
        .ok_or(McpControlError::InvalidState)
}

/// Durable preparation boundary resolved before the runtime credential registry is loaded.
pub(super) struct PreparedMcpClientAuthority {
    store: LocalAuthorityStateStore,
    document: AuthorityDocument,
}

impl std::fmt::Debug for PreparedMcpClientAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedMcpClientAuthority")
            .field("store", &self.store)
            .field("document", &"[NON-SECRET MCP CLIENT AUTHORITY]")
            .finish()
    }
}

impl PreparedMcpClientAuthority {
    /// Opens or initializes the crash-recovery journal for exactly Claude Code and Codex.
    pub(super) fn try_prepare(
        paths: &LocalPaths,
        runtime: RuntimeIdentity,
        secret_store: &Arc<dyn SecretStore>,
        registrations: [ClientCredentialRegistration; 2],
    ) -> Result<Self, McpControlError> {
        let store = LocalAuthorityStateStore::try_open(
            paths.control_root()?.root().join(AUTHORITY_DIRECTORY),
        )?;
        let document = match store.load()? {
            Some(encoded) => serde_json::from_slice::<AuthorityDocument>(&encoded)
                .map_err(|_error| McpControlError::InvalidState)?
                .validate(runtime.installation_id(), runtime.workspace_id())?,
            None => {
                let document = AuthorityDocument::new(runtime, registrations.clone())?
                    .validate(runtime.installation_id(), runtime.workspace_id())?;
                store_document(&store, &document)?;
                document
            }
        };
        document.validate_runtime_roots(&registrations)?;
        if let Some(pending) = &document.pending {
            let reconciled = CredentialRegistry::reconcile_planned_rotation(
                secret_store.as_ref(),
                &pending.plan,
            )?;
            if reconciled != pending.candidate {
                return Err(McpControlError::InvalidState);
            }
        }
        Ok(Self { store, document })
    }

    /// Effective registrations, including a durably journaled candidate after an interrupted
    /// rotation. The caller must load these exact generations into [`CredentialRegistry`].
    pub(super) fn registrations(
        &self,
    ) -> Result<[ClientCredentialRegistration; 2], McpControlError> {
        self.document.effective_registrations()
    }

    /// Activates dynamic authentication and reconciles any interrupted prior-generation cleanup.
    pub(super) fn activate(
        mut self,
        runtime: RuntimeIdentity,
        desktop_client_id: ClientId,
        secret_store: Arc<dyn SecretStore>,
        credentials: Arc<CredentialRegistry>,
        maximum_client_requests: usize,
        limits: McpLimitSpec,
    ) -> Result<Arc<InstalledMcpControl>, McpControlError> {
        if runtime.installation_id() != self.document.installation_id
            || runtime.workspace_id() != self.document.workspace_id
            || maximum_client_requests == 0
        {
            return Err(McpControlError::InvalidState);
        }
        reconcile_pending_cleanup(&self.store, &secret_store, &mut self.document)?;
        let registrations = self.document.effective_registrations()?;
        let entries = registrations
            .into_iter()
            .map(|registration| {
                let identity = AuthenticatedMcpClient::try_new(
                    registration.client(),
                    registration.client_id(),
                    registration.generation(),
                    maximum_client_requests,
                )?;
                Ok((
                    registration.client(),
                    ClientEntry {
                        registration,
                        identity,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, McpControlError>>()?;
        Ok(Arc::new(InstalledMcpControl {
            runtime,
            desktop_client_id,
            store: self.store,
            credentials,
            limits,
            started_at: Instant::now(),
            rejected_credentials: std::sync::atomic::AtomicU64::new(0),
            mutation_gate: Mutex::new(()),
            state: RwLock::new(ControlState {
                document: self.document,
                entries,
            }),
        }))
    }
}

struct ClientEntry {
    registration: ClientCredentialRegistration,
    identity: AuthenticatedMcpClient,
}

struct ControlState {
    document: AuthorityDocument,
    entries: HashMap<NamedClient, ClientEntry>,
}

/// Live dynamic authenticator, mutation authority, and bounded runtime evidence source.
pub(super) struct InstalledMcpControl {
    runtime: RuntimeIdentity,
    desktop_client_id: ClientId,
    store: LocalAuthorityStateStore,
    credentials: Arc<CredentialRegistry>,
    limits: McpLimitSpec,
    started_at: Instant,
    rejected_credentials: std::sync::atomic::AtomicU64,
    mutation_gate: Mutex<()>,
    state: RwLock<ControlState>,
}

impl std::fmt::Debug for InstalledMcpControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledMcpControl")
            .field("runtime", &self.runtime)
            .field("desktop_client_id", &self.desktop_client_id)
            .field("store", &self.store)
            .field("credentials", &"[CREDENTIAL AUTHORITY]")
            .field("state", &"[DYNAMIC MCP CLIENT IDENTITIES]")
            .finish_non_exhaustive()
    }
}

impl InstalledMcpControl {
    /// Current non-secret credential registration for one MCP client.
    pub(super) fn registration(
        &self,
        client: NamedClient,
    ) -> Result<ClientCredentialRegistration, McpControlError> {
        ensure_mcp_client(client)?;
        self.state
            .read()
            .entries
            .get(&client)
            .map(|entry| entry.registration.clone())
            .ok_or(McpControlError::InvalidState)
    }

    /// Current secret-free dashboard status and measured process/request facts.
    pub(super) fn status(&self) -> Result<McpRuntimeStatus, McpControlError> {
        let state = self.state.read();
        let mut clients = state
            .entries
            .values()
            .map(|entry| ClientRuntimeStatus {
                client: entry.registration.client(),
                client_id: entry.registration.client_id(),
                credential_generation: entry.registration.generation().get(),
                credential_identity: credential_identity(&entry.registration),
                maximum_active_requests: entry.identity.maximum_active_requests(),
                active_requests: entry.identity.active_requests(),
                admitted_requests: entry.identity.admitted_requests(),
                rate_limited_requests: entry.identity.saturated_requests(),
                observed_relay_initializations: entry.identity.initialized_relays(),
                last_activity_unix_seconds: entry.identity.last_activity_unix_seconds(),
                credential_rotation_recovery_pending: state
                    .document
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.client == entry.registration.client()),
                prior_credential_cleanup_pending: state.document.pending.as_ref().is_some_and(
                    |pending| {
                        pending.client == entry.registration.client()
                            && pending.candidate == entry.registration
                    },
                ),
                access_revoked: state.document.is_revoked(entry.registration.client()),
            })
            .collect::<Vec<_>>();
        clients.sort_by_key(|entry| match entry.client {
            NamedClient::ClaudeCode => 0,
            NamedClient::Codex => 1,
            NamedClient::Desktop | NamedClient::Cli => 2,
        });
        let active_requests = clients
            .iter()
            .try_fold(0_usize, |total, client| {
                total.checked_add(client.active_requests)
            })
            .ok_or(McpControlError::InvalidState)?;
        let admitted_requests = clients.iter().try_fold(0_u64, |total, client| {
            total.checked_add(client.admitted_requests)
        });
        let rate_limited_requests = clients.iter().try_fold(0_u64, |total, client| {
            total.checked_add(client.rate_limited_requests)
        });
        Ok(McpRuntimeStatus {
            session_model: "stateless_request_scoped",
            active_clients: clients
                .iter()
                .filter(|client| client.active_requests > 0)
                .count(),
            active_requests,
            admitted_requests,
            rate_limited_requests,
            rejected_credentials: self
                .rejected_credentials
                .load(std::sync::atomic::Ordering::Relaxed),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            process: process_resources(),
            limits: RuntimeLimits::from(self.limits),
            clients,
        })
    }

    /// Handles only the private desktop MCP control surface.
    pub(super) fn dispatch(&self, request: &AppRequestEnvelope) -> Result<Value, McpControlError> {
        if request.client_id() != self.desktop_client_id {
            return Err(McpControlError::Unauthorized);
        }
        match request.operation().as_str() {
            STATUS_OPERATION => {
                serde_json::to_value(self.status()?).map_err(|_error| McpControlError::InvalidState)
            }
            ACTIVATE_OPERATION | ROTATE_OPERATION | REVOKE_OPERATION => {
                let arguments =
                    serde_json::from_value::<MutationArguments>(request.arguments().clone())
                        .map_err(|_error| McpControlError::InvalidRequest)?;
                let client = arguments.client.named();
                let mutation = match request.operation().as_str() {
                    ACTIVATE_OPERATION => self.activate_access(client)?,
                    ROTATE_OPERATION => self.rotate_prior_access(client, false)?,
                    REVOKE_OPERATION => self.rotate_prior_access(client, true)?,
                    _ => return Err(McpControlError::InvalidRequest),
                };
                serde_json::to_value(mutation).map_err(|_error| McpControlError::InvalidState)
            }
            _ => Err(McpControlError::InvalidRequest),
        }
    }

    /// Closed operation effect used by the installed dispatcher without advertising this native
    /// control surface through CLI or MCP capability discovery.
    pub(super) fn effect(operation: &str) -> Option<OperationEffect> {
        match operation {
            STATUS_OPERATION => Some(OperationEffect::Read),
            ACTIVATE_OPERATION | ROTATE_OPERATION | REVOKE_OPERATION => {
                Some(OperationEffect::Mutation)
            }
            _ => None,
        }
    }

    fn rotate_prior_access(
        &self,
        client: NamedClient,
        revoke_access: bool,
    ) -> Result<CredentialMutationReceipt, McpControlError> {
        ensure_mcp_client(client)?;
        let _mutation = self.mutation_gate.lock();
        let (prior, prior_identity, prior_document) = {
            let state = self.state.read();
            if state.document.pending.is_some() {
                return Err(McpControlError::RecoveryPending);
            }
            let current = state
                .entries
                .get(&client)
                .ok_or(McpControlError::InvalidState)?;
            (
                current.registration.clone(),
                current.identity.clone(),
                state.document.clone(),
            )
        };
        let plan = self.credentials.plan_rotation(prior.client_id())?;
        let candidate = plan.candidate();
        let pending = PendingRotation {
            client,
            prior: prior.clone(),
            candidate: candidate.clone(),
            plan: plan.clone(),
        };
        let mut pending_document = prior_document.clone();
        pending_document.pending = Some(pending);
        pending_document.set_revoked(client, revoke_access);
        {
            self.state.write().document = pending_document.clone();
        }
        if let Err(error) = store_document(&self.store, &pending_document) {
            self.state.write().document = prior_document;
            return Err(error);
        }
        let candidate = self.credentials.begin_planned_rotation(&plan)?;
        let candidate_identity = prior_identity
            .with_credential_generation(candidate.client_id(), candidate.generation())?;
        self.state.write().entries.insert(
            client,
            ClientEntry {
                registration: candidate.clone(),
                identity: candidate_identity,
            },
        );
        let outcome = self
            .credentials
            .commit_rotation(candidate.client_id(), candidate.generation())?;
        let prior_retired = outcome.prior_retired();
        if prior_retired {
            let mut completed_document = pending_document;
            completed_document.replace_client(candidate.clone())?;
            completed_document.pending = None;
            store_document(&self.store, &completed_document)?;
            self.state.write().document = completed_document;
        }
        Ok(CredentialMutationReceipt {
            client,
            client_id: candidate.client_id(),
            credential_generation: candidate.generation().get(),
            credential_identity: credential_identity(&candidate),
            prior_generation_revoked: true,
            prior_credential_cleanup_pending: !prior_retired,
            access_revoked: revoke_access,
            completed_at_unix_seconds: wall_now()?,
        })
    }

    fn activate_access(
        &self,
        client: NamedClient,
    ) -> Result<CredentialMutationReceipt, McpControlError> {
        ensure_mcp_client(client)?;
        let _mutation = self.mutation_gate.lock();
        let (registration, mut document) = {
            let state = self.state.read();
            if state.document.pending.is_some() {
                return Err(McpControlError::RecoveryPending);
            }
            (
                state
                    .entries
                    .get(&client)
                    .map(|entry| entry.registration.clone())
                    .ok_or(McpControlError::InvalidState)?,
                state.document.clone(),
            )
        };
        document.set_revoked(client, false);
        store_document(&self.store, &document)?;
        self.state.write().document = document;
        Ok(CredentialMutationReceipt {
            client,
            client_id: registration.client_id(),
            credential_generation: registration.generation().get(),
            credential_identity: credential_identity(&registration),
            prior_generation_revoked: false,
            prior_credential_cleanup_pending: false,
            access_revoked: false,
            completed_at_unix_seconds: wall_now()?,
        })
    }
}

impl McpHttpAuthenticator for InstalledMcpControl {
    fn authenticate(&self, bearer_token: &str) -> Result<AuthenticatedMcpClient, McpHttpAuthError> {
        let state = self.state.read();
        let mut matched = None;
        for entry in state.entries.values() {
            if state.document.is_revoked(entry.registration.client()) {
                continue;
            }
            if self
                .credentials
                .authenticate(
                    entry.registration.client_id(),
                    entry.registration.generation(),
                    bearer_token.as_bytes(),
                )
                .is_ok()
            {
                if matched.is_some() {
                    return Err(McpHttpAuthError::Rejected);
                }
                matched = Some(entry.identity.clone());
            }
        }
        drop(state);
        if matched.is_none() {
            increment(&self.rejected_credentials);
        }
        matched.ok_or(McpHttpAuthError::Rejected)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct AuthorityDocument {
    format_version: u16,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    clients: Vec<ClientCredentialRegistration>,
    #[serde(default)]
    revoked_clients: Vec<NamedClient>,
    pending: Option<PendingRotation>,
}

impl AuthorityDocument {
    fn new(
        runtime: RuntimeIdentity,
        registrations: [ClientCredentialRegistration; 2],
    ) -> Result<Self, McpControlError> {
        Ok(Self {
            format_version: FORMAT_VERSION,
            installation_id: runtime.installation_id(),
            workspace_id: runtime.workspace_id(),
            clients: registrations.into(),
            revoked_clients: Vec::new(),
            pending: None,
        })
    }

    fn validate(
        self,
        installation_id: InstallationId,
        workspace_id: WorkspaceId,
    ) -> Result<Self, McpControlError> {
        let names = self
            .clients
            .iter()
            .map(ClientCredentialRegistration::client)
            .collect::<HashSet<_>>();
        let ids = self
            .clients
            .iter()
            .map(ClientCredentialRegistration::client_id)
            .collect::<HashSet<_>>();
        let revoked = self.revoked_clients.iter().copied().collect::<HashSet<_>>();
        if self.format_version != FORMAT_VERSION
            || self.installation_id != installation_id
            || self.workspace_id != workspace_id
            || self.clients.len() != 2
            || names != HashSet::from([NamedClient::ClaudeCode, NamedClient::Codex])
            || ids.len() != 2
            || revoked.len() != self.revoked_clients.len()
            || !revoked.is_subset(&HashSet::from([
                NamedClient::ClaudeCode,
                NamedClient::Codex,
            ]))
        {
            return Err(McpControlError::InvalidState);
        }
        if let Some(pending) = &self.pending {
            let prior = self
                .clients
                .iter()
                .find(|registration| registration.client() == pending.client)
                .ok_or(McpControlError::InvalidState)?;
            if prior != &pending.prior
                || pending.candidate.client() != pending.client
                || pending.candidate.client_id() != prior.client_id()
                || pending.plan.client() != pending.client
                || pending.plan.client_id() != prior.client_id()
                || pending.plan.current() != prior
                || pending.plan.candidate() != pending.candidate
                || pending.candidate.generation().get()
                    != prior
                        .generation()
                        .get()
                        .checked_add(1)
                        .ok_or(McpControlError::InvalidState)?
            {
                return Err(McpControlError::InvalidState);
            }
        }
        Ok(self)
    }

    fn validate_runtime_roots(
        &self,
        registrations: &[ClientCredentialRegistration; 2],
    ) -> Result<(), McpControlError> {
        for root in registrations {
            self.validate_runtime_root(root)?;
        }
        Ok(())
    }

    fn validate_runtime_root(
        &self,
        root: &ClientCredentialRegistration,
    ) -> Result<(), McpControlError> {
        let recorded = self
            .clients
            .iter()
            .find(|registration| registration.client() == root.client())
            .ok_or(McpControlError::InvalidState)?;
        if recorded.client_id() != root.client_id()
            || recorded.generation().get() < root.generation().get()
            || (recorded.generation() == root.generation() && recorded != root)
        {
            return Err(McpControlError::InvalidState);
        }
        Ok(())
    }

    fn effective_registrations(
        &self,
    ) -> Result<[ClientCredentialRegistration; 2], McpControlError> {
        let mut registrations = self.clients.clone();
        if let Some(pending) = &self.pending
            && let Some(registration) = registrations
                .iter_mut()
                .find(|registration| registration.client() == pending.client)
        {
            *registration = pending.candidate.clone();
        }
        registrations.sort_by_key(|registration| match registration.client() {
            NamedClient::ClaudeCode => 0,
            NamedClient::Codex => 1,
            NamedClient::Desktop | NamedClient::Cli => 2,
        });
        registrations
            .try_into()
            .map_err(|_registrations: Vec<ClientCredentialRegistration>| {
                McpControlError::InvalidState
            })
    }

    fn replace_client(
        &mut self,
        registration: ClientCredentialRegistration,
    ) -> Result<(), McpControlError> {
        let current = self
            .clients
            .iter_mut()
            .find(|current| current.client() == registration.client())
            .ok_or(McpControlError::InvalidState)?;
        *current = registration;
        Ok(())
    }

    fn is_revoked(&self, client: NamedClient) -> bool {
        self.revoked_clients.contains(&client)
    }

    fn set_revoked(&mut self, client: NamedClient, revoked: bool) {
        self.revoked_clients
            .retain(|candidate| *candidate != client);
        if revoked {
            self.revoked_clients.push(client);
            self.revoked_clients
                .sort_by_key(|candidate| match candidate {
                    NamedClient::ClaudeCode => 0,
                    NamedClient::Codex => 1,
                    NamedClient::Desktop | NamedClient::Cli => 2,
                });
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PendingRotation {
    client: NamedClient,
    prior: ClientCredentialRegistration,
    candidate: ClientCredentialRegistration,
    plan: ClientCredentialRotationPlan,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MutationArguments {
    client: SupportedMcpClient,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SupportedMcpClient {
    ClaudeCode,
    Codex,
}

impl SupportedMcpClient {
    const fn named(self) -> NamedClient {
        match self {
            Self::ClaudeCode => NamedClient::ClaudeCode,
            Self::Codex => NamedClient::Codex,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CredentialMutationReceipt {
    client: NamedClient,
    client_id: ClientId,
    credential_generation: u64,
    credential_identity: String,
    prior_generation_revoked: bool,
    prior_credential_cleanup_pending: bool,
    access_revoked: bool,
    completed_at_unix_seconds: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpRuntimeStatus {
    session_model: &'static str,
    active_clients: usize,
    active_requests: usize,
    admitted_requests: Option<u64>,
    rate_limited_requests: Option<u64>,
    rejected_credentials: u64,
    uptime_seconds: u64,
    process: ProcessResources,
    limits: RuntimeLimits,
    clients: Vec<ClientRuntimeStatus>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ClientRuntimeStatus {
    client: NamedClient,
    client_id: ClientId,
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

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProcessResources {
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeLimits {
    maximum_frame_bytes: usize,
    maximum_body_bytes: usize,
    maximum_active_requests: usize,
    maximum_inline_bytes: usize,
    maximum_inline_items: usize,
    maximum_result_bytes: usize,
    maximum_result_items: usize,
    request_timeout_milliseconds: u64,
}

impl From<McpLimitSpec> for RuntimeLimits {
    fn from(spec: McpLimitSpec) -> Self {
        Self {
            maximum_frame_bytes: spec.maximum_frame_bytes,
            maximum_body_bytes: spec.maximum_body_bytes,
            maximum_active_requests: spec.maximum_active_requests,
            maximum_inline_bytes: spec.maximum_inline_bytes,
            maximum_inline_items: spec.maximum_inline_items,
            maximum_result_bytes: spec.maximum_result_bytes,
            maximum_result_items: spec.maximum_result_items,
            request_timeout_milliseconds: match u64::try_from(spec.request_timeout.as_millis()) {
                Ok(milliseconds) => milliseconds,
                Err(_overflow) => u64::MAX,
            },
        }
    }
}

fn reconcile_pending_cleanup(
    store: &LocalAuthorityStateStore,
    secret_store: &Arc<dyn SecretStore>,
    document: &mut AuthorityDocument,
) -> Result<(), McpControlError> {
    let Some(pending) = document.pending.clone() else {
        return Ok(());
    };
    match secret_store.delete(
        pending.prior.reference(),
        &secret_control("installed-mcp-reconcile-rotation")?,
    ) {
        Ok(()) | Err(LocalSecretStoreError::NotFound) => {
            document.replace_client(pending.candidate)?;
            document.pending = None;
            store_document(store, document)
        }
        Err(_error) => Ok(()),
    }
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &AuthorityDocument,
) -> Result<(), McpControlError> {
    let encoded = serde_json::to_vec(document).map_err(|_error| McpControlError::InvalidState)?;
    store.store(&encoded)?;
    Ok(())
}

fn ensure_mcp_client(client: NamedClient) -> Result<(), McpControlError> {
    if matches!(client, NamedClient::ClaudeCode | NamedClient::Codex) {
        Ok(())
    } else {
        Err(McpControlError::InvalidRequest)
    }
}

fn credential_identity(registration: &ClientCredentialRegistration) -> String {
    format!(
        "{}:{}",
        registration.client_id().as_uuid(),
        registration.generation().get()
    )
}

fn secret_control(owner: &'static str) -> Result<SecretOperationControl, McpControlError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(McpControlError::SecretStore)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(|_error| McpControlError::SecretStore)
}

fn wall_now() -> Result<u64, McpControlError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_error| McpControlError::Clock)
}

fn process_resources() -> ProcessResources {
    let Ok(pid) = sysinfo::get_current_pid() else {
        return ProcessResources {
            resident_memory_bytes: None,
            virtual_memory_bytes: None,
        };
    };
    let mut system = System::new_with_specifics(RefreshKind::nothing());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    system.process(pid).map_or(
        ProcessResources {
            resident_memory_bytes: None,
            virtual_memory_bytes: None,
        },
        |process| ProcessResources {
            resident_memory_bytes: Some(process.memory()),
            virtual_memory_bytes: Some(process.virtual_memory()),
        },
    )
}

fn increment(counter: &std::sync::atomic::AtomicU64) {
    let _result = counter.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |value| Some(value.saturating_add(1)),
    );
}

/// Installed MCP client authority or runtime-evidence failure.
#[derive(Debug, Error)]
pub(super) enum McpControlError {
    #[error(transparent)]
    Path(#[from] PathError),
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    HttpAuthentication(#[from] McpHttpAuthError),
    #[error("the installed MCP client authority state is invalid")]
    InvalidState,
    #[error("the installed MCP client request is invalid")]
    InvalidRequest,
    #[error("the installed MCP client request is not authorized")]
    Unauthorized,
    #[error("a prior MCP credential rotation still requires recovery")]
    RecoveryPending,
    #[error("the installed secret authority is unavailable")]
    SecretStore,
    #[error("the system clock is before the Unix epoch")]
    Clock,
}
