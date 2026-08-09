//! Desktop-only authority for installed MCP credentials and runtime evidence.
//!
//! This module owns no presentation state. It retains the two MCP client registrations in a
//! crash-recoverable journal, resolves the current generation before the runtime credential
//! registry is loaded, authenticates each client through a dynamically replaceable identity, and
//! exposes bounded, secret-free facts to the native desktop bridge.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_mcp::{
    AuthenticatedMcpClient, McpHttpAuthError, McpHttpAuthenticator, McpLimitSpec,
};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError, SecretRef,
    SecretStore, SecretValue,
};
use market_squawk_runtime::{
    AppRequestEnvelope, ClientCredentialRegistration, ClientId, CredentialError,
    CredentialRegistry, InstallationId, NamedClient, OperationEffect, RuntimeIdentity, WorkspaceId,
};
use market_squawk_services::RequestContext;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

pub(super) const STATUS_OPERATION: &str = "Mcp.GetRuntimeStatus";
pub(super) const ACTIVATE_OPERATION: &str = "Mcp.ActivateCredential";
pub(super) const ROTATE_OPERATION: &str = "Mcp.RotateCredential";
pub(super) const REVOKE_OPERATION: &str = "Mcp.RevokeCredential";

const FORMAT_VERSION: u16 = 2;
const LEGACY_FORMAT_VERSION: u16 = 1;
const AUTHORITY_DIRECTORY: &str = "installed-service/mcp-client-authority";
const MAXIMUM_LEGACY_SECRET_REFERENCES: usize = 4;

/// Durable preparation boundary resolved before the runtime credential registry is loaded.
pub(super) struct PreparedMcpClientAuthority {
    authority_root: PathBuf,
    document: AuthorityDocument,
    registrations: [ClientCredentialRegistration; 2],
}

/// Product-owned capabilities required to activate the prepared MCP client authority.
pub(super) struct McpClientActivationAuthority {
    runtime: RuntimeIdentity,
    desktop_client_id: ClientId,
    credentials: Arc<CredentialRegistry>,
}

impl McpClientActivationAuthority {
    pub(super) fn new(
        runtime: RuntimeIdentity,
        desktop_client_id: ClientId,
        credentials: Arc<CredentialRegistry>,
    ) -> Self {
        Self {
            runtime,
            desktop_client_id,
            credentials,
        }
    }
}

impl std::fmt::Debug for PreparedMcpClientAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedMcpClientAuthority")
            .field("authority_root", &"[CONTROLLED MCP AUTHORITY ROOT]")
            .field("document", &"[NON-SECRET MCP CLIENT AUTHORITY]")
            .finish()
    }
}

impl PreparedMcpClientAuthority {
    /// Opens or initializes the crash-recovery journal for exactly Claude Code and Codex.
    pub(super) fn try_prepare(
        paths: &LocalPaths,
        runtime: RuntimeIdentity,
        _secret_store: &Arc<dyn SecretStore>,
        registrations: [ClientCredentialRegistration; 2],
    ) -> Result<Self, McpControlError> {
        let authority_root = paths.control_root()?.root().join(AUTHORITY_DIRECTORY);
        let store = LocalAuthorityStateStore::try_open(&authority_root)?;
        validate_current_registrations(&registrations)?;
        let (mut document, migrated_from_legacy) = match store.load()? {
            Some(encoded) => decode_authority_document(&encoded, runtime.installation_id())?,
            None => {
                let document =
                    AuthorityDocument::new(runtime).validate(runtime.installation_id())?;
                store_document(&store, &document)?;
                (document, false)
            }
        };
        let workspace_changed = document.workspace_id != runtime.workspace_id();
        if workspace_changed {
            document.workspace_id = runtime.workspace_id();
        }
        if migrated_from_legacy || workspace_changed {
            store_document(&store, &document)?;
        }
        drop(store);
        Ok(Self {
            authority_root,
            document,
            registrations,
        })
    }

    /// Activates dynamic authentication for the current service generation.
    pub(super) fn activate(
        self,
        authority: McpClientActivationAuthority,
        maximum_client_requests: usize,
        limits: McpLimitSpec,
    ) -> Result<Arc<InstalledMcpControl>, McpControlError> {
        let McpClientActivationAuthority {
            runtime,
            desktop_client_id,
            credentials,
        } = authority;
        if runtime.installation_id() != self.document.installation_id
            || runtime.workspace_id() != self.document.workspace_id
            || maximum_client_requests == 0
        {
            return Err(McpControlError::InvalidState);
        }
        let entries = self
            .registrations
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
                        credential: credentials.credential(&registration)?,
                        registration,
                        identity,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, McpControlError>>()?;
        Ok(Arc::new(InstalledMcpControl {
            runtime,
            desktop_client_id,
            authority_root: self.authority_root,
            credentials,
            limits,
            started_at: Instant::now(),
            rejected_credentials: std::sync::atomic::AtomicU64::new(0),
            mutation_gate: Arc::new(AsyncMutex::new(())),
            state: RwLock::new(ControlState {
                document: self.document,
                entries,
            }),
        }))
    }
}

struct ClientEntry {
    registration: ClientCredentialRegistration,
    credential: SecretValue,
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
    authority_root: PathBuf,
    credentials: Arc<CredentialRegistry>,
    limits: McpLimitSpec,
    started_at: Instant,
    rejected_credentials: std::sync::atomic::AtomicU64,
    mutation_gate: Arc<AsyncMutex<()>>,
    state: RwLock<ControlState>,
}

impl std::fmt::Debug for InstalledMcpControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledMcpControl")
            .field("runtime", &self.runtime)
            .field("desktop_client_id", &self.desktop_client_id)
            .field("authority_root", &"[CONTROLLED MCP AUTHORITY ROOT]")
            .field("credentials", &"[CREDENTIAL AUTHORITY]")
            .field("state", &"[DYNAMIC MCP CLIENT IDENTITIES]")
            .finish_non_exhaustive()
    }
}

impl InstalledMcpControl {
    /// Atomically admits the current non-revoked MCP generation and its exact secret.
    pub(super) fn admit_client(
        &self,
        client: NamedClient,
    ) -> Result<
        (
            ClientCredentialRegistration,
            market_squawk_platform::SecretValue,
        ),
        McpControlError,
    > {
        ensure_mcp_client(client)?;
        let state = self.state.read();
        if state.document.is_revoked(client) {
            return Err(McpControlError::Unauthorized);
        }
        let entry = state
            .entries
            .get(&client)
            .ok_or(McpControlError::InvalidState)?;
        Ok((
            entry.registration.clone(),
            duplicate_secret(&entry.credential)?,
        ))
    }

    /// Returns the exact number of MCP client identities with active requests.
    pub(super) fn active_client_count(&self) -> Result<usize, McpControlError> {
        let state = self.state.read();
        state.entries.values().try_fold(0_usize, |total, entry| {
            if entry.identity.active_requests() == 0 {
                Ok(total)
            } else {
                total.checked_add(1).ok_or(McpControlError::InvalidState)
            }
        })
    }

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
                credential_rotation_recovery_pending: false,
                prior_credential_cleanup_pending: false,
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
    pub(super) async fn dispatch(
        self: &Arc<Self>,
        request: &AppRequestEnvelope,
        context: &RequestContext,
    ) -> Result<Value, McpControlError> {
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
                ensure_mcp_client(client)?;
                let revoke_access = match request.operation().as_str() {
                    ACTIVATE_OPERATION => None,
                    ROTATE_OPERATION => Some(false),
                    REVOKE_OPERATION => Some(true),
                    _ => return Err(McpControlError::InvalidRequest),
                };
                let mutation_gate = Arc::clone(&self.mutation_gate);
                let mutation_permit = tokio::select! {
                    biased;
                    () = context.cancellation().cancelled() => {
                        return Err(McpControlError::Interrupted);
                    }
                    permit = tokio::time::timeout_at(
                        tokio::time::Instant::from_std(context.deadline()),
                        mutation_gate.lock_owned(),
                    ) => permit.map_err(|_elapsed| McpControlError::Interrupted)?,
                };
                if context.cancellation().is_cancelled() || Instant::now() >= context.deadline() {
                    return Err(McpControlError::Interrupted);
                }
                let authority = Arc::clone(self);
                let mutation = tokio::task::spawn_blocking(move || {
                    let _mutation_permit = mutation_permit;
                    revoke_access.map_or_else(
                        || authority.activate_access(client),
                        |revoke_access| authority.rotate_prior_access(client, revoke_access),
                    )
                })
                .await
                .map_err(|_error| McpControlError::Interrupted)??;
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
        let (prior, prior_identity, prior_document) = {
            let state = self.state.read();
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
        let candidate = self.credentials.begin_planned_rotation(&plan)?;
        let candidate_credential = self.credentials.credential(&candidate)?;
        let candidate_identity = prior_identity
            .with_credential_generation(candidate.client_id(), candidate.generation())?;
        let mut completed_document = prior_document;
        completed_document.set_revoked(client, revoke_access);
        if let Err(error) = store_document_at(&self.authority_root, &completed_document) {
            let _aborted = self
                .credentials
                .abort_rotation(candidate.client_id(), candidate.generation());
            return Err(error);
        }
        self.state.write().document = completed_document.clone();
        let outcome = self
            .credentials
            .commit_rotation(candidate.client_id(), candidate.generation())?;
        let prior_retired = outcome.prior_retired();
        let mut state = self.state.write();
        state.entries.insert(
            client,
            ClientEntry {
                registration: candidate.clone(),
                credential: candidate_credential,
                identity: candidate_identity,
            },
        );
        state.document = completed_document;
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
        let (registration, mut document) = {
            let state = self.state.read();
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
        store_document_at(&self.authority_root, &document)?;
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
    #[serde(default)]
    revoked_clients: Vec<NamedClient>,
    /// Retired V1 references are non-authoritative, cleanup-pending debt. They remain durable
    /// until an explicit foreground owner deletes and verifies the exact secrets.
    #[serde(default)]
    legacy_secret_cleanup: Vec<SecretRef>,
}

impl AuthorityDocument {
    fn new(runtime: RuntimeIdentity) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            installation_id: runtime.installation_id(),
            workspace_id: runtime.workspace_id(),
            revoked_clients: Vec::new(),
            legacy_secret_cleanup: Vec::new(),
        }
    }

    fn validate(self, installation_id: InstallationId) -> Result<Self, McpControlError> {
        let revoked = self.revoked_clients.iter().copied().collect::<HashSet<_>>();
        if self.format_version != FORMAT_VERSION
            || self.installation_id != installation_id
            || revoked.len() != self.revoked_clients.len()
            || self.legacy_secret_cleanup.len() > MAXIMUM_LEGACY_SECRET_REFERENCES
            || !revoked.is_subset(&HashSet::from([
                NamedClient::ClaudeCode,
                NamedClient::Codex,
            ]))
        {
            return Err(McpControlError::InvalidState);
        }
        let mut references = self.legacy_secret_cleanup.clone();
        references.sort();
        references.dedup();
        if references.len() != self.legacy_secret_cleanup.len() {
            return Err(McpControlError::InvalidState);
        }
        Ok(self)
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyClientCredentialRegistration {
    client_id: ClientId,
    client: NamedClient,
    reference: SecretRef,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyPendingRotation {
    client: NamedClient,
    prior: LegacyClientCredentialRegistration,
    candidate: LegacyClientCredentialRegistration,
    plan: Value,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyAuthorityDocument {
    format_version: u16,
    installation_id: InstallationId,
    workspace_id: WorkspaceId,
    clients: Vec<LegacyClientCredentialRegistration>,
    #[serde(default)]
    revoked_clients: Vec<NamedClient>,
    pending: Option<LegacyPendingRotation>,
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
            request_timeout_milliseconds: u64::try_from(spec.request_timeout.as_millis())
                .unwrap_or(u64::MAX),
        }
    }
}

fn store_document_at(
    authority_root: &Path,
    document: &AuthorityDocument,
) -> Result<(), McpControlError> {
    let store = LocalAuthorityStateStore::try_open(authority_root)?;
    store_document(&store, document)
}

fn store_document(
    store: &LocalAuthorityStateStore,
    document: &AuthorityDocument,
) -> Result<(), McpControlError> {
    let encoded = serde_json::to_vec(document).map_err(|_error| McpControlError::InvalidState)?;
    store.store(&encoded)?;
    Ok(())
}

/// Returns only retired, non-authoritative V1 references retained for explicit foreground cleanup.
pub(super) fn credential_references(paths: &LocalPaths) -> Result<Vec<SecretRef>, McpControlError> {
    let authority_root = paths.control_root()?.root().join(AUTHORITY_DIRECTORY);
    let store = LocalAuthorityStateStore::try_open(authority_root)?;
    let Some(encoded) = store.load()? else {
        return Ok(Vec::new());
    };
    decode_authority_references(&encoded)
}

fn decode_authority_document(
    encoded: &[u8],
    installation_id: InstallationId,
) -> Result<(AuthorityDocument, bool), McpControlError> {
    if let Ok(document) = serde_json::from_slice::<AuthorityDocument>(encoded) {
        return document
            .validate(installation_id)
            .map(|document| (document, false));
    }
    let legacy = serde_json::from_slice::<LegacyAuthorityDocument>(encoded)
        .map_err(|_error| McpControlError::InvalidState)?;
    migrate_legacy_authority(legacy, installation_id).map(|document| (document, true))
}

fn decode_authority_references(encoded: &[u8]) -> Result<Vec<SecretRef>, McpControlError> {
    if let Ok(document) = serde_json::from_slice::<AuthorityDocument>(encoded) {
        if document.format_version != FORMAT_VERSION
            || document.legacy_secret_cleanup.len() > MAXIMUM_LEGACY_SECRET_REFERENCES
        {
            return Err(McpControlError::InvalidState);
        }
        return Ok(document.legacy_secret_cleanup);
    }
    let legacy = serde_json::from_slice::<LegacyAuthorityDocument>(encoded)
        .map_err(|_error| McpControlError::InvalidState)?;
    let installation_id = legacy.installation_id;
    Ok(migrate_legacy_authority(legacy, installation_id)?.legacy_secret_cleanup)
}

fn migrate_legacy_authority(
    legacy: LegacyAuthorityDocument,
    installation_id: InstallationId,
) -> Result<AuthorityDocument, McpControlError> {
    if legacy.format_version != LEGACY_FORMAT_VERSION || legacy.installation_id != installation_id {
        return Err(McpControlError::InvalidState);
    }
    validate_legacy_registrations(&legacy.clients)?;
    if let Some(pending) = &legacy.pending {
        ensure_mcp_client(pending.client)?;
        if pending.prior.client != pending.client
            || pending.candidate.client != pending.client
            || pending.prior.client_id != pending.candidate.client_id
            || pending.plan.is_null()
        {
            return Err(McpControlError::InvalidState);
        }
    }
    let revoked = legacy
        .revoked_clients
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if revoked.len() != legacy.revoked_clients.len()
        || !revoked.is_subset(&HashSet::from([
            NamedClient::ClaudeCode,
            NamedClient::Codex,
        ]))
    {
        return Err(McpControlError::InvalidState);
    }
    let mut references = legacy
        .clients
        .into_iter()
        .map(|registration| registration.reference)
        .collect::<Vec<_>>();
    if let Some(pending) = legacy.pending {
        references.push(pending.prior.reference);
        references.push(pending.candidate.reference);
    }
    references.sort();
    references.dedup();
    AuthorityDocument {
        format_version: FORMAT_VERSION,
        installation_id,
        workspace_id: legacy.workspace_id,
        revoked_clients: legacy.revoked_clients,
        legacy_secret_cleanup: references,
    }
    .validate(installation_id)
}

fn validate_current_registrations(
    registrations: &[ClientCredentialRegistration; 2],
) -> Result<(), McpControlError> {
    let clients = registrations
        .iter()
        .map(ClientCredentialRegistration::client)
        .collect::<HashSet<_>>();
    let ids = registrations
        .iter()
        .map(ClientCredentialRegistration::client_id)
        .collect::<HashSet<_>>();
    if clients != HashSet::from([NamedClient::ClaudeCode, NamedClient::Codex]) || ids.len() != 2 {
        return Err(McpControlError::InvalidState);
    }
    Ok(())
}

fn validate_legacy_registrations(
    registrations: &[LegacyClientCredentialRegistration],
) -> Result<(), McpControlError> {
    if registrations.len() != 2 {
        return Err(McpControlError::InvalidState);
    }
    let clients = registrations
        .iter()
        .map(|registration| registration.client)
        .collect::<HashSet<_>>();
    let ids = registrations
        .iter()
        .map(|registration| registration.client_id)
        .collect::<HashSet<_>>();
    if clients != HashSet::from([NamedClient::ClaudeCode, NamedClient::Codex]) || ids.len() != 2 {
        return Err(McpControlError::InvalidState);
    }
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

fn duplicate_secret(secret: &SecretValue) -> Result<SecretValue, McpControlError> {
    SecretValue::new(secret.expose_secret().to_owned())
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
    #[error("the installed MCP client request was interrupted")]
    Interrupted,
    #[error("the installed secret authority is unavailable")]
    SecretStore,
    #[error("the system clock is before the Unix epoch")]
    Clock,
}
