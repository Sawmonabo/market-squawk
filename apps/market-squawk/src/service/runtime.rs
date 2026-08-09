//! Persistent installation identity and service-lifetime runtime authority.

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use market_squawk_domain::Timestamp;
use market_squawk_platform::{
    InstalledServiceInstanceGuard, LocalAuthorityStateStore, LocalPaths, SecretBackend,
    SecretInteractionPolicy, SecretMutationPlan, SecretRef, SecretStore, SecretValue,
};
use market_squawk_runtime::{
    ApplicationProtocolRange, ApplicationProtocolVersion, ApplicationRequestScope,
    ClientCredentialRegistration, ClientId, CorrelationId, CredentialRegistry, InstallationId,
    LoopbackApplicationClient, NamedClient, ProcessIdentity, ProcessIdentityVerifier,
    RendezvousAuthority, RendezvousError, RendezvousRecord, RuntimeIdentity,
};
use market_squawk_services::JsonStructureLimits;
use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::net::TcpListener;
use uuid::Uuid;

use super::{
    InstalledServiceError,
    mcp_control::{McpClientActivationAuthority, PreparedMcpClientAuthority},
    ready_admission::AdmittedRuntimeClient,
};
use crate::application::lifecycle::WorkspaceRuntimeIdentity;

const STATE_FORMAT_VERSION: u16 = 2;
const LEGACY_STATE_FORMAT_VERSION: u16 = 1;
const SERVICE_DIRECTORY: &str = "installed-service";
const IDENTITY_DIRECTORY: &str = "identity";
const RENDEZVOUS_DIRECTORY: &str = "rendezvous";
const SIGNING_SECRET_BYTES: usize = 32;
const MAXIMUM_LEGACY_SECRET_REFERENCES: usize = 5;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct DurableRuntimeState {
    format_version: u16,
    installation_id: InstallationId,
    /// Retired V1 references are non-authoritative, cleanup-pending debt. They remain durable
    /// until an explicit foreground owner deletes and verifies the exact secrets.
    #[serde(default)]
    legacy_secret_cleanup: Vec<SecretRef>,
}

impl DurableRuntimeState {
    fn new(installation_id: InstallationId) -> Self {
        Self {
            format_version: STATE_FORMAT_VERSION,
            installation_id,
            legacy_secret_cleanup: Vec::new(),
        }
    }

    fn validate(self) -> Result<Self, InstalledServiceError> {
        if self.format_version != STATE_FORMAT_VERSION
            || self.legacy_secret_cleanup.len() > MAXIMUM_LEGACY_SECRET_REFERENCES
        {
            return Err(InstalledServiceError::InvalidRuntimeState);
        }
        let mut references = self.legacy_secret_cleanup.clone();
        references.sort();
        references.dedup();
        if references.len() != self.legacy_secret_cleanup.len() {
            return Err(InstalledServiceError::InvalidRuntimeState);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
struct InstalledRuntimeState {
    runtime: RuntimeIdentity,
    endpoint: SocketAddr,
    credentials: Vec<ClientCredentialRegistration>,
}

impl InstalledRuntimeState {
    fn validate(self) -> Result<Self, InstalledServiceError> {
        if self.endpoint.ip() != Ipv4Addr::LOCALHOST
            || self.endpoint.port() == 0
            || self.credentials.len() != 4
            || [
                NamedClient::Desktop,
                NamedClient::Cli,
                NamedClient::ClaudeCode,
                NamedClient::Codex,
            ]
            .into_iter()
            .any(|client| {
                self.credentials
                    .iter()
                    .filter(|registration| registration.client() == client)
                    .count()
                    != 1
            })
        {
            return Err(InstalledServiceError::InvalidRuntimeState);
        }
        Ok(self)
    }

    fn registration(&self, client: NamedClient) -> Option<&ClientCredentialRegistration> {
        self.credentials
            .iter()
            .find(|registration| registration.client() == client)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyCredentialRegistration {
    client_id: ClientId,
    client: NamedClient,
    reference: SecretRef,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyCredentialPlan {
    client_id: ClientId,
    client: NamedClient,
    mutation: SecretMutationPlan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyRuntimeInitialization {
    format_version: u16,
    runtime: RuntimeIdentity,
    credentials: Vec<LegacyCredentialPlan>,
    rendezvous_signing_plan: SecretMutationPlan,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LegacyRuntimeState {
    format_version: u16,
    runtime: RuntimeIdentity,
    endpoint: SocketAddr,
    credentials: Vec<LegacyCredentialRegistration>,
    rendezvous_signing_secret: SecretRef,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "phase")]
enum LegacyRuntimeDocument {
    Initializing {
        initialization: LegacyRuntimeInitialization,
    },
    Active {
        state: LegacyRuntimeState,
    },
}

/// Prepared single-instance runtime. The rendezvous remains unpublished until composition ends.
pub(super) struct PreparedRuntime {
    state: InstalledRuntimeState,
    secret_store: Arc<dyn SecretStore>,
    credentials: Arc<CredentialRegistry>,
    desktop_credential: SecretValue,
    cli_credential: SecretValue,
    mcp_clients: Option<PreparedMcpClientAuthority>,
    listener: Option<TcpListener>,
    rendezvous: RendezvousAuthority,
    record: RendezvousRecord,
}

impl std::fmt::Debug for PreparedRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRuntime")
            .field("runtime", &self.state.runtime)
            .field("endpoint", &self.state.endpoint)
            .field("secret_store", &"[SECRET AUTHORITY]")
            .field("credentials", &"[CREDENTIAL AUTHORITY]")
            .field("rendezvous", &self.rendezvous)
            .finish_non_exhaustive()
    }
}

impl PreparedRuntime {
    pub(super) async fn prepare(
        paths: &LocalPaths,
        secret_store: Arc<dyn SecretStore>,
        selected: WorkspaceRuntimeIdentity,
        installation_id: InstallationId,
        _interaction: SecretInteractionPolicy,
    ) -> Result<Self, InstalledServiceError> {
        let service_root = paths.control_root()?.root().join(SERVICE_DIRECTORY);
        let identity_store =
            LocalAuthorityStateStore::try_open(service_root.join(IDENTITY_DIRECTORY))?;
        let durable = load_or_migrate_runtime_state(&identity_store, installation_id)?;
        let runtime = selected
            .to_runtime(durable.installation_id)
            .map_err(|_error| InstalledServiceError::InvalidRuntimeState)?;
        let clients = [
            NamedClient::Desktop,
            NamedClient::Cli,
            NamedClient::ClaudeCode,
            NamedClient::Codex,
        ]
        .map(|client| {
            ClientId::try_from_uuid(Uuid::new_v4())
                .map(|client_id| (client_id, client))
                .map_err(InstalledServiceError::from)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        let (credential_registry, registrations) = CredentialRegistry::provision_set(clients)?;
        let credentials = Arc::new(credential_registry);
        let listener = bind_loopback().await?;
        let state = InstalledRuntimeState {
            runtime,
            endpoint: listener.local_addr()?,
            credentials: registrations.into_vec(),
        }
        .validate()?;
        let signing_key = random_secret()?;
        let mcp_roots = [
            state
                .registration(NamedClient::ClaudeCode)
                .cloned()
                .ok_or(InstalledServiceError::InvalidRuntimeState)?,
            state
                .registration(NamedClient::Codex)
                .cloned()
                .ok_or(InstalledServiceError::InvalidRuntimeState)?,
        ];
        let mcp_clients = PreparedMcpClientAuthority::try_prepare(
            paths,
            state.runtime,
            &secret_store,
            mcp_roots,
        )?;
        let desktop_credential = credentials.credential(
            state
                .registration(NamedClient::Desktop)
                .ok_or(InstalledServiceError::InvalidRuntimeState)?,
        )?;
        let cli_credential = credentials.credential(
            state
                .registration(NamedClient::Cli)
                .ok_or(InstalledServiceError::InvalidRuntimeState)?,
        )?;
        drop(identity_store);
        let verifier = SystemProcessIdentityVerifier;
        let process = verifier.current()?;
        let published_at = wall_now()?;
        let protocols = ApplicationProtocolRange::single(ApplicationProtocolVersion::V1);
        let record = RendezvousRecord::try_new(
            state.runtime,
            state.endpoint,
            protocols,
            process,
            published_at,
        )?;
        let rendezvous = RendezvousAuthority::try_open(
            service_root.join(RENDEZVOUS_DIRECTORY),
            duplicate_secret(&signing_key)?,
        )?;
        Ok(Self {
            state,
            secret_store,
            credentials,
            desktop_credential,
            cli_credential,
            mcp_clients: Some(mcp_clients),
            listener: Some(listener),
            rendezvous,
            record,
        })
    }

    pub(super) const fn runtime(&self) -> RuntimeIdentity {
        self.state.runtime
    }

    pub(super) const fn endpoint(&self) -> SocketAddr {
        self.state.endpoint
    }

    pub(super) fn credentials(&self) -> Arc<CredentialRegistry> {
        Arc::clone(&self.credentials)
    }

    pub(super) fn secret_store(&self) -> Arc<dyn SecretStore> {
        Arc::clone(&self.secret_store)
    }

    pub(super) fn mcp_client_activation_authority(
        &self,
    ) -> Result<McpClientActivationAuthority, InstalledServiceError> {
        let desktop_client_id = self
            .state
            .registration(NamedClient::Desktop)
            .ok_or(InstalledServiceError::InvalidRuntimeState)?
            .client_id();
        Ok(McpClientActivationAuthority::new(
            self.state.runtime,
            desktop_client_id,
            Arc::clone(&self.credentials),
        ))
    }

    pub(super) fn take_mcp_clients(
        &mut self,
    ) -> Result<PreparedMcpClientAuthority, InstalledServiceError> {
        self.mcp_clients
            .take()
            .ok_or(InstalledServiceError::InvalidComposition)
    }

    pub(super) fn registration(
        &self,
        client: NamedClient,
    ) -> Result<ClientCredentialRegistration, InstalledServiceError> {
        self.state
            .registration(client)
            .cloned()
            .ok_or(InstalledServiceError::InvalidRuntimeState)
    }

    pub(super) fn read_credential(
        &self,
        client: NamedClient,
    ) -> Result<SecretValue, InstalledServiceError> {
        match client {
            NamedClient::Desktop => duplicate_secret(&self.desktop_credential),
            NamedClient::Cli => duplicate_secret(&self.cli_credential),
            NamedClient::ClaudeCode | NamedClient::Codex => {
                Err(InstalledServiceError::InvalidRuntimeState)
            }
        }
    }

    pub(super) fn admission_credentials(
        &self,
    ) -> Result<(SecretValue, SecretValue), InstalledServiceError> {
        Ok((
            duplicate_secret(&self.desktop_credential)?,
            duplicate_secret(&self.cli_credential)?,
        ))
    }

    pub(super) fn take_listener(&mut self) -> Result<TcpListener, InstalledServiceError> {
        self.listener
            .take()
            .ok_or(InstalledServiceError::ListenerAlreadyTaken)
    }

    pub(super) fn publish(&self) -> Result<(), InstalledServiceError> {
        self.rendezvous.publish(&self.record)?;
        Ok(())
    }

    pub(super) fn retire(&self) -> Result<bool, InstalledServiceError> {
        self.rendezvous
            .remove_if_current(self.state.runtime)
            .map_err(Into::into)
    }

    pub(super) const fn record(&self) -> &RendezvousRecord {
        &self.record
    }
}

pub(super) fn acquire_instance(
    paths: &LocalPaths,
) -> Result<InstalledServiceInstanceGuard, InstalledServiceError> {
    InstalledServiceInstanceGuard::try_acquire(paths.control_root()?)
        .map_err(InstalledServiceError::instance)
}

pub(super) fn installation_id(
    paths: &LocalPaths,
) -> Result<Option<InstallationId>, InstalledServiceError> {
    let service_root = paths.control_root()?.root().join(SERVICE_DIRECTORY);
    let identity_store = LocalAuthorityStateStore::try_open(service_root.join(IDENTITY_DIRECTORY))?;
    let Some(encoded) = identity_store.load()? else {
        return Ok(None);
    };
    Ok(Some(decode_runtime_state(&encoded)?.installation_id))
}

/// Returns only retired, non-authoritative V1 references retained for explicit foreground cleanup.
pub(super) fn credential_references(
    paths: &LocalPaths,
) -> Result<Vec<SecretRef>, InstalledServiceError> {
    let service_root = paths.control_root()?.root().join(SERVICE_DIRECTORY);
    let identity_store = LocalAuthorityStateStore::try_open(service_root.join(IDENTITY_DIRECTORY))?;
    let Some(encoded) = identity_store.load()? else {
        return Ok(Vec::new());
    };
    Ok(decode_runtime_state(&encoded)?.legacy_secret_cleanup)
}

pub(super) fn encrypted_fallback_eligible(
    paths: &LocalPaths,
) -> Result<bool, InstalledServiceError> {
    Ok(credential_references(paths)?
        .iter()
        .all(|reference| reference.backend() == SecretBackend::EncryptedFile))
}

async fn bind_loopback() -> Result<TcpListener, InstalledServiceError> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|_error| InstalledServiceError::EndpointUnavailable)
}

fn load_or_migrate_runtime_state(
    store: &LocalAuthorityStateStore,
    installation_id: InstallationId,
) -> Result<DurableRuntimeState, InstalledServiceError> {
    let state = store
        .load()?
        .map(|encoded| decode_runtime_state(&encoded))
        .transpose()?
        .unwrap_or_else(|| DurableRuntimeState::new(installation_id));
    store_runtime_state(store, &state)?;
    Ok(state)
}

fn decode_runtime_state(encoded: &[u8]) -> Result<DurableRuntimeState, InstalledServiceError> {
    if let Ok(state) = serde_json::from_slice::<DurableRuntimeState>(encoded) {
        return state.validate();
    }
    if let Ok(document) = serde_json::from_slice::<LegacyRuntimeDocument>(encoded) {
        return migrate_legacy_document(document);
    }
    serde_json::from_slice::<LegacyRuntimeState>(encoded)
        .map(migrate_legacy_state)
        .map_err(|_error| InstalledServiceError::InvalidRuntimeState)?
}

fn migrate_legacy_document(
    document: LegacyRuntimeDocument,
) -> Result<DurableRuntimeState, InstalledServiceError> {
    match document {
        LegacyRuntimeDocument::Initializing { initialization } => {
            migrate_legacy_initialization(initialization)
        }
        LegacyRuntimeDocument::Active { state } => migrate_legacy_state(state),
    }
}

fn migrate_legacy_initialization(
    initialization: LegacyRuntimeInitialization,
) -> Result<DurableRuntimeState, InstalledServiceError> {
    let clients = initialization
        .credentials
        .iter()
        .map(|plan| (plan.client_id, plan.client))
        .collect::<Vec<_>>();
    validate_legacy_clients(initialization.format_version, &clients)?;
    let mut references = initialization
        .credentials
        .into_iter()
        .map(|plan| plan.mutation.target().clone())
        .collect::<Vec<_>>();
    references.push(initialization.rendezvous_signing_plan.target().clone());
    migrated_state(initialization.runtime.installation_id(), references)
}

fn migrate_legacy_state(
    state: LegacyRuntimeState,
) -> Result<DurableRuntimeState, InstalledServiceError> {
    if state.endpoint.ip() != Ipv4Addr::LOCALHOST || state.endpoint.port() == 0 {
        return Err(InstalledServiceError::InvalidRuntimeState);
    }
    let clients = state
        .credentials
        .iter()
        .map(|registration| (registration.client_id, registration.client))
        .collect::<Vec<_>>();
    validate_legacy_clients(state.format_version, &clients)?;
    let mut references = state
        .credentials
        .into_iter()
        .map(|registration| registration.reference)
        .collect::<Vec<_>>();
    references.push(state.rendezvous_signing_secret);
    migrated_state(state.runtime.installation_id(), references)
}

fn validate_legacy_clients(
    format_version: u16,
    clients: &[(ClientId, NamedClient)],
) -> Result<(), InstalledServiceError> {
    let expected = [
        NamedClient::Desktop,
        NamedClient::Cli,
        NamedClient::ClaudeCode,
        NamedClient::Codex,
    ];
    if format_version != LEGACY_STATE_FORMAT_VERSION
        || clients.len() != expected.len()
        || expected.into_iter().any(|expected| {
            clients
                .iter()
                .filter(|(_id, client)| *client == expected)
                .count()
                != 1
        })
    {
        return Err(InstalledServiceError::InvalidRuntimeState);
    }
    let mut ids = clients.iter().map(|(id, _client)| *id).collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != clients.len() {
        return Err(InstalledServiceError::InvalidRuntimeState);
    }
    Ok(())
}

fn migrated_state(
    installation_id: InstallationId,
    mut references: Vec<SecretRef>,
) -> Result<DurableRuntimeState, InstalledServiceError> {
    references.sort();
    references.dedup();
    DurableRuntimeState {
        format_version: STATE_FORMAT_VERSION,
        installation_id,
        legacy_secret_cleanup: references,
    }
    .validate()
}

fn store_runtime_state(
    store: &LocalAuthorityStateStore,
    state: &DurableRuntimeState,
) -> Result<(), InstalledServiceError> {
    let encoded =
        serde_json::to_vec(state).map_err(|_error| InstalledServiceError::InvalidRuntimeState)?;
    store.store(&encoded)?;
    Ok(())
}

fn random_secret() -> Result<SecretValue, InstalledServiceError> {
    let mut bytes = [0_u8; SIGNING_SECRET_BYTES];
    fill_random(&mut bytes).map_err(|_error| InstalledServiceError::EntropyUnavailable)?;
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(InstalledServiceError::EntropyUnavailable);
    }
    let mut encoded = String::with_capacity(SIGNING_SECRET_BYTES * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SecretValue::new(encoded).map_err(|_error| InstalledServiceError::EntropyUnavailable)
}

fn duplicate_secret(secret: &SecretValue) -> Result<SecretValue, InstalledServiceError> {
    SecretValue::new(secret.expose_secret().to_owned())
        .map_err(|_error| InstalledServiceError::SecretStore)
}

fn wall_now() -> Result<Timestamp, InstalledServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| InstalledServiceError::ClockUnavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| InstalledServiceError::ClockUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Supported-platform verifier for PID reuse and stale rendezvous records.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemProcessIdentityVerifier;

impl SystemProcessIdentityVerifier {
    /// Returns the running service's exact PID/start discriminator.
    pub fn current(self) -> Result<ProcessIdentity, InstalledServiceError> {
        let pid = sysinfo::get_current_pid()
            .map_err(|_error| InstalledServiceError::ProcessIdentityUnavailable)?;
        process_identity(pid.as_u32())?.ok_or(InstalledServiceError::ProcessIdentityUnavailable)
    }
}

impl ProcessIdentityVerifier for SystemProcessIdentityVerifier {
    fn is_current(&self, identity: ProcessIdentity) -> Result<bool, RendezvousError> {
        process_identity(identity.process_id())
            .map(|observed| observed == Some(identity))
            .map_err(|_error| RendezvousError::ProcessVerificationUnavailable)
    }
}

fn process_identity(process_id: u32) -> Result<Option<ProcessIdentity>, InstalledServiceError> {
    if !sysinfo::IS_SUPPORTED_SYSTEM {
        return Err(InstalledServiceError::ProcessIdentityUnavailable);
    }
    let pid = Pid::from_u32(process_id);
    let mut system = System::new_with_specifics(RefreshKind::nothing());
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    system
        .process(pid)
        .map(|process| ProcessIdentity::try_new(process_id, process.start_time()))
        .transpose()
        .map_err(InstalledServiceError::from)
}

pub(super) fn current_timestamp() -> Result<Timestamp, InstalledServiceError> {
    wall_now()
}

pub(super) fn connect_admitted_client(
    admitted: AdmittedRuntimeClient,
    origin: Option<String>,
    structure: JsonStructureLimits,
    maximum_response_bytes: usize,
    transport_timeout: Duration,
) -> Result<LoopbackApplicationClient, InstalledServiceError> {
    let scope = ApplicationRequestScope::try_new(
        admitted.record.runtime(),
        admitted.client_id,
        admitted.generation,
        CorrelationId::try_from_uuid(Uuid::new_v4())?,
        structure,
        maximum_response_bytes,
    )?;
    LoopbackApplicationClient::try_new(
        &admitted.record,
        scope,
        admitted.credential,
        origin,
        maximum_response_bytes,
        structure,
        transport_timeout,
    )
    .map_err(Into::into)
}
