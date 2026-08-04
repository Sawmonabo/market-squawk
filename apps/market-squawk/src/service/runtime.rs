//! Persistent runtime identity, named credentials, listener ownership, and process verification.

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use getrandom::fill as fill_random;
use market_squawk_domain::Timestamp;
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalPaths, SecretCancellation, SecretGeneration,
    SecretInteractionPolicy, SecretKey, SecretMutationPlan, SecretOperationControl,
    SecretReconciliationObservation, SecretRef, SecretStore, SecretValue,
};
use market_squawk_runtime::{
    ApplicationProtocolRange, ApplicationProtocolVersion, ApplicationRequestScope,
    ClientCredentialProvisioningPlan, ClientCredentialRegistration, ClientId, CorrelationId,
    CredentialRegistry, InstallationId, LoopbackApplicationClient, NamedClient, ProcessIdentity,
    ProcessIdentityVerifier, RendezvousAuthority, RendezvousError, RendezvousRecord,
    RuntimeIdentity,
};
use market_squawk_services::JsonStructureLimits;
use serde::{Deserialize, Serialize};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::net::TcpListener;
use uuid::Uuid;

use super::{
    InstalledServiceError, mcp_control::PreparedMcpClientAuthority,
    ready_admission::AdmittedRuntimeClient,
};
use crate::application::lifecycle::WorkspaceRuntimeIdentity;

const STATE_FORMAT_VERSION: u16 = 1;
const SERVICE_DIRECTORY: &str = "installed-service";
const INSTANCE_DIRECTORY: &str = "instance";
const IDENTITY_DIRECTORY: &str = "identity";
const RENDEZVOUS_DIRECTORY: &str = "rendezvous";
const SIGNING_SECRET_SCOPE: &str = "runtime-service";
const SIGNING_SECRET_NAME: &str = "rendezvous-signing";
const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const SIGNING_SECRET_BYTES: usize = 32;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InstalledRuntimeState {
    format_version: u16,
    runtime: RuntimeIdentity,
    endpoint: SocketAddr,
    credentials: Vec<ClientCredentialRegistration>,
    rendezvous_signing_secret: SecretRef,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InstalledRuntimeInitialization {
    format_version: u16,
    runtime: RuntimeIdentity,
    credentials: Box<[ClientCredentialProvisioningPlan]>,
    rendezvous_signing_plan: SecretMutationPlan,
}

impl InstalledRuntimeInitialization {
    fn validate(self) -> Result<Self, InstalledServiceError> {
        if self.format_version != STATE_FORMAT_VERSION
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
                    .filter(|plan| plan.client() == client)
                    .count()
                    != 1
            })
        {
            return Err(InstalledServiceError::InvalidRuntimeState);
        }
        Ok(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "phase")]
enum InstalledRuntimeDocument {
    Initializing {
        initialization: InstalledRuntimeInitialization,
    },
    Active {
        state: InstalledRuntimeState,
    },
}

impl InstalledRuntimeState {
    fn validate(self) -> Result<Self, InstalledServiceError> {
        if self.format_version != STATE_FORMAT_VERSION
            || self.endpoint.ip() != Ipv4Addr::LOCALHOST
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

/// Prepared single-instance runtime. The rendezvous remains unpublished until composition ends.
pub(super) struct PreparedRuntime {
    state: InstalledRuntimeState,
    secret_store: Arc<dyn SecretStore>,
    credentials: Arc<CredentialRegistry>,
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
    ) -> Result<Self, InstalledServiceError> {
        let service_root = paths.control_root()?.root().join(SERVICE_DIRECTORY);
        let identity_store =
            LocalAuthorityStateStore::try_open(service_root.join(IDENTITY_DIRECTORY))?;
        let loaded = identity_store.load()?;
        let (state, _root_credentials, signing_key, listener) = match loaded
            .map(|encoded| decode_document(&encoded))
            .transpose()?
        {
            Some(InstalledRuntimeDocument::Active { state }) => {
                let mut state = state.validate()?;
                let listener = bind_loopback().await?;
                state.endpoint = listener.local_addr()?;
                state.runtime = selected
                    .to_runtime(state.runtime.installation_id())
                    .map_err(|_error| InstalledServiceError::InvalidRuntimeState)?;
                let credentials = Arc::new(CredentialRegistry::try_load(
                    Arc::clone(&secret_store),
                    state.credentials.clone(),
                )?);
                let signing_key = secret_store
                    .read(
                        &state.rendezvous_signing_secret,
                        &secret_control("installed-runtime-signing-load")?,
                    )
                    .map_err(|_error| InstalledServiceError::SecretStore)?;
                identity_store.store(&encode_document(&InstalledRuntimeDocument::Active {
                    state: state.clone(),
                })?)?;
                (state, credentials, signing_key, listener)
            }
            Some(InstalledRuntimeDocument::Initializing { mut initialization }) => {
                initialization.runtime = selected
                    .to_runtime(initialization.runtime.installation_id())
                    .map_err(|_error| InstalledServiceError::InvalidRuntimeState)?;
                identity_store.store(&encode_document(
                    &InstalledRuntimeDocument::Initializing {
                        initialization: initialization.validate()?,
                    },
                )?)?;
                let InstalledRuntimeDocument::Initializing { initialization } = decode_document(
                    &identity_store
                        .load()?
                        .ok_or(InstalledServiceError::InvalidRuntimeState)?,
                )?
                else {
                    return Err(InstalledServiceError::InvalidRuntimeState);
                };
                resume_initialization(&identity_store, Arc::clone(&secret_store), initialization)
                    .await?
            }
            None => {
                initialize_runtime(
                    &identity_store,
                    Arc::clone(&secret_store),
                    selected,
                    installation_id,
                )
                .await?
            }
        };
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
        let effective_mcp = mcp_clients.registrations()?;
        let mut effective_registrations = state.credentials.clone();
        for effective in effective_mcp {
            let root = effective_registrations
                .iter_mut()
                .find(|registration| registration.client() == effective.client())
                .ok_or(InstalledServiceError::InvalidRuntimeState)?;
            *root = effective;
        }
        let credentials = Arc::new(CredentialRegistry::try_load(
            Arc::clone(&secret_store),
            effective_registrations,
        )?);
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
        let registration = self
            .state
            .registration(client)
            .ok_or(InstalledServiceError::InvalidRuntimeState)?;
        self.secret_store
            .read(
                registration.reference(),
                &secret_control("installed-runtime-client-load")?,
            )
            .map_err(|_error| InstalledServiceError::SecretStore)
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

async fn initialize_runtime(
    identity_store: &LocalAuthorityStateStore,
    secret_store: Arc<dyn SecretStore>,
    selected: WorkspaceRuntimeIdentity,
    installation_id: InstallationId,
) -> Result<
    (
        InstalledRuntimeState,
        Arc<CredentialRegistry>,
        SecretValue,
        TcpListener,
    ),
    InstalledServiceError,
> {
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
    let credentials = CredentialRegistry::plan_set(secret_store.as_ref(), clients)?;
    let signing_key = signing_secret_key()?;
    let signing_plan = secret_store
        .plan_create(
            &signing_key,
            SecretGeneration::new(1).map_err(|_error| InstalledServiceError::SecretStore)?,
            &secret_control("installed-runtime-signing-plan")?,
        )
        .map_err(|_error| InstalledServiceError::SecretStore)?;
    let initialization = InstalledRuntimeInitialization {
        format_version: STATE_FORMAT_VERSION,
        runtime: selected
            .to_runtime(installation_id)
            .map_err(|_error| InstalledServiceError::InvalidRuntimeState)?,
        credentials,
        rendezvous_signing_plan: signing_plan,
    }
    .validate()?;
    identity_store.store(&encode_document(&InstalledRuntimeDocument::Initializing {
        initialization,
    })?)?;
    let encoded = identity_store
        .load()?
        .ok_or(InstalledServiceError::InvalidRuntimeState)?;
    let InstalledRuntimeDocument::Initializing { initialization } = decode_document(&encoded)?
    else {
        return Err(InstalledServiceError::InvalidRuntimeState);
    };
    resume_initialization(identity_store, secret_store, initialization).await
}

pub(super) fn acquire_instance(
    paths: &LocalPaths,
) -> Result<LocalAuthorityStateStore, InstalledServiceError> {
    let service_root = paths.control_root()?.root().join(SERVICE_DIRECTORY);
    LocalAuthorityStateStore::try_open(service_root.join(INSTANCE_DIRECTORY))
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
    let runtime = match decode_document(&encoded)? {
        InstalledRuntimeDocument::Initializing { initialization } => {
            initialization.validate()?.runtime
        }
        InstalledRuntimeDocument::Active { state } => state.validate()?.runtime,
    };
    Ok(Some(runtime.installation_id()))
}

async fn resume_initialization(
    identity_store: &LocalAuthorityStateStore,
    secret_store: Arc<dyn SecretStore>,
    initialization: InstalledRuntimeInitialization,
) -> Result<
    (
        InstalledRuntimeState,
        Arc<CredentialRegistry>,
        SecretValue,
        TcpListener,
    ),
    InstalledServiceError,
> {
    let (credentials, registrations) = CredentialRegistry::provision_planned_set(
        Arc::clone(&secret_store),
        &initialization.credentials,
    )?;
    let signing_key = signing_secret_key()?;
    let control = secret_control("installed-runtime-signing-inspect")?;
    match secret_store
        .inspect_planned(
            &signing_key,
            &initialization.rendezvous_signing_plan,
            &control,
        )
        .map_err(|_error| InstalledServiceError::SecretStore)?
    {
        SecretReconciliationObservation::Absent => {
            if let Err(failure) = secret_store.execute_planned(
                &signing_key,
                &initialization.rendezvous_signing_plan,
                random_secret()?,
                &secret_control("installed-runtime-signing-create")?,
            ) && !matches!(
                secret_store.inspect_planned(
                    &signing_key,
                    &initialization.rendezvous_signing_plan,
                    &secret_control("installed-runtime-signing-reinspect")?,
                ),
                Ok(SecretReconciliationObservation::PresentUnverified)
                    | Ok(SecretReconciliationObservation::Matches)
            ) {
                let _redacted = failure.into_error();
                return Err(InstalledServiceError::SecretStore);
            }
        }
        SecretReconciliationObservation::PresentUnverified
        | SecretReconciliationObservation::Matches => {}
        SecretReconciliationObservation::Mismatch => {
            return Err(InstalledServiceError::SecretStore);
        }
    }
    let signing_value = secret_store
        .read(
            initialization.rendezvous_signing_plan.target(),
            &secret_control("installed-runtime-signing-load")?,
        )
        .map_err(|_error| InstalledServiceError::SecretStore)?;
    let listener = bind_loopback().await?;
    let state = InstalledRuntimeState {
        format_version: STATE_FORMAT_VERSION,
        runtime: initialization.runtime,
        endpoint: listener.local_addr()?,
        credentials: registrations.into_vec(),
        rendezvous_signing_secret: initialization.rendezvous_signing_plan.target().clone(),
    }
    .validate()?;
    identity_store.store(&encode_document(&InstalledRuntimeDocument::Active {
        state: state.clone(),
    })?)?;
    Ok((state, Arc::new(credentials), signing_value, listener))
}

async fn bind_loopback() -> Result<TcpListener, InstalledServiceError> {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|_error| InstalledServiceError::EndpointUnavailable)
}

fn encode_document(document: &InstalledRuntimeDocument) -> Result<Vec<u8>, InstalledServiceError> {
    serde_json::to_vec(document).map_err(|_error| InstalledServiceError::InvalidRuntimeState)
}

fn decode_document(encoded: &[u8]) -> Result<InstalledRuntimeDocument, InstalledServiceError> {
    serde_json::from_slice(encoded)
        .or_else(|_error| {
            serde_json::from_slice::<InstalledRuntimeState>(encoded)
                .map(|state| InstalledRuntimeDocument::Active { state })
        })
        .map_err(|_error| InstalledServiceError::InvalidRuntimeState)
}

fn signing_secret_key() -> Result<SecretKey, InstalledServiceError> {
    SecretKey::try_new(SIGNING_SECRET_SCOPE, SIGNING_SECRET_NAME)
        .map_err(|_error| InstalledServiceError::SecretStore)
}

fn random_secret() -> Result<SecretValue, InstalledServiceError> {
    let mut bytes = [0_u8; SIGNING_SECRET_BYTES];
    fill_random(&mut bytes).map_err(|_error| InstalledServiceError::EntropyUnavailable)?;
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

fn secret_control(owner: &'static str) -> Result<SecretOperationControl, InstalledServiceError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(InstalledServiceError::InvalidRuntimeState)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
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

pub(super) fn load_client_credential(
    secret_store: &Arc<dyn SecretStore>,
    registration: &ClientCredentialRegistration,
) -> Result<SecretValue, InstalledServiceError> {
    secret_store
        .read(
            registration.reference(),
            &secret_control("installed-runtime-client-credential-load")?,
        )
        .map_err(|_error| InstalledServiceError::SecretStore)
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
