//! Named-client credential custody and exact-generation authentication.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use getrandom::fill as fill_random;
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretGeneration, SecretInteractionPolicy,
    SecretKey, SecretMutationPlan, SecretOperationControl, SecretReconciliationObservation,
    SecretRef, SecretStore, SecretValue,
};
use serde::{Deserialize, Deserializer, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;

use crate::ClientId;

const CREDENTIAL_BYTES: usize = 32;
const SECRET_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const SECRET_SCOPE: &str = "runtime-client";

/// Runtime-owned non-secret view of an exact named-client secret generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CredentialGeneration(u64);

impl CredentialGeneration {
    /// Creates a one-based credential generation.
    pub fn try_new(value: u64) -> Result<Self, CredentialError> {
        if value == 0 {
            Err(CredentialError::GenerationMismatch)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the portable one-based integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn to_secret_generation(self) -> Result<SecretGeneration, CredentialError> {
        SecretGeneration::new(self.get()).map_err(map_secret_error)
    }
}

impl From<SecretGeneration> for CredentialGeneration {
    fn from(value: SecretGeneration) -> Self {
        Self(value.get())
    }
}

impl<'de> Deserialize<'de> for CredentialGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

/// Closed installed-product client classes with independent credentials.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedClient {
    /// Native desktop application Rust bridge.
    Desktop,
    /// Local command-line client.
    Cli,
    /// Claude Code MCP relay.
    ClaudeCode,
    /// Codex MCP relay.
    Codex,
}

impl NamedClient {
    const fn secret_name(self) -> &'static str {
        match self {
            Self::Desktop => "desktop",
            Self::Cli => "cli",
            Self::ClaudeCode => "claude-code",
            Self::Codex => "codex",
        }
    }
}

/// Durable non-secret metadata for one named credential generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientCredentialRegistration {
    client_id: ClientId,
    client: NamedClient,
    reference: SecretRef,
}

impl ClientCredentialRegistration {
    /// Binds a named client identity to one exact opaque secret reference.
    #[must_use]
    pub const fn new(client_id: ClientId, client: NamedClient, reference: SecretRef) -> Self {
        Self {
            client_id,
            client,
            reference,
        }
    }

    /// Registered client identity.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Closed named-client class.
    #[must_use]
    pub const fn client(&self) -> NamedClient {
        self.client
    }

    /// Active non-secret credential generation.
    #[must_use]
    pub fn generation(&self) -> CredentialGeneration {
        self.reference.generation().into()
    }

    /// Opaque secret-store reference for durable metadata persistence.
    #[must_use]
    pub const fn reference(&self) -> &SecretRef {
        &self.reference
    }
}

/// Durable non-secret first-install plan for one named runtime credential.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientCredentialProvisioningPlan {
    client_id: ClientId,
    client: NamedClient,
    mutation: SecretMutationPlan,
}

impl ClientCredentialProvisioningPlan {
    /// Registered client identity reserved by this plan.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Closed named-client class reserved by this plan.
    #[must_use]
    pub const fn client(&self) -> NamedClient {
        self.client
    }

    /// Exact backend mutation selected before any credential bytes are written.
    #[must_use]
    pub const fn mutation(&self) -> &SecretMutationPlan {
        &self.mutation
    }
}

struct ActiveCredential {
    registration: ClientCredentialRegistration,
    value: SecretValue,
    pending: Option<PendingCredential>,
}

struct PendingCredential {
    registration: ClientCredentialRegistration,
    value: SecretValue,
}

impl fmt::Debug for ActiveCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveCredential")
            .field("registration", &self.registration)
            .field("value", &"[REDACTED]")
            .field("pending", &self.pending.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// In-process verifier backed by exact generations in the existing local secret authority.
pub struct CredentialRegistry {
    store: Arc<dyn SecretStore>,
    active: RwLock<HashMap<ClientId, ActiveCredential>>,
}

impl fmt::Debug for CredentialRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialRegistry([REDACTED CLIENT CREDENTIALS])")
    }
}

impl CredentialRegistry {
    /// Freezes exact first-generation secret targets before any credential bytes are written.
    pub fn plan_set(
        store: &dyn SecretStore,
        clients: impl IntoIterator<Item = (ClientId, NamedClient)>,
    ) -> Result<Box<[ClientCredentialProvisioningPlan]>, CredentialError> {
        let clients = clients.into_iter().collect::<Vec<_>>();
        validate_clients(&clients)?;
        let generation = SecretGeneration::new(1).map_err(map_secret_error)?;
        clients
            .into_iter()
            .map(|(client_id, client)| {
                let control = secret_control("runtime-auth-plan-set")?;
                let mutation = store
                    .plan_create(&secret_key(client)?, generation, &control)
                    .map_err(map_secret_error)?;
                Ok(ClientCredentialProvisioningPlan {
                    client_id,
                    client,
                    mutation,
                })
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
    }

    /// Executes or reconciles an already durable first-install plan, then loads the exact set.
    ///
    /// Existing planned targets are never replaced. A caller can safely retry this operation
    /// after process interruption because every target was fixed before the first mutation.
    pub fn provision_planned_set(
        store: Arc<dyn SecretStore>,
        plans: &[ClientCredentialProvisioningPlan],
    ) -> Result<(Self, Box<[ClientCredentialRegistration]>), CredentialError> {
        let clients = plans
            .iter()
            .map(|plan| (plan.client_id, plan.client))
            .collect::<Vec<_>>();
        validate_clients(&clients)?;
        let mut registrations = Vec::new();
        registrations
            .try_reserve_exact(plans.len())
            .map_err(|_| CredentialError::Unavailable)?;
        for plan in plans {
            let control = secret_control("runtime-auth-execute-planned-set")?;
            let key = secret_key(plan.client)?;
            match store
                .inspect_planned(&key, &plan.mutation, &control)
                .map_err(map_secret_error)?
            {
                SecretReconciliationObservation::Absent => {
                    if let Err(failure) = store.execute_planned(
                        &key,
                        &plan.mutation,
                        generate_credential()?,
                        &control,
                    ) && !matches!(
                        store.inspect_planned(&key, &plan.mutation, &control),
                        Ok(SecretReconciliationObservation::PresentUnverified)
                            | Ok(SecretReconciliationObservation::Matches)
                    ) {
                        return Err(map_secret_error(failure.into_error()));
                    }
                }
                SecretReconciliationObservation::PresentUnverified
                | SecretReconciliationObservation::Matches => {}
                SecretReconciliationObservation::Mismatch => {
                    return Err(CredentialError::SecretStore);
                }
            }
            registrations.push(ClientCredentialRegistration::new(
                plan.client_id,
                plan.client,
                plan.mutation.target().clone(),
            ));
        }
        let registry = Self::try_load(Arc::clone(&store), registrations.clone())?;
        Ok((registry, registrations.into_boxed_slice()))
    }

    /// Loads exact registered generations into zeroizing service memory.
    pub fn try_load(
        store: Arc<dyn SecretStore>,
        registrations: impl IntoIterator<Item = ClientCredentialRegistration>,
    ) -> Result<Self, CredentialError> {
        let mut active = HashMap::new();
        let mut names = HashSet::new();
        for registration in registrations {
            if active.contains_key(&registration.client_id) || !names.insert(registration.client) {
                return Err(CredentialError::DuplicateRegistration);
            }
            let control = secret_control("runtime-auth-load")?;
            let value = store
                .read(&registration.reference, &control)
                .map_err(map_secret_error)?;
            active.insert(
                registration.client_id,
                ActiveCredential {
                    registration,
                    value,
                    pending: None,
                },
            );
        }
        Ok(Self {
            store,
            active: RwLock::new(active),
        })
    }

    /// Provisions a separate high-entropy first generation for one named client.
    pub fn provision(
        store: Arc<dyn SecretStore>,
        client_id: ClientId,
        client: NamedClient,
    ) -> Result<(Self, ClientCredentialRegistration), CredentialError> {
        let (registry, registrations) =
            Self::provision_set(Arc::clone(&store), [(client_id, client)])?;
        let registration = registrations
            .into_vec()
            .pop()
            .ok_or(CredentialError::Unavailable)?;
        Ok((registry, registration))
    }

    /// Provisions one atomic in-process credential set and cleans every created generation if a
    /// later member or final registry load fails.
    ///
    /// The caller must durably retain the returned non-secret registrations before publishing the
    /// service. A process crash before that commit leaves a recoverable secret-store conflict; it
    /// never causes the service to replace an unknown credential generation.
    pub fn provision_set(
        store: Arc<dyn SecretStore>,
        clients: impl IntoIterator<Item = (ClientId, NamedClient)>,
    ) -> Result<(Self, Box<[ClientCredentialRegistration]>), CredentialError> {
        let clients = clients.into_iter().collect::<Vec<_>>();
        validate_clients(&clients)?;
        let generation = SecretGeneration::new(1).map_err(map_secret_error)?;
        let mut registrations = Vec::new();
        registrations
            .try_reserve_exact(clients.len())
            .map_err(|_| CredentialError::Unavailable)?;
        for (client_id, client) in clients {
            let result = (|| {
                let reference = store
                    .create(
                        &secret_key(client)?,
                        generation,
                        generate_credential()?,
                        &secret_control("runtime-auth-create-set")?,
                    )
                    .map_err(map_secret_error)?;
                Ok(ClientCredentialRegistration::new(
                    client_id, client, reference,
                ))
            })();
            match result {
                Ok(registration) => registrations.push(registration),
                Err(error) => {
                    cleanup_registrations(&store, &registrations);
                    return Err(error);
                }
            }
        }
        match Self::try_load(Arc::clone(&store), registrations.clone()) {
            Ok(registry) => Ok((registry, registrations.into_boxed_slice())),
            Err(error) => {
                cleanup_registrations(&store, &registrations);
                Err(error)
            }
        }
    }

    /// Authenticates one exact client/generation using constant-time byte comparison.
    pub fn authenticate(
        &self,
        client_id: ClientId,
        generation: CredentialGeneration,
        presented: &[u8],
    ) -> Result<NamedClient, CredentialError> {
        let active = self
            .active
            .read()
            .map_err(|_| CredentialError::Unavailable)?;
        let credential = active
            .get(&client_id)
            .ok_or(CredentialError::AuthenticationFailed)?;
        let expected = if credential.registration.generation() == generation {
            credential.value.expose_secret().as_bytes()
        } else if let Some(pending) = &credential.pending
            && pending.registration.generation() == generation
        {
            pending.value.expose_secret().as_bytes()
        } else {
            return Err(CredentialError::GenerationMismatch);
        };
        if expected.len() != presented.len() || expected.ct_eq(presented).unwrap_u8() != 1 {
            return Err(CredentialError::AuthenticationFailed);
        }
        Ok(credential.registration.client)
    }

    /// Creates a higher candidate while retaining the active generation until durable commit.
    pub fn begin_rotation(
        &self,
        client_id: ClientId,
    ) -> Result<ClientCredentialRegistration, CredentialError> {
        let mut active = self
            .active
            .write()
            .map_err(|_| CredentialError::Unavailable)?;
        let current = active.get(&client_id).ok_or(CredentialError::NotFound)?;
        if current.pending.is_some() {
            return Err(CredentialError::RotationInProgress);
        }
        let next_value = current
            .registration
            .generation()
            .get()
            .checked_add(1)
            .ok_or(CredentialError::GenerationExhausted)?;
        let next_generation = CredentialGeneration::try_new(next_value)?.to_secret_generation()?;
        let candidate = generate_credential()?;
        let key = secret_key(current.registration.client)?;
        let reference = self
            .store
            .replace(
                &key,
                &current.registration.reference,
                next_generation,
                candidate,
                &secret_control("runtime-auth-rotate")?,
            )
            .map_err(map_secret_error)?;
        let registration =
            ClientCredentialRegistration::new(client_id, current.registration.client, reference);
        let value = match self.store.read(
            &registration.reference,
            &secret_control("runtime-auth-reload")?,
        ) {
            Ok(value) => value,
            Err(error) => {
                if let Ok(control) = secret_control("runtime-auth-rotate-cleanup") {
                    let _cleanup = self.store.delete(&registration.reference, &control);
                }
                return Err(map_secret_error(error));
            }
        };
        active
            .get_mut(&client_id)
            .ok_or(CredentialError::NotFound)?
            .pending = Some(PendingCredential {
            registration: registration.clone(),
            value,
        });
        Ok(registration)
    }

    /// Activates a durably recorded candidate and reports whether prior-secret cleanup completed.
    pub fn commit_rotation(
        &self,
        client_id: ClientId,
        candidate_generation: CredentialGeneration,
    ) -> Result<CredentialRotationOutcome, CredentialError> {
        let retire_control = secret_control("runtime-auth-retire")?;
        let mut active = self
            .active
            .write()
            .map_err(|_| CredentialError::Unavailable)?;
        let credential = active
            .get_mut(&client_id)
            .ok_or(CredentialError::NotFound)?;
        if credential
            .pending
            .as_ref()
            .is_none_or(|pending| pending.registration.generation() != candidate_generation)
        {
            return Err(CredentialError::GenerationMismatch);
        }
        let pending = credential
            .pending
            .take()
            .ok_or(CredentialError::GenerationMismatch)?;
        let prior_registration =
            std::mem::replace(&mut credential.registration, pending.registration.clone());
        credential.value = pending.value;
        drop(active);
        let prior_retired = self
            .store
            .delete(prior_registration.reference(), &retire_control)
            .is_ok();
        Ok(CredentialRotationOutcome {
            registration: pending.registration,
            prior_retired,
        })
    }

    /// Removes an uncommitted candidate while leaving the current generation authoritative.
    pub fn abort_rotation(
        &self,
        client_id: ClientId,
        candidate_generation: CredentialGeneration,
    ) -> Result<(), CredentialError> {
        let mut active = self
            .active
            .write()
            .map_err(|_| CredentialError::Unavailable)?;
        let credential = active
            .get_mut(&client_id)
            .ok_or(CredentialError::NotFound)?;
        let pending = credential
            .pending
            .as_ref()
            .filter(|pending| pending.registration.generation() == candidate_generation)
            .ok_or(CredentialError::GenerationMismatch)?;
        self.store
            .delete(
                pending.registration.reference(),
                &secret_control("runtime-auth-abort-rotation")?,
            )
            .map_err(map_secret_error)?;
        credential.pending = None;
        Ok(())
    }

    /// Revokes authentication before deleting current and any pending exact generations.
    pub fn revoke(&self, client_id: ClientId) -> Result<(), CredentialError> {
        let removed = self
            .active
            .write()
            .map_err(|_| CredentialError::Unavailable)?
            .remove(&client_id)
            .ok_or(CredentialError::NotFound)?;
        let current = self
            .store
            .delete(
                &removed.registration.reference,
                &secret_control("runtime-auth-revoke")?,
            )
            .map_err(map_secret_error);
        let pending = removed.pending.map_or(Ok(()), |pending| {
            self.store
                .delete(
                    pending.registration.reference(),
                    &secret_control("runtime-auth-revoke-pending")?,
                )
                .map_err(map_secret_error)
        });
        current.and(pending)
    }
}

fn validate_clients(clients: &[(ClientId, NamedClient)]) -> Result<(), CredentialError> {
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    if clients.is_empty()
        || clients
            .iter()
            .any(|(id, client)| !ids.insert(*id) || !names.insert(*client))
    {
        Err(CredentialError::DuplicateRegistration)
    } else {
        Ok(())
    }
}

/// Non-secret completion evidence for a two-phase credential rotation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialRotationOutcome {
    registration: ClientCredentialRegistration,
    prior_retired: bool,
}

impl CredentialRotationOutcome {
    /// Newly authoritative registration that the caller durably recorded before commit.
    #[must_use]
    pub const fn registration(&self) -> &ClientCredentialRegistration {
        &self.registration
    }

    /// Whether the prior secret generation was removed during commit.
    #[must_use]
    pub const fn prior_retired(&self) -> bool {
        self.prior_retired
    }
}

fn generate_credential() -> Result<SecretValue, CredentialError> {
    let mut bytes = [0_u8; CREDENTIAL_BYTES];
    fill_random(&mut bytes).map_err(|_| CredentialError::RandomUnavailable)?;
    let mut encoded = String::with_capacity(CREDENTIAL_BYTES * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SecretValue::new(encoded).map_err(|_| CredentialError::RandomUnavailable)
}

fn secret_key(client: NamedClient) -> Result<SecretKey, CredentialError> {
    SecretKey::try_new(SECRET_SCOPE, client.secret_name()).map_err(map_secret_error)
}

fn secret_control(owner: &'static str) -> Result<SecretOperationControl, CredentialError> {
    let deadline = Instant::now()
        .checked_add(SECRET_OPERATION_TIMEOUT)
        .ok_or(CredentialError::Unavailable)?;
    SecretOperationControl::try_new(
        owner,
        deadline,
        1,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .map_err(map_secret_error)
}

fn cleanup_registrations(
    store: &Arc<dyn SecretStore>,
    registrations: &[ClientCredentialRegistration],
) {
    let Ok(control) = secret_control("runtime-auth-create-set-cleanup") else {
        return;
    };
    for registration in registrations {
        let _cleanup = store.delete(registration.reference(), &control);
    }
}

fn map_secret_error(_error: LocalSecretStoreError) -> CredentialError {
    CredentialError::SecretStore
}

/// Named-client credential lifecycle failure without secret contents.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CredentialError {
    /// A client ID or named class was registered more than once.
    #[error("client credential registration is duplicated")]
    DuplicateRegistration,
    /// Client is not registered.
    #[error("client credential registration was not found")]
    NotFound,
    /// Presented credential generation is stale or otherwise incorrect.
    #[error("client credential generation does not match")]
    GenerationMismatch,
    /// Presented bearer bytes did not authenticate.
    #[error("client authentication failed")]
    AuthenticationFailed,
    /// Credential generation cannot advance.
    #[error("client credential generation is exhausted")]
    GenerationExhausted,
    /// Another candidate must be committed or aborted before rotation can continue.
    #[error("client credential rotation is already in progress")]
    RotationInProgress,
    /// Operating-system entropy was unavailable.
    #[error("client credential entropy is unavailable")]
    RandomUnavailable,
    /// Existing local secret authority failed without exposing provider details.
    #[error("client credential store is unavailable")]
    SecretStore,
    /// Registry lock or bounded lifecycle state is unavailable.
    #[error("client credential registry is unavailable")]
    Unavailable,
}
