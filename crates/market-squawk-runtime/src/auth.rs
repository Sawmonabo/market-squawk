//! Service-lifetime named-client credentials and exact-generation authentication.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::RwLock,
};

use getrandom::fill as fill_random;
use market_squawk_platform::SecretValue;
use serde::{Deserialize, Deserializer, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;

use crate::ClientId;

const CREDENTIAL_BYTES: usize = 32;

/// Runtime-owned non-secret view of an exact named-client credential generation.
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

/// Non-secret metadata for one service-lifetime named credential generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientCredentialRegistration {
    client_id: ClientId,
    client: NamedClient,
    generation: CredentialGeneration,
}

impl ClientCredentialRegistration {
    /// Binds a named client identity to one exact service-lifetime generation.
    #[must_use]
    pub const fn new(
        client_id: ClientId,
        client: NamedClient,
        generation: CredentialGeneration,
    ) -> Self {
        Self {
            client_id,
            client,
            generation,
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
    pub const fn generation(&self) -> CredentialGeneration {
        self.generation
    }
}

/// Immutable plan for advancing one service-lifetime credential by one generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClientCredentialRotationPlan {
    current: ClientCredentialRegistration,
    candidate: ClientCredentialRegistration,
}

impl ClientCredentialRotationPlan {
    /// Registered client identity retained across the rotation.
    #[must_use]
    pub const fn client_id(&self) -> ClientId {
        self.current.client_id()
    }

    /// Closed named-client class retained across the rotation.
    #[must_use]
    pub const fn client(&self) -> NamedClient {
        self.current.client()
    }

    /// Exact pre-rotation registration that remains authoritative until commit.
    #[must_use]
    pub const fn current(&self) -> &ClientCredentialRegistration {
        &self.current
    }

    /// Candidate registration fixed before credential bytes are generated.
    #[must_use]
    pub fn candidate(&self) -> ClientCredentialRegistration {
        self.candidate.clone()
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

/// In-process verifier whose credential bytes never enter an OS keyring or durable file.
pub struct CredentialRegistry {
    active: RwLock<HashMap<ClientId, ActiveCredential>>,
}

impl fmt::Debug for CredentialRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CredentialRegistry([REDACTED CLIENT CREDENTIALS])")
    }
}

impl CredentialRegistry {
    /// Provisions a separate high-entropy first generation for one named client.
    pub fn provision(
        client_id: ClientId,
        client: NamedClient,
    ) -> Result<(Self, ClientCredentialRegistration), CredentialError> {
        let (registry, registrations) = Self::provision_set([(client_id, client)])?;
        let registration = registrations
            .into_vec()
            .pop()
            .ok_or(CredentialError::Unavailable)?;
        Ok((registry, registration))
    }

    /// Provisions one bounded credential set entirely in zeroizing process memory.
    pub fn provision_set(
        clients: impl IntoIterator<Item = (ClientId, NamedClient)>,
    ) -> Result<(Self, Box<[ClientCredentialRegistration]>), CredentialError> {
        let clients = clients.into_iter().collect::<Vec<_>>();
        validate_clients(&clients)?;
        let generation = CredentialGeneration::try_new(1)?;
        let mut active = HashMap::new();
        let mut registrations = Vec::new();
        active
            .try_reserve(clients.len())
            .map_err(|_| CredentialError::Unavailable)?;
        registrations
            .try_reserve_exact(clients.len())
            .map_err(|_| CredentialError::Unavailable)?;
        for (client_id, client) in clients {
            let registration = ClientCredentialRegistration::new(client_id, client, generation);
            let previous = active.insert(
                client_id,
                ActiveCredential {
                    registration: registration.clone(),
                    value: generate_credential()?,
                    pending: None,
                },
            );
            if previous.is_some() {
                return Err(CredentialError::DuplicateRegistration);
            }
            registrations.push(registration);
        }
        Ok((
            Self {
                active: RwLock::new(active),
            },
            registrations.into_boxed_slice(),
        ))
    }

    /// Returns the complete registered client set for bounded runtime activity accounting.
    pub fn registered_client_ids(&self) -> Result<Box<[ClientId]>, CredentialError> {
        let active = self
            .active
            .read()
            .map_err(|_| CredentialError::Unavailable)?;
        let mut clients = Vec::new();
        clients
            .try_reserve_exact(active.len())
            .map_err(|_| CredentialError::Unavailable)?;
        clients.extend(active.keys().copied());
        clients.sort_unstable();
        Ok(clients.into_boxed_slice())
    }

    /// Copies one exact credential for owner-authenticated local admission.
    pub fn credential(
        &self,
        registration: &ClientCredentialRegistration,
    ) -> Result<SecretValue, CredentialError> {
        let active = self
            .active
            .read()
            .map_err(|_| CredentialError::Unavailable)?;
        let credential = active
            .get(&registration.client_id())
            .ok_or(CredentialError::NotFound)?;
        let value = if credential.registration == *registration {
            &credential.value
        } else if let Some(pending) = &credential.pending
            && pending.registration == *registration
        {
            &pending.value
        } else {
            return Err(CredentialError::GenerationMismatch);
        };
        duplicate_secret(value)
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
        Ok(credential.registration.client())
    }

    /// Freezes the next generation without changing current authentication authority.
    pub fn plan_rotation(
        &self,
        client_id: ClientId,
    ) -> Result<ClientCredentialRotationPlan, CredentialError> {
        let active = self
            .active
            .read()
            .map_err(|_| CredentialError::Unavailable)?;
        let current = active.get(&client_id).ok_or(CredentialError::NotFound)?;
        if current.pending.is_some() {
            return Err(CredentialError::RotationInProgress);
        }
        let next = current
            .registration
            .generation()
            .get()
            .checked_add(1)
            .ok_or(CredentialError::GenerationExhausted)
            .and_then(CredentialGeneration::try_new)?;
        Ok(ClientCredentialRotationPlan {
            current: current.registration.clone(),
            candidate: ClientCredentialRegistration::new(
                client_id,
                current.registration.client(),
                next,
            ),
        })
    }

    /// Generates the planned candidate into a non-authoritative in-memory slot.
    pub fn begin_planned_rotation(
        &self,
        plan: &ClientCredentialRotationPlan,
    ) -> Result<ClientCredentialRegistration, CredentialError> {
        validate_rotation_plan(plan)?;
        let value = generate_credential()?;
        let mut active = self
            .active
            .write()
            .map_err(|_| CredentialError::Unavailable)?;
        let current = active
            .get_mut(&plan.client_id())
            .ok_or(CredentialError::NotFound)?;
        if current.registration != *plan.current() {
            return Err(CredentialError::GenerationMismatch);
        }
        if current.pending.is_some() {
            return Err(CredentialError::RotationInProgress);
        }
        current.pending = Some(PendingCredential {
            registration: plan.candidate(),
            value,
        });
        Ok(plan.candidate())
    }

    /// Activates one candidate and zeroizes the prior credential when it is dropped.
    pub fn commit_rotation(
        &self,
        client_id: ClientId,
        candidate_generation: CredentialGeneration,
    ) -> Result<CredentialRotationOutcome, CredentialError> {
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
        credential.registration = pending.registration.clone();
        credential.value = pending.value;
        Ok(CredentialRotationOutcome {
            registration: pending.registration,
            prior_retired: true,
        })
    }

    /// Drops an uncommitted candidate while leaving the current generation authoritative.
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
        if credential
            .pending
            .as_ref()
            .is_none_or(|pending| pending.registration.generation() != candidate_generation)
        {
            return Err(CredentialError::GenerationMismatch);
        }
        credential.pending = None;
        Ok(())
    }

    /// Revokes a named client and zeroizes its current and pending credentials on drop.
    pub fn revoke(&self, client_id: ClientId) -> Result<(), CredentialError> {
        self.active
            .write()
            .map_err(|_| CredentialError::Unavailable)?
            .remove(&client_id)
            .map(|_removed| ())
            .ok_or(CredentialError::NotFound)
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

fn validate_rotation_plan(plan: &ClientCredentialRotationPlan) -> Result<(), CredentialError> {
    let expected = plan
        .current()
        .generation()
        .get()
        .checked_add(1)
        .ok_or(CredentialError::GenerationExhausted)?;
    if plan.current().client_id() != plan.candidate.client_id()
        || plan.current().client() != plan.candidate.client()
        || plan.candidate.generation().get() != expected
    {
        return Err(CredentialError::GenerationMismatch);
    }
    Ok(())
}

/// Non-secret completion evidence for a credential rotation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialRotationOutcome {
    registration: ClientCredentialRegistration,
    prior_retired: bool,
}

impl CredentialRotationOutcome {
    /// Newly authoritative registration.
    #[must_use]
    pub const fn registration(&self) -> &ClientCredentialRegistration {
        &self.registration
    }

    /// Whether the prior in-memory credential was retired during commit.
    #[must_use]
    pub const fn prior_retired(&self) -> bool {
        self.prior_retired
    }
}

fn generate_credential() -> Result<SecretValue, CredentialError> {
    let mut bytes = [0_u8; CREDENTIAL_BYTES];
    fill_random(&mut bytes).map_err(|_| CredentialError::RandomUnavailable)?;
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(CredentialError::RandomUnavailable);
    }
    let mut encoded = String::with_capacity(CREDENTIAL_BYTES * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SecretValue::new(encoded).map_err(|_| CredentialError::RandomUnavailable)
}

fn duplicate_secret(secret: &SecretValue) -> Result<SecretValue, CredentialError> {
    SecretValue::new(secret.expose_secret().to_owned()).map_err(|_| CredentialError::Unavailable)
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
    /// Legacy compatibility value retained for stable application error mapping.
    #[error("client credential store is unavailable")]
    SecretStore,
    /// Legacy compatibility value retained for stable application error mapping.
    #[error("client credential store requires foreground user interaction")]
    SecretInteractionRequired,
    /// Registry lock or bounded lifecycle state is unavailable.
    #[error("client credential registry is unavailable")]
    Unavailable,
}
