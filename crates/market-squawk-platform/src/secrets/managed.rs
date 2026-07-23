//! Generation-bound secret references and bounded operation control.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::{LocalSecretStoreError, SecretKey};

const SECRET_REFERENCE_VERSION: u16 = 1;
const MAX_OPERATION_OWNER_BYTES: usize = 128;
const MAX_RETRY_BUDGET: u8 = 8;
const OPAQUE_LOCATOR_BYTES: usize = 64;

/// Concrete local backend named by an opaque secret reference.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretBackend {
    /// Capability-confined encrypted file vault.
    EncryptedFile,
    /// Apple Keychain Services.
    AppleKeychain,
    /// Windows Credential Manager.
    WindowsCredentialManager,
    /// freedesktop Secret Service.
    SecretService,
}

/// Monotonic nonzero credential generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SecretGeneration(NonZeroU64);

impl SecretGeneration {
    /// Constructs a nonzero credential generation.
    pub fn new(value: u64) -> Result<Self, LocalSecretStoreError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(LocalSecretStoreError::InvalidGeneration)
    }

    /// Returns the portable integer generation.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Opaque backend locator and generation safe for non-secret catalog metadata.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "SecretRefWire", into = "SecretRefWire")]
pub struct SecretRef {
    backend: SecretBackend,
    locator: String,
    generation: SecretGeneration,
}

impl SecretRef {
    pub(super) fn from_key(
        key: &SecretKey,
        backend: SecretBackend,
        generation: SecretGeneration,
    ) -> Result<Self, LocalSecretStoreError> {
        Ok(Self {
            backend,
            locator: key.generation_token(backend, generation)?,
            generation,
        })
    }

    /// Returns the exact backend selected for this reference.
    pub const fn backend(&self) -> SecretBackend {
        self.backend
    }

    /// Returns the credential generation without exposing the backend locator.
    pub const fn generation(&self) -> SecretGeneration {
        self.generation
    }

    pub(super) fn locator(&self) -> &str {
        &self.locator
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretRef")
            .field("backend", &self.backend)
            .field("generation", &self.generation)
            .field("locator", &"[OPAQUE]")
            .finish()
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecretRefWire {
    version: u16,
    backend: SecretBackend,
    locator: String,
    generation: SecretGeneration,
}

impl TryFrom<SecretRefWire> for SecretRef {
    type Error = LocalSecretStoreError;

    fn try_from(wire: SecretRefWire) -> Result<Self, Self::Error> {
        if wire.version != SECRET_REFERENCE_VERSION
            || wire.locator.len() != OPAQUE_LOCATOR_BYTES
            || !wire
                .locator
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        Ok(Self {
            backend: wire.backend,
            locator: wire.locator,
            generation: wire.generation,
        })
    }
}

impl From<SecretRef> for SecretRefWire {
    fn from(reference: SecretRef) -> Self {
        Self {
            version: SECRET_REFERENCE_VERSION,
            backend: reference.backend,
            locator: reference.locator,
            generation: reference.generation,
        }
    }
}

/// Whether the caller permits an operating-system interaction prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretInteractionPolicy {
    /// Do not initiate an operation that may prompt.
    Forbid,
    /// Permit the platform to display its native interaction.
    AllowPlatformPrompt,
}

/// Interaction behavior exposed by a secret backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretInteractionCapability {
    /// The backend never presents an operating-system prompt.
    Never,
    /// The backend may present a platform-owned prompt.
    PlatformManaged,
}

/// Deadline behavior honestly supported by a synchronous backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretDeadlineCapability {
    /// Deadline and cancellation are enforced immediately before and after one bounded operation.
    OperationBoundaries,
}

/// Non-secret backend capabilities returned by a successful probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecretStoreCapabilities {
    backend: SecretBackend,
    interaction: SecretInteractionCapability,
    deadline: SecretDeadlineCapability,
    exact_create: bool,
    exact_read: bool,
    exact_replace: bool,
    exact_delete: bool,
}

impl SecretStoreCapabilities {
    pub(super) const fn new(
        backend: SecretBackend,
        interaction: SecretInteractionCapability,
    ) -> Self {
        Self {
            backend,
            interaction,
            deadline: SecretDeadlineCapability::OperationBoundaries,
            exact_create: true,
            exact_read: true,
            exact_replace: true,
            exact_delete: true,
        }
    }

    /// Returns the exact backend.
    pub const fn backend(self) -> SecretBackend {
        self.backend
    }

    /// Returns whether the backend may use a platform-owned prompt.
    pub const fn interaction(self) -> SecretInteractionCapability {
        self.interaction
    }

    /// Returns the deadline enforcement model.
    pub const fn deadline(self) -> SecretDeadlineCapability {
        self.deadline
    }

    /// Returns whether all generation-bound CRUD operations are implemented.
    pub const fn supports_exact_lifecycle(self) -> bool {
        self.exact_create && self.exact_read && self.exact_replace && self.exact_delete
    }
}

/// Cloneable cooperative cancellation owned by one onboarding operation.
#[derive(Clone, Debug, Default)]
pub struct SecretCancellation {
    cancelled: Arc<AtomicBool>,
}

impl SecretCancellation {
    /// Constructs an uncancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Irreversibly cancels the operation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// One-owner, monotonic-deadline control for a bounded secret operation.
pub struct SecretOperationControl {
    owner: String,
    deadline: Instant,
    retry_budget: u8,
    interaction: SecretInteractionPolicy,
    cancellation: SecretCancellation,
}

impl SecretOperationControl {
    /// Constructs bounded control state for one lifecycle operation.
    pub fn try_new(
        owner: impl Into<String>,
        deadline: Instant,
        retry_budget: u8,
        interaction: SecretInteractionPolicy,
        cancellation: SecretCancellation,
    ) -> Result<Self, LocalSecretStoreError> {
        let owner = owner.into();
        if owner.is_empty()
            || owner.len() > MAX_OPERATION_OWNER_BYTES
            || owner.chars().any(char::is_control)
            || retry_budget > MAX_RETRY_BUDGET
        {
            return Err(LocalSecretStoreError::InvalidOperationControl);
        }
        Ok(Self {
            owner,
            deadline,
            retry_budget,
            interaction,
            cancellation,
        })
    }

    /// Returns the non-secret operation owner.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the fixed retry budget; backends may not exceed it.
    pub const fn retry_budget(&self) -> u8 {
        self.retry_budget
    }

    pub(super) fn preflight(
        &self,
        capabilities: SecretStoreCapabilities,
    ) -> Result<(), LocalSecretStoreError> {
        self.check_boundary(false)?;
        if capabilities.interaction == SecretInteractionCapability::PlatformManaged
            && self.interaction == SecretInteractionPolicy::Forbid
        {
            return Err(LocalSecretStoreError::InteractionRequired);
        }
        Ok(())
    }

    pub(super) fn read_postflight(&self) -> Result<(), LocalSecretStoreError> {
        self.check_boundary(false)
    }

    pub(super) fn mutation_postflight(&self) -> Result<(), LocalSecretStoreError> {
        self.check_boundary(true)
    }

    fn check_boundary(
        &self,
        mutation_may_have_completed: bool,
    ) -> Result<(), LocalSecretStoreError> {
        let cancelled = self.cancellation.is_cancelled();
        let expired = Instant::now() >= self.deadline;
        if mutation_may_have_completed && (cancelled || expired) {
            return Err(LocalSecretStoreError::IndeterminateCompletion);
        }
        if cancelled {
            return Err(LocalSecretStoreError::OperationCancelled);
        }
        if expired {
            return Err(LocalSecretStoreError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl fmt::Debug for SecretOperationControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretOperationControl")
            .field("owner", &self.owner)
            .field("retry_budget", &self.retry_budget)
            .field("interaction", &self.interaction)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}
