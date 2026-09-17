//! Generation-bound secret references and bounded operation control.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::{LocalSecretStoreError, SecretKey};

const SECRET_REFERENCE_VERSION: u16 = 1;
const SECRET_MUTATION_PLAN_VERSION: u16 = 1;
const MAX_OPERATION_OWNER_BYTES: usize = 128;
const MAX_RETRY_BUDGET: u8 = 8;
const OPAQUE_LOCATOR_BYTES: usize = 64;
const SECRET_PLAN_KEY_DOMAIN: &[u8] = b"market-squawk-secret-plan-key-v1\0";

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

/// Exact local mutation described by a durable non-secret plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SecretMutationKind {
    /// Creates a generation without replacing an existing generation.
    Create,
    /// Retains an exact current generation while preparing its successor.
    Replace {
        /// Exact generation that must remain present while the candidate is prepared.
        current: SecretRef,
    },
}

/// Durable non-secret identity for one exact secret mutation.
///
/// A plan freezes backend selection before credential bytes can be written. Its key binding is a
/// domain-separated digest and its debug representation does not expose that digest or the opaque
/// locator.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "SecretMutationPlanWire", into = "SecretMutationPlanWire")]
pub struct SecretMutationPlan {
    kind: SecretMutationKind,
    key_binding: [u8; 32],
    target: SecretRef,
}

impl SecretMutationPlan {
    pub(super) fn create(
        key: &SecretKey,
        backend: SecretBackend,
        generation: SecretGeneration,
    ) -> Result<Self, LocalSecretStoreError> {
        Self::try_new(
            key,
            SecretMutationKind::Create,
            SecretRef::from_key(key, backend, generation)?,
        )
    }

    pub(super) fn replace(
        key: &SecretKey,
        current: SecretRef,
        candidate_generation: SecretGeneration,
    ) -> Result<Self, LocalSecretStoreError> {
        let target = SecretRef::from_key(key, current.backend(), candidate_generation)?;
        Self::try_new(key, SecretMutationKind::Replace { current }, target)
    }

    fn try_new(
        key: &SecretKey,
        kind: SecretMutationKind,
        target: SecretRef,
    ) -> Result<Self, LocalSecretStoreError> {
        let plan = Self {
            kind,
            key_binding: secret_key_binding(key)?,
            target,
        };
        plan.validate_for(key)?;
        Ok(plan)
    }

    /// Returns the exact mutation class.
    pub const fn kind(&self) -> &SecretMutationKind {
        &self.kind
    }

    /// Returns the exact backend, opaque locator, and generation selected before mutation.
    pub const fn target(&self) -> &SecretRef {
        &self.target
    }

    pub(super) fn validate_for(&self, key: &SecretKey) -> Result<(), LocalSecretStoreError> {
        if self.key_binding != secret_key_binding(key)?
            || self.target
                != SecretRef::from_key(key, self.target.backend(), self.target.generation())?
        {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        match &self.kind {
            SecretMutationKind::Create => Ok(()),
            SecretMutationKind::Replace { current } => {
                if current.backend() != self.target.backend()
                    || current.generation() >= self.target.generation()
                    || *current
                        != SecretRef::from_key(key, current.backend(), current.generation())?
                {
                    Err(LocalSecretStoreError::Conflict)
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl fmt::Debug for SecretMutationPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretMutationPlan")
            .field("kind", &self.kind)
            .field("key_binding", &"[BOUND]")
            .field("target", &self.target)
            .finish()
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecretMutationPlanWire {
    version: u16,
    kind: SecretMutationKind,
    key_binding: [u8; 32],
    target: SecretRef,
}

impl TryFrom<SecretMutationPlanWire> for SecretMutationPlan {
    type Error = LocalSecretStoreError;

    fn try_from(wire: SecretMutationPlanWire) -> Result<Self, Self::Error> {
        if wire.version != SECRET_MUTATION_PLAN_VERSION || wire.key_binding == [0; 32] {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        match &wire.kind {
            SecretMutationKind::Create => {}
            SecretMutationKind::Replace { current } => {
                if current.backend() != wire.target.backend()
                    || current.generation() >= wire.target.generation()
                {
                    return Err(LocalSecretStoreError::InvalidReference);
                }
            }
        }
        Ok(Self {
            kind: wire.kind,
            key_binding: wire.key_binding,
            target: wire.target,
        })
    }
}

impl From<SecretMutationPlan> for SecretMutationPlanWire {
    fn from(plan: SecretMutationPlan) -> Self {
        Self {
            version: SECRET_MUTATION_PLAN_VERSION,
            kind: plan.kind,
            key_binding: plan.key_binding,
            target: plan.target,
        }
    }
}

/// Whether a failed planned mutation is known to have had no effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretMutationEffect {
    /// The backend was not mutated.
    NoEffect,
    /// The exact target may have been mutated and must be reconciled.
    MayHaveApplied,
}

/// Successful exact planned-store disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretMutationDisposition {
    /// The exact target was written and verified.
    Stored,
    /// The exact target already contained the submitted value.
    AlreadyMatches,
}

/// Non-secret observation used to reconcile an interrupted planned mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretReconciliationObservation {
    /// The exact target is absent.
    Absent,
    /// The exact target exists but no submitted value was available for comparison.
    PresentUnverified,
    /// The exact target exists and matches the resubmitted value.
    Matches,
    /// The exact target exists but differs from the resubmitted value.
    Mismatch,
}

/// Successful exact planned-deletion disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecretDeletionDisposition {
    /// The exact target was deleted.
    Deleted,
    /// The exact target was already absent.
    AlreadyAbsent,
}

/// Planned mutation failure with explicit external-effect classification.
#[derive(Debug, Error)]
#[error("planned secret mutation failed")]
pub struct SecretMutationFailure {
    effect: SecretMutationEffect,
    #[source]
    error: LocalSecretStoreError,
}

impl SecretMutationFailure {
    pub(super) const fn no_effect(error: LocalSecretStoreError) -> Self {
        Self {
            effect: SecretMutationEffect::NoEffect,
            error,
        }
    }

    pub(super) const fn may_have_applied(error: LocalSecretStoreError) -> Self {
        Self {
            effect: SecretMutationEffect::MayHaveApplied,
            error,
        }
    }

    pub(super) const fn from_store_error(error: LocalSecretStoreError) -> Self {
        if matches!(
            error,
            LocalSecretStoreError::IndeterminateCompletion
                | LocalSecretStoreError::CleanupRequired
                | LocalSecretStoreError::PublicationFailed
                | LocalSecretStoreError::RotationRecoveryRequired
                | LocalSecretStoreError::RotationFinalizationPending
                | LocalSecretStoreError::AuthorityRecoveryRequired
                | LocalSecretStoreError::AuthorityFinalizationPending
        ) {
            Self::may_have_applied(error)
        } else {
            Self::no_effect(error)
        }
    }

    /// Returns whether exact reconciliation is required.
    pub const fn effect(&self) -> SecretMutationEffect {
        self.effect
    }

    /// Returns the redacted backend error.
    pub const fn error(&self) -> &LocalSecretStoreError {
        &self.error
    }

    /// Consumes the classification and returns the redacted backend error.
    pub fn into_error(self) -> LocalSecretStoreError {
        self.error
    }
}

fn secret_key_binding(key: &SecretKey) -> Result<[u8; 32], LocalSecretStoreError> {
    let token = key.token()?;
    let mut hasher = Sha256::new();
    hasher.update(SECRET_PLAN_KEY_DOMAIN);
    hasher.update((token.len() as u64).to_be_bytes());
    hasher.update(token.as_bytes());
    Ok(hasher.finalize().into())
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

    #[cfg(target_os = "macos")]
    pub(super) const fn interaction_policy(&self) -> SecretInteractionPolicy {
        self.interaction
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
