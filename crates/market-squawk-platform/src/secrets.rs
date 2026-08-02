//! OS-backed credentials and a capability-confined encrypted local fallback.

mod crypto;
mod encrypted;
mod keyring;
mod managed;
mod preferred;

use std::fmt;

use sha2::{Digest as _, Sha256};
use thiserror::Error;

use self::crypto::encode_hex;
use crate::{LocalAuthorityStateStoreError, SecretValue};

pub use self::encrypted::EncryptedFileSecretStore;
pub use self::keyring::OsKeyringSecretStore;
pub use self::managed::{
    SecretBackend, SecretCancellation, SecretDeadlineCapability, SecretDeletionDisposition,
    SecretGeneration, SecretInteractionCapability, SecretInteractionPolicy,
    SecretMutationDisposition, SecretMutationEffect, SecretMutationFailure, SecretMutationKind,
    SecretMutationPlan, SecretOperationControl, SecretReconciliationObservation, SecretRef,
    SecretStoreCapabilities,
};
pub use self::preferred::{
    EncryptedFileFallbackStatus, EncryptedFileSecretFallback, EncryptedFileUnlockCapability,
    PreferredSecretStore,
};

const MAX_SCOPE_BYTES: usize = 64;
const MAX_NAME_BYTES: usize = 128;

/// A validated secret identity whose debug representation never exposes either component.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct SecretKey {
    scope: String,
    name: String,
}

impl SecretKey {
    /// Constructs a bounded portable secret namespace and name.
    pub fn try_new(scope: &str, name: &str) -> Result<Self, LocalSecretStoreError> {
        if !valid_component(scope, MAX_SCOPE_BYTES) || !valid_component(name, MAX_NAME_BYTES) {
            return Err(LocalSecretStoreError::InvalidKey);
        }
        Ok(Self {
            scope: scope.to_owned(),
            name: name.to_owned(),
        })
    }

    fn token(&self) -> Result<String, LocalSecretStoreError> {
        let mut hasher = Sha256::new();
        hasher.update((self.scope.len() as u64).to_be_bytes());
        hasher.update(self.scope.as_bytes());
        hasher.update((self.name.len() as u64).to_be_bytes());
        hasher.update(self.name.as_bytes());
        encode_hex(&hasher.finalize())
    }

    fn generation_token(
        &self,
        backend: SecretBackend,
        generation: SecretGeneration,
    ) -> Result<String, LocalSecretStoreError> {
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-secret-reference-v1\0");
        hasher.update([match backend {
            SecretBackend::EncryptedFile => 1,
            SecretBackend::AppleKeychain => 2,
            SecretBackend::WindowsCredentialManager => 3,
            SecretBackend::SecretService => 4,
        }]);
        hasher.update((self.scope.len() as u64).to_be_bytes());
        hasher.update(self.scope.as_bytes());
        hasher.update((self.name.len() as u64).to_be_bytes());
        hasher.update(self.name.as_bytes());
        hasher.update(generation.get().to_be_bytes());
        encode_hex(&hasher.finalize())
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretKey([REDACTED])")
    }
}

/// Replaceable storage contract for local credential material.
pub trait SecretStore: fmt::Debug + Send + Sync {
    /// Returns the non-secret readiness of an optional encrypted-file fallback.
    fn encrypted_file_fallback_status(
        &self,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        Ok(EncryptedFileFallbackStatus::Disabled)
    }

    /// Consumes explicit foreground unlock authority into process memory.
    fn unlock_encrypted_file_fallback(
        &self,
        _unlock: EncryptedFileUnlockCapability,
        _control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        Err(LocalSecretStoreError::UnsupportedOperation)
    }

    /// Drops process-held encrypted-file unlock authority.
    fn lock_encrypted_file_fallback(
        &self,
        _control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        Err(LocalSecretStoreError::UnsupportedOperation)
    }

    /// Probes non-secret backend capabilities without storing credential material.
    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError>;

    /// Selects and validates one exact create target without storing credential material.
    fn plan_create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError>;

    /// Selects and validates one exact replacement target without storing credential material.
    fn plan_replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError>;

    /// Executes only the backend and locator retained by an already durable plan.
    fn execute_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationDisposition, SecretMutationFailure>;

    /// Observes whether an exact planned target exists without returning credential material.
    fn inspect_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError>;

    /// Constant-work compares one resubmitted value with the exact planned target.
    fn matches_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        expected: &SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError>;

    /// Deletes only the planned target and treats an already absent target as reconciled.
    fn delete_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretDeletionDisposition, SecretMutationFailure>;

    /// Creates one exact generation and rejects an existing locator.
    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError>;

    /// Reads one exact opaque generation.
    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError>;

    /// Stores a higher candidate generation while retaining the current generation.
    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError>;

    /// Deletes only the exact opaque generation.
    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError>;

    /// Stores or rotates one secret without retaining the submitted value.
    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError>;

    /// Loads one secret into zeroizing, redacted memory.
    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError>;
}

fn execute_exact_plan(
    store: &dyn SecretStore,
    backend: SecretBackend,
    key: &SecretKey,
    plan: &SecretMutationPlan,
    value: SecretValue,
    control: &SecretOperationControl,
) -> Result<SecretMutationDisposition, SecretMutationFailure> {
    plan.validate_for(key)
        .map_err(SecretMutationFailure::no_effect)?;
    if plan.target().backend() != backend {
        return Err(SecretMutationFailure::no_effect(
            LocalSecretStoreError::InvalidReference,
        ));
    }
    match store.read(plan.target(), control) {
        Ok(existing) => {
            return if secret_values_match(&existing, &value) {
                Ok(SecretMutationDisposition::AlreadyMatches)
            } else {
                Err(SecretMutationFailure::no_effect(
                    LocalSecretStoreError::Conflict,
                ))
            };
        }
        Err(LocalSecretStoreError::NotFound) => {}
        Err(error) => return Err(SecretMutationFailure::no_effect(error)),
    }
    let stored = match plan.kind() {
        SecretMutationKind::Create => store.create(key, plan.target().generation(), value, control),
        SecretMutationKind::Replace { current } => {
            store.replace(key, current, plan.target().generation(), value, control)
        }
    }
    .map_err(SecretMutationFailure::from_store_error)?;
    if stored == *plan.target() {
        Ok(SecretMutationDisposition::Stored)
    } else {
        Err(SecretMutationFailure::may_have_applied(
            LocalSecretStoreError::InvalidReference,
        ))
    }
}

fn inspect_exact_plan(
    store: &dyn SecretStore,
    backend: SecretBackend,
    key: &SecretKey,
    plan: &SecretMutationPlan,
    control: &SecretOperationControl,
) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
    plan.validate_for(key)?;
    if plan.target().backend() != backend {
        return Err(LocalSecretStoreError::InvalidReference);
    }
    match store.read(plan.target(), control) {
        Ok(_value) => Ok(SecretReconciliationObservation::PresentUnverified),
        Err(LocalSecretStoreError::NotFound) => Ok(SecretReconciliationObservation::Absent),
        Err(error) => Err(error),
    }
}

fn match_exact_plan(
    store: &dyn SecretStore,
    backend: SecretBackend,
    key: &SecretKey,
    plan: &SecretMutationPlan,
    expected: &SecretValue,
    control: &SecretOperationControl,
) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
    plan.validate_for(key)?;
    if plan.target().backend() != backend {
        return Err(LocalSecretStoreError::InvalidReference);
    }
    match store.read(plan.target(), control) {
        Ok(value) if secret_values_match(&value, expected) => {
            Ok(SecretReconciliationObservation::Matches)
        }
        Ok(_value) => Ok(SecretReconciliationObservation::Mismatch),
        Err(LocalSecretStoreError::NotFound) => Ok(SecretReconciliationObservation::Absent),
        Err(error) => Err(error),
    }
}

fn delete_exact_plan(
    store: &dyn SecretStore,
    backend: SecretBackend,
    key: &SecretKey,
    plan: &SecretMutationPlan,
    control: &SecretOperationControl,
) -> Result<SecretDeletionDisposition, SecretMutationFailure> {
    plan.validate_for(key)
        .map_err(SecretMutationFailure::no_effect)?;
    if plan.target().backend() != backend {
        return Err(SecretMutationFailure::no_effect(
            LocalSecretStoreError::InvalidReference,
        ));
    }
    match store.delete(plan.target(), control) {
        Ok(()) => Ok(SecretDeletionDisposition::Deleted),
        Err(LocalSecretStoreError::NotFound) => Ok(SecretDeletionDisposition::AlreadyAbsent),
        Err(error) => Err(SecretMutationFailure::from_store_error(error)),
    }
}

fn secret_values_match(left: &SecretValue, right: &SecretValue) -> bool {
    let left = left.expose_secret().as_bytes();
    let right = right.expose_secret().as_bytes();
    let mut difference = left.len() ^ right.len();
    for (left, right) in left.iter().zip(right) {
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

/// Durable authority selected while resolving an interrupted unlock rotation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RotationAuthority {
    /// The pre-rotation unlock remains authoritative.
    Prior,
    /// The candidate unlock became authoritative.
    Candidate,
}

/// Successful unlock-rotation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RotationOutcome {
    /// Candidate publication committed and prior-key recovery material was removed.
    Complete,
}

/// Secret-provider, KDF, authentication, recovery, or confined-publication failure.
#[derive(Debug, Error)]
pub enum LocalSecretStoreError {
    /// Secret namespace or name is invalid.
    #[error("secret key is invalid")]
    InvalidKey,
    /// A credential generation was zero.
    #[error("secret generation is invalid")]
    InvalidGeneration,
    /// An opaque secret reference failed exact validation.
    #[error("secret reference is invalid")]
    InvalidReference,
    /// Operation ownership, deadline, or retry policy was invalid.
    #[error("secret operation control is invalid")]
    InvalidOperationControl,
    /// Submitted or decrypted secret material violates the in-memory bound.
    #[error("secret value is invalid")]
    InvalidSecret,
    /// OS credential storage is unavailable or rejected the operation.
    #[error("operating-system secret provider is unavailable")]
    ProviderUnavailable,
    /// The selected secure-store session or service is not available.
    #[error("operating-system secret session is unavailable")]
    SessionUnavailable,
    /// The store is locked or requires unavailable user interaction.
    #[error("secret store is locked")]
    Locked,
    /// The operation requires a platform prompt forbidden by caller policy.
    #[error("secret operation requires user interaction")]
    InteractionRequired,
    /// The platform reported that the user cancelled its prompt.
    #[error("secret operation was cancelled by the user")]
    UserCancelled,
    /// Cooperative cancellation was observed before a side effect.
    #[error("secret operation was cancelled")]
    OperationCancelled,
    /// The monotonic deadline elapsed before a side effect.
    #[error("secret operation deadline elapsed")]
    DeadlineExceeded,
    /// A mutating operation may have completed and requires exact reconciliation.
    #[error("secret operation completion is indeterminate")]
    IndeterminateCompletion,
    /// The requested operation is not supported by this exact backend.
    #[error("secret operation is unsupported")]
    UnsupportedOperation,
    /// The exact generation already exists or resolves ambiguously.
    #[error("secret generation conflicts with retained state")]
    Conflict,
    /// A durable mutation completed but bounded cleanup remains.
    #[error("secret operation cleanup is required")]
    CleanupRequired,
    /// No value exists for the requested secret identity.
    #[error("secret was not found")]
    NotFound,
    /// The encrypted vault is unsupported, malformed, or outside resource bounds.
    #[error("encrypted secret vault is corrupt or unsupported")]
    CorruptVault,
    /// Authentication failed under the submitted unlock secret or bound metadata.
    #[error("encrypted secret authentication failed")]
    AuthenticationFailed,
    /// A prepared candidate was supplied before it became durable authority.
    #[error("candidate unlock is not authoritative; recover with the prior unlock")]
    CandidateUnlockNotAuthoritative,
    /// A prior unlock was supplied after candidate authority committed.
    #[error("unlock was superseded by a committed rotation")]
    SupersededUnlock,
    /// Durable state may be prepared or committed and must be inspected before further access.
    #[error("encrypted secret rotation requires durable-phase recovery")]
    RotationRecoveryRequired,
    /// Candidate authority is durable but prior-key recovery material may remain.
    #[error("encrypted secret rotation requires finalization")]
    RotationFinalizationPending,
    /// A one-copy authority-state update must be repaired before secret access resumes.
    #[error("encrypted secret authority-state recovery is required")]
    AuthorityRecoveryRequired,
    /// The latest secret state is durable once but peer-copy finalization is pending.
    #[error("encrypted secret authority-state finalization is pending")]
    AuthorityFinalizationPending,
    /// The encrypted vault reached its hard entry or serialized-size bound.
    #[error("encrypted secret vault reached its capacity")]
    CapacityExceeded,
    /// A checked bounded allocation failed.
    #[error("encrypted secret bounded allocation failed")]
    Allocation,
    /// Random salt or nonce generation failed.
    #[error("secure random generation failed")]
    RandomUnavailable,
    /// Secret writer serialization is unavailable.
    #[error("secret writer serialization is unavailable")]
    WriterUnavailable,
    /// Capability-confined publication failed without exposing a path or secret identity.
    #[error("encrypted secret publication failed")]
    PublicationFailed,
    /// The secret root or reserved file type is unsafe.
    #[error("encrypted secret root or file type is unsafe")]
    UnsafeStorage,
    /// Another process owns the encrypted secret vault.
    #[error("encrypted secret vault is already locked")]
    AlreadyLocked,
}

fn valid_component(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

fn map_state_error(error: LocalAuthorityStateStoreError) -> LocalSecretStoreError {
    match error {
        LocalAuthorityStateStoreError::UnsafeRoot
        | LocalAuthorityStateStoreError::UnsafeFileType
        | LocalAuthorityStateStoreError::SecureRootUnsupported => {
            LocalSecretStoreError::UnsafeStorage
        }
        LocalAuthorityStateStoreError::AlreadyLocked => LocalSecretStoreError::AlreadyLocked,
        LocalAuthorityStateStoreError::Allocation => LocalSecretStoreError::Allocation,
        LocalAuthorityStateStoreError::WriterUnavailable => {
            LocalSecretStoreError::WriterUnavailable
        }
        LocalAuthorityStateStoreError::CorruptEnvelope
        | LocalAuthorityStateStoreError::GenerationConflict
        | LocalAuthorityStateStoreError::GenerationExhausted
        | LocalAuthorityStateStoreError::StaleCommitContext
        | LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }
        | LocalAuthorityStateStoreError::PayloadTooLarge { .. } => {
            LocalSecretStoreError::CorruptVault
        }
        LocalAuthorityStateStoreError::AtomicReplaceUnsupported
        | LocalAuthorityStateStoreError::VerificationFailed
        | LocalAuthorityStateStoreError::Io { .. } => LocalSecretStoreError::PublicationFailed,
        LocalAuthorityStateStoreError::RecoveryRequired => {
            LocalSecretStoreError::AuthorityRecoveryRequired
        }
        LocalAuthorityStateStoreError::FinalizationPending => {
            LocalSecretStoreError::AuthorityFinalizationPending
        }
    }
}
