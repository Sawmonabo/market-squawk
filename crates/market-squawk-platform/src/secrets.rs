//! OS-backed credentials and a capability-confined encrypted local fallback.

mod crypto;
mod encrypted;
mod keyring;

use std::fmt;

use sha2::{Digest as _, Sha256};
use thiserror::Error;

use self::crypto::encode_hex;
use crate::{LocalAuthorityStateStoreError, SecretValue};

pub use self::encrypted::EncryptedFileSecretStore;
pub use self::keyring::OsKeyringSecretStore;

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
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretKey([REDACTED])")
    }
}

/// Replaceable storage contract for local credential material.
pub trait SecretStore: fmt::Debug + Send + Sync {
    /// Stores or rotates one secret without retaining the submitted value.
    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError>;

    /// Loads one secret into zeroizing, redacted memory.
    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError>;
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
    /// Submitted or decrypted secret material violates the in-memory bound.
    #[error("secret value is invalid")]
    InvalidSecret,
    /// OS credential storage is unavailable or rejected the operation.
    #[error("operating-system secret provider is unavailable")]
    ProviderUnavailable,
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
