//! OS-first secret storage with explicit, reference-routed encrypted fallback.

use std::fmt;
use std::path::Path;

use super::{
    EncryptedFileSecretStore, LocalSecretStoreError, OsKeyringSecretStore, SecretBackend,
    SecretGeneration, SecretKey, SecretOperationControl, SecretRef, SecretStore,
    SecretStoreCapabilities,
};
use crate::SecretValue;

/// User-held authority to unlock one encrypted-file fallback.
///
/// Construction consumes the secret into zeroizing memory. The capability is deliberately neither
/// cloneable nor serializable and can only be consumed while opening a fallback store.
pub struct EncryptedFileUnlockCapability(SecretValue);

impl EncryptedFileUnlockCapability {
    /// Adopts an explicit user-supplied unlock without exposing it through configuration state.
    #[must_use]
    pub fn new(unlock: SecretValue) -> Self {
        Self(unlock)
    }
}

impl fmt::Debug for EncryptedFileUnlockCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptedFileUnlockCapability([REDACTED])")
    }
}

/// Already-open encrypted fallback backed by an explicit local root and unlock capability.
pub struct EncryptedFileSecretFallback {
    store: EncryptedFileSecretStore,
}

impl EncryptedFileSecretFallback {
    /// Opens the capability-confined fallback by consuming explicit user-held unlock authority.
    ///
    /// # Errors
    ///
    /// Returns a typed secret-store error when the root, lock, vault, or unlock is invalid.
    pub fn try_open(
        root: impl AsRef<Path>,
        unlock: EncryptedFileUnlockCapability,
    ) -> Result<Self, LocalSecretStoreError> {
        Ok(Self {
            store: EncryptedFileSecretStore::try_open(root, unlock.0)?,
        })
    }
}

impl fmt::Debug for EncryptedFileSecretFallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptedFileSecretFallback([REDACTED])")
    }
}

/// Production OS-first store that routes retained references to exactly one backend.
///
/// A generation is placed in the encrypted fallback only when a pre-mutation OS-keyring probe
/// proves that the platform backend is unavailable. Once a [`SecretRef`] exists, every operation
/// is routed solely by its backend field; the router never probes another backend with that
/// reference and never copies secret bytes between backends.
pub struct PreferredSecretStore {
    primary: OsKeyringSecretStore,
    fallback: Option<EncryptedFileSecretFallback>,
}

impl PreferredSecretStore {
    /// Constructs the OS-keyring authority with an optional explicitly unlocked fallback.
    ///
    /// # Errors
    ///
    /// Rejects an invalid keyring service namespace.
    pub fn try_new(
        service: &str,
        fallback: Option<EncryptedFileSecretFallback>,
    ) -> Result<Self, LocalSecretStoreError> {
        Ok(Self {
            primary: OsKeyringSecretStore::try_new(service)?,
            fallback,
        })
    }

    fn creation_store(
        &self,
        control: &SecretOperationControl,
    ) -> Result<&dyn SecretStore, LocalSecretStoreError> {
        match self.primary.probe(control) {
            Ok(capabilities) if capabilities.supports_exact_lifecycle() => Ok(&self.primary),
            Ok(_) => self
                .fallback
                .as_ref()
                .map(|fallback| &fallback.store as &dyn SecretStore)
                .ok_or(LocalSecretStoreError::UnsupportedOperation),
            Err(error) if keyring_is_unavailable(&error) => self
                .fallback
                .as_ref()
                .map(|fallback| &fallback.store as &dyn SecretStore)
                .ok_or(error),
            Err(error) => Err(error),
        }
    }

    fn referenced_store(
        &self,
        reference: &SecretRef,
    ) -> Result<&dyn SecretStore, LocalSecretStoreError> {
        match reference.backend() {
            SecretBackend::AppleKeychain
            | SecretBackend::WindowsCredentialManager
            | SecretBackend::SecretService => Ok(&self.primary),
            SecretBackend::EncryptedFile => self
                .fallback
                .as_ref()
                .map(|fallback| &fallback.store as &dyn SecretStore)
                .ok_or(LocalSecretStoreError::InvalidReference),
        }
    }
}

impl fmt::Debug for PreferredSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreferredSecretStore")
            .field("primary", &"[OS KEYRING]")
            .field("fallback_configured", &self.fallback.is_some())
            .finish()
    }
}

impl SecretStore for PreferredSecretStore {
    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError> {
        match self.primary.probe(control) {
            Ok(capabilities) if capabilities.supports_exact_lifecycle() => Ok(capabilities),
            Ok(_) => self
                .fallback
                .as_ref()
                .map(|fallback| fallback.store.probe(control))
                .unwrap_or(Err(LocalSecretStoreError::UnsupportedOperation)),
            Err(error) if keyring_is_unavailable(&error) => self
                .fallback
                .as_ref()
                .map(|fallback| fallback.store.probe(control))
                .unwrap_or(Err(error)),
            Err(error) => Err(error),
        }
    }

    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        self.creation_store(control)?
            .create(key, generation, value, control)
    }

    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        self.referenced_store(reference)?.read(reference, control)
    }

    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        self.referenced_store(current)?
            .replace(key, current, candidate_generation, value, control)
    }

    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        self.referenced_store(reference)?.delete(reference, control)
    }

    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        self.primary.store(key, value)
    }

    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        self.primary.load(key)
    }
}

fn keyring_is_unavailable(error: &LocalSecretStoreError) -> bool {
    matches!(
        error,
        LocalSecretStoreError::ProviderUnavailable
            | LocalSecretStoreError::SessionUnavailable
            | LocalSecretStoreError::UnsupportedOperation
    )
}
