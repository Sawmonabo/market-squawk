//! OS-first secret storage with explicit, reference-routed encrypted fallback.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

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
    root: PathBuf,
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
        let root = root.as_ref().to_path_buf();
        Ok(Self {
            store: EncryptedFileSecretStore::try_open(&root, unlock.0)?,
            root,
        })
    }
}

impl fmt::Debug for EncryptedFileSecretFallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EncryptedFileSecretFallback([REDACTED])")
    }
}

/// Process-local readiness of the optional encrypted-file fallback.
///
/// `Locked` means a safe encrypted fallback root is configured, but no unlock secret is retained.
/// The status is intentionally non-secret and carries no path or credential identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EncryptedFileFallbackStatus {
    /// No encrypted fallback root was configured.
    Disabled,
    /// A fallback root is configured and requires explicit foreground unlock.
    Locked,
    /// The unlock-derived vault authority is retained only in this process.
    Ready,
}

struct ConfiguredEncryptedFileFallback {
    root: PathBuf,
    store: Mutex<Option<EncryptedFileSecretStore>>,
}

impl ConfiguredEncryptedFileFallback {
    fn locked(root: PathBuf) -> Self {
        Self {
            root,
            store: Mutex::new(None),
        }
    }

    fn ready(fallback: EncryptedFileSecretFallback) -> Self {
        Self {
            root: fallback.root,
            store: Mutex::new(Some(fallback.store)),
        }
    }

    fn status(&self) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        self.store
            .lock()
            .map(|store| {
                if store.is_some() {
                    EncryptedFileFallbackStatus::Ready
                } else {
                    EncryptedFileFallbackStatus::Locked
                }
            })
            .map_err(|_error| LocalSecretStoreError::WriterUnavailable)
    }

    fn with_ready<T>(
        &self,
        operation: impl FnOnce(&EncryptedFileSecretStore) -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        let store = self
            .store
            .lock()
            .map_err(|_error| LocalSecretStoreError::WriterUnavailable)?;
        operation(store.as_ref().ok_or(LocalSecretStoreError::Locked)?)
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
    fallback: Option<ConfiguredEncryptedFileFallback>,
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
            fallback: fallback.map(ConfiguredEncryptedFileFallback::ready),
        })
    }

    /// Constructs the OS-keyring authority with a configured but locked encrypted fallback.
    ///
    /// The fallback root is not opened and no unlock is read from environment, arguments,
    /// configuration, or disk. Callers must later submit an
    /// [`EncryptedFileUnlockCapability`] through an explicitly owned foreground operation.
    ///
    /// # Errors
    ///
    /// Rejects an invalid keyring service namespace. Root safety and vault authentication are
    /// checked only when explicit unlock is attempted.
    pub fn try_new_with_locked_encrypted_file_fallback(
        service: &str,
        root: impl AsRef<Path>,
    ) -> Result<Self, LocalSecretStoreError> {
        Ok(Self {
            primary: OsKeyringSecretStore::try_new(service)?,
            fallback: Some(ConfiguredEncryptedFileFallback::locked(
                root.as_ref().to_path_buf(),
            )),
        })
    }

    fn fallback(&self) -> Result<&ConfiguredEncryptedFileFallback, LocalSecretStoreError> {
        self.fallback
            .as_ref()
            .ok_or(LocalSecretStoreError::UnsupportedOperation)
    }

    fn use_fallback<T>(
        &self,
        operation: impl FnOnce(&EncryptedFileSecretStore) -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        self.fallback()?.with_ready(operation)
    }

    fn encrypted_reference<T>(
        &self,
        reference: &SecretRef,
        operation: impl FnOnce(&EncryptedFileSecretStore) -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        match reference.backend() {
            SecretBackend::AppleKeychain
            | SecretBackend::WindowsCredentialManager
            | SecretBackend::SecretService => Err(LocalSecretStoreError::InvalidReference),
            SecretBackend::EncryptedFile => match self.fallback.as_ref() {
                Some(fallback) => fallback.with_ready(operation),
                None => Err(LocalSecretStoreError::InvalidReference),
            },
        }
    }
}

impl fmt::Debug for PreferredSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreferredSecretStore")
            .field("primary", &"[OS KEYRING]")
            .field("fallback_configured", &self.fallback.is_some())
            .field(
                "fallback_ready",
                &self
                    .fallback
                    .as_ref()
                    .and_then(|fallback| fallback.status().ok())
                    .is_some_and(|status| status == EncryptedFileFallbackStatus::Ready),
            )
            .finish()
    }
}

impl SecretStore for PreferredSecretStore {
    fn encrypted_file_fallback_status(
        &self,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        self.fallback
            .as_ref()
            .map_or(Ok(EncryptedFileFallbackStatus::Disabled), |fallback| {
                fallback.status()
            })
    }

    fn unlock_encrypted_file_fallback(
        &self,
        unlock: EncryptedFileUnlockCapability,
        control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        let fallback = self.fallback()?;
        control.preflight(SecretStoreCapabilities::new(
            SecretBackend::EncryptedFile,
            super::SecretInteractionCapability::Never,
        ))?;
        let mut store = fallback
            .store
            .lock()
            .map_err(|_error| LocalSecretStoreError::WriterUnavailable)?;
        if store.is_some() {
            return Ok(EncryptedFileFallbackStatus::Ready);
        }
        let opened = EncryptedFileSecretStore::try_open(&fallback.root, unlock.0)?;
        opened.validate_current_unlock(control)?;
        *store = Some(opened);
        control.mutation_postflight()?;
        Ok(EncryptedFileFallbackStatus::Ready)
    }

    fn lock_encrypted_file_fallback(
        &self,
        control: &SecretOperationControl,
    ) -> Result<EncryptedFileFallbackStatus, LocalSecretStoreError> {
        let fallback = self.fallback()?;
        control.preflight(SecretStoreCapabilities::new(
            SecretBackend::EncryptedFile,
            super::SecretInteractionCapability::Never,
        ))?;
        let prior = fallback
            .store
            .lock()
            .map_err(|_error| LocalSecretStoreError::WriterUnavailable)?
            .take();
        drop(prior);
        control.mutation_postflight()?;
        Ok(EncryptedFileFallbackStatus::Locked)
    }

    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError> {
        match self.primary.probe(control) {
            Ok(capabilities) if capabilities.supports_exact_lifecycle() => Ok(capabilities),
            Ok(_) => self.use_fallback(|fallback| fallback.probe(control)),
            Err(error) if keyring_is_unavailable(&error) => match self.fallback.as_ref() {
                Some(fallback) => fallback.with_ready(|store| store.probe(control)),
                None => Err(error),
            },
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
        match self.primary.probe(control) {
            Ok(capabilities) if capabilities.supports_exact_lifecycle() => {
                self.primary.create(key, generation, value, control)
            }
            Ok(_) => self.use_fallback(|fallback| fallback.create(key, generation, value, control)),
            Err(error) if keyring_is_unavailable(&error) => match self.fallback.as_ref() {
                Some(fallback) => {
                    fallback.with_ready(|store| store.create(key, generation, value, control))
                }
                None => Err(error),
            },
            Err(error) => Err(error),
        }
    }

    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        match reference.backend() {
            SecretBackend::AppleKeychain
            | SecretBackend::WindowsCredentialManager
            | SecretBackend::SecretService => self.primary.read(reference, control),
            SecretBackend::EncryptedFile => {
                self.encrypted_reference(reference, |store| store.read(reference, control))
            }
        }
    }

    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        match current.backend() {
            SecretBackend::AppleKeychain
            | SecretBackend::WindowsCredentialManager
            | SecretBackend::SecretService => {
                self.primary
                    .replace(key, current, candidate_generation, value, control)
            }
            SecretBackend::EncryptedFile => self.encrypted_reference(current, |store| {
                store.replace(key, current, candidate_generation, value, control)
            }),
        }
    }

    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        match reference.backend() {
            SecretBackend::AppleKeychain
            | SecretBackend::WindowsCredentialManager
            | SecretBackend::SecretService => self.primary.delete(reference, control),
            SecretBackend::EncryptedFile => {
                self.encrypted_reference(reference, |store| store.delete(reference, control))
            }
        }
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
