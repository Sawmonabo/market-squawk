//! Operating-system credential-store adapter with opaque, zeroizing error mapping.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use zeroize::Zeroize as _;

use super::{
    LocalSecretStoreError, SecretBackend, SecretGeneration, SecretInteractionCapability, SecretKey,
    SecretOperationControl, SecretRef, SecretStore, SecretStoreCapabilities, valid_component,
};
use crate::SecretValue;

const MAX_SERVICE_BYTES: usize = 128;

/// Production operating-system credential-store provider.
pub struct OsKeyringSecretStore {
    service: String,
    lifecycle: Mutex<()>,
}

impl OsKeyringSecretStore {
    /// Constructs a bounded OS keyring service namespace.
    pub fn try_new(service: &str) -> Result<Self, LocalSecretStoreError> {
        if !valid_component(service, MAX_SERVICE_BYTES) {
            return Err(LocalSecretStoreError::InvalidKey);
        }
        Ok(Self {
            service: service.to_owned(),
            lifecycle: Mutex::new(()),
        })
    }

    fn lock_lifecycle(&self) -> Result<MutexGuard<'_, ()>, LocalSecretStoreError> {
        self.lifecycle
            .lock()
            .map_err(|_| LocalSecretStoreError::WriterUnavailable)
    }

    fn entry(&self, key: &SecretKey) -> Result<::keyring::Entry, LocalSecretStoreError> {
        ::keyring::Entry::new(&self.service, &key.token()?).map_err(map_keyring_error)
    }

    fn referenced_entry(
        &self,
        reference: &SecretRef,
    ) -> Result<::keyring::Entry, LocalSecretStoreError> {
        if reference.backend() != os_backend() {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        ::keyring::Entry::new(&self.service, reference.locator()).map_err(map_keyring_error)
    }
}

impl fmt::Debug for OsKeyringSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OsKeyringSecretStore([REDACTED])")
    }
}

impl SecretStore for OsKeyringSecretStore {
    fn probe(
        &self,
        control: &SecretOperationControl,
    ) -> Result<SecretStoreCapabilities, LocalSecretStoreError> {
        let capabilities = os_capabilities();
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        let probe = SecretKey::try_new("market-squawk", "capability-probe")?;
        match self.entry(&probe)?.get_secret() {
            Ok(mut existing) => existing.zeroize(),
            Err(::keyring::Error::NoEntry) => {}
            Err(error) => return Err(map_keyring_read_error(error)),
        }
        control.read_postflight()?;
        Ok(capabilities)
    }

    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        let capabilities = os_capabilities();
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        let reference = SecretRef::from_key(key, capabilities.backend(), generation)?;
        let entry = self.referenced_entry(&reference)?;
        match entry.get_secret() {
            Ok(mut existing) => {
                existing.zeroize();
                return Err(LocalSecretStoreError::Conflict);
            }
            Err(::keyring::Error::NoEntry) => {}
            Err(error) => return Err(map_keyring_read_error(error)),
        }
        entry
            .set_secret(value.expose_secret().as_bytes())
            .map_err(map_keyring_mutation_error)?;
        verify_written(&entry, &value)?;
        control.mutation_postflight()?;
        Ok(reference)
    }

    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        let capabilities = os_capabilities();
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        let bytes = self
            .referenced_entry(reference)?
            .get_secret()
            .map_err(map_keyring_read_error)?;
        let value = decode_secret(bytes)?;
        control.read_postflight()?;
        Ok(value)
    }

    fn replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        let capabilities = os_capabilities();
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        if current.backend() != capabilities.backend()
            || candidate_generation <= current.generation()
            || SecretRef::from_key(key, capabilities.backend(), current.generation())? != *current
        {
            return Err(LocalSecretStoreError::Conflict);
        }
        let mut current_bytes = self
            .referenced_entry(current)?
            .get_secret()
            .map_err(map_keyring_read_error)?;
        current_bytes.zeroize();
        let candidate = SecretRef::from_key(key, capabilities.backend(), candidate_generation)?;
        let entry = self.referenced_entry(&candidate)?;
        match entry.get_secret() {
            Ok(mut existing) => {
                existing.zeroize();
                return Err(LocalSecretStoreError::Conflict);
            }
            Err(::keyring::Error::NoEntry) => {}
            Err(error) => return Err(map_keyring_read_error(error)),
        }
        entry
            .set_secret(value.expose_secret().as_bytes())
            .map_err(map_keyring_mutation_error)?;
        verify_written(&entry, &value)?;
        control.mutation_postflight()?;
        Ok(candidate)
    }

    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        let capabilities = os_capabilities();
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        self.referenced_entry(reference)?
            .delete_credential()
            .map_err(map_keyring_mutation_error)?;
        control.mutation_postflight()
    }

    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        let _lifecycle = self.lock_lifecycle()?;
        self.entry(key)?
            .set_secret(value.expose_secret().as_bytes())
            .map_err(map_keyring_error)
    }

    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        let _lifecycle = self.lock_lifecycle()?;
        let bytes = self.entry(key)?.get_secret().map_err(map_keyring_error)?;
        decode_secret(bytes)
    }
}

fn map_keyring_error(error: ::keyring::Error) -> LocalSecretStoreError {
    map_keyring_read_error(error)
}

fn map_keyring_read_error(error: ::keyring::Error) -> LocalSecretStoreError {
    match error {
        ::keyring::Error::NoEntry => LocalSecretStoreError::NotFound,
        ::keyring::Error::NoStorageAccess(_) => LocalSecretStoreError::Locked,
        ::keyring::Error::NoDefaultStore => LocalSecretStoreError::SessionUnavailable,
        ::keyring::Error::NotSupportedByStore(_) => LocalSecretStoreError::UnsupportedOperation,
        ::keyring::Error::Ambiguous(_) => LocalSecretStoreError::Conflict,
        ::keyring::Error::BadEncoding(mut bytes) => {
            bytes.zeroize();
            LocalSecretStoreError::CorruptVault
        }
        ::keyring::Error::BadDataFormat(mut bytes, _error) => {
            bytes.zeroize();
            LocalSecretStoreError::CorruptVault
        }
        ::keyring::Error::BadStoreFormat(_) => LocalSecretStoreError::CorruptVault,
        ::keyring::Error::TooLong(_, _) | ::keyring::Error::Invalid(_, _) => {
            LocalSecretStoreError::InvalidReference
        }
        _ => LocalSecretStoreError::ProviderUnavailable,
    }
}

fn map_keyring_mutation_error(error: ::keyring::Error) -> LocalSecretStoreError {
    match error {
        ::keyring::Error::NoEntry => LocalSecretStoreError::NotFound,
        ::keyring::Error::NoStorageAccess(_) => LocalSecretStoreError::Locked,
        ::keyring::Error::NoDefaultStore => LocalSecretStoreError::SessionUnavailable,
        ::keyring::Error::NotSupportedByStore(_) => LocalSecretStoreError::UnsupportedOperation,
        ::keyring::Error::Ambiguous(_) => LocalSecretStoreError::Conflict,
        ::keyring::Error::BadEncoding(mut bytes) => {
            bytes.zeroize();
            LocalSecretStoreError::CorruptVault
        }
        ::keyring::Error::BadDataFormat(mut bytes, _error) => {
            bytes.zeroize();
            LocalSecretStoreError::CorruptVault
        }
        ::keyring::Error::BadStoreFormat(_) => LocalSecretStoreError::CorruptVault,
        ::keyring::Error::TooLong(_, _) | ::keyring::Error::Invalid(_, _) => {
            LocalSecretStoreError::InvalidReference
        }
        _ => LocalSecretStoreError::IndeterminateCompletion,
    }
}

fn decode_secret(bytes: Vec<u8>) -> Result<SecretValue, LocalSecretStoreError> {
    let value = String::from_utf8(bytes).map_err(|error| {
        let mut bytes = error.into_bytes();
        bytes.zeroize();
        LocalSecretStoreError::CorruptVault
    })?;
    SecretValue::new(value).map_err(|_| LocalSecretStoreError::InvalidSecret)
}

fn verify_written(
    entry: &::keyring::Entry,
    expected: &SecretValue,
) -> Result<(), LocalSecretStoreError> {
    let mut retained = entry
        .get_secret()
        .map_err(|_| LocalSecretStoreError::IndeterminateCompletion)?;
    let exact = retained.as_slice() == expected.expose_secret().as_bytes();
    retained.zeroize();
    if exact {
        Ok(())
    } else {
        Err(LocalSecretStoreError::CleanupRequired)
    }
}

const fn os_capabilities() -> SecretStoreCapabilities {
    SecretStoreCapabilities::new(os_backend(), SecretInteractionCapability::PlatformManaged)
}

const fn os_backend() -> SecretBackend {
    if cfg!(target_os = "macos") {
        SecretBackend::AppleKeychain
    } else if cfg!(target_os = "windows") {
        SecretBackend::WindowsCredentialManager
    } else {
        SecretBackend::SecretService
    }
}
