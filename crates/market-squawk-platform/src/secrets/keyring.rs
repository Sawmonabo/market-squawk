//! Operating-system credential-store adapter with opaque, zeroizing error mapping.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use zeroize::Zeroize as _;

#[cfg(target_os = "macos")]
use super::SecretInteractionPolicy;
use super::{
    LocalSecretStoreError, SecretBackend, SecretDeletionDisposition, SecretGeneration,
    SecretInteractionCapability, SecretKey, SecretMutationDisposition, SecretMutationFailure,
    SecretMutationPlan, SecretOperationControl, SecretReconciliationObservation, SecretRef,
    SecretStore, SecretStoreCapabilities, delete_exact_plan, execute_exact_plan,
    inspect_exact_plan, match_exact_plan, valid_component,
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

    #[cfg(target_os = "macos")]
    fn delete_referenced_entry(&self, reference: &SecretRef) -> Result<(), LocalSecretStoreError> {
        use security_framework::item::{ItemClass, ItemSearchOptions};
        use security_framework::os::macos::keychain::{SecKeychain, SecPreferencesDomain};

        if reference.backend() != SecretBackend::AppleKeychain {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        let keychain = SecKeychain::default_for_domain(SecPreferencesDomain::User)
            .map_err(map_macos_keychain_read_error)?;
        let mut search = ItemSearchOptions::new();
        search
            .keychains(&[keychain])
            .class(ItemClass::generic_password())
            .service(&self.service)
            .account(reference.locator());
        search.delete().map_err(map_macos_keychain_mutation_error)
    }

    #[cfg(not(target_os = "macos"))]
    fn delete_referenced_entry(&self, reference: &SecretRef) -> Result<(), LocalSecretStoreError> {
        self.referenced_entry(reference)?
            .delete_credential()
            .map_err(map_keyring_mutation_error)
    }

    fn controlled_operation<T>(
        &self,
        control: &SecretOperationControl,
        operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        let capabilities = operation_capabilities(control);
        control.preflight(capabilities)?;
        let _lifecycle = self.lock_lifecycle()?;
        control.preflight(capabilities)?;
        with_platform_interaction_policy(control, operation)
    }

    fn legacy_operation<T>(
        &self,
        operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
    ) -> Result<T, LocalSecretStoreError> {
        let _lifecycle = self.lock_lifecycle()?;
        with_platform_default_interaction(operation)
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
        self.controlled_operation(control, || {
            let probe = SecretKey::try_new("market-squawk", "capability-probe")?;
            match self.entry(&probe)?.get_secret() {
                Ok(mut existing) => existing.zeroize(),
                Err(::keyring::Error::NoEntry) => {}
                Err(error) => return Err(map_keyring_read_error(error)),
            }
            control.read_postflight()
        })?;
        Ok(capabilities)
    }

    fn plan_create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError> {
        let capabilities = self.probe(control)?;
        SecretMutationPlan::create(key, capabilities.backend(), generation)
    }

    fn plan_replace(
        &self,
        key: &SecretKey,
        current: &SecretRef,
        candidate_generation: SecretGeneration,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationPlan, LocalSecretStoreError> {
        let capabilities = self.probe(control)?;
        if current.backend() != capabilities.backend() {
            return Err(LocalSecretStoreError::InvalidReference);
        }
        let _current = self.read(current, control)?;
        SecretMutationPlan::replace(key, current.clone(), candidate_generation)
    }

    fn execute_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretMutationDisposition, SecretMutationFailure> {
        execute_exact_plan(self, os_backend(), key, plan, value, control)
    }

    fn inspect_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
        inspect_exact_plan(self, os_backend(), key, plan, control)
    }

    fn matches_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        expected: &SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretReconciliationObservation, LocalSecretStoreError> {
        match_exact_plan(self, os_backend(), key, plan, expected, control)
    }

    fn delete_planned(
        &self,
        key: &SecretKey,
        plan: &SecretMutationPlan,
        control: &SecretOperationControl,
    ) -> Result<SecretDeletionDisposition, SecretMutationFailure> {
        delete_exact_plan(self, os_backend(), key, plan, control)
    }

    fn create(
        &self,
        key: &SecretKey,
        generation: SecretGeneration,
        value: SecretValue,
        control: &SecretOperationControl,
    ) -> Result<SecretRef, LocalSecretStoreError> {
        let capabilities = os_capabilities();
        self.controlled_operation(control, || {
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
        })
    }

    fn read(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        self.controlled_operation(control, || {
            let bytes = self
                .referenced_entry(reference)?
                .get_secret()
                .map_err(map_keyring_read_error)?;
            let value = decode_secret(bytes)?;
            control.read_postflight()?;
            Ok(value)
        })
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
        self.controlled_operation(control, || {
            if current.backend() != capabilities.backend()
                || candidate_generation <= current.generation()
                || SecretRef::from_key(key, capabilities.backend(), current.generation())?
                    != *current
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
        })
    }

    fn delete(
        &self,
        reference: &SecretRef,
        control: &SecretOperationControl,
    ) -> Result<(), LocalSecretStoreError> {
        self.controlled_operation(control, || {
            self.delete_referenced_entry(reference)?;
            control.mutation_postflight()
        })
    }

    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        self.legacy_operation(|| {
            self.entry(key)?
                .set_secret(value.expose_secret().as_bytes())
                .map_err(map_keyring_error)
        })
    }

    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        self.legacy_operation(|| {
            let bytes = self.entry(key)?.get_secret().map_err(map_keyring_error)?;
            decode_secret(bytes)
        })
    }
}

fn map_keyring_error(error: ::keyring::Error) -> LocalSecretStoreError {
    map_keyring_read_error(error)
}

fn map_keyring_read_error(error: ::keyring::Error) -> LocalSecretStoreError {
    #[cfg(target_os = "macos")]
    if macos_interaction_required(&error) {
        return LocalSecretStoreError::InteractionRequired;
    }
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
    #[cfg(target_os = "macos")]
    if macos_interaction_required(&error) {
        return LocalSecretStoreError::InteractionRequired;
    }
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
    let interaction = if cfg!(target_os = "windows") {
        SecretInteractionCapability::Never
    } else {
        SecretInteractionCapability::PlatformManaged
    };
    SecretStoreCapabilities::new(os_backend(), interaction)
}

const fn operation_capabilities(control: &SecretOperationControl) -> SecretStoreCapabilities {
    #[cfg(target_os = "macos")]
    if matches!(
        control.interaction_policy(),
        SecretInteractionPolicy::Forbid
    ) {
        return SecretStoreCapabilities::new(
            SecretBackend::AppleKeychain,
            SecretInteractionCapability::Never,
        );
    }

    let _ = control;
    os_capabilities()
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

#[cfg(target_os = "macos")]
static KEYCHAIN_INTERACTION: Mutex<()> = Mutex::new(());

#[cfg(target_os = "macos")]
fn with_platform_interaction_policy<T>(
    control: &SecretOperationControl,
    operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
) -> Result<T, LocalSecretStoreError> {
    use security_framework::os::macos::keychain::SecKeychain;

    let _serialization = KEYCHAIN_INTERACTION
        .lock()
        .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
    control.preflight(operation_capabilities(control))?;
    let _interaction_guard = if matches!(
        control.interaction_policy(),
        SecretInteractionPolicy::Forbid
    ) {
        Some(
            SecKeychain::disable_user_interaction()
                .map_err(|_| LocalSecretStoreError::ProviderUnavailable)?,
        )
    } else {
        None
    };
    operation()
}

#[cfg(not(target_os = "macos"))]
fn with_platform_interaction_policy<T>(
    _control: &SecretOperationControl,
    operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
) -> Result<T, LocalSecretStoreError> {
    operation()
}

#[cfg(target_os = "macos")]
fn with_platform_default_interaction<T>(
    operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
) -> Result<T, LocalSecretStoreError> {
    let _serialization = KEYCHAIN_INTERACTION
        .lock()
        .map_err(|_| LocalSecretStoreError::WriterUnavailable)?;
    operation()
}

#[cfg(not(target_os = "macos"))]
fn with_platform_default_interaction<T>(
    operation: impl FnOnce() -> Result<T, LocalSecretStoreError>,
) -> Result<T, LocalSecretStoreError> {
    operation()
}

#[cfg(target_os = "macos")]
fn macos_interaction_required(error: &::keyring::Error) -> bool {
    const ERR_SEC_INTERACTION_NOT_ALLOWED: i32 = -25_308;

    let platform_error = match error {
        ::keyring::Error::PlatformFailure(error) | ::keyring::Error::NoStorageAccess(error) => {
            error
        }
        _ => return false,
    };
    platform_error
        .downcast_ref::<security_framework::base::Error>()
        .is_some_and(|error| error.code() == ERR_SEC_INTERACTION_NOT_ALLOWED)
}

#[cfg(target_os = "macos")]
fn map_macos_keychain_read_error(error: security_framework::base::Error) -> LocalSecretStoreError {
    match error.code() {
        -25_300 => LocalSecretStoreError::NotFound,
        -25_308 => LocalSecretStoreError::InteractionRequired,
        -25_291 | -25_292 | -25_294 | -25_295 => LocalSecretStoreError::Locked,
        _ => LocalSecretStoreError::ProviderUnavailable,
    }
}

#[cfg(target_os = "macos")]
fn map_macos_keychain_mutation_error(
    error: security_framework::base::Error,
) -> LocalSecretStoreError {
    match error.code() {
        -25_300 => LocalSecretStoreError::NotFound,
        -25_308 => LocalSecretStoreError::InteractionRequired,
        -25_291 | -25_292 | -25_294 | -25_295 => LocalSecretStoreError::Locked,
        _ => LocalSecretStoreError::IndeterminateCompletion,
    }
}
