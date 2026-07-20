//! Operating-system credential-store adapter with opaque, zeroizing error mapping.

use std::fmt;

use zeroize::Zeroize as _;

use super::{LocalSecretStoreError, SecretKey, SecretStore, valid_component};
use crate::SecretValue;

const MAX_SERVICE_BYTES: usize = 128;

/// Production operating-system credential-store provider.
pub struct OsKeyringSecretStore {
    service: String,
}

impl OsKeyringSecretStore {
    /// Constructs a bounded OS keyring service namespace.
    pub fn try_new(service: &str) -> Result<Self, LocalSecretStoreError> {
        if !valid_component(service, MAX_SERVICE_BYTES) {
            return Err(LocalSecretStoreError::InvalidKey);
        }
        Ok(Self {
            service: service.to_owned(),
        })
    }

    fn entry(&self, key: &SecretKey) -> Result<::keyring::Entry, LocalSecretStoreError> {
        ::keyring::Entry::new(&self.service, &key.token()?).map_err(map_keyring_error)
    }
}

impl fmt::Debug for OsKeyringSecretStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OsKeyringSecretStore([REDACTED])")
    }
}

impl SecretStore for OsKeyringSecretStore {
    fn store(&self, key: &SecretKey, value: SecretValue) -> Result<(), LocalSecretStoreError> {
        self.entry(key)?
            .set_secret(value.expose_secret().as_bytes())
            .map_err(map_keyring_error)
    }

    fn load(&self, key: &SecretKey) -> Result<SecretValue, LocalSecretStoreError> {
        let bytes = self.entry(key)?.get_secret().map_err(map_keyring_error)?;
        let value = String::from_utf8(bytes).map_err(|error| {
            let mut bytes = error.into_bytes();
            bytes.zeroize();
            LocalSecretStoreError::CorruptVault
        })?;
        SecretValue::new(value).map_err(|_| LocalSecretStoreError::InvalidSecret)
    }
}

fn map_keyring_error(error: ::keyring::Error) -> LocalSecretStoreError {
    match error {
        ::keyring::Error::NoEntry => LocalSecretStoreError::NotFound,
        ::keyring::Error::BadEncoding(mut bytes) => {
            bytes.zeroize();
            LocalSecretStoreError::ProviderUnavailable
        }
        ::keyring::Error::BadDataFormat(mut bytes, _error) => {
            bytes.zeroize();
            LocalSecretStoreError::ProviderUnavailable
        }
        _ => LocalSecretStoreError::ProviderUnavailable,
    }
}
