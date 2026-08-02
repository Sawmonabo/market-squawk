//! Canonical non-secret configuration retained with provider onboarding sessions.

use std::collections::BTreeMap;

use serde::Serialize;
use thiserror::Error;

/// Maximum number of public configuration fields retained for one provider surface.
pub const MAX_PROVIDER_PUBLIC_CONFIGURATION_FIELDS: usize = 16;
/// Maximum canonical JSON size retained for one provider surface.
pub const MAX_PROVIDER_PUBLIC_CONFIGURATION_BYTES: usize = 4 * 1024;

const MAX_FIELD_NAME_BYTES: usize = 64;
const MAX_FIELD_VALUE_BYTES: usize = 512;
const FORBIDDEN_FIELD_TERMS: &[&str] = &[
    "authorization",
    "apikey",
    "credential",
    "key",
    "passphrase",
    "password",
    "private",
    "secret",
    "token",
];

/// Bounded canonical public setup values, never credential material.
///
/// The catalog binds this value to the session's exact provider surface and capability revision.
/// Provider-specific callers remain responsible for admitting only their documented field schema.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ProviderPublicConfiguration(BTreeMap<String, String>);

impl ProviderPublicConfiguration {
    /// Validates a bounded map whose field names cannot represent secret material.
    pub fn try_new(fields: BTreeMap<String, String>) -> Result<Self, PublicConfigurationError> {
        if fields.len() > MAX_PROVIDER_PUBLIC_CONFIGURATION_FIELDS {
            return Err(PublicConfigurationError::ResourceLimit);
        }
        for (name, value) in &fields {
            if name.is_empty()
                || name.len() > MAX_FIELD_NAME_BYTES
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
                || FORBIDDEN_FIELD_TERMS
                    .iter()
                    .any(|term| name.split('_').any(|part| part == *term))
                || value.is_empty()
                || value.len() > MAX_FIELD_VALUE_BYTES
                || !value.is_ascii()
                || value.chars().any(char::is_control)
            {
                return Err(PublicConfigurationError::InvalidRecord);
            }
        }
        let configuration = Self(fields);
        if configuration.canonical_json()?.len() > MAX_PROVIDER_PUBLIC_CONFIGURATION_BYTES {
            return Err(PublicConfigurationError::ResourceLimit);
        }
        Ok(configuration)
    }

    /// Revalidates canonical JSON loaded from durable state.
    pub fn try_from_json(bytes: &[u8]) -> Result<Self, PublicConfigurationError> {
        if bytes.is_empty() || bytes.len() > MAX_PROVIDER_PUBLIC_CONFIGURATION_BYTES {
            return Err(PublicConfigurationError::ResourceLimit);
        }
        let fields =
            serde_json::from_slice(bytes).map_err(|_| PublicConfigurationError::Serialization)?;
        Self::try_new(fields)
    }

    /// Returns canonical JSON with stable field ordering.
    pub fn canonical_json(&self) -> Result<Vec<u8>, PublicConfigurationError> {
        serde_json::to_vec(&self.0).map_err(|_| PublicConfigurationError::Serialization)
    }

    /// Returns one admitted public value.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Iterates public fields in canonical name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Returns whether this surface requires no retained public values.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Public provider-configuration validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PublicConfigurationError {
    /// A field name or value violated the non-secret public-data contract.
    #[error("provider public configuration is invalid")]
    InvalidRecord,
    /// A field count or canonical byte ceiling was exceeded.
    #[error("provider public configuration exceeds its resource limit")]
    ResourceLimit,
    /// Canonical JSON could not be encoded or decoded.
    #[error("provider public configuration serialization failed")]
    Serialization,
}
