//! Small validated protocol identities and parser bounds.

use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::EiaError;

/// Exact EIA API v2 HTTPS root.
pub const EIA_API_ROOT: &str = "https://api.eia.gov/v2/";

/// Documented maximum JSON rows returned by one EIA API v2 request.
pub const EIA_MAX_JSON_PAGE_ROWS: usize = 5_000;

const MAX_ROUTE_BYTES: usize = 1_024;
const MAX_IDENTIFIER_BYTES: usize = 512;

/// Bounded parser limits; these are application policies, not provider limits except page rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaParseLimits {
    max_body_bytes: usize,
    max_rows: usize,
    max_metadata_items: usize,
    max_fields_per_object: usize,
    max_string_bytes: usize,
    max_json_depth: usize,
    max_json_nodes: usize,
}

impl EiaParseLimits {
    /// Constructs explicit nonzero bounds without allowing a page above EIA's documented maximum.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        max_body_bytes: usize,
        max_rows: usize,
        max_metadata_items: usize,
        max_fields_per_object: usize,
        max_string_bytes: usize,
        max_json_depth: usize,
        max_json_nodes: usize,
    ) -> Result<Self, EiaError> {
        if max_body_bytes == 0
            || max_rows == 0
            || max_rows > EIA_MAX_JSON_PAGE_ROWS
            || max_metadata_items == 0
            || max_fields_per_object == 0
            || max_string_bytes == 0
            || max_string_bytes > max_body_bytes
            || max_json_depth == 0
            || max_json_nodes == 0
        {
            return Err(EiaError::InvalidLimit);
        }
        Ok(Self {
            max_body_bytes,
            max_rows,
            max_metadata_items,
            max_fields_per_object,
            max_string_bytes,
            max_json_depth,
            max_json_nodes,
        })
    }

    /// Returns conservative production parsing bounds.
    pub const fn production_defaults() -> Self {
        Self {
            max_body_bytes: 32 * 1024 * 1024,
            max_rows: EIA_MAX_JSON_PAGE_ROWS,
            max_metadata_items: 16_384,
            max_fields_per_object: 512,
            max_string_bytes: 32 * 1024,
            max_json_depth: 24,
            max_json_nodes: 1_000_000,
        }
    }

    pub(crate) const fn max_body_bytes(self) -> usize {
        self.max_body_bytes
    }

    pub(crate) const fn max_rows(self) -> usize {
        self.max_rows
    }

    pub(crate) const fn max_metadata_items(self) -> usize {
        self.max_metadata_items
    }

    pub(crate) const fn max_fields_per_object(self) -> usize {
        self.max_fields_per_object
    }

    pub(crate) const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }

    pub(crate) const fn max_json_depth(self) -> usize {
        self.max_json_depth
    }

    pub(crate) const fn max_json_nodes(self) -> usize {
        self.max_json_nodes
    }
}

/// One SHA-256 identity used for request, response, schema, series, or row evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EiaDigest([u8; 32]);

impl EiaDigest {
    /// Constructs a digest from exact SHA-256 bytes.
    pub const fn new(value: [u8; 32]) -> Self {
        Self(value)
    }

    /// Returns exact SHA-256 bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

macro_rules! bounded_identifier {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Returns the retained provider string.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<&str> for $name {
            type Error = EiaError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                validate_identifier(value)?;
                Ok(Self(value.to_owned()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = EiaError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                validate_identifier(&value)?;
                Ok(Self(value))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

bounded_identifier!(
    /// A provider-native data, facet, frequency, clock, or descriptor field identity.
    EiaFieldId
);
bounded_identifier!(
    /// An exact provider-native facet value identity.
    EiaFacetValue
);

/// A validated hierarchical EIA API v2 route without `/v2`, `/data`, or query parameters.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EiaRoute(String);

impl EiaRoute {
    /// Returns the route path.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }
}

impl TryFrom<&str> for EiaRoute {
    type Error = EiaError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > MAX_ROUTE_BYTES
            || value.starts_with('/')
            || value.ends_with('/')
            || value.split('/').any(|segment| {
                segment.is_empty()
                    || matches!(segment, "." | "..")
                    || !segment.bytes().all(is_unreserved_path_byte)
            })
        {
            return Err(EiaError::InvalidRoute);
        }
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for EiaRoute {
    type Error = EiaError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str()).map(|_| Self(value))
    }
}

impl fmt::Display for EiaRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Exact provider-serving API version retained from a response.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EiaApiVersion(String);

impl EiaApiVersion {
    pub(crate) fn try_new(value: &str) -> Result<Self, EiaError> {
        if value.len() > 64
            || !value.starts_with("2.")
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'.' || byte == b'-')
        {
            return Err(EiaError::ApiVersionDrift);
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the exact provider version string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_identifier(value: &str) -> Result<(), EiaError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.chars().any(char::is_control)
    {
        return Err(EiaError::InvalidIdentifier);
    }
    Ok(())
}

const fn is_unreserved_path_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

pub(crate) fn digest_bytes(bytes: &[u8]) -> EiaDigest {
    EiaDigest::new(Sha256::digest(bytes).into())
}

pub(crate) fn digest_parts<'a>(
    label: &[u8],
    parts: impl IntoIterator<Item = &'a [u8]>,
) -> EiaDigest {
    let mut digest = Sha256::new();
    digest.update(label);
    for part in parts {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_be_bytes());
        digest.update(part);
    }
    EiaDigest::new(digest.finalize().into())
}
