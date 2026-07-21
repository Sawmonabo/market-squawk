//! Network endpoint and shared provider-budget policy.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use crate::bounded::BoundedVec;

const MAX_ENDPOINTS: usize = 32;
const MAX_API_RULES: usize = 32;
const MAX_CANONICAL_AUTHORITIES: usize = MAX_ENDPOINTS + MAX_API_RULES;
const MAX_PROCESS_BUDGET_SCOPES: usize = 4_096;
const MAX_MERGED_CANONICAL_AUTHORITIES: usize =
    MAX_PROCESS_BUDGET_SCOPES * MAX_CANONICAL_AUTHORITIES;
const MAX_QUERY_RULES: usize = 32;
const MAX_ENDPOINT_URL_BYTES: usize = 2_048;
const MAX_REDIRECTS: u8 = 8;
const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT_NANOS: u64 = 10_000_000_000;
const DEFAULT_READ_TIMEOUT_NANOS: u64 = 30_000_000_000;
const DEFAULT_TOTAL_TIMEOUT_NANOS: u64 = 60_000_000_000;

/// Why a request target failed the endpoint allowlist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointDenialReason {
    /// Only HTTPS and secure WebSocket endpoints are accepted.
    InsecureScheme,
    /// The URL omitted an absolute host.
    MissingHost,
    /// User information would create ambiguous authority or credential handling.
    UserInfo,
    /// Query strings are excluded from configured endpoint identity.
    Query,
    /// Fragments are never sent to a server and are therefore ambiguous.
    Fragment,
    /// The target did not exactly match a configured normalized endpoint.
    NotAllowlisted,
    /// The URL was syntactically invalid or exceeded a bounded identity field.
    Invalid,
    /// Path did not match the exact/descendant rule.
    Path,
    /// Encoded traversal or separator ambiguity was detected.
    EncodedTraversal,
    /// Query key was not declared by the endpoint rule.
    UnknownQueryKey,
    /// Query key was repeated contrary to its rule.
    DuplicateQueryKey,
    /// Query count, encoded size, or decoded value exceeded a bound.
    QueryBound,
}

/// A fail-closed network or provider-policy error.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum NetworkPolicyError {
    /// No endpoint was configured.
    #[error("at least one endpoint must be configured")]
    EmptyEndpointSet,
    /// Too many endpoint alternatives were configured.
    #[error("endpoint count exceeds maximum {max}")]
    TooManyEndpoints {
        /// Maximum accepted endpoint count.
        max: usize,
    },
    /// A normalized endpoint was listed more than once.
    #[error("endpoint allowlist contains a duplicate")]
    DuplicateEndpoint,
    /// An endpoint or redirect target was denied.
    #[error("endpoint denied: {reason:?}")]
    EndpointDenied {
        /// Stable denial class; raw potentially sensitive URLs are not retained.
        reason: EndpointDenialReason,
    },
    /// A redirect chain exceeded the configured bound.
    #[error("redirect count {actual} exceeds maximum {max}")]
    TooManyRedirects {
        /// Observed redirect count.
        actual: usize,
        /// Configured maximum.
        max: u8,
    },
    /// A response exceeded the configured byte bound.
    #[error("response size {actual} exceeds maximum {max}")]
    ResponseTooLarge {
        /// Observed or declared response size.
        actual: u64,
        /// Configured response-size maximum.
        max: u64,
    },
    /// Request bounds were zero or outside hardened global ceilings.
    #[error("invalid HTTP request bounds")]
    InvalidRequestBounds,
    /// A provider budget field was internally inconsistent.
    #[error("invalid provider budget policy")]
    InvalidBudgetPolicy,
    /// A provider/account scope conflicts with the evidenced authorization mode or basis.
    #[error("provider budget scope conflicts with authorization identity")]
    InvalidBudgetScope,
}

/// Explicit connect, read, total, redirect, and response-size bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRequestBounds {
    connect_timeout_nanos: NonZeroU64,
    read_timeout_nanos: NonZeroU64,
    total_timeout_nanos: NonZeroU64,
    max_redirects: u8,
    max_response_bytes: NonZeroU64,
}

impl HttpRequestBounds {
    /// Constructs checked network-request bounds.
    ///
    /// # Errors
    ///
    /// Returns [`NetworkPolicyError::InvalidRequestBounds`] if the total timeout is shorter than a
    /// component timeout or a global redirect/response ceiling is exceeded.
    pub fn try_new(
        connect_timeout_nanos: NonZeroU64,
        read_timeout_nanos: NonZeroU64,
        total_timeout_nanos: NonZeroU64,
        max_redirects: u8,
        max_response_bytes: NonZeroU64,
    ) -> Result<Self, NetworkPolicyError> {
        if connect_timeout_nanos.get() > total_timeout_nanos.get()
            || read_timeout_nanos.get() > total_timeout_nanos.get()
            || max_redirects > MAX_REDIRECTS
            || max_response_bytes.get() > MAX_RESPONSE_BYTES
        {
            Err(NetworkPolicyError::InvalidRequestBounds)
        } else {
            Ok(Self {
                connect_timeout_nanos,
                read_timeout_nanos,
                total_timeout_nanos,
                max_redirects,
                max_response_bytes,
            })
        }
    }

    /// Returns the maximum redirect count.
    pub const fn max_redirects(self) -> u8 {
        self.max_redirects
    }

    /// Returns the maximum response body size.
    pub const fn max_response_bytes(self) -> u64 {
        self.max_response_bytes.get()
    }

    /// Returns the connection timeout in nanoseconds.
    pub const fn connect_timeout_nanos(self) -> u64 {
        self.connect_timeout_nanos.get()
    }

    /// Returns the read timeout in nanoseconds.
    pub const fn read_timeout_nanos(self) -> u64 {
        self.read_timeout_nanos.get()
    }

    /// Returns the total request timeout in nanoseconds.
    pub const fn total_timeout_nanos(self) -> u64 {
        self.total_timeout_nanos.get()
    }
}

impl Default for HttpRequestBounds {
    fn default() -> Self {
        Self {
            connect_timeout_nanos: NonZeroU64::new(DEFAULT_CONNECT_TIMEOUT_NANOS)
                .unwrap_or(NonZeroU64::MIN),
            read_timeout_nanos: NonZeroU64::new(DEFAULT_READ_TIMEOUT_NANOS)
                .unwrap_or(NonZeroU64::MIN),
            total_timeout_nanos: NonZeroU64::new(DEFAULT_TOTAL_TIMEOUT_NANOS)
                .unwrap_or(NonZeroU64::MIN),
            max_redirects: 3,
            max_response_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap_or(NonZeroU64::MIN),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpRequestBoundsWire {
    connect_timeout_nanos: NonZeroU64,
    read_timeout_nanos: NonZeroU64,
    total_timeout_nanos: NonZeroU64,
    max_redirects: u8,
    max_response_bytes: NonZeroU64,
}

impl<'de> Deserialize<'de> for HttpRequestBounds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = HttpRequestBoundsWire::deserialize(deserializer)?;
        Self::try_new(
            wire.connect_timeout_nanos,
            wire.read_timeout_nanos,
            wire.total_timeout_nanos,
            wire.max_redirects,
            wire.max_response_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NormalizedEndpoint {
    scheme: EndpointScheme,
    host: SourceIdentifier,
    port: u16,
    path: SourceIdentifier,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct EndpointText(String);

impl EndpointText {
    fn try_new(value: String) -> Result<Self, NetworkPolicyError> {
        if value.is_empty() || value.len() > MAX_ENDPOINT_URL_BYTES {
            Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Invalid,
            })
        } else {
            Ok(Self(value))
        }
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for EndpointText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EndpointScheme {
    Https,
    Wss,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CanonicalNetworkAuthority {
    host: SourceIdentifier,
    port: u16,
}

impl NormalizedEndpoint {
    fn parse(value: &str) -> Result<Self, NetworkPolicyError> {
        Self::parse_with_query(value, false)
    }

    fn parse_target(value: &str) -> Result<Self, NetworkPolicyError> {
        Self::parse_with_query(value, true)
    }

    fn parse_with_query(value: &str, allow_query: bool) -> Result<Self, NetworkPolicyError> {
        if value.len() > MAX_ENDPOINT_URL_BYTES || endpoint_text_is_ambiguous(value) {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Invalid,
            });
        }
        let url = Url::parse(value).map_err(|_| NetworkPolicyError::EndpointDenied {
            reason: EndpointDenialReason::Invalid,
        })?;
        let scheme = match url.scheme() {
            "https" => EndpointScheme::Https,
            "wss" => EndpointScheme::Wss,
            _ => {
                return Err(NetworkPolicyError::EndpointDenied {
                    reason: EndpointDenialReason::InsecureScheme,
                });
            }
        };
        if !url.username().is_empty() || url.password().is_some() {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::UserInfo,
            });
        }
        if !allow_query && url.query().is_some() {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Query,
            });
        }
        if url.fragment().is_some() {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Fragment,
            });
        }
        let host = url
            .host_str()
            .ok_or(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::MissingHost,
            })?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .ok_or(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Invalid,
            })?;
        let host =
            SourceIdentifier::try_from(host).map_err(|_| NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Invalid,
            })?;
        let path = SourceIdentifier::try_from(url.path()).map_err(|_| {
            NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Invalid,
            }
        })?;
        Ok(Self {
            scheme,
            host,
            port,
            path,
        })
    }

    fn canonical_string(&self) -> String {
        let scheme = match self.scheme {
            EndpointScheme::Https => "https",
            EndpointScheme::Wss => "wss",
        };
        let host = self.host.as_str();
        let host = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(host);
        if host.contains(':') {
            format!("{scheme}://[{host}]:{}{}", self.port, self.path)
        } else {
            format!("{scheme}://{host}:{}{}", self.port, self.path)
        }
    }

    fn same_origin(&self, other: &Self) -> bool {
        self.scheme == other.scheme && self.host == other.host && self.port == other.port
    }

    fn canonical_network_authority(&self) -> CanonicalNetworkAuthority {
        CanonicalNetworkAuthority {
            host: self.host.clone(),
            port: self.port,
        }
    }
}

/// Whether an API endpoint accepts only its base path or segment-boundary descendants.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PathScope {
    /// Final path must equal the configured base path.
    Exact,
    /// Final path may equal the base or descend below it at a `/` segment boundary.
    Descendants,
}

/// Whether a query parameter value must be treated as secret in diagnostics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuerySensitivity {
    /// Non-secret parameter; values still are not retained by policy errors.
    Public,
    /// Secret parameter such as an API key; value is never retained or formatted.
    Secret,
}

/// Bounded allowlist rule for one decoded query key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryParameterRule {
    key: SourceIdentifier,
    max_value_bytes: u16,
    allow_multiple: bool,
    sensitivity: QuerySensitivity,
}

impl QueryParameterRule {
    /// Constructs a query rule.
    ///
    /// # Errors
    ///
    /// Rejects delimiters in keys and zero or excessive value bounds.
    pub fn try_new(
        key: SourceIdentifier,
        max_value_bytes: u16,
        allow_multiple: bool,
        sensitivity: QuerySensitivity,
    ) -> Result<Self, NetworkPolicyError> {
        if max_value_bytes == 0 || max_value_bytes > 8_192 || key.as_str().contains(['&', '=']) {
            return Err(NetworkPolicyError::InvalidRequestBounds);
        }
        Ok(Self {
            key,
            max_value_bytes,
            allow_multiple,
            sensitivity,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryParameterRuleWire {
    key: SourceIdentifier,
    max_value_bytes: u16,
    allow_multiple: bool,
    sensitivity: QuerySensitivity,
}

impl<'de> Deserialize<'de> for QueryParameterRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = QueryParameterRuleWire::deserialize(deserializer)?;
        Self::try_new(
            wire.key,
            wire.max_value_bytes,
            wire.allow_multiple,
            wire.sensitivity,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Structural rule for bounded dynamic REST paths and query parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiEndpointRule {
    base: NormalizedEndpoint,
    path_scope: PathScope,
    query_rules: BoundedVec<QueryParameterRule, MAX_QUERY_RULES>,
    max_query_parameters: u8,
    max_encoded_query_bytes: u16,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct ApiEndpointRuleSerializeWire<'a> {
    base_url: EndpointText,
    path_scope: PathScope,
    query_rules: &'a BoundedVec<QueryParameterRule, MAX_QUERY_RULES>,
    max_query_parameters: u8,
    max_encoded_query_bytes: u16,
}

impl Serialize for ApiEndpointRule {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base_url = EndpointText::try_new(self.base.canonical_string())
            .map_err(serde::ser::Error::custom)?;
        ApiEndpointRuleSerializeWire {
            base_url,
            path_scope: self.path_scope,
            query_rules: &self.query_rules,
            max_query_parameters: self.max_query_parameters,
            max_encoded_query_bytes: self.max_encoded_query_bytes,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiEndpointRuleWire {
    base_url: EndpointText,
    path_scope: PathScope,
    query_rules: BoundedVec<QueryParameterRule, MAX_QUERY_RULES>,
    max_query_parameters: u8,
    max_encoded_query_bytes: u16,
}

impl<'de> Deserialize<'de> for ApiEndpointRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ApiEndpointRuleWire::deserialize(deserializer)?;
        Self::try_new(
            wire.base_url.as_str(),
            wire.path_scope,
            wire.query_rules.as_slice().to_vec(),
            wire.max_query_parameters,
            wire.max_encoded_query_bytes,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ApiEndpointRule {
    /// Constructs a dynamic API rule from a query-free HTTPS base URL.
    ///
    /// # Errors
    ///
    /// Rejects insecure/ambiguous bases, duplicate query keys, and excessive bounds.
    pub fn try_new(
        base_url: &str,
        path_scope: PathScope,
        query_rules: Vec<QueryParameterRule>,
        max_query_parameters: u8,
        max_encoded_query_bytes: u16,
    ) -> Result<Self, NetworkPolicyError> {
        let base = NormalizedEndpoint::parse(base_url)?;
        if base.scheme != EndpointScheme::Https
            || max_query_parameters == 0
            || usize::from(max_query_parameters) > 64
            || max_encoded_query_bytes == 0
            || max_encoded_query_bytes > 16_384
        {
            return Err(NetworkPolicyError::InvalidRequestBounds);
        }
        if query_rules.iter().enumerate().any(|(index, rule)| {
            query_rules[index.saturating_add(1)..]
                .iter()
                .any(|other| rule.key == other.key)
        }) {
            return Err(NetworkPolicyError::DuplicateEndpoint);
        }
        let query_rules = BoundedVec::try_new(query_rules)
            .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?;
        Ok(Self {
            base,
            path_scope,
            query_rules,
            max_query_parameters,
            max_encoded_query_bytes,
        })
    }

    fn authorize(
        &self,
        raw_target: &str,
        target: &NormalizedEndpoint,
    ) -> Result<bool, NetworkPolicyError> {
        if !self.base.same_origin(target) {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::NotAllowlisted,
            });
        }
        let lower = raw_target.to_ascii_lowercase();
        let raw_path = lower.split(['?', '#']).next().unwrap_or(&lower);
        if raw_path.contains("%2e") || raw_path.contains("%2f") || raw_path.contains("%5c") {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::EncodedTraversal,
            });
        }
        let base = self.base.path.as_str();
        let path = target.path.as_str();
        let allowed_path = match self.path_scope {
            PathScope::Exact => path == base,
            PathScope::Descendants => {
                base == "/"
                    || path == base
                    || path
                        .strip_prefix(base)
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }
        };
        if !allowed_path {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::Path,
            });
        }
        let url = Url::parse(raw_target).map_err(|_| NetworkPolicyError::EndpointDenied {
            reason: EndpointDenialReason::Invalid,
        })?;
        let encoded = url.query().unwrap_or("");
        if encoded.len() > usize::from(self.max_encoded_query_bytes) {
            return Err(NetworkPolicyError::EndpointDenied {
                reason: EndpointDenialReason::QueryBound,
            });
        }
        let mut seen: Vec<String> = Vec::new();
        let mut contains_secret = false;
        for (key, value) in url.query_pairs() {
            if seen.len() == usize::from(self.max_query_parameters) {
                return Err(NetworkPolicyError::EndpointDenied {
                    reason: EndpointDenialReason::QueryBound,
                });
            }
            let Some(rule) = self
                .query_rules
                .as_slice()
                .iter()
                .find(|rule| rule.key.as_str() == key)
            else {
                return Err(NetworkPolicyError::EndpointDenied {
                    reason: EndpointDenialReason::UnknownQueryKey,
                });
            };
            if !rule.allow_multiple && seen.iter().any(|seen_key| seen_key == key.as_ref()) {
                return Err(NetworkPolicyError::EndpointDenied {
                    reason: EndpointDenialReason::DuplicateQueryKey,
                });
            }
            if value.len() > usize::from(rule.max_value_bytes) {
                return Err(NetworkPolicyError::EndpointDenied {
                    reason: EndpointDenialReason::QueryBound,
                });
            }
            contains_secret |= rule.sensitivity == QuerySensitivity::Secret;
            seen.push(key.into_owned());
        }
        Ok(contains_secret)
    }
}

/// Mandatory HTTP-client hardening settings for every remote source adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpClientProfile {
    automatic_redirects: bool,
    ambient_system_proxy: bool,
    referer: bool,
    implicit_retries: bool,
    count_post_decompression_bytes: bool,
}

impl HttpClientProfile {
    /// Returns the only accepted production profile.
    pub const fn hardened() -> Self {
        Self {
            automatic_redirects: false,
            ambient_system_proxy: false,
            referer: false,
            implicit_retries: false,
            count_post_decompression_bytes: true,
        }
    }

    /// Returns whether automatic redirects must be disabled.
    pub const fn automatic_redirects_disabled(self) -> bool {
        !self.automatic_redirects
    }

    /// Returns whether ambient system proxies must be disabled.
    pub const fn ambient_system_proxy_disabled(self) -> bool {
        !self.ambient_system_proxy
    }

    /// Returns whether implicit client retries must be disabled.
    pub const fn implicit_retries_disabled(self) -> bool {
        !self.implicit_retries
    }

    /// Returns whether streamed post-decompression bytes must be counted.
    pub const fn counts_post_decompression_bytes(self) -> bool {
        self.count_post_decompression_bytes
    }
}

impl<'de> Deserialize<'de> for HttpClientProfile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            automatic_redirects: bool,
            ambient_system_proxy: bool,
            referer: bool,
            implicit_retries: bool,
            count_post_decompression_bytes: bool,
        }
        let wire = Wire::deserialize(deserializer)?;
        let candidate = Self {
            automatic_redirects: wire.automatic_redirects,
            ambient_system_proxy: wire.ambient_system_proxy,
            referer: wire.referer,
            implicit_retries: wire.implicit_retries,
            count_post_decompression_bytes: wire.count_post_decompression_bytes,
        };
        if candidate == Self::hardened() {
            Ok(candidate)
        } else {
            Err(serde::de::Error::custom(
                NetworkPolicyError::InvalidRequestBounds,
            ))
        }
    }
}

include!("policy/endpoint.rs");
include!("policy/budget.rs");
include!("policy/tests.rs");
