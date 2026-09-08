//! Crash-safe provider-local persistence for Yahoo admission and bounded response cache state.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AdmissionSnapshot, YahooHttpAttemptReceipt, YahooHttpRequest};

const YAHOO_DURABLE_STATE_SCHEMA_VERSION: u16 = 1;

/// Maximum aggregate exact Yahoo response bytes admitted to the durable cache.
///
/// The authority store accepts an eight-MiB logical payload. Reserving half for base64 expansion,
/// request/attempt metadata, and the admission snapshot leaves substantial headroom; the store
/// still enforces the exact serialized-payload boundary on every commit.
pub const MAX_YAHOO_DURABLE_CACHE_BODY_BYTES: usize =
    LocalAuthorityStateStore::maximum_payload_bytes() / 2;

/// Exclusively owned, two-copy crash-safe store for one Yahoo provider lane.
///
/// The value is intentionally not cloneable. A session consumes it so two independently admitted
/// workers cannot race one provider authority. Cookies and crumbs are structurally absent from the
/// persisted schema.
#[derive(Debug)]
pub struct YahooDurableStateStore {
    inner: LocalAuthorityStateStore,
    gate: Mutex<()>,
}

impl YahooDurableStateStore {
    /// Opens one capability-confined authority directory and acquires its lifetime lock.
    pub fn try_open(root: impl AsRef<Path>) -> Result<Self, YahooDurableStateError> {
        Ok(Self {
            inner: LocalAuthorityStateStore::try_open(root)?,
            gate: Mutex::new(()),
        })
    }

    pub(crate) fn load(&self) -> Result<Option<YahooDurableState>, YahooDurableStateError> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| YahooDurableStateError::SerializationUnavailable)?;
        self.load_locked()
    }

    pub(crate) fn compare_and_store(
        &self,
        expected_state_version: u64,
        admission: AdmissionSnapshot,
        cache: Vec<YahooDurableCacheEntry>,
    ) -> Result<u64, YahooDurableStateError> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| YahooDurableStateError::SerializationUnavailable)?;
        let current_version = self.load_locked()?.map_or(0, |state| state.state_version);
        if current_version != expected_state_version {
            return Err(YahooDurableStateError::StaleStateVersion);
        }
        let state_version = current_version
            .checked_add(1)
            .ok_or(YahooDurableStateError::StateVersionExhausted)?;
        validate_cache_identities(&cache)?;
        let wire = YahooDurableStateWire {
            schema_version: YAHOO_DURABLE_STATE_SCHEMA_VERSION,
            state_version,
            admission,
            cache: cache
                .into_iter()
                .map(YahooDurableCacheEntryWire::from)
                .collect(),
        };
        let payload = serde_json::to_vec(&wire).map_err(YahooDurableStateError::Encode)?;
        self.inner.store(&payload)?;
        Ok(state_version)
    }

    fn load_locked(&self) -> Result<Option<YahooDurableState>, YahooDurableStateError> {
        let Some(payload) = self.inner.load()? else {
            return Ok(None);
        };
        let wire: YahooDurableStateWire =
            serde_json::from_slice(&payload).map_err(YahooDurableStateError::Decode)?;
        if wire.schema_version != YAHOO_DURABLE_STATE_SCHEMA_VERSION || wire.state_version == 0 {
            return Err(YahooDurableStateError::InvalidState);
        }
        let mut cache = Vec::new();
        cache
            .try_reserve_exact(wire.cache.len())
            .map_err(|_| YahooDurableStateError::Allocation)?;
        for entry in wire.cache {
            cache.push(entry.try_into()?);
        }
        validate_cache_identities(&cache)?;
        Ok(Some(YahooDurableState {
            state_version: wire.state_version,
            admission: wire.admission,
            cache,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct YahooDurableState {
    pub(crate) state_version: u64,
    pub(crate) admission: AdmissionSnapshot,
    pub(crate) cache: Vec<YahooDurableCacheEntry>,
}

#[derive(Clone, Debug)]
pub(crate) struct YahooDurableCacheEntry {
    pub(crate) request_identity_sha256_hex: String,
    pub(crate) request: YahooHttpRequest,
    pub(crate) response_status: u16,
    pub(crate) response_content_type: Option<String>,
    pub(crate) response_sha256_hex: String,
    pub(crate) response_bytes: Vec<u8>,
    pub(crate) received_at_unix_ms: i64,
    pub(crate) available_at_unix_ms: i64,
    pub(crate) attempts: Vec<YahooHttpAttemptReceipt>,
    pub(crate) stored_at_unix_ms: i64,
    pub(crate) sequence: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct YahooDurableStateWire {
    schema_version: u16,
    state_version: u64,
    admission: AdmissionSnapshot,
    cache: Vec<YahooDurableCacheEntryWire>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct YahooDurableCacheEntryWire {
    request_identity_sha256_hex: String,
    request: YahooHttpRequest,
    response_status: u16,
    response_content_type: Option<String>,
    response_sha256_hex: String,
    response_body_base64: String,
    received_at_unix_ms: i64,
    available_at_unix_ms: i64,
    attempts: Vec<YahooHttpAttemptReceipt>,
    stored_at_unix_ms: i64,
    sequence: u64,
}

impl From<YahooDurableCacheEntry> for YahooDurableCacheEntryWire {
    fn from(value: YahooDurableCacheEntry) -> Self {
        Self {
            request_identity_sha256_hex: value.request_identity_sha256_hex,
            request: value.request,
            response_status: value.response_status,
            response_content_type: value.response_content_type,
            response_sha256_hex: value.response_sha256_hex,
            response_body_base64: STANDARD_NO_PAD.encode(value.response_bytes),
            received_at_unix_ms: value.received_at_unix_ms,
            available_at_unix_ms: value.available_at_unix_ms,
            attempts: value.attempts,
            stored_at_unix_ms: value.stored_at_unix_ms,
            sequence: value.sequence,
        }
    }
}

impl TryFrom<YahooDurableCacheEntryWire> for YahooDurableCacheEntry {
    type Error = YahooDurableStateError;

    fn try_from(value: YahooDurableCacheEntryWire) -> Result<Self, Self::Error> {
        let response_bytes = STANDARD_NO_PAD
            .decode(value.response_body_base64)
            .map_err(|_| YahooDurableStateError::InvalidState)?;
        Ok(Self {
            request_identity_sha256_hex: value.request_identity_sha256_hex,
            request: value.request,
            response_status: value.response_status,
            response_content_type: value.response_content_type,
            response_sha256_hex: value.response_sha256_hex,
            response_bytes,
            received_at_unix_ms: value.received_at_unix_ms,
            available_at_unix_ms: value.available_at_unix_ms,
            attempts: value.attempts,
            stored_at_unix_ms: value.stored_at_unix_ms,
            sequence: value.sequence,
        })
    }
}

fn validate_cache_identities(
    cache: &[YahooDurableCacheEntry],
) -> Result<(), YahooDurableStateError> {
    let mut identities = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    let mut body_bytes = 0_usize;
    for entry in cache {
        body_bytes = body_bytes
            .checked_add(entry.response_bytes.len())
            .ok_or(YahooDurableStateError::InvalidState)?;
        if entry.sequence == 0
            || !identities.insert(&entry.request_identity_sha256_hex)
            || !sequences.insert(entry.sequence)
        {
            return Err(YahooDurableStateError::InvalidState);
        }
    }
    if body_bytes > MAX_YAHOO_DURABLE_CACHE_BODY_BYTES {
        return Err(YahooDurableStateError::CacheBodyLimitExceeded);
    }
    Ok(())
}

/// Fail-closed durable Yahoo authority-state errors.
#[derive(Debug, Error)]
pub enum YahooDurableStateError {
    #[error("Yahoo durable authority-state store failed")]
    Store(#[from] LocalAuthorityStateStoreError),
    #[error("Yahoo durable authority state could not be decoded")]
    Decode(serde_json::Error),
    #[error("Yahoo durable authority state could not be encoded")]
    Encode(serde_json::Error),
    #[error("Yahoo durable authority state is invalid or unsupported")]
    InvalidState,
    #[error("Yahoo durable cache exceeds its bounded exact-body capacity")]
    CacheBodyLimitExceeded,
    #[error("Yahoo durable authority-state update is stale")]
    StaleStateVersion,
    #[error("Yahoo durable authority-state version is exhausted")]
    StateVersionExhausted,
    #[error("Yahoo durable authority-state serialization is unavailable")]
    SerializationUnavailable,
    #[error("Yahoo durable authority-state allocation failed")]
    Allocation,
}
