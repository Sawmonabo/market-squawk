//! Request identity, cancellation, deadline, and admitted service limits.

use std::{fmt, io::Write, sync::Arc, time::Instant};

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{ProgressLimits, ProgressReporter, ProgressSink};

const MAXIMUM_REQUEST_ID_BYTES: usize = 1024;
const MAXIMUM_JSON_DEPTH: usize = 64;
const MAXIMUM_JSON_STRING_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_JSON_ARRAY_ITEMS: usize = 1_000_000;
const MAXIMUM_JSON_MAP_ENTRIES: usize = 100_000;
const MAXIMUM_SERVICE_INLINE_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_SERVICE_INLINE_ITEMS: usize = 100_000;
const MAXIMUM_SERVICE_RESULT_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_SERVICE_RESULT_ITEMS: usize = 10_000_000;

/// Stable request identity shared by local transports and application services.
#[derive(Clone, Eq, Hash, PartialEq)]
pub enum RequestId {
    /// JSON-RPC integer identifier.
    Integer(i64),
    /// Bounded JSON-RPC string identifier.
    String(Arc<str>),
}

impl Serialize for RequestId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Integer(value) => value.serialize(serializer),
            Self::String(value) => value.as_ref().serialize(serializer),
        }
    }
}

impl RequestId {
    /// Creates a bounded string request identifier.
    ///
    /// # Errors
    ///
    /// Returns [`RequestIdError`] when the identifier exceeds 1,024 UTF-8 bytes.
    pub fn try_string(value: impl Into<Arc<str>>) -> Result<Self, RequestIdError> {
        let value = value.into();
        if value.len() > MAXIMUM_REQUEST_ID_BYTES {
            return Err(RequestIdError::TooLong {
                maximum_bytes: MAXIMUM_REQUEST_ID_BYTES,
            });
        }
        Ok(Self::String(value))
    }

    /// Returns canonical JSON bytes suitable for correlation hashing.
    ///
    /// # Errors
    ///
    /// Returns the serializer error if an identifier cannot be encoded.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

impl fmt::Debug for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => formatter.debug_tuple("Integer").field(value).finish(),
            Self::String(_) => formatter
                .debug_tuple("String")
                .field(&"[REQUEST ID REDACTED]")
                .finish(),
        }
    }
}

/// Invalid request identifier.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RequestIdError {
    /// String identifier exceeded its byte ceiling.
    #[error("request identifier exceeds {maximum_bytes} bytes")]
    TooLong {
        /// Maximum admitted UTF-8 bytes.
        maximum_bytes: usize,
    },
}

/// Maximum nested structure admitted for one JSON value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JsonStructureLimits {
    maximum_depth: usize,
    maximum_string_bytes: usize,
    maximum_array_items: usize,
    maximum_map_entries: usize,
}

impl JsonStructureLimits {
    /// Creates positive structural ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`JsonContractError::ZeroLimit`] when any ceiling is zero.
    pub fn try_new(
        maximum_depth: usize,
        maximum_string_bytes: usize,
        maximum_array_items: usize,
        maximum_map_entries: usize,
    ) -> Result<Self, JsonContractError> {
        if [
            maximum_depth,
            maximum_string_bytes,
            maximum_array_items,
            maximum_map_entries,
        ]
        .contains(&0)
        {
            return Err(JsonContractError::ZeroLimit);
        }
        if maximum_depth > MAXIMUM_JSON_DEPTH
            || maximum_string_bytes > MAXIMUM_JSON_STRING_BYTES
            || maximum_array_items > MAXIMUM_JSON_ARRAY_ITEMS
            || maximum_map_entries > MAXIMUM_JSON_MAP_ENTRIES
        {
            return Err(JsonContractError::LimitTooLarge);
        }
        Ok(Self {
            maximum_depth,
            maximum_string_bytes,
            maximum_array_items,
            maximum_map_entries,
        })
    }
}

/// Validates structural and encoded-size ceilings without retaining a second encoded payload.
///
/// # Errors
///
/// Returns [`JsonContractError`] when a nested value or its canonical compact JSON encoding exceeds
/// a configured ceiling.
pub fn validate_json_contract(
    value: &Value,
    structure: JsonStructureLimits,
    maximum_encoded_bytes: usize,
) -> Result<usize, JsonContractError> {
    if maximum_encoded_bytes == 0 {
        return Err(JsonContractError::ZeroLimit);
    }
    validate_nested(value, structure, 1)?;
    let mut counter = BoundedCounter::new(maximum_encoded_bytes);
    serde_json::to_writer(&mut counter, value).map_err(|_| JsonContractError::EncodingOrBytes)?;
    Ok(counter.written)
}

fn validate_nested(
    value: &Value,
    limits: JsonStructureLimits,
    depth: usize,
) -> Result<(), JsonContractError> {
    if depth > limits.maximum_depth {
        return Err(JsonContractError::Depth);
    }
    match value {
        Value::String(text) if text.len() > limits.maximum_string_bytes => {
            Err(JsonContractError::StringBytes)
        }
        Value::Array(values) => {
            if values.len() > limits.maximum_array_items {
                return Err(JsonContractError::ArrayItems);
            }
            for nested in values {
                validate_nested(nested, limits, depth.saturating_add(1))?;
            }
            Ok(())
        }
        Value::Object(values) => {
            if values.len() > limits.maximum_map_entries
                || values
                    .keys()
                    .any(|key| key.len() > limits.maximum_string_bytes)
            {
                return Err(JsonContractError::MapEntriesOrKeyBytes);
            }
            for nested in values.values() {
                validate_nested(nested, limits, depth.saturating_add(1))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

struct BoundedCounter {
    maximum: usize,
    written: usize,
}

impl BoundedCounter {
    const fn new(maximum: usize) -> Self {
        Self {
            maximum,
            written: 0,
        }
    }
}

impl Write for BoundedCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next = self
            .written
            .checked_add(buffer.len())
            .filter(|next| *next <= self.maximum)
            .ok_or_else(|| std::io::Error::other("bounded JSON byte ceiling exceeded"))?;
        self.written = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Invalid nested JSON contract.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JsonContractError {
    /// All ceilings must be positive.
    #[error("JSON limits must be nonzero")]
    ZeroLimit,
    /// A ceiling exceeded the implementation's stack or allocation safety bound.
    #[error("JSON limit exceeds the supported maximum")]
    LimitTooLarge,
    /// Nested value exceeded its depth ceiling.
    #[error("JSON depth limit exceeded")]
    Depth,
    /// String value exceeded its UTF-8 byte ceiling.
    #[error("JSON string byte limit exceeded")]
    StringBytes,
    /// Array exceeded its item ceiling.
    #[error("JSON array item limit exceeded")]
    ArrayItems,
    /// Object entry count or key bytes exceeded a ceiling.
    #[error("JSON object limit exceeded")]
    MapEntriesOrKeyBytes,
    /// Encoding failed or exceeded its byte ceiling.
    #[error("JSON encoded byte limit exceeded")]
    EncodingOrBytes,
}

/// Limits admitted by an application service for one request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceLimits {
    maximum_inline_bytes: usize,
    maximum_inline_items: usize,
    maximum_result_bytes: usize,
    maximum_result_items: usize,
    result_structure: JsonStructureLimits,
}

impl ServiceLimits {
    /// Creates internally consistent result limits.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceLimitsError`] when a limit is zero or an inline ceiling exceeds its hard
    /// result ceiling.
    pub fn try_new(
        maximum_inline_bytes: usize,
        maximum_inline_items: usize,
        maximum_result_bytes: usize,
        maximum_result_items: usize,
        result_structure: JsonStructureLimits,
    ) -> Result<Self, ServiceLimitsError> {
        if [
            maximum_inline_bytes,
            maximum_inline_items,
            maximum_result_bytes,
            maximum_result_items,
        ]
        .contains(&0)
        {
            return Err(ServiceLimitsError::Zero);
        }
        if maximum_inline_bytes > MAXIMUM_SERVICE_INLINE_BYTES
            || maximum_inline_items > MAXIMUM_SERVICE_INLINE_ITEMS
            || maximum_result_bytes > MAXIMUM_SERVICE_RESULT_BYTES
            || maximum_result_items > MAXIMUM_SERVICE_RESULT_ITEMS
        {
            return Err(ServiceLimitsError::LimitTooLarge);
        }
        if maximum_inline_bytes > maximum_result_bytes
            || maximum_inline_items > maximum_result_items
        {
            return Err(ServiceLimitsError::InlineExceedsResult);
        }
        Ok(Self {
            maximum_inline_bytes,
            maximum_inline_items,
            maximum_result_bytes,
            maximum_result_items,
            result_structure,
        })
    }

    /// Maximum encoded bytes returned inline.
    #[must_use]
    pub const fn maximum_inline_bytes(self) -> usize {
        self.maximum_inline_bytes
    }

    /// Maximum logical items returned inline.
    #[must_use]
    pub const fn maximum_inline_items(self) -> usize {
        self.maximum_inline_items
    }

    /// Hard ceiling for an encoded result, including artifact-bound results.
    #[must_use]
    pub const fn maximum_result_bytes(self) -> usize {
        self.maximum_result_bytes
    }

    /// Hard ceiling for logical result items.
    #[must_use]
    pub const fn maximum_result_items(self) -> usize {
        self.maximum_result_items
    }

    /// Nested JSON ceilings applied before a result becomes representable.
    #[must_use]
    pub const fn result_structure(self) -> JsonStructureLimits {
        self.result_structure
    }
}

/// Invalid service-limit configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServiceLimitsError {
    /// Every resource ceiling must be positive.
    #[error("service limits must be nonzero")]
    Zero,
    /// A ceiling exceeded the implementation's allocation safety bound.
    #[error("service limit exceeds the supported maximum")]
    LimitTooLarge,
    /// Inline data cannot exceed the hard result ceiling.
    #[error("inline service limits cannot exceed hard result limits")]
    InlineExceedsResult,
}

/// Authority-neutral context for one bounded service call.
#[derive(Clone)]
pub struct RequestContext {
    request_id: RequestId,
    cancellation: CancellationToken,
    deadline: Instant,
    limits: ServiceLimits,
    progress: ProgressReporter,
}

impl RequestContext {
    /// Creates a context from transport-admitted limits and lifecycle controls.
    #[must_use]
    pub fn new(
        request_id: RequestId,
        cancellation: CancellationToken,
        deadline: Instant,
        limits: ServiceLimits,
    ) -> Self {
        let progress =
            ProgressReporter::disabled(cancellation.clone(), deadline, ProgressLimits::default());
        Self {
            request_id,
            cancellation,
            deadline,
            limits,
            progress,
        }
    }

    /// Creates a context with a bounded transport-neutral progress capability.
    #[must_use]
    pub fn with_progress(
        request_id: RequestId,
        cancellation: CancellationToken,
        deadline: Instant,
        limits: ServiceLimits,
        progress_limits: ProgressLimits,
        progress_sink: Arc<dyn ProgressSink>,
    ) -> Self {
        let progress = ProgressReporter::enabled(
            progress_sink,
            cancellation.clone(),
            deadline,
            progress_limits,
        );
        Self {
            request_id,
            cancellation,
            deadline,
            limits,
            progress,
        }
    }

    /// Correlation identity for this request.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Child cancellation token owned by the request lifecycle.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Monotonic deadline after which work must stop.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Limits admitted for this call.
    #[must_use]
    pub const fn limits(&self) -> ServiceLimits {
        self.limits
    }

    /// Bounded progress reporter associated with this request.
    #[must_use]
    pub const fn progress(&self) -> &ProgressReporter {
        &self.progress
    }
}

impl fmt::Debug for RequestContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestContext")
            .field("request_id", &self.request_id)
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("deadline", &self.deadline)
            .field("limits", &self.limits)
            .field("progress", &self.progress)
            .finish()
    }
}
