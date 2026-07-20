//! Validated transport and service resource ceilings.

use std::time::Duration;

use market_squawk_services::{
    JsonContractError, JsonStructureLimits, ServiceLimits, ServiceLimitsError,
};
use thiserror::Error;

const MAXIMUM_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const MAXIMUM_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_DEPTH: usize = 64;
const MAXIMUM_STRING_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_ARRAY_ITEMS: usize = 1_000_000;
const MAXIMUM_MAP_ENTRIES: usize = 100_000;
const MAXIMUM_ACTIVE_REQUESTS: usize = 4_096;
const MAXIMUM_WRITER_QUEUE_CAPACITY: usize = 4_096;
const MAXIMUM_WRITER_QUEUE_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_INLINE_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_INLINE_ITEMS: usize = 100_000;
const MAXIMUM_RESULT_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_RESULT_ITEMS: usize = 10_000_000;
const MAXIMUM_RESPONSE_ENVELOPE_BYTES: usize = 16 * 1024;
const MAXIMUM_ACTIVE_BODY_BYTES: usize = 512 * 1024 * 1024;
const MAXIMUM_ACTIVE_RESULT_BYTES: usize = 512 * 1024 * 1024;

/// Untrusted configuration input for MCP resource ceilings.
#[derive(Clone, Copy, Debug)]
pub struct McpLimitSpec {
    /// Maximum bytes in one newline-delimited frame, excluding the delimiter.
    pub maximum_frame_bytes: usize,
    /// Maximum JSON body bytes admitted after framing.
    pub maximum_body_bytes: usize,
    /// Maximum nested JSON array/object depth.
    pub maximum_depth: usize,
    /// Maximum UTF-8 bytes in any JSON string.
    pub maximum_string_bytes: usize,
    /// Maximum items in any JSON array.
    pub maximum_array_items: usize,
    /// Maximum entries in any JSON object.
    pub maximum_map_entries: usize,
    /// Maximum active request identities.
    pub maximum_active_requests: usize,
    /// Maximum encoded messages awaiting the single writer.
    pub writer_queue_capacity: usize,
    /// Maximum encoded bytes retained by messages awaiting the single writer.
    pub maximum_writer_queue_bytes: usize,
    /// Maximum result bytes returned inline.
    pub maximum_inline_bytes: usize,
    /// Maximum logical result items returned inline.
    pub maximum_inline_items: usize,
    /// Hard ceiling for an encoded result, including artifact-bound output.
    pub maximum_result_bytes: usize,
    /// Hard ceiling for logical result items.
    pub maximum_result_items: usize,
    /// Default request execution deadline.
    pub request_timeout: Duration,
    /// Maximum queue admission or physical write duration.
    pub write_timeout: Duration,
    /// Maximum SDK and writer shutdown duration.
    pub shutdown_timeout: Duration,
}

impl Default for McpLimitSpec {
    fn default() -> Self {
        Self {
            maximum_frame_bytes: 1024 * 1024,
            maximum_body_bytes: 1024 * 1024,
            maximum_depth: 32,
            maximum_string_bytes: 64 * 1024,
            maximum_array_items: 10_000,
            maximum_map_entries: 2_000,
            maximum_active_requests: 8,
            writer_queue_capacity: 64,
            maximum_writer_queue_bytes: 8 * 1024 * 1024,
            maximum_inline_bytes: 64 * 1024,
            maximum_inline_items: 1_000,
            maximum_result_bytes: 64 * 1024 * 1024,
            maximum_result_items: 1_000_000,
            request_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// Validated MCP resource ceilings.
#[derive(Clone, Copy, Debug)]
pub struct McpLimits {
    spec: McpLimitSpec,
    service: ServiceLimits,
    input_structure: JsonStructureLimits,
}

impl TryFrom<McpLimitSpec> for McpLimits {
    type Error = McpLimitError;

    fn try_from(spec: McpLimitSpec) -> Result<Self, Self::Error> {
        if [
            spec.maximum_frame_bytes,
            spec.maximum_body_bytes,
            spec.maximum_depth,
            spec.maximum_string_bytes,
            spec.maximum_array_items,
            spec.maximum_map_entries,
            spec.maximum_active_requests,
            spec.writer_queue_capacity,
            spec.maximum_writer_queue_bytes,
            spec.maximum_inline_bytes,
            spec.maximum_inline_items,
            spec.maximum_result_bytes,
            spec.maximum_result_items,
        ]
        .contains(&0)
            || spec.request_timeout.is_zero()
            || spec.write_timeout.is_zero()
            || spec.shutdown_timeout.is_zero()
        {
            return Err(McpLimitError::Zero);
        }
        if spec.maximum_body_bytes > spec.maximum_frame_bytes {
            return Err(McpLimitError::BodyExceedsFrame);
        }
        if spec.maximum_frame_bytes > MAXIMUM_FRAME_BYTES
            || spec.maximum_body_bytes > MAXIMUM_FRAME_BYTES
            || spec.maximum_depth > MAXIMUM_DEPTH
            || spec.maximum_string_bytes > MAXIMUM_STRING_BYTES
            || spec.maximum_array_items > MAXIMUM_ARRAY_ITEMS
            || spec.maximum_map_entries > MAXIMUM_MAP_ENTRIES
            || spec.maximum_active_requests > MAXIMUM_ACTIVE_REQUESTS
            || spec.writer_queue_capacity > MAXIMUM_WRITER_QUEUE_CAPACITY
            || spec.maximum_writer_queue_bytes > MAXIMUM_WRITER_QUEUE_BYTES
            || spec.maximum_inline_bytes > MAXIMUM_INLINE_BYTES
            || spec.maximum_inline_items > MAXIMUM_INLINE_ITEMS
            || spec.maximum_result_bytes > MAXIMUM_RESULT_BYTES
            || spec.maximum_result_items > MAXIMUM_RESULT_ITEMS
        {
            return Err(McpLimitError::LimitTooLarge);
        }
        let framed_message_bytes = spec
            .maximum_frame_bytes
            .checked_add(1)
            .ok_or(McpLimitError::LimitTooLarge)?;
        if spec.maximum_writer_queue_bytes < framed_message_bytes {
            return Err(McpLimitError::WriterBudgetBelowFrame);
        }
        if spec
            .maximum_inline_bytes
            .checked_add(MAXIMUM_RESPONSE_ENVELOPE_BYTES)
            .is_none_or(|encoded| encoded > spec.maximum_frame_bytes)
        {
            return Err(McpLimitError::InlineExceedsFrame);
        }
        if spec
            .maximum_active_requests
            .checked_mul(spec.maximum_body_bytes)
            .is_none_or(|bytes| bytes > MAXIMUM_ACTIVE_BODY_BYTES)
            || spec
                .maximum_active_requests
                .checked_mul(spec.maximum_result_bytes)
                .is_none_or(|bytes| bytes > MAXIMUM_ACTIVE_RESULT_BYTES)
        {
            return Err(McpLimitError::AggregateActiveBytes);
        }
        if u32::try_from(spec.maximum_writer_queue_bytes).is_err() {
            return Err(McpLimitError::LimitTooLarge);
        }
        if [
            spec.request_timeout,
            spec.write_timeout,
            spec.shutdown_timeout,
        ]
        .into_iter()
        .any(|duration| duration > MAXIMUM_TIMEOUT)
        {
            return Err(McpLimitError::DurationTooLarge);
        }
        let input_structure = JsonStructureLimits::try_new(
            spec.maximum_depth,
            spec.maximum_string_bytes,
            spec.maximum_array_items,
            spec.maximum_map_entries,
        )?;
        let service = ServiceLimits::try_new(
            spec.maximum_inline_bytes,
            spec.maximum_inline_items,
            spec.maximum_result_bytes,
            spec.maximum_result_items,
            input_structure,
        )?;
        Ok(Self {
            spec,
            service,
            input_structure,
        })
    }
}

impl McpLimits {
    pub(crate) const fn maximum_frame_bytes(self) -> usize {
        self.spec.maximum_frame_bytes
    }

    pub(crate) const fn maximum_body_bytes(self) -> usize {
        self.spec.maximum_body_bytes
    }

    pub(crate) const fn maximum_active_requests(self) -> usize {
        self.spec.maximum_active_requests
    }

    pub(crate) const fn writer_queue_capacity(self) -> usize {
        self.spec.writer_queue_capacity
    }

    pub(crate) const fn maximum_writer_queue_bytes(self) -> usize {
        self.spec.maximum_writer_queue_bytes
    }

    pub(crate) const fn request_timeout(self) -> Duration {
        self.spec.request_timeout
    }

    pub(crate) const fn write_timeout(self) -> Duration {
        self.spec.write_timeout
    }

    pub(crate) const fn shutdown_timeout(self) -> Duration {
        self.spec.shutdown_timeout
    }

    pub(crate) const fn service_limits(self) -> ServiceLimits {
        self.service
    }

    pub(crate) const fn input_structure(self) -> JsonStructureLimits {
        self.input_structure
    }
}

/// Invalid resource-ceiling configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpLimitError {
    /// All ceilings and durations must be positive.
    #[error("MCP limits must be nonzero")]
    Zero,
    /// JSON body bytes cannot exceed the outer frame.
    #[error("MCP body limit cannot exceed frame limit")]
    BodyExceedsFrame,
    /// Inline structured data plus worst-case JSON-RPC identity/envelope must fit one frame.
    #[error("MCP inline result and response envelope exceed the frame limit")]
    InlineExceedsFrame,
    /// Cross-field active-request and result memory exposure must remain bounded.
    #[error("MCP aggregate active byte exposure exceeds the supported maximum")]
    AggregateActiveBytes,
    /// The writer byte budget must hold any single maximum-sized framed message.
    #[error("MCP writer byte budget cannot hold one maximum-sized frame")]
    WriterBudgetBelowFrame,
    /// Allocation and semaphore-backed limits must fit validated implementation bounds.
    #[error("MCP resource limit exceeds the supported maximum")]
    LimitTooLarge,
    /// Deadlines are capped so arithmetic and service lifetimes remain operationally bounded.
    #[error("MCP timeout exceeds 24 hours")]
    DurationTooLarge,
    /// Service result limits are internally inconsistent.
    #[error("invalid MCP service limits: {0}")]
    Service(#[from] ServiceLimitsError),
    /// JSON structural limits are invalid.
    #[error("invalid MCP JSON limits: {0}")]
    Json(#[from] JsonContractError),
}
