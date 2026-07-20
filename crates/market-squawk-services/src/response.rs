//! Bounded service-result contracts.

use std::fmt;

use serde_json::Value;
use thiserror::Error;

use crate::{JsonContractError, ServiceLimits, validate_json_contract};

/// Structured result returned by a transport-neutral application service.
#[derive(Clone)]
pub struct TypedToolResult {
    structured_content: Value,
    item_count: usize,
    encoded_bytes: usize,
}

impl TypedToolResult {
    /// Creates a structured result with an explicit logical item count.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError`] when content, logical items, or encoded bytes exceed the
    /// request's hard result contract.
    pub fn try_new(
        structured_content: Value,
        item_count: usize,
        limits: ServiceLimits,
    ) -> Result<Self, ServiceContractError> {
        if item_count == 0 && !structured_content.is_null() {
            return Err(ServiceContractError::ZeroItemsForNonNullResult);
        }
        if item_count > limits.maximum_result_items() {
            return Err(ServiceContractError::TooManyItems);
        }
        let encoded_bytes = validate_json_contract(
            &structured_content,
            limits.result_structure(),
            limits.maximum_result_bytes(),
        )?;
        Ok(Self {
            structured_content,
            item_count,
            encoded_bytes,
        })
    }

    /// Structured JSON content. Transports must apply their byte and item ceilings before output.
    #[must_use]
    pub const fn structured_content(&self) -> &Value {
        &self.structured_content
    }

    /// Logical number of records represented by this result.
    #[must_use]
    pub const fn item_count(&self) -> usize {
        self.item_count
    }

    /// Compact JSON size established by bounded construction.
    #[must_use]
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Consumes the result into its structured content and logical item count.
    #[must_use]
    pub fn into_parts(self) -> (Value, usize, usize) {
        (self.structured_content, self.item_count, self.encoded_bytes)
    }
}

impl fmt::Debug for TypedToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedToolResult")
            .field("structured_content", &"[STRUCTURED CONTENT REDACTED]")
            .field("item_count", &self.item_count)
            .field("encoded_bytes", &self.encoded_bytes)
            .finish()
    }
}

/// Invalid service response construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ServiceContractError {
    /// Non-null structured content must declare at least one logical item.
    #[error("non-null service results must declare at least one logical item")]
    ZeroItemsForNonNullResult,
    /// Logical result item count exceeded the request ceiling.
    #[error("service result item limit exceeded")]
    TooManyItems,
    /// Structured JSON violated its depth, container, string, or encoded-byte ceiling.
    #[error("service result JSON contract failed: {0}")]
    Json(#[from] JsonContractError),
}
