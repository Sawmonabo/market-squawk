//! Bounded service-result contracts.

use std::{fmt, io::Write};

use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{
    JsonContractError, JsonStructureLimits, ServiceLimits, SourceEvidencePolicy, ToolDescriptor,
    validate_json_contract,
};

const MAXIMUM_EVIDENCE_BYTES: usize = 8 * 1024;

/// Whether the service returned the complete admitted result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResultCompleteness {
    /// Every available item under the admitted request contract is present.
    Complete,
    /// A deterministic bounded prefix or selection is present and more items were available.
    Truncated,
}

impl ResultCompleteness {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Truncated => "truncated",
        }
    }
}

/// Bounded source, quality, and completeness metadata returned by every service call.
#[derive(Clone)]
pub struct ToolResultMetadata {
    completeness: ResultCompleteness,
    available_items: Option<usize>,
    source_coverage: Value,
    data_quality: Value,
    source_evidence: SourceEvidencePolicy,
}

impl ToolResultMetadata {
    /// Creates complete result metadata with source coverage and data-quality evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError::InvalidMetadata`] when either evidence value is empty or
    /// exceeds the transport-neutral evidence ceiling.
    pub fn try_complete(
        source_coverage: Value,
        data_quality: Value,
    ) -> Result<Self, ServiceContractError> {
        Self::try_with_source(
            ResultCompleteness::Complete,
            None,
            source_coverage,
            data_quality,
        )
    }

    /// Creates truncated result metadata with the complete available-item count and source
    /// evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError`] when the available-item count is zero or evidence is
    /// invalid. Construction of [`TypedToolResult`] additionally requires `available_items` to
    /// exceed the returned item count.
    pub fn try_truncated(
        available_items: usize,
        source_coverage: Value,
        data_quality: Value,
    ) -> Result<Self, ServiceContractError> {
        if available_items == 0 {
            return Err(ServiceContractError::InvalidCompleteness);
        }
        Self::try_with_source(
            ResultCompleteness::Truncated,
            Some(available_items),
            source_coverage,
            data_quality,
        )
    }

    /// Creates complete metadata for a result that is not derived from a data source.
    #[must_use]
    pub fn complete_not_applicable() -> Self {
        Self {
            completeness: ResultCompleteness::Complete,
            available_items: None,
            source_coverage: not_applicable_value(),
            data_quality: not_applicable_value(),
            source_evidence: SourceEvidencePolicy::NotApplicable,
        }
    }

    /// Creates truncated metadata for a non-source-derived result.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError::InvalidCompleteness`] when `available_items` is zero.
    pub fn try_truncated_not_applicable(
        available_items: usize,
    ) -> Result<Self, ServiceContractError> {
        if available_items == 0 {
            return Err(ServiceContractError::InvalidCompleteness);
        }
        Ok(Self {
            completeness: ResultCompleteness::Truncated,
            available_items: Some(available_items),
            source_coverage: not_applicable_value(),
            data_quality: not_applicable_value(),
            source_evidence: SourceEvidencePolicy::NotApplicable,
        })
    }

    fn try_with_source(
        completeness: ResultCompleteness,
        available_items: Option<usize>,
        source_coverage: Value,
        data_quality: Value,
    ) -> Result<Self, ServiceContractError> {
        validate_evidence(&source_coverage)?;
        validate_evidence(&data_quality)?;
        Ok(Self {
            completeness,
            available_items,
            source_coverage,
            data_quality,
            source_evidence: SourceEvidencePolicy::Required,
        })
    }

    /// Completeness classification.
    #[must_use]
    pub const fn completeness(&self) -> ResultCompleteness {
        self.completeness
    }

    /// Complete available-item count for a truncated result.
    #[must_use]
    pub const fn available_items(&self) -> Option<usize> {
        self.available_items
    }

    /// Source-coverage evidence or an explicit not-applicable object.
    #[must_use]
    pub const fn source_coverage(&self) -> &Value {
        &self.source_coverage
    }

    /// Data-quality evidence or an explicit not-applicable object.
    #[must_use]
    pub const fn data_quality(&self) -> &Value {
        &self.data_quality
    }

    /// Evidence policy represented by this metadata.
    #[must_use]
    pub const fn source_evidence(&self) -> SourceEvidencePolicy {
        self.source_evidence
    }

    fn validate_item_count(&self, returned_items: usize) -> Result<(), ServiceContractError> {
        match (self.completeness, self.available_items) {
            (ResultCompleteness::Complete, None) => Ok(()),
            (ResultCompleteness::Truncated, Some(available_items))
                if available_items > returned_items =>
            {
                Ok(())
            }
            _ => Err(ServiceContractError::InvalidCompleteness),
        }
    }

    fn view(&self, returned_items: usize) -> SerializedMetadata<'_> {
        SerializedMetadata {
            completeness: self.completeness,
            returned_items,
            available_items: self.available_items.unwrap_or(returned_items),
            source_coverage: &self.source_coverage,
            data_quality: &self.data_quality,
        }
    }

    fn to_value(&self, returned_items: usize) -> Value {
        self.clone().into_value(returned_items)
    }

    fn into_value(self, returned_items: usize) -> Value {
        let mut metadata = Map::new();
        metadata.insert(
            "completeness".to_owned(),
            Value::String(self.completeness.as_str().to_owned()),
        );
        metadata.insert("returnedItems".to_owned(), Value::from(returned_items));
        metadata.insert(
            "availableItems".to_owned(),
            Value::from(self.available_items.unwrap_or(returned_items)),
        );
        metadata.insert("sourceCoverage".to_owned(), self.source_coverage);
        metadata.insert("dataQuality".to_owned(), self.data_quality);
        Value::Object(metadata)
    }
}

impl fmt::Debug for ToolResultMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResultMetadata")
            .field("completeness", &self.completeness)
            .field("available_items", &self.available_items)
            .field("source_coverage", &"[SOURCE COVERAGE REDACTED]")
            .field("data_quality", &"[DATA QUALITY REDACTED]")
            .field("source_evidence", &self.source_evidence)
            .finish()
    }
}

/// Structured result returned by a transport-neutral application service.
#[derive(Clone)]
pub struct TypedToolResult {
    structured_content: Value,
    metadata: ToolResultMetadata,
    item_count: usize,
    encoded_bytes: usize,
}

impl TypedToolResult {
    /// Creates a structured result with explicit logical item and evidence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError`] when content, metadata, logical items, or the complete
    /// encoded result envelope exceeds the request's hard result contract.
    pub fn try_new(
        structured_content: Value,
        item_count: usize,
        metadata: ToolResultMetadata,
        limits: ServiceLimits,
    ) -> Result<Self, ServiceContractError> {
        if item_count == 0 && !structured_content.is_null() {
            return Err(ServiceContractError::ZeroItemsForNonNullResult);
        }
        if item_count > limits.maximum_result_items() {
            return Err(ServiceContractError::TooManyItems);
        }
        metadata.validate_item_count(item_count)?;
        validate_json_contract(
            &structured_content,
            limits.result_structure(),
            limits.maximum_result_bytes(),
        )?;
        let encoded_bytes =
            encoded_envelope_bytes(&structured_content, &metadata, item_count, limits)?;
        Ok(Self {
            structured_content,
            metadata,
            item_count,
            encoded_bytes,
        })
    }

    /// Structured business content without transport framing.
    #[must_use]
    pub const fn structured_content(&self) -> &Value {
        &self.structured_content
    }

    /// Completeness, coverage, and quality metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ToolResultMetadata {
        &self.metadata
    }

    /// Logical number of records represented by this result.
    #[must_use]
    pub const fn item_count(&self) -> usize {
        self.item_count
    }

    /// Compact JSON size of the complete data-and-metadata envelope.
    #[must_use]
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Revalidates this result against the exact limits admitted by its current caller.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError`] when the logical item count, metadata, or structured JSON
    /// exceeds any hard limit supplied by the current caller.
    pub fn validate_against(&self, limits: ServiceLimits) -> Result<(), ServiceContractError> {
        if self.item_count > limits.maximum_result_items() {
            return Err(ServiceContractError::TooManyItems);
        }
        self.metadata.validate_item_count(self.item_count)?;
        validate_json_contract(
            &self.structured_content,
            limits.result_structure(),
            limits.maximum_result_bytes(),
        )?;
        let _ = encoded_envelope_bytes(
            &self.structured_content,
            &self.metadata,
            self.item_count,
            limits,
        )?;
        Ok(())
    }

    /// Validates descriptor-owned source-evidence and operation-data policy before publication.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceContractError`] when the result does not satisfy the exact descriptor
    /// contract that admitted its request.
    pub fn validate_for(&self, descriptor: &ToolDescriptor) -> Result<(), ServiceContractError> {
        if self.metadata.source_evidence() != descriptor.contract().result().source_evidence() {
            return Err(ServiceContractError::SourceEvidencePolicy);
        }
        if !descriptor.validates_output_data(&self.structured_content) {
            return Err(ServiceContractError::SourceEvidencePolicy);
        }
        Ok(())
    }

    /// Returns a small structured copy of completeness, coverage, and quality metadata.
    #[must_use]
    pub fn metadata_value(&self) -> Value {
        self.metadata.to_value(self.item_count)
    }

    /// Consumes the result into the canonical transport-neutral envelope.
    #[must_use]
    pub fn into_envelope(self) -> Value {
        let mut envelope = Map::new();
        envelope.insert("data".to_owned(), self.structured_content);
        envelope.insert(
            "metadata".to_owned(),
            self.metadata.into_value(self.item_count),
        );
        Value::Object(envelope)
    }
}

impl fmt::Debug for TypedToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedToolResult")
            .field("structured_content", &"[STRUCTURED CONTENT REDACTED]")
            .field("metadata", &self.metadata)
            .field("item_count", &self.item_count)
            .field("encoded_bytes", &self.encoded_bytes)
            .finish()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedMetadata<'value> {
    completeness: ResultCompleteness,
    returned_items: usize,
    available_items: usize,
    source_coverage: &'value Value,
    data_quality: &'value Value,
}

#[derive(Serialize)]
struct SerializedEnvelope<'value> {
    data: &'value Value,
    metadata: SerializedMetadata<'value>,
}

fn encoded_envelope_bytes(
    structured_content: &Value,
    metadata: &ToolResultMetadata,
    item_count: usize,
    limits: ServiceLimits,
) -> Result<usize, ServiceContractError> {
    let envelope = SerializedEnvelope {
        data: structured_content,
        metadata: metadata.view(item_count),
    };
    let mut counter = BoundedCounter::new(limits.maximum_result_bytes());
    serde_json::to_writer(&mut counter, &envelope)
        .map_err(|_| JsonContractError::EncodingOrBytes)?;
    Ok(counter.written)
}

fn validate_evidence(value: &Value) -> Result<(), ServiceContractError> {
    let present = match value {
        Value::Object(values) => !values.is_empty(),
        Value::Array(values) => !values.is_empty(),
        Value::String(value) => !value.is_empty(),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    };
    if !present {
        return Err(ServiceContractError::InvalidMetadata);
    }
    let limits = JsonStructureLimits::try_new(8, 4 * 1024, 256, 256)
        .map_err(|_| ServiceContractError::InvalidMetadata)?;
    validate_json_contract(value, limits, MAXIMUM_EVIDENCE_BYTES)
        .map_err(|_| ServiceContractError::InvalidMetadata)?;
    Ok(())
}

fn not_applicable_value() -> Value {
    let mut value = Map::new();
    value.insert(
        "status".to_owned(),
        Value::String("not_applicable".to_owned()),
    );
    Value::Object(value)
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
        self.written = self
            .written
            .checked_add(buffer.len())
            .filter(|written| *written <= self.maximum)
            .ok_or_else(|| std::io::Error::other("bounded service result ceiling exceeded"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
    /// Coverage or quality evidence was absent, empty, or structurally unbounded.
    #[error("service result metadata is invalid")]
    InvalidMetadata,
    /// Complete/truncated metadata contradicted returned and available item counts.
    #[error("service result completeness metadata is inconsistent")]
    InvalidCompleteness,
    /// Source evidence or structured content contradicted the exact descriptor result policy.
    #[error("service result violates the descriptor-owned output contract")]
    SourceEvidencePolicy,
    /// Structured JSON violated its depth, container, string, or encoded-byte ceiling.
    #[error("service result JSON contract failed: {0}")]
    Json(#[from] JsonContractError),
}
