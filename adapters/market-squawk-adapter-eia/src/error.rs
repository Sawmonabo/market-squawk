//! Closed adapter failures that never retain a credential or secret-bearing URL.

use thiserror::Error;

/// Closed payload-free reason why a bounded EIA JSON structure was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaStructureLimitKind {
    /// JSON nesting exceeded the configured parser depth.
    JsonDepth,
    /// JSON node cardinality exceeded the configured parser count.
    JsonNodes,
    /// One JSON object contained too many fields.
    ObjectFields,
    /// One JSON object key exceeded the configured byte count.
    KeyBytes,
    /// One JSON string value exceeded the configured byte count.
    StringBytes,
    /// One route metadata collection exceeded the configured item count.
    MetadataItems,
    /// A retained provider string or object key contained a disallowed control character.
    ControlCharacter,
    /// Secret-redaction traversal exceeded the configured JSON depth.
    RedactionDepth,
    /// Schema-shape traversal exceeded its fixed depth ceiling.
    ShapeDepth,
}

/// Safe numeric receipt for one rejected EIA JSON structure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaStructureLimitReceipt {
    kind: EiaStructureLimitKind,
    observed: usize,
    limit: usize,
}

impl EiaStructureLimitReceipt {
    pub(crate) const fn new(kind: EiaStructureLimitKind, observed: usize, limit: usize) -> Self {
        Self {
            kind,
            observed,
            limit,
        }
    }

    /// Returns the closed structural dimension that was rejected.
    pub const fn kind(self) -> EiaStructureLimitKind {
        self.kind
    }

    /// Returns the safe numeric value observed at the rejection boundary.
    pub const fn observed(self) -> usize {
        self.observed
    }

    /// Returns the unchanged configured ceiling for that dimension.
    pub const fn limit(self) -> usize {
        self.limit
    }
}

/// A bounded EIA request, protocol, schema, pagination, or canonicalization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EiaError {
    /// An EIA API key is empty, oversized, or contains a control character.
    #[error("invalid EIA API key")]
    InvalidApiKey,
    /// A route is empty or contains a non-admitted path segment.
    #[error("invalid EIA API v2 route")]
    InvalidRoute,
    /// A field, facet, frequency, descriptor, or provider value identifier is invalid.
    #[error("invalid bounded EIA identifier")]
    InvalidIdentifier,
    /// Parser, page, or application-rate limits are zero or exceed the admitted boundary.
    #[error("invalid EIA admission limit")]
    InvalidLimit,
    /// A fallible bounded allocation could not be reserved.
    #[error("bounded EIA allocation failed")]
    AllocationFailure,
    /// A URL could not be constructed from already validated request coordinates.
    #[error("failed to construct EIA request")]
    RequestConstruction,
    /// A response exceeds the configured byte budget.
    #[error("EIA response exceeds the configured byte budget")]
    BodyTooLarge,
    /// A response is not valid JSON.
    #[error("invalid EIA JSON response")]
    InvalidJson,
    /// A response exceeds one exact admitted structural parser limit.
    #[error("EIA response exceeds structural parser limits: {receipt:?}")]
    StructureLimit {
        /// Closed numeric evidence that retains no provider payload.
        receipt: EiaStructureLimitReceipt,
    },
    /// A required documented response field is absent or has the wrong type.
    #[error("invalid EIA API v2 response shape")]
    InvalidProtocol,
    /// The response parameter object or request shape differs from the exact request.
    #[error("EIA interpreted-request parameters do not match the request")]
    RequestEchoMismatch,
    /// The response command differs from the exact requested route/surface.
    #[error("EIA interpreted-request command does not match the request")]
    RequestEchoCommandMismatch,
    /// The response carries more than one redactable API-key field.
    #[error("EIA interpreted-request secret-field count differs: {observed}")]
    RequestEchoSecretCountMismatch {
        /// Number of secret fields, without any field value or provider payload.
        observed: usize,
    },
    /// The response API version is not API v2 or differs from the frozen route contract.
    #[error("EIA API version drift")]
    ApiVersionDrift,
    /// Route metadata no longer matches the frozen field/facet/frequency contract.
    #[error("EIA route schema drift")]
    SchemaDrift,
    /// Metadata repeats one identity with incompatible definitions.
    #[error("conflicting EIA metadata identity")]
    MetadataConflict,
    /// A provider count, offset, length, or page transition is inconsistent.
    #[error("invalid EIA pagination evidence")]
    Pagination,
    /// Offset pagination does not carry a deterministic total row ordering.
    #[error("EIA query does not define a deterministic total sort")]
    NonTotalSort,
    /// An exact provider value does not satisfy its route-specific value contract.
    #[error("invalid EIA observation value")]
    InvalidValue,
    /// A row unit is absent or differs from the frozen metadata contract.
    #[error("invalid EIA observation unit")]
    InvalidUnit,
    /// A period, release, updated, or availability coordinate is malformed or contradictory.
    #[error("invalid EIA observation clock")]
    InvalidClock,
    /// Two rows in one acquisition claim the same family and period with different content.
    #[error("conflicting EIA observations in one acquisition")]
    ObservationConflict,
    /// A response repeated the same natural observation family instead of a disjoint offset row.
    #[error("replayed EIA observation in one acquisition")]
    ObservationReplay,
    /// A complete revision plan cannot be produced from the supplied previous heads.
    #[error("invalid EIA revision authority input")]
    InvalidRevision,
    /// A native value cannot enter the canonical macro observation family.
    #[error("EIA value cannot be normalized as a canonical macro observation")]
    Canonicalization,
    /// Canonical output does not match the exact actual-sealed response chain.
    #[error("EIA capture evidence does not match the response chain")]
    CaptureBinding,
}

impl EiaError {
    pub(crate) const fn structure_limit(
        kind: EiaStructureLimitKind,
        observed: usize,
        limit: usize,
    ) -> Self {
        Self::StructureLimit {
            receipt: EiaStructureLimitReceipt::new(kind, observed, limit),
        }
    }
}
