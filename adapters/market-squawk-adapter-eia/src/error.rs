//! Closed adapter failures that never retain a credential or secret-bearing URL.

use thiserror::Error;

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
    /// A URL could not be constructed from already validated request coordinates.
    #[error("failed to construct EIA request")]
    RequestConstruction,
    /// A response exceeds the configured byte budget.
    #[error("EIA response exceeds the configured byte budget")]
    BodyTooLarge,
    /// A response is not valid JSON.
    #[error("invalid EIA JSON response")]
    InvalidJson,
    /// A response exceeds the admitted nesting, node, field, or string limits.
    #[error("EIA response exceeds structural parser limits")]
    StructureLimit,
    /// A required documented response field is absent or has the wrong type.
    #[error("invalid EIA API v2 response shape")]
    InvalidProtocol,
    /// The response command does not match the exact requested route/surface.
    #[error("EIA interpreted-request command does not match the request")]
    RequestEchoMismatch,
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
    /// A complete revision plan cannot be produced from the supplied previous heads.
    #[error("invalid EIA revision authority input")]
    InvalidRevision,
    /// A native value cannot enter the canonical macro observation family.
    #[error("EIA value cannot be normalized as a canonical macro observation")]
    Canonicalization,
}
