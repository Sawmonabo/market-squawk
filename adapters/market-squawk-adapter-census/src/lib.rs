//! Bounded, metadata-driven U.S. Census Data API contracts.
//!
//! This crate owns provider-native request construction, discovery decoding, two-dimensional
//! response decoding, and a bounded registry-authorized HTTPS transport. It grants no publication
//! authority. Callers must seal the returned exact capture material and pass normalized
//! observations through shared revision, canonical-publication, and point-in-time authorities
//! before application use.

mod discovery;
mod doctor;
mod http;
mod policy;
mod query;
mod response;
mod runtime;
mod source;

pub use discovery::{
    CensusDatasetCatalog, CensusDatasetMetadata, CensusDiscoveryDocument, CensusGeographyAdmission,
    CensusGeographyCatalog, CensusGeographyMetadata, CensusGroupCatalog, CensusGroupMetadata,
    CensusMetadataEvidence, CensusPredicateType, CensusRequiredVariable, CensusVariableCatalog,
    CensusVariableMetadata,
};
pub use doctor::{
    CENSUS_DOCTOR_MAX_RESPONSE_BYTES, CENSUS_DOCTOR_TIMEOUT, CensusDoctorOutput,
    CensusDoctorRateHeaderEvidence, CensusDoctorReadiness, CensusDoctorReport, CensusDoctorScope,
    CensusPendingDoctorSeal,
};
pub use policy::{CensusRateDeclarationError, census_provider_rate_declaration};
pub use query::{
    CENSUS_APPLICATION_REQUESTS_PER_DAY, CENSUS_APPLICATION_REQUESTS_PER_SECOND,
    CENSUS_PROVIDER_VARIABLE_LIMIT, CensusApiKey, CensusApplicationPacing, CensusDataQuery,
    CensusDataset, CensusDatasetVintage, CensusDiscoveryKind, CensusDiscoveryRequest,
    CensusGeography, CensusGeographyClause, CensusGeographyCode, CensusPredicate, CensusSelection,
    CensusTimePoint, CensusTimePredicate, CensusUcgid,
};
pub use response::{
    CENSUS_OPERATION_MEMORY_LIMIT_BYTES, CensusAnnotation, CensusClocks, CensusCompleteness,
    CensusCompletenessIssue, CensusDataPage, CensusGeographyScope, CensusGeographyValue,
    CensusMissingReason, CensusObservation, CensusPagination, CensusParseLimits,
    CensusPredicateValue, CensusReportedTime, CensusResponseAccounting, CensusRevisionCandidate,
    CensusTypedValue, CensusValueState,
};
pub use runtime::{
    CENSUS_PROVIDER_SEMANTICS_SCHEMA, CensusActivatedDataset, CensusActivationCandidate,
    CensusActivationPlan, CensusCanonicalObservationBinding, CensusCaptureBinding,
    CensusCaptureRole, CensusPublicationCandidate, CensusPublicationPlan,
};
pub use source::{
    CensusAnnotatedMissingRule, CensusAnnotationMatch, CensusCapturedData, CensusCapturedDiscovery,
    CensusDatasetAcquisition, CensusDatasetContract, CensusDiscoveryOutput,
    CensusEffectiveTimePolicy, CensusMetadataBundle, CensusPendingDiscovery,
    CensusSealedDiscoveryAdmission, CensusSealedExtractionOutput, CensusSource, CensusSourceConfig,
    CensusSourceError, CensusSourceTelemetry, CensusVariableMapping,
    MAX_CENSUS_ANNOTATED_MISSING_RULES, MAX_CENSUS_ANNOTATION_RULE_BYTES,
    MAX_CENSUS_ANNOTATIONS_PER_RULE, MAX_CENSUS_CONFIGURED_DATASETS, census_api_endpoint_rules,
};

/// A Census contract, request, metadata, or response failure.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CensusAdapterError {
    /// A required value was empty, too long, or contained unsupported characters.
    #[error("invalid Census identifier or query component")]
    InvalidComponent,
    /// The data vintage was outside the supported four-digit route grammar.
    #[error("invalid Census dataset vintage")]
    InvalidVintage,
    /// The API key was empty, too large, or contained a control character.
    #[error("invalid Census API key")]
    InvalidApiKey,
    /// The request violated the provider query grammar.
    #[error("invalid Census query")]
    InvalidQuery,
    /// An ordinary `get` request exceeded Census's verified 50-variable maximum.
    #[error("Census get-variable limit exceeded")]
    VariableLimitExceeded,
    /// The input body exceeded an application-owned byte bound.
    #[error("Census response body exceeded the configured byte bound")]
    BodyTooLarge,
    /// Parsed input exceeded an application-owned row, column, cell, entry, or string bound.
    #[error("Census response exceeded a configured structural bound")]
    ResourceLimitExceeded,
    /// JSON syntax was invalid.
    #[error("invalid Census JSON")]
    InvalidJson,
    /// A discovery document or response matrix did not have the required provider shape.
    #[error("Census provider schema drift")]
    SchemaDrift,
    /// A provider identity appeared more than once where uniqueness is required.
    #[error("duplicate Census provider identity")]
    DuplicateIdentity,
    /// The supplied metadata does not describe the requested dataset or response field.
    #[error("Census metadata does not close the requested response")]
    MetadataMismatch,
    /// Receipt and ingestion chronology was invalid.
    #[error("invalid Census local chronology")]
    InvalidChronology,
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    Sha256::digest(bytes).into()
}

pub(crate) fn update_digest_component(hasher: &mut sha2::Sha256, bytes: &[u8]) {
    use sha2::Digest;

    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
