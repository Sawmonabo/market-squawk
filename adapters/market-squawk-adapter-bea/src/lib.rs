//! Bounded, metadata-driven Bureau of Economic Analysis API adapter core.
//!
//! It builds the five documented BEA request forms, borrows the operator's `UserID` only while
//! constructing an authenticated URL, performs registry-authorized bounded HTTPS acquisition, and
//! decodes metadata and data responses into closed source-native types. Rich acquisition methods
//! retain source-neutral raw capture material for the application's sole `MSJ1` sealing boundary;
//! typed doctor, actual-seal rejoin, and canonical-candidate bindings stop at the shared
//! publication boundary. Durable revision, manifest, restart, PIT, and product-query authority
//! remain solely in root composition.

mod auth;
mod binding;
mod canonical;
mod doctor;
mod error;
mod model;
mod pacing;
mod parser;
mod publication;
mod query;
mod quota;
mod revision;
mod sealed;
mod source;
mod transport;

pub use auth::{BeaAuthorizedRequest, BeaUserId};
pub use binding::{BeaSourceBinding, BeaSourceBindingError};
pub use canonical::{BeaCanonicalError, BeaCanonicalObservation};
pub use doctor::{
    BEA_DOCTOR_ADMISSION_VALIDITY_NANOS, BeaDoctorAdmissionEvidence, BeaDoctorError,
    BeaDoctorPageEvidence, BeaDoctorReceipt, BeaDoctorRun,
};
pub use error::{BeaError, BeaProviderError};
pub use model::{
    BEA_REGIONAL_SUPPRESSION_MARKER, BEA_REGIONAL_SUPPRESSION_REASON, BeaCompleteness, BeaDataPage,
    BeaDataType, BeaDatasetDefinition, BeaDatasetIdentity, BeaDimension, BeaFrequency,
    BeaMetadataGeneration, BeaMetadataPage, BeaMetadataRecords, BeaMissingValue, BeaNote,
    BeaObservation, BeaObservationIdentity, BeaObservationValue, BeaPageReceipt,
    BeaParameterDataType, BeaParameterDefinition, BeaParameterIdentity,
    BeaParameterValueDefinition, BeaProductionTime, BeaTimePeriod, BeaUnit,
};
pub use pacing::{
    BEA_APPLICATION_ERRORS_PER_MINUTE, BEA_APPLICATION_MAX_IN_FLIGHT,
    BEA_APPLICATION_REQUESTS_PER_MINUTE, BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE,
    BEA_MINIMUM_REQUEST_INTERVAL, BEA_OFFICIAL_ERRORS_PER_MINUTE, BEA_OFFICIAL_REQUESTS_PER_MINUTE,
    BEA_OFFICIAL_RESPONSE_BYTES_PER_MINUTE, BeaPacingPolicy, BeaWindowBudget,
};
pub use parser::{BeaParseLimits, parse_data_page, parse_metadata_page};
pub use publication::{
    BeaPublicationCandidate, BeaPublicationError, BeaPublicationRejoinCoordinates,
    BeaSharedPublicationParts,
};
pub use query::{
    BEA_API_ENDPOINT, BEA_MAX_APPLICATION_PAGES, BEA_MAX_APPLICATION_ROWS_PER_PAGE, BeaMethod,
    BeaPageScope, BeaQuery, BeaRequest,
};
pub use quota::{
    BeaProviderQuotaDeclaration, BeaQuotaDeclarationError, BeaQuotaWindowDeclaration,
    BeaRequiredSharedSettlement, bea_provider_quota_declaration,
};
pub use revision::{
    BeaCorrectionLedgerInput, BeaCorrectionNotice, BeaObservedVersion, BeaRevisionKind,
};
pub use sealed::{
    BeaPendingDiscoverySeal, BeaSealedAcquisitionError, BeaSealedAcquisitionReceipt,
    BeaSealedDiscoveryAdmission,
};
pub use source::{
    BEA_NATIVE_EXTRACTION_SCHEMA, BeaCapturedDataPage, BeaCapturedDiscovery,
    BeaCapturedMetadataPage, BeaDataEvidencePage, BeaDatasetAcquisition, BeaDatasetContract,
    BeaDatasetEvidence, BeaMetadataBundle, BeaMetadataEvidenceBundle, BeaMetadataEvidencePage,
    BeaResponseTelemetry, BeaSource, BeaSourceConfig, BeaSourceError, BeaSourceTelemetry,
    MAX_BEA_CONFIGURED_DATASETS, bea_api_endpoint_rule, bea_provider_rate_declaration,
};

#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod tests;
