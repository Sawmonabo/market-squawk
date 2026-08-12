//! Bounded, metadata-driven Bureau of Economic Analysis API adapter core.
//!
//! It builds the five documented BEA request forms, borrows the operator's `UserID` only while
//! constructing an authenticated URL, performs registry-authorized bounded HTTPS acquisition, and
//! decodes metadata and data responses into closed source-native types. Rich acquisition methods
//! retain source-neutral raw capture material for the application's sole `MSJ1` sealing boundary;
//! this adapter does not claim canonical publication or point-in-time selection authority.

mod auth;
mod error;
mod model;
mod pacing;
mod parser;
mod query;
mod revision;
mod source;
mod transport;

pub use auth::{BeaAuthorizedRequest, BeaUserId};
pub use error::{BeaError, BeaProviderError};
pub use model::{
    BeaCompleteness, BeaDataPage, BeaDataType, BeaDatasetDefinition, BeaDatasetIdentity,
    BeaDimension, BeaFrequency, BeaMetadataGeneration, BeaMetadataPage, BeaMetadataRecords,
    BeaMissingValue, BeaNote, BeaObservation, BeaObservationIdentity, BeaObservationValue,
    BeaPageReceipt, BeaParameterDataType, BeaParameterDefinition, BeaParameterIdentity,
    BeaParameterValueDefinition, BeaProductionTime, BeaTimePeriod, BeaUnit,
};
pub use pacing::{
    BEA_APPLICATION_ERRORS_PER_MINUTE, BEA_APPLICATION_MAX_IN_FLIGHT,
    BEA_APPLICATION_REQUESTS_PER_MINUTE, BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE,
    BEA_MINIMUM_REQUEST_INTERVAL, BEA_OFFICIAL_ERRORS_PER_MINUTE, BEA_OFFICIAL_REQUESTS_PER_MINUTE,
    BEA_OFFICIAL_RESPONSE_BYTES_PER_MINUTE, BeaPacingPolicy, BeaWindowBudget,
};
pub use parser::{BeaParseLimits, parse_data_page, parse_metadata_page};
pub use query::{
    BEA_API_ENDPOINT, BEA_MAX_APPLICATION_PAGES, BEA_MAX_APPLICATION_ROWS_PER_PAGE, BeaMethod,
    BeaPageScope, BeaQuery, BeaRequest,
};
pub use revision::{
    BeaCorrectionLedgerInput, BeaCorrectionNotice, BeaObservedVersion, BeaRevisionKind,
};
pub use source::{
    BEA_NATIVE_EXTRACTION_SCHEMA, BeaCapturedDataPage, BeaCapturedDiscovery, BeaCapturedExtraction,
    BeaCapturedMetadataPage, BeaDatasetAcquisition, BeaDatasetContract, BeaMetadataBundle,
    BeaResponseTelemetry, BeaSource, BeaSourceConfig, BeaSourceError, BeaSourceTelemetry,
    MAX_BEA_CONFIGURED_DATASETS, bea_api_endpoint_rule, bea_provider_rate_declaration,
};

#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod tests;
