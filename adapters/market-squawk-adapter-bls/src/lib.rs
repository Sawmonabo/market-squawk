//! Bounded BLS extraction for authorized unregistered v1 and user-supplied v2 credentials.

mod chunks;
mod client;
mod contract;
mod discovery;
mod doctor;
mod observations;
mod publication;
mod series_metadata;
mod source;

pub use chunks::{BlsAccessTier, BlsChunkError, BlsRequestChunk, BlsRequestLimits, BlsRequestPlan};
pub use client::{BlsAuthorization, BlsCredentialRejoin, BlsRegistrationKey, BlsSourceError};
#[cfg(all(feature = "scripted-transport-fixture", debug_assertions))]
pub use client::{BlsScriptedResponse, BlsScriptedTransportCounters, BlsScriptedTransportFactory};
pub use contract::{
    BLS_DOCTOR_ACTIVATION_TTL_NANOS, BlsActivationCandidate, BlsActivationPlan,
    BlsProviderRateDeclaration, bls_application_provider_budget, bls_provider_rate_declaration,
};
pub use discovery::{
    BlsDiscoveryAdmission, BlsDiscoveryObjectAdmission, BlsDiscoveryOutput, BlsPendingDiscovery,
};
pub use doctor::{BlsDoctorOutput, BlsDoctorReadiness, BlsDoctorReport};
pub use observations::{
    BlsFootnote, BlsObservation, BlsParseError, BlsResponse, BlsSeries, BlsVintageCapability,
};
pub use publication::{
    BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION, BlsCanonicalFootnote,
    BlsCanonicalObservationSemantics, BlsCanonicalProviderSemantics, BlsCanonicalSeriesManifest,
    BlsCompletePublicationPlanHandoff, BlsPublicationCandidate, BlsRootSchemaExtensionRequirement,
    BlsTimeseriesNativeLineageObservationV1, BlsTimeseriesNativeLineageRowV1,
    BlsTimeseriesNativeLineageSeriesV1,
};
pub use series_metadata::{BlsSeriesMetadata, BlsSeriesMetadataInput};
pub use source::{
    BlsExtractionOutput, BlsNormalizedPage, BlsSource, BlsSourceConfig, BlsSourceHealth,
};
