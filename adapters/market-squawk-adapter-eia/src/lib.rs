//! Bounded, revision-preserving EIA API v2 energy-data extraction contracts.
//!
//! This crate owns the hardened EIA HTTP boundary but no scheduler, credential store, or durable
//! publication authority. It builds exact authenticated requests, redacts EIA's echoed API key,
//! validates provider-native metadata and data pages, and produces capture-first typed integration
//! inputs for the shared Market Squawk rate, research, and point-in-time authorities.

mod canonical;
mod capacity;
mod data;
mod error;
mod lifecycle;
mod metadata;
mod request;
mod transport;
mod types;
mod wire;

pub use canonical::{
    EiaCanonicalObservation, EiaNativePublishedSeriesCoordinate, EiaNativePublishedSeriesPrecision,
    EiaPublicationCandidate, EiaPublicationPolicyEvidence, EiaPublicationRejoin,
    EiaPublishedSeries, EiaSharedPublicationParts, decode_eia_native_published_series_coordinate,
};
pub use capacity::{
    EIA_APPLICATION_MAX_CONCURRENT_REQUESTS, EIA_APPLICATION_MIN_REQUEST_INTERVAL,
    EiaApplicationBudget, EiaCapacityGuidance, EiaEvidenceClass, eia_application_provider_budget,
};
pub use data::{
    EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS, EiaAcquisition, EiaAcquisitionReceipt,
    EiaClockField, EiaClockKind, EiaDataFieldContract, EiaDataFieldContractInput, EiaDataPage,
    EiaDataPageReceipt, EiaDatasetContract, EiaDatasetContractInput, EiaDescriptor,
    EiaFacetCoordinate, EiaMissingPolicy, EiaNativeMissingValue, EiaNativeValue, EiaObservation,
    EiaObservationClocks, EiaObservationFamily, EiaPageCompleteness, EiaPaginationTracker,
    EiaPeriod, EiaPeriodKind, EiaSeriesIdentity, EiaUnitSource, EiaValueKind,
};
pub use error::EiaError;
pub use lifecycle::{
    EiaActivatedProvider, EiaActivationRequirements, EiaDatasetProfile, EiaDoctorOutput,
    EiaDoctorReport, EiaLifecycleError, EiaPendingActivation, EiaPublicationMode, run_eia_doctor,
};
pub use metadata::{
    EiaChildRoute, EiaDataColumnMetadata, EiaFacetCatalog, EiaFacetMetadata,
    EiaFacetMetadataReceipt, EiaFacetMetadataValue, EiaFrequencyMetadata, EiaMetadataChange,
    EiaMetadataReceipt, EiaRouteMetadata, compare_route_metadata, parse_facet_metadata,
    parse_route_metadata,
};
pub use request::{
    EiaApiKey, EiaAuthenticatedRequest, EiaDataPageRequest, EiaDataQuery, EiaDataQueryInput,
    EiaFacetFilter, EiaMetadataRequest, EiaMetadataRequestKind, EiaSort, EiaSortDirection,
};
pub use transport::{
    EiaDataAcquisitionCursor, EiaDataPageMaterial, EiaDataPageSealRejoin, EiaDataPageTransition,
    EiaDataProbeRetrieval, EiaDataRetrieval, EiaDataRetrievalSealRejoin, EiaDataTransportReceipt,
    EiaFacetMetadataRetrieval, EiaHttpFailureClass, EiaHttpFailureReceipt, EiaHttpReceipt,
    EiaPendingDataPage, EiaRawPageMaterial, EiaRetryAfterPresence, EiaRootPageJournalRejoin,
    EiaRouteMetadataRetrieval, EiaSourceTransport, EiaSourceTransportError, EiaTransportLimits,
    eia_api_endpoint_rules, eia_data_dataset_identifier,
};
pub use types::{
    EIA_API_ROOT, EIA_MAX_JSON_PAGE_ROWS, EiaApiVersion, EiaDigest, EiaFacetValue, EiaFieldId,
    EiaParseLimits, EiaRoute,
};

#[cfg(test)]
mod tests;
