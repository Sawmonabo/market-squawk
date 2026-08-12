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
mod metadata;
mod request;
mod transport;
mod types;
mod wire;

pub use canonical::{EiaCanonicalContext, EiaCanonicalObservation};
pub use capacity::{
    EIA_APPLICATION_MAX_CONCURRENT_REQUESTS, EIA_APPLICATION_MIN_REQUEST_INTERVAL,
    EiaApplicationBudget, EiaCapacityGuidance, EiaEvidenceClass, eia_application_provider_budget,
};
pub use data::{
    EiaAcquisition, EiaAcquisitionReceipt, EiaClockField, EiaClockKind, EiaDataFieldContract,
    EiaDataFieldContractInput, EiaDataPage, EiaDataPageReceipt, EiaDatasetContract,
    EiaDatasetContractInput, EiaDescriptor, EiaFacetCoordinate, EiaMissingPolicy,
    EiaNativeMissingValue, EiaNativeValue, EiaObservation, EiaObservationClocks,
    EiaObservationConflict, EiaObservationFamily, EiaPageCompleteness, EiaPaginationTracker,
    EiaPeriod, EiaPeriodKind, EiaRevisionDisposition, EiaRevisionHead, EiaRevisionPlanEntry,
    EiaSeriesIdentity, EiaUnitSource, EiaValueKind, plan_revisions,
};
pub use error::EiaError;
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
    EiaDataPageMaterial, EiaDataRetrieval, EiaDataTransportReceipt, EiaFacetMetadataRetrieval,
    EiaHttpReceipt, EiaRawPageMaterial, EiaRouteMetadataRetrieval, EiaSourceTransport,
    EiaSourceTransportError, EiaTransportLimits, eia_api_endpoint_rules,
    eia_data_dataset_identifier,
};
pub use types::{
    EIA_API_ROOT, EIA_MAX_JSON_PAGE_ROWS, EiaApiVersion, EiaDigest, EiaFacetValue, EiaFieldId,
    EiaParseLimits, EiaRoute,
};

#[cfg(test)]
mod tests;
