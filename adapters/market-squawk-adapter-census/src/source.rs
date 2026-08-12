use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MacroMissingValue, MacroObservation,
    PayloadHash, PayloadReference, ResearchContext, ResearchObservation, ResearchPeriod,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionNumber, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationMode, CURRENT_RESEARCH_RECORD_SCHEMA, CoverageDomain,
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionAuthorityError,
    ExtractionBatch, ExtractionRecord, ExtractionRequest, ExtractionRequestPermit,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError, HistoricalCapability,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, NetworkAccessPolicy, NetworkPolicyError, PathScope,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, QueryParameterRule, QuerySensitivity, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject, SourceProtocolProfile,
    payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::http::{
    CensusHttpRequest, CensusHttpResponse, CensusTransport, ReqwestCensusTransport,
    system_timestamp,
};
use crate::{
    CENSUS_APPLICATION_REQUESTS_PER_DAY, CENSUS_APPLICATION_REQUESTS_PER_SECOND,
    CensusAdapterError, CensusApiKey, CensusAuthorizedUrl, CensusClocks, CensusDataPage,
    CensusDataQuery, CensusDatasetVintage, CensusDiscoveryDocument, CensusDiscoveryKind,
    CensusDiscoveryRequest, CensusGeography, CensusMetadataEvidence, CensusMissingReason,
    CensusParseLimits, CensusPredicateType, CensusSelection, CensusTypedValue, CensusValueState,
    CensusVariableCatalog,
};

/// Maximum exact Census query contracts retained by one source instance.
pub const MAX_CENSUS_CONFIGURED_DATASETS: usize = 64;
const CENSUS_JSON_MEDIA_TYPE: &str = "application/json";
const CENSUS_DATASET_ID_PREFIX: &str = "census:data-v1:";
const CENSUS_ANALYTICAL_ID_PREFIX: &str = "census.data-v1.";
const ONE_SECOND_NANOS: u64 = 1_000_000_000;
const ONE_DAY_NANOS: u64 = 86_400_000_000_000;

/// One explicit provider-variable to canonical macro-series mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusVariableMapping {
    provider_variable: SourceIdentifier,
    series_namespace: SourceIdentifier,
    unit: SourceIdentifier,
}

impl CensusVariableMapping {
    /// Constructs a numeric macro mapping whose final geography/predicate-scoped series identity
    /// remains representable without truncation.
    pub fn try_new(
        provider_variable: SourceIdentifier,
        series_namespace: SourceIdentifier,
        unit: SourceIdentifier,
    ) -> Result<Self, CensusSourceError> {
        let maximum = series_namespace
            .as_str()
            .len()
            .checked_add(7 + 64)
            .ok_or(CensusSourceError::InvalidConfiguration)?;
        if maximum > SourceIdentifier::MAX_LENGTH {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(Self {
            provider_variable,
            series_namespace,
            unit,
        })
    }

    /// Returns the exact Census variable identity.
    pub const fn provider_variable(&self) -> &SourceIdentifier {
        &self.provider_variable
    }

    /// Returns the base canonical series namespace; geography and predicates are appended by a
    /// stable digest during normalization.
    pub const fn series_namespace(&self) -> &SourceIdentifier {
        &self.series_namespace
    }

    /// Returns the reviewed source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
}

/// Exact rule for obtaining a canonical effective coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CensusEffectiveTimePolicy {
    /// Every response row must carry a supported `time` value.
    RequireReportedTime,
    /// Rows must not carry `time`; this reviewed fixed coordinate supplies the dataset meaning.
    Fixed(ResearchTemporalCoordinate),
}

/// One immutable metadata-first Census query contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusDatasetContract {
    dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    query: CensusDataQuery,
    mappings: BTreeMap<SourceIdentifier, CensusVariableMapping>,
    effective_time: CensusEffectiveTimePolicy,
    metadata_requests: Vec<CensusDiscoveryRequest>,
}

impl CensusDatasetContract {
    /// Binds one exact provider query to explicit numeric macro mappings and time semantics.
    pub fn try_new(
        query: CensusDataQuery,
        mappings: impl IntoIterator<Item = CensusVariableMapping>,
        effective_time: CensusEffectiveTimePolicy,
    ) -> Result<Self, CensusSourceError> {
        let mut accepted = BTreeMap::new();
        for mapping in mappings {
            if accepted
                .insert(mapping.provider_variable.clone(), mapping)
                .is_some()
            {
                return Err(CensusSourceError::InvalidConfiguration);
            }
        }
        if accepted.is_empty() {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        if let CensusSelection::Variables { .. } = query.selection() {
            let expected = query
                .selection()
                .primary_variables()
                .iter()
                .collect::<BTreeSet<_>>();
            let actual = accepted.keys().collect::<BTreeSet<_>>();
            if actual != expected {
                return Err(CensusSourceError::InvalidConfiguration);
            }
        }
        let query_hex = lower_hex(query.request_digest());
        let dataset_id =
            SourceIdentifier::try_from(format!("{CENSUS_DATASET_ID_PREFIX}{query_hex}"))
                .map_err(|_| CensusSourceError::InvalidConfiguration)?;
        let analytical_dataset_id =
            SourceIdentifier::try_from(format!("{CENSUS_ANALYTICAL_ID_PREFIX}{query_hex}"))
                .map_err(|_| CensusSourceError::InvalidConfiguration)?;
        let dataset = query.dataset().clone();
        let catalog = match dataset.vintage() {
            CensusDatasetVintage::Year(vintage) => {
                CensusDiscoveryRequest::try_new(CensusDiscoveryKind::VintageDatasets { vintage })?
            }
            CensusDatasetVintage::TimeSeries => {
                CensusDiscoveryRequest::try_new(CensusDiscoveryKind::Datasets)?
            }
        };
        let mut metadata_requests = vec![
            catalog,
            CensusDiscoveryRequest::try_new(CensusDiscoveryKind::Groups {
                dataset: dataset.clone(),
            })?,
            CensusDiscoveryRequest::try_new(CensusDiscoveryKind::Variables {
                dataset: dataset.clone(),
            })?,
        ];
        if let Some(group) = query.selection().group_id() {
            metadata_requests.push(CensusDiscoveryRequest::group(
                dataset.clone(),
                group.as_str(),
            )?);
        }
        metadata_requests.push(CensusDiscoveryRequest::try_new(
            CensusDiscoveryKind::Geographies { dataset },
        )?);
        Ok(Self {
            dataset_id,
            analytical_dataset_id,
            query,
            mappings: accepted,
            effective_time,
            metadata_requests,
        })
    }

    /// Returns the exact provider-query dataset identity used by extraction requests.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the storage-safe analytical dataset identity.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }

    /// Returns the exact key-free data query.
    pub const fn query(&self) -> &CensusDataQuery {
        &self.query
    }

    /// Returns mappings in stable provider-variable order.
    pub fn mappings(&self) -> impl ExactSizeIterator<Item = &CensusVariableMapping> {
        self.mappings.values()
    }

    /// Returns the fixed metadata request sequence required before data decoding.
    pub fn metadata_requests(&self) -> &[CensusDiscoveryRequest] {
        &self.metadata_requests
    }

    fn mapping(&self, variable: &SourceIdentifier) -> Option<&CensusVariableMapping> {
        self.mappings.get(variable)
    }
}

/// Bounded immutable set of admitted Census query contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusSourceConfig {
    contracts: Vec<CensusDatasetContract>,
    parse_limits: CensusParseLimits,
}

impl CensusSourceConfig {
    /// Constructs a nonempty, duplicate-free source configuration.
    pub fn try_new(
        contracts: impl IntoIterator<Item = CensusDatasetContract>,
        parse_limits: CensusParseLimits,
    ) -> Result<Self, CensusSourceError> {
        let mut contracts = contracts.into_iter().collect::<Vec<_>>();
        if contracts.is_empty() || contracts.len() > MAX_CENSUS_CONFIGURED_DATASETS {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        contracts.sort_by(|left, right| left.dataset_id.cmp(&right.dataset_id));
        if contracts
            .windows(2)
            .any(|pair| pair[0].dataset_id == pair[1].dataset_id)
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(Self {
            contracts,
            parse_limits,
        })
    }

    /// Returns admitted contracts in stable dataset order.
    pub fn contracts(&self) -> &[CensusDatasetContract] {
        &self.contracts
    }

    /// Returns parser memory and structure limits.
    pub const fn parse_limits(&self) -> CensusParseLimits {
        self.parse_limits
    }

    fn contract(&self, dataset: &SourceIdentifier) -> Option<&CensusDatasetContract> {
        self.contracts
            .binary_search_by(|contract| contract.dataset_id.cmp(dataset))
            .ok()
            .map(|index| &self.contracts[index])
    }
}

/// Builds the two structural API rules needed by a source config.
///
/// The first rule covers `/data.json`; the second covers `/data/...` metadata and data paths.
/// Exact configured targets are still checked by [`CensusSource`] at construction and before send.
pub fn census_api_endpoint_rules(
    config: &CensusSourceConfig,
) -> Result<Vec<ApiEndpointRule>, NetworkPolicyError> {
    let key = query_rule("key", 512, false, QuerySensitivity::Secret)?;
    let root = ApiEndpointRule::try_new(
        "https://api.census.gov/data.json",
        PathScope::Exact,
        vec![key.clone()],
        1,
        1_024,
    )?;
    let mut rules = vec![
        key,
        query_rule("get", 8_192, false, QuerySensitivity::Public)?,
        query_rule("descriptive", 5, false, QuerySensitivity::Public)?,
        query_rule("outputFormat", 16, false, QuerySensitivity::Public)?,
        query_rule("for", 8_192, false, QuerySensitivity::Public)?,
        query_rule("in", 8_192, false, QuerySensitivity::Public)?,
        query_rule("ucgid", 8_192, false, QuerySensitivity::Public)?,
        query_rule("time", 512, false, QuerySensitivity::Public)?,
    ];
    let mut predicate_names = BTreeSet::new();
    for contract in config.contracts() {
        for predicate in contract.query().predicates() {
            predicate_names.insert(predicate.variable().clone());
        }
    }
    for name in predicate_names {
        rules.push(QueryParameterRule::try_new(
            name,
            512,
            true,
            QuerySensitivity::Public,
        )?);
    }
    let descendants = ApiEndpointRule::try_new(
        "https://api.census.gov/data",
        PathScope::Descendants,
        rules,
        64,
        16_384,
    )?;
    Ok(vec![root, descendants])
}

fn query_rule(
    key: &str,
    max: u16,
    multiple: bool,
    sensitivity: QuerySensitivity,
) -> Result<QueryParameterRule, NetworkPolicyError> {
    QueryParameterRule::try_new(
        SourceIdentifier::try_from(key).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        max,
        multiple,
        sensitivity,
    )
}

/// Cumulative or operation-local Census transport accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct CensusSourceTelemetry {
    requests: u64,
    successful_responses: u64,
    rate_limited_responses: u64,
    response_bytes: u64,
    latency_nanos: u64,
    metadata_entries: u64,
    requested_variables: u64,
    returned_variables: u64,
    missing_variables: u64,
    returned_rows: u64,
    usable_rows: u64,
    observations: u64,
    partial_responses: u64,
    failures: u64,
}

macro_rules! telemetry_getter {
    ($name:ident) => {
        #[doc = concat!("Returns exact accumulated `", stringify!($name), "` accounting.")]
        pub const fn $name(self) -> u64 {
            self.$name
        }
    };
}

impl CensusSourceTelemetry {
    telemetry_getter!(requests);
    telemetry_getter!(successful_responses);
    telemetry_getter!(rate_limited_responses);
    telemetry_getter!(response_bytes);
    telemetry_getter!(latency_nanos);
    telemetry_getter!(metadata_entries);
    telemetry_getter!(requested_variables);
    telemetry_getter!(returned_variables);
    telemetry_getter!(missing_variables);
    telemetry_getter!(returned_rows);
    telemetry_getter!(usable_rows);
    telemetry_getter!(observations);
    telemetry_getter!(partial_responses);
    telemetry_getter!(failures);

    fn checked_add(self, other: Self) -> Result<Self, CensusSourceError> {
        macro_rules! add {
            ($field:ident) => {
                self.$field
                    .checked_add(other.$field)
                    .ok_or(CensusSourceError::TelemetryOverflow)?
            };
        }
        Ok(Self {
            requests: add!(requests),
            successful_responses: add!(successful_responses),
            rate_limited_responses: add!(rate_limited_responses),
            response_bytes: add!(response_bytes),
            latency_nanos: add!(latency_nanos),
            metadata_entries: add!(metadata_entries),
            requested_variables: add!(requested_variables),
            returned_variables: add!(returned_variables),
            missing_variables: add!(missing_variables),
            returned_rows: add!(returned_rows),
            usable_rows: add!(usable_rows),
            observations: add!(observations),
            partial_responses: add!(partial_responses),
            failures: add!(failures),
        })
    }
}

#[derive(Debug, Default)]
struct CensusTelemetryState {
    requests: AtomicU64,
    successful_responses: AtomicU64,
    rate_limited_responses: AtomicU64,
    response_bytes: AtomicU64,
    latency_nanos: AtomicU64,
    metadata_entries: AtomicU64,
    requested_variables: AtomicU64,
    returned_variables: AtomicU64,
    missing_variables: AtomicU64,
    returned_rows: AtomicU64,
    usable_rows: AtomicU64,
    observations: AtomicU64,
    partial_responses: AtomicU64,
    failures: AtomicU64,
}

impl CensusTelemetryState {
    fn snapshot(&self) -> CensusSourceTelemetry {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        CensusSourceTelemetry {
            requests: load(&self.requests),
            successful_responses: load(&self.successful_responses),
            rate_limited_responses: load(&self.rate_limited_responses),
            response_bytes: load(&self.response_bytes),
            latency_nanos: load(&self.latency_nanos),
            metadata_entries: load(&self.metadata_entries),
            requested_variables: load(&self.requested_variables),
            returned_variables: load(&self.returned_variables),
            missing_variables: load(&self.missing_variables),
            returned_rows: load(&self.returned_rows),
            usable_rows: load(&self.usable_rows),
            observations: load(&self.observations),
            partial_responses: load(&self.partial_responses),
            failures: load(&self.failures),
        }
    }

    fn add(&self, telemetry: CensusSourceTelemetry) {
        macro_rules! add {
            ($field:ident) => {
                let _ = self
                    .$field
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                        Some(value.saturating_add(telemetry.$field))
                    });
            };
        }
        add!(requests);
        add!(successful_responses);
        add!(rate_limited_responses);
        add!(response_bytes);
        add!(latency_nanos);
        add!(metadata_entries);
        add!(requested_variables);
        add!(returned_variables);
        add!(missing_variables);
        add!(returned_rows);
        add!(usable_rows);
        add!(observations);
        add!(partial_responses);
        add!(failures);
    }
}

/// A validated metadata response retaining exact bounded bytes and a provider capture receipt.
#[derive(Clone, Debug)]
pub struct CensusCapturedDiscovery {
    request: CensusDiscoveryRequest,
    body: Bytes,
    document: CensusDiscoveryDocument,
    capture: ProviderCaptureSetReceipt,
    latency: Duration,
}

impl CensusCapturedDiscovery {
    pub const fn request(&self) -> &CensusDiscoveryRequest {
        &self.request
    }
    pub const fn body(&self) -> &Bytes {
        &self.body
    }
    pub const fn document(&self) -> &CensusDiscoveryDocument {
        &self.document
    }
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    pub const fn latency(&self) -> Duration {
        self.latency
    }
}

/// Complete metadata evidence required to interpret one configured Census data request.
#[derive(Clone, Debug)]
pub struct CensusMetadataBundle {
    dataset_id: SourceIdentifier,
    query_digest: [u8; 32],
    documents: Vec<CensusCapturedDiscovery>,
    content_digest: [u8; 32],
    telemetry: CensusSourceTelemetry,
}

impl CensusMetadataBundle {
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }
    pub fn documents(&self) -> &[CensusCapturedDiscovery] {
        &self.documents
    }
    pub const fn content_digest(&self) -> [u8; 32] {
        self.content_digest
    }
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }

    fn selected_variables(&self) -> Result<&CensusVariableCatalog, CensusSourceError> {
        self.documents
            .iter()
            .find_map(|captured| match captured.document() {
                CensusDiscoveryDocument::GroupVariables(catalog) => Some(catalog),
                _ => None,
            })
            .or_else(|| {
                self.documents
                    .iter()
                    .find_map(|captured| match captured.document() {
                        CensusDiscoveryDocument::Variables(catalog) => Some(catalog),
                        _ => None,
                    })
            })
            .ok_or(CensusSourceError::Protocol)
    }
}

/// One validated Census data response with exact bounded material and capture accounting.
#[derive(Clone, Debug)]
pub struct CensusCapturedData {
    body: Bytes,
    page: CensusDataPage,
    capture: ProviderCaptureSetReceipt,
    telemetry: CensusSourceTelemetry,
}

impl CensusCapturedData {
    pub const fn body(&self) -> &Bytes {
        &self.body
    }
    pub const fn page(&self) -> &CensusDataPage {
        &self.page
    }
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }
}

/// One complete metadata-first Census acquisition.
#[derive(Clone, Debug)]
pub struct CensusDatasetAcquisition {
    metadata: CensusMetadataBundle,
    data: CensusCapturedData,
    telemetry: CensusSourceTelemetry,
}

impl CensusDatasetAcquisition {
    pub const fn metadata(&self) -> &CensusMetadataBundle {
        &self.metadata
    }
    pub const fn data(&self) -> &CensusCapturedData {
        &self.data
    }
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }
}

/// Indivisible canonical Census result and every exact response needed to seal its raw evidence.
///
/// Capture materials are ordered exactly as the contract's metadata requests, followed by the
/// data response. Application composition must seal every material before admitting `batch` to
/// canonical publication.
#[derive(Debug)]
pub struct CensusExtractionOutput {
    batch: ExtractionBatch,
    acquisition: CensusDatasetAcquisition,
    captures: Box<[ProviderCaptureMaterial]>,
    telemetry: CensusSourceTelemetry,
}

impl CensusExtractionOutput {
    /// Returns the canonical shared extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns the exact typed metadata and data response used to build the canonical batch.
    pub const fn acquisition(&self) -> &CensusDatasetAcquisition {
        &self.acquisition
    }

    /// Returns every source-neutral exact response material in dependency order.
    pub fn captures(&self) -> &[ProviderCaptureMaterial] {
        &self.captures
    }

    /// Returns metadata response materials in the configured discovery-request order.
    pub fn metadata_captures(&self) -> &[ProviderCaptureMaterial] {
        let metadata_count = self.captures.len().saturating_sub(1);
        &self.captures[..metadata_count]
    }

    /// Returns the final data-response material that directly backs the source object.
    pub fn data_capture(&self) -> Option<&ProviderCaptureMaterial> {
        self.captures.last()
    }

    /// Returns actual request, response, row, missingness, byte, and latency accounting.
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }

    /// Consumes the application handoff. Every capture must be sealed before publishing `batch`.
    pub fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        CensusDatasetAcquisition,
        Box<[ProviderCaptureMaterial]>,
        CensusSourceTelemetry,
    ) {
        (self.batch, self.acquisition, self.captures, self.telemetry)
    }
}

/// Census source configuration, protocol, transport, or normalization failure.
#[derive(Debug, Error)]
pub enum CensusSourceError {
    #[error("Census source metadata is incompatible with the configured profile")]
    InvalidMetadata,
    #[error("Census source configuration is invalid")]
    InvalidConfiguration,
    #[error("Census provider response violated its exact protocol")]
    Protocol,
    #[error("Census HTTP transport failed")]
    Network,
    #[error("Census response exceeded its effective byte limit")]
    BodyTooLarge,
    #[error("Census request deadline elapsed")]
    DeadlineExceeded,
    #[error("Census request was cancelled")]
    Cancelled,
    #[error("Census local clock is unavailable")]
    Clock,
    #[error("Census extraction authority became unavailable")]
    Authority,
    #[error("Census telemetry accounting overflowed")]
    TelemetryOverflow,
    #[error("Census adapter contract failed: {0}")]
    Adapter(#[from] CensusAdapterError),
    #[error("Census capture receipt failed: {0}")]
    Capture(#[from] market_squawk_sources::ProviderCaptureError),
    #[error("Census revision evidence failed: {0}")]
    Revision(#[from] market_squawk_sources::ObservedRevisionError),
}

/// Registry-authorized production Census source.
pub struct CensusSource {
    metadata: SourceMetadata,
    api_key: CensusApiKey,
    config: CensusSourceConfig,
    transport: Arc<dyn CensusTransport>,
    response_limit: usize,
    request_timeout: Duration,
    telemetry: CensusTelemetryState,
}

impl std::fmt::Debug for CensusSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CensusSource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("api_key", &"[REDACTED]")
            .field("configured_datasets", &self.config.contracts.len())
            .field("response_limit", &self.response_limit)
            .finish_non_exhaustive()
    }
}

impl CensusSource {
    /// Builds a production source. The supplied metadata budget must be registered by app
    /// composition against its product-wide [`market_squawk_sources::ProviderRateAuthority`]; this
    /// adapter never creates a private quota pool.
    pub fn try_new(
        metadata: SourceMetadata,
        api_key: CensusApiKey,
        config: CensusSourceConfig,
    ) -> Result<Self, CensusSourceError> {
        Self::validate_metadata(&metadata, &config)?;
        let bounds = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.request_bounds(),
            NetworkAccessPolicy::Denied => return Err(CensusSourceError::InvalidMetadata),
        };
        let transport = Arc::new(ReqwestCensusTransport::try_new(bounds)?);
        Self::try_new_inner(metadata, api_key, config, transport)
    }

    #[cfg(test)]
    fn try_new_with_transport(
        metadata: SourceMetadata,
        api_key: CensusApiKey,
        config: CensusSourceConfig,
        transport: Arc<dyn CensusTransport>,
    ) -> Result<Self, CensusSourceError> {
        Self::validate_metadata(&metadata, &config)?;
        Self::try_new_inner(metadata, api_key, config, transport)
    }

    fn try_new_inner(
        metadata: SourceMetadata,
        api_key: CensusApiKey,
        config: CensusSourceConfig,
        transport: Arc<dyn CensusTransport>,
    ) -> Result<Self, CensusSourceError> {
        let bounds = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.request_bounds(),
            NetworkAccessPolicy::Denied => return Err(CensusSourceError::InvalidMetadata),
        };
        let response_limit = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| CensusSourceError::InvalidMetadata)?
            .min(config.parse_limits.max_bytes())
            .min(
                usize::try_from(MAX_PROVIDER_CAPTURE_PAGE_BYTES)
                    .map_err(|_| CensusSourceError::InvalidMetadata)?,
            );
        if response_limit == 0 {
            return Err(CensusSourceError::InvalidMetadata);
        }
        Ok(Self {
            metadata,
            api_key,
            config,
            transport,
            response_limit,
            request_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            telemetry: CensusTelemetryState::default(),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &CensusSourceConfig,
    ) -> Result<(), CensusSourceError> {
        let budget = metadata
            .budget_policy()
            .ok_or(CensusSourceError::InvalidMetadata)?;
        let has_second_window = (0..budget.window_count())
            .filter_map(|index| budget.window(index))
            .any(|window| {
                window.requests_per_window() <= CENSUS_APPLICATION_REQUESTS_PER_SECOND
                    && window.window_nanos() >= ONE_SECOND_NANOS
            });
        let has_daily_window = (0..budget.window_count())
            .filter_map(|index| budget.window(index))
            .any(|window| {
                window.requests_per_window() <= CENSUS_APPLICATION_REQUESTS_PER_DAY
                    && window.window_nanos() >= ONE_DAY_NANOS
            });
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "us-census"
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || budget.max_concurrent() != 1
            || !has_second_window
            || !has_daily_window
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(CensusSourceError::InvalidMetadata);
        }
        for contract in config.contracts() {
            authorize_configured_target(metadata, contract.query().redacted_url(), true)?;
            for request in contract.metadata_requests() {
                authorize_configured_target(metadata, request.redacted_url(), true)?;
            }
        }
        Ok(())
    }

    /// Returns the exact configured profile.
    pub const fn config(&self) -> &CensusSourceConfig {
        &self.config
    }

    /// Returns a lock-free saturating snapshot of cumulative operational accounting.
    pub fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry.snapshot()
    }

    /// Returns the storage-safe analytical identity for an admitted provider dataset.
    pub fn analytical_dataset_identifier(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, CensusSourceError> {
        self.config
            .contract(provider_dataset)
            .map(|contract| contract.analytical_dataset_id().clone())
            .ok_or(CensusSourceError::InvalidConfiguration)
    }

    /// Acquires and validates the complete metadata sequence required by one data query.
    pub async fn acquire_metadata(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<CensusMetadataBundle, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        let mut documents = Vec::new();
        let mut telemetry = CensusSourceTelemetry::default();
        for request in contract.metadata_requests() {
            let response = self
                .fetch_authorized(
                    authority,
                    request
                        .authorize(&self.api_key)
                        .map_err(map_adapter_error)?,
                    request.request_digest(),
                    provider_dataset,
                    deadline,
                    cancellation.clone(),
                )
                .await?;
            let document = match CensusDiscoveryDocument::parse(
                request,
                &response.body,
                self.effective_parse_limits(),
            ) {
                Ok(document) => document,
                Err(error) => {
                    self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                    return Err(map_adapter_error(error));
                }
            };
            let metadata_entries = discovery_evidence(&document).returned_entries();
            let response_telemetry = response
                .telemetry_with_metadata(metadata_entries)
                .map_err(map_source_error)?;
            telemetry = telemetry
                .checked_add(response_telemetry)
                .map_err(map_source_error)?;
            self.telemetry.add(CensusSourceTelemetry {
                metadata_entries: checked_u64(metadata_entries).map_err(map_source_error)?,
                ..CensusSourceTelemetry::default()
            });
            documents.push(CensusCapturedDiscovery {
                request: request.clone(),
                body: response.body,
                document,
                capture: response.capture,
                latency: response.latency,
            });
        }
        if let Err(error) = validate_metadata_bundle(contract, &documents) {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(map_source_error(error));
        }
        let content_digest = metadata_bundle_digest(contract, &documents);
        let bundle = CensusMetadataBundle {
            dataset_id: contract.dataset_id().clone(),
            query_digest: contract.query().request_digest(),
            documents,
            content_digest,
            telemetry,
        };
        Ok(bundle)
    }

    /// Fetches and parses one data response against an exact previously acquired metadata bundle.
    /// The result may retain structured partial evidence; callers must require
    /// `page().completeness().is_complete()` before publication.
    pub async fn acquire_data(
        &self,
        authority: &ExtractionAuthority,
        metadata: &CensusMetadataBundle,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<CensusCapturedData, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(metadata.dataset_id())
            .ok_or_else(invalid_protocol)?;
        if metadata.query_digest != contract.query().request_digest() {
            return Err(invalid_protocol());
        }
        let response = self
            .fetch_authorized(
                authority,
                contract
                    .query()
                    .authorize(&self.api_key)
                    .map_err(map_adapter_error)?,
                contract.query().request_digest(),
                contract.dataset_id(),
                deadline,
                cancellation,
            )
            .await?;
        let decoded_at = system_timestamp().map_err(map_source_error)?;
        let ingested_at = system_timestamp().map_err(map_source_error)?;
        let clocks =
            CensusClocks::local_first_observed(response.received_at, decoded_at, ingested_at)
                .map_err(map_adapter_error)?;
        let page = match CensusDataPage::parse(
            contract.query(),
            metadata.selected_variables().map_err(map_source_error)?,
            &response.body,
            self.effective_parse_limits(),
            clocks,
        ) {
            Ok(page) => page,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_adapter_error(error));
            }
        };
        let accounting = page.accounting();
        let telemetry = response.telemetry_with_data(
            accounting.requested_wire_variables(),
            accounting.returned_requested_variables(),
            accounting.missing_requested_variables(),
            accounting.returned_rows(),
            accounting.usable_rows(),
            accounting.observations(),
            !page.completeness().is_complete(),
        )?;
        self.telemetry.add(CensusSourceTelemetry {
            requested_variables: checked_u64(accounting.requested_wire_variables())
                .map_err(map_source_error)?,
            returned_variables: checked_u64(accounting.returned_requested_variables())
                .map_err(map_source_error)?,
            missing_variables: checked_u64(accounting.missing_requested_variables())
                .map_err(map_source_error)?,
            returned_rows: checked_u64(accounting.returned_rows()).map_err(map_source_error)?,
            usable_rows: checked_u64(accounting.usable_rows()).map_err(map_source_error)?,
            observations: checked_u64(accounting.observations()).map_err(map_source_error)?,
            partial_responses: u64::from(!page.completeness().is_complete()),
            ..CensusSourceTelemetry::default()
        });
        Ok(CensusCapturedData {
            body: response.body,
            page,
            capture: response.capture,
            telemetry,
        })
    }

    /// Runs the complete metadata-first acquisition and fails closed on every partial data page.
    pub async fn acquire_dataset(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<CensusDatasetAcquisition, ExtractionSourceError> {
        let metadata = self
            .acquire_metadata(authority, provider_dataset, deadline, cancellation.clone())
            .await?;
        let data = self
            .acquire_data(authority, &metadata, deadline, cancellation)
            .await?;
        if !data.page().completeness().is_complete() {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(invalid_protocol());
        }
        let telemetry = metadata
            .telemetry()
            .checked_add(data.telemetry())
            .map_err(map_source_error)?;
        Ok(CensusDatasetAcquisition {
            metadata,
            data,
            telemetry,
        })
    }

    /// Builds locally observed revision evidence for Census responses, which do not supply a
    /// universal provider revision chronology.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, CensusSourceError> {
        if batch.request().object().source_id() != self.metadata.source_id()
            || batch.request().object().metadata_revision() != self.metadata.revision()
        {
            return Err(CensusSourceError::InvalidMetadata);
        }
        ExtractionRevisionPlan::locally_observed(batch.records().len()).map_err(Into::into)
    }

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some() || request.max_results() != 1 {
            return Err(invalid_protocol());
        }
        let contract = self
            .config
            .contract(request.dataset())
            .ok_or_else(invalid_protocol)?;
        let acquired = self
            .acquire_dataset(
                &authority,
                request.dataset(),
                request.deadline(),
                cancellation,
            )
            .await?;
        let object = source_object(&self.metadata, &request, contract, &acquired)?;
        DiscoveryBatch::try_new(&request, vec![object]).map_err(Into::into)
    }

    /// Produces canonical rows together with every exact metadata and data response required for
    /// raw `MSJ1` sealing. The ordinary [`ExtractionSource::extract`] path is deliberately closed
    /// because its return type cannot carry this required material.
    pub async fn extract_with_capture(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<CensusExtractionOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
        {
            return Err(invalid_protocol());
        }
        let contract = self
            .config
            .contract(request.object().dataset())
            .ok_or_else(invalid_protocol)?;
        let object_identity = ParsedObjectId::parse(request.object().object_id())?;
        if object_identity.query_digest != contract.query().request_digest() {
            return Err(invalid_protocol());
        }
        let acquired = self
            .acquire_dataset(
                &authority,
                request.object().dataset(),
                request.deadline(),
                cancellation,
            )
            .await?;
        verify_acquisition(&request, &object_identity, &acquired)?;
        extraction_output(&self.metadata, &request, contract, acquired)
    }

    async fn fetch_authorized(
        &self,
        authority: &ExtractionAuthority,
        authorized: CensusAuthorizedUrl,
        request_digest: [u8; 32],
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedResponse, ExtractionSourceError> {
        self.validate_authority(authority)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let target = secret_target(&authorized).map_err(map_source_error)?;
        let permit =
            acquire_request_permit(authority, target.as_str(), deadline, cancellation.clone())
                .await?;
        let in_flight = permit.authorize_send(target.as_str())?;
        drop(target);
        let now = system_timestamp().map_err(map_source_error)?;
        let timeout = remaining_timeout(deadline, now, self.request_timeout)?;
        let result = self
            .transport
            .execute(
                CensusHttpRequest { authorized },
                &in_flight,
                self.response_limit,
                timeout,
                cancellation,
            )
            .await;
        self.telemetry.requests.fetch_add(1, Ordering::Relaxed);
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_source_error(error));
            }
        };
        let response_bytes = match self.record_response_metrics(&response) {
            Ok(response_bytes) => response_bytes,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if let Err(error) = in_flight.validate_response_size(response_bytes) {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(error.into());
        }
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type.as_deref())
        {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(invalid_protocol());
        }
        match response.status {
            200 => {}
            401 | 403 => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(SourceError::Unauthorized.into());
            }
            429 | 503 => {
                self.telemetry
                    .rate_limited_responses
                    .fetch_add(1, Ordering::Relaxed);
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                let wait =
                    match in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0) {
                        Ok(wait) => wait,
                        Err(error) => return Err(error.into()),
                    };
                return Err(SourceError::BudgetWaitUntil { deadline: wait }.into());
            }
            _ => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(SourceError::ProviderUnavailable.into());
            }
        }
        if response.body.is_empty() {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(invalid_protocol());
        }
        let body_digest = evidence_digest(sha256(&response.body));
        let request_identity = evidence_digest(request_digest);
        let page = match ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            response.status,
            response_bytes,
            body_digest,
            response.received_at,
        ) {
            Ok(page) => page,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_source_error(CensusSourceError::Capture(error)));
            }
        };
        let capture = match ProviderCaptureSetReceipt::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            provider_dataset.clone(),
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        ) {
            Ok(capture) => capture,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_source_error(CensusSourceError::Capture(error)));
            }
        };
        in_flight.release();
        self.telemetry
            .successful_responses
            .fetch_add(1, Ordering::Relaxed);
        Ok(FetchedResponse {
            body: response.body,
            capture,
            received_at: response.received_at,
            latency: response.latency,
        })
    }

    fn record_response_metrics(
        &self,
        response: &CensusHttpResponse,
    ) -> Result<u64, ExtractionSourceError> {
        let response_bytes = u64::try_from(response.body.len()).map_err(|_| invalid_protocol())?;
        let latency_nanos = u64::try_from(response.latency.as_nanos())
            .map_err(|_| map_source_error(CensusSourceError::TelemetryOverflow))?;
        self.telemetry.add(CensusSourceTelemetry {
            response_bytes,
            latency_nanos,
            ..CensusSourceTelemetry::default()
        });
        Ok(response_bytes)
    }

    fn effective_parse_limits(&self) -> CensusParseLimits {
        self.config.parse_limits
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), ExtractionSourceError> {
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(invalid_protocol());
        }
        Ok(())
    }
}

impl SourceMetadataProvider for CensusSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for CensusSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(self.discover_impl(authority, request, cancellation))
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

#[derive(Debug)]
struct FetchedResponse {
    body: Bytes,
    capture: ProviderCaptureSetReceipt,
    received_at: Timestamp,
    latency: Duration,
}

impl FetchedResponse {
    fn base_telemetry(&self) -> Result<CensusSourceTelemetry, CensusSourceError> {
        Ok(CensusSourceTelemetry {
            requests: 1,
            successful_responses: 1,
            response_bytes: u64::try_from(self.body.len())
                .map_err(|_| CensusSourceError::TelemetryOverflow)?,
            latency_nanos: u64::try_from(self.latency.as_nanos())
                .map_err(|_| CensusSourceError::TelemetryOverflow)?,
            ..CensusSourceTelemetry::default()
        })
    }

    fn telemetry_with_metadata(
        &self,
        entries: usize,
    ) -> Result<CensusSourceTelemetry, CensusSourceError> {
        self.base_telemetry()?.checked_add(CensusSourceTelemetry {
            metadata_entries: u64::try_from(entries)
                .map_err(|_| CensusSourceError::TelemetryOverflow)?,
            ..CensusSourceTelemetry::default()
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "provider request and response accounting remains dimensionally explicit"
    )]
    fn telemetry_with_data(
        &self,
        requested_variables: usize,
        returned_variables: usize,
        missing_variables: usize,
        returned_rows: usize,
        usable_rows: usize,
        observations: usize,
        partial: bool,
    ) -> Result<CensusSourceTelemetry, ExtractionSourceError> {
        self.base_telemetry()
            .and_then(|base| {
                base.checked_add(CensusSourceTelemetry {
                    requested_variables: checked_u64(requested_variables)?,
                    returned_variables: checked_u64(returned_variables)?,
                    missing_variables: checked_u64(missing_variables)?,
                    returned_rows: checked_u64(returned_rows)?,
                    usable_rows: checked_u64(usable_rows)?,
                    observations: checked_u64(observations)?,
                    partial_responses: u64::from(partial),
                    ..CensusSourceTelemetry::default()
                })
            })
            .map_err(map_source_error)
    }
}

fn checked_u64(value: usize) -> Result<u64, CensusSourceError> {
    u64::try_from(value).map_err(|_| CensusSourceError::TelemetryOverflow)
}

fn discovery_evidence(document: &CensusDiscoveryDocument) -> &CensusMetadataEvidence {
    match document {
        CensusDiscoveryDocument::Datasets(value) => value.evidence(),
        CensusDiscoveryDocument::Variables(value)
        | CensusDiscoveryDocument::GroupVariables(value) => value.evidence(),
        CensusDiscoveryDocument::Groups(value) => value.evidence(),
        CensusDiscoveryDocument::Geographies(value) => value.evidence(),
    }
}

fn validate_metadata_bundle(
    contract: &CensusDatasetContract,
    documents: &[CensusCapturedDiscovery],
) -> Result<(), CensusSourceError> {
    if documents.len() != contract.metadata_requests().len()
        || documents
            .iter()
            .zip(contract.metadata_requests())
            .any(|(document, request)| document.request() != request)
    {
        return Err(CensusSourceError::Protocol);
    }
    let dataset_seen = documents.iter().any(|captured| match captured.document() {
        CensusDiscoveryDocument::Datasets(catalog) => catalog
            .datasets()
            .iter()
            .any(|dataset| dataset.dataset() == contract.query().dataset()),
        _ => false,
    });
    let full_variables = documents
        .iter()
        .find_map(|captured| match captured.document() {
            CensusDiscoveryDocument::Variables(catalog) => Some(catalog),
            _ => None,
        })
        .ok_or(CensusSourceError::Protocol)?;
    let groups = documents
        .iter()
        .find_map(|captured| match captured.document() {
            CensusDiscoveryDocument::Groups(catalog) => Some(catalog),
            _ => None,
        })
        .ok_or(CensusSourceError::Protocol)?;
    let geographies = documents
        .iter()
        .find_map(|captured| match captured.document() {
            CensusDiscoveryDocument::Geographies(catalog) => Some(catalog),
            _ => None,
        })
        .ok_or(CensusSourceError::Protocol)?;
    if !dataset_seen
        || full_variables.dataset() != contract.query().dataset()
        || groups.dataset() != contract.query().dataset()
        || geographies.dataset() != contract.query().dataset()
    {
        return Err(CensusSourceError::Protocol);
    }
    let selected_variables = match contract.query().selection() {
        CensusSelection::Variables { .. } => full_variables,
        CensusSelection::Group { group } => {
            if !groups
                .groups()
                .iter()
                .any(|candidate| candidate.name() == group)
            {
                return Err(CensusSourceError::Protocol);
            }
            documents
                .iter()
                .find_map(|captured| match captured.document() {
                    CensusDiscoveryDocument::GroupVariables(catalog)
                        if catalog.group() == Some(group) =>
                    {
                        Some(catalog)
                    }
                    _ => None,
                })
                .ok_or(CensusSourceError::Protocol)?
        }
    };
    for mapping in contract.mappings() {
        let variable = selected_variables
            .get(mapping.provider_variable().as_str())
            .ok_or(CensusSourceError::Protocol)?;
        if !matches!(
            variable.predicate_type(),
            CensusPredicateType::Integer | CensusPredicateType::Float
        ) {
            return Err(CensusSourceError::Protocol);
        }
        for attribute in variable.attributes() {
            if selected_variables.get(attribute.as_str()).is_none() {
                return Err(CensusSourceError::Protocol);
            }
        }
    }
    for predicate in contract.query().predicates() {
        let metadata = full_variables
            .get(predicate.variable().as_str())
            .ok_or(CensusSourceError::Protocol)?;
        if metadata.predicate_type() != predicate.predicate_type() {
            return Err(CensusSourceError::Protocol);
        }
    }
    if contract.query().time().is_some()
        && full_variables
            .get("time")
            .is_none_or(|value| value.predicate_type() != &CensusPredicateType::Time)
    {
        return Err(CensusSourceError::Protocol);
    }
    match contract.query().geography() {
        CensusGeography::Standard {
            for_clause,
            in_clauses,
        } => {
            let levels = in_clauses
                .iter()
                .map(|clause| clause.level())
                .chain(std::iter::once(for_clause.level()));
            if levels
                .into_iter()
                .any(|level| geographies.named(level).next().is_none())
            {
                return Err(CensusSourceError::Protocol);
            }
        }
        CensusGeography::Uniform { .. } => {
            if full_variables
                .get("ucgid")
                .is_none_or(|value| value.predicate_type() != &CensusPredicateType::Ucgid)
            {
                return Err(CensusSourceError::Protocol);
            }
        }
    }
    Ok(())
}

fn metadata_bundle_digest(
    contract: &CensusDatasetContract,
    documents: &[CensusCapturedDiscovery],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/census-metadata-bundle/v1");
    digest.update(contract.query().request_digest());
    digest.update((documents.len() as u64).to_be_bytes());
    for document in documents {
        digest.update(document.request().request_digest());
        digest.update(discovery_evidence(document.document()).payload_digest());
    }
    digest.finalize().into()
}

fn authorize_configured_target(
    metadata: &SourceMetadata,
    redacted_url: &str,
    include_key: bool,
) -> Result<(), CensusSourceError> {
    let mut url = url::Url::parse(redacted_url).map_err(|_| CensusSourceError::InvalidMetadata)?;
    if include_key {
        url.query_pairs_mut().append_pair("key", "configured-key");
    }
    metadata
        .network_policy()
        .authorize(url.as_str())
        .map_err(|_| CensusSourceError::InvalidMetadata)
}

fn secret_target(authorized: &CensusAuthorizedUrl) -> Result<Zeroizing<String>, CensusSourceError> {
    let mut url = authorized.transport_url().clone();
    url.query_pairs_mut()
        .append_pair("key", authorized.key_query_value());
    Ok(Zeroizing::new(url.to_string()))
}

async fn acquire_request_permit(
    authority: &ExtractionAuthority,
    target: &str,
    deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<ExtractionRequestPermit, ExtractionSourceError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        match authority.try_network_request(target) {
            Ok(permit) => return Ok(permit),
            Err(ExtractionAuthorityError::BudgetWaitUntil {
                deadline: wait_until,
            }) => {
                let wait = authority.remaining_budget_wait(wait_until)?;
                let now = system_timestamp().map_err(map_source_error)?;
                let remaining = remaining_timeout(deadline, now, Duration::MAX)?;
                if wait > remaining {
                    return Err(ExtractionSourceError::DeadlineExceeded);
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(ExtractionSourceError::Cancelled);
                    }
                    () = tokio::time::sleep(wait) => {}
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured: Duration,
) -> Result<Duration, ExtractionSourceError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(Duration::from_nanos)
        .ok_or(ExtractionSourceError::DeadlineExceeded)?;
    Ok(remaining.min(configured))
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn source_object(
    metadata: &SourceMetadata,
    request: &DiscoveryRequest,
    contract: &CensusDatasetContract,
    acquired: &CensusDatasetAcquisition,
) -> Result<SourceObject, ExtractionSourceError> {
    let body_digest = acquired.data().page().response_payload_digest();
    let metadata_digest = acquired.metadata().content_digest();
    let query_digest = contract.query().request_digest();
    let object_id = SourceIdentifier::try_from(format!(
        "census-object:{}:{}:{}",
        lower_hex(query_digest),
        lower_hex(metadata_digest),
        lower_hex(body_digest),
    ))
    .map_err(|_| invalid_protocol())?;
    let locator = VersionPinnedSourceLocator::new(
        contract.dataset_id().clone(),
        SourceIdentifier::try_from(lower_hex(body_digest)).map_err(|_| invalid_protocol())?,
    );
    let evidence =
        ExactPayloadEvidence::with_version_pinned_locator(evidence_digest(body_digest), locator);
    let received_at = acquired.data().page().clocks().received_at();
    let effective = EffectiveInterval::new(received_at, None).map_err(|_| invalid_protocol())?;
    SourceObject::try_new_with_availability(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        SourceIdentifier::try_from(CENSUS_JSON_MEDIA_TYPE).map_err(|_| invalid_protocol())?,
        evidence,
        effective,
        None,
        market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        Some(u64::try_from(acquired.data().body().len()).map_err(|_| invalid_protocol())?),
    )
    .map_err(Into::into)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedObjectId {
    query_digest: [u8; 32],
    metadata_digest: [u8; 32],
    body_digest: [u8; 32],
}

impl ParsedObjectId {
    fn parse(value: &SourceIdentifier) -> Result<Self, ExtractionSourceError> {
        let mut fields = value.as_str().split(':');
        if fields.next() != Some("census-object") {
            return Err(invalid_protocol());
        }
        let query_digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
        let metadata_digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
        let body_digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
        if fields.next().is_some() {
            return Err(invalid_protocol());
        }
        Ok(Self {
            query_digest,
            metadata_digest,
            body_digest,
        })
    }
}

fn verify_acquisition(
    request: &ExtractionRequest,
    identity: &ParsedObjectId,
    acquired: &CensusDatasetAcquisition,
) -> Result<(), ExtractionSourceError> {
    let body = acquired.data().body();
    let body_bytes = u64::try_from(body.len()).map_err(|_| invalid_protocol())?;
    if identity.metadata_digest != acquired.metadata().content_digest()
        || identity.body_digest != acquired.data().page().response_payload_digest()
        || identity.body_digest != sha256(body)
        || !payload_matches_exact_evidence(body, request.object().evidence())
        || request
            .object()
            .expected_bytes()
            .is_some_and(|expected| expected != body_bytes)
    {
        return Err(SourceError::GenerationResynchronizationRequired.into());
    }
    Ok(())
}

fn extraction_output(
    metadata: &SourceMetadata,
    request: &ExtractionRequest,
    contract: &CensusDatasetContract,
    acquisition: CensusDatasetAcquisition,
) -> Result<CensusExtractionOutput, ExtractionSourceError> {
    let records = canonical_records(metadata, contract, acquisition.data().page())
        .map_err(map_source_error)?;
    if records.len() > request.max_records() as usize {
        return Err(
            market_squawk_sources::ExtractionError::RecordLimitExceeded {
                requested: request.max_records(),
            }
            .into(),
        );
    }
    let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
        .map_err(|_| invalid_protocol())?;
    let records = records
        .into_iter()
        .map(|record| {
            ExtractionRecord::try_new_with_time(
                request,
                schema.clone(),
                record.evidence,
                record.effective,
                None,
                record.availability,
                record.revision,
                None,
                record.payload,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = ExtractionBatch::try_new(request, records)?;
    let captures = capture_materials(metadata, &acquisition)?;
    let telemetry = acquisition.telemetry();
    Ok(CensusExtractionOutput {
        batch,
        acquisition,
        captures,
        telemetry,
    })
}

fn capture_materials(
    metadata: &SourceMetadata,
    acquired: &CensusDatasetAcquisition,
) -> Result<Box<[ProviderCaptureMaterial]>, ExtractionSourceError> {
    let capacity = acquired
        .metadata()
        .documents()
        .len()
        .checked_add(1)
        .ok_or_else(invalid_protocol)?;
    let mut materials = Vec::new();
    materials
        .try_reserve_exact(capacity)
        .map_err(|_| invalid_protocol())?;
    for document in acquired.metadata().documents() {
        materials.push(capture_material(
            metadata,
            document.capture(),
            document.body().clone(),
        )?);
    }
    materials.push(capture_material(
        metadata,
        acquired.data().capture(),
        acquired.data().body().clone(),
    )?);
    Ok(materials.into_boxed_slice())
}

fn capture_material(
    metadata: &SourceMetadata,
    receipt: &ProviderCaptureSetReceipt,
    body: Bytes,
) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
    if receipt.source_id() != metadata.source_id()
        || receipt.metadata_revision() != metadata.revision()
        || receipt.pages().len() != 1
    {
        return Err(invalid_protocol());
    }
    let page = receipt.pages().first().ok_or_else(invalid_protocol)?;
    let received_at = DateTime::<Utc>::from_timestamp_nanos(page.received_at().unix_nanos());
    let record = market_squawk_platform::RawCaptureRecord::try_new_live(
        deterministic_capture_uuid(b"event", receipt),
        Arc::from(metadata.source_id().as_str()),
        deterministic_capture_uuid(b"connection", receipt),
        Some(0),
        None,
        received_at,
        body,
    )
    .map_err(|_| invalid_protocol())?;
    ProviderCaptureMaterial::try_new(receipt.clone(), vec![record]).map_err(|_| invalid_protocol())
}

fn deterministic_capture_uuid(tag: &[u8], receipt: &ProviderCaptureSetReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/census-raw-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_set_identity().bytes());
    hash.update(receipt.observation_digest().bytes());
    let mut bytes: [u8; 16] = hash.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has a fixed length");
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

struct CanonicalCensusRecord {
    effective: ResearchTemporalCoordinate,
    availability: market_squawk_sources::AvailabilityEvidence,
    revision: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    payload: Bytes,
}

fn canonical_records(
    source: &SourceMetadata,
    contract: &CensusDatasetContract,
    page: &CensusDataPage,
) -> Result<Vec<CanonicalCensusRecord>, CensusSourceError> {
    if !page.completeness().is_complete()
        || page.dataset() != contract.query().dataset()
        || page.request_digest() != contract.query().request_digest()
    {
        return Err(CensusSourceError::Protocol);
    }
    let mut records = Vec::new();
    for observation in page.observations() {
        let Some(mapping) = contract.mapping(observation.variable()) else {
            continue;
        };
        let effective = effective_coordinate(&contract.effective_time, observation)?;
        let series = scoped_series(mapping, observation.revision_candidate().family_digest())?;
        let revision = SourceIdentifier::try_from(format!(
            "census-observed:{}",
            lower_hex(observation.revision_candidate().content_digest())
        ))
        .map_err(|_| CensusSourceError::Protocol)?;
        let received_at = observation.clocks().received_at();
        let ingested_at = observation.clocks().ingested_at();
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: source.source_id().clone(),
            instrument_id: None,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(format!(
                "census-row:{}",
                lower_hex(observation.row_digest())
            ))
            .map_err(|_| CensusSourceError::Protocol)?,
            source_timestamp: None,
            received_at,
            ingested_at,
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                DigestAlgorithm::Sha256,
                page.response_payload_digest(),
            )),
            availability: ResearchAvailabilityEvidence::local_first_observed(received_at),
        })
        .map_err(|_| CensusSourceError::Protocol)?;
        let time = ResearchTime::try_new_with_coordinates(
            effective.clone(),
            None,
            RevisionNumber::new(1).map_err(|_| CensusSourceError::Protocol)?,
            None,
        )
        .map_err(|_| CensusSourceError::Protocol)?;
        let context =
            ResearchContext::new(provenance, time).map_err(|_| CensusSourceError::Protocol)?;
        let macro_observation = match observation.value() {
            CensusValueState::Observed { value } => MacroObservation::new(
                context,
                series,
                canonical_decimal(value)?,
                mapping.unit().clone(),
            ),
            CensusValueState::Missing { reason, .. } => MacroObservation::missing(
                context,
                series,
                canonical_missing(*reason)?,
                mapping.unit().clone(),
            ),
            CensusValueState::Annotated { .. } | CensusValueState::Invalid { .. } => {
                return Err(CensusSourceError::Protocol);
            }
        };
        let payload = serde_json::to_vec(&ResearchObservation::Macro(macro_observation))
            .map(Bytes::from)
            .map_err(|_| CensusSourceError::Protocol)?;
        let payload_digest = sha256(&payload);
        records.push(CanonicalCensusRecord {
            effective,
            availability: market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
            revision,
            evidence: ExactPayloadEvidence::from_content_digest(evidence_digest(payload_digest)),
            payload,
        });
    }
    if records.is_empty() {
        return Err(CensusSourceError::Protocol);
    }
    Ok(records)
}

fn canonical_decimal(value: &CensusTypedValue) -> Result<Decimal, CensusSourceError> {
    match value {
        CensusTypedValue::Decimal(value) => Ok(*value),
        CensusTypedValue::Integer(value) => {
            Decimal::try_from_i128_with_scale(*value, 0).map_err(|_| CensusSourceError::Protocol)
        }
        CensusTypedValue::Text(_) | CensusTypedValue::Boolean(_) => {
            Err(CensusSourceError::Protocol)
        }
    }
}

fn canonical_missing(reason: CensusMissingReason) -> Result<MacroMissingValue, CensusSourceError> {
    let (marker, reason) = match reason {
        CensusMissingReason::JsonNull => ("json-null", "census-json-null"),
        CensusMissingReason::EmptyString => ("empty-string", "census-empty-string"),
        CensusMissingReason::ProviderAnnotatedMissing => {
            ("provider-annotated-missing", "census-provider-annotation")
        }
        CensusMissingReason::AnnotationColumnMissing => {
            return Err(CensusSourceError::Protocol);
        }
    };
    Ok(MacroMissingValue::new(
        SourceIdentifier::try_from(marker).map_err(|_| CensusSourceError::Protocol)?,
        Some(SourceIdentifier::try_from(reason).map_err(|_| CensusSourceError::Protocol)?),
    ))
}

fn effective_coordinate(
    policy: &CensusEffectiveTimePolicy,
    observation: &crate::CensusObservation,
) -> Result<ResearchTemporalCoordinate, CensusSourceError> {
    match (policy, observation.reported_time()) {
        (CensusEffectiveTimePolicy::Fixed(value), None) => Ok(value.clone()),
        (CensusEffectiveTimePolicy::Fixed(_), Some(_))
        | (CensusEffectiveTimePolicy::RequireReportedTime, None) => {
            Err(CensusSourceError::Protocol)
        }
        (
            CensusEffectiveTimePolicy::RequireReportedTime,
            Some(crate::CensusReportedTime::CalendarDate { date }),
        ) => Ok(ResearchTemporalCoordinate::calendar_date(*date)),
        (
            CensusEffectiveTimePolicy::RequireReportedTime,
            Some(crate::CensusReportedTime::Year { year }),
        ) => source_period("census-year", *year, 1, format!("{year:04}")),
        (
            CensusEffectiveTimePolicy::RequireReportedTime,
            Some(crate::CensusReportedTime::Month { year, month }),
        ) => source_period(
            "census-month",
            *year,
            u16::from(*month),
            format!("{year:04}-{month:02}"),
        ),
        (
            CensusEffectiveTimePolicy::RequireReportedTime,
            Some(crate::CensusReportedTime::Quarter { year, quarter }),
        ) => source_period(
            "census-quarter",
            *year,
            u16::from(*quarter),
            format!("{year:04}-Q{quarter}"),
        ),
        (
            CensusEffectiveTimePolicy::RequireReportedTime,
            Some(crate::CensusReportedTime::ProviderPeriod { .. }),
        ) => Err(CensusSourceError::Protocol),
    }
}

fn source_period(
    scheme: &str,
    year: u16,
    ordinal: u16,
    code: String,
) -> Result<ResearchTemporalCoordinate, CensusSourceError> {
    Ok(ResearchTemporalCoordinate::source_period(
        ResearchPeriod::try_new(
            SourceIdentifier::try_from(scheme).map_err(|_| CensusSourceError::Protocol)?,
            year,
            NonZeroU16::new(ordinal).ok_or(CensusSourceError::Protocol)?,
            SourceIdentifier::try_from(code).map_err(|_| CensusSourceError::Protocol)?,
        )
        .map_err(|_| CensusSourceError::Protocol)?,
    ))
}

fn scoped_series(
    mapping: &CensusVariableMapping,
    family_digest: [u8; 32],
) -> Result<SourceIdentifier, CensusSourceError> {
    SourceIdentifier::try_from(format!(
        "{}:scope:{}",
        mapping.series_namespace(),
        lower_hex(family_digest)
    ))
    .map_err(|_| CensusSourceError::Protocol)
}

fn parse_lower_hex(value: &str) -> Result<[u8; 32], ExtractionSourceError> {
    if value.len() != 64 {
        return Err(invalid_protocol());
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, ExtractionSourceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_protocol()),
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn invalid_protocol() -> ExtractionSourceError {
    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
}

fn map_adapter_error(error: CensusAdapterError) -> ExtractionSourceError {
    match error {
        CensusAdapterError::BodyTooLarge => ExtractionSourceError::Source(SourceError::Network),
        _ => invalid_protocol(),
    }
}

fn map_source_error(error: CensusSourceError) -> ExtractionSourceError {
    match error {
        CensusSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        CensusSourceError::Cancelled => ExtractionSourceError::Cancelled,
        CensusSourceError::Network | CensusSourceError::BodyTooLarge => {
            ExtractionSourceError::Source(SourceError::Network)
        }
        CensusSourceError::Clock => {
            ExtractionSourceError::Source(SourceError::TrustedTimeUnavailable)
        }
        CensusSourceError::Authority => {
            ExtractionSourceError::Source(SourceError::SessionNotCurrent)
        }
        CensusSourceError::InvalidMetadata
        | CensusSourceError::InvalidConfiguration
        | CensusSourceError::Protocol
        | CensusSourceError::TelemetryOverflow
        | CensusSourceError::Adapter(_)
        | CensusSourceError::Capture(_)
        | CensusSourceError::Revision(_) => invalid_protocol(),
    }
}

#[cfg(test)]
mod tests;
