//! Registry-authorized BEA source composition and capture-ready typed handoff.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    ResearchPeriod, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationMode, BackoffPolicy, BudgetScope, BudgetWindowSemantics,
    CoverageDomain, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionAuthorityError, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    ExtractionRequestPermit, ExtractionSource, ExtractionSourceError, HistoricalCapability,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, NetworkAccessPolicy, NetworkPolicyError, PathScope,
    ProviderBudgetPolicy, ProviderBudgetWindow, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderRateDeclaration, QueryParameterRule, QuerySensitivity, SourceClass, SourceError,
    SourceMetadata, SourceMetadataProvider, SourceObject, SourceObjectCaptureIdentity,
    SourceProtocolProfile,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::transport::{BeaHttpResponse, BeaTransport, ReqwestBeaTransport, system_timestamp};
use crate::{
    BEA_API_ENDPOINT, BEA_APPLICATION_REQUESTS_PER_MINUTE, BEA_MINIMUM_REQUEST_INTERVAL,
    BeaCompleteness, BeaDataPage, BeaDatasetIdentity, BeaError, BeaFrequency,
    BeaMetadataGeneration, BeaMetadataPage, BeaMetadataRecords, BeaMethod, BeaMissingValue,
    BeaObservation, BeaObservationValue, BeaParameterDefinition, BeaParameterIdentity,
    BeaParseLimits, BeaQuery, BeaRequest, BeaUserId, parse_data_page, parse_metadata_page,
};

/// Maximum explicit BEA data-query contracts retained by one adapter instance.
pub const MAX_BEA_CONFIGURED_DATASETS: usize = 64;
/// Source-native, non-canonical extraction payload schema.
pub const BEA_NATIVE_EXTRACTION_SCHEMA: &str = "market-squawk-bea-native-v1";

const BEA_DATASET_PREFIX: &str = "bea:data-v1:";
const BEA_ANALYTICAL_PREFIX: &str = "bea.data-v1.";
const BEA_JSON_MEDIA_TYPE: &str = "application/json";
const MAX_RETRY_AFTER_BYTES: usize = 256;

/// One metadata-first BEA data selection admitted by application composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDatasetContract {
    dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    dataset: BeaDatasetIdentity,
    parameters: BTreeMap<BeaParameterIdentity, String>,
    expected_rows: Option<usize>,
}

impl BeaDatasetContract {
    /// Binds one exact provider dataset and selector set to stable source and analytical IDs.
    pub fn try_new(
        dataset: BeaDatasetIdentity,
        parameters: BTreeMap<BeaParameterIdentity, String>,
        expected_rows: Option<usize>,
    ) -> Result<Self, BeaSourceError> {
        let provisional = BeaMetadataGeneration::from_response_digests(&[[1; 32]])?;
        let query = BeaQuery::data(dataset.clone(), parameters.clone(), provisional)?;
        let _request = query.single_page(expected_rows)?;
        let digest = contract_digest(&dataset, &parameters, expected_rows)?;
        let encoded = lower_hex(digest);
        let dataset_id = SourceIdentifier::try_from(format!("{BEA_DATASET_PREFIX}{encoded}"))
            .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        let analytical_dataset_id =
            SourceIdentifier::try_from(format!("{BEA_ANALYTICAL_PREFIX}{encoded}"))
                .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        Ok(Self {
            dataset_id,
            analytical_dataset_id,
            dataset,
            parameters,
            expected_rows,
        })
    }

    /// Returns the extraction-facing exact query identity.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the storage-safe analytical identity for later canonical composition.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }

    /// Returns the provider dataset discovered through `GetDatasetList`.
    pub const fn provider_dataset(&self) -> &BeaDatasetIdentity {
        &self.dataset
    }

    /// Returns exact selector values in deterministic parameter-name order.
    pub const fn parameters(&self) -> &BTreeMap<BeaParameterIdentity, String> {
        &self.parameters
    }

    /// Returns metadata-derived expected rows when composition can establish them.
    pub const fn expected_rows(&self) -> Option<usize> {
        self.expected_rows
    }

    fn metadata_requests(&self) -> Result<Vec<BeaRequest>, BeaSourceError> {
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(self.parameters.len().saturating_add(2))
            .map_err(|_| BeaSourceError::Allocation)?;
        requests.push(BeaQuery::dataset_list()?.single_page(None)?);
        requests.push(BeaQuery::parameter_list(self.dataset.clone())?.single_page(None)?);
        for parameter in self.parameters.keys() {
            requests.push(
                BeaQuery::parameter_values(self.dataset.clone(), parameter.clone())?
                    .single_page(None)?,
            );
        }
        Ok(requests)
    }

    fn data_request(
        &self,
        generation: BeaMetadataGeneration,
    ) -> Result<BeaRequest, BeaSourceError> {
        Ok(
            BeaQuery::data(self.dataset.clone(), self.parameters.clone(), generation)?
                .single_page(self.expected_rows)?,
        )
    }
}

/// Bounded immutable set of admitted BEA query contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaSourceConfig {
    contracts: Vec<BeaDatasetContract>,
    parse_limits: BeaParseLimits,
}

impl BeaSourceConfig {
    /// Constructs a nonempty, duplicate-free contract set.
    pub fn try_new(
        mut contracts: Vec<BeaDatasetContract>,
        parse_limits: BeaParseLimits,
    ) -> Result<Self, BeaSourceError> {
        if contracts.is_empty() || contracts.len() > MAX_BEA_CONFIGURED_DATASETS {
            return Err(BeaSourceError::InvalidConfiguration);
        }
        contracts.sort_by(|left, right| left.dataset_id.cmp(&right.dataset_id));
        if contracts
            .windows(2)
            .any(|pair| pair[0].dataset_id == pair[1].dataset_id)
        {
            return Err(BeaSourceError::InvalidConfiguration);
        }
        let unique_parameters = contracts
            .iter()
            .flat_map(|contract| contract.parameters.keys())
            .map(|parameter| parameter.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        // The shared endpoint contract admits at most 32 query-key rules. Six are BEA controls.
        if unique_parameters.len() > 26 {
            return Err(BeaSourceError::InvalidConfiguration);
        }
        Ok(Self {
            contracts,
            parse_limits,
        })
    }

    /// Returns exact configured contracts in stable identity order.
    pub fn contracts(&self) -> &[BeaDatasetContract] {
        &self.contracts
    }

    /// Returns parser memory and row bounds.
    pub const fn parse_limits(&self) -> BeaParseLimits {
        self.parse_limits
    }

    fn contract(&self, dataset: &SourceIdentifier) -> Option<&BeaDatasetContract> {
        self.contracts
            .binary_search_by(|contract| contract.dataset_id.cmp(dataset))
            .ok()
            .map(|index| &self.contracts[index])
    }
}

/// Builds the structural allowlist rule required by one BEA source configuration.
pub fn bea_api_endpoint_rule(
    config: &BeaSourceConfig,
) -> Result<ApiEndpointRule, NetworkPolicyError> {
    let mut rules = vec![
        query_rule("UserID", 36, QuerySensitivity::Secret)?,
        query_rule("Method", 64, QuerySensitivity::Public)?,
        query_rule("DatasetName", 128, QuerySensitivity::Public)?,
        query_rule("ParameterName", 128, QuerySensitivity::Public)?,
        query_rule("TargetParameter", 128, QuerySensitivity::Public)?,
        QueryParameterRule::try_new_exact_public(
            SourceIdentifier::try_from("ResultFormat")
                .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
            SourceIdentifier::try_from("JSON")
                .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        )?,
    ];
    let unique = config
        .contracts()
        .iter()
        .flat_map(|contract| contract.parameters().keys())
        .map(|parameter| parameter.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    for parameter in unique {
        rules.push(query_rule(&parameter, 4 * 1024, QuerySensitivity::Public)?);
    }
    ApiEndpointRule::try_new(BEA_API_ENDPOINT, PathScope::Exact, rules, 38, 16_384)
}

/// Builds the conservative product-wide provider-rate declaration for one BEA credential realm.
///
/// The collision subject is code-owned and stable; the `UserID` is never hashed into rate-policy
/// state. App composition registers this declaration with `ProviderRateAuthority` and binds every
/// BEA source/doctor/background job to that one allocation.
pub fn bea_provider_rate_declaration() -> Result<ProviderRateDeclaration, BeaSourceError> {
    let provider =
        SourceIdentifier::try_from("bea").map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let subject = ProviderRateDeclaration::governed_provider_subject(&provider)
        .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let window = ProviderBudgetWindow::try_new(
        NonZeroU32::new(BEA_APPLICATION_REQUESTS_PER_MINUTE)
            .ok_or(BeaSourceError::InvalidConfiguration)?,
        NonZeroU64::new(60_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let policy = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(provider, subject.clone()),
        &[window],
        NonZeroU16::new(1).ok_or(BeaSourceError::InvalidConfiguration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
            NonZeroU64::new(60_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
            0,
        )
        .map_err(|_| BeaSourceError::InvalidConfiguration)?,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    ProviderRateDeclaration::try_for_authorization_subject(policy, &subject)
        .map_err(|_| BeaSourceError::InvalidConfiguration)
}

fn query_rule(
    key: &str,
    max: u16,
    sensitivity: QuerySensitivity,
) -> Result<QueryParameterRule, NetworkPolicyError> {
    QueryParameterRule::try_new(
        SourceIdentifier::try_from(key).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        max,
        false,
        sensitivity,
    )
}

/// Exact per-response transport and completeness facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaResponseTelemetry {
    request_identity: EvidenceDigest,
    method: BeaMethod,
    status: u16,
    response_bytes: u64,
    latency_nanos: u64,
    retry_after: Option<Box<[u8]>>,
    page_number: u32,
    page_count: u32,
    requested_rows: Option<u64>,
    returned_rows: u64,
    missing_rows: Option<u64>,
    completeness: BeaCompleteness,
}

impl BeaResponseTelemetry {
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }
    pub const fn method(&self) -> BeaMethod {
        self.method
    }
    pub const fn status(&self) -> u16 {
        self.status
    }
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }
    pub fn retry_after(&self) -> Option<&[u8]> {
        self.retry_after.as_deref()
    }
    pub const fn page_number(&self) -> u32 {
        self.page_number
    }
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }
    pub const fn requested_rows(&self) -> Option<u64> {
        self.requested_rows
    }
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }
    pub const fn missing_rows(&self) -> Option<u64> {
        self.missing_rows
    }
    pub const fn completeness(&self) -> BeaCompleteness {
        self.completeness
    }
}

/// Lock-free cumulative operational accounting for scheduler and provider diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct BeaSourceTelemetry {
    requests: u64,
    successful_responses: u64,
    rate_limited_responses: u64,
    provider_errors: u64,
    retry_after_responses: u64,
    response_bytes: u64,
    latency_nanos: u64,
    metadata_records: u64,
    requested_rows: u64,
    returned_rows: u64,
    missing_rows: u64,
    unknown_completeness_pages: u64,
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

impl BeaSourceTelemetry {
    telemetry_getter!(requests);
    telemetry_getter!(successful_responses);
    telemetry_getter!(rate_limited_responses);
    telemetry_getter!(provider_errors);
    telemetry_getter!(retry_after_responses);
    telemetry_getter!(response_bytes);
    telemetry_getter!(latency_nanos);
    telemetry_getter!(metadata_records);
    telemetry_getter!(requested_rows);
    telemetry_getter!(returned_rows);
    telemetry_getter!(missing_rows);
    telemetry_getter!(unknown_completeness_pages);
    telemetry_getter!(failures);
}

#[derive(Debug, Default)]
struct BeaTelemetryState {
    requests: AtomicU64,
    successful_responses: AtomicU64,
    rate_limited_responses: AtomicU64,
    provider_errors: AtomicU64,
    retry_after_responses: AtomicU64,
    response_bytes: AtomicU64,
    latency_nanos: AtomicU64,
    metadata_records: AtomicU64,
    requested_rows: AtomicU64,
    returned_rows: AtomicU64,
    missing_rows: AtomicU64,
    unknown_completeness_pages: AtomicU64,
    failures: AtomicU64,
}

impl BeaTelemetryState {
    fn snapshot(&self) -> BeaSourceTelemetry {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        BeaSourceTelemetry {
            requests: load(&self.requests),
            successful_responses: load(&self.successful_responses),
            rate_limited_responses: load(&self.rate_limited_responses),
            provider_errors: load(&self.provider_errors),
            retry_after_responses: load(&self.retry_after_responses),
            response_bytes: load(&self.response_bytes),
            latency_nanos: load(&self.latency_nanos),
            metadata_records: load(&self.metadata_records),
            requested_rows: load(&self.requested_rows),
            returned_rows: load(&self.returned_rows),
            missing_rows: load(&self.missing_rows),
            unknown_completeness_pages: load(&self.unknown_completeness_pages),
            failures: load(&self.failures),
        }
    }

    fn add(&self, value: &AtomicU64, increment: u64) {
        let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_add(increment))
        });
    }
}

/// One validated metadata response with exact capture-ready material.
#[derive(Debug)]
pub struct BeaCapturedMetadataPage {
    request: BeaRequest,
    page: BeaMetadataPage,
    material: ProviderCaptureMaterial,
    telemetry: BeaResponseTelemetry,
}

impl BeaCapturedMetadataPage {
    pub const fn request(&self) -> &BeaRequest {
        &self.request
    }
    pub const fn page(&self) -> &BeaMetadataPage {
        &self.page
    }
    /// Returns the source-neutral receipt plus exact raw response ready for `MSJ1` sealing.
    pub const fn material(&self) -> &ProviderCaptureMaterial {
        &self.material
    }
    pub const fn telemetry(&self) -> &BeaResponseTelemetry {
        &self.telemetry
    }
}

/// Complete exact metadata sequence used to construct one BEA data request.
#[derive(Debug)]
pub struct BeaMetadataBundle {
    dataset_id: SourceIdentifier,
    pages: Vec<BeaCapturedMetadataPage>,
    generation: BeaMetadataGeneration,
}

impl BeaMetadataBundle {
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }
    pub fn pages(&self) -> &[BeaCapturedMetadataPage] {
        &self.pages
    }
    pub const fn generation(&self) -> BeaMetadataGeneration {
        self.generation
    }
}

/// One validated data response retaining typed rows and exact capture-ready material.
#[derive(Debug)]
pub struct BeaCapturedDataPage {
    request: BeaRequest,
    page: BeaDataPage,
    material: ProviderCaptureMaterial,
    telemetry: BeaResponseTelemetry,
}

impl BeaCapturedDataPage {
    pub const fn request(&self) -> &BeaRequest {
        &self.request
    }
    pub const fn page(&self) -> &BeaDataPage {
        &self.page
    }
    /// Returns the source-neutral receipt plus exact raw response ready for `MSJ1` sealing.
    pub const fn material(&self) -> &ProviderCaptureMaterial {
        &self.material
    }
    pub const fn telemetry(&self) -> &BeaResponseTelemetry {
        &self.telemetry
    }
}

/// Complete metadata-first acquisition; it is not canonical-publication authority.
#[derive(Debug)]
pub struct BeaDatasetAcquisition {
    metadata: BeaMetadataBundle,
    data: BeaCapturedDataPage,
}

impl BeaDatasetAcquisition {
    pub const fn metadata(&self) -> &BeaMetadataBundle {
        &self.metadata
    }
    pub const fn data(&self) -> &BeaCapturedDataPage {
        &self.data
    }

    /// Consumes the acquisition into its metadata captures followed by its data capture.
    ///
    /// This order is the exact request order and is the intended handoff to the sole `MSJ1`
    /// sealing boundary before any canonical publication can be attempted.
    pub fn into_capture_materials(self) -> Result<Vec<ProviderCaptureMaterial>, BeaSourceError> {
        let mut materials = Vec::new();
        materials
            .try_reserve_exact(self.metadata.pages.len().saturating_add(1))
            .map_err(|_| BeaSourceError::Allocation)?;
        materials.extend(self.metadata.pages.into_iter().map(|page| page.material));
        materials.push(self.data.material);
        Ok(materials)
    }
}

/// Rich discovery output preserving raw material that the trait-only batch cannot carry.
#[derive(Debug)]
pub struct BeaCapturedDiscovery {
    batch: DiscoveryBatch,
    acquisition: BeaDatasetAcquisition,
}

impl BeaCapturedDiscovery {
    /// Returns the validated source-object discovery batch.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }
    /// Returns the typed metadata-first acquisition behind the source object.
    pub const fn acquisition(&self) -> &BeaDatasetAcquisition {
        &self.acquisition
    }
    /// Consumes the rich output without discarding either the batch or capture evidence.
    pub fn into_parts(self) -> (DiscoveryBatch, BeaDatasetAcquisition) {
        (self.batch, self.acquisition)
    }

    /// Consumes discovery into its batch and exact ordered `MSJ1`-ready response materials.
    pub fn into_sealing_parts(
        self,
    ) -> Result<(DiscoveryBatch, Vec<ProviderCaptureMaterial>), BeaSourceError> {
        Ok((self.batch, self.acquisition.into_capture_materials()?))
    }
}

/// Rich extraction output requiring raw sealing before any later canonical publication.
#[derive(Debug)]
pub struct BeaCapturedExtraction {
    batch: ExtractionBatch,
    acquisition: BeaDatasetAcquisition,
}

impl BeaCapturedExtraction {
    /// Returns the source-native typed extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }
    /// Returns the typed metadata-first acquisition behind this batch.
    pub const fn acquisition(&self) -> &BeaDatasetAcquisition {
        &self.acquisition
    }
    /// Consumes the rich output without discarding either the batch or capture evidence.
    pub fn into_parts(self) -> (ExtractionBatch, BeaDatasetAcquisition) {
        (self.batch, self.acquisition)
    }

    /// Consumes extraction into source-native rows and exact ordered `MSJ1`-ready materials.
    ///
    /// The batch is not canonical publication authority. App composition must seal every returned
    /// material, then map/publish under the shared canonical and point-in-time contracts.
    pub fn into_sealing_parts(
        self,
    ) -> Result<(ExtractionBatch, Vec<ProviderCaptureMaterial>), BeaSourceError> {
        Ok((self.batch, self.acquisition.into_capture_materials()?))
    }
}

/// Source configuration, transport, capture, or typed-handoff failure.
#[derive(Debug, Error)]
pub enum BeaSourceError {
    #[error("BEA source metadata is incompatible with the configured profile")]
    InvalidMetadata,
    #[error("BEA source configuration is invalid")]
    InvalidConfiguration,
    #[error("BEA HTTP transport failed")]
    Network,
    #[error("BEA response exceeded its effective byte limit")]
    BodyTooLarge,
    #[error("BEA provider response violated its exact protocol")]
    Protocol,
    #[error("BEA request deadline elapsed")]
    DeadlineExceeded,
    #[error("BEA request was cancelled")]
    Cancelled,
    #[error("BEA local clock is unavailable")]
    Clock,
    #[error("BEA extraction authority became unavailable")]
    Authority,
    #[error("BEA bounded allocation failed")]
    Allocation,
    #[error("BEA typed adapter contract failed: {0}")]
    Adapter(#[from] BeaError),
    #[error("BEA capture receipt failed: {0}")]
    Capture(#[from] ProviderCaptureError),
    #[error("BEA raw capture record failed: {0}")]
    RawCapture(#[from] RawCaptureRecordError),
}

/// Registry-authorized production BEA extraction source.
pub struct BeaSource {
    metadata: SourceMetadata,
    user_id: BeaUserId,
    config: BeaSourceConfig,
    transport: Arc<dyn BeaTransport>,
    response_limit: usize,
    request_timeout: Duration,
    minimum_request_interval: Duration,
    last_request_start: Mutex<Option<tokio::time::Instant>>,
    telemetry: BeaTelemetryState,
}

impl std::fmt::Debug for BeaSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaSource")
            .field("source_id", self.metadata.source_id())
            .field("revision", self.metadata.revision())
            .field("user_id", &"[REDACTED]")
            .field("configured_datasets", &self.config.contracts.len())
            .field("response_limit", &self.response_limit)
            .finish_non_exhaustive()
    }
}

impl BeaSource {
    /// Builds a production source. App composition registers metadata against the product-wide
    /// provider-rate authority; this adapter never creates a private request quota.
    pub fn try_new(
        metadata: SourceMetadata,
        user_id: BeaUserId,
        config: BeaSourceConfig,
    ) -> Result<Self, BeaSourceError> {
        Self::validate_metadata(&metadata, &config, &user_id)?;
        let bounds = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.request_bounds(),
            NetworkAccessPolicy::Denied => return Err(BeaSourceError::InvalidMetadata),
        };
        let transport = Arc::new(ReqwestBeaTransport::try_new(bounds)?);
        Self::try_new_inner(
            metadata,
            user_id,
            config,
            transport,
            BEA_MINIMUM_REQUEST_INTERVAL,
        )
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: SourceMetadata,
        user_id: BeaUserId,
        config: BeaSourceConfig,
        transport: Arc<dyn BeaTransport>,
    ) -> Result<Self, BeaSourceError> {
        Self::validate_metadata(&metadata, &config, &user_id)?;
        Self::try_new_inner(metadata, user_id, config, transport, Duration::ZERO)
    }

    fn try_new_inner(
        metadata: SourceMetadata,
        user_id: BeaUserId,
        config: BeaSourceConfig,
        transport: Arc<dyn BeaTransport>,
        minimum_request_interval: Duration,
    ) -> Result<Self, BeaSourceError> {
        let bounds = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.request_bounds(),
            NetworkAccessPolicy::Denied => return Err(BeaSourceError::InvalidMetadata),
        };
        let response_limit = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| BeaSourceError::InvalidMetadata)?
            .min(config.parse_limits.max_bytes())
            .min(
                usize::try_from(MAX_PROVIDER_CAPTURE_PAGE_BYTES)
                    .map_err(|_| BeaSourceError::InvalidMetadata)?,
            );
        if response_limit == 0 {
            return Err(BeaSourceError::InvalidMetadata);
        }
        Ok(Self {
            metadata,
            user_id,
            config,
            transport,
            response_limit,
            request_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            minimum_request_interval,
            last_request_start: Mutex::new(None),
            telemetry: BeaTelemetryState::default(),
        })
    }

    fn validate_metadata(
        metadata: &SourceMetadata,
        config: &BeaSourceConfig,
        user_id: &BeaUserId,
    ) -> Result<(), BeaSourceError> {
        if metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "bea"
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || metadata.capabilities().live()
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::Historical
            || !matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        {
            return Err(BeaSourceError::InvalidMetadata);
        }
        let budget = metadata
            .budget_policy()
            .ok_or(BeaSourceError::InvalidMetadata)?;
        let has_application_window = (0..budget.window_count()).any(|index| {
            budget.window(index).is_some_and(|window| {
                window.requests_per_window() <= BEA_APPLICATION_REQUESTS_PER_MINUTE
                    && window.window_nanos() == 60_000_000_000
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        });
        if budget.scope().as_source_identifier() != metadata.provider()
            || budget.scope().authorization_account().is_none()
            || budget.max_concurrent() != 1
            || !has_application_window
        {
            return Err(BeaSourceError::InvalidMetadata);
        }
        for contract in config.contracts() {
            for request in contract.metadata_requests()? {
                authorize_configured_target(metadata, &request, user_id)?;
            }
            let generation = BeaMetadataGeneration::from_response_digests(&[[1; 32]])?;
            authorize_configured_target(metadata, &contract.data_request(generation)?, user_id)?;
        }
        Ok(())
    }

    /// Returns the immutable configured contract set.
    pub const fn config(&self) -> &BeaSourceConfig {
        &self.config
    }

    /// Returns a lock-free saturating telemetry snapshot.
    pub fn telemetry(&self) -> BeaSourceTelemetry {
        self.telemetry.snapshot()
    }

    /// Returns the storage-safe analytical identity for a configured provider request.
    pub fn analytical_dataset_identifier(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<SourceIdentifier, BeaSourceError> {
        self.config
            .contract(provider_dataset)
            .map(|contract| contract.analytical_dataset_id.clone())
            .ok_or(BeaSourceError::InvalidConfiguration)
    }

    /// Acquires and validates the exact metadata sequence used to construct `GetData`.
    pub async fn acquire_metadata(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaMetadataBundle, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        let requests = contract.metadata_requests().map_err(map_source_error)?;
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(requests.len())
            .map_err(|_| map_source_error(BeaSourceError::Allocation))?;
        for request in requests {
            let fetched = self
                .fetch(authority, &request, deadline, cancellation.clone())
                .await?;
            let page = parse_metadata_page(
                &fetched.response.body,
                &request,
                &self.user_id,
                self.effective_parse_limits(),
            )
            .map_err(|error| {
                self.record_parse_failure(&error);
                map_source_error(BeaSourceError::Adapter(error))
            })?;
            let telemetry = response_telemetry(&request, &page.receipt(), &fetched.response)?;
            self.record_page(&telemetry, page.records().len(), false);
            pages.push(BeaCapturedMetadataPage {
                request,
                page,
                material: fetched.material,
                telemetry,
            });
        }
        validate_metadata_bundle(contract, &pages).map_err(map_source_error)?;
        let response_digests = pages
            .iter()
            .map(|page| page.page.receipt().response_digest())
            .collect::<Vec<_>>();
        let generation = BeaMetadataGeneration::from_response_digests(&response_digests)
            .map_err(|error| map_source_error(error.into()))?;
        Ok(BeaMetadataBundle {
            dataset_id: provider_dataset.clone(),
            pages,
            generation,
        })
    }

    /// Acquires typed `GetData` rows against the exact metadata generation.
    pub async fn acquire_data(
        &self,
        authority: &ExtractionAuthority,
        metadata: &BeaMetadataBundle,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaCapturedDataPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(metadata.dataset_id())
            .ok_or_else(invalid_protocol)?;
        let request = contract
            .data_request(metadata.generation())
            .map_err(map_source_error)?;
        let fetched = self
            .fetch(authority, &request, deadline, cancellation)
            .await?;
        let page = parse_data_page(
            &fetched.response.body,
            &request,
            &self.user_id,
            self.effective_parse_limits(),
        )
        .map_err(|error| {
            self.record_parse_failure(&error);
            map_source_error(BeaSourceError::Adapter(error))
        })?;
        if page.metadata_generation() != metadata.generation() {
            return Err(invalid_protocol());
        }
        let telemetry = response_telemetry(&request, page.receipt(), &fetched.response)?;
        self.record_page(&telemetry, 0, true);
        Ok(BeaCapturedDataPage {
            request,
            page,
            material: fetched.material,
            telemetry,
        })
    }

    /// Runs the complete metadata-first acquisition and rejects known partial row sets.
    pub async fn acquire_dataset(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaDatasetAcquisition, ExtractionSourceError> {
        let metadata = self
            .acquire_metadata(authority, provider_dataset, deadline, cancellation.clone())
            .await?;
        let data = self
            .acquire_data(authority, &metadata, deadline, cancellation)
            .await?;
        if data.page().receipt().completeness() == BeaCompleteness::Partial {
            return Err(invalid_protocol());
        }
        Ok(BeaDatasetAcquisition { metadata, data })
    }

    /// Captures discovery with every exact metadata/data response still available for sealing.
    pub async fn discover_captured(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<BeaCapturedDiscovery, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some() || request.max_results() != 1 {
            return Err(invalid_protocol());
        }
        let contract = self
            .config
            .contract(request.dataset())
            .ok_or_else(invalid_protocol)?;
        let acquisition = self
            .acquire_dataset(
                &authority,
                request.dataset(),
                request.deadline(),
                cancellation,
            )
            .await?;
        let object = source_object(&self.metadata, &request, contract, &acquisition)?;
        let batch = DiscoveryBatch::try_new(&request, vec![object])?;
        Ok(BeaCapturedDiscovery { batch, acquisition })
    }

    /// Captures a source-native typed batch and the raw material required before publication.
    pub async fn extract_captured(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<BeaCapturedExtraction, ExtractionSourceError> {
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
        let expected = ParsedObjectId::parse(request.object().object_id())?;
        if expected.contract_digest
            != contract_digest(
                &contract.dataset,
                &contract.parameters,
                contract.expected_rows,
            )
            .map_err(map_source_error)?
        {
            return Err(invalid_protocol());
        }
        let acquisition = self
            .acquire_dataset(
                &authority,
                request.object().dataset(),
                request.deadline(),
                cancellation,
            )
            .await?;
        verify_acquisition(&request, &expected, &acquisition)?;
        let records = native_records(&request, acquisition.data())?;
        let batch = ExtractionBatch::try_new(&request, records)?;
        Ok(BeaCapturedExtraction { batch, acquisition })
    }

    async fn fetch(
        &self,
        authority: &ExtractionAuthority,
        request: &BeaRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedResponse, ExtractionSourceError> {
        self.validate_authority(authority)?;
        self.pace(deadline, cancellation.clone()).await?;
        let authorized = request
            .authorize(&self.user_id)
            .map_err(|error| map_source_error(BeaSourceError::Adapter(error)))?;
        let permit = acquire_request_permit(
            authority,
            authorized.expose_url(),
            deadline,
            cancellation.clone(),
        )
        .await?;
        let in_flight = permit.authorize_send(authorized.expose_url())?;
        let now = system_timestamp().map_err(map_source_error)?;
        let timeout = remaining_timeout(deadline, now, self.request_timeout)?;
        let result = self
            .transport
            .execute(
                authorized,
                &in_flight,
                self.response_limit,
                timeout,
                cancellation,
            )
            .await;
        self.telemetry.add(&self.telemetry.requests, 1);
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                self.telemetry.add(&self.telemetry.failures, 1);
                return Err(map_source_error(error));
            }
        };
        let response_bytes = u64::try_from(response.body.len()).map_err(|_| invalid_protocol())?;
        in_flight.validate_response_size(response_bytes)?;
        self.telemetry
            .add(&self.telemetry.response_bytes, response_bytes);
        self.telemetry.add(
            &self.telemetry.latency_nanos,
            duration_nanos(response.latency),
        );
        if response.retry_after.is_some() {
            self.telemetry.add(&self.telemetry.retry_after_responses, 1);
        }
        if response.retry_after.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > MAX_RETRY_AFTER_BYTES
                || value.iter().any(u8::is_ascii_control)
        }) {
            self.telemetry.add(&self.telemetry.failures, 1);
            return Err(invalid_protocol());
        }
        match response.status {
            200 => {}
            401 | 403 => {
                self.telemetry.add(&self.telemetry.failures, 1);
                return Err(SourceError::Unauthorized.into());
            }
            429 | 503 => {
                self.telemetry
                    .add(&self.telemetry.rate_limited_responses, 1);
                let wait =
                    in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
                return Err(SourceError::BudgetWaitUntil { deadline: wait }.into());
            }
            _ => {
                self.telemetry.add(&self.telemetry.failures, 1);
                return Err(SourceError::ProviderUnavailable.into());
            }
        }
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type.as_deref())
        {
            self.telemetry.add(&self.telemetry.failures, 1);
            return Err(invalid_protocol());
        }
        let request_identity = evidence_digest(request.request_digest());
        let body_digest = evidence_digest(Sha256::digest(&response.body).into());
        let capture = ProviderCaptureSetReceipt::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            capture_dataset_identity(request)?,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![
                ProviderCapturePageReceipt::try_new(
                    0,
                    request_identity,
                    None,
                    None,
                    response.status,
                    response_bytes,
                    body_digest,
                    response.received_at,
                )
                .map_err(map_capture_error)?,
            ],
        )
        .map_err(map_capture_error)?;
        let received_at = DateTime::<Utc>::from_timestamp_nanos(response.received_at.unix_nanos());
        let record = RawCaptureRecord::try_new_live(
            capture_uuid(b"event", &capture),
            Arc::from(self.metadata.source_id().as_str()),
            capture_uuid(b"connection", &capture),
            Some(0),
            None,
            received_at,
            response.body.clone(),
        )
        .map_err(|error| map_source_error(BeaSourceError::RawCapture(error)))?;
        let material =
            ProviderCaptureMaterial::try_new(capture, vec![record]).map_err(map_capture_error)?;
        in_flight.release();
        Ok(FetchedResponse { response, material })
    }

    async fn pace(
        &self,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<(), ExtractionSourceError> {
        let lock_timeout =
            deadline_remaining(deadline, system_timestamp().map_err(map_source_error)?)?;
        let mut last = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(ExtractionSourceError::Cancelled);
            }
            result = tokio::time::timeout(lock_timeout, self.last_request_start.lock()) => {
                result.map_err(|_| ExtractionSourceError::DeadlineExceeded)?
            }
        };
        if let Some(previous) = *last {
            let ready = previous + self.minimum_request_interval;
            let wait = ready.saturating_duration_since(tokio::time::Instant::now());
            if !wait.is_zero() {
                let now = system_timestamp().map_err(map_source_error)?;
                let remaining = deadline
                    .unix_nanos()
                    .checked_sub(now.unix_nanos())
                    .and_then(|value| u64::try_from(value).ok())
                    .map(Duration::from_nanos)
                    .ok_or(ExtractionSourceError::DeadlineExceeded)?;
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
        }
        *last = Some(tokio::time::Instant::now());
        Ok(())
    }

    fn record_page(&self, telemetry: &BeaResponseTelemetry, metadata_records: usize, data: bool) {
        self.telemetry.add(&self.telemetry.successful_responses, 1);
        self.telemetry.add(
            &self.telemetry.metadata_records,
            u64::try_from(metadata_records).unwrap_or(u64::MAX),
        );
        if data {
            self.telemetry.add(
                &self.telemetry.requested_rows,
                telemetry.requested_rows.unwrap_or(0),
            );
            self.telemetry
                .add(&self.telemetry.returned_rows, telemetry.returned_rows);
            self.telemetry.add(
                &self.telemetry.missing_rows,
                telemetry.missing_rows.unwrap_or(0),
            );
            if telemetry.completeness == BeaCompleteness::ExpectedCountUnknown {
                self.telemetry
                    .add(&self.telemetry.unknown_completeness_pages, 1);
            }
        }
    }

    fn record_parse_failure(&self, error: &BeaError) {
        self.telemetry.add(&self.telemetry.failures, 1);
        if matches!(error, BeaError::Provider(_)) {
            self.telemetry.add(&self.telemetry.provider_errors, 1);
        }
    }

    fn effective_parse_limits(&self) -> BeaParseLimits {
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

impl SourceMetadataProvider for BeaSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for BeaSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            Ok(self
                .discover_captured(authority, request, cancellation)
                .await?
                .into_parts()
                .0)
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(async move {
            Ok(self
                .extract_captured(authority, request, cancellation)
                .await?
                .into_parts()
                .0)
        })
    }
}

struct FetchedResponse {
    response: BeaHttpResponse,
    material: ProviderCaptureMaterial,
}

fn validate_metadata_bundle(
    contract: &BeaDatasetContract,
    pages: &[BeaCapturedMetadataPage],
) -> Result<(), BeaSourceError> {
    if pages.len() != contract.parameters.len().saturating_add(2) {
        return Err(BeaSourceError::Protocol);
    }
    let datasets = match pages.first().map(|page| page.page.records()) {
        Some(BeaMetadataRecords::Datasets(datasets)) => datasets,
        _ => return Err(BeaSourceError::Protocol),
    };
    if !datasets
        .iter()
        .any(|dataset| dataset.identity() == &contract.dataset)
    {
        return Err(BeaSourceError::Protocol);
    }
    let definitions = match pages.get(1).map(|page| page.page.records()) {
        Some(BeaMetadataRecords::Parameters(definitions)) => definitions,
        _ => return Err(BeaSourceError::Protocol),
    };
    for definition in definitions {
        if definition.is_required()
            && !contract.parameters.contains_key(definition.identity())
            && definition.default_value().is_none()
        {
            return Err(BeaSourceError::Protocol);
        }
    }
    for ((parameter, selected), page) in contract.parameters.iter().zip(pages.iter().skip(2)) {
        let definition = definition(definitions, parameter).ok_or(BeaSourceError::Protocol)?;
        if !definition.accepts_multiple_values() && selected.split(',').count() != 1 {
            return Err(BeaSourceError::Protocol);
        }
        let values = match page.page.records() {
            BeaMetadataRecords::ParameterValues(values) => values,
            _ => return Err(BeaSourceError::Protocol),
        };
        for value in selected.split(',') {
            let admitted_all = definition
                .all_value()
                .is_some_and(|all| all.eq_ignore_ascii_case(value));
            if !admitted_all && !values.iter().any(|candidate| candidate.key() == value) {
                return Err(BeaSourceError::Protocol);
            }
        }
    }
    Ok(())
}

fn definition<'a>(
    definitions: &'a [BeaParameterDefinition],
    identity: &BeaParameterIdentity,
) -> Option<&'a BeaParameterDefinition> {
    definitions
        .iter()
        .find(|definition| definition.identity() == identity)
}

fn source_object(
    metadata: &SourceMetadata,
    request: &DiscoveryRequest,
    contract: &BeaDatasetContract,
    acquisition: &BeaDatasetAcquisition,
) -> Result<SourceObject, ExtractionSourceError> {
    let data = acquisition.data();
    let capture = data.material().receipt();
    let object_id = object_id(contract, acquisition.metadata().generation(), capture)?;
    let received_at = capture
        .pages()
        .first()
        .ok_or_else(invalid_protocol)?
        .received_at();
    let effective = EffectiveInterval::new(received_at, None).map_err(|_| invalid_protocol())?;
    let published_at = data.page().production_time().map(|value| value.timestamp());
    SourceObject::try_new_with_capture_identity(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        SourceIdentifier::try_from(BEA_JSON_MEDIA_TYPE).map_err(|_| invalid_protocol())?,
        ExactPayloadEvidence::from_content_digest(capture.content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(capture).map_err(map_capture_error)?,
        effective,
        published_at,
        market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        Some(capture.total_body_bytes()),
    )
    .map_err(Into::into)
}

#[derive(Debug)]
struct ParsedObjectId {
    contract_digest: [u8; 32],
    metadata_digest: [u8; 32],
    capture_digest: [u8; 32],
}

impl ParsedObjectId {
    fn parse(value: &SourceIdentifier) -> Result<Self, ExtractionSourceError> {
        let mut parts = value.as_str().split(':');
        if parts.next() != Some("bea") || parts.next() != Some("object-v1") {
            return Err(invalid_protocol());
        }
        let contract_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        let metadata_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        let capture_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        if parts.next().is_some() {
            return Err(invalid_protocol());
        }
        Ok(Self {
            contract_digest,
            metadata_digest,
            capture_digest,
        })
    }
}

fn object_id(
    contract: &BeaDatasetContract,
    generation: BeaMetadataGeneration,
    capture: &ProviderCaptureSetReceipt,
) -> Result<SourceIdentifier, ExtractionSourceError> {
    let contract = contract_digest(
        &contract.dataset,
        &contract.parameters,
        contract.expected_rows,
    )
    .map_err(map_source_error)?;
    SourceIdentifier::try_from(format!(
        "bea:object-v1:{}:{}:{}",
        lower_hex(contract),
        lower_hex(generation.digest()),
        lower_hex(capture.content_digest().bytes()),
    ))
    .map_err(|_| invalid_protocol())
}

fn verify_acquisition(
    request: &ExtractionRequest,
    expected: &ParsedObjectId,
    acquisition: &BeaDatasetAcquisition,
) -> Result<(), ExtractionSourceError> {
    let capture = acquisition.data().material().receipt();
    if expected.metadata_digest != acquisition.metadata().generation().digest()
        || expected.capture_digest != capture.content_digest().bytes()
        || request.object().evidence().content_digest() != capture.content_digest()
        || request.object().expected_bytes() != Some(capture.total_body_bytes())
        || request.object().capture_identity()
            != SourceObjectCaptureIdentity::try_from_capture(capture).map_err(map_capture_error)?
    {
        return Err(SourceError::GenerationResynchronizationRequired.into());
    }
    Ok(())
}

fn native_records(
    request: &ExtractionRequest,
    captured: &BeaCapturedDataPage,
) -> Result<Vec<ExtractionRecord>, ExtractionSourceError> {
    if captured.page().observations().len() > request.max_records() as usize {
        return Err(
            market_squawk_sources::ExtractionError::RecordLimitExceeded {
                requested: request.max_records(),
            }
            .into(),
        );
    }
    let schema =
        SourceIdentifier::try_from(BEA_NATIVE_EXTRACTION_SCHEMA).map_err(|_| invalid_protocol())?;
    let received_at = captured
        .material()
        .receipt()
        .pages()
        .first()
        .ok_or_else(invalid_protocol)?
        .received_at();
    captured
        .page()
        .observations()
        .iter()
        .enumerate()
        .map(|(index, observation)| {
            let version =
                crate::BeaObservedVersion::try_from_page(captured.page(), index, received_at)
                    .map_err(|error| map_source_error(error.into()))?;
            let payload = native_payload(captured.request(), captured.page(), observation)?;
            let evidence = ExactPayloadEvidence::from_content_digest(evidence_digest(
                Sha256::digest(&payload).into(),
            ));
            let revision = SourceIdentifier::try_from(format!(
                "bea-version:{}",
                lower_hex(version.version_digest())
            ))
            .map_err(|_| invalid_protocol())?;
            ExtractionRecord::try_new_with_time(
                request,
                schema.clone(),
                evidence,
                effective_coordinate(observation)?,
                captured
                    .page()
                    .production_time()
                    .map(|time| ResearchTemporalCoordinate::exact(time.timestamp())),
                market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                    observed_at: received_at,
                },
                revision,
                None,
                payload,
            )
            .map_err(Into::into)
        })
        .collect()
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct NativeObservationWire<'a> {
    schema: &'static str,
    provider_method: &'static str,
    dataset: &'a str,
    parameters: Vec<NativeParameterWire<'a>>,
    metadata_generation: String,
    request_identity: String,
    completeness: &'static str,
    table: Option<&'a str>,
    line: Option<&'a str>,
    dimensions: &'a BTreeMap<String, String>,
    period: &'a str,
    frequency: &'static str,
    value: Option<String>,
    raw_value: Option<&'a str>,
    missing: Option<&'static str>,
    cl_unit: &'a str,
    unit_multiplier: i16,
    note_references: &'a [String],
    notes: Vec<NativeNoteWire<'a>>,
    result_attributes: &'a BTreeMap<String, String>,
    production_time: Option<&'a str>,
    observation_digest: String,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct NativeParameterWire<'a> {
    name: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct NativeNoteWire<'a> {
    reference: &'a str,
    text: &'a str,
}

fn native_payload(
    request: &BeaRequest,
    page: &BeaDataPage,
    observation: &BeaObservation,
) -> Result<Bytes, ExtractionSourceError> {
    let (value, raw_value, missing) = match observation.value() {
        BeaObservationValue::Observed { value, raw } => {
            (Some(value.to_string()), Some(raw.as_str()), None)
        }
        BeaObservationValue::Missing(BeaMissingValue::Absent) => (None, None, Some("absent")),
        BeaObservationValue::Missing(BeaMissingValue::Blank) => (None, None, Some("blank")),
    };
    let mut parameters = Vec::new();
    parameters
        .try_reserve_exact(request.query().supplied_parameters().len())
        .map_err(|_| invalid_protocol())?;
    parameters.extend(
        request
            .query()
            .supplied_parameters()
            .iter()
            .map(|(name, value)| NativeParameterWire {
                name: name.as_str(),
                value,
            }),
    );
    let mut notes = Vec::new();
    notes
        .try_reserve_exact(page.notes().len())
        .map_err(|_| invalid_protocol())?;
    notes.extend(page.notes().iter().filter_map(|note| {
        (note.reference().is_empty()
            || observation
                .note_references()
                .iter()
                .any(|reference| reference == note.reference()))
        .then_some(NativeNoteWire {
            reference: note.reference(),
            text: note.text(),
        })
    }));
    let wire = NativeObservationWire {
        schema: BEA_NATIVE_EXTRACTION_SCHEMA,
        provider_method: request.query().method().as_str(),
        dataset: observation.identity().dataset().as_str(),
        parameters,
        metadata_generation: lower_hex(page.metadata_generation().digest()),
        request_identity: lower_hex(request.request_digest()),
        completeness: match page.receipt().completeness() {
            BeaCompleteness::Complete => "complete",
            BeaCompleteness::Partial => "partial",
            BeaCompleteness::ExpectedCountUnknown => "expected_count_unknown",
        },
        table: observation.identity().table(),
        line: observation.identity().line(),
        dimensions: observation.identity().dimensions(),
        period: observation.period().raw(),
        frequency: match observation.period().frequency() {
            BeaFrequency::Annual => "annual",
            BeaFrequency::Quarterly => "quarterly",
            BeaFrequency::Monthly => "monthly",
        },
        value,
        raw_value,
        missing,
        cl_unit: observation.unit().cl_unit(),
        unit_multiplier: observation.unit().unit_multiplier(),
        note_references: observation.note_references(),
        notes,
        result_attributes: page.result_attributes(),
        production_time: page.production_time().map(|time| time.raw()),
        observation_digest: lower_hex(observation.digest()),
    };
    serde_json::to_vec(&wire)
        .map(Bytes::from)
        .map_err(|_| invalid_protocol())
}

fn effective_coordinate(
    observation: &BeaObservation,
) -> Result<ResearchTemporalCoordinate, ExtractionSourceError> {
    let scheme = match observation.period().frequency() {
        BeaFrequency::Annual => "bea-annual",
        BeaFrequency::Quarterly => "bea-quarterly",
        BeaFrequency::Monthly => "bea-monthly",
    };
    let period = ResearchPeriod::try_new(
        SourceIdentifier::try_from(scheme).map_err(|_| invalid_protocol())?,
        observation.period().year(),
        NonZeroU16::new(u16::from(observation.period().ordinal())).ok_or_else(invalid_protocol)?,
        SourceIdentifier::try_from(observation.period().raw()).map_err(|_| invalid_protocol())?,
    )
    .map_err(|_| invalid_protocol())?;
    Ok(ResearchTemporalCoordinate::source_period(period))
}

fn response_telemetry(
    request: &BeaRequest,
    receipt: &crate::BeaPageReceipt,
    response: &BeaHttpResponse,
) -> Result<BeaResponseTelemetry, ExtractionSourceError> {
    Ok(BeaResponseTelemetry {
        request_identity: evidence_digest(request.request_digest()),
        method: request.query().method(),
        status: response.status,
        response_bytes: u64::try_from(response.body.len()).map_err(|_| invalid_protocol())?,
        latency_nanos: duration_nanos(response.latency),
        retry_after: response.retry_after.clone().map(Vec::into_boxed_slice),
        page_number: receipt.page_number(),
        page_count: receipt.page_count(),
        requested_rows: receipt
            .requested_rows()
            .map(u64::try_from)
            .transpose()
            .map_err(|_| invalid_protocol())?,
        returned_rows: u64::try_from(receipt.returned_rows()).map_err(|_| invalid_protocol())?,
        missing_rows: receipt
            .missing_rows()
            .map(u64::try_from)
            .transpose()
            .map_err(|_| invalid_protocol())?,
        completeness: receipt.completeness(),
    })
}

fn authorize_configured_target(
    metadata: &SourceMetadata,
    request: &BeaRequest,
    user_id: &BeaUserId,
) -> Result<(), BeaSourceError> {
    let authorized = request.authorize(user_id)?;
    metadata
        .network_policy()
        .authorize(authorized.expose_url())
        .map_err(|_| BeaSourceError::InvalidMetadata)
}

fn capture_dataset_identity(
    request: &BeaRequest,
) -> Result<SourceIdentifier, ExtractionSourceError> {
    match request.query().dataset() {
        Some(dataset) => {
            SourceIdentifier::try_from(dataset.as_str()).map_err(|_| invalid_protocol())
        }
        None if request.query().method() == BeaMethod::GetDatasetList => {
            SourceIdentifier::try_from("BEAAPI.GetDatasetList").map_err(|_| invalid_protocol())
        }
        None => Err(invalid_protocol()),
    }
}

async fn acquire_request_permit(
    authority: &ExtractionAuthority,
    target: &str,
    wall_deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<ExtractionRequestPermit, ExtractionSourceError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        match authority.try_network_request(target) {
            Ok(permit) => return Ok(permit),
            Err(ExtractionAuthorityError::BudgetWaitUntil { deadline }) => {
                let wait = authority.remaining_budget_wait(deadline)?;
                let now = system_timestamp().map_err(map_source_error)?;
                let remaining = wall_deadline
                    .unix_nanos()
                    .checked_sub(now.unix_nanos())
                    .and_then(|value| u64::try_from(value).ok())
                    .map(Duration::from_nanos)
                    .ok_or(ExtractionSourceError::DeadlineExceeded)?;
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
    Ok(configured.min(deadline_remaining(deadline, now)?))
}

fn deadline_remaining(
    deadline: Timestamp,
    now: Timestamp,
) -> Result<Duration, ExtractionSourceError> {
    deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .map(Duration::from_nanos)
        .filter(|remaining| !remaining.is_zero())
        .ok_or(ExtractionSourceError::DeadlineExceeded)
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value.is_some_and(|value| {
        value
            .split(|byte| *byte == b';')
            .next()
            .is_some_and(|media| media.trim_ascii().eq_ignore_ascii_case(b"application/json"))
    })
}

fn contract_digest(
    dataset: &BeaDatasetIdentity,
    parameters: &BTreeMap<BeaParameterIdentity, String>,
    expected_rows: Option<usize>,
) -> Result<[u8; 32], BeaSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bea-data-contract/v1");
    hash_text(&mut hash, dataset.as_str())?;
    hash.update(
        u64::try_from(parameters.len())
            .map_err(|_| BeaSourceError::InvalidConfiguration)?
            .to_be_bytes(),
    );
    for (name, value) in parameters {
        hash_text(&mut hash, name.as_str())?;
        hash_text(&mut hash, value)?;
    }
    match expected_rows {
        Some(rows) => {
            hash.update([1]);
            hash.update(
                u64::try_from(rows)
                    .map_err(|_| BeaSourceError::InvalidConfiguration)?
                    .to_be_bytes(),
            );
        }
        None => hash.update([0]),
    }
    Ok(hash.finalize().into())
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), BeaSourceError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| BeaSourceError::InvalidConfiguration)?
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
    Ok(())
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn capture_uuid(tag: &[u8], capture: &ProviderCaptureSetReceipt) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bea-raw-capture-id/v1");
    hash.update(u64::try_from(tag.len()).unwrap_or(u64::MAX).to_be_bytes());
    hash.update(tag);
    hash.update(capture.request_set_identity().bytes());
    hash.update(capture.content_digest().bytes());
    hash.update(capture.observation_digest().bytes());
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn duration_nanos(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn parse_hex(value: &str) -> Result<[u8; 32], ExtractionSourceError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_protocol());
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Result<u8, ExtractionSourceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_protocol()),
    }
}

fn invalid_protocol() -> ExtractionSourceError {
    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
}

fn map_source_error(error: BeaSourceError) -> ExtractionSourceError {
    match error {
        BeaSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        BeaSourceError::Cancelled => ExtractionSourceError::Cancelled,
        BeaSourceError::Network | BeaSourceError::Clock => SourceError::Network.into(),
        BeaSourceError::BodyTooLarge
        | BeaSourceError::Protocol
        | BeaSourceError::InvalidMetadata
        | BeaSourceError::InvalidConfiguration
        | BeaSourceError::Authority
        | BeaSourceError::Allocation
        | BeaSourceError::Adapter(_)
        | BeaSourceError::Capture(_)
        | BeaSourceError::RawCapture(_) => invalid_protocol(),
    }
}

fn map_capture_error(error: ProviderCaptureError) -> ExtractionSourceError {
    map_source_error(BeaSourceError::Capture(error))
}
