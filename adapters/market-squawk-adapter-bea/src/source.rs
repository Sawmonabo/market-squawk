//! Registry-authorized BEA source composition and capture-ready typed handoff.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
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
    InFlightExtractionRequest, MAX_PROVIDER_CAPTURE_PAGE_BYTES, NetworkAccessPolicy,
    NetworkPolicyError, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCapturePageReceipt,
    ProviderCaptureSealRequest, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderRateDeclaration, ProviderRateResponseClass, ProviderRateResponseSettlement,
    ProviderRateRetryAfterDisposition, ProviderRateWeightedDimension, ProviderRateWeightedWindow,
    QueryParameterRule, QuerySensitivity, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider, SourceObject, SourceObjectCaptureIdentity, SourceProtocolProfile,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::sealed::bea_capture_graph_identity;
use crate::transport::{
    BeaHttpResponse, BeaRetainedResponseHeaders, BeaTransport, ReqwestBeaTransport,
    system_timestamp,
};
use crate::{
    BEA_API_ENDPOINT, BEA_APPLICATION_ERRORS_PER_MINUTE, BEA_APPLICATION_REQUESTS_PER_MINUTE,
    BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE, BEA_MINIMUM_REQUEST_INTERVAL, BeaCompleteness,
    BeaDataPage, BeaDatasetIdentity, BeaDoctorAdmissionEvidence, BeaDoctorRun, BeaError,
    BeaFrequency, BeaMetadataGeneration, BeaMetadataPage, BeaMetadataRecords, BeaMethod,
    BeaMissingValue, BeaObservation, BeaObservationValue, BeaParameterDefinition,
    BeaParameterIdentity, BeaParseLimits, BeaProviderQuotaDeclaration, BeaQuery, BeaRequest,
    BeaSourceBinding, BeaUserId, bea_provider_quota_declaration,
};

/// Maximum explicit BEA data-query contracts retained by one adapter instance.
pub const MAX_BEA_CONFIGURED_DATASETS: usize = 64;
/// Source-native, non-canonical extraction payload schema.
pub const BEA_NATIVE_EXTRACTION_SCHEMA: &str = "market-squawk-bea-native-v1";

const BEA_DATASET_PREFIX: &str = "bea:data-v1:";
const BEA_ANALYTICAL_PREFIX: &str = "bea.data-v1.";
const BEA_JSON_MEDIA_TYPE: &str = "application/json";
const MAX_RETRY_AFTER_BYTES: usize = 256;
const BEA_METADATA_DISCOVERY_DAG_SCHEMA: &[u8] = b"market-squawk/bea-metadata-discovery-dag/v2";

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

    fn metadata_root_requests(&self) -> Result<[BeaRequest; 2], BeaSourceError> {
        Ok([
            BeaQuery::dataset_list()?.single_page(None)?,
            BeaQuery::parameter_list(self.dataset.clone())?.single_page(None)?,
        ])
    }

    fn parameter_value_requests(
        &self,
        definitions: &[BeaParameterDefinition],
    ) -> Result<Vec<BeaRequest>, BeaSourceError> {
        for parameter in self.parameters.keys() {
            if definition(definitions, parameter).is_none() {
                return Err(BeaSourceError::Protocol);
            }
        }
        let regional = self.dataset.as_str().eq_ignore_ascii_case("Regional");
        let line_code = self.parameter_named("LineCode");
        let year = self.parameter_named("Year");
        let mut ordered = Vec::new();
        ordered
            .try_reserve_exact(self.parameters.len())
            .map_err(|_| BeaSourceError::Allocation)?;
        for parameter in self.parameters.keys() {
            if regional
                && (line_code.is_some_and(|value| value == parameter)
                    || year.is_some_and(|value| value == parameter))
            {
                continue;
            }
            ordered.push(
                BeaQuery::parameter_values(self.dataset.clone(), parameter.clone())?
                    .single_page(None)?,
            );
        }
        if regional && let Some(target) = line_code {
            let table_name = self
                .selected_parameter("TableName")
                .ok_or(BeaSourceError::Protocol)?;
            ordered.push(
                BeaQuery::parameter_values_filtered(
                    self.dataset.clone(),
                    target.clone(),
                    BTreeMap::from([(table_name.0.clone(), table_name.1.to_owned())]),
                )?
                .single_page(None)?,
            );
        }
        if regional && let Some(target) = year {
            let table_name = self
                .selected_parameter("TableName")
                .ok_or(BeaSourceError::Protocol)?;
            let geo_fips = self
                .selected_parameter("GeoFips")
                .ok_or(BeaSourceError::Protocol)?;
            ordered.push(
                BeaQuery::parameter_values_filtered(
                    self.dataset.clone(),
                    target.clone(),
                    BTreeMap::from([
                        (table_name.0.clone(), table_name.1.to_owned()),
                        (geo_fips.0.clone(), geo_fips.1.to_owned()),
                    ]),
                )?
                .single_page(None)?,
            );
        }
        if ordered.len() != self.parameters.len() {
            return Err(BeaSourceError::Protocol);
        }
        Ok(ordered)
    }

    fn metadata_policy_requests(&self) -> Result<Vec<BeaRequest>, BeaSourceError> {
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(self.parameters.len().saturating_add(2))
            .map_err(|_| BeaSourceError::Allocation)?;
        requests.extend(self.metadata_root_requests()?);
        let regional = self.dataset.as_str().eq_ignore_ascii_case("Regional");
        for parameter in self.parameters.keys() {
            let query = if regional && parameter.as_str().eq_ignore_ascii_case("LineCode") {
                let (filter, value) = self
                    .selected_parameter("TableName")
                    .ok_or(BeaSourceError::InvalidConfiguration)?;
                BeaQuery::parameter_values_filtered(
                    self.dataset.clone(),
                    parameter.clone(),
                    BTreeMap::from([(filter.clone(), value.to_owned())]),
                )?
            } else if regional && parameter.as_str().eq_ignore_ascii_case("Year") {
                let (table, table_value) = self
                    .selected_parameter("TableName")
                    .ok_or(BeaSourceError::InvalidConfiguration)?;
                let (geo, geo_value) = self
                    .selected_parameter("GeoFips")
                    .ok_or(BeaSourceError::InvalidConfiguration)?;
                BeaQuery::parameter_values_filtered(
                    self.dataset.clone(),
                    parameter.clone(),
                    BTreeMap::from([
                        (table.clone(), table_value.to_owned()),
                        (geo.clone(), geo_value.to_owned()),
                    ]),
                )?
            } else {
                BeaQuery::parameter_values(self.dataset.clone(), parameter.clone())?
            };
            requests.push(query.single_page(None)?);
        }
        Ok(requests)
    }

    fn parameter_named(&self, name: &str) -> Option<&BeaParameterIdentity> {
        self.parameters
            .keys()
            .find(|parameter| parameter.as_str().eq_ignore_ascii_case(name))
    }

    fn selected_parameter(&self, name: &str) -> Option<(&BeaParameterIdentity, &str)> {
        self.parameters
            .iter()
            .find(|(parameter, _)| parameter.as_str().eq_ignore_ascii_case(name))
            .map(|(parameter, value)| (parameter, value.as_str()))
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

    /// Returns the complete configured dataset-selection and parser-limit commitment.
    pub fn digest(&self) -> Result<EvidenceDigest, BeaSourceError> {
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-source-config/v1");
        hasher.update(BEA_METADATA_DISCOVERY_DAG_SCHEMA);
        hasher.update(
            u64::try_from(self.contracts.len())
                .map_err(|_| BeaSourceError::InvalidConfiguration)?
                .to_be_bytes(),
        );
        for contract in &self.contracts {
            hasher.update(contract_digest(
                &contract.dataset,
                &contract.parameters,
                contract.expected_rows,
            )?);
        }
        for limit in [
            self.parse_limits.max_rows(),
            self.parse_limits.max_metadata_records(),
            self.parse_limits.max_bytes(),
            self.parse_limits.max_string_bytes(),
            self.parse_limits.max_dimensions(),
            self.parse_limits.max_notes(),
        ] {
            hasher.update(
                u64::try_from(limit)
                    .map_err(|_| BeaSourceError::InvalidConfiguration)?
                    .to_be_bytes(),
            );
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hasher.finalize().into(),
        ))
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

/// Builds the exact product-wide weighted provider-rate declaration for one BEA credential realm.
///
/// The collision subject is code-owned and stable; the `UserID` is never hashed into rate-policy
/// state. App composition registers this declaration with `ProviderRateAuthority` and binds every
/// BEA source/doctor/background job to that one allocation.
pub fn bea_provider_rate_declaration() -> Result<ProviderRateDeclaration, BeaSourceError> {
    let provider =
        SourceIdentifier::try_from("bea").map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let subject = ProviderRateDeclaration::governed_provider_subject(&provider)
        .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let pacing_window = ProviderBudgetWindow::try_new(
        NonZeroU32::MIN,
        NonZeroU64::new(
            u64::try_from(BEA_MINIMUM_REQUEST_INTERVAL.as_nanos())
                .map_err(|_| BeaSourceError::InvalidConfiguration)?,
        )
        .ok_or(BeaSourceError::InvalidConfiguration)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let minute_window = ProviderBudgetWindow::try_new(
        NonZeroU32::new(BEA_APPLICATION_REQUESTS_PER_MINUTE)
            .ok_or(BeaSourceError::InvalidConfiguration)?,
        NonZeroU64::new(60_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let response_bytes_window = ProviderRateWeightedWindow::try_new(
        ProviderRateWeightedDimension::ResponseBytes,
        NonZeroU64::new(BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE)
            .ok_or(BeaSourceError::InvalidConfiguration)?,
        NonZeroU64::new(60_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let provider_errors_window = ProviderRateWeightedWindow::try_new(
        ProviderRateWeightedDimension::ProviderErrors,
        NonZeroU64::new(u64::from(BEA_APPLICATION_ERRORS_PER_MINUTE))
            .ok_or(BeaSourceError::InvalidConfiguration)?,
        NonZeroU64::new(60_000_000_000).ok_or(BeaSourceError::InvalidConfiguration)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| BeaSourceError::InvalidConfiguration)?;
    let policy = ProviderBudgetPolicy::try_new_weighted_conjunctive(
        BudgetScope::with_authorization_account(provider, subject.clone()),
        &[pacing_window, minute_window],
        &[response_bytes_window, provider_errors_window],
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

/// Typed metadata response retained after its raw material is handed to the shared sealer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaMetadataEvidencePage {
    request: BeaRequest,
    page: BeaMetadataPage,
    capture: ProviderCaptureSetReceipt,
    telemetry: BeaResponseTelemetry,
}

impl BeaMetadataEvidencePage {
    /// Returns the exact provider request without credential material.
    pub const fn request(&self) -> &BeaRequest {
        &self.request
    }
    /// Returns the closed parsed metadata response.
    pub const fn page(&self) -> &BeaMetadataPage {
        &self.page
    }
    /// Returns the expected provider capture receipt that a physical seal must match exactly.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    /// Returns bounded request/response accounting.
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

/// Typed data response retained after its raw material is handed to the shared sealer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDataEvidencePage {
    request: BeaRequest,
    page: BeaDataPage,
    capture: ProviderCaptureSetReceipt,
    telemetry: BeaResponseTelemetry,
}

impl BeaDataEvidencePage {
    /// Returns the exact provider request without credential material.
    pub const fn request(&self) -> &BeaRequest {
        &self.request
    }
    /// Returns the closed parsed native observations.
    pub const fn page(&self) -> &BeaDataPage {
        &self.page
    }
    /// Returns the expected provider capture receipt that a physical seal must match exactly.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    /// Returns bounded request/response accounting.
    pub const fn telemetry(&self) -> &BeaResponseTelemetry {
        &self.telemetry
    }
}

/// Metadata evidence retained after raw response ownership moves to the shared journal sealer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaMetadataEvidenceBundle {
    dataset_id: SourceIdentifier,
    pages: Vec<BeaMetadataEvidencePage>,
    generation: BeaMetadataGeneration,
}

impl BeaMetadataEvidenceBundle {
    /// Returns the configured provider dataset.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }
    /// Returns metadata pages in exact official request order.
    pub fn pages(&self) -> &[BeaMetadataEvidencePage] {
        &self.pages
    }
    /// Returns the exact metadata-generation commitment used by `GetData`.
    pub const fn generation(&self) -> BeaMetadataGeneration {
        self.generation
    }
}

/// Complete typed BEA acquisition evidence whose exact raw bytes are no longer clonable.
///
/// Root composition receives this value inside a pending seal continuation, seals the combined
/// request graph, then rejoins the exact opaque seal result. Canonical candidate construction
/// accepts only that one-use rejoined proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDatasetEvidence {
    metadata: BeaMetadataEvidenceBundle,
    data: BeaDataEvidencePage,
}

impl BeaDatasetEvidence {
    /// Returns metadata-first typed evidence.
    pub const fn metadata(&self) -> &BeaMetadataEvidenceBundle {
        &self.metadata
    }
    /// Returns the final typed data response.
    pub const fn data(&self) -> &BeaDataEvidencePage {
        &self.data
    }

    pub(crate) fn expected_capture_count(&self) -> usize {
        self.metadata.pages.len().saturating_add(1)
    }

    pub(crate) fn expected_capture(&self, ordinal: usize) -> Option<&ProviderCaptureSetReceipt> {
        self.metadata
            .pages
            .get(ordinal)
            .map(BeaMetadataEvidencePage::capture)
            .or_else(|| (ordinal == self.metadata.pages.len()).then_some(self.data.capture()))
    }

    pub(crate) fn expected_upstream_response_digest(
        &self,
        ordinal: usize,
    ) -> Option<EvidenceDigest> {
        self.metadata
            .pages
            .get(ordinal)
            .map(|page| page.page().receipt().upstream_response_digest())
            .or_else(|| {
                (ordinal == self.metadata.pages.len())
                    .then_some(self.data.page().receipt().upstream_response_digest())
            })
            .map(evidence_digest)
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

    /// Splits typed evidence from one-shot raw material without cloning provider bytes.
    ///
    /// This order is the exact request order and is the intended handoff to the sole `MSJ1`
    /// sealing boundary before any canonical publication can be attempted.
    pub fn into_sealing_parts(
        self,
    ) -> Result<(BeaDatasetEvidence, ProviderCaptureMaterial), BeaSourceError> {
        let Self { metadata, data } = self;
        let mut materials = Vec::new();
        materials
            .try_reserve_exact(metadata.pages.len().saturating_add(1))
            .map_err(|_| BeaSourceError::Allocation)?;
        let mut metadata_evidence = Vec::new();
        metadata_evidence
            .try_reserve_exact(metadata.pages.len())
            .map_err(|_| BeaSourceError::Allocation)?;
        for captured in metadata.pages {
            let BeaCapturedMetadataPage {
                request,
                page,
                material,
                telemetry,
            } = captured;
            let capture = material.receipt().clone();
            materials.push(material);
            metadata_evidence.push(BeaMetadataEvidencePage {
                request,
                page,
                capture,
                telemetry,
            });
        }
        let BeaCapturedDataPage {
            request,
            page,
            material,
            telemetry,
        } = data;
        let capture = material.receipt().clone();
        materials.push(material);
        let evidence = BeaDatasetEvidence {
            metadata: BeaMetadataEvidenceBundle {
                dataset_id: metadata.dataset_id,
                pages: metadata_evidence,
                generation: metadata.generation,
            },
            data: BeaDataEvidencePage {
                request,
                page,
                capture,
                telemetry,
            },
        };
        let mut capture_refs = Vec::new();
        capture_refs
            .try_reserve_exact(materials.len())
            .map_err(|_| BeaSourceError::Allocation)?;
        capture_refs.extend(materials.iter().map(ProviderCaptureMaterial::receipt));
        let graph_identity = bea_capture_graph_identity(
            evidence.metadata().dataset_id(),
            evidence.metadata().generation(),
            &capture_refs,
        )
        .map_err(|_| BeaSourceError::Protocol)?;
        let graph = ProviderCaptureMaterial::try_combine_request_graph(
            evidence.metadata().dataset_id().clone(),
            graph_identity,
            materials,
        )?;
        Ok((evidence, graph))
    }
}

/// Rich discovery output preserving raw material that the trait-only batch cannot carry.
pub struct BeaCapturedDiscovery {
    batch: DiscoveryBatch,
    acquisition: BeaDatasetAcquisition,
    doctor_admission_digest: EvidenceDigest,
    doctor_sealed_graph_digest: EvidenceDigest,
}

impl std::fmt::Debug for BeaCapturedDiscovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaCapturedDiscovery")
            .finish_non_exhaustive()
    }
}

impl BeaCapturedDiscovery {
    /// Hides the discovery batch behind the one-use continuation and exposes only the common seal
    /// request for the complete metadata-first graph.
    pub fn into_sealing_parts(
        self,
    ) -> Result<
        (
            crate::sealed::BeaPendingDiscoverySeal,
            ProviderCaptureSealRequest,
        ),
        BeaSourceError,
    > {
        let (evidence, graph) = self.acquisition.into_sealing_parts()?;
        let (expectation, request) = graph.into_whole_seal_parts();
        Ok((
            crate::sealed::BeaPendingDiscoverySeal::from_source(
                self.batch,
                evidence,
                self.doctor_admission_digest,
                self.doctor_sealed_graph_digest,
                expectation,
            ),
            request,
        ))
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
    quota: BeaProviderQuotaDeclaration,
    source_binding: BeaSourceBinding,
    active_datasets: RwLock<BTreeMap<SourceIdentifier, Arc<BeaDoctorAdmissionEvidence>>>,
    transport: Arc<dyn BeaTransport>,
    response_limit: usize,
    request_timeout: Duration,
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
            .field("source_binding", &self.source_binding.binding_digest())
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
        credential_generation_digest: EvidenceDigest,
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
            credential_generation_digest,
            transport,
        )
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: SourceMetadata,
        user_id: BeaUserId,
        config: BeaSourceConfig,
        credential_generation_digest: EvidenceDigest,
        transport: Arc<dyn BeaTransport>,
    ) -> Result<Self, BeaSourceError> {
        Self::validate_metadata(&metadata, &config, &user_id)?;
        Self::try_new_inner(
            metadata,
            user_id,
            config,
            credential_generation_digest,
            transport,
        )
    }

    fn try_new_inner(
        metadata: SourceMetadata,
        user_id: BeaUserId,
        config: BeaSourceConfig,
        credential_generation_digest: EvidenceDigest,
        transport: Arc<dyn BeaTransport>,
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
        let quota = BeaProviderQuotaDeclaration::try_new()?;
        let source_binding = BeaSourceBinding::try_new(
            metadata.source_id().clone(),
            metadata.revision().clone(),
            config.digest()?,
            credential_generation_digest,
            quota.declaration_digest(),
        )
        .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        Ok(Self {
            metadata,
            user_id,
            config,
            quota,
            source_binding,
            active_datasets: RwLock::new(BTreeMap::new()),
            transport,
            response_limit,
            request_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
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
        let expected_quota = bea_provider_quota_declaration()?;
        let minimum_request_interval_nanos = u64::try_from(BEA_MINIMUM_REQUEST_INTERVAL.as_nanos())
            .map_err(|_| BeaSourceError::InvalidMetadata)?;
        let has_pacing_window = (0..budget.window_count()).any(|index| {
            budget.window(index).is_some_and(|window| {
                window.requests_per_window() == 1
                    && window.window_nanos() == minimum_request_interval_nanos
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        });
        let has_application_window = (0..budget.window_count()).any(|index| {
            budget.window(index).is_some_and(|window| {
                window.requests_per_window() == BEA_APPLICATION_REQUESTS_PER_MINUTE
                    && window.window_nanos() == 60_000_000_000
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        });
        let has_response_byte_window = (0..budget.weighted_window_count()).any(|index| {
            budget.weighted_window(index).is_some_and(|window| {
                window.dimension() == ProviderRateWeightedDimension::ResponseBytes
                    && window.maximum_units() == BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE
                    && window.window_nanos() == 60_000_000_000
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        });
        let has_provider_error_window = (0..budget.weighted_window_count()).any(|index| {
            budget.weighted_window(index).is_some_and(|window| {
                window.dimension() == ProviderRateWeightedDimension::ProviderErrors
                    && window.maximum_units() == u64::from(BEA_APPLICATION_ERRORS_PER_MINUTE)
                    && window.window_nanos() == 60_000_000_000
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        });
        if budget != expected_quota.shared_declaration().policy()
            || budget.scope().as_source_identifier() != metadata.provider()
            || budget.scope().authorization_account().is_none()
            || budget.max_concurrent() != 1
            || !has_pacing_window
            || !has_application_window
            || budget.weighted_window_count() != 2
            || !has_response_byte_window
            || !has_provider_error_window
        {
            return Err(BeaSourceError::InvalidMetadata);
        }
        for contract in config.contracts() {
            if contract
                .provider_dataset()
                .as_str()
                .contains(user_id.expose_secret())
                || contract.parameters().iter().any(|(name, value)| {
                    name.as_str().contains(user_id.expose_secret())
                        || value.contains(user_id.expose_secret())
                })
            {
                return Err(BeaSourceError::InvalidConfiguration);
            }
            for request in contract.metadata_policy_requests()? {
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

    /// Returns request admission plus the byte/error settlement requirements for shared authority.
    pub const fn quota_declaration(&self) -> &BeaProviderQuotaDeclaration {
        &self.quota
    }

    /// Returns the non-secret source/configuration/credential/quota binding.
    pub const fn source_binding(&self) -> &BeaSourceBinding {
        &self.source_binding
    }

    /// Returns a lock-free saturating telemetry snapshot.
    pub fn telemetry(&self) -> BeaSourceTelemetry {
        self.telemetry.snapshot()
    }

    /// Runs the real metadata-first provider journey without creating activation authority.
    pub async fn doctor(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaDoctorRun, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        let acquisition = self
            .acquire_dataset(authority, provider_dataset, deadline, cancellation.clone())
            .await?;
        let verified_at = system_timestamp().map_err(map_source_error)?;
        let run = BeaDoctorRun::try_new(
            &self.source_binding,
            &self.quota,
            provider_dataset.clone(),
            contract.analytical_dataset_id().clone(),
            acquisition,
            verified_at,
        )
        .map_err(|_| invalid_protocol())?;
        self.validate_operation_current(authority, deadline, &cancellation)?;
        Ok(run)
    }

    /// Admits one dataset in this process only after doctor evidence has an actual shared seal.
    ///
    /// This method creates no restart or publication authority. Root composition remains the sole
    /// owner of durable provider state and must re-establish readiness after process restart.
    pub fn activate_doctor(
        &self,
        admission: Arc<BeaDoctorAdmissionEvidence>,
    ) -> Result<(), BeaSourceError> {
        let observed_at = system_timestamp()?;
        let contract = self
            .config
            .contract(admission.dataset_id())
            .ok_or(BeaSourceError::InvalidConfiguration)?;
        admission
            .validate_current(
                &self.source_binding,
                contract.dataset_id(),
                contract.analytical_dataset_id(),
                observed_at,
            )
            .map_err(|_| BeaSourceError::InvalidConfiguration)?;
        self.active_datasets
            .write()
            .map_err(|_| BeaSourceError::Authority)?
            .insert(contract.dataset_id().clone(), admission);
        Ok(())
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
    async fn acquire_metadata(
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
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(contract.parameters.len().saturating_add(2))
            .map_err(|_| map_source_error(BeaSourceError::Allocation))?;
        for request in contract
            .metadata_root_requests()
            .map_err(map_source_error)?
        {
            pages.push(
                self.acquire_metadata_request(authority, request, deadline, cancellation.clone())
                    .await?,
            );
        }
        validate_metadata_roots(contract, &pages).map_err(map_source_error)?;
        let definitions = match pages.get(1).map(|page| page.page.records()) {
            Some(BeaMetadataRecords::Parameters(definitions)) => definitions,
            _ => return Err(invalid_protocol()),
        };
        let value_requests = contract
            .parameter_value_requests(definitions)
            .map_err(map_source_error)?;
        for request in value_requests {
            pages.push(
                self.acquire_metadata_request(authority, request, deadline, cancellation.clone())
                    .await?,
            );
        }
        validate_metadata_bundle(contract, &pages).map_err(map_source_error)?;
        let mut response_receipts = Vec::new();
        response_receipts
            .try_reserve_exact(pages.len())
            .map_err(|_| map_source_error(BeaSourceError::Allocation))?;
        response_receipts.extend(pages.iter().map(|page| page.page.receipt()));
        let generation = BeaMetadataGeneration::from_page_receipts(&response_receipts)
            .map_err(|error| map_source_error(error.into()))?;
        Ok(BeaMetadataBundle {
            dataset_id: provider_dataset.clone(),
            pages,
            generation,
        })
    }

    async fn acquire_metadata_request(
        &self,
        authority: &ExtractionAuthority,
        request: BeaRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaCapturedMetadataPage, ExtractionSourceError> {
        let fetched = self
            .fetch(authority, &request, deadline, cancellation.clone())
            .await?;
        self.validate_operation_current(authority, deadline, &cancellation)?;
        let capture_dataset = capture_dataset_identity(&request)?;
        let mut fetched = fetched.sanitize(
            &self.metadata,
            &request,
            &self.user_id,
            self.effective_parse_limits(),
            capture_dataset,
        )?;
        let retained_body = match fetched.retained_body() {
            Ok(body) => body,
            Err(error) => {
                fetched.settle_invalid_response()?;
                return Err(error);
            }
        };
        let page = match crate::parser::parse_metadata_page_sanitized(
            retained_body,
            &request,
            self.effective_parse_limits(),
        ) {
            Ok(page) => page,
            Err(error) => {
                self.record_parse_failure(&error);
                fetched.settle_parse_error(&error)?;
                return Err(map_source_error(BeaSourceError::Adapter(error)));
            }
        };
        let page =
            match page.bind_sanitized_capture(fetched.upstream_digest, fetched.retained_digest) {
                Ok(page) => page,
                Err(error) => {
                    self.record_parse_failure(&error);
                    fetched.settle_parse_error(&error)?;
                    return Err(map_source_error(BeaSourceError::Adapter(error)));
                }
            };
        let telemetry = match response_telemetry(&request, page.receipt(), &fetched) {
            Ok(telemetry) => telemetry,
            Err(error) => {
                fetched.settle_invalid_response()?;
                return Err(error);
            }
        };
        if page.receipt().completeness() == BeaCompleteness::Partial {
            fetched.settle_invalid_response()?;
            return Err(invalid_protocol());
        }
        self.validate_operation_current(authority, deadline, &cancellation)?;
        fetched.settle_success()?;
        self.record_page(&telemetry, page.records().len(), false);
        Ok(BeaCapturedMetadataPage {
            request,
            page,
            material: fetched.material,
            telemetry,
        })
    }

    /// Acquires typed `GetData` rows against the exact metadata generation.
    async fn acquire_data(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        metadata_generation: BeaMetadataGeneration,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<BeaCapturedDataPage, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        let request = contract
            .data_request(metadata_generation)
            .map_err(map_source_error)?;
        let fetched = self
            .fetch(authority, &request, deadline, cancellation.clone())
            .await?;
        self.validate_operation_current(authority, deadline, &cancellation)?;
        let mut fetched = fetched.sanitize(
            &self.metadata,
            &request,
            &self.user_id,
            self.effective_parse_limits(),
            contract.dataset_id().clone(),
        )?;
        let retained_body = match fetched.retained_body() {
            Ok(body) => body,
            Err(error) => {
                fetched.settle_invalid_response()?;
                return Err(error);
            }
        };
        let page = match crate::parser::parse_data_page_sanitized(
            retained_body,
            &request,
            self.effective_parse_limits(),
        ) {
            Ok(page) => page,
            Err(error) => {
                self.record_parse_failure(&error);
                fetched.settle_parse_error(&error)?;
                return Err(map_source_error(BeaSourceError::Adapter(error)));
            }
        };
        if page.metadata_generation() != metadata_generation {
            fetched.settle_invalid_response()?;
            return Err(invalid_protocol());
        }
        let page =
            match page.bind_sanitized_capture(fetched.upstream_digest, fetched.retained_digest) {
                Ok(page) => page,
                Err(error) => {
                    self.record_parse_failure(&error);
                    fetched.settle_parse_error(&error)?;
                    return Err(map_source_error(BeaSourceError::Adapter(error)));
                }
            };
        let telemetry = match response_telemetry(&request, page.receipt(), &fetched) {
            Ok(telemetry) => telemetry,
            Err(error) => {
                fetched.settle_invalid_response()?;
                return Err(error);
            }
        };
        if page.receipt().completeness() == BeaCompleteness::Partial {
            fetched.settle_invalid_response()?;
            return Err(invalid_protocol());
        }
        self.validate_operation_current(authority, deadline, &cancellation)?;
        fetched.settle_success()?;
        self.record_page(&telemetry, 0, true);
        Ok(BeaCapturedDataPage {
            request,
            page,
            material: fetched.material,
            telemetry,
        })
    }

    /// Runs the complete metadata-first acquisition and rejects known partial row sets.
    async fn acquire_dataset(
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
            .acquire_data(
                authority,
                metadata.dataset_id(),
                metadata.generation(),
                deadline,
                cancellation,
            )
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
        let activation = self.require_activation(request.dataset())?;
        if request.deadline() >= activation.expires_at() {
            return Err(invalid_protocol());
        }
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
                cancellation.clone(),
            )
            .await?;
        if acquisition.metadata().generation().digest() != activation.metadata_generation().bytes()
        {
            return Err(SourceError::GenerationResynchronizationRequired.into());
        }
        let object = source_object(&self.metadata, &request, contract, &acquisition)?;
        let batch = DiscoveryBatch::try_new(&request, vec![object])?;
        let completed_at = system_timestamp().map_err(map_source_error)?;
        activation
            .validate_current(
                &self.source_binding,
                contract.dataset_id(),
                contract.analytical_dataset_id(),
                completed_at,
            )
            .map_err(|_| invalid_protocol())?;
        self.validate_operation_current(&authority, request.deadline(), &cancellation)?;
        Ok(BeaCapturedDiscovery {
            batch,
            acquisition,
            doctor_admission_digest: activation.admission_digest(),
            doctor_sealed_graph_digest: activation.doctor_sealed_graph_digest(),
        })
    }

    /// Consumes one physically sealed discovery graph directly into the provider publication
    /// candidate. No provider request is made here; metadata, observations, capture evidence, and
    /// the final whole-capture token all come from the original discovery acquisition.
    pub fn extract_sealed_discovery(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        discovery: crate::sealed::BeaSealedDiscoveryAdmission,
        cancellation: CancellationToken,
    ) -> Result<crate::BeaPublicationCandidate, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        let activation = self.require_activation(request.object().dataset())?;
        if request.deadline() >= activation.expires_at() {
            return Err(invalid_protocol());
        }
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
        let (
            discovery_batch,
            sealed_acquisition,
            capture_token,
            doctor_admission_digest,
            doctor_sealed_graph_digest,
        ) = discovery.into_extraction_parts();
        let discovered_object = discovery_batch
            .objects()
            .first()
            .filter(|_| discovery_batch.objects().len() == 1)
            .ok_or_else(invalid_protocol)?;
        if discovered_object != request.object()
            || doctor_admission_digest != activation.admission_digest()
            || doctor_sealed_graph_digest != activation.doctor_sealed_graph_digest()
            || sealed_acquisition
                .evidence()
                .metadata()
                .generation()
                .digest()
                != activation.metadata_generation().bytes()
        {
            return Err(invalid_protocol());
        }
        verify_sealed_acquisition(&request, &expected, &sealed_acquisition)?;
        let records = native_records(&request, sealed_acquisition.evidence().data())?;
        let batch = ExtractionBatch::try_new(&request, records)?;
        let source_batch_digest = source_batch_digest(&batch)?;
        let completed_at = system_timestamp().map_err(map_source_error)?;
        activation
            .validate_current(
                &self.source_binding,
                contract.dataset_id(),
                contract.analytical_dataset_id(),
                completed_at,
            )
            .map_err(|_| invalid_protocol())?;
        self.validate_operation_current(&authority, request.deadline(), &cancellation)?;
        let sealed_output = crate::sealed::BeaSealedExtractionOutput::from_sealed_discovery(
            batch,
            source_batch_digest,
            sealed_acquisition,
            capture_token,
        );
        crate::BeaPublicationCandidate::try_new(
            &self.source_binding,
            activation.as_ref(),
            sealed_output,
        )
        .map_err(|_| invalid_protocol())
    }

    async fn fetch(
        &self,
        authority: &ExtractionAuthority,
        request: &BeaRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedResponse, ExtractionSourceError> {
        self.validate_authority(authority)?;
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
        let mut response = match result {
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
        let headers = match response.retain_secret_free_headers(&self.user_id) {
            Ok(headers) => headers,
            Err(error) => {
                self.telemetry.add(&self.telemetry.failures, 1);
                settle_complete_response(
                    in_flight,
                    response_bytes,
                    ProviderRateResponseClass::InvalidProviderResponse,
                    ProviderRateRetryAfterDisposition::Absent,
                    0,
                )?;
                return Err(map_source_error(error));
            }
        };
        if headers.retry_after.is_some() {
            self.telemetry.add(&self.telemetry.retry_after_responses, 1);
        }
        if headers.retry_after.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > MAX_RETRY_AFTER_BYTES
                || value.iter().any(u8::is_ascii_control)
        }) {
            self.telemetry.add(&self.telemetry.failures, 1);
            settle_complete_response(
                in_flight,
                response_bytes,
                ProviderRateResponseClass::InvalidProviderResponse,
                ProviderRateRetryAfterDisposition::parse_http(headers.retry_after.as_deref()),
                0,
            )?;
            return Err(invalid_protocol());
        }
        match response.status {
            200 => {}
            401 | 403 => {
                self.telemetry.add(&self.telemetry.failures, 1);
                settle_complete_response(
                    in_flight,
                    response_bytes,
                    ProviderRateResponseClass::HttpProviderError,
                    ProviderRateRetryAfterDisposition::Absent,
                    0,
                )?;
                return Err(SourceError::Unauthorized.into());
            }
            429 | 503 => {
                self.telemetry
                    .add(&self.telemetry.rate_limited_responses, 1);
                settle_complete_response(
                    in_flight,
                    response_bytes,
                    ProviderRateResponseClass::ProviderRefusal,
                    ProviderRateRetryAfterDisposition::parse_http(headers.retry_after.as_deref()),
                    0,
                )?;
                return Err(SourceError::ProviderUnavailable.into());
            }
            _ => {
                self.telemetry.add(&self.telemetry.failures, 1);
                settle_complete_response(
                    in_flight,
                    response_bytes,
                    ProviderRateResponseClass::HttpProviderError,
                    ProviderRateRetryAfterDisposition::Absent,
                    0,
                )?;
                return Err(SourceError::ProviderUnavailable.into());
            }
        }
        if headers
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(headers.content_type.as_deref())
        {
            self.telemetry.add(&self.telemetry.failures, 1);
            settle_complete_response(
                in_flight,
                response_bytes,
                ProviderRateResponseClass::InvalidProviderResponse,
                ProviderRateRetryAfterDisposition::parse_http(headers.retry_after.as_deref()),
                0,
            )?;
            return Err(invalid_protocol());
        }
        Ok(FetchedResponse {
            response,
            headers,
            response_bytes,
            in_flight: Some(in_flight),
        })
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

    fn validate_operation_current(
        &self,
        authority: &ExtractionAuthority,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<(), ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        self.validate_authority(authority)?;
        let now = system_timestamp().map_err(map_source_error)?;
        let _remaining = deadline_remaining(deadline, now)?;
        Ok(())
    }

    fn require_activation(
        &self,
        provider_dataset: &SourceIdentifier,
    ) -> Result<Arc<BeaDoctorAdmissionEvidence>, ExtractionSourceError> {
        let now = system_timestamp().map_err(map_source_error)?;
        let admission = self
            .active_datasets
            .read()
            .map_err(|_| invalid_protocol())?
            .get(provider_dataset)
            .cloned()
            .ok_or_else(invalid_protocol)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        admission
            .validate_current(
                &self.source_binding,
                contract.dataset_id(),
                contract.analytical_dataset_id(),
                now,
            )
            .map_err(|_| invalid_protocol())?;
        Ok(admission)
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
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
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

struct FetchedResponse {
    response: BeaHttpResponse,
    headers: BeaRetainedResponseHeaders,
    response_bytes: u64,
    in_flight: Option<InFlightExtractionRequest>,
}

impl FetchedResponse {
    fn sanitize(
        self,
        metadata: &SourceMetadata,
        request: &BeaRequest,
        user_id: &BeaUserId,
        limits: BeaParseLimits,
        capture_dataset: SourceIdentifier,
    ) -> Result<SanitizedFetchedResponse, ExtractionSourceError> {
        let Self {
            response,
            headers,
            response_bytes,
            mut in_flight,
        } = self;
        let BeaRetainedResponseHeaders {
            retry_after,
            content_encoding: _,
            content_type: _,
        } = headers;
        let BeaHttpResponse {
            status,
            retry_after: _,
            content_encoding: _,
            content_type: _,
            body,
            received_at,
            latency,
        } = response;
        let sanitized = (|| {
            let body = body
                .sanitize_validated_echo(request, user_id, limits)
                .map_err(|error| map_source_error(BeaSourceError::Adapter(error)))?;
            if u64::try_from(body.bytes().len()).map_err(|_| invalid_protocol())? != response_bytes
            {
                return Err(invalid_protocol());
            }
            let request_identity = evidence_digest(request.request_digest());
            let body_digest = evidence_digest(body.retained_digest());
            let capture = ProviderCaptureSetReceipt::try_new(
                metadata.source_id().clone(),
                metadata.revision().clone(),
                capture_dataset,
                request_identity,
                ProviderCaptureTerminalDisposition::StandaloneResponse,
                vec![
                    ProviderCapturePageReceipt::try_new(
                        0,
                        request_identity,
                        None,
                        None,
                        status,
                        response_bytes,
                        body_digest,
                        received_at,
                    )
                    .map_err(map_capture_error)?,
                ],
            )
            .map_err(map_capture_error)?;
            let received = DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos());
            let record = RawCaptureRecord::try_new_live(
                capture_uuid(b"event", &capture),
                Arc::from(metadata.source_id().as_str()),
                capture_uuid(b"connection", &capture),
                Some(0),
                None,
                received,
                body.bytes().clone(),
            )
            .map_err(|error| map_source_error(BeaSourceError::RawCapture(error)))?;
            let material = ProviderCaptureMaterial::try_new(capture, vec![record])
                .map_err(map_capture_error)?;
            Ok((body.upstream_digest(), body.retained_digest(), material))
        })();
        match sanitized {
            Ok((upstream_digest, retained_digest, material)) => Ok(SanitizedFetchedResponse {
                status,
                retry_after,
                response_bytes,
                latency,
                upstream_digest,
                retained_digest,
                material,
                in_flight,
            }),
            Err(error) => {
                settle_complete_response(
                    in_flight.take().ok_or_else(invalid_protocol)?,
                    response_bytes,
                    ProviderRateResponseClass::InvalidProviderResponse,
                    ProviderRateRetryAfterDisposition::parse_http(retry_after.as_deref()),
                    0,
                )?;
                Err(error)
            }
        }
    }
}

struct SanitizedFetchedResponse {
    status: u16,
    retry_after: Option<Vec<u8>>,
    response_bytes: u64,
    latency: Duration,
    upstream_digest: [u8; 32],
    retained_digest: [u8; 32],
    material: ProviderCaptureMaterial,
    in_flight: Option<InFlightExtractionRequest>,
}

impl SanitizedFetchedResponse {
    fn retained_body(&self) -> Result<&[u8], ExtractionSourceError> {
        self.material
            .records()
            .first()
            .filter(|_| self.material.records().len() == 1)
            .map(|record| record.payload())
            .ok_or_else(invalid_protocol)
    }

    fn settle_success(&mut self) -> Result<(), ExtractionSourceError> {
        self.settle(ProviderRateResponseClass::ValidatedSuccess)
    }

    fn settle_parse_error(&mut self, error: &BeaError) -> Result<(), ExtractionSourceError> {
        self.settle(match error {
            BeaError::Provider(_) | BeaError::FilteredParameterValuesUnsupported => {
                ProviderRateResponseClass::ProviderBodyError
            }
            BeaError::InvalidCredential
            | BeaError::InvalidRequest
            | BeaError::InvalidLimit
            | BeaError::BodyTooLarge
            | BeaError::RowLimitExceeded
            | BeaError::StringLimitExceeded
            | BeaError::Allocation
            | BeaError::InvalidJson
            | BeaError::InvalidField(_)
            | BeaError::RequestEchoMismatch
            | BeaError::InvalidDecimal
            | BeaError::InvalidTimePeriod
            | BeaError::InvalidRevision => ProviderRateResponseClass::InvalidProviderResponse,
        })
    }

    fn settle_invalid_response(&mut self) -> Result<(), ExtractionSourceError> {
        self.settle(ProviderRateResponseClass::InvalidProviderResponse)
    }

    fn settle(
        &mut self,
        response_class: ProviderRateResponseClass,
    ) -> Result<(), ExtractionSourceError> {
        let retry_after = if response_class == ProviderRateResponseClass::InvalidProviderResponse {
            ProviderRateRetryAfterDisposition::parse_http(self.retry_after.as_deref())
        } else {
            ProviderRateRetryAfterDisposition::Absent
        };
        settle_complete_response(
            self.in_flight.take().ok_or_else(invalid_protocol)?,
            self.response_bytes,
            response_class,
            retry_after,
            0,
        )
    }
}

fn settle_complete_response(
    in_flight: InFlightExtractionRequest,
    response_bytes: u64,
    response_class: ProviderRateResponseClass,
    retry_after: ProviderRateRetryAfterDisposition,
    fallback_jitter_sample_basis_points: u16,
) -> Result<(), ExtractionSourceError> {
    let settlement = ProviderRateResponseSettlement::try_new(
        response_bytes,
        response_class,
        retry_after,
        fallback_jitter_sample_basis_points,
    )
    .map_err(|_| invalid_protocol())?;
    let _receipt = in_flight.settle_response(settlement)?;
    Ok(())
}

fn validate_metadata_bundle(
    contract: &BeaDatasetContract,
    pages: &[BeaCapturedMetadataPage],
) -> Result<(), BeaSourceError> {
    validate_metadata_roots(contract, pages)?;
    if pages.len() != contract.parameters.len().saturating_add(2) {
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
    let expected = contract.parameter_value_requests(definitions)?;
    for (expected_request, page) in expected.iter().zip(pages.iter().skip(2)) {
        if page.request() != expected_request {
            return Err(BeaSourceError::Protocol);
        }
        let parameter = expected_request
            .query()
            .parameter()
            .or_else(|| expected_request.query().target_parameter())
            .ok_or(BeaSourceError::Protocol)?;
        let (configured_parameter, selected) = contract
            .selected_parameter(parameter.as_str())
            .ok_or(BeaSourceError::Protocol)?;
        if configured_parameter != parameter {
            return Err(BeaSourceError::Protocol);
        }
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

fn validate_metadata_roots(
    contract: &BeaDatasetContract,
    pages: &[BeaCapturedMetadataPage],
) -> Result<(), BeaSourceError> {
    if pages.len() < 2 {
        return Err(BeaSourceError::Protocol);
    }
    let expected = contract.metadata_root_requests()?;
    if pages[0].request() != &expected[0] || pages[1].request() != &expected[1] {
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
    if !matches!(
        pages.get(1).map(|page| page.page.records()),
        Some(BeaMetadataRecords::Parameters(_))
    ) {
        return Err(BeaSourceError::Protocol);
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
    let received_at = capture
        .pages()
        .first()
        .filter(|_| capture.pages().len() == 1)
        .ok_or_else(invalid_protocol)?
        .received_at();
    let effective = EffectiveInterval::new(received_at, None).map_err(|_| invalid_protocol())?;
    let media_type =
        SourceIdentifier::try_from(BEA_JSON_MEDIA_TYPE).map_err(|_| invalid_protocol())?;
    let evidence = ExactPayloadEvidence::from_content_digest(capture.content_digest());
    let capture_identity =
        SourceObjectCaptureIdentity::try_from_capture(capture).map_err(map_capture_error)?;
    let availability = market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
        observed_at: received_at,
    };
    let expected_bytes = Some(capture.total_body_bytes());
    let lineage_digest = source_object_lineage_digest(&SourceObjectLineageWire {
        source_id: metadata.source_id().as_str(),
        metadata_revision: metadata.revision().as_source_identifier().as_str(),
        dataset: request.dataset().as_str(),
        discovery_request_id: request.request_id(),
        media_type: media_type.as_str(),
        evidence: evidence.content_digest(),
        capture_identity,
        effective,
        published_at: None,
        availability: &availability,
        expected_bytes,
    })?;
    let object_id = object_id(
        contract,
        acquisition.metadata().generation(),
        capture,
        lineage_digest,
    )?;
    SourceObject::try_new_with_capture_identity(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        media_type,
        evidence,
        capture_identity,
        effective,
        None,
        availability,
        expected_bytes,
    )
    .map_err(Into::into)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SourceObjectLineageWire<'a> {
    source_id: &'a str,
    metadata_revision: &'a str,
    dataset: &'a str,
    discovery_request_id: market_squawk_sources::DiscoveryRequestId,
    media_type: &'a str,
    evidence: EvidenceDigest,
    capture_identity: SourceObjectCaptureIdentity,
    effective: EffectiveInterval,
    published_at: Option<Timestamp>,
    availability: &'a market_squawk_sources::AvailabilityEvidence,
    expected_bytes: Option<u64>,
}

fn source_object_lineage_digest(
    lineage: &SourceObjectLineageWire<'_>,
) -> Result<[u8; 32], ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bea-source-object-lineage/v1");
    serde_json::to_writer(Sha256Writer(&mut hash), lineage).map_err(|_| invalid_protocol())?;
    Ok(hash.finalize().into())
}

fn existing_source_object_lineage_digest(
    object: &SourceObject,
) -> Result<[u8; 32], ExtractionSourceError> {
    source_object_lineage_digest(&SourceObjectLineageWire {
        source_id: object.source_id().as_str(),
        metadata_revision: object.metadata_revision().as_source_identifier().as_str(),
        dataset: object.dataset().as_str(),
        discovery_request_id: object.discovery_request_id(),
        media_type: object.media_type().as_str(),
        evidence: object.evidence().content_digest(),
        capture_identity: object.capture_identity(),
        effective: object.effective_interval(),
        published_at: object.published_at(),
        availability: object.availability(),
        expected_bytes: object.expected_bytes(),
    })
}

#[derive(Debug)]
struct ParsedObjectId {
    contract_digest: [u8; 32],
    metadata_digest: [u8; 32],
    capture_digest: [u8; 32],
    lineage_digest: [u8; 32],
}

impl ParsedObjectId {
    fn parse(value: &SourceIdentifier) -> Result<Self, ExtractionSourceError> {
        let mut parts = value.as_str().split(':');
        if parts.next() != Some("bea") || parts.next() != Some("object-v2") {
            return Err(invalid_protocol());
        }
        let contract_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        let metadata_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        let capture_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        let lineage_digest = parse_hex(parts.next().ok_or_else(invalid_protocol)?)?;
        if parts.next().is_some() {
            return Err(invalid_protocol());
        }
        Ok(Self {
            contract_digest,
            metadata_digest,
            capture_digest,
            lineage_digest,
        })
    }
}

fn object_id(
    contract: &BeaDatasetContract,
    generation: BeaMetadataGeneration,
    capture: &ProviderCaptureSetReceipt,
    lineage_digest: [u8; 32],
) -> Result<SourceIdentifier, ExtractionSourceError> {
    let contract = contract_digest(
        &contract.dataset,
        &contract.parameters,
        contract.expected_rows,
    )
    .map_err(map_source_error)?;
    SourceIdentifier::try_from(format!(
        "bea:object-v2:{}:{}:{}:{}",
        lower_hex(contract),
        lower_hex(generation.digest()),
        lower_hex(capture.content_digest().bytes()),
        lower_hex(lineage_digest),
    ))
    .map_err(|_| invalid_protocol())
}

fn verify_sealed_acquisition(
    request: &ExtractionRequest,
    expected: &ParsedObjectId,
    acquisition: &crate::sealed::BeaSealedAcquisitionReceipt,
) -> Result<(), ExtractionSourceError> {
    let capture = acquisition.evidence().data().capture();
    if expected.metadata_digest != acquisition.evidence().metadata().generation().digest()
        || expected.capture_digest != capture.content_digest().bytes()
        || expected.lineage_digest != existing_source_object_lineage_digest(request.object())?
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
    captured: &BeaDataEvidencePage,
) -> Result<Vec<ExtractionRecord>, ExtractionSourceError> {
    let provider_request = captured.request();
    let page = captured.page();
    let capture = captured.capture();
    if page.observations().len() > request.max_records() as usize {
        return Err(
            market_squawk_sources::ExtractionError::RecordLimitExceeded {
                requested: request.max_records(),
            }
            .into(),
        );
    }
    let schema =
        SourceIdentifier::try_from(BEA_NATIVE_EXTRACTION_SCHEMA).map_err(|_| invalid_protocol())?;
    let received_at = capture
        .pages()
        .first()
        .filter(|_| capture.pages().len() == 1)
        .ok_or_else(invalid_protocol)?
        .received_at();
    let mut records = Vec::new();
    records
        .try_reserve_exact(page.observations().len())
        .map_err(|_| invalid_protocol())?;
    for (index, observation) in page.observations().iter().enumerate() {
        let version = crate::BeaObservedVersion::try_from_page(page, index, received_at)
            .map_err(|error| map_source_error(error.into()))?;
        let payload = native_payload(provider_request, page, observation)?;
        let evidence = ExactPayloadEvidence::from_content_digest(evidence_digest(
            Sha256::digest(&payload).into(),
        ));
        let revision = SourceIdentifier::try_from(format!(
            "bea-version:{}",
            lower_hex(version.version_digest())
        ))
        .map_err(|_| invalid_protocol())?;
        records.push(ExtractionRecord::try_new_with_time(
            request,
            schema.clone(),
            evidence,
            effective_coordinate(observation)?,
            // `UTCProductionTime` is retained as provider response metadata only. BEA does
            // not define it as the observation's publication/release instant.
            None,
            market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
            revision,
            None,
            payload,
        )?);
    }
    Ok(records)
}

fn source_batch_digest(batch: &ExtractionBatch) -> Result<EvidenceDigest, ExtractionSourceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/bea-source-extraction-output/v1");
    serde_json::to_writer(Sha256Writer(&mut hash), batch.request())
        .map_err(|_| invalid_protocol())?;
    hash.update(
        u64::try_from(batch.records().len())
            .map_err(|_| invalid_protocol())?
            .to_be_bytes(),
    );
    for record in batch.records() {
        hash_text(&mut hash, record.schema().as_str()).map_err(map_source_error)?;
        hash_text(&mut hash, record.revision().as_str()).map_err(map_source_error)?;
        hash.update(record.evidence().content_digest().bytes());
    }
    Ok(evidence_digest(hash.finalize().into()))
}

struct Sha256Writer<'a>(&'a mut Sha256);

impl std::io::Write for Sha256Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    missing_marker: Option<&'static str>,
    missing_reason: Option<&'static str>,
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
    let (value, raw_value, missing, missing_marker, missing_reason) = match observation.value() {
        BeaObservationValue::Observed { value, raw } => (
            Some(value.to_string()),
            Some(raw.as_str()),
            None,
            None,
            None,
        ),
        BeaObservationValue::Missing(BeaMissingValue::Absent) => (
            None,
            None,
            Some("absent"),
            None,
            Some("value-dimension-absent-or-null"),
        ),
        BeaObservationValue::Missing(BeaMissingValue::Blank) => (
            None,
            None,
            Some("blank"),
            Some(""),
            Some("provider-empty-lexical-value"),
        ),
        BeaObservationValue::Missing(BeaMissingValue::SuppressedRegional) => (
            None,
            None,
            Some("suppressed_regional"),
            Some(crate::BEA_REGIONAL_SUPPRESSION_MARKER),
            Some(crate::BEA_REGIONAL_SUPPRESSION_REASON),
        ),
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
        missing_marker,
        missing_reason,
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
    response: &SanitizedFetchedResponse,
) -> Result<BeaResponseTelemetry, ExtractionSourceError> {
    Ok(BeaResponseTelemetry {
        request_identity: evidence_digest(request.request_digest()),
        method: request.query().method(),
        status: response.status,
        response_bytes: response.response_bytes,
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
