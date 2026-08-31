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
    ExtractionSource, ExtractionSourceError, HistoricalCapability, InFlightExtractionRequest,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, NetworkAccessPolicy, NetworkPolicyError, PathScope,
    ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSealExpectation,
    ProviderCaptureSealRequest, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, ProviderWholeCaptureToken, QueryParameterRule,
    QuerySensitivity, SealedProviderCaptureBinding, SealedProviderCaptureMaterial, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject, SourceObjectCaptureIdentity,
    SourceProtocolProfile, payload_matches_exact_evidence,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::http::{
    CensusHttpRequest, CensusHttpResponse, CensusRateLimitHeaders, CensusTransport,
    ReqwestCensusTransport, system_timestamp,
};
use crate::query::CensusAuthorizedUrl;
use crate::{
    CENSUS_APPLICATION_REQUESTS_PER_DAY, CENSUS_APPLICATION_REQUESTS_PER_SECOND,
    CensusAdapterError, CensusApiKey, CensusCatalogFailurePredicate, CensusClocks, CensusDataPage,
    CensusDataQuery, CensusDatasetVintage, CensusDiscoveryDocument, CensusDiscoveryKind,
    CensusDiscoveryRequest, CensusGeography, CensusGeographyFailurePredicate,
    CensusMetadataEvidence, CensusMissingReason, CensusParseLimits, CensusPredicateType,
    CensusRequiredVariable, CensusSelection, CensusTypedValue, CensusValueState,
    CensusVariableCatalog,
};

/// Maximum exact Census query contracts retained by one source instance.
pub const MAX_CENSUS_CONFIGURED_DATASETS: usize = 64;
/// Maximum reviewed annotated-missing interpretations retained for one provider variable.
pub const MAX_CENSUS_ANNOTATED_MISSING_RULES: usize = 64;
/// Maximum annotation coordinates that may define one exact interpretation rule.
pub const MAX_CENSUS_ANNOTATIONS_PER_RULE: usize = 32;
/// Maximum exact provider annotation text retained by one configured rule.
pub const MAX_CENSUS_ANNOTATION_RULE_BYTES: usize = 4 * 1024;
const CENSUS_JSON_MEDIA_TYPE: &str = "application/json";
const CENSUS_DATASET_ID_PREFIX: &str = "census:data-v1:";
const CENSUS_ANALYTICAL_ID_PREFIX: &str = "census.data-v1.";
const MAX_CENSUS_DIAGNOSTIC_REQUESTS: u8 = 7;
const ONE_SECOND_NANOS: u64 = 1_000_000_000;
const ONE_DAY_NANOS: u64 = 86_400_000_000_000;

/// One exact provider annotation coordinate used by a reviewed missing-value interpretation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize)]
pub struct CensusAnnotationMatch {
    variable: SourceIdentifier,
    raw: String,
}

impl CensusAnnotationMatch {
    /// Constructs one bounded, nonempty exact annotation coordinate.
    pub fn try_new(
        variable: SourceIdentifier,
        raw: impl Into<String>,
    ) -> Result<Self, CensusSourceError> {
        let raw = raw.into();
        if raw.is_empty()
            || raw.len() > MAX_CENSUS_ANNOTATION_RULE_BYTES
            || raw.chars().any(char::is_control)
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(Self { variable, raw })
    }

    /// Returns the exact Census annotation-variable identity.
    pub const fn variable(&self) -> &SourceIdentifier {
        &self.variable
    }

    /// Returns the exact provider annotation cell text.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    fn matches(&self, annotation: &crate::CensusAnnotation) -> bool {
        &self.variable == annotation.variable() && self.raw == annotation.raw()
    }
}

/// One reviewed exact annotation set and its canonical provider-native missing evidence.
///
/// The canonical marker must be the exact text of one member annotation and its reason must be
/// that member's exact annotation-variable identity. Other annotations remain in the publication
/// binding, so this mapping never erases evidence even when several provider flags are present.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct CensusAnnotatedMissingRule {
    annotations: Box<[CensusAnnotationMatch]>,
    missing: MacroMissingValue,
}

impl CensusAnnotatedMissingRule {
    /// Constructs one deterministic, duplicate-free exact annotation interpretation.
    pub fn try_new(
        annotations: impl IntoIterator<Item = CensusAnnotationMatch>,
        missing: MacroMissingValue,
    ) -> Result<Self, CensusSourceError> {
        let mut annotations = annotations.into_iter().collect::<Vec<_>>();
        annotations.sort();
        if annotations.is_empty()
            || annotations.len() > MAX_CENSUS_ANNOTATIONS_PER_RULE
            || annotations
                .windows(2)
                .any(|pair| pair[0].variable == pair[1].variable)
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        let reason = missing
            .reason()
            .ok_or(CensusSourceError::InvalidConfiguration)?;
        if !annotations.iter().any(|annotation| {
            annotation.variable() == reason && annotation.raw() == missing.marker().as_str()
        }) {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        Ok(Self {
            annotations: annotations.into_boxed_slice(),
            missing,
        })
    }

    /// Returns the complete sorted provider annotation set this rule recognizes.
    pub fn annotations(&self) -> &[CensusAnnotationMatch] {
        &self.annotations
    }

    /// Returns the reviewed exact provider-native canonical missing evidence.
    pub const fn missing(&self) -> &MacroMissingValue {
        &self.missing
    }

    fn matches(&self, annotations: &[crate::CensusAnnotation]) -> bool {
        if self.annotations.len() != annotations.len() {
            return false;
        }
        let mut actual = annotations.iter().collect::<Vec<_>>();
        actual.sort_by(|left, right| {
            left.variable()
                .cmp(right.variable())
                .then_with(|| left.raw().cmp(right.raw()))
        });
        self.annotations
            .iter()
            .zip(actual)
            .all(|(expected, actual)| expected.matches(actual))
    }
}

/// One explicit provider-variable to canonical macro-series mapping.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct CensusVariableMapping {
    provider_variable: SourceIdentifier,
    series_namespace: SourceIdentifier,
    unit: SourceIdentifier,
    annotated_missing_rules: Box<[CensusAnnotatedMissingRule]>,
}

impl CensusVariableMapping {
    /// Constructs a numeric macro mapping whose final stable row-scoped series identity remains
    /// representable without truncation.
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
            annotated_missing_rules: Vec::new().into_boxed_slice(),
        })
    }

    /// Constructs a numeric macro mapping with explicit exact interpretations for provider-
    /// annotated missing values. Unknown annotation combinations remain unavailable.
    pub fn try_new_with_annotated_missing(
        provider_variable: SourceIdentifier,
        series_namespace: SourceIdentifier,
        unit: SourceIdentifier,
        rules: impl IntoIterator<Item = CensusAnnotatedMissingRule>,
    ) -> Result<Self, CensusSourceError> {
        let mut mapping = Self::try_new(provider_variable, series_namespace, unit)?;
        let mut rules = rules.into_iter().collect::<Vec<_>>();
        rules.sort_by(|left, right| left.annotations.cmp(&right.annotations));
        if rules.len() > MAX_CENSUS_ANNOTATED_MISSING_RULES
            || rules
                .windows(2)
                .any(|pair| pair[0].annotations == pair[1].annotations)
        {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        mapping.annotated_missing_rules = rules.into_boxed_slice();
        Ok(mapping)
    }

    /// Returns the exact Census variable identity.
    pub const fn provider_variable(&self) -> &SourceIdentifier {
        &self.provider_variable
    }

    /// Returns the base canonical series namespace; stable provider row coordinates are appended
    /// by digest during normalization.
    pub const fn series_namespace(&self) -> &SourceIdentifier {
        &self.series_namespace
    }

    /// Returns the reviewed source-native unit identity.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }

    /// Returns the deterministic exact annotation interpretations admitted for this variable.
    pub fn annotated_missing_rules(&self) -> &[CensusAnnotatedMissingRule] {
        &self.annotated_missing_rules
    }

    fn annotated_missing(
        &self,
        annotations: &[crate::CensusAnnotation],
    ) -> Option<&MacroMissingValue> {
        self.annotated_missing_rules
            .iter()
            .find(|rule| rule.matches(annotations))
            .map(CensusAnnotatedMissingRule::missing)
    }
}

/// Exact rule for obtaining a canonical effective coordinate.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "coordinate")]
pub enum CensusEffectiveTimePolicy {
    /// Every response row must carry a supported `time` value.
    RequireReportedTime,
    /// Rows must not carry `time`; this reviewed fixed coordinate supplies the dataset meaning.
    Fixed(ResearchTemporalCoordinate),
}

/// One immutable metadata-first Census query contract.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
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

    /// Returns the exact reviewed effective-time rule for this dataset.
    pub const fn effective_time_policy(&self) -> &CensusEffectiveTimePolicy {
        &self.effective_time
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
    configuration_digest: EvidenceDigest,
}

impl CensusSourceConfig {
    /// Constructs a nonempty, duplicate-free technical source configuration. Root credentials,
    /// currentness, rate, sealing, and publication authority remain in shared components.
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
        let configuration_digest = census_configuration_digest(&contracts, parse_limits)?;
        Ok(Self {
            contracts,
            parse_limits,
            configuration_digest,
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

    /// Returns the exact digest of the provider query contracts and parser bounds.
    pub const fn configuration_digest(&self) -> EvidenceDigest {
        self.configuration_digest
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

/// Closed provider-journey phase retained only when a Census application operation fails.
///
/// The phase carries no request target, credential, header, response body, provider text, or
/// filesystem identity. Ordinary application and provider-neutral read DTOs never expose it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CensusDiagnosticPhase {
    /// Local doctor authority, configuration, and request validation before transport execution.
    DoctorPreflight,
    /// Credentialed doctor transport and exact response validation.
    DoctorResponse,
    /// Physical sealing of the exact doctor response.
    DoctorSeal,
    /// Post-seal doctor rejoin and activation-candidate construction.
    DoctorActivation,
    /// Bounded discovery-request construction after doctor activation.
    DiscoveryRequest,
    /// Dataset-catalog transport, parsing, and validation.
    MetadataCatalog,
    /// Dataset-group transport, parsing, and validation.
    MetadataGroups,
    /// Dataset or selected-group variable transport, parsing, and validation.
    MetadataVariables,
    /// Dataset-geography transport, parsing, and validation.
    MetadataGeography,
    /// Cross-document metadata closure after every required metadata response succeeds.
    MetadataClosure,
    /// Credentialed data transport, parsing, accounting, and completeness validation.
    DataResponse,
    /// Complete metadata-and-data capture graph and discovered-object construction.
    CaptureGraph,
    /// Physical sealing of the complete metadata-and-data capture graph.
    CaptureGraphSeal,
    /// Physical capture receipt rejoin and exact retained-acquisition validation.
    SealedRejoin,
    /// Admission of the sole sealed discovered object.
    AdmissionObject,
    /// Bounded extraction-request construction from that admitted object.
    ExtractionRequest,
    /// Provider-neutral normalization, lineage, and publication-candidate construction.
    Canonicalize,
}

/// Closed failure class retained without dynamic error material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CensusDiagnosticFailureClass {
    /// Provider response or local source protocol invariants failed.
    Protocol,
    /// Provider transport or availability failed.
    Transport,
    /// Provider credentials were rejected.
    Authorization,
    /// Shared provider-budget admission or cooldown prevented progress.
    Budget,
    /// The bounded provider operation exceeded its deadline.
    Deadline,
    /// The operation was cancelled.
    Cancellation,
    /// Registry, generation, or trusted-time authority was unavailable.
    Authority,
    /// Provider-specific adapter composition or capture rejoin failed.
    AdapterContract,
    /// Shared bounded extraction-contract construction or validation failed.
    ExtractionContract,
    /// Physical provider-capture sealing failed.
    CaptureSeal,
    /// Application operation authority became unavailable after sealing.
    ApplicationAuthority,
}

/// Compact payload-free detail for the currently failing closed phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CensusDiagnosticSubreason {
    /// The catalog failed before its body reached the catalog parser.
    CatalogResponse,
    /// The catalog parser rejected one exact closed validation predicate.
    CatalogParse(CensusCatalogFailurePredicate),
    /// Parsed catalog evidence failed bounded response/request accounting.
    CatalogAccounting,
    /// Geography metadata failed before its body reached the geography parser.
    GeographyResponse,
    /// The geography parser rejected one exact closed validation predicate.
    GeographyParse(CensusGeographyFailurePredicate),
    /// Parsed geography evidence failed bounded response/request accounting.
    GeographyAccounting,
}

/// Bounded, payload-free failure evidence for one Census application journey.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CensusFailureDiagnostic {
    phase: CensusDiagnosticPhase,
    attempted_requests: u8,
    successful_requests: u8,
    failure_class: CensusDiagnosticFailureClass,
    subreason: Option<CensusDiagnosticSubreason>,
}

impl CensusFailureDiagnostic {
    /// Returns the last closed phase entered before failure.
    pub const fn phase(self) -> CensusDiagnosticPhase {
        self.phase
    }

    /// Returns actual attempts, bounded by doctor plus the maximum metadata-and-data graph.
    pub const fn attempted_requests(self) -> u8 {
        self.attempted_requests
    }

    /// Returns responses fully validated and committed to request accounting.
    pub const fn successful_requests(self) -> u8 {
        self.successful_requests
    }

    /// Returns the closed failure class without retaining dynamic error material.
    pub const fn failure_class(self) -> CensusDiagnosticFailureClass {
        self.failure_class
    }

    /// Returns compact phase-local detail when one is available.
    pub const fn subreason(self) -> Option<CensusDiagnosticSubreason> {
        self.subreason
    }
}

/// Operation-local diagnostic state for the application-owned Census journey.
///
/// This value is deliberately neither serializable nor stored. It exists only to retain a closed
/// phase and bounded counts when the ordinary generic extraction error has insufficient detail.
#[derive(Debug)]
pub struct CensusDiagnosticJourney {
    phase: CensusDiagnosticPhase,
    attempted_requests: u8,
    successful_requests: u8,
    subreason: Option<CensusDiagnosticSubreason>,
}

impl CensusDiagnosticJourney {
    /// Starts the fixed doctor-first application journey.
    pub const fn new() -> Self {
        Self {
            phase: CensusDiagnosticPhase::DoctorPreflight,
            attempted_requests: 0,
            successful_requests: 0,
            subreason: None,
        }
    }

    /// Marks the local physical-seal rejoin before canonical extraction.
    pub fn enter_sealed_rejoin(&mut self) {
        self.enter(CensusDiagnosticPhase::SealedRejoin);
    }

    /// Marks physical doctor-response sealing.
    pub fn enter_doctor_seal(&mut self) {
        self.enter(CensusDiagnosticPhase::DoctorSeal);
    }

    /// Marks post-seal doctor activation-candidate construction.
    pub fn enter_doctor_activation(&mut self) {
        self.enter(CensusDiagnosticPhase::DoctorActivation);
    }

    /// Marks bounded discovery-request construction.
    pub fn enter_discovery_request(&mut self) {
        self.enter(CensusDiagnosticPhase::DiscoveryRequest);
    }

    /// Marks physical sealing of the complete provider capture graph.
    pub fn enter_capture_graph_seal(&mut self) {
        self.enter(CensusDiagnosticPhase::CaptureGraphSeal);
    }

    /// Marks retrieval of the sole admitted sealed object.
    pub fn enter_admission_object(&mut self) {
        self.enter(CensusDiagnosticPhase::AdmissionObject);
    }

    /// Marks bounded extraction-request construction.
    pub fn enter_extraction_request(&mut self) {
        self.enter(CensusDiagnosticPhase::ExtractionRequest);
    }

    fn enter(&mut self, phase: CensusDiagnosticPhase) {
        self.phase = phase;
        self.subreason = None;
    }

    fn note_subreason(&mut self, subreason: CensusDiagnosticSubreason) {
        self.subreason = Some(subreason);
    }

    fn record_attempt(&mut self) -> Result<(), ExtractionSourceError> {
        self.attempted_requests = self
            .attempted_requests
            .checked_add(1)
            .filter(|attempts| *attempts <= MAX_CENSUS_DIAGNOSTIC_REQUESTS)
            .ok_or_else(invalid_protocol)?;
        Ok(())
    }

    fn record_success(&mut self) -> Result<(), ExtractionSourceError> {
        self.successful_requests = self
            .successful_requests
            .checked_add(1)
            .filter(|successes| *successes <= self.attempted_requests)
            .ok_or_else(invalid_protocol)?;
        Ok(())
    }

    /// Freezes the current payload-free operation diagnostic.
    pub fn freeze_failure(
        &self,
        failure_class: CensusDiagnosticFailureClass,
    ) -> CensusFailureDiagnostic {
        CensusFailureDiagnostic {
            phase: self.phase,
            attempted_requests: self.attempted_requests,
            successful_requests: self.successful_requests,
            failure_class,
            subreason: self.subreason,
        }
    }
}

impl Default for CensusDiagnosticJourney {
    fn default() -> Self {
        Self::new()
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

    fn geography_admission(
        &self,
        query: &CensusDataQuery,
    ) -> Result<crate::CensusGeographyAdmission, CensusSourceError> {
        self.documents
            .iter()
            .find_map(|captured| match captured.document() {
                CensusDiscoveryDocument::Geographies(catalog) => Some(catalog),
                _ => None,
            })
            .ok_or(CensusSourceError::Protocol)?
            .admit(query.geography())
            .map_err(Into::into)
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

/// Indivisible Census discovery result and every exact response used to identify its object.
///
/// The ordinary [`ExtractionSource::discover`] return type cannot retain provider bytes, so the
/// production path returns this value and requires root composition to seal the complete ordered
/// metadata-plus-data graph. No provider response may be discarded after it influenced discovery.
#[derive(Debug)]
pub struct CensusDiscoveryOutput {
    batch: DiscoveryBatch,
    acquisition: CensusDatasetAcquisition,
    capture_material: ProviderCaptureMaterial,
    telemetry: CensusSourceTelemetry,
    activation: crate::CensusActivationCandidate,
}

impl CensusDiscoveryOutput {
    /// Returns the exact discovered source object batch.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Returns the typed metadata and data evidence used to identify the object.
    pub const fn acquisition(&self) -> &CensusDatasetAcquisition {
        &self.acquisition
    }

    /// Returns the complete ordered request graph root must seal before retaining discovery.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    /// Returns actual request, response, row, byte, and latency accounting.
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }

    /// Separates root-owned physical sealing from the opaque no-refetch continuation.
    pub fn into_sealing_parts(self) -> (CensusPendingDiscovery, ProviderCaptureSealRequest) {
        let (capture_expectation, seal_request) = self.capture_material.into_whole_seal_parts();
        (
            CensusPendingDiscovery {
                batch: self.batch,
                acquisition: self.acquisition,
                capture_expectation,
                telemetry: self.telemetry,
                activation: self.activation,
            },
            seal_request,
        )
    }
}

/// Non-cloneable discovery state awaiting the exact metadata-plus-data graph seal.
#[derive(Debug)]
pub struct CensusPendingDiscovery {
    batch: DiscoveryBatch,
    acquisition: CensusDatasetAcquisition,
    capture_expectation: ProviderCaptureSealExpectation,
    telemetry: CensusSourceTelemetry,
    activation: crate::CensusActivationCandidate,
}

impl CensusPendingDiscovery {
    /// Returns the unpublishable discovery batch for root scheduling inspection.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Consumes this continuation only when the physical receipt binds the exact original graph.
    pub fn try_bind_sealed(
        self,
        sealed_capture: SealedProviderCaptureMaterial,
    ) -> Result<CensusSealedDiscoveryAdmission, CensusSourceError> {
        let capture_token = self
            .capture_expectation
            .try_rejoin(sealed_capture)
            .and_then(|rejoined| rejoined.try_into_whole())
            .map_err(|_| CensusSourceError::Protocol)?;
        let sealed_capture = capture_token.persisted_receipt();
        let [object] = self.batch.objects() else {
            return Err(CensusSourceError::Protocol);
        };
        if sealed_capture.receipt_digest().bytes() == [0; 32]
            || !SourceObjectCaptureIdentity::try_from_capture(sealed_capture.capture())
                .is_ok_and(|identity| identity == object.capture_identity())
        {
            return Err(CensusSourceError::Protocol);
        }
        Ok(CensusSealedDiscoveryAdmission {
            batch: self.batch,
            acquisition: self.acquisition,
            capture_token,
            telemetry: self.telemetry,
            activation: self.activation,
        })
    }
}

/// One non-reusable, physically sealed Census discovery admitted for extraction without refetch.
#[derive(Debug)]
pub struct CensusSealedDiscoveryAdmission {
    batch: DiscoveryBatch,
    acquisition: CensusDatasetAcquisition,
    capture_token: ProviderWholeCaptureToken,
    telemetry: CensusSourceTelemetry,
    activation: crate::CensusActivationCandidate,
}

impl CensusSealedDiscoveryAdmission {
    /// Returns the sole exact object from which root must build the extraction request.
    pub fn object(&self) -> Result<&SourceObject, CensusSourceError> {
        let [object] = self.batch.objects() else {
            return Err(CensusSourceError::Protocol);
        };
        Ok(object)
    }
}

/// Sealed no-refetch extraction ready to become one provider-neutral publication candidate.
#[derive(Debug)]
pub struct CensusSealedExtractionOutput {
    candidate: crate::CensusPublicationCandidate,
    telemetry: CensusSourceTelemetry,
}

impl CensusSealedExtractionOutput {
    /// Returns the validated provider-neutral candidate.
    pub const fn candidate(&self) -> &crate::CensusPublicationCandidate {
        &self.candidate
    }

    /// Returns discovery/acquisition telemetry retained through the seal continuation.
    pub const fn telemetry(&self) -> CensusSourceTelemetry {
        self.telemetry
    }

    /// Consumes the sealed adapter output without exposing any provider-local store.
    pub fn into_parts(self) -> (crate::CensusPublicationCandidate, CensusSourceTelemetry) {
        (self.candidate, self.telemetry)
    }
}

/// Internal graph-bound canonical Census result used to revalidate the retained acquisition.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
struct CensusExtractionOutput {
    batch: ExtractionBatch,
    acquisition: CensusDatasetAcquisition,
    publication_plan: crate::CensusPublicationPlan,
    telemetry: CensusSourceTelemetry,
}

#[cfg_attr(not(test), allow(dead_code))]
impl CensusExtractionOutput {
    /// Returns the canonical shared extraction batch.
    pub(crate) const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns the exact provider-specific plan that root composition must satisfy atomically.
    pub(crate) const fn publication_plan(&self) -> &crate::CensusPublicationPlan {
        &self.publication_plan
    }

    /// Consumes the application handoff. The graph must be sealed before publishing `batch`.
    pub(crate) fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        CensusDatasetAcquisition,
        crate::CensusPublicationPlan,
        CensusSourceTelemetry,
    ) {
        (
            self.batch,
            self.acquisition,
            self.publication_plan,
            self.telemetry,
        )
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
    #[error("Census shared rate declaration failed: {0}")]
    RateDeclaration(#[from] crate::CensusRateDeclarationError),
}

trait CensusProcessingClock: Send + Sync {
    /// Samples local processing clocks only after the complete bounded page exists.
    fn sample_after_complete_parse(
        &self,
        page: &CensusDataPage,
    ) -> Result<(Timestamp, Timestamp), CensusSourceError>;
}

#[derive(Debug)]
struct SystemCensusProcessingClock;

impl CensusProcessingClock for SystemCensusProcessingClock {
    fn sample_after_complete_parse(
        &self,
        _page: &CensusDataPage,
    ) -> Result<(Timestamp, Timestamp), CensusSourceError> {
        Ok((system_timestamp()?, system_timestamp()?))
    }
}

/// Registry-authorized production Census source.
pub struct CensusSource {
    metadata: SourceMetadata,
    api_key: CensusApiKey,
    config: CensusSourceConfig,
    transport: Arc<dyn CensusTransport>,
    response_limit: usize,
    request_timeout: Duration,
    processing_clock: Arc<dyn CensusProcessingClock>,
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
        Self::try_new_inner(
            metadata,
            api_key,
            config,
            transport,
            Arc::new(SystemCensusProcessingClock),
        )
    }

    #[cfg(test)]
    fn try_new_with_transport_and_processing_clock(
        metadata: SourceMetadata,
        api_key: CensusApiKey,
        config: CensusSourceConfig,
        transport: Arc<dyn CensusTransport>,
        processing_clock: Arc<dyn CensusProcessingClock>,
    ) -> Result<Self, CensusSourceError> {
        Self::validate_metadata(&metadata, &config)?;
        Self::try_new_inner(metadata, api_key, config, transport, processing_clock)
    }

    fn try_new_inner(
        metadata: SourceMetadata,
        api_key: CensusApiKey,
        config: CensusSourceConfig,
        transport: Arc<dyn CensusTransport>,
        processing_clock: Arc<dyn CensusProcessingClock>,
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
            processing_clock,
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
        let authorization_subject = budget
            .scope()
            .authorization_account()
            .ok_or(CensusSourceError::InvalidMetadata)?;
        let expected_rate = crate::census_provider_rate_declaration(authorization_subject)?;
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
            || expected_rate.policy() != budget
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
                authorize_configured_target(metadata, request.redacted_url(), false)?;
            }
        }
        Ok(())
    }

    /// Returns the exact configured profile.
    pub const fn config(&self) -> &CensusSourceConfig {
        &self.config
    }

    /// Returns the exact provider-local activation recipe for root composition.
    pub fn activation_plan(&self) -> Result<crate::CensusActivationPlan, CensusSourceError> {
        crate::runtime::build_activation_plan(&self.metadata, &self.config)
    }

    /// Admits one freshly sealed successful doctor using the adapter's trusted local clock.
    pub fn activation_candidate(
        &self,
        pending: crate::CensusPendingDoctorSeal,
        sealed_doctor_capture: SealedProviderCaptureMaterial,
    ) -> Result<crate::CensusActivationCandidate, CensusSourceError> {
        let (doctor, capture_token) = pending.try_rejoin(sealed_doctor_capture)?;
        crate::CensusActivationCandidate::try_new(
            self.activation_plan()?,
            doctor,
            capture_token,
            system_timestamp()?,
        )
    }

    fn validate_activation(
        &self,
        activation: &crate::CensusActivationCandidate,
        operation_at: Timestamp,
    ) -> Result<(), CensusSourceError> {
        let expected = self.activation_plan()?;
        if activation.plan() != &expected {
            return Err(CensusSourceError::InvalidConfiguration);
        }
        activation.validate(operation_at)
    }

    /// Runs one credential-bearing, 16-KiB/10-second, production-path ACS doctor request.
    ///
    /// The doctor shares the source's durable request allocation and returns exact raw material
    /// for root sealing. It proves only the pinned 2024 ACS1 United States population surface,
    /// never bulk capacity or publication readiness.
    pub async fn doctor(
        &self,
        authority: &ExtractionAuthority,
        deadline: Timestamp,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<crate::CensusDoctorOutput, ExtractionSourceError> {
        diagnostic.enter(CensusDiagnosticPhase::DoctorPreflight);
        self.validate_authority(authority)?;
        let query = crate::doctor::doctor_query().map_err(map_source_error)?;
        authorize_configured_target(&self.metadata, query.redacted_url(), true)
            .map_err(map_source_error)?;
        let provider_dataset =
            crate::doctor::doctor_dataset_identity().map_err(map_source_error)?;
        diagnostic.enter(CensusDiagnosticPhase::DoctorResponse);
        let mut response = self
            .fetch_authorized_with_limits(
                authority,
                query.authorize(&self.api_key).map_err(map_adapter_error)?,
                query.request_digest(),
                &provider_dataset,
                deadline,
                cancellation.clone(),
                crate::CENSUS_DOCTOR_MAX_RESPONSE_BYTES.min(self.response_limit),
                crate::CENSUS_DOCTOR_TIMEOUT.min(self.request_timeout),
                diagnostic,
            )
            .await?;
        let report = crate::doctor::build_doctor_report(
            &self.metadata,
            &self.config,
            &query,
            &response.body,
            &response.capture,
            &response.rate_headers,
            response.received_at,
            response.latency,
        )
        .map_err(map_source_error)?;
        let material = capture_material(&self.metadata, &response.capture, response.body.clone())?;
        response.record_success()?;
        diagnostic.record_success()?;
        self.telemetry
            .successful_responses
            .fetch_add(1, Ordering::Relaxed);
        let output = crate::CensusDoctorOutput::new(report, material);
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let completed_at = system_timestamp().map_err(map_source_error)?;
        if completed_at >= deadline {
            return Err(ExtractionSourceError::DeadlineExceeded);
        }
        self.validate_authority(authority)?;
        Ok(output)
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
    pub(crate) async fn acquire_metadata(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusMetadataBundle, ExtractionSourceError> {
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(provider_dataset)
            .ok_or_else(invalid_protocol)?;
        let mut documents = Vec::new();
        let mut telemetry = CensusSourceTelemetry::default();
        for request in contract.metadata_requests() {
            diagnostic.enter(metadata_diagnostic_phase(request.kind()));
            let is_catalog = matches!(
                request.kind(),
                CensusDiscoveryKind::Datasets | CensusDiscoveryKind::VintageDatasets { .. }
            );
            let is_geography = matches!(request.kind(), CensusDiscoveryKind::Geographies { .. });
            if is_catalog {
                diagnostic.note_subreason(CensusDiagnosticSubreason::CatalogResponse);
            } else if is_geography {
                diagnostic.note_subreason(CensusDiagnosticSubreason::GeographyResponse);
            }
            let mut response = self
                .fetch_authorized(
                    authority,
                    request.public_request().map_err(map_adapter_error)?,
                    request.request_digest(),
                    provider_dataset,
                    deadline,
                    cancellation.clone(),
                    diagnostic,
                )
                .await?;
            let document = if is_catalog {
                match CensusDiscoveryDocument::parse_catalog_diagnosed(
                    request,
                    &response.body,
                    self.effective_parse_limits(),
                ) {
                    Ok(document) => document,
                    Err(failure) => {
                        diagnostic.note_subreason(CensusDiagnosticSubreason::CatalogParse(
                            failure.predicate(),
                        ));
                        self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                        return Err(map_adapter_error(failure.into_source()));
                    }
                }
            } else if is_geography {
                match CensusDiscoveryDocument::parse_geography_diagnosed(
                    request,
                    &response.body,
                    self.effective_parse_limits(),
                ) {
                    Ok(document) => document,
                    Err(failure) => {
                        diagnostic.note_subreason(CensusDiagnosticSubreason::GeographyParse(
                            failure.predicate(),
                        ));
                        self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                        return Err(map_adapter_error(failure.into_source()));
                    }
                }
            } else {
                match CensusDiscoveryDocument::parse(
                    request,
                    &response.body,
                    self.effective_parse_limits(),
                ) {
                    Ok(document) => document,
                    Err(error) => {
                        self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                        return Err(map_adapter_error(error));
                    }
                }
            };
            if is_catalog {
                diagnostic.note_subreason(CensusDiagnosticSubreason::CatalogAccounting);
            } else if is_geography {
                diagnostic.note_subreason(CensusDiagnosticSubreason::GeographyAccounting);
            }
            let metadata_entries = discovery_evidence(&document).returned_entries();
            let response_telemetry = response
                .telemetry_with_metadata(metadata_entries)
                .map_err(map_source_error)?;
            telemetry = telemetry
                .checked_add(response_telemetry)
                .map_err(map_source_error)?;
            response.record_success()?;
            diagnostic.record_success()?;
            self.telemetry
                .successful_responses
                .fetch_add(1, Ordering::Relaxed);
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
        diagnostic.enter(CensusDiagnosticPhase::MetadataClosure);
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
    pub(crate) async fn acquire_data(
        &self,
        authority: &ExtractionAuthority,
        metadata: &CensusMetadataBundle,
        deadline: Timestamp,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusCapturedData, ExtractionSourceError> {
        diagnostic.enter(CensusDiagnosticPhase::DataResponse);
        self.validate_authority(authority)?;
        let contract = self
            .config
            .contract(metadata.dataset_id())
            .ok_or_else(invalid_protocol)?;
        if metadata.query_digest != contract.query().request_digest() {
            return Err(invalid_protocol());
        }
        let mut response = self
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
                diagnostic,
            )
            .await?;
        let provisional_clocks = CensusClocks::local_first_observed(
            response.received_at,
            response.received_at,
            response.received_at,
        )
        .map_err(map_adapter_error)?;
        let page = match CensusDataPage::parse(
            contract.query(),
            metadata.selected_variables().map_err(map_source_error)?,
            &metadata
                .geography_admission(contract.query())
                .map_err(map_source_error)?,
            &response.body,
            self.effective_parse_limits(),
            provisional_clocks,
        ) {
            Ok(page) => page,
            Err(error) => {
                self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
                return Err(map_adapter_error(error));
            }
        };
        let (decoded_at, ingested_at) = self
            .processing_clock
            .sample_after_complete_parse(&page)
            .map_err(map_source_error)?;
        let page = page
            .try_with_completed_processing_clocks(decoded_at, ingested_at)
            .map_err(map_adapter_error)?;
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
        response.record_success()?;
        diagnostic.record_success()?;
        self.telemetry
            .successful_responses
            .fetch_add(1, Ordering::Relaxed);
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
    pub(crate) async fn acquire_dataset(
        &self,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusDatasetAcquisition, ExtractionSourceError> {
        let metadata = self
            .acquire_metadata(
                authority,
                provider_dataset,
                deadline,
                cancellation.clone(),
                diagnostic,
            )
            .await?;
        let data = self
            .acquire_data(authority, &metadata, deadline, cancellation, diagnostic)
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

    async fn discover_impl(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        activation: crate::CensusActivationCandidate,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusDiscoveryOutput, ExtractionSourceError> {
        self.validate_authority(&authority)?;
        if request.effective_at().is_some() || request.max_results() != 1 {
            return Err(invalid_protocol());
        }
        let contract = self
            .config
            .contract(request.dataset())
            .ok_or_else(invalid_protocol)?;
        let final_cancellation = cancellation.clone();
        let acquired = self
            .acquire_dataset(
                &authority,
                request.dataset(),
                request.deadline(),
                cancellation,
                diagnostic,
            )
            .await?;
        diagnostic.enter(CensusDiagnosticPhase::CaptureGraph);
        let capture_material = combined_capture_material(&self.metadata, contract, &acquired)?;
        let object = source_object(
            &self.metadata,
            &request,
            contract,
            &acquired,
            capture_material.receipt(),
        )?;
        let batch = DiscoveryBatch::try_new(&request, vec![object])?;
        let telemetry = acquired.telemetry();
        if final_cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let completed_at = system_timestamp().map_err(map_source_error)?;
        remaining_timeout(request.deadline(), completed_at, Duration::MAX)?;
        self.validate_authority(&authority)?;
        self.validate_activation(&activation, completed_at)
            .map_err(map_source_error)?;
        Ok(CensusDiscoveryOutput {
            batch,
            acquisition: acquired,
            capture_material,
            telemetry,
            activation,
        })
    }

    /// Discovers one exact source object and retains every response for raw sealing.
    pub async fn discover_with_activation(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        activation: crate::CensusActivationCandidate,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusDiscoveryOutput, ExtractionSourceError> {
        diagnostic.enter(CensusDiagnosticPhase::MetadataCatalog);
        let operation_at = system_timestamp().map_err(map_source_error)?;
        self.validate_activation(&activation, operation_at)
            .map_err(map_source_error)?;
        self.discover_impl(authority, request, activation, cancellation, diagnostic)
            .await
    }

    /// Consumes one physically sealed discovery and canonicalizes its retained acquisition without
    /// another provider request. The ordinary [`ExtractionSource::extract`] path is deliberately
    /// closed because its return type cannot enforce this seal-first admission.
    pub async fn extract_sealed_discovery(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        admission: CensusSealedDiscoveryAdmission,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<CensusSealedExtractionOutput, ExtractionSourceError> {
        diagnostic.enter(CensusDiagnosticPhase::SealedRejoin);
        self.validate_authority(&authority)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let operation_at = system_timestamp().map_err(map_source_error)?;
        remaining_timeout(request.deadline(), operation_at, Duration::MAX)?;
        let CensusSealedDiscoveryAdmission {
            batch: discovery_batch,
            acquisition,
            capture_token,
            telemetry,
            activation,
        } = admission;
        self.validate_activation(&activation, operation_at)
            .map_err(map_source_error)?;
        let [discovered_object] = discovery_batch.objects() else {
            return Err(invalid_protocol());
        };
        if request.object() != discovered_object
            || request.object().source_id() != self.metadata.source_id()
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
        verify_acquisition(
            &self.metadata,
            contract,
            &request,
            &object_identity,
            &acquisition,
            capture_token.persisted_receipt().capture(),
        )?;
        diagnostic.enter(CensusDiagnosticPhase::Canonicalize);
        let output = extraction_output(
            &self.metadata,
            &self.config,
            &request,
            contract,
            acquisition,
            capture_token.persisted_receipt().capture(),
        )?;
        let CensusExtractionOutput {
            batch,
            acquisition: _,
            publication_plan,
            telemetry: extraction_telemetry,
        } = output;
        if telemetry != extraction_telemetry {
            return Err(invalid_protocol());
        }
        let native_lineage = census_native_lineage(&publication_plan, &batch)?;
        let data_component = capture_token
            .persisted_receipt()
            .capture()
            .request_graph_components()
            .last()
            .ok_or_else(invalid_protocol)?;
        let data_page = capture_token
            .persisted_receipt()
            .capture()
            .pages()
            .get(usize::from(data_component.first_page_ordinal()))
            .ok_or_else(invalid_protocol)?;
        if data_component.dataset() != publication_plan.provider_dataset()
            || data_component.page_count().get() != 1
            || data_page.body_digest() != publication_plan.data_response_digest()
        {
            return Err(invalid_protocol());
        }
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(batch.records().len())
            .map_err(|_| invalid_protocol())?;
        row_capture_page_ordinals.extend(
            std::iter::repeat(data_component.first_page_ordinal()).take(batch.records().len()),
        );
        let sealed_capture_binding = SealedProviderCaptureBinding::try_whole(
            capture_token,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        )
        .map_err(|_| invalid_protocol())?;
        let candidate = crate::CensusPublicationCandidate::try_new(
            publication_plan,
            sealed_capture_binding,
            activation,
        )
        .map_err(map_source_error)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let completed_at = system_timestamp().map_err(map_source_error)?;
        remaining_timeout(request.deadline(), completed_at, Duration::MAX)?;
        self.validate_authority(&authority)?;
        candidate
            .validate_activation_at(completed_at)
            .map_err(map_source_error)?;
        Ok(CensusSealedExtractionOutput {
            candidate,
            telemetry,
        })
    }

    async fn fetch_authorized(
        &self,
        authority: &ExtractionAuthority,
        authorized: CensusAuthorizedUrl<'_>,
        request_digest: [u8; 32],
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<FetchedResponse, ExtractionSourceError> {
        self.fetch_authorized_with_limits(
            authority,
            authorized,
            request_digest,
            provider_dataset,
            deadline,
            cancellation,
            self.response_limit,
            self.request_timeout,
            diagnostic,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "authority, identity, deadline, cancellation, byte, and duration limits stay explicit"
    )]
    async fn fetch_authorized_with_limits(
        &self,
        authority: &ExtractionAuthority,
        authorized: CensusAuthorizedUrl<'_>,
        request_digest: [u8; 32],
        provider_dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
        response_limit: usize,
        request_timeout: Duration,
        diagnostic: &mut CensusDiagnosticJourney,
    ) -> Result<FetchedResponse, ExtractionSourceError> {
        self.validate_authority(authority)?;
        if request_digest != authorized.request_digest()
            || authorized.transport_url().as_str() != authorized.redacted_url()
        {
            return Err(invalid_protocol());
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        if response_limit == 0
            || response_limit > self.response_limit
            || request_timeout.is_zero()
            || request_timeout > self.request_timeout
        {
            return Err(invalid_protocol());
        }
        let credentialed = authorized.is_credentialed();
        let target = secret_target(&authorized).map_err(map_source_error)?;
        let permit =
            acquire_request_permit(authority, target.as_str(), deadline, cancellation.clone())
                .await?;
        let in_flight = permit.authorize_send(target.as_str())?;
        drop(target);
        let now = system_timestamp().map_err(map_source_error)?;
        let timeout = remaining_timeout(deadline, now, request_timeout)?;
        diagnostic.record_attempt()?;
        let result = self
            .transport
            .execute(
                CensusHttpRequest { authorized },
                &in_flight,
                response_limit,
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
        if response.key_error {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(if credentialed {
                SourceError::Unauthorized.into()
            } else {
                invalid_protocol()
            });
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
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type.as_deref())
        {
            self.telemetry.failures.fetch_add(1, Ordering::Relaxed);
            return Err(invalid_protocol());
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
        Ok(FetchedResponse {
            body: response.body,
            capture,
            rate_headers: response.rate_headers,
            received_at: response.received_at,
            latency: response.latency,
            in_flight: Some(in_flight),
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

#[derive(Debug)]
struct FetchedResponse {
    body: Bytes,
    capture: ProviderCaptureSetReceipt,
    rate_headers: CensusRateLimitHeaders,
    received_at: Timestamp,
    latency: Duration,
    in_flight: Option<InFlightExtractionRequest>,
}

impl FetchedResponse {
    fn record_success(&mut self) -> Result<(), ExtractionSourceError> {
        self.in_flight
            .take()
            .ok_or_else(invalid_protocol)?
            .record_success()
            .map_err(Into::into)
    }

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

fn metadata_diagnostic_phase(kind: &CensusDiscoveryKind) -> CensusDiagnosticPhase {
    match kind {
        CensusDiscoveryKind::Datasets | CensusDiscoveryKind::VintageDatasets { .. } => {
            CensusDiagnosticPhase::MetadataCatalog
        }
        CensusDiscoveryKind::Groups { .. } => CensusDiagnosticPhase::MetadataGroups,
        CensusDiscoveryKind::Variables { .. } | CensusDiscoveryKind::Group { .. } => {
            CensusDiagnosticPhase::MetadataVariables
        }
        CensusDiscoveryKind::Geographies { .. } => CensusDiagnosticPhase::MetadataGeography,
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
    let mut get_coordinates = BTreeSet::<String>::new();
    match contract.query().selection() {
        CensusSelection::Variables { .. } => {
            get_coordinates.extend(
                contract
                    .query()
                    .selection()
                    .wire_variables()
                    .iter()
                    .map(|variable| variable.as_str().to_owned()),
            );
        }
        CensusSelection::Group { .. } => {
            get_coordinates.extend(
                selected_variables
                    .variables()
                    .filter(|variable| !variable.is_context())
                    .map(|variable| variable.name().as_str().to_owned()),
            );
        }
    }
    let predicate_coordinates = contract
        .query()
        .predicates()
        .iter()
        .map(|predicate| predicate.variable().as_str().to_owned())
        .collect::<BTreeSet<_>>();
    if get_coordinates
        .iter()
        .any(|coordinate| predicate_coordinates.contains(coordinate))
    {
        return Err(CensusSourceError::Protocol);
    }
    let is_predicate_coordinate = |variable: &crate::CensusVariableMetadata| match (
        variable.predicate_type(),
        contract.query().geography(),
    ) {
        (CensusPredicateType::FipsFor, CensusGeography::Standard { for_clause, .. }) => {
            variable.name().as_str() == "for" || variable.name().as_str() == for_clause.level()
        }
        (CensusPredicateType::FipsIn, CensusGeography::Standard { in_clauses, .. }) => {
            !in_clauses.is_empty()
                && (variable.name().as_str() == "in"
                    || in_clauses
                        .iter()
                        .any(|clause| clause.level() == variable.name().as_str()))
        }
        (CensusPredicateType::Ucgid, CensusGeography::Uniform { .. }) => true,
        (CensusPredicateType::Time, _) => {
            variable.name().as_str() == "time" && contract.query().time().is_some()
        }
        _ => predicate_coordinates.contains(variable.name().as_str()),
    };
    for variable in selected_variables.variables() {
        let in_get = get_coordinates.contains(variable.name().as_str());
        let in_predicate = is_predicate_coordinate(variable);
        let referenced = in_get || in_predicate;
        if referenced && variable.provider_limit().is_some_and(|limit| limit != 0) {
            return Err(CensusSourceError::Protocol);
        }
        match variable.required() {
            CensusRequiredVariable::Required => {
                if usize::from(in_get) + usize::from(in_predicate) != 1 {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::RequiredPredicateOnly => {
                if in_get || !in_predicate {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::PredicateOnly => {
                if in_get {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::Optional
            | CensusRequiredVariable::DefaultDisplayed
            | CensusRequiredVariable::Unspecified => {}
        }
    }
    for variable in full_variables.variables() {
        let in_get = get_coordinates.contains(variable.name().as_str());
        let in_predicate = is_predicate_coordinate(variable);
        if (in_get
            || in_predicate
            || matches!(
                variable.required(),
                CensusRequiredVariable::Required | CensusRequiredVariable::RequiredPredicateOnly
            ))
            && variable.provider_limit().is_some_and(|limit| limit != 0)
        {
            return Err(CensusSourceError::Protocol);
        }
        match variable.required() {
            CensusRequiredVariable::Required => {
                if usize::from(in_get) + usize::from(in_predicate) != 1 {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::RequiredPredicateOnly => {
                if in_get || !in_predicate {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::PredicateOnly => {
                if in_get {
                    return Err(CensusSourceError::Protocol);
                }
            }
            CensusRequiredVariable::Optional
            | CensusRequiredVariable::DefaultDisplayed
            | CensusRequiredVariable::Unspecified => {}
        }
    }
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
    if let Some(key) = authorized.key_query_value() {
        url.query_pairs_mut().append_pair("key", key);
    }
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
    graph_capture: &ProviderCaptureSetReceipt,
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
    SourceObject::try_new_with_capture_identity(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        SourceIdentifier::try_from(CENSUS_JSON_MEDIA_TYPE).map_err(|_| invalid_protocol())?,
        evidence,
        SourceObjectCaptureIdentity::try_from_capture(graph_capture)
            .map_err(|_| invalid_protocol())?,
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
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    request: &ExtractionRequest,
    identity: &ParsedObjectId,
    acquired: &CensusDatasetAcquisition,
    graph_capture: &ProviderCaptureSetReceipt,
) -> Result<(), ExtractionSourceError> {
    let body = acquired.data().body();
    let body_bytes = u64::try_from(body.len()).map_err(|_| invalid_protocol())?;
    validate_acquisition_capture_graph(metadata, contract, acquired, graph_capture)
        .map_err(map_source_error)?;
    let capture_identity = SourceObjectCaptureIdentity::try_from_capture(graph_capture)
        .map_err(|_| invalid_protocol())?;
    if identity.metadata_digest != acquired.metadata().content_digest()
        || identity.body_digest != acquired.data().page().response_payload_digest()
        || identity.body_digest != sha256(body)
        || !payload_matches_exact_evidence(body, request.object().evidence())
        || request.object().capture_identity() != capture_identity
        || request
            .object()
            .expected_bytes()
            .is_some_and(|expected| expected != body_bytes)
    {
        return Err(SourceError::GenerationResynchronizationRequired.into());
    }
    Ok(())
}

fn validate_acquisition_capture_graph(
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    acquired: &CensusDatasetAcquisition,
    graph_capture: &ProviderCaptureSetReceipt,
) -> Result<(), CensusSourceError> {
    let component_count = acquired
        .metadata()
        .documents()
        .len()
        .checked_add(1)
        .ok_or(CensusSourceError::Protocol)?;
    if graph_capture.source_id() != metadata.source_id()
        || graph_capture.metadata_revision() != metadata.revision()
        || graph_capture.dataset() != contract.dataset_id()
        || graph_capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || graph_capture.request_graph_components().len() != component_count
        || graph_capture.semantic_binding().is_some()
    {
        return Err(CensusSourceError::Protocol);
    }

    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(component_count)
        .map_err(|_| CensusSourceError::Protocol)?;
    receipts.extend(
        acquired
            .metadata()
            .documents()
            .iter()
            .map(CensusCapturedDiscovery::capture),
    );
    receipts.push(acquired.data().capture());
    let expected_graph_identity =
        census_capture_graph_identity_from_receipts(metadata, contract, &receipts)?;
    if graph_capture.request_set_identity() != expected_graph_identity {
        return Err(CensusSourceError::Protocol);
    }

    let mut flattened_page_ordinal = 0_usize;
    for (component_ordinal, (component, receipt)) in graph_capture
        .request_graph_components()
        .iter()
        .zip(&receipts)
        .enumerate()
    {
        if receipt.source_id() != metadata.source_id()
            || receipt.metadata_revision() != metadata.revision()
            || receipt.dataset() != contract.dataset_id()
            || receipt.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || receipt.semantic_binding().is_some()
            || component.ordinal() as usize != component_ordinal
            || component.dataset() != receipt.dataset()
            || component.request_set_identity() != receipt.request_set_identity()
            || component.terminal() != receipt.terminal()
            || usize::from(component.first_page_ordinal()) != flattened_page_ordinal
            || usize::from(component.page_count().get()) != receipt.pages().len()
            || component.total_body_bytes() != receipt.total_body_bytes()
            || component.content_digest() != receipt.content_digest()
            || component.observation_digest() != receipt.observation_digest()
        {
            return Err(CensusSourceError::Protocol);
        }
        for standalone_page in receipt.pages() {
            let graph_page = graph_capture
                .pages()
                .get(flattened_page_ordinal)
                .ok_or(CensusSourceError::Protocol)?;
            let flattened_ordinal =
                u16::try_from(flattened_page_ordinal).map_err(|_| CensusSourceError::Protocol)?;
            if graph_page.ordinal() != flattened_ordinal
                || graph_page.request_identity() != standalone_page.request_identity()
                || graph_page.request_page_token_digest()
                    != standalone_page.request_page_token_digest()
                || graph_page.response_next_page_token_digest()
                    != standalone_page.response_next_page_token_digest()
                || graph_page.http_status() != standalone_page.http_status()
                || graph_page.body_bytes() != standalone_page.body_bytes()
                || graph_page.body_digest() != standalone_page.body_digest()
                || graph_page.received_at() != standalone_page.received_at()
            {
                return Err(CensusSourceError::Protocol);
            }
            flattened_page_ordinal = flattened_page_ordinal
                .checked_add(1)
                .ok_or(CensusSourceError::Protocol)?;
        }
    }
    if flattened_page_ordinal != graph_capture.pages().len() {
        return Err(CensusSourceError::Protocol);
    }
    Ok(())
}

fn extraction_output(
    metadata: &SourceMetadata,
    config: &CensusSourceConfig,
    request: &ExtractionRequest,
    contract: &CensusDatasetContract,
    acquisition: CensusDatasetAcquisition,
    capture_receipt: &ProviderCaptureSetReceipt,
) -> Result<CensusExtractionOutput, ExtractionSourceError> {
    let canonical = canonical_records(metadata, contract, acquisition.data().page())
        .map_err(map_source_error)?;
    if canonical.len() > request.max_records() as usize {
        return Err(
            market_squawk_sources::ExtractionError::RecordLimitExceeded {
                requested: request.max_records(),
            }
            .into(),
        );
    }
    let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
        .map_err(|_| invalid_protocol())?;
    let mut bindings = Vec::new();
    bindings
        .try_reserve_exact(canonical.len())
        .map_err(|_| invalid_protocol())?;
    let records = canonical
        .into_iter()
        .map(|record| {
            bindings.push(record.binding);
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
    let batch = batch
        .try_bind_provider_capture(capture_receipt)
        .map_err(|_| invalid_protocol())?;
    let publication_plan = crate::runtime::build_publication_plan(
        metadata,
        config,
        contract,
        &acquisition,
        &batch,
        capture_receipt,
        bindings.into_boxed_slice(),
    )
    .map_err(map_source_error)?;
    let telemetry = acquisition.telemetry();
    Ok(CensusExtractionOutput {
        batch,
        acquisition,
        publication_plan,
        telemetry,
    })
}

#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
struct CensusNativeLineageRowV1<'a> {
    dataset: &'a crate::CensusDataset,
    provider_variable: &'a SourceIdentifier,
    label: &'a str,
    concept: Option<&'a str>,
    group: Option<&'a SourceIdentifier>,
    geography: &'a crate::CensusGeographyValue,
    predicates: &'a [crate::CensusPredicateValue],
    reported_time: Option<&'a crate::CensusReportedTime>,
    value_state: &'a CensusValueState,
}

fn census_native_lineage(
    plan: &crate::CensusPublicationPlan,
    batch: &ExtractionBatch,
) -> Result<ProviderNativeLineageBatch, ExtractionSourceError> {
    if plan.observations().len() != batch.records().len() || plan.observations().is_empty() {
        return Err(invalid_protocol());
    }
    let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::CensusTabularV1,
        batch,
    )
    .map_err(|_| invalid_protocol())?;
    // Response-wide closure (query, metadata, clocks, capture graph, accounting, and canonical
    // bindings) cannot be reconstructed safely from any one row. Retain the exact validated plan
    // as the atomic macro publisher's bounded semantic companion.
    native_lineage
        .try_set_batch_sidecar(plan)
        .map_err(|_| invalid_protocol())?;
    for observation in plan.observations() {
        native_lineage
            .try_push(&CensusNativeLineageRowV1 {
                dataset: observation.dataset(),
                provider_variable: observation.provider_variable(),
                label: observation.variable_label(),
                concept: observation.concept(),
                group: observation.group(),
                geography: observation.geography(),
                predicates: observation.predicates(),
                reported_time: observation.reported_time(),
                value_state: observation.value_state(),
            })
            .map_err(|_| invalid_protocol())?;
    }
    native_lineage.finish().map_err(|_| invalid_protocol())
}

fn combined_capture_material(
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    acquisition: &CensusDatasetAcquisition,
) -> Result<ProviderCaptureMaterial, ExtractionSourceError> {
    let captures = capture_materials(metadata, acquisition)?;
    let graph_identity =
        census_capture_graph_identity(metadata, contract, &captures).map_err(map_source_error)?;
    ProviderCaptureMaterial::try_combine_request_graph(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        contract.dataset_id().clone(),
        graph_identity,
        captures.into_vec(),
    )
    .map_err(|_| invalid_protocol())
}

fn census_capture_graph_identity(
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    captures: &[ProviderCaptureMaterial],
) -> Result<EvidenceDigest, CensusSourceError> {
    let mut receipts = Vec::new();
    receipts
        .try_reserve_exact(captures.len())
        .map_err(|_| CensusSourceError::Protocol)?;
    receipts.extend(captures.iter().map(ProviderCaptureMaterial::receipt));
    census_capture_graph_identity_from_receipts(metadata, contract, &receipts)
}

fn census_capture_graph_identity_from_receipts(
    metadata: &SourceMetadata,
    contract: &CensusDatasetContract,
    receipts: &[&ProviderCaptureSetReceipt],
) -> Result<EvidenceDigest, CensusSourceError> {
    if receipts.is_empty() {
        return Err(CensusSourceError::Protocol);
    }
    let mut digest = Sha256::new();
    crate::update_digest_component(
        &mut digest,
        b"market-squawk/census-metadata-data-request-graph/v1",
    );
    crate::update_digest_component(&mut digest, metadata.source_id().as_str().as_bytes());
    crate::update_digest_component(
        &mut digest,
        metadata
            .revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    crate::update_digest_component(&mut digest, contract.dataset_id().as_str().as_bytes());
    crate::update_digest_component(
        &mut digest,
        &u64::try_from(receipts.len())
            .map_err(|_| CensusSourceError::Protocol)?
            .to_be_bytes(),
    );
    for receipt in receipts {
        if receipt.source_id() != metadata.source_id()
            || receipt.metadata_revision() != metadata.revision()
            || receipt.dataset() != contract.dataset_id()
        {
            return Err(CensusSourceError::Protocol);
        }
        crate::update_digest_component(&mut digest, &receipt.request_set_identity().bytes());
        crate::update_digest_component(&mut digest, &receipt.content_digest().bytes());
        crate::update_digest_component(&mut digest, &receipt.observation_digest().bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
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
    let digest = hash.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
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
    binding: crate::CensusCanonicalObservationBinding,
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
        let series = scoped_series(mapping, observation)?;
        let source_identifier = SourceIdentifier::try_from(format!(
            "census:v1:family:{}:content:{}",
            lower_hex(observation.revision_candidate().family_digest()),
            lower_hex(observation.revision_candidate().content_digest()),
        ))
        .map_err(|_| CensusSourceError::Protocol)?;
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
            source_identifier: source_identifier.clone(),
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
                series.clone(),
                canonical_decimal(value)?,
                mapping.unit().clone(),
            ),
            CensusValueState::Missing {
                reason,
                annotations,
            } => MacroObservation::missing(
                context,
                series.clone(),
                canonical_missing(mapping, *reason, annotations)?,
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
        let canonical_ordinal =
            u64::try_from(records.len()).map_err(|_| CensusSourceError::Protocol)?;
        let binding = crate::CensusCanonicalObservationBinding::new(
            canonical_ordinal,
            observation,
            effective.clone(),
            series,
            source_identifier,
            mapping.unit().clone(),
        );
        records.push(CanonicalCensusRecord {
            effective,
            availability: market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
            revision,
            evidence: ExactPayloadEvidence::from_content_digest(evidence_digest(payload_digest)),
            payload,
            binding,
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

fn canonical_missing(
    mapping: &CensusVariableMapping,
    reason: CensusMissingReason,
    annotations: &[crate::CensusAnnotation],
) -> Result<MacroMissingValue, CensusSourceError> {
    match (reason, annotations.is_empty()) {
        (CensusMissingReason::JsonNull, true) => Ok(MacroMissingValue::new(
            SourceIdentifier::try_from("null").map_err(|_| CensusSourceError::Protocol)?,
            Some(
                SourceIdentifier::try_from("census-json-null")
                    .map_err(|_| CensusSourceError::Protocol)?,
            ),
        )),
        (CensusMissingReason::EmptyString, true) => Ok(MacroMissingValue::new(
            SourceIdentifier::try_from("\"\"").map_err(|_| CensusSourceError::Protocol)?,
            Some(
                SourceIdentifier::try_from("census-empty-string")
                    .map_err(|_| CensusSourceError::Protocol)?,
            ),
        )),
        (CensusMissingReason::ProviderAnnotatedMissing, false) => mapping
            .annotated_missing(annotations)
            .cloned()
            .ok_or(CensusSourceError::Protocol),
        (CensusMissingReason::JsonNull, false)
        | (CensusMissingReason::EmptyString, false)
        | (CensusMissingReason::ProviderAnnotatedMissing, true)
        | (CensusMissingReason::AnnotationColumnMissing, _) => Err(CensusSourceError::Protocol),
    }
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
    observation: &crate::CensusObservation,
) -> Result<SourceIdentifier, CensusSourceError> {
    let stable_scope = serde_json::to_vec(&(
        observation.dataset().path(),
        observation.variable(),
        observation.geography().canonical_row_identity_digest(),
        observation.predicates(),
    ))
    .map_err(|_| CensusSourceError::Protocol)?;
    let mut digest = Sha256::new();
    crate::update_digest_component(
        &mut digest,
        b"market-squawk/census-canonical-series-scope/v1",
    );
    crate::update_digest_component(&mut digest, &stable_scope);
    let stable_scope_digest: [u8; 32] = digest.finalize().into();
    SourceIdentifier::try_from(format!(
        "{}:scope:{}",
        mapping.series_namespace(),
        lower_hex(stable_scope_digest)
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
        | CensusSourceError::Revision(_)
        | CensusSourceError::RateDeclaration(_) => invalid_protocol(),
    }
}

fn census_configuration_digest(
    contracts: &[CensusDatasetContract],
    parse_limits: CensusParseLimits,
) -> Result<EvidenceDigest, CensusSourceError> {
    let wire = serde_json::to_vec(&(contracts, parse_limits))
        .map_err(|_| CensusSourceError::InvalidConfiguration)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/census-source-config/v4");
    digest.update(
        u64::try_from(wire.len())
            .map_err(|_| CensusSourceError::InvalidConfiguration)?
            .to_be_bytes(),
    );
    digest.update(wire);
    Ok(evidence_digest(digest.finalize().into()))
}

#[cfg(test)]
mod tests;
