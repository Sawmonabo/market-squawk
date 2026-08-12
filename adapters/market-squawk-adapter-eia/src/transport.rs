//! Authority-bound EIA HTTP transport and terminal offset-page acquisition.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    ApiEndpointRule, BudgetWindowSemantics, ExtractionAuthority, ExtractionAuthorityError,
    ExtractionRequestPermit, ExtractionSourceError, HttpRequestBounds, MAX_PROVIDER_CAPTURE_BYTES,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, MAX_PROVIDER_CAPTURE_PAGES, NetworkAccessPolicy,
    NetworkPolicyError, PathScope, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    QueryParameterRule, QuerySensitivity, SourceError, SourceMetadata, SourceMetadataProvider,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER, USER_AGENT,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::capacity::matches_application_provider_budget;
use crate::types::{digest_bytes, digest_parts};
use crate::{
    EIA_API_ROOT, EiaAcquisition, EiaApiKey, EiaAuthenticatedRequest, EiaDataPage,
    EiaDataPageReceipt, EiaDatasetContract, EiaDigest, EiaError, EiaFacetCatalog,
    EiaMetadataRequest, EiaPageCompleteness, EiaParseLimits, EiaRouteMetadata,
};

const USER_AGENT_VALUE: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " eia-v2-adapter"
);
const JSON_MEDIA_TYPE: &str = "application/json";
const EIA_POLICY_ROOT: &str = "https://api.eia.gov/v2";
const MAX_API_KEY_QUERY_BYTES: u16 = 1_024;
const MAX_ENCODED_API_KEY_QUERY_BYTES: u16 = 4_096;
const MAX_EIA_QUERY_PARAMETERS: usize = 64;
const MAX_EIA_QUERY_RULES: usize = 32;
const MAX_EIA_ENCODED_QUERY_BYTES: u16 = 16_384;

/// Bounded HTTP and whole-acquisition limits for the EIA source transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaTransportLimits {
    parse: EiaParseLimits,
    max_page_bytes: usize,
    max_pages: u16,
    max_acquisition_bytes: u64,
}

impl EiaTransportLimits {
    /// Constructs limits no broader than the shared raw-capture boundary.
    pub fn try_new(
        parse: EiaParseLimits,
        max_page_bytes: usize,
        max_pages: u16,
        max_acquisition_bytes: u64,
    ) -> Result<Self, EiaSourceTransportError> {
        if max_page_bytes == 0
            || u64::try_from(max_page_bytes)
                .map_or(true, |bytes| bytes > MAX_PROVIDER_CAPTURE_PAGE_BYTES)
            || max_page_bytes > parse.max_body_bytes()
            || max_pages == 0
            || usize::from(max_pages) > MAX_PROVIDER_CAPTURE_PAGES
            || max_acquisition_bytes == 0
            || max_acquisition_bytes > MAX_PROVIDER_CAPTURE_BYTES
            || max_acquisition_bytes < u64::try_from(max_page_bytes).unwrap_or(u64::MAX)
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        Ok(Self {
            parse,
            max_page_bytes,
            max_pages,
            max_acquisition_bytes,
        })
    }

    /// Returns conservative limits aligned to the current source-neutral capture contract.
    pub const fn production_defaults() -> Self {
        Self {
            parse: EiaParseLimits::production_defaults(),
            max_page_bytes: MAX_PROVIDER_CAPTURE_PAGE_BYTES as usize,
            max_pages: MAX_PROVIDER_CAPTURE_PAGES as u16,
            max_acquisition_bytes: MAX_PROVIDER_CAPTURE_BYTES,
        }
    }

    /// Returns parser bounds applied after bounded HTTP capture.
    pub const fn parse_limits(self) -> EiaParseLimits {
        self.parse
    }

    /// Returns the largest admitted transport body for one page.
    pub const fn max_page_bytes(self) -> usize {
        self.max_page_bytes
    }

    /// Returns the largest admitted terminal page set.
    pub const fn max_pages(self) -> u16 {
        self.max_pages
    }

    /// Returns the aggregate sanitized raw-material ceiling.
    pub const fn max_acquisition_bytes(self) -> u64 {
        self.max_acquisition_bytes
    }
}

/// One successful HTTP request receipt containing no credential-bearing target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaHttpReceipt {
    request_digest: EiaDigest,
    transport_payload_digest: EiaDigest,
    retained_payload_digest: EiaDigest,
    secret_free_url: Box<str>,
    status: u16,
    response_bytes: u64,
    retained_bytes: u64,
    latency: Duration,
    received_at: Timestamp,
}

impl EiaHttpReceipt {
    /// Returns the SHA-256 identity of the credential-free request URL.
    pub const fn request_digest(&self) -> EiaDigest {
        self.request_digest
    }

    /// Returns the digest of exact ephemeral transport bytes before response redaction.
    pub const fn transport_payload_digest(&self) -> EiaDigest {
        self.transport_payload_digest
    }

    /// Returns the digest of exact sanitized bytes admitted for persistence.
    pub const fn retained_payload_digest(&self) -> EiaDigest {
        self.retained_payload_digest
    }

    /// Returns the exact credential-free request URL admitted for diagnostics and receipts.
    pub fn secret_free_url(&self) -> &str {
        &self.secret_free_url
    }

    /// Returns the successful HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns exact bytes received before secret redaction.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns exact sanitized bytes retained for raw capture.
    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    /// Returns measured request-send through complete-body latency.
    pub const fn latency(&self) -> Duration {
        self.latency
    }

    /// Returns the wall-clock observation after the complete response body arrived.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
}

/// Sanitized bounded response material and its source-neutral capture receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaRawPageMaterial {
    payload: Arc<[u8]>,
    http: EiaHttpReceipt,
    capture: ProviderCapturePageReceipt,
}

impl EiaRawPageMaterial {
    /// Returns exact sanitized JSON bytes; the API key is never present.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns exact transport and redaction evidence.
    pub const fn http_receipt(&self) -> &EiaHttpReceipt {
        &self.http
    }

    /// Returns the source-neutral page receipt for raw-capture composition.
    pub const fn capture_receipt(&self) -> &ProviderCapturePageReceipt {
        &self.capture
    }
}

/// One route-metadata discovery result with complete standalone raw lineage.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaRouteMetadataRetrieval {
    metadata: EiaRouteMetadata,
    raw: EiaRawPageMaterial,
    capture: ProviderCaptureMaterial,
}

impl EiaRouteMetadataRetrieval {
    /// Returns validated route, facet, frequency, and field metadata.
    pub const fn metadata(&self) -> &EiaRouteMetadata {
        &self.metadata
    }

    /// Returns sanitized raw metadata material.
    pub const fn raw_page(&self) -> &EiaRawPageMaterial {
        &self.raw
    }

    /// Returns the terminal standalone capture receipt.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.capture.receipt()
    }

    /// Returns the terminal receipt and ordered sanitized raw records ready for `MSJ1` sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the indivisible metadata handoff. Composition seals `capture` before publishing
    /// any canonical schema derived from `metadata`.
    pub fn into_parts(
        self,
    ) -> (
        EiaRouteMetadata,
        EiaRawPageMaterial,
        ProviderCaptureMaterial,
    ) {
        (self.metadata, self.raw, self.capture)
    }
}

/// One facet-value discovery result with complete standalone raw lineage.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaFacetMetadataRetrieval {
    catalog: EiaFacetCatalog,
    raw: EiaRawPageMaterial,
    capture: ProviderCaptureMaterial,
}

impl EiaFacetMetadataRetrieval {
    /// Returns the validated complete facet catalog.
    pub const fn catalog(&self) -> &EiaFacetCatalog {
        &self.catalog
    }

    /// Returns sanitized raw facet material.
    pub const fn raw_page(&self) -> &EiaRawPageMaterial {
        &self.raw
    }

    /// Returns the terminal standalone capture receipt.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.capture.receipt()
    }

    /// Returns the terminal receipt and ordered sanitized raw records ready for `MSJ1` sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the indivisible facet-metadata handoff for capture-first composition.
    pub fn into_parts(self) -> (EiaFacetCatalog, EiaRawPageMaterial, ProviderCaptureMaterial) {
        (self.catalog, self.raw, self.capture)
    }
}

/// Sanitized material and row-count evidence for one ordered data page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDataPageMaterial {
    raw: EiaRawPageMaterial,
    data: EiaDataPageReceipt,
}

impl EiaDataPageMaterial {
    /// Returns sanitized raw response material.
    pub const fn raw_page(&self) -> &EiaRawPageMaterial {
        &self.raw
    }

    /// Returns requested, returned, missing, offset, total, and completion evidence.
    pub const fn data_receipt(&self) -> &EiaDataPageReceipt {
        &self.data
    }
}

/// Exact transport totals for one terminally closed data acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EiaDataTransportReceipt {
    requests: u16,
    requested_rows: u64,
    returned_rows: u64,
    observations: u64,
    missing_observations: u64,
    response_bytes: u64,
    retained_bytes: u64,
    latency: Duration,
}

impl EiaDataTransportReceipt {
    /// Returns provider HTTP requests used by the complete offset chain.
    pub const fn requests(self) -> u16 {
        self.requests
    }

    /// Returns requested page slots; this is deliberately distinct from returned rows.
    pub const fn requested_rows(self) -> u64 {
        self.requested_rows
    }

    /// Returns rows actually returned and validated.
    pub const fn returned_rows(self) -> u64 {
        self.returned_rows
    }

    /// Returns typed field observations emitted from returned rows.
    pub const fn observations(self) -> u64 {
        self.observations
    }

    /// Returns explicit provider-missing typed observations.
    pub const fn missing_observations(self) -> u64 {
        self.missing_observations
    }

    /// Returns exact transport bytes before response redaction.
    pub const fn response_bytes(self) -> u64 {
        self.response_bytes
    }

    /// Returns exact sanitized bytes admitted for raw capture.
    pub const fn retained_bytes(self) -> u64 {
        self.retained_bytes
    }

    /// Returns the checked sum of measured request latencies.
    pub const fn latency(self) -> Duration {
        self.latency
    }
}

/// Complete typed EIA acquisition plus bounded raw and terminal capture evidence.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaDataRetrieval {
    dataset: SourceIdentifier,
    acquisition: EiaAcquisition,
    pages: Box<[EiaDataPageMaterial]>,
    capture: ProviderCaptureMaterial,
    transport: EiaDataTransportReceipt,
}

impl EiaDataRetrieval {
    /// Returns the stable query-bound provider dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns typed observations and exact offset-chain completeness.
    pub const fn acquisition(&self) -> &EiaAcquisition {
        &self.acquisition
    }

    /// Returns ordered sanitized raw pages and per-page metrics.
    pub fn pages(&self) -> &[EiaDataPageMaterial] {
        &self.pages
    }

    /// Returns the terminal source-neutral capture receipt.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.capture.receipt()
    }

    /// Returns the terminal receipt and ordered sanitized raw records ready for `MSJ1` sealing.
    ///
    /// Application composition must durably seal this material before canonical publication.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Returns exact aggregate request, row, byte, missing, and latency metrics.
    pub const fn transport_receipt(&self) -> EiaDataTransportReceipt {
        self.transport
    }

    /// Consumes the indivisible data handoff. Composition seals `capture` before publishing any
    /// canonical observations derived from `acquisition`.
    pub fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        EiaAcquisition,
        Box<[EiaDataPageMaterial]>,
        EiaDataTransportReceipt,
        ProviderCaptureMaterial,
    ) {
        (
            self.dataset,
            self.acquisition,
            self.pages,
            self.transport,
            self.capture,
        )
    }
}

/// Closed transport/configuration failure that never retains an API key or authenticated URL.
#[derive(Debug, Error)]
pub enum EiaSourceTransportError {
    /// Source metadata, query policy, or transport bounds are incompatible.
    #[error("invalid EIA source transport configuration")]
    InvalidConfiguration,
    /// Provider-native parsing, schema, pagination, unit, value, or clock failure.
    #[error(transparent)]
    Protocol(#[from] EiaError),
    /// Registry, provider-rate, cancellation, deadline, or network source failure.
    #[error(transparent)]
    Extraction(#[from] ExtractionSourceError),
    /// Source-neutral capture receipt rejected the response set.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    /// Sanitized response material could not enter the shared bounded raw-record envelope.
    #[error(transparent)]
    RawCapture(#[from] RawCaptureRecordError),
    /// The bounded acquisition would require more pages than admitted.
    #[error("EIA acquisition exceeds maximum page count {max}")]
    PageLimitExceeded {
        /// Configured acquisition-page ceiling.
        max: u16,
    },
    /// The bounded acquisition crossed its aggregate sanitized-byte ceiling.
    #[error("EIA acquisition exceeds maximum sanitized bytes {max}")]
    AcquisitionTooLarge {
        /// Configured aggregate sanitized-byte ceiling.
        max: u64,
    },
    /// Wall-clock or duration arithmetic was unavailable.
    #[error("EIA transport clock is unavailable")]
    ClockUnavailable,
}

impl From<ExtractionAuthorityError> for EiaSourceTransportError {
    fn from(error: ExtractionAuthorityError) -> Self {
        Self::Extraction(ExtractionSourceError::Authority(error))
    }
}

/// Builds the metadata and exact-query endpoint rules required by an EIA source registration.
///
/// The metadata rule admits only a secret `api_key` beneath `/v2`. The data rule binds one exact
/// route and requires the query's fixed public coordinates while bounding repeated fields/facets.
pub fn eia_api_endpoint_rules(
    query: &crate::EiaDataQuery,
) -> Result<Vec<ApiEndpointRule>, NetworkPolicyError> {
    let api_key = query_rule(
        "api_key",
        MAX_API_KEY_QUERY_BYTES,
        false,
        QuerySensitivity::Secret,
    )?;
    let metadata = ApiEndpointRule::try_new(
        EIA_POLICY_ROOT,
        PathScope::Descendants,
        vec![api_key.clone()],
        1,
        MAX_ENCODED_API_KEY_QUERY_BYTES,
    )?;

    let mut rules = vec![
        api_key,
        query_rule("data[]", 512, true, QuerySensitivity::Public)?,
    ];
    rules.push(exact_query_rule("frequency", query.frequency().as_str())?);
    if let Some(start) = query.start() {
        rules.push(exact_query_rule("start", start)?);
    }
    if let Some(end) = query.end() {
        rules.push(exact_query_rule("end", end)?);
    }
    for facet in query.facets() {
        rules.push(query_rule(
            &format!("facets[{}][]", facet.facet()),
            512,
            true,
            QuerySensitivity::Public,
        )?);
    }
    for (index, sort) in query.sorts().iter().enumerate() {
        rules.push(exact_query_rule(
            &format!("sort[{index}][column]"),
            sort.column().as_str(),
        )?);
        rules.push(exact_query_rule(
            &format!("sort[{index}][direction]"),
            sort.direction().as_query_value(),
        )?);
    }
    rules.push(query_rule("offset", 20, false, QuerySensitivity::Public)?);
    rules.push(exact_query_rule("length", &query.length().to_string())?);
    rules.push(exact_query_rule("out", "json")?);

    let parameter_count = 1_usize
        .checked_add(query.data_fields().len())
        .and_then(|count| {
            query.facets().iter().try_fold(count, |count, facet| {
                count.checked_add(facet.values().len())
            })
        })
        .and_then(|count| count.checked_add(1))
        .and_then(|count| count.checked_add(usize::from(query.start().is_some())))
        .and_then(|count| count.checked_add(usize::from(query.end().is_some())))
        .and_then(|count| count.checked_add(query.sorts().len().saturating_mul(2)))
        .and_then(|count| count.checked_add(3))
        .filter(|count| *count <= MAX_EIA_QUERY_PARAMETERS)
        .ok_or(NetworkPolicyError::InvalidRequestBounds)?;
    if rules.len() > MAX_EIA_QUERY_RULES {
        return Err(NetworkPolicyError::InvalidRequestBounds);
    }
    let data_endpoint = format!("{}{}/data", EIA_API_ROOT, query.route());
    let data = ApiEndpointRule::try_new(
        &data_endpoint,
        PathScope::Exact,
        rules,
        u8::try_from(parameter_count).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        MAX_EIA_ENCODED_QUERY_BYTES,
    )?;
    Ok(vec![metadata, data])
}

/// Authority-bound production EIA API v2 transport.
pub struct EiaSourceTransport {
    metadata: SourceMetadata,
    api_key: EiaApiKey,
    limits: EiaTransportLimits,
    transport: Arc<dyn EiaHttpTransport>,
    total_timeout: Duration,
    metadata_dataset_prefix: Box<str>,
}

impl std::fmt::Debug for EiaSourceTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EiaSourceTransport")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("api_key", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl EiaSourceTransport {
    /// Binds one injected key and one immutable source registration to the hardened HTTP client.
    pub fn try_new(
        metadata: SourceMetadata,
        api_key: EiaApiKey,
        limits: EiaTransportLimits,
    ) -> Result<Self, EiaSourceTransportError> {
        validate_metadata(&metadata)?;
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        };
        let bounds = policy.request_bounds();
        let transport = Arc::new(ReqwestEiaTransport::try_new(bounds)?);
        Ok(Self::from_transport(
            metadata, api_key, limits, transport, bounds,
        ))
    }

    #[cfg(test)]
    pub(crate) fn try_new_with_transport(
        metadata: SourceMetadata,
        api_key: EiaApiKey,
        limits: EiaTransportLimits,
        transport: Arc<dyn EiaHttpTransport>,
    ) -> Result<Self, EiaSourceTransportError> {
        validate_metadata(&metadata)?;
        let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        };
        let bounds = policy.request_bounds();
        Ok(Self::from_transport(
            metadata, api_key, limits, transport, bounds,
        ))
    }

    fn from_transport(
        metadata: SourceMetadata,
        api_key: EiaApiKey,
        limits: EiaTransportLimits,
        transport: Arc<dyn EiaHttpTransport>,
        bounds: HttpRequestBounds,
    ) -> Self {
        Self {
            metadata,
            api_key,
            limits,
            transport,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            metadata_dataset_prefix: "eia-v2-metadata".into(),
        }
    }

    /// Discovers and validates one route's hierarchy, frequencies, facets, and data columns.
    pub async fn discover_route_metadata(
        &self,
        authority: &ExtractionAuthority,
        request: &EiaMetadataRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaRouteMetadataRetrieval, EiaSourceTransportError> {
        let authenticated = request.authenticate(&self.api_key)?;
        let parsed = self
            .fetch_parsed(
                authority,
                authenticated,
                deadline,
                cancellation.clone(),
                |bytes, received_at| {
                    let metadata = crate::parse_route_metadata(
                        bytes,
                        request,
                        received_at,
                        self.limits.parse,
                    )?;
                    Ok(ParsedResponse {
                        request_digest: metadata.receipt().request_digest(),
                        transport_payload_digest: metadata.receipt().transport_payload_digest(),
                        retained_payload_digest: metadata.receipt().retained_payload_digest(),
                        received_at: metadata.receipt().received_at(),
                        retained_payload: metadata.retained_payload_arc(),
                        value: metadata,
                    })
                },
            )
            .await?;
        let dataset =
            metadata_dataset_identifier(&self.metadata_dataset_prefix, parsed.http.request_digest)?;
        let raw = raw_page(parsed.evidence(), 0, None, None)?;
        let capture = standalone_capture(&self.metadata, dataset, &raw)?;
        let capture = provider_capture_material(capture, [&raw])?;
        Ok(EiaRouteMetadataRetrieval {
            metadata: parsed.value,
            raw,
            capture,
        })
    }

    /// Discovers and validates the complete value catalog for one exact route facet.
    pub async fn discover_facet_metadata(
        &self,
        authority: &ExtractionAuthority,
        request: &EiaMetadataRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaFacetMetadataRetrieval, EiaSourceTransportError> {
        let authenticated = request.authenticate(&self.api_key)?;
        let parsed = self
            .fetch_parsed(
                authority,
                authenticated,
                deadline,
                cancellation,
                |bytes, received_at| {
                    let catalog = crate::parse_facet_metadata(
                        bytes,
                        request,
                        received_at,
                        self.limits.parse,
                    )?;
                    Ok(ParsedResponse {
                        request_digest: catalog.receipt().request_digest(),
                        transport_payload_digest: catalog.receipt().transport_payload_digest(),
                        retained_payload_digest: catalog.receipt().retained_payload_digest(),
                        received_at: catalog.receipt().received_at(),
                        retained_payload: catalog.retained_payload_arc(),
                        value: catalog,
                    })
                },
            )
            .await?;
        let dataset =
            metadata_dataset_identifier(&self.metadata_dataset_prefix, parsed.http.request_digest)?;
        let raw = raw_page(parsed.evidence(), 0, None, None)?;
        let capture = standalone_capture(&self.metadata, dataset, &raw)?;
        let capture = provider_capture_material(capture, [&raw])?;
        Ok(EiaFacetMetadataRetrieval {
            catalog: parsed.value,
            raw,
            capture,
        })
    }

    /// Retrieves every offset page and returns only after exact provider-total closure.
    pub async fn retrieve_data(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaDataRetrieval, EiaSourceTransportError> {
        self.validate_authority(authority)?;
        let dataset = eia_data_dataset_identifier(contract)?;
        let mut offset = 0_u64;
        let mut ordinal = 0_u16;
        let mut aggregate_retained_bytes = 0_u64;
        let mut fetched = Vec::new();
        loop {
            if ordinal >= self.limits.max_pages {
                return Err(EiaSourceTransportError::PageLimitExceeded {
                    max: self.limits.max_pages,
                });
            }
            let page = self
                .fetch_data_page(
                    authority,
                    contract,
                    offset,
                    ordinal,
                    deadline,
                    cancellation.clone(),
                )
                .await?;
            aggregate_retained_bytes = aggregate_retained_bytes
                .checked_add(page.raw.http.retained_bytes)
                .filter(|bytes| *bytes <= self.limits.max_acquisition_bytes)
                .ok_or(EiaSourceTransportError::AcquisitionTooLarge {
                    max: self.limits.max_acquisition_bytes,
                })?;
            let completeness = page.page.receipt().completeness();
            fetched.push(page);
            match completeness {
                EiaPageCompleteness::More { next_offset } => {
                    offset = next_offset;
                    ordinal = ordinal.checked_add(1).ok_or(
                        EiaSourceTransportError::PageLimitExceeded {
                            max: self.limits.max_pages,
                        },
                    )?;
                }
                EiaPageCompleteness::Complete => break,
            }
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        authority
            .validate_current()
            .map_err(ExtractionSourceError::from)?;

        let mut typed_pages = Vec::with_capacity(fetched.len());
        let mut raw_pages = Vec::with_capacity(fetched.len());
        for page in fetched {
            typed_pages.push(page.page);
            raw_pages.push(EiaDataPageMaterial {
                raw: page.raw,
                data: page.data_receipt,
            });
        }
        let acquisition = EiaAcquisition::try_from_pages(typed_pages)?;
        let transport = aggregate_transport_receipt(&raw_pages, &acquisition)?;
        let capture_pages = raw_pages
            .iter()
            .map(|page| page.raw.capture.clone())
            .collect();
        let capture = ProviderCaptureSetReceipt::try_new(
            self.metadata.source_id().clone(),
            self.metadata.revision().clone(),
            dataset.clone(),
            evidence_digest(contract.query().identity()),
            ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
            capture_pages,
        )?;
        let capture = provider_capture_material(
            capture,
            raw_pages.iter().map(EiaDataPageMaterial::raw_page),
        )?;
        Ok(EiaDataRetrieval {
            dataset,
            acquisition,
            pages: raw_pages.into_boxed_slice(),
            capture,
            transport,
        })
    }

    async fn fetch_data_page(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        offset: u64,
        ordinal: u16,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedDataPage, EiaSourceTransportError> {
        if (ordinal == 0) != (offset == 0) {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let request = contract.query().page(offset);
        let authenticated = request.authenticate(&self.api_key)?;
        let parsed = self
            .fetch_parsed(
                authority,
                authenticated,
                deadline,
                cancellation,
                |bytes, received_at| {
                    let page = EiaDataPage::parse(
                        bytes,
                        request,
                        contract,
                        received_at,
                        self.limits.parse,
                    )?;
                    Ok(ParsedResponse {
                        request_digest: page.receipt().request_digest(),
                        transport_payload_digest: page.receipt().transport_payload_digest(),
                        retained_payload_digest: page.receipt().retained_payload_digest(),
                        received_at: page.receipt().received_at(),
                        retained_payload: page.retained_payload_arc(),
                        value: page,
                    })
                },
            )
            .await?;
        let request_token = (offset != 0).then(|| offset_token_digest(offset));
        let next_token = match parsed.value.receipt().completeness() {
            EiaPageCompleteness::More { next_offset } => Some(offset_token_digest(next_offset)),
            EiaPageCompleteness::Complete => None,
        };
        let raw = raw_page(parsed.evidence(), ordinal, request_token, next_token)?;
        Ok(FetchedDataPage {
            data_receipt: parsed.value.receipt().clone(),
            page: parsed.value,
            raw,
        })
    }

    async fn fetch_parsed<T, F>(
        &self,
        authority: &ExtractionAuthority,
        request: EiaAuthenticatedRequest,
        deadline: Timestamp,
        cancellation: CancellationToken,
        parse: F,
    ) -> Result<FetchedParsed<T>, EiaSourceTransportError>
    where
        F: FnOnce(&[u8], Timestamp) -> Result<ParsedResponse<T>, EiaError>,
    {
        self.validate_authority(authority)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        let request_digest = request.request_digest();
        let (authenticated_url, secret_free_url) = request.into_urls()?;
        let authenticated_target = authenticated_url.as_str();
        let permit = acquire_request_permit(
            authority,
            authenticated_target,
            deadline,
            cancellation.clone(),
        )
        .await?;
        if !permit.authorization().contains_sensitive_query() {
            permit.release();
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let bounds = permit.request_bounds()?;
        let timeout = remaining_timeout(deadline, self.total_timeout)?;
        let max_bytes = usize::try_from(bounds.max_response_bytes())
            .unwrap_or(usize::MAX)
            .min(self.limits.max_page_bytes);
        let in_flight = permit.authorize_send(authenticated_target)?;
        let response = self
            .transport
            .execute(
                EiaHttpRequest {
                    authenticated_url,
                    secret_free_url: secret_free_url.clone(),
                },
                max_bytes,
                timeout,
                cancellation.clone(),
                &in_flight,
            )
            .await?;
        if response.status == 429 {
            let retry_deadline =
                in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
            return Err(ExtractionSourceError::Source(SourceError::BudgetWaitUntil {
                deadline: retry_deadline,
            })
            .into());
        }
        if matches!(response.status, 401 | 403) {
            return Err(ExtractionSourceError::Source(SourceError::Unauthorized).into());
        }
        if response.status != 200 {
            return Err(ExtractionSourceError::Source(SourceError::ProviderUnavailable).into());
        }
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
            || !content_type_is_json(response.content_type.as_deref())
            || response.body.is_empty()
        {
            return Err(ExtractionSourceError::Source(SourceError::InvalidProtocolState).into());
        }
        let response_bytes = u64::try_from(response.body.len())
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        in_flight.validate_response_size(response_bytes)?;
        let parsed = parse(&response.body, response.received_at)?;
        if parsed.request_digest != request_digest
            || parsed.received_at != response.received_at
            || parsed.transport_payload_digest != digest_bytes(&response.body)
            || parsed.retained_payload_digest != digest_bytes(&parsed.retained_payload)
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let retained_bytes = u64::try_from(parsed.retained_payload.len())
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        if retained_bytes == 0
            || retained_bytes > u64::try_from(self.limits.max_page_bytes).unwrap_or(u64::MAX)
        {
            return Err(EiaSourceTransportError::AcquisitionTooLarge {
                max: u64::try_from(self.limits.max_page_bytes).unwrap_or(u64::MAX),
            });
        }
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        ensure_deadline(deadline)?;
        authority
            .validate_current()
            .map_err(ExtractionSourceError::from)?;
        in_flight.release();
        Ok(FetchedParsed {
            value: parsed.value,
            retained_payload: parsed.retained_payload,
            http: EiaHttpReceipt {
                request_digest,
                transport_payload_digest: parsed.transport_payload_digest,
                retained_payload_digest: parsed.retained_payload_digest,
                secret_free_url: secret_free_url.as_str().into(),
                status: response.status,
                response_bytes,
                retained_bytes,
                latency: response.latency,
                received_at: response.received_at,
            },
        })
    }

    fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), EiaSourceTransportError> {
        authority
            .validate_current()
            .map_err(ExtractionSourceError::from)?;
        if authority.metadata() != &self.metadata {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        Ok(())
    }
}

impl SourceMetadataProvider for EiaSourceTransport {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

struct ParsedResponse<T> {
    value: T,
    request_digest: EiaDigest,
    transport_payload_digest: EiaDigest,
    retained_payload_digest: EiaDigest,
    received_at: Timestamp,
    retained_payload: Arc<[u8]>,
}

struct FetchedParsed<T> {
    value: T,
    retained_payload: Arc<[u8]>,
    http: EiaHttpReceipt,
}

impl<T> FetchedParsed<T> {
    fn evidence(&self) -> ParsedMaterial {
        ParsedMaterial {
            payload: Arc::clone(&self.retained_payload),
            http: self.http.clone(),
        }
    }
}

struct FetchedDataPage {
    page: EiaDataPage,
    raw: EiaRawPageMaterial,
    data_receipt: EiaDataPageReceipt,
}

struct ParsedMaterial {
    payload: Arc<[u8]>,
    http: EiaHttpReceipt,
}

fn raw_page(
    parsed: ParsedMaterial,
    ordinal: u16,
    request_token: Option<EvidenceDigest>,
    next_token: Option<EvidenceDigest>,
) -> Result<EiaRawPageMaterial, EiaSourceTransportError> {
    let capture = ProviderCapturePageReceipt::try_new(
        ordinal,
        evidence_digest(parsed.http.request_digest),
        request_token,
        next_token,
        parsed.http.status,
        parsed.http.retained_bytes,
        evidence_digest(parsed.http.retained_payload_digest),
        parsed.http.received_at,
    )?;
    Ok(EiaRawPageMaterial {
        payload: parsed.payload,
        http: parsed.http,
        capture,
    })
}

fn standalone_capture(
    metadata: &SourceMetadata,
    dataset: SourceIdentifier,
    raw: &EiaRawPageMaterial,
) -> Result<ProviderCaptureSetReceipt, EiaSourceTransportError> {
    Ok(ProviderCaptureSetReceipt::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        dataset,
        evidence_digest(raw.http.request_digest),
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![raw.capture.clone()],
    )?)
}

fn provider_capture_material<'a>(
    capture: ProviderCaptureSetReceipt,
    pages: impl IntoIterator<Item = &'a EiaRawPageMaterial>,
) -> Result<ProviderCaptureMaterial, EiaSourceTransportError> {
    // The observation digest binds source, dataset, ordered pages, and every receive time, making
    // it an exact deterministic identity for this one completed response set rather than a stable
    // retry/content identity.
    let connection_id = Uuid::new_v5(&Uuid::NAMESPACE_URL, &capture.observation_digest().bytes());
    if connection_id.is_nil() {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    let source: Arc<str> = Arc::from(capture.source_id().as_str());
    let mut records = Vec::with_capacity(capture.pages().len());
    for (expected_ordinal, page) in pages.into_iter().enumerate() {
        let ordinal = u16::try_from(expected_ordinal)
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        if page.capture.ordinal() != ordinal {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let ordinal_bytes = ordinal.to_be_bytes();
        let body_digest = page.capture.body_digest().bytes();
        let event_identity = digest_parts(
            b"market-squawk/eia-provider-capture-event/v1",
            [ordinal_bytes.as_slice(), body_digest.as_slice()],
        );
        let event_id = Uuid::new_v5(&connection_id, &event_identity.bytes());
        if event_id.is_nil() {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        records.push(RawCaptureRecord::try_new_live(
            event_id,
            Arc::clone(&source),
            connection_id,
            Some(u64::from(ordinal)),
            None,
            DateTime::<Utc>::from_timestamp_nanos(page.http.received_at.unix_nanos()),
            Bytes::copy_from_slice(&page.payload),
        )?);
    }
    Ok(ProviderCaptureMaterial::try_new(capture, records)?)
}

fn aggregate_transport_receipt(
    pages: &[EiaDataPageMaterial],
    acquisition: &EiaAcquisition,
) -> Result<EiaDataTransportReceipt, EiaSourceTransportError> {
    let mut receipt = EiaDataTransportReceipt {
        requests: 0,
        requested_rows: 0,
        returned_rows: 0,
        observations: 0,
        missing_observations: 0,
        response_bytes: 0,
        retained_bytes: 0,
        latency: Duration::ZERO,
    };
    for page in pages {
        receipt.requests = receipt
            .requests
            .checked_add(1)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.requested_rows = receipt
            .requested_rows
            .checked_add(u64::from(page.data.requested_length()))
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.returned_rows = receipt
            .returned_rows
            .checked_add(page.data.returned_rows())
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.observations = receipt
            .observations
            .checked_add(page.data.observation_count())
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.missing_observations = receipt
            .missing_observations
            .checked_add(page.data.missing_observation_count())
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.response_bytes = receipt
            .response_bytes
            .checked_add(page.raw.http.response_bytes)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.retained_bytes = receipt
            .retained_bytes
            .checked_add(page.raw.http.retained_bytes)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        receipt.latency = receipt
            .latency
            .checked_add(page.raw.http.latency)
            .ok_or(EiaSourceTransportError::ClockUnavailable)?;
    }
    let acquisition_receipt = acquisition.receipt();
    if u32::from(receipt.requests) != acquisition_receipt.page_count()
        || receipt.returned_rows != acquisition_receipt.returned_rows()
        || receipt.observations != acquisition_receipt.observation_count()
        || receipt.missing_observations != acquisition_receipt.missing_observation_count()
        || receipt.response_bytes != acquisition_receipt.response_bytes()
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    Ok(receipt)
}

/// Derives the stable capture dataset identity for one frozen EIA query contract.
pub fn eia_data_dataset_identifier(
    contract: &EiaDatasetContract,
) -> Result<SourceIdentifier, EiaSourceTransportError> {
    digest_identifier("eia-v2-data", contract.query().identity())
}

fn metadata_dataset_identifier(
    prefix: &str,
    digest: EiaDigest,
) -> Result<SourceIdentifier, EiaSourceTransportError> {
    digest_identifier(prefix, digest)
}

fn digest_identifier(
    prefix: &str,
    digest: EiaDigest,
) -> Result<SourceIdentifier, EiaSourceTransportError> {
    SourceIdentifier::try_from(format!("{prefix}-{}", lower_hex(digest.bytes())))
        .map_err(|_| EiaSourceTransportError::InvalidConfiguration)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn evidence_digest(digest: EiaDigest) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.bytes())
}

fn offset_token_digest(offset: u64) -> EvidenceDigest {
    let offset = offset.to_be_bytes();
    evidence_digest(digest_parts(b"eia-v2-offset-token-v1", [offset.as_slice()]))
}

fn query_rule(
    key: &str,
    max_value_bytes: u16,
    allow_multiple: bool,
    sensitivity: QuerySensitivity,
) -> Result<QueryParameterRule, NetworkPolicyError> {
    QueryParameterRule::try_new(
        SourceIdentifier::try_from(key).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        max_value_bytes,
        allow_multiple,
        sensitivity,
    )
}

fn exact_query_rule(key: &str, value: &str) -> Result<QueryParameterRule, NetworkPolicyError> {
    QueryParameterRule::try_new_exact_public(
        SourceIdentifier::try_from(key).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
        SourceIdentifier::try_from(value).map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
    )
}

fn validate_metadata(metadata: &SourceMetadata) -> Result<(), EiaSourceTransportError> {
    let NetworkAccessPolicy::Allowlisted(_) = metadata.network_policy() else {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    };
    let budget = metadata
        .budget_policy()
        .filter(|budget| matches_application_provider_budget(budget))
        .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
    if budget
        .window(0)
        .is_none_or(|window| window.semantics() != BudgetWindowSemantics::Sliding)
        || !metadata.capabilities().extraction()
        || metadata.capabilities().live()
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    Ok(())
}

async fn acquire_request_permit(
    authority: &ExtractionAuthority,
    target: &str,
    deadline: Timestamp,
    cancellation: CancellationToken,
) -> Result<ExtractionRequestPermit, EiaSourceTransportError> {
    loop {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        match authority.try_network_request(target) {
            Ok(permit) => return Ok(permit),
            Err(ExtractionAuthorityError::BudgetWaitUntil {
                deadline: budget_deadline,
            }) => {
                let wait = authority.remaining_budget_wait(budget_deadline)?;
                let remaining = remaining_timeout(deadline, wait)?;
                if wait > remaining {
                    return Err(ExtractionSourceError::DeadlineExceeded.into());
                }
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(ExtractionSourceError::Cancelled.into());
                    }
                    () = tokio::time::sleep(wait) => {}
                }
            }
            Err(error) => return Err(ExtractionSourceError::Authority(error).into()),
        }
    }
}

fn remaining_timeout(
    deadline: Timestamp,
    configured_total: Duration,
) -> Result<Duration, EiaSourceTransportError> {
    let now = system_timestamp()?;
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|nanos| u64::try_from(nanos).ok())
        .filter(|nanos| *nanos > 0)
        .map(Duration::from_nanos)
        .ok_or(ExtractionSourceError::DeadlineExceeded)?;
    Ok(remaining.min(configured_total))
}

fn ensure_deadline(deadline: Timestamp) -> Result<(), EiaSourceTransportError> {
    if system_timestamp()? >= deadline {
        Err(ExtractionSourceError::DeadlineExceeded.into())
    } else {
        Ok(())
    }
}

fn system_timestamp() -> Result<Timestamp, EiaSourceTransportError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| EiaSourceTransportError::ClockUnavailable)?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(EiaSourceTransportError::ClockUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[derive(Debug)]
pub(crate) struct EiaHttpRequest {
    authenticated_url: Url,
    secret_free_url: Url,
}

#[derive(Clone, Debug)]
pub(crate) struct EiaHttpResponse {
    status: u16,
    retry_after: Option<Box<[u8]>>,
    content_encoding: Option<Box<[u8]>>,
    content_type: Option<Box<[u8]>>,
    body: Bytes,
    received_at: Timestamp,
    latency: Duration,
}

pub(crate) trait EiaHttpTransport: std::fmt::Debug + Send + Sync {
    fn execute<'a>(
        &'a self,
        request: EiaHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
        in_flight: &'a market_squawk_sources::InFlightExtractionRequest,
    ) -> BoxFuture<'a, Result<EiaHttpResponse, EiaSourceTransportError>>;
}

#[derive(Debug)]
struct ReqwestEiaTransport {
    client: reqwest::Client,
}

impl ReqwestEiaTransport {
    fn try_new(bounds: HttpRequestBounds) -> Result<Self, EiaSourceTransportError> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_backend_rustls()
            .min_tls_version(reqwest::tls::Version::TLS_1_2)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
            .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
            .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
            .build()
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        Ok(Self { client })
    }
}

impl EiaHttpTransport for ReqwestEiaTransport {
    fn execute<'a>(
        &'a self,
        request: EiaHttpRequest,
        max_bytes: usize,
        timeout: Duration,
        cancellation: CancellationToken,
        in_flight: &'a market_squawk_sources::InFlightExtractionRequest,
    ) -> BoxFuture<'a, Result<EiaHttpResponse, EiaSourceTransportError>> {
        Box::pin(async move {
            let operation = async {
                if request.authenticated_url.scheme() != "https"
                    || request.authenticated_url.host_str() != Some("api.eia.gov")
                    || request.authenticated_url.fragment().is_some()
                    || request
                        .secret_free_url
                        .query_pairs()
                        .any(|(key, _)| key.eq_ignore_ascii_case("api_key"))
                {
                    return Err(EiaSourceTransportError::InvalidConfiguration);
                }
                let started = Instant::now();
                let response = self
                    .client
                    .get(request.authenticated_url)
                    .header(ACCEPT, JSON_MEDIA_TYPE)
                    .header(ACCEPT_ENCODING, "identity")
                    .header(USER_AGENT, USER_AGENT_VALUE)
                    .send()
                    .await
                    .map_err(|_| ExtractionSourceError::Source(SourceError::Network))?;
                if response.content_length().is_some_and(|length| {
                    usize::try_from(length).map_or(true, |length| length > max_bytes)
                }) {
                    return Err(ExtractionSourceError::Source(SourceError::FrameTooLarge {
                        max: max_bytes,
                    })
                    .into());
                }
                let status = response.status().as_u16();
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes().into());
                let content_encoding = response
                    .headers()
                    .get(CONTENT_ENCODING)
                    .map(|value| value.as_bytes().into());
                let content_type = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .map(|value| value.as_bytes().into());
                let body =
                    collect_bounded_stream(response.bytes_stream(), max_bytes, in_flight).await?;
                Ok(EiaHttpResponse {
                    status,
                    retry_after,
                    content_encoding,
                    content_type,
                    body,
                    received_at: system_timestamp()?,
                    latency: started.elapsed(),
                })
            };
            tokio::select! {
                biased;
                () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled.into()),
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| EiaSourceTransportError::Extraction(
                        ExtractionSourceError::DeadlineExceeded
                    ))?
                }
            }
        })
    }
}

async fn collect_bounded_stream<S, E>(
    mut stream: S,
    max_bytes: usize,
    in_flight: &market_squawk_sources::InFlightExtractionRequest,
) -> Result<Bytes, EiaSourceTransportError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut body = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        in_flight.validate_current()?;
        let chunk = chunk.map_err(|_| ExtractionSourceError::Source(SourceError::Network))?;
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or(ExtractionSourceError::Source(SourceError::FrameTooLarge {
                max: max_bytes,
            }))?;
        if next > max_bytes {
            return Err(ExtractionSourceError::Source(SourceError::FrameTooLarge {
                max: max_bytes,
            })
            .into());
        }
        in_flight.validate_response_size(
            u64::try_from(next).map_err(|_| EiaSourceTransportError::InvalidConfiguration)?,
        )?;
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn content_type_is_json(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(JSON_MEDIA_TYPE))
}

#[cfg(test)]
pub(crate) use test_seam::{EiaHttpResponseFixture, EiaMockTransport};

#[cfg(test)]
mod test_seam {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Debug)]
    pub(crate) struct EiaHttpResponseFixture {
        pub(crate) status: u16,
        pub(crate) retry_after: Option<Box<[u8]>>,
        pub(crate) content_encoding: Option<Box<[u8]>>,
        pub(crate) content_type: Option<Box<[u8]>>,
        pub(crate) body: Bytes,
        pub(crate) received_at: Timestamp,
        pub(crate) latency: Duration,
    }

    #[derive(Debug)]
    pub(crate) struct EiaMockTransport {
        pub(crate) responses: Mutex<VecDeque<EiaHttpResponseFixture>>,
        pub(crate) safe_urls: Mutex<Vec<String>>,
        pub(crate) expected_api_key: Box<str>,
    }

    impl EiaHttpTransport for EiaMockTransport {
        fn execute<'a>(
            &'a self,
            request: EiaHttpRequest,
            max_bytes: usize,
            _timeout: Duration,
            cancellation: CancellationToken,
            in_flight: &'a market_squawk_sources::InFlightExtractionRequest,
        ) -> BoxFuture<'a, Result<EiaHttpResponse, EiaSourceTransportError>> {
            Box::pin(async move {
                if cancellation.is_cancelled() {
                    return Err(ExtractionSourceError::Cancelled.into());
                }
                in_flight.validate_current()?;
                let key_matches = request.authenticated_url.query_pairs().any(|(key, value)| {
                    key == "api_key" && value == self.expected_api_key.as_ref()
                });
                if !key_matches
                    || request
                        .secret_free_url
                        .query_pairs()
                        .any(|(key, _)| key == "api_key")
                {
                    return Err(EiaSourceTransportError::InvalidConfiguration);
                }
                self.safe_urls
                    .lock()
                    .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?
                    .push(request.secret_free_url.into());
                let fixture = self
                    .responses
                    .lock()
                    .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?
                    .pop_front()
                    .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
                if fixture.body.len() > max_bytes {
                    return Err(ExtractionSourceError::Source(SourceError::FrameTooLarge {
                        max: max_bytes,
                    })
                    .into());
                }
                Ok(EiaHttpResponse {
                    status: fixture.status,
                    retry_after: fixture.retry_after,
                    content_encoding: fixture.content_encoding,
                    content_type: fixture.content_type,
                    body: fixture.body,
                    received_at: fixture.received_at,
                    latency: fixture.latency,
                })
            })
        }
    }
}
