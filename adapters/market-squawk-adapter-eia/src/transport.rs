//! Authority-bound EIA HTTP transport and terminal offset-page acquisition.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{io, mem::size_of};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
    checked_arc_bytes_allocation_bytes, checked_arc_str_allocation_bytes,
};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationMode, BudgetWindowSemantics, CoverageDomain, ExtractionAuthority,
    ExtractionAuthorityError, ExtractionRequestPermit, ExtractionSourceError, HistoricalCapability,
    HttpRequestBounds, MAX_OBSERVED_REVISION_BATCH_BYTES, MAX_PROVIDER_CAPTURE_BYTES,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, MAX_PROVIDER_CAPTURE_PAGES, NetworkAccessPolicy,
    NetworkPolicyError, PathScope, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, ProviderOrderedCaptureSegments,
    ProviderWholeCaptureToken, QueryParameterRule, QuerySensitivity, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt, SourceClass, SourceError, SourceMetadata,
    SourceMetadataProvider,
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
    EiaMetadataRequest, EiaPageCompleteness, EiaPaginationTracker, EiaParseLimits,
    EiaRouteMetadata,
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
#[derive(Debug)]
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
#[derive(Debug, Eq, PartialEq)]
pub struct EiaDataPageMaterial {
    raw: EiaRawPageMaterial,
    data: EiaDataPageReceipt,
    root_journal_rejoin: EiaRootPageJournalRejoin,
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

    /// Returns exact page coordinates the root-owned SQLite journal must persist before advancing.
    ///
    /// This value is deliberately non-serializable and is not a provider checkpoint. Root
    /// composition owns durable encoding, transactionality, currentness, and restart admission.
    pub const fn root_journal_rejoin(&self) -> &EiaRootPageJournalRejoin {
        &self.root_journal_rejoin
    }

    pub(crate) fn into_root_journal_rejoin(self) -> EiaRootPageJournalRejoin {
        self.root_journal_rejoin
    }
}

/// Non-authoritative exact coordinates for one page in the root-owned durable offset journal.
///
/// The accompanying [`EiaDataPageMaterial`] retains the actual sanitized bytes and actual shared
/// page-capture receipt. This adapter neither persists this value nor claims that a restart is
/// admitted merely because the coordinates can be constructed.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaRootPageJournalRejoin {
    source_metadata: Arc<SourceMetadata>,
    provider_dataset: SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: crate::EiaApiVersion,
    page_ordinal: u16,
    offset: u64,
    next_offset: Option<u64>,
    provider_total: u64,
    capture_receipt: ProviderCapturePageReceipt,
}

impl EiaRootPageJournalRejoin {
    /// Returns the exact current source and authorization generation for root comparison.
    pub fn source_metadata(&self) -> &SourceMetadata {
        self.source_metadata.as_ref()
    }

    pub(crate) fn source_metadata_arc(&self) -> &Arc<SourceMetadata> {
        &self.source_metadata
    }

    /// Returns the exact provider-query raw dataset.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the immutable base-query identity shared by every page in the chain.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns the frozen metadata/query/native-schema identity.
    pub const fn contract_schema_digest(&self) -> EiaDigest {
        self.contract_schema_digest
    }

    /// Returns the provider API version observed by this page.
    pub const fn api_version(&self) -> &crate::EiaApiVersion {
        &self.api_version
    }

    /// Returns the contiguous zero-based page ordinal.
    pub const fn page_ordinal(&self) -> u16 {
        self.page_ordinal
    }

    /// Returns the exact offset used by this request.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the provider-derived next offset, or `None` only for terminal total closure.
    pub const fn next_offset(&self) -> Option<u64> {
        self.next_offset
    }

    /// Returns the provider total that must remain constant across a resumed chain.
    pub const fn provider_total(&self) -> u64 {
        self.provider_total
    }

    /// Returns the actual source-neutral page receipt, including request/next-token continuity.
    pub const fn capture_receipt(&self) -> &ProviderCapturePageReceipt {
        &self.capture_receipt
    }

    /// Revalidates these coordinates against current root source/contract state and actual bytes.
    pub fn validate(
        &self,
        current_source: &SourceMetadata,
        contract: &EiaDatasetContract,
        material: &EiaDataPageMaterial,
    ) -> Result<(), EiaSourceTransportError> {
        if current_source != self.source_metadata.as_ref()
            || contract.query().identity() != self.query_digest
            || contract.schema_digest() != self.contract_schema_digest
            || contract.metadata().api_version() != &self.api_version
            || &material.root_journal_rejoin != self
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        validate_page_journal_rejoin(
            self.source_metadata.as_ref(),
            &self.provider_dataset,
            contract,
            self.page_ordinal,
            &material.raw,
            &material.data,
            self,
        )
    }
}

/// Linear, non-serializable continuation for one root-controlled EIA offset acquisition.
///
/// Construction and transitions remain adapter-owned. Root can obtain the next continuation only
/// by sealing and rejoining the exact page returned by [`EiaPendingDataPage`].
#[derive(Debug)]
pub struct EiaDataAcquisitionCursor {
    source_metadata: Arc<SourceMetadata>,
    provider_dataset: SourceIdentifier,
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: crate::EiaApiVersion,
    max_pages: u16,
    max_publication_bytes: usize,
    next_ordinal: u16,
    next_offset: u64,
    retained_bytes: u64,
    publication_retained_bytes: usize,
    raw_capture_copy_retained_bytes: usize,
    pagination_tracker: Option<EiaPaginationTracker>,
    typed_pages: Vec<EiaDataPage>,
    page_materials: Vec<EiaDataPageMaterial>,
    sealed_pages: Vec<ProviderWholeCaptureToken>,
}

impl EiaDataAcquisitionCursor {
    /// Returns the only offset the next adapter request may use.
    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Returns the only contiguous ordinal the next adapter request may use.
    pub const fn next_ordinal(&self) -> u16 {
        self.next_ordinal
    }

    /// Returns exact already-sealed page count. No page is added before physical rejoin succeeds.
    pub fn sealed_page_count(&self) -> usize {
        self.sealed_pages.len()
    }
}

/// One fetched page whose exact root-journal seal must be rejoined before any next-page request.
#[derive(Debug)]
pub struct EiaPendingDataPage {
    cursor: EiaDataAcquisitionCursor,
    typed_page: EiaDataPage,
    page_material: EiaDataPageMaterial,
    page_capture_receipt: ProviderCaptureSetReceipt,
    raw_capture_copy_retained_bytes: usize,
    capture: ProviderCaptureMaterial,
}

impl EiaPendingDataPage {
    /// Returns the actual typed/raw page and its exact root-journal coordinates before sealing.
    pub const fn page_material(&self) -> &EiaDataPageMaterial {
        &self.page_material
    }

    /// Returns the standalone exact page capture root must durably seal.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the pending page into a linear rejoin state and actual shared capture material.
    pub fn into_parts(self) -> (EiaDataPageSealRejoin, ProviderCaptureSealRequest) {
        let (capture_expectation, seal_request) = self.capture.into_whole_seal_parts();
        (
            EiaDataPageSealRejoin {
                cursor: self.cursor,
                typed_page: self.typed_page,
                page_material: self.page_material,
                page_capture_receipt: self.page_capture_receipt,
                raw_capture_copy_retained_bytes: self.raw_capture_copy_retained_bytes,
                capture_expectation,
            },
            seal_request,
        )
    }
}

/// Linear page state awaiting one exact actual shared seal.
#[derive(Debug)]
pub struct EiaDataPageSealRejoin {
    cursor: EiaDataAcquisitionCursor,
    typed_page: EiaDataPage,
    page_material: EiaDataPageMaterial,
    page_capture_receipt: ProviderCaptureSetReceipt,
    raw_capture_copy_retained_bytes: usize,
    capture_expectation: ProviderCaptureSealExpectation,
}

impl EiaDataPageSealRejoin {
    /// Returns immutable page coordinates root binds to its durable journal transaction.
    pub const fn root_journal_rejoin(&self) -> &EiaRootPageJournalRejoin {
        self.page_material.root_journal_rejoin()
    }
}

/// Sole post-seal transition: another exact page or a terminal complete acquisition.
#[derive(Debug)]
pub enum EiaDataPageTransition {
    /// Root sealed the page and the provider total requires one exact next offset.
    More(EiaDataAcquisitionCursor),
    /// Root sealed the terminal page and the complete ordered chain is nonpublishable until its
    /// standalone page seals are consumed into canonical candidate construction.
    Complete(EiaDataRetrieval),
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
#[derive(Debug)]
pub struct EiaDataRetrieval {
    dataset: SourceIdentifier,
    acquisition: EiaAcquisition,
    pages: Box<[EiaDataPageMaterial]>,
    ordered_capture: ProviderOrderedCaptureSegments,
    transport: EiaDataTransportReceipt,
}

/// Linear terminal state carrying one ordered physical segment per exact logical response page.
///
/// This value has no public constructor or serialization and requires no combined reseal.
#[derive(Debug)]
pub struct EiaDataRetrievalSealRejoin {
    dataset: SourceIdentifier,
    acquisition: EiaAcquisition,
    pages: Box<[EiaDataPageMaterial]>,
    ordered_capture: ProviderOrderedCaptureSegments,
    transport: EiaDataTransportReceipt,
}

/// One offset-zero data response used only to prove credential, request-echo, and frozen-schema
/// health during activation. It is deliberately not a complete analytical acquisition.
#[derive(Debug, Eq, PartialEq)]
pub struct EiaDataProbeRetrieval {
    dataset: SourceIdentifier,
    page: EiaDataPage,
    raw: EiaRawPageMaterial,
    capture: ProviderCaptureMaterial,
}

impl EiaDataProbeRetrieval {
    /// Returns the provider dataset being probed.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the fully parsed first page, including its real terminal/more disposition.
    pub const fn page(&self) -> &EiaDataPage {
        &self.page
    }

    /// Returns the sanitized exact response material.
    pub const fn raw_page(&self) -> &EiaRawPageMaterial {
        &self.raw
    }

    /// Returns the standalone raw-capture material that must be sealed before activation.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the probe handoff for capture-first activation.
    pub fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        EiaDataPage,
        EiaRawPageMaterial,
        ProviderCaptureMaterial,
    ) {
        (self.dataset, self.page, self.raw, self.capture)
    }
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

    /// Returns the exact number of standalone physical page seals retained in chain order.
    pub fn sealed_page_count(&self) -> usize {
        self.ordered_capture.segment_count()
    }

    /// Returns persisted evidence for one standalone physical page seal.
    pub fn sealed_page_receipt(&self, ordinal: usize) -> Option<&SealedProviderCaptureSetReceipt> {
        self.ordered_capture.persisted_segment_receipt(ordinal)
    }

    /// Returns the terminal source-neutral capture receipt.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.ordered_capture.root_capture()
    }

    /// Returns exact aggregate request, row, byte, missing, and latency metrics.
    pub const fn transport_receipt(&self) -> EiaDataTransportReceipt {
        self.transport
    }

    /// Consumes the terminal acquisition into canonical-publication rejoin without resealing.
    pub fn into_publication_rejoin(self) -> EiaDataRetrievalSealRejoin {
        let Self {
            dataset,
            acquisition,
            pages,
            ordered_capture,
            transport,
        } = self;
        EiaDataRetrievalSealRejoin {
            dataset,
            acquisition,
            pages,
            ordered_capture,
            transport,
        }
    }
}

impl EiaDataRetrievalSealRejoin {
    /// Returns the complete logical capture joined to its ordered standalone physical seals.
    pub const fn capture_receipt(&self) -> &ProviderCaptureSetReceipt {
        self.ordered_capture.root_capture()
    }

    /// Returns persisted evidence for one standalone physical page seal.
    pub fn sealed_page_receipt(&self, ordinal: usize) -> Option<&SealedProviderCaptureSetReceipt> {
        self.ordered_capture.persisted_segment_receipt(ordinal)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        EiaAcquisition,
        Box<[EiaDataPageMaterial]>,
        EiaDataTransportReceipt,
        ProviderOrderedCaptureSegments,
    ) {
        (
            self.dataset,
            self.acquisition,
            self.pages,
            self.transport,
            self.ordered_capture,
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
    pub(crate) const fn max_pages(&self) -> u16 {
        self.limits.max_pages
    }

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

    /// Starts one linear root-controlled acquisition at exact offset zero.
    pub(crate) fn begin_data_retrieval(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
    ) -> Result<EiaDataAcquisitionCursor, EiaSourceTransportError> {
        self.begin_bounded_data_retrieval(
            authority,
            contract,
            self.limits.max_pages,
            MAX_OBSERVED_REVISION_BATCH_BYTES,
        )
    }

    /// Starts one application-bounded acquisition without reserving transport-wide capacity.
    pub(crate) fn begin_bounded_data_retrieval(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        max_pages: u16,
        max_publication_bytes: usize,
    ) -> Result<EiaDataAcquisitionCursor, EiaSourceTransportError> {
        self.validate_authority(authority)?;
        if max_pages == 0
            || max_pages > self.limits.max_pages
            || max_publication_bytes == 0
            || max_publication_bytes > MAX_OBSERVED_REVISION_BATCH_BYTES
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let dataset = eia_data_dataset_identifier(contract)?;
        let publication_retained_bytes = cursor_base_publication_retained_bytes(
            &self.metadata,
            &dataset,
            contract.metadata().api_version(),
            max_pages,
        )?;
        if publication_retained_bytes > max_publication_bytes {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let capacity = usize::from(max_pages);
        let mut typed_pages = Vec::new();
        typed_pages
            .try_reserve_exact(capacity)
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        let mut page_materials = Vec::new();
        page_materials
            .try_reserve_exact(capacity)
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        let mut sealed_pages = Vec::new();
        sealed_pages
            .try_reserve_exact(capacity)
            .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        Ok(EiaDataAcquisitionCursor {
            source_metadata: Arc::new(self.metadata.clone()),
            provider_dataset: dataset,
            query_digest: contract.query().identity(),
            contract_schema_digest: contract.schema_digest(),
            api_version: contract.metadata().api_version().clone(),
            max_pages,
            max_publication_bytes,
            next_ordinal: 0,
            next_offset: 0,
            retained_bytes: 0,
            publication_retained_bytes,
            raw_capture_copy_retained_bytes: 0,
            pagination_tracker: None,
            typed_pages,
            page_materials,
            sealed_pages,
        })
    }

    /// Fetches exactly one page and withholds every continuation until its actual seal is rejoined.
    pub(crate) async fn fetch_next_data_page(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        cursor: EiaDataAcquisitionCursor,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaPendingDataPage, EiaSourceTransportError> {
        self.validate_data_cursor(authority, contract, &cursor)?;
        if cursor.next_ordinal >= cursor.max_pages {
            return Err(EiaSourceTransportError::PageLimitExceeded {
                max: cursor.max_pages,
            });
        }
        let fetched = self
            .fetch_data_page(
                authority,
                contract,
                cursor.next_offset,
                cursor.next_ordinal,
                cursor.max_pages,
                cursor
                    .max_publication_bytes
                    .checked_sub(cursor.publication_retained_bytes)
                    .ok_or(EiaSourceTransportError::InvalidConfiguration)?,
                deadline,
                cancellation,
            )
            .await?;
        cursor
            .retained_bytes
            .checked_add(fetched.raw.http.retained_bytes)
            .filter(|bytes| *bytes <= self.limits.max_acquisition_bytes)
            .ok_or(EiaSourceTransportError::AcquisitionTooLarge {
                max: self.limits.max_acquisition_bytes,
            })?;
        let root_journal_rejoin = root_page_journal_rejoin(
            &cursor.source_metadata,
            &cursor.provider_dataset,
            contract,
            &fetched.raw,
            &fetched.data_receipt,
        )?;
        let page_material = EiaDataPageMaterial {
            raw: fetched.raw,
            data: fetched.data_receipt,
            root_journal_rejoin,
        };
        let chain_page = page_material.raw_page().capture_receipt();
        let page_dataset = root_page_journal_dataset(
            cursor.query_digest,
            cursor.next_ordinal,
            cursor.next_offset,
            chain_page.body_digest(),
        )?;
        let raw_capture_copy_retained_bytes = raw_capture_copy_working_set_bytes(
            &self.metadata,
            &page_dataset,
            [page_material.raw_page()],
        )?;
        cursor
            .publication_retained_bytes
            .checked_add(fetched.page.receipt().publication_retained_bytes())
            .and_then(|bytes| bytes.checked_add(raw_capture_copy_retained_bytes))
            .filter(|bytes| *bytes <= cursor.max_publication_bytes)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        let standalone_page = ProviderCapturePageReceipt::try_new(
            0,
            chain_page.request_identity(),
            None,
            None,
            chain_page.http_status(),
            chain_page.body_bytes(),
            chain_page.body_digest(),
            chain_page.received_at(),
        )?;
        let standalone_raw = EiaRawPageMaterial {
            payload: Arc::clone(&page_material.raw.payload),
            http: page_material.raw.http.clone(),
            capture: standalone_page,
        };
        let page_capture = standalone_capture(&self.metadata, page_dataset, &standalone_raw)?;
        let page_capture = provider_capture_material(page_capture, [&standalone_raw])?;
        let page_capture_receipt = page_capture.receipt().clone();
        Ok(EiaPendingDataPage {
            cursor,
            typed_page: fetched.page,
            page_material,
            page_capture_receipt,
            raw_capture_copy_retained_bytes,
            capture: page_capture,
        })
    }

    /// Rejoins one exact actual page seal and only then exposes the next offset or terminal chain.
    pub(crate) fn rejoin_data_page(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        rejoin: EiaDataPageSealRejoin,
        sealed_page: SealedProviderCaptureMaterial,
    ) -> Result<EiaDataPageTransition, EiaSourceTransportError> {
        self.validate_data_cursor(authority, contract, &rejoin.cursor)?;
        let sealed_page = rejoin
            .capture_expectation
            .try_rejoin(sealed_page)
            .and_then(|rejoined| rejoined.try_into_whole())?;
        let sealed_page_receipt = sealed_page.persisted_receipt();
        rejoin.page_material.root_journal_rejoin().validate(
            &self.metadata,
            contract,
            &rejoin.page_material,
        )?;
        validate_root_page_seal(
            rejoin.cursor.query_digest,
            rejoin.cursor.next_ordinal,
            &rejoin.page_material,
            sealed_page_receipt,
        )?;
        if sealed_page_receipt.capture() != &rejoin.page_capture_receipt
            || sealed_page_receipt.receipt_digest().bytes() == [0; 32]
            || sealed_page_receipt
                .segment()
                .physical_receipt_digest()
                .bytes()
                == [0; 32]
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let page_dataset = root_page_journal_dataset(
            rejoin.cursor.query_digest,
            rejoin.cursor.next_ordinal,
            rejoin.cursor.next_offset,
            rejoin.page_material.raw.capture.body_digest(),
        )?;
        let raw_capture_copy_retained_bytes = raw_capture_copy_working_set_bytes(
            &self.metadata,
            &page_dataset,
            [rejoin.page_material.raw_page()],
        )?;
        if raw_capture_copy_retained_bytes != rejoin.raw_capture_copy_retained_bytes {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let mut cursor = rejoin.cursor;
        let page_lineage_retained_bytes =
            page_lineage_publication_retained_bytes(&rejoin.page_material, sealed_page_receipt)?;
        cursor.retained_bytes = cursor
            .retained_bytes
            .checked_add(rejoin.page_material.raw.http.retained_bytes)
            .filter(|bytes| *bytes <= self.limits.max_acquisition_bytes)
            .ok_or(EiaSourceTransportError::AcquisitionTooLarge {
                max: self.limits.max_acquisition_bytes,
            })?;
        cursor.publication_retained_bytes = cursor
            .publication_retained_bytes
            .checked_add(rejoin.typed_page.receipt().publication_retained_bytes())
            .and_then(|bytes| bytes.checked_add(page_lineage_retained_bytes))
            .and_then(|bytes| bytes.checked_add(raw_capture_copy_retained_bytes))
            .filter(|bytes| *bytes <= cursor.max_publication_bytes)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        cursor.raw_capture_copy_retained_bytes = cursor
            .raw_capture_copy_retained_bytes
            .checked_add(raw_capture_copy_retained_bytes)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        let completeness = rejoin.page_material.data.completeness();
        let tracked_next_offset = match cursor.pagination_tracker.as_mut() {
            Some(tracker) => {
                tracker.push(&rejoin.typed_page)?;
                tracker.charge_publication_retained_bytes(
                    page_lineage_retained_bytes
                        .checked_add(raw_capture_copy_retained_bytes)
                        .ok_or(EiaSourceTransportError::InvalidConfiguration)?,
                )?;
                tracker.next_offset()
            }
            None if cursor.typed_pages.is_empty() && cursor.next_ordinal == 0 => {
                let mut tracker = EiaPaginationTracker::start(&rejoin.typed_page)?;
                let base_and_lineage = cursor_base_publication_retained_bytes(
                    cursor.source_metadata.as_ref(),
                    &cursor.provider_dataset,
                    &cursor.api_version,
                    cursor.max_pages,
                )?
                .checked_add(page_lineage_retained_bytes)
                .and_then(|bytes| bytes.checked_add(raw_capture_copy_retained_bytes))
                .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
                tracker.charge_publication_retained_bytes(base_and_lineage)?;
                let next_offset = tracker.next_offset();
                cursor.pagination_tracker = Some(tracker);
                next_offset
            }
            None => return Err(EiaSourceTransportError::InvalidConfiguration),
        };
        let receipt_next_offset = match completeness {
            EiaPageCompleteness::More { next_offset } => Some(next_offset),
            EiaPageCompleteness::Complete => None,
        };
        if tracked_next_offset != receipt_next_offset {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        cursor.typed_pages.push(rejoin.typed_page);
        cursor.page_materials.push(rejoin.page_material);
        cursor.sealed_pages.push(sealed_page);
        match completeness {
            EiaPageCompleteness::More { next_offset } => {
                cursor.next_ordinal = cursor.next_ordinal.checked_add(1).ok_or(
                    EiaSourceTransportError::PageLimitExceeded {
                        max: cursor.max_pages,
                    },
                )?;
                cursor.next_offset = next_offset;
                Ok(EiaDataPageTransition::More(cursor))
            }
            EiaPageCompleteness::Complete => {
                let tracker = cursor
                    .pagination_tracker
                    .take()
                    .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
                let typed_pages = std::mem::take(&mut cursor.typed_pages);
                let acquisition = EiaAcquisition::try_from_tracked_pages(typed_pages, tracker)?;
                let transport = aggregate_transport_receipt(&cursor.page_materials, &acquisition)?;
                let mut capture_pages = Vec::new();
                capture_pages
                    .try_reserve_exact(cursor.page_materials.len())
                    .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
                capture_pages.extend(
                    cursor
                        .page_materials
                        .iter()
                        .map(|page| page.raw.capture.clone()),
                );
                let capture = ProviderCaptureSetReceipt::try_new(
                    self.metadata.source_id().clone(),
                    self.metadata.revision().clone(),
                    cursor.provider_dataset.clone(),
                    evidence_digest(contract.query().identity()),
                    ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
                    capture_pages,
                )?;
                let ordered_capture =
                    ProviderOrderedCaptureSegments::try_rejoin(capture, cursor.sealed_pages)?;
                Ok(EiaDataPageTransition::Complete(EiaDataRetrieval {
                    dataset: cursor.provider_dataset,
                    acquisition,
                    pages: cursor.page_materials.into_boxed_slice(),
                    ordered_capture,
                    transport,
                }))
            }
        }
    }

    fn validate_data_cursor(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        cursor: &EiaDataAcquisitionCursor,
    ) -> Result<(), EiaSourceTransportError> {
        self.validate_authority(authority)?;
        let expected_dataset = eia_data_dataset_identifier(contract)?;
        let tracker_is_current = match &cursor.pagination_tracker {
            None => cursor.next_ordinal == 0 && cursor.typed_pages.is_empty(),
            Some(tracker) => {
                tracker.admitted_page_count() == u32::from(cursor.next_ordinal)
                    && tracker.next_offset() == Some(cursor.next_offset)
                    && tracker.publication_retained_bytes() == cursor.publication_retained_bytes
            }
        };
        if cursor.source_metadata.as_ref() != &self.metadata
            || &cursor.provider_dataset != &expected_dataset
            || cursor.query_digest != contract.query().identity()
            || cursor.contract_schema_digest != contract.schema_digest()
            || &cursor.api_version != contract.metadata().api_version()
            || cursor.max_pages == 0
            || cursor.max_pages > self.limits.max_pages
            || cursor.max_publication_bytes == 0
            || cursor.max_publication_bytes > MAX_OBSERVED_REVISION_BATCH_BYTES
            || cursor.publication_retained_bytes > cursor.max_publication_bytes
            || cursor.next_ordinal >= cursor.max_pages
            || cursor.typed_pages.len() != cursor.page_materials.len()
            || cursor.page_materials.len() != cursor.sealed_pages.len()
            || cursor.page_materials.len() != usize::from(cursor.next_ordinal)
            || !tracker_is_current
            || (cursor.next_ordinal == 0)
                != (cursor.next_offset == 0 && cursor.page_materials.is_empty())
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        let mut retained_bytes = 0_u64;
        let mut publication_retained_bytes = cursor_base_publication_retained_bytes(
            cursor.source_metadata.as_ref(),
            &cursor.provider_dataset,
            &cursor.api_version,
            cursor.max_pages,
        )?;
        let mut raw_capture_copy_retained_bytes = 0_usize;
        let mut expected_offset = 0_u64;
        for (ordinal, ((typed, material), sealed)) in cursor
            .typed_pages
            .iter()
            .zip(&cursor.page_materials)
            .zip(&cursor.sealed_pages)
            .enumerate()
        {
            let ordinal = u16::try_from(ordinal)
                .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
            let sealed = sealed.persisted_receipt();
            if material.data.offset() != expected_offset
                || typed.receipt() != &material.data
                || typed.retained_payload() != material.raw.payload()
                || material.data.completeness() == EiaPageCompleteness::Complete
                || material.root_journal_rejoin().page_ordinal() != ordinal
            {
                return Err(EiaSourceTransportError::InvalidConfiguration);
            }
            material
                .root_journal_rejoin()
                .validate(&self.metadata, contract, material)?;
            validate_root_page_seal(cursor.query_digest, ordinal, material, sealed)?;
            retained_bytes = retained_bytes
                .checked_add(material.raw.http.retained_bytes)
                .filter(|bytes| *bytes <= self.limits.max_acquisition_bytes)
                .ok_or(EiaSourceTransportError::AcquisitionTooLarge {
                    max: self.limits.max_acquisition_bytes,
                })?;
            let page_lineage_retained_bytes =
                page_lineage_publication_retained_bytes(material, sealed)?;
            let page_dataset = root_page_journal_dataset(
                cursor.query_digest,
                ordinal,
                expected_offset,
                material.raw.capture.body_digest(),
            )?;
            let page_raw_capture_copy_retained_bytes = raw_capture_copy_working_set_bytes(
                &self.metadata,
                &page_dataset,
                [material.raw_page()],
            )?;
            publication_retained_bytes = publication_retained_bytes
                .checked_add(typed.receipt().publication_retained_bytes())
                .and_then(|bytes| bytes.checked_add(page_lineage_retained_bytes))
                .and_then(|bytes| bytes.checked_add(page_raw_capture_copy_retained_bytes))
                .filter(|bytes| *bytes <= cursor.max_publication_bytes)
                .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
            raw_capture_copy_retained_bytes = raw_capture_copy_retained_bytes
                .checked_add(page_raw_capture_copy_retained_bytes)
                .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
            let EiaPageCompleteness::More { next_offset } = material.data.completeness() else {
                return Err(EiaSourceTransportError::InvalidConfiguration);
            };
            expected_offset = next_offset;
        }
        if retained_bytes != cursor.retained_bytes
            || publication_retained_bytes != cursor.publication_retained_bytes
            || raw_capture_copy_retained_bytes != cursor.raw_capture_copy_retained_bytes
            || expected_offset != cursor.next_offset
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        Ok(())
    }

    /// Probes the exact offset-zero data request without walking an unbounded provider total.
    ///
    /// The returned capture is framed as a standalone diagnostic response. Its parsed data receipt
    /// still retains whether more rows existed, so it cannot be mistaken for publication evidence.
    pub async fn probe_data(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<EiaDataProbeRetrieval, EiaSourceTransportError> {
        self.validate_authority(authority)?;
        let dataset = eia_data_dataset_identifier(contract)?;
        let fetched = self
            .fetch_data_page(
                authority,
                contract,
                0,
                0,
                self.limits.max_pages,
                MAX_OBSERVED_REVISION_BATCH_BYTES,
                deadline,
                cancellation,
            )
            .await?;
        let EiaRawPageMaterial { payload, http, .. } = fetched.raw;
        let standalone_page = ProviderCapturePageReceipt::try_new(
            0,
            evidence_digest(http.request_digest),
            None,
            None,
            http.status,
            http.retained_bytes,
            evidence_digest(http.retained_payload_digest),
            http.received_at,
        )?;
        let raw = EiaRawPageMaterial {
            payload,
            http,
            capture: standalone_page,
        };
        let probe_dataset = digest_identifier("eia-v2-data-probe", contract.query().identity())?;
        let capture = standalone_capture(&self.metadata, probe_dataset, &raw)?;
        let capture = provider_capture_material(capture, [&raw])?;
        Ok(EiaDataProbeRetrieval {
            dataset,
            page: fetched.page,
            raw,
            capture,
        })
    }

    async fn fetch_data_page(
        &self,
        authority: &ExtractionAuthority,
        contract: &EiaDatasetContract,
        offset: u64,
        ordinal: u16,
        max_pages: u16,
        remaining_publication_bytes: usize,
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
                    let page = EiaDataPage::parse_with_publication_budget(
                        bytes,
                        request,
                        contract,
                        received_at,
                        self.limits.parse,
                        remaining_publication_bytes,
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
        validate_provider_page_total(
            parsed.value.receipt().total(),
            contract.query().length(),
            max_pages,
        )?;
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
        // A completed bounded response is capture evidence. Once it is fully retained, let the
        // root-owned sealer receive it even if cancellation races this completion. Invalid,
        // partial, oversized, or late responses cannot reach this point.
        ensure_received_by_deadline(response.received_at, deadline)?;
        authority
            .validate_current()
            .map_err(ExtractionSourceError::from)?;
        in_flight.record_success()?;
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

    pub(crate) fn validate_authority(
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

#[derive(Default)]
struct EncodedLength {
    bytes: usize,
}

impl io::Write for EncodedLength {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("encoded EIA evidence length overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encoded_length(value: &impl serde::Serialize) -> Result<usize, EiaSourceTransportError> {
    let mut counter = EncodedLength::default();
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
    Ok(counter.bytes)
}

fn cursor_base_publication_retained_bytes(
    source_metadata: &SourceMetadata,
    provider_dataset: &SourceIdentifier,
    api_version: &crate::EiaApiVersion,
    max_pages: u16,
) -> Result<usize, EiaSourceTransportError> {
    let page_capacity = usize::from(max_pages);
    let source_metadata_encoded_bytes = encoded_length(source_metadata)?;
    let vector_slots = size_of::<EiaDataPage>()
        .checked_add(size_of::<EiaDataPageMaterial>())
        .and_then(|bytes| bytes.checked_add(size_of::<SealedProviderCaptureSetReceipt>()))
        .and_then(|bytes| bytes.checked_mul(page_capacity))
        .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
    size_of::<EiaDataAcquisitionCursor>()
        .checked_add(size_of::<SourceMetadata>())
        .and_then(|bytes| bytes.checked_add(source_metadata_encoded_bytes))
        .and_then(|bytes| bytes.checked_add(provider_dataset.retained_bytes()))
        .and_then(|bytes| bytes.checked_add(api_version.as_str().len()))
        .and_then(|bytes| bytes.checked_add(vector_slots))
        .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
        .ok_or(EiaSourceTransportError::InvalidConfiguration)
}

pub(crate) fn validate_provider_page_total(
    total: u64,
    requested_length: u16,
    max_pages: u16,
) -> Result<(), EiaSourceTransportError> {
    let requested_length = u64::from(requested_length);
    if requested_length == 0 {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    let required_pages = total / requested_length + u64::from(total % requested_length != 0);
    if required_pages > u64::from(max_pages) {
        return Err(EiaSourceTransportError::PageLimitExceeded { max: max_pages });
    }
    Ok(())
}

fn raw_capture_copy_working_set_bytes<'a>(
    metadata: &SourceMetadata,
    dataset: &SourceIdentifier,
    pages: impl IntoIterator<Item = &'a EiaRawPageMaterial>,
) -> Result<usize, EiaSourceTransportError> {
    let mut page_count = 0_usize;
    let mut capture_payload_allocations = 0_usize;
    let mut bytes_owner_allocations = 0_usize;
    for page in pages {
        page_count = page_count
            .checked_add(1)
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        capture_payload_allocations = capture_payload_allocations
            .checked_add(
                checked_arc_bytes_allocation_bytes(page.payload.len())
                    .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?,
            )
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        // `Bytes::from_owner(Arc<[u8]>)` shares the already charged raw page allocation. It owns
        // only this fixed control block while `CapturePayload::try_from_live` creates the one
        // duplicate right-sized payload allocation charged above.
        bytes_owner_allocations = bytes_owner_allocations
            .checked_add(
                size_of::<usize>()
                    .checked_add(size_of::<Arc<[u8]>>())
                    .ok_or(EiaSourceTransportError::InvalidConfiguration)?,
            )
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
    }
    if page_count == 0 {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    let record_slots = page_count
        .checked_mul(size_of::<RawCaptureRecord>())
        .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
    let page_receipt_slots = page_count
        .checked_mul(size_of::<ProviderCapturePageReceipt>())
        .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
    let source_allocation = checked_arc_str_allocation_bytes(metadata.source_id().as_str().len())
        .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
    size_of::<ProviderCaptureMaterial>()
        .checked_add(size_of::<Vec<RawCaptureRecord>>())
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<ProviderCapturePageReceipt>>()))
        .and_then(|bytes| bytes.checked_add(record_slots))
        .and_then(|bytes| bytes.checked_add(page_receipt_slots))
        .and_then(|bytes| bytes.checked_add(capture_payload_allocations))
        .and_then(|bytes| bytes.checked_add(bytes_owner_allocations))
        .and_then(|bytes| bytes.checked_add(source_allocation))
        .and_then(|bytes| bytes.checked_add(metadata.source_id().retained_bytes()))
        .and_then(|bytes| {
            bytes.checked_add(metadata.revision().as_source_identifier().retained_bytes())
        })
        .and_then(|bytes| bytes.checked_add(dataset.retained_bytes()))
        .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
        .ok_or(EiaSourceTransportError::InvalidConfiguration)
}

fn page_lineage_publication_retained_bytes(
    material: &EiaDataPageMaterial,
    sealed: &SealedProviderCaptureSetReceipt,
) -> Result<usize, EiaSourceTransportError> {
    let sealed_encoded_bytes = encoded_length(sealed)?;
    material
        .raw
        .http
        .secret_free_url
        .len()
        .checked_add(material.root_journal_rejoin.provider_dataset.retained_bytes())
        .and_then(|bytes| {
            bytes.checked_add(material.root_journal_rejoin.api_version.as_str().len())
        })
        // JSON is used only as an allocation-free counter. Its actual field values conservatively
        // dominate every variable-length string/array retained by the immutable shared seal.
        .and_then(|bytes| bytes.checked_add(sealed_encoded_bytes))
        .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
        .ok_or(EiaSourceTransportError::InvalidConfiguration)
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

fn root_page_journal_rejoin(
    source_metadata: &Arc<SourceMetadata>,
    provider_dataset: &SourceIdentifier,
    contract: &EiaDatasetContract,
    raw: &EiaRawPageMaterial,
    data: &EiaDataPageReceipt,
) -> Result<EiaRootPageJournalRejoin, EiaSourceTransportError> {
    let next_offset = match data.completeness() {
        EiaPageCompleteness::More { next_offset } => Some(next_offset),
        EiaPageCompleteness::Complete => None,
    };
    let rejoin = EiaRootPageJournalRejoin {
        source_metadata: Arc::clone(source_metadata),
        provider_dataset: provider_dataset.clone(),
        query_digest: contract.query().identity(),
        contract_schema_digest: contract.schema_digest(),
        api_version: contract.metadata().api_version().clone(),
        page_ordinal: raw.capture.ordinal(),
        offset: data.offset(),
        next_offset,
        provider_total: data.total(),
        capture_receipt: raw.capture.clone(),
    };
    validate_page_journal_rejoin(
        source_metadata.as_ref(),
        provider_dataset,
        contract,
        rejoin.page_ordinal,
        raw,
        data,
        &rejoin,
    )?;
    Ok(rejoin)
}

#[allow(
    clippy::too_many_arguments,
    reason = "root rejoin validation keeps every independent authority coordinate explicit"
)]
fn validate_page_journal_rejoin(
    source_metadata: &SourceMetadata,
    provider_dataset: &SourceIdentifier,
    contract: &EiaDatasetContract,
    page_ordinal: u16,
    raw: &EiaRawPageMaterial,
    data: &EiaDataPageReceipt,
    rejoin: &EiaRootPageJournalRejoin,
) -> Result<(), EiaSourceTransportError> {
    let expected_dataset = eia_data_dataset_identifier(contract)?;
    let expected_request_token = (data.offset() != 0).then(|| offset_token_digest(data.offset()));
    let (expected_next_offset, expected_next_token) = match data.completeness() {
        EiaPageCompleteness::More { next_offset } => {
            (Some(next_offset), Some(offset_token_digest(next_offset)))
        }
        EiaPageCompleteness::Complete => (None, None),
    };
    if &expected_dataset != provider_dataset
        || rejoin.source_metadata.as_ref() != source_metadata
        || &rejoin.provider_dataset != provider_dataset
        || rejoin.query_digest != contract.query().identity()
        || rejoin.contract_schema_digest != contract.schema_digest()
        || &rejoin.api_version != contract.metadata().api_version()
        || !source_metadata
            .authorization()
            .is_effective_at(data.received_at())
        || rejoin.page_ordinal != page_ordinal
        || rejoin.offset != data.offset()
        || rejoin.next_offset != expected_next_offset
        || rejoin.provider_total != data.total()
        || &rejoin.capture_receipt != &raw.capture
        || (page_ordinal == 0) != (data.offset() == 0)
        || data.query_digest() != contract.query().identity()
        || data.contract_schema_digest() != contract.schema_digest()
        || raw.http.request_digest() != data.request_digest()
        || raw.http.retained_payload_digest() != data.retained_payload_digest()
        || raw.http.received_at() != data.received_at()
        || raw.http.status() != 200
        || raw.capture.request_identity() != evidence_digest(data.request_digest())
        || raw.capture.request_page_token_digest() != expected_request_token
        || raw.capture.response_next_page_token_digest() != expected_next_token
        || raw.capture.body_digest() != evidence_digest(data.retained_payload_digest())
        || raw.capture.body_bytes() != raw.http.retained_bytes()
        || raw.capture.received_at() != data.received_at()
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    Ok(())
}

fn root_page_journal_dataset(
    query_digest: EiaDigest,
    ordinal: u16,
    offset: u64,
    body_digest: EvidenceDigest,
) -> Result<SourceIdentifier, EiaSourceTransportError> {
    let query_bytes = query_digest.bytes();
    let ordinal_bytes = ordinal.to_be_bytes();
    let offset_bytes = offset.to_be_bytes();
    let body_bytes = body_digest.bytes();
    let identity = digest_parts(
        b"market-squawk/eia-root-page-journal-capture/v1",
        [
            query_bytes.as_slice(),
            ordinal_bytes.as_slice(),
            offset_bytes.as_slice(),
            body_bytes.as_slice(),
        ],
    );
    digest_identifier("eia-v2-root-page", identity)
}

fn validate_root_page_seal(
    query_digest: EiaDigest,
    ordinal: u16,
    material: &EiaDataPageMaterial,
    sealed: &SealedProviderCaptureSetReceipt,
) -> Result<(), EiaSourceTransportError> {
    if material.root_journal_rejoin.query_digest != query_digest
        || material.root_journal_rejoin.page_ordinal != ordinal
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    validate_root_page_rejoin_seal(&material.root_journal_rejoin, sealed)
}

pub(crate) fn validate_root_page_rejoin_seal(
    rejoin: &EiaRootPageJournalRejoin,
    sealed: &SealedProviderCaptureSetReceipt,
) -> Result<(), EiaSourceTransportError> {
    if rejoin.capture_receipt.ordinal() != rejoin.page_ordinal
        || rejoin.capture_receipt.body_digest().bytes() == [0; 32]
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    let chain_page = &rejoin.capture_receipt;
    let standalone_page = ProviderCapturePageReceipt::try_new(
        0,
        chain_page.request_identity(),
        None,
        None,
        chain_page.http_status(),
        chain_page.body_bytes(),
        chain_page.body_digest(),
        chain_page.received_at(),
    )?;
    let dataset = root_page_journal_dataset(
        rejoin.query_digest,
        rejoin.page_ordinal,
        rejoin.offset,
        chain_page.body_digest(),
    )?;
    let source = &rejoin.source_metadata;
    let expected = ProviderCaptureSetReceipt::try_new(
        source.source_id().clone(),
        source.revision().clone(),
        dataset,
        chain_page.request_identity(),
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![standalone_page],
    )?;
    if sealed.capture() != &expected
        || sealed.receipt_digest().bytes() == [0; 32]
        || sealed.segment().physical_receipt_digest().bytes() == [0; 32]
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    Ok(())
}

pub(crate) fn validate_terminal_data_rejoin(
    current_source: &SourceMetadata,
    contract: &EiaDatasetContract,
    retrieval: &EiaDataRetrievalSealRejoin,
) -> Result<(), EiaSourceTransportError> {
    let expected_dataset = eia_data_dataset_identifier(contract)?;
    let acquisition_receipt = retrieval.acquisition.receipt();
    let full_capture = retrieval.ordered_capture.root_capture();
    if current_source
        != retrieval
            .pages
            .first()
            .map(|page| page.root_journal_rejoin.source_metadata())
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?
        || &retrieval.dataset != &expected_dataset
        || full_capture.source_id() != current_source.source_id()
        || full_capture.metadata_revision() != current_source.revision()
        || full_capture.dataset() != &expected_dataset
        || full_capture.request_set_identity().bytes() != contract.query().identity().bytes()
        || full_capture.terminal() != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        || acquisition_receipt.query_digest() != contract.query().identity()
        || acquisition_receipt.contract_schema_digest() != contract.schema_digest()
        || acquisition_receipt.api_version() != contract.metadata().api_version()
        || retrieval.pages.len() != retrieval.ordered_capture.segment_count()
        || retrieval.pages.len() != full_capture.pages().len()
        || retrieval.pages.len()
            != usize::try_from(acquisition_receipt.page_count())
                .map_err(|_| EiaSourceTransportError::InvalidConfiguration)?
        || retrieval.transport
            != aggregate_transport_receipt(&retrieval.pages, &retrieval.acquisition)?
        || retrieval.ordered_capture.receipt_digest().bytes() == [0; 32]
    {
        return Err(EiaSourceTransportError::InvalidConfiguration);
    }
    let mut previous_received_at = None;
    for (ordinal, (material, full_page)) in
        retrieval.pages.iter().zip(full_capture.pages()).enumerate()
    {
        let ordinal =
            u16::try_from(ordinal).map_err(|_| EiaSourceTransportError::InvalidConfiguration)?;
        let sealed_page = retrieval
            .ordered_capture
            .persisted_segment_receipt(usize::from(ordinal))
            .ok_or(EiaSourceTransportError::InvalidConfiguration)?;
        material
            .root_journal_rejoin()
            .validate(current_source, contract, material)?;
        validate_root_page_seal(contract.query().identity(), ordinal, material, sealed_page)?;
        if material.raw.capture_receipt() != full_page
            || material.root_journal_rejoin.capture_receipt() != full_page
            || acquisition_receipt
                .page_digests()
                .get(usize::from(ordinal))
                .is_none_or(|digest| digest.bytes() != full_page.body_digest().bytes())
            || previous_received_at.is_some_and(|received_at| received_at > full_page.received_at())
            || (ordinal == 0 && full_page.received_at() != acquisition_receipt.first_received_at())
            || (usize::from(ordinal) + 1 == retrieval.pages.len()
                && full_page.received_at() != acquisition_receipt.last_received_at())
        {
            return Err(EiaSourceTransportError::InvalidConfiguration);
        }
        previous_received_at = Some(full_page.received_at());
    }
    Ok(())
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
            Bytes::from_owner(Arc::clone(&page.payload)),
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
        || metadata.source_class() != SourceClass::OfficialAgency
        || metadata.provider().as_str() != "us-eia"
        || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        || metadata.coverage().domain() != CoverageDomain::Macroeconomic
        || metadata.quality_ceiling() != market_squawk_domain::DataQuality::OfficialDelayed
        || !metadata.capabilities().extraction()
        || metadata.capabilities().live()
        || metadata.capabilities().historical() != HistoricalCapability::RevisionPreserving
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

fn ensure_received_by_deadline(
    received_at: Timestamp,
    deadline: Timestamp,
) -> Result<(), EiaSourceTransportError> {
    if received_at >= deadline {
        Err(ExtractionSourceError::DeadlineExceeded.into())
    } else {
        Ok(())
    }
}

pub(crate) fn system_timestamp() -> Result<Timestamp, EiaSourceTransportError> {
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
            if cancellation.is_cancelled() {
                return Err(ExtractionSourceError::Cancelled.into());
            }
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
                result = tokio::time::timeout(timeout, operation) => {
                    result.map_err(|_| EiaSourceTransportError::Extraction(
                        ExtractionSourceError::DeadlineExceeded
                    ))?
                }
                () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled.into()),
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
        pub(crate) cancel_after_completion: bool,
    }

    impl EiaHttpResponseFixture {
        pub(crate) const fn cancel_after_completion(mut self) -> Self {
            self.cancel_after_completion = true;
            self
        }
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
                if fixture.cancel_after_completion {
                    cancellation.cancel();
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
