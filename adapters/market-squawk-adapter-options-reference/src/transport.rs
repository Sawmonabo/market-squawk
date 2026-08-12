//! Exact official-source request planning and bounded HTTP response admission.

use std::fmt::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier,
    Timestamp,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    CBOE_ALL_SERIES_MAX_BYTES, CboeAllSeriesCsvSchema, CboeVenue, OCC_DLP_MAX_BYTES,
    OCC_MEMO_MAX_BYTES, ObjectClockEvidence, OccMemoCsvSchema, PublicationRequest,
    ReferenceObjectContext, ReferenceSurface,
};

const OCC_DLP_SELECTED_LOCATOR: &str = "https://marketdata.theocc.com/delo-download?prodType=ALL&downloadFields=OS;US;SN;EXCH;PL;ONN&format=txt";
const OCC_DLP_DAILY_BASE: &str = "https://marketdata.theocc.com/daily-delo-download";
const OCC_MEMO_EXPORT_LOCATOR: &str = "https://infomemo.theocc.com/infomemo/exportmemo";
const OCC_MEMO_DOCUMENT_BASE: &str = "https://infomemo.theocc.com/infomemos";

/// Application maximum for a retained complete OCC memo document.
///
/// This is a local raw-object safety bound, not a provider-published response ceiling.
pub const OCC_MEMO_DOCUMENT_MAX_BYTES: usize = 32 * 1024 * 1024;

const MAX_REDIRECTS: usize = 4;
const MAX_HEADER_VALUE_BYTES: usize = 1_024;
const MAX_OPERATION_DURATION: Duration = Duration::from_secs(10 * 60);

/// A closed set of reviewed Cboe `All Series` schemas eligible for one acquisition cycle.
///
/// Runtime selection succeeds only when the exact first CSV line matches one member. This is an
/// application freeze, not a claim that Cboe guarantees every admitted revision indefinitely.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeSchemaFreeze {
    admitted: Vec<CboeAllSeriesCsvSchema>,
}

impl CboeSchemaFreeze {
    /// Freezes acquisition to one exact header revision.
    pub fn single(schema: CboeAllSeriesCsvSchema) -> Self {
        Self {
            admitted: vec![schema],
        }
    }

    /// Freezes acquisition to an explicit, duplicate-free set of reviewed revisions.
    ///
    /// # Errors
    ///
    /// Rejects an empty set, duplicate values, or more variants than this crate defines.
    pub fn try_new(
        mut admitted: Vec<CboeAllSeriesCsvSchema>,
    ) -> Result<Self, ReferenceTransportError> {
        if admitted.is_empty() || admitted.len() > 3 {
            return Err(ReferenceTransportError::InvalidSchemaFreeze);
        }
        admitted.sort();
        if admitted.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ReferenceTransportError::InvalidSchemaFreeze);
        }
        Ok(Self { admitted })
    }

    /// Returns exact reviewed schema revisions in deterministic order.
    pub fn admitted(&self) -> &[CboeAllSeriesCsvSchema] {
        &self.admitted
    }

    fn select(&self, body: &[u8]) -> Result<CboeAllSeriesCsvSchema, ReferenceTransportError> {
        let line = first_line(body)?;
        let mut selected = self
            .admitted
            .iter()
            .copied()
            .filter(|schema| schema.matches_header_line(line));
        let schema = selected
            .next()
            .ok_or(ReferenceTransportError::UnrecognizedSchema)?;
        if selected.next().is_some() {
            return Err(ReferenceTransportError::AmbiguousSchema);
        }
        Ok(schema)
    }
}

/// Exact code-owned decisions needed to build one official publication cycle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialPublicationPolicy {
    cboe_schema_freeze: CboeSchemaFreeze,
    occ_memo_csv_schema: OccMemoCsvSchema,
    occ_dlp_daily_date: Option<CalendarDate>,
}

impl OfficialPublicationPolicy {
    /// Creates the schema/date policy for one publication request.
    pub const fn new(
        cboe_schema_freeze: CboeSchemaFreeze,
        occ_memo_csv_schema: OccMemoCsvSchema,
        occ_dlp_daily_date: Option<CalendarDate>,
    ) -> Self {
        Self {
            cboe_schema_freeze,
            occ_memo_csv_schema,
            occ_dlp_daily_date,
        }
    }

    /// Returns the exact Cboe schema freeze.
    pub const fn cboe_schema_freeze(&self) -> &CboeSchemaFreeze {
        &self.cboe_schema_freeze
    }

    /// Returns the exact OCC memo CSV decoder revision.
    pub const fn occ_memo_csv_schema(&self) -> OccMemoCsvSchema {
        self.occ_memo_csv_schema
    }

    /// Returns the provider report date used by the dated OCC DLP request.
    pub const fn occ_dlp_daily_date(&self) -> Option<CalendarDate> {
        self.occ_dlp_daily_date
    }
}

/// HTTP method admitted by the selected official reference surfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceHttpMethod {
    /// An idempotent HTTP GET.
    Get,
}

/// A bounded ASCII HTTP field value retained exactly as received.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ReferenceHeaderValue(String);

impl ReferenceHeaderValue {
    /// Validates one response/request field value without accepting control characters.
    ///
    /// # Errors
    ///
    /// Rejects empty, non-ASCII, control-bearing, or oversized values.
    pub fn try_new(value: impl Into<String>) -> Result<Self, ReferenceTransportError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_HEADER_VALUE_BYTES
            || !value.is_ascii()
            || value.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(ReferenceTransportError::InvalidHeaderValue);
        }
        Ok(Self(value))
    }

    /// Returns the exact retained field value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Runtime-observed standard HTTP cache fields.
///
/// Presence is evidence from one response, not a provider guarantee that future conditional
/// requests will be honored.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCacheEvidence {
    etag: Option<ReferenceHeaderValue>,
    last_modified: Option<ReferenceHeaderValue>,
}

impl HttpCacheEvidence {
    /// Constructs captured response validators.
    pub const fn new(
        etag: Option<ReferenceHeaderValue>,
        last_modified: Option<ReferenceHeaderValue>,
    ) -> Self {
        Self {
            etag,
            last_modified,
        }
    }

    /// Returns the exact observed ETag field.
    pub const fn etag(&self) -> Option<&ReferenceHeaderValue> {
        self.etag.as_ref()
    }

    /// Returns the exact observed Last-Modified field.
    pub const fn last_modified(&self) -> Option<&ReferenceHeaderValue> {
        self.last_modified.as_ref()
    }

    fn preferred_validator(&self) -> Option<CacheValidator> {
        self.etag
            .clone()
            .filter(|value| usable_etag(value.as_str()))
            .map(CacheValidator::Etag)
            .or_else(|| {
                self.last_modified
                    .clone()
                    .filter(|value| looks_like_imf_fixdate(value.as_str()))
                    .map(CacheValidator::LastModified)
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
enum CacheValidator {
    Etag(ReferenceHeaderValue),
    LastModified(ReferenceHeaderValue),
}

/// Prior exact-object evidence authorizing one conditional request and an exact 304 reuse.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConditionalCacheRequest {
    validator: CacheValidator,
    prior_payload_digest: EvidenceDigest,
    prior_payload_bytes: u64,
    prior_object_id: SourceIdentifier,
}

impl ConditionalCacheRequest {
    /// Selects the strongest observed validator and binds it to the prior exact object.
    ///
    /// # Errors
    ///
    /// Rejects absent validators or an empty prior payload.
    pub fn try_new(
        cache: &HttpCacheEvidence,
        prior_payload_digest: EvidenceDigest,
        prior_payload_bytes: u64,
        prior_object_id: SourceIdentifier,
    ) -> Result<Self, ReferenceTransportError> {
        if prior_payload_bytes == 0 {
            return Err(ReferenceTransportError::InvalidConditionalEvidence);
        }
        Ok(Self {
            validator: cache
                .preferred_validator()
                .ok_or(ReferenceTransportError::InvalidConditionalEvidence)?,
            prior_payload_digest,
            prior_payload_bytes,
            prior_object_id,
        })
    }

    /// Returns the prior exact payload digest reused by a valid 304 response.
    pub const fn prior_payload_digest(&self) -> EvidenceDigest {
        self.prior_payload_digest
    }

    /// Returns the prior exact payload byte count.
    pub const fn prior_payload_bytes(&self) -> u64 {
        self.prior_payload_bytes
    }

    /// Returns the prior exact object identity.
    pub const fn prior_object_id(&self) -> &SourceIdentifier {
        &self.prior_object_id
    }
}

/// One exact official source request in a publication plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialReferenceRequest {
    request_id: SourceIdentifier,
    surface: ReferenceSurface,
    locator: SourceIdentifier,
    accept: ReferenceHeaderValue,
    maximum_decoded_bytes: usize,
    wall_started_at: Timestamp,
    wall_deadline: Timestamp,
    decoder_policy: DecoderPolicy,
    conditional: Option<ConditionalCacheRequest>,
}

impl OfficialReferenceRequest {
    /// Returns the parent publication request identity.
    pub const fn request_id(&self) -> &SourceIdentifier {
        &self.request_id
    }

    /// Returns the exact requested provider surface.
    pub const fn surface(&self) -> &ReferenceSurface {
        &self.surface
    }

    /// Returns the exact code-owned official locator.
    pub const fn locator(&self) -> &SourceIdentifier {
        &self.locator
    }

    /// Returns the exact `Accept` field value.
    pub const fn accept(&self) -> &ReferenceHeaderValue {
        &self.accept
    }

    /// Returns the per-object decoded body ceiling supplied to the executor.
    pub const fn maximum_decoded_bytes(&self) -> usize {
        self.maximum_decoded_bytes
    }

    /// Returns when the parent publication request was admitted.
    pub const fn wall_started_at(&self) -> Timestamp {
        self.wall_started_at
    }

    /// Returns the publication's wall-clock deadline.
    pub const fn wall_deadline(&self) -> Timestamp {
        self.wall_deadline
    }

    /// Adds prior exact-object evidence for a conditional request.
    ///
    /// # Errors
    ///
    /// Rejects a prior object larger than this exact request's byte ceiling.
    pub fn try_with_conditional_cache(
        mut self,
        conditional: ConditionalCacheRequest,
    ) -> Result<Self, ReferenceTransportError> {
        if usize::try_from(conditional.prior_payload_bytes)
            .map_or(true, |bytes| bytes > self.maximum_decoded_bytes)
        {
            return Err(ReferenceTransportError::InvalidConditionalEvidence);
        }
        self.conditional = Some(conditional);
        Ok(self)
    }

    /// Returns conditional reuse evidence when supplied.
    pub const fn conditional_cache(&self) -> Option<&ConditionalCacheRequest> {
        self.conditional.as_ref()
    }

    fn http_request(&self) -> ReferenceHttpRequest {
        let (if_none_match, if_modified_since) = match self
            .conditional
            .as_ref()
            .map(|conditional| &conditional.validator)
        {
            Some(CacheValidator::Etag(value)) => (Some(value.clone()), None),
            Some(CacheValidator::LastModified(value)) => (None, Some(value.clone())),
            None => (None, None),
        };
        ReferenceHttpRequest {
            method: ReferenceHttpMethod::Get,
            locator: self.locator.clone(),
            accept: self.accept.clone(),
            accept_encoding_identity: true,
            if_none_match,
            if_modified_since,
            maximum_decoded_bytes: self.maximum_decoded_bytes,
            maximum_redirects: MAX_REDIRECTS,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum DecoderPolicy {
    CboeAllSeries { freeze: CboeSchemaFreeze },
    OccDlpText,
    OccMemoCsv { schema: OccMemoCsvSchema },
    OccMemoDocumentUninterpreted,
}

/// Exact duplicate-free HTTP requests for one admitted publication request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialPublicationPlan {
    request: PublicationRequest,
    requests: Vec<OfficialReferenceRequest>,
}

impl OfficialPublicationPlan {
    /// Builds each exact official request once in the publication's sorted surface order.
    ///
    /// Supported acquisition surfaces are the four Cboe `All Series` files, OCC selected/daily
    /// DLP text, the OCC memo CSV export, and a complete memo document by memo number. The closed
    /// JSON placeholder and attachment surface are rejected because no exact official request is
    /// established for them here.
    ///
    /// # Errors
    ///
    /// Rejects unsupported surfaces, a missing DLP report date, invalid identifiers, or a total
    /// byte ceiling that cannot admit even one byte per requested object.
    pub fn try_new(
        request: PublicationRequest,
        policy: OfficialPublicationPolicy,
    ) -> Result<Self, ReferenceTransportError> {
        let mut requests = Vec::new();
        requests
            .try_reserve_exact(request.surfaces().len())
            .map_err(|_| ReferenceTransportError::AllocationFailed)?;
        let aggregate_limit =
            usize::try_from(request.limits().max_total_bytes()).unwrap_or(usize::MAX);
        if aggregate_limit < request.surfaces().len() {
            return Err(ReferenceTransportError::InvalidPublicationPlan);
        }
        let per_object_aggregate_share = aggregate_limit / request.surfaces().len();
        for surface in request.surfaces() {
            let (locator, accept, safety_maximum, decoder_policy) = match surface {
                ReferenceSurface::CboeAllSeries { venue } => (
                    venue.all_series_locator().to_owned(),
                    "text/csv,application/octet-stream;q=0.5",
                    CBOE_ALL_SERIES_MAX_BYTES,
                    DecoderPolicy::CboeAllSeries {
                        freeze: policy.cboe_schema_freeze.clone(),
                    },
                ),
                ReferenceSurface::OccDlpSelectedText => (
                    OCC_DLP_SELECTED_LOCATOR.to_owned(),
                    "text/plain,application/octet-stream;q=0.5",
                    OCC_DLP_MAX_BYTES,
                    DecoderPolicy::OccDlpText,
                ),
                ReferenceSurface::OccDlpDailyText => {
                    let date = policy
                        .occ_dlp_daily_date
                        .ok_or(ReferenceTransportError::MissingPublicationDate)?;
                    (
                        format!(
                            "{OCC_DLP_DAILY_BASE}?reportDate={:04}{:02}{:02}&format=txt",
                            date.year(),
                            date.month(),
                            date.day()
                        ),
                        "text/plain,application/octet-stream;q=0.5",
                        OCC_DLP_MAX_BYTES,
                        DecoderPolicy::OccDlpText,
                    )
                }
                ReferenceSurface::OccMemoIndexCsv => (
                    OCC_MEMO_EXPORT_LOCATOR.to_owned(),
                    "text/csv,application/octet-stream;q=0.5",
                    OCC_MEMO_MAX_BYTES,
                    DecoderPolicy::OccMemoCsv {
                        schema: policy.occ_memo_csv_schema,
                    },
                ),
                ReferenceSurface::OccMemoDocument { memo_number } => (
                    format!("{OCC_MEMO_DOCUMENT_BASE}?number={memo_number}"),
                    "text/html,application/pdf;q=0.9",
                    OCC_MEMO_DOCUMENT_MAX_BYTES,
                    DecoderPolicy::OccMemoDocumentUninterpreted,
                ),
                ReferenceSurface::OccMemoIndexJson | ReferenceSurface::OccMemoAttachment { .. } => {
                    return Err(ReferenceTransportError::UnsupportedOfficialSurface);
                }
            };
            let maximum_decoded_bytes = safety_maximum.min(per_object_aggregate_share);
            if maximum_decoded_bytes == 0 {
                return Err(ReferenceTransportError::InvalidPublicationPlan);
            }
            requests.push(OfficialReferenceRequest {
                request_id: request.request_id().clone(),
                surface: surface.clone(),
                locator: SourceIdentifier::try_from(locator)
                    .map_err(|_| ReferenceTransportError::InvalidOfficialLocator)?,
                accept: ReferenceHeaderValue::try_new(accept)?,
                maximum_decoded_bytes,
                wall_started_at: request.requested_at(),
                wall_deadline: request.deadline(),
                decoder_policy,
                conditional: None,
            });
        }
        Ok(Self { request, requests })
    }

    /// Returns the exact publication request answered by this plan.
    pub const fn publication_request(&self) -> &PublicationRequest {
        &self.request
    }

    /// Returns the exact ordered official request closure.
    pub fn requests(&self) -> &[OfficialReferenceRequest] {
        &self.requests
    }

    /// Consumes the plan into independent official requests.
    pub fn into_requests(self) -> Vec<OfficialReferenceRequest> {
        self.requests
    }
}

/// Concrete HTTP request passed to the injected bounded executor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceHttpRequest {
    method: ReferenceHttpMethod,
    locator: SourceIdentifier,
    accept: ReferenceHeaderValue,
    accept_encoding_identity: bool,
    if_none_match: Option<ReferenceHeaderValue>,
    if_modified_since: Option<ReferenceHeaderValue>,
    maximum_decoded_bytes: usize,
    maximum_redirects: usize,
}

impl ReferenceHttpRequest {
    /// Returns the HTTP method.
    pub const fn method(&self) -> ReferenceHttpMethod {
        self.method
    }

    /// Returns the exact provider URL.
    pub const fn locator(&self) -> &SourceIdentifier {
        &self.locator
    }

    /// Returns the exact Accept field.
    pub const fn accept(&self) -> &ReferenceHeaderValue {
        &self.accept
    }

    /// Returns whether compressed transfer was declined so content length and retained bytes share
    /// one representation.
    pub const fn accept_encoding_identity(&self) -> bool {
        self.accept_encoding_identity
    }

    /// Returns the conditional ETag field when a prior exact object supplied one.
    pub const fn if_none_match(&self) -> Option<&ReferenceHeaderValue> {
        self.if_none_match.as_ref()
    }

    /// Returns the conditional modification date when no ETag was observed.
    pub const fn if_modified_since(&self) -> Option<&ReferenceHeaderValue> {
        self.if_modified_since.as_ref()
    }

    /// Returns the maximum decoded bytes the executor may retain.
    pub const fn maximum_decoded_bytes(&self) -> usize {
        self.maximum_decoded_bytes
    }

    /// Returns the application redirect-hop ceiling.
    pub const fn maximum_redirects(&self) -> usize {
        self.maximum_redirects
    }
}

/// A clonable cancellation signal shared with one executor operation.
#[derive(Clone, Debug, Default)]
pub struct ReferenceCancellation(Arc<AtomicBool>);

impl ReferenceCancellation {
    /// Creates an open cancellation signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Returns whether cancellation was requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Monotonic deadline and cancellation authority for one HTTP operation.
#[derive(Clone, Debug)]
pub struct ReferenceFetchControl {
    deadline: Instant,
    cancellation: ReferenceCancellation,
}

impl ReferenceFetchControl {
    /// Creates a bounded operation window no longer than ten minutes.
    ///
    /// # Errors
    ///
    /// Rejects zero or excessive duration and monotonic overflow.
    pub fn for_duration(
        duration: Duration,
        cancellation: ReferenceCancellation,
    ) -> Result<Self, ReferenceTransportError> {
        if duration.is_zero() || duration > MAX_OPERATION_DURATION {
            return Err(ReferenceTransportError::InvalidDeadline);
        }
        Ok(Self {
            deadline: Instant::now()
                .checked_add(duration)
                .ok_or(ReferenceTransportError::InvalidDeadline)?,
            cancellation,
        })
    }

    /// Returns the shared cancellation signal.
    pub const fn cancellation(&self) -> &ReferenceCancellation {
        &self.cancellation
    }

    /// Returns the remaining monotonic duration.
    ///
    /// # Errors
    ///
    /// Returns cancellation or deadline expiry without permitting further transport work.
    pub fn remaining(&self) -> Result<Duration, ReferenceTransportError> {
        self.ensure_open()?;
        Ok(self.deadline.saturating_duration_since(Instant::now()))
    }

    fn ensure_open(&self) -> Result<(), ReferenceTransportError> {
        if self.cancellation.is_cancelled() {
            Err(ReferenceTransportError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ReferenceTransportError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

/// Structured result returned by an injected HTTP executor.
///
/// The executor must stop reading before `request.maximum_decoded_bytes`, honor the control's
/// cancellation/deadline, disable transparent content decoding when identity encoding is
/// requested, and supply the exact final locator and redirect chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceHttpResponse {
    status: u16,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    content_type: Option<ReferenceHeaderValue>,
    content_length: Option<u64>,
    content_encoding: Option<ReferenceHeaderValue>,
    cache: HttpCacheEvidence,
    retry_after: Option<ReferenceHeaderValue>,
    received_at: Timestamp,
    body_complete: bool,
    body: Vec<u8>,
}

impl ReferenceHttpResponse {
    /// Constructs one exact response envelope for source admission.
    ///
    /// # Errors
    ///
    /// Rejects excessive redirect evidence. Body and status contracts are validated by the source
    /// after it regains control from the executor.
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact HTTP response evidence boundary is intentionally explicit"
    )]
    pub fn try_new(
        status: u16,
        final_locator: SourceIdentifier,
        redirect_chain: Vec<SourceIdentifier>,
        content_type: Option<ReferenceHeaderValue>,
        content_length: Option<u64>,
        content_encoding: Option<ReferenceHeaderValue>,
        cache: HttpCacheEvidence,
        retry_after: Option<ReferenceHeaderValue>,
        received_at: Timestamp,
        body_complete: bool,
        body: Vec<u8>,
    ) -> Result<Self, ReferenceTransportError> {
        if redirect_chain.len() > MAX_REDIRECTS {
            return Err(ReferenceTransportError::RedirectLimitExceeded);
        }
        Ok(Self {
            status,
            final_locator,
            redirect_chain,
            content_type,
            content_length,
            content_encoding,
            cache,
            retry_after,
            received_at,
            body_complete,
            body,
        })
    }

    /// Returns the exact status code.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the exact final locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns exact redirect-hop locators in observation order.
    pub fn redirect_chain(&self) -> &[SourceIdentifier] {
        &self.redirect_chain
    }

    /// Returns the exact response content type when supplied.
    pub const fn content_type(&self) -> Option<&ReferenceHeaderValue> {
        self.content_type.as_ref()
    }

    /// Returns the response-declared body length when supplied.
    pub const fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    /// Returns the exact response content encoding when supplied.
    pub const fn content_encoding(&self) -> Option<&ReferenceHeaderValue> {
        self.content_encoding.as_ref()
    }

    /// Returns captured cache fields.
    pub const fn cache(&self) -> &HttpCacheEvidence {
        &self.cache
    }

    /// Returns the exact Retry-After field when supplied.
    pub const fn retry_after(&self) -> Option<&ReferenceHeaderValue> {
        self.retry_after.as_ref()
    }

    /// Returns the trusted local receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns whether the executor observed the response-body terminal condition.
    pub const fn body_complete(&self) -> bool {
        self.body_complete
    }

    /// Returns exact retained body bytes.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

/// Bounded HTTP execution seam supplied by the root source-composition layer.
pub trait ReferenceHttpExecutor {
    /// Executes one exact request while enforcing streaming byte, monotonic deadline, and
    /// cancellation bounds.
    ///
    /// # Errors
    ///
    /// Returns a closed transport failure; provider response statuses belong in the response.
    fn execute(
        &self,
        request: &ReferenceHttpRequest,
        control: &ReferenceFetchControl,
    ) -> Result<ReferenceHttpResponse, ReferenceTransportError>;
}

/// Selected provider-native decoder after exact response/schema admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "schema")]
pub enum SelectedReferenceDecoder {
    /// One exact Cboe `All Series` CSV header revision.
    CboeAllSeries(CboeAllSeriesCsvSchema),
    /// OCC DLP six-field tab-separated text.
    OccDlpTextV1,
    /// One exact OCC memo CSV export header revision.
    OccMemoCsv(OccMemoCsvSchema),
    /// Complete raw memo content retained for later typed interpretation.
    OccMemoDocumentUninterpreted,
}

impl SelectedReferenceDecoder {
    fn native_schema(self) -> &'static str {
        match self {
            Self::CboeAllSeries(schema) => schema.native_schema(),
            Self::OccDlpTextV1 => "occ-dlp-text-os-us-sn-exch-pl-onn-v1",
            Self::OccMemoCsv(schema) => schema.native_schema(),
            Self::OccMemoDocumentUninterpreted => "occ-memo-operative-document-uninterpreted-v1",
        }
    }

    fn canonical_media_type(self, observed: &str) -> &'static str {
        match self {
            Self::CboeAllSeries(_) | Self::OccMemoCsv(_) => "text/csv",
            Self::OccDlpTextV1 => "text/plain",
            Self::OccMemoDocumentUninterpreted
                if base_media_type(observed) == "application/pdf" =>
            {
                "application/pdf"
            }
            Self::OccMemoDocumentUninterpreted => "text/html",
        }
    }
}

/// Exact admitted HTTP evidence for a modified response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceHttpReceipt {
    status: u16,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    observed_content_type: ReferenceHeaderValue,
    declared_content_length: Option<u64>,
    cache: HttpCacheEvidence,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    body_complete: bool,
}

impl ReferenceHttpReceipt {
    /// Returns the HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the exact configured locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the exact final locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns retained redirect evidence.
    pub fn redirect_chain(&self) -> &[SourceIdentifier] {
        &self.redirect_chain
    }

    /// Returns the exact observed Content-Type field.
    pub const fn observed_content_type(&self) -> &ReferenceHeaderValue {
        &self.observed_content_type
    }

    /// Returns the declared identity body length when supplied.
    pub const fn declared_content_length(&self) -> Option<u64> {
        self.declared_content_length
    }

    /// Returns runtime-observed cache fields.
    pub const fn cache(&self) -> &HttpCacheEvidence {
        &self.cache
    }

    /// Returns the trusted local receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the exact payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns exact retained payload bytes.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the executor's terminal-body observation.
    pub const fn body_complete(&self) -> bool {
        self.body_complete
    }
}

/// Complete exact bytes, decoder selection, and provenance for one modified object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievedReferenceObject {
    bytes: Vec<u8>,
    decoder: SelectedReferenceDecoder,
    context: ReferenceObjectContext,
    receipt: ReferenceHttpReceipt,
}

impl RetrievedReferenceObject {
    /// Returns exact retained source bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the exact selected provider-native decoder.
    pub const fn decoder(&self) -> SelectedReferenceDecoder {
        self.decoder
    }

    /// Returns record-level raw-object context.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns complete transport evidence for raw capture and manifest binding.
    pub const fn receipt(&self) -> &ReferenceHttpReceipt {
        &self.receipt
    }
}

/// Retry-After evidence preserved without inventing a provider capacity or retry schedule.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RetryAfterEvidence {
    /// Provider supplied integer delta seconds.
    DeltaSeconds(u64),
    /// Provider supplied an IMF-fixdate-shaped value; conversion to a scheduler instant is owned
    /// by the root clock authority.
    HttpDate(ReferenceHeaderValue),
    /// Provider supplied a nonempty field outside the two admitted standard shapes.
    Unrecognized(ReferenceHeaderValue),
}

/// Valid 304 evidence bound to the exact prior object authorized for reuse.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceNotModifiedReceipt {
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    received_at: Timestamp,
    prior_payload_digest: EvidenceDigest,
    prior_payload_bytes: u64,
    prior_object_id: SourceIdentifier,
    response_cache: HttpCacheEvidence,
}

impl ReferenceNotModifiedReceipt {
    /// Returns the exact prior payload digest authorized for reuse.
    pub const fn prior_payload_digest(&self) -> EvidenceDigest {
        self.prior_payload_digest
    }

    /// Returns exact prior payload bytes.
    pub const fn prior_payload_bytes(&self) -> u64 {
        self.prior_payload_bytes
    }

    /// Returns the exact prior object identity.
    pub const fn prior_object_id(&self) -> &SourceIdentifier {
        &self.prior_object_id
    }

    /// Returns when the 304 was locally observed.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns cache fields repeated or updated by the 304 response.
    pub const fn response_cache(&self) -> &HttpCacheEvidence {
        &self.response_cache
    }

    /// Returns the configured locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        &self.configured_locator
    }

    /// Returns the final locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        &self.final_locator
    }

    /// Returns exact redirect evidence.
    pub fn redirect_chain(&self) -> &[SourceIdentifier] {
        &self.redirect_chain
    }
}

/// Outcome of one exact official object acquisition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReferenceFetchOutcome {
    /// HTTP 200 supplied a complete bounded exact object.
    Modified(RetrievedReferenceObject),
    /// HTTP 304 authorized reuse of one exact prior object.
    NotModified(ReferenceNotModifiedReceipt),
}

/// Official source orchestration over a caller-supplied bounded HTTP executor.
#[derive(Clone, Debug)]
pub struct OfficialReferenceSource<T> {
    executor: T,
}

impl<T> OfficialReferenceSource<T>
where
    T: ReferenceHttpExecutor,
{
    /// Binds the official-source validator to an executor.
    pub const fn new(executor: T) -> Self {
        Self { executor }
    }

    /// Retrieves and validates one exact request without retries or hidden fallback.
    ///
    /// # Errors
    ///
    /// Fails closed on cancellation/deadline, redirect or locator drift, statuses other than 200
    /// or authorized 304, 429, encoding/content-length/content-type drift, byte overflow, or an
    /// unrecognized frozen schema. No partial bytes are returned.
    pub fn fetch(
        &self,
        request: &OfficialReferenceRequest,
        control: &ReferenceFetchControl,
    ) -> Result<ReferenceFetchOutcome, ReferenceTransportError> {
        control.ensure_open()?;
        let http_request = request.http_request();
        let response = self.executor.execute(&http_request, control)?;
        control.ensure_open()?;
        validate_locator_lineage(request, &response)?;
        if response.received_at < request.wall_started_at {
            return Err(ReferenceTransportError::InvalidResponseClock);
        }
        if response.received_at > request.wall_deadline {
            return Err(ReferenceTransportError::DeadlineExceeded);
        }

        match response.status {
            304 => return admit_not_modified(request, response),
            429 => {
                return Err(ReferenceTransportError::Throttled {
                    retry_after: response.retry_after.map(classify_retry_after),
                });
            }
            200 => {}
            status => return Err(ReferenceTransportError::HttpStatus(status)),
        }

        if response.body.is_empty() {
            return Err(ReferenceTransportError::EmptyBody);
        }
        if !response.body_complete {
            return Err(ReferenceTransportError::IncompleteBody);
        }
        if response.body.len() > request.maximum_decoded_bytes {
            return Err(ReferenceTransportError::ResponseTooLarge);
        }
        if response.content_length.is_some_and(|declared| {
            usize::try_from(declared).map_or(true, |value| value > request.maximum_decoded_bytes)
        }) {
            return Err(ReferenceTransportError::ResponseTooLarge);
        }
        if response
            .content_encoding
            .as_ref()
            .is_some_and(|value| !value.as_str().eq_ignore_ascii_case("identity"))
        {
            return Err(ReferenceTransportError::UnexpectedContentEncoding);
        }
        let payload_bytes = u64::try_from(response.body.len())
            .map_err(|_| ReferenceTransportError::ResponseTooLarge)?;
        if response
            .content_length
            .is_some_and(|declared| declared != payload_bytes)
        {
            return Err(ReferenceTransportError::ContentLengthMismatch);
        }
        let observed_content_type = response
            .content_type
            .clone()
            .ok_or(ReferenceTransportError::MissingContentType)?;
        let decoder = select_decoder(
            &request.decoder_policy,
            observed_content_type.as_str(),
            &response.body,
        )?;
        let digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            <[u8; 32]>::from(Sha256::digest(&response.body)),
        );
        let object_id = object_identifier(&request.surface, digest)?;
        let canonical_media_type = decoder.canonical_media_type(observed_content_type.as_str());
        let clocks = ObjectClockEvidence::try_new(
            None,
            None,
            AvailabilityEvidence::local_first_observed(response.received_at),
            response.received_at,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let context = ReferenceObjectContext::try_new(
            request.surface.provider(),
            request.surface.clone(),
            object_id,
            request.locator.clone(),
            response.final_locator.clone(),
            SourceIdentifier::try_from(canonical_media_type)
                .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?,
            digest,
            payload_bytes,
            SourceIdentifier::try_from(decoder.native_schema())
                .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?,
            clocks,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let receipt = ReferenceHttpReceipt {
            status: response.status,
            configured_locator: request.locator.clone(),
            final_locator: response.final_locator,
            redirect_chain: response.redirect_chain,
            observed_content_type,
            declared_content_length: response.content_length,
            cache: response.cache,
            received_at: response.received_at,
            payload_digest: digest,
            payload_bytes,
            body_complete: response.body_complete,
        };
        Ok(ReferenceFetchOutcome::Modified(RetrievedReferenceObject {
            bytes: response.body,
            decoder,
            context,
            receipt,
        }))
    }
}

fn admit_not_modified(
    request: &OfficialReferenceRequest,
    response: ReferenceHttpResponse,
) -> Result<ReferenceFetchOutcome, ReferenceTransportError> {
    if !response.body_complete
        || !response.body.is_empty()
        || response.content_length.is_some_and(|length| length != 0)
        || response.content_encoding.is_some()
    {
        return Err(ReferenceTransportError::InvalidNotModifiedResponse);
    }
    let conditional = request
        .conditional
        .as_ref()
        .ok_or(ReferenceTransportError::UnexpectedNotModified)?;
    Ok(ReferenceFetchOutcome::NotModified(
        ReferenceNotModifiedReceipt {
            configured_locator: request.locator.clone(),
            final_locator: response.final_locator,
            redirect_chain: response.redirect_chain,
            received_at: response.received_at,
            prior_payload_digest: conditional.prior_payload_digest,
            prior_payload_bytes: conditional.prior_payload_bytes,
            prior_object_id: conditional.prior_object_id.clone(),
            response_cache: response.cache,
        },
    ))
}

fn validate_locator_lineage(
    request: &OfficialReferenceRequest,
    response: &ReferenceHttpResponse,
) -> Result<(), ReferenceTransportError> {
    if response.final_locator != request.locator
        || response
            .redirect_chain
            .iter()
            .any(|locator| locator != &request.locator)
    {
        return Err(ReferenceTransportError::InvalidRedirect);
    }
    Ok(())
}

fn select_decoder(
    policy: &DecoderPolicy,
    observed_content_type: &str,
    body: &[u8],
) -> Result<SelectedReferenceDecoder, ReferenceTransportError> {
    let media_type = base_media_type(observed_content_type);
    match policy {
        DecoderPolicy::CboeAllSeries { freeze } => {
            ensure_media_type(
                media_type,
                &["text/csv", "application/csv", "application/octet-stream"],
            )?;
            freeze
                .select(body)
                .map(SelectedReferenceDecoder::CboeAllSeries)
        }
        DecoderPolicy::OccDlpText => {
            ensure_media_type(media_type, &["text/plain", "application/octet-stream"])?;
            Ok(SelectedReferenceDecoder::OccDlpTextV1)
        }
        DecoderPolicy::OccMemoCsv { schema } => {
            ensure_media_type(
                media_type,
                &["text/csv", "application/csv", "application/octet-stream"],
            )?;
            let line = first_line(body)?;
            if !schema.matches_header_line(line) {
                return Err(ReferenceTransportError::UnrecognizedSchema);
            }
            Ok(SelectedReferenceDecoder::OccMemoCsv(*schema))
        }
        DecoderPolicy::OccMemoDocumentUninterpreted => {
            ensure_media_type(media_type, &["text/html", "application/pdf"])?;
            Ok(SelectedReferenceDecoder::OccMemoDocumentUninterpreted)
        }
    }
}

fn ensure_media_type(actual: &str, admitted: &[&str]) -> Result<(), ReferenceTransportError> {
    if admitted
        .iter()
        .any(|expected| actual.eq_ignore_ascii_case(expected))
    {
        Ok(())
    } else {
        Err(ReferenceTransportError::UnexpectedContentType)
    }
}

fn base_media_type(value: &str) -> &str {
    value.split(';').next().unwrap_or(value).trim()
}

fn first_line(body: &[u8]) -> Result<&[u8], ReferenceTransportError> {
    let end = body
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(body.len());
    let line = body
        .get(..end)
        .ok_or(ReferenceTransportError::UnrecognizedSchema)?;
    Ok(line.strip_suffix(b"\r").unwrap_or(line))
}

fn object_identifier(
    surface: &ReferenceSurface,
    digest: EvidenceDigest,
) -> Result<SourceIdentifier, ReferenceTransportError> {
    let label = match surface {
        ReferenceSurface::CboeAllSeries { venue } => match venue {
            CboeVenue::C1 => "cboe-c1-all-series",
            CboeVenue::Bzx => "cboe-bzx-all-series",
            CboeVenue::C2 => "cboe-c2-all-series",
            CboeVenue::Edgx => "cboe-edgx-all-series",
        },
        ReferenceSurface::OccDlpSelectedText => "occ-dlp-selected",
        ReferenceSurface::OccDlpDailyText => "occ-dlp-daily",
        ReferenceSurface::OccMemoIndexCsv => "occ-memo-index-csv",
        ReferenceSurface::OccMemoDocument { .. } => "occ-memo-document",
        ReferenceSurface::OccMemoIndexJson | ReferenceSurface::OccMemoAttachment { .. } => {
            return Err(ReferenceTransportError::UnsupportedOfficialSurface);
        }
    };
    let mut value = String::with_capacity(label.len().saturating_add(72));
    value.push_str(label);
    value.push_str(":sha256:");
    for byte in digest.bytes() {
        write!(&mut value, "{byte:02x}").map_err(|_| ReferenceTransportError::AllocationFailed)?;
    }
    SourceIdentifier::try_from(value).map_err(|_| ReferenceTransportError::InvalidObjectEvidence)
}

fn classify_retry_after(value: ReferenceHeaderValue) -> RetryAfterEvidence {
    if value.as_str().bytes().all(|byte| byte.is_ascii_digit()) {
        if let Ok(seconds) = value.as_str().parse::<u64>() {
            return RetryAfterEvidence::DeltaSeconds(seconds);
        }
    }
    if looks_like_imf_fixdate(value.as_str()) {
        RetryAfterEvidence::HttpDate(value)
    } else {
        RetryAfterEvidence::Unrecognized(value)
    }
}

fn usable_etag(value: &str) -> bool {
    let opaque = value.strip_prefix("W/").unwrap_or(value);
    let Some(opaque) = opaque
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return false;
    };
    !opaque.is_empty()
        && opaque
            .bytes()
            .all(|byte| byte == 0x21 || (0x23..=0x7e).contains(&byte))
}

fn looks_like_imf_fixdate(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 29
        && bytes.get(3) == Some(&b',')
        && bytes.get(4) == Some(&b' ')
        && bytes.get(7) == Some(&b' ')
        && bytes.get(11) == Some(&b' ')
        && bytes.get(16) == Some(&b' ')
        && bytes.get(19) == Some(&b':')
        && bytes.get(22) == Some(&b':')
        && bytes.get(25) == Some(&b' ')
        && bytes.get(26..29) == Some(b"GMT")
        && bytes[0..3].iter().all(u8::is_ascii_alphabetic)
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..11].iter().all(u8::is_ascii_alphabetic)
        && bytes[12..16].iter().all(u8::is_ascii_digit)
        && bytes[17..19].iter().all(u8::is_ascii_digit)
        && bytes[20..22].iter().all(u8::is_ascii_digit)
        && bytes[23..25].iter().all(u8::is_ascii_digit)
}

/// Closed source-planning, execution, and response-admission failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ReferenceTransportError {
    /// A schema freeze was empty, duplicated, or outside the closed contract.
    #[error("invalid Cboe schema freeze")]
    InvalidSchemaFreeze,
    /// A response header did not match any exact frozen schema.
    #[error("unrecognized frozen provider schema")]
    UnrecognizedSchema,
    /// More than one frozen schema matched the same response header.
    #[error("ambiguous frozen provider schema")]
    AmbiguousSchema,
    /// An exact current official request has not been established for the requested surface.
    #[error("unsupported official option-reference surface")]
    UnsupportedOfficialSurface,
    /// A dated OCC DLP surface lacked a report date.
    #[error("dated OCC DLP request requires a report date")]
    MissingPublicationDate,
    /// The publication request could not be represented by safe per-object bounds.
    #[error("invalid official option-reference publication plan")]
    InvalidPublicationPlan,
    /// A code-owned official locator could not be represented safely.
    #[error("invalid official option-reference locator")]
    InvalidOfficialLocator,
    /// An HTTP field value was empty, oversized, non-ASCII, or control-bearing.
    #[error("invalid option-reference HTTP header value")]
    InvalidHeaderValue,
    /// Prior object evidence did not include a usable validator and nonempty exact payload.
    #[error("invalid option-reference conditional-cache evidence")]
    InvalidConditionalEvidence,
    /// A requested operation duration was zero, excessive, or overflowed monotonic time.
    #[error("invalid option-reference HTTP deadline")]
    InvalidDeadline,
    /// Caller cancellation was observed before complete admission.
    #[error("option-reference HTTP operation was cancelled")]
    Cancelled,
    /// The monotonic or publication wall-clock deadline elapsed.
    #[error("option-reference HTTP operation exceeded its deadline")]
    DeadlineExceeded,
    /// Trusted local receipt preceded publication-request admission.
    #[error("option-reference HTTP response clock preceded request admission")]
    InvalidResponseClock,
    /// The injected executor failed before returning an HTTP response.
    #[error("option-reference HTTP executor failed")]
    ExecutorFailed,
    /// Redirect evidence exceeded the application ceiling.
    #[error("option-reference HTTP redirect limit exceeded")]
    RedirectLimitExceeded,
    /// Final or intermediate transport lineage departed from the exact admitted locator.
    #[error("option-reference HTTP redirect or final locator is invalid")]
    InvalidRedirect,
    /// The provider returned a non-success status not otherwise classified.
    #[error("option-reference provider returned HTTP status {0}")]
    HttpStatus(u16),
    /// The provider returned HTTP 429; the optional field remains evidence, not a rate promise.
    #[error("option-reference provider throttled the request")]
    Throttled {
        /// Exact classified Retry-After evidence when supplied.
        retry_after: Option<RetryAfterEvidence>,
    },
    /// HTTP 304 was returned without prior exact-object conditional evidence.
    #[error("unexpected option-reference HTTP 304 response")]
    UnexpectedNotModified,
    /// HTTP 304 carried representation bytes or encoding state incompatible with exact reuse.
    #[error("invalid option-reference HTTP 304 response")]
    InvalidNotModifiedResponse,
    /// A successful response had no bytes.
    #[error("empty option-reference HTTP response")]
    EmptyBody,
    /// The executor did not observe the response body's terminal condition.
    #[error("incomplete option-reference HTTP response body")]
    IncompleteBody,
    /// Response bytes exceeded the request's application ceiling.
    #[error("option-reference HTTP response exceeded its byte bound")]
    ResponseTooLarge,
    /// A non-identity content encoding contradicted the exact retained representation contract.
    #[error("unexpected option-reference HTTP content encoding")]
    UnexpectedContentEncoding,
    /// A declared identity-encoded content length differed from retained bytes.
    #[error("option-reference HTTP content length did not match retained bytes")]
    ContentLengthMismatch,
    /// A successful response omitted Content-Type.
    #[error("option-reference HTTP response omitted content type")]
    MissingContentType,
    /// Content-Type was outside the closed surface contract.
    #[error("unexpected option-reference HTTP content type")]
    UnexpectedContentType,
    /// Exact raw-object context could not be constructed.
    #[error("invalid option-reference raw-object evidence")]
    InvalidObjectEvidence,
    /// A bounded vector or identifier allocation failed.
    #[error("option-reference bounded allocation failed")]
    AllocationFailed,
}

impl ReferenceTransportError {
    /// Builds the closed executor-failure value without retaining unbounded transport text.
    pub const fn executor_failed() -> Self {
        Self::ExecutorFailed
    }
}
