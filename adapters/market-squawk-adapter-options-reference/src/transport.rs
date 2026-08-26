//! Exact official-source request planning and bounded HTTP response admission.

use std::fmt::Write as _;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt as _;
use market_squawk_domain::{
    AssetClass, AvailabilityEvidence, CalendarDate, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectAdmission, ResearchObjectClaim, ResearchObjectControl,
    ResearchObjectControlError, ResearchObjectControlPoint, ResearchObjectReceipt,
    SealedResearchJournalStore, SealedResearchJournalStoreError, VerifiedResearchObject,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationMode, BackoffPolicy, BudgetScope, BudgetWindowSemantics,
    CoverageDomain, EndpointPolicy, ExtractionAuthority, ExtractionAuthorityError,
    HistoricalCapability, HttpClientProfile, HttpRequestBounds, InFlightExtractionRequest,
    NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderRateDeclaration, QueryParameterRule, QuerySensitivity, SourceClass, SourceMetadata,
    SourceProtocolProfile,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[cfg(test)]
use crate::ReferenceRequestMethod;
use crate::{
    CBOE_ALL_SERIES_MAX_BYTES, CboeAllSeriesCsvSchema, CboeVenue, HttpLastModifiedEvidence,
    OCC_DLP_MAX_BYTES, OCC_MEMO_MAX_BYTES, ObjectClockEvidence, OccDlpSchema, OccMemoCsvSchema,
    OptionsReferenceCurrentnessDisposition, PublicationRequest, ReferenceConditionalPriorEvidence,
    ReferenceConditionalValidatorEvidence, ReferenceNativeSchemaIdentity, ReferenceObjectContext,
    ReferenceOfficialRequestEvidence, ReferenceProvider, ReferenceSurface,
    ReferenceTransportEvidence, payload::ExactPayloadReader,
};

const OCC_DLP_SELECTED_LOCATOR: &str = "https://marketdata.theocc.com/delo-download?prodType=ALL&downloadFields=OS;US;SN;EXCH;PL;ONN&format=txt";
const OCC_DLP_SELECTED_BASE: &str = "https://marketdata.theocc.com/delo-download";
const OCC_DLP_DAILY_BASE: &str = "https://marketdata.theocc.com/daily-delo-download";
const OCC_MEMO_EXPORT_LOCATOR: &str = "https://infomemo.theocc.com/infomemo/exportmemo";
const OCC_MEMO_DOCUMENT_BASE: &str = "https://infomemo.theocc.com/infomemos";
/// Stable source identity required for the OCC option-reference profile.
pub const OCC_OPTIONS_REFERENCE_SOURCE_ID: &str = "occ.options-reference";
/// Stable shared-budget provider identity required for OCC option-reference requests.
pub const OCC_OPTIONS_REFERENCE_PROVIDER_ID: &str = "occ-reference";
/// Stable source identity required for the Cboe option-reference profile.
pub const CBOE_OPTIONS_REFERENCE_SOURCE_ID: &str = "cboe.options-reference";
/// Stable shared-budget provider identity required for Cboe option-reference requests.
pub const CBOE_OPTIONS_REFERENCE_PROVIDER_ID: &str = "cboe-reference";
const APPLICATION_SINGLE_FLIGHT: u16 = 1;
const NANOS_PER_MINUTE: u64 = 60_000_000_000;
const OCC_MINIMUM_RESPONSE_BYTES: u64 = OCC_DLP_MAX_BYTES as u64;
const CBOE_MINIMUM_RESPONSE_BYTES: u64 = CBOE_ALL_SERIES_MAX_BYTES as u64;

/// Application maximum for a retained complete OCC memo document.
///
/// This is a local raw-object safety bound, not a provider-published response ceiling.
pub const OCC_MEMO_DOCUMENT_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Conservative Market Squawk request count for each independent OCC and Cboe public queue.
///
/// Neither provider currently publishes a numeric automated request ceiling for these selected
/// surfaces. This value is an application admission policy, not a provider capacity claim.
pub const OPTIONS_REFERENCE_APPLICATION_REQUESTS_PER_MINUTE: u32 = 1;

/// Conservative Market Squawk sliding-window duration for each provider's public queue.
pub const OPTIONS_REFERENCE_APPLICATION_WINDOW_NANOS: u64 = NANOS_PER_MINUTE;

/// Maximum concurrent request count admitted independently for OCC and Cboe.
pub const OPTIONS_REFERENCE_APPLICATION_MAX_CONCURRENT: u16 = APPLICATION_SINGLE_FLIGHT;

/// Minimum connect-time budget admitted for the selected official large-file endpoints.
pub const OPTIONS_REFERENCE_MINIMUM_CONNECT_TIMEOUT_NANOS: u64 = 10 * 1_000_000_000;

/// Minimum no-progress read budget admitted for one selected official response.
pub const OPTIONS_REFERENCE_MINIMUM_READ_TIMEOUT_NANOS: u64 = 60 * 1_000_000_000;

/// Minimum whole-request budget admitted for one selected official large-file response.
pub const OPTIONS_REFERENCE_MINIMUM_TOTAL_TIMEOUT_NANOS: u64 = 5 * 60 * 1_000_000_000;

const OPTIONS_REFERENCE_MAX_REDIRECTS: u8 = 0;
const OPTIONS_REFERENCE_USER_AGENT: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " options-reference-adapter"
);
const MAX_HEADER_VALUE_BYTES: usize = 1_024;
const MAX_OPERATION_DURATION: Duration = Duration::from_secs(10 * 60);
const RAW_STREAM_CHANNEL_CAPACITY: usize = 8;
const RAW_STREAM_WRITE_CHUNK_BYTES: usize = 64 * 1024;
const LOGICAL_OBJECT_INTEGRITY_CHUNK_BYTES: u64 = 16 * 1024 * 1024;

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
        if admitted.len() != 1 {
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
    surface: ReferenceSurface,
    configured_locator: SourceIdentifier,
    canonical_media_type: SourceIdentifier,
    native_schema: ReferenceNativeSchemaIdentity,
    prior_payload_digest: EvidenceDigest,
    prior_payload_bytes: u64,
    prior_object_id: SourceIdentifier,
    prior_transport_receipt_digest: EvidenceDigest,
}

impl ConditionalCacheRequest {
    /// Selects the strongest observed validator and binds it to one common raw-object claim.
    ///
    /// The exact raw bytes are reverified by the production streaming client before a `304` can
    /// authorize reuse. Constructing this value alone never proves that the raw object is still
    /// present or intact.
    ///
    /// # Errors
    ///
    /// Rejects absent validators, an empty payload, or internally divergent sealed evidence.
    pub fn try_from_prior(
        raw_claim: &ResearchObjectClaim,
        context: &ReferenceObjectContext,
        receipt: &ReferenceHttpReceipt,
    ) -> Result<Self, ReferenceTransportError> {
        if raw_claim.content_digest() != context.payload_digest()
            || raw_claim.size_bytes() != context.payload_bytes()
        {
            return Err(ReferenceTransportError::InvalidConditionalEvidence);
        }
        Self::try_from_evidence(context, receipt)
    }

    fn try_from_evidence(
        context: &ReferenceObjectContext,
        receipt: &ReferenceHttpReceipt,
    ) -> Result<Self, ReferenceTransportError> {
        if receipt.status() != 200
            || !receipt.body_complete()
            || receipt.payload_bytes() == 0
            || receipt.payload_digest() != context.payload_digest()
            || receipt.payload_bytes() != context.payload_bytes()
            || receipt.configured_locator() != context.configured_locator()
            || receipt.final_locator() != context.final_locator()
            || context.transport_evidence() != receipt.transport_evidence()
        {
            return Err(ReferenceTransportError::InvalidConditionalEvidence);
        }
        Ok(Self {
            validator: receipt
                .cache
                .preferred_validator()
                .ok_or(ReferenceTransportError::InvalidConditionalEvidence)?,
            surface: context.surface().clone(),
            configured_locator: context.configured_locator().clone(),
            canonical_media_type: context.media_type().clone(),
            native_schema: context.native_schema_identity().clone(),
            prior_payload_digest: receipt.payload_digest(),
            prior_payload_bytes: receipt.payload_bytes(),
            prior_object_id: context.object_id().clone(),
            prior_transport_receipt_digest: receipt.transport_evidence().receipt_digest(),
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

    fn retained_evidence(
        &self,
    ) -> Result<ReferenceConditionalPriorEvidence, ReferenceTransportError> {
        let validator = match &self.validator {
            CacheValidator::Etag(value) => {
                ReferenceConditionalValidatorEvidence::EntityTag(value.as_str().to_owned())
            }
            CacheValidator::LastModified(value) => {
                ReferenceConditionalValidatorEvidence::LastModified(value.as_str().to_owned())
            }
        };
        ReferenceConditionalPriorEvidence::try_new(
            validator,
            self.surface.clone(),
            self.configured_locator.clone(),
            self.canonical_media_type.clone(),
            self.native_schema.clone(),
            self.prior_payload_digest,
            self.prior_payload_bytes,
            self.prior_object_id.clone(),
            self.prior_transport_receipt_digest,
        )
        .map_err(|_| ReferenceTransportError::InvalidConditionalEvidence)
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
    expected_publication_date: Option<CalendarDate>,
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

    /// Returns the code-owned source publication date expected by a dated request.
    pub const fn expected_publication_date(&self) -> Option<CalendarDate> {
        self.expected_publication_date
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
            || conditional.surface != self.surface
            || conditional.configured_locator != self.locator
            || self.decoder_policy.native_schema_identity()? != conditional.native_schema
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

    #[allow(
        clippy::too_many_arguments,
        reason = "the request is sealed from the complete validated transport authority"
    )]
    fn seal(
        &self,
        source_id: SourceIdentifier,
        provider_id: SourceIdentifier,
        source_contract_digest: EvidenceDigest,
        connect_timeout_nanos: u64,
        read_timeout_nanos: u64,
        total_timeout_nanos: u64,
        operation_timeout: Duration,
    ) -> Result<ReferenceOfficialRequestEvidence, ReferenceTransportError> {
        let operation_timeout_nanos = u64::try_from(operation_timeout.as_nanos())
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ReferenceTransportError::InvalidDeadline)?;
        let conditional_prior = self
            .conditional
            .as_ref()
            .map(ConditionalCacheRequest::retained_evidence)
            .transpose()?;
        ReferenceOfficialRequestEvidence::try_new(
            source_id,
            provider_id,
            source_contract_digest,
            self.surface.provider(),
            self.surface.clone(),
            self.request_id.clone(),
            self.locator.clone(),
            self.accept.as_str(),
            OPTIONS_REFERENCE_USER_AGENT,
            u64::try_from(self.maximum_decoded_bytes)
                .map_err(|_| ReferenceTransportError::ResponseTooLarge)?,
            OPTIONS_REFERENCE_MAX_REDIRECTS,
            connect_timeout_nanos,
            read_timeout_nanos,
            total_timeout_nanos,
            operation_timeout_nanos,
            self.wall_started_at,
            self.wall_deadline,
            self.expected_publication_date,
            self.decoder_policy.native_schema_identity()?,
            conditional_prior,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)
    }

    #[cfg(test)]
    fn seal_for_injected_executor(
        &self,
        operation_timeout: Duration,
    ) -> Result<ReferenceOfficialRequestEvidence, ReferenceTransportError> {
        let source_id = source_identifier(reference_source_id(self.surface.provider()))?;
        let provider_id = source_identifier(reference_budget_provider_id(self.surface.provider()))?;
        self.seal(
            source_id,
            provider_id,
            injected_source_contract_digest(self.surface.provider()),
            OPTIONS_REFERENCE_MINIMUM_CONNECT_TIMEOUT_NANOS,
            OPTIONS_REFERENCE_MINIMUM_READ_TIMEOUT_NANOS,
            OPTIONS_REFERENCE_MINIMUM_TOTAL_TIMEOUT_NANOS,
            operation_timeout,
        )
    }

    #[cfg(test)]
    fn http_request(
        &self,
        operation_timeout: Duration,
    ) -> Result<ReferenceHttpRequest, ReferenceTransportError> {
        ReferenceHttpRequest::try_from_evidence(self.seal_for_injected_executor(operation_timeout)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum DecoderPolicy {
    CboeAllSeries { freeze: CboeSchemaFreeze },
    OccDlp { schema: OccDlpSchema },
    OccMemoCsv { schema: OccMemoCsvSchema },
    OccMemoDocumentUninterpreted,
}

impl DecoderPolicy {
    fn native_schema_identity(
        &self,
    ) -> Result<ReferenceNativeSchemaIdentity, ReferenceTransportError> {
        let name = match self {
            Self::CboeAllSeries { freeze } => freeze
                .admitted()
                .first()
                .ok_or(ReferenceTransportError::InvalidSchemaFreeze)?
                .native_schema(),
            Self::OccDlp { schema } => schema.native_schema(),
            Self::OccMemoCsv { schema } => schema.native_schema(),
            Self::OccMemoDocumentUninterpreted => "occ-memo-operative-document-uninterpreted-v1",
        };
        native_schema_identity(name)
    }
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
    /// DLP TXT/XML, the OCC memo CSV export, and a complete memo document by memo number. The
    /// closed JSON placeholder and attachment surface are rejected because no exact official
    /// request is established for them here.
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
            let (locator, accept, safety_maximum, decoder_policy, expected_publication_date) =
                match surface {
                    ReferenceSurface::CboeAllSeries { venue } => (
                        venue.all_series_locator().to_owned(),
                        "text/csv,application/octet-stream;q=0.5",
                        CBOE_ALL_SERIES_MAX_BYTES,
                        DecoderPolicy::CboeAllSeries {
                            freeze: policy.cboe_schema_freeze.clone(),
                        },
                        None,
                    ),
                    ReferenceSurface::OccDlpSelectedText => (
                        OCC_DLP_SELECTED_LOCATOR.to_owned(),
                        "text/plain,application/octet-stream;q=0.5",
                        OCC_DLP_MAX_BYTES,
                        DecoderPolicy::OccDlp {
                            schema: OccDlpSchema::SelectedTextV1,
                        },
                        None,
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
                            DecoderPolicy::OccDlp {
                                schema: OccDlpSchema::DailyTextV1,
                            },
                            Some(date),
                        )
                    }
                    ReferenceSurface::OccDlpDailyXml => {
                        let date = policy
                            .occ_dlp_daily_date
                            .ok_or(ReferenceTransportError::MissingPublicationDate)?;
                        (
                            format!(
                                "{OCC_DLP_DAILY_BASE}?reportDate={:04}{:02}{:02}&format=xml",
                                date.year(),
                                date.month(),
                                date.day()
                            ),
                            "application/xml,text/xml;q=0.9,application/octet-stream;q=0.5",
                            OCC_DLP_MAX_BYTES,
                            DecoderPolicy::OccDlp {
                                schema: OccDlpSchema::DailyXmlV1,
                            },
                            Some(date),
                        )
                    }
                    ReferenceSurface::OccMemoIndexCsv => (
                        OCC_MEMO_EXPORT_LOCATOR.to_owned(),
                        "text/csv,application/octet-stream;q=0.5",
                        OCC_MEMO_MAX_BYTES,
                        DecoderPolicy::OccMemoCsv {
                            schema: policy.occ_memo_csv_schema,
                        },
                        None,
                    ),
                    ReferenceSurface::OccMemoDocument { memo_number } => (
                        format!("{OCC_MEMO_DOCUMENT_BASE}?number={memo_number}"),
                        "text/html,application/pdf;q=0.9",
                        OCC_MEMO_DOCUMENT_MAX_BYTES,
                        DecoderPolicy::OccMemoDocumentUninterpreted,
                        None,
                    ),
                    ReferenceSurface::OccMemoIndexJson
                    | ReferenceSurface::OccMemoAttachment { .. } => {
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
                expected_publication_date,
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
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReferenceHttpRequest {
    evidence: ReferenceOfficialRequestEvidence,
    if_none_match: Option<ReferenceHeaderValue>,
    if_modified_since: Option<ReferenceHeaderValue>,
}

#[cfg(test)]
impl ReferenceHttpRequest {
    fn try_from_evidence(
        evidence: ReferenceOfficialRequestEvidence,
    ) -> Result<Self, ReferenceTransportError> {
        let (if_none_match, if_modified_since) = conditional_headers(&evidence)?;
        Ok(Self {
            evidence,
            if_none_match,
            if_modified_since,
        })
    }

    /// Returns the HTTP method.
    pub(crate) const fn method(&self) -> ReferenceRequestMethod {
        self.evidence.method()
    }

    /// Returns the exact provider URL.
    pub(crate) const fn locator(&self) -> &SourceIdentifier {
        self.evidence.configured_locator()
    }

    /// Returns whether compressed transfer was declined so content length and retained bytes share
    /// one representation.
    pub(crate) const fn accept_encoding_identity(&self) -> bool {
        self.evidence.accept_encoding_identity()
    }

    /// Returns the conditional ETag field when a prior exact object supplied one.
    pub(crate) const fn if_none_match(&self) -> Option<&ReferenceHeaderValue> {
        self.if_none_match.as_ref()
    }

    /// Returns the conditional modification date when no ETag was observed.
    pub(crate) const fn if_modified_since(&self) -> Option<&ReferenceHeaderValue> {
        self.if_modified_since.as_ref()
    }

    /// Returns the application redirect-hop ceiling.
    pub(crate) const fn maximum_redirects(&self) -> usize {
        self.evidence.maximum_redirects() as usize
    }

    /// Returns the exact sealed request evidence used to build this injected request.
    pub(crate) const fn evidence(&self) -> &ReferenceOfficialRequestEvidence {
        &self.evidence
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

    pub(crate) fn ensure_open(&self) -> Result<(), ReferenceTransportError> {
        if self.cancellation.is_cancelled() {
            Err(ReferenceTransportError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ReferenceTransportError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

struct ReapedBlockingWorker<T> {
    result: tokio::sync::oneshot::Receiver<Result<T, ReferenceTransportError>>,
    cancellation: ReferenceCancellation,
    active: bool,
}

impl<T> ReapedBlockingWorker<T>
where
    T: Send + 'static,
{
    fn spawn<F>(operation: F) -> Self
    where
        F: FnOnce(ReferenceCancellation) -> Result<T, ReferenceTransportError> + Send + 'static,
    {
        let cancellation = ReferenceCancellation::new();
        let worker_cancellation = cancellation.clone();
        let blocking = tokio::task::spawn_blocking(move || operation(worker_cancellation));
        let (sender, result) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let terminal = blocking
                .await
                .map_err(|_| ReferenceTransportError::RawCaptureWorkerFailed)
                .and_then(std::convert::identity);
            let _ = sender.send(terminal);
        });
        Self {
            result,
            cancellation,
            active: true,
        }
    }

    fn cancel(&self) {
        self.cancellation.cancel();
    }

    fn complete(
        &mut self,
        result: Result<Result<T, ReferenceTransportError>, tokio::sync::oneshot::error::RecvError>,
    ) -> Result<T, ReferenceTransportError> {
        match result {
            Ok(terminal) => {
                self.active = false;
                terminal
            }
            Err(_) => {
                self.cancellation.cancel();
                self.active = false;
                Err(ReferenceTransportError::RawCaptureWorkerFailed)
            }
        }
    }
}

impl<T> Drop for ReapedBlockingWorker<T> {
    fn drop(&mut self) {
        if self.active {
            self.cancellation.cancel();
        }
    }
}

/// Structured result returned by an injected HTTP executor.
///
/// The executor must stop reading before `request.maximum_decoded_bytes`, honor the control's
/// cancellation/deadline, disable transparent content decoding when identity encoding is
/// requested, and supply the exact final locator and redirect chain.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReferenceHttpResponse {
    status: u16,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    content_type: Option<ReferenceHeaderValue>,
    content_disposition: Option<ReferenceHeaderValue>,
    content_length: Option<u64>,
    content_encoding: Option<ReferenceHeaderValue>,
    cache: HttpCacheEvidence,
    retry_after: Option<ReferenceHeaderValue>,
    received_at: Timestamp,
    body_complete: bool,
    body: Vec<u8>,
}

#[cfg(test)]
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
    pub(crate) fn try_new(
        status: u16,
        final_locator: SourceIdentifier,
        redirect_chain: Vec<SourceIdentifier>,
        content_type: Option<ReferenceHeaderValue>,
        content_disposition: Option<ReferenceHeaderValue>,
        content_length: Option<u64>,
        content_encoding: Option<ReferenceHeaderValue>,
        cache: HttpCacheEvidence,
        retry_after: Option<ReferenceHeaderValue>,
        received_at: Timestamp,
        body_complete: bool,
        body: Vec<u8>,
    ) -> Result<Self, ReferenceTransportError> {
        if redirect_chain.len() > usize::from(OPTIONS_REFERENCE_MAX_REDIRECTS) {
            return Err(ReferenceTransportError::RedirectLimitExceeded);
        }
        Ok(Self {
            status,
            final_locator,
            redirect_chain,
            content_type,
            content_disposition,
            content_length,
            content_encoding,
            cache,
            retry_after,
            received_at,
            body_complete,
            body,
        })
    }
}

/// Bounded HTTP execution seam supplied by the root source-composition layer.
#[cfg(test)]
pub(crate) trait ReferenceHttpExecutor {
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
    /// OCC DLP six-field text or XML contract.
    OccDlp(OccDlpSchema),
    /// One exact OCC memo CSV export header revision.
    OccMemoCsv(OccMemoCsvSchema),
    /// Complete raw memo content retained for later typed interpretation.
    OccMemoDocumentUninterpreted,
}

impl SelectedReferenceDecoder {
    fn native_schema(self) -> &'static str {
        match self {
            Self::CboeAllSeries(schema) => schema.native_schema(),
            Self::OccDlp(schema) => schema.native_schema(),
            Self::OccMemoCsv(schema) => schema.native_schema(),
            Self::OccMemoDocumentUninterpreted => "occ-memo-operative-document-uninterpreted-v1",
        }
    }

    fn canonical_media_type(self, observed: &str) -> &'static str {
        match self {
            Self::CboeAllSeries(_) | Self::OccMemoCsv(_) => "text/csv",
            Self::OccDlp(schema) => schema.media_type(),
            Self::OccMemoDocumentUninterpreted
                if base_media_type(observed) == "application/pdf" =>
            {
                "application/pdf"
            }
            Self::OccMemoDocumentUninterpreted => "text/html",
        }
    }

    fn native_schema_identity(
        self,
    ) -> Result<ReferenceNativeSchemaIdentity, ReferenceTransportError> {
        native_schema_identity(self.native_schema())
    }
}

/// Exact admitted HTTP evidence for a modified response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceHttpReceipt {
    transport: ReferenceTransportEvidence,
    observed_content_type: ReferenceHeaderValue,
    observed_content_disposition: Option<ReferenceHeaderValue>,
    source_filename: Option<SourceIdentifier>,
    source_publication_date: Option<CalendarDate>,
    cache: HttpCacheEvidence,
}

impl ReferenceHttpReceipt {
    fn try_modified(
        transport: ReferenceTransportEvidence,
        observed_content_type: ReferenceHeaderValue,
        observed_content_disposition: Option<ReferenceHeaderValue>,
        source_filename: Option<SourceIdentifier>,
        source_publication_date: Option<CalendarDate>,
        cache: HttpCacheEvidence,
    ) -> Result<Self, ReferenceTransportError> {
        if !transport.is_modified()
            || transport.observed_content_type() != Some(observed_content_type.as_str())
            || transport.observed_content_disposition()
                != observed_content_disposition
                    .as_ref()
                    .map(ReferenceHeaderValue::as_str)
            || transport.etag() != cache.etag().map(ReferenceHeaderValue::as_str)
            || transport.cache_last_modified()
                != cache.last_modified().map(ReferenceHeaderValue::as_str)
        {
            return Err(ReferenceTransportError::InvalidObjectEvidence);
        }
        Ok(Self {
            transport,
            observed_content_type,
            observed_content_disposition,
            source_filename,
            source_publication_date,
            cache,
        })
    }

    /// Returns the complete sealed request/response evidence.
    pub const fn transport_evidence(&self) -> &ReferenceTransportEvidence {
        &self.transport
    }

    /// Returns the SHA-256 identity of the exact official request this response answered.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.transport.request_digest()
    }

    /// Returns a domain-separated SHA-256 identity of this complete secret-free HTTP receipt.
    pub fn evidence_digest(&self) -> Result<EvidenceDigest, ReferenceTransportError> {
        Ok(self.transport.receipt_digest())
    }
    /// Returns the HTTP status.
    pub const fn status(&self) -> u16 {
        self.transport.status()
    }

    /// Returns the exact configured locator.
    pub const fn configured_locator(&self) -> &SourceIdentifier {
        self.transport.request().configured_locator()
    }

    /// Returns the exact final locator.
    pub const fn final_locator(&self) -> &SourceIdentifier {
        self.transport.final_locator()
    }

    /// Returns retained redirect evidence.
    pub fn redirect_chain(&self) -> &[SourceIdentifier] {
        self.transport.redirect_chain()
    }

    /// Returns the exact observed Content-Type field.
    pub const fn observed_content_type(&self) -> &ReferenceHeaderValue {
        &self.observed_content_type
    }

    /// Returns the exact observed Content-Disposition field when supplied.
    pub const fn observed_content_disposition(&self) -> Option<&ReferenceHeaderValue> {
        self.observed_content_disposition.as_ref()
    }

    /// Returns the validated provider filename when this surface defines one.
    pub const fn source_filename(&self) -> Option<&SourceIdentifier> {
        self.source_filename.as_ref()
    }

    /// Returns the source publication date without inventing a time or timezone.
    pub const fn source_publication_date(&self) -> Option<CalendarDate> {
        self.source_publication_date
    }

    /// Returns the declared identity body length when supplied.
    pub const fn declared_content_length(&self) -> Option<u64> {
        self.transport.declared_content_length()
    }

    /// Returns runtime-observed cache fields.
    pub const fn cache(&self) -> &HttpCacheEvidence {
        &self.cache
    }

    /// Returns the trusted local receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.transport.body_completed_at()
    }

    /// Returns the trusted local response-header receipt instant.
    pub const fn headers_received_at(&self) -> Timestamp {
        self.transport.headers_received_at()
    }

    /// Returns monotonic request-send through terminal response-body elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport.transport_elapsed_nanos()
    }

    /// Returns the exact payload digest.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.transport.response_body_digest()
    }

    /// Returns exact retained payload bytes.
    pub const fn payload_bytes(&self) -> u64 {
        self.transport.response_body_bytes()
    }

    /// Returns the executor's terminal-body observation.
    pub const fn body_complete(&self) -> bool {
        self.transport.body_complete()
    }
}

/// Complete exact bytes, decoder selection, and provenance for one modified object.
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RetrievedReferenceObject {
    bytes: Vec<u8>,
    decoder: SelectedReferenceDecoder,
    context: ReferenceObjectContext,
    receipt: ReferenceHttpReceipt,
}

#[cfg(test)]
pub(crate) fn capture_retrieved_for_test(
    store: Arc<SealedResearchJournalStore>,
    request: &OfficialReferenceRequest,
    object: RetrievedReferenceObject,
    control: &ReferenceFetchControl,
) -> Result<StreamedReferenceObject, ReferenceTransportError> {
    control.ensure_open()?;
    let RetrievedReferenceObject {
        bytes,
        decoder,
        context,
        receipt,
    } = object;
    let maximum_bytes = u64::try_from(request.maximum_decoded_bytes)
        .map_err(|_| ReferenceTransportError::ResponseTooLarge)?;
    if bytes.is_empty()
        || u64::try_from(bytes.len()).ok() != Some(context.payload_bytes())
        || context.payload_bytes() > maximum_bytes
        || context.surface() != request.surface()
        || context.configured_locator() != request.locator()
        || context.native_schema_identity() != &decoder.native_schema_identity()?
        || context.transport_evidence() != receipt.transport_evidence()
    {
        return Err(ReferenceTransportError::InvalidObjectEvidence);
    }
    let admission = logical_object_admission(maximum_bytes)?;
    let mut pending = store
        .begin_logical_object(admission)
        .map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
    for chunk in bytes.chunks(RAW_STREAM_WRITE_CHUNK_BYTES) {
        control.ensure_open()?;
        pending
            .write_all(chunk)
            .map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
    }
    let finish_control = SourceRawObjectControl {
        control: control.clone(),
        worker_cancellation: ReferenceCancellation::new(),
        wall_deadline: request.wall_deadline,
        authority: SourceRawObjectAuthority::Fixture,
    };
    let raw_object = store
        .finish_logical_object(pending, &finish_control)
        .map_err(map_logical_object_error)?;
    if raw_object.content_digest() != context.payload_digest()
        || raw_object.size_bytes() != context.payload_bytes()
    {
        return Err(ReferenceTransportError::InvalidObjectEvidence);
    }
    Ok(StreamedReferenceObject {
        raw_object,
        context,
        http_receipt: receipt,
        decoder,
        maximum_decoded_bytes: maximum_bytes,
        completion: ReferenceCompletionAuthority::Fixture,
        wall_deadline: request.wall_deadline,
        control: control.clone(),
    })
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
    transport: ReferenceTransportEvidence,
    configured_locator: SourceIdentifier,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    received_at: Timestamp,
    transport_elapsed_nanos: u64,
    prior_payload_digest: EvidenceDigest,
    prior_payload_bytes: u64,
    prior_object_id: SourceIdentifier,
    response_cache: HttpCacheEvidence,
}

impl ReferenceNotModifiedReceipt {
    /// Returns the complete sealed request and admitted 304 response evidence.
    pub const fn transport_evidence(&self) -> &ReferenceTransportEvidence {
        &self.transport
    }

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

    /// Returns monotonic request-send through the terminal 304 response elapsed time.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.transport_elapsed_nanos
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
#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReferenceFetchOutcome {
    /// HTTP 200 supplied a complete bounded exact object.
    Modified(RetrievedReferenceObject),
    /// HTTP 304 authorized reuse of one exact prior object.
    NotModified(ReferenceNotModifiedReceipt),
}

/// Official source orchestration over a caller-supplied bounded HTTP executor.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct OfficialReferenceSource<T> {
    executor: T,
}

#[cfg(test)]
impl<T> OfficialReferenceSource<T>
where
    T: ReferenceHttpExecutor,
{
    /// Binds the official-source validator to an executor.
    pub(crate) const fn new(executor: T) -> Self {
        Self { executor }
    }

    /// Retrieves and validates one exact request without retries or hidden fallback.
    ///
    /// # Errors
    ///
    /// Fails closed on cancellation/deadline, redirect or locator drift, statuses other than 200
    /// or authorized 304, 429, encoding/content-length/content-type drift, byte overflow, or an
    /// unrecognized frozen schema. No partial bytes are returned.
    pub(crate) fn fetch(
        &self,
        request: &OfficialReferenceRequest,
        control: &ReferenceFetchControl,
    ) -> Result<ReferenceFetchOutcome, ReferenceTransportError> {
        control.ensure_open()?;
        let transport_timeout = control.remaining()?;
        let http_request = request.http_request(transport_timeout)?;
        let transport_started = Instant::now();
        let response = self.executor.execute(&http_request, control)?;
        let transport_elapsed_nanos = elapsed_nanos(transport_started, transport_timeout)?;
        control.ensure_open()?;
        validate_locator_lineage(request, &response)?;
        if response.received_at < request.wall_started_at {
            return Err(ReferenceTransportError::InvalidResponseClock);
        }
        if response.received_at > request.wall_deadline {
            return Err(ReferenceTransportError::DeadlineExceeded);
        }

        match response.status {
            304 => {
                return admit_not_modified(
                    http_request.evidence().clone(),
                    response,
                    transport_elapsed_nanos,
                );
            }
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
        let source_file =
            validate_source_file_evidence(request, response.content_disposition.as_ref())?;
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
        let http_last_modified = response
            .cache
            .last_modified()
            .map(|value| HttpLastModifiedEvidence::try_from_header(value.as_str()))
            .transpose()
            .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let clocks = ObjectClockEvidence::try_new(
            source_file
                .publication_date
                .map(ResearchTemporalCoordinate::calendar_date),
            None,
            AvailabilityEvidence::local_first_observed(response.received_at),
            response.received_at,
            transport_elapsed_nanos,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let canonical_media_type = SourceIdentifier::try_from(canonical_media_type)
            .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let native_schema = decoder.native_schema_identity()?;
        let transport = ReferenceTransportEvidence::try_modified(
            http_request.evidence().clone(),
            response.status,
            response.final_locator.clone(),
            response.redirect_chain.clone(),
            observed_content_type.as_str(),
            response
                .content_disposition
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            response
                .content_encoding
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            response.content_length,
            response.cache.etag().map(|value| value.as_str().to_owned()),
            response
                .cache
                .last_modified()
                .map(|value| value.as_str().to_owned()),
            response.received_at,
            response.received_at,
            transport_elapsed_nanos,
            digest,
            payload_bytes,
            canonical_media_type.clone(),
            native_schema.clone(),
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let receipt = ReferenceHttpReceipt::try_modified(
            transport.clone(),
            observed_content_type,
            response.content_disposition,
            source_file.filename.clone(),
            source_file.publication_date,
            response.cache,
        )?;
        let context = ReferenceObjectContext::try_new(
            request.surface.provider(),
            request.surface.clone(),
            object_id,
            request.locator.clone(),
            response.final_locator.clone(),
            canonical_media_type,
            digest,
            payload_bytes,
            native_schema,
            clocks,
            source_file.filename,
            source_file.publication_date,
            http_last_modified,
            transport,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        Ok(ReferenceFetchOutcome::Modified(RetrievedReferenceObject {
            bytes: response.body,
            decoder,
            context,
            receipt,
        }))
    }
}

/// Modified object captured through the shared logical raw-object authority.
///
/// This descriptor is intentionally noncloneable and non-serializable. It must be strictly parsed
/// and consumed into a pending handoff before shared composition can retain its common raw receipt.
pub struct StreamedReferenceObject {
    raw_object: VerifiedResearchObject,
    context: ReferenceObjectContext,
    http_receipt: ReferenceHttpReceipt,
    decoder: SelectedReferenceDecoder,
    maximum_decoded_bytes: u64,
    completion: ReferenceCompletionAuthority,
    wall_deadline: Timestamp,
    control: ReferenceFetchControl,
}

impl std::fmt::Debug for StreamedReferenceObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamedReferenceObject")
            .field("object_id", self.context.object_id())
            .field("decoder", &self.decoder)
            .field("awaiting_schema_completion", &true)
            .finish_non_exhaustive()
    }
}

enum ReferenceCompletionAuthority {
    Production(Arc<InFlightExtractionRequest>),
    #[cfg(test)]
    Fixture,
}

impl ReferenceCompletionAuthority {
    fn validate_current(&self) -> Result<(), ReferenceTransportError> {
        match self {
            Self::Production(in_flight) => in_flight
                .validate_current()
                .map_err(ReferenceTransportError::Authority),
            #[cfg(test)]
            Self::Fixture => Ok(()),
        }
    }

    fn record_success(self) -> Result<(), ReferenceTransportError> {
        match self {
            Self::Production(in_flight) => Arc::try_unwrap(in_flight)
                .map_err(|_| ReferenceTransportError::InvalidProtocolState)?
                .record_success()
                .map_err(ReferenceTransportError::Authority),
            #[cfg(test)]
            Self::Fixture => Ok(()),
        }
    }
}

/// Non-forgeable terminal schema evidence produced only by one strict provider parser.
#[derive(Debug, Eq, PartialEq)]
pub struct StrictReferenceParseReceipt {
    kind: StrictReferenceParseReceiptKind,
}

/// Non-forgeable proof that a complete OCC memo object matched its declared document envelope.
#[derive(Debug, Eq, PartialEq)]
pub struct StrictUninterpretedMemoDocumentReceipt {
    context: ReferenceObjectContext,
}

impl StrictUninterpretedMemoDocumentReceipt {
    /// Returns the exact raw-object evidence validated by the document-envelope parser.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }
}

#[derive(Debug, Eq, PartialEq)]
enum StrictReferenceParseReceiptKind {
    /// Exact Cboe `All Series` parser completion.
    Cboe(crate::CboeAllSeriesParseReceipt),
    /// Exact OCC DLP selected/daily TXT/XML parser completion.
    OccDlp(crate::OccDlpParseReceipt),
    /// Exact OCC memo index parser completion.
    OccMemo(crate::OccMemoParseReceipt),
}

impl From<crate::CboeAllSeriesParseReceipt> for StrictReferenceParseReceipt {
    fn from(receipt: crate::CboeAllSeriesParseReceipt) -> Self {
        Self {
            kind: StrictReferenceParseReceiptKind::Cboe(receipt),
        }
    }
}

impl From<crate::OccDlpParseReceipt> for StrictReferenceParseReceipt {
    fn from(receipt: crate::OccDlpParseReceipt) -> Self {
        Self {
            kind: StrictReferenceParseReceiptKind::OccDlp(receipt),
        }
    }
}

impl From<crate::OccMemoParseReceipt> for StrictReferenceParseReceipt {
    fn from(receipt: crate::OccMemoParseReceipt) -> Self {
        Self {
            kind: StrictReferenceParseReceiptKind::OccMemo(receipt),
        }
    }
}

impl StrictReferenceParseReceipt {
    fn into_page_receipt(
        self,
        decoder: SelectedReferenceDecoder,
    ) -> Result<crate::ReferencePageReceipt, ReferenceTransportError> {
        let page = match (self.kind, decoder) {
            (
                StrictReferenceParseReceiptKind::Cboe(receipt),
                SelectedReferenceDecoder::CboeAllSeries(schema),
            ) if receipt.schema() == schema => receipt.page_receipt(),
            (
                StrictReferenceParseReceiptKind::OccDlp(receipt),
                SelectedReferenceDecoder::OccDlp(schema),
            ) => {
                let page = receipt.page_receipt();
                if page.context().native_schema().as_str() != schema.native_schema() {
                    return Err(ReferenceTransportError::SchemaValidationFailed);
                }
                page
            }
            (
                StrictReferenceParseReceiptKind::OccMemo(receipt),
                SelectedReferenceDecoder::OccMemoCsv(schema),
            ) => {
                let page = receipt.page_receipt();
                if page.context().native_schema().as_str() != schema.native_schema() {
                    return Err(ReferenceTransportError::SchemaValidationFailed);
                }
                page
            }
            _ => return Err(ReferenceTransportError::SchemaValidationFailed),
        };
        Ok(page)
    }
}

struct ControlledSchemaReader<'a> {
    raw_object: &'a mut VerifiedResearchObject,
    wall_deadline: Timestamp,
    control: &'a ReferenceFetchControl,
    completion: &'a ReferenceCompletionAuthority,
    control_failure: Option<ReferenceTransportError>,
}

impl ControlledSchemaReader<'_> {
    fn checkpoint(&mut self) -> std::io::Result<()> {
        if let Err(error) =
            ensure_handoff_current(self.wall_deadline, self.control, self.completion)
        {
            let message = error.to_string();
            if self.control_failure.is_none() {
                self.control_failure = Some(error);
            }
            return Err(std::io::Error::other(message));
        }
        Ok(())
    }

    fn take_control_failure(&mut self) -> Option<ReferenceTransportError> {
        self.control_failure.take()
    }
}

impl std::io::Read for ControlledSchemaReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        self.checkpoint()?;
        let admitted = buffer.len().min(RAW_STREAM_WRITE_CHUNK_BYTES);
        let read = self.raw_object.read(&mut buffer[..admitted])?;
        self.checkpoint()?;
        Ok(read)
    }
}

impl StreamedReferenceObject {
    /// Returns provider, surface, schema, digest, and clock evidence needed to bind a strict
    /// parser. Common raw-object authority remains hidden until parser completion.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns the exact selected provider-native decoder.
    pub const fn decoder(&self) -> SelectedReferenceDecoder {
        self.decoder
    }

    /// Strictly stream-parses one exact Cboe `All Series` object to a caller-owned sink.
    ///
    /// # Errors
    ///
    /// Rejects any non-Cboe decoder, exact-payload drift, malformed schema, sink failure, stale
    /// extraction authority, cancellation, or deadline expiry. Caller staging must be discarded
    /// unless this method and final handoff completion both succeed.
    pub fn parse_cboe_all_series<F>(
        &mut self,
        sink: F,
    ) -> Result<crate::CboeAllSeriesParseReceipt, ReferenceTransportError>
    where
        F: FnMut(crate::CboeSeriesReference) -> Result<(), crate::CboeParseError>,
    {
        let (venue, schema) = match (self.context.surface(), self.decoder) {
            (
                ReferenceSurface::CboeAllSeries { venue },
                SelectedReferenceDecoder::CboeAllSeries(schema),
            ) => (*venue, schema),
            _ => return Err(ReferenceTransportError::SchemaValidationFailed),
        };
        let parser = crate::CboeAllSeriesParser::try_new(venue, schema, self.context.clone())
            .map_err(|_| ReferenceTransportError::SchemaValidationFailed)?;
        let mut reader = self.controlled_schema_reader()?;
        let result = parser.parse(&mut reader, sink);
        let control_failure = reader.take_control_failure();
        drop(reader);
        if let Some(error) = control_failure {
            return Err(error);
        }
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        result.map_err(|_| ReferenceTransportError::SchemaValidationFailed)
    }

    /// Strictly stream-parses one exact OCC DLP TXT/XML object to a caller-owned sink.
    ///
    /// # Errors
    ///
    /// Rejects any non-DLP decoder, exact-payload drift, malformed schema, sink failure, stale
    /// extraction authority, cancellation, or deadline expiry. Caller staging must be discarded
    /// unless this method and final handoff completion both succeed.
    pub fn parse_occ_dlp<F>(
        &mut self,
        sink: F,
    ) -> Result<crate::OccDlpParseReceipt, ReferenceTransportError>
    where
        F: FnMut(crate::OccDlpProductReference) -> Result<(), crate::OccParseError>,
    {
        if !matches!(self.decoder, SelectedReferenceDecoder::OccDlp(_)) {
            return Err(ReferenceTransportError::SchemaValidationFailed);
        }
        let parser = crate::OccDlpParser::try_new(self.context.clone())
            .map_err(|_| ReferenceTransportError::SchemaValidationFailed)?;
        let mut reader = self.controlled_schema_reader()?;
        let result = parser.parse(&mut reader, sink);
        let control_failure = reader.take_control_failure();
        drop(reader);
        if let Some(error) = control_failure {
            return Err(error);
        }
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        result.map_err(|_| ReferenceTransportError::SchemaValidationFailed)
    }

    /// Strictly stream-parses one exact OCC memo CSV export to a caller-owned sink.
    ///
    /// # Errors
    ///
    /// Rejects any non-memo-CSV decoder, exact-payload drift, malformed schema, sink failure,
    /// stale extraction authority, cancellation, or deadline expiry. Caller staging must be
    /// discarded unless this method and final handoff completion both succeed.
    pub fn parse_occ_memo_csv<F>(
        &mut self,
        sink: F,
    ) -> Result<crate::OccMemoParseReceipt, ReferenceTransportError>
    where
        F: FnMut(crate::OccMemoDiscovery) -> Result<(), crate::OccParseError>,
    {
        let SelectedReferenceDecoder::OccMemoCsv(schema) = self.decoder else {
            return Err(ReferenceTransportError::SchemaValidationFailed);
        };
        let context = self.context.clone();
        let mut reader = self.controlled_schema_reader()?;
        let result = crate::OccMemoParser::parse_csv(schema, context, &mut reader, sink);
        let control_failure = reader.take_control_failure();
        drop(reader);
        if let Some(error) = control_failure {
            return Err(error);
        }
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        result.map_err(|_| ReferenceTransportError::SchemaValidationFailed)
    }

    /// Strictly validates the declared HTML or PDF envelope of one complete OCC memo document.
    ///
    /// This establishes only document-format admission. The document remains explicitly
    /// uninterpreted, and no title or body text becomes lifecycle or contract economics.
    ///
    /// # Errors
    ///
    /// Rejects any non-document surface/decoder, raw evidence drift, or mismatched HTML/PDF
    /// envelope.
    pub fn validate_uninterpreted_memo_document(
        &mut self,
    ) -> Result<StrictUninterpretedMemoDocumentReceipt, ReferenceTransportError> {
        if self.decoder != SelectedReferenceDecoder::OccMemoDocumentUninterpreted
            || !matches!(
                self.context.surface(),
                ReferenceSurface::OccMemoDocument { .. }
            )
        {
            return Err(ReferenceTransportError::SchemaValidationFailed);
        }
        let mut prefix = Vec::with_capacity(4_096);
        let mut tail = Vec::with_capacity(1_024);
        let context = self.context.clone();
        let mut controlled = self.controlled_schema_reader()?;
        let validation = (|| -> Result<(), ReferenceTransportError> {
            let mut payload =
                ExactPayloadReader::try_new(&mut controlled, &context, OCC_MEMO_DOCUMENT_MAX_BYTES)
                    .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
            let mut buffer = [0_u8; RAW_STREAM_WRITE_CHUNK_BYTES];
            loop {
                let read = payload
                    .read(&mut buffer)
                    .map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
                if read == 0 {
                    break;
                }
                let bytes = &buffer[..read];
                let prefix_remaining = 4_096_usize.saturating_sub(prefix.len());
                prefix.extend_from_slice(&bytes[..bytes.len().min(prefix_remaining)]);
                retain_rolling_tail(&mut tail, bytes, 1_024);
            }
            payload
                .finish()
                .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
            Ok(())
        })();
        let control_failure = controlled.take_control_failure();
        drop(controlled);
        if let Some(error) = control_failure {
            return Err(error);
        }
        validation?;
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        validate_uninterpreted_document_envelope(&context, &prefix, &tail)?;
        Ok(StrictUninterpretedMemoDocumentReceipt { context })
    }

    fn controlled_schema_reader(
        &mut self,
    ) -> Result<ControlledSchemaReader<'_>, ReferenceTransportError> {
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        if self.context.payload_bytes() == 0
            || self.context.payload_bytes() > self.maximum_decoded_bytes
            || self.raw_object.content_digest() != self.context.payload_digest()
            || self.raw_object.size_bytes() != self.context.payload_bytes()
        {
            return Err(ReferenceTransportError::InvalidObjectEvidence);
        }
        self.raw_object
            .seek(SeekFrom::Start(0))
            .map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
        Ok(ControlledSchemaReader {
            raw_object: &mut self.raw_object,
            wall_deadline: self.wall_deadline,
            control: &self.control,
            completion: &self.completion,
            control_failure: None,
        })
    }

    /// Completes shared-budget success only after a strict parser produced terminal evidence for
    /// these exact sealed bytes. This consumes the pending in-flight authority.
    ///
    /// # Errors
    ///
    /// Rejects mismatched object context, partial/rejected/empty parser output, a document-only
    /// surface, stale authority, or durable shared-budget terminalization failure.
    pub fn complete_after_schema_validation(
        self,
        receipt: StrictReferenceParseReceipt,
    ) -> Result<PendingReferenceTypedHandoff, ReferenceTransportError> {
        let receipt = receipt.into_page_receipt(self.decoder)?;
        if receipt.context() != &self.context
            || receipt.page_ordinal() != std::num::NonZeroU32::MIN
            || receipt.returned_records() == 0
            || receipt.rejected_records() != 0
            || !matches!(receipt.terminal_state(), crate::PageTerminalState::Terminal)
            || matches!(
                self.context.surface(),
                ReferenceSurface::OccMemoDocument { .. }
                    | ReferenceSurface::OccMemoAttachment { .. }
            )
        {
            return Err(ReferenceTransportError::SchemaValidationFailed);
        }
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        Ok(PendingReferenceTypedHandoff {
            raw_object: self.raw_object,
            context: self.context,
            http_receipt: self.http_receipt,
            page_receipt: receipt,
            completion: self.completion,
            wall_deadline: self.wall_deadline,
            control: self.control,
        })
    }

    /// Completes transport success for a strictly envelope-validated memo document while
    /// preserving its explicit uninterpreted state. This never promotes title text or document
    /// bytes to lifecycle economics.
    ///
    /// # Errors
    ///
    /// Rejects non-document decoders/surfaces, mismatched parser proof, stale authority, or
    /// shared-budget failure.
    pub fn complete_uninterpreted_memo_document(
        self,
        receipt: StrictUninterpretedMemoDocumentReceipt,
    ) -> Result<PendingUninterpretedMemoHandoff, ReferenceTransportError> {
        if self.decoder != SelectedReferenceDecoder::OccMemoDocumentUninterpreted
            || !matches!(
                self.context.surface(),
                ReferenceSurface::OccMemoDocument { .. }
            )
            || receipt.context != self.context
        {
            return Err(ReferenceTransportError::SchemaValidationFailed);
        }
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        Ok(PendingUninterpretedMemoHandoff {
            raw_object: self.raw_object,
            context: self.context,
            http_receipt: self.http_receipt,
            completion: self.completion,
            wall_deadline: self.wall_deadline,
            control: self.control,
        })
    }
}

/// One-use typed provider-reference handoff awaiting final raw re-verification.
///
/// The capability is intentionally noncloneable and non-serializable. Dropping it never reports a
/// successful provider request and never creates a generation or canonical identity.
pub struct PendingReferenceTypedHandoff {
    raw_object: VerifiedResearchObject,
    context: ReferenceObjectContext,
    http_receipt: ReferenceHttpReceipt,
    page_receipt: crate::ReferencePageReceipt,
    completion: ReferenceCompletionAuthority,
    wall_deadline: Timestamp,
    control: ReferenceFetchControl,
}

struct HandoffRawObjectControl<'a> {
    wall_deadline: Timestamp,
    control: &'a ReferenceFetchControl,
    completion: &'a ReferenceCompletionAuthority,
    worker_cancellation: &'a ReferenceCancellation,
}

impl ResearchObjectControl for HandoffRawObjectControl<'_> {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if self.worker_cancellation.is_cancelled() {
            return Err(ResearchObjectControlError::Cancelled);
        }
        ensure_handoff_current(self.wall_deadline, self.control, self.completion).map_err(|error| {
            match error {
                ReferenceTransportError::Cancelled => ResearchObjectControlError::Cancelled,
                ReferenceTransportError::DeadlineExceeded => {
                    ResearchObjectControlError::DeadlineExceeded
                }
                _ => ResearchObjectControlError::Unavailable,
            }
        })
    }
}

async fn reverify_handoff_raw_object(
    raw_object: VerifiedResearchObject,
    completion: ReferenceCompletionAuthority,
    wall_deadline: Timestamp,
    control: ReferenceFetchControl,
) -> Result<(ResearchObjectReceipt, ReferenceCompletionAuthority), ReferenceTransportError> {
    let worker_control = control.clone();
    let mut worker = ReapedBlockingWorker::spawn(move |worker_cancellation| {
        let verification_control = HandoffRawObjectControl {
            wall_deadline,
            control: &worker_control,
            completion: &completion,
            worker_cancellation: &worker_cancellation,
        };
        let raw_receipt = raw_object
            .reverify_for_commit(&verification_control)
            .map_err(map_logical_object_error)?;
        Ok((raw_receipt, completion))
    });
    loop {
        tokio::select! {
            result = &mut worker.result => {
                return worker.complete(result);
            }
            () = tokio::time::sleep(Duration::from_millis(25)) => {
                let current = control.ensure_open().and_then(|()| {
                    if trusted_timestamp()? > wall_deadline {
                        Err(ReferenceTransportError::DeadlineExceeded)
                    } else {
                        Ok(())
                    }
                });
                if let Err(error) = current {
                    worker.cancel();
                    let result = (&mut worker.result).await;
                    let _ = worker.complete(result);
                    return Err(error);
                }
            },
        }
    }
}

impl std::fmt::Debug for PendingReferenceTypedHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingReferenceTypedHandoff")
            .field("object_id", self.context.object_id())
            .field("records", &self.page_receipt.returned_records())
            .finish_non_exhaustive()
    }
}

impl PendingReferenceTypedHandoff {
    /// Re-verifies the same descriptor, completes the provider request, and consumes this
    /// process-local capability into a shared-raw/provider-typed handoff.
    ///
    /// # Errors
    ///
    /// Rejects cancellation, expired authority, raw evidence drift, or rate-budget completion
    /// failure. No product publication, PIT, restart, or currentness authority is created.
    pub async fn finish(self) -> Result<ReferenceTypedHandoff, ReferenceTransportError> {
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        let Self {
            raw_object,
            context,
            http_receipt,
            page_receipt,
            completion,
            wall_deadline,
            control,
        } = self;
        let (raw_receipt, completion) =
            reverify_handoff_raw_object(raw_object, completion, wall_deadline, control.clone())
                .await?;
        validate_common_raw_receipt(&raw_receipt, &context, &http_receipt)?;
        ensure_handoff_current(wall_deadline, &control, &completion)?;
        completion.record_success()?;
        Ok(ReferenceTypedHandoff {
            raw_receipt,
            context,
            http_receipt,
            page_receipt,
        })
    }
}

/// Completed raw/typed coordinates for caller-owned shared composition.
///
/// This object is intentionally noncloneable and non-serializable. It is not an immutable
/// generation, a catalog publication, a restart proof, or a canonical identity decision.
pub struct ReferenceTypedHandoff {
    raw_receipt: ResearchObjectReceipt,
    context: ReferenceObjectContext,
    http_receipt: ReferenceHttpReceipt,
    page_receipt: crate::ReferencePageReceipt,
}

impl std::fmt::Debug for ReferenceTypedHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceTypedHandoff")
            .field("object_id", self.context.object_id())
            .field("records", &self.page_receipt.returned_records())
            .finish_non_exhaustive()
    }
}

impl ReferenceTypedHandoff {
    /// Returns the common non-forgeable raw-object receipt.
    pub const fn raw_receipt(&self) -> &ResearchObjectReceipt {
        &self.raw_receipt
    }

    /// Returns exact provider object, schema, checksum, and clock evidence.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns the exact modified-response receipt.
    pub const fn http_receipt(&self) -> &ReferenceHttpReceipt {
        &self.http_receipt
    }

    /// Returns strict terminal parse completeness for these exact bytes.
    pub const fn page_receipt(&self) -> &crate::ReferencePageReceipt {
        &self.page_receipt
    }

    /// Returns the explicit requirement for shared freshness classification.
    pub const fn currentness(&self) -> OptionsReferenceCurrentnessDisposition {
        OptionsReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification
    }

    /// Consumes the handoff into shared raw authority and exact provider-native typed coordinates.
    pub fn into_parts(
        self,
    ) -> (
        ResearchObjectReceipt,
        ReferenceObjectContext,
        ReferenceHttpReceipt,
        crate::ReferencePageReceipt,
    ) {
        (
            self.raw_receipt,
            self.context,
            self.http_receipt,
            self.page_receipt,
        )
    }
}

/// One-use uninterpreted OCC memo handoff awaiting final raw re-verification.
pub struct PendingUninterpretedMemoHandoff {
    raw_object: VerifiedResearchObject,
    context: ReferenceObjectContext,
    http_receipt: ReferenceHttpReceipt,
    completion: ReferenceCompletionAuthority,
    wall_deadline: Timestamp,
    control: ReferenceFetchControl,
}

impl std::fmt::Debug for PendingUninterpretedMemoHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingUninterpretedMemoHandoff")
            .field("object_id", self.context.object_id())
            .finish_non_exhaustive()
    }
}

impl PendingUninterpretedMemoHandoff {
    /// Consumes the validated envelope into raw evidence without interpreting document economics.
    ///
    /// # Errors
    ///
    /// Rejects cancellation, expired authority, raw evidence drift, or budget completion failure.
    pub async fn finish(
        self,
    ) -> Result<ReferenceUninterpretedMemoHandoff, ReferenceTransportError> {
        ensure_handoff_current(self.wall_deadline, &self.control, &self.completion)?;
        let Self {
            raw_object,
            context,
            http_receipt,
            completion,
            wall_deadline,
            control,
        } = self;
        let (raw_receipt, completion) =
            reverify_handoff_raw_object(raw_object, completion, wall_deadline, control.clone())
                .await?;
        validate_common_raw_receipt(&raw_receipt, &context, &http_receipt)?;
        ensure_handoff_current(wall_deadline, &control, &completion)?;
        completion.record_success()?;
        Ok(ReferenceUninterpretedMemoHandoff {
            raw_receipt,
            context,
            http_receipt,
        })
    }
}

/// Raw OCC memo coordinates that remain explicitly uninterpreted.
pub struct ReferenceUninterpretedMemoHandoff {
    raw_receipt: ResearchObjectReceipt,
    context: ReferenceObjectContext,
    http_receipt: ReferenceHttpReceipt,
}

impl std::fmt::Debug for ReferenceUninterpretedMemoHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferenceUninterpretedMemoHandoff")
            .field("object_id", self.context.object_id())
            .finish_non_exhaustive()
    }
}

impl ReferenceUninterpretedMemoHandoff {
    /// Returns the common non-forgeable raw-object receipt.
    pub const fn raw_receipt(&self) -> &ResearchObjectReceipt {
        &self.raw_receipt
    }

    /// Returns exact provider object, schema, checksum, and clock evidence.
    pub const fn context(&self) -> &ReferenceObjectContext {
        &self.context
    }

    /// Returns the exact modified-response receipt.
    pub const fn http_receipt(&self) -> &ReferenceHttpReceipt {
        &self.http_receipt
    }

    /// Consumes the handoff into common raw authority and exact uninterpreted memo evidence.
    pub fn into_parts(
        self,
    ) -> (
        ResearchObjectReceipt,
        ReferenceObjectContext,
        ReferenceHttpReceipt,
    ) {
        (self.raw_receipt, self.context, self.http_receipt)
    }
}

/// Outcome of a production streaming official-source request.
#[derive(Debug)]
pub enum StreamingReferenceFetchOutcome {
    /// HTTP 200 streamed, fsynced, and content-addressed an object awaiting strict schema proof.
    Modified(StreamedReferenceObject),
    /// HTTP 304 authorized reuse of one exact prior sealed object.
    NotModified(ReferenceNotModifiedReceipt),
}

/// Hardened no-retry HTTPS client that streams each source object directly to durable raw capture.
#[derive(Clone, Debug)]
pub struct OfficialReferenceStreamingClient {
    client: reqwest::Client,
    provider: ReferenceProvider,
    metadata: SourceMetadata,
    maximum_response_bytes: u64,
    total_timeout: Duration,
}

impl OfficialReferenceStreamingClient {
    /// Builds the exact production transport: HTTPS only, Rustls TLS 1.2+, no proxy, redirects,
    /// retries, referer, compression, or hidden representation decoding.
    pub fn try_new(
        provider: ReferenceProvider,
        metadata: &SourceMetadata,
    ) -> Result<Self, ReferenceTransportError> {
        validate_reference_source_metadata(provider, metadata)?;
        let NetworkAccessPolicy::Allowlisted(endpoint) = metadata.network_policy() else {
            return Err(ReferenceTransportError::InvalidAuthorityMetadata);
        };
        if metadata.budget_policy().is_none()
            || endpoint.client_profile() != HttpClientProfile::hardened()
        {
            return Err(ReferenceTransportError::InvalidAuthorityMetadata);
        }
        let bounds = endpoint.request_bounds();
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
            .map_err(|_| ReferenceTransportError::ExecutorFailed)?;
        Ok(Self {
            client,
            provider,
            metadata: metadata.clone(),
            maximum_response_bytes: bounds.max_response_bytes(),
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    /// Returns the one closed provider namespace this client can acquire.
    pub const fn provider(&self) -> ReferenceProvider {
        self.provider
    }

    /// Fetches one exact request and streams a 200 body through a bounded channel to the
    /// shared logical raw-object store. The network task never owns the complete file in memory.
    pub async fn fetch_to_store(
        &self,
        store: &Arc<SealedResearchJournalStore>,
        authority: &ExtractionAuthority,
        request: &OfficialReferenceRequest,
        prior_raw_claim: Option<&ResearchObjectClaim>,
        control: &ReferenceFetchControl,
    ) -> Result<StreamingReferenceFetchOutcome, ReferenceTransportError> {
        if request.surface().provider() != self.provider {
            return Err(ReferenceTransportError::ProviderSurfaceMismatch);
        }
        authority.validate_current()?;
        if authority.metadata() != &self.metadata {
            return Err(ReferenceTransportError::InvalidAuthorityMetadata);
        }
        control.ensure_open()?;
        let permit = authority.try_network_request(request.locator.as_str())?;
        let request_bounds = permit.request_bounds()?;
        if permit.client_profile()? != HttpClientProfile::hardened()
            || request_bounds.max_response_bytes() != self.maximum_response_bytes
        {
            permit.release();
            return Err(ReferenceTransportError::InvalidAuthorityMetadata);
        }
        let in_flight = Arc::new(permit.authorize_send(request.locator.as_str())?);
        let request_timeout = admitted_remaining(request, control, self.total_timeout)?;
        let request_evidence = request.seal(
            SourceIdentifier::try_from(self.metadata.source_id().as_str())
                .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)?,
            self.metadata.provider().clone(),
            self.metadata
                .revision_evidence()
                .payload_evidence()
                .content_digest(),
            request_bounds.connect_timeout_nanos(),
            request_bounds.read_timeout_nanos(),
            request_bounds.total_timeout_nanos(),
            request_timeout,
        )?;
        let mut builder = self
            .client
            .get(request.locator.as_str())
            .header(reqwest::header::ACCEPT, request.accept.as_str())
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .header(
                reqwest::header::USER_AGENT,
                concat!(
                    "market-squawk/",
                    env!("CARGO_PKG_VERSION"),
                    " options-reference-adapter"
                ),
            )
            .timeout(request_timeout);
        let (if_none_match, if_modified_since) = conditional_headers(&request_evidence)?;
        if let Some(value) = if_none_match {
            builder = builder.header(reqwest::header::IF_NONE_MATCH, value.as_str());
        }
        if let Some(value) = if_modified_since {
            builder = builder.header(reqwest::header::IF_MODIFIED_SINCE, value.as_str());
        }
        let transport_started = Instant::now();
        let response = await_reqwest_with_control(builder.send(), control, &in_flight).await?;
        control.ensure_open()?;
        let received_headers_at = trusted_timestamp()?;
        let headers_transport_elapsed_nanos = elapsed_nanos(transport_started, request_timeout)?;
        if response.url().as_str() != request.locator.as_str() {
            return Err(ReferenceTransportError::InvalidRedirect);
        }
        if received_headers_at < request.wall_started_at
            || received_headers_at > request.wall_deadline
        {
            return Err(ReferenceTransportError::InvalidResponseClock);
        }
        let status = response.status().as_u16();
        let headers = response.headers();
        if matches!(status, 429 | 503) {
            let retry_after = bounded_raw_header(headers, &reqwest::header::RETRY_AFTER);
            let retry_deadline = Arc::try_unwrap(in_flight)
                .map_err(|_| ReferenceTransportError::InvalidProtocolState)?
                .apply_retry_after_header(retry_after.as_deref(), 0)?;
            return Err(ReferenceTransportError::Authority(
                ExtractionAuthorityError::BudgetWaitUntil {
                    deadline: retry_deadline,
                },
            ));
        }
        if status == 403
            && matches!(
                request.surface(),
                ReferenceSurface::OccMemoIndexCsv
                    | ReferenceSurface::OccMemoDocument { .. }
                    | ReferenceSurface::OccMemoAttachment { .. }
            )
            && exact_raw_header_value(
                headers,
                &reqwest::header::HeaderName::from_static("cf-mitigated"),
                b"challenge",
            )
        {
            let _retry_deadline = Arc::try_unwrap(in_flight)
                .map_err(|_| ReferenceTransportError::InvalidProtocolState)?
                .apply_retry_after_header(None, 0)?;
            return Err(ReferenceTransportError::ProviderAntiBotChallenge);
        }
        let content_type = retained_reqwest_header(headers, &reqwest::header::CONTENT_TYPE)?;
        let content_disposition =
            retained_reqwest_header(headers, &reqwest::header::CONTENT_DISPOSITION)?;
        let content_encoding =
            retained_reqwest_header(headers, &reqwest::header::CONTENT_ENCODING)?;
        let content_length = retained_content_length(headers)?;
        let cache = HttpCacheEvidence::new(
            retained_reqwest_header(headers, &reqwest::header::ETAG)?,
            retained_reqwest_header(headers, &reqwest::header::LAST_MODIFIED)?,
        );
        let final_locator = SourceIdentifier::try_from(response.url().as_str())
            .map_err(|_| ReferenceTransportError::InvalidRedirect)?;

        if status == 304 {
            let prior =
                prior_raw_claim.ok_or(ReferenceTransportError::InvalidConditionalEvidence)?;
            if content_length.is_some_and(|length| length != 0) || content_encoding.is_some() {
                return Err(ReferenceTransportError::InvalidNotModifiedResponse);
            }
            validate_conditional_prior(
                Arc::clone(store),
                request,
                prior,
                control,
                Arc::clone(&in_flight),
            )
            .await?;
            let receipt = admit_not_modified_evidence(
                request_evidence.clone(),
                final_locator,
                Vec::new(),
                content_type.as_ref().map(|value| value.as_str().to_owned()),
                content_disposition
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                content_encoding
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                content_length,
                received_headers_at,
                headers_transport_elapsed_nanos,
                cache,
            )?;
            ensure_operation_current(request, control, &in_flight)?;
            Arc::try_unwrap(in_flight)
                .map_err(|_| ReferenceTransportError::InvalidProtocolState)?
                .record_success()?;
            return Ok(StreamingReferenceFetchOutcome::NotModified(receipt));
        }
        if status != 200 {
            return Err(ReferenceTransportError::HttpStatus(status));
        }
        if content_encoding
            .as_ref()
            .is_some_and(|value| !value.as_str().eq_ignore_ascii_case("identity"))
        {
            return Err(ReferenceTransportError::UnexpectedContentEncoding);
        }
        if content_length.is_some_and(|bytes| {
            usize::try_from(bytes).map_or(true, |bytes| bytes > request.maximum_decoded_bytes)
        }) {
            return Err(ReferenceTransportError::ResponseTooLarge);
        }
        if let Some(declared) = content_length {
            in_flight.validate_response_size(declared)?;
        }
        let observed_content_type = content_type
            .clone()
            .ok_or(ReferenceTransportError::MissingContentType)?;
        let source_file = validate_source_file_evidence(request, content_disposition.as_ref())?;

        let maximum_raw_bytes = u64::try_from(request.maximum_decoded_bytes)
            .map_err(|_| ReferenceTransportError::ResponseTooLarge)?
            .min(self.maximum_response_bytes);
        let RawCaptureWorker {
            sender,
            mut readiness,
            mut worker,
        } = begin_stream_capture(
            Arc::clone(store),
            content_length,
            maximum_raw_bytes,
            control,
            request.wall_deadline,
            Arc::clone(&in_flight),
        )?;
        if let Err(error) =
            await_raw_capture_readiness(&mut readiness, request, control, &in_flight).await
        {
            drop(sender);
            await_cancelled_raw_capture_worker(&mut worker).await;
            return Err(error);
        }
        let mut prefix = Vec::new();
        prefix
            .try_reserve_exact(4_096)
            .map_err(|_| ReferenceTransportError::AllocationFailed)?;
        let mut decoder =
            decoder_without_header(&request.decoder_policy, observed_content_type.as_str())?;
        let mut stream = response.bytes_stream();
        let mut streamed_bytes = 0_u64;
        let stream_result: Result<(Timestamp, u64), ReferenceTransportError> = async {
            while let Some(chunk) =
                await_stream_chunk_with_control(&mut stream, control, &in_flight).await?
            {
                streamed_bytes = streamed_bytes
                    .checked_add(
                        u64::try_from(chunk.len())
                            .map_err(|_| ReferenceTransportError::ResponseTooLarge)?,
                    )
                    .ok_or(ReferenceTransportError::ResponseTooLarge)?;
                in_flight.validate_response_size(streamed_bytes)?;
                if streamed_bytes
                    > u64::try_from(request.maximum_decoded_bytes)
                        .map_err(|_| ReferenceTransportError::ResponseTooLarge)?
                {
                    return Err(ReferenceTransportError::ResponseTooLarge);
                }
                if decoder.is_none() {
                    retain_header_prefix(&mut prefix, &chunk)?;
                    if prefix.contains(&b'\n') {
                        decoder = Some(select_decoder(
                            &request.decoder_policy,
                            observed_content_type.as_str(),
                            &prefix,
                        )?);
                    }
                }
                send_raw_stream_frame_with_control(
                    &sender,
                    RawStreamFrame::Chunk(chunk),
                    request,
                    control,
                    &in_flight,
                )
                .await?;
            }
            let body_completed_at = trusted_timestamp()?;
            if body_completed_at < received_headers_at || body_completed_at > request.wall_deadline
            {
                return Err(ReferenceTransportError::InvalidResponseClock);
            }
            let transport_elapsed_nanos = elapsed_nanos(transport_started, request_timeout)?;
            if decoder.is_none() {
                decoder = Some(select_decoder(
                    &request.decoder_policy,
                    observed_content_type.as_str(),
                    &prefix,
                )?);
            }
            send_raw_stream_frame_with_control(
                &sender,
                RawStreamFrame::Complete,
                request,
                control,
                &in_flight,
            )
            .await?;
            Ok((body_completed_at, transport_elapsed_nanos))
        }
        .await;
        drop(sender);
        let (body_completed_at, transport_elapsed_nanos) = match stream_result {
            Ok(evidence) => evidence,
            Err(error) => {
                await_cancelled_raw_capture_worker(&mut worker).await;
                return Err(error);
            }
        };
        let capture = await_raw_capture_worker(worker, request, control, &in_flight).await?;
        ensure_operation_current(request, control, &in_flight)?;
        if content_length.is_some_and(|declared| declared != capture.size_bytes()) {
            return Err(ReferenceTransportError::ContentLengthMismatch);
        }
        let decoder = decoder.ok_or(ReferenceTransportError::UnrecognizedSchema)?;
        let object_id = object_identifier(&request.surface, capture.content_digest())?;
        let canonical_media_type = decoder.canonical_media_type(observed_content_type.as_str());
        let http_last_modified = cache
            .last_modified()
            .map(|value| HttpLastModifiedEvidence::try_from_header(value.as_str()))
            .transpose()
            .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let clocks = ObjectClockEvidence::try_new(
            source_file
                .publication_date
                .map(ResearchTemporalCoordinate::calendar_date),
            None,
            AvailabilityEvidence::local_first_observed(body_completed_at),
            body_completed_at,
            transport_elapsed_nanos,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let canonical_media_type = SourceIdentifier::try_from(canonical_media_type)
            .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let native_schema = decoder.native_schema_identity()?;
        let transport = ReferenceTransportEvidence::try_modified(
            request_evidence,
            status,
            final_locator.clone(),
            Vec::new(),
            observed_content_type.as_str(),
            content_disposition
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            content_encoding.map(|value| value.as_str().to_owned()),
            content_length,
            cache.etag().map(|value| value.as_str().to_owned()),
            cache.last_modified().map(|value| value.as_str().to_owned()),
            received_headers_at,
            body_completed_at,
            transport_elapsed_nanos,
            capture.content_digest(),
            capture.size_bytes(),
            canonical_media_type.clone(),
            native_schema.clone(),
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        let receipt = ReferenceHttpReceipt::try_modified(
            transport.clone(),
            observed_content_type,
            content_disposition,
            source_file.filename.clone(),
            source_file.publication_date,
            cache,
        )?;
        let context = ReferenceObjectContext::try_new(
            request.surface.provider(),
            request.surface.clone(),
            object_id,
            request.locator.clone(),
            final_locator.clone(),
            canonical_media_type,
            capture.content_digest(),
            capture.size_bytes(),
            native_schema,
            clocks,
            source_file.filename,
            source_file.publication_date,
            http_last_modified,
            transport,
        )
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
        ensure_operation_current(request, control, &in_flight)?;
        Ok(StreamingReferenceFetchOutcome::Modified(
            StreamedReferenceObject {
                raw_object: capture,
                context,
                http_receipt: receipt,
                decoder,
                maximum_decoded_bytes: u64::try_from(request.maximum_decoded_bytes)
                    .map_err(|_| ReferenceTransportError::ResponseTooLarge)?,
                completion: ReferenceCompletionAuthority::Production(in_flight),
                wall_deadline: request.wall_deadline,
                control: control.clone(),
            },
        ))
    }
}

pub(crate) const fn reference_source_id(provider: ReferenceProvider) -> &'static str {
    match provider {
        ReferenceProvider::Occ => OCC_OPTIONS_REFERENCE_SOURCE_ID,
        ReferenceProvider::Cboe => CBOE_OPTIONS_REFERENCE_SOURCE_ID,
    }
}

pub(crate) const fn reference_budget_provider_id(provider: ReferenceProvider) -> &'static str {
    match provider {
        ReferenceProvider::Occ => OCC_OPTIONS_REFERENCE_PROVIDER_ID,
        ReferenceProvider::Cboe => CBOE_OPTIONS_REFERENCE_PROVIDER_ID,
    }
}

/// Builds the conservative application budget for one independent OCC or Cboe public queue.
///
/// The selected official pages do not publish a numeric automated-request ceiling. This policy
/// therefore admits one request per sliding minute and one in-flight request until separately
/// reviewed evidence justifies a replacement. Provider `Retry-After` evidence and refusals still
/// extend the shared durable backoff state.
///
/// # Errors
///
/// Returns a closed configuration failure if the provider scope or budget cannot be represented.
pub fn options_reference_application_budget_policy(
    provider: ReferenceProvider,
    backoff: BackoffPolicy,
) -> Result<ProviderBudgetPolicy, ReferenceTransportError> {
    let scope = BudgetScope::new(
        SourceIdentifier::try_from(reference_budget_provider_id(provider))
            .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)?,
    );
    let window = ProviderBudgetWindow::try_new(
        NonZeroU32::new(OPTIONS_REFERENCE_APPLICATION_REQUESTS_PER_MINUTE)
            .ok_or(ReferenceTransportError::InvalidAuthorityMetadata)?,
        NonZeroU64::new(OPTIONS_REFERENCE_APPLICATION_WINDOW_NANOS)
            .ok_or(ReferenceTransportError::InvalidAuthorityMetadata)?,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)?;
    ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &[window],
        NonZeroU16::new(OPTIONS_REFERENCE_APPLICATION_MAX_CONCURRENT)
            .ok_or(ReferenceTransportError::InvalidAuthorityMetadata)?,
        backoff,
    )
    .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

/// Builds the product-wide public rate declaration for the exact selected provider endpoints.
///
/// Root composition registers this declaration once with the shared durable rate authority and
/// binds every provider doctor and background acquisition to the resulting allocation.
///
/// # Errors
///
/// Rejects invalid request bounds, policy construction, or canonical endpoint authority.
pub fn options_reference_provider_rate_declaration(
    provider: ReferenceProvider,
    bounds: HttpRequestBounds,
    backoff: BackoffPolicy,
) -> Result<ProviderRateDeclaration, ReferenceTransportError> {
    let policy = options_reference_application_budget_policy(provider, backoff)?;
    let endpoints = options_reference_endpoint_policy(provider, bounds)?;
    ProviderRateDeclaration::try_for_endpoint(policy, &endpoints)
        .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

/// Builds the exact code-owned endpoint allowlist for one selected option-reference provider.
///
/// Root composition uses this same contract for `SourceMetadata` and the durable rate declaration,
/// preventing a duplicated or broader application allowlist from drifting away from acquisition.
///
/// # Errors
///
/// Rejects request bounds or an endpoint/query rule that cannot be represented exactly.
pub fn options_reference_endpoint_policy(
    provider: ReferenceProvider,
    bounds: HttpRequestBounds,
) -> Result<EndpointPolicy, ReferenceTransportError> {
    let policy = match provider {
        ReferenceProvider::Cboe => EndpointPolicy::try_new_with_bounds(
            [
                CboeVenue::C1.all_series_locator(),
                CboeVenue::Bzx.all_series_locator(),
                CboeVenue::C2.all_series_locator(),
                CboeVenue::Edgx.all_series_locator(),
            ],
            bounds,
        ),
        ReferenceProvider::Occ => EndpointPolicy::try_new_combined(
            [OCC_MEMO_EXPORT_LOCATOR],
            vec![
                ApiEndpointRule::try_new(
                    OCC_DLP_SELECTED_BASE,
                    PathScope::Exact,
                    vec![
                        exact_public_query_rule("prodType", "ALL")?,
                        exact_public_query_rule("downloadFields", "OS;US;SN;EXCH;PL;ONN")?,
                        exact_public_query_rule("format", "txt")?,
                    ],
                    3,
                    128,
                )
                .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata),
                ApiEndpointRule::try_new(
                    OCC_DLP_DAILY_BASE,
                    PathScope::Exact,
                    vec![
                        bounded_public_query_rule("reportDate", 8)?,
                        exact_public_query_rule("format", "txt")?,
                    ],
                    2,
                    64,
                )
                .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata),
                ApiEndpointRule::try_new(
                    OCC_DLP_DAILY_BASE,
                    PathScope::Exact,
                    vec![
                        bounded_public_query_rule("reportDate", 8)?,
                        exact_public_query_rule("format", "xml")?,
                    ],
                    2,
                    64,
                )
                .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata),
                ApiEndpointRule::try_new(
                    OCC_MEMO_DOCUMENT_BASE,
                    PathScope::Exact,
                    vec![bounded_public_query_rule("number", 20)?],
                    1,
                    32,
                )
                .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata),
            ]
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?,
            bounds,
        ),
    };
    policy.map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

fn exact_public_query_rule(
    key: &'static str,
    value: &'static str,
) -> Result<QueryParameterRule, ReferenceTransportError> {
    QueryParameterRule::try_new_exact_public(source_identifier(key)?, source_identifier(value)?)
        .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

fn bounded_public_query_rule(
    key: &'static str,
    maximum_bytes: u16,
) -> Result<QueryParameterRule, ReferenceTransportError> {
    QueryParameterRule::try_new(
        source_identifier(key)?,
        maximum_bytes,
        false,
        QuerySensitivity::Public,
    )
    .map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

fn source_identifier(value: &'static str) -> Result<SourceIdentifier, ReferenceTransportError> {
    SourceIdentifier::try_from(value).map_err(|_| ReferenceTransportError::InvalidAuthorityMetadata)
}

pub(crate) fn native_schema_identity(
    name: &str,
) -> Result<ReferenceNativeSchemaIdentity, ReferenceTransportError> {
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"market-squawk:options-reference-native-schema-fingerprint:v1\0");
    fingerprint.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
    fingerprint.update(name.as_bytes());
    ReferenceNativeSchemaIdentity::try_new(
        SourceIdentifier::try_from(name)
            .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?,
        NonZeroU32::MIN,
        EvidenceDigest::new(DigestAlgorithm::Sha256, fingerprint.finalize().into()),
    )
    .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)
}

#[cfg(test)]
pub(crate) fn injected_source_contract_digest(provider: ReferenceProvider) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk:options-reference-injected-source-contract:v1\0");
    digest.update(reference_source_id(provider).as_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn conditional_headers(
    request: &ReferenceOfficialRequestEvidence,
) -> Result<(Option<ReferenceHeaderValue>, Option<ReferenceHeaderValue>), ReferenceTransportError> {
    let Some(prior) = request.conditional_prior() else {
        return Ok((None, None));
    };
    match prior.validator() {
        ReferenceConditionalValidatorEvidence::EntityTag(value) => {
            Ok((Some(ReferenceHeaderValue::try_new(value.clone())?), None))
        }
        ReferenceConditionalValidatorEvidence::LastModified(value) => {
            Ok((None, Some(ReferenceHeaderValue::try_new(value.clone())?)))
        }
    }
}

fn validate_reference_source_metadata(
    provider: ReferenceProvider,
    metadata: &SourceMetadata,
) -> Result<(), ReferenceTransportError> {
    let NetworkAccessPolicy::Allowlisted(endpoint) = metadata.network_policy() else {
        return Err(ReferenceTransportError::InvalidAuthorityMetadata);
    };
    let bounds = endpoint.request_bounds();
    let budget = metadata
        .budget_policy()
        .ok_or(ReferenceTransportError::InvalidAuthorityMetadata)?;
    let (history, delivery, minimum_response_bytes, topology_valid) = match provider {
        ReferenceProvider::Occ => (
            HistoricalCapability::Historical,
            DeliveryEvidence::Unknown,
            OCC_MINIMUM_RESPONSE_BYTES,
            metadata.coverage().topology().is_not_applicable(),
        ),
        ReferenceProvider::Cboe => (
            HistoricalCapability::None,
            DeliveryEvidence::DirectVenue,
            CBOE_MINIMUM_RESPONSE_BYTES,
            exact_cboe_topology(metadata),
        ),
    };
    let metadata_valid = metadata.source_id().as_str() == reference_source_id(provider)
        && metadata.provider().as_str() == reference_budget_provider_id(provider)
        && metadata.source_class() == SourceClass::Exchange
        && metadata.authorization().mode() == AuthorizationMode::PublicInterface
        && metadata.coverage().domain() == CoverageDomain::Instruments
        && metadata.coverage().asset_classes() == [AssetClass::Option]
        && metadata.coverage().instruments().instruments().is_empty()
        && metadata.coverage().live().is_none()
        && matches!(metadata.coverage().delay(), CoverageDelay::Delayed(_))
        && metadata.coverage().delivery() == delivery
        && topology_valid
        && metadata.quality_ceiling() == DataQuality::OfficialDelayed
        && !metadata.capabilities().live()
        && metadata.capabilities().extraction()
        && metadata.capabilities().historical() == history
        && !metadata.capabilities().source_timestamps()
        && matches!(metadata.protocol_profile(), SourceProtocolProfile::NotLive)
        && budget.scope().as_source_identifier() == metadata.provider()
        && budget.scope().authorization_account().is_none()
        && matches_options_reference_application_budget(budget)
        && bounds.max_redirects() == 0
        && bounds.connect_timeout_nanos() >= OPTIONS_REFERENCE_MINIMUM_CONNECT_TIMEOUT_NANOS
        && bounds.read_timeout_nanos() >= OPTIONS_REFERENCE_MINIMUM_READ_TIMEOUT_NANOS
        && bounds.total_timeout_nanos() >= OPTIONS_REFERENCE_MINIMUM_TOTAL_TIMEOUT_NANOS
        && bounds.total_timeout_nanos()
            <= u64::try_from(MAX_OPERATION_DURATION.as_nanos()).unwrap_or(u64::MAX)
        && bounds.max_response_bytes() >= minimum_response_bytes
        && endpoint == &options_reference_endpoint_policy(provider, bounds)?;
    if metadata_valid {
        Ok(())
    } else {
        Err(ReferenceTransportError::InvalidAuthorityMetadata)
    }
}

fn matches_options_reference_application_budget(policy: &ProviderBudgetPolicy) -> bool {
    policy.max_concurrent() == OPTIONS_REFERENCE_APPLICATION_MAX_CONCURRENT
        && policy.backoff().maximum_nanos() >= OPTIONS_REFERENCE_APPLICATION_WINDOW_NANOS
        && (0..policy.window_count()).any(|index| {
            policy.window(index).is_some_and(|window| {
                window.requests_per_window() == OPTIONS_REFERENCE_APPLICATION_REQUESTS_PER_MINUTE
                    && window.window_nanos() >= OPTIONS_REFERENCE_APPLICATION_WINDOW_NANOS
                    && window.semantics() == BudgetWindowSemantics::Sliding
            })
        })
}

fn exact_cboe_topology(metadata: &SourceMetadata) -> bool {
    let topology = metadata.coverage().topology();
    let venues = topology.venues();
    topology.is_partial()
        && venues.len() == 4
        && [
            CboeVenue::C1,
            CboeVenue::Bzx,
            CboeVenue::C2,
            CboeVenue::Edgx,
        ]
        .into_iter()
        .all(|expected| {
            venues
                .iter()
                .any(|venue| venue.as_str() == expected.stable_label())
        })
}

fn validate_uninterpreted_document_envelope(
    context: &ReferenceObjectContext,
    prefix: &[u8],
    tail: &[u8],
) -> Result<(), ReferenceTransportError> {
    let valid = match context.media_type().as_str() {
        "application/pdf" => {
            prefix.starts_with(b"%PDF-")
                && tail
                    .windows(b"%%EOF".len())
                    .any(|window| window == b"%%EOF")
        }
        "text/html" => {
            let prefix = prefix.strip_prefix(b"\xef\xbb\xbf").unwrap_or(prefix);
            let first = prefix
                .iter()
                .position(|byte| !byte.is_ascii_whitespace())
                .unwrap_or(prefix.len());
            prefix.get(first..).is_some_and(|document| {
                document
                    .get(..b"<!doctype html".len())
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"<!doctype html"))
                    || document
                        .get(..b"<html".len())
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"<html"))
            })
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ReferenceTransportError::SchemaValidationFailed)
    }
}

fn retain_rolling_tail(tail: &mut Vec<u8>, bytes: &[u8], maximum: usize) {
    if bytes.len() >= maximum {
        tail.clear();
        tail.extend_from_slice(&bytes[bytes.len() - maximum..]);
        return;
    }
    let overflow = tail
        .len()
        .saturating_add(bytes.len())
        .saturating_sub(maximum);
    if overflow > 0 {
        tail.drain(..overflow);
    }
    tail.extend_from_slice(bytes);
}

fn decoder_without_header(
    policy: &DecoderPolicy,
    observed_content_type: &str,
) -> Result<Option<SelectedReferenceDecoder>, ReferenceTransportError> {
    match policy {
        DecoderPolicy::CboeAllSeries { .. } | DecoderPolicy::OccMemoCsv { .. } => Ok(None),
        DecoderPolicy::OccDlp { .. } | DecoderPolicy::OccMemoDocumentUninterpreted => {
            select_decoder(policy, observed_content_type, &[]).map(Some)
        }
    }
}

fn retain_header_prefix(prefix: &mut Vec<u8>, chunk: &[u8]) -> Result<(), ReferenceTransportError> {
    if prefix.contains(&b'\n') {
        return Ok(());
    }
    let newline = chunk
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(chunk.len(), |index| index.saturating_add(1));
    let remaining = 4_096_usize.saturating_sub(prefix.len());
    if newline > remaining {
        return Err(ReferenceTransportError::UnrecognizedSchema);
    }
    prefix.extend_from_slice(&chunk[..newline]);
    Ok(())
}

async fn await_reqwest_with_control(
    future: impl std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<reqwest::Response, ReferenceTransportError> {
    tokio::pin!(future);
    loop {
        tokio::select! {
            response = &mut future => {
                return response.map_err(|_| ReferenceTransportError::ExecutorFailed);
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                control.ensure_open()?;
                in_flight.validate_current()?;
            },
        }
    }
}

async fn await_stream_chunk_with_control<S>(
    stream: &mut S,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<Option<bytes::Bytes>, ReferenceTransportError>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    loop {
        tokio::select! {
            chunk = stream.next() => {
                return chunk.transpose().map_err(|_| ReferenceTransportError::ExecutorFailed);
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {
                control.ensure_open()?;
                in_flight.validate_current()?;
            },
        }
    }
}

async fn send_raw_stream_frame_with_control(
    sender: &tokio::sync::mpsc::Sender<RawStreamFrame>,
    frame: RawStreamFrame,
    request: &OfficialReferenceRequest,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<(), ReferenceTransportError> {
    ensure_operation_current(request, control, in_flight)?;
    let permit = loop {
        tokio::select! {
            permit = sender.reserve() => {
                break permit.map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
            }
            () = tokio::time::sleep(Duration::from_millis(25)) => {
                ensure_operation_current(request, control, in_flight)?;
            },
        }
    };
    ensure_operation_current(request, control, in_flight)?;
    permit.send(frame);
    Ok(())
}

enum RawStreamFrame {
    Chunk(bytes::Bytes),
    Complete,
}

enum SourceRawObjectAuthority {
    Production(Arc<InFlightExtractionRequest>),
    #[cfg(test)]
    Fixture,
}

struct SourceRawObjectControl {
    control: ReferenceFetchControl,
    worker_cancellation: ReferenceCancellation,
    wall_deadline: Timestamp,
    authority: SourceRawObjectAuthority,
}

impl SourceRawObjectControl {
    fn ensure_current(&self) -> Result<(), ResearchObjectControlError> {
        ensure_raw_stream_worker_open(&self.control, &self.worker_cancellation)?;
        match &self.authority {
            SourceRawObjectAuthority::Production(in_flight) => in_flight
                .validate_current()
                .map_err(|_| ResearchObjectControlError::Unavailable)?,
            #[cfg(test)]
            SourceRawObjectAuthority::Fixture => {}
        }
        match trusted_timestamp() {
            Ok(now) if now <= self.wall_deadline => Ok(()),
            Ok(_) => Err(ResearchObjectControlError::DeadlineExceeded),
            Err(_) => Err(ResearchObjectControlError::Unavailable),
        }
    }
}

impl ResearchObjectControl for SourceRawObjectControl {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        self.ensure_current()
    }
}

enum RawCaptureReadiness {
    Ready,
    Failed,
}

struct RawCaptureWorker {
    sender: tokio::sync::mpsc::Sender<RawStreamFrame>,
    readiness: tokio::sync::oneshot::Receiver<RawCaptureReadiness>,
    worker: ReapedBlockingWorker<VerifiedResearchObject>,
}

fn begin_stream_capture(
    store: Arc<SealedResearchJournalStore>,
    expected_bytes: Option<u64>,
    maximum_bytes: u64,
    control: &ReferenceFetchControl,
    wall_deadline: Timestamp,
    in_flight: Arc<InFlightExtractionRequest>,
) -> Result<RawCaptureWorker, ReferenceTransportError> {
    if maximum_bytes == 0 || expected_bytes.is_some_and(|bytes| bytes > maximum_bytes) {
        return Err(ReferenceTransportError::InvalidObjectEvidence);
    }
    let admission = logical_object_admission(maximum_bytes)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(RAW_STREAM_CHANNEL_CAPACITY);
    let (readiness_sender, readiness) = tokio::sync::oneshot::channel();
    let worker_control = control.clone();
    let worker = ReapedBlockingWorker::spawn(move |worker_cancellation| {
        persist_raw_stream(
            store,
            admission,
            receiver,
            readiness_sender,
            expected_bytes,
            maximum_bytes,
            worker_control,
            worker_cancellation,
            wall_deadline,
            in_flight,
        )
    });
    Ok(RawCaptureWorker {
        sender,
        readiness,
        worker,
    })
}

fn logical_object_admission(
    maximum_bytes: u64,
) -> Result<ResearchObjectAdmission, ReferenceTransportError> {
    let chunks = maximum_bytes.div_ceil(LOGICAL_OBJECT_INTEGRITY_CHUNK_BYTES);
    let chunks = usize::try_from(chunks).map_err(|_| ReferenceTransportError::ResponseTooLarge)?;
    ResearchObjectAdmission::try_new(maximum_bytes, chunks)
        .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)
}

fn persist_raw_stream(
    store: Arc<SealedResearchJournalStore>,
    admission: ResearchObjectAdmission,
    mut receiver: tokio::sync::mpsc::Receiver<RawStreamFrame>,
    readiness: tokio::sync::oneshot::Sender<RawCaptureReadiness>,
    expected_bytes: Option<u64>,
    maximum_bytes: u64,
    control: ReferenceFetchControl,
    worker_cancellation: ReferenceCancellation,
    wall_deadline: Timestamp,
    in_flight: Arc<InFlightExtractionRequest>,
) -> Result<VerifiedResearchObject, ReferenceTransportError> {
    let mut pending = match store.begin_logical_object(admission) {
        Ok(pending) => {
            readiness
                .send(RawCaptureReadiness::Ready)
                .map_err(|_| ReferenceTransportError::Cancelled)?;
            pending
        }
        Err(_) => {
            let _ = readiness.send(RawCaptureReadiness::Failed);
            return Err(ReferenceTransportError::RawCaptureFailed);
        }
    };
    let finish_control = SourceRawObjectControl {
        control,
        worker_cancellation,
        wall_deadline,
        authority: SourceRawObjectAuthority::Production(in_flight),
    };
    let mut observed = 0_u64;
    let mut complete = false;
    while let Some(frame) = receiver.blocking_recv() {
        finish_control
            .ensure_current()
            .map_err(map_raw_control_error)?;
        match frame {
            RawStreamFrame::Chunk(chunk) if !complete => {
                observed = observed
                    .checked_add(
                        u64::try_from(chunk.len())
                            .map_err(|_| ReferenceTransportError::ResponseTooLarge)?,
                    )
                    .ok_or(ReferenceTransportError::ResponseTooLarge)?;
                if observed > maximum_bytes
                    || expected_bytes.is_some_and(|expected| observed > expected)
                {
                    return Err(ReferenceTransportError::ContentLengthMismatch);
                }
                for part in chunk.chunks(RAW_STREAM_WRITE_CHUNK_BYTES) {
                    finish_control
                        .ensure_current()
                        .map_err(map_raw_control_error)?;
                    pending
                        .write_all(part)
                        .map_err(|_| ReferenceTransportError::RawCaptureFailed)?;
                }
            }
            RawStreamFrame::Complete if !complete => complete = true,
            RawStreamFrame::Chunk(_) | RawStreamFrame::Complete => {
                return Err(ReferenceTransportError::InvalidProtocolState);
            }
        }
    }
    if !complete || observed == 0 || expected_bytes.is_some_and(|expected| expected != observed) {
        return Err(ReferenceTransportError::ContentLengthMismatch);
    }
    finish_control
        .ensure_current()
        .map_err(map_raw_control_error)?;
    let raw_object = store
        .finish_logical_object(pending, &finish_control)
        .map_err(map_logical_object_error)?;
    if raw_object.size_bytes() != observed {
        return Err(ReferenceTransportError::InvalidObjectEvidence);
    }
    Ok(raw_object)
}

fn ensure_raw_stream_worker_open(
    control: &ReferenceFetchControl,
    worker_cancellation: &ReferenceCancellation,
) -> Result<(), ResearchObjectControlError> {
    if worker_cancellation.is_cancelled() {
        return Err(ResearchObjectControlError::Cancelled);
    }
    match control.ensure_open() {
        Ok(()) => Ok(()),
        Err(ReferenceTransportError::Cancelled) => Err(ResearchObjectControlError::Cancelled),
        Err(ReferenceTransportError::DeadlineExceeded) => {
            Err(ResearchObjectControlError::DeadlineExceeded)
        }
        Err(_) => Err(ResearchObjectControlError::Unavailable),
    }
}

fn map_raw_control_error(error: ResearchObjectControlError) -> ReferenceTransportError {
    match error {
        ResearchObjectControlError::Cancelled => ReferenceTransportError::Cancelled,
        ResearchObjectControlError::DeadlineExceeded => ReferenceTransportError::DeadlineExceeded,
        ResearchObjectControlError::Unavailable => ReferenceTransportError::RawCaptureFailed,
    }
}

fn map_logical_object_error(error: SealedResearchJournalStoreError) -> ReferenceTransportError {
    match error {
        SealedResearchJournalStoreError::ObjectControl(control) => map_raw_control_error(control),
        _ => ReferenceTransportError::RawCaptureFailed,
    }
}

async fn await_raw_capture_readiness(
    readiness: &mut tokio::sync::oneshot::Receiver<RawCaptureReadiness>,
    request: &OfficialReferenceRequest,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<(), ReferenceTransportError> {
    loop {
        tokio::select! {
            state = &mut *readiness => {
                return match state {
                    Ok(RawCaptureReadiness::Ready) => Ok(()),
                    Ok(RawCaptureReadiness::Failed) | Err(_) => {
                        Err(ReferenceTransportError::RawCaptureFailed)
                    }
                };
            }
            () = tokio::time::sleep(Duration::from_millis(25)) => {
                ensure_operation_current(request, control, in_flight)?;
            },
        }
    }
}

async fn await_raw_capture_worker(
    mut worker: ReapedBlockingWorker<VerifiedResearchObject>,
    request: &OfficialReferenceRequest,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<VerifiedResearchObject, ReferenceTransportError> {
    loop {
        tokio::select! {
            result = &mut worker.result => {
                return worker.complete(result);
            }
            () = tokio::time::sleep(Duration::from_millis(25)) => {
                if let Err(error) = ensure_operation_current(request, control, in_flight) {
                    await_cancelled_raw_capture_worker(&mut worker).await;
                    return Err(error);
                }
            },
        }
    }
}

async fn await_cancelled_raw_capture_worker(
    worker: &mut ReapedBlockingWorker<VerifiedResearchObject>,
) {
    // `spawn_blocking` cannot be aborted after it starts. Joining is therefore part of the
    // operation's terminal boundary: the worker observes cooperative cancellation between
    // bounded writes before the API returns. A successful late receipt is discarded here; the
    // shared logical-object authority owns any staging recovery.
    worker.cancel();
    let result = (&mut worker.result).await;
    let _ = worker.complete(result);
}

fn ensure_operation_current(
    request: &OfficialReferenceRequest,
    control: &ReferenceFetchControl,
    in_flight: &InFlightExtractionRequest,
) -> Result<(), ReferenceTransportError> {
    control.ensure_open()?;
    in_flight.validate_current()?;
    if trusted_timestamp()? > request.wall_deadline {
        Err(ReferenceTransportError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn ensure_handoff_current(
    wall_deadline: Timestamp,
    control: &ReferenceFetchControl,
    completion: &ReferenceCompletionAuthority,
) -> Result<(), ReferenceTransportError> {
    control.ensure_open()?;
    completion.validate_current()?;
    if trusted_timestamp()? > wall_deadline {
        Err(ReferenceTransportError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn validate_common_raw_receipt(
    raw: &ResearchObjectReceipt,
    context: &ReferenceObjectContext,
    http: &ReferenceHttpReceipt,
) -> Result<(), ReferenceTransportError> {
    if raw.content_digest() != context.payload_digest()
        || raw.size_bytes() != context.payload_bytes()
        || http.payload_digest() != context.payload_digest()
        || http.payload_bytes() != context.payload_bytes()
        || http.configured_locator() != context.configured_locator()
        || http.final_locator() != context.final_locator()
        || http.transport_evidence() != context.transport_evidence()
        || !http.body_complete()
    {
        return Err(ReferenceTransportError::InvalidObjectEvidence);
    }
    Ok(())
}

fn retained_reqwest_header(
    headers: &reqwest::header::HeaderMap,
    name: &reqwest::header::HeaderName,
) -> Result<Option<ReferenceHeaderValue>, ReferenceTransportError> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(|value| {
            value
                .to_str()
                .map_err(|_| ReferenceTransportError::InvalidHeaderValue)
                .and_then(ReferenceHeaderValue::try_new)
        })
        .transpose()?;
    if values.next().is_some() {
        return Err(ReferenceTransportError::InvalidHeaderValue);
    }
    Ok(value)
}

fn bounded_raw_header(
    headers: &reqwest::header::HeaderMap,
    name: &reqwest::header::HeaderName,
) -> Option<Vec<u8>> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    if values.next().is_some() || value.as_bytes().len() > MAX_HEADER_VALUE_BYTES {
        None
    } else {
        Some(value.as_bytes().to_vec())
    }
}

fn exact_raw_header_value(
    headers: &reqwest::header::HeaderMap,
    name: &reqwest::header::HeaderName,
    expected: &[u8],
) -> bool {
    let mut values = headers.get_all(name).iter();
    values
        .next()
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(expected))
        && values.next().is_none()
}

fn retained_content_length(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<u64>, ReferenceTransportError> {
    retained_reqwest_header(headers, &reqwest::header::CONTENT_LENGTH)?
        .map(|value| {
            if value.as_str().bytes().all(|byte| byte.is_ascii_digit()) {
                value
                    .as_str()
                    .parse::<u64>()
                    .map_err(|_| ReferenceTransportError::ContentLengthMismatch)
            } else {
                Err(ReferenceTransportError::ContentLengthMismatch)
            }
        })
        .transpose()
}

pub(crate) fn trusted_timestamp() -> Result<Timestamp, ReferenceTransportError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReferenceTransportError::TrustedTimeUnavailable)?;
    let nanos = i64::try_from(duration.as_nanos())
        .map_err(|_| ReferenceTransportError::TrustedTimeUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn admitted_remaining(
    request: &OfficialReferenceRequest,
    control: &ReferenceFetchControl,
    configured: Duration,
) -> Result<Duration, ReferenceTransportError> {
    let now = trusted_timestamp()?;
    if now < request.wall_started_at {
        return Err(ReferenceTransportError::InvalidResponseClock);
    }
    let wall_remaining = request
        .wall_deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(Duration::from_nanos)
        .ok_or(ReferenceTransportError::DeadlineExceeded)?;
    Ok(configured.min(control.remaining()?).min(wall_remaining))
}

fn elapsed_nanos(started: Instant, maximum: Duration) -> Result<u64, ReferenceTransportError> {
    let elapsed = u64::try_from(started.elapsed().as_nanos())
        .ok()
        .filter(|elapsed| *elapsed > 0)
        .ok_or(ReferenceTransportError::InvalidResponseClock)?;
    if u128::from(elapsed) > maximum.as_nanos() {
        return Err(ReferenceTransportError::DeadlineExceeded);
    }
    Ok(elapsed)
}

async fn validate_conditional_prior(
    store: Arc<SealedResearchJournalStore>,
    request: &OfficialReferenceRequest,
    prior: &ResearchObjectClaim,
    control: &ReferenceFetchControl,
    in_flight: Arc<InFlightExtractionRequest>,
) -> Result<(), ReferenceTransportError> {
    let conditional = request
        .conditional
        .as_ref()
        .cloned()
        .ok_or(ReferenceTransportError::InvalidConditionalEvidence)?;
    let configured_locator = request.locator().clone();
    let prior = prior.clone();
    let wall_deadline = request.wall_deadline;
    let worker_control = control.clone();
    let worker_in_flight = Arc::clone(&in_flight);
    let mut worker = ReapedBlockingWorker::spawn(move |worker_cancellation| {
        let verification_control = SourceRawObjectControl {
            control: worker_control,
            worker_cancellation,
            wall_deadline,
            authority: SourceRawObjectAuthority::Production(worker_in_flight),
        };
        let verified = store
            .open_verified_logical_object_claim(&prior, &verification_control)
            .map_err(|error| match error {
                SealedResearchJournalStoreError::ObjectControl(control) => {
                    map_raw_control_error(control)
                }
                _ => ReferenceTransportError::InvalidConditionalEvidence,
            })?;
        if conditional.configured_locator != configured_locator
            || conditional.prior_payload_digest != verified.content_digest()
            || conditional.prior_payload_bytes != verified.size_bytes()
            || conditional.prior_payload_digest != prior.content_digest()
            || conditional.prior_payload_bytes != prior.size_bytes()
        {
            return Err(ReferenceTransportError::InvalidConditionalEvidence);
        }
        Ok(())
    });
    loop {
        tokio::select! {
            result = &mut worker.result => {
                return worker.complete(result);
            }
            () = tokio::time::sleep(Duration::from_millis(25)) => {
                if let Err(error) = ensure_operation_current(request, control, &in_flight) {
                    worker.cancel();
                    let result = (&mut worker.result).await;
                    let _ = worker.complete(result);
                    return Err(error);
                }
            },
        }
    }
}

#[cfg(test)]
fn admit_not_modified(
    request: ReferenceOfficialRequestEvidence,
    response: ReferenceHttpResponse,
    transport_elapsed_nanos: u64,
) -> Result<ReferenceFetchOutcome, ReferenceTransportError> {
    if !response.body_complete
        || !response.body.is_empty()
        || response.content_length.is_some_and(|length| length != 0)
        || response.content_encoding.is_some()
    {
        return Err(ReferenceTransportError::InvalidNotModifiedResponse);
    }
    let receipt = admit_not_modified_evidence(
        request,
        response.final_locator,
        response.redirect_chain,
        response.content_type.map(|value| value.as_str().to_owned()),
        response
            .content_disposition
            .map(|value| value.as_str().to_owned()),
        response
            .content_encoding
            .map(|value| value.as_str().to_owned()),
        response.content_length,
        response.received_at,
        transport_elapsed_nanos,
        response.cache,
    )?;
    Ok(ReferenceFetchOutcome::NotModified(receipt))
}

fn admit_not_modified_evidence(
    request: ReferenceOfficialRequestEvidence,
    final_locator: SourceIdentifier,
    redirect_chain: Vec<SourceIdentifier>,
    observed_content_type: Option<String>,
    observed_content_disposition: Option<String>,
    observed_content_encoding: Option<String>,
    declared_content_length: Option<u64>,
    received_at: Timestamp,
    transport_elapsed_nanos: u64,
    response_cache: HttpCacheEvidence,
) -> Result<ReferenceNotModifiedReceipt, ReferenceTransportError> {
    let conditional = request
        .conditional_prior()
        .cloned()
        .ok_or(ReferenceTransportError::UnexpectedNotModified)?;
    let transport = ReferenceTransportEvidence::try_not_modified(
        request,
        304,
        final_locator.clone(),
        redirect_chain.clone(),
        observed_content_type,
        observed_content_disposition,
        observed_content_encoding,
        declared_content_length,
        response_cache.etag().map(|value| value.as_str().to_owned()),
        response_cache
            .last_modified()
            .map(|value| value.as_str().to_owned()),
        received_at,
        received_at,
        transport_elapsed_nanos,
    )
    .map_err(|_| ReferenceTransportError::InvalidObjectEvidence)?;
    Ok(ReferenceNotModifiedReceipt {
        configured_locator: transport.request().configured_locator().clone(),
        transport,
        final_locator,
        redirect_chain,
        received_at,
        transport_elapsed_nanos,
        prior_payload_digest: conditional.prior_payload_digest(),
        prior_payload_bytes: conditional.prior_payload_bytes(),
        prior_object_id: conditional.prior_object_id().clone(),
        response_cache,
    })
}

struct SourceFileEvidence {
    filename: Option<SourceIdentifier>,
    publication_date: Option<CalendarDate>,
}

fn validate_source_file_evidence(
    request: &OfficialReferenceRequest,
    content_disposition: Option<&ReferenceHeaderValue>,
) -> Result<SourceFileEvidence, ReferenceTransportError> {
    match request.surface() {
        ReferenceSurface::CboeAllSeries { venue } => {
            let filename = required_attachment_filename(content_disposition)?;
            let (expected_prefix, date) = match venue {
                CboeVenue::C1 => (
                    "cone_listed_symbol_reference_",
                    parse_cboe_filename(&filename, "cone")?,
                ),
                CboeVenue::Bzx => (
                    "opt_listed_symbol_reference_",
                    parse_cboe_filename(&filename, "opt")?,
                ),
                CboeVenue::C2 => (
                    "ctwo_listed_symbol_reference_",
                    parse_cboe_filename(&filename, "ctwo")?,
                ),
                CboeVenue::Edgx => (
                    "exo_listed_symbol_reference_",
                    parse_cboe_filename(&filename, "exo")?,
                ),
            };
            if !filename.starts_with(expected_prefix) {
                return Err(ReferenceTransportError::InvalidContentDisposition);
            }
            Ok(SourceFileEvidence {
                filename: Some(source_filename(filename)?),
                publication_date: Some(date),
            })
        }
        ReferenceSurface::OccDlpSelectedText => {
            let filename = required_attachment_filename(content_disposition)?;
            if filename != "dlpDownload.txt" {
                return Err(ReferenceTransportError::InvalidContentDisposition);
            }
            Ok(SourceFileEvidence {
                filename: Some(source_filename(filename)?),
                publication_date: None,
            })
        }
        ReferenceSurface::OccDlpDailyText | ReferenceSurface::OccDlpDailyXml => {
            let filename = required_attachment_filename(content_disposition)?;
            let expected_date = request
                .expected_publication_date
                .ok_or(ReferenceTransportError::MissingPublicationDate)?;
            let extension = if matches!(request.surface(), ReferenceSurface::OccDlpDailyXml) {
                "xml"
            } else {
                "txt"
            };
            let expected = format!(
                "listedoptions.{:04}{:02}{:02}.{extension}",
                expected_date.year(),
                expected_date.month(),
                expected_date.day()
            );
            if filename != expected {
                return Err(ReferenceTransportError::PublicationDateMismatch);
            }
            Ok(SourceFileEvidence {
                filename: Some(source_filename(filename)?),
                publication_date: Some(expected_date),
            })
        }
        ReferenceSurface::OccMemoIndexCsv
        | ReferenceSurface::OccMemoIndexJson
        | ReferenceSurface::OccMemoDocument { .. }
        | ReferenceSurface::OccMemoAttachment { .. } => Ok(SourceFileEvidence {
            filename: None,
            publication_date: request.expected_publication_date,
        }),
    }
}

fn required_attachment_filename(
    disposition: Option<&ReferenceHeaderValue>,
) -> Result<String, ReferenceTransportError> {
    let value = disposition.ok_or(ReferenceTransportError::MissingContentDisposition)?;
    let raw = value
        .as_str()
        .strip_prefix("attachment; filename=")
        .ok_or(ReferenceTransportError::InvalidContentDisposition)?;
    let filename = raw
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap_or(raw);
    if filename.is_empty()
        || filename.len() > 128
        || filename.contains(['/', '\\'])
        || !filename
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ReferenceTransportError::InvalidContentDisposition);
    }
    Ok(filename.to_owned())
}

fn parse_cboe_filename(
    filename: &str,
    venue_prefix: &str,
) -> Result<CalendarDate, ReferenceTransportError> {
    let prefix = format!("{venue_prefix}_listed_symbol_reference_");
    let coordinate = filename
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(".csv"))
        .ok_or(ReferenceTransportError::InvalidContentDisposition)?;
    if coordinate.len() != 19
        || coordinate
            .bytes()
            .enumerate()
            .any(|(index, byte)| match index {
                4 | 7 | 10 | 13 | 16 => byte != b'_',
                _ => !byte.is_ascii_digit(),
            })
    {
        return Err(ReferenceTransportError::InvalidContentDisposition);
    }
    let year = coordinate[0..4]
        .parse::<u16>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    let month = coordinate[5..7]
        .parse::<u8>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    let day = coordinate[8..10]
        .parse::<u8>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    let hour = coordinate[11..13]
        .parse::<u8>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    let minute = coordinate[14..16]
        .parse::<u8>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    let second = coordinate[17..19]
        .parse::<u8>()
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)?;
    if hour > 23 || minute > 59 || second > 60 {
        return Err(ReferenceTransportError::InvalidContentDisposition);
    }
    CalendarDate::new(year, month, day)
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)
}

fn source_filename(value: String) -> Result<SourceIdentifier, ReferenceTransportError> {
    SourceIdentifier::try_from(value)
        .map_err(|_| ReferenceTransportError::InvalidContentDisposition)
}

#[cfg(test)]
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
        DecoderPolicy::OccDlp { schema } => {
            let admitted = match schema {
                OccDlpSchema::SelectedTextV1 | OccDlpSchema::DailyTextV1 => {
                    &["text/plain", "application/octet-stream"][..]
                }
                OccDlpSchema::DailyXmlV1 => {
                    &["application/xml", "text/xml", "application/octet-stream"][..]
                }
            };
            ensure_media_type(media_type, admitted)?;
            Ok(SelectedReferenceDecoder::OccDlp(*schema))
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
        ReferenceSurface::OccDlpDailyXml => "occ-dlp-daily-xml",
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

#[cfg(test)]
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
    /// Registry extraction authority or shared provider-budget state refused the operation.
    #[error("option-reference extraction authority failed: {0}")]
    Authority(#[from] ExtractionAuthorityError),
    /// Client metadata was local-only, budget-free, non-hardened, or differed from the authority.
    #[error("invalid option-reference extraction authority metadata")]
    InvalidAuthorityMetadata,
    /// A provider-bound production client was asked to acquire another provider's surface.
    #[error("option-reference request surface does not belong to the bound provider")]
    ProviderSurfaceMismatch,
    /// A pending production response was completed without exact terminal parser evidence.
    #[error("option-reference response lacks matching terminal schema validation")]
    SchemaValidationFailed,
    /// A one-use production transport completion capability was reused or lost.
    #[error("invalid option-reference production transport state")]
    InvalidProtocolState,
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
    /// OCC memo acquisition was explicitly blocked by a provider anti-bot challenge response.
    #[error("OCC memo acquisition encountered a provider anti-bot challenge")]
    ProviderAntiBotChallenge,
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
    /// A file surface omitted its required source filename evidence.
    #[error("option-reference HTTP response omitted content disposition")]
    MissingContentDisposition,
    /// Source filename or disposition syntax differed from the exact selected contract.
    #[error("invalid option-reference HTTP content disposition")]
    InvalidContentDisposition,
    /// The source filename date differed from the exact dated publication request.
    #[error("option-reference source publication date mismatch")]
    PublicationDateMismatch,
    /// Content-Type was outside the closed surface contract.
    #[error("unexpected option-reference HTTP content type")]
    UnexpectedContentType,
    /// Exact raw-object context could not be constructed.
    #[error("invalid option-reference raw-object evidence")]
    InvalidObjectEvidence,
    /// A bounded vector or identifier allocation failed.
    #[error("option-reference bounded allocation failed")]
    AllocationFailed,
    /// Shared logical raw capture rejected or could not finish the streamed object.
    #[error("option-reference durable raw capture failed")]
    RawCaptureFailed,
    /// The bounded raw-capture worker ended without a durable result.
    #[error("option-reference durable raw-capture worker failed")]
    RawCaptureWorkerFailed,
    /// A trusted local wall-clock observation could not be represented.
    #[error("option-reference trusted local time is unavailable")]
    TrustedTimeUnavailable,
}

impl ReferenceTransportError {
    /// Builds the closed executor-failure value without retaining unbounded transport text.
    #[cfg(test)]
    pub(crate) const fn executor_failed() -> Self {
        Self::ExecutorFailed
    }
}
