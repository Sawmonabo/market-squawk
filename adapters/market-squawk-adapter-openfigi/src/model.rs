use bytes::Bytes;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, Figi, MetadataRevision,
    ProviderInstrumentId, SourceId, Timestamp, VenueId,
};
use sha2::{Digest as _, Sha256};

use crate::{OpenFigiModelError, OpenFigiRateLimitError};

/// Fixed production OpenFIGI V3 mapping endpoint.
pub const OPENFIGI_V3_MAPPING_URL: &str = "https://api.openfigi.com/v3/mapping";
/// Provider identity required by the adapter's immutable source metadata.
pub const OPENFIGI_V3_PROVIDER: &str = "openfigi-v3";
/// Conservative unauthenticated request ceiling.
pub const OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW: u32 = 25;
/// One minute expressed in nanoseconds for the unauthenticated shared budget.
pub const OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS: u64 = 60_000_000_000;
/// Conservative V3 unauthenticated mapping-job ceiling.
///
/// OpenFIGI's current documentation disagrees between five and ten jobs for unauthenticated
/// requests. This uses the stricter published ceiling until the provider contract is unambiguous.
pub const OPENFIGI_PUBLIC_MAX_JOBS: usize = 5;
/// Official authenticated request ceiling for one six-second window.
pub const OPENFIGI_API_KEY_REQUESTS_PER_WINDOW: u32 = 25;
/// Six seconds expressed in nanoseconds for an API-key-qualified shared budget.
pub const OPENFIGI_API_KEY_REQUEST_WINDOW_NANOS: u64 = 6_000_000_000;
/// Official authenticated V3 mapping-job ceiling.
pub const OPENFIGI_API_KEY_MAX_JOBS: usize = 100;

const MAX_NASDAQ_SYMBOL_BYTES: usize = 14;
const MAX_RATE_HEADER_BYTES: usize = 20;

/// OpenFIGI mapping access tier selected by evidence-backed source metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenFigiAccess {
    /// No account or API key; the adapter enforces five jobs and 25 requests per minute.
    Public,
    /// User-authorized API key; the key is borrowed only for each request.
    ApiKey,
}

impl OpenFigiAccess {
    /// Returns the maximum mapping jobs accepted in one request.
    pub const fn max_jobs_per_request(self) -> usize {
        match self {
            Self::Public => OPENFIGI_PUBLIC_MAX_JOBS,
            Self::ApiKey => OPENFIGI_API_KEY_MAX_JOBS,
        }
    }

    pub(crate) const fn request_window(self) -> (u32, u64) {
        match self {
            Self::Public => (
                OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW,
                OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS,
            ),
            Self::ApiKey => (
                OPENFIGI_API_KEY_REQUESTS_PER_WINDOW,
                OPENFIGI_API_KEY_REQUEST_WINDOW_NANOS,
            ),
        }
    }
}

/// Current Nasdaq listing identity and exact source evidence submitted for FIGI mapping.
///
/// Only `symbol` and `mic` leave the process. The remaining fields bind any accepted mapping to
/// the exact current-directory observation that requested it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFigiListingMappingJob {
    listing_source_id: SourceId,
    listing_metadata_revision: MetadataRevision,
    listing_payload_evidence: ExactPayloadEvidence,
    listing_source_timestamp: Timestamp,
    listing_observed_at: Timestamp,
    symbol: ProviderInstrumentId,
    mic: VenueId,
}

impl OpenFigiListingMappingJob {
    /// Constructs a source-qualified mapping job for one current Nasdaq directory record.
    ///
    /// # Errors
    ///
    /// Rejects non-ASCII or oversized symbols, non-MIC venue identifiers, and source timestamps
    /// later than the local observation time.
    #[allow(
        clippy::too_many_arguments,
        reason = "listing identity and every provenance coordinate remain explicit"
    )]
    pub fn try_new(
        listing_source_id: SourceId,
        listing_metadata_revision: MetadataRevision,
        listing_payload_evidence: ExactPayloadEvidence,
        listing_source_timestamp: Timestamp,
        listing_observed_at: Timestamp,
        symbol: ProviderInstrumentId,
        mic: VenueId,
    ) -> Result<Self, OpenFigiModelError> {
        let symbol_bytes = symbol.as_str().as_bytes();
        if symbol_bytes.is_empty()
            || symbol_bytes.len() > MAX_NASDAQ_SYMBOL_BYTES
            || !symbol_bytes.is_ascii()
            || symbol_bytes.iter().any(u8::is_ascii_lowercase)
        {
            return Err(OpenFigiModelError::InvalidSymbol);
        }
        let mic_bytes = mic.as_str().as_bytes();
        if mic_bytes.len() != 4
            || !mic_bytes
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        {
            return Err(OpenFigiModelError::InvalidMic);
        }
        if listing_source_timestamp > listing_observed_at {
            return Err(OpenFigiModelError::InvalidTemporalOrder);
        }
        Ok(Self {
            listing_source_id,
            listing_metadata_revision,
            listing_payload_evidence,
            listing_source_timestamp,
            listing_observed_at,
            symbol,
            mic,
        })
    }

    /// Returns the exact source registration that supplied the listing.
    pub const fn listing_source_id(&self) -> &SourceId {
        &self.listing_source_id
    }

    /// Returns the source metadata revision that supplied the listing.
    pub const fn listing_metadata_revision(&self) -> &MetadataRevision {
        &self.listing_metadata_revision
    }

    /// Returns exact evidence for the source payload containing the listing.
    pub const fn listing_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.listing_payload_evidence
    }

    /// Returns the source-supplied current-directory timestamp.
    pub const fn listing_source_timestamp(&self) -> Timestamp {
        self.listing_source_timestamp
    }

    /// Returns when the installation first observed the listing source payload.
    pub const fn listing_observed_at(&self) -> Timestamp {
        self.listing_observed_at
    }

    /// Returns the exact provider listing symbol sent as `idValue`.
    pub const fn symbol(&self) -> &ProviderInstrumentId {
        &self.symbol
    }

    /// Returns the listing venue MIC sent as `micCode`.
    pub const fn mic(&self) -> &VenueId {
        &self.mic
    }
}

/// FIGI-only candidate derived from one V3 mapping response item.
///
/// Descriptive response fields deliberately do not appear here. The exchange-level FIGI is the
/// assigned result; composite and share-class FIGIs are retained only as FIGI relationships.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OpenFigiIdentityCandidate {
    exchange_figi: Figi,
    composite_figi: Option<Figi>,
    share_class_figi: Option<Figi>,
}

impl OpenFigiIdentityCandidate {
    pub(crate) const fn new(
        exchange_figi: Figi,
        composite_figi: Option<Figi>,
        share_class_figi: Option<Figi>,
    ) -> Self {
        Self {
            exchange_figi,
            composite_figi,
            share_class_figi,
        }
    }

    /// Returns the checksum-valid exchange-level FIGI assigned by the mapping response.
    pub const fn exchange_figi(&self) -> &Figi {
        &self.exchange_figi
    }

    /// Returns the checksum-valid composite FIGI relationship when supplied.
    pub const fn composite_figi(&self) -> Option<&Figi> {
        self.composite_figi.as_ref()
    }

    /// Returns the checksum-valid share-class FIGI relationship when supplied.
    pub const fn share_class_figi(&self) -> Option<&Figi> {
        self.share_class_figi.as_ref()
    }
}

/// Why a provider job result cannot support any typed identity admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenFigiConflictReason {
    /// No V3 `data`, `warning`, or `error` member was present.
    MissingOutcome,
    /// More than one mutually exclusive V3 outcome member was present.
    MultipleOutcomeKinds,
    /// A V3 `data` member contained no candidates.
    EmptyData,
    /// Candidate count exceeded the local mapping bound.
    CandidateLimitExceeded,
    /// A returned FIGI was absent or failed syntax/checksum validation.
    InvalidFigi,
    /// Provider returned the exact same FIGI relationship more than once.
    DuplicateCandidate,
    /// One exchange FIGI was paired with contradictory composite/share-class relationships.
    RelationshipConflict,
    /// Provider warning or error text was empty or outside its bounded response contract.
    InvalidProviderMessage,
}

/// Fail-closed result for one request-position-preserving V3 mapping job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenFigiMappingOutcome {
    /// Exactly one unique, internally consistent FIGI relationship was returned.
    Exact(OpenFigiIdentityCandidate),
    /// V3 returned its dedicated nonempty `warning` outcome and no FIGI.
    NoMatch,
    /// Multiple distinct exchange FIGIs remain; callers must not select one implicitly.
    Ambiguous {
        /// Bounded deterministic candidate set retained for explicit resolution.
        candidates: Vec<OpenFigiIdentityCandidate>,
    },
    /// Provider response was structurally or relationally contradictory.
    Conflict {
        /// Stable fail-closed conflict class.
        reason: OpenFigiConflictReason,
    },
    /// V3 returned a non-no-match `error`; only its digest is normalized.
    ProviderError {
        /// SHA-256 of the exact bounded provider error text retained in the raw response.
        message_digest: EvidenceDigest,
    },
}

/// One source-qualified job paired with the result at the same V3 response-array position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFigiMappingResult {
    job: OpenFigiListingMappingJob,
    outcome: OpenFigiMappingOutcome,
}

impl OpenFigiMappingResult {
    pub(crate) const fn new(
        job: OpenFigiListingMappingJob,
        outcome: OpenFigiMappingOutcome,
    ) -> Self {
        Self { job, outcome }
    }

    /// Returns the exact listing identity and provenance submitted for this result.
    pub const fn job(&self) -> &OpenFigiListingMappingJob {
        &self.job
    }

    /// Returns the typed fail-closed identity outcome.
    pub const fn outcome(&self) -> &OpenFigiMappingOutcome {
        &self.outcome
    }
}

/// Exact bounded HTTP payload and SHA-256 evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFigiRawPayload {
    bytes: Bytes,
    evidence: ExactPayloadEvidence,
}

impl OpenFigiRawPayload {
    pub(crate) fn new(bytes: Bytes) -> Self {
        let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&bytes).into(),
        ));
        Self { bytes, evidence }
    }

    /// Returns the exact HTTP entity bytes as a borrowed slice.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    /// Returns SHA-256 evidence for those exact bytes.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }
}

/// Exact OpenFIGI request-window headers plus their validated numeric interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFigiRateLimitEvidence {
    raw_limit: String,
    raw_remaining: String,
    raw_reset: String,
    limit: u32,
    remaining: u32,
    reset_after_seconds: u64,
}

impl OpenFigiRateLimitEvidence {
    /// Parses the exact singleton `ratelimit-*` header values.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, nondecimal, overflowing, or inconsistent values.
    pub fn try_from_raw(
        limit: &[u8],
        remaining: &[u8],
        reset: &[u8],
    ) -> Result<Self, OpenFigiRateLimitError> {
        let (raw_limit, limit) = parse_header_u32(limit)?;
        let (raw_remaining, remaining) = parse_header_u32(remaining)?;
        let (raw_reset, reset_after_seconds) = parse_header_u64(reset)?;
        if limit == 0 || remaining > limit {
            return Err(OpenFigiRateLimitError::Inconsistent);
        }
        Ok(Self {
            raw_limit,
            raw_remaining,
            raw_reset,
            limit,
            remaining,
            reset_after_seconds,
        })
    }

    /// Returns the provider-declared request count for the current window.
    pub const fn limit(&self) -> u32 {
        self.limit
    }

    /// Returns the provider-declared remaining request count.
    pub const fn remaining(&self) -> u32 {
        self.remaining
    }

    /// Returns provider-declared seconds until the current window resets.
    pub const fn reset_after_seconds(&self) -> u64 {
        self.reset_after_seconds
    }

    /// Returns the exact `ratelimit-limit` bytes.
    pub fn raw_limit(&self) -> &[u8] {
        self.raw_limit.as_bytes()
    }

    /// Returns the exact `ratelimit-remaining` bytes.
    pub fn raw_remaining(&self) -> &[u8] {
        self.raw_remaining.as_bytes()
    }

    /// Returns the exact `ratelimit-reset` bytes.
    pub fn raw_reset(&self) -> &[u8] {
        self.raw_reset.as_bytes()
    }
}

/// Complete evidence receipt for one successful V3 mapping exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenFigiMappingReceipt {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    coverage_evidence: ExactPayloadEvidence,
    access: OpenFigiAccess,
    requested_at: Timestamp,
    received_at: Timestamp,
    request: OpenFigiRawPayload,
    response: OpenFigiRawPayload,
    rate_limit: OpenFigiRateLimitEvidence,
    results: Vec<OpenFigiMappingResult>,
}

impl OpenFigiMappingReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "receipt authority, wire evidence, time, and results are independent"
    )]
    pub(crate) fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        coverage_evidence: ExactPayloadEvidence,
        access: OpenFigiAccess,
        requested_at: Timestamp,
        received_at: Timestamp,
        request: OpenFigiRawPayload,
        response: OpenFigiRawPayload,
        rate_limit: OpenFigiRateLimitEvidence,
        results: Vec<OpenFigiMappingResult>,
    ) -> Result<Self, OpenFigiModelError> {
        if requested_at > received_at || results.is_empty() {
            return Err(OpenFigiModelError::InvalidReceipt);
        }
        Ok(Self {
            source_id,
            metadata_revision,
            coverage_evidence,
            access,
            requested_at,
            received_at,
            request,
            response,
            rate_limit,
            results,
        })
    }

    /// Returns the registered OpenFIGI source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact registered metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns exact evidence for the registered venue/asset coverage declaration.
    pub const fn coverage_evidence(&self) -> &ExactPayloadEvidence {
        &self.coverage_evidence
    }

    /// Returns the access tier under which the request was admitted.
    pub const fn access(&self) -> OpenFigiAccess {
        self.access
    }

    /// Returns local time immediately before the one authorized HTTP send.
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns local time after the complete bounded response was received.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact request bytes and their SHA-256 evidence.
    pub const fn request(&self) -> &OpenFigiRawPayload {
        &self.request
    }

    /// Returns exact response bytes and their SHA-256 evidence.
    ///
    /// This is source material, not normalized entitlement for descriptive metadata fields.
    pub const fn response(&self) -> &OpenFigiRawPayload {
        &self.response
    }

    /// Returns exact and parsed provider rate-window evidence.
    pub const fn rate_limit(&self) -> &OpenFigiRateLimitEvidence {
        &self.rate_limit
    }

    /// Returns request-order-preserving typed outcomes.
    pub fn results(&self) -> &[OpenFigiMappingResult] {
        &self.results
    }
}

pub(crate) fn digest_message(value: &str) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn parse_header_u32(value: &[u8]) -> Result<(String, u32), OpenFigiRateLimitError> {
    let (raw, value) = parse_header_u64(value)?;
    let value = u32::try_from(value).map_err(|_| OpenFigiRateLimitError::Invalid)?;
    Ok((raw, value))
}

fn parse_header_u64(value: &[u8]) -> Result<(String, u64), OpenFigiRateLimitError> {
    if value.is_empty()
        || value.len() > MAX_RATE_HEADER_BYTES
        || !value.iter().all(u8::is_ascii_digit)
    {
        return Err(OpenFigiRateLimitError::Invalid);
    }
    let raw = std::str::from_utf8(value)
        .map_err(|_| OpenFigiRateLimitError::Invalid)?
        .to_owned();
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| OpenFigiRateLimitError::Invalid)?;
    Ok((raw, parsed))
}
