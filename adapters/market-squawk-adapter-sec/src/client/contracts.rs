//! Validated SEC locators, retained representations, health, and typed failures.

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{AvailabilityEvidence, EvidenceDigest, Timestamp};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_sources::{
    BudgetPoolError, BudgetUnavailableReason, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    CompanyFactsDocument, ParsedXbrlDocument, RawEvidenceError, RawEvidenceStore, SecParserError,
    SecParserLimits, SecRepresentationError, SubmissionsDocument,
};

const DATA_BASE: &str = "https://data.sec.gov";
const ARCHIVES_BASE: &str = "https://www.sec.gov/Archives/edgar/data";

/// SEC-required declared organization and administrative contact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecContact {
    user_agent: String,
}

impl SecContact {
    /// Constructs a bounded ASCII user agent containing organization and contact email.
    pub fn try_new(organization: &str, administrative_email: &str) -> Result<Self, SecClientError> {
        if organization.is_empty()
            || administrative_email.is_empty()
            || organization.len() > 128
            || administrative_email.len() > 128
            || !organization.is_ascii()
            || !administrative_email.is_ascii()
            || organization.bytes().any(|byte| byte.is_ascii_control())
            || administrative_email
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            || !administrative_email
                .split_once('@')
                .is_some_and(|(local, domain)| {
                    !local.is_empty()
                        && domain.contains('.')
                        && !domain.starts_with('.')
                        && !domain.ends_with('.')
                })
        {
            return Err(SecClientError::InvalidContact);
        }
        let user_agent = format!("{organization} {administrative_email}");
        reqwest::header::HeaderValue::from_str(&user_agent)
            .map_err(|_| SecClientError::InvalidContact)?;
        Ok(Self { user_agent })
    }

    /// Returns the validated SEC user-agent value.
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }
}

/// Provider locator constructed only from validated SEC identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecObjectLocator {
    url: String,
}

impl SecObjectLocator {
    /// Locates the current submissions object for a CIK.
    pub fn submissions(cik: &str) -> Result<Self, SecClientError> {
        let cik = normalized_cik(cik)?;
        Self::from_url(format!("{DATA_BASE}/submissions/CIK{cik}.json"))
    }

    /// Locates the current Company Facts object for a CIK.
    pub fn company_facts(cik: &str) -> Result<Self, SecClientError> {
        let cik = normalized_cik(cik)?;
        Self::from_url(format!("{DATA_BASE}/api/xbrl/companyfacts/CIK{cik}.json"))
    }

    /// Locates one exact SEC quarterly N-PORT or N-CEN derived-data archive.
    pub fn quarterly_bulk_archive(
        family: crate::SecBulkFamily,
        quarter: crate::SecQuarter,
    ) -> Result<Self, SecClientError> {
        let snapshot = crate::SecBulkCatalogSnapshot::official_2026_08_14()
            .map_err(|_| SecClientError::InvalidLocator)?;
        if !quarter.is_catalogued(family, snapshot) {
            return Err(SecClientError::InvalidLocator);
        }
        let dataset = match family {
            crate::SecBulkFamily::Nport => "form-n-port-data-sets",
            crate::SecBulkFamily::Ncen => "form-n-cen-data-sets",
        };
        Self::from_url(format!(
            "https://www.sec.gov/files/dera/data/{dataset}/{}q{}_{}.zip",
            quarter.year(),
            quarter.quarter(),
            family.tag(),
        ))
    }

    /// Locates the current official readme for one quarterly derived-data family.
    pub fn quarterly_bulk_readme(family: crate::SecBulkFamily) -> Result<Self, SecClientError> {
        Self::from_url(format!(
            "https://www.sec.gov/files/{}_readme.pdf",
            family.tag(),
        ))
    }

    /// Locates the official nightly complete-submissions bootstrap archive.
    pub fn bulk_submissions() -> Result<Self, SecClientError> {
        Self::from_url(
            "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip".to_owned(),
        )
    }

    /// Locates the official nightly Company Facts bootstrap archive.
    pub fn bulk_company_facts() -> Result<Self, SecClientError> {
        Self::from_url(
            "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip".to_owned(),
        )
    }

    /// Locates a provider-declared submissions companion object.
    pub fn companion(name: &str) -> Result<Self, SecClientError> {
        validate_filename(name, ".json")?;
        if !name.starts_with("CIK") || !name.contains("-submissions-") {
            return Err(SecClientError::InvalidLocator);
        }
        Self::from_url(format!("{DATA_BASE}/submissions/{name}"))
    }

    /// Locates one filing document under its CIK and accession directory.
    pub fn filing_document(
        cik: &str,
        accession: &str,
        document: &str,
    ) -> Result<Self, SecClientError> {
        let cik = normalized_cik(cik)?;
        validate_accession(accession)?;
        validate_filing_filename(document)?;
        let numeric_cik = cik.trim_start_matches('0');
        let numeric_cik = if numeric_cik.is_empty() {
            "0"
        } else {
            numeric_cik
        };
        let compact_accession = accession.replace('-', "");
        Self::from_url(format!(
            "{ARCHIVES_BASE}/{numeric_cik}/{compact_accession}/{document}"
        ))
    }

    fn from_url(url: String) -> Result<Self, SecClientError> {
        Url::parse(&url).map_err(|_| SecClientError::InvalidLocator)?;
        Ok(Self { url })
    }

    /// Returns the exact validated provider URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}

/// Retrieved, persisted exact bytes and receipt evidence.
#[derive(Clone, Debug)]
pub struct RetrievedSecBytes {
    pub(super) bytes: Bytes,
    pub(super) evidence: EvidenceDigest,
    pub(super) received_at: Timestamp,
    pub(super) availability: AvailabilityEvidence,
    pub(super) locator: Option<String>,
    pub(super) retrieval_revision: Option<u64>,
    pub(super) capture_receipt: Option<ProviderCaptureSetReceipt>,
}

impl RetrievedSecBytes {
    /// Returns exact decoded response bytes persisted in the raw store.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Returns SHA-256 raw-evidence identity.
    pub const fn evidence(&self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns the trusted local receipt used for instrument resolution and ingestion ordering.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns conservative public-availability evidence for these exact bytes.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns the exact authorized provider locator for online retrievals.
    pub fn locator(&self) -> Option<&str> {
        self.locator.as_deref()
    }

    /// Returns the durable retrieval revision for online representations.
    pub const fn retrieval_revision(&self) -> Option<u64> {
        self.retrieval_revision
    }

    /// Returns the source-neutral receipt for an exact online response body.
    ///
    /// Offline imports and local composite manifests deliberately have no provider capture.
    pub const fn capture_receipt(&self) -> Option<&ProviderCaptureSetReceipt> {
        self.capture_receipt.as_ref()
    }

    /// Rebuilds the exact body-only material required by the shared raw publication boundary.
    ///
    /// Request headers, including the identifying SEC `User-Agent`, are structurally absent.
    pub fn capture_material(&self) -> Result<Option<ProviderCaptureMaterial>, SecClientError> {
        let Some(receipt) = self.capture_receipt.as_ref() else {
            return Ok(None);
        };
        let page = receipt
            .pages()
            .first()
            .filter(|page| {
                receipt.pages().len() == 1
                    && receipt.terminal() == ProviderCaptureTerminalDisposition::StandaloneResponse
                    && receipt.request_set_identity() == page.request_identity()
                    && page.body_digest() == self.evidence
                    && self.locator.as_deref() == Some(receipt.dataset().as_str())
                    && self.retrieval_revision.is_some()
            })
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        if page.body_bytes()
            != u64::try_from(self.bytes.len()).map_err(|_| SecClientError::ResponseTooLarge)?
        {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        provider_capture_material(receipt.clone(), self.bytes.clone()).map(Some)
    }

    pub(crate) fn restored_online(
        bytes: Vec<u8>,
        evidence: EvidenceDigest,
        received_at: Timestamp,
        locator: String,
        retrieval_revision: u64,
    ) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            evidence,
            received_at,
            availability: AvailabilityEvidence::local_first_observed(received_at),
            locator: Some(locator),
            retrieval_revision: Some(retrieval_revision),
            capture_receipt: None,
        }
    }

    pub(crate) fn captured_online(
        bytes: Vec<u8>,
        evidence: EvidenceDigest,
        first_observed_at: Timestamp,
        locator: String,
        retrieval_revision: u64,
        capture_receipt: ProviderCaptureSetReceipt,
    ) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            evidence,
            received_at: first_observed_at,
            availability: AvailabilityEvidence::local_first_observed(first_observed_at),
            locator: Some(locator),
            retrieval_revision: Some(retrieval_revision),
            capture_receipt: Some(capture_receipt),
        }
    }

    pub(crate) fn offline_import(
        bytes: &[u8],
        evidence: EvidenceDigest,
        received_at: Timestamp,
    ) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(bytes),
            evidence,
            received_at,
            availability: AvailabilityEvidence::Unknown,
            locator: None,
            retrieval_revision: None,
            capture_receipt: None,
        }
    }

    pub(crate) fn local_composite(
        bytes: Vec<u8>,
        evidence: EvidenceDigest,
        available_at: Timestamp,
    ) -> Self {
        Self {
            bytes: Bytes::from(bytes),
            evidence,
            received_at: available_at,
            availability: AvailabilityEvidence::local_first_observed(available_at),
            locator: None,
            retrieval_revision: None,
            capture_receipt: None,
        }
    }
}

impl fmt::Display for RetrievedSecBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SEC evidence ({} bytes)", self.bytes.len())
    }
}

/// Parsed submissions paired with retained exact source bytes.
#[derive(Clone, Debug)]
pub struct RetrievedSubmissions {
    document: SubmissionsDocument,
    raw: RetrievedSecBytes,
    components: Vec<RetrievedSecBytes>,
}

impl RetrievedSubmissions {
    pub(crate) fn new(
        document: SubmissionsDocument,
        raw: RetrievedSecBytes,
        components: Vec<RetrievedSecBytes>,
    ) -> Self {
        Self {
            document,
            raw,
            components,
        }
    }

    /// Returns the parsed and reconciled submissions document.
    pub const fn document(&self) -> &SubmissionsDocument {
        &self.document
    }

    /// Returns exact current or composite evidence.
    pub const fn raw(&self) -> &RetrievedSecBytes {
        &self.raw
    }

    /// Returns every exact remote representation covered by a composite manifest.
    pub fn components(&self) -> &[RetrievedSecBytes] {
        &self.components
    }

    /// Returns the exact current submissions representation that authored company metadata.
    ///
    /// Complete submissions snapshots retain this as the first component beside their composite
    /// manifest. A direct current-only retrieval uses [`Self::raw`] as that same representation.
    pub fn current_component(&self) -> &RetrievedSecBytes {
        self.components.first().unwrap_or(&self.raw)
    }
}

/// Parsed Company Facts paired with retained exact source bytes.
#[derive(Clone, Debug)]
pub struct RetrievedCompanyFacts {
    pub(super) document: CompanyFactsDocument,
    pub(super) raw: RetrievedSecBytes,
}

impl RetrievedCompanyFacts {
    /// Imports exact offline bytes without claiming when the provider first made them available.
    pub fn import_exact_bytes(
        bytes: &[u8],
        raw_store: &RawEvidenceStore,
        parser_limits: SecParserLimits,
    ) -> Result<Self, SecClientError> {
        let evidence = raw_store.persist(bytes)?;
        let received_at = system_timestamp()?;
        Ok(Self {
            document: CompanyFactsDocument::parse(bytes, parser_limits)?,
            raw: RetrievedSecBytes::offline_import(bytes, evidence, received_at),
        })
    }

    pub(crate) fn restored(
        bytes: Vec<u8>,
        evidence: EvidenceDigest,
        received_at: Timestamp,
        availability: AvailabilityEvidence,
        parser_limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecClientError> {
        let document =
            CompanyFactsDocument::parse_with_cancellation(&bytes, parser_limits, cancellation)?;
        Ok(Self {
            document,
            raw: RetrievedSecBytes {
                bytes: Bytes::from(bytes),
                evidence,
                received_at,
                availability,
                locator: None,
                retrieval_revision: None,
                capture_receipt: None,
            },
        })
    }

    /// Returns the exact parsed Company Facts document.
    pub const fn document(&self) -> &CompanyFactsDocument {
        &self.document
    }

    /// Returns exact immutable source evidence.
    pub const fn raw(&self) -> &RetrievedSecBytes {
        &self.raw
    }

    /// Returns the exact online Company Facts response material, or `None` for an offline import.
    pub fn capture_material(&self) -> Result<Option<ProviderCaptureMaterial>, SecClientError> {
        self.raw.capture_material()
    }
}

/// Parsed filing XBRL paired with retained exact source bytes.
#[derive(Clone, Debug)]
pub struct RetrievedXbrlDocument {
    pub(super) document: ParsedXbrlDocument,
    pub(super) raw: RetrievedSecBytes,
}

impl RetrievedXbrlDocument {
    /// Returns parsed numeric and nonnumeric occurrence families.
    pub const fn document(&self) -> &ParsedXbrlDocument {
        &self.document
    }

    /// Returns exact immutable source evidence.
    pub const fn raw(&self) -> &RetrievedSecBytes {
        &self.raw
    }

    /// Returns the exact online filing/XBRL response material.
    pub fn capture_material(&self) -> Result<ProviderCaptureMaterial, SecClientError> {
        self.raw
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)
    }
}

pub(crate) fn provider_capture_material(
    receipt: ProviderCaptureSetReceipt,
    bytes: Bytes,
) -> Result<ProviderCaptureMaterial, SecClientError> {
    if receipt.pages().len() != 1 {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let page = receipt
        .pages()
        .first()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let received_at = DateTime::<Utc>::from_timestamp_nanos(page.received_at().unix_nanos());
    let record = RawCaptureRecord::try_new_live(
        deterministic_capture_uuid(b"event", &receipt, 0),
        Arc::from(receipt.source_id().as_str()),
        deterministic_capture_uuid(b"connection", &receipt, 0),
        Some(0),
        None,
        received_at,
        bytes,
    )?;
    ProviderCaptureMaterial::try_new(receipt, vec![record]).map_err(Into::into)
}

pub(crate) fn deterministic_capture_uuid(
    tag: &[u8],
    receipt: &ProviderCaptureSetReceipt,
    ordinal: u16,
) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sec-provider-capture-id/v1");
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update(receipt.request_set_identity().bytes());
    hash.update(receipt.observation_digest().bytes());
    hash.update(ordinal.to_be_bytes());
    let mut bytes: [u8; 16] = hash.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has a fixed length");
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

/// Current extraction-specific provider health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecExtractionHealthState {
    /// Retrieval is authorized and no current provider or local fault is known.
    Ready,
    /// Provider-directed cooldown is active after throttling or temporary unavailability.
    CoolingDown,
    /// Provider rejected the declared public identity as unauthorized.
    Unauthorized,
    /// Provider or intermediary blocked the authorized request.
    Blocked,
    /// Provider returned an availability failure not covered by a cooldown state.
    ProviderUnavailable,
    /// Provider response violated the expected representation contract.
    InvalidResponse,
    /// A local persistence, parsing, synchronization, or worker failure occurred.
    LocalFailure,
}

/// Timestamped extraction health retained independently from market freshness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecExtractionHealth {
    pub(super) state: SecExtractionHealthState,
    pub(super) observed_at: Timestamp,
    pub(super) http_status: Option<u16>,
}

impl SecExtractionHealth {
    /// Returns the current extraction-health classification.
    pub const fn state(self) -> SecExtractionHealthState {
        self.state
    }

    /// Returns when the health state was last observed locally.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the provider HTTP status when the state originated from a response status.
    pub const fn http_status(self) -> Option<u16> {
        self.http_status
    }
}

pub(super) const fn health_for_http_status(status: u16) -> SecExtractionHealthState {
    match status {
        401 => SecExtractionHealthState::Unauthorized,
        403 => SecExtractionHealthState::Blocked,
        429 | 503 => SecExtractionHealthState::CoolingDown,
        _ => SecExtractionHealthState::ProviderUnavailable,
    }
}

pub(super) const fn validation_health_for_error(
    error: &SecClientError,
) -> Option<SecExtractionHealthState> {
    match error {
        SecClientError::Cancelled | SecClientError::Parser(SecParserError::Cancelled) => None,
        SecClientError::Parser(SecParserError::AllocationFailed)
        | SecClientError::AllocationFailed
        | SecClientError::BlockingAdmissionClosed
        | SecClientError::BlockingWorkerFailed
        | SecClientError::RawEvidence(_)
        | SecClientError::Representation(_)
        | SecClientError::RevisionAuthority(_)
        | SecClientError::Normalization(_) => Some(SecExtractionHealthState::LocalFailure),
        SecClientError::Parser(_)
        | SecClientError::CompanyIdentity(_)
        | SecClientError::ResponseCikMismatch
        | SecClientError::InvalidCompanionSet
        | SecClientError::InvalidCompositeRepresentation
        | SecClientError::InvalidCaptureMaterial
        | SecClientError::ProviderCapture(_)
        | SecClientError::CompanionObjectLimitExceeded
        | SecClientError::CompositeByteLimitExceeded => {
            Some(SecExtractionHealthState::InvalidResponse)
        }
        SecClientError::RawCapture(_) => Some(SecExtractionHealthState::LocalFailure),
        _ => Some(SecExtractionHealthState::LocalFailure),
    }
}

pub(crate) fn system_timestamp() -> Result<Timestamp, SecClientError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SecClientError::ClockOutOfRange)?;
    let nanos = duration
        .as_nanos()
        .try_into()
        .map_err(|_| SecClientError::ClockOutOfRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

pub(super) fn normalized_cik(value: &str) -> Result<String, SecClientError> {
    if value.is_empty() || value.len() > 10 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SecClientError::InvalidLocator);
    }
    Ok(format!("{value:0>10}"))
}

fn validate_accession(value: &str) -> Result<(), SecClientError> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[10] != b'-'
        || bytes[13] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 10 && index != 13 && !byte.is_ascii_digit())
    {
        Err(SecClientError::InvalidLocator)
    } else {
        Ok(())
    }
}

fn validate_filename(value: &str, suffix: &str) -> Result<(), SecClientError> {
    if value.is_empty()
        || value.len() > 255
        || !value.ends_with(suffix)
        || value.contains('/')
        || value.contains('\\')
        || value.contains("..")
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        Err(SecClientError::InvalidLocator)
    } else {
        Ok(())
    }
}

fn validate_filing_filename(value: &str) -> Result<(), SecClientError> {
    for suffix in [".htm", ".html", ".xml"] {
        if value.ends_with(suffix) {
            return validate_filename(value, suffix);
        }
    }
    Err(SecClientError::InvalidLocator)
}

/// SEC retrieval and authority failure.
#[derive(Debug, Error)]
pub enum SecClientError {
    #[error("SEC contact declaration is invalid")]
    InvalidContact,
    #[error("SEC object locator is invalid")]
    InvalidLocator,
    #[error("SEC source registration does not match metadata")]
    RegistrationMismatch,
    #[error("SEC source network access is denied")]
    NetworkDenied,
    #[error("SEC source lacks a registry-coordinated shared budget")]
    MissingSharedBudget,
    #[error("SEC taxonomy publisher rate registration failed")]
    TaxonomyRateRegistration(#[source] BudgetPoolError),
    #[error("SEC taxonomy publisher rate authority is unavailable: {0:?}")]
    TaxonomyRateUnavailable(BudgetUnavailableReason),
    #[error("SEC source budget exceeds the official aggregate request ceiling")]
    UnsafeBudgetPolicy,
    #[error("SEC source HTTP client profile is unsafe")]
    UnsafeClientProfile,
    #[error("SEC retrieval was cancelled")]
    Cancelled,
    #[error("SEC response read timed out")]
    ReadTimeout,
    #[error("SEC response exceeded its decoded-byte bound")]
    ResponseTooLarge,
    #[error("SEC response bounded allocation failed")]
    AllocationFailed,
    #[error("SEC redirect was invalid")]
    InvalidRedirect,
    #[error("SEC complete-submissions deadline was exceeded")]
    DeadlineExceeded,
    #[error("SEC complete-submissions bounds are invalid")]
    InvalidCompositeBounds,
    #[error("SEC declared more submissions companions than the configured object bound")]
    CompanionObjectLimitExceeded,
    #[error("SEC complete submissions exceeded the configured decoded-byte bound")]
    CompositeByteLimitExceeded,
    #[error("SEC submissions companion set is duplicated or inconsistent with its CIK")]
    InvalidCompanionSet,
    #[error("SEC composite representation evidence is incomplete")]
    InvalidCompositeRepresentation,
    #[error("SEC provider capture material is incomplete or inconsistent")]
    InvalidCaptureMaterial,
    #[error("SEC composite-manifest serialization failed")]
    CompositeSerialization,
    #[error("SEC conditional-response validator header is invalid")]
    InvalidValidatorHeader,
    #[error("SEC returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("SEC response CIK does not match the exact requested CIK")]
    ResponseCikMismatch,
    #[error("SEC raw evidence did not match its computed identity")]
    RawEvidenceMismatch,
    #[error("system clock is outside the domain timestamp range")]
    ClockOutOfRange,
    #[error("SEC extraction health state is poisoned")]
    HealthStatePoisoned,
    #[error("SEC bounded blocking admission is closed")]
    BlockingAdmissionClosed,
    #[error("SEC bounded blocking worker failed")]
    BlockingWorkerFailed,
    #[error(transparent)]
    Authority(#[from] market_squawk_sources::ExtractionAuthorityError),
    #[error(transparent)]
    NetworkPolicy(#[from] market_squawk_sources::NetworkPolicyError),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error(transparent)]
    Parser(SecParserError),
    #[error(transparent)]
    Xbrl(#[from] crate::SecXbrlError),
    #[error(transparent)]
    RawEvidence(RawEvidenceError),
    #[error(transparent)]
    Representation(SecRepresentationError),
    #[error(transparent)]
    Normalization(crate::SecNormalizationError),
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error(transparent)]
    CompanyIdentity(#[from] market_squawk_domain::CompanyIdentityError),
    #[error(transparent)]
    RevisionAuthority(#[from] market_squawk_sources::ObservedRevisionError),
    #[error(transparent)]
    ProviderCapture(#[from] ProviderCaptureError),
    #[error(transparent)]
    RawCapture(#[from] RawCaptureRecordError),
}

impl From<RawEvidenceError> for SecClientError {
    fn from(value: RawEvidenceError) -> Self {
        match value {
            RawEvidenceError::Cancelled => Self::Cancelled,
            error => Self::RawEvidence(error),
        }
    }
}

impl From<SecRepresentationError> for SecClientError {
    fn from(value: SecRepresentationError) -> Self {
        match value {
            SecRepresentationError::Cancelled => Self::Cancelled,
            error => Self::Representation(error),
        }
    }
}

impl From<SecParserError> for SecClientError {
    fn from(value: SecParserError) -> Self {
        match value {
            SecParserError::Cancelled => Self::Cancelled,
            error => Self::Parser(error),
        }
    }
}

impl From<crate::SecNormalizationError> for SecClientError {
    fn from(value: crate::SecNormalizationError) -> Self {
        match value {
            crate::SecNormalizationError::Cancelled => Self::Cancelled,
            error => Self::Normalization(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SecClientError, SecExtractionHealthState, health_for_http_status,
        validation_health_for_error,
    };
    use crate::SecParserError;

    #[test]
    fn provider_statuses_fail_closed_into_distinct_extraction_health() {
        assert_eq!(
            health_for_http_status(401),
            SecExtractionHealthState::Unauthorized
        );
        assert_eq!(
            health_for_http_status(403),
            SecExtractionHealthState::Blocked
        );
        assert_eq!(
            health_for_http_status(429),
            SecExtractionHealthState::CoolingDown
        );
        assert_eq!(
            health_for_http_status(503),
            SecExtractionHealthState::CoolingDown
        );
    }

    #[test]
    fn release_blocking_remediation_validation_health_is_fail_closed_and_cancellation_neutral() {
        let cases = [
            (
                SecClientError::Parser(SecParserError::DuplicateKey),
                Some(SecExtractionHealthState::InvalidResponse),
            ),
            (
                SecClientError::Parser(SecParserError::AllocationFailed),
                Some(SecExtractionHealthState::LocalFailure),
            ),
            (
                SecClientError::BlockingWorkerFailed,
                Some(SecExtractionHealthState::LocalFailure),
            ),
            (SecClientError::Cancelled, None),
        ];

        for (error, expected) in cases {
            assert_eq!(validation_health_for_error(&error), expected);
        }
    }
}
