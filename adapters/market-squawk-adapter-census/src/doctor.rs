use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp};
use market_squawk_sources::{ProviderCaptureMaterial, ProviderCaptureSetReceipt, SourceMetadata};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::{
    CensusDataQuery, CensusDataset, CensusGeography, CensusGeographyClause,
    CensusGeographyCode, CensusSelection, CensusSourceConfig, CensusSourceError,
    census_provider_rate_declaration, update_digest_component,
};
use crate::http::CensusRateLimitHeaders;

/// Maximum credential-bearing response retained by the pinned Census doctor.
pub const CENSUS_DOCTOR_MAX_RESPONSE_BYTES: usize = 16 * 1024;
/// Maximum network duration for the pinned Census doctor request.
pub const CENSUS_DOCTOR_TIMEOUT: Duration = Duration::from_secs(10);

/// Exact bounded Census surface proven by the provider doctor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusDoctorScope {
    /// 2024 ACS one-year United States total population estimate.
    Acs2024OneYearUnitedStatesPopulation,
}

/// Provider readiness proven by a successful report.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusDoctorReadiness {
    /// The key and pinned production request/response contract were directly verified.
    Available,
}

/// Sanitized numeric rate-limit headers, including an explicit all-absent state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusDoctorRateHeaderEvidence {
    limit: Option<u64>,
    remaining: Option<u64>,
    reset: Option<u64>,
}

impl CensusDoctorRateHeaderEvidence {
    /// Returns whether every supported provider rate header was explicitly absent.
    pub const fn explicitly_absent(&self) -> bool {
        self.limit.is_none() && self.remaining.is_none() && self.reset.is_none()
    }

    /// Returns the provider-reported request limit when present.
    pub const fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Returns the provider-reported remaining requests when present.
    pub const fn remaining(&self) -> Option<u64> {
        self.remaining
    }

    /// Returns the provider-reported numeric reset coordinate when present.
    pub const fn reset(&self) -> Option<u64> {
        self.reset
    }
}

/// Redacted, exact evidence from one narrowly bounded credential-bearing production request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusDoctorReport {
    readiness: CensusDoctorReadiness,
    scope: CensusDoctorScope,
    source_id: SourceId,
    metadata_revision: SourceIdentifier,
    configuration_digest: EvidenceDigest,
    credential_generation_digest: EvidenceDigest,
    owner_authorization_digest: EvidenceDigest,
    presentation_obligation_digest: EvidenceDigest,
    provider_rate_declaration_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    response_digest: EvidenceDigest,
    response_status: u16,
    response_bytes: u64,
    latency_nanos: u64,
    checked_at: Timestamp,
    population: u64,
    row_count: u32,
    column_count: u32,
    rate_header_evidence: CensusDoctorRateHeaderEvidence,
    native_schema: SourceIdentifier,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    non_endorsement_notice_required: bool,
    report_digest: EvidenceDigest,
}

impl CensusDoctorReport {
    /// Returns the exact registry source exercised by the doctor.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision exercised by the doctor.
    pub const fn metadata_revision(&self) -> &SourceIdentifier {
        &self.metadata_revision
    }

    /// Returns the exact source configuration exercised by the doctor.
    pub const fn configuration_digest(&self) -> EvidenceDigest {
        self.configuration_digest
    }

    /// Returns the frozen owner authorization exercised by the doctor.
    pub const fn owner_authorization_digest(&self) -> EvidenceDigest {
        self.owner_authorization_digest
    }

    /// Returns the exact shared rate declaration exercised by the doctor.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns directly proven readiness.
    pub const fn readiness(&self) -> CensusDoctorReadiness {
        self.readiness
    }

    /// Returns the exact bounded provider surface tested.
    pub const fn scope(&self) -> CensusDoctorScope {
        self.scope
    }

    /// Returns the exact key-free request digest.
    pub const fn request_digest(&self) -> EvidenceDigest {
        self.request_digest
    }

    /// Returns the protected credential-record generation exercised by this doctor run.
    pub const fn credential_generation_digest(&self) -> EvidenceDigest {
        self.credential_generation_digest
    }

    /// Returns the exact capture receipt identity.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }

    /// Returns the exact bounded response content identity.
    pub const fn response_digest(&self) -> EvidenceDigest {
        self.response_digest
    }

    /// Returns the validated HTTP status.
    pub const fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Returns response byte count.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns provider request latency measured by the bounded transport.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }

    /// Returns local verification time.
    pub const fn checked_at(&self) -> Timestamp {
        self.checked_at
    }

    /// Returns the positive population value used only as a semantic doctor assertion.
    pub const fn population(&self) -> u64 {
        self.population
    }

    /// Returns the exact number of validated data rows, excluding the header.
    pub const fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Returns the exact validated response column count.
    pub const fn column_count(&self) -> u32 {
        self.column_count
    }

    /// Returns the complete redacted report identity.
    pub const fn report_digest(&self) -> EvidenceDigest {
        self.report_digest
    }

    /// Returns whether product presentation must include the Census API non-endorsement notice.
    pub const fn non_endorsement_notice_required(&self) -> bool {
        self.non_endorsement_notice_required
    }

    /// Returns the exact presentation and no-reidentification obligation identity.
    pub const fn presentation_obligation_digest(&self) -> EvidenceDigest {
        self.presentation_obligation_digest
    }

    /// Returns sanitized provider rate headers or a closed explicit-absence state.
    pub const fn rate_header_evidence(&self) -> CensusDoctorRateHeaderEvidence {
        self.rate_header_evidence
    }

    /// Returns the exact native doctor decoder schema.
    pub const fn native_schema(&self) -> &SourceIdentifier {
        &self.native_schema
    }

    /// Returns the exact native decoder schema version.
    pub const fn native_schema_version(&self) -> u16 {
        self.native_schema_version
    }

    /// Returns the exact native decoder schema fingerprint.
    pub const fn native_schema_fingerprint(&self) -> EvidenceDigest {
        self.native_schema_fingerprint
    }
}

/// Doctor report plus the exact raw response material root composition must seal.
#[derive(Debug)]
pub struct CensusDoctorOutput {
    report: CensusDoctorReport,
    capture: ProviderCaptureMaterial,
}

impl CensusDoctorOutput {
    pub(crate) const fn new(
        report: CensusDoctorReport,
        capture: ProviderCaptureMaterial,
    ) -> Self {
        Self { report, capture }
    }

    /// Returns the redacted readiness evidence.
    pub const fn report(&self) -> &CensusDoctorReport {
        &self.report
    }

    /// Returns the exact bounded response material for raw sealing.
    pub const fn capture(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the indivisible doctor handoff.
    pub fn into_parts(self) -> (CensusDoctorReport, ProviderCaptureMaterial) {
        (self.report, self.capture)
    }
}

pub(crate) fn doctor_query() -> Result<CensusDataQuery, CensusSourceError> {
    Ok(CensusDataQuery::try_new(
        CensusDataset::try_new(2024, "acs/acs1")?,
        CensusSelection::variables(["NAME", "B01001_001E"])?,
        Vec::new(),
        CensusGeography::standard(
            CensusGeographyClause::try_new(
                "us",
                [CensusGeographyCode::try_new("1")?],
            )?,
            Vec::new(),
        )?,
        None,
    )?)
}

pub(crate) fn doctor_dataset_identity() -> Result<SourceIdentifier, CensusSourceError> {
    SourceIdentifier::try_from("census:doctor:2024-acs1-us-population")
        .map_err(|_| CensusSourceError::InvalidConfiguration)
}

pub(crate) fn build_doctor_report(
    metadata: &SourceMetadata,
    config: &CensusSourceConfig,
    query: &CensusDataQuery,
    body: &Bytes,
    capture: &ProviderCaptureSetReceipt,
    rate_headers: &CensusRateLimitHeaders,
    received_at: Timestamp,
    latency: Duration,
) -> Result<CensusDoctorReport, CensusSourceError> {
    if body.len() > CENSUS_DOCTOR_MAX_RESPONSE_BYTES
        || capture.pages().len() != 1
        || capture.request_set_identity() != evidence_digest(query.request_digest())
    {
        return Err(CensusSourceError::Protocol);
    }
    let matrix = serde_json::from_slice::<Value>(body)
        .map_err(|_| CensusSourceError::Protocol)?
        .as_array()
        .cloned()
        .ok_or(CensusSourceError::Protocol)?;
    if matrix.len() != 2 {
        return Err(CensusSourceError::Protocol);
    }
    let header = matrix[0].as_array().ok_or(CensusSourceError::Protocol)?;
    let row = matrix[1].as_array().ok_or(CensusSourceError::Protocol)?;
    if header.len() != 3
        || row.len() != 3
        || header[0].as_str() != Some("NAME")
        || header[1].as_str() != Some("B01001_001E")
        || header[2].as_str() != Some("us")
        || row[0].as_str() != Some("United States")
        || row[2].as_str() != Some("1")
    {
        return Err(CensusSourceError::Protocol);
    }
    let population = row[1]
        .as_str()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(CensusSourceError::Protocol)?;
    let budget = metadata
        .budget_policy()
        .ok_or(CensusSourceError::InvalidMetadata)?;
    let subject = budget
        .scope()
        .authorization_account()
        .ok_or(CensusSourceError::InvalidMetadata)?;
    let rate = census_provider_rate_declaration(subject)?;
    if rate.policy() != budget {
        return Err(CensusSourceError::InvalidMetadata);
    }
    let response_digest = evidence_digest(crate::sha256(body));
    let response_bytes = u64::try_from(body.len()).map_err(|_| CensusSourceError::Protocol)?;
    let latency_nanos = u64::try_from(latency.as_nanos())
        .map_err(|_| CensusSourceError::TelemetryOverflow)?;
    let rate_header_evidence = CensusDoctorRateHeaderEvidence {
        limit: parse_numeric_header(rate_headers.limit.as_deref())?,
        remaining: parse_numeric_header(rate_headers.remaining.as_deref())?,
        reset: parse_numeric_header(rate_headers.reset.as_deref())?,
    };
    let native_schema = SourceIdentifier::try_from("census-json-matrix-acs1-us-population")
        .map_err(|_| CensusSourceError::Protocol)?;
    let native_schema_version = 1;
    let native_schema_fingerprint = native_schema_fingerprint();
    let page = capture.pages().first().ok_or(CensusSourceError::Protocol)?;
    if page.received_at() != received_at || page.body_digest() != response_digest {
        return Err(CensusSourceError::Protocol);
    }
    let mut report = CensusDoctorReport {
        readiness: CensusDoctorReadiness::Available,
        scope: CensusDoctorScope::Acs2024OneYearUnitedStatesPopulation,
        source_id: metadata.source_id().clone(),
        metadata_revision: metadata.revision().as_source_identifier().clone(),
        configuration_digest: config.configuration_digest(),
        credential_generation_digest: config.credential_generation_digest(),
        owner_authorization_digest: config.owner_authorization().authorization_digest(),
        presentation_obligation_digest: config
            .owner_authorization()
            .presentation_obligation_digest(),
        provider_rate_declaration_digest: rate.declaration_digest(),
        request_digest: evidence_digest(query.request_digest()),
        capture_observation_digest: capture.observation_digest(),
        response_digest,
        response_status: 200,
        response_bytes,
        latency_nanos,
        checked_at: received_at,
        population,
        row_count: 1,
        column_count: 3,
        rate_header_evidence,
        native_schema,
        native_schema_version,
        native_schema_fingerprint,
        non_endorsement_notice_required: config
            .owner_authorization()
            .requires_non_endorsement_notice(),
        report_digest: evidence_digest([0; 32]),
    };
    report.report_digest = report_digest(&report)?;
    Ok(report)
}

fn report_digest(report: &CensusDoctorReport) -> Result<EvidenceDigest, CensusSourceError> {
    let wire = serde_json::to_vec(&CensusDoctorDigestWire {
        readiness: report.readiness,
        scope: report.scope,
        source_id: &report.source_id,
        metadata_revision: &report.metadata_revision,
        configuration_digest: report.configuration_digest,
        credential_generation_digest: report.credential_generation_digest,
        owner_authorization_digest: report.owner_authorization_digest,
        presentation_obligation_digest: report.presentation_obligation_digest,
        provider_rate_declaration_digest: report.provider_rate_declaration_digest,
        request_digest: report.request_digest,
        capture_observation_digest: report.capture_observation_digest,
        response_digest: report.response_digest,
        response_status: report.response_status,
        response_bytes: report.response_bytes,
        latency_nanos: report.latency_nanos,
        checked_at: report.checked_at,
        population: report.population,
        row_count: report.row_count,
        column_count: report.column_count,
        rate_header_evidence: report.rate_header_evidence,
        native_schema: &report.native_schema,
        native_schema_version: report.native_schema_version,
        native_schema_fingerprint: report.native_schema_fingerprint,
        non_endorsement_notice_required: report.non_endorsement_notice_required,
    })
    .map_err(|_| CensusSourceError::Protocol)?;
    let mut digest = Sha256::new();
    update_digest_component(&mut digest, b"market-squawk/census-doctor-report/v2");
    update_digest_component(&mut digest, &wire);
    Ok(evidence_digest(digest.finalize().into()))
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct CensusDoctorDigestWire<'a> {
    readiness: CensusDoctorReadiness,
    scope: CensusDoctorScope,
    source_id: &'a SourceId,
    metadata_revision: &'a SourceIdentifier,
    configuration_digest: EvidenceDigest,
    credential_generation_digest: EvidenceDigest,
    owner_authorization_digest: EvidenceDigest,
    presentation_obligation_digest: EvidenceDigest,
    provider_rate_declaration_digest: EvidenceDigest,
    request_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    response_digest: EvidenceDigest,
    response_status: u16,
    response_bytes: u64,
    latency_nanos: u64,
    checked_at: Timestamp,
    population: u64,
    row_count: u32,
    column_count: u32,
    rate_header_evidence: CensusDoctorRateHeaderEvidence,
    native_schema: &'a SourceIdentifier,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
    non_endorsement_notice_required: bool,
}

fn parse_numeric_header(value: Option<&[u8]>) -> Result<Option<u64>, CensusSourceError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() || value.len() > 20 || !value.iter().all(u8::is_ascii_digit) {
        return Err(CensusSourceError::Protocol);
    }
    let value = std::str::from_utf8(value)
        .map_err(|_| CensusSourceError::Protocol)?
        .parse::<u64>()
        .map_err(|_| CensusSourceError::Protocol)?;
    Ok(Some(value))
}

fn native_schema_fingerprint() -> EvidenceDigest {
    let mut digest = Sha256::new();
    update_digest_component(
        &mut digest,
        b"market-squawk/census-native-doctor-schema/v1",
    );
    update_digest_component(&mut digest, b"NAME:utf8:exact=United States");
    update_digest_component(&mut digest, b"B01001_001E:u64:positive");
    update_digest_component(&mut digest, b"us:utf8:exact=1");
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn evidence_digest(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}
