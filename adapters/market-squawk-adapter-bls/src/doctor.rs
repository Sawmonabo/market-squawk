//! Redacted, real-request BLS provider doctor evidence and bounded raw capture.

use std::sync::Arc;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ProviderCaptureMaterial, ProviderCaptureTerminalDisposition,
};
use sha2::{Digest as _, Sha256};

use crate::contract::BlsRuntimeInstanceCapability;
use crate::{
    BlsAccessTier, BlsCredentialRejoin, BlsRequestLimits, BlsRootRightsRejoin,
    BlsSourceError,
};

/// Closed readiness result for one exact BLS doctor request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlsDoctorReadiness {
    /// The exact requested series returned at least one observed value without provider messages.
    Available,
    /// The request contract succeeded but returned provider messages or explicit missing values.
    Degraded,
    /// The exact requested series returned no observation rows or no observed values.
    Unavailable,
}

/// Redacted semantic evidence from one bounded official BLS POST.
///
/// Exact response bytes live only in the paired [`BlsDoctorOutput::capture_material`], ready for
/// application-owned physical sealing. This report never contains the registered-v2 key, request
/// body, provider message text, a manifest, or a claimed analytical generation.
#[derive(Debug)]
pub struct BlsDoctorReport {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    tier: BlsAccessTier,
    dataset: SourceIdentifier,
    series_id: SourceIdentifier,
    year: u16,
    readiness: BlsDoctorReadiness,
    returned_series: u16,
    returned_observations: u32,
    observed_values: u32,
    missing_values: u32,
    preliminary_values: u32,
    footnotes: u32,
    provider_messages: u16,
    provider_response_time_millis: u64,
    received_at: Timestamp,
    response_bytes: u64,
    response_content_digest: EvidenceDigest,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    request_set_identity: EvidenceDigest,
    provider_usage_policy_digest: EvidenceDigest,
    root_rights_rejoin: BlsRootRightsRejoin,
    credential_rejoin: BlsCredentialRejoin,
    presentation_obligation_digest: EvidenceDigest,
    provider_rate_declaration_digest: EvidenceDigest,
    limits: BlsRequestLimits,
    report_digest: EvidenceDigest,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
}

impl BlsDoctorReport {
    #[allow(
        clippy::too_many_arguments,
        reason = "the doctor receipt keeps every bounded provider disposition explicit"
    )]
    pub(crate) fn new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        tier: BlsAccessTier,
        dataset: SourceIdentifier,
        series_id: SourceIdentifier,
        year: u16,
        readiness: BlsDoctorReadiness,
        returned_series: u16,
        returned_observations: u32,
        observed_values: u32,
        missing_values: u32,
        preliminary_values: u32,
        footnotes: u32,
        provider_messages: u16,
        provider_response_time_millis: u64,
        received_at: Timestamp,
        response_bytes: u64,
        response_content_digest: EvidenceDigest,
        capture_content_digest: EvidenceDigest,
        capture_observation_digest: EvidenceDigest,
        request_set_identity: EvidenceDigest,
        provider_usage_policy_digest: EvidenceDigest,
        root_rights_rejoin: BlsRootRightsRejoin,
        credential_rejoin: BlsCredentialRejoin,
        presentation_obligation_digest: EvidenceDigest,
        provider_rate_declaration_digest: EvidenceDigest,
        limits: BlsRequestLimits,
        runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<Self, BlsSourceError> {
        let mut report = Self {
            source_id,
            metadata_revision,
            tier,
            dataset,
            series_id,
            year,
            readiness,
            returned_series,
            returned_observations,
            observed_values,
            missing_values,
            preliminary_values,
            footnotes,
            provider_messages,
            provider_response_time_millis,
            received_at,
            response_bytes,
            response_content_digest,
            capture_content_digest,
            capture_observation_digest,
            request_set_identity,
            provider_usage_policy_digest,
            root_rights_rejoin,
            credential_rejoin,
            presentation_obligation_digest,
            provider_rate_declaration_digest,
            limits,
            report_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            runtime_instance,
        };
        report.report_digest = report.compute_digest()?;
        Ok(report)
    }

    /// Returns the exact registered source exercised by the doctor request.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the immutable source-metadata revision exercised by the doctor request.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact public-v1 or registered-v2 surface exercised by the request.
    pub const fn tier(&self) -> BlsAccessTier {
        self.tier
    }

    /// Returns the complete request-plan identity of the source under diagnosis.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact configured series used for the bounded probe.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the one inclusive year requested by the bounded probe.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns semantic readiness derived from provider rows, values, and messages.
    pub const fn readiness(&self) -> BlsDoctorReadiness {
        self.readiness
    }

    /// Returns the exact number of series returned after requested-set validation.
    pub const fn returned_series(&self) -> u16 {
        self.returned_series
    }

    /// Returns all provider observation rows, including explicit missing values.
    pub const fn returned_observations(&self) -> u32 {
        self.returned_observations
    }

    /// Returns rows carrying an exact decimal observation.
    pub const fn observed_values(&self) -> u32 {
        self.observed_values
    }

    /// Returns rows carrying the provider's explicit missing marker.
    pub const fn missing_values(&self) -> u32 {
        self.missing_values
    }

    /// Returns rows marked preliminary by provider footnote `P`.
    pub const fn preliminary_values(&self) -> u32 {
        self.preliminary_values
    }

    /// Returns the total number of retained provider footnotes.
    pub const fn footnotes(&self) -> u32 {
        self.footnotes
    }

    /// Returns the number of provider messages without exposing arbitrary provider text.
    pub const fn provider_messages(&self) -> u16 {
        self.provider_messages
    }

    /// Returns the provider-reported processing time.
    pub const fn provider_response_time_millis(&self) -> u64 {
        self.provider_response_time_millis
    }

    /// Returns the socket-boundary receipt time for the exact response bytes.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the exact bounded response byte count.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns the SHA-256 identity of the exact official response bytes.
    pub const fn response_content_digest(&self) -> EvidenceDigest {
        self.response_content_digest
    }

    /// Returns the stable provider-capture identity excluding receipt time.
    pub const fn capture_content_digest(&self) -> EvidenceDigest {
        self.capture_content_digest
    }

    /// Returns the receive-time-bound provider-capture identity.
    pub const fn capture_observation_digest(&self) -> EvidenceDigest {
        self.capture_observation_digest
    }

    /// Returns the exact secret-free doctor request identity retained by the capture.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns the fixed provider-local private-use/no-distribution policy.
    pub const fn provider_usage_policy_digest(&self) -> EvidenceDigest {
        self.provider_usage_policy_digest
    }

    /// Returns the non-authoritative root rights coordinate used by this source instance.
    pub const fn root_rights_rejoin(&self) -> BlsRootRightsRejoin {
        self.root_rights_rejoin
    }

    /// Returns the explicit public marker or registered protected-generation coordinate.
    pub const fn credential_rejoin(&self) -> BlsCredentialRejoin {
        self.credential_rejoin
    }

    /// Returns the exact BLS attribution and disclaimer duties joined by product reads.
    pub const fn presentation_obligation_digest(&self) -> EvidenceDigest {
        self.presentation_obligation_digest
    }

    /// Returns the exact shared provider-rate declaration used by this attempt.
    pub const fn provider_rate_declaration_digest(&self) -> EvidenceDigest {
        self.provider_rate_declaration_digest
    }

    /// Returns provider facts and conservative application ceilings used by the source.
    pub const fn limits(&self) -> BlsRequestLimits {
        self.limits
    }

    /// Returns the complete domain-separated identity of this redacted doctor report.
    pub const fn report_digest(&self) -> EvidenceDigest {
        self.report_digest
    }

    pub(crate) fn validate(&self) -> Result<(), BlsSourceError> {
        if self.source_id.as_str().is_empty()
            || self.metadata_revision.as_source_identifier().as_str().is_empty()
            || self.dataset.as_str().is_empty()
            || self.series_id.as_str().is_empty()
            || self.year == 0
            || self.response_bytes == 0
            || self.response_content_digest.bytes() == [0; 32]
            || self.capture_content_digest.bytes() == [0; 32]
            || self.capture_observation_digest.bytes() == [0; 32]
            || self.request_set_identity.bytes() == [0; 32]
            || self.provider_usage_policy_digest.bytes() == [0; 32]
            || self.root_rights_rejoin.validate().is_err()
            || self.presentation_obligation_digest.bytes() == [0; 32]
            || self.provider_rate_declaration_digest.bytes() == [0; 32]
            || self.report_digest != self.compute_digest()?
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EvidenceDigest, BlsSourceError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/bls-doctor-report/v2\0");
        hash_report_field(&mut digest, self.source_id.as_str().as_bytes())?;
        hash_report_field(
            &mut digest,
            self.metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_report_field(
            &mut digest,
            match self.tier {
                BlsAccessTier::PublicV1 => b"public-v1".as_slice(),
                BlsAccessTier::RegisteredV2 => b"registered-v2".as_slice(),
            },
        )?;
        hash_report_field(&mut digest, self.dataset.as_str().as_bytes())?;
        hash_report_field(&mut digest, self.series_id.as_str().as_bytes())?;
        digest.update(self.year.to_be_bytes());
        hash_report_field(
            &mut digest,
            match self.readiness {
                BlsDoctorReadiness::Available => b"available".as_slice(),
                BlsDoctorReadiness::Degraded => b"degraded".as_slice(),
                BlsDoctorReadiness::Unavailable => b"unavailable".as_slice(),
            },
        )?;
        digest.update(self.returned_series.to_be_bytes());
        digest.update(self.returned_observations.to_be_bytes());
        digest.update(self.observed_values.to_be_bytes());
        digest.update(self.missing_values.to_be_bytes());
        digest.update(self.preliminary_values.to_be_bytes());
        digest.update(self.footnotes.to_be_bytes());
        digest.update(self.provider_messages.to_be_bytes());
        digest.update(self.provider_response_time_millis.to_be_bytes());
        digest.update(self.received_at.unix_nanos().to_be_bytes());
        digest.update(self.response_bytes.to_be_bytes());
        for value in [
            self.response_content_digest,
            self.capture_content_digest,
            self.capture_observation_digest,
            self.request_set_identity,
            self.provider_usage_policy_digest,
            self.root_rights_rejoin.root_decision_digest(),
            self.root_rights_rejoin.provider_policy_digest(),
            self.presentation_obligation_digest,
            self.provider_rate_declaration_digest,
        ] {
            hash_report_digest(&mut digest, value);
        }
        hash_credential_rejoin(&mut digest, self.credential_rejoin);
        digest.update(
            u64::try_from(self.limits.series_per_query())
                .map_err(|_| BlsSourceError::InvalidPublication)?
                .to_be_bytes(),
        );
        digest.update(self.limits.documented_years_per_query().to_be_bytes());
        digest.update(self.limits.enforced_years_per_query().to_be_bytes());
        digest.update(self.limits.documented_daily_queries().to_be_bytes());
        digest.update(self.limits.daily_queries().to_be_bytes());
        digest.update(self.limits.enforced_requests_per_second().to_be_bytes());
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }

    pub(crate) fn matches_runtime_instance(
        &self,
        expected: &Arc<BlsRuntimeInstanceCapability>,
    ) -> bool {
        Arc::ptr_eq(&self.runtime_instance, expected)
    }
}

/// Indivisible doctor result pairing redacted semantics with exact bounded raw evidence.
#[derive(Debug)]
pub struct BlsDoctorOutput {
    report: BlsDoctorReport,
    capture_material: ProviderCaptureMaterial,
}

impl BlsDoctorOutput {
    pub(crate) fn try_new(
        report: BlsDoctorReport,
        capture_material: ProviderCaptureMaterial,
    ) -> Result<Self, BlsSourceError> {
        report.validate()?;
        let capture = capture_material.receipt();
        let page = capture
            .pages()
            .first()
            .ok_or(BlsSourceError::InvalidPublication)?;
        if capture.source_id() != &report.source_id
            || capture.metadata_revision() != &report.metadata_revision
            || capture.dataset() != &report.dataset
            || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || capture.pages().len() != 1
            || !capture.request_graph_components().is_empty()
            || capture.total_body_bytes() != report.response_bytes
            || page.body_digest() != report.response_content_digest
            || page.received_at() != report.received_at
            || capture.content_digest() != report.capture_content_digest
            || capture.observation_digest() != report.capture_observation_digest
            || capture.request_set_identity() != report.request_set_identity
            || capture_material.records().len() != 1
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(Self {
            report,
            capture_material,
        })
    }

    /// Returns redacted semantic readiness and accounting evidence.
    pub const fn report(&self) -> &BlsDoctorReport {
        &self.report
    }

    /// Returns exact bounded response material ready for application-owned physical sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    /// Consumes the indivisible doctor result into semantic and raw-capture parts.
    pub fn into_parts(self) -> (BlsDoctorReport, ProviderCaptureMaterial) {
        (self.report, self.capture_material)
    }
}

fn hash_report_digest(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}

fn hash_credential_rejoin(digest: &mut Sha256, value: BlsCredentialRejoin) {
    match value {
        BlsCredentialRejoin::PublicNoCredential => digest.update(b"public-no-credential"),
        BlsCredentialRejoin::RegisteredGeneration(generation) => {
            digest.update(b"registered-generation");
            hash_report_digest(digest, generation);
        }
    }
}

fn hash_report_field(digest: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}
