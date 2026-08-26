//! Redacted real-provider doctor evidence joined to the shared raw request-graph seal.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::ProviderCaptureMaterial;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    BeaCompleteness, BeaDatasetAcquisition, BeaMethod, BeaProviderQuotaDeclaration,
    BeaSealedAcquisitionReceipt, BeaSourceBinding, BeaSourceError,
};

/// Code-owned lifetime of one successful in-process BEA doctor admission.
///
/// This is a freshness rule only. It is not a durable restart checkpoint; root composition must
/// own durable provider state and must either rejoin trusted persisted evidence or rerun doctor.
pub const BEA_DOCTOR_ADMISSION_VALIDITY_NANOS: i64 = 86_400_000_000_000;

/// Doctor evidence construction failure after transport/parser success.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaDoctorError {
    /// Source binding, quota, dataset, or capture evidence did not match.
    #[error("invalid BEA doctor authority")]
    InvalidAuthority,
    /// Page clocks, counts, order, completeness, or checked totals were invalid.
    #[error("invalid BEA doctor evidence")]
    InvalidEvidence,
    /// The sealed doctor evidence is no longer current for runtime admission.
    #[error("BEA doctor evidence expired")]
    Expired,
}

/// One exact credential-free response observation in doctor request order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDoctorPageEvidence {
    method: BeaMethod,
    request_identity: EvidenceDigest,
    upstream_response_digest: EvidenceDigest,
    response_digest: EvidenceDigest,
    status: u16,
    response_bytes: u64,
    latency_nanos: u64,
    received_at: Timestamp,
    returned_rows: u64,
    missing_rows: Option<u64>,
    completeness: BeaCompleteness,
}

impl BeaDoctorPageEvidence {
    /// Returns the exact official BEA method.
    pub const fn method(&self) -> BeaMethod {
        self.method
    }

    /// Returns the credential-free request commitment.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns SHA-256 of the exact provider body before validated echo redaction.
    pub const fn upstream_response_digest(&self) -> EvidenceDigest {
        self.upstream_response_digest
    }

    /// Returns the retained secret-free response-body commitment.
    pub const fn response_digest(&self) -> EvidenceDigest {
        self.response_digest
    }

    /// Returns the exact HTTP status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns exact bounded response bytes.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns transport latency.
    pub const fn latency_nanos(&self) -> u64 {
        self.latency_nanos
    }

    /// Returns when the complete body reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns provider rows for this method response.
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }

    /// Returns known absent rows when a count was configured.
    pub const fn missing_rows(&self) -> Option<u64> {
        self.missing_rows
    }

    /// Returns the response cardinality disposition.
    pub const fn completeness(&self) -> BeaCompleteness {
        self.completeness
    }
}

/// Secret-free evidence that the complete metadata-first official BEA journey succeeded.
///
/// This receipt alone is diagnostic. It becomes runtime-admissible only through
/// [`Self::bind_sealed`], which requires the actual shared physical request-graph receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDoctorReceipt {
    dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    source_binding_digest: EvidenceDigest,
    quota_declaration_digest: EvidenceDigest,
    metadata_generation: EvidenceDigest,
    pages: Vec<BeaDoctorPageEvidence>,
    request_count: u32,
    total_response_bytes: u64,
    returned_rows: u64,
    missing_rows: Option<u64>,
    data_completeness: BeaCompleteness,
    source_production_time: Option<Timestamp>,
    verified_at: Timestamp,
    receipt_digest: EvidenceDigest,
}

impl BeaDoctorReceipt {
    /// Builds a complete receipt from already validated official acquisition evidence.
    pub(crate) fn try_from_acquisition(
        binding: &BeaSourceBinding,
        quota: &BeaProviderQuotaDeclaration,
        dataset_id: SourceIdentifier,
        analytical_dataset_id: SourceIdentifier,
        acquisition: &BeaDatasetAcquisition,
        verified_at: Timestamp,
    ) -> Result<Self, BeaDoctorError> {
        if quota.declaration_digest() != binding.quota_declaration_digest()
            || acquisition.metadata().dataset_id() != &dataset_id
            || acquisition.data().page().receipt().completeness() == BeaCompleteness::Partial
        {
            return Err(BeaDoctorError::InvalidAuthority);
        }
        let expected_pages = acquisition.metadata().pages().len().saturating_add(1);
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(expected_pages)
            .map_err(|_| BeaDoctorError::InvalidEvidence)?;
        for captured in acquisition.metadata().pages() {
            pages.push(page_evidence(
                captured.telemetry().method(),
                captured.telemetry(),
                captured.page().receipt(),
                captured.material(),
            )?);
        }
        pages.push(page_evidence(
            acquisition.data().telemetry().method(),
            acquisition.data().telemetry(),
            acquisition.data().page().receipt(),
            acquisition.data().material(),
        )?);
        if pages.len() != expected_pages
            || pages.iter().any(|page| page.received_at > verified_at)
            || acquisition
                .data()
                .page()
                .production_time()
                .is_some_and(|production| {
                    pages
                        .last()
                        .is_none_or(|page| production.timestamp() > page.received_at)
                })
        {
            return Err(BeaDoctorError::InvalidEvidence);
        }
        let request_count =
            u32::try_from(pages.len()).map_err(|_| BeaDoctorError::InvalidEvidence)?;
        let total_response_bytes = pages.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.response_bytes)
                .ok_or(BeaDoctorError::InvalidEvidence)
        })?;
        let data = pages.last().ok_or(BeaDoctorError::InvalidEvidence)?;
        let returned_rows = data.returned_rows;
        let missing_rows = data.missing_rows;
        let data_completeness = data.completeness;
        let metadata_generation = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            acquisition.metadata().generation().digest(),
        );
        let source_production_time = acquisition
            .data()
            .page()
            .production_time()
            .map(|production| production.timestamp());
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-doctor-receipt/v4");
        hash_text(&mut hasher, dataset_id.as_str())?;
        hash_text(&mut hasher, analytical_dataset_id.as_str())?;
        for digest in [
            binding.binding_digest(),
            quota.declaration_digest(),
            metadata_generation,
        ] {
            hasher.update(digest.bytes());
        }
        hasher.update(request_count.to_be_bytes());
        hasher.update(total_response_bytes.to_be_bytes());
        for page in &pages {
            hash_text(&mut hasher, page.method.as_str())?;
            hasher.update(page.request_identity.bytes());
            hasher.update(page.upstream_response_digest.bytes());
            hasher.update(page.response_digest.bytes());
            hasher.update(page.status.to_be_bytes());
            hasher.update(page.response_bytes.to_be_bytes());
            hasher.update(page.latency_nanos.to_be_bytes());
            hasher.update(page.received_at.unix_nanos().to_be_bytes());
            hasher.update(page.returned_rows.to_be_bytes());
            match page.missing_rows {
                Some(missing) => {
                    hasher.update([1]);
                    hasher.update(missing.to_be_bytes());
                }
                None => hasher.update([0]),
            }
            hasher.update([completeness_tag(page.completeness)]);
        }
        match source_production_time {
            Some(production) => {
                hasher.update([1]);
                hasher.update(production.unix_nanos().to_be_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update(verified_at.unix_nanos().to_be_bytes());
        let receipt_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            dataset_id,
            analytical_dataset_id,
            source_binding_digest: binding.binding_digest(),
            quota_declaration_digest: quota.declaration_digest(),
            metadata_generation,
            pages,
            request_count,
            total_response_bytes,
            returned_rows,
            missing_rows,
            data_completeness,
            source_production_time,
            verified_at,
            receipt_digest,
        })
    }

    /// Rejoins this diagnostic receipt with the actual shared physical request-graph receipt.
    ///
    /// The returned evidence is non-serializable in-process readiness. It cannot restore itself,
    /// mint an immutable dataset generation, or attest a point-in-time query.
    pub fn bind_sealed(
        self,
        binding: &BeaSourceBinding,
        sealed: &BeaSealedAcquisitionReceipt,
    ) -> Result<BeaDoctorAdmissionEvidence, BeaDoctorError> {
        if self.source_binding_digest != binding.binding_digest()
            || self.quota_declaration_digest != binding.quota_declaration_digest()
            || sealed.source_id() != binding.source_id()
            || sealed.metadata_revision() != binding.metadata_revision()
            || sealed.dataset_id() != &self.dataset_id
            || sealed.evidence().metadata().generation().digest()
                != self.metadata_generation.bytes()
            || sealed.evidence().expected_capture_count() != self.pages.len()
            || sealed
                .data_response_digest()
                .map_err(|_| BeaDoctorError::InvalidEvidence)?
                != self
                    .pages
                    .last()
                    .map(|page| page.response_digest)
                    .ok_or(BeaDoctorError::InvalidEvidence)?
        {
            return Err(BeaDoctorError::InvalidAuthority);
        }
        for (ordinal, expected) in self.pages.iter().enumerate() {
            let capture = sealed
                .evidence()
                .expected_capture(ordinal)
                .ok_or(BeaDoctorError::InvalidEvidence)?;
            let page = capture
                .pages()
                .first()
                .filter(|_| capture.pages().len() == 1)
                .ok_or(BeaDoctorError::InvalidEvidence)?;
            if capture.request_set_identity() != expected.request_identity
                || page.request_identity() != expected.request_identity
                || sealed
                    .evidence()
                    .expected_upstream_response_digest(ordinal)
                    .ok_or(BeaDoctorError::InvalidEvidence)?
                    != expected.upstream_response_digest
                || page.body_digest() != expected.response_digest
                || page.http_status() != expected.status
                || page.body_bytes() != expected.response_bytes
                || page.received_at() != expected.received_at
            {
                return Err(BeaDoctorError::InvalidEvidence);
            }
        }
        if self.data_completeness == BeaCompleteness::Partial {
            return Err(BeaDoctorError::InvalidEvidence);
        }
        let expires_at = self
            .verified_at
            .checked_add_nanos(BEA_DOCTOR_ADMISSION_VALIDITY_NANOS)
            .map_err(|_| BeaDoctorError::InvalidEvidence)?;
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-doctor-admission/v2");
        hash_text(&mut hasher, binding.source_id().as_str())?;
        hash_text(
            &mut hasher,
            binding.metadata_revision().as_source_identifier().as_str(),
        )?;
        hash_text(&mut hasher, self.dataset_id.as_str())?;
        hash_text(&mut hasher, self.analytical_dataset_id.as_str())?;
        for digest in [
            binding.binding_digest(),
            self.receipt_digest,
            sealed.sealed_graph_digest(),
            self.metadata_generation,
            self.quota_declaration_digest,
        ] {
            hasher.update(digest.bytes());
        }
        hasher.update(self.verified_at.unix_nanos().to_be_bytes());
        hasher.update(expires_at.unix_nanos().to_be_bytes());
        let admission_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(BeaDoctorAdmissionEvidence {
            source_id: binding.source_id().clone(),
            metadata_revision: binding.metadata_revision().clone(),
            dataset_id: self.dataset_id,
            analytical_dataset_id: self.analytical_dataset_id,
            source_binding_digest: binding.binding_digest(),
            quota_declaration_digest: self.quota_declaration_digest,
            metadata_generation: self.metadata_generation,
            doctor_receipt_digest: self.receipt_digest,
            doctor_sealed_graph_digest: sealed.sealed_graph_digest(),
            verified_at: self.verified_at,
            expires_at,
            returned_rows: self.returned_rows,
            missing_rows: self.missing_rows,
            completeness: self.data_completeness,
            admission_digest,
        })
    }

    /// Returns the configured provider-query identity.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the analytical publication identity.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }

    /// Returns exact page evidence in network request order.
    pub fn pages(&self) -> &[BeaDoctorPageEvidence] {
        &self.pages
    }

    /// Returns successful official request count.
    pub const fn request_count(&self) -> u32 {
        self.request_count
    }

    /// Returns total exact response-body bytes.
    pub const fn total_response_bytes(&self) -> u64 {
        self.total_response_bytes
    }

    /// Returns validated data rows.
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }

    /// Returns known missing rows.
    pub const fn missing_rows(&self) -> Option<u64> {
        self.missing_rows
    }

    /// Returns data-page completeness.
    pub const fn data_completeness(&self) -> BeaCompleteness {
        self.data_completeness
    }

    /// Returns BEA response production time when supplied.
    pub const fn source_production_time(&self) -> Option<Timestamp> {
        self.source_production_time
    }

    /// Returns local completion time.
    pub const fn verified_at(&self) -> Timestamp {
        self.verified_at
    }

    /// Returns the complete secret-free doctor receipt commitment.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Non-serializable runtime evidence that an exact doctor result was physically sealed.
///
/// This type can gate provider work in the current process. It deliberately has no restore or
/// publication constructor and contains no manifest or immutable-generation coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDoctorAdmissionEvidence {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset_id: SourceIdentifier,
    analytical_dataset_id: SourceIdentifier,
    source_binding_digest: EvidenceDigest,
    quota_declaration_digest: EvidenceDigest,
    metadata_generation: EvidenceDigest,
    doctor_receipt_digest: EvidenceDigest,
    doctor_sealed_graph_digest: EvidenceDigest,
    verified_at: Timestamp,
    expires_at: Timestamp,
    returned_rows: u64,
    missing_rows: Option<u64>,
    completeness: BeaCompleteness,
    admission_digest: EvidenceDigest,
}

impl BeaDoctorAdmissionEvidence {
    /// Validates this in-process admission against the current source and wall-clock observation.
    pub fn validate_current(
        &self,
        binding: &BeaSourceBinding,
        dataset_id: &SourceIdentifier,
        analytical_dataset_id: &SourceIdentifier,
        observed_at: Timestamp,
    ) -> Result<(), BeaDoctorError> {
        if &self.source_id != binding.source_id()
            || &self.metadata_revision != binding.metadata_revision()
            || self.source_binding_digest != binding.binding_digest()
            || self.quota_declaration_digest != binding.quota_declaration_digest()
            || &self.dataset_id != dataset_id
            || &self.analytical_dataset_id != analytical_dataset_id
        {
            return Err(BeaDoctorError::InvalidAuthority);
        }
        if observed_at < self.verified_at || observed_at >= self.expires_at {
            return Err(BeaDoctorError::Expired);
        }
        Ok(())
    }

    /// Returns the exact source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the configured provider-query contract identity.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the canonical analytical dataset target.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }

    /// Returns the exact source binding commitment.
    pub const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }

    /// Returns the complete quota declaration commitment.
    pub const fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.quota_declaration_digest
    }

    /// Returns the metadata generation observed during doctor.
    pub const fn metadata_generation(&self) -> EvidenceDigest {
        self.metadata_generation
    }

    /// Returns the redacted doctor receipt commitment.
    pub const fn doctor_receipt_digest(&self) -> EvidenceDigest {
        self.doctor_receipt_digest
    }

    /// Returns the actual shared request-graph seal used to admit doctor.
    pub const fn doctor_sealed_graph_digest(&self) -> EvidenceDigest {
        self.doctor_sealed_graph_digest
    }

    /// Returns when the successful doctor completed.
    pub const fn verified_at(&self) -> Timestamp {
        self.verified_at
    }

    /// Returns the exclusive in-process refresh deadline.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns validated data rows from the doctor request.
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }

    /// Returns known missing rows from the doctor request.
    pub const fn missing_rows(&self) -> Option<u64> {
        self.missing_rows
    }

    /// Returns the doctor data-page completeness state.
    pub const fn completeness(&self) -> BeaCompleteness {
        self.completeness
    }

    /// Returns the complete actual-seal-bound doctor admission commitment.
    pub const fn admission_digest(&self) -> EvidenceDigest {
        self.admission_digest
    }
}

/// Successful doctor receipt and every raw response awaiting one shared graph seal.
#[derive(Debug)]
pub struct BeaDoctorRun {
    receipt: BeaDoctorReceipt,
    acquisition: BeaDatasetAcquisition,
}

impl BeaDoctorRun {
    /// Binds a validated acquisition into one complete diagnostic doctor result.
    pub(crate) fn try_new(
        binding: &BeaSourceBinding,
        quota: &BeaProviderQuotaDeclaration,
        dataset_id: SourceIdentifier,
        analytical_dataset_id: SourceIdentifier,
        acquisition: BeaDatasetAcquisition,
        verified_at: Timestamp,
    ) -> Result<Self, BeaDoctorError> {
        let receipt = BeaDoctorReceipt::try_from_acquisition(
            binding,
            quota,
            dataset_id,
            analytical_dataset_id,
            &acquisition,
            verified_at,
        )?;
        Ok(Self {
            receipt,
            acquisition,
        })
    }

    /// Returns the redacted diagnostic doctor receipt.
    pub const fn receipt(&self) -> &BeaDoctorReceipt {
        &self.receipt
    }

    /// Returns the typed official acquisition and raw-capture material.
    pub const fn acquisition(&self) -> &BeaDatasetAcquisition {
        &self.acquisition
    }

    /// Consumes the run into evidence and one exact request graph for the shared `MSJ1` sealer.
    pub fn into_sealing_parts(
        self,
    ) -> Result<
        (
            BeaDoctorReceipt,
            crate::BeaDatasetEvidence,
            ProviderCaptureMaterial,
        ),
        BeaSourceError,
    > {
        let (evidence, graph) = self.acquisition.into_sealing_parts()?;
        Ok((self.receipt, evidence, graph))
    }
}

fn page_evidence(
    method: BeaMethod,
    telemetry: &crate::BeaResponseTelemetry,
    receipt: &crate::BeaPageReceipt,
    material: &ProviderCaptureMaterial,
) -> Result<BeaDoctorPageEvidence, BeaDoctorError> {
    let capture = material.receipt();
    let page = capture
        .pages()
        .first()
        .filter(|_| capture.pages().len() == 1 && material.records().len() == 1)
        .ok_or(BeaDoctorError::InvalidEvidence)?;
    if telemetry.status() != page.http_status()
        || telemetry.response_bytes() != page.body_bytes()
        || telemetry.request_identity() != page.request_identity()
        || receipt.request_digest() != telemetry.request_identity().bytes()
        || receipt.response_digest() != page.body_digest().bytes()
        || receipt.upstream_response_digest() == receipt.response_digest()
    {
        return Err(BeaDoctorError::InvalidEvidence);
    }
    Ok(BeaDoctorPageEvidence {
        method,
        request_identity: telemetry.request_identity(),
        upstream_response_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            receipt.upstream_response_digest(),
        ),
        response_digest: page.body_digest(),
        status: telemetry.status(),
        response_bytes: telemetry.response_bytes(),
        latency_nanos: telemetry.latency_nanos(),
        received_at: page.received_at(),
        returned_rows: telemetry.returned_rows(),
        missing_rows: telemetry.missing_rows(),
        completeness: telemetry.completeness(),
    })
}

fn completeness_tag(value: BeaCompleteness) -> u8 {
    match value {
        BeaCompleteness::Complete => 1,
        BeaCompleteness::Partial => 2,
        BeaCompleteness::ExpectedCountUnknown => 3,
    }
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaDoctorError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaDoctorError::InvalidEvidence)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
