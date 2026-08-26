//! Actual-seal-bound BEA canonical candidate and shared-publication rejoin coordinates.

use std::num::NonZeroU32;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision, SourceId,
    SourceIdentifier,
};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ExtractionRecord, ExtractionRevisionPlan,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::canonical::{BeaCanonicalBatch, BeaCanonicalContext};
use crate::{
    BeaCanonicalObservation, BeaDatasetIdentity, BeaDoctorAdmissionEvidence,
    BeaSealedExtractionOutput, BeaSourceBinding,
};

/// Provider-specific canonical-candidate or rejoin invariant failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaPublicationError {
    /// Doctor, source-binding, or raw-seal evidence did not match.
    #[error("invalid BEA publication-candidate authority")]
    InvalidAuthority,
    /// Candidate rows, bounds, clocks, or rejoin coordinates were inconsistent.
    #[error("invalid BEA publication-candidate evidence")]
    InvalidEvidence,
}

/// Exact coordinates the shared revision, Arrow, manifest, and catalog authorities must rejoin.
///
/// These coordinates are all derived from actual shared raw-seal receipts and closed canonical
/// rows. They intentionally contain no immutable generation, manifest, commit, restart, PIT-query,
/// or currentness assertion; only root composition can create those authorities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaPublicationRejoinCoordinates {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset_id: SourceIdentifier,
    provider_dataset: BeaDatasetIdentity,
    analytical_dataset_id: SourceIdentifier,
    source_binding_digest: EvidenceDigest,
    doctor_admission_digest: EvidenceDigest,
    doctor_sealed_graph_digest: EvidenceDigest,
    acquisition_sealed_graph_digest: EvidenceDigest,
    acquisition_capture_receipt_digest: EvidenceDigest,
    acquisition_physical_receipt_digest: EvidenceDigest,
    data_component_ordinal: u16,
    metadata_generation: EvidenceDigest,
    data_upstream_response_digest: EvidenceDigest,
    data_response_digest: EvidenceDigest,
    canonical_batch_digest: EvidenceDigest,
    row_count: u64,
    candidate_digest: EvidenceDigest,
}

impl BeaPublicationRejoinCoordinates {
    /// Returns the exact registered source.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source registration revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the configured provider-query contract identity.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the provider dataset echoed by `GetData`.
    pub const fn provider_dataset(&self) -> &BeaDatasetIdentity {
        &self.provider_dataset
    }

    /// Returns the intended shared analytical dataset family.
    pub const fn analytical_dataset_id(&self) -> &SourceIdentifier {
        &self.analytical_dataset_id
    }

    /// Returns the exact non-secret source binding.
    pub const fn source_binding_digest(&self) -> EvidenceDigest {
        self.source_binding_digest
    }

    /// Returns the actual-seal-bound doctor admission.
    pub const fn doctor_admission_digest(&self) -> EvidenceDigest {
        self.doctor_admission_digest
    }

    /// Returns the physical graph that admitted doctor in this process.
    pub const fn doctor_sealed_graph_digest(&self) -> EvidenceDigest {
        self.doctor_sealed_graph_digest
    }

    /// Returns the physical request graph containing these canonical rows.
    pub const fn acquisition_sealed_graph_digest(&self) -> EvidenceDigest {
        self.acquisition_sealed_graph_digest
    }

    /// Returns the shared seal receipt for the complete acquisition graph.
    pub const fn acquisition_capture_receipt_digest(&self) -> EvidenceDigest {
        self.acquisition_capture_receipt_digest
    }

    /// Returns the shared journal's exact physical segment receipt.
    pub const fn acquisition_physical_receipt_digest(&self) -> EvidenceDigest {
        self.acquisition_physical_receipt_digest
    }

    /// Returns the graph component containing the exact `GetData` response.
    pub const fn data_component_ordinal(&self) -> u16 {
        self.data_component_ordinal
    }

    /// Returns the metadata generation used to construct `GetData`.
    pub const fn metadata_generation(&self) -> EvidenceDigest {
        self.metadata_generation
    }

    /// Returns SHA-256 of the exact provider `GetData` body before echo redaction.
    pub const fn data_upstream_response_digest(&self) -> EvidenceDigest {
        self.data_upstream_response_digest
    }

    /// Returns the retained secret-free `GetData` response-body digest.
    pub const fn data_response_digest(&self) -> EvidenceDigest {
        self.data_response_digest
    }

    /// Returns the exact bounded canonical-row batch commitment.
    pub const fn canonical_batch_digest(&self) -> EvidenceDigest {
        self.canonical_batch_digest
    }

    /// Returns the exact number of canonical rows awaiting shared publication.
    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Returns the complete provider candidate commitment.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }
}

/// Canonical provider batch bound to one actual shared raw request-graph seal.
///
/// The contained `MacroObservation` values use revision one as a provisional normalization shape.
/// The shared observed-revision authority must assign and bind durable revisions before Arrow and
/// immutable dataset publication. This adapter exposes no API that accepts or certifies a caller-
/// supplied generation, manifest, commit clock, restart checkpoint, or PIT result.
#[derive(Debug)]
pub struct BeaPublicationCandidate {
    coordinates: BeaPublicationRejoinCoordinates,
    observations: Vec<BeaCanonicalObservation>,
    batch: ExtractionBatch,
    revision_plan: ExtractionRevisionPlan,
    sealed_output: BeaSealedExtractionOutput,
}

impl BeaPublicationCandidate {
    /// Builds a shared-publication candidate from the source output and its acquisition seal.
    pub fn try_new(
        binding: &BeaSourceBinding,
        doctor: &BeaDoctorAdmissionEvidence,
        sealed_output: BeaSealedExtractionOutput,
    ) -> Result<Self, BeaPublicationError> {
        let sealed_acquisition = sealed_output.sealed_acquisition();
        let maximum_records = NonZeroU32::new(sealed_output.source_batch().request().max_records())
            .ok_or(BeaPublicationError::InvalidEvidence)?;
        let canonicalized_at = crate::transport::system_timestamp()
            .map_err(|_| BeaPublicationError::InvalidAuthority)?;
        let context =
            BeaCanonicalContext::try_new(binding, doctor, &sealed_acquisition, canonicalized_at)
                .map_err(|_| BeaPublicationError::InvalidAuthority)?;
        let batch =
            BeaCanonicalBatch::try_from_sealed(&sealed_acquisition, &context, maximum_records)
                .map_err(|_| BeaPublicationError::InvalidEvidence)?;
        let (source_id, dataset_id, analytical_dataset_id, observations, canonical_batch_digest) =
            batch.into_parts();
        let first = observations
            .first()
            .ok_or(BeaPublicationError::InvalidEvidence)?;
        if &source_id != binding.source_id()
            || &dataset_id != sealed_acquisition.dataset_id()
            || &analytical_dataset_id != doctor.analytical_dataset_id()
            || first.source_binding_digest() != binding.binding_digest()
            || first.doctor_admission_digest() != doctor.admission_digest()
            || first.doctor_sealed_graph_digest() != doctor.doctor_sealed_graph_digest()
            || first.raw_seal_digest() != sealed_acquisition.sealed_graph_digest()
            || observations.iter().any(|observation| {
                observation.source_binding_digest() != first.source_binding_digest()
                    || observation.doctor_admission_digest() != first.doctor_admission_digest()
                    || observation.doctor_sealed_graph_digest()
                        != first.doctor_sealed_graph_digest()
                    || observation.raw_seal_digest() != first.raw_seal_digest()
                    || observation.metadata_generation() != first.metadata_generation()
                    || observation.upstream_response_digest() != first.upstream_response_digest()
                    || observation.raw_page_digest() != first.raw_page_digest()
            })
        {
            return Err(BeaPublicationError::InvalidAuthority);
        }
        let row_count =
            u64::try_from(observations.len()).map_err(|_| BeaPublicationError::InvalidEvidence)?;
        let revision_plan = ExtractionRevisionPlan::locally_observed(observations.len())
            .map_err(|_| BeaPublicationError::InvalidEvidence)?;
        let data_response_digest = sealed_acquisition
            .data_response_digest()
            .map_err(|_| BeaPublicationError::InvalidEvidence)?;
        let data_upstream_response_digest = sealed_acquisition
            .data_upstream_response_digest()
            .map_err(|_| BeaPublicationError::InvalidEvidence)?;
        if first.metadata_generation().bytes()
            != sealed_acquisition
                .evidence()
                .metadata()
                .generation()
                .digest()
            || first.upstream_response_digest() != data_upstream_response_digest
            || first.raw_page_digest() != data_response_digest
        {
            return Err(BeaPublicationError::InvalidEvidence);
        }
        let capture_receipt_digest = sealed_acquisition.sealed_capture().receipt_digest();
        let physical_receipt_digest = sealed_acquisition
            .sealed_capture()
            .segment()
            .physical_receipt_digest();
        let mut coordinates = BeaPublicationRejoinCoordinates {
            source_id,
            metadata_revision: binding.metadata_revision().clone(),
            dataset_id,
            provider_dataset: sealed_acquisition.provider_dataset().clone(),
            analytical_dataset_id,
            source_binding_digest: binding.binding_digest(),
            doctor_admission_digest: doctor.admission_digest(),
            doctor_sealed_graph_digest: doctor.doctor_sealed_graph_digest(),
            acquisition_sealed_graph_digest: sealed_acquisition.sealed_graph_digest(),
            acquisition_capture_receipt_digest: capture_receipt_digest,
            acquisition_physical_receipt_digest: physical_receipt_digest,
            data_component_ordinal: sealed_acquisition.data_component_ordinal(),
            metadata_generation: first.metadata_generation(),
            data_upstream_response_digest,
            data_response_digest,
            canonical_batch_digest,
            row_count,
            candidate_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        coordinates.candidate_digest = candidate_digest(
            &coordinates,
            &revision_plan,
            sealed_output.source_batch_digest(),
        )?;
        let canonical_batch = canonical_extraction_batch(
            sealed_output.source_batch(),
            &observations,
            sealed_output.sealed_acquisition(),
        )?;
        let candidate = Self {
            coordinates,
            observations,
            batch: canonical_batch,
            revision_plan,
            sealed_output,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    /// Revalidates every immutable provider/native/canonical/physical coordinate.
    pub fn validate(&self) -> Result<(), BeaPublicationError> {
        let coordinates = &self.coordinates;
        let sealed_acquisition = self.sealed_output.sealed_acquisition();
        if self.observations.is_empty()
            || self.revision_plan.len() != self.observations.len()
            || !self.revision_plan.is_locally_observed()
            || u64::try_from(self.observations.len())
                .map_err(|_| BeaPublicationError::InvalidEvidence)?
                != coordinates.row_count
            || self.sealed_output.source_batch().records().len() != self.observations.len()
            || self.batch.records().len() != self.observations.len()
            || sealed_acquisition.source_id() != &coordinates.source_id
            || sealed_acquisition.metadata_revision() != &coordinates.metadata_revision
            || sealed_acquisition.dataset_id() != &coordinates.dataset_id
            || sealed_acquisition.provider_dataset() != &coordinates.provider_dataset
            || sealed_acquisition.sealed_graph_digest()
                != coordinates.acquisition_sealed_graph_digest
            || sealed_acquisition.sealed_capture().receipt_digest()
                != coordinates.acquisition_capture_receipt_digest
            || sealed_acquisition
                .sealed_capture()
                .segment()
                .physical_receipt_digest()
                != coordinates.acquisition_physical_receipt_digest
            || sealed_acquisition.data_component_ordinal() != coordinates.data_component_ordinal
            || sealed_acquisition
                .data_response_digest()
                .map_err(|_| BeaPublicationError::InvalidEvidence)?
                != coordinates.data_response_digest
            || sealed_acquisition
                .data_upstream_response_digest()
                .map_err(|_| BeaPublicationError::InvalidEvidence)?
                != coordinates.data_upstream_response_digest
            || sealed_acquisition
                .evidence()
                .metadata()
                .generation()
                .digest()
                != coordinates.metadata_generation.bytes()
        {
            return Err(BeaPublicationError::InvalidEvidence);
        }
        if candidate_digest(
            coordinates,
            &self.revision_plan,
            self.sealed_output.source_batch_digest(),
        )? != coordinates.candidate_digest
        {
            return Err(BeaPublicationError::InvalidEvidence);
        }
        Ok(())
    }

    /// Returns exact root-owned publication rejoin coordinates.
    pub const fn rejoin_coordinates(&self) -> &BeaPublicationRejoinCoordinates {
        &self.coordinates
    }

    /// Returns the joined source batch, native evidence, and physical acquisition sidecar.
    pub const fn sealed_output(&self) -> &BeaSealedExtractionOutput {
        &self.sealed_output
    }

    /// Returns canonical rows in deterministic provider response order.
    pub fn observations(&self) -> &[BeaCanonicalObservation] {
        &self.observations
    }

    /// Returns the bounded one-for-one local-content revision input for shared authority.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Returns the complete provider candidate commitment.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.coordinates.candidate_digest
    }

    /// Consumes the candidate into a canonical batch and its retained source/physical sidecars.
    pub fn into_shared_publication_parts(self) -> BeaSharedPublicationParts {
        let Self {
            coordinates,
            observations: _,
            batch,
            revision_plan,
            sealed_output,
        } = self;
        BeaSharedPublicationParts {
            coordinates,
            batch,
            revision_plan,
            sealed_output,
        }
    }
}

/// Owned handoff to shared revision, Arrow, immutable-publication, and query composition.
#[derive(Debug)]
pub struct BeaSharedPublicationParts {
    coordinates: BeaPublicationRejoinCoordinates,
    batch: ExtractionBatch,
    revision_plan: ExtractionRevisionPlan,
    sealed_output: BeaSealedExtractionOutput,
}

impl BeaSharedPublicationParts {
    /// Returns exact root validation/rejoin coordinates.
    pub const fn coordinates(&self) -> &BeaPublicationRejoinCoordinates {
        &self.coordinates
    }

    /// Returns the exact canonical batch root must submit to shared ingestion.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns the exact bounded revision plan aligned with [`Self::batch`].
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Returns the original source batch, native dictionaries, and physical capture sidecar.
    pub const fn sealed_output(&self) -> &BeaSealedExtractionOutput {
        &self.sealed_output
    }

    /// Consumes the handoff into the only values shared composition needs.
    pub fn into_parts(
        self,
    ) -> (
        BeaPublicationRejoinCoordinates,
        ExtractionBatch,
        ExtractionRevisionPlan,
        BeaSealedExtractionOutput,
    ) {
        (
            self.coordinates,
            self.batch,
            self.revision_plan,
            self.sealed_output,
        )
    }
}

fn canonical_extraction_batch(
    source_batch: &ExtractionBatch,
    observations: &[BeaCanonicalObservation],
    sealed_acquisition: &crate::BeaSealedAcquisitionReceipt,
) -> Result<ExtractionBatch, BeaPublicationError> {
    if source_batch.records().len() != observations.len() || source_batch.records().is_empty() {
        return Err(BeaPublicationError::InvalidEvidence);
    }
    let request = source_batch.request();
    let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
        .map_err(|_| BeaPublicationError::InvalidEvidence)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(observations.len())
        .map_err(|_| BeaPublicationError::InvalidEvidence)?;
    for (source_record, observation) in source_batch.records().iter().zip(observations) {
        let context = observation.observation().context();
        let provenance = context.provenance();
        if source_record.effective_time() != context.time().effective()
            || source_record.published_time() != context.time().published()
            || source_record.superseded_time() != context.time().superseded()
            || source_record.available_at() != provenance.availability().conservative_available_at()
        {
            return Err(BeaPublicationError::InvalidEvidence);
        }
        records.push(
            ExtractionRecord::try_new_with_time(
                request,
                schema.clone(),
                ExactPayloadEvidence::from_content_digest(observation.canonical_payload_digest()),
                source_record.effective_time().clone(),
                source_record.published_time().cloned(),
                source_record.availability().clone(),
                source_record.revision().clone(),
                source_record.superseded_time().cloned(),
                observation.canonical_payload().clone(),
            )
            .map_err(|_| BeaPublicationError::InvalidEvidence)?,
        );
    }
    let batch = ExtractionBatch::try_new(request, records)
        .map_err(|_| BeaPublicationError::InvalidEvidence)?;
    batch
        .try_bind_provider_capture(sealed_acquisition.sealed_capture().capture())
        .map_err(|_| BeaPublicationError::InvalidEvidence)
}

fn candidate_digest(
    coordinates: &BeaPublicationRejoinCoordinates,
    revision_plan: &ExtractionRevisionPlan,
    source_batch_digest: EvidenceDigest,
) -> Result<EvidenceDigest, BeaPublicationError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/bea-publication-candidate/v7");
    hash_text(&mut hasher, coordinates.source_id.as_str())?;
    hash_text(
        &mut hasher,
        coordinates
            .metadata_revision
            .as_source_identifier()
            .as_str(),
    )?;
    hash_text(&mut hasher, coordinates.dataset_id.as_str())?;
    hash_text(&mut hasher, coordinates.provider_dataset.as_str())?;
    hash_text(&mut hasher, coordinates.analytical_dataset_id.as_str())?;
    for digest in [
        coordinates.source_binding_digest,
        coordinates.doctor_admission_digest,
        coordinates.doctor_sealed_graph_digest,
        coordinates.acquisition_sealed_graph_digest,
        coordinates.acquisition_capture_receipt_digest,
        coordinates.acquisition_physical_receipt_digest,
        coordinates.metadata_generation,
        coordinates.data_upstream_response_digest,
        coordinates.data_response_digest,
        coordinates.canonical_batch_digest,
    ] {
        hasher.update(digest.bytes());
    }
    hasher.update(coordinates.data_component_ordinal.to_be_bytes());
    hasher.update(coordinates.row_count.to_be_bytes());
    if coordinates.row_count
        != u64::try_from(revision_plan.len()).map_err(|_| BeaPublicationError::InvalidEvidence)?
        || !revision_plan.is_locally_observed()
    {
        return Err(BeaPublicationError::InvalidEvidence);
    }
    hasher.update(source_batch_digest.bytes());
    hasher.update(
        u64::try_from(revision_plan.len())
            .map_err(|_| BeaPublicationError::InvalidEvidence)?
            .to_be_bytes(),
    );
    hasher.update(b"locally-observed-canonical-content");
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaPublicationError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaPublicationError::InvalidEvidence)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
