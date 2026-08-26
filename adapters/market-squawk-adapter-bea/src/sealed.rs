//! Typed rejoin of BEA native evidence with one shared immutable raw request-graph receipt.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_sources::{
    ExtractionBatch, ProviderCaptureSealExpectation, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderWholeCaptureToken, RejoinedProviderCapture,
    SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{BeaDatasetEvidence, BeaDatasetIdentity, BeaMetadataGeneration};

/// A typed BEA acquisition could not be matched to the shared journal's physical receipt.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaSealedAcquisitionError {
    /// Component count/order, provider receipt, dataset, or physical identity did not match.
    #[error("invalid sealed BEA acquisition evidence")]
    InvalidEvidence,
}

/// Complete native BEA evidence rejoined with one exact shared `MSJ1` request-graph seal.
///
/// Construction consumes typed evidence and an actual shared seal receipt. Every independent
/// metadata/data request remains visible as an ordered graph component, while the physical store
/// commits the complete bounded doctor or acquisition journey as one segment. No constructor
/// accepts a caller-supplied seal digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaSealedAcquisitionReceipt {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset_id: SourceIdentifier,
    provider_dataset: BeaDatasetIdentity,
    evidence: BeaDatasetEvidence,
    sealed_capture: SealedProviderCaptureSetReceipt,
    data_component_ordinal: u16,
    sealed_graph_digest: EvidenceDigest,
}

impl BeaSealedAcquisitionReceipt {
    /// Derives persisted sidecar evidence only while retaining the exact live whole-capture token.
    pub(crate) fn try_from_token(
        evidence: BeaDatasetEvidence,
        capture_token: &ProviderWholeCaptureToken,
    ) -> Result<Self, BeaSealedAcquisitionError> {
        let sealed_capture = capture_token.persisted_receipt().clone();
        let expected_graph_identity = validate_sealed_capture_sidecar(&evidence, &sealed_capture)?;
        let expected_count = evidence.expected_capture_count();
        let first = evidence
            .expected_capture(0)
            .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
        let source_id = first.source_id().clone();
        let metadata_revision = first.metadata_revision().clone();
        let dataset_id = evidence.metadata().dataset_id().clone();
        let provider_dataset = evidence.data().page().dataset().clone();
        let data_component_ordinal = u16::try_from(expected_count.saturating_sub(1))
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?;
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-sealed-acquisition/v3");
        hash_text(&mut hasher, source_id.as_str())?;
        hash_text(
            &mut hasher,
            metadata_revision.as_source_identifier().as_str(),
        )?;
        hash_text(&mut hasher, dataset_id.as_str())?;
        hash_text(&mut hasher, provider_dataset.as_str())?;
        hasher.update(evidence.metadata().generation().digest());
        for page in evidence.metadata().pages() {
            hasher.update(page.page().receipt().upstream_response_digest());
            hasher.update(page.page().receipt().response_digest());
        }
        hasher.update(evidence.data().page().receipt().upstream_response_digest());
        hasher.update(evidence.data().page().receipt().response_digest());
        hasher.update(expected_graph_identity.bytes());
        hasher.update(sealed_capture.receipt_digest().bytes());
        hasher.update(sealed_capture.segment().physical_receipt_digest().bytes());
        let sealed_graph_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            source_id,
            metadata_revision,
            dataset_id,
            provider_dataset,
            evidence,
            sealed_capture,
            data_component_ordinal,
            sealed_graph_digest,
        })
    }

    /// Returns the exact source shared by every graph component.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source registration revision shared by every graph component.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the configured provider-query contract identity used as the graph dataset.
    pub const fn dataset_id(&self) -> &SourceIdentifier {
        &self.dataset_id
    }

    /// Returns the exact BEA dataset echoed by the final `GetData` response.
    pub const fn provider_dataset(&self) -> &BeaDatasetIdentity {
        &self.provider_dataset
    }

    /// Returns the retained parsed native evidence.
    pub const fn evidence(&self) -> &BeaDatasetEvidence {
        &self.evidence
    }

    /// Returns the actual shared physical request-graph receipt.
    pub const fn sealed_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_capture
    }

    /// Returns the graph component ordinal containing the `GetData` response.
    pub const fn data_component_ordinal(&self) -> u16 {
        self.data_component_ordinal
    }

    /// Returns the complete typed/native/physical acquisition commitment.
    pub const fn sealed_graph_digest(&self) -> EvidenceDigest {
        self.sealed_graph_digest
    }

    /// Returns the exact final data-response content digest.
    pub fn data_response_digest(&self) -> Result<EvidenceDigest, BeaSealedAcquisitionError> {
        self.evidence
            .data()
            .capture()
            .pages()
            .first()
            .filter(|_| self.evidence.data().capture().pages().len() == 1)
            .map(|page| page.body_digest())
            .ok_or(BeaSealedAcquisitionError::InvalidEvidence)
    }

    /// Returns SHA-256 of the exact final provider body before validated echo redaction.
    pub fn data_upstream_response_digest(
        &self,
    ) -> Result<EvidenceDigest, BeaSealedAcquisitionError> {
        let upstream = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.evidence
                .data()
                .page()
                .receipt()
                .upstream_response_digest(),
        );
        if upstream == self.data_response_digest()? {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }
        Ok(upstream)
    }

    /// Consumes the rejoin into native evidence and its physical sidecar receipt.
    pub fn into_parts(self) -> (BeaDatasetEvidence, SealedProviderCaptureSetReceipt) {
        (self.evidence, self.sealed_capture)
    }
}

/// Opaque source output waiting for the shared sealer's physical receipt.
#[derive(Debug)]
pub struct BeaPendingExtractionSeal {
    source_batch: ExtractionBatch,
    evidence: BeaDatasetEvidence,
    source_batch_digest: EvidenceDigest,
    expectation: ProviderCaptureSealExpectation,
}

impl BeaPendingExtractionSeal {
    pub(crate) fn from_source(
        source_batch: ExtractionBatch,
        evidence: BeaDatasetEvidence,
        source_batch_digest: EvidenceDigest,
        expectation: ProviderCaptureSealExpectation,
    ) -> Self {
        Self {
            source_batch,
            evidence,
            source_batch_digest,
            expectation,
        }
    }

    /// Consumes only the opaque physical result split from this exact source output.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<BeaSealedExtractionOutput, BeaSealedAcquisitionError> {
        let capture_token = match self
            .expectation
            .try_rejoin(sealed)
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?
        {
            RejoinedProviderCapture::Whole(token) => token,
            RejoinedProviderCapture::Components(_) => {
                return Err(BeaSealedAcquisitionError::InvalidEvidence);
            }
        };
        let sealed_acquisition =
            BeaSealedAcquisitionReceipt::try_from_token(self.evidence, &capture_token)?;
        Ok(BeaSealedExtractionOutput {
            source_batch: self.source_batch,
            source_batch_digest: self.source_batch_digest,
            sealed_acquisition,
            capture_token,
        })
    }
}

/// Original source extraction output joined to the physical acquisition sidecar.
///
/// The source batch retains provider-content identity and its original request bounds. The sealed
/// acquisition separately retains the complete native dictionaries and shared physical receipt.
#[derive(Debug)]
pub struct BeaSealedExtractionOutput {
    source_batch: ExtractionBatch,
    source_batch_digest: EvidenceDigest,
    sealed_acquisition: BeaSealedAcquisitionReceipt,
    capture_token: ProviderWholeCaptureToken,
}

impl BeaSealedExtractionOutput {
    /// Returns the original source-produced extraction batch.
    pub const fn source_batch(&self) -> &ExtractionBatch {
        &self.source_batch
    }

    /// Returns native dictionaries and the physical acquisition sidecar.
    pub const fn sealed_acquisition(&self) -> &BeaSealedAcquisitionReceipt {
        &self.sealed_acquisition
    }

    pub(crate) fn into_publication_parts(
        self,
    ) -> (
        ExtractionBatch,
        EvidenceDigest,
        BeaSealedAcquisitionReceipt,
        ProviderWholeCaptureToken,
    ) {
        (
            self.source_batch,
            self.source_batch_digest,
            self.sealed_acquisition,
            self.capture_token,
        )
    }
}

fn validate_sealed_capture_sidecar(
    evidence: &BeaDatasetEvidence,
    sealed_capture: &SealedProviderCaptureSetReceipt,
) -> Result<EvidenceDigest, BeaSealedAcquisitionError> {
    let expected_count = evidence.expected_capture_count();
    if expected_count < 2 {
        return Err(BeaSealedAcquisitionError::InvalidEvidence);
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(expected_count)
        .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?;
    for ordinal in 0..expected_count {
        expected.push(
            evidence
                .expected_capture(ordinal)
                .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?,
        );
    }
    let first = expected
        .first()
        .copied()
        .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
    if expected.iter().any(|capture| {
        capture.source_id() != first.source_id()
            || capture.metadata_revision() != first.metadata_revision()
            || capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || !capture.request_graph_components().is_empty()
    }) {
        return Err(BeaSealedAcquisitionError::InvalidEvidence);
    }
    let expected_graph_identity = bea_capture_graph_identity(
        evidence.metadata().dataset_id(),
        evidence.metadata().generation(),
        &expected,
    )?;
    let graph = sealed_capture.capture();
    if graph.source_id() != first.source_id()
        || graph.metadata_revision() != first.metadata_revision()
        || graph.dataset() != evidence.metadata().dataset_id()
        || graph.request_set_identity() != expected_graph_identity
        || graph.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || graph.request_graph_components().len() != expected_count
        || !valid_digest(sealed_capture.receipt_digest())
        || !valid_digest(sealed_capture.segment().physical_receipt_digest())
    {
        return Err(BeaSealedAcquisitionError::InvalidEvidence);
    }
    let mut page_ordinal = 0_usize;
    for (ordinal, expected_capture) in expected.iter().enumerate() {
        let component = graph
            .request_graph_components()
            .get(ordinal)
            .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
        if usize::from(component.ordinal()) != ordinal
            || component.dataset() != expected_capture.dataset()
            || component.request_set_identity() != expected_capture.request_set_identity()
            || component.terminal() != expected_capture.terminal()
            || usize::from(component.first_page_ordinal()) != page_ordinal
            || usize::from(component.page_count().get()) != expected_capture.pages().len()
            || component.total_body_bytes() != expected_capture.total_body_bytes()
            || component.content_digest() != expected_capture.content_digest()
            || component.observation_digest() != expected_capture.observation_digest()
        {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }
        for expected_page in expected_capture.pages() {
            let actual_page = graph
                .pages()
                .get(page_ordinal)
                .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
            if usize::from(actual_page.ordinal()) != page_ordinal
                || actual_page.request_identity() != expected_page.request_identity()
                || actual_page.request_page_token_digest()
                    != expected_page.request_page_token_digest()
                || actual_page.response_next_page_token_digest()
                    != expected_page.response_next_page_token_digest()
                || actual_page.http_status() != expected_page.http_status()
                || actual_page.body_bytes() != expected_page.body_bytes()
                || actual_page.body_digest() != expected_page.body_digest()
                || actual_page.received_at() != expected_page.received_at()
            {
                return Err(BeaSealedAcquisitionError::InvalidEvidence);
            }
            page_ordinal = page_ordinal
                .checked_add(1)
                .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
        }
    }
    let expected_total = expected
        .iter()
        .try_fold(0_u64, |total, capture| {
            total.checked_add(capture.total_body_bytes())
        })
        .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
    if page_ordinal != graph.pages().len() || graph.total_body_bytes() != expected_total {
        return Err(BeaSealedAcquisitionError::InvalidEvidence);
    }
    Ok(expected_graph_identity)
}

pub(crate) fn bea_capture_graph_identity(
    dataset_id: &SourceIdentifier,
    metadata_generation: BeaMetadataGeneration,
    captures: &[&ProviderCaptureSetReceipt],
) -> Result<EvidenceDigest, BeaSealedAcquisitionError> {
    if captures.len() < 2 {
        return Err(BeaSealedAcquisitionError::InvalidEvidence);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/bea-capture-request-graph/v1");
    hash_text(&mut hasher, dataset_id.as_str())?;
    hasher.update(metadata_generation.digest());
    hasher.update(
        u32::try_from(captures.len())
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?
            .to_be_bytes(),
    );
    for (ordinal, capture) in captures.iter().enumerate() {
        hasher.update(
            u16::try_from(ordinal)
                .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?
                .to_be_bytes(),
        );
        hash_text(&mut hasher, capture.source_id().as_str())?;
        hash_text(
            &mut hasher,
            capture.metadata_revision().as_source_identifier().as_str(),
        )?;
        hash_text(&mut hasher, capture.dataset().as_str())?;
        hasher.update(capture.request_set_identity().bytes());
        hasher.update(capture.content_digest().bytes());
        hasher.update(capture.observation_digest().bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn valid_digest(value: EvidenceDigest) -> bool {
    value.algorithm() == DigestAlgorithm::Sha256 && value.bytes() != [0; 32]
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaSealedAcquisitionError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
