//! Typed rejoin of BEA native evidence with one shared immutable raw request-graph receipt.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_sources::{
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SealedProviderCaptureSetReceipt,
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
    /// Rejoins typed native evidence with the physical receipt returned by the shared sealer.
    pub fn try_new(
        evidence: BeaDatasetEvidence,
        sealed_capture: SealedProviderCaptureSetReceipt,
    ) -> Result<Self, BeaSealedAcquisitionError> {
        let expected_count = evidence.expected_capture_count();
        if expected_count < 2 {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }
        let expected = (0..expected_count)
            .map(|ordinal| {
                evidence
                    .expected_capture(ordinal)
                    .ok_or(BeaSealedAcquisitionError::InvalidEvidence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let first = expected
            .first()
            .copied()
            .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
        let source_id = first.source_id().clone();
        let metadata_revision = first.metadata_revision().clone();
        if expected.iter().any(|capture| {
            capture.source_id() != &source_id
                || capture.metadata_revision() != &metadata_revision
                || capture.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                || !capture.request_graph_components().is_empty()
        }) {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }

        let dataset_id = evidence.metadata().dataset_id().clone();
        let provider_dataset = evidence.data().page().dataset().clone();
        let expected_graph_identity = bea_capture_graph_identity(
            &dataset_id,
            evidence.metadata().generation(),
            &expected,
        )?;
        let graph = sealed_capture.capture();
        if graph.source_id() != &source_id
            || graph.metadata_revision() != &metadata_revision
            || graph.dataset() != &dataset_id
            || graph.request_set_identity() != expected_graph_identity
            || graph.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || graph.request_graph_components().len() != expected_count
            || !valid_digest(sealed_capture.receipt_digest())
            || !valid_digest(sealed_capture.segment().physical_receipt_digest())
        {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }

        let mut flattened_page_ordinal = 0_usize;
        for (ordinal, (component, expected_capture)) in graph
            .request_graph_components()
            .iter()
            .zip(&expected)
            .enumerate()
        {
            if usize::from(component.ordinal()) != ordinal
                || component.dataset() != expected_capture.dataset()
                || component.request_set_identity() != expected_capture.request_set_identity()
                || component.terminal() != expected_capture.terminal()
                || usize::from(component.first_page_ordinal()) != flattened_page_ordinal
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
                    .get(flattened_page_ordinal)
                    .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
                if usize::from(actual_page.ordinal()) != flattened_page_ordinal
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
                flattened_page_ordinal = flattened_page_ordinal
                    .checked_add(1)
                    .ok_or(BeaSealedAcquisitionError::InvalidEvidence)?;
            }
        }
        if flattened_page_ordinal != graph.pages().len()
            || graph.total_body_bytes()
                != expected.iter().try_fold(0_u64, |total, capture| {
                    total.checked_add(capture.total_body_bytes())
                }).ok_or(BeaSealedAcquisitionError::InvalidEvidence)?
        {
            return Err(BeaSealedAcquisitionError::InvalidEvidence);
        }

        let data_component_ordinal = u16::try_from(expected_count.saturating_sub(1))
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?;
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-sealed-acquisition/v2");
        hash_text(&mut hasher, source_id.as_str())?;
        hash_text(
            &mut hasher,
            metadata_revision.as_source_identifier().as_str(),
        )?;
        hash_text(&mut hasher, dataset_id.as_str())?;
        hash_text(&mut hasher, provider_dataset.as_str())?;
        hasher.update(evidence.metadata().generation().digest());
        hasher.update(expected_graph_identity.bytes());
        hasher.update(sealed_capture.receipt_digest().bytes());
        hasher.update(
            sealed_capture
                .segment()
                .physical_receipt_digest()
                .bytes(),
        );
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

fn hash_text(
    hasher: &mut Sha256,
    value: &str,
) -> Result<(), BeaSealedAcquisitionError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaSealedAcquisitionError::InvalidEvidence)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
