//! Opaque bounded closure of one already captured filing's taxonomy graph.

use std::collections::VecDeque;

use market_squawk_domain::{MetadataRevision, SourceId};
use market_squawk_sources::{FilingTaxonomySourceAuthority, SEC_EDGAR_AUTHORITY};
use tokio_util::sync::CancellationToken;

use super::SecXbrlError;
use super::model::{
    MAX_TAXONOMY_ARTIFACT_BYTES, MAX_TAXONOMY_ARTIFACTS, MAX_TAXONOMY_GRAPH_SCAN_BYTES,
    MAX_TAXONOMY_REFERENCES, MAX_TAXONOMY_SET_BYTES, SecXbrlTaxonomyArtifactRequest,
    filing_taxonomy_seed_requests, taxonomy_request_dependencies,
};
use crate::{RetrievedSecBytes, SecParserLimits};

/// One code-owned request emitted by the opaque taxonomy-closure state machine.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct SecTaxonomyAcquisitionRequest {
    inner: SecXbrlTaxonomyArtifactRequest,
    maximum_response_bytes: u64,
}

impl SecTaxonomyAcquisitionRequest {
    pub(crate) fn logical_locator(&self) -> &str {
        self.inner.logical_locator().as_str()
    }

    pub(crate) fn physical_locator(&self) -> &str {
        self.inner.physical_locator().as_str()
    }

    pub(crate) fn authority(&self) -> Result<FilingTaxonomySourceAuthority, SecXbrlError> {
        self.inner.authority()
    }

    pub(crate) fn maximum_response_bytes(&self) -> u64 {
        self.maximum_response_bytes
    }
}

/// Single-use bounded breadth-first closure builder for one exact captured filing.
pub(crate) struct SecTaxonomyClosure {
    filing_locator: String,
    root_source_id: SourceId,
    root_metadata_revision: MetadataRevision,
    parser_limits: SecParserLimits,
    pending: VecDeque<SecXbrlTaxonomyArtifactRequest>,
    in_flight: Option<SecXbrlTaxonomyArtifactRequest>,
    retained_requests: Vec<SecXbrlTaxonomyArtifactRequest>,
    artifacts: Vec<RetrievedSecBytes>,
    reference_count: usize,
    physical_bytes: u64,
    scanned_bytes: u64,
}

impl SecTaxonomyClosure {
    pub(crate) fn try_start(
        filing_document: &RetrievedSecBytes,
        root_source_id: SourceId,
        root_metadata_revision: MetadataRevision,
        parser_limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecXbrlError> {
        check_cancelled(cancellation)?;
        if root_source_id.as_str() != SEC_EDGAR_AUTHORITY.source_id() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let filing_locator = filing_document
            .locator()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?
            .to_owned();
        let filing_bytes = u64::try_from(filing_document.bytes().len())
            .map_err(|_| SecXbrlError::ByteLimitExceeded)?;
        if filing_bytes == 0 || filing_bytes > MAX_TAXONOMY_GRAPH_SCAN_BYTES {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        let seeds = filing_taxonomy_seed_requests(filing_document, parser_limits, cancellation)?;
        if seeds.is_empty() || seeds.len() > MAX_TAXONOMY_REFERENCES {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let mut pending = VecDeque::new();
        pending
            .try_reserve(seeds.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        pending.extend(seeds);
        let mut retained_requests = Vec::new();
        retained_requests
            .try_reserve_exact(MAX_TAXONOMY_REFERENCES)
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        let mut artifacts = Vec::new();
        artifacts
            .try_reserve_exact(MAX_TAXONOMY_ARTIFACTS)
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        Ok(Self {
            filing_locator,
            root_source_id,
            root_metadata_revision,
            parser_limits,
            reference_count: pending.len(),
            pending,
            in_flight: None,
            retained_requests,
            artifacts,
            physical_bytes: 0,
            scanned_bytes: filing_bytes,
        })
    }

    pub(crate) fn next_request(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<SecTaxonomyAcquisitionRequest>, SecXbrlError> {
        check_cancelled(cancellation)?;
        if self.in_flight.is_some() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        while let Some(request) = self.pending.pop_front() {
            check_cancelled(cancellation)?;
            let logical = request.logical_locator().as_str();
            let logical_index = match self
                .retained_requests
                .binary_search_by(|candidate| candidate.logical_locator().as_str().cmp(logical))
            {
                Ok(index) => {
                    if self.retained_requests[index] != request {
                        return Err(SecXbrlError::InvalidTaxonomySet);
                    }
                    continue;
                }
                Err(index) => index,
            };
            let physical = request.physical_locator().as_str();
            if let Some(existing) = self
                .retained_requests
                .iter()
                .find(|candidate| candidate.physical_locator().as_str() == physical)
                && !existing.same_physical_contract(&request)
            {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            if self.retained_requests.len() >= MAX_TAXONOMY_REFERENCES {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            if self.retained_requests.len() == self.retained_requests.capacity() {
                return Err(SecXbrlError::RetainedOutputLimitExceeded);
            }
            self.retained_requests
                .insert(logical_index, request.clone());
            if let Ok(index) = self
                .artifacts
                .binary_search_by(|artifact| artifact.locator().cmp(&Some(physical)))
            {
                let artifact = self.artifacts[index].clone();
                let scanned_bytes = self
                    .scanned_bytes
                    .checked_add(
                        u64::try_from(artifact.bytes().len())
                            .map_err(|_| SecXbrlError::ByteLimitExceeded)?,
                    )
                    .filter(|bytes| *bytes <= MAX_TAXONOMY_GRAPH_SCAN_BYTES)
                    .ok_or(SecXbrlError::ByteLimitExceeded)?;
                self.scanned_bytes = scanned_bytes;
                self.scan_captured_request(&request, &artifact, cancellation)?;
                continue;
            }
            if self.artifacts.len() >= MAX_TAXONOMY_ARTIFACTS {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            let maximum_response_bytes = MAX_TAXONOMY_SET_BYTES
                .checked_sub(self.physical_bytes)
                .filter(|remaining| *remaining > 0)
                .ok_or(SecXbrlError::ByteLimitExceeded)?;
            self.in_flight = Some(request.clone());
            return Ok(Some(SecTaxonomyAcquisitionRequest {
                inner: request,
                maximum_response_bytes,
            }));
        }
        Ok(None)
    }

    pub(crate) fn accept_captured(
        &mut self,
        request: SecTaxonomyAcquisitionRequest,
        artifact: RetrievedSecBytes,
        cancellation: &CancellationToken,
    ) -> Result<(), SecXbrlError> {
        check_cancelled(cancellation)?;
        let expected = self
            .in_flight
            .take()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if expected != request.inner || artifact.locator() != Some(request.physical_locator()) {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        let artifact_index = match self
            .artifacts
            .binary_search_by(|candidate| candidate.locator().cmp(&artifact.locator()))
        {
            Ok(_) => return Err(SecXbrlError::InvalidTaxonomySet),
            Err(index) => index,
        };
        let authority = request.authority()?;
        let expected_source = authority
            .canonical_source_id()
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?;
        let expected_revision = if authority == SEC_EDGAR_AUTHORITY {
            if expected_source != self.root_source_id {
                return Err(SecXbrlError::InvalidTaxonomySet);
            }
            self.root_metadata_revision.clone()
        } else {
            authority
                .metadata_revision()
                .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
        };
        let receipt = artifact
            .capture_receipt()
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        if receipt.source_id() != &expected_source
            || receipt.metadata_revision() != &expected_revision
            || receipt.dataset().as_str() != request.physical_locator()
        {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        artifact
            .capture_material()
            .map_err(|_| SecXbrlError::InvalidTaxonomySet)?
            .ok_or(SecXbrlError::InvalidTaxonomySet)?;
        let bytes =
            u64::try_from(artifact.bytes().len()).map_err(|_| SecXbrlError::ByteLimitExceeded)?;
        if bytes == 0 || bytes > MAX_TAXONOMY_ARTIFACT_BYTES {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        let physical_bytes = self
            .physical_bytes
            .checked_add(bytes)
            .ok_or(SecXbrlError::ByteLimitExceeded)?;
        let scanned_bytes = self
            .scanned_bytes
            .checked_add(bytes)
            .ok_or(SecXbrlError::ByteLimitExceeded)?;
        if physical_bytes > MAX_TAXONOMY_SET_BYTES
            || scanned_bytes > MAX_TAXONOMY_GRAPH_SCAN_BYTES
            || self.artifacts.len() >= MAX_TAXONOMY_ARTIFACTS
        {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        if self.artifacts.len() == self.artifacts.capacity() {
            return Err(SecXbrlError::RetainedOutputLimitExceeded);
        }
        self.scanned_bytes = scanned_bytes;
        self.scan_captured_request(&request.inner, &artifact, cancellation)?;
        self.artifacts.insert(artifact_index, artifact);
        self.physical_bytes = physical_bytes;
        Ok(())
    }

    fn scan_captured_request(
        &mut self,
        request: &SecXbrlTaxonomyArtifactRequest,
        artifact: &RetrievedSecBytes,
        cancellation: &CancellationToken,
    ) -> Result<(), SecXbrlError> {
        // The body is scanned only after the transport has durably persisted both raw bytes and
        // its source-qualified representation and returned the exact capture receipt above.
        let dependencies = taxonomy_request_dependencies(
            &self.filing_locator,
            request,
            artifact.bytes(),
            self.parser_limits,
            cancellation,
        )?;
        let reference_count = self
            .reference_count
            .checked_add(dependencies.len())
            .ok_or(SecXbrlError::RecordLimitExceeded)?;
        if reference_count > MAX_TAXONOMY_REFERENCES
            || reference_count > self.parser_limits.records()
        {
            return Err(SecXbrlError::RecordLimitExceeded);
        }
        self.pending
            .try_reserve(dependencies.len())
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        self.pending.extend(dependencies);
        self.reference_count = reference_count;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        cancellation: &CancellationToken,
    ) -> Result<Vec<RetrievedSecBytes>, SecXbrlError> {
        check_cancelled(cancellation)?;
        if self.in_flight.is_some() || !self.pending.is_empty() || self.artifacts.is_empty() {
            return Err(SecXbrlError::InvalidTaxonomySet);
        }
        Ok(self.artifacts)
    }
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
    if cancellation.is_cancelled() {
        Err(SecXbrlError::Cancelled)
    } else {
        Ok(())
    }
}
