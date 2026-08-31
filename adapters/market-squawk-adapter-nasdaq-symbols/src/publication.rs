//! One-use raw-sealed handoff for the complete Nasdaq Trader equity directory.

use market_squawk_domain::{ExactPayloadEvidence, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ProviderCaptureError,
    ProviderCaptureMaterialSealError, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt, SourceObjectCaptureIdentity,
};
use thiserror::Error;

use crate::NasdaqSymbolDirectoryDiscovery;
use crate::archive::NasdaqHttpResponseEvidence;
use crate::model::{NasdaqDirectoryKind, NasdaqListingRecord, NasdaqModelError};
use crate::source::{directory_locator, parse_object_id};

const COMPLETE_DIRECTORY_COMPONENTS: usize = 2;

/// One normalized provider row that cannot be obtained before the matching raw graph is sealed.
#[derive(Debug)]
pub struct NasdaqSealedListingRow {
    record: NasdaqListingRecord,
    record_revision: SourceIdentifier,
    record_payload_evidence: ExactPayloadEvidence,
}

impl NasdaqSealedListingRow {
    /// Returns the exact validated provider row.
    pub const fn record(&self) -> &NasdaqListingRecord {
        &self.record
    }

    /// Returns the request-bound immutable row revision.
    pub const fn record_revision(&self) -> &SourceIdentifier {
        &self.record_revision
    }

    /// Returns evidence for the exact normalized row bytes.
    pub const fn record_payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.record_payload_evidence
    }
}

/// One independently clocked official file inside the sealed complete-directory graph.
#[derive(Debug)]
pub struct NasdaqSealedDirectoryComponent {
    family: NasdaqDirectoryKind,
    source_object_id: SourceIdentifier,
    source_reference: SourceIdentifier,
    capture_identity: SourceObjectCaptureIdentity,
    file_creation_time: String,
    payload_evidence: ExactPayloadEvidence,
    source_last_modified_at: Timestamp,
    received_at: Timestamp,
    rows: Box<[NasdaqSealedListingRow]>,
}

impl NasdaqSealedDirectoryComponent {
    pub const fn family(&self) -> NasdaqDirectoryKind {
        self.family
    }

    pub const fn source_object_id(&self) -> &SourceIdentifier {
        &self.source_object_id
    }

    pub const fn source_reference(&self) -> &SourceIdentifier {
        &self.source_reference
    }

    pub fn file_creation_time(&self) -> &str {
        &self.file_creation_time
    }

    pub const fn payload_evidence(&self) -> &ExactPayloadEvidence {
        &self.payload_evidence
    }

    pub const fn source_last_modified_at(&self) -> Timestamp {
        self.source_last_modified_at
    }

    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub fn rows(&self) -> &[NasdaqSealedListingRow] {
        &self.rows
    }
}

/// Opaque complete-directory rows awaiting their exact physical raw seal.
#[derive(Debug)]
pub struct NasdaqPendingDirectoryPublication {
    expectation: ProviderCaptureSealExpectation,
    components: [NasdaqSealedDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS],
}

impl NasdaqPendingDirectoryPublication {
    /// Validates the two extracted objects and splits their complete raw graph for sealing.
    pub fn try_prepare(
        discovery: NasdaqSymbolDirectoryDiscovery,
        batches: Vec<ExtractionBatch>,
    ) -> Result<(Self, ProviderCaptureSealRequest), NasdaqDirectoryPublicationError> {
        let (discovery, capture, responses) = discovery.into_parts();
        if discovery.objects().len() != COMPLETE_DIRECTORY_COMPONENTS
            || capture.receipt().terminal()
                != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || capture.receipt().request_graph_components().len() != COMPLETE_DIRECTORY_COMPONENTS
            || capture.receipt().pages().len() != COMPLETE_DIRECTORY_COMPONENTS
            || responses.len() != COMPLETE_DIRECTORY_COMPONENTS
            || batches.len() != COMPLETE_DIRECTORY_COMPONENTS
        {
            return Err(NasdaqDirectoryPublicationError::Protocol);
        }
        let responses: [NasdaqHttpResponseEvidence; COMPLETE_DIRECTORY_COMPONENTS] = responses
            .into_vec()
            .try_into()
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
        let batches: [ExtractionBatch; COMPLETE_DIRECTORY_COMPONENTS] = batches
            .try_into()
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(COMPLETE_DIRECTORY_COMPONENTS)
            .map_err(|_| NasdaqDirectoryPublicationError::Capacity)?;
        for (ordinal, (((family, object), response), batch)) in
            NasdaqDirectoryKind::EQUITY_DIRECTORIES
                .into_iter()
                .zip(discovery.objects())
                .zip(responses)
                .zip(batches)
                .enumerate()
        {
            components.push(NasdaqSealedDirectoryComponent::try_from_batch(
                family,
                u16::try_from(ordinal).map_err(|_| NasdaqDirectoryPublicationError::Protocol)?,
                object,
                response,
                batch,
                capture.receipt(),
            )?);
        }
        let components = components
            .try_into()
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
        let (expectation, request) = capture.into_component_seal_parts()?;
        Ok((
            Self {
                expectation,
                components,
            },
            request,
        ))
    }

    /// Opens the normalized handoff only after the exact two response bodies are durably sealed.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<NasdaqSealedDirectoryPublication, NasdaqDirectoryPublicationError> {
        let tokens = self.expectation.try_rejoin(sealed)?.try_into_components()?;
        if tokens.len() != COMPLETE_DIRECTORY_COMPONENTS {
            return Err(NasdaqDirectoryPublicationError::Protocol);
        }
        let mut receipt = None;
        for (ordinal, token) in tokens.into_tokens().into_vec().into_iter().enumerate() {
            if token.ordinal()
                != u16::try_from(ordinal).map_err(|_| NasdaqDirectoryPublicationError::Protocol)?
            {
                return Err(NasdaqDirectoryPublicationError::Protocol);
            }
            match &receipt {
                Some(expected) if expected != token.persisted_receipt() => {
                    return Err(NasdaqDirectoryPublicationError::Protocol);
                }
                Some(_) => {}
                None => receipt = Some(token.persisted_receipt().clone()),
            }
        }
        let sealed_capture = receipt.ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        validate_sealed_components(&sealed_capture, &self.components)?;
        Ok(NasdaqSealedDirectoryPublication {
            sealed_capture,
            components: self.components,
        })
    }
}

impl NasdaqSealedDirectoryComponent {
    fn try_from_batch(
        family: NasdaqDirectoryKind,
        component_ordinal: u16,
        object: &market_squawk_sources::SourceObject,
        response: NasdaqHttpResponseEvidence,
        batch: ExtractionBatch,
        capture: &market_squawk_sources::ProviderCaptureSetReceipt,
    ) -> Result<Self, NasdaqDirectoryPublicationError> {
        let (object_family, object_digest) = parse_object_id(object.object_id())?;
        let component = capture
            .request_graph_components()
            .get(usize::from(component_ordinal))
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        let page = capture
            .pages()
            .get(usize::from(component.first_page_ordinal()))
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        let expected_capture_identity = SourceObjectCaptureIdentity::Paged {
            content_digest: component.content_digest(),
            page_count: component.page_count(),
            terminal: component.terminal(),
        };
        if object_family != family
            || batch.request().object() != object
            || component.ordinal() != component_ordinal
            || component.source_id() != object.source_id()
            || component.metadata_revision() != object.metadata_revision()
            || component.dataset() != object.dataset()
            || object.capture_identity() != expected_capture_identity
            || component.page_count().get() != 1
            || component.first_page_ordinal() != component_ordinal
            || component.total_body_bytes() != page.body_bytes()
            || page.ordinal() != component_ordinal
            || page.body_digest() != object.evidence().content_digest()
            || page.received_at() != response.received_at()
            || response.status() != page.http_status()
            || response
                .declared_content_length()
                .is_some_and(|declared| declared != page.body_bytes())
            || response.last_modified_at()
                != object
                    .published_at()
                    .ok_or(NasdaqDirectoryPublicationError::Protocol)?
            || response.received_at()
                != object
                    .availability()
                    .conservative_available_at()
                    .ok_or(NasdaqDirectoryPublicationError::Protocol)?
            || batch.records().is_empty()
        {
            return Err(NasdaqDirectoryPublicationError::Protocol);
        }

        let mut rows = Vec::new();
        rows.try_reserve_exact(batch.records().len())
            .map_err(|_| NasdaqDirectoryPublicationError::Capacity)?;
        for extracted in batch.records() {
            let record = NasdaqListingRecord::from_json(extracted.payload())?;
            let expected_revision = SourceIdentifier::try_from(format!(
                "nasdaq-symbols:{}:row-{}:{object_digest}",
                family.object_component(),
                record.provider_row_number(),
            ))
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
            if extracted.schema().as_str() != CURRENT_RESEARCH_RECORD_SCHEMA
                || extracted.object_id() != object.object_id()
                || extracted.object_evidence() != object.evidence()
                || extracted.available_at() != Some(response.received_at())
                || extracted.revision() != &expected_revision
            {
                return Err(NasdaqDirectoryPublicationError::Protocol);
            }
            if record.provider_fields().directory_kind() != family
                || record.source_payload_evidence() != object.evidence()
                || record.source_last_modified_at() != response.last_modified_at()
                || record.first_observed_at() != response.received_at()
            {
                return Err(NasdaqDirectoryPublicationError::Protocol);
            }
            rows.push(NasdaqSealedListingRow {
                record,
                record_revision: extracted.revision().clone(),
                record_payload_evidence: extracted.evidence().clone(),
            });
        }
        let first = rows
            .first()
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        let file_creation_time = first.record.file_creation_time().raw().to_owned();
        if rows.iter().any(|row| {
            row.record.file_creation_time().raw() != file_creation_time
                || row.record.directory_presence()
                    != crate::NasdaqDirectoryPresence::CurrentDirectory
                || row.record.quality() != market_squawk_domain::DataQuality::OfficialDelayed
        }) {
            return Err(NasdaqDirectoryPublicationError::Protocol);
        }
        Ok(Self {
            family,
            source_object_id: object.object_id().clone(),
            source_reference: SourceIdentifier::try_from(
                directory_locator(family).ok_or(NasdaqDirectoryPublicationError::Protocol)?,
            )
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?,
            capture_identity: expected_capture_identity,
            file_creation_time,
            payload_evidence: object.evidence().clone(),
            source_last_modified_at: response.last_modified_at(),
            received_at: response.received_at(),
            rows: rows.into_boxed_slice(),
        })
    }
}

/// Complete sealed `nasdaqlisted.txt` plus `otherlisted.txt` generation input.
#[derive(Debug)]
pub struct NasdaqSealedDirectoryPublication {
    sealed_capture: SealedProviderCaptureSetReceipt,
    components: [NasdaqSealedDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS],
}

impl NasdaqSealedDirectoryPublication {
    /// Returns the raw-seal receipt shared by the two exact graph components.
    pub const fn sealed_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_capture
    }

    /// Returns `nasdaqlisted.txt`, then `otherlisted.txt`.
    pub const fn components(
        &self,
    ) -> &[NasdaqSealedDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS] {
        &self.components
    }
}

fn validate_sealed_components(
    sealed: &SealedProviderCaptureSetReceipt,
    components: &[NasdaqSealedDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS],
) -> Result<(), NasdaqDirectoryPublicationError> {
    let capture = sealed.capture();
    if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.request_graph_components().len() != COMPLETE_DIRECTORY_COMPONENTS
        || sealed.segment().frames().len() != COMPLETE_DIRECTORY_COMPONENTS
    {
        return Err(NasdaqDirectoryPublicationError::Protocol);
    }
    for (ordinal, component) in components.iter().enumerate() {
        let graph = capture
            .request_graph_components()
            .get(ordinal)
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        let frame = sealed
            .segment()
            .frames()
            .get(ordinal)
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        let capture_content_digest = component
            .capture_identity
            .paged_content_digest()
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
        if graph.ordinal()
            != u16::try_from(ordinal).map_err(|_| NasdaqDirectoryPublicationError::Protocol)?
            || graph.content_digest() != capture_content_digest
            || frame.provider_payload_digest() != component.payload_evidence.content_digest()
            || frame.received_at() != component.received_at
        {
            return Err(NasdaqDirectoryPublicationError::Protocol);
        }
    }
    Ok(())
}

/// Complete-directory mapping or seal failure.
#[derive(Debug, Error)]
pub enum NasdaqDirectoryPublicationError {
    #[error("Nasdaq complete-directory publication evidence is inconsistent")]
    Protocol,
    #[error("Nasdaq complete-directory publication capacity is unavailable")]
    Capacity,
    #[error("Nasdaq normalized row is invalid: {0}")]
    Record(#[from] NasdaqModelError),
    #[error("Nasdaq raw capture is invalid: {0}")]
    Capture(#[from] ProviderCaptureError),
    #[error("Nasdaq raw capture could not be sealed: {0}")]
    Seal(#[from] ProviderCaptureMaterialSealError),
    #[error("Nasdaq extraction failed: {0}")]
    Extraction(#[from] market_squawk_sources::ExtractionSourceError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use market_squawk_domain::{
        DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision, SourceId,
        SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::{LocalPaths, RawCaptureRecord};
    use market_squawk_sources::{
        ProviderCaptureMaterial, ProviderCapturePageReceipt, ProviderCaptureSetReceipt,
    };
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{
        NasdaqDirectoryPublicationError, NasdaqSealedDirectoryComponent, validate_sealed_components,
    };
    use crate::NasdaqDirectoryKind;

    #[test]
    fn sealed_component_binds_capture_envelope_separately_from_raw_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_id = SourceId::try_from("nasdaq-reference-test")?;
        let revision = MetadataRevision::new(SourceIdentifier::try_from("nasdaq-reference-r1")?);
        let dataset = SourceIdentifier::try_from("nasdaq.symbol-directory.us-listed.v1")?;
        let first = capture_component(
            &source_id,
            revision.clone(),
            dataset.clone(),
            b"nasdaq-listed-body",
            Timestamp::from_unix_nanos(10),
            [1; 16],
        )?;
        let second = capture_component(
            &source_id,
            revision.clone(),
            dataset.clone(),
            b"other-listed-body",
            Timestamp::from_unix_nanos(20),
            [2; 16],
        )?;
        let capture = ProviderCaptureMaterial::try_combine_request_graph(
            source_id,
            revision,
            dataset,
            digest(b"complete-directory-graph"),
            vec![first, second],
        )?;
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let store = paths.sealed_research_journal_store()?;
        let (expectation, request) = capture.into_component_seal_parts()?;
        let sealed = request.seal(&store)?;
        let tokens = expectation.try_rejoin(sealed)?.try_into_components()?;
        let sealed = tokens
            .into_tokens()
            .first()
            .ok_or("sealed graph did not contain a first component")?
            .persisted_receipt()
            .clone();
        let mut components = [
            component(&sealed, NasdaqDirectoryKind::NasdaqListed, 0)?,
            component(&sealed, NasdaqDirectoryKind::OtherListed, 1)?,
        ];

        validate_sealed_components(&sealed, &components)?;

        let raw_body_digest = sealed.capture().pages()[0].body_digest();
        let graph_component = &sealed.capture().request_graph_components()[0];
        assert_ne!(graph_component.content_digest(), raw_body_digest);
        components[0].capture_identity =
            market_squawk_sources::SourceObjectCaptureIdentity::Paged {
                content_digest: raw_body_digest,
                page_count: graph_component.page_count(),
                terminal: graph_component.terminal(),
            };
        assert!(matches!(
            validate_sealed_components(&sealed, &components),
            Err(NasdaqDirectoryPublicationError::Protocol)
        ));
        Ok(())
    }

    fn capture_component(
        source_id: &SourceId,
        revision: MetadataRevision,
        dataset: SourceIdentifier,
        body: &[u8],
        received_at: Timestamp,
        record_id: [u8; 16],
    ) -> Result<ProviderCaptureMaterial, Box<dyn std::error::Error>> {
        let body_digest = digest(body);
        let receipt = ProviderCaptureSetReceipt::try_new(
            source_id.clone(),
            revision,
            dataset,
            digest(b"component-request"),
            market_squawk_sources::ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![ProviderCapturePageReceipt::try_new(
                0,
                digest(b"page-request"),
                None,
                None,
                200,
                u64::try_from(body.len())?,
                body_digest,
                received_at,
            )?],
        )?;
        let record = RawCaptureRecord::try_new_live(
            Uuid::from_bytes(record_id),
            Arc::from(source_id.as_str()),
            Uuid::from_bytes(record_id.map(|byte| byte.saturating_add(8))),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
            Bytes::copy_from_slice(body),
        )?;
        Ok(ProviderCaptureMaterial::try_new(receipt, vec![record])?)
    }

    fn component(
        sealed: &market_squawk_sources::SealedProviderCaptureSetReceipt,
        family: NasdaqDirectoryKind,
        ordinal: usize,
    ) -> Result<NasdaqSealedDirectoryComponent, Box<dyn std::error::Error>> {
        let graph_component = sealed
            .capture()
            .request_graph_components()
            .get(ordinal)
            .ok_or("sealed graph component is absent")?;
        let page = sealed
            .capture()
            .pages()
            .get(ordinal)
            .ok_or("sealed graph page is absent")?;
        Ok(NasdaqSealedDirectoryComponent {
            family,
            source_object_id: SourceIdentifier::try_from(format!("object-{ordinal}"))?,
            source_reference: SourceIdentifier::try_from(format!("reference-{ordinal}"))?,
            capture_identity: market_squawk_sources::SourceObjectCaptureIdentity::Paged {
                content_digest: graph_component.content_digest(),
                page_count: graph_component.page_count(),
                terminal: graph_component.terminal(),
            },
            file_creation_time: "08142026".to_owned(),
            payload_evidence: ExactPayloadEvidence::from_content_digest(page.body_digest()),
            source_last_modified_at: page.received_at(),
            received_at: page.received_at(),
            rows: Box::new([]),
        })
    }

    fn digest(value: &[u8]) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
    }
}
