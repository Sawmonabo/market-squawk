//! One-use raw-sealed handoff for the complete Nasdaq Trader equity directory.

use market_squawk_domain::{ExactPayloadEvidence, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ProviderCaptureError,
    ProviderCaptureMaterialSealError, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureTerminalDisposition, SealedProviderCaptureMaterial,
    SealedProviderCaptureSetReceipt,
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
        if object_family != family
            || batch.request().object() != object
            || component.ordinal() != component_ordinal
            || component.page_count().get() != 1
            || component.first_page_ordinal() != component_ordinal
            || component.total_body_bytes() != page.body_bytes()
            || component.content_digest() != object.evidence().content_digest()
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
        if graph.ordinal()
            != u16::try_from(ordinal).map_err(|_| NasdaqDirectoryPublicationError::Protocol)?
            || graph.content_digest() != component.payload_evidence.content_digest()
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
