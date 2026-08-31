//! One-use raw-sealed handoff for the complete Nasdaq Trader equity directory.

use market_squawk_domain::{ExactPayloadEvidence, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ProviderCaptureError,
    ProviderCaptureMaterialSealError, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    SealedProviderCaptureBinding, SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt,
    SourceObjectCaptureIdentity,
};
use serde::Serialize;
use thiserror::Error;

use crate::NasdaqSymbolDirectoryDiscovery;
use crate::archive::NasdaqHttpResponseEvidence;
use crate::model::{NasdaqDirectoryKind, NasdaqListingRecord, NasdaqModelError};
use crate::source::{directory_locator, parse_object_id};

const COMPLETE_DIRECTORY_COMPONENTS: usize = 2;

/// Exact provider-native directory semantics retained beside one normalized row.
///
/// Local clocks, capture positioning, and normalized-row evidence remain in their dedicated
/// common contracts. This projection holds only provider-authored row semantics.
#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct NasdaqSymbolDirectoryNativeRow<'a> {
    family: NasdaqDirectoryKind,
    provider_row_number: u32,
    file_creation_time: &'a str,
    provider_fields: &'a crate::NasdaqProviderFields,
}

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
    components: [NasdaqPendingDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS],
}

/// One component's retained raw-to-canonical continuation awaiting the graph seal.
#[derive(Debug)]
struct NasdaqPendingDirectoryComponent {
    component: NasdaqSealedDirectoryComponent,
    batch: ExtractionBatch,
    native_lineage: ProviderNativeLineageBatch,
    row_capture_page_ordinals: Vec<u16>,
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
            components.push(NasdaqPendingDirectoryComponent::try_from_batch(
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
        let tokens = tokens.into_tokens().into_vec();
        let sealed_capture = tokens
            .first()
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?
            .persisted_receipt()
            .clone();
        for (ordinal, token) in tokens.iter().enumerate() {
            if token.ordinal()
                != u16::try_from(ordinal).map_err(|_| NasdaqDirectoryPublicationError::Protocol)?
            {
                return Err(NasdaqDirectoryPublicationError::Protocol);
            }
            if token.persisted_receipt() != &sealed_capture {
                return Err(NasdaqDirectoryPublicationError::Protocol);
            }
        }
        validate_sealed_components(
            &sealed_capture,
            self.components.iter().map(|component| &component.component),
        )?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(COMPLETE_DIRECTORY_COMPONENTS)
            .map_err(|_| NasdaqDirectoryPublicationError::Capacity)?;
        let mut component_bindings = Vec::new();
        component_bindings
            .try_reserve_exact(COMPLETE_DIRECTORY_COMPONENTS)
            .map_err(|_| NasdaqDirectoryPublicationError::Capacity)?;
        for (token, component) in tokens.into_iter().zip(self.components) {
            component_bindings.push(SealedProviderCaptureBinding::try_component(
                token,
                component.batch,
                component.native_lineage,
                component.row_capture_page_ordinals,
            )?);
            components.push(component.component);
        }
        let components = components
            .try_into()
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
        let component_bindings = component_bindings
            .try_into()
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
        Ok(NasdaqSealedDirectoryPublication {
            sealed_capture,
            components,
            component_bindings,
        })
    }
}

impl NasdaqPendingDirectoryComponent {
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
        let component = NasdaqSealedDirectoryComponent {
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
        };
        let native_lineage = nasdaq_native_lineage(&component, &batch)?;
        let row_capture_page_ordinals = vec![component_ordinal; batch.records().len()];
        Ok(Self {
            component,
            batch,
            native_lineage,
            row_capture_page_ordinals,
        })
    }
}

/// Complete sealed `nasdaqlisted.txt` plus `otherlisted.txt` generation input.
#[derive(Debug)]
pub struct NasdaqSealedDirectoryPublication {
    // Restart evidence remains available for current read-only consumers. The two bindings below,
    // not this cloneable receipt, carry the non-reusable durable-publication authority.
    sealed_capture: SealedProviderCaptureSetReceipt,
    components: [NasdaqSealedDirectoryComponent; COMPLETE_DIRECTORY_COMPONENTS],
    component_bindings: [SealedProviderCaptureBinding; COMPLETE_DIRECTORY_COMPONENTS],
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

    /// Returns the exact one-use bindings in complete request-graph component order.
    pub const fn component_bindings(
        &self,
    ) -> &[SealedProviderCaptureBinding; COMPLETE_DIRECTORY_COMPONENTS] {
        &self.component_bindings
    }
}

fn nasdaq_native_lineage(
    component: &NasdaqSealedDirectoryComponent,
    batch: &ExtractionBatch,
) -> Result<ProviderNativeLineageBatch, NasdaqDirectoryPublicationError> {
    if component.rows().len() != batch.records().len() {
        return Err(NasdaqDirectoryPublicationError::Protocol);
    }
    let mut lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::NasdaqSymbolDirectoryV1,
        batch,
    )
    .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
    for row in component.rows() {
        lineage
            .try_push(&NasdaqSymbolDirectoryNativeRow {
                family: component.family(),
                provider_row_number: row.record().provider_row_number(),
                file_creation_time: row.record().file_creation_time().raw(),
                provider_fields: row.record().provider_fields(),
            })
            .map_err(|_| NasdaqDirectoryPublicationError::Protocol)?;
    }
    lineage
        .finish()
        .map_err(|_| NasdaqDirectoryPublicationError::Protocol)
}

fn validate_sealed_components<'a>(
    sealed: &SealedProviderCaptureSetReceipt,
    components: impl Iterator<Item = &'a NasdaqSealedDirectoryComponent>,
) -> Result<(), NasdaqDirectoryPublicationError> {
    let capture = sealed.capture();
    if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.request_graph_components().len() != COMPLETE_DIRECTORY_COMPONENTS
        || sealed.segment().frames().len() != COMPLETE_DIRECTORY_COMPONENTS
    {
        return Err(NasdaqDirectoryPublicationError::Protocol);
    }
    let mut component_count: usize = 0;
    for (ordinal, component) in components.enumerate() {
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
        component_count = component_count
            .checked_add(1)
            .ok_or(NasdaqDirectoryPublicationError::Protocol)?;
    }
    if component_count != COMPLETE_DIRECTORY_COMPONENTS {
        return Err(NasdaqDirectoryPublicationError::Protocol);
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
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::Arc;

    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use market_squawk_domain::{
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::{LocalPaths, RawCaptureRecord};
    use market_squawk_sources::{
        AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
        ExtractionBatch, ExtractionRecord, ExtractionRequest, ProviderCaptureMaterial,
        ProviderCapturePageReceipt, ProviderCaptureSetReceipt, SourceObject,
        SourceObjectCaptureIdentity,
    };
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::NasdaqPendingDirectoryPublication;
    use crate::source::directory_discovery_for_test;
    use crate::{
        NasdaqDirectoryKind, NasdaqFileCreationTime, NasdaqFinancialStatus,
        NasdaqHttpResponseEvidence, NasdaqListingRecord, NasdaqMarketCategory, NasdaqOtherExchange,
        NasdaqProviderFields,
    };

    #[test]
    fn prepared_directory_rejoin_retains_each_component_capture_binding()
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
            source_id.clone(),
            revision.clone(),
            dataset.clone(),
            digest(b"complete-directory-graph"),
            vec![first, second],
        )?;
        let discovery_request = DiscoveryRequest::try_new(
            dataset.clone(),
            None,
            NonZeroU16::new(2).ok_or("directory object count")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let objects = NasdaqDirectoryKind::EQUITY_DIRECTORIES.map(|family| {
            source_object(
                &source_id,
                revision.clone(),
                &discovery_request,
                &capture,
                family,
            )
        });
        let objects = objects.into_iter().collect::<Result<Vec<_>, _>>()?;
        let discovery = directory_discovery_for_test(
            DiscoveryBatch::try_new(&discovery_request, objects.clone())?,
            capture,
            responses(&objects)?,
        );
        let batches = objects
            .iter()
            .enumerate()
            .map(|(ordinal, object)| batch(object, ordinal))
            .collect::<Result<Vec<_>, _>>()?;
        let (pending, request) =
            NasdaqPendingDirectoryPublication::try_prepare(discovery, batches)?;
        let temporary = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(temporary.path())?;
        let store = paths.sealed_research_journal_store()?;
        let sealed = request.seal(&store)?;
        let publication = pending.try_rejoin(sealed)?;
        let bindings = publication.component_bindings();
        assert_eq!(bindings.len(), 2);
        let graph = publication.sealed_capture().capture();
        assert_ne!(
            graph.request_graph_components()[0].content_digest(),
            graph.pages()[0].body_digest()
        );
        for (ordinal, binding) in bindings.iter().enumerate() {
            assert_eq!(binding.component_ordinal(), Some(u16::try_from(ordinal)?));
            assert_eq!(binding.batch().records().len(), 1);
            assert_eq!(binding.row_frames().len(), 1);
            assert_eq!(
                binding.row_frames()[0].capture_page_ordinal(),
                u16::try_from(ordinal)?
            );
            assert_eq!(
                binding.row_frames()[0].physical_frame_ordinal(),
                u32::try_from(ordinal)?
            );
            assert_eq!(
                binding.capture_evidence().request_graph_components()[ordinal].content_digest(),
                graph.request_graph_components()[ordinal].content_digest()
            );
        }
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

    fn source_object(
        source_id: &SourceId,
        revision: MetadataRevision,
        request: &DiscoveryRequest,
        capture: &ProviderCaptureMaterial,
        family: NasdaqDirectoryKind,
    ) -> Result<SourceObject, Box<dyn std::error::Error>> {
        let ordinal = match family {
            NasdaqDirectoryKind::NasdaqListed => 0,
            NasdaqDirectoryKind::OtherListed => 1,
            NasdaqDirectoryKind::Bonds | NasdaqDirectoryKind::Options => {
                return Err("unsupported directory fixture".into());
            }
        };
        let graph_component = capture
            .receipt()
            .request_graph_components()
            .get(ordinal)
            .ok_or("graph component")?;
        let page = capture.receipt().pages().get(ordinal).ok_or("graph page")?;
        let evidence = ExactPayloadEvidence::from_content_digest(page.body_digest());
        let object_id = SourceIdentifier::try_from(format!(
            "nasdaq-symbols:{}:{}",
            family.object_component(),
            hexadecimal(page.body_digest().bytes())
        ))?;
        SourceObject::try_new_with_capture_identity(
            source_id.clone(),
            revision,
            request,
            object_id,
            SourceIdentifier::try_from("text/plain")?,
            evidence,
            SourceObjectCaptureIdentity::Paged {
                content_digest: graph_component.content_digest(),
                page_count: graph_component.page_count(),
                terminal: graph_component.terminal(),
            },
            EffectiveInterval::new(page.received_at(), None)?,
            Some(page.received_at()),
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: page.received_at(),
            },
            Some(page.body_bytes()),
        )
        .map_err(Into::into)
    }

    fn responses(
        objects: &[SourceObject],
    ) -> Result<Box<[NasdaqHttpResponseEvidence]>, Box<dyn std::error::Error>> {
        let mut responses = Vec::new();
        responses.try_reserve_exact(objects.len())?;
        for object in objects {
            let received_at = object
                .availability()
                .conservative_available_at()
                .ok_or("response received_at")?;
            responses.push(NasdaqHttpResponseEvidence::try_new(
                200,
                "text/plain".to_owned(),
                Some("identity".to_owned()),
                object.expected_bytes(),
                None,
                1,
                object.published_at().ok_or("response last_modified_at")?,
                received_at,
            )?);
        }
        Ok(responses.into_boxed_slice())
    }

    fn batch(
        object: &SourceObject,
        ordinal: usize,
    ) -> Result<ExtractionBatch, Box<dyn std::error::Error>> {
        let request = ExtractionRequest::try_new(
            object.clone(),
            NonZeroU32::new(1).ok_or("record limit")?,
            NonZeroU64::new(1_000_000).ok_or("byte limit")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let family = match ordinal {
            0 => NasdaqDirectoryKind::NasdaqListed,
            1 => NasdaqDirectoryKind::OtherListed,
            _ => return Err("component ordinal".into()),
        };
        let fields = match family {
            NasdaqDirectoryKind::NasdaqListed => NasdaqProviderFields::try_nasdaq_listed(
                "NDAQ".to_owned(),
                "NASDAQ INC".to_owned(),
                NasdaqMarketCategory::GlobalSelect,
                false,
                NasdaqFinancialStatus::Normal,
                100,
                false,
                false,
            )?,
            NasdaqDirectoryKind::OtherListed => NasdaqProviderFields::try_other_listed(
                "IBM".to_owned(),
                "INTERNATIONAL BUSINESS MACHINES".to_owned(),
                NasdaqOtherExchange::Nyse,
                "IBM".to_owned(),
                false,
                100,
                false,
                "IBM".to_owned(),
            )?,
            NasdaqDirectoryKind::Bonds | NasdaqDirectoryKind::Options => {
                return Err("unsupported directory fixture".into());
            }
        };
        let received_at = object
            .availability()
            .conservative_available_at()
            .ok_or("record received_at")?;
        let record = NasdaqListingRecord::try_new(
            2,
            NasdaqFileCreationTime::try_from_provider_value("0814202612:00")?,
            object.published_at().ok_or("record last_modified_at")?,
            received_at,
            object.evidence().clone(),
            fields,
        )?;
        let payload = Bytes::from(serde_json::to_vec(&record)?);
        let payload_evidence = ExactPayloadEvidence::from_content_digest(digest(&payload));
        let (_, object_digest) = crate::source::parse_object_id(object.object_id())?;
        let revision = SourceIdentifier::try_from(format!(
            "nasdaq-symbols:{}:row-2:{object_digest}",
            family.object_component()
        ))?;
        Ok(ExtractionBatch::try_new(
            &request,
            vec![ExtractionRecord::try_new(
                &request,
                SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)?,
                payload_evidence,
                object.published_at().ok_or("record effective_at")?,
                Some(object.published_at().ok_or("record published_at")?),
                object.availability().clone(),
                revision,
                None,
                payload,
            )?],
        )?)
    }

    fn digest(value: &[u8]) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
    }

    fn hexadecimal(bytes: [u8; 32]) -> String {
        let mut value = String::with_capacity(64);
        for byte in bytes {
            use std::fmt::Write as _;

            write!(&mut value, "{byte:02x}").expect("writing into a string cannot fail");
        }
        value
    }
}
