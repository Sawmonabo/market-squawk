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
    use std::num::NonZeroU16;

    use market_squawk_domain::{MetadataRevision, SourceId, SourceIdentifier, Timestamp};
    use market_squawk_platform::LocalPaths;
    use market_squawk_sources::DiscoveryRequest;

    use super::{NasdaqDirectoryPublicationError, NasdaqPendingDirectoryPublication};
    use crate::source::directory_journey_for_test;

    const NASDAQ_LISTED_BODY: &[u8] = b"Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\nNDAQ|NASDAQ INC|Q|N|N|100|N|N\nFile Creation Time: 0814202612:00|||||||\n";
    const OTHER_LISTED_BODY: &[u8] = b"ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\nIBM|INTERNATIONAL BUSINESS MACHINES|N|IBM|N|100|N|IBM\nFile Creation Time: 0814202612:00||||||\n";

    #[test]
    fn prepared_directory_rejoin_retains_each_component_capture_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_id = SourceId::try_from("nasdaq-reference-test")?;
        let revision = MetadataRevision::new(SourceIdentifier::try_from("nasdaq-reference-r1")?);
        let dataset = SourceIdentifier::try_from("nasdaq.symbol-directory.us-listed.v1")?;
        let discovery_request = DiscoveryRequest::try_new(
            dataset.clone(),
            None,
            NonZeroU16::new(2).ok_or("directory object count")?,
            Timestamp::from_unix_nanos(1_786_666_000_000_000_000),
        )?;

        let (rejected_discovery, rejected_batches) = directory_journey_for_test(
            &source_id,
            &revision,
            &dataset,
            &discovery_request,
            [
                (
                    NASDAQ_LISTED_BODY,
                    Timestamp::from_unix_nanos(1_786_665_600_000_000_000),
                    Timestamp::from_unix_nanos(1_786_665_610_000_000_000),
                ),
                (
                    OTHER_LISTED_BODY,
                    Timestamp::from_unix_nanos(1_786_665_620_000_000_000),
                    Timestamp::from_unix_nanos(1_786_665_630_000_000_000),
                ),
            ],
        )?;
        assert_ne!(
            rejected_discovery.capture_material().receipt().pages()[0].request_identity(),
            rejected_discovery.capture_material().receipt().pages()[1].request_identity(),
        );
        let (mut rejected_pending, rejected_request) =
            NasdaqPendingDirectoryPublication::try_prepare(rejected_discovery, rejected_batches)?;
        rejected_pending.components[0].row_capture_page_ordinals[0] = 1;
        let rejected_temporary = tempfile::tempdir()?;
        let rejected_paths = LocalPaths::prepare(rejected_temporary.path())?;
        let rejected_store = rejected_paths.sealed_research_journal_store()?;
        let rejected_sealed = rejected_request.seal(&rejected_store)?;
        assert!(matches!(
            rejected_pending.try_rejoin(rejected_sealed),
            Err(NasdaqDirectoryPublicationError::Capture(_))
        ));

        let (discovery, batches) = directory_journey_for_test(
            &source_id,
            &revision,
            &dataset,
            &discovery_request,
            [
                (
                    NASDAQ_LISTED_BODY,
                    Timestamp::from_unix_nanos(1_786_665_600_000_000_000),
                    Timestamp::from_unix_nanos(1_786_665_610_000_000_000),
                ),
                (
                    OTHER_LISTED_BODY,
                    Timestamp::from_unix_nanos(1_786_665_620_000_000_000),
                    Timestamp::from_unix_nanos(1_786_665_630_000_000_000),
                ),
            ],
        )?;
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
}
