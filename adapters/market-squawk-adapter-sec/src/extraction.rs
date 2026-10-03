//! Canonical `ExtractionSource` composition for SEC filings and facts.

use std::collections::BTreeMap;
use std::io::Write;
use std::mem::size_of;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence, CompanyIdentityObservation, CompanyIdentityObservationInput,
    CompanyIdentitySurface, DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, FormerCompanyName, MetadataRevision, ProviderReportedSecurityAssociation,
    ResearchContext, ResearchObservation, SchemaVersion, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionBatchAccumulator, ExtractionError,
    ExtractionRecord, ExtractionRequest, ExtractionRevisionEvidence, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, MAX_EXTRACTION_RECORD_BYTES,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ObservedProviderOrder, ProviderCaptureMaterial,
    ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    ProviderWholeCaptureToken, SourceError, SourceMetadataProvider, SourceObject,
    SourceObjectCaptureIdentity,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::normalize::{
    SecFilingXbrlNormalization, compare_company_facts, compare_filings,
    normalize_company_fact_occurrence, normalize_filing_xbrl_with_cancellation,
    same_company_fact_family,
};
use crate::product::{SEC_FILING_XBRL_DATASET_PREFIX, SecFilingXbrlCoordinates};
use crate::xbrl::{
    SecPendingValidatedXbrlTaxonomySet, SecXbrlTaxonomyRegistry, XbrlDocumentContext,
    XbrlDocumentParser,
};
use crate::{
    CompanyFactOccurrence, RawEvidenceStore, RetrievedCompanyFacts, RetrievedSecBytes,
    RetrievedSubmissions, SecClientError, SecCompositeBounds, SecEdgarSource, SecFiling,
    SecNormalizationError, SecObjectLocator, SecParserError, SecParserLimits, SecRepresentation,
    SecRepresentationRegistry, SecResearchDataset, SecResearchDatasetKind,
    normalize_company_facts_with_cancellation, normalize_filings_with_cancellation,
};

const RESEARCH_RECORD_SCHEMA: &str = "market-squawk-research-v3";

/// SEC analytical extraction paired with optional evidence-bound company identity.
///
/// Company identity remains research metadata and cannot establish a tradable instrument, venue
/// mapping, or execution-quality observation.
#[derive(Debug)]
pub struct SecExtractionResult {
    batch: ExtractionBatch,
    company_identity: Option<CompanyIdentityObservation>,
    native_lineage: ProviderNativeLineageBatch,
    row_capture_page_ordinals: Vec<u16>,
}

/// Indivisible SEC discovery handoff containing one source object and its exact HTTP body set.
///
/// Application composition must seal [`Self::capture_material`] before retaining the discovery
/// object or permitting canonical research/company-identity publication from it.
#[derive(Debug)]
pub struct SecDiscoveryResult {
    batch: DiscoveryBatch,
    capture_material: ProviderCaptureMaterial,
}

struct DiscoveredSecMaterial {
    raw: RetrievedSecBytes,
    object_id: SourceIdentifier,
    capture_material: ProviderCaptureMaterial,
    media_type: SourceIdentifier,
    published_at: Option<Timestamp>,
    availability: ExtractionAvailabilityEvidence,
}

/// Non-serializable authority proving one filing root belongs to the requested captured
/// submissions/accession and exactly matches this source's retained representation and raw bytes.
pub(crate) struct AdmittedFilingXbrlRoot {
    sealed_root: ProviderWholeCaptureToken,
    filing: SecFilingXbrlCoordinates,
    filing_document: RetrievedSecBytes,
    filing_representation: SecRepresentation,
}

impl AdmittedFilingXbrlRoot {
    pub(crate) fn filing_document(&self) -> &RetrievedSecBytes {
        &self.filing_document
    }
}

/// Process-local filing-XBRL admission awaiting canonical extraction and physical sealing.
///
/// This capability is intentionally non-cloneable and non-serializable. The application receives
/// only the closed [`SecFilingXbrlCaptureHandoff`], never the taxonomy admission or raw-store
/// authority held here.
struct SecPendingFilingXbrlAdmission {
    dataset: SecResearchDataset,
    filing: SecFilingXbrlCoordinates,
    submissions: RetrievedSubmissions,
    filing_document: RetrievedSecBytes,
    filing_representation: SecRepresentation,
    taxonomy: SecPendingValidatedXbrlTaxonomySet,
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    parser_limits: SecParserLimits,
}

impl SecPendingFilingXbrlAdmission {
    fn revalidate(&self, cancellation: &CancellationToken) -> Result<(), SecClientError> {
        self.filing.revalidate_current_submissions(
            self.submissions.current_component(),
            &self.raw_store,
            &self.source_id,
            &self.metadata_revision,
            self.parser_limits,
            cancellation,
        )?;
        validate_captured_filing_document(
            &self.filing,
            &self.filing_document,
            &self.filing_representation,
            &self.raw_store,
            &self.source_id,
            &self.metadata_revision,
            cancellation,
        )?;
        self.taxonomy.revalidate(cancellation)?;
        Ok(())
    }

    /// Produces the one ordered request graph while preserving the opaque pending half for the
    /// later exact `SealedProviderCaptureSetReceipt` rejoin.
    fn into_sealing_parts(
        self,
        cancellation: &CancellationToken,
    ) -> Result<(Self, ProviderCaptureMaterial), SecClientError> {
        self.revalidate(cancellation)?;
        let Self {
            dataset,
            filing,
            submissions,
            filing_document,
            filing_representation,
            taxonomy,
            raw_store,
            source_id,
            metadata_revision,
            parser_limits,
        } = self;
        let current_material = submissions
            .current_component()
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let filing_material = filing_document
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let (taxonomy, taxonomy_materials) = taxonomy.into_sealing_parts(cancellation)?;
        let material_count = taxonomy_materials
            .len()
            .checked_add(2)
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let mut components = Vec::new();
        components
            .try_reserve_exact(material_count)
            .map_err(|_| SecClientError::AllocationFailed)?;
        components.push(current_material);
        components.push(filing_material);
        components.extend(taxonomy_materials);
        let graph_identity = filing_xbrl_request_graph_identity(&dataset, &components)?;
        let material = ProviderCaptureMaterial::try_combine_request_graph(
            source_id.clone(),
            metadata_revision.clone(),
            dataset.dataset().clone(),
            graph_identity,
            components,
        )?;
        Ok((
            Self {
                dataset,
                filing,
                submissions,
                filing_document,
                filing_representation,
                taxonomy,
                raw_store,
                source_id,
                metadata_revision,
                parser_limits,
            },
            material,
        ))
    }
}

// Immutable parser inputs retained after the exact capture graph and taxonomy were verified.
struct SecPreparedFilingXbrlExtraction {
    dataset: SecResearchDataset,
    submissions: RetrievedSubmissions,
    filing_document: RetrievedSecBytes,
    document_context: XbrlDocumentContext,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    parser_limits: SecParserLimits,
}

impl SecPreparedFilingXbrlExtraction {
    fn checked_retained_bytes(&self) -> Result<usize, SecClientError> {
        let submissions = self.submissions.checked_retained_bytes()?;
        let document = self.filing_document.checked_retained_bytes()?;
        let company = self.submissions.document().company_metadata();
        let company_slots = company
            .former_names()
            .len()
            .checked_mul(size_of::<FormerCompanyName>())
            .and_then(|n| {
                n.checked_add(
                    company
                        .ticker_exchange_pairs()
                        .len()
                        .checked_mul(size_of::<ProviderReportedSecurityAssociation>())?,
                )
            })
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(size_of::<CompanyIdentityObservation>()))
            .ok_or(SecClientError::ResponseTooLarge)?;
        size_of::<Self>()
            .checked_add(self.dataset.checked_dynamic_retained_bytes().ok_or(SecClientError::ResponseTooLarge)?)
            .and_then(|n| n.checked_add(submissions))
            // Reserve the full source allocation again for projected company strings/evidence,
            // plus destination vector capacity and header before projection construction.
            .and_then(|n| n.checked_add(submissions))
            .and_then(|n| n.checked_add(company_slots))
            .and_then(|n| n.checked_add(document))
            .and_then(|n| n.checked_add(self.document_context.checked_dynamic_retained_bytes(self.dataset.xbrl_taxonomy()?)?))
            .and_then(|n| n.checked_add(self.source_id.retained_bytes()))
            .and_then(|n| n.checked_add(self.metadata_revision.as_source_identifier().retained_bytes()))
            .ok_or(SecClientError::ResponseTooLarge)
    }
}

/// Closed filing-XBRL capture graph ready for bounded canonical extraction.
///
/// Construction proves the accession belongs to the exact captured current submissions object,
/// the primary filing document is the retained object named by that filing, and every admitted
/// taxonomy artifact is an exact captured official artifact. Consumption produces the canonical
/// batch and the same one-use raw graph material; callers cannot replace either half.
pub struct SecFilingXbrlCaptureHandoff {
    pending: SecPreparedFilingXbrlExtraction,
    capture_material: ProviderCaptureMaterial,
}

impl std::fmt::Debug for SecFilingXbrlCaptureHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecFilingXbrlCaptureHandoff")
            .field("dataset", self.pending.dataset.dataset())
            .field(
                "accession",
                &self
                    .pending
                    .dataset
                    .filing_xbrl_coordinates()
                    .map(SecFilingXbrlCoordinates::accession),
            )
            .field("capture", self.capture_material.receipt())
            .finish_non_exhaustive()
    }
}

impl SecFilingXbrlCaptureHandoff {
    /// Returns the exact accession/document/taxonomy dataset selected by this graph.
    pub const fn dataset(&self) -> &SecResearchDataset {
        &self.pending.dataset
    }

    /// Returns the complete exact response graph that must enter the common physical sealer.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    /// Consumes this graph into one bounded canonical/native extraction and its inseparable raw
    /// material. This is CPU-bound and should be called from the application's blocking executor.
    pub fn extract(
        self,
        authority: ExtractionAuthority,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
        deadline: Timestamp,
        cancellation: CancellationToken,
        scratch_parent: &std::path::Path,
    ) -> Result<(SecResearchExtractionStream, ProviderCaptureMaterial), SecClientError> {
        extract_filing_xbrl_handoff(
            self.pending,
            self.capture_material,
            authority,
            max_records,
            max_bytes,
            deadline,
            &cancellation,
            scratch_parent,
        )
    }
}

impl SecDiscoveryResult {
    /// Returns the ordinary source-neutral discovery batch.
    pub const fn batch(&self) -> &DiscoveryBatch {
        &self.batch
    }

    /// Returns the exact bounded body-only provider material backing this discovery.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture_material
    }

    /// Consumes the capture-first application handoff.
    pub fn into_parts(self) -> (DiscoveryBatch, ProviderCaptureMaterial) {
        (self.batch, self.capture_material)
    }
}

impl SecExtractionResult {
    /// Returns the ordinary source-agnostic analytical batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns identity evidence parsed from the same exact retrieved representation.
    pub const fn company_identity(&self) -> Option<&CompanyIdentityObservation> {
        self.company_identity.as_ref()
    }

    /// Returns mandatory provider-native lineage beside the canonical batch.
    pub const fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        &self.native_lineage
    }

    /// Returns the exact captured response page that authored each canonical row.
    ///
    /// Ordinals are aligned one-for-one with the canonical batch and native-lineage rows. For
    /// complete submissions, page zero is current submissions and later pages follow the SEC
    /// declared companion order. Company Facts is the sole standalone page zero.
    pub fn row_capture_page_ordinals(&self) -> &[u16] {
        &self.row_capture_page_ordinals
    }

    /// Consumes this result into its canonical, identity, provider-native, and physical-row map.
    pub fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        Option<CompanyIdentityObservation>,
        ProviderNativeLineageBatch,
        Vec<u16>,
    ) {
        (
            self.batch,
            self.company_identity,
            self.native_lineage,
            self.row_capture_page_ordinals,
        )
    }
}

impl SecEdgarSource {
    /// Discovers one SEC source object together with every exact HTTP body required for raw
    /// publication.
    ///
    /// Complete submissions produces one terminal ordered capture containing the current object
    /// and every provider-declared companion. Company Facts produces one standalone capture.
    pub fn discover_with_capture(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<SecDiscoveryResult, ExtractionSourceError>> {
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let child = cancellation.child_token();
            let remaining = deadline_remaining(request.deadline())?;
            let dataset = SecResearchDataset::try_from_identifier(request.dataset())
                .map_err(map_client_error)?;
            let discovered = tokio::time::timeout(remaining, async {
                match dataset.kind() {
                    SecResearchDatasetKind::Submissions => self
                        .fetch_complete_submissions(
                            &authority,
                            dataset.cik(),
                            SecCompositeBounds::production_defaults(),
                            request.deadline(),
                            child.clone(),
                        )
                        .await
                        .and_then(|value| {
                            let capture_material = value
                                .capture_material()?
                                .ok_or(SecClientError::InvalidCaptureMaterial)?;
                            Ok(DiscoveredSecMaterial {
                                raw: value.raw().clone(),
                                object_id: dataset.source_object_id().clone(),
                                capture_material,
                                media_type: SourceIdentifier::try_from("application/json")?,
                                published_at: None,
                                availability: extraction_availability(value.raw().availability()),
                            })
                        }),
                    SecResearchDatasetKind::CompanyFacts => self
                        .fetch_company_facts(&authority, dataset.cik(), child.clone())
                        .await
                        .and_then(|value| {
                            let object_id = value
                                .raw()
                                .locator()
                                .ok_or(SecClientError::InvalidCompositeRepresentation)
                                .and_then(|locator| {
                                    SourceIdentifier::try_from(locator).map_err(Into::into)
                                })?;
                            let capture_material = value
                                .capture_material()?
                                .ok_or(SecClientError::InvalidCaptureMaterial)?;
                            Ok(DiscoveredSecMaterial {
                                raw: value.raw().clone(),
                                object_id,
                                capture_material,
                                media_type: SourceIdentifier::try_from("application/json")?,
                                published_at: None,
                                availability: extraction_availability(value.raw().availability()),
                            })
                        }),
                    SecResearchDatasetKind::FilingXbrl => {
                        Err(SecClientError::InvalidCompositeRepresentation)
                    }
                }
            })
            .await
            .map_err(|_| {
                child.cancel();
                ExtractionSourceError::DeadlineExceeded
            })?
            .inspect_err(|error| trace_protocol_failure(error, "discovery", dataset.kind()))
            .map_err(map_client_error)?;
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let capture_identity = SourceObjectCaptureIdentity::try_from_capture(
                discovered.capture_material.receipt(),
            )
            .map_err(|_| invalid_protocol())?;
            let object = SourceObject::try_new_with_capture_identity(
                self.metadata().source_id().clone(),
                self.metadata().revision().clone(),
                &request,
                discovered.object_id,
                discovered.media_type,
                retrieved_payload_evidence(&discovered.raw).map_err(|_| invalid_protocol())?,
                capture_identity,
                market_squawk_domain::EffectiveInterval::new(discovered.raw.received_at(), None)
                    .map_err(|_| invalid_protocol())?,
                discovered.published_at,
                discovered.availability,
                Some(u64::try_from(discovered.raw.bytes().len()).map_err(|_| invalid_protocol())?),
            )?;
            let batch = DiscoveryBatch::try_new(&request, vec![object])?;
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            Ok(SecDiscoveryResult {
                batch,
                capture_material: discovered.capture_material,
            })
        })
    }

    /// Builds revision evidence aligned to one extracted SEC batch.
    ///
    /// Submissions and Filing XBRL retain locally observed native content revisions: a filing
    /// accession identifies the filing, but its published metadata can change. Company Facts
    /// retains exact source-record version and publication ordering evidence. Final immutable
    /// revision numbers remain owned by the shared publication plan.
    ///
    /// # Errors
    ///
    /// Returns [`SecClientError::RegistrationMismatch`] when the batch belongs to another source
    /// metadata revision. Returns [`SecClientError::InvalidCompositeRepresentation`] when a record
    /// lacks conservative availability, and [`SecClientError::RevisionAuthority`] when bounded
    /// exact-evidence invariants fail.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, SecClientError> {
        if batch.request().object().source_id() != self.metadata().source_id()
            || batch.request().object().metadata_revision() != self.metadata().revision()
        {
            return Err(SecClientError::RegistrationMismatch);
        }
        let dataset = batch.request().object().dataset();
        // Filing identifiers commit their retained coordinates rather than encoding a CIK,
        // so they do not use the company-dataset parser.
        if dataset.as_str().starts_with(SEC_FILING_XBRL_DATASET_PREFIX)
            || SecResearchDataset::try_from_identifier(dataset)
                .map_err(|_| SecClientError::InvalidCompositeRepresentation)?
                .kind()
                == SecResearchDatasetKind::Submissions
        {
            return ExtractionRevisionPlan::locally_observed_with_native_lineage(
                batch.records().len(),
            )
            .map_err(Into::into);
        }
        let mut evidence = Vec::new();
        evidence
            .try_reserve_exact(batch.records().len())
            .map_err(|_| SecClientError::AllocationFailed)?;
        for record in batch.records() {
            let version = record.revision().as_str().as_bytes();
            let published = record
                .published_time()
                .cloned()
                .ok_or(SecClientError::InvalidCompositeRepresentation)?;
            let order = ObservedProviderOrder::try_new(published, version)?;
            evidence.push(ExtractionRevisionEvidence::provider_supplied(
                version, order,
            )?);
        }
        ExtractionRevisionPlan::try_new_with_native_lineage(evidence).map_err(Into::into)
    }

    /// Opens complete CompanyFacts as bounded canonical/native chunks from the exact discovered
    /// capture. Whole-input parsing keeps its existing admission; chunk size never limits the
    /// complete occurrence set. Raw material remains inseparable from its admitted stream.
    pub fn extract_company_facts_stream(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        material: ProviderCaptureMaterial,
        cancellation: CancellationToken,
    ) -> BoxFuture<
        '_,
        Result<(SecResearchExtractionStream, ProviderCaptureMaterial), ExtractionSourceError>,
    > {
        let raw_store = self.raw_store();
        let source_id = self.metadata().source_id().clone();
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let remaining = deadline_remaining(request.deadline())?;
            let worker_cancellation = cancellation.child_token();
            let worker_authority = authority.clone();
            let worker = self.run_validation_blocking(&worker_cancellation, move |token| {
                let stream = company_facts_stream_blocking(
                    request,
                    raw_store,
                    source_id,
                    worker_authority,
                    &material,
                    token,
                )?;
                Ok((stream, material))
            });
            tokio::pin!(worker);
            tokio::select! {
                result = &mut worker => {
                    let result = result.inspect_err(|error| trace_protocol_failure(error, "extraction", SecResearchDatasetKind::CompanyFacts)).map_err(map_client_error)?;
                    self.validate_authority(&authority).map_err(map_client_error)?;
                    Ok(result)
                },
                () = tokio::time::sleep(remaining) => { worker_cancellation.cancel(); let _ = worker.await; Err(ExtractionSourceError::DeadlineExceeded) },
                () = cancellation.cancelled() => { worker_cancellation.cancel(); let _ = worker.await; Err(ExtractionSourceError::Cancelled) },
            }
        })
    }

    /// Extracts SEC analytical records with company identity from the same exact source bytes.
    ///
    /// The capture-first application bridge consumes this result together with exact raw material.
    /// The generic [`ExtractionSource`] entry point cannot publish SEC company evidence; the
    /// application retains company identity and mandatory native lineage from this same parse.
    pub fn extract_with_company_identity(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<SecExtractionResult, ExtractionSourceError>> {
        let raw_store = self.raw_store();
        let source_id = self.metadata().source_id().clone();
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let remaining = deadline_remaining(request.deadline())?;
            let family = SecResearchDataset::try_from_identifier(request.object().dataset())
                .map(|dataset| dataset.kind());
            let worker_cancellation = cancellation.child_token();
            let worker_authority = authority.clone();
            let worker = self.run_validation_blocking(&worker_cancellation, move |worker_token| {
                extract_blocking(
                    request,
                    raw_store,
                    source_id,
                    worker_authority,
                    worker_token,
                )
            });
            tokio::pin!(worker);
            tokio::select! {
                result = &mut worker => {
                    let extracted = result.inspect_err(|error| {
                        if let Ok(family) = family {
                            trace_protocol_failure(error, "extraction", family);
                        }
                    }).map_err(map_client_error)?;
                    self.validate_authority(&authority).map_err(map_client_error)?;
                    Ok(extracted)
                },
                () = tokio::time::sleep(remaining) => {
                    worker_cancellation.cancel();
                    Err(ExtractionSourceError::DeadlineExceeded)
                }
                () = cancellation.cancelled() => {
                    worker_cancellation.cancel();
                    Err(ExtractionSourceError::Cancelled)
                }
            }
        })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "captured filing, source authority, parser bounds, and exact taxonomy bodies remain explicit"
)]
pub(crate) fn admit_filing_xbrl_root_from_sealed_capture(
    sealed_root: ProviderWholeCaptureToken,
    raw_store: Arc<RawEvidenceStore>,
    representation_registry: Arc<SecRepresentationRegistry>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    parser_limits: SecParserLimits,
    submissions: &RetrievedSubmissions,
    accession: &str,
    filing_document: &RetrievedSecBytes,
    cancellation: &CancellationToken,
) -> Result<AdmittedFilingXbrlRoot, SecClientError> {
    if Some(sealed_root.persisted_receipt().capture()) != filing_document.capture_receipt() {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let filing = SecFilingXbrlCoordinates::from_captured_current_submissions(
        submissions,
        accession,
        &raw_store,
        &source_id,
        &metadata_revision,
        parser_limits,
        cancellation,
    )?;
    let locator = SecObjectLocator::filing_document(
        filing.cik(),
        filing.accession().as_str(),
        filing.document().as_str(),
    )?;
    let filing_representation = representation_registry
        .representation_for_source(&source_id, locator.url())?
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    let receipt = filing_document
        .capture_receipt()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    if receipt.source_id() != &source_id
        || receipt.metadata_revision() != &metadata_revision
        || receipt.dataset().as_str() != locator.url()
        || receipt.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || receipt.pages().len() != 1
        || receipt.pages()[0].body_digest() != filing_document.evidence()
        || receipt.pages()[0].body_bytes()
            != u64::try_from(filing_document.bytes().len())
                .map_err(|_| SecClientError::ResponseTooLarge)?
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    validate_captured_filing_document(
        &filing,
        &filing_document,
        &filing_representation,
        &raw_store,
        &source_id,
        &metadata_revision,
        cancellation,
    )?;
    Ok(AdmittedFilingXbrlRoot {
        sealed_root,
        filing,
        filing_document: filing_document.clone(),
        filing_representation,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "admitted filing authority, parser bounds, and exact taxonomy bodies remain explicit"
)]
pub(crate) fn prepare_filing_xbrl_capture_from_admitted_root(
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    parser_limits: SecParserLimits,
    submissions: RetrievedSubmissions,
    admitted_root: AdmittedFilingXbrlRoot,
    taxonomy_artifacts: Vec<RetrievedSecBytes>,
    cancellation: &CancellationToken,
) -> Result<SecFilingXbrlCaptureHandoff, SecClientError> {
    let AdmittedFilingXbrlRoot {
        sealed_root,
        filing,
        filing_document,
        filing_representation,
    } = admitted_root;
    if Some(sealed_root.persisted_receipt().capture()) != filing_document.capture_receipt() {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let taxonomy = SecXbrlTaxonomyRegistry::code_owned().try_admit_captured(
        Arc::clone(&raw_store),
        &source_id,
        &metadata_revision,
        &filing_document,
        taxonomy_artifacts,
        parser_limits,
        cancellation,
    )?;
    let dataset = SecResearchDataset::filing_xbrl(filing.clone(), taxonomy.validated().clone())?;
    let pending = SecPendingFilingXbrlAdmission {
        dataset,
        filing,
        submissions,
        filing_document,
        filing_representation,
        taxonomy,
        raw_store,
        source_id,
        metadata_revision,
        parser_limits,
    };
    let (pending, capture_material) = pending.into_sealing_parts(cancellation)?;
    // Finish immutable custody validation before minting the opaque extraction capability. This
    // context cannot be substituted by a caller. Mutable source/lifecycle authority is still
    // checked on every extraction and at shared publication.
    let document_context = XbrlDocumentContext::from_validated_taxonomy(
        pending.filing.accession().clone(),
        SourceIdentifier::try_from(pending.filing.cik())?,
        &pending.taxonomy,
        retrieved_payload_evidence(&pending.filing_document)?,
        pending.filing_document.received_at(),
        cancellation,
    )?;
    let SecPendingFilingXbrlAdmission {
        dataset,
        submissions,
        filing_document,
        source_id,
        metadata_revision,
        parser_limits,
        ..
    } = pending;
    Ok(SecFilingXbrlCaptureHandoff {
        pending: SecPreparedFilingXbrlExtraction {
            dataset,
            submissions,
            filing_document,
            document_context,
            source_id,
            metadata_revision,
            parser_limits,
        },
        capture_material,
    })
}

impl ExtractionSource for SecEdgarSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let _ = (authority, request, cancellation);
        Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the closed graph, exact extraction bounds, deadline, and authorities remain explicit"
)]
fn extract_filing_xbrl_handoff(
    pending: SecPreparedFilingXbrlExtraction,
    capture_material: ProviderCaptureMaterial,
    authority: ExtractionAuthority,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
    deadline: Timestamp,
    cancellation: &CancellationToken,
    scratch_parent: &std::path::Path,
) -> Result<(SecResearchExtractionStream, ProviderCaptureMaterial), SecClientError> {
    authority.validate_current()?;
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let maximum = usize::try_from(max_bytes.get().min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES))
        .map_err(|_| SecClientError::ResponseTooLarge)?;
    let fixed_retained = pending
        .checked_retained_bytes()?
        .checked_add(capture_material.checked_plain_retained_bytes()?)
        .ok_or(SecClientError::ResponseTooLarge)?;
    // Canonical serialization may retain both the growing buffer and its ownership transition.
    let record_scratch = MAX_EXTRACTION_RECORD_BYTES
        .checked_mul(2)
        .ok_or(SecClientError::ResponseTooLarge)?;
    let parser_admission = maximum
        .checked_sub(fixed_retained)
        .and_then(|n| n.checked_sub(record_scratch))
        .filter(|n| *n > 0)
        .ok_or(SecClientError::ResponseTooLarge)?;
    if authority.metadata().source_id() != &pending.source_id
        || authority.metadata().revision() != &pending.metadata_revision
    {
        return Err(SecClientError::RegistrationMismatch);
    }
    let capture = capture_material.receipt();
    if capture.source_id() != &pending.source_id
        || capture.metadata_revision() != &pending.metadata_revision
        || capture.dataset() != pending.dataset.dataset()
        || capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || capture.request_graph_components().len() < 3
        || capture.request_graph_components()[0].dataset().as_str()
            != pending
                .submissions
                .current_component()
                .locator()
                .ok_or(SecClientError::InvalidCaptureMaterial)?
        || capture.request_graph_components()[1].dataset().as_str()
            != pending
                .filing_document
                .locator()
                .ok_or(SecClientError::InvalidCaptureMaterial)?
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }

    let received_at = capture
        .pages()
        .iter()
        .map(|page| page.received_at())
        .max()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let filing = pending
        .dataset
        .filing_xbrl_coordinates()
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    let (published_at, availability) = filing_discovery_availability(filing, received_at)?;
    let discovery = DiscoveryRequest::try_new(
        pending.dataset.dataset().clone(),
        None,
        NonZeroU16::new(1).ok_or(SecClientError::InvalidCompositeRepresentation)?,
        deadline,
    )
    .map_err(map_extraction_contract_error)?;
    let object = SourceObject::try_new_with_capture_identity(
        pending.source_id.clone(),
        pending.metadata_revision.clone(),
        &discovery,
        pending.dataset.source_object_id().clone(),
        filing_media_type(filing.document())?,
        ExactPayloadEvidence::from_content_digest(capture.content_digest()),
        SourceObjectCaptureIdentity::try_from_capture(capture)?,
        EffectiveInterval::new(received_at, None)
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?,
        published_at,
        availability,
        Some(capture.total_body_bytes()),
    )
    .map_err(map_extraction_contract_error)?;
    let request = ExtractionRequest::try_new(object, max_records, max_bytes, deadline)
        .map_err(map_extraction_contract_error)?;

    let request_retained = usize::try_from(
        request
            .dynamic_retained_bytes()
            .map_err(map_extraction_contract_error)?,
    )
    .map_err(|_| SecClientError::ResponseTooLarge)?
    .checked_add(size_of::<ExtractionRequest>())
    .ok_or(SecClientError::ResponseTooLarge)?;
    // Original/local/accumulator requests and two in-flight record metadata/clone allowances.
    let request_owners = request_retained
        .checked_mul(5)
        .ok_or(SecClientError::ResponseTooLarge)?;
    let parser_admission = parser_admission
        .checked_sub(request_owners)
        .filter(|n| *n > 0)
        .ok_or(SecClientError::ResponseTooLarge)?;
    let parser_limits = pending
        .parser_limits
        .intersect(request_parser_limits_with_record_limit(
            &request,
            pending.filing_document.bytes().len(),
            0,
            // This filing is indexed before bounded extraction chunks are emitted. The chunk
            // row window is not the complete parser's occurrence/work admission.
            pending.parser_limits.records(),
        )?)?
        .with_retained_bytes(parser_admission)?;
    let document = XbrlDocumentParser::parse_indexed_in_with_cancellation(
        pending.filing_document.bytes(),
        parser_limits,
        pending.document_context,
        cancellation,
        scratch_parent,
    )?;
    let ingested_at = crate::client::system_timestamp()?;
    let company_identity = company_identity_from_submissions(
        &request,
        &pending.source_id,
        &pending.submissions,
        CompanyIdentitySurface::SecFilingXbrl,
        ingested_at,
        cancellation,
    )?;
    let normalized = normalize_filing_xbrl_with_cancellation(
        &pending.source_id,
        pending.dataset,
        document,
        pending.filing_document.evidence(),
        pending.filing_document.received_at(),
        ingested_at,
        parser_admission,
        cancellation,
        scratch_parent,
    )?;
    let normalization_retained = normalized.working_set_retained_bytes();
    let canonical_admission = parser_admission
        .checked_sub(normalization_retained)
        .filter(|n| *n > 0)
        .ok_or(SecClientError::ResponseTooLarge)?;
    let request = ExtractionRequest::try_new(
        request.object().clone(),
        max_records,
        NonZeroU64::new(
            u64::try_from(canonical_admission).map_err(|_| SecClientError::ResponseTooLarge)?,
        )
        .ok_or(SecClientError::ResponseTooLarge)?,
        deadline,
    )
    .map_err(map_extraction_contract_error)?;
    if normalized.numeric_fact_count() == 0 {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    Ok((
        SecResearchExtractionStream {
            authority,
            request,
            normalized: SecResearchNormalization::FilingXbrl(normalized),
            company_identity,
            pending_record: None,
            emitted: 0,
            finished: false,
            maximum_working_bytes: canonical_admission,
        },
        capture_material,
    ))
}

#[derive(Debug)]
enum SecResearchNormalization {
    FilingXbrl(SecFilingXbrlNormalization),
    CompanyFacts(SecCompanyFactsNormalization),
}

impl SecResearchNormalization {
    const fn family(&self) -> SecResearchDatasetKind {
        match self {
            Self::FilingXbrl(_) => SecResearchDatasetKind::FilingXbrl,
            Self::CompanyFacts(_) => SecResearchDatasetKind::CompanyFacts,
        }
    }
    const fn total_records(&self) -> usize {
        match self {
            Self::FilingXbrl(value) => value.numeric_fact_count(),
            Self::CompanyFacts(value) => value.ordered.len(),
        }
    }
    const fn capture_page_ordinal(&self) -> u16 {
        match self {
            Self::FilingXbrl(_) => 1,
            Self::CompanyFacts(_) => 0,
        }
    }
    fn try_next_observation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<ResearchObservation>, SecNormalizationError> {
        match self {
            Self::FilingXbrl(value) => value.try_next_observation(cancellation),
            Self::CompanyFacts(value) => value.try_next_observation(cancellation),
        }
    }
    fn native_for_batch(
        &mut self,
        batch: &ExtractionBatch,
        start: usize,
        maximum_retained_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<ProviderNativeLineageBatch, market_squawk_sources::ProviderNativeLineageError> {
        match self {
            Self::FilingXbrl(value) => {
                value.native_for_batch(batch, start, maximum_retained_bytes, cancellation)
            }
            Self::CompanyFacts(value) => {
                value.native_for_batch(batch, start, maximum_retained_bytes, cancellation)
            }
        }
    }
}

/// The bounded parsed source remains owned while canonical/native rows are emitted together.
/// Only source-row indices and the preceding family revision cross chunk boundaries; there is
/// no complete canonical-observation vector and no per-chunk reset of revision ordering.
#[derive(Debug)]
struct SecCompanyFactsNormalization {
    source_id: SourceId,
    retrieved: RetrievedCompanyFacts,
    ingested_at: Timestamp,
    ordered: Vec<usize>,
    next: usize,
    family_revision: u32,
}
impl SecCompanyFactsNormalization {
    fn try_new(
        source_id: SourceId,
        retrieved: RetrievedCompanyFacts,
        ingested_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecClientError> {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let occurrences = retrieved.document().occurrences();
        let mut ordered = Vec::new();
        ordered
            .try_reserve_exact(occurrences.len())
            .map_err(|_| SecClientError::AllocationFailed)?;
        ordered.extend(0..occurrences.len());
        ordered.sort_unstable_by(|left, right| {
            compare_company_facts(&occurrences[*left], &occurrences[*right])
        });
        Ok(Self {
            source_id,
            retrieved,
            ingested_at,
            ordered,
            next: 0,
            family_revision: 0,
        })
    }
    fn try_next_observation(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<ResearchObservation>, SecNormalizationError> {
        if cancellation.is_cancelled() {
            return Err(SecNormalizationError::Cancelled);
        }
        let Some(&ordinal) = self.ordered.get(self.next) else {
            return Ok(None);
        };
        let occurrences = self.retrieved.document().occurrences();
        let occurrence = &occurrences[ordinal];
        if self.next > 0
            && same_company_fact_family(&occurrences[self.ordered[self.next - 1]], occurrence)
        {
            self.family_revision = self
                .family_revision
                .checked_add(1)
                .ok_or(SecNormalizationError::RevisionOverflow)?;
        } else {
            self.family_revision = 1;
        }
        let observation = normalize_company_fact_occurrence(
            &self.source_id,
            &self.retrieved,
            occurrence,
            self.family_revision,
            self.ingested_at,
        )?;
        self.next += 1;
        Ok(Some(observation))
    }
    fn native_for_batch(
        &self,
        batch: &ExtractionBatch,
        start: usize,
        maximum_retained_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<ProviderNativeLineageBatch, market_squawk_sources::ProviderNativeLineageError> {
        use market_squawk_sources::ProviderNativeLineageError;
        let end = start
            .checked_add(batch.records().len())
            .ok_or(ProviderNativeLineageError::AlignmentMismatch)?;
        let ordered = self
            .ordered
            .get(start..end)
            .ok_or(ProviderNativeLineageError::AlignmentMismatch)?;
        let document = self.retrieved.document();
        let mut native = ProviderNativeLineageBatchBuilder::try_new_bounded(
            ProviderNativeLineageImplementation::SecEdgarV1,
            batch,
            maximum_retained_bytes,
        )?;
        native.try_set_batch_sidecar(&SecCompanyFactsNativeBatchV1 {
            version: 1,
            family: "company_facts",
            dataset: batch.request().object().dataset(),
            cik: document.cik(),
            entity_name: document.entity_name(),
        })?;
        for ordinal in ordered {
            if cancellation.is_cancelled() {
                return Err(ProviderNativeLineageError::SerializationFailure);
            }
            native.try_push(&SecCompanyFactsNativeRowV1 {
                family: "company_fact",
                occurrence: &document.occurrences()[*ordinal],
            })?;
        }
        native.finish()
    }
}

/// Complete validated SEC research producer. Each yielded range is bounded; EOF is mandatory
/// before the common logical-publication owner seals the complete family canonical identity.
#[derive(Debug)]
pub struct SecResearchExtractionStream {
    authority: ExtractionAuthority,
    request: ExtractionRequest,
    normalized: SecResearchNormalization,
    company_identity: CompanyIdentityObservation,
    pending_record: Option<ExtractionRecord>,
    emitted: usize,
    finished: bool,
    maximum_working_bytes: usize,
}
impl SecResearchExtractionStream {
    pub const fn request(&self) -> &ExtractionRequest {
        &self.request
    }
    pub const fn total_records(&self) -> usize {
        self.normalized.total_records()
    }
    pub const fn company_identity(&self) -> &CompanyIdentityObservation {
        &self.company_identity
    }
    /// Returns the independently evidenced family represented by this complete stream.
    pub const fn family(&self) -> SecResearchDatasetKind {
        self.normalized.family()
    }
    pub const fn emitted_records(&self) -> usize {
        self.emitted
    }
    pub fn next_chunk(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<SecExtractionResult>, SecClientError> {
        self.authority.validate_current()?;
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        if crate::client::system_timestamp()? >= self.request.deadline() {
            return Err(SecClientError::DeadlineExceeded);
        }
        if self.finished {
            return Ok(None);
        }
        let start = self.emitted;
        let mut count = 0usize;
        let mut records = ExtractionBatchAccumulator::try_new(&self.request)
            .map_err(map_extraction_contract_error)?;
        while count < 256 && count < self.request.max_records() as usize {
            if crate::client::system_timestamp()? >= self.request.deadline() {
                return Err(SecClientError::DeadlineExceeded);
            }
            let record = if let Some(record) = self.pending_record.take() {
                record
            } else {
                let Some(observation) = self.normalized.try_next_observation(cancellation)? else {
                    self.finished = true;
                    break;
                };
                canonical_record(&self.request, observation, &self.authority, cancellation)?
            };
            if let Some(record) = records
                .try_push_or_return(record)
                .map_err(map_extraction_contract_error)?
            {
                self.pending_record = Some(record);
                break;
            }
            count += 1;
        }
        if count == 0 {
            if self.finished && self.emitted == self.total_records() {
                return Ok(None);
            }
            return Err(SecClientError::ResponseTooLarge);
        }
        let batch = records.finish().map_err(map_extraction_contract_error)?;
        let batch_bytes =
            usize::try_from(batch.total_bytes().map_err(map_extraction_contract_error)?)
                .map_err(|_| SecClientError::ResponseTooLarge)?;
        let native_admission = self
            .maximum_working_bytes
            .checked_sub(batch_bytes)
            .and_then(|bytes| bytes.checked_sub(count * 2 * size_of::<u16>()))
            .ok_or(SecClientError::ResponseTooLarge)?;
        let native_result =
            self.normalized
                .native_for_batch(&batch, start, native_admission, cancellation);
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        if crate::client::system_timestamp()? >= self.request.deadline() {
            return Err(SecClientError::DeadlineExceeded);
        }
        let native_lineage =
            native_result.map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
        self.emitted = self
            .emitted
            .checked_add(count)
            .ok_or(SecClientError::ResponseTooLarge)?;
        if self.emitted > self.total_records()
            || (self.finished && self.emitted != self.total_records())
        {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        self.authority.validate_current()?;
        Ok(Some(SecExtractionResult {
            batch,
            company_identity: Some(self.company_identity.clone()),
            native_lineage,
            row_capture_page_ordinals: vec![self.normalized.capture_page_ordinal(); count],
        }))
    }
}

pub(crate) fn company_facts_stream_blocking(
    request: ExtractionRequest,
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    authority: ExtractionAuthority,
    material: &ProviderCaptureMaterial,
    cancellation: &CancellationToken,
) -> Result<SecResearchExtractionStream, SecClientError> {
    authority.validate_current()?;
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let dataset = SecResearchDataset::try_from_identifier(request.object().dataset())?;
    let capture = material.receipt();
    if dataset.kind() != SecResearchDatasetKind::CompanyFacts
        || authority.metadata().source_id() != &source_id
        || request.object().source_id() != &source_id
        || request.object().metadata_revision() != authority.metadata().revision()
        || request.object().object_id() != dataset.source_object_id()
        || capture.source_id() != &source_id
        || capture.metadata_revision() != authority.metadata().revision()
        || capture.dataset() != dataset.dataset()
        || capture.pages().len() != 1
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || SourceObjectCaptureIdentity::try_from_capture(capture)?
            != request.object().capture_identity()
        || capture.pages()[0].body_digest() != request.object().evidence().content_digest()
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let maximum = request
        .max_bytes()
        .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES);
    let bytes = raw_store.read_verified_bounded_cancellable(
        &request.object().evidence().content_digest(),
        maximum,
        cancellation,
    )?;
    let parser_limits = request_parser_limits(&request, bytes.len(), bytes.capacity())?;
    let received_at = request.object().effective_interval().starts_at();
    // Representation availability remains its first observed content clock. A physical
    // response has its own receipt clock: the first response precedes representation
    // persistence, while an unchanged later response follows it. Neither clock replaces
    // the other; both must precede this admitted extraction and its absolute deadline.
    let observed_at = crate::client::system_timestamp()?;
    if received_at > observed_at
        || received_at > request.deadline()
        || capture.pages()[0].received_at() > observed_at
        || capture.pages()[0].received_at() > request.deadline()
        || capture.pages()[0].body_bytes() != bytes.len() as u64
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let retrieved = RetrievedCompanyFacts::restored(
        bytes,
        request.object().evidence().content_digest(),
        received_at,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        parser_limits,
        cancellation,
    )?;
    if retrieved.document().cik().as_str() != dataset.cik() {
        return Err(SecClientError::ResponseCikMismatch);
    }
    let ingested_at = crate::client::system_timestamp()?;
    let company_identity = company_identity_from_company_facts(
        &request,
        &source_id,
        &retrieved,
        ingested_at,
        cancellation,
    )?;
    let normalized =
        SecCompanyFactsNormalization::try_new(source_id, retrieved, ingested_at, cancellation)?;
    if normalized.ordered.is_empty() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    authority.validate_current()?;
    Ok(SecResearchExtractionStream {
        authority,
        request,
        normalized: SecResearchNormalization::CompanyFacts(normalized),
        company_identity,
        pending_record: None,
        emitted: 0,
        finished: false,
        maximum_working_bytes: usize::try_from(maximum)
            .map_err(|_| SecClientError::ResponseTooLarge)?,
    })
}

fn extract_blocking(
    request: ExtractionRequest,
    raw_store: Arc<RawEvidenceStore>,
    source_id: SourceId,
    authority: ExtractionAuthority,
    cancellation: &CancellationToken,
) -> Result<SecExtractionResult, SecClientError> {
    authority.validate_current()?;
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let working_set_limit = request
        .max_bytes()
        .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES);
    let bytes = raw_store.read_verified_bounded_cancellable(
        &request.object().evidence().content_digest(),
        working_set_limit,
        cancellation,
    )?;
    authority.validate_current()?;
    let received_at = request.object().effective_interval().starts_at();
    let availability = AvailabilityEvidence::LocalFirstObserved {
        observed_at: received_at,
    };
    let parser_limits = request_parser_limits(&request, bytes.len(), bytes.capacity())?;
    let ingested_at = crate::client::system_timestamp()?;
    let dataset = SecResearchDataset::try_from_identifier(request.object().dataset())
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    let (batch, company_identity, native_lineage, row_capture_page_ordinals) = match dataset.kind()
    {
        SecResearchDatasetKind::Submissions => {
            let retrieved = crate::composite::restore_online_submissions(
                &raw_store,
                &bytes,
                request.object().evidence().content_digest(),
                SecCompositeBounds::production_defaults(),
                parser_limits,
                cancellation,
            )?;
            if retrieved.document().cik().as_str() != dataset.cik() {
                return Err(SecClientError::ResponseCikMismatch);
            }
            let observations = normalize_filings_with_cancellation(
                &source_id,
                &retrieved,
                ingested_at,
                cancellation,
            )?;
            let company_identity = company_identity_from_submissions(
                &request,
                &source_id,
                &retrieved,
                CompanyIdentitySurface::SecSubmissions,
                ingested_at,
                cancellation,
            )?;
            let batch = canonical_batch(&request, observations, &authority, cancellation)?;
            let native_lineage = submissions_native_lineage(&request, &retrieved, &batch)?;
            let row_capture_page_ordinals = submissions_row_capture_page_ordinals(
                &request,
                &retrieved,
                &batch,
                parser_limits,
                cancellation,
            )?;
            (
                batch,
                Some(company_identity),
                native_lineage,
                row_capture_page_ordinals,
            )
        }
        SecResearchDatasetKind::CompanyFacts => {
            let retrieved = RetrievedCompanyFacts::restored(
                bytes,
                request.object().evidence().content_digest(),
                received_at,
                availability,
                parser_limits,
                cancellation,
            )?;
            if retrieved.document().cik().as_str() != dataset.cik() {
                return Err(SecClientError::ResponseCikMismatch);
            }
            let observations = normalize_company_facts_with_cancellation(
                &source_id,
                &retrieved,
                ingested_at,
                cancellation,
            )?;
            let company_identity = company_identity_from_company_facts(
                &request,
                &source_id,
                &retrieved,
                ingested_at,
                cancellation,
            )?;
            let batch = canonical_batch(&request, observations, &authority, cancellation)?;
            let native_lineage = company_facts_native_lineage(&request, &retrieved, &batch)?;
            let row_capture_page_ordinals =
                company_facts_row_capture_page_ordinals(&request, &batch)?;
            (
                batch,
                Some(company_identity),
                native_lineage,
                row_capture_page_ordinals,
            )
        }
        SecResearchDatasetKind::FilingXbrl => {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
    };
    authority.validate_current()?;
    Ok(SecExtractionResult {
        batch,
        company_identity,
        native_lineage,
        row_capture_page_ordinals,
    })
}

fn submissions_row_capture_page_ordinals(
    request: &ExtractionRequest,
    retrieved: &RetrievedSubmissions,
    batch: &ExtractionBatch,
    parser_limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<Vec<u16>, SecClientError> {
    let components = retrieved.components();
    let expected_pages = retrieved
        .document()
        .companions()
        .len()
        .checked_add(1)
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    let SourceObjectCaptureIdentity::Paged {
        page_count,
        terminal: ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        ..
    } = request.object().capture_identity()
    else {
        return Err(SecClientError::InvalidCompositeRepresentation);
    };
    if components.len() != expected_pages || usize::from(page_count.get()) != expected_pages {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }

    let current = components
        .first()
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    let current_document = crate::SubmissionsDocument::parse_with_cancellation(
        current.bytes(),
        parser_limits,
        cancellation,
    )?;
    if current_document.cik() != retrieved.document().cik()
        || current_document.companions() != retrieved.document().companions()
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }

    let mut origins = BTreeMap::new();
    for filing in current_document.filings() {
        if origins
            .insert(
                filing.accession().as_str().to_owned(),
                (filing.clone(), 0_u16),
            )
            .is_some()
        {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
    }
    for ((component_ordinal, component), declaration) in components
        .iter()
        .enumerate()
        .skip(1)
        .zip(current_document.companions())
    {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let component_ordinal = u16::try_from(component_ordinal)
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
        let archive = crate::SubmissionsDocument::parse_archive_with_cancellation(
            component.bytes(),
            parser_limits,
            cancellation,
        )?;
        crate::json::validate_companion_coverage(declaration, &archive, current_document.cik())?;
        for filing in archive.filings() {
            match origins.get(filing.accession().as_str()) {
                Some((existing, _)) if existing == filing => {}
                Some(_) => return Err(SecClientError::InvalidCompositeRepresentation),
                None => {
                    origins.insert(
                        filing.accession().as_str().to_owned(),
                        (filing.clone(), component_ordinal),
                    );
                }
            }
        }
    }

    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(batch.records().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    for record in batch.records() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let canonical = retrieved
            .document()
            .filing(record.revision().as_str())
            .ok_or(SecClientError::InvalidCompositeRepresentation)?;
        let (captured, page_ordinal) = origins
            .get(record.revision().as_str())
            .ok_or(SecClientError::InvalidCompositeRepresentation)?;
        if captured != canonical {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        ordinals.push(*page_ordinal);
    }
    if ordinals.len() != retrieved.document().filings().len()
        || origins.len() != retrieved.document().filings().len()
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    Ok(ordinals)
}

fn company_facts_row_capture_page_ordinals(
    request: &ExtractionRequest,
    batch: &ExtractionBatch,
) -> Result<Vec<u16>, SecClientError> {
    let SourceObjectCaptureIdentity::Paged {
        page_count,
        terminal: ProviderCaptureTerminalDisposition::StandaloneResponse,
        ..
    } = request.object().capture_identity()
    else {
        return Err(SecClientError::InvalidCompositeRepresentation);
    };
    if page_count.get() != 1 || batch.records().is_empty() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let mut ordinals = Vec::new();
    ordinals
        .try_reserve_exact(batch.records().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    ordinals.resize(batch.records().len(), 0);
    Ok(ordinals)
}

fn canonical_batch(
    request: &ExtractionRequest,
    observations: Vec<ResearchObservation>,
    authority: &ExtractionAuthority,
    cancellation: &CancellationToken,
) -> Result<ExtractionBatch, SecClientError> {
    authority.validate_current()?;
    let mut records =
        ExtractionBatchAccumulator::try_new(request).map_err(map_extraction_contract_error)?;
    for observation in observations {
        authority.validate_current()?;
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        records
            .push(canonical_record(
                request,
                observation,
                authority,
                cancellation,
            )?)
            .map_err(map_extraction_contract_error)?;
    }
    authority.validate_current()?;
    let batch = records.finish().map_err(map_extraction_contract_error)?;
    authority.validate_current()?;
    Ok(batch)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecSubmissionsNativeBatchV1<'a> {
    version: u16,
    family: &'static str,
    dataset: &'a SourceIdentifier,
    cik: &'a SourceIdentifier,
    company_metadata: &'a crate::SecSubmissionCompanyMetadata,
    companions: &'a [crate::SecSubmissionsCompanion],
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecSubmissionsNativeRowV1<'a> {
    family: &'static str,
    filing: &'a SecFiling,
}

fn submissions_native_lineage(
    request: &ExtractionRequest,
    retrieved: &RetrievedSubmissions,
    batch: &ExtractionBatch,
) -> Result<ProviderNativeLineageBatch, SecClientError> {
    let document = retrieved.document();
    if document.filings().len() != batch.records().len() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(document.filings().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    ordered.extend(document.filings());
    ordered.sort_by(|left, right| compare_filings(left, right));
    if ordered
        .iter()
        .zip(batch.records())
        .any(|(filing, record)| record.revision() != filing.accession())
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::SecEdgarV1,
        batch,
    )
    .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    native_lineage
        .try_set_batch_sidecar(&SecSubmissionsNativeBatchV1 {
            version: 1,
            family: "submissions",
            dataset: request.object().dataset(),
            cik: document.cik(),
            company_metadata: document.company_metadata(),
            companions: document.companions(),
        })
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    for filing in ordered {
        native_lineage
            .try_push(&SecSubmissionsNativeRowV1 {
                family: "filing",
                filing,
            })
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    }
    native_lineage
        .finish()
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecCompanyFactsNativeBatchV1<'a> {
    version: u16,
    family: &'static str,
    dataset: &'a SourceIdentifier,
    cik: &'a SourceIdentifier,
    entity_name: &'a str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SecCompanyFactsNativeRowV1<'a> {
    family: &'static str,
    occurrence: &'a CompanyFactOccurrence,
}

fn company_facts_native_lineage(
    request: &ExtractionRequest,
    retrieved: &RetrievedCompanyFacts,
    batch: &ExtractionBatch,
) -> Result<ProviderNativeLineageBatch, SecClientError> {
    let document = retrieved.document();
    if document.occurrences().len() != batch.records().len() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let mut ordered = Vec::new();
    ordered
        .try_reserve_exact(document.occurrences().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    ordered.extend(document.occurrences());
    ordered.sort_unstable_by(|left, right| compare_company_facts(left, right));
    let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::SecEdgarV1,
        batch,
    )
    .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    native_lineage
        .try_set_batch_sidecar(&SecCompanyFactsNativeBatchV1 {
            version: 1,
            family: "company_facts",
            dataset: request.object().dataset(),
            cik: document.cik(),
            entity_name: document.entity_name(),
        })
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    for occurrence in ordered {
        native_lineage
            .try_push(&SecCompanyFactsNativeRowV1 {
                family: "company_fact",
                occurrence,
            })
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    }
    native_lineage
        .finish()
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)
}

fn company_identity_from_submissions(
    request: &ExtractionRequest,
    source_id: &SourceId,
    retrieved: &RetrievedSubmissions,
    surface: CompanyIdentitySurface,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<CompanyIdentityObservation, SecClientError> {
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let metadata = retrieved.document().company_metadata();
    let mut former_names = Vec::new();
    former_names
        .try_reserve_exact(metadata.former_names().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    if former_names.capacity() > metadata.former_names().len().saturating_mul(2) {
        return Err(SecClientError::AllocationFailed);
    }
    for former_name in metadata.former_names() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        former_names.push(FormerCompanyName::try_new(
            former_name.name(),
            former_name.effective_from(),
            former_name.effective_to(),
        )?);
    }
    let mut associations = Vec::new();
    associations
        .try_reserve_exact(metadata.ticker_exchange_pairs().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    if associations.capacity() > metadata.ticker_exchange_pairs().len().saturating_mul(2) {
        return Err(SecClientError::AllocationFailed);
    }
    for association in metadata.ticker_exchange_pairs() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        associations.push(ProviderReportedSecurityAssociation::try_new(
            association.ticker(),
            association.exchange(),
        )?);
    }
    let identity_raw = retrieved.current_component();
    CompanyIdentityObservation::try_new(CompanyIdentityObservationInput {
        schema_version: SchemaVersion::CURRENT,
        source_id: source_id.clone(),
        provider_company_id: retrieved.document().cik().clone(),
        surface,
        conformed_name: metadata.conformed_name().to_owned(),
        former_names,
        entity_type: metadata.entity_type().map(str::to_owned),
        sic: metadata.sic().map(str::to_owned),
        sic_description: metadata.sic_description().map(str::to_owned),
        associations,
        parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
            request.object().evidence().content_digest(),
        ),
        identity_payload_evidence: retrieved_payload_evidence(identity_raw)?,
        received_at: identity_raw.received_at(),
        availability: identity_raw.availability().clone(),
        ingested_at,
        quality: DataQuality::OfficialDelayed,
    })
    .map_err(Into::into)
}

fn company_identity_from_company_facts(
    request: &ExtractionRequest,
    source_id: &SourceId,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<CompanyIdentityObservation, SecClientError> {
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let identity_raw = retrieved.raw();
    CompanyIdentityObservation::try_new(CompanyIdentityObservationInput {
        schema_version: SchemaVersion::CURRENT,
        source_id: source_id.clone(),
        provider_company_id: retrieved.document().cik().clone(),
        surface: CompanyIdentitySurface::SecCompanyFacts,
        conformed_name: retrieved.document().entity_name().to_owned(),
        former_names: Vec::new(),
        entity_type: None,
        sic: None,
        sic_description: None,
        associations: Vec::new(),
        parent_ingest_payload_evidence: request.object().evidence().clone(),
        identity_payload_evidence: retrieved_payload_evidence(identity_raw)?,
        received_at: identity_raw.received_at(),
        availability: identity_raw.availability().clone(),
        ingested_at,
        quality: DataQuality::OfficialDelayed,
    })
    .map_err(Into::into)
}

fn retrieved_payload_evidence(
    retrieved: &RetrievedSecBytes,
) -> Result<ExactPayloadEvidence, SecClientError> {
    match (retrieved.locator(), retrieved.retrieval_revision()) {
        (Some(locator), Some(revision)) => Ok(ExactPayloadEvidence::with_version_pinned_locator(
            retrieved.evidence(),
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(locator)?,
                SourceIdentifier::try_from(revision.to_string())?,
            ),
        )),
        (None, None) => Ok(ExactPayloadEvidence::from_content_digest(
            retrieved.evidence(),
        )),
        _ => Err(SecClientError::InvalidCompositeRepresentation),
    }
}

fn canonical_record(
    request: &ExtractionRequest,
    observation: ResearchObservation,
    authority: &ExtractionAuthority,
    cancellation: &CancellationToken,
) -> Result<ExtractionRecord, SecClientError> {
    authority.validate_current()?;
    let context = observation_context(&observation)?;
    let time = context.time();
    let availability = extraction_availability(context.provenance().availability());
    let revision = context.provenance().source_identifier().clone();
    let mut writer = CanonicalRecordWriter::new(cancellation);
    if serde_json::to_writer(&mut writer, &observation).is_err() {
        return if cancellation.is_cancelled() {
            Err(SecClientError::Cancelled)
        } else {
            Err(SecClientError::CompositeSerialization)
        };
    }
    let payload = writer.into_inner();
    authority.validate_current()?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    ExtractionRecord::try_new_with_time(
        request,
        SourceIdentifier::try_from(RESEARCH_RECORD_SCHEMA)?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest,
        )),
        time.effective().clone(),
        time.published().cloned(),
        availability,
        revision,
        time.superseded().cloned(),
        Bytes::from(payload),
    )
    .map_err(|_| SecClientError::InvalidCompositeRepresentation)
}

fn filing_discovery_availability(
    filing: &SecFilingXbrlCoordinates,
    received_at: Timestamp,
) -> Result<(Option<Timestamp>, ExtractionAvailabilityEvidence), SecClientError> {
    match filing.acceptance() {
        Some(acceptance) => {
            if acceptance.accepted_at() > received_at {
                return Err(SecClientError::InvalidCompositeRepresentation);
            }
            Ok((
                Some(acceptance.accepted_at()),
                ExtractionAvailabilityEvidence::Observed {
                    available_at: acceptance.accepted_at(),
                    evidence: acceptance.evidence().clone(),
                },
            ))
        }
        None => Ok((
            None,
            ExtractionAvailabilityEvidence::LocalFirstObserved {
                observed_at: received_at,
            },
        )),
    }
}

fn validate_captured_filing_document(
    filing: &SecFilingXbrlCoordinates,
    retrieved: &RetrievedSecBytes,
    representation: &SecRepresentation,
    raw_store: &RawEvidenceStore,
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    cancellation: &CancellationToken,
) -> Result<(), SecClientError> {
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let locator = SecObjectLocator::filing_document(
        filing.cik(),
        filing.accession().as_str(),
        filing.document().as_str(),
    )?;
    let receipt = retrieved
        .capture_receipt()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    if receipt.source_id() != source_id
        || receipt.metadata_revision() != metadata_revision
        || receipt.dataset().as_str() != locator.url()
        || retrieved.locator() != Some(locator.url())
    {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    retrieved
        .capture_material()?
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let size_bytes =
        u64::try_from(retrieved.bytes().len()).map_err(|_| SecClientError::ResponseTooLarge)?;
    if representation.locator() != locator.url()
        || representation.evidence() != retrieved.evidence()
        || representation.size_bytes() != size_bytes
        || representation.first_observed_at() != retrieved.received_at()
        || Some(representation.retrieval_revision()) != retrieved.retrieval_revision()
    {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let reopened = raw_store.read_verified_bounded_cancellable(
        &retrieved.evidence(),
        size_bytes,
        cancellation,
    )?;
    if reopened.len() != retrieved.bytes().len()
        || reopened.as_slice() != retrieved.bytes().as_ref()
    {
        return Err(SecClientError::RawEvidenceMismatch);
    }
    Ok(())
}

fn filing_xbrl_request_graph_identity(
    dataset: &SecResearchDataset,
    components: &[ProviderCaptureMaterial],
) -> Result<EvidenceDigest, SecClientError> {
    let first = components
        .first()
        .ok_or(SecClientError::InvalidCaptureMaterial)?;
    let mut digest = Sha256::new();
    hash_request_graph_field(
        &mut digest,
        b"market-squawk/sec-filing-xbrl-request-graph/v1",
    )?;
    hash_request_graph_field(&mut digest, first.receipt().source_id().as_str().as_bytes())?;
    hash_request_graph_field(
        &mut digest,
        first
            .receipt()
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_request_graph_field(&mut digest, dataset.dataset().as_str().as_bytes())?;
    digest.update(
        u64::try_from(components.len())
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?
            .to_be_bytes(),
    );
    for component in components {
        let receipt = component.receipt();
        hash_request_graph_field(&mut digest, receipt.source_id().as_str().as_bytes())?;
        hash_request_graph_field(
            &mut digest,
            receipt
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_request_graph_field(&mut digest, receipt.dataset().as_str().as_bytes())?;
        digest.update(receipt.request_set_identity().bytes());
        digest.update(receipt.content_digest().bytes());
        digest.update(receipt.observation_digest().bytes());
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_request_graph_field(digest: &mut Sha256, value: &[u8]) -> Result<(), SecClientError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}

fn filing_media_type(document: &SourceIdentifier) -> Result<SourceIdentifier, SecClientError> {
    let media_type = if document.as_str().ends_with(".htm") || document.as_str().ends_with(".html")
    {
        "text/html"
    } else if document.as_str().ends_with(".xml") {
        "application/xml"
    } else {
        return Err(SecClientError::InvalidLocator);
    };
    SourceIdentifier::try_from(media_type).map_err(Into::into)
}

fn extraction_availability(availability: &AvailabilityEvidence) -> ExtractionAvailabilityEvidence {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => ExtractionAvailabilityEvidence::Observed {
            available_at: *available_at,
            evidence: evidence.clone(),
        },
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            ExtractionAvailabilityEvidence::LocalFirstObserved {
                observed_at: *observed_at,
            }
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => ExtractionAvailabilityEvidence::Inferred {
            inferred_at: *inferred_at,
            method: method.clone(),
        },
        AvailabilityEvidence::Unknown => ExtractionAvailabilityEvidence::Unknown,
    }
}

struct CanonicalRecordWriter<'a> {
    payload: Vec<u8>,
    cancellation: &'a CancellationToken,
}

impl<'a> CanonicalRecordWriter<'a> {
    const fn new(cancellation: &'a CancellationToken) -> Self {
        Self {
            payload: Vec::new(),
            cancellation,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.payload
    }
}

impl Write for CanonicalRecordWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "SEC canonical record serialization cancelled",
            ));
        }
        let new_len = self
            .payload
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("SEC canonical record is too large"))?;
        if new_len > MAX_EXTRACTION_RECORD_BYTES {
            return Err(std::io::Error::other("SEC canonical record is too large"));
        }
        if new_len > self.payload.capacity() {
            let capacity = new_len
                .max(self.payload.capacity().saturating_mul(2).max(8))
                .min(MAX_EXTRACTION_RECORD_BYTES);
            self.payload
                .try_reserve_exact(capacity - self.payload.len())
                .map_err(|_| std::io::Error::other("SEC canonical record allocation failed"))?;
        }
        if self.payload.capacity() > MAX_EXTRACTION_RECORD_BYTES {
            return Err(std::io::Error::other(
                "SEC canonical record allocation exceeds admission",
            ));
        }
        self.payload.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn observation_context(
    observation: &ResearchObservation,
) -> Result<&ResearchContext, SecClientError> {
    match observation {
        ResearchObservation::Filing(observation) => Ok(observation.context()),
        ResearchObservation::Fundamental(observation) => Ok(observation.context()),
        _ => Err(SecClientError::InvalidCompositeRepresentation),
    }
}

pub(crate) fn request_parser_limits(
    request: &ExtractionRequest,
    decoded_bytes: usize,
    decoded_capacity: usize,
) -> Result<SecParserLimits, SecClientError> {
    let dataset = SecResearchDataset::try_from_identifier(request.object().dataset())
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    // A submissions manifest indexes separately verified current/archive bodies. Admit those
    // bodies against the remaining request budget, not the small manifest's encoded length.
    // Filing XBRL calls the family-agnostic helper directly with its opaque admitted dataset.
    let decoded_limit = if dataset.kind() == SecResearchDatasetKind::Submissions {
        usize::try_from(
            request
                .max_bytes()
                .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES),
        )
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?
        .checked_sub(decoded_capacity)
        .filter(|remaining| *remaining > 0)
        .ok_or(SecClientError::ResponseTooLarge)?
    } else {
        decoded_bytes
    };
    request_parser_limits_with_record_limit(
        request,
        decoded_limit,
        decoded_capacity,
        usize::try_from(request.max_records())
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?,
    )
}

fn request_parser_limits_with_record_limit(
    request: &ExtractionRequest,
    decoded_bytes: usize,
    decoded_capacity: usize,
    parser_record_limit: usize,
) -> Result<SecParserLimits, SecClientError> {
    let working_set_limit = usize::try_from(
        request
            .max_bytes()
            .min(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES),
    )
    .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    let retained_output_limit = working_set_limit
        .checked_sub(decoded_capacity)
        .filter(|remaining| *remaining > 0)
        .ok_or(SecClientError::ResponseTooLarge)?;
    let decoded_limit = decoded_bytes.max(1);
    let total_string_limit = decoded_limit
        .min(24 * 1024 * 1024)
        .min(retained_output_limit)
        .max(1);
    let string_limit = decoded_limit.min(256 * 1024).min(total_string_limit).max(1);
    SecParserLimits::try_new(
        decoded_limit,
        parser_record_limit,
        128,
        string_limit,
        total_string_limit,
        retained_output_limit,
    )
    .map_err(Into::into)
}

fn deadline_remaining(
    deadline: market_squawk_domain::Timestamp,
) -> Result<Duration, ExtractionSourceError> {
    let now = crate::client::system_timestamp().map_err(map_client_error)?;
    let remaining = deadline.unix_nanos().saturating_sub(now.unix_nanos());
    if remaining <= 0 {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        u64::try_from(remaining)
            .map(Duration::from_nanos)
            .map_err(|_| ExtractionSourceError::DeadlineExceeded)
    }
}

// Static categories retain the concrete rejection while excluding JSON/XML text, URLs,
// contact declarations and transport errors that can contain request data.
fn trace_protocol_failure(
    error: &SecClientError,
    stage: &'static str,
    family: SecResearchDatasetKind,
) {
    let category = match error {
        SecClientError::Parser(error) => match error {
            SecParserError::Cancelled => return,
            SecParserError::InvalidLimits => "parser_invalid_limits",
            SecParserError::ByteLimitExceeded => "parser_byte_limit_exceeded",
            SecParserError::DepthLimitExceeded => "parser_depth_limit_exceeded",
            SecParserError::StringLimitExceeded => "parser_string_limit_exceeded",
            SecParserError::RecordLimitExceeded => "parser_record_limit_exceeded",
            SecParserError::NodeLimitExceeded => "parser_node_limit_exceeded",
            SecParserError::ByteCountOverflow => "parser_byte_count_overflow",
            SecParserError::AllocationFailed => "parser_allocation_failed",
            SecParserError::AllocationAuthorityPoisoned => "parser_allocation_authority_poisoned",
            SecParserError::RetainedOutputLimitExceeded => "parser_retained_output_limit_exceeded",
            SecParserError::DuplicateKey => "parser_duplicate_key",
            SecParserError::InvalidNumber => "parser_invalid_number",
            SecParserError::MissingField => "parser_missing_field",
            SecParserError::WrongType => "parser_wrong_type",
            SecParserError::ColumnLengthMismatch => "parser_column_length_mismatch",
            SecParserError::InvalidCik => "parser_invalid_cik",
            SecParserError::InvalidAccession => "parser_invalid_accession",
            SecParserError::InvalidCompanionName => "parser_invalid_companion_name",
            SecParserError::InvalidCompanionCoverage => "parser_invalid_companion_coverage",
            SecParserError::InvalidConcept => "parser_invalid_concept",
            SecParserError::InvalidDate => "parser_invalid_date",
            SecParserError::InvalidTimestamp => "parser_invalid_timestamp",
            SecParserError::InvalidPeriod => "parser_invalid_period",
            SecParserError::InvalidFiscalContext => "parser_invalid_fiscal_context",
            SecParserError::InvalidDecimal => "parser_invalid_decimal",
            SecParserError::NonNumericCompanyFact => "parser_non_numeric_company_fact",
            SecParserError::ConflictingAccession => "parser_conflicting_accession",
            SecParserError::InvalidCompanyMetadata => "parser_invalid_company_metadata",
            SecParserError::MetadataAssociationLengthMismatch => {
                "parser_metadata_association_length_mismatch"
            }
            SecParserError::DuplicateMetadataAssociation => "parser_duplicate_metadata_association",
            SecParserError::ConflictingMetadataAssociation => {
                "parser_conflicting_metadata_association"
            }
            SecParserError::Json(_) => "parser_json",
            SecParserError::Identity(_) => "parser_identity",
            SecParserError::FilingForm(_) => "parser_filing_form",
            SecParserError::Time(_) => "parser_time",
        },
        SecClientError::Normalization(error) => match error {
            SecNormalizationError::Xbrl(_) => "normalization_xbrl",
            SecNormalizationError::Cancelled => return,
            SecNormalizationError::IngestedBeforeReceived => {
                "normalization_ingested_before_received"
            }
            SecNormalizationError::InvalidXbrlDataset => "normalization_invalid_xbrl_dataset",
            SecNormalizationError::XbrlDocumentBindingMismatch => {
                "normalization_xbrl_document_binding_mismatch"
            }
            SecNormalizationError::PublicationAfterReceipt => {
                "normalization_publication_after_receipt"
            }
            SecNormalizationError::RevisionOverflow => "normalization_revision_overflow",
            SecNormalizationError::AllocationFailed => "normalization_allocation_failed",
            SecNormalizationError::FundamentalContext(_) => "normalization_fundamental_context",
            SecNormalizationError::PublicationAfterIngestion => {
                "normalization_publication_after_ingestion"
            }
            SecNormalizationError::Identity(_) => "normalization_identity",
            SecNormalizationError::Provenance(_) => "normalization_provenance",
            SecNormalizationError::Research(_) => "normalization_research",
        },
        SecClientError::CompanyIdentity(_) => "company_identity",
        SecClientError::Xbrl(_) => "xbrl",
        SecClientError::RevisionAuthority(_) => "revision_authority",
        SecClientError::ProviderCapture(_) => "provider_capture",
        SecClientError::RawCapture(_) => "raw_capture",
        SecClientError::RegistrationMismatch => "registration_mismatch",
        SecClientError::ResponseCikMismatch => "response_cik_mismatch",
        SecClientError::InvalidCaptureMaterial => "invalid_capture_material",
        SecClientError::InvalidCompositeRepresentation => "invalid_composite_representation",
        SecClientError::InvalidCompanionSet => "invalid_companion_set",
        _ => return,
    };
    let family = match family {
        SecResearchDatasetKind::Submissions => "submissions",
        SecResearchDatasetKind::CompanyFacts => "company_facts",
        SecResearchDatasetKind::FilingXbrl => "filing_xbrl",
    };
    if let SecClientError::Parser(SecParserError::Json(error)) = error {
        tracing::warn!(
            stage,
            family,
            category,
            line = error.line(),
            column = error.column(),
            "SEC extraction rejected retained input"
        );
    } else {
        tracing::warn!(
            stage,
            family,
            category,
            "SEC extraction rejected retained input"
        );
    }
}

fn map_client_error(error: SecClientError) -> ExtractionSourceError {
    let source = match error {
        SecClientError::Cancelled => return ExtractionSourceError::Cancelled,
        SecClientError::DeadlineExceeded => return ExtractionSourceError::DeadlineExceeded,
        SecClientError::Authority(error) => return ExtractionSourceError::Authority(error),
        SecClientError::HttpStatus(401 | 403) => SourceError::Unauthorized,
        SecClientError::HttpStatus(429 | 503) => SourceError::ProviderUnavailable,
        SecClientError::ClockOutOfRange => SourceError::TrustedTimeUnavailable,
        SecClientError::Parser(SecParserError::Cancelled)
        | SecClientError::Normalization(SecNormalizationError::Cancelled) => {
            return ExtractionSourceError::Cancelled;
        }
        SecClientError::Parser(_)
        | SecClientError::CompanyIdentity(_)
        | SecClientError::Normalization(_)
        | SecClientError::Xbrl(_)
        | SecClientError::RevisionAuthority(_)
        | SecClientError::ProviderCapture(_)
        | SecClientError::RawCapture(_)
        | SecClientError::RegistrationMismatch
        | SecClientError::ResponseCikMismatch
        | SecClientError::InvalidCaptureMaterial
        | SecClientError::InvalidCompositeRepresentation
        | SecClientError::InvalidCompanionSet => SourceError::InvalidProtocolState,
        _ => SourceError::Network,
    };
    ExtractionSourceError::Source(source)
}

fn map_extraction_contract_error(error: ExtractionError) -> SecClientError {
    match error {
        ExtractionError::AllocationFailed => SecClientError::AllocationFailed,
        _ => SecClientError::InvalidCompositeRepresentation,
    }
}

fn invalid_protocol() -> ExtractionSourceError {
    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
}
