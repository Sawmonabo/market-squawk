//! Canonical `ExtractionSource` composition for SEC filings and facts.

use std::collections::BTreeMap;
use std::io::Write;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence, CompanyIdentityObservation, CompanyIdentityObservationInput,
    CompanyIdentitySurface, DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, FormerCompanyName, MetadataRevision, ProviderIdentityRegistry,
    ProviderReportedSecurityAssociation, ResearchContext, ResearchObservation, SchemaVersion,
    SourceId, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionBatchAccumulator, ExtractionError,
    ExtractionRecord, ExtractionRequest, ExtractionRevisionEvidence, ExtractionRevisionPlan,
    ExtractionSource, ExtractionSourceError, MAX_EXTRACTION_RECORD_BYTES,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ObservedProviderOrder, ProviderCaptureMaterial,
    ProviderCaptureScope, ProviderCaptureTerminalDisposition, ProviderNativeLineageBatch,
    ProviderNativeLineageBatchBuilder, ProviderNativeLineageImplementation,
    SealedProviderCaptureBinding, SourceError, SourceMetadataProvider, SourceObject,
    SourceObjectCaptureIdentity,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::normalize::{
    compare_company_facts, compare_filings, normalize_filing_xbrl_with_cancellation,
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
    reconcile_submissions_with_cancellation,
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
    sealed_root: SealedProviderCaptureBinding,
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
    identities: Arc<ProviderIdentityRegistry>,
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
            identities,
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
                identities,
                source_id,
                metadata_revision,
                parser_limits,
            },
            material,
        ))
    }
}

/// Closed filing-XBRL capture graph ready for bounded canonical extraction.
///
/// Construction proves the accession belongs to the exact captured current submissions object,
/// the primary filing document is the retained object named by that filing, and every admitted
/// taxonomy artifact is an exact captured official artifact. Consumption produces the canonical
/// batch and the same one-use raw graph material; callers cannot replace either half.
pub struct SecFilingXbrlCaptureHandoff {
    pending: SecPendingFilingXbrlAdmission,
    capture_material: ProviderCaptureMaterial,
}

impl std::fmt::Debug for SecFilingXbrlCaptureHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecFilingXbrlCaptureHandoff")
            .field("dataset", self.pending.dataset.dataset())
            .field("accession", self.pending.filing.accession())
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
    ) -> Result<(SecExtractionResult, ProviderCaptureMaterial), SecClientError> {
        extract_filing_xbrl_handoff(
            self.pending,
            self.capture_material,
            authority,
            max_records,
            max_bytes,
            deadline,
            &cancellation,
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

    /// Builds provider-owned revision evidence aligned to one extracted SEC batch.
    ///
    /// Exact canonical source-record identity is the version token. Conservative availability is
    /// the ordering coordinate; filing civil dates never become knowledge time. Final immutable
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
        if batch
            .request()
            .object()
            .dataset()
            .as_str()
            .starts_with(SEC_FILING_XBRL_DATASET_PREFIX)
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

    /// Extracts SEC analytical records with company identity from the same exact source bytes.
    ///
    /// The ordinary [`ExtractionSource`] implementation delegates here and discards only the
    /// adapter-specific sidecar. Callers that own company-identity publication use this method so
    /// no second raw-store read or parser pass is required.
    pub fn extract_with_company_identity(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<SecExtractionResult, ExtractionSourceError>> {
        let raw_store = self.raw_store();
        let identities = self.identity_registry();
        let source_id = self.metadata().source_id().clone();
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let remaining = deadline_remaining(request.deadline())?;
            let worker_cancellation = cancellation.child_token();
            let worker_authority = authority.clone();
            let worker = self.run_validation_blocking(&worker_cancellation, move |worker_token| {
                extract_blocking(
                    request,
                    raw_store,
                    identities,
                    source_id,
                    worker_authority,
                    worker_token,
                )
            });
            tokio::pin!(worker);
            tokio::select! {
                result = &mut worker => {
                    let extracted = result.map_err(map_client_error)?;
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
pub(crate) fn admit_filing_xbrl_root_from_sealed_binding(
    sealed_root: SealedProviderCaptureBinding,
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
    sealed_root
        .validate()
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
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
    let batch = sealed_root.batch();
    let object = batch.request().object();
    let record = batch
        .records()
        .first()
        .filter(|_| batch.records().len() == 1)
        .ok_or(SecClientError::InvalidCompositeRepresentation)?;
    if sealed_root.scope() != ProviderCaptureScope::Whole
        || sealed_root.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::SecEdgarV1
        || sealed_root.capture_evidence() != receipt
        || object.source_id() != &source_id
        || object.metadata_revision() != &metadata_revision
        || object.dataset().as_str() != locator.url()
        || object.object_id().as_str() != locator.url()
        || object.evidence().content_digest() != filing_document.evidence()
        || record.payload().as_ref() != filing_document.bytes().as_ref()
        || record.evidence().content_digest() != filing_document.evidence()
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
    identities: Arc<ProviderIdentityRegistry>,
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
    sealed_root
        .validate()
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
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
        identities,
        source_id,
        metadata_revision,
        parser_limits,
    };
    let (pending, capture_material) = pending.into_sealing_parts(cancellation)?;
    Ok(SecFilingXbrlCaptureHandoff {
        pending,
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
    pending: SecPendingFilingXbrlAdmission,
    capture_material: ProviderCaptureMaterial,
    authority: ExtractionAuthority,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(SecExtractionResult, ProviderCaptureMaterial), SecClientError> {
    authority.validate_current()?;
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    pending.revalidate(cancellation)?;
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
    let (published_at, availability) = filing_discovery_availability(&pending.filing, received_at)?;
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
        filing_media_type(pending.filing.document())?,
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

    let filing_payload = retrieved_payload_evidence(&pending.filing_document)?;
    let document_context = XbrlDocumentContext::from_validated_taxonomy(
        pending.filing.accession().clone(),
        SourceIdentifier::try_from(pending.filing.cik())?,
        &pending.taxonomy,
        filing_payload,
        pending.filing_document.received_at(),
        cancellation,
    )?;
    let document = XbrlDocumentParser::parse_with_cancellation(
        pending.filing_document.bytes(),
        pending.parser_limits,
        document_context,
        cancellation,
    )?;
    let ingested_at = crate::client::system_timestamp()?;
    let company_identity = company_identity_from_submissions(
        &request,
        &pending.source_id,
        &pending.submissions,
        ingested_at,
        cancellation,
    )?;
    let mut normalized = normalize_filing_xbrl_with_cancellation(
        &pending.source_id,
        &pending.identities,
        pending.dataset,
        document,
        pending.filing_document.evidence(),
        pending.filing_document.received_at(),
        ingested_at,
        cancellation,
    )?;
    let mut records =
        ExtractionBatchAccumulator::try_new(&request).map_err(map_extraction_contract_error)?;
    while let Some(observation) = normalized.try_next_observation(cancellation)? {
        authority.validate_current()?;
        records
            .push(canonical_record(
                &request,
                observation,
                &authority,
                cancellation,
            )?)
            .map_err(map_extraction_contract_error)?;
    }
    let batch = records.finish().map_err(map_extraction_contract_error)?;
    if batch.records().is_empty() {
        return Err(SecClientError::InvalidCompositeRepresentation);
    }
    let native_lineage = normalized
        .into_native_lineage()?
        .try_into_provider_native_lineage(&batch)
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    let mut row_capture_page_ordinals = Vec::new();
    row_capture_page_ordinals
        .try_reserve_exact(batch.records().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    row_capture_page_ordinals.resize(batch.records().len(), 1);
    authority.validate_current()?;
    Ok((
        SecExtractionResult {
            batch,
            company_identity: Some(company_identity),
            native_lineage,
            row_capture_page_ordinals,
        },
        capture_material,
    ))
}

fn extract_blocking(
    request: ExtractionRequest,
    raw_store: Arc<RawEvidenceStore>,
    identities: Arc<ProviderIdentityRegistry>,
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
                &identities,
                &retrieved,
                ingested_at,
                cancellation,
            )?;
            let company_identity = company_identity_from_submissions(
                &request,
                &source_id,
                &retrieved,
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
                &identities,
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
        surface: CompanyIdentitySurface::SecSubmissions,
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
        self.payload
            .try_reserve(buffer.len())
            .map_err(|_| std::io::Error::other("SEC canonical record allocation failed"))?;
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

fn request_parser_limits(
    request: &ExtractionRequest,
    decoded_bytes: usize,
    decoded_capacity: usize,
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
        usize::try_from(request.max_records())
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?,
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
