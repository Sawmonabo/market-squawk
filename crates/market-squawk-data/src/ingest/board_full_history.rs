//! Source-owned one-file H.15 publication through the existing atomic logical data transaction.

use super::*;
use crate::ProviderLogicalPublicationOrigin;
use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, BoardDatasetProfile, BoardFullHistoryCanonicalCursor,
    BoardPreparedFullHistory,
};
use market_squawk_platform::SealedResearchJournalStore;

/// Opaque source preparation bound to the actual canonical Macro schema and fixed dataset.
#[derive(Debug)]
pub struct BoardFullHistoryPublicationInput {
    dataset: DatasetId,
    metadata: SourceMetadata,
    cursor: BoardFullHistoryCanonicalCursor,
    binding: SealedProviderLogicalPublicationBinding,
    original_checkpoint: Box<[u8]>,
}

impl BoardFullHistoryPublicationInput {
    /// Only an actual completed adapter preparation can mint this publication input.
    pub fn try_from_source(prepared: BoardPreparedFullHistory) -> Result<Self, IngestError> {
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let metadata = prepared.metadata().clone();
        let (cursor, binding, original_checkpoint) = prepared.into_parts();
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(ArrowConversionError::from)?;
        if metadata.source_id().as_str() != BOARD_DDP_SOURCE_ID
            || binding.terminal().source_id() != metadata.source_id()
            || binding.terminal().total_canonical_rows() == 0
            || binding.canonical_partitions().is_empty()
            || binding.canonical_partitions().len() > 1_024
            || binding
                .canonical_partitions()
                .iter()
                .any(|partition| partition.schema_identity().bytes() != schema.fingerprint())
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        Ok(Self {
            dataset,
            metadata,
            cursor,
            binding,
            original_checkpoint,
        })
    }
    /// Exact acquired source metadata for the existing registration and persistence authority.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
    /// Fixed complete-file analytical identity, independent of outcome values.
    pub const fn dataset(&self) -> &DatasetId {
        &self.dataset
    }
    /// The actual complete logical binding is both rights and ingest reservation payload.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }
    /// Original file locator retained by the source-owned durable acquisition checkpoint.
    pub fn original_checkpoint(&self) -> &[u8] {
        &self.original_checkpoint
    }
}

/// Inert exact-generation replay locator. Serving source proof requires physical original reopen.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoardFullHistoryPublicationReference {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    original_digest: EvidenceDigest,
}
impl BoardFullHistoryPublicationReference {
    /// Exact creating canonical generation, never a later descendant.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Exact logical raw/native/row-map/terminal relation retained with that creating run.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    /// Exact source transport/native full-file identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
}

/// Linear complete-file publication, holding only existing operation and object-publication leases.
/// Synchronous methods run on the application's existing retained research I/O owner.
pub struct BoardFullHistoryPublication {
    data: Arc<AnalyticalDataService>,
    _operation: crate::analytical_backup::AnalyticalOperationLease,
    publication: crate::publication_coordinator::PublicationLease,
    reservation: IngestReservation,
    input: BoardFullHistoryPublicationInput,
    schema: DatasetSchemaRef,
    published: Vec<PublishedObject>,
    objects: Vec<ManifestObject>,
    validated: bool,
    awaiting_partition: bool,
    complete: bool,
}
impl std::fmt::Debug for BoardFullHistoryPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoardFullHistoryPublication")
            .field("dataset", &self.input.dataset)
            .field("published_partitions", &self.published.len())
            .finish_non_exhaustive()
    }
}

/// One bounded original canonical/native partition before durable revision assignment.
#[derive(Debug)]
pub struct BoardFullHistoryNativePartition {
    binding_digest: EvidenceDigest,
    ordinal: u32,
    batch: ExtractionBatch,
    observed: market_squawk_sources::ObservedRevisionBatch,
}
/// Same original partition with actual revisions assigned by this existing data catalog.
#[derive(Debug)]
pub struct BoardFullHistoryAssignedPartition {
    catalog_id: uuid::Uuid,
    binding_digest: EvidenceDigest,
    ordinal: u32,
    batch: ExtractionBatch,
    assignments: market_squawk_sources::ObservedRevisionAssignments,
}
/// Bounded checked Arrow partition, prepared on the retained I/O owner before async staging.
#[derive(Debug)]
pub struct BoardFullHistoryArrowPartition {
    catalog_id: uuid::Uuid,
    binding_digest: EvidenceDigest,
    ordinal: u32,
    converted: ResearchArrowBatch,
    lineage: EvidenceDigest,
}
impl BoardFullHistoryAssignedPartition {
    /// Performs CPU normalization on the retained research I/O owner, not an async executor.
    pub fn convert(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryArrowPartition, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let converted = ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions(
            &self.batch,
            self.assignments.as_slice(),
        )?;
        let lineage = converted.lineage_digest()?;
        check_market_event_read(deadline, cancellation)?;
        Ok(BoardFullHistoryArrowPartition {
            catalog_id: self.catalog_id,
            binding_digest: self.binding_digest,
            ordinal: self.ordinal,
            converted,
            lineage,
        })
    }
}

/// Holds the existing analytical operation gate from original raw sealing until atomic commit.
/// Recovery uses this same gate, so completed unpublished source partitions cannot be quarantined.
pub struct BoardFullHistoryStagingLease {
    data: Arc<AnalyticalDataService>,
    operation: crate::analytical_backup::AnalyticalOperationLease,
}
impl std::fmt::Debug for BoardFullHistoryStagingLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoardFullHistoryStagingLease([EXISTING ANALYTICAL OPERATION])")
    }
}
/// Actual source reservation while the original single raw-publication lease remains held.
#[derive(Debug)]
pub struct BoardFullHistoryReservedPublication {
    staging: BoardFullHistoryStagingLease,
    reservation: IngestReservation,
    input: BoardFullHistoryPublicationInput,
}
impl AnalyticalDataService {
    /// Acquire after HTTP and before any raw seal; move this lease through the same retained I/O
    /// closures as the raw file so cancellation cannot release recovery exclusion before they join.
    pub async fn begin_board_full_history_staging(
        self: &Arc<Self>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryStagingLease, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let operation = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(IngestError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(IngestError::DeadlineExceeded),
            lease = self.operation_gate.acquire(cancellation) => lease.ok_or(IngestError::Cancelled)?,
        };
        check_market_event_read(deadline, cancellation)?;
        Ok(BoardFullHistoryStagingLease {
            data: Arc::clone(self),
            operation,
        })
    }
}
impl BoardFullHistoryStagingLease {
    /// Uses the existing source/rights/ingest authority under this already-held operation gate.
    /// A second reserve_source_ingest gate acquisition would deadlock this linear publication.
    /// Run this synchronous reservation on the existing research I/O owner.
    pub fn reserve_publication(
        self,
        input: BoardFullHistoryPublicationInput,
        rights: RightsDecisionInput,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryReservedPublication, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let payload = input.publication_digest();
        if rights.source_id != *input.metadata.source_id()
            || rights.payload_digest != payload
            || !rights
                .permitted_operations
                .contains(&SourceOperation::Persist)
        {
            return Err(IngestError::ReservationPayloadMismatch);
        }
        let id: String = payload
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let identity = IngestIdentity::try_new(
            input.metadata.source_id().clone(),
            payload,
            SourceOperation::Persist,
            format!("board-h15-full-history:{id}"),
        )
        .map_err(|_| IngestError::ReservationPayloadMismatch)?;
        let reservation = {
            let authority = self
                .data
                .market_recovery_authority(deadline, cancellation)?;
            authority
                .catalog()
                .market_recovery_read(deadline, cancellation, || {
                    if authority.source(input.metadata.source_id())?.as_ref()
                        != Some(&input.metadata)
                    {
                        return Err(CatalogError::SourceRevisionConflict);
                    }
                    let grant = authority.admit_source_rights(rights)?;
                    authority.reserve_ingest(&identity, &grant)
                })
                .map_err(map_market_recovery_catalog_error)?
        };
        check_market_event_read(deadline, cancellation)?;
        Ok(BoardFullHistoryReservedPublication {
            staging: self,
            reservation,
            input,
        })
    }
}
impl BoardFullHistoryReservedPublication {
    /// Acquires the existing async Parquet publication lease, retaining the raw/recovery gate.
    pub async fn begin(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryPublication, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let publication = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(IngestError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(IngestError::DeadlineExceeded),
            lease = self.staging.data.objects.begin_publication(cancellation) => lease?,
        };
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(ArrowConversionError::from)?;
        check_market_event_read(deadline, cancellation)?;
        Ok(BoardFullHistoryPublication {
            data: self.staging.data,
            _operation: self.staging.operation,
            publication,
            reservation: self.reservation,
            input: self.input,
            schema,
            published: Vec::new(),
            objects: Vec::new(),
            validated: false,
            awaiting_partition: false,
            complete: false,
        })
    }
}
impl BoardFullHistoryPublication {
    /// Authenticates the actual ingest reservation and original complete binding under live rights.
    /// Returns an exact prior committed result only when the existing successful run matches.
    pub fn validate(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
        precommit: &dyn IngestPrecommitAuthority,
    ) -> Result<Option<BoardFullHistoryPublicationReference>, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        precommit.validate_precommit()?;
        if self.validated {
            return Err(IngestError::ReplayConflict);
        }
        let data = &self.data;
        data.manifests.validate_append_schema_bounded(
            &self.input.dataset,
            &self.schema,
            deadline,
            cancellation,
        )?;
        let authority = data.market_recovery_authority(deadline, cancellation)?;
        let coordinates = self
            .input
            .binding
            .canonical_partitions()
            .iter()
            .enumerate()
            .map(|(ordinal, _)| ProviderArtifactInputCoordinate::try_new(ordinal, 0))
            .collect::<Result<Vec<_>, _>>()?;
        let payload = self.input.binding.binding_digest();
        let source = self.input.metadata.source_id();
        let prior =
            authority
                .catalog()
                .market_recovery_read(deadline, cancellation, || {
                    Ok((|| -> Result<Option<BoardFullHistoryPublicationReference>, IngestError> {
        let run = data.validate_run(&authority, &self.reservation, payload, Some(source))?;
        match run.state() {
            IngestRunState::Failed => return Err(IngestError::TerminalRun),
            IngestRunState::Succeeded => {
                let existing = data
                    .manifests
                    .for_run_bounded(self.reservation.run_id(), deadline, cancellation)?
                    .ok_or(IngestError::IncompleteSuccessfulRun)?;
                let retained = authority
                    .provider_logical_publication_binding(payload)?
                    .ok_or(IngestError::IncompleteSuccessfulRun)?;
                if existing.manifest().dataset_id() != &self.input.dataset
                    || retained.terminal() != self.input.binding.terminal()
                    || retained.canonical_partitions() != self.input.binding.canonical_partitions()
                    || !authority.catalog().provider_logical_partition_inputs_match_for_run(
                        self.reservation.run_id(), &self.input.binding, &coordinates,
                    )?
                    || !authority
                        .catalog()
                        .provider_publication_input_matches_for_run(
                            self.reservation.run_id(),
                            payload,
                            "provider_logical",
                            source.as_str(),
                            ProviderArtifactInputCoordinate::try_new(0, 0)?,
                        )?
                {
                    return Err(IngestError::ReplayConflict);
                }
                check_market_event_read(deadline, cancellation)?;
                precommit.validate_precommit()?;
                return Ok(Some(BoardFullHistoryPublicationReference {
                    manifest: existing.manifest().clone(),
                    binding_digest: payload,
                    original_digest: retained.terminal().provider_terminal_evidence_digest(),
                }));
            }
            IngestRunState::Reserved => {}
        }
                Ok(None)
            })())
                })
                .map_err(map_market_recovery_catalog_error)??;
        if prior.is_some() {
            return Ok(prior);
        }
        self.published
            .try_reserve_exact(self.input.binding.canonical_partitions().len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        self.objects
            .try_reserve_exact(self.input.binding.canonical_partitions().len())
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        self.validated = true;
        check_market_event_read(deadline, cancellation)?;
        Ok(None)
    }
    /// Repeats original bounded normalization and native equality on the existing I/O owner.
    pub fn next_partition(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<BoardFullHistoryNativePartition>, IngestError> {
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        board_publication_checkpoint(&control)?;
        if !self.validated || self.awaiting_partition || self.complete {
            return Err(IngestError::ReplayConflict);
        }
        let partition = self
            .input
            .cursor
            .next_partition(&control)
            .map_err(|error| map_board_source_partition_error(error, &control))?;
        let Some(partition) = partition else {
            if self.published.len() != self.input.binding.canonical_partitions().len() {
                return Err(IngestError::InvalidProviderMacroPlan);
            }
            self.complete = true;
            return Ok(None);
        };
        let expected = self
            .input
            .binding
            .canonical_partitions()
            .get(self.published.len())
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        if partition.ordinal() != expected.partition_ordinal()
            || partition.range() != expected.row_range()
            || partition.digest() != expected.semantic_digest()
            || expected.schema_identity().bytes() != self.schema.fingerprint()
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let ordinal = partition.ordinal();
        let (batch, native, revisions) = partition.into_parts();
        let source = self.input.metadata.source_id();
        if batch.request().object().source_id() != source
            || batch.request().object().metadata_revision() != self.input.metadata.revision()
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let observations = ResearchArrowBatch::validated_extraction_observations(&batch)?;
        if observations
            .iter()
            .any(|value| !matches!(value, ResearchObservation::Macro(_)))
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let observed = revisions
            .into_observed_batch_with_native_lineage(source.clone(), &batch, &observations, &native)
            .map_err(map_revision_error)?;
        board_publication_checkpoint(&control)?;
        self.awaiting_partition = true;
        Ok(Some(BoardFullHistoryNativePartition {
            binding_digest: self.input.binding.binding_digest(),
            ordinal,
            batch,
            observed,
        }))
    }
    /// Calls only the existing asynchronous durable revision owner for this exact source batch.
    pub async fn assign_partition(
        &self,
        partition: BoardFullHistoryNativePartition,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BoardFullHistoryAssignedPartition, IngestError> {
        check_market_event_read(deadline, &cancellation)?;
        if !self.awaiting_partition
            || partition.binding_digest != self.input.binding.binding_digest()
            || partition.ordinal as usize != self.published.len()
        {
            return Err(IngestError::ReplayConflict);
        }
        let assignments = self
            .data
            .observed_revision_authority()
            .assign(partition.observed, deadline, cancellation.clone())
            .await
            .map_err(map_revision_error)?;
        check_market_event_read(deadline, &cancellation)?;
        Ok(BoardFullHistoryAssignedPartition {
            catalog_id: self.data.catalog_id,
            binding_digest: partition.binding_digest,
            ordinal: partition.ordinal,
            batch: partition.batch,
            assignments,
        })
    }
    /// Stages only a source-authenticated, revision-assigned Arrow partition through the existing
    /// Parquet publisher; its actual lease remains held until the one final manifest transaction.
    pub async fn stage_partition(
        &mut self,
        partition: BoardFullHistoryArrowPartition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), IngestError> {
        check_market_event_read(deadline, cancellation)?;
        if !self.awaiting_partition
            || partition.catalog_id != self.data.catalog_id
            || partition.binding_digest != self.input.binding.binding_digest()
            || partition.ordinal as usize != self.published.len()
            || partition.converted.schema_ref() != &self.schema
        {
            return Err(IngestError::ReplayConflict);
        }
        let expected = self
            .input
            .binding
            .canonical_partitions()
            .get(self.published.len())
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        let object = self
            .data
            .objects
            .publish_dataset_under_lease(
                &partition.converted.dataset_batch(),
                cancellation,
                &self.publication,
            )
            .await?;
        if object.row_count() != u64::from(expected.row_range().item_count().get()) {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        self.objects.push(ManifestObject::try_new(
            object.content_hash(),
            object.row_count(),
            object.size_bytes(),
            Sha256Digest::new(partition.lineage.bytes()),
        )?);
        self.published.push(object);
        self.awaiting_partition = false;
        check_market_event_read(deadline, cancellation)
    }
    /// Performs the existing sole atomic logical/manifest publication on the retained I/O owner.
    /// Original capture custody becomes published in this same transaction, never beforehand.
    pub fn commit(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
        precommit: &dyn IngestPrecommitAuthority,
    ) -> Result<BoardFullHistoryPublicationReference, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        precommit.validate_precommit()?;
        if !self.validated
            || !self.complete
            || self.awaiting_partition
            || self.published.len() != self.input.binding.canonical_partitions().len()
            || self
                .published
                .iter()
                .try_fold(0_u64, |sum, object| sum.checked_add(object.row_count()))
                != Some(self.input.binding.terminal().total_canonical_rows())
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let data = &self.data;
        let coordinates = self
            .input
            .binding
            .canonical_partitions()
            .iter()
            .enumerate()
            .map(|(ordinal, _)| ProviderArtifactInputCoordinate::try_new(ordinal, 0))
            .collect::<Result<Vec<_>, _>>()?;
        let payload = self.input.binding.binding_digest();
        let authority = data.market_recovery_authority(deadline, cancellation)?;
        authority
            .catalog()
            .market_recovery_read(deadline, cancellation, || {
                Ok(
                    (|| -> Result<BoardFullHistoryPublicationReference, IngestError> {
                        let run = data.validate_run(
                            &authority,
                            &self.reservation,
                            payload,
                            Some(self.input.metadata.source_id()),
                        )?;
                        let dataset_name = SourceIdentifier::try_from(self.input.dataset.as_str())
                            .map_err(|_| IngestError::InvalidDataset)?;
                        let plan = data.manifests.preview_append_bounded(
                            self.input.dataset,
                            &self.schema,
                            self.objects,
                            deadline,
                            cancellation,
                        )?;
                        let committed = data.commit_plan(
                            &authority,
                            &self.reservation,
                            &run,
                            dataset_name,
                            self.schema,
                            plan,
                            &self.published,
                            GenerationKind::Ingest,
                            Some(precommit),
                            None,
                            None,
                            None,
                            PublicationSourceEvidence::ProviderLogicalOriginal(
                                &self.input.binding,
                                &coordinates,
                                deadline,
                                cancellation,
                            ),
                        )?;
                        let retained = authority
                            .provider_logical_publication_binding(payload)?
                            .ok_or(IngestError::IncompleteSuccessfulRun)?;
                        if retained.terminal() != self.input.binding.terminal()
                            || retained.canonical_partitions()
                                != self.input.binding.canonical_partitions()
                        {
                            return Err(IngestError::ReplayConflict);
                        }
                        Ok(BoardFullHistoryPublicationReference {
                            manifest: committed.manifest().clone(),
                            binding_digest: payload,
                            original_digest: retained
                                .terminal()
                                .provider_terminal_evidence_digest(),
                        })
                    })(),
                )
            })
            .map_err(map_market_recovery_catalog_error)?
    }
}

fn board_publication_checkpoint(control: &MarketEventReadControl<'_>) -> Result<(), IngestError> {
    control
        .checkpoint(ResearchObjectControlPoint::BeforeVerification)
        .map_err(|error| match error {
            ResearchObjectControlError::Cancelled => IngestError::Cancelled,
            ResearchObjectControlError::DeadlineExceeded => IngestError::DeadlineExceeded,
            ResearchObjectControlError::Unavailable => IngestError::PublicationAuthorityRevoked,
        })
}
fn map_board_source_partition_error(
    _error: market_squawk_adapter_federal_reserve::BoardFullHistoryError,
    control: &MarketEventReadControl<'_>,
) -> IngestError {
    board_publication_checkpoint(control)
        .err()
        .unwrap_or(IngestError::InvalidProviderMacroPlan)
}

/// Exact original source rows after physically verifying their complete native/map partitions.
/// The eleven values are still source-normalized observations with their original revision one;
/// each serving canonical row must match after the existing revision authority's sole rewrite.
#[derive(Debug)]
pub struct BoardFullHistoryAnnualRead {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    original_digest: EvidenceDigest,
    observations: [market_squawk_domain::MacroObservation; 11],
}
impl BoardFullHistoryAnnualRead {
    /// Exact creating generation reopened under the original source cutoff.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Actual complete original raw/native/canonical relation.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    /// Source-owned full transport/native identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Original source values, units, clocks and dates; no canonical digest substitutes for them.
    pub const fn observations(&self) -> &[market_squawk_domain::MacroObservation; 11] {
        &self.observations
    }
}

/// Canonical selected H.15 rows authenticated against the original complete native/map
/// partitions. This is read evidence bound to one existing analytical selection, not authority
/// to publish data, revise observations, or select different rows.
#[derive(Debug)]
pub struct BoardFullHistoryMacroRead {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    original_digest: EvidenceDigest,
    selection_digest: EvidenceDigest,
    observations: Box<[market_squawk_domain::MacroObservation]>,
}
impl BoardFullHistoryMacroRead {
    /// Exact creating generation authorized by the analytical selection.
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    /// Actual complete original raw/native/canonical relation.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    /// Source-owned full transport/native identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Exact typed canonical selection whose rows were verified against native source evidence.
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }
    /// Canonical values, units, missingness and clocks, retaining only the catalog's revision.
    pub fn observations(&self) -> &[market_squawk_domain::MacroObservation] {
        &self.observations
    }
}

impl AnalyticalDataService {
    /// Authenticates 1–11 selected curve rows through the same bounded original replay as the
    /// annual sample. Call on the existing research I/O owner after the typed analytical reader
    /// has authorized this exact creating manifest. Every canonical field must equal the native
    /// source row; the catalog-assigned revision is the only permitted difference.
    pub fn reopen_board_full_history_macro_selection(
        &self,
        origin: &ProviderLogicalPublicationOrigin,
        selected: &crate::AnalyticalMacroLatestKnownOutput,
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryMacroRead, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let observations = selected.observations();
        if selected.output().manifest() != origin.manifest()
            || selected.source_id() != origin.publication().terminal().source_id()
            || !(1..=11).contains(&observations.len())
            || observations.iter().enumerate().any(|(index, row)| {
                observations[..index]
                    .iter()
                    .any(|previous| previous.series() == row.series())
            })
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let rows = observations
            .iter()
            .map(|row| {
                Ok((
                    row.series().clone(),
                    row.context()
                        .time()
                        .effective()
                        .calendar_date_value()
                        .ok_or(IngestError::InvalidProviderMacroPlan)?,
                ))
            })
            .collect::<Result<Vec<_>, IngestError>>()?;
        let (original_digest, native) =
            self.reopen_board_full_history_rows(origin, &rows, store, deadline, cancellation)?;
        for (original, canonical) in native.into_iter().zip(observations) {
            let expected = ResearchObservation::Macro(original)
                .with_revision(canonical.context().time().revision())
                .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
            if expected != ResearchObservation::Macro(canonical.clone()) {
                return Err(IngestError::InvalidProviderMacroPlan);
            }
        }
        check_market_event_read(deadline, cancellation)?;
        Ok(BoardFullHistoryMacroRead {
            manifest: origin.manifest().clone(),
            binding_digest: origin.publication().binding_digest(),
            original_digest,
            selection_digest: selected.selection_digest(),
            observations: observations.into(),
        })
    }

    /// Reopens only an actual creating-generation catalog origin. Call on the existing research
    /// I/O owner after its canonical reader has authorized this exact manifest. Exact original
    /// raw/transport objects and only the selected complete native/map partitions are physically
    /// reopened. The committed full binding authenticates membership; this is not a whole-file
    /// partition scrub and stages no new object.
    pub fn reopen_board_full_history_origin(
        &self,
        origin: &ProviderLogicalPublicationOrigin,
        dates: [market_squawk_domain::CalendarDate; 11],
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryAnnualRead, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        if dates.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let descriptor = market_squawk_adapter_federal_reserve::h15_treasury_constant_maturities_dashboard_series()
            .iter().find(|series| series.slot() == "10y").ok_or(IngestError::InvalidProviderMacroPlan)?;
        let series = descriptor
            .canonical_macro_series_identifier()
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        let rows = dates.map(|date| (series.clone(), date));
        let (original_digest, observations) =
            self.reopen_board_full_history_rows(origin, &rows, store, deadline, cancellation)?;
        Ok(BoardFullHistoryAnnualRead {
            manifest: origin.manifest().clone(),
            binding_digest: origin.publication().binding_digest(),
            original_digest,
            observations: observations
                .try_into()
                .map_err(|_| IngestError::InvalidProviderMacroPlan)?,
        })
    }

    fn reopen_board_full_history_rows(
        &self,
        origin: &ProviderLogicalPublicationOrigin,
        rows: &[(SourceIdentifier, market_squawk_domain::CalendarDate)],
        store: &SealedResearchJournalStore,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(EvidenceDigest, Vec<market_squawk_domain::MacroObservation>), IngestError> {
        use market_squawk_adapter_federal_reserve::{
            BoardFullHistoryCanonicalPartition, BoardFullHistoryOriginal,
        };
        use market_squawk_sources::LogicalPartitionFamily;
        let control = MarketEventReadControl {
            deadline,
            cancellation,
        };
        board_publication_checkpoint(&control)?;
        let retained = origin.publication();
        let receipt = self
            .provider_logical_original(
                origin.manifest().dataset_id(),
                retained.terminal().source_id(),
                BoardFullHistoryOriginal::native_partition_schema_digest(),
                retained.terminal().source_revision_digest(),
                Some(retained.terminal().provider_terminal_evidence_digest()),
                deadline,
                cancellation,
            )?
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        if receipt.publication_digest() != Some(retained.binding_digest()) {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let original = BoardFullHistoryOriginal::reopen_selected_checkpoint(
            receipt.checkpoint_bytes(),
            rows,
            store,
            &control,
        )
        .map_err(|error| map_board_source_partition_error(error, &control))?;
        let source = original.metadata().source_id();
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()
            .map_err(|_| IngestError::InvalidProviderMacroPlan)?;
        if origin.manifest().dataset_id().as_str() != profile.analytical_dataset().as_str()
            || source.as_str() != BOARD_DDP_SOURCE_ID
            || retained.terminal().source_id() != source
            || retained.terminal().source_revision_digest()
                != EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(
                        serde_json::to_vec(original.metadata())
                            .map_err(|_| IngestError::InvalidProviderMacroPlan)?,
                    )
                    .into(),
                )
            || retained.terminal().execution_attempt_digest() != Some(original.original_digest())
            || retained.terminal().provider_terminal_evidence_digest() != original.original_digest()
            || retained.objects().len() != original.objects().len()
            || retained
                .objects()
                .iter()
                .zip(original.objects())
                .any(|(stored, actual)| {
                    stored.role() != actual.role()
                        || stored.ordinal() != actual.ordinal()
                        || stored.semantic_identity() != actual.semantic_identity()
                        || stored.claim() != actual.object().claim()
                })
            || original.original_observation_count() != retained.terminal().total_canonical_rows()
            || retained.canonical_partitions().is_empty()
            || retained.canonical_partitions().len() > 1_024
            || retained.partitions().len() != 2 * retained.canonical_partitions().len()
        {
            return Err(IngestError::InvalidProviderMacroPlan);
        }
        let original_digest = original.original_digest();
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(ArrowConversionError::from)?;
        let mut selected = vec![None; rows.len()];
        let mut cursor = original.into_canonical_cursor();
        let mut selected_partition_count = 0_u32;
        while let Some(partition) = cursor
            .next_partition(&control)
            .map_err(|error| map_board_source_partition_error(error, &control))?
        {
            board_publication_checkpoint(&control)?;
            selected_partition_count += 1;
            if selected_partition_count as usize > rows.len() {
                return Err(IngestError::InvalidProviderMacroPlan);
            }
            let expected = retained
                .canonical_partitions()
                .get(partition.ordinal() as usize)
                .ok_or(IngestError::InvalidProviderMacroPlan)?;
            if partition.ordinal() != expected.partition_ordinal()
                || partition.range() != expected.row_range()
                || partition.digest() != expected.semantic_digest()
                || expected.schema_identity().bytes() != schema.fingerprint()
            {
                return Err(IngestError::InvalidProviderMacroPlan);
            }
            for family in [
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap,
            ] {
                let mut claims = retained.partitions().iter().filter(|claim| {
                    claim.family() == family && claim.partition_ordinal() == partition.ordinal()
                });
                let claim = claims.next().ok_or(IngestError::InvalidProviderMacroPlan)?;
                if claims.next().is_some()
                    || claim.item_range() != partition.range()
                    || claim.schema_identity()
                        != BoardFullHistoryCanonicalPartition::evidence_schema(family)
                            .map_err(|error| map_board_source_partition_error(error, &control))?
                {
                    return Err(IngestError::InvalidProviderMacroPlan);
                }
                let mut object = store
                    .open_verified_logical_object_claim(claim.claim(), &control)
                    .map_err(|_| {
                        board_publication_checkpoint(&control)
                            .err()
                            .unwrap_or(IngestError::InvalidProviderMacroPlan)
                    })?;
                partition
                    .verify_retained_frames(family, &mut object, &control)
                    .map_err(|error| map_board_source_partition_error(error, &control))?;
            }
            let (batch, _, _) = partition.into_parts();
            for observation in ResearchArrowBatch::validated_extraction_observations(&batch)? {
                let ResearchObservation::Macro(value) = observation else {
                    return Err(IngestError::InvalidProviderMacroPlan);
                };
                if let Some(index) = rows.iter().position(|(series, date)| {
                    value.series() == series
                        && value.context().time().effective().calendar_date_value() == Some(*date)
                }) {
                    if selected[index].replace(value).is_some() {
                        return Err(IngestError::InvalidProviderMacroPlan);
                    }
                }
            }
        }
        let values: Vec<_> = selected
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(IngestError::InvalidProviderMacroPlan)?;
        board_publication_checkpoint(&control)?;
        Ok((original_digest, values))
    }
}
