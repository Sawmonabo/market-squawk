//! Existing source acquisition, immutable publication and exact native EOD action reopen.

mod recovery;
mod replay;
pub(crate) use replay::TiingoCompletedEodHistoryReference;

use super::*;
use crate::application::ResearchProviderPublicationOperation;
use crate::provider_activation::tiingo::{TiingoEodHistoryOperation, TiingoHistoryMetadataInput};
use market_squawk_adapter_tiingo::{
    TiingoEodHistoryStage, TiingoHistoryPlan, TiingoHttpSource, ValidatedTiingoEodHistory,
};
use market_squawk_data::CatalogAuthority;
use market_squawk_domain::ExactPayloadEvidence;
use std::num::{NonZeroU32, NonZeroU64};

/// Exact existing canonical generation containing the complete source date-window graph.
#[derive(Clone, Debug)]
pub(crate) struct TiingoEodHistoryPublicationReceipt {
    restart: TiingoLatestRestartBinding,
}
impl TiingoEodHistoryPublicationReceipt {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.restart.manifest
    }
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.restart.binding_digest
    }
}

struct HistoryPrecommit {
    inner: Arc<dyn IngestPrecommitAuthority>,
    calendar: Arc<dyn IngestPrecommitAuthority>,
}
impl std::fmt::Debug for HistoryPrecommit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiingoHistoryPrecommit")
            .field("calendar", &"[ORIGINAL EXPECTED CALENDAR GUARD]")
            .finish_non_exhaustive()
    }
}
impl IngestPrecommitAuthority for HistoryPrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.inner.validate_precommit()?;
        self.calendar.validate_precommit()?;
        Ok(())
    }
    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.inner.validate_catalog_precommit(catalog)?;
        self.calendar.validate_catalog_precommit(catalog)?;
        Ok(())
    }
}

impl ProductionResearchIngestCoordinator {
    /// Fetches, seals and checkpoints each actual source window before dispatching its successor.
    /// No source body survives in a parallel store and no provider paging token is invented.
    pub(crate) async fn acquire_and_publish_tiingo_eod_history(
        &self,
        source: Arc<TiingoHttpSource>,
        operation: TiingoEodHistoryOperation,
        publication: ResearchProviderPublicationOperation,
        source_deadline: Timestamp,
        seal_deadline: Instant,
    ) -> Result<TiingoEodHistoryPublicationReceipt, TiingoHistoryApplicationError> {
        if operation.plan.pages().is_empty() {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let original_lease = Arc::new(
            self.research
                .analytical()
                .acquire_provider_capture_original_lease(seal_deadline, publication.cancellation())
                .await?,
        );
        let mut checkpoint = operation.checkpoint;
        let cancellation = publication.cancellation().clone();
        let connection_id = uuid::Uuid::new_v4();
        publication
            .validate_precommit()
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let metadata = operation.captured_metadata.decoded().clone();
        let expected_count = u16::try_from(operation.plan.pages().len() + 1)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let original_metadata = match operation.captured_metadata {
            TiingoHistoryMetadataInput::Fresh(captured) => {
                if checkpoint.next_page_index() != 0 {
                    return Err(TiingoHistoryApplicationError::OriginalContinuationRequired);
                }
                self.retain_tiingo_original(
                    captured.capture_material(uuid::Uuid::new_v4(), connection_id)?,
                    &operation.original_context,
                    0,
                    expected_count,
                    &operation.analytical_dataset,
                    metadata.evidence().decoded_at(),
                    &publication,
                    Arc::clone(&original_lease),
                    seal_deadline,
                )
                .await?
            }
            TiingoHistoryMetadataInput::Original { original, .. } => original,
        };
        if original_metadata.session() != operation.original_context.session
            || original_metadata.expected_count() != expected_count
            || original_metadata.dataset() != &operation.analytical_dataset
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let (metadata_token, metadata_body) = self
            .reseal_tiingo_original(
                original_metadata,
                Arc::clone(&original_lease),
                seal_deadline,
                &cancellation,
            )
            .await?;
        // A completed seal may precede its checkpoint by one page; no larger gap is valid.
        let invalid_tail = checkpoint
            .next_page_index()
            .checked_add(2)
            .and_then(|n| u16::try_from(n).ok())
            .ok_or(TiingoHistoryApplicationError::Admission)?;
        if self
            .tiingo_original(
                operation.original_context.session,
                invalid_tail,
                seal_deadline,
                &cancellation,
            )
            .await?
            .is_some()
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        let scratch = Arc::new(
            self.research
                .analytical()
                .operation_scratch()
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
        );
        let stage_scratch = Arc::clone(&scratch);
        let plan = operation.plan.clone();
        let instrument = operation.instrument.clone();
        let contract = operation.contract.clone();
        let cash_unit = operation.cash_unit.clone();
        let admitted_plan_digest = operation.admitted_plan_digest;
        let store = self.research.provider_capture_store();
        let (mut stage, mut pack) = self
            .research
            .run_owned_research_io(seal_deadline, &cancellation, move |worker| {
                let control = HistoryStreamControl {
                    deadline: seal_deadline,
                    cancellation: worker,
                };
                let stage = TiingoEodHistoryStage::try_new(
                    plan,
                    metadata,
                    metadata_token.persisted_receipt().clone(),
                    instrument,
                    contract,
                    cash_unit,
                    admitted_plan_digest,
                    stage_scratch.path(),
                )
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                let mut pack = market_squawk_sources::PendingProviderCapturePack::begin(
                    Arc::clone(&store),
                    history_object_admission(8 * 1024 * 1024 * 1024)?,
                )
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                pack.append(metadata_token, vec![metadata_body], &control)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                Ok::<_, TiingoHistoryApplicationError>((stage, pack))
            })
            .await??;
        let mut last_page_identity = None;
        let mut observed_at = Timestamp::from_unix_nanos(i64::MIN);
        let mut native_rows = 0_u64;
        for index in 0..operation.plan.pages().len() {
            publication
                .validate_precommit()
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
            let ordinal =
                u16::try_from(index + 1).map_err(|_| TiingoHistoryApplicationError::Admission)?;
            let original = match self
                .tiingo_original(
                    operation.original_context.session,
                    ordinal,
                    seal_deadline,
                    &cancellation,
                )
                .await?
            {
                Some(original) => original,
                None => {
                    if index != checkpoint.next_page_index() as usize {
                        return Err(TiingoHistoryApplicationError::OriginalContinuationRequired);
                    }
                    let captured = source
                        .fetch_history_page(
                            &operation.plan,
                            &checkpoint,
                            source_deadline,
                            &cancellation,
                        )
                        .await?;
                    let decoded_at = captured.decoded().evidence().decoded_at();
                    self.retain_tiingo_original(
                        captured.capture_material(uuid::Uuid::new_v4(), connection_id)?,
                        &operation.original_context,
                        ordinal,
                        expected_count,
                        &operation.analytical_dataset,
                        decoded_at,
                        &publication,
                        Arc::clone(&original_lease),
                        seal_deadline,
                    )
                    .await?
                }
            };
            let (response, token, body) = self
                .reopen_tiingo_original_page(
                    original,
                    operation.plan.pages()[index].clone(),
                    &operation.original_context,
                    Arc::clone(&original_lease),
                    seal_deadline,
                    &cancellation,
                )
                .await?;
            native_rows = native_rows
                .checked_add(response.rows().len() as u64)
                .ok_or(TiingoHistoryApplicationError::Admission)?;
            observed_at = observed_at.max(response.evidence().received_at());
            let (returned_stage, returned_pack, sealed_page) = self
                .research
                .run_owned_research_io(seal_deadline, &cancellation, move |worker| {
                    let control = HistoryStreamControl {
                        deadline: seal_deadline,
                        cancellation: worker.clone(),
                    };
                    let sealed_page = stage
                        .push_page(
                            &response,
                            token.persisted_receipt(),
                            response.evidence().decoded_at(),
                            &worker,
                        )
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    pack.append(token, vec![body], &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    Ok::<_, TiingoHistoryApplicationError>((stage, pack, sealed_page))
                })
                .await??;
            stage = returned_stage;
            pack = returned_pack;
            if index + 1 == checkpoint.next_page_index() as usize
                && checkpoint.predecessor_page_identity() != Some(sealed_page.page_identity())
            {
                return Err(TiingoHistoryApplicationError::Admission);
            }
            if index == checkpoint.next_page_index() as usize {
                publication
                    .validate_precommit()
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                checkpoint =
                    source.checkpoint_history_page(&operation.plan, &checkpoint, &sealed_page)?;
            }
            last_page_identity = Some(sealed_page.page_identity());
            // The checkpoint commits only after durable original custody and source validation.
            // Drop every decoded/mapped row from this page before acquiring its successor.
        }
        let terminal =
            source.validate_history_terminal(&operation.plan, &checkpoint, last_page_identity)?;
        let expected = Arc::clone(&operation.expected_session_authority);
        let store = self.research.provider_capture_store();
        let (validated, pack) = self
            .research
            .run_owned_research_io(seal_deadline, &cancellation, move |worker| {
                let validated = stage
                    .finish(terminal, expected.as_ref(), &worker)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                let control = HistoryStreamControl {
                    deadline: seal_deadline,
                    cancellation: worker,
                };
                let pack = pack
                    .finish(&store, &control, 0)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                Ok::<_, TiingoHistoryApplicationError>((validated, pack))
            })
            .await??;
        let expected = operation
            .expected_session_authority
            .expected_evidence()
            .clone();
        let (validated, objects, pack_seal) = self
            .retain_tiingo_history_logical_original(
                validated,
                pack,
                operation.analytical_dataset.clone(),
                &publication,
                Arc::clone(&original_lease),
                observed_at,
                seal_deadline,
                &cancellation,
            )
            .await?;
        drop(original_lease);
        let calendar = operation
            .expected_session_authority
            .acquire_publication_authority(&expected)
            .await
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let precommit = Arc::new(HistoryPrecommit {
            inner: publication.precommit_authority(),
            calendar,
        });
        self.publish_tiingo_history_stream(
            validated,
            objects,
            pack_seal,
            scratch,
            operation.analytical_dataset,
            &publication,
            precommit,
            observed_at,
            native_rows,
            source_deadline,
            seal_deadline,
            cancellation,
        )
        .await
    }
}

/// Opaque same-publication bar/action evidence consumed by the action ledger.
#[derive(Debug)]
pub(crate) struct TiingoCompletedEodActionRead {
    reference: TiingoCompletedEodHistoryReference,
    source: Arc<market_squawk_data::RetainedTiingoEodActionHistory>,
}
impl TiingoCompletedEodActionRead {
    pub(crate) fn history(&self) -> &market_squawk_data::CompleteMarketBarHistoryCursor {
        self.source.history()
    }
    pub(crate) fn binding(
        &self,
    ) -> &market_squawk_data::PersistedProviderLogicalPublicationBinding {
        self.source.binding()
    }
    pub(crate) fn actions(&self) -> &ValidatedTiingoEodHistory {
        self.source.actions()
    }
    pub(crate) fn knowledge_cutoff(&self) -> Timestamp {
        self.source.knowledge_cutoff()
    }
    /// Shared genuine data-owned source read, never reconstructed from app JSON or normalized rows.
    pub(crate) const fn source_history(
        &self,
    ) -> &Arc<market_squawk_data::RetainedTiingoEodActionHistory> {
        &self.source
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TiingoHistoryApplicationError {
    #[error("Tiingo complete history is outside the exact admitted source or resource scope")]
    Admission,
    #[error("Tiingo history requires its original retained partial acquisition continuation")]
    OriginalContinuationRequired,
    #[error(transparent)]
    Latest(#[from] TiingoLatestApplicationError),
    #[error(transparent)]
    Source(#[from] market_squawk_adapter_tiingo::TiingoHttpSourceError),
    #[error(transparent)]
    CaptureMaterial(#[from] market_squawk_adapter_tiingo::TiingoCaptureMaterialError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Eod(#[from] market_squawk_adapter_tiingo::TiingoEodMapError),
    #[error(transparent)]
    Evidence(#[from] market_squawk_adapter_tiingo::TiingoHistoryEvidenceError),
    #[error(transparent)]
    Publication(#[from] market_squawk_adapter_tiingo::TiingoEodHistoryPublicationError),
    #[error(transparent)]
    Extraction(#[from] market_squawk_sources::ExtractionError),
    #[error(transparent)]
    Calendar(#[from] crate::application::market_calendar::CompletedMarketSessionError),
    #[error(transparent)]
    Read(#[from] market_squawk_data::AnalyticalReadError),
    #[error(transparent)]
    Ingest(#[from] market_squawk_data::IngestError),
}

struct HistoryStreamControl {
    deadline: Instant,
    cancellation: CancellationToken,
}
impl market_squawk_platform::ResearchObjectControl for HistoryStreamControl {
    fn checkpoint(
        &self,
        _: market_squawk_platform::ResearchObjectControlPoint,
    ) -> Result<(), market_squawk_platform::ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            return Err(market_squawk_platform::ResearchObjectControlError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(market_squawk_platform::ResearchObjectControlError::DeadlineExceeded);
        }
        Ok(())
    }
}
fn history_object_admission(
    bytes: u64,
) -> Result<market_squawk_platform::ResearchObjectAdmission, TiingoHistoryApplicationError> {
    market_squawk_platform::ResearchObjectAdmission::try_new(bytes.max(1), 4095)
        .map_err(|_| TiingoHistoryApplicationError::Admission)
}

impl ProductionResearchIngestCoordinator {
    /// Seals and retains the completed logical original before releasing acquisition custody.
    #[allow(clippy::too_many_arguments)]
    async fn retain_tiingo_history_logical_original(
        &self,
        proof: ValidatedTiingoEodHistory,
        pack: market_squawk_sources::SealedProviderCapturePack,
        dataset: DatasetId,
        publication: &ResearchProviderPublicationOperation,
        original_lease: Arc<market_squawk_data::ProviderCaptureOriginalLease>,
        observed_at: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            ValidatedTiingoEodHistory,
            Vec<market_squawk_sources::SealedLogicalObjectInput>,
            market_squawk_sources::ProviderCapturePackSeal,
        ),
        TiingoHistoryApplicationError,
    > {
        use market_squawk_sources::{LogicalObjectRole, SealedLogicalObjectInput};
        use std::io::Write as _;
        let descriptor = serde_json::to_vec(proof.descriptor())
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let rights = publication
            .rights()
            .decision(history_digest(&descriptor), observed_at)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let source = publication.source().clone();
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        let (pack, pack_seal) = pack.into_parts();
        let worker_store = Arc::clone(&store);
        let (proof, objects, _) = self
            .research
            .run_owned_research_io(deadline, cancellation, move |worker| {
                let _lease = original_lease;
                let control = HistoryStreamControl {
                    deadline,
                    cancellation: worker.clone(),
                };
                let descriptor = serde_json::to_vec(proof.descriptor())
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                let terminal_digest = history_digest(&descriptor);
                let mut objects = vec![pack];
                let mut pending = worker_store
                    .begin_logical_object(history_object_admission(descriptor.len() as u64)?)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                if pending.write_all(&descriptor).is_err() {
                    let _ = worker_store.abort_logical_object(pending);
                    return Err(TiingoHistoryApplicationError::Admission);
                }
                let object = worker_store
                    .finish_logical_object(pending, &control)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                objects.push(
                    SealedLogicalObjectInput::try_from_verified(
                        LogicalObjectRole::Catalog,
                        1,
                        terminal_digest,
                        object,
                        &control,
                    )
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                );
                for ordinal in 2..=4 {
                    let mut pending = worker_store
                        .begin_logical_object(history_object_admission(8 * 1024 * 1024 * 1024)?)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    let written = match ordinal {
                        2 => proof.write_page_index(&mut pending, &worker),
                        3 => proof.write_session_index(&mut pending, &worker),
                        _ => proof.write_action_index(&mut pending, &worker),
                    };
                    if written.is_err() {
                        let _ = worker_store.abort_logical_object(pending);
                        return Err(TiingoHistoryApplicationError::Admission);
                    }
                    let object = worker_store
                        .finish_logical_object(pending, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    let digest = object.content_digest();
                    objects.push(
                        SealedLogicalObjectInput::try_from_verified(
                            LogicalObjectRole::ProviderComponent,
                            ordinal,
                            digest,
                            object,
                            &control,
                        )
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    );
                }
                data.retain_provider_logical_original_for_contract(
                    &source,
                    &dataset,
                    market_squawk_sources::ProviderNativeLineageSchema::for_implementation(
                        ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
                    )
                    .fingerprint(),
                    terminal_digest,
                    proof.descriptor().max_received_at(),
                    &descriptor,
                    &objects,
                    &rights,
                    &worker_store,
                    deadline,
                    &worker,
                )?;
                Ok::<_, TiingoHistoryApplicationError>((proof, objects, terminal_digest))
            })
            .await??;
        Ok((proof, objects, pack_seal))
    }

    /// Publishes the complete native history through the common logical stream owner. Original
    /// payloads, all ordered indexes and canonical chunks cross one atomic terminal commit.
    #[allow(clippy::too_many_arguments)]
    async fn publish_tiingo_history_stream(
        &self,
        mut proof: ValidatedTiingoEodHistory,
        mut objects: Vec<market_squawk_sources::SealedLogicalObjectInput>,
        pack_seal: market_squawk_sources::ProviderCapturePackSeal,
        _scratch: Arc<market_squawk_data::OperationScratchDirectory>,
        dataset: DatasetId,
        publication: &ResearchProviderPublicationOperation,
        precommit: Arc<dyn IngestPrecommitAuthority>,
        observed_at: Timestamp,
        native_rows: u64,
        source_deadline: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TiingoEodHistoryPublicationReceipt, TiingoHistoryApplicationError> {
        use market_squawk_data::{IngestIdentity, SourceOperation};
        use market_squawk_sources::{
            LogicalItemRange, LogicalPartitionFamily, LogicalPartitionSetAdmission,
            PendingLogicalPartitionSet, ProviderLogicalTerminalInput, SealedLogicalPartitionInput,
            SealedProviderLogicalPublicationBinding,
        };
        use sha2::{Digest as _, Sha256};
        let source = publication.source().clone();
        if observed_at < publication.source_registered_at() || !source.is_effective_at(observed_at)
        {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        precommit.validate_precommit()?;
        let store = self.research.provider_capture_store();
        let mut staging = self.research.analytical().begin_tiingo_history_stream(
            dataset.clone(),
            &proof,
            &cancellation,
        )?;
        let partition_admission = LogicalPartitionSetAdmission::try_new(
            history_object_admission(32 * 1024 * 1024)?,
            4096,
            1024,
            128 * 1024,
        )
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let schema = market_squawk_sources::ProviderNativeLineageSchema::for_implementation(
            ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
        );
        let mut native_partitions = PendingLogicalPartitionSet::begin(
            LogicalPartitionFamily::ProviderNative,
            schema.fingerprint(),
            partition_admission,
            0,
        )
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let mut row_partitions = PendingLogicalPartitionSet::begin(
            LogicalPartitionFamily::CanonicalRowMap,
            history_digest(b"market-squawk/tiingo-history/logical-row-map/v1"),
            partition_admission,
            0,
        )
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let mut expectations = Vec::new();
        let mut canonical_rows = 0_u64;
        loop {
            let returned = self
                .research
                .run_owned_research_io(deadline, &cancellation, move |worker| {
                    let request = proof
                        .extraction_request(
                            source_deadline,
                            NonZeroU32::new(1024)
                                .ok_or(TiingoHistoryApplicationError::Admission)?,
                            NonZeroU64::new(32 * 1024 * 1024)
                                .ok_or(TiingoHistoryApplicationError::Admission)?,
                        )
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    let Some(request) = request else {
                        return Ok((proof, None));
                    };
                    let ordinal = proof
                        .next_page_ordinal()
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .ok_or(TiingoHistoryApplicationError::Admission)?;
                    let receipt = proof
                        .page_at(ordinal)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .ok_or(TiingoHistoryApplicationError::Admission)?
                        .sealed_receipt()
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .clone();
                    let chunk = proof
                        .next_chunk(&request, &worker)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .ok_or(TiingoHistoryApplicationError::Admission)?;
                    Ok::<_, TiingoHistoryApplicationError>((proof, Some((chunk, receipt, ordinal))))
                })
                .await??;
            proof = returned.0;
            let Some((chunk, receipt, original_page_ordinal)) = returned.1 else {
                break;
            };
            if chunk.global_start() != canonical_rows
                || chunk.original_page_ordinal() != original_page_ordinal
            {
                return Err(TiingoHistoryApplicationError::Admission);
            }
            let row_count = chunk.batch().records().len();
            let raw_store = Arc::clone(&store);
            let start = canonical_rows;
            let (returned_chunk, returned_native, returned_rows, returned_receipt) = self.research.run_owned_research_io(
                deadline, &cancellation, move |worker| {
                    let control = HistoryStreamControl { deadline, cancellation: worker };
                    let native = chunk.native_lineage();
                    native.validate(chunk.batch()).map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    for (local, (record, row)) in chunk.batch().records().iter().zip(native.rows()).enumerate() {
                        let ordinal = start.checked_add(local as u64).ok_or(TiingoHistoryApplicationError::Admission)?;
                        native_partitions.stage_frame(&raw_store, &control, ordinal,
                            row.semantic_payload(), row.semantic_payload_digest())
                            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                        let frame = receipt.row_frame(u32::try_from(ordinal).map_err(|_| TiingoHistoryApplicationError::Admission)?, 0)?;
                        let mapping = serde_json::to_vec(&serde_json::json!({
                            "canonical_row_ordinal": frame.canonical_row_ordinal(), "capture_page_ordinal": frame.capture_page_ordinal(),
                            "segment_ordinal": frame.segment_ordinal(), "physical_frame_ordinal": frame.physical_frame_ordinal(),
                            "page_body_digest": frame.page_body_digest(), "received_at": frame.received_at(), "source_sequence": frame.source_sequence(),
                            "canonical_record_digest": record.evidence().content_digest(), "native_semantic_digest": row.semantic_payload_digest(),
                            "original_page_ordinal": original_page_ordinal,
                        })).map_err(|_| TiingoHistoryApplicationError::Admission)?;
                        row_partitions.stage_frame(&raw_store, &control, ordinal, &mapping, history_digest(&mapping))
                            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    }
                    native_partitions.seal_current_partition(&raw_store, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    row_partitions.seal_current_partition(&raw_store, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                    Ok::<_, TiingoHistoryApplicationError>((chunk, native_partitions, row_partitions, receipt))
                }).await??;
            native_partitions = returned_native;
            row_partitions = returned_rows;
            expectations.push(
                self.research
                    .analytical()
                    .stage_tiingo_history_chunk(
                        &mut staging,
                        returned_chunk,
                        &returned_receipt,
                        &cancellation,
                    )
                    .await?,
            );
            canonical_rows = canonical_rows
                .checked_add(row_count as u64)
                .ok_or(TiingoHistoryApplicationError::Admission)?;
        }
        let session_index = objects
            .get(3)
            .ok_or(TiingoHistoryApplicationError::Admission)?
            .object()
            .clone();
        let session_count = NonZeroU32::new(
            u32::try_from(proof.descriptor().session_count())
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
        )
        .ok_or(TiingoHistoryApplicationError::Admission)?;
        let raw_store = Arc::clone(&store);
        let partitions = self
            .research
            .run_owned_research_io(deadline, &cancellation, move |worker| {
                let control = HistoryStreamControl {
                    deadline,
                    cancellation: worker,
                };
                let decoded = SealedLogicalPartitionInput::try_from_framed_object(
                    LogicalPartitionFamily::DecodedEvent,
                    0,
                    LogicalItemRange::try_new(0, session_count)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    history_digest(b"market-squawk/tiingo-history/session-index/v1"),
                    128 * 1024,
                    raw_store
                        .open_verified_logical_object(&session_index, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                    &control,
                )
                .map_err(|_| TiingoHistoryApplicationError::Admission)?;
                let mut partitions = vec![decoded];
                partitions.extend(
                    native_partitions
                        .finish(&raw_store, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .into_partitions()
                        .into_vec(),
                );
                partitions.extend(
                    row_partitions
                        .finish(&raw_store, &control)
                        .map_err(|_| TiingoHistoryApplicationError::Admission)?
                        .into_partitions()
                        .into_vec(),
                );
                Ok::<_, TiingoHistoryApplicationError>(partitions)
            })
            .await??;
        let logical_bytes = objects
            .iter()
            .try_fold(0_u64, |total, object| {
                total.checked_add(object.object().size_bytes())
            })
            .ok_or(TiingoHistoryApplicationError::Admission)?;
        let terminal_digest = history_digest(
            &serde_json::to_vec(proof.descriptor())
                .map_err(|_| TiingoHistoryApplicationError::Admission)?,
        );
        let binding = SealedProviderLogicalPublicationBinding::try_new(
            ProviderLogicalTerminalInput {
                source_id: source.source_id().clone(),
                source_revision_digest: source
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest(),
                execution_attempt_digest: Some(pack_seal.captures_digest()),
                provider_terminal_evidence_digest: terminal_digest,
                total_decoded_events: native_rows,
                total_canonical_rows: canonical_rows,
                total_logical_object_bytes: logical_bytes,
            },
            &[
                LogicalPartitionFamily::DecodedEvent,
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap,
            ],
            std::mem::take(&mut objects),
            partitions,
            expectations,
        )
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let digest = binding.binding_digest();
        let rights = publication
            .rights()
            .decision(digest, observed_at)
            .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let identity = IngestIdentity::try_new(
            source.source_id().clone(),
            digest,
            SourceOperation::Persist,
            format!(
                "tiingo-history-logical:{}:{:x}",
                dataset.as_str(),
                Sha256::digest(digest.bytes())
            ),
        )
        .map_err(|_| TiingoHistoryApplicationError::Admission)?;
        let reservation = self
            .research
            .analytical()
            .reserve_source_ingest(&source, observed_at, rights, &identity, &cancellation)
            .await?;
        precommit.validate_precommit()?;
        let (committed, retained_digest) = self
            .research
            .analytical()
            .finish_tiingo_history_stream(
                staging,
                reservation,
                binding,
                proof,
                pack_seal,
                precommit,
                cancellation,
            )
            .await?;
        if retained_digest != digest {
            return Err(TiingoHistoryApplicationError::Admission);
        }
        Ok(TiingoEodHistoryPublicationReceipt {
            restart: TiingoLatestRestartBinding {
                manifest: committed.manifest().clone(),
                binding_digest: digest,
                source_id: source.source_id().clone(),
                expected_record_count: usize::try_from(canonical_rows)
                    .map_err(|_| TiingoHistoryApplicationError::Admission)?,
                native_schema_version: schema.version(),
                native_schema_fingerprint: schema.fingerprint(),
            },
        })
    }
}
fn history_digest(bytes: &[u8]) -> EvidenceDigest {
    use sha2::{Digest as _, Sha256};
    EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        Sha256::digest(bytes).into(),
    )
}
