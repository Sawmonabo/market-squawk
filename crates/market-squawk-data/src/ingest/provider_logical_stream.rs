//! Private bounded canonical staging for one complete provider logical publication.
use super::*;
use crate::OperationScratchDirectory;
use crate::arrow_convert::ResearchLineageDigestAccumulator;
use market_squawk_adapter_tiingo::{TiingoEodHistoryChunk, ValidatedTiingoEodHistory};
use market_squawk_domain::RevisionNumber;
use market_squawk_sources::ProviderCapturePackSeal;
use market_squawk_sources::{
    ExtractionContentAccumulator, ExtractionRequest, ProviderNativeLineageBatch,
};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroU32;

const MAX_CHUNK_BYTES: u64 = 32 * 1024 * 1024;
const MAX_STAGING_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const WRITER_MEMORY_BYTES: usize = 64 * 1024 * 1024;

/// Private staged chunks; only a complete live logical binding can publish them.
pub struct ProviderLogicalStreamStaging {
    kind: StreamKind,
    catalog_id: uuid::Uuid,
    dataset: DatasetId,
    source_id: SourceId,
    directory: Arc<OperationScratchDirectory>,
    request: Option<ExtractionRequest>,
    chunks: Vec<StagedChunk>,
    rows: u64,
    bytes: u64,
    poisoned: bool,
    capture: Option<market_squawk_sources::SealedProviderCaptureSetReceipt>,
}
#[derive(Clone, Copy)]
enum StreamKind {
    SecFiling,
    TiingoHistory { descriptor_digest: EvidenceDigest },
}
enum StreamTerminal {
    SecFiling(CompanyIdentityObservation),
    TiingoHistory {
        proof: ValidatedTiingoEodHistory,
        pack: ProviderCapturePackSeal,
    },
}
impl fmt::Debug for ProviderLogicalStreamStaging {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderLogicalStreamStaging")
            .field("dataset", &self.dataset)
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}
struct CancelOnDrop(BlockingIoSupervisor);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct StagedChunk {
    ordinal: usize,
    bytes: u64,
    digest: [u8; 32],
    expectation: CanonicalPartitionExpectation,
    native_schema: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    original_page_ordinal: Option<usize>,
    row_map_schema: EvidenceDigest,
    native_object_digest: EvidenceDigest,
    row_map_object_digest: EvidenceDigest,
}
#[derive(Serialize, Deserialize)]
struct ChunkValues {
    batch: ExtractionBatch,
    revisions: Vec<RevisionNumber>,
    native_digests: Vec<EvidenceDigest>,
}

/// Minted only after original live capture values and whole-publication terminal closure agree.
#[derive(Debug)]
pub(crate) struct LogicalCompanyIdentityAuthorization {
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    observation_digest: EvidenceDigest,
    parent_digest: EvidenceDigest,
}
impl LogicalCompanyIdentityAuthorization {
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub(crate) const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }
    pub(crate) const fn parent_digest(&self) -> EvidenceDigest {
        self.parent_digest
    }
}

impl AnalyticalDataService {
    /// Starts private canonical staging without retaining a filing-sized batch or publication lock.
    pub fn begin_provider_logical_stream(
        &self,
        dataset: DatasetId,
        source_id: SourceId,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalStreamStaging, IngestError> {
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        Ok(ProviderLogicalStreamStaging {
            kind: StreamKind::SecFiling,
            catalog_id: self.catalog_id,
            dataset,
            source_id,
            directory: Arc::new(self.objects.operation_scratch()?),
            request: None,
            chunks: Vec::new(),
            rows: 0,
            bytes: 0,
            poisoned: false,
            capture: None,
        })
    }

    /// Starts a Tiingo publication only from the adapter's calendar-closed typed proof.
    pub fn begin_tiingo_history_stream(
        &self,
        dataset: DatasetId,
        proof: &ValidatedTiingoEodHistory,
        cancellation: &CancellationToken,
    ) -> Result<ProviderLogicalStreamStaging, IngestError> {
        if !proof.publication_authorized() {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let mut staging = self.begin_provider_logical_stream(
            dataset,
            SourceId::try_from("tiingo-starter").map_err(|_| IngestError::InvalidDataset)?,
            cancellation,
        )?;
        staging.kind = StreamKind::TiingoHistory {
            descriptor_digest: descriptor_digest(proof)?,
        };
        Ok(staging)
    }

    /// Stages only a chunk emitted by the typed Tiingo history proof, preserving its original page.
    pub async fn stage_tiingo_history_chunk(
        &self,
        staging: &mut ProviderLogicalStreamStaging,
        chunk: TiingoEodHistoryChunk,
        receipt: &market_squawk_sources::SealedProviderCaptureSetReceipt,
        cancellation: &CancellationToken,
    ) -> Result<CanonicalPartitionExpectation, IngestError> {
        if !matches!(staging.kind, StreamKind::TiingoHistory { .. }) {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let (batch, native, revisions, original_page_ordinal, global_start) = chunk.into_parts();
        if global_start != staging.rows {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let pages = vec![0u16; batch.records().len()];
        self.stage_logical_chunk(
            staging,
            batch,
            native,
            revisions,
            receipt,
            &pages,
            Some(original_page_ordinal),
            cancellation,
        )
        .await
    }

    /// Validates live native/revision authority, then retains only one checked chunk on private disk.
    pub async fn stage_provider_logical_stream_chunk(
        &self,
        staging: &mut ProviderLogicalStreamStaging,
        batch: ExtractionBatch,
        native: ProviderNativeLineageBatch,
        revisions: ExtractionRevisionPlan,
        receipt: &market_squawk_sources::SealedProviderCaptureSetReceipt,
        page_ordinals: &[u16],
        cancellation: &CancellationToken,
    ) -> Result<CanonicalPartitionExpectation, IngestError> {
        if !matches!(staging.kind, StreamKind::SecFiling) {
            return Err(IngestError::ProviderCaptureRequired);
        }
        self.stage_logical_chunk(
            staging,
            batch,
            native,
            revisions,
            receipt,
            page_ordinals,
            None,
            cancellation,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_logical_chunk(
        &self,
        staging: &mut ProviderLogicalStreamStaging,
        batch: ExtractionBatch,
        native: ProviderNativeLineageBatch,
        revisions: ExtractionRevisionPlan,
        receipt: &market_squawk_sources::SealedProviderCaptureSetReceipt,
        page_ordinals: &[u16],
        original_page_ordinal: Option<usize>,
        cancellation: &CancellationToken,
    ) -> Result<CanonicalPartitionExpectation, IngestError> {
        let sec_filing = matches!(staging.kind, StreamKind::SecFiling);
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        if staging.poisoned
            || staging.catalog_id != self.catalog_id
            || staging.chunks.len() >= market_squawk_sources::MAX_PROVIDER_CANONICAL_PARTITIONS
            || batch.records().is_empty()
            || batch.records().len() > if sec_filing { 256 } else { 1024 }
            || batch.request().object().source_id() != &staging.source_id
            || sec_filing
                && staging
                    .request
                    .as_ref()
                    .is_some_and(|request| request != batch.request())
            || !matches!(
                batch.request().object().capture_identity(),
                SourceObjectCaptureIdentity::Paged { .. }
            )
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let original_request = batch.request().clone();
        let batch = batch
            .try_bind_provider_capture(receipt.capture())
            .map_err(IngestError::ContentIdentity)?;
        if batch.request() != &original_request
            || page_ordinals.len() != batch.records().len()
            || sec_filing
                && staging
                    .capture
                    .as_ref()
                    .is_some_and(|capture| capture != receipt)
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        // Any failure or dropped future after admission invalidates the incomplete staging owner.
        staging.poisoned = true;
        native
            .validate(&batch)
            .map_err(|_| IngestError::ProviderCaptureRequired)?;
        let observations = ResearchArrowBatch::validated_extraction_observations(&batch)?;
        let observed = revisions
            .into_observed_batch_with_native_lineage(
                staging.source_id.clone(),
                &batch,
                &observations,
                &native,
            )
            .map_err(map_revision_error)?;
        let assignments = self
            .observed_revision_authority()
            .assign(
                observed,
                Instant::now() + REVISION_ASSIGNMENT_DEADLINE,
                cancellation.clone(),
            )
            .await
            .map_err(map_revision_error)?;
        drop(observations);
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| IngestError::InvalidDataset)?;
        let content = ExtractionContentIdentity::try_from_batch(&batch)
            .map_err(IngestError::ContentIdentity)?;
        let ordinal = staging.chunks.len();
        let range = LogicalItemRange::try_new(
            staging.rows,
            NonZeroU32::new(batch.records().len() as u32).ok_or(IngestError::InvalidDataset)?,
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        let expectation = CanonicalPartitionExpectation::try_new(
            ordinal as u32,
            range,
            EvidenceDigest::new(DigestAlgorithm::Sha256, schema.fingerprint()),
            content.digest(),
            ordinal as u32,
            ordinal as u32,
        )
        .map_err(|_| IngestError::ProviderCaptureRequired)?;
        let native_schema = native.schema().fingerprint();
        let native_batch_digest = native.batch_digest();
        let row_map_schema = row_map_schema(sec_filing);
        let mut native_hash = Sha256::new();
        let mut map_hash = Sha256::new();
        for (local, ((record, row), page)) in batch
            .records()
            .iter()
            .zip(native.rows())
            .zip(page_ordinals)
            .enumerate()
        {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            let ordinal = staging.rows + local as u64;
            hash_frame(&mut native_hash, ordinal, row.semantic_payload());
            let frame = receipt.row_frame(
                u32::try_from(ordinal).map_err(|_| IngestError::InvalidDataset)?,
                *page,
            )?;
            let mut mapping = serde_json::json!({
                "canonical_row_ordinal": frame.canonical_row_ordinal(), "capture_page_ordinal": frame.capture_page_ordinal(),
                "segment_ordinal": frame.segment_ordinal(), "physical_frame_ordinal": frame.physical_frame_ordinal(),
                "page_body_digest": frame.page_body_digest(), "received_at": frame.received_at(), "source_sequence": frame.source_sequence(),
                "canonical_record_digest": record.evidence().content_digest(), "native_semantic_digest": row.semantic_payload_digest(),
            });
            if let Some(page) = original_page_ordinal {
                mapping["original_page_ordinal"] = serde_json::json!(page);
            }
            let mapping = serde_json::to_vec(&mapping)?;
            hash_frame(&mut map_hash, ordinal, &mapping);
        }
        let native_object_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, native_hash.finalize().into());
        let row_map_object_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, map_hash.finalize().into());
        let native_digests = native
            .rows()
            .iter()
            .map(|row| row.semantic_payload_digest())
            .collect();
        drop(native);
        let request = batch.request().clone();
        let values = ChunkValues {
            batch,
            revisions: assignments.as_slice().to_vec(),
            native_digests,
        };
        let directory = Arc::clone(&staging.directory);
        let supervisor = BlockingIoSupervisor::new(cancellation.child_token());
        let _cancel_on_drop = CancelOnDrop(supervisor.clone());
        let token = supervisor.cancellation().clone();
        let worker = supervisor
            .spawn_blocking(move || write_chunk(&directory, ordinal, &values, &token))
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        let (bytes, digest) = worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)??;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        staging.bytes = staging
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_STAGING_BYTES)
            .ok_or(IngestError::InvalidDataset)?;
        staging.rows = range
            .end_exclusive()
            .map_err(|_| IngestError::InvalidDataset)?;
        if sec_filing || staging.request.is_none() {
            staging.request = Some(request);
        }
        if sec_filing {
            staging.capture = Some(receipt.clone());
        }
        staging.chunks.push(StagedChunk {
            ordinal,
            bytes,
            digest,
            expectation: expectation.clone(),
            native_schema,
            native_batch_digest,
            original_page_ordinal,
            row_map_schema,
            native_object_digest,
            row_map_object_digest,
        });
        staging.poisoned = false;
        Ok(expectation)
    }

    /// Replays verified bounded chunks under one lease and atomically commits their terminal seal.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_provider_logical_stream(
        &self,
        staging: ProviderLogicalStreamStaging,
        reservation: IngestReservation,
        binding: SealedProviderLogicalPublicationBinding,
        company_identity: CompanyIdentityObservation,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), IngestError> {
        if !matches!(staging.kind, StreamKind::SecFiling) {
            return Err(IngestError::ProviderCaptureRequired);
        }
        self.finish_logical_stream(
            staging,
            reservation,
            binding,
            StreamTerminal::SecFiling(company_identity),
            precommit_authority,
            cancellation,
        )
        .await
    }

    /// Publishes an exact Tiingo history only after its opaque adapter proof and raw pack agree.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_tiingo_history_stream(
        &self,
        staging: ProviderLogicalStreamStaging,
        reservation: IngestReservation,
        binding: SealedProviderLogicalPublicationBinding,
        proof: ValidatedTiingoEodHistory,
        pack: ProviderCapturePackSeal,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), IngestError> {
        let StreamKind::TiingoHistory {
            descriptor_digest: expected,
        } = staging.kind
        else {
            return Err(IngestError::ProviderCaptureRequired);
        };
        if expected != descriptor_digest(&proof)? {
            return Err(IngestError::ProviderCaptureRequired);
        }
        self.finish_logical_stream(
            staging,
            reservation,
            binding,
            StreamTerminal::TiingoHistory { proof, pack },
            precommit_authority,
            cancellation,
        )
        .await
    }

    async fn finish_logical_stream(
        &self,
        staging: ProviderLogicalStreamStaging,
        reservation: IngestReservation,
        binding: SealedProviderLogicalPublicationBinding,
        terminal: StreamTerminal,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), IngestError> {
        precommit_authority.validate_precommit()?;
        let request = staging
            .request
            .as_ref()
            .ok_or(IngestError::ProviderCaptureRequired)?;
        if staging.poisoned
            || staging.catalog_id != self.catalog_id
            || staging.chunks.is_empty()
            || binding.terminal().source_id() != &staging.source_id
            || binding.terminal().total_canonical_rows() != staging.rows
            || binding.canonical_partitions().len() != staging.chunks.len()
            || !binding
                .canonical_partitions()
                .iter()
                .zip(&staging.chunks)
                .all(|(actual, chunk)| actual == &chunk.expectation)
            || binding.partitions().len()
                != staging.chunks.len() * 2
                    + usize::from(matches!(terminal, StreamTerminal::TiingoHistory { .. }))
        {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let (company_identity, market_bar_history, mut content) = match &terminal {
            StreamTerminal::SecFiling(company) => {
                if company.source_id() != &staging.source_id
                    || company.parent_ingest_payload_evidence() != request.object().evidence()
                {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                let capture = staging
                    .capture
                    .as_ref()
                    .ok_or(IngestError::ProviderCaptureRequired)?;
                let payloads = binding
                    .objects()
                    .iter()
                    .filter(|object| object.role() == LogicalObjectRole::ProviderPayload)
                    .collect::<Vec<_>>();
                if payloads.len() != capture.capture().pages().len()
                    || !payloads
                        .iter()
                        .zip(capture.capture().pages())
                        .all(|(object, page)| {
                            object.semantic_identity() == capture.receipt_digest()
                                && object.object().content_digest() == page.body_digest()
                                && object.object().size_bytes() == page.body_bytes()
                        })
                {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                let accumulator = ExtractionContentAccumulator::try_new(
                    request,
                    usize::try_from(staging.rows).map_err(|_| IngestError::InvalidDataset)?,
                )
                .map_err(IngestError::ContentIdentity)?;
                (Some(company), None, Some(accumulator))
            }
            StreamTerminal::TiingoHistory { proof, pack } => {
                validate_tiingo_stream(&staging, &binding, proof, pack, &cancellation)?;
                let candidate =
                    MarketBarHistoryPublicationCandidate::try_from_tiingo_logical(proof, &binding)?;
                (None, Some(candidate), None)
            }
        };
        for chunk in &staging.chunks {
            for (family, schema, digest) in [
                (
                    LogicalPartitionFamily::ProviderNative,
                    chunk.native_schema,
                    chunk.native_object_digest,
                ),
                (
                    LogicalPartitionFamily::CanonicalRowMap,
                    chunk.row_map_schema,
                    chunk.row_map_object_digest,
                ),
            ] {
                let partition = binding
                    .partitions()
                    .iter()
                    .find(|partition| {
                        partition.family() == family
                            && partition.partition_ordinal() == chunk.ordinal as u32
                    })
                    .ok_or(IngestError::ProviderCaptureRequired)?;
                if partition.item_range() != chunk.expectation.row_range()
                    || partition.schema_identity() != schema
                    || partition.object().content_digest() != digest
                {
                    return Err(IngestError::ProviderCaptureRequired);
                }
            }
        }
        let digest = binding.binding_digest();
        let dataset_name = SourceIdentifier::try_from(staging.dataset.as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| IngestError::InvalidDataset)?;
        self.manifests
            .validate_append_schema(&staging.dataset, &schema)?;
        let operation = self
            .operation_gate
            .acquire(&cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        {
            let authority = self.lock_authority()?;
            let run =
                self.validate_run(&authority, &reservation, digest, Some(&staging.source_id))?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if run.state() == IngestRunState::Succeeded {
                let committed = self.reconcile_succeeded_provider_logical_fund_run(
                    &authority,
                    &reservation,
                    &staging.dataset,
                    digest,
                )?;
                authority
                    .validate_provider_company_identity_replay(&reservation, company_identity)?;
                if !self.manifests.market_bar_history_candidate_matches(
                    committed.manifest(),
                    market_bar_history.as_ref(),
                )? {
                    return Err(IngestError::ReplayConflict);
                }
                return Ok((committed, digest));
            }
        }
        let publication = self.objects.begin_publication(&cancellation).await?;
        let mut writer = None;
        let mut lineage = ResearchLineageDigestAccumulator::new();
        let mut coordinates = Vec::with_capacity(staging.chunks.len());
        let supervisor = BlockingIoSupervisor::new(cancellation.child_token());
        let _cancel_on_drop = CancelOnDrop(supervisor.clone());
        for chunk in &staging.chunks {
            if cancellation.is_cancelled() {
                return Err(IngestError::Cancelled);
            }
            let directory = Arc::clone(&staging.directory);
            let ordinal = chunk.ordinal;
            let bytes = chunk.bytes;
            let expected_digest = chunk.digest;
            let token = supervisor.cancellation().clone();
            let worker = supervisor
                .spawn_blocking(move || {
                    read_chunk(&directory, ordinal, bytes, expected_digest, &token)
                })
                .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
            let values = worker
                .await
                .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)??;
            if let Some(content) = content.as_mut() {
                for record in values.batch.records() {
                    content.push(record).map_err(IngestError::ContentIdentity)?;
                }
            }
            let converted = ResearchArrowBatch::try_from_extraction_batch_with_assigned_revisions_and_logical_binding(
                &values.batch, &values.revisions, &binding, ordinal, &values.native_digests, dataset_name.clone())?;
            drop(values);
            lineage.append(&converted)?;
            if writer.is_none() {
                let opened = self
                    .objects
                    .begin_dataset_writer_under_lease(
                        converted.record_batch().schema(),
                        WRITER_MEMORY_BYTES,
                        &cancellation,
                        &publication,
                    )
                    .await?;
                writer = Some(opened);
            }
            writer
                .as_mut()
                .ok_or(IngestError::InvalidDataset)?
                .write_dataset_batch(&converted.dataset_batch())
                .await?;
            coordinates.push(ProviderArtifactInputCoordinate::try_new(0, ordinal)?);
        }
        if let Some(content) = content {
            if content
                .finish()
                .map_err(IngestError::ContentIdentity)?
                .digest()
                != binding.terminal().provider_terminal_evidence_digest()
            {
                return Err(IngestError::ProviderCaptureRequired);
            }
        }
        let authorization = company_identity
            .map(|company| -> Result<_, IngestError> {
                Ok(LogicalCompanyIdentityAuthorization {
                    binding_digest: digest,
                    source_id: staging.source_id.clone(),
                    parent_digest: request.object().evidence().content_digest(),
                    observation_digest: EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(serde_json::to_vec(company)?).into(),
                    ),
                })
            })
            .transpose()?;
        let staged = writer.ok_or(IngestError::InvalidDataset)?.finish().await?;
        let objects = Arc::clone(&self.objects);
        let worker = supervisor
            .spawn_blocking(move || {
                let published = objects.finalize_staged_under_lease(staged, &publication);
                (published, publication, operation)
            })
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        let (published, _publication, _operation) = worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        let published = published?;
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        if published.row_count() != staging.rows {
            return Err(IngestError::ProviderCaptureRequired);
        }
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            Sha256Digest::new(lineage.finish().bytes()),
        )?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(&authority, &reservation, digest, Some(&staging.source_id))?;
        let plan = self
            .manifests
            .preview_append(staging.dataset, &schema, vec![object])?;
        let commit_deadline = Instant::now() + REVISION_ASSIGNMENT_DEADLINE;
        let source_evidence = match authorization.as_ref() {
            Some(authorization) => PublicationSourceEvidence::ProviderLogicalWithCompanyIdentity(
                &binding,
                &coordinates,
                authorization,
            ),
            None => {
                let StreamTerminal::TiingoHistory { pack, .. } = &terminal else {
                    return Err(IngestError::ProviderCaptureRequired);
                };
                PublicationSourceEvidence::ProviderLogicalOriginalCaptures(
                    &binding,
                    &coordinates,
                    pack,
                    commit_deadline,
                    &cancellation,
                )
            }
        };
        let committed = self.commit_plan(
            &authority,
            &reservation,
            &run,
            dataset_name,
            schema,
            plan,
            std::slice::from_ref(&published),
            GenerationKind::Ingest,
            Some(precommit_authority.as_ref()),
            company_identity,
            market_bar_history.as_ref(),
            None,
            source_evidence,
        )?;
        Ok((committed, digest))
    }
}

fn io_error(error: std::io::Error) -> IngestError {
    IngestError::Parquet(ParquetStoreError::Io(error))
}
struct BoundedChunkWriter<W> {
    writer: W,
    bytes: u64,
    cancellation: CancellationToken,
}
impl<W: Write> Write for BoundedChunkWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::other("cancelled"));
        }
        if self.bytes.saturating_add(bytes.len() as u64) > MAX_CHUNK_BYTES {
            return Err(std::io::Error::other(
                "canonical staging chunk exceeds disk bound",
            ));
        }
        let written = self.writer.write(bytes)?;
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
fn write_chunk(
    directory: &OperationScratchDirectory,
    ordinal: usize,
    values: &ChunkValues,
    cancellation: &CancellationToken,
) -> Result<(u64, [u8; 32]), IngestError> {
    let path = directory.path().join(format!("canonical-{ordinal}.json"));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(io_error)?;
    let mut output = BoundedChunkWriter {
        writer: BufWriter::new(file),
        bytes: 0,
        cancellation: cancellation.clone(),
    };
    serde_json::to_writer(&mut output, values)?;
    output.flush().map_err(io_error)?;
    let bytes = output.bytes;
    drop(output);
    let mut file = File::open(path).map_err(io_error)?;
    Ok((bytes, hash_file(&mut file, cancellation)?))
}
fn hash_file(file: &mut File, cancellation: &CancellationToken) -> Result<[u8; 32], IngestError> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hash.finalize().into())
}
fn read_chunk(
    directory: &OperationScratchDirectory,
    ordinal: usize,
    bytes: u64,
    digest: [u8; 32],
    cancellation: &CancellationToken,
) -> Result<ChunkValues, IngestError> {
    use std::io::{Seek as _, SeekFrom};
    let mut file =
        File::open(directory.path().join(format!("canonical-{ordinal}.json"))).map_err(io_error)?;
    if bytes > MAX_CHUNK_BYTES
        || file.metadata().map_err(io_error)?.len() != bytes
        || hash_file(&mut file, cancellation)? != digest
    {
        return Err(IngestError::ProviderCaptureRequired);
    }
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    let result = serde_json::from_reader(BufReader::new(file))?;
    if cancellation.is_cancelled() {
        return Err(IngestError::Cancelled);
    }
    Ok(result)
}

fn hash_frame(hash: &mut Sha256, ordinal: u64, bytes: &[u8]) {
    hash.update(ordinal.to_le_bytes());
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}

fn row_map_schema(sec_filing: bool) -> EvidenceDigest {
    let domain: &[u8] = if sec_filing {
        b"market-squawk/sec-filing/logical-row-map/v1"
    } else {
        b"market-squawk/tiingo-history/logical-row-map/v1"
    };
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(domain).into())
}
fn descriptor_digest(proof: &ValidatedTiingoEodHistory) -> Result<EvidenceDigest, IngestError> {
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(serde_json::to_vec(proof.descriptor())?).into(),
    ))
}
struct IndexDigestWriter {
    hash: Sha256,
    bytes: u64,
}
impl IndexDigestWriter {
    fn new() -> Self {
        Self {
            hash: Sha256::new(),
            bytes: 0,
        }
    }
    fn identity(self) -> (EvidenceDigest, u64) {
        (
            EvidenceDigest::new(DigestAlgorithm::Sha256, self.hash.finalize().into()),
            self.bytes,
        )
    }
}
impl Write for IndexDigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("index byte overflow"))?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn validate_tiingo_stream(
    staging: &ProviderLogicalStreamStaging,
    binding: &SealedProviderLogicalPublicationBinding,
    proof: &ValidatedTiingoEodHistory,
    pack: &ProviderCapturePackSeal,
    cancellation: &CancellationToken,
) -> Result<(), IngestError> {
    let invalid = || IngestError::ProviderCaptureRequired;
    let descriptor = serde_json::to_vec(proof.descriptor())?;
    let descriptor_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&descriptor).into());
    if !proof.publication_authorized()
        || !proof.is_exhausted()
        || proof.chunk_count() != staging.chunks.len()
        || proof.total_canonical_rows() != staging.rows
        || binding.terminal().provider_terminal_evidence_digest() != descriptor_digest
        || binding.terminal().total_decoded_events() != proof.descriptor().session_count() as u64
        || binding.terminal().execution_attempt_digest() != Some(pack.captures_digest())
        || binding.terminal().source_revision_digest()
            != proof
                .descriptor()
                .normalization()
                .source_contract_evidence
                .content_digest()
        || binding.objects().len() != 5
        || pack.logical_ordinal() != 0
        || pack.source_id() != &staging.source_id
        || pack.capture_count() != proof.page_count() as u64 + 1
        || pack.body_count() != pack.capture_count()
        || pack.captures_digest() != proof.capture_pack_identity()
    {
        return Err(invalid());
    }
    let payload = &binding.objects()[0];
    if payload.role() != LogicalObjectRole::ProviderPayload
        || payload.object() != pack.object()
        || payload.semantic_identity() != pack.captures_digest()
    {
        return Err(invalid());
    }
    let catalog = &binding.objects()[1];
    if catalog.role() != LogicalObjectRole::Catalog
        || catalog.semantic_identity() != descriptor_digest
        || catalog.object().content_digest() != descriptor_digest
        || catalog.object().size_bytes() != descriptor.len() as u64
    {
        return Err(invalid());
    }
    let session_partition = binding
        .partitions()
        .iter()
        .find(|partition| partition.family() == LogicalPartitionFamily::DecodedEvent)
        .ok_or_else(invalid)?;
    let session_schema = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(b"market-squawk/tiingo-history/session-index/v1").into(),
    );
    if session_partition.partition_ordinal() != 0
        || session_partition.item_range().first_ordinal() != 0
        || u64::from(session_partition.item_range().item_count().get())
            != proof.descriptor().session_count() as u64
        || session_partition.schema_identity() != session_schema
        || session_partition.object() != binding.objects()[3].object()
    {
        return Err(invalid());
    }
    for (index, chunk) in staging.chunks.iter().enumerate() {
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let expected = proof
            .canonical_chunk_at(index)
            .map_err(|_| invalid())?
            .ok_or_else(invalid)?;
        if expected.global_start() != chunk.expectation.row_range().first_ordinal()
            || expected.row_count() as u64
                != u64::from(chunk.expectation.row_range().item_count().get())
            || expected.extraction_content_digest() != chunk.expectation.semantic_digest()
            || expected.native_batch_digest() != chunk.native_batch_digest
            || Some(expected.original_page_ordinal()) != chunk.original_page_ordinal
        {
            return Err(invalid());
        }
    }
    for ordinal in 2..=4 {
        if cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        let mut writer = IndexDigestWriter::new();
        match ordinal {
            2 => proof.write_page_index(&mut writer, cancellation),
            3 => proof.write_session_index(&mut writer, cancellation),
            _ => proof.write_action_index(&mut writer, cancellation),
        }
        .map_err(|_| invalid())?;
        let (digest, bytes) = writer.identity();
        let object = &binding.objects()[ordinal];
        if object.role() != LogicalObjectRole::ProviderComponent
            || object.semantic_identity() != digest
            || object.object().content_digest() != digest
            || object.object().size_bytes() != bytes
        {
            return Err(invalid());
        }
    }
    Ok(())
}
