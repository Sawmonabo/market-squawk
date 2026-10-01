//! Bounded maintenance on the existing retained I/O and publication owners.

use super::*;
use crate::DatasetSchemaRegistry;
use crate::catalog::market_event_store::{
    archive::plan_market_event_archive, load_market_event_rows,
};

impl AnalyticalDataService {
    /// Visits one dataset and archives one bounded prefix without delaying canonical ingest ACKs.
    /// The caller retains the maintenance future and supplies its original shutdown/deadline.
    pub async fn maintain_market_event_archive(
        &self,
        after_dataset: Option<&DatasetId>,
        limits: MarketEventArchiveLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketEventArchiveTurn, IngestError> {
        check_market_event_read(deadline, &cancellation)?;
        let operation = cancellation.child_token();
        let _cancel_on_drop = operation.clone().drop_guard();
        let supervisor = BlockingIoSupervisor::new(operation.clone());
        let outcome = {
            let turn = self.market_event_archive_turn(
                after_dataset,
                limits,
                deadline,
                &operation,
                &supervisor,
            );
            tokio::pin!(turn);
            tokio::select! { biased;
                () = cancellation.cancelled() => Err(IngestError::Cancelled),
                () = tokio::time::sleep_until(deadline.into()) => Err(IngestError::DeadlineExceeded),
                result = &mut turn => result,
            }
        };
        // Drop the turn, writer and any buffered worker results before joining. Cancellation
        // can leave encoding/SQL work in the retained reaper; it still owns root authority.
        supervisor.cancel();
        supervisor.wait_idle().await;
        outcome
    }

    async fn market_event_archive_turn(
        &self,
        after_dataset: Option<&DatasetId>,
        limits: MarketEventArchiveLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
        supervisor: &BlockingIoSupervisor,
    ) -> Result<MarketEventArchiveTurn, IngestError> {
        let manifests = Arc::clone(&self.manifests);
        let after = after_dataset.cloned();
        let read_limits = self.catalog_read_limits;
        let token = supervisor.cancellation().clone();
        let permit = self.objects.acquire_blocking_permit(cancellation).await?;
        let worker = supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                let snapshot = manifests.read_snapshot(read_limits, deadline, &token)?;
                snapshot.read(|snapshot| {
                    plan_market_event_archive(
                        snapshot.connection(),
                        after.as_ref(),
                        limits.maximum_publications,
                        limits.target_bytes,
                        deadline,
                        &token,
                    )
                })
            })
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        let plan = worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)??;
        let Some(plan) = plan else {
            return Ok(MarketEventArchiveTurn {
                next_dataset: None,
                archived_publications: 0,
                archived_rows: 0,
            });
        };
        if plan.commits.is_empty() {
            return Ok(MarketEventArchiveTurn {
                next_dataset: Some(plan.dataset),
                archived_publications: 0,
                archived_rows: 0,
            });
        }
        let schema = DatasetSchemaRegistry::local()
            .resolve(plan.commits[0].schema())
            .map_err(|_| IngestError::InvalidDataset)?;
        let mut writer = self
            .objects
            .begin_archive_writer(Arc::clone(&schema), limits.working_bytes, supervisor)
            .await?;
        for commit in &plan.commits {
            check_market_event_read(deadline, cancellation)?;
            let manifests = Arc::clone(&self.manifests);
            let objects = Arc::clone(&self.objects);
            let commit = commit.clone();
            let token = supervisor.cancellation().clone();
            let schema = Arc::clone(&schema);
            let permit = self.objects.acquire_blocking_permit(cancellation).await?;
            let worker = supervisor.spawn_blocking(move || -> Result<_, IngestError> {
                let _permit = permit;
                let snapshot = manifests.read_snapshot(read_limits, deadline, &token)?;
                snapshot.read(|snapshot| -> Result<_,IngestError> {
                    let rows = load_market_event_rows(snapshot.connection(), &commit, &objects, read_limits, deadline, &token)?;
                    let evidence = snapshot.publication_evidence(commit.publication_digest())?
                        .ok_or(IngestError::ProviderCaptureRequired)?;
                    evidence.verify_integrity()?;
                    let batch = ProviderMarketEventArrowBatch::try_from_canonical_json_with_publication_evidence(
                        rows, &evidence, MAX_EVENT_PUBLICATION_READ_BYTES)?;
                    let lineage:Vec<u8> = snapshot.connection().query_row(
                        "SELECT lineage_digest FROM market_event_commits WHERE dataset_id=?1 AND commit_sequence=?2",
                        rusqlite::params![commit.dataset_id().as_str(),i64::try_from(commit.sequence()).map_err(|_|IngestError::InvalidDataset)?],
                        |row|row.get(0)).map_err(CatalogError::from)?;
                    if batch.schema_ref()!=commit.schema() || batch.events().len() as u64!=commit.row_count()
                        || lineage!=batch.lineage_digest()?.bytes() {return Err(IngestError::ProviderCaptureRequired)}
                    snapshot.validate_event_metadata(batch.events(),&evidence)?;
                    // Publication-specific metadata stays in the immutable catalog. Archive fields
                    // keep the existing registered types and exact canonical JSON for every variant.
                    arrow::record_batch::RecordBatch::try_new(schema,batch.dataset_batch().record_batch().columns().to_vec())
                        .map_err(|_|IngestError::ProviderCaptureRequired)
                })
            }).map_err(|_|IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
            let batch = worker
                .await
                .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)??;
            let payloads = batch
                .column_by_name("event_json")
                .and_then(|column| column.as_any().downcast_ref::<arrow::array::BinaryArray>())
                .ok_or(IngestError::ProviderCaptureRequired)?;
            // Compact slices own only their selected buffers. A zero-copy Arrow slice would
            // still charge the entire original publication to the encoder's working budget.
            let chunk_bytes = (limits.working_bytes / 128).max(1);
            let mut first = 0usize;
            while first < batch.num_rows() {
                check_market_event_read(deadline, cancellation)?;
                let mut end = first;
                let mut bytes = 0usize;
                while end < batch.num_rows() && bytes < chunk_bytes {
                    bytes = bytes
                        .checked_add(payloads.value(end).len())
                        .ok_or(IngestError::InvalidDataset)?;
                    end += 1;
                }
                let indices = (first..end)
                    .map(|row| u32::try_from(row).map_err(|_| IngestError::InvalidDataset))
                    .collect::<Result<Vec<_>, _>>()?;
                let compact = arrow::compute::take_record_batch(
                    &batch,
                    &arrow::array::UInt32Array::from(indices),
                )
                .map_err(|_| IngestError::ProviderCaptureRequired)?;
                writer.write_batch(compact).await?;
                first = end;
            }
        }
        let staged = writer.finish().await?;
        check_market_event_read(deadline, cancellation)?;
        // Encoding owns only scratch. Ordinary gate -> publication ordering begins here.
        let operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        let publication = self.objects.begin_publication(cancellation).await?;
        let authority = Arc::clone(&self.authority);
        let objects = Arc::clone(&self.objects);
        let token = supervisor.cancellation().clone();
        let permit = self.objects.acquire_blocking_permit(cancellation).await?;
        let worker = supervisor
            .spawn_blocking(move || -> Result<_, IngestError> {
                let (_operation, _permit) = (operation, permit);
                check_market_event_read(deadline, &token)?;
                let object = objects.finalize_staged_under_lease(staged, &publication)?;
                check_market_event_read(deadline, &token)?;
                let authority = authority
                    .lock()
                    .map_err(|_| IngestError::AuthorityLockPoisoned)?;
                authority
                    .catalog()
                    .commit_market_event_archive(&plan, &object, deadline, &token)?;
                drop(authority);
                drop(publication);
                Ok(MarketEventArchiveTurn {
                    next_dataset: Some(plan.dataset),
                    archived_publications: plan.commits.len() as u64,
                    archived_rows: plan.rows,
                })
            })
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?
    }
}
