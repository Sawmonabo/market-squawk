//! Streaming compaction of an exact retained canonical market-event generation.

use super::*;

impl AnalyticalDataService {
    /// Returns an exact market generation only when the next append requires compaction.
    pub fn market_event_compaction_request(
        &self,
        dataset: &DatasetId,
        additional_objects: usize,
    ) -> Result<Option<CompactionRequest>, IngestError> {
        Ok(self
            .manifests
            .market_event_compaction_source(dataset, additional_objects)?
            .map(CompactionRequest::new))
    }

    /// Streams one exact market-event generation into a single immutable replacement.
    ///
    /// The reservation must bind the exact [`CompactionRequest`]. Original publications and
    /// their raw/native evidence remain retained; compaction creates no new provider evidence.
    /// The caller's publication authority, deadline and cancellation govern the entire operation.
    pub async fn compact_provider_market_events(
        &self,
        reservation: IngestReservation,
        request: CompactionRequest,
        deadline: Instant,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<CommittedDataset, IngestError> {
        let operation_cancellation = cancellation.child_token();
        let _cancel_on_drop = operation_cancellation.clone().drop_guard();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(IngestError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => Err(IngestError::DeadlineExceeded),
            result = self.compact_provider_market_events_inner(
                &reservation,
                &request,
                deadline,
                &operation_cancellation,
                precommit_authority.as_ref(),
            ) => result,
        }
    }

    async fn compact_provider_market_events_inner(
        &self,
        reservation: &IngestReservation,
        request: &CompactionRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
        precommit_authority: &dyn IngestPrecommitAuthority,
    ) -> Result<CommittedDataset, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        precommit_authority.validate_precommit()?;
        let schema = crate::DatasetSchemaRegistry::local()
            .canonical_market_events()
            .map_err(ArrowConversionError::from)?;
        if request.source().schema() != &schema {
            return Err(IngestError::Arrow(
                ArrowConversionError::UnexpectedDatasetSchema,
            ));
        }
        let _operation = self
            .operation_gate
            .acquire(cancellation)
            .await
            .ok_or(IngestError::Cancelled)?;
        check_market_event_read(deadline, cancellation)?;
        let pinned = self.manifests.pinned(request.source())?;
        let source_id = self.manifests.source_id(request.source())?;
        {
            let authority = self.lock_authority()?;
            let run = self.validate_run(
                &authority,
                reservation,
                request.payload_digest(),
                Some(&source_id),
            )?;
            if run.state() == IngestRunState::Failed {
                return Err(IngestError::TerminalRun);
            }
            if run.state() == IngestRunState::Reserved
                && self
                    .manifests
                    .latest(request.source().dataset_id())?
                    .as_ref()
                    != Some(request.source())
            {
                return Err(IngestError::ReplayConflict);
            }
            precommit_authority.validate_catalog_precommit(&authority)?;
        }
        let dataset_name = SourceIdentifier::try_from(request.source().dataset_id().as_str())
            .map_err(|_| IngestError::InvalidDataset)?;
        let target_schema = crate::schema::market_event_compaction_schema(&dataset_name)
            .map_err(ArrowConversionError::from)?;
        let publication = self.objects.begin_publication(cancellation).await?;
        let mut cursor = self.objects.pinned_batch_cursor(
            &pinned,
            1024,
            MAX_EVENT_PUBLICATION_READ_BYTES,
            cancellation,
        )?;
        let mut writer = self
            .objects
            .begin_dataset_writer_under_lease(
                Arc::clone(&target_schema),
                MAX_EVENT_PUBLICATION_READ_BYTES,
                cancellation,
                &publication,
            )
            .await?;
        let mut rows = 0_u64;
        while let Some(batch) = cursor.next_batch().await? {
            check_market_event_read(deadline, cancellation)?;
            rows = rows
                .checked_add(
                    u64::try_from(batch.num_rows()).map_err(|_| IngestError::InvalidDataset)?,
                )
                .ok_or(IngestError::InvalidDataset)?;
            if rows > pinned.plan().row_count() || batch.schema().fields() != target_schema.fields()
            {
                return Err(IngestError::ReplayConflict);
            }
            // Each input's complete file hash and registered fields were verified by the cursor.
            // Rebind only object-level metadata; every canonical column and row stays unchanged.
            let normalized =
                RecordBatch::try_new(Arc::clone(&target_schema), batch.columns().to_vec())
                    .map_err(ArrowConversionError::from)?;
            writer.write_batch(normalized).await?;
        }
        if rows != pinned.plan().row_count() {
            return Err(IngestError::ReplayConflict);
        }
        drop(cursor);
        check_market_event_read(deadline, cancellation)?;
        let staged = writer.finish().await?;
        check_market_event_read(deadline, cancellation)?;
        precommit_authority.validate_precommit()?;
        let published = self
            .objects
            .finalize_staged_under_lease(staged, &publication)?;
        let object = ManifestObject::try_new(
            published.content_hash(),
            published.row_count(),
            published.size_bytes(),
            pinned.plan().lineage_digest(),
        )?;
        if object.row_count() != rows {
            return Err(IngestError::ReplayConflict);
        }
        check_market_event_read(deadline, cancellation)?;
        let authority = self.lock_authority()?;
        let run = self.validate_run(
            &authority,
            reservation,
            request.payload_digest(),
            Some(&source_id),
        )?;
        precommit_authority.validate_catalog_precommit(&authority)?;
        if let Some(existing) = self.reconcile_existing(
            &authority,
            reservation,
            run.state(),
            request.source().dataset_id(),
            &schema,
            &object,
            None,
            None,
            None,
        )? {
            return Ok(existing);
        }
        if self
            .manifests
            .latest(request.source().dataset_id())?
            .as_ref()
            != Some(request.source())
        {
            return Err(IngestError::ReplayConflict);
        }
        let plan = self
            .manifests
            .preview_compaction(request.source(), object)?;
        self.commit_plan(
            &authority,
            reservation,
            &run,
            dataset_name,
            schema,
            plan,
            std::slice::from_ref(&published),
            GenerationKind::Compaction,
            Some(precommit_authority),
            None,
            None,
            None,
            PublicationSourceEvidence::NoNewRawInput,
        )
    }
}
