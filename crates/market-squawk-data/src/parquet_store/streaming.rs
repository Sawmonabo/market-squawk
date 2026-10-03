//! Incremental staging under the existing immutable-publication authority.

use super::cursor::cursor_admission_error;
use super::*;
use datafusion::execution::memory_pool::MemoryReservation;

/// A single controlled staged object. Input batches are released after encoding; the writer
/// retains at most one configured row group plus the complete file footer metadata.
pub(crate) struct StreamingParquetWriter {
    state: Option<WriterState>,
    supervisor: BlockingIoSupervisor,
    cancellation: CancellationToken,
    blocking_tasks: Arc<Semaphore>,
}

struct WriterState {
    store: ParquetObjectStore,
    writer: ArrowWriter<File>,
    cleanup: OwnedStagingCleanup,
    scratch: Option<OperationScratchDirectory>,
    rows: u64,
    schema: SchemaRef,
    active_limit: usize,
    page_bytes: usize,
    row_group_bytes: usize,
    metadata_bytes: usize,
    memory: Option<QueryArtifactMemoryLease>,
    memory_limit: u64,
    max_output_bytes: u64,
    #[cfg(test)]
    barrier: Option<crate::ingest::QueryArtifactWriterWorkerBarrier>,
}

impl ParquetObjectStore {
    /// Opens an incremental writer for one registered dataset schema under the existing lease.
    pub(crate) async fn begin_dataset_writer_under_lease(
        &self,
        schema: SchemaRef,
        working_bytes: usize,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
    ) -> Result<StreamingParquetWriter, ParquetStoreError> {
        if working_bytes == 0 {
            return Err(ParquetStoreError::InvalidConfiguration);
        }
        self.begin_streaming_writer(
            schema,
            cancellation,
            lease,
            None,
            u64::try_from(working_bytes).map_err(|_| ParquetStoreError::SizeOverflow)?,
            self.config.max_staging_bytes,
            #[cfg(test)]
            None,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "independent publication and resource authorities"
    )]
    pub(crate) async fn begin_streaming_writer(
        &self,
        schema: SchemaRef,
        cancellation: &CancellationToken,
        lease: &PublicationLease,
        memory: Option<QueryArtifactMemoryLease>,
        memory_limit: u64,
        max_output_bytes: u64,
        #[cfg(test)] barrier: Option<crate::ingest::QueryArtifactWriterWorkerBarrier>,
    ) -> Result<StreamingParquetWriter, ParquetStoreError> {
        if !self.authority.publication.owns(lease) {
            return Err(ParquetStoreError::InvalidPublicationLease);
        }
        self.open_streaming_writer(
            schema,
            BlockingIoSupervisor::new(cancellation.child_token()),
            memory,
            memory_limit,
            max_output_bytes,
            None,
            #[cfg(test)]
            barrier,
        )
    }

    /// Stages bounded archival work in operation-owned storage, outside publication admission.
    pub(crate) async fn begin_archive_writer(
        &self,
        schema: SchemaRef,
        working_bytes: usize,
        supervisor: &BlockingIoSupervisor,
    ) -> Result<StreamingParquetWriter, ParquetStoreError> {
        if working_bytes == 0 {
            return Err(ParquetStoreError::InvalidConfiguration);
        }
        self.open_streaming_writer(
            schema,
            supervisor.clone(),
            None,
            u64::try_from(working_bytes).map_err(|_| ParquetStoreError::SizeOverflow)?,
            self.config.max_staging_bytes,
            Some(self.operation_scratch()?),
            #[cfg(test)]
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "existing writer resource authorities and owned scratch"
    )]
    fn open_streaming_writer(
        &self,
        schema: SchemaRef,
        supervisor: BlockingIoSupervisor,
        memory: Option<QueryArtifactMemoryLease>,
        memory_limit: u64,
        max_output_bytes: u64,
        scratch: Option<OperationScratchDirectory>,
        #[cfg(test)] barrier: Option<crate::ingest::QueryArtifactWriterWorkerBarrier>,
    ) -> Result<StreamingParquetWriter, ParquetStoreError> {
        let cancellation = supervisor.cancellation().child_token();
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        let store = Self {
            root: self.root.clone(),
            directory: self.directory.try_clone()?,
            config: self.config,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            authority: Arc::clone(&self.authority),
        };
        let stage = match &scratch {
            Some(scratch) => {
                let relative = scratch
                    .path()
                    .strip_prefix(self.root.root())
                    .map_err(|_| ParquetStoreError::InvalidStagedObject)?;
                let relative = relative
                    .to_str()
                    .ok_or(ParquetStoreError::InvalidStagedObject)?;
                format!("{relative}/{}.tmp", Uuid::new_v4())
            }
            None => format!("{STAGING}/{}.tmp", Uuid::new_v4()),
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        configure_private_staging(&mut options);
        let file = store.directory.open_with(&stage, &options)?.into_std();
        let cleanup = OwnedStagingCleanup {
            directory: store.directory.try_clone()?,
            reference: Some(stage),
        };
        // Size the actual encoder pages from its existing working allocation. A wide
        // schema must not reserve a fixed 64 KiB page for every column even for tiny batches.
        let page_bytes = usize::try_from(memory_limit / 8)
            .unwrap_or(usize::MAX)
            .checked_div(schema.fields().len().max(1))
            .ok_or(ParquetStoreError::SizeOverflow)?
            .clamp(1, QUERY_WRITER_PAGE_BYTES);
        // Use the existing budget-derived byte target for every streaming writer. Waiting
        // only for a row-count boundary can leave no room for the next bounded input batch
        // beside the accumulated encoder and its flush workspace.
        let row_group_bytes = usize::try_from(memory_limit / 16)
            .map_err(|_| ParquetStoreError::SizeOverflow)?
            .max(1);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(self.config.max_row_group_rows))
            .set_max_row_group_bytes(Some(row_group_bytes))
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_write_page_header_statistics(false)
            .set_data_page_size_limit(page_bytes)
            .set_write_batch_size((page_bytes / size_of::<u64>()).clamp(1, 1024));
        let writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties.build()))?;
        Ok(StreamingParquetWriter {
            state: Some(WriterState {
                store,
                writer,
                cleanup,
                scratch,
                rows: 0,
                schema,
                active_limit: 0,
                page_bytes,
                row_group_bytes,
                metadata_bytes: 0,
                memory,
                memory_limit,
                max_output_bytes: max_output_bytes.min(self.config.max_staging_bytes),
                #[cfg(test)]
                barrier,
            }),
            supervisor,
            cancellation,
            blocking_tasks: Arc::clone(&self.blocking_tasks),
        })
    }
}

impl StreamingParquetWriter {
    /// Writes only schema-validated dataset batches, retaining no earlier Arrow batches.
    pub(crate) async fn write_dataset_batch(
        &mut self,
        batch: &DatasetArrowBatch,
    ) -> Result<(), ParquetStoreError> {
        self.write_batch(batch.record_batch().clone()).await
    }

    pub(crate) async fn write_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<(), ParquetStoreError> {
        self.write_owned_batch(batch, None).await
    }

    pub(crate) async fn write_query_batch(
        &mut self,
        batch: RecordBatch,
        input: MemoryReservation,
    ) -> Result<(), ParquetStoreError> {
        self.write_owned_batch(batch, Some(input)).await
    }

    async fn write_owned_batch(
        &mut self,
        batch: RecordBatch,
        input: Option<MemoryReservation>,
    ) -> Result<(), ParquetStoreError> {
        let mut state = self
            .state
            .take()
            .ok_or(ParquetStoreError::InvalidStagedObject)?;
        let cancellation = self.cancellation.clone();
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            permit = Arc::clone(&self.blocking_tasks).acquire_owned() => permit.map_err(|_| ParquetStoreError::BlockingTaskFailed)?,
        };
        // Reservation ownership moves with the writer into the supervised worker, including
        // after the caller drops a timed-out future. No detached unaccounted encoder survives.
        let flush = state.admit_or_flush(&batch, input.is_some())?;
        let worker_cancel = cancellation.clone();
        let mut worker = self
            .supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                #[cfg(test)]
                if let Some(barrier) = state.barrier.take() {
                    barrier.wait();
                }
                if flush {
                    if worker_cancel.is_cancelled() {
                        return Err(ParquetStoreError::Cancelled);
                    }
                    state.writer.flush()?;
                    if u64::try_from(state.writer.bytes_written())
                        .map_err(|_| ParquetStoreError::SizeOverflow)?
                        > state.max_output_bytes
                    {
                        return Err(ParquetStoreError::StagingLimitExceeded);
                    }
                    // Account the actual flushed footer before admitting the new group.
                    state.admit(&batch, input.is_some())?;
                }
                state.write(&batch, &worker_cancel)?;
                drop(batch);
                drop(input);
                if let Some(memory) = &state.memory {
                    let retained = state
                        .metadata_bytes
                        .checked_add(state.writer.memory_size())
                        .ok_or(ParquetStoreError::SizeOverflow)?;
                    memory.resize(retained, state.memory_limit)?;
                }
                Ok::<_, ParquetStoreError>(state)
            })
            .map_err(cursor_admission_error)?;
        self.state = Some(tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            result = &mut worker => result.map_err(|_| ParquetStoreError::BlockingTaskFailed)??,
        });
        Ok(())
    }

    /// Seals, syncs and hashes the complete file; it remains outside the final namespace.
    pub(crate) async fn finish(mut self) -> Result<StagedObject, ParquetStoreError> {
        let state = self
            .state
            .take()
            .ok_or(ParquetStoreError::InvalidStagedObject)?;
        let cancellation = self.cancellation.clone();
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            permit = Arc::clone(&self.blocking_tasks).acquire_owned() => permit.map_err(|_| ParquetStoreError::BlockingTaskFailed)?,
        };
        let worker_cancel = cancellation.clone();
        let mut worker = self
            .supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                state.finish(&worker_cancel)
            })
            .map_err(cursor_admission_error)?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ParquetStoreError::Cancelled),
            result = &mut worker => result.map_err(|_| ParquetStoreError::BlockingTaskFailed)?,
        }
    }
}

impl Drop for StreamingParquetWriter {
    fn drop(&mut self) {
        // The archive supervisor also owns subsequent catalog publication. Dropping a finished
        // writer cancels only its encoder, not the enclosing operation's remaining workers.
        self.cancellation.cancel();
    }
}

impl WriterState {
    /// Admit encoding, or only the flush of a previously admitted nonempty group.
    /// The caller performs that I/O on its supervised worker and then repeats admission.
    fn admit_or_flush(
        &mut self,
        batch: &RecordBatch,
        input_reserved: bool,
    ) -> Result<bool, ParquetStoreError> {
        match self.admit(batch, input_reserved) {
            Ok(()) => return Ok(false),
            Err(ParquetStoreError::WriterMemoryLimitExceeded { .. })
                if self.writer.in_progress_rows() > 0 => {}
            Err(error) => return Err(error),
        }
        // Retain the previous group's already-admitted encoding/flush workspace, including
        // its footer, while the current input remains owned by the same worker. A separately
        // reserved query input stays charged to its original reservation until it is dropped.
        let working = self
            .active_limit
            .checked_add(self.metadata_bytes)
            .and_then(|bytes| {
                bytes.checked_add(if input_reserved {
                    0
                } else {
                    batch.get_array_memory_size()
                })
            })
            .ok_or(ParquetStoreError::SizeOverflow)?;
        if u64::try_from(working).map_err(|_| ParquetStoreError::SizeOverflow)? > self.memory_limit
        {
            self.report_memory_rejection(
                "flush_admission",
                working,
                batch.get_array_memory_size(),
                self.metadata_bytes,
                self.active_limit,
            );
            return Err(ParquetStoreError::WriterMemoryLimitExceeded {
                limit: self.memory_limit,
            });
        }
        if let Some(memory) = &self.memory {
            memory.resize(working, self.memory_limit)?;
        }
        Ok(true)
    }

    fn admit(
        &mut self,
        batch: &RecordBatch,
        input_reserved: bool,
    ) -> Result<(), ParquetStoreError> {
        if batch.schema() != self.schema {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let admission = self
            .store
            .query_artifact_writer_admission(batch, self.page_bytes)?;
        let per_group = batch
            .num_columns()
            .checked_mul(QUERY_WRITER_COLUMN_METADATA)
            .and_then(|bytes| bytes.checked_add(QUERY_WRITER_ROW_GROUP_METADATA))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        // ArrowWriter combines small input batches into its configured row-group bound. Its
        // current group can produce one more footer entry than the incoming batch alone.
        let retained_groups = self
            .writer
            .flushed_row_groups()
            .len()
            .checked_add(usize::from(self.writer.in_progress_rows() > 0))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        // Byte-triggered groups can outnumber the row-count groups in the original
        // admission. Parquet 58.3 flushes at the byte target, or when average-row rounding
        // leaves no row that fits. In that early case current_bytes > target / 2.
        // The existing uncompressed encoder bound plus the current encoded group therefore
        // bounds those extra groups by ceil(2 * encoded_bytes / target). A nonempty group
        // also consumes a row, so incoming row count is an independent upper bound. Existing
        // row-count/partial-group accounting below covers groups below the half-target.
        let byte_groups = self
            .writer
            .in_progress_size()
            .checked_add(admission.active_writer_bytes)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(self.row_group_bytes - 1))
            .map(|bytes| (bytes / self.row_group_bytes).min(batch.num_rows()))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let metadata_bytes = retained_groups
            .checked_add(byte_groups)
            .and_then(|groups| groups.checked_mul(per_group))
            .and_then(|bytes| bytes.checked_add(admission.metadata_bytes))
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let active_limit = admission
            .active_writer_bytes
            .checked_add(self.writer.memory_size())
            .ok_or(ParquetStoreError::SizeOverflow)?;
        let working = active_limit
            .checked_add(metadata_bytes)
            .and_then(|bytes| {
                bytes.checked_add(if input_reserved {
                    0
                } else {
                    batch.get_array_memory_size()
                })
            })
            .ok_or(ParquetStoreError::SizeOverflow)?;
        if u64::try_from(working).map_err(|_| ParquetStoreError::SizeOverflow)? > self.memory_limit
        {
            self.report_memory_rejection(
                "batch_admission",
                working,
                batch.get_array_memory_size(),
                metadata_bytes,
                active_limit,
            );
            return Err(ParquetStoreError::WriterMemoryLimitExceeded {
                limit: self.memory_limit,
            });
        }
        if let Some(memory) = &self.memory {
            memory.resize(working, self.memory_limit)?;
        }
        self.active_limit = active_limit;
        self.metadata_bytes = metadata_bytes;
        Ok(())
    }

    fn write(
        &mut self,
        batch: &RecordBatch,
        cancellation: &CancellationToken,
    ) -> Result<(), ParquetStoreError> {
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        // The maintained encoder flushes at max_row_group_rows. Flushing every input batch
        // would turn small query batches into an unbounded count of tiny footer entries.
        for offset in (0..batch.num_rows()).step_by(self.store.config.max_row_group_rows) {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            let rows = self
                .store
                .config
                .max_row_group_rows
                .min(batch.num_rows() - offset);
            self.writer.write(&batch.slice(offset, rows))?;
            if self.writer.memory_size() > self.active_limit {
                self.report_memory_rejection(
                    "encoded_writer_growth",
                    self.writer.memory_size(),
                    batch.get_array_memory_size(),
                    self.metadata_bytes,
                    self.active_limit,
                );
                return Err(ParquetStoreError::WriterMemoryLimitExceeded {
                    limit: self.memory_limit,
                });
            }
            if u64::try_from(self.writer.bytes_written())
                .map_err(|_| ParquetStoreError::SizeOverflow)?
                > self.max_output_bytes
            {
                return Err(ParquetStoreError::StagingLimitExceeded);
            }
        }
        self.rows = self
            .rows
            .checked_add(
                u64::try_from(batch.num_rows()).map_err(|_| ParquetStoreError::SizeOverflow)?,
            )
            .ok_or(ParquetStoreError::SizeOverflow)?;
        Ok(())
    }

    fn report_memory_rejection(
        &self,
        stage: &'static str,
        required_bytes: usize,
        input_bytes: usize,
        metadata_bytes: usize,
        active_bytes: usize,
    ) {
        // Only code-owned stage names and resource counters; never source payloads or paths.
        tracing::warn!(
            stage,
            limit_bytes = self.memory_limit,
            required_bytes,
            writer_bytes = self.writer.memory_size(),
            input_bytes,
            metadata_bytes,
            active_bytes,
            buffered_rows = self.writer.in_progress_rows(),
            flushed_groups = self.writer.flushed_row_groups().len(),
            "Parquet writer memory admission rejected"
        );
    }

    fn finish(self, cancellation: &CancellationToken) -> Result<StagedObject, ParquetStoreError> {
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        // The final partial row group still needs encoding workspace. Keep the lease in
        // this worker through flush, footer serialization, sync, and hashing.
        let working = self
            .active_limit
            .checked_add(self.metadata_bytes)
            .ok_or(ParquetStoreError::SizeOverflow)?;
        if let Some(memory) = &self.memory {
            memory.resize(working, self.memory_limit)?;
        }
        let Self {
            writer,
            cleanup,
            scratch,
            rows,
            memory: _memory,
            max_output_bytes,
            ..
        } = self;
        let mut file = writer.into_inner()?;
        file.sync_all()?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 || metadata.len() > max_output_bytes {
            return Err(ParquetStoreError::StagingLimitExceeded);
        }
        let content_hash = hash_file(&mut file, Some(cancellation))?;
        Ok(StagedObject {
            cleanup,
            _scratch: scratch,
            content_hash,
            size_bytes: metadata.len(),
            row_count: rows,
            created_at: timestamp_from_system_time(metadata.modified()?)?,
        })
    }
}
