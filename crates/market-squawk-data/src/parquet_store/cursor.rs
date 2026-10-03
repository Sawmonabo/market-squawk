//! Sequential verified reads retaining one decoder and one caller-owned batch.

use std::{fmt, time::Instant};

use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader},
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

#[path = "cursor/admission.rs"]
mod admission;
use admission::{admit_cursor_working_set, admit_offset_index};

use super::*;

/// A repeatable manifest selection consumed in bounded Arrow batches.
///
/// A successful `None` is the complete-input receipt: callers must exhaust the cursor before
/// publishing derived output. Dropping it cancels in-flight I/O and releases its immutable pin.
pub struct PinnedBatchCursor {
    state: Option<CursorState>,
    supervisor: BlockingIoSupervisor,
    blocking_tasks: Arc<Semaphore>,
    _authority: Arc<RootAuthority>,
    complete: bool,
    deadline: Option<tokio::time::Instant>,
    expires_at: Option<Timestamp>,
}

struct CursorState {
    directory: Dir,
    _pin: Option<PinnedDataset>,
    objects: Vec<CursorObject>,
    schema: Option<SchemaRef>,
    next_object: usize,
    reader: Option<ParquetRecordBatchReader>,
    reader_schema: Option<SchemaRef>,
    object_rows: u64,
    generation_rows: u64,
    expected_rows: u64,
    batch_rows: usize,
    max_batch_bytes: usize,
    start_row: usize,
    projection: Option<Vec<usize>>,
}

struct CursorObject {
    reference: String,
    digest: Sha256Digest,
    bytes: u64,
    rows: u64,
}

impl CursorObject {
    fn from_pinned(object: &PinnedManifestObject) -> Self {
        Self {
            reference: object.relative_reference().to_owned(),
            digest: object.object().content_hash(),
            bytes: object.object().size_bytes(),
            rows: object.object().row_count(),
        }
    }
}

impl fmt::Debug for PinnedBatchCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PinnedBatchCursor")
            .field("complete", &self.complete)
            .finish_non_exhaustive()
    }
}

impl ParquetObjectStore {
    /// Opens a lazy, exact-generation cursor. No object contents are retained at construction.
    pub fn pinned_batch_cursor(
        &self,
        dataset: &PinnedDataset,
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        self.make_batch_cursor(
            dataset,
            dataset.objects().to_vec(),
            dataset.plan().row_count(),
            batch_rows,
            max_batch_bytes,
            cancellation,
        )
    }

    /// Resumes a physical manifest cursor at an exact object ordinal and row offset.
    /// Earlier objects are skipped; the selected object is still fully digest-verified.
    #[allow(
        clippy::too_many_arguments,
        reason = "immutable position and independent reader bounds"
    )]
    pub fn pinned_batch_cursor_from(
        &self,
        dataset: &PinnedDataset,
        start_object: usize,
        start_row: u64,
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        let mut cursor =
            self.pinned_batch_cursor(dataset, batch_rows, max_batch_bytes, cancellation)?;
        let state = cursor
            .state
            .as_mut()
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        if start_object > state.objects.len()
            || (start_object == state.objects.len() && start_row != 0)
            || state
                .objects
                .get(start_object)
                .is_some_and(|object| start_row >= object.rows)
        {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        state.generation_rows =
            state.objects[..start_object]
                .iter()
                .try_fold(start_row, |rows, object| {
                    rows.checked_add(object.rows)
                        .ok_or(ParquetStoreError::SizeOverflow)
                })?;
        state.next_object = start_object;
        state.start_row =
            usize::try_from(start_row).map_err(|_| ParquetStoreError::SizeOverflow)?;
        Ok(cursor)
    }

    /// Resumes an immutable row occurrence while decoding only the named base columns.
    /// Object digests, full source schema and physical row counts remain verified.
    #[allow(
        clippy::too_many_arguments,
        reason = "immutable position, projection and reader bounds"
    )]
    pub fn pinned_batch_cursor_from_projection(
        &self,
        dataset: &PinnedDataset,
        start_object: usize,
        start_row: u64,
        columns: &[&str],
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        self.pinned_batch_cursor_from(
            dataset,
            start_object,
            start_row,
            batch_rows,
            max_batch_bytes,
            cancellation,
        )?
        .with_projection(columns)
    }

    /// Opens only the exact artifact and ordinal selected by a retained immutable manifest.
    pub fn pinned_object_batch_cursor(
        &self,
        dataset: &PinnedDataset,
        artifact_id: Uuid,
        ordinal: usize,
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        let object = dataset
            .objects()
            .get(ordinal)
            .filter(|object| object.artifact_id() == artifact_id)
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        self.make_batch_cursor(
            dataset,
            vec![object.clone()],
            object.object().row_count(),
            batch_rows,
            max_batch_bytes,
            cancellation,
        )
    }

    /// Projects columns from one exact artifact while verifying the complete object first.
    /// Physical row ordinals, full source schema, content digest and EOF counts are unchanged.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact artifact, projection and reader bounds"
    )]
    pub fn pinned_object_batch_cursor_with_projection(
        &self,
        dataset: &PinnedDataset,
        artifact_id: Uuid,
        ordinal: usize,
        columns: &[&str],
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        self.pinned_object_batch_cursor(
            dataset,
            artifact_id,
            ordinal,
            batch_rows,
            max_batch_bytes,
            cancellation,
        )?
        .with_projection(columns)
    }

    fn make_batch_cursor(
        &self,
        dataset: &PinnedDataset,
        objects: Vec<PinnedManifestObject>,
        expected_rows: u64,
        batch_rows: usize,
        max_batch_bytes: usize,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        if batch_rows == 0 || max_batch_bytes == 0 || objects.is_empty() {
            return Err(ParquetStoreError::ReadLimitExceeded);
        }
        let schema = crate::schema::DatasetSchemaRegistry::local()
            .resolve(dataset.manifest().schema())
            .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?;
        Ok(PinnedBatchCursor {
            state: Some(CursorState {
                directory: self.directory.try_clone()?,
                _pin: Some(dataset.clone()),
                objects: objects.iter().map(CursorObject::from_pinned).collect(),
                schema: Some(schema),
                next_object: 0,
                reader: None,
                reader_schema: None,
                object_rows: 0,
                generation_rows: 0,
                expected_rows,
                batch_rows,
                max_batch_bytes,
                start_row: 0,
                projection: None,
            }),
            supervisor: BlockingIoSupervisor::new(cancellation.child_token()),
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            _authority: Arc::clone(&self.authority),
            complete: false,
            deadline: None,
            expires_at: None,
        })
    }
    /// Reopens only a catalog-selected row interval on the caller's supervised blocking worker.
    /// The complete immutable file identity and decoder working set are checked before decoding.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact placement and independent read controls"
    )]
    pub(crate) fn read_published_row_range(
        &self,
        object: &PublishedObject,
        schema: SchemaRef,
        first_row: u64,
        row_count: u64,
        projection: Vec<usize>,
        max_batch_bytes: usize,
        deadline: Option<Instant>,
        cancellation: &CancellationToken,
        mut consume: impl FnMut(RecordBatch) -> Result<(), ParquetStoreError>,
    ) -> Result<(), ParquetStoreError> {
        if row_count == 0
            || max_batch_bytes == 0
            || projection.is_empty()
            || first_row
                .checked_add(row_count)
                .is_none_or(|end| end > object.row_count)
            || projection.windows(2).any(|pair| pair[0] >= pair[1])
            || projection
                .last()
                .is_some_and(|column| *column >= schema.fields().len())
        {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let mut state = CursorState {
            directory: self.directory.try_clone()?,
            _pin: None,
            objects: vec![CursorObject {
                reference: object.relative_reference.clone(),
                digest: object.content_hash,
                bytes: object.size_bytes,
                rows: object.row_count,
            }],
            schema: Some(schema),
            next_object: 0,
            reader: None,
            reader_schema: None,
            object_rows: 0,
            generation_rows: first_row,
            expected_rows: object.row_count,
            batch_rows: self.config.max_row_group_rows,
            max_batch_bytes,
            start_row: usize::try_from(first_row).map_err(|_| ParquetStoreError::SizeOverflow)?,
            projection: Some(projection),
        };
        let mut remaining = row_count;
        while remaining > 0 {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(ParquetStoreError::RecoveryDeadlineExceeded);
            }
            let batch = state
                .next(cancellation)?
                .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
            let take = usize::try_from(remaining.min(batch.num_rows() as u64))
                .map_err(|_| ParquetStoreError::SizeOverflow)?;
            if take == 0 {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            consume(batch.slice(0, take))?;
            remaining -= take as u64;
        }
        if cancellation.is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(ParquetStoreError::RecoveryDeadlineExceeded);
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact artifact ownership and independent reader bounds"
    )]
    pub(crate) fn published_batch_cursor(
        &self,
        object: &PublishedObject,
        batch_rows: usize,
        max_batch_bytes: usize,
        expires_at: Timestamp,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedBatchCursor, ParquetStoreError> {
        if batch_rows == 0 || max_batch_bytes == 0 {
            return Err(ParquetStoreError::ReadLimitExceeded);
        }
        Ok(PinnedBatchCursor {
            state: Some(CursorState {
                directory: self.directory.try_clone()?,
                _pin: None,
                objects: vec![CursorObject {
                    reference: object.relative_reference.clone(),
                    digest: object.content_hash,
                    bytes: object.size_bytes,
                    rows: object.row_count,
                }],
                schema: None,
                next_object: 0,
                reader: None,
                reader_schema: None,
                object_rows: 0,
                generation_rows: 0,
                expected_rows: object.row_count,
                batch_rows,
                max_batch_bytes,
                start_row: 0,
                projection: None,
            }),
            supervisor: BlockingIoSupervisor::new(cancellation.child_token()),
            blocking_tasks: Arc::clone(&self.blocking_tasks),
            _authority: Arc::clone(&self.authority),
            complete: false,
            deadline: Some(deadline),
            expires_at: Some(expires_at),
        })
    }
}

impl PinnedBatchCursor {
    fn with_projection(mut self, columns: &[&str]) -> Result<Self, ParquetStoreError> {
        if columns.is_empty() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        let state = self
            .state
            .as_mut()
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        let schema = state
            .schema
            .as_ref()
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        let mut projection = columns
            .iter()
            .map(|name| {
                schema
                    .index_of(name)
                    .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        projection.sort_unstable();
        projection.dedup();
        if projection.len() != columns.len() {
            return Err(ParquetStoreError::ObjectMetadataMismatch);
        }
        state.projection = Some(projection);
        Ok(self)
    }

    /// Returns the exact next unread object ordinal and row, suitable for a generation-bound page.
    pub fn position(&self) -> (usize, u64) {
        let Some(state) = &self.state else {
            return (usize::MAX, u64::MAX);
        };
        if state.reader.is_some() {
            let object = state.next_object - 1;
            if state.object_rows == state.objects[object].rows {
                (state.next_object, 0)
            } else {
                (object, state.object_rows)
            }
        } else {
            (state.next_object, state.start_row as u64)
        }
    }

    /// Decodes one batch, or verifies complete row counts and returns `None`.
    /// An error poisons this cursor; no subsequent call can turn a partial read into success.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ParquetStoreError> {
        if self.supervisor.cancellation().is_cancelled() {
            return Err(ParquetStoreError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            || self.expires_at.is_some_and(|expiry| {
                timestamp_from_system_time(SystemTime::now()).map_or(true, |now| now >= expiry)
            })
        {
            self.supervisor.cancel();
            return Err(ParquetStoreError::ReadDeadlineExceeded);
        }
        if self.complete {
            return Ok(None);
        }
        let mut state = self
            .state
            .take()
            .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
        let cancellation = self.supervisor.cancellation().clone();
        let deadline = self.deadline;
        let permit = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            _ = cursor_deadline(deadline) => { self.supervisor.cancel(); return Err(ParquetStoreError::ReadDeadlineExceeded); },
            permit = Arc::clone(&self.blocking_tasks).acquire_owned() => permit.map_err(|_| ParquetStoreError::BlockingTaskFailed)?,
        };
        let worker_cancel = cancellation.clone();
        let mut worker = self
            .supervisor
            .spawn_blocking(move || {
                let _permit = permit;
                let batch = state.next(&worker_cancel)?;
                Ok::<_, ParquetStoreError>((state, batch))
            })
            .map_err(cursor_admission_error)?;
        let (state, batch) = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ParquetStoreError::Cancelled),
            _ = cursor_deadline(deadline) => { self.supervisor.cancel(); return Err(ParquetStoreError::ReadDeadlineExceeded); },
            result = &mut worker => result.map_err(|_| ParquetStoreError::BlockingTaskFailed)??,
        };
        self.complete = batch.is_none();
        self.state = Some(state);
        Ok(batch)
    }
}

impl Drop for PinnedBatchCursor {
    fn drop(&mut self) {
        self.supervisor.cancel();
    }
}

impl CursorState {
    fn next(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecordBatch>, ParquetStoreError> {
        loop {
            if cancellation.is_cancelled() {
                return Err(ParquetStoreError::Cancelled);
            }
            if let Some(reader) = self.reader.as_mut() {
                if let Some(batch) = reader.next() {
                    let batch = batch?;
                    if batch.get_array_memory_size() > self.max_batch_bytes {
                        return Err(ParquetStoreError::ReadLimitExceeded);
                    }
                    let rows = u64::try_from(batch.num_rows())
                        .map_err(|_| ParquetStoreError::SizeOverflow)?;
                    self.object_rows = self
                        .object_rows
                        .checked_add(rows)
                        .ok_or(ParquetStoreError::SizeOverflow)?;
                    self.generation_rows = self
                        .generation_rows
                        .checked_add(rows)
                        .ok_or(ParquetStoreError::SizeOverflow)?;
                    if self.object_rows > self.objects[self.next_object - 1].rows
                        || self.generation_rows > self.expected_rows
                    {
                        return Err(ParquetStoreError::ObjectMetadataMismatch);
                    }
                    // Parquet's array reader may omit Arrow schema metadata. Restore the
                    // exact verified object schema (with the same projection) without copying
                    // column buffers: canonical decoders use this metadata as schema authority.
                    let schema = self
                        .reader_schema
                        .as_ref()
                        .ok_or(ParquetStoreError::ObjectMetadataMismatch)?;
                    return Ok(Some(RecordBatch::try_new(
                        Arc::clone(schema),
                        batch.columns().to_vec(),
                    )?));
                }
                if self.object_rows != self.objects[self.next_object - 1].rows {
                    return Err(ParquetStoreError::ObjectMetadataMismatch);
                }
                self.reader = None;
                self.reader_schema = None;
            }
            let Some(pinned) = self.objects.get(self.next_object) else {
                if self.generation_rows != self.expected_rows {
                    return Err(ParquetStoreError::ObjectMetadataMismatch);
                }
                return Ok(None);
            };
            let digest = encode_hex(pinned.digest.bytes());
            if pinned.reference != format!("{OBJECTS}/{}/{}.parquet", &digest[..2], digest) {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let mut file = self
                .directory
                .open_with(&pinned.reference, &options)?
                .into_std();
            if !file.metadata()?.is_file()
                || file.metadata()?.len() != pinned.bytes
                || hash_file(&mut file, Some(cancellation))? != pinned.digest
            {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            if pinned.bytes < 8 {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            file.seek(SeekFrom::End(-8))?;
            let mut footer = [0_u8; 8];
            file.read_exact(&mut footer)?;
            let footer_bytes = u32::from_le_bytes(
                footer[..4]
                    .try_into()
                    .map_err(|_| ParquetStoreError::ObjectMetadataMismatch)?,
            );
            if &footer[4..] != b"PAR1" || u64::from(footer_bytes) > pinned.bytes - 8 {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            if usize::try_from(footer_bytes)
                .ok()
                .and_then(|bytes| bytes.checked_mul(16))
                .is_none_or(|bytes| bytes > self.max_batch_bytes)
            {
                return Err(ParquetStoreError::ReadLimitExceeded);
            }
            file.seek(SeekFrom::Start(0))?;
            let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;
            // Indexes live outside the footer. The library fetches their entire enclosing
            // byte range, so admit that range and its decoded metadata before loading it.
            admit_offset_index(
                &metadata,
                pinned.bytes - 8 - u64::from(footer_bytes),
                self.max_batch_bytes,
            )?;
            let mut metadata_reader = ParquetMetaDataReader::new_with_metadata(metadata)
                .with_offset_index_policy(PageIndexPolicy::Optional);
            metadata_reader.read_page_indexes(&file)?;
            let metadata = ArrowReaderMetadata::try_new(
                Arc::new(metadata_reader.finish()?),
                ArrowReaderOptions::new(),
            )?;
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata);
            if u64::try_from(builder.metadata().file_metadata().num_rows()).ok()
                != Some(pinned.rows)
                || self
                    .schema
                    .as_ref()
                    .is_some_and(|schema| builder.schema().fields() != schema.fields())
            {
                return Err(ParquetStoreError::ObjectMetadataMismatch);
            }
            admit_cursor_working_set(
                builder.metadata(),
                builder.schema(),
                self.projection.as_deref(),
                self.batch_rows,
                self.start_row,
                self.max_batch_bytes,
                cancellation,
            )?;
            self.reader_schema = Some(if let Some(columns) = &self.projection {
                Arc::new(builder.schema().project(columns)?)
            } else {
                Arc::clone(builder.schema())
            });
            let builder = if let Some(columns) = &self.projection {
                let projection =
                    ProjectionMask::roots(builder.parquet_schema(), columns.iter().copied());
                builder.with_projection(projection)
            } else {
                builder
            };
            self.reader = Some(
                builder
                    .with_batch_size(self.batch_rows)
                    .with_offset(self.start_row)
                    .build()?,
            );
            self.next_object += 1;
            self.object_rows = self.start_row as u64;
            self.start_row = 0;
        }
    }
}

pub(super) fn cursor_admission_error(error: BlockingIoAdmissionError) -> ParquetStoreError {
    match error {
        BlockingIoAdmissionError::Cancelled => ParquetStoreError::Cancelled,
        BlockingIoAdmissionError::Saturated => ParquetStoreError::BlockingTaskLimitExceeded,
        BlockingIoAdmissionError::ReaperUnavailable => ParquetStoreError::BlockingTaskFailed,
    }
}

async fn cursor_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}
