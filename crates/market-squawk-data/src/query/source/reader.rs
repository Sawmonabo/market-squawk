//! Reserved one-reader execution and immutable RecordBatch streams.

use std::future::Future;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::RecordBatchStream;
use futures_util::Stream;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::reader::{ChunkReader, Length};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::scan::ScanProjection;
use super::{PinnedIoAdmissionError, PinnedIoCancelledError, PinnedRangeMemoryError};
use crate::QueryError;
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor, BlockingIoTask};
use crate::parquet_store::VerifiedPinnedObject;

const STREAM_FIXED_RECEIPT: usize = 64 * 1024;
const DECODER_FIXED_SCRATCH: usize = 64 * 1024;
const MAX_ACTIVE_FILE_RECEIPT: usize = 512 * 1024 * 1024;
const READER_BATCH_ROWS: usize = 8_192;

pub(super) fn shared_batch_stream(
    schema: SchemaRef,
    batches: Arc<Box<[RecordBatch]>>,
    memory_pool: &Arc<dyn MemoryPool>,
) -> DataFusionResult<SendableRecordBatchStream> {
    let wrapper_bytes = batches.iter().try_fold(0_usize, |total, batch| {
        batch
            .num_columns()
            .checked_mul(size_of::<arrow::array::ArrayRef>())
            .and_then(|columns| columns.checked_add(size_of::<RecordBatch>()))
            .and_then(|batch_bytes| total.checked_add(batch_bytes))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("batch stream receipt overflow".into())
            })
    })?;
    let receipt = MemoryConsumer::new("market-squawk-immutable-batch-stream").register(memory_pool);
    receipt.try_grow(wrapper_bytes).map_err(|_| {
        DataFusionError::ResourcesExhausted(
            "immutable batch stream exceeded the query memory pool".into(),
        )
    })?;
    Ok(Box::pin(SharedBatchStream {
        schema,
        batches,
        index: 0,
        _receipt: receipt,
    }))
}

pub(super) fn execute_pinned(
    files: &Arc<Box<[VerifiedPinnedObject]>>,
    projection: &Arc<ScanProjection>,
    memory_pool: Arc<dyn MemoryPool>,
    supervisor: BlockingIoSupervisor,
) -> DataFusionResult<SendableRecordBatchStream> {
    let reader_bytes = files
        .iter()
        .try_fold(0_usize, |maximum, file| {
            active_file_receipt(
                file.reader_metadata().schema(),
                projection.decode_indices.as_deref(),
                file,
            )
            .map(|value| maximum.max(value))
        })
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let receipt_bytes = STREAM_FIXED_RECEIPT
        .checked_add(reader_bytes)
        .ok_or_else(|| DataFusionError::External(Box::new(QueryError::SizeOverflow)))?;
    let reservation =
        MemoryConsumer::new("market-squawk-active-pinned-reader").register(&memory_pool);
    reservation
        .try_grow(receipt_bytes)
        .map_err(|_| DataFusionError::External(Box::new(PinnedRangeMemoryError)))?;
    let receipt = Arc::new(ActiveReaderReceipt {
        _reservation: reservation,
    });
    let cancellation = supervisor.cancellation().child_token();
    let worker_cancellation = cancellation.clone();
    let worker_supervisor = supervisor.clone();
    let files = Arc::clone(files);
    let schema = Arc::clone(&projection.schema);
    let projection = Arc::clone(projection);
    let worker_receipt = Arc::clone(&receipt);
    let (sender, receiver) = mpsc::channel(1);
    let worker = supervisor
        .spawn_blocking(move || {
            let _receipt = worker_receipt;
            let result = read_pinned_files(
                &files,
                &projection,
                &worker_cancellation,
                &worker_supervisor,
                &sender,
            );
            if let Err(error) = result {
                let _ignored = sender.blocking_send(Err(error));
            }
        })
        .map_err(blocking_admission_datafusion)?;
    Ok(Box::pin(PinnedBatchStream {
        schema,
        receiver,
        worker: Some(worker),
        cancellation,
        _receipt: receipt,
        done: false,
    }))
}

#[derive(Debug)]
struct ActiveReaderReceipt {
    _reservation: MemoryReservation,
}

struct SharedBatchStream {
    schema: SchemaRef,
    batches: Arc<Box<[RecordBatch]>>,
    index: usize,
    _receipt: MemoryReservation,
}

impl Stream for SharedBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(batch) = self.batches.get(self.index).cloned() else {
            return Poll::Ready(None);
        };
        self.index += 1;
        Poll::Ready(Some(Ok(batch)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batches.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

impl RecordBatchStream for SharedBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct PinnedBatchStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<DataFusionResult<RecordBatch>>,
    worker: Option<BlockingIoTask<()>>,
    cancellation: CancellationToken,
    _receipt: Arc<ActiveReaderReceipt>,
    done: bool,
}

impl Stream for PinnedBatchStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(batch)),
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                let Some(worker) = self.worker.as_mut() else {
                    self.done = true;
                    return Poll::Ready(None);
                };
                match Pin::new(worker).poll(context) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        self.worker = None;
                        self.done = true;
                        Poll::Ready(None)
                    }
                    Poll::Ready(Err(error)) => {
                        self.worker = None;
                        self.done = true;
                        Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))))
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for PinnedBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for PinnedBatchStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.receiver.close();
    }
}

fn read_pinned_files(
    files: &[VerifiedPinnedObject],
    projection: &ScanProjection,
    cancellation: &CancellationToken,
    _supervisor: &BlockingIoSupervisor,
    sender: &mpsc::Sender<DataFusionResult<RecordBatch>>,
) -> DataFusionResult<()> {
    #[cfg(test)]
    _supervisor
        .wait_at_test_range_barrier()
        .map_err(|error| DataFusionError::External(error.into()))?;
    for file in files {
        if cancellation.is_cancelled() {
            return Err(cancelled_datafusion());
        }
        let chunk_reader = PinnedChunkReader {
            file: file.file(),
            length: file.object_meta().size,
            cancellation: cancellation.clone(),
        };
        let projection_mask = projection.decode_indices.as_deref().map(|indices| {
            ProjectionMask::roots(
                file.reader_metadata().parquet_schema(),
                indices.iter().copied(),
            )
        });
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            chunk_reader,
            file.reader_metadata().clone(),
        )
        .with_batch_size(READER_BATCH_ROWS);
        if let Some(mask) = projection_mask {
            builder = builder.with_projection(mask);
        }
        let reader = builder.build().map_err(parquet_datafusion)?;
        for batch in reader {
            if cancellation.is_cancelled() {
                return Err(cancelled_datafusion());
            }
            let mut batch =
                batch.map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
            if let Some(remap) = projection.output_remap.as_deref() {
                batch = batch.project(remap)?;
            }
            sender
                .blocking_send(Ok(batch))
                .map_err(|_| cancelled_datafusion())?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct PinnedChunkReader {
    file: Arc<Mutex<std::fs::File>>,
    length: u64,
    cancellation: CancellationToken,
}

impl Length for PinnedChunkReader {
    fn len(&self) -> u64 {
        self.length
    }
}

impl ChunkReader for PinnedChunkReader {
    type T = PinnedSequentialRead;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled_parquet());
        }
        Ok(PinnedSequentialRead {
            file: Arc::clone(&self.file),
            position: start,
            cancellation: self.cancellation.clone(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        let end = start
            .checked_add(u64::try_from(length).map_err(|_| cancelled_parquet())?)
            .ok_or_else(cancelled_parquet)?;
        if end > self.length || self.cancellation.is_cancelled() {
            return Err(cancelled_parquet());
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|error| ParquetError::External(Box::new(error)))?;
        bytes.resize(length, 0);
        let mut file = self
            .file
            .lock()
            .map_err(|_| ParquetError::General("verified file mutex was poisoned".into()))?;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut bytes)?;
        if self.cancellation.is_cancelled() {
            return Err(cancelled_parquet());
        }
        Ok(bytes.into())
    }
}

struct PinnedSequentialRead {
    file: Arc<Mutex<std::fs::File>>,
    position: u64,
    cancellation: CancellationToken,
}

impl Read for PinnedSequentialRead {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "pinned Parquet reader cancelled",
            ));
        }
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("verified file mutex was poisoned"))?;
        file.seek(SeekFrom::Start(self.position))?;
        let read = file.read(buffer)?;
        self.position = self
            .position
            .checked_add(u64::try_from(read).map_err(std::io::Error::other)?)
            .ok_or_else(|| std::io::Error::other("pinned read position overflow"))?;
        Ok(read)
    }
}

pub(super) fn validate_supported_schema(schema: &SchemaRef) -> Result<(), QueryError> {
    for field in schema.fields() {
        match field.data_type() {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::Binary => {}
            _ => return Err(QueryError::UnsupportedSourceSchema),
        }
    }
    Ok(())
}

pub(super) fn validate_file_schema(
    schema: &SchemaRef,
    file: &VerifiedPinnedObject,
) -> Result<(), QueryError> {
    let file_schema = file.reader_metadata().schema();
    if file_schema.fields().len() != schema.fields().len()
        || file_schema
            .fields()
            .iter()
            .zip(schema.fields())
            .any(|(file, expected)| {
                file.data_type() != expected.data_type()
                    || file.is_nullable() != expected.is_nullable()
            })
    {
        return Err(QueryError::InvalidSource);
    }
    Ok(())
}

pub(super) fn active_file_receipt(
    table_schema: &SchemaRef,
    projection: Option<&[usize]>,
    file: &VerifiedPinnedObject,
) -> Result<usize, QueryError> {
    let selected = projection.unwrap_or(&[]);
    let all = projection.is_none();
    let metadata = file.reader_metadata().metadata();
    let mut largest_row_group = 0_usize;
    for row_group in metadata.row_groups() {
        if row_group.num_columns() != table_schema.fields().len() {
            return Err(QueryError::InvalidSource);
        }
        let rows = usize::try_from(row_group.num_rows()).map_err(|_| QueryError::InvalidSource)?;
        let mut decoded = 0_usize;
        for (index, field) in table_schema.fields().iter().enumerate() {
            if !all && !selected.contains(&index) {
                continue;
            }
            let column = row_group.column(index);
            let uncompressed = usize::try_from(column.uncompressed_size())
                .map_err(|_| QueryError::InvalidSource)?;
            let compressed =
                usize::try_from(column.compressed_size()).map_err(|_| QueryError::InvalidSource)?;
            let validity = rows.checked_add(7).ok_or(QueryError::SizeOverflow)? / 8;
            let arrow = match field.data_type() {
                DataType::UInt8 => Some(rows),
                DataType::UInt16 => rows.checked_mul(2),
                DataType::UInt32 => rows.checked_mul(4),
                DataType::Int64 | DataType::Timestamp(_, _) => rows.checked_mul(8),
                DataType::Decimal128(_, _) => rows.checked_mul(16),
                DataType::Utf8 | DataType::Binary => rows
                    .checked_add(1)
                    .and_then(|value| value.checked_mul(size_of::<i32>()))
                    .and_then(|offsets| {
                        rows.checked_mul(uncompressed)
                            .and_then(|values| offsets.checked_add(values))
                    }),
                _ => return Err(QueryError::UnsupportedSourceSchema),
            }
            .ok_or(QueryError::SizeOverflow)?;
            decoded = decoded
                .checked_add(compressed)
                .and_then(|value| value.checked_add(uncompressed))
                .and_then(|value| value.checked_add(validity))
                .and_then(|value| value.checked_add(arrow))
                .ok_or(QueryError::SizeOverflow)?;
        }
        largest_row_group =
            largest_row_group.max(decoded.checked_mul(2).ok_or(QueryError::SizeOverflow)?);
    }
    let compressed_file =
        usize::try_from(file.object_meta().size).map_err(|_| QueryError::SizeOverflow)?;
    let receipt = DECODER_FIXED_SCRATCH
        .checked_add(compressed_file)
        .and_then(|value| value.checked_add(largest_row_group))
        .ok_or(QueryError::SizeOverflow)?;
    if receipt > MAX_ACTIVE_FILE_RECEIPT {
        return Err(QueryError::ReaderMemoryBoundExceeded);
    }
    Ok(receipt)
}

fn cancelled_parquet() -> ParquetError {
    ParquetError::External(Box::new(PinnedIoCancelledError))
}

fn cancelled_datafusion() -> DataFusionError {
    DataFusionError::External(Box::new(PinnedIoCancelledError))
}

fn blocking_admission_datafusion(error: BlockingIoAdmissionError) -> DataFusionError {
    match error {
        BlockingIoAdmissionError::Cancelled => cancelled_datafusion(),
        BlockingIoAdmissionError::Saturated => {
            DataFusionError::External(Box::new(PinnedIoAdmissionError))
        }
    }
}

fn parquet_datafusion(error: ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
