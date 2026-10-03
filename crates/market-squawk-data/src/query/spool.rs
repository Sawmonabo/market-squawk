//! Sealed operation-local query batches, without durable artifact publication authority.
use super::QueryError;
use crate::OperationScratchDirectory;
use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use sha2::{Digest as _, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    sync::Arc,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(super) struct PendingQueryBatchStore {
    directory: OperationScratchDirectory,
    writer: Option<StreamWriter<File>>,
    rows: usize,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl PendingQueryBatchStore {
    pub(super) fn new(
        directory: OperationScratchDirectory,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Self, QueryError> {
        Ok(Self {
            directory,
            writer: None,
            rows: 0,
            deadline,
            cancellation,
        })
    }
    pub(super) fn write(&mut self, batch: RecordBatch) -> Result<(), QueryError> {
        checkpoint(self.deadline, &self.cancellation)?;
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.path().join("query.arrow"))?;
            self.writer = Some(StreamWriter::try_new(file, &batch.schema())?);
        }
        self.rows = self
            .rows
            .checked_add(batch.num_rows())
            .ok_or(QueryError::SizeOverflow)?;
        self.writer
            .as_mut()
            .ok_or(QueryError::InvalidSource)?
            .write(&batch)?;
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<SealedQueryBatchStore, QueryError> {
        checkpoint(self.deadline, &self.cancellation)?;
        if let Some(mut writer) = self.writer.take() {
            writer.finish()?;
            writer.get_ref().sync_all()?;
        } else {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(self.directory.path().join("query.arrow"))?
                .sync_all()?;
        }
        let path = self.directory.path().join("query.arrow");
        let (bytes, digest) =
            file_identity(&mut File::open(path)?, self.deadline, &self.cancellation)?;
        Ok(SealedQueryBatchStore {
            directory: Arc::new(self.directory),
            rows: self.rows,
            bytes,
            digest,
            deadline: self.deadline,
            cancellation: self.cancellation,
        })
    }
}

/// Complete manifest/query-authenticated batches. There is no public minting constructor.
#[derive(Debug)]
pub struct SealedQueryBatchStore {
    directory: Arc<OperationScratchDirectory>,
    rows: usize,
    bytes: u64,
    digest: [u8; 32],
    deadline: Instant,
    cancellation: CancellationToken,
}
impl SealedQueryBatchStore {
    /// Shares the source operation lease for authenticated downstream staging.
    pub fn operation_scratch(&self) -> Arc<OperationScratchDirectory> {
        Arc::clone(&self.directory)
    }
    /// Reopens complete verified bytes and releases each batch as it is consumed.
    pub fn cursor(&self) -> Result<SealedQueryBatchCursor, QueryError> {
        let path = self.directory.path().join("query.arrow");
        let mut file = File::open(&path)?;
        if file_identity(&mut file, self.deadline, &self.cancellation)? != (self.bytes, self.digest)
        {
            return Err(QueryError::InvalidSource);
        }
        file.seek(SeekFrom::Start(0))?;
        Ok(SealedQueryBatchCursor {
            reader: if self.bytes == 0 {
                None
            } else {
                Some(StreamReader::try_new(BufReader::new(file), None)?)
            },
            expected_rows: self.rows,
            rows: 0,
            done: false,
            deadline: self.deadline,
            cancellation: self.cancellation.clone(),
            _directory: Arc::clone(&self.directory),
        })
    }
}
/// A bounded sequential cursor over one sealed query's complete IPC stream.
pub struct SealedQueryBatchCursor {
    reader: Option<StreamReader<BufReader<File>>>,
    expected_rows: usize,
    rows: usize,
    done: bool,
    deadline: Instant,
    cancellation: CancellationToken,
    _directory: Arc<OperationScratchDirectory>,
}
impl std::fmt::Debug for SealedQueryBatchCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedQueryBatchCursor")
            .field("rows", &self.rows)
            .field("expected_rows", &self.expected_rows)
            .finish_non_exhaustive()
    }
}
impl Iterator for SealedQueryBatchCursor {
    type Item = Result<RecordBatch, QueryError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Err(error) = checkpoint(self.deadline, &self.cancellation) {
            self.done = true;
            return Some(Err(error));
        }
        match self.reader.as_mut().and_then(Iterator::next) {
            Some(Ok(batch)) => match self.rows.checked_add(batch.num_rows()) {
                Some(rows) if rows <= self.expected_rows => {
                    self.rows = rows;
                    Some(Ok(batch))
                }
                _ => {
                    self.done = true;
                    Some(Err(QueryError::InvalidSource))
                }
            },
            Some(Err(error)) => {
                self.done = true;
                Some(Err(error.into()))
            }
            None => {
                self.done = true;
                if self.rows == self.expected_rows {
                    None
                } else {
                    Some(Err(QueryError::InvalidSource))
                }
            }
        }
    }
}
fn file_identity(
    file: &mut File,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(u64, [u8; 32]), QueryError> {
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        checkpoint(deadline, cancellation)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes = bytes
            .checked_add(count as u64)
            .ok_or(QueryError::SizeOverflow)?;
    }
    Ok((bytes, digest.finalize().into()))
}

fn checkpoint(deadline: Instant, cancellation: &CancellationToken) -> Result<(), QueryError> {
    if cancellation.is_cancelled() {
        return Err(QueryError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(QueryError::DeadlineExceeded);
    }
    Ok(())
}
