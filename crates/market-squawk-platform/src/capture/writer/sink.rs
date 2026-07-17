use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;

use crate::{JournalError, JournalWriter};

use super::destination::CaptureDestination;
use crate::capture::CapturedRawRecord;

/// Cooperative capture-I/O shutdown observation.
///
/// A successful checkpoint permits entering the next bounded sink operation. It does not prove an
/// already-running operating-system call was cancelled or completed.
#[derive(Clone, Debug)]
pub struct CaptureIoContext {
    pub(super) shutdown_requested: Arc<AtomicBool>,
    pub(super) shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
}

impl CaptureIoContext {
    pub(super) fn new(
        shutdown_requested: Arc<AtomicBool>,
        shutdown_deadline: Arc<std::sync::Mutex<Option<std::time::Instant>>>,
    ) -> Self {
        Self {
            shutdown_requested,
            shutdown_deadline,
        }
    }

    /// Fails when the configured shutdown deadline has elapsed.
    ///
    /// This is an operation-boundary check only. It never cancels a call already inside the sink.
    pub fn checkpoint(&self) -> Result<(), CaptureSinkError> {
        if self.deadline_reached() {
            Err(CaptureSinkError::ShutdownDeadline)
        } else {
            Ok(())
        }
    }

    pub(super) fn shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::Acquire)
    }

    pub(super) fn deadline_reached(&self) -> bool {
        let deadline = match self.shutdown_deadline.lock() {
            Ok(deadline) => *deadline,
            Err(poisoned) => *poisoned.into_inner(),
        };
        deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline)
    }
}

/// Typed storage failure returned to the supervised capture writer.
#[derive(Debug, Error)]
pub enum CaptureSinkError {
    /// Cooperative shutdown reached its deadline at an operation boundary.
    #[error("capture shutdown deadline reached")]
    ShutdownDeadline,
    /// Journal storage rejected a record or flush.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// Alternative capture storage failed.
    #[error("capture storage failed: {class:?}")]
    Storage {
        /// Bounded non-secret error classification.
        class: CaptureStorageErrorClass,
    },
}

impl CaptureSinkError {
    /// Constructs an alternative-storage error from a bounded non-secret class.
    pub const fn storage(class: CaptureStorageErrorClass) -> Self {
        Self::Storage { class }
    }
}

/// Bounded, non-secret classification for alternative capture-sink failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureStorageErrorClass {
    /// The storage endpoint is unavailable.
    Unavailable,
    /// The storage endpoint rejected data as corrupt.
    Corruption,
    /// A configured or physical capacity bound was exceeded.
    Capacity,
    /// An unclassified storage failure occurred.
    Other,
}

/// Synchronous diagnostic storage contract consumed only by the background writer.
pub trait CaptureSink: fmt::Debug + Send + 'static {
    /// Returns the mandatory stable identity of the underlying physical storage endpoint.
    ///
    /// Separate sink handles that can write the same endpoint must return an equal,
    /// collision-resistant identity for the entire process lifetime. Alternative sinks must not
    /// assign per-instance or random aliases to shared storage. This process-local fence does not
    /// provide cross-process exclusion, so a sink reachable from multiple processes must enforce
    /// its own operating-system or storage-level ownership primitive.
    ///
    /// [`JournalWriter`] derives this identity from its prepared canonical root and separately
    /// retains the journal's exclusive file lock.
    fn destination(&self) -> CaptureDestination;
    /// Appends one bounded diagnostic record.
    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError>;
    /// Flushes buffered records durably according to the sink contract.
    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError>;
}

impl CaptureSink for JournalWriter {
    fn destination(&self) -> CaptureDestination {
        CaptureDestination::for_journal(self.path())
    }

    fn append(
        &mut self,
        captured: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.append(captured.record()).map_err(Into::into)
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.flush().map_err(Into::into)
    }
}

/// In-memory sink for deterministic supervision tests and diagnostics.
#[derive(Debug)]
pub struct MemoryCaptureSink {
    destination: CaptureDestination,
    records: Vec<CapturedRawRecord>,
}

impl Default for MemoryCaptureSink {
    fn default() -> Self {
        Self {
            destination: CaptureDestination::unique_memory(),
            records: Vec::new(),
        }
    }
}

impl MemoryCaptureSink {
    /// Returns retained records.
    pub fn records(&self) -> &[CapturedRawRecord] {
        &self.records
    }
}

impl CaptureSink for MemoryCaptureSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        self.records.push(record.clone());
        Ok(())
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()
    }
}
