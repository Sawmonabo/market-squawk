use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use market_squawk_domain::CaptureRetainedSizeError;
use thiserror::Error;

use crate::{JournalError, JournalWriter};

use super::destination::CaptureDestination;
use super::lifecycle::WriterLifecycleCore;
use crate::capture::CapturedRawRecord;

/// Cooperative capture-I/O shutdown observation.
///
/// A successful checkpoint permits entering the next bounded sink operation. It does not prove an
/// already-running operating-system call was cancelled or completed.
#[derive(Clone, Debug)]
pub struct CaptureIoContext {
    pub(super) lifecycle: Arc<WriterLifecycleCore>,
}

impl CaptureIoContext {
    pub(super) fn new(lifecycle: Arc<WriterLifecycleCore>) -> Self {
        Self { lifecycle }
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
        self.lifecycle.shutdown_requested.load(Ordering::Acquire)
    }

    pub(super) fn deadline_reached(&self) -> bool {
        let deadline = match self.lifecycle.shutdown_deadline.lock() {
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
    /// A retained-record graph could not be sized exactly.
    #[error("capture sink retained-size validation failed: {0}")]
    RetainedSize(#[from] CaptureRetainedSizeError),
    /// A cloned record did not preserve the source and payload allocations.
    #[error("capture sink clone did not preserve record allocation identity")]
    InvalidPayloadSharing,
    /// The configured record-count limit was reached.
    #[error("capture sink record limit {limit} reached")]
    RecordLimitExceeded {
        /// Configured maximum retained records.
        limit: usize,
    },
    /// Retaining the record would exceed the sink-owned byte ceiling.
    #[error("capture sink would retain {required} bytes but limit is {limit} bytes")]
    RetainedByteLimitExceeded {
        /// Complete sink-owned bytes required by the rejected append.
        required: usize,
        /// Configured sink-owned byte ceiling.
        limit: usize,
    },
    /// Sink ledger arithmetic failed closed.
    #[error("capture sink retained-byte accounting overflowed")]
    AccountingInvariant,
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
    /// Completes the sink's explicit shutdown protocol after the capture queue is drained.
    ///
    /// The default implementation performs a final durable flush. Sinks with a distinct shutdown
    /// handshake override this method so normal completion can be acknowledged without placing
    /// blocking protocol work in [`Drop`].
    fn finish(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        self.flush(context)
    }
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
    max_records: usize,
    retained_byte_limit: usize,
    fixed_retained_bytes: usize,
    dynamic_retained_bytes: usize,
}

/// Failure to construct a never-growing, separately bounded in-memory capture sink.
#[derive(Debug, Error)]
pub enum MemoryCaptureSinkConstructionError {
    /// The minimum fixed vector graph exceeds the configured sink ceiling.
    #[error("memory capture sink fixed storage requires {required} bytes but limit is {limit}")]
    FixedStorageBudgetExceeded {
        /// Exact lower-bound or observed fixed bytes required.
        required: usize,
        /// Configured sink-owned byte ceiling.
        limit: usize,
    },
    /// Fixed-storage arithmetic overflowed.
    #[error("memory capture sink fixed-storage arithmetic overflowed")]
    ArithmeticOverflow,
    /// The fallible record-vector allocation was refused.
    #[error("memory capture sink allocation for {requested_records} records failed")]
    AllocationFailed {
        /// Exact requested logical record capacity.
        requested_records: usize,
    },
}

impl MemoryCaptureSink {
    /// Constructs a fallibly preallocated sink with independent count and retained-byte limits.
    ///
    /// The logical count limit is never replaced by allocator spare capacity. Once construction
    /// succeeds, appends cannot grow the vector and every retained clone is charged conservatively
    /// even when multiple records share the same source or payload allocation.
    ///
    /// # Errors
    ///
    /// Returns a typed arithmetic, allocation, or fixed-storage refusal before the sink escapes.
    pub fn try_new(
        max_records: NonZeroUsize,
        max_retained_bytes: NonZeroUsize,
    ) -> Result<Self, MemoryCaptureSinkConstructionError> {
        let max_records = max_records.get();
        let retained_byte_limit = max_retained_bytes.get();
        let minimum_fixed = max_records
            .checked_mul(std::mem::size_of::<CapturedRawRecord>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(MemoryCaptureSinkConstructionError::ArithmeticOverflow)?;
        if minimum_fixed > retained_byte_limit {
            return Err(
                MemoryCaptureSinkConstructionError::FixedStorageBudgetExceeded {
                    required: minimum_fixed,
                    limit: retained_byte_limit,
                },
            );
        }
        let mut records = Vec::new();
        records.try_reserve_exact(max_records).map_err(|_error| {
            MemoryCaptureSinkConstructionError::AllocationFailed {
                requested_records: max_records,
            }
        })?;
        let fixed_retained_bytes = records
            .capacity()
            .checked_mul(std::mem::size_of::<CapturedRawRecord>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(MemoryCaptureSinkConstructionError::ArithmeticOverflow)?;
        if fixed_retained_bytes > retained_byte_limit {
            return Err(
                MemoryCaptureSinkConstructionError::FixedStorageBudgetExceeded {
                    required: fixed_retained_bytes,
                    limit: retained_byte_limit,
                },
            );
        }
        Ok(Self {
            destination: CaptureDestination::unique_memory(),
            records,
            max_records,
            retained_byte_limit,
            fixed_retained_bytes,
            dynamic_retained_bytes: 0,
        })
    }

    /// Returns retained records.
    pub fn records(&self) -> &[CapturedRawRecord] {
        &self.records
    }

    /// Returns the exact observed fixed Rust graph charged at construction.
    pub const fn fixed_retained_bytes(&self) -> usize {
        self.fixed_retained_bytes
    }

    /// Returns the conservative sum charged for retained record clones.
    pub const fn dynamic_retained_bytes(&self) -> usize {
        self.dynamic_retained_bytes
    }

    /// Returns the complete sink-owned ledger total.
    pub fn total_retained_bytes(&self) -> Result<usize, CaptureSinkError> {
        self.fixed_retained_bytes
            .checked_add(self.dynamic_retained_bytes)
            .ok_or(CaptureSinkError::AccountingInvariant)
    }

    /// Returns the logical count limit, independent of allocator spare capacity.
    pub const fn max_records(&self) -> usize {
        self.max_records
    }

    /// Returns the observed preallocated record-slot capacity.
    ///
    /// This value is fixed for the sink lifetime. It may conservatively exceed the logical record
    /// limit when the allocator grants spare capacity, but append admission never uses that spare
    /// capacity and therefore never triggers a subsequent vector growth.
    pub const fn allocated_record_capacity(&self) -> usize {
        self.records.capacity()
    }

    /// Returns the separate sink-owned retained-byte ceiling.
    pub const fn retained_byte_limit(&self) -> usize {
        self.retained_byte_limit
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
        if self.records.len() >= self.max_records {
            return Err(CaptureSinkError::RecordLimitExceeded {
                limit: self.max_records,
            });
        }
        let record_dynamic_bytes = record.checked_sink_dynamic_retained_bytes()?;
        let next_dynamic = self
            .dynamic_retained_bytes
            .checked_add(record_dynamic_bytes)
            .ok_or(CaptureSinkError::AccountingInvariant)?;
        let required = self
            .fixed_retained_bytes
            .checked_add(next_dynamic)
            .ok_or(CaptureSinkError::AccountingInvariant)?;
        if required > self.retained_byte_limit {
            return Err(CaptureSinkError::RetainedByteLimitExceeded {
                required,
                limit: self.retained_byte_limit,
            });
        }
        let retained = record.clone();
        if !record.shares_record_allocations_with(&retained) {
            return Err(CaptureSinkError::InvalidPayloadSharing);
        }
        self.records.push(retained);
        self.dynamic_retained_bytes = next_dynamic;
        Ok(())
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()
    }
}
