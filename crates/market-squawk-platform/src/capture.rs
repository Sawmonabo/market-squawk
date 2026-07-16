//! Non-blocking raw-capture publication and supervised storage.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
};

use crate::{RawCaptureRecord, RawCaptureRecordError};
use arc_swap::ArcSwap;
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const HEALTHY: u8 = 0;
const INITIALIZING: u8 = 1;
const GENERATION_INVALIDATED: u8 = 2;
const WRITER_NOT_STARTED: u8 = 0;
const WRITER_RUNNING: u8 = 1;
const WRITER_STOPPED: u8 = 2;
const HEALTH_EVENT_CAPACITY: usize = 64;
const CAPTURE_QUEUE_BYTE_BUDGET: usize = 64 * 1024 * 1024;
const CAPTURE_RECORD_OVERHEAD_BUDGET: usize = 512;

fn saturating_atomic_increment(counter: &AtomicU64) {
    let _previous = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_add(1))
    });
}

/// Complete source/session/generation identity to which capture health applies.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CaptureGenerationKey {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: SourceIdentifier,
    generation: ConnectionGeneration,
    connection_id: Uuid,
}

impl CaptureGenerationKey {
    /// Constructs an exact capture-integrity scope from validated domain types.
    pub const fn new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        session_id: SourceIdentifier,
        generation: ConnectionGeneration,
        connection_id: Uuid,
    ) -> Self {
        Self {
            source_id,
            metadata_revision,
            session_id,
            generation,
            connection_id,
        }
    }

    /// Returns the source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the source-metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the source session identity.
    pub const fn session_id(&self) -> &SourceIdentifier {
        &self.session_id
    }

    /// Returns the connection generation.
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    /// Returns the raw-wire connection identity bound to this generation.
    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    fn same_binding_except_generation(&self, other: &Self) -> bool {
        self.source_id == other.source_id
            && self.metadata_revision == other.metadata_revision
            && self.session_id == other.session_id
    }
}

/// Accepted raw record retaining its exact out-of-band capture authority binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedRawRecord {
    key: Arc<CaptureGenerationKey>,
    record: RawCaptureRecord,
}

#[derive(Debug)]
enum CaptureMessage {
    Record {
        captured: CapturedRawRecord,
        reservation: QueueByteReservation,
    },
    Wake,
}

#[derive(Debug)]
struct QueueByteReservation {
    state: Weak<CaptureState>,
    bytes: usize,
}

impl Drop for QueueByteReservation {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state.release_queue_bytes_exact(self.bytes);
        }
    }
}

impl CapturedRawRecord {
    /// Returns the exact source/revision/session/generation binding accepted at publication.
    pub fn key(&self) -> &CaptureGenerationKey {
        &self.key
    }

    /// Returns the unchanged committed-wire record.
    pub const fn record(&self) -> &RawCaptureRecord {
        &self.record
    }
}

#[derive(Debug)]
struct GenerationCaptureState {
    key: Arc<CaptureGenerationKey>,
    integrity: AtomicU8,
    accepting: AtomicBool,
}

impl GenerationCaptureState {
    fn new(key: CaptureGenerationKey, integrity: CaptureIntegrityState) -> Self {
        Self {
            key: Arc::new(key),
            integrity: AtomicU8::new(if integrity == CaptureIntegrityState::Healthy {
                HEALTHY
            } else {
                INITIALIZING
            }),
            accepting: AtomicBool::new(true),
        }
    }

    fn integrity(&self) -> CaptureIntegrityState {
        if self.integrity.load(Ordering::Acquire) == HEALTHY {
            CaptureIntegrityState::Healthy
        } else {
            CaptureIntegrityState::Incomplete
        }
    }
}

#[derive(Debug)]
struct CaptureState {
    active: ArcSwap<GenerationCaptureState>,
    writer_lifecycle: AtomicU8,
    records_written: AtomicU64,
    health_sender: mpsc::SyncSender<CaptureHealthEvent>,
    health_receiver: std::sync::Mutex<mpsc::Receiver<CaptureHealthEvent>>,
    dropped_health_events: AtomicU64,
    accounting_invariant_failures: AtomicU64,
    queued_bytes: AtomicUsize,
}

impl CaptureState {
    fn mark_current_incomplete(&self, reason: CaptureHealthReason) {
        let active = self.active.load_full();
        self.mark_incomplete_for_generation(&active, reason);
    }

    fn mark_incomplete_for_generation(
        &self,
        generation: &Arc<GenerationCaptureState>,
        reason: CaptureHealthReason,
    ) {
        generation
            .integrity
            .store(GENERATION_INVALIDATED, Ordering::Release);
        if self
            .health_sender
            .try_send(CaptureHealthEvent {
                key: generation.key.as_ref().clone(),
                integrity: CaptureIntegrityState::Incomplete,
                reason,
            })
            .is_err()
        {
            saturating_atomic_increment(&self.dropped_health_events);
        }
    }

    fn mark_writer_failed(&self) {
        self.writer_lifecycle
            .store(WRITER_STOPPED, Ordering::Release);
        self.mark_current_incomplete(CaptureHealthReason::WriterFailed);
    }

    fn mark_writer_stopped(&self) {
        self.writer_lifecycle
            .store(WRITER_STOPPED, Ordering::Release);
    }

    fn increment_written(&self, current: u64) -> Option<u64> {
        let next = current.checked_add(1)?;
        self.records_written.store(next, Ordering::Release);
        Some(next)
    }

    fn try_reserve_queue_bytes(self: &Arc<Self>, bytes: usize) -> Option<QueueByteReservation> {
        let mut current = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > CAPTURE_QUEUE_BYTE_BUDGET {
                return None;
            }
            match self.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => {
                    return Some(QueueByteReservation {
                        state: Arc::downgrade(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release_queue_bytes_exact(&self, bytes: usize) {
        let mut current = self.queued_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_sub(bytes) else {
                saturating_atomic_increment(&self.accounting_invariant_failures);
                self.mark_current_incomplete(CaptureHealthReason::AccountingInvariant);
                return;
            };
            match self.queued_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_previous) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Why capture health changed outside the event-to-action path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureHealthReason {
    /// The bounded raw-capture queue had no available slot.
    Saturated,
    /// The capture receiver was closed.
    Closed,
    /// The supervised storage writer failed.
    WriterFailed,
    /// A publisher was used before supervision started or after it stopped.
    WriterUnavailable,
    /// A newly received record failed strict live-capture validation and was not captured.
    InvalidLiveRecord,
    /// Capture authority state was poisoned and failed closed.
    AuthorityPoisoned,
    /// Queue reservation accounting violated its exactly-once invariant.
    AccountingInvariant,
    /// The sole positive capture-allocation supervisor exited or was dropped.
    SupervisorStopped,
    /// Shutdown could not drain to the explicit deadline.
    ShutdownDeadline,
}

/// Bounded control-plane capture-health event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureHealthEvent {
    key: CaptureGenerationKey,
    integrity: CaptureIntegrityState,
    reason: CaptureHealthReason,
}

/// Atomic key-and-integrity view from one immutable generation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureHealthSnapshot {
    key: Arc<CaptureGenerationKey>,
    integrity: CaptureIntegrityState,
}

impl CaptureHealthSnapshot {
    /// Returns the exact generation to which this integrity assessment applies.
    pub fn key(&self) -> &CaptureGenerationKey {
        &self.key
    }

    /// Returns capture integrity for that exact generation.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }
}

impl CaptureHealthEvent {
    /// Returns the exact source/session/generation affected.
    pub const fn key(&self) -> &CaptureGenerationKey {
        &self.key
    }

    /// Returns the new capture-integrity state.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }

    /// Returns the failure reason.
    pub const fn reason(&self) -> CaptureHealthReason {
        self.reason
    }
}

/// Immediate raw-capture publication failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CapturePublishError {
    /// The authority lock was poisoned; execution eligibility must fail closed.
    #[error("raw capture authority state is poisoned")]
    AuthorityPoisoned,
    /// A compatibility record did not satisfy stricter new-live-capture requirements.
    #[error("invalid new live-capture record: {0}")]
    InvalidLiveRecord(#[from] RawCaptureRecordError),
    /// The raw record's connection identity was transplanted from another generation.
    #[error("raw capture record connection does not match the active generation")]
    ConnectionMismatch,
    /// The supplied record binding is not the publisher's exact active binding.
    #[error("raw capture binding does not match the active source/session/generation")]
    BindingMismatch {
        /// Active exact binding.
        expected: Arc<CaptureGenerationKey>,
        /// Supplied exact binding.
        received: Arc<CaptureGenerationKey>,
    },
    /// The bounded queue is full.
    #[error("raw capture queue is saturated")]
    Saturated,
    /// The writer receiver has closed.
    #[error("raw capture writer is closed")]
    Closed,
    /// Publication requires a running supervised writer.
    #[error("raw capture writer is not running")]
    WriterUnavailable,
    /// Positive admission is unavailable until the non-clone control activates this allocation.
    #[error("raw capture allocation is not active")]
    AllocationInactive,
}

impl From<CapturePublishError> for std::io::Error {
    fn from(error: CapturePublishError) -> Self {
        Self::other(error)
    }
}

/// Cloneable publisher whose hot-path operation is validation, state inspection, and `try_send`.
#[derive(Clone, Debug)]
pub struct RawCapturePublisher {
    sender: mpsc::SyncSender<CaptureMessage>,
    state: Arc<CaptureState>,
}

impl RawCapturePublisher {
    /// Publishes one exact raw record for the supplied active binding without waiting for storage.
    pub fn try_publish(
        &self,
        key: &CaptureGenerationKey,
        record: RawCaptureRecord,
    ) -> Result<CaptureAdmissionReceipt, CapturePublishError> {
        let active = self.state.active.load_full();
        if key != active.key.as_ref() {
            return Err(CapturePublishError::BindingMismatch {
                expected: Arc::clone(&active.key),
                received: Arc::new(key.clone()),
            });
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING
            || !active.accepting.load(Ordering::Acquire)
        {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CapturePublishError::WriterUnavailable);
        }
        if active.integrity.load(Ordering::Acquire) != HEALTHY {
            return Err(CapturePublishError::AllocationInactive);
        }
        if let Err(error) = record.validate_live() {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::InvalidLiveRecord);
            return Err(CapturePublishError::InvalidLiveRecord(error));
        }
        if record.connection_id() != active.key.connection_id() {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::InvalidLiveRecord);
            return Err(CapturePublishError::ConnectionMismatch);
        }
        let receipt = CaptureAdmissionReceipt {
            allocation: Arc::clone(&active),
            event_id: record.event_id(),
            source_sequence: record.source_sequence(),
            received_at: record.received_at(),
            payload_digest: Sha256::digest(record.payload()).into(),
        };
        let Some(reserved_bytes) = record
            .payload()
            .len()
            .checked_add(CAPTURE_RECORD_OVERHEAD_BUDGET)
        else {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::Saturated);
            return Err(CapturePublishError::Saturated);
        };
        let Some(reservation) = self.state.try_reserve_queue_bytes(reserved_bytes) else {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::Saturated);
            return Err(CapturePublishError::Saturated);
        };
        let send_result = self.sender.try_send(CaptureMessage::Record {
            captured: CapturedRawRecord {
                key: Arc::clone(&active.key),
                record,
            },
            reservation,
        });
        match send_result {
            Ok(()) => {
                let current = self.state.active.load_full();
                if self.state.writer_lifecycle.load(Ordering::Acquire) == WRITER_RUNNING
                    && active.accepting.load(Ordering::Acquire)
                    && Arc::ptr_eq(&active, &current)
                {
                    Ok(receipt)
                } else {
                    self.state.mark_incomplete_for_generation(
                        &active,
                        CaptureHealthReason::WriterUnavailable,
                    );
                    Err(CapturePublishError::WriterUnavailable)
                }
            }
            Err(mpsc::TrySendError::Full(_message)) => {
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::Saturated);
                Err(CapturePublishError::Saturated)
            }
            Err(mpsc::TrySendError::Disconnected(_message)) => {
                self.state.mark_writer_stopped();
                self.state
                    .mark_incomplete_for_generation(&active, CaptureHealthReason::Closed);
                Err(CapturePublishError::Closed)
            }
        }
    }

    /// Returns the active exact binding.
    pub fn key(&self) -> Result<Arc<CaptureGenerationKey>, CaptureGenerationError> {
        Ok(Arc::clone(&self.state.active.load_full().key))
    }

    /// Returns binding-scoped capture integrity.
    pub fn integrity(&self) -> CaptureIntegrityState {
        self.health_snapshot().integrity()
    }

    /// Atomically loads one exact generation and its associated integrity state.
    pub fn health_snapshot(&self) -> CaptureHealthSnapshot {
        let active = self.state.active.load_full();
        CaptureHealthSnapshot {
            key: Arc::clone(&active.key),
            integrity: active.integrity(),
        }
    }

    /// Returns Incomplete for stale/mismatched generation holders.
    pub fn integrity_for(&self, key: &CaptureGenerationKey) -> CaptureIntegrityState {
        let snapshot = self.health_snapshot();
        if snapshot.key() == key {
            snapshot.integrity()
        } else {
            CaptureIntegrityState::Incomplete
        }
    }

    /// Polls one bounded control-plane health event without blocking.
    pub fn try_next_health(&self) -> Option<CaptureHealthEvent> {
        let receiver = match self.state.health_receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        receiver.try_recv().ok()
    }

    /// Returns the number of bounded health events dropped after the event queue filled.
    pub fn dropped_health_events(&self) -> u64 {
        self.state.dropped_health_events.load(Ordering::Acquire)
    }

    /// Returns aggregate bytes reserved by accepted records awaiting writer processing.
    ///
    /// This is a diagnostic/control-plane counter and is never consulted by strategy logic.
    pub fn queued_bytes(&self) -> usize {
        self.state.queued_bytes.load(Ordering::Acquire)
    }

    /// Returns exactly-once queue-reservation invariant failures.
    pub fn accounting_invariant_failures(&self) -> u64 {
        self.state
            .accounting_invariant_failures
            .load(Ordering::Acquire)
    }
}

/// Receiver owned by the supervised raw-capture writer.
#[derive(Debug)]
pub struct RawCaptureWriter {
    receiver: Arc<std::sync::Mutex<mpsc::Receiver<CaptureMessage>>>,
    sender: Option<mpsc::SyncSender<CaptureMessage>>,
    state: Arc<CaptureState>,
}

impl Drop for RawCaptureWriter {
    fn drop(&mut self) {
        let receiver = match self.receiver.lock() {
            Ok(receiver) => receiver,
            Err(poisoned) => poisoned.into_inner(),
        };
        while receiver.try_recv().is_ok() {}
        if self
            .state
            .writer_lifecycle
            .swap(WRITER_STOPPED, Ordering::AcqRel)
            != WRITER_STOPPED
        {
            self.state
                .mark_current_incomplete(CaptureHealthReason::Closed);
        }
    }
}

/// Creates a bounded capture channel for one exact registered source/session/generation.
pub fn raw_capture_channel(
    capacity: NonZeroUsize,
    key: CaptureGenerationKey,
) -> (RawCapturePublisher, RawCaptureControl, RawCaptureWriter) {
    let (sender, receiver) = mpsc::sync_channel(capacity.get());
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let (health_sender, health_receiver) = mpsc::sync_channel(HEALTH_EVENT_CAPACITY);
    let state = Arc::new(CaptureState {
        active: ArcSwap::from_pointee(GenerationCaptureState::new(
            key,
            CaptureIntegrityState::Incomplete,
        )),
        writer_lifecycle: AtomicU8::new(WRITER_NOT_STARTED),
        records_written: AtomicU64::new(0),
        health_sender,
        health_receiver: std::sync::Mutex::new(health_receiver),
        dropped_health_events: AtomicU64::new(0),
        accounting_invariant_failures: AtomicU64::new(0),
        queued_bytes: AtomicUsize::new(0),
    });
    (
        RawCapturePublisher {
            sender: sender.clone(),
            state: Arc::clone(&state),
        },
        RawCaptureControl {
            state: Arc::clone(&state),
        },
        RawCaptureWriter {
            receiver,
            sender: Some(sender),
            state,
        },
    )
}

mod control;
mod policy;
mod receipt;
mod writer;

pub use control::{CaptureGenerationError, RawCaptureControl};
pub use policy::{CaptureWriterPolicy, CaptureWriterPolicyError};
pub use receipt::CaptureAdmissionReceipt;
pub use writer::{
    CaptureShutdown, CaptureSink, CaptureSinkError, CaptureStorageErrorClass, CaptureWriterHandle,
    CaptureWriterOutcome, CaptureWriterSpawnError, MemoryCaptureSink, spawn_capture_writer,
};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::saturating_atomic_increment;

    #[test]
    fn diagnostic_counter_increment_saturates_at_the_numeric_limit() {
        let counter = AtomicU64::new(u64::MAX);

        saturating_atomic_increment(&counter);

        assert_eq!(counter.load(Ordering::Acquire), u64::MAX);
    }
}
