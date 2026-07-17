//! Public capture health, limit, and failure contracts.

use std::num::NonZeroUsize;

use market_squawk_domain::{
    CaptureAuthorityError, CaptureAuthorityIdentity, CaptureIntegrityState,
    CaptureRetainedSizeError,
};
use thiserror::Error;

use super::CaptureIdentitySnapshot;
use crate::RawCaptureRecord;

/// Accepted diagnostic journal record derived from an exact authoritative raw frame.
///
/// This value carries audit identity only. Neither it nor its MSJ1 representation can recreate a
/// source-registry receipt or current live authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapturedRawRecord {
    identity: CaptureIdentitySnapshot,
    frame_ordinal: std::num::NonZeroU64,
    record: RawCaptureRecord,
}

impl CapturedRawRecord {
    pub(super) fn new(
        identity: CaptureIdentitySnapshot,
        frame_ordinal: std::num::NonZeroU64,
        record: RawCaptureRecord,
    ) -> Self {
        Self {
            identity,
            frame_ordinal,
            record,
        }
    }

    /// Returns immutable source/session/generation audit identity.
    pub fn identity(&self) -> &CaptureAuthorityIdentity {
        self.identity.identity()
    }

    /// Returns the exact nonzero generation-local frame ordinal.
    pub const fn frame_ordinal(&self) -> std::num::NonZeroU64 {
        self.frame_ordinal
    }

    /// Returns the diagnostic committed-wire record.
    pub const fn record(&self) -> &RawCaptureRecord {
        &self.record
    }

    pub(super) fn checked_sink_dynamic_retained_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError> {
        self.record.checked_dynamic_retained_bytes()
    }

    pub(super) fn shares_record_allocations_with(&self, other: &Self) -> bool {
        self.record.shares_allocations_with(&other.record)
    }
}

/// Why capture health changed outside the event-to-action path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureHealthReason {
    /// The unified capture-memory ceiling rejected a record or generation reservation.
    CaptureMemoryBudgetExceeded,
    /// The fixed capture queue reached its exact logical record capacity.
    QueueFull,
    /// The nonblocking capture producer could not acquire the queue state immediately.
    QueueContended,
    /// The fixed queue's synchronization state was poisoned.
    QueuePoisoned,
    /// The fixed queue sender count could not be incremented exactly.
    SenderCountOverflow,
    /// The capture receiver was closed.
    Closed,
    /// The supervised storage writer failed.
    WriterFailed,
    /// The writer stopped normally; capture authority still ends with the writer lifetime.
    WriterStopped,
    /// A publisher was used before supervision started or after it stopped.
    WriterUnavailable,
    /// Concrete source-registry admission failed.
    AuthorityRejected,
    /// The nonblocking admission issuer was already in use.
    AuthorityBusy,
    /// Retained-byte accounting overflowed.
    RetainedSizeOverflow,
    /// A complete retained-size or allocation-identity proof failed closed.
    InvalidAuthorityGraph,
    /// Writer-thread diagnostic conversion rejected an exact raw frame.
    DiagnosticConversion,
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
    pub(super) identity: CaptureIdentitySnapshot,
    pub(super) integrity: CaptureIntegrityState,
    pub(super) reason: CaptureHealthReason,
}

impl CaptureHealthEvent {
    /// Returns the exact source/session/generation diagnostic identity.
    pub fn identity(&self) -> &CaptureAuthorityIdentity {
        self.identity.identity()
    }

    /// Returns the one-way integrity state.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }

    /// Returns the failure reason.
    pub const fn reason(&self) -> CaptureHealthReason {
        self.reason
    }
}

/// Atomic identity-and-integrity view from one immutable generation allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureHealthSnapshot {
    pub(super) identity: CaptureIdentitySnapshot,
    pub(super) integrity: CaptureIntegrityState,
}

impl CaptureHealthSnapshot {
    /// Returns exact audit identity.
    pub fn identity(&self) -> &CaptureAuthorityIdentity {
        self.identity.identity()
    }

    /// Returns exact one-way integrity.
    pub const fn integrity(&self) -> CaptureIntegrityState {
        self.integrity
    }
}

impl AsRef<CaptureAuthorityIdentity> for CaptureHealthSnapshot {
    fn as_ref(&self) -> &CaptureAuthorityIdentity {
        self.identity()
    }
}

impl std::ops::Deref for CaptureHealthSnapshot {
    type Target = CaptureAuthorityIdentity;

    fn deref(&self) -> &Self::Target {
        self.identity()
    }
}

/// Immediate raw-capture publication failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CapturePublishError {
    /// Concrete registry authority rejected the frame or generation.
    #[error("capture authority rejected publication: {0}")]
    Authority(#[from] CaptureAuthorityError),
    /// Another publisher currently owns the sole non-clone admission issuer.
    #[error("capture admission authority is busy")]
    AuthorityBusy,
    /// Deep retained-size accounting overflowed.
    #[error("raw capture retained-size accounting overflowed")]
    RetainedSizeOverflow,
    /// A complete checked retained-size or allocation-identity proof failed.
    #[error("raw capture retained-size proof failed: {0}")]
    RetainedSize(#[from] CaptureRetainedSizeError),
    /// A successful frame report was below an independently known structural minimum.
    #[error("raw capture retained-size report underreported structural storage")]
    RetainedSizeUnderreported,
    /// The borrowed payload view did not match the ownership-preserving capture payload.
    #[error("raw capture borrowed payload view differs from owned payload")]
    InvalidPayloadView,
    /// A generic frame attempted to bypass the fixed live payload ceiling.
    #[error("raw capture payload is {actual} bytes; live maximum is {maximum}")]
    PayloadTooLarge {
        /// Actual owned payload length.
        actual: usize,
        /// Fixed live capture ceiling.
        maximum: usize,
    },
    /// The unified fixed/resident/record memory ceiling rejected the reservation.
    #[error("capture memory requires {required} bytes but ceiling is {ceiling} bytes")]
    CaptureMemoryBudgetExceeded {
        /// Total bytes required by the rejected reservation.
        required: usize,
        /// Configured channel ceiling.
        ceiling: usize,
    },
    /// The fixed queue was full at the count boundary.
    #[error("raw capture queue is full")]
    QueueFull,
    /// The nonblocking publisher could not acquire the queue state immediately.
    #[error("raw capture queue is contended")]
    QueueContended,
    /// The fixed queue's synchronization state was poisoned.
    #[error("raw capture queue state is poisoned")]
    QueuePoisoned,
    /// The writer receiver has closed.
    #[error("raw capture queue is closed")]
    QueueClosed,
    /// Unified accounting entered a terminal fail-closed state.
    #[error("raw capture accounting invariant failed")]
    AccountingInvariant,
    /// Publication requires a running supervised writer.
    #[error("raw capture writer is not running")]
    WriterUnavailable,
}

/// Explicit bounded storage and unified-memory limits for one capture channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureChannelLimits {
    capture_queue_capacity: NonZeroUsize,
    capture_memory_ceiling_bytes: NonZeroUsize,
}

impl CaptureChannelLimits {
    /// Creates limits without allocating or applying an implicit default.
    pub const fn new(
        capture_queue_capacity: NonZeroUsize,
        capture_memory_ceiling_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            capture_queue_capacity,
            capture_memory_ceiling_bytes,
        }
    }

    /// Returns the exact logical record-ring capacity.
    pub const fn capture_queue_capacity(self) -> NonZeroUsize {
        self.capture_queue_capacity
    }

    /// Returns the complete per-channel capture-memory ceiling in bytes.
    pub const fn capture_memory_ceiling_bytes(self) -> NonZeroUsize {
        self.capture_memory_ceiling_bytes
    }
}

/// Fixed queue whose dominant slot allocation failed during channel construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureQueueKind {
    /// The raw-record queue.
    Record,
    /// The bounded capture-health queue.
    Health,
}

/// Failure to construct a fixed-capacity raw-capture channel.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureChannelError {
    /// The initial generation could not prove its complete retained graph.
    #[error("initial capture generation preparation failed: {0}")]
    GenerationPreparation(CaptureRetainedSizeError),
    /// Fixed infrastructure or the initial resident generation exceeded the channel ceiling.
    #[error("capture fixed storage requires {required} bytes but ceiling is {ceiling} bytes")]
    FixedStorageBudgetExceeded {
        /// Complete bytes required by the rejected construction step.
        required: usize,
        /// Configured per-channel ceiling.
        ceiling: usize,
    },
    /// A dominant preallocated ring-slot allocation was refused recoverably.
    #[error("{queue:?} capture queue allocation of {requested_slots} slots failed")]
    QueueAllocationFailed {
        /// Queue whose preallocation failed.
        queue: CaptureQueueKind,
        /// Requested logical slot count.
        requested_slots: NonZeroUsize,
    },
}

/// Failure to prepare another raw-capture publisher handle.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CapturePublisherCloneError {
    /// The queue has closed to new producers.
    #[error("raw capture queue is closed")]
    QueueClosed,
    /// The queue synchronization state is poisoned.
    #[error("raw capture queue state is poisoned")]
    QueuePoisoned,
    /// The exact live sender count cannot be incremented.
    #[error("raw capture publisher count overflowed")]
    SenderCountOverflow,
}

impl From<CapturePublishError> for std::io::Error {
    fn from(error: CapturePublishError) -> Self {
        Self::other(error)
    }
}
