//! Closed benchmark-support value types.

use thiserror::Error;

/// Frozen real operation selected by the benchmark harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkOperation {
    /// Standard capture queue nonblocking send.
    QueuePush,
    /// Single-writer standard capture queue nonblocking receive.
    QueuePop,
    /// Complete validated publisher admission.
    CaptureAdmission,
    /// Writer-driven bounded sink append.
    WriterAppend,
    /// Writer-driven append through its corresponding policy flush.
    FlushInclusiveWriter,
}

/// Exact outcome of one timed capacity-permitted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkAttemptOutcome {
    /// The operation completed successfully.
    Accepted,
}

/// Result of one unthrottled offer to the real bounded standard capture queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BenchmarkOfferedLoadOutcome {
    /// The queue accepted the complete capture message.
    Accepted,
    /// The queue refused the complete capture message because it was full.
    QueueFull,
}

/// Exact terminal accounting for an offered-load case.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkOfferedLoadReconciliation {
    pub(super) accepted: usize,
    pub(super) consumed: usize,
    pub(super) queued_bytes: usize,
    pub(super) accounting_invariant_failures: u64,
}

/// Exact terminal accounting and immutable-observer samples for one named-operation case.
#[derive(Debug, Eq, PartialEq)]
pub struct BenchmarkCaseReconciliation {
    pub(super) accepted: usize,
    pub(super) consumed: usize,
    pub(super) deferred_samples: Vec<u64>,
    pub(super) queued_bytes: usize,
    pub(super) accounting_invariant_failures: u64,
}

impl BenchmarkCaseReconciliation {
    /// Returns the exact accepted-operation count.
    pub const fn accepted(&self) -> usize {
        self.accepted
    }

    /// Returns the exact drained-operation count.
    pub const fn consumed(&self) -> usize {
        self.consumed
    }

    /// Returns the post-drain queued-byte reservation.
    pub const fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    /// Returns the post-drain accounting invariant failure count.
    pub const fn accounting_invariant_failures(&self) -> u64 {
        self.accounting_invariant_failures
    }

    /// Consumes the opaque reconciliation and returns immutable-observer latency samples.
    pub fn into_samples(self) -> Vec<u64> {
        self.deferred_samples
    }
}

impl BenchmarkOfferedLoadReconciliation {
    /// Returns the number of queue offers accepted during the case.
    pub const fn accepted(self) -> usize {
        self.accepted
    }

    /// Returns the number of accepted messages consumed before shutdown.
    pub const fn consumed(self) -> usize {
        self.consumed
    }

    /// Returns the post-drain queued-byte reservation.
    pub const fn queued_bytes(self) -> usize {
        self.queued_bytes
    }

    /// Returns the post-drain accounting invariant failure count.
    pub const fn accounting_invariant_failures(self) -> u64 {
        self.accounting_invariant_failures
    }
}

/// One named-operation observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkAttempt {
    pub(super) latency_nanos: u64,
}

impl BenchmarkAttempt {
    /// Returns the successful typed outcome.
    pub const fn outcome(self) -> BenchmarkAttemptOutcome {
        BenchmarkAttemptOutcome::Accepted
    }

    /// Returns the named operation's latency. Writer endpoint samples are returned during case
    /// finalization because concurrent publication order is not writer record order.
    pub const fn latency_nanos(self) -> u64 {
        self.latency_nanos
    }
}

/// Benchmark-support preparation or invariant failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BenchmarkSupportError {
    /// A fixed fixture value was invalid, exceeded a bound, or overflowed.
    #[error("benchmark fixture is invalid")]
    InvalidFixture,
    /// Capture construction or activation failed.
    #[error("benchmark capture composition failed")]
    CaptureComposition,
    /// Capacity or item readiness did not complete within the fixed deadline.
    #[error("benchmark capacity permit timed out")]
    PermitTimeout,
    /// A capacity-permitted timed operation refused.
    #[error("benchmark capacity-permitted operation refused")]
    UnexpectedRefusal,
    /// A harness synchronization primitive was poisoned.
    #[error("benchmark synchronization was poisoned")]
    SynchronizationPoisoned,
    /// A bounded harness synchronization barrier did not complete before its deadline.
    #[error("benchmark synchronization deadline elapsed")]
    SynchronizationDeadlineElapsed,
    /// Bounded observation or accounting state was inconsistent.
    #[error("benchmark observation invariant failed")]
    ObservationInvariant,
    /// Writer shutdown or post-drain reconciliation failed.
    #[error("benchmark writer reconciliation failed")]
    Reconciliation,
}

pub(super) fn elapsed_nanos(started: std::time::Instant) -> Result<u64, BenchmarkSupportError> {
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|_error| BenchmarkSupportError::ObservationInvariant)
}

pub(super) fn increment(
    counter: &std::sync::atomic::AtomicUsize,
) -> Result<(), BenchmarkSupportError> {
    counter
        .fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |current| current.checked_add(1),
        )
        .map(|_previous| ())
        .map_err(|_error| BenchmarkSupportError::ObservationInvariant)
}
