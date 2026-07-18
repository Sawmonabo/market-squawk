//! Opaque production-operation seam for the frozen capture benchmark.
//!
//! This module exists only under the non-default `capture-benchmark` feature. It exposes no raw
//! queue, receiver, authority state, receipt, filesystem handle, or bypass operation. Setup and
//! capacity acquisition occur outside each named latency interval.

mod capture_case;
mod fixture;
mod observer;
mod permit;
mod queue;
mod types;

use std::num::NonZeroUsize;

pub use types::{
    BenchmarkAttempt, BenchmarkAttemptOutcome, BenchmarkCaseReconciliation,
    BenchmarkForcedLockReconciliation, BenchmarkOfferedLoadOutcome,
    BenchmarkOfferedLoadReconciliation, BenchmarkOperation, BenchmarkSupportError,
};

/// Returns the closed identity of the queue transport monomorphized into this benchmark artifact.
pub const fn benchmark_transport_identity() -> &'static str {
    super::benchmark_transport_identity()
}

/// Returns how this artifact classifies its queue implementation's private storage bytes.
///
/// Candidate `FixedQueue` bytes are exact. Standard `sync_channel` bytes are `not_measured`
/// because stable Rust exposes no allocator-retained byte receipt for that implementation.
pub const fn benchmark_private_storage_accounting() -> &'static str {
    super::benchmark_private_storage_accounting()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BenchmarkMemoryReceipt {
    queue_private_storage_bytes: Option<usize>,
    fixed_capture_bytes: Option<usize>,
    total_accounted_bytes: Option<usize>,
}

fn benchmark_memory_receipt(
    queue_private_storage_bytes: Option<usize>,
    accounting: super::CaptureAccountingSnapshot,
) -> Result<BenchmarkMemoryReceipt, BenchmarkSupportError> {
    let Some(queue_private_storage_bytes) = queue_private_storage_bytes else {
        return Ok(BenchmarkMemoryReceipt {
            queue_private_storage_bytes: None,
            fixed_capture_bytes: None,
            total_accounted_bytes: None,
        });
    };
    let fixed_capture_bytes = accounting.fixed_capture_bytes();
    let total_accounted_bytes = accounting.total_accounted_bytes();
    if queue_private_storage_bytes == 0
        || fixed_capture_bytes < queue_private_storage_bytes
        || total_accounted_bytes < fixed_capture_bytes
    {
        return Err(BenchmarkSupportError::ObservationInvariant);
    }
    Ok(BenchmarkMemoryReceipt {
        queue_private_storage_bytes: Some(queue_private_storage_bytes),
        fixed_capture_bytes: Some(fixed_capture_bytes),
        total_accounted_bytes: Some(total_accounted_bytes),
    })
}

/// Runs the candidate-only fixed-ring lock-contention evidence fixture.
///
/// This symbol is absent from the standard-reference compilation.
#[cfg(capture_bench_backend = "candidate")]
pub fn benchmark_candidate_forced_lock()
-> Result<BenchmarkForcedLockReconciliation, BenchmarkSupportError> {
    queue::run_candidate_forced_lock()
}

#[cfg(test)]
fn run_candidate_forced_lock_for_test()
-> Result<BenchmarkForcedLockReconciliation, BenchmarkSupportError> {
    queue::run_candidate_forced_lock()
}

#[derive(Debug)]
enum ProducerFactory {
    Queue(queue::QueueProducerFactory),
    Capture(capture_case::CaptureProducerFactory),
}

#[derive(Debug)]
enum ProducerWorker {
    Queue(queue::QueueProducer),
    Capture(capture_case::CaptureProducer),
}

#[derive(Debug)]
enum PreparedWorker {
    Queue(queue::PreparedQueueOperation),
    Capture(capture_case::PreparedCaptureOperation),
}

#[derive(Debug)]
enum Lifecycle {
    Queue(queue::QueueLifecycle),
    Capture(capture_case::CaptureLifecycle),
}

/// Opaque prepared production case and its single-owner lifecycle.
#[derive(Debug)]
pub struct BenchmarkCase {
    operation: BenchmarkOperation,
    configured_queue_depth: NonZeroUsize,
    effective_capacity: NonZeroUsize,
    producer_factory: ProducerFactory,
    lifecycle: std::sync::Mutex<Option<Lifecycle>>,
}

/// Opaque per-producer handle. Queue-pop handles submit work to one receiver-owning consumer; they
/// never clone or contend on the production receiver.
#[derive(Debug)]
pub struct BenchmarkProducer {
    worker: ProducerWorker,
}

/// Opaque operation whose permits, payload, and message setup are complete before execution.
#[derive(Debug)]
pub struct BenchmarkPreparedOperation {
    worker: PreparedWorker,
}

/// Opaque unthrottled producer case used only for offered-load queue and backend-memory evidence.
#[derive(Debug)]
pub struct BenchmarkOfferedLoadCase {
    producer_factory: queue::OfferedLoadProducerFactory,
    lifecycle: std::sync::Mutex<Option<queue::OfferedLoadLifecycle>>,
}

/// Opaque offered-load producer. It exposes only typed accept/full outcomes, never queue state.
#[derive(Debug)]
pub struct BenchmarkOfferedLoadProducer {
    worker: queue::OfferedLoadProducer,
}

impl BenchmarkCase {
    #[cfg(test)]
    fn execute_success_path_for_test(
        &self,
        operation: BenchmarkPreparedOperation,
    ) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        match (lifecycle.as_ref(), operation.worker) {
            (Some(Lifecycle::Queue(queue)), PreparedWorker::Queue(operation)) => {
                queue.execute_success_path_for_test(operation)
            }
            (Some(Lifecycle::Capture(capture)), PreparedWorker::Capture(operation)) => {
                capture.execute_capture_uncontended_for_test(operation)
            }
            _ => Err(BenchmarkSupportError::InvalidFixture),
        }
    }

    #[cfg(test)]
    fn with_receiver_paused_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, BenchmarkSupportError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        match lifecycle.as_ref() {
            Some(Lifecycle::Queue(queue)) => queue.with_receiver_paused_for_test(action),
            _ => Err(BenchmarkSupportError::InvalidFixture),
        }
    }

    /// Prepares one real operation case. `maximum_samples` is the exact writer-observation bound.
    pub fn try_new(
        operation: BenchmarkOperation,
        payload_bytes: usize,
        queue_depth: NonZeroUsize,
        maximum_samples: usize,
    ) -> Result<Self, BenchmarkSupportError> {
        let (producer_factory, lifecycle, effective_capacity) = match operation {
            BenchmarkOperation::QueuePush | BenchmarkOperation::QueuePop => {
                let (factory, lifecycle, capacity) =
                    queue::prepare(operation, payload_bytes, queue_depth, maximum_samples)?;
                (
                    ProducerFactory::Queue(factory),
                    Lifecycle::Queue(lifecycle),
                    capacity,
                )
            }
            BenchmarkOperation::CaptureAdmission
            | BenchmarkOperation::WriterAppend
            | BenchmarkOperation::FlushInclusiveWriter => {
                let (factory, lifecycle, capacity) =
                    capture_case::prepare(operation, payload_bytes, queue_depth, maximum_samples)?;
                (
                    ProducerFactory::Capture(factory),
                    Lifecycle::Capture(lifecycle),
                    capacity,
                )
            }
        };
        Ok(Self {
            operation,
            configured_queue_depth: queue_depth,
            effective_capacity,
            producer_factory,
            lifecycle: std::sync::Mutex::new(Some(lifecycle)),
        })
    }

    /// Returns the actual configured count bound of the production queue.
    pub const fn configured_queue_depth(&self) -> NonZeroUsize {
        self.configured_queue_depth
    }

    /// Returns the lower of configured queue depth and the exact byte-budget-derived capacity.
    pub const fn effective_capacity(&self) -> NonZeroUsize {
        self.effective_capacity
    }

    /// Duplicates one producer through the selected transport's explicit fallible seam.
    pub fn try_producer(&self) -> Result<BenchmarkProducer, BenchmarkSupportError> {
        let worker = match &self.producer_factory {
            ProducerFactory::Queue(factory) => ProducerWorker::Queue(factory.try_producer()?),
            ProducerFactory::Capture(factory) => {
                ProducerWorker::Capture(factory.try_producer(self.operation)?)
            }
        };
        Ok(BenchmarkProducer { worker })
    }

    /// Stops background ownership, drains, and returns exact writer endpoint observations.
    pub fn finish(&self) -> Result<BenchmarkCaseReconciliation, BenchmarkSupportError> {
        let lifecycle = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
            lifecycle
                .take()
                .ok_or(BenchmarkSupportError::Reconciliation)?
        };
        match lifecycle {
            Lifecycle::Queue(queue) => queue.finish(),
            Lifecycle::Capture(capture) => capture.finish(),
        }
    }
}

impl BenchmarkProducer {
    /// Acquires capacity and prepares the complete message outside the immutable timed boundary.
    pub fn try_prepare_operation(
        &self,
    ) -> Result<BenchmarkPreparedOperation, BenchmarkSupportError> {
        let worker = match &self.worker {
            ProducerWorker::Queue(queue) => PreparedWorker::Queue(queue.try_prepare_operation()?),
            ProducerWorker::Capture(capture) => {
                PreparedWorker::Capture(capture.try_prepare_operation()?)
            }
        };
        Ok(BenchmarkPreparedOperation { worker })
    }
}

impl BenchmarkPreparedOperation {
    /// Performs only the named production operation within the immutable observer boundary.
    pub fn execute(self) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        match self.worker {
            PreparedWorker::Queue(queue) => queue.execute(),
            PreparedWorker::Capture(capture) => capture.execute(),
        }
    }
}

impl BenchmarkOfferedLoadCase {
    #[cfg(test)]
    fn with_receiver_paused_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, BenchmarkSupportError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        lifecycle
            .as_ref()
            .ok_or(BenchmarkSupportError::Reconciliation)?
            .with_receiver_paused_for_test(action)
    }

    /// Prepares the selected real bounded queue without successful-operation capacity permits.
    pub fn try_new(
        payload_bytes: usize,
        queue_depth: NonZeroUsize,
    ) -> Result<Self, BenchmarkSupportError> {
        let (producer_factory, lifecycle) =
            queue::prepare_offered_load(payload_bytes, queue_depth)?;
        Ok(Self {
            producer_factory,
            lifecycle: std::sync::Mutex::new(Some(lifecycle)),
        })
    }

    /// Duplicates one unthrottled producer handle.
    pub fn try_producer(&self) -> Result<BenchmarkOfferedLoadProducer, BenchmarkSupportError> {
        Ok(BenchmarkOfferedLoadProducer {
            worker: self.producer_factory.try_producer()?,
        })
    }

    /// Drains and reconciles every accepted message before stopping the consumer.
    pub fn finish(&self) -> Result<BenchmarkOfferedLoadReconciliation, BenchmarkSupportError> {
        let lifecycle = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
            lifecycle
                .take()
                .ok_or(BenchmarkSupportError::Reconciliation)?
        };
        lifecycle.finish()
    }
}

impl BenchmarkOfferedLoadProducer {
    /// Attempts one complete message offer without waiting for queue capacity.
    pub fn try_offer(&self) -> Result<BenchmarkOfferedLoadOutcome, BenchmarkSupportError> {
        self.worker.try_offer()
    }
}

/// Executes the selected real depth-one queue's deterministic full-refusal fixture.
pub fn verify_comparable_full() -> Result<(), BenchmarkSupportError> {
    queue::verify_comparable_full()
}

/// Returns the production reservation quote's exact effective fixture capacity.
pub fn benchmark_effective_capacity(
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
) -> Result<NonZeroUsize, BenchmarkSupportError> {
    Ok(fixture::prepare_fixture(payload_bytes, queue_depth)?.effective_capacity)
}

#[cfg(test)]
mod tests;
