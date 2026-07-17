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
    BenchmarkOfferedLoadOutcome, BenchmarkOfferedLoadReconciliation, BenchmarkOperation,
    BenchmarkSupportError,
};

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

    /// Duplicates one producer using the selected standard backend's explicit fallible seam.
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
    /// Prepares a real standard capture queue without successful-operation capacity permits.
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

/// Executes the real depth-one standard queue's deterministic full refusal fixture.
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
