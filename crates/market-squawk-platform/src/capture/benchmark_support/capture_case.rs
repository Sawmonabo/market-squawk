//! Production publisher and single-writer sink endpoint cases.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
#[cfg(all(test, not(market_squawk_loom)))]
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use market_squawk_domain::CaptureIntegrityState;

#[cfg(test)]
use super::super::CaptureWorkerReapError;
use super::super::writer::{
    BenchmarkCaptureWriterHandle, BenchmarkCaptureWriterShutdown, spawn_benchmark_capture_writer,
};
use super::super::{
    BenchmarkCapturePublisher, CaptureAccountingSnapshotError, CaptureIoContext,
    CapturePublishError, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
    CaptureWriterOutcome, CaptureWriterPolicy, DiagnosticCaptureBundle, DiagnosticCaptureFrame,
    RawCaptureControl, benchmark_capture_channel,
};
#[cfg(all(test, not(market_squawk_loom)))]
use super::fixture::MessageFactory;
use super::fixture::{
    channel_limits, fixture_identity, next_destination, prepare_fixture, process_infrastructure,
};
use super::observer::{LatencyObserver, LatencySpan, measure_operation};
use super::permit::{AcquiredPermit, PermitCoordinator};
use super::types::{
    BenchmarkAttempt, BenchmarkCaseReconciliation, BenchmarkOperation, BenchmarkSupportError,
    increment,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct CaptureProducerFactory {
    publisher: BenchmarkCapturePublisher<DiagnosticCaptureBundle>,
    frame: DiagnosticCaptureFrame,
    permits: Arc<PermitCoordinator>,
    accepted: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct CaptureProducer {
    operation: BenchmarkOperation,
    publisher: BenchmarkCapturePublisher<DiagnosticCaptureBundle>,
    frame: DiagnosticCaptureFrame,
    permits: Arc<PermitCoordinator>,
    accepted: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct PreparedCaptureOperation {
    operation: BenchmarkOperation,
    publisher: BenchmarkCapturePublisher<DiagnosticCaptureBundle>,
    frame: DiagnosticCaptureFrame,
    permit: AcquiredPermit,
    accepted: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct CaptureLifecycle {
    publisher: BenchmarkCapturePublisher<DiagnosticCaptureBundle>,
    observer: Arc<std::sync::Mutex<ObserverState>>,
    latency_observer: Arc<LatencyObserver>,
    permits: Arc<PermitCoordinator>,
    accepted: Arc<AtomicUsize>,
    expected_samples: usize,
    _control: RawCaptureControl<DiagnosticCaptureBundle>,
    writer: Option<BenchmarkCaptureWriterHandle<DiagnosticCaptureBundle>>,
}

#[derive(Debug)]
struct ObserverState {
    records_written: usize,
}

#[derive(Debug)]
struct ObserverSink {
    destination: super::super::CaptureDestination,
    operation: BenchmarkOperation,
    observer: Arc<std::sync::Mutex<ObserverState>>,
    latency_observer: Arc<LatencyObserver>,
    permits: Arc<PermitCoordinator>,
    pending_flush_span: Option<LatencySpan>,
    #[cfg(test)]
    flush_gate: Option<Arc<TestFlushGate>>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TestFlushGate {
    state: Mutex<TestFlushGateState>,
    changed: Condvar,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TestFlushGateState {
    entered: bool,
    released: bool,
}

pub(super) fn prepare(
    operation: BenchmarkOperation,
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
    maximum_samples: usize,
) -> Result<(CaptureProducerFactory, CaptureLifecycle, NonZeroUsize), BenchmarkSupportError> {
    prepare_inner(operation, payload_bytes, queue_depth, maximum_samples, None)
}

fn prepare_inner(
    operation: BenchmarkOperation,
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
    maximum_samples: usize,
    #[cfg(test)] flush_gate: Option<Arc<TestFlushGate>>,
    #[cfg(not(test))] _flush_gate: Option<()>,
) -> Result<(CaptureProducerFactory, CaptureLifecycle, NonZeroUsize), BenchmarkSupportError> {
    let retained_samples = if operation == BenchmarkOperation::CaptureAdmission {
        0
    } else {
        maximum_samples
    };
    if operation != BenchmarkOperation::CaptureAdmission && retained_samples == 0 {
        return Err(BenchmarkSupportError::InvalidFixture);
    }
    let latency_observer = Arc::new(LatencyObserver::try_new(retained_samples)?);
    let fixture = prepare_fixture(payload_bytes, queue_depth)?;
    let permits = Arc::new(PermitCoordinator::new(
        fixture.effective_capacity,
        fixture.effective_capacity.get(),
    )?);
    let observer = Arc::new(std::sync::Mutex::new(ObserverState { records_written: 0 }));
    let bundle = DiagnosticCaptureBundle::new(fixture_identity()?);
    let process = process_infrastructure()?;
    let (publisher, mut control, writer) =
        benchmark_capture_channel(&process, channel_limits(queue_depth), bundle)
            .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let sink = ObserverSink {
        destination: next_destination()?,
        operation,
        observer: Arc::clone(&observer),
        latency_observer: Arc::clone(&latency_observer),
        permits: Arc::clone(&permits),
        pending_flush_span: None,
        #[cfg(test)]
        flush_gate,
    };
    let flush_every = if operation == BenchmarkOperation::FlushInclusiveWriter {
        NonZeroUsize::MIN
    } else {
        NonZeroUsize::MAX
    };
    let policy = CaptureWriterPolicy::try_new(flush_every, Duration::from_secs(60))
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let writer = spawn_benchmark_capture_writer(writer, sink, policy)
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    control
        .activate_initial()
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let accepted = Arc::new(AtomicUsize::new(0));
    Ok((
        CaptureProducerFactory {
            publisher: publisher
                .try_clone()
                .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
            frame: fixture.frame,
            permits: Arc::clone(&permits),
            accepted: Arc::clone(&accepted),
        },
        CaptureLifecycle {
            publisher,
            observer,
            latency_observer,
            permits,
            accepted,
            expected_samples: retained_samples,
            _control: control,
            writer: Some(writer),
        },
        fixture.effective_capacity,
    ))
}

impl CaptureProducerFactory {
    pub(super) fn try_producer(
        &self,
        operation: BenchmarkOperation,
    ) -> Result<CaptureProducer, BenchmarkSupportError> {
        Ok(CaptureProducer {
            operation,
            publisher: self
                .publisher
                .try_clone()
                .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
            frame: self.frame.clone(),
            permits: Arc::clone(&self.permits),
            accepted: Arc::clone(&self.accepted),
        })
    }
}

impl CaptureProducer {
    pub(super) fn try_prepare_operation(
        &self,
    ) -> Result<PreparedCaptureOperation, BenchmarkSupportError> {
        let permit = self.permits.acquire()?;
        Ok(PreparedCaptureOperation {
            operation: self.operation,
            publisher: self
                .publisher
                .try_clone()
                .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
            frame: self.frame.clone(),
            permit,
            accepted: Arc::clone(&self.accepted),
        })
    }
}

impl PreparedCaptureOperation {
    pub(super) fn execute(self) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        let (result, latency_nanos) = if self.operation == BenchmarkOperation::CaptureAdmission {
            measure_operation(|| self.publisher.try_publish(&self.frame))?
        } else {
            (self.publisher.try_publish(&self.frame), 0)
        };
        match result {
            Ok(receipt) => {
                increment(&self.accepted)?;
                self.permit.commit();
                drop(receipt);
                Ok(BenchmarkAttempt {
                    latency_nanos: if self.operation == BenchmarkOperation::CaptureAdmission {
                        latency_nanos
                    } else {
                        0
                    },
                })
            }
            Err(error) => Err(map_publish_error(error)),
        }
    }
}

impl CaptureLifecycle {
    #[cfg(test)]
    pub(super) fn execute_capture_uncontended_for_test(
        &self,
        operation: PreparedCaptureOperation,
    ) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        let writer = self
            .writer
            .as_ref()
            .ok_or(BenchmarkSupportError::Reconciliation)?;
        writer
            .with_receiver_paused_for_test(SHUTDOWN_TIMEOUT, || operation.execute())
            .map_err(map_receiver_pause_error_for_test)?
    }

    pub(super) fn finish(mut self) -> Result<BenchmarkCaseReconciliation, BenchmarkSupportError> {
        let accepted = self.accepted.load(Ordering::Acquire);
        let deadline = Instant::now()
            .checked_add(SHUTDOWN_TIMEOUT)
            .ok_or(BenchmarkSupportError::Reconciliation)?;
        loop {
            let written = self
                .observer
                .lock()
                .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?
                .records_written;
            let available_permits = self.permits.available()?;
            let reservation_released = match self
                .publisher
                .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
            {
                Ok(accounting) => accounting.record_reservation_bytes() == 0,
                Err(CaptureAccountingSnapshotError::Contended { .. }) => false,
                Err(
                    CaptureAccountingSnapshotError::TransitionOverflow
                    | CaptureAccountingSnapshotError::EpochOverflow
                    | CaptureAccountingSnapshotError::InvariantViolated,
                ) => return Err(BenchmarkSupportError::Reconciliation),
            };
            if written == accepted
                && available_permits == self.permits.maximum()
                && reservation_released
            {
                break;
            }
            if written > accepted
                || available_permits > self.permits.maximum()
                || Instant::now() >= deadline
            {
                return Err(BenchmarkSupportError::Reconciliation);
            }
            std::thread::yield_now();
        }
        let accounting = self
            .publisher
            .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
            .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        if self.permits.available()? != self.permits.maximum()
            || accounting.record_reservation_bytes() != 0
            || accounting.accounting_invariant_failures() != 0
            || self.publisher.dropped_health_events() != 0
            || self.publisher.try_next_health().is_some()
            || self.publisher.integrity() != CaptureIntegrityState::Healthy
        {
            return Err(BenchmarkSupportError::Reconciliation);
        }
        self.permits.close()?;
        let writer = self
            .writer
            .take()
            .ok_or(BenchmarkSupportError::Reconciliation)?;
        let shutdown = writer
            .shutdown_and_join(SHUTDOWN_TIMEOUT)
            .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        let termination = match shutdown {
            BenchmarkCaptureWriterShutdown::Terminated(termination) => termination,
            BenchmarkCaptureWriterShutdown::DeadlineElapsed(mut pending)
            | BenchmarkCaptureWriterShutdown::ControlFailed(mut pending) => {
                if pending.is_worker_terminated() {
                    let _termination = pending
                        .try_reap()
                        .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
                }
                return Err(BenchmarkSupportError::Reconciliation);
            }
        };
        let records_written =
            u64::try_from(accepted).map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        if termination.outcome() != &(CaptureWriterOutcome::Complete { records_written }) {
            return Err(BenchmarkSupportError::Reconciliation);
        }
        let observer = self
            .observer
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        if observer.records_written != accepted {
            return Err(BenchmarkSupportError::ObservationInvariant);
        }
        drop(observer);
        let expected_samples = if self.expected_samples == 0 {
            0
        } else {
            accepted
        };
        let accounting = self
            .publisher
            .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
            .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        let memory = super::benchmark_memory_receipt(
            self.publisher.benchmark_queue_private_storage_bytes(),
            accounting,
        )?;
        Ok(BenchmarkCaseReconciliation {
            accepted,
            consumed: accepted,
            deferred_samples: self.latency_observer.take_exact(expected_samples)?,
            queued_bytes: accounting.record_reservation_bytes(),
            queue_private_storage_bytes: memory.queue_private_storage_bytes,
            fixed_capture_bytes: memory.fixed_capture_bytes,
            total_accounted_bytes: memory.total_accounted_bytes,
            accounting_invariant_failures: accounting.accounting_invariant_failures(),
        })
    }
}

#[cfg(test)]
pub(super) const fn map_receiver_pause_error_for_test(
    error: super::super::writer::lifecycle::CaptureReceiverTestCoordinationError,
) -> BenchmarkSupportError {
    match error {
        super::super::writer::lifecycle::CaptureReceiverTestCoordinationError::Poisoned => {
            BenchmarkSupportError::SynchronizationPoisoned
        }
        super::super::writer::lifecycle::CaptureReceiverTestCoordinationError::DeadlineElapsed => {
            BenchmarkSupportError::SynchronizationDeadlineElapsed
        }
    }
}

impl CaptureSink for ObserverSink {
    fn destination(&self) -> super::super::CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &super::super::CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        match self.operation {
            BenchmarkOperation::CaptureAdmission => {
                self.append_record(record, context)?;
                self.release_permit()?;
            }
            BenchmarkOperation::WriterAppend => {
                let latency_observer = Arc::clone(&self.latency_observer);
                latency_observer
                    .observe(|| self.append_record(record, context))
                    .map_err(map_observation_error)??;
                self.release_permit()?;
                #[cfg(test)]
                if let Some(gate) = &self.flush_gate {
                    gate.enter_and_wait()?;
                }
            }
            BenchmarkOperation::FlushInclusiveWriter => {
                let span = self.latency_observer.begin_span();
                self.append_record(record, context)?;
                if self.pending_flush_span.replace(span).is_some() {
                    return Err(CaptureSinkError::storage(
                        CaptureStorageErrorClass::Corruption,
                    ));
                }
            }
            BenchmarkOperation::QueuePush | BenchmarkOperation::QueuePop => {
                return Err(CaptureSinkError::storage(
                    CaptureStorageErrorClass::Corruption,
                ));
            }
        }
        Ok(())
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        if self.operation == BenchmarkOperation::FlushInclusiveWriter
            && let Some(span) = self.pending_flush_span.take()
        {
            #[cfg(test)]
            if let Some(gate) = &self.flush_gate {
                gate.enter_and_wait()?;
            }
            self.latency_observer
                .complete_span(span)
                .map_err(map_observation_error)?;
            self.release_permit()?;
        }
        Ok(())
    }
}

impl ObserverSink {
    fn append_record(
        &mut self,
        record: &super::super::CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        context.checkpoint()?;
        black_box(record.frame_ordinal());
        let mut observer = self
            .observer
            .lock()
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Corruption))?;
        observer.records_written = observer
            .records_written
            .checked_add(1)
            .ok_or_else(|| CaptureSinkError::storage(CaptureStorageErrorClass::Capacity))?;
        Ok(())
    }

    fn release_permit(&self) -> Result<(), CaptureSinkError> {
        self.permits
            .release()
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Corruption))
    }
}

fn map_publish_error(_error: CapturePublishError) -> BenchmarkSupportError {
    BenchmarkSupportError::UnexpectedRefusal
}

fn map_observation_error(_error: BenchmarkSupportError) -> CaptureSinkError {
    CaptureSinkError::storage(CaptureStorageErrorClass::Corruption)
}

#[cfg(test)]
impl TestFlushGate {
    fn enter_and_wait(&self) -> Result<(), CaptureSinkError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Corruption))?;
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).map_err(|_error| {
                CaptureSinkError::storage(CaptureStorageErrorClass::Corruption)
            })?;
        }
        Ok(())
    }

    fn wait_until_entered(&self, timeout: Duration) -> Result<bool, BenchmarkSupportError> {
        let state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        let (state, _timeout) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.entered)
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        Ok(state.entered)
    }

    fn release(&self) -> Result<(), BenchmarkSupportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        state.released = true;
        self.changed.notify_all();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
