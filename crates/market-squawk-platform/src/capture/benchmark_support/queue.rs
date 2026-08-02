//! Compile-time-selected queue endpoints with single receiver ownership.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

#[cfg(any(test, capture_bench_backend = "candidate"))]
use super::super::capture_channel_core;
use super::super::queue::{
    RecvTimeoutError as FixedRecvTimeoutError, TrySendError as FixedTrySendError,
};
use super::super::transport::CaptureQueueTransport;
#[cfg(any(test, capture_bench_backend = "candidate"))]
use super::super::transport::FixedRingTransport;
#[cfg(all(not(test), capture_bench_backend = "standard"))]
use super::super::transport::{CaptureQueueReceiver, CaptureQueueSender};
use super::super::{
    BenchmarkCaptureWriter, CaptureMessage, DiagnosticCaptureBundle, RawCaptureControl,
    SelectedBenchmarkTransport, benchmark_capture_channel,
};
use super::fixture::{
    MessageFactory, channel_limits, fixture_identity, prepare_fixture, process_infrastructure,
};
use super::observer::{LatencyObserver, measure_operation};
use super::permit::{AcquiredPermit, PermitCoordinator};
#[cfg(any(test, capture_bench_backend = "candidate"))]
use super::types::BenchmarkForcedLockReconciliation;
use super::types::{
    BenchmarkAttempt, BenchmarkCaseReconciliation, BenchmarkOfferedLoadOutcome,
    BenchmarkOfferedLoadReconciliation, BenchmarkOperation, BenchmarkSupportError, increment,
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

type BenchmarkSender = <SelectedBenchmarkTransport as CaptureQueueTransport>::Sender<
    CaptureMessage<DiagnosticCaptureBundle>,
>;
type BenchmarkReceiver = <SelectedBenchmarkTransport as CaptureQueueTransport>::Receiver<
    CaptureMessage<DiagnosticCaptureBundle>,
>;

#[cfg(any(test, capture_bench_backend = "candidate"))]
pub(super) fn run_candidate_forced_lock()
-> Result<BenchmarkForcedLockReconciliation, BenchmarkSupportError> {
    let fixture = prepare_fixture(0, NonZeroUsize::MIN)?;
    let bundle = DiagnosticCaptureBundle::new(fixture_identity()?);
    let process = process_infrastructure()?;
    let (publisher, _control, mut writer) = capture_channel_core::<_, FixedRingTransport>(
        &process,
        channel_limits(NonZeroUsize::MIN),
        bundle,
    )
    .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let messages = MessageFactory::try_new(Arc::clone(&writer.state), fixture.frame)?;
    let holder = publisher.into_benchmark_sender();
    let contender = holder
        .try_clone()
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let message = messages.prepare()?;
    let attempt = holder
        .with_state_locked_for_benchmark(|| contender.try_send(message))
        .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
    match attempt {
        Err(FixedTrySendError::Invariant(returned)) => drop(returned),
        Ok(())
        | Err(
            FixedTrySendError::Full(_)
            | FixedTrySendError::Closed(_)
            | FixedTrySendError::Poisoned(_),
        ) => return Err(BenchmarkSupportError::ObservationInvariant),
    }
    writer
        .queue_control
        .close_and_drain()
        .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
    let receiver = writer
        .receiver
        .take()
        .ok_or(BenchmarkSupportError::Reconciliation)?;
    if !matches!(
        receiver.try_recv(),
        Err(super::super::queue::TryRecvError::Closed)
    ) {
        return Err(BenchmarkSupportError::ObservationInvariant);
    }
    let accounting = writer
        .state
        .accounting
        .try_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
        .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
    let queue_private_storage_bytes = writer
        .state
        .queue_storage
        .retained_queue_bytes()
        .ok_or(BenchmarkSupportError::ObservationInvariant)?;
    Ok(BenchmarkForcedLockReconciliation {
        slot_lock_unavailable: 1,
        accepted: 0,
        consumed: 0,
        queued_bytes: accounting.record_reservation_bytes(),
        record_reservations: None,
        queue_private_storage_bytes,
        accounting_invariant_failures: accounting.accounting_invariant_failures(),
    })
}

#[derive(Debug)]
pub(super) enum QueueProducerFactory {
    Push {
        sender: BenchmarkSender,
        messages: Arc<MessageFactory>,
        permits: Arc<PermitCoordinator>,
        accepted: Arc<AtomicUsize>,
    },
    Pop {
        sender: BenchmarkSender,
        messages: Arc<MessageFactory>,
        permits: Arc<PermitCoordinator>,
        requests: mpsc::SyncSender<PopRequest>,
        accepted: Arc<AtomicUsize>,
    },
}

#[derive(Debug)]
pub(super) enum QueueProducer {
    Push {
        sender: BenchmarkSender,
        messages: Arc<MessageFactory>,
        permits: Arc<PermitCoordinator>,
        accepted: Arc<AtomicUsize>,
    },
    Pop {
        sender: BenchmarkSender,
        messages: Arc<MessageFactory>,
        permits: Arc<PermitCoordinator>,
        requests: mpsc::SyncSender<PopRequest>,
        accepted: Arc<AtomicUsize>,
    },
}

#[derive(Debug)]
pub(super) enum PreparedQueueOperation {
    Push {
        sender: BenchmarkSender,
        message: CaptureMessage<DiagnosticCaptureBundle>,
        permit: AcquiredPermit,
        accepted: Arc<AtomicUsize>,
    },
    Pop {
        sender: BenchmarkSender,
        message: CaptureMessage<DiagnosticCaptureBundle>,
        requests: mpsc::SyncSender<PopRequest>,
        request: PopRequest,
        response: mpsc::Receiver<Result<(), BenchmarkSupportError>>,
        permit: AcquiredPermit,
        accepted: Arc<AtomicUsize>,
    },
}

#[derive(Debug)]
pub(super) struct QueueLifecycle {
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    background: Option<std::thread::JoinHandle<()>>,
    permits: Arc<PermitCoordinator>,
    accepted: Arc<AtomicUsize>,
    consumed: Arc<AtomicUsize>,
    operation: BenchmarkOperation,
    latency_observer: Arc<LatencyObserver>,
    _control: RawCaptureControl<DiagnosticCaptureBundle>,
    writer: BenchmarkCaptureWriter<DiagnosticCaptureBundle>,
}

#[derive(Debug)]
pub(super) struct PopRequest {
    response: mpsc::SyncSender<Result<(), BenchmarkSupportError>>,
}

#[derive(Debug)]
pub(super) struct OfferedLoadProducerFactory {
    sender: BenchmarkSender,
    messages: Arc<MessageFactory>,
    accepted: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct OfferedLoadProducer {
    sender: BenchmarkSender,
    messages: Arc<MessageFactory>,
    accepted: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub(super) struct OfferedLoadLifecycle {
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    background: Option<std::thread::JoinHandle<()>>,
    accepted: Arc<AtomicUsize>,
    consumed: Arc<AtomicUsize>,
    _control: RawCaptureControl<DiagnosticCaptureBundle>,
    writer: BenchmarkCaptureWriter<DiagnosticCaptureBundle>,
}

pub(super) fn prepare_offered_load(
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
) -> Result<(OfferedLoadProducerFactory, OfferedLoadLifecycle), BenchmarkSupportError> {
    let fixture = prepare_fixture(payload_bytes, queue_depth)?;
    let bundle = DiagnosticCaptureBundle::new(fixture_identity()?);
    let process = process_infrastructure()?;
    let (publisher, control, mut writer) =
        benchmark_capture_channel(&process, channel_limits(queue_depth), bundle)
            .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let sender = publisher.into_benchmark_sender();
    let receiver = writer
        .receiver
        .take()
        .ok_or(BenchmarkSupportError::CaptureComposition)?;
    let messages = Arc::new(MessageFactory::try_new(
        Arc::clone(&writer.state),
        fixture.frame,
    )?);
    let accepted = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let background = spawn_offered_load_consumer(
        receiver,
        Arc::clone(&consumed),
        Arc::clone(&stop),
        Arc::clone(&failed),
    )?;
    Ok((
        OfferedLoadProducerFactory {
            sender,
            messages,
            accepted: Arc::clone(&accepted),
        },
        OfferedLoadLifecycle {
            stop,
            failed,
            background: Some(background),
            accepted,
            consumed,
            _control: control,
            writer,
        },
    ))
}

pub(super) fn prepare(
    operation: BenchmarkOperation,
    payload_bytes: usize,
    queue_depth: NonZeroUsize,
    maximum_samples: usize,
) -> Result<(QueueProducerFactory, QueueLifecycle, NonZeroUsize), BenchmarkSupportError> {
    let fixture = prepare_fixture(payload_bytes, queue_depth)?;
    let bundle = DiagnosticCaptureBundle::new(fixture_identity()?);
    let process = process_infrastructure()?;
    let (publisher, control, mut writer) =
        benchmark_capture_channel(&process, channel_limits(queue_depth), bundle)
            .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let sender = publisher.into_benchmark_sender();
    let receiver = writer
        .receiver
        .take()
        .ok_or(BenchmarkSupportError::CaptureComposition)?;
    let messages = Arc::new(MessageFactory::try_new(
        Arc::clone(&writer.state),
        fixture.frame,
    )?);
    let permits = Arc::new(PermitCoordinator::new(
        fixture.effective_capacity,
        fixture.effective_capacity.get(),
    )?);
    let accepted = Arc::new(AtomicUsize::new(0));
    let consumed = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let failed = Arc::new(AtomicBool::new(false));
    let retained_samples = if operation == BenchmarkOperation::QueuePop {
        maximum_samples
    } else {
        0
    };
    if operation == BenchmarkOperation::QueuePop && retained_samples == 0 {
        return Err(BenchmarkSupportError::InvalidFixture);
    }
    let latency_observer = Arc::new(LatencyObserver::try_new(retained_samples)?);
    let (factory, background) = match operation {
        BenchmarkOperation::QueuePush => (
            QueueProducerFactory::Push {
                sender,
                messages,
                permits: Arc::clone(&permits),
                accepted: Arc::clone(&accepted),
            },
            spawn_push_consumer(
                receiver,
                Arc::clone(&permits),
                Arc::clone(&consumed),
                Arc::clone(&stop),
                Arc::clone(&failed),
            )?,
        ),
        BenchmarkOperation::QueuePop => {
            let (requests, request_receiver) = mpsc::sync_channel(fixture.effective_capacity.get());
            (
                QueueProducerFactory::Pop {
                    sender,
                    messages,
                    permits: Arc::clone(&permits),
                    requests,
                    accepted: Arc::clone(&accepted),
                },
                spawn_single_pop_consumer(
                    receiver,
                    request_receiver,
                    Arc::clone(&permits),
                    Arc::clone(&consumed),
                    Arc::clone(&stop),
                    Arc::clone(&failed),
                    Arc::clone(&latency_observer),
                )?,
            )
        }
        _ => return Err(BenchmarkSupportError::InvalidFixture),
    };
    Ok((
        factory,
        QueueLifecycle {
            stop,
            failed,
            background: Some(background),
            permits,
            accepted,
            consumed,
            operation,
            latency_observer,
            _control: control,
            writer,
        },
        fixture.effective_capacity,
    ))
}

impl QueueProducerFactory {
    pub(super) fn try_producer(&self) -> Result<QueueProducer, BenchmarkSupportError> {
        Ok(match self {
            Self::Push {
                sender,
                messages,
                permits,
                accepted,
            } => QueueProducer::Push {
                sender: sender
                    .try_clone()
                    .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
                messages: Arc::clone(messages),
                permits: Arc::clone(permits),
                accepted: Arc::clone(accepted),
            },
            Self::Pop {
                sender,
                messages,
                permits,
                requests,
                accepted,
            } => QueueProducer::Pop {
                sender: sender
                    .try_clone()
                    .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
                messages: Arc::clone(messages),
                permits: Arc::clone(permits),
                requests: requests.clone(),
                accepted: Arc::clone(accepted),
            },
        })
    }
}

impl OfferedLoadProducerFactory {
    pub(super) fn try_producer(&self) -> Result<OfferedLoadProducer, BenchmarkSupportError> {
        Ok(OfferedLoadProducer {
            sender: self
                .sender
                .try_clone()
                .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
            messages: Arc::clone(&self.messages),
            accepted: Arc::clone(&self.accepted),
        })
    }
}

impl OfferedLoadProducer {
    pub(super) fn try_offer(&self) -> Result<BenchmarkOfferedLoadOutcome, BenchmarkSupportError> {
        match self.sender.try_send(self.messages.prepare()?) {
            Ok(()) => {
                increment(&self.accepted)?;
                Ok(BenchmarkOfferedLoadOutcome::Accepted)
            }
            Err(FixedTrySendError::Full(_message)) => Ok(BenchmarkOfferedLoadOutcome::QueueFull),
            Err(
                FixedTrySendError::Closed(_message)
                | FixedTrySendError::Poisoned(_message)
                | FixedTrySendError::Invariant(_message),
            ) => Err(BenchmarkSupportError::Reconciliation),
        }
    }
}

impl QueueProducer {
    pub(super) fn try_prepare_operation(
        &self,
    ) -> Result<PreparedQueueOperation, BenchmarkSupportError> {
        match self {
            Self::Push {
                sender,
                messages,
                permits,
                accepted,
            } => {
                let permit = permits.acquire()?;
                let message = messages.prepare()?;
                Ok(PreparedQueueOperation::Push {
                    sender: sender
                        .try_clone()
                        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
                    message,
                    permit,
                    accepted: Arc::clone(accepted),
                })
            }
            Self::Pop {
                sender,
                messages,
                permits,
                requests,
                accepted,
            } => {
                let permit = permits.acquire()?;
                let message = messages.prepare()?;
                let (response, result) = mpsc::sync_channel(1);
                Ok(PreparedQueueOperation::Pop {
                    sender: sender
                        .try_clone()
                        .map_err(|_error| BenchmarkSupportError::CaptureComposition)?,
                    message,
                    requests: requests.clone(),
                    request: PopRequest { response },
                    response: result,
                    permit,
                    accepted: Arc::clone(accepted),
                })
            }
        }
    }
}

impl PreparedQueueOperation {
    pub(super) fn execute(self) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        match self {
            Self::Push {
                sender,
                message,
                permit,
                accepted,
            } => {
                let (result, latency_nanos) = measure_operation(|| sender.try_send(message))?;
                match result {
                    Ok(()) => {}
                    Err(
                        FixedTrySendError::Full(_message)
                        | FixedTrySendError::Closed(_message)
                        | FixedTrySendError::Poisoned(_message)
                        | FixedTrySendError::Invariant(_message),
                    ) => return Err(BenchmarkSupportError::UnexpectedRefusal),
                }
                increment(&accepted)?;
                permit.commit();
                Ok(BenchmarkAttempt { latency_nanos })
            }
            Self::Pop {
                sender,
                message,
                requests,
                request,
                response,
                permit,
                accepted,
            } => {
                match sender.try_send(message) {
                    Ok(()) => {}
                    Err(
                        FixedTrySendError::Full(_message)
                        | FixedTrySendError::Closed(_message)
                        | FixedTrySendError::Poisoned(_message)
                        | FixedTrySendError::Invariant(_message),
                    ) => return Err(BenchmarkSupportError::UnexpectedRefusal),
                }
                requests
                    .send(request)
                    .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
                response
                    .recv()
                    .map_err(|_error| BenchmarkSupportError::Reconciliation)??;
                increment(&accepted)?;
                permit.commit();
                Ok(BenchmarkAttempt { latency_nanos: 0 })
            }
        }
    }
}

impl QueueLifecycle {
    #[cfg(test)]
    pub(super) fn execute_success_path_for_test(
        &self,
        operation: PreparedQueueOperation,
    ) -> Result<BenchmarkAttempt, BenchmarkSupportError> {
        match (&operation, self.operation) {
            (PreparedQueueOperation::Push { .. }, BenchmarkOperation::QueuePush) => self
                .writer
                .queue_control
                .with_receiver_paused_for_test(SHUTDOWN_TIMEOUT, || operation.execute())
                .map_err(map_receiver_pause_error_for_test)?,
            (PreparedQueueOperation::Pop { .. }, BenchmarkOperation::QueuePop) => {
                operation.execute()
            }
            _ => Err(BenchmarkSupportError::InvalidFixture),
        }
    }

    #[cfg(test)]
    pub(super) fn with_receiver_paused_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, BenchmarkSupportError> {
        self.writer
            .queue_control
            .with_receiver_paused_for_test(SHUTDOWN_TIMEOUT, action)
            .map_err(map_receiver_pause_error_for_test)
    }

    pub(super) fn finish(mut self) -> Result<BenchmarkCaseReconciliation, BenchmarkSupportError> {
        let deadline = Instant::now()
            .checked_add(SHUTDOWN_TIMEOUT)
            .ok_or(BenchmarkSupportError::Reconciliation)?;
        while self.consumed.load(Ordering::Acquire) < self.accepted.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                return Err(BenchmarkSupportError::Reconciliation);
            }
            std::thread::yield_now();
        }
        self.stop.store(true, Ordering::Release);
        let joined = self
            .background
            .take()
            .is_some_and(|background| background.join().is_ok());
        if !joined || self.failed.load(Ordering::Acquire) {
            return Err(BenchmarkSupportError::Reconciliation);
        }
        self.permits.close()?;
        let accounting = self
            .writer
            .state
            .accounting
            .try_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
            .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        let queued_bytes = accounting.record_reservation_bytes();
        let accounting_invariant_failures = accounting.accounting_invariant_failures();
        let memory = super::benchmark_memory_receipt(
            self.writer.state.queue_storage.retained_queue_bytes(),
            accounting,
        )?;
        let accepted = self.accepted.load(Ordering::Acquire);
        let consumed = self.consumed.load(Ordering::Acquire);
        if queued_bytes != 0 || accounting_invariant_failures != 0 || accepted != consumed {
            return Err(BenchmarkSupportError::Reconciliation);
        }
        let expected_samples = if self.operation == BenchmarkOperation::QueuePop {
            accepted
        } else {
            0
        };
        Ok(BenchmarkCaseReconciliation {
            accepted,
            consumed,
            deferred_samples: self.latency_observer.take_exact(expected_samples)?,
            queued_bytes,
            queue_private_storage_bytes: memory.queue_private_storage_bytes,
            fixed_capture_bytes: memory.fixed_capture_bytes,
            total_accounted_bytes: memory.total_accounted_bytes,
            accounting_invariant_failures,
        })
    }
}

impl OfferedLoadLifecycle {
    #[cfg(test)]
    pub(super) fn with_receiver_paused_for_test<R>(
        &self,
        action: impl FnOnce() -> R,
    ) -> Result<R, BenchmarkSupportError> {
        self.writer
            .queue_control
            .with_receiver_paused_for_test(SHUTDOWN_TIMEOUT, action)
            .map_err(map_receiver_pause_error_for_test)
    }

    pub(super) fn finish(
        mut self,
    ) -> Result<BenchmarkOfferedLoadReconciliation, BenchmarkSupportError> {
        let deadline = Instant::now()
            .checked_add(SHUTDOWN_TIMEOUT)
            .ok_or(BenchmarkSupportError::Reconciliation)?;
        while self.consumed.load(Ordering::Acquire) < self.accepted.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                return Err(BenchmarkSupportError::Reconciliation);
            }
            std::thread::yield_now();
        }
        self.stop.store(true, Ordering::Release);
        let joined = self
            .background
            .take()
            .is_some_and(|background| background.join().is_ok());
        let accepted = self.accepted.load(Ordering::Acquire);
        let consumed = self.consumed.load(Ordering::Acquire);
        let accounting = self
            .writer
            .state
            .accounting
            .try_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
            .map_err(|_error| BenchmarkSupportError::Reconciliation)?;
        let queued_bytes = accounting.record_reservation_bytes();
        let accounting_invariant_failures = accounting.accounting_invariant_failures();
        let memory = super::benchmark_memory_receipt(
            self.writer.state.queue_storage.retained_queue_bytes(),
            accounting,
        )?;
        if !joined
            || self.failed.load(Ordering::Acquire)
            || accepted != consumed
            || queued_bytes != 0
            || accounting_invariant_failures != 0
        {
            return Err(BenchmarkSupportError::Reconciliation);
        }
        Ok(BenchmarkOfferedLoadReconciliation {
            accepted,
            consumed,
            queued_bytes,
            queue_private_storage_bytes: memory.queue_private_storage_bytes,
            fixed_capture_bytes: memory.fixed_capture_bytes,
            total_accounted_bytes: memory.total_accounted_bytes,
            accounting_invariant_failures,
        })
    }
}

#[cfg(test)]
pub(super) const fn map_receiver_pause_error_for_test(
    error: super::super::queue::ReceiverPauseError,
) -> BenchmarkSupportError {
    match error {
        super::super::queue::ReceiverPauseError::Poisoned => {
            BenchmarkSupportError::SynchronizationPoisoned
        }
        super::super::queue::ReceiverPauseError::DeadlineElapsed => {
            BenchmarkSupportError::SynchronizationDeadlineElapsed
        }
    }
}

pub(super) fn verify_comparable_full() -> Result<(), BenchmarkSupportError> {
    let fixture = prepare_fixture(0, NonZeroUsize::MIN)?;
    let bundle = DiagnosticCaptureBundle::new(fixture_identity()?);
    let process = process_infrastructure()?;
    let (publisher, _control, writer) =
        benchmark_capture_channel(&process, channel_limits(NonZeroUsize::MIN), bundle)
            .map_err(|_error| BenchmarkSupportError::CaptureComposition)?;
    let factory = MessageFactory::try_new(Arc::clone(&writer.state), fixture.frame)?;
    let sender = publisher.into_benchmark_sender();
    sender
        .try_send(factory.prepare()?)
        .map_err(|_error| BenchmarkSupportError::UnexpectedRefusal)?;
    match sender.try_send(factory.prepare()?) {
        Err(FixedTrySendError::Full(_message)) => Ok(()),
        Ok(())
        | Err(
            FixedTrySendError::Closed(_)
            | FixedTrySendError::Poisoned(_)
            | FixedTrySendError::Invariant(_),
        ) => Err(BenchmarkSupportError::UnexpectedRefusal),
    }
}

fn spawn_push_consumer(
    receiver: BenchmarkReceiver,
    permits: Arc<PermitCoordinator>,
    consumed: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, BenchmarkSupportError> {
    std::thread::Builder::new()
        .name("capture-benchmark-push-consumer".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match receiver.recv_timeout(Duration::from_millis(1)) {
                    Ok(message) => {
                        drop(message);
                        if increment(&consumed).is_err() || permits.release().is_err() {
                            failed.store(true, Ordering::Release);
                            break;
                        }
                    }
                    Err(FixedRecvTimeoutError::Timeout) => {}
                    Err(FixedRecvTimeoutError::Closed | FixedRecvTimeoutError::Poisoned) => {
                        failed.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        })
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)
}

fn spawn_single_pop_consumer(
    receiver: BenchmarkReceiver,
    requests: mpsc::Receiver<PopRequest>,
    permits: Arc<PermitCoordinator>,
    consumed: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    latency_observer: Arc<LatencyObserver>,
) -> Result<std::thread::JoinHandle<()>, BenchmarkSupportError> {
    std::thread::Builder::new()
        .name("capture-benchmark-pop-owner".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let request = match requests.recv_timeout(Duration::from_millis(1)) {
                    Ok(request) => request,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let received = latency_observer.observe(|| receiver.try_recv());
                let result = match received {
                    Ok(Ok(message)) => {
                        drop(message);
                        if increment(&consumed).is_ok() && permits.release().is_ok() {
                            Ok(())
                        } else {
                            Err(BenchmarkSupportError::ObservationInvariant)
                        }
                    }
                    Ok(Err(_error)) => Err(BenchmarkSupportError::UnexpectedRefusal),
                    Err(error) => Err(error),
                };
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                if request.response.send(result).is_err() {
                    failed.store(true, Ordering::Release);
                }
            }
        })
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)
}

fn spawn_offered_load_consumer(
    receiver: BenchmarkReceiver,
    consumed: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, BenchmarkSupportError> {
    std::thread::Builder::new()
        .name("capture-benchmark-offered-load-consumer".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match receiver.recv_timeout(Duration::from_millis(1)) {
                    Ok(message) => {
                        drop(message);
                        if increment(&consumed).is_err() {
                            failed.store(true, Ordering::Release);
                            break;
                        }
                    }
                    Err(FixedRecvTimeoutError::Timeout) => {}
                    Err(FixedRecvTimeoutError::Closed | FixedRecvTimeoutError::Poisoned) => {
                        failed.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        })
        .map_err(|_error| BenchmarkSupportError::CaptureComposition)
}
