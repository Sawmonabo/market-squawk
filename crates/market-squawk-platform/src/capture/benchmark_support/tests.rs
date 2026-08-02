//! Bounded contract tests for the feature-gated production seam.

use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::{
    BenchmarkAttemptOutcome, BenchmarkCase, BenchmarkOfferedLoadCase, BenchmarkOfferedLoadOutcome,
    BenchmarkOperation, BenchmarkSupportError, benchmark_effective_capacity,
    fixture::{BENCHMARK_RECORD_RESERVATION_BUDGET_BYTES, reservation_bytes_for_test},
    verify_comparable_full,
};

#[derive(Debug, Default)]
struct ConcurrentStartState {
    ready: usize,
    released: bool,
}

#[derive(Debug)]
struct ConcurrentStartGate {
    participants: usize,
    state: Mutex<ConcurrentStartState>,
    changed: Condvar,
}

impl ConcurrentStartGate {
    fn new(participants: NonZeroUsize) -> Self {
        Self {
            participants: participants.get(),
            state: Mutex::new(ConcurrentStartState::default()),
            changed: Condvar::new(),
        }
    }

    fn arrive_and_wait(&self, timeout: Duration) -> Result<(), BenchmarkSupportError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        state.ready = state
            .ready
            .checked_add(1)
            .ok_or(BenchmarkSupportError::ObservationInvariant)?;
        self.changed.notify_all();
        let (state, result) = self
            .changed
            .wait_timeout_while(state, timeout, |state| !state.released)
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        if result.timed_out() && !state.released {
            return Err(BenchmarkSupportError::SynchronizationDeadlineElapsed);
        }
        Ok(())
    }

    fn release_when_ready(&self, timeout: Duration) -> Result<(), BenchmarkSupportError> {
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
        while state.ready != self.participants {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(BenchmarkSupportError::SynchronizationDeadlineElapsed);
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_error| BenchmarkSupportError::SynchronizationPoisoned)?;
            state = next;
            if result.timed_out() && state.ready != self.participants {
                return Err(BenchmarkSupportError::SynchronizationDeadlineElapsed);
            }
        }
        state.released = true;
        self.changed.notify_all();
        Ok(())
    }
}

#[test]
fn seam_runs_real_operations_from_two_producer_handles_on_uncontended_path()
-> Result<(), Box<dyn std::error::Error>> {
    for operation in [
        BenchmarkOperation::QueuePush,
        BenchmarkOperation::QueuePop,
        BenchmarkOperation::CaptureAdmission,
        BenchmarkOperation::WriterAppend,
        BenchmarkOperation::FlushInclusiveWriter,
    ] {
        let case = BenchmarkCase::try_new(operation, 8, NonZeroUsize::MIN, 2)?;
        let first = case.try_producer()?;
        let second = case.try_producer()?;
        assert_eq!(
            case.execute_success_path_for_test(first.try_prepare_operation()?)?
                .outcome(),
            BenchmarkAttemptOutcome::Accepted
        );
        assert_eq!(
            case.execute_success_path_for_test(second.try_prepare_operation()?)?
                .outcome(),
            BenchmarkAttemptOutcome::Accepted
        );
        let reconciliation = case.finish()?;
        assert_eq!(reconciliation.accepted(), 2);
        assert_eq!(reconciliation.consumed(), 2);
        assert_eq!(reconciliation.queued_bytes(), 0);
        assert_eq!(reconciliation.accounting_invariant_failures(), 0);
        let samples = reconciliation.into_samples();
        if matches!(
            operation,
            BenchmarkOperation::QueuePop
                | BenchmarkOperation::WriterAppend
                | BenchmarkOperation::FlushInclusiveWriter
        ) {
            assert_eq!(samples.len(), 2);
        } else {
            assert!(samples.is_empty());
        }
    }
    Ok(())
}

#[test]
fn queue_push_concurrent_start_reports_truthful_outcomes_and_reconciles()
-> Result<(), Box<dyn std::error::Error>> {
    let queue_depth = NonZeroUsize::new(2).ok_or("queue depth is zero")?;
    let case = BenchmarkCase::try_new(BenchmarkOperation::QueuePush, 8, queue_depth, 0)?;
    let first = case.try_producer()?.try_prepare_operation()?;
    let second = case.try_producer()?.try_prepare_operation()?;
    let outcomes = case.with_receiver_paused_for_test(|| {
        let participants = NonZeroUsize::new(2).ok_or(BenchmarkSupportError::InvalidFixture)?;
        let start = Arc::new(ConcurrentStartGate::new(participants));
        std::thread::scope(|scope| {
            let first_start = Arc::clone(&start);
            let first = scope.spawn(move || {
                first_start.arrive_and_wait(Duration::from_secs(1))?;
                first.execute()
            });
            let second_start = Arc::clone(&start);
            let second = scope.spawn(move || {
                second_start.arrive_and_wait(Duration::from_secs(1))?;
                second.execute()
            });
            start.release_when_ready(Duration::from_secs(1))?;
            [
                first
                    .join()
                    .map_err(|_panic| BenchmarkSupportError::Reconciliation)?,
                second
                    .join()
                    .map_err(|_panic| BenchmarkSupportError::Reconciliation)?,
            ]
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
        })
    })??;
    let mut accepted = 0_usize;
    for outcome in outcomes {
        assert_eq!(outcome.outcome(), BenchmarkAttemptOutcome::Accepted);
        accepted += 1;
    }
    assert_eq!(accepted, 2);
    let reconciliation = case.finish()?;
    assert_eq!(reconciliation.accepted(), accepted);
    assert_eq!(reconciliation.consumed(), accepted);
    assert_eq!(reconciliation.queued_bytes(), 0);
    assert_eq!(reconciliation.accounting_invariant_failures(), 0);
    Ok(())
}

#[test]
fn receiver_pause_errors_preserve_poison_and_deadline_classes() {
    use super::super::queue::ReceiverPauseError;
    use super::super::writer::lifecycle::CaptureReceiverTestCoordinationError;

    assert_eq!(
        super::queue::map_receiver_pause_error_for_test(ReceiverPauseError::Poisoned),
        BenchmarkSupportError::SynchronizationPoisoned
    );
    assert_eq!(
        super::queue::map_receiver_pause_error_for_test(ReceiverPauseError::DeadlineElapsed),
        BenchmarkSupportError::SynchronizationDeadlineElapsed
    );
    assert_eq!(
        super::capture_case::map_receiver_pause_error_for_test(
            CaptureReceiverTestCoordinationError::Poisoned,
        ),
        BenchmarkSupportError::SynchronizationPoisoned
    );
    assert_eq!(
        super::capture_case::map_receiver_pause_error_for_test(
            CaptureReceiverTestCoordinationError::DeadlineElapsed,
        ),
        BenchmarkSupportError::SynchronizationDeadlineElapsed
    );
}

#[test]
fn effective_capacity_reports_exact_byte_budget_cap_and_one_over()
-> Result<(), Box<dyn std::error::Error>> {
    let reservation_bytes = reservation_bytes_for_test(4 * 1024 * 1024)?;
    let exact = BENCHMARK_RECORD_RESERVATION_BUDGET_BYTES / reservation_bytes;
    let configured = NonZeroUsize::new(exact + 1).ok_or("invalid configured capacity")?;
    let case = BenchmarkCase::try_new(
        BenchmarkOperation::QueuePush,
        4 * 1024 * 1024,
        configured,
        1,
    )?;

    assert_eq!(case.configured_queue_depth(), configured);
    assert_eq!(case.effective_capacity().get(), exact);
    assert_eq!(
        benchmark_effective_capacity(4 * 1024 * 1024, configured)?.get(),
        exact
    );
    assert!(case.effective_capacity() < case.configured_queue_depth());
    let reconciliation = case.finish()?;
    assert_eq!(reconciliation.accepted(), 0);
    assert_eq!(reconciliation.consumed(), 0);
    Ok(())
}

#[test]
fn comparable_full_uses_the_real_selected_capture_queue() -> Result<(), Box<dyn std::error::Error>>
{
    verify_comparable_full()?;
    Ok(())
}

#[test]
fn permit_wait_and_pre_execution_delay_are_excluded_from_queue_push_latency()
-> Result<(), Box<dyn std::error::Error>> {
    let case = BenchmarkCase::try_new(BenchmarkOperation::QueuePush, 8, NonZeroUsize::MIN, 0)?;
    let first = case.try_producer()?.try_prepare_operation()?;
    let second = case.try_producer()?;
    let setup_delay = std::time::Duration::from_millis(50);
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let _ready = ready_sender.send(());
        let prepared = second.try_prepare_operation();
        let elapsed = started.elapsed();
        let _send = sender.send((prepared, elapsed));
    });
    ready_receiver.recv_timeout(std::time::Duration::from_secs(1))?;
    std::thread::sleep(setup_delay);
    let first_attempt = case.execute_success_path_for_test(first)?;
    let (second_prepared, wait_elapsed) =
        receiver.recv_timeout(std::time::Duration::from_secs(1))?;
    let second_attempt = case.execute_success_path_for_test(second_prepared?)?;
    if waiter.join().is_err() {
        return Err("permit waiter panicked".into());
    }
    assert!(wait_elapsed >= setup_delay);
    assert!(first_attempt.latency_nanos() < u64::try_from(setup_delay.as_nanos())?);
    assert!(second_attempt.latency_nanos() < u64::try_from(wait_elapsed.as_nanos())?);
    let reconciliation = case.finish()?;
    assert_eq!(reconciliation.accepted(), 2);
    assert_eq!(reconciliation.consumed(), 2);
    Ok(())
}

#[test]
fn offered_load_queue_reports_real_refusals_and_reconciles()
-> Result<(), Box<dyn std::error::Error>> {
    let case = BenchmarkOfferedLoadCase::try_new(1_024, NonZeroUsize::MIN)?;
    let producer = case.try_producer()?;
    let (first, second) =
        case.with_receiver_paused_for_test(|| (producer.try_offer(), producer.try_offer()))?;
    assert_eq!(first?, BenchmarkOfferedLoadOutcome::Accepted);
    assert_eq!(second?, BenchmarkOfferedLoadOutcome::QueueFull);
    let reconciliation = case.finish()?;
    assert_eq!(reconciliation.accepted(), 1);
    assert_eq!(reconciliation.consumed(), 1);
    Ok(())
}

#[test]
fn candidate_forced_lock_uses_one_real_fixed_ring_attempt_and_reconciles()
-> Result<(), Box<dyn std::error::Error>> {
    let result = super::run_candidate_forced_lock_for_test()?;
    assert_eq!(result.slot_lock_unavailable(), 1);
    assert_eq!(result.accepted(), 0);
    assert_eq!(result.consumed(), 0);
    assert_eq!(result.queued_bytes(), 0);
    assert_eq!(result.record_reservations(), None);
    assert!(result.queue_private_storage_bytes() > 0);
    assert_eq!(result.accounting_invariant_failures(), 0);
    Ok(())
}
