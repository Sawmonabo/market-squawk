//! Bounded contract tests for the feature-gated production seam.

use std::num::NonZeroUsize;

use super::{
    BenchmarkAttemptOutcome, BenchmarkCase, BenchmarkOfferedLoadCase, BenchmarkOfferedLoadOutcome,
    BenchmarkOperation, BenchmarkSupportError, benchmark_effective_capacity,
    fixture::{BENCHMARK_RECORD_RESERVATION_BUDGET_BYTES, reservation_bytes_for_test},
    verify_comparable_full,
};

#[test]
fn seam_runs_real_operations_with_two_concurrent_producers()
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
        std::thread::scope(|scope| -> Result<(), BenchmarkSupportError> {
            let first = scope.spawn(move || first.try_prepare_operation()?.execute());
            let second = scope.spawn(move || second.try_prepare_operation()?.execute());
            assert_eq!(
                first
                    .join()
                    .map_err(|_panic| BenchmarkSupportError::Reconciliation)??
                    .outcome(),
                BenchmarkAttemptOutcome::Accepted
            );
            assert_eq!(
                second
                    .join()
                    .map_err(|_panic| BenchmarkSupportError::Reconciliation)??
                    .outcome(),
                BenchmarkAttemptOutcome::Accepted
            );
            Ok(())
        })?;
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
fn comparable_full_uses_the_real_standard_capture_queue() -> Result<(), Box<dyn std::error::Error>>
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
    let first_attempt = first.execute()?;
    let (second_prepared, wait_elapsed) =
        receiver.recv_timeout(std::time::Duration::from_secs(1))?;
    let second_attempt = second_prepared?.execute()?;
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
    let mut accepted = 0_usize;
    let mut full = 0_usize;
    for _ in 0..10_000 {
        match producer.try_offer()? {
            BenchmarkOfferedLoadOutcome::Accepted => accepted += 1,
            BenchmarkOfferedLoadOutcome::QueueFull => full += 1,
        }
        if accepted > 0 && full > 0 {
            break;
        }
    }
    assert!(accepted > 0);
    assert!(full > 0);
    let reconciliation = case.finish()?;
    assert_eq!(reconciliation.accepted(), accepted);
    assert_eq!(reconciliation.consumed(), accepted);
    Ok(())
}
