use super::*;

#[cfg(not(loom))]
#[derive(Debug)]
struct BarrierReleaseGuard(Option<Arc<Barrier>>);

#[cfg(not(loom))]
impl BarrierReleaseGuard {
    fn release(&mut self) {
        if let Some(barrier) = self.0.take() {
            let _leader = barrier.wait();
        }
    }
}

#[cfg(not(loom))]
impl Drop for BarrierReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug)]
struct TestFlushGateReleaseGuard {
    gate: Option<Arc<TestFlushGate>>,
}

impl TestFlushGateReleaseGuard {
    fn new(gate: Arc<TestFlushGate>) -> Self {
        Self { gate: Some(gate) }
    }

    fn release(&mut self) -> Result<(), BenchmarkSupportError> {
        let Some(gate) = self.gate.take() else {
            return Ok(());
        };
        gate.release()
    }
}

impl Drop for TestFlushGateReleaseGuard {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            let _release_result = gate.release();
        }
    }
}

#[test]
fn finalization_waits_for_flush_after_append_observation() -> Result<(), Box<dyn std::error::Error>>
{
    let gate = Arc::new(TestFlushGate::default());
    let (factory, lifecycle, _capacity) = prepare_inner(
        BenchmarkOperation::FlushInclusiveWriter,
        8,
        NonZeroUsize::MIN,
        1,
        Some(Arc::clone(&gate)),
    )?;
    let producer = factory.try_producer(BenchmarkOperation::FlushInclusiveWriter)?;
    lifecycle.execute_capture_uncontended_for_test(producer.try_prepare_operation()?)?;
    if !gate.wait_until_entered(Duration::from_secs(1))? {
        return Err("writer did not reach the controlled flush boundary".into());
    }

    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
    let finalizer = std::thread::spawn(move || {
        let _send_result = result_sender.send(lifecycle.finish());
    });
    let early = result_receiver.recv_timeout(Duration::from_millis(50));
    gate.release()?;
    let (finished_before_release, result) = match early {
        Ok(result) => (true, result),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => (
            false,
            result_receiver
                .recv_timeout(Duration::from_secs(1))
                .map_err(|_error| "finalizer did not complete after flush release")?,
        ),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err("finalizer result channel disconnected".into());
        }
    };
    if finalizer.join().is_err() {
        return Err("finalizer thread panicked".into());
    }

    assert!(!finished_before_release);
    assert_eq!(result?.into_samples().len(), 1);
    Ok(())
}

#[test]
fn finalization_waits_for_record_reservation_release_after_append_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let gate = Arc::new(TestFlushGate::default());
    let (factory, lifecycle, _capacity) = prepare_inner(
        BenchmarkOperation::WriterAppend,
        8,
        NonZeroUsize::MIN,
        1,
        Some(Arc::clone(&gate)),
    )?;
    let producer = factory.try_producer(BenchmarkOperation::WriterAppend)?;
    lifecycle.execute_capture_uncontended_for_test(producer.try_prepare_operation()?)?;
    if !gate.wait_until_entered(Duration::from_secs(1))? {
        return Err("writer did not reach the controlled post-append boundary".into());
    }

    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
    let finalizer = std::thread::spawn(move || {
        let _send_result = result_sender.send(lifecycle.finish());
    });
    let early = result_receiver.recv_timeout(Duration::from_millis(50));
    gate.release()?;
    let (finished_before_release, result) = match early {
        Ok(result) => (true, result),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => (
            false,
            result_receiver
                .recv_timeout(Duration::from_secs(1))
                .map_err(|_error| "finalizer did not complete after append release")?,
        ),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err("finalizer result channel disconnected".into());
        }
    };
    if finalizer.join().is_err() {
        return Err("finalizer thread panicked".into());
    }

    assert!(!finished_before_release);
    assert_eq!(result?.into_samples().len(), 1);
    Ok(())
}

#[test]
fn benchmark_shutdown_timeout_returns_pending_owner_without_blocking_drop()
-> Result<(), Box<dyn std::error::Error>> {
    let gate = Arc::new(TestFlushGate::default());
    let (factory, mut lifecycle, _capacity) = prepare_inner(
        BenchmarkOperation::WriterAppend,
        8,
        NonZeroUsize::MIN,
        1,
        Some(Arc::clone(&gate)),
    )?;
    let producer = factory.try_producer(BenchmarkOperation::WriterAppend)?;
    lifecycle.execute_capture_uncontended_for_test(producer.try_prepare_operation()?)?;
    if !gate.wait_until_entered(Duration::from_secs(1))? {
        return Err("writer did not reach the controlled append boundary".into());
    }
    let mut gate_release = TestFlushGateReleaseGuard::new(Arc::clone(&gate));
    let writer = lifecycle
        .writer
        .take()
        .ok_or("benchmark writer owner is absent")?;
    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(1);
    let shutdown_started = Instant::now();
    let shutdown = std::thread::spawn(move || {
        let _sent = result_sender.send(writer.shutdown_and_join(Duration::from_millis(10)));
    });

    let result = result_receiver
        .recv_timeout(Duration::from_millis(250))
        .map_err(|_error| "benchmark timeout remained blocked in handle drop")??;
    assert!(shutdown_started.elapsed() < Duration::from_millis(250));
    let mut pending = match result {
        BenchmarkCaptureWriterShutdown::Terminated(_termination) => {
            return Err("blocked benchmark writer reported termination".into());
        }
        BenchmarkCaptureWriterShutdown::DeadlineElapsed(pending) => pending,
        BenchmarkCaptureWriterShutdown::ControlFailed(_pending) => {
            return Err("benchmark queue close failed during the timeout fixture".into());
        }
    };
    assert!(pending.retains_owner_storage_for_test());
    assert!(matches!(
        pending.try_reap(),
        Err(CaptureWorkerReapError::WorkerStillRunning)
    ));
    gate_release.release()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or("termination deadline overflow")?;
    while !pending.is_worker_terminated() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let termination = pending
        .try_reap()
        .map_err(|_error| "terminated benchmark worker could not be reaped")?
        .ok_or("benchmark pending owner omitted its termination")?;
    assert!(termination.outcome().is_incomplete());
    assert!(termination.shutdown_deadline_elapsed());
    assert!(!pending.retains_owner_storage_for_test());
    let accounting = lifecycle
        .publisher
        .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    assert_eq!(accounting.record_reservation_bytes(), 0);
    shutdown.join().map_err(|_| "shutdown worker panicked")?;
    Ok(())
}

#[cfg(not(loom))]
#[test]
fn shutdown_drains_a_send_admitted_before_close_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let (factory, mut lifecycle, _capacity) = prepare_inner(
        BenchmarkOperation::WriterAppend,
        8,
        NonZeroUsize::MIN,
        1,
        None,
    )?;
    let permit = lifecycle.permits.acquire()?;
    permit.commit();
    let publisher = lifecycle
        .publisher
        .try_clone()
        .map_err(|_error| "benchmark publisher clone failed")?;
    let messages =
        MessageFactory::try_new(publisher.benchmark_state_for_test(), factory.frame.clone())?;
    let sender = publisher.into_benchmark_sender();
    let message = messages.prepare()?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let entered_worker = Arc::clone(&entered);
    let release_worker = Arc::clone(&release);
    let send = std::thread::spawn(move || {
        sender.try_send_after_registration_paused_for_test(
            message,
            &entered_worker,
            &release_worker,
        )
    });
    entered.wait();
    let mut release_guard = BarrierReleaseGuard(Some(release));
    let writer = lifecycle
        .writer
        .take()
        .ok_or("benchmark writer owner is absent")?;
    let (shutdown_sender, shutdown_receiver) = std::sync::mpsc::sync_channel(1);
    let shutdown = std::thread::spawn(move || {
        let result = writer.shutdown_and_join(Duration::from_secs(1));
        let _sent = shutdown_sender.send(result);
    });

    let early = shutdown_receiver.recv_timeout(Duration::from_millis(50));
    release_guard.release();
    let send_result = send.join().map_err(|_| "send worker panicked")?;
    let shutdown_result = shutdown_receiver
        .recv_timeout(Duration::from_secs(1))
        .map_err(|_error| "shutdown did not finish after admitted send release")??;

    assert!(matches!(
        early,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(send_result.is_ok());
    let termination = match shutdown_result {
        BenchmarkCaptureWriterShutdown::Terminated(termination) => termination,
        BenchmarkCaptureWriterShutdown::DeadlineElapsed(_pending)
        | BenchmarkCaptureWriterShutdown::ControlFailed(_pending) => {
            return Err("admitted send did not drain before shutdown deadline".into());
        }
    };
    assert_eq!(
        termination.outcome(),
        &CaptureWriterOutcome::Complete { records_written: 1 }
    );
    assert_eq!(
        lifecycle
            .observer
            .lock()
            .map_err(|_error| "observer state poisoned")?
            .records_written,
        1
    );
    assert_eq!(lifecycle.permits.available()?, lifecycle.permits.maximum());
    assert_eq!(
        lifecycle
            .publisher
            .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?
            .record_reservation_bytes(),
        0
    );
    shutdown.join().map_err(|_| "shutdown worker panicked")?;
    Ok(())
}
