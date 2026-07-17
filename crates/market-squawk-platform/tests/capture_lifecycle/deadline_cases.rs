use super::*;

#[derive(Debug)]
struct DeadlineGatedAppendSink {
    destination: CaptureDestination,
    entered: Option<std::sync::mpsc::SyncSender<()>>,
    release: Option<std::sync::mpsc::Receiver<()>>,
    fail_after_release: bool,
}

impl CaptureSink for DeadlineGatedAppendSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        _record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        if let Some(entered) = self.entered.take() {
            entered
                .send(())
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        if let Some(release) = self.release.take() {
            release
                .recv()
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        if self.fail_after_release {
            Err(CaptureSinkError::storage(
                CaptureStorageErrorClass::Unavailable,
            ))
        } else {
            Ok(())
        }
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_append_returns_owned_pending_worker_and_persists_late_write()
-> Result<(), Box<dyn std::error::Error>> {
    let destination = CaptureDestination::try_named("deadline-gated-append")?;
    let first_identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(3).ok_or("invalid test capacity")?,
        DiagnosticCaptureBundle::new(first_identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        DeadlineGatedAppendSink {
            destination: destination.clone(),
            entered: Some(entered_sender),
            release: Some(release_receiver),
            fail_after_release: false,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let first_receipt = publisher.try_publish(&frame(first_identity.clone(), 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let second_receipt = publisher.try_publish(&frame(first_identity, 2)?)?;
    let queued_before_shutdown = accounted_record_bytes(&publisher)?;
    assert!(queued_before_shutdown > 0);

    let mut pending = handle.shutdown(Duration::from_millis(10));
    tokio::time::timeout(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_millis(1)),
    )
    .await?;
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::DeadlineElapsed
    );
    assert!(!pending.is_worker_terminated());
    let writer_owned_bytes = accounted_record_bytes(&publisher)?;
    assert!(writer_owned_bytes > 0);
    assert!(writer_owned_bytes < queued_before_shutdown);
    assert!(!first_receipt.generation_is_complete());
    assert!(!second_receipt.generation_is_complete());
    assert!(matches!(
        pending.try_reap(),
        Err(CaptureWorkerReapError::WorkerStillRunning)
    ));

    release_sender.send(())?;
    pending.wait_until_terminated().await;
    assert!(pending.is_worker_terminated());

    let (_blocked_publisher, _blocked_control, blocked_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    assert!(matches!(
        spawn_capture_writer(
            blocked_writer,
            DeadlineGatedAppendSink {
                destination: destination.clone(),
                entered: None,
                release: None,
                fail_after_release: false,
            },
            CaptureWriterPolicy::default(),
        ),
        Err(CaptureWriterSpawnError::DestinationFence {
            source: market_squawk_platform::CaptureDestinationFenceError::Busy,
            ..
        })
    ));

    let termination = pending
        .try_reap()?
        .ok_or("finished append worker did not retain termination")?;
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    assert_eq!(
        termination.outcome(),
        &CaptureWriterOutcome::Incomplete {
            records_written: 1,
            reason: CaptureHealthReason::ShutdownDeadline,
        }
    );
    assert!(termination.shutdown_deadline_elapsed());
    assert_eq!(termination.records_written_at_revocation(), 0);
    assert_eq!(termination.final_records_written(), 1);
    assert_eq!(termination.late_records_written(), 1);

    let (_successor_publisher, _successor_control, successor_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let successor_handle = spawn_capture_writer(
        successor_writer,
        DeadlineGatedAppendSink {
            destination,
            entered: None,
            release: None,
            fail_after_release: false,
        },
        CaptureWriterPolicy::default(),
    )?;
    let mut successor_pending = successor_handle.shutdown(Duration::from_secs(1));
    assert_eq!(
        successor_pending.wait_until_deadline().await,
        CaptureShutdownStatus::WorkerTerminated
    );
    let successor_termination = successor_pending
        .try_reap()?
        .ok_or("successor worker did not retain termination")?;
    assert!(!successor_termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_append_error_preserves_deadline_and_storage_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let exact_identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(exact_identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        DeadlineGatedAppendSink {
            destination: CaptureDestination::try_named("deadline-gated-append-error")?,
            entered: Some(entered_sender),
            release: Some(release_receiver),
            fail_after_release: true,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(exact_identity, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let writer_owned_bytes = accounted_record_bytes(&publisher)?;
    assert!(writer_owned_bytes > 0);

    let mut pending = handle.shutdown(Duration::from_millis(10));
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::DeadlineElapsed
    );
    assert_eq!(accounted_record_bytes(&publisher)?, writer_owned_bytes);
    release_sender.send(())?;
    pending.wait_until_terminated().await;
    let termination = pending
        .try_reap()?
        .ok_or("finished append-error worker did not retain termination")?;
    assert_eq!(accounted_record_bytes(&publisher)?, 0);

    assert_eq!(
        termination.outcome(),
        &CaptureWriterOutcome::Incomplete {
            records_written: 0,
            reason: CaptureHealthReason::WriterFailed,
        }
    );
    assert!(termination.shutdown_deadline_elapsed());
    assert_eq!(termination.records_written_at_revocation(), 0);
    assert_eq!(termination.final_records_written(), 0);
    assert_eq!(termination.late_records_written(), 0);
    assert!(!receipt.generation_is_complete());
    Ok(())
}

#[derive(Debug)]
struct DeadlineGatedFlushSink {
    destination: CaptureDestination,
    entered: Option<std::sync::mpsc::SyncSender<()>>,
    release: Option<std::sync::mpsc::Receiver<()>>,
    fail_after_release: bool,
}

impl CaptureSink for DeadlineGatedFlushSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        _record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        Ok(())
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        if let Some(entered) = self.entered.take() {
            entered
                .send(())
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        if let Some(release) = self.release.take() {
            release
                .recv()
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        if self.fail_after_release {
            Err(CaptureSinkError::storage(
                CaptureStorageErrorClass::Unavailable,
            ))
        } else {
            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_flush_returns_owned_pending_worker_without_false_termination()
-> Result<(), Box<dyn std::error::Error>> {
    let exact_identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(exact_identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let policy = CaptureWriterPolicy::try_new(NonZeroUsize::MIN, Duration::from_secs(1))?;
    let handle = spawn_capture_writer(
        writer,
        DeadlineGatedFlushSink {
            destination: CaptureDestination::try_named("deadline-gated-flush")?,
            entered: Some(entered_sender),
            release: Some(release_receiver),
            fail_after_release: false,
        },
        policy,
    )?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(exact_identity, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let writer_owned_bytes = accounted_record_bytes(&publisher)?;
    assert!(writer_owned_bytes > 0);

    let mut pending = handle.shutdown(Duration::from_millis(10));
    tokio::time::timeout(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_millis(1)),
    )
    .await?;
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::DeadlineElapsed
    );
    assert_eq!(accounted_record_bytes(&publisher)?, writer_owned_bytes);
    assert!(!pending.is_worker_terminated());
    assert!(!receipt.generation_is_complete());

    release_sender.send(())?;
    pending.wait_until_terminated().await;
    let termination = pending
        .try_reap()?
        .ok_or("finished flush worker did not retain termination")?;
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    assert_eq!(
        termination.outcome(),
        &CaptureWriterOutcome::Incomplete {
            records_written: 1,
            reason: CaptureHealthReason::ShutdownDeadline,
        }
    );
    assert!(termination.shutdown_deadline_elapsed());
    assert_eq!(termination.records_written_at_revocation(), 1);
    assert_eq!(termination.final_records_written(), 1);
    assert_eq!(termination.late_records_written(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_flush_error_preserves_deadline_and_storage_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let exact_identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(exact_identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let policy = CaptureWriterPolicy::try_new(NonZeroUsize::MIN, Duration::from_secs(1))?;
    let handle = spawn_capture_writer(
        writer,
        DeadlineGatedFlushSink {
            destination: CaptureDestination::try_named("deadline-gated-flush-error")?,
            entered: Some(entered_sender),
            release: Some(release_receiver),
            fail_after_release: true,
        },
        policy,
    )?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(exact_identity, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let writer_owned_bytes = accounted_record_bytes(&publisher)?;
    assert!(writer_owned_bytes > 0);

    let mut pending = handle.shutdown(Duration::from_millis(10));
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::DeadlineElapsed
    );
    assert_eq!(accounted_record_bytes(&publisher)?, writer_owned_bytes);
    release_sender.send(())?;
    pending.wait_until_terminated().await;
    let termination = pending
        .try_reap()?
        .ok_or("finished flush-error worker did not retain termination")?;
    assert_eq!(accounted_record_bytes(&publisher)?, 0);

    assert_eq!(
        termination.outcome(),
        &CaptureWriterOutcome::Incomplete {
            records_written: 1,
            reason: CaptureHealthReason::WriterFailed,
        }
    );
    assert!(termination.shutdown_deadline_elapsed());
    assert_eq!(termination.records_written_at_revocation(), 1);
    assert_eq!(termination.final_records_written(), 1);
    assert_eq!(termination.late_records_written(), 0);
    assert!(!receipt.generation_is_complete());
    Ok(())
}
