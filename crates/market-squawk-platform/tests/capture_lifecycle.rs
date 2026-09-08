use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityIdentity, CaptureIntegrityState, ConnectionGeneration, MetadataRevision,
    RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    CaptureChannelLimits, CaptureDestination, CaptureGenerationError, CaptureHealthReason,
    CaptureIoContext, CaptureProcessInfrastructureLimits, CaptureShutdownStatus, CaptureSink,
    CaptureSinkError, CaptureStorageErrorClass, CaptureWorkerReapError, CaptureWorkerTermination,
    CaptureWriterHandle, CaptureWriterOutcome, CaptureWriterPolicy, CaptureWriterSpawnError,
    CapturedRawRecord, DiagnosticCaptureBundle, DiagnosticCaptureFrame, DiagnosticCaptureReceipt,
    LocalPaths, MemoryCaptureSink, ProcessCaptureHelperTestBehavior,
    ProcessCaptureShutdownDisposition, ProcessCaptureShutdownPolicy,
    ProcessCaptureWriterSpawnError, ProcessJournalCaptureConfig, RawCaptureChannel,
    RawCaptureControl, RawCapturePublisher, initialize_capture_process_infrastructure,
    raw_capture_channel, spawn_capture_writer, spawn_process_journal_capture_writer,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(RawCapturePublisher<DiagnosticCaptureBundle>: Send, Sync);
assert_not_impl_any!(RawCapturePublisher<DiagnosticCaptureBundle>: Clone);
assert_not_impl_any!(RawCaptureControl<DiagnosticCaptureBundle>: Clone);
assert_not_impl_any!(DiagnosticCaptureBundle: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(DiagnosticCaptureReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);

const TEST_CAPTURE_MEMORY_CEILING_BYTES: usize = 64 * 1024 * 1024;
const TEST_DESTINATION_REGISTRY_CEILING_BYTES: usize = 1024 * 1024;
const TEST_MEMORY_SINK_MAX_RECORDS: usize = 4_096;
const TEST_MEMORY_SINK_RETAINED_CEILING_BYTES: usize = 64 * 1024 * 1024;

fn test_memory_capture_sink() -> Result<MemoryCaptureSink, Box<dyn std::error::Error>> {
    Ok(MemoryCaptureSink::try_new(
        NonZeroUsize::new(TEST_MEMORY_SINK_MAX_RECORDS).ok_or("invalid test sink record limit")?,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES)
            .ok_or("invalid test sink retained-byte ceiling")?,
    )?)
}

fn test_capture_channel(
    capacity: NonZeroUsize,
    bundle: DiagnosticCaptureBundle,
) -> Result<RawCaptureChannel<DiagnosticCaptureBundle>, Box<dyn std::error::Error>> {
    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            NonZeroUsize::new(TEST_DESTINATION_REGISTRY_CEILING_BYTES).unwrap_or(NonZeroUsize::MIN),
        ))?;
    Ok(raw_capture_channel(
        &process,
        CaptureChannelLimits::new(
            capacity,
            NonZeroUsize::new(TEST_CAPTURE_MEMORY_CEILING_BYTES).unwrap_or(NonZeroUsize::MIN),
        ),
        bundle,
    )?)
}

fn accounted_record_bytes(
    publisher: &RawCapturePublisher<DiagnosticCaptureBundle>,
) -> Result<usize, market_squawk_platform::CaptureAccountingSnapshotError> {
    publisher
        .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
        .map(market_squawk_platform::CaptureAccountingSnapshot::record_reservation_bytes)
}

fn identity(generation: u64) -> Result<CaptureAuthorityIdentity, Box<dyn std::error::Error>> {
    Ok(CaptureAuthorityIdentity::new(
        SourceId::try_from("diagnostic-source")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
        SourceIdentifier::try_from("session-a")?,
        ConnectionGeneration::new(generation)?,
    ))
}

fn frame(
    identity: CaptureAuthorityIdentity,
    ordinal: u64,
) -> Result<DiagnosticCaptureFrame, Box<dyn std::error::Error>> {
    Ok(DiagnosticCaptureFrame::try_new(
        identity,
        NonZeroU64::new(ordinal).ok_or("test ordinal must be nonzero")?,
        Timestamp::from_unix_nanos(i64::try_from(ordinal)?),
        Bytes::from(vec![7_u8; 256]),
    )?)
}

async fn shutdown_and_reap(
    handle: CaptureWriterHandle<DiagnosticCaptureBundle>,
    deadline: Duration,
) -> Result<CaptureWorkerTermination, Box<dyn std::error::Error>> {
    let mut pending = handle.shutdown(deadline);
    if pending.wait_until_deadline().await != CaptureShutdownStatus::WorkerTerminated {
        return Err("capture worker exceeded the fixed test deadline".into());
    }
    pending
        .try_reap()?
        .cloned()
        .ok_or_else(|| "terminated capture worker did not retain a final report".into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_deadline_is_reported_while_an_admitted_publisher_clone_is_paused()
-> Result<(), Box<dyn std::error::Error>> {
    let deadline_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;

    let publisher = Arc::new(publisher);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let clone_worker = {
        let publisher = Arc::clone(&publisher);
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        std::thread::spawn(move || {
            publisher.try_clone_after_registration_paused_for_test(&entered, &release)
        })
    };
    entered.wait();

    let (shutdown_sender, shutdown_receiver) = std::sync::mpsc::sync_channel(1);
    let shutdown_worker = std::thread::spawn(move || {
        let pending = handle.shutdown(Duration::from_millis(25));
        let _sent = shutdown_sender.send(pending);
    });
    let pending = match shutdown_receiver.recv_timeout(Duration::from_secs(1)) {
        Ok(pending) => pending,
        Err(error) => {
            release.wait();
            let clone_result = clone_worker.join();
            let shutdown_result = shutdown_worker.join();
            drop(shutdown_receiver);
            let clone = clone_result.map_err(|_panic| "publisher clone worker panicked")??;
            drop(clone);
            shutdown_result.map_err(|_panic| "shutdown worker panicked")?;
            return Err(format!("shutdown did not return before clone release: {error}").into());
        }
    };

    let (deadline_sender, deadline_receiver) = std::sync::mpsc::sync_channel(1);
    let deadline_worker = std::thread::spawn(move || {
        let mut pending = pending;
        let status = deadline_runtime.block_on(pending.wait_until_deadline());
        let _sent = deadline_sender.send((status, pending));
    });
    let mut observed = deadline_receiver.recv_timeout(Duration::from_secs(1));
    let reported_before_release = observed.is_ok();
    let (storage_retained, worker_retained) = match &mut observed {
        Ok((_status, pending)) => (
            pending.fixed_storage_receipt().is_some(),
            matches!(
                pending.try_reap(),
                Err(CaptureWorkerReapError::WorkerStillRunning)
            ),
        ),
        Err(_error) => (false, false),
    };
    release.wait();
    let deadline_result = match observed {
        Ok(result) => Ok(result),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => deadline_receiver
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_error| "deadline wait did not return after clone release"),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err("deadline result channel disconnected")
        }
    };
    let clone_result = clone_worker.join();
    let shutdown_result = shutdown_worker.join();
    let deadline_join_result = deadline_worker.join();
    let clone = clone_result.map_err(|_panic| "publisher clone worker panicked")??;
    drop(clone);
    shutdown_result.map_err(|_panic| "shutdown worker panicked")?;
    deadline_join_result.map_err(|_panic| "deadline worker panicked")?;
    let (status, mut pending) = deadline_result?;
    pending.wait_until_terminated().await;
    let _termination = pending
        .try_reap()?
        .ok_or("terminated writer omitted its final report")?;

    assert!(
        reported_before_release,
        "deadline reporting waited for an already-admitted publisher clone"
    );
    assert_eq!(status, CaptureShutdownStatus::DeadlineElapsed);
    assert!(
        storage_retained,
        "deadline reporting released pending writer fixed storage"
    );
    assert!(
        worker_retained,
        "deadline reporting lost ownership of the running capture worker"
    );
    Ok(())
}

#[tokio::test]
async fn activation_before_writer_start_is_retryable_after_writer_start()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (_publisher, mut control, writer) =
        test_capture_channel(NonZeroUsize::MIN, DiagnosticCaptureBundle::new(identity))?;
    assert_eq!(
        control.activate_initial(),
        Err(CaptureGenerationError::WriterUnavailable)
    );
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    assert!(!termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn worker_terminated_before_deadline_is_not_retroactively_timed_out_at_late_reap()
-> Result<(), Box<dyn std::error::Error>> {
    let (_publisher, _control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    let mut pending = handle.shutdown(Duration::from_millis(100));
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::WorkerTerminated
    );
    tokio::time::sleep(Duration::from_millis(110)).await;
    let termination = pending
        .try_reap()?
        .ok_or("finished worker did not retain termination after delayed reap")?;

    assert!(!termination.shutdown_deadline_elapsed());
    assert!(!termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn natural_writer_completion_degrades_every_previously_issued_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity.clone()),
    )?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let exact_frame = frame(identity, 1)?;
    let receipt = publisher.try_publish(&exact_frame)?;
    drop(publisher);

    let termination = tokio::time::timeout(
        Duration::from_secs(1),
        shutdown_and_reap(handle, Duration::from_secs(1)),
    )
    .await??;
    assert!(!termination.outcome().is_incomplete());
    assert!(!receipt.generation_is_complete());
    drop(control);
    Ok(())
}

#[derive(Debug)]
struct GatedFailingSink {
    destination: CaptureDestination,
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl CaptureSink for GatedFailingSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        _record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        self.entered
            .send(())
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        self.release
            .recv()
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        Err(CaptureSinkError::storage(
            CaptureStorageErrorClass::Unavailable,
        ))
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test]
async fn old_queued_frame_failure_degrades_the_current_writer_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let first = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        DiagnosticCaptureBundle::new(first.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedFailingSink {
            destination: CaptureDestination::try_named("gated-failing-sink")?,
            entered: entered_sender,
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let old_frame = frame(first, 1)?;
    let old_receipt = publisher.try_publish(&old_frame)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    control.rotate_generation(DiagnosticCaptureBundle::new(identity(2)?))?;
    assert!(!old_receipt.generation_is_complete());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);

    release_sender.send(())?;
    assert!(
        shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}

#[derive(Debug)]
struct GatedSink {
    destination: CaptureDestination,
    entered: Option<std::sync::mpsc::SyncSender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl CaptureSink for GatedSink {
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
            self.release
                .recv()
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        Ok(())
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_sink_does_not_stall_tokio_and_releases_writer_owned_reservation_after_append()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(3).ok_or("invalid test capacity")?,
        DiagnosticCaptureBundle::new(identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            destination: CaptureDestination::try_named("gated-sink")?,
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let first = frame(identity.clone(), 1)?;
    let second = frame(identity.clone(), 2)?;
    let third = frame(identity, 3)?;
    let _first_receipt = publisher.try_publish(&first)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let one_frame_charge = accounted_record_bytes(&publisher)?;
    assert!(one_frame_charge > first.payload().len());
    tokio::time::timeout(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_millis(1)),
    )
    .await?;
    let _second_receipt = publisher.try_publish(&second)?;
    assert_eq!(
        accounted_record_bytes(&publisher)?,
        one_frame_charge.saturating_mul(2)
    );
    let _third_receipt = publisher.try_publish(&third)?;
    assert_eq!(
        accounted_record_bytes(&publisher)?,
        one_frame_charge.saturating_mul(3)
    );

    let (drop_complete_sender, drop_complete_receiver) = std::sync::mpsc::sync_channel(1);
    let drop_thread = std::thread::spawn(move || {
        drop(handle);
        let _sent = drop_complete_sender.send(());
    });
    let queue_release_deadline = std::time::Instant::now() + Duration::from_secs(1);
    while accounted_record_bytes(&publisher)? != one_frame_charge
        && std::time::Instant::now() < queue_release_deadline
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(accounted_record_bytes(&publisher)?, one_frame_charge);
    assert!(matches!(
        drop_complete_receiver.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    release_sender.send(())?;
    drop_complete_receiver.recv_timeout(Duration::from_secs(1))?;
    drop_thread
        .join()
        .map_err(|_panic| "capture handle drop thread panicked")?;
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    drop(control);
    Ok(())
}

#[derive(Debug)]
struct DestinationGatedSink {
    destination: CaptureDestination,
    entered: Option<std::sync::mpsc::SyncSender<()>>,
    release: Option<std::sync::mpsc::Receiver<()>>,
}

impl CaptureSink for DestinationGatedSink {
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
        Ok(())
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn destination_fence_rejects_concurrent_independent_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let destination_label = "capture-lifecycle-shared-destination";
    let destination = CaptureDestination::try_named(destination_label)?;
    let first_identity = identity(1)?;
    let (first_publisher, mut first_control, first_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(first_identity.clone()),
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let first_handle = spawn_capture_writer(
        first_writer,
        DestinationGatedSink {
            destination: destination.clone(),
            entered: Some(entered_sender),
            release: Some(release_receiver),
        },
        CaptureWriterPolicy::default(),
    )?;
    first_control.activate_initial()?;
    let first_frame = frame(first_identity, 1)?;
    let _first_receipt = first_publisher.try_publish(&first_frame)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;

    let (_second_publisher, _second_control, second_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    assert!(matches!(
        spawn_capture_writer(
            second_writer,
            DestinationGatedSink {
                destination: CaptureDestination::try_named(destination_label)?,
                entered: None,
                release: None,
            },
            CaptureWriterPolicy::default(),
        ),
        Err(CaptureWriterSpawnError::DestinationFence {
            source: market_squawk_platform::CaptureDestinationFenceError::Busy,
            ..
        })
    ));

    release_sender.send(())?;
    drop(first_publisher);
    assert!(
        !shutdown_and_reap(first_handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );

    let (_third_publisher, _third_control, third_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let third_handle = spawn_capture_writer(
        third_writer,
        DestinationGatedSink {
            destination: CaptureDestination::try_named(destination_label)?,
            entered: None,
            release: None,
        },
        CaptureWriterPolicy::default(),
    )?;
    let third_termination = shutdown_and_reap(third_handle, Duration::from_secs(1)).await?;
    assert!(!third_termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn finished_unreaped_journal_destination_remains_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let first_sink = paths.open_journal_writer("capture-lifecycle")?;
    let destination = first_sink.destination();
    let (_first_publisher, _first_control, first_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let first_handle =
        spawn_capture_writer(first_writer, first_sink, CaptureWriterPolicy::default())?;
    let mut pending = first_handle.shutdown(Duration::from_secs(5));
    assert_eq!(
        pending.wait_until_deadline().await,
        CaptureShutdownStatus::WorkerTerminated
    );
    assert!(pending.is_worker_terminated());

    // An unreaped owner must retain the journal's exact in-process destination fence even while
    // the OS thread is in its final teardown window.
    let (_blocked_publisher, _blocked_control, blocked_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    assert!(matches!(
        spawn_capture_writer(
            blocked_writer,
            DestinationGatedSink {
                destination,
                entered: None,
                release: None,
            },
            CaptureWriterPolicy::default(),
        ),
        Err(CaptureWriterSpawnError::DestinationFence {
            source: market_squawk_platform::CaptureDestinationFenceError::Busy,
            ..
        })
    ));

    let first_termination = pending
        .try_reap()?
        .ok_or("finished journal worker did not retain termination")?;
    assert!(!first_termination.outcome().is_incomplete());
    let successor_sink = paths.open_journal_writer("capture-lifecycle")?;
    let (_successor_publisher, _successor_control, successor_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let successor = spawn_capture_writer(
        successor_writer,
        successor_sink,
        CaptureWriterPolicy::default(),
    )?;
    let successor_termination = shutdown_and_reap(successor, Duration::from_secs(5)).await?;
    assert!(!successor_termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_process_journal_is_killed_reaped_and_releases_its_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let source = "capture-lifecycle-process";
    let generation = identity(1)?;
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(generation.clone()),
    )?;
    let process = ProcessJournalCaptureConfig::try_new_for_test(
        paths.root(),
        source,
        env!("CARGO_BIN_EXE_market-squawk-platform-capture-helper-test"),
        ProcessCaptureHelperTestBehavior::StallAfterAppend,
        Duration::from_secs(5),
    )?;
    let handle =
        spawn_process_journal_capture_writer(writer, process, CaptureWriterPolicy::default())?;
    control.activate_initial()?;
    let _receipt = publisher.try_publish(&frame(generation, 1)?)?;

    let shutdown = handle
        .shutdown(ProcessCaptureShutdownPolicy::try_new(
            Duration::from_millis(25),
            Duration::from_secs(5),
        )?)
        .await;

    assert_eq!(
        shutdown.disposition(),
        ProcessCaptureShutdownDisposition::HelperKilled
    );
    assert!(shutdown.helper_reaped());
    let worker = shutdown
        .worker_termination()
        .ok_or("killed helper did not yield a capture-worker termination")?;
    assert!(worker.shutdown_deadline_elapsed());
    assert!(worker.outcome().is_incomplete());
    drop(control);
    drop(publisher);

    let successor = paths.open_journal_writer(source)?;
    drop(successor);
    Ok(())
}

#[tokio::test]
async fn post_fence_startup_failure_retains_destination_until_helper_reap()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let source = "capture-lifecycle-startup-rollback";
    let (publisher, control, writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let process = ProcessJournalCaptureConfig::try_new_for_test(
        paths.root(),
        source,
        env!("CARGO_BIN_EXE_market-squawk-platform-capture-helper-test"),
        ProcessCaptureHelperTestBehavior::FailAfterDestinationFence {
            cleanup_deadline: Duration::from_millis(25),
            reap_observation_delay: Duration::from_millis(250),
        },
        Duration::from_secs(5),
    )?;

    let error =
        match spawn_process_journal_capture_writer(writer, process, CaptureWriterPolicy::default())
        {
            Ok(_writer) => {
                return Err(
                    "injected post-fence startup failure unexpectedly returned a writer".into(),
                );
            }
            Err(error) => error,
        };

    let ProcessCaptureWriterSpawnError::InjectedPostFenceFailure { rollback_elapsed } = error
    else {
        return Err(format!("unexpected startup error: {error:?}").into());
    };
    assert!(rollback_elapsed < Duration::from_millis(150));
    drop(control);
    drop(publisher);

    let (_busy_publisher, _busy_control, busy_writer) = test_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity(1)?),
    )?;
    let busy_process = ProcessJournalCaptureConfig::try_new_for_test(
        paths.root(),
        source,
        env!("CARGO_BIN_EXE_market-squawk-platform-capture-helper-test"),
        ProcessCaptureHelperTestBehavior::StallAfterAppend,
        Duration::from_secs(5),
    )?;
    assert!(matches!(
        spawn_process_journal_capture_writer(
            busy_writer,
            busy_process,
            CaptureWriterPolicy::default(),
        ),
        Err(ProcessCaptureWriterSpawnError::CaptureWriter(
            CaptureWriterSpawnError::DestinationFence {
                source: market_squawk_platform::CaptureDestinationFenceError::Busy,
                ..
            }
        ))
    ));

    let reacquisition_deadline = Instant::now() + Duration::from_secs(5);
    let (_successor_publisher, _successor_control, successor) = loop {
        let (publisher, control, writer) = test_capture_channel(
            NonZeroUsize::MIN,
            DiagnosticCaptureBundle::new(identity(1)?),
        )?;
        let process = ProcessJournalCaptureConfig::try_new_for_test(
            paths.root(),
            source,
            env!("CARGO_BIN_EXE_market-squawk-platform-capture-helper-test"),
            ProcessCaptureHelperTestBehavior::StallAfterAppend,
            Duration::from_secs(5),
        )?;
        match spawn_process_journal_capture_writer(writer, process, CaptureWriterPolicy::default())
        {
            Ok(successor) => break (publisher, control, successor),
            Err(ProcessCaptureWriterSpawnError::CaptureWriter(
                CaptureWriterSpawnError::DestinationFence {
                    source: market_squawk_platform::CaptureDestinationFenceError::Busy,
                    ..
                },
            )) if Instant::now() < reacquisition_deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let shutdown = successor
        .shutdown(ProcessCaptureShutdownPolicy::try_new(
            Duration::from_secs(5),
            Duration::from_secs(5),
        )?)
        .await;
    assert_eq!(
        shutdown.disposition(),
        ProcessCaptureShutdownDisposition::Complete
    );
    assert!(shutdown.helper_reaped());
    Ok(())
}

#[path = "capture_lifecycle/deadline_cases.rs"]
mod deadline_cases;
