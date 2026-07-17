use super::*;

#[derive(Debug)]
struct FailingSink(CaptureDestination);

impl CaptureSink for FailingSink {
    fn destination(&self) -> CaptureDestination {
        self.0.clone()
    }

    fn append(
        &mut self,
        _record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        Err(CaptureSinkError::storage(
            CaptureStorageErrorClass::Unavailable,
        ))
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test]
async fn writer_failure_invalidates_an_already_issued_concrete_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(
        writer,
        FailingSink(CaptureDestination::try_named("authority-bridge-failing")?),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(identity, 1)?)?;

    assert!(
        shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    assert!(!receipt.is_healthy());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.queued_bytes(), 0);
    Ok(())
}

#[derive(Debug)]
struct RecordingSink {
    destination: CaptureDestination,
    sender: std::sync::mpsc::SyncSender<CapturedRawRecord>,
}

impl CaptureSink for RecordingSink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        self.sender
            .send(record.clone())
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test]
async fn writer_converts_exact_frame_to_bounded_diagnostic_record_without_authority()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(7)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        RecordingSink {
            destination: CaptureDestination::try_named("authority-bridge-recording")?,
            sender,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let expected = frame(identity.clone(), 41)?;
    let _receipt = publisher.try_publish(&expected)?;
    let captured = receiver.recv_timeout(Duration::from_secs(1))?;

    assert_eq!(captured.identity(), &identity);
    assert_eq!(captured.frame_ordinal().get(), 41);
    assert_eq!(captured.record().source(), "test-source");
    assert_eq!(captured.record().payload(), expected.payload());
    assert!(!captured.record().event_id().is_nil());
    assert!(!captured.record().connection_id().is_nil());

    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn retained_size_overflow_is_synchronous_and_terminal_before_enqueue()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let mut oversized = frame(identity, 1)?;
    oversized.retained_override = Some(usize::MAX);

    assert_eq!(
        publisher.try_publish(&oversized).err(),
        Some(CapturePublishError::RetainedSizeOverflow)
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.queued_bytes(), 0);
    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn transplanted_frame_is_rejected_without_poisoning_the_current_exact_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(2)?;
    let current = bundle.identity();
    let (publisher, mut control, writer) =
        raw_capture_channel(NonZeroUsize::new(2).ok_or("invalid test capacity")?, bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (old_bundle, _old_issued) = TestBundle::try_new(1)?;

    assert!(matches!(
        publisher.try_publish(&frame(old_bundle.identity(), 1)?),
        Err(CapturePublishError::Authority(
            CaptureAuthorityError::FrameBindingMismatch
        ))
    ));
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    assert!(publisher.try_publish(&frame(current, 1)?).is_ok());

    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn writer_and_positive_control_drop_each_fail_the_exact_generation_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let (publisher, control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    drop(writer);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    drop(control);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}

#[tokio::test]
async fn rotation_rejects_wrong_session_and_nonincreasing_whole_bundles()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(2)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (wrong_session, _wrong_issued) = TestBundle::try_new_for("test-source", "session-b", 3)?;
    assert!(matches!(
        control.rotate_generation(wrong_session),
        Err(market_squawk_platform::CaptureGenerationError::BindingMismatch { .. })
    ));
    let (not_newer, _not_newer_issued) = TestBundle::try_new(2)?;
    assert_eq!(
        control.rotate_generation(not_newer),
        Err(market_squawk_platform::CaptureGenerationError::NotNewer)
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[derive(Debug)]
struct SlowFlushSink(CaptureDestination);

impl CaptureSink for SlowFlushSink {
    fn destination(&self) -> CaptureDestination {
        self.0.clone()
    }

    fn append(
        &mut self,
        _record: &CapturedRawRecord,
        _context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        Ok(())
    }

    fn flush(&mut self, _context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_deadline_invalidates_authority_and_releases_queued_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(
        writer,
        SlowFlushSink(CaptureDestination::try_named(
            "authority-bridge-slow-flush",
        )?),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(identity, 1)?)?;

    assert!(
        shutdown_and_reap(handle, Duration::from_millis(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    assert!(!receipt.is_healthy());
    assert_eq!(publisher.queued_bytes(), 0);
    Ok(())
}
