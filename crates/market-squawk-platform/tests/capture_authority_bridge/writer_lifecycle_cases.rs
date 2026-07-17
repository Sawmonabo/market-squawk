use super::*;

#[tokio::test]
async fn rotation_rejects_wrong_session_and_nonincreasing_whole_bundles()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(2)?;
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
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
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
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
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    Ok(())
}
