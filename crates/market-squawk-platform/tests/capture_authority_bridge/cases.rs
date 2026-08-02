use super::*;

#[tokio::test]
async fn in_flight_old_publication_cannot_return_healthy_after_rotation_linearizes()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut first_bundle, _issued) = TestBundle::try_new(1)?;
    let first_identity = first_bundle.identity();
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    first_bundle.admission.issue_gate = Some(IssueGate {
        entered: entered_sender,
        release: Arc::new(std::sync::Mutex::new(release_receiver)),
    });
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        first_bundle,
    )?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let thread_publisher = publisher.try_clone()?;
    let publish = std::thread::spawn(move || {
        let publishing_frame = frame(first_identity, 1).map_err(|_error| {
            CapturePublishError::Authority(CaptureAuthorityError::FrameRejected)
        })?;
        thread_publisher.try_publish(&publishing_frame)
    });
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let (next_bundle, _next_issued) = TestBundle::try_new(2)?;
    control.rotate_generation(next_bundle)?;
    release_sender.send(())?;
    let publish_result = publish
        .join()
        .map_err(|_panic| "publisher thread panicked")?;
    assert!(matches!(
        publish_result,
        Err(CapturePublishError::Authority(
            CaptureAuthorityError::GenerationIncomplete
        ))
    ));
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn failed_rotation_degrades_the_uninstalled_next_bundle_only()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (wrong, _wrong_issued) = TestBundle::try_new_for("test-source", "wrong-session", 2)?;
    let wrong_health = wrong.degradation.clone();

    assert!(matches!(
        control.rotate_generation(wrong),
        Err(market_squawk_platform::CaptureGenerationError::BindingMismatch { .. })
    ));
    assert_eq!(wrong_health.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn preflight_lock_is_released_before_frame_clone_and_bounded_enqueue()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) =
        test_capture_channel(NonZeroUsize::new(4).ok_or("invalid test capacity")?, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let mut first = frame(identity.clone(), 1)?;
    first.clone_gate = Some(FrameCloneGate {
        entered: entered_sender,
        release: Arc::new(std::sync::Mutex::new(release_receiver)),
    });
    let thread_publisher = publisher.try_clone()?;
    let first_publish = std::thread::spawn(move || thread_publisher.try_publish(&first));
    entered_receiver.recv_timeout(Duration::from_secs(1))?;

    let second_receipt = publisher.try_publish(&frame(identity, 2)?)?;
    assert!(second_receipt.is_healthy());
    release_sender.send(())?;
    let first_result = first_publish
        .join()
        .map_err(|_panic| "publisher thread panicked")?;
    assert!(first_result.is_ok());

    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn successor_initializes_before_old_revocation_and_failed_init_preserves_old()
-> Result<(), Box<dyn std::error::Error>> {
    let (first_bundle, _issued) = TestBundle::try_new(1)?;
    let first_identity = first_bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        first_bundle,
    )?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let old_receipt = publisher.try_publish(&frame(first_identity, 1)?)?;

    let (mut successor, _successor_issued) = TestBundle::try_new(2)?;
    successor.initializer.required_healthy = Some(Arc::clone(&old_receipt.state));
    control.rotate_generation(successor)?;
    assert!(!old_receipt.is_healthy());
    let second_identity = publisher.identity();
    let current_receipt = publisher.try_publish(&frame(second_identity.as_ref().clone(), 1)?)?;

    let (failed, _failed_issued) = TestBundle::try_new(3)?;
    failed.degradation.mark_incomplete();
    assert!(matches!(
        control.rotate_generation(failed),
        Err(market_squawk_platform::CaptureGenerationError::Authority(
            CaptureAuthorityError::GenerationIncomplete
        ))
    ));
    assert!(current_receipt.is_healthy());
    assert_eq!(publisher.identity().connection_generation().get(), 2);

    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}

#[tokio::test]
async fn writer_stop_serializes_with_rotation_and_cannot_leave_successor_healthy()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut first_bundle, _issued) = TestBundle::try_new(1)?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    first_bundle.degradation.gate = Some(DegradationGate {
        entered: entered_sender,
        release: Arc::new(std::sync::Mutex::new(release_receiver)),
        used: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, first_bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let (successor, _successor_issued) = TestBundle::try_new(2)?;
    let successor_health = successor.degradation.clone();
    let rotation = std::thread::spawn(move || {
        let result = control.rotate_generation(successor);
        (control, result)
    });
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let stop = std::thread::spawn(move || drop(handle));
    release_sender.send(())?;
    let (control, rotation_result) = rotation
        .join()
        .map_err(|_panic| "rotation thread panicked")?;
    rotation_result?;
    stop.join()
        .map_err(|_panic| "writer stop thread panicked")?;

    assert_eq!(
        successor_health.integrity(),
        CaptureIntegrityState::Incomplete
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    drop(control);
    Ok(())
}
