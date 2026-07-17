use super::*;

#[tokio::test]
async fn concrete_associated_receipt_is_issued_only_after_bounded_enqueue()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;

    let receipt = publisher.try_publish(&frame(identity, 1)?)?;
    assert_eq!(receipt.ordinal.get(), 1);
    assert!(receipt.is_healthy());
    assert_eq!(issued.load(Ordering::Acquire), 1);

    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    assert!(!termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn publisher_validates_and_issues_from_the_one_exact_clone_it_enqueues()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let clone_count = Arc::new(AtomicU64::new(0));
    let mut original = frame(identity, 1)?;
    original.clone_count = Some(Arc::clone(&clone_count));
    original.clone_mutation = Some(FrameCloneMutation {
        ordinal: Some(NonZeroU64::new(9).ok_or("clone ordinal must be nonzero")?),
        received_at: Some(Timestamp::from_unix_nanos(99)),
        payload: Some(CapturePayload::try_from_live(b"detached-clone-payload")?),
        ..FrameCloneMutation::default()
    });

    let receipt = publisher.try_publish(&original)?;
    assert_eq!(clone_count.load(Ordering::Acquire), 1);
    assert_eq!(receipt.ordinal.get(), 9);
    assert_eq!(receipt.received_at, Timestamp::from_unix_nanos(99));
    assert_eq!(receipt.payload_length, b"detached-clone-payload".len());
    assert_eq!(receipt.payload_first_byte, Some(b'd'));
    assert_eq!(issued.load(Ordering::Acquire), 1);

    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    assert!(!termination.outcome().is_incomplete());
    Ok(())
}

#[tokio::test]
async fn publisher_rejects_an_identity_changed_by_the_one_enqueued_clone()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (foreign_bundle, _foreign_issued) = TestBundle::try_new_for("foreign", "session-b", 2)?;
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let clone_count = Arc::new(AtomicU64::new(0));
    let mut original = frame(identity, 1)?;
    original.clone_count = Some(Arc::clone(&clone_count));
    original.clone_mutation = Some(FrameCloneMutation {
        identity: Some(foreign_bundle.identity()),
        ..FrameCloneMutation::default()
    });

    assert_eq!(
        publisher.try_publish(&original).err(),
        Some(CapturePublishError::Authority(
            CaptureAuthorityError::FrameBindingMismatch
        ))
    );
    assert_eq!(clone_count.load(Ordering::Acquire), 1);
    assert_eq!(issued.load(Ordering::Acquire), 0);

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn publisher_rejects_retained_bytes_underreported_only_by_the_enqueued_clone()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let clone_count = Arc::new(AtomicU64::new(0));
    let mut original = frame(identity, 1)?;
    original.clone_count = Some(Arc::clone(&clone_count));
    original.clone_mutation = Some(FrameCloneMutation {
        retained_override: Some(0),
        ..FrameCloneMutation::default()
    });

    assert_eq!(
        publisher.try_publish(&original).err(),
        Some(CapturePublishError::RetainedSizeUnderreported)
    );
    assert_eq!(clone_count.load(Ordering::Acquire), 1);
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn receipt_method_reentry_runs_after_the_admission_guard_is_released()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bundle, issued) = TestBundle::try_new(1)?;
    let probe = Arc::new(ReentryProbe::default());
    bundle.admission.receipt_method_probe = Some(Arc::clone(&probe));
    let identity = bundle.identity();
    let capacity = NonZeroUsize::new(4).ok_or("invalid queue capacity")?;
    let (publisher, mut control, writer) = test_capture_channel(capacity, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    probe.install(publisher.try_clone()?, frame(identity.clone(), 2)?)?;

    let receipt = publisher.try_publish(&frame(identity, 1)?)?;
    assert!(receipt.is_healthy());
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    let observed = probe.observed_error();
    assert!(matches!(
        observed,
        None | Some(CapturePublishError::QueueContended)
    ));
    assert_eq!(
        issued.load(Ordering::Acquire),
        if observed.is_none() { 2 } else { 1 }
    );

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn rejected_receipt_drop_reentry_runs_after_the_admission_guard_is_released()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bundle, issued) = TestBundle::try_new(1)?;
    let probe = Arc::new(ReentryProbe::default());
    bundle.admission.receipt_override = ReceiptOverride::SubstituteResident;
    bundle.admission.receipt_drop_probe = Some(Arc::clone(&probe));
    let identity = bundle.identity();
    let capacity = NonZeroUsize::new(4).ok_or("invalid queue capacity")?;
    let (publisher, mut control, writer) = test_capture_channel(capacity, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    probe.install(publisher.try_clone()?, frame(identity.clone(), 2)?)?;

    assert!(matches!(
        publisher.try_publish(&frame(identity, 1)?),
        Err(CapturePublishError::RetainedSize(_))
    ));
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        probe.observed_error(),
        Some(CapturePublishError::WriterUnavailable)
    );
    assert_eq!(issued.load(Ordering::Acquire), 1);

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn degradation_reentry_runs_after_the_admission_guard_is_released()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut bundle, issued) = TestBundle::try_new(1)?;
    let probe = Arc::new(ReentryProbe::default());
    bundle.degradation.reentry_probe = Some(Arc::clone(&probe));
    let identity = bundle.identity();
    let capacity = NonZeroUsize::new(4).ok_or("invalid queue capacity")?;
    let (publisher, mut control, writer) = test_capture_channel(capacity, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    probe.install(publisher.try_clone()?, frame(identity.clone(), 2)?)?;
    let mut underreported = frame(identity, 1)?;
    underreported.retained_override = Some(0);

    assert_eq!(
        publisher.try_publish(&underreported).err(),
        Some(CapturePublishError::RetainedSizeUnderreported)
    );
    assert_eq!(probe.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        probe.observed_error(),
        Some(CapturePublishError::Authority(
            CaptureAuthorityError::GenerationIncomplete
        ))
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

async fn assert_receipt_override_fails_closed(
    receipt_override: ReceiptOverride,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut bundle, issued) = TestBundle::try_new(1)?;
    bundle.admission.receipt_override = receipt_override;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;

    assert_eq!(
        publisher.try_publish(&frame(identity, 1)?).err(),
        Some(CapturePublishError::RetainedSize(
            CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::CaptureLease,
            },
        ))
    );
    assert_eq!(issued.load(Ordering::Acquire), 1);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);

    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn substituted_receipt_resident_token_is_rejected_terminally()
-> Result<(), Box<dyn std::error::Error>> {
    assert_receipt_override_fails_closed(ReceiptOverride::SubstituteResident).await
}

#[tokio::test]
async fn unreserved_receipt_dynamic_allocation_is_rejected_terminally()
-> Result<(), Box<dyn std::error::Error>> {
    assert_receipt_override_fails_closed(ReceiptOverride::NonzeroDynamic).await
}

#[tokio::test]
async fn generic_frame_cannot_publish_a_compatibility_only_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let actual = MAX_LIVE_CAPTURE_PAYLOAD_BYTES + 1;
    let oversized = TestFrame {
        identity,
        ordinal: NonZeroU64::MIN,
        received_at: Timestamp::from_unix_nanos(1),
        payload: CapturePayload::try_from_committed_wire(&vec![0_u8; actual])?,
        payload_view_override: None,
        retained_override: None,
        clone_gate: None,
        clone_mutation: None,
        clone_count: None,
    };

    assert_eq!(
        publisher.try_publish(&oversized).err(),
        Some(CapturePublishError::PayloadTooLarge {
            actual,
            maximum: MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
        })
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn footprint_cannot_underreport_the_owned_payload_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let mut underreported = frame(identity, 1)?;
    underreported.retained_override = Some(0);

    assert_eq!(
        publisher.try_publish(&underreported).err(),
        Some(CapturePublishError::RetainedSizeUnderreported)
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

async fn assert_substituted_borrowed_payload_view_is_rejected(
    replacement: Arc<[u8]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let mut substituted = frame(identity, 1)?;
    substituted.payload_view_override = Some(replacement);

    assert_eq!(
        publisher.try_publish(&substituted).err(),
        Some(CapturePublishError::InvalidPayloadView)
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let _termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn equal_bytes_from_a_different_payload_allocation_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    assert_substituted_borrowed_payload_view_is_rejected(Arc::from(b"frame-1".as_slice())).await
}

#[tokio::test]
async fn different_bytes_from_a_different_payload_allocation_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    assert_substituted_borrowed_payload_view_is_rejected(Arc::from(b"wrong-1".as_slice())).await
}

#[test]
fn diagnostic_bundle_counts_both_distinct_identity_graphs() -> Result<(), Box<dyn std::error::Error>>
{
    let identity = CaptureAuthorityIdentity::new(
        SourceId::try_from("diagnostic-source")?,
        MetadataRevision::new(SourceIdentifier::try_from("diagnostic-revision")?),
        SourceIdentifier::try_from("diagnostic-session")?,
        ConnectionGeneration::new(1)?,
    );
    let identity_bytes = identity.checked_dynamic_retained_bytes()?;
    let state_bytes = checked_arc_value_allocation_bytes::<AtomicU8>(0)?;
    let expected = std::mem::size_of::<DiagnosticCaptureBundle>()
        .checked_add(identity_bytes)
        .and_then(|bytes| bytes.checked_add(identity_bytes))
        .and_then(|bytes| bytes.checked_add(state_bytes))
        .ok_or("diagnostic retained fixture overflowed")?;
    let bundle = DiagnosticCaptureBundle::new(identity);
    assert_eq!(bundle.checked_retained_bytes()?, expected);
    Ok(())
}

#[tokio::test]
async fn capture_writer_start_reserves_every_named_term_until_final_reap()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let before =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    let receipt = handle
        .fixed_storage_receipt()
        .ok_or("running writer did not retain its fixed-storage receipt")?;
    let reconstructed = [
        receipt.source_scratch_bytes(),
        receipt.generation_scratch_bytes(),
        receipt.event_scratch_bytes(),
        receipt.destination_lease_bytes(),
        receipt.owner_allocation_bytes(),
        receipt.thread_name_bytes(),
        receipt.spawn_packet_bytes(),
        receipt.private_runtime_bytes(),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)
    .ok_or("writer fixed-storage receipt overflowed in reconstruction")?;
    assert_eq!(reconstructed, receipt.total_bytes());
    assert!(receipt.source_scratch_bytes() > 0);
    assert!(receipt.generation_scratch_bytes() > 0);
    assert!(receipt.event_scratch_bytes() > 0);
    assert!(receipt.destination_lease_bytes() > 0);
    assert!(receipt.owner_allocation_bytes() > 0);
    assert!(receipt.thread_name_bytes() > 0);
    assert!(receipt.spawn_packet_bytes() > 0);
    assert!(receipt.private_runtime_bytes() > 0);
    assert_eq!(receipt.formula_revision(), 1);
    assert_ne!(receipt.artifact_sha256(), [0; 32]);
    assert!(!receipt.compiled_target().is_empty());
    let running =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    assert_eq!(
        running.fixed_capture_bytes(),
        before
            .fixed_capture_bytes()
            .checked_add(receipt.total_bytes())
            .ok_or("writer fixed-storage snapshot overflowed")?
    );

    control.activate_initial()?;
    let mut pending = handle.shutdown(Duration::from_secs(1));
    let _status = pending.wait_until_deadline().await;
    if !pending.is_worker_terminated() {
        pending.wait_until_terminated().await;
    }
    let before_reap =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    assert_eq!(
        before_reap.fixed_capture_bytes(),
        running.fixed_capture_bytes()
    );
    assert!(pending.try_reap()?.is_some());
    assert_eq!(pending.fixed_storage_receipt(), None);
    let reaped =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    assert_eq!(reaped.fixed_capture_bytes(), before.fixed_capture_bytes());
    Ok(())
}

#[test]
fn capture_writer_start_budget_refusal_releases_fixed_storage_and_cannot_activate()
-> Result<(), Box<dyn std::error::Error>> {
    let (first_bundle, _issued) = TestBundle::try_new(1)?;
    let (first_publisher, first_control, first_writer) =
        test_capture_channel(NonZeroUsize::MIN, first_bundle)?;
    let required_without_writer = first_publisher
        .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?
        .total_accounted_bytes();
    drop(first_writer);
    drop(first_control);
    drop(first_publisher);

    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            NonZeroUsize::new(TEST_DESTINATION_REGISTRY_CEILING_BYTES).unwrap_or(NonZeroUsize::MIN),
        ))?;
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let (publisher, mut control, writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(
            NonZeroUsize::MIN,
            NonZeroUsize::new(required_without_writer)
                .ok_or("initial capture graph must retain nonzero storage")?,
        ),
        bundle,
    )?;
    let before =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    let result = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    );
    assert!(matches!(
        result,
        Err(CaptureWriterSpawnError::FixedStorageBudgetExceeded {
            required,
            limit
        }) if required > limit && limit == required_without_writer
    ));
    let after =
        publisher.try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))?;
    assert_eq!(after, before);
    assert!(matches!(
        control.activate_initial(),
        Err(CaptureGenerationError::WriterUnavailable)
    ));
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

#[tokio::test]
async fn queue_saturation_degrades_exact_generation_without_issuing_a_third_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            destination: CaptureDestination::try_named("authority-bridge-gated")?,
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let first = publisher.try_publish(&frame(identity.clone(), 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let second = publisher.try_publish(&frame(identity.clone(), 2)?)?;
    assert!(matches!(
        publisher.try_publish(&frame(identity, 3)?),
        Err(CapturePublishError::QueueFull)
    ));
    assert_eq!(issued.load(Ordering::Acquire), 2);
    assert!(!first.is_healthy());
    assert!(!second.is_healthy());

    release_sender.send(())?;
    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    Ok(())
}

#[tokio::test]
async fn whole_bundle_rotation_invalidates_old_receipt_and_accepts_only_new_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let (first_bundle, _first_issued) = TestBundle::try_new(1)?;
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
    let old_receipt = publisher.try_publish(&frame(first_identity.clone(), 1)?)?;

    let (second_bundle, _second_issued) = TestBundle::try_new(2)?;
    let second_identity = second_bundle.identity();
    control.rotate_generation(second_bundle)?;
    assert!(!old_receipt.is_healthy());
    assert!(matches!(
        publisher.try_publish(&frame(first_identity, 2)?),
        Err(CapturePublishError::Authority(_))
    ));
    let current = publisher.try_publish(&frame(second_identity, 1)?)?;
    assert!(current.is_healthy());

    assert!(
        !shutdown_and_reap(handle, Duration::from_secs(1))
            .await?
            .outcome()
            .is_incomplete()
    );
    Ok(())
}
