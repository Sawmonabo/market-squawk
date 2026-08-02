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
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
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
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    Ok(())
}

#[derive(Debug)]
struct RecordingSink {
    destination: CaptureDestination,
    sender: std::sync::mpsc::SyncSender<CapturedRawRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservedMemorySinkError {
    RecordLimit { limit: usize },
    RetainedByteLimit { required: usize, limit: usize },
    InvalidPayloadSharing,
    AccountingInvariant,
}

#[derive(Debug)]
struct InspectableMemorySink {
    destination: CaptureDestination,
    inner: Arc<std::sync::Mutex<MemoryCaptureSink>>,
    first_append_gate: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
    observed_error: Arc<std::sync::Mutex<Option<ObservedMemorySinkError>>>,
}

impl CaptureSink for InspectableMemorySink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        if let Some((entered, release)) = self.first_append_gate.take() {
            entered
                .send(())
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
            release
                .recv()
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        let result = match self.inner.lock() {
            Ok(mut inner) => inner.append(record, context),
            Err(poisoned) => poisoned.into_inner().append(record, context),
        };
        let observed = match &result {
            Err(CaptureSinkError::RecordLimitExceeded { limit }) => {
                Some(ObservedMemorySinkError::RecordLimit { limit: *limit })
            }
            Err(CaptureSinkError::RetainedByteLimitExceeded { required, limit }) => {
                Some(ObservedMemorySinkError::RetainedByteLimit {
                    required: *required,
                    limit: *limit,
                })
            }
            Err(CaptureSinkError::InvalidPayloadSharing) => {
                Some(ObservedMemorySinkError::InvalidPayloadSharing)
            }
            Err(CaptureSinkError::AccountingInvariant) => {
                Some(ObservedMemorySinkError::AccountingInvariant)
            }
            _ => None,
        };
        if let Some(observed) = observed {
            match self.observed_error.lock() {
                Ok(mut slot) => *slot = Some(observed),
                Err(poisoned) => *poisoned.into_inner() = Some(observed),
            }
        }
        result
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        match self.inner.lock() {
            Ok(mut inner) => inner.flush(context),
            Err(poisoned) => poisoned.into_inner().flush(context),
        }
    }
}

#[derive(Debug)]
struct FlushGatedMemorySink {
    destination: CaptureDestination,
    inner: Arc<std::sync::Mutex<MemoryCaptureSink>>,
    flush_gate: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
}

impl CaptureSink for FlushGatedMemorySink {
    fn destination(&self) -> CaptureDestination {
        self.destination.clone()
    }

    fn append(
        &mut self,
        record: &CapturedRawRecord,
        context: &CaptureIoContext,
    ) -> Result<(), CaptureSinkError> {
        match self.inner.lock() {
            Ok(mut inner) => inner.append(record, context),
            Err(poisoned) => poisoned.into_inner().append(record, context),
        }
    }

    fn flush(&mut self, context: &CaptureIoContext) -> Result<(), CaptureSinkError> {
        if let Some((entered, release)) = self.flush_gate.take() {
            entered
                .send(())
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
            release
                .recv()
                .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))?;
        }
        match self.inner.lock() {
            Ok(mut inner) => inner.flush(context),
            Err(poisoned) => poisoned.into_inner().flush(context),
        }
    }
}

type InspectableMemorySinkParts = (
    InspectableMemorySink,
    Arc<std::sync::Mutex<MemoryCaptureSink>>,
    Arc<std::sync::Mutex<Option<ObservedMemorySinkError>>>,
);

fn inspectable_memory_sink(
    inner: MemoryCaptureSink,
    first_append_gate: Option<(
        std::sync::mpsc::SyncSender<()>,
        std::sync::mpsc::Receiver<()>,
    )>,
) -> InspectableMemorySinkParts {
    let destination = inner.destination();
    let inner = Arc::new(std::sync::Mutex::new(inner));
    let observed_error = Arc::new(std::sync::Mutex::new(None));
    (
        InspectableMemorySink {
            destination,
            inner: Arc::clone(&inner),
            first_append_gate,
            observed_error: Arc::clone(&observed_error),
        },
        inner,
        observed_error,
    )
}

fn record_dynamic_bytes(frame: &TestFrame) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(
        checked_arc_str_allocation_bytes(frame.source_id().as_str().len())?
            .checked_add(checked_arc_bytes_allocation_bytes(frame.payload().len())?)
            .ok_or("test record dynamic-byte quote overflowed")?,
    )
}

fn observed_sink_error(
    observed: &Arc<std::sync::Mutex<Option<ObservedMemorySinkError>>>,
) -> Option<ObservedMemorySinkError> {
    match observed.lock() {
        Ok(observed) => *observed,
        Err(poisoned) => *poisoned.into_inner(),
    }
}

#[test]
fn capture_sink_construction_accepts_exact_fixed_storage_and_types_refusal_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let max_records = NonZeroUsize::new(2).ok_or("invalid sink record limit")?;
    let probe = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES)
            .ok_or("invalid probe sink ceiling")?,
    )?;
    let fixed = probe.fixed_retained_bytes();
    assert_eq!(probe.max_records(), 2);
    assert!(probe.allocated_record_capacity() >= probe.max_records());
    assert_eq!(
        fixed,
        std::mem::size_of::<MemoryCaptureSink>()
            + probe.allocated_record_capacity() * std::mem::size_of::<CapturedRawRecord>()
    );
    drop(probe);

    let exact = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(fixed).ok_or("observed fixed storage must be nonzero")?,
    )?;
    assert_eq!(exact.total_retained_bytes()?, fixed);
    assert_eq!(exact.retained_byte_limit(), fixed);
    drop(exact);

    let one_under = fixed
        .checked_sub(1)
        .and_then(NonZeroUsize::new)
        .ok_or("observed fixed storage must exceed one byte")?;
    assert!(matches!(
        MemoryCaptureSink::try_new(max_records, one_under),
        Err(MemoryCaptureSinkConstructionError::FixedStorageBudgetExceeded {
            required,
            limit
        }) if required == fixed && limit == fixed - 1
    ));
    assert!(matches!(
        MemoryCaptureSink::try_new(NonZeroUsize::MAX, NonZeroUsize::MAX),
        Err(MemoryCaptureSinkConstructionError::ArithmeticOverflow)
    ));

    let record_size = std::mem::size_of::<CapturedRawRecord>();
    let allocation_refusal_records = usize::try_from(isize::MAX)?
        .checked_div(record_size)
        .and_then(|records| records.checked_add(1))
        .and_then(NonZeroUsize::new)
        .ok_or("invalid allocation-refusal record count")?;
    assert!(matches!(
        MemoryCaptureSink::try_new(allocation_refusal_records, NonZeroUsize::MAX),
        Err(MemoryCaptureSinkConstructionError::AllocationFailed {
            requested_records
        }) if requested_records == allocation_refusal_records.get()
    ));
    Ok(())
}

#[tokio::test]
async fn capture_sink_charges_the_complete_shared_arc_graph_for_every_retained_record()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let repeated = frame(identity, 1)?;
    let dynamic_per_record = record_dynamic_bytes(&repeated)?;
    let max_records = NonZeroUsize::new(2).ok_or("invalid exact sink record limit")?;
    let probe = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES)
            .ok_or("invalid probe sink ceiling")?,
    )?;
    let fixed = probe.fixed_retained_bytes();
    let allocated_capacity = probe.allocated_record_capacity();
    drop(probe);
    let exact_ceiling = fixed
        .checked_add(
            dynamic_per_record
                .checked_mul(2)
                .ok_or("test sink dynamic quote overflowed")?,
        )
        .ok_or("test sink exact ceiling overflowed")?;
    let exact = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(exact_ceiling).ok_or("invalid exact sink ceiling")?,
    )?;
    assert_eq!(exact.fixed_retained_bytes(), fixed);
    let (sink, retained, observed_error) = inspectable_memory_sink(exact, None);
    let (publisher, mut control, writer) = test_capture_channel(max_records, bundle)?;
    let handle = spawn_capture_writer(writer, sink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;

    let _first_receipt = publisher.try_publish(&repeated)?;
    let _second_receipt = publisher.try_publish(&repeated)?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    assert!(!termination.outcome().is_incomplete());
    assert_eq!(observed_sink_error(&observed_error), None);

    let retained = match retained.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert_eq!(retained.records().len(), 2);
    assert_eq!(retained.allocated_record_capacity(), allocated_capacity);
    assert_eq!(retained.dynamic_retained_bytes(), dynamic_per_record * 2);
    assert_eq!(retained.total_retained_bytes()?, exact_ceiling);
    assert_eq!(retained.retained_byte_limit(), exact_ceiling);
    assert_eq!(
        retained.records()[0].record().payload().as_ptr(),
        retained.records()[1].record().payload().as_ptr()
    );
    Ok(())
}

#[tokio::test]
async fn capture_sink_rejects_one_over_its_logical_record_limit_without_growing()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let repeated = frame(identity, 1)?;
    let dynamic_per_record = record_dynamic_bytes(&repeated)?;
    let max_records = NonZeroUsize::MIN;
    let inner = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES).ok_or("invalid sink ceiling")?,
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let (sink, retained, observed_error) =
        inspectable_memory_sink(inner, Some((entered_sender, release_receiver)));
    let (publisher, mut control, writer) = test_capture_channel(
        NonZeroUsize::new(2).ok_or("invalid queue capacity")?,
        bundle,
    )?;
    let handle = spawn_capture_writer(writer, sink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;

    let first_receipt = publisher.try_publish(&repeated)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let second_receipt = publisher.try_publish(&repeated)?;
    release_sender.send(())?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;

    assert!(termination.outcome().is_incomplete());
    assert_eq!(
        observed_sink_error(&observed_error),
        Some(ObservedMemorySinkError::RecordLimit { limit: 1 })
    );
    assert!(!first_receipt.is_healthy());
    assert!(!second_receipt.is_healthy());
    let retained = match retained.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert_eq!(retained.records().len(), 1);
    assert_eq!(retained.dynamic_retained_bytes(), dynamic_per_record);
    Ok(())
}

#[tokio::test]
async fn capture_sink_rejects_one_over_its_retained_byte_limit_without_ledger_drift()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let repeated = frame(identity, 1)?;
    let dynamic_per_record = record_dynamic_bytes(&repeated)?;
    let max_records = NonZeroUsize::new(2).ok_or("invalid sink record limit")?;
    let probe = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES)
            .ok_or("invalid probe sink ceiling")?,
    )?;
    let fixed = probe.fixed_retained_bytes();
    drop(probe);
    let retained_limit = fixed
        .checked_add(dynamic_per_record)
        .ok_or("test retained-byte limit overflowed")?;
    let inner = MemoryCaptureSink::try_new(
        max_records,
        NonZeroUsize::new(retained_limit).ok_or("invalid sink retained-byte limit")?,
    )?;
    assert_eq!(inner.fixed_retained_bytes(), fixed);
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let (sink, retained, observed_error) =
        inspectable_memory_sink(inner, Some((entered_sender, release_receiver)));
    let (publisher, mut control, writer) = test_capture_channel(max_records, bundle)?;
    let handle = spawn_capture_writer(writer, sink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;

    let first_receipt = publisher.try_publish(&repeated)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let second_receipt = publisher.try_publish(&repeated)?;
    release_sender.send(())?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;

    assert!(termination.outcome().is_incomplete());
    assert_eq!(
        observed_sink_error(&observed_error),
        Some(ObservedMemorySinkError::RetainedByteLimit {
            required: fixed + dynamic_per_record * 2,
            limit: retained_limit,
        })
    );
    assert!(!first_receipt.is_healthy());
    assert!(!second_receipt.is_healthy());
    let retained = match retained.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert_eq!(retained.records().len(), 1);
    assert_eq!(retained.dynamic_retained_bytes(), dynamic_per_record);
    assert_eq!(retained.total_retained_bytes()?, retained_limit);
    Ok(())
}

#[tokio::test]
async fn capture_conversion_reserves_only_the_shared_payload_graph_and_preserves_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let original = frame(identity, 1)?;
    let complete_frame = original
        .checked_retained_footprint()?
        .checked_complete_bytes()?;
    let queued_frame_allocation_overhead = checked_arc_value_allocation_bytes::<TestFrame>(0)?
        .checked_sub(std::mem::size_of::<TestFrame>())
        .ok_or("queued frame allocation overhead underflowed")?;
    let conversion_source_allocation =
        checked_arc_str_allocation_bytes(original.source_id().as_str().len())?;
    let exact_reservation = complete_frame
        .checked_add(queued_frame_allocation_overhead)
        .and_then(|bytes| bytes.checked_add(conversion_source_allocation))
        .ok_or("exact conversion reservation overflowed")?;
    let original_payload = original.payload().as_ptr();
    let inner = MemoryCaptureSink::try_new(
        NonZeroUsize::MIN,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES).ok_or("invalid sink ceiling")?,
    )?;
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let (sink, retained, _observed_error) =
        inspectable_memory_sink(inner, Some((entered_sender, release_receiver)));
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(writer, sink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;

    let _receipt = publisher.try_publish(&original)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let reservation_during_append = accounted_record_bytes(&publisher)?;
    release_sender.send(())?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;
    assert_eq!(reservation_during_append, exact_reservation);
    assert!(!termination.outcome().is_incomplete());
    assert_eq!(accounted_record_bytes(&publisher)?, 0);

    let retained = match retained.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    let captured = retained
        .records()
        .first()
        .ok_or("conversion sink retained no record")?;
    assert_eq!(captured.record().payload().as_ptr(), original_payload);
    Ok(())
}

#[tokio::test]
async fn capture_conversion_reservation_remains_owned_through_record_triggered_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let original = frame(identity, 1)?;
    let complete_frame = original
        .checked_retained_footprint()?
        .checked_complete_bytes()?;
    let queued_frame_allocation_overhead = checked_arc_value_allocation_bytes::<TestFrame>(0)?
        .checked_sub(std::mem::size_of::<TestFrame>())
        .ok_or("queued frame allocation overhead underflowed")?;
    let conversion_source_allocation =
        checked_arc_str_allocation_bytes(original.source_id().as_str().len())?;
    let exact_reservation = complete_frame
        .checked_add(queued_frame_allocation_overhead)
        .and_then(|bytes| bytes.checked_add(conversion_source_allocation))
        .ok_or("exact conversion reservation overflowed")?;
    let inner = MemoryCaptureSink::try_new(
        NonZeroUsize::MIN,
        NonZeroUsize::new(TEST_MEMORY_SINK_RETAINED_CEILING_BYTES).ok_or("invalid sink ceiling")?,
    )?;
    let destination = inner.destination();
    let inner = Arc::new(std::sync::Mutex::new(inner));
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let sink = FlushGatedMemorySink {
        destination,
        inner: Arc::clone(&inner),
        flush_gate: Some((entered_sender, release_receiver)),
    };
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        sink,
        CaptureWriterPolicy::try_new(NonZeroUsize::MIN, Duration::from_secs(1))?,
    )?;
    control.activate_initial()?;

    let _receipt = publisher.try_publish(&original)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let reservation_during_flush = accounted_record_bytes(&publisher)?;
    release_sender.send(())?;
    let termination = shutdown_and_reap(handle, Duration::from_secs(1)).await?;

    assert_eq!(reservation_during_flush, exact_reservation);
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
    assert!(!termination.outcome().is_incomplete());
    let retained = match inner.lock() {
        Ok(retained) => retained,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert_eq!(retained.records().len(), 1);
    Ok(())
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
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
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
    let (publisher, mut control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let mut oversized = frame(identity, 1)?;
    oversized.retained_override = Some(usize::MAX);

    assert_eq!(
        publisher.try_publish(&oversized).err(),
        Some(CapturePublishError::RetainedSize(
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            },
        ))
    );
    assert_eq!(issued.load(Ordering::Acquire), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(accounted_record_bytes(&publisher)?, 0);
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
        test_capture_channel(NonZeroUsize::new(2).ok_or("invalid test capacity")?, bundle)?;
    let handle = spawn_capture_writer(
        writer,
        test_memory_capture_sink()?,
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
    let (publisher, control, writer) = test_capture_channel(NonZeroUsize::MIN, bundle)?;
    drop(writer);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    drop(control);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}
