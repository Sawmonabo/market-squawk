use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
};
use market_squawk_platform::{
    CaptureGenerationError, CaptureGenerationKey, CaptureHealthReason, CapturePublishError,
    CaptureSink, CaptureSinkError, CaptureStorageErrorClass, CaptureWriterPolicy,
    CaptureWriterPolicyError, CapturedRawRecord, RawCaptureRecord, raw_capture_channel,
    spawn_capture_writer,
};
use sha2::{Digest, Sha256};
use static_assertions::{assert_impl_all, assert_not_impl_any};
use uuid::Uuid;

assert_impl_all!(market_squawk_platform::RawCapturePublisher: Clone, Send, Sync);
assert_not_impl_any!(market_squawk_platform::RawCaptureControl: Clone);
assert_not_impl_any!(market_squawk_platform::CaptureAdmissionReceipt: Clone);

#[derive(Debug)]
struct GatedSink {
    entered: Option<std::sync::mpsc::SyncSender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl CaptureSink for GatedSink {
    fn append(&mut self, _record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
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

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

fn generation_key(
    source: &str,
    revision: &str,
    session: &str,
    generation: u64,
) -> Result<CaptureGenerationKey, Box<dyn std::error::Error>> {
    Ok(CaptureGenerationKey::new(
        SourceId::try_from(source)?,
        MetadataRevision::new(SourceIdentifier::try_from(revision)?),
        SourceIdentifier::try_from(session)?,
        ConnectionGeneration::new(generation)?,
        Uuid::from_u128(u128::from(generation) + 10_000),
    ))
}

#[tokio::test]
async fn non_clone_control_activates_initial_and_rotates_allocations_independently_of_market_state()
-> Result<(), Box<dyn std::error::Error>> {
    let key_one = generation_key("source-a", "revision-a", "session-a", 1)?;
    let key_two = generation_key("source-a", "revision-a", "session-a", 2)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key_one.clone());
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;

    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(
        publisher.try_publish(&key_one, frame(&key_one, 1)?),
        Err(CapturePublishError::AllocationInactive)
    );
    control.activate_initial(&key_one)?;
    let old_receipt = publisher.try_publish(&key_one, frame(&key_one, 7)?)?;
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    control.rotate_generation(key_two.clone())?;
    assert!(!old_receipt.allocation_is_healthy());
    assert_eq!(control.key().as_ref(), &key_two);
    assert_eq!(publisher.key()?.as_ref(), &key_two);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

fn frame(
    key: &CaptureGenerationKey,
    sequence: u64,
) -> Result<RawCaptureRecord, Box<dyn std::error::Error>> {
    Ok(RawCaptureRecord::try_new_live(
        Uuid::from_u128(u128::from(sequence) + 100),
        Arc::from("test-source"),
        key.connection_id(),
        Some(sequence),
        None,
        Utc.timestamp_opt(1_752_607_200, 0)
            .single()
            .ok_or("invalid fixed test timestamp")?,
        Bytes::from(format!(r#"{{"sequence":{sequence}}}"#)),
    )?)
}

#[tokio::test]
async fn capture_saturation_fails_closed_without_waiting_for_disk()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    assert!(publisher.try_publish(&key, frame(&key, 1)?).is_ok());
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    assert!(publisher.try_publish(&key, frame(&key, 2)?).is_ok());

    assert_eq!(
        publisher.try_publish(&key, frame(&key, 3)?),
        Err(CapturePublishError::Saturated)
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let event = publisher
        .try_next_health()
        .ok_or("missing overflow health event")?;
    assert_eq!(event.key(), &key);
    assert_eq!(event.reason(), CaptureHealthReason::Saturated);
    release_sender.send(())?;
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn an_unstarted_writer_fails_closed_without_an_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, _control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    drop(writer);

    assert_eq!(
        publisher.try_publish(&key, frame(&key, 1)?),
        Err(CapturePublishError::WriterUnavailable)
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let reasons = [publisher.try_next_health(), publisher.try_next_health()]
        .into_iter()
        .flatten()
        .map(|event| event.reason())
        .collect::<Vec<_>>();
    assert!(reasons.contains(&CaptureHealthReason::Closed));
    assert!(reasons.contains(&CaptureHealthReason::WriterUnavailable));
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn an_invalidated_allocation_cannot_recover_and_requires_control_owned_rotation()
-> Result<(), Box<dyn std::error::Error>> {
    let key_one = generation_key("source-a", "revision-a", "session-a", 1)?;
    let key_two = generation_key("source-a", "revision-a", "session-a", 2)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key_one.clone());
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key_one)?;
    publisher.try_publish(&key_one, frame(&key_one, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    publisher.try_publish(&key_one, frame(&key_one, 2)?)?;
    assert_eq!(
        publisher.try_publish(&key_one, frame(&key_one, 3)?),
        Err(CapturePublishError::Saturated)
    );

    assert_eq!(
        publisher.try_publish(&key_one, frame(&key_one, 4)?),
        Err(CapturePublishError::AllocationInactive)
    );
    assert!(matches!(
        control.activate_initial(&key_one),
        Err(CaptureGenerationError::GenerationMustAdvance)
    ));
    control.rotate_generation(key_two.clone())?;
    assert_eq!(publisher.key()?.as_ref(), &key_two);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    assert!(matches!(
        publisher.try_publish(&key_one, frame(&key_one, 4)?),
        Err(CapturePublishError::BindingMismatch { .. })
    ));
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);
    release_sender.send(())?;
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn raw_connection_identity_cannot_be_transplanted_under_a_valid_generation_key()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    let transplanted = RawCaptureRecord::try_new_live(
        Uuid::from_u128(55),
        Arc::from("test-source"),
        Uuid::from_u128(999_999),
        Some(1),
        None,
        Utc.timestamp_opt(1_752_607_200, 0)
            .single()
            .ok_or("invalid fixed test timestamp")?,
        Bytes::from_static(b"{}"),
    )?;

    assert_eq!(
        publisher.try_publish(&key, transplanted),
        Err(CapturePublishError::ConnectionMismatch)
    );
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn successful_admission_returns_owned_exact_allocation_and_frame_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    let expected = frame(&key, 42)?;
    let receipt = publisher.try_publish(&key, expected.clone())?;

    assert_eq!(receipt.key(), &key);
    assert_eq!(receipt.event_id(), expected.event_id());
    assert_eq!(receipt.source_sequence(), expected.source_sequence());
    assert_eq!(receipt.received_at(), expected.received_at());
    let expected_digest: [u8; 32] = Sha256::digest(expected.payload()).into();
    assert_eq!(receipt.payload_digest(), &expected_digest);
    assert!(receipt.allocation_is_healthy());
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn barrier_racing_old_publish_cannot_leave_a_healthy_receipt_after_rotation_returns()
-> Result<(), Box<dyn std::error::Error>> {
    let key_one = generation_key("source-a", "revision-a", "session-a", 1)?;
    let key_two = generation_key("source-a", "revision-a", "session-a", 2)?;
    let capacity = NonZeroUsize::new(8).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) = raw_capture_channel(capacity, key_one.clone());
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key_one)?;
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let thread_barrier = Arc::clone(&barrier);
    let thread_publisher = publisher.clone();
    let thread_key = key_one.clone();
    let old_frame = frame(&key_one, 9)?;
    let publisher_thread = std::thread::spawn(move || {
        thread_barrier.wait();
        thread_publisher.try_publish(&thread_key, old_frame)
    });

    barrier.wait();
    control.rotate_generation(key_two)?;
    let publish_result = publisher_thread
        .join()
        .map_err(|_panic| "publisher thread panicked")?;
    match publish_result {
        Ok(receipt) => assert!(!receipt.allocation_is_healthy()),
        Err(CapturePublishError::BindingMismatch { .. })
        | Err(CapturePublishError::AllocationInactive)
        | Err(CapturePublishError::WriterUnavailable) => {}
        Err(error) => return Err(Box::new(error) as Box<dyn std::error::Error>),
    }
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[test]
fn source_revision_session_and_generation_transplants_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let active = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, _writer) = raw_capture_channel(NonZeroUsize::MIN, active.clone());
    for transplanted in [
        generation_key("source-b", "revision-a", "session-a", 1)?,
        generation_key("source-a", "revision-b", "session-a", 1)?,
        generation_key("source-a", "revision-a", "session-b", 1)?,
        generation_key("source-a", "revision-a", "session-a", 2)?,
    ] {
        assert!(matches!(
            publisher.try_publish(&transplanted, frame(&active, 10)?),
            Err(CapturePublishError::BindingMismatch { .. })
        ));
        assert_eq!(publisher.key()?.as_ref(), &active);
    }
    assert!(matches!(
        control.rotate_generation(generation_key("source-b", "revision-a", "session-a", 2)?),
        Err(CaptureGenerationError::BindingMismatch { .. })
    ));
    Ok(())
}

#[derive(Debug)]
struct FailingSink;

impl CaptureSink for FailingSink {
    fn append(&mut self, _record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        Err(CaptureSinkError::storage(
            CaptureStorageErrorClass::Unavailable,
        ))
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[derive(Debug)]
struct GatedFailingSink {
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

impl CaptureSink for GatedFailingSink {
    fn append(&mut self, _record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
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

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        Ok(())
    }
}

#[tokio::test]
async fn terminal_sink_failure_releases_all_queued_byte_reservations()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let capacity = NonZeroUsize::new(4).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) = raw_capture_channel(capacity, key.clone());
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedFailingSink {
            entered: entered_sender,
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    let _receipt_one = publisher.try_publish(&key, frame(&key, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let _receipt_two = publisher.try_publish(&key, frame(&key, 2)?)?;
    let _receipt_three = publisher.try_publish(&key, frame(&key, 3)?)?;
    assert!(publisher.queued_bytes() > 0);

    release_sender.send(())?;
    assert!(handle.wait().await.is_incomplete());
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn writer_failure_is_reported_separately_and_marks_the_generation_incomplete()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let handle = spawn_capture_writer(writer, FailingSink, CaptureWriterPolicy::default())?;
    control.activate_initial(&key)?;
    let receipt = publisher.try_publish(&key, frame(&key, 1)?)?;

    let outcome = handle.wait().await;
    assert!(outcome.is_incomplete());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(
        publisher.try_next_health().map(|event| event.reason()),
        Some(CaptureHealthReason::WriterFailed)
    );
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    assert!(!receipt.allocation_is_healthy());
    Ok(())
}

#[tokio::test]
async fn failure_of_an_old_queued_record_marks_the_current_generation_incomplete()
-> Result<(), Box<dyn std::error::Error>> {
    let key_one = generation_key("source-a", "revision-a", "session-a", 1)?;
    let key_two = generation_key("source-a", "revision-a", "session-a", 2)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key_one.clone());
    let handle = spawn_capture_writer(writer, FailingSink, CaptureWriterPolicy::default())?;
    control.activate_initial(&key_one)?;
    publisher.try_publish(&key_one, frame(&key_one, 1)?)?;
    control.rotate_generation(key_two.clone())?;

    assert!(handle.wait().await.is_incomplete());
    assert_eq!(publisher.key()?.as_ref(), &key_two);
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert!(matches!(
        control.rotate_generation(generation_key("source-a", "revision-a", "session-a", 3)?),
        Err(CaptureGenerationError::WriterUnavailable)
    ));
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test]
async fn a_dropped_writer_handle_requests_shutdown_and_marks_shared_health()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, _control, writer) = raw_capture_channel(NonZeroUsize::MIN, key);
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    drop(handle);

    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(
        publisher.try_next_health().map(|event| event.reason()),
        Some(CaptureHealthReason::WriterFailed)
    );
    Ok(())
}

#[test]
fn zero_flush_interval_is_rejected_before_a_task_can_panic() {
    assert!(matches!(
        CaptureWriterPolicy::try_new(NonZeroUsize::MIN, Duration::ZERO),
        Err(CaptureWriterPolicyError::ZeroFlushInterval)
    ));
    assert!(
        CaptureWriterPolicy::try_new(
            NonZeroUsize::MIN,
            Duration::from_secs(60) + Duration::from_nanos(1)
        )
        .is_err()
    );
}

#[tokio::test]
async fn invalid_stale_binding_cannot_poison_current_generation_health()
-> Result<(), Box<dyn std::error::Error>> {
    let key_one = generation_key("source-a", "revision-a", "session-a", 1)?;
    let key_two = generation_key("source-a", "revision-a", "session-a", 2)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key_one.clone());
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key_one)?;
    control.rotate_generation(key_two.clone())?;
    let invalid = RawCaptureRecord::try_from_compatibility_parts(
        Uuid::nil(),
        String::new(),
        Uuid::nil(),
        None,
        None,
        Utc.timestamp_opt(1_752_607_200, 0)
            .single()
            .ok_or("invalid fixed test timestamp")?,
        Vec::new(),
    )?;

    assert!(matches!(
        publisher.try_publish(&key_one, invalid),
        Err(CapturePublishError::BindingMismatch { .. })
    ));
    let snapshot = publisher.health_snapshot();
    assert_eq!(snapshot.key(), &key_two);
    assert_eq!(snapshot.integrity(), CaptureIntegrityState::Healthy);
    assert_eq!(
        publisher.integrity_for(&key_one),
        CaptureIntegrityState::Incomplete
    );
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn blocking_sink_io_does_not_stall_tokio_cooperative_timers()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    publisher.try_publish(&key, frame(&key, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;

    tokio::time::timeout(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_millis(1)),
    )
    .await?;
    release_sender.send(())?;
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn dropping_all_publishers_allows_natural_writer_completion()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, _control, writer) = raw_capture_channel(NonZeroUsize::MIN, key);
    let handle = spawn_capture_writer(
        writer,
        market_squawk_platform::MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    drop(publisher);

    let outcome = tokio::time::timeout(Duration::from_secs(1), handle.wait()).await?;
    assert!(!outcome.is_incomplete());
    Ok(())
}

#[derive(Debug)]
struct SlowFlushSink;

impl CaptureSink for SlowFlushSink {
    fn append(&mut self, _record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        Ok(())
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_an_expired_deadline_returns_incomplete_instead_of_waiting()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, key.clone());
    let handle = spawn_capture_writer(writer, SlowFlushSink, CaptureWriterPolicy::default())?;
    control.activate_initial(&key)?;
    publisher.try_publish(&key, frame(&key, 1)?)?;

    let outcome = handle.shutdown(Duration::from_millis(1)).await;
    assert!(outcome.is_incomplete());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_detach_releases_reservations_even_with_a_stalled_sink_and_queued_record()
-> Result<(), Box<dyn std::error::Error>> {
    let key = generation_key("source-a", "revision-a", "session-a", 1)?;
    let capacity = NonZeroUsize::new(4).ok_or("invalid fixed test capacity")?;
    let (publisher, mut control, writer) = raw_capture_channel(capacity, key.clone());
    let (entered_sender, entered_receiver) = std::sync::mpsc::sync_channel(1);
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(1);
    let handle = spawn_capture_writer(
        writer,
        GatedSink {
            entered: Some(entered_sender),
            release: release_receiver,
        },
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial(&key)?;
    let _receipt_one = publisher.try_publish(&key, frame(&key, 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let _receipt_two = publisher.try_publish(&key, frame(&key, 2)?)?;
    assert!(publisher.queued_bytes() > 0);

    let outcome = handle.shutdown(Duration::from_millis(1)).await;
    assert!(outcome.is_incomplete());
    assert_eq!(publisher.queued_bytes(), 0);
    assert_eq!(publisher.accounting_invariant_failures(), 0);
    release_sender.send(())?;
    Ok(())
}
