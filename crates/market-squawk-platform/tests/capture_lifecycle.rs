use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityIdentity, CaptureIntegrityState, ConnectionGeneration, MetadataRevision,
    RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    CaptureGenerationError, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
    CaptureWriterPolicy, CapturedRawRecord, DiagnosticCaptureBundle, DiagnosticCaptureFrame,
    DiagnosticCaptureReceipt, MemoryCaptureSink, RawCaptureControl, RawCapturePublisher,
    raw_capture_channel, spawn_capture_writer,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(RawCapturePublisher<DiagnosticCaptureBundle>: Clone, Send, Sync);
assert_not_impl_any!(RawCaptureControl<DiagnosticCaptureBundle>: Clone);
assert_not_impl_any!(DiagnosticCaptureBundle: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(DiagnosticCaptureReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);

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

#[tokio::test]
async fn activation_before_writer_start_is_retryable_after_writer_start()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (_publisher, mut control, writer) =
        raw_capture_channel(NonZeroUsize::MIN, DiagnosticCaptureBundle::new(identity));
    assert_eq!(
        control.activate_initial(),
        Err(CaptureGenerationError::WriterUnavailable)
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn natural_writer_completion_degrades_every_previously_issued_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::MIN,
        DiagnosticCaptureBundle::new(identity.clone()),
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let exact_frame = frame(identity, 1)?;
    let receipt = publisher.try_publish(&exact_frame)?;
    drop(publisher);

    let outcome = tokio::time::timeout(Duration::from_secs(1), handle.wait()).await?;
    assert!(!outcome.is_incomplete());
    assert!(!receipt.generation_is_complete());
    drop(control);
    Ok(())
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
async fn old_queued_frame_failure_degrades_the_current_writer_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let first = identity(1)?;
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        DiagnosticCaptureBundle::new(first.clone()),
    );
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
    control.activate_initial()?;
    let old_frame = frame(first, 1)?;
    let old_receipt = publisher.try_publish(&old_frame)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    control.rotate_generation(DiagnosticCaptureBundle::new(identity(2)?))?;
    assert!(!old_receipt.generation_is_complete());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Healthy);

    release_sender.send(())?;
    assert!(handle.wait().await.is_incomplete());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_sink_does_not_stall_tokio_and_handle_drop_releases_exact_queued_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = identity(1)?;
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(3).ok_or("invalid test capacity")?,
        DiagnosticCaptureBundle::new(identity.clone()),
    );
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
    control.activate_initial()?;
    let first = frame(identity.clone(), 1)?;
    let second = frame(identity.clone(), 2)?;
    let third = frame(identity, 3)?;
    let _first_receipt = publisher.try_publish(&first)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    tokio::time::timeout(
        Duration::from_millis(50),
        tokio::time::sleep(Duration::from_millis(1)),
    )
    .await?;
    let _second_receipt = publisher.try_publish(&second)?;
    let one_frame_charge = publisher.queued_bytes();
    let _third_receipt = publisher.try_publish(&third)?;
    assert_eq!(publisher.queued_bytes(), one_frame_charge.saturating_mul(2));
    assert!(one_frame_charge > second.payload().len());

    drop(handle);
    assert_eq!(publisher.queued_bytes(), 0);
    release_sender.send(())?;
    drop(control);
    Ok(())
}
