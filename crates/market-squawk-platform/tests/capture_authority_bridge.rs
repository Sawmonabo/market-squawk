use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureInitializer, CaptureIntegrityState, ConnectionGeneration,
    MetadataRevision, RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    CapturePublishError, CaptureSink, CaptureSinkError, CaptureStorageErrorClass,
    CaptureWriterPolicy, CapturedRawRecord, MemoryCaptureSink, raw_capture_channel,
    spawn_capture_writer,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;

#[derive(Debug)]
struct TestFrame {
    identity: CaptureAuthorityIdentity,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: Arc<[u8]>,
    retained_override: Option<usize>,
    clone_gate: Option<FrameCloneGate>,
}

#[derive(Clone, Debug)]
struct FrameCloneGate {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl Clone for TestFrame {
    fn clone(&self) -> Self {
        if let Some(gate) = &self.clone_gate {
            let _entered = gate.entered.send(());
            match gate.release.lock() {
                Ok(release) => {
                    let _released = release.recv();
                }
                Err(poisoned) => {
                    let _released = poisoned.into_inner().recv();
                }
            }
        }
        Self {
            identity: self.identity.clone(),
            ordinal: self.ordinal,
            received_at: self.received_at,
            payload: Arc::clone(&self.payload),
            retained_override: self.retained_override,
            clone_gate: None,
        }
    }
}

impl RawCaptureFrameView for TestFrame {
    fn source_id(&self) -> &SourceId {
        self.identity.source_id()
    }

    fn metadata_revision(&self) -> &MetadataRevision {
        self.identity.metadata_revision()
    }

    fn session_identifier(&self) -> &SourceIdentifier {
        self.identity.session_identifier()
    }

    fn connection_generation(&self) -> ConnectionGeneration {
        self.identity.connection_generation()
    }

    fn frame_ordinal(&self) -> NonZeroU64 {
        self.ordinal
    }

    fn received_at(&self) -> Timestamp {
        self.received_at
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn retained_bytes(&self) -> usize {
        self.retained_override
            .unwrap_or_else(|| std::mem::size_of::<Self>().saturating_add(self.payload.len()))
    }
}

#[derive(Debug)]
struct TestInitializer {
    state: Arc<AtomicU8>,
    required_healthy: Option<Arc<AtomicU8>>,
}

impl CaptureInitializer for TestInitializer {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError> {
        if self
            .required_healthy
            .as_ref()
            .is_some_and(|required| required.load(Ordering::Acquire) != HEALTHY)
        {
            return Err(CaptureAuthorityError::GenerationIncomplete);
        }
        match self.state.compare_exchange(
            INITIALIZING,
            HEALTHY,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(HEALTHY) => Ok(()),
            Err(_) => Err(CaptureAuthorityError::GenerationIncomplete),
        }
    }
}

#[derive(Debug)]
struct TestAdmission {
    identity: CaptureAuthorityIdentity,
    state: Arc<AtomicU8>,
    issued: Arc<AtomicU64>,
    issue_gate: Option<IssueGate>,
}

#[derive(Debug)]
struct IssueGate {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[derive(Debug)]
struct TestReceipt {
    state: Arc<AtomicU8>,
    ordinal: NonZeroU64,
}

impl TestReceipt {
    fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == HEALTHY
    }
}

impl CaptureAdmission<TestFrame> for TestAdmission {
    type Receipt = TestReceipt;

    fn preflight(&self, frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &TestFrame,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        self.validate(frame)?;
        if let Some(gate) = &self.issue_gate {
            gate.entered
                .send(())
                .map_err(|_error| CaptureAuthorityError::FrameRejected)?;
            match gate.release.lock() {
                Ok(release) => release
                    .recv()
                    .map_err(|_error| CaptureAuthorityError::FrameRejected)?,
                Err(poisoned) => poisoned
                    .into_inner()
                    .recv()
                    .map_err(|_error| CaptureAuthorityError::FrameRejected)?,
            }
            self.validate(frame)?;
        }
        let _previous = self.issued.fetch_add(1, Ordering::AcqRel);
        Ok(TestReceipt {
            state: Arc::clone(&self.state),
            ordinal: frame.ordinal,
        })
    }

    fn validate_active(&self, frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }
}

impl TestAdmission {
    fn validate(&self, frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        if frame.identity != self.identity {
            return Err(CaptureAuthorityError::FrameBindingMismatch);
        }
        match self.state.load(Ordering::Acquire) {
            HEALTHY => Ok(()),
            INITIALIZING => Err(CaptureAuthorityError::GenerationNotReady),
            _ => Err(CaptureAuthorityError::GenerationIncomplete),
        }
    }
}

#[derive(Clone, Debug)]
struct TestDegradation {
    state: Arc<AtomicU8>,
    gate: Option<DegradationGate>,
}

#[derive(Clone, Debug)]
struct DegradationGate {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
    used: Arc<std::sync::atomic::AtomicBool>,
}

impl CaptureDegradation for TestDegradation {
    fn mark_incomplete(&self) {
        self.state.store(INCOMPLETE, Ordering::Release);
        if let Some(gate) = &self.gate
            && !gate.used.swap(true, Ordering::AcqRel)
        {
            let _entered = gate.entered.send(());
            match gate.release.lock() {
                Ok(release) => {
                    let _released = release.recv();
                }
                Err(poisoned) => {
                    let _released = poisoned.into_inner().recv();
                }
            }
        }
    }

    fn integrity(&self) -> CaptureIntegrityState {
        if self.state.load(Ordering::Acquire) == HEALTHY {
            CaptureIntegrityState::Healthy
        } else {
            CaptureIntegrityState::Incomplete
        }
    }
}

#[derive(Debug)]
struct TestBundle {
    identity: CaptureAuthorityIdentity,
    initializer: TestInitializer,
    admission: TestAdmission,
    degradation: TestDegradation,
}

impl TestBundle {
    fn try_new(generation: u64) -> Result<(Self, Arc<AtomicU64>), Box<dyn std::error::Error>> {
        Self::try_new_for("test-source", "session-a", generation)
    }

    fn try_new_for(
        source: &str,
        session: &str,
        generation: u64,
    ) -> Result<(Self, Arc<AtomicU64>), Box<dyn std::error::Error>> {
        let identity = CaptureAuthorityIdentity::new(
            SourceId::try_from(source)?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SourceIdentifier::try_from(session)?,
            ConnectionGeneration::new(generation)?,
        );
        let state = Arc::new(AtomicU8::new(INITIALIZING));
        let issued = Arc::new(AtomicU64::new(0));
        Ok((
            Self {
                identity: identity.clone(),
                initializer: TestInitializer {
                    state: Arc::clone(&state),
                    required_healthy: None,
                },
                admission: TestAdmission {
                    identity,
                    state: Arc::clone(&state),
                    issued: Arc::clone(&issued),
                    issue_gate: None,
                },
                degradation: TestDegradation { state, gate: None },
            },
            issued,
        ))
    }
}

impl CaptureAuthorityBundle for TestBundle {
    type Frame = TestFrame;
    type Receipt = TestReceipt;
    type Initializer = TestInitializer;
    type Admission = TestAdmission;
    type Degradation = TestDegradation;

    fn identity(&self) -> CaptureAuthorityIdentity {
        self.identity.clone()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        (self.initializer, self.admission, self.degradation)
    }
}

assert_impl_all!(market_squawk_platform::RawCapturePublisher<TestBundle>: Clone, Send, Sync);
assert_not_impl_any!(market_squawk_platform::RawCaptureControl<TestBundle>: Clone);
assert_not_impl_any!(TestBundle: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(TestReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);

fn frame(
    identity: CaptureAuthorityIdentity,
    ordinal: u64,
) -> Result<TestFrame, Box<dyn std::error::Error>> {
    Ok(TestFrame {
        identity,
        ordinal: NonZeroU64::new(ordinal).ok_or("test ordinal must be nonzero")?,
        received_at: Timestamp::from_unix_nanos(i64::try_from(ordinal)?),
        payload: Arc::from(format!("frame-{ordinal}").into_bytes()),
        retained_override: None,
        clone_gate: None,
    })
}

#[tokio::test]
async fn concrete_associated_receipt_is_issued_only_after_bounded_enqueue()
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

    let receipt = publisher.try_publish(&frame(identity, 1)?)?;
    assert_eq!(receipt.ordinal.get(), 1);
    assert!(receipt.is_healthy());
    assert_eq!(issued.load(Ordering::Acquire), 1);

    let outcome = handle.shutdown(Duration::from_secs(1)).await;
    assert!(!outcome.is_incomplete());
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

#[tokio::test]
async fn queue_saturation_degrades_exact_generation_without_issuing_a_third_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
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
    let first = publisher.try_publish(&frame(identity.clone(), 1)?)?;
    entered_receiver.recv_timeout(Duration::from_secs(1))?;
    let second = publisher.try_publish(&frame(identity.clone(), 2)?)?;
    assert!(matches!(
        publisher.try_publish(&frame(identity, 3)?),
        Err(CapturePublishError::Saturated)
    ));
    assert_eq!(issued.load(Ordering::Acquire), 2);
    assert!(!first.is_healthy());
    assert!(!second.is_healthy());

    release_sender.send(())?;
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    assert_eq!(publisher.queued_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn whole_bundle_rotation_invalidates_old_receipt_and_accepts_only_new_binding()
-> Result<(), Box<dyn std::error::Error>> {
    let (first_bundle, _first_issued) = TestBundle::try_new(1)?;
    let first_identity = first_bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        first_bundle,
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
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

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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

#[tokio::test]
async fn writer_failure_invalidates_an_already_issued_concrete_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(writer, FailingSink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(identity, 1)?)?;

    assert!(handle.wait().await.is_incomplete());
    assert!(!receipt.is_healthy());
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
    assert_eq!(publisher.queued_bytes(), 0);
    Ok(())
}

#[derive(Debug)]
struct RecordingSink(std::sync::mpsc::SyncSender<CapturedRawRecord>);

impl CaptureSink for RecordingSink {
    fn append(&mut self, record: &CapturedRawRecord) -> Result<(), CaptureSinkError> {
        self.0
            .send(record.clone())
            .map_err(|_error| CaptureSinkError::storage(CaptureStorageErrorClass::Other))
    }

    fn flush(&mut self) -> Result<(), CaptureSinkError> {
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
        RecordingSink(sender),
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

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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
async fn shutdown_deadline_invalidates_authority_and_releases_queued_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(writer, SlowFlushSink, CaptureWriterPolicy::default())?;
    control.activate_initial()?;
    let receipt = publisher.try_publish(&frame(identity, 1)?)?;

    assert!(
        handle
            .shutdown(Duration::from_millis(1))
            .await
            .is_incomplete()
    );
    assert!(!receipt.is_healthy());
    assert_eq!(publisher.queued_bytes(), 0);
    Ok(())
}

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
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        first_bundle,
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
        CaptureWriterPolicy::default(),
    )?;
    control.activate_initial()?;
    let thread_publisher = publisher.clone();
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
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn failed_rotation_degrades_the_uninstalled_next_bundle_only()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
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
    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn preflight_lock_is_released_before_frame_clone_and_bounded_enqueue()
-> Result<(), Box<dyn std::error::Error>> {
    let (bundle, _issued) = TestBundle::try_new(1)?;
    let identity = bundle.identity();
    let (publisher, mut control, writer) =
        raw_capture_channel(NonZeroUsize::new(4).ok_or("invalid test capacity")?, bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
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
    let thread_publisher = publisher.clone();
    let first_publish = std::thread::spawn(move || thread_publisher.try_publish(&first));
    entered_receiver.recv_timeout(Duration::from_secs(1))?;

    let second_receipt = publisher.try_publish(&frame(identity, 2)?)?;
    assert!(second_receipt.is_healthy());
    release_sender.send(())?;
    let first_result = first_publish
        .join()
        .map_err(|_panic| "publisher thread panicked")?;
    assert!(first_result.is_ok());

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
    Ok(())
}

#[tokio::test]
async fn successor_initializes_before_old_revocation_and_failed_init_preserves_old()
-> Result<(), Box<dyn std::error::Error>> {
    let (first_bundle, _issued) = TestBundle::try_new(1)?;
    let first_identity = first_bundle.identity();
    let (publisher, mut control, writer) = raw_capture_channel(
        NonZeroUsize::new(4).ok_or("invalid test capacity")?,
        first_bundle,
    );
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
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

    let _outcome = handle.shutdown(Duration::from_secs(1)).await;
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
    let (publisher, mut control, writer) = raw_capture_channel(NonZeroUsize::MIN, first_bundle);
    let handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::default(),
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
