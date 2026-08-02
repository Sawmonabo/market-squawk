use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureFrameFootprint, CaptureInitializer, CaptureIntegrityState,
    CapturePayload, CaptureResidentGenerationLease, CaptureResidentToken, CaptureRetainedComponent,
    CaptureRetainedReceipt, CaptureRetainedSizeError, ConnectionGeneration,
    MAX_LIVE_CAPTURE_PAYLOAD_BYTES, MetadataRevision, RawCaptureFrameView, SourceId,
    SourceIdentifier, Timestamp, checked_arc_bytes_allocation_bytes,
    checked_arc_str_allocation_bytes, checked_arc_value_allocation_bytes,
};
use market_squawk_platform::{
    CaptureChannelLimits, CaptureDestination, CaptureGenerationError, CaptureIoContext,
    CaptureProcessInfrastructureLimits, CapturePublishError, CaptureShutdownStatus, CaptureSink,
    CaptureSinkError, CaptureStorageErrorClass, CaptureWorkerTermination, CaptureWriterHandle,
    CaptureWriterPolicy, CaptureWriterSpawnError, CapturedRawRecord, DiagnosticCaptureBundle,
    MemoryCaptureSink, MemoryCaptureSinkConstructionError, RawCaptureChannel, RawCapturePublisher,
    initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;
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

fn test_capture_channel<B: CaptureAuthorityBundle>(
    capacity: NonZeroUsize,
    bundle: B,
) -> Result<RawCaptureChannel<B>, Box<dyn std::error::Error>> {
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

fn accounted_record_bytes<B: CaptureAuthorityBundle>(
    publisher: &RawCapturePublisher<B>,
) -> Result<usize, market_squawk_platform::CaptureAccountingSnapshotError> {
    publisher
        .try_accounting_snapshot(NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN))
        .map(market_squawk_platform::CaptureAccountingSnapshot::record_reservation_bytes)
}

#[derive(Debug)]
struct TestFrame {
    identity: CaptureAuthorityIdentity,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: CapturePayload,
    payload_view_override: Option<Arc<[u8]>>,
    retained_override: Option<usize>,
    clone_gate: Option<FrameCloneGate>,
    clone_mutation: Option<FrameCloneMutation>,
    clone_count: Option<Arc<AtomicU64>>,
}

#[derive(Clone, Debug, Default)]
struct FrameCloneMutation {
    identity: Option<CaptureAuthorityIdentity>,
    ordinal: Option<NonZeroU64>,
    received_at: Option<Timestamp>,
    payload: Option<CapturePayload>,
    retained_override: Option<usize>,
}

#[derive(Clone, Debug)]
struct FrameCloneGate {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl Clone for TestFrame {
    fn clone(&self) -> Self {
        if let Some(clone_count) = &self.clone_count {
            let _previous = clone_count.fetch_add(1, Ordering::AcqRel);
        }
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
        let mut cloned = Self {
            identity: self.identity.clone(),
            ordinal: self.ordinal,
            received_at: self.received_at,
            payload: self.payload.clone(),
            payload_view_override: self.payload_view_override.clone(),
            retained_override: self.retained_override,
            clone_gate: None,
            clone_mutation: None,
            clone_count: self.clone_count.clone(),
        };
        if let Some(mutation) = &self.clone_mutation {
            if let Some(identity) = &mutation.identity {
                cloned.identity = identity.clone();
            }
            if let Some(ordinal) = mutation.ordinal {
                cloned.ordinal = ordinal;
            }
            if let Some(received_at) = mutation.received_at {
                cloned.received_at = received_at;
            }
            if let Some(payload) = &mutation.payload {
                cloned.payload = payload.clone();
                cloned.payload_view_override = None;
            }
            if mutation.retained_override.is_some() {
                cloned.retained_override = mutation.retained_override;
            }
        }
        cloned
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
        self.payload_view_override
            .as_deref()
            .unwrap_or_else(|| self.payload.as_bytes())
    }

    fn capture_payload(&self) -> &CapturePayload {
        &self.payload
    }

    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError> {
        CaptureFrameFootprint::try_new(
            std::mem::size_of::<Self>(),
            0,
            match self.retained_override {
                Some(bytes) => bytes,
                None => self.payload.checked_retained_allocation_bytes()?,
            },
        )
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
    receipt_override: ReceiptOverride,
    receipt_method_probe: Option<Arc<ReentryProbe>>,
    receipt_drop_probe: Option<Arc<ReentryProbe>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReceiptOverride {
    Exact,
    SubstituteResident,
    NonzeroDynamic,
}

#[derive(Debug)]
struct DummyResidentToken;

impl CaptureResidentToken for DummyResidentToken {}

#[derive(Debug)]
struct IssueGate {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[derive(Debug)]
struct ReentryAction {
    publisher: market_squawk_platform::RawCapturePublisher<TestBundle>,
    frame: TestFrame,
}

#[derive(Debug, Default)]
struct ReentryProbe {
    action: std::sync::Mutex<Option<ReentryAction>>,
    observed_error: std::sync::Mutex<Option<CapturePublishError>>,
    calls: AtomicU64,
}

impl ReentryProbe {
    fn install(
        &self,
        publisher: market_squawk_platform::RawCapturePublisher<TestBundle>,
        frame: TestFrame,
    ) -> Result<(), &'static str> {
        let mut action = match self.action.lock() {
            Ok(action) => action,
            Err(poisoned) => poisoned.into_inner(),
        };
        if action.is_some() {
            return Err("reentry probe already has an installed action");
        }
        *action = Some(ReentryAction { publisher, frame });
        Ok(())
    }

    fn invoke_once(&self) {
        let action = {
            let mut action = match self.action.lock() {
                Ok(action) => action,
                Err(poisoned) => poisoned.into_inner(),
            };
            action.take()
        };
        let Some(action) = action else {
            return;
        };
        let _previous = self.calls.fetch_add(1, Ordering::AcqRel);
        let error = action.publisher.try_publish(&action.frame).err();
        let mut observed = match self.observed_error.lock() {
            Ok(observed) => observed,
            Err(poisoned) => poisoned.into_inner(),
        };
        *observed = error;
    }

    fn observed_error(&self) -> Option<CapturePublishError> {
        match self.observed_error.lock() {
            Ok(observed) => *observed,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }
}

#[derive(Debug)]
struct TestReceipt {
    state: Arc<AtomicU8>,
    ordinal: NonZeroU64,
    resident: CaptureResidentGenerationLease,
    additional_dynamic_bytes: usize,
    received_at: Timestamp,
    payload_length: usize,
    payload_first_byte: Option<u8>,
    method_probe: Option<Arc<ReentryProbe>>,
    drop_probe: Option<Arc<ReentryProbe>>,
}

impl TestReceipt {
    fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == HEALTHY
    }
}

impl CaptureRetainedReceipt for TestReceipt {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        if let Some(probe) = &self.method_probe {
            probe.invoke_once();
        }
        &self.resident
    }

    fn checked_additional_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            state: _,
            ordinal: _,
            resident: _,
            additional_dynamic_bytes,
            received_at: _,
            payload_length: _,
            payload_first_byte: _,
            method_probe: _,
            drop_probe: _,
        } = self;
        Ok(*additional_dynamic_bytes)
    }
}

impl Drop for TestReceipt {
    fn drop(&mut self) {
        if let Some(probe) = &self.drop_probe {
            probe.invoke_once();
        }
    }
}

impl CaptureAdmission<TestFrame> for TestAdmission {
    type Receipt = TestReceipt;

    fn checked_resident_shared_frame_bytes(
        &self,
        _frame: &TestFrame,
    ) -> Result<usize, CaptureRetainedSizeError> {
        Ok(0)
    }

    fn preflight(&self, frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &TestFrame,
        resident: CaptureResidentGenerationLease,
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
        let (resident, additional_dynamic_bytes) = match self.receipt_override {
            ReceiptOverride::Exact => (resident, 0),
            ReceiptOverride::SubstituteResident => (
                CaptureResidentGenerationLease::new(Arc::new(DummyResidentToken)),
                0,
            ),
            ReceiptOverride::NonzeroDynamic => (resident, 1),
        };
        Ok(TestReceipt {
            state: Arc::clone(&self.state),
            ordinal: frame.ordinal,
            resident,
            additional_dynamic_bytes,
            received_at: frame.received_at,
            payload_length: frame.payload().len(),
            payload_first_byte: frame.payload().first().copied(),
            method_probe: self.receipt_method_probe.clone(),
            drop_probe: self.receipt_drop_probe.clone(),
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
    reentry_probe: Option<Arc<ReentryProbe>>,
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
        if let Some(probe) = &self.reentry_probe {
            probe.invoke_once();
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
    declared_retained_bytes: usize,
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
                    receipt_override: ReceiptOverride::Exact,
                    receipt_method_probe: None,
                    receipt_drop_probe: None,
                },
                degradation: TestDegradation {
                    state,
                    gate: None,
                    reentry_probe: None,
                },
                declared_retained_bytes: 4_096,
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

    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        if !Arc::ptr_eq(&self.initializer.state, &self.admission.state)
            || !Arc::ptr_eq(&self.initializer.state, &self.degradation.state)
        {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Bundle,
            });
        }
        Ok(self.declared_retained_bytes)
    }

    fn identity(&self) -> CaptureAuthorityIdentity {
        self.identity.clone()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        (self.initializer, self.admission, self.degradation)
    }
}

assert_impl_all!(market_squawk_platform::RawCapturePublisher<TestBundle>: Send, Sync);
assert_not_impl_any!(market_squawk_platform::RawCapturePublisher<TestBundle>: Clone);
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
        payload: CapturePayload::try_from_live(format!("frame-{ordinal}").as_bytes())?,
        payload_view_override: None,
        retained_override: None,
        clone_gate: None,
        clone_mutation: None,
        clone_count: None,
    })
}

async fn shutdown_and_reap(
    handle: CaptureWriterHandle<TestBundle>,
    deadline: Duration,
) -> Result<CaptureWorkerTermination, Box<dyn std::error::Error>> {
    let mut pending = handle.shutdown(deadline);
    if pending.wait_until_deadline().await == CaptureShutdownStatus::DeadlineElapsed {
        pending.wait_until_terminated().await;
    }
    pending
        .try_reap()?
        .cloned()
        .ok_or_else(|| "terminated capture worker did not retain a final report".into())
}

#[path = "capture_authority_bridge/direct_cases.rs"]
mod direct_cases;

#[path = "capture_authority_bridge/writer_cases.rs"]
mod writer_cases;

#[path = "capture_authority_bridge/writer_lifecycle_cases.rs"]
mod writer_lifecycle_cases;

#[path = "capture_authority_bridge/cases.rs"]
mod cases;
