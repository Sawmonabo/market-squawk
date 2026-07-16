use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureInitializer, CaptureIntegrityState, ConnectionGeneration,
    MetadataRevision, RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};
use static_assertions::assert_not_impl_any;

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;

#[derive(Clone, Debug)]
struct TestFrame {
    source_id: SourceId,
    revision: MetadataRevision,
    session: SourceIdentifier,
    generation: ConnectionGeneration,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: Vec<u8>,
}

impl RawCaptureFrameView for TestFrame {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn metadata_revision(&self) -> &MetadataRevision {
        &self.revision
    }

    fn session_identifier(&self) -> &SourceIdentifier {
        &self.session
    }

    fn connection_generation(&self) -> ConnectionGeneration {
        self.generation
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
        self.payload.capacity()
    }
}

#[derive(Debug)]
struct TestInitializer {
    state: Arc<AtomicU8>,
}

impl CaptureInitializer for TestInitializer {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError> {
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
    state: Arc<AtomicU8>,
}

#[derive(Debug, Eq, PartialEq)]
struct TestReceipt(NonZeroU64);

impl CaptureAdmission<TestFrame> for TestAdmission {
    type Receipt = TestReceipt;

    fn preflight(&self, _frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.require_healthy()
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &TestFrame,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        self.require_healthy()?;
        Ok(TestReceipt(frame.frame_ordinal()))
    }

    fn validate_active(&self, _frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.require_healthy()
    }
}

impl TestAdmission {
    fn require_healthy(&self) -> Result<(), CaptureAuthorityError> {
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
}

impl CaptureDegradation for TestDegradation {
    fn mark_incomplete(&self) {
        self.state.store(INCOMPLETE, Ordering::Release);
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
    fn try_new() -> Result<Self, Box<dyn std::error::Error>> {
        let state = Arc::new(AtomicU8::new(INITIALIZING));
        Ok(Self {
            identity: CaptureAuthorityIdentity::new(
                SourceId::try_from("test-source")?,
                MetadataRevision::new(SourceIdentifier::try_from("test-revision")?),
                SourceIdentifier::try_from("test-session")?,
                ConnectionGeneration::new(1)?,
            ),
            initializer: TestInitializer {
                state: Arc::clone(&state),
            },
            admission: TestAdmission {
                state: Arc::clone(&state),
            },
            degradation: TestDegradation { state },
        })
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

assert_not_impl_any!(TestBundle: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(TestReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);

#[test]
fn whole_bundle_preserves_one_way_authority_and_associated_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut initializer, mut admission, degradation) = TestBundle::try_new()?.into_parts();
    let frame = TestFrame {
        source_id: SourceId::try_from("coinbase")?,
        revision: MetadataRevision::new(SourceIdentifier::try_from("coinbase-v1")?),
        session: SourceIdentifier::try_from("session-1")?,
        generation: ConnectionGeneration::new(1)?,
        ordinal: NonZeroU64::MIN,
        received_at: Timestamp::from_unix_nanos(7),
        payload: vec![1, 2, 3],
    };

    assert_eq!(
        admission.preflight(&frame),
        Err(CaptureAuthorityError::GenerationNotReady)
    );
    initializer.mark_healthy()?;
    admission.preflight(&frame)?;
    assert_eq!(
        admission.issue_after_enqueue(&frame)?,
        TestReceipt(NonZeroU64::MIN)
    );
    admission.validate_active(&frame)?;

    degradation.mark_incomplete();
    assert_eq!(
        admission.validate_active(&frame),
        Err(CaptureAuthorityError::GenerationIncomplete)
    );
    assert_eq!(
        initializer.mark_healthy(),
        Err(CaptureAuthorityError::GenerationIncomplete)
    );
    Ok(())
}

#[test]
fn frame_view_exposes_exact_bounded_audit_identity() -> Result<(), Box<dyn std::error::Error>> {
    let frame = TestFrame {
        source_id: SourceId::try_from("kraken")?,
        revision: MetadataRevision::new(SourceIdentifier::try_from("book-v2")?),
        session: SourceIdentifier::try_from("session-9")?,
        generation: ConnectionGeneration::new(9)?,
        ordinal: NonZeroU64::new(41).ok_or("test ordinal must be nonzero")?,
        received_at: Timestamp::from_unix_nanos(123),
        payload: vec![4, 5, 6, 7],
    };

    assert_eq!(frame.source_id().as_str(), "kraken");
    assert_eq!(
        frame.metadata_revision().as_source_identifier().as_str(),
        "book-v2"
    );
    assert_eq!(frame.session_identifier().as_str(), "session-9");
    assert_eq!(frame.connection_generation().get(), 9);
    assert_eq!(frame.frame_ordinal().get(), 41);
    assert_eq!(frame.received_at().unix_nanos(), 123);
    assert_eq!(frame.payload(), &[4, 5, 6, 7]);
    assert!(frame.retained_bytes() >= frame.payload().len());
    Ok(())
}

#[test]
fn bundle_exposes_read_only_audit_identity_and_degradation_health()
-> Result<(), Box<dyn std::error::Error>> {
    let bundle = TestBundle::try_new()?;
    let identity = bundle.identity();
    assert_eq!(identity.source_id().as_str(), "test-source");
    assert_eq!(
        identity.metadata_revision().as_source_identifier().as_str(),
        "test-revision"
    );
    assert_eq!(identity.session_identifier().as_str(), "test-session");
    assert_eq!(identity.connection_generation().get(), 1);

    let (mut initializer, _admission, degradation) = bundle.into_parts();
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Incomplete);
    initializer.mark_healthy()?;
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Healthy);
    degradation.mark_incomplete();
    assert_eq!(degradation.integrity(), CaptureIntegrityState::Incomplete);
    Ok(())
}
