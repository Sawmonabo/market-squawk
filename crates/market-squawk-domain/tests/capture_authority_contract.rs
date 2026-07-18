use std::alloc::Layout;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicU8, Ordering};

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureFrameFootprint, CaptureInitializer, CaptureIntegrityState,
    CapturePayload, CapturePayloadError, CaptureResidentGenerationLease, CaptureResidentToken,
    CaptureRetainedComponent, CaptureRetainedReceipt, CaptureRetainedSizeError,
    ConnectionGeneration, MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES, MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
    MetadataRevision, RawCaptureFrameView, RetainedLayoutError, SourceId, SourceIdentifier,
    Timestamp, checked_arc_bytes_allocation_bytes, checked_arc_value_allocation_bytes,
};
use static_assertions::assert_not_impl_any;

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;

#[test]
fn capture_payload_debug_is_bounded_and_redacted() -> Result<(), Box<dyn std::error::Error>> {
    let sentinel = b"broker-account-secret-sentinel";
    let payload = CapturePayload::try_from_live(sentinel)?;
    let diagnostic = format!("{payload:?}");

    assert!(diagnostic.len() < 128);
    assert!(diagnostic.contains("shared"));
    assert!(diagnostic.contains(&sentinel.len().to_string()));
    assert!(!diagnostic.contains("broker-account-secret-sentinel"));
    Ok(())
}

#[derive(Clone, Debug)]
struct TestFrame {
    source_id: SourceId,
    revision: MetadataRevision,
    session: SourceIdentifier,
    generation: ConnectionGeneration,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: CapturePayload,
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
        self.payload.as_bytes()
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
            self.payload.checked_retained_allocation_bytes()?,
        )
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

#[derive(Debug)]
struct DropObservedResidentToken(Arc<AtomicUsize>);

impl CaptureResidentToken for DropObservedResidentToken {}

impl Drop for DropObservedResidentToken {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct TestReceipt {
    ordinal: NonZeroU64,
    resident: CaptureResidentGenerationLease,
}

impl CaptureRetainedReceipt for TestReceipt {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        &self.resident
    }

    fn checked_additional_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            ordinal: _,
            resident: _,
        } = self;
        Ok(0)
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

    fn preflight(&self, _frame: &TestFrame) -> Result<(), CaptureAuthorityError> {
        self.require_healthy()
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &TestFrame,
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        self.require_healthy()?;
        Ok(TestReceipt {
            ordinal: frame.frame_ordinal(),
            resident,
        })
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

    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        Ok(std::mem::size_of::<Self>())
    }

    fn identity(&self) -> CaptureAuthorityIdentity {
        self.identity.clone()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        (self.initializer, self.admission, self.degradation)
    }
}

assert_not_impl_any!(TestBundle: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(TestReceipt: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(CaptureResidentGenerationLease: Clone, serde::Serialize, serde::de::DeserializeOwned);

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
        payload: CapturePayload::try_from_live(&[1, 2, 3])?,
    };

    assert_eq!(
        admission.preflight(&frame),
        Err(CaptureAuthorityError::GenerationNotReady)
    );
    initializer.mark_healthy()?;
    admission.preflight(&frame)?;
    let drops = Arc::new(AtomicUsize::new(0));
    let token = Arc::new(DropObservedResidentToken(Arc::clone(&drops)));
    let receipt = admission.issue_after_enqueue(
        &frame,
        CaptureResidentGenerationLease::new(Arc::clone(&token)),
    )?;
    assert_eq!(receipt.ordinal, NonZeroU64::MIN);
    assert!(
        receipt
            .resident_generation_lease()
            .shares_allocation_with(&token)
    );
    assert_eq!(receipt.checked_additional_dynamic_retained_bytes()?, 0);
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
    drop(token);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(receipt);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn opaque_resident_lease_keeps_the_exact_token_alive_without_detachment() {
    let drops = Arc::new(AtomicUsize::new(0));
    let token = Arc::new(DropObservedResidentToken(Arc::clone(&drops)));
    let receipt = TestReceipt {
        ordinal: NonZeroU64::MIN,
        resident: CaptureResidentGenerationLease::new(Arc::clone(&token)),
    };

    drop(token);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(receipt.ordinal, NonZeroU64::MIN);
    assert_eq!(format!("{receipt:?}").matches("<opaque>").count(), 1);
    assert!(!format!("{receipt:?}").contains("DropObservedResidentToken"));
    drop(receipt);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
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
        payload: CapturePayload::try_from_live(&[4, 5, 6, 7])?,
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
    assert_eq!(frame.capture_payload().as_bytes(), frame.payload());
    assert!(
        frame
            .checked_retained_footprint()?
            .checked_complete_bytes()?
            >= frame.payload().len()
    );
    Ok(())
}

#[test]
fn payload_enforces_both_frozen_limits_before_allocation() -> Result<(), Box<dyn std::error::Error>>
{
    let live_exact = vec![7_u8; MAX_LIVE_CAPTURE_PAYLOAD_BYTES];
    assert_eq!(
        CapturePayload::try_from_live(&live_exact)?.as_bytes().len(),
        MAX_LIVE_CAPTURE_PAYLOAD_BYTES
    );
    let live_one_over = vec![7_u8; MAX_LIVE_CAPTURE_PAYLOAD_BYTES + 1];
    assert_eq!(
        CapturePayload::try_from_live(&live_one_over),
        Err(CapturePayloadError::TooLarge {
            actual: MAX_LIVE_CAPTURE_PAYLOAD_BYTES + 1,
            maximum: NonZeroUsize::new(MAX_LIVE_CAPTURE_PAYLOAD_BYTES)
                .ok_or("live maximum must be nonzero")?,
        })
    );

    let compatibility_exact = vec![9_u8; MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES];
    assert_eq!(
        CapturePayload::try_from_committed_wire(&compatibility_exact)?
            .as_bytes()
            .len(),
        MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES
    );
    assert_eq!(
        CapturePayload::try_from_committed_wire(&vec![
            9_u8;
            MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES + 1
        ]),
        Err(CapturePayloadError::TooLarge {
            actual: MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES + 1,
            maximum: NonZeroUsize::new(MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES)
                .ok_or("compatibility maximum must be nonzero")?,
        })
    );
    Ok(())
}

#[test]
fn payload_empty_and_shared_storage_have_exact_allocation_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let empty = CapturePayload::try_from_live(&[])?;
    let another_empty = CapturePayload::try_from_committed_wire(&[])?;
    assert_eq!(empty.checked_retained_allocation_bytes()?, 0);
    assert!(empty.shares_allocation_with(&another_empty));

    let payload = CapturePayload::try_from_live(b"allocation identity")?;
    let clone = payload.clone();
    let distinct = CapturePayload::try_from_live(payload.as_bytes())?;
    assert!(payload.shares_allocation_with(&clone));
    assert!(!payload.shares_allocation_with(&distinct));
    assert_eq!(
        payload.checked_retained_allocation_bytes()?,
        checked_arc_bytes_allocation_bytes(payload.as_bytes().len())?
    );
    Ok(())
}

#[test]
fn arc_layout_helpers_match_checked_rust_layout_composition()
-> Result<(), Box<dyn std::error::Error>> {
    #[repr(C)]
    struct ProxyHeader {
        strong: AtomicUsize,
        weak: AtomicUsize,
    }

    #[repr(align(128))]
    struct OverAligned([u8; 3]);

    fn expected_value<T>(dynamic: usize) -> Result<usize, RetainedLayoutError> {
        let (layout, _) = Layout::new::<ProxyHeader>()
            .extend(Layout::new::<T>())
            .map_err(|_| RetainedLayoutError::LayoutOverflow)?;
        layout
            .pad_to_align()
            .size()
            .checked_add(dynamic)
            .ok_or(RetainedLayoutError::DynamicAllocationOverflow)
    }

    for dynamic in [0, 1, 4_096] {
        assert_eq!(
            checked_arc_value_allocation_bytes::<u8>(dynamic)?,
            expected_value::<u8>(dynamic)?
        );
        assert_eq!(
            checked_arc_value_allocation_bytes::<OverAligned>(dynamic)?,
            expected_value::<OverAligned>(dynamic)?
        );
    }
    let (slice_layout, _) = Layout::new::<ProxyHeader>().extend(Layout::array::<u8>(4_096)?)?;
    assert_eq!(
        checked_arc_bytes_allocation_bytes(4_096)?,
        slice_layout.pad_to_align().size()
    );
    let OverAligned(bytes) = OverAligned([1, 2, 3]);
    assert_eq!(bytes, [1, 2, 3]);
    Ok(())
}

#[test]
fn footprint_and_identity_accounting_are_checked_and_capacity_based()
-> Result<(), Box<dyn std::error::Error>> {
    let footprint = CaptureFrameFootprint::try_new(11, 13, 17)?;
    assert_eq!(footprint.inline_slot_funded_bytes(), 11);
    assert_eq!(footprint.resident_shared_bytes(), 13);
    assert_eq!(footprint.unique_frame_dynamic_bytes(), 17);
    assert_eq!(footprint.checked_complete_bytes()?, 41);
    assert_eq!(
        CaptureFrameFootprint::try_new(usize::MAX, 1, 0),
        Err(CaptureRetainedSizeError::Overflow {
            component: CaptureRetainedComponent::Frame,
        })
    );

    let source = String::with_capacity(SourceId::MAX_LENGTH);
    let revision = String::with_capacity(SourceIdentifier::MAX_LENGTH);
    let session = String::with_capacity(SourceIdentifier::MAX_LENGTH);
    let mut source = source;
    let mut revision = revision;
    let mut session = session;
    source.push('s');
    revision.push('r');
    session.push('x');
    let identity = CaptureAuthorityIdentity::new(
        SourceId::try_from(source)?,
        MetadataRevision::new(SourceIdentifier::try_from(revision)?),
        SourceIdentifier::try_from(session)?,
        ConnectionGeneration::new(1)?,
    );
    assert_eq!(
        identity.checked_dynamic_retained_bytes()?,
        SourceId::MAX_LENGTH + 2 * SourceIdentifier::MAX_LENGTH
    );
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
