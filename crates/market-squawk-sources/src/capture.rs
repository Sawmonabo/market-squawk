//! Exact-generation raw-capture admission authority.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use market_squawk_domain::{
    CaptureAuthorityError, CaptureAuthorityIdentity, CaptureIntegrityState, DigestAlgorithm,
    EvidenceDigest, Timestamp,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{FrameId, FrameSessionBinding, RawMarketFrame};

const CAPTURE_INITIALIZING: u8 = 0;
const CAPTURE_HEALTHY: u8 = 1;
const CAPTURE_INCOMPLETE: u8 = 2;

#[derive(Debug)]
struct CaptureGenerationState {
    state: AtomicU8,
}

impl CaptureGenerationState {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(CAPTURE_INITIALIZING),
        }
    }
}

/// Observable one-way capture state for one exact connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureGenerationHealth {
    /// Capture has not completed initialization.
    Initializing,
    /// Capture admission is available and no loss is known.
    Healthy,
    /// Capture is known incomplete and can never recover in this generation.
    Incomplete,
}

/// Opaque view retained by current live authority and queued shard work.
#[derive(Clone, Debug)]
pub struct CaptureGenerationLease {
    state: Arc<CaptureGenerationState>,
}

/// Once-issued ownership bundle for wiring one exact generation's capture pipeline.
///
/// The authoritative registry is the only constructor. The bundle is deliberately neither
/// cloneable nor serializable, so composition must consume it exactly once.
#[derive(Debug)]
pub struct CaptureGenerationCapabilities {
    binding: FrameSessionBinding,
    lease: CaptureGenerationLease,
    initialization: CaptureInitializationControl,
    admission: CaptureAdmissionIssuer,
    degradation: CaptureDegradationCapability,
}

impl CaptureGenerationCapabilities {
    pub(crate) fn new(binding: FrameSessionBinding, lease: CaptureGenerationLease) -> Self {
        Self {
            binding: binding.clone(),
            lease: lease.clone(),
            initialization: CaptureInitializationControl::new(lease.clone()),
            admission: CaptureAdmissionIssuer::new(binding, lease.clone()),
            degradation: CaptureDegradationCapability::new(lease),
        }
    }

    /// Returns exact immutable generation identity without conferring registry authority.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the exact one-way generation-health lease retained by this bundle.
    pub const fn lease(&self) -> &CaptureGenerationLease {
        &self.lease
    }

    /// Consumes the once-issued bundle into its least-authority wiring parts.
    pub fn into_parts(
        self,
    ) -> (
        CaptureInitializationControl,
        CaptureAdmissionIssuer,
        CaptureDegradationCapability,
    ) {
        (self.initialization, self.admission, self.degradation)
    }
}

impl market_squawk_domain::CaptureAuthorityBundle for CaptureGenerationCapabilities {
    type Frame = RawMarketFrame;
    type Receipt = CaptureAdmissionReceipt;
    type Initializer = CaptureInitializationControl;
    type Admission = CaptureAdmissionIssuer;
    type Degradation = CaptureDegradationCapability;

    fn identity(&self) -> CaptureAuthorityIdentity {
        CaptureAuthorityIdentity::new(
            self.binding.source_id().clone(),
            self.binding.metadata_revision().clone(),
            self.binding.session_id().as_source_identifier().clone(),
            self.binding.connection_generation(),
        )
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        CaptureGenerationCapabilities::into_parts(self)
    }
}

impl CaptureGenerationLease {
    pub(crate) fn new_generation() -> Self {
        Self {
            state: Arc::new(CaptureGenerationState::new()),
        }
    }

    /// Returns current one-way generation health.
    pub fn health(&self) -> CaptureGenerationHealth {
        match self.state.state.load(Ordering::Acquire) {
            CAPTURE_INITIALIZING => CaptureGenerationHealth::Initializing,
            CAPTURE_HEALTHY => CaptureGenerationHealth::Healthy,
            _ => CaptureGenerationHealth::Incomplete,
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.health() == CaptureGenerationHealth::Healthy
    }

    pub(crate) fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Returns the conservative charge for the shared capture state and its `Arc` control block.
    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        std::mem::size_of::<CaptureGenerationState>().checked_add(
            crate::conservative_arc_control_block_charge::<CaptureGenerationState>(),
        )
    }

    pub(crate) fn mark_incomplete(&self) {
        self.state
            .state
            .store(CAPTURE_INCOMPLETE, Ordering::Release);
    }
}

/// Non-clone admission capability moved into the capture publisher composition root.
#[derive(Debug)]
pub struct CaptureAdmissionIssuer {
    binding: FrameSessionBinding,
    lease: CaptureGenerationLease,
    not_sync: PhantomData<Cell<()>>,
}

impl CaptureAdmissionIssuer {
    pub(crate) fn new(binding: FrameSessionBinding, lease: CaptureGenerationLease) -> Self {
        Self {
            binding,
            lease,
            not_sync: PhantomData,
        }
    }

    /// Checks generation health and exact frame allocation before bounded enqueue.
    ///
    /// This method never issues a receipt. The caller must successfully enqueue first and then
    /// invoke [`Self::issue_after_enqueue`].
    ///
    /// # Errors
    ///
    /// Rejects unready/incomplete capture and any frame from another allocation/generation.
    pub fn preflight(&self, frame: &RawMarketFrame) -> Result<(), CaptureAdmissionError> {
        self.validate_active(frame)
    }

    /// Issues exact admission proof only after the caller completed bounded capture-queue enqueue.
    ///
    /// The receipt is not a disk/durability acknowledgement. A later publisher failure uses the
    /// degradation capability and invalidates already queued live work for this generation.
    ///
    /// # Errors
    ///
    /// Rejects unready/incomplete capture and any frame from another allocation/generation.
    pub fn issue_after_enqueue(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<CaptureAdmissionReceipt, CaptureAdmissionError> {
        self.validate_active(frame)?;
        let digest: [u8; 32] = Sha256::digest(frame.payload()).into();
        self.validate_active(frame)?;
        Ok(CaptureAdmissionReceipt {
            binding: frame.binding().clone(),
            frame_id: frame.frame_id(),
            received_at: frame.received_at(),
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            lease: self.lease.clone(),
        })
    }

    /// Rechecks exact allocation and one-way health after receipt issuance/enqueue composition.
    ///
    /// # Errors
    ///
    /// Fails closed if degradation raced with enqueue/receipt issuance or the frame was
    /// transplanted from another generation.
    pub fn validate_active(&self, frame: &RawMarketFrame) -> Result<(), CaptureAdmissionError> {
        if !self.lease.is_healthy() {
            return Err(CaptureAdmissionError::NotHealthy);
        }
        if !self.binding.shares_allocation_with(frame.binding()) {
            return Err(CaptureAdmissionError::BindingMismatch);
        }
        Ok(())
    }
}

impl market_squawk_domain::CaptureAdmission<RawMarketFrame> for CaptureAdmissionIssuer {
    type Receipt = CaptureAdmissionReceipt;

    fn preflight(&self, frame: &RawMarketFrame) -> Result<(), CaptureAuthorityError> {
        CaptureAdmissionIssuer::preflight(self, frame).map_err(|error| self.to_domain_error(error))
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        CaptureAdmissionIssuer::issue_after_enqueue(self, frame)
            .map_err(|error| self.to_domain_error(error))
    }

    fn validate_active(&self, frame: &RawMarketFrame) -> Result<(), CaptureAuthorityError> {
        CaptureAdmissionIssuer::validate_active(self, frame)
            .map_err(|error| self.to_domain_error(error))
    }
}

impl CaptureAdmissionIssuer {
    fn to_domain_error(&self, error: CaptureAdmissionError) -> CaptureAuthorityError {
        match error {
            CaptureAdmissionError::BindingMismatch => CaptureAuthorityError::FrameBindingMismatch,
            CaptureAdmissionError::Incomplete => CaptureAuthorityError::GenerationIncomplete,
            CaptureAdmissionError::NotHealthy => match self.lease.health() {
                CaptureGenerationHealth::Initializing => CaptureAuthorityError::GenerationNotReady,
                CaptureGenerationHealth::Healthy => CaptureAuthorityError::FrameRejected,
                CaptureGenerationHealth::Incomplete => CaptureAuthorityError::GenerationIncomplete,
            },
        }
    }
}

/// Non-clone supervisor-only capture initialization capability.
///
/// This handle is never passed to raw-frame sinks or source callbacks.
#[derive(Debug)]
pub struct CaptureInitializationControl {
    lease: CaptureGenerationLease,
    not_sync: PhantomData<Cell<()>>,
}

impl CaptureInitializationControl {
    pub(crate) const fn new(lease: CaptureGenerationLease) -> Self {
        Self {
            lease,
            not_sync: PhantomData,
        }
    }

    /// Promotes initializing capture to healthy exactly once.
    ///
    /// # Errors
    ///
    /// Fails permanently after any degradation to incomplete.
    pub fn mark_healthy(&mut self) -> Result<(), CaptureAdmissionError> {
        match self.lease.state.state.compare_exchange(
            CAPTURE_INITIALIZING,
            CAPTURE_HEALTHY,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(CAPTURE_HEALTHY) => Ok(()),
            Err(_) => Err(CaptureAdmissionError::Incomplete),
        }
    }
}

impl market_squawk_domain::CaptureInitializer for CaptureInitializationControl {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError> {
        CaptureInitializationControl::mark_healthy(self).map_err(|error| match error {
            CaptureAdmissionError::Incomplete | CaptureAdmissionError::NotHealthy => {
                CaptureAuthorityError::GenerationIncomplete
            }
            CaptureAdmissionError::BindingMismatch => CaptureAuthorityError::FrameBindingMismatch,
        })
    }
}

/// Cloneable failure-only capability; it cannot initialize, promote, admit, or rotate capture.
#[derive(Clone, Debug)]
pub struct CaptureDegradationCapability {
    lease: CaptureGenerationLease,
}

impl CaptureDegradationCapability {
    pub(crate) const fn new(lease: CaptureGenerationLease) -> Self {
        Self { lease }
    }

    /// Irreversibly marks this exact generation incomplete.
    pub fn mark_incomplete(&self) {
        self.lease.mark_incomplete();
    }
}

impl market_squawk_domain::CaptureDegradation for CaptureDegradationCapability {
    fn mark_incomplete(&self) {
        CaptureDegradationCapability::mark_incomplete(self);
    }

    fn integrity(&self) -> CaptureIntegrityState {
        if self.lease.is_healthy() {
            CaptureIntegrityState::Healthy
        } else {
            CaptureIntegrityState::Incomplete
        }
    }
}

/// Owned, non-serializable proof of exact raw-frame capture admission.
#[derive(Debug)]
pub struct CaptureAdmissionReceipt {
    binding: FrameSessionBinding,
    frame_id: FrameId,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
    lease: CaptureGenerationLease,
}

impl CaptureAdmissionReceipt {
    pub(crate) const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    pub(crate) const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub(crate) const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    pub(crate) const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub(crate) const fn lease(&self) -> &CaptureGenerationLease {
        &self.lease
    }
}

/// Capture-generation state or frame-admission failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureAdmissionError {
    /// Capture has not become healthy or was degraded.
    #[error("capture generation is not healthy")]
    NotHealthy,
    /// Incomplete is terminal for the exact generation.
    #[error("capture generation is permanently incomplete")]
    Incomplete,
    /// Frame belongs to another session/generation allocation.
    #[error("raw frame belongs to another capture generation")]
    BindingMismatch,
}
