//! Exact-generation raw-capture admission authority.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use market_squawk_domain::{
    CaptureAuthorityError, CaptureAuthorityIdentity, CaptureIntegrityState,
    CaptureResidentGenerationLease, CaptureRetainedComponent, CaptureRetainedReceipt,
    CaptureRetainedSizeError, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
    checked_arc_value_allocation_bytes,
};
use market_squawk_platform::RawCaptureRecord;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::authority_time::{
    AuthorityTimeContinuity, TrustedReceiptObservation, TrustedRegistryTime,
};
use crate::{FrameId, FrameSessionBinding, ProviderEventMicrobatchMaterial, RawMarketFrame};

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
    continuity: AuthorityTimeContinuity,
    session_started_at: TrustedRegistryTime,
}

/// Once-issued ownership bundle for wiring one exact generation's capture pipeline.
///
/// The authoritative registry is the only constructor. The bundle is deliberately neither
/// cloneable nor serializable, so composition must consume it exactly once.
#[derive(Debug)]
pub struct CaptureGenerationCapabilities {
    binding: FrameSessionBinding,
    continuity: AuthorityTimeContinuity,
    lease: CaptureGenerationLease,
    initialization: CaptureInitializationControl,
    admission: CaptureAdmissionIssuer,
    degradation: CaptureDegradationCapability,
}

impl CaptureGenerationCapabilities {
    pub(crate) fn new(binding: FrameSessionBinding, lease: CaptureGenerationLease) -> Self {
        Self {
            binding: binding.clone(),
            continuity: lease.continuity.clone(),
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

    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            binding,
            continuity,
            lease,
            initialization,
            admission,
            degradation,
        } = self;
        if !binding.shares_allocation_with(&admission.binding) {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::SessionBinding,
            });
        }
        if !lease.shares_allocation_with(&initialization.lease)
            || !lease.shares_allocation_with(&admission.lease)
            || !lease.shares_allocation_with(&degradation.lease)
        {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::CaptureLease,
            });
        }
        if !continuity.shares_allocation_with(&lease.continuity)
            || !continuity.shares_allocation_with(&initialization.lease.continuity)
            || !continuity.shares_allocation_with(&admission.lease.continuity)
            || !continuity.shares_allocation_with(&degradation.lease.continuity)
        {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Continuity,
            });
        }
        let lease_bytes = lease.checked_shared_allocation_bytes()?;
        let continuity_bytes = continuity.checked_shared_allocation_bytes()?;
        std::mem::size_of::<Self>()
            .checked_add(binding.checked_shared_allocation_bytes()?)
            .and_then(|bytes| bytes.checked_add(lease_bytes))
            .and_then(|bytes| bytes.checked_add(continuity_bytes))
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Bundle,
            })
    }

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
    pub(crate) fn new_generation(
        continuity: AuthorityTimeContinuity,
        session_started_at: TrustedRegistryTime,
    ) -> Self {
        Self {
            state: Arc::new(CaptureGenerationState::new()),
            continuity,
            session_started_at,
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
        self.health() == CaptureGenerationHealth::Healthy && self.continuity.is_continuous()
    }

    pub(crate) fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
            && self.continuity.shares_allocation_with(&other.continuity)
            && self.session_started_at == other.session_started_at
    }

    pub(crate) fn is_bound_to(
        &self,
        continuity: &AuthorityTimeContinuity,
        session_started_at: TrustedRegistryTime,
    ) -> bool {
        self.continuity.shares_allocation_with(continuity)
            && self.session_started_at == session_started_at
    }

    pub(crate) fn validate_receipt(
        &self,
        receipt: &TrustedReceiptObservation,
    ) -> Result<(), crate::RegistryError> {
        self.continuity
            .validate_receipt(receipt, self.session_started_at)
    }

    /// Returns the conservative charge for the shared capture state and its `Arc` control block.
    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        self.checked_shared_allocation_bytes().ok()
    }

    fn checked_shared_allocation_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        checked_arc_value_allocation_bytes::<CaptureGenerationState>(0).map_err(|_| {
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::CaptureLease,
            }
        })
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
    connection_id: Uuid,
    not_sync: PhantomData<Cell<()>>,
}

impl CaptureAdmissionIssuer {
    pub(crate) fn new(binding: FrameSessionBinding, lease: CaptureGenerationLease) -> Self {
        Self {
            binding,
            lease,
            connection_id: Uuid::new_v4(),
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
        resident: CaptureResidentGenerationLease,
    ) -> Result<CaptureAdmissionReceipt, CaptureAdmissionError> {
        self.validate_active(frame)?;
        let digest: [u8; 32] = Sha256::digest(frame.payload()).into();
        self.validate_active(frame)?;
        let event_id = Uuid::new_v4();
        Ok(CaptureAdmissionReceipt {
            binding: frame.binding().clone(),
            frame_id: frame.frame_id(),
            receipt: frame
                .trusted_receipt()
                .ok_or(CaptureAdmissionError::TrustedTimeInvalid)?
                .clone(),
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            material_identity: Some(CaptureProviderEventIdentity {
                connection_id: self.connection_id,
                event_id,
            }),
            lease: self.lease.clone(),
            resident,
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
        let receipt = frame
            .trusted_receipt()
            .ok_or(CaptureAdmissionError::TrustedTimeInvalid)?;
        self.lease
            .validate_receipt(receipt)
            .map_err(|_error| CaptureAdmissionError::TrustedTimeInvalid)?;
        Ok(())
    }
}

impl market_squawk_domain::CaptureAdmission<RawMarketFrame> for CaptureAdmissionIssuer {
    type Receipt = CaptureAdmissionReceipt;

    fn checked_resident_shared_frame_bytes(
        &self,
        frame: &RawMarketFrame,
    ) -> Result<usize, CaptureRetainedSizeError> {
        if !self.binding.shares_allocation_with(frame.binding()) {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::SessionBinding,
            });
        }
        let receipt =
            frame
                .trusted_receipt()
                .ok_or(CaptureRetainedSizeError::InvalidAuthorityGraph {
                    component: CaptureRetainedComponent::Continuity,
                })?;
        if !self
            .lease
            .continuity
            .shares_allocation_with(receipt.continuity())
        {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Continuity,
            });
        }
        self.binding
            .checked_shared_allocation_bytes()?
            .checked_add(self.lease.continuity.checked_shared_allocation_bytes()?)
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Continuity,
            })
    }

    fn preflight(&self, frame: &RawMarketFrame) -> Result<(), CaptureAuthorityError> {
        CaptureAdmissionIssuer::preflight(self, frame).map_err(|error| self.to_domain_error(error))
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &RawMarketFrame,
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        CaptureAdmissionIssuer::issue_after_enqueue(self, frame, resident)
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
            CaptureAdmissionError::FrameIdentityMismatch => {
                CaptureAuthorityError::FrameBindingMismatch
            }
            CaptureAdmissionError::Incomplete => CaptureAuthorityError::GenerationIncomplete,
            CaptureAdmissionError::TrustedTimeInvalid
            | CaptureAdmissionError::PayloadMismatch
            | CaptureAdmissionError::MaterializationFailed
            | CaptureAdmissionError::MaterialAlreadyIssued => CaptureAuthorityError::FrameRejected,
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
            CaptureAdmissionError::Incomplete
            | CaptureAdmissionError::NotHealthy
            | CaptureAdmissionError::TrustedTimeInvalid
            | CaptureAdmissionError::PayloadMismatch
            | CaptureAdmissionError::MaterializationFailed
            | CaptureAdmissionError::MaterialAlreadyIssued => {
                CaptureAuthorityError::GenerationIncomplete
            }
            CaptureAdmissionError::BindingMismatch
            | CaptureAdmissionError::FrameIdentityMismatch => {
                CaptureAuthorityError::FrameBindingMismatch
            }
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

/// Capture-minted raw-envelope identities available to exactly one material issuance.
#[derive(Debug)]
struct CaptureProviderEventIdentity {
    connection_id: Uuid,
    event_id: Uuid,
}

/// One-use capture-owned raw-envelope identity for an already admitted exact frame.
///
/// This capability carries no payload bytes and cannot admit, decode, or publish data. It exists
/// only for application composition that must bind an adapter-owned logical handoff back to the
/// physical connection/event identities minted by the capture generation.
#[derive(Debug)]
pub struct CaptureProviderEventIdentityClaim {
    binding: FrameSessionBinding,
    frame_id: FrameId,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
    connection_id: Uuid,
    event_id: Uuid,
}

impl CaptureProviderEventIdentityClaim {
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    pub const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }
}

/// Owned, non-serializable proof of exact raw-frame capture admission.
#[derive(Debug)]
pub struct CaptureAdmissionReceipt {
    binding: FrameSessionBinding,
    frame_id: FrameId,
    receipt: TrustedReceiptObservation,
    payload_digest: EvidenceDigest,
    material_identity: Option<CaptureProviderEventIdentity>,
    lease: CaptureGenerationLease,
    resident: CaptureResidentGenerationLease,
}

impl CaptureRetainedReceipt for CaptureAdmissionReceipt {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        &self.resident
    }

    fn checked_additional_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            binding: _,
            frame_id: _,
            receipt: _,
            payload_digest: _,
            material_identity: _,
            lease: _,
            resident: _,
        } = self;
        Ok(0)
    }
}

impl CaptureAdmissionReceipt {
    pub(crate) const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    pub(crate) const fn received_at(&self) -> Timestamp {
        self.receipt.received_at()
    }

    pub(crate) const fn trusted_receipt(&self) -> &TrustedReceiptObservation {
        &self.receipt
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

    /// Issues the capture-minted connection/event identity once for an exact admitted frame.
    ///
    /// The receipt remains available for mandatory authoritative-registry consumption. Successful
    /// issuance prevents the same identity from also being issued as provider-event material.
    pub fn try_issue_provider_event_identity_claim(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<CaptureProviderEventIdentityClaim, CaptureAdmissionError> {
        self.validate_material_frame(frame)?;
        self.validate_material_identity()?;
        let identity = self
            .material_identity
            .take()
            .ok_or(CaptureAdmissionError::MaterialAlreadyIssued)?;
        Ok(CaptureProviderEventIdentityClaim {
            binding: self.binding.clone(),
            frame_id: self.frame_id,
            received_at: self.receipt.received_at(),
            payload_digest: self.payload_digest,
            connection_id: identity.connection_id,
            event_id: identity.event_id,
        })
    }

    /// Issues exact durable provider-event material once without consuming registry admission.
    ///
    /// Source identity, metadata revision, event identity, connection identity, receive time, and
    /// payload all come from the exact capture generation and frame. The caller supplies only the
    /// bounded code-owned logical dataset and stream identifiers. Raw transport does not own a
    /// provider sequence or exchange timestamp, so both remain absent. Successful issuance clears
    /// the private material identity; the receipt remains available for its mandatory later
    /// authoritative-registry consumption.
    ///
    /// # Errors
    ///
    /// Rejects degraded generations, transplanted frames, mismatched frame/receipt/payload
    /// evidence, nil capture-owned identities, a second issuance, and any failure to preserve the
    /// exact raw frame. Any failure before successful construction retains the one-use material
    /// identity so the exact same admitted frame may be retried.
    pub fn try_issue_provider_event_microbatch_material(
        &mut self,
        frame: &RawMarketFrame,
        dataset: SourceIdentifier,
        stream_identity: SourceIdentifier,
    ) -> Result<ProviderEventMicrobatchMaterial, CaptureAdmissionError> {
        self.validate_material_frame(frame)?;
        self.validate_material_identity()?;
        let identity = self
            .material_identity
            .as_ref()
            .ok_or(CaptureAdmissionError::MaterialAlreadyIssued)?;
        let record = RawCaptureRecord::try_from_exact_capture_frame(
            identity.event_id,
            identity.connection_id,
            frame,
        )
        .map_err(|_error| CaptureAdmissionError::MaterializationFailed)?;
        if record.event_id() != identity.event_id
            || record.connection_id() != identity.connection_id
            || record.source() != self.binding.source_id().as_str()
            || record.source_sequence().is_some()
            || record.exchange_at().is_some()
            || record.received_at().timestamp_nanos_opt()
                != Some(self.receipt.received_at().unix_nanos())
            || record.payload() != frame.payload()
        {
            return Err(CaptureAdmissionError::MaterializationFailed);
        }
        let material = ProviderEventMicrobatchMaterial::try_new(
            self.binding.source_id().clone(),
            self.binding.metadata_revision().clone(),
            dataset,
            stream_identity,
            vec![record],
        )
        .map_err(|_error| CaptureAdmissionError::MaterializationFailed)?;
        let [material_frame] = material.receipt().frames() else {
            return Err(CaptureAdmissionError::MaterializationFailed);
        };
        let payload_bytes = u64::try_from(frame.payload().len())
            .map_err(|_error| CaptureAdmissionError::MaterializationFailed)?;
        if material.receipt().source_id() != self.binding.source_id()
            || material.receipt().metadata_revision() != self.binding.metadata_revision()
            || material_frame.event_id() != *identity.event_id.as_bytes()
            || material_frame.connection_id() != *identity.connection_id.as_bytes()
            || material_frame.source_sequence().is_some()
            || material_frame.exchange_at().is_some()
            || material_frame.received_at() != self.receipt.received_at()
            || material_frame.payload_bytes() != payload_bytes
            || material_frame.payload_digest() != self.payload_digest
        {
            return Err(CaptureAdmissionError::MaterializationFailed);
        }
        self.material_identity = None;
        Ok(material)
    }

    fn validate_material_identity(&self) -> Result<(), CaptureAdmissionError> {
        let identity = self
            .material_identity
            .as_ref()
            .ok_or(CaptureAdmissionError::MaterialAlreadyIssued)?;
        if identity.connection_id.is_nil() || identity.event_id.is_nil() {
            Err(CaptureAdmissionError::MaterializationFailed)
        } else {
            Ok(())
        }
    }

    fn validate_material_frame(&self, frame: &RawMarketFrame) -> Result<(), CaptureAdmissionError> {
        if !self.lease.is_healthy() {
            return Err(CaptureAdmissionError::NotHealthy);
        }
        if !self.binding.shares_allocation_with(frame.binding())
            || self.binding.source_id() != frame.source_id()
            || self.binding.metadata_revision() != frame.metadata_revision()
            || self.binding.session_id() != frame.session_id()
            || self.binding.connection_generation() != frame.connection_generation()
        {
            return Err(CaptureAdmissionError::BindingMismatch);
        }
        if self.frame_id != frame.frame_id() {
            return Err(CaptureAdmissionError::FrameIdentityMismatch);
        }
        let frame_receipt = frame
            .trusted_receipt()
            .ok_or(CaptureAdmissionError::TrustedTimeInvalid)?;
        if frame_receipt != &self.receipt {
            return Err(CaptureAdmissionError::TrustedTimeInvalid);
        }
        self.lease
            .validate_receipt(&self.receipt)
            .map_err(|_error| CaptureAdmissionError::TrustedTimeInvalid)?;
        let digest: [u8; 32] = Sha256::digest(frame.payload()).into();
        if self.payload_digest != EvidenceDigest::new(DigestAlgorithm::Sha256, digest) {
            return Err(CaptureAdmissionError::PayloadMismatch);
        }
        Ok(())
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
    /// Receipt and frame have different exact generation-local identities.
    #[error("raw frame identity does not match its capture admission receipt")]
    FrameIdentityMismatch,
    /// Frame has no source-owned receipt proof or belongs to another continuity lineage.
    #[error("raw frame trusted-time continuity proof is invalid")]
    TrustedTimeInvalid,
    /// Exact raw payload bytes no longer match the digest issued after enqueue.
    #[error("raw frame payload does not match its capture admission receipt")]
    PayloadMismatch,
    /// Exact admitted raw evidence could not be represented without weakening its invariants.
    #[error("admitted raw frame could not become provider event material")]
    MaterializationFailed,
    /// The receipt already issued its capture-minted raw-envelope identity.
    #[error("provider event material was already issued for this capture admission receipt")]
    MaterialAlreadyIssued,
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bytes::Bytes;
    use market_squawk_domain::{
        CaptureAdmission, CaptureAuthorityBundle, CaptureRetainedComponent,
        CaptureRetainedSizeError, ConnectionGeneration, MetadataRevision, SourceId,
        SourceIdentifier, Timestamp,
    };

    use super::{CaptureAdmissionIssuer, CaptureGenerationCapabilities, CaptureGenerationLease};
    use crate::authority_time::{TrustedReceiptObservation, trusted_test_receipt};
    use crate::{FrameId, FrameSessionBinding, RawMarketFrame, SessionId, TransportFrameKind};

    fn binding(session: &str) -> Result<FrameSessionBinding, Box<dyn std::error::Error>> {
        Ok(FrameSessionBinding::new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-a")?),
            SessionId::new(SourceIdentifier::try_from(session)?),
            ConnectionGeneration::new(1)?,
        ))
    }

    fn receipt() -> Result<TrustedReceiptObservation, Box<dyn std::error::Error>> {
        Ok(trusted_test_receipt(Timestamp::from_unix_nanos(1), 1)?)
    }

    fn generation()
    -> Result<(CaptureGenerationLease, TrustedReceiptObservation), Box<dyn std::error::Error>> {
        let receipt = receipt()?;
        let lease =
            CaptureGenerationLease::new_generation(receipt.continuity().clone(), receipt.time());
        Ok((lease, receipt))
    }

    fn frame(
        binding: FrameSessionBinding,
        receipt: TrustedReceiptObservation,
    ) -> Result<RawMarketFrame, Box<dyn std::error::Error>> {
        Ok(RawMarketFrame::try_from_parts(
            binding,
            FrameId::new(NonZeroU64::MIN),
            receipt,
            TransportFrameKind::Binary,
            Bytes::from_static(b"frame"),
        )?)
    }

    fn assert_invalid_component(
        bundle: &CaptureGenerationCapabilities,
        component: CaptureRetainedComponent,
    ) {
        assert_eq!(
            bundle.checked_retained_bytes(),
            Err(CaptureRetainedSizeError::InvalidAuthorityGraph { component })
        );
    }

    #[test]
    fn admission_resident_charge_requires_the_exact_frame_binding_pointer()
    -> Result<(), Box<dyn std::error::Error>> {
        let issuer_binding = binding("session-a")?;
        let (lease, receipt) = generation()?;
        let expected = issuer_binding
            .checked_shared_allocation_bytes()?
            .checked_add(receipt.continuity().checked_shared_allocation_bytes()?)
            .ok_or("resident expected total overflowed")?;
        let issuer = CaptureAdmissionIssuer::new(issuer_binding.clone(), lease);
        let exact = frame(issuer_binding, receipt.clone())?;
        let copied_identity = frame(binding("session-a")?, receipt)?;

        assert_eq!(
            issuer.checked_resident_shared_frame_bytes(&exact)?,
            expected
        );
        assert_eq!(
            issuer.checked_resident_shared_frame_bytes(&copied_identity),
            Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::SessionBinding,
            })
        );
        Ok(())
    }

    #[test]
    fn production_bundle_formula_is_exact_and_rejects_every_pointer_edge()
    -> Result<(), Box<dyn std::error::Error>> {
        let exact_binding = binding("session-a")?;
        let (exact_lease, exact_receipt) = generation()?;
        let exact = CaptureGenerationCapabilities::new(exact_binding.clone(), exact_lease.clone());
        let expected = std::mem::size_of::<CaptureGenerationCapabilities>()
            .checked_add(exact_binding.checked_shared_allocation_bytes()?)
            .and_then(|bytes| {
                bytes.checked_add(exact_lease.checked_shared_allocation_bytes().ok()?)
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    exact_receipt
                        .continuity()
                        .checked_shared_allocation_bytes()
                        .ok()?,
                )
            })
            .ok_or("bundle expected total overflowed")?;
        assert_eq!(exact.checked_retained_bytes()?, expected);

        let (lease, _receipt) = generation()?;
        let mut wrong_binding = CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        wrong_binding.admission.binding = binding("session-a")?;
        assert_invalid_component(&wrong_binding, CaptureRetainedComponent::SessionBinding);

        let (lease, _receipt) = generation()?;
        let mut wrong_top_lease = CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        wrong_top_lease.lease = generation()?.0;
        assert_invalid_component(&wrong_top_lease, CaptureRetainedComponent::CaptureLease);

        let (lease, _receipt) = generation()?;
        let mut wrong_initializer =
            CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        wrong_initializer.initialization.lease = generation()?.0;
        assert_invalid_component(&wrong_initializer, CaptureRetainedComponent::CaptureLease);

        let (lease, _receipt) = generation()?;
        let mut wrong_admission = CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        wrong_admission.admission.lease = generation()?.0;
        assert_invalid_component(&wrong_admission, CaptureRetainedComponent::CaptureLease);

        let (lease, _receipt) = generation()?;
        let mut wrong_degradation =
            CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        wrong_degradation.degradation.lease = generation()?.0;
        assert_invalid_component(&wrong_degradation, CaptureRetainedComponent::CaptureLease);

        let (lease, _receipt) = generation()?;
        let mut wrong_continuity = CaptureGenerationCapabilities::new(exact_binding, lease);
        wrong_continuity.continuity = receipt()?.continuity().clone();
        assert_invalid_component(&wrong_continuity, CaptureRetainedComponent::Continuity);
        Ok(())
    }

    #[test]
    fn capture_rejects_wrong_and_missing_continuity_with_the_exact_binding_pointer()
    -> Result<(), Box<dyn std::error::Error>> {
        let exact_binding = binding("session-a")?;
        let (lease, exact_receipt) = generation()?;
        let capabilities = CaptureGenerationCapabilities::new(exact_binding.clone(), lease);
        let (mut initialization, admission, _degradation) = capabilities.into_parts();
        initialization.mark_healthy()?;

        let wrong = frame(exact_binding.clone(), receipt()?)?;
        assert_eq!(
            admission.preflight(&wrong),
            Err(super::CaptureAdmissionError::TrustedTimeInvalid)
        );
        assert_eq!(
            admission.checked_resident_shared_frame_bytes(&wrong),
            Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Continuity,
            })
        );

        let mut missing = frame(exact_binding, exact_receipt)?;
        missing.strip_trusted_receipt_for_test();
        assert_eq!(
            admission.preflight(&missing),
            Err(super::CaptureAdmissionError::TrustedTimeInvalid)
        );
        assert_eq!(
            admission.checked_resident_shared_frame_bytes(&missing),
            Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Continuity,
            })
        );
        Ok(())
    }
}
