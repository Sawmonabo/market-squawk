//! Explicitly non-authoritative compatibility capture composition.
//!
//! The pre-adapter application path uses this bundle only to retain MSJ1 diagnostics. Its
//! receipt is not accepted by the source registry and cannot establish current live authority.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureFrameFootprint, CaptureInitializer, CaptureIntegrityState,
    CapturePayload, CaptureResidentGenerationLease, CaptureRetainedComponent,
    CaptureRetainedReceipt, CaptureRetainedSizeError, ConnectionGeneration,
    MAX_LIVE_CAPTURE_PAYLOAD_BYTES, MetadataRevision, RawCaptureFrameView, SourceId,
    SourceIdentifier, Timestamp, checked_arc_value_allocation_bytes,
};
use thiserror::Error;

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;
const MAX_DIAGNOSTIC_FRAME_BYTES: usize = MAX_LIVE_CAPTURE_PAYLOAD_BYTES;

/// Bounded legacy-application frame that carries audit data but no registry authority.
#[derive(Clone, Debug)]
pub struct DiagnosticCaptureFrame {
    identity: CaptureAuthorityIdentity,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: CapturePayload,
}

impl DiagnosticCaptureFrame {
    /// Constructs a normalized bounded diagnostic frame.
    pub fn try_new(
        identity: CaptureAuthorityIdentity,
        ordinal: NonZeroU64,
        received_at: Timestamp,
        payload: Bytes,
    ) -> Result<Self, DiagnosticCaptureError> {
        let payload = CapturePayload::try_from_live(&payload).map_err(|_error| {
            DiagnosticCaptureError::FrameTooLarge {
                bytes: payload.len(),
                max: MAX_DIAGNOSTIC_FRAME_BYTES,
            }
        })?;
        Ok(Self {
            identity,
            ordinal,
            received_at,
            payload,
        })
    }
}

impl RawCaptureFrameView for DiagnosticCaptureFrame {
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
        self.payload.as_bytes()
    }

    fn capture_payload(&self) -> &CapturePayload {
        &self.payload
    }

    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError> {
        let identity_bytes = self.identity.checked_dynamic_retained_bytes()?;
        let unique = identity_bytes
            .checked_add(self.payload.checked_retained_allocation_bytes()?)
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })?;
        CaptureFrameFootprint::try_new(std::mem::size_of::<Self>(), 0, unique)
    }
}

#[derive(Debug)]
pub struct DiagnosticCaptureInitializer(Arc<AtomicU8>);

impl CaptureInitializer for DiagnosticCaptureInitializer {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError> {
        match self
            .0
            .compare_exchange(INITIALIZING, HEALTHY, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(HEALTHY) => Ok(()),
            Err(_) => Err(CaptureAuthorityError::GenerationIncomplete),
        }
    }
}

#[derive(Debug)]
pub struct DiagnosticCaptureAdmission {
    identity: CaptureAuthorityIdentity,
    state: Arc<AtomicU8>,
}

/// Non-Serde acknowledgement for diagnostic queue admission only.
#[derive(Debug)]
pub struct DiagnosticCaptureReceipt {
    state: Arc<AtomicU8>,
    ordinal: NonZeroU64,
    resident: CaptureResidentGenerationLease,
}

impl DiagnosticCaptureReceipt {
    /// Returns whether the diagnostic generation remains complete.
    ///
    /// This method is not a source-quality or execution-authority check.
    pub fn generation_is_complete(&self) -> bool {
        self.state.load(Ordering::Acquire) == HEALTHY
    }

    /// Returns the acknowledged generation-local diagnostic ordinal.
    pub const fn frame_ordinal(&self) -> NonZeroU64 {
        self.ordinal
    }
}

impl CaptureAdmission<DiagnosticCaptureFrame> for DiagnosticCaptureAdmission {
    type Receipt = DiagnosticCaptureReceipt;

    fn checked_resident_shared_frame_bytes(
        &self,
        _frame: &DiagnosticCaptureFrame,
    ) -> Result<usize, CaptureRetainedSizeError> {
        Ok(0)
    }

    fn preflight(&self, frame: &DiagnosticCaptureFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &DiagnosticCaptureFrame,
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        self.validate(frame)?;
        Ok(DiagnosticCaptureReceipt {
            state: Arc::clone(&self.state),
            ordinal: frame.ordinal,
            resident,
        })
    }

    fn validate_active(&self, frame: &DiagnosticCaptureFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }
}

impl CaptureRetainedReceipt for DiagnosticCaptureReceipt {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        &self.resident
    }

    fn checked_additional_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            state: _,
            ordinal: _,
            resident: _,
        } = self;
        Ok(0)
    }
}

impl DiagnosticCaptureAdmission {
    fn validate(&self, frame: &DiagnosticCaptureFrame) -> Result<(), CaptureAuthorityError> {
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
pub struct DiagnosticCaptureDegradation(Arc<AtomicU8>);

impl CaptureDegradation for DiagnosticCaptureDegradation {
    fn mark_incomplete(&self) {
        self.0.store(INCOMPLETE, Ordering::Release);
    }

    fn integrity(&self) -> CaptureIntegrityState {
        if self.0.load(Ordering::Acquire) == HEALTHY {
            CaptureIntegrityState::Healthy
        } else {
            CaptureIntegrityState::Incomplete
        }
    }
}

/// Whole diagnostic bundle; its types are disjoint from registry current-source authority.
#[derive(Debug)]
pub struct DiagnosticCaptureBundle {
    identity: CaptureAuthorityIdentity,
    initializer: DiagnosticCaptureInitializer,
    admission: DiagnosticCaptureAdmission,
    degradation: DiagnosticCaptureDegradation,
}

impl DiagnosticCaptureBundle {
    /// Constructs one diagnostic-only generation bundle.
    pub fn new(identity: CaptureAuthorityIdentity) -> Self {
        let state = Arc::new(AtomicU8::new(INITIALIZING));
        Self {
            identity: identity.clone(),
            initializer: DiagnosticCaptureInitializer(Arc::clone(&state)),
            admission: DiagnosticCaptureAdmission {
                identity,
                state: Arc::clone(&state),
            },
            degradation: DiagnosticCaptureDegradation(state),
        }
    }
}

impl CaptureAuthorityBundle for DiagnosticCaptureBundle {
    type Frame = DiagnosticCaptureFrame;
    type Receipt = DiagnosticCaptureReceipt;
    type Initializer = DiagnosticCaptureInitializer;
    type Admission = DiagnosticCaptureAdmission;
    type Degradation = DiagnosticCaptureDegradation;

    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let Self {
            identity,
            initializer,
            admission,
            degradation,
        } = self;
        if !Arc::ptr_eq(&initializer.0, &admission.state)
            || !Arc::ptr_eq(&initializer.0, &degradation.0)
        {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::DiagnosticState,
            });
        }
        if identity != &admission.identity {
            return Err(CaptureRetainedSizeError::InvalidAuthorityGraph {
                component: CaptureRetainedComponent::Identity,
            });
        }
        let state_bytes = checked_arc_value_allocation_bytes::<AtomicU8>(0).map_err(|_| {
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::DiagnosticState,
            }
        })?;
        let admission_identity_bytes = admission.identity.checked_dynamic_retained_bytes()?;
        std::mem::size_of::<Self>()
            .checked_add(identity.checked_dynamic_retained_bytes()?)
            .and_then(|bytes| bytes.checked_add(admission_identity_bytes))
            .and_then(|bytes| bytes.checked_add(state_bytes))
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Bundle,
            })
    }

    fn identity(&self) -> CaptureAuthorityIdentity {
        self.identity.clone()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        (self.initializer, self.admission, self.degradation)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bytes::Bytes;
    use market_squawk_domain::{
        CaptureAuthorityIdentity, ConnectionGeneration, MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
        MetadataRevision, RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
    };

    use super::DiagnosticCaptureFrame;

    fn identity() -> Result<CaptureAuthorityIdentity, Box<dyn std::error::Error>> {
        Ok(CaptureAuthorityIdentity::new(
            SourceId::try_from("diagnostic-boundary")?,
            MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
            SourceIdentifier::try_from("session-1")?,
            ConnectionGeneration::new(1)?,
        ))
    }

    #[test]
    fn diagnostic_frame_accepts_live_exact_and_rejects_one_over()
    -> Result<(), Box<dyn std::error::Error>> {
        let exact = DiagnosticCaptureFrame::try_new(
            identity()?,
            NonZeroU64::MIN,
            Timestamp::from_unix_nanos(1),
            Bytes::from(vec![0_u8; MAX_LIVE_CAPTURE_PAYLOAD_BYTES]),
        )?;
        assert_eq!(
            exact.payload.as_bytes().len(),
            MAX_LIVE_CAPTURE_PAYLOAD_BYTES
        );
        assert!(
            DiagnosticCaptureFrame::try_new(
                identity()?,
                NonZeroU64::MIN,
                Timestamp::from_unix_nanos(1),
                Bytes::from(vec![0_u8; MAX_LIVE_CAPTURE_PAYLOAD_BYTES + 1]),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn diagnostic_frame_accessors_and_footprint_are_stable_across_clone()
    -> Result<(), Box<dyn std::error::Error>> {
        let frame = DiagnosticCaptureFrame::try_new(
            identity()?,
            NonZeroU64::MIN,
            Timestamp::from_unix_nanos(1),
            Bytes::from_static(b"stable-diagnostic-frame"),
        )?;
        let source_pointer = frame.source_id().as_str().as_ptr();
        let payload_pointer = frame.payload().as_ptr();
        let footprint = frame.checked_retained_footprint()?;
        for _iteration in 0..3 {
            assert_eq!(frame.source_id().as_str().as_ptr(), source_pointer);
            assert_eq!(frame.payload().as_ptr(), payload_pointer);
            assert_eq!(frame.capture_payload().as_bytes().as_ptr(), payload_pointer);
            assert_eq!(frame.checked_retained_footprint()?, footprint);
        }

        let cloned = frame.clone();
        assert_eq!(cloned.source_id(), frame.source_id());
        assert_eq!(cloned.metadata_revision(), frame.metadata_revision());
        assert_eq!(cloned.session_identifier(), frame.session_identifier());
        assert_eq!(
            cloned.connection_generation(),
            frame.connection_generation()
        );
        assert_eq!(cloned.frame_ordinal(), frame.frame_ordinal());
        assert_eq!(cloned.received_at(), frame.received_at());
        assert!(
            cloned
                .capture_payload()
                .shares_allocation_with(frame.capture_payload())
        );
        assert_eq!(cloned.checked_retained_footprint()?, footprint);
        Ok(())
    }
}

/// Diagnostic frame construction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DiagnosticCaptureError {
    /// Frame exceeds the fixed compatibility capture ceiling.
    #[error("diagnostic capture frame is {bytes} bytes; maximum is {max}")]
    FrameTooLarge {
        /// Visible payload bytes.
        bytes: usize,
        /// Configured maximum.
        max: usize,
    },
}
