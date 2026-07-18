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
    CaptureDegradation, CaptureInitializer, CaptureIntegrityState, ConnectionGeneration,
    MetadataRevision, RawCaptureFrameView, SourceId, SourceIdentifier, Timestamp,
};
use thiserror::Error;

const INITIALIZING: u8 = 0;
const HEALTHY: u8 = 1;
const INCOMPLETE: u8 = 2;
const MAX_DIAGNOSTIC_FRAME_BYTES: usize = 4 * 1024 * 1024;

/// Bounded legacy-application frame that carries audit data but no registry authority.
#[derive(Clone, Debug)]
pub struct DiagnosticCaptureFrame {
    identity: CaptureAuthorityIdentity,
    ordinal: NonZeroU64,
    received_at: Timestamp,
    payload: Bytes,
}

impl DiagnosticCaptureFrame {
    /// Constructs a normalized bounded diagnostic frame.
    pub fn try_new(
        identity: CaptureAuthorityIdentity,
        ordinal: NonZeroU64,
        received_at: Timestamp,
        payload: Bytes,
    ) -> Result<Self, DiagnosticCaptureError> {
        if payload.len() > MAX_DIAGNOSTIC_FRAME_BYTES {
            return Err(DiagnosticCaptureError::FrameTooLarge {
                bytes: payload.len(),
                max: MAX_DIAGNOSTIC_FRAME_BYTES,
            });
        }
        Ok(Self {
            identity,
            ordinal,
            received_at,
            payload: Bytes::copy_from_slice(&payload),
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
        &self.payload
    }

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.payload.len())
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

    fn preflight(&self, frame: &DiagnosticCaptureFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
    }

    fn issue_after_enqueue(
        &mut self,
        frame: &DiagnosticCaptureFrame,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        self.validate(frame)?;
        Ok(DiagnosticCaptureReceipt {
            state: Arc::clone(&self.state),
            ordinal: frame.ordinal,
        })
    }

    fn validate_active(&self, frame: &DiagnosticCaptureFrame) -> Result<(), CaptureAuthorityError> {
        self.validate(frame)
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

    fn identity(&self) -> CaptureAuthorityIdentity {
        self.identity.clone()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        (self.initializer, self.admission, self.degradation)
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
