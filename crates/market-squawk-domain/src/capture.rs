//! Dependency-neutral authority contracts for asynchronous raw capture.
//!
//! These traits preserve the concrete frame-to-receipt relationship while allowing the local
//! platform crate to own source-registry capabilities without depending on the sources crate.
//! They are compile-time composition contracts, not a runtime extension registry.

use std::fmt;
use std::num::NonZeroU64;

use crate::{
    CaptureIntegrityState, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};

/// Immutable diagnostic identity of one registry-issued capture generation.
///
/// This value is deliberately data only: it supports local health reporting and journal
/// attribution but cannot establish capture or execution authority.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CaptureAuthorityIdentity {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_identifier: SourceIdentifier,
    connection_generation: ConnectionGeneration,
}

impl CaptureAuthorityIdentity {
    /// Constructs a complete diagnostic identity from validated domain components.
    pub const fn new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        session_identifier: SourceIdentifier,
        connection_generation: ConnectionGeneration,
    ) -> Self {
        Self {
            source_id,
            metadata_revision,
            session_identifier,
            connection_generation,
        }
    }

    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact immutable metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the source-defined connection-session identifier.
    pub const fn session_identifier(&self) -> &SourceIdentifier {
        &self.session_identifier
    }

    /// Returns the nonzero source connection generation.
    pub const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }
}

/// A source-registry capture authority operation failed closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureAuthorityError {
    /// Capture has not completed the supervisor-owned initialization transition.
    GenerationNotReady,
    /// The exact generation was irreversibly degraded and cannot become healthy again.
    GenerationIncomplete,
    /// A frame does not belong to the capability's exact source-session allocation.
    FrameBindingMismatch,
    /// The registry rejected other exact frame evidence required for admission.
    FrameRejected,
}

impl fmt::Display for CaptureAuthorityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationNotReady => {
                formatter.write_str("capture generation has not completed initialization")
            }
            Self::GenerationIncomplete => {
                formatter.write_str("capture generation is permanently incomplete")
            }
            Self::FrameBindingMismatch => {
                formatter.write_str("raw frame belongs to another capture allocation")
            }
            Self::FrameRejected => {
                formatter.write_str("raw frame failed capture authority validation")
            }
        }
    }
}

impl std::error::Error for CaptureAuthorityError {}

/// Bounded exact raw-frame data required by local capture.
///
/// Implementations may share immutable storage across clones. [`Self::retained_bytes`] must
/// conservatively include the frame's deep retained memory so the platform can enforce a byte
/// bound independently of its message-count bound.
pub trait RawCaptureFrameView: Clone + Send + Sync + 'static {
    /// Returns the registered source identity.
    fn source_id(&self) -> &SourceId;

    /// Returns the exact immutable source metadata revision.
    fn metadata_revision(&self) -> &MetadataRevision;

    /// Returns the source-defined connection-session identifier.
    fn session_identifier(&self) -> &SourceIdentifier;

    /// Returns the nonzero source connection generation.
    fn connection_generation(&self) -> ConnectionGeneration;

    /// Returns the nonzero, generation-local, never-reused frame ordinal.
    fn frame_ordinal(&self) -> NonZeroU64;

    /// Returns the trusted local transport receive time.
    fn received_at(&self) -> Timestamp;

    /// Returns the exact immutable transport payload.
    fn payload(&self) -> &[u8];

    /// Returns a conservative deep retained-memory charge for this frame.
    fn retained_bytes(&self) -> usize;
}

/// Supervisor-only authority for the one-way capture initialization transition.
///
/// Production implementations are non-`Clone` and non-Serde. Calling [`Self::mark_healthy`] on an
/// already healthy generation may succeed idempotently; an incomplete generation must fail.
pub trait CaptureInitializer: fmt::Debug + Send + 'static {
    /// Marks the exact capture generation healthy after its bounded writer is ready.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureAuthorityError::GenerationIncomplete`] after terminal degradation.
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError>;
}

/// Nonblocking admission authority associated with one exact frame and receipt type.
///
/// `preflight` never issues a receipt. The platform calls `issue_after_enqueue` only after the
/// bounded queue accepts the frame, then calls `validate_active` immediately before returning the
/// receipt. Registry consumption performs its own later current-allocation validation.
pub trait CaptureAdmission<Frame>: fmt::Debug + Send + 'static {
    /// Concrete non-Serde proof type accepted by the implementing registry.
    type Receipt: fmt::Debug + Send + 'static;

    /// Validates admission authority before reserving and enqueueing the frame.
    ///
    /// # Errors
    ///
    /// Fails when the generation is not healthy or the frame binding is not exact.
    fn preflight(&self, frame: &Frame) -> Result<(), CaptureAuthorityError>;

    /// Issues the concrete receipt after successful bounded enqueue.
    ///
    /// # Errors
    ///
    /// Rechecks one-way generation state and exact frame binding. Failure leaves no receipt.
    fn issue_after_enqueue(
        &mut self,
        frame: &Frame,
    ) -> Result<Self::Receipt, CaptureAuthorityError>;

    /// Rechecks the exact allocation after receipt issuance and immediately before return.
    ///
    /// # Errors
    ///
    /// Fails if concurrent degradation or generation replacement invalidated this frame.
    fn validate_active(&self, frame: &Frame) -> Result<(), CaptureAuthorityError>;
}

/// Cloneable failure-only authority for one exact capture-generation allocation.
pub trait CaptureDegradation: Clone + fmt::Debug + Send + Sync + 'static {
    /// Irreversibly marks the exact generation incomplete.
    fn mark_incomplete(&self);

    /// Returns the current one-way capture-integrity state for diagnostics and fail-closed gates.
    fn integrity(&self) -> CaptureIntegrityState;
}

/// Once-issued whole-generation capture wiring authority.
///
/// Platform channel construction and rotation accept this whole bundle. Production bundle values
/// are registry-only-constructible, non-`Clone`, and non-Serde; their consuming implementation
/// returns only capabilities tied to the same private allocation.
pub trait CaptureAuthorityBundle: fmt::Debug + Send + Sized + 'static {
    /// Exact bounded frame accepted by this authority allocation.
    type Frame: RawCaptureFrameView;
    /// Exact registry receipt issued for [`Self::Frame`].
    type Receipt: fmt::Debug + Send + 'static;
    /// Supervisor-only initialization authority.
    type Initializer: CaptureInitializer;
    /// Nonblocking admission authority bound to the frame and receipt types.
    type Admission: CaptureAdmission<Self::Frame, Receipt = Self::Receipt>;
    /// Cloneable degradation-only authority.
    type Degradation: CaptureDegradation;

    /// Returns immutable diagnostic identity without separating any authority capability.
    fn identity(&self) -> CaptureAuthorityIdentity;

    /// Consumes this once-issued bundle into capabilities for its one private allocation.
    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation);
}
