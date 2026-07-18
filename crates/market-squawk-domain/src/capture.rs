//! Dependency-neutral authority contracts for asynchronous raw capture.
//!
//! These traits preserve the concrete frame-to-receipt relationship while allowing the local
//! platform crate to own source-registry capabilities without depending on the sources crate.
//! They are compile-time composition contracts, not a runtime extension registry.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use serde::{Serialize, Serializer};

use crate::{
    CaptureIntegrityState, ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp, checked_arc_bytes_allocation_bytes,
};

use crate::RetainedLayoutError;

/// Maximum exact payload accepted at every live capture boundary.
pub const MAX_LIVE_CAPTURE_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Maximum historical payload accepted only by committed-wire compatibility readers.
pub const MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES: usize = 33_554_431;

/// The exact authority/capture graph component whose retained-size contract failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureRetainedComponent {
    /// Source/revision/session identity-owned dynamic storage.
    Identity,
    /// A source generation's shared frame-session binding.
    SessionBinding,
    /// Capture-generation state shared by least-authority handles.
    CaptureLease,
    /// Source-owned trusted-time continuity state.
    Continuity,
    /// An immutable raw payload allocation.
    Payload,
    /// One raw frame and its reachable allocations.
    Frame,
    /// One whole authority bundle and its reachable allocations.
    Bundle,
    /// Diagnostic-only authority state.
    DiagnosticState,
    /// Platform-owned capture generation state.
    PlatformGeneration,
    /// Platform-owned generation identity and resident token.
    PlatformIdentity,
}

/// A retained-size report failed closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureRetainedSizeError {
    /// Checked arithmetic overflowed while accounting the named component.
    Overflow {
        /// Component being accounted when overflow occurred.
        component: CaptureRetainedComponent,
    },
    /// Required allocation identity relationships did not describe one valid authority graph.
    InvalidAuthorityGraph {
        /// Component whose pointer/ownership graph was invalid.
        component: CaptureRetainedComponent,
    },
}

impl fmt::Display for CaptureRetainedSizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow { component } => {
                write!(formatter, "capture retained-size overflow in {component:?}")
            }
            Self::InvalidAuthorityGraph { component } => {
                write!(
                    formatter,
                    "invalid capture authority graph in {component:?}"
                )
            }
        }
    }
}

impl std::error::Error for CaptureRetainedSizeError {}

/// Marker implemented by the platform's exact resident-generation accounting token.
///
/// This marker conveys lifetime only. It grants no capture, source, strategy, risk, or execution
/// authority, and it exposes no operation that can release accounting early.
pub trait CaptureResidentToken: fmt::Debug + Send + Sync + 'static {}

/// Opaque exact resident-generation token retained by every issued receipt.
///
/// The wrapper deliberately exposes no inner value or detaching operation. Dropping the final
/// wrapper is the only way a receipt can release its share of the resident generation lifetime.
pub struct CaptureResidentGenerationLease {
    token: Arc<dyn CaptureResidentToken>,
}

impl fmt::Debug for CaptureResidentGenerationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CaptureResidentGenerationLease")
            .field("token", &"<opaque>")
            .finish()
    }
}

impl CaptureResidentGenerationLease {
    /// Erases a concrete resident token without allocating a second pointee.
    ///
    /// This is a lifetime-only wrapper. The platform remains responsible for constructing the
    /// correct accounted token and proving its pointer graph before receipt issuance.
    pub fn new<T>(token: Arc<T>) -> Self
    where
        T: CaptureResidentToken,
    {
        Self { token }
    }

    /// Returns whether this lease retains the exact concrete token allocation.
    ///
    /// The temporary Arc unsizing conversion allocates no second pointee and exposes no authority
    /// or owned token to the caller.
    pub fn shares_allocation_with<T>(&self, token: &Arc<T>) -> bool
    where
        T: CaptureResidentToken,
    {
        let concrete = Arc::clone(token);
        let erased: Arc<dyn CaptureResidentToken> = concrete;
        Arc::ptr_eq(&self.token, &erased)
    }
}

/// Receipt contract that closes resident lifetime and hidden dynamic-allocation accounting.
pub trait CaptureRetainedReceipt: fmt::Debug + Send + 'static {
    /// Borrows the exact resident-generation lease consumed during receipt issuance.
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease;

    /// Returns dynamic allocations retained beyond already funded generation-resident storage.
    ///
    /// A4 publication accepts only zero. A future nonzero receipt allocation requires an explicit
    /// reservation design before the platform may return it.
    ///
    /// # Errors
    ///
    /// Returns a typed retained-size failure when the receipt cannot prove its complete dynamic
    /// allocation contribution.
    fn checked_additional_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError>;
}

/// Closed decomposition of one frame's complete retained footprint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureFrameFootprint {
    inline_slot_funded_bytes: usize,
    resident_shared_bytes: usize,
    unique_frame_dynamic_bytes: usize,
}

impl CaptureFrameFootprint {
    /// Constructs a footprint after checking that its complete sum is representable.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureRetainedSizeError::Overflow`] when the complete frame sum overflows.
    pub fn try_new(
        inline_slot_funded_bytes: usize,
        resident_shared_bytes: usize,
        unique_frame_dynamic_bytes: usize,
    ) -> Result<Self, CaptureRetainedSizeError> {
        let footprint = Self {
            inline_slot_funded_bytes,
            resident_shared_bytes,
            unique_frame_dynamic_bytes,
        };
        let _complete = footprint.checked_complete_bytes()?;
        Ok(footprint)
    }

    /// Returns inline frame bytes already funded by its fixed queue slot.
    pub const fn inline_slot_funded_bytes(self) -> usize {
        self.inline_slot_funded_bytes
    }

    /// Returns allocations already resident in the active generation.
    pub const fn resident_shared_bytes(self) -> usize {
        self.resident_shared_bytes
    }

    /// Returns frame-exclusive dynamic allocations, including the shared conversion payload.
    pub const fn unique_frame_dynamic_bytes(self) -> usize {
        self.unique_frame_dynamic_bytes
    }

    /// Returns the checked complete retained footprint.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureRetainedSizeError::Overflow`] when the sum is not representable.
    pub fn checked_complete_bytes(self) -> Result<usize, CaptureRetainedSizeError> {
        self.inline_slot_funded_bytes
            .checked_add(self.resident_shared_bytes)
            .and_then(|bytes| bytes.checked_add(self.unique_frame_dynamic_bytes))
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })
    }
}

/// A bounded capture payload could not be normalized.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapturePayloadError {
    /// Input exceeded the named constructor's fixed policy ceiling.
    TooLarge {
        /// Actual input length in bytes.
        actual: usize,
        /// Maximum accepted length in bytes.
        maximum: NonZeroUsize,
    },
    /// The retained allocation layout could not be represented.
    RetainedLayout(RetainedLayoutError),
}

impl fmt::Display for CapturePayloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { actual, maximum } => write!(
                formatter,
                "capture payload length {actual} exceeds maximum {maximum}"
            ),
            Self::RetainedLayout(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CapturePayloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TooLarge { .. } => None,
            Self::RetainedLayout(error) => Some(error),
        }
    }
}

impl From<RetainedLayoutError> for CapturePayloadError {
    fn from(error: RetainedLayoutError) -> Self {
        Self::RetainedLayout(error)
    }
}

#[derive(Clone, Eq, PartialEq)]
enum PayloadStorage {
    Empty,
    Shared(Arc<[u8]>),
}

/// Exact bounded capture payload with a closed empty-or-right-sized shared allocation graph.
#[derive(Clone, Eq, PartialEq)]
pub struct CapturePayload(PayloadStorage);

impl fmt::Debug for CapturePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let storage = match self.0 {
            PayloadStorage::Empty => "empty",
            PayloadStorage::Shared(_) => "shared",
        };
        formatter
            .debug_struct("CapturePayload")
            .field("storage", &storage)
            .field("length", &self.as_bytes().len())
            .finish()
    }
}

impl CapturePayload {
    /// Copies one live producer payload after enforcing the fixed 4 MiB ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`CapturePayloadError::TooLarge`] before allocation when input exceeds the live
    /// ceiling, or [`CapturePayloadError::RetainedLayout`] when its allocation formula overflows.
    pub fn try_from_live(input: &[u8]) -> Result<Self, CapturePayloadError> {
        Self::try_from_bounded(input, MAX_LIVE_CAPTURE_PAYLOAD_BYTES)
    }

    /// Copies one already-committed journal payload under the historical compatibility ceiling.
    ///
    /// This constructor is for committed-wire readers. Every live frame constructor must reapply
    /// [`Self::try_from_live`] and cannot accept this value as an authority bypass.
    ///
    /// # Errors
    ///
    /// Returns [`CapturePayloadError::TooLarge`] before allocation when input exceeds the fixed
    /// compatibility ceiling, or [`CapturePayloadError::RetainedLayout`] on formula overflow.
    pub fn try_from_committed_wire(input: &[u8]) -> Result<Self, CapturePayloadError> {
        Self::try_from_bounded(input, MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES)
    }

    fn try_from_bounded(input: &[u8], maximum: usize) -> Result<Self, CapturePayloadError> {
        let maximum = NonZeroUsize::new(maximum).ok_or(CapturePayloadError::RetainedLayout(
            RetainedLayoutError::DynamicAllocationOverflow,
        ))?;
        if input.len() > maximum.get() {
            return Err(CapturePayloadError::TooLarge {
                actual: input.len(),
                maximum,
            });
        }
        if input.is_empty() {
            return Ok(Self(PayloadStorage::Empty));
        }
        let _retained = checked_arc_bytes_allocation_bytes(input.len())?;
        Ok(Self(PayloadStorage::Shared(Arc::from(input))))
    }

    /// Returns the immutable payload bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            PayloadStorage::Empty => &[],
            PayloadStorage::Shared(bytes) => bytes,
        }
    }

    /// Returns the complete Rust-visible allocation retained by this payload.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureRetainedSizeError::Overflow`] if the layout cannot be represented.
    pub fn checked_retained_allocation_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        match &self.0 {
            PayloadStorage::Empty => Ok(0),
            PayloadStorage::Shared(bytes) => checked_arc_bytes_allocation_bytes(bytes.len())
                .map_err(|_| CaptureRetainedSizeError::Overflow {
                    component: CaptureRetainedComponent::Payload,
                }),
        }
    }

    /// Returns true only when both payloads are empty or retain the same shared allocation.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (PayloadStorage::Empty, PayloadStorage::Empty) => true,
            (PayloadStorage::Shared(left), PayloadStorage::Shared(right)) => {
                Arc::ptr_eq(left, right)
            }
            (PayloadStorage::Empty, PayloadStorage::Shared(_))
            | (PayloadStorage::Shared(_), PayloadStorage::Empty) => false,
        }
    }
}

impl Serialize for CapturePayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

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

    /// Returns the checked dynamic bytes owned by the three bounded identity strings.
    ///
    /// Inline fields and any enclosing allocation are deliberately excluded.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureRetainedSizeError::Overflow`] when capacities cannot be summed.
    pub fn checked_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        self.source_id
            .retained_bytes()
            .checked_add(
                self.metadata_revision
                    .as_source_identifier()
                    .retained_bytes(),
            )
            .and_then(|bytes| bytes.checked_add(self.session_identifier.retained_bytes()))
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Identity,
            })
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
/// Implementations may share immutable storage across clones. The required footprint separates
/// fixed-slot, resident-generation, and frame-exclusive bytes so platform admission neither omits
/// nor double-charges shared allocations.
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

    /// Returns the ownership-preserving normalized payload used by record conversion.
    fn capture_payload(&self) -> &CapturePayload;

    /// Returns the complete checked footprint decomposition for this frame.
    fn checked_retained_footprint(&self)
    -> Result<CaptureFrameFootprint, CaptureRetainedSizeError>;
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
    type Receipt: CaptureRetainedReceipt;

    /// Returns allocations already resident in this exact active generation for `frame`.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureRetainedSizeError::InvalidAuthorityGraph`] unless required shared
    /// allocations are pointer-identical to the admission capability's allocations.
    fn checked_resident_shared_frame_bytes(
        &self,
        frame: &Frame,
    ) -> Result<usize, CaptureRetainedSizeError>;

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
        resident: CaptureResidentGenerationLease,
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
    type Receipt: CaptureRetainedReceipt;
    /// Supervisor-only initialization authority.
    type Initializer: CaptureInitializer;
    /// Nonblocking admission authority bound to the frame and receipt types.
    type Admission: CaptureAdmission<Self::Frame, Receipt = Self::Receipt>;
    /// Cloneable degradation-only authority.
    type Degradation: CaptureDegradation;

    /// Returns the complete retained bytes for this bundle, including its inline value.
    ///
    /// # Errors
    ///
    /// Returns a typed overflow or invalid-graph failure before the bundle is consumed.
    fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError>;

    /// Returns immutable diagnostic identity without separating any authority capability.
    fn identity(&self) -> CaptureAuthorityIdentity;

    /// Consumes this once-issued bundle into capabilities for its one private allocation.
    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation);
}
