//! Owned, non-serializable evidence of bounded raw-frame admission.

use std::{
    fmt,
    sync::{Arc, atomic::Ordering},
};

use market_squawk_domain::CaptureIntegrityState;
use uuid::Uuid;

use super::{CaptureGenerationKey, GenerationCaptureState};

/// Owned evidence that one exact raw frame was admitted to bounded capture.
///
/// The receipt is deliberately non-Serde and non-`Clone`. It is not execution authority. Task 5's
/// registry-owned one-way lease consumes this evidence when binding capture admission to current
/// source authority.
pub struct CaptureAdmissionReceipt {
    pub(super) allocation: Arc<GenerationCaptureState>,
    pub(super) event_id: Uuid,
    pub(super) source_sequence: Option<u64>,
    pub(super) received_at: chrono::DateTime<chrono::Utc>,
    pub(super) payload_digest: [u8; 32],
}

impl PartialEq for CaptureAdmissionReceipt {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.allocation, &other.allocation)
            && self.event_id == other.event_id
            && self.source_sequence == other.source_sequence
            && self.received_at == other.received_at
            && self.payload_digest == other.payload_digest
    }
}

impl Eq for CaptureAdmissionReceipt {}

impl fmt::Debug for CaptureAdmissionReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CaptureAdmissionReceipt")
            .field("key", &self.allocation.key)
            .field("event_id", &self.event_id)
            .field("source_sequence", &self.source_sequence)
            .field("received_at", &self.received_at)
            .field("payload_digest", &"[SHA-256 DIGEST OMITTED]")
            .finish()
    }
}

impl CaptureAdmissionReceipt {
    /// Returns the exact capture allocation that admitted the frame.
    pub fn key(&self) -> &CaptureGenerationKey {
        &self.allocation.key
    }

    /// Returns the exact local event identity.
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    /// Returns the source sequence carried by the raw frame, when supplied.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    /// Returns the socket-boundary receive timestamp for this frame.
    pub const fn received_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.received_at
    }

    /// Returns the exact SHA-256 digest of the admitted raw frame.
    pub const fn payload_digest(&self) -> &[u8; 32] {
        &self.payload_digest
    }

    /// Returns whether this exact one-way allocation remains healthy.
    ///
    /// A later rotation, overflow, writer failure, or shutdown invalidates an already-issued
    /// receipt through the retained allocation state.
    pub fn allocation_is_healthy(&self) -> bool {
        self.allocation.accepting.load(Ordering::Acquire)
            && self.allocation.integrity() == CaptureIntegrityState::Healthy
    }
}
