//! Fixed-shape bounded paper execution audit stream.

use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use market_squawk_domain::{OrderId, QuantityLots, Timestamp};
use tokio::sync::mpsc;

use crate::PaperOrderState;

/// Paper state mutation represented without unbounded text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaperAuditKind {
    Accepted,
    Filled,
    ActivatedOrResting,
    CancelRequested,
    Canceled,
    Rejected,
    Expired,
    RecoveryLoaded,
    ReconciliationRequired,
}

/// One fixed-size before/after mutation record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaperAuditRecord {
    sequence: u64,
    order_id: Option<OrderId>,
    kind: PaperAuditKind,
    previous_state: Option<PaperOrderState>,
    new_state: Option<PaperOrderState>,
    event_at: Timestamp,
    fill_quantity: Option<QuantityLots>,
    configuration_digest: [u8; 32],
    input_digest: [u8; 32],
}

impl PaperAuditRecord {
    #[allow(
        clippy::too_many_arguments,
        reason = "audit evidence retains every independent mutation binding"
    )]
    pub(crate) const fn new(
        sequence: u64,
        order_id: Option<OrderId>,
        kind: PaperAuditKind,
        previous_state: Option<PaperOrderState>,
        new_state: Option<PaperOrderState>,
        event_at: Timestamp,
        fill_quantity: Option<QuantityLots>,
        configuration_digest: [u8; 32],
        input_digest: [u8; 32],
    ) -> Self {
        Self {
            sequence,
            order_id,
            kind,
            previous_state,
            new_state,
            event_at,
            fill_quantity,
            configuration_digest,
            input_digest,
        }
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }
    pub const fn order_id(self) -> Option<OrderId> {
        self.order_id
    }
    pub const fn kind(self) -> PaperAuditKind {
        self.kind
    }
    pub const fn previous_state(self) -> Option<PaperOrderState> {
        self.previous_state
    }
    pub const fn new_state(self) -> Option<PaperOrderState> {
        self.new_state
    }
    pub const fn event_at(self) -> Timestamp {
        self.event_at
    }
    pub const fn fill_quantity(self) -> Option<QuantityLots> {
        self.fill_quantity
    }
    pub const fn configuration_digest(self) -> [u8; 32] {
        self.configuration_digest
    }
    pub const fn input_digest(self) -> [u8; 32] {
        self.input_digest
    }
    pub const fn retained_bytes() -> usize {
        size_of::<Self>()
    }
}

/// Sole consumer of the bounded audit stream.
#[derive(Debug)]
pub struct PaperAuditReader {
    receiver: mpsc::Receiver<PaperAuditRecord>,
    persistence_failed: Arc<AtomicBool>,
}

impl PaperAuditReader {
    pub(crate) fn new(
        receiver: mpsc::Receiver<PaperAuditRecord>,
        persistence_failed: Arc<AtomicBool>,
    ) -> Self {
        Self {
            receiver,
            persistence_failed,
        }
    }

    /// Receives the next audit record or `None` after worker shutdown.
    pub async fn recv(&mut self) -> Option<PaperAuditRecord> {
        self.receiver.recv().await
    }

    /// Reports a downstream persistence failure. The worker then blocks new submissions and
    /// preserves outstanding state for reconciliation.
    pub fn report_persistence_failure(&self) {
        self.persistence_failed.store(true, Ordering::Release);
    }
}
