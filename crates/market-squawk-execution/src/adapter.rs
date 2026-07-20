//! Stable adapter result vocabulary without a public submission bypass.

use market_squawk_domain::{OrderId, Timestamp};
use thiserror::Error;

/// Successful backend acceptance of one risk-dispatched order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionReceipt {
    order_id: OrderId,
    accepted_at: Timestamp,
}

impl ExecutionReceipt {
    /// Creates an immutable accepted-order receipt.
    pub const fn new(order_id: OrderId, accepted_at: Timestamp) -> Self {
        Self {
            order_id,
            accepted_at,
        }
    }

    /// Returns the internal order identity.
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the trusted backend acceptance timestamp.
    pub const fn accepted_at(self) -> Timestamp {
        self.accepted_at
    }
}

/// Result of a cancellation request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelStatus {
    /// Cancellation was accepted but may still race a fill.
    Pending,
    /// The order reached a terminal canceled state.
    Canceled,
    /// The order was already terminal when cancellation arrived.
    AlreadyTerminal,
}

/// Immutable typed cancellation receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelReceipt {
    order_id: OrderId,
    status: CancelStatus,
    observed_at: Timestamp,
}

impl CancelReceipt {
    /// Creates a cancellation receipt from a backend-owned observation.
    pub const fn new(order_id: OrderId, status: CancelStatus, observed_at: Timestamp) -> Self {
        Self {
            order_id,
            status,
            observed_at,
        }
    }

    /// Returns the internal order identity.
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the observed cancellation state.
    pub const fn status(self) -> CancelStatus {
        self.status
    }

    /// Returns the trusted observation timestamp.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
}

/// Bounded account/order reconciliation summary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionState {
    observed_at: Timestamp,
    open_orders: u32,
    reconciliation_required: bool,
}

impl ExecutionState {
    /// Creates a bounded reconciliation summary.
    pub const fn new(
        observed_at: Timestamp,
        open_orders: u32,
        reconciliation_required: bool,
    ) -> Self {
        Self {
            observed_at,
            open_orders,
            reconciliation_required,
        }
    }

    /// Returns when the backend state was observed.
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the bounded count of known open orders.
    pub const fn open_orders(self) -> u32 {
        self.open_orders
    }

    /// Returns whether execution is fail-closed pending reconciliation.
    pub const fn reconciliation_required(self) -> bool {
        self.reconciliation_required
    }
}

/// Execution-backend failure classification used by the later one-use dispatcher.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExecutionAdapterError {
    /// The backend definitively rejected the command.
    #[error("execution backend rejected the command")]
    Rejected,
    /// The backend definitively did not accept the command.
    #[error("execution backend failed before accepting the command")]
    KnownFailure,
    /// Acceptance cannot be determined and retry is forbidden until reconciliation.
    #[error("execution outcome is uncertain and requires reconciliation")]
    UncertainOutcome,
    /// The adapter is already fail-closed pending reconciliation.
    #[error("execution adapter requires reconciliation")]
    ReconciliationRequired,
    /// The adapter's bounded command capacity is unavailable.
    #[error("execution adapter is busy")]
    Busy,
}
