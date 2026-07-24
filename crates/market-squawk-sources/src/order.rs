//! Provider-neutral, sequence-bearing level-3 order contracts.

use market_squawk_domain::{ProviderProduct, SequenceNumber, SourceIdentifier, Timestamp};
use thiserror::Error;

use crate::{ProviderBookLevel, ProviderBookSide, ProviderQuantity};

/// A complete order owned by a provider's level-3 book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOrderRecord {
    order_id: SourceIdentifier,
    side: ProviderBookSide,
    level: ProviderBookLevel,
}

impl ProviderOrderRecord {
    /// Constructs an exact provider order without inventing normalized tick or lot evidence.
    pub const fn new(
        order_id: SourceIdentifier,
        side: ProviderBookSide,
        level: ProviderBookLevel,
    ) -> Self {
        Self {
            order_id,
            side,
            level,
        }
    }

    /// Returns the exact provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the order-book side.
    pub const fn side(&self) -> ProviderBookSide {
        self.side
    }

    /// Returns the exact price and remaining quantity.
    pub const fn level(&self) -> &ProviderBookLevel {
        &self.level
    }
}

/// Documented sequenced messages that advance a product cursor without changing public book state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderCursorOnlyReason {
    /// An order was received but has not yet become an open public-book order.
    Received,
    /// A pinned protocol message is proven not to mutate the maintained public book.
    DocumentedNoBookMutation,
}

/// One classified level-3 mutation or cursor-only advance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderOrderEventKind {
    /// The exact sequence advances without a public-book mutation.
    CursorOnly(ProviderCursorOnlyReason),
    /// Insert one newly open order.
    Open(ProviderOrderRecord),
    /// Decrement the known maker order by the matched quantity.
    Match {
        /// Exact maker order identity.
        maker_order_id: SourceIdentifier,
        /// Exact executed quantity.
        quantity: ProviderQuantity,
    },
    /// Remove a known order. A valid unknown received-only order is a cursor-only no-op.
    Done {
        /// Exact provider order identity.
        order_id: SourceIdentifier,
    },
    /// Replace the remaining size of a known order. Unknown received-only orders are no-ops.
    Change {
        /// Exact provider order identity.
        order_id: SourceIdentifier,
        /// New remaining size when the message describes a maintained limit order. `None` is a
        /// documented non-book funds change.
        new_quantity: Option<ProviderQuantity>,
    },
}

/// One exact, product-scoped sequenced event plus its retained raw-frame byte charge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOrderEvent {
    product: ProviderProduct,
    sequence: SequenceNumber,
    timestamp: Timestamp,
    kind: ProviderOrderEventKind,
    wire_bytes: usize,
}

impl ProviderOrderEvent {
    /// Constructs a bounded queue-admission value.
    ///
    /// # Errors
    ///
    /// Rejects zero byte charges and values larger than one capturable WebSocket frame.
    pub fn try_new(
        product: ProviderProduct,
        sequence: SequenceNumber,
        timestamp: Timestamp,
        kind: ProviderOrderEventKind,
        wire_bytes: usize,
    ) -> Result<Self, ProviderOrderEventError> {
        if wire_bytes == 0 || wire_bytes > crate::MAX_RAW_FRAME_BYTES {
            return Err(ProviderOrderEventError::InvalidWireBytes);
        }
        Ok(Self {
            product,
            sequence,
            timestamp,
            kind,
            wire_bytes,
        })
    }

    /// Returns the exact provider product sequence domain.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the exact product sequence.
    pub const fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// Returns the venue-supplied event time.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the classified order mutation.
    pub const fn kind(&self) -> &ProviderOrderEventKind {
        &self.kind
    }

    /// Returns the exact raw-frame byte charge used by bounded replay admission.
    pub const fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }
}

/// Invalid provider order-event construction.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderOrderEventError {
    /// Queue byte accounting must bind one nonempty capturable raw frame.
    #[error("provider order event has an invalid raw-frame byte charge")]
    InvalidWireBytes,
}
