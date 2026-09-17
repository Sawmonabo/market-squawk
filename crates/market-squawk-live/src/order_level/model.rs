use market_squawk_domain::{
    ConnectionGeneration, InstrumentId, PriceTicks, QuantityLots, SequenceNumber, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    MAX_DECODED_BOOK_ITEMS, MAX_DECODED_EVENTS, ProviderOrderChangeReason,
};
use std::mem::size_of;
use thiserror::Error;

use crate::{BookSide, DepthLimit};

/// Maximum individual orders retained by one canonical order-level read model.
///
/// This matches the existing bounded Direct-feed owner. Message-level mutation counts retain the
/// lower shared decoder ceiling; a complete Direct REST snapshot is not a decoded market message
/// and may legitimately contain more orders.
pub const MAX_ORDER_LEVEL_ORDERS: usize = 2_000_000;

/// Immutable source and connection identity owned by one order-level book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelRoute {
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    provider_instrument: SourceIdentifier,
    generation: ConnectionGeneration,
}

impl OrderLevelRoute {
    /// Constructs one exact provider route for one connection generation.
    pub const fn new(
        source_id: SourceId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        provider_instrument: SourceIdentifier,
        generation: ConnectionGeneration,
    ) -> Self {
        Self {
            source_id,
            venue_id,
            instrument_id,
            provider_instrument,
            generation,
        }
    }

    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the direct venue identity.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the provider's exact instrument or product symbol.
    pub const fn provider_instrument(&self) -> &SourceIdentifier {
        &self.provider_instrument
    }

    /// Returns the connection generation that owns this state.
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.source_id
            .retained_bytes()
            .checked_add(self.venue_id.retained_bytes())?
            .checked_add(self.provider_instrument.retained_bytes())
    }
}

/// Hard retained-state and derived-projection limits for one order-level book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderLevelLimits {
    max_orders: usize,
    price_level_depth: DepthLimit,
}

impl OrderLevelLimits {
    /// Constructs nonzero retained-state limits within the canonical hard ceiling.
    ///
    /// # Errors
    ///
    /// Rejects zero or more than [`MAX_ORDER_LEVEL_ORDERS`] retained orders.
    pub fn new(
        max_orders: usize,
        price_level_depth: DepthLimit,
    ) -> Result<Self, OrderLevelLimitError> {
        if max_orders == 0 || max_orders > MAX_ORDER_LEVEL_ORDERS {
            return Err(OrderLevelLimitError::InvalidOrderLimit {
                requested: max_orders,
                maximum: MAX_ORDER_LEVEL_ORDERS,
            });
        }
        Ok(Self {
            max_orders,
            price_level_depth,
        })
    }

    /// Returns the maximum retained individual orders.
    pub const fn max_orders(self) -> usize {
        self.max_orders
    }

    /// Returns the maximum projected price levels per side.
    pub const fn price_level_depth(self) -> DepthLimit {
        self.price_level_depth
    }
}

/// Invalid order-level retained-state limits.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderLevelLimitError {
    /// The individual-order ceiling was zero or exceeded the shared hard maximum.
    #[error("invalid order limit {requested}; maximum is {maximum}")]
    InvalidOrderLimit { requested: usize, maximum: usize },
}

/// Exact provider-supplied queue or order priority.
///
/// The value is retained only when the provider explicitly defines it. Arrival order, a local
/// ordinal, and a timestamp must not be relabeled as provider priority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelPriority {
    value: u64,
    rule: SourceIdentifier,
}

impl OrderLevelPriority {
    /// Binds a provider priority value to the provider rule that defines it.
    pub const fn new(value: u64, rule: SourceIdentifier) -> Self {
        Self { value, rule }
    }

    /// Returns the provider priority scalar.
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Returns the provider rule defining the scalar.
    pub const fn rule(&self) -> &SourceIdentifier {
        &self.rule
    }

    fn dynamic_retained_bytes(&self) -> usize {
        self.rule.retained_bytes()
    }
}

/// One complete visible individual order after exact tick/lot normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelVisibleOrder {
    order_id: SourceIdentifier,
    side: BookSide,
    price: PriceTicks,
    quantity: QuantityLots,
    provider_order_timestamp: Option<Timestamp>,
    provider_priority: Option<OrderLevelPriority>,
}

impl OrderLevelVisibleOrder {
    /// Constructs a positive-quantity individual order.
    ///
    /// # Errors
    ///
    /// Zero is reserved for an explicit delete-on-zero mutation.
    pub fn new(
        order_id: SourceIdentifier,
        side: BookSide,
        price: PriceTicks,
        quantity: QuantityLots,
        provider_order_timestamp: Option<Timestamp>,
        provider_priority: Option<OrderLevelPriority>,
    ) -> Result<Self, OrderLevelModelError> {
        if quantity.get() == 0 {
            return Err(OrderLevelModelError::ZeroVisibleQuantity);
        }
        Ok(Self {
            order_id,
            side,
            price,
            quantity,
            provider_order_timestamp,
            provider_priority,
        })
    }

    /// Returns the stable provider order identity.
    pub const fn order_id(&self) -> &SourceIdentifier {
        &self.order_id
    }

    /// Returns the visible book side.
    pub const fn side(&self) -> BookSide {
        self.side
    }

    /// Returns the exact normalized tick price.
    pub const fn price(&self) -> PriceTicks {
        self.price
    }

    /// Returns the exact normalized remaining lots.
    pub const fn quantity(&self) -> QuantityLots {
        self.quantity
    }

    /// Returns the provider's order timestamp when supplied.
    pub const fn provider_order_timestamp(&self) -> Option<Timestamp> {
        self.provider_order_timestamp
    }

    /// Returns provider-defined priority when supplied.
    pub const fn provider_priority(&self) -> Option<&OrderLevelPriority> {
        self.provider_priority.as_ref()
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.order_id.retained_bytes().checked_add(
            self.provider_priority
                .as_ref()
                .map_or(0, OrderLevelPriority::dynamic_retained_bytes),
        )
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        SourceIdentifier,
        BookSide,
        PriceTicks,
        QuantityLots,
        Option<Timestamp>,
        Option<OrderLevelPriority>,
    ) {
        (
            self.order_id,
            self.side,
            self.price,
            self.quantity,
            self.provider_order_timestamp,
            self.provider_priority,
        )
    }
}

/// How a change updates provider-defined priority evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderLevelPriorityUpdate {
    /// The provider contract says the prior priority remains applicable.
    Preserve,
    /// Replace the prior evidence; `None` explicitly means the new priority is unavailable.
    Replace(Option<OrderLevelPriority>),
}

/// Quantity evidence carried by an order deletion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderLevelDeleteQuantity {
    /// Provider supplied the exact current remaining quantity before removal.
    ExactRemaining(QuantityLots),
    /// Provider uses a zero-quantity record as the explicit deletion operation.
    ZeroMeansDelete,
    /// Provider omitted quantity evidence for an order that may be a documented cursor-only no-op.
    Unavailable,
}

/// Provider-authorized handling when a mutation names no retained order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownOrderDisposition {
    /// Treat the message as an integrity failure.
    Reject,
    /// Treat it as a documented cursor-only advance with no book mutation.
    CursorOnly,
}

/// One normalized individual-order mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderLevelOperation {
    /// A sequenced provider event that is proven not to mutate the visible book.
    CursorOnly,
    /// Add one newly visible order; duplicate identities are rejected.
    Open(OrderLevelVisibleOrder),
    /// Decrement one exact maker order, removing it if the remaining quantity reaches zero.
    Match {
        /// Provider order identity.
        order_id: SourceIdentifier,
        /// Expected current side.
        side: BookSide,
        /// Expected current price.
        price: PriceTicks,
        /// Executed quantity to subtract.
        quantity: QuantityLots,
    },
    /// Remove one order under exact provider evidence.
    Done {
        /// Provider order identity.
        order_id: SourceIdentifier,
        /// Expected side when supplied.
        side: Option<BookSide>,
        /// Expected price when supplied.
        price: Option<PriceTicks>,
        /// Provider quantity semantics.
        quantity: OrderLevelDeleteQuantity,
        /// Provider order timestamp when supplied by this mutation.
        provider_order_timestamp: Option<Timestamp>,
        /// Provider-authorized unknown-order behavior.
        unknown_order: UnknownOrderDisposition,
    },
    /// Change one known order under a closed provider reason.
    Change {
        /// Provider order identity.
        order_id: SourceIdentifier,
        /// Provider-defined change reason.
        reason: ProviderOrderChangeReason,
        /// Expected current side.
        side: BookSide,
        /// Expected current price when supplied.
        previous_price: Option<PriceTicks>,
        /// Expected current quantity when supplied.
        previous_quantity: Option<QuantityLots>,
        /// Replacement price when the provider changed it.
        new_price: Option<PriceTicks>,
        /// Replacement remaining quantity; `None` is a documented non-book change.
        new_quantity: Option<QuantityLots>,
        /// Provider order timestamp after this mutation when supplied.
        provider_order_timestamp: Option<Timestamp>,
        /// How queue/order priority evidence changes.
        priority: OrderLevelPriorityUpdate,
        /// Provider-authorized unknown-order behavior.
        unknown_order: UnknownOrderDisposition,
    },
}

impl OrderLevelOperation {
    fn dynamic_retained_bytes(&self) -> Option<usize> {
        match self {
            Self::CursorOnly => Some(0),
            Self::Open(order) => order.dynamic_retained_bytes(),
            Self::Match { order_id, .. } | Self::Done { order_id, .. } => {
                Some(order_id.retained_bytes())
            }
            Self::Change {
                order_id, priority, ..
            } => order_id.retained_bytes().checked_add(match priority {
                OrderLevelPriorityUpdate::Replace(Some(priority)) => {
                    priority.dynamic_retained_bytes()
                }
                OrderLevelPriorityUpdate::Preserve | OrderLevelPriorityUpdate::Replace(None) => 0,
            }),
        }
    }
}

/// One provider message containing an atomic ordered mutation set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLevelEvent {
    provider_sequence: Option<SequenceNumber>,
    diagnostic_ordinal: Option<u64>,
    source_timestamp: Timestamp,
    received_at: Timestamp,
    operations: Vec<OrderLevelOperation>,
}

impl OrderLevelEvent {
    /// Constructs one bounded event without conflating a local ordinal with provider sequence.
    ///
    /// # Errors
    ///
    /// Rejects an empty or excessive mutation set and simultaneous provider-sequence and local
    /// diagnostic values.
    pub fn try_new(
        provider_sequence: Option<SequenceNumber>,
        diagnostic_ordinal: Option<u64>,
        source_timestamp: Timestamp,
        received_at: Timestamp,
        operations: Vec<OrderLevelOperation>,
    ) -> Result<Self, OrderLevelModelError> {
        if provider_sequence.is_some() && diagnostic_ordinal.is_some() {
            return Err(OrderLevelModelError::SequenceDiagnosticCollision);
        }
        if operations.is_empty() || operations.len() > MAX_DECODED_BOOK_ITEMS {
            return Err(OrderLevelModelError::InvalidOperationCount {
                observed: operations.len(),
                maximum: MAX_DECODED_BOOK_ITEMS,
            });
        }
        Ok(Self {
            provider_sequence,
            diagnostic_ordinal,
            source_timestamp,
            received_at,
            operations,
        })
    }

    /// Returns provider sequence only when the protocol supplies it.
    pub const fn provider_sequence(&self) -> Option<SequenceNumber> {
        self.provider_sequence
    }

    /// Returns a local diagnostic ordinal that carries no provider-sequence authority.
    pub const fn diagnostic_ordinal(&self) -> Option<u64> {
        self.diagnostic_ordinal
    }

    /// Returns the provider event timestamp.
    pub const fn source_timestamp(&self) -> Timestamp {
        self.source_timestamp
    }

    /// Returns when this provider message reached the process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns operations in exact provider wire order.
    pub fn operations(&self) -> &[OrderLevelOperation] {
        &self.operations
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Option<usize> {
        self.operations
            .capacity()
            .checked_mul(size_of::<OrderLevelOperation>())?
            .checked_add(
                self.operations
                    .iter()
                    .try_fold(0_usize, |total, operation| {
                        total.checked_add(operation.dynamic_retained_bytes()?)
                    })?,
            )
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        Option<SequenceNumber>,
        Option<u64>,
        Timestamp,
        Timestamp,
        Vec<OrderLevelOperation>,
    ) {
        (
            self.provider_sequence,
            self.diagnostic_ordinal,
            self.source_timestamp,
            self.received_at,
            self.operations,
        )
    }
}

/// Whether a committed transaction initializes or incrementally updates the book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderLevelBatchKind {
    /// Complete snapshot, optionally followed by a contiguous pre-handoff replay suffix.
    Snapshot,
    /// One or more events following the current snapshot.
    Update,
}

/// Current lifecycle state for one generation-owned order-level book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderLevelPhase {
    /// No complete snapshot currently owns the state.
    AwaitingSnapshot,
    /// Snapshot and accepted successors are coherent.
    Healthy,
    /// Retained state is isolated and cannot produce execution-eligible data.
    Quarantined(OrderLevelQuarantineReason),
}

/// Stable reason that order-level state was isolated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderLevelQuarantineReason {
    /// A batch was routed to the wrong source, venue, instrument, or generation.
    RouteMismatch,
    /// Provider sequence evidence failed or contradicted the retained cursor.
    Sequence,
    /// Provider checksum evidence failed or was incomplete.
    Checksum,
    /// Snapshot/update lifecycle ordering was invalid.
    Snapshot,
    /// An order mutation contradicted retained provider state.
    Mutation,
    /// The candidate book crossed or exceeded configured bounds.
    Book,
    /// Checked arithmetic or bounded allocation failed.
    Resource,
}

/// Invalid order or event construction.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OrderLevelModelError {
    /// A visible order cannot retain zero quantity.
    #[error("visible order quantity must be positive")]
    ZeroVisibleQuantity,
    /// Provider sequence and a local diagnostic ordinal were both supplied.
    #[error("provider sequence cannot be conflated with a local diagnostic ordinal")]
    SequenceDiagnosticCollision,
    /// One provider event contained no operations or exceeded the shared hard bound.
    #[error("order-level event contains {observed} operations; allowed range is 1..={maximum}")]
    InvalidOperationCount { observed: usize, maximum: usize },
}

pub(super) const fn max_batch_events() -> usize {
    MAX_DECODED_EVENTS
}
