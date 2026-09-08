//! Bounded order-identity-preserving live read state.
//!
//! Order-level state is intentionally separate from the canonical price-level
//! [`market_squawk_domain::MarketEvent`] family. Duplicate-price orders remain distinct in this
//! module; price-level consumers receive only an explicitly derived projection carrying the
//! original source, generation, integrity, freshness, and quality evidence.

mod batch;
mod book;
mod model;
mod provider;

pub use batch::{
    OrderLevelBatch, OrderLevelBatchError, OrderLevelBatchInput, OrderLevelBatchPayload,
};
pub use book::{
    OrderLevelBook, OrderLevelBookError, OrderLevelCommit, OrderLevelEntry,
    OrderLevelPriceProjection, OrderLevelProjectionError, PriceLevelProjection,
};
pub use model::{
    MAX_ORDER_LEVEL_ORDERS, OrderLevelBatchKind, OrderLevelDeleteQuantity, OrderLevelEvent,
    OrderLevelLimitError, OrderLevelLimits, OrderLevelModelError, OrderLevelOperation,
    OrderLevelPhase, OrderLevelPriority, OrderLevelPriorityUpdate, OrderLevelQuarantineReason,
    OrderLevelRoute, OrderLevelVisibleOrder, UnknownOrderDisposition,
};
pub use provider::{
    SequencedProviderConversionError, provider_order, provider_snapshot_orders,
    sequenced_provider_event,
};

#[cfg(test)]
mod tests;
