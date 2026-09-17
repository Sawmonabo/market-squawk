use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{
    ProviderBookSide, ProviderOrderEvent, ProviderOrderEventKind, ProviderOrderRecord,
};
use thiserror::Error;

use super::model::{
    OrderLevelDeleteQuantity, OrderLevelEvent, OrderLevelModelError, OrderLevelOperation,
    OrderLevelPriorityUpdate, OrderLevelVisibleOrder, UnknownOrderDisposition,
};
use crate::BookSide;

/// Converts one already scaled provider snapshot order without flattening its identity.
///
/// Coinbase Direct uses this boundary for every `[price, size, order_id]` snapshot row. Provider
/// snapshot order is retained by the caller's vector order; it is not claimed as queue priority.
///
/// # Errors
///
/// Returns a bounded identity/allocation error or rejects zero visible quantity.
pub fn provider_order(
    order: &ProviderOrderRecord,
) -> Result<OrderLevelVisibleOrder, SequencedProviderConversionError> {
    OrderLevelVisibleOrder::new(
        clone_identifier(order.order_id())?,
        side(order.side()),
        order.price(),
        order.quantity(),
        None,
        None,
    )
    .map_err(SequencedProviderConversionError::Model)
}

/// Converts one complete provider snapshot with fallible bounded reservation.
///
/// # Errors
///
/// Returns the first invalid order or allocation failure without publishing a partial result.
pub fn provider_snapshot_orders(
    orders: &[ProviderOrderRecord],
) -> Result<Vec<OrderLevelVisibleOrder>, SequencedProviderConversionError> {
    let mut converted = Vec::new();
    converted
        .try_reserve_exact(orders.len())
        .map_err(|_| SequencedProviderConversionError::Allocation)?;
    for order in orders {
        converted.push(provider_order(order)?);
    }
    Ok(converted)
}

/// Converts one sequence-bearing provider event into an order-aware atomic event.
///
/// Coinbase's exact provider sequence remains `provider_sequence`. Unknown `done`/`change`
/// identities retain the provider-neutral Direct contract's documented cursor-only behavior. No
/// local ordinal, quality promotion, checksum assertion, or queue-priority inference is added.
///
/// # Errors
///
/// Returns a bounded identity/allocation or event-construction error.
pub fn sequenced_provider_event(
    event: &ProviderOrderEvent,
) -> Result<OrderLevelEvent, SequencedProviderConversionError> {
    let operation = match event.kind() {
        ProviderOrderEventKind::CursorOnly(_) => OrderLevelOperation::CursorOnly,
        ProviderOrderEventKind::Open(order) => OrderLevelOperation::Open(provider_order(order)?),
        ProviderOrderEventKind::Match {
            maker_order_id,
            maker_side,
            maker_price,
            quantity,
        } => OrderLevelOperation::Match {
            order_id: clone_identifier(maker_order_id)?,
            side: side(*maker_side),
            price: *maker_price,
            quantity: *quantity,
        },
        ProviderOrderEventKind::Done {
            order_id,
            side: provider_side,
            price,
            remaining_quantity,
        } => OrderLevelOperation::Done {
            order_id: clone_identifier(order_id)?,
            side: provider_side.map(side),
            price: *price,
            quantity: remaining_quantity.map_or(
                OrderLevelDeleteQuantity::Unavailable,
                OrderLevelDeleteQuantity::ExactRemaining,
            ),
            provider_order_timestamp: None,
            unknown_order: UnknownOrderDisposition::CursorOnly,
        },
        ProviderOrderEventKind::Change {
            order_id,
            reason,
            side: provider_side,
            previous_price,
            previous_quantity,
            new_price,
            new_quantity,
        } => OrderLevelOperation::Change {
            order_id: clone_identifier(order_id)?,
            reason: *reason,
            side: side(*provider_side),
            previous_price: *previous_price,
            previous_quantity: *previous_quantity,
            new_price: *new_price,
            new_quantity: *new_quantity,
            provider_order_timestamp: None,
            priority: OrderLevelPriorityUpdate::Preserve,
            unknown_order: UnknownOrderDisposition::CursorOnly,
        },
    };
    let mut operations = Vec::new();
    operations
        .try_reserve_exact(1)
        .map_err(|_| SequencedProviderConversionError::Allocation)?;
    operations.push(operation);
    OrderLevelEvent::try_new(
        Some(event.sequence()),
        None,
        event.timestamp(),
        event.evidence().received_at(),
        operations,
    )
    .map_err(SequencedProviderConversionError::Model)
}

const fn side(side: ProviderBookSide) -> BookSide {
    match side {
        ProviderBookSide::Bid => BookSide::Bid,
        ProviderBookSide::Ask => BookSide::Ask,
    }
}

fn clone_identifier(
    value: &SourceIdentifier,
) -> Result<SourceIdentifier, SequencedProviderConversionError> {
    let mut clone = String::new();
    clone
        .try_reserve_exact(value.as_str().len())
        .map_err(|_| SequencedProviderConversionError::Allocation)?;
    clone.push_str(value.as_str());
    SourceIdentifier::try_from(clone).map_err(|_| SequencedProviderConversionError::Identifier)
}

/// Failed conversion from the provider-neutral sequence-bearing order contract.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SequencedProviderConversionError {
    /// A bounded output or identifier allocation failed.
    #[error("sequenced provider order conversion allocation failed")]
    Allocation,
    /// A previously validated provider identifier could not be reconstructed.
    #[error("sequenced provider order identifier invariant failed")]
    Identifier,
    /// Normalized visible-order or event invariants failed.
    #[error("sequenced provider order model is invalid: {0}")]
    Model(#[from] OrderLevelModelError),
}
