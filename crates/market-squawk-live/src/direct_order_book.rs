//! Bounded level-3 ownership and atomic snapshot/replay handoff.

use std::collections::{HashMap, VecDeque};

use market_squawk_domain::{
    ConnectionGeneration, ProviderProduct, SequenceNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ProviderBookLevel, ProviderBookSide, ProviderDecimalLexeme, ProviderOrderEvent,
    ProviderOrderEventKind, ProviderOrderRecord, ProviderPrice, ProviderQuantity,
    SegmentedHttpResponseReceipt,
};
use rust_decimal::Decimal;
use thiserror::Error;

const MAX_DIRECT_ORDERS: usize = 2_000_000;
const MAX_DIRECT_PRICE_LEVELS: usize = 1_000_000;
const MAX_DIRECT_QUEUE_EVENTS: usize = 1_000_000;
const MAX_PUBLISHED_DEPTH: usize = 10_000;

/// Explicit synchronization phase for one exact direct-feed generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DirectSyncPhase {
    /// WebSocket events may be queued, but no snapshot candidate exists.
    AwaitingSnapshot,
    /// A complete bounded snapshot candidate exists but queued events are not replayed.
    SnapshotLoaded,
    /// A contiguous queued suffix is being applied to the unpublished candidate.
    Replaying,
    /// Snapshot and complete replay were atomically handed to the live owner.
    Healthy,
    /// Integrity failed; only a fresh connection generation may recover.
    Quarantined,
}

/// Immutable count and byte limits for one direct level-3 owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectBookLimits {
    max_orders: usize,
    max_price_levels: usize,
    max_queue_events: usize,
    max_queue_bytes: usize,
    published_depth: usize,
}

impl DirectBookLimits {
    /// Constructs explicit, globally capped direct-book limits.
    ///
    /// # Errors
    ///
    /// Rejects zero, contradictory, or globally excessive limits.
    pub fn try_new(
        max_orders: usize,
        max_price_levels: usize,
        max_queue_events: usize,
        max_queue_bytes: usize,
        published_depth: usize,
    ) -> Result<Self, DirectOrderBookError> {
        let maximum_queue_bytes = max_queue_events
            .checked_mul(market_squawk_sources::MAX_RAW_FRAME_BYTES)
            .ok_or(DirectOrderBookError::InvalidLimits)?;
        if max_orders == 0
            || max_orders > MAX_DIRECT_ORDERS
            || max_price_levels == 0
            || max_price_levels > max_orders
            || max_price_levels > MAX_DIRECT_PRICE_LEVELS
            || max_queue_events == 0
            || max_queue_events > MAX_DIRECT_QUEUE_EVENTS
            || max_queue_bytes == 0
            || max_queue_bytes > maximum_queue_bytes
            || published_depth == 0
            || published_depth > max_price_levels
            || published_depth > MAX_PUBLISHED_DEPTH
        {
            return Err(DirectOrderBookError::InvalidLimits);
        }
        Ok(Self {
            max_orders,
            max_price_levels,
            max_queue_events,
            max_queue_bytes,
            published_depth,
        })
    }

    /// Returns the maximum retained order count.
    pub const fn max_orders(self) -> usize {
        self.max_orders
    }

    /// Returns the maximum distinct aggregate price count.
    pub const fn max_price_levels(self) -> usize {
        self.max_price_levels
    }

    /// Returns the maximum queued WebSocket-event count.
    pub const fn max_queue_events(self) -> usize {
        self.max_queue_events
    }

    /// Returns the maximum sum of exact queued raw-frame bytes.
    pub const fn max_queue_bytes(self) -> usize {
        self.max_queue_bytes
    }

    /// Returns the top price-level depth exposed to the shared live runtime.
    pub const fn published_depth(self) -> usize {
        self.published_depth
    }
}

#[derive(Clone, Debug)]
struct OwnedOrder {
    side: ProviderBookSide,
    price: ProviderPrice,
    quantity: ProviderQuantity,
}

impl OwnedOrder {
    fn from_record(record: &ProviderOrderRecord) -> Self {
        Self {
            side: record.side(),
            price: record.level().price().clone(),
            quantity: record.level().quantity().clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct PriceAggregate {
    side: ProviderBookSide,
    price: ProviderPrice,
    quantity: Decimal,
    orders: usize,
}

#[derive(Debug)]
struct OrderBookState {
    orders: HashMap<SourceIdentifier, OwnedOrder>,
    levels: HashMap<(ProviderBookSide, Decimal), PriceAggregate>,
    limits: DirectBookLimits,
}

impl OrderBookState {
    fn try_new(limits: DirectBookLimits) -> Result<Self, DirectOrderBookError> {
        let mut orders = HashMap::new();
        orders
            .try_reserve(limits.max_orders)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        let mut levels = HashMap::new();
        levels
            .try_reserve(limits.max_price_levels)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        Ok(Self {
            orders,
            levels,
            limits,
        })
    }

    fn insert(&mut self, record: &ProviderOrderRecord) -> Result<(), DirectOrderBookError> {
        let price = record.level().price().value().decimal();
        let quantity = record.level().quantity().value().decimal();
        validate_positive(price)?;
        validate_positive(quantity)?;
        if self.orders.contains_key(record.order_id()) {
            return Err(DirectOrderBookError::DuplicateOrder);
        }
        if self.orders.len() == self.limits.max_orders {
            return Err(DirectOrderBookError::OrderCapacityExceeded);
        }
        let key = (record.side(), price);
        if !self.levels.contains_key(&key) && self.levels.len() == self.limits.max_price_levels {
            return Err(DirectOrderBookError::PriceLevelCapacityExceeded);
        }
        match self.levels.get_mut(&key) {
            Some(aggregate) => {
                aggregate.quantity = aggregate
                    .quantity
                    .checked_add(quantity)
                    .ok_or(DirectOrderBookError::NumericInvariant)?;
                aggregate.orders = aggregate
                    .orders
                    .checked_add(1)
                    .ok_or(DirectOrderBookError::NumericInvariant)?;
            }
            None => {
                self.levels.insert(
                    key,
                    PriceAggregate {
                        side: record.side(),
                        price: record.level().price().clone(),
                        quantity,
                        orders: 1,
                    },
                );
            }
        }
        self.orders
            .insert(record.order_id().clone(), OwnedOrder::from_record(record));
        Ok(())
    }

    fn remove_if_known(
        &mut self,
        order_id: &SourceIdentifier,
    ) -> Result<bool, DirectOrderBookError> {
        let Some(order) = self.orders.remove(order_id) else {
            return Ok(false);
        };
        let key = (order.side, order.price.value().decimal());
        let remove_level = {
            let aggregate = self
                .levels
                .get_mut(&key)
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            let quantity = order.quantity.value().decimal();
            if aggregate.orders == 0 || aggregate.quantity < quantity {
                return Err(DirectOrderBookError::AggregateInvariant);
            }
            aggregate.orders -= 1;
            aggregate.quantity = aggregate
                .quantity
                .checked_sub(quantity)
                .ok_or(DirectOrderBookError::NumericInvariant)?;
            (aggregate.orders == 0) == aggregate.quantity.is_zero() && aggregate.orders == 0
        };
        if remove_level {
            self.levels.remove(&key);
        } else {
            let aggregate = self
                .levels
                .get(&key)
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            if aggregate.orders == 0 || aggregate.quantity <= Decimal::ZERO {
                return Err(DirectOrderBookError::AggregateInvariant);
            }
        }
        Ok(true)
    }

    fn apply_match(
        &mut self,
        maker_order_id: &SourceIdentifier,
        quantity: &ProviderQuantity,
    ) -> Result<(), DirectOrderBookError> {
        let matched = quantity.value().decimal();
        validate_positive(matched)?;
        let order = self
            .orders
            .get(maker_order_id)
            .cloned()
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?;
        let remaining = order.quantity.value().decimal();
        if matched > remaining {
            return Err(DirectOrderBookError::MatchExceedsRemaining);
        }
        if matched == remaining {
            if !self.remove_if_known(maker_order_id)? {
                return Err(DirectOrderBookError::UnknownMakerOrder);
            }
            return Ok(());
        }
        let next = remaining
            .checked_sub(matched)
            .ok_or(DirectOrderBookError::NumericInvariant)?;
        let next_quantity = quantity_from_decimal(next)?;
        let key = (order.side, order.price.value().decimal());
        let aggregate = self
            .levels
            .get_mut(&key)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        aggregate.quantity = aggregate
            .quantity
            .checked_sub(matched)
            .ok_or(DirectOrderBookError::NumericInvariant)?;
        if aggregate.quantity <= Decimal::ZERO {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.orders
            .get_mut(maker_order_id)
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?
            .quantity = next_quantity;
        Ok(())
    }

    fn apply_change(
        &mut self,
        order_id: &SourceIdentifier,
        new_quantity: Option<&ProviderQuantity>,
    ) -> Result<(), DirectOrderBookError> {
        let Some(new_quantity) = new_quantity else {
            return Ok(());
        };
        let Some(order) = self.orders.get(order_id).cloned() else {
            return Ok(());
        };
        let next = new_quantity.value().decimal();
        validate_positive(next)?;
        let previous = order.quantity.value().decimal();
        let key = (order.side, order.price.value().decimal());
        let aggregate = self
            .levels
            .get_mut(&key)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        aggregate.quantity = aggregate
            .quantity
            .checked_sub(previous)
            .and_then(|value| value.checked_add(next))
            .ok_or(DirectOrderBookError::NumericInvariant)?;
        if aggregate.quantity <= Decimal::ZERO {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.orders
            .get_mut(order_id)
            .ok_or(DirectOrderBookError::AggregateInvariant)?
            .quantity = new_quantity.clone();
        Ok(())
    }

    fn apply(&mut self, kind: &ProviderOrderEventKind) -> Result<(), DirectOrderBookError> {
        match kind {
            ProviderOrderEventKind::CursorOnly(_) => Ok(()),
            ProviderOrderEventKind::Open(order) => self.insert(order),
            ProviderOrderEventKind::Match {
                maker_order_id,
                quantity,
            } => self.apply_match(maker_order_id, quantity),
            ProviderOrderEventKind::Done { order_id } => {
                let _known = self.remove_if_known(order_id)?;
                Ok(())
            }
            ProviderOrderEventKind::Change {
                order_id,
                new_quantity,
            } => self.apply_change(order_id, new_quantity.as_ref()),
        }
    }

    fn published(&self, depth: usize) -> Result<DirectPublishedBook, DirectOrderBookError> {
        let mut bids = self
            .levels
            .values()
            .filter(|level| level.side == ProviderBookSide::Bid)
            .collect::<Vec<_>>();
        let mut asks = self
            .levels
            .values()
            .filter(|level| level.side == ProviderBookSide::Ask)
            .collect::<Vec<_>>();
        bids.sort_unstable_by(|left, right| {
            right
                .price
                .value()
                .decimal()
                .cmp(&left.price.value().decimal())
        });
        asks.sort_unstable_by(|left, right| {
            left.price
                .value()
                .decimal()
                .cmp(&right.price.value().decimal())
        });
        let bids = published_side(bids.into_iter().take(depth), depth)?;
        let asks = published_side(asks.into_iter().take(depth), depth)?;
        Ok(DirectPublishedBook { bids, asks })
    }
}

fn published_side<'a>(
    levels: impl Iterator<Item = &'a PriceAggregate>,
    depth: usize,
) -> Result<Box<[ProviderBookLevel]>, DirectOrderBookError> {
    let mut published = Vec::new();
    published
        .try_reserve_exact(depth)
        .map_err(|_| DirectOrderBookError::Allocation)?;
    for aggregate in levels {
        published.push(ProviderBookLevel::new(
            aggregate.price.clone(),
            quantity_from_decimal(aggregate.quantity)?,
        ));
    }
    published.shrink_to_fit();
    Ok(published.into_boxed_slice())
}

fn quantity_from_decimal(value: Decimal) -> Result<ProviderQuantity, DirectOrderBookError> {
    validate_positive(value)?;
    let lexeme = value.normalize().to_string();
    Ok(ProviderQuantity::new(
        ProviderDecimalLexeme::try_new(&lexeme)
            .map_err(|_| DirectOrderBookError::NumericInvariant)?,
    ))
}

fn validate_positive(value: Decimal) -> Result<(), DirectOrderBookError> {
    if value <= Decimal::ZERO {
        Err(DirectOrderBookError::NumericInvariant)
    } else {
        Ok(())
    }
}

/// Top configured price levels derived from the complete healthy order owner.
#[derive(Clone, Debug)]
pub struct DirectPublishedBook {
    bids: Box<[ProviderBookLevel]>,
    asks: Box<[ProviderBookLevel]>,
}

impl DirectPublishedBook {
    /// Returns bids in descending price order.
    pub fn bids(&self) -> &[ProviderBookLevel] {
        &self.bids
    }

    /// Returns asks in ascending price order.
    pub fn asks(&self) -> &[ProviderBookLevel] {
        &self.asks
    }
}

/// Single-writer owner for one product and connection generation.
#[derive(Debug)]
pub struct DirectOrderBook {
    generation: ConnectionGeneration,
    product: ProviderProduct,
    limits: DirectBookLimits,
    phase: DirectSyncPhase,
    queue: VecDeque<ProviderOrderEvent>,
    queue_bytes: usize,
    candidate: Option<OrderBookState>,
    candidate_sequence: Option<SequenceNumber>,
    candidate_timestamp: Option<Timestamp>,
    candidate_receipt: Option<SegmentedHttpResponseReceipt>,
    active: Option<OrderBookState>,
    last_sequence: Option<SequenceNumber>,
    source_timestamp: Option<Timestamp>,
    published: Option<DirectPublishedBook>,
    snapshot_receipt: Option<SegmentedHttpResponseReceipt>,
}

impl DirectOrderBook {
    /// Creates one bounded, non-authoritative generation in `AwaitingSnapshot`.
    ///
    /// # Errors
    ///
    /// Returns allocation failure before any provider state is accepted.
    pub fn try_new(
        generation: ConnectionGeneration,
        product: ProviderProduct,
        limits: DirectBookLimits,
    ) -> Result<Self, DirectOrderBookError> {
        let mut queue = VecDeque::new();
        queue
            .try_reserve(limits.max_queue_events)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        Ok(Self {
            generation,
            product,
            limits,
            phase: DirectSyncPhase::AwaitingSnapshot,
            queue,
            queue_bytes: 0,
            candidate: None,
            candidate_sequence: None,
            candidate_timestamp: None,
            candidate_receipt: None,
            active: None,
            last_sequence: None,
            source_timestamp: None,
            published: None,
            snapshot_receipt: None,
        })
    }

    /// Returns the exact immutable connection generation.
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    /// Returns the immutable provider product sequence domain.
    pub const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    /// Returns the current synchronization phase.
    pub const fn phase(&self) -> DirectSyncPhase {
        self.phase
    }

    /// Queues one captured sequenced event under count, byte, product, and wire-order bounds.
    ///
    /// Any failure irreversibly quarantines this owner.
    pub fn try_queue(&mut self, event: ProviderOrderEvent) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if !matches!(
                self.phase,
                DirectSyncPhase::AwaitingSnapshot
                    | DirectSyncPhase::SnapshotLoaded
                    | DirectSyncPhase::Replaying
            ) {
                return Err(DirectOrderBookError::WrongPhase);
            }
            self.validate_product(&event)?;
            if self.queue.len() == self.limits.max_queue_events {
                return Err(DirectOrderBookError::QueueCountExceeded);
            }
            let next_bytes = self
                .queue_bytes
                .checked_add(event.wire_bytes())
                .ok_or(DirectOrderBookError::QueueBytesExceeded)?;
            if next_bytes > self.limits.max_queue_bytes {
                return Err(DirectOrderBookError::QueueBytesExceeded);
            }
            if let Some(previous) =
                self.queue
                    .back()
                    .map(ProviderOrderEvent::sequence)
                    .or_else(|| {
                        (self.phase == DirectSyncPhase::Replaying)
                            .then_some(self.candidate_sequence)
                            .flatten()
                    })
            {
                validate_successor(previous, event.sequence())?;
            }
            self.queue.push_back(event);
            self.queue_bytes = next_bytes;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Starts streaming a level-3 snapshot into an unpublished bounded candidate.
    pub fn begin_snapshot(&mut self, sequence: SequenceNumber) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::AwaitingSnapshot {
                return Err(DirectOrderBookError::WrongPhase);
            }
            self.candidate = Some(OrderBookState::try_new(self.limits)?);
            self.candidate_sequence = Some(sequence);
            self.candidate_timestamp = None;
            self.candidate_receipt = None;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Streams one snapshot order directly into the bounded order-ID owner.
    pub fn try_push_snapshot_order(
        &mut self,
        order: ProviderOrderRecord,
    ) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::AwaitingSnapshot || self.candidate_sequence.is_none()
            {
                return Err(DirectOrderBookError::WrongPhase);
            }
            self.candidate
                .as_mut()
                .ok_or(DirectOrderBookError::SnapshotRequired)?
                .insert(&order)
        })();
        self.fail_closed(outcome)
    }

    /// Finishes a complete snapshot using the required venue-supplied source time.
    pub fn finish_snapshot(
        &mut self,
        source_timestamp: Timestamp,
    ) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::AwaitingSnapshot
                || self.candidate.is_none()
                || self.candidate_sequence.is_none()
            {
                return Err(DirectOrderBookError::SnapshotRequired);
            }
            self.candidate_timestamp = Some(source_timestamp);
            self.phase = DirectSyncPhase::SnapshotLoaded;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Binds the complete segmented HTTP response receipt to the unpublished snapshot.
    ///
    /// Fixture-only callers may omit this evidence, but no production snapshot decoder should.
    pub fn bind_snapshot_receipt(
        &mut self,
        receipt: SegmentedHttpResponseReceipt,
    ) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::AwaitingSnapshot
                || self.candidate.is_none()
                || self.candidate_sequence.is_none()
                || self.candidate_receipt.is_some()
            {
                return Err(DirectOrderBookError::WrongPhase);
            }
            self.candidate_receipt = Some(receipt);
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Discards queued events at or below the snapshot cursor and enters replay.
    pub fn begin_replay(&mut self) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::SnapshotLoaded {
                return Err(DirectOrderBookError::WrongPhase);
            }
            let snapshot = self
                .candidate_sequence
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            while self
                .queue
                .front()
                .is_some_and(|event| event.sequence() <= snapshot)
            {
                let discarded = self
                    .queue
                    .pop_front()
                    .ok_or(DirectOrderBookError::QueueInvariant)?;
                self.queue_bytes = self
                    .queue_bytes
                    .checked_sub(discarded.wire_bytes())
                    .ok_or(DirectOrderBookError::QueueInvariant)?;
            }
            if let Some(first) = self.queue.front() {
                validate_successor(snapshot, first.sequence())?;
            }
            self.phase = DirectSyncPhase::Replaying;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Applies one queued event to the unpublished candidate.
    ///
    /// Returns `false` only when the replay queue is currently drained. A caller may queue more
    /// exact-successor events before the atomic handoff.
    pub fn replay_next(&mut self) -> Result<bool, DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::Replaying {
                return Err(DirectOrderBookError::WrongPhase);
            }
            let Some(event) = self.queue.pop_front() else {
                return Ok(false);
            };
            self.queue_bytes = self
                .queue_bytes
                .checked_sub(event.wire_bytes())
                .ok_or(DirectOrderBookError::QueueInvariant)?;
            let previous = self
                .candidate_sequence
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            validate_successor(previous, event.sequence())?;
            self.candidate
                .as_mut()
                .ok_or(DirectOrderBookError::SnapshotRequired)?
                .apply(event.kind())?;
            self.candidate_sequence = Some(event.sequence());
            self.candidate_timestamp = Some(event.timestamp());
            Ok(true)
        })();
        self.fail_closed(outcome)
    }

    /// Atomically publishes the snapshot plus fully drained contiguous replay suffix.
    pub fn finish_replay(&mut self) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::Replaying {
                return Err(DirectOrderBookError::WrongPhase);
            }
            if !self.queue.is_empty() || self.queue_bytes != 0 {
                return Err(DirectOrderBookError::ReplayNotDrained);
            }
            let candidate = self
                .candidate
                .take()
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            let sequence = self
                .candidate_sequence
                .take()
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            let timestamp = self
                .candidate_timestamp
                .take()
                .ok_or(DirectOrderBookError::SnapshotTimestampRequired)?;
            let receipt = self.candidate_receipt.take();
            let published = candidate.published(self.limits.published_depth)?;
            self.active = Some(candidate);
            self.last_sequence = Some(sequence);
            self.source_timestamp = Some(timestamp);
            self.published = Some(published);
            self.snapshot_receipt = receipt;
            self.phase = DirectSyncPhase::Healthy;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Applies one exact live successor after the atomic healthy handoff.
    pub fn try_apply_live(
        &mut self,
        event: ProviderOrderEvent,
    ) -> Result<(), DirectOrderBookError> {
        let outcome = (|| {
            if self.phase != DirectSyncPhase::Healthy {
                return Err(DirectOrderBookError::WrongPhase);
            }
            self.validate_product(&event)?;
            let previous = self
                .last_sequence
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            validate_successor(previous, event.sequence())?;
            let mut active = self
                .active
                .take()
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            active.apply(event.kind())?;
            let published = active.published(self.limits.published_depth)?;
            self.active = Some(active);
            self.last_sequence = Some(event.sequence());
            self.source_timestamp = Some(event.timestamp());
            self.published = Some(published);
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Returns a healthy top-depth book. Every earlier or failed phase returns `None`.
    pub fn published_book(&self) -> Option<&DirectPublishedBook> {
        (self.phase == DirectSyncPhase::Healthy)
            .then_some(self.published.as_ref())
            .flatten()
    }

    /// Returns the unpublished replay cursor for diagnostics.
    pub const fn candidate_sequence(&self) -> Option<SequenceNumber> {
        self.candidate_sequence
    }

    /// Returns the authoritative cursor only after healthy handoff.
    pub const fn last_sequence(&self) -> Option<SequenceNumber> {
        if matches!(self.phase, DirectSyncPhase::Healthy) {
            self.last_sequence
        } else {
            None
        }
    }

    /// Returns the venue timestamp associated with the healthy cursor.
    pub const fn source_timestamp(&self) -> Option<Timestamp> {
        if matches!(self.phase, DirectSyncPhase::Healthy) {
            self.source_timestamp
        } else {
            None
        }
    }

    /// Returns the exact snapshot HTTP receipt before replay or after healthy handoff.
    pub fn snapshot_receipt(&self) -> Option<&SegmentedHttpResponseReceipt> {
        match self.phase {
            DirectSyncPhase::SnapshotLoaded | DirectSyncPhase::Replaying => {
                self.candidate_receipt.as_ref()
            }
            DirectSyncPhase::Healthy => self.snapshot_receipt.as_ref(),
            DirectSyncPhase::AwaitingSnapshot | DirectSyncPhase::Quarantined => None,
        }
    }

    /// Irreversibly invalidates this generation after transport, schema, or decoder failure.
    pub fn invalidate_generation(&mut self) {
        self.quarantine();
    }

    fn validate_product(&self, event: &ProviderOrderEvent) -> Result<(), DirectOrderBookError> {
        if event.product() == &self.product {
            Ok(())
        } else {
            Err(DirectOrderBookError::WrongProduct)
        }
    }

    fn fail_closed<T>(
        &mut self,
        outcome: Result<T, DirectOrderBookError>,
    ) -> Result<T, DirectOrderBookError> {
        if outcome.is_err() {
            self.quarantine();
        }
        outcome
    }

    fn quarantine(&mut self) {
        self.phase = DirectSyncPhase::Quarantined;
        self.queue.clear();
        self.queue_bytes = 0;
        self.candidate = None;
        self.candidate_sequence = None;
        self.candidate_timestamp = None;
        self.candidate_receipt = None;
        self.active = None;
        self.last_sequence = None;
        self.source_timestamp = None;
        self.published = None;
        self.snapshot_receipt = None;
    }
}

fn validate_successor(
    previous: SequenceNumber,
    observed: SequenceNumber,
) -> Result<(), DirectOrderBookError> {
    if observed == previous {
        return Err(DirectOrderBookError::DuplicateSequence);
    }
    if observed < previous {
        return Err(DirectOrderBookError::SequenceRegression);
    }
    let expected = previous
        .checked_next()
        .map_err(|_| DirectOrderBookError::SequenceExhausted)?;
    if observed != expected {
        return Err(DirectOrderBookError::SequenceGap);
    }
    Ok(())
}

/// Bounded direct order-book synchronization failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DirectOrderBookError {
    /// A configured count/byte/depth bound is invalid.
    #[error("direct order-book limits are invalid")]
    InvalidLimits,
    /// Preallocation or bounded publication allocation failed.
    #[error("direct order-book allocation failed")]
    Allocation,
    /// An operation is not valid from the current synchronization phase.
    #[error("direct order-book operation is invalid in the current phase")]
    WrongPhase,
    /// An event belongs to another product sequence domain.
    #[error("direct order-book event belongs to the wrong product")]
    WrongProduct,
    /// The bounded replay queue exhausted its event count.
    #[error("direct replay queue event capacity was exceeded")]
    QueueCountExceeded,
    /// The bounded replay queue exhausted its exact raw-byte budget.
    #[error("direct replay queue byte capacity was exceeded")]
    QueueBytesExceeded,
    /// Internal replay byte accounting became inconsistent.
    #[error("direct replay queue accounting invariant failed")]
    QueueInvariant,
    /// Snapshot order count exceeded the configured owner.
    #[error("direct order count capacity was exceeded")]
    OrderCapacityExceeded,
    /// Snapshot aggregate price count exceeded the configured owner.
    #[error("direct aggregate price-level capacity was exceeded")]
    PriceLevelCapacityExceeded,
    /// An order identity was inserted twice.
    #[error("direct order identity is duplicated")]
    DuplicateOrder,
    /// A match referred to no maintained maker order.
    #[error("direct match referred to an unknown maker order")]
    UnknownMakerOrder,
    /// A match quantity exceeded the maker's remaining quantity.
    #[error("direct match quantity exceeded remaining maker quantity")]
    MatchExceedsRemaining,
    /// Exact numeric state was nonpositive, inexact, or overflowed.
    #[error("direct order numeric invariant failed")]
    NumericInvariant,
    /// Price aggregation and the order-ID map disagreed.
    #[error("direct aggregate state invariant failed")]
    AggregateInvariant,
    /// A snapshot candidate has not been initialized.
    #[error("direct order-book snapshot is required")]
    SnapshotRequired,
    /// The snapshot omitted its required provider source time.
    #[error("direct order-book snapshot source time is required")]
    SnapshotTimestampRequired,
    /// Replay handoff was attempted with queued events remaining.
    #[error("direct replay queue is not drained")]
    ReplayNotDrained,
    /// A product sequence was repeated.
    #[error("direct product sequence was duplicated")]
    DuplicateSequence,
    /// A product sequence moved backward.
    #[error("direct product sequence regressed")]
    SequenceRegression,
    /// A product sequence skipped an exact successor.
    #[error("direct product sequence has a gap")]
    SequenceGap,
    /// An exact successor cannot be represented.
    #[error("direct product sequence is exhausted")]
    SequenceExhausted,
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{
        ConnectionGeneration, ProviderProduct, SequenceNumber, SourceIdentifier, Timestamp,
    };
    use market_squawk_sources::{
        ProviderBookLevel, ProviderBookSide, ProviderCursorOnlyReason, ProviderDecimalLexeme,
        ProviderOrderEvent, ProviderOrderEventKind, ProviderOrderRecord, ProviderPrice,
        ProviderQuantity,
    };
    use rust_decimal::Decimal;

    use super::{DirectBookLimits, DirectOrderBook, DirectOrderBookError, DirectSyncPhase};

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn id(value: &str) -> TestResult<SourceIdentifier> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    fn product() -> TestResult<ProviderProduct> {
        Ok(ProviderProduct::new(id("BTC-USD")?))
    }

    fn level(price: &str, quantity: &str) -> TestResult<ProviderBookLevel> {
        Ok(ProviderBookLevel::new(
            ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
            ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
        ))
    }

    fn order(
        order_id: &str,
        side: ProviderBookSide,
        price: &str,
        quantity: &str,
    ) -> TestResult<ProviderOrderRecord> {
        Ok(ProviderOrderRecord::new(
            id(order_id)?,
            side,
            level(price, quantity)?,
        ))
    }

    fn event(
        sequence: u64,
        kind: ProviderOrderEventKind,
        wire_bytes: usize,
    ) -> TestResult<ProviderOrderEvent> {
        Ok(ProviderOrderEvent::try_new(
            product()?,
            SequenceNumber::new(sequence),
            Timestamp::from_unix_nanos(i64::try_from(sequence)?),
            kind,
            wire_bytes,
        )?)
    }

    fn limits() -> TestResult<DirectBookLimits> {
        Ok(DirectBookLimits::try_new(8, 8, 8, 4_096, 2)?)
    }

    #[test]
    fn cursor_only_replay_advances_exactly_and_publishes_only_after_atomic_handoff() -> TestResult {
        let mut owner =
            DirectOrderBook::try_new(ConnectionGeneration::new(1)?, product()?, limits()?)?;
        assert_eq!(owner.phase(), DirectSyncPhase::AwaitingSnapshot);
        assert!(owner.published_book().is_none());

        owner.try_queue(event(
            11,
            ProviderOrderEventKind::CursorOnly(ProviderCursorOnlyReason::Received),
            64,
        )?)?;
        owner.try_queue(event(
            12,
            ProviderOrderEventKind::Done {
                order_id: id("unknown-received-only")?,
            },
            64,
        )?)?;

        owner.begin_snapshot(SequenceNumber::new(10))?;
        owner.try_push_snapshot_order(order("bid-a", ProviderBookSide::Bid, "100.00", "5.00")?)?;
        owner.try_push_snapshot_order(order("ask-a", ProviderBookSide::Ask, "101.00", "4.00")?)?;
        owner.finish_snapshot(Timestamp::from_unix_nanos(10))?;
        assert_eq!(owner.phase(), DirectSyncPhase::SnapshotLoaded);
        assert!(owner.published_book().is_none());

        owner.begin_replay()?;
        assert_eq!(owner.phase(), DirectSyncPhase::Replaying);
        assert!(owner.published_book().is_none());
        assert!(owner.replay_next()?);
        assert!(owner.replay_next()?);
        assert!(!owner.replay_next()?);
        assert_eq!(owner.candidate_sequence(), Some(SequenceNumber::new(12)));
        assert!(owner.published_book().is_none());

        owner.finish_replay()?;
        assert_eq!(owner.phase(), DirectSyncPhase::Healthy);
        assert_eq!(owner.last_sequence(), Some(SequenceNumber::new(12)));
        let published = owner.published_book().ok_or("missing healthy book")?;
        assert_eq!(published.bids().len(), 1);
        assert_eq!(published.asks().len(), 1);
        assert_eq!(
            published.bids()[0].quantity().value().decimal(),
            Decimal::new(500, 2)
        );
        assert_eq!(
            published.asks()[0].quantity().value().decimal(),
            Decimal::new(400, 2)
        );
        Ok(())
    }

    #[test]
    fn replay_invariant_failure_quarantines_without_exposing_candidate_state() -> TestResult {
        let mut owner =
            DirectOrderBook::try_new(ConnectionGeneration::new(1)?, product()?, limits()?)?;
        owner.try_queue(event(
            11,
            ProviderOrderEventKind::Match {
                maker_order_id: id("missing-maker")?,
                quantity: ProviderQuantity::new(ProviderDecimalLexeme::try_new("1.00")?),
            },
            96,
        )?)?;
        owner.begin_snapshot(SequenceNumber::new(10))?;
        owner.try_push_snapshot_order(order("bid-a", ProviderBookSide::Bid, "100.00", "5.00")?)?;
        owner.finish_snapshot(Timestamp::from_unix_nanos(10))?;
        owner.begin_replay()?;

        assert_eq!(
            owner.replay_next(),
            Err(DirectOrderBookError::UnknownMakerOrder)
        );
        assert_eq!(owner.phase(), DirectSyncPhase::Quarantined);
        assert!(owner.published_book().is_none());
        assert_eq!(owner.last_sequence(), None);
        Ok(())
    }
}
