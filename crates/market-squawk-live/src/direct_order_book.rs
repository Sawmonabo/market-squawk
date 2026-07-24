//! Bounded level-3 ownership and atomic snapshot/replay handoff.

use std::collections::{BTreeMap, HashMap, VecDeque};

use market_squawk_domain::{
    ConnectionGeneration, InstrumentExecutionTerms, PriceTicks, ProviderProduct, QuantityLots,
    SequenceNumber, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    FrameSessionBinding, ProviderBookSide, ProviderOrderEvent, ProviderOrderEventKind,
    ProviderOrderRecord, SegmentedHttpResponseReceipt,
};
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
    price: PriceTicks,
    quantity: QuantityLots,
}

impl OwnedOrder {
    fn from_record(record: &ProviderOrderRecord) -> Self {
        Self {
            side: record.side(),
            price: record.price(),
            quantity: record.quantity(),
        }
    }
}

#[derive(Clone, Debug)]
struct PriceAggregate {
    quantity: QuantityLots,
    orders: usize,
}

#[derive(Debug)]
struct OrderBookState {
    orders: HashMap<SourceIdentifier, OwnedOrder>,
    bids: BTreeMap<PriceTicks, PriceAggregate>,
    asks: BTreeMap<PriceTicks, PriceAggregate>,
    limits: DirectBookLimits,
}

impl OrderBookState {
    fn try_new(limits: DirectBookLimits) -> Result<Self, DirectOrderBookError> {
        let mut orders = HashMap::new();
        orders
            .try_reserve(limits.max_orders)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        Ok(Self {
            orders,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            limits,
        })
    }

    fn levels(&self, side: ProviderBookSide) -> &BTreeMap<PriceTicks, PriceAggregate> {
        match side {
            ProviderBookSide::Bid => &self.bids,
            ProviderBookSide::Ask => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: ProviderBookSide) -> &mut BTreeMap<PriceTicks, PriceAggregate> {
        match side {
            ProviderBookSide::Bid => &mut self.bids,
            ProviderBookSide::Ask => &mut self.asks,
        }
    }

    fn level_count(&self) -> Result<usize, DirectOrderBookError> {
        self.bids
            .len()
            .checked_add(self.asks.len())
            .ok_or(DirectOrderBookError::NumericInvariant)
    }

    fn insert(&mut self, record: &ProviderOrderRecord) -> Result<(), DirectOrderBookError> {
        let price = record.price();
        let quantity = record.quantity();
        validate_positive_price(price)?;
        validate_positive_quantity(quantity)?;
        if self.orders.contains_key(record.order_id()) {
            return Err(DirectOrderBookError::DuplicateOrder);
        }
        if self.orders.len() == self.limits.max_orders {
            return Err(DirectOrderBookError::OrderCapacityExceeded);
        }
        let existing = self.levels(record.side()).get(&price);
        if existing.is_none() && self.level_count()? == self.limits.max_price_levels {
            return Err(DirectOrderBookError::PriceLevelCapacityExceeded);
        }
        let next_existing = existing
            .map(|aggregate| {
                if aggregate.orders == 0 || aggregate.quantity.get() == 0 {
                    return Err(DirectOrderBookError::AggregateInvariant);
                }
                let next_quantity = aggregate
                    .quantity
                    .checked_add(quantity)
                    .map_err(|_| DirectOrderBookError::NumericInvariant)?;
                let next_orders = aggregate
                    .orders
                    .checked_add(1)
                    .ok_or(DirectOrderBookError::NumericInvariant)?;
                Ok((next_quantity, next_orders))
            })
            .transpose()?;
        match next_existing {
            Some((next_quantity, next_orders)) => {
                let aggregate = self
                    .levels_mut(record.side())
                    .get_mut(&price)
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
                aggregate.quantity = next_quantity;
                aggregate.orders = next_orders;
            }
            None => {
                self.levels_mut(record.side()).insert(
                    price,
                    PriceAggregate {
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
        let Some(order) = self.orders.get(order_id).cloned() else {
            return Ok(false);
        };
        let price = order.price;
        let quantity = order.quantity;
        let aggregate = self
            .levels(order.side)
            .get(&price)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        validate_owned_aggregate(aggregate, quantity)?;
        let next_orders = aggregate
            .orders
            .checked_sub(1)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        let next_quantity = aggregate
            .quantity
            .checked_sub(quantity)
            .map_err(|_| DirectOrderBookError::NumericInvariant)?;
        let remove_level = next_orders == 0;
        if remove_level != (next_quantity.get() == 0) {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        if remove_level {
            self.levels_mut(order.side).remove(&price);
        } else {
            let aggregate = self
                .levels_mut(order.side)
                .get_mut(&price)
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            aggregate.orders = next_orders;
            aggregate.quantity = next_quantity;
        }
        self.orders
            .remove(order_id)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        Ok(true)
    }

    fn apply_match(
        &mut self,
        maker_order_id: &SourceIdentifier,
        quantity: QuantityLots,
    ) -> Result<(), DirectOrderBookError> {
        let matched = quantity;
        validate_positive_quantity(matched)?;
        let order = self
            .orders
            .get(maker_order_id)
            .cloned()
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?;
        let remaining = order.quantity;
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
            .map_err(|_| DirectOrderBookError::NumericInvariant)?;
        let price = order.price;
        let aggregate = self
            .levels(order.side)
            .get(&price)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        validate_owned_aggregate(aggregate, remaining)?;
        let next_aggregate_quantity = aggregate
            .quantity
            .checked_sub(matched)
            .map_err(|_| DirectOrderBookError::NumericInvariant)?;
        if next_aggregate_quantity.get() == 0 {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.levels_mut(order.side)
            .get_mut(&price)
            .ok_or(DirectOrderBookError::AggregateInvariant)?
            .quantity = next_aggregate_quantity;
        self.orders
            .get_mut(maker_order_id)
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?
            .quantity = next;
        Ok(())
    }

    fn apply_change(
        &mut self,
        order_id: &SourceIdentifier,
        previous_price: Option<PriceTicks>,
        new_price: Option<PriceTicks>,
        new_quantity: Option<QuantityLots>,
    ) -> Result<(), DirectOrderBookError> {
        if new_price.is_some() && previous_price.is_none() {
            return Err(DirectOrderBookError::InvalidChange);
        }
        if previous_price.is_none() && new_price.is_none() && new_quantity.is_none() {
            return Ok(());
        }
        let Some(order) = self.orders.get(order_id).cloned() else {
            return Ok(());
        };

        let previous_price_value = order.price;
        if previous_price.is_some_and(|expected| expected != previous_price_value) {
            return Err(DirectOrderBookError::ChangePriceMismatch);
        }
        let next_price = new_price.unwrap_or(order.price);
        let next_quantity = new_quantity.unwrap_or(order.quantity);
        let next_price_value = next_price;
        let previous_quantity_value = order.quantity;
        let next_quantity_value = next_quantity;
        validate_positive_price(next_price_value)?;
        validate_positive_quantity(next_quantity_value)?;

        let previous_aggregate = self
            .levels(order.side)
            .get(&previous_price_value)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        validate_owned_aggregate(previous_aggregate, previous_quantity_value)?;
        if next_price_value == previous_price_value {
            let next_aggregate_quantity = previous_aggregate
                .quantity
                .checked_sub(previous_quantity_value)
                .and_then(|value| value.checked_add(next_quantity_value))
                .map_err(|_| DirectOrderBookError::NumericInvariant)?;
            validate_positive_quantity(next_aggregate_quantity)?;
            self.levels_mut(order.side)
                .get_mut(&previous_price_value)
                .ok_or(DirectOrderBookError::AggregateInvariant)?
                .quantity = next_aggregate_quantity;
        } else {
            let previous_level_quantity = previous_aggregate
                .quantity
                .checked_sub(previous_quantity_value)
                .map_err(|_| DirectOrderBookError::NumericInvariant)?;
            let remove_previous_level = previous_aggregate.orders == 1;
            if remove_previous_level != (previous_level_quantity.get() == 0) {
                return Err(DirectOrderBookError::AggregateInvariant);
            }

            let next_existing = self.levels(order.side).get(&next_price_value);
            if next_existing.is_none()
                && self.level_count()? == self.limits.max_price_levels
                && !remove_previous_level
            {
                return Err(DirectOrderBookError::PriceLevelCapacityExceeded);
            }
            let next_level_quantity =
                next_existing.map_or(Ok(next_quantity_value), |aggregate| {
                    if aggregate.orders == 0 || aggregate.quantity.get() == 0 {
                        return Err(DirectOrderBookError::AggregateInvariant);
                    }
                    aggregate
                        .quantity
                        .checked_add(next_quantity_value)
                        .map_err(|_| DirectOrderBookError::NumericInvariant)
                })?;
            let next_level_orders = next_existing.map_or(Ok(1), |aggregate| {
                aggregate
                    .orders
                    .checked_add(1)
                    .ok_or(DirectOrderBookError::NumericInvariant)
            })?;
            validate_positive_quantity(next_level_quantity)?;

            if remove_previous_level {
                self.levels_mut(order.side).remove(&previous_price_value);
            } else {
                let aggregate = self
                    .levels_mut(order.side)
                    .get_mut(&previous_price_value)
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
                aggregate.quantity = previous_level_quantity;
                aggregate.orders -= 1;
            }
            match self.levels_mut(order.side).get_mut(&next_price_value) {
                Some(aggregate) => {
                    aggregate.quantity = next_level_quantity;
                    aggregate.orders = next_level_orders;
                }
                None => {
                    self.levels_mut(order.side).insert(
                        next_price_value,
                        PriceAggregate {
                            quantity: next_level_quantity,
                            orders: next_level_orders,
                        },
                    );
                }
            }
        }

        let maintained = self
            .orders
            .get_mut(order_id)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        maintained.price = next_price;
        maintained.quantity = next_quantity;
        Ok(())
    }

    fn apply(&mut self, kind: &ProviderOrderEventKind) -> Result<(), DirectOrderBookError> {
        match kind {
            ProviderOrderEventKind::CursorOnly(_) => Ok(()),
            ProviderOrderEventKind::Open(order) => self.insert(order),
            ProviderOrderEventKind::Match {
                maker_order_id,
                quantity,
            } => self.apply_match(maker_order_id, *quantity),
            ProviderOrderEventKind::Done { order_id } => {
                let _known = self.remove_if_known(order_id)?;
                Ok(())
            }
            ProviderOrderEventKind::Change {
                order_id,
                previous_price,
                new_price,
                new_quantity,
            } => self.apply_change(order_id, *previous_price, *new_price, *new_quantity),
        }
    }

    fn published(&self, depth: usize) -> Result<DirectPublishedBook<'_>, DirectOrderBookError> {
        if depth != self.limits.published_depth {
            return Err(DirectOrderBookError::InvalidLimits);
        }
        Ok(DirectPublishedBook {
            bids: &self.bids,
            asks: &self.asks,
            depth,
        })
    }
}

fn validate_owned_aggregate(
    aggregate: &PriceAggregate,
    owned_quantity: QuantityLots,
) -> Result<(), DirectOrderBookError> {
    if aggregate.orders == 0
        || aggregate.quantity < owned_quantity
        || (aggregate.orders == 1) != (aggregate.quantity == owned_quantity)
    {
        Err(DirectOrderBookError::AggregateInvariant)
    } else {
        Ok(())
    }
}

fn validate_positive_price(value: PriceTicks) -> Result<(), DirectOrderBookError> {
    if value.get() <= 0 {
        Err(DirectOrderBookError::NumericInvariant)
    } else {
        Ok(())
    }
}

fn validate_positive_quantity(value: QuantityLots) -> Result<(), DirectOrderBookError> {
    if value.get() == 0 {
        Err(DirectOrderBookError::NumericInvariant)
    } else {
        Ok(())
    }
}

/// One allocation-free aggregate level view from a healthy direct order book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectPublishedLevel {
    price: PriceTicks,
    quantity: QuantityLots,
}

impl DirectPublishedLevel {
    /// Returns the instrument-scaled aggregate price.
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the instrument-scaled aggregate quantity.
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// Allocation-free top-depth view derived directly from the ordered aggregate owner.
#[derive(Clone, Copy, Debug)]
pub struct DirectPublishedBook<'a> {
    bids: &'a BTreeMap<PriceTicks, PriceAggregate>,
    asks: &'a BTreeMap<PriceTicks, PriceAggregate>,
    depth: usize,
}

impl DirectPublishedBook<'_> {
    /// Iterates bids in descending price order without copying or allocating.
    pub fn bids(&self) -> impl Iterator<Item = DirectPublishedLevel> + '_ {
        self.bids
            .iter()
            .rev()
            .take(self.depth)
            .map(|(price, aggregate)| DirectPublishedLevel {
                price: *price,
                quantity: aggregate.quantity,
            })
    }

    /// Iterates asks in ascending price order without copying or allocating.
    pub fn asks(&self) -> impl Iterator<Item = DirectPublishedLevel> + '_ {
        self.asks
            .iter()
            .take(self.depth)
            .map(|(price, aggregate)| DirectPublishedLevel {
                price: *price,
                quantity: aggregate.quantity,
            })
    }
}

/// Single-writer owner for one product and connection generation.
#[derive(Debug)]
pub struct DirectOrderBook {
    generation: ConnectionGeneration,
    binding: Option<FrameSessionBinding>,
    product: ProviderProduct,
    terms: InstrumentExecutionTerms,
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
        terms: InstrumentExecutionTerms,
        limits: DirectBookLimits,
    ) -> Result<Self, DirectOrderBookError> {
        let mut queue = VecDeque::new();
        queue
            .try_reserve(limits.max_queue_events)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        Ok(Self {
            generation,
            binding: None,
            product,
            terms,
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

    /// Returns the immutable instrument terms required by every normalized event and snapshot.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.terms
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
            self.bind_event_authority(&event)?;
            self.validate_product(&event)?;
            self.validate_terms(&event)?;
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
            if order.execution_terms() != self.terms {
                return Err(DirectOrderBookError::InstrumentTermsMismatch);
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
            if self.phase != DirectSyncPhase::AwaitingSnapshot {
                return Err(DirectOrderBookError::WrongPhase);
            }
            if self.candidate.is_none() || self.candidate_sequence.is_none() {
                return Err(DirectOrderBookError::SnapshotRequired);
            }
            if self.candidate_receipt.is_none() {
                return Err(DirectOrderBookError::SnapshotReceiptRequired);
            }
            self.candidate_timestamp = Some(source_timestamp);
            self.phase = DirectSyncPhase::SnapshotLoaded;
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Binds the complete segmented HTTP response receipt to the unpublished snapshot.
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
            self.bind_snapshot_authority(receipt.binding())?;
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
            let receipt = self
                .candidate_receipt
                .take()
                .ok_or(DirectOrderBookError::SnapshotReceiptRequired)?;
            if self.binding.is_none() {
                return Err(DirectOrderBookError::SnapshotReceiptRequired);
            }
            self.active = Some(candidate);
            self.last_sequence = Some(sequence);
            self.source_timestamp = Some(timestamp);
            self.snapshot_receipt = Some(receipt);
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
            self.bind_event_authority(&event)?;
            self.validate_product(&event)?;
            self.validate_terms(&event)?;
            let previous = self
                .last_sequence
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            validate_successor(previous, event.sequence())?;
            self.active
                .as_mut()
                .ok_or(DirectOrderBookError::SnapshotRequired)?
                .apply(event.kind())?;
            self.last_sequence = Some(event.sequence());
            self.source_timestamp = Some(event.timestamp());
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Returns a healthy top-depth book. Every earlier or failed phase returns `None`.
    pub fn published_book(&self) -> Option<DirectPublishedBook<'_>> {
        if self.phase != DirectSyncPhase::Healthy {
            return None;
        }
        self.active
            .as_ref()?
            .published(self.limits.published_depth)
            .ok()
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

    fn validate_terms(&self, event: &ProviderOrderEvent) -> Result<(), DirectOrderBookError> {
        if event.execution_terms() == self.terms {
            Ok(())
        } else {
            Err(DirectOrderBookError::InstrumentTermsMismatch)
        }
    }

    fn bind_event_authority(
        &mut self,
        event: &ProviderOrderEvent,
    ) -> Result<(), DirectOrderBookError> {
        let observed = event.evidence().binding();
        if observed.connection_generation() != self.generation {
            return Err(DirectOrderBookError::EventGenerationMismatch);
        }
        match self.binding.as_ref() {
            Some(expected) if !expected.shares_allocation_with(observed) => {
                Err(DirectOrderBookError::EventSessionMismatch)
            }
            Some(_) => Ok(()),
            None => {
                self.binding = Some(observed.clone());
                Ok(())
            }
        }
    }

    fn bind_snapshot_authority(
        &mut self,
        observed: &FrameSessionBinding,
    ) -> Result<(), DirectOrderBookError> {
        if observed.connection_generation() != self.generation {
            return Err(DirectOrderBookError::SnapshotGenerationMismatch);
        }
        match self.binding.as_ref() {
            Some(expected) if !expected.shares_allocation_with(observed) => {
                Err(DirectOrderBookError::SnapshotSessionMismatch)
            }
            Some(_) => Ok(()),
            None => {
                self.binding = Some(observed.clone());
                Ok(())
            }
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
        self.binding = None;
        self.queue.clear();
        self.queue_bytes = 0;
        self.candidate = None;
        self.candidate_sequence = None;
        self.candidate_timestamp = None;
        self.candidate_receipt = None;
        self.active = None;
        self.last_sequence = None;
        self.source_timestamp = None;
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
    /// An event was normalized against different instrument execution terms.
    #[error("direct order-book event uses different instrument execution terms")]
    InstrumentTermsMismatch,
    /// A WebSocket event belongs to another connection generation.
    #[error("direct order-book event belongs to another connection generation")]
    EventGenerationMismatch,
    /// A WebSocket event does not share the exact source/session allocation.
    #[error("direct order-book event belongs to another source session")]
    EventSessionMismatch,
    /// A snapshot receipt belongs to another connection generation.
    #[error("direct order-book snapshot belongs to another connection generation")]
    SnapshotGenerationMismatch,
    /// A snapshot receipt does not share the exact source/session allocation.
    #[error("direct order-book snapshot belongs to another source session")]
    SnapshotSessionMismatch,
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
    /// A change omitted the old-price evidence required for a price replacement.
    #[error("direct order change is structurally invalid")]
    InvalidChange,
    /// A change's previous price disagreed with the maintained order.
    #[error("direct order change previous price does not match maintained state")]
    ChangePriceMismatch,
    /// Exact numeric state was nonpositive, inexact, or overflowed.
    #[error("direct order numeric invariant failed")]
    NumericInvariant,
    /// Price aggregation and the order-ID map disagreed.
    #[error("direct aggregate state invariant failed")]
    AggregateInvariant,
    /// A snapshot candidate has not been initialized.
    #[error("direct order-book snapshot is required")]
    SnapshotRequired,
    /// A complete registry-trusted HTTP receipt was not bound to the snapshot.
    #[error("direct order-book snapshot receipt is required")]
    SnapshotReceiptRequired,
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
    use std::str::FromStr as _;

    use market_squawk_domain::{
        ConnectionGeneration, Currency, Denomination, InstrumentDefinitionRevision,
        InstrumentExecutionTerms, InstrumentId, LotSize, PriceTicks, ProviderProduct, QuantityLots,
        SequenceNumber, SourceIdentifier, TickSize, Timestamp,
    };
    use market_squawk_sources::{ProviderBookSide, ProviderOrderEventKind, ProviderOrderRecord};
    use rust_decimal::Decimal;

    use super::{
        DirectBookLimits, DirectOrderBook, DirectOrderBookError, DirectSyncPhase, OrderBookState,
    };

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    fn id(value: &str) -> TestResult<SourceIdentifier> {
        Ok(SourceIdentifier::try_from(value)?)
    }

    fn product() -> TestResult<ProviderProduct> {
        Ok(ProviderProduct::new(id("BTC-USD")?))
    }

    fn terms() -> TestResult<InstrumentExecutionTerms> {
        Ok(InstrumentExecutionTerms::try_new(
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            InstrumentDefinitionRevision::try_from(1)?,
            TickSize::try_from_decimal(Decimal::new(1, 2))?,
            LotSize::try_from_decimal(Decimal::new(1, 2))?,
            Currency::try_from("USD")?,
            Denomination::Currency(Currency::try_from("BTC")?),
            Decimal::ONE,
        )?)
    }

    fn order(
        order_id: &str,
        side: ProviderBookSide,
        price: &str,
        quantity: &str,
    ) -> TestResult<ProviderOrderRecord> {
        let terms = terms()?;
        Ok(ProviderOrderRecord::new(
            id(order_id)?,
            side,
            PriceTicks::try_from_decimal(Decimal::from_str_exact(price)?, terms.price_tick())?,
            QuantityLots::try_from_decimal(Decimal::from_str_exact(quantity)?, terms.lot_size())?,
            terms,
        ))
    }

    fn limits() -> TestResult<DirectBookLimits> {
        Ok(DirectBookLimits::try_new(8, 8, 8, 4_096, 2)?)
    }

    #[test]
    fn snapshot_without_a_trusted_receipt_can_never_reach_replay() -> TestResult {
        let mut owner = DirectOrderBook::try_new(
            ConnectionGeneration::new(1)?,
            product()?,
            terms()?,
            limits()?,
        )?;
        owner.begin_snapshot(SequenceNumber::new(10))?;
        owner.try_push_snapshot_order(order("bid-a", ProviderBookSide::Bid, "100.00", "5.00")?)?;
        assert_eq!(
            owner.finish_snapshot(Timestamp::from_unix_nanos(10)),
            Err(DirectOrderBookError::SnapshotReceiptRequired)
        );
        assert_eq!(owner.phase(), DirectSyncPhase::Quarantined);
        assert!(owner.published_book().is_none());
        Ok(())
    }

    #[test]
    fn bounded_incremental_depth_handles_atomic_price_moves_and_capacity_failure() -> TestResult {
        let limits = DirectBookLimits::try_new(3, 2, 2, 1_024, 1)?;
        let mut state = OrderBookState::try_new(limits)?;
        state.insert(&order("bid-a", ProviderBookSide::Bid, "100.00", "2.00")?)?;
        state.insert(&order("bid-b", ProviderBookSide::Bid, "99.00", "3.00")?)?;
        assert_eq!(
            state
                .published(1)?
                .bids()
                .next()
                .ok_or(DirectOrderBookError::AggregateInvariant)?
                .price(),
            PriceTicks::new(10_000)
        );

        state.apply(&ProviderOrderEventKind::Change {
            order_id: id("bid-a")?,
            previous_price: Some(PriceTicks::new(10_000)),
            new_price: Some(PriceTicks::new(9_800)),
            new_quantity: Some(QuantityLots::new(200)?),
        })?;
        assert_eq!(
            state
                .published(1)?
                .bids()
                .next()
                .ok_or(DirectOrderBookError::AggregateInvariant)?
                .price(),
            PriceTicks::new(9_900)
        );
        assert_eq!(
            state.insert(&order("bid-c", ProviderBookSide::Bid, "97.00", "1.00")?),
            Err(DirectOrderBookError::PriceLevelCapacityExceeded)
        );
        let published = state.published(1)?;
        assert_eq!(
            published
                .bids()
                .next()
                .ok_or(DirectOrderBookError::AggregateInvariant)?
                .price(),
            PriceTicks::new(9_900)
        );

        assert_eq!(
            state.apply(&ProviderOrderEventKind::Change {
                order_id: id("bid-b")?,
                previous_price: Some(PriceTicks::new(10_100)),
                new_price: Some(PriceTicks::new(9_600)),
                new_quantity: Some(QuantityLots::new(300)?),
            }),
            Err(DirectOrderBookError::ChangePriceMismatch)
        );
        assert_eq!(
            state
                .published(1)?
                .bids()
                .next()
                .ok_or(DirectOrderBookError::AggregateInvariant)?
                .price(),
            PriceTicks::new(9_900)
        );
        Ok(())
    }
}
