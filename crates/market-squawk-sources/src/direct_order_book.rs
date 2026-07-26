//! Provider-generic bounded level-3 ownership and atomic snapshot/replay handoff.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

use crate::{
    FrameSessionBinding, FrameSessionLease, ProviderBookSide, ProviderOrderChangeReason,
    ProviderOrderEvent, ProviderOrderEventKind, ProviderOrderRecord, SegmentedHttpResponseReceipt,
};
use market_squawk_domain::{
    ConnectionGeneration, InstrumentExecutionTerms, PriceTicks, ProviderProduct, QuantityLots,
    SequenceNumber, SourceIdentifier, Timestamp,
};
use thiserror::Error;

const MAX_DIRECT_ORDERS: usize = 2_000_000;
const MAX_DIRECT_PRICE_LEVELS: usize = 1_000_000;
const MAX_DIRECT_QUEUE_EVENTS: usize = 1_000_000;
const MAX_PUBLISHED_DEPTH: usize = 10_000;
const MAX_LEVEL_TREE_HEIGHT: usize = 64;

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
            .checked_mul(crate::MAX_RAW_FRAME_BYTES)
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
struct PriceLevelNode {
    price: PriceTicks,
    aggregate: PriceAggregate,
    left: Option<usize>,
    right: Option<usize>,
    height: u8,
}

impl PriceLevelNode {
    const fn new(price: PriceTicks, aggregate: PriceAggregate) -> Self {
        Self {
            price,
            aggregate,
            left: None,
            right: None,
            height: 1,
        }
    }
}

/// Fallibly pre-admitted, shared-capacity indexed AVL forest for both book sides.
///
/// Node and free-index vectors reserve their complete configured capacity before use. Inserts,
/// removals, lookups, and rotations are worst-case O(log n) and never ask the allocator for
/// per-level storage.
#[derive(Debug)]
struct OrderedLevelArena {
    nodes: Vec<Option<PriceLevelNode>>,
    free: Vec<usize>,
    roots: [Option<usize>; 2],
    len: usize,
    capacity: usize,
}

impl OrderedLevelArena {
    fn try_new(capacity: usize) -> Result<Self, DirectOrderBookError> {
        let mut nodes = Vec::new();
        nodes
            .try_reserve_exact(capacity)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        let mut free = Vec::new();
        free.try_reserve_exact(capacity)
            .map_err(|_| DirectOrderBookError::Allocation)?;
        Ok(Self {
            nodes,
            free,
            roots: [None, None],
            len: 0,
            capacity,
        })
    }

    const fn side_index(side: ProviderBookSide) -> usize {
        match side {
            ProviderBookSide::Bid => 0,
            ProviderBookSide::Ask => 1,
        }
    }

    fn node(&self, index: usize) -> Result<&PriceLevelNode, DirectOrderBookError> {
        self.nodes
            .get(index)
            .and_then(Option::as_ref)
            .ok_or(DirectOrderBookError::AggregateInvariant)
    }

    fn node_mut(&mut self, index: usize) -> Result<&mut PriceLevelNode, DirectOrderBookError> {
        self.nodes
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or(DirectOrderBookError::AggregateInvariant)
    }

    fn height(&self, index: Option<usize>) -> Result<u8, DirectOrderBookError> {
        index.map_or(Ok(0), |index| self.node(index).map(|node| node.height))
    }

    fn update_height(&mut self, index: usize) -> Result<(), DirectOrderBookError> {
        let (left, right) = {
            let node = self.node(index)?;
            (node.left, node.right)
        };
        let height = self
            .height(left)?
            .max(self.height(right)?)
            .checked_add(1)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        if usize::from(height) > MAX_LEVEL_TREE_HEIGHT {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.node_mut(index)?.height = height;
        Ok(())
    }

    fn balance_factor(&self, index: usize) -> Result<i16, DirectOrderBookError> {
        let node = self.node(index)?;
        Ok(i16::from(self.height(node.left)?) - i16::from(self.height(node.right)?))
    }

    fn rotate_left(&mut self, root: usize) -> Result<usize, DirectOrderBookError> {
        let pivot = self
            .node(root)?
            .right
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        let middle = self.node(pivot)?.left;
        self.node_mut(root)?.right = middle;
        self.node_mut(pivot)?.left = Some(root);
        self.update_height(root)?;
        self.update_height(pivot)?;
        Ok(pivot)
    }

    fn rotate_right(&mut self, root: usize) -> Result<usize, DirectOrderBookError> {
        let pivot = self
            .node(root)?
            .left
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        let middle = self.node(pivot)?.right;
        self.node_mut(root)?.left = middle;
        self.node_mut(pivot)?.right = Some(root);
        self.update_height(root)?;
        self.update_height(pivot)?;
        Ok(pivot)
    }

    fn rebalance(&mut self, root: usize) -> Result<usize, DirectOrderBookError> {
        self.update_height(root)?;
        let balance = self.balance_factor(root)?;
        if balance > 1 {
            let left = self
                .node(root)?
                .left
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            if self.balance_factor(left)? < 0 {
                let rotated = self.rotate_left(left)?;
                self.node_mut(root)?.left = Some(rotated);
            }
            return self.rotate_right(root);
        }
        if balance < -1 {
            let right = self
                .node(root)?
                .right
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            if self.balance_factor(right)? > 0 {
                let rotated = self.rotate_right(right)?;
                self.node_mut(root)?.right = Some(rotated);
            }
            return self.rotate_left(root);
        }
        Ok(root)
    }

    fn allocate_node(
        &mut self,
        price: PriceTicks,
        aggregate: PriceAggregate,
    ) -> Result<usize, DirectOrderBookError> {
        if let Some(index) = self.free.pop() {
            let slot = self
                .nodes
                .get_mut(index)
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
            if slot.is_some() {
                return Err(DirectOrderBookError::AggregateInvariant);
            }
            *slot = Some(PriceLevelNode::new(price, aggregate));
            return Ok(index);
        }
        if self.nodes.len() == self.capacity {
            return Err(DirectOrderBookError::PriceLevelCapacityExceeded);
        }
        let index = self.nodes.len();
        self.nodes.push(Some(PriceLevelNode::new(price, aggregate)));
        Ok(index)
    }

    fn release_node(&mut self, index: usize) -> Result<(), DirectOrderBookError> {
        let slot = self
            .nodes
            .get_mut(index)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        if slot.take().is_none() {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.free.push(index);
        Ok(())
    }

    fn insert_recursive(
        &mut self,
        root: usize,
        inserted: usize,
    ) -> Result<usize, DirectOrderBookError> {
        let inserted_price = self.node(inserted)?.price;
        let root_price = self.node(root)?.price;
        match inserted_price.cmp(&root_price) {
            Ordering::Less => {
                let next = match self.node(root)?.left {
                    Some(left) => Some(self.insert_recursive(left, inserted)?),
                    None => Some(inserted),
                };
                self.node_mut(root)?.left = next;
            }
            Ordering::Greater => {
                let next = match self.node(root)?.right {
                    Some(right) => Some(self.insert_recursive(right, inserted)?),
                    None => Some(inserted),
                };
                self.node_mut(root)?.right = next;
            }
            Ordering::Equal => return Err(DirectOrderBookError::AggregateInvariant),
        }
        self.rebalance(root)
    }

    fn insert(
        &mut self,
        side: ProviderBookSide,
        price: PriceTicks,
        aggregate: PriceAggregate,
    ) -> Result<(), DirectOrderBookError> {
        if self.get(side, price).is_some() {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        if self.len == self.capacity {
            return Err(DirectOrderBookError::PriceLevelCapacityExceeded);
        }
        let inserted = self.allocate_node(price, aggregate)?;
        let side_index = Self::side_index(side);
        let next_root = match self.roots[side_index] {
            Some(root) => self.insert_recursive(root, inserted)?,
            None => inserted,
        };
        self.roots[side_index] = Some(next_root);
        self.len = self
            .len
            .checked_add(1)
            .ok_or(DirectOrderBookError::NumericInvariant)?;
        Ok(())
    }

    fn minimum_index(&self, mut index: usize) -> Result<usize, DirectOrderBookError> {
        while let Some(left) = self.node(index)?.left {
            index = left;
        }
        Ok(index)
    }

    fn remove_recursive(
        &mut self,
        root: Option<usize>,
        price: PriceTicks,
        removed: &mut Option<PriceAggregate>,
        capture: bool,
    ) -> Result<Option<usize>, DirectOrderBookError> {
        let Some(root) = root else {
            return Ok(None);
        };
        let root_price = self.node(root)?.price;
        match price.cmp(&root_price) {
            Ordering::Less => {
                let left = self.node(root)?.left;
                let next = self.remove_recursive(left, price, removed, capture)?;
                self.node_mut(root)?.left = next;
            }
            Ordering::Greater => {
                let right = self.node(root)?.right;
                let next = self.remove_recursive(right, price, removed, capture)?;
                self.node_mut(root)?.right = next;
            }
            Ordering::Equal => {
                let (left, right) = {
                    let node = self.node(root)?;
                    (node.left, node.right)
                };
                if capture {
                    *removed = Some(self.node(root)?.aggregate.clone());
                }
                match (left, right) {
                    (None, None) => {
                        self.release_node(root)?;
                        return Ok(None);
                    }
                    (Some(child), None) | (None, Some(child)) => {
                        self.release_node(root)?;
                        return Ok(Some(child));
                    }
                    (Some(_), Some(right)) => {
                        let successor = self.minimum_index(right)?;
                        let successor_price = self.node(successor)?.price;
                        let successor_aggregate = self.node(successor)?.aggregate.clone();
                        {
                            let node = self.node_mut(root)?;
                            node.price = successor_price;
                            node.aggregate = successor_aggregate;
                        }
                        let next =
                            self.remove_recursive(Some(right), successor_price, removed, false)?;
                        self.node_mut(root)?.right = next;
                    }
                }
            }
        }
        self.rebalance(root).map(Some)
    }

    fn remove(
        &mut self,
        side: ProviderBookSide,
        price: PriceTicks,
    ) -> Result<Option<PriceAggregate>, DirectOrderBookError> {
        let side_index = Self::side_index(side);
        let mut removed = None;
        let root = self.remove_recursive(self.roots[side_index], price, &mut removed, true)?;
        self.roots[side_index] = root;
        if removed.is_some() {
            self.len = self
                .len
                .checked_sub(1)
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
        }
        Ok(removed)
    }

    fn get(&self, side: ProviderBookSide, price: PriceTicks) -> Option<&PriceAggregate> {
        let mut current = self.roots[Self::side_index(side)];
        while let Some(index) = current {
            let node = self.nodes.get(index)?.as_ref()?;
            match price.cmp(&node.price) {
                Ordering::Less => current = node.left,
                Ordering::Greater => current = node.right,
                Ordering::Equal => return Some(&node.aggregate),
            }
        }
        None
    }

    fn get_mut(
        &mut self,
        side: ProviderBookSide,
        price: PriceTicks,
    ) -> Option<&mut PriceAggregate> {
        let mut current = self.roots[Self::side_index(side)];
        while let Some(index) = current {
            let ordering = {
                let node = self.nodes.get(index)?.as_ref()?;
                price.cmp(&node.price)
            };
            match ordering {
                Ordering::Less => current = self.nodes.get(index)?.as_ref()?.left,
                Ordering::Greater => current = self.nodes.get(index)?.as_ref()?.right,
                Ordering::Equal => {
                    return self
                        .nodes
                        .get_mut(index)?
                        .as_mut()
                        .map(|node| &mut node.aggregate);
                }
            }
        }
        None
    }

    fn best_price(&self, side: ProviderBookSide) -> Option<PriceTicks> {
        let mut current = self.roots[Self::side_index(side)]?;
        loop {
            let node = self.nodes.get(current)?.as_ref()?;
            let next = match side {
                ProviderBookSide::Bid => node.right,
                ProviderBookSide::Ask => node.left,
            };
            match next {
                Some(index) => current = index,
                None => return Some(node.price),
            }
        }
    }

    const fn len(&self) -> usize {
        self.len
    }

    fn iter(&self, side: ProviderBookSide, depth: usize) -> OrderedLevelIter<'_> {
        OrderedLevelIter::new(self, side, depth)
    }
}

#[derive(Clone, Debug)]
struct OrderedLevelIter<'a> {
    arena: &'a OrderedLevelArena,
    stack: [usize; MAX_LEVEL_TREE_HEIGHT],
    stack_len: usize,
    descending: bool,
    remaining: usize,
    valid: bool,
}

impl<'a> OrderedLevelIter<'a> {
    fn new(arena: &'a OrderedLevelArena, side: ProviderBookSide, depth: usize) -> Self {
        let mut iterator = Self {
            arena,
            stack: [0; MAX_LEVEL_TREE_HEIGHT],
            stack_len: 0,
            descending: side == ProviderBookSide::Bid,
            remaining: depth,
            valid: true,
        };
        iterator.push_branch(arena.roots[OrderedLevelArena::side_index(side)]);
        iterator
    }

    fn push_branch(&mut self, mut current: Option<usize>) {
        while let Some(index) = current {
            if self.stack_len == self.stack.len() {
                self.valid = false;
                self.remaining = 0;
                return;
            }
            self.stack[self.stack_len] = index;
            self.stack_len += 1;
            let Some(node) = self.arena.nodes.get(index).and_then(Option::as_ref) else {
                self.valid = false;
                self.remaining = 0;
                return;
            };
            current = if self.descending {
                node.right
            } else {
                node.left
            };
        }
    }
}

impl Iterator for OrderedLevelIter<'_> {
    type Item = DirectPublishedLevel;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid || self.remaining == 0 || self.stack_len == 0 {
            return None;
        }
        self.stack_len -= 1;
        let index = self.stack[self.stack_len];
        let node = self.arena.nodes.get(index)?.as_ref()?;
        let level = DirectPublishedLevel {
            price: node.price,
            quantity: node.aggregate.quantity,
        };
        let branch = if self.descending {
            node.left
        } else {
            node.right
        };
        self.remaining -= 1;
        self.push_branch(branch);
        Some(level)
    }
}

#[derive(Debug)]
struct OrderBookState {
    orders: HashMap<SourceIdentifier, OwnedOrder>,
    levels: OrderedLevelArena,
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
            levels: OrderedLevelArena::try_new(limits.max_price_levels)?,
            limits,
        })
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
        let existing = self.levels.get(record.side(), price);
        if existing.is_none() && self.levels.len() == self.limits.max_price_levels {
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
                    .levels
                    .get_mut(record.side(), price)
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
                aggregate.quantity = next_quantity;
                aggregate.orders = next_orders;
            }
            None => {
                self.levels.insert(
                    record.side(),
                    price,
                    PriceAggregate {
                        quantity,
                        orders: 1,
                    },
                )?;
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
            .levels
            .get(order.side, price)
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
            self.levels
                .remove(order.side, price)?
                .ok_or(DirectOrderBookError::AggregateInvariant)?;
        } else {
            let aggregate = self
                .levels
                .get_mut(order.side, price)
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
        maker_side: ProviderBookSide,
        maker_price: PriceTicks,
        quantity: QuantityLots,
    ) -> Result<(), DirectOrderBookError> {
        let matched = quantity;
        validate_positive_quantity(matched)?;
        let order = self
            .orders
            .get(maker_order_id)
            .cloned()
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?;
        if maker_side != order.side {
            return Err(DirectOrderBookError::MatchSideMismatch);
        }
        if maker_price != order.price {
            return Err(DirectOrderBookError::MatchPriceMismatch);
        }
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
            .levels
            .get(order.side, price)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        validate_owned_aggregate(aggregate, remaining)?;
        let next_aggregate_quantity = aggregate
            .quantity
            .checked_sub(matched)
            .map_err(|_| DirectOrderBookError::NumericInvariant)?;
        if next_aggregate_quantity.get() == 0 {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        self.levels
            .get_mut(order.side, price)
            .ok_or(DirectOrderBookError::AggregateInvariant)?
            .quantity = next_aggregate_quantity;
        self.orders
            .get_mut(maker_order_id)
            .ok_or(DirectOrderBookError::UnknownMakerOrder)?
            .quantity = next;
        Ok(())
    }

    fn apply_done(
        &mut self,
        order_id: &SourceIdentifier,
        side: Option<ProviderBookSide>,
        price: Option<PriceTicks>,
        remaining_quantity: Option<QuantityLots>,
    ) -> Result<(), DirectOrderBookError> {
        let Some(order) = self.orders.get(order_id).cloned() else {
            return Ok(());
        };
        let (Some(side), Some(price), Some(remaining_quantity)) = (side, price, remaining_quantity)
        else {
            return Err(DirectOrderBookError::InvalidDone);
        };
        if side != order.side || price != order.price || remaining_quantity != order.quantity {
            return Err(DirectOrderBookError::InvalidDone);
        }
        if !self.remove_if_known(order_id)? {
            return Err(DirectOrderBookError::AggregateInvariant);
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "closed change evidence stays explicit through state validation"
    )]
    fn apply_change(
        &mut self,
        order_id: &SourceIdentifier,
        reason: ProviderOrderChangeReason,
        side: ProviderBookSide,
        previous_price: Option<PriceTicks>,
        previous_quantity: Option<QuantityLots>,
        new_price: Option<PriceTicks>,
        new_quantity: Option<QuantityLots>,
    ) -> Result<(), DirectOrderBookError> {
        let Some(order) = self.orders.get(order_id).cloned() else {
            return Ok(());
        };
        if side != order.side {
            return Err(DirectOrderBookError::ChangeSideMismatch);
        }

        match reason {
            ProviderOrderChangeReason::SelfTradePrevention => {
                match (previous_price, previous_quantity, new_price, new_quantity) {
                    (Some(_), Some(previous), None, Some(new)) if new > previous => {
                        return Err(DirectOrderBookError::StpQuantityIncrease);
                    }
                    (Some(_), Some(_), None, Some(_)) => {}
                    _ => return Err(DirectOrderBookError::InvalidChange),
                }
            }
            ProviderOrderChangeReason::ModifyOrder => {
                if previous_price.is_none()
                    || previous_quantity.is_none()
                    || new_price.is_none()
                    || new_quantity.is_none()
                {
                    return Err(DirectOrderBookError::InvalidChange);
                }
            }
        }

        let previous_price_value = order.price;
        if previous_price.is_some_and(|expected| expected != previous_price_value) {
            return Err(DirectOrderBookError::ChangePriceMismatch);
        }
        if previous_quantity.is_some_and(|expected| expected != order.quantity) {
            return Err(DirectOrderBookError::ChangeQuantityMismatch);
        }
        let next_price = new_price.unwrap_or(order.price);
        let next_quantity = new_quantity.unwrap_or(order.quantity);
        let next_price_value = next_price;
        let previous_quantity_value = order.quantity;
        let next_quantity_value = next_quantity;
        validate_positive_price(next_price_value)?;
        validate_positive_quantity(next_quantity_value)?;

        let previous_aggregate = self
            .levels
            .get(order.side, previous_price_value)
            .ok_or(DirectOrderBookError::AggregateInvariant)?;
        validate_owned_aggregate(previous_aggregate, previous_quantity_value)?;
        if next_price_value == previous_price_value {
            let next_aggregate_quantity = previous_aggregate
                .quantity
                .checked_sub(previous_quantity_value)
                .and_then(|value| value.checked_add(next_quantity_value))
                .map_err(|_| DirectOrderBookError::NumericInvariant)?;
            validate_positive_quantity(next_aggregate_quantity)?;
            self.levels
                .get_mut(order.side, previous_price_value)
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

            let next_existing = self.levels.get(order.side, next_price_value);
            if next_existing.is_none()
                && self.levels.len() == self.limits.max_price_levels
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
                self.levels
                    .remove(order.side, previous_price_value)?
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
            } else {
                let aggregate = self
                    .levels
                    .get_mut(order.side, previous_price_value)
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
                aggregate.quantity = previous_level_quantity;
                aggregate.orders = aggregate
                    .orders
                    .checked_sub(1)
                    .ok_or(DirectOrderBookError::AggregateInvariant)?;
            }
            match self.levels.get_mut(order.side, next_price_value) {
                Some(aggregate) => {
                    aggregate.quantity = next_level_quantity;
                    aggregate.orders = next_level_orders;
                }
                None => {
                    self.levels.insert(
                        order.side,
                        next_price_value,
                        PriceAggregate {
                            quantity: next_level_quantity,
                            orders: next_level_orders,
                        },
                    )?;
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
                maker_side,
                maker_price,
                quantity,
            } => self.apply_match(maker_order_id, *maker_side, *maker_price, *quantity),
            ProviderOrderEventKind::Done {
                order_id,
                side,
                price,
                remaining_quantity,
            } => self.apply_done(order_id, *side, *price, *remaining_quantity),
            ProviderOrderEventKind::Change {
                order_id,
                reason,
                side,
                previous_price,
                previous_quantity,
                new_price,
                new_quantity,
            } => self.apply_change(
                order_id,
                *reason,
                *side,
                *previous_price,
                *previous_quantity,
                *new_price,
                *new_quantity,
            ),
        }
    }

    fn validate_uncrossed(&self) -> Result<(), DirectOrderBookError> {
        if self
            .levels
            .best_price(ProviderBookSide::Bid)
            .zip(self.levels.best_price(ProviderBookSide::Ask))
            .is_some_and(|(bid, ask)| bid >= ask)
        {
            Err(DirectOrderBookError::CrossedBook)
        } else {
            Ok(())
        }
    }

    fn published(&self, depth: usize) -> Result<DirectPublishedBook<'_>, DirectOrderBookError> {
        if depth != self.limits.published_depth {
            return Err(DirectOrderBookError::InvalidLimits);
        }
        Ok(DirectPublishedBook {
            levels: &self.levels,
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
    levels: &'a OrderedLevelArena,
    depth: usize,
}

impl DirectPublishedBook<'_> {
    /// Iterates bids in descending price order without copying or allocating.
    pub fn bids(&self) -> impl Iterator<Item = DirectPublishedLevel> + '_ {
        self.levels.iter(ProviderBookSide::Bid, self.depth)
    }

    /// Iterates asks in ascending price order without copying or allocating.
    pub fn asks(&self) -> impl Iterator<Item = DirectPublishedLevel> + '_ {
        self.levels.iter(ProviderBookSide::Ask, self.depth)
    }
}

/// Single-writer owner for one product and connection generation.
#[derive(Debug)]
pub struct DirectOrderBook {
    generation: ConnectionGeneration,
    binding: Option<FrameSessionBinding>,
    currentness: Option<FrameSessionLease>,
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
            currentness: None,
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
    pub fn phase(&self) -> DirectSyncPhase {
        if self.phase == DirectSyncPhase::Healthy && self.validate_current_authority().is_err() {
            DirectSyncPhase::Quarantined
        } else {
            self.phase
        }
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
            self.validate_current_authority()?;
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
            self.validate_current_authority()?;
            self.candidate
                .as_ref()
                .ok_or(DirectOrderBookError::SnapshotRequired)?
                .validate_uncrossed()?;
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
            self.bind_snapshot_authority(receipt.binding(), receipt.currentness_lease())?;
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
            self.validate_current_authority()?;
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
            self.validate_current_authority()?;
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
            let candidate = self
                .candidate
                .as_mut()
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            candidate.apply(event.kind())?;
            candidate.validate_uncrossed()?;
            self.validate_current_authority()?;
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
            self.validate_current_authority()?;
            self.candidate
                .as_ref()
                .ok_or(DirectOrderBookError::SnapshotRequired)?
                .validate_uncrossed()?;
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
            let active = self
                .active
                .as_mut()
                .ok_or(DirectOrderBookError::SnapshotRequired)?;
            active.apply(event.kind())?;
            active.validate_uncrossed()?;
            self.validate_current_authority()?;
            self.last_sequence = Some(event.sequence());
            self.source_timestamp = Some(event.timestamp());
            Ok(())
        })();
        self.fail_closed(outcome)
    }

    /// Returns a healthy top-depth book. Every earlier or failed phase returns `None`.
    pub fn published_book(&mut self) -> Option<DirectPublishedBook<'_>> {
        if self.phase != DirectSyncPhase::Healthy {
            return None;
        }
        if self.validate_current_authority().is_err() {
            self.quarantine();
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
    pub fn last_sequence(&self) -> Option<SequenceNumber> {
        if self.phase == DirectSyncPhase::Healthy && self.validate_current_authority().is_ok() {
            self.last_sequence
        } else {
            None
        }
    }

    /// Returns the venue timestamp associated with the healthy cursor.
    pub fn source_timestamp(&self) -> Option<Timestamp> {
        if self.phase == DirectSyncPhase::Healthy && self.validate_current_authority().is_ok() {
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
            DirectSyncPhase::Healthy if self.validate_current_authority().is_ok() => {
                self.snapshot_receipt.as_ref()
            }
            DirectSyncPhase::Healthy => None,
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
        let currentness = event.evidence().currentness_lease();
        if observed.connection_generation() != self.generation {
            return Err(DirectOrderBookError::EventGenerationMismatch);
        }
        currentness
            .validate_current()
            .map_err(|_error| DirectOrderBookError::AuthorityNotCurrent)?;
        if !currentness.binding().shares_allocation_with(observed) {
            return Err(DirectOrderBookError::EventSessionMismatch);
        }
        match self.binding.as_ref() {
            Some(expected) if !expected.shares_allocation_with(observed) => {
                Err(DirectOrderBookError::EventSessionMismatch)
            }
            Some(_) => {
                if !self
                    .currentness
                    .as_ref()
                    .is_some_and(|expected| expected.shares_authority_with(currentness))
                {
                    return Err(DirectOrderBookError::EventSessionMismatch);
                }
                Ok(())
            }
            None => {
                self.binding = Some(observed.clone());
                self.currentness = Some(currentness.clone());
                Ok(())
            }
        }
    }

    fn bind_snapshot_authority(
        &mut self,
        observed: &FrameSessionBinding,
        currentness: &FrameSessionLease,
    ) -> Result<(), DirectOrderBookError> {
        if observed.connection_generation() != self.generation {
            return Err(DirectOrderBookError::SnapshotGenerationMismatch);
        }
        currentness
            .validate_current()
            .map_err(|_error| DirectOrderBookError::AuthorityNotCurrent)?;
        if !currentness.binding().shares_allocation_with(observed) {
            return Err(DirectOrderBookError::SnapshotSessionMismatch);
        }
        match self.binding.as_ref() {
            Some(expected) if !expected.shares_allocation_with(observed) => {
                Err(DirectOrderBookError::SnapshotSessionMismatch)
            }
            Some(_) => {
                if !self
                    .currentness
                    .as_ref()
                    .is_some_and(|expected| expected.shares_authority_with(currentness))
                {
                    return Err(DirectOrderBookError::SnapshotSessionMismatch);
                }
                Ok(())
            }
            None => {
                self.binding = Some(observed.clone());
                self.currentness = Some(currentness.clone());
                Ok(())
            }
        }
    }

    fn validate_current_authority(&self) -> Result<(), DirectOrderBookError> {
        self.currentness
            .as_ref()
            .ok_or(DirectOrderBookError::AuthorityNotCurrent)?
            .validate_current()
            .map_err(|_error| DirectOrderBookError::AuthorityNotCurrent)
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
        self.currentness = None;
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
    /// The process-local source session authority rolled over or was revoked.
    #[error("direct order-book source authority is no longer current")]
    AuthorityNotCurrent,
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
    /// A match's maker side disagreed with the maintained maker order.
    #[error("direct match maker side does not match maintained state")]
    MatchSideMismatch,
    /// A match's price disagreed with the maintained maker order.
    #[error("direct match maker price does not match maintained state")]
    MatchPriceMismatch,
    /// A potentially book-removing done omitted or contradicted required maintained-order evidence.
    #[error("direct done message is structurally invalid")]
    InvalidDone,
    /// A change omitted the old-price evidence required for a price replacement.
    #[error("direct order change is structurally invalid")]
    InvalidChange,
    /// A change's previous price disagreed with the maintained order.
    #[error("direct order change previous price does not match maintained state")]
    ChangePriceMismatch,
    /// A change's side disagreed with the maintained order.
    #[error("direct order change side does not match maintained state")]
    ChangeSideMismatch,
    /// A change's previous quantity disagreed with the maintained order.
    #[error("direct order change previous quantity does not match maintained state")]
    ChangeQuantityMismatch,
    /// Self-trade prevention attempted to increase remaining size.
    #[error("direct self-trade-prevention change increased remaining quantity")]
    StpQuantityIncrease,
    /// The best bid meets or exceeds the best ask in a continuous book.
    #[error("direct continuous order book is crossed")]
    CrossedBook,
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

    use crate::{
        ProviderBookSide, ProviderOrderChangeReason, ProviderOrderEventKind, ProviderOrderRecord,
    };
    use market_squawk_domain::{
        ConnectionGeneration, Currency, Denomination, InstrumentDefinitionRevision,
        InstrumentExecutionTerms, InstrumentId, LotSize, PriceTicks, ProviderProduct, QuantityLots,
        SequenceNumber, SourceIdentifier, TickSize, Timestamp,
    };
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
            reason: ProviderOrderChangeReason::ModifyOrder,
            side: ProviderBookSide::Bid,
            previous_price: Some(PriceTicks::new(10_000)),
            previous_quantity: Some(QuantityLots::new(200)?),
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
                reason: ProviderOrderChangeReason::ModifyOrder,
                side: ProviderBookSide::Bid,
                previous_price: Some(PriceTicks::new(10_100)),
                previous_quantity: Some(QuantityLots::new(300)?),
                new_price: Some(PriceTicks::new(9_600)),
                new_quantity: Some(QuantityLots::new(300)?),
            }),
            Err(DirectOrderBookError::ChangePriceMismatch)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Match {
                maker_order_id: id("bid-b")?,
                maker_side: ProviderBookSide::Ask,
                maker_price: PriceTicks::new(9_900),
                quantity: QuantityLots::new(100)?,
            }),
            Err(DirectOrderBookError::MatchSideMismatch)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Match {
                maker_order_id: id("bid-b")?,
                maker_side: ProviderBookSide::Bid,
                maker_price: PriceTicks::new(9_800),
                quantity: QuantityLots::new(100)?,
            }),
            Err(DirectOrderBookError::MatchPriceMismatch)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Change {
                order_id: id("bid-b")?,
                reason: ProviderOrderChangeReason::SelfTradePrevention,
                side: ProviderBookSide::Bid,
                previous_price: Some(PriceTicks::new(9_900)),
                previous_quantity: Some(QuantityLots::new(200)?),
                new_price: None,
                new_quantity: Some(QuantityLots::new(100)?),
            }),
            Err(DirectOrderBookError::ChangeQuantityMismatch)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Change {
                order_id: id("bid-b")?,
                reason: ProviderOrderChangeReason::SelfTradePrevention,
                side: ProviderBookSide::Ask,
                previous_price: Some(PriceTicks::new(9_900)),
                previous_quantity: Some(QuantityLots::new(300)?),
                new_price: None,
                new_quantity: Some(QuantityLots::new(200)?),
            }),
            Err(DirectOrderBookError::ChangeSideMismatch)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Change {
                order_id: id("bid-b")?,
                reason: ProviderOrderChangeReason::SelfTradePrevention,
                side: ProviderBookSide::Bid,
                previous_price: Some(PriceTicks::new(9_900)),
                previous_quantity: Some(QuantityLots::new(300)?),
                new_price: None,
                new_quantity: Some(QuantityLots::new(400)?),
            }),
            Err(DirectOrderBookError::StpQuantityIncrease)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Done {
                order_id: id("bid-b")?,
                side: Some(ProviderBookSide::Ask),
                price: Some(PriceTicks::new(9_900)),
                remaining_quantity: Some(QuantityLots::new(300)?),
            }),
            Err(DirectOrderBookError::InvalidDone)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Done {
                order_id: id("bid-b")?,
                side: Some(ProviderBookSide::Bid),
                price: Some(PriceTicks::new(9_800)),
                remaining_quantity: Some(QuantityLots::new(300)?),
            }),
            Err(DirectOrderBookError::InvalidDone)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Done {
                order_id: id("bid-b")?,
                side: Some(ProviderBookSide::Bid),
                price: Some(PriceTicks::new(9_900)),
                remaining_quantity: Some(QuantityLots::new(200)?),
            }),
            Err(DirectOrderBookError::InvalidDone)
        );
        assert_eq!(
            state.apply(&ProviderOrderEventKind::Done {
                order_id: id("bid-b")?,
                side: Some(ProviderBookSide::Bid),
                price: None,
                remaining_quantity: None,
            }),
            Err(DirectOrderBookError::InvalidDone)
        );
        state.apply(&ProviderOrderEventKind::Change {
            order_id: id("received-only")?,
            reason: ProviderOrderChangeReason::SelfTradePrevention,
            side: ProviderBookSide::Bid,
            previous_price: None,
            previous_quantity: None,
            new_price: None,
            new_quantity: None,
        })?;
        state.apply(&ProviderOrderEventKind::Done {
            order_id: id("received-only")?,
            side: None,
            price: None,
            remaining_quantity: None,
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
        state.apply(&ProviderOrderEventKind::Done {
            order_id: id("bid-a")?,
            side: Some(ProviderBookSide::Bid),
            price: Some(PriceTicks::new(9_800)),
            remaining_quantity: Some(QuantityLots::new(200)?),
        })?;
        state.insert(&order("ask-cross", ProviderBookSide::Ask, "98.00", "1.00")?)?;
        assert_eq!(
            state.validate_uncrossed(),
            Err(DirectOrderBookError::CrossedBook)
        );

        let mut rotated = OrderBookState::try_new(DirectBookLimits::try_new(8, 8, 2, 1_024, 8)?)?;
        for (order_id, price) in [
            ("rot-a", "95.00"),
            ("rot-b", "96.00"),
            ("rot-c", "97.00"),
            ("rot-d", "98.00"),
        ] {
            rotated.insert(&order(order_id, ProviderBookSide::Bid, price, "1.00")?)?;
        }
        assert_eq!(
            rotated
                .published(8)?
                .bids()
                .map(|level| level.price().get())
                .collect::<Vec<_>>(),
            vec![9_800, 9_700, 9_600, 9_500]
        );
        rotated.apply(&ProviderOrderEventKind::Done {
            order_id: id("rot-b")?,
            side: Some(ProviderBookSide::Bid),
            price: Some(PriceTicks::new(9_600)),
            remaining_quantity: Some(QuantityLots::new(100)?),
        })?;
        assert_eq!(
            rotated
                .published(8)?
                .bids()
                .map(|level| level.price().get())
                .collect::<Vec<_>>(),
            vec![9_800, 9_700, 9_500]
        );
        Ok(())
    }
}
