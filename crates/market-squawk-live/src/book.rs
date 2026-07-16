//! Pure scaled-integer price-level book invariants.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use market_squawk_domain::{PriceTicks, QuantityLots};
use market_squawk_sources::MAX_DECODED_BOOK_ITEMS;
use thiserror::Error;

/// Maximum retained price levels on either side of one book.
const MAX_BOOK_DEPTH: usize = 10_000;

/// Maximum number of decoded level entries accepted by one snapshot or delta message.
///
/// This is deliberately aligned with the bounded source decoder. Public callers cannot bypass
/// decoder admission and make the live book scan or allocate for a larger message.
pub const MAX_BOOK_MESSAGE_ITEMS: usize = MAX_DECODED_BOOK_ITEMS;

/// Checked per-side depth retained by one price-level book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DepthLimit(NonZeroUsize);

impl DepthLimit {
    /// Constructs a nonzero bounded depth.
    ///
    /// # Errors
    ///
    /// Rejects zero and depths greater than 10,000 levels per side.
    pub fn new(value: usize) -> Result<Self, BookError> {
        let value = NonZeroUsize::new(value).ok_or(BookError::InvalidDepth {
            requested: value,
            maximum: MAX_BOOK_DEPTH,
        })?;
        if value.get() > MAX_BOOK_DEPTH {
            return Err(BookError::InvalidDepth {
                requested: value.get(),
                maximum: MAX_BOOK_DEPTH,
            });
        }
        Ok(Self(value))
    }

    /// Returns the maximum number of retained levels per side.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Side of one order-book price level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BookSide {
    /// Bid side, ordered from the highest price down.
    Bid,
    /// Ask side, ordered from the lowest price up.
    Ask,
}

/// One already normalized price-level mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LevelUpdate {
    side: BookSide,
    price: PriceTicks,
    quantity: QuantityLots,
}

impl LevelUpdate {
    /// Constructs one scaled mutation. A zero quantity means deletion only when applying a delta.
    pub const fn new(side: BookSide, price: PriceTicks, quantity: QuantityLots) -> Self {
        Self {
            side,
            price,
            quantity,
        }
    }

    /// Returns the side being mutated.
    pub const fn side(self) -> BookSide {
        self.side
    }

    /// Returns the scaled price.
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the scaled quantity. Zero represents a delta deletion.
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// A pure bounded price-level book.
///
/// Mutations are message-atomic. A rejected snapshot or delta leaves the previous committed book
/// byte-for-byte unchanged. Stream quarantine and authority revocation are owned by the stateful
/// live processor rather than this reusable mathematical container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaledBook {
    depth: DepthLimit,
    bids: BTreeMap<PriceTicks, QuantityLots>,
    asks: BTreeMap<PriceTicks, QuantityLots>,
}

impl ScaledBook {
    /// Creates an empty bounded book.
    pub const fn new(depth: DepthLimit) -> Self {
        Self {
            depth,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Atomically replaces the complete book image.
    ///
    /// Inputs must be strict best-to-worst order, carry the expected side, contain no duplicate
    /// prices, and contain only positive quantities. Levels beyond configured depth are validated
    /// before being deterministically truncated.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant or allocation error without changing the committed book.
    pub fn replace_snapshot(
        &mut self,
        bids: &[LevelUpdate],
        asks: &[LevelUpdate],
    ) -> Result<(), BookError> {
        let observed = bids
            .len()
            .checked_add(asks.len())
            .ok_or(BookError::MessageTooLarge {
                observed: usize::MAX,
                maximum: MAX_BOOK_MESSAGE_ITEMS,
            })?;
        ensure_message_bound(observed)?;
        validate_snapshot_side(bids, BookSide::Bid)?;
        validate_snapshot_side(asks, BookSide::Ask)?;

        let mut candidate_bids = BTreeMap::new();
        let mut candidate_asks = BTreeMap::new();

        for update in bids.iter().take(self.depth.get()) {
            if candidate_bids
                .insert(update.price, update.quantity)
                .is_some()
            {
                return Err(BookError::DuplicatePrice {
                    side: BookSide::Bid,
                    price: update.price,
                });
            }
        }
        for update in asks.iter().take(self.depth.get()) {
            if candidate_asks
                .insert(update.price, update.quantity)
                .is_some()
            {
                return Err(BookError::DuplicatePrice {
                    side: BookSide::Ask,
                    price: update.price,
                });
            }
        }
        validate_uncrossed(&candidate_bids, &candidate_asks)?;
        self.bids = candidate_bids;
        self.asks = candidate_asks;
        Ok(())
    }

    /// Applies one complete provider delta using a bounded rollback journal.
    ///
    /// All supplied changes and depth evictions either commit together or are restored in reverse
    /// order. Zero deletes a level; positive quantity inserts or replaces it.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant or allocation error without exposing a partial candidate.
    pub fn apply_delta(&mut self, changes: &[LevelUpdate]) -> Result<(), BookError> {
        let _committed = self.begin_delta_checked(changes, |_| Ok(()))?;
        Ok(())
    }

    /// Applies a delta and runs a caller's candidate-state validation before commit.
    ///
    /// This crate-private hook lets the provider book validate exact-lexeme checksums while the
    /// scaled rollback journal is still live. Any validator failure restores the prior book.
    pub(crate) fn begin_delta_checked<E, F>(
        &mut self,
        changes: &[LevelUpdate],
        validate: F,
    ) -> Result<ScaledBookRollback, E>
    where
        E: From<BookError>,
        F: FnOnce(&Self) -> Result<(), E>,
    {
        ensure_message_bound(changes.len()).map_err(E::from)?;
        if changes.is_empty() {
            return Err(E::from(BookError::EmptyDelta));
        }
        let rollback_capacity = delta_rollback_capacity(changes.len()).map_err(E::from)?;
        let mut rollback = Vec::new();
        rollback
            .try_reserve(rollback_capacity)
            .map_err(|_| E::from(BookError::Allocation))?;

        for update in changes {
            let map = match update.side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
            };
            let previous = map.get(&update.price).copied();
            rollback.push(RollbackEntry {
                side: update.side,
                price: update.price,
                previous,
            });
            if update.quantity.get() == 0 {
                map.remove(&update.price);
            } else {
                map.insert(update.price, update.quantity);
            }
        }
        truncate_with_rollback(&mut self.bids, BookSide::Bid, self.depth, &mut rollback);
        truncate_with_rollback(&mut self.asks, BookSide::Ask, self.depth, &mut rollback);

        if let Err(error) = validate_uncrossed(&self.bids, &self.asks) {
            self.rollback(rollback);
            return Err(E::from(error));
        }
        if let Err(error) = validate(self) {
            self.rollback(rollback);
            return Err(error);
        }
        Ok(ScaledBookRollback { entries: rollback })
    }

    /// Returns the configured per-side depth.
    pub const fn depth_limit(&self) -> DepthLimit {
        self.depth
    }

    /// Returns the current best bid.
    pub fn best_bid(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, quantity)| (*price, *quantity))
    }

    /// Returns the current best ask.
    pub fn best_ask(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.asks
            .iter()
            .next()
            .map(|(price, quantity)| (*price, *quantity))
    }

    /// Returns a bounded best-to-worst bid snapshot.
    pub fn bid_levels(&self) -> Vec<(PriceTicks, QuantityLots)> {
        self.bids
            .iter()
            .rev()
            .map(|(price, quantity)| (*price, *quantity))
            .collect()
    }

    /// Returns a bounded best-to-worst ask snapshot.
    pub fn ask_levels(&self) -> Vec<(PriceTicks, QuantityLots)> {
        self.asks
            .iter()
            .map(|(price, quantity)| (*price, *quantity))
            .collect()
    }

    pub(crate) fn bid_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.bids
            .iter()
            .rev()
            .map(|(price, quantity)| (*price, *quantity))
    }

    pub(crate) fn ask_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.asks
            .iter()
            .map(|(price, quantity)| (*price, *quantity))
    }

    fn rollback(&mut self, rollback: Vec<RollbackEntry>) {
        for entry in rollback.into_iter().rev() {
            let map = match entry.side {
                BookSide::Bid => &mut self.bids,
                BookSide::Ask => &mut self.asks,
            };
            match entry.previous {
                Some(quantity) => {
                    map.insert(entry.price, quantity);
                }
                None => {
                    map.remove(&entry.price);
                }
            }
        }
    }

    pub(crate) fn rollback_delta(&mut self, rollback: ScaledBookRollback) {
        self.rollback(rollback.entries);
    }
}

/// Internal scaled journal owned by the fail-safe provider-book transaction.
#[derive(Debug)]
pub(crate) struct ScaledBookRollback {
    entries: Vec<RollbackEntry>,
}

fn delta_rollback_capacity(message_items: usize) -> Result<usize, BookError> {
    message_items.checked_mul(2).ok_or(BookError::Allocation)
}

fn ensure_message_bound(observed: usize) -> Result<(), BookError> {
    if observed > MAX_BOOK_MESSAGE_ITEMS {
        Err(BookError::MessageTooLarge {
            observed,
            maximum: MAX_BOOK_MESSAGE_ITEMS,
        })
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct RollbackEntry {
    side: BookSide,
    price: PriceTicks,
    previous: Option<QuantityLots>,
}

fn validate_snapshot_side(updates: &[LevelUpdate], side: BookSide) -> Result<(), BookError> {
    let mut previous = None;
    for update in updates {
        if update.side != side {
            return Err(BookError::SideMismatch {
                expected: side,
                found: update.side,
            });
        }
        if update.quantity.get() == 0 {
            return Err(BookError::ZeroSnapshotQuantity);
        }
        if let Some(previous_price) = previous {
            let ordered = match side {
                BookSide::Bid => previous_price > update.price,
                BookSide::Ask => previous_price < update.price,
            };
            if !ordered {
                if previous_price == update.price {
                    return Err(BookError::DuplicatePrice {
                        side,
                        price: update.price,
                    });
                }
                return Err(BookError::InvalidOrdering { side });
            }
        }
        previous = Some(update.price);
    }
    Ok(())
}

fn truncate_with_rollback(
    map: &mut BTreeMap<PriceTicks, QuantityLots>,
    side: BookSide,
    depth: DepthLimit,
    rollback: &mut Vec<RollbackEntry>,
) {
    while map.len() > depth.get() {
        let evicted = match side {
            BookSide::Bid => map.first_key_value(),
            BookSide::Ask => map.last_key_value(),
        }
        .map(|(price, quantity)| (*price, *quantity));
        if let Some((price, quantity)) = evicted {
            rollback.push(RollbackEntry {
                side,
                price,
                previous: Some(quantity),
            });
            map.remove(&price);
        } else {
            break;
        }
    }
}

fn validate_uncrossed(
    bids: &BTreeMap<PriceTicks, QuantityLots>,
    asks: &BTreeMap<PriceTicks, QuantityLots>,
) -> Result<(), BookError> {
    if bids
        .last_key_value()
        .zip(asks.first_key_value())
        .is_some_and(|((bid, _), (ask, _))| bid >= ask)
    {
        Err(BookError::Crossed)
    } else {
        Ok(())
    }
}

/// Order-book construction or mutation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum BookError {
    /// Configured depth is zero or exceeds the hard bound.
    #[error("invalid book depth {requested}; maximum is {maximum}")]
    InvalidDepth { requested: usize, maximum: usize },
    /// One decoded provider message exceeded the live admission ceiling.
    #[error("book message contains {observed} items; maximum is {maximum}")]
    MessageTooLarge { observed: usize, maximum: usize },
    /// Snapshot input carried a change for the wrong side.
    #[error("book side mismatch: expected {expected:?}, found {found:?}")]
    SideMismatch { expected: BookSide, found: BookSide },
    /// Snapshot levels were not in strict best-to-worst order.
    #[error("snapshot {side:?} levels are not in strict best-to-worst order")]
    InvalidOrdering { side: BookSide },
    /// A snapshot repeated one side/price key.
    #[error("duplicate {side:?} price {price:?}")]
    DuplicatePrice { side: BookSide, price: PriceTicks },
    /// Snapshot quantities must be positive.
    #[error("snapshot quantity must be positive")]
    ZeroSnapshotQuantity,
    /// A delta message must contain at least one change.
    #[error("book delta must contain at least one change")]
    EmptyDelta,
    /// Candidate best bid is at or above candidate best ask.
    #[error("candidate order book is crossed")]
    Crossed,
    /// A bounded candidate or rollback allocation failed.
    #[error("bounded book allocation failed")]
    Allocation,
}

#[cfg(test)]
mod tests {
    use super::delta_rollback_capacity;

    #[test]
    fn rollback_capacity_scales_with_message_not_configured_depth() {
        assert_eq!(delta_rollback_capacity(1), Ok(2));
        assert_eq!(delta_rollback_capacity(10), Ok(20));
    }
}
