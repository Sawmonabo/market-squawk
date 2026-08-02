//! Pure scaled-integer price-level book invariants.

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
    /// Ascending price order. This reusable mathematical container is not the production provider
    /// book; the latter owns preallocated active/inactive buffers in `provider_book`.
    bids: Vec<(PriceTicks, QuantityLots)>,
    /// Ascending price order.
    asks: Vec<(PriceTicks, QuantityLots)>,
}

impl ScaledBook {
    /// Creates an empty bounded book.
    pub const fn new(depth: DepthLimit) -> Self {
        Self {
            depth,
            bids: Vec::new(),
            asks: Vec::new(),
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

        let mut candidate_bids = Vec::new();
        candidate_bids
            .try_reserve_exact(self.depth.get().min(bids.len()))
            .map_err(|_| BookError::Allocation)?;
        candidate_bids.extend(
            bids.iter()
                .take(self.depth.get())
                .rev()
                .map(|update| (update.price, update.quantity)),
        );
        let mut candidate_asks = Vec::new();
        candidate_asks
            .try_reserve_exact(self.depth.get().min(asks.len()))
            .map_err(|_| BookError::Allocation)?;
        candidate_asks.extend(
            asks.iter()
                .take(self.depth.get())
                .map(|update| (update.price, update.quantity)),
        );
        validate_uncrossed(&candidate_bids, &candidate_asks)?;
        self.bids = candidate_bids;
        self.asks = candidate_asks;
        Ok(())
    }

    /// Applies one complete scaled delta using an unpublished candidate image.
    ///
    /// All supplied changes and depth evictions either commit together or are restored in reverse
    /// order. Zero deletes a level; positive quantity inserts or replaces it.
    ///
    /// # Errors
    ///
    /// Returns a typed invariant or allocation error without exposing a partial candidate.
    pub fn apply_delta(&mut self, changes: &[LevelUpdate]) -> Result<(), BookError> {
        ensure_message_bound(changes.len())?;
        if changes.is_empty() {
            return Err(BookError::EmptyDelta);
        }
        let mut candidate_bids = self.bids.clone();
        let mut candidate_asks = self.asks.clone();
        candidate_bids
            .try_reserve_exact(changes.len())
            .map_err(|_| BookError::Allocation)?;
        candidate_asks
            .try_reserve_exact(changes.len())
            .map_err(|_| BookError::Allocation)?;

        for update in changes {
            let side = match update.side {
                BookSide::Bid => &mut candidate_bids,
                BookSide::Ask => &mut candidate_asks,
            };
            apply_sorted_update(side, update.price, update.quantity);
        }
        truncate_sorted(&mut candidate_bids, BookSide::Bid, self.depth);
        truncate_sorted(&mut candidate_asks, BookSide::Ask, self.depth);

        validate_uncrossed(&candidate_bids, &candidate_asks)?;
        self.bids = candidate_bids;
        self.asks = candidate_asks;
        Ok(())
    }

    /// Returns the configured per-side depth.
    pub const fn depth_limit(&self) -> DepthLimit {
        self.depth
    }

    /// Returns the current best bid.
    pub fn best_bid(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.bids.last().copied()
    }

    /// Returns the current best ask.
    pub fn best_ask(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.asks.first().copied()
    }

    /// Returns a bounded best-to-worst bid snapshot.
    pub fn bid_levels(&self) -> Vec<(PriceTicks, QuantityLots)> {
        self.bids.iter().rev().copied().collect()
    }

    /// Returns a bounded best-to-worst ask snapshot.
    pub fn ask_levels(&self) -> Vec<(PriceTicks, QuantityLots)> {
        self.asks.clone()
    }
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

fn apply_sorted_update(
    levels: &mut Vec<(PriceTicks, QuantityLots)>,
    price: PriceTicks,
    quantity: QuantityLots,
) {
    match levels.binary_search_by_key(&price, |(candidate, _)| *candidate) {
        Ok(index) if quantity.get() == 0 => {
            levels.remove(index);
        }
        Ok(index) => levels[index].1 = quantity,
        Err(_) if quantity.get() == 0 => {}
        Err(index) => levels.insert(index, (price, quantity)),
    }
}

fn truncate_sorted(
    levels: &mut Vec<(PriceTicks, QuantityLots)>,
    side: BookSide,
    depth: DepthLimit,
) {
    if levels.len() <= depth.get() {
        return;
    }
    match side {
        BookSide::Bid => {
            let remove = levels.len() - depth.get();
            levels.drain(..remove);
        }
        BookSide::Ask => levels.truncate(depth.get()),
    }
}

fn validate_uncrossed(
    bids: &[(PriceTicks, QuantityLots)],
    asks: &[(PriceTicks, QuantityLots)],
) -> Result<(), BookError> {
    if bids
        .last()
        .zip(asks.first())
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
    /// A bounded unpublished candidate allocation failed.
    #[error("bounded book allocation failed")]
    Allocation,
}
