//! Preallocated provider-lexeme-preserving transactional book state.

#![allow(
    dead_code,
    reason = "the actor runtime is the sole production caller of this crate-private book"
)]

use std::cmp::Ordering;
use std::sync::Arc;

use market_squawk_domain::{BookLevel, LotSize, PriceTicks, QuantityLots, TickSize};
use market_squawk_sources::{
    ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderChecksumEvidence,
};
use thiserror::Error;

use crate::{
    BookError, BookSide, ChecksumValidationError, DepthLimit, ResolvedChecksumValidator,
    normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};

#[path = "provider_book/storage.rs"]
mod storage;

pub(crate) use storage::BookProcessingScratch;
use storage::{ExactProviderLevel, FixedBuffer, NormalizedChange, SideBuffers, UnifiedBookLevel};

/// Unified scaled book plus immutable exact provider lexemes required by venue checksum rules.
#[derive(Debug)]
pub(crate) struct ProviderBook {
    depth: DepthLimit,
    bids: SideBuffers,
    asks: SideBuffers,
}

impl ProviderBook {
    pub(crate) fn try_new(depth: DepthLimit) -> Result<Self, ProviderBookError> {
        Ok(Self {
            depth,
            bids: SideBuffers::try_new(depth.get())?,
            asks: SideBuffers::try_new(depth.get())?,
        })
    }

    pub(crate) const fn scaled_depth(&self) -> DepthLimit {
        self.depth
    }

    pub(crate) fn replace_snapshot(
        &mut self,
        bids: &[ProviderBookLevel],
        asks: &[ProviderBookLevel],
        tick_size: TickSize,
        lot_size: LotSize,
        checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
        scratch: &mut BookProcessingScratch,
    ) -> Result<Option<u32>, ProviderBookError> {
        let transaction =
            self.begin_snapshot(bids, asks, tick_size, lot_size, checksum, scratch)?;
        let computed = transaction.computed_checksum();
        transaction.commit();
        Ok(computed)
    }

    pub(crate) fn begin_snapshot<'a>(
        &'a mut self,
        bids: &[ProviderBookLevel],
        asks: &[ProviderBookLevel],
        tick_size: TickSize,
        lot_size: LotSize,
        checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
        scratch: &'a mut BookProcessingScratch,
    ) -> Result<ProviderBookTransaction<'a>, ProviderBookError> {
        let observed = bids
            .len()
            .checked_add(asks.len())
            .ok_or(ProviderBookError::Allocation)?;
        ensure_message_and_scratch_bounds(observed, scratch)?;
        scratch.clear();
        normalize_snapshot_side(
            &mut scratch.changes,
            bids,
            BookSide::Bid,
            tick_size,
            lot_size,
            0,
        )?;
        normalize_snapshot_side(
            &mut scratch.changes,
            asks,
            BookSide::Ask,
            tick_size,
            lot_size,
            bids.len(),
        )?;
        self.bids.clear_candidate();
        self.asks.clear_candidate();
        let result = (|| {
            for change in scratch
                .changes
                .iter()
                .filter(|change| change.side == BookSide::Bid)
                .take(self.depth.get())
            {
                self.bids.push_candidate(unified_from_change(change)?)?;
            }
            for change in scratch
                .changes
                .iter()
                .filter(|change| change.side == BookSide::Ask)
                .take(self.depth.get())
            {
                self.asks.push_candidate(unified_from_change(change)?)?;
            }
            validate_uncrossed(&self.bids.candidate, &self.asks.candidate)?;
            validate_checksum(
                self.asks
                    .candidate
                    .iter()
                    .map(|level| Arc::as_ref(&level.exact)),
                self.bids
                    .candidate
                    .iter()
                    .map(|level| Arc::as_ref(&level.exact)),
                checksum,
            )
        })();
        let computed = match result {
            Ok(value) => value,
            Err(error) => {
                self.bids.clear_candidate();
                self.asks.clear_candidate();
                scratch.clear();
                return Err(error);
            }
        };
        Ok(ProviderBookTransaction {
            book: self,
            scratch,
            computed,
            committed: false,
        })
    }

    pub(crate) fn begin_delta<'a>(
        &'a mut self,
        changes: &[ProviderBookChange],
        tick_size: TickSize,
        lot_size: LotSize,
        checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
        scratch: &'a mut BookProcessingScratch,
    ) -> Result<ProviderBookTransaction<'a>, ProviderBookError> {
        if changes.is_empty() {
            return Err(BookError::EmptyDelta.into());
        }
        ensure_message_and_scratch_bounds(changes.len(), scratch)?;
        scratch.clear();
        for (ordinal, change) in changes.iter().enumerate() {
            scratch.push(normalize_change(change, tick_size, lot_size, ordinal)?)?;
        }
        scratch.changes.sort_initialized_by(compare_normalized);
        self.bids.clear_candidate();
        self.asks.clear_candidate();
        let result = (|| {
            merge_side(
                BookSide::Bid,
                &self.bids.active,
                &mut self.bids.candidate,
                &scratch.changes,
            )?;
            merge_side(
                BookSide::Ask,
                &self.asks.active,
                &mut self.asks.candidate,
                &scratch.changes,
            )?;
            validate_uncrossed(&self.bids.candidate, &self.asks.candidate)?;
            validate_checksum(
                self.asks
                    .candidate
                    .iter()
                    .map(|level| Arc::as_ref(&level.exact)),
                self.bids
                    .candidate
                    .iter()
                    .map(|level| Arc::as_ref(&level.exact)),
                checksum,
            )
        })();
        let computed = match result {
            Ok(value) => value,
            Err(error) => {
                self.bids.clear_candidate();
                self.asks.clear_candidate();
                scratch.clear();
                return Err(error);
            }
        };
        scratch
            .changes
            .sort_initialized_by(|left, right| left.ordinal.cmp(&right.ordinal));
        Ok(ProviderBookTransaction {
            book: self,
            scratch,
            computed,
            committed: false,
        })
    }

    pub(crate) fn bid_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        canonical_levels(&self.bids.active)
    }

    pub(crate) fn ask_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        canonical_levels(&self.asks.active)
    }

    pub(crate) fn bid_level_count(&self) -> usize {
        self.bids.active.len()
    }

    pub(crate) fn ask_level_count(&self) -> usize {
        self.asks.active.len()
    }

    pub(crate) fn scaled_bid_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.bids
            .active
            .iter()
            .map(|level| (level.price, level.quantity))
    }

    pub(crate) fn scaled_ask_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.asks
            .active
            .iter()
            .map(|level| (level.price, level.quantity))
    }
}

/// Borrowed candidate view kept private to the reversible transaction.
#[derive(Debug)]
pub(crate) struct ProviderBookCandidate<'a> {
    book: &'a ProviderBook,
}

impl ProviderBookCandidate<'_> {
    pub(crate) fn bid_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        canonical_levels(&self.book.bids.candidate)
    }

    pub(crate) fn ask_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        canonical_levels(&self.book.asks.candidate)
    }

    #[cfg(test)]
    fn level_count(&self) -> usize {
        self.book.bids.candidate.len() + self.book.asks.candidate.len()
    }

    pub(crate) fn scaled_bid_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.book
            .bids
            .candidate
            .iter()
            .map(|level| (level.price, level.quantity))
    }

    pub(crate) fn scaled_ask_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.book
            .asks
            .candidate
            .iter()
            .map(|level| (level.price, level.quantity))
    }
}

/// Fail-safe mutable provider-book candidate. Drop clears the inactive buffers; the committed
/// image is never mutated before explicit commit.
#[derive(Debug)]
pub(crate) struct ProviderBookTransaction<'a> {
    book: &'a mut ProviderBook,
    scratch: &'a mut BookProcessingScratch,
    computed: Option<u32>,
    committed: bool,
}

impl ProviderBookTransaction<'_> {
    pub(crate) fn candidate(&self) -> ProviderBookCandidate<'_> {
        ProviderBookCandidate { book: self.book }
    }

    pub(crate) const fn computed_checksum(&self) -> Option<u32> {
        self.computed
    }

    pub(crate) fn normalized_changes(
        &self,
    ) -> Result<Vec<market_squawk_domain::BookChange>, ProviderBookError> {
        checked_canonical_changes(&self.scratch.changes)
    }

    #[cfg(test)]
    fn observed_scratch_backing_bytes(&self) -> u64 {
        self.scratch.observed_backing_bytes()
    }

    /// Atomically swaps both candidate sides into the active image.
    pub(crate) fn commit(mut self) {
        self.book.bids.commit();
        self.book.asks.commit();
        self.scratch.clear();
        self.committed = true;
    }
}

impl Drop for ProviderBookTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.book.bids.clear_candidate();
            self.book.asks.clear_candidate();
            self.scratch.clear();
        }
    }
}

fn normalize_snapshot_side(
    output: &mut FixedBuffer<NormalizedChange>,
    levels: &[ProviderBookLevel],
    side: BookSide,
    tick_size: TickSize,
    lot_size: LotSize,
    ordinal_offset: usize,
) -> Result<(), ProviderBookError> {
    let mut previous = None;
    for (offset, level) in levels.iter().enumerate() {
        let price = normalize_price(level.price(), tick_size)?;
        let quantity = normalize_positive_quantity(level.quantity(), lot_size)?;
        if previous.is_some_and(|prior| match side {
            BookSide::Bid => prior <= price,
            BookSide::Ask => prior >= price,
        }) {
            return Err(if previous == Some(price) {
                BookError::DuplicatePrice { side, price }.into()
            } else {
                BookError::InvalidOrdering { side }.into()
            });
        }
        previous = Some(price);
        output.push(NormalizedChange {
            side,
            price,
            quantity,
            exact: ExactProviderLevel::try_from_provider(level)?,
            ordinal: ordinal_offset
                .checked_add(offset)
                .ok_or(ProviderBookError::Allocation)?,
        })?;
    }
    Ok(())
}

fn normalize_change(
    change: &ProviderBookChange,
    tick_size: TickSize,
    lot_size: LotSize,
    ordinal: usize,
) -> Result<NormalizedChange, ProviderBookError> {
    let side = match change.side() {
        ProviderBookSide::Bid => BookSide::Bid,
        ProviderBookSide::Ask => BookSide::Ask,
    };
    Ok(NormalizedChange {
        side,
        price: normalize_price(change.level().price(), tick_size)?,
        quantity: normalize_delta_quantity(change.level().quantity(), lot_size)?,
        exact: ExactProviderLevel::try_from_provider(change.level())?,
        ordinal,
    })
}

fn compare_normalized(left: &NormalizedChange, right: &NormalizedChange) -> Ordering {
    side_tag(left.side)
        .cmp(&side_tag(right.side))
        .then_with(|| compare_price(left.side, left.price, right.price))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

const fn side_tag(side: BookSide) -> u8 {
    match side {
        BookSide::Bid => 0,
        BookSide::Ask => 1,
    }
}

fn compare_price(side: BookSide, left: PriceTicks, right: PriceTicks) -> Ordering {
    match side {
        BookSide::Bid => right.cmp(&left),
        BookSide::Ask => left.cmp(&right),
    }
}

fn merge_side(
    side: BookSide,
    active: &FixedBuffer<UnifiedBookLevel>,
    candidate: &mut FixedBuffer<UnifiedBookLevel>,
    changes: &FixedBuffer<NormalizedChange>,
) -> Result<(), ProviderBookError> {
    candidate.clear();
    let mut side_start = 0;
    while changes
        .get(side_start)
        .is_some_and(|change| side_tag(change.side) < side_tag(side))
    {
        side_start += 1;
    }
    let mut side_end = side_start;
    while changes
        .get(side_end)
        .is_some_and(|change| change.side == side)
    {
        side_end += 1;
    }
    let mut existing_index = 0;
    let mut change_index = side_start;
    while candidate.len() < candidate.capacity()
        && (existing_index < active.len() || change_index < side_end)
    {
        let next_change = winner_at(changes, change_index, side_end);
        match (active.get(existing_index), next_change) {
            (Some(existing), Some((winner, next_index))) => {
                match compare_price(side, existing.price, winner.price) {
                    Ordering::Less => {
                        candidate.push(existing.clone())?;
                        existing_index += 1;
                    }
                    Ordering::Equal => {
                        if winner.quantity.get() != 0 {
                            candidate.push(unified_from_change(winner)?)?;
                        }
                        existing_index += 1;
                        change_index = next_index;
                    }
                    Ordering::Greater => {
                        if winner.quantity.get() != 0 {
                            candidate.push(unified_from_change(winner)?)?;
                        }
                        change_index = next_index;
                    }
                }
            }
            (Some(existing), None) => {
                candidate.push(existing.clone())?;
                existing_index += 1;
            }
            (None, Some((winner, next_index))) => {
                if winner.quantity.get() != 0 {
                    candidate.push(unified_from_change(winner)?)?;
                }
                change_index = next_index;
            }
            (None, None) => break,
        }
    }
    Ok(())
}

fn winner_at(
    changes: &FixedBuffer<NormalizedChange>,
    start: usize,
    end_limit: usize,
) -> Option<(&NormalizedChange, usize)> {
    if start >= end_limit {
        return None;
    }
    let first = changes.get(start)?;
    let mut end = start + 1;
    while end < end_limit
        && changes
            .get(end)
            .is_some_and(|change| change.price == first.price)
    {
        end += 1;
    }
    changes.get(end - 1).map(|winner| (winner, end))
}

fn unified_from_change(change: &NormalizedChange) -> Result<UnifiedBookLevel, ProviderBookError> {
    Ok(UnifiedBookLevel {
        price: change.price,
        quantity: change.quantity,
        exact: Arc::new(change.exact.clone()),
    })
}

fn validate_uncrossed(
    bids: &FixedBuffer<UnifiedBookLevel>,
    asks: &FixedBuffer<UnifiedBookLevel>,
) -> Result<(), ProviderBookError> {
    if bids
        .get(0)
        .zip(asks.get(0))
        .is_some_and(|(bid, ask)| bid.price >= ask.price)
    {
        Err(BookError::Crossed.into())
    } else {
        Ok(())
    }
}

fn canonical_levels(
    levels: &FixedBuffer<UnifiedBookLevel>,
) -> Result<Vec<BookLevel>, ProviderBookError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(levels.len())
        .map_err(|_| ProviderBookError::Allocation)?;
    for level in levels.iter() {
        output.push(BookLevel::new(level.price, level.quantity)?);
    }
    // `try_reserve_exact` may legally expose spare logical capacity. The box round trip
    // canonicalizes the published Vec to `len == capacity` without treating allocator policy as
    // a market-data failure. The memory model charges the possible old/new allocation overlap.
    Ok(output.into_boxed_slice().into_vec())
}

fn checked_canonical_changes(
    changes: &FixedBuffer<NormalizedChange>,
) -> Result<Vec<market_squawk_domain::BookChange>, ProviderBookError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(changes.len())
        .map_err(|_| ProviderBookError::Allocation)?;
    for change in changes.iter() {
        output.push(market_squawk_domain::BookChange::new(
            match change.side {
                BookSide::Bid => market_squawk_domain::MarketSide::Bid,
                BookSide::Ask => market_squawk_domain::MarketSide::Ask,
            },
            change.price,
            change.quantity,
        ));
    }
    Ok(output.into_boxed_slice().into_vec())
}

fn validate_checksum<'a, A, B>(
    asks: A,
    bids: B,
    checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
) -> Result<Option<u32>, ProviderBookError>
where
    A: IntoIterator<Item = &'a ExactProviderLevel>,
    B: IntoIterator<Item = &'a ExactProviderLevel>,
{
    let Some((validator, evidence)) = checksum else {
        return Ok(None);
    };
    validator
        .validate_exact_ordered(asks, bids, evidence)
        .map(Some)
        .map_err(ProviderBookError::from)
}

fn ensure_message_and_scratch_bounds(
    observed: usize,
    scratch: &BookProcessingScratch,
) -> Result<(), ProviderBookError> {
    if observed > crate::MAX_BOOK_MESSAGE_ITEMS {
        return Err(BookError::MessageTooLarge {
            observed,
            maximum: crate::MAX_BOOK_MESSAGE_ITEMS,
        }
        .into());
    }
    if observed > scratch.maximum_items() {
        return Err(ProviderBookError::ScratchCapacityExceeded {
            observed,
            maximum: scratch.maximum_items(),
        });
    }
    Ok(())
}

/// Provider book conversion, mutation, or exact-integrity failure.
#[derive(Debug, Error)]
pub(crate) enum ProviderBookError {
    #[error(transparent)]
    Normalization(#[from] crate::NormalizationError),
    #[error(transparent)]
    Book(#[from] BookError),
    #[error(transparent)]
    Checksum(#[from] ChecksumValidationError),
    #[error("provider book bounded allocation failed")]
    Allocation,
    #[error("book scratch capacity {requested} is invalid; maximum is {maximum}")]
    InvalidScratchCapacity { requested: usize, maximum: usize },
    #[error("book message contains {observed} items; shard scratch maximum is {maximum}")]
    ScratchCapacityExceeded { observed: usize, maximum: usize },
    #[error(transparent)]
    Market(#[from] market_squawk_domain::MarketEventError),
}

/// Returns the fixed backing bytes owned by one provider book, excluding exact `Arc` pointees.
pub(crate) fn provider_book_buffer_bytes(depth: usize) -> Option<u64> {
    let slots = (depth as u64).checked_mul(4)?;
    slots.checked_mul(std::mem::size_of::<Option<UnifiedBookLevel>>() as u64)
}

/// Returns the complete immutable exact-level `Arc` allocation including strong/weak counters and
/// worst-case alignment padding before the fixed-size pointee.
pub(crate) const fn exact_level_arc_allocation_bytes() -> u64 {
    let counters = 2 * std::mem::size_of::<usize>();
    let padding = std::mem::align_of::<ExactProviderLevel>() - 1;
    (counters + padding + std::mem::size_of::<ExactProviderLevel>()) as u64
}

/// Returns the exact preallocated shard scratch backing for a configured item bound.
pub(crate) fn shard_book_scratch_bytes(maximum_items: usize) -> Option<u64> {
    (maximum_items as u64).checked_mul(std::mem::size_of::<Option<NormalizedChange>>() as u64)
}

/// Returns the minimum inline provider item retained by source admission. Dividing the exact
/// command ceiling by this value is a conservative upper bound on book items in one admitted
/// command; command and observation overhead only reduce the feasible count.
pub(crate) const fn minimum_admitted_provider_book_item_bytes() -> usize {
    let level = std::mem::size_of::<ProviderBookLevel>();
    let change = std::mem::size_of::<ProviderBookChange>();
    if level < change { level } else { change }
}

/// Derives a conservative nonzero scratch item bound from the command retained-byte ceiling and
/// the decoder hard cap. Inline command/observation/evidence bytes are deliberately not subtracted.
pub(crate) fn maximum_book_items_for_message(maximum_message_bytes: u32) -> usize {
    let minimum = minimum_admitted_provider_book_item_bytes().max(1);
    usize::try_from(maximum_message_bytes)
        .unwrap_or(usize::MAX)
        .checked_div(minimum)
        .unwrap_or(0)
        .clamp(1, crate::MAX_BOOK_MESSAGE_ITEMS)
}

#[cfg(test)]
#[path = "provider_book/tests.rs"]
mod tests;
