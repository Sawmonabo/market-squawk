//! Provider-lexeme-preserving transactional book state.

#![allow(
    dead_code,
    reason = "Task 8 actor wiring reaches this crate-private book through InstrumentLiveProcessor"
)]

use std::collections::BTreeMap;

use market_squawk_domain::{BookLevel, LotSize, PriceTicks, QuantityLots, TickSize};
use market_squawk_sources::{
    ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderChecksumEvidence,
};
use thiserror::Error;

use crate::{
    BookError, BookSide, ChecksumValidationError, DepthLimit, LevelUpdate,
    ResolvedChecksumValidator, ScaledBook, normalize_delta_quantity, normalize_positive_quantity,
    normalize_price,
};

/// Scaled book plus exact provider decimal lexemes required by venue checksum rules.
#[derive(Clone, Debug)]
pub(crate) struct ProviderBook {
    scaled: ScaledBook,
    bids: BTreeMap<PriceTicks, ProviderBookLevel>,
    asks: BTreeMap<PriceTicks, ProviderBookLevel>,
}

impl ProviderBook {
    pub(crate) const fn new(depth: DepthLimit) -> Self {
        Self {
            scaled: ScaledBook::new(depth),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub(crate) const fn scaled_depth(&self) -> DepthLimit {
        self.scaled.depth_limit()
    }

    pub(crate) fn replace_snapshot(
        &mut self,
        bids: &[ProviderBookLevel],
        asks: &[ProviderBookLevel],
        tick_size: TickSize,
        lot_size: LotSize,
        checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
    ) -> Result<Option<u32>, ProviderBookError> {
        let bid_updates = normalize_snapshot_side(bids, BookSide::Bid, tick_size, lot_size)?;
        let ask_updates = normalize_snapshot_side(asks, BookSide::Ask, tick_size, lot_size)?;
        let mut candidate_scaled = ScaledBook::new(self.scaled.depth_limit());
        candidate_scaled.replace_snapshot(&bid_updates, &ask_updates)?;
        let candidate_bids =
            retained_snapshot_levels(bids, &bid_updates, self.scaled.depth_limit())?;
        let candidate_asks =
            retained_snapshot_levels(asks, &ask_updates, self.scaled.depth_limit())?;
        let computed = validate_checksum(&candidate_asks, &candidate_bids, checksum)?;
        self.scaled = candidate_scaled;
        self.bids = candidate_bids;
        self.asks = candidate_asks;
        Ok(computed)
    }

    pub(crate) fn begin_delta(
        &mut self,
        changes: &[ProviderBookChange],
        tick_size: TickSize,
        lot_size: LotSize,
        checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
    ) -> Result<ProviderBookDeltaTransaction<'_>, ProviderBookError> {
        let normalized = changes
            .iter()
            .map(|change| normalize_change(change, tick_size, lot_size))
            .collect::<Result<Vec<_>, _>>()?;
        let updates = normalized
            .iter()
            .map(|change| change.update)
            .collect::<Vec<_>>();
        let scaled_rollback = self
            .scaled
            .begin_delta_checked(&updates, |_| Ok::<(), ProviderBookError>(()))?;
        let exact_rollback = match apply_exact_changes(
            &mut self.bids,
            &mut self.asks,
            &normalized,
            self.scaled.depth_limit(),
        ) {
            Ok(value) => value,
            Err(error) => {
                self.scaled.rollback_delta(scaled_rollback);
                return Err(error);
            }
        };
        let computed = match validate_checksum(&self.asks, &self.bids, checksum) {
            Ok(value) => value,
            Err(error) => {
                rollback_exact(&mut self.bids, &mut self.asks, exact_rollback);
                self.scaled.rollback_delta(scaled_rollback);
                return Err(error);
            }
        };
        Ok(ProviderBookDeltaTransaction {
            book: self,
            scaled_rollback: Some(scaled_rollback),
            exact_rollback: Some(exact_rollback),
            computed,
        })
    }

    pub(crate) fn bid_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        self.scaled
            .bid_levels()
            .into_iter()
            .map(|(price, quantity)| {
                BookLevel::new(price, quantity).map_err(ProviderBookError::from)
            })
            .collect()
    }

    pub(crate) fn ask_levels(&self) -> Result<Vec<BookLevel>, ProviderBookError> {
        self.scaled
            .ask_levels()
            .into_iter()
            .map(|(price, quantity)| {
                BookLevel::new(price, quantity).map_err(ProviderBookError::from)
            })
            .collect()
    }

    pub(crate) fn bid_levels_limited(
        &self,
        limit: usize,
    ) -> Result<Vec<BookLevel>, ProviderBookError> {
        self.scaled_bid_iter()
            .take(limit)
            .map(|(price, quantity)| {
                BookLevel::new(price, quantity).map_err(ProviderBookError::from)
            })
            .collect()
    }

    pub(crate) fn ask_levels_limited(
        &self,
        limit: usize,
    ) -> Result<Vec<BookLevel>, ProviderBookError> {
        self.scaled_ask_iter()
            .take(limit)
            .map(|(price, quantity)| {
                BookLevel::new(price, quantity).map_err(ProviderBookError::from)
            })
            .collect()
    }

    pub(crate) fn bid_level_count(&self) -> usize {
        self.scaled_bid_iter().len()
    }

    pub(crate) fn ask_level_count(&self) -> usize {
        self.scaled_ask_iter().len()
    }

    pub(crate) fn scaled_bid_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.scaled.bid_iter()
    }

    pub(crate) fn scaled_ask_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (PriceTicks, QuantityLots)> + '_ {
        self.scaled.ask_iter()
    }
}

/// Fail-safe mutable provider-book candidate. Drop restores exact last-good state.
#[derive(Debug)]
pub(crate) struct ProviderBookDeltaTransaction<'a> {
    book: &'a mut ProviderBook,
    scaled_rollback: Option<crate::book::ScaledBookRollback>,
    exact_rollback: Option<Vec<ExactRollback>>,
    computed: Option<u32>,
}

impl ProviderBookDeltaTransaction<'_> {
    pub(crate) const fn candidate(&self) -> &ProviderBook {
        self.book
    }

    pub(crate) const fn computed_checksum(&self) -> Option<u32> {
        self.computed
    }

    /// Explicitly commits the candidate. Omission or any early return rolls back on drop.
    pub(crate) fn commit(mut self) {
        self.scaled_rollback = None;
        self.exact_rollback = None;
    }
}

impl Drop for ProviderBookDeltaTransaction<'_> {
    fn drop(&mut self) {
        if let Some(exact) = self.exact_rollback.take() {
            rollback_exact(&mut self.book.bids, &mut self.book.asks, exact);
        }
        if let Some(scaled) = self.scaled_rollback.take() {
            self.book.scaled.rollback_delta(scaled);
        }
    }
}

#[derive(Clone, Debug)]
struct NormalizedChange {
    update: LevelUpdate,
    provider: ProviderBookLevel,
}

fn normalize_snapshot_side(
    levels: &[ProviderBookLevel],
    side: BookSide,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<Vec<LevelUpdate>, ProviderBookError> {
    levels
        .iter()
        .map(|level| {
            Ok(LevelUpdate::new(
                side,
                normalize_price(level.price(), tick_size)?,
                normalize_positive_quantity(level.quantity(), lot_size)?,
            ))
        })
        .collect()
}

fn normalize_change(
    change: &ProviderBookChange,
    tick_size: TickSize,
    lot_size: LotSize,
) -> Result<NormalizedChange, ProviderBookError> {
    let side = match change.side() {
        ProviderBookSide::Bid => BookSide::Bid,
        ProviderBookSide::Ask => BookSide::Ask,
    };
    Ok(NormalizedChange {
        update: LevelUpdate::new(
            side,
            normalize_price(change.level().price(), tick_size)?,
            normalize_delta_quantity(change.level().quantity(), lot_size)?,
        ),
        provider: change.level().clone(),
    })
}

fn retained_snapshot_levels(
    provider: &[ProviderBookLevel],
    updates: &[LevelUpdate],
    depth: DepthLimit,
) -> Result<BTreeMap<PriceTicks, ProviderBookLevel>, ProviderBookError> {
    let mut retained = BTreeMap::new();
    for (level, update) in provider.iter().zip(updates).take(depth.get()) {
        retained.insert(update.price(), level.clone());
    }
    Ok(retained)
}

#[derive(Clone, Debug)]
struct ExactRollback {
    side: BookSide,
    price: PriceTicks,
    previous: Option<ProviderBookLevel>,
}

fn apply_exact_changes(
    bids: &mut BTreeMap<PriceTicks, ProviderBookLevel>,
    asks: &mut BTreeMap<PriceTicks, ProviderBookLevel>,
    changes: &[NormalizedChange],
    depth: DepthLimit,
) -> Result<Vec<ExactRollback>, ProviderBookError> {
    let capacity = changes
        .len()
        .checked_mul(2)
        .ok_or(ProviderBookError::Allocation)?;
    let mut rollback = Vec::new();
    rollback
        .try_reserve(capacity)
        .map_err(|_| ProviderBookError::Allocation)?;
    for change in changes {
        let map = match change.update.side() {
            BookSide::Bid => &mut *bids,
            BookSide::Ask => &mut *asks,
        };
        rollback.push(ExactRollback {
            side: change.update.side(),
            price: change.update.price(),
            previous: map.get(&change.update.price()).cloned(),
        });
        if change.update.quantity().get() == 0 {
            map.remove(&change.update.price());
        } else {
            map.insert(change.update.price(), change.provider.clone());
        }
    }
    truncate_exact(bids, BookSide::Bid, depth, &mut rollback);
    truncate_exact(asks, BookSide::Ask, depth, &mut rollback);
    Ok(rollback)
}

fn truncate_exact(
    map: &mut BTreeMap<PriceTicks, ProviderBookLevel>,
    side: BookSide,
    depth: DepthLimit,
    rollback: &mut Vec<ExactRollback>,
) {
    while map.len() > depth.get() {
        let evicted = match side {
            BookSide::Bid => map.first_key_value(),
            BookSide::Ask => map.last_key_value(),
        }
        .map(|(price, level)| (*price, level.clone()));
        let Some((price, level)) = evicted else {
            break;
        };
        rollback.push(ExactRollback {
            side,
            price,
            previous: Some(level),
        });
        map.remove(&price);
    }
}

fn rollback_exact(
    bids: &mut BTreeMap<PriceTicks, ProviderBookLevel>,
    asks: &mut BTreeMap<PriceTicks, ProviderBookLevel>,
    rollback: Vec<ExactRollback>,
) {
    for entry in rollback.into_iter().rev() {
        let map = match entry.side {
            BookSide::Bid => &mut *bids,
            BookSide::Ask => &mut *asks,
        };
        match entry.previous {
            Some(level) => {
                map.insert(entry.price, level);
            }
            None => {
                map.remove(&entry.price);
            }
        }
    }
}

fn validate_checksum(
    asks: &BTreeMap<PriceTicks, ProviderBookLevel>,
    bids: &BTreeMap<PriceTicks, ProviderBookLevel>,
    checksum: Option<(&ResolvedChecksumValidator, &ProviderChecksumEvidence)>,
) -> Result<Option<u32>, ProviderBookError> {
    let Some((validator, evidence)) = checksum else {
        return Ok(None);
    };
    validator
        .validate_ordered(asks.values(), bids.values().rev(), evidence)
        .map(Some)
        .map_err(ProviderBookError::from)
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
    #[error(transparent)]
    Market(#[from] market_squawk_domain::MarketEventError),
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{LotSize, TickSize};
    use market_squawk_sources::{
        ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderDecimalLexeme,
        ProviderPrice, ProviderQuantity,
    };
    use rust_decimal::Decimal;

    use super::ProviderBook;
    use crate::DepthLimit;

    fn level(price: &str, quantity: &str) -> Result<ProviderBookLevel, Box<dyn std::error::Error>> {
        Ok(ProviderBookLevel::new(
            ProviderPrice::new(ProviderDecimalLexeme::try_new(price)?),
            ProviderQuantity::new(ProviderDecimalLexeme::try_new(quantity)?),
        ))
    }

    #[test]
    fn dropped_late_delta_candidate_restores_scaled_and_exact_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let tick = TickSize::try_from_decimal(Decimal::ONE)?;
        let lot = LotSize::try_from_decimal(Decimal::ONE)?;
        let mut book = ProviderBook::new(DepthLimit::new(10_000)?);
        book.replace_snapshot(
            &[level("100", "2")?],
            &[level("101", "3")?],
            tick,
            lot,
            None,
        )?;
        let before = book.scaled_bid_iter().collect::<Vec<_>>();
        {
            let transaction = book.begin_delta(
                &[ProviderBookChange::new(
                    ProviderBookSide::Bid,
                    level("100", "9")?,
                )],
                tick,
                lot,
                None,
            )?;
            assert_ne!(
                transaction
                    .candidate()
                    .scaled_bid_iter()
                    .collect::<Vec<_>>(),
                before
            );
            // Simulates a digest/evidence/qualification error: no explicit commit.
        }
        assert_eq!(book.scaled_bid_iter().collect::<Vec<_>>(), before);
        Ok(())
    }
}
