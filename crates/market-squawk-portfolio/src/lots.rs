//! Exact long and short tax-lot state.

use std::collections::BTreeSet;

use market_squawk_domain::{InstrumentId, Money, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;

use crate::{PortfolioError, checked_decimal_add};

/// Direction of an open tax lot.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LotDirection {
    /// Owned inventory with nonnegative cost basis.
    Long,
    /// Borrowed inventory with retained opening proceeds.
    Short,
}

/// Explicit lot-disposal policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LotSelection {
    /// Dispose the earliest acquired compatible lots first.
    Fifo,
    /// Dispose only the named opening transaction lots, in caller order.
    SpecificIdentification(Vec<SourceIdentifier>),
}

impl LotSelection {
    pub(crate) fn validate(&self) -> Result<(), PortfolioError> {
        if let Self::SpecificIdentification(ids) = self {
            if ids.is_empty() {
                return Err(PortfolioError::InvalidLotSelection);
            }
            let unique = ids.iter().collect::<BTreeSet<_>>();
            if unique.len() != ids.len() {
                return Err(PortfolioError::InvalidLotSelection);
            }
        }
        Ok(())
    }
}

/// One immutable open tax lot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lot {
    pub(crate) id: SourceIdentifier,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) direction: LotDirection,
    pub(crate) opened_at: Timestamp,
    pub(crate) quantity: Decimal,
    pub(crate) basis: Money,
    pub(crate) basis_complete: bool,
}

impl Lot {
    /// Returns the opening transaction identity used for specific identification.
    pub const fn id(&self) -> &SourceIdentifier {
        &self.id
    }

    /// Returns the canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns long or short inventory direction.
    pub const fn direction(&self) -> LotDirection {
        self.direction
    }

    /// Returns the immutable opening time.
    pub const fn opened_at(&self) -> Timestamp {
        self.opened_at
    }

    /// Returns strictly positive open units.
    pub const fn quantity(&self) -> Decimal {
        self.quantity
    }

    /// Returns remaining long cost basis or short opening proceeds.
    pub const fn basis(&self) -> Money {
        self.basis
    }

    /// Returns whether source evidence permits the lot basis to be treated as fully allocated.
    pub const fn basis_complete(&self) -> bool {
        self.basis_complete
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Disposal {
    pub(crate) basis: Money,
    pub(crate) basis_complete: bool,
}

pub(crate) fn dispose(
    lots: &mut Vec<Lot>,
    instrument_id: InstrumentId,
    direction: LotDirection,
    quantity: Decimal,
    selection: &LotSelection,
) -> Result<Disposal, PortfolioError> {
    selection.validate()?;
    let available = lots
        .iter()
        .filter(|lot| lot.instrument_id == instrument_id && lot.direction == direction)
        .try_fold(Decimal::ZERO, |total, lot| {
            checked_decimal_add(total, lot.quantity)
        })?;
    if available < quantity {
        return Err(PortfolioError::InsufficientInventory);
    }
    let indices = selected_indices(lots, instrument_id, direction, selection)?;
    let currency = indices
        .first()
        .and_then(|index| lots.get(*index))
        .map(|lot| lot.basis.currency())
        .ok_or(PortfolioError::InvalidLotSelection)?;
    let mut remaining = quantity;
    let mut removed_basis = Money::new(Decimal::ZERO, currency);
    let mut basis_complete = true;
    for index in indices {
        if remaining.is_zero() {
            break;
        }
        let lot = lots
            .get_mut(index)
            .ok_or(PortfolioError::InvalidLotSelection)?;
        let removed_quantity = remaining.min(lot.quantity);
        let basis_amount = if removed_quantity == lot.quantity {
            lot.basis.amount()
        } else {
            lot.basis
                .amount()
                .checked_mul(removed_quantity)
                .and_then(|value| value.checked_div(lot.quantity))
                .ok_or(PortfolioError::Arithmetic)?
                .normalize()
        };
        let allocation = Money::new(basis_amount, currency);
        lot.quantity = lot
            .quantity
            .checked_sub(removed_quantity)
            .ok_or(PortfolioError::Arithmetic)?
            .normalize();
        lot.basis = lot
            .basis
            .checked_sub(allocation)
            .map_err(|_| PortfolioError::Arithmetic)?;
        removed_basis = removed_basis
            .checked_add(allocation)
            .map_err(|_| PortfolioError::Arithmetic)?;
        basis_complete &= lot.basis_complete;
        remaining = remaining
            .checked_sub(removed_quantity)
            .ok_or(PortfolioError::Arithmetic)?
            .normalize();
    }
    if !remaining.is_zero() {
        return Err(PortfolioError::InvalidLotSelection);
    }
    lots.retain(|lot| !lot.quantity.is_zero());
    Ok(Disposal {
        basis: removed_basis,
        basis_complete,
    })
}

fn selected_indices(
    lots: &[Lot],
    instrument_id: InstrumentId,
    direction: LotDirection,
    selection: &LotSelection,
) -> Result<Vec<usize>, PortfolioError> {
    match selection {
        LotSelection::Fifo => Ok(lots
            .iter()
            .enumerate()
            .filter(|(_, lot)| lot.instrument_id == instrument_id && lot.direction == direction)
            .map(|(index, _)| index)
            .collect()),
        LotSelection::SpecificIdentification(ids) => {
            let mut indices = Vec::new();
            indices
                .try_reserve_exact(ids.len())
                .map_err(|_| PortfolioError::AllocationFailed)?;
            for id in ids {
                let index = lots
                    .iter()
                    .position(|lot| {
                        lot.instrument_id == instrument_id
                            && lot.direction == direction
                            && lot.id == *id
                    })
                    .ok_or(PortfolioError::InvalidLotSelection)?;
                indices.push(index);
            }
            Ok(indices)
        }
    }
}
