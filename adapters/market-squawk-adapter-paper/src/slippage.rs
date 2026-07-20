//! Checked tick-space slippage and impact bounds.

use market_squawk_domain::{BasisPoints, OrderSide, PriceTicks};
use thiserror::Error;

pub(crate) fn adverse_bound(
    reference: PriceTicks,
    side: OrderSide,
    slippage: BasisPoints,
) -> Result<PriceTicks, SlippageError> {
    if reference.get() <= 0 || !(0..=10_000).contains(&slippage.get()) {
        return Err(SlippageError::InvalidInput);
    }
    scale_ticks(
        reference,
        side,
        u32::try_from(slippage.get()).map_err(|_| SlippageError::InvalidInput)?,
    )
}

pub(crate) fn apply_level_impact(
    price: PriceTicks,
    side: OrderSide,
    basis_points_per_level: u32,
    level_index: usize,
) -> Result<PriceTicks, SlippageError> {
    let level = u32::try_from(level_index).map_err(|_| SlippageError::Overflow)?;
    let impact = basis_points_per_level
        .checked_mul(level)
        .ok_or(SlippageError::Overflow)?
        .min(10_000);
    scale_ticks(price, side, impact)
}

fn scale_ticks(
    price: PriceTicks,
    side: OrderSide,
    basis_points: u32,
) -> Result<PriceTicks, SlippageError> {
    let base = i128::from(price.get());
    let factor = match side {
        OrderSide::Buy => 10_000_i128 + i128::from(basis_points),
        OrderSide::Sell => 10_000_i128 - i128::from(basis_points),
    };
    let numerator = base.checked_mul(factor).ok_or(SlippageError::Overflow)?;
    let quotient = numerator / 10_000_i128;
    let remainder = numerator % 10_000_i128;
    let adjusted = if side == OrderSide::Buy && remainder != 0 {
        quotient.checked_add(1).ok_or(SlippageError::Overflow)?
    } else {
        quotient
    };
    let ticks = i64::try_from(adjusted).map_err(|_| SlippageError::Overflow)?;
    if ticks <= 0 {
        return Err(SlippageError::InvalidInput);
    }
    Ok(PriceTicks::new(ticks))
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SlippageError {
    #[error("paper slippage input is invalid")]
    InvalidInput,
    #[error("paper slippage arithmetic overflowed")]
    Overflow,
}
