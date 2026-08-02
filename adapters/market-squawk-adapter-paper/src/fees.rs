//! Exact configurable paper fee calculation.

use market_squawk_domain::{Currency, Money, RoundingPolicy};
use market_squawk_execution::MAX_PAPER_FEE_BASIS_POINTS;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Whether a simulated fill removed or supplied displayed liquidity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
}

/// Immutable fee rules applied to every paper fill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeeSchedule {
    maker_basis_points: u32,
    taker_basis_points: u32,
    minimum_fee: Money,
    maximum_fee: Option<Money>,
    money_scale: u32,
}

impl FeeSchedule {
    /// Constructs a nonnegative, currency-consistent fee schedule.
    ///
    /// # Errors
    ///
    /// Rejects fee rates above one hundred percent, unsupported scales, negative bounds, mixed
    /// currencies, or an inverted fee range.
    pub fn try_new(
        maker_basis_points: u32,
        taker_basis_points: u32,
        minimum_fee: Money,
        maximum_fee: Option<Money>,
        money_scale: u32,
    ) -> Result<Self, FeeError> {
        if u64::from(maker_basis_points) > MAX_PAPER_FEE_BASIS_POINTS
            || u64::from(taker_basis_points) > MAX_PAPER_FEE_BASIS_POINTS
            || money_scale > Decimal::MAX_SCALE
            || minimum_fee.amount().is_sign_negative()
        {
            return Err(FeeError::InvalidSchedule);
        }
        if let Some(maximum) = maximum_fee
            && (maximum.currency() != minimum_fee.currency()
                || maximum.amount().is_sign_negative()
                || maximum.amount() < minimum_fee.amount())
        {
            return Err(FeeError::InvalidSchedule);
        }
        Ok(Self {
            maker_basis_points,
            taker_basis_points,
            minimum_fee,
            maximum_fee,
            money_scale,
        })
    }

    /// Returns the schedule currency.
    pub const fn currency(self) -> Currency {
        self.minimum_fee.currency()
    }

    pub(crate) const fn maker_basis_points(self) -> u32 {
        self.maker_basis_points
    }

    pub(crate) const fn taker_basis_points(self) -> u32 {
        self.taker_basis_points
    }

    pub(crate) const fn minimum_fee(self) -> Money {
        self.minimum_fee
    }

    pub(crate) const fn maximum_fee(self) -> Option<Money> {
        self.maximum_fee
    }

    pub(crate) const fn money_scale(self) -> u32 {
        self.money_scale
    }

    /// Calculates one explicitly rounded, bounded fee.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies, negative notional, or checked decimal overflow.
    pub fn charge(self, notional: Money, role: LiquidityRole) -> Result<Money, FeeError> {
        let zero = Money::new(Decimal::ZERO, notional.currency());
        match role {
            LiquidityRole::Maker => self.charge_cumulative(notional, zero),
            LiquidityRole::Taker => self.charge_cumulative(zero, notional),
        }
    }

    pub(crate) fn charge_cumulative(
        self,
        maker_notional: Money,
        taker_notional: Money,
    ) -> Result<Money, FeeError> {
        if maker_notional.currency() != self.currency()
            || taker_notional.currency() != self.currency()
        {
            return Err(FeeError::CurrencyMismatch);
        }
        if maker_notional.amount().is_sign_negative() || taker_notional.amount().is_sign_negative()
        {
            return Err(FeeError::NegativeNotional);
        }
        if maker_notional.amount().is_zero() && taker_notional.amount().is_zero() {
            return Ok(Money::new(Decimal::ZERO, self.currency()));
        }
        let mut amount = maker_notional
            .checked_weighted_basis_points(
                self.maker_basis_points,
                taker_notional,
                self.taker_basis_points,
                self.money_scale,
                RoundingPolicy::NearestEven,
            )
            .map_err(|_| FeeError::Overflow)?
            .amount();
        amount = amount.max(self.minimum_fee.amount());
        if let Some(maximum) = self.maximum_fee {
            amount = amount.min(maximum.amount());
        }
        Ok(Money::new(amount, self.currency()))
    }
}

/// Exact fee-rule failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FeeError {
    #[error("paper fee schedule is invalid")]
    InvalidSchedule,
    #[error("paper fee currency does not match fill notional")]
    CurrencyMismatch,
    #[error("paper fee notional must not be negative")]
    NegativeNotional,
    #[error("paper fee arithmetic overflowed")]
    Overflow,
}
