//! Exact allocation-free trade feature inputs and kernels.

use std::num::NonZeroUsize;

use market_squawk_domain::{AggressorSide, PriceTicks, QuantityLots, Timestamp};
use thiserror::Error;

use crate::{ExactFeatureRatio, FeatureError, FeatureValidity, FeatureValue};

/// Maximum observations accepted by one pure trade slice calculation.
pub const MAX_TRADE_FEATURE_OBSERVATIONS: usize = 1_048_576;

/// Immutable exact trade inputs used by live and rolling kernels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradeFeatureView {
    price: PriceTicks,
    quantity: QuantityLots,
    aggressor: AggressorSide,
    observed_at: Timestamp,
}

impl TradeFeatureView {
    /// Constructs a positive-quantity trade feature view.
    ///
    /// # Errors
    ///
    /// Returns [`TradeFeatureError::NonPositiveQuantity`] for a zero quantity.
    pub fn try_new(
        price: PriceTicks,
        quantity: QuantityLots,
        aggressor: AggressorSide,
        observed_at: Timestamp,
    ) -> Result<Self, TradeFeatureError> {
        if quantity.get() == 0 {
            return Err(TradeFeatureError::NonPositiveQuantity);
        }
        Ok(Self {
            price,
            quantity,
            aggressor,
            observed_at,
        })
    }

    /// Returns the exact trade price.
    #[must_use]
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the positive trade quantity.
    #[must_use]
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }

    /// Returns the source-established aggressor side.
    #[must_use]
    pub const fn aggressor(self) -> AggressorSide {
        self.aggressor
    }

    /// Returns the trade observation timestamp.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
}

/// Computes exact `(buy lots - sell lots) / classified lots` over a bounded trade slice.
///
/// Unknown aggressor volume is deliberately excluded from both numerator and denominator. An empty
/// classified set produces [`FeatureValidity::Unavailable`] rather than manufacturing a zero.
///
/// # Errors
///
/// Returns a typed error when `trades` exceeds the declared or production bound, timestamps regress
/// within the slice, or foundational feature-state construction fails.
pub fn aggressor_imbalance(
    trades: &[TradeFeatureView],
    maximum_observations: NonZeroUsize,
) -> Result<FeatureValue<ExactFeatureRatio>, TradeFeatureError> {
    if maximum_observations.get() > MAX_TRADE_FEATURE_OBSERVATIONS {
        return Err(TradeFeatureError::ObservationBoundTooLarge);
    }
    if trades.len() > maximum_observations.get() {
        return Err(TradeFeatureError::ObservationBoundExceeded);
    }
    let Some(last) = trades.last() else {
        return Err(TradeFeatureError::EmptyObservations);
    };
    let observed_at = last.observed_at;
    if trades
        .windows(2)
        .any(|pair| pair[1].observed_at < pair[0].observed_at)
    {
        return Ok(FeatureValue::invalid(
            FeatureValidity::TimestampRegression,
            observed_at,
        )?);
    }

    let mut buy = 0_i128;
    let mut sell = 0_i128;
    for trade in trades {
        match trade.aggressor {
            AggressorSide::Buy => {
                buy = buy
                    .checked_add(i128::from(trade.quantity.get()))
                    .ok_or(TradeFeatureError::ArithmeticOverflow)?;
            }
            AggressorSide::Sell => {
                sell = sell
                    .checked_add(i128::from(trade.quantity.get()))
                    .ok_or(TradeFeatureError::ArithmeticOverflow)?;
            }
            AggressorSide::Unknown => {}
        }
    }
    let classified = buy
        .checked_add(sell)
        .ok_or(TradeFeatureError::ArithmeticOverflow)?;
    if classified == 0 {
        return Ok(FeatureValue::invalid(
            FeatureValidity::Unavailable,
            observed_at,
        )?);
    }
    let denominator =
        u128::try_from(classified).map_err(|_| TradeFeatureError::ArithmeticOverflow)?;
    Ok(FeatureValue::ready(
        ExactFeatureRatio::try_new(buy - sell, denominator)?,
        observed_at,
    ))
}

/// Trade feature validation, bound, or exact-arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TradeFeatureError {
    /// A trade had zero quantity.
    #[error("trade feature quantity must be positive")]
    NonPositiveQuantity,
    /// The configured observation bound exceeded the production maximum.
    #[error("trade feature observation bound exceeds its production maximum")]
    ObservationBoundTooLarge,
    /// The supplied slice exceeded its caller-declared observation bound.
    #[error("trade feature observations exceed the declared bound")]
    ObservationBoundExceeded,
    /// The supplied observation slice was empty.
    #[error("trade feature observations must not be empty")]
    EmptyObservations,
    /// Checked exact arithmetic overflowed.
    #[error("trade feature arithmetic overflowed")]
    ArithmeticOverflow,
    /// Foundational feature-state construction failed.
    #[error(transparent)]
    FeatureState(#[from] FeatureError),
}
