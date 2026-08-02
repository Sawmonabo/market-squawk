//! Exact side-aware liquidity and slippage estimation over bounded depth.

use std::num::NonZeroUsize;

use market_squawk_domain::{OrderSide, QuantityLots, Timestamp};
use thiserror::Error;

use crate::{
    BookDepthView, BookFeatureError, ExactFeatureRatio, FeatureError, FeatureValidity,
    FeatureValue, PriceLevelView,
};

/// Borrowed validated depth for a side-aware liquidity walk.
#[derive(Clone, Copy, Debug)]
pub struct LiquidityBookView<'a> {
    bids: &'a [PriceLevelView],
    asks: &'a [PriceLevelView],
    observed_at: Timestamp,
    is_complete_and_uncrossed: bool,
}

impl<'a> LiquidityBookView<'a> {
    /// Validates bounded, strictly ordered aggregated depth.
    ///
    /// Empty or crossed depth is retained so estimation publishes explicit unavailable values.
    ///
    /// # Errors
    ///
    /// Returns a typed book validation error for excessive or unordered depth.
    pub fn try_new(
        bids: &'a [PriceLevelView],
        asks: &'a [PriceLevelView],
        maximum_levels: NonZeroUsize,
        observed_at: Timestamp,
    ) -> Result<Self, LiquidityFeatureError> {
        let depth = BookDepthView::try_new(bids, asks, maximum_levels, observed_at)?;
        let is_complete_and_uncrossed = match (bids.first(), asks.first()) {
            (Some(bid), Some(ask)) => bid.price().get() < ask.price().get(),
            _ => false,
        };
        Ok(Self {
            bids: depth.bids(),
            asks: depth.asks(),
            observed_at,
            is_complete_and_uncrossed,
        })
    }

    /// Returns the observation timestamp.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
}

/// Exact side-aware market-order liquidity estimate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiquidityEstimate {
    available_quantity: FeatureValue<i128>,
    weighted_fill_price: FeatureValue<ExactFeatureRatio>,
    slippage_basis_points: FeatureValue<ExactFeatureRatio>,
}

impl LiquidityEstimate {
    /// Returns total available lots on the walked side.
    #[must_use]
    pub const fn available_quantity(&self) -> &FeatureValue<i128> {
        &self.available_quantity
    }

    /// Returns the exact weighted fill price when complete depth is sufficient.
    #[must_use]
    pub const fn weighted_fill_price(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.weighted_fill_price
    }

    /// Returns exact adverse slippage relative to the walked side's best price.
    #[must_use]
    pub const fn slippage_basis_points(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.slippage_basis_points
    }

    fn invalid(
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<Self, LiquidityFeatureError> {
        Ok(Self {
            available_quantity: invalid(validity, observed_at)?,
            weighted_fill_price: invalid(validity, observed_at)?,
            slippage_basis_points: invalid(validity, observed_at)?,
        })
    }

    fn with_available(
        available_quantity: FeatureValue<i128>,
        validity: FeatureValidity,
        observed_at: Timestamp,
    ) -> Result<Self, LiquidityFeatureError> {
        Ok(Self {
            available_quantity,
            weighted_fill_price: invalid(validity, observed_at)?,
            slippage_basis_points: invalid(validity, observed_at)?,
        })
    }
}

/// Walks bounded depth for a buy or sell request without allocation or price rounding.
///
/// A request larger than displayed depth retains exact available quantity while returning no
/// weighted price or slippage. Crossed, empty, or nonpositive-reference depth is unavailable.
///
/// # Errors
///
/// Returns [`LiquidityFeatureError::NonPositiveRequest`] for zero requested quantity, or a typed
/// foundational error if an invalid feature state cannot be represented.
pub fn estimate_market_order(
    book: LiquidityBookView<'_>,
    side: OrderSide,
    requested_quantity: QuantityLots,
) -> Result<LiquidityEstimate, LiquidityFeatureError> {
    if requested_quantity.get() == 0 {
        return Err(LiquidityFeatureError::NonPositiveRequest);
    }
    if !book.is_complete_and_uncrossed {
        return LiquidityEstimate::invalid(FeatureValidity::Unavailable, book.observed_at);
    }
    let levels = match side {
        OrderSide::Buy => book.asks,
        OrderSide::Sell => book.bids,
    };

    let mut available = 0_i128;
    for level in levels {
        let Some(next) = available.checked_add(i128::from(level.quantity().get())) else {
            return LiquidityEstimate::invalid(FeatureValidity::Overflow, book.observed_at);
        };
        available = next;
    }
    let available_quantity = FeatureValue::ready(available, book.observed_at);
    if available < i128::from(requested_quantity.get()) {
        return LiquidityEstimate::with_available(
            available_quantity,
            FeatureValidity::Unavailable,
            book.observed_at,
        );
    }

    let mut remaining = requested_quantity.get();
    let mut weighted_numerator = 0_i128;
    for level in levels {
        if remaining == 0 {
            break;
        }
        let fill = remaining.min(level.quantity().get());
        let Some(weighted) = i128::from(level.price().get()).checked_mul(i128::from(fill)) else {
            return LiquidityEstimate::with_available(
                available_quantity,
                FeatureValidity::Overflow,
                book.observed_at,
            );
        };
        let Some(next) = weighted_numerator.checked_add(weighted) else {
            return LiquidityEstimate::with_available(
                available_quantity,
                FeatureValidity::Overflow,
                book.observed_at,
            );
        };
        weighted_numerator = next;
        remaining -= fill;
    }
    if remaining != 0 {
        return Err(LiquidityFeatureError::DepthInvariant);
    }
    let fill_denominator = u128::try_from(requested_quantity.get())
        .map_err(|_| LiquidityFeatureError::DepthInvariant)?;
    let weighted_fill_price = FeatureValue::ready(
        ExactFeatureRatio::try_new(weighted_numerator, fill_denominator)?,
        book.observed_at,
    );

    let reference = levels
        .first()
        .ok_or(LiquidityFeatureError::DepthInvariant)?
        .price()
        .get();
    let slippage_basis_points = if reference <= 0 {
        invalid(FeatureValidity::Unavailable, book.observed_at)?
    } else {
        let Some(reference_weighted) =
            i128::from(reference).checked_mul(i128::from(requested_quantity.get()))
        else {
            return Ok(LiquidityEstimate {
                available_quantity,
                weighted_fill_price,
                slippage_basis_points: invalid(FeatureValidity::Overflow, book.observed_at)?,
            });
        };
        let adverse_ticks = match side {
            OrderSide::Buy => weighted_numerator.checked_sub(reference_weighted),
            OrderSide::Sell => reference_weighted.checked_sub(weighted_numerator),
        };
        match adverse_ticks
            .and_then(|ticks| ticks.checked_mul(10_000))
            .zip(u128::try_from(reference_weighted).ok())
        {
            Some((numerator, denominator)) => FeatureValue::ready(
                ExactFeatureRatio::try_new(numerator, denominator)?,
                book.observed_at,
            ),
            None => invalid(FeatureValidity::Overflow, book.observed_at)?,
        }
    };
    Ok(LiquidityEstimate {
        available_quantity,
        weighted_fill_price,
        slippage_basis_points,
    })
}

/// Liquidity validation, bounded-depth, or exact-arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiquidityFeatureError {
    /// The requested quantity was zero.
    #[error("liquidity request quantity must be positive")]
    NonPositiveRequest,
    /// Validated displayed depth did not satisfy its private walk invariant.
    #[error("liquidity depth invariant failed")]
    DepthInvariant,
    /// Borrowed book validation failed.
    #[error(transparent)]
    Book(#[from] BookFeatureError),
    /// Foundational feature-state construction failed.
    #[error(transparent)]
    FeatureState(#[from] FeatureError),
}

fn invalid<T>(
    validity: FeatureValidity,
    observed_at: Timestamp,
) -> Result<FeatureValue<T>, LiquidityFeatureError> {
    Ok(FeatureValue::invalid(validity, observed_at)?)
}
