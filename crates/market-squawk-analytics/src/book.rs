//! Exact allocation-free order-book feature kernels.

use std::num::{NonZeroU128, NonZeroUsize};

use market_squawk_domain::{PriceTicks, QuantityLots, Timestamp};
use thiserror::Error;

use crate::{ExactFeatureRatio, FeatureError, FeatureValidity, FeatureValue};

/// Maximum price levels accepted per side by one pure book calculation.
pub const MAX_BOOK_FEATURE_LEVELS: usize = 10_000;

/// A price represented exactly in half-tick units.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HalfTickPrice(i128);

impl HalfTickPrice {
    /// Returns the numerator whose implicit denominator is two ticks.
    #[must_use]
    pub const fn half_ticks(self) -> i128 {
        self.0
    }
}

/// One positive-quantity aggregated price level borrowed by pure feature kernels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceLevelView {
    price: PriceTicks,
    quantity: QuantityLots,
}

impl PriceLevelView {
    /// Constructs a positive-quantity price level.
    ///
    /// # Errors
    ///
    /// Returns [`BookFeatureError::NonPositiveQuantity`] for a zero quantity.
    pub fn try_new(price: PriceTicks, quantity: QuantityLots) -> Result<Self, BookFeatureError> {
        if quantity.get() == 0 {
            return Err(BookFeatureError::NonPositiveQuantity);
        }
        Ok(Self { price, quantity })
    }

    /// Returns the exact price ticks.
    #[must_use]
    pub const fn price(self) -> PriceTicks {
        self.price
    }

    /// Returns the positive quantity lots.
    #[must_use]
    pub const fn quantity(self) -> QuantityLots {
        self.quantity
    }
}

/// Immutable exact top-of-book inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TopOfBookView {
    bid_price: PriceTicks,
    bid_quantity: QuantityLots,
    ask_price: PriceTicks,
    ask_quantity: QuantityLots,
    observed_at: Timestamp,
}

impl TopOfBookView {
    /// Constructs top-of-book inputs without silently discarding a crossed state.
    ///
    /// A crossed or locked price relationship is retained so each derived feature can explicitly
    /// publish [`FeatureValidity::Unavailable`]. Quantities must be positive.
    ///
    /// # Errors
    ///
    /// Returns [`BookFeatureError::NonPositiveQuantity`] for either zero quantity.
    pub fn try_new(
        bid_price: PriceTicks,
        bid_quantity: QuantityLots,
        ask_price: PriceTicks,
        ask_quantity: QuantityLots,
        observed_at: Timestamp,
    ) -> Result<Self, BookFeatureError> {
        if bid_quantity.get() == 0 || ask_quantity.get() == 0 {
            return Err(BookFeatureError::NonPositiveQuantity);
        }
        Ok(Self {
            bid_price,
            bid_quantity,
            ask_price,
            ask_quantity,
            observed_at,
        })
    }

    /// Returns the best bid price.
    #[must_use]
    pub const fn bid_price(self) -> PriceTicks {
        self.bid_price
    }

    /// Returns the best bid quantity.
    #[must_use]
    pub const fn bid_quantity(self) -> QuantityLots {
        self.bid_quantity
    }

    /// Returns the best ask price.
    #[must_use]
    pub const fn ask_price(self) -> PriceTicks {
        self.ask_price
    }

    /// Returns the best ask quantity.
    #[must_use]
    pub const fn ask_quantity(self) -> QuantityLots {
        self.ask_quantity
    }

    /// Returns the observation timestamp.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    const fn is_crossed_or_locked(self) -> bool {
        self.bid_price.get() >= self.ask_price.get()
    }
}

/// Borrowed, caller-bounded aggregated depth.
#[derive(Clone, Copy, Debug)]
pub struct BookDepthView<'a> {
    bids: &'a [PriceLevelView],
    asks: &'a [PriceLevelView],
    observed_at: Timestamp,
}

impl<'a> BookDepthView<'a> {
    /// Validates bounded, strictly ordered price-level depth.
    ///
    /// Empty or crossed sides are preserved for explicit unavailable feature output. Nonempty bids
    /// must descend and asks must ascend.
    ///
    /// # Errors
    ///
    /// Returns a typed error when either side exceeds `maximum_levels` or ordering is invalid.
    pub fn try_new(
        bids: &'a [PriceLevelView],
        asks: &'a [PriceLevelView],
        maximum_levels: NonZeroUsize,
        observed_at: Timestamp,
    ) -> Result<Self, BookFeatureError> {
        if maximum_levels.get() > MAX_BOOK_FEATURE_LEVELS {
            return Err(BookFeatureError::DepthBoundTooLarge);
        }
        if bids.len() > maximum_levels.get() || asks.len() > maximum_levels.get() {
            return Err(BookFeatureError::DepthBoundExceeded);
        }
        if !strictly_ordered(bids, true) || !strictly_ordered(asks, false) {
            return Err(BookFeatureError::InvalidDepthOrdering);
        }
        Ok(Self {
            bids,
            asks,
            observed_at,
        })
    }

    /// Returns bid levels in descending price order.
    #[must_use]
    pub const fn bids(self) -> &'a [PriceLevelView] {
        self.bids
    }

    /// Returns ask levels in ascending price order.
    #[must_use]
    pub const fn asks(self) -> &'a [PriceLevelView] {
        self.asks
    }

    /// Returns the depth observation timestamp.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    fn is_complete_and_uncrossed(self) -> bool {
        match (self.bids.first(), self.asks.first()) {
            (Some(bid), Some(ask)) => bid.price.get() < ask.price.get(),
            _ => false,
        }
    }
}

/// Exact top-of-book feature bundle for one observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopOfBookFeatures {
    spread: FeatureValue<PriceTicks>,
    midpoint: FeatureValue<HalfTickPrice>,
    microprice: FeatureValue<ExactFeatureRatio>,
    book_imbalance: FeatureValue<ExactFeatureRatio>,
}

impl TopOfBookFeatures {
    /// Returns the exact spread.
    #[must_use]
    pub const fn spread(&self) -> &FeatureValue<PriceTicks> {
        &self.spread
    }

    /// Returns the exact half-tick midpoint.
    #[must_use]
    pub const fn midpoint(&self) -> &FeatureValue<HalfTickPrice> {
        &self.midpoint
    }

    /// Returns the exact quantity-weighted microprice.
    #[must_use]
    pub const fn microprice(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.microprice
    }

    /// Returns exact `(bid quantity - ask quantity) / total quantity`.
    #[must_use]
    pub const fn book_imbalance(&self) -> &FeatureValue<ExactFeatureRatio> {
        &self.book_imbalance
    }
}

/// Computes spread, midpoint, microprice, and book imbalance without allocation or rounding.
///
/// # Errors
///
/// Returns [`BookFeatureError::FeatureState`] only if construction of an unavailable feature state
/// violates the foundational feature contract.
pub fn top_of_book_features(top: TopOfBookView) -> Result<TopOfBookFeatures, BookFeatureError> {
    if top.is_crossed_or_locked() {
        return Ok(TopOfBookFeatures {
            spread: invalid(FeatureValidity::Unavailable, top.observed_at)?,
            midpoint: invalid(FeatureValidity::Unavailable, top.observed_at)?,
            microprice: invalid(FeatureValidity::Unavailable, top.observed_at)?,
            book_imbalance: invalid(FeatureValidity::Unavailable, top.observed_at)?,
        });
    }

    let spread = match top.ask_price.get().checked_sub(top.bid_price.get()) {
        Some(value) => FeatureValue::ready(PriceTicks::new(value), top.observed_at),
        None => invalid(FeatureValidity::Overflow, top.observed_at)?,
    };
    let midpoint = FeatureValue::ready(
        HalfTickPrice(i128::from(top.bid_price.get()) + i128::from(top.ask_price.get())),
        top.observed_at,
    );
    let bid_quantity = i128::from(top.bid_quantity.get());
    let ask_quantity = i128::from(top.ask_quantity.get());
    let denominator = positive_denominator(bid_quantity + ask_quantity)?;
    let microprice = i128::from(top.ask_price.get())
        .checked_mul(bid_quantity)
        .and_then(|left| {
            i128::from(top.bid_price.get())
                .checked_mul(ask_quantity)
                .and_then(|right| left.checked_add(right))
        })
        .and_then(|numerator| ratio(numerator, denominator).ok())
        .map_or_else(
            || invalid(FeatureValidity::Overflow, top.observed_at),
            |value| Ok(FeatureValue::ready(value, top.observed_at)),
        )?;
    let book_imbalance = FeatureValue::ready(
        ratio(bid_quantity - ask_quantity, denominator)?,
        top.observed_at,
    );
    Ok(TopOfBookFeatures {
        spread,
        midpoint,
        microprice,
        book_imbalance,
    })
}

/// Computes the standard signed top-of-book order-flow imbalance using exact `i128` lots.
///
/// # Errors
///
/// Returns a typed foundational-state error if an invalid result cannot be represented.
pub fn order_flow_imbalance(
    previous: TopOfBookView,
    current: TopOfBookView,
) -> Result<FeatureValue<i128>, BookFeatureError> {
    if current.observed_at < previous.observed_at {
        return invalid(FeatureValidity::TimestampRegression, current.observed_at);
    }
    if previous.is_crossed_or_locked() || current.is_crossed_or_locked() {
        return invalid(FeatureValidity::Unavailable, current.observed_at);
    }
    let bid_flow = match current.bid_price.cmp(&previous.bid_price) {
        std::cmp::Ordering::Greater => i128::from(current.bid_quantity.get()),
        std::cmp::Ordering::Equal => {
            i128::from(current.bid_quantity.get()) - i128::from(previous.bid_quantity.get())
        }
        std::cmp::Ordering::Less => -i128::from(previous.bid_quantity.get()),
    };
    let ask_flow = match current.ask_price.cmp(&previous.ask_price) {
        std::cmp::Ordering::Less => i128::from(current.ask_quantity.get()),
        std::cmp::Ordering::Equal => {
            i128::from(current.ask_quantity.get()) - i128::from(previous.ask_quantity.get())
        }
        std::cmp::Ordering::Greater => -i128::from(previous.ask_quantity.get()),
    };
    Ok(FeatureValue::ready(
        bid_flow - ask_flow,
        current.observed_at,
    ))
}

/// Computes a quantity-weighted price across both sides of bounded depth.
///
/// # Errors
///
/// Returns a typed error if the exact denominator cannot be represented.
pub fn depth_weighted_price(
    depth: BookDepthView<'_>,
) -> Result<FeatureValue<ExactFeatureRatio>, BookFeatureError> {
    if !depth.is_complete_and_uncrossed() {
        return invalid(FeatureValidity::Unavailable, depth.observed_at);
    }
    let mut numerator = 0_i128;
    let mut denominator = 0_u128;
    for level in depth.bids.iter().chain(depth.asks) {
        let quantity = i128::from(level.quantity.get());
        let Some(weighted) = i128::from(level.price.get()).checked_mul(quantity) else {
            return invalid(FeatureValidity::Overflow, depth.observed_at);
        };
        let Some(next_numerator) = numerator.checked_add(weighted) else {
            return invalid(FeatureValidity::Overflow, depth.observed_at);
        };
        let quantity = u128::try_from(level.quantity.get())
            .map_err(|_| BookFeatureError::NonPositiveQuantity)?;
        let Some(next_denominator) = denominator.checked_add(quantity) else {
            return invalid(FeatureValidity::Overflow, depth.observed_at);
        };
        numerator = next_numerator;
        denominator = next_denominator;
    }
    Ok(FeatureValue::ready(
        ExactFeatureRatio::try_new(numerator, denominator)?,
        depth.observed_at,
    ))
}

/// Order-book feature validation or foundational-state failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BookFeatureError {
    /// A displayed level had zero quantity.
    #[error("book feature quantities must be positive")]
    NonPositiveQuantity,
    /// The caller-declared level bound exceeded the production maximum.
    #[error("book feature depth bound exceeds its production maximum")]
    DepthBoundTooLarge,
    /// A borrowed side exceeded the caller-declared bound.
    #[error("book feature depth exceeds its bound")]
    DepthBoundExceeded,
    /// Aggregated price levels were not strictly ordered.
    #[error("book feature depth ordering is invalid")]
    InvalidDepthOrdering,
    /// Foundational feature-state construction failed.
    #[error(transparent)]
    FeatureState(#[from] FeatureError),
}

fn strictly_ordered(levels: &[PriceLevelView], descending: bool) -> bool {
    levels.windows(2).all(|pair| {
        if descending {
            pair[0].price.get() > pair[1].price.get()
        } else {
            pair[0].price.get() < pair[1].price.get()
        }
    })
}

fn positive_denominator(value: i128) -> Result<NonZeroU128, BookFeatureError> {
    let unsigned = u128::try_from(value).map_err(|_| BookFeatureError::NonPositiveQuantity)?;
    NonZeroU128::new(unsigned).ok_or(BookFeatureError::NonPositiveQuantity)
}

fn ratio(numerator: i128, denominator: NonZeroU128) -> Result<ExactFeatureRatio, BookFeatureError> {
    Ok(ExactFeatureRatio::try_new(numerator, denominator.get())?)
}

fn invalid<T>(
    validity: FeatureValidity,
    observed_at: Timestamp,
) -> Result<FeatureValue<T>, BookFeatureError> {
    Ok(FeatureValue::invalid(validity, observed_at)?)
}
