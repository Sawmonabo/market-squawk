//! Exact provider-decimal conversion at a source-adapter boundary.

use market_squawk_domain::{
    LotSize, PriceError, PriceTicks, QuantityError, QuantityLots, TickSize,
};
use thiserror::Error;

use crate::{ProviderPrice, ProviderQuantity};

/// Converts one exact provider price to instrument ticks without rounding.
///
/// # Errors
///
/// Returns [`NormalizationError::InexactPrice`] or [`NormalizationError::PriceOverflow`] when the
/// provider decimal is not exactly representable using the instrument tick size.
pub fn normalize_price(
    provider: &ProviderPrice,
    tick_size: TickSize,
) -> Result<PriceTicks, NormalizationError> {
    PriceTicks::try_from_decimal(provider.value().decimal(), tick_size).map_err(|error| match error
    {
        PriceError::InexactTick => NormalizationError::InexactPrice,
        PriceError::Overflow => NormalizationError::PriceOverflow,
    })
}

/// Converts a provider quantity that must be strictly positive.
///
/// # Errors
///
/// Rejects negative, zero, inexact, and overflowing quantities.
pub fn normalize_positive_quantity(
    provider: &ProviderQuantity,
    lot_size: LotSize,
) -> Result<QuantityLots, NormalizationError> {
    let quantity = normalize_delta_quantity(provider, lot_size)?;
    if quantity.get() == 0 {
        Err(NormalizationError::ZeroQuantity)
    } else {
        Ok(quantity)
    }
}

/// Converts a provider delta quantity, retaining zero exclusively as delete-on-zero evidence.
///
/// # Errors
///
/// Rejects negative, inexact, and overflowing quantities.
pub fn normalize_delta_quantity(
    provider: &ProviderQuantity,
    lot_size: LotSize,
) -> Result<QuantityLots, NormalizationError> {
    QuantityLots::try_from_decimal(provider.value().decimal(), lot_size).map_err(
        |error| match error {
            QuantityError::NegativeQuantity => NormalizationError::NegativeQuantity,
            QuantityError::InexactLot => NormalizationError::InexactQuantity,
            QuantityError::Overflow => NormalizationError::QuantityOverflow,
        },
    )
}

/// Exact provider-number normalization failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum NormalizationError {
    /// Provider price is not an integral number of instrument ticks.
    #[error("provider price is not exactly representable in instrument ticks")]
    InexactPrice,
    /// Provider price exceeds the scaled integer range.
    #[error("provider price exceeds scaled tick range")]
    PriceOverflow,
    /// Provider quantity is negative.
    #[error("provider quantity is negative")]
    NegativeQuantity,
    /// Provider quantity is not an integral number of instrument lots.
    #[error("provider quantity is not exactly representable in instrument lots")]
    InexactQuantity,
    /// Provider quantity exceeds the scaled integer range.
    #[error("provider quantity exceeds scaled lot range")]
    QuantityOverflow,
    /// A snapshot, quote, trade, or auction quantity was zero.
    #[error("provider quantity must be positive for this payload")]
    ZeroQuantity,
}
