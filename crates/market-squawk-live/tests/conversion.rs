use std::error::Error;

use market_squawk_domain::{LotSize, PriceTicks, QuantityLots, TickSize};
use market_squawk_live::{
    NormalizationError, normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};
use market_squawk_sources::{ProviderDecimalLexeme, ProviderPrice, ProviderQuantity};
use rust_decimal::Decimal;

fn price(value: &str) -> Result<ProviderPrice, Box<dyn Error>> {
    Ok(ProviderPrice::new(ProviderDecimalLexeme::try_new(value)?))
}

fn quantity(value: &str) -> Result<ProviderQuantity, Box<dyn Error>> {
    Ok(ProviderQuantity::new(ProviderDecimalLexeme::try_new(
        value,
    )?))
}

#[test]
fn provider_decimals_are_converted_exactly_without_rounding() -> Result<(), Box<dyn Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let lot = LotSize::try_from_decimal(Decimal::new(1, 3))?;

    assert_eq!(
        normalize_price(&price("123.45")?, tick)?,
        PriceTicks::new(2_469)
    );
    assert_eq!(
        normalize_positive_quantity(&quantity("1.234")?, lot)?,
        QuantityLots::new(1_234)?
    );
    assert!(matches!(
        normalize_price(&price("123.46")?, tick),
        Err(NormalizationError::InexactPrice)
    ));
    assert!(matches!(
        normalize_positive_quantity(&quantity("1.2345")?, lot),
        Err(NormalizationError::InexactQuantity)
    ));
    Ok(())
}

#[test]
fn zero_is_allowed_only_for_delta_delete_semantics() -> Result<(), Box<dyn Error>> {
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;
    let zero = quantity("0")?;

    assert_eq!(normalize_delta_quantity(&zero, lot)?, QuantityLots::new(0)?);
    assert_eq!(
        normalize_positive_quantity(&zero, lot),
        Err(NormalizationError::ZeroQuantity)
    );
    Ok(())
}

#[test]
fn negative_and_range_overflow_are_fail_closed() -> Result<(), Box<dyn Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(1, 28))?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;

    assert!(matches!(
        normalize_price(&price("79228162514264337593543950335")?, tick),
        Err(NormalizationError::PriceOverflow)
    ));
    assert_eq!(
        normalize_delta_quantity(&quantity("-1")?, lot),
        Err(NormalizationError::NegativeQuantity)
    );
    Ok(())
}
