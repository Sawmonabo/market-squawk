use std::str::FromStr;

use market_squawk_domain::{
    BasisPoints, Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError,
    QuantityLots, RoundingPolicy, TickSize,
};
use num_bigint::BigInt;
use rust_decimal::Decimal;

#[test]
fn exact_conversion_rejects_a_sub_decimal_half_tick_and_lot()
-> Result<(), Box<dyn std::error::Error>> {
    let value = Decimal::try_new(1, Decimal::MAX_SCALE)?;
    let tick = TickSize::try_from_decimal(Decimal::new(2, 0))?;
    let lot = LotSize::try_from_decimal(Decimal::new(2, 0))?;

    assert_eq!(
        PriceTicks::try_from_decimal(value, tick),
        Err(PriceError::InexactTick)
    );
    assert_eq!(
        QuantityLots::try_from_decimal(value, lot),
        Err(QuantityError::InexactLot)
    );
    Ok(())
}

#[test]
fn scaled_values_reject_a_product_that_decimal_would_round()
-> Result<(), Box<dyn std::error::Error>> {
    let increment = Decimal::from_str("7.9228162514264337593543950335")?;
    let tick = TickSize::try_from_decimal(increment)?;
    let lot = LotSize::try_from_decimal(increment)?;

    assert_eq!(
        PriceTicks::new(3).checked_to_decimal(tick),
        Err(PriceError::Overflow)
    );
    assert_eq!(
        QuantityLots::new(3)?.checked_to_decimal(lot),
        Err(QuantityError::Overflow)
    );
    Ok(())
}

#[test]
fn notional_rejects_rounded_scaled_intermediates() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::from_str("7.9228162514264337593543950335")?)?;
    let lot = LotSize::try_from_decimal(Decimal::ONE)?;

    assert_eq!(
        PriceTicks::new(3).checked_mul_quantity(
            QuantityLots::new(1)?,
            tick,
            lot,
            Currency::try_from("USD")?,
        ),
        Err(FinancialError::Overflow)
    );
    Ok(())
}

#[test]
fn money_scalars_reject_silent_products_and_round_exact_fee_rationals()
-> Result<(), Box<dyn std::error::Error>> {
    let usd = Currency::try_from("USD")?;
    let multiplier = Decimal::from_str("7.9228162514264337593543950334")?;
    let amount = Money::new(Decimal::new(3, 0), usd);
    assert!(amount.amount().checked_mul(multiplier).is_some());
    assert_eq!(
        amount.checked_mul_decimal(multiplier),
        Err(FinancialError::Overflow)
    );

    let dust = Money::new(Decimal::try_new(1, Decimal::MAX_SCALE)?, usd);
    let legacy = dust
        .amount()
        .checked_mul(Decimal::ONE)
        .and_then(|value| value.checked_div(Decimal::from(10_000_u32)));
    assert_eq!(legacy, Some(Decimal::ZERO));

    let exact_numerator = BigInt::from(dust.amount().mantissa());
    let exact_denominator = BigInt::from(10_000_u32);
    assert!(exact_numerator % exact_denominator != BigInt::from(0_u8));
    assert_eq!(
        dust.checked_basis_points(
            BasisPoints::new(1),
            Decimal::MAX_SCALE,
            RoundingPolicy::Ceiling,
        )?,
        Money::new(Decimal::try_new(1, Decimal::MAX_SCALE)?, usd)
    );
    Ok(())
}

#[test]
fn basis_point_rounding_normalizes_large_integral_and_adverse_fractional_fees()
-> Result<(), Box<dyn std::error::Error>> {
    let usd = Currency::try_from("USD")?;

    let high_notional = Money::new(Decimal::from(1_000_000_u32), usd);
    assert_eq!(
        high_notional.checked_basis_points(
            BasisPoints::new(100),
            Decimal::MAX_SCALE,
            RoundingPolicy::Ceiling,
        )?,
        Money::new(Decimal::from(10_000_u32), usd)
    );

    let fractional_notional = Money::new(Decimal::from_str("1234567.89")?, usd);
    assert_eq!(
        fractional_notional.checked_basis_points(
            BasisPoints::new(1),
            4,
            RoundingPolicy::Ceiling,
        )?,
        Money::new(Decimal::from_str("123.4568")?, usd)
    );
    Ok(())
}

#[test]
fn money_deserialization_uses_constructor_canonicalization()
-> Result<(), Box<dyn std::error::Error>> {
    let money: Money = serde_json::from_str(r#"{"amount":"1.2300","currency":"usd"}"#)?;

    assert_eq!(money.amount().mantissa(), 123);
    assert_eq!(money.amount().scale(), 2);
    assert_eq!(money.currency(), Currency::try_from("USD")?);
    Ok(())
}

#[test]
fn explicit_rounding_uses_the_exact_rational_value_before_rounding()
-> Result<(), Box<dyn std::error::Error>> {
    let positive_dust = Decimal::try_new(1, Decimal::MAX_SCALE)?;
    let negative_dust = -positive_dust;
    let tick = TickSize::try_from_decimal(Decimal::new(2, 0))?;
    let lot = LotSize::try_from_decimal(Decimal::new(2, 0))?;

    assert_eq!(
        PriceTicks::from_decimal_rounded(positive_dust, tick, RoundingPolicy::AwayFromZero)?,
        PriceTicks::new(1)
    );
    assert_eq!(
        PriceTicks::from_decimal_rounded(positive_dust, tick, RoundingPolicy::Ceiling)?,
        PriceTicks::new(1)
    );
    assert_eq!(
        PriceTicks::from_decimal_rounded(negative_dust, tick, RoundingPolicy::Floor)?,
        PriceTicks::new(-1)
    );
    assert_eq!(
        QuantityLots::from_decimal_rounded(positive_dust, lot, RoundingPolicy::AwayFromZero,)?,
        QuantityLots::new(1)?
    );
    Ok(())
}

#[test]
fn exact_conversion_distinguishes_inexact_divisibility_from_range_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    let tiny_tick = TickSize::try_from_decimal(Decimal::try_new(1, Decimal::MAX_SCALE)?)?;
    let non_divisor = TickSize::try_from_decimal(Decimal::try_new(11, Decimal::MAX_SCALE)?)?;

    assert_eq!(
        PriceTicks::try_from_decimal(Decimal::MAX, tiny_tick),
        Err(PriceError::Overflow)
    );
    assert_eq!(
        PriceTicks::try_from_decimal(Decimal::MAX, non_divisor),
        Err(PriceError::InexactTick)
    );
    assert_eq!(
        PriceTicks::try_from_decimal(Decimal::from(i64::MIN), TickSize::power_of_ten(0)?),
        Ok(PriceTicks::new(i64::MIN))
    );
    Ok(())
}

#[test]
fn nearest_even_rounding_handles_positive_and_negative_exact_ties()
-> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(2, 0))?;

    for (value, expected) in [(5, 2), (7, 4), (-5, -2), (-7, -4)] {
        assert_eq!(
            PriceTicks::from_decimal_rounded(
                Decimal::new(value, 0),
                tick,
                RoundingPolicy::NearestEven,
            )?,
            PriceTicks::new(expected)
        );
    }
    Ok(())
}
