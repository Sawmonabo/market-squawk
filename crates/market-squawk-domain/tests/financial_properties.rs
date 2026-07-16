use market_squawk_domain::{
    Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError, QuantityLots,
    TickSize,
};
use num_bigint::BigInt;
use proptest::prelude::*;
use proptest::test_runner::Config;
use rust_decimal::Decimal;

const MAX_DECIMAL_MANTISSA: i128 = 79_228_162_514_264_337_593_543_950_335;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RatioOracleError {
    Inexact,
    Overflow,
}

fn decimal_from_parts(high: u32, low: u64, is_negative: bool, scale: u32) -> Option<Decimal> {
    let magnitude = (u128::from(high) << 64) | u128::from(low);
    let mantissa = i128::try_from(magnitude).ok()?;
    let signed = if is_negative { -mantissa } else { mantissa };
    Decimal::try_from_i128_with_scale(signed, scale).ok()
}

fn positive_decimal_from_parts(high: u32, low: u64, scale: u32) -> Option<Decimal> {
    let value = decimal_from_parts(high, low, false, scale)?;
    (!value.is_zero()).then_some(value)
}

fn ratio_oracle(value: Decimal, increment: Decimal) -> Result<i64, RatioOracleError> {
    let numerator = BigInt::from(value.mantissa()) * BigInt::from(10_u8).pow(increment.scale());
    let denominator = BigInt::from(increment.mantissa()) * BigInt::from(10_u8).pow(value.scale());
    if &numerator % &denominator != BigInt::from(0_u8) {
        return Err(RatioOracleError::Inexact);
    }
    i64::try_from(numerator / denominator).map_err(|_| RatioOracleError::Overflow)
}

fn is_power_of_ten(mut value: u64) -> bool {
    while value > 1 && value.is_multiple_of(10) {
        value /= 10;
    }
    value == 1
}

fn product_oracle(count: i64, increment: Decimal) -> Option<Decimal> {
    normalized_big_decimal(
        BigInt::from(count) * BigInt::from(increment.mantissa()),
        increment.scale(),
    )
}

fn notional_oracle(
    price_ticks: i64,
    tick: Decimal,
    quantity_lots: i64,
    lot: Decimal,
) -> Option<Decimal> {
    normalized_big_decimal(
        BigInt::from(price_ticks)
            * BigInt::from(tick.mantissa())
            * BigInt::from(quantity_lots)
            * BigInt::from(lot.mantissa()),
        tick.scale().checked_add(lot.scale())?,
    )
}

fn sum_oracle(left: Decimal, right: Decimal, subtract: bool) -> Option<Decimal> {
    let scale = left.scale().max(right.scale());
    let left = BigInt::from(left.mantissa()) * BigInt::from(10_u8).pow(scale - left.scale());
    let right = BigInt::from(right.mantissa()) * BigInt::from(10_u8).pow(scale - right.scale());
    normalized_big_decimal(if subtract { left - right } else { left + right }, scale)
}

fn normalized_big_decimal(mut mantissa: BigInt, mut scale: u32) -> Option<Decimal> {
    while scale > 0 && &mantissa % 10_u8 == BigInt::from(0_u8) {
        mantissa /= 10_u8;
        scale -= 1;
    }
    let maximum = BigInt::from(MAX_DECIMAL_MANTISSA);
    if mantissa > maximum || mantissa < -BigInt::from(MAX_DECIMAL_MANTISSA) {
        return None;
    }
    let mantissa = i128::try_from(mantissa).ok()?;
    Decimal::try_from_i128_with_scale(mantissa, scale).ok()
}

#[test]
fn power_of_ten_rejects_unrepresentable_decimal_scale() {
    assert_eq!(
        TickSize::power_of_ten(Decimal::MAX_SCALE + 1),
        Err(FinancialError::UnsupportedScale {
            scale: Decimal::MAX_SCALE + 1,
            max: Decimal::MAX_SCALE,
        })
    );
}

proptest! {
    #![proptest_config(Config {
        failure_persistence: None,
        ..Config::default()
    })]

    #[test]
    fn checked_tick_round_trip(ticks in -1_000_000_i64..1_000_000, scale in 0_u32..8) {
        let tick_result = TickSize::power_of_ten(scale);
        prop_assert!(tick_result.is_ok());
        let Some(tick) = tick_result.ok() else {
            return Ok(());
        };
        let price = PriceTicks::new(ticks);
        let result = price.checked_to_decimal(tick)
            .and_then(|value| PriceTicks::try_from_decimal(value, tick));
        prop_assert_eq!(result, Ok(price));
    }

    #[test]
    fn checked_price_add_matches_primitive_checked_add(
        left in any::<i64>(),
        right in any::<i64>(),
    ) {
        let actual = PriceTicks::new(left).checked_add(PriceTicks::new(right));
        match left.checked_add(right) {
            Some(expected) => prop_assert_eq!(actual, Ok(PriceTicks::new(expected))),
            None => prop_assert_eq!(actual, Err(PriceError::Overflow)),
        }
    }

    #[test]
    fn checked_lot_round_trip(lots in 0_i64..1_000_000, scale in 0_u32..8) {
        let increment_result = Decimal::try_new(1, scale)
            .map_err(|_| FinancialError::UnsupportedScale {
                scale,
                max: Decimal::MAX_SCALE,
            })
            .and_then(LotSize::try_from_decimal);
        prop_assert!(increment_result.is_ok());
        let Some(increment) = increment_result.ok() else {
            return Ok(());
        };
        let quantity_result = QuantityLots::new(lots);
        prop_assert!(quantity_result.is_ok());
        let Some(quantity) = quantity_result.ok() else {
            return Ok(());
        };
        let result = quantity.checked_to_decimal(increment)
            .and_then(|value| QuantityLots::try_from_decimal(value, increment));
        prop_assert_eq!(result, Ok(quantity));
    }

    #[test]
    fn exact_price_division_matches_a_full_range_integer_rational_oracle(
        value_high in any::<u32>(),
        value_low in any::<u64>(),
        value_negative in any::<bool>(),
        value_scale in 0_u32..=Decimal::MAX_SCALE,
        increment_high in any::<u32>(),
        increment_low in any::<u64>(),
        increment_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        let Some(value) = decimal_from_parts(
            value_high,
            value_low,
            value_negative,
            value_scale,
        ) else {
            return Ok(());
        };
        let Some(increment) = positive_decimal_from_parts(
            increment_high,
            increment_low,
            increment_scale,
        ) else {
            return Ok(());
        };
        let tick_result = TickSize::try_from_decimal(increment);
        prop_assert!(tick_result.is_ok());
        let Some(tick) = tick_result.ok() else {
            return Ok(());
        };

        let actual = PriceTicks::try_from_decimal(value, tick);
        match ratio_oracle(value, tick.as_decimal()) {
            Ok(expected) => prop_assert_eq!(actual, Ok(PriceTicks::new(expected))),
            Err(RatioOracleError::Inexact) => {
                prop_assert_eq!(actual, Err(PriceError::InexactTick));
            }
            Err(RatioOracleError::Overflow) => {
                prop_assert_eq!(actual, Err(PriceError::Overflow));
            }
        }
    }

    #[test]
    fn exact_quantity_division_matches_a_nonnegative_integer_rational_oracle(
        value_high in any::<u32>(),
        value_low in any::<u64>(),
        value_scale in 0_u32..=Decimal::MAX_SCALE,
        increment in 1_u64..=1_000_000,
        increment_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        prop_assume!(!is_power_of_ten(increment));
        let Some(value) = decimal_from_parts(value_high, value_low, false, value_scale) else {
            return Ok(());
        };
        let increment_result = Decimal::try_from_i128_with_scale(
            i128::from(increment),
            increment_scale,
        );
        prop_assert!(increment_result.is_ok());
        let Some(increment) = increment_result.ok() else {
            return Ok(());
        };
        let lot_result = LotSize::try_from_decimal(increment);
        prop_assert!(lot_result.is_ok());
        let Some(lot) = lot_result.ok() else {
            return Ok(());
        };

        let actual = QuantityLots::try_from_decimal(value, lot);
        match ratio_oracle(value, lot.as_decimal()) {
            Ok(expected) if expected >= 0 => {
                let expected = QuantityLots::new(expected);
                prop_assert_eq!(actual, expected);
            }
            Ok(_) => prop_assert_eq!(actual, Err(QuantityError::NegativeQuantity)),
            Err(RatioOracleError::Inexact) => {
                prop_assert_eq!(actual, Err(QuantityError::InexactLot));
            }
            Err(RatioOracleError::Overflow) => {
                prop_assert_eq!(actual, Err(QuantityError::Overflow));
            }
        }
    }

    #[test]
    fn scaled_price_product_round_trips_or_returns_overflow_without_rounding(
        ticks in any::<i64>(),
        increment_high in any::<u32>(),
        increment_low in any::<u64>(),
        increment_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        let Some(increment) = positive_decimal_from_parts(
            increment_high,
            increment_low,
            increment_scale,
        ) else {
            return Ok(());
        };
        let tick_result = TickSize::try_from_decimal(increment);
        prop_assert!(tick_result.is_ok());
        let Some(tick) = tick_result.ok() else {
            return Ok(());
        };

        let actual = PriceTicks::new(ticks).checked_to_decimal(tick);
        match product_oracle(ticks, tick.as_decimal()) {
            Some(expected) => {
                prop_assert_eq!(actual, Ok(expected));
                prop_assert_eq!(
                    PriceTicks::try_from_decimal(expected, tick),
                    Ok(PriceTicks::new(ticks)),
                );
            }
            None => prop_assert_eq!(actual, Err(PriceError::Overflow)),
        }
    }

    #[test]
    fn scaled_nonnegative_quantity_round_trips_or_returns_exact_overflow(
        lots in 0_i64..=i64::MAX,
        increment in 2_u64..=1_000_000,
        increment_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        prop_assume!(!is_power_of_ten(increment));
        let increment_result = Decimal::try_from_i128_with_scale(
            i128::from(increment),
            increment_scale,
        );
        prop_assert!(increment_result.is_ok());
        let Some(increment) = increment_result.ok() else {
            return Ok(());
        };
        let lot_result = LotSize::try_from_decimal(increment);
        prop_assert!(lot_result.is_ok());
        let Some(lot) = lot_result.ok() else {
            return Ok(());
        };
        let quantity_result = QuantityLots::new(lots);
        prop_assert!(quantity_result.is_ok());
        let Some(quantity) = quantity_result.ok() else {
            return Ok(());
        };

        let actual = quantity.checked_to_decimal(lot);
        match product_oracle(lots, lot.as_decimal()) {
            Some(expected) => {
                prop_assert_eq!(actual, Ok(expected));
                prop_assert_eq!(
                    QuantityLots::try_from_decimal(expected, lot),
                    Ok(quantity),
                );
            }
            None => prop_assert_eq!(actual, Err(QuantityError::Overflow)),
        }
    }

    #[test]
    fn notional_matches_a_single_exact_integer_product_without_intermediate_rounding(
        price_ticks in any::<i64>(),
        quantity_lots in 0_i64..=i64::MAX,
        tick_mantissa in 2_i64..=1_000_000,
        tick_scale in 0_u32..=Decimal::MAX_SCALE,
        lot_mantissa in 2_i64..=1_000_000,
        lot_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        prop_assume!(!is_power_of_ten(tick_mantissa.unsigned_abs()));
        prop_assume!(!is_power_of_ten(lot_mantissa.unsigned_abs()));
        let tick_decimal = Decimal::try_new(tick_mantissa, tick_scale);
        let lot_decimal = Decimal::try_new(lot_mantissa, lot_scale);
        prop_assert!(tick_decimal.is_ok());
        prop_assert!(lot_decimal.is_ok());
        let (Some(tick_decimal), Some(lot_decimal)) =
            (tick_decimal.ok(), lot_decimal.ok())
        else {
            return Ok(());
        };
        let tick_result = TickSize::try_from_decimal(tick_decimal);
        let lot_result = LotSize::try_from_decimal(lot_decimal);
        prop_assert!(tick_result.is_ok());
        prop_assert!(lot_result.is_ok());
        let (Some(tick), Some(lot)) = (tick_result.ok(), lot_result.ok()) else {
            return Ok(());
        };
        let quantity_result = QuantityLots::new(quantity_lots);
        let currency_result = Currency::try_from("USD");
        prop_assert!(quantity_result.is_ok());
        prop_assert!(currency_result.is_ok());
        let (Some(quantity), Some(currency)) =
            (quantity_result.ok(), currency_result.ok())
        else {
            return Ok(());
        };

        let actual = PriceTicks::new(price_ticks)
            .checked_mul_quantity(quantity, tick, lot, currency)
            .map(Money::amount);
        match notional_oracle(
            price_ticks,
            tick.as_decimal(),
            quantity_lots,
            lot.as_decimal(),
        ) {
            Some(expected) => prop_assert_eq!(actual, Ok(expected)),
            None => prop_assert_eq!(actual, Err(FinancialError::Overflow)),
        }
    }

    #[test]
    fn money_add_and_subtract_match_exact_full_scale_integer_oracles(
        left_high in any::<u32>(),
        left_low in any::<u64>(),
        left_negative in any::<bool>(),
        left_scale in 0_u32..=Decimal::MAX_SCALE,
        right_high in any::<u32>(),
        right_low in any::<u64>(),
        right_negative in any::<bool>(),
        right_scale in 0_u32..=Decimal::MAX_SCALE,
    ) {
        let Some(left) = decimal_from_parts(
            left_high,
            left_low,
            left_negative,
            left_scale,
        ) else {
            return Ok(());
        };
        let Some(right) = decimal_from_parts(
            right_high,
            right_low,
            right_negative,
            right_scale,
        ) else {
            return Ok(());
        };
        let currency_result = Currency::try_from("USD");
        prop_assert!(currency_result.is_ok());
        let Some(currency) = currency_result.ok() else {
            return Ok(());
        };
        let left_money = Money::new(left, currency);
        let right_money = Money::new(right, currency);

        match sum_oracle(left_money.amount(), right_money.amount(), false) {
            Some(expected) => prop_assert_eq!(
                left_money.checked_add(right_money).map(Money::amount),
                Ok(expected),
            ),
            None => prop_assert_eq!(
                left_money.checked_add(right_money),
                Err(FinancialError::Overflow),
            ),
        }
        match sum_oracle(left_money.amount(), right_money.amount(), true) {
            Some(expected) => prop_assert_eq!(
                left_money.checked_sub(right_money).map(Money::amount),
                Ok(expected),
            ),
            None => prop_assert_eq!(
                left_money.checked_sub(right_money),
                Err(FinancialError::Overflow),
            ),
        }
    }
}
