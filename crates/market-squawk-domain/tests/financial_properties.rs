use market_squawk_domain::{
    FinancialError, LotSize, PriceError, PriceTicks, QuantityLots, TickSize,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

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
}
