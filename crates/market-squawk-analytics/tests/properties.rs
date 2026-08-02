use market_squawk_analytics::{
    AnalyticsError, DatedStatisticalInput, StatisticalInput, StatisticalScale, StatisticalUnit,
    correlation, simple_returns,
};
use market_squawk_domain::{Currency, Timestamp};
use proptest::prelude::*;

proptest! {
    #[test]
    fn price_scaling_preserves_simple_returns(
        first in 1_i32..10_000,
        second in 1_i32..10_000,
        scale in 1_i32..10_000,
    ) {
        let usd = Currency::try_from("USD")?;
        let make = |at, value| -> Result<DatedStatisticalInput, AnalyticsError> {
            Ok(DatedStatisticalInput::new(
                Timestamp::from_unix_nanos(at),
                StatisticalInput::try_new(
                    f64::from(value),
                    StatisticalUnit::Currency(usd),
                    StatisticalScale::Unit,
                )?,
            ))
        };
        let base = simple_returns(&[make(1, first)?, make(2, second)?])?;
        let scaled_first = first * scale;
        let scaled_second = second * scale;
        let scaled = simple_returns(&[
            make(1, scaled_first)?,
            make(2, scaled_second)?,
        ])?;
        prop_assert!((base.values()[0].value() - scaled.values()[0].value()).abs() < 1e-12);
    }

    #[test]
    fn correlation_is_invariant_to_positive_affine_transforms(
        xs in prop::collection::vec(-1_000_i16..1_000, 3..64),
        offset in -1_000_i16..1_000,
        scale in 1_i16..100,
    ) {
        prop_assume!(xs.iter().any(|value| value != &xs[0]));
        let x = xs.iter()
            .map(|value| StatisticalInput::try_new(
                f64::from(*value), StatisticalUnit::Return, StatisticalScale::Unit))
            .collect::<Result<Vec<_>, _>>()?;
        let y = xs.iter()
            .map(|value| StatisticalInput::try_new(
                f64::from(*value) * f64::from(scale) + f64::from(offset),
                StatisticalUnit::Return,
                StatisticalScale::Unit))
            .collect::<Result<Vec<_>, _>>()?;
        prop_assert!((correlation(&x, &y)?.value() - 1.0).abs() < 1e-10);
    }
}
