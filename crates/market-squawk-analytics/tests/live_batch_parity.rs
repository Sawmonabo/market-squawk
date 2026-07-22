use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    Annualization, DatedStatisticalInput, MissingValuePolicy, RollingFeatureState,
    RollingWindowConfig, StatisticalInput, StatisticalScale, StatisticalUnit, TradeFeatureView,
    VarianceConvention, cumulative_return, simple_returns, volatility,
};
use market_squawk_domain::{AggressorSide, Currency, PriceTicks, QuantityLots, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn live_rolling_and_batch_return_kernels_share_identical_semantics() -> TestResult {
    let config = RollingWindowConfig::try_new(
        NonZeroUsize::new(3).ok_or("capacity")?,
        NonZeroUsize::new(3).ok_or("warm-up")?,
        NonZeroU64::new(10).ok_or("duration")?,
        NonZeroUsize::new(64 * 1024).ok_or("retained bytes")?,
    )?;
    let mut live = RollingFeatureState::try_new(config)?;
    let usd = Currency::try_from("USD")?;
    let mut batch_prices = Vec::new();
    let mut live_values = None;
    for (price, at) in [(100_i32, 1_i64), (110, 2), (99, 3)] {
        let timestamp = Timestamp::from_unix_nanos(at);
        live_values = Some(live.update(TradeFeatureView::try_new(
            PriceTicks::new(i64::from(price)),
            QuantityLots::new(1)?,
            AggressorSide::Buy,
            timestamp,
        )?)?);
        batch_prices.push(DatedStatisticalInput::new(
            timestamp,
            StatisticalInput::try_new(
                f64::from(price),
                StatisticalUnit::Currency(usd),
                StatisticalScale::Unit,
            )?,
        ));
    }
    let live_values = live_values.ok_or("missing live result")?;
    let batch_returns = simple_returns(&batch_prices)?;
    let batch_cumulative = cumulative_return(batch_returns.values())?;
    let periodic_returns = batch_returns.try_into_returns(Annualization::None)?;
    let batch_volatility = volatility(
        &periodic_returns,
        VarianceConvention::Population,
        MissingValuePolicy::Reject,
    )?;
    let live_return = live_values
        .rolling_return()
        .ready_value()
        .ok_or("missing live return")?;
    let live_volatility = live_values
        .rolling_volatility()
        .ready_value()
        .ok_or("missing live volatility")?;

    assert!((live_return.get() - batch_cumulative.value()).abs() < 1e-12);
    assert!((live_volatility.get() - batch_volatility.value()).abs() < 1e-12);
    Ok(())
}
