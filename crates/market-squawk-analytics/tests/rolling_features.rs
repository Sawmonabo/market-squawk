use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    FeatureValidity, RollingFeatureState, RollingWindowConfig, TradeFeatureView,
};
use market_squawk_domain::{AggressorSide, PriceTicks, QuantityLots, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn trade(
    price: i64,
    quantity: i64,
    at: i64,
) -> Result<TradeFeatureView, Box<dyn std::error::Error>> {
    Ok(TradeFeatureView::try_new(
        PriceTicks::new(price),
        QuantityLots::new(quantity)?,
        AggressorSide::Buy,
        Timestamp::from_unix_nanos(at),
    )?)
}

#[test]
fn rolling_state_is_preallocated_and_regression_clears_every_ready_value() -> TestResult {
    let config = RollingWindowConfig::try_new(
        NonZeroUsize::new(3).ok_or("capacity")?,
        NonZeroUsize::new(2).ok_or("warm-up")?,
        NonZeroU64::new(10).ok_or("duration")?,
        NonZeroUsize::new(64 * 1024).ok_or("retained bound")?,
    )?;
    let mut state = RollingFeatureState::try_new(config)?;
    let retained = state.retained_bytes();

    let warming = state.update(trade(100, 2, 10)?)?;
    assert_eq!(warming.vwap().validity(), FeatureValidity::WarmingUp);
    assert_eq!(warming.vwap().value(), None);

    let duplicate_time = state.update(trade(102, 2, 10)?)?;
    let vwap = duplicate_time.vwap().ready_value().ok_or("missing vwap")?;
    assert_eq!((vwap.numerator(), vwap.denominator().get()), (101, 1));
    assert_eq!(
        duplicate_time.volume_velocity().validity(),
        FeatureValidity::Unavailable
    );

    let ready = state.update(trade(104, 4, 15)?)?;
    assert_eq!(ready.momentum().ready_value(), Some(PriceTicks::new(4)));
    assert!(ready.rolling_return().ready_value().is_some());
    assert!(ready.rolling_volatility().ready_value().is_some());
    assert_eq!(state.retained_bytes(), retained);

    let capped = state.update(trade(106, 1, 16)?)?;
    assert_eq!(state.len(), 3);
    assert_eq!(capped.momentum().ready_value(), Some(PriceTicks::new(4)));
    assert_eq!(state.retained_bytes(), retained);

    let regressed = state.update(trade(99, 1, 15)?)?;
    assert_eq!(
        regressed.vwap().validity(),
        FeatureValidity::TimestampRegression
    );
    assert_eq!(regressed.vwap().value(), None);
    assert_eq!(regressed.momentum().value(), None);
    assert_eq!(regressed.rolling_return().value(), None);
    assert_eq!(state.len(), 0);
    assert_eq!(state.retained_bytes(), retained);

    let overflow_config = RollingWindowConfig::try_new(
        NonZeroUsize::new(3).ok_or("overflow capacity")?,
        NonZeroUsize::new(3).ok_or("overflow warm-up")?,
        NonZeroU64::new(10).ok_or("overflow duration")?,
        NonZeroUsize::new(64 * 1024).ok_or("overflow retained bound")?,
    )?;
    let mut overflow_state = RollingFeatureState::try_new(overflow_config)?;
    let _ = overflow_state.update(trade(i64::MAX, i64::MAX, 1)?)?;
    let _ = overflow_state.update(trade(i64::MAX, i64::MAX, 2)?)?;
    let overflow = overflow_state.update(trade(i64::MAX, i64::MAX, 3)?)?;
    assert_eq!(overflow.vwap().validity(), FeatureValidity::Overflow);
    assert_eq!(overflow.vwap().value(), None);
    assert!(overflow.volume_velocity().ready_value().is_some());
    Ok(())
}

#[test]
fn reset_reuses_the_ring_and_restarts_warm_up() -> TestResult {
    let config = RollingWindowConfig::try_new(
        NonZeroUsize::new(3).ok_or("capacity")?,
        NonZeroUsize::new(2).ok_or("warm-up")?,
        NonZeroU64::new(10).ok_or("duration")?,
        NonZeroUsize::new(64 * 1024).ok_or("retained bound")?,
    )?;
    let mut state = RollingFeatureState::try_new(config)?;
    let retained = state.retained_bytes();
    let _ = state.update(trade(100, 1, 1)?)?;
    assert!(
        state
            .update(trade(101, 1, 2)?)?
            .vwap()
            .ready_value()
            .is_some()
    );

    state.reset();
    assert!(state.is_empty());
    assert_eq!(state.retained_bytes(), retained);
    assert_eq!(
        state.update(trade(102, 1, 3)?)?.vwap().validity(),
        FeatureValidity::WarmingUp
    );
    Ok(())
}
