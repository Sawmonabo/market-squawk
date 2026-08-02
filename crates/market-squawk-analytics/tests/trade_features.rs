use std::num::NonZeroUsize;

use market_squawk_analytics::{TradeFeatureView, aggressor_imbalance};
use market_squawk_domain::{AggressorSide, PriceTicks, QuantityLots, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn aggressor_imbalance_is_exact_and_unknown_volume_is_not_misclassified() -> TestResult {
    let trades = [
        TradeFeatureView::try_new(
            PriceTicks::new(100),
            QuantityLots::new(12)?,
            AggressorSide::Buy,
            Timestamp::from_unix_nanos(1),
        )?,
        TradeFeatureView::try_new(
            PriceTicks::new(101),
            QuantityLots::new(4)?,
            AggressorSide::Sell,
            Timestamp::from_unix_nanos(2),
        )?,
        TradeFeatureView::try_new(
            PriceTicks::new(102),
            QuantityLots::new(50)?,
            AggressorSide::Unknown,
            Timestamp::from_unix_nanos(3),
        )?,
    ];

    let value = aggressor_imbalance(
        &trades,
        NonZeroUsize::new(trades.len()).ok_or("trade bound")?,
    )?
    .ready_value()
    .ok_or("missing aggressor imbalance")?;

    assert_eq!((value.numerator(), value.denominator().get()), (1, 2));
    Ok(())
}
