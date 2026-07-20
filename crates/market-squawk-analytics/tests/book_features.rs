use std::num::NonZeroUsize;

use market_squawk_analytics::{
    BookDepthView, FeatureValidity, PriceLevelView, TopOfBookView, depth_weighted_price,
    order_flow_imbalance, top_of_book_features,
};
use market_squawk_domain::{PriceTicks, QuantityLots, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn book_features_are_exact_and_crossed_books_never_publish_values() -> TestResult {
    let at = Timestamp::from_unix_nanos(1_000);
    let top = TopOfBookView::try_new(
        PriceTicks::new(10_000),
        QuantityLots::new(20)?,
        PriceTicks::new(10_002),
        QuantityLots::new(10)?,
        at,
    )?;
    let features = top_of_book_features(top)?;
    assert_eq!(features.spread().ready_value(), Some(PriceTicks::new(2)));
    assert_eq!(
        features
            .midpoint()
            .ready_value()
            .map(|value| value.half_ticks()),
        Some(20_002)
    );
    let microprice = features
        .microprice()
        .ready_value()
        .ok_or("missing microprice")?;
    assert_eq!(
        (microprice.numerator(), microprice.denominator().get()),
        (30_004, 3)
    );
    let imbalance = features
        .book_imbalance()
        .ready_value()
        .ok_or("missing imbalance")?;
    assert_eq!(
        (imbalance.numerator(), imbalance.denominator().get()),
        (1, 3)
    );

    let previous = TopOfBookView::try_new(
        PriceTicks::new(9_999),
        QuantityLots::new(5)?,
        PriceTicks::new(10_003),
        QuantityLots::new(7)?,
        Timestamp::from_unix_nanos(999),
    )?;
    assert_eq!(order_flow_imbalance(previous, top)?.ready_value(), Some(10));

    let bids = [
        PriceLevelView::try_new(PriceTicks::new(10_000), QuantityLots::new(20)?)?,
        PriceLevelView::try_new(PriceTicks::new(9_999), QuantityLots::new(10)?)?,
    ];
    let asks = [
        PriceLevelView::try_new(PriceTicks::new(10_002), QuantityLots::new(10)?)?,
        PriceLevelView::try_new(PriceTicks::new(10_003), QuantityLots::new(10)?)?,
    ];
    let depth = BookDepthView::try_new(&bids, &asks, NonZeroUsize::new(2).ok_or("depth")?, at)?;
    let weighted = depth_weighted_price(depth)?
        .ready_value()
        .ok_or("missing depth price")?;
    assert_eq!(
        (weighted.numerator(), weighted.denominator().get()),
        (50_004, 5)
    );

    let crossed = TopOfBookView::try_new(
        PriceTicks::new(10_003),
        QuantityLots::new(1)?,
        PriceTicks::new(10_002),
        QuantityLots::new(1)?,
        at,
    )?;
    let invalid = top_of_book_features(crossed)?;
    assert_eq!(invalid.spread().validity(), FeatureValidity::Unavailable);
    assert_eq!(invalid.spread().value(), None);
    assert_eq!(invalid.midpoint().value(), None);
    assert_eq!(invalid.microprice().value(), None);

    let maximum_quantity = QuantityLots::new(i64::MAX)?;
    let overflow_bids = [
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 4), maximum_quantity)?,
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 5), maximum_quantity)?,
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 6), maximum_quantity)?,
    ];
    let overflow_asks = [
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 3), maximum_quantity)?,
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 2), maximum_quantity)?,
        PriceLevelView::try_new(PriceTicks::new(i64::MAX - 1), maximum_quantity)?,
    ];
    let overflow_depth = BookDepthView::try_new(
        &overflow_bids,
        &overflow_asks,
        NonZeroUsize::new(3).ok_or("overflow depth")?,
        at,
    )?;
    let overflow = depth_weighted_price(overflow_depth)?;
    assert_eq!(overflow.validity(), FeatureValidity::Overflow);
    assert_eq!(overflow.value(), None);
    Ok(())
}
