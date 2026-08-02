use std::num::NonZeroUsize;

use market_squawk_analytics::{
    FeatureValidity, LiquidityBookView, PriceLevelView, estimate_market_order,
};
use market_squawk_domain::{OrderSide, PriceTicks, QuantityLots, Timestamp};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn liquidity_walk_is_side_aware_exact_and_fail_closed_on_insufficient_depth() -> TestResult {
    let bids = [
        PriceLevelView::try_new(PriceTicks::new(100), QuantityLots::new(10)?)?,
        PriceLevelView::try_new(PriceTicks::new(99), QuantityLots::new(20)?)?,
    ];
    let asks = [
        PriceLevelView::try_new(PriceTicks::new(101), QuantityLots::new(10)?)?,
        PriceLevelView::try_new(PriceTicks::new(102), QuantityLots::new(20)?)?,
    ];
    let book = LiquidityBookView::try_new(
        &bids,
        &asks,
        NonZeroUsize::new(2).ok_or("depth")?,
        Timestamp::from_unix_nanos(50),
    )?;

    let buy = estimate_market_order(book, OrderSide::Buy, QuantityLots::new(15)?)?;
    assert_eq!(buy.available_quantity().ready_value(), Some(30));
    let average = buy
        .weighted_fill_price()
        .ready_value()
        .ok_or("weighted price")?;
    assert_eq!((average.numerator(), average.denominator().get()), (304, 3));
    let slippage = buy
        .slippage_basis_points()
        .ready_value()
        .ok_or("slippage")?;
    assert_eq!(
        (slippage.numerator(), slippage.denominator().get()),
        (10_000, 303)
    );

    let sell = estimate_market_order(book, OrderSide::Sell, QuantityLots::new(15)?)?;
    let sell_average = sell
        .weighted_fill_price()
        .ready_value()
        .ok_or("sell weighted price")?;
    assert_eq!(
        (sell_average.numerator(), sell_average.denominator().get()),
        (299, 3)
    );
    let sell_slippage = sell
        .slippage_basis_points()
        .ready_value()
        .ok_or("sell slippage")?;
    assert_eq!(
        (sell_slippage.numerator(), sell_slippage.denominator().get()),
        (100, 3)
    );

    let unavailable = estimate_market_order(book, OrderSide::Buy, QuantityLots::new(31)?)?;
    assert_eq!(
        unavailable.weighted_fill_price().validity(),
        FeatureValidity::Unavailable
    );
    assert_eq!(unavailable.weighted_fill_price().value(), None);
    assert_eq!(unavailable.slippage_basis_points().value(), None);
    Ok(())
}
