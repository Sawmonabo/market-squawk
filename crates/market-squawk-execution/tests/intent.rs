#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed assertions must terminate this test binary"
)]

use std::str::FromStr;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, ModelId,
    OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TickSize,
    TimeInForce, Timestamp,
};
use market_squawk_execution::{OrderIntent, OrderIntentError, OrderIntentInput};
use rust_decimal::Decimal;

fn input(order_type: OrderType) -> OrderIntentInput {
    let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000001")
        .unwrap_or_else(|error| panic!("valid fixture instrument: {error}"));
    let usd =
        Currency::try_from("USD").unwrap_or_else(|error| panic!("valid fixture currency: {error}"));
    let terms = InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(7)
            .unwrap_or_else(|error| panic!("valid fixture revision: {error}")),
        TickSize::try_from_decimal(Decimal::new(1, 2))
            .unwrap_or_else(|error| panic!("valid fixture tick: {error}")),
        LotSize::try_from_decimal(Decimal::ONE)
            .unwrap_or_else(|error| panic!("valid fixture lot: {error}")),
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )
    .unwrap_or_else(|error| panic!("valid fixture terms: {error}"));

    let (limit_price, stop_price, time_in_force) = match order_type {
        OrderType::Market => (None, None, TimeInForce::ImmediateOrCancel),
        OrderType::Limit => (Some(PriceTicks::new(10_000)), None, TimeInForce::Day),
        OrderType::Stop => (None, Some(PriceTicks::new(9_500)), TimeInForce::Day),
        OrderType::StopLimit => (
            Some(PriceTicks::new(9_400)),
            Some(PriceTicks::new(9_500)),
            TimeInForce::GoodTilCancelled,
        ),
    };

    OrderIntentInput {
        order_id: OrderId::from_str("20000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid fixture order: {error}")),
        client_order_id: ClientOrderId::try_from("strategy-a-0001")
            .unwrap_or_else(|error| panic!("valid fixture client order: {error}")),
        strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid fixture strategy: {error}")),
        model_id: Some(
            ModelId::from_str("40000000-0000-0000-0000-000000000001")
                .unwrap_or_else(|error| panic!("valid fixture model: {error}")),
        ),
        account_id: AccountId::from_str("50000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid fixture account: {error}")),
        execution_terms: terms,
        side: OrderSide::Buy,
        order_type,
        quantity: QuantityLots::new(10)
            .unwrap_or_else(|error| panic!("valid fixture quantity: {error}")),
        limit_price,
        stop_price,
        time_in_force,
        signal_at: Timestamp::from_unix_nanos(1_000),
        expires_at: Timestamp::from_unix_nanos(2_000),
        reason_codes: vec![
            OrderReasonCode::try_from("signal.momentum")
                .unwrap_or_else(|error| panic!("valid fixture reason: {error}")),
        ],
        maximum_slippage: BasisPoints::new(25),
        required_quality: DataQuality::DirectVerified,
    }
}

#[test]
fn order_type_price_and_time_in_force_matrix_is_closed() {
    for order_type in [
        OrderType::Market,
        OrderType::Limit,
        OrderType::Stop,
        OrderType::StopLimit,
    ] {
        OrderIntent::try_new(input(order_type))
            .unwrap_or_else(|error| panic!("valid {order_type:?} intent: {error}"));
    }

    let mut market_with_limit = input(OrderType::Market);
    market_with_limit.limit_price = Some(PriceTicks::new(1));
    assert_eq!(
        OrderIntent::try_new(market_with_limit),
        Err(OrderIntentError::UnexpectedLimitPrice)
    );

    let mut limit_without_limit = input(OrderType::Limit);
    limit_without_limit.limit_price = None;
    assert_eq!(
        OrderIntent::try_new(limit_without_limit),
        Err(OrderIntentError::MissingLimitPrice)
    );

    let mut stop_without_stop = input(OrderType::Stop);
    stop_without_stop.stop_price = None;
    assert_eq!(
        OrderIntent::try_new(stop_without_stop),
        Err(OrderIntentError::MissingStopPrice)
    );

    let mut stop_limit_without_limit = input(OrderType::StopLimit);
    stop_limit_without_limit.limit_price = None;
    assert_eq!(
        OrderIntent::try_new(stop_limit_without_limit),
        Err(OrderIntentError::MissingLimitPrice)
    );

    let mut persistent_market = input(OrderType::Market);
    persistent_market.time_in_force = TimeInForce::GoodTilCancelled;
    assert_eq!(
        OrderIntent::try_new(persistent_market),
        Err(OrderIntentError::UnsupportedTimeInForce)
    );

    let mut immediate_stop = input(OrderType::Stop);
    immediate_stop.time_in_force = TimeInForce::ImmediateOrCancel;
    assert_eq!(
        OrderIntent::try_new(immediate_stop),
        Err(OrderIntentError::UnsupportedTimeInForce)
    );
}

#[test]
fn intent_rejects_invalid_size_time_reasons_slippage_and_quality() {
    let mut zero = input(OrderType::Limit);
    zero.quantity = QuantityLots::new(0)
        .unwrap_or_else(|error| panic!("zero is domain-representable: {error}"));
    assert_eq!(
        OrderIntent::try_new(zero),
        Err(OrderIntentError::ZeroQuantity)
    );

    let mut chronology = input(OrderType::Limit);
    chronology.expires_at = chronology.signal_at;
    assert_eq!(
        OrderIntent::try_new(chronology),
        Err(OrderIntentError::InvalidChronology)
    );

    let mut reasons = input(OrderType::Limit);
    reasons.reason_codes.clear();
    assert_eq!(
        OrderIntent::try_new(reasons),
        Err(OrderIntentError::MissingReasonCode)
    );

    let mut slippage = input(OrderType::Limit);
    slippage.maximum_slippage = BasisPoints::new(-1);
    assert_eq!(
        OrderIntent::try_new(slippage),
        Err(OrderIntentError::NegativeMaximumSlippage)
    );

    let mut quality = input(OrderType::Limit);
    quality.required_quality = DataQuality::DirectUnverified;
    assert_eq!(
        OrderIntent::try_new(quality),
        Err(OrderIntentError::IneligibleRequiredQuality)
    );
}

#[test]
fn digest_is_stable_and_binds_every_order_field() {
    let baseline = OrderIntent::try_new(input(OrderType::Limit))
        .unwrap_or_else(|error| panic!("valid baseline intent: {error}"));
    let identical = OrderIntent::try_new(input(OrderType::Limit))
        .unwrap_or_else(|error| panic!("valid identical intent: {error}"));
    assert_eq!(baseline.digest(), identical.digest());

    let mut changed = input(OrderType::Limit);
    changed.maximum_slippage = BasisPoints::new(26);
    let changed = OrderIntent::try_new(changed)
        .unwrap_or_else(|error| panic!("valid changed intent: {error}"));
    assert_ne!(baseline.digest(), changed.digest());

    assert_eq!(baseline.quantity().get(), 10);
    assert_eq!(baseline.execution_terms().definition_revision().get(), 7);
    assert_eq!(baseline.reason_codes()[0].as_str(), "signal.momentum");
}
