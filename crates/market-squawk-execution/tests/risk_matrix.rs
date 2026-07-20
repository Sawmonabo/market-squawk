#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed assertions must terminate this test binary"
)]

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::sync::Arc;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money, OrderId,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TickSize,
    TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountRiskCoordinator, AccountRiskViolation,
    MarketRiskInput, OrderIntent, OrderIntentInput, PreAuthorityRiskOutcome, RiskLimits,
    RiskLimitsInput, RiskRejectionCode, RiskService,
};
use rust_decimal::Decimal;

#[test]
fn risk_returns_stably_ordered_source_market_and_account_reasons_before_mutation() {
    let fixture = Fixture::new();
    let coordinator = Arc::new(
        AccountRiskCoordinator::try_new(
            AccountCoordinatorConfig::default(),
            [fixture.account(Decimal::new(50, 0))],
        )
        .unwrap_or_else(|error| panic!("valid coordinator: {error}")),
    );
    let service = RiskService::new(coordinator, fixture.limits());
    let intent = fixture.intent(1, 1, 100);
    let market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectUnverified,
        false,
        false,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(2),
        PriceTicks::new(100),
        PriceTicks::new(100),
    )
    .unwrap_or_else(|error| panic!("structurally valid market input: {error}"));

    let PreAuthorityRiskOutcome::Rejected(rejection) =
        service.evaluate_pre_authority(&intent, &market)
    else {
        panic!("invalid source and underfunded account must be rejected");
    };
    let reasons = rejection.reasons();
    assert!(reasons.windows(2).all(|pair| pair[0] < pair[1]));
    for expected in [
        RiskRejectionCode::SourceQuality,
        RiskRejectionCode::SourceIneligible,
        RiskRejectionCode::SourceStale,
        RiskRejectionCode::InstrumentNotTrading,
        RiskRejectionCode::Account(AccountRiskViolation::InsufficientCash),
        RiskRejectionCode::Account(AccountRiskViolation::CapitalLimit),
    ] {
        assert!(
            reasons.contains(&expected),
            "missing {expected:?}: {reasons:?}"
        );
    }

    let overflow_intent = fixture.intent(2, i64::MAX, i64::MAX);
    let current_market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        PriceTicks::new(i64::MAX),
        PriceTicks::new(i64::MAX),
    )
    .unwrap_or_else(|error| panic!("valid overflow market input: {error}"));
    let PreAuthorityRiskOutcome::Rejected(overflow) =
        service.evaluate_pre_authority(&overflow_intent, &current_market)
    else {
        panic!("checked financial overflow must reject");
    };
    assert!(overflow.reasons().contains(&RiskRejectionCode::Account(
        AccountRiskViolation::ArithmeticOverflow
    )));
}

struct Fixture {
    account_id: AccountId,
    instrument_id: InstrumentId,
    terms: InstrumentExecutionTerms,
    usd: Currency,
}

impl Fixture {
    fn new() -> Self {
        let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid account fixture: {error}"));
        let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|error| panic!("valid instrument fixture: {error}"));
        let usd = Currency::try_from("USD")
            .unwrap_or_else(|error| panic!("valid currency fixture: {error}"));
        let terms = InstrumentExecutionTerms::try_new(
            instrument_id,
            InstrumentDefinitionRevision::try_from(1)
                .unwrap_or_else(|error| panic!("valid revision fixture: {error}")),
            TickSize::try_from_decimal(Decimal::ONE)
                .unwrap_or_else(|error| panic!("valid tick fixture: {error}")),
            LotSize::try_from_decimal(Decimal::ONE)
                .unwrap_or_else(|error| panic!("valid lot fixture: {error}")),
            usd,
            Denomination::Currency(usd),
            Decimal::ONE,
        )
        .unwrap_or_else(|error| panic!("valid terms fixture: {error}"));
        Self {
            account_id,
            instrument_id,
            terms,
            usd,
        }
    }

    fn account(&self, capital: Decimal) -> AccountBootstrap {
        AccountBootstrap {
            account_id: self.account_id,
            revision: NonZeroU64::new(1).unwrap_or_else(|| panic!("fixture revision is nonzero")),
            eligible: true,
            cash: Money::new(capital, self.usd),
            capital: Money::new(capital, self.usd),
            peak_capital: Money::new(Decimal::new(100, 0), self.usd),
            gross_exposure: Money::new(Decimal::ZERO, self.usd),
            realized_loss: Money::new(Decimal::ZERO, self.usd),
            positions: vec![(self.instrument_id, 0)],
        }
    }

    fn limits(&self) -> RiskLimits {
        RiskLimits::try_new(RiskLimitsInput {
            currency: self.usd,
            eligible_instruments: BTreeSet::from([self.instrument_id]),
            maximum_position_lots: 100,
            maximum_order_notional: Money::new(Decimal::new(1_000, 0), self.usd),
            maximum_gross_exposure: Money::new(Decimal::new(1_000, 0), self.usd),
            maximum_leverage: BasisPoints::new(20_000),
            minimum_capital: Money::new(Decimal::new(100, 0), self.usd),
            maximum_loss: Money::new(Decimal::new(1_000, 0), self.usd),
            maximum_drawdown: Money::new(Decimal::new(1_000, 0), self.usd),
            maximum_fee: BasisPoints::new(0),
            maximum_price_deviation: BasisPoints::new(100),
            maximum_slippage: BasisPoints::new(100),
            maximum_orders_per_window: NonZeroU32::new(8)
                .unwrap_or_else(|| panic!("fixture rate count is nonzero")),
            order_rate_window_nanos: 1_000_000_000,
            reservation_ttl_nanos: 1_000_000_000,
            allow_short: false,
            kill_switch: false,
        })
        .unwrap_or_else(|error| panic!("valid risk limits: {error}"))
    }

    fn intent(&self, suffix: u8, quantity: i64, limit_price: i64) -> OrderIntent {
        let order_id = format!("20000000-0000-0000-0000-{suffix:012}");
        OrderIntent::try_new(OrderIntentInput {
            order_id: OrderId::from_str(&order_id)
                .unwrap_or_else(|error| panic!("valid order fixture: {error}")),
            client_order_id: ClientOrderId::try_from(format!("risk-{suffix}"))
                .unwrap_or_else(|error| panic!("valid client-order fixture: {error}")),
            strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000001")
                .unwrap_or_else(|error| panic!("valid strategy fixture: {error}")),
            model_id: None,
            account_id: self.account_id,
            execution_terms: self.terms,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: QuantityLots::new(quantity)
                .unwrap_or_else(|error| panic!("valid quantity fixture: {error}")),
            limit_price: Some(PriceTicks::new(limit_price)),
            stop_price: None,
            time_in_force: TimeInForce::Day,
            signal_at: Timestamp::from_unix_nanos(1),
            expires_at: Timestamp::from_unix_nanos(i64::MAX),
            reason_codes: vec![
                OrderReasonCode::try_from("risk.test")
                    .unwrap_or_else(|error| panic!("valid reason fixture: {error}")),
            ],
            maximum_slippage: BasisPoints::new(50),
            required_quality: DataQuality::DirectVerified,
        })
        .unwrap_or_else(|error| panic!("valid intent fixture: {error}"))
    }
}
