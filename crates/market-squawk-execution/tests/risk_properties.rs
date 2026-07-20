#![allow(
    clippy::panic,
    reason = "invalid generated fixtures and failed property setup must terminate this test binary"
)]

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64};
use std::str::FromStr;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money, OrderId,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, StrategyId, TickSize,
    TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountRiskCoordinator, OrderIntent,
    OrderIntentInput, RiskLimits, RiskLimitsInput,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

proptest! {
    #[test]
    fn accepted_reservations_never_exceed_exact_aggregate_cash(
        quantities in prop::collection::vec(1_i64..50, 1..24)
    ) {
        let fixture = Fixture::new();
        let coordinator = AccountRiskCoordinator::try_new(
            AccountCoordinatorConfig::default(),
            [fixture.account()],
        ).unwrap_or_else(|error| panic!("valid coordinator: {error}"));
        let limits = fixture.limits();
        let mut retained = Vec::new();
        let mut accepted_notional = 0_i64;

        for (index, quantity) in quantities.into_iter().enumerate() {
            let intent = fixture.intent(index + 1, quantity);
            if let Ok(reservation) = coordinator.try_reserve(&intent, PriceTicks::new(10), &limits) {
                accepted_notional = accepted_notional
                    .checked_add(quantity * 10)
                    .unwrap_or_else(|| panic!("bounded fixture sum cannot overflow"));
                retained.push(reservation);
            }
        }

        prop_assert!(accepted_notional <= 1_000);
        prop_assert!(retained.iter().all(|reservation| reservation.validate_current().is_ok()));
    }
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

    fn account(&self) -> AccountBootstrap {
        let cash = Money::new(Decimal::new(1_000, 0), self.usd);
        AccountBootstrap {
            account_id: self.account_id,
            revision: NonZeroU64::new(1).unwrap_or_else(|| panic!("fixture revision is nonzero")),
            eligible: true,
            cash,
            capital: cash,
            peak_capital: cash,
            gross_exposure: Money::new(Decimal::ZERO, self.usd),
            realized_loss: Money::new(Decimal::ZERO, self.usd),
            positions: vec![(self.instrument_id, 0)],
        }
    }

    fn limits(&self) -> RiskLimits {
        let ceiling = Money::new(Decimal::new(1_000, 0), self.usd);
        RiskLimits::try_new(RiskLimitsInput {
            currency: self.usd,
            eligible_instruments: BTreeSet::from([self.instrument_id]),
            maximum_position_lots: 100,
            maximum_order_notional: ceiling,
            maximum_gross_exposure: ceiling,
            maximum_leverage: BasisPoints::new(10_000),
            minimum_capital: Money::new(Decimal::ONE, self.usd),
            maximum_loss: ceiling,
            maximum_drawdown: ceiling,
            maximum_fee: BasisPoints::new(0),
            maximum_price_deviation: BasisPoints::new(100),
            maximum_slippage: BasisPoints::new(100),
            maximum_orders_per_window: NonZeroU32::new(128)
                .unwrap_or_else(|| panic!("fixture rate count is nonzero")),
            order_rate_window_nanos: 1_000_000_000,
            reservation_ttl_nanos: 1_000_000_000,
            allow_short: false,
            kill_switch: false,
        })
        .unwrap_or_else(|error| panic!("valid limits: {error}"))
    }

    fn intent(&self, suffix: usize, quantity: i64) -> OrderIntent {
        let order_id = format!("20000000-0000-0000-0000-{suffix:012}");
        OrderIntent::try_new(OrderIntentInput {
            order_id: OrderId::from_str(&order_id)
                .unwrap_or_else(|error| panic!("valid order fixture: {error}")),
            client_order_id: ClientOrderId::try_from(format!("property-{suffix}"))
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
            limit_price: Some(PriceTicks::new(10)),
            stop_price: None,
            time_in_force: TimeInForce::Day,
            signal_at: Timestamp::from_unix_nanos(1),
            expires_at: Timestamp::from_unix_nanos(i64::MAX),
            reason_codes: vec![
                OrderReasonCode::try_from("property")
                    .unwrap_or_else(|error| panic!("valid reason fixture: {error}")),
            ],
            maximum_slippage: BasisPoints::new(10),
            required_quality: DataQuality::DirectVerified,
        })
        .unwrap_or_else(|error| panic!("valid intent fixture: {error}"))
    }
}
