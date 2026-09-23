#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed assertions must terminate this test binary"
)]

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::Arc;

use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money, OrderId,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, RuleVersion, SourceIdentifier,
    StrategyId, TickSize, TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, AccountRiskReservation, AccountRiskViolation, ApprovedOrder,
    ExecutionAuditConfig, ExecutionAuditWriter, MarketRiskInput, OrderIntent, OrderIntentInput,
    PaperRiskAdvisoryDraft, PreAuthorityRiskOutcome, RiskAdvisoryAuthority, RiskAdvisoryCheck,
    RiskAdvisoryEvidence, RiskAdvisoryOutcome, RiskLimits, RiskLimitsInput, RiskOutcome,
    RiskPolicyIdentity, RiskRejectionCode, RiskService, RiskServiceConfig,
};
use rust_decimal::Decimal;
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(
    RiskAdvisoryEvidence:
        Into<AccountRiskReservation>,
        Into<ApprovedOrder>,
        Into<PreAuthorityRiskOutcome>,
        Into<RiskOutcome>
);

#[test]
fn risk_returns_stably_ordered_source_market_and_account_reasons_before_mutation() {
    let fixture = Fixture::new();
    let account_config = AccountCoordinatorConfig {
        maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
            .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
        ..AccountCoordinatorConfig::default()
    };
    let coordinator = Arc::new(
        AccountRiskCoordinator::try_new(account_config, [fixture.account(Decimal::new(50, 0))])
            .unwrap_or_else(|error| panic!("valid coordinator: {error}")),
    );
    let (audit, _audit_reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::new(8)
            .unwrap_or_else(|| panic!("fixture audit count is nonzero")),
        maximum_bytes: NonZeroU32::new(8_192)
            .unwrap_or_else(|| panic!("fixture audit bytes are nonzero")),
    })
    .unwrap_or_else(|error| panic!("valid audit fixture: {error}"));
    let service = RiskService::try_new(
        coordinator,
        super::portfolio_state_integration::portfolio_capability(
            fixture.account_id,
            fixture.usd,
            Decimal::new(50, 0),
        )
        .unwrap_or_else(|error| panic!("valid portfolio fixture: {error}")),
        fixture.limits(),
        audit,
        RiskServiceConfig {
            policy: RiskPolicyIdentity::new(
                &SourceIdentifier::try_from("risk/default")
                    .unwrap_or_else(|error| panic!("valid policy identity: {error}")),
                RuleVersion::new(1).unwrap_or_else(|error| panic!("valid policy version: {error}")),
            ),
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: std::time::Duration::from_secs(1),
        },
    )
    .unwrap_or_else(|error| panic!("valid risk service: {error}"));
    let intent = fixture.intent(1, 1, 100, 50);
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

    let overflow_intent = fixture.intent(2, i64::MAX, i64::MAX, 50);
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

    let loose_intent = fixture.intent(3, 1, 100, 101);
    let PreAuthorityRiskOutcome::Rejected(loose) =
        service.evaluate_pre_authority(&loose_intent, &current_market)
    else {
        panic!("intent slippage above policy must be rejected before reservation");
    };
    assert!(
        loose
            .reasons()
            .contains(&RiskRejectionCode::PolicySlippageLimit)
    );

    let sell = fixture.sell_market_intent(4, 10, 100);
    let sell_market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        PriceTicks::new(100),
        PriceTicks::new(100),
    )
    .unwrap_or_else(|error| panic!("valid sell market input: {error}"));
    let PreAuthorityRiskOutcome::Rejected(sell_rejection) =
        service.evaluate_pre_authority(&sell, &sell_market)
    else {
        panic!("sell exposure must reserve through its enforceable upper execution-price bound");
    };
    assert!(
        sell_rejection
            .reasons()
            .contains(&RiskRejectionCode::Account(
                AccountRiskViolation::OrderNotionalLimit
            ))
    );
}

#[test]
fn paper_risk_advisory_is_analysis_only_and_preserves_exact_state() {
    let fixture = Fixture::new();
    let account_config = AccountCoordinatorConfig {
        maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
            .unwrap_or_else(|| panic!("fixture intent lifetime is nonzero")),
        ..AccountCoordinatorConfig::default()
    };
    let mut account = fixture.account(Decimal::new(500, 0));
    account.positions.clear();
    account.position_cost_basis.clear();
    let coordinator = Arc::new(
        AccountRiskCoordinator::try_new(account_config, [account])
            .unwrap_or_else(|error| panic!("valid coordinator: {error}")),
    );
    let (audit, mut audit_reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::new(8)
            .unwrap_or_else(|| panic!("fixture audit count is nonzero")),
        maximum_bytes: NonZeroU32::new(8_192)
            .unwrap_or_else(|| panic!("fixture audit bytes are nonzero")),
    })
    .unwrap_or_else(|error| panic!("valid audit fixture: {error}"));
    let limits = fixture.limits();
    let limits_digest = limits.digest();
    let policy = RiskPolicyIdentity::new(
        &SourceIdentifier::try_from("risk/default")
            .unwrap_or_else(|error| panic!("valid policy identity: {error}")),
        RuleVersion::new(1).unwrap_or_else(|error| panic!("valid policy version: {error}")),
    );
    let service = RiskService::try_new(
        Arc::clone(&coordinator),
        super::portfolio_state_integration::portfolio_capability(
            fixture.account_id,
            fixture.usd,
            Decimal::new(500, 0),
        )
        .unwrap_or_else(|error| panic!("valid portfolio fixture: {error}")),
        limits,
        audit,
        RiskServiceConfig {
            policy,
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: std::time::Duration::from_secs(1),
        },
    )
    .unwrap_or_else(|error| panic!("valid risk service: {error}"));
    let intent = fixture.intent(5, 1, 100, 50);
    let market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        PriceTicks::new(100),
        PriceTicks::new(100),
    )
    .unwrap_or_else(|error| panic!("valid advisory market input: {error}"));
    let same_market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        PriceTicks::new(100),
        PriceTicks::new(100),
    )
    .unwrap_or_else(|error| panic!("valid duplicate market input: {error}"));
    let changed_market = MarketRiskInput::try_new(
        fixture.terms,
        DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        PriceTicks::new(101),
        PriceTicks::new(100),
    )
    .unwrap_or_else(|error| panic!("valid changed market input: {error}"));
    assert_eq!(market.digest(), same_market.digest());
    assert_ne!(market.digest(), changed_market.digest());

    let account_before = coordinator
        .snapshot_recovery_state(fixture.account_id)
        .unwrap_or_else(|error| panic!("account state available before advisory: {error}"));
    let idempotency_before = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("idempotency state available before advisory: {error}"));
    assert_eq!(
        audit_reader
            .try_next_record()
            .unwrap_or_else(|error| panic!("audit reader available before advisory: {error}")),
        None
    );

    let generation = service
        .current_advisory_generation(&intent)
        .unwrap_or_else(|error| panic!("current advisory generation available: {error}"));
    let draft = PaperRiskAdvisoryDraft::new(&intent, market, &generation);
    let evidence = service
        .evaluate_advisory(&draft)
        .unwrap_or_else(|error| panic!("current advisory evaluation succeeds: {error}"));

    assert_eq!(evidence.intent_digest(), intent.digest());
    assert_eq!(evidence.generation(), &generation);
    assert_eq!(evidence.policy_digest(), policy.digest());
    assert_eq!(evidence.ruleset_version(), policy.ruleset_version());
    assert_eq!(evidence.limits_digest(), limits_digest);
    assert_eq!(evidence.market_input_digest(), market.digest());
    assert_ne!(evidence.digest(), [0; 32]);
    assert!(!evidence.kill_switch());
    assert_eq!(
        evidence.checks_evaluated(),
        [
            RiskAdvisoryCheck::Policy,
            RiskAdvisoryCheck::Market,
            RiskAdvisoryCheck::AccountGeneration,
            RiskAdvisoryCheck::PositionGeneration,
            RiskAdvisoryCheck::AccountLimits,
            RiskAdvisoryCheck::StateRecheck,
        ]
    );
    assert!(evidence.checks_unavailable().is_empty());
    assert!(evidence.reasons().is_empty());
    assert_eq!(
        evidence.outcome(),
        RiskAdvisoryOutcome::WouldPassAtEvaluation
    );
    assert_eq!(evidence.authority(), RiskAdvisoryAuthority::AnalysisOnly);
    assert!(evidence.valid_until() >= evidence.evaluated_at());

    let changed_draft = PaperRiskAdvisoryDraft::new(&intent, changed_market, &generation);
    let changed_evidence = service
        .evaluate_advisory(&changed_draft)
        .unwrap_or_else(|error| panic!("changed market advisory remains evaluable: {error}"));
    assert_eq!(
        changed_evidence.market_input_digest(),
        changed_market.digest()
    );
    assert_ne!(
        changed_evidence.market_input_digest(),
        evidence.market_input_digest()
    );
    assert_ne!(changed_evidence.digest(), evidence.digest());

    let account_after = coordinator
        .snapshot_recovery_state(fixture.account_id)
        .unwrap_or_else(|error| panic!("account state available after advisory: {error}"));
    let idempotency_after = coordinator
        .snapshot_idempotency(fixture.account_id)
        .unwrap_or_else(|error| panic!("idempotency state available after advisory: {error}"));
    let generation_after = service
        .current_advisory_generation(&intent)
        .unwrap_or_else(|error| panic!("advisory generation unchanged: {error}"));
    assert_eq!(account_after, account_before);
    assert_eq!(idempotency_after, idempotency_before);
    assert_eq!(generation_after, generation);
    assert_eq!(
        audit_reader
            .try_next_record()
            .unwrap_or_else(|error| panic!("audit reader available after advisory: {error}")),
        None
    );

    let PreAuthorityRiskOutcome::Reserved(reservation) =
        service.evaluate_pre_authority(&intent, &market)
    else {
        panic!("separate current-authority risk must still be able to reserve capacity");
    };
    drop(reservation);
}

pub(super) struct Fixture {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) terms: InstrumentExecutionTerms,
    pub(super) usd: Currency,
}

impl Fixture {
    pub(super) fn new() -> Self {
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

    pub(super) fn account(&self, capital: Decimal) -> AccountBootstrap {
        AccountBootstrap {
            account_id: self.account_id,
            revision: NonZeroU64::new(1).unwrap_or_else(|| panic!("fixture revision is nonzero")),
            eligible: true,
            cash: Money::new(capital, self.usd),
            capital: Money::new(capital, self.usd),
            peak_capital: Money::new(capital.max(Decimal::new(100, 0)), self.usd),
            gross_exposure: Money::new(Decimal::ZERO, self.usd),
            realized_pnl: Money::new(Decimal::ZERO, self.usd),
            realized_loss: Money::new(Decimal::ZERO, self.usd),
            positions: vec![(self.instrument_id, 0)],
            position_cost_basis: vec![(self.instrument_id, Money::new(Decimal::ZERO, self.usd))],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }
    }

    pub(super) fn limits(&self) -> RiskLimits {
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

    pub(super) fn intent(
        &self,
        suffix: u8,
        quantity: i64,
        limit_price: i64,
        maximum_slippage: i32,
    ) -> OrderIntent {
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
            maximum_slippage: BasisPoints::new(maximum_slippage),
            required_quality: DataQuality::DirectVerified,
        })
        .unwrap_or_else(|error| panic!("valid intent fixture: {error}"))
    }

    fn sell_market_intent(&self, suffix: u8, quantity: i64, maximum_slippage: i32) -> OrderIntent {
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
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            quantity: QuantityLots::new(quantity)
                .unwrap_or_else(|error| panic!("valid quantity fixture: {error}")),
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::ImmediateOrCancel,
            signal_at: Timestamp::from_unix_nanos(1),
            expires_at: Timestamp::from_unix_nanos(i64::MAX),
            reason_codes: vec![
                OrderReasonCode::try_from("risk.test")
                    .unwrap_or_else(|error| panic!("valid reason fixture: {error}")),
            ],
            maximum_slippage: BasisPoints::new(maximum_slippage),
            required_quality: DataQuality::DirectVerified,
        })
        .unwrap_or_else(|error| panic!("valid intent fixture: {error}"))
    }
}
