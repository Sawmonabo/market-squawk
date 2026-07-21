use std::num::NonZeroU64;
use std::str::FromStr;

use market_squawk_adapter_paper::{
    FeeSchedule, LiquidityRole, PaperAccountBootstrap, PaperExposureValuation, PaperLedger,
    PaperLedgerConfig, PaperOrderLifecycle, PaperOrderState, PaperSessionCalendarError,
    PaperStateError, PaperVenueSession, PaperVenueSessionCalendar,
};
use market_squawk_domain::{
    AccountId, Currency, Denomination, InstrumentDefinitionRevision, InstrumentExecutionTerms,
    InstrumentId, LotSize, Money, OrderId, OrderSide, PriceTicks, QuantityLots, RuleVersion,
    SourceIdentifier, TickSize, Timestamp, VenueId,
};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn day_session_calendar_is_exact_versioned_and_fails_closed_without_evidence() -> TestResult {
    let venue = VenueId::try_from("coinbase")?;
    let session = PaperVenueSession::try_new(
        SourceIdentifier::try_from("session-2026-07-20")?,
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(200),
    )?;
    let calendar = PaperVenueSessionCalendar::try_new(
        SourceIdentifier::try_from("coinbase-calendar")?,
        RuleVersion::new(7)?,
        venue.clone(),
        "America/New_York",
        vec![session],
    )?;
    assert_eq!(calendar.ruleset_version().get(), 7);
    assert_eq!(calendar.time_zone(), "America/New_York");
    assert_eq!(
        calendar.day_expires_at(&venue, Timestamp::from_unix_nanos(150))?,
        Timestamp::from_unix_nanos(199)
    );
    assert_eq!(
        calendar.day_expires_at(&venue, Timestamp::from_unix_nanos(200)),
        Err(PaperSessionCalendarError::MissingSessionEvidence)
    );
    assert_eq!(
        calendar.day_expires_at(
            &VenueId::try_from("kraken")?,
            Timestamp::from_unix_nanos(150)
        ),
        Err(PaperSessionCalendarError::VenueMismatch)
    );
    Ok(())
}

#[test]
fn lifecycle_transitions_are_monotonic_fill_safe_and_terminal() -> Result<(), PaperStateError> {
    let mut order = PaperOrderLifecycle::try_new(
        QuantityLots::new(10).map_err(|_| PaperStateError::InvalidQuantity)?,
    )?;
    order.accept(1)?;
    order.apply_fill(
        QuantityLots::new(4).map_err(|_| PaperStateError::InvalidQuantity)?,
        2,
    )?;
    assert_eq!(order.state(), PaperOrderState::PartiallyFilled);
    order.request_cancel(3)?;
    order.apply_fill(
        QuantityLots::new(2).map_err(|_| PaperStateError::InvalidQuantity)?,
        4,
    )?;
    assert_eq!(order.state(), PaperOrderState::CancelPending);
    order.confirm_cancel(5)?;
    assert_eq!(order.cumulative_filled().get(), 6);
    assert_eq!(order.revision(), 5);
    assert_eq!(order.accept(6), Err(PaperStateError::Terminal));

    let before = order.clone();
    assert_eq!(
        order.apply_fill(
            QuantityLots::new(5).map_err(|_| PaperStateError::InvalidQuantity)?,
            7
        ),
        Err(PaperStateError::Terminal)
    );
    assert_eq!(order, before);
    Ok(())
}

#[test]
fn rejection_expiry_full_fill_and_invalid_sequences_are_closed() -> Result<(), PaperStateError> {
    let quantity = QuantityLots::new(2).map_err(|_| PaperStateError::InvalidQuantity)?;
    let mut rejected = PaperOrderLifecycle::try_new(quantity)?;
    rejected.reject(1)?;
    assert_eq!(rejected.state(), PaperOrderState::Rejected);

    let mut expired = PaperOrderLifecycle::try_new(quantity)?;
    expired.expire(1)?;
    assert_eq!(expired.state(), PaperOrderState::Expired);

    let mut filled = PaperOrderLifecycle::try_new(quantity)?;
    filled.accept(1)?;
    filled.apply_fill(quantity, 2)?;
    assert_eq!(filled.state(), PaperOrderState::Filled);
    assert_eq!(filled.request_cancel(3), Err(PaperStateError::Terminal));

    let mut invalid = PaperOrderLifecycle::try_new(quantity)?;
    assert_eq!(invalid.accept(0), Err(PaperStateError::SequenceRegression));
    invalid.accept(1)?;
    let before = invalid.clone();
    assert_eq!(
        invalid.apply_fill(
            QuantityLots::new(3).map_err(|_| PaperStateError::InvalidQuantity)?,
            2
        ),
        Err(PaperStateError::Overfill)
    );
    assert_eq!(invalid, before);
    Ok(())
}

#[test]
fn ledger_is_exact_reserved_and_transactional_across_scaled_fills() -> TestResult {
    let usd = Currency::try_from("USD")?;
    assert!(FeeSchedule::try_new(10_001, 0, Money::new(Decimal::ZERO, usd), None, 2,).is_err());
    let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000001")?;
    let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000001")?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(Decimal::new(25, 2))?,
        LotSize::try_from_decimal(Decimal::new(5, 1))?,
        usd,
        Denomination::Currency(usd),
        Decimal::new(2, 0),
    )?;
    let fees = FeeSchedule::try_new(
        5,
        10,
        Money::new(Decimal::new(1, 2), usd),
        Some(Money::new(Decimal::new(100, 0), usd)),
        2,
    )?;
    let mut ledger = PaperLedger::try_new(
        PaperLedgerConfig {
            allow_short: false,
            exposure_valuation: PaperExposureValuation::OpenCost,
            maximum_accounts: 1,
            maximum_balances: 1,
            maximum_positions: 1,
            maximum_reservations: 4,
            fee_schedule: fees,
        },
        [PaperAccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: vec![Money::new(Decimal::new(100, 0), usd)],
            capital: Money::new(Decimal::new(100, 0), usd),
            peak_capital: Money::new(Decimal::new(100, 0), usd),
            gross_exposure: Money::new(Decimal::ZERO, usd),
            realized_pnl: Money::new(Decimal::ZERO, usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(instrument_id, 0)],
            position_cost_basis: vec![(instrument_id, Money::new(Decimal::ZERO, usd))],
        }],
    )?;
    let order_id = OrderId::from_str("20000000-0000-0000-0000-000000000001")?;
    let quantity = QuantityLots::new(4)?;
    ledger.reserve(
        order_id,
        account_id,
        terms,
        OrderSide::Buy,
        quantity,
        PriceTicks::new(10),
    )?;
    assert_eq!(
        ledger.available_cash(account_id, usd)?.amount(),
        Decimal::new(8999, 2)
    );

    let fill = ledger.apply_fill(
        order_id,
        terms,
        &[(PriceTicks::new(9), QuantityLots::new(2)?)],
        LiquidityRole::Taker,
    )?;
    assert_eq!(fill.notional().amount(), Decimal::new(450, 2));
    assert_eq!(fill.fee().amount(), Decimal::new(1, 2));
    assert_eq!(ledger.position_lots(account_id, instrument_id)?, 2);

    let wrong_revision = InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(2)?,
        terms.price_tick(),
        terms.lot_size(),
        usd,
        Denomination::Currency(usd),
        terms.contract_multiplier(),
    )?;
    let before = ledger.clone();
    assert!(
        ledger
            .apply_fill(
                order_id,
                wrong_revision,
                &[(PriceTicks::new(9), QuantityLots::new(1)?)],
                LiquidityRole::Maker,
            )
            .is_err()
    );
    assert_eq!(ledger, before);

    ledger.release(order_id)?;
    assert_eq!(
        ledger.available_cash(account_id, usd)?.amount(),
        Decimal::new(9549, 2)
    );
    assert_eq!(
        ledger.cash(account_id, usd)?.amount(),
        Decimal::new(9549, 2)
    );
    Ok(())
}

#[test]
fn minimum_fee_is_charged_once_across_partial_fills() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000011")?;
    let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000011")?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(Decimal::ONE)?,
        LotSize::try_from_decimal(Decimal::ONE)?,
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )?;
    let mut ledger = PaperLedger::try_new(
        PaperLedgerConfig {
            allow_short: false,
            exposure_valuation: PaperExposureValuation::OpenCost,
            maximum_accounts: 1,
            maximum_balances: 1,
            maximum_positions: 1,
            maximum_reservations: 2,
            fee_schedule: FeeSchedule::try_new(0, 0, Money::new(Decimal::new(2, 0), usd), None, 2)?,
        },
        [PaperAccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: vec![Money::new(Decimal::TEN, usd)],
            capital: Money::new(Decimal::TEN, usd),
            peak_capital: Money::new(Decimal::TEN, usd),
            gross_exposure: Money::new(Decimal::ZERO, usd),
            realized_pnl: Money::new(Decimal::ZERO, usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(instrument_id, 0)],
            position_cost_basis: vec![(instrument_id, Money::new(Decimal::ZERO, usd))],
        }],
    )?;
    let order_id = OrderId::from_str("20000000-0000-0000-0000-000000000011")?;
    ledger.reserve(
        order_id,
        account_id,
        terms,
        OrderSide::Buy,
        QuantityLots::new(2)?,
        PriceTicks::new(1),
    )?;

    let first = ledger.apply_fill(
        order_id,
        terms,
        &[(PriceTicks::new(1), QuantityLots::new(1)?)],
        LiquidityRole::Taker,
    )?;
    let second = ledger.apply_fill(
        order_id,
        terms,
        &[(PriceTicks::new(1), QuantityLots::new(1)?)],
        LiquidityRole::Taker,
    )?;

    assert_eq!(first.fee().amount(), Decimal::new(2, 0));
    assert_eq!(second.fee().amount(), Decimal::ZERO);
    assert_eq!(ledger.cash(account_id, usd)?.amount(), Decimal::new(6, 0));

    let sell_id = OrderId::from_str("20000000-0000-0000-0000-000000000014")?;
    ledger.reserve(
        sell_id,
        account_id,
        terms,
        OrderSide::Sell,
        QuantityLots::new(1)?,
        PriceTicks::new(1),
    )?;
    assert_eq!(
        ledger.available_cash(account_id, usd)?.amount(),
        Decimal::new(5, 0)
    );
    let sell = ledger.apply_fill(
        sell_id,
        terms,
        &[(PriceTicks::new(1), QuantityLots::new(1)?)],
        LiquidityRole::Taker,
    )?;
    assert_eq!(sell.fee().amount(), Decimal::new(2, 0));
    assert_eq!(ledger.cash(account_id, usd)?.amount(), Decimal::new(5, 0));
    Ok(())
}

#[test]
fn allowed_short_open_and_cover_are_accounted_without_exposure_underflow() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let account_id = AccountId::from_str("50000000-0000-0000-0000-000000000012")?;
    let instrument_id = InstrumentId::from_str("10000000-0000-0000-0000-000000000012")?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(Decimal::ONE)?,
        LotSize::try_from_decimal(Decimal::ONE)?,
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )?;
    let mut ledger = PaperLedger::try_new(
        PaperLedgerConfig {
            allow_short: true,
            exposure_valuation: PaperExposureValuation::OpenCost,
            maximum_accounts: 1,
            maximum_balances: 1,
            maximum_positions: 1,
            maximum_reservations: 2,
            fee_schedule: FeeSchedule::try_new(0, 0, Money::new(Decimal::ZERO, usd), None, 2)?,
        },
        [PaperAccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: vec![Money::new(Decimal::new(100, 0), usd)],
            capital: Money::new(Decimal::new(100, 0), usd),
            peak_capital: Money::new(Decimal::new(100, 0), usd),
            gross_exposure: Money::new(Decimal::ZERO, usd),
            realized_pnl: Money::new(Decimal::ZERO, usd),
            realized_loss: Money::new(Decimal::ZERO, usd),
            positions: vec![(instrument_id, 0)],
            position_cost_basis: vec![(instrument_id, Money::new(Decimal::ZERO, usd))],
        }],
    )?;
    let sell_id = OrderId::from_str("20000000-0000-0000-0000-000000000012")?;
    ledger.reserve(
        sell_id,
        account_id,
        terms,
        OrderSide::Sell,
        QuantityLots::new(1)?,
        PriceTicks::new(10),
    )?;
    ledger.apply_fill(
        sell_id,
        terms,
        &[(PriceTicks::new(10), QuantityLots::new(1)?)],
        LiquidityRole::Taker,
    )?;
    assert_eq!(ledger.position_lots(account_id, instrument_id)?, -1);
    assert_eq!(
        ledger
            .position_cost_basis(account_id, instrument_id)?
            .amount(),
        Decimal::TEN
    );
    let opened = ledger.account_risk(account_id)?;
    assert_eq!(opened.gross_exposure().amount(), Decimal::TEN);
    assert_eq!(opened.realized_pnl().amount(), Decimal::ZERO);

    let cover_id = OrderId::from_str("20000000-0000-0000-0000-000000000013")?;
    ledger.reserve(
        cover_id,
        account_id,
        terms,
        OrderSide::Buy,
        QuantityLots::new(1)?,
        PriceTicks::new(8),
    )?;
    ledger.apply_fill(
        cover_id,
        terms,
        &[(PriceTicks::new(8), QuantityLots::new(1)?)],
        LiquidityRole::Taker,
    )?;

    assert_eq!(ledger.position_lots(account_id, instrument_id)?, 0);
    assert_eq!(ledger.cash(account_id, usd)?.amount(), Decimal::new(102, 0));
    assert_eq!(
        ledger
            .position_cost_basis(account_id, instrument_id)?
            .amount(),
        Decimal::ZERO
    );
    let covered = ledger.account_risk(account_id)?;
    assert_eq!(covered.capital().amount(), Decimal::new(102, 0));
    assert_eq!(covered.peak_capital().amount(), Decimal::new(102, 0));
    assert_eq!(covered.gross_exposure().amount(), Decimal::ZERO);
    assert_eq!(covered.realized_pnl().amount(), Decimal::new(2, 0));
    assert_eq!(covered.realized_loss().amount(), Decimal::ZERO);

    let buy_id = OrderId::from_str("20000000-0000-0000-0000-000000000015")?;
    ledger.reserve(
        buy_id,
        account_id,
        terms,
        OrderSide::Buy,
        QuantityLots::new(2)?,
        PriceTicks::new(10),
    )?;
    ledger.apply_fill(
        buy_id,
        terms,
        &[(PriceTicks::new(10), QuantityLots::new(2)?)],
        LiquidityRole::Taker,
    )?;
    let cross_id = OrderId::from_str("20000000-0000-0000-0000-000000000016")?;
    ledger.reserve(
        cross_id,
        account_id,
        terms,
        OrderSide::Sell,
        QuantityLots::new(3)?,
        PriceTicks::new(8),
    )?;
    ledger.apply_fill(
        cross_id,
        terms,
        &[(PriceTicks::new(8), QuantityLots::new(3)?)],
        LiquidityRole::Taker,
    )?;

    assert_eq!(ledger.position_lots(account_id, instrument_id)?, -1);
    assert_eq!(
        ledger
            .position_cost_basis(account_id, instrument_id)?
            .amount(),
        Decimal::new(8, 0)
    );
    let crossed = ledger.account_risk(account_id)?;
    assert_eq!(crossed.capital().amount(), Decimal::new(98, 0));
    assert_eq!(crossed.peak_capital().amount(), Decimal::new(102, 0));
    assert_eq!(crossed.gross_exposure().amount(), Decimal::new(8, 0));
    assert_eq!(crossed.realized_pnl().amount(), Decimal::new(-2, 0));
    assert_eq!(crossed.realized_loss().amount(), Decimal::new(4, 0));
    Ok(())
}
