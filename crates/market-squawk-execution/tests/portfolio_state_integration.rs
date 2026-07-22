#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed authority assertions must terminate this test"
)]

use std::error::Error;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use market_squawk_data::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};
use market_squawk_domain::{
    AccountId, Currency, InstrumentId, Money, RevisionNumber, RuleVersion, SourceIdentifier,
    Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountRiskCoordinator, AccountRiskViolation,
    ExecutionAuditConfig, ExecutionAuditWriter, MarketRiskInput, PortfolioReadCapability,
    PortfolioReadError, PortfolioReadLimits, PreAuthorityRiskOutcome, RiskPolicyIdentity,
    RiskRejectionCode, RiskService, RiskServiceConfig, portfolio_execution_state,
};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, PortfolioLedger, PortfolioLimitInput,
    PortfolioLimits, PortfolioRevision, PortfolioRevisionToken, PortfolioService,
    PortfolioServiceLimitInput, PortfolioServiceLimits, PriceEvidence, RevisionEvidence, Trade,
    TradeSide, TransactionRevision, ValuationSet,
};
use rust_decimal::Decimal;

use super::risk_matrix::Fixture;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn risk_rejects_missing_revoked_and_mismatched_portfolio_state_before_reservation() -> TestResult {
    let fixture = Fixture::new();
    let intent = fixture.intent(40, 1, 100, 50);
    let market = MarketRiskInput::try_new(
        fixture.terms,
        market_squawk_domain::DataQuality::DirectVerified,
        true,
        true,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(i64::MAX),
        market_squawk_domain::PriceTicks::new(100),
        market_squawk_domain::PriceTicks::new(100),
    )?;

    let current = revision(fixture.account_id, fixture.usd, Decimal::new(500, 0), 1)?;
    let (_, current_capability) = state(service(vec![current], Vec::new())?)?;
    let current_risk = risk(&fixture, Decimal::new(500, 0), current_capability)?;
    if let PreAuthorityRiskOutcome::Rejected(rejection) =
        current_risk.evaluate_pre_authority(&intent, &market)
    {
        panic!(
            "current authoritative portfolio rejected: {:?}",
            rejection.reasons()
        );
    }

    let missing_capability = PortfolioReadCapability::unavailable(PortfolioReadLimits::default())?;
    assert_rejection(
        risk(&fixture, Decimal::new(500, 0), missing_capability)?
            .evaluate_pre_authority(&fixture.intent(41, 1, 100, 50), &market),
        RiskRejectionCode::Portfolio(PortfolioReadError::MissingAccount),
    );

    let stale = revision(fixture.account_id, fixture.usd, Decimal::new(500, 0), 2)?;
    let current = revision(fixture.account_id, fixture.usd, Decimal::new(500, 0), 3)?;
    let (_, revoked_capability) = state(service(vec![current], vec![stale.token()])?)?;
    let revoked_binding = revoked_capability.bind_current(
        fixture.account_id,
        fixture.instrument_id,
        market_squawk_domain::OrderSide::Buy,
        fixture.usd,
    )?;
    assert_ne!(revoked_binding.0.revision(), &stale.token());

    let mismatched = revision(fixture.account_id, fixture.usd, Decimal::new(499, 0), 4)?;
    let (_, mismatch_capability) = state(service(vec![mismatched], Vec::new())?)?;
    assert_rejection(
        risk(&fixture, Decimal::new(500, 0), mismatch_capability)?
            .evaluate_pre_authority(&fixture.intent(42, 1, 100, 50), &market),
        RiskRejectionCode::Account(AccountRiskViolation::PortfolioStateMismatch),
    );

    let current = revision(fixture.account_id, fixture.usd, Decimal::new(500, 0), 5)?;
    let (publisher, revoked_capability) = state(service(vec![current], Vec::new())?)?;
    publisher.revoke();
    assert_rejection(
        risk(&fixture, Decimal::new(500, 0), revoked_capability)?
            .evaluate_pre_authority(&fixture.intent(43, 1, 100, 50), &market),
        RiskRejectionCode::Portfolio(PortfolioReadError::RevokedCapability),
    );

    let other_instrument = InstrumentId::from_str("10000000-0000-0000-0000-000000000002")?;
    let exposed = revision_with_holding(
        fixture.account_id,
        fixture.usd,
        other_instrument,
        Decimal::new(2_000, 0),
        Decimal::TEN,
        Decimal::new(100, 0),
        6,
    )?;
    let (_, exposed_capability) = state(service(vec![exposed], Vec::new())?)?;
    let mut understated = fixture.account(Decimal::new(1_000, 0));
    understated.capital = Money::new(Decimal::new(10_000, 0), fixture.usd);
    understated.peak_capital = understated.capital;
    understated.positions.push((other_instrument, 10));
    understated.position_cost_basis.push((
        other_instrument,
        Money::new(Decimal::new(1_000, 0), fixture.usd),
    ));
    assert_rejection(
        risk_with_account(&fixture, understated, exposed_capability)?
            .evaluate_pre_authority(&fixture.intent(44, 1, 100, 50), &market),
        RiskRejectionCode::Account(AccountRiskViolation::ExposureLimit),
    );
    Ok(())
}

#[test]
fn exact_portfolio_revision_is_rechecked_across_atomic_publication() -> TestResult {
    let fixture = Fixture::new();
    let first = revision(fixture.account_id, fixture.usd, Decimal::new(50, 0), 10)?;
    let first_token = first.token();
    let second = successor(&first, 11)?;
    let (publisher, capability) = state(service(vec![first.clone()], Vec::new())?)?;
    let (binding, _) = capability.bind_current(
        fixture.account_id,
        fixture.instrument_id,
        market_squawk_domain::OrderSide::Buy,
        fixture.usd,
    )?;
    assert_eq!(binding.revision(), &first_token);
    assert_ne!(binding.content_digest(), [0; 32]);
    capability.recheck(&binding)?;

    publisher.publish(service(vec![second.clone()], Vec::new())?)?;
    assert_eq!(
        capability.recheck(&binding),
        Err(PortfolioReadError::StaleRevision)
    );

    assert!(
        publisher
            .publish(service(vec![first.clone()], Vec::new())?)
            .is_err(),
        "a retired revision must not be resurrected"
    );
    publisher.publish(service(vec![second], vec![first_token.clone()])?)?;
    assert_eq!(
        capability.recheck(&binding),
        Err(PortfolioReadError::RevokedRevision)
    );
    assert!(
        publisher
            .publish(service(vec![first.clone()], Vec::new())?)
            .is_err(),
        "an explicitly revoked revision must remain permanently retired"
    );

    let left = service(vec![successor(&first, 12)?], Vec::new())?;
    let right = service(vec![successor(&first, 13)?], Vec::new())?;
    let (publisher, _) = state(service(vec![first], Vec::new())?)?;
    let publisher = Arc::new(publisher);
    let barrier = Arc::new(Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let left_publisher = Arc::clone(&publisher);
        let left_barrier = Arc::clone(&barrier);
        let left = scope.spawn(move || {
            left_barrier.wait();
            left_publisher.publish(left)
        });
        let right_publisher = Arc::clone(&publisher);
        let right_barrier = Arc::clone(&barrier);
        let right = scope.spawn(move || {
            right_barrier.wait();
            right_publisher.publish(right)
        });
        [
            left.join()
                .unwrap_or_else(|_| panic!("left publisher panicked")),
            right
                .join()
                .unwrap_or_else(|_| panic!("right publisher panicked")),
        ]
    });
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    Ok(())
}

pub(super) fn portfolio_capability(
    account_id: AccountId,
    currency: Currency,
    cash: Decimal,
) -> Result<PortfolioReadCapability, Box<dyn Error>> {
    let revision = revision(account_id, currency, cash, 90)?;
    Ok(state(service(vec![revision], Vec::new())?)?.1)
}

fn assert_rejection(outcome: PreAuthorityRiskOutcome, expected: RiskRejectionCode) {
    let PreAuthorityRiskOutcome::Rejected(rejection) = outcome else {
        panic!("portfolio failure must reject before reservation");
    };
    assert!(rejection.reasons().contains(&expected));
}

fn risk(
    fixture: &Fixture,
    account_cash: Decimal,
    portfolio: PortfolioReadCapability,
) -> Result<RiskService, Box<dyn Error>> {
    risk_with_account(fixture, fixture.account(account_cash), portfolio)
}

fn risk_with_account(
    fixture: &Fixture,
    account: AccountBootstrap,
    portfolio: PortfolioReadCapability,
) -> Result<RiskService, Box<dyn Error>> {
    let accounts = Arc::new(AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig {
            maximum_intent_lifetime_nanos: NonZeroU64::new(i64::MAX as u64)
                .ok_or("account lifetime")?,
            ..AccountCoordinatorConfig::default()
        },
        [account],
    )?);
    let (audit, _reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::new(8).ok_or("audit records")?,
        maximum_bytes: NonZeroU32::new(64 * 1024).ok_or("audit bytes")?,
    })?;
    Ok(RiskService::try_new(
        accounts,
        portfolio,
        fixture.limits(),
        audit,
        RiskServiceConfig {
            policy: RiskPolicyIdentity::new(
                &SourceIdentifier::try_from("portfolio-risk-test")?,
                RuleVersion::new(1)?,
            ),
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(1),
        },
    )?)
}

fn state(
    service: PortfolioService,
) -> Result<
    (
        market_squawk_execution::PortfolioServicePublisher,
        PortfolioReadCapability,
    ),
    PortfolioReadError,
> {
    portfolio_execution_state(service, PortfolioReadLimits::default())
}

fn service(
    revisions: Vec<PortfolioRevision>,
    revoked: Vec<PortfolioRevisionToken>,
) -> Result<PortfolioService, Box<dyn Error>> {
    Ok(PortfolioService::try_new(
        revisions,
        revoked,
        PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
            max_accounts: NonZeroUsize::new(4).ok_or("service accounts")?,
            max_history_per_account: NonZeroUsize::new(8).ok_or("service history")?,
            max_results: NonZeroUsize::new(16).ok_or("service results")?,
            max_retained_bytes: NonZeroUsize::new(1024 * 1024).ok_or("service bytes")?,
        })?,
    )?)
}

fn revision(
    account_id: AccountId,
    currency: Currency,
    cash: Decimal,
    marker: u8,
) -> Result<PortfolioRevision, Box<dyn Error>> {
    let limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 4,
        max_lots: 8,
        max_transactions: 8,
        max_factors: 4,
        max_scenarios: 4,
        max_history: 4,
        max_results: 16,
        max_retained_bytes: 1024 * 1024,
    })?;
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("execution-portfolio-test")?,
        u64::from(marker.max(1)),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker.max(1); 32]),
    )?;
    let at = Timestamp::from_unix_nanos(i64::from(marker.max(1)));
    let source = SourceIdentifier::try_from(format!("portfolio-source-{marker}"))?;
    let mut ledger = PortfolioLedger::try_new(account_id, currency, limits)?;
    let entry = LedgerEntry::try_new(
        account_id,
        TransactionRevision::try_new(
            SourceIdentifier::try_from(format!("portfolio-cash-{marker}"))?,
            RevisionNumber::new(1)?,
            None,
        )?,
        at,
        source.clone(),
        LedgerEntryKind::CashFlow(CashFlow::try_new(
            CashFlowKind::Deposit,
            Money::new(cash, currency),
            None,
        )?),
    )?;
    let point_in_time = Sha256Digest::new([marker.wrapping_add(1).max(1); 32]);
    let valuation = ValuationSet::try_new(
        currency,
        at,
        dataset.clone(),
        point_in_time,
        Vec::new(),
        Vec::new(),
        limits,
    )?;
    let evidence = RevisionEvidence::try_new(
        at,
        dataset,
        point_in_time,
        Sha256Digest::new([marker.wrapping_add(2).max(1); 32]),
        vec![source],
        Vec::new(),
        None,
    )?;
    Ok(ledger.try_apply(vec![entry], None, valuation, evidence)?)
}

fn successor(
    revision: &PortfolioRevision,
    marker: u8,
) -> Result<PortfolioRevision, Box<dyn Error>> {
    let limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 4,
        max_lots: 8,
        max_transactions: 8,
        max_factors: 4,
        max_scenarios: 4,
        max_history: 4,
        max_results: 16,
        max_retained_bytes: 1024 * 1024,
    })?;
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("execution-portfolio-test")?,
        u64::from(marker),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker; 32]),
    )?;
    let at = Timestamp::from_unix_nanos(i64::from(marker));
    let source = SourceIdentifier::try_from(format!("portfolio-source-{marker}"))?;
    let point_in_time = Sha256Digest::new([marker.wrapping_add(1); 32]);
    Ok(revision.clone().into_ledger()?.try_apply(
        Vec::new(),
        None,
        ValuationSet::try_new(
            revision.base_currency(),
            at,
            dataset.clone(),
            point_in_time,
            Vec::new(),
            Vec::new(),
            limits,
        )?,
        RevisionEvidence::try_new(
            at,
            dataset,
            point_in_time,
            Sha256Digest::new([marker.wrapping_add(2); 32]),
            vec![source],
            Vec::new(),
            None,
        )?,
    )?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the adversarial fixture makes every portfolio financial input explicit"
)]
fn revision_with_holding(
    account_id: AccountId,
    currency: Currency,
    instrument_id: InstrumentId,
    deposit: Decimal,
    quantity: Decimal,
    price: Decimal,
    marker: u8,
) -> Result<PortfolioRevision, Box<dyn Error>> {
    let limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 4,
        max_lots: 8,
        max_transactions: 8,
        max_factors: 4,
        max_scenarios: 4,
        max_history: 4,
        max_results: 16,
        max_retained_bytes: 1024 * 1024,
    })?;
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("execution-portfolio-test")?,
        u64::from(marker),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker; 32]),
    )?;
    let at = Timestamp::from_unix_nanos(i64::from(marker));
    let source = SourceIdentifier::try_from(format!("portfolio-source-{marker}"))?;
    let mut ledger = PortfolioLedger::try_new(account_id, currency, limits)?;
    let entries = vec![
        LedgerEntry::try_new(
            account_id,
            TransactionRevision::try_new(
                SourceIdentifier::try_from(format!("portfolio-cash-{marker}"))?,
                RevisionNumber::new(1)?,
                None,
            )?,
            at,
            source.clone(),
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                Money::new(deposit, currency),
                None,
            )?),
        )?,
        LedgerEntry::try_new(
            account_id,
            TransactionRevision::try_new(
                SourceIdentifier::try_from(format!("portfolio-trade-{marker}"))?,
                RevisionNumber::new(1)?,
                None,
            )?,
            at,
            source.clone(),
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_id,
                quantity,
                Money::new(price, currency),
                Money::new(Decimal::ZERO, currency),
                market_squawk_portfolio::LotSelection::Fifo,
            )?),
        )?,
    ];
    let point_in_time = Sha256Digest::new([marker.wrapping_add(1); 32]);
    Ok(ledger.try_apply(
        entries,
        None,
        ValuationSet::try_new(
            currency,
            at,
            dataset.clone(),
            point_in_time,
            vec![PriceEvidence::try_new(
                instrument_id,
                Money::new(price, currency),
                at,
                source.clone(),
            )?],
            Vec::new(),
            limits,
        )?,
        RevisionEvidence::try_new(
            at,
            dataset,
            point_in_time,
            Sha256Digest::new([marker.wrapping_add(2); 32]),
            vec![source],
            Vec::new(),
            None,
        )?,
    )?)
}
