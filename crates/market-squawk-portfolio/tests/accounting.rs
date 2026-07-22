mod analytics;
mod service;

use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy,
};
use market_squawk_domain::{
    CorporateActionKind, Currency, DigestAlgorithm, EvidenceDigest, MergerConsideration, Money,
    NormalizedPortfolioTransactionClass, NormalizedPortfolioTransactionEvidence,
    NormalizedPortfolioTransactionEvidenceInput, RevisionNumber, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_portfolio::{
    BasisMeasurement, CashFlow, CashFlowKind, FxRateEvidence, LedgerEntry, LedgerEntryKind,
    LotSelection, PortfolioError, PortfolioLedger, PortfolioLimitInput, PortfolioLimits,
    PriceEvidence, ReconciliationTolerance, RevisionEvidence, SourcePortfolioTotals, Trade,
    TradeSide, TransactionRevision, ValuationSet,
};
use proptest::prelude::*;
use rust_decimal::Decimal;

use analytics::{
    account, action_record, corporate_action_records, dataset, instrument, money, source,
};

type TestResult = Result<(), Box<dyn Error>>;

fn limits() -> Result<PortfolioLimits, PortfolioError> {
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 4,
        max_instruments: 16,
        max_lots: 64,
        max_transactions: 128,
        max_factors: 16,
        max_scenarios: 16,
        max_history: 16,
        max_results: 128,
        max_retained_bytes: 1024 * 1024,
    })
}

fn revision_evidence(marker: u8, at: i64) -> TestResultEvidence {
    Ok(RevisionEvidence::try_new(
        Timestamp::from_unix_nanos(at),
        dataset(marker)?,
        market_squawk_data::Sha256Digest::new([marker; 32]),
        market_squawk_data::Sha256Digest::new([marker.wrapping_add(1); 32]),
        vec![source(&format!("ledger-source-{marker}"))?],
        Vec::new(),
        None,
    )?)
}

fn revision_evidence_with_plan(
    marker: u8,
    at: i64,
    plan: &CorporateActionPlan,
) -> TestResultEvidence {
    Ok(RevisionEvidence::try_new(
        Timestamp::from_unix_nanos(at),
        dataset(marker)?,
        market_squawk_data::Sha256Digest::new([marker; 32]),
        market_squawk_data::Sha256Digest::new([marker.wrapping_add(1); 32]),
        vec![source("corporate-actions")?],
        Vec::new(),
        Some(market_squawk_portfolio::CorporateActionBinding::from_plan(
            plan,
        )),
    )?)
}

type TestResultEvidence = Result<RevisionEvidence, Box<dyn Error>>;

fn valuation(marker: u8, at: i64, prices: &[(u8, i64)]) -> Result<ValuationSet, Box<dyn Error>> {
    let usd = Currency::try_from("USD")?;
    let price_evidence = prices
        .iter()
        .map(|(instrument_marker, amount)| {
            Ok(PriceEvidence::try_new(
                instrument(*instrument_marker)?,
                Money::new(Decimal::from(*amount), usd),
                Timestamp::from_unix_nanos(at),
                source(&format!("price-{marker}-{instrument_marker}"))?,
            )?)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    Ok(ValuationSet::try_new(
        usd,
        Timestamp::from_unix_nanos(at),
        dataset(marker)?,
        market_squawk_data::Sha256Digest::new([marker; 32]),
        price_evidence,
        Vec::new(),
        limits()?,
    )?)
}

fn transaction(
    id: &str,
    revision: u32,
    supersedes: Option<u32>,
) -> Result<TransactionRevision, Box<dyn Error>> {
    Ok(TransactionRevision::try_new(
        SourceIdentifier::try_from(id)?,
        RevisionNumber::new(revision)?,
        supersedes.map(RevisionNumber::new).transpose()?,
    )?)
}

fn entry(
    id: &str,
    revision: u32,
    supersedes: Option<u32>,
    at: i64,
    marker: u8,
    kind: LedgerEntryKind,
) -> Result<LedgerEntry, Box<dyn Error>> {
    Ok(LedgerEntry::try_new(
        account()?,
        transaction(id, revision, supersedes)?,
        Timestamp::from_unix_nanos(at),
        source(&format!("source-{marker}"))?,
        kind,
    )?)
}

#[test]
fn normalized_task10_transaction_flows_through_the_immutable_ledger() -> TestResult {
    let imported = task10_cash_transaction(1)?;
    let usd = Currency::try_from("USD")?;
    let mut ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let revision = ledger.try_apply_import(
        &[imported],
        &[],
        None,
        valuation(10, 110, &[])?,
        revision_evidence(10, 110)?,
    )?;
    assert_eq!(revision.cash().amount(), Decimal::from(100_u32));
    assert_eq!(revision.evidence().dataset().manifest_version(), 10);

    let mut different_ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let different_revision = different_ledger.try_apply_import(
        &[task10_cash_transaction(2)?],
        &[],
        None,
        valuation(10, 110, &[])?,
        revision_evidence(10, 110)?,
    )?;
    assert_ne!(revision.id(), different_revision.id());
    Ok(())
}

fn task10_cash_transaction(
    digest_marker: u8,
) -> Result<NormalizedPortfolioTransactionEvidence, Box<dyn Error>> {
    Ok(NormalizedPortfolioTransactionEvidence::try_new(
        NormalizedPortfolioTransactionEvidenceInput {
            source_id: SourceId::try_from("portfolio-task10")?,
            logical_record_id: source("deposit-record")?,
            source_revision: source("statement-1")?,
            supersedes_source_revision: None,
            revision: RevisionNumber::new(1)?,
            raw_source_reference: source(&format!("task10-deposit-record-{digest_marker}"))?,
            raw_payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [digest_marker; 32]),
            broker_transaction_id: source("broker-deposit")?,
            account_id: account()?,
            instrument_id: None,
            classification: NormalizedPortfolioTransactionClass::CashTransfer,
            amount: money(100, Currency::try_from("USD")?),
            quantity: None,
            occurred_at: Timestamp::from_unix_nanos(99),
            lot_method: None,
        },
    )?)
}

fn action_plan(
    records: Vec<market_squawk_data::CorporateActionRecord>,
    valuation_cutoff: i64,
) -> Result<CorporateActionPlan, Box<dyn Error>> {
    Ok(CorporateActionPlan::try_build(
        CorporateActionPolicy::new(CorporateActionAdjustment::TotalReturn, NonZeroU32::MIN),
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(valuation_cutoff),
        records,
        CorporateActionLimits::try_new(
            NonZeroUsize::new(16).ok_or("actions")?,
            NonZeroUsize::new(1024 * 1024).ok_or("bytes")?,
        )?,
    )?)
}

#[test]
fn cumulative_corporate_action_plan_replaces_prior_snapshot_without_replaying_steps() -> TestResult
{
    let usd = Currency::try_from("USD")?;
    let instrument_id = instrument(1)?;
    let records = corporate_action_records(instrument_id, usd)?;
    let first_plan = action_plan(records.clone(), 6)?;
    let cumulative_plan = action_plan(records, 7)?;
    let mut ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;

    let first = ledger.try_apply(
        vec![entry(
            "cumulative-buy",
            1,
            None,
            1,
            1,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_id,
                Decimal::TEN,
                money(10, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?],
        Some(&first_plan),
        valuation(12, 20, &[(1, 5)])?,
        revision_evidence_with_plan(12, 20, &first_plan)?,
    )?;
    assert_eq!(
        first
            .position(instrument_id)
            .ok_or("first position")?
            .quantity(),
        Decimal::from(20_u32)
    );

    let cumulative = ledger.try_apply(
        Vec::new(),
        Some(&cumulative_plan),
        valuation(13, 20, &[(1, 5)])?,
        revision_evidence_with_plan(13, 20, &cumulative_plan)?,
    )?;
    assert_eq!(
        cumulative
            .position(instrument_id)
            .ok_or("cumulative position")?
            .quantity(),
        Decimal::from(20_u32)
    );
    assert_eq!(cumulative.cash().amount(), Decimal::from(-80_i32));
    assert_eq!(cumulative.corporate_action_bindings().len(), 1);
    assert_eq!(
        first
            .corporate_action_binding()
            .ok_or("first action binding")?
            .content_identity(),
        first_plan.content_hash()
    );
    assert!(matches!(
        ledger.try_apply(
            Vec::new(),
            Some(&first_plan),
            valuation(14, 21, &[(1, 5)])?,
            revision_evidence_with_plan(14, 21, &first_plan)?,
        ),
        Err(PortfolioError::EvidenceMismatch)
    ));
    Ok(())
}

#[test]
fn opposing_lots_preserve_gross_exposure_and_merger_cash_direction() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let subject = instrument(1)?;
    let successor = instrument(2)?;
    let entries = vec![
        entry(
            "merger-long",
            1,
            None,
            1,
            1,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                subject,
                Decimal::TEN,
                money(10, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "merger-short",
            1,
            None,
            2,
            2,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::SellShort,
                subject,
                Decimal::from(4_u32),
                money(10, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
    ];
    let mut cash_ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let before = cash_ledger.try_apply(
        entries.clone(),
        None,
        valuation(21, 8, &[(1, 12)])?,
        revision_evidence(21, 8)?,
    )?;
    assert_eq!(before.market_value().amount(), Decimal::from(72_u32));
    assert_eq!(before.gross_exposure().amount(), Decimal::from(168_u32));

    let cash_plan = action_plan(
        vec![action_record(
            4,
            subject,
            CorporateActionKind::Merger {
                successor,
                consideration: MergerConsideration::Cash {
                    amount: money(15, usd),
                },
            },
        )?],
        20,
    )?;
    let cash_result = cash_ledger.try_apply(
        Vec::new(),
        Some(&cash_plan),
        valuation(22, 20, &[])?,
        revision_evidence_with_plan(22, 20, &cash_plan)?,
    )?;
    assert_eq!(cash_result.cash().amount(), Decimal::from(30_u32));
    assert_eq!(cash_result.realized_gain().amount(), Decimal::from(30_u32));
    assert_eq!(cash_result.realized_loss().amount(), Decimal::from(20_u32));
    assert!(cash_result.positions().is_empty());

    let mixed_plan = action_plan(
        vec![action_record(
            5,
            subject,
            CorporateActionKind::Merger {
                successor,
                consideration: MergerConsideration::Mixed {
                    numerator: NonZeroU32::MIN,
                    denominator: NonZeroU32::new(2).ok_or("two")?,
                    cash: money(5, usd),
                },
            },
        )?],
        20,
    )?;
    let mut mixed_ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    assert!(matches!(
        mixed_ledger.try_apply(
            entries,
            Some(&mixed_plan),
            valuation(23, 20, &[(2, 20)])?,
            revision_evidence_with_plan(23, 20, &mixed_plan)?,
        ),
        Err(PortfolioError::UnresolvedCorporateAction)
    ));
    Ok(())
}

#[test]
fn return_of_capital_reduces_each_complete_lot_and_realizes_each_excess() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let instrument_id = instrument(1)?;
    let plan = action_plan(
        vec![action_record(
            4,
            instrument_id,
            CorporateActionKind::ReturnOfCapital {
                amount: money(10, usd),
            },
        )?],
        20,
    )?;
    let mut ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let revision = ledger.try_apply(
        vec![
            entry(
                "cheap-lot",
                1,
                None,
                1,
                1,
                LedgerEntryKind::Trade(Trade::try_new(
                    TradeSide::Buy,
                    instrument_id,
                    Decimal::ONE,
                    money(1, usd),
                    money(0, usd),
                    LotSelection::Fifo,
                )?),
            )?,
            entry(
                "expensive-lot",
                1,
                None,
                2,
                2,
                LedgerEntryKind::Trade(Trade::try_new(
                    TradeSide::Buy,
                    instrument_id,
                    Decimal::ONE,
                    money(100, usd),
                    money(0, usd),
                    LotSelection::Fifo,
                )?),
            )?,
        ],
        Some(&plan),
        valuation(15, 20, &[(1, 10)])?,
        revision_evidence_with_plan(15, 20, &plan)?,
    )?;
    let position = revision.position(instrument_id).ok_or("ROC position")?;
    let cheap = position
        .lots()
        .iter()
        .find(|lot| lot.id().as_str() == "cheap-lot")
        .ok_or("cheap lot")?;
    let expensive = position
        .lots()
        .iter()
        .find(|lot| lot.id().as_str() == "expensive-lot")
        .ok_or("expensive lot")?;
    assert_eq!(cheap.basis().amount(), Decimal::ZERO);
    assert_eq!(expensive.basis().amount(), Decimal::from(90_u32));
    assert_eq!(revision.return_of_capital().amount(), Decimal::from(20_u32));
    assert_eq!(revision.realized_gain().amount(), Decimal::from(9_u32));
    Ok(())
}

#[test]
fn accounting_vertical_is_exact_revisioned_and_reconciles_without_overwrite() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let account_id = account()?;
    let instrument_a = instrument(1)?;
    let mut ledger = PortfolioLedger::try_new(account_id, usd, limits()?)?;
    let entries = vec![
        entry(
            "cash-1",
            1,
            None,
            1,
            1,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                money(1_000, usd),
                None,
            )?),
        )?,
        entry(
            "buy-1",
            1,
            None,
            2,
            2,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_a,
                Decimal::TEN,
                money(10, usd),
                money(1, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "dividend-1",
            1,
            None,
            3,
            3,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Dividend,
                money(5, usd),
                Some(instrument_a),
            )?),
        )?,
        entry(
            "interest-1",
            1,
            None,
            4,
            4,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Interest,
                money(2, usd),
                None,
            )?),
        )?,
        entry(
            "withholding-1",
            1,
            None,
            5,
            5,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Withholding,
                money(1, usd),
                Some(instrument_a),
            )?),
        )?,
        entry(
            "sell-1",
            1,
            None,
            6,
            6,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Sell,
                instrument_a,
                Decimal::from(4_u32),
                money(15, usd),
                money(1, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "short-1",
            1,
            None,
            7,
            7,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::SellShort,
                instrument(2)?,
                Decimal::from(2_u32),
                money(20, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "cover-1",
            1,
            None,
            8,
            8,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::BuyToCover,
                instrument(2)?,
                Decimal::ONE,
                money(15, usd),
                money(1, usd),
                LotSelection::Fifo,
            )?),
        )?,
    ];
    let revision = ledger.try_apply(
        entries,
        None,
        valuation(1, 10, &[(1, 12), (2, 18)])?,
        revision_evidence(1, 10)?,
    )?;

    assert_eq!(revision.cash().amount(), Decimal::from(988_u32));
    assert_eq!(revision.income().amount(), Decimal::from(7_u32));
    assert_eq!(revision.withholding().amount(), Decimal::ONE);
    assert_eq!(revision.fees().amount(), Decimal::from(3_u32));
    assert_eq!(
        revision
            .position(instrument_a)
            .ok_or("long position")?
            .quantity(),
        Decimal::from(6_u32)
    );
    assert_eq!(
        revision
            .position(instrument(2)?)
            .ok_or("short position")?
            .quantity(),
        -Decimal::ONE
    );
    assert_eq!(revision.realized_gain().amount(), Decimal::new(226, 1));
    assert_eq!(
        revision
            .unrealized_gain()
            .complete()
            .ok_or("complete unrealized gain")?
            .amount(),
        Decimal::new(134, 1)
    );
    assert!(revision.cash().amount().is_sign_positive());
    assert_eq!(revision.previous_revision_id(), None);
    assert_eq!(revision.evidence().dataset().manifest_version(), 1);

    let supplied = SourcePortfolioTotals::try_new(
        account_id,
        usd,
        Some(money(970, usd)),
        Some(money(54, usd)),
        Some(money(101, usd)),
        ReconciliationTolerance::try_absolute(money(0, usd))?,
        source("supplied-total")?,
    )?;
    let discrepancies = revision.reconcile_supplied(&[supplied], limits()?)?;
    assert_eq!(discrepancies.len(), 2);
    assert!(
        discrepancies
            .iter()
            .all(|item| item.supplied() != item.calculated())
    );

    let corrected = ledger.try_apply(
        vec![entry(
            "interest-1",
            2,
            Some(1),
            4,
            9,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Interest,
                money(3, usd),
                None,
            )?),
        )?],
        None,
        valuation(2, 11, &[(1, 12), (2, 18)])?,
        revision_evidence(2, 11)?,
    )?;
    assert_eq!(corrected.income().amount(), Decimal::from(8_u32));
    assert_eq!(corrected.previous_revision_id(), Some(revision.id()));
    assert_eq!(revision.income().amount(), Decimal::from(7_u32));

    assert!(matches!(
        ledger.try_apply(
            vec![entry(
                "interest-1",
                2,
                Some(1),
                4,
                10,
                LedgerEntryKind::CashFlow(CashFlow::try_new(
                    CashFlowKind::Interest,
                    money(4, usd),
                    None,
                )?),
            )?],
            None,
            valuation(3, 12, &[(1, 12), (2, 18)])?,
            revision_evidence(3, 12)?,
        ),
        Err(PortfolioError::DuplicateTransactionRevision)
    ));
    Ok(())
}

#[test]
fn specific_lots_corporate_actions_negative_cash_and_overflow_fail_closed() -> TestResult {
    let usd = Currency::try_from("USD")?;
    let instrument_a = instrument(1)?;
    let mut ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let mut initial = vec![
        entry(
            "buy-cheap",
            1,
            None,
            1,
            1,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_a,
                Decimal::from(2_u32),
                money(10, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "buy-expensive",
            1,
            None,
            2,
            2,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_a,
                Decimal::from(2_u32),
                money(20, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
        entry(
            "sell-specific",
            1,
            None,
            3,
            3,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Sell,
                instrument_a,
                Decimal::ONE,
                money(30, usd),
                money(0, usd),
                LotSelection::SpecificIdentification(vec![source("buy-expensive")?]),
            )?),
        )?,
    ];
    initial.reverse();
    let first = ledger.try_apply(
        initial,
        None,
        valuation(4, 4, &[(1, 30)])?,
        revision_evidence(4, 4)?,
    )?;
    assert_eq!(first.cash().amount(), Decimal::from(-30_i32));
    assert_eq!(
        first
            .cost_basis()
            .complete()
            .ok_or("complete cost basis")?
            .amount(),
        Decimal::from(40_u32)
    );
    assert_eq!(first.realized_gain().amount(), Decimal::TEN);

    let records = corporate_action_records(instrument_a, usd)?;
    let plan = CorporateActionPlan::try_build(
        CorporateActionPolicy::new(CorporateActionAdjustment::TotalReturn, NonZeroU32::MIN),
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(20),
        records,
        CorporateActionLimits::try_new(
            NonZeroUsize::new(16).ok_or("actions")?,
            NonZeroUsize::new(1024 * 1024).ok_or("bytes")?,
        )?,
    )?;
    let adjusted = ledger.try_apply(
        Vec::new(),
        Some(&plan),
        valuation(5, 20, &[(1, 15), (3, 8), (4, 12)])?,
        RevisionEvidence::try_new(
            Timestamp::from_unix_nanos(20),
            dataset(5)?,
            market_squawk_data::Sha256Digest::new([5; 32]),
            market_squawk_data::Sha256Digest::new([6; 32]),
            vec![source("corporate-actions")?],
            Vec::new(),
            Some(market_squawk_portfolio::CorporateActionBinding::from_plan(
                &plan,
            )),
        )?,
    )?;
    assert_eq!(
        adjusted
            .corporate_action_binding()
            .ok_or("action binding")?
            .content_identity(),
        plan.content_hash()
    );
    assert!(adjusted.position(instrument(3)?).is_some());
    assert!(adjusted.position(instrument(4)?).is_some());
    let incomplete = adjusted
        .position(instrument(3)?)
        .ok_or("spinoff position")?;
    assert_eq!(incomplete.cost_basis(), BasisMeasurement::Incomplete);
    assert_eq!(incomplete.unrealized_gain(), BasisMeasurement::Incomplete);
    assert_eq!(adjusted.cost_basis(), BasisMeasurement::Incomplete);
    assert_eq!(adjusted.unrealized_gain(), BasisMeasurement::Incomplete);
    assert!(adjusted.return_of_capital().amount().is_sign_positive());
    assert!(adjusted.income().amount().is_sign_positive());

    assert!(matches!(
        ledger.try_apply(
            vec![entry(
                "sell-incomplete-spinoff",
                1,
                None,
                21,
                22,
                LedgerEntryKind::Trade(Trade::try_new(
                    TradeSide::Sell,
                    instrument(3)?,
                    Decimal::ONE,
                    money(8, usd),
                    money(0, usd),
                    LotSelection::Fifo,
                )?),
            )?],
            None,
            valuation(6, 21, &[(3, 8), (4, 12)])?,
            revision_evidence_with_plan(6, 21, &plan)?,
        ),
        Err(PortfolioError::UnresolvedCorporateAction)
    ));

    let eur = Currency::try_from("EUR")?;
    let fx_revision = ledger.try_apply(
        vec![entry(
            "eur-deposit",
            1,
            None,
            21,
            21,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                money(10, eur),
                None,
            )?),
        )?],
        None,
        ValuationSet::try_new(
            usd,
            Timestamp::from_unix_nanos(21),
            dataset(11)?,
            market_squawk_data::Sha256Digest::new([11; 32]),
            vec![
                PriceEvidence::try_new(
                    instrument(3)?,
                    money(8, usd),
                    Timestamp::from_unix_nanos(21),
                    source("price-spinoff")?,
                )?,
                PriceEvidence::try_new(
                    instrument(4)?,
                    money(12, usd),
                    Timestamp::from_unix_nanos(21),
                    source("price-successor")?,
                )?,
            ],
            vec![FxRateEvidence::try_new(
                eur,
                usd,
                Decimal::new(12, 1),
                Timestamp::from_unix_nanos(21),
                source("official-eur-usd")?,
            )?],
            limits()?,
        )?,
        revision_evidence_with_plan(11, 21, &plan)?,
    )?;
    assert!(
        fx_revision.cash_balances().iter().any(|balance| {
            balance.currency() == eur && balance.amount().amount() == Decimal::TEN
        })
    );
    assert_eq!(
        fx_revision.cash().amount(),
        adjusted.cash().amount() + Decimal::from(12_u32)
    );

    let mut overflow_ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
    let overflow = overflow_ledger.try_apply(
        vec![entry(
            "overflow",
            1,
            None,
            1,
            1,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument_a,
                Decimal::MAX,
                Money::new(Decimal::MAX, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?],
        None,
        valuation(6, 2, &[(1, 1)])?,
        revision_evidence(6, 2)?,
    );
    assert!(matches!(overflow, Err(PortfolioError::Arithmetic)));
    Ok(())
}

proptest! {
    #[test]
    fn partial_fifo_disposal_conserves_units_and_cost(acquired in 2_u32..100, disposed in 1_u32..50) {
        let disposed = disposed.min(acquired - 1);
        let result = (|| -> TestResult {
            let usd = Currency::try_from("USD")?;
            let quantity = Decimal::from(acquired);
            let sold = Decimal::from(disposed);
            let mut ledger = PortfolioLedger::try_new(account()?, usd, limits()?)?;
            let revision = ledger.try_apply(
                vec![
                    entry("property-buy", 1, None, 1, 1, LedgerEntryKind::Trade(Trade::try_new(
                        TradeSide::Buy, instrument(1)?, quantity, money(7, usd), money(0, usd), LotSelection::Fifo,
                    )?))?,
                    entry("property-sell", 1, None, 2, 2, LedgerEntryKind::Trade(Trade::try_new(
                        TradeSide::Sell, instrument(1)?, sold, money(11, usd), money(0, usd), LotSelection::Fifo,
                    )?))?,
                ],
                None,
                valuation(7, 3, &[(1, 11)])?,
                revision_evidence(7, 3)?,
            )?;
            let remaining = Decimal::from(acquired - disposed);
            assert_eq!(revision.position(instrument(1)?).ok_or("position")?.quantity(), remaining);
            assert_eq!(
                revision
                    .cost_basis()
                    .complete()
                    .ok_or("complete property cost basis")?
                    .amount(),
                remaining * Decimal::from(7_u32)
            );
            assert_eq!(revision.realized_gain().amount(), sold * Decimal::from(4_u32));
            Ok(())
        })();
        prop_assert!(result.is_ok(), "{result:?}");
    }
}
