use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;

use market_squawk_analytics::{
    Annualization, ExactDecimalScale, ExactRate, FeatureKey, Quantile, ReturnSeries,
    ShockComposition, StatisticalInput, StatisticalScale, StatisticalUnit,
};
use market_squawk_data::{
    CorporateActionRecord, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    AccountId, AvailabilityEvidence, CorporateActionKind, CorporateActionObservation, Currency,
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, MergerConsideration, Money,
    PayloadReference, ResearchContext, ResearchProvenance, ResearchProvenanceInput, ResearchTime,
    RevisionNumber, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_portfolio::{
    AnalyticsPolicyBinding, AttributionInput, AttributionReport, CashFlow, CashFlowKind,
    ExposureReport, FactorLoading, InstrumentClassification, LedgerEntry, LedgerEntryKind,
    LotSelection, MoneyWeightedMethod, PerformancePeriod, PerformancePolicy, PerformanceReport,
    PortfolioAnalyticsEvidence, PortfolioError, PortfolioLedger, PortfolioLimitInput,
    PortfolioLimits, PortfolioRiskReport, RebalanceConstraintInput, RebalanceConstraints,
    RebalanceProposal, RebalanceTarget, ScenarioDefinition, Trade, TradeSide, TransactionRevision,
};
use rust_decimal::Decimal;

type TestResult = Result<(), Box<dyn Error>>;

pub(super) fn account() -> Result<AccountId, Box<dyn Error>> {
    Ok("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?)
}

pub(super) fn instrument(marker: u8) -> Result<InstrumentId, Box<dyn Error>> {
    let value = format!("00000000-0000-4000-8000-{marker:012x}");
    Ok(InstrumentId::from_str(&value)?)
}

pub(super) fn source(value: &str) -> Result<SourceIdentifier, Box<dyn Error>> {
    Ok(SourceIdentifier::try_from(value)?)
}

pub(super) fn money(value: i64, currency: Currency) -> Money {
    Money::new(Decimal::from(value), currency)
}

pub(super) fn dataset(marker: u8) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("portfolio-test")?,
        u64::from(marker.max(1)),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker.max(1); 32]),
    )?)
}

pub(super) fn action_record(
    marker: u8,
    subject: InstrumentId,
    action: CorporateActionKind,
) -> Result<CorporateActionRecord, Box<dyn Error>> {
    let source_identifier = source(&format!("action-{marker}"))?;
    let at = Timestamp::from_unix_nanos(i64::from(marker) + 5);
    let context = ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("official-actions")?,
            instrument_id: Some(subject),
            venue_id: Some(VenueId::try_from("XNYS")?),
            source_identifier: source_identifier.clone(),
            source_timestamp: Some(at),
            received_at: at,
            ingested_at: Timestamp::from_unix_nanos(20),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(source_identifier.clone()),
            availability: AvailabilityEvidence::evidenced(at, source_identifier),
        })?,
        ResearchTime::new(at, None, RevisionNumber::new(1)?, None)?,
    )?;
    Ok(CorporateActionRecord::new(
        CorporateActionObservation::new(context, action)?,
        dataset(marker)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]),
    ))
}

pub(super) fn corporate_action_records(
    subject: InstrumentId,
    usd: Currency,
) -> Result<Vec<CorporateActionRecord>, Box<dyn Error>> {
    Ok(vec![
        action_record(
            1,
            subject,
            CorporateActionKind::Split {
                numerator: NonZeroU32::new(2).ok_or("two")?,
                denominator: NonZeroU32::MIN,
            },
        )?,
        action_record(
            2,
            subject,
            CorporateActionKind::CashDividend {
                amount: Money::new(Decimal::ONE, usd),
            },
        )?,
        action_record(
            3,
            subject,
            CorporateActionKind::Spinoff {
                distributed_instrument: instrument(3)?,
                numerator: NonZeroU32::MIN,
                denominator: NonZeroU32::new(2).ok_or("two")?,
            },
        )?,
        action_record(
            4,
            subject,
            CorporateActionKind::ReturnOfCapital {
                amount: Money::new(Decimal::ONE, usd),
            },
        )?,
        action_record(
            5,
            subject,
            CorporateActionKind::Merger {
                successor: instrument(4)?,
                consideration: MergerConsideration::Stock {
                    numerator: NonZeroU32::MIN,
                    denominator: NonZeroU32::new(2).ok_or("two")?,
                },
            },
        )?,
    ])
}

pub(super) fn analytics_revision()
-> Result<market_squawk_portfolio::PortfolioRevision, Box<dyn Error>> {
    let usd = Currency::try_from("USD")?;
    let limits = super::limits()?;
    let mut ledger = PortfolioLedger::try_new(account()?, usd, limits)?;
    let entries = vec![
        LedgerEntry::try_new(
            account()?,
            TransactionRevision::try_new(source("deposit")?, RevisionNumber::new(1)?, None)?,
            Timestamp::from_unix_nanos(1),
            source("deposit-source")?,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                money(1_000, usd),
                None,
            )?),
        )?,
        LedgerEntry::try_new(
            account()?,
            TransactionRevision::try_new(source("buy-a")?, RevisionNumber::new(1)?, None)?,
            Timestamp::from_unix_nanos(2),
            source("trade-source")?,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument(1)?,
                Decimal::TEN,
                money(10, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
        LedgerEntry::try_new(
            account()?,
            TransactionRevision::try_new(source("buy-b")?, RevisionNumber::new(1)?, None)?,
            Timestamp::from_unix_nanos(3),
            source("trade-source")?,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument(2)?,
                Decimal::from(5_u32),
                money(20, usd),
                money(0, usd),
                LotSelection::Fifo,
            )?),
        )?,
    ];
    Ok(ledger.try_apply(
        entries,
        None,
        super::valuation(8, 4, &[(1, 12), (2, 18)])?,
        super::revision_evidence(8, 4)?,
    )?)
}

fn analytics_evidence(
    revision: &market_squawk_portfolio::PortfolioRevision,
    effective_through: i64,
    available_through: i64,
) -> Result<PortfolioAnalyticsEvidence, Box<dyn Error>> {
    Ok(PortfolioAnalyticsEvidence::try_from_revision(
        revision,
        Timestamp::from_unix_nanos(effective_through),
        Timestamp::from_unix_nanos(available_through),
        AnalyticsPolicyBinding::try_new(source("valuation-policy")?, NonZeroU32::MIN)?,
        AnalyticsPolicyBinding::try_new(source("fx-policy")?, NonZeroU32::MIN)?,
        AnalyticsPolicyBinding::try_new(source("as-of-policy")?, NonZeroU32::MIN)?,
    )?)
}

fn analytics_limits(
    max_factors: usize,
    max_scenarios: usize,
    max_results: usize,
    max_retained_bytes: usize,
) -> Result<PortfolioLimits, PortfolioError> {
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 4,
        max_instruments: 16,
        max_lots: 64,
        max_transactions: 128,
        max_factors,
        max_scenarios,
        max_history: 16,
        max_results,
        max_retained_bytes,
    })
}

#[test]
fn analytics_evidence_rejects_future_horizons() -> TestResult {
    let revision = analytics_revision()?;
    let error = match analytics_evidence(&revision, 5, 4) {
        Ok(_) => return Err("future analytics evidence was accepted".into()),
        Err(error) => error,
    };
    assert_eq!(
        error.downcast_ref::<PortfolioError>(),
        Some(&PortfolioError::EvidenceMismatch)
    );
    Ok(())
}

#[test]
fn analytics_work_is_rejected_before_report_allocation() -> TestResult {
    let revision = analytics_revision()?;
    let evidence = analytics_evidence(&revision, 4, 4)?;
    let usd = Currency::try_from("USD")?;
    let dimension_limits = analytics_limits(1, 1, 128, 1024 * 1024)?;

    assert!(matches!(
        InstrumentClassification::try_new(
            instrument(1)?,
            source("technology")?,
            source("issuer-a")?,
            VenueId::try_from("XNAS")?,
            usd,
            vec![
                FactorLoading::try_new(
                    FeatureKey::try_new("market.beta", NonZeroU32::MIN)?,
                    ExactRate::try_new(Decimal::ONE, ExactDecimalScale::Unit)?,
                )?,
                FactorLoading::try_new(
                    FeatureKey::try_new("size.beta", NonZeroU32::MIN)?,
                    ExactRate::try_new(Decimal::ONE, ExactDecimalScale::Unit)?,
                )?,
            ],
            dimension_limits,
        ),
        Err(PortfolioError::LimitExceeded { .. })
    ));
    assert!(matches!(
        ScenarioDefinition::try_new(
            source("two-shock-scenario")?,
            ShockComposition::Additive,
            vec![
                market_squawk_analytics::ScenarioShock::try_new(
                    &format!("instrument-{}", instrument(1)?),
                    ExactRate::try_new(Decimal::NEGATIVE_ONE, ExactDecimalScale::Unit)?,
                )?,
                market_squawk_analytics::ScenarioShock::try_new(
                    &format!("instrument-{}", instrument(2)?),
                    ExactRate::try_new(Decimal::NEGATIVE_ONE, ExactDecimalScale::Unit)?,
                )?,
            ],
            dimension_limits,
        ),
        Err(PortfolioError::LimitExceeded { .. })
    ));

    let retained_limits = analytics_limits(16, 16, 128, 1)?;
    let periods = [PerformancePeriod::try_new(
        Timestamp::from_unix_nanos(0),
        Timestamp::from_unix_nanos(1),
        money(100, usd),
        money(101, usd),
        money(0, usd),
    )?];
    assert!(matches!(
        PerformanceReport::try_calculate(
            &revision,
            &evidence,
            &periods,
            PerformancePolicy::new(
                market_squawk_portfolio::CashFlowTiming::EndOfPeriod,
                MoneyWeightedMethod::ModifiedDietz,
                NonZeroU32::MIN,
            ),
            retained_limits,
        ),
        Err(PortfolioError::RetainedBytesExceeded { .. })
    ));
    Ok(())
}

#[test]
fn analytics_reports_are_policy_explicit_bounded_and_revision_bound() -> TestResult {
    let revision = analytics_revision()?;
    let evidence = analytics_evidence(&revision, 4, 4)?;
    let usd = Currency::try_from("USD")?;
    let periods = vec![
        PerformancePeriod::try_new(
            Timestamp::from_unix_nanos(0),
            Timestamp::from_unix_nanos(1),
            money(1_000, usd),
            money(1_100, usd),
            money(0, usd),
        )?,
        PerformancePeriod::try_new(
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(2),
            money(1_100, usd),
            money(1_260, usd),
            money(100, usd),
        )?,
    ];
    let performance = PerformanceReport::try_calculate(
        &revision,
        &evidence,
        &periods,
        PerformancePolicy::new(
            market_squawk_portfolio::CashFlowTiming::EndOfPeriod,
            MoneyWeightedMethod::ModifiedDietz,
            NonZeroU32::MIN,
        ),
        super::limits()?,
    )?;
    assert_eq!(performance.revision_id(), revision.id());
    assert_eq!(
        performance.time_weighted_return().value(),
        Decimal::new(16, 2)
    );
    assert_eq!(
        performance.money_weighted_return().value(),
        Decimal::new(16, 2)
    );
    let start_weighted = PerformanceReport::try_calculate(
        &revision,
        &evidence,
        &periods,
        PerformancePolicy::new(
            market_squawk_portfolio::CashFlowTiming::StartOfPeriod,
            MoneyWeightedMethod::ModifiedDietz,
            NonZeroU32::MIN,
        ),
        super::limits()?,
    )?;
    assert_eq!(
        start_weighted.money_weighted_return().value(),
        Decimal::from(16_u32)
            .checked_div(Decimal::from(105_u32))
            .ok_or("modified Dietz expectation")?
    );

    let classifications = vec![
        InstrumentClassification::try_new(
            instrument(1)?,
            source("technology")?,
            source("issuer-a")?,
            VenueId::try_from("XNAS")?,
            usd,
            vec![FactorLoading::try_new(
                FeatureKey::try_new("market.beta", NonZeroU32::MIN)?,
                ExactRate::try_new(Decimal::new(12, 1), ExactDecimalScale::Unit)?,
            )?],
            super::limits()?,
        )?,
        InstrumentClassification::try_new(
            instrument(2)?,
            source("financials")?,
            source("issuer-b")?,
            VenueId::try_from("XNYS")?,
            usd,
            vec![FactorLoading::try_new(
                FeatureKey::try_new("market.beta", NonZeroU32::MIN)?,
                ExactRate::try_new(Decimal::new(8, 1), ExactDecimalScale::Unit)?,
            )?],
            super::limits()?,
        )?,
    ];
    let exposure =
        ExposureReport::try_calculate(&revision, &evidence, &classifications, super::limits()?)?;
    assert_eq!(exposure.revision_id(), revision.id());
    assert_eq!(exposure.instrument().len(), 2);
    assert_eq!(exposure.sector().len(), 2);
    assert_eq!(exposure.factor().len(), 1);
    assert_eq!(exposure.currency().len(), 1);
    assert_eq!(exposure.issuer().len(), 2);
    assert_eq!(exposure.venue().len(), 2);
    assert_eq!(exposure.allocation_total().amount(), Decimal::from(210_u32));
    assert_eq!(exposure.gross().amount(), Decimal::from(210_u32));

    let attribution = AttributionReport::try_calculate(
        &revision,
        &evidence,
        &[
            AttributionInput::try_new(
                instrument(1)?,
                money(120, usd),
                ExactRate::try_new(Decimal::new(1, 1), ExactDecimalScale::Unit)?,
            )?,
            AttributionInput::try_new(
                instrument(2)?,
                money(90, usd),
                ExactRate::try_new(Decimal::new(-5, 2), ExactDecimalScale::Unit)?,
            )?,
        ],
        super::limits()?,
    )?;
    assert_eq!(attribution.total().amount(), Decimal::new(75, 1));

    let targets = vec![
        RebalanceTarget::try_new(
            instrument(1)?,
            ExactRate::try_new(Decimal::new(6, 1), ExactDecimalScale::Unit)?,
        )?,
        RebalanceTarget::try_new(
            instrument(2)?,
            ExactRate::try_new(Decimal::new(4, 1), ExactDecimalScale::Unit)?,
        )?,
    ];
    let constraints = RebalanceConstraints::try_new(RebalanceConstraintInput {
        max_proposals: NonZeroUsize::new(4).ok_or("proposal bound")?,
        max_turnover: ExactRate::try_new(Decimal::new(5, 1), ExactDecimalScale::Unit)?,
        minimum_cash: money(700, usd),
        allow_short: false,
    })?;
    let rebalance =
        RebalanceProposal::try_calculate(&revision, &targets, constraints, super::limits()?)?;
    assert_eq!(rebalance.revision_id(), revision.id());
    assert!(rebalance.trades().len() <= 2);
    assert!(
        rebalance
            .trades()
            .iter()
            .all(|trade| !trade.value_change().amount().is_zero())
    );

    let turnover_only = RebalanceProposal::try_calculate(
        &revision,
        &targets,
        RebalanceConstraints::try_new(RebalanceConstraintInput {
            max_proposals: NonZeroUsize::new(4).ok_or("proposal bound")?,
            max_turnover: ExactRate::try_new(Decimal::new(5, 1), ExactDecimalScale::Unit)?,
            minimum_cash: money(0, usd),
            allow_short: false,
        })?,
        super::limits()?,
    )?;
    assert!(!turnover_only.constrained());
    assert_eq!(
        turnover_only.turnover().value(),
        Decimal::from(400_u32)
            .checked_div(Decimal::from(1_010_u32))
            .ok_or("one-way turnover expectation")?
    );

    let returns = ReturnSeries::try_new(
        vec![
            StatisticalInput::try_new(0.01, StatisticalUnit::Return, StatisticalScale::Unit)?,
            StatisticalInput::try_new(0.03, StatisticalUnit::Return, StatisticalScale::Unit)?,
            StatisticalInput::try_new(-0.01, StatisticalUnit::Return, StatisticalScale::Unit)?,
        ],
        Annualization::PeriodsPerYear(NonZeroU32::new(252).ok_or("annualization")?),
    )?;
    let benchmark = ReturnSeries::try_new(
        vec![
            StatisticalInput::try_new(0.00, StatisticalUnit::Return, StatisticalScale::Unit)?,
            StatisticalInput::try_new(0.02, StatisticalUnit::Return, StatisticalScale::Unit)?,
            StatisticalInput::try_new(0.00, StatisticalUnit::Return, StatisticalScale::Unit)?,
        ],
        Annualization::PeriodsPerYear(NonZeroU32::new(252).ok_or("annualization")?),
    )?;
    let losses = [
        StatisticalInput::try_new(1.0, StatisticalUnit::Currency(usd), StatisticalScale::Unit)?,
        StatisticalInput::try_new(2.0, StatisticalUnit::Currency(usd), StatisticalScale::Unit)?,
        StatisticalInput::try_new(8.0, StatisticalUnit::Currency(usd), StatisticalScale::Unit)?,
        StatisticalInput::try_new(9.0, StatisticalUnit::Currency(usd), StatisticalScale::Unit)?,
    ];
    let scenarios = vec![ScenarioDefinition::try_new(
        source("equity-crash")?,
        ShockComposition::Additive,
        vec![
            market_squawk_analytics::ScenarioShock::try_new(
                &format!("instrument-{}", instrument(1)?),
                ExactRate::try_new(Decimal::new(-2, 1), ExactDecimalScale::Unit)?,
            )?,
            market_squawk_analytics::ScenarioShock::try_new(
                &format!("instrument-{}", instrument(2)?),
                ExactRate::try_new(Decimal::new(-1, 1), ExactDecimalScale::Unit)?,
            )?,
        ],
        super::limits()?,
    )?];
    let risk = PortfolioRiskReport::try_calculate(
        &revision,
        &evidence,
        &returns,
        &benchmark,
        &losses,
        Quantile::try_new(0.75)?,
        &scenarios,
        super::limits()?,
    )?;
    assert_eq!(risk.revision_id(), revision.id());
    assert!(risk.tracking_error().value() > 0.0);
    assert_eq!(risk.value_at_risk().value(), 8.0);
    assert_eq!(risk.expected_shortfall().value(), 9.0);
    assert_eq!(risk.scenarios().len(), 1);
    assert!(risk.scenarios()[0].impact().amount().is_sign_negative());
    let evidence_digest = evidence.semantic_digest();
    assert_eq!(
        [
            performance.analytics_evidence_digest(),
            exposure.analytics_evidence_digest(),
            attribution.analytics_evidence_digest(),
            risk.analytics_evidence_digest(),
        ],
        [evidence_digest; 4]
    );
    Ok(())
}
