use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, Denomination, DigestAlgorithm, EvidenceDigest,
    FinancialError, InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize,
    Money, QuantityLots, RevisionNumber, TickSize, Timestamp,
};
use market_squawk_modeling::{ForecastCentralStatistic, ProductionFeatureRegistry};
use market_squawk_portfolio::{PortfolioRevisionToken, RebalanceTarget};
use market_squawk_valuation::{
    DecisionId, FairValueSelectionReceiptHash, MeasurementId, ValuationAmountBasis,
};
use rust_decimal::Decimal;
use std::{
    any::TypeId,
    num::{NonZeroU32, NonZeroUsize},
};

use crate::{
    AfterTaxPnlAvailability, AppendOutcome, AsOfSemantics, BenchmarkReturnAvailability,
    CandidateFlag, CandidateId, CandidateInput, CandidatePortfolioSizingState,
    CandidateSizingConstraints, CapacityRange, ComparisonOperator, CostAdjustedPitBacktestEvidence,
    DecisionActorId, DecisionAuthority, DecisionContentDigest, DecisionContractError,
    DecisionDossier, DecisionRepository, DecisionRepositoryError, DecisionRepositoryLimits,
    DecisionText, Dossier, DossierEvidence, DossierId, DossierReference, DossierSection,
    ExactFinancialRatio, ExactPositionScale, ExpectedGrossPricePnlAvailability,
    ExpectedReturnAvailability, FeasibleLotRangeAvailability, FeasibleNotionalRangeAvailability,
    ForecastCalibrationSummary, ForecastPriceRanges, GeneratedInvestmentProposal,
    GovernedTargetSet, GrossPricePnlAvailability, InvestmentAnalysisEvidence,
    InvestmentAnalysisEvidenceInput, InvestmentOutcomeProjection, InvestmentProjectionAuthority,
    InvestmentProposalAuthority, InvestmentProposalDecision, InvestmentProposalError,
    InvestmentProposalIndexOutcome, InvestmentSizingInputs, InvestmentSizingProjection,
    InvestmentTargetSet, InvestmentTargetSetId, LiquidityEvidence, LotRange,
    MarketReferenceAdjustmentBasis, MarketReferenceEvidence, MarketReferencePriceKind,
    NetPnlAvailability, NoActionReason, NonnegativeMoneyRange, NullPolicy, PortfolioPositionState,
    PortfolioRiskEvidence, PriceForecastEvidence, ProposalEvidenceWindow,
    ProposalExecutionEligibility, ProposalForecastVintageId, ProposalInvalidator,
    ProposalUnavailableReason, RankingDirection, RecommendationAction, RecommendationEvidenceKind,
    RecommendationPolicy, ReferenceMark, SavedScreen, ScreenConstraints, ScreenFeatureBinding,
    ScreenFeatureObservation, ScreenId, ScreenPredicate, ScreenRanking, ScreenRevision, ScreenRun,
    ScreenRunId, SignedMoneyRange, SizingCapacityAvailability, SizingCapacityEvidence,
    SizingConstraintCap, SizingConstraintKind, SizingUnavailableReason, TargetAssumption,
    TargetDecisionContext, TargetEvidence, TargetGovernanceInput, TargetInvalidationId,
    TargetMethod, TargetPriceCases, TargetPriceRange, TargetReview, TargetReviewDisposition,
    TargetReviewId, TargetStatus, ValuationEvidence,
};

fn content_digest(byte: u8) -> Result<DecisionContentDigest, DecisionContractError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]))
}

fn money(amount: i64, currency: &str) -> Result<Money, FinancialError> {
    Ok(Money::new(
        Decimal::new(amount, 2),
        Currency::try_from(currency)?,
    ))
}

const ALPHA_INSTRUMENT: &str = "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01";
const BETA_INSTRUMENT: &str = "018f8f6a-9d6f-7b43-9f38-55db5f4b0e02";
const DAY_NANOS: i64 = 86_400_000_000_000;

fn proposal_evidence(
    forecast_instrument: InstrumentId,
    market_age_seconds: i64,
    mark_amount: i64,
    fair_value_amount: i64,
) -> Result<InvestmentAnalysisEvidence, Box<dyn std::error::Error>> {
    proposal_evidence_for_position(
        forecast_instrument,
        market_age_seconds,
        mark_amount,
        fair_value_amount,
        PortfolioPositionState::NoPosition,
    )
}

fn proposal_evidence_for_position(
    forecast_instrument: InstrumentId,
    market_age_seconds: i64,
    mark_amount: i64,
    fair_value_amount: i64,
    position_state: PortfolioPositionState,
) -> Result<InvestmentAnalysisEvidence, Box<dyn std::error::Error>> {
    proposal_evidence_for_position_with_expected_terminal(
        forecast_instrument,
        market_age_seconds,
        mark_amount,
        fair_value_amount,
        position_state,
        true,
    )
}

fn proposal_evidence_for_position_with_expected_terminal(
    forecast_instrument: InstrumentId,
    market_age_seconds: i64,
    mark_amount: i64,
    fair_value_amount: i64,
    position_state: PortfolioPositionState,
    include_expected_terminal: bool,
) -> Result<InvestmentAnalysisEvidence, Box<dyn std::error::Error>> {
    let instrument_id = ALPHA_INSTRUMENT.parse::<InstrumentId>()?;
    let account_id = "018f8f6a-9d6f-7b43-9f38-55db5f4b1a01".parse::<AccountId>()?;
    let as_of = Timestamp::from_unix_nanos(400 * DAY_NANOS);
    let future_window = |observed_at: Timestamp,
                         available_at: Timestamp,
                         days: i64,
                         identity: u8|
     -> Result<ProposalEvidenceWindow, Box<dyn std::error::Error>> {
        Ok(ProposalEvidenceWindow::try_new(
            observed_at,
            available_at,
            as_of.checked_add_nanos(days * DAY_NANOS)?,
            content_digest(identity)?,
        )?)
    };
    let market = MarketReferenceEvidence::try_new(
        instrument_id,
        money(mark_amount, "USD")?,
        DataQuality::DirectVerified,
        MarketReferencePriceKind::LastTrade,
        MarketReferenceAdjustmentBasis::UnadjustedSpot,
        content_digest(101)?,
        content_digest(102)?,
        future_window(
            as_of.checked_sub_nanos(market_age_seconds * 1_000_000_000)?,
            as_of.checked_sub_nanos(1_000_000_000)?,
            1,
            103,
        )?,
    )?;
    let forecast_horizon_at = as_of.checked_add_nanos(365 * DAY_NANOS)?;
    let output_binding_identity = content_digest(105)?;
    let (
        expected_terminal_statistic,
        expected_terminal_price,
        expected_terminal_horizon_at,
        expected_terminal_statistic_identity,
    ) = if include_expected_terminal {
        (
            Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
            Some(money(13_000, "USD")?),
            Some(forecast_horizon_at),
            Some(output_binding_identity),
        )
    } else {
        (None, None, None, None)
    };
    let forecast = PriceForecastEvidence::try_new(
        forecast_instrument,
        TargetPriceCases::try_new(
            money(7_000, "USD")?,
            money(13_000, "USD")?,
            money(17_000, "USD")?,
        )?,
        ForecastPriceRanges::try_new(
            TargetPriceRange::try_new(money(6_000, "USD")?, money(8_000, "USD")?)?,
            TargetPriceRange::try_new(money(12_000, "USD")?, money(14_000, "USD")?)?,
            TargetPriceRange::try_new(money(16_000, "USD")?, money(18_000, "USD")?)?,
        )?,
        forecast_horizon_at,
        expected_terminal_statistic,
        expected_terminal_price,
        expected_terminal_horizon_at,
        expected_terminal_statistic_identity,
        ProposalForecastVintageId::try_from_bytes([104; 32])?,
        output_binding_identity,
        content_digest(106)?,
        content_digest(107)?,
        ForecastCalibrationSummary::try_new(
            800_000,
            780_000,
            NonZeroU32::new(100).ok_or(DecisionContractError::InvalidBound)?,
        )?,
        future_window(
            as_of.checked_sub_nanos(2 * DAY_NANOS)?,
            as_of.checked_sub_nanos(DAY_NANOS)?,
            30,
            108,
        )?,
    )?;
    let valuation = ValuationEvidence::try_new_for_test(
        instrument_id,
        money(fair_value_amount, "USD")?,
        ValuationAmountBasis::PerInstrumentUnit,
        as_of.checked_add_nanos(365 * DAY_NANOS)?,
        "6d".repeat(32).parse::<MeasurementId>()?,
        "6e".repeat(32).parse::<DecisionId>()?,
        "6f".repeat(32).parse::<FairValueSelectionReceiptHash>()?,
        future_window(
            as_of.checked_sub_nanos(5 * DAY_NANOS)?,
            as_of.checked_sub_nanos(4 * DAY_NANOS)?,
            60,
            112,
        )?,
    )?;
    let backtest = CostAdjustedPitBacktestEvidence::try_new(
        instrument_id,
        Currency::try_from("USD")?,
        365 * DAY_NANOS,
        BasisPoints::new(1_200),
        BasisPoints::new(2_000),
        BasisPoints::new(10),
        BasisPoints::new(5),
        BasisPoints::new(0),
        NonZeroU32::new(1_000).ok_or(DecisionContractError::InvalidBound)?,
        NonZeroU32::new(10).ok_or(DecisionContractError::InvalidBound)?,
        850_000,
        as_of.checked_sub_nanos(31 * DAY_NANOS)?,
        content_digest(113)?,
        content_digest(114)?,
        content_digest(115)?,
        content_digest(116)?,
        content_digest(117)?,
        content_digest(118)?,
        future_window(
            as_of.checked_sub_nanos(30 * DAY_NANOS)?,
            as_of.checked_sub_nanos(29 * DAY_NANOS)?,
            365,
            119,
        )?,
    )?;
    let liquidity = LiquidityEvidence::try_new(
        instrument_id,
        Currency::try_from("USD")?,
        BasisPoints::new(20),
        900_000,
        DataQuality::DirectVerified,
        content_digest(120)?,
        future_window(
            as_of.checked_sub_nanos(10_000_000_000)?,
            as_of.checked_sub_nanos(5_000_000_000)?,
            1,
            121,
        )?,
    )?;
    let portfolio_risk = PortfolioRiskEvidence::try_new(
        instrument_id,
        account_id,
        Currency::try_from("USD")?,
        PortfolioRevisionToken::from_bytes([122; 32]),
        position_state,
        900_000,
        content_digest(123)?,
        future_window(
            as_of.checked_sub_nanos(60_000_000_000)?,
            as_of.checked_sub_nanos(30_000_000_000)?,
            1,
            124,
        )?,
    )?;
    Ok(InvestmentAnalysisEvidence::new(
        InvestmentAnalysisEvidenceInput {
            instrument_id,
            currency: Currency::try_from("USD")?,
            account_id,
            as_of,
            market: Some(market),
            price_forecast: Some(forecast),
            valuation: Some(valuation),
            backtest: Some(backtest),
            liquidity: Some(liquidity),
            portfolio_risk: Some(portfolio_risk),
        },
    ))
}

fn target(expires_at: Timestamp) -> Result<InvestmentTargetSet, Box<dyn std::error::Error>> {
    let observed_at = Timestamp::from_unix_nanos(10);
    let reference = ReferenceMark::try_new(money(10_000, "USD")?, observed_at, content_digest(1)?)?;
    Ok(InvestmentTargetSet::try_new(
        InvestmentTargetSetId::try_new("target.alpha")?,
        RevisionNumber::new(1)?,
        DossierId::try_new("dossier.alpha")?,
        "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse::<InstrumentId>()?,
        reference,
        TargetPriceCases::try_new(
            money(8_000, "USD")?,
            money(12_000, "USD")?,
            money(16_000, "USD")?,
        )?,
        TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "USD")?)?,
        TargetPriceRange::try_new(money(13_000, "USD")?, money(14_000, "USD")?)?,
        TargetPriceRange::try_new(money(7_000, "USD")?, money(8_000, "USD")?)?,
        Timestamp::from_unix_nanos(20),
        Timestamp::from_unix_nanos(90),
        expires_at,
        content_digest(2)?,
    )?)
}

fn limits() -> Result<DecisionRepositoryLimits, DecisionRepositoryError> {
    DecisionRepositoryLimits::try_new(8, 8, 8, 8, 8, 8, 8, 8)
}

fn governed_target(
    revision: u32,
    supersedes: Option<(RevisionNumber, Timestamp)>,
) -> Result<GovernedTargetSet, Box<dyn std::error::Error>> {
    governed_target_for(
        "target.alpha",
        "dossier.alpha",
        ALPHA_INSTRUMENT.parse()?,
        revision,
        supersedes,
    )
}

fn governed_target_for(
    target_id: &str,
    dossier_id: &str,
    instrument_id: InstrumentId,
    revision: u32,
    supersedes: Option<(RevisionNumber, Timestamp)>,
) -> Result<GovernedTargetSet, Box<dyn std::error::Error>> {
    let content_identity_byte = if revision == 1 {
        2
    } else {
        10_u8
            .checked_add(u8::try_from(revision)?)
            .ok_or(DecisionContractError::InvalidBound)?
    };
    let created_at = if revision == 1 {
        20
    } else {
        20_i64
            .checked_add(i64::from(revision))
            .ok_or(DecisionContractError::InvalidBound)?
    };
    let core = InvestmentTargetSet::try_new(
        InvestmentTargetSetId::try_new(target_id)?,
        RevisionNumber::new(revision)?,
        DossierId::try_new(dossier_id)?,
        instrument_id,
        ReferenceMark::try_new(
            money(10_000, "USD")?,
            Timestamp::from_unix_nanos(10),
            content_digest(1)?,
        )?,
        TargetPriceCases::try_new(
            money(8_000, "USD")?,
            money(12_000, "USD")?,
            money(16_000, "USD")?,
        )?,
        TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "USD")?)?,
        TargetPriceRange::try_new(money(13_000, "USD")?, money(14_000, "USD")?)?,
        TargetPriceRange::try_new(money(7_000, "USD")?, money(8_000, "USD")?)?,
        Timestamp::from_unix_nanos(created_at),
        Timestamp::from_unix_nanos(90),
        Timestamp::from_unix_nanos(120),
        content_digest(content_identity_byte)?,
    )?;
    Ok(GovernedTargetSet::try_new(TargetGovernanceInput {
        target: core,
        add_case: money(10_500, "USD")?,
        method: TargetMethod::ForecastDistribution,
        assumptions: vec![TargetAssumption::new(
            DecisionText::try_new("revenue growth remains positive")?,
            content_digest(20)?,
        )],
        decision_context: TargetDecisionContext::new(DossierId::try_new(dossier_id)?, None),
        effective_at: Timestamp::from_unix_nanos(25 + i64::from(revision)),
        review_due_at: Timestamp::from_unix_nanos(80),
        supersedes,
        thesis: DecisionText::try_new("durable operating leverage")?,
        risks: vec![DecisionText::try_new("demand contraction")?],
        invalidation_conditions: vec![DecisionText::try_new("guidance withdrawn")?],
        evidence: TargetEvidence::new(Some(content_digest(21)?), None),
        mark_quality: DataQuality::DirectVerified,
        author: DecisionActorId::try_new("author.alpha")?,
        ruleset_version: NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
    })?)
}

fn repository_with_target_dossiers() -> Result<DecisionRepository, Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let metadata = registry
        .feature_registry()
        .entries()
        .find(|metadata| {
            metadata.is_point_in_time_compatible()
                && metadata.output_type()
                    == market_squawk_analytics::FeatureOutputType::StatisticalF64
        })
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
    let saved = SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.target-dossiers")?,
            RevisionNumber::new(1)?,
        ),
        content_digest(70)?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThanOrEqual,
            market_squawk_analytics::StatisticalF64::try_new(0.5)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(binding.clone(), RankingDirection::Descending),
        NonZeroUsize::new(2).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            market_squawk_analytics::StatisticalF64::try_new(0.8)?,
            market_squawk_analytics::StatisticalF64::try_new(1_000.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    )?;
    let mut authority = DecisionAuthority::new(DecisionRepository::try_new(limits()?)?);
    authority.save_screen(None, saved.clone())?;
    let run = ScreenRun::try_new(
        ScreenRunId::try_new("run.target-dossiers")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(50),
        content_digest(71)?,
        saved.universe_identity(),
        vec![binding.clone()],
    )?;
    let alpha = CandidateInput::try_new(
        CandidateId::try_new("candidate.target.alpha")?,
        ALPHA_INSTRUMENT.parse()?,
        vec![ScreenFeatureObservation::new(
            binding.clone(),
            Some(market_squawk_analytics::StatisticalF64::try_new(0.75)?),
        )],
        market_squawk_analytics::StatisticalF64::try_new(0.95)?,
        market_squawk_analytics::StatisticalF64::try_new(10_000.0)?,
        DataQuality::DirectVerified,
        None,
        vec![CandidateFlag::ModelDependent],
        content_digest(72)?,
    )?;
    let beta = CandidateInput::try_new(
        CandidateId::try_new("candidate.target.beta")?,
        BETA_INSTRUMENT.parse()?,
        vec![ScreenFeatureObservation::new(
            binding,
            Some(market_squawk_analytics::StatisticalF64::try_new(0.70)?),
        )],
        market_squawk_analytics::StatisticalF64::try_new(0.95)?,
        market_squawk_analytics::StatisticalF64::try_new(10_000.0)?,
        DataQuality::DirectVerified,
        None,
        vec![CandidateFlag::ModelDependent],
        content_digest(73)?,
    )?;
    let execution = authority.run_screen(run, vec![alpha, beta], Timestamp::from_unix_nanos(51))?;
    for (candidate_id, dossier_id, evidence, reference) in [
        ("candidate.target.alpha", "dossier.alpha", 74, 75),
        ("candidate.target.beta", "dossier.beta", 76, 77),
    ] {
        let candidate_id = CandidateId::try_new(candidate_id)?;
        let candidate = execution
            .candidates()
            .iter()
            .find(|candidate| candidate.record().id() == &candidate_id)
            .ok_or(DecisionRepositoryError::NotFound)?;
        let dossier = DecisionDossier::try_new(
            Dossier::try_new(
                DossierId::try_new(dossier_id)?,
                candidate.record(),
                Timestamp::from_unix_nanos(52),
                DossierEvidence::new(None, None, None, content_digest(evidence)?),
            )?,
            vec![DossierReference::new(
                DossierSection::Data,
                content_digest(reference)?,
            )],
        )?;
        authority.append_dossier(dossier)?;
    }
    Ok(authority.into_repository())
}

#[test]
fn target_set_rejects_mixed_currency() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceRange::try_new(money(9_000, "USD")?, money(10_000, "EUR")?);

    assert_eq!(result, Err(DecisionContractError::CurrencyMismatch));
    Ok(())
}

#[test]
fn activation_review_rejects_expired_target() -> Result<(), Box<dyn std::error::Error>> {
    let target = target(Timestamp::from_unix_nanos(100))?;

    let result = TargetReview::try_new(
        TargetReviewId::try_new("review.alpha")?,
        &target,
        DecisionActorId::try_new("reviewer.alpha")?,
        Timestamp::from_unix_nanos(100),
        TargetReviewDisposition::Activate,
        content_digest(3)?,
    );

    assert_eq!(result, Err(DecisionContractError::ExpiredActivation));
    Ok(())
}

#[test]
fn target_set_rejects_reversed_case_order() -> Result<(), Box<dyn std::error::Error>> {
    let result = TargetPriceCases::try_new(
        money(13_000, "USD")?,
        money(12_000, "USD")?,
        money(16_000, "USD")?,
    );

    assert_eq!(result, Err(DecisionContractError::InvalidPriceOrder));
    Ok(())
}

#[test]
fn screen_run_binds_exact_pit_inputs_and_rejects_semantic_substitution()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let metadata = registry
        .feature_registry()
        .entries()
        .find(|metadata| {
            metadata.is_point_in_time_compatible()
                && metadata.output_type()
                    == market_squawk_analytics::FeatureOutputType::StatisticalF64
        })
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
    let saved = SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.quality")?,
            RevisionNumber::new(1)?,
        ),
        content_digest(30)?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThanOrEqual,
            market_squawk_analytics::StatisticalF64::try_new(0.5)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(binding.clone(), RankingDirection::Descending),
        NonZeroUsize::new(2).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            market_squawk_analytics::StatisticalF64::try_new(0.8)?,
            market_squawk_analytics::StatisticalF64::try_new(1_000.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    )?;
    let mut authority = DecisionAuthority::new(DecisionRepository::try_new(limits()?)?);
    authority.save_screen(None, saved.clone())?;
    let dataset = content_digest(31)?;
    let universe = saved.universe_identity();
    let run = ScreenRun::try_new(
        ScreenRunId::try_new("run.quality.1")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(50),
        dataset,
        universe,
        vec![binding.clone()],
    )?;
    let input = CandidateInput::try_new(
        CandidateId::try_new("candidate.alpha")?,
        "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01".parse()?,
        vec![ScreenFeatureObservation::new(
            binding.clone(),
            Some(market_squawk_analytics::StatisticalF64::try_new(0.75)?),
        )],
        market_squawk_analytics::StatisticalF64::try_new(0.95)?,
        market_squawk_analytics::StatisticalF64::try_new(10_000.0)?,
        DataQuality::DirectVerified,
        None,
        vec![CandidateFlag::ModelDependent],
        content_digest(32)?,
    )?;
    let execution =
        authority.run_screen(run, vec![input.clone()], Timestamp::from_unix_nanos(51))?;

    assert_eq!(execution.run().dataset_identity(), dataset);
    assert_eq!(execution.run().universe_identity(), universe);
    assert_eq!(
        execution.run().feature_bindings(),
        std::slice::from_ref(&binding)
    );
    assert_eq!(execution.candidates().len(), 1);

    let duplicate_candidate_run = ScreenRun::try_new(
        ScreenRunId::try_new("run.quality.collision")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(52),
        dataset,
        universe,
        vec![binding.clone()],
    )?;
    assert_eq!(
        authority.run_screen(
            duplicate_candidate_run,
            vec![input],
            Timestamp::from_unix_nanos(52),
        ),
        Err(DecisionRepositoryError::Conflict)
    );

    let dossier = DecisionDossier::try_new(
        Dossier::try_new(
            DossierId::try_new("dossier.quality")?,
            execution.candidates()[0].record(),
            Timestamp::from_unix_nanos(52),
            DossierEvidence::new(None, None, None, content_digest(33)?),
        )?,
        vec![DossierReference::new(
            DossierSection::Data,
            content_digest(34)?,
        )],
    )?;
    authority.append_dossier(dossier.clone())?;

    let runs = authority.list_screen_runs(1)?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run().id(), execution.run().id());
    assert_eq!(runs[0].candidate_count(), 1);
    assert!(
        authority
            .list_screen_runs_after(Some(execution.run().id()), 1)?
            .is_empty()
    );
    assert_eq!(
        authority.list_screen_runs_after(Some(&ScreenRunId::try_new("run.unknown")?), 1),
        Err(DecisionRepositoryError::NotFound)
    );

    let dossiers = authority.list_candidate_dossiers(execution.candidates()[0].record().id(), 1)?;
    assert_eq!(dossiers, vec![dossier]);
    assert!(
        authority
            .list_candidate_dossiers_after(
                execution.candidates()[0].record().id(),
                Some(&DossierId::try_new("dossier.quality")?),
                1,
            )?
            .is_empty()
    );

    let wrong_metadata = registry
        .feature_registry()
        .entries()
        .find(|candidate| candidate.key() != metadata.key())
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let wrong_binding = ScreenFeatureBinding::new(
        wrong_metadata.key().clone(),
        wrong_metadata.semantic_digest(),
    );
    let substituted = ScreenRun::try_new(
        ScreenRunId::try_new("run.quality.2")?,
        saved.revision().clone(),
        Timestamp::from_unix_nanos(52),
        content_digest(34)?,
        universe,
        vec![wrong_binding],
    )?;
    assert_eq!(
        authority.run_screen(substituted, Vec::new(), Timestamp::from_unix_nanos(53)),
        Err(DecisionRepositoryError::EvidenceMismatch)
    );
    Ok(())
}

#[test]
fn saved_screen_rejects_sql_like_unknown_features_without_a_formula_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let injected_key = market_squawk_analytics::FeatureKey::try_new(
        "select.from.securities",
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
    )?;
    let injected = ScreenFeatureBinding::new(
        injected_key,
        registry
            .feature_registry()
            .entries()
            .next()
            .ok_or(DecisionContractError::UnknownScreenFeature)?
            .semantic_digest(),
    );
    let result = SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.injected")?,
            RevisionNumber::new(1)?,
        ),
        content_digest(40)?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            injected.clone(),
            ComparisonOperator::Equal,
            market_squawk_analytics::StatisticalF64::try_new(1.0)?,
            NullPolicy::Exclude,
        )],
        ScreenRanking::new(injected, RankingDirection::Descending),
        NonZeroUsize::new(1).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            market_squawk_analytics::StatisticalF64::try_new(0.5)?,
            market_squawk_analytics::StatisticalF64::try_new(0.0)?,
            vec![DataQuality::DirectVerified],
        )?,
        registry.feature_registry(),
    );

    assert_eq!(result, Err(DecisionContractError::UnknownScreenFeature));
    assert!(
        market_squawk_analytics::FeatureKey::try_new(
            "price * 2; select *",
            NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn target_revision_history_is_append_only_and_not_a_rebalance_target()
-> Result<(), Box<dyn std::error::Error>> {
    let mut repository = repository_with_target_dossiers()?;
    let first = governed_target(1, None)?;
    let second = governed_target(
        2,
        Some((RevisionNumber::new(1)?, Timestamp::from_unix_nanos(27))),
    )?;
    assert_eq!(
        repository.append_target(None, first.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        repository.append_target(Some(RevisionNumber::new(1)?), second.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        repository.append_target(Some(RevisionNumber::new(1)?), second.clone())?,
        AppendOutcome::AlreadyPresent
    );

    let revisions = repository
        .target_revisions(first.target().id())
        .collect::<Vec<_>>();
    assert_eq!(revisions, vec![&first, &second]);
    assert_eq!(
        repository.target_status(first.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::Superseded
    );
    assert_ne!(
        TypeId::of::<GovernedTargetSet>(),
        TypeId::of::<RebalanceTarget>()
    );

    let before_rejections = repository.try_snapshot()?;
    let index_before_rejections = repository.list_target_index(8)?;
    let missing_dossier = governed_target_for(
        "target.missing-dossier",
        "dossier.missing",
        ALPHA_INSTRUMENT.parse()?,
        1,
        None,
    )?;
    assert_eq!(
        repository.append_target(None, missing_dossier),
        Err(DecisionRepositoryError::NotFound)
    );
    let cross_instrument = governed_target_for(
        "target.cross-instrument",
        "dossier.beta",
        ALPHA_INSTRUMENT.parse()?,
        1,
        None,
    )?;
    assert_eq!(
        repository.append_target(None, cross_instrument),
        Err(DecisionRepositoryError::EvidenceMismatch)
    );
    assert_eq!(repository.try_snapshot()?, before_rejections);
    assert_eq!(repository.list_target_index(8)?, index_before_rejections);

    let mut recovered = DecisionRepository::recover(limits()?, before_rejections.clone())?;
    assert_eq!(recovered.try_snapshot()?, before_rejections);
    assert_eq!(
        recovered.append_target(Some(RevisionNumber::new(1)?), second)?,
        AppendOutcome::AlreadyPresent
    );
    Ok(())
}

#[test]
fn target_index_discovers_only_each_series_latest_immutable_state()
-> Result<(), Box<dyn std::error::Error>> {
    let mut repository = repository_with_target_dossiers()?;
    let first = governed_target(1, None)?;
    let second = governed_target(
        2,
        Some((RevisionNumber::new(1)?, Timestamp::from_unix_nanos(27))),
    )?;
    repository.append_target(None, first.clone())?;
    repository.append_target(Some(RevisionNumber::new(1)?), second.clone())?;

    let index = repository.list_target_index(1)?;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].id(), second.target().id());
    assert_eq!(index[0].revision(), RevisionNumber::new(2)?);
    assert_eq!(index[0].instrument_id(), second.target().instrument_id());
    assert_eq!(index[0].status(), TargetStatus::PendingReview);
    assert!(
        repository
            .list_target_index_after(Some(second.target().id()), 1)?
            .is_empty()
    );
    assert_eq!(
        repository
            .list_target_index_after(Some(&InvestmentTargetSetId::try_new("target.unknown")?), 1),
        Err(DecisionRepositoryError::NotFound)
    );
    assert_eq!(
        repository.list_target_index(0),
        Err(DecisionRepositoryError::InvalidLimits)
    );
    Ok(())
}

#[test]
fn every_invalidator_appends_idempotent_needs_review_without_replacing_approval()
-> Result<(), Box<dyn std::error::Error>> {
    let mut repository = repository_with_target_dossiers()?;
    let governed = governed_target(1, None)?;
    let unchanged = governed.clone();
    repository.append_target(None, governed.clone())?;
    let review = TargetReview::try_new(
        TargetReviewId::try_new("review.activate")?,
        governed.target(),
        DecisionActorId::try_new("reviewer.alpha")?,
        Timestamp::from_unix_nanos(60),
        TargetReviewDisposition::Activate,
        content_digest(50)?,
    )?;
    repository.append_review(review.clone())?;
    assert_eq!(
        repository.target_status(governed.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::Active
    );

    for (index, kind) in crate::InvalidationKind::ALL.into_iter().enumerate() {
        let invalidation = crate::TargetInvalidation::try_new(
            TargetInvalidationId::try_new(format!("invalidate.{index}"))?,
            governed.target(),
            kind,
            DecisionActorId::try_new("reviewer.alpha")?,
            Timestamp::from_unix_nanos(70 + i64::try_from(index)?),
            content_digest(60 + u8::try_from(index)?)?,
        )?;
        assert_eq!(invalidation.actor().as_str(), "reviewer.alpha");
        assert_eq!(
            repository.append_invalidation(invalidation.clone())?,
            AppendOutcome::Appended
        );
        assert_eq!(
            repository.append_invalidation(invalidation)?,
            AppendOutcome::AlreadyPresent
        );
    }

    assert_eq!(
        repository.target_status(governed.target().id(), RevisionNumber::new(1)?)?,
        TargetStatus::NeedsReview
    );
    assert_eq!(
        repository
            .target_revisions(governed.target().id())
            .collect::<Vec<_>>(),
        vec![&unchanged]
    );
    assert_eq!(
        repository
            .reviews(governed.target().id(), RevisionNumber::new(1)?)
            .collect::<Vec<_>>(),
        vec![&review]
    );
    assert_eq!(
        repository
            .invalidations(governed.target().id(), RevisionNumber::new(1)?)
            .count(),
        crate::InvalidationKind::ALL.len()
    );
    Ok(())
}

#[test]
fn generated_investment_proposal_is_deterministic_and_abstains_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let alpha = ALPHA_INSTRUMENT.parse::<InstrumentId>()?;
    let beta = BETA_INSTRUMENT.parse::<InstrumentId>()?;
    let policy = RecommendationPolicy::v1()?;
    let evidence = proposal_evidence(alpha, 10, 10_000, 12_500)?;

    let receipt_hash = "6f".repeat(32).parse::<FairValueSelectionReceiptHash>()?;
    let receipt_window = ProposalEvidenceWindow::try_new(
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(2),
        Timestamp::from_unix_nanos(10),
        content_digest(111)?,
    )?;
    let recovered_valuation = ValuationEvidence::try_recover_receipt_bound_projection(
        alpha,
        money(12_500, "USD")?,
        ValuationAmountBasis::PerInstrumentUnit,
        Timestamp::from_unix_nanos(20),
        "6d".repeat(32).parse::<MeasurementId>()?,
        "6e".repeat(32).parse::<DecisionId>()?,
        receipt_hash,
        receipt_window,
    )?;
    assert_eq!(recovered_valuation.selection_receipt_hash(), receipt_hash);
    assert!(matches!(
        ValuationEvidence::try_recover_receipt_bound_projection(
            alpha,
            money(12_500, "USD")?,
            ValuationAmountBasis::PerInstrumentUnit,
            Timestamp::from_unix_nanos(20),
            "6d".repeat(32).parse::<MeasurementId>()?,
            "6e".repeat(32).parse::<DecisionId>()?,
            receipt_hash,
            ProposalEvidenceWindow::try_new(
                Timestamp::from_unix_nanos(1),
                Timestamp::from_unix_nanos(2),
                Timestamp::from_unix_nanos(10),
                content_digest(112)?,
            )?,
        ),
        Err(InvestmentProposalError::InvalidValuationSelection)
    ));

    let first = InvestmentProposalAuthority::generate(evidence.clone(), policy.clone())?;
    let second = InvestmentProposalAuthority::generate(evidence.clone(), policy.clone())?;
    assert_eq!(first, second);
    let generated = match &first {
        InvestmentProposalDecision::Generated(value) => value,
        InvestmentProposalDecision::NoAction(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("complete bullish evidence must generate a proposal".into());
        }
    };
    assert_eq!(generated.action(), RecommendationAction::Buy);
    assert_eq!(generated.action_zone_semantics_version(), NonZeroU32::MIN);
    assert_eq!(
        generated.expires_at(),
        Timestamp::from_unix_nanos(400 * DAY_NANOS).checked_add_nanos(50_000_000_000)?
    );
    assert_eq!(
        generated.execution_eligibility(),
        ProposalExecutionEligibility::ResearchOnlyExecutionIneligible
    );
    assert_eq!(
        generated.proposal_time_benchmark_availability(),
        crate::ProposalTimeBenchmarkAvailability::UnavailableByPolicyV1
    );
    assert_eq!(
        generated.action_specific_cost_availability(),
        crate::ActionSpecificCostAvailability::UnavailableByPolicyV1
    );
    assert_eq!(
        generated.confidence().meaning(),
        crate::RecommendationConfidenceMeaning::PolicyWeightedEvidenceReliabilityV1
    );
    assert_eq!(generated.confidence().value_ppm(), 920_000);
    let Some(retained_backtest) = generated.evidence().backtest() else {
        return Err("generated proposal must retain admitted backtest evidence".into());
    };
    assert_eq!(retained_backtest.fee_basis_points(), BasisPoints::new(10));
    assert_eq!(
        retained_backtest.slippage_basis_points(),
        BasisPoints::new(5)
    );
    assert_eq!(
        retained_backtest.maximum_random_slippage_basis_points(),
        BasisPoints::new(0)
    );
    let ladder = generated.price_ladder();
    assert_eq!(
        ladder.downside_range(),
        TargetPriceRange::try_new(money(6_000, "USD")?, money(8_000, "USD")?)?
    );
    assert_eq!(
        ladder.exit_range(),
        TargetPriceRange::try_new(money(9_050, "USD")?, money(9_470, "USD")?)?
    );
    assert_eq!(
        ladder.add_range(),
        TargetPriceRange::try_new(money(10_310, "USD")?, money(10_730, "USD")?)?
    );
    assert_eq!(ladder.add_case(), money(10_520, "USD")?);
    assert_eq!(
        ladder.entry_range(),
        TargetPriceRange::try_new(money(11_150, "USD")?, money(11_570, "USD")?)?
    );
    assert_eq!(
        ladder.base_range(),
        TargetPriceRange::try_new(money(12_200, "USD")?, money(13_400, "USD")?)?
    );
    assert_eq!(ladder.cases().base(), money(12_800, "USD")?);
    assert_eq!(
        ladder.trim_range(),
        TargetPriceRange::try_new(money(13_790, "USD")?, money(14_180, "USD")?)?
    );
    assert_eq!(
        ladder.upside_range(),
        TargetPriceRange::try_new(money(16_000, "USD")?, money(18_000, "USD")?)?
    );
    assert_eq!(
        generated.action_trigger_reference_zone(),
        Some(ladder.entry_range())
    );
    assert_eq!(
        generated.action_trigger_floor_exclusive(),
        Some(ladder.exit_range().upper())
    );
    assert_eq!(generated.action_trigger_floor_inclusive(), None);
    assert_eq!(
        generated.action_trigger_ceiling_inclusive(),
        Some(ladder.entry_range().upper())
    );

    let usd = Currency::try_from("USD")?;
    let definition_revision = InstrumentDefinitionRevision::try_from(1)?;
    let execution_terms = InstrumentExecutionTerms::try_new(
        alpha,
        definition_revision,
        TickSize::try_from_decimal(Decimal::new(1, 1))?,
        LotSize::try_from_decimal(Decimal::new(5, 1))?,
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )?;
    let outcome = InvestmentOutcomeProjection::try_from_proposal(
        generated,
        Some(ExactPositionScale::new(
            execution_terms,
            QuantityLots::new(3)?,
        )),
    )?;
    assert_eq!(outcome.binding().proposal_id(), generated.proposal_id());
    assert_eq!(
        outcome.binding().derivation_digest(),
        generated.derivation_digest()
    );
    assert_eq!(
        outcome.authority(),
        InvestmentProjectionAuthority::AnalysisOnlyNoMutationNoExecution
    );
    assert_eq!(
        outcome.downside().absolute_change(),
        SignedMoneyRange::try_new(money(-4_000, "USD")?, money(-2_000, "USD")?)?
    );
    assert_eq!(
        outcome
            .downside()
            .gross_return_from_mark()
            .lower()
            .numerator(),
        money(-4_000, "USD")?
    );
    assert_eq!(
        outcome
            .downside()
            .gross_return_from_mark()
            .lower()
            .denominator(),
        money(10_000, "USD")?
    );
    assert_eq!(
        outcome.base().gross_return_from_mark().lower().numerator(),
        money(2_200, "USD")?
    );
    assert_eq!(
        outcome.downside().gross_price_pnl(),
        GrossPricePnlAvailability::Available(SignedMoneyRange::try_new(
            money(-6_000, "USD")?,
            money(-3_000, "USD")?,
        )?)
    );
    assert_eq!(
        outcome.entry_distance().absolute_distance(),
        SignedMoneyRange::try_new(money(1_150, "USD")?, money(1_570, "USD")?)?
    );
    assert_eq!(
        outcome.exit_distance().absolute_distance(),
        SignedMoneyRange::try_new(money(-950, "USD")?, money(-530, "USD")?)?
    );
    assert_eq!(
        outcome.expected_return(),
        ExpectedReturnAvailability::Available(ExactFinancialRatio::try_new(
            money(3_000, "USD")?,
            money(10_000, "USD")?,
        )?)
    );
    assert_eq!(
        outcome.expected_gross_price_pnl(),
        ExpectedGrossPricePnlAvailability::Available(money(4_500, "USD")?)
    );
    assert_eq!(
        outcome.net_pnl(),
        NetPnlAvailability::UnavailableExactForwardCostEvidenceNotSupplied
    );
    assert_eq!(
        outcome.benchmark_return(),
        BenchmarkReturnAvailability::UnavailableExactProposalTimeBenchmarkEvidenceNotSupplied
    );
    assert_eq!(
        outcome.after_tax_pnl(),
        AfterTaxPnlAvailability::UnavailableExactTaxEvidenceNotSupplied
    );
    assert_ne!(outcome.result_digest().bytes(), [0; 32]);
    assert_eq!(
        outcome,
        InvestmentOutcomeProjection::try_from_proposal(
            generated,
            Some(ExactPositionScale::new(
                execution_terms,
                QuantityLots::new(3)?,
            )),
        )?
    );
    let unscaled_outcome = InvestmentOutcomeProjection::try_from_proposal(generated, None)?;
    assert_eq!(
        unscaled_outcome.downside().gross_price_pnl(),
        GrossPricePnlAvailability::UnavailableExactQuantityNotSupplied
    );
    assert_eq!(
        unscaled_outcome.expected_return(),
        outcome.expected_return()
    );
    assert_eq!(
        unscaled_outcome.expected_gross_price_pnl(),
        ExpectedGrossPricePnlAvailability::UnavailableExactQuantityNotSupplied
    );
    assert_ne!(unscaled_outcome.result_digest(), outcome.result_digest());

    let range_only = InvestmentProposalAuthority::generate(
        proposal_evidence_for_position_with_expected_terminal(
            alpha,
            10,
            10_000,
            12_500,
            PortfolioPositionState::NoPosition,
            false,
        )?,
        policy.clone(),
    )?;
    let range_only_generated = match &range_only {
        InvestmentProposalDecision::Generated(value) => value,
        InvestmentProposalDecision::NoAction(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("complete range-only evidence must still generate a proposal".into());
        }
    };
    assert_ne!(range_only_generated.analysis_id(), generated.analysis_id());
    assert_ne!(
        range_only_generated.derivation_digest(),
        generated.derivation_digest()
    );
    assert_ne!(range_only_generated.proposal_id(), generated.proposal_id());
    let range_only_outcome = InvestmentOutcomeProjection::try_from_proposal(
        range_only_generated,
        Some(ExactPositionScale::new(
            execution_terms,
            QuantityLots::new(3)?,
        )),
    )?;
    assert_eq!(
        range_only_outcome.expected_return(),
        ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied
    );
    assert_eq!(
        range_only_outcome.expected_gross_price_pnl(),
        ExpectedGrossPricePnlAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied
    );

    let positioned = InvestmentProposalAuthority::generate(
        proposal_evidence_for_position(
            alpha,
            10,
            10_000,
            12_500,
            PortfolioPositionState::Position {
                add_allowed: true,
                trim_allowed: true,
                exit_allowed: true,
            },
        )?,
        policy.clone(),
    )?;
    let sizing_proposal = match &positioned {
        InvestmentProposalDecision::Generated(value) => value,
        InvestmentProposalDecision::NoAction(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("position-aware evidence must generate a sizing proposal".into());
        }
    };
    assert_eq!(sizing_proposal.action(), RecommendationAction::Add);
    let as_of = sizing_proposal.evidence().as_of();
    let account_id = sizing_proposal.evidence().account_id();
    let Some(portfolio_risk) = sizing_proposal.evidence().portfolio_risk() else {
        return Err("generated proposal must retain portfolio-risk evidence".into());
    };
    let portfolio_revision = portfolio_risk.portfolio_revision().clone();
    let selected_mark = money(10_000, "USD")?;
    let capacity_evidence = |range: CapacityRange,
                             identity: u8|
     -> Result<SizingCapacityEvidence, Box<dyn std::error::Error>> {
        Ok(SizingCapacityEvidence::try_new(
            alpha,
            account_id,
            portfolio_revision.clone(),
            definition_revision,
            selected_mark,
            range,
            content_digest(identity)?,
            as_of.checked_sub_nanos(3_000_000_000)?,
            as_of.checked_sub_nanos(2_000_000_000)?,
            as_of.checked_add_nanos(40_000_000_000)?,
        )?)
    };
    let zero_to_five = LotRange::try_new(QuantityLots::new(0)?, QuantityLots::new(5)?)?;
    let liquidity_capacity = SizingCapacityAvailability::Available(Box::new(capacity_evidence(
        CapacityRange::Lots(zero_to_five),
        201,
    )?));
    let risk_capacity = SizingCapacityAvailability::Available(Box::new(capacity_evidence(
        CapacityRange::Notional(NonnegativeMoneyRange::try_new(
            money(0, "USD")?,
            money(25_000, "USD")?,
        )?),
        202,
    )?));
    let forward_cost_capacity = SizingCapacityAvailability::Available(Box::new(capacity_evidence(
        CapacityRange::Lots(zero_to_five),
        203,
    )?));
    let portfolio_state = CandidatePortfolioSizingState::try_new(
        account_id,
        alpha,
        portfolio_revision,
        money(100_000, "USD")?,
        money(25_000, "USD")?,
        QuantityLots::new(2)?,
    )?;
    let sizing_constraints =
        CandidateSizingConstraints::try_new(money(10_000, "USD")?, 2_100, 4_900, 1_000)?;
    let sizing_inputs = InvestmentSizingInputs::new(
        as_of,
        execution_terms,
        selected_mark,
        portfolio_state.clone(),
        sizing_constraints,
        liquidity_capacity.clone(),
        risk_capacity.clone(),
        forward_cost_capacity,
    );
    let sizing =
        InvestmentSizingProjection::try_from_proposal(sizing_proposal, sizing_inputs.clone())?;
    assert_eq!(
        sizing.binding().proposal_id(),
        sizing_proposal.proposal_id()
    );
    assert_eq!(
        sizing.binding().derivation_digest(),
        sizing_proposal.derivation_digest()
    );
    assert_eq!(
        sizing.authority(),
        InvestmentProjectionAuthority::AnalysisOnlyNoMutationNoExecution
    );
    assert_eq!(sizing.per_lot_notional(), money(5_000, "USD")?);
    assert_eq!(sizing.per_lot_downside_loss(), money(2_000, "USD")?);
    assert_eq!(
        sizing.hard_feasible_lots(),
        &FeasibleLotRangeAvailability::Available(zero_to_five)
    );
    let five_only = LotRange::try_new(QuantityLots::new(5)?, QuantityLots::new(5)?)?;
    assert_eq!(
        sizing.preferred_feasible_lots(),
        &FeasibleLotRangeAvailability::Available(five_only)
    );
    assert_eq!(
        sizing.hard_feasible_target_notional(),
        &FeasibleNotionalRangeAvailability::Available(NonnegativeMoneyRange::try_new(
            money(0, "USD")?,
            money(25_000, "USD")?,
        )?)
    );
    assert_eq!(
        sizing.preferred_feasible_target_notional(),
        &FeasibleNotionalRangeAvailability::Available(NonnegativeMoneyRange::try_new(
            money(25_000, "USD")?,
            money(25_000, "USD")?,
        )?)
    );
    assert_eq!(
        sizing.preferred_weight_rounding().lower_round_up_excess(),
        money(4_000, "USD")?
    );
    assert_eq!(
        sizing
            .preferred_weight_rounding()
            .upper_round_down_remainder(),
        money(4_000, "USD")?
    );
    assert_eq!(
        sizing.constraint_caps(),
        &[
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::CashReserve,
                lot_range: zero_to_five,
                capacity_identity: None,
            },
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::DownsideLoss,
                lot_range: zero_to_five,
                capacity_identity: None,
            },
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::Liquidity,
                lot_range: zero_to_five,
                capacity_identity: Some(content_digest(201)?),
            },
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::PortfolioRisk,
                lot_range: zero_to_five,
                capacity_identity: Some(content_digest(202)?),
            },
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::ForwardCost,
                lot_range: zero_to_five,
                capacity_identity: Some(content_digest(203)?),
            },
            SizingConstraintCap::Available {
                kind: SizingConstraintKind::PreferredWeight,
                lot_range: LotRange::try_new(QuantityLots::new(5)?, QuantityLots::new(9)?)?,
                capacity_identity: None,
            },
        ]
    );
    assert_eq!(
        sizing.hard_binding_caps(),
        &[
            SizingConstraintKind::CashReserve,
            SizingConstraintKind::DownsideLoss,
            SizingConstraintKind::Liquidity,
            SizingConstraintKind::PortfolioRisk,
            SizingConstraintKind::ForwardCost,
        ]
    );
    assert_eq!(
        sizing.preferred_binding_caps(),
        &[
            SizingConstraintKind::CashReserve,
            SizingConstraintKind::DownsideLoss,
            SizingConstraintKind::Liquidity,
            SizingConstraintKind::PortfolioRisk,
            SizingConstraintKind::ForwardCost,
            SizingConstraintKind::PreferredWeight,
        ]
    );
    assert_ne!(sizing.result_digest().bytes(), [0; 32]);
    assert_eq!(
        sizing,
        InvestmentSizingProjection::try_from_proposal(sizing_proposal, sizing_inputs)?
    );

    let unavailable_cost_sizing = InvestmentSizingProjection::try_from_proposal(
        sizing_proposal,
        InvestmentSizingInputs::new(
            as_of,
            execution_terms,
            selected_mark,
            portfolio_state,
            sizing_constraints,
            liquidity_capacity,
            risk_capacity,
            SizingCapacityAvailability::UnavailableNotSupplied,
        ),
    )?;
    assert_eq!(
        unavailable_cost_sizing.hard_feasible_lots(),
        &FeasibleLotRangeAvailability::Unavailable(Box::new([
            SizingUnavailableReason::CapacityNotSupplied(SizingConstraintKind::ForwardCost),
        ]))
    );
    assert_eq!(
        unavailable_cost_sizing.preferred_feasible_lots(),
        &FeasibleLotRangeAvailability::Unavailable(Box::new([
            SizingUnavailableReason::CapacityNotSupplied(SizingConstraintKind::ForwardCost),
        ]))
    );
    assert_eq!(
        unavailable_cost_sizing.hard_feasible_target_notional(),
        &FeasibleNotionalRangeAvailability::Unavailable(Box::new([
            SizingUnavailableReason::CapacityNotSupplied(SizingConstraintKind::ForwardCost),
        ]))
    );

    let recovered = InvestmentProposalAuthority::try_recover_generated(
        evidence,
        policy.clone(),
        generated.analysis_id(),
        generated.derivation_digest(),
        generated.proposal_id(),
    )?;
    assert_eq!(&recovered, generated);
    assert_ne!(
        TypeId::of::<GeneratedInvestmentProposal>(),
        TypeId::of::<RebalanceTarget>()
    );
    let mut proposal_repository = DecisionRepository::try_new(limits()?)?;
    assert_eq!(
        proposal_repository.append_investment_proposal(first.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        proposal_repository.append_investment_proposal(first.clone())?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(
        proposal_repository.investment_proposal(first.analysis_id()),
        Some(&first)
    );
    let index = proposal_repository.list_investment_proposal_index(1)?;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].analysis_id(), first.analysis_id());
    assert_eq!(
        index[0].outcome(),
        InvestmentProposalIndexOutcome::Generated(RecommendationAction::Buy)
    );
    assert!(
        proposal_repository
            .list_investment_proposal_index_after(Some(first.analysis_id()), 1)?
            .is_empty()
    );
    let recovered_repository =
        DecisionRepository::recover(limits()?, proposal_repository.try_snapshot()?)?;
    assert_eq!(
        recovered_repository.investment_proposal(first.analysis_id()),
        Some(&first)
    );

    let generate_for_position =
        |mark_amount: i64,
         position_state: PortfolioPositionState|
         -> Result<GeneratedInvestmentProposal, Box<dyn std::error::Error>> {
            match InvestmentProposalAuthority::generate(
                proposal_evidence_for_position(alpha, 10, mark_amount, 12_500, position_state)?,
                policy.clone(),
            )? {
                InvestmentProposalDecision::Generated(value) => Ok(value),
                InvestmentProposalDecision::NoAction(_)
                | InvestmentProposalDecision::Unavailable(_) => {
                    Err("complete evidence in an actionable or hold zone must generate".into())
                }
            }
        };
    let entry_ceiling_buy = generate_for_position(11_570, PortfolioPositionState::NoPosition)?;
    assert_eq!(entry_ceiling_buy.action(), RecommendationAction::Buy);
    assert_eq!(
        entry_ceiling_buy.action_trigger_ceiling_inclusive(),
        Some(entry_ceiling_buy.price_ladder().entry_range().upper())
    );

    match InvestmentProposalAuthority::generate(
        proposal_evidence_for_position(
            alpha,
            10,
            9_470,
            12_500,
            PortfolioPositionState::NoPosition,
        )?,
        policy.clone(),
    )? {
        InvestmentProposalDecision::NoAction(value) => {
            assert_eq!(value.reason(), NoActionReason::PositionStateNotActionable);
        }
        InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("a no-position mark at invalidation must not generate Buy".into());
        }
    }

    let add = generate_for_position(
        10_730,
        PortfolioPositionState::Position {
            add_allowed: true,
            trim_allowed: true,
            exit_allowed: true,
        },
    )?;
    assert_eq!(add.action(), RecommendationAction::Add);
    assert_eq!(
        add.action_trigger_reference_zone(),
        Some(add.price_ladder().add_range())
    );
    assert_eq!(
        add.action_trigger_floor_exclusive(),
        Some(add.price_ladder().exit_range().upper())
    );
    assert_eq!(
        add.action_trigger_ceiling_inclusive(),
        Some(add.price_ladder().add_range().upper())
    );

    let hold = generate_for_position(
        12_000,
        PortfolioPositionState::Position {
            add_allowed: true,
            trim_allowed: true,
            exit_allowed: true,
        },
    )?;
    assert_eq!(hold.action(), RecommendationAction::Hold);
    assert_eq!(hold.action_trigger_reference_zone(), None);
    assert_eq!(hold.action_trigger_floor_exclusive(), None);
    assert_eq!(hold.action_trigger_floor_inclusive(), None);
    assert_eq!(hold.action_trigger_ceiling_inclusive(), None);

    let trim = generate_for_position(
        13_790,
        PortfolioPositionState::Position {
            add_allowed: true,
            trim_allowed: true,
            exit_allowed: true,
        },
    )?;
    assert_eq!(trim.action(), RecommendationAction::Trim);
    assert_eq!(
        trim.action_trigger_reference_zone(),
        Some(trim.price_ladder().trim_range())
    );
    assert_eq!(
        trim.action_trigger_floor_inclusive(),
        Some(trim.price_ladder().trim_range().lower())
    );
    assert_eq!(trim.action_trigger_ceiling_inclusive(), None);

    let sell = generate_for_position(
        9_470,
        PortfolioPositionState::Position {
            add_allowed: true,
            trim_allowed: true,
            exit_allowed: true,
        },
    )?;
    assert_eq!(sell.action(), RecommendationAction::Sell);
    assert_eq!(
        sell.action_trigger_reference_zone(),
        Some(sell.price_ladder().exit_range())
    );
    assert_eq!(sell.action_trigger_floor_exclusive(), None);
    assert_eq!(
        sell.action_trigger_ceiling_inclusive(),
        Some(sell.price_ladder().exit_range().upper())
    );

    for mark_amount in [9_470, 10_730, 13_790] {
        let denied = generate_for_position(
            mark_amount,
            PortfolioPositionState::Position {
                add_allowed: false,
                trim_allowed: false,
                exit_allowed: false,
            },
        )?;
        assert_eq!(denied.action(), RecommendationAction::Hold);
        assert_eq!(denied.action_trigger_reference_zone(), None);
    }

    let exit_permission_above_invalidation = generate_for_position(
        20_000,
        PortfolioPositionState::Position {
            add_allowed: false,
            trim_allowed: false,
            exit_allowed: true,
        },
    )?;
    assert_eq!(
        exit_permission_above_invalidation.action(),
        RecommendationAction::Hold
    );
    assert_ne!(
        exit_permission_above_invalidation.action(),
        RecommendationAction::Sell
    );

    match InvestmentProposalAuthority::generate(
        proposal_evidence(alpha, 120, 10_000, 12_500)?,
        policy.clone(),
    )? {
        InvestmentProposalDecision::Unavailable(value) => assert_eq!(
            value.reason(),
            ProposalUnavailableReason::StaleEvidence(RecommendationEvidenceKind::Market)
        ),
        InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::NoAction(_) => {
            return Err("stale market evidence must be unavailable".into());
        }
    }

    match InvestmentProposalAuthority::generate(
        proposal_evidence(beta, 10, 10_000, 12_500)?,
        policy.clone(),
    )? {
        InvestmentProposalDecision::Unavailable(value) => assert_eq!(
            value.reason(),
            ProposalUnavailableReason::InstrumentMismatch {
                evidence: RecommendationEvidenceKind::PriceForecast,
                expected: alpha,
                actual: beta,
            }
        ),
        InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::NoAction(_) => {
            return Err("cross-instrument forecast evidence must be unavailable".into());
        }
    }

    let conflict_evidence = proposal_evidence(alpha, 10, 10_000, 7_000)?;
    match InvestmentProposalAuthority::generate(conflict_evidence.clone(), policy.clone())? {
        InvestmentProposalDecision::NoAction(value) => {
            assert_eq!(
                value.reason(),
                NoActionReason::ConflictingForecastAndValuation
            );
            assert_eq!(
                value.invalidators(),
                &[ProposalInvalidator::ForecastValuationConflict]
            );
            assert_eq!(
                value.execution_eligibility(),
                ProposalExecutionEligibility::ResearchOnlyExecutionIneligible
            );
            assert!(value.evidence().market().is_some());
            assert!(value.evidence().price_forecast().is_some());
            assert!(value.evidence().valuation().is_some());
            assert!(value.evidence().backtest().is_some());
            assert!(value.evidence().liquidity().is_some());
            assert!(value.evidence().portfolio_risk().is_some());
            let recovered = InvestmentProposalAuthority::try_recover_no_action(
                conflict_evidence,
                policy.clone(),
                value.analysis_id(),
                value.derivation_digest(),
                value.proposal_id(),
            )?;
            assert_eq!(recovered, value);
        }
        InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("complete conflicting evidence must produce no action".into());
        }
    }

    match InvestmentProposalAuthority::generate(
        proposal_evidence(alpha, 10, 20_000, 12_500)?,
        policy,
    )? {
        InvestmentProposalDecision::NoAction(value) => {
            assert_eq!(value.reason(), NoActionReason::PositionStateNotActionable);
            assert_ne!(
                value.reason(),
                NoActionReason::ConflictingForecastAndValuation
            );
        }
        InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::Unavailable(_) => {
            return Err("adverse zero-position evidence must not generate sell".into());
        }
    }
    Ok(())
}
