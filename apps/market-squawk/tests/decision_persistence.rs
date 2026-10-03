use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use market_squawk::application::decision::{DecisionApplication, DecisionApplicationError};
use market_squawk_analytics::{
    FeatureImplementationDigest, FeatureOutputType, HarmonicDirection, HarmonicPatternKind,
    HarmonicPatternQuality, StatisticalF64,
};
use market_squawk_decisions::{
    AnalyticalProfileBindingReference, AppendOutcome, AsOfSemantics, CandidateFlag, CandidateId,
    CandidateInput, CandidatePortfolioSizingState, CandidateSizingConstraints,
    ChronologicalOutOfSampleEvidence, ComparisonOperator, CostAdjustedBacktestEvidence,
    DecisionContentDigest, DecisionContractError, DecisionRepositoryLimits, FinancialModelEvidence,
    FinancialModelMacroAssumptions, FinancialModelValueRange, ForecastCalibrationSummary,
    ForecastPriceRanges, HarmonicPatternEvidenceReceipt, InvestmentAnalysisEvidence,
    InvestmentAnalysisEvidenceInput, InvestmentAnalysisWorkflowReference,
    InvestmentOutcomeProjection, InvestmentProposalAuthority, InvestmentProposalDecision,
    InvestmentProposalIndexOutcome, InvestmentSizingInputs, InvestmentSizingProjection,
    LiquidityEvidence, MacroRateMaturity, MacroRateReferenceEvidence,
    MarketReferenceAdjustmentBasis, MarketReferenceEvidence, MarketReferencePriceKind, NullPolicy,
    PortfolioPositionState, PortfolioRiskEvidence, PreparedPublishedInvestmentAnalysis,
    PriceForecastEvidence, ProposalEvidenceWindow, ProposalForecastVintageId,
    PublishedInvestmentAnalysis, RankingDirection, RecommendationAction,
    RecommendationOutcomeObservation, RecommendationOutcomePendingReason,
    RecommendationOutcomeStatusRecord, RecommendationOutcomeUnavailableReason,
    RecommendationPolicy, RecommendationStudyQualification, SavedScreen, ScreenConstraints,
    ScreenFeatureBinding, ScreenFeatureObservation, ScreenId, ScreenPredicate, ScreenRanking,
    ScreenRevision, ScreenRun, ScreenRunId, SelectedCandidateAnalysisEvidence,
    SizingCapacityAvailability, TargetPriceCases, TargetPriceRange, ValuationEvidence,
};
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, DataQuality, Denomination, DigestAlgorithm, EvidenceDigest,
    HistoricalStudyBasis, InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId,
    LotSize, Money, PriceTicks, QuantityLots, RevisionNumber, SourceIdentifier, TickSize, Timestamp,
};
use market_squawk_modeling::{ForecastCentralStatistic, ProductionFeatureRegistry};
use market_squawk_platform::LocalPaths;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::{
    AutomaticValuationAssumption, AutomaticValuationAssumptionKind, AutomaticValuationMethod,
    ValuationAmountBasis,
};
use rusqlite::Connection;
use rust_decimal::Decimal;

const PROPOSAL_INSTRUMENT: &str = "018f8f6a-9d6f-7b43-9f38-55db5f4b0e01";
const DAY_NANOS: i64 = 86_400_000_000_000;

#[test]
fn decision_append_is_durable_idempotent_and_recovers_under_one_writer_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = persist_decision_fixture()?;
    // Release the setup frame before entering the complete strict journal replay.
    let recovered = DecisionApplication::open(fixture.location.clone(), fixture.limits)?;
    assert_recovered_decision_fixture(recovered, fixture)
}

struct PersistedDecisionFixture {
    directory: tempfile::TempDir,
    location: market_squawk_platform::DecisionDatabaseLocation,
    limits: DecisionRepositoryLimits,
    screen: SavedScreen,
    generated: InvestmentProposalDecision,
    no_action: InvestmentProposalDecision,
    unavailable: InvestmentProposalDecision,
    ad_hoc: PreparedPublishedInvestmentAnalysis,
    prepared: PreparedPublishedInvestmentAnalysis,
    generated_publication: PublishedInvestmentAnalysis,
    outcome_projection: InvestmentOutcomeProjection,
    sizing_projection: InvestmentSizingProjection,
    completed: RecommendationOutcomeStatusRecord,
    profile: AnalyticalProfileBindingReference,
    completed_available_at: Timestamp,
    candidate_coverage: StatisticalF64,
    candidate_liquidity: StatisticalF64,
    candidate_portfolio: PortfolioRevisionToken,
}

fn persist_decision_fixture() -> Result<Box<PersistedDecisionFixture>, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let location = paths.control_root()?.decision_database_location();
    let limits = DecisionRepositoryLimits::try_new(8, 8, 8, 8, 8, 8, 8, 32)?;
    let screen = saved_screen()?;
    let (generated, without_comparison) = generated_proposal()?;
    let no_action = no_action_proposal(&generated)?;
    let unavailable = unavailable_proposal(&generated)?;
    let profile = AnalyticalProfileBindingReference::new(
        SourceIdentifier::try_from("profile.balanced-v1")?,
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
        content_digest(201)?,
    );
    let workflow = InvestmentAnalysisWorkflowReference::new(
        SourceIdentifier::try_from("workflow.one-click-investment-analysis-v1")?,
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
        content_digest(202)?,
    );
    let selected_workflow = InvestmentAnalysisWorkflowReference::new(
        SourceIdentifier::try_from("workflow.selected-investment-analysis-v1")?,
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
        content_digest(210)?,
    );
    let no_action_workflow = InvestmentAnalysisWorkflowReference::new(
        SourceIdentifier::try_from("workflow.no-action-investment-analysis-v1")?,
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
        content_digest(211)?,
    );
    let unavailable_workflow = InvestmentAnalysisWorkflowReference::new(
        SourceIdentifier::try_from("workflow.unavailable-investment-analysis-v1")?,
        NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
        content_digest(212)?,
    );
    let published_at = generated.evidence().as_of();
    let candidate_as_of = generated
        .evidence()
        .as_of()
        .checked_sub_nanos(2 * DAY_NANOS)?;
    let candidate_selected_at = generated.evidence().as_of().checked_sub_nanos(DAY_NANOS)?;
    let screen_run = ScreenRun::try_new(
        ScreenRunId::try_new("run.restart-proof")?,
        screen.revision().clone(),
        candidate_as_of,
        content_digest(42)?,
        screen.universe_identity(),
        screen.feature_bindings().to_vec(),
    )?;
    let candidate_value = StatisticalF64::try_new(0.9)?;
    let candidate_coverage = StatisticalF64::try_new(0.9)?;
    let candidate_liquidity = StatisticalF64::try_new(1_234.0)?;
    let candidate_portfolio = PortfolioRevisionToken::from_bytes([44; 32]);
    let candidate_input = CandidateInput::try_new(
        CandidateId::try_new("candidate.restart-proof")?,
        PROPOSAL_INSTRUMENT.parse::<InstrumentId>()?,
        screen
            .feature_bindings()
            .iter()
            .cloned()
            .map(|binding| ScreenFeatureObservation::new(binding, Some(candidate_value)))
            .collect(),
        candidate_coverage,
        Some(candidate_liquidity),
        DataQuality::DirectVerified,
        Some(candidate_portfolio.clone()),
        vec![CandidateFlag::ModelDependent],
        content_digest(43)?,
    )?;
    let generated_publication = PublishedInvestmentAnalysis::try_new(
        &generated,
        profile.clone(),
        workflow.clone(),
        published_at,
    )?;
    let no_action_publication = PublishedInvestmentAnalysis::try_new(
        &no_action,
        profile.clone(),
        no_action_workflow,
        published_at,
    )?;
    let unavailable_publication = PublishedInvestmentAnalysis::try_new(
        &unavailable,
        profile.clone(),
        unavailable_workflow,
        published_at,
    )?;
    let InvestmentProposalDecision::Generated(generated_value) = &generated else {
        return Err("generated fixture changed decision family".into());
    };
    let outcome_projection = InvestmentOutcomeProjection::try_from_proposal(generated_value, None)?;
    let sizing_projection = sizing_projection(generated_value)?;
    let pending = RecommendationOutcomeStatusRecord::try_pending(
        &generated,
        &generated_publication,
        RevisionNumber::new(1)?,
        None,
        published_at,
        RecommendationOutcomePendingReason::AwaitingHorizon,
    )?;
    let completed_available_at = generated.horizon_at().checked_add_nanos(1_000_000_000)?;
    let completed = RecommendationOutcomeStatusRecord::try_completed(
        &generated,
        &generated_publication,
        RevisionNumber::new(2)?,
        Some(pending.status_digest()),
        completed_available_at,
        outcome_observation(
            money(12_000, generated.evidence().currency()),
            generated.horizon_at(),
            completed_available_at,
            203,
        )?,
    )?;
    let no_action_completed = RecommendationOutcomeStatusRecord::try_completed(
        &no_action,
        &no_action_publication,
        RevisionNumber::new(1)?,
        None,
        completed_available_at,
        outcome_observation(
            money(9_500, no_action.evidence().currency()),
            no_action.horizon_at(),
            completed_available_at,
            206,
        )?,
    )?;
    let unavailable_status = RecommendationOutcomeStatusRecord::try_unavailable(
        &unavailable,
        &unavailable_publication,
        RevisionNumber::new(1)?,
        None,
        published_at,
        match &unavailable {
            InvestmentProposalDecision::Unavailable(value) => {
                RecommendationOutcomeUnavailableReason::AnalysisUnavailable(value.reason())
            }
            InvestmentProposalDecision::Generated(_) | InvestmentProposalDecision::NoAction(_) => {
                return Err("unavailable fixture changed decision family".into());
            }
        },
    )?;

    let application = DecisionApplication::open(location.clone(), limits)?;
    assert!(matches!(
        application.append_investment_proposal(without_comparison),
        Err(DecisionApplicationError::InvalidPersistentState)
    ));
    assert_eq!(investment_record_count(location.path())?, 0);
    assert_eq!(
        application.save_screen(None, screen.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        application.save_screen(None, screen.clone())?,
        AppendOutcome::AlreadyPresent
    );
    let execution =
        application.run_screen(screen_run, vec![candidate_input], candidate_selected_at)?;
    let selected_candidate = SelectedCandidateAnalysisEvidence::try_new(
        &screen,
        execution.run(),
        execution
            .candidates()
            .first()
            .ok_or("screen fixture did not retain its candidate")?,
    )?;
    assert!(selected_candidate.as_of() < generated.evidence().as_of());
    assert!(selected_candidate.selected_at() < generated.evidence().as_of());
    let selected_decision = InvestmentProposalAuthority::generate(
        generated
            .evidence()
            .clone()
            .try_with_selected_candidate(selected_candidate.clone())?,
        generated.policy().clone(),
    )?;
    let prepared = PreparedPublishedInvestmentAnalysis::try_new(
        selected_decision,
        Some(selected_candidate),
        profile.clone(),
        selected_workflow,
        published_at,
    )?;
    let prepared_analysis_id = prepared.decision().analysis_id();
    install_prepared_bundle_rejection(location.path())?;
    assert!(matches!(
        application.append_prepared_published_investment_analysis(prepared.clone()),
        Err(DecisionApplicationError::Persistence)
    ));
    assert_eq!(investment_record_count(location.path())?, 0);
    remove_prepared_bundle_rejection(location.path())?;
    drop(application);

    let application = DecisionApplication::open(location.clone(), limits)?;
    for result in [
        application
            .get_investment_proposal(prepared_analysis_id)
            .map(|_| ()),
        application
            .get_investment_analysis_publication(prepared_analysis_id)
            .map(|_| ()),
        application
            .get_prepared_published_investment_analysis(prepared_analysis_id)
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(DecisionApplicationError::Repository(
                market_squawk_decisions::DecisionRepositoryError::NotFound
            ))
        ));
    }
    let ad_hoc = PreparedPublishedInvestmentAnalysis::try_new(
        generated.clone(),
        None,
        profile.clone(),
        workflow.clone(),
        published_at,
    )?
    .try_with_sizing_inputs(sizing_projection.inputs().clone())?;
    assert_eq!(
        application.append_prepared_published_investment_analysis(ad_hoc.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        application.append_prepared_published_investment_analysis(ad_hoc.clone())?,
        AppendOutcome::AlreadyPresent
    );
    for decision in [&no_action, &unavailable] {
        assert_eq!(
            application.append_investment_proposal(decision.clone())?,
            AppendOutcome::Appended
        );
    }
    assert_eq!(
        application.append_prepared_published_investment_analysis(prepared.clone())?,
        AppendOutcome::Appended
    );
    assert_eq!(
        application.append_prepared_published_investment_analysis(prepared.clone())?,
        AppendOutcome::AlreadyPresent
    );
    let conflicting_prepared = PreparedPublishedInvestmentAnalysis::try_new(
        prepared.decision().clone(),
        prepared.selected_candidate().cloned(),
        profile.clone(),
        InvestmentAnalysisWorkflowReference::new(
            SourceIdentifier::try_from("workflow.other-analysis-v1")?,
            NonZeroU32::new(1).ok_or(DecisionContractError::InvalidBound)?,
            content_digest(209)?,
        ),
        published_at,
    )?;
    assert!(matches!(
        application.append_prepared_published_investment_analysis(conflicting_prepared),
        Err(DecisionApplicationError::Repository(
            market_squawk_decisions::DecisionRepositoryError::Conflict
        ))
    ));
    for publication in [&no_action_publication, &unavailable_publication] {
        assert_eq!(
            application.append_investment_analysis_publication(publication.clone())?,
            AppendOutcome::Appended
        );
    }
    for status in [
        &pending,
        &completed,
        &no_action_completed,
        &unavailable_status,
    ] {
        assert_eq!(
            application.append_recommendation_outcome_status(status.clone())?,
            AppendOutcome::Appended
        );
    }
    assert!(matches!(
        DecisionApplication::open(location.clone(), limits),
        Err(DecisionApplicationError::Persistence)
    ));
    assert_eq!(record_count(location.path())?, 12);

    drop(application);
    Ok(Box::new(PersistedDecisionFixture {
        directory,
        location,
        limits,
        screen,
        generated,
        no_action,
        unavailable,
        ad_hoc,
        prepared,
        generated_publication,
        outcome_projection,
        sizing_projection,
        completed,
        profile,
        completed_available_at,
        candidate_coverage,
        candidate_liquidity,
        candidate_portfolio,
    }))
}

fn assert_recovered_decision_fixture(
    recovered: DecisionApplication,
    fixture: Box<PersistedDecisionFixture>,
) -> Result<(), Box<dyn std::error::Error>> {
    let PersistedDecisionFixture {
        directory: _directory,
        location,
        limits: _,
        screen,
        generated,
        no_action,
        unavailable,
        ad_hoc,
        prepared,
        generated_publication,
        outcome_projection,
        sizing_projection,
        completed,
        profile,
        completed_available_at,
        candidate_coverage,
        candidate_liquidity,
        candidate_portfolio,
    } = *fixture;
    let analysis_id = generated.analysis_id();
    let prepared_analysis_id = prepared.decision().analysis_id();
    let InvestmentProposalDecision::Generated(generated_value) = &generated else {
        return Err("generated fixture changed decision family".into());
    };
    let recovered_screen =
        recovered.get_screen(screen.revision().id(), screen.revision().revision())?;
    assert_eq!(recovered_screen, screen);
    assert_eq!(
        recovered_screen.predicates()[0].operator(),
        ComparisonOperator::GreaterThan
    );
    assert_eq!(
        recovered_screen.predicates()[0].null_policy(),
        NullPolicy::Include
    );
    assert_eq!(
        recovered_screen.ranking().direction(),
        RankingDirection::Ascending
    );
    assert_eq!(recovered_screen.maximum_results().get(), 3);
    assert_eq!(
        recovered_screen.constraints().minimum_coverage(),
        StatisticalF64::try_new(0.85)?
    );
    assert_eq!(
        recovered_screen.constraints().minimum_liquidity(),
        StatisticalF64::try_new(1_200.0)?
    );
    assert_eq!(
        recovered_screen.constraints().admitted_data_qualities(),
        &[DataQuality::DirectVerified, DataQuality::OfficialDelayed]
    );
    assert_eq!(
        recovered.get_prepared_published_investment_analysis(analysis_id)?,
        ad_hoc
    );
    let recovered_generated = recovered.get_investment_proposal(analysis_id)?;
    assert_eq!(recovered_generated, generated);
    assert_eq!(
        recovered_generated
            .evidence()
            .harmonic_pattern()
            .ok_or("typed price-pattern evidence was not recovered")?
            .evidence_digest(),
        generated
            .evidence()
            .harmonic_pattern()
            .ok_or("typed price-pattern evidence was not persisted")?
            .evidence_digest()
    );
    assert_eq!(
        recovered_generated.evidence().out_of_sample(),
        generated.evidence().out_of_sample()
    );
    assert_eq!(
        recovered_generated.evidence().financial_model(),
        generated.evidence().financial_model()
    );
    assert_eq!(
        recovered.get_investment_proposal(no_action.analysis_id())?,
        no_action
    );
    assert_eq!(
        recovered.get_investment_proposal(unavailable.analysis_id())?,
        unavailable
    );
    let recovered_prepared =
        recovered.get_prepared_published_investment_analysis(prepared_analysis_id)?;
    assert_eq!(recovered_prepared, prepared);
    assert_eq!(
        recovered_prepared
            .selected_candidate()
            .ok_or("missing retained candidate")?
            .coverage(),
        candidate_coverage
    );
    assert_eq!(
        recovered_prepared
            .selected_candidate()
            .ok_or("missing retained candidate")?
            .liquidity(),
        Some(candidate_liquidity)
    );
    assert_eq!(
        recovered_prepared
            .selected_candidate()
            .ok_or("missing retained candidate")?
            .data_quality(),
        DataQuality::DirectVerified
    );
    assert_eq!(
        recovered_prepared
            .selected_candidate()
            .ok_or("missing retained candidate")?
            .portfolio_impact(),
        Some(&candidate_portfolio)
    );
    assert_eq!(
        recovered_prepared
            .selected_candidate()
            .ok_or("missing retained candidate")?
            .flags(),
        &[
            CandidateFlag::ModelDependent,
            CandidateFlag::PortfolioImpactBound,
        ]
    );
    assert_eq!(
        recovered.get_investment_proposal(prepared_analysis_id)?,
        prepared.decision().clone()
    );
    assert_eq!(
        recovered.get_investment_analysis_publication(prepared_analysis_id)?,
        prepared.publication().clone()
    );
    let proposal_index = recovered.list_investment_proposal_index(4)?;
    assert_eq!(proposal_index.len(), 4);
    assert_eq!(proposal_index[0].analysis_id(), analysis_id);
    assert_eq!(proposal_index[0].proposal_id(), generated.proposal_id());
    assert_eq!(
        proposal_index[0].derivation_digest(),
        generated.derivation_digest()
    );
    assert!(matches!(
        proposal_index[1].outcome(),
        InvestmentProposalIndexOutcome::NoAction(_)
    ));
    assert!(matches!(
        proposal_index[2].outcome(),
        InvestmentProposalIndexOutcome::Unavailable(_)
    ));
    assert_eq!(proposal_index[3].analysis_id(), prepared_analysis_id);
    assert_eq!(
        recovered.get_investment_analysis_publication(analysis_id)?,
        generated_publication
    );
    assert_eq!(
        recovered.get_investment_outcome_projection(generated_value.proposal_id())?,
        outcome_projection
    );
    assert_eq!(
        recovered.get_investment_sizing_projection(generated_value.proposal_id())?,
        sizing_projection
    );
    let current = recovered.get_investment_analysis_current(analysis_id)?;
    assert_eq!(
        current.current_outcome().map(|value| value.status()),
        Some(completed.status())
    );
    let track_record =
        recovered.recommendation_track_record(&profile, 365 * DAY_NANOS, completed_available_at)?;
    assert_eq!(track_record.analysis_unavailable_count(), 1);
    assert_eq!(
        track_record
            .groups()
            .iter()
            .find(|group| {
                group.cohort()
                    == market_squawk_decisions::RecommendationOutcomeCohort::Generated(
                        RecommendationAction::Buy,
                    )
            })
            .map(|group| group.completed_count()),
        Some(1)
    );
    assert_eq!(
        track_record
            .groups()
            .iter()
            .find(|group| {
                group.cohort()
                    == market_squawk_decisions::RecommendationOutcomeCohort::NoActionControl
            })
            .map(|group| group.completed_count()),
        Some(1)
    );
    assert_eq!(
        proposal_index[0].outcome(),
        InvestmentProposalIndexOutcome::Generated(RecommendationAction::Buy)
    );
    assert_eq!(
        recovered.save_screen(None, screen)?,
        AppendOutcome::AlreadyPresent
    );
    assert!(matches!(
        recovered.append_investment_proposal(generated),
        Err(DecisionApplicationError::Repository(
            market_squawk_decisions::DecisionRepositoryError::Conflict
        ))
    ));
    assert_eq!(
        recovered.append_prepared_published_investment_analysis(ad_hoc)?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(
        recovered.append_recommendation_outcome_status(completed)?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(
        recovered.append_prepared_published_investment_analysis(prepared)?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(record_count(location.path())?, 12);
    Ok(())
}

fn content_digest(byte: u8) -> Result<DecisionContentDigest, DecisionContractError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]))
}

fn money(amount: i64, currency: Currency) -> Money {
    Money::new(Decimal::new(amount, 2), currency)
}

fn generated_proposal()
-> Result<(InvestmentProposalDecision, InvestmentProposalDecision), Box<dyn std::error::Error>> {
    let instrument_id = PROPOSAL_INSTRUMENT.parse::<InstrumentId>()?;
    let account_id = "018f8f6a-9d6f-7b43-9f38-55db5f4b1a01".parse::<AccountId>()?;
    let currency = Currency::try_from("USD")?;
    let (as_of, valuation) = selected_valuation_fixture(account_id, instrument_id, currency)?;
    let window = |observed_at: Timestamp,
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
        money(10_000, currency),
        DataQuality::DirectVerified,
        MarketReferencePriceKind::LastTrade,
        MarketReferenceAdjustmentBasis::UnadjustedSpot,
        content_digest(101)?,
        content_digest(102)?,
        window(
            as_of.checked_sub_nanos(10_000_000_000)?,
            as_of.checked_sub_nanos(1_000_000_000)?,
            1,
            103,
        )?,
    )?;
    let forecast_horizon_at = as_of
        .checked_sub_nanos(2 * DAY_NANOS)?
        .checked_add_nanos(365 * DAY_NANOS)?;
    let output_binding_identity = content_digest(105)?;
    let forecast = PriceForecastEvidence::try_new(
        instrument_id,
        TargetPriceCases::try_new(
            money(7_000, currency),
            money(13_000, currency),
            money(17_000, currency),
        )?,
        ForecastPriceRanges::try_new(
            TargetPriceRange::try_new(money(6_000, currency), money(8_000, currency))?,
            TargetPriceRange::try_new(money(12_000, currency), money(14_000, currency))?,
            TargetPriceRange::try_new(money(16_000, currency), money(18_000, currency))?,
        )?,
        forecast_horizon_at,
        Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
        Some(money(13_000, currency)),
        Some(forecast_horizon_at),
        Some(output_binding_identity),
        ProposalForecastVintageId::try_from_bytes([104; 32])?,
        output_binding_identity,
        content_digest(106)?,
        content_digest(107)?,
        ForecastCalibrationSummary::try_new(
            800_000,
            780_000,
            NonZeroU32::new(100).ok_or(DecisionContractError::InvalidBound)?,
        )?,
        window(
            as_of.checked_sub_nanos(2 * DAY_NANOS)?,
            as_of.checked_sub_nanos(DAY_NANOS)?,
            30,
            108,
        )?,
    )?;
    let assumption_available_at = as_of.checked_sub_nanos(6 * DAY_NANOS)?;
    let assumption_expires_at = as_of.checked_add_nanos(60 * DAY_NANOS)?;
    let macro_assumptions = FinancialModelMacroAssumptions::try_new(
        MacroRateReferenceEvidence::try_new(
            MacroRateMaturity::TenYear,
            Decimal::new(4, 0),
            content_digest(130)?.evidence_digest(),
            content_digest(133)?.evidence_digest(),
            as_of.checked_sub_nanos(5 * DAY_NANOS)?,
            market_squawk_domain::CalendarDate::new(2025, 1, 1)?,
            assumption_available_at,
            assumption_expires_at,
        )?,
        AutomaticValuationAssumption::try_new(
            AutomaticValuationAssumptionKind::DiscountRate,
            "annual-risk-premium",
            Decimal::new(6, 2),
            content_digest(134)?.evidence_digest(),
            assumption_available_at,
            assumption_expires_at,
        )?,
        AutomaticValuationAssumptionKind::DiscountRate,
        "annual-discount-rate",
    )?;
    let model_assumptions = vec![
        macro_assumptions.assumption().clone(),
        AutomaticValuationAssumption::try_new(
            AutomaticValuationAssumptionKind::UncertaintyLower,
            "lower",
            Decimal::new(11_000, 2),
            content_digest(135)?.evidence_digest(),
            assumption_available_at,
            assumption_expires_at,
        )?,
        AutomaticValuationAssumption::try_new(
            AutomaticValuationAssumptionKind::UncertaintyUpper,
            "upper",
            Decimal::new(14_000, 2),
            content_digest(136)?.evidence_digest(),
            assumption_available_at,
            assumption_expires_at,
        )?,
    ]
    .into_boxed_slice();
    let financial_model = FinancialModelEvidence::try_recover_projection(
        instrument_id,
        account_id,
        AutomaticValuationMethod::DiscountedCashFlow,
        Some(NonZeroU32::MIN),
        FinancialModelValueRange::try_new(
            money(11_000, currency),
            money(12_500, currency),
            money(14_000, currency),
        )?,
        TargetPriceCases::try_new(
            money(8_000, currency),
            money(12_500, currency),
            money(16_000, currency),
        )?,
        TargetPriceRange::try_new(money(11_500, currency), money(13_500, currency))?,
        forecast_horizon_at,
        content_digest(125)?,
        content_digest(126)?,
        model_assumptions,
        content_digest(128)?,
        content_digest(129)?,
        Some(macro_assumptions),
        window(
            as_of.checked_sub_nanos(5 * DAY_NANOS)?,
            as_of.checked_sub_nanos(4 * DAY_NANOS)?,
            60,
            126,
        )?,
    )?;
    let study_qualification =
        RecommendationStudyQualification::try_new(HistoricalStudyBasis::HistoricalAsKnown, &[])?;
    let backtest = CostAdjustedBacktestEvidence::try_new(
        instrument_id,
        currency,
        study_qualification,
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
        window(
            as_of.checked_sub_nanos(30 * DAY_NANOS)?,
            as_of.checked_sub_nanos(29 * DAY_NANOS)?,
            365,
            119,
        )?,
    )?;
    let out_of_sample = ChronologicalOutOfSampleEvidence::try_new(
        instrument_id,
        currency,
        study_qualification,
        365 * DAY_NANOS,
        as_of.checked_sub_nanos(120 * DAY_NANOS)?,
        as_of.checked_sub_nanos(32 * DAY_NANOS)?,
        as_of.checked_sub_nanos(31 * DAY_NANOS)?,
        NonZeroU32::new(900).ok_or(DecisionContractError::InvalidBound)?,
        NonZeroU32::new(1_000).ok_or(DecisionContractError::InvalidBound)?,
        NonZeroU32::new(10).ok_or(DecisionContractError::InvalidBound)?,
        900_000,
        content_digest(113)?,
        content_digest(114)?,
        content_digest(115)?,
        content_digest(116)?,
        window(
            as_of.checked_sub_nanos(30 * DAY_NANOS)?,
            as_of.checked_sub_nanos(29 * DAY_NANOS)?,
            365,
            119,
        )?,
    )?;
    let harmonic_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [131; 32]);
    let harmonic_pattern = HarmonicPatternEvidenceReceipt::try_recover_projection(
        instrument_id,
        NonZeroU64::new(u64::try_from(DAY_NANOS)?).ok_or(DecisionContractError::InvalidBound)?,
        HarmonicPatternKind::Bat,
        HarmonicDirection::Bullish,
        HarmonicPatternQuality::PreferredBatB,
        PriceTicks::new(9_500),
        PriceTicks::new(10_000),
        [
            PriceTicks::new(11_000),
            PriceTicks::new(12_000),
            PriceTicks::new(13_000),
        ],
        PriceTicks::new(9_000),
        as_of.checked_sub_nanos(2 * DAY_NANOS)?,
        as_of.checked_sub_nanos(DAY_NANOS)?,
        as_of.checked_sub_nanos(DAY_NANOS)?,
        as_of.checked_add_nanos(3 * DAY_NANOS)?,
        FeatureImplementationDigest::try_from_sha256([132; 32])?,
        harmonic_digest,
        ProposalEvidenceWindow::try_new(
            as_of.checked_sub_nanos(2 * DAY_NANOS)?,
            as_of.checked_sub_nanos(DAY_NANOS)?,
            as_of.checked_add_nanos(3 * DAY_NANOS)?,
            DecisionContentDigest::try_new(harmonic_digest)?,
        )?,
    )?;
    let liquidity = LiquidityEvidence::try_new(
        instrument_id,
        currency,
        BasisPoints::new(20),
        Some(900_000),
        Some(900_000),
        DataQuality::DirectVerified,
        content_digest(120)?,
        window(
            as_of.checked_sub_nanos(10_000_000_000)?,
            as_of.checked_sub_nanos(5_000_000_000)?,
            1,
            121,
        )?,
    )?;
    let portfolio_risk = PortfolioRiskEvidence::try_new(
        instrument_id,
        account_id,
        currency,
        PortfolioRevisionToken::from_bytes([122; 32]),
        PortfolioPositionState::NoPosition,
        900_000,
        content_digest(123)?,
        window(
            as_of.checked_sub_nanos(60_000_000_000)?,
            as_of.checked_sub_nanos(30_000_000_000)?,
            1,
            124,
        )?,
    )?;
    let evidence = InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
        instrument_id,
        currency,
        account_id,
        as_of,
        admitted_at: as_of,
        market: Some(market),
        price_forecast: Some(forecast),
        valuation: Some(valuation),
        financial_model: Some(financial_model),
        backtest: Some(backtest),
        out_of_sample: Some(out_of_sample),
        harmonic_pattern: Some(harmonic_pattern),
        liquidity: Some(liquidity),
        portfolio_risk: Some(portfolio_risk),
    });
    // A selected comparison survives even when no probability or comparison prices exist.
    // This is an explicit fixture request, not a fabricated catalog selection or price source.
    let without_comparison =
        InvestmentProposalAuthority::generate(evidence.clone(), RecommendationPolicy::v1()?)?;
    let record = format!(
        r#"{{"version":1,"requested":"{PROPOSAL_INSTRUMENT}","selected":null,"accompanying":null,"history":{{"state":"selection_unavailable"}}}}"#,
    ).into_bytes().into_boxed_slice();
    let comparison = market_squawk_decisions::SavedBenchmarkComparisonEvidence::try_new(
        instrument_id,
        currency,
        as_of,
        forecast.window().observed_at(),
        record,
    )?;
    let evidence = evidence.try_with_benchmark_comparison(comparison)?;
    let proposal = InvestmentProposalAuthority::generate(evidence, RecommendationPolicy::v1()?)?;
    assert_ne!(proposal.analysis_id(), without_comparison.analysis_id());
    match &proposal {
        InvestmentProposalDecision::Generated(value)
            if value.action() == RecommendationAction::Buy => {}
        InvestmentProposalDecision::Generated(_)
        | InvestmentProposalDecision::NoAction(_)
        | InvestmentProposalDecision::Unavailable(_) => {
            return Err("complete restart fixture must generate a buy proposal".into());
        }
    }
    Ok((proposal, without_comparison))
}

// Fresh proposal generation requires an authentic selected valuation, not a replay projection.
// This fixture creates its accounting chain through the existing portfolio and fair-value owners.
fn selected_valuation_fixture(
    account: AccountId,
    instrument: InstrumentId,
    currency: Currency,
) -> Result<(Timestamp, ValuationEvidence), Box<dyn std::error::Error>> {
    use market_squawk_data::{
        AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
        CatalogLimit, CatalogResultLimits, DatasetId, DatasetManifestRef, DatasetSchemaRegistry,
        ObjectStoreConfig, Sha256Digest,
    };
    use market_squawk_portfolio::{
        CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, LotSelection, PortfolioLedger,
        PortfolioLimitInput, PortfolioLimits, PriceEvidence, RevisionEvidence, Trade, TradeSide,
        TransactionRevision, ValuationSet,
    };
    use market_squawk_valuation::{
        ActorId, ClassificationRuleset, FairValueLimitInput, FairValueLimits,
        FairValueSelectionRequest, FairValueService, InputSignificance, ValuationAmount,
        ValuationInput, ValuationMeasurement, ValuationMeasurementSpec, ValuationMethod,
    };
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let now = || -> Result<Timestamp, Box<dyn std::error::Error>> {
        Ok(Timestamp::from_unix_nanos(i64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        )?))
    };
    let started_at = now()?;
    let source_at = started_at.checked_sub_nanos(6 * DAY_NANOS)?;
    let measurement_at = started_at.checked_sub_nanos(5 * DAY_NANOS)?;
    let prepared_at = started_at.checked_sub_nanos(4 * DAY_NANOS)?;
    let approved_at = started_at.checked_sub_nanos(3 * DAY_NANOS)?;
    let expires_at = started_at.checked_add_nanos(60 * DAY_NANOS)?;
    let portfolio_limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 2,
        max_lots: 4,
        max_transactions: 4,
        max_factors: 1,
        max_scenarios: 1,
        max_history: 2,
        max_results: 4,
        max_retained_bytes: 256 * 1024,
    })?;
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("decision-persistence-valuation-fixture")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([141; 32]),
    )?;
    let mut ledger = PortfolioLedger::try_new(account, currency, portfolio_limits)?;
    let entries = vec![
        LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(
                SourceIdentifier::try_from("fixture-deposit")?,
                RevisionNumber::new(1)?,
                None,
            )?,
            source_at.checked_sub_nanos(2)?,
            SourceIdentifier::try_from("fixture-cash")?,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                money(100_000, currency),
                None,
            )?),
        )?,
        LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(
                SourceIdentifier::try_from("fixture-buy")?,
                RevisionNumber::new(1)?,
                None,
            )?,
            source_at.checked_sub_nanos(1)?,
            SourceIdentifier::try_from("fixture-trade")?,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument,
                Decimal::ONE,
                money(12_500, currency),
                Money::new(Decimal::ZERO, currency),
                LotSelection::Fifo,
            )?),
        )?,
    ];
    let prices = ValuationSet::try_new(
        currency,
        source_at,
        manifest.clone(),
        Sha256Digest::new([142; 32]),
        vec![PriceEvidence::try_new(
            instrument,
            money(12_500, currency),
            source_at,
            SourceIdentifier::try_from("fixture-price")?,
        )?],
        Vec::new(),
        portfolio_limits,
    )?;
    let revision_evidence = RevisionEvidence::try_new(
        source_at,
        manifest,
        Sha256Digest::new([142; 32]),
        Sha256Digest::new([143; 32]),
        vec![SourceIdentifier::try_from("fixture-ledger")?],
        Vec::new(),
        None,
    )?;
    let revision = ledger.try_apply(entries, None, prices, revision_evidence)?;
    // The single-unit position amount is exactly the per-instrument-unit measurement.
    assert_eq!(
        revision
            .position(instrument)
            .ok_or("fixture position absent")?
            .quantity(),
        Decimal::ONE
    );
    let input = ValuationInput::from_portfolio_position(
        &revision,
        instrument,
        InputSignificance::Significant,
    )?;
    let amount = ValuationAmount::try_new(
        input.amount().money(),
        input.amount().scale(),
        ValuationAmountBasis::PerInstrumentUnit,
    )?;
    let measurement = ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id: account,
        instrument_id: instrument,
        amount,
        measurement_at,
        prepared_at,
        prepared_by: ActorId::try_from("fixture-preparer")?,
        method: ValuationMethod::MarketApproach,
        inputs: vec![input],
    })?;

    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("valuation"))?;
    let catalog = CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 16 * 1024 * 1024)?,
    )?)?;
    let analytical = AnalyticalDataService::initialize(
        catalog,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(1024 * 1024, 32, Duration::from_secs(60))?,
    )?;
    let mut fair_value = FairValueService::open(
        analytical.fair_value_catalog(),
        FairValueLimits::try_new(FairValueLimitInput {
            max_measurements: 1,
            max_inputs_per_measurement: 1,
            max_records_per_family: 4,
            max_query_results: 1,
            max_retained_bytes: 2 * 1024 * 1024,
        })?,
    )?;
    let classification = fair_value.classify(
        measurement,
        ClassificationRuleset::current(u64::try_from(2 * DAY_NANOS)?)?,
    )?;
    fair_value.approve(
        classification.id(),
        ActorId::try_from("fixture-independent-approver")?,
        approved_at,
        expires_at,
    )?;
    // Catalog append clocks are genuine current clocks. Select only after both durable writes;
    // the old fixed 1971 cutoff cannot contain their admitted accounting chain.
    let as_of = now()?;
    let selected = fair_value.select_latest_fair_value(FairValueSelectionRequest::new(
        instrument,
        currency,
        ValuationAmountBasis::PerInstrumentUnit,
        Some(account),
        as_of,
        NonZeroUsize::MIN,
    ))?;
    let horizon_at = as_of
        .checked_sub_nanos(2 * DAY_NANOS)?
        .checked_add_nanos(365 * DAY_NANOS)?;
    let valuation = ValuationEvidence::try_from_fair_value_selection(
        &selected,
        account,
        instrument,
        currency,
        horizon_at,
        ProposalEvidenceWindow::try_new(
            measurement_at,
            as_of,
            expires_at,
            DecisionContentDigest::try_new(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                selected.hash().bytes(),
            ))?,
        )?,
    )?;
    Ok((as_of, valuation))
}

fn no_action_proposal(
    generated: &InvestmentProposalDecision,
) -> Result<InvestmentProposalDecision, Box<dyn std::error::Error>> {
    let evidence = generated.evidence();
    let retained = evidence
        .liquidity()
        .ok_or("generated fixture must retain liquidity evidence")?;
    let low_liquidity = LiquidityEvidence::try_new(
        retained.instrument_id(),
        retained.currency(),
        retained.quoted_spread(),
        Some(100_000),
        retained.trim_sell_capacity_ppm(),
        retained.quality(),
        retained.assessment_identity(),
        retained.window(),
    )?;
    let decision = InvestmentProposalAuthority::generate(
        InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
            instrument_id: evidence.instrument_id(),
            currency: evidence.currency(),
            account_id: evidence.account_id(),
            as_of: evidence.as_of(),
            admitted_at: evidence.admitted_at(),
            market: evidence.market().copied(),
            price_forecast: evidence.price_forecast().copied(),
            valuation: evidence.valuation().copied(),
            financial_model: evidence.financial_model().cloned(),
            backtest: evidence.backtest().copied(),
            out_of_sample: evidence.out_of_sample().copied(),
            harmonic_pattern: evidence.harmonic_pattern().cloned(),
            liquidity: Some(low_liquidity),
            portfolio_risk: evidence.portfolio_risk().cloned(),
        })
        .try_with_benchmark_comparison(
            evidence
                .benchmark_comparison()
                .ok_or("fixture comparison absent")?
                .clone(),
        )?,
        generated.policy().clone(),
    )?;
    if !matches!(decision, InvestmentProposalDecision::NoAction(_)) {
        return Err("low-liquidity fixture must produce no action".into());
    }
    Ok(decision)
}

fn unavailable_proposal(
    generated: &InvestmentProposalDecision,
) -> Result<InvestmentProposalDecision, Box<dyn std::error::Error>> {
    let evidence = generated.evidence();
    let decision = InvestmentProposalAuthority::generate(
        InvestmentAnalysisEvidence::new(InvestmentAnalysisEvidenceInput {
            instrument_id: evidence.instrument_id(),
            currency: evidence.currency(),
            account_id: evidence.account_id(),
            as_of: evidence.as_of(),
            admitted_at: evidence.admitted_at(),
            market: None,
            price_forecast: evidence.price_forecast().copied(),
            valuation: evidence.valuation().copied(),
            financial_model: evidence.financial_model().cloned(),
            backtest: evidence.backtest().copied(),
            out_of_sample: evidence.out_of_sample().copied(),
            harmonic_pattern: evidence.harmonic_pattern().cloned(),
            liquidity: evidence.liquidity().copied(),
            portfolio_risk: evidence.portfolio_risk().cloned(),
        })
        .try_with_benchmark_comparison(
            evidence
                .benchmark_comparison()
                .ok_or("fixture comparison absent")?
                .clone(),
        )?,
        generated.policy().clone(),
    )?;
    if !matches!(decision, InvestmentProposalDecision::Unavailable(_)) {
        return Err("missing-market fixture must produce unavailable".into());
    }
    Ok(decision)
}

fn sizing_projection(
    proposal: &market_squawk_decisions::GeneratedInvestmentProposal,
) -> Result<InvestmentSizingProjection, Box<dyn std::error::Error>> {
    let evidence = proposal.evidence();
    let market = evidence
        .market()
        .ok_or("generated fixture must retain market evidence")?;
    let risk = evidence
        .portfolio_risk()
        .ok_or("generated fixture must retain portfolio-risk evidence")?;
    let currency = evidence.currency();
    let terms = InstrumentExecutionTerms::try_new(
        evidence.instrument_id(),
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(Decimal::new(1, 2))?,
        LotSize::try_from_decimal(Decimal::ONE)?,
        currency,
        Denomination::Currency(currency),
        Decimal::ONE,
    )?;
    let portfolio = CandidatePortfolioSizingState::try_new(
        evidence.account_id(),
        evidence.instrument_id(),
        risk.portfolio_revision().clone(),
        money(100_000, currency),
        money(50_000, currency),
        QuantityLots::new(0)?,
    )?;
    let constraints = CandidateSizingConstraints::try_new(money(0, currency), 0, 10_000, 10_000)?;
    Ok(InvestmentSizingProjection::try_from_proposal(
        proposal,
        InvestmentSizingInputs::new(
            evidence.as_of(),
            terms,
            market.price(),
            portfolio,
            constraints,
            SizingCapacityAvailability::UnavailableNotSupplied,
            SizingCapacityAvailability::UnavailableNotSupplied,
            SizingCapacityAvailability::UnavailableNotSupplied,
        ),
    )?)
}

fn outcome_observation(
    endpoint_price: Money,
    observed_at: Timestamp,
    available_at: Timestamp,
    identity: u8,
) -> Result<RecommendationOutcomeObservation, Box<dyn std::error::Error>> {
    Ok(RecommendationOutcomeObservation::try_new(
        endpoint_price,
        observed_at,
        available_at,
        content_digest(identity)?,
        content_digest(identity + 1)?,
        content_digest(identity + 2)?,
    )?)
}

fn saved_screen() -> Result<SavedScreen, Box<dyn std::error::Error>> {
    let registry = ProductionFeatureRegistry::try_new()?;
    let metadata = registry
        .feature_registry()
        .entries()
        .find(|metadata| {
            metadata.is_point_in_time_compatible()
                && metadata.output_type() == FeatureOutputType::StatisticalF64
        })
        .ok_or(DecisionContractError::UnknownScreenFeature)?;
    let binding = ScreenFeatureBinding::new(metadata.key().clone(), metadata.semantic_digest());
    Ok(SavedScreen::try_new(
        ScreenRevision::new(
            ScreenId::try_new("screen.restart-proof")?,
            RevisionNumber::new(1)?,
        ),
        DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, [41; 32]))?,
        AsOfSemantics::AvailableAtOrBeforeCutoff,
        vec![ScreenPredicate::new(
            binding.clone(),
            ComparisonOperator::GreaterThan,
            StatisticalF64::try_new(0.75)?,
            NullPolicy::Include,
        )],
        ScreenRanking::new(binding, RankingDirection::Ascending),
        NonZeroUsize::new(3).ok_or(DecisionContractError::InvalidBound)?,
        ScreenConstraints::try_new(
            StatisticalF64::try_new(0.85)?,
            StatisticalF64::try_new(1_200.0)?,
            vec![DataQuality::DirectVerified, DataQuality::OfficialDelayed],
        )?,
        registry.feature_registry(),
    )?)
}

fn record_count(path: &std::path::Path) -> rusqlite::Result<i64> {
    Connection::open(path)?.query_row("SELECT COUNT(*) FROM decision_records", [], |row| {
        row.get(0)
    })
}

fn investment_record_count(path: &std::path::Path) -> rusqlite::Result<i64> {
    Connection::open(path)?.query_row(
        "SELECT COUNT(*) FROM decision_records WHERE kind = 8",
        [],
        |row| row.get(0),
    )
}

fn install_prepared_bundle_rejection(path: &std::path::Path) -> rusqlite::Result<()> {
    Connection::open(path)?.execute_batch(
        "CREATE TRIGGER reject_prepared_bundle
         BEFORE INSERT ON decision_records
         WHEN NEW.kind = 8 AND NEW.record_key LIKE 'prepared_%'
         BEGIN
             SELECT RAISE(ABORT, 'prepared bundle persistence rejection proof');
         END;",
    )
}

fn remove_prepared_bundle_rejection(path: &std::path::Path) -> rusqlite::Result<()> {
    Connection::open(path)?.execute_batch("DROP TRIGGER reject_prepared_bundle;")
}
