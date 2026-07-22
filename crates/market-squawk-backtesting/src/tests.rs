use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use crate::dataset::{BacktestDatasetInput, BacktestObservationInput};
use crate::{
    AccountingReconciliation, BacktestAdmissionError, BacktestBuildReceipt,
    BacktestBuildRegistration, BacktestCohortCandidate, BacktestCohortFold, BacktestCohortPlan,
    BacktestDataset, BacktestEngine, BacktestLimits, BacktestLimitsInput,
    BacktestModelDecisionMapper, BacktestModelStrategy, BacktestObservation, BacktestOutcome,
    BacktestRequest, BacktestService, BacktestServiceError, BacktestStrategy,
    BacktestStrategyClass, BacktestStrategyFactory, BacktestStrategyInstance,
    BacktestStrategyRegistry, BacktestTrialPlan, ExperimentError, ExperimentInventory,
    ExperimentLimits, ExperimentLimitsInput, HistoricalUniverseStatus, PortfolioSeed,
    RESEARCH_EXECUTION_POLICY_VERSION, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput, ResearchLiquidityPriority, TrialComponentBinding, TrialId,
    TrialParameter, TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
};
use market_squawk_data::{
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy,
    CorporateActionRecord, DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    AccountId, AvailabilityEvidence, BasisPoints, ClientOrderId, CorporateActionKind,
    CorporateActionObservation, Currency, DataQuality, Denomination, DigestAlgorithm,
    EvidenceDigest, InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize,
    Money, OrderReasonCode, OrderSide, OrderType, PayloadReference, PriceTicks, QuantityLots,
    ResearchContext, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, TickSize, TimeInForce, Timestamp, VenueId,
};
use market_squawk_execution::{BoundedOrderIntents, OrderIntent, OrderIntentInput, StrategyError};
use market_squawk_modeling::{InferenceError, ModelFailure, ModelOutput};
use market_squawk_portfolio::{PortfolioLimitInput, PortfolioLimits};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

type TestResult = Result<(), Box<dyn Error>>;

#[derive(Debug)]
struct BuyOnce {
    account_id: AccountId,
    emitted: bool,
    last_position: Decimal,
}

impl BacktestStrategy for BuyOnce {
    fn on_observation(
        &mut self,
        context: &crate::BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        self.last_position = context.position();
        let mut output = BoundedOrderIntents::new();
        if !self.emitted {
            output.try_push(buy_intent(
                context,
                self.account_id,
                "00000000-0000-0000-0000-000000000040",
                "backtest-buy-1",
                4,
            )?)?;
            self.emitted = true;
        }
        Ok(output)
    }
}

#[derive(Debug)]
struct BuyOnceFactory {
    account_id: AccountId,
}

impl BacktestStrategyFactory for BuyOnceFactory {
    fn build(&self) -> Result<BacktestStrategyInstance, BacktestAdmissionError> {
        Ok(BacktestStrategyInstance::RuleBased(Box::new(BuyOnce {
            account_id: self.account_id,
            emitted: false,
            last_position: Decimal::ZERO,
        })))
    }
}

#[derive(Debug)]
struct BuyTwice {
    account_id: AccountId,
    emitted: bool,
}

impl BacktestStrategy for BuyTwice {
    fn on_observation(
        &mut self,
        context: &crate::BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let mut output = BoundedOrderIntents::new();
        if !self.emitted {
            for (order_id, client_order_id) in [
                ("00000000-0000-0000-0000-000000000040", "depth-buy-1"),
                ("00000000-0000-0000-0000-000000000042", "depth-buy-2"),
            ] {
                output.try_push(buy_intent(
                    context,
                    self.account_id,
                    order_id,
                    client_order_id,
                    2,
                )?)?;
            }
            self.emitted = true;
        }
        Ok(output)
    }
}

fn buy_intent(
    context: &crate::BacktestContext<'_>,
    account_id: AccountId,
    order_id: &str,
    client_order_id: &str,
    quantity: i64,
) -> Result<OrderIntent, StrategyError> {
    OrderIntent::try_new(OrderIntentInput {
        order_id: order_id.parse().map_err(|_| StrategyError::Evaluation)?,
        client_order_id: ClientOrderId::try_from(client_order_id)
            .map_err(|_| StrategyError::Evaluation)?,
        strategy_id: "00000000-0000-0000-0000-000000000041"
            .parse()
            .map_err(|_| StrategyError::Evaluation)?,
        model_id: None,
        account_id,
        execution_terms: context.execution_terms(),
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        quantity: QuantityLots::new(quantity).map_err(|_| StrategyError::Evaluation)?,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::Day,
        signal_at: context.decision_at(),
        expires_at: context
            .decision_at()
            .checked_add_nanos(100)
            .map_err(|_| StrategyError::Evaluation)?,
        reason_codes: vec![
            OrderReasonCode::try_from("research-signal").map_err(|_| StrategyError::Evaluation)?,
        ],
        maximum_slippage: BasisPoints::new(100),
        required_quality: DataQuality::DirectVerified,
    })
    .map_err(|_| StrategyError::Evaluation)
}

#[derive(Debug)]
struct RejectMapper;

impl BacktestModelDecisionMapper for RejectMapper {
    fn map(
        &mut self,
        _context: &crate::BacktestContext<'_>,
        _output: &ModelOutput,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        Err(StrategyError::Evaluation)
    }
}

#[test]
fn signal_executes_only_on_next_eligible_snapshot_and_reconciles_partial_fill() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let admitted_dataset = dataset(terms)?;
    let changed_terms = execution_terms_revision(2)?;
    assert_ne!(
        admitted_dataset.identity(),
        dataset(changed_terms)?.identity()
    );
    let request = request(account_id, admitted_dataset, None)?;
    let mut strategy = BuyOnce {
        account_id,
        emitted: false,
        last_position: Decimal::ZERO,
    };

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert_eq!(result.fills().len(), 1);
    let fill = &result.fills()[0];
    assert_eq!(fill.signal_at(), Timestamp::from_unix_nanos(10));
    assert_eq!(fill.executed_at(), Timestamp::from_unix_nanos(20));
    assert_eq!(fill.quantity(), QuantityLots::new(2)?);
    assert!(fill.partial());
    assert_eq!(
        result
            .portfolio()
            .position(terms.instrument_id())
            .map(|p| p.quantity()),
        Some(Decimal::from(2))
    );
    assert_eq!(result.portfolio().fees(), fill.fee());
    assert_eq!(
        result.accounting_reconciliation(),
        AccountingReconciliation::Independent
    );
    Ok(())
}

#[test]
fn competing_intents_share_one_observation_liquidity_budget() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let request = request(account_id, dataset(execution_terms()?)?, None)?;
    let mut strategy = BuyTwice {
        account_id,
        emitted: false,
    };

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    let contested_fills = result
        .fills()
        .iter()
        .filter(|fill| fill.executed_at() == Timestamp::from_unix_nanos(20))
        .collect::<Vec<_>>();
    assert_eq!(contested_fills.len(), 1);
    assert_eq!(
        contested_fills
            .iter()
            .map(|fill| fill.quantity().get())
            .sum::<i64>(),
        2
    );
    Ok(())
}

#[test]
fn governed_service_reserves_before_run_and_publishes_one_immutable_terminal() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(
        root,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
    )?;
    let service = BacktestService::new(inventory);
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let request = request(account_id, dataset(execution_terms()?)?, None)?;
    let (registry, build_id) = strategy_registry(account_id)?;
    let mut strategy = registry.admit(&build_id)?;
    let outcome = service.run(
        request,
        &mut strategy,
        BacktestTrialPlan::new(
            Vec::new(),
            Vec::new(),
            SourceIdentifier::try_from("total-return")?,
        ),
        &CancellationToken::new(),
    )?;
    let BacktestOutcome::Completed(result) = outcome else {
        return Err("expected completed governed trial".into());
    };
    let TrialStatus::Completed(completion) = result.trial().status() else {
        return Err("expected completed terminal".into());
    };
    assert_eq!(result.run().fills().len(), 1);
    assert_eq!(completion.metrics().len(), 7);
    assert_eq!(
        completion
            .dataset_partition()
            .ok_or("missing dataset partition")?
            .ends_at(),
        Timestamp::from_unix_nanos(30)
    );
    assert!(
        temporary
            .path()
            .join(completion.artifact().reference())
            .is_file()
    );
    Ok(())
}

#[test]
fn cohort_evaluation_uses_completed_metrics_and_publishes_one_immutable_record() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(
        root,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 16,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
    )?;
    let service = BacktestService::new(inventory);
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let (registry, build_id) = strategy_registry(account_id)?;
    let (first_fold, selection_candidates) =
        completed_cohort_fold(&service, &registry, &build_id, account_id, 10, 40)?;
    let (second_fold, second_selection) =
        completed_cohort_fold(&service, &registry, &build_id, account_id, 70, 100)?;
    let invalid_plan = BacktestCohortPlan::try_new(
        vec![first_fold.clone(), second_fold.clone()],
        selection_candidates
            .iter()
            .chain(&second_selection)
            .copied()
            .collect(),
        SourceIdentifier::try_from("total-return")?,
    )?;
    assert!(matches!(
        service.evaluate_cohort(invalid_plan),
        Err(BacktestServiceError::InvalidCohort)
    ));
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/cohorts"))?.count(),
        0
    );
    let plan = BacktestCohortPlan::try_new(
        vec![first_fold, second_fold],
        selection_candidates.clone(),
        SourceIdentifier::try_from("total-return")?,
    )?;

    let evaluation = service.evaluate_cohort(plan.clone())?;
    let repeated = service.evaluate_cohort(plan)?;

    assert_eq!(evaluation.id(), repeated.id());
    assert_eq!(evaluation.members().len(), 8);
    assert!(selection_candidates.contains(&evaluation.selected().trial_id()));
    assert!(
        (0.0..=1.0).contains(
            &evaluation
                .probability_of_backtest_overfitting()
                .probability()
        )
    );
    assert!((0.0..=1.0).contains(&evaluation.deflated_performance().probability()));
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/cohorts"))?.count(),
        1
    );
    Ok(())
}

#[test]
fn expired_trial_attempt_can_be_recovered_without_overlapping_an_active_lease() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(
        root,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
    )?;
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let request = request(account_id, dataset(execution_terms()?)?, None)?;
    let spec = TrialSpec::try_new(TrialSpecInput {
        dataset_identity: request.dataset_identity(),
        object_graph_digest: request.object_graph_digest(),
        execution_assumption_digest: request.assumption_digest(),
        model: None,
        strategy: binding("strategy-v1", 5)?,
        code: binding("code-revision", 6)?,
        configuration_digest: Sha256Digest::new([7; 32]),
        seed: request.seed(),
        parameters: Vec::new(),
        search_space: Vec::new(),
        selection_criterion: SourceIdentifier::try_from("total-return")?,
    })?;

    let _first = inventory.reserve_at(spec.clone(), Timestamp::from_unix_nanos(100), 10)?;
    assert!(matches!(
        inventory.reserve_at(spec.clone(), Timestamp::from_unix_nanos(105), 10),
        Err(ExperimentError::TrialInProgress)
    ));
    let _recovered = inventory.reserve_at(spec.clone(), Timestamp::from_unix_nanos(110), 10)?;
    assert!(matches!(
        inventory.reserve_at(spec, Timestamp::from_unix_nanos(111), 10),
        Err(ExperimentError::TrialInProgress)
    ));
    Ok(())
}

#[test]
fn corporate_action_state_is_visible_at_event_time_and_independently_reconciled() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let plan = split_plan(terms.instrument_id())?;
    let request = request(account_id, dataset(terms)?, Some(plan))?;
    let mut strategy = BuyOnce {
        account_id,
        emitted: false,
        last_position: Decimal::ZERO,
    };

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert_eq!(
        result.accounting_reconciliation(),
        AccountingReconciliation::Independent
    );
    assert_eq!(strategy.last_position, Decimal::from(4));
    assert_eq!(
        result
            .portfolio()
            .position(terms.instrument_id())
            .map(|position| position.quantity()),
        Some(Decimal::from(4))
    );
    Ok(())
}

#[test]
fn typed_model_failure_is_audited_no_action() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let request = request(account_id, dataset(execution_terms()?)?, None)?;
    let mut strategy = BacktestModelStrategy::try_new(
        Err(ModelFailure::from(InferenceError::NonFiniteComputation)),
        Box::new(RejectMapper),
    )?;

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert!(result.fills().is_empty());
    assert_eq!(result.no_action_count(), 3);
    assert!(result.portfolio().positions().is_empty());
    Ok(())
}

fn strategy_registry(
    account_id: AccountId,
) -> Result<(BacktestStrategyRegistry, SourceIdentifier), Box<dyn Error>> {
    let build_id = SourceIdentifier::try_from("buy-once-v1")?;
    let receipt = BacktestBuildReceipt::try_from_evidence(
        build_id.clone(),
        BacktestStrategyClass::RuleBased,
        SourceIdentifier::try_from("buy-once")?,
        b"buy-once-source-closure-v1",
        b"buy-once-executable-v1",
        b"{\"quantity_lots\":4}",
    )?;
    let registry = BacktestStrategyRegistry::try_new(vec![BacktestBuildRegistration::new(
        receipt,
        Arc::new(BuyOnceFactory { account_id }),
    )])?;
    Ok((registry, build_id))
}

fn completed_cohort_fold(
    service: &BacktestService,
    registry: &BacktestStrategyRegistry,
    build_id: &SourceIdentifier,
    account_id: AccountId,
    in_sample_start: i64,
    out_of_sample_start: i64,
) -> Result<(BacktestCohortFold, Vec<TrialId>), Box<dyn Error>> {
    let mut candidates = Vec::new();
    let mut selection = Vec::new();
    for parameter in ["fast", "slow"] {
        let in_sample = run_governed_trial(
            service,
            registry,
            build_id,
            account_id,
            in_sample_start,
            parameter,
        )?;
        let out_of_sample = run_governed_trial(
            service,
            registry,
            build_id,
            account_id,
            out_of_sample_start,
            parameter,
        )?;
        candidates.push(BacktestCohortCandidate::new(in_sample, out_of_sample));
        selection.push(out_of_sample);
    }
    Ok((BacktestCohortFold::try_new(candidates)?, selection))
}

fn run_governed_trial(
    service: &BacktestService,
    registry: &BacktestStrategyRegistry,
    build_id: &SourceIdentifier,
    account_id: AccountId,
    dataset_start: i64,
    parameter: &str,
) -> Result<TrialId, Box<dyn Error>> {
    let mut strategy = registry.admit(build_id)?;
    let parameter_name = SourceIdentifier::try_from("speed")?;
    let outcome = service.run(
        request(
            account_id,
            dataset_at(execution_terms()?, dataset_start)?,
            None,
        )?,
        &mut strategy,
        BacktestTrialPlan::new(
            vec![TrialParameter::new(
                parameter_name.clone(),
                SourceIdentifier::try_from(parameter)?,
            )],
            vec![TrialSearchDimension::try_new(
                parameter_name,
                vec![
                    SourceIdentifier::try_from("fast")?,
                    SourceIdentifier::try_from("slow")?,
                ],
            )?],
            SourceIdentifier::try_from("total-return")?,
        ),
        &CancellationToken::new(),
    )?;
    let BacktestOutcome::Completed(result) = outcome else {
        return Err("expected completed cohort member".into());
    };
    Ok(result.trial().spec().id())
}

fn binding(name: &str, byte: u8) -> Result<TrialComponentBinding, Box<dyn Error>> {
    Ok(TrialComponentBinding::try_new(
        SourceIdentifier::try_from(name)?,
        Sha256Digest::new([byte; 32]),
    )?)
}

fn dataset(terms: InstrumentExecutionTerms) -> Result<BacktestDataset, Box<dyn Error>> {
    dataset_at(terms, 10)
}

fn dataset_at(
    terms: InstrumentExecutionTerms,
    starts_at: i64,
) -> Result<BacktestDataset, Box<dyn Error>> {
    Ok(BacktestDataset::try_new(BacktestDatasetInput {
        manifest: feature_manifest()?,
        object_graph_digest: Sha256Digest::new([2; 32]),
        point_in_time_content: Sha256Digest::new([3; 32]),
        point_in_time_audit: Sha256Digest::new([4; 32]),
        observations: vec![
            observation(terms, starts_at, 100, 10)?,
            observation(terms, starts_at + 10, 110, 2)?,
            observation(terms, starts_at + 20, 120, 10)?,
        ],
    })?)
}

fn request(
    account_id: AccountId,
    dataset: BacktestDataset,
    corporate_actions: Option<market_squawk_data::CorporateActionPlan>,
) -> Result<BacktestRequest, Box<dyn Error>> {
    Ok(BacktestRequest::try_new(
        dataset,
        ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
            version: RESEARCH_EXECUTION_POLICY_VERSION,
            fee_basis_points: BasisPoints::new(10),
            slippage_basis_points: BasisPoints::new(5),
            maximum_random_slippage_basis_points: BasisPoints::new(0),
            maximum_participation_basis_points: BasisPoints::new(10_000),
            liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
            latency_nanos: 1,
            allow_partial_fills: true,
            fee_decimal_scale: 4,
        })?,
        PortfolioSeed::try_new(
            account_id,
            Money::new(Decimal::new(100_000, 2), Currency::try_from("USD")?),
            portfolio_limits()?,
        )?,
        corporate_actions,
        vec![SourceIdentifier::try_from("task11-feature-labels")?],
        7,
        BacktestLimits::try_new(BacktestLimitsInput {
            max_observations: 100,
            max_pending_intents: 16,
            max_fills: 16,
            max_retained_bytes: 1_000_000,
        })?,
    )?)
}

fn split_plan(instrument_id: InstrumentId) -> Result<CorporateActionPlan, Box<dyn Error>> {
    let effective_at = Timestamp::from_unix_nanos(25);
    let source_identifier = SourceIdentifier::try_from("backtest-split")?;
    let observation = CorporateActionObservation::new(
        ResearchContext::new(
            ResearchProvenance::try_new(ResearchProvenanceInput {
                source_id: SourceId::try_from("official-actions")?,
                instrument_id: Some(instrument_id),
                venue_id: Some(VenueId::try_from("XNAS")?),
                source_identifier: source_identifier.clone(),
                source_timestamp: Some(effective_at),
                received_at: effective_at,
                ingested_at: Timestamp::from_unix_nanos(26),
                quality: DataQuality::OfficialDelayed,
                payload_reference: PayloadReference::SourceReference(source_identifier.clone()),
                availability: AvailabilityEvidence::evidenced(effective_at, source_identifier),
            })?,
            ResearchTime::new(effective_at, None, RevisionNumber::new(1)?, None)?,
        )?,
        CorporateActionKind::Split {
            numerator: NonZeroU32::new(2).ok_or("split numerator")?,
            denominator: NonZeroU32::MIN,
        },
    )?;
    let record = CorporateActionRecord::new(
        observation,
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("backtest-actions")?,
            1,
            DatasetSchemaRegistry::local().canonical_research_observations()?,
            Sha256Digest::new([9; 32]),
        )?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [10; 32]),
    );
    Ok(CorporateActionPlan::try_build(
        CorporateActionPolicy::new(CorporateActionAdjustment::TotalReturn, NonZeroU32::MIN),
        Timestamp::from_unix_nanos(30),
        Timestamp::from_unix_nanos(30),
        vec![record],
        CorporateActionLimits::try_new(
            NonZeroUsize::new(8).ok_or("action limit")?,
            NonZeroUsize::new(64 * 1024).ok_or("action bytes")?,
        )?,
    )?)
}

fn observation(
    execution_terms: InstrumentExecutionTerms,
    at: i64,
    mid_price_ticks: i64,
    depth_lots: i64,
) -> Result<BacktestObservation, Box<dyn Error>> {
    Ok(BacktestObservation::try_new(BacktestObservationInput {
        execution_terms,
        event_at: Timestamp::from_unix_nanos(at - 2),
        available_at: Timestamp::from_unix_nanos(at - 1),
        decision_at: Timestamp::from_unix_nanos(at),
        stale_at: Timestamp::from_unix_nanos(at + 5),
        mid_price: Some(PriceTicks::new(mid_price_ticks)),
        spread_basis_points: BasisPoints::new(20),
        executable_depth: QuantityLots::new(depth_lots)?,
        universe: HistoricalUniverseStatus::Eligible,
        features: Vec::new(),
        lineage_digest: Sha256Digest::new([u8::try_from(at)?; 32]),
    })?)
}

fn feature_manifest() -> Result<DatasetManifestRef, Box<dyn Error>> {
    let schema = DatasetSchemaRegistry::local().canonical_feature_labels()?;
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("backtest-features")?,
        1,
        schema,
        Sha256Digest::new([1; 32]),
    )?)
}

fn execution_terms() -> Result<InstrumentExecutionTerms, Box<dyn Error>> {
    execution_terms_revision(1)
}

fn execution_terms_revision(revision: u64) -> Result<InstrumentExecutionTerms, Box<dyn Error>> {
    let instrument_id: InstrumentId = "00000000-0000-0000-0000-000000000020".parse()?;
    Ok(InstrumentExecutionTerms::try_new(
        instrument_id,
        InstrumentDefinitionRevision::try_from(revision)?,
        TickSize::try_from_decimal(Decimal::ONE)?,
        LotSize::try_from_decimal(Decimal::ONE)?,
        Currency::try_from("USD")?,
        Denomination::Currency(Currency::try_from("USD")?),
        Decimal::ONE,
    )?)
}

fn portfolio_limits() -> Result<PortfolioLimits, Box<dyn Error>> {
    Ok(PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 16,
        max_lots: 64,
        max_transactions: 64,
        max_factors: 16,
        max_scenarios: 16,
        max_history: 16,
        max_results: 128,
        max_retained_bytes: 1_000_000,
    })?)
}
