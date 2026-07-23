use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use crate::dataset::{BacktestDatasetInput, BacktestObservationInput};
use crate::experiments::TrialCompletionInput;
use crate::{
    AccountingReconciliation, BacktestAdmissionError, BacktestBuildReceipt,
    BacktestBuildRegistration, BacktestCohortCandidate, BacktestCohortFold,
    BacktestCohortFoldPartition, BacktestCohortPartition, BacktestCohortPlan,
    BacktestCohortUniverse, BacktestDataset, BacktestEngine, BacktestError, BacktestLimits,
    BacktestLimitsInput, BacktestModelDecisionMapper, BacktestModelStrategy, BacktestObservation,
    BacktestOutcome, BacktestRequest, BacktestService, BacktestServiceError, BacktestStrategy,
    BacktestStrategyClass, BacktestStrategyFactory, BacktestStrategyInstance,
    BacktestStrategyRegistry, BacktestTrialPlan, ExperimentError, ExperimentInventory,
    ExperimentLimits, ExperimentLimitsInput, HistoricalUniverseStatus,
    MAX_COHORT_CANDIDATES_PER_FOLD, PortfolioSeed, RESEARCH_EXECUTION_POLICY_VERSION,
    ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, ResearchLiquidityPriority,
    TrialComponentBinding, TrialDatasetPartition, TrialId, TrialMetric, TrialParameter,
    TrialSearchDimension, TrialSpec, TrialSpecInput, TrialStatus,
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
    time_in_force: TimeInForce,
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
                self.time_in_force,
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
            time_in_force: TimeInForce::Day,
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
                    TimeInForce::Day,
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
    time_in_force: TimeInForce,
) -> Result<OrderIntent, StrategyError> {
    let (order_type, limit_price) = if time_in_force == TimeInForce::GoodTilCancelled {
        (OrderType::Limit, Some(PriceTicks::new(1_000_000)))
    } else {
        (OrderType::Market, None)
    };
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
        order_type,
        quantity: QuantityLots::new(quantity).map_err(|_| StrategyError::Evaluation)?,
        limit_price,
        stop_price: None,
        time_in_force,
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
        time_in_force: TimeInForce::Day,
        emitted: false,
        last_position: Decimal::ZERO,
    };

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert_eq!(result.fills().len(), 2);
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
        Some(Decimal::from(4))
    );
    assert_eq!(
        result.portfolio().fees().amount(),
        result
            .fills()
            .iter()
            .map(|fill| fill.fee().amount())
            .sum::<Decimal>()
    );
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
fn immediate_time_in_force_is_terminal_while_gtc_survives_an_unfilled_attempt() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    assert!(
        run_with_time_in_force(account_id, dataset(terms)?, TimeInForce::FillOrKill)?
            .fills()
            .is_empty()
    );
    let delayed_liquidity = dataset_with_depths(terms, [10, 0, 10])?;
    assert!(
        run_with_time_in_force(
            account_id,
            delayed_liquidity.clone(),
            TimeInForce::ImmediateOrCancel,
        )?
        .fills()
        .is_empty()
    );
    let good_til_cancelled_result =
        run_with_time_in_force(account_id, delayed_liquidity, TimeInForce::GoodTilCancelled)?;
    assert_eq!(good_til_cancelled_result.fills().len(), 1);
    assert_eq!(
        good_til_cancelled_result.fills()[0].executed_at(),
        Timestamp::from_unix_nanos(30)
    );

    let partial_liquidity = dataset_with_depths(terms, [10, 2, 2])?;
    for time_in_force in [TimeInForce::Day, TimeInForce::GoodTilCancelled] {
        let result = run_with_time_in_force(account_id, partial_liquidity.clone(), time_in_force)?;
        assert_eq!(result.fills().len(), 2);
        assert_eq!(
            result
                .fills()
                .iter()
                .map(|fill| fill.quantity().get())
                .sum::<i64>(),
            4
        );
    }
    let immediate = run_with_time_in_force(
        account_id,
        partial_liquidity.clone(),
        TimeInForce::ImmediateOrCancel,
    )?;
    assert_eq!(immediate.fills().len(), 1);
    assert_eq!(immediate.fills()[0].quantity(), QuantityLots::new(2)?);
    assert!(
        run_with_time_in_force(account_id, partial_liquidity, TimeInForce::FillOrKill)?
            .fills()
            .is_empty()
    );
    Ok(())
}

fn run_with_time_in_force(
    account_id: AccountId,
    dataset: BacktestDataset,
    time_in_force: TimeInForce,
) -> Result<crate::BacktestRun, Box<dyn Error>> {
    let mut strategy = BuyOnce {
        account_id,
        time_in_force,
        emitted: false,
        last_position: Decimal::ZERO,
    };
    Ok(BacktestEngine::run(
        &request(account_id, dataset, None)?,
        &mut strategy,
        &CancellationToken::new(),
    )?)
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
    assert_eq!(result.run().fills().len(), 2);
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
fn governed_trial_identity_binds_every_immutable_request_input() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let base = request(account_id, dataset(terms)?, None)?;
    let mut requests = vec![base.clone()];

    let mut changed_cash = base.clone();
    changed_cash.portfolio.initial_cash =
        Money::new(Decimal::new(100_100, 2), Currency::try_from("USD")?);
    requests.push(changed_cash);

    let mut changed_portfolio_limits = base.clone();
    changed_portfolio_limits.portfolio.limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 16,
        max_instruments: 1,
        max_lots: 64,
        max_transactions: 64,
        max_factors: 16,
        max_scenarios: 16,
        max_history: 16,
        max_results: 128,
        max_retained_bytes: 1_000_000,
    })?;
    requests.push(changed_portfolio_limits);

    let mut changed_actions = base.clone();
    changed_actions.corporate_actions = Some(split_plan(terms.instrument_id())?);
    requests.push(changed_actions);

    let mut changed_sources = base.clone();
    changed_sources.sources =
        vec![SourceIdentifier::try_from("alternate-feature-lineage")?].into_boxed_slice();
    requests.push(changed_sources);

    let mut changed_limits = base;
    changed_limits.limits = BacktestLimits::try_new(BacktestLimitsInput {
        max_observations: 99,
        max_pending_intents: 15,
        max_fills: 15,
        max_retained_bytes: 999_999,
    })?;
    requests.push(changed_limits);

    let mut identities = std::collections::BTreeSet::new();
    for request in requests {
        identities.insert(
            TrialSpec::try_new(TrialSpecInput {
                dataset_identity: request.dataset_identity(),
                object_graph_digest: request.object_graph_digest(),
                execution_assumption_digest: request.assumption_digest(),
                run_input_digest: request.run_input_digest(),
                cohort_authority_digest: request.cohort_authority_digest(),
                cohort_universe_digest: None,
                model: None,
                strategy: binding("strategy-v1", 5)?,
                code: binding("code-revision", 6)?,
                configuration_digest: Sha256Digest::new([7; 32]),
                seed: request.seed(),
                parameters: Vec::new(),
                search_space: Vec::new(),
                selection_criterion: SourceIdentifier::try_from("total-return")?,
            })?
            .id(),
        );
    }
    assert_eq!(identities.len(), 6);
    Ok(())
}

#[test]
fn request_rejects_a_dataset_that_cannot_form_a_nonempty_trial_partition() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let single_observation = BacktestDataset::try_new(BacktestDatasetInput {
        manifest: feature_manifest()?,
        object_graph_digest: Sha256Digest::new([2; 32]),
        point_in_time_content: Sha256Digest::new([3; 32]),
        point_in_time_audit: Sha256Digest::new([4; 32]),
        observations: vec![observation(terms, 10, 100, 10)?],
    })?;
    let Err(error) = request(account_id, single_observation, None) else {
        return Err("single-observation request was admitted".into());
    };
    assert!(matches!(
        error.downcast_ref::<BacktestError>(),
        Some(BacktestError::InvalidRequest)
    ));
    Ok(())
}

#[test]
fn post_reservation_validation_fails_terminally_before_artifact_publication() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let temporary = tempfile::tempdir()?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(
        root,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 1,
        })?,
    )?;
    let service = BacktestService::new(inventory);
    let (registry, build_id) = strategy_registry(account_id)?;
    let mut strategy = registry.admit(&build_id)?;
    assert!(matches!(
        service.run(
            request(account_id, dataset(terms)?, None)?,
            &mut strategy,
            BacktestTrialPlan::new(
                Vec::new(),
                Vec::new(),
                SourceIdentifier::try_from("total-return")?,
            ),
            &CancellationToken::new(),
        ),
        Err(BacktestServiceError::Experiment(
            ExperimentError::InvalidCompletion
        ))
    ));
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/reservations"))?.count(),
        1
    );
    let terminal_directory = std::fs::read_dir(temporary.path().join("backtesting/v1/terminals"))?
        .next()
        .ok_or("missing failed terminal directory")??
        .path();
    assert_eq!(std::fs::read_dir(terminal_directory)?.count(), 1);
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/artifacts/sha256"))?.count(),
        0
    );
    Ok(())
}

#[test]
fn cohort_evaluation_uses_completed_metrics_and_publishes_one_immutable_record() -> TestResult {
    let oversized_fold = (0_u64..=u64::try_from(MAX_COHORT_CANDIDATES_PER_FOLD)?)
        .map(|index| {
            let mut in_sample = [0_u8; 32];
            in_sample[..8].copy_from_slice(
                &index
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(1))
                    .ok_or("candidate identity overflow")?
                    .to_be_bytes(),
            );
            let mut out_of_sample = [0_u8; 32];
            out_of_sample[..8].copy_from_slice(
                &index
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(2))
                    .ok_or("candidate identity overflow")?
                    .to_be_bytes(),
            );
            Ok(BacktestCohortCandidate::new(
                TrialId::from_digest(Sha256Digest::new(in_sample)),
                TrialId::from_digest(Sha256Digest::new(out_of_sample)),
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    assert!(matches!(
        BacktestCohortFold::try_new(oversized_fold),
        Err(ExperimentError::InvalidDiagnostic)
    ));

    let temporary = tempfile::tempdir()?;
    let limits = ExperimentLimits::try_new(ExperimentLimitsInput {
        max_trials: 16,
        max_record_bytes: 64 * 1024,
        max_artifact_bytes: 64 * 1024,
        max_metrics: 8,
    })?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(root, limits)?;
    let service = BacktestService::new(inventory);
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let (registry, build_id) = strategy_registry(account_id)?;
    let parameters = &["fast", "medium", "slow"];
    let universe = cohort_universe(&[(10, 40), (70, 100)], 40)?;
    let differently_bounded_universe = BacktestCohortUniverse::try_new(
        universe.generator_version().clone(),
        universe.generation_parameters().to_vec(),
        2,
        universe.folds().to_vec(),
        universe.selection_partition(),
    )?;
    assert_ne!(universe.digest(), differently_bounded_universe.digest());
    let aggregate_overflow_folds = [(10, 35), (55, 80), (100, 125), (145, 170), (190, 215)]
        .into_iter()
        .map(|(in_sample, out_of_sample)| {
            BacktestCohortFoldPartition::try_new(
                cohort_partition(in_sample)?,
                cohort_partition(out_of_sample)?,
            )
        })
        .collect::<Result<Vec<_>, ExperimentError>>()?;
    assert!(matches!(
        BacktestCohortUniverse::try_new(
            SourceIdentifier::try_from("walk-forward-generator-v1")?,
            vec![TrialParameter::new(
                SourceIdentifier::try_from("fold-count")?,
                SourceIdentifier::try_from("five-folds")?,
            )],
            MAX_COHORT_CANDIDATES_PER_FOLD,
            aggregate_overflow_folds,
            cohort_partition(35)?,
        ),
        Err(ExperimentError::InvalidDiagnostic)
    ));
    let mut mismatched_strategy = registry.admit(&build_id)?;
    let parameter_name = SourceIdentifier::try_from("speed")?;
    assert!(matches!(
        service.run(
            request(account_id, dataset_at(execution_terms()?, 10)?, None)?,
            &mut mismatched_strategy,
            BacktestTrialPlan::new(
                vec![TrialParameter::new(
                    parameter_name.clone(),
                    SourceIdentifier::try_from("fast")?,
                )],
                vec![TrialSearchDimension::try_new(
                    parameter_name,
                    vec![
                        SourceIdentifier::try_from("fast")?,
                        SourceIdentifier::try_from("slow")?,
                    ],
                )?],
                SourceIdentifier::try_from("total-return")?,
            )
            .with_cohort_universe(universe.clone()),
            &CancellationToken::new(),
        ),
        Err(BacktestServiceError::InvalidCohort)
    ));
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/reservations"))?.count(),
        0
    );
    let (first_fold, selection_candidates, first_candidates) = completed_cohort_fold(
        &service, &registry, &build_id, account_id, 10, 40, parameters, &universe,
    )?;
    let (second_fold, second_selection, second_candidates) = completed_cohort_fold(
        &service, &registry, &build_id, account_id, 70, 100, parameters, &universe,
    )?;
    assert!(matches!(
        BacktestCohortPlan::try_new(
            universe.clone(),
            vec![
                BacktestCohortFold::try_new(first_candidates[..2].to_vec())?,
                BacktestCohortFold::try_new(second_candidates[..2].to_vec())?,
            ],
            selection_candidates[..2].to_vec(),
            SourceIdentifier::try_from("total-return")?,
        ),
        Err(ExperimentError::InvalidDiagnostic)
    ));
    assert!(matches!(
        BacktestCohortPlan::try_new(
            cohort_universe(&[(10, 40), (70, 100), (130, 160)], 40)?,
            vec![first_fold.clone(), second_fold.clone()],
            selection_candidates.clone(),
            SourceIdentifier::try_from("total-return")?,
        ),
        Err(ExperimentError::InvalidDiagnostic)
    ));
    assert!(matches!(
        BacktestCohortPlan::try_new(
            universe.clone(),
            vec![first_fold.clone(), second_fold.clone()],
            selection_candidates
                .iter()
                .chain(&second_selection)
                .copied()
                .collect(),
            SourceIdentifier::try_from("total-return")?,
        ),
        Err(ExperimentError::InvalidDiagnostic)
    ));
    let mut changed_request = request(account_id, dataset_at(execution_terms()?, 40)?, None)?;
    changed_request.limits = BacktestLimits::try_new(BacktestLimitsInput {
        max_observations: 99,
        max_pending_intents: 16,
        max_fills: 16,
        max_retained_bytes: 1_000_000,
    })?;
    let changed_authority = run_governed_request(
        &service,
        &registry,
        &build_id,
        changed_request,
        "fast",
        &universe,
    )?;
    let mut authority_candidates = first_candidates.clone();
    authority_candidates[0] =
        BacktestCohortCandidate::new(authority_candidates[0].in_sample(), changed_authority);
    let mut authority_selection = selection_candidates.clone();
    authority_selection[0] = changed_authority;
    let authority_plan = BacktestCohortPlan::try_new(
        universe.clone(),
        vec![
            BacktestCohortFold::try_new(authority_candidates)?,
            second_fold.clone(),
        ],
        authority_selection,
        SourceIdentifier::try_from("total-return")?,
    )?;
    assert!(matches!(
        service.evaluate_cohort(authority_plan),
        Err(BacktestServiceError::InvalidCohort)
    ));
    assert_eq!(
        std::fs::read_dir(temporary.path().join("backtesting/v1/cohorts"))?.count(),
        0
    );
    let plan = BacktestCohortPlan::try_new(
        universe,
        vec![first_fold, second_fold],
        selection_candidates.clone(),
        SourceIdentifier::try_from("total-return")?,
    )?;
    let constrained_plan = plan.clone();

    let evaluation = service.evaluate_cohort(plan.clone())?;
    let repeated = service.evaluate_cohort(plan)?;

    assert_eq!(evaluation.id(), repeated.id());
    assert_eq!(evaluation.members().len(), 12);
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
    let evaluation_id = evaluation.id();
    drop(service);
    let constrained_root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let constrained = BacktestService::new(ExperimentInventory::try_new(
        constrained_root,
        ExperimentLimits::try_new(ExperimentLimitsInput {
            max_trials: 8,
            max_record_bytes: 64 * 1024,
            max_artifact_bytes: 64 * 1024,
            max_metrics: 8,
        })?,
    )?);
    assert!(matches!(
        constrained.evaluate_cohort(constrained_plan),
        Err(BacktestServiceError::Experiment(
            ExperimentError::LimitExceeded
        ))
    ));
    drop(constrained);
    let reopened_root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let reopened = ExperimentInventory::try_new(reopened_root, limits)?;
    assert_eq!(reopened.cohort_evaluation(evaluation_id)?, evaluation);
    Ok(())
}

#[test]
fn expired_trial_attempt_can_be_recovered_without_overlapping_an_active_lease() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let limits = ExperimentLimits::try_new(ExperimentLimitsInput {
        max_trials: 8,
        max_record_bytes: 64 * 1024,
        max_artifact_bytes: 64 * 1024,
        max_metrics: 8,
    })?;
    let root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let inventory = ExperimentInventory::try_new(root, limits)?;
    let competing_root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    assert!(matches!(
        ExperimentInventory::try_new(competing_root, limits),
        Err(ExperimentError::Unavailable)
    ));

    const V1_TRIAL_ID: [u8; 32] = [
        204, 192, 179, 164, 28, 112, 23, 169, 97, 98, 112, 138, 90, 51, 232, 124, 231, 70, 161,
        146, 67, 140, 231, 172, 104, 88, 18, 93, 39, 92, 71, 221,
    ];
    const V1_RESERVATION: &str = r#"{"schema_version":1,"trial_id":"ccc0b3a41c7017a96162708a5a33e87ce746a192438ce7ac6858125d275c47dd","spec":{"dataset_identity":"0101010101010101010101010101010101010101010101010101010101010101","object_graph_digest":"0202020202020202020202020202020202020202020202020202020202020202","execution_assumption_digest":"0303030303030303030303030303030303030303030303030303030303030303","model":null,"strategy":{"name":"strategy-v1","digest":"0505050505050505050505050505050505050505050505050505050505050505"},"code":{"name":"code-revision","digest":"0606060606060606060606060606060606060606060606060606060606060606"},"configuration_digest":"0707070707070707070707070707070707070707070707070707070707070707","seed":7,"parameters":[],"search_space":[],"selection_criterion":"total-return"}}"#;
    std::fs::write(
        temporary
            .path()
            .join("backtesting/v1/reservations/ccc0b3a41c7017a96162708a5a33e87ce746a192438ce7ac6858125d275c47dd.json"),
        V1_RESERVATION,
    )?;
    let legacy_id = TrialId::from_digest(Sha256Digest::new(V1_TRIAL_ID));
    assert_eq!(inventory.trial(legacy_id)?.spec().id(), legacy_id);

    let legacy_artifact = br#"{"legacy":true}"#;
    let legacy_artifact_reference = "backtesting/v1/artifacts/sha256/60/600bfa81b1561fa6281505a8630327ec94da208976f36c142c781b0b46a95725.json";
    let legacy_terminal_path = temporary
        .path()
        .join("backtesting/v1/terminals/ccc0b3a41c7017a96162708a5a33e87ce746a192438ce7ac6858125d275c47dd.json");
    std::fs::write(
        &legacy_terminal_path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "trial_id": "ccc0b3a41c7017a96162708a5a33e87ce746a192438ce7ac6858125d275c47dd",
            "status": "completed",
            "completed": {
                "result_digest": "0909090909090909090909090909090909090909090909090909090909090909",
                "artifact_reference": legacy_artifact_reference,
                "artifact_digest": "600bfa81b1561fa6281505a8630327ec94da208976f36c142c781b0b46a95725",
                "artifact_bytes": legacy_artifact.len(),
                "metrics": [],
                "probability_of_backtest_overfitting": 0.25,
                "probability_fold_count": 2,
                "deflated_performance_probability": 0.75,
                "expected_maximum_sharpe": 0.5,
                "selected": true
            },
            "failed": null
        }))?,
    )?;
    assert!(matches!(
        inventory.trial(legacy_id),
        Err(ExperimentError::CorruptRecord)
    ));
    let legacy_artifact_path = temporary.path().join(legacy_artifact_reference);
    std::fs::create_dir_all(
        legacy_artifact_path
            .parent()
            .ok_or("missing legacy artifact parent")?,
    )?;
    std::fs::write(&legacy_artifact_path, legacy_artifact)?;
    assert!(matches!(
        inventory.trial(legacy_id)?.status(),
        TrialStatus::Completed(_)
    ));
    std::fs::write(&legacy_artifact_path, b"xxxxxxxxxxxxxxx")?;
    assert!(matches!(
        inventory.trial(legacy_id),
        Err(ExperimentError::CorruptRecord)
    ));

    const V2_TRIAL_ID: [u8; 32] = [
        62, 253, 167, 147, 117, 60, 225, 151, 9, 248, 210, 184, 142, 210, 23, 180, 136, 19, 17,
        215, 191, 201, 233, 214, 201, 33, 9, 47, 49, 121, 83, 50,
    ];
    const V2_TRIAL_HEX: &str = "3efda793753ce19709f8d2b88ed217b4881311d7bfc9e9d6c921092f31795332";
    const V2_RESERVATION: &str = r#"{"schema_version":2,"trial_id":"3efda793753ce19709f8d2b88ed217b4881311d7bfc9e9d6c921092f31795332","spec":{"dataset_identity":"0101010101010101010101010101010101010101010101010101010101010101","object_graph_digest":"0202020202020202020202020202020202020202020202020202020202020202","execution_assumption_digest":"0303030303030303030303030303030303030303030303030303030303030303","run_input_digest":"0404040404040404040404040404040404040404040404040404040404040404","model":null,"strategy":{"name":"strategy-v1","digest":"0505050505050505050505050505050505050505050505050505050505050505"},"code":{"name":"code-revision","digest":"0606060606060606060606060606060606060606060606060606060606060606"},"configuration_digest":"0707070707070707070707070707070707070707070707070707070707070707","seed":7,"parameters":[],"search_space":[],"selection_criterion":"total-return"}}"#;
    std::fs::write(
        temporary
            .path()
            .join(format!("backtesting/v1/reservations/{V2_TRIAL_HEX}.json")),
        V2_RESERVATION,
    )?;
    let v2_terminal_path = temporary
        .path()
        .join(format!("backtesting/v1/terminals/{V2_TRIAL_HEX}.json"));
    let failed_terminal = |schema_version| {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": schema_version,
            "trial_id": V2_TRIAL_HEX,
            "status": "failed",
            "completed": null,
            "failed": {
                "code": "legacy-failure",
                "evidence_digest": "0808080808080808080808080808080808080808080808080808080808080808"
            }
        }))
    };
    std::fs::write(&v2_terminal_path, failed_terminal(1)?)?;
    let v2_id = TrialId::from_digest(Sha256Digest::new(V2_TRIAL_ID));
    assert!(matches!(
        inventory.trial(v2_id),
        Err(ExperimentError::CorruptRecord)
    ));
    std::fs::write(&v2_terminal_path, failed_terminal(2)?)?;
    assert!(matches!(
        inventory.trial(v2_id)?.status(),
        TrialStatus::Failed(_)
    ));

    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let request = request(account_id, dataset(execution_terms()?)?, None)?;
    let spec = TrialSpec::try_new(TrialSpecInput {
        dataset_identity: request.dataset_identity(),
        object_graph_digest: request.object_graph_digest(),
        execution_assumption_digest: request.assumption_digest(),
        run_input_digest: request.run_input_digest(),
        cohort_authority_digest: request.cohort_authority_digest(),
        cohort_universe_digest: None,
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
    let current_trial_hex = spec
        .id()
        .digest()
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let current_legacy_terminal = temporary
        .path()
        .join(format!("backtesting/v1/terminals/{current_trial_hex}.json"));
    std::fs::write(
        &current_legacy_terminal,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 2,
            "trial_id": current_trial_hex,
            "status": "failed",
            "completed": null,
            "failed": {
                "code": "wrong-path",
                "evidence_digest": "0808080808080808080808080808080808080808080808080808080808080808"
            }
        }))?,
    )?;
    assert!(matches!(
        inventory.trial(spec.id()),
        Err(ExperimentError::CorruptRecord)
    ));
    std::fs::remove_file(current_legacy_terminal)?;
    assert!(matches!(
        inventory.reserve_at(spec.clone(), Timestamp::from_unix_nanos(110), 10),
        Err(ExperimentError::TrialInProgress)
    ));
    drop(_first);
    let recovered = inventory.reserve_at(spec.clone(), Timestamp::from_unix_nanos(110), 10)?;
    let artifact_bytes = br#"{"recovered":true}"#;
    let artifact = inventory.prepare_artifact(artifact_bytes)?;
    let completion = inventory.prepare_completion(
        &recovered,
        TrialCompletionInput {
            result_digest: Sha256Digest::new([9; 32]),
            artifact: artifact.clone(),
            metrics: vec![TrialMetric::try_new(
                SourceIdentifier::try_from("total-return")?,
                0.1,
            )?],
            dataset_partition: Some(TrialDatasetPartition::try_new(
                Timestamp::from_unix_nanos(10),
                Timestamp::from_unix_nanos(30),
            )?),
        },
    )?;
    inventory.complete(recovered, completion, artifact_bytes)?;

    let final_path = temporary.path().join(artifact.reference());
    let pending_path = temporary
        .path()
        .join("backtesting/v1/pending")
        .join(current_trial_hex)
        .join("00000000000000000002.json");
    std::fs::create_dir_all(pending_path.parent().ok_or("missing pending parent")?)?;
    std::fs::copy(&final_path, &pending_path)?;
    std::fs::remove_file(&final_path)?;
    assert!(matches!(
        inventory.trial(spec.id())?.status(),
        TrialStatus::Completed(_)
    ));
    assert!(final_path.is_file());
    assert!(!pending_path.exists());

    drop(inventory);
    let reopened_root = Dir::open_ambient_dir(temporary.path(), ambient_authority())?;
    let reopened = ExperimentInventory::try_new(reopened_root, limits)?;
    assert!(matches!(
        reopened.trial(spec.id())?.status(),
        TrialStatus::Completed(_)
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
        time_in_force: TimeInForce::Day,
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

type CompletedCohortFold = (
    BacktestCohortFold,
    Vec<TrialId>,
    Vec<BacktestCohortCandidate>,
);

#[allow(
    clippy::too_many_arguments,
    reason = "the existing cohort harness keeps each authority and exact partition explicit"
)]
fn completed_cohort_fold(
    service: &BacktestService,
    registry: &BacktestStrategyRegistry,
    build_id: &SourceIdentifier,
    account_id: AccountId,
    in_sample_start: i64,
    out_of_sample_start: i64,
    parameters: &[&str],
    universe: &BacktestCohortUniverse,
) -> Result<CompletedCohortFold, Box<dyn Error>> {
    let mut candidates = Vec::new();
    let mut selection = Vec::new();
    for parameter in parameters {
        let in_sample = run_governed_trial(
            service,
            registry,
            build_id,
            account_id,
            in_sample_start,
            parameter,
            universe,
        )?;
        let out_of_sample = run_governed_trial(
            service,
            registry,
            build_id,
            account_id,
            out_of_sample_start,
            parameter,
            universe,
        )?;
        candidates.push(BacktestCohortCandidate::new(in_sample, out_of_sample));
        selection.push(out_of_sample);
    }
    Ok((
        BacktestCohortFold::try_new(candidates.clone())?,
        selection,
        candidates,
    ))
}

fn run_governed_trial(
    service: &BacktestService,
    registry: &BacktestStrategyRegistry,
    build_id: &SourceIdentifier,
    account_id: AccountId,
    dataset_start: i64,
    parameter: &str,
    universe: &BacktestCohortUniverse,
) -> Result<TrialId, Box<dyn Error>> {
    run_governed_request(
        service,
        registry,
        build_id,
        request(
            account_id,
            dataset_at(execution_terms()?, dataset_start)?,
            None,
        )?,
        parameter,
        universe,
    )
}

fn run_governed_request(
    service: &BacktestService,
    registry: &BacktestStrategyRegistry,
    build_id: &SourceIdentifier,
    request: BacktestRequest,
    parameter: &str,
    universe: &BacktestCohortUniverse,
) -> Result<TrialId, Box<dyn Error>> {
    let mut strategy = registry.admit(build_id)?;
    let parameter_name = SourceIdentifier::try_from("speed")?;
    let outcome = service.run(
        request,
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
                    SourceIdentifier::try_from("medium")?,
                    SourceIdentifier::try_from("slow")?,
                ],
            )?],
            SourceIdentifier::try_from("total-return")?,
        )
        .with_cohort_universe(universe.clone()),
        &CancellationToken::new(),
    )?;
    let BacktestOutcome::Completed(result) = outcome else {
        return Err("expected completed cohort member".into());
    };
    Ok(result.trial().spec().id())
}

fn cohort_universe(
    fold_starts: &[(i64, i64)],
    selection_start: i64,
) -> Result<BacktestCohortUniverse, Box<dyn Error>> {
    let folds = fold_starts
        .iter()
        .map(|(in_sample, out_of_sample)| {
            BacktestCohortFoldPartition::try_new(
                cohort_partition(*in_sample)?,
                cohort_partition(*out_of_sample)?,
            )
        })
        .collect::<Result<Vec<_>, ExperimentError>>()?;
    Ok(BacktestCohortUniverse::try_new(
        SourceIdentifier::try_from("walk-forward-generator-v1")?,
        vec![TrialParameter::new(
            SourceIdentifier::try_from("fold-count")?,
            SourceIdentifier::try_from(format!("{}-folds", fold_starts.len()))?,
        )],
        3,
        folds,
        cohort_partition(selection_start)?,
    )?)
}

fn cohort_partition(starts_at: i64) -> Result<BacktestCohortPartition, ExperimentError> {
    let dataset = dataset_at(
        execution_terms().map_err(|_| ExperimentError::InvalidDiagnostic)?,
        starts_at,
    )
    .map_err(|_| ExperimentError::InvalidDiagnostic)?;
    BacktestCohortPartition::try_new(
        dataset.identity(),
        dataset.object_graph_digest(),
        TrialDatasetPartition::try_new(
            Timestamp::from_unix_nanos(starts_at),
            Timestamp::from_unix_nanos(starts_at + 20),
        )?,
    )
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

fn dataset_with_depths(
    terms: InstrumentExecutionTerms,
    depths: [i64; 3],
) -> Result<BacktestDataset, Box<dyn Error>> {
    Ok(BacktestDataset::try_new(BacktestDatasetInput {
        manifest: feature_manifest()?,
        object_graph_digest: Sha256Digest::new([2; 32]),
        point_in_time_content: Sha256Digest::new([3; 32]),
        point_in_time_audit: Sha256Digest::new([4; 32]),
        observations: [10, 20, 30]
            .into_iter()
            .zip(depths)
            .map(|(at, depth)| observation(terms, at, 100 + at, depth))
            .collect::<Result<Vec<_>, _>>()?,
    })?)
}

fn request(
    account_id: AccountId,
    dataset: BacktestDataset,
    corporate_actions: Option<market_squawk_data::CorporateActionPlan>,
) -> Result<BacktestRequest, Box<dyn Error>> {
    Ok(BacktestRequest::try_new(
        dataset,
        research_assumptions()?,
        PortfolioSeed::try_new(
            account_id,
            Money::new(Decimal::new(100_000, 2), Currency::try_from("USD")?),
            portfolio_limits()?,
        )?,
        corporate_actions,
        vec![SourceIdentifier::try_from("task11-feature-labels")?],
        7,
        backtest_limits()?,
    )?)
}

fn research_assumptions() -> Result<ResearchExecutionAssumptions, Box<dyn Error>> {
    Ok(ResearchExecutionAssumptions::try_new(
        ResearchExecutionAssumptionsInput {
            version: RESEARCH_EXECUTION_POLICY_VERSION,
            fee_basis_points: BasisPoints::new(10),
            slippage_basis_points: BasisPoints::new(5),
            maximum_random_slippage_basis_points: BasisPoints::new(0),
            maximum_participation_basis_points: BasisPoints::new(10_000),
            liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
            latency_nanos: 1,
            allow_partial_fills: true,
            fee_decimal_scale: 4,
        },
    )?)
}

fn backtest_limits() -> Result<BacktestLimits, Box<dyn Error>> {
    Ok(BacktestLimits::try_new(BacktestLimitsInput {
        max_observations: 100,
        max_pending_intents: 16,
        max_fills: 16,
        max_retained_bytes: 1_000_000,
    })?)
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
