use std::error::Error;
use std::num::{NonZeroU32, NonZeroUsize};

use cap_std::ambient_authority;
use cap_std::fs::Dir;

use crate::dataset::{BacktestDatasetInput, BacktestObservationInput};
use crate::{
    AccountingReconciliation, BacktestDataset, BacktestEngine, BacktestEvaluation, BacktestLimits,
    BacktestLimitsInput, BacktestModelDecisionMapper, BacktestModelStrategy, BacktestObservation,
    BacktestOutcome, BacktestOverfittingDiagnostic, BacktestOverfittingFold,
    BacktestOverfittingInput, BacktestOverfittingScore, BacktestRequest, BacktestService,
    BacktestStrategy, DeflatedPerformanceDiagnostic, DeflatedPerformanceInput, ExperimentInventory,
    ExperimentLimits, ExperimentLimitsInput, HistoricalUniverseStatus, PortfolioSeed,
    ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, TrialComponentBinding,
    TrialMetric, TrialSpec, TrialSpecInput, TrialStatus,
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
}

impl BacktestStrategy for BuyOnce {
    fn on_observation(
        &mut self,
        context: &crate::BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let mut output = BoundedOrderIntents::new();
        if !self.emitted {
            output.try_push(
                OrderIntent::try_new(OrderIntentInput {
                    order_id: "00000000-0000-0000-0000-000000000040"
                        .parse()
                        .map_err(|_| StrategyError::Evaluation)?,
                    client_order_id: ClientOrderId::try_from("backtest-buy-1")
                        .map_err(|_| StrategyError::Evaluation)?,
                    strategy_id: "00000000-0000-0000-0000-000000000041"
                        .parse()
                        .map_err(|_| StrategyError::Evaluation)?,
                    model_id: None,
                    account_id: self.account_id,
                    execution_terms: context.execution_terms(),
                    side: OrderSide::Buy,
                    order_type: OrderType::Market,
                    quantity: QuantityLots::new(4).map_err(|_| StrategyError::Evaluation)?,
                    limit_price: None,
                    stop_price: None,
                    time_in_force: TimeInForce::Day,
                    signal_at: context.decision_at(),
                    expires_at: context
                        .decision_at()
                        .checked_add_nanos(100)
                        .map_err(|_| StrategyError::Evaluation)?,
                    reason_codes: vec![
                        OrderReasonCode::try_from("research-signal")
                            .map_err(|_| StrategyError::Evaluation)?,
                    ],
                    maximum_slippage: BasisPoints::new(100),
                    required_quality: DataQuality::DirectVerified,
                })
                .map_err(|_| StrategyError::Evaluation)?,
            )?;
            self.emitted = true;
        }
        Ok(output)
    }
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
    let spec = TrialSpec::try_new(TrialSpecInput {
        dataset_identity: request.dataset_identity(),
        object_graph_digest: request.object_graph_digest(),
        execution_assumption_digest: request.assumption_digest(),
        model: Some(binding("model-v1", 4)?),
        strategy: binding("strategy-v1", 5)?,
        code: binding("code-revision", 6)?,
        configuration_digest: Sha256Digest::new([7; 32]),
        seed: request.seed(),
        parameters: Vec::new(),
        search_space: Vec::new(),
        selection_criterion: SourceIdentifier::try_from("deflated-sharpe")?,
    })?;
    let overfitting = BacktestOverfittingDiagnostic::try_compute(&BacktestOverfittingInput {
        folds: vec![
            BacktestOverfittingFold {
                candidates: vec![
                    BacktestOverfittingScore {
                        in_sample: 2.0,
                        out_of_sample: 0.1,
                    },
                    BacktestOverfittingScore {
                        in_sample: 1.0,
                        out_of_sample: 0.2,
                    },
                ],
            },
            BacktestOverfittingFold {
                candidates: vec![
                    BacktestOverfittingScore {
                        in_sample: 1.0,
                        out_of_sample: 0.3,
                    },
                    BacktestOverfittingScore {
                        in_sample: 2.0,
                        out_of_sample: 0.2,
                    },
                ],
            },
        ],
    })?;
    let deflated = DeflatedPerformanceDiagnostic::try_compute(DeflatedPerformanceInput {
        observed_sharpe: 1.2,
        independent_trials: 4,
        observations: 252,
        trial_sharpe_variance: 0.04,
        return_skewness: 0.0,
        return_excess_kurtosis: 0.0,
    })?;
    let evaluation = BacktestEvaluation::try_new(
        vec![TrialMetric::try_new(
            SourceIdentifier::try_from("sharpe")?,
            1.2,
        )?],
        overfitting,
        deflated,
        false,
    )?;
    let mut strategy = BuyOnce {
        account_id,
        emitted: false,
    };
    let outcome = service.run(
        spec,
        request,
        &mut strategy,
        evaluation,
        &CancellationToken::new(),
    )?;
    let BacktestOutcome::Completed(result) = outcome else {
        return Err("expected completed governed trial".into());
    };
    let TrialStatus::Completed(completion) = result.trial().status() else {
        return Err("expected completed terminal".into());
    };
    assert_eq!(result.run().fills().len(), 1);
    assert!(
        temporary
            .path()
            .join(completion.artifact().reference())
            .is_file()
    );
    Ok(())
}

#[test]
fn corporate_action_accounting_is_explicitly_task16_authoritative() -> TestResult {
    let account_id: AccountId = "00000000-0000-0000-0000-000000000030".parse()?;
    let terms = execution_terms()?;
    let plan = split_plan(terms.instrument_id())?;
    let request = request(account_id, dataset(terms)?, Some(plan))?;
    let mut strategy = BuyOnce {
        account_id,
        emitted: false,
    };

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert_eq!(
        result.accounting_reconciliation(),
        AccountingReconciliation::Task16AuthoritativeCorporateActions
    );
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
    let mut strategy = BacktestModelStrategy::new(
        Err(ModelFailure::from(InferenceError::NonFiniteComputation)),
        Box::new(RejectMapper),
    );

    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;

    assert!(result.fills().is_empty());
    assert_eq!(result.no_action_count(), 3);
    assert!(result.portfolio().positions().is_empty());
    Ok(())
}

fn binding(name: &str, byte: u8) -> Result<TrialComponentBinding, Box<dyn Error>> {
    Ok(TrialComponentBinding::try_new(
        SourceIdentifier::try_from(name)?,
        Sha256Digest::new([byte; 32]),
    )?)
}

fn dataset(terms: InstrumentExecutionTerms) -> Result<BacktestDataset, Box<dyn Error>> {
    Ok(BacktestDataset::try_new(BacktestDatasetInput {
        manifest: feature_manifest()?,
        object_graph_digest: Sha256Digest::new([2; 32]),
        point_in_time_content: Sha256Digest::new([3; 32]),
        point_in_time_audit: Sha256Digest::new([4; 32]),
        observations: vec![
            observation(terms, 10, 100, 10)?,
            observation(terms, 20, 110, 2)?,
            observation(terms, 30, 120, 10)?,
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
            version: 1,
            fee_basis_points: BasisPoints::new(10),
            slippage_basis_points: BasisPoints::new(5),
            maximum_random_slippage_basis_points: BasisPoints::new(0),
            maximum_participation_basis_points: BasisPoints::new(10_000),
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
