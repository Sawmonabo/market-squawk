//! Installed source recipe for a real governed three-fold recommendation study.

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use market_squawk_backtesting::{
    BacktestExecutionBasis, BacktestLimitsInput, RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1,
    RecommendationBacktestLimits, RecommendationBacktestLimitsInput,
    RecommendationBacktestPolicyV1, RecommendationBacktestPolicyV1Input,
    RecommendationBenchmarkPolicyV1, recommendation_conservative_execution_assumptions_v1,
};
use market_squawk_data::{
    AnalyticalFeatureDataset, CompleteMarketBarHistoryCursor, CorporateActionAdjustment,
    CorporateActionLimits, CorporateActionPlan, DatasetBuildPurpose, FeatureDatasetProductContract,
};
use market_squawk_domain::{Money, QuantityLots, SourceIdentifier};
use market_squawk_portfolio::PortfolioLimitInput;
use market_squawk_services::{RequestContext, ServiceError};
use rust_decimal::Decimal;

use super::{
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputRegistrationInput,
    GovernedBacktestPortfolioSeedInput, GovernedBacktestQueryLimitsInput,
    GovernedRecommendationSignalIssuerV1, HistoricalRecommendationAlphaProducer,
    ProductionGovernedBacktestInputAuthority,
};
use crate::{
    BacktestExperimentPlan, application::analysis::GovernedRecommendationBacktestRequestV1,
};

/// Existing source-owned reads, not public request fields or caller economic instructions.
/// The caller must obtain the action plan from the canonical source selection before this call.
pub(crate) struct RecommendationStudyPreparationInputV1 {
    pub(crate) producer: HistoricalRecommendationAlphaProducer,
    pub(crate) dataset: AnalyticalFeatureDataset,
    pub(crate) histories: [CompleteMarketBarHistoryCursor; 3],
    pub(crate) corporate_actions: CorporateActionPlan,
    pub(crate) source_action_reference:
        crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference,
}

/// The request and its concrete issuer are admitted together and cannot be exchanged between jobs.
pub(crate) struct PreparedRecommendationStudyV1 {
    request: GovernedRecommendationBacktestRequestV1,
    issuer: Arc<GovernedRecommendationSignalIssuerV1>,
}

impl PreparedRecommendationStudyV1 {
    pub(crate) fn into_parts(
        self,
    ) -> (
        GovernedRecommendationBacktestRequestV1,
        Arc<GovernedRecommendationSignalIssuerV1>,
    ) {
        (self.request, self.issuer)
    }
}

impl ProductionGovernedBacktestInputAuthority {
    /// Reopens every source and builds the exact installed raw-price/action recipe before issuing
    /// any instruction. The existing materializer owns all realized outcomes; the alpha producer
    /// only receives its separately sealed current-coordinate features.
    pub(crate) async fn prepare_recommendation_study(
        &self,
        input: RecommendationStudyPreparationInputV1,
        context: &RequestContext,
    ) -> Result<PreparedRecommendationStudyV1, ServiceError> {
        let RecommendationStudyPreparationInputV1 {
            producer,
            dataset,
            histories,
            corporate_actions,
            source_action_reference,
        } = input;
        let policy = dataset.study_policy().ok_or(ServiceError::InvalidRequest)?;
        let starts_at = producer.evaluation_starts_at();
        let ends_at = starts_at
            .checked_add_nanos(RECOMMENDATION_OOS_EVALUATION_HORIZON_NANOS_V1)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let source_cutoff = policy.snapshot_as_of();
        let subject = producer.subject_instrument_id();
        let benchmarks = producer.benchmarks();
        let mut instruments = [
            subject,
            benchmarks.primary().instrument_id(),
            benchmarks.accompanying().instrument_id(),
        ];
        instruments.sort_unstable();
        if dataset.product_contract()
            != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
            || policy.purpose() != DatasetBuildPurpose::StudyInputs
            || dataset.source_snapshot_digest() != Some(producer.source_snapshot_digest()?)
            || source_cutoff != producer.snapshot_as_of()?
            || policy.basis() != producer.study_basis()?
            || source_cutoff < ends_at
            || corporate_actions.policy().adjustment() != CorporateActionAdjustment::TotalReturn
            || source_action_reference.knowledge_cutoff() != source_cutoff
            || corporate_actions.knowledge_cutoff() != source_cutoff
            || corporate_actions.valuation_cutoff() != ends_at
            || !corporate_actions.conflicts().is_empty()
            || !corporate_actions.exclusions().is_empty()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut actual_instruments = histories
            .each_ref()
            .map(|history| history.selection().receipt().instrument_id());
        actual_instruments.sort_unstable();
        if actual_instruments != instruments {
            return Err(ServiceError::InvalidRequest);
        }
        let coverage = corporate_actions
            .source_admission()
            .ok_or(ServiceError::Unavailable)?;
        let mut coverage_instruments = coverage.instruments().to_vec();
        coverage_instruments.sort_unstable();
        if coverage_instruments != instruments
            || coverage.knowledge_cutoff() != source_cutoff
            || !coverage.unresolved().is_empty()
            || !coverage.ordinary_gaps().is_empty()
            || coverage.retained_source_audit().is_empty()
        {
            return Err(ServiceError::Unavailable);
        }
        if histories.iter().any(|history| {
            !coverage
                .history_input_manifests()
                .contains(history.selection().pinned().manifest())
        }) {
            return Err(ServiceError::InvalidRequest);
        }
        let coverage_starts_at = instruments
            .iter()
            .map(|id| coverage.application_starts_at(*id))
            .collect::<Option<Vec<_>>>()
            .and_then(|values| values.into_iter().max())
            .ok_or(ServiceError::Unavailable)?;
        if coverage_starts_at > starts_at {
            return Err(ServiceError::Unavailable);
        }
        let currency = histories
            .iter()
            .find(|history| history.selection().receipt().instrument_id() == subject)
            .and_then(|history| history.bars().next())
            .transpose()
            .map_err(|_| ServiceError::Unavailable)?
            .map(|bar| bar.currency())
            .ok_or(ServiceError::Unavailable)?;
        let mut roots = vec![dataset.generation().manifest().clone()];
        roots.extend(coverage.source_manifests().iter().cloned());
        roots.extend(coverage.history_input_manifests().iter().cloned());
        for history in &histories {
            let receipt = history.selection().receipt();
            let clocks = receipt.knowledge_clocks();
            let sessions = history.native_sessions().ok_or(ServiceError::Unavailable)?;
            let first = sessions
                .sessions()
                .first()
                .map_err(|_| ServiceError::Unavailable)?
                .ok_or(ServiceError::Unavailable)?;
            let last = sessions
                .sessions()
                .last()
                .map_err(|_| ServiceError::Unavailable)?
                .ok_or(ServiceError::Unavailable)?;
            if !receipt.realized_outcome_eligible()
                || clocks
                    .0
                    .max(clocks.1)
                    .max(clocks.2)
                    .max(receipt.published_at())
                    .max(receipt.capture_recorded_at())
                    > source_cutoff
                || first.closes_at_exclusive() > starts_at
                || last.closes_at_exclusive()
                    < ends_at
                        .checked_sub_nanos(1)
                        .map_err(|_| ServiceError::InvalidRequest)?
                || history
                    .bars()
                    .try_fold(false, |mismatch, bar| {
                        bar.map(|bar| mismatch || bar.currency() != currency)
                    })
                    .map_err(|_| ServiceError::Unavailable)?
            {
                return Err(ServiceError::Unavailable);
            }
            roots.push(history.selection().pinned().manifest().clone());
            roots.push(history.read_receipt().origin_manifest().clone());
        }
        // Source absence is not synthesized: the actual selected plan (including any genuine
        // zero-action disposition) must already have crossed canonical acquisition/admission.
        roots.extend(
            corporate_actions
                .admitted()
                .iter()
                .map(|record| record.source_manifest().clone()),
        );
        let sources = self
            .materializer
            .source_ids_for_roots(roots, context.cancellation().clone(), context.deadline())
            .await?;
        let account_id = producer.account_id();
        let issuer = Arc::new(GovernedRecommendationSignalIssuerV1::from_producer(
            producer, context,
        )?);
        let one = QuantityLots::new(1).map_err(|_| ServiceError::Internal)?;
        let assumptions = recommendation_conservative_execution_assumptions_v1()
            .map_err(|_| ServiceError::Internal)?;
        let registration = GovernedBacktestInputRegistrationInput {
            strategy_id: SourceIdentifier::try_from("recommendation-financial-alpha").map_err(|_| ServiceError::Internal)?,
            manifest: dataset.generation().manifest().clone(),
            table_name: "feature_labels".to_owned(),
            sql: "SELECT * FROM feature_labels ORDER BY decision_at, instrument_id, example_id, component_kind, component_name".to_owned(),
            query_limits: GovernedBacktestQueryLimitsInput {
                max_rows: 65_536, max_bytes: 256 * 1024, max_memory_bytes: 64 * 1024 * 1024,
                max_partitions: 64, max_ast_nodes: 512, max_plan_nodes: 2048,
                deadline: Duration::from_secs(60),
            },
            instruments: instruments.to_vec(), starts_at, ends_at, definition_history_limit: 4096,
            execution_assumptions: market_squawk_backtesting::ResearchExecutionAssumptionsInput {
                version: 3, fee_basis_points: assumptions.fee_basis_points(),
                slippage_basis_points: assumptions.slippage_basis_points(),
                maximum_random_slippage_basis_points: assumptions.maximum_random_slippage_basis_points(),
                maximum_participation_basis_points: assumptions.maximum_participation_basis_points(),
                liquidity_priority: assumptions.liquidity_priority(), latency_nanos: assumptions.latency_nanos(),
                allow_partial_fills: assumptions.allow_partial_fills(), fee_decimal_scale: assumptions.fee_decimal_scale(),
            },
            portfolio: GovernedBacktestPortfolioSeedInput {
                account_id, initial_cash: Money::new(Decimal::from(100_000), currency),
                limits: PortfolioLimitInput {
                    max_accounts: 1, max_instruments: 3, max_lots: 8192, max_transactions: 16384,
                    max_factors: 16, max_scenarios: 16, max_history: 8192, max_results: 16384,
                    max_retained_bytes: 32 * 1024 * 1024,
                },
            },
            corporate_actions: Some(GovernedBacktestCorporateActionsInput {
                policy: corporate_actions.policy(), knowledge_cutoff: source_cutoff, valuation_cutoff: ends_at,
                actions: corporate_actions.admitted().to_vec(),
                limits: CorporateActionLimits::try_new(
                    NonZeroUsize::new(4096).ok_or(ServiceError::Internal)?,
                    NonZeroUsize::new(4 * 1024 * 1024).ok_or(ServiceError::Internal)?,
                ).map_err(|_| ServiceError::InvalidRequest)?,
            }),
            sources, seed: 7,
            limits: BacktestLimitsInput {
                max_observations: 32_768, max_pending_intents: 8192, max_fills: 16_384,
                max_retained_bytes: 96 * 1024 * 1024,
            },
            experiment: BacktestExperimentPlan { parameters: Vec::new(), search_space: Vec::new(),
                selection_criterion: SourceIdentifier::try_from("predeclared-cost-adjusted-three-fold-study").map_err(|_| ServiceError::Internal)?, },
            cohort: None,
        };
        // Original source availability is frozen above; current admission/publication clocks stay actual.
        let admitted_at = super::issuer::wall_now().map_err(|_| ServiceError::Unavailable)?;
        let registration = self
            .register_recommendation_daily(
                registration,
                &histories,
                admitted_at,
                source_action_reference,
                context.cancellation().clone(),
                context.deadline(),
            )
            .await?;
        let benchmarks = issuer.producer.benchmarks();
        let policy = RecommendationBacktestPolicyV1::try_new(RecommendationBacktestPolicyV1Input {
            study_qualification: registration.study_qualification(),
            subject_instrument_id: subject,
            benchmark: RecommendationBenchmarkPolicyV1::try_new(
                benchmarks.primary().instrument_id(),
                benchmarks.primary().approval_digest(),
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            accompanying_benchmark: RecommendationBenchmarkPolicyV1::try_new(
                benchmarks.accompanying().instrument_id(),
                benchmarks.accompanying().approval_digest(),
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            raw_price_evidence_digest: registration.raw_price_evidence_digest(),
            corporate_action_content_digest: registration.corporate_action_content_digest(),
            corporate_action_audit_digest: registration.corporate_action_audit_digest(),
            corporate_action_coverage_starts_at: coverage_starts_at,
            execution_basis: BacktestExecutionBasis::CompletedDailyBar,
            reporting_currency: currency,
            subject_quantity: one,
            benchmark_quantity: one,
            maximum_entry_lag_nanos: 14 * 86_400_000_000_000,
            maximum_exit_lag_nanos: 14 * 86_400_000_000_000,
            execution_assumptions: assumptions,
            seed: 7,
        })
        .map_err(|_| ServiceError::InvalidRequest)?;
        let request = GovernedRecommendationBacktestRequestV1 {
            command: registration.into_command(),
            issuer_identity_digest: issuer.identity().digest(),
            issuer_reference: serde_json::to_string(issuer.reference())
                .map_err(|_| ServiceError::InvalidResult)?
                .into_boxed_str(),
            policy,
            evaluation_starts_at: starts_at,
            limits: RecommendationBacktestLimits::try_new(RecommendationBacktestLimitsInput {
                max_folds: 3,
                max_signals: 4096,
                max_equity_points_per_outcome: 512,
                max_total_equity_points: 1_000_000,
                max_observation_visits: 10_000_000,
            })
            .map_err(|_| ServiceError::InvalidRequest)?,
        };
        Ok(PreparedRecommendationStudyV1 { request, issuer })
    }
}
