//! Original subject-only registration and real all-origin execution under the existing owner.
use super::{
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputRegistrationInput,
    GovernedBacktestPortfolioSeedInput, GovernedBacktestQueryLimitsInput,
    ProductionGovernedBacktestInputAuthority,
};
use crate::{
    BacktestExperimentPlan, application::research::SourceAppliedCorporateActionPlanReference,
};
use market_squawk_backtesting::{
    AllOriginRoundTripEvaluationV1, AllOriginRoundTripPolicyV1, BacktestLimitsInput,
    RecommendationBacktestLimits, RecommendationBacktestLimitsInput, ResearchExecutionAssumptions,
};
use market_squawk_data::{
    AnalyticalFeatureDataset, CompleteMarketBarHistoryCursor, CorporateActionAdjustment,
    CorporateActionLimits, CorporateActionPlan, DatasetBuildPurpose, FeatureDatasetProductContract,
    ProbabilityCostPolicyV1, ProbabilityExecutionBasisV1, ProbabilityLiquidityPriorityV1,
    ProbabilityRoundTripConventionV1,
};
use market_squawk_domain::{AccountId, Currency, Money, SourceIdentifier};
use market_squawk_portfolio::PortfolioLimitInput;
use market_squawk_services::{RequestContext, ServiceError};
use rust_decimal::Decimal;
use std::{num::NonZeroUsize, time::Duration};
impl ProductionGovernedBacktestInputAuthority {
    /// Same explicit one-lot, fourteen-day fill windows and seed as the installed study recipe.
    pub(crate) fn probability_cost_policy(
        &self,
        currency: Currency,
        assumptions: ResearchExecutionAssumptions,
    ) -> Result<ProbabilityCostPolicyV1, ServiceError> {
        let policy = ProbabilityCostPolicyV1 {
            version: 1,
            execution_policy_version: 3,
            fee_basis_points: assumptions.fee_basis_points().get(),
            slippage_basis_points: assumptions.slippage_basis_points().get(),
            maximum_random_slippage_basis_points: assumptions
                .maximum_random_slippage_basis_points()
                .get(),
            maximum_participation_basis_points: assumptions
                .maximum_participation_basis_points()
                .get(),
            latency_nanos: assumptions.latency_nanos(),
            allow_partial_fills: assumptions.allow_partial_fills(),
            fee_decimal_scale: assumptions.fee_decimal_scale(),
            reporting_currency: currency,
            quantity_lots: 1,
            maximum_entry_lag_nanos: 14 * 86_400_000_000_000,
            maximum_exit_lag_nanos: 14 * 86_400_000_000_000,
            seed: 7,
            execution_basis: ProbabilityExecutionBasisV1::CompletedDailyBar,
            daily_bar_assumed_spread_basis_points: Some(20),
            liquidity_priority: ProbabilityLiquidityPriorityV1::SignalTimeThenOrderId,
            convention:
                ProbabilityRoundTripConventionV1::LongRoundTripTotalWealthIncludingEntitlements,
        };
        policy
            .validate()
            .map_err(|_| ServiceError::InvalidRequest)?;
        Ok(policy)
    }
    pub(crate) async fn prepare_probability_evaluation(
        &self,
        dataset: AnalyticalFeatureDataset,
        history: &CompleteMarketBarHistoryCursor,
        corporate_actions: CorporateActionPlan,
        source_action_reference: SourceAppliedCorporateActionPlanReference,
        policy: ProbabilityCostPolicyV1,
        account_id: AccountId,
        context: &RequestContext,
    ) -> Result<AllOriginRoundTripEvaluationV1, ServiceError> {
        let study = dataset.study_policy().ok_or(ServiceError::InvalidRequest)?;
        let horizon = study
            .target_horizon()
            .exact_elapsed()
            .and_then(|v| i64::try_from(v.as_nanos()).ok())
            .filter(|v| *v > 0)
            .ok_or(ServiceError::InvalidRequest)?;
        let execution = AllOriginRoundTripPolicyV1::try_new(policy, horizon)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let assumptions = execution.execution_assumptions();
        let source_cutoff = study.snapshot_as_of();
        let subject = history.selection().receipt().instrument_id();
        let sessions = history.native_sessions().ok_or(ServiceError::Unavailable)?;
        let starts_at = sessions
            .sessions()
            .first()
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::Unavailable)?
            .closes_at_exclusive();
        let ends_at = source_action_reference.valuation_cutoff();
        let currency = policy.reporting_currency;
        if dataset.product_contract()
            != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
            || study.purpose() != DatasetBuildPurpose::StudyInputs
            || source_cutoff < ends_at
            || starts_at >= ends_at
            || source_action_reference.knowledge_cutoff() != source_cutoff
            || corporate_actions.knowledge_cutoff() != source_cutoff
            || corporate_actions.valuation_cutoff() != ends_at
            || corporate_actions.policy().adjustment() != CorporateActionAdjustment::TotalReturn
            || !corporate_actions.conflicts().is_empty()
            || !corporate_actions.exclusions().is_empty()
            || !history.selection().receipt().realized_outcome_eligible()
            || history
                .bars()
                .try_fold(false, |mismatch, bar| {
                    bar.map(|bar| mismatch || bar.currency() != currency)
                })
                .map_err(|_| ServiceError::Unavailable)?
        {
            return Err(ServiceError::InvalidRequest);
        }
        let coverage = corporate_actions
            .source_admission()
            .ok_or(ServiceError::Unavailable)?;
        if !coverage.instruments().contains(&subject)
            || !coverage.unresolved().is_empty()
            || !coverage.ordinary_gaps().is_empty()
            || coverage.retained_source_audit().is_empty()
            || !coverage
                .history_input_manifests()
                .contains(history.selection().pinned().manifest())
            || coverage
                .application_starts_at(subject)
                .is_none_or(|at| at > starts_at)
        {
            return Err(ServiceError::Unavailable);
        }
        let mut roots = vec![
            dataset.generation().manifest().clone(),
            history.selection().pinned().manifest().clone(),
            history.read_receipt().origin_manifest().clone(),
        ];
        roots.extend(coverage.source_manifests().iter().cloned());
        roots.extend(coverage.history_input_manifests().iter().cloned());
        roots.extend(
            corporate_actions
                .admitted()
                .iter()
                .map(|row| row.source_manifest().clone()),
        );
        let sources = self
            .materializer
            .source_ids_for_roots(roots, context.cancellation().clone(), context.deadline())
            .await?;
        let registration = GovernedBacktestInputRegistrationInput {
            strategy_id: SourceIdentifier::try_from("probability-all-origin-costs").map_err(|_| ServiceError::Internal)?,
            manifest: dataset.generation().manifest().clone(),
            table_name: "feature_labels".to_owned(),
            sql: "SELECT * FROM feature_labels ORDER BY decision_at, instrument_id, example_id, component_kind, component_name".to_owned(),
            query_limits: GovernedBacktestQueryLimitsInput {
                max_rows: 65_536, max_bytes: 256 * 1024, max_memory_bytes: 64 * 1024 * 1024,
                max_partitions: 64, max_ast_nodes: 512, max_plan_nodes: 2048,
                deadline: Duration::from_secs(60),
            },
            instruments: vec![subject], starts_at, ends_at, definition_history_limit: 4096,
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
                    max_accounts: 1, max_instruments: 1, max_lots: 8192, max_transactions: 16384,
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
            sources, seed: policy.seed,
            limits: BacktestLimitsInput {
                max_observations: 32_768, max_pending_intents: 8192, max_fills: 16_384,
                max_retained_bytes: 96 * 1024 * 1024,
            },
            experiment: BacktestExperimentPlan { parameters: Vec::new(), search_space: Vec::new(),
                selection_criterion: SourceIdentifier::try_from("predeclared-all-origin-round-trip").map_err(|_| ServiceError::Internal)?, },
            cohort: None,
        };
        // Original source availability is frozen above; current admission/publication clocks stay actual.
        let admitted_at = super::issuer::wall_now().map_err(|_| ServiceError::Unavailable)?;
        let registration = self
            .register_recommendation_daily(
                registration,
                std::slice::from_ref(history),
                admitted_at,
                source_action_reference,
                context.cancellation().clone(),
                context.deadline(),
            )
            .await?;

        self.evaluate_all_origin_round_trips(
            registration.command(),
            subject,
            execution,
            RecommendationBacktestLimits::try_new(RecommendationBacktestLimitsInput {
                max_folds: 3,
                max_signals: 4096,
                max_equity_points_per_outcome: 512,
                max_total_equity_points: 1_000_000,
                max_observation_visits: 10_000_000,
            })
            .map_err(|_| ServiceError::Internal)?,
            context.cancellation().clone(),
            context.deadline(),
        )
        .await
    }
}
