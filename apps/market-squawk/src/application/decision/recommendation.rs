//! Explicit composition from recommendation backtests into proposal evidence.

#![allow(
    dead_code,
    reason = "the workflow producer calls this narrow adapter at the next composition seam"
)]

use std::num::NonZeroU32;

use market_squawk_backtesting::{
    RECOMMENDATION_TARGET_HORIZON_NANOS_V1, RecommendationAggregateEvidenceV1,
};
use market_squawk_decisions::{
    ChronologicalOutOfSampleEvidence, CostAdjustedPitBacktestEvidence, DecisionContentDigest,
    FinancialModelEvidence, ProposalEvidenceWindow, TargetPriceCases, TargetPriceRange,
};
use market_squawk_domain::{BasisPoints, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_valuation::AutomaticValuationMethodReceipt;
use rust_decimal::Decimal;

use crate::application::{
    analysis::GovernedRecommendationBacktestEvidenceV1, research::MacroInvestmentContext,
};

/// Why complete recommendation-backtest evidence could not be admitted into a proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationBacktestAdapterError {
    IncompleteAggregate,
    InvalidCount,
    InexactBasisPoints,
    InvalidIdentity,
    InvalidProposalEvidence,
}

/// Exact historical-test and chronological OOS projections from one governed study.
pub(crate) struct RecommendationBacktestProposalEvidence {
    pub(crate) historical_test: CostAdjustedPitBacktestEvidence,
    pub(crate) out_of_sample: ChronologicalOutOfSampleEvidence,
}

/// Why a model receipt and exact Macro assumption context could not be joined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationFinancialModelAdapterError {
    MacroContextMismatch,
    InvalidIdentity,
    InvalidProposalEvidence,
}

/// Projects one method-specific model receipt with its exact contemporaneous Macro context.
///
/// Government yields remain assumption context. Individual methods must evidence any economic
/// assumption they actually use; this seam never relabels the context as a model, fair value,
/// recommendation confidence, or execution authority.
#[allow(
    clippy::too_many_arguments,
    reason = "model, scenario, sensitivity, Macro, horizon, and timing authorities remain explicit"
)]
pub(crate) fn adapt_financial_model_evidence(
    receipt: &AutomaticValuationMethodReceipt,
    macro_context: &MacroInvestmentContext,
    scenarios: TargetPriceCases,
    scenario_identity: DecisionContentDigest,
    sensitivity_range: TargetPriceRange,
    sensitivity_identity: DecisionContentDigest,
    horizon_at: Timestamp,
    window: ProposalEvidenceWindow,
) -> Result<FinancialModelEvidence, RecommendationFinancialModelAdapterError> {
    if macro_context.parent_manifests().is_empty()
        || macro_context.knowledge_cutoff() > receipt.measurement_at()
    {
        return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch);
    }
    let macro_context_identity = DecisionContentDigest::try_new(macro_context.evidence_digest())
        .map_err(|_| RecommendationFinancialModelAdapterError::InvalidIdentity)?;
    FinancialModelEvidence::try_from_automatic_valuation_receipt(
        receipt,
        scenarios,
        scenario_identity,
        sensitivity_range,
        sensitivity_identity,
        macro_context_identity,
        horizon_at,
        window,
    )
    .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)
}

/// Adapts the one strict 365-day recommendation backtest into proposal evidence.
///
/// The mapping retains PIT dataset, preauthorized signal cohort, execution-cost policy,
/// independent-fold stability, publication timing, and aggregate identities. It refuses partial
/// aggregates or decimal metrics that cannot be represented exactly by the proposal's basis-point
/// contract.
pub(crate) fn adapt_recommendation_backtest_v1(
    evidence: &GovernedRecommendationBacktestEvidenceV1,
) -> Result<RecommendationBacktestProposalEvidence, RecommendationBacktestAdapterError> {
    let aggregate = match evidence.aggregate() {
        RecommendationAggregateEvidenceV1::Available(value) => value,
        RecommendationAggregateEvidenceV1::Unavailable(_) => {
            return Err(RecommendationBacktestAdapterError::IncompleteAggregate);
        }
    };
    let policy = evidence.policy();
    let execution = policy.execution_assumptions();
    let publication = evidence.publication();
    let observations = NonZeroU32::new(
        u32::try_from(aggregate.observation_count())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let trials = NonZeroU32::new(
        u32::try_from(aggregate.trial_count())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let window = ProposalEvidenceWindow::try_new(
        publication.evaluated_at(),
        publication.available_at(),
        publication.expires_at(),
        content(publication.digest().bytes())?,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    let historical_test = CostAdjustedPitBacktestEvidence::try_new(
        policy.subject_instrument_id(),
        policy.reporting_currency(),
        RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
        exact_basis_points(aggregate.cost_adjusted_total_return())?,
        exact_basis_points(aggregate.worst_maximum_drawdown())?,
        execution.fee_basis_points(),
        execution.slippage_basis_points(),
        execution.maximum_random_slippage_basis_points(),
        observations,
        trials,
        aggregate.positive_fold_stability_ppm(),
        publication.simulation_cutoff(),
        content(evidence.dataset_identity().bytes())?,
        content(evidence.signal_plan_digest().bytes())?,
        content(aggregate.digest().bytes())?,
        content(evidence.digest().bytes())?,
        content(evidence.preauthorized_signal_plan_digest().bytes())?,
        content(policy.execution_assumption_digest().bytes())?,
        window,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    let study = evidence.study();
    let first_fold = study
        .folds()
        .first()
        .ok_or(RecommendationBacktestAdapterError::IncompleteAggregate)?;
    let last_fold = study
        .folds()
        .last()
        .ok_or(RecommendationBacktestAdapterError::IncompleteAggregate)?;
    let total_signals = NonZeroU32::new(
        u32::try_from(study.results().len())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let fold_count = NonZeroU32::new(
        u32::try_from(study.folds().len())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let completion_coverage_ppm = u32::try_from(
        u64::from(observations.get())
            .checked_mul(1_000_000)
            .ok_or(RecommendationBacktestAdapterError::InvalidCount)?
            / u64::from(total_signals.get()),
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?;
    let out_of_sample = ChronologicalOutOfSampleEvidence::try_new(
        policy.subject_instrument_id(),
        policy.reporting_currency(),
        RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
        first_fold.starts_at(),
        last_fold.ends_at(),
        publication.simulation_cutoff(),
        observations,
        total_signals,
        fold_count,
        completion_coverage_ppm,
        content(evidence.dataset_identity().bytes())?,
        content(evidence.signal_plan_digest().bytes())?,
        content(aggregate.digest().bytes())?,
        content(evidence.digest().bytes())?,
        window,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    Ok(RecommendationBacktestProposalEvidence {
        historical_test,
        out_of_sample,
    })
}

fn exact_basis_points(value: Decimal) -> Result<BasisPoints, RecommendationBacktestAdapterError> {
    let scaled = value
        .checked_mul(Decimal::from(10_000_u32))
        .ok_or(RecommendationBacktestAdapterError::InexactBasisPoints)?
        .normalize();
    if scaled.scale() != 0 {
        return Err(RecommendationBacktestAdapterError::InexactBasisPoints);
    }
    let integer = i32::try_from(scaled.mantissa())
        .map_err(|_| RecommendationBacktestAdapterError::InexactBasisPoints)?;
    Ok(BasisPoints::new(integer))
}

fn content(bytes: [u8; 32]) -> Result<DecisionContentDigest, RecommendationBacktestAdapterError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_| RecommendationBacktestAdapterError::InvalidIdentity)
}
