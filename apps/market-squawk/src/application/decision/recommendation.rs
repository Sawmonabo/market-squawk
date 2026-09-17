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
    CostAdjustedPitBacktestEvidence, DecisionContentDigest, ProposalEvidenceWindow,
};
use market_squawk_domain::{BasisPoints, DigestAlgorithm, EvidenceDigest};
use rust_decimal::Decimal;

use crate::application::analysis::GovernedRecommendationBacktestEvidenceV1;

/// Why complete recommendation-backtest evidence could not be admitted into a proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationBacktestAdapterError {
    IncompleteAggregate,
    InvalidCount,
    InexactBasisPoints,
    InvalidIdentity,
    InvalidProposalEvidence,
}

/// Adapts the one strict 365-day recommendation backtest into proposal evidence.
///
/// The mapping retains PIT dataset, preauthorized signal cohort, execution-cost policy,
/// independent-fold stability, publication timing, and aggregate identities. It refuses partial
/// aggregates or decimal metrics that cannot be represented exactly by the proposal's basis-point
/// contract.
pub(crate) fn adapt_recommendation_backtest_v1(
    evidence: &GovernedRecommendationBacktestEvidenceV1,
) -> Result<CostAdjustedPitBacktestEvidence, RecommendationBacktestAdapterError> {
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
    CostAdjustedPitBacktestEvidence::try_new(
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
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)
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
