use std::cmp::Ordering;

use market_squawk_domain::{DataQuality, ExecutionEligibility};

use super::requirements::{depth_preference, quality_preference};
use super::{
    AdmittedDowngrade, BudgetAvailability, CandidateRejectionReason, DowngradeDimension,
    EligibleCandidate, FreshnessBasis, HealthState, IntegrityState, MarketSelectionError,
    MarketSelectionPolicy, MarketSelectionReceipt, MarketSelectionRequest, RejectedCandidate,
    RightsState, SourceCandidate,
};

const MAXIMUM_REJECTION_REASONS: usize = 24;
const MAXIMUM_DOWNGRADE_DIMENSIONS: usize = 5;

enum EvaluatedCandidate {
    Eligible(EligibleCandidate),
    Rejected(RejectedCandidate),
}

/// Deterministically selects the richest eligible source and retains the complete decision receipt.
pub(crate) fn select_market_source(
    policy: MarketSelectionPolicy,
    request: MarketSelectionRequest,
    mut candidates: Vec<SourceCandidate>,
) -> Result<MarketSelectionReceipt, MarketSelectionError> {
    if candidates.len() > policy.maximum_candidates() {
        return Err(MarketSelectionError::TooManyCandidates {
            maximum: policy.maximum_candidates(),
            actual: candidates.len(),
        });
    }

    candidates.sort_unstable_by(|left, right| left.identity().stable_cmp(right.identity()));
    if candidates
        .windows(2)
        .any(|pair| pair[0].identity().stable_cmp(pair[1].identity()) == Ordering::Equal)
    {
        return Err(MarketSelectionError::DuplicateCandidateIdentity);
    }

    let selected_at = request.freshness().as_of();
    let mut evaluated = Vec::new();
    evaluated
        .try_reserve_exact(candidates.len())
        .map_err(|_| MarketSelectionError::Allocation)?;
    for candidate in candidates {
        evaluated.push(evaluate_candidate(&request, candidate)?);
    }

    let eligible_count = evaluated
        .iter()
        .filter(|candidate| matches!(candidate, EvaluatedCandidate::Eligible(_)))
        .count();
    let rejected_count = evaluated.len() - eligible_count;
    let mut eligible = Vec::new();
    eligible
        .try_reserve_exact(eligible_count)
        .map_err(|_| MarketSelectionError::Allocation)?;
    let mut rejected = Vec::new();
    rejected
        .try_reserve_exact(rejected_count)
        .map_err(|_| MarketSelectionError::Allocation)?;
    for candidate in evaluated {
        match candidate {
            EvaluatedCandidate::Eligible(candidate) => eligible.push(candidate),
            EvaluatedCandidate::Rejected(candidate) => rejected.push(candidate),
        }
    }

    eligible.sort_unstable_by(compare_eligible);
    rejected.sort_unstable_by(|left, right| {
        left.candidate()
            .identity()
            .stable_cmp(right.candidate().identity())
    });

    Ok(MarketSelectionReceipt::new(
        policy.revision(),
        policy.digest(),
        request,
        eligible,
        rejected,
        selected_at,
    ))
}

fn evaluate_candidate(
    request: &MarketSelectionRequest,
    candidate: SourceCandidate,
) -> Result<EvaluatedCandidate, MarketSelectionError> {
    let mut reasons = Vec::new();
    reasons
        .try_reserve_exact(MAXIMUM_REJECTION_REASONS)
        .map_err(|_| MarketSelectionError::Allocation)?;
    let mut downgrade = Vec::new();
    downgrade
        .try_reserve_exact(MAXIMUM_DOWNGRADE_DIMENSIONS)
        .map_err(|_| MarketSelectionError::Allocation)?;

    evaluate_identity_and_operation(request, &candidate, &mut reasons);
    evaluate_source_state(request, &candidate, &mut reasons);
    evaluate_requirements(request, &candidate, &mut reasons, &mut downgrade);
    let freshness_age = evaluate_freshness(request, &candidate, &mut reasons, &mut downgrade);

    if reasons.is_empty() {
        let age = freshness_age.ok_or(MarketSelectionError::InvalidTimestampOrder)?;
        let downgrade = if downgrade.is_empty() {
            None
        } else {
            Some(AdmittedDowngrade::new(downgrade))
        };
        Ok(EvaluatedCandidate::Eligible(EligibleCandidate::new(
            candidate, age, downgrade,
        )))
    } else {
        Ok(EvaluatedCandidate::Rejected(RejectedCandidate::new(
            candidate, reasons,
        )))
    }
}

fn evaluate_identity_and_operation(
    request: &MarketSelectionRequest,
    candidate: &SourceCandidate,
    reasons: &mut Vec<CandidateRejectionReason>,
) {
    let capabilities = candidate.capabilities();
    if capabilities.asset_class() != request.asset_class() {
        reasons.push(CandidateRejectionReason::AssetMismatch {
            required: request.asset_class(),
            actual: capabilities.asset_class(),
        });
    }
    if !capabilities.operations().contains(request.operation()) {
        reasons.push(CandidateRejectionReason::OperationUnsupported {
            operation: request.operation(),
        });
    }
}

fn evaluate_source_state(
    request: &MarketSelectionRequest,
    candidate: &SourceCandidate,
    reasons: &mut Vec<CandidateRejectionReason>,
) {
    let as_of = request.freshness().as_of();
    let timestamps = candidate.timestamps();
    let admission = candidate.admission();
    let health = admission.health();
    let budget = admission.budget();
    let rights = admission.rights();
    let integrity = admission.integrity();

    if timestamps.effective_at() > as_of {
        reasons.push(CandidateRejectionReason::EffectiveAfterSelection);
    }
    if timestamps.available_at() > as_of {
        reasons.push(CandidateRejectionReason::AvailableAfterSelection);
    }
    if timestamps.ingested_at() > as_of {
        reasons.push(CandidateRejectionReason::IngestedAfterSelection);
    }

    if health.observed_at() > as_of {
        reasons.push(CandidateRejectionReason::HealthObservedAfterSelection);
    }
    match health.state() {
        HealthState::Healthy => {}
        HealthState::Degraded if !request.operation().requires_execution_quality() => {}
        HealthState::Degraded => {
            reasons.push(CandidateRejectionReason::ExecutionRequiresHealthySource);
        }
        state @ (HealthState::Unavailable | HealthState::Quarantined) => {
            reasons.push(CandidateRejectionReason::HealthUnavailable { state });
        }
    }

    if budget.observed_at() > as_of {
        reasons.push(CandidateRejectionReason::BudgetObservedAfterSelection);
    }
    match budget.availability() {
        BudgetAvailability::Exhausted | BudgetAvailability::Unknown => {
            reasons.push(CandidateRejectionReason::BudgetUnavailable {
                state: budget.availability(),
            });
        }
        BudgetAvailability::InteractiveOnly if !budget.admits(request.priority()) => {
            reasons.push(CandidateRejectionReason::BudgetPriorityDenied);
        }
        BudgetAvailability::NotRequired
        | BudgetAvailability::Open
        | BudgetAvailability::InteractiveOnly => {}
    }

    if rights.state() != RightsState::Admitted {
        reasons.push(CandidateRejectionReason::RightsUnavailable {
            state: rights.state(),
        });
    } else {
        if rights.decided_at() > as_of {
            reasons.push(CandidateRejectionReason::RightsObservedAfterSelection);
        }
        if !rights.permitted_operations().contains(request.operation()) {
            reasons.push(CandidateRejectionReason::RightsOperationDenied);
        }
        if rights.effective_from().is_none_or(|from| as_of < from) {
            reasons.push(CandidateRejectionReason::RightsNotEffective);
        }
        if rights.effective_until().is_some_and(|until| as_of > until) {
            reasons.push(CandidateRejectionReason::RightsExpired);
        }
    }

    if integrity.assessed_at() > as_of {
        reasons.push(CandidateRejectionReason::IntegrityObservedAfterSelection);
    }
    if matches!(
        integrity.state(),
        IntegrityState::Failed | IntegrityState::Quarantined
    ) {
        reasons.push(CandidateRejectionReason::IntegrityUnavailable {
            state: integrity.state(),
        });
    }
    if candidate.capabilities().quality() == DataQuality::Quarantined {
        reasons.push(CandidateRejectionReason::QuarantinedQuality);
    }

    if request.operation().requires_execution_quality() {
        if candidate.capabilities().quality() != DataQuality::DirectVerified {
            reasons.push(CandidateRejectionReason::ExecutionRequiresDirectVerified {
                actual: candidate.capabilities().quality(),
            });
        }
        if admission.execution_eligibility() != ExecutionEligibility::Eligible {
            reasons.push(CandidateRejectionReason::ExecutionIneligible {
                actual: admission.execution_eligibility(),
            });
        }
    }
}

fn evaluate_requirements(
    request: &MarketSelectionRequest,
    candidate: &SourceCandidate,
    reasons: &mut Vec<CandidateRejectionReason>,
    downgrade: &mut Vec<DowngradeDimension>,
) {
    let capabilities = candidate.capabilities();
    let policy = request.downgrade();

    if capabilities.timing() != request.timing() {
        if policy.allows_timing(capabilities.timing()) {
            downgrade.push(DowngradeDimension::Timing {
                required: request.timing(),
                selected: capabilities.timing(),
            });
        } else {
            reasons.push(CandidateRejectionReason::TimingUnavailable {
                required: request.timing(),
                actual: capabilities.timing(),
            });
        }
    }

    if let Some(minimum) = request.minimum_depth()
        && depth_preference(capabilities.depth()) < depth_preference(Some(minimum))
    {
        if policy.allows_depth(capabilities.depth()) {
            downgrade.push(DowngradeDimension::Depth {
                minimum,
                selected: capabilities.depth(),
            });
        } else {
            reasons.push(CandidateRejectionReason::DepthUnavailable {
                minimum,
                actual: capabilities.depth(),
            });
        }
    }

    if quality_preference(capabilities.quality()) < quality_preference(request.minimum_quality()) {
        if policy.allows_quality(capabilities.quality()) {
            downgrade.push(DowngradeDimension::Quality {
                minimum: request.minimum_quality(),
                selected: capabilities.quality(),
            });
        } else {
            reasons.push(CandidateRejectionReason::QualityBelowMinimum {
                minimum: request.minimum_quality(),
                actual: capabilities.quality(),
            });
        }
    }

    if capabilities.coverage() != request.coverage() {
        if policy.allows_coverage(capabilities.coverage()) {
            downgrade.push(DowngradeDimension::Coverage {
                required: request.coverage(),
                selected: capabilities.coverage(),
            });
        } else {
            reasons.push(CandidateRejectionReason::CoverageUnavailable {
                required: request.coverage(),
                actual: capabilities.coverage(),
            });
        }
    }
}

fn evaluate_freshness(
    request: &MarketSelectionRequest,
    candidate: &SourceCandidate,
    reasons: &mut Vec<CandidateRejectionReason>,
    downgrade: &mut Vec<DowngradeDimension>,
) -> Option<u64> {
    let freshness = request.freshness();
    let timestamps = candidate.timestamps();
    let anchor = match freshness.basis() {
        FreshnessBasis::Source => timestamps.source_timestamp(),
        FreshnessBasis::Effective => Some(timestamps.effective_at()),
        FreshnessBasis::Received => Some(timestamps.received_at()),
        FreshnessBasis::Available => Some(timestamps.available_at()),
        FreshnessBasis::Ingested => Some(timestamps.ingested_at()),
    };
    let Some(anchor) = anchor else {
        reasons.push(CandidateRejectionReason::FreshnessAnchorMissing);
        return None;
    };
    let Some(age) = freshness
        .as_of()
        .unix_nanos()
        .checked_sub(anchor.unix_nanos())
        .and_then(|age| u64::try_from(age).ok())
    else {
        reasons.push(CandidateRejectionReason::FreshnessAnchorAfterSelection);
        return None;
    };

    if age > freshness.maximum_age_nanos() {
        if request
            .downgrade()
            .maximum_age_nanos()
            .is_some_and(|maximum| age <= maximum)
        {
            downgrade.push(DowngradeDimension::Freshness {
                maximum_age_nanos: freshness.maximum_age_nanos(),
                selected_age_nanos: age,
            });
        } else {
            reasons.push(CandidateRejectionReason::FreshnessExceeded {
                maximum_age_nanos: freshness.maximum_age_nanos(),
                actual_age_nanos: age,
            });
        }
    }
    Some(age)
}

fn compare_eligible(left: &EligibleCandidate, right: &EligibleCandidate) -> Ordering {
    let left_candidate = left.candidate();
    let right_candidate = right.candidate();
    let left_capabilities = left_candidate.capabilities();
    let right_capabilities = right_candidate.capabilities();
    let left_downgrades = left
        .downgrade()
        .map_or(0, |downgrade| downgrade.dimensions().len());
    let right_downgrades = right
        .downgrade()
        .map_or(0, |downgrade| downgrade.dimensions().len());

    left_downgrades
        .cmp(&right_downgrades)
        .then_with(|| {
            quality_preference(right_capabilities.quality())
                .cmp(&quality_preference(left_capabilities.quality()))
        })
        .then_with(|| {
            depth_preference(right_capabilities.depth())
                .cmp(&depth_preference(left_capabilities.depth()))
        })
        .then_with(|| {
            right_capabilities
                .timing()
                .preference()
                .cmp(&left_capabilities.timing().preference())
        })
        .then_with(|| {
            right_capabilities
                .coverage()
                .preference()
                .cmp(&left_capabilities.coverage().preference())
        })
        .then_with(|| left.freshness_age_nanos().cmp(&right.freshness_age_nanos()))
        .then_with(|| {
            health_preference(right_candidate.admission().health().state()).cmp(&health_preference(
                left_candidate.admission().health().state(),
            ))
        })
        .then_with(|| {
            right_candidate
                .admission()
                .budget()
                .preference()
                .cmp(&left_candidate.admission().budget().preference())
        })
        .then_with(|| {
            left_candidate
                .identity()
                .stable_cmp(right_candidate.identity())
        })
}

const fn health_preference(state: HealthState) -> u8 {
    match state {
        HealthState::Healthy => 2,
        HealthState::Degraded => 1,
        HealthState::Unavailable | HealthState::Quarantined => 0,
    }
}
