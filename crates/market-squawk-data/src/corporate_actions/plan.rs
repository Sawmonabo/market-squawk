//! Point-in-time admission and policy-specific adjustment planning.

use market_squawk_domain::{AvailabilityEvidence, CorporateActionKind, MergerConsideration};

use super::canonical::{audit_hash, canonical_record_bytes, content_hash};
use super::retained::{
    checked_plan_retained_bytes, plan_shape, require_retained_limit, try_reserve_exact,
};
use super::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusion, CorporateActionExclusionReason,
    CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
};

#[derive(Debug)]
struct EncodedRecord {
    canonical: Vec<u8>,
    record: CorporateActionRecord,
}

impl CorporateActionPlan {
    /// Builds one bounded immutable plan from source records and two independent cutoffs.
    ///
    /// An action is admitted only when conservative availability is no later than
    /// `knowledge_cutoff` and its exact effective instant is no later than `valuation_cutoff`.
    /// Raw records are moved into the result unchanged; adjustment steps refer to their canonical
    /// admitted index and never mutate source evidence.
    ///
    /// # Errors
    ///
    /// Rejects excessive work or retained memory, canonical encoding overflow, and fallible
    /// allocation failure.
    pub fn try_build(
        policy: CorporateActionPolicy,
        knowledge_cutoff: market_squawk_domain::Timestamp,
        valuation_cutoff: market_squawk_domain::Timestamp,
        actions: Vec<CorporateActionRecord>,
        limits: CorporateActionLimits,
    ) -> Result<Self, CorporateActionError> {
        if actions.len() > limits.max_actions.get() {
            return Err(CorporateActionError::ActionLimitExceeded {
                limit: limits.max_actions.get(),
                observed: actions.len(),
            });
        }
        let shape = plan_shape(policy, knowledge_cutoff, valuation_cutoff, &actions)?;
        require_retained_limit(
            shape.minimum_retained_bytes,
            limits.max_retained_bytes.get(),
        )?;

        let mut encoded = Vec::new();
        try_reserve_exact(&mut encoded, actions.len())?;
        let mut canonical_work_bytes = 0_usize;
        for record in actions {
            let canonical = canonical_record_bytes(&record)?;
            canonical_work_bytes = canonical_work_bytes
                .checked_add(canonical.capacity())
                .ok_or(CorporateActionError::RetainedSizeOverflow)?;
            require_retained_limit(canonical_work_bytes, limits.max_retained_bytes.get())?;
            encoded.push(EncodedRecord { canonical, record });
        }
        encoded.sort_by(|left, right| left.canonical.cmp(&right.canonical));

        let mut admitted = Vec::new();
        try_reserve_exact(&mut admitted, shape.admitted)?;
        let mut exclusions = Vec::new();
        try_reserve_exact(&mut exclusions, shape.exclusions)?;
        let mut steps = Vec::new();
        try_reserve_exact(&mut steps, shape.steps)?;
        let mut conflicts = Vec::new();
        try_reserve_exact(&mut conflicts, shape.conflicts)?;

        for encoded_record in encoded {
            let record = encoded_record.record;
            if let Some(reason) = exclusion_reason(&record, knowledge_cutoff, valuation_cutoff) {
                exclusions.push(CorporateActionExclusion { record, reason });
            } else {
                let admitted_index = admitted.len();
                append_policy_outputs(
                    policy.adjustment(),
                    admitted_index,
                    &record,
                    &mut steps,
                    &mut conflicts,
                );
                admitted.push(record);
            }
        }

        let content_hash = content_hash(
            policy,
            knowledge_cutoff,
            valuation_cutoff,
            &admitted,
            &steps,
            &conflicts,
        )?;
        let audit_hash = audit_hash(content_hash, &exclusions)?;
        let retained_bytes = checked_plan_retained_bytes(
            admitted.capacity(),
            &admitted,
            exclusions.capacity(),
            &exclusions,
            steps.capacity(),
            &steps,
            conflicts.capacity(),
        )?;
        require_retained_limit(retained_bytes, limits.max_retained_bytes.get())?;
        Ok(Self {
            policy,
            knowledge_cutoff,
            valuation_cutoff,
            admitted,
            exclusions,
            steps,
            conflicts,
            content_hash,
            audit_hash,
            retained_bytes,
        })
    }
}

pub(super) fn exclusion_reason(
    record: &CorporateActionRecord,
    knowledge_cutoff: market_squawk_domain::Timestamp,
    valuation_cutoff: market_squawk_domain::Timestamp,
) -> Option<CorporateActionExclusionReason> {
    let availability = record.observation.context().provenance().availability();
    match availability {
        AvailabilityEvidence::Evidenced { available_at, .. }
            if *available_at > knowledge_cutoff =>
        {
            return Some(CorporateActionExclusionReason::FutureAvailability);
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at }
            if *observed_at > knowledge_cutoff =>
        {
            return Some(CorporateActionExclusionReason::FutureAvailability);
        }
        AvailabilityEvidence::Inferred { .. } => {
            return Some(CorporateActionExclusionReason::InferredAvailability);
        }
        AvailabilityEvidence::Unknown => {
            return Some(CorporateActionExclusionReason::UnknownAvailability);
        }
        AvailabilityEvidence::Evidenced { .. }
        | AvailabilityEvidence::LocalFirstObserved { .. } => {}
    }
    let Some(effective_at) = record
        .observation
        .context()
        .time()
        .effective()
        .exact_timestamp()
    else {
        return Some(CorporateActionExclusionReason::AmbiguousEffectiveTime);
    };
    if effective_at > valuation_cutoff {
        Some(CorporateActionExclusionReason::FutureEffectiveTime)
    } else {
        None
    }
}

fn append_policy_outputs(
    adjustment: CorporateActionAdjustment,
    admitted_index: usize,
    record: &CorporateActionRecord,
    steps: &mut Vec<AdjustmentStep>,
    conflicts: &mut Vec<AdjustmentConflict>,
) {
    if adjustment == CorporateActionAdjustment::Raw {
        return;
    }
    let action = record.observation.action();
    if let CorporateActionKind::Split {
        numerator,
        denominator,
    } = action
    {
        steps.push(AdjustmentStep::Split {
            admitted_index,
            price_factor: AdjustmentRatio::new(*denominator, *numerator),
            quantity_factor: AdjustmentRatio::new(*numerator, *denominator),
        });
        return;
    }
    if adjustment == CorporateActionAdjustment::SplitAdjusted {
        return;
    }
    match action {
        CorporateActionKind::CashDividend { amount } => {
            steps.push(AdjustmentStep::CashDividend {
                admitted_index,
                amount: *amount,
            });
        }
        CorporateActionKind::ReturnOfCapital { amount } => {
            steps.push(AdjustmentStep::ReturnOfCapital {
                admitted_index,
                amount: *amount,
            });
        }
        CorporateActionKind::Spinoff {
            distributed_instrument,
            numerator,
            denominator,
        } => {
            steps.push(AdjustmentStep::Spinoff {
                admitted_index,
                distributed_instrument: *distributed_instrument,
                distribution_ratio: AdjustmentRatio::new(*numerator, *denominator),
            });
        }
        CorporateActionKind::Merger {
            successor,
            consideration: MergerConsideration::Unspecified,
        } => {
            conflicts.push(AdjustmentConflict::IncompleteMergerTerms {
                admitted_index,
                successor: *successor,
            });
        }
        CorporateActionKind::Merger {
            successor,
            consideration,
        } => {
            steps.push(AdjustmentStep::Merger {
                admitted_index,
                successor: *successor,
                consideration: *consideration,
            });
        }
        CorporateActionKind::Delisting => {
            steps.push(AdjustmentStep::Delisting { admitted_index });
        }
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => {
            steps.push(AdjustmentStep::SymbolChange {
                admitted_index,
                venue_id: venue_id.clone(),
                previous: previous.clone(),
                current: current.clone(),
            });
        }
        CorporateActionKind::Split { .. } => {}
    }
}
