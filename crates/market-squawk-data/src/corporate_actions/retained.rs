//! Checked work and retained-memory admission for corporate-action plans.

use std::mem::size_of;

use market_squawk_domain::{
    AvailabilityEvidence, CorporateActionKind, MergerConsideration, PayloadReference,
    ResearchTemporalPrecision,
};

use super::plan::exclusion_reason;
use super::{
    AdjustmentConflict, AdjustmentStep, CorporateActionAdjustment, CorporateActionError,
    CorporateActionExclusion, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PlanShape {
    pub(super) admitted: usize,
    pub(super) exclusions: usize,
    pub(super) steps: usize,
    pub(super) conflicts: usize,
    pub(super) minimum_retained_bytes: usize,
}

pub(super) fn plan_shape(
    policy: CorporateActionPolicy,
    knowledge_cutoff: market_squawk_domain::Timestamp,
    valuation_cutoff: market_squawk_domain::Timestamp,
    records: &[CorporateActionRecord],
) -> Result<PlanShape, CorporateActionError> {
    let mut admitted = 0_usize;
    let mut exclusions = 0_usize;
    let mut steps = 0_usize;
    let mut conflicts = 0_usize;
    let mut dynamic = 0_usize;
    for record in records {
        dynamic = checked_add(dynamic, record_dynamic_bytes(record)?)?;
        if exclusion_reason(record, knowledge_cutoff, valuation_cutoff).is_some() {
            exclusions = checked_add(exclusions, 1)?;
        } else {
            admitted = checked_add(admitted, 1)?;
            let (record_steps, record_conflicts) = output_counts(policy.adjustment(), record);
            steps = checked_add(steps, record_steps)?;
            conflicts = checked_add(conflicts, record_conflicts)?;
            if record_steps != 0 {
                dynamic = checked_add(dynamic, step_dynamic_bytes(record)?)?;
            }
        }
    }
    let mut minimum_retained_bytes = size_of::<CorporateActionPlan>();
    minimum_retained_bytes = checked_add(
        minimum_retained_bytes,
        vector_bytes::<CorporateActionRecord>(admitted)?,
    )?;
    minimum_retained_bytes = checked_add(
        minimum_retained_bytes,
        vector_bytes::<CorporateActionExclusion>(exclusions)?,
    )?;
    minimum_retained_bytes = checked_add(
        minimum_retained_bytes,
        vector_bytes::<AdjustmentStep>(steps)?,
    )?;
    minimum_retained_bytes = checked_add(
        minimum_retained_bytes,
        vector_bytes::<AdjustmentConflict>(conflicts)?,
    )?;
    minimum_retained_bytes = checked_add(minimum_retained_bytes, dynamic)?;
    Ok(PlanShape {
        admitted,
        exclusions,
        steps,
        conflicts,
        minimum_retained_bytes,
    })
}

pub(super) fn checked_plan_retained_bytes(
    admitted_capacity: usize,
    admitted: &[CorporateActionRecord],
    exclusion_capacity: usize,
    exclusions: &[CorporateActionExclusion],
    step_capacity: usize,
    steps: &[AdjustmentStep],
    conflict_capacity: usize,
) -> Result<usize, CorporateActionError> {
    let mut retained = size_of::<CorporateActionPlan>();
    retained = checked_add(
        retained,
        vector_bytes::<CorporateActionRecord>(admitted_capacity)?,
    )?;
    retained = checked_add(
        retained,
        vector_bytes::<CorporateActionExclusion>(exclusion_capacity)?,
    )?;
    retained = checked_add(retained, vector_bytes::<AdjustmentStep>(step_capacity)?)?;
    retained = checked_add(
        retained,
        vector_bytes::<AdjustmentConflict>(conflict_capacity)?,
    )?;
    for record in admitted {
        retained = checked_add(retained, record_dynamic_bytes(record)?)?;
    }
    for exclusion in exclusions {
        retained = checked_add(retained, record_dynamic_bytes(&exclusion.record)?)?;
    }
    for step in steps {
        retained = checked_add(retained, retained_step_dynamic_bytes(step)?)?;
    }
    Ok(retained)
}

pub(super) fn require_retained_limit(
    required: usize,
    limit: usize,
) -> Result<(), CorporateActionError> {
    if required > limit {
        Err(CorporateActionError::RetainedByteLimitExceeded { limit, required })
    } else {
        Ok(())
    }
}

pub(super) fn try_reserve_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
) -> Result<(), CorporateActionError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| CorporateActionError::AllocationFailed)
}

fn output_counts(
    adjustment: CorporateActionAdjustment,
    record: &CorporateActionRecord,
) -> (usize, usize) {
    match (adjustment, record.observation.action()) {
        (CorporateActionAdjustment::Raw, _) => (0, 0),
        (
            CorporateActionAdjustment::SplitAdjusted | CorporateActionAdjustment::TotalReturn,
            CorporateActionKind::Split { .. },
        ) => (1, 0),
        (CorporateActionAdjustment::SplitAdjusted, _) => (0, 0),
        (
            CorporateActionAdjustment::TotalReturn,
            CorporateActionKind::Merger {
                consideration: MergerConsideration::Unspecified,
                ..
            },
        ) => (0, 1),
        (CorporateActionAdjustment::TotalReturn, _) => (1, 0),
    }
}

fn record_dynamic_bytes(record: &CorporateActionRecord) -> Result<usize, CorporateActionError> {
    let context = record.observation.context();
    let provenance = context.provenance();
    let mut retained = provenance.source_id().retained_bytes();
    retained = checked_add(retained, provenance.source_identifier().retained_bytes())?;
    if let Some(venue) = provenance.venue_id() {
        retained = checked_add(retained, venue.retained_bytes())?;
    }
    if let PayloadReference::SourceReference(reference) = provenance.payload_reference() {
        retained = checked_add(retained, reference.retained_bytes())?;
    }
    retained = match provenance.availability() {
        AvailabilityEvidence::Evidenced { evidence, .. } => {
            checked_add(retained, evidence.retained_bytes())?
        }
        AvailabilityEvidence::Inferred { method, .. } => {
            checked_add(retained, method.retained_bytes())?
        }
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => retained,
    };
    retained = checked_add(
        retained,
        temporal_dynamic_bytes(context.time().effective())?,
    )?;
    if let Some(published) = context.time().published() {
        retained = checked_add(retained, temporal_dynamic_bytes(published)?)?;
    }
    if let Some(superseded) = context.time().superseded() {
        retained = checked_add(retained, temporal_dynamic_bytes(superseded)?)?;
    }
    retained = checked_add(retained, action_dynamic_bytes(record.observation.action())?)?;
    retained = checked_add(retained, record.source_manifest.dataset_id().as_str().len())?;
    checked_add(retained, record.source_manifest.schema().name().len())
}

fn temporal_dynamic_bytes(
    coordinate: &market_squawk_domain::ResearchTemporalCoordinate,
) -> Result<usize, CorporateActionError> {
    if coordinate.precision() != ResearchTemporalPrecision::SourcePeriod {
        return Ok(0);
    }
    let period = coordinate
        .source_period_value()
        .ok_or(CorporateActionError::RetainedSizeOverflow)?;
    checked_add(
        period.scheme().retained_bytes(),
        period.code().retained_bytes(),
    )
}

fn action_dynamic_bytes(action: &CorporateActionKind) -> Result<usize, CorporateActionError> {
    match action {
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => venue_id
            .retained_bytes()
            .checked_add(previous.retained_bytes())
            .and_then(|value| value.checked_add(current.retained_bytes()))
            .ok_or(CorporateActionError::RetainedSizeOverflow),
        CorporateActionKind::Split { .. }
        | CorporateActionKind::CashDividend { .. }
        | CorporateActionKind::Spinoff { .. }
        | CorporateActionKind::ReturnOfCapital { .. }
        | CorporateActionKind::Merger { .. }
        | CorporateActionKind::Delisting => Ok(0),
    }
}

fn step_dynamic_bytes(record: &CorporateActionRecord) -> Result<usize, CorporateActionError> {
    match record.observation.action() {
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => venue_id
            .retained_bytes()
            .checked_add(previous.as_str().len())
            .and_then(|value| value.checked_add(current.as_str().len()))
            .ok_or(CorporateActionError::RetainedSizeOverflow),
        _ => Ok(0),
    }
}

fn retained_step_dynamic_bytes(step: &AdjustmentStep) -> Result<usize, CorporateActionError> {
    match step {
        AdjustmentStep::SymbolChange {
            venue_id,
            previous,
            current,
            ..
        } => venue_id
            .retained_bytes()
            .checked_add(previous.retained_bytes())
            .and_then(|value| value.checked_add(current.retained_bytes()))
            .ok_or(CorporateActionError::RetainedSizeOverflow),
        _ => Ok(0),
    }
}

fn vector_bytes<T>(capacity: usize) -> Result<usize, CorporateActionError> {
    capacity
        .checked_mul(size_of::<T>())
        .ok_or(CorporateActionError::RetainedSizeOverflow)
}

fn checked_add(left: usize, right: usize) -> Result<usize, CorporateActionError> {
    left.checked_add(right)
        .ok_or(CorporateActionError::RetainedSizeOverflow)
}
