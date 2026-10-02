//! Pure deterministic point-in-time admission, revision selection, and audit construction.

use std::cmp::Ordering;
use std::mem::size_of;
use std::time::Instant;

use market_squawk_domain::{
    AvailabilityEvidence, ResearchObservation, ResearchTemporalCoordinate, Timestamp,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::canonical::{evidence_identity, family_encoding, payload_identity, provenance_identity};
use super::model::observation_context;
use super::retained::{OperationControl, RetainedBudget, checked_add, reserve_exact};
use super::{
    PointInTimeCandidate, PointInTimeConflict, PointInTimeConflictCounts,
    PointInTimeConflictReport, PointInTimeError, PointInTimeExclusion, PointInTimeExclusionCounts,
    PointInTimeExclusionReason, PointInTimeExclusionReasons, PointInTimeRecord, PointInTimeRequest,
    PointInTimeRevisionCounts, PointInTimeRevisionMode, PointInTimeRevisionState,
    PointInTimeSelection,
};
use crate::Sha256Digest;

#[path = "select/identity.rs"]
mod identity;

struct PreparedCandidate<'a> {
    candidate: &'a PointInTimeCandidate,
    family_key: Vec<u8>,
    family_identity: Sha256Digest,
    payload_identity: Sha256Digest,
    provenance_identity: Sha256Digest,
    evidence_identity: Sha256Digest,
    revision: u32,
    initial_reasons: PointInTimeExclusionReasons,
    revision_state: PointInTimeRevisionState,
}

impl<'a> PreparedCandidate<'a> {
    const fn record(&self) -> PointInTimeRecord<'a> {
        PointInTimeRecord {
            candidate: self.candidate,
            family_identity: self.family_identity,
            payload_identity: self.payload_identity,
            provenance_identity: self.provenance_identity,
            evidence_identity: self.evidence_identity,
            revision_state: self.revision_state,
        }
    }
}

#[derive(Clone, Copy)]
enum Decision {
    Pending,
    Selected(PointInTimeRevisionState),
    Excluded(PointInTimeExclusionReasons),
    Conflict,
}

#[derive(Clone, Copy)]
struct ConflictSpan {
    start: usize,
    end: usize,
    payload_variants: usize,
}

pub(super) fn select<'a>(
    request: &PointInTimeRequest,
    candidates: &'a [PointInTimeCandidate],
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<PointInTimeSelection<'a>, PointInTimeError<'a>> {
    let limits = request.limits();
    if candidates.len() > limits.max_candidates() {
        return Err(PointInTimeError::CandidateLimitExceeded {
            limit: limits.max_candidates(),
            observed: candidates.len(),
        });
    }
    let mut control = OperationControl::new(cancellation, deadline)?;
    let mut budget = RetainedBudget::new(limits.max_retained_bytes());
    let mut prepared = Vec::new();
    reserve_exact(&mut prepared, candidates.len(), &mut budget)?;
    for candidate in candidates {
        control.observe()?;
        let family = family_encoding(candidate, &mut control, &mut budget)?;
        let payload = payload_identity(candidate, &mut control)?;
        let provenance = provenance_identity(candidate, &mut control)?;
        let evidence = evidence_identity(
            candidate,
            family.identity,
            payload,
            provenance,
            &mut control,
        )?;
        let temporal = TemporalAdmission::from_observation(candidate.observation());
        let (initial_reasons, revision_state) = admission(request, &temporal);
        prepared.push(PreparedCandidate {
            candidate,
            family_key: family.bytes,
            family_identity: family.identity,
            payload_identity: payload,
            provenance_identity: provenance,
            evidence_identity: evidence,
            revision: candidate.revision().get(),
            initial_reasons,
            revision_state,
        });
    }

    let (order, _sort_scratch) = cancellable_order(&prepared, &mut budget, &mut control)?;
    let family_count = count_families(&prepared, &order, &mut control)?;
    if family_count > limits.max_families() {
        return Err(PointInTimeError::FamilyLimitExceeded {
            limit: limits.max_families(),
            observed: family_count,
        });
    }

    let (conflict_count, conflicting_candidates, payload_variants) =
        count_conflicts(&prepared, &order, &mut control)?;
    if conflict_count > limits.max_conflicts() {
        return Err(PointInTimeError::ConflictLimitExceeded {
            limit: limits.max_conflicts(),
            observed: conflict_count,
        });
    }

    let mut decisions = Vec::new();
    reserve_exact(&mut decisions, prepared.len(), &mut budget)?;
    for candidate in &prepared {
        control.observe()?;
        decisions.push(if candidate.initial_reasons.is_empty() {
            Decision::Pending
        } else {
            Decision::Excluded(candidate.initial_reasons)
        });
    }
    assign_revision_decisions(request, &prepared, &order, &mut decisions, &mut control)?;

    let mut selected_count = 0;
    let mut excluded_count = 0;
    for decision in &decisions {
        control.observe()?;
        match decision {
            Decision::Selected(_) => selected_count = checked_add(selected_count, 1)?,
            Decision::Excluded(_) => excluded_count = checked_add(excluded_count, 1)?,
            Decision::Pending | Decision::Conflict => {}
        }
    }
    if selected_count > limits.max_result_rows() {
        return Err(PointInTimeError::ResultRowLimitExceeded {
            limit: limits.max_result_rows(),
            observed: selected_count,
        });
    }
    let mut records = Vec::new();
    reserve_exact(&mut records, selected_count, &mut budget)?;
    let mut exclusions = Vec::new();
    reserve_exact(&mut exclusions, excluded_count, &mut budget)?;
    let mut exclusion_counts = PointInTimeExclusionCounts::default();
    let mut revision_counts = PointInTimeRevisionCounts::default();
    for index in &order {
        control.observe()?;
        let prepared = &prepared[*index];
        match decisions[*index] {
            Decision::Selected(state) => {
                revision_counts.record(state);
                records.push(prepared.record());
            }
            Decision::Excluded(reasons) => {
                exclusion_counts.record(reasons);
                exclusions.push(PointInTimeExclusion {
                    record: prepared.record(),
                    reasons,
                });
            }
            Decision::Conflict | Decision::Pending => {}
        }
    }

    let conflict_counts = PointInTimeConflictCounts {
        conflicting_groups: conflict_count,
        conflicting_candidates,
        payload_variants,
    };
    if conflict_count > 0 {
        let conflicts =
            materialize_conflicts(&prepared, &order, conflict_count, &mut budget, &mut control)?;
        let audit_identity = identity::audit_identity(
            request,
            &prepared,
            &order,
            &decisions,
            exclusion_counts,
            revision_counts,
            conflict_counts,
            &mut control,
        )?;
        budget.charge(size_of::<PointInTimeConflictReport<'a>>())?;
        let retained_bytes = budget.peak();
        return Err(PointInTimeError::RevisionConflicts {
            report: Box::new(PointInTimeConflictReport {
                conflicts,
                conflict_counts,
                exclusions,
                exclusion_counts,
                audit_identity,
                retained_bytes,
            }),
        });
    }

    let content_identity = identity::content_identity(request, &records, &mut control)?;
    let audit_identity = identity::audit_identity(
        request,
        &prepared,
        &order,
        &decisions,
        exclusion_counts,
        revision_counts,
        conflict_counts,
        &mut control,
    )?;
    control.check_now()?;
    Ok(PointInTimeSelection {
        records,
        exclusions,
        exclusion_counts,
        revision_counts,
        content_identity,
        audit_identity,
        retained_bytes: budget.peak(),
    })
}

/// Cutoff-independent temporal inputs derived from an already admitted observation.
/// Payload and provenance identities still cover the complete original evidence.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TemporalAdmission {
    availability: AdmissionAvailability,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    superseded: Option<ResearchTemporalCoordinate>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
enum AdmissionAvailability {
    Known(Timestamp),
    Inferred,
    Unknown,
}

impl TemporalAdmission {
    pub(super) fn from_observation(observation: &ResearchObservation) -> Self {
        let context = observation_context(observation);
        let availability = match context.provenance().availability() {
            AvailabilityEvidence::Evidenced { available_at, .. }
            | AvailabilityEvidence::LocalFirstObserved {
                observed_at: available_at,
            } => AdmissionAvailability::Known(*available_at),
            AvailabilityEvidence::Inferred { .. } => AdmissionAvailability::Inferred,
            AvailabilityEvidence::Unknown => AdmissionAvailability::Unknown,
        };
        Self {
            availability,
            effective: context.time().effective().clone(),
            published: context.time().published().cloned(),
            superseded: context.time().superseded().cloned(),
        }
    }
}

pub(super) fn admission(
    request: &PointInTimeRequest,
    temporal: &TemporalAdmission,
) -> (PointInTimeExclusionReasons, PointInTimeRevisionState) {
    let mut reasons = PointInTimeExclusionReasons::default();
    match temporal.availability {
        AdmissionAvailability::Known(available_at) if available_at > request.as_of() => {
            reasons.insert(PointInTimeExclusionReason::AvailabilityAfterAsOf);
        }
        AdmissionAvailability::Known(_) => {}
        AdmissionAvailability::Inferred => {
            reasons.insert(PointInTimeExclusionReason::InferredAvailability);
        }
        AdmissionAvailability::Unknown => {
            reasons.insert(PointInTimeExclusionReason::UnknownAvailability);
        }
    }
    if let Some(published) = &temporal.published {
        publication_reasons(request, published, &mut reasons);
    }
    effective_reasons(request, &temporal.effective, &mut reasons);
    let revision_state = revision_state(request, temporal.superseded.as_ref());
    if request.policy().revision_mode() == PointInTimeRevisionMode::LatestKnown {
        match revision_state {
            PointInTimeRevisionState::Superseded => {
                reasons.insert(PointInTimeExclusionReason::SupersededByKnowledgeTime);
            }
            PointInTimeRevisionState::SupersessionIncomparable => {
                reasons.insert(PointInTimeExclusionReason::SupersessionIncomparable);
            }
            PointInTimeRevisionState::Current => {}
        }
    }
    (reasons, revision_state)
}

fn publication_reasons(
    request: &PointInTimeRequest,
    published: &ResearchTemporalCoordinate,
    reasons: &mut PointInTimeExclusionReasons,
) {
    if published
        .exact_timestamp()
        .is_some_and(|timestamp| timestamp > request.as_of())
    {
        reasons.insert(PointInTimeExclusionReason::PublicationAfterAsOf);
    }
    if let Some(cutoff) = request.publication_cutoff() {
        match published.partial_cmp(cutoff) {
            Some(Ordering::Greater) => {
                reasons.insert(PointInTimeExclusionReason::PublicationAfterCutoff);
            }
            Some(Ordering::Less | Ordering::Equal) => {}
            None => reasons.insert(PointInTimeExclusionReason::PublicationIncomparable),
        }
    }
}

fn effective_reasons(
    request: &PointInTimeRequest,
    effective: &ResearchTemporalCoordinate,
    reasons: &mut PointInTimeExclusionReasons,
) {
    match request.label_cutoff() {
        None => match effective.partial_cmp(request.effective_cutoff()) {
            Some(Ordering::Greater) => {
                reasons.insert(PointInTimeExclusionReason::EffectiveAfterCutoff);
            }
            Some(Ordering::Less | Ordering::Equal) => {}
            None => reasons.insert(PointInTimeExclusionReason::EffectiveIncomparable),
        },
        Some(label_cutoff) => {
            let lower = effective.partial_cmp(request.effective_cutoff());
            let upper = effective.partial_cmp(label_cutoff);
            if lower.is_none() || upper.is_none() {
                reasons.insert(PointInTimeExclusionReason::EffectiveIncomparable);
                return;
            }
            if matches!(lower, Some(Ordering::Less | Ordering::Equal)) {
                reasons.insert(PointInTimeExclusionReason::EffectiveNotAfterCutoff);
            }
            if matches!(upper, Some(Ordering::Greater)) {
                reasons.insert(PointInTimeExclusionReason::EffectiveAfterLabelCutoff);
            }
        }
    }
}

fn revision_state(
    request: &PointInTimeRequest,
    superseded: Option<&ResearchTemporalCoordinate>,
) -> PointInTimeRevisionState {
    let Some(superseded) = superseded else {
        return PointInTimeRevisionState::Current;
    };
    if let Some(timestamp) = superseded.exact_timestamp() {
        return if timestamp <= request.as_of() {
            PointInTimeRevisionState::Superseded
        } else {
            PointInTimeRevisionState::Current
        };
    }
    match request
        .publication_cutoff()
        .and_then(|cutoff| superseded.partial_cmp(cutoff))
    {
        Some(Ordering::Less | Ordering::Equal) => PointInTimeRevisionState::Superseded,
        Some(Ordering::Greater) => PointInTimeRevisionState::Current,
        None => PointInTimeRevisionState::SupersessionIncomparable,
    }
}

fn cancellable_order<'a>(
    prepared: &[PreparedCandidate<'a>],
    budget: &mut RetainedBudget,
    control: &mut OperationControl,
) -> Result<(Vec<usize>, Vec<usize>), PointInTimeError<'a>> {
    let mut order = Vec::new();
    reserve_exact(&mut order, prepared.len(), budget)?;
    order.extend(0..prepared.len());
    let mut scratch = Vec::new();
    reserve_exact(&mut scratch, prepared.len(), budget)?;
    scratch.resize(prepared.len(), 0);
    let mut width = 1_usize;
    while width < order.len() {
        let mut start = 0;
        while start < order.len() {
            let middle = start.saturating_add(width).min(order.len());
            let end = middle.saturating_add(width).min(order.len());
            merge(prepared, &order, &mut scratch, start, middle, end, control)?;
            start = end;
        }
        std::mem::swap(&mut order, &mut scratch);
        width = width
            .checked_mul(2)
            .ok_or(PointInTimeError::AccountingOverflow)?;
    }
    Ok((order, scratch))
}

fn merge<'a>(
    prepared: &[PreparedCandidate<'a>],
    input: &[usize],
    output: &mut [usize],
    start: usize,
    middle: usize,
    end: usize,
    control: &mut OperationControl,
) -> Result<(), PointInTimeError<'a>> {
    let (mut left, mut right) = (start, middle);
    for slot in &mut output[start..end] {
        control.observe()?;
        if right >= end
            || (left < middle
                && compare_prepared(&prepared[input[left]], &prepared[input[right]])
                    != Ordering::Greater)
        {
            *slot = input[left];
            left += 1;
        } else {
            *slot = input[right];
            right += 1;
        }
    }
    Ok(())
}

fn compare_prepared(left: &PreparedCandidate<'_>, right: &PreparedCandidate<'_>) -> Ordering {
    left.family_key
        .cmp(&right.family_key)
        .then_with(|| left.revision.cmp(&right.revision))
        .then_with(|| left.payload_identity.cmp(&right.payload_identity))
        .then_with(|| left.evidence_identity.cmp(&right.evidence_identity))
}

fn count_families<'a>(
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    control: &mut OperationControl,
) -> Result<usize, PointInTimeError<'a>> {
    let mut families = 0;
    let mut previous: Option<&[u8]> = None;
    for index in order {
        control.observe()?;
        let family = prepared[*index].family_key.as_slice();
        if previous != Some(family) {
            families = checked_add(families, 1)?;
            previous = Some(family);
        }
    }
    Ok(families)
}

fn count_conflicts<'a>(
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    control: &mut OperationControl,
) -> Result<(usize, usize, usize), PointInTimeError<'a>> {
    let mut groups = 0;
    let mut candidates = 0;
    let mut variants = 0;
    visit_revision_groups(
        prepared,
        order,
        control,
        |start, end, variant_count, control| {
            if variant_count > 1 {
                groups = checked_add(groups, 1)?;
                variants = checked_add(variants, variant_count)?;
                let mut eligible = 0;
                for index in &order[start..end] {
                    control.observe()?;
                    if prepared[*index].initial_reasons.is_empty() {
                        eligible = checked_add(eligible, 1)?;
                    }
                }
                candidates = checked_add(candidates, eligible)?;
            }
            Ok(())
        },
    )?;
    Ok((groups, candidates, variants))
}

fn assign_revision_decisions<'a>(
    request: &PointInTimeRequest,
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    decisions: &mut [Decision],
    control: &mut OperationControl,
) -> Result<(), PointInTimeError<'a>> {
    let mut family_start = 0;
    while family_start < order.len() {
        control.observe()?;
        let family_key = prepared[order[family_start]].family_key.as_slice();
        let mut family_end = family_start + 1;
        while family_end < order.len()
            && prepared[order[family_end]].family_key.as_slice() == family_key
        {
            control.observe()?;
            family_end += 1;
        }
        let winner = if request.policy().revision_mode() == PointInTimeRevisionMode::LatestKnown {
            highest_nonconflicting_revision(prepared, &order[family_start..family_end], control)?
        } else {
            None
        };
        let mut revision_start = family_start;
        while revision_start < family_end {
            let revision = prepared[order[revision_start]].revision;
            let mut revision_end = revision_start + 1;
            while revision_end < family_end && prepared[order[revision_end]].revision == revision {
                control.observe()?;
                revision_end += 1;
            }
            assign_revision_group(
                request,
                prepared,
                &order[revision_start..revision_end],
                winner,
                decisions,
                control,
            )?;
            revision_start = revision_end;
        }
        family_start = family_end;
    }
    Ok(())
}

fn highest_nonconflicting_revision<'a>(
    prepared: &[PreparedCandidate<'a>],
    family: &[usize],
    control: &mut OperationControl,
) -> Result<Option<u32>, PointInTimeError<'a>> {
    let mut highest = None;
    let mut start = 0;
    while start < family.len() {
        let revision = prepared[family[start]].revision;
        let mut end = start;
        while end < family.len() && prepared[family[end]].revision == revision {
            control.observe()?;
            end += 1;
        }
        let variants = payload_variants(prepared, &family[start..end], control)?;
        let mut eligible = false;
        for index in &family[start..end] {
            control.observe()?;
            eligible |= prepared[*index].initial_reasons.is_empty();
        }
        if variants <= 1 && eligible {
            highest = Some(revision);
        }
        start = end;
    }
    Ok(highest)
}

fn assign_revision_group<'a>(
    request: &PointInTimeRequest,
    prepared: &[PreparedCandidate<'a>],
    group: &[usize],
    latest_winner: Option<u32>,
    decisions: &mut [Decision],
    control: &mut OperationControl,
) -> Result<(), PointInTimeError<'a>> {
    let variants = payload_variants(prepared, group, control)?;
    if variants > 1 {
        for index in group {
            control.observe()?;
            if prepared[*index].initial_reasons.is_empty() {
                decisions[*index] = Decision::Conflict;
            }
        }
        return Ok(());
    }
    let mut selected = false;
    for index in group {
        control.observe()?;
        if !prepared[*index].initial_reasons.is_empty() {
            continue;
        }
        let select_revision = request.policy().revision_mode() == PointInTimeRevisionMode::AllKnown
            || latest_winner == Some(prepared[*index].revision);
        if !select_revision {
            let mut reasons = PointInTimeExclusionReasons::default();
            reasons.insert(PointInTimeExclusionReason::LowerRevision);
            decisions[*index] = Decision::Excluded(reasons);
        } else if selected {
            let mut reasons = PointInTimeExclusionReasons::default();
            reasons.insert(PointInTimeExclusionReason::DuplicateRevision);
            decisions[*index] = Decision::Excluded(reasons);
        } else {
            decisions[*index] = Decision::Selected(prepared[*index].revision_state);
            selected = true;
        }
    }
    Ok(())
}

fn payload_variants<'a>(
    prepared: &[PreparedCandidate<'a>],
    group: &[usize],
    control: &mut OperationControl,
) -> Result<usize, PointInTimeError<'a>> {
    let mut count = 0;
    let mut previous = None;
    for index in group {
        control.observe()?;
        let candidate = &prepared[*index];
        if candidate.initial_reasons.is_empty() && previous != Some(candidate.payload_identity) {
            count = checked_add(count, 1)?;
            previous = Some(candidate.payload_identity);
        }
    }
    Ok(count)
}

fn visit_revision_groups<'a, F>(
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    control: &mut OperationControl,
    mut visit: F,
) -> Result<(), PointInTimeError<'a>>
where
    F: FnMut(usize, usize, usize, &mut OperationControl) -> Result<(), PointInTimeError<'a>>,
{
    let mut start = 0;
    while start < order.len() {
        control.observe()?;
        let family = prepared[order[start]].family_key.as_slice();
        let revision = prepared[order[start]].revision;
        let mut end = start + 1;
        while end < order.len()
            && prepared[order[end]].family_key.as_slice() == family
            && prepared[order[end]].revision == revision
        {
            control.observe()?;
            end += 1;
        }
        let variants = payload_variants(prepared, &order[start..end], control)?;
        visit(start, end, variants, control)?;
        start = end;
    }
    Ok(())
}

fn materialize_conflicts<'a>(
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    conflict_count: usize,
    budget: &mut RetainedBudget,
    control: &mut OperationControl,
) -> Result<Vec<PointInTimeConflict<'a>>, PointInTimeError<'a>> {
    let mut conflicts = Vec::new();
    reserve_exact(&mut conflicts, conflict_count, budget)?;
    let mut spans = Vec::new();
    reserve_exact(&mut spans, conflict_count, budget)?;
    visit_revision_groups(
        prepared,
        order,
        control,
        |start, end, variants, _control| {
            if variants > 1 {
                spans.push(ConflictSpan {
                    start,
                    end,
                    payload_variants: variants,
                });
            }
            Ok(())
        },
    )?;
    for span in spans {
        control.observe()?;
        let mut eligible = 0;
        for index in &order[span.start..span.end] {
            control.observe()?;
            if prepared[*index].initial_reasons.is_empty() {
                eligible = checked_add(eligible, 1)?;
            }
        }
        let mut records = Vec::new();
        reserve_exact(&mut records, eligible, budget)?;
        for index in &order[span.start..span.end] {
            control.observe()?;
            if prepared[*index].initial_reasons.is_empty() {
                records.push(prepared[*index].record());
            }
        }
        let first = records.first().ok_or(PointInTimeError::CanonicalEncoding)?;
        debug_assert!(span.payload_variants > 1);
        conflicts.push(PointInTimeConflict {
            family_identity: first.family_identity(),
            revision: first.candidate().revision(),
            records,
        });
    }
    Ok(conflicts)
}

#[cfg(test)]
mod disk_regression {
    use std::error::Error;
    use std::num::{NonZeroU16, NonZeroU32};
    use std::time::{Duration, Instant};

    use market_squawk_domain::{
        AvailabilityEvidence, CalendarDate, DataQuality, DigestAlgorithm, MacroObservation,
        PayloadHash, PayloadReference, ResearchContext, ResearchObservation, ResearchPeriod,
        ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
        RevisionNumber, SourceId, SourceIdentifier, Timestamp,
    };
    use rust_decimal::Decimal;
    use tokio_util::sync::CancellationToken;

    use crate::pit::{
        PointInTimeCandidate, PointInTimeError, PointInTimeLimits, PointInTimePolicy,
        PointInTimeRequest, PointInTimeRevisionMode, disk::CandidateStore,
    };
    use crate::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};

    #[test]
    fn disk_batches_preserve_complete_revision_and_conflict_audits() -> Result<(), Box<dyn Error>> {
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("pit-disk-regression")?,
            1,
            DatasetSchemaRegistry::local().canonical_research_observations()?,
            Sha256Digest::new([1; 32]),
        )?;
        let make_temporal = |revision,
                             value,
                             availability,
                             effective,
                             published,
                             superseded|
         -> Result<PointInTimeCandidate, Box<dyn Error>> {
            let context = ResearchContext::new(
                ResearchProvenance::try_new(ResearchProvenanceInput {
                    source_id: SourceId::try_from("pit-disk")?,
                    instrument_id: None,
                    venue_id: None,
                    source_identifier: SourceIdentifier::try_from("same-natural-family")?,
                    source_timestamp: None,
                    received_at: Timestamp::from_unix_nanos(1000),
                    ingested_at: Timestamp::from_unix_nanos(1001),
                    quality: DataQuality::OfficialDelayed,
                    payload_reference: PayloadReference::ContentHash(PayloadHash::new(
                        DigestAlgorithm::Sha256,
                        [7; 32],
                    )),
                    availability,
                })?,
                ResearchTime::try_new_with_coordinates(
                    effective,
                    published,
                    RevisionNumber::new(revision)?,
                    superseded,
                )?,
            )?;
            Ok(PointInTimeCandidate::new(
                ResearchObservation::Macro(MacroObservation::new(
                    context,
                    SourceIdentifier::try_from("series")?,
                    Decimal::new(value, 0),
                    SourceIdentifier::try_from("index")?,
                )),
                manifest.clone(),
            ))
        };
        let exact = |value| ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(value));
        let make = |revision, value, available| {
            make_temporal(
                revision,
                value,
                AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(available)),
                exact(10),
                None,
                None,
            )
        };
        let cancellation = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        // A duplicate and a lower revision cross append boundaries. A future conflicting
        // payload must remain an exclusion and must not poison the eligible revision group.
        let mut candidates = vec![
            make(2, 20, 20)?,
            make(1, 10, 20)?,
            make(2, 20, 20)?,
            make(2, 99, 200)?,
        ];
        let mut disk =
            CandidateStore::for_test(1024 * 1024, 32 * 1024 * 1024, &cancellation, deadline)?;
        for candidate in &candidates {
            disk.append(
                vec![candidate.observation().clone()],
                candidate.source_manifest(),
            )?;
        }
        let limits = PointInTimeLimits::try_new(100, 100, 100, 100, 1024 * 1024)?;
        for mode in [
            PointInTimeRevisionMode::LatestKnown,
            PointInTimeRevisionMode::AllKnown,
        ] {
            let request = PointInTimeRequest::try_new(
                PointInTimePolicy::try_new(NonZeroU32::MIN, mode)?,
                Timestamp::from_unix_nanos(100),
                None,
                ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(50)),
                None,
                limits,
            )?;
            let expected = super::select(&request, &candidates, &cancellation, deadline)
                .map_err(|error| format!("{error:?}"))?;
            // The consumer may retain no selected rows without changing the complete audit.
            let filtered = disk.select(&request, |_| false)?;
            assert!(filtered.records().is_empty());
            assert_eq!(filtered.content_identity(), expected.content_identity());
            assert_eq!(filtered.audit_identity(), expected.audit_identity());
            let decisions = disk.select_decisions(&request)?;
            assert!(decisions.records().is_empty());
            assert_eq!(decisions.content_identity(), expected.content_identity());
            assert_eq!(decisions.audit_identity(), expected.audit_identity());
            assert_eq!(decisions.exclusion_counts(), expected.exclusion_counts());
            assert_eq!(decisions.revision_counts(), expected.revision_counts());
            let complete = disk.select(&request, |_| true)?;
            assert_eq!(complete.records().len(), expected.records().len());
            for (actual, expected) in complete.records().iter().zip(expected.records()) {
                assert_eq!(actual.candidate(), expected.candidate());
                assert_eq!(actual.evidence_identity(), expected.evidence_identity());
            }
        }
        // Metadata survives repeated selection at different cutoffs without promoting
        // inferred availability or collapsing calendar/provider precision into timestamps.
        let date = ResearchTemporalCoordinate::calendar_date(CalendarDate::new(1970, 1, 1)?);
        let period = ResearchTemporalCoordinate::source_period(ResearchPeriod::try_new(
            SourceIdentifier::try_from("monthly")?,
            2026,
            NonZeroU16::MIN,
            SourceIdentifier::try_from("M01")?,
        )?);
        let known = AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(20));
        let temporal_cases = [
            (AvailabilityEvidence::unknown(), exact(10), None, None),
            (
                AvailabilityEvidence::inferred(
                    Timestamp::from_unix_nanos(20),
                    SourceIdentifier::try_from("estimate")?,
                ),
                exact(10),
                None,
                None,
            ),
            (
                AvailabilityEvidence::evidenced(
                    Timestamp::from_unix_nanos(100),
                    SourceIdentifier::try_from("release")?,
                ),
                exact(50),
                None,
                None,
            ),
            (AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(200)), exact(10), Some(exact(200)), None),
            (known.clone(), exact(10), Some(exact(20)), Some(exact(100))),
            (known.clone(), exact(10), None, Some(exact(200))),
            (known.clone(), exact(10), Some(date.clone()), None),
            (known.clone(), exact(10), None, Some(date.clone())),
            (known.clone(), date.clone(), None, None),
            (known.clone(), period.clone(), None, None),
            (known.clone(), exact(10), Some(period.clone()), None),
            (known, exact(10), None, Some(period.clone())),
        ];
        let mut temporal_candidates = Vec::new();
        let mut temporal_disk =
            CandidateStore::for_test(1024 * 1024, 32 * 1024 * 1024, &cancellation, deadline)?;
        for (index, (available, effective, published, superseded)) in
            temporal_cases.into_iter().enumerate()
        {
            let candidate = make_temporal(
                u32::try_from(index)? + 1,
                10,
                available,
                effective,
                published,
                superseded,
            )?;
            temporal_disk.append(
                vec![candidate.observation().clone()],
                candidate.source_manifest(),
            )?;
            temporal_candidates.push(candidate);
        }
        for mode in [
            PointInTimeRevisionMode::LatestKnown,
            PointInTimeRevisionMode::AllKnown,
        ] {
            for (as_of, publication, effective, label) in [
                (100, None, exact(50), None),
                (300, Some(exact(50)), exact(50), None),
                (100, Some(exact(100)), exact(10), Some(exact(50))),
                (100, Some(date.clone()), date.clone(), None),
                (100, Some(period.clone()), period.clone(), None),
            ] {
                let request = PointInTimeRequest::try_new(
                    PointInTimePolicy::try_new(NonZeroU32::MIN, mode)?,
                    Timestamp::from_unix_nanos(as_of),
                    publication,
                    effective,
                    label,
                    limits,
                )?;
                let expected =
                    super::select(&request, &temporal_candidates, &cancellation, deadline)
                        .map_err(|error| format!("{error:?}"))?;
                let decisions = temporal_disk.select_decisions(&request)?;
                assert!(decisions.records().is_empty());
                assert_eq!(decisions.content_identity(), expected.content_identity());
                assert_eq!(decisions.audit_identity(), expected.audit_identity());
                assert_eq!(decisions.exclusion_counts(), expected.exclusion_counts());
                assert_eq!(decisions.revision_counts(), expected.revision_counts());
                let complete = temporal_disk.select(&request, |_| true)?;
                assert_eq!(complete.records().len(), expected.records().len());
                assert_eq!(complete.content_identity(), expected.content_identity());
                assert_eq!(complete.audit_identity(), expected.audit_identity());
                for (actual, expected) in complete.records().iter().zip(expected.records()) {
                    assert_eq!(actual.candidate(), expected.candidate());
                    assert_eq!(actual.evidence_identity(), expected.evidence_identity());
                }
            }
        }
        // A new read-only connection reuses immutable candidates after the operation
        // staging owner is gone; all request decisions remain connection-local TEMP state.
        let prepared_owner = crate::parquet_store::OperationScratchDirectory::for_test()?;
        let prepared_path = prepared_owner.path().join("prepared.sqlite3");
        let destination = rusqlite::Connection::open(&prepared_path)?;
        temporal_disk.export_prepared(&destination)?;
        drop(destination);
        drop(temporal_disk);
        let immutable_bytes = std::fs::read(&prepared_path)?;
        for mode in [
            PointInTimeRevisionMode::LatestKnown,
            PointInTimeRevisionMode::AllKnown,
        ] {
            let connection = rusqlite::Connection::open_with_flags(
                &prepared_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            let mut prepared = CandidateStore::open_prepared(
                connection,
                vec![manifest.clone()],
                temporal_candidates.len(),
                1024 * 1024,
                32 * 1024 * 1024,
                &cancellation,
                deadline,
            )?;
            let request = PointInTimeRequest::try_new(
                PointInTimePolicy::try_new(NonZeroU32::MIN, mode)?,
                Timestamp::from_unix_nanos(100),
                None,
                exact(50),
                None,
                limits,
            )?;
            let admitted: Vec<_> = temporal_candidates
                .iter()
                .enumerate()
                .filter(|(ordinal, _)| *ordinal != 1 && *ordinal != 7)
                .map(|(_, candidate)| candidate.clone())
                .collect();
            let expected = super::select(&request, &admitted, &cancellation, deadline)
                .map_err(|error| format!("{error:?}"))?;
            let actual = prepared
                .select_decisions_filtered(&request, |ordinal| Ok(ordinal != 1 && ordinal != 7))?;
            assert!(actual.records().is_empty());
            assert_eq!(actual.content_identity(), expected.content_identity());
            assert_eq!(actual.audit_identity(), expected.audit_identity());
            assert_eq!(actual.exclusion_counts(), expected.exclusion_counts());
            assert_eq!(actual.revision_counts(), expected.revision_counts());
            let mut ordinals = Vec::new();
            prepared.visit_decisions(|ordinal, _, _| {
                ordinals.push(ordinal);
                Ok(())
            })?;
            ordinals.sort_unstable();
            assert_eq!(
                ordinals,
                (0..temporal_candidates.len())
                    .filter(|ordinal| *ordinal != 1 && *ordinal != 7)
                    .collect::<Vec<_>>()
            );
            let empty = super::select(&request, &[], &cancellation, deadline)
                .map_err(|error| format!("{error:?}"))?;
            let actual = prepared.select_decisions_filtered(&request, |_| Ok(false))?;
            assert_eq!(actual.content_identity(), empty.content_identity());
            assert_eq!(actual.audit_identity(), empty.audit_identity());
            let complete = super::select(&request, &temporal_candidates, &cancellation, deadline)
                .map_err(|error| format!("{error:?}"))?;
            let retained = prepared.select(&request, |_| true)?;
            assert_eq!(retained.records().len(), complete.records().len());
            assert_eq!(retained.content_identity(), complete.content_identity());
            assert_eq!(retained.audit_identity(), complete.audit_identity());
            for (actual, expected) in retained.records().iter().zip(complete.records()) {
                assert_eq!(actual.candidate(), expected.candidate());
                assert_eq!(actual.evidence_identity(), expected.evidence_identity());
            }
            // Candidate and family limits apply to this request's admitted source input.
            let restricted = PointInTimeRequest::try_new(
                request.policy(),
                request.as_of(),
                None,
                exact(50),
                None,
                PointInTimeLimits::try_new(1, 1, 1, 1, 1024 * 1024)?,
            )?;
            let expected = super::select(
                &restricted,
                &temporal_candidates[2..3],
                &cancellation,
                deadline,
            )
            .map_err(|error| format!("{error:?}"))?;
            let actual =
                prepared.select_decisions_filtered(&restricted, |ordinal| Ok(ordinal == 2))?;
            assert_eq!(actual.content_identity(), expected.content_identity());
            assert_eq!(actual.audit_identity(), expected.audit_identity());
            assert!(matches!(
                prepared.append(Vec::new(), &manifest),
                Err(PointInTimeError::ScratchStorage)
            ));
            drop(prepared);
            assert_eq!(std::fs::read(&prepared_path)?, immutable_bytes);
        }
        let conflict = make(2, 21, 20)?;
        disk.append(
            vec![conflict.observation().clone()],
            conflict.source_manifest(),
        )?;
        candidates.push(conflict);
        let request = PointInTimeRequest::try_new(
            PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)?,
            Timestamp::from_unix_nanos(100),
            None,
            ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(50)),
            None,
            limits,
        )?;
        let Err(PointInTimeError::RevisionConflicts { report }) =
            super::select(&request, &candidates, &cancellation, deadline)
        else {
            return Err("expected pure conflict".into());
        };
        let Err(PointInTimeError::DiskRevisionConflicts {
            counts,
            audit_identity,
        }) = disk.select(&request, |_| true)
        else {
            return Err("expected disk conflict".into());
        };
        assert_eq!(counts, report.conflict_counts());
        assert_eq!(audit_identity, report.audit_identity());
        let Err(PointInTimeError::DiskRevisionConflicts {
            counts,
            audit_identity,
        }) = disk.select_decisions(&request)
        else {
            return Err("expected decision-only disk conflict".into());
        };
        assert_eq!(counts, report.conflict_counts());
        assert_eq!(audit_identity, report.audit_identity());
        let conflict_path = prepared_owner.path().join("conflicts.sqlite3");
        let destination = rusqlite::Connection::open(&conflict_path)?;
        disk.export_prepared(&destination)?;
        drop(destination);
        let connection = rusqlite::Connection::open_with_flags(
            &conflict_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut prepared = CandidateStore::open_prepared(
            connection,
            vec![manifest.clone()],
            candidates.len(),
            1024 * 1024,
            32 * 1024 * 1024,
            &cancellation,
            deadline,
        )?;
        let Err(PointInTimeError::DiskRevisionConflicts {
            counts,
            audit_identity,
        }) = prepared.select_decisions(&request)
        else {
            return Err("expected reopened conflict".into());
        };
        assert_eq!(counts, report.conflict_counts());
        assert_eq!(audit_identity, report.audit_identity());
        let expected = super::select(
            &request,
            &candidates[..candidates.len() - 1],
            &cancellation,
            deadline,
        )
        .map_err(|error| format!("{error:?}"))?;
        let actual = prepared
            .select_decisions_filtered(&request, |ordinal| Ok(ordinal < candidates.len() - 1))?;
        assert_eq!(actual.content_identity(), expected.content_identity());
        assert_eq!(actual.audit_identity(), expected.audit_identity());
        cancellation.cancel();
        assert!(matches!(
            disk.select(&request, |_| true),
            Err(PointInTimeError::Cancelled)
        ));
        Ok(())
    }
}
