//! Fail-closed point-in-time selection and overlap rejection.

use market_squawk_domain::{AvailabilityEvidence, InstrumentId, Timestamp};

use super::canonical::{compare_memberships, snapshot_audit_hash, snapshot_content_hash};
use super::model::{
    UniverseConflictCounts, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
use super::retained::{
    checked_snapshot_minimum_retained_bytes, checked_snapshot_retained_bytes,
    into_conflict_evidence, require_retained_limit, try_reserve_exact,
};

impl UniverseSnapshot {
    /// Builds one bounded snapshot without consulting current instrument status.
    ///
    /// A candidate is admitted only when its half-open interval contains `as_of` and its
    /// availability is evidenced or locally first observed no later than `as_of`.
    ///
    /// # Errors
    ///
    /// Rejects invalid limits, excessive work or retained memory, allocation or encoding failure,
    /// and every set that admits multiple memberships for one stable instrument.
    pub fn try_build(
        universe_id: UniverseId,
        as_of: Timestamp,
        candidates: Vec<UniverseMembership>,
        limits: UniverseLimits,
    ) -> Result<Self, UniverseError> {
        if candidates.len() > limits.max_candidates {
            return Err(UniverseError::CandidateLimitExceeded {
                limit: limits.max_candidates,
                observed: candidates.len(),
            });
        }

        let (membership_count, exclusion_count) = candidates.iter().fold(
            (0_usize, 0_usize),
            |(memberships, exclusions), membership| {
                if exclusion_reason(membership, as_of).is_some() {
                    (memberships, exclusions + 1)
                } else {
                    (memberships + 1, exclusions)
                }
            },
        );
        let minimum_retained =
            checked_snapshot_minimum_retained_bytes(&universe_id, as_of, &candidates)?;
        require_retained_limit(minimum_retained, limits.max_retained_bytes)?;

        let mut memberships = Vec::new();
        try_reserve_exact(&mut memberships, membership_count)?;
        let mut exclusions = Vec::new();
        try_reserve_exact(&mut exclusions, exclusion_count)?;
        let mut exclusion_counts = UniverseExclusionCounts::default();
        for membership in candidates {
            if let Some(reason) = exclusion_reason(&membership, as_of) {
                exclusion_counts.record(reason);
                exclusions.push(UniverseExclusion { membership, reason });
            } else {
                memberships.push(membership);
            }
        }
        memberships.sort_by(compare_memberships);
        exclusions.sort_by(|left, right| {
            compare_memberships(&left.membership, &right.membership)
                .then_with(|| left.reason.cmp(&right.reason))
        });

        let (conflict_counts, first_conflict) = count_conflicts(&memberships)?;
        if let Some(first_instrument) = first_conflict {
            let conflict_evidence =
                into_conflict_evidence(memberships, conflict_counts, limits.max_retained_bytes)?;
            return Err(UniverseError::OverlappingAdmittedMemberships {
                first_instrument,
                conflicts: conflict_counts,
                conflict_evidence,
                exclusions: exclusion_counts,
            });
        }
        let content_hash = snapshot_content_hash(&universe_id, as_of, &memberships)?;
        let audit_hash = snapshot_audit_hash(
            &universe_id,
            as_of,
            &memberships,
            &exclusions,
            exclusion_counts,
        )?;
        let retained_bytes = checked_snapshot_retained_bytes(
            &universe_id,
            memberships.capacity(),
            &memberships,
            exclusions.capacity(),
            &exclusions,
        )?;
        require_retained_limit(retained_bytes, limits.max_retained_bytes)?;
        Ok(Self {
            universe_id,
            as_of,
            memberships,
            exclusions,
            exclusion_counts,
            conflict_counts,
            content_hash,
            audit_hash,
            retained_bytes,
        })
    }
}

pub(super) fn exclusion_reason(
    membership: &UniverseMembership,
    as_of: Timestamp,
) -> Option<UniverseExclusionReason> {
    let interval = membership.effective_interval;
    if as_of < interval.starts_at() || interval.ends_at().is_some_and(|end| as_of >= end) {
        return Some(UniverseExclusionReason::NotEffective);
    }
    match &membership.availability {
        AvailabilityEvidence::Evidenced { available_at, .. } if *available_at <= as_of => None,
        AvailabilityEvidence::LocalFirstObserved { observed_at } if *observed_at <= as_of => None,
        AvailabilityEvidence::Evidenced { .. }
        | AvailabilityEvidence::LocalFirstObserved { .. } => {
            Some(UniverseExclusionReason::FutureAvailability)
        }
        AvailabilityEvidence::Inferred { .. } => {
            Some(UniverseExclusionReason::InferredAvailability)
        }
        AvailabilityEvidence::Unknown => Some(UniverseExclusionReason::UnknownAvailability),
    }
}

fn count_conflicts(
    memberships: &[UniverseMembership],
) -> Result<(UniverseConflictCounts, Option<InstrumentId>), UniverseError> {
    let mut counts = UniverseConflictCounts::default();
    let mut first_conflict = None;
    let mut start = 0;
    while start < memberships.len() {
        let instrument_id = memberships[start].instrument_id;
        let mut end = start + 1;
        while end < memberships.len() && memberships[end].instrument_id == instrument_id {
            end += 1;
        }
        let group_len = end - start;
        if group_len > 1 {
            if first_conflict.is_none() {
                first_conflict = Some(instrument_id);
            }
            counts.conflicting_instruments += 1;
            counts.conflicting_memberships += group_len;
            let group_len =
                u64::try_from(group_len).map_err(|_| UniverseError::CanonicalEncodingOverflow)?;
            let pairs = group_len
                .checked_mul(group_len - 1)
                .and_then(|value| value.checked_div(2))
                .ok_or(UniverseError::CanonicalEncodingOverflow)?;
            counts.overlap_pairs = counts
                .overlap_pairs
                .checked_add(pairs)
                .ok_or(UniverseError::CanonicalEncodingOverflow)?;
        }
        start = end;
    }
    Ok((counts, first_conflict))
}
