//! Checked retained-memory admission for historical-universe results.

use std::mem::size_of;

use market_squawk_domain::{AvailabilityEvidence, Timestamp};

use super::build::exclusion_reason;
use super::{
    UniverseConflictCounts, UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseId,
    UniverseMembership,
};

pub(super) fn try_reserve_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
) -> Result<(), UniverseError> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| UniverseError::AllocationFailed)
}

pub(super) fn into_conflict_evidence(
    memberships: Vec<UniverseMembership>,
    counts: UniverseConflictCounts,
    retained_limit: usize,
) -> Result<UniverseConflictEvidence, UniverseError> {
    let mut conflicting_instruments = Vec::new();
    try_reserve_exact(&mut conflicting_instruments, counts.conflicting_instruments)?;
    let mut start = 0;
    while start < memberships.len() {
        let instrument_id = memberships[start].instrument_id;
        let mut end = start + 1;
        while end < memberships.len() && memberships[end].instrument_id == instrument_id {
            end += 1;
        }
        if end - start > 1 {
            conflicting_instruments.push(instrument_id);
        }
        start = end;
    }

    let mut evidence = Vec::new();
    try_reserve_exact(&mut evidence, counts.conflicting_memberships)?;
    for membership in memberships {
        if conflicting_instruments
            .binary_search(&membership.instrument_id)
            .is_ok()
        {
            evidence.push(membership);
        }
    }
    let retained_bytes = checked_membership_vector_bytes(evidence.capacity(), &evidence)?;
    require_retained_limit(retained_bytes, retained_limit)?;
    Ok(UniverseConflictEvidence {
        memberships: evidence,
        retained_bytes,
    })
}

pub(super) fn checked_snapshot_minimum_retained_bytes(
    universe_id: &UniverseId,
    as_of: Timestamp,
    candidates: &[UniverseMembership],
) -> Result<usize, UniverseError> {
    let mut retained = universe_id.as_str().len();
    for membership in candidates {
        let inline_bytes = if exclusion_reason(membership, as_of).is_some() {
            size_of::<UniverseExclusion>()
        } else {
            size_of::<UniverseMembership>()
        };
        let dynamic_bytes = checked_membership_dynamic_bytes(membership)?;
        retained = retained
            .checked_add(inline_bytes)
            .and_then(|value| value.checked_add(dynamic_bytes))
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    Ok(retained)
}

pub(super) fn checked_snapshot_retained_bytes(
    universe_id: &UniverseId,
    membership_capacity: usize,
    memberships: &[UniverseMembership],
    exclusion_capacity: usize,
    exclusions: &[UniverseExclusion],
) -> Result<usize, UniverseError> {
    let membership_capacity_bytes = membership_capacity
        .checked_mul(size_of::<UniverseMembership>())
        .ok_or(UniverseError::RetainedSizeOverflow)?;
    let exclusion_capacity_bytes = exclusion_capacity
        .checked_mul(size_of::<UniverseExclusion>())
        .ok_or(UniverseError::RetainedSizeOverflow)?;
    let mut retained = universe_id
        .as_str()
        .len()
        .checked_add(membership_capacity_bytes)
        .and_then(|value| value.checked_add(exclusion_capacity_bytes))
        .ok_or(UniverseError::RetainedSizeOverflow)?;
    for membership in memberships {
        retained = retained
            .checked_add(checked_membership_dynamic_bytes(membership)?)
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    for exclusion in exclusions {
        retained = retained
            .checked_add(checked_membership_dynamic_bytes(&exclusion.membership)?)
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    Ok(retained)
}

pub(super) fn require_retained_limit(required: usize, limit: usize) -> Result<(), UniverseError> {
    if required > limit {
        Err(UniverseError::RetainedByteLimitExceeded { limit, required })
    } else {
        Ok(())
    }
}

fn checked_membership_vector_bytes(
    capacity: usize,
    memberships: &[UniverseMembership],
) -> Result<usize, UniverseError> {
    let mut retained = capacity
        .checked_mul(size_of::<UniverseMembership>())
        .ok_or(UniverseError::RetainedSizeOverflow)?;
    for membership in memberships {
        retained = retained
            .checked_add(checked_membership_dynamic_bytes(membership)?)
            .ok_or(UniverseError::RetainedSizeOverflow)?;
    }
    Ok(retained)
}

fn checked_membership_dynamic_bytes(
    membership: &UniverseMembership,
) -> Result<usize, UniverseError> {
    let availability_bytes = match &membership.availability {
        AvailabilityEvidence::Evidenced { evidence, .. } => evidence.retained_bytes(),
        AvailabilityEvidence::Inferred { method, .. } => method.retained_bytes(),
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => 0,
    };
    membership
        .source_manifest
        .dataset_id()
        .as_str()
        .len()
        .checked_add(membership.source_manifest.schema().name().len())
        .and_then(|value| value.checked_add(availability_bytes))
        .ok_or(UniverseError::RetainedSizeOverflow)
}
