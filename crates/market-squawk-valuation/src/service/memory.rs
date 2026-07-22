//! Conservative retained-memory accounting for recovered and incremental state.

use super::*;

pub(super) fn recovered_retained_bytes(
    state: &persistence::RecoveredState,
    audit: &[Arc<FairValueAuditEvent>],
    record_ids: usize,
    operation_ids: usize,
) -> Result<usize, FairValueError> {
    let mut total = size_of::<FairValueService<'static>>();
    for value in state.measurements.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in state.decisions.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in state.overrides.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in state.approvals.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in state.revocations.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in state.market_access.values() {
        total = checked_add(total, value.retained_bytes())?;
    }
    for value in audit {
        total = checked_add(total, value.retained_bytes)?;
    }
    let domain_entries = state
        .measurements
        .len()
        .checked_add(state.decisions.len())
        .and_then(|value| value.checked_add(state.overrides.len()))
        .and_then(|value| value.checked_add(state.approvals.len()))
        .and_then(|value| value.checked_add(state.revocations.len()))
        .and_then(|value| value.checked_add(state.market_access.len()))
        .ok_or(FairValueError::Arithmetic)?;
    total = checked_add(
        total,
        checked_mul(domain_entries, DOMAIN_INDEX_ENTRY_OVERHEAD_BYTES)?,
    )?;
    total = checked_add(
        total,
        checked_mul(
            checked_add(record_ids, operation_ids)?,
            IDENTITY_INDEX_ENTRY_OVERHEAD_BYTES,
        )?,
    )?;
    total = checked_add(
        total,
        checked_mul(audit.len(), AUDIT_INDEX_ENTRY_OVERHEAD_BYTES)?,
    )?;
    Ok(total)
}

pub(super) fn incremental_index_bytes(
    domain_entries: usize,
    new_record_ids: usize,
) -> Result<usize, FairValueError> {
    checked_add(
        checked_mul(domain_entries, DOMAIN_INDEX_ENTRY_OVERHEAD_BYTES)?,
        checked_add(
            checked_mul(
                checked_add(new_record_ids, 1)?,
                IDENTITY_INDEX_ENTRY_OVERHEAD_BYTES,
            )?,
            AUDIT_INDEX_ENTRY_OVERHEAD_BYTES,
        )?,
    )
}

pub(super) fn checked_mul(left: usize, right: usize) -> Result<usize, FairValueError> {
    left.checked_mul(right).ok_or(FairValueError::Arithmetic)
}
