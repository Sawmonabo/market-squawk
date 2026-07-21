//! Bounded single-source batches and input-aligned revision assignments.

use std::cmp::Ordering;
use std::mem::size_of;

use market_squawk_domain::{RevisionNumber, SourceId};

use super::{
    CanonicalObservationFamily, MAX_OBSERVED_REVISION_BATCH_BYTES,
    MAX_OBSERVED_REVISION_BATCH_RECORDS, ObservedProviderOrder, ObservedRevisionError,
    ObservedSemanticPayload, ObservedVersionEvidence, ObservedVersionKind,
};

/// One exact observed family/version/payload tuple awaiting durable revision assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedRevisionRecord {
    family: CanonicalObservationFamily,
    version: ObservedVersionEvidence,
    payload: ObservedSemanticPayload,
    provider_order: Option<ObservedProviderOrder>,
}

impl ObservedRevisionRecord {
    /// Binds exact family, version, semantic payload, and provider ordering when applicable.
    ///
    /// # Errors
    ///
    /// Rejects provider-supplied versions without explicit provider order, locally observed
    /// versions carrying provider order, and locally observed version evidence not derived from
    /// this exact semantic payload.
    pub fn try_new(
        family: CanonicalObservationFamily,
        version: ObservedVersionEvidence,
        payload: ObservedSemanticPayload,
        provider_order: Option<ObservedProviderOrder>,
    ) -> Result<Self, ObservedRevisionError> {
        match (version.kind(), provider_order.is_some()) {
            (ObservedVersionKind::ProviderSupplied, false)
            | (ObservedVersionKind::LocallyObservedContent, true) => {
                return Err(ObservedRevisionError::AmbiguousProviderOrder);
            }
            (ObservedVersionKind::ProviderSupplied, true)
            | (ObservedVersionKind::LocallyObservedContent, false) => {}
        }
        if version.kind() == ObservedVersionKind::LocallyObservedContent
            && version.exact_evidence() != payload.exact_evidence()
        {
            return Err(ObservedRevisionError::Conflict);
        }
        Ok(Self {
            family,
            version,
            payload,
            provider_order,
        })
    }

    /// Returns the exact canonical observation family.
    pub const fn family(&self) -> &CanonicalObservationFamily {
        &self.family
    }

    /// Returns exact version evidence.
    pub const fn version(&self) -> &ObservedVersionEvidence {
        &self.version
    }

    /// Returns exact canonical semantic payload evidence.
    pub const fn semantic_payload(&self) -> &ObservedSemanticPayload {
        &self.payload
    }

    /// Returns explicit provider ordering evidence when supplied.
    pub const fn provider_order(&self) -> Option<&ObservedProviderOrder> {
        self.provider_order.as_ref()
    }

    fn retained_bytes(&self) -> Result<usize, ObservedRevisionError> {
        let retained = self
            .family
            .retained_bytes()?
            .checked_add(self.version.exact_evidence().len())
            .and_then(|bytes| bytes.checked_add(self.payload.exact_evidence().len()))
            .ok_or(ObservedRevisionError::ByteCountOverflow)?;
        match &self.provider_order {
            Some(order) => retained
                .checked_add(order.retained_bytes()?)
                .ok_or(ObservedRevisionError::ByteCountOverflow),
            None => Ok(retained),
        }
    }
}

#[derive(Debug)]
struct CoalescedRecord {
    record: ObservedRevisionRecord,
    input_indexes: Vec<usize>,
}

/// Bounded, validated, single-source records prepared for one atomic durable assignment.
///
/// Exact duplicates are coalesced before authority I/O. [`Self::align_assignments`] expands one
/// assignment per unique record back to the caller's original input order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedRevisionBatch {
    source_id: SourceId,
    input_len: usize,
    unique_records: Box<[ObservedRevisionRecord]>,
    input_to_unique: Box<[usize]>,
    retained_bytes: usize,
}

impl ObservedRevisionBatch {
    /// Validates, bounds, deterministically orders, and coalesces one single-source batch.
    ///
    /// # Errors
    ///
    /// Rejects source transplants, count/deep-byte overflow, divergent same-version payloads,
    /// inconsistent same-version provider order, or multiple versions lacking a comparable unique
    /// provider order.
    pub fn try_new(
        source_id: SourceId,
        records: Vec<ObservedRevisionRecord>,
    ) -> Result<Self, ObservedRevisionError> {
        Self::try_new_with_limits(
            source_id,
            records,
            MAX_OBSERVED_REVISION_BATCH_RECORDS,
            MAX_OBSERVED_REVISION_BATCH_BYTES,
        )
    }

    fn try_new_with_limits(
        source_id: SourceId,
        records: Vec<ObservedRevisionRecord>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Self, ObservedRevisionError> {
        let input_len = records.len();
        if input_len > max_records {
            return Err(ObservedRevisionError::RecordLimitExceeded { max: max_records });
        }
        let retained_bytes = checked_batch_input_bytes(&source_id, &records, max_bytes)?;
        if retained_bytes > max_bytes {
            return Err(ObservedRevisionError::BatchByteLimitExceeded { max: max_bytes });
        }
        if records
            .iter()
            .any(|record| record.family.source_id() != &source_id)
        {
            return Err(ObservedRevisionError::SourceMismatch);
        }

        let mut indexed = Vec::new();
        indexed
            .try_reserve_exact(input_len)
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        indexed.extend(records.into_iter().enumerate());
        indexed.sort_unstable_by(|left, right| record_identity_cmp(&left.1, &right.1));

        let mut coalesced: Vec<CoalescedRecord> = Vec::new();
        coalesced
            .try_reserve_exact(input_len)
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        for (input_index, record) in indexed {
            if let Some(previous) = coalesced.last_mut()
                && same_exact_version(&previous.record, &record)
            {
                if previous.record.payload != record.payload
                    || previous.record.provider_order != record.provider_order
                {
                    return Err(ObservedRevisionError::Conflict);
                }
                previous
                    .input_indexes
                    .try_reserve(1)
                    .map_err(|_| ObservedRevisionError::AllocationFailure)?;
                previous.input_indexes.push(input_index);
                continue;
            }
            let mut input_indexes = Vec::new();
            input_indexes
                .try_reserve_exact(1)
                .map_err(|_| ObservedRevisionError::AllocationFailure)?;
            input_indexes.push(input_index);
            coalesced.push(CoalescedRecord {
                record,
                input_indexes,
            });
        }

        order_family_versions(&mut coalesced)?;

        let mut input_to_unique = Vec::new();
        input_to_unique
            .try_reserve_exact(input_len)
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        input_to_unique.resize(input_len, usize::MAX);
        let mut unique_records = Vec::new();
        unique_records
            .try_reserve_exact(coalesced.len())
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        for (unique_index, coalesced_record) in coalesced.into_iter().enumerate() {
            for input_index in coalesced_record.input_indexes {
                let destination = input_to_unique
                    .get_mut(input_index)
                    .ok_or(ObservedRevisionError::CorruptAuthorityState)?;
                *destination = unique_index;
            }
            unique_records.push(coalesced_record.record);
        }
        if input_to_unique.contains(&usize::MAX) {
            return Err(ObservedRevisionError::CorruptAuthorityState);
        }

        Ok(Self {
            source_id,
            input_len,
            unique_records: unique_records.into_boxed_slice(),
            input_to_unique: input_to_unique.into_boxed_slice(),
            retained_bytes,
        })
    }

    #[cfg(test)]
    pub(super) fn try_new_with_test_limits(
        source_id: SourceId,
        records: Vec<ObservedRevisionRecord>,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Self, ObservedRevisionError> {
        Self::try_new_with_limits(source_id, records, max_records, max_bytes)
    }

    /// Returns the exact source shared by every family in this batch.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the caller's original record count before exact duplicate coalescing.
    pub const fn input_len(&self) -> usize {
        self.input_len
    }

    /// Returns deterministically ordered unique records for durable authority processing.
    pub fn unique_records(&self) -> &[ObservedRevisionRecord] {
        &self.unique_records
    }

    /// Returns the checked conservative bytes admitted before duplicate coalescing.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Expands one assignment per unique record to the caller's original input order.
    ///
    /// # Errors
    ///
    /// Rejects a unique-assignment count that does not exactly match this batch.
    pub fn align_assignments(
        self,
        unique_assignments: Vec<RevisionNumber>,
    ) -> Result<ObservedRevisionAssignments, ObservedRevisionError> {
        let expected = self.unique_records.len();
        if unique_assignments.len() != expected {
            return Err(ObservedRevisionError::AssignmentCountMismatch {
                expected,
                observed: unique_assignments.len(),
            });
        }
        let mut aligned = Vec::new();
        aligned
            .try_reserve_exact(self.input_len)
            .map_err(|_| ObservedRevisionError::AllocationFailure)?;
        for unique_index in self.input_to_unique {
            let assignment = unique_assignments
                .get(unique_index)
                .copied()
                .ok_or(ObservedRevisionError::CorruptAuthorityState)?;
            aligned.push(assignment);
        }
        Ok(ObservedRevisionAssignments {
            revisions: aligned.into_boxed_slice(),
        })
    }
}

/// Durable revisions aligned one-for-one with an authority request's original input order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedRevisionAssignments {
    revisions: Box<[RevisionNumber]>,
}

impl ObservedRevisionAssignments {
    /// Returns one assigned durable revision per original batch input.
    pub fn as_slice(&self) -> &[RevisionNumber] {
        &self.revisions
    }
}

fn checked_batch_input_bytes(
    source_id: &SourceId,
    records: &[ObservedRevisionRecord],
    max_bytes: usize,
) -> Result<usize, ObservedRevisionError> {
    let record_scratch = size_of::<ObservedRevisionRecord>()
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(size_of::<Vec<usize>>()))
        .and_then(|bytes| bytes.checked_add(size_of::<usize>() * 2))
        .ok_or(ObservedRevisionError::ByteCountOverflow)?;
    let fixed_records = records
        .len()
        .checked_mul(record_scratch)
        .ok_or(ObservedRevisionError::ByteCountOverflow)?;
    let mut retained = source_id
        .retained_bytes()
        .checked_add(fixed_records)
        .ok_or(ObservedRevisionError::ByteCountOverflow)?;
    for record in records {
        retained = retained
            .checked_add(record.retained_bytes()?)
            .ok_or(ObservedRevisionError::ByteCountOverflow)?;
        if retained > max_bytes {
            return Err(ObservedRevisionError::BatchByteLimitExceeded { max: max_bytes });
        }
    }
    Ok(retained)
}

fn record_identity_cmp(left: &ObservedRevisionRecord, right: &ObservedRevisionRecord) -> Ordering {
    left.family
        .exact_bytes()
        .cmp(right.family.exact_bytes())
        .then_with(|| left.version.kind().cmp(&right.version.kind()))
        .then_with(|| {
            left.version
                .exact_evidence()
                .cmp(right.version.exact_evidence())
        })
}

fn same_exact_version(left: &ObservedRevisionRecord, right: &ObservedRevisionRecord) -> bool {
    left.family.exact_bytes() == right.family.exact_bytes()
        && left.version.kind() == right.version.kind()
        && left.version.exact_evidence() == right.version.exact_evidence()
}

fn order_family_versions(records: &mut [CoalescedRecord]) -> Result<(), ObservedRevisionError> {
    let mut family_start = 0;
    while family_start < records.len() {
        let family = records[family_start].record.family.exact_bytes();
        let mut family_end = family_start + 1;
        while family_end < records.len()
            && records[family_end].record.family.exact_bytes() == family
        {
            family_end += 1;
        }
        let family_versions = &mut records[family_start..family_end];
        if family_versions.len() > 1 {
            if family_versions
                .iter()
                .any(|record| record.record.provider_order.is_none())
            {
                return Err(ObservedRevisionError::AmbiguousProviderOrder);
            }
            let first_order = family_versions[0]
                .record
                .provider_order
                .as_ref()
                .ok_or(ObservedRevisionError::AmbiguousProviderOrder)?;
            if family_versions.iter().skip(1).any(|record| {
                record
                    .record
                    .provider_order
                    .as_ref()
                    .is_none_or(|order| first_order.checked_cmp(order).is_none())
            }) {
                return Err(ObservedRevisionError::AmbiguousProviderOrder);
            }
            family_versions.sort_unstable_by(|left, right| {
                let left_order = left.record.provider_order.as_ref();
                let right_order = right.record.provider_order.as_ref();
                match (left_order, right_order) {
                    (Some(left), Some(right)) => left.checked_cmp(right).unwrap_or(Ordering::Equal),
                    _ => Ordering::Equal,
                }
            });
            if family_versions.windows(2).any(|pair| {
                let left = pair[0].record.provider_order.as_ref();
                let right = pair[1].record.provider_order.as_ref();
                !matches!(
                    (left, right),
                    (Some(left), Some(right))
                        if left.checked_cmp(right).is_some_and(Ordering::is_lt)
                )
            }) {
                return Err(ObservedRevisionError::AmbiguousProviderOrder);
            }
        }
        family_start = family_end;
    }
    Ok(())
}
