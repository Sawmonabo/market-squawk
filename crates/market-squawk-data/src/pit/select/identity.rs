//! Usable-content and complete-decision audit identities.

use super::{Decision, PreparedCandidate};
use crate::Sha256Digest;
use crate::pit::canonical::{
    AUDIT_DOMAIN, CONTENT_DOMAIN, CanonicalEncoder, encode_request, encode_revision_state,
    map_error,
};
use crate::pit::retained::OperationControl;
use crate::pit::{
    PointInTimeConflictCounts, PointInTimeError, PointInTimeExclusionCounts, PointInTimeRecord,
    PointInTimeRequest, PointInTimeRevisionCounts,
};

pub(super) fn content_identity<'a>(
    request: &PointInTimeRequest,
    records: &[PointInTimeRecord<'a>],
    control: &mut OperationControl,
) -> Result<Sha256Digest, PointInTimeError<'a>> {
    let mut encoder = CanonicalEncoder::new(CONTENT_DOMAIN, control).map_err(map_error)?;
    encode_request(&mut encoder, request).map_err(map_error)?;
    encoder
        .u64(u64::try_from(records.len()).map_err(|_| PointInTimeError::CanonicalEncoding)?)
        .map_err(map_error)?;
    for record in records {
        encoder
            .digest(record.family_identity())
            .map_err(map_error)?;
        encoder
            .u32(record.candidate().revision().get())
            .map_err(map_error)?;
        encoder
            .digest(record.payload_identity())
            .map_err(map_error)?;
        encode_revision_state(&mut encoder, record.revision_state()).map_err(map_error)?;
    }
    Ok(encoder.finish())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn audit_identity<'a>(
    request: &PointInTimeRequest,
    prepared: &[PreparedCandidate<'a>],
    order: &[usize],
    decisions: &[Decision],
    exclusion_counts: PointInTimeExclusionCounts,
    revision_counts: PointInTimeRevisionCounts,
    conflict_counts: PointInTimeConflictCounts,
    control: &mut OperationControl,
) -> Result<Sha256Digest, PointInTimeError<'a>> {
    let mut encoder = CanonicalEncoder::new(AUDIT_DOMAIN, control).map_err(map_error)?;
    encode_request(&mut encoder, request).map_err(map_error)?;
    encoder
        .u64(u64::try_from(order.len()).map_err(|_| PointInTimeError::CanonicalEncoding)?)
        .map_err(map_error)?;
    for index in order {
        encoder
            .digest(prepared[*index].evidence_identity)
            .map_err(map_error)?;
        match decisions[*index] {
            Decision::Pending => encoder.u8(0).map_err(map_error)?,
            Decision::Selected(state) => {
                encoder.u8(1).map_err(map_error)?;
                encode_revision_state(&mut encoder, state).map_err(map_error)?;
            }
            Decision::Excluded(reasons) => {
                encoder.u8(2).map_err(map_error)?;
                encoder.u16(reasons.bits()).map_err(map_error)?;
            }
            Decision::Conflict => encoder.u8(3).map_err(map_error)?,
        }
    }
    for count in exclusion_counts.counts() {
        encoder
            .u64(u64::try_from(count).map_err(|_| PointInTimeError::CanonicalEncoding)?)
            .map_err(map_error)?;
    }
    for count in revision_counts.values() {
        encoder
            .u64(u64::try_from(count).map_err(|_| PointInTimeError::CanonicalEncoding)?)
            .map_err(map_error)?;
    }
    for count in [
        conflict_counts.conflicting_groups(),
        conflict_counts.conflicting_candidates(),
        conflict_counts.payload_variants(),
    ] {
        encoder
            .u64(u64::try_from(count).map_err(|_| PointInTimeError::CanonicalEncoding)?)
            .map_err(map_error)?;
    }
    Ok(encoder.finish())
}
