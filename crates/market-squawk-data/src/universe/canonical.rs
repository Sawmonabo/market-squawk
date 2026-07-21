//! Canonical ordering and digest encodings for historical-universe results.

use std::cmp::Ordering;

use market_squawk_domain::{AvailabilityEvidence, DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};

use super::{
    UniverseError, UniverseExclusion, UniverseExclusionCounts, UniverseExclusionReason, UniverseId,
    UniverseMembership,
};
use crate::Sha256Digest;

pub(super) fn compare_memberships(
    left: &UniverseMembership,
    right: &UniverseMembership,
) -> Ordering {
    left.instrument_id
        .cmp(&right.instrument_id)
        .then_with(|| {
            left.effective_interval
                .starts_at()
                .cmp(&right.effective_interval.starts_at())
        })
        .then_with(|| {
            left.effective_interval
                .ends_at()
                .cmp(&right.effective_interval.ends_at())
        })
        .then_with(|| compare_availability(&left.availability, &right.availability))
        .then_with(|| {
            left.source_manifest
                .dataset_id()
                .as_str()
                .cmp(right.source_manifest.dataset_id().as_str())
        })
        .then_with(|| {
            left.source_manifest
                .manifest_version()
                .cmp(&right.source_manifest.manifest_version())
        })
        .then_with(|| {
            left.source_manifest
                .schema()
                .cmp(right.source_manifest.schema())
        })
        .then_with(|| {
            left.source_manifest
                .content_hash()
                .cmp(&right.source_manifest.content_hash())
        })
        .then_with(|| compare_evidence_digest(left.evidence_digest, right.evidence_digest))
}

fn compare_availability(left: &AvailabilityEvidence, right: &AvailabilityEvidence) -> Ordering {
    availability_rank(left)
        .cmp(&availability_rank(right))
        .then_with(|| match (left, right) {
            (
                AvailabilityEvidence::Evidenced {
                    available_at: left_time,
                    evidence: left_evidence,
                },
                AvailabilityEvidence::Evidenced {
                    available_at: right_time,
                    evidence: right_evidence,
                },
            ) => left_time
                .cmp(right_time)
                .then_with(|| left_evidence.cmp(right_evidence)),
            (
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: left_time,
                },
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: right_time,
                },
            ) => left_time.cmp(right_time),
            (
                AvailabilityEvidence::Inferred {
                    inferred_at: left_time,
                    method: left_method,
                },
                AvailabilityEvidence::Inferred {
                    inferred_at: right_time,
                    method: right_method,
                },
            ) => left_time
                .cmp(right_time)
                .then_with(|| left_method.cmp(right_method)),
            _ => Ordering::Equal,
        })
}

const fn availability_rank(value: &AvailabilityEvidence) -> u8 {
    match value {
        AvailabilityEvidence::Evidenced { .. } => 0,
        AvailabilityEvidence::LocalFirstObserved { .. } => 1,
        AvailabilityEvidence::Inferred { .. } => 2,
        AvailabilityEvidence::Unknown => 3,
    }
}

fn compare_evidence_digest(left: EvidenceDigest, right: EvidenceDigest) -> Ordering {
    digest_algorithm_rank(left.algorithm())
        .cmp(&digest_algorithm_rank(right.algorithm()))
        .then_with(|| left.bytes().cmp(&right.bytes()))
}

const fn digest_algorithm_rank(value: DigestAlgorithm) -> u8 {
    match value {
        DigestAlgorithm::Sha256 => 0,
        DigestAlgorithm::Blake3 => 1,
    }
}

pub(super) fn snapshot_content_hash(
    universe_id: &UniverseId,
    as_of: Timestamp,
    memberships: &[UniverseMembership],
) -> Result<Sha256Digest, UniverseError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.universe-snapshot.v1\0");
    hash_string(&mut digest, universe_id.as_str())?;
    digest.update(as_of.unix_nanos().to_be_bytes());
    hash_length(&mut digest, memberships.len())?;
    for membership in memberships {
        hash_membership(&mut digest, membership)?;
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

pub(super) fn snapshot_audit_hash(
    universe_id: &UniverseId,
    as_of: Timestamp,
    memberships: &[UniverseMembership],
    exclusions: &[UniverseExclusion],
    counts: UniverseExclusionCounts,
) -> Result<Sha256Digest, UniverseError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.universe-audit.v1\0");
    hash_string(&mut digest, universe_id.as_str())?;
    digest.update(as_of.unix_nanos().to_be_bytes());
    hash_length(&mut digest, memberships.len())?;
    for membership in memberships {
        hash_membership(&mut digest, membership)?;
    }
    hash_length(&mut digest, exclusions.len())?;
    for exclusion in exclusions {
        digest.update([exclusion_reason_rank(exclusion.reason)]);
        hash_membership(&mut digest, &exclusion.membership)?;
    }
    for count in [
        counts.total,
        counts.not_effective,
        counts.future_availability,
        counts.inferred_availability,
        counts.unknown_availability,
    ] {
        hash_length(&mut digest, count)?;
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_membership(
    digest: &mut Sha256,
    membership: &UniverseMembership,
) -> Result<(), UniverseError> {
    digest.update(membership.instrument_id.as_uuid().as_bytes());
    digest.update(
        membership
            .effective_interval
            .starts_at()
            .unix_nanos()
            .to_be_bytes(),
    );
    match membership.effective_interval.ends_at() {
        Some(ends_at) => {
            digest.update([1]);
            digest.update(ends_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    hash_availability(digest, &membership.availability)?;
    hash_string(digest, membership.source_manifest.dataset_id().as_str())?;
    digest.update(membership.source_manifest.manifest_version().to_be_bytes());
    hash_string(digest, membership.source_manifest.schema().name())?;
    digest.update(
        membership
            .source_manifest
            .schema()
            .version()
            .get()
            .to_be_bytes(),
    );
    digest.update(membership.source_manifest.schema().fingerprint());
    digest.update(membership.source_manifest.content_hash().bytes());
    digest.update([digest_algorithm_rank(
        membership.evidence_digest.algorithm(),
    )]);
    digest.update(membership.evidence_digest.bytes());
    Ok(())
}

const fn exclusion_reason_rank(reason: UniverseExclusionReason) -> u8 {
    match reason {
        UniverseExclusionReason::NotEffective => 0,
        UniverseExclusionReason::FutureAvailability => 1,
        UniverseExclusionReason::InferredAvailability => 2,
        UniverseExclusionReason::UnknownAvailability => 3,
    }
}

fn hash_availability(
    digest: &mut Sha256,
    availability: &AvailabilityEvidence,
) -> Result<(), UniverseError> {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => {
            digest.update([0]);
            digest.update(available_at.unix_nanos().to_be_bytes());
            hash_string(digest, evidence.as_str())?;
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            digest.update([1]);
            digest.update(observed_at.unix_nanos().to_be_bytes());
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => {
            digest.update([2]);
            digest.update(inferred_at.unix_nanos().to_be_bytes());
            hash_string(digest, method.as_str())?;
        }
        AvailabilityEvidence::Unknown => digest.update([3]),
    }
    Ok(())
}

fn hash_string(digest: &mut Sha256, value: &str) -> Result<(), UniverseError> {
    hash_length(digest, value.len())?;
    digest.update(value.as_bytes());
    Ok(())
}

fn hash_length(digest: &mut Sha256, value: usize) -> Result<(), UniverseError> {
    let value = u64::try_from(value).map_err(|_| UniverseError::CanonicalEncodingOverflow)?;
    digest.update(value.to_be_bytes());
    Ok(())
}
