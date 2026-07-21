//! Canonical ordering and hashes for derivative-universe results.

use std::cmp::Ordering;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId,
};
use sha2::{Digest as _, Sha256};

use super::{
    ContractRollEvidence, DerivativeCivilDate, DerivativeDecisionRecord, DerivativeLifecycle,
    DerivativeLifecycleEvidence, DerivativeSelectionDecision, boundary_rank,
};
use crate::manifest::compare_manifest_refs;
use crate::{DatasetManifestRef, Sha256Digest, UniverseError, UniverseSnapshot};

pub(super) fn compare_lifecycle_evidence(
    left: &DerivativeLifecycleEvidence,
    right: &DerivativeLifecycleEvidence,
) -> Ordering {
    left.instrument_id
        .cmp(&right.instrument_id)
        .then_with(|| compare_manifest_refs(&left.source_manifest, &right.source_manifest))
        .then_with(|| compare_evidence(left.evidence_digest, right.evidence_digest))
}

pub(super) fn compare_civil_dates(
    left: &DerivativeCivilDate,
    right: &DerivativeCivilDate,
) -> Ordering {
    left.instrument_id
        .cmp(&right.instrument_id)
        .then_with(|| left.date.cmp(&right.date))
        .then_with(|| left.snapshot_at.cmp(&right.snapshot_at))
        .then_with(|| {
            left.calendar_rule
                .as_str()
                .cmp(right.calendar_rule.as_str())
        })
        .then_with(|| compare_evidence(left.rule_evidence, right.rule_evidence))
}

pub(super) fn compare_roll_evidence(
    left: &ContractRollEvidence,
    right: &ContractRollEvidence,
) -> Ordering {
    left.mapping
        .from_instrument_id()
        .cmp(&right.mapping.from_instrument_id())
        .then_with(|| {
            left.mapping
                .to_instrument_id()
                .cmp(&right.mapping.to_instrument_id())
        })
        .then_with(|| {
            left.mapping
                .effective_at()
                .cmp(&right.mapping.effective_at())
        })
        .then_with(|| compare_manifest_refs(&left.source_manifest, &right.source_manifest))
        .then_with(|| compare_evidence(left.evidence_digest, right.evidence_digest))
}

pub(super) fn content_hash(
    base: &UniverseSnapshot,
    active: &[InstrumentId],
    decisions: &[DerivativeDecisionRecord],
) -> Result<Sha256Digest, UniverseError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.derivative-universe-content.v1\0");
    digest.update(base.content_hash().bytes());
    hash_length(&mut digest, active.len())?;
    for instrument_id in active {
        digest.update(instrument_id.as_uuid().as_bytes());
    }
    for decision in decisions {
        if let DerivativeSelectionDecision::Rolled { .. } = decision.decision {
            hash_decision(&mut digest, decision);
        }
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

pub(super) fn audit_hash(
    base: &UniverseSnapshot,
    lifecycle: &[DerivativeLifecycleEvidence],
    civil_dates: &[DerivativeCivilDate],
    rolls: &[ContractRollEvidence],
    decisions: &[DerivativeDecisionRecord],
) -> Result<Sha256Digest, UniverseError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.derivative-universe-audit.v2\0");
    digest.update(base.audit_hash().bytes());
    hash_length(&mut digest, lifecycle.len())?;
    for evidence in lifecycle {
        hash_lifecycle(&mut digest, evidence)?;
    }
    hash_length(&mut digest, civil_dates.len())?;
    for value in civil_dates {
        digest.update(value.instrument_id.as_uuid().as_bytes());
        hash_date(&mut digest, value.date);
        digest.update(value.snapshot_at.unix_nanos().to_be_bytes());
        hash_string(&mut digest, value.calendar_rule.as_str())?;
        hash_evidence(&mut digest, value.rule_evidence);
        hash_availability(&mut digest, &value.rule_availability)?;
    }
    hash_length(&mut digest, rolls.len())?;
    for roll in rolls {
        hash_roll(&mut digest, roll)?;
    }
    hash_length(&mut digest, decisions.len())?;
    for decision in decisions {
        hash_decision(&mut digest, decision);
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn hash_lifecycle(
    digest: &mut Sha256,
    evidence: &DerivativeLifecycleEvidence,
) -> Result<(), UniverseError> {
    digest.update(evidence.instrument_id.as_uuid().as_bytes());
    match &evidence.lifecycle {
        DerivativeLifecycle::Option {
            identity,
            expiration_date,
        } => {
            digest.update([0]);
            hash_string(digest, identity.as_str())?;
            hash_date(digest, *expiration_date);
        }
        DerivativeLifecycle::Future(lifecycle) => {
            digest.update([1]);
            for date in [
                lifecycle.first_trade_date(),
                lifecycle.maturity_date(),
                lifecycle.expiration_date(),
                lifecycle.last_trade_date(),
                lifecycle.settlement_date(),
                lifecycle.first_notice_date(),
                lifecycle.last_notice_date(),
                lifecycle.first_delivery_date(),
                lifecycle.last_delivery_date(),
            ] {
                hash_optional_date(digest, date);
            }
        }
    }
    hash_availability(digest, &evidence.availability)?;
    hash_manifest(digest, &evidence.source_manifest)?;
    hash_evidence(digest, evidence.evidence_digest);
    Ok(())
}

fn hash_roll(digest: &mut Sha256, roll: &ContractRollEvidence) -> Result<(), UniverseError> {
    digest.update(roll.mapping.from_instrument_id().as_uuid().as_bytes());
    digest.update(roll.mapping.to_instrument_id().as_uuid().as_bytes());
    digest.update(roll.mapping.effective_at().unix_nanos().to_be_bytes());
    hash_availability(digest, &roll.availability)?;
    hash_manifest(digest, &roll.source_manifest)?;
    hash_evidence(digest, roll.evidence_digest);
    Ok(())
}

fn hash_decision(digest: &mut Sha256, record: &DerivativeDecisionRecord) {
    digest.update(record.instrument_id.as_uuid().as_bytes());
    match record.decision {
        DerivativeSelectionDecision::Active => digest.update([0]),
        DerivativeSelectionDecision::SameDateUnresolved { boundary, date } => {
            digest.update([1, boundary_rank(boundary)]);
            hash_date(digest, date);
        }
        DerivativeSelectionDecision::OptionExpired { boundary, date } => {
            digest.update([2, boundary_rank(boundary)]);
            hash_date(digest, date);
        }
        DerivativeSelectionDecision::FutureExpiredWithoutRoll { boundary, date } => {
            digest.update([3, boundary_rank(boundary)]);
            hash_date(digest, date);
        }
        DerivativeSelectionDecision::MissingTerminationBoundary => digest.update([4]),
        DerivativeSelectionDecision::LifecycleEvidenceUnavailable => digest.update([5]),
        DerivativeSelectionDecision::Rolled {
            to_instrument_id,
            effective_at,
        } => {
            digest.update([6]);
            digest.update(to_instrument_id.as_uuid().as_bytes());
            digest.update(effective_at.unix_nanos().to_be_bytes());
        }
    }
}

fn hash_manifest(digest: &mut Sha256, manifest: &DatasetManifestRef) -> Result<(), UniverseError> {
    hash_string(digest, manifest.dataset_id().as_str())?;
    digest.update(manifest.manifest_version().to_be_bytes());
    hash_string(digest, manifest.schema().name())?;
    digest.update(manifest.schema().version().get().to_be_bytes());
    digest.update(manifest.schema().fingerprint());
    digest.update(manifest.content_hash().bytes());
    Ok(())
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

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([digest_algorithm_rank(evidence.algorithm())]);
    digest.update(evidence.bytes());
}

fn compare_evidence(left: EvidenceDigest, right: EvidenceDigest) -> Ordering {
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

fn hash_optional_date(digest: &mut Sha256, date: Option<CalendarDate>) {
    if let Some(date) = date {
        digest.update([1]);
        hash_date(digest, date);
    } else {
        digest.update([0]);
    }
}

fn hash_date(digest: &mut Sha256, date: CalendarDate) {
    digest.update(date.year().to_be_bytes());
    digest.update([date.month(), date.day()]);
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
