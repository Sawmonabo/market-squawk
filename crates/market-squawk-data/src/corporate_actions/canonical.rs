//! Canonical ordering and SHA-256 identities for corporate-action plans.

use market_squawk_domain::{
    AvailabilityEvidence, CorporateActionKind, DataQuality, DigestAlgorithm, MergerConsideration,
    Money, PayloadReference, ResearchTemporalCoordinate, ResearchTemporalPrecision,
};
use sha2::{Digest as _, Sha256};

use super::{
    AdjustmentConflict, AdjustmentStep, CorporateActionError, CorporateActionExclusion,
    CorporateActionExclusionReason, CorporateActionPolicy, CorporateActionRecord,
};
use crate::Sha256Digest;

pub(super) fn canonical_record_bytes(
    record: &CorporateActionRecord,
) -> Result<Vec<u8>, CorporateActionError> {
    let mut output = Vec::new();
    let context = record.observation.context();
    let provenance = context.provenance();
    put_u16(&mut output, provenance.schema_version().get())?;
    put_string(&mut output, provenance.source_id().as_str())?;
    let instrument = provenance
        .instrument_id()
        .ok_or(CorporateActionError::MissingInstrument)?;
    put_fixed(&mut output, instrument.as_uuid().as_bytes())?;
    put_optional_string(
        &mut output,
        provenance.venue_id().map(|venue| venue.as_str()),
    )?;
    put_string(&mut output, provenance.source_identifier().as_str())?;
    put_optional_timestamp(&mut output, provenance.source_timestamp())?;
    put_i64(&mut output, provenance.received_at().unix_nanos())?;
    put_i64(&mut output, provenance.ingested_at().unix_nanos())?;
    put_u8(&mut output, data_quality_tag(provenance.quality()))?;
    put_payload_reference(&mut output, provenance.payload_reference())?;
    put_availability(&mut output, provenance.availability())?;

    let time = context.time();
    put_temporal_coordinate(&mut output, time.effective())?;
    put_optional_coordinate(&mut output, time.published())?;
    put_u32(&mut output, time.revision().get())?;
    put_optional_coordinate(&mut output, time.superseded())?;
    put_action(&mut output, record.observation.action())?;

    let manifest = &record.source_manifest;
    put_string(&mut output, manifest.dataset_id().as_str())?;
    put_u64(&mut output, manifest.manifest_version())?;
    put_string(&mut output, manifest.schema().name())?;
    put_u16(&mut output, manifest.schema().version().get())?;
    put_fixed(&mut output, &manifest.schema().fingerprint())?;
    put_fixed(&mut output, &manifest.content_hash().bytes())?;
    put_evidence_digest(&mut output, record.evidence_digest)?;
    Ok(output)
}

pub(super) fn content_hash(
    policy: CorporateActionPolicy,
    knowledge_cutoff: market_squawk_domain::Timestamp,
    valuation_cutoff: market_squawk_domain::Timestamp,
    admitted: &[CorporateActionRecord],
    steps: &[AdjustmentStep],
    conflicts: &[AdjustmentConflict],
) -> Result<Sha256Digest, CorporateActionError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.corporate-action-plan.v1\0");
    digest.update([policy_tag(policy)]);
    digest.update(policy.version().get().to_be_bytes());
    digest.update(knowledge_cutoff.unix_nanos().to_be_bytes());
    digest.update(valuation_cutoff.unix_nanos().to_be_bytes());
    hash_length(&mut digest, admitted.len())?;
    for record in admitted {
        hash_bytes(&mut digest, &canonical_record_bytes(record)?)?;
    }
    hash_length(&mut digest, steps.len())?;
    for step in steps {
        hash_bytes(&mut digest, &canonical_step_bytes(step)?)?;
    }
    hash_length(&mut digest, conflicts.len())?;
    for conflict in conflicts {
        hash_bytes(&mut digest, &canonical_conflict_bytes(*conflict)?)?;
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

pub(super) fn audit_hash(
    content_hash: Sha256Digest,
    exclusions: &[CorporateActionExclusion],
) -> Result<Sha256Digest, CorporateActionError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk.corporate-action-audit.v1\0");
    digest.update(content_hash.bytes());
    hash_length(&mut digest, exclusions.len())?;
    for exclusion in exclusions {
        digest.update([exclusion_reason_tag(exclusion.reason)]);
        hash_bytes(&mut digest, &canonical_record_bytes(&exclusion.record)?)?;
    }
    Ok(Sha256Digest::new(digest.finalize().into()))
}

fn canonical_step_bytes(step: &AdjustmentStep) -> Result<Vec<u8>, CorporateActionError> {
    let mut output = Vec::new();
    match step {
        AdjustmentStep::Split {
            admitted_index,
            price_factor,
            quantity_factor,
        } => {
            put_u8(&mut output, 0)?;
            put_usize(&mut output, *admitted_index)?;
            put_ratio(&mut output, *price_factor)?;
            put_ratio(&mut output, *quantity_factor)?;
        }
        AdjustmentStep::CashDividend {
            admitted_index,
            amount,
        } => {
            put_u8(&mut output, 1)?;
            put_usize(&mut output, *admitted_index)?;
            put_money(&mut output, *amount)?;
        }
        AdjustmentStep::ReturnOfCapital {
            admitted_index,
            amount,
        } => {
            put_u8(&mut output, 2)?;
            put_usize(&mut output, *admitted_index)?;
            put_money(&mut output, *amount)?;
        }
        AdjustmentStep::Spinoff {
            admitted_index,
            distributed_instrument,
            distribution_ratio,
        } => {
            put_u8(&mut output, 3)?;
            put_usize(&mut output, *admitted_index)?;
            put_fixed(&mut output, distributed_instrument.as_uuid().as_bytes())?;
            put_ratio(&mut output, *distribution_ratio)?;
        }
        AdjustmentStep::Merger {
            admitted_index,
            successor,
            consideration,
        } => {
            put_u8(&mut output, 4)?;
            put_usize(&mut output, *admitted_index)?;
            put_fixed(&mut output, successor.as_uuid().as_bytes())?;
            put_merger_consideration(&mut output, *consideration)?;
        }
        AdjustmentStep::Delisting { admitted_index } => {
            put_u8(&mut output, 5)?;
            put_usize(&mut output, *admitted_index)?;
        }
        AdjustmentStep::SymbolChange {
            admitted_index,
            venue_id,
            previous,
            current,
        } => {
            put_u8(&mut output, 6)?;
            put_usize(&mut output, *admitted_index)?;
            put_string(&mut output, venue_id.as_str())?;
            put_string(&mut output, previous.as_str())?;
            put_string(&mut output, current.as_str())?;
        }
    }
    Ok(output)
}

fn canonical_conflict_bytes(conflict: AdjustmentConflict) -> Result<Vec<u8>, CorporateActionError> {
    let mut output = Vec::new();
    match conflict {
        AdjustmentConflict::IncompleteMergerTerms {
            admitted_index,
            successor,
        } => {
            put_u8(&mut output, 0)?;
            put_usize(&mut output, admitted_index)?;
            put_fixed(&mut output, successor.as_uuid().as_bytes())?;
        }
    }
    Ok(output)
}

fn put_action(
    output: &mut Vec<u8>,
    action: &CorporateActionKind,
) -> Result<(), CorporateActionError> {
    match action {
        CorporateActionKind::Split {
            numerator,
            denominator,
        } => {
            put_u8(output, 0)?;
            put_u32(output, numerator.get())?;
            put_u32(output, denominator.get())
        }
        CorporateActionKind::CashDividend { amount } => {
            put_u8(output, 1)?;
            put_money(output, *amount)
        }
        CorporateActionKind::Spinoff {
            distributed_instrument,
            numerator,
            denominator,
        } => {
            put_u8(output, 2)?;
            put_fixed(output, distributed_instrument.as_uuid().as_bytes())?;
            put_u32(output, numerator.get())?;
            put_u32(output, denominator.get())
        }
        CorporateActionKind::ReturnOfCapital { amount } => {
            put_u8(output, 3)?;
            put_money(output, *amount)
        }
        CorporateActionKind::Merger {
            successor,
            consideration,
        } => {
            put_u8(output, 4)?;
            put_fixed(output, successor.as_uuid().as_bytes())?;
            put_merger_consideration(output, *consideration)
        }
        CorporateActionKind::Delisting => put_u8(output, 5),
        CorporateActionKind::SymbolChange {
            venue_id,
            previous,
            current,
        } => {
            put_u8(output, 6)?;
            put_string(output, venue_id.as_str())?;
            put_string(output, previous.as_str())?;
            put_string(output, current.as_str())
        }
    }
}

fn put_merger_consideration(
    output: &mut Vec<u8>,
    consideration: MergerConsideration,
) -> Result<(), CorporateActionError> {
    match consideration {
        MergerConsideration::Unspecified => put_u8(output, 0),
        MergerConsideration::Stock {
            numerator,
            denominator,
        } => {
            put_u8(output, 1)?;
            put_u32(output, numerator.get())?;
            put_u32(output, denominator.get())
        }
        MergerConsideration::Cash { amount } => {
            put_u8(output, 2)?;
            put_money(output, amount)
        }
        MergerConsideration::Mixed {
            numerator,
            denominator,
            cash,
        } => {
            put_u8(output, 3)?;
            put_u32(output, numerator.get())?;
            put_u32(output, denominator.get())?;
            put_money(output, cash)
        }
    }
}

fn put_money(output: &mut Vec<u8>, money: Money) -> Result<(), CorporateActionError> {
    let amount = money.amount().normalize();
    put_fixed(output, &amount.mantissa().to_be_bytes())?;
    put_u32(output, amount.scale())?;
    put_fixed(output, money.currency().as_str().as_bytes())
}

fn put_ratio(
    output: &mut Vec<u8>,
    ratio: super::AdjustmentRatio,
) -> Result<(), CorporateActionError> {
    put_u32(output, ratio.numerator().get())?;
    put_u32(output, ratio.denominator().get())
}

fn put_payload_reference(
    output: &mut Vec<u8>,
    reference: &PayloadReference,
) -> Result<(), CorporateActionError> {
    match reference {
        PayloadReference::ContentHash(hash) => {
            put_u8(output, 0)?;
            put_u8(output, digest_algorithm_tag(hash.algorithm()))?;
            put_fixed(output, &hash.digest())
        }
        PayloadReference::SourceReference(reference) => {
            put_u8(output, 1)?;
            put_string(output, reference.as_str())
        }
    }
}

fn put_availability(
    output: &mut Vec<u8>,
    availability: &AvailabilityEvidence,
) -> Result<(), CorporateActionError> {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => {
            put_u8(output, 0)?;
            put_i64(output, available_at.unix_nanos())?;
            put_string(output, evidence.as_str())
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            put_u8(output, 1)?;
            put_i64(output, observed_at.unix_nanos())
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => {
            put_u8(output, 2)?;
            put_i64(output, inferred_at.unix_nanos())?;
            put_string(output, method.as_str())
        }
        AvailabilityEvidence::Unknown => put_u8(output, 3),
    }
}

fn put_optional_coordinate(
    output: &mut Vec<u8>,
    coordinate: Option<&ResearchTemporalCoordinate>,
) -> Result<(), CorporateActionError> {
    match coordinate {
        Some(value) => {
            put_u8(output, 1)?;
            put_temporal_coordinate(output, value)
        }
        None => put_u8(output, 0),
    }
}

fn put_temporal_coordinate(
    output: &mut Vec<u8>,
    coordinate: &ResearchTemporalCoordinate,
) -> Result<(), CorporateActionError> {
    match coordinate.precision() {
        ResearchTemporalPrecision::ExactTimestamp => {
            put_u8(output, 0)?;
            let value = coordinate
                .exact_timestamp()
                .ok_or(CorporateActionError::CanonicalEncodingOverflow)?;
            put_i64(output, value.unix_nanos())
        }
        ResearchTemporalPrecision::CalendarDate => {
            put_u8(output, 1)?;
            let value = coordinate
                .calendar_date_value()
                .ok_or(CorporateActionError::CanonicalEncodingOverflow)?;
            put_u16(output, value.year())?;
            put_u8(output, value.month())?;
            put_u8(output, value.day())
        }
        ResearchTemporalPrecision::SourcePeriod => {
            put_u8(output, 2)?;
            let value = coordinate
                .source_period_value()
                .ok_or(CorporateActionError::CanonicalEncodingOverflow)?;
            put_string(output, value.scheme().as_str())?;
            put_u16(output, value.year())?;
            put_u16(output, value.ordinal().get())?;
            put_string(output, value.code().as_str())
        }
    }
}

fn put_evidence_digest(
    output: &mut Vec<u8>,
    evidence: market_squawk_domain::EvidenceDigest,
) -> Result<(), CorporateActionError> {
    put_u8(output, digest_algorithm_tag(evidence.algorithm()))?;
    put_fixed(output, &evidence.bytes())
}

fn put_optional_string(
    output: &mut Vec<u8>,
    value: Option<&str>,
) -> Result<(), CorporateActionError> {
    match value {
        Some(value) => {
            put_u8(output, 1)?;
            put_string(output, value)
        }
        None => put_u8(output, 0),
    }
}

fn put_optional_timestamp(
    output: &mut Vec<u8>,
    value: Option<market_squawk_domain::Timestamp>,
) -> Result<(), CorporateActionError> {
    match value {
        Some(value) => {
            put_u8(output, 1)?;
            put_i64(output, value.unix_nanos())
        }
        None => put_u8(output, 0),
    }
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<(), CorporateActionError> {
    put_bytes(output, value.as_bytes())
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), CorporateActionError> {
    put_usize(output, value.len())?;
    put_fixed(output, value)
}

fn put_usize(output: &mut Vec<u8>, value: usize) -> Result<(), CorporateActionError> {
    let value =
        u64::try_from(value).map_err(|_| CorporateActionError::CanonicalEncodingOverflow)?;
    put_u64(output, value)
}

fn put_u64(output: &mut Vec<u8>, value: u64) -> Result<(), CorporateActionError> {
    put_fixed(output, &value.to_be_bytes())
}

fn put_i64(output: &mut Vec<u8>, value: i64) -> Result<(), CorporateActionError> {
    put_fixed(output, &value.to_be_bytes())
}

fn put_u32(output: &mut Vec<u8>, value: u32) -> Result<(), CorporateActionError> {
    put_fixed(output, &value.to_be_bytes())
}

fn put_u16(output: &mut Vec<u8>, value: u16) -> Result<(), CorporateActionError> {
    put_fixed(output, &value.to_be_bytes())
}

fn put_u8(output: &mut Vec<u8>, value: u8) -> Result<(), CorporateActionError> {
    output
        .try_reserve_exact(1)
        .map_err(|_| CorporateActionError::AllocationFailed)?;
    output.push(value);
    Ok(())
}

fn put_fixed(output: &mut Vec<u8>, value: &[u8]) -> Result<(), CorporateActionError> {
    output
        .try_reserve_exact(value.len())
        .map_err(|_| CorporateActionError::AllocationFailed)?;
    output.extend_from_slice(value);
    Ok(())
}

fn hash_length(digest: &mut Sha256, value: usize) -> Result<(), CorporateActionError> {
    let value =
        u64::try_from(value).map_err(|_| CorporateActionError::CanonicalEncodingOverflow)?;
    digest.update(value.to_be_bytes());
    Ok(())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), CorporateActionError> {
    hash_length(digest, value.len())?;
    digest.update(value);
    Ok(())
}

const fn policy_tag(policy: CorporateActionPolicy) -> u8 {
    match policy.adjustment() {
        super::CorporateActionAdjustment::Raw => 0,
        super::CorporateActionAdjustment::SplitAdjusted => 1,
        super::CorporateActionAdjustment::TotalReturn => 2,
    }
}

const fn exclusion_reason_tag(reason: CorporateActionExclusionReason) -> u8 {
    match reason {
        CorporateActionExclusionReason::FutureAvailability => 0,
        CorporateActionExclusionReason::InferredAvailability => 1,
        CorporateActionExclusionReason::UnknownAvailability => 2,
        CorporateActionExclusionReason::FutureEffectiveTime => 3,
        CorporateActionExclusionReason::AmbiguousEffectiveTime => 4,
    }
}

const fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 0,
        DigestAlgorithm::Blake3 => 1,
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}
