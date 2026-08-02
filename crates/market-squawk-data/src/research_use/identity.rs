//! Canonical durable identities and closed SQLite scalar encodings.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use rusqlite::{Transaction, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::catalog::{
    DerivedOutputObjectInput, ResearchUseCatalogError, ResearchUseGrantInput,
    ResearchUseRevocationInput, retention_operation_name,
};
use super::{DerivedRetentionOperation, ResearchUse};
use crate::IngestReservation;

pub(super) fn grant_digest(input: &ResearchUseGrantInput) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/research-use-grant/v1");
    hash.update(input.rights_id);
    hash.update([input.permitted_uses.mask()]);
    hash.update([digest_algorithm_tag(input.evidence.algorithm())]);
    hash.update(input.evidence.bytes());
    hash.update([u8::from(input.authorization_expires_at.is_some())]);
    if let Some(expiry) = input.authorization_expires_at {
        hash.update(expiry.unix_nanos().to_be_bytes());
    }
    hash.finalize().into()
}

pub(super) fn revocation_digest(input: &ResearchUseRevocationInput) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/research-use-revocation/v1");
    hash.update(input.grant_id);
    hash.update([input.revoked_uses.mask(), input.reason.tag()]);
    hash.update([digest_algorithm_tag(input.evidence.algorithm())]);
    hash.update(input.evidence.bytes());
    hash.update(input.effective_at.unix_nanos().to_be_bytes());
    hash.finalize().into()
}

pub(super) fn output_reservation_digest(
    session_id: Uuid,
    reservation: &IngestReservation,
    operation: DerivedRetentionOperation,
    rights_id: [u8; 32],
    input: &DerivedOutputObjectInput,
) -> [u8; 32] {
    output_reservation_digest_parts(
        session_id,
        reservation.run_id(),
        reservation.requested_at(),
        operation,
        rights_id,
        input,
    )
}

pub(super) fn output_reservation_digest_parts(
    session_id: Uuid,
    run_id: Uuid,
    requested_at: Timestamp,
    operation: DerivedRetentionOperation,
    rights_id: [u8; 32],
    input: &DerivedOutputObjectInput,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/derived-output-reservation/v1");
    hash.update(session_id.as_bytes());
    hash.update(run_id.as_bytes());
    hash.update(requested_at.unix_nanos().to_be_bytes());
    hash.update(retention_operation_name(operation).as_bytes());
    hash.update(rights_id);
    hash.update(input.artifact_id.as_bytes());
    hash.update(input.content_hash.bytes());
    hash.update(input.row_count.to_be_bytes());
    hash.update(input.size_bytes.to_be_bytes());
    hash.update(input.lineage_digest.bytes());
    hash.finalize().into()
}

pub(super) fn parse_digest(value: Vec<u8>) -> Result<[u8; 32], ResearchUseCatalogError> {
    value
        .try_into()
        .map_err(|_| ResearchUseCatalogError::CorruptCatalog)
}

pub(super) fn parse_evidence(
    algorithm: i64,
    value: Vec<u8>,
) -> Result<EvidenceDigest, ResearchUseCatalogError> {
    let algorithm = match algorithm {
        1 => DigestAlgorithm::Sha256,
        2 => DigestAlgorithm::Blake3,
        _ => return Err(ResearchUseCatalogError::CorruptCatalog),
    };
    Ok(EvidenceDigest::new(algorithm, parse_digest(value)?))
}

pub(super) const fn digest_algorithm(algorithm: DigestAlgorithm) -> i64 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

const fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
}

pub(super) const fn research_use_mask(research_use: ResearchUse) -> i64 {
    match research_use {
        ResearchUse::Display => 1,
        ResearchUse::LocalAnalysis => 2,
        ResearchUse::Train => 4,
    }
}

pub(super) fn to_i64(value: u64) -> Result<i64, ResearchUseCatalogError> {
    i64::try_from(value).map_err(|_| ResearchUseCatalogError::LimitExceeded)
}

pub(super) fn to_i64_usize(value: usize) -> Result<i64, ResearchUseCatalogError> {
    i64::try_from(value).map_err(|_| ResearchUseCatalogError::LimitExceeded)
}

pub(super) fn duration_nanos(value: std::time::Duration) -> Result<i64, ResearchUseCatalogError> {
    i64::try_from(value.as_nanos()).map_err(|_| ResearchUseCatalogError::LimitExceeded)
}

pub(super) fn positive_u64(value: i64) -> Result<u64, ResearchUseCatalogError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(ResearchUseCatalogError::CorruptCatalog)
}

pub(super) fn append_audit(
    transaction: &Transaction<'_>,
    event_type: &str,
    subject_id: &str,
    details_digest: [u8; 32],
    occurred_at: Timestamp,
) -> Result<(), ResearchUseCatalogError> {
    transaction.execute(
        "INSERT INTO audit_events(event_type, subject_id, details_digest, occurred_at_ns)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            event_type,
            subject_id,
            details_digest,
            occurred_at.unix_nanos()
        ],
    )?;
    Ok(())
}

pub(super) fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
