//! Exact raw-record validation, capacity accounting, and semantic identity.

use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{ExtractionRecord, payload_matches_exact_evidence};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::archive::RawPortfolioRecord;
use crate::{PortfolioImportError, PortfolioImportLimits};

const RAW_RECORD_SCHEMA: &str = "market-squawk-portfolio-raw-v1";
const RAW_REFERENCE_PREFIX: &str = "portfolio-raw-";

pub(crate) fn validate_raw_record(record: &ExtractionRecord) -> Result<(), PortfolioImportError> {
    if record.schema().as_str() != RAW_RECORD_SCHEMA {
        return Err(PortfolioImportError::UnsupportedRecordSchema);
    }
    if !payload_matches_exact_evidence(record.payload(), record.evidence()) {
        return Err(PortfolioImportError::RawEvidenceMismatch);
    }
    Ok(())
}

pub(crate) fn validate_raw_capacity(
    records: &[RawPortfolioRecord],
    limits: PortfolioImportLimits,
) -> Result<(), PortfolioImportError> {
    if records.len() > limits.max_archive_records {
        return Err(PortfolioImportError::ArchiveRecordLimitExceeded {
            max: limits.max_archive_records,
        });
    }
    let raw_bytes = records.iter().try_fold(0_u64, |total, record| {
        let bytes = u64::try_from(record.bytes().len()).map_err(|_| {
            PortfolioImportError::ArchiveByteLimitExceeded {
                max: limits.max_archive_bytes,
            }
        })?;
        total
            .checked_add(bytes)
            .ok_or(PortfolioImportError::ArchiveByteLimitExceeded {
                max: limits.max_archive_bytes,
            })
    })?;
    if raw_bytes > limits.max_archive_bytes {
        return Err(PortfolioImportError::ArchiveByteLimitExceeded {
            max: limits.max_archive_bytes,
        });
    }
    Ok(())
}

pub(crate) fn raw_source_reference(
    record: &ExtractionRecord,
) -> Result<SourceIdentifier, PortfolioImportError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-raw-reference/v1");
    hash_text(&mut digest, record.source_id().as_str())?;
    hash_text(
        &mut digest,
        record.metadata_revision().as_source_identifier().as_str(),
    )?;
    hash_text(&mut digest, record.dataset().as_str())?;
    hash_text(&mut digest, record.object_id().as_str())?;
    hash_serialized(&mut digest, record.object_evidence())?;
    hash_text(&mut digest, record.schema().as_str())?;
    hash_serialized(&mut digest, record.evidence())?;
    hash_serialized(&mut digest, record.effective_time())?;
    hash_serialized(&mut digest, &record.published_time())?;
    hash_serialized(&mut digest, record.availability())?;
    hash_text(&mut digest, record.revision().as_str())?;
    hash_serialized(&mut digest, &record.superseded_time())?;
    hash_bytes(&mut digest, record.payload())?;
    let encoded = hex_lower(&digest.finalize());
    identifier(&format!("{RAW_REFERENCE_PREFIX}{encoded}"))
}

fn hash_serialized<T: Serialize>(
    digest: &mut Sha256,
    value: &T,
) -> Result<(), PortfolioImportError> {
    let bytes = serde_json::to_vec(value).map_err(|_| PortfolioImportError::InvalidRecord)?;
    hash_bytes(digest, &bytes)
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), PortfolioImportError> {
    hash_bytes(digest, value.as_bytes())
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), PortfolioImportError> {
    let length = u64::try_from(bytes.len()).map_err(|_| PortfolioImportError::InvalidRecord)?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn identifier(value: &str) -> Result<SourceIdentifier, PortfolioImportError> {
    SourceIdentifier::try_from(value).map_err(|_| PortfolioImportError::InvalidRecord)
}
