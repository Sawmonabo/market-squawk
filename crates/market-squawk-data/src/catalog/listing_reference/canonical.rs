use std::collections::BTreeSet;
use std::mem::size_of;
use std::time::Instant;

use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence};
use market_squawk_sources::SourceMetadata;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    ListingReferenceError, ListingReferenceFileKind, ListingReferenceGenerationInput,
    ListingReferenceRecordInput, ListingReferenceSourceFileInput, MAX_FILE_RECORDS,
    MAX_LISTING_REFERENCE_RECORDS, MAX_RETAINED_INPUT_BYTES,
};

const CANONICAL_DOMAIN: &[u8] = b"market-squawk/listing-reference/v1";

pub(super) fn valid_file_creation_time(value: &str) -> bool {
    if value.len() != 13 {
        return false;
    }
    if value.as_bytes().get(10) != Some(&b':')
        || !value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 10 || byte.is_ascii_digit())
    {
        return false;
    }
    let Ok(month) = value[0..2].parse::<u8>() else {
        return false;
    };
    let Ok(day) = value[2..4].parse::<u8>() else {
        return false;
    };
    let Ok(year) = value[4..8].parse::<u16>() else {
        return false;
    };
    CalendarDate::new(year, month, day).is_ok()
        && value[8..10].parse::<u8>().is_ok_and(|hour| hour <= 23)
        && value[11..13].parse::<u8>().is_ok_and(|minute| minute <= 59)
}

pub(super) fn validate_record(
    kind: ListingReferenceFileKind,
    record: &ListingReferenceRecordInput,
) -> Result<(), ListingReferenceError> {
    let valid_symbol = |value: &str| {
        !value.is_empty()
            && value.len() <= 14
            && value
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && byte != b'|')
    };
    let normalized_name = normalize_name(&record.security_name);
    if !(2..=32_769).contains(&record.provider_row_number)
        || !valid_symbol(&record.provider_symbol)
        || record.security_name.len() > 255
        || record.security_name.trim().is_empty()
        || record.security_name.chars().any(char::is_control)
        || record.security_name.contains('|')
        || normalized_name.is_empty()
        || normalized_name.len() > 255
        || !(1..=999_999).contains(&record.round_lot_size)
        || !valid_file_creation_time(&record.source_file_creation_time)
        || record.source_last_modified_at > record.first_observed_at
        || record.record_payload_evidence.content_digest().bytes() == [0; 32]
        || record.source_file_payload_evidence.content_digest().bytes() == [0; 32]
    {
        return Err(ListingReferenceError::InvalidInput);
    }
    let valid = match kind {
        ListingReferenceFileKind::NasdaqListed => {
            record.listing_venue.as_str() == "XNAS"
                && record.exchange_code.is_none()
                && record.cqs_symbol.is_none()
                && record.nasdaq_symbol.is_none()
                && record.market_category.is_some()
                && record.financial_status.is_some()
                && record.is_next_shares.is_some()
        }
        ListingReferenceFileKind::OtherListed => {
            record
                .exchange_code
                .is_some_and(|exchange| record.listing_venue.as_str() == exchange.expected_venue())
                && record.cqs_symbol.as_deref().is_some_and(valid_symbol)
                && record.nasdaq_symbol.as_deref().is_some_and(valid_symbol)
                && record.market_category.is_none()
                && record.financial_status.is_none()
                && record.is_next_shares.is_none()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ListingReferenceError::InvalidInput)
    }
}

pub(super) fn validate_and_order_generation(
    files: &[ListingReferenceSourceFileInput],
    records: &mut [(ListingReferenceFileKind, ListingReferenceRecordInput)],
) -> Result<(), ListingReferenceError> {
    if files.len() != 2
        || files[0].kind == files[1].kind
        || files[0].source_object_id == files[1].source_object_id
        || records.len() < 2
        || records.len() > MAX_LISTING_REFERENCE_RECORDS
    {
        return Err(ListingReferenceError::InvalidInput);
    }
    records.sort_by(|left, right| {
        (left.0, left.1.provider_row_number, &left.1.provider_symbol).cmp(&(
            right.0,
            right.1.provider_row_number,
            &right.1.provider_symbol,
        ))
    });
    let mut revisions = BTreeSet::new();
    for kind in [
        ListingReferenceFileKind::NasdaqListed,
        ListingReferenceFileKind::OtherListed,
    ] {
        let file = files
            .iter()
            .find(|file| file.kind == kind)
            .ok_or(ListingReferenceError::InvalidInput)?;
        let matching: Vec<_> = records
            .iter()
            .filter(|(record_kind, _)| *record_kind == kind)
            .collect();
        if matching.is_empty() || matching.len() > MAX_FILE_RECORDS {
            return Err(ListingReferenceError::InvalidInput);
        }
        let mut symbols = BTreeSet::new();
        for (index, (_, record)) in matching.into_iter().enumerate() {
            validate_record(kind, record)?;
            let expected_row = u32::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(2))
                .ok_or(ListingReferenceError::InvalidInput)?;
            if record.provider_row_number != expected_row
                || record.source_file_creation_time != file.file_creation_time
                || record.source_last_modified_at != file.source_last_modified_at
                || record.first_observed_at != file.received_at
                || record.first_observed_at != file.available_at
                || record.source_file_payload_evidence != file.payload_evidence
                || !symbols.insert(record.provider_symbol.as_str())
                || !revisions.insert(record.record_revision.as_str())
            {
                return Err(ListingReferenceError::InvalidInput);
            }
        }
    }
    Ok(())
}

pub(super) fn enforce_retained_input_bound(
    source: &SourceMetadata,
    files: &[ListingReferenceSourceFileInput],
    records: &[(ListingReferenceFileKind, ListingReferenceRecordInput)],
) -> Result<(), ListingReferenceError> {
    let source_bytes = serde_json::to_vec(source)?.len();
    let file_bytes = files.iter().try_fold(0_usize, |total, file| {
        checked_sum([
            total,
            size_of::<ListingReferenceSourceFileInput>(),
            file.source_object_id.as_str().len(),
            file.source_reference.as_str().len(),
            file.file_creation_time.len(),
            evidence_retained_bytes(&file.payload_evidence)?,
        ])
    })?;
    let record_bytes = records.iter().try_fold(0_usize, |total, (_, record)| {
        checked_sum([
            total,
            size_of::<ListingReferenceRecordInput>(),
            record.provider_symbol.len(),
            record.security_name.len(),
            record.cqs_symbol.as_ref().map_or(0, String::len),
            record.nasdaq_symbol.as_ref().map_or(0, String::len),
            record.record_revision.as_str().len(),
            record.source_file_creation_time.len(),
            evidence_retained_bytes(&record.record_payload_evidence)?,
            evidence_retained_bytes(&record.source_file_payload_evidence)?,
        ])
    })?;
    let retained = checked_sum([source_bytes, file_bytes, record_bytes])?;
    if retained > MAX_RETAINED_INPUT_BYTES {
        Err(ListingReferenceError::MemoryLimitExceeded)
    } else {
        Ok(())
    }
}

fn evidence_retained_bytes(
    evidence: &ExactPayloadEvidence,
) -> Result<usize, ListingReferenceError> {
    evidence
        .dynamic_retained_bytes()
        .and_then(|dynamic| dynamic.checked_add(size_of::<ExactPayloadEvidence>()))
        .ok_or(ListingReferenceError::MemoryLimitExceeded)
}

fn checked_sum<const N: usize>(values: [usize; N]) -> Result<usize, ListingReferenceError> {
    values
        .into_iter()
        .try_fold(0_usize, usize::checked_add)
        .ok_or(ListingReferenceError::MemoryLimitExceeded)
}

pub(super) fn normalize_symbol(value: &str) -> String {
    value.to_ascii_uppercase()
}

pub(super) fn normalize_name(value: &str) -> String {
    value.trim().to_lowercase()
}

pub(super) fn value_digest(record: &ListingReferenceRecordInput) -> [u8; 32] {
    let mut hash = canonical_hasher(b"value");
    hash_field(
        &mut hash,
        b"provider_symbol",
        record.provider_symbol.as_bytes(),
    );
    hash_field(&mut hash, b"security_name", record.security_name.as_bytes());
    hash_field(
        &mut hash,
        b"listing_venue",
        record.listing_venue.as_str().as_bytes(),
    );
    hash_optional_text(
        &mut hash,
        b"exchange_code",
        record.exchange_code.map(|value| value.database_name()),
    );
    hash_optional_text(&mut hash, b"cqs_symbol", record.cqs_symbol.as_deref());
    hash_optional_text(&mut hash, b"nasdaq_symbol", record.nasdaq_symbol.as_deref());
    hash_optional_text(
        &mut hash,
        b"market_category",
        record.market_category.map(|value| value.database_name()),
    );
    hash_optional_text(
        &mut hash,
        b"financial_status",
        record.financial_status.map(|value| value.database_name()),
    );
    hash_field(&mut hash, b"is_etf", &[u8::from(record.is_etf)]);
    hash_field(
        &mut hash,
        b"is_test_issue",
        &[u8::from(record.is_test_issue)],
    );
    hash_field(
        &mut hash,
        b"round_lot_size",
        &record.round_lot_size.to_be_bytes(),
    );
    hash_optional_bool(&mut hash, b"is_next_shares", record.is_next_shares);
    hash.finalize().into()
}

pub(super) fn record_digest(
    kind: ListingReferenceFileKind,
    record: &ListingReferenceRecordInput,
    value_digest: [u8; 32],
) -> [u8; 32] {
    let mut hash = canonical_hasher(b"membership");
    hash_field(&mut hash, b"file_kind", kind.database_name().as_bytes());
    hash_field(
        &mut hash,
        b"provider_row_number",
        &record.provider_row_number.to_be_bytes(),
    );
    hash_field(
        &mut hash,
        b"record_revision",
        record.record_revision.as_str().as_bytes(),
    );
    hash_evidence(
        &mut hash,
        b"record_payload",
        &record.record_payload_evidence,
    );
    hash_field(&mut hash, b"value_digest", &value_digest);
    hash.finalize().into()
}

pub(super) fn records_digest(
    records: &[(ListingReferenceFileKind, ListingReferenceRecordInput)],
) -> [u8; 32] {
    let mut hash = canonical_hasher(b"records");
    hash_field(
        &mut hash,
        b"record_count",
        &(records.len() as u64).to_be_bytes(),
    );
    for (kind, record) in records {
        let value = value_digest(record);
        // Generation identity follows the immutable provider object and provider row. The
        // normalized record payload also carries local first-observed time, so binding its digest
        // here would turn an unchanged post-restart refetch into a false successor generation.
        // The separately stored membership digest still binds that exact normalized payload.
        let mut membership = canonical_hasher(b"generation-membership");
        hash_field(
            &mut membership,
            b"file_kind",
            kind.database_name().as_bytes(),
        );
        hash_field(
            &mut membership,
            b"provider_row_number",
            &record.provider_row_number.to_be_bytes(),
        );
        hash_field(
            &mut membership,
            b"record_revision",
            record.record_revision.as_str().as_bytes(),
        );
        hash_field(&mut membership, b"value_digest", &value);
        hash_field(&mut hash, b"record", &membership.finalize());
    }
    hash.finalize().into()
}

pub(super) fn source_payload_set_digest(files: &[ListingReferenceSourceFileInput]) -> [u8; 32] {
    let mut hash = canonical_hasher(b"source-payload-set");
    hash_field(
        &mut hash,
        b"file_count",
        &(files.len() as u64).to_be_bytes(),
    );
    for file in files {
        hash_field(
            &mut hash,
            b"file_kind",
            file.kind.database_name().as_bytes(),
        );
        hash_field(
            &mut hash,
            b"source_object_id",
            file.source_object_id.as_str().as_bytes(),
        );
        hash_field(
            &mut hash,
            b"source_reference",
            file.source_reference.as_str().as_bytes(),
        );
        hash_evidence(&mut hash, b"payload", &file.payload_evidence);
    }
    hash.finalize().into()
}

pub(super) fn generation_digest(
    dataset: &market_squawk_domain::SourceIdentifier,
    input: &ListingReferenceGenerationInput,
    source_revision_digest: [u8; 32],
    rights_id: [u8; 32],
    records_digest: [u8; 32],
) -> [u8; 32] {
    generation_digest_parts(
        dataset,
        input.source.source_id().as_str(),
        input.source.revision().as_source_identifier().as_str(),
        source_revision_digest,
        rights_id,
        &input.files,
        records_digest,
    )
}

pub(super) fn generation_digest_parts(
    dataset: &market_squawk_domain::SourceIdentifier,
    source_id: &str,
    source_revision: &str,
    source_revision_digest: [u8; 32],
    rights_id: [u8; 32],
    files: &[ListingReferenceSourceFileInput],
    records_digest: [u8; 32],
) -> [u8; 32] {
    let mut hash = canonical_hasher(b"generation");
    hash_field(&mut hash, b"dataset", dataset.as_str().as_bytes());
    hash_field(&mut hash, b"source_id", source_id.as_bytes());
    hash_field(&mut hash, b"source_revision", source_revision.as_bytes());
    hash_field(
        &mut hash,
        b"source_revision_digest",
        &source_revision_digest,
    );
    hash_field(&mut hash, b"rights_id", &rights_id);
    for file in files {
        hash_field(
            &mut hash,
            b"file_kind",
            file.kind.database_name().as_bytes(),
        );
        hash_field(
            &mut hash,
            b"source_object_id",
            file.source_object_id.as_str().as_bytes(),
        );
        hash_field(
            &mut hash,
            b"source_reference",
            file.source_reference.as_str().as_bytes(),
        );
        hash_field(
            &mut hash,
            b"file_creation_time",
            file.file_creation_time.as_bytes(),
        );
        hash_evidence(&mut hash, b"file_payload", &file.payload_evidence);
        hash_field(
            &mut hash,
            b"source_last_modified_at",
            &file.source_last_modified_at.unix_nanos().to_be_bytes(),
        );
    }
    hash_field(&mut hash, b"records_digest", &records_digest);
    hash.finalize().into()
}

pub(super) fn evidence_columns(
    evidence: &ExactPayloadEvidence,
) -> (i64, [u8; 32], Option<&str>, Option<&str>) {
    let digest = evidence.content_digest();
    let algorithm = match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    };
    let locator = evidence.version_pinned_locator();
    (
        algorithm,
        digest.bytes(),
        locator.map(|value| value.reference().as_str()),
        locator.map(|value| value.version().as_str()),
    )
}

pub(super) fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ListingReferenceError> {
    if cancellation.is_cancelled() {
        Err(ListingReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ListingReferenceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn canonical_hasher(kind: &[u8]) -> Sha256 {
    let mut hash = Sha256::new();
    hash_field(&mut hash, b"domain", CANONICAL_DOMAIN);
    hash_field(&mut hash, b"kind", kind);
    hash
}

fn hash_evidence(hash: &mut Sha256, tag: &[u8], evidence: &ExactPayloadEvidence) {
    let digest = evidence.content_digest();
    hash_field(hash, tag, b"exact-payload-evidence/v1");
    hash_field(
        hash,
        b"algorithm",
        &[match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        }],
    );
    hash_field(hash, b"digest", &digest.bytes());
    if let Some(locator) = evidence.version_pinned_locator() {
        hash_field(hash, b"locator", &[1]);
        hash_field(
            hash,
            b"locator_reference",
            locator.reference().as_str().as_bytes(),
        );
        hash_field(
            hash,
            b"locator_version",
            locator.version().as_str().as_bytes(),
        );
    } else {
        hash_field(hash, b"locator", &[0]);
    }
}

fn hash_optional_text(hash: &mut Sha256, tag: &[u8], value: Option<&str>) {
    if let Some(value) = value {
        hash_field(hash, tag, &[1]);
        hash_field(hash, b"value", value.as_bytes());
    } else {
        hash_field(hash, tag, &[0]);
    }
}

fn hash_optional_bool(hash: &mut Sha256, tag: &[u8], value: Option<bool>) {
    hash_field(
        hash,
        tag,
        &[match value {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        }],
    );
}

fn hash_field(hash: &mut Sha256, tag: &[u8], value: &[u8]) {
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

pub(super) fn digest(value: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, value)
}
