//! Versioned Arrow schema for canonical research observations.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use market_squawk_domain::{EvidenceDigest, SourceIdentifier};

pub(crate) const SCHEMA_VERSION_KEY: &str = "market_squawk.schema_version";
pub(crate) const DATASET_KEY: &str = "market_squawk.dataset";
pub(crate) const REQUEST_DIGEST_KEY: &str = "market_squawk.request_sha256";
pub(crate) const SCHEMA_NAME: &str = "market_squawk.research_observations";
pub(crate) const RESEARCH_RECORD_SCHEMA: &str = "market-squawk-research-v2";
pub(crate) const RESEARCH_SCHEMA_VERSION: u16 = 2;

pub(crate) fn research_schema(
    dataset: &SourceIdentifier,
    request_digest: EvidenceDigest,
) -> SchemaRef {
    // Arrow 58's canonical UTC constructor encodes the fixed offset as `+00:00`.
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));
    let fields = vec![
        Field::new("schema_version", DataType::UInt16, false),
        Field::new("request_sha256", DataType::Binary, false),
        Field::new("extraction_lineage_json", DataType::Binary, false),
        Field::new("observation_kind", DataType::Utf8, false),
        Field::new("source_id", DataType::Utf8, false),
        Field::new("instrument_id", DataType::Utf8, true),
        Field::new("venue_id", DataType::Utf8, true),
        Field::new("source_identifier", DataType::Utf8, false),
        Field::new("source_timestamp", timestamp.clone(), true),
        Field::new("received_at", timestamp.clone(), false),
        Field::new("available_at", timestamp.clone(), true),
        Field::new(
            "availability_reported_or_inferred_at",
            timestamp.clone(),
            true,
        ),
        Field::new("availability_kind", DataType::Utf8, false),
        Field::new("availability_evidence", DataType::Utf8, true),
        Field::new("availability_method", DataType::Utf8, true),
        Field::new("ingested_at", timestamp.clone(), false),
        Field::new("effective_precision", DataType::Utf8, false),
        Field::new("effective_at", timestamp.clone(), true),
        Field::new("effective_date", DataType::Date32, true),
        Field::new("effective_period_scheme", DataType::Utf8, true),
        Field::new("effective_period_year", DataType::UInt16, true),
        Field::new("effective_period_ordinal", DataType::UInt16, true),
        Field::new("effective_period_code", DataType::Utf8, true),
        Field::new("published_precision", DataType::Utf8, true),
        Field::new("published_at", timestamp.clone(), true),
        Field::new("published_date", DataType::Date32, true),
        Field::new("published_period_scheme", DataType::Utf8, true),
        Field::new("published_period_year", DataType::UInt16, true),
        Field::new("published_period_ordinal", DataType::UInt16, true),
        Field::new("published_period_code", DataType::Utf8, true),
        Field::new("revision", DataType::UInt32, false),
        Field::new("superseded_precision", DataType::Utf8, true),
        Field::new("superseded_at", timestamp, true),
        Field::new("superseded_date", DataType::Date32, true),
        Field::new("superseded_period_scheme", DataType::Utf8, true),
        Field::new("superseded_period_year", DataType::UInt16, true),
        Field::new("superseded_period_ordinal", DataType::UInt16, true),
        Field::new("superseded_period_code", DataType::Utf8, true),
        Field::new("quality", DataType::Utf8, false),
        Field::new("value_mantissa", DataType::Decimal128(38, 0), true),
        Field::new("value_scale", DataType::UInt8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("payload_sha256", DataType::Binary, false),
        Field::new("payload_json", DataType::Binary, false),
    ];
    let metadata = HashMap::from([
        ("market_squawk.schema".to_owned(), SCHEMA_NAME.to_owned()),
        (
            SCHEMA_VERSION_KEY.to_owned(),
            RESEARCH_SCHEMA_VERSION.to_string(),
        ),
        (DATASET_KEY.to_owned(), dataset.as_str().to_owned()),
        (
            REQUEST_DIGEST_KEY.to_owned(),
            encode_hex(request_digest.bytes()),
        ),
        (
            "market_squawk.decimal_encoding".to_owned(),
            "decimal128-mantissa-scale-column-v1".to_owned(),
        ),
        (
            "market_squawk.timestamp_timezone".to_owned(),
            "UTC".to_owned(),
        ),
    ]);
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

pub(crate) fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) fn decode_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = decode_nibble(pair[0])?
            .checked_mul(16)?
            .checked_add(decode_nibble(pair[1])?)?;
    }
    Some(bytes)
}

const fn decode_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
