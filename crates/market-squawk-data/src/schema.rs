//! Closed, versioned Arrow dataset-schema registry.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use market_squawk_domain::{EvidenceDigest, ResearchObservation, SchemaVersion, SourceIdentifier};
use market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

pub(crate) const SCHEMA_NAME_KEY: &str = "market_squawk.schema";
pub(crate) const SCHEMA_VERSION_KEY: &str = "market_squawk.schema_version";
pub(crate) const SCHEMA_FINGERPRINT_KEY: &str = "market_squawk.schema_fingerprint_sha256";
pub(crate) const RESEARCH_PAYLOAD_CONTRACT_KEY: &str =
    "market_squawk.research_payload_contract_sha256";
pub(crate) const DATASET_KEY: &str = "market_squawk.dataset";
pub(crate) const REQUEST_DIGEST_KEY: &str = "market_squawk.request_sha256";
pub(crate) const BUILD_DIGEST_KEY: &str = "market_squawk.build_sha256";
pub(crate) const UNIVERSE_DIGEST_KEY: &str = "market_squawk.universe_sha256";
pub(crate) const POLICY_DIGEST_KEY: &str = "market_squawk.policy_sha256";
pub(crate) const RESEARCH_SCHEMA_NAME: &str = "market_squawk.research_observations";
pub(crate) const FEATURE_LABEL_SCHEMA_NAME: &str = "market_squawk.feature_label_components";
pub(crate) const RESEARCH_RECORD_SCHEMA: &str = CURRENT_RESEARCH_RECORD_SCHEMA;
pub(crate) const RESEARCH_SCHEMA_VERSION: u16 = 3;
pub(crate) const FEATURE_LABEL_SCHEMA_VERSION: u16 = 3;
pub(crate) const FEATURE_LABEL_EXAMPLE_ID_BYTES: i32 = 256;
pub(crate) const FEATURE_LABEL_INSTRUMENT_ID_BYTES: i32 = 16;
pub(crate) const FEATURE_LABEL_COMPONENT_NAME_BYTES: i32 = 256;
pub(crate) const FEATURE_LABEL_UNIT_BYTES: i32 = 32;
pub(crate) const FEATURE_LABEL_CURRENCY_BYTES: i32 = 3;
pub(crate) const FEATURE_LABEL_MISSING_REASON_BYTES: i32 = 256;

const MAX_SCHEMA_NAME_BYTES: usize = 128;

const RESEARCH_PAYLOAD_CONTRACT_DOMAIN: &[u8] =
    b"market-squawk/research-observation-payload-contract/v1";
const RESEARCH_PAYLOAD_ENVELOPE_CONTRACT: &[u8] = b"serde-json:adjacently-tagged;tag=observation;content=payload;rename=snake_case;deny_unknown_fields=true|semantic:MSQPIT;identity_schema=2;domain=market-squawk/pit/payload;integers=little-endian;bytes=u64-length-prefixed;decimal=normalized-i128-mantissa+u32-scale";

/// One closed JSON discriminator and its canonical PIT semantic encoder identity.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ResearchPayloadContractEntry {
    json_tag: &'static str,
    semantic_tag: u8,
    semantic_encoder: &'static str,
}

impl ResearchPayloadContractEntry {
    /// Returns the exact serde discriminator and Arrow observation-kind label.
    pub(crate) const fn json_tag(self) -> &'static str {
        self.json_tag
    }

    /// Returns the first variant byte written by the canonical semantic payload encoder.
    pub(crate) const fn semantic_tag(self) -> u8 {
        self.semantic_tag
    }
}

const FILING_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "filing",
    semantic_tag: 1,
    semantic_encoder: "filing-v1:form_type,accession",
};
const FUNDAMENTAL_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "fundamental",
    semantic_tag: 2,
    semantic_encoder: "fundamental-v2:concept,normalized_decimal,fact_context,optional_xbrl_evidence",
};
const MACRO_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "macro",
    semantic_tag: 3,
    semantic_encoder: "macro-v1:series,value_state(observed=1|missing=2),value_or_marker_reason,unit",
};
const MARKET_BAR_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "market_bar",
    semantic_tag: 10,
    semantic_encoder: "market_bar-v1:provider_instrument,feed,interval,adjustment(raw=1|split=2|dividend=3|spin_off=4|all=5),period_start,period_end,timestamp_basis(start=1|end=2),session(kind=1..4,ruleset,evidence_algorithm,evidence),ohlc,currency,volume,optional_trade_count,optional_vwap",
};
const FUND_NAV_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "fund_nav",
    semantic_tag: 11,
    semantic_encoder: "fund_nav-v1:provider_instrument,instrument_reference_revision,provider_product,provider_channel,nav_date,valuation_basis(per_share=1),currency,value_state(observed=1|missing=2),money_or_missing_reason(1..5),canonical_published_at,native_schema+entitlement+request+raw_object+raw_row+page+checkpoint+completeness+disposition_lineage,source_revision+correction+finality+predecessor+successor_evidence",
};
const PORTFOLIO_POSITION_PAYLOAD_CONTRACT: ResearchPayloadContractEntry =
    ResearchPayloadContractEntry {
        json_tag: "portfolio_position",
        semantic_tag: 4,
        semantic_encoder: "portfolio_position-v1:account,side(long=1|short=2),absolute_quantity",
    };
const TRANSACTION_PAYLOAD_CONTRACT: ResearchPayloadContractEntry = ResearchPayloadContractEntry {
    json_tag: "transaction",
    semantic_tag: 5,
    semantic_encoder: "transaction-v1:account,transaction_type,source_record_id",
};
const CORPORATE_ACTION_PAYLOAD_CONTRACT: ResearchPayloadContractEntry =
    ResearchPayloadContractEntry {
        json_tag: "corporate_action",
        semantic_tag: 6,
        semantic_encoder: "corporate_action-v1:canonical_serde_action",
    };
const UNIVERSE_MEMBERSHIP_PAYLOAD_CONTRACT: ResearchPayloadContractEntry =
    ResearchPayloadContractEntry {
        json_tag: "universe_membership",
        semantic_tag: 8,
        semantic_encoder: "universe_membership-v1:universe,start,optional_end",
    };
const ALTERNATIVE_DATA_PAYLOAD_CONTRACT: ResearchPayloadContractEntry =
    ResearchPayloadContractEntry {
        json_tag: "alternative_data",
        semantic_tag: 7,
        semantic_encoder: "alternative_data-v1:dataset,field,normalized_decimal,optional_unit",
    };

const RESEARCH_PAYLOAD_CONTRACT: [ResearchPayloadContractEntry; 10] = [
    FILING_PAYLOAD_CONTRACT,
    FUNDAMENTAL_PAYLOAD_CONTRACT,
    MACRO_PAYLOAD_CONTRACT,
    MARKET_BAR_PAYLOAD_CONTRACT,
    FUND_NAV_PAYLOAD_CONTRACT,
    PORTFOLIO_POSITION_PAYLOAD_CONTRACT,
    TRANSACTION_PAYLOAD_CONTRACT,
    CORPORATE_ACTION_PAYLOAD_CONTRACT,
    UNIVERSE_MEMBERSHIP_PAYLOAD_CONTRACT,
    ALTERNATIVE_DATA_PAYLOAD_CONTRACT,
];

/// Selects the sole registered payload-contract entry for a typed observation.
pub(crate) const fn research_payload_contract_for(
    observation: &ResearchObservation,
) -> ResearchPayloadContractEntry {
    match observation {
        ResearchObservation::Filing(_) => FILING_PAYLOAD_CONTRACT,
        ResearchObservation::Fundamental(_) => FUNDAMENTAL_PAYLOAD_CONTRACT,
        ResearchObservation::Macro(_) => MACRO_PAYLOAD_CONTRACT,
        ResearchObservation::MarketBar(_) => MARKET_BAR_PAYLOAD_CONTRACT,
        ResearchObservation::FundNav(_) => FUND_NAV_PAYLOAD_CONTRACT,
        ResearchObservation::PortfolioPosition(_) => PORTFOLIO_POSITION_PAYLOAD_CONTRACT,
        ResearchObservation::Transaction(_) => TRANSACTION_PAYLOAD_CONTRACT,
        ResearchObservation::CorporateAction(_) => CORPORATE_ACTION_PAYLOAD_CONTRACT,
        ResearchObservation::UniverseMembership(_) => UNIVERSE_MEMBERSHIP_PAYLOAD_CONTRACT,
        ResearchObservation::AlternativeData(_) => ALTERNATIVE_DATA_PAYLOAD_CONTRACT,
    }
}

/// Exact immutable identity of one registered Arrow dataset schema.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatasetSchemaRef {
    name: Box<str>,
    version: SchemaVersion,
    fingerprint: [u8; 32],
}

impl DatasetSchemaRef {
    /// Constructs a bounded schema identity without claiming it is locally registered.
    ///
    /// Call [`DatasetSchemaRegistry::resolve`] before trusting caller- or storage-provided
    /// identities. Keeping construction distinct from resolution lets readers reject a retained
    /// fingerprint without replacing it with a locally computed value.
    pub fn try_new(
        name: impl AsRef<str>,
        version: SchemaVersion,
        fingerprint: [u8; 32],
    ) -> Result<Self, DatasetSchemaError> {
        let name = name.as_ref();
        if name.is_empty()
            || name.len() > MAX_SCHEMA_NAME_BYTES
            || name.bytes().any(|byte| {
                !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_'))
            })
        {
            return Err(DatasetSchemaError::InvalidName);
        }
        Ok(Self {
            name: name.into(),
            version,
            fingerprint,
        })
    }

    /// Returns the stable schema name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the nonzero schema version scoped by [`Self::name`].
    pub const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Returns the SHA-256 fingerprint of the exact canonical Arrow schema representation.
    pub const fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }
}

impl fmt::Debug for DatasetSchemaRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatasetSchemaRef")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("fingerprint", &encode_hex(self.fingerprint))
            .finish()
    }
}

/// Closed process-local registry of schemas this release can interpret.
///
/// Adding a schema is an explicit source change and migration event. Callers cannot register an
/// arbitrary Arrow layout at runtime or reinterpret an unknown retained identity.
#[derive(Clone, Copy, Debug, Default)]
pub struct DatasetSchemaRegistry {
    _private: (),
}

/// Exact non-schema bindings required on every feature/label component batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureLabelBatchBindings {
    dataset: SourceIdentifier,
    build_digest: [u8; 32],
    universe_digest: [u8; 32],
    policy_digest: [u8; 32],
}

impl FeatureLabelBatchBindings {
    /// Constructs complete feature/label publication bindings.
    pub const fn new(
        dataset: SourceIdentifier,
        build_digest: [u8; 32],
        universe_digest: [u8; 32],
        policy_digest: [u8; 32],
    ) -> Self {
        Self {
            dataset,
            build_digest,
            universe_digest,
            policy_digest,
        }
    }
}

impl DatasetSchemaRegistry {
    /// Returns this release's immutable local registry.
    pub const fn local() -> Self {
        Self { _private: () }
    }

    /// Returns the exact canonical research-observation identity.
    pub fn canonical_research_observations(self) -> Result<DatasetSchemaRef, DatasetSchemaError> {
        identity_for_schema(RESEARCH_SCHEMA_NAME, research_schema_definition()?)
    }

    /// Returns the exact typed long-form feature/label component identity.
    pub fn canonical_feature_labels(self) -> Result<DatasetSchemaRef, DatasetSchemaError> {
        identity_for_schema(FEATURE_LABEL_SCHEMA_NAME, feature_label_schema_definition())
    }

    /// Resolves an exact known identity to its canonical Arrow schema.
    ///
    /// Unknown names or versions and known names with altered fingerprints fail closed.
    pub fn resolve(self, schema_ref: &DatasetSchemaRef) -> Result<SchemaRef, DatasetSchemaError> {
        let schema = match (schema_ref.name(), schema_ref.version().get()) {
            (RESEARCH_SCHEMA_NAME, RESEARCH_SCHEMA_VERSION) => research_schema_definition()?,
            (FEATURE_LABEL_SCHEMA_NAME, FEATURE_LABEL_SCHEMA_VERSION) => {
                feature_label_schema_definition()
            }
            _ => return Err(DatasetSchemaError::UnknownIdentity),
        };
        let expected = identity_for_schema(schema_ref.name(), schema.clone())?;
        if expected != *schema_ref {
            return Err(DatasetSchemaError::FingerprintMismatch);
        }
        Ok(Arc::new(schema_with_fingerprint(schema, schema_ref)))
    }

    /// Adds the complete validated publication bindings to the feature/label field schema.
    pub fn bind_feature_labels(
        self,
        schema_ref: &DatasetSchemaRef,
        bindings: &FeatureLabelBatchBindings,
    ) -> Result<SchemaRef, DatasetSchemaError> {
        if schema_ref.name() != FEATURE_LABEL_SCHEMA_NAME {
            return Err(DatasetSchemaError::UnknownIdentity);
        }
        let schema = self.resolve(schema_ref)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(DATASET_KEY.to_owned(), bindings.dataset.as_str().to_owned());
        metadata.insert(
            BUILD_DIGEST_KEY.to_owned(),
            encode_hex(bindings.build_digest),
        );
        metadata.insert(
            UNIVERSE_DIGEST_KEY.to_owned(),
            encode_hex(bindings.universe_digest),
        );
        metadata.insert(
            POLICY_DIGEST_KEY.to_owned(),
            encode_hex(bindings.policy_digest),
        );
        Ok(Arc::new(Schema::new_with_metadata(
            schema.fields().clone(),
            metadata,
        )))
    }
}

/// Dataset-schema construction or resolution failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DatasetSchemaError {
    /// A schema name is empty, oversized, or outside the stable lowercase identifier grammar.
    #[error("dataset schema name is invalid")]
    InvalidName,
    /// The name and version pair is not compiled into this release's closed registry.
    #[error("dataset schema identity is not registered")]
    UnknownIdentity,
    /// A known name and version carry a different Arrow-schema fingerprint.
    #[error("dataset Arrow schema fingerprint does not match the registered identity")]
    FingerprintMismatch,
    /// A compiled schema uses an Arrow type absent from the stable fingerprint encoding.
    #[error("dataset Arrow schema contains an unsupported fingerprint type")]
    UnsupportedArrowType,
}

pub(crate) fn research_schema(
    dataset: &SourceIdentifier,
    request_digest: EvidenceDigest,
) -> Result<SchemaRef, DatasetSchemaError> {
    let registry = DatasetSchemaRegistry::local();
    let schema_ref = registry.canonical_research_observations()?;
    let schema = registry.resolve(&schema_ref)?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(DATASET_KEY.to_owned(), dataset.as_str().to_owned());
    metadata.insert(
        REQUEST_DIGEST_KEY.to_owned(),
        encode_hex(request_digest.bytes()),
    );
    Ok(Arc::new(Schema::new_with_metadata(
        schema.fields().clone(),
        metadata,
    )))
}

pub(crate) fn schema_ref_from_metadata(
    schema: &Schema,
) -> Result<DatasetSchemaRef, DatasetSchemaError> {
    let name = schema
        .metadata()
        .get(SCHEMA_NAME_KEY)
        .ok_or(DatasetSchemaError::UnknownIdentity)?;
    let version = schema
        .metadata()
        .get(SCHEMA_VERSION_KEY)
        .and_then(|value| value.parse::<u16>().ok())
        .and_then(|value| SchemaVersion::new(value).ok())
        .ok_or(DatasetSchemaError::UnknownIdentity)?;
    let fingerprint = schema
        .metadata()
        .get(SCHEMA_FINGERPRINT_KEY)
        .and_then(|value| decode_hex(value))
        .ok_or(DatasetSchemaError::FingerprintMismatch)?;
    if name == RESEARCH_SCHEMA_NAME {
        let expected_payload_contract = encode_hex(research_payload_contract_digest()?);
        if schema
            .metadata()
            .get(RESEARCH_PAYLOAD_CONTRACT_KEY)
            .map(String::as_str)
            != Some(expected_payload_contract.as_str())
        {
            return Err(DatasetSchemaError::FingerprintMismatch);
        }
    }
    DatasetSchemaRef::try_new(name, version, fingerprint)
}

fn research_schema_definition() -> Result<Schema, DatasetSchemaError> {
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
        Field::new("macro_series", DataType::Utf8, true),
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
        Field::new("value_state", DataType::Utf8, false),
        Field::new("missing_marker", DataType::Utf8, true),
        Field::new("missing_reason", DataType::Utf8, true),
        Field::new("value_mantissa", DataType::Decimal128(38, 0), true),
        Field::new("value_scale", DataType::UInt8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("currency", DataType::Utf8, true),
        Field::new("payload_sha256", DataType::Binary, false),
        Field::new("payload_json", DataType::Binary, false),
    ];
    Ok(Schema::new_with_metadata(
        fields,
        HashMap::from([
            (SCHEMA_NAME_KEY.to_owned(), RESEARCH_SCHEMA_NAME.to_owned()),
            (
                SCHEMA_VERSION_KEY.to_owned(),
                RESEARCH_SCHEMA_VERSION.to_string(),
            ),
            (
                "market_squawk.decimal_encoding".to_owned(),
                "decimal128-mantissa-scale-column-v1".to_owned(),
            ),
            (
                "market_squawk.timestamp_timezone".to_owned(),
                "UTC".to_owned(),
            ),
            (
                RESEARCH_PAYLOAD_CONTRACT_KEY.to_owned(),
                encode_hex(research_payload_contract_digest()?),
            ),
        ]),
    ))
}

fn feature_label_schema_definition() -> Schema {
    let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));
    Schema::new_with_metadata(
        vec![
            Field::new(
                "example_id",
                DataType::FixedSizeBinary(FEATURE_LABEL_EXAMPLE_ID_BYTES),
                false,
            ),
            Field::new(
                "instrument_id",
                DataType::FixedSizeBinary(FEATURE_LABEL_INSTRUMENT_ID_BYTES),
                false,
            ),
            Field::new("cutoff_at", timestamp.clone(), false),
            Field::new("observed_effective_at", timestamp.clone(), true),
            Field::new("label_effective_at", timestamp, true),
            Field::new("target_coordinate_kind", DataType::UInt8, false),
            Field::new("split", DataType::UInt8, false),
            Field::new("component_kind", DataType::UInt8, false),
            Field::new(
                "component_name",
                DataType::FixedSizeBinary(FEATURE_LABEL_COMPONENT_NAME_BYTES),
                false,
            ),
            Field::new("component_version", DataType::UInt32, false),
            Field::new("value_f64", DataType::Float64, true),
            Field::new("value_decimal_mantissa", DataType::Decimal128(38, 0), true),
            Field::new("value_decimal_scale", DataType::UInt8, true),
            Field::new(
                "unit",
                DataType::FixedSizeBinary(FEATURE_LABEL_UNIT_BYTES),
                true,
            ),
            Field::new(
                "currency",
                DataType::FixedSizeBinary(FEATURE_LABEL_CURRENCY_BYTES),
                true,
            ),
            Field::new(
                "missing_reason",
                DataType::FixedSizeBinary(FEATURE_LABEL_MISSING_REASON_BYTES),
                true,
            ),
            Field::new("lineage_sha256", DataType::FixedSizeBinary(32), false),
        ],
        HashMap::from([
            (
                SCHEMA_NAME_KEY.to_owned(),
                FEATURE_LABEL_SCHEMA_NAME.to_owned(),
            ),
            (
                SCHEMA_VERSION_KEY.to_owned(),
                FEATURE_LABEL_SCHEMA_VERSION.to_string(),
            ),
            (
                "market_squawk.component_layout".to_owned(),
                "fixed-width-long-form-v3".to_owned(),
            ),
            (
                "market_squawk.timestamp_timezone".to_owned(),
                "UTC".to_owned(),
            ),
        ]),
    )
}

fn identity_for_schema(name: &str, schema: Schema) -> Result<DatasetSchemaRef, DatasetSchemaError> {
    if schema.metadata().get(SCHEMA_NAME_KEY).map(String::as_str) != Some(name) {
        return Err(DatasetSchemaError::UnknownIdentity);
    }
    let version = schema
        .metadata()
        .get(SCHEMA_VERSION_KEY)
        .and_then(|value| value.parse::<u16>().ok())
        .and_then(|value| SchemaVersion::new(value).ok())
        .ok_or(DatasetSchemaError::UnknownIdentity)?;
    DatasetSchemaRef::try_new(name, version, fingerprint_schema(&schema)?)
}

fn schema_with_fingerprint(schema: Schema, schema_ref: &DatasetSchemaRef) -> Schema {
    let mut metadata = schema.metadata().clone();
    metadata.insert(
        SCHEMA_FINGERPRINT_KEY.to_owned(),
        encode_hex(schema_ref.fingerprint()),
    );
    schema.with_metadata(metadata)
}

fn fingerprint_schema(schema: &Schema) -> Result<[u8; 32], DatasetSchemaError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/arrow-dataset-schema/v1");
    update_length(&mut digest, schema.fields().len())?;
    for field in schema.fields() {
        update_field(&mut digest, field)?;
    }
    update_metadata(&mut digest, schema.metadata())?;
    Ok(digest.finalize().into())
}

fn research_payload_contract_digest() -> Result<[u8; 32], DatasetSchemaError> {
    let mut digest = Sha256::new();
    update_bytes(&mut digest, RESEARCH_PAYLOAD_CONTRACT_DOMAIN)?;
    update_bytes(&mut digest, RESEARCH_RECORD_SCHEMA.as_bytes())?;
    update_bytes(&mut digest, RESEARCH_PAYLOAD_ENVELOPE_CONTRACT)?;
    update_length(&mut digest, RESEARCH_PAYLOAD_CONTRACT.len())?;
    for entry in RESEARCH_PAYLOAD_CONTRACT {
        update_bytes(&mut digest, entry.json_tag.as_bytes())?;
        digest.update([entry.semantic_tag]);
        update_bytes(&mut digest, entry.semantic_encoder.as_bytes())?;
    }
    Ok(digest.finalize().into())
}

fn update_field(digest: &mut Sha256, field: &Field) -> Result<(), DatasetSchemaError> {
    update_bytes(digest, field.name().as_bytes())?;
    digest.update([u8::from(field.is_nullable())]);
    update_data_type(digest, field.data_type())?;
    update_metadata(digest, field.metadata())
}

fn update_data_type(digest: &mut Sha256, data_type: &DataType) -> Result<(), DatasetSchemaError> {
    match data_type {
        DataType::UInt8 => update_bytes(digest, b"uint8")?,
        DataType::UInt16 => update_bytes(digest, b"uint16")?,
        DataType::UInt32 => update_bytes(digest, b"uint32")?,
        DataType::Float64 => update_bytes(digest, b"float64")?,
        DataType::Utf8 => update_bytes(digest, b"utf8")?,
        DataType::Binary => update_bytes(digest, b"binary")?,
        DataType::FixedSizeBinary(width) => {
            update_bytes(digest, b"fixed_size_binary")?;
            digest.update(width.to_be_bytes());
        }
        DataType::Date32 => update_bytes(digest, b"date32")?,
        DataType::Decimal128(precision, scale) => {
            update_bytes(digest, b"decimal128")?;
            digest.update([*precision]);
            digest.update(scale.to_be_bytes());
        }
        DataType::Timestamp(unit, timezone) => {
            update_bytes(digest, b"timestamp")?;
            update_bytes(
                digest,
                match unit {
                    TimeUnit::Second => b"second".as_slice(),
                    TimeUnit::Millisecond => b"millisecond".as_slice(),
                    TimeUnit::Microsecond => b"microsecond".as_slice(),
                    TimeUnit::Nanosecond => b"nanosecond".as_slice(),
                },
            )?;
            match timezone {
                Some(timezone) => {
                    digest.update([1]);
                    update_bytes(digest, timezone.as_bytes())?;
                }
                None => digest.update([0]),
            }
        }
        _ => return Err(DatasetSchemaError::UnsupportedArrowType),
    }
    Ok(())
}

fn update_metadata(
    digest: &mut Sha256,
    metadata: &HashMap<String, String>,
) -> Result<(), DatasetSchemaError> {
    let mut entries = metadata
        .iter()
        .filter(|(key, _)| key.as_str() != SCHEMA_FINGERPRINT_KEY)
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
    update_length(digest, entries.len())?;
    for (key, value) in entries {
        update_bytes(digest, key.as_bytes())?;
        update_bytes(digest, value.as_bytes())?;
    }
    Ok(())
}

fn update_length(digest: &mut Sha256, length: usize) -> Result<(), DatasetSchemaError> {
    let length = u64::try_from(length).map_err(|_| DatasetSchemaError::UnsupportedArrowType)?;
    digest.update(length.to_be_bytes());
    Ok(())
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), DatasetSchemaError> {
    update_length(digest, bytes.len())?;
    digest.update(bytes);
    Ok(())
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
