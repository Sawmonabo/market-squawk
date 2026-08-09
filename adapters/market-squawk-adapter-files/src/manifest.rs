//! Closed, versioned source-manifest schema and validation.

use std::collections::BTreeSet;

use market_squawk_domain::{
    EffectiveInterval, InstrumentId, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;

use crate::{ExtractionLimits, FileAdapterError};

pub(super) const MANIFEST_SCHEMA_VERSION: u16 = 5;
const PREVIOUS_MANIFEST_SCHEMA_VERSION: u16 = 4;
const LEGACY_MANIFEST_SCHEMA_VERSION: u16 = 3;
const MAX_MANIFEST_OBJECTS: usize = 4_096;
pub(super) const MAX_MAPPINGS: usize = 1_024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileSourceManifest {
    schema_version: u16,
    #[serde(deserialize_with = "deserialize_objects")]
    pub(crate) objects: Vec<FileObjectSpec>,
}

impl FileSourceManifest {
    pub(crate) fn parse(bytes: &[u8], limits: ExtractionLimits) -> Result<Self, FileAdapterError> {
        crate::manifest_bounds::admit(bytes, limits)?;
        let mut manifest: Self =
            serde_json::from_slice(bytes).map_err(|_| FileAdapterError::InvalidManifest)?;
        for object in &mut manifest.objects {
            object.manifest_schema_version = manifest.schema_version;
        }
        Ok(manifest)
    }

    pub(crate) fn validate(&self) -> Result<(), FileAdapterError> {
        if !matches!(
            self.schema_version,
            LEGACY_MANIFEST_SCHEMA_VERSION
                | PREVIOUS_MANIFEST_SCHEMA_VERSION
                | MANIFEST_SCHEMA_VERSION
        ) || self.objects.is_empty()
            || self.objects.len() > MAX_MANIFEST_OBJECTS
        {
            return Err(FileAdapterError::InvalidManifest);
        }
        let mut identities = BTreeSet::new();
        for object in &self.objects {
            if !identities.insert((object.dataset.clone(), object.object_id.clone()))
                || object.row_policy.fields.is_empty()
                || object.row_policy.fields.len() > MAX_MAPPINGS
                || object
                    .superseded_at
                    .is_some_and(|value| value <= object.effective_at)
                || object.revision_number == 0
                || (self.schema_version == LEGACY_MANIFEST_SCHEMA_VERSION
                    && object.universe_membership.is_some())
                || (self.schema_version != MANIFEST_SCHEMA_VERSION && object.row_time.is_some())
                || object
                    .universe_membership
                    .as_ref()
                    .is_some_and(|membership| {
                        object.instrument_binding.instrument_id().is_none()
                            || EffectiveInterval::new(membership.starts_at, membership.ends_at)
                                .is_err()
                    })
                || ResearchTime::try_new_with_coordinates(
                    object.record_time.effective.clone(),
                    object.record_time.published.clone(),
                    RevisionNumber::new(object.revision_number)
                        .map_err(|_| FileAdapterError::InvalidManifest)?,
                    object.record_time.superseded.clone(),
                )
                .is_err()
            {
                return Err(FileAdapterError::InvalidManifest);
            }
            object.format.validate()?;
            object.row_policy.validate()?;
            if let Some(row_time) = &object.row_time {
                row_time.validate()?;
            }
            object.validate_format_policy()?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileObjectSpec {
    #[serde(skip)]
    pub(crate) manifest_schema_version: u16,
    pub(crate) dataset: SourceIdentifier,
    pub(crate) object_id: SourceIdentifier,
    pub(crate) path: String,
    pub(crate) format: FileFormat,
    pub(crate) effective_at: Timestamp,
    pub(crate) published_at: Option<Timestamp>,
    pub(crate) revision: SourceIdentifier,
    pub(crate) revision_number: u32,
    pub(crate) superseded_at: Option<Timestamp>,
    /// Record coordinates retained independently from the exact discovery-object interval.
    pub(crate) record_time: FileRecordTimeSpec,
    /// Optional exact source-field overrides for canonical per-row time and revision semantics.
    #[serde(default)]
    pub(crate) row_time: Option<RowTimeFieldSpec>,
    pub(crate) instrument_binding: InstrumentBinding,
    /// Optional explicit source-authored historical-universe evidence for this exact object.
    #[serde(default)]
    pub(crate) universe_membership: Option<UniverseMembershipSpec>,
    pub(crate) row_policy: RowPolicy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UniverseMembershipSpec {
    pub(crate) universe: SourceIdentifier,
    pub(crate) starts_at: Timestamp,
    pub(crate) ends_at: Option<Timestamp>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum InstrumentBinding {
    Unscoped,
    InternalInstrument { instrument_id: InstrumentId },
}

impl InstrumentBinding {
    pub(crate) const fn instrument_id(&self) -> Option<InstrumentId> {
        match self {
            Self::Unscoped => None,
            Self::InternalInstrument { instrument_id } => Some(*instrument_id),
        }
    }

    pub(crate) fn bind_identity(&self, hasher: &mut sha2::Sha256) {
        use sha2::Digest as _;

        match self {
            Self::Unscoped => hasher.update(b"unscoped"),
            Self::InternalInstrument { instrument_id } => {
                hasher.update(b"internal-instrument");
                hasher.update(instrument_id.as_uuid().as_bytes());
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileRecordTimeSpec {
    pub(crate) effective: ResearchTemporalCoordinate,
    pub(crate) published: Option<ResearchTemporalCoordinate>,
    pub(crate) superseded: Option<ResearchTemporalCoordinate>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RowTimeFieldSpec {
    pub(crate) effective_field: Option<String>,
    pub(crate) published_field: Option<String>,
    pub(crate) available_field: Option<String>,
    pub(crate) revision_field: Option<String>,
    pub(crate) revision_number_field: Option<String>,
    pub(crate) superseded_field: Option<String>,
}

impl RowTimeFieldSpec {
    fn validate(&self) -> Result<(), FileAdapterError> {
        let mut fields = self.fields().peekable();
        if fields.peek().is_none() || fields.any(|field| !valid_source_column_name(field)) {
            return Err(FileAdapterError::InvalidManifest);
        }
        Ok(())
    }

    pub(crate) fn fields(&self) -> impl Iterator<Item = &str> {
        [
            self.effective_field.as_deref(),
            self.published_field.as_deref(),
            self.available_field.as_deref(),
            self.revision_field.as_deref(),
            self.revision_number_field.as_deref(),
            self.superseded_field.as_deref(),
        ]
        .into_iter()
        .flatten()
    }
}

impl FileObjectSpec {
    fn validate_format_policy(&self) -> Result<(), FileAdapterError> {
        let FileFormat::Sqlite { columns, .. } = &self.format else {
            return Ok(());
        };
        let selected: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
        if !selected.contains(self.row_policy.identity_field.as_str())
            || self
                .row_policy
                .fields
                .iter()
                .any(|mapping| !selected.contains(mapping.source.as_str()))
            || self
                .row_time
                .as_ref()
                .is_some_and(|row_time| row_time.fields().any(|field| !selected.contains(field)))
        {
            return Err(FileAdapterError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum FileFormat {
    Csv {
        delimiter: u8,
    },
    Tsv {},
    Json {},
    Ndjson {},
    Xml {
        record_element: String,
    },
    Excel {
        formula_policy: FormulaPolicy,
    },
    Parquet {},
    Sqlite {
        table: String,
        columns: Vec<String>,
        order_by: Vec<String>,
    },
    Ofx {
        account_id: String,
        currency: String,
    },
    Qfx {
        account_id: String,
        currency: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileFormatKind {
    Csv,
    Tsv,
    Json,
    Ndjson,
    Xml,
    Excel,
    Parquet,
    Sqlite,
    Ofx,
    Qfx,
}

#[derive(Clone, Copy)]
enum FileFormatField {
    Kind,
    Delimiter,
    RecordElement,
    FormulaPolicy,
    Table,
    Columns,
    OrderBy,
    AccountId,
    Currency,
}

#[derive(Default)]
struct FileFormatWire {
    kind: Option<FileFormatKind>,
    delimiter: Option<u8>,
    record_element: Option<String>,
    formula_policy: Option<FormulaPolicy>,
    table: Option<String>,
    columns: Option<Vec<String>>,
    order_by: Option<Vec<String>>,
    account_id: Option<String>,
    currency: Option<String>,
}

struct FileFormatVisitor;
struct FileFormatFieldVisitor;
struct FormatStrings(Vec<String>);

const FILE_FORMAT_FIELDS: &[&str] = &[
    "kind",
    "delimiter",
    "record_element",
    "formula_policy",
    "table",
    "columns",
    "order_by",
    "account_id",
    "currency",
];

impl FileFormatKind {
    const fn allows(self, field: FileFormatField) -> bool {
        matches!(
            (self, field),
            (Self::Csv, FileFormatField::Delimiter)
                | (Self::Xml, FileFormatField::RecordElement)
                | (Self::Excel, FileFormatField::FormulaPolicy)
                | (Self::Sqlite, FileFormatField::Table)
                | (Self::Sqlite, FileFormatField::Columns)
                | (Self::Sqlite, FileFormatField::OrderBy)
                | (Self::Ofx | Self::Qfx, FileFormatField::AccountId)
                | (Self::Ofx | Self::Qfx, FileFormatField::Currency)
        )
    }
}

impl FileFormatWire {
    fn finish<E>(self) -> Result<FileFormat, E>
    where
        E: serde::de::Error,
    {
        let kind = self.kind.ok_or_else(|| E::missing_field("kind"))?;
        for (present, field) in [
            (self.delimiter.is_some(), FileFormatField::Delimiter),
            (
                self.record_element.is_some(),
                FileFormatField::RecordElement,
            ),
            (
                self.formula_policy.is_some(),
                FileFormatField::FormulaPolicy,
            ),
            (self.table.is_some(), FileFormatField::Table),
            (self.columns.is_some(), FileFormatField::Columns),
            (self.order_by.is_some(), FileFormatField::OrderBy),
            (self.account_id.is_some(), FileFormatField::AccountId),
            (self.currency.is_some(), FileFormatField::Currency),
        ] {
            if present && !kind.allows(field) {
                return Err(E::custom("file format field does not match its kind"));
            }
        }
        match kind {
            FileFormatKind::Csv => Ok(FileFormat::Csv {
                delimiter: self
                    .delimiter
                    .ok_or_else(|| E::missing_field("delimiter"))?,
            }),
            FileFormatKind::Tsv => Ok(FileFormat::Tsv {}),
            FileFormatKind::Json => Ok(FileFormat::Json {}),
            FileFormatKind::Ndjson => Ok(FileFormat::Ndjson {}),
            FileFormatKind::Xml => Ok(FileFormat::Xml {
                record_element: self
                    .record_element
                    .ok_or_else(|| E::missing_field("record_element"))?,
            }),
            FileFormatKind::Excel => Ok(FileFormat::Excel {
                formula_policy: self
                    .formula_policy
                    .ok_or_else(|| E::missing_field("formula_policy"))?,
            }),
            FileFormatKind::Parquet => Ok(FileFormat::Parquet {}),
            FileFormatKind::Sqlite => Ok(FileFormat::Sqlite {
                table: self.table.ok_or_else(|| E::missing_field("table"))?,
                columns: self.columns.ok_or_else(|| E::missing_field("columns"))?,
                order_by: self.order_by.ok_or_else(|| E::missing_field("order_by"))?,
            }),
            FileFormatKind::Ofx => Ok(FileFormat::Ofx {
                account_id: self
                    .account_id
                    .ok_or_else(|| E::missing_field("account_id"))?,
                currency: self.currency.ok_or_else(|| E::missing_field("currency"))?,
            }),
            FileFormatKind::Qfx => Ok(FileFormat::Qfx {
                account_id: self
                    .account_id
                    .ok_or_else(|| E::missing_field("account_id"))?,
                currency: self.currency.ok_or_else(|| E::missing_field("currency"))?,
            }),
        }
    }
}

impl<'de> Deserialize<'de> for FileFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(FileFormatVisitor)
    }
}

impl<'de> Visitor<'de> for FileFormatVisitor {
    type Value = FileFormat;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a closed file format object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut wire = FileFormatWire::default();
        while let Some(field) = map.next_key::<FileFormatField>()? {
            match field {
                FileFormatField::Kind => {
                    if wire.kind.is_some() {
                        return Err(serde::de::Error::duplicate_field("kind"));
                    }
                    wire.kind = Some(map.next_value()?);
                }
                FileFormatField::Delimiter => {
                    if wire.delimiter.is_some() {
                        return Err(serde::de::Error::duplicate_field("delimiter"));
                    }
                    wire.delimiter = Some(map.next_value()?);
                }
                FileFormatField::RecordElement => {
                    if wire.record_element.is_some() {
                        return Err(serde::de::Error::duplicate_field("record_element"));
                    }
                    wire.record_element = Some(map.next_value()?);
                }
                FileFormatField::FormulaPolicy => {
                    if wire.formula_policy.is_some() {
                        return Err(serde::de::Error::duplicate_field("formula_policy"));
                    }
                    wire.formula_policy = Some(map.next_value()?);
                }
                FileFormatField::Table => {
                    if wire.table.is_some() {
                        return Err(serde::de::Error::duplicate_field("table"));
                    }
                    wire.table = Some(map.next_value()?);
                }
                FileFormatField::Columns => {
                    if wire.columns.is_some() {
                        return Err(serde::de::Error::duplicate_field("columns"));
                    }
                    wire.columns = Some(map.next_value::<FormatStrings>()?.0);
                }
                FileFormatField::OrderBy => {
                    if wire.order_by.is_some() {
                        return Err(serde::de::Error::duplicate_field("order_by"));
                    }
                    wire.order_by = Some(map.next_value::<FormatStrings>()?.0);
                }
                FileFormatField::AccountId => {
                    if wire.account_id.is_some() {
                        return Err(serde::de::Error::duplicate_field("account_id"));
                    }
                    wire.account_id = Some(map.next_value()?);
                }
                FileFormatField::Currency => {
                    if wire.currency.is_some() {
                        return Err(serde::de::Error::duplicate_field("currency"));
                    }
                    wire.currency = Some(map.next_value()?);
                }
            }
        }
        wire.finish()
    }
}

impl<'de> Deserialize<'de> for FileFormatField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(FileFormatFieldVisitor)
    }
}

impl<'de> Visitor<'de> for FileFormatFieldVisitor {
    type Value = FileFormatField;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a known file format field")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "kind" => Ok(FileFormatField::Kind),
            "delimiter" => Ok(FileFormatField::Delimiter),
            "record_element" => Ok(FileFormatField::RecordElement),
            "formula_policy" => Ok(FileFormatField::FormulaPolicy),
            "table" => Ok(FileFormatField::Table),
            "columns" => Ok(FileFormatField::Columns),
            "order_by" => Ok(FileFormatField::OrderBy),
            "account_id" => Ok(FileFormatField::AccountId),
            "currency" => Ok(FileFormatField::Currency),
            _ => Err(E::unknown_field(value, FILE_FORMAT_FIELDS)),
        }
    }
}

impl<'de> Deserialize<'de> for FormatStrings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer
            .deserialize_seq(FallibleVecVisitor::<String, MAX_MAPPINGS>(PhantomData))
            .map(Self)
    }
}

impl FileFormat {
    pub(crate) fn validate(&self) -> Result<(), FileAdapterError> {
        match self {
            Self::Csv { delimiter } if matches!(*delimiter, b'\n' | b'\r' | b'"' | 0) => {
                Err(FileAdapterError::InvalidManifest)
            }
            Self::Xml { record_element } if !valid_canonical_field_name(record_element) => {
                Err(FileAdapterError::InvalidManifest)
            }
            Self::Sqlite {
                table,
                columns,
                order_by,
            } if !valid_sql_identifier(table)
                || columns.is_empty()
                || columns.len() > MAX_MAPPINGS
                || order_by.is_empty()
                || order_by.len() > columns.len()
                || columns.iter().any(|column| !valid_sql_identifier(column))
                || order_by.iter().any(|column| !valid_sql_identifier(column))
                || columns.iter().collect::<BTreeSet<_>>().len() != columns.len()
                || order_by.iter().collect::<BTreeSet<_>>().len() != order_by.len()
                || order_by.iter().any(|column| !columns.contains(column)) =>
            {
                Err(FileAdapterError::InvalidManifest)
            }
            Self::Excel { formula_policy } => {
                match formula_policy {
                    FormulaPolicy::Reject | FormulaPolicy::CachedValues => {}
                }
                Ok(())
            }
            Self::Ofx {
                account_id,
                currency,
            }
            | Self::Qfx {
                account_id,
                currency,
            } if !valid_account_id(account_id) || !valid_currency(currency) => {
                Err(FileAdapterError::InvalidManifest)
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn media_type(&self) -> Result<SourceIdentifier, FileAdapterError> {
        let value = match self {
            Self::Csv { .. } => "text-csv",
            Self::Tsv { .. } => "text-tab-separated-values",
            Self::Json { .. } => "application-json",
            Self::Ndjson { .. } => "application-ndjson",
            Self::Xml { .. } => "application-xml",
            Self::Excel { .. } => "application-xlsx",
            Self::Parquet { .. } => "application-parquet",
            Self::Sqlite { .. } => "application-sqlite3",
            Self::Ofx { .. } => "application-ofx",
            Self::Qfx { .. } => "application-qfx",
        };
        SourceIdentifier::try_from(value).map_err(|_| FileAdapterError::Contract)
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FormulaPolicy {
    Reject,
    CachedValues,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RowPolicy {
    pub(crate) identity_field: String,
    #[serde(deserialize_with = "deserialize_mappings")]
    pub(crate) fields: Vec<FieldMapping>,
}

impl RowPolicy {
    fn validate(&self) -> Result<(), FileAdapterError> {
        if !valid_source_column_name(&self.identity_field) {
            return Err(FileAdapterError::InvalidManifest);
        }
        let mut sources = BTreeSet::new();
        let mut outputs = BTreeSet::new();
        for field in &self.fields {
            if !valid_source_column_name(&field.source)
                || field.decimal_scale > Decimal::MAX_SCALE
                || !sources.insert(field.source.as_str())
                || !outputs.insert(field.field.as_str())
            {
                return Err(FileAdapterError::InvalidManifest);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FieldMapping {
    pub(crate) source: String,
    pub(crate) field: SourceIdentifier,
    pub(crate) decimal_scale: u32,
    pub(crate) unit: Option<SourceIdentifier>,
}

fn valid_canonical_field_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
}

fn valid_source_column_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn valid_sql_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        })
}

fn valid_account_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && !value.chars().any(|character| character.is_control())
}

fn valid_currency(value: &str) -> bool {
    value.len() == 3 && value.bytes().all(|byte| byte.is_ascii_uppercase())
}

fn deserialize_objects<'de, D>(deserializer: D) -> Result<Vec<FileObjectSpec>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(FallibleVecVisitor::<FileObjectSpec, MAX_MANIFEST_OBJECTS>(
        PhantomData,
    ))
}

fn deserialize_mappings<'de, D>(deserializer: D) -> Result<Vec<FieldMapping>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_seq(FallibleVecVisitor::<FieldMapping, MAX_MAPPINGS>(
        PhantomData,
    ))
}

struct FallibleVecVisitor<T, const MAXIMUM: usize>(PhantomData<T>);

impl<'de, T, const MAXIMUM: usize> Visitor<'de> for FallibleVecVisitor<T, MAXIMUM>
where
    T: Deserialize<'de>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a sequence containing at most {MAXIMUM} entries")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|hint| hint > MAXIMUM) {
            return Err(serde::de::Error::custom(
                "bounded manifest sequence is too large",
            ));
        }
        let mut values = Vec::new();
        while values.len() < MAXIMUM {
            let Some(value) = sequence.next_element()? else {
                return Ok(values);
            };
            values
                .try_reserve_exact(1)
                .map_err(|_| serde::de::Error::custom("bounded manifest allocation failed"))?;
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(
                "bounded manifest sequence is too large",
            ))
        } else {
            Ok(values)
        }
    }
}
