//! Closed, versioned source-manifest schema and validation.

use std::collections::BTreeSet;

use market_squawk_domain::{SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::marker::PhantomData;

use crate::{ExtractionLimits, FileAdapterError};

const MANIFEST_SCHEMA_VERSION: u16 = 1;
const MAX_MANIFEST_OBJECTS: usize = 4_096;
const MAX_MAPPINGS: usize = 1_024;

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
        serde_json::from_slice(bytes).map_err(|_| FileAdapterError::InvalidManifest)
    }

    pub(crate) fn validate(&self) -> Result<(), FileAdapterError> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION
            || self.objects.is_empty()
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
            {
                return Err(FileAdapterError::InvalidManifest);
            }
            object.format.validate()?;
            object.row_policy.validate()?;
            object.validate_format_policy()?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileObjectSpec {
    pub(crate) dataset: SourceIdentifier,
    pub(crate) object_id: SourceIdentifier,
    pub(crate) path: String,
    pub(crate) format: FileFormat,
    pub(crate) effective_at: Timestamp,
    pub(crate) published_at: Option<Timestamp>,
    pub(crate) revision: SourceIdentifier,
    pub(crate) revision_number: u32,
    pub(crate) superseded_at: Option<Timestamp>,
    pub(crate) row_policy: RowPolicy,
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
        {
            return Err(FileAdapterError::InvalidManifest);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
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

impl FileFormat {
    fn validate(&self) -> Result<(), FileAdapterError> {
        match self {
            Self::Csv { delimiter } if matches!(*delimiter, b'\n' | b'\r' | b'"' | 0) => {
                Err(FileAdapterError::InvalidManifest)
            }
            Self::Xml { record_element } if !valid_field_name(record_element) => {
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
        if !valid_field_name(&self.identity_field) {
            return Err(FileAdapterError::InvalidManifest);
        }
        let mut sources = BTreeSet::new();
        let mut outputs = BTreeSet::new();
        for field in &self.fields {
            if !valid_field_name(&field.source)
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

fn valid_field_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
        })
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
