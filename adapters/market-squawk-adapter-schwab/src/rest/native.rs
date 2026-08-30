use std::{collections::BTreeSet, fmt};

use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};

use crate::{ParseBounds, SchwabAdapterError};

/// Exact provider JSON number lexeme.
///
/// It is intentionally not converted through binary floating point or a display string.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NativeNumber(Box<str>);

impl NativeNumber {
    pub(crate) fn from_json(number: Number) -> Self {
        Self(number.to_string().into_boxed_str())
    }

    /// Exact JSON number representation retained by `serde_json`'s arbitrary-precision mode.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Closed scalar value admitted from a named provider-native field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeScalar {
    /// Provider explicitly returned JSON null.
    Null,
    /// Boolean field.
    Bool(bool),
    /// Exact JSON number.
    Number(NativeNumber),
    /// Bounded text field.
    Text(Box<str>),
}

impl NativeScalar {
    pub(crate) fn try_from_json(value: Value) -> Result<Self, SchwabAdapterError> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::Bool(value) => Ok(Self::Bool(value)),
            Value::Number(value) => Ok(Self::Number(NativeNumber::from_json(value))),
            Value::String(value) => Ok(Self::Text(value.into_boxed_str())),
            Value::Array(_) | Value::Object(_) => Err(SchwabAdapterError::SchemaViolation),
        }
    }

    /// Returns an exact numeric field when the provider supplied one.
    pub const fn number(&self) -> Option<&NativeNumber> {
        match self {
            Self::Number(value) => Some(value),
            Self::Null | Self::Bool(_) | Self::Text(_) => None,
        }
    }

    /// Returns a text field when the provider supplied one.
    pub fn text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            Self::Null | Self::Bool(_) | Self::Number(_) => None,
        }
    }
}

/// Distinguishes provider absence from explicit null and a typed value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NativeField<T> {
    /// Key absent from the provider response.
    Absent,
    /// Key present with JSON null.
    Null,
    /// Key present with a validated value.
    Value(T),
}

impl<T> Default for NativeField<T> {
    fn default() -> Self {
        Self::Absent
    }
}

/// One field selected by a closed provider-native dictionary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeFieldEntry<K> {
    name: K,
    value: NativeScalar,
}

impl<K> NativeFieldEntry<K> {
    pub(crate) const fn new(name: K, value: NativeScalar) -> Self {
        Self { name, value }
    }

    /// Closed field identity.
    pub const fn name(&self) -> &K {
        &self.name
    }

    /// Exact provider-native scalar.
    pub const fn value(&self) -> &NativeScalar {
        &self.value
    }
}

/// Bounded diagnostics for provider fields outside the current native dictionary.
///
/// Unknown values remain only in the separately retained raw object. This summary exposes paths
/// and a commitment, never an arbitrary value map that a canonical consumer could misuse.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownFieldSummary {
    field_count: usize,
    encoded_bytes: usize,
    paths: Box<[Box<str>]>,
    digest: [u8; 32],
}

impl UnknownFieldSummary {
    /// Number of unknown fields.
    pub const fn field_count(&self) -> usize {
        self.field_count
    }

    /// Canonical JSON bytes committed by the summary.
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Bounded JSON paths, without values.
    pub fn paths(&self) -> &[Box<str>] {
        &self.paths
    }

    /// SHA-256 commitment to paths and canonical JSON values in encounter order.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// One bounded provider-native parse result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedNative<T> {
    schema_name: &'static str,
    schema_version: u16,
    raw_sha256: [u8; 32],
    unknown_fields: UnknownFieldSummary,
    value: T,
}

impl<T> ParsedNative<T> {
    pub(crate) const fn new(
        schema_name: &'static str,
        raw_sha256: [u8; 32],
        unknown_fields: UnknownFieldSummary,
        value: T,
    ) -> Self {
        Self {
            schema_name,
            schema_version: 1,
            raw_sha256,
            unknown_fields,
            value,
        }
    }

    /// Code-owned provider-native schema name.
    pub const fn schema_name(&self) -> &'static str {
        self.schema_name
    }

    /// Nonzero provider-native schema version.
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// SHA-256 of the exact response/frame bytes.
    pub const fn raw_sha256(&self) -> [u8; 32] {
        self.raw_sha256
    }

    /// Unknown-field diagnostic. Values remain in the raw object only.
    pub const fn unknown_fields(&self) -> &UnknownFieldSummary {
        &self.unknown_fields
    }

    /// Closed typed provider-native payload.
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Consumes the parse envelope and returns the typed payload.
    pub fn into_value(self) -> T {
        self.value
    }
}

pub(crate) struct ParseContext {
    bounds: ParseBounds,
    records: usize,
    unknown_fields: usize,
    unknown_bytes: usize,
    unknown_paths: Vec<Box<str>>,
    unknown_hasher: Sha256,
}

impl fmt::Debug for ParseContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParseContext")
            .field("bounds", &self.bounds)
            .field("records", &self.records)
            .field("unknown_fields", &self.unknown_fields)
            .finish_non_exhaustive()
    }
}

impl ParseContext {
    pub(crate) fn new(bounds: ParseBounds) -> Self {
        Self {
            bounds,
            records: 0,
            unknown_fields: 0,
            unknown_bytes: 0,
            unknown_paths: Vec::new(),
            unknown_hasher: Sha256::new(),
        }
    }

    pub(crate) fn take_record(&mut self) -> Result<(), SchwabAdapterError> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        if self.records > self.bounds.max_records() {
            return Err(SchwabAdapterError::BoundsExceeded);
        }
        Ok(())
    }

    pub(crate) fn record_unknown(
        &mut self,
        path: &str,
        key: &str,
        value: &Value,
    ) -> Result<(), SchwabAdapterError> {
        self.unknown_fields = self
            .unknown_fields
            .checked_add(1)
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        let encoded = serde_json::to_vec(value).map_err(|_| SchwabAdapterError::SchemaViolation)?;
        self.unknown_bytes = self
            .unknown_bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(encoded.len()))
            .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        if self.unknown_fields > self.bounds.max_unknown_fields()
            || self.unknown_bytes > self.bounds.max_unknown_bytes()
        {
            return Err(SchwabAdapterError::BoundsExceeded);
        }
        let full_path = if path == "$" {
            format!("$.{key}")
        } else {
            format!("{path}.{key}")
        };
        self.unknown_paths.push(full_path.clone().into_boxed_str());
        hash_len_prefixed(&mut self.unknown_hasher, full_path.as_bytes())?;
        hash_len_prefixed(&mut self.unknown_hasher, &encoded)?;
        Ok(())
    }

    pub(crate) fn finish(self) -> UnknownFieldSummary {
        UnknownFieldSummary {
            field_count: self.unknown_fields,
            encoded_bytes: self.unknown_bytes,
            paths: self.unknown_paths.into_boxed_slice(),
            digest: self.unknown_hasher.finalize().into(),
        }
    }
}

pub(crate) fn parse_json_payload(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<(Value, [u8; 32], ParseContext), SchwabAdapterError> {
    if bytes.is_empty() || bytes.len() > bounds.max_response_bytes() {
        return Err(SchwabAdapterError::BoundsExceeded);
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| SchwabAdapterError::SchemaViolation)?;
    let mut nodes = 0usize;
    validate_json_resource(&value, 1, bounds, &mut nodes)?;
    Ok((
        value,
        Sha256::digest(bytes).into(),
        ParseContext::new(bounds),
    ))
}

/// Rejects duplicate keys in a provider response whose top-level object is semantically keyed.
///
/// `serde_json::Value` necessarily collapses duplicate object keys, so keyed response families
/// must perform this streaming pass over the exact bytes before treating the materialized map as
/// unambiguous provider evidence.
pub(crate) fn validate_unique_top_level_keys(bytes: &[u8]) -> Result<(), SchwabAdapterError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    TopLevelUniqueKeys
        .deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
        .map_err(|_| SchwabAdapterError::SchemaViolation)
}

struct TopLevelUniqueKeys;

impl<'de> Visitor<'de> for TopLevelUniqueKeys {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object with unique top-level keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(serde::de::Error::custom("duplicate top-level object key"));
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for TopLevelUniqueKeys {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

pub(crate) fn take_object(value: Value) -> Result<Map<String, Value>, SchwabAdapterError> {
    value
        .as_object()
        .cloned()
        .ok_or(SchwabAdapterError::SchemaViolation)
}

fn validate_json_resource(
    value: &Value,
    depth: usize,
    bounds: ParseBounds,
    nodes: &mut usize,
) -> Result<(), SchwabAdapterError> {
    if depth > bounds.max_json_depth() {
        return Err(SchwabAdapterError::BoundsExceeded);
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
    if *nodes > bounds.max_json_nodes() {
        return Err(SchwabAdapterError::BoundsExceeded);
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_json_resource(value, depth + 1, bounds, nodes)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                if key.is_empty() {
                    return Err(SchwabAdapterError::SchemaViolation);
                }
                validate_json_resource(value, depth + 1, bounds, nodes)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn hash_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), SchwabAdapterError> {
    let length = u64::try_from(bytes.len()).map_err(|_| SchwabAdapterError::ArithmeticOverflow)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}
