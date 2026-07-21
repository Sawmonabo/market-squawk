//! Duplicate-rejecting bounded JSON and NDJSON extraction.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::{CellValue, FileAdapterError, ParseBudget, ParsedRow, ParserLimit};

const ARBITRARY_PRECISION_NUMBER_TOKEN: &str = "$serde_json::private::Number";

enum JsonValue {
    Object(BTreeMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    Text(String),
    Null,
    Unsupported,
}

struct JsonSeed<'budget, 'token> {
    budget: &'budget mut ParseBudget<'token>,
    depth: usize,
    admit_row: bool,
}

struct JsonVisitor<'budget, 'token> {
    budget: &'budget mut ParseBudget<'token>,
    depth: usize,
    admit_row: bool,
}

struct RowJsonSeed<'budget, 'token> {
    budget: &'budget mut ParseBudget<'token>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for RowJsonSeed<'_, '_> {
    type Value = JsonValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.budget.record().map_err(serde::de::Error::custom)?;
        JsonSeed {
            budget: self.budget,
            depth: self.depth,
            admit_row: false,
        }
        .deserialize(deserializer)
    }
}

impl<'de> DeserializeSeed<'de> for JsonSeed<'_, '_> {
    type Value = JsonValue;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.budget
            .depth(self.depth)
            .map_err(serde::de::Error::custom)?;
        deserializer.deserialize_any(JsonVisitor {
            budget: self.budget,
            depth: self.depth,
            admit_row: self.admit_row,
        })
    }
}

impl<'de> Visitor<'de> for JsonVisitor<'_, '_> {
    type Value = JsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(JsonValue::Null)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(JsonValue::Unsupported)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let value = self
            .budget
            .formatted_text(20, format_args!("{value}"))
            .map_err(E::custom)?;
        Ok(JsonValue::Text(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let value = self
            .budget
            .formatted_text(20, format_args!("{value}"))
            .map_err(E::custom)?;
        Ok(JsonValue::Text(value))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(E::custom("floating-point JSON boundary is forbidden"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let value = self.budget.owned_text(value).map_err(E::custom)?;
        Ok(JsonValue::Text(value))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.budget.text(value.len()).map_err(E::custom)?;
        Ok(JsonValue::Text(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let maximum = if self.admit_row {
            self.budget.row_limit()
        } else {
            self.budget.limits.input.max_fields_per_record
        };
        let capacity = sequence.size_hint().unwrap_or(0).min(maximum).min(1_024);
        let mut values = self
            .budget
            .vec_with_capacity(capacity)
            .map_err(serde::de::Error::custom)?;
        while values.len() < maximum {
            self.budget.checkpoint().map_err(serde::de::Error::custom)?;
            let value = if self.admit_row {
                sequence.next_element_seed(RowJsonSeed {
                    budget: self.budget,
                    depth: self.depth.saturating_add(1),
                })?
            } else {
                sequence.next_element_seed(JsonSeed {
                    budget: self.budget,
                    depth: self.depth.saturating_add(1),
                    admit_row: false,
                })?
            };
            let Some(value) = value else {
                return Ok(JsonValue::Array(values));
            };
            self.budget
                .reserve_vec_slot(&mut values)
                .map_err(serde::de::Error::custom)?;
            values.push(value);
        }
        let extra = if self.admit_row {
            sequence.next_element_seed(RowJsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
            })?
        } else {
            sequence.next_element_seed(JsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
                admit_row: false,
            })?
        };
        if extra.is_some() {
            return Err(serde::de::Error::custom(FileAdapterError::LimitExceeded(
                ParserLimit::Fields,
            )));
        }
        Ok(JsonValue::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        self.budget.checkpoint().map_err(serde::de::Error::custom)?;
        let Some(first_key) = map.next_key::<String>()? else {
            if self.admit_row {
                self.budget.record().map_err(serde::de::Error::custom)?;
            }
            return Ok(JsonValue::Object(BTreeMap::new()));
        };
        if first_key == ARBITRARY_PRECISION_NUMBER_TOKEN {
            let value = map.next_value::<String>()?;
            self.budget
                .text(value.len())
                .map_err(serde::de::Error::custom)?;
            self.budget
                .string_allocation(&value)
                .map_err(serde::de::Error::custom)?;
            if map.next_key::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(FileAdapterError::InvalidRecord));
            }
            return Ok(JsonValue::Text(value));
        }
        if self.admit_row {
            self.budget.record().map_err(serde::de::Error::custom)?;
        }
        let maximum = self.budget.limits.input.max_fields_per_record;
        let mut values = BTreeMap::new();
        let mut next_key = Some(first_key);
        while let Some(key) = next_key {
            self.budget.checkpoint().map_err(serde::de::Error::custom)?;
            self.budget
                .text(key.len())
                .map_err(serde::de::Error::custom)?;
            if values.len() >= maximum {
                return Err(serde::de::Error::custom(FileAdapterError::LimitExceeded(
                    ParserLimit::Fields,
                )));
            }
            if values.contains_key(&key) {
                return Err(serde::de::Error::custom(FileAdapterError::DuplicateField));
            }
            let value = map.next_value_seed(JsonSeed {
                budget: self.budget,
                depth: self.depth.saturating_add(1),
                admit_row: false,
            })?;
            self.budget
                .map_entry::<String, JsonValue>()
                .map_err(serde::de::Error::custom)?;
            values.insert(key, value);
            next_key = map.next_key::<String>()?;
        }
        Ok(JsonValue::Object(values))
    }
}

pub(crate) fn parse_json(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    budget
        .decompressed(u64::try_from(bytes.len()).map_err(|_| FileAdapterError::InvalidRecord)?)?;
    let value = parse_one(bytes, budget, true)?;
    root_rows(value, budget)
}

pub(crate) fn parse_ndjson(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    budget
        .decompressed(u64::try_from(bytes.len()).map_err(|_| FileAdapterError::InvalidRecord)?)?;
    let mut rows = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        budget.checkpoint()?;
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        budget.record()?;
        let value = parse_one(line, budget, false)?;
        let JsonValue::Object(object) = value else {
            return Err(FileAdapterError::InvalidRecord);
        };
        let row = object_to_row(object, budget)?;
        budget.reserve_vec_slot(&mut rows)?;
        rows.push(row);
    }
    Ok(rows)
}

fn parse_one(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
    admit_row: bool,
) -> Result<JsonValue, FileAdapterError> {
    reject_reserved_number_keys(bytes, budget)?;
    // serde_json may allocate a decoded scratch string before invoking the visitor for escaped
    // values and owned map keys. Decoded UTF-8 cannot exceed the raw JSON slice, and the doubled
    // bound covers allocator growth before any visitor callback can run.
    budget.pre_admit_dynamic_bytes(bytes.len())?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = match (JsonSeed {
        budget,
        depth: 0,
        admit_row,
    })
    .deserialize(&mut deserializer)
    {
        Ok(value) => value,
        Err(error) => {
            let message = error.to_string();
            budget.string_allocation(&message)?;
            return Err(classify_json_error(&message, budget));
        }
    };
    deserializer
        .end()
        .map_err(|_| FileAdapterError::InvalidRecord)?;
    Ok(value)
}

fn reject_reserved_number_keys(
    bytes: &[u8],
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    let mut index = 0_usize;
    let mut next_checkpoint = 4_096_usize;
    while index < bytes.len() {
        if index >= next_checkpoint {
            budget.checkpoint()?;
            next_checkpoint = next_checkpoint.saturating_add(4_096);
        }
        if bytes[index] != b'"' {
            index += 1;
            continue;
        }
        let (matches_reserved, after_string) = string_matches_reserved_number_token(bytes, index);
        index = after_string;
        let mut following = after_string;
        while bytes.get(following).is_some_and(u8::is_ascii_whitespace) {
            following += 1;
        }
        if matches_reserved && bytes.get(following) == Some(&b':') {
            return Err(FileAdapterError::InvalidRecord);
        }
    }
    Ok(())
}

fn string_matches_reserved_number_token(bytes: &[u8], opening_quote: usize) -> (bool, usize) {
    let token = ARBITRARY_PRECISION_NUMBER_TOKEN.as_bytes();
    let mut index = opening_quote.saturating_add(1);
    let mut token_index = 0_usize;
    let mut matches = true;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'"' {
            return (matches && token_index == token.len(), index + 1);
        }
        let (decoded, consumed) = if byte == b'\\' {
            match bytes.get(index + 1).copied() {
                Some(b'"') => (Some(b'"'), 2),
                Some(b'\\') => (Some(b'\\'), 2),
                Some(b'/') => (Some(b'/'), 2),
                Some(b'b') => (Some(0x08), 2),
                Some(b'f') => (Some(0x0c), 2),
                Some(b'n') => (Some(b'\n'), 2),
                Some(b'r') => (Some(b'\r'), 2),
                Some(b't') => (Some(b'\t'), 2),
                Some(b'u') => {
                    let value = bytes
                        .get(index + 2..index + 6)
                        .and_then(decode_ascii_hex_quad);
                    (value.and_then(|value| u8::try_from(value).ok()), 6)
                }
                Some(_) => (None, 2),
                None => return (false, bytes.len()),
            }
        } else {
            (Some(byte), 1)
        };
        if decoded.is_none_or(|decoded| token.get(token_index) != Some(&decoded)) {
            matches = false;
        }
        token_index = token_index.saturating_add(1);
        index = index.saturating_add(consumed).min(bytes.len());
    }
    (false, bytes.len())
}

fn decode_ascii_hex_quad(bytes: &[u8]) -> Option<u16> {
    if bytes.len() != 4 {
        return None;
    }
    bytes.iter().try_fold(0_u16, |value, byte| {
        let digit = match byte {
            b'0'..=b'9' => u16::from(byte - b'0'),
            b'a'..=b'f' => u16::from(byte - b'a') + 10,
            b'A'..=b'F' => u16::from(byte - b'A') + 10,
            _ => return None,
        };
        value.checked_mul(16)?.checked_add(digit)
    })
}

fn classify_json_error(message: &str, budget: &ParseBudget<'_>) -> FileAdapterError {
    if message.contains("local extraction was cancelled") {
        FileAdapterError::Cancelled
    } else if message.contains("local extraction deadline was exceeded") {
        FileAdapterError::DeadlineExceeded
    } else if message.contains("local extraction clock failed") {
        FileAdapterError::ClockFailure
    } else if message.contains("duplicate field") {
        FileAdapterError::DuplicateField
    } else if message.contains("NestingDepth") {
        FileAdapterError::LimitExceeded(ParserLimit::NestingDepth)
    } else if message.contains("TextBytes") {
        FileAdapterError::LimitExceeded(ParserLimit::TextBytes)
    } else if message.contains("DecodedBytes") {
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    } else if message.contains("requested record maximum") || message.contains("Records") {
        budget.row_limit_error()
    } else if message.contains("Fields") {
        FileAdapterError::LimitExceeded(ParserLimit::Fields)
    } else {
        FileAdapterError::InvalidRecord
    }
}

fn root_rows(
    value: JsonValue,
    budget: &mut ParseBudget<'_>,
) -> Result<Vec<ParsedRow>, FileAdapterError> {
    match value {
        JsonValue::Object(object) => {
            let mut rows = Vec::new();
            let row = object_to_row(object, budget)?;
            budget.reserve_vec_slot(&mut rows)?;
            rows.push(row);
            Ok(rows)
        }
        JsonValue::Array(values) => {
            let mut rows = budget.vec_with_capacity(values.len())?;
            for value in values {
                let JsonValue::Object(object) = value else {
                    return Err(FileAdapterError::InvalidRecord);
                };
                rows.push(object_to_row(object, budget)?);
            }
            Ok(rows)
        }
        JsonValue::Text(_) | JsonValue::Null | JsonValue::Unsupported => {
            Err(FileAdapterError::InvalidRecord)
        }
    }
}

fn object_to_row(
    object: BTreeMap<String, JsonValue>,
    budget: &mut ParseBudget<'_>,
) -> Result<ParsedRow, FileAdapterError> {
    let mut fields = BTreeMap::new();
    flatten_object(None, object, &mut fields, budget)?;
    budget.fields(fields.len())?;
    ParsedRow::try_new(fields, budget)
}

fn flatten_object(
    prefix: Option<&str>,
    object: BTreeMap<String, JsonValue>,
    fields: &mut BTreeMap<String, CellValue>,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    for (key, value) in object {
        let key = if let Some(prefix) = prefix {
            let maximum = prefix
                .len()
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(key.len()))
                .ok_or(FileAdapterError::LimitExceeded(ParserLimit::TextBytes))?;
            budget.formatted_text(maximum, format_args!("{prefix}.{key}"))?
        } else {
            key
        };
        budget.text(key.len())?;
        match value {
            JsonValue::Object(nested) => flatten_object(Some(&key), nested, fields, budget)?,
            JsonValue::Array(_) => return Err(FileAdapterError::InvalidRecord),
            JsonValue::Text(value) => insert(fields, key, CellValue::Text(value), budget)?,
            JsonValue::Null => insert(fields, key, CellValue::Null, budget)?,
            JsonValue::Unsupported => insert(fields, key, CellValue::Unsupported, budget)?,
        }
    }
    Ok(())
}

fn insert(
    fields: &mut BTreeMap<String, CellValue>,
    key: String,
    value: CellValue,
    budget: &mut ParseBudget<'_>,
) -> Result<(), FileAdapterError> {
    budget.map_entry::<String, CellValue>()?;
    if fields.insert(key, value).is_some() {
        Err(FileAdapterError::DuplicateField)
    } else {
        Ok(())
    }
}
