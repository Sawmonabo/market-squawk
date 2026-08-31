//! Bounded parsers for official SEC submissions and Company Facts JSON shapes.

mod company_facts;
mod submissions;

use std::fmt;
use std::mem::size_of;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Datelike as _, NaiveDate, NaiveDateTime};
use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp};
use serde::de;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use tokio_util::sync::CancellationToken;

pub use company_facts::{CompanyFactOccurrence, CompanyFactPeriod, CompanyFactsDocument};
pub use submissions::{
    SecFiling, SecFormerName, SecSubmissionCompanyMetadata, SecSubmissionsCompanion,
    SecTickerExchangePair, SubmissionsArchive, SubmissionsDocument, reconcile_submissions,
    reconcile_submissions_with_cancellation,
};
pub(crate) use submissions::{admit_document_allocations, validate_companion_coverage};

/// Allocator-independent ceiling for one balanced-tree node's links, bookkeeping, and alignment.
/// String and value payloads are charged separately from this per-entry allocation ceiling.
const BTREE_NODE_OVERHEAD_CEILING: usize = 8 * size_of::<usize>();

#[derive(Clone)]
pub(crate) struct RetainedJsonBudget {
    state: Arc<Mutex<RetainedJsonBudgetState>>,
}

struct RetainedJsonBudgetState {
    admitted: usize,
    limit: usize,
}

impl RetainedJsonBudget {
    pub(crate) fn new(limits: SecParserLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(RetainedJsonBudgetState {
                admitted: 0,
                limit: limits.retained_output_bytes(),
            })),
        }
    }

    pub(crate) fn admit_bytes(&self, bytes: usize) -> Result<(), SecParserError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SecParserError::AllocationAuthorityPoisoned)?;
        let admitted = state
            .admitted
            .checked_add(bytes)
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
        if admitted > state.limit {
            return Err(SecParserError::RetainedOutputLimitExceeded);
        }
        state.admitted = admitted;
        Ok(())
    }

    fn reserve_remaining(&self, minimum: usize) -> Result<usize, SecParserError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SecParserError::AllocationAuthorityPoisoned)?;
        let remaining = state
            .limit
            .checked_sub(state.admitted)
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
        if minimum > remaining {
            return Err(SecParserError::RetainedOutputLimitExceeded);
        }
        state.admitted = state.limit;
        Ok(remaining)
    }

    fn release_bytes(&self, bytes: usize) -> Result<(), SecParserError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SecParserError::AllocationAuthorityPoisoned)?;
        state.admitted = state
            .admitted
            .checked_sub(bytes)
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
        Ok(())
    }

    pub(crate) fn admit_btree_entry<K, V>(
        &self,
        dynamic_bytes: usize,
    ) -> Result<(), SecParserError> {
        let inline = size_of::<K>()
            .checked_add(size_of::<V>())
            .and_then(|bytes| bytes.checked_add(BTREE_NODE_OVERHEAD_CEILING))
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
        self.admit_bytes(
            inline
                .checked_add(dynamic_bytes)
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )
    }
}

pub(crate) fn try_reserve_exact_bounded<T>(
    values: &mut Vec<T>,
    additional: usize,
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let old_capacity = values.capacity();
    let required = values
        .len()
        .checked_add(additional)
        .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
    let requested_delta = required.saturating_sub(old_capacity);
    let requested_bytes = requested_delta
        .checked_mul(size_of::<T>())
        .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
    if requested_bytes == 0 {
        return Ok(());
    }
    // Rust deliberately does not promise the capacity selected by `try_reserve_exact`. Reserve
    // every still-available byte in the aggregate authority before asking the allocator, then
    // reconcile downward to its reported capacity. This makes no allocator growth assumption.
    let admitted_bytes = retained.reserve_remaining(requested_bytes)?;
    if let Err(_error) = values.try_reserve_exact(additional) {
        retained.release_bytes(admitted_bytes)?;
        return Err(SecParserError::AllocationFailed);
    }
    let actual_bytes = values
        .capacity()
        .saturating_sub(old_capacity)
        .checked_mul(size_of::<T>())
        .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
    if actual_bytes > admitted_bytes {
        return Err(SecParserError::RetainedOutputLimitExceeded);
    }
    if actual_bytes < admitted_bytes {
        retained.release_bytes(admitted_bytes - actual_bytes)?;
    }
    Ok(())
}

pub(crate) fn owned_string_bounded(
    value: &str,
    retained: &RetainedJsonBudget,
) -> Result<String, SecParserError> {
    let mut owned = String::new();
    if value.is_empty() {
        return Ok(owned);
    }
    let admitted_bytes = retained.reserve_remaining(value.len())?;
    if owned.try_reserve_exact(value.len()).is_err() {
        retained.release_bytes(admitted_bytes)?;
        return Err(SecParserError::AllocationFailed);
    }
    let capacity = owned.capacity();
    if capacity > admitted_bytes {
        return Err(SecParserError::RetainedOutputLimitExceeded);
    }
    if capacity < admitted_bytes {
        retained.release_bytes(admitted_bytes - capacity)?;
    }
    owned.push_str(value);
    Ok(owned)
}

pub(crate) fn admit_string_allocation(
    value: &str,
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let capacity_ceiling = if value.is_empty() {
        0
    } else {
        value
            .len()
            .checked_next_power_of_two()
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?
    };
    retained.admit_bytes(capacity_ceiling)
}

pub(crate) fn source_identifier_bounded(
    value: &str,
    retained: &RetainedJsonBudget,
) -> Result<SourceIdentifier, SecParserError> {
    admit_string_allocation(value, retained)?;
    SourceIdentifier::try_from(value).map_err(Into::into)
}

/// Production parser ceilings applied before canonical construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecParserLimits {
    max_decoded_bytes: usize,
    max_records: usize,
    max_depth: usize,
    max_string_bytes: usize,
    max_total_string_bytes: usize,
    max_retained_output_bytes: usize,
}

impl SecParserLimits {
    /// Conservative defaults for individual SEC JSON documents.
    pub const fn production_defaults() -> Self {
        Self {
            max_decoded_bytes: 32 * 1024 * 1024,
            max_records: 250_000,
            max_depth: 128,
            max_string_bytes: 256 * 1024,
            max_total_string_bytes: 24 * 1024 * 1024,
            max_retained_output_bytes: 128 * 1024 * 1024,
        }
    }

    /// Constructs explicit nonzero parser ceilings.
    pub const fn try_new(
        max_decoded_bytes: usize,
        max_records: usize,
        max_depth: usize,
        max_string_bytes: usize,
        max_total_string_bytes: usize,
        max_retained_output_bytes: usize,
    ) -> Result<Self, SecParserError> {
        if max_decoded_bytes == 0
            || max_records == 0
            || max_depth == 0
            || max_string_bytes == 0
            || max_total_string_bytes == 0
            || max_retained_output_bytes == 0
            || max_string_bytes > max_total_string_bytes
        {
            return Err(SecParserError::InvalidLimits);
        }
        Ok(Self {
            max_decoded_bytes,
            max_records,
            max_depth,
            max_string_bytes,
            max_total_string_bytes,
            max_retained_output_bytes,
        })
    }

    /// Returns the aggregate retained-allocation ceiling for decoded trees and owned results.
    pub const fn retained_output_bytes(self) -> usize {
        self.max_retained_output_bytes
    }

    pub(crate) const fn decoded_bytes(self) -> usize {
        self.max_decoded_bytes
    }

    pub(crate) const fn records(self) -> usize {
        self.max_records
    }

    pub(crate) const fn depth(self) -> usize {
        self.max_depth
    }

    pub(crate) const fn string_bytes(self) -> usize {
        self.max_string_bytes
    }
}

pub(crate) fn parse_bounded_json_with_cancellation(
    bytes: &[u8],
    limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<Value, SecParserError> {
    parse_bounded_json_with_retained_budget(bytes, limits, cancellation).map(|(value, _)| value)
}

pub(crate) fn parse_bounded_json_with_retained_budget(
    bytes: &[u8],
    limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<(Value, RetainedJsonBudget), SecParserError> {
    let retained = RetainedJsonBudget::new(limits);
    let value = parse_bounded_json_with_allocation_authority(
        bytes,
        limits,
        cancellation,
        retained.clone(),
    )?;
    Ok((value, retained))
}

pub(crate) fn parse_bounded_json_with_allocation_authority(
    bytes: &[u8],
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: RetainedJsonBudget,
) -> Result<Value, SecParserError> {
    check_parser_cancelled(cancellation)?;
    if bytes.len() > limits.max_decoded_bytes {
        return Err(SecParserError::ByteLimitExceeded);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let mut budget = JsonBudget::new(limits, cancellation.clone(), retained);
    let value = BoundedValueSeed {
        budget: &mut budget,
        depth: 1,
    }
    .deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value)
}

struct JsonBudget {
    limits: SecParserLimits,
    total_string_bytes: usize,
    nodes: usize,
    max_nodes: usize,
    cancellation: CancellationToken,
    retained: RetainedJsonBudget,
}

impl JsonBudget {
    fn new(
        limits: SecParserLimits,
        cancellation: CancellationToken,
        retained: RetainedJsonBudget,
    ) -> Self {
        Self {
            limits,
            total_string_bytes: 0,
            nodes: 0,
            max_nodes: limits.max_records.saturating_mul(16),
            cancellation,
            retained,
        }
    }

    fn charge_node(&mut self) -> Result<(), SecParserError> {
        check_parser_cancelled(&self.cancellation)?;
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or(SecParserError::ByteCountOverflow)?;
        if self.nodes > self.max_nodes {
            Err(SecParserError::NodeLimitExceeded)
        } else {
            Ok(())
        }
    }

    fn charge_string(&mut self, text: &str) -> Result<(), SecParserError> {
        check_parser_cancelled(&self.cancellation)?;
        if text.len() > self.limits.max_string_bytes {
            return Err(SecParserError::StringLimitExceeded);
        }
        self.total_string_bytes = self
            .total_string_bytes
            .checked_add(text.len())
            .ok_or(SecParserError::ByteCountOverflow)?;
        if self.total_string_bytes > self.limits.max_total_string_bytes {
            Err(SecParserError::StringLimitExceeded)
        } else {
            Ok(())
        }
    }
}

struct BoundedValueSeed<'a> {
    budget: &'a mut JsonBudget,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for BoundedValueSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if self.depth > self.budget.limits.max_depth {
            return Err(de::Error::custom(SecParserError::DepthLimitExceeded));
        }
        self.budget.charge_node().map_err(de::Error::custom)?;
        deserializer.deserialize_any(BoundedValueVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

struct BoundedStringSeed<'a> {
    budget: &'a mut JsonBudget,
}

impl<'de> DeserializeSeed<'de> for BoundedStringSeed<'_> {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(BoundedStringVisitor {
            budget: self.budget,
        })
    }
}

struct BoundedStringVisitor<'a> {
    budget: &'a mut JsonBudget,
}

impl Visitor<'_> for BoundedStringVisitor<'_> {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget.charge_string(value).map_err(E::custom)?;
        owned_string_bounded(value, &self.budget.retained).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget.charge_string(&value).map_err(E::custom)?;
        self.budget
            .retained
            .admit_bytes(value.capacity())
            .map_err(E::custom)?;
        Ok(value)
    }
}

struct BoundedValueVisitor<'a> {
    budget: &'a mut JsonBudget,
    depth: usize,
}

impl<'de> Visitor<'de> for BoundedValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom(SecParserError::InvalidNumber))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget.charge_string(value).map_err(E::custom)?;
        owned_string_bounded(value, &self.budget.retained)
            .map(Value::String)
            .map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.budget.charge_string(&value).map_err(E::custom)?;
        self.budget
            .retained
            .admit_bytes(value.capacity())
            .map_err(E::custom)?;
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        BoundedValueSeed {
            budget: self.budget,
            depth: self.depth,
        }
        .deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let child_depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom(SecParserError::DepthLimitExceeded))?;
        let initial = sequence.size_hint().unwrap_or(0).min(1_024);
        let mut values = Vec::new();
        try_reserve_exact_bounded(&mut values, initial, &self.budget.retained)
            .map_err(de::Error::custom)?;
        while let Some(value) = sequence.next_element_seed(BoundedValueSeed {
            budget: self.budget,
            depth: child_depth,
        })? {
            if values.len() == values.capacity() {
                try_reserve_exact_bounded(&mut values, 1, &self.budget.retained)
                    .map_err(de::Error::custom)?;
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let child_depth = self
            .depth
            .checked_add(1)
            .ok_or_else(|| de::Error::custom(SecParserError::DepthLimitExceeded))?;
        let Some(first_key) = map.next_key_seed(BoundedStringSeed {
            budget: self.budget,
        })?
        else {
            return Ok(Value::Object(Map::new()));
        };
        self.budget.charge_node().map_err(de::Error::custom)?;
        if first_key == "$serde_json::private::Number" {
            let lexical = map.next_value_seed(BoundedStringSeed {
                budget: self.budget,
            })?;
            return Number::from_str(&lexical)
                .map(Value::Number)
                .map_err(de::Error::custom);
        }
        let mut values = Map::new();
        let first_value = map.next_value_seed(BoundedValueSeed {
            budget: self.budget,
            depth: child_depth,
        })?;
        self.budget
            .retained
            .admit_btree_entry::<String, Value>(0)
            .map_err(de::Error::custom)?;
        values.insert(first_key, first_value);
        while let Some(key) = map.next_key_seed(BoundedStringSeed {
            budget: self.budget,
        })? {
            self.budget.charge_node().map_err(de::Error::custom)?;
            if values.contains_key(&key) {
                return Err(de::Error::custom(SecParserError::DuplicateKey));
            }
            let value = map.next_value_seed(BoundedValueSeed {
                budget: self.budget,
                depth: child_depth,
            })?;
            self.budget
                .retained
                .admit_btree_entry::<String, Value>(0)
                .map_err(de::Error::custom)?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn owned_string(value: &str) -> Result<String, SecParserError> {
    let mut owned = String::new();
    owned
        .try_reserve(value.len())
        .map_err(|_| SecParserError::AllocationFailed)?;
    owned.push_str(value);
    Ok(owned)
}

fn check_parser_cancelled(cancellation: &CancellationToken) -> Result<(), SecParserError> {
    if cancellation.is_cancelled() {
        Err(SecParserError::Cancelled)
    } else {
        Ok(())
    }
}

fn parse_cik(value: &Value) -> Result<SourceIdentifier, SecParserError> {
    let raw = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return Err(SecParserError::InvalidCik),
    };
    if raw.len() > 10 || raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SecParserError::InvalidCik);
    }
    Ok(SourceIdentifier::try_from(format!("{raw:0>10}"))?)
}

fn parse_cik_with_allocation_authority(
    value: &Value,
    retained: &RetainedJsonBudget,
) -> Result<SourceIdentifier, SecParserError> {
    let raw = match value {
        Value::String(value) => value.as_str(),
        Value::Number(value) => {
            retained.admit_bytes(32)?;
            return parse_cik(&Value::String(value.to_string()));
        }
        _ => return Err(SecParserError::InvalidCik),
    };
    if raw.len() > 10 || raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SecParserError::InvalidCik);
    }
    retained.admit_bytes(10)?;
    Ok(SourceIdentifier::try_from(format!("{raw:0>10}"))?)
}

fn parse_date(value: &str) -> Result<CalendarDate, SecParserError> {
    let date =
        NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| SecParserError::InvalidDate)?;
    Ok(CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| SecParserError::InvalidDate)?,
        u8::try_from(date.month()).map_err(|_| SecParserError::InvalidDate)?,
        u8::try_from(date.day()).map_err(|_| SecParserError::InvalidDate)?,
    )?)
}

fn parse_acceptance_timestamp(value: &str) -> Result<Timestamp, SecParserError> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%d%H%M%S")
        .map_err(|_| SecParserError::InvalidTimestamp)?
        .and_utc();
    let nanos = timestamp
        .timestamp_nanos_opt()
        .ok_or(SecParserError::InvalidTimestamp)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn parse_rfc3339_timestamp(value: &str) -> Result<Timestamp, SecParserError> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| SecParserError::InvalidTimestamp)?
        .timestamp_nanos_opt()
        .ok_or(SecParserError::InvalidTimestamp)?;
    Ok(Timestamp::from_unix_nanos(timestamp))
}

fn validated_metadata_text(value: &str, max_bytes: usize) -> Result<String, SecParserError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(SecParserError::InvalidCompanyMetadata);
    }
    owned_string(value)
}

fn validate_accession(value: &str) -> Result<(), SecParserError> {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[10] != b'-'
        || bytes[13] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| index != 10 && index != 13 && !byte.is_ascii_digit())
    {
        Err(SecParserError::InvalidAccession)
    } else {
        Ok(())
    }
}

fn validate_accession_owner(
    value: &SourceIdentifier,
    cik: &SourceIdentifier,
) -> Result<(), SecParserError> {
    validate_accession(value.as_str())?;
    if value.as_str().get(..10) != Some(cik.as_str()) {
        return Err(SecParserError::InvalidAccessionOwner);
    }
    Ok(())
}

fn validate_component(value: &str) -> Result<(), SecParserError> {
    if value.is_empty()
        || value.len() > 240
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(SecParserError::InvalidConcept)
    } else {
        Ok(())
    }
}

fn required<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a Value, SecParserError> {
    object.get(key).ok_or(SecParserError::MissingField)
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a str, SecParserError> {
    required(object, key)?
        .as_str()
        .ok_or(SecParserError::WrongType)
}

fn optional_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>, SecParserError> {
    object
        .get(key)
        .map(|value| value.as_str().ok_or(SecParserError::WrongType))
        .transpose()
}

fn as_object<'a>(value: &'a Value, _field: &str) -> Result<&'a Map<String, Value>, SecParserError> {
    value.as_object().ok_or(SecParserError::WrongType)
}

fn as_array<'a>(value: &'a Value, _field: &str) -> Result<&'a [Value], SecParserError> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or(SecParserError::WrongType)
}

fn required_array<'a>(
    object: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a [Value], SecParserError> {
    as_array(required(object, key)?, key)
}

fn array_string(values: &[Value], index: usize) -> Result<&str, SecParserError> {
    values
        .get(index)
        .and_then(Value::as_str)
        .ok_or(SecParserError::WrongType)
}

fn column_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<&'a str, SecParserError> {
    array_string(required_array(object, key)?, index)
}

fn nonempty_column_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<Option<&'a str>, SecParserError> {
    Ok(match column_string(object, key, index)? {
        "" => None,
        value => Some(value),
    })
}

fn optional_nonempty_column_string<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<Option<&'a str>, SecParserError> {
    let Some(column) = object.get(key) else {
        return Ok(None);
    };
    Ok(match array_string(as_array(column, key)?, index)? {
        "" => None,
        value => Some(value),
    })
}

fn optional_column_u64(
    object: &Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<Option<u64>, SecParserError> {
    object
        .get(key)
        .map(|value| {
            as_array(value, key)?
                .get(index)
                .and_then(Value::as_u64)
                .ok_or(SecParserError::WrongType)
        })
        .transpose()
}

fn optional_column_boolish(
    object: &Map<String, Value>,
    key: &str,
    index: usize,
) -> Result<Option<bool>, SecParserError> {
    object
        .get(key)
        .map(|value| {
            match as_array(value, key)?
                .get(index)
                .ok_or(SecParserError::WrongType)?
            {
                Value::Bool(value) => Ok(*value),
                Value::Number(value) if value.as_u64() == Some(0) => Ok(false),
                Value::Number(value) if value.as_u64() == Some(1) => Ok(true),
                _ => Err(SecParserError::WrongType),
            }
        })
        .transpose()
}

/// SEC parser failure class.
#[derive(Debug)]
pub enum SecParserError {
    Cancelled,
    InvalidLimits,
    ByteLimitExceeded,
    DepthLimitExceeded,
    StringLimitExceeded,
    RecordLimitExceeded,
    NodeLimitExceeded,
    ByteCountOverflow,
    AllocationFailed,
    AllocationAuthorityPoisoned,
    RetainedOutputLimitExceeded,
    DuplicateKey,
    InvalidNumber,
    MissingField,
    WrongType,
    ColumnLengthMismatch,
    InvalidCik,
    InvalidAccession,
    InvalidAccessionOwner,
    InvalidCompanionName,
    InvalidCompanionCoverage,
    InvalidConcept,
    InvalidDate,
    InvalidTimestamp,
    InvalidPeriod,
    InvalidFiscalContext,
    InvalidDecimal,
    NonNumericCompanyFact,
    ConflictingAccession,
    InvalidCompanyMetadata,
    MetadataAssociationLengthMismatch,
    DuplicateMetadataAssociation,
    ConflictingMetadataAssociation,
    Json(serde_json::Error),
    Identity(market_squawk_domain::IdentityError),
    Time(market_squawk_domain::TimeError),
}

impl fmt::Display for SecParserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SEC parser rejected input: {self:?}")
    }
}

impl std::error::Error for SecParserError {}

impl From<serde_json::Error> for SecParserError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<market_squawk_domain::IdentityError> for SecParserError {
    fn from(value: market_squawk_domain::IdentityError) -> Self {
        Self::Identity(value)
    }
}

impl From<market_squawk_domain::TimeError> for SecParserError {
    fn from(value: market_squawk_domain::TimeError) -> Self {
        Self::Time(value)
    }
}
