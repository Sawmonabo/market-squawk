//! Bounded BEA JSON envelope, metadata, dimension, and observation parsing.

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use chrono::NaiveDateTime;
use market_squawk_domain::Timestamp;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use crate::auth::{BEA_REDACTED_USER_ID, BeaSanitizedBody, BeaSensitiveBody};
use crate::{
    BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE, BEA_MAX_APPLICATION_ROWS_PER_PAGE,
    BEA_REGIONAL_SUPPRESSION_MARKER, BeaCompleteness, BeaDataPage, BeaDataType,
    BeaDatasetDefinition, BeaDatasetIdentity, BeaDimension, BeaError, BeaFrequency,
    BeaMetadataPage, BeaMetadataRecords, BeaMethod, BeaMissingValue, BeaNote, BeaObservation,
    BeaObservationIdentity, BeaObservationValue, BeaPageReceipt, BeaParameterDataType,
    BeaParameterDefinition, BeaParameterIdentity, BeaParameterValueDefinition, BeaProductionTime,
    BeaProviderError, BeaRequest, BeaTimePeriod, BeaUnit, BeaUserId,
};

const MAX_METADATA_RECORDS: usize = 20_000;
const MAX_STRING_BYTES: usize = 64 * 1024;
const MAX_DIMENSIONS: usize = 128;
const MAX_NOTES: usize = 10_000;

/// Explicit parser limits for one BEA response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeaParseLimits {
    max_rows: usize,
    max_metadata_records: usize,
    max_bytes: usize,
    max_string_bytes: usize,
    max_dimensions: usize,
    max_notes: usize,
}

impl BeaParseLimits {
    /// Builds explicit nonzero limits.
    #[allow(
        clippy::too_many_arguments,
        reason = "each independently exhausted parser resource remains visible"
    )]
    pub fn try_new(
        max_rows: usize,
        max_metadata_records: usize,
        max_bytes: usize,
        max_string_bytes: usize,
        max_dimensions: usize,
        max_notes: usize,
    ) -> Result<Self, BeaError> {
        if max_rows == 0
            || max_metadata_records == 0
            || max_bytes == 0
            || max_string_bytes == 0
            || max_dimensions == 0
            || max_notes == 0
            || max_string_bytes > max_bytes
            || max_rows > BEA_MAX_APPLICATION_ROWS_PER_PAGE
            || u64::try_from(max_bytes).map_or(true, |bytes| {
                bytes > BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE
            })
        {
            return Err(BeaError::InvalidLimit);
        }
        Ok(Self {
            max_rows,
            max_metadata_records,
            max_bytes,
            max_string_bytes,
            max_dimensions,
            max_notes,
        })
    }

    /// Returns the application-policy production limits.
    pub const fn production_defaults() -> Self {
        Self {
            max_rows: BEA_MAX_APPLICATION_ROWS_PER_PAGE,
            max_metadata_records: MAX_METADATA_RECORDS,
            max_bytes: BEA_APPLICATION_RESPONSE_BYTES_PER_MINUTE as usize,
            max_string_bytes: MAX_STRING_BYTES,
            max_dimensions: MAX_DIMENSIONS,
            max_notes: MAX_NOTES,
        }
    }

    /// Returns the admitted data-row ceiling.
    pub const fn max_rows(self) -> usize {
        self.max_rows
    }

    /// Returns the admitted metadata-record ceiling.
    pub const fn max_metadata_records(self) -> usize {
        self.max_metadata_records
    }

    /// Returns the admitted response byte ceiling.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the maximum retained provider string bytes.
    pub const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }

    /// Returns the maximum declared response dimensions.
    pub const fn max_dimensions(self) -> usize {
        self.max_dimensions
    }

    /// Returns the maximum retained provider notes.
    pub const fn max_notes(self) -> usize {
        self.max_notes
    }
}

/// Parses one exact metadata response after validating and redacting the complete echoed request.
///
/// # Errors
///
/// Rejects oversized/invalid JSON, a mismatched echo, provider errors, an unexpected method
/// result, or any collection beyond the explicit limits.
pub fn parse_metadata_page(
    bytes: &[u8],
    request: &BeaRequest,
    user_id: &BeaUserId,
    limits: BeaParseLimits,
) -> Result<BeaMetadataPage, BeaError> {
    let body = sanitize_borrowed_response(bytes, request, user_id, limits)?;
    parse_metadata_page_sanitized(body.bytes(), request, limits)?
        .bind_sanitized_capture(body.upstream_digest(), body.retained_digest())
}

pub(crate) fn parse_metadata_page_sanitized(
    bytes: &[u8],
    request: &BeaRequest,
    limits: BeaParseLimits,
) -> Result<BeaMetadataPage, BeaError> {
    parse_metadata_page_sanitized_inner(bytes, request, limits)
}

fn parse_metadata_page_sanitized_inner(
    bytes: &[u8],
    request: &BeaRequest,
    limits: BeaParseLimits,
) -> Result<BeaMetadataPage, BeaError> {
    if !request.query().method().is_metadata() {
        return Err(BeaError::InvalidRequest);
    }
    let EnvelopeParts {
        mut request_echo,
        results,
    } = parse_envelope(bytes, limits)?;
    validate_request_echo(&mut request_echo, request, limits)?;
    let mut results = result_object(results)?;
    reject_provider_error(&mut results, request.query().method(), limits)?;
    let records = match request.query().method() {
        BeaMethod::GetDatasetList => {
            let values = take_required_array(&mut results, "Dataset")?;
            ensure_empty(&results, "dataset result")?;
            BeaMetadataRecords::Datasets(parse_datasets(values, limits)?)
        }
        BeaMethod::GetParameterList => {
            let values = take_required_array(&mut results, "Parameter")?;
            ensure_empty(&results, "parameter result")?;
            BeaMetadataRecords::Parameters(parse_parameters(values, limits)?)
        }
        BeaMethod::GetParameterValues | BeaMethod::GetParameterValuesFiltered => {
            let values = take_required_array(&mut results, "ParamValue")?;
            ensure_empty(&results, "parameter-value result")?;
            BeaMetadataRecords::ParameterValues(parse_parameter_values(values, limits)?)
        }
        BeaMethod::GetData => return Err(BeaError::InvalidRequest),
    };
    let receipt = page_receipt(bytes, request, records.len(), limits)?;
    Ok(BeaMetadataPage::new(
        request.query().method(),
        records,
        receipt,
    ))
}

/// Parses one exact `GetData` response after validated echo redaction.
///
/// # Errors
///
/// Rejects schema drift, a mismatched echoed request, provider errors, unbounded collections,
/// missing required `TimePeriod`/`CL_UNIT`/`UNIT_MULT` dimensions, invalid note references, and
/// non-exact numeric values.
pub fn parse_data_page(
    bytes: &[u8],
    request: &BeaRequest,
    user_id: &BeaUserId,
    limits: BeaParseLimits,
) -> Result<BeaDataPage, BeaError> {
    let body = sanitize_borrowed_response(bytes, request, user_id, limits)?;
    parse_data_page_sanitized(body.bytes(), request, limits)?
        .bind_sanitized_capture(body.upstream_digest(), body.retained_digest())
}

pub(crate) fn parse_data_page_sanitized(
    bytes: &[u8],
    request: &BeaRequest,
    limits: BeaParseLimits,
) -> Result<BeaDataPage, BeaError> {
    parse_data_page_sanitized_inner(bytes, request, limits)
}

fn parse_data_page_sanitized_inner(
    bytes: &[u8],
    request: &BeaRequest,
    limits: BeaParseLimits,
) -> Result<BeaDataPage, BeaError> {
    if request.query().method() != BeaMethod::GetData {
        return Err(BeaError::InvalidRequest);
    }
    let dataset = request
        .query()
        .dataset()
        .cloned()
        .ok_or(BeaError::InvalidRequest)?;
    let metadata_generation = request
        .query()
        .metadata_generation()
        .ok_or(BeaError::InvalidRequest)?;
    let EnvelopeParts {
        mut request_echo,
        results,
    } = parse_envelope(bytes, limits)?;
    validate_request_echo(&mut request_echo, request, limits)?;
    let mut results = result_object(results)?;
    reject_provider_error(&mut results, request.query().method(), limits)?;

    let dimensions = parse_dimensions(take_required_array(&mut results, "Dimensions")?, limits)?;
    let dimension_names = semantic_dimension_names(&dimensions)?;
    let notes = parse_notes(take_optional_array(&mut results, "Notes")?, limits)?;
    let note_index = note_index(&notes)?;
    let result_note_references = take_optional_string(&mut results, "NoteRef", limits)?
        .map(|value| split_note_references(&value, limits))
        .transpose()?
        .unwrap_or_default();
    validate_note_references(&result_note_references, &note_index)?;
    let production_time = take_optional_string(&mut results, "UTCProductionTime", limits)?
        .map(parse_production_time)
        .transpose()?;
    let data = take_required_array(&mut results, "Data")?;
    if data.len() > limits.max_rows {
        return Err(BeaError::RowLimitExceeded);
    }
    let result_attributes = parse_result_attributes(results, limits)?;
    let observations = parse_observations(
        data,
        &dimensions,
        dimension_names,
        &dataset,
        request,
        &note_index,
        limits,
    )?;
    let receipt = page_receipt(bytes, request, observations.len(), limits)?;
    Ok(BeaDataPage::new(
        dataset,
        metadata_generation,
        result_attributes,
        dimensions,
        observations,
        notes,
        result_note_references,
        production_time,
        receipt,
    ))
}

struct EnvelopeParts {
    request_echo: Vec<RequestParameterWire>,
    results: Value,
}

fn parse_envelope(bytes: &[u8], limits: BeaParseLimits) -> Result<EnvelopeParts, BeaError> {
    if bytes.is_empty() {
        return Err(BeaError::InvalidField("body"));
    }
    if bytes.len() > limits.max_bytes {
        return Err(BeaError::BodyTooLarge);
    }
    let wire: EnvelopeWire = serde_json::from_slice(bytes).map_err(|_| BeaError::InvalidJson)?;
    if wire.bea_api.request.request_parameters.len() > 64 {
        return Err(BeaError::InvalidField("request echo"));
    }
    Ok(EnvelopeParts {
        request_echo: wire.bea_api.request.request_parameters,
        results: wire.bea_api.results,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeWire {
    #[serde(rename = "BEAAPI")]
    bea_api: ApiWire,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiWire {
    #[serde(rename = "Request")]
    request: RequestWire,
    #[serde(rename = "Results")]
    results: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestWire {
    #[serde(rename = "RequestParam")]
    request_parameters: Vec<RequestParameterWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestParameterWire {
    #[serde(rename = "ParameterName")]
    name: String,
    #[serde(rename = "ParameterValue")]
    value: String,
}

impl Drop for RequestParameterWire {
    fn drop(&mut self) {
        self.value.zeroize();
    }
}

fn validate_request_echo(
    echoed: &mut [RequestParameterWire],
    request: &BeaRequest,
    limits: BeaParseLimits,
) -> Result<(), BeaError> {
    let mut expected = BTreeMap::new();
    for (name, value) in request.query_pairs() {
        expected.insert(name.to_ascii_uppercase(), value);
    }
    let mut seen = BTreeSet::new();
    let mut user_id_seen = false;
    for parameter in echoed {
        validate_string(&parameter.name, limits)?;
        validate_string(&parameter.value, limits)?;
        let name = parameter.name.to_ascii_uppercase();
        if !seen.insert(name.clone()) {
            if name == "USERID" {
                parameter.value.zeroize();
            }
            return Err(BeaError::RequestEchoMismatch);
        }
        if name == "USERID" {
            let matches = parameter.value.as_bytes() == BEA_REDACTED_USER_ID;
            parameter.value.zeroize();
            if !matches {
                return Err(BeaError::RequestEchoMismatch);
            }
            user_id_seen = true;
            continue;
        }
        let Some(expected_value) = expected.remove(&name) else {
            return Err(BeaError::RequestEchoMismatch);
        };
        if !echo_value_matches(expected_value, &parameter.value) {
            return Err(BeaError::RequestEchoMismatch);
        }
    }
    if !user_id_seen || !expected.is_empty() {
        return Err(BeaError::RequestEchoMismatch);
    }
    Ok(())
}

fn sanitize_borrowed_response(
    bytes: &[u8],
    request: &BeaRequest,
    user_id: &BeaUserId,
    limits: BeaParseLimits,
) -> Result<BeaSanitizedBody, BeaError> {
    if bytes.is_empty() || bytes.len() > limits.max_bytes() {
        return Err(BeaError::BodyTooLarge);
    }
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(bytes.len())
        .map_err(|_| BeaError::Allocation)?;
    owned.extend_from_slice(bytes);
    sanitize_response_body(BeaSensitiveBody::from_vec(owned), request, user_id, limits)
}

/// Replaces the sole literal `UserID` before structurally validating the sanitized response.
pub(crate) fn sanitize_response_body(
    body: BeaSensitiveBody,
    request: &BeaRequest,
    user_id: &BeaUserId,
    limits: BeaParseLimits,
) -> Result<BeaSanitizedBody, BeaError> {
    if body.is_empty() || body.len() > limits.max_bytes() {
        return Err(BeaError::BodyTooLarge);
    }
    let mut original = body.into_zeroizing();
    let upstream_digest = Sha256::digest(original.as_slice()).into();
    let secret = user_id.expose_secret().as_bytes();
    if original
        .windows(BEA_REDACTED_USER_ID.len())
        .any(|candidate| candidate == BEA_REDACTED_USER_ID)
    {
        return Err(BeaError::RequestEchoMismatch);
    }
    let mut matches = original
        .windows(secret.len())
        .enumerate()
        .filter_map(|(offset, candidate)| (candidate == secret).then_some(offset));
    let offset = matches.next().ok_or(BeaError::RequestEchoMismatch)?;
    if matches.next().is_some() {
        return Err(BeaError::RequestEchoMismatch);
    }
    original[offset..offset + secret.len()].copy_from_slice(BEA_REDACTED_USER_ID);
    let mut wire: EnvelopeWire =
        serde_json::from_slice(original.as_slice()).map_err(|_| BeaError::InvalidJson)?;
    if wire.bea_api.request.request_parameters.len() > 64 {
        return Err(BeaError::InvalidField("request echo"));
    }
    let mut unexpected_secret = false;
    for parameter in &mut wire.bea_api.request.request_parameters {
        if parameter.name.contains(user_id.expose_secret()) {
            parameter.name.zeroize();
            unexpected_secret = true;
        }
        if parameter.value.contains(user_id.expose_secret()) {
            parameter.value.zeroize();
            unexpected_secret = true;
        }
    }
    unexpected_secret =
        scrub_unexpected_secret(&mut wire.bea_api.results, user_id) || unexpected_secret;
    if unexpected_secret {
        return Err(BeaError::RequestEchoMismatch);
    }
    validate_request_echo(
        &mut wire.bea_api.request.request_parameters,
        request,
        limits,
    )?;
    let sanitized = mem::take(&mut *original);
    Ok(BeaSanitizedBody::from_secret_free_vec(
        sanitized,
        upstream_digest,
    ))
}

/// Removes every decoded result-field occurrence before rejecting an unexpected credential echo.
///
/// This cleanup prevents a hostile or malformed provider response from leaving a credential copy
/// in an ordinary `serde_json::Value` string while ensuring no such response can reach parsing,
/// capture, telemetry, doctor evidence, or canonicalization.
fn scrub_unexpected_secret(value: &mut Value, user_id: &BeaUserId) -> bool {
    match value {
        Value::String(value) => {
            if value.contains(user_id.expose_secret()) {
                value.zeroize();
                true
            } else {
                false
            }
        }
        Value::Array(values) => {
            let mut found = false;
            for value in values {
                found |= scrub_unexpected_secret(value, user_id);
            }
            found
        }
        Value::Object(values) => {
            let secret_in_name = values
                .keys()
                .any(|name| name.contains(user_id.expose_secret()));
            if secret_in_name {
                let original = mem::take(values);
                let mut found = true;
                for (mut name, mut value) in original {
                    if name.contains(user_id.expose_secret()) {
                        name.zeroize();
                    }
                    found |= scrub_unexpected_secret(&mut value, user_id);
                }
                found
            } else {
                let mut found = false;
                for value in values.values_mut() {
                    found |= scrub_unexpected_secret(value, user_id);
                }
                found
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn echo_value_matches(expected: &str, actual: &str) -> bool {
    if expected.is_ascii() && actual.is_ascii() {
        expected.eq_ignore_ascii_case(actual)
    } else {
        expected == actual
    }
}

fn result_object(value: Value) -> Result<Map<String, Value>, BeaError> {
    match value {
        Value::Object(value) => Ok(value),
        Value::Array(mut values) if values.len() == 1 => match values.pop() {
            Some(Value::Object(value)) => Ok(value),
            Some(_) | None => Err(BeaError::InvalidField("Results")),
        },
        _ => Err(BeaError::InvalidField("Results")),
    }
}

fn reject_provider_error(
    results: &mut Map<String, Value>,
    method: BeaMethod,
    limits: BeaParseLimits,
) -> Result<(), BeaError> {
    let Some(error) = remove_case_insensitive(results, "Error")? else {
        return Ok(());
    };
    if !results.is_empty() {
        return Err(BeaError::InvalidField("provider error result"));
    }
    let mut error = match error {
        Value::Object(error) => error,
        _ => return Err(BeaError::InvalidField("Error")),
    };
    let code = take_required_scalar_string(&mut error, "APIErrorCode", limits)?
        .parse::<u32>()
        .map_err(|_| BeaError::InvalidField("APIErrorCode"))?;
    let description = take_required_string(&mut error, "APIErrorDescription", limits)?;
    ensure_empty(&error, "provider error")?;
    if code == 34 && method == BeaMethod::GetParameterValuesFiltered {
        return Err(BeaError::FilteredParameterValuesUnsupported);
    }
    Err(BeaError::Provider(BeaProviderError::new(code, description)))
}

fn parse_datasets(
    values: Vec<Value>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaDatasetDefinition>, BeaError> {
    admit_metadata_count(values.len(), limits)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    let mut identities = BTreeSet::new();
    for value in values {
        let mut value = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("Dataset")),
        };
        let identity =
            BeaDatasetIdentity::try_new(take_required_string(&mut value, "DatasetName", limits)?)?;
        let description = take_required_string(&mut value, "DatasetDescription", limits)?;
        ensure_empty(&value, "Dataset")?;
        if !identities.insert(identity.as_str().to_ascii_uppercase()) {
            return Err(BeaError::InvalidField("duplicate dataset"));
        }
        output.push(BeaDatasetDefinition::new(identity, description));
    }
    Ok(output)
}

fn parse_parameters(
    values: Vec<Value>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaParameterDefinition>, BeaError> {
    admit_metadata_count(values.len(), limits)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    let mut identities = BTreeSet::new();
    for value in values {
        let mut value = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("Parameter")),
        };
        let identity = BeaParameterIdentity::try_new(take_required_string(
            &mut value,
            "ParameterName",
            limits,
        )?)?;
        let data_type = match take_required_string(&mut value, "ParameterDataType", limits)?
            .to_ascii_lowercase()
            .as_str()
        {
            "string" => BeaParameterDataType::String,
            "integer" => BeaParameterDataType::Integer,
            _ => return Err(BeaError::InvalidField("ParameterDataType")),
        };
        let description = take_required_string(&mut value, "ParameterDescription", limits)?;
        let required = take_flag_alias(
            &mut value,
            "ParameterIsRequiredFlag",
            "ParameterIsRequired",
            limits,
        )?;
        let multiple_values = take_flag_alias(
            &mut value,
            "MultipleAcceptedFlag",
            "MultipleAccepted",
            limits,
        )?;
        let default_value = take_optional_string(&mut value, "ParameterDefaultValue", limits)?;
        let all_value = take_optional_string(&mut value, "AllValue", limits)?;
        ensure_empty(&value, "Parameter")?;
        if !identities.insert(identity.as_str().to_ascii_uppercase()) {
            return Err(BeaError::InvalidField("duplicate parameter"));
        }
        output.push(BeaParameterDefinition::new(
            identity,
            data_type,
            description,
            required,
            multiple_values,
            default_value,
            all_value,
        ));
    }
    Ok(output)
}

fn parse_parameter_values(
    values: Vec<Value>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaParameterValueDefinition>, BeaError> {
    admit_metadata_count(values.len(), limits)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    let mut keys = BTreeSet::new();
    for value in values {
        let mut value = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("ParamValue")),
        };
        let key = take_required_scalar_string(&mut value, "Key", limits)?;
        if key.is_empty() || !keys.insert(key.clone()) {
            return Err(BeaError::InvalidField("parameter value key"));
        }
        let description = take_optional_string_alias(&mut value, "Desc", "Description", limits)?;
        let attributes = parse_result_attributes(value, limits)?;
        output.push(BeaParameterValueDefinition::new(
            key,
            description,
            attributes,
        ));
    }
    Ok(output)
}

fn parse_dimensions(
    values: Vec<Value>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaDimension>, BeaError> {
    if values.is_empty() || values.len() > limits.max_dimensions {
        return Err(BeaError::InvalidField("Dimensions"));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    let mut names = BTreeSet::new();
    let mut ordinals = BTreeSet::new();
    let mut ordinal_presence = None;
    let mut value_dimensions = 0usize;
    for value in values {
        let mut value = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("Dimension")),
        };
        let name = take_required_string(&mut value, "Name", limits)?;
        if name.is_empty() || !names.insert(name.to_ascii_uppercase()) {
            return Err(BeaError::InvalidField("dimension name"));
        }
        let ordinal = take_optional_scalar_string(&mut value, "Ordinal", limits)?
            .map(|value| {
                value
                    .parse::<u16>()
                    .map_err(|_| BeaError::InvalidField("dimension ordinal"))
            })
            .transpose()?;
        if ordinal == Some(0) || ordinal.is_some_and(|value| !ordinals.insert(value)) {
            return Err(BeaError::InvalidField("dimension ordinal"));
        }
        if ordinal_presence.is_some_and(|present| present != ordinal.is_some()) {
            return Err(BeaError::InvalidField("dimension ordinal"));
        }
        ordinal_presence.get_or_insert(ordinal.is_some());
        let data_type = match take_required_string(&mut value, "DataType", limits)?
            .to_ascii_lowercase()
            .as_str()
        {
            "string" => BeaDataType::String,
            "numeric" => BeaDataType::Numeric,
            _ => return Err(BeaError::InvalidField("dimension data type")),
        };
        let is_value = take_flag(&mut value, "IsValue", limits)?;
        value_dimensions = value_dimensions.saturating_add(usize::from(is_value));
        ensure_empty(&value, "Dimension")?;
        output.push(BeaDimension::new(name, ordinal, data_type, is_value));
    }
    if value_dimensions != 1 {
        return Err(BeaError::InvalidField("value dimension"));
    }
    Ok(output)
}

struct SemanticDimensionNames<'a> {
    value: &'a str,
    time_period: &'a str,
    cl_unit: &'a str,
    unit_multiplier: &'a str,
}

fn semantic_dimension_names(
    dimensions: &[BeaDimension],
) -> Result<SemanticDimensionNames<'_>, BeaError> {
    let value = dimensions
        .iter()
        .find(|dimension| dimension.is_value())
        .map(BeaDimension::name)
        .ok_or(BeaError::InvalidField("value dimension"))?;
    let time_period = named_dimension(dimensions, "TimePeriod", BeaDataType::String)?;
    let cl_unit = named_dimension(dimensions, "CL_UNIT", BeaDataType::String)?;
    let unit_multiplier = named_dimension(dimensions, "UNIT_MULT", BeaDataType::Numeric)?;
    Ok(SemanticDimensionNames {
        value,
        time_period,
        cl_unit,
        unit_multiplier,
    })
}

fn named_dimension<'a>(
    dimensions: &'a [BeaDimension],
    name: &'static str,
    data_type: BeaDataType,
) -> Result<&'a str, BeaError> {
    dimensions
        .iter()
        .find(|dimension| dimension.name().eq_ignore_ascii_case(name))
        .filter(|dimension| dimension.data_type() == data_type && !dimension.is_value())
        .map(BeaDimension::name)
        .ok_or(BeaError::InvalidField(name))
}

fn parse_notes(
    values: Option<Vec<Value>>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaNote>, BeaError> {
    let values = values.unwrap_or_default();
    if values.len() > limits.max_notes {
        return Err(BeaError::RowLimitExceeded);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    for value in values {
        let mut value = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("Note")),
        };
        let reference = take_required_string(&mut value, "NoteRef", limits)?;
        let text = take_required_string(&mut value, "NoteText", limits)?;
        if text.is_empty() {
            return Err(BeaError::InvalidField("NoteText"));
        }
        ensure_empty(&value, "Note")?;
        output.push(BeaNote::new(reference, text));
    }
    Ok(output)
}

fn note_index(notes: &[BeaNote]) -> Result<BTreeSet<String>, BeaError> {
    let mut index = BTreeSet::new();
    for note in notes {
        if !index.insert(note.reference().to_owned()) {
            return Err(BeaError::InvalidField("duplicate NoteRef"));
        }
    }
    Ok(index)
}

#[allow(
    clippy::too_many_arguments,
    reason = "dimensions, identity, request scope, notes, and bounds remain explicit"
)]
fn parse_observations(
    values: Vec<Value>,
    dimensions: &[BeaDimension],
    semantic_names: SemanticDimensionNames<'_>,
    dataset: &BeaDatasetIdentity,
    request: &BeaRequest,
    note_index: &BTreeSet<String>,
    limits: BeaParseLimits,
) -> Result<Vec<BeaObservation>, BeaError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| BeaError::Allocation)?;
    let mut identities = BTreeSet::new();
    for value in values {
        let mut row = match value {
            Value::Object(value) => value,
            _ => return Err(BeaError::InvalidField("Data row")),
        };
        let note_references = take_optional_string(&mut row, "NoteRef", limits)?
            .map(|value| split_note_references(&value, limits))
            .transpose()?
            .unwrap_or_default();
        validate_note_references(&note_references, note_index)?;

        let value = take_observation_value(&mut row, semantic_names.value, dataset, limits)?;
        let mut members = BTreeMap::new();
        for dimension in dimensions.iter().filter(|dimension| !dimension.is_value()) {
            let member = take_required_dimension_scalar(&mut row, dimension.name(), limits)?;
            members.insert(dimension.name().to_owned(), member);
        }
        ensure_empty(&row, "Data row")?;
        let period = parse_time_period(
            member(&members, semantic_names.time_period)
                .ok_or(BeaError::InvalidField("TimePeriod"))?,
        )?;
        let cl_unit = member(&members, semantic_names.cl_unit)
            .filter(|value| !value.trim().is_empty())
            .ok_or(BeaError::InvalidField("CL_UNIT"))?
            .to_owned();
        let unit_multiplier = member(&members, semantic_names.unit_multiplier)
            .ok_or(BeaError::InvalidField("UNIT_MULT"))?
            .trim()
            .parse::<i16>()
            .map_err(|_| BeaError::InvalidField("UNIT_MULT"))?;
        let table =
            first_member_or_parameter(&members, request, &["TableName", "TableID", "TableId"]);
        let line = first_member_or_parameter(
            &members,
            request,
            &["SeriesCode", "LineNumber", "LineCode", "Code", "Indicator"],
        );
        let identity = BeaObservationIdentity::new(dataset.clone(), table, line, members)?;
        let identity_key = (identity.digest(), period.raw().to_owned());
        if !identities.insert(identity_key) {
            return Err(BeaError::InvalidField("duplicate observation"));
        }
        output.push(BeaObservation::new(
            identity,
            period,
            value,
            BeaUnit::new(cl_unit, unit_multiplier),
            note_references,
        )?);
    }
    Ok(output)
}

fn take_observation_value(
    row: &mut Map<String, Value>,
    name: &str,
    dataset: &BeaDatasetIdentity,
    limits: BeaParseLimits,
) -> Result<BeaObservationValue, BeaError> {
    let Some(value) = remove_case_insensitive(row, name)? else {
        return Ok(BeaObservationValue::Missing(BeaMissingValue::Absent));
    };
    match value {
        Value::Null => Ok(BeaObservationValue::Missing(BeaMissingValue::Absent)),
        Value::String(raw) => {
            validate_string(&raw, limits)?;
            if raw.trim().is_empty() {
                return Ok(BeaObservationValue::Missing(BeaMissingValue::Blank));
            }
            if dataset.as_str().eq_ignore_ascii_case("Regional")
                && raw.trim() == BEA_REGIONAL_SUPPRESSION_MARKER
            {
                return Ok(BeaObservationValue::Missing(
                    BeaMissingValue::SuppressedRegional,
                ));
            }
            let value = parse_decimal(&raw)?;
            Ok(BeaObservationValue::Observed { value, raw })
        }
        Value::Number(number) => {
            let raw = number.to_string();
            validate_string(&raw, limits)?;
            let value = Decimal::from_str_exact(&raw).map_err(|_| BeaError::InvalidDecimal)?;
            Ok(BeaObservationValue::Observed { value, raw })
        }
        _ => Err(BeaError::InvalidField("value dimension")),
    }
}

fn parse_decimal(raw: &str) -> Result<Decimal, BeaError> {
    let trimmed = raw.trim();
    if trimmed.contains(',') && !valid_grouped_decimal(trimmed) {
        return Err(BeaError::InvalidDecimal);
    }
    let normalized = trimmed.replace(',', "");
    Decimal::from_str_exact(&normalized).map_err(|_| BeaError::InvalidDecimal)
}

fn valid_grouped_decimal(value: &str) -> bool {
    let unsigned = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let mut groups = integer.split(',');
    let Some(first) = groups.next() else {
        return false;
    };
    if first.is_empty() || first.len() > 3 || !first.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    groups.all(|group| group.len() == 3 && group.bytes().all(|byte| byte.is_ascii_digit()))
}

fn parse_time_period(raw: &str) -> Result<BeaTimePeriod, BeaError> {
    if raw.len() < 4 || !raw[..4].bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(BeaError::InvalidTimePeriod);
    }
    let year = raw[..4]
        .parse::<u16>()
        .map_err(|_| BeaError::InvalidTimePeriod)?;
    if year == 0 {
        return Err(BeaError::InvalidTimePeriod);
    }
    let suffix = &raw[4..];
    let (frequency, ordinal) = if suffix.is_empty() {
        (BeaFrequency::Annual, 1)
    } else if let Some(quarter) = suffix.strip_prefix('Q') {
        let quarter = quarter
            .parse::<u8>()
            .map_err(|_| BeaError::InvalidTimePeriod)?;
        if !(1..=4).contains(&quarter) {
            return Err(BeaError::InvalidTimePeriod);
        }
        (BeaFrequency::Quarterly, quarter)
    } else if let Some(month) = suffix.strip_prefix('M') {
        let month = month
            .parse::<u8>()
            .map_err(|_| BeaError::InvalidTimePeriod)?;
        if !(1..=12).contains(&month) {
            return Err(BeaError::InvalidTimePeriod);
        }
        (BeaFrequency::Monthly, month)
    } else {
        return Err(BeaError::InvalidTimePeriod);
    };
    Ok(BeaTimePeriod::new(raw.to_owned(), year, frequency, ordinal))
}

fn parse_production_time(raw: String) -> Result<BeaProductionTime, BeaError> {
    let value = NaiveDateTime::parse_from_str(&raw, "%Y-%m-%dT%H:%M:%S%.f")
        .map_err(|_| BeaError::InvalidField("UTCProductionTime"))?
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or(BeaError::InvalidField("UTCProductionTime"))?;
    Ok(BeaProductionTime::new(
        raw,
        Timestamp::from_unix_nanos(value),
    ))
}

fn first_member_or_parameter(
    members: &BTreeMap<String, String>,
    request: &BeaRequest,
    candidates: &[&str],
) -> Option<String> {
    candidates
        .iter()
        .find_map(|candidate| member(members, candidate).map(ToOwned::to_owned))
        .or_else(|| {
            candidates.iter().find_map(|candidate| {
                request
                    .query()
                    .supplied_parameters()
                    .iter()
                    .find(|(name, _)| name.as_str().eq_ignore_ascii_case(candidate))
                    .map(|(_, value)| value.clone())
            })
        })
}

fn member<'a>(members: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    members
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn parse_result_attributes(
    values: Map<String, Value>,
    limits: BeaParseLimits,
) -> Result<BTreeMap<String, String>, BeaError> {
    let mut output = BTreeMap::new();
    for (name, value) in values {
        validate_string(&name, limits)?;
        let value = scalar_string(value).ok_or(BeaError::InvalidField("result attribute"))?;
        validate_string(&value, limits)?;
        output.insert(name, value);
    }
    Ok(output)
}

fn page_receipt(
    bytes: &[u8],
    request: &BeaRequest,
    returned_rows: usize,
    limits: BeaParseLimits,
) -> Result<BeaPageReceipt, BeaError> {
    if returned_rows > limits.max_rows
        || request
            .page_scope()
            .expected_rows()
            .is_some_and(|expected| expected > limits.max_rows || returned_rows > expected)
    {
        return Err(BeaError::RowLimitExceeded);
    }
    let response_digest: [u8; 32] = Sha256::digest(bytes).into();
    let (missing_rows, completeness) = match request.page_scope().expected_rows() {
        Some(expected) => {
            let missing = expected
                .checked_sub(returned_rows)
                .ok_or(BeaError::InvalidField("returned row count"))?;
            (
                Some(missing),
                if missing == 0 {
                    BeaCompleteness::Complete
                } else {
                    BeaCompleteness::Partial
                },
            )
        }
        None => (None, BeaCompleteness::ExpectedCountUnknown),
    };
    Ok(BeaPageReceipt::new(
        request.request_digest(),
        response_digest,
        bytes.len(),
        request.page_scope().page_number(),
        request.page_scope().page_count(),
        request.page_scope().expected_rows(),
        returned_rows,
        missing_rows,
        completeness,
    ))
}

fn split_note_references(value: &str, limits: BeaParseLimits) -> Result<Vec<String>, BeaError> {
    validate_string(value, limits)?;
    let mut output = Vec::new();
    for reference in value.split(|character: char| character == ',' || character.is_whitespace()) {
        let reference = reference.trim();
        if reference.is_empty() {
            continue;
        }
        if output.len() >= limits.max_notes {
            return Err(BeaError::RowLimitExceeded);
        }
        if output.iter().any(|value| value == reference) {
            return Err(BeaError::InvalidField("duplicate row NoteRef"));
        }
        output.push(reference.to_owned());
    }
    Ok(output)
}

fn validate_note_references(
    references: &[String],
    index: &BTreeSet<String>,
) -> Result<(), BeaError> {
    if references.iter().all(|reference| index.contains(reference)) {
        Ok(())
    } else {
        Err(BeaError::InvalidField("unresolved NoteRef"))
    }
}

fn take_required_array(
    values: &mut Map<String, Value>,
    name: &'static str,
) -> Result<Vec<Value>, BeaError> {
    match remove_case_insensitive(values, name)? {
        Some(Value::Array(values)) => Ok(values),
        Some(_) | None => Err(BeaError::InvalidField(name)),
    }
}

fn take_optional_array(
    values: &mut Map<String, Value>,
    name: &'static str,
) -> Result<Option<Vec<Value>>, BeaError> {
    match remove_case_insensitive(values, name)? {
        Some(Value::Array(values)) => Ok(Some(values)),
        Some(_) => Err(BeaError::InvalidField(name)),
        None => Ok(None),
    }
}

fn take_required_string(
    values: &mut Map<String, Value>,
    name: &'static str,
    limits: BeaParseLimits,
) -> Result<String, BeaError> {
    let value = remove_case_insensitive(values, name)?
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(BeaError::InvalidField(name))?;
    validate_string(&value, limits)?;
    Ok(value)
}

fn take_optional_string(
    values: &mut Map<String, Value>,
    name: &'static str,
    limits: BeaParseLimits,
) -> Result<Option<String>, BeaError> {
    let Some(value) = remove_case_insensitive(values, name)? else {
        return Ok(None);
    };
    let value = value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or(BeaError::InvalidField(name))?;
    validate_string(&value, limits)?;
    Ok(Some(value))
}

fn take_optional_string_alias(
    values: &mut Map<String, Value>,
    name: &'static str,
    alias: &'static str,
    limits: BeaParseLimits,
) -> Result<Option<String>, BeaError> {
    let primary = take_optional_string(values, name, limits)?;
    let alternate = take_optional_string(values, alias, limits)?;
    match (primary, alternate) {
        (Some(_), Some(_)) => Err(BeaError::InvalidField(name)),
        (value @ Some(_), None) | (None, value @ Some(_)) => Ok(value),
        (None, None) => Ok(None),
    }
}

fn take_required_scalar_string(
    values: &mut Map<String, Value>,
    name: &'static str,
    limits: BeaParseLimits,
) -> Result<String, BeaError> {
    let value = remove_case_insensitive(values, name)?
        .and_then(scalar_string)
        .ok_or(BeaError::InvalidField(name))?;
    validate_string(&value, limits)?;
    Ok(value)
}

fn take_required_dimension_scalar(
    values: &mut Map<String, Value>,
    name: &str,
    limits: BeaParseLimits,
) -> Result<String, BeaError> {
    let value = remove_case_insensitive(values, name)?
        .and_then(scalar_string)
        .ok_or(BeaError::InvalidField("dimension member"))?;
    validate_string(&value, limits)?;
    Ok(value)
}

fn take_optional_scalar_string(
    values: &mut Map<String, Value>,
    name: &'static str,
    limits: BeaParseLimits,
) -> Result<Option<String>, BeaError> {
    let Some(value) = remove_case_insensitive(values, name)? else {
        return Ok(None);
    };
    let value = scalar_string(value).ok_or(BeaError::InvalidField(name))?;
    validate_string(&value, limits)?;
    Ok(Some(value))
}

fn scalar_string(value: Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some("null".to_owned()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn take_flag(
    values: &mut Map<String, Value>,
    name: &'static str,
    limits: BeaParseLimits,
) -> Result<bool, BeaError> {
    match take_required_scalar_string(values, name, limits)?.as_str() {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(BeaError::InvalidField(name)),
    }
}

fn take_flag_alias(
    values: &mut Map<String, Value>,
    name: &'static str,
    alias: &'static str,
    limits: BeaParseLimits,
) -> Result<bool, BeaError> {
    let primary = take_optional_scalar_string(values, name, limits)?;
    let alternate = take_optional_scalar_string(values, alias, limits)?;
    let value = match (primary, alternate) {
        (Some(_), Some(_)) | (None, None) => return Err(BeaError::InvalidField(name)),
        (Some(value), None) | (None, Some(value)) => value,
    };
    match value.as_str() {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(BeaError::InvalidField(name)),
    }
}

fn remove_case_insensitive(
    values: &mut Map<String, Value>,
    name: &str,
) -> Result<Option<Value>, BeaError> {
    let matches = values
        .keys()
        .filter(|candidate| candidate.eq_ignore_ascii_case(name))
        .cloned()
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(BeaError::InvalidField("duplicate case-insensitive field"));
    }
    Ok(matches
        .first()
        .and_then(|matched| values.remove(matched.as_str())))
}

fn admit_metadata_count(count: usize, limits: BeaParseLimits) -> Result<(), BeaError> {
    if count > limits.max_metadata_records {
        Err(BeaError::RowLimitExceeded)
    } else {
        Ok(())
    }
}

fn validate_string(value: &str, limits: BeaParseLimits) -> Result<(), BeaError> {
    if value.len() > limits.max_string_bytes {
        Err(BeaError::StringLimitExceeded)
    } else if value.chars().any(|character| character == '\0') {
        Err(BeaError::InvalidField("string"))
    } else {
        Ok(())
    }
}

fn ensure_empty(values: &Map<String, Value>, field: &'static str) -> Result<(), BeaError> {
    if values.is_empty() {
        Ok(())
    } else {
        Err(BeaError::InvalidField(field))
    }
}
