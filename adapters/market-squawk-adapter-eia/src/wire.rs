//! Shared bounded JSON envelope parsing and API-key echo redaction.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::types::{digest_bytes, digest_parts};
use crate::{EiaApiVersion, EiaDigest, EiaError, EiaParseLimits};

pub(crate) struct EiaEnvelope {
    pub(crate) response: Map<String, Value>,
    pub(crate) api_version: EiaApiVersion,
    pub(crate) transport_payload_digest: EiaDigest,
    pub(crate) retained_payload_digest: EiaDigest,
    pub(crate) request_echo_digest: EiaDigest,
    pub(crate) envelope_schema_digest: EiaDigest,
    pub(crate) retained_payload: Vec<u8>,
    pub(crate) redacted_secret_fields: usize,
}

pub(crate) fn parse_envelope(
    bytes: &[u8],
    expected_command: &str,
    expected_params: &Map<String, Value>,
    limits: EiaParseLimits,
) -> Result<EiaEnvelope, EiaError> {
    if bytes.len() > limits.max_body_bytes() {
        return Err(EiaError::BodyTooLarge);
    }
    let transport_payload_digest = digest_bytes(bytes);
    let mut root: Value = serde_json::from_slice(bytes).map_err(|_| EiaError::InvalidJson)?;
    validate_structure(&root, limits)?;

    let object = root.as_object_mut().ok_or(EiaError::InvalidProtocol)?;
    if object.contains_key("error") {
        return Err(EiaError::InvalidProtocol);
    }

    let api_version = object
        .get("apiVersion")
        .and_then(Value::as_str)
        .ok_or(EiaError::InvalidProtocol)
        .and_then(EiaApiVersion::try_new)?;
    {
        let request = object
            .get("request")
            .and_then(Value::as_object)
            .ok_or(EiaError::InvalidProtocol)?;
        let command = request
            .get("command")
            .and_then(Value::as_str)
            .ok_or(EiaError::InvalidProtocol)?;
        if command.contains('?')
            || command.to_ascii_lowercase().contains("api_key")
            || normalize_command(command) != normalize_command(expected_command)
        {
            return Err(EiaError::RequestEchoMismatch);
        }
        if !request.contains_key("params") {
            return Err(EiaError::InvalidProtocol);
        }
    }
    let mut redacted_secret_fields = 0;
    redact_api_keys(
        &mut root,
        0,
        limits.max_json_depth(),
        &mut redacted_secret_fields,
    )?;
    if redacted_secret_fields != 1 {
        return Err(EiaError::RequestEchoMismatch);
    }
    let request = root
        .as_object()
        .and_then(|object| object.get("request"))
        .and_then(Value::as_object)
        .ok_or(EiaError::InvalidProtocol)?;
    if request.len() != 2
        || request
            .get("params")
            .and_then(Value::as_object)
            .is_none_or(|params| params != expected_params)
    {
        return Err(EiaError::RequestEchoMismatch);
    }
    let request_echo_bytes = serde_json::to_vec(request).map_err(|_| EiaError::InvalidJson)?;
    let request_echo_digest = digest_bytes(&request_echo_bytes);
    let envelope_schema_digest = schema_shape_digest(&root)?;
    let retained_payload = serde_json::to_vec(&root).map_err(|_| EiaError::InvalidJson)?;
    let retained_payload_digest = digest_bytes(&retained_payload);
    let response = root
        .as_object_mut()
        .and_then(|object| object.remove("response"))
        .and_then(|response| response.as_object().cloned())
        .ok_or(EiaError::InvalidProtocol)?;

    Ok(EiaEnvelope {
        response,
        api_version,
        transport_payload_digest,
        retained_payload_digest,
        request_echo_digest,
        envelope_schema_digest,
        retained_payload,
        redacted_secret_fields,
    })
}

pub(crate) fn parse_bounded_string(
    value: &Value,
    limits: EiaParseLimits,
) -> Result<String, EiaError> {
    let value = value.as_str().ok_or(EiaError::InvalidProtocol)?;
    if value.len() > limits.max_string_bytes() || value.chars().any(char::is_control) {
        return Err(EiaError::StructureLimit);
    }
    Ok(value.to_owned())
}

pub(crate) fn parse_count(value: &Value) -> Result<u64, EiaError> {
    match value {
        Value::String(value) => value.parse::<u64>().map_err(|_| EiaError::InvalidProtocol),
        Value::Number(value) => value.as_u64().ok_or(EiaError::InvalidProtocol),
        _ => Err(EiaError::InvalidProtocol),
    }
}

pub(crate) fn object_schema_digest(object: &Map<String, Value>) -> Result<EiaDigest, EiaError> {
    schema_shape_digest(&Value::Object(object.clone()))
}

fn validate_structure(root: &Value, limits: EiaParseLimits) -> Result<(), EiaError> {
    let mut stack = vec![(root, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.checked_add(1).ok_or(EiaError::StructureLimit)?;
        if nodes > limits.max_json_nodes() || depth > limits.max_json_depth() {
            return Err(EiaError::StructureLimit);
        }
        match value {
            Value::Object(object) => {
                if object.len() > limits.max_fields_per_object() {
                    return Err(EiaError::StructureLimit);
                }
                for (key, value) in object {
                    if key.len() > limits.max_string_bytes() || key.chars().any(char::is_control) {
                        return Err(EiaError::StructureLimit);
                    }
                    stack.push((value, depth.saturating_add(1)));
                }
            }
            Value::Array(values) => {
                for value in values {
                    stack.push((value, depth.saturating_add(1)));
                }
            }
            Value::String(value) if value.len() > limits.max_string_bytes() => {
                return Err(EiaError::StructureLimit);
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

fn redact_api_keys(
    value: &mut Value,
    depth: usize,
    max_depth: usize,
    redacted: &mut usize,
) -> Result<(), EiaError> {
    if depth > max_depth {
        return Err(EiaError::StructureLimit);
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if key.eq_ignore_ascii_case("api_key") {
                    *value = Value::String("[REDACTED]".to_owned());
                    *redacted = redacted.checked_add(1).ok_or(EiaError::StructureLimit)?;
                } else {
                    redact_api_keys(value, depth.saturating_add(1), max_depth, redacted)?;
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_api_keys(value, depth.saturating_add(1), max_depth, redacted)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn normalize_command(value: &str) -> &str {
    value.trim_end_matches('/')
}

fn schema_shape_digest(value: &Value) -> Result<EiaDigest, EiaError> {
    let mut tokens = Vec::new();
    append_shape(value, 0, &mut tokens)?;
    let parts = tokens.iter().map(Vec::as_slice);
    Ok(digest_parts(b"eia-json-shape-v1", parts))
}

fn append_shape(value: &Value, depth: usize, tokens: &mut Vec<Vec<u8>>) -> Result<(), EiaError> {
    if depth > 64 {
        return Err(EiaError::StructureLimit);
    }
    match value {
        Value::Null => tokens.push(b"null".to_vec()),
        Value::Bool(_) => tokens.push(b"bool".to_vec()),
        Value::Number(_) => tokens.push(b"number".to_vec()),
        Value::String(_) => tokens.push(b"string".to_vec()),
        Value::Array(values) => {
            tokens.push(b"array".to_vec());
            let mut element_shapes = BTreeSet::new();
            for value in values {
                let mut element = Vec::new();
                append_shape(value, depth.saturating_add(1), &mut element)?;
                let digest = digest_parts(
                    b"eia-array-element-shape-v1",
                    element.iter().map(Vec::as_slice),
                );
                element_shapes.insert(digest.bytes());
            }
            for shape in element_shapes {
                tokens.push(shape.to_vec());
            }
        }
        Value::Object(object) => {
            tokens.push(b"object".to_vec());
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort_unstable();
            for key in keys {
                tokens.push(key.as_bytes().to_vec());
                append_shape(
                    object.get(key).ok_or(EiaError::InvalidProtocol)?,
                    depth.saturating_add(1),
                    tokens,
                )?;
            }
        }
    }
    Ok(())
}
