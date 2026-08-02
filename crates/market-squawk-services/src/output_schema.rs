//! Descriptor-owned structured-result schemas and their bounded validator.

use serde_json::{Map, Value, json};

use crate::ToolArtifactPolicy;

const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

pub(crate) fn validate_data_schema(schema: &Value) -> bool {
    schema_definition_is_supported(schema, true)
}

pub(crate) fn validate_data(schema: &Value, value: &Value) -> bool {
    validate_instance(schema, value)
}

pub(crate) fn output_schema(
    data_schema: &Value,
    artifact_policy: ToolArtifactPolicy,
) -> Map<String, Value> {
    let mut variants = vec![inline_envelope_schema(data_schema.clone())];
    if matches!(artifact_policy, ToolArtifactPolicy::OpaqueOnOverflow) {
        variants.push(artifact_envelope_schema());
    }
    Map::from_iter([
        (
            "$schema".to_owned(),
            Value::String(JSON_SCHEMA_DIALECT.to_owned()),
        ),
        ("type".to_owned(), Value::String("object".to_owned())),
        ("oneOf".to_owned(), Value::Array(variants)),
    ])
}

fn inline_envelope_schema(data_schema: Value) -> Value {
    closed_object(
        [("data", data_schema), ("metadata", metadata_schema())],
        &["data", "metadata"],
    )
}

fn artifact_envelope_schema() -> Value {
    closed_object(
        [
            (
                "artifact",
                closed_object(
                    [
                        ("id", json!({"type": "string", "minLength": 1})),
                        (
                            "sha256",
                            json!({"type": "string", "minLength": 64, "maxLength": 64}),
                        ),
                        ("byteCount", json!({"type": "integer", "minimum": 1})),
                        ("mediaType", json!({"type": "string", "minLength": 1})),
                    ],
                    &["id", "sha256", "byteCount", "mediaType"],
                ),
            ),
            ("metadata", metadata_schema()),
        ],
        &["artifact", "metadata"],
    )
}

fn metadata_schema() -> Value {
    closed_object(
        [
            (
                "completeness",
                json!({"type": "string", "enum": ["complete", "truncated"]}),
            ),
            ("returnedItems", json!({"type": "integer", "minimum": 0})),
            ("availableItems", json!({"type": "integer", "minimum": 0})),
            ("sourceCoverage", evidence_schema()),
            ("dataQuality", evidence_schema()),
        ],
        &[
            "completeness",
            "returnedItems",
            "availableItems",
            "sourceCoverage",
            "dataQuality",
        ],
    )
}

fn evidence_schema() -> Value {
    json!({
        "oneOf": [
            {"type": "object", "minProperties": 1},
            {"type": "array", "minItems": 1, "items": scalar_or_container_schema()},
            {"type": "string", "minLength": 1}
        ]
    })
}

fn scalar_or_container_schema() -> Value {
    json!({
        "oneOf": [
            {"type": "null"},
            {"type": "boolean"},
            {"type": "number"},
            {"type": "string"},
            {"type": "array", "items": {}},
            {"type": "object"}
        ]
    })
}

fn closed_object<const N: usize>(properties: [(&str, Value); N], required: &[&str]) -> Value {
    let properties = properties
        .into_iter()
        .map(|(name, schema)| (name.to_owned(), schema))
        .collect::<Map<_, _>>();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

fn schema_definition_is_supported(schema: &Value, root: bool) -> bool {
    let Some(schema) = schema.as_object() else {
        return false;
    };
    if let Some(variants) = schema.get("oneOf") {
        if schema
            .keys()
            .any(|key| !matches!(key.as_str(), "oneOf" | "description"))
        {
            return false;
        }
        let Some(variants) = variants.as_array() else {
            return false;
        };
        return variants.len() >= 2
            && variants
                .iter()
                .all(|variant| schema_definition_is_supported(variant, root));
    }

    let Some(schema_type) = schema.get("type").and_then(Value::as_str) else {
        return false;
    };
    if schema.keys().any(|key| {
        !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "items"
                | "const"
                | "enum"
                | "minimum"
                | "maximum"
                | "minLength"
                | "maxLength"
                | "minItems"
                | "maxItems"
                | "minProperties"
                | "maxProperties"
                | "format"
                | "description"
        )
    }) {
        return false;
    }

    let type_is_supported = match schema_type {
        "object" => object_schema_is_supported(schema, root),
        "array" => schema
            .get("items")
            .is_some_and(|items| schema_definition_is_supported(items, false)),
        "string" | "integer" | "number" | "boolean" | "null" => true,
        _ => false,
    };
    type_is_supported
        && string_format_is_supported(schema, schema_type)
        && numeric_keyword_is_number(schema, "minimum")
        && numeric_keyword_is_number(schema, "maximum")
        && unsigned_keyword_is_integer(schema, "minLength")
        && unsigned_keyword_is_integer(schema, "maxLength")
        && unsigned_keyword_is_integer(schema, "minItems")
        && unsigned_keyword_is_integer(schema, "maxItems")
        && unsigned_keyword_is_integer(schema, "minProperties")
        && unsigned_keyword_is_integer(schema, "maxProperties")
        && schema
            .get("enum")
            .is_none_or(|values| values.as_array().is_some_and(|values| !values.is_empty()))
}

fn string_format_is_supported(schema: &Map<String, Value>, schema_type: &str) -> bool {
    match schema.get("format") {
        None => true,
        Some(Value::String(format)) if schema_type == "string" => {
            matches!(format.as_str(), "uuid" | "date-time")
        }
        Some(_) => false,
    }
}

fn object_schema_is_supported(schema: &Map<String, Value>, root: bool) -> bool {
    let properties = schema.get("properties").and_then(Value::as_object);
    let additional_properties = schema.get("additionalProperties").and_then(Value::as_bool);
    if schema.contains_key("properties") && properties.is_none() {
        return false;
    }
    if properties.is_some_and(|properties| {
        properties
            .values()
            .any(|property| !schema_definition_is_supported(property, false))
    }) {
        return false;
    }
    let required = match schema.get("required") {
        Some(Value::Array(required)) => required.as_slice(),
        Some(_) => return false,
        None => &[],
    };
    let mut required_names = Vec::with_capacity(required.len());
    for name in required {
        let Some(name) = name.as_str() else {
            return false;
        };
        if required_names.contains(&name)
            || !properties.is_some_and(|properties| properties.contains_key(name))
        {
            return false;
        }
        required_names.push(name);
    }
    if root && (properties.is_none_or(Map::is_empty) || required_names.is_empty()) {
        return false;
    }
    !schema.contains_key("additionalProperties") || additional_properties.is_some()
}

fn numeric_keyword_is_number(schema: &Map<String, Value>, keyword: &str) -> bool {
    schema.get(keyword).is_none_or(Value::is_number)
}

fn unsigned_keyword_is_integer(schema: &Map<String, Value>, keyword: &str) -> bool {
    schema
        .get(keyword)
        .is_none_or(|value| value.as_u64().is_some())
}

fn validate_instance(schema: &Value, value: &Value) -> bool {
    if let Some(accepts_every_instance) = schema.as_bool() {
        return accepts_every_instance;
    }
    let Some(schema) = schema.as_object() else {
        return false;
    };
    if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
        return variants
            .iter()
            .filter(|variant| validate_instance(variant, value))
            .count()
            == 1;
    }
    if schema
        .get("const")
        .is_some_and(|expected| expected != value)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.contains(value))
    {
        return false;
    }
    match schema.get("type").and_then(Value::as_str) {
        Some("null") => value.is_null(),
        Some("boolean") => value.is_boolean(),
        Some("string") => value.as_str().is_some_and(|value| {
            bounded_length(
                value.chars().count() as u64,
                schema.get("minLength"),
                schema.get("maxLength"),
            ) && string_format_matches(schema.get("format"), value)
        }),
        Some("integer") => {
            (value.as_i64().is_some() || value.as_u64().is_some())
                && bounded_number(value, schema.get("minimum"), schema.get("maximum"))
        }
        Some("number") => {
            value.is_number() && bounded_number(value, schema.get("minimum"), schema.get("maximum"))
        }
        Some("array") => value.as_array().is_some_and(|values| {
            bounded_length(
                values.len() as u64,
                schema.get("minItems"),
                schema.get("maxItems"),
            ) && schema
                .get("items")
                .is_some_and(|items| values.iter().all(|value| validate_instance(items, value)))
        }),
        Some("object") => value
            .as_object()
            .is_some_and(|value| validate_object_instance(schema, value)),
        _ => false,
    }
}

fn string_format_matches(format: Option<&Value>, value: &str) -> bool {
    match format.and_then(Value::as_str) {
        None => true,
        Some("uuid") => uuid::Uuid::parse_str(value)
            .is_ok_and(|parsed| parsed.hyphenated().to_string() == value),
        Some("date-time") => chrono::DateTime::parse_from_rfc3339(value).is_ok(),
        Some(_) => false,
    }
}

fn validate_object_instance(schema: &Map<String, Value>, value: &Map<String, Value>) -> bool {
    if !bounded_length(
        value.len() as u64,
        schema.get("minProperties"),
        schema.get("maxProperties"),
    ) {
        return false;
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    if schema
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| {
            required
                .iter()
                .any(|name| name.as_str().is_none_or(|name| !value.contains_key(name)))
        })
    {
        return false;
    }
    for (name, item) in value {
        match properties.and_then(|properties| properties.get(name)) {
            Some(item_schema) if !validate_instance(item_schema, item) => return false,
            Some(_) => {}
            None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                return false;
            }
            None => {}
        }
    }
    true
}

fn bounded_length(length: u64, minimum: Option<&Value>, maximum: Option<&Value>) -> bool {
    minimum
        .and_then(Value::as_u64)
        .is_none_or(|min| length >= min)
        && maximum
            .and_then(Value::as_u64)
            .is_none_or(|max| length <= max)
}

fn bounded_number(value: &Value, minimum: Option<&Value>, maximum: Option<&Value>) -> bool {
    let Some(number) = value.as_f64() else {
        return false;
    };
    minimum
        .and_then(Value::as_f64)
        .is_none_or(|minimum| number >= minimum)
        && maximum
            .and_then(Value::as_f64)
            .is_none_or(|maximum| number <= maximum)
}

#[cfg(test)]
mod tests {
    use super::{validate_data, validate_data_schema};
    use serde_json::json;

    #[test]
    fn operation_schema_must_be_specific_and_runtime_validation_is_closed() {
        assert!(!validate_data_schema(&json!({"type": "object"})));
        assert!(validate_data(
            &json!(true),
            &json!({"legacyCompatibility": ["any bounded JSON data"]})
        ));
        let schema = json!({
            "type": "object",
            "properties": {"expected": {"type": "boolean"}},
            "required": ["expected"],
            "additionalProperties": false
        });
        assert!(validate_data_schema(&schema));
        assert!(validate_data(&schema, &json!({"expected": true})));
        assert!(!validate_data(&schema, &json!({"unexpected": true})));

        let uuid_schema = json!({"type": "string", "format": "uuid"});
        assert!(validate_data_schema(&uuid_schema));
        assert!(validate_data(
            &uuid_schema,
            &json!("c127919d-6540-47f8-9f6b-902523578cb5")
        ));
        assert!(!validate_data(&uuid_schema, &json!("not-a-uuid")));

        let timestamp_schema = json!({"type": "string", "format": "date-time"});
        assert!(validate_data_schema(&timestamp_schema));
        assert!(validate_data(
            &timestamp_schema,
            &json!("2026-07-26T12:34:56.123456789Z")
        ));
        assert!(!validate_data(&timestamp_schema, &json!("2026-07-26")));
    }
}
