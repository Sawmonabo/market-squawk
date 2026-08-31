//! Route, frequency, data-column, and facet-value metadata discovery.

use std::collections::BTreeSet;
use std::sync::Arc;

use market_squawk_domain::Timestamp;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::types::digest_parts;
use crate::wire::{object_schema_digest, parse_bounded_string, parse_count, parse_envelope};
use crate::{
    EiaApiVersion, EiaDigest, EiaError, EiaFacetValue, EiaFieldId, EiaMetadataRequest,
    EiaMetadataRequestKind, EiaParseLimits, EiaRoute,
};

/// One child route discovered at a metadata node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaChildRoute {
    id: EiaFieldId,
    name: Option<String>,
    description: Option<String>,
}

impl EiaChildRoute {
    /// Returns the child route segment.
    pub const fn id(&self) -> &EiaFieldId {
        &self.id
    }

    /// Returns the provider display name when supplied.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the provider description when supplied.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

/// One provider-supported frequency and exact period format.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaFrequencyMetadata {
    id: EiaFieldId,
    description: Option<String>,
    query: Option<String>,
    format: String,
}

impl EiaFrequencyMetadata {
    /// Returns the provider frequency identity.
    pub const fn id(&self) -> &EiaFieldId {
        &self.id
    }

    /// Returns the optional provider description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the optional provider query code.
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Returns the provider-declared period format.
    pub fn format(&self) -> &str {
        &self.format
    }
}

/// One route facet identity discovered before querying its available values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaFacetMetadata {
    id: EiaFieldId,
    description: Option<String>,
}

impl EiaFacetMetadata {
    /// Returns the facet identity.
    pub const fn id(&self) -> &EiaFieldId {
        &self.id
    }

    /// Returns the optional provider description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

/// One provider data-column definition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaDataColumnMetadata {
    id: EiaFieldId,
    alias: Option<String>,
    units: Option<String>,
}

impl EiaDataColumnMetadata {
    /// Returns the data-column identity.
    pub const fn id(&self) -> &EiaFieldId {
        &self.id
    }

    /// Returns the optional provider alias.
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    /// Returns exact provider unit text when metadata supplies it.
    pub fn units(&self) -> Option<&str> {
        self.units.as_deref()
    }
}

/// Secret-free receipt for one route metadata response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaMetadataReceipt {
    request_digest: EiaDigest,
    transport_payload_digest: EiaDigest,
    retained_payload_digest: EiaDigest,
    request_echo_digest: EiaDigest,
    envelope_schema_digest: EiaDigest,
    received_at: Timestamp,
    response_bytes: usize,
    redacted_secret_fields: usize,
}

impl EiaMetadataReceipt {
    /// Returns the secret-free request identity.
    pub const fn request_digest(&self) -> EiaDigest {
        self.request_digest
    }

    /// Returns the digest of exact transport bytes before ephemeral secret redaction.
    pub const fn transport_payload_digest(&self) -> EiaDigest {
        self.transport_payload_digest
    }

    /// Returns the digest of the secret-free retained bytes.
    pub const fn retained_payload_digest(&self) -> EiaDigest {
        self.retained_payload_digest
    }

    /// Returns the digest of the interpreted request after API-key redaction.
    pub const fn request_echo_digest(&self) -> EiaDigest {
        self.request_echo_digest
    }

    /// Returns the provider envelope shape identity.
    pub const fn envelope_schema_digest(&self) -> EiaDigest {
        self.envelope_schema_digest
    }

    /// Returns the local response receipt time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact transport response bytes.
    pub const fn response_bytes(&self) -> usize {
        self.response_bytes
    }

    /// Returns how many echoed secret fields were replaced before retention.
    pub const fn redacted_secret_fields(&self) -> usize {
        self.redacted_secret_fields
    }
}

/// One route metadata generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaRouteMetadata {
    route: EiaRoute,
    api_version: EiaApiVersion,
    id: Option<EiaFieldId>,
    name: Option<String>,
    description: Option<String>,
    child_routes: Vec<EiaChildRoute>,
    frequencies: Vec<EiaFrequencyMetadata>,
    facets: Vec<EiaFacetMetadata>,
    data_columns: Vec<EiaDataColumnMetadata>,
    start_period: Option<String>,
    end_period: Option<String>,
    default_date_format: Option<String>,
    default_frequency: Option<String>,
    unmapped_response_fields: Vec<String>,
    schema_digest: EiaDigest,
    receipt: EiaMetadataReceipt,
    retained_payload: Arc<[u8]>,
}

impl EiaRouteMetadata {
    /// Returns the exact requested route.
    pub const fn route(&self) -> &EiaRoute {
        &self.route
    }

    /// Returns the serving API version.
    pub const fn api_version(&self) -> &EiaApiVersion {
        &self.api_version
    }

    /// Returns the route's provider identity when supplied.
    pub const fn id(&self) -> Option<&EiaFieldId> {
        self.id.as_ref()
    }

    /// Returns the provider route name when supplied.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the provider route description when supplied.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns sorted child routes.
    pub fn child_routes(&self) -> &[EiaChildRoute] {
        &self.child_routes
    }

    /// Returns sorted supported frequencies.
    pub fn frequencies(&self) -> &[EiaFrequencyMetadata] {
        &self.frequencies
    }

    /// Returns sorted route facets.
    pub fn facets(&self) -> &[EiaFacetMetadata] {
        &self.facets
    }

    /// Returns sorted data columns.
    pub fn data_columns(&self) -> &[EiaDataColumnMetadata] {
        &self.data_columns
    }

    /// Looks up one exact frequency.
    pub fn frequency(&self, id: &EiaFieldId) -> Option<&EiaFrequencyMetadata> {
        self.frequencies
            .binary_search_by(|candidate| candidate.id.cmp(id))
            .ok()
            .map(|index| &self.frequencies[index])
    }

    /// Looks up one exact route facet.
    pub fn facet(&self, id: &EiaFieldId) -> Option<&EiaFacetMetadata> {
        self.facets
            .binary_search_by(|candidate| candidate.id.cmp(id))
            .ok()
            .map(|index| &self.facets[index])
    }

    /// Looks up one exact data column.
    pub fn data_column(&self, id: &EiaFieldId) -> Option<&EiaDataColumnMetadata> {
        self.data_columns
            .binary_search_by(|candidate| candidate.id.cmp(id))
            .ok()
            .map(|index| &self.data_columns[index])
    }

    /// Returns the lower provider period bound when supplied.
    pub fn start_period(&self) -> Option<&str> {
        self.start_period.as_deref()
    }

    /// Returns the upper provider period bound when supplied.
    pub fn end_period(&self) -> Option<&str> {
        self.end_period.as_deref()
    }

    /// Returns the provider default date format when supplied.
    pub fn default_date_format(&self) -> Option<&str> {
        self.default_date_format.as_deref()
    }

    /// Returns the provider default frequency when supplied.
    pub fn default_frequency(&self) -> Option<&str> {
        self.default_frequency.as_deref()
    }

    /// Returns additional provider response field names retained as drift evidence, never as an
    /// open canonical property map.
    pub fn unmapped_response_fields(&self) -> &[String] {
        &self.unmapped_response_fields
    }

    /// Returns the route contract's structural/semantic digest.
    pub const fn schema_digest(&self) -> EiaDigest {
        self.schema_digest
    }

    /// Returns secret-free response evidence.
    pub const fn receipt(&self) -> &EiaMetadataReceipt {
        &self.receipt
    }

    /// Returns the secret-free response bytes suitable for bounded raw capture.
    pub fn retained_payload(&self) -> &[u8] {
        &self.retained_payload
    }

    pub(crate) fn retained_payload_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.retained_payload)
    }
}

/// One exact value returned by the facet metadata endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaFacetMetadataValue {
    id: EiaFacetValue,
    name: Option<String>,
    alias: Option<String>,
}

impl EiaFacetMetadataValue {
    /// Returns the exact facet value identity.
    pub const fn id(&self) -> &EiaFacetValue {
        &self.id
    }

    /// Returns the provider name when supplied.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the provider alias when supplied.
    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
}

/// Secret-free receipt for one complete facet catalog.
pub type EiaFacetMetadataReceipt = EiaMetadataReceipt;

/// One complete facet-value catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaFacetCatalog {
    route: EiaRoute,
    facet: EiaFieldId,
    api_version: EiaApiVersion,
    total_facets: u64,
    values: Vec<EiaFacetMetadataValue>,
    schema_digest: EiaDigest,
    receipt: EiaFacetMetadataReceipt,
    retained_payload: Arc<[u8]>,
}

impl EiaFacetCatalog {
    /// Returns the exact requested route.
    pub const fn route(&self) -> &EiaRoute {
        &self.route
    }

    /// Returns the exact facet identity.
    pub const fn facet(&self) -> &EiaFieldId {
        &self.facet
    }

    /// Returns the serving API version.
    pub const fn api_version(&self) -> &EiaApiVersion {
        &self.api_version
    }

    /// Returns provider-declared available facet values.
    pub const fn total_facets(&self) -> u64 {
        self.total_facets
    }

    /// Returns sorted values.
    pub fn values(&self) -> &[EiaFacetMetadataValue] {
        &self.values
    }

    /// Returns whether one exact value was discovered.
    pub fn contains(&self, value: &EiaFacetValue) -> bool {
        self.values
            .binary_search_by(|candidate| candidate.id.cmp(value))
            .is_ok()
    }

    /// Returns the facet-catalog schema/semantic digest.
    pub const fn schema_digest(&self) -> EiaDigest {
        self.schema_digest
    }

    /// Returns the secret-free receipt.
    pub const fn receipt(&self) -> &EiaFacetMetadataReceipt {
        &self.receipt
    }

    /// Returns the secret-free response bytes suitable for bounded raw capture.
    pub fn retained_payload(&self) -> &[u8] {
        &self.retained_payload
    }

    pub(crate) fn retained_payload_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.retained_payload)
    }
}

/// Relationship between two route metadata generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaMetadataChange {
    /// Both retained content and schema identity are unchanged.
    Unchanged,
    /// Non-schema metadata changed while the frozen route contract remained the same.
    Revision {
        /// Previous retained payload digest.
        previous: EiaDigest,
        /// Current retained payload digest.
        current: EiaDigest,
    },
    /// Frequencies, facets, fields, units, or native response shape changed.
    SchemaDrift {
        /// Previous route schema digest.
        previous: EiaDigest,
        /// Current route schema digest.
        current: EiaDigest,
    },
}

/// Classifies a newly discovered route metadata generation without overwriting either document.
pub fn compare_route_metadata(
    previous: &EiaRouteMetadata,
    current: &EiaRouteMetadata,
) -> Result<EiaMetadataChange, EiaError> {
    if previous.route != current.route {
        return Err(EiaError::MetadataConflict);
    }
    if previous.api_version != current.api_version
        || previous.schema_digest != current.schema_digest
    {
        return Ok(EiaMetadataChange::SchemaDrift {
            previous: previous.schema_digest,
            current: current.schema_digest,
        });
    }
    let previous_content = previous.receipt.retained_payload_digest;
    let current_content = current.receipt.retained_payload_digest;
    Ok(if previous_content == current_content {
        EiaMetadataChange::Unchanged
    } else {
        EiaMetadataChange::Revision {
            previous: previous_content,
            current: current_content,
        }
    })
}

/// Parses one exact route metadata response and redacts any echoed API key before retention.
pub fn parse_route_metadata(
    bytes: &[u8],
    request: &EiaMetadataRequest,
    received_at: Timestamp,
    limits: EiaParseLimits,
) -> Result<EiaRouteMetadata, EiaError> {
    if !matches!(request.kind(), EiaMetadataRequestKind::Route) {
        return Err(EiaError::RequestEchoMismatch);
    }
    let secret_free = request.secret_free()?;
    let envelope = parse_envelope(
        bytes,
        &request.expected_command()?,
        &request.expected_echo_params(),
        limits,
    )?;
    let response_schema = object_schema_digest(&envelope.response)?;
    let mut response = envelope.response;

    let id = take_optional_identifier(&mut response, "id", limits)?;
    let name = take_optional_string(&mut response, "name", limits)?;
    let description = take_optional_string(&mut response, "description", limits)?;
    let child_routes = parse_children(response.remove("routes"), limits)?;
    let frequencies = parse_frequencies(response.remove("frequency"), limits)?;
    let facets = parse_facets(response.remove("facets"), limits)?;
    let data_columns = parse_data_columns(response.remove("data"), limits)?;
    let start_period = take_optional_string(&mut response, "startPeriod", limits)?;
    let end_period = take_optional_string(&mut response, "endPeriod", limits)?;
    let default_date_format = take_optional_string(&mut response, "defaultDateFormat", limits)?;
    let default_frequency = take_optional_string(&mut response, "defaultFrequency", limits)?;
    let _sources = response
        .remove("sources")
        .or_else(|| response.remove("Sources"))
        .map(|value| parse_bounded_string(&value, limits))
        .transpose()?;
    let mut unmapped_response_fields: Vec<_> =
        response.into_iter().map(|(key, _value)| key).collect();
    unmapped_response_fields.sort();

    let schema_bytes = serde_json::to_vec(&(
        request.route_value(),
        &envelope.api_version,
        &child_routes,
        &frequencies,
        &facets,
        &data_columns,
        &unmapped_response_fields,
        response_schema,
    ))
    .map_err(|_| EiaError::InvalidJson)?;
    let schema_digest = digest_parts(b"eia-route-metadata-schema-v1", [schema_bytes.as_slice()]);
    let receipt = EiaMetadataReceipt {
        request_digest: secret_free.request_digest(),
        transport_payload_digest: envelope.transport_payload_digest,
        retained_payload_digest: envelope.retained_payload_digest,
        request_echo_digest: envelope.request_echo_digest,
        envelope_schema_digest: envelope.envelope_schema_digest,
        received_at,
        response_bytes: bytes.len(),
        redacted_secret_fields: envelope.redacted_secret_fields,
    };
    Ok(EiaRouteMetadata {
        route: request.route_value().clone(),
        api_version: envelope.api_version,
        id,
        name,
        description,
        child_routes,
        frequencies,
        facets,
        data_columns,
        start_period,
        end_period,
        default_date_format,
        default_frequency,
        unmapped_response_fields,
        schema_digest,
        receipt,
        retained_payload: Arc::from(envelope.retained_payload),
    })
}

/// Parses one exact facet-value response and proves the provider's `totalFacets` count.
pub fn parse_facet_metadata(
    bytes: &[u8],
    request: &EiaMetadataRequest,
    received_at: Timestamp,
    limits: EiaParseLimits,
) -> Result<EiaFacetCatalog, EiaError> {
    let EiaMetadataRequestKind::Facet(facet) = request.kind() else {
        return Err(EiaError::RequestEchoMismatch);
    };
    let secret_free = request.secret_free()?;
    let envelope = parse_envelope(
        bytes,
        &request.expected_command()?,
        &request.expected_echo_params(),
        limits,
    )?;
    let response_schema = object_schema_digest(&envelope.response)?;
    let mut response = envelope.response;
    if response
        .keys()
        .any(|key| key != "totalFacets" && key != "facets")
    {
        return Err(EiaError::SchemaDrift);
    }
    let total_facets = response
        .remove("totalFacets")
        .as_ref()
        .ok_or(EiaError::InvalidProtocol)
        .and_then(parse_count)?;
    let mut values = parse_facet_values(response.remove("facets"), limits)?;
    if total_facets != u64::try_from(values.len()).map_err(|_| EiaError::InvalidLimit)? {
        return Err(EiaError::Pagination);
    }
    values.sort_by(|left, right| left.id.cmp(&right.id));
    ensure_unique_by(values.iter().map(|value| &value.id))?;
    let schema_bytes = serde_json::to_vec(&(
        request.route_value(),
        facet,
        &envelope.api_version,
        &values,
        response_schema,
    ))
    .map_err(|_| EiaError::InvalidJson)?;
    let schema_digest = digest_parts(b"eia-facet-metadata-schema-v1", [schema_bytes.as_slice()]);
    let receipt = EiaMetadataReceipt {
        request_digest: secret_free.request_digest(),
        transport_payload_digest: envelope.transport_payload_digest,
        retained_payload_digest: envelope.retained_payload_digest,
        request_echo_digest: envelope.request_echo_digest,
        envelope_schema_digest: envelope.envelope_schema_digest,
        received_at,
        response_bytes: bytes.len(),
        redacted_secret_fields: envelope.redacted_secret_fields,
    };
    Ok(EiaFacetCatalog {
        route: request.route_value().clone(),
        facet: facet.clone(),
        api_version: envelope.api_version,
        total_facets,
        values,
        schema_digest,
        receipt,
        retained_payload: Arc::from(envelope.retained_payload),
    })
}

fn parse_children(
    value: Option<Value>,
    limits: EiaParseLimits,
) -> Result<Vec<EiaChildRoute>, EiaError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or(EiaError::InvalidProtocol)?;
    admit_items(values.len(), limits)?;
    let mut children = Vec::with_capacity(values.len());
    for value in values {
        let mut object = exact_object(value, &["id", "name", "description"])?;
        children.push(EiaChildRoute {
            id: take_required_identifier(&mut object, "id", limits)?,
            name: take_optional_string(&mut object, "name", limits)?,
            description: take_optional_string(&mut object, "description", limits)?,
        });
    }
    children.sort_by(|left, right| left.id.cmp(&right.id));
    ensure_unique_by(children.iter().map(|value| &value.id))?;
    Ok(children)
}

fn parse_frequencies(
    value: Option<Value>,
    limits: EiaParseLimits,
) -> Result<Vec<EiaFrequencyMetadata>, EiaError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or(EiaError::InvalidProtocol)?;
    admit_items(values.len(), limits)?;
    let mut frequencies = Vec::with_capacity(values.len());
    for value in values {
        let mut object = exact_object(value, &["id", "description", "query", "format"])?;
        frequencies.push(EiaFrequencyMetadata {
            id: take_required_identifier(&mut object, "id", limits)?,
            description: take_optional_string(&mut object, "description", limits)?,
            query: take_optional_string(&mut object, "query", limits)?,
            format: take_required_string(&mut object, "format", limits)?,
        });
    }
    frequencies.sort_by(|left, right| left.id.cmp(&right.id));
    ensure_unique_by(frequencies.iter().map(|value| &value.id))?;
    Ok(frequencies)
}

fn parse_facets(
    value: Option<Value>,
    limits: EiaParseLimits,
) -> Result<Vec<EiaFacetMetadata>, EiaError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or(EiaError::InvalidProtocol)?;
    admit_items(values.len(), limits)?;
    let mut facets = Vec::with_capacity(values.len());
    for value in values {
        let mut object = exact_object(value, &["id", "description"])?;
        facets.push(EiaFacetMetadata {
            id: take_required_identifier(&mut object, "id", limits)?,
            description: take_optional_string(&mut object, "description", limits)?,
        });
    }
    facets.sort_by(|left, right| left.id.cmp(&right.id));
    ensure_unique_by(facets.iter().map(|value| &value.id))?;
    Ok(facets)
}

fn parse_data_columns(
    value: Option<Value>,
    limits: EiaParseLimits,
) -> Result<Vec<EiaDataColumnMetadata>, EiaError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let object = value.as_object().ok_or(EiaError::InvalidProtocol)?;
    admit_items(object.len(), limits)?;
    let mut columns = Vec::with_capacity(object.len());
    for (id, value) in object {
        let mut definition = exact_object(value, &["alias", "units"])?;
        columns.push(EiaDataColumnMetadata {
            id: EiaFieldId::try_from(id.as_str())?,
            alias: take_optional_string(&mut definition, "alias", limits)?,
            units: take_optional_string(&mut definition, "units", limits)?,
        });
    }
    columns.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(columns)
}

fn parse_facet_values(
    value: Option<Value>,
    limits: EiaParseLimits,
) -> Result<Vec<EiaFacetMetadataValue>, EiaError> {
    let value = value.ok_or(EiaError::InvalidProtocol)?;
    let values = value.as_array().ok_or(EiaError::InvalidProtocol)?;
    admit_items(values.len(), limits)?;
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let mut object = exact_object(value, &["id", "name", "alias"])?;
        parsed.push(EiaFacetMetadataValue {
            id: EiaFacetValue::try_from(take_required_string(&mut object, "id", limits)?)?,
            name: take_optional_string(&mut object, "name", limits)?,
            alias: take_optional_string(&mut object, "alias", limits)?,
        });
    }
    Ok(parsed)
}

fn exact_object(value: &Value, allowed: &[&str]) -> Result<Map<String, Value>, EiaError> {
    let object = value.as_object().ok_or(EiaError::InvalidProtocol)?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(EiaError::SchemaDrift);
    }
    Ok(object.clone())
}

fn take_required_identifier(
    object: &mut Map<String, Value>,
    key: &'static str,
    limits: EiaParseLimits,
) -> Result<EiaFieldId, EiaError> {
    EiaFieldId::try_from(take_required_string(object, key, limits)?)
}

fn take_optional_identifier(
    object: &mut Map<String, Value>,
    key: &'static str,
    limits: EiaParseLimits,
) -> Result<Option<EiaFieldId>, EiaError> {
    take_optional_string(object, key, limits)?
        .map(EiaFieldId::try_from)
        .transpose()
}

fn take_required_string(
    object: &mut Map<String, Value>,
    key: &'static str,
    limits: EiaParseLimits,
) -> Result<String, EiaError> {
    object
        .remove(key)
        .as_ref()
        .ok_or(EiaError::InvalidProtocol)
        .and_then(|value| parse_bounded_string(value, limits))
}

fn take_optional_string(
    object: &mut Map<String, Value>,
    key: &'static str,
    limits: EiaParseLimits,
) -> Result<Option<String>, EiaError> {
    object
        .remove(key)
        .map(|value| parse_bounded_string(&value, limits))
        .transpose()
}

fn ensure_unique_by<'a, T: Ord + 'a>(values: impl Iterator<Item = &'a T>) -> Result<(), EiaError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(EiaError::MetadataConflict);
        }
    }
    Ok(())
}

fn admit_items(count: usize, limits: EiaParseLimits) -> Result<(), EiaError> {
    if count > limits.max_metadata_items() {
        Err(EiaError::StructureLimit)
    } else {
        Ok(())
    }
}
