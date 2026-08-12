//! Frozen route contracts, offset pagination, exact native values, clocks, and revisions.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use market_squawk_domain::{CalendarDate, RevisionNumber, Timestamp};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::metadata::EiaRouteMetadata;
use crate::request::EiaDataPageRequest;
use crate::types::digest_parts;
use crate::wire::{parse_bounded_string, parse_count, parse_envelope};
use crate::{
    EiaApiVersion, EiaDataQuery, EiaDigest, EiaError, EiaFacetValue, EiaFieldId, EiaParseLimits,
    EiaRoute,
};

const MAX_DESCRIPTOR_FIELDS: usize = 128;
const MAX_CLOCK_FIELDS: usize = 16;
const MAX_MISSING_MARKERS: usize = 32;

/// Expected native kind of one selected EIA data column.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum EiaValueKind {
    /// Exact base-10 decimal, returned as a JSON string or exact JSON number.
    Decimal,
    /// Provider-native string retained as nonnumeric research evidence.
    String,
}

/// Route-specific unit treatment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum EiaUnitSource {
    /// Every row must contain `<field>-units` and it must match the optional frozen metadata unit.
    RowField,
    /// Every row uses this exact frozen unit and must not contain a conflicting row unit.
    Fixed(String),
}

/// Route-specific provider missing-marker contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaMissingPolicy {
    markers: BTreeSet<String>,
    null_is_missing: bool,
}

impl EiaMissingPolicy {
    /// Constructs an explicit bounded marker set. Empty strings are valid only when explicitly
    /// selected by the route contract.
    pub fn try_new(
        markers: impl IntoIterator<Item = String>,
        null_is_missing: bool,
    ) -> Result<Self, EiaError> {
        let markers: BTreeSet<_> = markers.into_iter().collect();
        if markers.len() > MAX_MISSING_MARKERS
            || markers
                .iter()
                .any(|marker| marker.len() > 512 || marker.chars().any(char::is_control))
        {
            return Err(EiaError::InvalidLimit);
        }
        Ok(Self {
            markers,
            null_is_missing,
        })
    }

    /// Returns whether JSON null is an admitted provider missing value.
    pub const fn null_is_missing(&self) -> bool {
        self.null_is_missing
    }

    /// Returns admitted exact lexical missing markers.
    pub const fn markers(&self) -> &BTreeSet<String> {
        &self.markers
    }
}

/// Complete input for one route data field.
#[derive(Clone, Debug)]
pub struct EiaDataFieldContractInput {
    /// Provider data-column identity.
    pub field: EiaFieldId,
    /// Closed native value kind.
    pub value_kind: EiaValueKind,
    /// Exact unit source.
    pub unit_source: EiaUnitSource,
    /// Explicit missing-value contract.
    pub missing_policy: EiaMissingPolicy,
}

/// One frozen selected data-column contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaDataFieldContract {
    field: EiaFieldId,
    value_kind: EiaValueKind,
    unit_source: EiaUnitSource,
    missing_policy: EiaMissingPolicy,
}

impl EiaDataFieldContract {
    /// Constructs a selected data-column contract.
    pub fn new(input: EiaDataFieldContractInput) -> Self {
        Self {
            field: input.field,
            value_kind: input.value_kind,
            unit_source: input.unit_source,
            missing_policy: input.missing_policy,
        }
    }

    /// Returns the provider field identity.
    pub const fn field(&self) -> &EiaFieldId {
        &self.field
    }

    /// Returns the closed value kind.
    pub const fn value_kind(&self) -> EiaValueKind {
        self.value_kind
    }

    /// Returns the exact unit source.
    pub const fn unit_source(&self) -> &EiaUnitSource {
        &self.unit_source
    }

    /// Returns provider missing-value rules.
    pub const fn missing_policy(&self) -> &EiaMissingPolicy {
        &self.missing_policy
    }
}

/// A row descriptor retained in stable series identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EiaDescriptor {
    field: EiaFieldId,
    value: String,
}

impl EiaDescriptor {
    /// Returns the provider field.
    pub const fn field(&self) -> &EiaFieldId {
        &self.field
    }

    /// Returns the exact provider value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// One exact row facet coordinate.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EiaFacetCoordinate {
    facet: EiaFieldId,
    value: EiaFacetValue,
}

impl EiaFacetCoordinate {
    /// Returns the facet field.
    pub const fn facet(&self) -> &EiaFieldId {
        &self.facet
    }

    /// Returns the exact facet value.
    pub const fn value(&self) -> &EiaFacetValue {
        &self.value
    }
}

/// Interpretation of a provider clock field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum EiaClockKind {
    /// Source release/publication time.
    Released,
    /// Provider update/correction time.
    Updated,
    /// Explicit provider-visible availability time.
    Available,
}

/// One route-specific exact clock-field mapping.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaClockField {
    field: EiaFieldId,
    kind: EiaClockKind,
}

impl EiaClockField {
    /// Constructs a clock mapping.
    pub const fn new(field: EiaFieldId, kind: EiaClockKind) -> Self {
        Self { field, kind }
    }

    /// Returns the provider field.
    pub const fn field(&self) -> &EiaFieldId {
        &self.field
    }

    /// Returns its semantic meaning.
    pub const fn kind(&self) -> EiaClockKind {
        self.kind
    }
}

/// Complete input to a frozen dataset contract.
#[derive(Clone, Debug)]
pub struct EiaDatasetContractInput {
    /// Frozen discovery metadata.
    pub metadata: EiaRouteMetadata,
    /// Exact selected data query.
    pub query: EiaDataQuery,
    /// Frozen selected data fields.
    pub fields: Vec<EiaDataFieldContract>,
    /// Additional non-facet fields retained in series identity.
    pub descriptor_fields: Vec<EiaFieldId>,
    /// Route-specific provider clock fields.
    pub clock_fields: Vec<EiaClockField>,
}

/// Route-specific native schema frozen from discovery before data acquisition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDatasetContract {
    metadata: EiaRouteMetadata,
    query: EiaDataQuery,
    fields: Vec<EiaDataFieldContract>,
    descriptor_fields: Vec<EiaFieldId>,
    clock_fields: Vec<EiaClockField>,
    schema_digest: EiaDigest,
}

impl EiaDatasetContract {
    /// Validates a query against its exact route metadata generation.
    pub fn try_new(input: EiaDatasetContractInput) -> Result<Self, EiaError> {
        let EiaDatasetContractInput {
            metadata,
            query,
            mut fields,
            mut descriptor_fields,
            mut clock_fields,
        } = input;
        if metadata.route() != query.route()
            || metadata.frequency(query.frequency()).is_none()
            || fields.is_empty()
            || fields.len() != query.data_fields().len()
            || descriptor_fields.len() > MAX_DESCRIPTOR_FIELDS
            || clock_fields.len() > MAX_CLOCK_FIELDS
        {
            return Err(EiaError::SchemaDrift);
        }
        fields.sort_by(|left, right| left.field.cmp(&right.field));
        descriptor_fields.sort();
        clock_fields.sort_by(|left, right| left.field.cmp(&right.field));
        ensure_unique(fields.iter().map(|field| &field.field))?;
        ensure_unique(descriptor_fields.iter())?;
        ensure_unique(clock_fields.iter().map(|clock| &clock.field))?;
        if fields
            .iter()
            .map(|field| &field.field)
            .ne(query.data_fields().iter())
        {
            return Err(EiaError::SchemaDrift);
        }
        for field in &fields {
            let metadata_field = metadata
                .data_column(&field.field)
                .ok_or(EiaError::SchemaDrift)?;
            match &field.unit_source {
                EiaUnitSource::Fixed(unit) => {
                    validate_retained_string(unit)?;
                    if metadata_field.units().is_some_and(|known| known != unit) {
                        return Err(EiaError::InvalidUnit);
                    }
                }
                EiaUnitSource::RowField => {}
            }
        }
        for facet in query.facets() {
            if metadata.facet(facet.facet()).is_none() {
                return Err(EiaError::SchemaDrift);
            }
        }
        let semantic = serde_json::to_vec(&(
            metadata.route(),
            metadata.api_version(),
            metadata.schema_digest(),
            query.identity(),
            &fields,
            &descriptor_fields,
            &clock_fields,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let schema_digest = digest_parts(b"eia-dataset-contract-v1", [semantic.as_slice()]);
        Ok(Self {
            metadata,
            query,
            fields,
            descriptor_fields,
            clock_fields,
            schema_digest,
        })
    }

    /// Returns the frozen route metadata.
    pub const fn metadata(&self) -> &EiaRouteMetadata {
        &self.metadata
    }

    /// Returns the exact base query.
    pub const fn query(&self) -> &EiaDataQuery {
        &self.query
    }

    /// Returns sorted selected field contracts.
    pub fn fields(&self) -> &[EiaDataFieldContract] {
        &self.fields
    }

    /// Returns additional series-descriptor fields.
    pub fn descriptor_fields(&self) -> &[EiaFieldId] {
        &self.descriptor_fields
    }

    /// Returns route-specific clock fields.
    pub fn clock_fields(&self) -> &[EiaClockField] {
        &self.clock_fields
    }

    /// Returns the complete route-native schema identity.
    pub const fn schema_digest(&self) -> EiaDigest {
        self.schema_digest
    }
}

/// Native period precision retained without inventing midnight.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum EiaPeriodKind {
    /// Civil day.
    CalendarDate(CalendarDate),
    /// Year and month.
    Month { year: u16, month: u8 },
    /// Provider quarter code.
    Quarter { year: u16, quarter: u8 },
    /// Civil year.
    Year(u16),
    /// Other provider format retained exactly.
    Provider(String),
}

/// Exact source period plus provider-declared format and frequency.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EiaPeriod {
    raw: String,
    format: String,
    frequency: EiaFieldId,
    kind: EiaPeriodKind,
}

impl EiaPeriod {
    /// Returns the exact provider lexical period.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the provider-declared format.
    pub fn format(&self) -> &str {
        &self.format
    }

    /// Returns the frequency.
    pub const fn frequency(&self) -> &EiaFieldId {
        &self.frequency
    }

    /// Returns parsed precision without inventing a finer coordinate.
    pub const fn kind(&self) -> &EiaPeriodKind {
        &self.kind
    }
}

/// Exact provider-native missing-value evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaNativeMissingValue {
    lexical: Option<String>,
}

impl EiaNativeMissingValue {
    /// Returns the exact lexical marker, or `None` for an admitted JSON null.
    pub fn lexical(&self) -> Option<&str> {
        self.lexical.as_deref()
    }
}

/// Closed provider-native observation value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum EiaNativeValue {
    /// Exact decimal with its retained provider lexical form.
    Decimal {
        /// Parsed exact decimal.
        value: Decimal,
        /// Exact provider lexical representation.
        lexical: String,
    },
    /// Provider-native string value.
    String(String),
    /// Explicit route-admitted missing value.
    Missing(EiaNativeMissingValue),
}

/// Provider-supplied clocks plus the mandatory local receipt clock.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EiaObservationClocks {
    released_at: Option<Timestamp>,
    updated_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    received_at: Timestamp,
}

impl EiaObservationClocks {
    /// Returns the source release clock when the route supplied it.
    pub const fn released_at(&self) -> Option<Timestamp> {
        self.released_at
    }

    /// Returns the provider update clock when supplied.
    pub const fn updated_at(&self) -> Option<Timestamp> {
        self.updated_at
    }

    /// Returns explicit provider availability when supplied.
    pub const fn available_at(&self) -> Option<Timestamp> {
        self.available_at
    }

    /// Returns the mandatory local receipt clock.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
}

/// Stable cross-page natural series identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EiaSeriesIdentity {
    route: EiaRoute,
    data_field: EiaFieldId,
    frequency: EiaFieldId,
    facets: Vec<EiaFacetCoordinate>,
    descriptors: Vec<EiaDescriptor>,
    unit: String,
    digest: EiaDigest,
}

impl EiaSeriesIdentity {
    /// Returns the route.
    pub const fn route(&self) -> &EiaRoute {
        &self.route
    }

    /// Returns the selected data field.
    pub const fn data_field(&self) -> &EiaFieldId {
        &self.data_field
    }

    /// Returns the frequency.
    pub const fn frequency(&self) -> &EiaFieldId {
        &self.frequency
    }

    /// Returns exact facet coordinates.
    pub fn facets(&self) -> &[EiaFacetCoordinate] {
        &self.facets
    }

    /// Returns exact additional descriptors.
    pub fn descriptors(&self) -> &[EiaDescriptor] {
        &self.descriptors
    }

    /// Returns exact unit text.
    pub fn unit(&self) -> &str {
        &self.unit
    }

    /// Returns the stable series digest.
    pub const fn digest(&self) -> EiaDigest {
        self.digest
    }
}

/// Stable natural family key excluding revision and value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EiaObservationFamily {
    series_digest: EiaDigest,
    period: EiaPeriod,
}

impl EiaObservationFamily {
    /// Returns the stable series identity.
    pub const fn series_digest(&self) -> EiaDigest {
        self.series_digest
    }

    /// Returns the effective period.
    pub const fn period(&self) -> &EiaPeriod {
        &self.period
    }
}

/// One exact provider-native observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaObservation {
    family: EiaObservationFamily,
    series: EiaSeriesIdentity,
    period: EiaPeriod,
    value: EiaNativeValue,
    clocks: EiaObservationClocks,
    row_schema_digest: EiaDigest,
    semantic_digest: EiaDigest,
    row_digest: EiaDigest,
    page_payload_digest: EiaDigest,
}

impl EiaObservation {
    /// Returns the natural family.
    pub const fn family(&self) -> &EiaObservationFamily {
        &self.family
    }

    /// Returns stable series identity.
    pub const fn series(&self) -> &EiaSeriesIdentity {
        &self.series
    }

    /// Returns the effective period.
    pub const fn period(&self) -> &EiaPeriod {
        &self.period
    }

    /// Returns exact provider-native value state.
    pub const fn value(&self) -> &EiaNativeValue {
        &self.value
    }

    /// Returns all applicable provider/local clocks.
    pub const fn clocks(&self) -> &EiaObservationClocks {
        &self.clocks
    }

    /// Returns the native row shape identity.
    pub const fn row_schema_digest(&self) -> EiaDigest {
        self.row_schema_digest
    }

    /// Returns provider-semantic identity excluding local receipt and raw-page placement.
    pub const fn semantic_digest(&self) -> EiaDigest {
        self.semantic_digest
    }

    /// Returns the complete typed row content identity.
    pub const fn row_digest(&self) -> EiaDigest {
        self.row_digest
    }

    /// Returns exact raw page payload identity.
    pub const fn page_payload_digest(&self) -> EiaDigest {
        self.page_payload_digest
    }
}

/// Page completion state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaPageCompleteness {
    /// More rows remain at `next_offset`.
    More { next_offset: u64 },
    /// This page exactly closes the provider-declared total.
    Complete,
}

/// One secret-free request/page/returned-row receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDataPageReceipt {
    query_digest: EiaDigest,
    request_digest: EiaDigest,
    transport_payload_digest: EiaDigest,
    retained_payload_digest: EiaDigest,
    request_echo_digest: EiaDigest,
    envelope_schema_digest: EiaDigest,
    row_schema_digest: EiaDigest,
    offset: u64,
    requested_length: u16,
    total: u64,
    returned_rows: u64,
    observation_count: u64,
    missing_observation_count: u64,
    response_bytes: usize,
    received_at: Timestamp,
    completeness: EiaPageCompleteness,
    redacted_secret_fields: usize,
}

impl EiaDataPageReceipt {
    /// Returns base-query identity.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns this page request identity.
    pub const fn request_digest(&self) -> EiaDigest {
        self.request_digest
    }

    /// Returns the digest of exact transport bytes before ephemeral secret redaction.
    pub const fn transport_payload_digest(&self) -> EiaDigest {
        self.transport_payload_digest
    }

    /// Returns secret-free retained raw payload identity.
    pub const fn retained_payload_digest(&self) -> EiaDigest {
        self.retained_payload_digest
    }

    /// Returns redacted interpreted-request identity.
    pub const fn request_echo_digest(&self) -> EiaDigest {
        self.request_echo_digest
    }

    /// Returns API envelope schema identity.
    pub const fn envelope_schema_digest(&self) -> EiaDigest {
        self.envelope_schema_digest
    }

    /// Returns native row-shape identity.
    pub const fn row_schema_digest(&self) -> EiaDigest {
        self.row_schema_digest
    }

    /// Returns requested zero-based offset.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns requested rows, which are not counted as observations.
    pub const fn requested_length(&self) -> u16 {
        self.requested_length
    }

    /// Returns provider-declared total matching rows.
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// Returns provider rows successfully validated on this page.
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }

    /// Returns actual typed field observations emitted.
    pub const fn observation_count(&self) -> u64 {
        self.observation_count
    }

    /// Returns typed observations whose value was explicit provider missing evidence.
    pub const fn missing_observation_count(&self) -> u64 {
        self.missing_observation_count
    }

    /// Returns exact response bytes.
    pub const fn response_bytes(&self) -> usize {
        self.response_bytes
    }

    /// Returns the local receipt clock.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns exact completion state.
    pub const fn completeness(&self) -> EiaPageCompleteness {
        self.completeness
    }

    /// Returns redacted echoed secret-field count.
    pub const fn redacted_secret_fields(&self) -> usize {
        self.redacted_secret_fields
    }
}

/// One parsed page and its exact secret-free retained payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDataPage {
    api_version: EiaApiVersion,
    frequency: EiaFieldId,
    date_format: String,
    observations: Vec<EiaObservation>,
    receipt: EiaDataPageReceipt,
    retained_payload: Arc<[u8]>,
}

impl EiaDataPage {
    /// Parses one page against a previously frozen exact route contract.
    pub fn parse(
        bytes: &[u8],
        request: EiaDataPageRequest<'_>,
        contract: &EiaDatasetContract,
        received_at: Timestamp,
        limits: EiaParseLimits,
    ) -> Result<Self, EiaError> {
        if request.query() != contract.query() {
            return Err(EiaError::RequestEchoMismatch);
        }
        let secret_free = request.secret_free()?;
        let expected_command = format!("/v2/{}/data", contract.query().route());
        let envelope = parse_envelope(bytes, &expected_command, limits)?;
        if &envelope.api_version != contract.metadata().api_version() {
            return Err(EiaError::ApiVersionDrift);
        }
        let mut response = envelope.response;
        const RESPONSE_FIELDS: &[&str] = &["total", "dateFormat", "frequency", "data"];
        if response
            .keys()
            .any(|key| !RESPONSE_FIELDS.contains(&key.as_str()))
        {
            return Err(EiaError::SchemaDrift);
        }
        let total = response
            .remove("total")
            .as_ref()
            .ok_or(EiaError::InvalidProtocol)
            .and_then(parse_count)?;
        let frequency = EiaFieldId::try_from(
            parse_bounded_string(
                response
                    .remove("frequency")
                    .as_ref()
                    .ok_or(EiaError::InvalidProtocol)?,
                limits,
            )?
            .as_str(),
        )?;
        if &frequency != contract.query().frequency() {
            return Err(EiaError::SchemaDrift);
        }
        let date_format = parse_bounded_string(
            response
                .remove("dateFormat")
                .as_ref()
                .ok_or(EiaError::InvalidProtocol)?,
            limits,
        )?;
        let frozen_format = contract
            .metadata()
            .frequency(&frequency)
            .ok_or(EiaError::SchemaDrift)?
            .format();
        if date_format != frozen_format {
            return Err(EiaError::SchemaDrift);
        }
        let rows = response
            .remove("data")
            .and_then(|value| value.as_array().cloned())
            .ok_or(EiaError::InvalidProtocol)?;
        if rows.len() > usize::from(contract.query().length()) || rows.len() > limits.max_rows() {
            return Err(EiaError::Pagination);
        }
        let returned_rows = u64::try_from(rows.len()).map_err(|_| EiaError::InvalidLimit)?;
        let consumed = request
            .offset()
            .checked_add(returned_rows)
            .ok_or(EiaError::Pagination)?;
        if request.offset() > total
            || consumed > total
            || (returned_rows == 0 && request.offset() < total)
            || (consumed < total && returned_rows != u64::from(contract.query().length()))
        {
            return Err(EiaError::Pagination);
        }
        let completeness = if consumed == total {
            EiaPageCompleteness::Complete
        } else {
            EiaPageCompleteness::More {
                next_offset: consumed,
            }
        };
        let row_schema_digest = common_row_schema_digest(&rows)?;
        let page_payload_digest = envelope.retained_payload_digest;
        let mut observations = Vec::new();
        let mut families = BTreeMap::new();
        let row_context = RowParseContext {
            contract,
            received_at,
            page_payload_digest,
            row_schema_digest,
            limits,
        };
        for row in rows {
            let object = row.as_object().ok_or(EiaError::InvalidProtocol)?;
            parse_row(object, &row_context, &mut observations, &mut families)?;
        }
        let observation_count =
            u64::try_from(observations.len()).map_err(|_| EiaError::InvalidLimit)?;
        let missing_observation_count = u64::try_from(
            observations
                .iter()
                .filter(|observation| matches!(observation.value, EiaNativeValue::Missing(_)))
                .count(),
        )
        .map_err(|_| EiaError::InvalidLimit)?;
        let receipt = EiaDataPageReceipt {
            query_digest: contract.query().identity(),
            request_digest: secret_free.request_digest(),
            transport_payload_digest: envelope.transport_payload_digest,
            retained_payload_digest: envelope.retained_payload_digest,
            request_echo_digest: envelope.request_echo_digest,
            envelope_schema_digest: envelope.envelope_schema_digest,
            row_schema_digest,
            offset: request.offset(),
            requested_length: contract.query().length(),
            total,
            returned_rows,
            observation_count,
            missing_observation_count,
            response_bytes: bytes.len(),
            received_at,
            completeness,
            redacted_secret_fields: envelope.redacted_secret_fields,
        };
        Ok(Self {
            api_version: envelope.api_version,
            frequency,
            date_format,
            observations,
            receipt,
            retained_payload: Arc::from(envelope.retained_payload),
        })
    }

    /// Returns the serving API version.
    pub const fn api_version(&self) -> &EiaApiVersion {
        &self.api_version
    }

    /// Returns provider-confirmed frequency.
    pub const fn frequency(&self) -> &EiaFieldId {
        &self.frequency
    }

    /// Returns provider-confirmed date format.
    pub fn date_format(&self) -> &str {
        &self.date_format
    }

    /// Returns all emitted field observations.
    pub fn observations(&self) -> &[EiaObservation] {
        &self.observations
    }

    /// Returns the complete page receipt.
    pub const fn receipt(&self) -> &EiaDataPageReceipt {
        &self.receipt
    }

    /// Returns secret-free raw bytes suitable for bounded capture.
    pub fn retained_payload(&self) -> &[u8] {
        &self.retained_payload
    }

    pub(crate) fn retained_payload_arc(&self) -> Arc<[u8]> {
        Arc::clone(&self.retained_payload)
    }
}

/// Stateful completeness verifier for an ordered offset chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaPaginationTracker {
    query_digest: EiaDigest,
    api_version: EiaApiVersion,
    total: u64,
    next_offset: u64,
    page_count: u32,
    returned_rows: u64,
    observation_count: u64,
    missing_observation_count: u64,
    response_bytes: u64,
    closed: bool,
    retained_page_digests: BTreeSet<EiaDigest>,
}

impl EiaPaginationTracker {
    /// Starts from an exact first page at offset zero.
    pub fn start(page: &EiaDataPage) -> Result<Self, EiaError> {
        if page.receipt.offset != 0 {
            return Err(EiaError::Pagination);
        }
        let mut tracker = Self {
            query_digest: page.receipt.query_digest,
            api_version: page.api_version.clone(),
            total: page.receipt.total,
            next_offset: 0,
            page_count: 0,
            returned_rows: 0,
            observation_count: 0,
            missing_observation_count: 0,
            response_bytes: 0,
            closed: false,
            retained_page_digests: BTreeSet::new(),
        };
        tracker.push(page)?;
        Ok(tracker)
    }

    /// Admits exactly the next offset page and rejects total/version/schema/replay drift.
    pub fn push(&mut self, page: &EiaDataPage) -> Result<(), EiaError> {
        if self.closed
            || page.receipt.query_digest != self.query_digest
            || page.api_version != self.api_version
            || page.receipt.total != self.total
            || page.receipt.offset != self.next_offset
            || !self
                .retained_page_digests
                .insert(page.receipt.retained_payload_digest)
        {
            return Err(EiaError::Pagination);
        }
        self.page_count = self.page_count.checked_add(1).ok_or(EiaError::Pagination)?;
        self.returned_rows = self
            .returned_rows
            .checked_add(page.receipt.returned_rows)
            .ok_or(EiaError::Pagination)?;
        self.observation_count = self
            .observation_count
            .checked_add(page.receipt.observation_count)
            .ok_or(EiaError::Pagination)?;
        self.missing_observation_count = self
            .missing_observation_count
            .checked_add(page.receipt.missing_observation_count)
            .ok_or(EiaError::Pagination)?;
        self.response_bytes = self
            .response_bytes
            .checked_add(
                u64::try_from(page.receipt.response_bytes).map_err(|_| EiaError::Pagination)?,
            )
            .ok_or(EiaError::Pagination)?;
        match page.receipt.completeness {
            EiaPageCompleteness::More { next_offset } => self.next_offset = next_offset,
            EiaPageCompleteness::Complete => {
                self.next_offset = self.total;
                self.closed = true;
            }
        }
        Ok(())
    }

    /// Returns the exact next offset, or `None` after closure.
    pub const fn next_offset(&self) -> Option<u64> {
        if self.closed {
            None
        } else {
            Some(self.next_offset)
        }
    }

    /// Finishes only after the exact returned-row total closes.
    pub fn finish(self) -> Result<EiaAcquisitionReceipt, EiaError> {
        if !self.closed || self.returned_rows != self.total {
            return Err(EiaError::Pagination);
        }
        let page_digests: Vec<_> = self.retained_page_digests.into_iter().collect();
        let digest_material = serde_json::to_vec(&(
            self.query_digest,
            &self.api_version,
            self.total,
            self.page_count,
            self.returned_rows,
            self.observation_count,
            self.missing_observation_count,
            self.response_bytes,
            &page_digests,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        Ok(EiaAcquisitionReceipt {
            query_digest: self.query_digest,
            api_version: self.api_version,
            total: self.total,
            page_count: self.page_count,
            returned_rows: self.returned_rows,
            observation_count: self.observation_count,
            missing_observation_count: self.missing_observation_count,
            response_bytes: self.response_bytes,
            page_digests,
            content_digest: digest_parts(
                b"eia-acquisition-receipt-v1",
                [digest_material.as_slice()],
            ),
        })
    }
}

/// Complete page-chain receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaAcquisitionReceipt {
    query_digest: EiaDigest,
    api_version: EiaApiVersion,
    total: u64,
    page_count: u32,
    returned_rows: u64,
    observation_count: u64,
    missing_observation_count: u64,
    response_bytes: u64,
    page_digests: Vec<EiaDigest>,
    content_digest: EiaDigest,
}

impl EiaAcquisitionReceipt {
    /// Returns exact matching-row total.
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// Returns admitted page count.
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Returns returned rows; this remains distinct from observations.
    pub const fn returned_rows(&self) -> u64 {
        self.returned_rows
    }

    /// Returns actual emitted observations.
    pub const fn observation_count(&self) -> u64 {
        self.observation_count
    }

    /// Returns missing observations.
    pub const fn missing_observation_count(&self) -> u64 {
        self.missing_observation_count
    }

    /// Returns response bytes over all pages.
    pub const fn response_bytes(&self) -> u64 {
        self.response_bytes
    }

    /// Returns exact unique retained page payload identities.
    pub fn page_digests(&self) -> &[EiaDigest] {
        &self.page_digests
    }

    /// Returns the complete-chain receipt identity.
    pub const fn content_digest(&self) -> EiaDigest {
        self.content_digest
    }
}

/// Complete observations plus their page-chain receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaAcquisition {
    observations: Vec<EiaObservation>,
    receipt: EiaAcquisitionReceipt,
}

impl EiaAcquisition {
    /// Combines pages only after exact pagination closure and rejects cross-page conflicts.
    pub fn try_from_pages(pages: Vec<EiaDataPage>) -> Result<Self, EiaError> {
        let first = pages.first().ok_or(EiaError::Pagination)?;
        let mut tracker = EiaPaginationTracker::start(first)?;
        for page in pages.iter().skip(1) {
            tracker.push(page)?;
        }
        let receipt = tracker.finish()?;
        let mut observations = Vec::new();
        let mut families = BTreeMap::new();
        for page in pages {
            for observation in page.observations {
                if let Some(existing) =
                    families.insert(observation.family.clone(), observation.semantic_digest)
                    && existing != observation.semantic_digest
                {
                    return Err(EiaError::ObservationConflict);
                }
                observations.push(observation);
            }
        }
        Ok(Self {
            observations,
            receipt,
        })
    }

    /// Returns all typed observations.
    pub fn observations(&self) -> &[EiaObservation] {
        &self.observations
    }

    /// Returns complete pagination evidence.
    pub const fn receipt(&self) -> &EiaAcquisitionReceipt {
        &self.receipt
    }
}

/// Previous durable head supplied by the owning shared revision authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaRevisionHead {
    family: EiaObservationFamily,
    revision: RevisionNumber,
    semantic_digest: EiaDigest,
}

impl EiaRevisionHead {
    /// Constructs one previous head.
    pub const fn new(
        family: EiaObservationFamily,
        revision: RevisionNumber,
        semantic_digest: EiaDigest,
    ) -> Self {
        Self {
            family,
            revision,
            semantic_digest,
        }
    }
}

/// Append disposition for one reacquired family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaRevisionDisposition {
    /// First locally observed version.
    New,
    /// Exact repeat; no new semantic revision.
    Duplicate,
    /// Changed provider value/clocks/series evidence; append a new revision.
    Revised,
}

/// One deterministic revision plan entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaRevisionPlanEntry {
    observation_index: usize,
    disposition: EiaRevisionDisposition,
    revision: RevisionNumber,
    predecessor_digest: Option<EiaDigest>,
}

impl EiaRevisionPlanEntry {
    /// Returns the input observation index.
    pub const fn observation_index(&self) -> usize {
        self.observation_index
    }

    /// Returns append disposition.
    pub const fn disposition(&self) -> EiaRevisionDisposition {
        self.disposition
    }

    /// Returns selected one-based revision.
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }

    /// Returns predecessor row identity for a revision.
    pub const fn predecessor_digest(&self) -> Option<EiaDigest> {
        self.predecessor_digest
    }
}

/// Diagnostic for an input conflict before revision planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaObservationConflict {
    family: EiaObservationFamily,
    first_digest: EiaDigest,
    second_digest: EiaDigest,
}

impl EiaObservationConflict {
    /// Returns the conflicting natural family.
    pub const fn family(&self) -> &EiaObservationFamily {
        &self.family
    }

    /// Returns the first row identity.
    pub const fn first_digest(&self) -> EiaDigest {
        self.first_digest
    }

    /// Returns the second row identity.
    pub const fn second_digest(&self) -> EiaDigest {
        self.second_digest
    }
}

/// Produces an append-only plan while leaving durable compare-and-swap to the shared authority.
pub fn plan_revisions(
    observations: &[EiaObservation],
    previous: &[EiaRevisionHead],
) -> Result<Vec<EiaRevisionPlanEntry>, EiaError> {
    let mut previous_by_family = BTreeMap::new();
    for head in previous {
        if previous_by_family
            .insert(head.family.clone(), head)
            .is_some()
        {
            return Err(EiaError::InvalidRevision);
        }
    }
    let mut current = BTreeMap::new();
    let mut plan = Vec::with_capacity(observations.len());
    for (index, observation) in observations.iter().enumerate() {
        if let Some(existing) =
            current.insert(observation.family.clone(), observation.semantic_digest)
            && existing != observation.semantic_digest
        {
            return Err(EiaError::ObservationConflict);
        }
        let entry = match previous_by_family.get(&observation.family) {
            None => EiaRevisionPlanEntry {
                observation_index: index,
                disposition: EiaRevisionDisposition::New,
                revision: RevisionNumber::new(1).map_err(|_| EiaError::InvalidRevision)?,
                predecessor_digest: None,
            },
            Some(head) if head.semantic_digest == observation.semantic_digest => {
                EiaRevisionPlanEntry {
                    observation_index: index,
                    disposition: EiaRevisionDisposition::Duplicate,
                    revision: head.revision,
                    predecessor_digest: Some(head.semantic_digest),
                }
            }
            Some(head) => EiaRevisionPlanEntry {
                observation_index: index,
                disposition: EiaRevisionDisposition::Revised,
                revision: RevisionNumber::new(
                    head.revision
                        .get()
                        .checked_add(1)
                        .ok_or(EiaError::InvalidRevision)?,
                )
                .map_err(|_| EiaError::InvalidRevision)?,
                predecessor_digest: Some(head.semantic_digest),
            },
        };
        plan.push(entry);
    }
    Ok(plan)
}

struct RowParseContext<'a> {
    contract: &'a EiaDatasetContract,
    received_at: Timestamp,
    page_payload_digest: EiaDigest,
    row_schema_digest: EiaDigest,
    limits: EiaParseLimits,
}

fn parse_row(
    row: &Map<String, Value>,
    context: &RowParseContext<'_>,
    observations: &mut Vec<EiaObservation>,
    families: &mut BTreeMap<EiaObservationFamily, EiaDigest>,
) -> Result<(), EiaError> {
    let RowParseContext {
        contract,
        received_at,
        page_payload_digest,
        row_schema_digest,
        limits,
    } = *context;
    let expected = expected_row_fields(contract);
    if row.keys().any(|key| !expected.contains(key)) {
        return Err(EiaError::SchemaDrift);
    }
    for key in &expected {
        if !row.contains_key(key) {
            return Err(EiaError::SchemaDrift);
        }
    }
    let period = parse_period(
        row.get("period").ok_or(EiaError::InvalidProtocol)?,
        contract,
        limits,
    )?;
    let facets = contract
        .query()
        .facets()
        .iter()
        .map(|facet| {
            let value = parse_bounded_string(
                row.get(facet.facet().as_str())
                    .ok_or(EiaError::SchemaDrift)?,
                limits,
            )?;
            let value = EiaFacetValue::try_from(value)?;
            if !facet.values().contains(&value) {
                return Err(EiaError::SchemaDrift);
            }
            Ok(EiaFacetCoordinate {
                facet: facet.facet().clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>, EiaError>>()?;
    let descriptors = contract
        .descriptor_fields()
        .iter()
        .map(|field| {
            Ok(EiaDescriptor {
                field: field.clone(),
                value: parse_bounded_string(
                    row.get(field.as_str()).ok_or(EiaError::SchemaDrift)?,
                    limits,
                )?,
            })
        })
        .collect::<Result<Vec<_>, EiaError>>()?;
    let clocks = parse_clocks(row, contract, received_at, limits)?;
    for field in contract.fields() {
        let value = parse_value(
            row.get(field.field().as_str())
                .ok_or(EiaError::SchemaDrift)?,
            field,
            limits,
        )?;
        let unit = parse_unit(row, field, contract.metadata(), limits)?;
        let series_digest_material = serde_json::to_vec(&(
            contract.query().route(),
            field.field(),
            contract.query().frequency(),
            &facets,
            &descriptors,
            &unit,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let series_digest = digest_parts(
            b"eia-series-identity-v1",
            [series_digest_material.as_slice()],
        );
        let series = EiaSeriesIdentity {
            route: contract.query().route().clone(),
            data_field: field.field().clone(),
            frequency: contract.query().frequency().clone(),
            facets: facets.clone(),
            descriptors: descriptors.clone(),
            unit,
            digest: series_digest,
        };
        let family = EiaObservationFamily {
            series_digest,
            period: period.clone(),
        };
        let semantic_material = serde_json::to_vec(&(
            contract.schema_digest(),
            &family,
            &series,
            &period,
            &value,
            clocks.released_at,
            clocks.updated_at,
            clocks.available_at,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let semantic_digest = digest_parts(
            b"eia-native-observation-semantic-v1",
            [semantic_material.as_slice()],
        );
        let row_material = serde_json::to_vec(&(
            semantic_digest,
            clocks.received_at,
            row_schema_digest,
            page_payload_digest,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let row_digest = digest_parts(b"eia-native-observation-v1", [row_material.as_slice()]);
        if let Some(existing) = families.insert(family.clone(), semantic_digest) {
            if existing != semantic_digest {
                return Err(EiaError::ObservationConflict);
            }
            continue;
        }
        observations.push(EiaObservation {
            family,
            series,
            period: period.clone(),
            value,
            clocks: clocks.clone(),
            row_schema_digest,
            semantic_digest,
            row_digest,
            page_payload_digest,
        });
    }
    Ok(())
}

fn expected_row_fields(contract: &EiaDatasetContract) -> BTreeSet<String> {
    let mut expected = BTreeSet::from(["period".to_owned()]);
    for field in contract.fields() {
        expected.insert(field.field().as_str().to_owned());
        if matches!(field.unit_source(), EiaUnitSource::RowField) {
            expected.insert(format!("{}-units", field.field()));
        }
    }
    for facet in contract.query().facets() {
        expected.insert(facet.facet().as_str().to_owned());
    }
    for field in contract.descriptor_fields() {
        expected.insert(field.as_str().to_owned());
    }
    for clock in contract.clock_fields() {
        expected.insert(clock.field().as_str().to_owned());
    }
    expected
}

fn parse_period(
    value: &Value,
    contract: &EiaDatasetContract,
    limits: EiaParseLimits,
) -> Result<EiaPeriod, EiaError> {
    let raw = parse_bounded_string(value, limits)?;
    let frequency = contract.query().frequency().clone();
    let format = contract
        .metadata()
        .frequency(&frequency)
        .ok_or(EiaError::SchemaDrift)?
        .format()
        .to_owned();
    let kind = match format.as_str() {
        "YYYY" => EiaPeriodKind::Year(parse_year(&raw)?),
        "YYYY-MM" => {
            let (year, month) = parse_year_month(&raw)?;
            EiaPeriodKind::Month { year, month }
        }
        "YYYY-Q" | "YYYY-Q#" => {
            let (year, quarter) = parse_quarter(&raw)?;
            EiaPeriodKind::Quarter { year, quarter }
        }
        "YYYY-MM-DD" => EiaPeriodKind::CalendarDate(parse_calendar_date(&raw)?),
        _ => {
            validate_retained_string(&raw)?;
            EiaPeriodKind::Provider(raw.clone())
        }
    };
    Ok(EiaPeriod {
        raw,
        format,
        frequency,
        kind,
    })
}

fn parse_value(
    value: &Value,
    contract: &EiaDataFieldContract,
    limits: EiaParseLimits,
) -> Result<EiaNativeValue, EiaError> {
    if value.is_null() {
        return contract
            .missing_policy()
            .null_is_missing()
            .then_some(EiaNativeValue::Missing(EiaNativeMissingValue {
                lexical: None,
            }))
            .ok_or(EiaError::InvalidValue);
    }
    let lexical = match value {
        Value::String(value) => {
            if value.len() > limits.max_string_bytes() || value.chars().any(char::is_control) {
                return Err(EiaError::StructureLimit);
            }
            value.clone()
        }
        Value::Number(value) => value.to_string(),
        _ => return Err(EiaError::InvalidValue),
    };
    if contract.missing_policy().markers().contains(&lexical) {
        return Ok(EiaNativeValue::Missing(EiaNativeMissingValue {
            lexical: Some(lexical),
        }));
    }
    match contract.value_kind() {
        EiaValueKind::Decimal => Decimal::from_str_exact(&lexical)
            .map(|value| EiaNativeValue::Decimal {
                value: value.normalize(),
                lexical,
            })
            .map_err(|_| EiaError::InvalidValue),
        EiaValueKind::String => Ok(EiaNativeValue::String(lexical)),
    }
}

fn parse_unit(
    row: &Map<String, Value>,
    field: &EiaDataFieldContract,
    metadata: &EiaRouteMetadata,
    limits: EiaParseLimits,
) -> Result<String, EiaError> {
    let metadata_unit = metadata
        .data_column(field.field())
        .ok_or(EiaError::SchemaDrift)?
        .units();
    match field.unit_source() {
        EiaUnitSource::Fixed(unit) => Ok(unit.clone()),
        EiaUnitSource::RowField => {
            let key = format!("{}-units", field.field());
            let unit = parse_bounded_string(row.get(&key).ok_or(EiaError::InvalidUnit)?, limits)?;
            if metadata_unit.is_some_and(|known| known != unit) {
                return Err(EiaError::InvalidUnit);
            }
            Ok(unit)
        }
    }
}

fn parse_clocks(
    row: &Map<String, Value>,
    contract: &EiaDatasetContract,
    received_at: Timestamp,
    limits: EiaParseLimits,
) -> Result<EiaObservationClocks, EiaError> {
    let mut released_at = None;
    let mut updated_at = None;
    let mut available_at = None;
    for mapping in contract.clock_fields() {
        let lexical = parse_bounded_string(
            row.get(mapping.field().as_str())
                .ok_or(EiaError::InvalidClock)?,
            limits,
        )?;
        let parsed = parse_timestamp(&lexical)?;
        let slot = match mapping.kind() {
            EiaClockKind::Released => &mut released_at,
            EiaClockKind::Updated => &mut updated_at,
            EiaClockKind::Available => &mut available_at,
        };
        if slot.replace(parsed).is_some() {
            return Err(EiaError::InvalidClock);
        }
    }
    if released_at.is_some_and(|value| value > received_at)
        || updated_at.is_some_and(|value| value > received_at)
        || available_at.is_some_and(|value| value > received_at)
        || matches!((released_at, updated_at), (Some(released), Some(updated)) if updated < released)
        || matches!((released_at, available_at), (Some(released), Some(available)) if available < released)
    {
        return Err(EiaError::InvalidClock);
    }
    Ok(EiaObservationClocks {
        released_at,
        updated_at,
        available_at,
        received_at,
    })
}

fn common_row_schema_digest(rows: &[Value]) -> Result<EiaDigest, EiaError> {
    let mut expected: Option<Vec<String>> = None;
    for row in rows {
        let object = row.as_object().ok_or(EiaError::InvalidProtocol)?;
        let mut keys: Vec<_> = object.keys().cloned().collect();
        keys.sort();
        if expected.as_ref().is_some_and(|expected| expected != &keys) {
            return Err(EiaError::SchemaDrift);
        }
        expected = Some(keys);
    }
    let encoded =
        serde_json::to_vec(&expected.unwrap_or_default()).map_err(|_| EiaError::InvalidJson)?;
    Ok(digest_parts(b"eia-row-fields-v1", [encoded.as_slice()]))
}

fn parse_timestamp(value: &str) -> Result<Timestamp, EiaError> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| EiaError::InvalidClock)?;
    let parsed = parsed.with_timezone(&Utc);
    let seconds = parsed.timestamp();
    let nanos = i64::from(parsed.timestamp_subsec_nanos());
    let total = seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(nanos))
        .ok_or(EiaError::InvalidClock)?;
    Ok(Timestamp::from_unix_nanos(total))
}

fn parse_calendar_date(value: &str) -> Result<CalendarDate, EiaError> {
    let value = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| EiaError::InvalidClock)?;
    CalendarDate::new(
        u16::try_from(value.year()).map_err(|_| EiaError::InvalidClock)?,
        u8::try_from(value.month()).map_err(|_| EiaError::InvalidClock)?,
        u8::try_from(value.day()).map_err(|_| EiaError::InvalidClock)?,
    )
    .map_err(|_| EiaError::InvalidClock)
}

fn parse_year(value: &str) -> Result<u16, EiaError> {
    if value.len() != 4 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(EiaError::InvalidClock);
    }
    let year = value.parse::<u16>().map_err(|_| EiaError::InvalidClock)?;
    NonZeroU32::new(u32::from(year))
        .map(|_| year)
        .ok_or(EiaError::InvalidClock)
}

fn parse_year_month(value: &str) -> Result<(u16, u8), EiaError> {
    if value.len() != 7 || value.as_bytes().get(4) != Some(&b'-') {
        return Err(EiaError::InvalidClock);
    }
    let year = parse_year(&value[..4])?;
    let month = value[5..]
        .parse::<u8>()
        .map_err(|_| EiaError::InvalidClock)?;
    if !(1..=12).contains(&month) {
        return Err(EiaError::InvalidClock);
    }
    Ok((year, month))
}

fn parse_quarter(value: &str) -> Result<(u16, u8), EiaError> {
    let (year, quarter) = value.split_once("-Q").ok_or(EiaError::InvalidClock)?;
    let year = parse_year(year)?;
    let quarter = quarter.parse::<u8>().map_err(|_| EiaError::InvalidClock)?;
    if !(1..=4).contains(&quarter) {
        return Err(EiaError::InvalidClock);
    }
    Ok((year, quarter))
}

fn ensure_unique<'a, T: Ord + 'a>(values: impl Iterator<Item = &'a T>) -> Result<(), EiaError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(EiaError::MetadataConflict);
        }
    }
    Ok(())
}

fn validate_retained_string(value: &str) -> Result<(), EiaError> {
    if value.is_empty() || value.len() > 32 * 1024 || value.chars().any(char::is_control) {
        Err(EiaError::InvalidIdentifier)
    } else {
        Ok(())
    }
}
