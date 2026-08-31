//! Frozen route contracts, offset pagination, exact native values, clocks, and revisions.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::num::NonZeroU32;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use market_squawk_domain::{CalendarDate, Timestamp};
use market_squawk_sources::{
    MAX_OBSERVED_REVISION_BATCH_BYTES, MAX_OBSERVED_REVISION_BATCH_RECORDS,
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::metadata::{EiaFacetCatalog, EiaFacetMetadata, EiaRouteMetadata};
use crate::request::EiaDataPageRequest;
use crate::types::digest_parts;
use crate::wire::{parse_bounded_string, parse_count, parse_envelope};
use crate::{
    EiaApiVersion, EiaDataQuery, EiaDigest, EiaError, EiaFacetFilter, EiaFacetValue, EiaFieldId,
    EiaParseLimits, EiaRoute, EiaSortDirection,
};

const MAX_DESCRIPTOR_FIELDS: usize = 128;
const MAX_CLOCK_FIELDS: usize = 16;
const MAX_MISSING_MARKERS: usize = 32;

/// Largest record cardinality accepted before exact deep-byte admission is applied.
///
/// This is only the shared record-count ceiling. Every page and terminal canonical candidate is
/// independently charged from its actual retained fields against the shared 64 MiB byte ceiling.
pub const EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS: usize = MAX_OBSERVED_REVISION_BATCH_RECORDS;

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
    /// Complete value catalogs for every route facet in the query.
    pub facet_catalogs: Vec<EiaFacetCatalog>,
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
    facet_catalogs: Vec<EiaFacetCatalog>,
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
            mut facet_catalogs,
            mut descriptor_fields,
            mut clock_fields,
        } = input;
        if metadata.route() != query.route()
            || metadata.frequency(query.frequency()).is_none()
            || fields.is_empty()
            || fields.len() != query.data_fields().len()
            || facet_catalogs.len() != query.facets().len()
            || metadata.facets().len() != query.facets().len()
            || descriptor_fields.len() > MAX_DESCRIPTOR_FIELDS
            || clock_fields.len() > MAX_CLOCK_FIELDS
        {
            return Err(EiaError::SchemaDrift);
        }
        fields.sort_by(|left, right| left.field.cmp(&right.field));
        facet_catalogs.sort_by(|left, right| left.facet().cmp(right.facet()));
        descriptor_fields.sort();
        clock_fields.sort_by(|left, right| left.field.cmp(&right.field));
        ensure_unique(fields.iter().map(|field| &field.field))?;
        ensure_unique(facet_catalogs.iter().map(EiaFacetCatalog::facet))?;
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
        if metadata
            .facets()
            .iter()
            .map(EiaFacetMetadata::id)
            .ne(query.facets().iter().map(EiaFacetFilter::facet))
        {
            return Err(EiaError::SchemaDrift);
        }
        for (facet, catalog) in query.facets().iter().zip(&facet_catalogs) {
            if metadata.facet(facet.facet()).is_none()
                || catalog.route() != query.route()
                || catalog.facet() != facet.facet()
                || catalog.api_version() != metadata.api_version()
                || catalog.total_facets() == 0
                || catalog.receipt().retained_payload_digest().bytes() == [0; 32]
                || facet.values().iter().any(|value| !catalog.contains(value))
            {
                return Err(EiaError::SchemaDrift);
            }
        }
        validate_total_sort(&query, &descriptor_fields)?;
        let semantic = serde_json::to_vec(&(
            metadata.route(),
            metadata.api_version(),
            metadata.schema_digest(),
            query.identity(),
            &fields,
            facet_catalogs
                .iter()
                .map(|catalog| {
                    (
                        catalog.facet(),
                        catalog.schema_digest(),
                        catalog.receipt().retained_payload_digest(),
                    )
                })
                .collect::<Vec<_>>(),
            &descriptor_fields,
            &clock_fields,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let schema_digest = digest_parts(b"eia-dataset-contract-v2", [semantic.as_slice()]);
        Ok(Self {
            metadata,
            query,
            fields,
            facet_catalogs,
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

    /// Returns the complete frozen value catalogs aligned to the query facets.
    pub fn facet_catalogs(&self) -> &[EiaFacetCatalog] {
        &self.facet_catalogs
    }

    /// Returns the sealed-discovery catalog for one exact route facet.
    pub fn facet_catalog(&self, facet: &EiaFieldId) -> Option<&EiaFacetCatalog> {
        self.facet_catalogs
            .binary_search_by(|catalog| catalog.facet().cmp(facet))
            .ok()
            .map(|index| &self.facet_catalogs[index])
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

    /// Returns the conservative knowledge clock used by canonical PIT evidence.
    ///
    /// An EIA release/update/availability instant can precede this installation's first receipt,
    /// but it cannot make the exact response knowable locally before that receipt.
    pub const fn conservative_available_at(&self) -> Timestamp {
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

    pub(crate) fn into_canonical_lineage(self) -> (EiaSeriesIdentity, EiaPeriod, EiaNativeValue) {
        (self.series, self.period, self.value)
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct EiaReturnedSortCoordinate {
    value: String,
    direction: EiaSortDirection,
}

/// Exact provider-returned row coordinates in the query's ordered sort directions.
#[derive(Clone, Debug, Eq, PartialEq)]
struct EiaReturnedSortKey {
    coordinates: Arc<[EiaReturnedSortCoordinate]>,
}

impl EiaReturnedSortKey {
    fn try_new(
        contract: &EiaDatasetContract,
        period: &EiaPeriod,
        facets: &[EiaFacetCoordinate],
        descriptors: &[EiaDescriptor],
    ) -> Result<Self, EiaError> {
        let mut coordinates = Vec::new();
        coordinates
            .try_reserve_exact(contract.query().sorts().len())
            .map_err(|_| EiaError::AllocationFailure)?;
        for sort in contract.query().sorts() {
            let value = if sort.column().as_str() == "period" {
                period.raw().to_owned()
            } else if let Some(facet) = facets.iter().find(|facet| facet.facet() == sort.column()) {
                facet.value().as_str().to_owned()
            } else if let Some(descriptor) = descriptors
                .iter()
                .find(|descriptor| descriptor.field() == sort.column())
            {
                descriptor.value().to_owned()
            } else {
                return Err(EiaError::NonTotalSort);
            };
            coordinates.push(EiaReturnedSortCoordinate {
                value,
                direction: sort.direction(),
            });
        }
        if coordinates.is_empty() {
            return Err(EiaError::NonTotalSort);
        }
        Ok(Self {
            coordinates: Arc::from(coordinates),
        })
    }

    fn retained_bytes(&self) -> Result<usize, EiaError> {
        self.coordinates
            .len()
            .checked_mul(size_of::<EiaReturnedSortCoordinate>())
            .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
            .and_then(|bytes| {
                self.coordinates
                    .iter()
                    .try_fold(bytes, |total, coordinate| {
                        total.checked_add(coordinate.value.len())
                    })
            })
            .ok_or(EiaError::InvalidLimit)
    }
}

fn validate_returned_sort_transition(
    previous: &EiaReturnedSortKey,
    next: &EiaReturnedSortKey,
) -> Result<(), EiaError> {
    if previous.coordinates.len() != next.coordinates.len() {
        return Err(EiaError::NonTotalSort);
    }
    for (previous, next) in previous.coordinates.iter().zip(next.coordinates.iter()) {
        if previous.direction != next.direction {
            return Err(EiaError::NonTotalSort);
        }
        match previous.value.cmp(&next.value) {
            Ordering::Equal => {}
            Ordering::Less if previous.direction == EiaSortDirection::Ascending => return Ok(()),
            Ordering::Greater if previous.direction == EiaSortDirection::Descending => {
                return Ok(());
            }
            Ordering::Less | Ordering::Greater => return Err(EiaError::NonTotalSort),
        }
    }
    Err(EiaError::NonTotalSort)
}

fn returned_sort_endpoints_retained_bytes(
    first: Option<&EiaReturnedSortKey>,
    last: Option<&EiaReturnedSortKey>,
) -> Result<usize, EiaError> {
    match (first, last) {
        (None, None) => Ok(0),
        (Some(first), Some(last)) => {
            let first_bytes = first.retained_bytes()?;
            if Arc::ptr_eq(&first.coordinates, &last.coordinates) {
                Ok(first_bytes)
            } else {
                first_bytes
                    .checked_add(last.retained_bytes()?)
                    .ok_or(EiaError::InvalidLimit)
            }
        }
        _ => Err(EiaError::NonTotalSort),
    }
}

/// One secret-free request/page/returned-row receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDataPageReceipt {
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
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
    publication_retained_bytes: usize,
    received_at: Timestamp,
    completeness: EiaPageCompleteness,
    redacted_secret_fields: usize,
}

impl EiaDataPageReceipt {
    /// Returns base-query identity.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns the exact frozen native route/query schema identity.
    pub const fn contract_schema_digest(&self) -> EiaDigest {
        self.contract_schema_digest
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

    /// Returns checked native/raw bytes charged to the terminal publication working set.
    pub const fn publication_retained_bytes(&self) -> usize {
        self.publication_retained_bytes
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
    description: Option<String>,
    observations: Vec<EiaObservation>,
    first_sort_key: Option<EiaReturnedSortKey>,
    last_sort_key: Option<EiaReturnedSortKey>,
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
        Self::parse_with_publication_budget(
            bytes,
            request,
            contract,
            received_at,
            limits,
            MAX_OBSERVED_REVISION_BATCH_BYTES,
        )
    }

    pub(crate) fn parse_with_publication_budget(
        bytes: &[u8],
        request: EiaDataPageRequest<'_>,
        contract: &EiaDatasetContract,
        received_at: Timestamp,
        limits: EiaParseLimits,
        remaining_publication_bytes: usize,
    ) -> Result<Self, EiaError> {
        if remaining_publication_bytes == 0
            || remaining_publication_bytes > MAX_OBSERVED_REVISION_BATCH_BYTES
        {
            return Err(EiaError::InvalidLimit);
        }
        if request.query() != contract.query() {
            return Err(EiaError::RequestEchoMismatch);
        }
        let secret_free = request.secret_free()?;
        let expected_command = format!("/v2/{}/data", contract.query().route());
        let envelope = parse_envelope(
            bytes,
            &expected_command,
            &request.expected_echo_params(),
            limits,
        )?;
        if &envelope.api_version != contract.metadata().api_version() {
            return Err(EiaError::ApiVersionDrift);
        }
        let mut response = envelope.response;
        const RESPONSE_FIELDS: &[&str] =
            &["total", "dateFormat", "frequency", "description", "data"];
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
        validate_publication_cardinality(total, contract.fields().len())?;
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
        let description = response
            .remove("description")
            .map(|value| parse_bounded_string(&value, limits))
            .transpose()?;
        let rows = match response.remove("data") {
            Some(Value::Array(rows)) => rows,
            Some(_) | None => return Err(EiaError::InvalidProtocol),
        };
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
        let page_observations = rows
            .len()
            .checked_mul(contract.fields().len())
            .ok_or(EiaError::InvalidLimit)?;
        let mut publication_budget = PublicationByteBudget::try_new(
            remaining_publication_bytes,
            envelope
                .retained_payload
                .len()
                .checked_add(size_of::<EiaDataPage>())
                .and_then(|bytes| bytes.checked_add(envelope.api_version.as_str().len()))
                .and_then(|bytes| bytes.checked_add(frequency.as_str().len()))
                .and_then(|bytes| bytes.checked_add(date_format.len()))
                .and_then(|bytes| bytes.checked_add(description.as_ref().map_or(0, String::len)))
                .and_then(|bytes| {
                    bytes.checked_add(
                        page_observations
                            .checked_mul(size_of::<EiaObservation>())?
                            .checked_mul(2)?,
                    )
                })
                .ok_or(EiaError::InvalidLimit)?,
        )?;
        observations
            .try_reserve_exact(page_observations)
            .map_err(|_| EiaError::AllocationFailure)?;
        let mut families = BTreeMap::new();
        let mut first_sort_key = None;
        let mut last_sort_key = None;
        let row_context = RowParseContext {
            contract,
            received_at,
            page_payload_digest,
            row_schema_digest,
            limits,
        };
        for row in rows {
            let object = row.as_object().ok_or(EiaError::InvalidProtocol)?;
            let sort_key = parse_row(
                object,
                &row_context,
                &mut observations,
                &mut families,
                &mut publication_budget,
            )?;
            if let Some(previous) = last_sort_key.as_ref() {
                validate_returned_sort_transition(previous, &sort_key)?;
            } else {
                first_sort_key = Some(sort_key.clone());
            }
            last_sort_key = Some(sort_key);
        }
        if returned_rows == 0 {
            if first_sort_key.is_some() || last_sort_key.is_some() {
                return Err(EiaError::NonTotalSort);
            }
        } else if first_sort_key.is_none() || last_sort_key.is_none() {
            return Err(EiaError::NonTotalSort);
        }
        publication_budget.charge(returned_sort_endpoints_retained_bytes(
            first_sort_key.as_ref(),
            last_sort_key.as_ref(),
        )?)?;
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
            contract_schema_digest: contract.schema_digest(),
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
            publication_retained_bytes: publication_budget.retained(),
            received_at,
            completeness,
            redacted_secret_fields: envelope.redacted_secret_fields,
        };
        Ok(Self {
            api_version: envelope.api_version,
            frequency,
            date_format,
            description,
            observations,
            first_sort_key,
            last_sort_key,
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

    /// Returns the provider's bounded route description when supplied on the data surface.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
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
    contract_schema_digest: EiaDigest,
    api_version: EiaApiVersion,
    row_schema_digest: EiaDigest,
    frequency: EiaFieldId,
    date_format: String,
    description: Option<String>,
    total: u64,
    next_offset: u64,
    page_count: u32,
    returned_rows: u64,
    observation_count: u64,
    missing_observation_count: u64,
    response_bytes: u64,
    publication_retained_bytes: usize,
    first_received_at: Option<Timestamp>,
    last_received_at: Option<Timestamp>,
    last_sort_key: Option<EiaReturnedSortKey>,
    closed: bool,
    seen_page_digests: BTreeSet<EiaDigest>,
    ordered_page_digests: Vec<EiaDigest>,
    families: BTreeMap<EiaObservationFamily, EiaDigest>,
}

impl EiaPaginationTracker {
    /// Starts from an exact first page at offset zero.
    pub fn start(page: &EiaDataPage) -> Result<Self, EiaError> {
        if page.receipt.offset != 0 {
            return Err(EiaError::Pagination);
        }
        let mut tracker = Self {
            query_digest: page.receipt.query_digest,
            contract_schema_digest: page.receipt.contract_schema_digest,
            api_version: page.api_version.clone(),
            row_schema_digest: page.receipt.row_schema_digest,
            frequency: page.frequency.clone(),
            date_format: page.date_format.clone(),
            description: page.description.clone(),
            total: page.receipt.total,
            next_offset: 0,
            page_count: 0,
            returned_rows: 0,
            observation_count: 0,
            missing_observation_count: 0,
            response_bytes: 0,
            publication_retained_bytes: 0,
            first_received_at: None,
            last_received_at: None,
            last_sort_key: None,
            closed: false,
            seen_page_digests: BTreeSet::new(),
            ordered_page_digests: Vec::new(),
            families: BTreeMap::new(),
        };
        tracker.push(page)?;
        Ok(tracker)
    }

    /// Admits exactly the next offset page and rejects total/version/schema/replay drift.
    pub fn push(&mut self, page: &EiaDataPage) -> Result<(), EiaError> {
        if self.closed
            || page.receipt.query_digest != self.query_digest
            || page.receipt.contract_schema_digest != self.contract_schema_digest
            || page.api_version != self.api_version
            || page.receipt.row_schema_digest != self.row_schema_digest
            || page.frequency != self.frequency
            || page.date_format != self.date_format
            || page.description != self.description
            || page.receipt.total != self.total
            || page.receipt.offset != self.next_offset
            || self
                .seen_page_digests
                .contains(&page.receipt.retained_payload_digest)
            || self
                .last_received_at
                .is_some_and(|received_at| page.receipt.received_at < received_at)
        {
            return Err(EiaError::Pagination);
        }
        // Keep each physical, JSON-kind-sensitive envelope digest in its page receipt. It is not
        // a cross-page semantic invariant: the frozen contract legitimately admits null missing
        // values and decimal string/number variants. Every page has already proved exact response
        // and row key sets plus the declared value/missing variants before reaching this tracker.
        if page.receipt.returned_rows == 0 {
            if page.first_sort_key.is_some() || page.last_sort_key.is_some() {
                return Err(EiaError::NonTotalSort);
            }
        } else {
            let first = page.first_sort_key.as_ref().ok_or(EiaError::NonTotalSort)?;
            let last = page.last_sort_key.as_ref().ok_or(EiaError::NonTotalSort)?;
            if let Some(previous) = self.last_sort_key.as_ref() {
                validate_returned_sort_transition(previous, first)?;
            }
            if page.receipt.returned_rows == 1 && first != last {
                return Err(EiaError::NonTotalSort);
            }
        }
        for observation in &page.observations {
            if let Some(existing) = self.families.get(&observation.family) {
                return Err(if *existing == observation.semantic_digest {
                    EiaError::ObservationReplay
                } else {
                    EiaError::ObservationConflict
                });
            }
        }
        for observation in &page.observations {
            if self
                .families
                .insert(observation.family.clone(), observation.semantic_digest)
                .is_some()
            {
                return Err(EiaError::ObservationConflict);
            }
        }
        if !self
            .seen_page_digests
            .insert(page.receipt.retained_payload_digest)
        {
            return Err(EiaError::Pagination);
        }
        self.ordered_page_digests
            .push(page.receipt.retained_payload_digest);
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
        self.publication_retained_bytes = self
            .publication_retained_bytes
            .checked_add(page.receipt.publication_retained_bytes)
            .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
            .ok_or(EiaError::InvalidLimit)?;
        if self.first_received_at.is_none() {
            self.first_received_at = Some(page.receipt.received_at);
        }
        self.last_received_at = Some(page.receipt.received_at);
        self.last_sort_key = page.last_sort_key.clone();
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

    /// Returns the exact number of pages already admitted by this in-memory verifier.
    pub(crate) const fn admitted_page_count(&self) -> u32 {
        self.page_count
    }

    pub(crate) const fn publication_retained_bytes(&self) -> usize {
        self.publication_retained_bytes
    }

    /// Adds actual root-rejoin/seal lineage retained beside the already charged typed page.
    pub(crate) fn charge_publication_retained_bytes(
        &mut self,
        retained_bytes: usize,
    ) -> Result<(), EiaError> {
        self.publication_retained_bytes = self
            .publication_retained_bytes
            .checked_add(retained_bytes)
            .filter(|bytes| *bytes <= MAX_OBSERVED_REVISION_BATCH_BYTES)
            .ok_or(EiaError::InvalidLimit)?;
        Ok(())
    }

    /// Finishes only after the exact returned-row total closes.
    pub fn finish(self) -> Result<EiaAcquisitionReceipt, EiaError> {
        self.finish_with_families().map(|(receipt, _)| receipt)
    }

    fn finish_with_families(
        self,
    ) -> Result<
        (
            EiaAcquisitionReceipt,
            BTreeMap<EiaObservationFamily, EiaDigest>,
        ),
        EiaError,
    > {
        if !self.closed || self.returned_rows != self.total {
            return Err(EiaError::Pagination);
        }
        if self.families.len()
            != usize::try_from(self.observation_count).map_err(|_| EiaError::InvalidLimit)?
        {
            return Err(EiaError::ObservationConflict);
        }
        let page_digests = self.ordered_page_digests;
        let digest_material = serde_json::to_vec(&(
            self.query_digest,
            self.contract_schema_digest,
            &self.api_version,
            self.total,
            self.page_count,
            self.returned_rows,
            self.observation_count,
            self.missing_observation_count,
            self.response_bytes,
            self.publication_retained_bytes,
            self.first_received_at,
            self.last_received_at,
            &page_digests,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let receipt = EiaAcquisitionReceipt {
            query_digest: self.query_digest,
            contract_schema_digest: self.contract_schema_digest,
            api_version: self.api_version,
            total: self.total,
            page_count: self.page_count,
            returned_rows: self.returned_rows,
            observation_count: self.observation_count,
            missing_observation_count: self.missing_observation_count,
            response_bytes: self.response_bytes,
            publication_retained_bytes: self.publication_retained_bytes,
            first_received_at: self.first_received_at.ok_or(EiaError::InvalidClock)?,
            last_received_at: self.last_received_at.ok_or(EiaError::InvalidClock)?,
            page_digests,
            content_digest: digest_parts(
                b"eia-acquisition-receipt-v2",
                [digest_material.as_slice()],
            ),
        };
        Ok((receipt, self.families))
    }
}

/// Complete page-chain receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaAcquisitionReceipt {
    query_digest: EiaDigest,
    contract_schema_digest: EiaDigest,
    api_version: EiaApiVersion,
    total: u64,
    page_count: u32,
    returned_rows: u64,
    observation_count: u64,
    missing_observation_count: u64,
    response_bytes: u64,
    publication_retained_bytes: usize,
    first_received_at: Timestamp,
    last_received_at: Timestamp,
    page_digests: Vec<EiaDigest>,
    content_digest: EiaDigest,
}

impl EiaAcquisitionReceipt {
    /// Returns the exact frozen query identity.
    pub const fn query_digest(&self) -> EiaDigest {
        self.query_digest
    }

    /// Returns the exact frozen native route/query schema identity.
    pub const fn contract_schema_digest(&self) -> EiaDigest {
        self.contract_schema_digest
    }

    /// Returns the exact API version shared by every admitted page.
    pub const fn api_version(&self) -> &EiaApiVersion {
        &self.api_version
    }

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

    /// Returns the checked native/raw publication working-set charge for the complete chain.
    pub const fn publication_retained_bytes(&self) -> usize {
        self.publication_retained_bytes
    }

    /// Returns the first admitted page receipt clock.
    pub const fn first_received_at(&self) -> Timestamp {
        self.first_received_at
    }

    /// Returns the last admitted page receipt clock after nondecreasing-chain validation.
    pub const fn last_received_at(&self) -> Timestamp {
        self.last_received_at
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
        Self::try_from_tracked_pages(pages, tracker)
    }

    /// Consumes pages already admitted in the same order by the supplied linear verifier.
    pub(crate) fn try_from_tracked_pages(
        pages: Vec<EiaDataPage>,
        tracker: EiaPaginationTracker,
    ) -> Result<Self, EiaError> {
        let (receipt, mut families) = tracker.finish_with_families()?;
        if pages.len() != usize::try_from(receipt.page_count).map_err(|_| EiaError::InvalidLimit)?
            || pages
                .iter()
                .zip(&receipt.page_digests)
                .any(|(page, digest)| page.receipt.retained_payload_digest != *digest)
        {
            return Err(EiaError::Pagination);
        }
        let mut observations = Vec::new();
        let observation_count =
            usize::try_from(receipt.observation_count).map_err(|_| EiaError::InvalidLimit)?;
        if observation_count > EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS {
            return Err(EiaError::InvalidLimit);
        }
        observations
            .try_reserve_exact(observation_count)
            .map_err(|_| EiaError::AllocationFailure)?;
        for page in pages {
            for observation in page.observations {
                match families.remove(&observation.family) {
                    Some(expected) if expected == observation.semantic_digest => {}
                    Some(_) => return Err(EiaError::ObservationConflict),
                    None => return Err(EiaError::ObservationReplay),
                }
                observations.push(observation);
            }
        }
        if !families.is_empty() {
            return Err(EiaError::ObservationConflict);
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

    pub(crate) fn into_parts(self) -> (Vec<EiaObservation>, EiaAcquisitionReceipt) {
        (self.observations, self.receipt)
    }
}

struct RowParseContext<'a> {
    contract: &'a EiaDatasetContract,
    received_at: Timestamp,
    page_payload_digest: EiaDigest,
    row_schema_digest: EiaDigest,
    limits: EiaParseLimits,
}

struct PublicationByteBudget {
    retained: usize,
    limit: usize,
}

impl PublicationByteBudget {
    fn try_new(limit: usize, initial: usize) -> Result<Self, EiaError> {
        if initial > limit {
            return Err(EiaError::InvalidLimit);
        }
        Ok(Self {
            retained: initial,
            limit,
        })
    }

    fn charge(&mut self, bytes: usize) -> Result<(), EiaError> {
        self.retained = self
            .retained
            .checked_add(bytes)
            .filter(|retained| *retained <= self.limit)
            .ok_or(EiaError::InvalidLimit)?;
        Ok(())
    }

    const fn retained(&self) -> usize {
        self.retained
    }
}

fn parse_row(
    row: &Map<String, Value>,
    context: &RowParseContext<'_>,
    observations: &mut Vec<EiaObservation>,
    families: &mut BTreeMap<EiaObservationFamily, EiaDigest>,
    publication_budget: &mut PublicationByteBudget,
) -> Result<EiaReturnedSortKey, EiaError> {
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
            if !facet.values().contains(&value)
                || !contract
                    .facet_catalog(facet.facet())
                    .is_some_and(|catalog| catalog.contains(&value))
            {
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
    let sort_key = EiaReturnedSortKey::try_new(contract, &period, &facets, &descriptors)?;
    publication_budget.charge(row_parse_scratch_bytes(&period, &facets, &descriptors)?)?;
    let row_series_material = serde_json::to_vec(&(
        contract.query().route(),
        contract.query().frequency(),
        &facets,
        &descriptors,
    ))
    .map_err(|_| EiaError::InvalidJson)?;
    let row_series_coordinates_digest = digest_parts(
        b"eia-row-series-coordinates-v2",
        [row_series_material.as_slice()],
    );
    for field in contract.fields() {
        let value = parse_value(
            row.get(field.field().as_str())
                .ok_or(EiaError::SchemaDrift)?,
            field,
            limits,
        )?;
        let unit = parse_unit(row, field, contract.metadata(), limits)?;
        publication_budget.charge(native_observation_retained_bytes(
            contract,
            field,
            &period,
            &facets,
            &descriptors,
            &value,
            &unit,
        )?)?;
        let series_digest_material =
            serde_json::to_vec(&(row_series_coordinates_digest, field.field(), &unit))
                .map_err(|_| EiaError::InvalidJson)?;
        let series_digest = digest_parts(
            b"eia-series-identity-v2",
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
            series.digest,
            &value,
            clocks.released_at,
            clocks.updated_at,
            clocks.available_at,
        ))
        .map_err(|_| EiaError::InvalidJson)?;
        let semantic_digest = digest_parts(
            b"eia-native-observation-semantic-v2",
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
            return Err(if existing == semantic_digest {
                EiaError::ObservationReplay
            } else {
                EiaError::ObservationConflict
            });
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
    Ok(sort_key)
}

fn row_parse_scratch_bytes(
    period: &EiaPeriod,
    facets: &[EiaFacetCoordinate],
    descriptors: &[EiaDescriptor],
) -> Result<usize, EiaError> {
    period_retained_bytes(period)?
        .checked_add(facet_coordinates_retained_bytes(facets)?)
        .and_then(|bytes| bytes.checked_add(descriptor_retained_bytes(descriptors).ok()?))
        .ok_or(EiaError::InvalidLimit)
}

fn native_observation_retained_bytes(
    contract: &EiaDatasetContract,
    field: &EiaDataFieldContract,
    period: &EiaPeriod,
    facets: &[EiaFacetCoordinate],
    descriptors: &[EiaDescriptor],
    value: &EiaNativeValue,
    unit: &str,
) -> Result<usize, EiaError> {
    let period_bytes = period_retained_bytes(period)?;
    let series_bytes = contract
        .query()
        .route()
        .as_str()
        .len()
        .checked_add(field.field().as_str().len())
        .and_then(|bytes| bytes.checked_add(contract.query().frequency().as_str().len()))
        .and_then(|bytes| bytes.checked_add(unit.len()))
        .and_then(|bytes| bytes.checked_add(facet_coordinates_retained_bytes(facets).ok()?))
        .and_then(|bytes| bytes.checked_add(descriptor_retained_bytes(descriptors).ok()?))
        .ok_or(EiaError::InvalidLimit)?;
    let value_bytes = match value {
        EiaNativeValue::Decimal { lexical, .. } | EiaNativeValue::String(lexical) => lexical.len(),
        EiaNativeValue::Missing(missing) => missing.lexical.as_ref().map_or(0, String::len),
    };
    // Two periods are retained by the observation/family and a third is held by the exact-family
    // conflict verifier until the page closes. The fixed observation slot was charged before any
    // observation allocation; this adds only actual owned payload plus conservative map scratch.
    period_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(series_bytes))
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .and_then(|bytes| {
            bytes.checked_add(
                size_of::<EiaObservationFamily>()
                    .checked_add(size_of::<EiaDigest>())?
                    .checked_add(4 * size_of::<usize>())?,
            )
        })
        .ok_or(EiaError::InvalidLimit)
}

fn period_retained_bytes(period: &EiaPeriod) -> Result<usize, EiaError> {
    period
        .raw
        .len()
        .checked_add(period.format.len())
        .and_then(|bytes| bytes.checked_add(period.frequency.as_str().len()))
        .ok_or(EiaError::InvalidLimit)
}

fn facet_coordinates_retained_bytes(facets: &[EiaFacetCoordinate]) -> Result<usize, EiaError> {
    let mut bytes = facets
        .len()
        .checked_mul(size_of::<EiaFacetCoordinate>())
        .ok_or(EiaError::InvalidLimit)?;
    for facet in facets {
        bytes = bytes
            .checked_add(facet.facet.as_str().len())
            .and_then(|value| value.checked_add(facet.value.as_str().len()))
            .ok_or(EiaError::InvalidLimit)?;
    }
    Ok(bytes)
}

fn descriptor_retained_bytes(descriptors: &[EiaDescriptor]) -> Result<usize, EiaError> {
    let mut bytes = descriptors
        .len()
        .checked_mul(size_of::<EiaDescriptor>())
        .ok_or(EiaError::InvalidLimit)?;
    for descriptor in descriptors {
        bytes = bytes
            .checked_add(descriptor.field.as_str().len())
            .and_then(|value| value.checked_add(descriptor.value.len()))
            .ok_or(EiaError::InvalidLimit)?;
    }
    Ok(bytes)
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
        "YYYY-Q" | "YYYY-Q#" | "YYYY-\"Q\"Q" => {
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
        || matches!((updated_at, available_at), (Some(updated), Some(available)) if available < updated)
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

fn validate_total_sort(
    query: &EiaDataQuery,
    descriptor_fields: &[EiaFieldId],
) -> Result<(), EiaError> {
    let period = EiaFieldId::try_from("period")?;
    let mut required = BTreeSet::new();
    required.insert(period);
    for coordinate in query
        .facets()
        .iter()
        .map(|facet| facet.facet())
        .chain(descriptor_fields)
    {
        if !required.insert(coordinate.clone()) {
            return Err(EiaError::NonTotalSort);
        }
    }
    let actual = query
        .sorts()
        .iter()
        .map(|sort| sort.column().clone())
        .collect::<BTreeSet<_>>();
    if actual != required || query.sorts().len() != required.len() {
        return Err(EiaError::NonTotalSort);
    }
    Ok(())
}

pub(crate) fn validate_publication_cardinality(
    total_rows: u64,
    selected_fields: usize,
) -> Result<usize, EiaError> {
    let rows = usize::try_from(total_rows).map_err(|_| EiaError::InvalidLimit)?;
    let observations = rows
        .checked_mul(selected_fields)
        .ok_or(EiaError::InvalidLimit)?;
    if observations > EIA_MAX_CANONICAL_PUBLICATION_OBSERVATIONS {
        return Err(EiaError::InvalidLimit);
    }
    Ok(observations)
}

fn validate_retained_string(value: &str) -> Result<(), EiaError> {
    if value.is_empty() || value.len() > 32 * 1024 || value.chars().any(char::is_control) {
        Err(EiaError::InvalidIdentifier)
    } else {
        Ok(())
    }
}
