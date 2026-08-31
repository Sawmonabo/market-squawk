use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::mem::size_of;

use market_squawk_domain::{AvailabilityEvidence, CalendarDate, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use serde::de::{DeserializeSeed, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::CensusGeographyAdmission;
use crate::discovery::{CensusPredicateType, CensusVariableCatalog, CensusVariableMetadata};
use crate::query::{
    CensusDataQuery, CensusGeography, CensusGeographyCode, CensusSelection, CensusTimePoint,
    CensusTimePredicate,
};
use crate::{CensusAdapterError, sha256, update_digest_component};

/// Absolute raw + decoded + canonical-output memory ceiling for one Census operation.
pub const CENSUS_OPERATION_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const CENSUS_MAX_SINGLE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const CENSUS_MAX_DECODED_CELLS: usize = 64 * 1024;

/// Bounded Census JSON parser limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CensusParseLimits {
    max_bytes: usize,
    max_rows: usize,
    max_columns: usize,
    max_cells: usize,
    max_metadata_entries: usize,
    max_string_bytes: usize,
}

impl CensusParseLimits {
    /// Constructs application-owned parser bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero bounds or a cell ceiling smaller than the row/column ceilings.
    pub fn try_new(
        max_bytes: usize,
        max_rows: usize,
        max_columns: usize,
        max_cells: usize,
        max_metadata_entries: usize,
        max_string_bytes: usize,
    ) -> Result<Self, CensusAdapterError> {
        if [
            max_bytes,
            max_rows,
            max_columns,
            max_cells,
            max_metadata_entries,
            max_string_bytes,
        ]
        .contains(&0)
            || max_cells < max_rows
            || max_cells < max_columns
            || max_bytes > CENSUS_MAX_SINGLE_RESPONSE_BYTES
            || max_cells > CENSUS_MAX_DECODED_CELLS
            || max_string_bytes > max_bytes
        {
            return Err(CensusAdapterError::InvalidQuery);
        }
        Ok(Self {
            max_bytes,
            max_rows,
            max_columns,
            max_cells,
            max_metadata_entries,
            max_string_bytes,
        })
    }

    /// Returns the exact body-byte ceiling.
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the data-row ceiling, excluding the header.
    pub const fn max_rows(self) -> usize {
        self.max_rows
    }

    /// Returns the response-column ceiling.
    pub const fn max_columns(self) -> usize {
        self.max_columns
    }

    /// Returns the total data-cell ceiling.
    pub const fn max_cells(self) -> usize {
        self.max_cells
    }

    /// Returns the discovery-entry ceiling.
    pub const fn max_metadata_entries(self) -> usize {
        self.max_metadata_entries
    }

    /// Returns the per-string byte ceiling.
    pub const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }
}

impl Default for CensusParseLimits {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_rows: 32_768,
            max_columns: 2_048,
            max_cells: CENSUS_MAX_DECODED_CELLS,
            max_metadata_entries: 65_536,
            max_string_bytes: 32 * 1024,
        }
    }
}

/// Local receipt, decode, ingestion, and conservative availability clocks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusClocks {
    received_at: Timestamp,
    decoded_at: Timestamp,
    ingested_at: Timestamp,
    availability: AvailabilityEvidence,
}

impl CensusClocks {
    /// Constructs local chronology without inventing a provider publication instant.
    ///
    /// # Errors
    ///
    /// Rejects reversed local chronology or availability later than local ingestion.
    pub fn try_new(
        received_at: Timestamp,
        decoded_at: Timestamp,
        ingested_at: Timestamp,
        availability: AvailabilityEvidence,
    ) -> Result<Self, CensusAdapterError> {
        if received_at > decoded_at
            || decoded_at > ingested_at
            || availability
                .reported_at()
                .is_some_and(|available_at| available_at > ingested_at)
        {
            return Err(CensusAdapterError::InvalidChronology);
        }
        Ok(Self {
            received_at,
            decoded_at,
            ingested_at,
            availability,
        })
    }

    /// Constructs the normal conservative first-local-observation chronology.
    pub fn local_first_observed(
        received_at: Timestamp,
        decoded_at: Timestamp,
        ingested_at: Timestamp,
    ) -> Result<Self, CensusAdapterError> {
        Self::try_new(
            received_at,
            decoded_at,
            ingested_at,
            AvailabilityEvidence::local_first_observed(received_at),
        )
    }

    /// Returns when the complete response reached this process.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns when bounded provider-native decoding completed.
    pub const fn decoded_at(&self) -> Timestamp {
        self.decoded_at
    }

    /// Returns when the decoded native records crossed the local ingestion boundary.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns conservative point-in-time availability evidence.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }
}

/// A provider-reported observation period without invented timestamp precision.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "precision")]
pub enum CensusReportedTime {
    /// Four-digit year.
    Year { year: u16 },
    /// Calendar month.
    Month { year: u16, month: u8 },
    /// Calendar quarter.
    Quarter { year: u16, quarter: u8 },
    /// Exact civil date supplied by a dataset.
    CalendarDate { date: CalendarDate },
    /// Dataset-specific bounded provider period retained for later admitted mapping.
    ProviderPeriod { value: SourceIdentifier },
}

impl CensusReportedTime {
    fn parse(value: &str) -> Result<Self, CensusAdapterError> {
        if let Ok(year) = value.parse::<u16>()
            && (1000..=9999).contains(&year)
        {
            return Ok(Self::Year { year });
        }
        if let Some(date) = parse_calendar_date(value)? {
            return Ok(Self::CalendarDate { date });
        }
        let value_bytes = value.as_bytes();
        if value_bytes.len() == 7
            && value_bytes[4] == b'-'
            && value_bytes[5] == b'Q'
            && value_bytes[..4].iter().all(u8::is_ascii_digit)
            && value_bytes[6].is_ascii_digit()
        {
            let year = std::str::from_utf8(&value_bytes[..4])
                .map_err(|_| CensusAdapterError::InvalidComponent)?
                .parse::<u16>()
                .map_err(|_| CensusAdapterError::InvalidComponent)?;
            let quarter = std::str::from_utf8(&value_bytes[6..])
                .map_err(|_| CensusAdapterError::InvalidComponent)?
                .parse::<u8>()
                .map_err(|_| CensusAdapterError::InvalidComponent)?;
            if (1000..=9999).contains(&year) && (1..=4).contains(&quarter) {
                return Ok(Self::Quarter { year, quarter });
            }
        }
        if value_bytes.len() == 7
            && value_bytes[4] == b'-'
            && value_bytes[..4].iter().all(u8::is_ascii_digit)
            && value_bytes[5..].iter().all(u8::is_ascii_digit)
        {
            let year = std::str::from_utf8(&value_bytes[..4])
                .map_err(|_| CensusAdapterError::InvalidComponent)?
                .parse::<u16>()
                .map_err(|_| CensusAdapterError::InvalidComponent)?;
            let month = std::str::from_utf8(&value_bytes[5..])
                .map_err(|_| CensusAdapterError::InvalidComponent)?
                .parse::<u8>()
                .map_err(|_| CensusAdapterError::InvalidComponent)?;
            if (1000..=9999).contains(&year) && (1..=12).contains(&month) {
                return Ok(Self::Month { year, month });
            }
        }
        Ok(Self::ProviderPeriod {
            value: SourceIdentifier::try_from(value)
                .map_err(|_| CensusAdapterError::InvalidComponent)?,
        })
    }
}

/// One provider-native exact value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum CensusTypedValue {
    /// Exact integer.
    Integer(i128),
    /// Exact base-10 decimal.
    Decimal(Decimal),
    /// Provider text.
    Text(String),
    /// Provider Boolean.
    Boolean(bool),
}

/// One metadata-declared annotation or flag cell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusAnnotation {
    variable: SourceIdentifier,
    raw: String,
}

impl CensusAnnotation {
    /// Returns the annotation variable.
    pub const fn variable(&self) -> &SourceIdentifier {
        &self.variable
    }

    /// Returns the exact provider text.
    pub fn raw(&self) -> &str {
        &self.raw
    }
}

/// Exact missing-value evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusMissingReason {
    /// Provider JSON null.
    JsonNull,
    /// Provider empty string.
    EmptyString,
    /// Provider annotation accompanies an absent primary value.
    ProviderAnnotatedMissing,
    /// A metadata-declared annotation column was absent from the response contract.
    AnnotationColumnMissing,
}

/// One primary value with closed missing, annotation, and invalid states.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum CensusValueState {
    /// A typed value with every requested metadata annotation column present and empty.
    Observed { value: CensusTypedValue },
    /// A provider value accompanied by one or more nonempty annotations. Consumers must apply a
    /// dataset-specific annotation policy before treating the optional typed candidate as data.
    Annotated {
        /// Exact source cell text.
        raw: String,
        /// Type-compatible candidate, never silently promoted while annotations remain.
        typed_candidate: Option<CensusTypedValue>,
        /// Exact annotation cells.
        annotations: Vec<CensusAnnotation>,
    },
    /// Provider-native absence.
    Missing {
        /// Exact missing disposition.
        reason: CensusMissingReason,
        /// Any provider annotations accompanying the absence.
        annotations: Vec<CensusAnnotation>,
    },
    /// A present source value did not match metadata-declared typing.
    Invalid {
        /// Exact source cell text.
        raw: String,
        /// Metadata predicate/value type.
        expected: CensusPredicateType,
        /// Any provider annotations accompanying the invalid cell.
        annotations: Vec<CensusAnnotation>,
    },
}

/// One retained non-geographic request predicate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusPredicateValue {
    variable: SourceIdentifier,
    predicate_type: CensusPredicateType,
    values: Vec<String>,
}

impl CensusPredicateValue {
    /// Returns the predicate variable.
    pub const fn variable(&self) -> &SourceIdentifier {
        &self.variable
    }

    /// Returns metadata-declared predicate typing.
    pub const fn predicate_type(&self) -> &CensusPredicateType {
        &self.predicate_type
    }

    /// Returns repeated query values.
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

/// One exact standard geography component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CensusGeographyComponent {
    level: String,
    code: String,
}

impl CensusGeographyComponent {
    /// Returns the provider geography level.
    pub fn level(&self) -> &str {
        &self.level
    }

    /// Returns the exact FIPS/other provider code as text, preserving leading zeros.
    pub fn code(&self) -> &str {
        &self.code
    }
}

/// Semantic scope of one safely identified provider geography.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusGeographyScope {
    /// One exact aggregate coordinate.
    Aggregate,
    /// One member of an exact multi-geography detail request.
    Detail,
    /// Provider result cardinality cannot be proven from the request grammar.
    Unknown,
}

/// Exact provider geography attached to one response row.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusGeographyValue {
    /// Standard `for`/`in` hierarchy.
    Standard {
        /// Request-derived semantic scope.
        scope: CensusGeographyScope,
        /// Ordered containing/output coordinates.
        components: Vec<CensusGeographyComponent>,
        /// Fully qualified GEOID when explicitly returned.
        fully_qualified_geoid: Option<String>,
        /// Provider geography name when explicitly returned.
        name: Option<String>,
    },
    /// UCGID result identified by a returned fully qualified GEOID.
    Uniform {
        /// Request-derived semantic scope.
        scope: CensusGeographyScope,
        /// Exact fully qualified provider GEOID.
        fully_qualified_geoid: String,
        /// Provider geography name when explicitly returned.
        name: Option<String>,
    },
}

impl CensusGeographyValue {
    /// Returns the request-derived aggregate/detail/unknown state.
    pub const fn scope(&self) -> CensusGeographyScope {
        match self {
            Self::Standard { scope, .. } | Self::Uniform { scope, .. } => *scope,
        }
    }

    /// Returns a stable identity digest that excludes mutable display names and optional labels.
    pub fn identity_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        update_digest_component(&mut digest, b"market-squawk/census-geography-identity/v1");
        let scope_tag = match self.scope() {
            CensusGeographyScope::Aggregate => b"aggregate".as_slice(),
            CensusGeographyScope::Detail => b"detail".as_slice(),
            CensusGeographyScope::Unknown => b"unknown".as_slice(),
        };
        match self {
            Self::Standard { components, .. } => {
                update_digest_component(&mut digest, b"standard");
                update_digest_component(&mut digest, scope_tag);
                for component in components {
                    update_digest_component(&mut digest, component.level.as_bytes());
                    update_digest_component(&mut digest, component.code.as_bytes());
                }
            }
            Self::Uniform {
                fully_qualified_geoid,
                ..
            } => {
                update_digest_component(&mut digest, b"uniform");
                update_digest_component(&mut digest, scope_tag);
                update_digest_component(&mut digest, fully_qualified_geoid.as_bytes());
            }
        }
        digest.finalize().into()
    }

    /// Returns the stable provider row geography used by canonical analytical series identity.
    ///
    /// Aggregate/detail scope is a property of the request shape, not of the returned provider
    /// coordinate. It remains in native/provenance evidence through [`Self::identity_digest`] but
    /// is deliberately excluded here so the same row joins across differently batched requests.
    pub fn canonical_row_identity_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        update_digest_component(
            &mut digest,
            b"market-squawk/census-canonical-row-geography/v1",
        );
        match self {
            Self::Standard { components, .. } => {
                update_digest_component(&mut digest, b"standard");
                for component in components {
                    update_digest_component(&mut digest, component.level.as_bytes());
                    update_digest_component(&mut digest, component.code.as_bytes());
                }
            }
            Self::Uniform {
                fully_qualified_geoid,
                ..
            } => {
                update_digest_component(&mut digest, b"uniform");
                update_digest_component(&mut digest, fully_qualified_geoid.as_bytes());
            }
        }
        digest.finalize().into()
    }
}

/// A locally observed source-revision candidate.
///
/// This is not a canonical revision number. The shared observed-revision authority assigns and
/// publishes the durable one-based revision after comparing this family/content evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusRevisionCandidate {
    family_digest: [u8; 32],
    content_digest: [u8; 32],
    first_observed_at: Timestamp,
}

impl CensusRevisionCandidate {
    /// Returns the stable source + variable + geography + predicate + time family identity.
    pub const fn family_digest(&self) -> [u8; 32] {
        self.family_digest
    }

    /// Returns exact typed value + annotation + metadata content identity.
    pub const fn content_digest(&self) -> [u8; 32] {
        self.content_digest
    }

    /// Returns the first-local-observation time for this response.
    pub const fn first_observed_at(&self) -> Timestamp {
        self.first_observed_at
    }
}

/// One closed provider-native Census macro/reference observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusObservation {
    dataset: crate::CensusDataset,
    row_number: usize,
    variable: SourceIdentifier,
    label: String,
    concept: Option<String>,
    group: Option<SourceIdentifier>,
    value: CensusValueState,
    geography: CensusGeographyValue,
    predicates: Vec<CensusPredicateValue>,
    reported_time: Option<CensusReportedTime>,
    request_digest: [u8; 32],
    response_payload_digest: [u8; 32],
    metadata_payload_digest: [u8; 32],
    row_digest: [u8; 32],
    clocks: CensusClocks,
    revision: CensusRevisionCandidate,
}

impl CensusObservation {
    /// Returns the exact dataset vintage/path.
    pub const fn dataset(&self) -> &crate::CensusDataset {
        &self.dataset
    }

    /// Returns the one-based provider data-row number, excluding the header.
    pub const fn row_number(&self) -> usize {
        self.row_number
    }

    /// Returns the exact variable identity.
    pub const fn variable(&self) -> &SourceIdentifier {
        &self.variable
    }

    /// Returns the metadata label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns the metadata concept when supplied.
    pub fn concept(&self) -> Option<&str> {
        self.concept.as_deref()
    }

    /// Returns the metadata group when supplied.
    pub const fn group(&self) -> Option<&SourceIdentifier> {
        self.group.as_ref()
    }

    /// Returns exact value/missing/annotation state.
    pub const fn value(&self) -> &CensusValueState {
        &self.value
    }

    /// Returns exact row geography.
    pub const fn geography(&self) -> &CensusGeographyValue {
        &self.geography
    }

    /// Returns retained non-geographic predicate semantics.
    pub fn predicates(&self) -> &[CensusPredicateValue] {
        &self.predicates
    }

    /// Returns the source-reported observation period when a response field supplied one.
    pub const fn reported_time(&self) -> Option<&CensusReportedTime> {
        self.reported_time.as_ref()
    }

    /// Returns the key-free request identity.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns the exact response-body identity.
    pub const fn response_payload_digest(&self) -> [u8; 32] {
        self.response_payload_digest
    }

    /// Returns the exact variable-metadata identity.
    pub const fn metadata_payload_digest(&self) -> [u8; 32] {
        self.metadata_payload_digest
    }

    /// Returns the exact closed provider-native row identity.
    pub const fn row_digest(&self) -> [u8; 32] {
        self.row_digest
    }

    /// Returns local availability and ingestion chronology.
    pub const fn clocks(&self) -> &CensusClocks {
        &self.clocks
    }

    /// Returns observed source-revision evidence awaiting shared durable revision authority.
    pub const fn revision_candidate(&self) -> &CensusRevisionCandidate {
        &self.revision
    }
}

/// Why one otherwise bounded response is not complete for its exact request/metadata scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusCompletenessIssue {
    /// One requested response variable was absent from the header.
    MissingRequestedVariable { variable: SourceIdentifier },
    /// One provider group variable was absent from a group response.
    MissingGroupVariable { variable: SourceIdentifier },
    /// A primary variable's metadata-declared annotation was not requested.
    UnrequestedDeclaredAttribute {
        /// Primary variable.
        variable: SourceIdentifier,
        /// Related attribute variable.
        attribute: SourceIdentifier,
    },
    /// A requested annotation column was absent from the response.
    MissingDeclaredAttribute {
        /// Primary variable.
        variable: SourceIdentifier,
        /// Related attribute variable.
        attribute: SourceIdentifier,
    },
    /// An unrequested, non-geographic header column appeared.
    UnexpectedColumn { column: SourceIdentifier },
    /// A provider-returned request predicate did not match any exact admitted request value.
    UnexpectedPredicateValue {
        /// Provider row number.
        row_number: usize,
        /// Exact predicate variable.
        variable: SourceIdentifier,
        /// Returned value, or `None` for an absent/null cell.
        value: Option<String>,
    },
    /// A standard FIPS geography column was absent.
    MissingGeographyLevel { level: String },
    /// A UCGID response did not return `GEO_ID`, so rows could not be assigned safely.
    MissingUniformGeographyId,
    /// One response row lacked a required geography code.
    MissingRowGeography { row_number: usize, level: String },
    /// One exact UCGID result did not match any individually requested geography.
    UnexpectedUniformGeography { row_number: usize, geoid: String },
    /// One exact requested standard geography code did not appear in the response.
    MissingRequestedGeography { level: String, code: String },
    /// One returned standard geography code was outside an exact request clause.
    UnexpectedGeographyCode {
        row_number: usize,
        level: String,
        code: String,
    },
    /// One exact requested UCGID did not appear in the response.
    MissingRequestedUniformGeography { geoid: String },
    /// A wildcard request produced no safely identified geography.
    MissingWildcardGeography,
    /// Wildcard or provider-pseudo geography cardinality cannot be proven from this response.
    UnverifiedGeographyScope,
    /// Returned exact standard geography cardinality did not match the requested Cartesian scope.
    GeographyCardinalityMismatch { expected: usize, returned: usize },
    /// The response repeated one geography/time row identity.
    DuplicateReturnedGeography {
        row_number: usize,
        first_row_number: usize,
    },
    /// The response repeated one economic variable/geography/time/value identity.
    DuplicateEconomicObservation {
        row_number: usize,
        first_row_number: usize,
        variable: SourceIdentifier,
    },
    /// One present cell did not conform to metadata-declared typing.
    InvalidTypedValue {
        /// Provider row number.
        row_number: usize,
        /// Variable identity.
        variable: SourceIdentifier,
    },
    /// A time-scoped query row omitted its provider period.
    MissingReportedTime { row_number: usize },
    /// A returned provider period fell outside the exact requested time predicate.
    UnexpectedReportedTime {
        row_number: usize,
        reported_time: CensusReportedTime,
    },
}

/// Complete or explicitly partial response status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum CensusCompleteness {
    /// Every requested/metadata field and usable row closed.
    Complete,
    /// Parsed evidence is safe but one or more exact scope elements are missing or invalid.
    Partial {
        issues: Vec<CensusCompletenessIssue>,
    },
}

impl CensusCompleteness {
    /// Returns whether the response closes its exact request and metadata scope.
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Returns structured partial-response issues.
    pub fn issues(&self) -> &[CensusCompletenessIssue] {
        match self {
            Self::Complete => &[],
            Self::Partial { issues } => issues,
        }
    }
}

/// Census Data API response pagination evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusPagination {
    /// The reviewed ordinary data-query contract returned one complete JSON matrix and no cursor.
    SingleResponse { request_count: u32 },
}

/// Exact request/return accounting.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusResponseAccounting {
    requests: u32,
    requested_primary_variables: usize,
    requested_wire_variables: usize,
    returned_columns: usize,
    returned_requested_variables: usize,
    missing_requested_variables: usize,
    requested_predicates: usize,
    returned_predicate_columns: usize,
    verified_predicate_values: usize,
    unexpected_predicate_values: usize,
    requested_geographies: Option<usize>,
    returned_geographies: usize,
    returned_rows: usize,
    usable_rows: usize,
    skipped_rows: usize,
    observations: usize,
    observed_values: usize,
    missing_values: usize,
    annotated_values: usize,
    invalid_values: usize,
}

impl CensusResponseAccounting {
    /// Returns provider requests represented by this page.
    pub const fn requests(&self) -> u32 {
        self.requests
    }

    /// Returns requested primary measurements (or metadata-resolved group measurements).
    pub const fn requested_primary_variables(&self) -> usize {
        self.requested_primary_variables
    }

    /// Returns exact variables requested on the wire (or metadata-resolved group variables).
    pub const fn requested_wire_variables(&self) -> usize {
        self.requested_wire_variables
    }

    /// Returns response header columns.
    pub const fn returned_columns(&self) -> usize {
        self.returned_columns
    }

    /// Returns requested variables actually present in the response header.
    pub const fn returned_requested_variables(&self) -> usize {
        self.returned_requested_variables
    }

    /// Returns requested variables absent from the response header.
    pub const fn missing_requested_variables(&self) -> usize {
        self.missing_requested_variables
    }

    /// Returns exact non-geographic predicates bound into the request.
    pub const fn requested_predicates(&self) -> usize {
        self.requested_predicates
    }

    /// Returns requested predicate columns explicitly echoed by the response family.
    pub const fn returned_predicate_columns(&self) -> usize {
        self.returned_predicate_columns
    }

    /// Returns response predicate cells verified against exact admitted request values.
    pub const fn verified_predicate_values(&self) -> usize {
        self.verified_predicate_values
    }

    /// Returns response predicate cells that contradicted the exact request.
    pub const fn unexpected_predicate_values(&self) -> usize {
        self.unexpected_predicate_values
    }

    /// Returns exact requested geography cardinality, or `None` when provider expansion is open.
    pub const fn requested_geographies(&self) -> Option<usize> {
        self.requested_geographies
    }

    /// Returns distinct stable geography identities safely decoded from rows.
    pub const fn returned_geographies(&self) -> usize {
        self.returned_geographies
    }

    /// Returns provider data rows excluding the header.
    pub const fn returned_rows(&self) -> usize {
        self.returned_rows
    }

    /// Returns rows with an exact geography coordinate.
    pub const fn usable_rows(&self) -> usize {
        self.usable_rows
    }

    /// Returns rows skipped because safe geography assignment was impossible.
    pub const fn skipped_rows(&self) -> usize {
        self.skipped_rows
    }

    /// Returns emitted provider-native observations.
    pub const fn observations(&self) -> usize {
        self.observations
    }

    /// Returns unannotated typed values.
    pub const fn observed_values(&self) -> usize {
        self.observed_values
    }

    /// Returns explicit missing values.
    pub const fn missing_values(&self) -> usize {
        self.missing_values
    }

    /// Returns provider-annotated values requiring dataset-specific interpretation.
    pub const fn annotated_values(&self) -> usize {
        self.annotated_values
    }

    /// Returns cells that violated metadata-declared typing.
    pub const fn invalid_values(&self) -> usize {
        self.invalid_values
    }
}

/// One bounded two-dimensional Census JSON response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusDataPage {
    dataset: crate::CensusDataset,
    request_digest: [u8; 32],
    response_payload_digest: [u8; 32],
    response_payload_bytes: usize,
    metadata_payload_digest: [u8; 32],
    header: Vec<SourceIdentifier>,
    observations: Vec<CensusObservation>,
    completeness: CensusCompleteness,
    pagination: CensusPagination,
    accounting: CensusResponseAccounting,
    clocks: CensusClocks,
}

impl CensusDataPage {
    /// Parses one bounded provider matrix using the exact dataset variable/group metadata.
    ///
    /// Missing requested fields, missing annotation columns, unsafe geography rows, and invalid
    /// typed cells become structured partial evidence. Malformed JSON, duplicate headers,
    /// inconsistent row widths, metadata/dataset mismatch, and resource overruns fail closed.
    ///
    /// # Errors
    ///
    /// Returns a typed adapter error for malformed, mismatched, or over-bounded input.
    pub fn parse(
        query: &CensusDataQuery,
        metadata: &CensusVariableCatalog,
        geography_admission: &CensusGeographyAdmission,
        bytes: &[u8],
        limits: CensusParseLimits,
        clocks: CensusClocks,
    ) -> Result<Self, CensusAdapterError> {
        if metadata.dataset() != query.dataset() {
            return Err(CensusAdapterError::MetadataMismatch);
        }
        match (query.selection(), metadata.group()) {
            (CensusSelection::Group { group }, Some(metadata_group)) if group == metadata_group => {
            }
            (CensusSelection::Group { .. }, _) | (_, Some(_)) => {
                return Err(CensusAdapterError::MetadataMismatch);
            }
            (CensusSelection::Variables { .. }, None) => {}
        }
        geography_admission.validate_query(query.geography())?;
        if bytes.len() > limits.max_bytes {
            return Err(CensusAdapterError::BodyTooLarge);
        }
        let predecode_work = bytes
            .len()
            .checked_mul(2)
            .and_then(|bytes| {
                bytes.checked_add(limits.max_cells.checked_mul(size_of::<CensusCell>())?)
            })
            .and_then(|bytes| {
                bytes.checked_add(limits.max_rows.checked_mul(size_of::<Vec<CensusCell>>())?)
            })
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        if predecode_work > CENSUS_OPERATION_MEMORY_LIMIT_BYTES {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        let response_payload_digest = sha256(bytes);
        let metadata_payload_digest = metadata.evidence().payload_digest();
        let semantic_failure = RefCell::new(None);
        let seed = CensusMatrixSeed {
            query,
            metadata,
            geography_admission,
            limits,
            clocks,
            response_payload_digest,
            response_payload_bytes: bytes.len(),
            metadata_payload_digest,
            semantic_failure: &semantic_failure,
        };
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let page = match seed.deserialize(&mut deserializer) {
            Ok(page) => page,
            Err(_) => {
                return Err(semantic_failure
                    .into_inner()
                    .unwrap_or(CensusAdapterError::InvalidJson));
            }
        };
        deserializer
            .end()
            .map_err(|_| CensusAdapterError::InvalidJson)?;
        if page.conservative_retained_bytes()? > CENSUS_OPERATION_MEMORY_LIMIT_BYTES {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        Ok(page)
    }

    /// Rebinds the local processing clocks after bounded decoding completes.
    ///
    /// The parser needs receipt/availability evidence while it constructs provider-native rows,
    /// but only its caller can observe the instant decoding finishes. This consuming transition
    /// preserves the exact receipt and availability authority while replacing the provisional
    /// decode/ingest instants on the page and every aligned observation.
    pub(crate) fn try_with_completed_processing_clocks(
        mut self,
        decoded_at: Timestamp,
        ingested_at: Timestamp,
    ) -> Result<Self, CensusAdapterError> {
        if self.clocks.decoded_at() != self.clocks.received_at()
            || self.clocks.ingested_at() != self.clocks.received_at()
        {
            return Err(CensusAdapterError::InvalidChronology);
        }
        let clocks = CensusClocks::try_new(
            self.clocks.received_at(),
            decoded_at,
            ingested_at,
            self.clocks.availability().clone(),
        )?;
        for observation in &mut self.observations {
            observation.clocks = clocks.clone();
        }
        self.clocks = clocks;
        Ok(self)
    }

    /// Returns the exact dataset.
    pub const fn dataset(&self) -> &crate::CensusDataset {
        &self.dataset
    }

    /// Returns the key-free request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns the exact response payload digest.
    pub const fn response_payload_digest(&self) -> [u8; 32] {
        self.response_payload_digest
    }

    /// Returns a conservative raw + decoded + canonical-work retained-byte estimate.
    pub fn conservative_retained_bytes(&self) -> Result<usize, CensusAdapterError> {
        let mut bytes = size_of::<Self>()
            .checked_add(
                self.response_payload_bytes
                    .checked_mul(2)
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
            )
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        bytes =
            checked_capacity_bytes(bytes, self.header.capacity(), size_of::<SourceIdentifier>())?;
        for column in &self.header {
            bytes = checked_capacity_bytes(bytes, column.as_str().len(), 1)?;
        }
        bytes = checked_capacity_bytes(
            bytes,
            self.observations.capacity(),
            size_of::<CensusObservation>(),
        )?;
        for observation in &self.observations {
            bytes = conservative_observation_bytes(bytes, observation)?;
            // Publication bindings retain a second exact provider-native identity graph.
            bytes = conservative_observation_bytes(bytes, observation)?;
        }
        if let CensusCompleteness::Partial { issues } = &self.completeness {
            bytes = checked_capacity_bytes(
                bytes,
                issues.capacity(),
                size_of::<CensusCompletenessIssue>(),
            )?;
            for issue in issues {
                bytes = checked_capacity_bytes(bytes, completeness_issue_string_bytes(issue), 1)?;
            }
        }
        // Canonical publication retains a payload, evidence, and binding per provider-native row.
        // Reserve a bounded structural floor in addition to every exact retained source string.
        checked_capacity_bytes(bytes, self.observations.capacity(), 1024)
    }

    /// Returns the exact metadata payload digest.
    pub const fn metadata_payload_digest(&self) -> [u8; 32] {
        self.metadata_payload_digest
    }

    /// Returns the exact response header.
    pub fn header(&self) -> &[SourceIdentifier] {
        &self.header
    }

    /// Returns safe provider-native observations.
    pub fn observations(&self) -> &[CensusObservation] {
        &self.observations
    }

    /// Returns complete or structured partial status.
    pub const fn completeness(&self) -> &CensusCompleteness {
        &self.completeness
    }

    /// Returns single-response pagination evidence.
    pub const fn pagination(&self) -> CensusPagination {
        self.pagination
    }

    /// Returns exact request/response accounting.
    pub const fn accounting(&self) -> &CensusResponseAccounting {
        &self.accounting
    }

    /// Returns shared page clocks.
    pub const fn clocks(&self) -> &CensusClocks {
        &self.clocks
    }
}

struct CensusMatrixSeed<'a> {
    query: &'a CensusDataQuery,
    metadata: &'a CensusVariableCatalog,
    geography_admission: &'a CensusGeographyAdmission,
    limits: CensusParseLimits,
    clocks: CensusClocks,
    response_payload_digest: [u8; 32],
    response_payload_bytes: usize,
    metadata_payload_digest: [u8; 32],
    semantic_failure: &'a RefCell<Option<CensusAdapterError>>,
}

impl<'de> DeserializeSeed<'de> for CensusMatrixSeed<'_> {
    type Value = CensusDataPage;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CensusMatrixVisitor {
            query: self.query,
            metadata: self.metadata,
            geography_admission: self.geography_admission,
            limits: self.limits,
            clocks: self.clocks,
            response_payload_digest: self.response_payload_digest,
            response_payload_bytes: self.response_payload_bytes,
            metadata_payload_digest: self.metadata_payload_digest,
            semantic_failure: self.semantic_failure,
        })
    }
}

struct CensusMatrixVisitor<'a> {
    query: &'a CensusDataQuery,
    metadata: &'a CensusVariableCatalog,
    geography_admission: &'a CensusGeographyAdmission,
    limits: CensusParseLimits,
    clocks: CensusClocks,
    response_payload_digest: [u8; 32],
    response_payload_bytes: usize,
    metadata_payload_digest: [u8; 32],
    semantic_failure: &'a RefCell<Option<CensusAdapterError>>,
}

impl<'de> Visitor<'de> for CensusMatrixVisitor<'_> {
    type Value = CensusDataPage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded Census JSON matrix")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let header_cells = sequence
            .next_element_seed(CensusRowSeed {
                expected_columns: None,
                limits: self.limits,
            })?
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
        let header = header_from_cells(header_cells)
            .map_err(|error| semantic_decode_error::<A::Error>(self.semantic_failure, error))?;
        let mut builder = CensusPageBuilder::try_new(
            self.query,
            self.metadata,
            self.geography_admission,
            self.limits,
            self.clocks,
            self.response_payload_digest,
            self.response_payload_bytes,
            self.metadata_payload_digest,
            header,
        )
        .map_err(|error| semantic_decode_error::<A::Error>(self.semantic_failure, error))?;
        loop {
            if builder.returned_rows == self.limits.max_rows {
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(semantic_decode_error::<A::Error>(
                        self.semantic_failure,
                        CensusAdapterError::ResourceLimitExceeded,
                    ));
                }
                break;
            }
            let Some(row) = sequence.next_element_seed(CensusRowSeed {
                expected_columns: Some(builder.header.len()),
                limits: self.limits,
            })?
            else {
                break;
            };
            let next_rows = builder.returned_rows.checked_add(1).ok_or_else(|| {
                semantic_decode_error::<A::Error>(
                    self.semantic_failure,
                    CensusAdapterError::ResourceLimitExceeded,
                )
            })?;
            if next_rows
                .checked_mul(builder.header.len())
                .is_none_or(|cells| cells > self.limits.max_cells)
            {
                return Err(semantic_decode_error::<A::Error>(
                    self.semantic_failure,
                    CensusAdapterError::ResourceLimitExceeded,
                ));
            }
            builder
                .push_row(row)
                .map_err(|error| semantic_decode_error::<A::Error>(self.semantic_failure, error))?;
        }
        builder
            .finish()
            .map_err(|error| semantic_decode_error::<A::Error>(self.semantic_failure, error))
    }
}

fn semantic_decode_error<E>(
    failure: &RefCell<Option<CensusAdapterError>>,
    error: CensusAdapterError,
) -> E
where
    E: serde::de::Error,
{
    if failure.borrow().is_none() {
        *failure.borrow_mut() = Some(error);
    }
    E::custom("bounded Census semantic decode failure")
}

#[derive(Clone, Copy)]
struct CensusRowSeed {
    expected_columns: Option<usize>,
    limits: CensusParseLimits,
}

impl<'de> DeserializeSeed<'de> for CensusRowSeed {
    type Value = Vec<CensusCell>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CensusRowVisitor(self))
    }
}

struct CensusRowVisitor(CensusRowSeed);

impl<'de> Visitor<'de> for CensusRowVisitor {
    type Value = Vec<CensusCell>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("one bounded Census matrix row")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let maximum = self.0.expected_columns.unwrap_or(self.0.limits.max_columns);
        if maximum == 0 || sequence.size_hint().is_some_and(|hint| hint > maximum) {
            return Err(serde::de::Error::custom(
                "Census row exceeds its column bound",
            ));
        }
        let reserve = sequence.size_hint().unwrap_or(maximum).min(maximum);
        let mut row = Vec::new();
        row.try_reserve_exact(reserve)
            .map_err(|_| serde::de::Error::custom("Census row allocation failed"))?;
        while row.len() < maximum {
            let Some(cell) = sequence.next_element_seed(CensusCellSeed {
                limits: self.0.limits,
            })?
            else {
                break;
            };
            row.push(cell);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some()
            || row.is_empty()
            || self
                .0
                .expected_columns
                .is_some_and(|expected| row.len() != expected)
        {
            return Err(serde::de::Error::custom("invalid Census row width"));
        }
        Ok(row)
    }
}

#[derive(Clone, Copy)]
struct CensusCellSeed {
    limits: CensusParseLimits,
}

impl<'de> DeserializeSeed<'de> for CensusCellSeed {
    type Value = CensusCell;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CensusCellVisitor {
            limits: self.limits,
        })
    }
}

struct CensusCellVisitor {
    limits: CensusParseLimits,
}

impl CensusCellVisitor {
    fn text<E>(self, value: &str) -> Result<CensusCell, E>
    where
        E: serde::de::Error,
    {
        ensure_string(value, self.limits).map_err(E::custom)?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(value.len())
            .map_err(|_| E::custom("Census cell allocation failed"))?;
        owned.push_str(value);
        Ok(CensusCell::Text(owned))
    }

    fn number<E, T>(self, value: T) -> Result<CensusCell, E>
    where
        E: serde::de::Error,
        T: ToString,
    {
        let value = value.to_string();
        ensure_string(&value, self.limits).map_err(E::custom)?;
        Ok(CensusCell::Number(value))
    }
}

impl<'de> Visitor<'de> for CensusCellVisitor {
    type Value = CensusCell;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a null, string, number, or Boolean Census cell")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(CensusCell::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(CensusCell::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(CensusCell::Boolean(value))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.text(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.text(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        ensure_string(&value, self.limits).map_err(E::custom)?;
        Ok(CensusCell::Text(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.number(value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.number(value)
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.number(value)
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.number(value)
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if !value.is_finite() {
            return Err(E::custom("non-finite Census number"));
        }
        self.number(value)
    }
}

struct CensusPageBuilder<'a> {
    query: &'a CensusDataQuery,
    metadata: &'a CensusVariableCatalog,
    clocks: CensusClocks,
    response_payload_digest: [u8; 32],
    response_payload_bytes: usize,
    metadata_payload_digest: [u8; 32],
    header: Vec<SourceIdentifier>,
    header_index: HashMap<String, usize>,
    expected_wire: Vec<SourceIdentifier>,
    primary: Vec<SourceIdentifier>,
    predicates: Vec<CensusPredicateValue>,
    issues: BTreeSet<CensusCompletenessIssue>,
    observations: Vec<CensusObservation>,
    returned_geographies: HashMap<[u8; 32], CensusGeographyValue>,
    returned_rows: usize,
    usable_rows: usize,
    skipped_rows: usize,
    observed_values: usize,
    missing_values: usize,
    annotated_values: usize,
    invalid_values: usize,
    verified_predicate_values: usize,
    unexpected_predicate_values: usize,
    retained_observation_bytes: usize,
    retained_geography_index_bytes: usize,
    row_identities: HashMap<([u8; 32], Option<CensusReportedTime>), usize>,
    economic_identities: HashMap<[u8; 32], usize>,
}

impl<'a> CensusPageBuilder<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "exact query, metadata, grammar, clocks, payload identities, and bounds stay explicit"
    )]
    fn try_new(
        query: &'a CensusDataQuery,
        metadata: &'a CensusVariableCatalog,
        geography_admission: &CensusGeographyAdmission,
        limits: CensusParseLimits,
        clocks: CensusClocks,
        response_payload_digest: [u8; 32],
        response_payload_bytes: usize,
        metadata_payload_digest: [u8; 32],
        header: Vec<SourceIdentifier>,
    ) -> Result<Self, CensusAdapterError> {
        geography_admission.validate_query(query.geography())?;
        if header.is_empty() || header.len() > limits.max_columns {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        if header.iter().collect::<BTreeSet<_>>().len() != header.len() {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        let mut header_index = HashMap::new();
        header_index
            .try_reserve(header.len())
            .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
        for (index, name) in header.iter().enumerate() {
            if header_index
                .insert(name.as_str().to_owned(), index)
                .is_some()
            {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
        }
        let expected_wire = expected_wire_variables(query, metadata)?;
        let primary = primary_variables(query, metadata, &header)?;
        let expected_set = expected_wire
            .iter()
            .map(SourceIdentifier::as_str)
            .collect::<BTreeSet<_>>();
        let geography_levels = query.geography().required_response_levels();
        let geography_set = geography_levels.iter().copied().collect::<BTreeSet<_>>();
        let predicate_set = query
            .predicates()
            .iter()
            .map(|predicate| predicate.variable().as_str())
            .collect::<BTreeSet<_>>();
        let mut issues = BTreeSet::new();
        for variable in &expected_wire {
            if !header_index.contains_key(variable.as_str()) {
                issues.insert(match query.selection() {
                    CensusSelection::Variables { .. } => {
                        CensusCompletenessIssue::MissingRequestedVariable {
                            variable: variable.clone(),
                        }
                    }
                    CensusSelection::Group { .. } => {
                        CensusCompletenessIssue::MissingGroupVariable {
                            variable: variable.clone(),
                        }
                    }
                });
            }
        }
        for variable in &primary {
            let variable_metadata = metadata
                .get(variable.as_str())
                .ok_or(CensusAdapterError::MetadataMismatch)?;
            for attribute in variable_metadata.attributes() {
                if !expected_set.contains(attribute.as_str()) {
                    issues.insert(CensusCompletenessIssue::UnrequestedDeclaredAttribute {
                        variable: variable.clone(),
                        attribute: attribute.clone(),
                    });
                } else if !header_index.contains_key(attribute.as_str()) {
                    issues.insert(CensusCompletenessIssue::MissingDeclaredAttribute {
                        variable: variable.clone(),
                        attribute: attribute.clone(),
                    });
                }
            }
        }
        for level in &geography_levels {
            if !header_index.contains_key(*level) {
                issues.insert(CensusCompletenessIssue::MissingGeographyLevel {
                    level: (*level).to_owned(),
                });
            }
        }
        if matches!(query.geography(), CensusGeography::Uniform { .. })
            && !header_index.contains_key("GEO_ID")
        {
            issues.insert(CensusCompletenessIssue::MissingUniformGeographyId);
        }
        for column in &header {
            let name = column.as_str();
            let allowed_context = geography_set.contains(name)
                || predicate_set.contains(name)
                || matches!(name, "GEO_ID" | "NAME" | "time");
            if !expected_set.contains(name) && !allowed_context {
                issues.insert(CensusCompletenessIssue::UnexpectedColumn {
                    column: column.clone(),
                });
            }
        }
        let predicates = query
            .predicates()
            .iter()
            .map(|predicate| CensusPredicateValue {
                variable: predicate.variable().clone(),
                predicate_type: predicate.predicate_type().clone(),
                values: predicate.values().to_vec(),
            })
            .collect::<Vec<_>>();
        let maximum_observations = limits.max_cells.min(
            limits
                .max_rows
                .checked_mul(primary.len())
                .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
        );
        let minimum_preallocation = response_payload_bytes
            .checked_add(size_of::<Self>())
            .and_then(|bytes| {
                bytes.checked_add(header.len().checked_mul(size_of::<SourceIdentifier>())?)
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    maximum_observations
                        .min(primary.len())
                        .checked_mul(size_of::<CensusObservation>().checked_add(1024)?)?,
                )
            })
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        if minimum_preallocation > CENSUS_OPERATION_MEMORY_LIMIT_BYTES {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        let observations = Vec::new();
        let returned_geographies = HashMap::new();
        let row_identities = HashMap::new();
        let economic_identities = HashMap::new();
        Ok(Self {
            query,
            metadata,
            clocks,
            response_payload_digest,
            response_payload_bytes,
            metadata_payload_digest,
            header,
            header_index,
            expected_wire,
            primary,
            predicates,
            issues,
            observations,
            returned_geographies,
            returned_rows: 0,
            usable_rows: 0,
            skipped_rows: 0,
            observed_values: 0,
            missing_values: 0,
            annotated_values: 0,
            invalid_values: 0,
            verified_predicate_values: 0,
            unexpected_predicate_values: 0,
            retained_observation_bytes: 0,
            retained_geography_index_bytes: 0,
            row_identities,
            economic_identities,
        })
    }

    fn push_row(&mut self, row: Vec<CensusCell>) -> Result<(), CensusAdapterError> {
        self.returned_rows = checked_increment(self.returned_rows)?;
        let row_number = self.returned_rows;
        for predicate in self.query.predicates() {
            let Some(index) = self.header_index.get(predicate.variable().as_str()) else {
                continue;
            };
            let value = row[*index].exact_text();
            if value
                .as_ref()
                .is_some_and(|value| predicate.values().contains(value))
            {
                self.verified_predicate_values = checked_increment(self.verified_predicate_values)?;
            } else {
                self.unexpected_predicate_values =
                    checked_increment(self.unexpected_predicate_values)?;
                self.issues
                    .insert(CensusCompletenessIssue::UnexpectedPredicateValue {
                        row_number,
                        variable: predicate.variable().clone(),
                        value,
                    });
            }
        }
        let Some(geography) = row_geography(
            self.query,
            &self.header_index,
            &row,
            row_number,
            &mut self.issues,
        )?
        else {
            self.skipped_rows = checked_increment(self.skipped_rows)?;
            return Ok(());
        };
        self.usable_rows = checked_increment(self.usable_rows)?;
        let reported_time = self
            .header_index
            .get("time")
            .and_then(|index| row[*index].nonempty_text())
            .map(CensusReportedTime::parse)
            .transpose()?;
        if let Some(predicate) = self.query.time() {
            match reported_time.as_ref() {
                None => {
                    self.issues
                        .insert(CensusCompletenessIssue::MissingReportedTime { row_number });
                }
                Some(reported_time) if !time_predicate_contains(predicate, reported_time) => {
                    self.issues
                        .insert(CensusCompletenessIssue::UnexpectedReportedTime {
                            row_number,
                            reported_time: reported_time.clone(),
                        });
                }
                Some(_) => {}
            }
        }
        let geography_digest = geography.identity_digest();
        if let Some(first_row_number) = self
            .row_identities
            .insert((geography_digest, reported_time.clone()), row_number)
        {
            self.issues
                .insert(CensusCompletenessIssue::DuplicateReturnedGeography {
                    row_number,
                    first_row_number,
                });
        }
        if !self.returned_geographies.contains_key(&geography_digest) {
            let geography_index_bytes = conservative_geography_bytes(
                size_of::<CensusGeographyValue>()
                    .checked_add(size_of::<[u8; 32]>())
                    .and_then(|bytes| bytes.checked_add(4 * size_of::<usize>()))
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
                &geography,
            )?;
            self.retained_geography_index_bytes = self
                .retained_geography_index_bytes
                .checked_add(geography_index_bytes)
                .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
            self.returned_geographies
                .insert(geography_digest, geography.clone());
        }
        for variable in &self.primary {
            if !self.header_index.contains_key(variable.as_str()) {
                continue;
            }
            let variable_metadata = self
                .metadata
                .get(variable.as_str())
                .ok_or(CensusAdapterError::MetadataMismatch)?;
            let (value, missing_attribute) =
                value_state(variable_metadata, &self.header_index, &row, self.metadata)?;
            if let Some(attribute) = missing_attribute {
                self.issues
                    .insert(CensusCompletenessIssue::MissingDeclaredAttribute {
                        variable: variable.clone(),
                        attribute,
                    });
            }
            match &value {
                CensusValueState::Observed { .. } => {
                    self.observed_values = checked_increment(self.observed_values)?;
                }
                CensusValueState::Missing { .. } => {
                    self.missing_values = checked_increment(self.missing_values)?;
                }
                CensusValueState::Annotated { .. } => {
                    self.annotated_values = checked_increment(self.annotated_values)?;
                }
                CensusValueState::Invalid { .. } => {
                    self.invalid_values = checked_increment(self.invalid_values)?;
                    self.issues
                        .insert(CensusCompletenessIssue::InvalidTypedValue {
                            row_number,
                            variable: variable.clone(),
                        });
                }
            }
            let row_digest = row_digest(
                self.query,
                variable,
                &value,
                &geography,
                reported_time.as_ref(),
                self.metadata_payload_digest,
            )?;
            if let Some(first_row_number) = self.economic_identities.insert(row_digest, row_number)
            {
                self.issues
                    .insert(CensusCompletenessIssue::DuplicateEconomicObservation {
                        row_number,
                        first_row_number,
                        variable: variable.clone(),
                    });
            }
            let family_digest =
                family_digest(self.query, variable, &geography, reported_time.as_ref())?;
            let candidate_bytes = conservative_observation_components_bytes(
                self.query.dataset(),
                variable,
                variable_metadata.label(),
                variable_metadata.concept(),
                variable_metadata.group(),
                &value,
                &geography,
                &self.predicates,
                reported_time.as_ref(),
            )?;
            self.ensure_next_observation_capacity(candidate_bytes)?;
            let first_observed_at = self
                .clocks
                .availability()
                .conservative_available_at()
                .unwrap_or(self.clocks.received_at());
            self.observations
                .try_reserve_exact(1)
                .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
            self.observations.push(CensusObservation {
                dataset: self.query.dataset().clone(),
                row_number,
                variable: variable.clone(),
                label: variable_metadata.label().to_owned(),
                concept: variable_metadata.concept().map(str::to_owned),
                group: variable_metadata.group().cloned(),
                value,
                geography: geography.clone(),
                predicates: self.predicates.clone(),
                reported_time: reported_time.clone(),
                request_digest: self.query.request_digest(),
                response_payload_digest: self.response_payload_digest,
                metadata_payload_digest: self.metadata_payload_digest,
                row_digest,
                clocks: self.clocks.clone(),
                revision: CensusRevisionCandidate {
                    family_digest,
                    content_digest: row_digest,
                    first_observed_at,
                },
            });
            self.retained_observation_bytes = self
                .retained_observation_bytes
                .checked_add(candidate_bytes)
                .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        }
        Ok(())
    }

    fn ensure_next_observation_capacity(
        &self,
        candidate_bytes: usize,
    ) -> Result<(), CensusAdapterError> {
        let row_index_work = self
            .returned_rows
            .checked_mul(
                size_of::<([u8; 32], Option<CensusReportedTime>)>()
                    .checked_add(4 * size_of::<usize>())
                    .and_then(|bytes| bytes.checked_mul(2))
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
            )
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        let economic_index_work = self
            .observations
            .len()
            .checked_add(1)
            .and_then(|count| {
                count.checked_mul(
                    size_of::<[u8; 32]>()
                        .checked_add(4 * size_of::<usize>())?
                        .checked_mul(2)?,
                )
            })
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        let issue_work = self
            .issues
            .len()
            .checked_mul(
                size_of::<CensusCompletenessIssue>()
                    .checked_add(4 * size_of::<usize>())
                    .and_then(|bytes| bytes.checked_mul(2))
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
            )
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        let issue_string_work = self.issues.iter().try_fold(0_usize, |bytes, issue| {
            bytes
                .checked_add(completeness_issue_string_bytes(issue))
                .ok_or(CensusAdapterError::ResourceLimitExceeded)
        })?;
        let projected = self
            .response_payload_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(self.retained_observation_bytes))
            .and_then(|bytes| bytes.checked_add(candidate_bytes))
            .and_then(|bytes| bytes.checked_add(self.retained_geography_index_bytes))
            .and_then(|bytes| bytes.checked_add(row_index_work))
            .and_then(|bytes| bytes.checked_add(economic_index_work))
            .and_then(|bytes| bytes.checked_add(issue_work))
            .and_then(|bytes| bytes.checked_add(issue_string_work))
            .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
        if projected > CENSUS_OPERATION_MEMORY_LIMIT_BYTES {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<CensusDataPage, CensusAdapterError> {
        reconcile_geography_scope(self.query, &self.returned_geographies, &mut self.issues)?;
        let returned_requested_variables = self
            .expected_wire
            .iter()
            .filter(|variable| self.header_index.contains_key(variable.as_str()))
            .count();
        let missing_requested_variables = self
            .expected_wire
            .len()
            .checked_sub(returned_requested_variables)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        let returned_predicate_columns = self
            .query
            .predicates()
            .iter()
            .filter(|predicate| {
                self.header_index
                    .contains_key(predicate.variable().as_str())
            })
            .count();
        let accounting = CensusResponseAccounting {
            requests: 1,
            requested_primary_variables: self.primary.len(),
            requested_wire_variables: self.expected_wire.len(),
            returned_columns: self.header.len(),
            returned_requested_variables,
            missing_requested_variables,
            requested_predicates: self.query.predicates().len(),
            returned_predicate_columns,
            verified_predicate_values: self.verified_predicate_values,
            unexpected_predicate_values: self.unexpected_predicate_values,
            requested_geographies: requested_geography_count(self.query)?,
            returned_geographies: self.returned_geographies.len(),
            returned_rows: self.returned_rows,
            usable_rows: self.usable_rows,
            skipped_rows: self.skipped_rows,
            observations: self.observations.len(),
            observed_values: self.observed_values,
            missing_values: self.missing_values,
            annotated_values: self.annotated_values,
            invalid_values: self.invalid_values,
        };
        let completeness = if self.issues.is_empty() {
            CensusCompleteness::Complete
        } else {
            CensusCompleteness::Partial {
                issues: self.issues.into_iter().collect(),
            }
        };
        Ok(CensusDataPage {
            dataset: self.query.dataset().clone(),
            request_digest: self.query.request_digest(),
            response_payload_digest: self.response_payload_digest,
            response_payload_bytes: self.response_payload_bytes,
            metadata_payload_digest: self.metadata_payload_digest,
            header: self.header,
            observations: self.observations,
            completeness,
            pagination: CensusPagination::SingleResponse { request_count: 1 },
            accounting,
            clocks: self.clocks,
        })
    }
}

fn header_from_cells(cells: Vec<CensusCell>) -> Result<Vec<SourceIdentifier>, CensusAdapterError> {
    let mut header = Vec::new();
    header
        .try_reserve_exact(cells.len())
        .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
    for cell in cells {
        let CensusCell::Text(value) = cell else {
            return Err(CensusAdapterError::SchemaDrift);
        };
        header
            .push(SourceIdentifier::try_from(value).map_err(|_| CensusAdapterError::SchemaDrift)?);
    }
    Ok(header)
}

fn checked_increment(value: usize) -> Result<usize, CensusAdapterError> {
    value
        .checked_add(1)
        .ok_or(CensusAdapterError::ResourceLimitExceeded)
}

fn checked_capacity_bytes(
    current: usize,
    capacity: usize,
    element_bytes: usize,
) -> Result<usize, CensusAdapterError> {
    current
        .checked_add(
            capacity
                .checked_mul(element_bytes)
                .ok_or(CensusAdapterError::ResourceLimitExceeded)?,
        )
        .ok_or(CensusAdapterError::ResourceLimitExceeded)
}

fn conservative_observation_bytes(
    mut bytes: usize,
    observation: &CensusObservation,
) -> Result<usize, CensusAdapterError> {
    for segment in observation.dataset.path() {
        bytes = checked_capacity_bytes(bytes, segment.capacity(), 1)?;
    }
    bytes = checked_capacity_bytes(bytes, observation.variable.as_str().len(), 1)?;
    bytes = checked_capacity_bytes(bytes, observation.label.capacity(), 1)?;
    if let Some(concept) = &observation.concept {
        bytes = checked_capacity_bytes(bytes, concept.capacity(), 1)?;
    }
    if let Some(group) = &observation.group {
        bytes = checked_capacity_bytes(bytes, group.as_str().len(), 1)?;
    }
    bytes = conservative_value_bytes(bytes, &observation.value)?;
    bytes = conservative_geography_bytes(bytes, &observation.geography)?;
    bytes = checked_capacity_bytes(
        bytes,
        observation.predicates.capacity(),
        size_of::<CensusPredicateValue>(),
    )?;
    for predicate in &observation.predicates {
        bytes = checked_capacity_bytes(bytes, predicate.variable.as_str().len(), 1)?;
        bytes = checked_capacity_bytes(bytes, predicate.values.capacity(), size_of::<String>())?;
        for value in &predicate.values {
            bytes = checked_capacity_bytes(bytes, value.capacity(), 1)?;
        }
    }
    if let Some(CensusReportedTime::ProviderPeriod { value }) = &observation.reported_time {
        bytes = checked_capacity_bytes(bytes, value.as_str().len(), 1)?;
    }
    Ok(bytes)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every heap-bearing observation component stays explicit for preallocation admission"
)]
fn conservative_observation_components_bytes(
    dataset: &crate::CensusDataset,
    variable: &SourceIdentifier,
    label: &str,
    concept: Option<&str>,
    group: Option<&SourceIdentifier>,
    value: &CensusValueState,
    geography: &CensusGeographyValue,
    predicates: &[CensusPredicateValue],
    reported_time: Option<&CensusReportedTime>,
) -> Result<usize, CensusAdapterError> {
    let mut heap_bytes = 0_usize;
    for segment in dataset.path() {
        heap_bytes = checked_capacity_bytes(heap_bytes, segment.capacity(), 1)?;
    }
    heap_bytes = checked_capacity_bytes(heap_bytes, variable.as_str().len(), 1)?;
    heap_bytes = checked_capacity_bytes(heap_bytes, label.len(), 1)?;
    if let Some(concept) = concept {
        heap_bytes = checked_capacity_bytes(heap_bytes, concept.len(), 1)?;
    }
    if let Some(group) = group {
        heap_bytes = checked_capacity_bytes(heap_bytes, group.as_str().len(), 1)?;
    }
    heap_bytes = conservative_value_bytes(heap_bytes, value)?;
    heap_bytes = conservative_geography_bytes(heap_bytes, geography)?;
    heap_bytes = checked_capacity_bytes(
        heap_bytes,
        predicates.len(),
        size_of::<CensusPredicateValue>(),
    )?;
    for predicate in predicates {
        heap_bytes = checked_capacity_bytes(heap_bytes, predicate.variable.as_str().len(), 1)?;
        heap_bytes =
            checked_capacity_bytes(heap_bytes, predicate.values.len(), size_of::<String>())?;
        for value in &predicate.values {
            heap_bytes = checked_capacity_bytes(heap_bytes, value.capacity(), 1)?;
        }
    }
    if let Some(CensusReportedTime::ProviderPeriod { value }) = reported_time {
        heap_bytes = checked_capacity_bytes(heap_bytes, value.as_str().len(), 1)?;
    }
    size_of::<CensusObservation>()
        .checked_add(1024)
        .and_then(|bytes| bytes.checked_add(heap_bytes.checked_mul(2)?))
        .ok_or(CensusAdapterError::ResourceLimitExceeded)
}

fn conservative_value_bytes(
    mut bytes: usize,
    value: &CensusValueState,
) -> Result<usize, CensusAdapterError> {
    let (raw, annotations) = match value {
        CensusValueState::Observed {
            value: CensusTypedValue::Text(value),
        } => {
            bytes = checked_capacity_bytes(bytes, value.capacity(), 1)?;
            return Ok(bytes);
        }
        CensusValueState::Observed { .. } => return Ok(bytes),
        CensusValueState::Annotated {
            raw,
            typed_candidate,
            annotations,
        } => {
            if let Some(CensusTypedValue::Text(value)) = typed_candidate {
                bytes = checked_capacity_bytes(bytes, value.capacity(), 1)?;
            }
            (Some(raw), annotations)
        }
        CensusValueState::Invalid {
            raw, annotations, ..
        } => (Some(raw), annotations),
        CensusValueState::Missing { annotations, .. } => (None, annotations),
    };
    if let Some(raw) = raw {
        bytes = checked_capacity_bytes(bytes, raw.capacity(), 1)?;
    }
    bytes = checked_capacity_bytes(bytes, annotations.capacity(), size_of::<CensusAnnotation>())?;
    for annotation in annotations {
        bytes = checked_capacity_bytes(bytes, annotation.variable.as_str().len(), 1)?;
        bytes = checked_capacity_bytes(bytes, annotation.raw.capacity(), 1)?;
    }
    Ok(bytes)
}

fn conservative_geography_bytes(
    mut bytes: usize,
    geography: &CensusGeographyValue,
) -> Result<usize, CensusAdapterError> {
    match geography {
        CensusGeographyValue::Standard {
            components,
            fully_qualified_geoid,
            name,
            ..
        } => {
            bytes = checked_capacity_bytes(
                bytes,
                components.capacity(),
                size_of::<CensusGeographyComponent>(),
            )?;
            for component in components {
                bytes = checked_capacity_bytes(bytes, component.level.capacity(), 1)?;
                bytes = checked_capacity_bytes(bytes, component.code.capacity(), 1)?;
            }
            if let Some(geoid) = fully_qualified_geoid {
                bytes = checked_capacity_bytes(bytes, geoid.capacity(), 1)?;
            }
            if let Some(name) = name {
                bytes = checked_capacity_bytes(bytes, name.capacity(), 1)?;
            }
        }
        CensusGeographyValue::Uniform {
            fully_qualified_geoid,
            name,
            ..
        } => {
            bytes = checked_capacity_bytes(bytes, fully_qualified_geoid.capacity(), 1)?;
            if let Some(name) = name {
                bytes = checked_capacity_bytes(bytes, name.capacity(), 1)?;
            }
        }
    }
    Ok(bytes)
}

fn completeness_issue_string_bytes(issue: &CensusCompletenessIssue) -> usize {
    match issue {
        CensusCompletenessIssue::MissingRequestedVariable { variable }
        | CensusCompletenessIssue::MissingGroupVariable { variable }
        | CensusCompletenessIssue::UnexpectedColumn { column: variable }
        | CensusCompletenessIssue::InvalidTypedValue { variable, .. }
        | CensusCompletenessIssue::DuplicateEconomicObservation { variable, .. } => {
            variable.as_str().len()
        }
        CensusCompletenessIssue::UnexpectedPredicateValue {
            variable, value, ..
        } => variable
            .as_str()
            .len()
            .saturating_add(value.as_ref().map_or(0, String::len)),
        CensusCompletenessIssue::UnrequestedDeclaredAttribute {
            variable,
            attribute,
        }
        | CensusCompletenessIssue::MissingDeclaredAttribute {
            variable,
            attribute,
        } => variable
            .as_str()
            .len()
            .saturating_add(attribute.as_str().len()),
        CensusCompletenessIssue::MissingGeographyLevel { level }
        | CensusCompletenessIssue::MissingRowGeography { level, .. } => level.len(),
        CensusCompletenessIssue::UnexpectedUniformGeography { geoid, .. }
        | CensusCompletenessIssue::MissingRequestedUniformGeography { geoid } => geoid.len(),
        CensusCompletenessIssue::MissingRequestedGeography { level, code }
        | CensusCompletenessIssue::UnexpectedGeographyCode { level, code, .. } => {
            level.len().saturating_add(code.len())
        }
        CensusCompletenessIssue::MissingUniformGeographyId
        | CensusCompletenessIssue::MissingWildcardGeography
        | CensusCompletenessIssue::UnverifiedGeographyScope
        | CensusCompletenessIssue::GeographyCardinalityMismatch { .. }
        | CensusCompletenessIssue::DuplicateReturnedGeography { .. }
        | CensusCompletenessIssue::MissingReportedTime { .. } => 0,
        CensusCompletenessIssue::UnexpectedReportedTime {
            reported_time: CensusReportedTime::ProviderPeriod { value },
            ..
        } => value.as_str().len(),
        CensusCompletenessIssue::UnexpectedReportedTime { .. } => 0,
    }
}

fn geography_scope(query: &CensusDataQuery) -> Result<CensusGeographyScope, CensusAdapterError> {
    match requested_geography_count(query)? {
        Some(1) => Ok(CensusGeographyScope::Aggregate),
        Some(_) => Ok(CensusGeographyScope::Detail),
        None => Ok(CensusGeographyScope::Unknown),
    }
}

fn requested_geography_count(query: &CensusDataQuery) -> Result<Option<usize>, CensusAdapterError> {
    match query.geography() {
        CensusGeography::Standard {
            for_clause,
            in_clauses,
        } => in_clauses
            .iter()
            .chain(std::iter::once(for_clause))
            .try_fold(Some(1_usize), |count, clause| {
                if clause
                    .codes()
                    .iter()
                    .any(|code| matches!(code, CensusGeographyCode::Wildcard))
                {
                    return Ok(None);
                }
                count
                    .and_then(|count| count.checked_mul(clause.codes().len()))
                    .map(Some)
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)
            }),
        CensusGeography::Uniform { values } => {
            if values
                .iter()
                .any(|value| value.as_str().starts_with("pseudo("))
            {
                Ok(None)
            } else {
                Ok(Some(values.len()))
            }
        }
    }
}

fn time_predicate_contains(predicate: CensusTimePredicate, reported: &CensusReportedTime) -> bool {
    let reported = match reported {
        CensusReportedTime::Year { year } => CensusTimePoint::Year { year: *year },
        CensusReportedTime::Month { year, month } => CensusTimePoint::Month {
            year: *year,
            month: *month,
        },
        CensusReportedTime::Quarter { year, quarter } => CensusTimePoint::Quarter {
            year: *year,
            quarter: *quarter,
        },
        CensusReportedTime::CalendarDate { .. } | CensusReportedTime::ProviderPeriod { .. } => {
            return false;
        }
    };
    match predicate {
        CensusTimePredicate::At { point } => reported == point,
        CensusTimePredicate::From { start } => {
            std::mem::discriminant(&reported) == std::mem::discriminant(&start) && reported >= start
        }
        CensusTimePredicate::To { end } => {
            std::mem::discriminant(&reported) == std::mem::discriminant(&end) && reported <= end
        }
        CensusTimePredicate::Range { start, end } => {
            std::mem::discriminant(&reported) == std::mem::discriminant(&start)
                && reported >= start
                && reported <= end
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CensusCell {
    Null,
    Text(String),
    Number(String),
    Boolean(bool),
}

impl CensusCell {
    fn nonempty_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) | Self::Number(value) if !value.is_empty() => Some(value),
            Self::Boolean(true) => Some("true"),
            Self::Boolean(false) => Some("false"),
            Self::Null | Self::Text(_) | Self::Number(_) => None,
        }
    }

    fn exact_text(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Text(value) | Self::Number(value) => Some(value.clone()),
            Self::Boolean(value) => Some(value.to_string()),
        }
    }
}

fn expected_wire_variables(
    query: &CensusDataQuery,
    metadata: &CensusVariableCatalog,
) -> Result<Vec<SourceIdentifier>, CensusAdapterError> {
    match query.selection() {
        CensusSelection::Variables { wire, .. } => {
            if wire
                .iter()
                .any(|variable| metadata.get(variable.as_str()).is_none())
            {
                return Err(CensusAdapterError::MetadataMismatch);
            }
            Ok(wire.clone())
        }
        CensusSelection::Group { .. } => Ok(metadata
            .variables()
            .filter(|variable| {
                !matches!(
                    variable.predicate_type(),
                    CensusPredicateType::FipsFor
                        | CensusPredicateType::FipsIn
                        | CensusPredicateType::Ucgid
                        | CensusPredicateType::Time
                )
            })
            .map(|variable| variable.name().clone())
            .collect()),
    }
}

fn primary_variables(
    query: &CensusDataQuery,
    metadata: &CensusVariableCatalog,
    header: &[SourceIdentifier],
) -> Result<Vec<SourceIdentifier>, CensusAdapterError> {
    let candidates = match query.selection() {
        CensusSelection::Variables { primary, .. } => primary.clone(),
        CensusSelection::Group { .. } => header.to_vec(),
    };
    let mut primary = Vec::new();
    for variable in candidates {
        let Some(variable_metadata) = metadata.get(variable.as_str()) else {
            if matches!(query.selection(), CensusSelection::Variables { .. }) {
                return Err(CensusAdapterError::MetadataMismatch);
            }
            continue;
        };
        let attribute = !metadata.attribute_owners(variable.as_str()).is_empty();
        let fixed_context = matches!(variable.as_str(), "GEO_ID" | "NAME" | "time")
            || query
                .geography()
                .required_response_levels()
                .contains(&variable.as_str());
        if !attribute && !variable_metadata.is_context() && !fixed_context {
            primary.push(variable);
        }
    }
    Ok(primary)
}

fn value_state(
    metadata: &CensusVariableMetadata,
    header: &HashMap<String, usize>,
    row: &[CensusCell],
    catalog: &CensusVariableCatalog,
) -> Result<(CensusValueState, Option<SourceIdentifier>), CensusAdapterError> {
    let index = header
        .get(metadata.name().as_str())
        .copied()
        .ok_or(CensusAdapterError::MetadataMismatch)?;
    let mut annotations = Vec::new();
    let mut missing_attribute = None;
    for attribute in metadata.attributes() {
        if catalog.get(attribute.as_str()).is_none() {
            return Err(CensusAdapterError::MetadataMismatch);
        }
        let Some(index) = header.get(attribute.as_str()).copied() else {
            missing_attribute.get_or_insert_with(|| attribute.clone());
            continue;
        };
        if let Some(raw) = row[index].nonempty_text() {
            annotations.push(CensusAnnotation {
                variable: attribute.clone(),
                raw: raw.to_owned(),
            });
        }
    }
    if missing_attribute.is_some() {
        return Ok((
            CensusValueState::Missing {
                reason: CensusMissingReason::AnnotationColumnMissing,
                annotations,
            },
            missing_attribute,
        ));
    }
    match &row[index] {
        CensusCell::Null => Ok((
            CensusValueState::Missing {
                reason: if annotations.is_empty() {
                    CensusMissingReason::JsonNull
                } else {
                    CensusMissingReason::ProviderAnnotatedMissing
                },
                annotations,
            },
            None,
        )),
        CensusCell::Text(raw) if raw.is_empty() => Ok((
            CensusValueState::Missing {
                reason: if annotations.is_empty() {
                    CensusMissingReason::EmptyString
                } else {
                    CensusMissingReason::ProviderAnnotatedMissing
                },
                annotations,
            },
            None,
        )),
        cell => {
            let raw = cell.exact_text().ok_or(CensusAdapterError::SchemaDrift)?;
            let typed = typed_value(cell, metadata.predicate_type());
            if !annotations.is_empty() {
                return Ok((
                    CensusValueState::Annotated {
                        raw,
                        typed_candidate: typed.ok(),
                        annotations,
                    },
                    None,
                ));
            }
            match typed {
                Ok(value) => Ok((CensusValueState::Observed { value }, None)),
                Err(()) => Ok((
                    CensusValueState::Invalid {
                        raw,
                        expected: metadata.predicate_type().clone(),
                        annotations,
                    },
                    None,
                )),
            }
        }
    }
}

fn typed_value(
    cell: &CensusCell,
    predicate_type: &CensusPredicateType,
) -> Result<CensusTypedValue, ()> {
    match (cell, predicate_type) {
        (CensusCell::Boolean(value), _) => Ok(CensusTypedValue::Boolean(*value)),
        (CensusCell::Text(value) | CensusCell::Number(value), CensusPredicateType::Integer) => {
            value
                .parse::<i128>()
                .map(CensusTypedValue::Integer)
                .map_err(|_| ())
        }
        (CensusCell::Text(value) | CensusCell::Number(value), CensusPredicateType::Float) => value
            .parse::<Decimal>()
            .map(CensusTypedValue::Decimal)
            .map_err(|_| ()),
        (CensusCell::Text(value) | CensusCell::Number(value), _) => {
            Ok(CensusTypedValue::Text(value.clone()))
        }
        (CensusCell::Null, _) => Err(()),
    }
}

fn row_geography(
    query: &CensusDataQuery,
    header: &HashMap<String, usize>,
    row: &[CensusCell],
    row_number: usize,
    issues: &mut BTreeSet<CensusCompletenessIssue>,
) -> Result<Option<CensusGeographyValue>, CensusAdapterError> {
    let scope = geography_scope(query)?;
    let name = header
        .get("NAME")
        .and_then(|index| row[*index].nonempty_text())
        .map(str::to_owned);
    let returned_geoid = header
        .get("GEO_ID")
        .and_then(|index| row[*index].nonempty_text())
        .map(str::to_owned);
    match query.geography() {
        CensusGeography::Standard {
            for_clause,
            in_clauses,
        } => {
            let mut components = Vec::new();
            for level in query.geography().required_response_levels() {
                let Some(index) = header.get(level).copied() else {
                    return Ok(None);
                };
                let Some(code) = row[index].nonempty_text() else {
                    issues.insert(CensusCompletenessIssue::MissingRowGeography {
                        row_number,
                        level: level.to_owned(),
                    });
                    return Ok(None);
                };
                let clause = in_clauses
                    .iter()
                    .find(|clause| clause.level() == level)
                    .unwrap_or(for_clause);
                let exact = clause
                    .codes()
                    .iter()
                    .filter_map(|candidate| match candidate {
                        CensusGeographyCode::Exact(value) => Some(value.as_str()),
                        CensusGeographyCode::Wildcard => None,
                    })
                    .collect::<BTreeSet<_>>();
                if !exact.is_empty() && !exact.contains(code) {
                    issues.insert(CensusCompletenessIssue::UnexpectedGeographyCode {
                        row_number,
                        level: level.to_owned(),
                        code: code.to_owned(),
                    });
                    return Ok(None);
                }
                components.push(CensusGeographyComponent {
                    level: level.to_owned(),
                    code: code.to_owned(),
                });
            }
            Ok(Some(CensusGeographyValue::Standard {
                scope,
                components,
                fully_qualified_geoid: returned_geoid,
                name,
            }))
        }
        CensusGeography::Uniform { values } => {
            let Some(geoid) = returned_geoid else {
                return Ok(None);
            };
            let has_pseudo = values
                .iter()
                .any(|value| value.as_str().starts_with("pseudo("));
            if !has_pseudo && !values.iter().any(|value| value.as_str() == geoid) {
                issues.insert(CensusCompletenessIssue::UnexpectedUniformGeography {
                    row_number,
                    geoid: geoid.clone(),
                });
                return Ok(None);
            }
            Ok(Some(CensusGeographyValue::Uniform {
                scope,
                fully_qualified_geoid: geoid,
                name,
            }))
        }
    }
}

fn reconcile_geography_scope(
    query: &CensusDataQuery,
    returned: &HashMap<[u8; 32], CensusGeographyValue>,
    issues: &mut BTreeSet<CensusCompletenessIssue>,
) -> Result<(), CensusAdapterError> {
    match query.geography() {
        CensusGeography::Standard {
            for_clause,
            in_clauses,
        } => {
            let clauses = in_clauses
                .iter()
                .chain(std::iter::once(for_clause))
                .collect::<Vec<_>>();
            let has_wildcard = clauses.iter().any(|clause| {
                clause
                    .codes()
                    .iter()
                    .any(|code| matches!(code, CensusGeographyCode::Wildcard))
            });
            if has_wildcard && returned.is_empty() {
                issues.insert(CensusCompletenessIssue::MissingWildcardGeography);
            }
            if has_wildcard {
                issues.insert(CensusCompletenessIssue::UnverifiedGeographyScope);
            }
            for clause in &clauses {
                let observed = returned
                    .values()
                    .filter_map(|geography| match geography {
                        CensusGeographyValue::Standard { components, .. } => components
                            .iter()
                            .find(|component| component.level() == clause.level())
                            .map(CensusGeographyComponent::code),
                        CensusGeographyValue::Uniform { .. } => None,
                    })
                    .collect::<BTreeSet<_>>();
                for code in clause.codes() {
                    if let CensusGeographyCode::Exact(code) = code
                        && !observed.contains(code.as_str())
                    {
                        issues.insert(CensusCompletenessIssue::MissingRequestedGeography {
                            level: clause.level().to_owned(),
                            code: code.clone(),
                        });
                    }
                }
            }
            if !has_wildcard {
                let expected = clauses.iter().try_fold(1_usize, |product, clause| {
                    product
                        .checked_mul(clause.codes().len())
                        .ok_or(CensusAdapterError::ResourceLimitExceeded)
                })?;
                if returned.len() != expected {
                    issues.insert(CensusCompletenessIssue::GeographyCardinalityMismatch {
                        expected,
                        returned: returned.len(),
                    });
                }
            }
        }
        CensusGeography::Uniform { values } => {
            let returned_geoids = returned
                .values()
                .filter_map(|geography| match geography {
                    CensusGeographyValue::Uniform {
                        fully_qualified_geoid,
                        ..
                    } => Some(fully_qualified_geoid.as_str()),
                    CensusGeographyValue::Standard { .. } => None,
                })
                .collect::<BTreeSet<_>>();
            let has_pseudo = values
                .iter()
                .any(|value| value.as_str().starts_with("pseudo("));
            if has_pseudo && returned_geoids.is_empty() {
                issues.insert(CensusCompletenessIssue::MissingWildcardGeography);
            }
            if has_pseudo {
                issues.insert(CensusCompletenessIssue::UnverifiedGeographyScope);
            }
            for value in values
                .iter()
                .filter(|value| !value.as_str().starts_with("pseudo("))
            {
                if !returned_geoids.contains(value.as_str()) {
                    issues.insert(CensusCompletenessIssue::MissingRequestedUniformGeography {
                        geoid: value.as_str().to_owned(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn row_digest(
    query: &CensusDataQuery,
    variable: &SourceIdentifier,
    value: &CensusValueState,
    geography: &CensusGeographyValue,
    time: Option<&CensusReportedTime>,
    metadata_digest: [u8; 32],
) -> Result<[u8; 32], CensusAdapterError> {
    let payload = serde_json::to_vec(&(value, geography, time))
        .map_err(|_| CensusAdapterError::SchemaDrift)?;
    let mut hasher = Sha256::new();
    update_digest_component(&mut hasher, b"market-squawk-census-native-row-v2");
    update_digest_component(&mut hasher, &query.request_digest());
    update_digest_component(&mut hasher, &metadata_digest);
    update_digest_component(&mut hasher, variable.as_str().as_bytes());
    update_digest_component(&mut hasher, &payload);
    Ok(hasher.finalize().into())
}

fn family_digest(
    query: &CensusDataQuery,
    variable: &SourceIdentifier,
    geography: &CensusGeographyValue,
    time: Option<&CensusReportedTime>,
) -> Result<[u8; 32], CensusAdapterError> {
    let family = serde_json::to_vec(&(
        query.dataset(),
        variable,
        geography.identity_digest(),
        query.predicates(),
        query.time(),
        time,
    ))
    .map_err(|_| CensusAdapterError::SchemaDrift)?;
    let mut hasher = Sha256::new();
    update_digest_component(&mut hasher, b"market-squawk-census-family-v1");
    update_digest_component(&mut hasher, &family);
    Ok(hasher.finalize().into())
}

fn ensure_string(value: &str, limits: CensusParseLimits) -> Result<(), CensusAdapterError> {
    if value.len() > limits.max_string_bytes || value.chars().any(char::is_control) {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    Ok(())
}

fn parse_calendar_date(value: &str) -> Result<Option<CalendarDate>, CensusAdapterError> {
    let mut parts = value.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Ok(None);
    };
    let (Ok(year), Ok(month), Ok(day)) =
        (year.parse::<u16>(), month.parse::<u8>(), day.parse::<u8>())
    else {
        return Ok(None);
    };
    CalendarDate::new(year, month, day)
        .map(Some)
        .map_err(|_| CensusAdapterError::SchemaDrift)
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{SourceIdentifier, Timestamp};

    use crate::{
        CensusDataPage, CensusDataQuery, CensusDataset, CensusDiscoveryDocument,
        CensusDiscoveryKind, CensusDiscoveryRequest, CensusGeography, CensusGeographyAdmission,
        CensusGeographyClause, CensusGeographyCode, CensusParseLimits, CensusSelection,
        CensusValueState,
    };

    use super::{CensusClocks, CensusCompletenessIssue, CensusTypedValue};

    fn dataset() -> Result<CensusDataset, crate::CensusAdapterError> {
        CensusDataset::try_new(2024, "acs/acs1")
    }

    fn metadata() -> Result<crate::CensusVariableCatalog, crate::CensusAdapterError> {
        let request = CensusDiscoveryRequest::try_new(CensusDiscoveryKind::Variables {
            dataset: dataset()?,
        })?;
        let bytes = br#"{
          "variables": {
            "B01001_001E": {
              "label": "Estimate!!Total:",
              "concept": "Sex by Age",
              "predicateType": "int",
              "group": "B01001",
              "attributes": "B01001_001EA",
              "limit": 0,
              "required": false
            },
            "B01001_001EA": {
              "label": "Annotation of Estimate!!Total:",
              "concept": "Sex by Age",
              "predicateType": "string",
              "group": "B01001",
              "attributes": "",
              "limit": 0,
              "required": false
            },
            "NAME": {
              "label": "Geographic Area Name",
              "concept": "Census API Geography Specification",
              "predicateType": "string",
              "group": "N/A",
              "attributes": "",
              "limit": 0,
              "required": false
            },
            "state": {
              "label": "State",
              "concept": "Census API Geography Specification",
              "predicateType": "fips-for",
              "group": "N/A",
              "attributes": "",
              "limit": 0,
              "required": "predicate-only"
            }
          }
        }"#;
        match CensusDiscoveryDocument::parse(&request, bytes, CensusParseLimits::default())? {
            CensusDiscoveryDocument::Variables(catalog) => Ok(catalog),
            _ => Err(crate::CensusAdapterError::SchemaDrift),
        }
    }

    fn query(
        metadata: &crate::CensusVariableCatalog,
    ) -> Result<CensusDataQuery, crate::CensusAdapterError> {
        let selection =
            CensusSelection::variables_with_attributes(["B01001_001E", "NAME"], metadata)?;
        let geography = CensusGeography::standard(
            CensusGeographyClause::try_new(
                "state",
                [
                    CensusGeographyCode::try_new("02")?,
                    CensusGeographyCode::try_new("72")?,
                    CensusGeographyCode::try_new("99")?,
                ],
            )?,
            Vec::new(),
        )?;
        CensusDataQuery::try_new(dataset()?, selection, Vec::new(), geography, None)
    }

    fn geography_admission() -> Result<CensusGeographyAdmission, crate::CensusAdapterError> {
        let query = query(&metadata()?)?;
        Ok(CensusGeographyAdmission::Standard {
            for_level: "state".to_owned(),
            geo_level_display: Some(
                SourceIdentifier::try_from("040")
                    .map_err(|_| crate::CensusAdapterError::InvalidComponent)?,
            ),
            requires: Box::new([]),
            wildcard_parents: Box::new([]),
            optional_with_wildcard_for: Box::new([]),
            for_is_wildcard: false,
            grammar_digest: query.request_digest(),
        })
    }

    fn clocks() -> Result<CensusClocks, crate::CensusAdapterError> {
        CensusClocks::local_first_observed(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(101),
            Timestamp::from_unix_nanos(102),
        )
    }

    #[test]
    fn completed_processing_rebinds_page_and_observation_clocks_after_complete_parse()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = metadata()?;
        let query = query(&metadata)?;
        let received_at = Timestamp::from_unix_nanos(100);
        let page = CensusDataPage::parse(
            &query,
            &metadata,
            &geography_admission()?,
            br#"[
              ["B01001_001E", "B01001_001EA", "NAME", "state"],
              ["733391", null, "Alaska", "02"],
              ["-666666666", "(X)", "Puerto Rico", "72"],
              ["", "(X)", "Unknown", "99"]
            ]"#,
            CensusParseLimits::default(),
            CensusClocks::local_first_observed(received_at, received_at, received_at)?,
        )?
        .try_with_completed_processing_clocks(
            Timestamp::from_unix_nanos(101),
            Timestamp::from_unix_nanos(102),
        )?;

        assert!(page.completeness().is_complete());
        assert_eq!(page.clocks().received_at(), received_at);
        assert_eq!(page.clocks().decoded_at(), Timestamp::from_unix_nanos(101));
        assert_eq!(page.clocks().ingested_at(), Timestamp::from_unix_nanos(102));
        assert_eq!(
            page.clocks().availability().conservative_available_at(),
            Some(received_at)
        );
        assert_eq!(page.observations().len(), 3);
        assert!(
            page.observations()
                .iter()
                .all(|observation| observation.clocks() == page.clocks())
        );
        Ok(())
    }

    #[test]
    fn metadata_drives_exact_typing_and_partial_missing_accounting()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = metadata()?;
        let query = query(&metadata)?;
        let complete = br#"[
          ["B01001_001E", "B01001_001EA", "NAME", "state"],
          ["733391", null, "Alaska", "02"],
          ["-666666666", "(X)", "Puerto Rico", "72"],
          ["", "(X)", "Unknown", "99"]
        ]"#;
        let page = CensusDataPage::parse(
            &query,
            &metadata,
            &geography_admission()?,
            complete,
            CensusParseLimits::default(),
            clocks()?,
        )?;
        assert!(page.completeness().is_complete());
        assert_eq!(page.accounting().returned_rows(), 3);
        assert_eq!(page.accounting().observations(), 3);
        assert_eq!(page.accounting().observed_values(), 1);
        assert_eq!(page.accounting().annotated_values(), 1);
        assert_eq!(page.accounting().missing_values(), 1);
        assert_eq!(
            page.observations()[0].value(),
            &CensusValueState::Observed {
                value: CensusTypedValue::Integer(733_391)
            }
        );
        assert!(matches!(
            page.observations()[1].value(),
            CensusValueState::Annotated { .. }
        ));
        assert!(matches!(
            page.observations()[2].value(),
            CensusValueState::Missing { .. }
        ));

        let partial = br#"[
          ["B01001_001E", "NAME", "state"],
          ["733391", "Alaska", "02"]
        ]"#;
        let page = CensusDataPage::parse(
            &query,
            &metadata,
            &geography_admission()?,
            partial,
            CensusParseLimits::default(),
            clocks()?,
        )?;
        assert!(!page.completeness().is_complete());
        assert_eq!(page.accounting().missing_requested_variables(), 1);
        assert_eq!(page.accounting().observations(), 1);
        assert_eq!(page.accounting().missing_values(), 1);
        let annotation = SourceIdentifier::try_from("B01001_001EA")?;
        assert!(page.completeness().issues().contains(
            &CensusCompletenessIssue::MissingRequestedVariable {
                variable: annotation.clone()
            }
        ));
        assert!(page.completeness().issues().contains(
            &CensusCompletenessIssue::MissingDeclaredAttribute {
                variable: SourceIdentifier::try_from("B01001_001E")?,
                attribute: annotation,
            }
        ));
        Ok(())
    }
}
