use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{AvailabilityEvidence, CalendarDate, SourceIdentifier, Timestamp};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::discovery::{CensusPredicateType, CensusVariableCatalog, CensusVariableMetadata};
use crate::query::{CensusDataQuery, CensusGeography, CensusSelection};
use crate::{CensusAdapterError, sha256, update_digest_component};

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
            max_bytes: 16 * 1024 * 1024,
            max_rows: 100_000,
            max_columns: 4_096,
            max_cells: 2_000_000,
            max_metadata_entries: 100_000,
            max_string_bytes: 64 * 1024,
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
        if let Some((year, month)) = parse_year_suffix(value, '-')
            && (1..=12).contains(&month)
        {
            return Ok(Self::Month { year, month });
        }
        if let Some((year, quarter)) = value
            .split_once("-Q")
            .and_then(|(year, quarter)| Some((year.parse().ok()?, quarter.parse().ok()?)))
            && (1..=4).contains(&quarter)
        {
            return Ok(Self::Quarter { year, quarter });
        }
        if let Some(date) = parse_calendar_date(value)? {
            return Ok(Self::CalendarDate { date });
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

/// Exact provider geography attached to one response row.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusGeographyValue {
    /// Standard `for`/`in` hierarchy.
    Standard {
        /// Ordered containing/output coordinates.
        components: Vec<CensusGeographyComponent>,
        /// Fully qualified GEOID when explicitly returned.
        fully_qualified_geoid: Option<String>,
        /// Provider geography name when explicitly returned.
        name: Option<String>,
    },
    /// UCGID result identified by a returned fully qualified GEOID.
    Uniform {
        /// Exact fully qualified provider GEOID.
        fully_qualified_geoid: String,
        /// Provider geography name when explicitly returned.
        name: Option<String>,
    },
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
    /// A standard FIPS geography column was absent.
    MissingGeographyLevel { level: String },
    /// A UCGID response did not return `GEO_ID`, so rows could not be assigned safely.
    MissingUniformGeographyId,
    /// One response row lacked a required geography code.
    MissingRowGeography { row_number: usize, level: String },
    /// One exact UCGID result did not match any individually requested geography.
    UnexpectedUniformGeography { row_number: usize, geoid: String },
    /// One present cell did not conform to metadata-declared typing.
    InvalidTypedValue {
        /// Provider row number.
        row_number: usize,
        /// Variable identity.
        variable: SourceIdentifier,
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
        if bytes.len() > limits.max_bytes {
            return Err(CensusAdapterError::BodyTooLarge);
        }
        let matrix = serde_json::from_slice::<Value>(bytes)
            .map_err(|_| CensusAdapterError::InvalidJson)?
            .as_array()
            .cloned()
            .ok_or(CensusAdapterError::SchemaDrift)?;
        if matrix.is_empty() {
            return Err(CensusAdapterError::SchemaDrift);
        }
        let header_values = matrix[0]
            .as_array()
            .ok_or(CensusAdapterError::SchemaDrift)?;
        if header_values.is_empty() || header_values.len() > limits.max_columns {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        let data_rows = matrix.len() - 1;
        if data_rows > limits.max_rows
            || data_rows
                .checked_mul(header_values.len())
                .is_none_or(|cells| cells > limits.max_cells)
        {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        let header = header_values
            .iter()
            .map(|value| {
                bounded_cell_string(value, limits).and_then(|value| {
                    SourceIdentifier::try_from(value).map_err(|_| CensusAdapterError::SchemaDrift)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if header.iter().collect::<BTreeSet<_>>().len() != header.len() {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        let header_index = header
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let rows = matrix[1..]
            .iter()
            .map(|row| {
                let row = row.as_array().ok_or(CensusAdapterError::SchemaDrift)?;
                if row.len() != header.len() {
                    return Err(CensusAdapterError::SchemaDrift);
                }
                row.iter()
                    .map(|value| CensusCell::parse(value, limits))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let response_payload_digest = sha256(bytes);
        let metadata_payload_digest = metadata.evidence().payload_digest();
        let predicates = query
            .predicates()
            .iter()
            .map(|predicate| CensusPredicateValue {
                variable: predicate.variable().clone(),
                predicate_type: predicate.predicate_type().clone(),
                values: predicate.values().to_vec(),
            })
            .collect::<Vec<_>>();
        let mut issues = BTreeSet::new();
        let expected_wire = expected_wire_variables(query, metadata)?;
        let primary = primary_variables(query, metadata, &header)?;
        let expected_set = expected_wire
            .iter()
            .map(SourceIdentifier::as_str)
            .collect::<BTreeSet<_>>();
        let geography_levels = query.geography().required_response_levels();
        let geography_set = geography_levels.iter().copied().collect::<BTreeSet<_>>();

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
            if !header_index.contains_key(level) {
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
            let allowed_context =
                geography_set.contains(name) || matches!(name, "GEO_ID" | "NAME" | "time");
            if !expected_set.contains(name) && !allowed_context {
                issues.insert(CensusCompletenessIssue::UnexpectedColumn {
                    column: column.clone(),
                });
            }
        }

        let mut observations = Vec::new();
        let mut usable_rows = 0_usize;
        let mut skipped_rows = 0_usize;
        let mut observed_values = 0_usize;
        let mut missing_values = 0_usize;
        let mut annotated_values = 0_usize;
        let mut invalid_values = 0_usize;
        for (row_index, row) in rows.iter().enumerate() {
            let row_number = row_index + 1;
            let Some(geography) =
                row_geography(query, &header_index, row, row_number, &mut issues)?
            else {
                skipped_rows = skipped_rows
                    .checked_add(1)
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
                continue;
            };
            usable_rows = usable_rows
                .checked_add(1)
                .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
            let reported_time = header_index
                .get("time")
                .and_then(|index| row[*index].nonempty_text())
                .map(CensusReportedTime::parse)
                .transpose()?;
            for variable in &primary {
                let Some(value_index) = header_index.get(variable.as_str()).copied() else {
                    continue;
                };
                let variable_metadata = metadata
                    .get(variable.as_str())
                    .ok_or(CensusAdapterError::MetadataMismatch)?;
                let (value, missing_attribute) =
                    value_state(variable_metadata, &header_index, row, metadata)?;
                if let Some(attribute) = missing_attribute {
                    issues.insert(CensusCompletenessIssue::MissingDeclaredAttribute {
                        variable: variable.clone(),
                        attribute,
                    });
                }
                match &value {
                    CensusValueState::Observed { .. } => observed_values += 1,
                    CensusValueState::Missing { .. } => missing_values += 1,
                    CensusValueState::Annotated { .. } => annotated_values += 1,
                    CensusValueState::Invalid { .. } => {
                        invalid_values += 1;
                        issues.insert(CensusCompletenessIssue::InvalidTypedValue {
                            row_number,
                            variable: variable.clone(),
                        });
                    }
                }
                let row_digest = row_digest(
                    query,
                    row_number,
                    variable,
                    &value,
                    &geography,
                    reported_time.as_ref(),
                    metadata_payload_digest,
                )?;
                let family_digest =
                    family_digest(query, variable, &geography, reported_time.as_ref())?;
                let first_observed_at = clocks
                    .availability()
                    .conservative_available_at()
                    .unwrap_or(clocks.received_at());
                let observation = CensusObservation {
                    dataset: query.dataset().clone(),
                    row_number,
                    variable: variable.clone(),
                    label: variable_metadata.label().to_owned(),
                    concept: variable_metadata.concept().map(str::to_owned),
                    group: variable_metadata.group().cloned(),
                    value,
                    geography: geography.clone(),
                    predicates: predicates.clone(),
                    reported_time: reported_time.clone(),
                    request_digest: query.request_digest(),
                    response_payload_digest,
                    metadata_payload_digest,
                    row_digest,
                    clocks: clocks.clone(),
                    revision: CensusRevisionCandidate {
                        family_digest,
                        content_digest: row_digest,
                        first_observed_at,
                    },
                };
                let _ = value_index;
                observations.push(observation);
            }
        }

        let returned_requested_variables = expected_wire
            .iter()
            .filter(|variable| header_index.contains_key(variable.as_str()))
            .count();
        let missing_requested_variables = expected_wire
            .len()
            .checked_sub(returned_requested_variables)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        let accounting = CensusResponseAccounting {
            requests: 1,
            requested_primary_variables: primary.len(),
            requested_wire_variables: expected_wire.len(),
            returned_columns: header.len(),
            returned_requested_variables,
            missing_requested_variables,
            returned_rows: rows.len(),
            usable_rows,
            skipped_rows,
            observations: observations.len(),
            observed_values,
            missing_values,
            annotated_values,
            invalid_values,
        };
        let completeness = if issues.is_empty() {
            CensusCompleteness::Complete
        } else {
            CensusCompleteness::Partial {
                issues: issues.into_iter().collect(),
            }
        };
        Ok(Self {
            dataset: query.dataset().clone(),
            request_digest: query.request_digest(),
            response_payload_digest,
            metadata_payload_digest,
            header,
            observations,
            completeness,
            pagination: CensusPagination::SingleResponse { request_count: 1 },
            accounting,
            clocks,
        })
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum CensusCell {
    Null,
    Text(String),
    Number(String),
    Boolean(bool),
}

impl CensusCell {
    fn parse(value: &Value, limits: CensusParseLimits) -> Result<Self, CensusAdapterError> {
        match value {
            Value::Null => Ok(Self::Null),
            Value::String(value) => {
                ensure_string(value, limits)?;
                Ok(Self::Text(value.clone()))
            }
            Value::Number(value) => {
                let value = value.to_string();
                ensure_string(&value, limits)?;
                Ok(Self::Number(value))
            }
            Value::Bool(value) => Ok(Self::Boolean(*value)),
            Value::Array(_) | Value::Object(_) => Err(CensusAdapterError::SchemaDrift),
        }
    }

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
    header: &BTreeMap<&str, usize>,
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
    header: &BTreeMap<&str, usize>,
    row: &[CensusCell],
    row_number: usize,
    issues: &mut BTreeSet<CensusCompletenessIssue>,
) -> Result<Option<CensusGeographyValue>, CensusAdapterError> {
    let name = header
        .get("NAME")
        .and_then(|index| row[*index].nonempty_text())
        .map(str::to_owned);
    let returned_geoid = header
        .get("GEO_ID")
        .and_then(|index| row[*index].nonempty_text())
        .map(str::to_owned);
    match query.geography() {
        CensusGeography::Standard { .. } => {
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
                components.push(CensusGeographyComponent {
                    level: level.to_owned(),
                    code: code.to_owned(),
                });
            }
            Ok(Some(CensusGeographyValue::Standard {
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
                fully_qualified_geoid: geoid,
                name,
            }))
        }
    }
}

fn row_digest(
    query: &CensusDataQuery,
    row_number: usize,
    variable: &SourceIdentifier,
    value: &CensusValueState,
    geography: &CensusGeographyValue,
    time: Option<&CensusReportedTime>,
    metadata_digest: [u8; 32],
) -> Result<[u8; 32], CensusAdapterError> {
    let payload = serde_json::to_vec(&(value, geography, time))
        .map_err(|_| CensusAdapterError::SchemaDrift)?;
    let mut hasher = Sha256::new();
    update_digest_component(&mut hasher, b"market-squawk-census-native-row-v1");
    update_digest_component(&mut hasher, &query.request_digest());
    update_digest_component(&mut hasher, &metadata_digest);
    update_digest_component(&mut hasher, variable.as_str().as_bytes());
    update_digest_component(&mut hasher, &(row_number as u64).to_be_bytes());
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
        geography,
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

fn bounded_cell_string(
    value: &Value,
    limits: CensusParseLimits,
) -> Result<&str, CensusAdapterError> {
    let value = value.as_str().ok_or(CensusAdapterError::SchemaDrift)?;
    ensure_string(value, limits)?;
    Ok(value)
}

fn ensure_string(value: &str, limits: CensusParseLimits) -> Result<(), CensusAdapterError> {
    if value.len() > limits.max_string_bytes || value.chars().any(char::is_control) {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    Ok(())
}

fn parse_year_suffix(value: &str, separator: char) -> Option<(u16, u8)> {
    let (year, suffix) = value.split_once(separator)?;
    let year = year.parse::<u16>().ok()?;
    if !(1000..=9999).contains(&year) {
        return None;
    }
    let suffix = suffix.strip_prefix('Q').unwrap_or(suffix);
    Some((year, suffix.parse::<u8>().ok()?))
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
        CensusDiscoveryKind, CensusDiscoveryRequest, CensusGeography, CensusGeographyClause,
        CensusGeographyCode, CensusParseLimits, CensusSelection, CensusValueState,
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
            CensusGeographyClause::try_new("state", [CensusGeographyCode::try_new("*")?])?,
            Vec::new(),
        )?;
        CensusDataQuery::try_new(dataset()?, selection, Vec::new(), geography, None)
    }

    fn clocks() -> Result<CensusClocks, crate::CensusAdapterError> {
        CensusClocks::local_first_observed(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(101),
            Timestamp::from_unix_nanos(102),
        )
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
