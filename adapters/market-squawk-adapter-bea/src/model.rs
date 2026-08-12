//! Closed source-native BEA metadata, observation, and completeness contracts.

use std::collections::BTreeMap;

use market_squawk_domain::Timestamp;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::{BeaError, BeaMethod};

const MAX_IDENTIFIER_BYTES: usize = 128;

/// One validated provider dataset name discovered through `GetDatasetList`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BeaDatasetIdentity(String);

impl BeaDatasetIdentity {
    /// Builds a bounded dataset identity suitable for an exact BEA query.
    pub fn try_new(value: impl Into<String>) -> Result<Self, BeaError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    /// Returns the provider dataset name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One validated provider parameter name discovered through `GetParameterList`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BeaParameterIdentity(String);

impl BeaParameterIdentity {
    /// Builds a bounded BEA parameter identity.
    pub fn try_new(value: impl Into<String>) -> Result<Self, BeaError> {
        let value = value.into();
        validate_identifier(&value)?;
        Ok(Self(value))
    }

    /// Returns the provider parameter name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_identifier(value: &str) -> Result<(), BeaError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || !value.is_ascii()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(BeaError::InvalidRequest);
    }
    Ok(())
}

/// Immutable digest of the exact admitted dataset/parameter/value discovery pages.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BeaMetadataGeneration([u8; 32]);

impl BeaMetadataGeneration {
    /// Commits to one or more exact metadata response digests in deterministic page order.
    ///
    /// # Errors
    ///
    /// Rejects an empty discovery set.
    pub fn from_response_digests(digests: &[[u8; 32]]) -> Result<Self, BeaError> {
        if digests.is_empty() {
            return Err(BeaError::InvalidRequest);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-metadata-generation-v1");
        hasher.update(
            u64::try_from(digests.len())
                .map_err(|_| BeaError::InvalidRequest)?
                .to_be_bytes(),
        );
        for digest in digests {
            hasher.update(digest);
        }
        Ok(Self(hasher.finalize().into()))
    }

    /// Returns the exact SHA-256 commitment.
    pub const fn digest(self) -> [u8; 32] {
        self.0
    }
}

/// One dataset advertised by BEA metadata discovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDatasetDefinition {
    identity: BeaDatasetIdentity,
    description: String,
}

impl BeaDatasetDefinition {
    pub(crate) const fn new(identity: BeaDatasetIdentity, description: String) -> Self {
        Self {
            identity,
            description,
        }
    }

    /// Returns the exact provider dataset identity.
    pub const fn identity(&self) -> &BeaDatasetIdentity {
        &self.identity
    }

    /// Returns the bounded provider description.
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// BEA parameter type returned by `GetParameterList`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaParameterDataType {
    /// Provider string parameter.
    String,
    /// Provider integer parameter.
    Integer,
}

/// One exact parameter contract for a discovered dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaParameterDefinition {
    identity: BeaParameterIdentity,
    data_type: BeaParameterDataType,
    description: String,
    required: bool,
    multiple_values: bool,
    default_value: Option<String>,
    all_value: Option<String>,
}

impl BeaParameterDefinition {
    #[allow(
        clippy::too_many_arguments,
        reason = "the complete provider parameter contract remains explicit"
    )]
    pub(crate) const fn new(
        identity: BeaParameterIdentity,
        data_type: BeaParameterDataType,
        description: String,
        required: bool,
        multiple_values: bool,
        default_value: Option<String>,
        all_value: Option<String>,
    ) -> Self {
        Self {
            identity,
            data_type,
            description,
            required,
            multiple_values,
            default_value,
            all_value,
        }
    }

    /// Returns the exact request parameter identity.
    pub const fn identity(&self) -> &BeaParameterIdentity {
        &self.identity
    }

    /// Returns the provider-declared parameter data type.
    pub const fn data_type(&self) -> BeaParameterDataType {
        self.data_type
    }

    /// Returns the bounded provider description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns whether the provider requires this parameter.
    pub const fn is_required(&self) -> bool {
        self.required
    }

    /// Returns whether comma-separated values are admitted by the provider.
    pub const fn accepts_multiple_values(&self) -> bool {
        self.multiple_values
    }

    /// Returns the provider default when supplied.
    pub fn default_value(&self) -> Option<&str> {
        self.default_value.as_deref()
    }

    /// Returns the provider's dataset-specific all-values marker when supplied.
    pub fn all_value(&self) -> Option<&str> {
        self.all_value.as_deref()
    }
}

/// One valid value advertised for a dataset parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaParameterValueDefinition {
    key: String,
    description: Option<String>,
    attributes: BTreeMap<String, String>,
}

impl BeaParameterValueDefinition {
    pub(crate) const fn new(
        key: String,
        description: Option<String>,
        attributes: BTreeMap<String, String>,
    ) -> Self {
        Self {
            key,
            description,
            attributes,
        }
    }

    /// Returns the exact value to send in a later request.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the provider description when supplied.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns additional bounded scalar metadata without promoting it to canonical macro data.
    pub const fn attributes(&self) -> &BTreeMap<String, String> {
        &self.attributes
    }
}

/// Closed metadata method result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeaMetadataRecords {
    /// Dataset catalog rows.
    Datasets(Vec<BeaDatasetDefinition>),
    /// Parameter contracts for one dataset.
    Parameters(Vec<BeaParameterDefinition>),
    /// Unfiltered or filtered parameter values.
    ParameterValues(Vec<BeaParameterValueDefinition>),
}

impl BeaMetadataRecords {
    /// Returns the number of provider-returned metadata records.
    pub fn len(&self) -> usize {
        match self {
            Self::Datasets(values) => values.len(),
            Self::Parameters(values) => values.len(),
            Self::ParameterValues(values) => values.len(),
        }
    }

    /// Returns whether no records were returned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Scope completeness for one application-planned BEA response page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaCompleteness {
    /// Returned rows exactly equal a metadata-derived expected count.
    Complete,
    /// A known expected count was not fully returned.
    Partial,
    /// The body is structurally complete, but BEA publishes no total-count or pagination field.
    ExpectedCountUnknown,
}

/// Credential-free request, response, count, and application-page evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaPageReceipt {
    request_digest: [u8; 32],
    response_digest: [u8; 32],
    response_bytes: usize,
    page_number: u32,
    page_count: u32,
    requested_rows: Option<usize>,
    returned_rows: usize,
    missing_rows: Option<usize>,
    completeness: BeaCompleteness,
}

impl BeaPageReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "request, response, page, and cardinality evidence must stay explicit"
    )]
    pub(crate) const fn new(
        request_digest: [u8; 32],
        response_digest: [u8; 32],
        response_bytes: usize,
        page_number: u32,
        page_count: u32,
        requested_rows: Option<usize>,
        returned_rows: usize,
        missing_rows: Option<usize>,
        completeness: BeaCompleteness,
    ) -> Self {
        Self {
            request_digest,
            response_digest,
            response_bytes,
            page_number,
            page_count,
            requested_rows,
            returned_rows,
            missing_rows,
            completeness,
        }
    }

    /// Returns the credential-free request digest, including application page scope.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns SHA-256 of the exact response body.
    pub const fn response_digest(&self) -> [u8; 32] {
        self.response_digest
    }

    /// Returns exact received response bytes.
    pub const fn response_bytes(&self) -> usize {
        self.response_bytes
    }

    /// Returns the one-based application-planned page number.
    pub const fn page_number(&self) -> u32 {
        self.page_number
    }

    /// Returns the complete application-planned page count.
    pub const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Returns the metadata-derived expected row count when known.
    pub const fn requested_rows(&self) -> Option<usize> {
        self.requested_rows
    }

    /// Returns validated provider rows, not requested symbols or selector slots.
    pub const fn returned_rows(&self) -> usize {
        self.returned_rows
    }

    /// Returns the exact missing-row count when an expected cardinality was known.
    pub const fn missing_rows(&self) -> Option<usize> {
        self.missing_rows
    }

    /// Returns the closed completeness state.
    pub const fn completeness(&self) -> BeaCompleteness {
        self.completeness
    }
}

/// One validated metadata response and its exact evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaMetadataPage {
    method: BeaMethod,
    records: BeaMetadataRecords,
    receipt: BeaPageReceipt,
}

impl BeaMetadataPage {
    pub(crate) const fn new(
        method: BeaMethod,
        records: BeaMetadataRecords,
        receipt: BeaPageReceipt,
    ) -> Self {
        Self {
            method,
            records,
            receipt,
        }
    }

    /// Returns the exact metadata method.
    pub const fn method(&self) -> BeaMethod {
        self.method
    }

    /// Returns the closed metadata records.
    pub const fn records(&self) -> &BeaMetadataRecords {
        &self.records
    }

    /// Returns request/response/page completeness evidence.
    pub const fn receipt(&self) -> &BeaPageReceipt {
        &self.receipt
    }
}

/// Data dimension scalar type declared by a BEA response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaDataType {
    /// String dimension.
    String,
    /// Numeric dimension.
    Numeric,
}

/// One named BEA response dimension, addressed by name rather than JSON member order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDimension {
    name: String,
    ordinal: Option<u16>,
    data_type: BeaDataType,
    is_value: bool,
}

impl BeaDimension {
    pub(crate) const fn new(
        name: String,
        ordinal: Option<u16>,
        data_type: BeaDataType,
        is_value: bool,
    ) -> Self {
        Self {
            name,
            ordinal,
            data_type,
            is_value,
        }
    }

    /// Returns the exact provider dimension name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the provider ordinal when supplied.
    pub const fn ordinal(&self) -> Option<u16> {
        self.ordinal
    }

    /// Returns the declared scalar type.
    pub const fn data_type(&self) -> BeaDataType {
        self.data_type
    }

    /// Returns whether this is the one value-bearing dimension.
    pub const fn is_value(&self) -> bool {
        self.is_value
    }
}

/// Annual, quarterly, or monthly frequency carried by a BEA `TimePeriod`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeaFrequency {
    /// Calendar/reference year.
    Annual,
    /// Calendar/reference quarter.
    Quarterly,
    /// Calendar/reference month.
    Monthly,
}

/// A precision-preserving BEA observation period.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaTimePeriod {
    raw: String,
    year: u16,
    frequency: BeaFrequency,
    ordinal: u8,
}

impl BeaTimePeriod {
    pub(crate) const fn new(raw: String, year: u16, frequency: BeaFrequency, ordinal: u8) -> Self {
        Self {
            raw,
            year,
            frequency,
            ordinal,
        }
    }

    /// Returns the exact provider lexical period.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the provider period year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns the closed frequency.
    pub const fn frequency(&self) -> BeaFrequency {
        self.frequency
    }

    /// Returns 1 for annual, or the one-based quarter/month ordinal.
    pub const fn ordinal(&self) -> u8 {
        self.ordinal
    }
}

/// Explicit row-level missing evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeaMissingValue {
    /// The value dimension was absent or JSON null.
    Absent,
    /// The provider supplied an empty lexical value.
    Blank,
}

/// Exact observed decimal or explicit provider missing state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeaObservationValue {
    /// Exact base value as returned, before applying `UNIT_MULT`.
    Observed {
        /// Exact decimal after removing only valid thousands separators.
        value: Decimal,
        /// Exact provider lexical representation.
        raw: String,
    },
    /// Explicit nonzero-distinct missing state.
    Missing(BeaMissingValue),
}

impl BeaObservationValue {
    /// Returns an exact value when observed.
    pub const fn observed(&self) -> Option<Decimal> {
        match self {
            Self::Observed { value, .. } => Some(*value),
            Self::Missing(_) => None,
        }
    }

    /// Returns the provider lexical value when observed.
    pub fn raw(&self) -> Option<&str> {
        match self {
            Self::Observed { raw, .. } => Some(raw),
            Self::Missing(_) => None,
        }
    }
}

/// BEA calculation unit and base-10 scale (`value × 10^UNIT_MULT`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaUnit {
    cl_unit: String,
    unit_multiplier: i16,
}

impl BeaUnit {
    pub(crate) const fn new(cl_unit: String, unit_multiplier: i16) -> Self {
        Self {
            cl_unit,
            unit_multiplier,
        }
    }

    /// Returns the exact `CL_UNIT` value.
    pub fn cl_unit(&self) -> &str {
        &self.cl_unit
    }

    /// Returns the exact base-10 exponent from `UNIT_MULT`.
    pub const fn unit_multiplier(&self) -> i16 {
        self.unit_multiplier
    }
}

/// One provider note retained by exact reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaNote {
    reference: String,
    text: String,
}

impl BeaNote {
    pub(crate) const fn new(reference: String, text: String) -> Self {
        Self { reference, text }
    }

    /// Returns the exact `NoteRef` token, which may be blank for a result-wide release note.
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Returns the exact bounded provider text.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// Exact response production instant reported in UTC by BEA.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaProductionTime {
    raw: String,
    timestamp: Timestamp,
}

impl BeaProductionTime {
    pub(crate) const fn new(raw: String, timestamp: Timestamp) -> Self {
        Self { raw, timestamp }
    }

    /// Returns the exact provider lexical timestamp.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns the parsed UTC instant without changing precision evidence.
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Exact dataset/table/line/dimension identity for a provider-native row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaObservationIdentity {
    dataset: BeaDatasetIdentity,
    table: Option<String>,
    line: Option<String>,
    dimensions: BTreeMap<String, String>,
    digest: [u8; 32],
}

impl BeaObservationIdentity {
    pub(crate) fn new(
        dataset: BeaDatasetIdentity,
        table: Option<String>,
        line: Option<String>,
        dimensions: BTreeMap<String, String>,
    ) -> Result<Self, BeaError> {
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-observation-identity-v1");
        hash_text(&mut hasher, dataset.as_str())?;
        hash_optional_text(&mut hasher, table.as_deref())?;
        hash_optional_text(&mut hasher, line.as_deref())?;
        hasher.update(
            u64::try_from(dimensions.len())
                .map_err(|_| BeaError::InvalidField("dimensions"))?
                .to_be_bytes(),
        );
        for (name, value) in &dimensions {
            hash_text(&mut hasher, name)?;
            hash_text(&mut hasher, value)?;
        }
        Ok(Self {
            dataset,
            table,
            line,
            dimensions,
            digest: hasher.finalize().into(),
        })
    }

    /// Returns the discovered dataset identity.
    pub const fn dataset(&self) -> &BeaDatasetIdentity {
        &self.dataset
    }

    /// Returns the exact table identity when the selected dataset supplies one.
    pub fn table(&self) -> Option<&str> {
        self.table.as_deref()
    }

    /// Returns the exact line/series identity when supplied.
    pub fn line(&self) -> Option<&str> {
        self.line.as_deref()
    }

    /// Returns every exact non-value dimension member.
    pub const fn dimensions(&self) -> &BTreeMap<String, String> {
        &self.dimensions
    }

    /// Returns the deterministic provider-native identity digest.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// One closed provider-native BEA macro row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaObservation {
    identity: BeaObservationIdentity,
    period: BeaTimePeriod,
    value: BeaObservationValue,
    unit: BeaUnit,
    note_references: Vec<String>,
    digest: [u8; 32],
}

impl BeaObservation {
    pub(crate) fn new(
        identity: BeaObservationIdentity,
        period: BeaTimePeriod,
        value: BeaObservationValue,
        unit: BeaUnit,
        note_references: Vec<String>,
    ) -> Result<Self, BeaError> {
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-observation-v1");
        hasher.update(identity.digest());
        hash_text(&mut hasher, period.raw())?;
        match &value {
            BeaObservationValue::Observed { value, raw } => {
                hasher.update([1]);
                hash_text(&mut hasher, &value.to_string())?;
                hash_text(&mut hasher, raw)?;
            }
            BeaObservationValue::Missing(BeaMissingValue::Absent) => hasher.update([2]),
            BeaObservationValue::Missing(BeaMissingValue::Blank) => hasher.update([3]),
        }
        hash_text(&mut hasher, unit.cl_unit())?;
        hasher.update(unit.unit_multiplier().to_be_bytes());
        hasher.update(
            u64::try_from(note_references.len())
                .map_err(|_| BeaError::InvalidField("note references"))?
                .to_be_bytes(),
        );
        for reference in &note_references {
            hash_text(&mut hasher, reference)?;
        }
        Ok(Self {
            identity,
            period,
            value,
            unit,
            note_references,
            digest: hasher.finalize().into(),
        })
    }

    /// Returns the exact provider-native identity.
    pub const fn identity(&self) -> &BeaObservationIdentity {
        &self.identity
    }

    /// Returns the precision-preserving observation period.
    pub const fn period(&self) -> &BeaTimePeriod {
        &self.period
    }

    /// Returns the exact value-or-missing state.
    pub const fn value(&self) -> &BeaObservationValue {
        &self.value
    }

    /// Returns source unit and scale evidence.
    pub const fn unit(&self) -> &BeaUnit {
        &self.unit
    }

    /// Returns validated row note references.
    pub fn note_references(&self) -> &[String] {
        &self.note_references
    }

    /// Returns the complete provider-native row digest.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// One complete bounded BEA data response page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaDataPage {
    dataset: BeaDatasetIdentity,
    metadata_generation: BeaMetadataGeneration,
    result_attributes: BTreeMap<String, String>,
    dimensions: Vec<BeaDimension>,
    observations: Vec<BeaObservation>,
    notes: Vec<BeaNote>,
    result_note_references: Vec<String>,
    production_time: Option<BeaProductionTime>,
    receipt: BeaPageReceipt,
}

impl BeaDataPage {
    #[allow(
        clippy::too_many_arguments,
        reason = "data, metadata, release, notes, and completeness remain separate evidence"
    )]
    pub(crate) const fn new(
        dataset: BeaDatasetIdentity,
        metadata_generation: BeaMetadataGeneration,
        result_attributes: BTreeMap<String, String>,
        dimensions: Vec<BeaDimension>,
        observations: Vec<BeaObservation>,
        notes: Vec<BeaNote>,
        result_note_references: Vec<String>,
        production_time: Option<BeaProductionTime>,
        receipt: BeaPageReceipt,
    ) -> Self {
        Self {
            dataset,
            metadata_generation,
            result_attributes,
            dimensions,
            observations,
            notes,
            result_note_references,
            production_time,
            receipt,
        }
    }

    /// Returns the dataset selected by the exact echoed request.
    pub const fn dataset(&self) -> &BeaDatasetIdentity {
        &self.dataset
    }

    /// Returns the exact discovery generation used to validate/build this request.
    pub const fn metadata_generation(&self) -> BeaMetadataGeneration {
        self.metadata_generation
    }

    /// Returns bounded result-level scalars not promoted to canonical macro fields.
    pub const fn result_attributes(&self) -> &BTreeMap<String, String> {
        &self.result_attributes
    }

    /// Returns dimensions in provider order; consumers address them by `name`.
    pub fn dimensions(&self) -> &[BeaDimension] {
        &self.dimensions
    }

    /// Returns exact validated provider rows.
    pub fn observations(&self) -> &[BeaObservation] {
        &self.observations
    }

    /// Returns every response note, including blank-reference release notes.
    pub fn notes(&self) -> &[BeaNote] {
        &self.notes
    }

    /// Returns result-wide note references.
    pub fn result_note_references(&self) -> &[String] {
        &self.result_note_references
    }

    /// Returns provider UTC production time when supplied and valid.
    pub const fn production_time(&self) -> Option<&BeaProductionTime> {
        self.production_time.as_ref()
    }

    /// Returns request/response/page completeness evidence.
    pub const fn receipt(&self) -> &BeaPageReceipt {
        &self.receipt
    }
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaError::InvalidField("identity"))?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}

fn hash_optional_text(hasher: &mut Sha256, value: Option<&str>) -> Result<(), BeaError> {
    match value {
        Some(value) => {
            hasher.update([1]);
            hash_text(hasher, value)
        }
        None => {
            hasher.update([0]);
            Ok(())
        }
    }
}
