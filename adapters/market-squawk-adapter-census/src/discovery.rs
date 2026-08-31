use std::collections::{BTreeMap, BTreeSet};

use market_squawk_domain::{CalendarDate, SourceIdentifier};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::query::{
    CensusDataset, CensusDatasetVintage, CensusDiscoveryKind, CensusDiscoveryRequest,
    CensusGeography, CensusGeographyCode,
};
use crate::response::CensusParseLimits;
use crate::{CensusAdapterError, sha256};

/// Closed, payload-free predicate identifying one failed dataset-catalog validation step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CensusCatalogFailurePredicate {
    /// Exact response bytes exceeded the configured parser bound.
    BodyBound,
    /// Response bytes were not valid JSON.
    JsonSyntax,
    /// The JSON root was not an object.
    RootObject,
    /// The root omitted the required dataset array.
    DatasetArray,
    /// The dataset array exceeded the configured entry bound.
    EntryBound,
    /// Bounded dataset storage could not be allocated.
    Allocation,
    /// A dataset-array member was not an object.
    EntryObject,
    /// A dataset entry omitted its vintage.
    VintageMissing,
    /// A dataset entry supplied a null vintage.
    VintageNull,
    /// A dataset entry supplied an unrecognized bounded string vintage.
    VintageStringVocabulary,
    /// A dataset entry supplied a numeric vintage that was not an unsigned integer.
    VintageNumberShape,
    /// A dataset entry supplied an unsigned vintage outside the supported integer range.
    VintageNumberRange,
    /// A dataset entry supplied a vintage with an unsupported JSON type.
    VintageType,
    /// A vintage-scoped catalog returned a different vintage.
    VintageMismatch,
    /// A dataset entry omitted or malformed its route path.
    DatasetPath,
    /// Two catalog entries resolved to the same dataset identity.
    DuplicateDataset,
    /// A dataset entry omitted or malformed its title.
    Title,
    /// A dataset entry omitted or malformed its description.
    Description,
    /// A dataset entry malformed its optional API distribution.
    Distribution,
    /// A dataset entry malformed its optional variables link.
    VariablesLink,
    /// A dataset entry malformed its optional groups link.
    GroupsLink,
    /// A dataset entry malformed its optional geography link.
    GeographyLink,
    /// A dataset entry malformed its optional availability flag.
    AvailableFlag,
    /// A dataset entry malformed its optional aggregate flag.
    AggregateFlag,
    /// A dataset entry malformed its optional time-series flag.
    TimeSeriesFlag,
    /// A non-catalog request was supplied to the catalog-only parser.
    RequestKind,
}

/// Closed, payload-free predicate identifying one failed geography validation step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CensusGeographyFailurePredicate {
    /// Exact response bytes exceeded the configured parser bound.
    BodyBound,
    /// Response bytes were not valid JSON.
    JsonSyntax,
    /// The JSON root was not an object.
    RootObject,
    /// The root omitted the required FIPS array.
    FipsArray,
    /// The FIPS array exceeded the configured entry bound.
    EntryBound,
    /// Bounded geography storage could not be allocated.
    Allocation,
    /// A FIPS-array member was not an object.
    EntryObject,
    /// A geography entry omitted or malformed its name.
    Name,
    /// A geography entry malformed its optional summary-level display metadata.
    GeoLevelDisplay,
    /// Two geography entries resolved to the same identity.
    DuplicateIdentity,
    /// A geography entry malformed its required parent list.
    Requires,
    /// A geography entry malformed its wildcard parent list.
    Wildcard,
    /// A geography entry malformed its optional wildcard-parent list.
    OptionalWildcard,
    /// Wildcard metadata referred to a parent outside the entry's required set.
    ParentGrammar,
    /// A geography entry malformed its optional reference date.
    ReferenceDate,
    /// The complete geography graph referred to an unknown parent.
    Closure,
    /// A non-geography request was supplied to the geography-only parser.
    RequestKind,
}

#[derive(Debug)]
pub(crate) struct CensusCatalogParseFailure {
    source: CensusAdapterError,
    predicate: CensusCatalogFailurePredicate,
}

impl CensusCatalogParseFailure {
    fn new(source: CensusAdapterError, predicate: CensusCatalogFailurePredicate) -> Self {
        Self { source, predicate }
    }

    pub(crate) const fn predicate(&self) -> CensusCatalogFailurePredicate {
        self.predicate
    }

    pub(crate) fn into_source(self) -> CensusAdapterError {
        self.source
    }
}

#[derive(Debug)]
pub(crate) struct CensusGeographyParseFailure {
    source: CensusAdapterError,
    predicate: CensusGeographyFailurePredicate,
}

impl CensusGeographyParseFailure {
    fn new(source: CensusAdapterError, predicate: CensusGeographyFailurePredicate) -> Self {
        Self { source, predicate }
    }

    pub(crate) const fn predicate(&self) -> CensusGeographyFailurePredicate {
        self.predicate
    }

    pub(crate) fn into_source(self) -> CensusAdapterError {
        self.source
    }
}

fn catalog_failure(
    predicate: CensusCatalogFailurePredicate,
) -> impl FnOnce(CensusAdapterError) -> CensusCatalogParseFailure {
    move |source| CensusCatalogParseFailure::new(source, predicate)
}

fn geography_failure(
    predicate: CensusGeographyFailurePredicate,
) -> impl FnOnce(CensusAdapterError) -> CensusGeographyParseFailure {
    move |source| CensusGeographyParseFailure::new(source, predicate)
}

/// Exact payload identity and single-document accounting for discovery metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusMetadataEvidence {
    payload_digest: [u8; 32],
    payload_bytes: usize,
    returned_entries: usize,
    complete_single_document: bool,
}

impl CensusMetadataEvidence {
    fn new(payload: &[u8], returned_entries: usize) -> Self {
        Self {
            payload_digest: sha256(payload),
            payload_bytes: payload.len(),
            returned_entries,
            complete_single_document: true,
        }
    }

    /// Returns the SHA-256 digest of the exact discovery body.
    pub const fn payload_digest(&self) -> [u8; 32] {
        self.payload_digest
    }

    /// Returns retained source bytes.
    pub const fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    /// Returns the number of decoded metadata entries.
    pub const fn returned_entries(&self) -> usize {
        self.returned_entries
    }

    /// Returns true because Census discovery uses one bounded JSON document, not a cursor.
    pub const fn complete_single_document(&self) -> bool {
        self.complete_single_document
    }
}

/// A decoded machine-readable discovery response tied to the exact request kind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CensusDiscoveryDocument {
    /// Global or vintage-filtered dataset catalog.
    Datasets(CensusDatasetCatalog),
    /// Dataset variable catalog.
    Variables(CensusVariableCatalog),
    /// Dataset group catalog.
    Groups(CensusGroupCatalog),
    /// One group's variable catalog.
    GroupVariables(CensusVariableCatalog),
    /// Dataset FIPS geography grammar.
    Geographies(CensusGeographyCatalog),
}

impl CensusDiscoveryDocument {
    /// Parses only the document shape selected by the request.
    ///
    /// # Errors
    ///
    /// Rejects oversized JSON, structural drift, duplicate identities, invalid provider fields,
    /// or metadata that does not match the requested vintage/dataset/group.
    pub fn parse(
        request: &CensusDiscoveryRequest,
        bytes: &[u8],
        limits: CensusParseLimits,
    ) -> Result<Self, CensusAdapterError> {
        if matches!(
            request.kind(),
            CensusDiscoveryKind::Datasets | CensusDiscoveryKind::VintageDatasets { .. }
        ) {
            return Self::parse_catalog_diagnosed(request, bytes, limits)
                .map_err(CensusCatalogParseFailure::into_source);
        }
        if matches!(request.kind(), CensusDiscoveryKind::Geographies { .. }) {
            return Self::parse_geography_diagnosed(request, bytes, limits)
                .map_err(CensusGeographyParseFailure::into_source);
        }
        ensure_body_bound(bytes, limits)?;
        match request.kind() {
            CensusDiscoveryKind::Datasets | CensusDiscoveryKind::VintageDatasets { .. } => {
                Err(CensusAdapterError::InvalidQuery)
            }
            CensusDiscoveryKind::Variables { dataset } => {
                CensusVariableCatalog::parse_inner(bytes, dataset.clone(), None, limits)
                    .map(Self::Variables)
            }
            CensusDiscoveryKind::Groups { dataset } => {
                CensusGroupCatalog::parse_inner(bytes, dataset.clone(), limits).map(Self::Groups)
            }
            CensusDiscoveryKind::Group { dataset, group } => CensusVariableCatalog::parse_inner(
                bytes,
                dataset.clone(),
                Some(group.clone()),
                limits,
            )
            .map(Self::GroupVariables),
            CensusDiscoveryKind::Geographies { .. } => Err(CensusAdapterError::InvalidQuery),
        }
    }

    pub(crate) fn parse_catalog_diagnosed(
        request: &CensusDiscoveryRequest,
        bytes: &[u8],
        limits: CensusParseLimits,
    ) -> Result<Self, CensusCatalogParseFailure> {
        ensure_body_bound(bytes, limits)
            .map_err(catalog_failure(CensusCatalogFailurePredicate::BodyBound))?;
        let expected_vintage = match request.kind() {
            CensusDiscoveryKind::Datasets => None,
            CensusDiscoveryKind::VintageDatasets { vintage } => Some(*vintage),
            _ => {
                return Err(CensusCatalogParseFailure::new(
                    CensusAdapterError::InvalidQuery,
                    CensusCatalogFailurePredicate::RequestKind,
                ));
            }
        };
        CensusDatasetCatalog::parse_inner_diagnosed(bytes, expected_vintage, limits)
            .map(Self::Datasets)
    }

    pub(crate) fn parse_geography_diagnosed(
        request: &CensusDiscoveryRequest,
        bytes: &[u8],
        limits: CensusParseLimits,
    ) -> Result<Self, CensusGeographyParseFailure> {
        ensure_body_bound(bytes, limits).map_err(geography_failure(
            CensusGeographyFailurePredicate::BodyBound,
        ))?;
        let CensusDiscoveryKind::Geographies { dataset } = request.kind() else {
            return Err(CensusGeographyParseFailure::new(
                CensusAdapterError::InvalidQuery,
                CensusGeographyFailurePredicate::RequestKind,
            ));
        };
        CensusGeographyCatalog::parse_inner_diagnosed(bytes, dataset.clone(), limits)
            .map(Self::Geographies)
    }
}

/// One dataset entry from Census's machine-readable discovery catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusDatasetMetadata {
    dataset: CensusDataset,
    title: String,
    description: String,
    api_base_url: Option<String>,
    variables_url: Option<String>,
    groups_url: Option<String>,
    geography_url: Option<String>,
    available: Option<bool>,
    aggregate: Option<bool>,
    time_series: Option<bool>,
}

impl CensusDatasetMetadata {
    /// Returns the exact dataset coordinate.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns the provider title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the provider description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the catalog's API access URL when supplied.
    pub fn api_base_url(&self) -> Option<&str> {
        self.api_base_url.as_deref()
    }

    /// Returns the variable discovery link when supplied.
    pub fn variables_url(&self) -> Option<&str> {
        self.variables_url.as_deref()
    }

    /// Returns the group discovery link when supplied.
    pub fn groups_url(&self) -> Option<&str> {
        self.groups_url.as_deref()
    }

    /// Returns the geography discovery link when supplied.
    pub fn geography_url(&self) -> Option<&str> {
        self.geography_url.as_deref()
    }

    /// Returns Census's availability marker when present.
    pub const fn available(&self) -> Option<bool> {
        self.available
    }

    /// Returns Census's aggregate-dataset marker when present.
    pub const fn aggregate(&self) -> Option<bool> {
        self.aggregate
    }

    /// Returns Census's time-series marker when present.
    pub const fn time_series(&self) -> Option<bool> {
        self.time_series
    }
}

/// A complete bounded dataset discovery document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusDatasetCatalog {
    evidence: CensusMetadataEvidence,
    datasets: Vec<CensusDatasetMetadata>,
}

impl CensusDatasetCatalog {
    fn parse_inner_diagnosed(
        bytes: &[u8],
        expected_vintage: Option<u16>,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusCatalogParseFailure> {
        let root = parse_object(bytes).map_err(|error| {
            let predicate = if matches!(error, CensusAdapterError::InvalidJson) {
                CensusCatalogFailurePredicate::JsonSyntax
            } else {
                CensusCatalogFailurePredicate::RootObject
            };
            CensusCatalogParseFailure::new(error, predicate)
        })?;
        let entries = root
            .get("dataset")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                CensusCatalogParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusCatalogFailurePredicate::DatasetArray,
                )
            })?;
        ensure_entry_bound(entries.len(), limits)
            .map_err(catalog_failure(CensusCatalogFailurePredicate::EntryBound))?;
        let mut datasets = Vec::new();
        datasets.try_reserve_exact(entries.len()).map_err(|_| {
            CensusCatalogParseFailure::new(
                CensusAdapterError::ResourceLimitExceeded,
                CensusCatalogFailurePredicate::Allocation,
            )
        })?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            let entry = entry.as_object().ok_or_else(|| {
                CensusCatalogParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusCatalogFailurePredicate::EntryObject,
                )
            })?;
            let path = required_string_array(entry, "c_dataset", limits)
                .map_err(catalog_failure(CensusCatalogFailurePredicate::DatasetPath))?;
            let Some(vintage) = catalog_vintage_diagnosed(entry, &path, expected_vintage)? else {
                // The global catalog also contains non-year catalog records. Their bounded path
                // and optional API distribution are retained in the sealed source document, but
                // cannot become a CensusDataset without inventing a vintage coordinate.
                validate_timeless_catalog_entry(entry, limits)?;
                continue;
            };
            if expected_vintage
                .is_some_and(|expected| vintage != CensusDatasetVintage::Year(expected))
            {
                return Err(CensusCatalogParseFailure::new(
                    CensusAdapterError::MetadataMismatch,
                    CensusCatalogFailurePredicate::VintageMismatch,
                ));
            }
            let dataset = match vintage {
                CensusDatasetVintage::Year(year) => CensusDataset::try_new(year, path.join("/")),
                CensusDatasetVintage::TimeSeries => {
                    let path = path
                        .first()
                        .is_some_and(|segment| segment == "timeseries")
                        .then_some(&path[1..])
                        .unwrap_or(path.as_slice());
                    CensusDataset::try_time_series(path.join("/"))
                }
            }
            .map_err(catalog_failure(CensusCatalogFailurePredicate::DatasetPath))?;
            if !identities.insert(dataset.clone()) {
                return Err(CensusCatalogParseFailure::new(
                    CensusAdapterError::DuplicateIdentity,
                    CensusCatalogFailurePredicate::DuplicateDataset,
                ));
            }
            let title = required_text(entry, "title", limits)
                .map_err(catalog_failure(CensusCatalogFailurePredicate::Title))?;
            let description = required_text(entry, "description", limits)
                .map_err(catalog_failure(CensusCatalogFailurePredicate::Description))?;
            let api_base_url = distribution_api_url(entry, limits)
                .map_err(catalog_failure(CensusCatalogFailurePredicate::Distribution))?;
            let variables_url = optional_text(entry, "c_variablesLink", limits).map_err(
                catalog_failure(CensusCatalogFailurePredicate::VariablesLink),
            )?;
            let groups_url = optional_text(entry, "c_groupsLink", limits)
                .map_err(catalog_failure(CensusCatalogFailurePredicate::GroupsLink))?;
            let geography_url = optional_text(entry, "c_geographyLink", limits).map_err(
                catalog_failure(CensusCatalogFailurePredicate::GeographyLink),
            )?;
            let available = optional_bool(entry, "c_isAvailable").map_err(catalog_failure(
                CensusCatalogFailurePredicate::AvailableFlag,
            ))?;
            let aggregate = optional_bool(entry, "c_isAggregate").map_err(catalog_failure(
                CensusCatalogFailurePredicate::AggregateFlag,
            ))?;
            let time_series = optional_bool(entry, "c_isTimeseries").map_err(catalog_failure(
                CensusCatalogFailurePredicate::TimeSeriesFlag,
            ))?;
            datasets.push(CensusDatasetMetadata {
                dataset,
                title,
                description,
                api_base_url,
                variables_url,
                groups_url,
                geography_url,
                available,
                aggregate,
                time_series,
            });
        }
        datasets.sort_by(|left, right| left.dataset.cmp(&right.dataset));
        Ok(Self {
            evidence: CensusMetadataEvidence::new(bytes, entries.len()),
            datasets,
        })
    }

    /// Returns exact discovery evidence.
    pub const fn evidence(&self) -> &CensusMetadataEvidence {
        &self.evidence
    }

    /// Returns datasets in stable coordinate order.
    pub fn datasets(&self) -> &[CensusDatasetMetadata] {
        &self.datasets
    }
}

/// The provider's variable predicate grammar.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "provider_value")]
pub enum CensusPredicateType {
    /// String equality/prefix-wildcard predicate and text response value.
    String,
    /// Integer/range predicate and integer response candidate.
    Integer,
    /// Decimal/range predicate and decimal response candidate.
    Float,
    /// Standard geography output selector.
    FipsFor,
    /// Standard containing-geography selector.
    FipsIn,
    /// UCGID geography selector.
    Ucgid,
    /// Time-series time predicate.
    Time,
    /// Variable is not available as a predicate.
    NotPredicate,
    /// A newly observed provider grammar retained without treating it as typed data.
    Unknown(String),
}

impl CensusPredicateType {
    fn parse(value: Option<&Value>, limits: CensusParseLimits) -> Result<Self, CensusAdapterError> {
        let Some(value) = value else {
            return Ok(Self::NotPredicate);
        };
        let value = bounded_str(value, limits)?;
        Ok(match value {
            "string" => Self::String,
            "int" => Self::Integer,
            "float" => Self::Float,
            "fips-for" => Self::FipsFor,
            "fips-in" => Self::FipsIn,
            "ucgid" => Self::Ucgid,
            "time" | "datetime" => Self::Time,
            "" | "not a predicate" => Self::NotPredicate,
            other => Self::Unknown(other.to_owned()),
        })
    }
}

/// Whether provider metadata requires a variable or admits it only as a predicate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusRequiredVariable {
    /// Required in the request.
    Required,
    /// Optional request variable.
    Optional,
    /// Predicate-only variable.
    PredicateOnly,
    /// Required variable that may appear only as a predicate.
    RequiredPredicateOnly,
    /// Provider displays this variable by default.
    DefaultDisplayed,
    /// Metadata did not establish the state.
    Unspecified,
}

/// One variable metadata entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusVariableMetadata {
    name: SourceIdentifier,
    label: String,
    concept: Option<String>,
    group: Option<SourceIdentifier>,
    predicate_type: CensusPredicateType,
    required: CensusRequiredVariable,
    attributes: Vec<SourceIdentifier>,
    provider_limit: Option<u64>,
}

impl CensusVariableMetadata {
    /// Returns the exact variable identity.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the provider label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Returns the provider concept when supplied.
    pub fn concept(&self) -> Option<&str> {
        self.concept.as_deref()
    }

    /// Returns the provider group when supplied.
    pub const fn group(&self) -> Option<&SourceIdentifier> {
        self.group.as_ref()
    }

    /// Returns the exact provider predicate type.
    pub const fn predicate_type(&self) -> &CensusPredicateType {
        &self.predicate_type
    }

    /// Returns provider-required/predicate-only state.
    pub const fn required(&self) -> CensusRequiredVariable {
        self.required
    }

    /// Returns metadata-declared related attribute variables.
    pub fn attributes(&self) -> &[SourceIdentifier] {
        &self.attributes
    }

    /// Returns the provider-declared variable-specific limit when supplied.
    pub const fn provider_limit(&self) -> Option<u64> {
        self.provider_limit
    }

    /// Returns whether this variable is response context rather than a selected measurement.
    pub fn is_context(&self) -> bool {
        matches!(
            self.predicate_type,
            CensusPredicateType::FipsFor
                | CensusPredicateType::FipsIn
                | CensusPredicateType::Ucgid
                | CensusPredicateType::Time
        ) || matches!(
            self.required,
            CensusRequiredVariable::PredicateOnly | CensusRequiredVariable::RequiredPredicateOnly
        )
    }
}

/// One complete variable or group-variable discovery document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusVariableCatalog {
    dataset: CensusDataset,
    group: Option<SourceIdentifier>,
    evidence: CensusMetadataEvidence,
    variables: BTreeMap<SourceIdentifier, CensusVariableMetadata>,
    attribute_owners: BTreeMap<SourceIdentifier, Vec<SourceIdentifier>>,
}

impl CensusVariableCatalog {
    fn parse_inner(
        bytes: &[u8],
        dataset: CensusDataset,
        expected_group: Option<SourceIdentifier>,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusAdapterError> {
        let root = parse_object(bytes)?;
        let entries = root
            .get("variables")
            .and_then(Value::as_object)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        ensure_entry_bound(entries.len(), limits)?;
        let mut variables = BTreeMap::new();
        for (name, value) in entries {
            let name = identifier(name)?;
            let entry = value.as_object().ok_or(CensusAdapterError::SchemaDrift)?;
            let label = required_text(entry, "label", limits)?;
            let concept =
                optional_text(entry, "concept", limits)?.filter(|value| !value.is_empty());
            let group = optional_text(entry, "group", limits)?
                .filter(|value| value != "N/A" && !value.is_empty())
                .map(|value| identifier(&value))
                .transpose()?;
            if expected_group
                .as_ref()
                .is_some_and(|expected| group.as_ref().is_some_and(|group| group != expected))
            {
                return Err(CensusAdapterError::MetadataMismatch);
            }
            let predicate_type = CensusPredicateType::parse(entry.get("predicateType"), limits)?;
            let required = parse_required(entry)?;
            let attributes = parse_attributes(entry.get("attributes"), limits)?;
            let provider_limit = optional_u64(entry, "limit")?;
            let metadata = CensusVariableMetadata {
                name: name.clone(),
                label,
                concept,
                group,
                predicate_type,
                required,
                attributes,
                provider_limit,
            };
            if variables.insert(name, metadata).is_some() {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
        }
        let mut attribute_owners = BTreeMap::<SourceIdentifier, Vec<SourceIdentifier>>::new();
        let mut attribute_relationships = 0_usize;
        for metadata in variables.values() {
            for attribute in &metadata.attributes {
                if !variables.contains_key(attribute) {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                attribute_relationships = attribute_relationships
                    .checked_add(1)
                    .ok_or(CensusAdapterError::ResourceLimitExceeded)?;
                if attribute_relationships > limits.max_cells() {
                    return Err(CensusAdapterError::ResourceLimitExceeded);
                }
                let owners = attribute_owners.entry(attribute.clone()).or_default();
                owners
                    .try_reserve_exact(1)
                    .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
                owners.push(metadata.name.clone());
            }
        }
        Ok(Self {
            dataset,
            group: expected_group,
            evidence: CensusMetadataEvidence::new(bytes, variables.len()),
            variables,
            attribute_owners,
        })
    }

    /// Returns the exact dataset.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns the group for group-detail discovery.
    pub const fn group(&self) -> Option<&SourceIdentifier> {
        self.group.as_ref()
    }

    /// Returns exact discovery evidence.
    pub const fn evidence(&self) -> &CensusMetadataEvidence {
        &self.evidence
    }

    /// Looks up one exact variable.
    pub fn get(&self, name: &str) -> Option<&CensusVariableMetadata> {
        self.variables
            .values()
            .find(|variable| variable.name.as_str() == name)
    }

    /// Returns variables in stable identity order.
    pub fn variables(&self) -> impl ExactSizeIterator<Item = &CensusVariableMetadata> {
        self.variables.values()
    }

    /// Returns primary variables that declare the named variable as an attribute.
    pub fn attribute_owners(&self, name: &str) -> &[SourceIdentifier] {
        self.attribute_owners
            .iter()
            .find(|(attribute, _)| attribute.as_str() == name)
            .map_or(&[], |(_, owners)| owners.as_slice())
    }
}

/// One group-list entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusGroupMetadata {
    name: SourceIdentifier,
    description: String,
    variables_url: String,
}

impl CensusGroupMetadata {
    /// Returns the exact group identity.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the provider description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the exact provider variable-list URL.
    pub fn variables_url(&self) -> &str {
        &self.variables_url
    }
}

/// One complete group-list discovery document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusGroupCatalog {
    dataset: CensusDataset,
    evidence: CensusMetadataEvidence,
    groups: Vec<CensusGroupMetadata>,
}

impl CensusGroupCatalog {
    fn parse_inner(
        bytes: &[u8],
        dataset: CensusDataset,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusAdapterError> {
        let root = parse_object(bytes)?;
        let entries = root
            .get("groups")
            .and_then(Value::as_array)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        ensure_entry_bound(entries.len(), limits)?;
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(entries.len())
            .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            let entry = entry.as_object().ok_or(CensusAdapterError::SchemaDrift)?;
            let name = identifier(&required_text(entry, "name", limits)?)?;
            if !identities.insert(name.clone()) {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
            groups.push(CensusGroupMetadata {
                name,
                description: required_text(entry, "description", limits)?,
                variables_url: required_text(entry, "variables", limits)?,
            });
        }
        groups.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Self {
            dataset,
            evidence: CensusMetadataEvidence::new(bytes, groups.len()),
            groups,
        })
    }

    /// Returns the exact dataset.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns exact discovery evidence.
    pub const fn evidence(&self) -> &CensusMetadataEvidence {
        &self.evidence
    }

    /// Returns groups in stable identity order.
    pub fn groups(&self) -> &[CensusGroupMetadata] {
        &self.groups
    }
}

/// One geography grammar entry from `geography.json`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusGeographyMetadata {
    name: String,
    geo_level_display: Option<SourceIdentifier>,
    reference_date: Option<CalendarDate>,
    requires: Vec<String>,
    wildcard: Vec<String>,
    optional_with_wildcard_for: Vec<String>,
}

impl CensusGeographyMetadata {
    /// Returns the provider geography name used in `for`/`in` response headers.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional provider summary-level display metadata.
    pub fn geo_level_display(&self) -> Option<&SourceIdentifier> {
        self.geo_level_display.as_ref()
    }

    /// Returns the dataset geography reference date when supplied.
    pub const fn reference_date(&self) -> Option<CalendarDate> {
        self.reference_date
    }

    /// Returns required containing geography names.
    pub fn requires(&self) -> &[String] {
        &self.requires
    }

    /// Returns geography names for which wildcard selection is supported.
    pub fn wildcard(&self) -> &[String] {
        &self.wildcard
    }

    /// Returns optional containing geography names under wildcard `for` selection.
    pub fn optional_with_wildcard_for(&self) -> &[String] {
        &self.optional_with_wildcard_for
    }
}

/// One complete geography discovery document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CensusGeographyCatalog {
    dataset: CensusDataset,
    evidence: CensusMetadataEvidence,
    geographies: Vec<CensusGeographyMetadata>,
}

/// Exact metadata-admitted grammar for one configured geography request.
///
/// The admission binds the selected request to one unambiguous `geography.json` entry. It must be
/// reopened during final response parsing; merely finding a level with the same name is not
/// sufficient production admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusGeographyAdmission {
    /// One standard `for`/`in` grammar compiled from the provider's exact metadata entry.
    Standard {
        for_level: String,
        geo_level_display: Option<SourceIdentifier>,
        requires: Box<[String]>,
        wildcard_parents: Box<[String]>,
        optional_with_wildcard_for: Box<[String]>,
        for_is_wildcard: bool,
        grammar_digest: [u8; 32],
    },
    /// `ucgid` request grammar is bound to the exact geography discovery document. The complete
    /// metadata-bundle validator separately requires the dataset's exact `ucgid` variable entry.
    Uniform { grammar_digest: [u8; 32] },
}

impl CensusGeographyAdmission {
    /// Revalidates the final query against the exact admitted grammar.
    pub fn validate_query(&self, geography: &CensusGeography) -> Result<(), CensusAdapterError> {
        match (self, geography) {
            (
                Self::Standard {
                    for_level,
                    requires,
                    wildcard_parents,
                    optional_with_wildcard_for,
                    for_is_wildcard,
                    grammar_digest,
                    ..
                },
                CensusGeography::Standard {
                    for_clause,
                    in_clauses,
                },
            ) => {
                let actual_for_wildcard = clause_is_wildcard(for_clause.codes())?;
                let required_set = requires.iter().map(String::as_str).collect::<BTreeSet<_>>();
                let wildcard_set = wildcard_parents
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                if for_clause.level() != for_level
                    || actual_for_wildcard != *for_is_wildcard
                    || *grammar_digest == [0; 32]
                    || required_set.len() != requires.len()
                    || wildcard_set.len() != wildcard_parents.len()
                    || wildcard_set
                        .iter()
                        .any(|parent| !required_set.contains(parent))
                {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                let optional = optional_with_wildcard_for
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                if optional.len() != optional_with_wildcard_for.len()
                    || optional.iter().any(|parent| !required_set.contains(parent))
                {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                let supplied = in_clauses
                    .iter()
                    .map(|clause| clause.level())
                    .collect::<Vec<_>>();
                let expected = requires
                    .iter()
                    .map(String::as_str)
                    .filter(|required| {
                        !(*for_is_wildcard && optional.contains(*required))
                            || supplied.contains(required)
                    })
                    .collect::<Vec<_>>();
                if supplied != expected {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                for clause in in_clauses {
                    if clause_is_wildcard(clause.codes())?
                        && !wildcard_parents
                            .iter()
                            .any(|parent| parent == clause.level())
                    {
                        return Err(CensusAdapterError::MetadataMismatch);
                    }
                }
                Ok(())
            }
            (Self::Uniform { grammar_digest }, CensusGeography::Uniform { .. })
                if *grammar_digest != [0; 32] =>
            {
                Ok(())
            }
            _ => Err(CensusAdapterError::MetadataMismatch),
        }
    }

    /// Returns the exact provider-metadata/request grammar identity.
    pub const fn grammar_digest(&self) -> [u8; 32] {
        match self {
            Self::Standard { grammar_digest, .. } | Self::Uniform { grammar_digest } => {
                *grammar_digest
            }
        }
    }
}

impl CensusGeographyCatalog {
    fn parse_inner_diagnosed(
        bytes: &[u8],
        dataset: CensusDataset,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusGeographyParseFailure> {
        let root = parse_object(bytes).map_err(|error| {
            let predicate = if matches!(error, CensusAdapterError::InvalidJson) {
                CensusGeographyFailurePredicate::JsonSyntax
            } else {
                CensusGeographyFailurePredicate::RootObject
            };
            CensusGeographyParseFailure::new(error, predicate)
        })?;
        let entries = root.get("fips").and_then(Value::as_array).ok_or_else(|| {
            CensusGeographyParseFailure::new(
                CensusAdapterError::SchemaDrift,
                CensusGeographyFailurePredicate::FipsArray,
            )
        })?;
        ensure_entry_bound(entries.len(), limits).map_err(geography_failure(
            CensusGeographyFailurePredicate::EntryBound,
        ))?;
        let mut geographies = Vec::new();
        geographies.try_reserve_exact(entries.len()).map_err(|_| {
            CensusGeographyParseFailure::new(
                CensusAdapterError::ResourceLimitExceeded,
                CensusGeographyFailurePredicate::Allocation,
            )
        })?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            let entry = entry.as_object().ok_or_else(|| {
                CensusGeographyParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusGeographyFailurePredicate::EntryObject,
                )
            })?;
            let name = required_text(entry, "name", limits)
                .map_err(geography_failure(CensusGeographyFailurePredicate::Name))?;
            let geo_level_display = optional_text(entry, "geoLevelDisplay", limits)
                .and_then(|value| {
                    value
                        .filter(|value| !value.is_empty())
                        .map(|value| identifier(&value))
                        .transpose()
                })
                .map_err(geography_failure(
                    CensusGeographyFailurePredicate::GeoLevelDisplay,
                ))?;
            if !identities.insert(name.clone()) {
                return Err(CensusGeographyParseFailure::new(
                    CensusAdapterError::DuplicateIdentity,
                    CensusGeographyFailurePredicate::DuplicateIdentity,
                ));
            }
            let requires = optional_string_array(entry, "requires", limits)
                .map_err(geography_failure(CensusGeographyFailurePredicate::Requires))?;
            let wildcard = optional_string_array(entry, "wildcard", limits)
                .map_err(geography_failure(CensusGeographyFailurePredicate::Wildcard))?;
            let optional_with_wildcard_for =
                optional_string_array_or_scalar(entry, "optionalWithWCFor", limits).map_err(
                    geography_failure(CensusGeographyFailurePredicate::OptionalWildcard),
                )?;
            let required_set = requires.iter().map(String::as_str).collect::<BTreeSet<_>>();
            if wildcard
                .iter()
                .chain(&optional_with_wildcard_for)
                .any(|parent| !required_set.contains(parent.as_str()))
            {
                return Err(CensusGeographyParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusGeographyFailurePredicate::ParentGrammar,
                ));
            }
            let reference_date = optional_text(entry, "referenceDate", limits)
                .and_then(|value| value.map(|value| parse_date(&value)).transpose())
                .map_err(geography_failure(
                    CensusGeographyFailurePredicate::ReferenceDate,
                ))?;
            geographies.push(CensusGeographyMetadata {
                name,
                geo_level_display,
                reference_date,
                requires,
                wildcard,
                optional_with_wildcard_for,
            });
        }
        let geography_names = geographies
            .iter()
            .map(|geography| geography.name.as_str())
            .collect::<BTreeSet<_>>();
        if geographies.iter().any(|geography| {
            geography
                .requires
                .iter()
                .any(|parent| !geography_names.contains(parent.as_str()))
        }) {
            return Err(CensusGeographyParseFailure::new(
                CensusAdapterError::MetadataMismatch,
                CensusGeographyFailurePredicate::Closure,
            ));
        }
        geographies.sort_by(|left, right| {
            (&left.name, &left.geo_level_display).cmp(&(&right.name, &right.geo_level_display))
        });
        Ok(Self {
            dataset,
            evidence: CensusMetadataEvidence::new(bytes, geographies.len()),
            geographies,
        })
    }

    /// Returns the exact dataset.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns exact discovery evidence.
    pub const fn evidence(&self) -> &CensusMetadataEvidence {
        &self.evidence
    }

    /// Returns geography grammar entries in stable provider-name/display order.
    pub fn geographies(&self) -> &[CensusGeographyMetadata] {
        &self.geographies
    }

    /// Finds every metadata entry with one exact provider geography name.
    pub fn named<'a>(
        &'a self,
        name: &'a str,
    ) -> impl Iterator<Item = &'a CensusGeographyMetadata> + 'a {
        self.geographies
            .iter()
            .filter(move |geography| geography.name == name)
    }

    /// Compiles one final query geography against exact provider metadata.
    pub fn admit(
        &self,
        geography: &CensusGeography,
    ) -> Result<CensusGeographyAdmission, CensusAdapterError> {
        let admission = match geography {
            CensusGeography::Standard { for_clause, .. } => {
                let mut matches = self.named(for_clause.level());
                let entry = matches.next().ok_or(CensusAdapterError::MetadataMismatch)?;
                if matches.next().is_some() {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                let for_is_wildcard = clause_is_wildcard(for_clause.codes())?;
                let grammar_digest =
                    geography_grammar_digest(&self.dataset, &self.evidence, entry, geography)?;
                CensusGeographyAdmission::Standard {
                    for_level: entry.name.clone(),
                    geo_level_display: entry.geo_level_display.clone(),
                    requires: entry.requires.clone().into_boxed_slice(),
                    wildcard_parents: entry.wildcard.clone().into_boxed_slice(),
                    optional_with_wildcard_for: entry
                        .optional_with_wildcard_for
                        .clone()
                        .into_boxed_slice(),
                    for_is_wildcard,
                    grammar_digest,
                }
            }
            CensusGeography::Uniform { .. } => {
                let wire =
                    serde_json::to_vec(&(&self.dataset, self.evidence.payload_digest(), geography))
                        .map_err(|_| CensusAdapterError::SchemaDrift)?;
                let mut digest = sha2::Sha256::new();
                use sha2::Digest as _;
                digest.update(b"market-squawk/census-ucgid-admission/v2");
                digest.update(wire);
                CensusGeographyAdmission::Uniform {
                    grammar_digest: digest.finalize().into(),
                }
            }
        };
        admission.validate_query(geography)?;
        Ok(admission)
    }
}

fn ensure_body_bound(bytes: &[u8], limits: CensusParseLimits) -> Result<(), CensusAdapterError> {
    if bytes.len() > limits.max_bytes() {
        return Err(CensusAdapterError::BodyTooLarge);
    }
    Ok(())
}

fn ensure_entry_bound(count: usize, limits: CensusParseLimits) -> Result<(), CensusAdapterError> {
    if count > limits.max_metadata_entries() {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    Ok(())
}

fn parse_object(bytes: &[u8]) -> Result<Map<String, Value>, CensusAdapterError> {
    match serde_json::from_slice::<Value>(bytes).map_err(|_| CensusAdapterError::InvalidJson)? {
        Value::Object(object) => Ok(object),
        _ => Err(CensusAdapterError::SchemaDrift),
    }
}

fn required_text(
    object: &Map<String, Value>,
    key: &str,
    limits: CensusParseLimits,
) -> Result<String, CensusAdapterError> {
    object
        .get(key)
        .ok_or(CensusAdapterError::SchemaDrift)
        .and_then(|value| bounded_str(value, limits).map(str::to_owned))
}

fn optional_text(
    object: &Map<String, Value>,
    key: &str,
    limits: CensusParseLimits,
) -> Result<Option<String>, CensusAdapterError> {
    object
        .get(key)
        .filter(|value| !value.is_null())
        .map(|value| bounded_str(value, limits).map(str::to_owned))
        .transpose()
}

fn bounded_str(value: &Value, limits: CensusParseLimits) -> Result<&str, CensusAdapterError> {
    let value = value.as_str().ok_or(CensusAdapterError::SchemaDrift)?;
    if value.len() > limits.max_string_bytes() || value.chars().any(char::is_control) {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    Ok(value)
}

fn catalog_vintage_diagnosed(
    object: &Map<String, Value>,
    path: &[String],
    expected_vintage: Option<u16>,
) -> Result<Option<CensusDatasetVintage>, CensusCatalogParseFailure> {
    let Some(value) = object.get("c_vintage") else {
        if expected_vintage.is_some() {
            return Err(CensusCatalogParseFailure::new(
                CensusAdapterError::SchemaDrift,
                CensusCatalogFailurePredicate::VintageMissing,
            ));
        }
        return Ok(path
            .first()
            .is_some_and(|segment| segment == "timeseries")
            .then_some(CensusDatasetVintage::TimeSeries));
    };
    match value {
        Value::Null => Err(CensusCatalogParseFailure::new(
            CensusAdapterError::SchemaDrift,
            CensusCatalogFailurePredicate::VintageNull,
        )),
        Value::String(value) if value == "timeseries" => Ok(Some(CensusDatasetVintage::TimeSeries)),
        Value::String(value) => value
            .parse::<u16>()
            .map(CensusDatasetVintage::Year)
            .map(Some)
            .map_err(|_| {
                CensusCatalogParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusCatalogFailurePredicate::VintageStringVocabulary,
                )
            }),
        Value::Number(value) => {
            let value = value.as_u64().ok_or_else(|| {
                CensusCatalogParseFailure::new(
                    CensusAdapterError::SchemaDrift,
                    CensusCatalogFailurePredicate::VintageNumberShape,
                )
            })?;
            u16::try_from(value)
                .map(CensusDatasetVintage::Year)
                .map(Some)
                .map_err(|_| {
                    CensusCatalogParseFailure::new(
                        CensusAdapterError::SchemaDrift,
                        CensusCatalogFailurePredicate::VintageNumberRange,
                    )
                })
        }
        Value::Bool(_) | Value::Array(_) | Value::Object(_) => Err(CensusCatalogParseFailure::new(
            CensusAdapterError::SchemaDrift,
            CensusCatalogFailurePredicate::VintageType,
        )),
    }
}

fn validate_timeless_catalog_entry(
    object: &Map<String, Value>,
    limits: CensusParseLimits,
) -> Result<(), CensusCatalogParseFailure> {
    optional_text(object, "title", limits)
        .map_err(catalog_failure(CensusCatalogFailurePredicate::Title))?;
    optional_text(object, "description", limits)
        .map_err(catalog_failure(CensusCatalogFailurePredicate::Description))?;
    distribution_api_url(object, limits)
        .map_err(catalog_failure(CensusCatalogFailurePredicate::Distribution))?;
    optional_text(object, "c_variablesLink", limits).map_err(catalog_failure(
        CensusCatalogFailurePredicate::VariablesLink,
    ))?;
    optional_text(object, "c_groupsLink", limits)
        .map_err(catalog_failure(CensusCatalogFailurePredicate::GroupsLink))?;
    optional_text(object, "c_geographyLink", limits).map_err(catalog_failure(
        CensusCatalogFailurePredicate::GeographyLink,
    ))?;
    optional_bool(object, "c_isAvailable").map_err(catalog_failure(
        CensusCatalogFailurePredicate::AvailableFlag,
    ))?;
    optional_bool(object, "c_isAggregate").map_err(catalog_failure(
        CensusCatalogFailurePredicate::AggregateFlag,
    ))?;
    optional_bool(object, "c_isTimeseries").map_err(catalog_failure(
        CensusCatalogFailurePredicate::TimeSeriesFlag,
    ))?;
    Ok(())
}

fn optional_u64(object: &Map<String, Value>, key: &str) -> Result<Option<u64>, CensusAdapterError> {
    object
        .get(key)
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
                .ok_or(CensusAdapterError::SchemaDrift)
        })
        .transpose()
}

fn optional_bool(
    object: &Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, CensusAdapterError> {
    object
        .get(key)
        .filter(|value| !value.is_null())
        .map(|value| value.as_bool().ok_or(CensusAdapterError::SchemaDrift))
        .transpose()
}

fn required_string_array(
    object: &Map<String, Value>,
    key: &str,
    limits: CensusParseLimits,
) -> Result<Vec<String>, CensusAdapterError> {
    let values = object
        .get(key)
        .and_then(Value::as_array)
        .ok_or(CensusAdapterError::SchemaDrift)?;
    if values.is_empty() || values.len() > limits.max_columns() {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
    for value in values {
        output.push(bounded_str(value, limits)?.to_owned());
    }
    Ok(output)
}

fn optional_string_array(
    object: &Map<String, Value>,
    key: &str,
    limits: CensusParseLimits,
) -> Result<Vec<String>, CensusAdapterError> {
    let Some(value) = object.get(key) else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or(CensusAdapterError::SchemaDrift)?;
    if values.len() > limits.max_columns() {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(values.len())
        .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
    let mut identities = BTreeSet::new();
    for value in values {
        let value = bounded_str(value, limits)?;
        if value.is_empty() || !identities.insert(value) {
            return Err(CensusAdapterError::SchemaDrift);
        }
        output.push(value.to_owned());
    }
    Ok(output)
}

fn optional_string_array_or_scalar(
    object: &Map<String, Value>,
    key: &str,
    limits: CensusParseLimits,
) -> Result<Vec<String>, CensusAdapterError> {
    let Some(value) = object.get(key).filter(|value| !value.is_null()) else {
        return Ok(Vec::new());
    };
    match value {
        Value::String(value) => {
            if value.is_empty()
                || value.len() > limits.max_string_bytes()
                || value.chars().any(char::is_control)
            {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            let mut output = Vec::new();
            output
                .try_reserve_exact(1)
                .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
            output.push(value.clone());
            Ok(output)
        }
        Value::Array(_) => optional_string_array(object, key, limits),
        _ => Err(CensusAdapterError::SchemaDrift),
    }
}

fn clause_is_wildcard(codes: &[CensusGeographyCode]) -> Result<bool, CensusAdapterError> {
    let wildcard_count = codes
        .iter()
        .filter(|code| matches!(code, CensusGeographyCode::Wildcard))
        .count();
    if wildcard_count > 0 && (wildcard_count != 1 || codes.len() != 1) {
        return Err(CensusAdapterError::InvalidQuery);
    }
    Ok(wildcard_count == 1)
}

fn geography_grammar_digest(
    dataset: &CensusDataset,
    evidence: &CensusMetadataEvidence,
    entry: &CensusGeographyMetadata,
    geography: &CensusGeography,
) -> Result<[u8; 32], CensusAdapterError> {
    let wire = serde_json::to_vec(&(
        dataset,
        evidence.payload_digest(),
        entry.name(),
        entry.geo_level_display(),
        entry.reference_date(),
        entry.requires(),
        entry.wildcard(),
        entry.optional_with_wildcard_for(),
        geography,
    ))
    .map_err(|_| CensusAdapterError::SchemaDrift)?;
    let mut digest = sha2::Sha256::new();
    use sha2::Digest as _;
    digest.update(b"market-squawk/census-geography-admission/v2");
    digest.update(wire);
    Ok(digest.finalize().into())
}

fn parse_required(
    object: &Map<String, Value>,
) -> Result<CensusRequiredVariable, CensusAdapterError> {
    let predicate_only = object
        .get("predicateOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let Some(value) = object.get("required") else {
        return Ok(if predicate_only {
            CensusRequiredVariable::PredicateOnly
        } else {
            CensusRequiredVariable::Unspecified
        });
    };
    let parsed = match value {
        Value::Bool(true) => CensusRequiredVariable::Required,
        Value::Bool(false) => CensusRequiredVariable::Optional,
        Value::String(value) if value == "predicate-only" => CensusRequiredVariable::PredicateOnly,
        Value::String(value) if value == "required, predicate-only" => {
            CensusRequiredVariable::RequiredPredicateOnly
        }
        Value::String(value) if value == "default displayed" => {
            CensusRequiredVariable::DefaultDisplayed
        }
        Value::String(value) if value == "true" => CensusRequiredVariable::Required,
        Value::String(value) if value == "false" => CensusRequiredVariable::Optional,
        Value::Null => CensusRequiredVariable::Unspecified,
        _ => return Err(CensusAdapterError::SchemaDrift),
    };
    Ok(match (parsed, predicate_only) {
        (CensusRequiredVariable::Required, true) => CensusRequiredVariable::RequiredPredicateOnly,
        (CensusRequiredVariable::Optional | CensusRequiredVariable::Unspecified, true) => {
            CensusRequiredVariable::PredicateOnly
        }
        (other, _) => other,
    })
}

fn parse_attributes(
    value: Option<&Value>,
    limits: CensusParseLimits,
) -> Result<Vec<SourceIdentifier>, CensusAdapterError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(Vec::new());
    };
    let values = match value {
        Value::String(value) => {
            if value.len() > limits.max_string_bytes() || value.chars().any(char::is_control) {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            if value.is_empty() {
                Vec::new()
            } else {
                let mut values = Vec::new();
                for candidate in value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                {
                    if values.len() == limits.max_columns() {
                        return Err(CensusAdapterError::ResourceLimitExceeded);
                    }
                    values
                        .try_reserve_exact(1)
                        .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
                    values.push(candidate);
                }
                values
            }
        }
        Value::Array(values) => {
            if values.len() > limits.max_columns() {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            let mut parsed = Vec::new();
            parsed
                .try_reserve_exact(values.len())
                .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
            for value in values {
                parsed.push(bounded_str(value, limits)?);
            }
            parsed
        }
        _ => return Err(CensusAdapterError::SchemaDrift),
    };
    let mut attributes = Vec::new();
    attributes
        .try_reserve_exact(values.len())
        .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
    let mut identities = BTreeSet::new();
    for value in values.into_iter().filter(|value| !value.is_empty()) {
        let attribute = identifier(value)?;
        if !identities.insert(attribute.clone()) {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        attributes.push(attribute);
    }
    Ok(attributes)
}

fn distribution_api_url(
    object: &Map<String, Value>,
    limits: CensusParseLimits,
) -> Result<Option<String>, CensusAdapterError> {
    let Some(distributions) = object.get("distribution") else {
        return Ok(None);
    };
    let distributions = distributions
        .as_array()
        .ok_or(CensusAdapterError::SchemaDrift)?;
    if distributions.len() > limits.max_columns() {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    let mut admitted_api_url = None;
    for distribution in distributions {
        let distribution = distribution
            .as_object()
            .ok_or(CensusAdapterError::SchemaDrift)?;
        let format = distribution
            .get("format")
            .filter(|value| !value.is_null())
            .map(|value| bounded_str(value, limits))
            .transpose()?;
        let access_url = distribution
            .get("accessURL")
            .filter(|value| !value.is_null())
            .map(|value| bounded_str(value, limits))
            .transpose()?;
        if admitted_api_url.is_none()
            && format.is_some_and(|format| format.eq_ignore_ascii_case("api"))
            && let Some(access_url) = access_url
        {
            admitted_api_url = Some(access_url.to_owned());
        }
    }
    Ok(admitted_api_url)
}

fn parse_date(value: &str) -> Result<CalendarDate, CensusAdapterError> {
    let mut components = value.split('-');
    let year = components
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(CensusAdapterError::SchemaDrift)?;
    let month = components
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or(CensusAdapterError::SchemaDrift)?;
    let day = components
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or(CensusAdapterError::SchemaDrift)?;
    if components.next().is_some() {
        return Err(CensusAdapterError::SchemaDrift);
    }
    CalendarDate::new(year, month, day).map_err(|_| CensusAdapterError::SchemaDrift)
}

fn identifier(value: &str) -> Result<SourceIdentifier, CensusAdapterError> {
    SourceIdentifier::try_from(value).map_err(|_| CensusAdapterError::InvalidComponent)
}
