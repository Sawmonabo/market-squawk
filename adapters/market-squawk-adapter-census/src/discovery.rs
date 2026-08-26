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
        ensure_body_bound(bytes, limits)?;
        match request.kind() {
            CensusDiscoveryKind::Datasets => {
                CensusDatasetCatalog::parse_inner(bytes, None, limits).map(Self::Datasets)
            }
            CensusDiscoveryKind::VintageDatasets { vintage } => {
                CensusDatasetCatalog::parse_inner(bytes, Some(*vintage), limits).map(Self::Datasets)
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
            CensusDiscoveryKind::Geographies { dataset } => {
                CensusGeographyCatalog::parse_inner(bytes, dataset.clone(), limits)
                    .map(Self::Geographies)
            }
        }
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
    fn parse_inner(
        bytes: &[u8],
        expected_vintage: Option<u16>,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusAdapterError> {
        let root = parse_object(bytes)?;
        let entries = root
            .get("dataset")
            .and_then(Value::as_array)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        ensure_entry_bound(entries.len(), limits)?;
        let mut datasets = Vec::new();
        datasets
            .try_reserve_exact(entries.len())
            .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            let entry = entry.as_object().ok_or(CensusAdapterError::SchemaDrift)?;
            let vintage = required_vintage(entry, "c_vintage")?;
            if expected_vintage
                .is_some_and(|expected| vintage != CensusDatasetVintage::Year(expected))
            {
                return Err(CensusAdapterError::MetadataMismatch);
            }
            let path = required_string_array(entry, "c_dataset", limits)?;
            let dataset = match vintage {
                CensusDatasetVintage::Year(year) => CensusDataset::try_new(year, path.join("/"))?,
                CensusDatasetVintage::TimeSeries => {
                    let path = path
                        .first()
                        .is_some_and(|segment| segment == "timeseries")
                        .then_some(&path[1..])
                        .unwrap_or(path.as_slice());
                    CensusDataset::try_time_series(path.join("/"))?
                }
            };
            if !identities.insert(dataset.clone()) {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
            let title = required_text(entry, "title", limits)?;
            let description = required_text(entry, "description", limits)?;
            let api_base_url = distribution_api_url(entry, limits)?;
            let variables_url = optional_text(entry, "c_variablesLink", limits)?;
            let groups_url = optional_text(entry, "c_groupsLink", limits)?;
            let geography_url = optional_text(entry, "c_geographyLink", limits)?;
            datasets.push(CensusDatasetMetadata {
                dataset,
                title,
                description,
                api_base_url,
                variables_url,
                groups_url,
                geography_url,
                available: optional_bool(entry, "c_isAvailable")?,
                aggregate: optional_bool(entry, "c_isAggregate")?,
                time_series: optional_bool(entry, "c_isTimeseries")?,
            });
        }
        datasets.sort_by(|left, right| left.dataset.cmp(&right.dataset));
        Ok(Self {
            evidence: CensusMetadataEvidence::new(bytes, datasets.len()),
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
            CensusRequiredVariable::PredicateOnly
                | CensusRequiredVariable::RequiredPredicateOnly
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
        for metadata in variables.values() {
            for attribute in &metadata.attributes {
                if !variables.contains_key(attribute) {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                attribute_owners
                    .entry(attribute.clone())
                    .or_default()
                    .push(metadata.name.clone());
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
    geo_level_display: SourceIdentifier,
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

    /// Returns the provider summary-level display identity.
    pub const fn geo_level_display(&self) -> &SourceIdentifier {
        &self.geo_level_display
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
        geo_level_display: SourceIdentifier,
        requires: Box<[String]>,
        wildcard_parents: Box<[String]>,
        optional_with_wildcard_for: Box<[String]>,
        for_is_wildcard: bool,
        grammar_digest: [u8; 32],
    },
    /// `ucgid` is admitted from exact variable metadata rather than `fips` geography entries.
    Uniform {
        grammar_digest: [u8; 32],
    },
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
                if for_clause.level() != for_level
                    || actual_for_wildcard != *for_is_wildcard
                    || *grammar_digest == [0; 32]
                {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                let optional = optional_with_wildcard_for
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                let supplied = in_clauses
                    .iter()
                    .map(|clause| clause.level())
                    .collect::<BTreeSet<_>>();
                if supplied.len() != in_clauses.len()
                    || in_clauses
                        .iter()
                        .any(|clause| !requires.iter().any(|required| required == clause.level()))
                    || requires.iter().any(|required| {
                        !supplied.contains(required.as_str())
                            && !(*for_is_wildcard && optional.contains(required.as_str()))
                    })
                {
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
    fn parse_inner(
        bytes: &[u8],
        dataset: CensusDataset,
        limits: CensusParseLimits,
    ) -> Result<Self, CensusAdapterError> {
        let root = parse_object(bytes)?;
        let entries = root
            .get("fips")
            .and_then(Value::as_array)
            .ok_or(CensusAdapterError::SchemaDrift)?;
        ensure_entry_bound(entries.len(), limits)?;
        let mut geographies = Vec::new();
        geographies
            .try_reserve_exact(entries.len())
            .map_err(|_| CensusAdapterError::ResourceLimitExceeded)?;
        let mut identities = BTreeSet::new();
        for entry in entries {
            let entry = entry.as_object().ok_or(CensusAdapterError::SchemaDrift)?;
            let name = required_text(entry, "name", limits)?;
            let geo_level_display = identifier(&required_text(entry, "geoLevelDisplay", limits)?)?;
            if !identities.insert((name.clone(), geo_level_display.clone())) {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
            geographies.push(CensusGeographyMetadata {
                name,
                geo_level_display,
                reference_date: optional_text(entry, "referenceDate", limits)?
                    .map(|value| parse_date(&value))
                    .transpose()?,
                requires: optional_string_array(entry, "requires", limits)?,
                wildcard: optional_string_array(entry, "wildcard", limits)?,
                optional_with_wildcard_for: optional_string_array_or_scalar(
                    entry,
                    "optionalWithWCFor",
                    limits,
                )?,
            });
        }
        geographies.sort_by(|left, right| {
            (&left.geo_level_display, &left.name).cmp(&(&right.geo_level_display, &right.name))
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

    /// Returns geography grammar entries in stable summary-level/name order.
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
                let grammar_digest = geography_grammar_digest(entry, geography)?;
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
                let wire = serde_json::to_vec(geography)
                    .map_err(|_| CensusAdapterError::SchemaDrift)?;
                let mut digest = sha2::Sha256::new();
                use sha2::Digest as _;
                digest.update(b"market-squawk/census-ucgid-admission/v1");
                digest.update(self.evidence.payload_digest);
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

fn required_u16(object: &Map<String, Value>, key: &str) -> Result<u16, CensusAdapterError> {
    let value = object.get(key).ok_or(CensusAdapterError::SchemaDrift)?;
    let value = value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or(CensusAdapterError::SchemaDrift)?;
    u16::try_from(value).map_err(|_| CensusAdapterError::SchemaDrift)
}

fn required_vintage(
    object: &Map<String, Value>,
    key: &str,
) -> Result<CensusDatasetVintage, CensusAdapterError> {
    if object.get(key).and_then(Value::as_str) == Some("timeseries") {
        return Ok(CensusDatasetVintage::TimeSeries);
    }
    required_u16(object, key).map(CensusDatasetVintage::Year)
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
    values
        .iter()
        .map(|value| bounded_str(value, limits).map(str::to_owned))
        .collect()
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
    values
        .iter()
        .map(|value| bounded_str(value, limits).map(str::to_owned))
        .collect()
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
            if value.len() > limits.max_string_bytes() || value.chars().any(char::is_control) {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            Ok(vec![value.clone()])
        }
        Value::Array(_) => optional_string_array(object, key, limits),
        _ => Err(CensusAdapterError::SchemaDrift),
    }
}

fn clause_is_wildcard(
    codes: &[CensusGeographyCode],
) -> Result<bool, CensusAdapterError> {
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
    entry: &CensusGeographyMetadata,
    geography: &CensusGeography,
) -> Result<[u8; 32], CensusAdapterError> {
    let wire = serde_json::to_vec(&(
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
    digest.update(b"market-squawk/census-geography-admission/v1");
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
        Value::String(value) if value == "predicate-only" => {
            CensusRequiredVariable::PredicateOnly
        }
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
        (CensusRequiredVariable::Required, true) => {
            CensusRequiredVariable::RequiredPredicateOnly
        }
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
            if value.len() > limits.max_string_bytes() {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            if value.is_empty() {
                Vec::new()
            } else {
                value.split(',').map(str::trim).collect::<Vec<_>>()
            }
        }
        Value::Array(values) => {
            if values.len() > limits.max_columns() {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            values
                .iter()
                .map(|value| bounded_str(value, limits))
                .collect::<Result<Vec<_>, _>>()?
        }
        _ => return Err(CensusAdapterError::SchemaDrift),
    };
    let attributes = values
        .into_iter()
        .filter(|value| !value.is_empty())
        .map(identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if attributes.iter().collect::<BTreeSet<_>>().len() != attributes.len() {
        return Err(CensusAdapterError::DuplicateIdentity);
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
    for distribution in distributions {
        let distribution = distribution
            .as_object()
            .ok_or(CensusAdapterError::SchemaDrift)?;
        if distribution
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| format.eq_ignore_ascii_case("api"))
            && let Some(access_url) = distribution.get("accessURL")
        {
            return bounded_str(access_url, limits).map(|value| Some(value.to_owned()));
        }
    }
    Ok(None)
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
