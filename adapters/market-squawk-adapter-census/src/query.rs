use std::collections::BTreeSet;
use std::fmt;

use market_squawk_domain::SourceIdentifier;
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    CensusAdapterError, CensusPredicateType, CensusVariableCatalog, update_digest_component,
};

const CENSUS_API_ORIGIN: &str = "https://api.census.gov";
const MAX_QUERY_COMPONENT_BYTES: usize = 512;
const MAX_API_KEY_BYTES: usize = 512;
const MAX_PREDICATE_VALUES: usize = 256;
const MAX_GEOGRAPHY_COMPONENTS: usize = 16;

/// Census's verified maximum for an ordinary comma-separated `get` variable list.
///
/// The provider's `group(NAME)` function is a separate grammar and can return more fields.
pub const CENSUS_PROVIDER_VARIABLE_LIMIT: usize = 50;

/// Market Squawk's conservative keyed-request pacing policy, not a provider ceiling.
pub const CENSUS_APPLICATION_REQUESTS_PER_SECOND: u32 = 1;

/// Market Squawk's conservative keyed-request daily policy, not a provider ceiling.
pub const CENSUS_APPLICATION_REQUESTS_PER_DAY: u32 = 400;

/// The application-owned Census pacing contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CensusApplicationPacing {
    requests_per_second: u32,
    requests_per_day: u32,
    keyed_provider_requests_per_second: Option<u32>,
    keyed_provider_requests_per_day: Option<u32>,
}

impl CensusApplicationPacing {
    /// Returns the conservative policy used until retained runtime evidence and a reviewed current
    /// provider contract establish a numeric keyed ceiling.
    pub const fn conservative() -> Self {
        Self {
            requests_per_second: CENSUS_APPLICATION_REQUESTS_PER_SECOND,
            requests_per_day: CENSUS_APPLICATION_REQUESTS_PER_DAY,
            keyed_provider_requests_per_second: None,
            keyed_provider_requests_per_day: None,
        }
    }

    /// Returns the application-owned requests-per-second ceiling.
    pub const fn requests_per_second(self) -> u32 {
        self.requests_per_second
    }

    /// Returns the application-owned requests-per-day ceiling.
    pub const fn requests_per_day(self) -> u32 {
        self.requests_per_day
    }

    /// Returns the current reviewed numeric keyed provider rate, which is intentionally absent.
    pub const fn keyed_provider_requests_per_second(self) -> Option<u32> {
        self.keyed_provider_requests_per_second
    }

    /// Returns the current reviewed numeric keyed daily ceiling, which is intentionally absent.
    pub const fn keyed_provider_requests_per_day(self) -> Option<u32> {
        self.keyed_provider_requests_per_day
    }
}

impl Default for CensusApplicationPacing {
    fn default() -> Self {
        Self::conservative()
    }
}

/// The provider route coordinate for one Census dataset.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusDatasetVintage {
    /// A four-digit statistical-product vintage.
    Year(u16),
    /// The provider's `/data/timeseries/...` route family.
    TimeSeries,
}

impl CensusDatasetVintage {
    fn route_segment(self) -> String {
        match self {
            Self::Year(year) => format!("{year:04}"),
            Self::TimeSeries => "timeseries".to_owned(),
        }
    }
}

/// A validated Census dataset vintage and path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CensusDataset {
    vintage: CensusDatasetVintage,
    path: Vec<String>,
}

impl CensusDataset {
    /// Constructs one exact `/data/{vintage}/{dataset...}` identity.
    ///
    /// # Errors
    ///
    /// Rejects non-four-digit vintages, empty paths, empty segments, and segments outside the
    /// provider's route-safe identifier grammar.
    pub fn try_new(vintage: u16, path: impl AsRef<str>) -> Result<Self, CensusAdapterError> {
        if !(1000..=9999).contains(&vintage) {
            return Err(CensusAdapterError::InvalidVintage);
        }
        let raw_path = path.as_ref();
        if raw_path.is_empty() || raw_path.len() > MAX_QUERY_COMPONENT_BYTES {
            return Err(CensusAdapterError::InvalidComponent);
        }
        let path = raw_path
            .split('/')
            .map(|segment| {
                if segment.is_empty()
                    || segment.len() > 128
                    || !segment.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
                    })
                {
                    return Err(CensusAdapterError::InvalidComponent);
                }
                Ok(segment.to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if path.is_empty() {
            return Err(CensusAdapterError::InvalidComponent);
        }
        Ok(Self {
            vintage: CensusDatasetVintage::Year(vintage),
            path,
        })
    }

    /// Constructs one exact `/data/timeseries/{dataset...}` identity.
    ///
    /// # Errors
    ///
    /// Rejects an empty or invalid time-series dataset path.
    pub fn try_time_series(path: impl AsRef<str>) -> Result<Self, CensusAdapterError> {
        let mut dataset = Self::try_new(1000, path)?;
        dataset.vintage = CensusDatasetVintage::TimeSeries;
        Ok(dataset)
    }

    /// Returns the exact dataset vintage.
    pub const fn vintage(&self) -> CensusDatasetVintage {
        self.vintage
    }

    /// Returns the ordered provider path segments.
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Returns the slash-separated provider path.
    pub fn path_string(&self) -> String {
        self.path.join("/")
    }

    pub(crate) fn append_to_url(&self, url: &mut Url) -> Result<(), CensusAdapterError> {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| CensusAdapterError::InvalidQuery)?;
        segments.push("data");
        segments.push(&self.vintage.route_segment());
        for segment in &self.path {
            segments.push(segment);
        }
        Ok(())
    }
}

/// A Census API key whose debug representation and drop behavior do not expose the secret.
pub struct CensusApiKey(Zeroizing<String>);

impl CensusApiKey {
    /// Validates and owns one key without recording its value in an error.
    ///
    /// # Errors
    ///
    /// Rejects empty, overlong, or control-character-bearing key material.
    pub fn try_new(value: String) -> Result<Self, CensusAdapterError> {
        if value.is_empty()
            || value.len() > MAX_API_KEY_BYTES
            || value.chars().any(char::is_control)
        {
            let mut value = value;
            value.zeroize();
            return Err(CensusAdapterError::InvalidApiKey);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for CensusApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CensusApiKey([REDACTED])")
    }
}

/// A key-free URL plus an optional separately retained data-query key, with safe debug behavior.
///
/// Public metadata requests retain no key. Credential-bearing data requests keep the key outside
/// the URL until transport serialization. Logs, traces, receipts, and errors must use
/// [`Self::redacted_url`] or [`Self::request_digest`].
pub(crate) struct CensusAuthorizedUrl<'a> {
    public_url: Url,
    api_key: Option<&'a CensusApiKey>,
    redacted_url: String,
    request_digest: [u8; 32],
}

impl CensusAuthorizedUrl<'_> {
    /// Returns the key-free URL for transport.
    pub(crate) fn transport_url(&self) -> &Url {
        &self.public_url
    }

    /// Returns the optional key query value for direct handoff to a transport's serializer.
    ///
    /// The value must not be placed in a URL, log, trace, error, receipt, or provider status by
    /// the caller. Keeping it separate prevents debug formatting of this request from exposing it.
    pub(crate) fn key_query_value(&self) -> Option<&str> {
        self.api_key.map(CensusApiKey::expose)
    }

    /// Returns whether this request carries the separately retained API-key credential.
    pub(crate) const fn is_credentialed(&self) -> bool {
        self.api_key.is_some()
    }

    /// Returns the key-free URL suitable for diagnostics and retained receipts.
    pub(crate) fn redacted_url(&self) -> &str {
        &self.redacted_url
    }

    /// Returns the SHA-256 identity of the exact key-free request.
    pub(crate) const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
}

impl fmt::Debug for CensusAuthorizedUrl<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CensusAuthorizedUrl")
            .field("url", &self.redacted_url)
            .field("request_digest", &self.request_digest)
            .finish()
    }
}

impl fmt::Display for CensusAuthorizedUrl<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.redacted_url)
    }
}

/// The `get` selection for one Census data request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusSelection {
    /// A bounded ordinary variable list.
    Variables {
        /// Primary variables selected by the caller.
        primary: Vec<SourceIdentifier>,
        /// Exact wire variables, including selected metadata-declared attributes.
        wire: Vec<SourceIdentifier>,
    },
    /// One provider group function.
    Group {
        /// Exact group identity.
        group: SourceIdentifier,
    },
}

impl CensusSelection {
    /// Constructs an ordinary variable selection without attribute expansion.
    ///
    /// Prefer [`Self::variables_with_attributes`] for analytical collection. The response parser
    /// reports metadata-declared but unrequested attributes as incomplete evidence.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, invalid, or more-than-50 variable lists.
    pub fn variables<I, S>(variables: I) -> Result<Self, CensusAdapterError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let variables = validated_identifiers(variables, CENSUS_PROVIDER_VARIABLE_LIMIT)?;
        Ok(Self::Variables {
            primary: variables.clone(),
            wire: variables,
        })
    }

    /// Constructs an ordinary selection and adds every metadata-declared attribute of each
    /// primary variable.
    ///
    /// # Errors
    ///
    /// Rejects unknown primary/attribute metadata and expanded lists above the provider limit.
    pub fn variables_with_attributes<I, S>(
        variables: I,
        catalog: &CensusVariableCatalog,
    ) -> Result<Self, CensusAdapterError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let primary = validated_identifiers(variables, CENSUS_PROVIDER_VARIABLE_LIMIT)?;
        let mut seen = BTreeSet::new();
        let mut wire = Vec::new();
        for variable in &primary {
            let metadata = catalog
                .get(variable.as_str())
                .ok_or(CensusAdapterError::MetadataMismatch)?;
            push_unique(&mut wire, &mut seen, variable.clone());
            for attribute in metadata.attributes() {
                if catalog.get(attribute.as_str()).is_none() {
                    return Err(CensusAdapterError::MetadataMismatch);
                }
                push_unique(&mut wire, &mut seen, attribute.clone());
            }
        }
        if wire.len() > CENSUS_PROVIDER_VARIABLE_LIMIT {
            return Err(CensusAdapterError::VariableLimitExceeded);
        }
        Ok(Self::Variables { primary, wire })
    }

    /// Constructs one exact `group(NAME)` selection.
    ///
    /// # Errors
    ///
    /// Rejects an invalid group identity.
    pub fn group(group: impl AsRef<str>) -> Result<Self, CensusAdapterError> {
        Ok(Self::Group {
            group: identifier(group.as_ref())?,
        })
    }

    /// Returns primary variables for ordinary selection.
    pub fn primary_variables(&self) -> &[SourceIdentifier] {
        match self {
            Self::Variables { primary, .. } => primary,
            Self::Group { .. } => &[],
        }
    }

    /// Returns exact `get` wire variables for ordinary selection.
    pub fn wire_variables(&self) -> &[SourceIdentifier] {
        match self {
            Self::Variables { wire, .. } => wire,
            Self::Group { .. } => &[],
        }
    }

    /// Returns the selected group identity.
    pub const fn group_id(&self) -> Option<&SourceIdentifier> {
        match self {
            Self::Group { group } => Some(group),
            Self::Variables { .. } => None,
        }
    }

    fn get_value(&self) -> String {
        match self {
            Self::Variables { wire, .. } => wire
                .iter()
                .map(SourceIdentifier::as_str)
                .collect::<Vec<_>>()
                .join(","),
            Self::Group { group } => format!("group({group})"),
        }
    }
}

/// One typed non-geographic Census predicate with one or more provider values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusPredicate {
    variable: SourceIdentifier,
    predicate_type: CensusPredicateType,
    values: Vec<String>,
}

impl CensusPredicate {
    /// Constructs one repeated-key predicate after type-aware wildcard/range validation.
    ///
    /// # Errors
    ///
    /// Rejects empty or over-bounded values, reserved query keys, and values inconsistent with the
    /// metadata predicate type.
    pub fn try_new<I, S>(
        variable: impl AsRef<str>,
        predicate_type: CensusPredicateType,
        values: I,
    ) -> Result<Self, CensusAdapterError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let variable = identifier(variable.as_ref())?;
        if is_reserved_query_key(variable.as_str())
            || matches!(
                predicate_type,
                CensusPredicateType::FipsFor
                    | CensusPredicateType::FipsIn
                    | CensusPredicateType::Time
                    | CensusPredicateType::NotPredicate
                    | CensusPredicateType::Unknown(_)
            )
        {
            return Err(CensusAdapterError::InvalidQuery);
        }
        let values = validated_values(values, MAX_PREDICATE_VALUES)?;
        for value in &values {
            validate_predicate_value(value, &predicate_type)?;
        }
        Ok(Self {
            variable,
            predicate_type,
            values,
        })
    }

    /// Returns the predicate variable.
    pub const fn variable(&self) -> &SourceIdentifier {
        &self.variable
    }

    /// Returns the discovery-declared predicate type.
    pub const fn predicate_type(&self) -> &CensusPredicateType {
        &self.predicate_type
    }

    /// Returns values emitted as repeated query keys.
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

/// One exact or wildcard geography code.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum CensusGeographyCode {
    /// One exact provider geography code.
    Exact(String),
    /// The provider `*` wildcard.
    Wildcard,
}

impl CensusGeographyCode {
    /// Constructs a code from an exact token or `*`.
    ///
    /// # Errors
    ///
    /// Rejects empty, overlong, comma-bearing, whitespace-bearing, or control-bearing tokens.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, CensusAdapterError> {
        let value = value.as_ref();
        if value == "*" {
            return Ok(Self::Wildcard);
        }
        validate_bounded_value(value)?;
        if value.contains(',') || value.chars().any(char::is_whitespace) {
            return Err(CensusAdapterError::InvalidComponent);
        }
        Ok(Self::Exact(value.to_owned()))
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Exact(value) => value,
            Self::Wildcard => "*",
        }
    }
}

/// One `for` or `in` geography level and its selected codes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusGeographyClause {
    level: String,
    codes: Vec<CensusGeographyCode>,
}

impl CensusGeographyClause {
    /// Constructs a geography clause.
    ///
    /// # Errors
    ///
    /// Rejects empty levels/codes, duplicate codes, or unsupported delimiters.
    pub fn try_new<I>(level: impl AsRef<str>, codes: I) -> Result<Self, CensusAdapterError>
    where
        I: IntoIterator<Item = CensusGeographyCode>,
    {
        let level = level.as_ref();
        validate_bounded_value(level)?;
        if level.contains([':', ',', '&']) || level.chars().any(char::is_control) {
            return Err(CensusAdapterError::InvalidComponent);
        }
        let codes = codes.into_iter().collect::<Vec<_>>();
        if codes.is_empty() || codes.len() > MAX_PREDICATE_VALUES {
            return Err(CensusAdapterError::InvalidQuery);
        }
        let unique = codes.iter().collect::<BTreeSet<_>>();
        if unique.len() != codes.len() {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        Ok(Self {
            level: level.to_owned(),
            codes,
        })
    }

    /// Returns the provider geography level.
    pub fn level(&self) -> &str {
        &self.level
    }

    /// Returns the exact selected codes.
    pub fn codes(&self) -> &[CensusGeographyCode] {
        &self.codes
    }

    fn query_value(&self) -> String {
        format!(
            "{}:{}",
            self.level,
            self.codes
                .iter()
                .map(CensusGeographyCode::as_str)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

/// A bounded fully qualified `ucgid` or provider pseudo-geography expression.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct CensusUcgid(String);

impl CensusUcgid {
    /// Validates one provider UCGID expression without interpreting its dataset-specific meaning.
    ///
    /// # Errors
    ///
    /// Rejects empty, overlong, whitespace/control-bearing, comma-bearing, or delimiter-unbalanced
    /// expressions. Entitlement against a dataset is established from its variable discovery.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, CensusAdapterError> {
        let value = value.as_ref();
        validate_bounded_value(value)?;
        if value.contains(',')
            || value.chars().any(char::is_whitespace)
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'(' | b')' | b'$' | b'-' | b'_')
            })
        {
            return Err(CensusAdapterError::InvalidComponent);
        }
        let balanced = if let Some(inner) = value
            .strip_prefix("pseudo(")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            !inner.is_empty() && !inner.contains(['(', ')']) && inner.contains('$')
        } else {
            !value.contains(['(', ')', '$'])
        };
        if !balanced {
            return Err(CensusAdapterError::InvalidComponent);
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the exact provider expression.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One standard or UCGID geography selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusGeography {
    /// Provider `for` plus optional `in` clauses.
    Standard {
        /// Output geography.
        for_clause: CensusGeographyClause,
        /// Parent/containing geographies in provider order.
        in_clauses: Vec<CensusGeographyClause>,
    },
    /// Provider `ucgid` alternative.
    Uniform {
        /// One or more exact/pseudo fully qualified geography IDs.
        values: Vec<CensusUcgid>,
    },
}

impl CensusGeography {
    /// Constructs standard geography predicates.
    ///
    /// # Errors
    ///
    /// Rejects too many or duplicate levels.
    pub fn standard(
        for_clause: CensusGeographyClause,
        in_clauses: Vec<CensusGeographyClause>,
    ) -> Result<Self, CensusAdapterError> {
        if in_clauses.len() > MAX_GEOGRAPHY_COMPONENTS {
            return Err(CensusAdapterError::ResourceLimitExceeded);
        }
        let mut levels = BTreeSet::new();
        levels.insert(for_clause.level.clone());
        if in_clauses
            .iter()
            .any(|clause| !levels.insert(clause.level.clone()))
        {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        Ok(Self::Standard {
            for_clause,
            in_clauses,
        })
    }

    /// Constructs a UCGID selection.
    ///
    /// # Errors
    ///
    /// Rejects empty, duplicate, or over-bounded lists.
    pub fn uniform(values: Vec<CensusUcgid>) -> Result<Self, CensusAdapterError> {
        if values.is_empty() || values.len() > MAX_PREDICATE_VALUES {
            return Err(CensusAdapterError::InvalidQuery);
        }
        if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        Ok(Self::Uniform { values })
    }

    /// Returns response geography levels required for standard selection.
    pub fn required_response_levels(&self) -> Vec<&str> {
        match self {
            Self::Standard {
                for_clause,
                in_clauses,
            } => in_clauses
                .iter()
                .map(CensusGeographyClause::level)
                .chain(std::iter::once(for_clause.level()))
                .collect(),
            Self::Uniform { .. } => Vec::new(),
        }
    }

    fn append_query_pairs(&self, url: &mut Url) {
        match self {
            Self::Standard {
                for_clause,
                in_clauses,
            } => {
                let mut query = url.query_pairs_mut();
                query.append_pair("for", &for_clause.query_value());
                if !in_clauses.is_empty() {
                    query.append_pair(
                        "in",
                        &in_clauses
                            .iter()
                            .map(CensusGeographyClause::query_value)
                            .collect::<Vec<_>>()
                            .join(" "),
                    );
                }
            }
            Self::Uniform { values } => {
                url.query_pairs_mut().append_pair(
                    "ucgid",
                    &values
                        .iter()
                        .map(CensusUcgid::as_str)
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
        }
    }
}

/// An exact time point admitted by the current Census query guide.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "precision")]
pub enum CensusTimePoint {
    /// Calendar year.
    Year { year: u16 },
    /// Calendar month.
    Month { year: u16, month: u8 },
    /// Calendar quarter.
    Quarter { year: u16, quarter: u8 },
}

impl CensusTimePoint {
    /// Constructs a four-digit calendar year.
    pub fn year(year: u16) -> Result<Self, CensusAdapterError> {
        validate_year(year)?;
        Ok(Self::Year { year })
    }

    /// Constructs a calendar month.
    pub fn month(year: u16, month: u8) -> Result<Self, CensusAdapterError> {
        validate_year(year)?;
        if !(1..=12).contains(&month) {
            return Err(CensusAdapterError::InvalidQuery);
        }
        Ok(Self::Month { year, month })
    }

    /// Constructs a calendar quarter.
    pub fn quarter(year: u16, quarter: u8) -> Result<Self, CensusAdapterError> {
        validate_year(year)?;
        if !(1..=4).contains(&quarter) {
            return Err(CensusAdapterError::InvalidQuery);
        }
        Ok(Self::Quarter { year, quarter })
    }

    pub(crate) fn provider_value(self) -> String {
        match self {
            Self::Year { year } => format!("{year:04}"),
            Self::Month { year, month } => format!("{year:04}-{month:02}"),
            Self::Quarter { year, quarter } => format!("{year:04}-Q{quarter}"),
        }
    }

    fn validate(self) -> Result<(), CensusAdapterError> {
        match self {
            Self::Year { year } => validate_year(year),
            Self::Month { year, month } => {
                validate_year(year)?;
                if !(1..=12).contains(&month) {
                    return Err(CensusAdapterError::InvalidQuery);
                }
                Ok(())
            }
            Self::Quarter { year, quarter } => {
                validate_year(year)?;
                if !(1..=4).contains(&quarter) {
                    return Err(CensusAdapterError::InvalidQuery);
                }
                Ok(())
            }
        }
    }
}

/// The verified point/range forms of the time-series `time` predicate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusTimePredicate {
    /// One exact period.
    At { point: CensusTimePoint },
    /// From one period through the provider's current end.
    From { start: CensusTimePoint },
    /// Through one period.
    To { end: CensusTimePoint },
    /// Inclusive provider time range.
    Range {
        /// Range start.
        start: CensusTimePoint,
        /// Range end.
        end: CensusTimePoint,
    },
}

impl CensusTimePredicate {
    /// Constructs a range with matching precision and nondecreasing endpoints.
    ///
    /// # Errors
    ///
    /// Rejects cross-precision or reversed ranges.
    pub fn range(start: CensusTimePoint, end: CensusTimePoint) -> Result<Self, CensusAdapterError> {
        if std::mem::discriminant(&start) != std::mem::discriminant(&end) || start > end {
            return Err(CensusAdapterError::InvalidQuery);
        }
        Ok(Self::Range { start, end })
    }

    fn provider_value(self) -> String {
        match self {
            Self::At { point } => point.provider_value(),
            Self::From { start } => format!("from {}", start.provider_value()),
            Self::To { end } => format!("to {}", end.provider_value()),
            Self::Range { start, end } => {
                format!(
                    "from {} to {}",
                    start.provider_value(),
                    end.provider_value()
                )
            }
        }
    }
}

/// One complete, key-free Census data request specification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusDataQuery {
    dataset: CensusDataset,
    selection: CensusSelection,
    predicates: Vec<CensusPredicate>,
    geography: CensusGeography,
    time: Option<CensusTimePredicate>,
    request_digest: [u8; 32],
    redacted_url: String,
}

impl CensusDataQuery {
    /// Validates and constructs one deterministic JSON data query.
    ///
    /// # Errors
    ///
    /// Rejects duplicate predicates, variable/get overlap, invalid time collisions, and malformed
    /// route or URL state.
    pub fn try_new(
        dataset: CensusDataset,
        selection: CensusSelection,
        mut predicates: Vec<CensusPredicate>,
        geography: CensusGeography,
        time: Option<CensusTimePredicate>,
    ) -> Result<Self, CensusAdapterError> {
        validate_selection(&selection)?;
        validate_geography(&geography)?;
        if time.is_some() && dataset.vintage() != CensusDatasetVintage::TimeSeries {
            return Err(CensusAdapterError::InvalidQuery);
        }
        if let Some(time) = time {
            validate_time_predicate(time)?;
        }
        predicates.sort_by(|left, right| left.variable.cmp(&right.variable));
        if predicates
            .windows(2)
            .any(|pair| pair[0].variable == pair[1].variable)
        {
            return Err(CensusAdapterError::DuplicateIdentity);
        }
        let get_variables = selection
            .wire_variables()
            .iter()
            .map(SourceIdentifier::as_str)
            .collect::<BTreeSet<_>>();
        if predicates
            .iter()
            .any(|predicate| get_variables.contains(predicate.variable.as_str()))
        {
            return Err(CensusAdapterError::InvalidQuery);
        }

        let mut url = dataset_url(&dataset)?;
        url.query_pairs_mut()
            .append_pair("get", &selection.get_value());
        for predicate in &predicates {
            for value in &predicate.values {
                url.query_pairs_mut()
                    .append_pair(predicate.variable.as_str(), value);
            }
        }
        if let Some(time) = time {
            url.query_pairs_mut()
                .append_pair("time", &time.provider_value());
        }
        geography.append_query_pairs(&mut url);
        url.query_pairs_mut()
            .append_pair("descriptive", "false")
            .append_pair("outputFormat", "json");
        let redacted_url = url.to_string();
        let request_digest = request_digest(&redacted_url);
        Ok(Self {
            dataset,
            selection,
            predicates,
            geography,
            time,
            request_digest,
            redacted_url,
        })
    }

    /// Returns the exact dataset coordinate.
    pub const fn dataset(&self) -> &CensusDataset {
        &self.dataset
    }

    /// Returns the exact get/group selection.
    pub const fn selection(&self) -> &CensusSelection {
        &self.selection
    }

    /// Returns ordered non-geographic predicates.
    pub fn predicates(&self) -> &[CensusPredicate] {
        &self.predicates
    }

    /// Returns the geography selection.
    pub const fn geography(&self) -> &CensusGeography {
        &self.geography
    }

    /// Returns the optional time-series predicate.
    pub const fn time(&self) -> Option<CensusTimePredicate> {
        self.time
    }

    /// Returns the exact key-free URL for diagnostics and evidence.
    pub fn redacted_url(&self) -> &str {
        &self.redacted_url
    }

    /// Returns the exact key-free request identity.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Appends a key only to a transport-owned URL wrapper.
    pub(crate) fn authorize<'a>(
        &self,
        key: &'a CensusApiKey,
    ) -> Result<CensusAuthorizedUrl<'a>, CensusAdapterError> {
        authorize_url(&self.redacted_url, self.request_digest, Some(key))
    }
}

/// A Census machine-readable discovery endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CensusDiscoveryKind {
    /// All dataset vintages.
    Datasets,
    /// All datasets under one vintage.
    VintageDatasets { vintage: u16 },
    /// Dataset variable metadata.
    Variables { dataset: CensusDataset },
    /// Dataset group list.
    Groups { dataset: CensusDataset },
    /// Variables in one group.
    Group {
        dataset: CensusDataset,
        group: SourceIdentifier,
    },
    /// Dataset FIPS geography grammar.
    Geographies { dataset: CensusDataset },
}

/// One key-free discovery request with secret-independent identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CensusDiscoveryRequest {
    kind: CensusDiscoveryKind,
    request_digest: [u8; 32],
    redacted_url: String,
}

impl CensusDiscoveryRequest {
    /// Constructs one deterministic `.json` discovery request.
    ///
    /// # Errors
    ///
    /// Rejects invalid vintages, groups, or URL construction.
    pub fn try_new(kind: CensusDiscoveryKind) -> Result<Self, CensusAdapterError> {
        let redacted_url = discovery_url(&kind)?.to_string();
        let request_digest = request_digest(&redacted_url);
        Ok(Self {
            kind,
            request_digest,
            redacted_url,
        })
    }

    /// Convenience constructor for one group-detail request.
    pub fn group(
        dataset: CensusDataset,
        group: impl AsRef<str>,
    ) -> Result<Self, CensusAdapterError> {
        Self::try_new(CensusDiscoveryKind::Group {
            dataset,
            group: identifier(group.as_ref())?,
        })
    }

    /// Returns the discovery contract.
    pub const fn kind(&self) -> &CensusDiscoveryKind {
        &self.kind
    }

    /// Returns the key-free discovery URL.
    pub fn redacted_url(&self) -> &str {
        &self.redacted_url
    }

    /// Returns the key-free request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Builds the public metadata request wrapper without attaching the data-query API key.
    pub(crate) fn public_request(
        &self,
    ) -> Result<CensusAuthorizedUrl<'static>, CensusAdapterError> {
        authorize_url(&self.redacted_url, self.request_digest, None)
    }
}

fn dataset_url(dataset: &CensusDataset) -> Result<Url, CensusAdapterError> {
    let mut url = Url::parse(CENSUS_API_ORIGIN).map_err(|_| CensusAdapterError::InvalidQuery)?;
    dataset.append_to_url(&mut url)?;
    Ok(url)
}

fn discovery_url(kind: &CensusDiscoveryKind) -> Result<Url, CensusAdapterError> {
    let mut url = Url::parse(CENSUS_API_ORIGIN).map_err(|_| CensusAdapterError::InvalidQuery)?;
    match kind {
        CensusDiscoveryKind::Datasets => url.set_path("/data.json"),
        CensusDiscoveryKind::VintageDatasets { vintage } => {
            validate_year(*vintage)?;
            url.set_path(&format!("/data/{vintage:04}.json"));
        }
        CensusDiscoveryKind::Variables { dataset }
        | CensusDiscoveryKind::Groups { dataset }
        | CensusDiscoveryKind::Geographies { dataset } => {
            dataset.append_to_url(&mut url)?;
            let suffix = match kind {
                CensusDiscoveryKind::Variables { .. } => "variables.json",
                CensusDiscoveryKind::Groups { .. } => "groups.json",
                CensusDiscoveryKind::Geographies { .. } => "geography.json",
                _ => return Err(CensusAdapterError::InvalidQuery),
            };
            url.path_segments_mut()
                .map_err(|_| CensusAdapterError::InvalidQuery)?
                .push(suffix);
        }
        CensusDiscoveryKind::Group { dataset, group } => {
            dataset.append_to_url(&mut url)?;
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| CensusAdapterError::InvalidQuery)?;
            segments.push("groups");
            segments.push(&format!("{group}.json"));
        }
    }
    Ok(url)
}

fn authorize_url<'a>(
    redacted_url: &str,
    request_digest: [u8; 32],
    api_key: Option<&'a CensusApiKey>,
) -> Result<CensusAuthorizedUrl<'a>, CensusAdapterError> {
    let public_url = Url::parse(redacted_url).map_err(|_| CensusAdapterError::InvalidQuery)?;
    Ok(CensusAuthorizedUrl {
        public_url,
        api_key,
        redacted_url: redacted_url.to_owned(),
        request_digest,
    })
}

fn request_digest(redacted_url: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    update_digest_component(&mut hasher, b"market-squawk-census-request-v1");
    update_digest_component(&mut hasher, redacted_url.as_bytes());
    hasher.finalize().into()
}

fn validated_identifiers<I, S>(
    values: I,
    max: usize,
) -> Result<Vec<SourceIdentifier>, CensusAdapterError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = values
        .into_iter()
        .map(|value| identifier(value.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err(CensusAdapterError::InvalidQuery);
    }
    if values.len() > max {
        return Err(CensusAdapterError::VariableLimitExceeded);
    }
    if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(CensusAdapterError::DuplicateIdentity);
    }
    Ok(values)
}

fn validated_values<I, S>(values: I, max: usize) -> Result<Vec<String>, CensusAdapterError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let values = values
        .into_iter()
        .map(|value| {
            let value = value.as_ref();
            validate_bounded_value(value)?;
            Ok(value.to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() || values.len() > max {
        return Err(CensusAdapterError::ResourceLimitExceeded);
    }
    Ok(values)
}

fn validate_predicate_value(
    value: &str,
    predicate_type: &CensusPredicateType,
) -> Result<(), CensusAdapterError> {
    match predicate_type {
        CensusPredicateType::String => {
            if value.contains(':') {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
        CensusPredicateType::Integer => {
            if value.contains('*') || !valid_numeric_predicate(value, true) {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
        CensusPredicateType::Float => {
            if value.contains('*') || !valid_numeric_predicate(value, false) {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
        CensusPredicateType::FipsFor
        | CensusPredicateType::FipsIn
        | CensusPredicateType::Ucgid
        | CensusPredicateType::Time
        | CensusPredicateType::NotPredicate
        | CensusPredicateType::Unknown(_) => Err(CensusAdapterError::InvalidQuery),
    }
}

fn valid_numeric_predicate(value: &str, integer: bool) -> bool {
    let mut parts = value.split(':');
    let first = parts.next().unwrap_or_default();
    let second = parts.next();
    if parts.next().is_some() || first.is_empty() || second.is_some_and(str::is_empty) {
        return false;
    }
    let parses = |part: &str| {
        if integer {
            part.parse::<i128>().is_ok()
        } else {
            part.parse::<rust_decimal::Decimal>().is_ok()
        }
    };
    parses(first) && second.is_none_or(parses)
}

fn validate_bounded_value(value: &str) -> Result<(), CensusAdapterError> {
    if value.is_empty()
        || value.len() > MAX_QUERY_COMPONENT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(CensusAdapterError::InvalidComponent);
    }
    Ok(())
}

fn validate_year(year: u16) -> Result<(), CensusAdapterError> {
    if !(1000..=9999).contains(&year) {
        return Err(CensusAdapterError::InvalidVintage);
    }
    Ok(())
}

fn identifier(value: &str) -> Result<SourceIdentifier, CensusAdapterError> {
    SourceIdentifier::try_from(value).map_err(|_| CensusAdapterError::InvalidComponent)
}

fn push_unique(
    values: &mut Vec<SourceIdentifier>,
    seen: &mut BTreeSet<SourceIdentifier>,
    value: SourceIdentifier,
) {
    if seen.insert(value.clone()) {
        values.push(value);
    }
}

fn is_reserved_query_key(value: &str) -> bool {
    matches!(
        value,
        "get" | "for" | "in" | "ucgid" | "time" | "key" | "descriptive" | "outputFormat"
    )
}

fn validate_selection(selection: &CensusSelection) -> Result<(), CensusAdapterError> {
    match selection {
        CensusSelection::Variables { primary, wire } => {
            if primary.is_empty()
                || wire.is_empty()
                || wire.len() > CENSUS_PROVIDER_VARIABLE_LIMIT
                || primary.len() > CENSUS_PROVIDER_VARIABLE_LIMIT
                || primary.iter().collect::<BTreeSet<_>>().len() != primary.len()
                || wire.iter().collect::<BTreeSet<_>>().len() != wire.len()
                || primary.iter().any(|variable| !wire.contains(variable))
                || wire
                    .iter()
                    .any(|variable| is_reserved_query_key(variable.as_str()))
            {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
        CensusSelection::Group { .. } => Ok(()),
    }
}

fn validate_geography(geography: &CensusGeography) -> Result<(), CensusAdapterError> {
    match geography {
        CensusGeography::Standard {
            for_clause,
            in_clauses,
        } => {
            if in_clauses.len() > MAX_GEOGRAPHY_COMPONENTS {
                return Err(CensusAdapterError::ResourceLimitExceeded);
            }
            let mut levels = BTreeSet::new();
            levels.insert(for_clause.level.as_str());
            if in_clauses
                .iter()
                .any(|clause| !levels.insert(clause.level.as_str()))
            {
                return Err(CensusAdapterError::DuplicateIdentity);
            }
            validate_geography_clause(for_clause)?;
            for clause in in_clauses {
                validate_geography_clause(clause)?;
            }
            Ok(())
        }
        CensusGeography::Uniform { values } => {
            if values.is_empty()
                || values.len() > MAX_PREDICATE_VALUES
                || values.iter().collect::<BTreeSet<_>>().len() != values.len()
            {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
    }
}

fn validate_geography_clause(clause: &CensusGeographyClause) -> Result<(), CensusAdapterError> {
    validate_bounded_value(&clause.level)?;
    if clause.level.contains([':', ',', '&'])
        || clause.level.chars().any(char::is_whitespace)
        || clause.codes.is_empty()
        || clause.codes.len() > MAX_PREDICATE_VALUES
        || clause.codes.iter().collect::<BTreeSet<_>>().len() != clause.codes.len()
    {
        return Err(CensusAdapterError::InvalidQuery);
    }
    let wildcard_count = clause
        .codes
        .iter()
        .filter(|code| matches!(code, CensusGeographyCode::Wildcard))
        .count();
    if wildcard_count > 0 && (wildcard_count != 1 || clause.codes.len() != 1) {
        return Err(CensusAdapterError::InvalidQuery);
    }
    for code in &clause.codes {
        if let CensusGeographyCode::Exact(value) = code {
            validate_bounded_value(value)?;
            if value == "*" || value.contains(',') || value.chars().any(char::is_whitespace) {
                return Err(CensusAdapterError::InvalidQuery);
            }
        }
    }
    Ok(())
}

fn validate_time_predicate(time: CensusTimePredicate) -> Result<(), CensusAdapterError> {
    match time {
        CensusTimePredicate::At { point } => point.validate(),
        CensusTimePredicate::From { start } => start.validate(),
        CensusTimePredicate::To { end } => end.validate(),
        CensusTimePredicate::Range { start, end } => {
            start.validate()?;
            end.validate()?;
            if std::mem::discriminant(&start) != std::mem::discriminant(&end) || start > end {
                return Err(CensusAdapterError::InvalidQuery);
            }
            Ok(())
        }
    }
}
