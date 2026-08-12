//! Exact API-key request construction with a separate secret-free evidence identity.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use url::Url;
use zeroize::Zeroizing;

use crate::types::digest_bytes;
use crate::{
    EIA_API_ROOT, EIA_MAX_JSON_PAGE_ROWS, EiaDigest, EiaError, EiaFacetValue, EiaFieldId, EiaRoute,
};

const MAX_API_KEY_BYTES: usize = 1_024;
const MAX_DATA_FIELDS: usize = 64;
const MAX_FACETS: usize = 64;
const MAX_VALUES_PER_FACET: usize = 1_024;
const MAX_SORTS: usize = 8;

/// A bounded EIA API key whose debug representation is always redacted.
pub struct EiaApiKey(Zeroizing<String>);

impl EiaApiKey {
    /// Admits a nonempty key without inventing an undocumented provider format.
    pub fn try_new(value: impl Into<String>) -> Result<Self, EiaError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_API_KEY_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(EiaError::InvalidApiKey);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for EiaApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EiaApiKey([REDACTED])")
    }
}

/// Direction for one explicit EIA sort coordinate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EiaSortDirection {
    /// Ascending provider order.
    Ascending,
    /// Descending provider order.
    Descending,
}

impl EiaSortDirection {
    pub(crate) const fn as_query_value(self) -> &'static str {
        match self {
            Self::Ascending => "asc",
            Self::Descending => "desc",
        }
    }
}

/// One explicit sort coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaSort {
    column: EiaFieldId,
    direction: EiaSortDirection,
}

impl EiaSort {
    /// Constructs a sort coordinate.
    pub const fn new(column: EiaFieldId, direction: EiaSortDirection) -> Self {
        Self { column, direction }
    }

    /// Returns the provider column.
    pub const fn column(&self) -> &EiaFieldId {
        &self.column
    }

    /// Returns the requested direction.
    pub const fn direction(&self) -> EiaSortDirection {
        self.direction
    }
}

/// One facet filter with exact provider values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaFacetFilter {
    facet: EiaFieldId,
    values: Vec<EiaFacetValue>,
}

impl EiaFacetFilter {
    /// Constructs a nonempty, duplicate-free facet filter.
    pub fn try_new(facet: EiaFieldId, mut values: Vec<EiaFacetValue>) -> Result<Self, EiaError> {
        if values.is_empty() || values.len() > MAX_VALUES_PER_FACET {
            return Err(EiaError::InvalidLimit);
        }
        values.sort();
        if values.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(EiaError::InvalidIdentifier);
        }
        Ok(Self { facet, values })
    }

    /// Returns the facet identity.
    pub const fn facet(&self) -> &EiaFieldId {
        &self.facet
    }

    /// Returns exact sorted requested values.
    pub fn values(&self) -> &[EiaFacetValue] {
        &self.values
    }
}

/// Complete input to an immutable EIA route-data query.
#[derive(Clone, Debug)]
pub struct EiaDataQueryInput {
    /// Hierarchical route.
    pub route: EiaRoute,
    /// Selected provider data columns.
    pub data_fields: Vec<EiaFieldId>,
    /// Optional facet filters.
    pub facets: Vec<EiaFacetFilter>,
    /// Explicit provider frequency.
    pub frequency: EiaFieldId,
    /// Optional exact source period lower bound.
    pub start: Option<String>,
    /// Optional exact source period upper bound.
    pub end: Option<String>,
    /// Explicit stable ordering; at least one coordinate is mandatory.
    pub sorts: Vec<EiaSort>,
    /// Requested JSON rows per page.
    pub length: u16,
}

/// Immutable, secret-free base query shared by all offset pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaDataQuery {
    route: EiaRoute,
    data_fields: Vec<EiaFieldId>,
    facets: Vec<EiaFacetFilter>,
    frequency: EiaFieldId,
    start: Option<String>,
    end: Option<String>,
    sorts: Vec<EiaSort>,
    length: u16,
    identity: EiaDigest,
}

impl EiaDataQuery {
    /// Validates and canonicalizes a route-data query.
    pub fn try_new(input: EiaDataQueryInput) -> Result<Self, EiaError> {
        let EiaDataQueryInput {
            route,
            mut data_fields,
            mut facets,
            frequency,
            start,
            end,
            sorts,
            length,
        } = input;
        if data_fields.is_empty()
            || data_fields.len() > MAX_DATA_FIELDS
            || facets.len() > MAX_FACETS
            || sorts.is_empty()
            || sorts.len() > MAX_SORTS
            || length == 0
            || usize::from(length) > EIA_MAX_JSON_PAGE_ROWS
        {
            return Err(EiaError::InvalidLimit);
        }
        validate_period_bound(start.as_deref())?;
        validate_period_bound(end.as_deref())?;
        if let (Some(start), Some(end)) = (start.as_deref(), end.as_deref())
            && start > end
        {
            return Err(EiaError::InvalidClock);
        }

        data_fields.sort();
        if data_fields.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(EiaError::InvalidIdentifier);
        }
        facets.sort_by(|left, right| left.facet.cmp(&right.facet));
        if facets.windows(2).any(|pair| pair[0].facet == pair[1].facet) {
            return Err(EiaError::InvalidIdentifier);
        }
        let mut sort_columns = BTreeSet::new();
        if sorts
            .iter()
            .any(|sort| !sort_columns.insert(sort.column.clone()))
        {
            return Err(EiaError::InvalidIdentifier);
        }

        let mut query = Self {
            route,
            data_fields,
            facets,
            frequency,
            start,
            end,
            sorts,
            length,
            identity: EiaDigest::new([0; 32]),
        };
        query.identity = digest_bytes(query.secret_free_base_locator()?.as_bytes());
        Ok(query)
    }

    /// Returns the exact route.
    pub const fn route(&self) -> &EiaRoute {
        &self.route
    }

    /// Returns sorted selected data fields.
    pub fn data_fields(&self) -> &[EiaFieldId] {
        &self.data_fields
    }

    /// Returns sorted facet filters.
    pub fn facets(&self) -> &[EiaFacetFilter] {
        &self.facets
    }

    /// Returns the explicit frequency.
    pub const fn frequency(&self) -> &EiaFieldId {
        &self.frequency
    }

    /// Returns the optional lower period bound.
    pub fn start(&self) -> Option<&str> {
        self.start.as_deref()
    }

    /// Returns the optional upper period bound.
    pub fn end(&self) -> Option<&str> {
        self.end.as_deref()
    }

    /// Returns the explicit ordered sort coordinates.
    pub fn sorts(&self) -> &[EiaSort] {
        &self.sorts
    }

    /// Returns requested rows per page.
    pub const fn length(&self) -> u16 {
        self.length
    }

    /// Returns the stable secret-free identity shared by every offset page.
    pub const fn identity(&self) -> EiaDigest {
        self.identity
    }

    /// Selects one exact zero-based offset page.
    pub const fn page(&self, offset: u64) -> EiaDataPageRequest<'_> {
        EiaDataPageRequest {
            query: self,
            offset,
        }
    }

    fn secret_free_base_locator(&self) -> Result<String, EiaError> {
        let mut url = route_url(&self.route, &["data"])?;
        append_query(&mut url, self, None, None);
        Ok(url.into())
    }
}

/// One exact offset page of a base query.
#[derive(Clone, Copy, Debug)]
pub struct EiaDataPageRequest<'a> {
    query: &'a EiaDataQuery,
    offset: u64,
}

impl<'a> EiaDataPageRequest<'a> {
    /// Returns the base query.
    pub const fn query(self) -> &'a EiaDataQuery {
        self.query
    }

    /// Returns the exact requested offset.
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Builds an authenticated request while retaining a separate secret-free identity.
    pub fn authenticate(self, api_key: &EiaApiKey) -> Result<EiaAuthenticatedRequest, EiaError> {
        let mut secret_free = route_url(self.query.route(), &["data"])?;
        append_query(&mut secret_free, self.query, Some(self.offset), None);
        authenticated(secret_free, api_key)
    }

    /// Builds the exact secret-free evidence locator and digest without a credential.
    pub fn secret_free(self) -> Result<EiaAuthenticatedRequest, EiaError> {
        let mut secret_free = route_url(self.query.route(), &["data"])?;
        append_query(&mut secret_free, self.query, Some(self.offset), None);
        EiaAuthenticatedRequest::without_credential(secret_free)
    }
}

/// Metadata request surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EiaMetadataRequestKind {
    /// Route hierarchy, frequency, facet, and data-column metadata.
    Route,
    /// Available values for one exact facet.
    Facet(EiaFieldId),
}

/// One exact route or facet metadata request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EiaMetadataRequest {
    route: EiaRoute,
    kind: EiaMetadataRequestKind,
}

impl EiaMetadataRequest {
    /// Constructs route metadata discovery.
    pub const fn route(route: EiaRoute) -> Self {
        Self {
            route,
            kind: EiaMetadataRequestKind::Route,
        }
    }

    /// Constructs exact facet-value metadata discovery.
    pub const fn facet(route: EiaRoute, facet: EiaFieldId) -> Self {
        Self {
            route,
            kind: EiaMetadataRequestKind::Facet(facet),
        }
    }

    /// Returns the route.
    pub const fn route_value(&self) -> &EiaRoute {
        &self.route
    }

    /// Returns the metadata surface.
    pub const fn kind(&self) -> &EiaMetadataRequestKind {
        &self.kind
    }

    /// Builds an authenticated metadata request.
    pub fn authenticate(&self, api_key: &EiaApiKey) -> Result<EiaAuthenticatedRequest, EiaError> {
        authenticated(self.secret_free_url()?, api_key)
    }

    /// Builds a secret-free request identity for parsing fixtures or delegated transport.
    pub fn secret_free(&self) -> Result<EiaAuthenticatedRequest, EiaError> {
        EiaAuthenticatedRequest::without_credential(self.secret_free_url()?)
    }

    pub(crate) fn expected_command(&self) -> Result<String, EiaError> {
        Ok(self
            .secret_free_url()?
            .path()
            .trim_end_matches('/')
            .to_owned())
    }

    fn secret_free_url(&self) -> Result<Url, EiaError> {
        match &self.kind {
            EiaMetadataRequestKind::Route => route_url(&self.route, &[]),
            EiaMetadataRequestKind::Facet(facet) => {
                route_url(&self.route, &["facet", facet.as_str()])
            }
        }
    }
}

/// Exact authenticated URL plus the only loggable/request-evidence URL.
pub struct EiaAuthenticatedRequest {
    authenticated_url: Option<Url>,
    secret_free_url: Url,
    request_digest: EiaDigest,
}

impl EiaAuthenticatedRequest {
    fn without_credential(secret_free_url: Url) -> Result<Self, EiaError> {
        let request_digest = digest_bytes(secret_free_url.as_str().as_bytes());
        Ok(Self {
            authenticated_url: None,
            secret_free_url,
            request_digest,
        })
    }

    /// Returns the credential-bearing URL only inside the bounded transport boundary.
    #[cfg(test)]
    pub(crate) fn authenticated_url(&self) -> Option<&Url> {
        self.authenticated_url.as_ref()
    }

    pub(crate) fn into_urls(self) -> Result<(Url, Url), EiaError> {
        let authenticated = self
            .authenticated_url
            .ok_or(EiaError::RequestConstruction)?;
        Ok((authenticated, self.secret_free_url))
    }

    /// Returns the exact safe URL admitted for logs, receipts, and cache identity.
    pub const fn secret_free_url(&self) -> &Url {
        &self.secret_free_url
    }

    /// Returns the SHA-256 identity of the secret-free URL.
    pub const fn request_digest(&self) -> EiaDigest {
        self.request_digest
    }
}

impl fmt::Debug for EiaAuthenticatedRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EiaAuthenticatedRequest")
            .field("url", &self.secret_free_url)
            .field("request_digest", &self.request_digest)
            .field(
                "authentication",
                &self.authenticated_url.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

fn authenticated(
    secret_free_url: Url,
    api_key: &EiaApiKey,
) -> Result<EiaAuthenticatedRequest, EiaError> {
    let mut authenticated_url = secret_free_url.clone();
    authenticated_url
        .query_pairs_mut()
        .append_pair("api_key", api_key.expose());
    let request_digest = digest_bytes(secret_free_url.as_str().as_bytes());
    Ok(EiaAuthenticatedRequest {
        authenticated_url: Some(authenticated_url),
        secret_free_url,
        request_digest,
    })
}

fn route_url(route: &EiaRoute, suffix: &[&str]) -> Result<Url, EiaError> {
    let mut url = Url::parse(EIA_API_ROOT).map_err(|_| EiaError::RequestConstruction)?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| EiaError::RequestConstruction)?;
        segments.pop_if_empty();
        for segment in route.segments() {
            segments.push(segment);
        }
        for segment in suffix {
            segments.push(segment);
        }
    }
    Ok(url)
}

fn append_query(
    url: &mut Url,
    query: &EiaDataQuery,
    offset: Option<u64>,
    additional: Option<&BTreeMap<String, String>>,
) {
    let mut pairs = url.query_pairs_mut();
    for field in query.data_fields() {
        pairs.append_pair("data[]", field.as_str());
    }
    for facet in query.facets() {
        let key = format!("facets[{}][]", facet.facet().as_str());
        for value in facet.values() {
            pairs.append_pair(&key, value.as_str());
        }
    }
    pairs.append_pair("frequency", query.frequency().as_str());
    if let Some(start) = query.start() {
        pairs.append_pair("start", start);
    }
    if let Some(end) = query.end() {
        pairs.append_pair("end", end);
    }
    for (index, sort) in query.sorts().iter().enumerate() {
        pairs.append_pair(&format!("sort[{index}][column]"), sort.column().as_str());
        pairs.append_pair(
            &format!("sort[{index}][direction]"),
            sort.direction().as_query_value(),
        );
    }
    if let Some(offset) = offset {
        pairs.append_pair("offset", &offset.to_string());
    }
    pairs.append_pair("length", &query.length().to_string());
    pairs.append_pair("out", "json");
    if let Some(additional) = additional {
        for (key, value) in additional {
            pairs.append_pair(key, value);
        }
    }
}

fn validate_period_bound(value: Option<&str>) -> Result<(), EiaError> {
    if value.is_some_and(|value| {
        value.is_empty() || value.len() > 128 || value.chars().any(char::is_control)
    }) {
        Err(EiaError::InvalidClock)
    } else {
        Ok(())
    }
}
