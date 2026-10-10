//! Exact credential-free BEA query and application-page identities.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use sha2::{Digest, Sha256};

use crate::{
    BeaAuthorizedRequest, BeaDatasetIdentity, BeaError, BeaMetadataGeneration,
    BeaParameterIdentity, BeaUserId,
};

/// The sole reviewed BEA API endpoint.
pub const BEA_API_ENDPOINT: &str = "https://apps.bea.gov/api/data";
/// Application policy: maximum independent selector pages in one acquisition plan.
pub const BEA_MAX_APPLICATION_PAGES: u32 = 1_024;
/// Application policy: maximum parsed rows in one response.
///
/// BEA documents neither a universal row ceiling nor provider pagination. This is deliberately an
/// application memory/admission bound, not a provider fact.
pub const BEA_MAX_APPLICATION_ROWS_PER_PAGE: usize = 100_000;

const MAX_PARAMETERS: usize = 32;
const MAX_PARAMETER_VALUE_BYTES: usize = 4 * 1024;
const MAX_VALUES_PER_PARAMETER: usize = 1_000;

/// The five documented BEA API methods used by Market Squawk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BeaMethod {
    /// Discover datasets.
    GetDatasetList,
    /// Discover one dataset's parameter contracts.
    GetParameterList,
    /// Discover all values for one parameter.
    GetParameterValues,
    /// Discover values filtered by other dataset parameters.
    GetParameterValuesFiltered,
    /// Retrieve observations.
    GetData,
}

impl BeaMethod {
    /// Returns the exact method query value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetDatasetList => "GetDatasetList",
            Self::GetParameterList => "GetParameterList",
            Self::GetParameterValues => "GetParameterValues",
            Self::GetParameterValuesFiltered => "GetParameterValuesFiltered",
            Self::GetData => "GetData",
        }
    }

    /// Returns whether this method produces metadata rather than observations.
    pub const fn is_metadata(self) -> bool {
        !matches!(self, Self::GetData)
    }
}

/// One provider-independent application page in a sequence of explicit BEA selector requests.
///
/// The page numbers are local plan evidence. They are never sent as provider pagination because
/// the reviewed BEA API publishes no pagination parameters or total-row response field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeaPageScope {
    page_number: NonZeroU32,
    page_count: NonZeroU32,
    expected_rows: Option<usize>,
}

impl BeaPageScope {
    /// Creates a bounded application page with optional metadata-derived row cardinality.
    ///
    /// # Errors
    ///
    /// Rejects page numbers beyond the plan, plans above 1,024 pages, or expected rows above the
    /// application parser ceiling.
    pub fn try_new(
        page_number: NonZeroU32,
        page_count: NonZeroU32,
        expected_rows: Option<usize>,
    ) -> Result<Self, BeaError> {
        if page_number > page_count
            || page_count.get() > BEA_MAX_APPLICATION_PAGES
            || expected_rows.is_some_and(|rows| rows > BEA_MAX_APPLICATION_ROWS_PER_PAGE)
        {
            return Err(BeaError::InvalidLimit);
        }
        Ok(Self {
            page_number,
            page_count,
            expected_rows,
        })
    }

    /// Builds the common one-response scope.
    pub fn single(expected_rows: Option<usize>) -> Result<Self, BeaError> {
        let one = NonZeroU32::new(1).ok_or(BeaError::InvalidLimit)?;
        Self::try_new(one, one, expected_rows)
    }

    /// Returns the one-based local page number.
    pub const fn page_number(self) -> u32 {
        self.page_number.get()
    }

    /// Returns the complete local plan page count.
    pub const fn page_count(self) -> u32 {
        self.page_count.get()
    }

    /// Returns an exact metadata-derived expected row count when one exists.
    pub const fn expected_rows(self) -> Option<usize> {
        self.expected_rows
    }
}

/// Credential-free BEA query semantics before application page binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaQuery {
    method: BeaMethod,
    dataset: Option<BeaDatasetIdentity>,
    parameter: Option<BeaParameterIdentity>,
    target_parameter: Option<BeaParameterIdentity>,
    supplied_parameters: BTreeMap<BeaParameterIdentity, String>,
    metadata_generation: Option<BeaMetadataGeneration>,
    query_pairs: Vec<(String, String)>,
    query_digest: [u8; 32],
}

impl BeaQuery {
    /// Builds the exact dataset-catalog query.
    pub fn dataset_list() -> Result<Self, BeaError> {
        Self::build(
            BeaMethod::GetDatasetList,
            None,
            None,
            None,
            BTreeMap::new(),
            None,
        )
    }

    /// Builds exact parameter discovery for one dataset.
    pub fn parameter_list(dataset: BeaDatasetIdentity) -> Result<Self, BeaError> {
        Self::build(
            BeaMethod::GetParameterList,
            Some(dataset),
            None,
            None,
            BTreeMap::new(),
            None,
        )
    }

    /// Builds exact valid-value discovery for one dataset parameter.
    pub fn parameter_values(
        dataset: BeaDatasetIdentity,
        parameter: BeaParameterIdentity,
    ) -> Result<Self, BeaError> {
        Self::build(
            BeaMethod::GetParameterValues,
            Some(dataset),
            Some(parameter),
            None,
            BTreeMap::new(),
            None,
        )
    }

    /// Builds a filtered parameter-value query for datasets that implement method error 34's
    /// optional surface.
    pub fn parameter_values_filtered(
        dataset: BeaDatasetIdentity,
        target_parameter: BeaParameterIdentity,
        filters: BTreeMap<BeaParameterIdentity, String>,
    ) -> Result<Self, BeaError> {
        if filters.is_empty() {
            return Err(BeaError::InvalidRequest);
        }
        Self::build(
            BeaMethod::GetParameterValuesFiltered,
            Some(dataset),
            None,
            Some(target_parameter),
            filters,
            None,
        )
    }

    /// Builds a metadata-generation-bound observation query.
    ///
    /// Each parameter value is the exact discovered provider value or comma-separated set. At
    /// most one parameter may use `ALL` or `X`, matching the reviewed BEA broad-query guidance and
    /// Market Squawk's stricter application policy.
    pub fn data(
        dataset: BeaDatasetIdentity,
        parameters: BTreeMap<BeaParameterIdentity, String>,
        metadata_generation: BeaMetadataGeneration,
    ) -> Result<Self, BeaError> {
        if parameters.is_empty() {
            return Err(BeaError::InvalidRequest);
        }
        Self::build(
            BeaMethod::GetData,
            Some(dataset),
            None,
            None,
            parameters,
            Some(metadata_generation),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the five method-specific request coordinates remain structurally distinct"
    )]
    fn build(
        method: BeaMethod,
        dataset: Option<BeaDatasetIdentity>,
        parameter: Option<BeaParameterIdentity>,
        target_parameter: Option<BeaParameterIdentity>,
        supplied_parameters: BTreeMap<BeaParameterIdentity, String>,
        metadata_generation: Option<BeaMetadataGeneration>,
    ) -> Result<Self, BeaError> {
        validate_shape(
            method,
            dataset.as_ref(),
            parameter.as_ref(),
            target_parameter.as_ref(),
            &supplied_parameters,
            metadata_generation,
        )?;
        validate_parameters(method, target_parameter.as_ref(), &supplied_parameters)?;

        let mut query_pairs = Vec::new();
        query_pairs
            .try_reserve_exact(4usize.saturating_add(supplied_parameters.len()))
            .map_err(|_| BeaError::Allocation)?;
        query_pairs.push(("Method".to_owned(), method.as_str().to_owned()));
        if let Some(dataset) = dataset.as_ref() {
            query_pairs.push(("DatasetName".to_owned(), dataset.as_str().to_owned()));
        }
        if let Some(parameter) = parameter.as_ref() {
            query_pairs.push(("ParameterName".to_owned(), parameter.as_str().to_owned()));
        }
        if let Some(target) = target_parameter.as_ref() {
            query_pairs.push(("TargetParameter".to_owned(), target.as_str().to_owned()));
        }
        query_pairs.extend(
            supplied_parameters
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value.clone())),
        );
        query_pairs.push(("ResultFormat".to_owned(), "JSON".to_owned()));
        let query_digest = digest_query(&query_pairs, metadata_generation)?;
        Ok(Self {
            method,
            dataset,
            parameter,
            target_parameter,
            supplied_parameters,
            metadata_generation,
            query_pairs,
            query_digest,
        })
    }

    /// Binds a local page and optional expected row count into the exact request identity.
    pub fn page(self, scope: BeaPageScope) -> Result<BeaRequest, BeaError> {
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk-bea-page-request-v1");
        hasher.update(self.query_digest);
        hasher.update(scope.page_number().to_be_bytes());
        hasher.update(scope.page_count().to_be_bytes());
        match scope.expected_rows() {
            Some(rows) => {
                hasher.update([1]);
                hasher.update(
                    u64::try_from(rows)
                        .map_err(|_| BeaError::InvalidLimit)?
                        .to_be_bytes(),
                );
            }
            None => hasher.update([0]),
        }
        Ok(BeaRequest {
            query: self,
            scope,
            request_digest: hasher.finalize().into(),
        })
    }

    /// Builds the common single-response request.
    pub fn single_page(self, expected_rows: Option<usize>) -> Result<BeaRequest, BeaError> {
        self.page(BeaPageScope::single(expected_rows)?)
    }

    /// Returns the exact BEA method.
    pub const fn method(&self) -> BeaMethod {
        self.method
    }

    /// Returns the selected dataset when the method requires one.
    pub const fn dataset(&self) -> Option<&BeaDatasetIdentity> {
        self.dataset.as_ref()
    }

    /// Returns the selected parameter for unfiltered value discovery.
    pub const fn parameter(&self) -> Option<&BeaParameterIdentity> {
        self.parameter.as_ref()
    }

    /// Returns the selected target parameter for filtered discovery.
    pub const fn target_parameter(&self) -> Option<&BeaParameterIdentity> {
        self.target_parameter.as_ref()
    }

    /// Returns method-specific filter or data parameters in deterministic identity order.
    pub const fn supplied_parameters(&self) -> &BTreeMap<BeaParameterIdentity, String> {
        &self.supplied_parameters
    }

    /// Returns the discovery generation required by `GetData`.
    pub const fn metadata_generation(&self) -> Option<BeaMetadataGeneration> {
        self.metadata_generation
    }

    /// Returns the credential-free query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }
}

/// One exact BEA request including local plan page scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaRequest {
    query: BeaQuery,
    scope: BeaPageScope,
    request_digest: [u8; 32],
}

impl BeaRequest {
    /// Borrows the credential and constructs the exact GET URL in zeroizing storage.
    pub fn authorize(&self, user_id: &BeaUserId) -> Result<BeaAuthorizedRequest, BeaError> {
        BeaAuthorizedRequest::build(self, user_id)
    }

    /// Returns the credential-free query semantics.
    pub const fn query(&self) -> &BeaQuery {
        &self.query
    }

    /// Returns the local page evidence.
    pub const fn page_scope(&self) -> BeaPageScope {
        self.scope
    }

    /// Returns the credential-free full request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    pub(crate) fn query_pairs(&self) -> impl Iterator<Item = (&str, &str)> {
        self.query
            .query_pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

fn validate_shape(
    method: BeaMethod,
    dataset: Option<&BeaDatasetIdentity>,
    parameter: Option<&BeaParameterIdentity>,
    target: Option<&BeaParameterIdentity>,
    supplied: &BTreeMap<BeaParameterIdentity, String>,
    generation: Option<BeaMetadataGeneration>,
) -> Result<(), BeaError> {
    let valid = match method {
        BeaMethod::GetDatasetList => {
            dataset.is_none()
                && parameter.is_none()
                && target.is_none()
                && supplied.is_empty()
                && generation.is_none()
        }
        BeaMethod::GetParameterList => {
            dataset.is_some()
                && parameter.is_none()
                && target.is_none()
                && supplied.is_empty()
                && generation.is_none()
        }
        BeaMethod::GetParameterValues => {
            dataset.is_some()
                && parameter.is_some()
                && target.is_none()
                && supplied.is_empty()
                && generation.is_none()
        }
        BeaMethod::GetParameterValuesFiltered => {
            dataset.is_some()
                && parameter.is_none()
                && target.is_some()
                && !supplied.is_empty()
                && generation.is_none()
        }
        BeaMethod::GetData => {
            dataset.is_some()
                && parameter.is_none()
                && target.is_none()
                && !supplied.is_empty()
                && generation.is_some()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(BeaError::InvalidRequest)
    }
}

fn validate_parameters(
    method: BeaMethod,
    target: Option<&BeaParameterIdentity>,
    parameters: &BTreeMap<BeaParameterIdentity, String>,
) -> Result<(), BeaError> {
    if parameters.len() > MAX_PARAMETERS {
        return Err(BeaError::InvalidRequest);
    }
    let reserved = [
        "USERID",
        "METHOD",
        "DATASETNAME",
        "PARAMETERNAME",
        "TARGETPARAMETER",
        "RESULTFORMAT",
        "JSONP",
    ];
    let mut names = BTreeSet::new();
    let mut broad_selectors = 0usize;
    for (name, value) in parameters {
        let uppercase = name.as_str().to_ascii_uppercase();
        if reserved.contains(&uppercase.as_str())
            || !names.insert(uppercase)
            || target.is_some_and(|target| target.as_str().eq_ignore_ascii_case(name.as_str()))
        {
            return Err(BeaError::InvalidRequest);
        }
        validate_parameter_value(value)?;
        broad_selectors = broad_selectors.saturating_add(
            value
                .split(',')
                .filter(|part| part.eq_ignore_ascii_case("ALL") || *part == "X")
                .count(),
        );
    }
    if method == BeaMethod::GetData && broad_selectors > 1 {
        return Err(BeaError::InvalidRequest);
    }
    Ok(())
}

fn validate_parameter_value(value: &str) -> Result<(), BeaError> {
    if value.is_empty()
        || value.len() > MAX_PARAMETER_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(BeaError::InvalidRequest);
    }
    let mut count = 0usize;
    for part in value.split(',') {
        if part.trim().is_empty() {
            return Err(BeaError::InvalidRequest);
        }
        count = count.checked_add(1).ok_or(BeaError::InvalidRequest)?;
    }
    if count > MAX_VALUES_PER_PARAMETER {
        return Err(BeaError::InvalidRequest);
    }
    Ok(())
}

fn digest_query(
    pairs: &[(String, String)],
    generation: Option<BeaMetadataGeneration>,
) -> Result<[u8; 32], BeaError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-bea-query-v1");
    hasher.update(
        u64::try_from(pairs.len())
            .map_err(|_| BeaError::InvalidRequest)?
            .to_be_bytes(),
    );
    for (name, value) in pairs {
        hash_text(&mut hasher, name)?;
        hash_text(&mut hasher, value)?;
    }
    match generation {
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.digest());
        }
        None => hasher.update([0]),
    }
    Ok(hasher.finalize().into())
}

fn hash_text(hasher: &mut Sha256, value: &str) -> Result<(), BeaError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| BeaError::InvalidRequest)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}
