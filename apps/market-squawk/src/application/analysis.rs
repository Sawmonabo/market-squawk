//! Manifest-pinned analytical kernels, feature datasets, and governed backtest services.

use std::{collections::HashSet, fmt, str::FromStr, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_analytics::{
    ExactDecimalUnit, FactorRegressionResult, MonetaryBasis, PortfolioAttribution,
    StatisticalResult,
};
use market_squawk_data::{
    AnalyticalFeatureDataset, AnalyticalReadCapability, AnalyticalReadError, AnalyticalReadLimit,
    DatasetId, ManifestCatalogError, QueryError,
};
use market_squawk_domain::{InstrumentId, RoundingPolicy, SourceId, Timestamp};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde_json::{Map, Value, json};

use super::{
    ApplicationDomainService,
    domain_support::{DomainLifecycle, admitted_result_limits, ensure_request_live},
};

mod backtest;
mod catalog;
mod serialization;

pub use backtest::{
    BacktestScope, GovernedBacktestAuthority, GovernedBacktestCommand,
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputAuthorityLimits,
    GovernedBacktestInputRegistrar, GovernedBacktestInputRegistrationInput,
    GovernedBacktestInputRegistrationJsonError, GovernedBacktestInputRegistrationReceipt,
    GovernedBacktestInputResolver, GovernedBacktestPortfolioSeedInput,
    GovernedBacktestQueryLimitsInput, GovernedBacktestRecord, GovernedBacktestRepository,
    GovernedBacktestRepositoryLimits, MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES,
    ProductionBacktestAuthority, ProductionGovernedBacktestInputAuthority,
    ProductionGovernedBacktestInputAuthorityError, ProductionGovernedBacktestRepository,
    ProductionGovernedBacktestRepositoryError, ResolvedGovernedBacktestInput,
};
pub use catalog::{
    AnalysisCatalog, AnalysisCatalogError, AnalysisDataset, AnalysisDatasetScope,
    FactorAnalysisInput, FeatureDatasetRegistration, ReturnAnalysisInput, ScenarioAnalysisInput,
    ValuationAnalysisInput,
};

use catalog::FeatureDatasetRegistration as FeatureDataset;
use serialization::{
    feature_dataset_value, feature_metadata_value, manifest_value, published_feature_dataset_value,
};

const GET_RETURNS: &str = "Analysis.GetReturns";
const GET_FACTORS: &str = "Analysis.GetFactors";
const GET_VALUATION: &str = "Analysis.GetValuation";
const GET_SCENARIOS: &str = "Analysis.GetScenarios";
const GET_FEATURE_DATASETS: &str = "Analysis.GetFeatureDatasets";
const GET_BACKTESTS: &str = "Analysis.GetBacktests";
const RUN_BACKTEST: &str = "Analysis.RunBacktest";

/// Application-owned analytical surface over immutable inputs and governed experiment authority.
pub struct AnalysisDomainService {
    catalog: Arc<AnalysisCatalog>,
    feature_reader: Option<AnalyticalReadCapability>,
    backtest_inputs: Arc<dyn GovernedBacktestInputRegistrar>,
    backtests: Arc<dyn GovernedBacktestAuthority>,
    lifecycle: Arc<DomainLifecycle>,
}

impl AnalysisDomainService {
    /// Binds immutable analytical input generations and the sole governed backtest authority.
    #[must_use]
    pub fn new(
        catalog: Arc<AnalysisCatalog>,
        backtest_inputs: Arc<dyn GovernedBacktestInputRegistrar>,
        backtests: Arc<dyn GovernedBacktestAuthority>,
    ) -> Self {
        Self {
            catalog,
            feature_reader: None,
            backtest_inputs,
            backtests,
            lifecycle: DomainLifecycle::new(),
        }
    }

    /// Binds the durable feature-dataset registry in addition to immutable analytical inputs.
    #[must_use]
    pub fn new_with_feature_reader(
        catalog: Arc<AnalysisCatalog>,
        feature_reader: AnalyticalReadCapability,
        backtest_inputs: Arc<dyn GovernedBacktestInputRegistrar>,
        backtests: Arc<dyn GovernedBacktestAuthority>,
    ) -> Self {
        Self {
            catalog,
            feature_reader: Some(feature_reader),
            backtest_inputs,
            backtests,
            lifecycle: DomainLifecycle::new(),
        }
    }

    fn returns(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let selected = self.selected_dataset(request)?;
        let input = selected.returns().ok_or(ServiceError::NotFound)?;
        ensure_request_live(context, &self.lifecycle)?;
        let calculated = input.calculate().map_err(|_| ServiceError::Internal)?;
        ensure_request_live(context, &self.lifecycle)?;
        let limits = admitted_result_limits(request, context)?;
        let available = calculated.values().len();
        let values = calculated
            .values()
            .iter()
            .take(limits.maximum_result_items())
            .map(|value| value.value())
            .collect::<Vec<_>>();
        source_result(
            json!({
                "manifest": manifest_value(selected.pinned().manifest()),
                "returnKind": match input {
                    ReturnAnalysisInput::Simple(_) => "price",
                    ReturnAnalysisInput::Total { .. } => "total"
                },
                "values": values
            }),
            values.len(),
            available,
            selected.scope(),
            request,
            context,
        )
    }

    fn factors(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let selected = self.selected_dataset(request)?;
        let input = selected.factors().ok_or(ServiceError::NotFound)?;
        ensure_request_live(context, &self.lifecycle)?;
        let result = market_squawk_analytics::factor_regression(input.observations())
            .map_err(|_| ServiceError::Internal)?;
        ensure_request_live(context, &self.lifecycle)?;
        let limits = admitted_result_limits(request, context)?;
        let available = result.exposures().len();
        let exposures = input
            .names()
            .iter()
            .zip(result.exposures())
            .take(limits.maximum_result_items())
            .map(|(name, value)| {
                json!({
                    "factor": name.as_str(),
                    "estimate": statistical_result_value(*value)
                })
            })
            .collect::<Vec<_>>();
        source_result(
            factor_result_value(selected, &result, exposures.clone()),
            exposures.len(),
            available,
            selected.scope(),
            request,
            context,
        )
    }

    fn valuation(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let selected = self.selected_dataset(request)?;
        let input = selected.valuation().ok_or(ServiceError::NotFound)?;
        ensure_request_live(context, &self.lifecycle)?;
        let result = input.calculate().map_err(|_| ServiceError::Internal)?;
        ensure_request_live(context, &self.lifecycle)?;
        source_result(
            json!({
                "manifest": manifest_value(selected.pinned().manifest()),
                "measure": "valuation_multiple",
                "value": result.value().to_string(),
                "unit": exact_decimal_unit_name(result.unit()),
                "decimalPolicy": {
                    "scale": result.policy().scale(),
                    "rounding": rounding_policy_name(result.policy().rounding())
                }
            }),
            1,
            1,
            selected.scope(),
            request,
            context,
        )
    }

    fn scenarios(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let selected = self.selected_dataset(request)?;
        let input = selected.scenario().ok_or(ServiceError::NotFound)?;
        ensure_request_live(context, &self.lifecycle)?;
        let result = input.calculate().map_err(|_| ServiceError::Internal)?;
        ensure_request_live(context, &self.lifecycle)?;
        let limits = admitted_result_limits(request, context)?;
        let available = result.contributions().len();
        let contributions = result
            .contributions()
            .iter()
            .take(limits.maximum_result_items())
            .map(|contribution| {
                json!({
                    "dimension": contribution.dimension(),
                    "amount": monetary_value(contribution.amount())
                })
            })
            .collect::<Vec<_>>();
        source_result(
            scenario_result_value(selected, &result, contributions.clone()),
            contributions.len(),
            available,
            selected.scope(),
            request,
            context,
        )
    }

    fn feature_datasets(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let scope = ReadScope::from_arguments(request.arguments())?;
        let requested_dataset = optional_feature_dataset(request, "dataset")?;
        let after_dataset = optional_feature_dataset(request, "afterDataset")?;
        if requested_dataset.is_some() && after_dataset.is_some() {
            return Err(ServiceError::InvalidRequest);
        }
        if after_dataset.is_some() && scope.has_filter() {
            return Err(ServiceError::InvalidRequest);
        }
        let continuation = after_dataset.is_some();
        let limits = admitted_result_limits(request, context)?;
        let mut selected = if continuation {
            Vec::new()
        } else {
            self.catalog
                .feature_datasets()
                .iter()
                .filter(|dataset| {
                    requested_dataset.as_ref().is_none_or(|requested| {
                        dataset.dataset().manifest().dataset_id() == requested
                    }) && scope.matches(dataset.scope())
                })
                .collect::<Vec<_>>()
        };
        selected.sort_unstable_by(|left, right| {
            left.dataset()
                .manifest()
                .dataset_id()
                .as_str()
                .cmp(right.dataset().manifest().dataset_id().as_str())
        });
        self.remove_durable_legacy_overlaps(&mut selected, &scope, context)?;
        let static_available = if continuation {
            0
        } else {
            self.catalog.feature_catalog().entries().len()
        };
        let first_page_available = static_available
            .checked_add(selected.len())
            .ok_or(ServiceError::ResourceExhausted)?;
        let published_limit = limits
            .maximum_result_items()
            .saturating_sub(first_page_available)
            .max(1)
            .min(64);
        let (published, published_available, published_has_more) = self
            .published_feature_datasets(
                requested_dataset.as_ref(),
                after_dataset.as_ref(),
                &scope,
                published_limit,
                context,
            )?;
        if requested_dataset.is_some() && selected.is_empty() && published.is_empty() {
            return Err(ServiceError::NotFound);
        }
        if scope.has_filter() && selected.is_empty() {
            return Err(ServiceError::NotFound);
        }
        let available = static_available
            .checked_add(selected.len())
            .and_then(|available| available.checked_add(published_available))
            .ok_or(ServiceError::ResourceExhausted)?;
        let mut items = Vec::new();
        let context_capacity = limits
            .maximum_result_items()
            .saturating_sub(published.len());
        if !continuation {
            let selected_reserve = if requested_dataset.is_some() {
                selected.len()
            } else {
                0
            };
            let static_capacity = context_capacity.saturating_sub(selected_reserve);
            for metadata in self
                .catalog
                .feature_catalog()
                .entries()
                .iter()
                .take(static_capacity)
            {
                items.push(feature_metadata_value(metadata));
            }
            for dataset in selected
                .iter()
                .take(context_capacity.saturating_sub(items.len()))
            {
                items.push(feature_dataset_value(dataset.dataset()));
            }
        }
        for dataset in &published {
            items.push(published_feature_dataset_value(dataset));
        }
        let metadata = combined_feature_metadata(&selected, &published, items.len(), available)?;
        let item_count = items.len().max(1);
        let next_after_dataset = published
            .last()
            .filter(|_| published_has_more)
            .map(|dataset| dataset.generation().manifest().dataset_id().as_str());
        TypedToolResult::try_new(
            json!({
                "items": items,
                "hasMore": published_has_more,
                "nextAfterDataset": next_after_dataset
            }),
            item_count,
            metadata,
            limits,
        )
        .map_err(|_| ServiceError::ResourceExhausted)
    }

    fn published_feature_datasets(
        &self,
        requested_dataset: Option<&DatasetId>,
        after_dataset: Option<&DatasetId>,
        scope: &ReadScope,
        limit: usize,
        context: &RequestContext,
    ) -> Result<(Vec<AnalyticalFeatureDataset>, usize, bool), ServiceError> {
        let Some(reader) = self.feature_reader.as_ref().filter(|_| !scope.has_filter()) else {
            if after_dataset.is_some() {
                return Err(ServiceError::Unavailable);
            }
            return Ok((Vec::new(), 0, false));
        };
        if let Some(dataset) = requested_dataset {
            let selected = reader
                .feature_dataset(dataset, context.deadline(), context.cancellation())
                .map_err(map_feature_read_error)?
                .into_iter()
                .collect::<Vec<_>>();
            let available = selected.len();
            return Ok((selected, available, false));
        }
        let limit = AnalyticalReadLimit::try_new(limit).map_err(map_feature_read_error)?;
        let page = reader
            .feature_datasets(
                after_dataset,
                limit,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_feature_read_error)?;
        Ok((page.datasets().to_vec(), page.available(), page.has_more()))
    }

    fn remove_durable_legacy_overlaps(
        &self,
        selected: &mut Vec<&FeatureDataset>,
        scope: &ReadScope,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let Some(reader) = self.feature_reader.as_ref().filter(|_| !scope.has_filter()) else {
            return Ok(());
        };
        let mut legacy_only = Vec::with_capacity(selected.len());
        for dataset in selected.drain(..) {
            let durable = reader
                .feature_dataset(
                    dataset.dataset().manifest().dataset_id(),
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_feature_read_error)?;
            if durable.is_none() {
                legacy_only.push(dataset);
            }
        }
        *selected = legacy_only;
        Ok(())
    }

    async fn get_backtest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let run_id = request
            .arguments()
            .get("runId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)?;
        let record = self
            .backtests
            .get(run_id, context.cancellation().clone(), context.deadline())
            .await?
            .ok_or(ServiceError::NotFound)?;
        not_applicable_result(record.content().clone(), request, context)
    }

    async fn run_backtest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let registration = request
            .arguments()
            .get("registration")
            .and_then(Value::as_object)
            .ok_or(ServiceError::InvalidRequest)?;
        let encoded = serde_json::to_vec(registration).map_err(|_| ServiceError::InvalidRequest)?;
        let input =
            GovernedBacktestInputRegistrationInput::try_from_json(&encoded).map_err(|error| {
                match error {
                    GovernedBacktestInputRegistrationJsonError::Invalid => {
                        ServiceError::InvalidRequest
                    }
                    GovernedBacktestInputRegistrationJsonError::ResourceExhausted => {
                        ServiceError::ResourceExhausted
                    }
                }
            })?;
        let registration = self
            .backtest_inputs
            .register_input(input, context.cancellation().clone(), context.deadline())
            .await?;
        let record = self
            .backtests
            .run(
                registration.into_command(),
                context.cancellation().clone(),
                context.deadline(),
            )
            .await?;
        not_applicable_result(record.content().clone(), request, context)
    }

    fn selected_dataset(
        &self,
        request: &TypedToolRequest,
    ) -> Result<&AnalysisDataset, ServiceError> {
        let dataset_id = request
            .arguments()
            .get("dataset")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)?;
        let dataset = self
            .catalog
            .dataset(dataset_id)
            .ok_or(ServiceError::NotFound)?;
        let scope = ReadScope::from_arguments(request.arguments())?;
        if !scope.matches(dataset.scope()) {
            return Err(ServiceError::NotFound);
        }
        Ok(dataset)
    }
}

impl fmt::Debug for AnalysisDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalysisDomainService")
            .field("catalog", &self.catalog)
            .field(
                "backtest_inputs",
                &"[GOVERNED INPUT REGISTRATION AUTHORITY]",
            )
            .field("backtests", &"[GOVERNED BACKTEST AUTHORITY]")
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

#[async_trait]
impl ApplicationDomainService for AnalysisDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Analysis
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.contract().domain() != ServiceDomain::Analysis {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = DomainLifecycle::enter(&self.lifecycle, &context)?;
        let result = match request.name() {
            GET_RETURNS => self.returns(&request, &context),
            GET_FACTORS => self.factors(&request, &context),
            GET_VALUATION => self.valuation(&request, &context),
            GET_SCENARIOS => self.scenarios(&request, &context),
            GET_FEATURE_DATASETS => self.feature_datasets(&request, &context),
            GET_BACKTESTS => self.get_backtest(&request, &context).await,
            RUN_BACKTEST => self.run_backtest(&request, &context).await,
            _ => Err(ServiceError::NotFound),
        }?;
        ensure_request_live(&context, &self.lifecycle)?;
        Ok(result)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
        self.backtests.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        self.lifecycle.finish_shutdown(deadline).await?;
        self.backtests.finish_shutdown(deadline).await
    }
}

impl Drop for AnalysisDomainService {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct ReadScope {
    instruments: Option<Vec<InstrumentId>>,
    time_range: Option<(Timestamp, Timestamp)>,
    sources: Option<Vec<SourceId>>,
}

impl ReadScope {
    fn from_arguments(arguments: &Map<String, Value>) -> Result<Self, ServiceError> {
        let instruments = arguments
            .get("instrumentIds")
            .map(|value| {
                value
                    .as_array()
                    .ok_or(ServiceError::InvalidRequest)?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or(ServiceError::InvalidRequest)
                            .and_then(|value| {
                                InstrumentId::from_str(value)
                                    .map_err(|_| ServiceError::InvalidRequest)
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .map(|mut values| {
                values.sort_unstable();
                values
            });
        let time_range = arguments
            .get("timeRange")
            .map(parse_time_range)
            .transpose()?;
        let sources = arguments
            .get("sourceCoverage")
            .map(|value| {
                value
                    .as_array()
                    .ok_or(ServiceError::InvalidRequest)?
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .ok_or(ServiceError::InvalidRequest)
                            .and_then(|value| {
                                SourceId::try_from(value).map_err(|_| ServiceError::InvalidRequest)
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .map(|mut values| {
                values.sort_unstable();
                values
            });
        Ok(Self {
            instruments,
            time_range,
            sources,
        })
    }

    fn matches(&self, scope: &AnalysisDatasetScope) -> bool {
        self.instruments
            .as_ref()
            .is_none_or(|values| values.as_slice() == scope.instruments())
            && self
                .time_range
                .is_none_or(|range| range == (scope.starts_at(), scope.ends_at()))
            && self
                .sources
                .as_ref()
                .is_none_or(|values| values.as_slice() == scope.sources())
    }

    const fn has_filter(&self) -> bool {
        self.instruments.is_some() || self.time_range.is_some() || self.sources.is_some()
    }
}

fn optional_feature_dataset(
    request: &TypedToolRequest,
    name: &str,
) -> Result<Option<DatasetId>, ServiceError> {
    request
        .arguments()
        .get(name)
        .map(|value| {
            value
                .as_str()
                .ok_or(ServiceError::InvalidRequest)
                .and_then(|value| {
                    DatasetId::try_from(value).map_err(|_| ServiceError::InvalidRequest)
                })
        })
        .transpose()
}

fn parse_time_range(value: &Value) -> Result<(Timestamp, Timestamp), ServiceError> {
    let object = value.as_object().ok_or(ServiceError::InvalidRequest)?;
    let parse = |name: &str| {
        object
            .get(name)
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidRequest)
            .and_then(|value| {
                DateTime::parse_from_rfc3339(value)
                    .map_err(|_| ServiceError::InvalidRequest)?
                    .timestamp_nanos_opt()
                    .map(Timestamp::from_unix_nanos)
                    .ok_or(ServiceError::InvalidRequest)
            })
    };
    let start = parse("start")?;
    let end = parse("end")?;
    if start >= end {
        return Err(ServiceError::InvalidRequest);
    }
    Ok((start, end))
}

fn source_result(
    content: Value,
    returned: usize,
    available: usize,
    scope: &AnalysisDatasetScope,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let metadata = dataset_metadata(scope, returned, available)?;
    TypedToolResult::try_new(
        content,
        returned.max(1),
        metadata,
        admitted_result_limits(request, context)?,
    )
    .map_err(|_| ServiceError::ResourceExhausted)
}

fn dataset_metadata(
    scope: &AnalysisDatasetScope,
    returned: usize,
    available: usize,
) -> Result<ToolResultMetadata, ServiceError> {
    let coverage = json!({
        "sources": scope.sources(),
        "instruments": scope.instruments(),
        "startsAtUnixNanos": scope.starts_at().unix_nanos(),
        "endsAtUnixNanos": scope.ends_at().unix_nanos(),
        "pointInTime": true
    });
    let quality = json!({
        "classes": scope.qualities(),
        "executionEligible": false
    });
    if returned < available {
        ToolResultMetadata::try_truncated(available, coverage, quality).map_err(Into::into)
    } else {
        ToolResultMetadata::try_complete(coverage, quality).map_err(Into::into)
    }
}

fn combined_feature_metadata(
    selected: &[&FeatureDataset],
    published: &[AnalyticalFeatureDataset],
    returned: usize,
    available: usize,
) -> Result<ToolResultMetadata, ServiceError> {
    let mut sources = Vec::new();
    let mut qualities = Vec::new();
    let mut source_set = HashSet::new();
    let mut quality_set = HashSet::new();
    for dataset in selected {
        for source in dataset.scope().sources() {
            if source_set.insert(source.clone()) {
                sources.push(source.clone());
            }
        }
        for quality in dataset.scope().qualities() {
            if quality_set.insert(*quality) {
                qualities.push(*quality);
            }
        }
    }
    for dataset in published {
        for source in dataset.source_ids() {
            if source_set.insert(source.clone()) {
                sources.push(source.clone());
            }
        }
    }
    sources.sort_unstable();
    let dataset_count = selected
        .len()
        .checked_add(published.len())
        .ok_or(ServiceError::ResourceExhausted)?;
    let coverage = json!({
        "sources": sources,
        "datasetCount": dataset_count,
        "pointInTime": true
    });
    let quality = json!({
        "classes": qualities,
        "executionEligible": false
    });
    if returned < available {
        ToolResultMetadata::try_truncated(available, coverage, quality).map_err(Into::into)
    } else {
        ToolResultMetadata::try_complete(coverage, quality).map_err(Into::into)
    }
}

fn map_feature_read_error(error: AnalyticalReadError) -> ServiceError {
    match error {
        AnalyticalReadError::InvalidLimit
        | AnalyticalReadError::InstrumentLimitExceeded
        | AnalyticalReadError::InvalidKnowledgeRange
        | AnalyticalReadError::InvalidObservationSchema => ServiceError::InvalidRequest,
        AnalyticalReadError::Manifest(ManifestCatalogError::Cancelled)
        | AnalyticalReadError::Query(QueryError::Cancelled) => ServiceError::Cancelled,
        AnalyticalReadError::Manifest(ManifestCatalogError::DeadlineExceeded)
        | AnalyticalReadError::Query(QueryError::DeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        AnalyticalReadError::Manifest(
            ManifestCatalogError::ObjectLimitExceeded { .. }
            | ManifestCatalogError::ReferenceWorkLimitExceeded { .. }
            | ManifestCatalogError::CountOverflow
            | ManifestCatalogError::AllocationContract,
        )
        | AnalyticalReadError::Query(
            QueryError::InvalidLimits
            | QueryError::RowLimitExceeded { .. }
            | QueryError::ByteLimitExceeded { .. }
            | QueryError::MemoryLimitExceeded { .. }
            | QueryError::SizeOverflow
            | QueryError::DependencyAllocationContract
            | QueryError::BlockingTaskLimitExceeded
            | QueryError::ReaderMemoryBoundExceeded
            | QueryError::ArtifactStoreRequired
            | QueryError::ArtifactAuthorityRequired,
        ) => ServiceError::ResourceExhausted,
        AnalyticalReadError::Manifest(_) | AnalyticalReadError::Query(_) => {
            ServiceError::Unavailable
        }
    }
}

fn not_applicable_result(
    content: Value,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        content,
        1,
        ToolResultMetadata::complete_not_applicable(),
        admitted_result_limits(request, context)?,
    )
    .map_err(|_| ServiceError::ResourceExhausted)
}

fn factor_result_value(
    selected: &AnalysisDataset,
    result: &FactorRegressionResult,
    exposures: Vec<Value>,
) -> Value {
    json!({
        "manifest": manifest_value(selected.pinned().manifest()),
        "intercept": statistical_result_value(result.intercept()),
        "exposures": exposures,
        "rSquared": statistical_result_value(result.r_squared())
    })
}

fn scenario_result_value(
    selected: &AnalysisDataset,
    result: &PortfolioAttribution,
    contributions: Vec<Value>,
) -> Value {
    json!({
        "manifest": manifest_value(selected.pinned().manifest()),
        "contributions": contributions,
        "total": monetary_value(result.total())
    })
}

fn statistical_result_value(result: StatisticalResult) -> Value {
    json!({
        "value": result.value(),
        "observations": result.observations()
    })
}

fn monetary_value(value: market_squawk_analytics::MonetaryValue) -> Value {
    json!({
        "amount": value.money().amount().to_string(),
        "currency": value.money().currency().to_string(),
        "basis": monetary_basis_name(value.basis())
    })
}

const fn monetary_basis_name(value: MonetaryBasis) -> &'static str {
    match value {
        MonetaryBasis::Total => "total",
        MonetaryBasis::PerShare => "per_share",
    }
}

const fn exact_decimal_unit_name(value: ExactDecimalUnit) -> &'static str {
    match value {
        ExactDecimalUnit::Ratio => "ratio",
        ExactDecimalUnit::Rate => "rate",
        ExactDecimalUnit::Standardized => "standardized",
    }
}

const fn rounding_policy_name(value: RoundingPolicy) -> &'static str {
    match value {
        RoundingPolicy::NearestEven => "nearest_even",
        RoundingPolicy::AwayFromZero => "away_from_zero",
        RoundingPolicy::TowardZero => "toward_zero",
        RoundingPolicy::Floor => "floor",
        RoundingPolicy::Ceiling => "ceiling",
    }
}
