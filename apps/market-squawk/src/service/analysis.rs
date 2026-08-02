//! Bounded cross-domain discovery over installed-product read authorities.

use std::{collections::BTreeSet, sync::Arc};

use market_squawk_data::{AnalyticalReadCapability, AnalyticalReadLimit};
use market_squawk_jobs::{JobListPageLimit, SqliteJobRepository};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    LocalProduct,
    application::{
        decision::{DecisionApplication, DecisionApplicationError},
        job::{JobApplication, JobApplicationError},
    },
    jobs::InstalledJobAuthority,
    provider_onboarding::ProviderOnboardingService,
};

const LOOKUP: &str = "Analysis.Lookup";
const OVERVIEW: &str = "Analysis.GetDecisionOverview";
const MAXIMUM_LOOKUP_ITEMS: usize = 64;
const MAXIMUM_QUERY_BYTES: usize = 256;
const ALL_CATEGORIES: [&str; 9] = [
    "command",
    "dataset",
    "instrument",
    "job",
    "model",
    "portfolio",
    "provider",
    "screen",
    "target",
];

/// Closed cross-domain analysis surface shared by installed transports.
pub(super) struct InstalledAnalysisOperations {
    capabilities: ServiceCapabilities,
    providers: Arc<ProviderOnboardingService>,
    analytical: AnalyticalReadCapability,
    decisions: Arc<DecisionApplication>,
    jobs: JobApplication<SqliteJobRepository>,
}

impl InstalledAnalysisOperations {
    pub(super) fn new(product: &LocalProduct, jobs: &InstalledJobAuthority) -> Self {
        Self {
            capabilities: product.application().capabilities(),
            providers: product.provider_onboarding(),
            analytical: product.research().analytical_reader(),
            decisions: product.decisions(),
            jobs: JobApplication::new(jobs.repository(), jobs.authority()),
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, LOOKUP | OVERVIEW)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let (content, count) = match request.name() {
            LOOKUP => self.lookup(request.arguments(), context).await?,
            OVERVIEW => self.overview(context).await?,
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            count.max(1),
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    async fn lookup(
        &self,
        arguments: &Map<String, Value>,
        context: &RequestContext,
    ) -> Result<(Value, usize), ServiceError> {
        let request: LookupRequest = decode(arguments)?;
        let query = request.query.trim().to_ascii_lowercase();
        if query.is_empty() || query.len() > MAXIMUM_QUERY_BYTES {
            return Err(ServiceError::InvalidRequest);
        }
        let categories = requested_categories(request.categories)?;
        let maximum = context
            .limits()
            .maximum_result_items()
            .min(MAXIMUM_LOOKUP_ITEMS);
        if maximum == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        let mut matches = Vec::new();
        let mut status = Vec::new();

        for category in categories {
            ensure_live(context)?;
            match category.as_str() {
                "command" => {
                    for descriptor in self.capabilities.tools() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let haystack = format!(
                            "{} {} {:?}",
                            descriptor.name(),
                            descriptor.description(),
                            descriptor.contract().domain()
                        )
                        .to_ascii_lowercase();
                        if haystack.contains(&query) {
                            matches.push(json!({
                                "category": "command",
                                "id": descriptor.name(),
                                "label": descriptor.description(),
                                "detail": {"domain": format!("{:?}", descriptor.contract().domain())}
                            }));
                        }
                    }
                    status.push(available("command"));
                }
                "provider" => {
                    for profile in self.providers.profiles() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let value = encode(&profile)?;
                        if value.to_string().to_ascii_lowercase().contains(&query) {
                            matches.push(json!({
                                "category": "provider",
                                "id": profile.id(),
                                "label": profile.id(),
                                "detail": value
                            }));
                        }
                    }
                    status.push(available("provider"));
                }
                "dataset" => {
                    let page = self.dataset_page(context)?;
                    for generation in page.generations() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let id = generation.manifest().dataset_id().as_str();
                        let source = generation.source_id().to_string();
                        if id.to_ascii_lowercase().contains(&query)
                            || source.to_ascii_lowercase().contains(&query)
                        {
                            matches.push(json!({
                                "category": "dataset",
                                "id": id,
                                "label": id,
                                "detail": {
                                    "manifestVersion": generation.manifest().manifest_version(),
                                    "sourceId": source,
                                    "rowCount": generation.row_count(),
                                    "totalBytes": generation.total_bytes()
                                }
                            }));
                        }
                    }
                    status.push(available("dataset"));
                }
                "screen" => {
                    for screen in self
                        .decisions
                        .list_screens(MAXIMUM_LOOKUP_ITEMS)
                        .map_err(map_decision)?
                    {
                        if matches.len() >= maximum {
                            break;
                        }
                        let id = screen.revision().id().as_str();
                        if id.to_ascii_lowercase().contains(&query) {
                            matches.push(json!({
                                "category": "screen",
                                "id": id,
                                "label": id,
                                "detail": {
                                    "revision": screen.revision().revision().get(),
                                    "maximumResults": screen.maximum_results().get()
                                }
                            }));
                        }
                    }
                    status.push(available("screen"));
                }
                "job" => {
                    let page = self.job_page().await?;
                    for job in page.jobs() {
                        if matches.len() >= maximum {
                            break;
                        }
                        let value = encode(job)?;
                        if value.to_string().to_ascii_lowercase().contains(&query) {
                            let id = value
                                .get("jobId")
                                .and_then(Value::as_str)
                                .ok_or(ServiceError::Internal)?;
                            matches.push(json!({
                                "category": "job",
                                "id": id,
                                "label": value.get("kind").cloned().unwrap_or(Value::String(id.to_owned())),
                                "detail": value
                            }));
                        }
                    }
                    status.push(available("job"));
                }
                unavailable => status.push(json!({
                    "category": unavailable,
                    "state": "unavailable",
                    "reason": "no bounded installed-product index is available for this category"
                })),
            }
        }
        let truncated = matches.len() == maximum;
        let count = matches.len();
        Ok((
            json!({
                "query": query,
                "matches": matches,
                "categories": status,
                "truncated": truncated
            }),
            count,
        ))
    }

    async fn overview(&self, context: &RequestContext) -> Result<(Value, usize), ServiceError> {
        let datasets = self.dataset_page(context)?;
        let screens = self
            .decisions
            .list_screens(MAXIMUM_LOOKUP_ITEMS)
            .map_err(map_decision)?;
        let jobs = self.job_page().await?;
        let providers = self.providers.profiles();
        Ok((
            json!({
                "providers": {
                    "state": "available",
                    "count": providers.len(),
                    "items": providers
                },
                "datasets": {
                    "state": "available",
                    "count": datasets.generations().len(),
                    "hasMore": datasets.has_more()
                },
                "screens": {
                    "state": "available",
                    "count": screens.len(),
                    "items": screens.iter().map(|screen| json!({
                        "id": screen.revision().id().as_str(),
                        "revision": screen.revision().revision().get(),
                        "maximumResults": screen.maximum_results().get()
                    })).collect::<Vec<_>>()
                },
                "jobs": {
                    "state": "available",
                    "count": jobs.jobs().len(),
                    "items": jobs.jobs()
                },
                "commands": {
                    "state": "available",
                    "count": self.capabilities.tools().len()
                },
                "unavailable": [
                    {"category": "instrument", "reason": "no bounded all-instrument index is available"},
                    {"category": "model", "reason": "model bundles remain available through Model.ListBundles"},
                    {"category": "portfolio", "reason": "accounts remain available through Portfolio.ListAccounts"},
                    {"category": "target", "reason": "targets require a known target-series identity"}
                ]
            }),
            1,
        ))
    }

    fn dataset_page(
        &self,
        context: &RequestContext,
    ) -> Result<market_squawk_data::AnalyticalGenerationPage, ServiceError> {
        let limit = AnalyticalReadLimit::try_new(MAXIMUM_LOOKUP_ITEMS)
            .map_err(|_error| ServiceError::Internal)?;
        self.analytical
            .datasets(None, limit, context.deadline(), context.cancellation())
            .map_err(|_error| ServiceError::Unavailable)
    }

    async fn job_page(&self) -> Result<crate::application::job::JobViewPage, ServiceError> {
        let limit = JobListPageLimit::try_new(MAXIMUM_LOOKUP_ITEMS)
            .map_err(|_error| ServiceError::Internal)?;
        self.jobs.list(None, limit).await.map_err(map_job)
    }
}

impl std::fmt::Debug for InstalledAnalysisOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledAnalysisOperations")
            .field("capabilities", &self.capabilities)
            .field("providers", &"[PROVIDER AUTHORITY]")
            .field("analytical", &self.analytical)
            .field("decisions", &"[DECISION AUTHORITY]")
            .field("jobs", &"[JOB AUTHORITY]")
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct LookupRequest {
    query: String,
    #[serde(default)]
    categories: Vec<String>,
}

fn requested_categories(categories: Vec<String>) -> Result<BTreeSet<String>, ServiceError> {
    let categories = if categories.is_empty() {
        ALL_CATEGORIES.iter().map(ToString::to_string).collect()
    } else {
        categories
    };
    if categories.len() > ALL_CATEGORIES.len()
        || categories
            .iter()
            .any(|category| !ALL_CATEGORIES.contains(&category.as_str()))
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(categories.into_iter().collect())
}

fn available(category: &str) -> Value {
    json!({"category": category, "state": "available"})
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode(value: impl serde::Serialize) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_job(error: JobApplicationError) -> ServiceError {
    match error {
        JobApplicationError::NotFound => ServiceError::NotFound,
        JobApplicationError::WaitCancelled => ServiceError::Cancelled,
        JobApplicationError::WaitDeadlineExceeded => ServiceError::DeadlineExceeded,
        JobApplicationError::Contract => ServiceError::InvalidRequest,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}

fn map_decision(_error: DecisionApplicationError) -> ServiceError {
    ServiceError::Unavailable
}
