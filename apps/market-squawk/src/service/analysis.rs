//! Bounded cross-domain discovery over installed-product read authorities.

use std::{collections::BTreeSet, sync::Arc};

use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadLimit, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentRecord,
};
use market_squawk_decisions::{SavedScreen, ScreenId};
use market_squawk_domain::{AssetClass, MarketDataInstrumentDefinition};
use market_squawk_jobs::{JobListPageLimit, SqliteJobRepository};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, TOOL_RESULT_LIMITS_FIELD,
    ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    LocalProduct, ResearchService, ResearchServiceError,
    application::{
        PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT, PRODUCT_LOOKUP_ACTION_OPEN_SAVED_SCREEN,
        PRODUCT_LOOKUP_CATEGORIES, PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
        PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN,
        decision::{DecisionApplication, DecisionApplicationError},
        job::{JobApplication, JobApplicationError},
        map_market_definition_read_error,
        market_selection::product::individual_selection_token,
        product_lookup_query_is_canonical,
    },
    jobs::InstalledJobAuthority,
    provider_onboarding::ProviderOnboardingService,
};

const LOOKUP: &str = "Analysis.Lookup";
const OVERVIEW: &str = "Analysis.GetDecisionOverview";
const MAXIMUM_LOOKUP_ITEMS: usize = 64;

/// Closed cross-domain analysis surface shared by installed transports.
pub(super) struct InstalledAnalysisOperations {
    capabilities: ServiceCapabilities,
    providers: Arc<ProviderOnboardingService>,
    analytical: AnalyticalReadCapability,
    research: Arc<ResearchService>,
    decisions: Arc<DecisionApplication>,
    jobs: JobApplication<SqliteJobRepository>,
}

impl InstalledAnalysisOperations {
    pub(super) fn new(product: &LocalProduct, jobs: &InstalledJobAuthority) -> Self {
        Self {
            capabilities: product.application().capabilities(),
            providers: product.provider_onboarding(),
            analytical: product.research().analytical_reader(),
            research: product.research(),
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
            count,
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
        let query = request.query.as_str();
        if !product_lookup_query_is_canonical(query) {
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
        let mut status = Vec::new();
        let mut category_matches = Vec::new();
        let normalized_query = query.to_lowercase();

        for category in categories {
            ensure_live(context)?;
            match category.as_str() {
                PRODUCT_LOOKUP_CATEGORY_INVESTMENT => {
                    let (matches, complete) =
                        self.investment_matches(query, maximum, context).await?;
                    category_matches.push(matches);
                    status.push(if complete {
                        available(PRODUCT_LOOKUP_CATEGORY_INVESTMENT)
                    } else {
                        json!({
                            "category": PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
                            "state": "unavailable",
                            "message": "Some matching investments are not available to open right now."
                        })
                    });
                }
                PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN => {
                    let mut matches = Vec::new();
                    let mut after = None::<ScreenId>;
                    let has_more = loop {
                        ensure_live(context)?;
                        let page = self
                            .decisions
                            .list_current_screens_after(after.as_ref(), maximum)
                            .map_err(map_decision)?;
                        for screen in page.screens() {
                            let screen_id = screen.revision().id().as_str();
                            if !screen_id.to_lowercase().contains(&normalized_query) {
                                continue;
                            }
                            matches.push(saved_screen_product_value(screen));
                            if matches.len() > maximum {
                                break;
                            }
                        }
                        if matches.len() > maximum {
                            break true;
                        }
                        if !page.has_more() {
                            break false;
                        }
                        after = page
                            .screens()
                            .last()
                            .map(|screen| screen.revision().id().clone());
                        if after.is_none() {
                            return Err(ServiceError::Internal);
                        }
                    };
                    matches.truncate(maximum);
                    category_matches.push(CategoryMatches { matches, has_more });
                    status.push(available(PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN));
                }
                unavailable => status.push(json!({
                    "category": unavailable,
                    "state": "unavailable",
                    "message": "Search is unavailable for this area right now."
                })),
            }
        }
        let available_matches = category_matches
            .iter()
            .try_fold(0_usize, |count, category| {
                count.checked_add(category.matches.len())
            })
            .ok_or(ServiceError::Internal)?;
        let truncated = available_matches > maximum
            || category_matches.iter().any(|category| category.has_more);
        let matches = merge_category_matches(category_matches, maximum);
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

    async fn investment_matches(
        &self,
        query: &str,
        maximum: usize,
        context: &RequestContext,
    ) -> Result<(CategoryMatches, bool), ServiceError> {
        let query = query.to_owned();
        let markets = self.research.market_data_instruments();
        let at = super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let deadline = context.deadline();
        self.research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                let page = markets
                    .search_as_of(&query, at, at, maximum, deadline, &cancellation)
                    .map_err(map_market_definition_read_error)?;
                let mut matches = Vec::with_capacity(page.matches().len());
                if page.matches().is_empty() {
                    return Ok((
                        CategoryMatches {
                            matches,
                            has_more: page.has_more(),
                        },
                        true,
                    ));
                }
                let population = MarketDataInstrumentPopulationQuery::try_new(
                    page.matches()
                        .iter()
                        .map(|item| item.record().definition().instrument_id())
                        .collect(),
                    at,
                    at,
                )
                .map_err(map_market_definition_read_error)?;
                let selected = markets
                    .pin_population_as_of(population, deadline, &cancellation)
                    .map_err(map_market_definition_read_error)?;
                // Preserve search relevance, but only navigate using the selected canonical revision.
                for item in page.matches() {
                    if let Ok(index) = selected.records().binary_search_by_key(
                        &item.record().definition().instrument_id(),
                        |record| record.definition().instrument_id(),
                    ) {
                        matches.push(instrument_lookup_match(&selected.records()[index])?);
                    }
                }
                Ok((
                    CategoryMatches {
                        matches,
                        has_more: page.has_more(),
                    },
                    selected.disposition() == MarketDataInstrumentPopulationDisposition::Complete,
                ))
            })
            .await
            .map_err(|error| match error {
                ResearchServiceError::Ingest(market_squawk_data::IngestError::Cancelled) => {
                    ServiceError::Cancelled
                }
                ResearchServiceError::Ingest(market_squawk_data::IngestError::DeadlineExceeded) => {
                    ServiceError::DeadlineExceeded
                }
                _ => ServiceError::Internal,
            })?
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
            .field("research", &"[RESEARCH READ AUTHORITY]")
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
        PRODUCT_LOOKUP_CATEGORIES
            .iter()
            .map(ToString::to_string)
            .collect()
    } else {
        categories
    };
    if categories.len() > PRODUCT_LOOKUP_CATEGORIES.len()
        || categories
            .iter()
            .any(|category| !PRODUCT_LOOKUP_CATEGORIES.contains(&category.as_str()))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let count = categories.len();
    let categories = categories.into_iter().collect::<BTreeSet<_>>();
    if categories.len() != count {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(categories)
}

fn available(category: &str) -> Value {
    json!({"category": category, "state": "available"})
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    let mut business_arguments = arguments.clone();
    business_arguments.remove(TOOL_RESULT_LIMITS_FIELD);
    serde_json::from_value(Value::Object(business_arguments))
        .map_err(|_error| ServiceError::InvalidRequest)
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
        JobApplicationError::Contract => ServiceError::InvalidRequest,
        JobApplicationError::Repository | JobApplicationError::Authority => {
            ServiceError::Unavailable
        }
    }
}

fn map_decision(_error: DecisionApplicationError) -> ServiceError {
    ServiceError::Unavailable
}

fn instrument_lookup_match(record: &MarketDataInstrumentRecord) -> Result<Value, ServiceError> {
    let definition = record.definition();
    let selection_token = individual_selection_token(record)?;
    Ok(json!({
        "category": PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
        "title": instrument_title(definition),
        "subtitle": format!(
            "{} · {}",
            asset_class_label(definition.asset_class()),
            definition.quote_currency(),
        ),
        "destination": {
            "action": PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT,
            "instrumentId": definition.instrument_id().to_string(),
            "selectionToken": selection_token
        }
    }))
}

struct CategoryMatches {
    matches: Vec<Value>,
    has_more: bool,
}

fn merge_category_matches(categories: Vec<CategoryMatches>, maximum: usize) -> Vec<Value> {
    let mut iterators = categories
        .into_iter()
        .map(|category| category.matches.into_iter())
        .collect::<Vec<_>>();
    let mut matches = Vec::with_capacity(maximum);
    while matches.len() < maximum {
        let mut added = false;
        for iterator in &mut iterators {
            if matches.len() >= maximum {
                break;
            }
            if let Some(value) = iterator.next() {
                matches.push(value);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    matches
}

fn instrument_title(definition: &MarketDataInstrumentDefinition) -> String {
    definition
        .venue_mappings()
        .iter()
        .min_by_key(|mapping| (mapping.venue_symbol().as_str(), mapping.venue_id().as_str()))
        .map(|mapping| mapping.venue_symbol().as_str().to_owned())
        .unwrap_or_else(|| "Investment".to_owned())
}

const fn asset_class_label(asset_class: AssetClass) -> &'static str {
    match asset_class {
        AssetClass::Equity => "Stock",
        AssetClass::FixedIncome => "Bond",
        AssetClass::Option => "Option",
        AssetClass::Future => "Futures contract",
        AssetClass::ForeignExchange => "Currency pair",
        AssetClass::Crypto => "Crypto asset",
        AssetClass::Commodity => "Commodity",
        AssetClass::Fund => "Fund",
        AssetClass::Index => "Market index",
        AssetClass::Cash => "Cash",
    }
}

fn product_title(value: &str, fallback: &str) -> String {
    let display = value
        .strip_prefix("screen.")
        .or_else(|| value.strip_prefix("screen-"))
        .or_else(|| value.strip_prefix("screen_"))
        .unwrap_or(value);
    let title = display
        .split(['-', '_', '.'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_uppercase().collect::<String>() + characters.as_str()
            })
        })
        .collect::<Vec<_>>()
        .join(" ");
    if title.is_empty() {
        fallback.to_owned()
    } else {
        title
    }
}

pub(super) fn saved_screen_product_value(screen: &SavedScreen) -> Value {
    let screen_id = screen.revision().id().as_str();
    json!({
        "category": PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN,
        "title": product_title(screen_id, "Saved screen"),
        "subtitle": "Saved investment screen",
        "destination": {
            "action": PRODUCT_LOOKUP_ACTION_OPEN_SAVED_SCREEN,
            "screenId": screen_id
        }
    })
}
