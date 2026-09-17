//! Bounded cross-domain discovery over installed-product read authorities.

use std::{collections::BTreeSet, sync::Arc};

use market_squawk_data::{
    AnalyticalReadCapability, AnalyticalReadLimit, CatalogError,
    InstrumentDefinitionReadCapability, InstrumentSearchMatch,
};
use market_squawk_decisions::{SavedScreen, ScreenId};
use market_squawk_domain::{AssetClass, InstrumentDefinition, TradingStatus};
use market_squawk_jobs::{JobListPageLimit, SqliteJobRepository};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, TOOL_RESULT_LIMITS_FIELD,
    ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    LocalProduct,
    application::{
        PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT, PRODUCT_LOOKUP_ACTION_OPEN_SAVED_SCREEN,
        PRODUCT_LOOKUP_CATEGORIES, PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
        PRODUCT_LOOKUP_CATEGORY_SAVED_SCREEN,
        decision::{DecisionApplication, DecisionApplicationError},
        job::{JobApplication, JobApplicationError},
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
    instrument_definitions: InstrumentDefinitionReadCapability,
    decisions: Arc<DecisionApplication>,
    jobs: JobApplication<SqliteJobRepository>,
}

impl InstalledAnalysisOperations {
    pub(super) fn new(product: &LocalProduct, jobs: &InstalledJobAuthority) -> Self {
        Self {
            capabilities: product.application().capabilities(),
            providers: product.provider_onboarding(),
            analytical: product.research().analytical_reader(),
            instrument_definitions: product.research().instrument_definitions(),
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
                    let page = self
                        .instrument_definitions
                        .search(query, maximum, context.deadline(), context.cancellation())
                        .map_err(map_instrument_search)?;
                    category_matches.push(CategoryMatches {
                        matches: page.matches().iter().map(instrument_lookup_match).collect(),
                        has_more: page.has_more(),
                    });
                    status.push(available(PRODUCT_LOOKUP_CATEGORY_INVESTMENT));
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
            .field("instrument_definitions", &self.instrument_definitions)
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

fn map_instrument_search(error: CatalogError) -> ServiceError {
    match error {
        CatalogError::InstrumentDefinitionReadCancelled => ServiceError::Cancelled,
        CatalogError::InstrumentDefinitionReadDeadlineExceeded => ServiceError::DeadlineExceeded,
        CatalogError::InvalidLimit | CatalogError::InvalidRecord => ServiceError::InvalidRequest,
        _ => ServiceError::Unavailable,
    }
}

fn instrument_lookup_match(search_match: &InstrumentSearchMatch) -> Value {
    let definition = search_match.definition();
    json!({
        "category": PRODUCT_LOOKUP_CATEGORY_INVESTMENT,
        "title": instrument_title(definition),
        "subtitle": format!(
            "{} · {} · {}",
            asset_class_label(definition.asset_class()),
            definition.quote_currency(),
            trading_status_label(definition.trading_status()),
        ),
        "destination": {
            "action": PRODUCT_LOOKUP_ACTION_OPEN_INVESTMENT,
            "instrumentId": definition.instrument_id().to_string()
        }
    })
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

fn instrument_title(definition: &InstrumentDefinition) -> String {
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

const fn trading_status_label(status: TradingStatus) -> &'static str {
    match status {
        TradingStatus::Active => "Active",
        TradingStatus::Halted => "Temporarily halted",
        TradingStatus::Inactive => "Inactive",
        TradingStatus::Delisted => "Delisted",
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
