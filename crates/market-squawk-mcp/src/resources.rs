//! Closed, provider-neutral MCP resource catalog for ordinary product reads.

use std::sync::Arc;

use market_squawk_services::{
    ResultEnvelopeProjection, ServiceCapabilities, ToolAuthorization, ToolDescriptor,
};
use rmcp::model::{Resource, ResourceTemplate};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

const MARKET_OVERVIEW_URI: &str = "market-squawk://market-overview";
const ECONOMIC_CONTEXT_URI: &str = "market-squawk://economic-context";
const FORECASTS_URI: &str = "market-squawk://forecasts";
const BACKTESTS_URI: &str = "market-squawk://backtests";
const INVESTMENT_ANALYSES_URI: &str = "market-squawk://investment-analyses";

const FORECAST_TEMPLATE: &str = "market-squawk://forecasts/{forecast_token}";
const FORECAST_OUTCOMES_TEMPLATE: &str = "market-squawk://forecasts/{forecast_token}/outcomes";
const BACKTEST_TEMPLATE: &str = "market-squawk://backtests/{backtest_token}";
const INVESTMENT_ANALYSIS_TEMPLATE: &str = "market-squawk://investment-analyses/{action_token}";
const RECOMMENDATION_TRACK_RECORD_TEMPLATE: &str =
    "market-squawk://investment-analyses/{action_token}/track-record";

const MARKET_GET_OVERVIEW: &str = "Market.GetOverview";
const MACRO_GET_CONTEXT: &str = "Macro.GetContext";
const MODEL_LIST_FORECASTS: &str = "Model.ListForecasts";
const MODEL_GET_FORECAST: &str = "Model.GetForecast";
const MODEL_GET_FORECAST_OUTCOMES: &str = "Model.GetForecastOutcomes";
const ANALYSIS_LIST_PRODUCT_BACKTESTS: &str = "Analysis.ListProductBacktests";
const ANALYSIS_GET_PRODUCT_BACKTEST: &str = "Analysis.GetProductBacktest";
const DECISION_LIST_INVESTMENT_ANALYSES: &str = "Decision.ListInvestmentAnalyses";
const DECISION_GET_INVESTMENT_ANALYSIS: &str = "Decision.GetInvestmentAnalysis";
const DECISION_GET_RECOMMENDATION_TRACK_RECORD: &str = "Decision.GetRecommendationTrackRecord";

const INVESTMENT_ANALYSIS_LIST_LIMIT: u64 = 100;
const RESOURCE_MAXIMUM_ITEMS: u64 = 1_000;
const RESOURCE_MAXIMUM_BYTES: u64 = 64 * 1024;

/// One admitted resource in the closed ordinary-product namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProductResource {
    MarketOverview,
    EconomicContext,
    Forecasts,
    Forecast(Uuid),
    ForecastOutcomes(Uuid),
    Backtests,
    Backtest(Uuid),
    InvestmentAnalyses,
    InvestmentAnalysis(Uuid),
    RecommendationTrackRecord(Uuid),
}

impl ProductResource {
    /// Parses only exact code-owned V1 URIs.
    pub(crate) fn try_from_uri(uri: &str) -> Result<Self, ProductResourceError> {
        match uri {
            MARKET_OVERVIEW_URI => Ok(Self::MarketOverview),
            ECONOMIC_CONTEXT_URI => Ok(Self::EconomicContext),
            FORECASTS_URI => Ok(Self::Forecasts),
            BACKTESTS_URI => Ok(Self::Backtests),
            INVESTMENT_ANALYSES_URI => Ok(Self::InvestmentAnalyses),
            _ => parse_parameterized(uri),
        }
    }

    pub(crate) const fn operation(self) -> &'static str {
        match self {
            Self::MarketOverview => MARKET_GET_OVERVIEW,
            Self::EconomicContext => MACRO_GET_CONTEXT,
            Self::Forecasts => MODEL_LIST_FORECASTS,
            Self::Forecast(_) => MODEL_GET_FORECAST,
            Self::ForecastOutcomes(_) => MODEL_GET_FORECAST_OUTCOMES,
            Self::Backtests => ANALYSIS_LIST_PRODUCT_BACKTESTS,
            Self::Backtest(_) => ANALYSIS_GET_PRODUCT_BACKTEST,
            Self::InvestmentAnalyses => DECISION_LIST_INVESTMENT_ANALYSES,
            Self::InvestmentAnalysis(_) => DECISION_GET_INVESTMENT_ANALYSIS,
            Self::RecommendationTrackRecord(_) => DECISION_GET_RECOMMENDATION_TRACK_RECORD,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::MarketOverview => "market-overview",
            Self::EconomicContext => "economic-context",
            Self::Forecasts => "forecasts",
            Self::Forecast(_) => "forecast",
            Self::ForecastOutcomes(_) => "forecast-outcomes",
            Self::Backtests => "backtests",
            Self::Backtest(_) => "backtest",
            Self::InvestmentAnalyses => "investment-analyses",
            Self::InvestmentAnalysis(_) => "investment-analysis",
            Self::RecommendationTrackRecord(_) => "recommendation-track-record",
        }
    }

    pub(crate) fn arguments(self) -> Map<String, Value> {
        let mut arguments = Map::new();
        if !matches!(self, Self::MarketOverview) {
            arguments.insert(
                "resultLimits".to_owned(),
                serde_json::json!({
                    "maximumItems": RESOURCE_MAXIMUM_ITEMS,
                    "maximumBytes": RESOURCE_MAXIMUM_BYTES,
                }),
            );
        }
        match self {
            Self::Forecast(token) | Self::ForecastOutcomes(token) => {
                arguments.insert("forecastToken".to_owned(), Value::String(token.to_string()));
            }
            Self::Backtest(token) => {
                arguments.insert("backtestToken".to_owned(), Value::String(token.to_string()));
            }
            Self::InvestmentAnalyses => {
                arguments.insert(
                    "limit".to_owned(),
                    Value::from(INVESTMENT_ANALYSIS_LIST_LIMIT),
                );
            }
            Self::InvestmentAnalysis(token) | Self::RecommendationTrackRecord(token) => {
                arguments.insert("actionToken".to_owned(), Value::String(token.to_string()));
            }
            Self::MarketOverview | Self::EconomicContext | Self::Forecasts | Self::Backtests => {}
        }
        arguments
    }

    pub(crate) fn uri(self) -> String {
        match self {
            Self::MarketOverview => MARKET_OVERVIEW_URI.to_owned(),
            Self::EconomicContext => ECONOMIC_CONTEXT_URI.to_owned(),
            Self::Forecasts => FORECASTS_URI.to_owned(),
            Self::Forecast(token) => format!("{FORECASTS_URI}/{token}"),
            Self::ForecastOutcomes(token) => format!("{FORECASTS_URI}/{token}/outcomes"),
            Self::Backtests => BACKTESTS_URI.to_owned(),
            Self::Backtest(token) => format!("{BACKTESTS_URI}/{token}"),
            Self::InvestmentAnalyses => INVESTMENT_ANALYSES_URI.to_owned(),
            Self::InvestmentAnalysis(token) => format!("{INVESTMENT_ANALYSES_URI}/{token}"),
            Self::RecommendationTrackRecord(token) => {
                format!("{INVESTMENT_ANALYSES_URI}/{token}/track-record")
            }
        }
    }
}

/// Invalid or unsupported product-resource URI or composition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ProductResourceError {
    #[error("resource URI is invalid")]
    InvalidUri,
    #[error("resource catalog is not supported by the product service")]
    InvalidComposition,
}

pub(crate) fn stable_resources() -> Arc<[Resource]> {
    [
        resource(
            MARKET_OVERVIEW_URI,
            "market-overview",
            "Market overview",
            "Current market conditions and key investing context.",
        ),
        resource(
            ECONOMIC_CONTEXT_URI,
            "economic-context",
            "Economic context",
            "Current economic conditions and interest-rate context.",
        ),
        resource(
            FORECASTS_URI,
            "forecasts",
            "Investment forecasts",
            "Saved forward-looking investment ranges and horizons.",
        ),
        resource(
            BACKTESTS_URI,
            "backtests",
            "Investment test results",
            "Saved historical investment tests with costs and uncertainty.",
        ),
        resource(
            INVESTMENT_ANALYSES_URI,
            "investment-analyses",
            "Investment analyses",
            "Saved investment decisions, reasons, risks, ranges, and uncertainty.",
        ),
    ]
    .into()
}

pub(crate) fn stable_resource_templates() -> Arc<[ResourceTemplate]> {
    [
        template(
            FORECAST_TEMPLATE,
            "forecast",
            "Investment forecast",
            "One saved investment forecast.",
        ),
        template(
            FORECAST_OUTCOMES_TEMPLATE,
            "forecast-outcomes",
            "Forecast outcomes",
            "Observed outcomes for one completed forecast horizon.",
        ),
        template(
            BACKTEST_TEMPLATE,
            "backtest",
            "Investment test",
            "One saved investment test with costs, drawdown, and uncertainty.",
        ),
        template(
            INVESTMENT_ANALYSIS_TEMPLATE,
            "investment-analysis",
            "Investment analysis",
            "One saved investment decision with its supporting evidence.",
        ),
        template(
            RECOMMENDATION_TRACK_RECORD_TEMPLATE,
            "recommendation-track-record",
            "Recommendation track record",
            "Comparable realized history for one saved investment decision.",
        ),
    ]
    .into()
}

/// Proves that every resource maps to an admitted read-only Product V1 descriptor.
pub(crate) fn validate_catalog(
    capabilities: &ServiceCapabilities,
) -> Result<(), ProductResourceError> {
    if capabilities
        .tools()
        .iter()
        .any(|descriptor| descriptor.result_projection() != ResultEnvelopeProjection::ProductV1)
    {
        return Err(ProductResourceError::InvalidComposition);
    }
    let sample = Uuid::from_u128(1);
    for resource in [
        ProductResource::MarketOverview,
        ProductResource::EconomicContext,
        ProductResource::Forecasts,
        ProductResource::Forecast(sample),
        ProductResource::ForecastOutcomes(sample),
        ProductResource::Backtests,
        ProductResource::Backtest(sample),
        ProductResource::InvestmentAnalyses,
        ProductResource::InvestmentAnalysis(sample),
        ProductResource::RecommendationTrackRecord(sample),
    ] {
        let descriptor = capabilities
            .find(resource.operation())
            .ok_or(ProductResourceError::InvalidComposition)?;
        validate_descriptor(descriptor)?;
        descriptor
            .admit(resource.arguments())
            .map_err(|_| ProductResourceError::InvalidComposition)?;
    }
    Ok(())
}

pub(crate) fn validate_descriptor(descriptor: &ToolDescriptor) -> Result<(), ProductResourceError> {
    if !matches!(
        descriptor.contract().authorization(),
        ToolAuthorization::ReadOnly
    ) || descriptor.result_projection() != ResultEnvelopeProjection::ProductV1
    {
        return Err(ProductResourceError::InvalidComposition);
    }
    Ok(())
}

fn parse_parameterized(uri: &str) -> Result<ProductResource, ProductResourceError> {
    if let Some(value) = uri
        .strip_prefix("market-squawk://forecasts/")
        .and_then(|value| value.strip_suffix("/outcomes"))
    {
        return parse_token(value).map(ProductResource::ForecastOutcomes);
    }
    if let Some(value) = uri.strip_prefix("market-squawk://forecasts/") {
        return parse_token(value).map(ProductResource::Forecast);
    }
    if let Some(value) = uri.strip_prefix("market-squawk://backtests/") {
        return parse_token(value).map(ProductResource::Backtest);
    }
    if let Some(value) = uri
        .strip_prefix("market-squawk://investment-analyses/")
        .and_then(|value| value.strip_suffix("/track-record"))
    {
        return parse_token(value).map(ProductResource::RecommendationTrackRecord);
    }
    if let Some(value) = uri.strip_prefix("market-squawk://investment-analyses/") {
        return parse_token(value).map(ProductResource::InvestmentAnalysis);
    }
    Err(ProductResourceError::InvalidUri)
}

fn parse_token(value: &str) -> Result<Uuid, ProductResourceError> {
    let token = Uuid::parse_str(value).map_err(|_| ProductResourceError::InvalidUri)?;
    if token.is_nil() || token.to_string() != value {
        return Err(ProductResourceError::InvalidUri);
    }
    Ok(token)
}

fn resource(uri: &str, name: &str, title: &str, description: &str) -> Resource {
    Resource::new(uri, name)
        .with_title(title)
        .with_description(description)
        .with_mime_type("application/json")
}

fn template(uri: &str, name: &str, title: &str, description: &str) -> ResourceTemplate {
    ResourceTemplate::new(uri, name)
        .with_title(title)
        .with_description(description)
        .with_mime_type("application/json")
}
