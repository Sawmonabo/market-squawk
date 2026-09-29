//! Stateless installed transport for financial settings; client workflow state remains separate.

use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;

use crate::application::{
    analytical_profile::{AnalyticalProfileConfiguration, catalog, resolve},
    model::forecast_preparation::ForecastPreparationCatalog,
};

pub(super) const GET_CATALOG: &str = "AnalyticalProfile.GetCatalog";
pub(super) const RESOLVE: &str = "AnalyticalProfile.Resolve";

pub(super) fn owns(operation: &str) -> bool {
    matches!(operation, GET_CATALOG | RESOLVE)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveRequest {
    configuration: Option<AnalyticalProfileConfiguration>,
}

/// Uses the existing forecast preparation catalogue supplied by installed composition. This
/// adapter owns no runtime, model registry, profile persistence, or independent financial default.
pub(super) fn call(
    request: &TypedToolRequest,
    context: &RequestContext,
    models: Option<&ForecastPreparationCatalog>,
    benchmarks: &crate::application::RecommendationBenchmarkSelectionReadCapability,
) -> Result<TypedToolResult, ServiceError> {
    ensure_live(context)?;
    let arguments = super::business_arguments(request.arguments());
    let (content, count) = match request.name() {
        GET_CATALOG => {
            if !arguments.is_empty() {
                return Err(ServiceError::InvalidRequest);
            }
            let observed_at = super::runtime::current_timestamp()
                .map_err(|_| ServiceError::Internal)?;
            let choices = benchmarks.comparison_choices(
                observed_at, observed_at, context.deadline(), context.cancellation(),
            )?;
            (catalog(models, &choices)?, 10)
        }
        RESOLVE => {
            let input: ResolveRequest =
                serde_json::from_value(serde_json::Value::Object(arguments))
                    .map_err(|_| ServiceError::InvalidRequest)?;
            let resolved = resolve(input.configuration, models)?;
            (
                serde_json::to_value(resolved.resolution())
                    .map_err(|_| ServiceError::InvalidResult)?,
                10,
            )
        }
        _ => return Err(ServiceError::NotFound),
    };
    ensure_live(context)?;
    TypedToolResult::try_new(
        content,
        count,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(ServiceError::from)
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
