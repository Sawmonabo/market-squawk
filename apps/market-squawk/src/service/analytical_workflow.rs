//! Installed operation adapter for the sole shared native workflow owner.
use crate::application::analytical_workflow::{
    self, AnalyticalControllerCommand,
    host::{WorkflowHost, WorkflowState},
};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde_json::{Map, Value};
use std::sync::Arc;
pub(super) fn owns(name: &str) -> bool {
    matches!(
        name,
        "Analysis.ReadWorkflow"
            | "Analysis.UpdateWorkflow"
            | "Analysis.GetWorkflowProduct"
            | "Analysis.StartForecastDelivery"
            | "Analysis.StartBacktestDelivery"
    )
}
pub(super) async fn call(
    host: &Arc<WorkflowHost>,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let generation = host
        .generation(context.origin().ok_or(ServiceError::Unauthorized)?)
        .map_err(|_| ServiceError::Unauthorized)?;
    let content = match request.name() {
        "Analysis.ReadWorkflow" | "Analysis.UpdateWorkflow" => {
            let command: AnalyticalControllerCommand = serde_json::from_value(
                request
                    .arguments()
                    .get("request")
                    .cloned()
                    .ok_or(ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
            let update = request.name() == "Analysis.UpdateWorkflow";
            if command.requires_confirmation() != update
                || update && request.arguments().get("confirm") != Some(&Value::Bool(true))
            {
                return Err(ServiceError::Unauthorized);
            }
            analytical_workflow::analytical_controller(command, update, generation).await
        }
        "Analysis.GetWorkflowProduct" => analytical_workflow::analytical_product(generation)
            .await
            .map(|value| value["data"].clone()),
        "Analysis.StartForecastDelivery" | "Analysis.StartBacktestDelivery" => {
            if request.arguments().get("confirm") != Some(&Value::Bool(true)) {
                return Err(ServiceError::Unauthorized);
            }
            let operation = if request.name() == "Analysis.StartForecastDelivery" {
                "Model.StartPreparedForecast"
            } else {
                "Analysis.StartPreparedBacktest"
            };
            let mut arguments = Map::new();
            arguments.insert(
                "confirmationToken".into(),
                request
                    .arguments()
                    .get("confirmationToken")
                    .cloned()
                    .ok_or(ServiceError::InvalidRequest)?,
            );
            let result = analytical_workflow::start_prepared(
                &WorkflowState,
                &generation,
                operation,
                arguments,
                true,
            )
            .await
            .map(|value| value["data"].clone());
            host.launch(context.origin().ok_or(ServiceError::Unauthorized)?)
                .map_err(|_| ServiceError::Unavailable)?;
            result
        }
        _ => return Err(ServiceError::NotFound),
    };
    let content = content.unwrap_or_else(|error| error.into_projection());
    TypedToolResult::try_new(
        content,
        1,
        ToolResultMetadata::complete_not_applicable(),
        context.limits(),
    )
    .map_err(ServiceError::from)
}
