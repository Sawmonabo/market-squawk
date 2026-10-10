//! Ordinary financial market-session context backed by actual committed returned entries.
use crate::application::{
    market_calendar::context::{
        MarketSessionContextReadCapability, MarketSessionContextReference,
        MarketSessionContextRequest,
    },
    MarketRuntimeRegistry,
};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
pub(super) const GET: &str = "Market.GetSessionContext";
pub(super) const READ: &str = "Market.ReadSessionContext";
pub(super) fn owns(operation: &str) -> bool {
    matches!(operation, GET | READ)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest {
    reference: MarketSessionContextReference,
}
pub(super) async fn call(
    request: &TypedToolRequest,
    context: &RequestContext,
    runtime: &MarketRuntimeRegistry,
    reader: &MarketSessionContextReadCapability,
) -> Result<TypedToolResult, ServiceError> {
    ensure_live(context)?;
    let arguments = serde_json::Value::Object(super::business_arguments(request.arguments()));
    let read = match request.name() {
        GET => {
            let input: MarketSessionContextRequest =
                serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
            let (committed, binding) = runtime
                .acquire_market_session_context(
                    &input,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?;
            reader
                .read_committed(
                    &input,
                    &committed,
                    binding,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?
        }
        READ => {
            let input: ReadRequest =
                serde_json::from_value(arguments).map_err(|_| ServiceError::InvalidRequest)?;
            reader
                .read_reference(
                    &input.reference,
                    context.deadline(),
                    context.cancellation().clone(),
                )
                .await?
        }
        _ => return Err(ServiceError::NotFound),
    };
    ensure_live(context)?;
    TypedToolResult::try_new(
        read.projection()?,
        read.entry_count(),
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
