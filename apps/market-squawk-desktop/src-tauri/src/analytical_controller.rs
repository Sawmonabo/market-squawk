//! Rendering and confirmation adapter over the installed service's sole workflow owner.
use crate::{
    bridge::{
        DesktopGeneration, DesktopState, InvocationAuthority, invoke_analytical_operation,
        invoke_read_application,
    },
    contracts::{DesktopCommandError, ProductSessionToken},
};
pub(crate) use market_squawk::application::analytical_workflow::AnalyticalControllerCommand;
use market_squawk_services::RequestId;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use tauri::State;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

fn admitted_data(response: Value) -> Result<Value, DesktopCommandError> {
    let data = response
        .get("data")
        .cloned()
        .ok_or_else(DesktopCommandError::internal)?;
    if data.get("kind").and_then(Value::as_str) == Some("unavailable") {
        let code = data
            .get("code")
            .and_then(Value::as_str)
            .and_then(market_squawk::application::analytical_workflow::workflow_error_code)
            .ok_or_else(DesktopCommandError::internal)?;
        let message = data
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(DesktopCommandError::internal)?;
        return Err(DesktopCommandError::new(code, message));
    }
    Ok(data)
}
async fn invoke(
    generation: &Arc<DesktopGeneration>,
    operation: &'static str,
    arguments: Map<String, Value>,
    confirmed: bool,
) -> Result<Value, DesktopCommandError> {
    let authority = if confirmed {
        InvocationAuthority::ExactConfirmed(operation)
    } else {
        InvocationAuthority::ReadOnly
    };
    let response = invoke_analytical_operation(
        generation,
        operation,
        arguments,
        authority,
        RequestId::try_string(format!("desktop-workflow-{}", Uuid::new_v4()))
            .map_err(|_| DesktopCommandError::internal())?,
        CancellationToken::new(),
    )
    .await?;
    admitted_data(response)
}
#[tauri::command]
pub(crate) async fn analytical_controller(
    request: AnalyticalControllerCommand,
    confirmed: bool,
    state: State<'_, DesktopState>,
    request_id: Option<Uuid>,
    product_session_token: Option<ProductSessionToken>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let update = request.requires_confirmation();
    if update && !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm this analysis change before continuing.",
        ));
    }
    let operation = if update {
        "Analysis.UpdateWorkflow"
    } else {
        "Analysis.ReadWorkflow"
    };
    let mut arguments = Map::new();
    arguments.insert(
        "request".into(),
        serde_json::to_value(request).map_err(|_| DesktopCommandError::internal())?,
    );
    let data = if update {
        if request_id.is_some() || product_session_token.is_some() {
            return Err(DesktopCommandError::invalid_request(
                "An analysis change is not a cancellable read.",
            ));
        }
        invoke(&generation, operation, arguments, true).await?
    } else {
        let read = generation.begin_read(
            request_id.ok_or_else(DesktopCommandError::internal)?,
            product_session_token.ok_or_else(DesktopCommandError::internal)?,
        )?;
        admitted_data(invoke_read_application(operation, arguments, &state, &read).await?)?
    };
    state.admit_current(&generation)?;
    Ok(data)
}
#[tauri::command]
pub(crate) async fn analytical_product(
    state: State<'_, DesktopState>,
    request_id: Uuid,
    product_session_token: ProductSessionToken,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let read = generation.begin_read(request_id, product_session_token)?;
    let data = admitted_data(
        invoke_read_application("Analysis.GetWorkflowProduct", Map::new(), &state, &read).await?,
    )?;
    state.admit_current(&generation)?;
    Ok(
        json!({"data":data,"metadata":{"completeness":"complete","returnedItems":1,"availableItems":1}}),
    )
}
pub(crate) async fn start_prepared(
    state: &DesktopState,
    generation: &Arc<DesktopGeneration>,
    operation: &'static str,
    arguments: Map<String, Value>,
    confirmed: bool,
) -> Result<Value, DesktopCommandError> {
    if !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm this analysis before starting it.",
        ));
    }
    state.admit_current(generation)?;
    let operation = match operation {
        "Model.StartPreparedForecast" => "Analysis.StartForecastDelivery",
        "Analysis.StartPreparedBacktest" => "Analysis.StartBacktestDelivery",
        _ => return Err(DesktopCommandError::internal()),
    };
    let data = invoke(generation, operation, arguments, true).await?;
    state.admit_current(generation)?;
    Ok(
        json!({"data":data,"metadata":{"completeness":"complete","returnedItems":1,"availableItems":1}}),
    )
}
