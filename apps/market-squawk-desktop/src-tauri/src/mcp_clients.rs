//! Secret-free readiness projection for the shared installed MCP endpoint.

use tauri::State;

use crate::{
    bridge::{DesktopState, map_application_client_error},
    contracts::{DesktopCommandError, McpStatus},
};

#[tauri::command]
pub(crate) async fn mcp_status(
    state: State<'_, DesktopState>,
) -> Result<McpStatus, DesktopCommandError> {
    state
        .application()
        .probe_ready(state.cancellation())
        .await
        .map_err(map_application_client_error)?;
    Ok(McpStatus::service_ready(state.mcp_ready()))
}
