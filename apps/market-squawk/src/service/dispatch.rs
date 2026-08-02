//! Installed-service application dispatch and named MCP client authentication.

use std::{fmt, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use market_squawk_runtime::{
    AppRequestEnvelope, ApplicationDispatcher, DispatchError, NamedClient, OperationEffect,
};
use market_squawk_services::{ServiceError, ServiceErrorClass, ToolAuthorization, ToolServices};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::LocalProduct;

use super::{
    governance::InstalledGovernanceOperations,
    mcp_control::{InstalledMcpControl, McpControlError},
    tool_services::InstalledToolServices,
};

/// Runtime dispatch adapter over the sole transport-neutral application authority.
pub(super) struct InstalledApplicationDispatcher {
    services: Arc<InstalledToolServices>,
    mcp: Arc<InstalledMcpControl>,
    governance: Arc<InstalledGovernanceOperations>,
    bootstrap: Value,
}

impl InstalledApplicationDispatcher {
    pub(super) fn try_new(
        services: Arc<InstalledToolServices>,
        product: &LocalProduct,
        runtime: market_squawk_runtime::RuntimeIdentity,
        endpoint: SocketAddr,
        mcp: Arc<InstalledMcpControl>,
        governance: Arc<InstalledGovernanceOperations>,
    ) -> Result<Self, DispatchError> {
        let claude_code = mcp
            .registration(NamedClient::ClaudeCode)
            .map_err(map_mcp_error)?;
        let codex = mcp
            .registration(NamedClient::Codex)
            .map_err(map_mcp_error)?;
        if claude_code.client() != NamedClient::ClaudeCode || codex.client() != NamedClient::Codex {
            return Err(DispatchError::Unavailable);
        }
        let mut operations = services
            .capabilities()
            .tools()
            .iter()
            .map(|descriptor| {
                let effects = descriptor.effects();
                json!({
                    "name": descriptor.name(),
                    "version": descriptor.version(),
                    "description": descriptor.description(),
                    "inputSchema": descriptor.input_schema(),
                    "outputSchema": descriptor.output_schema(),
                    "contract": descriptor.contract(),
                    "metadata": descriptor.metadata(),
                    "effects": {
                        "readOnly": effects.read_only(),
                        "destructive": effects.destructive(),
                        "idempotent": effects.idempotent(),
                        "openWorld": effects.open_world(),
                    },
                })
            })
            .collect::<Vec<_>>();
        operations.extend(governance.desktop_capabilities());
        let onboarding = product.provider_onboarding();
        let profiles = serde_json::to_value(onboarding.profiles())
            .map_err(|_error| DispatchError::Unavailable)?;
        let encrypted_fallback = onboarding
            .encrypted_file_fallback_status()
            .map_err(|_error| DispatchError::Unavailable)?;
        let bootstrap = json!({
            "schemaVersion": 1,
            "product": {
                "name": "Market Squawk",
                "version": env!("CARGO_PKG_VERSION"),
                "deployment": "self_hosted",
            },
            "runtime": runtime,
            "application": {
                "contractVersion": crate::application::APPLICATION_CONTRACT_VERSION,
                "operations": operations,
            },
            "sources": {
                "profiles": profiles,
                "encryptedFileFallback": encrypted_fallback,
            },
            "mcpAuthority": {
                "endpointIdentity": format!("{:x}", Sha256::digest(endpoint.to_string())),
                "claudeCodeCredentialIdentity": format!(
                    "{}:{}",
                    claude_code.client_id().as_uuid(),
                    claude_code.generation().get(),
                ),
                "codexCredentialIdentity": format!(
                    "{}:{}",
                    codex.client_id().as_uuid(),
                    codex.generation().get(),
                ),
            },
            "readiness": {
                "service": true,
                "nativeApplication": true,
                "cli": true,
                "mcp": true,
                "modelRuntimeConfigured": product.model_runtime().is_some(),
            },
        });
        Ok(Self {
            services,
            mcp,
            governance,
            bootstrap,
        })
    }
}

impl fmt::Debug for InstalledApplicationDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledApplicationDispatcher")
            .field("services", &"[INSTALLED TOOL SERVICES]")
            .field("mcp", &"[DYNAMIC MCP CLIENT AUTHORITY]")
            .field("governance", &"[PRIVATE GOVERNANCE AUTHORITY]")
            .field("bootstrap", &"[NON-SECRET PRODUCT SNAPSHOT]")
            .finish()
    }
}

#[async_trait]
impl ApplicationDispatcher for InstalledApplicationDispatcher {
    fn bootstrap(&self) -> Result<Value, DispatchError> {
        let mut bootstrap = self.bootstrap.clone();
        let authority = bootstrap
            .get_mut("mcpAuthority")
            .and_then(Value::as_object_mut)
            .ok_or(DispatchError::Unavailable)?;
        let claude = self
            .mcp
            .registration(NamedClient::ClaudeCode)
            .map_err(map_mcp_error)?;
        let codex = self
            .mcp
            .registration(NamedClient::Codex)
            .map_err(map_mcp_error)?;
        authority.insert(
            "claudeCodeCredentialIdentity".to_owned(),
            Value::String(format!(
                "{}:{}",
                claude.client_id().as_uuid(),
                claude.generation().get()
            )),
        );
        authority.insert(
            "codexCredentialIdentity".to_owned(),
            Value::String(format!(
                "{}:{}",
                codex.client_id().as_uuid(),
                codex.generation().get()
            )),
        );
        Ok(bootstrap)
    }

    fn effect(
        &self,
        operation: &market_squawk_domain::SourceIdentifier,
    ) -> Result<OperationEffect, DispatchError> {
        if let Some(effect) = InstalledMcpControl::effect(operation.as_str()) {
            return Ok(effect);
        }
        if InstalledGovernanceOperations::owns(operation.as_str()) {
            return Ok(
                if InstalledGovernanceOperations::is_mutation(operation.as_str()) {
                    OperationEffect::Mutation
                } else {
                    OperationEffect::Read
                },
            );
        }
        let descriptor = self
            .services
            .capabilities()
            .find(operation.as_str())
            .cloned()
            .ok_or(DispatchError::Rejected)?;
        Ok(
            if matches!(
                descriptor.contract().authorization(),
                ToolAuthorization::ReadOnly
            ) {
                OperationEffect::Read
            } else {
                OperationEffect::Mutation
            },
        )
    }

    async fn dispatch(
        &self,
        request: &AppRequestEnvelope,
        context: market_squawk_services::RequestContext,
    ) -> Result<Value, DispatchError> {
        if InstalledMcpControl::effect(request.operation().as_str()).is_some() {
            return self.mcp.dispatch(request).map_err(map_mcp_error);
        }
        let Value::Object(arguments) = request.arguments() else {
            return Err(DispatchError::Rejected);
        };
        if InstalledGovernanceOperations::owns(request.operation().as_str()) {
            return self
                .governance
                .call(request.operation().as_str(), arguments, &context)
                .await
                .map_err(map_service_error);
        }
        let descriptor = self
            .services
            .capabilities()
            .find(request.operation().as_str())
            .cloned()
            .ok_or(DispatchError::Rejected)?;
        let request = descriptor
            .admit(arguments.clone())
            .map_err(map_service_error)?;
        self.services
            .call(request, context)
            .await
            .map(market_squawk_services::TypedToolResult::into_envelope)
            .map_err(map_service_error)
    }
}

fn map_mcp_error(error: McpControlError) -> DispatchError {
    match error {
        McpControlError::InvalidRequest | McpControlError::Unauthorized => DispatchError::Rejected,
        McpControlError::AuthorityStore(_)
        | McpControlError::Clock
        | McpControlError::Credential(_)
        | McpControlError::HttpAuthentication(_)
        | McpControlError::InvalidState
        | McpControlError::Path(_)
        | McpControlError::RecoveryPending
        | McpControlError::SecretStore => DispatchError::Unavailable,
    }
}

fn map_service_error(error: ServiceError) -> DispatchError {
    match error.class() {
        ServiceErrorClass::InvalidRequest | ServiceErrorClass::NotFound => DispatchError::Rejected,
        ServiceErrorClass::Cancelled | ServiceErrorClass::DeadlineExceeded => {
            DispatchError::Interrupted
        }
        ServiceErrorClass::Unauthorized
        | ServiceErrorClass::ResourceExhausted
        | ServiceErrorClass::Unavailable
        | ServiceErrorClass::InvalidResult
        | ServiceErrorClass::Internal => DispatchError::Unavailable,
    }
}
