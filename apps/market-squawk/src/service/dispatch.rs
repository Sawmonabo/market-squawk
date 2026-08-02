//! Installed-service application dispatch and named MCP client authentication.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use market_squawk_mcp::{AuthenticatedMcpClient, McpHttpAuthError, McpHttpAuthenticator};
use market_squawk_runtime::{
    AppRequestEnvelope, ApplicationDispatcher, ClientCredentialRegistration, CredentialRegistry,
    DispatchError, NamedClient, OperationEffect,
};
use market_squawk_services::{ServiceError, ServiceErrorClass, ToolAuthorization, ToolServices};
use serde_json::{Value, json};

use crate::LocalProduct;

use super::tool_services::InstalledToolServices;

/// Runtime dispatch adapter over the sole transport-neutral application authority.
pub(super) struct InstalledApplicationDispatcher {
    services: Arc<InstalledToolServices>,
    bootstrap: Value,
}

impl InstalledApplicationDispatcher {
    pub(super) fn try_new(
        services: Arc<InstalledToolServices>,
        product: &LocalProduct,
        runtime: market_squawk_runtime::RuntimeIdentity,
    ) -> Result<Self, DispatchError> {
        let operations = services
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
            bootstrap,
        })
    }
}

impl fmt::Debug for InstalledApplicationDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledApplicationDispatcher")
            .field("services", &"[INSTALLED TOOL SERVICES]")
            .field("bootstrap", &"[NON-SECRET PRODUCT SNAPSHOT]")
            .finish()
    }
}

#[async_trait]
impl ApplicationDispatcher for InstalledApplicationDispatcher {
    fn bootstrap(&self) -> Result<Value, DispatchError> {
        Ok(self.bootstrap.clone())
    }

    fn effect(
        &self,
        operation: &market_squawk_domain::SourceIdentifier,
    ) -> Result<OperationEffect, DispatchError> {
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
        let Value::Object(arguments) = request.arguments() else {
            return Err(DispatchError::Rejected);
        };
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

/// Exact two-client authenticator for the shared stateless MCP endpoint.
pub(super) struct InstalledMcpAuthenticator {
    registry: Arc<CredentialRegistry>,
    clients: [(ClientCredentialRegistration, AuthenticatedMcpClient); 2],
}

impl InstalledMcpAuthenticator {
    pub(super) fn new(
        registry: Arc<CredentialRegistry>,
        claude_code: ClientCredentialRegistration,
        codex: ClientCredentialRegistration,
        maximum_active_requests: usize,
    ) -> Result<Self, McpHttpAuthError> {
        if claude_code.client() != NamedClient::ClaudeCode
            || codex.client() != NamedClient::Codex
            || maximum_active_requests == 0
        {
            return Err(McpHttpAuthError::InvalidIdentity);
        }
        let claude_identity = AuthenticatedMcpClient::try_new(
            claude_code.client(),
            claude_code.client_id(),
            claude_code.generation(),
            maximum_active_requests,
        )?;
        let codex_identity = AuthenticatedMcpClient::try_new(
            codex.client(),
            codex.client_id(),
            codex.generation(),
            maximum_active_requests,
        )?;
        Ok(Self {
            registry,
            clients: [(claude_code, claude_identity), (codex, codex_identity)],
        })
    }
}

impl fmt::Debug for InstalledMcpAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledMcpAuthenticator")
            .field("registry", &"[CREDENTIAL AUTHORITY]")
            .field("clients", &"[MCP CLIENT IDENTITIES]")
            .finish()
    }
}

impl McpHttpAuthenticator for InstalledMcpAuthenticator {
    fn authenticate(&self, bearer_token: &str) -> Result<AuthenticatedMcpClient, McpHttpAuthError> {
        let mut matched = None;
        for (registration, identity) in &self.clients {
            if self
                .registry
                .authenticate(
                    registration.client_id(),
                    registration.generation(),
                    bearer_token.as_bytes(),
                )
                .is_ok()
            {
                if matched.is_some() {
                    return Err(McpHttpAuthError::Rejected);
                }
                matched = Some(identity.clone());
            }
        }
        matched.ok_or(McpHttpAuthError::Rejected)
    }
}
