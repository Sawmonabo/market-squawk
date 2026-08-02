//! Stable, bounded MCP resource contracts over application-owned metadata.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use market_squawk_jobs::JobId;
use market_squawk_services::RequestContext;
use rmcp::model::{Resource, ResourceTemplate};
use serde_json::Value;
use thiserror::Error;

use crate::jobs::{JOB_RESOURCE_TEMPLATE, parse_job_resource_uri};

const SERVICE_URI: &str = "market-squawk://service";
const WORKSPACE_URI: &str = "market-squawk://workspace";
const SOURCE_TEMPLATE: &str = "market-squawk://sources/{source_id}";
const MODEL_TEMPLATE: &str = "market-squawk://models/{model_id}";
const ARTIFACT_TEMPLATE: &str = "market-squawk://artifacts/{artifact_id}";

/// Closed resource identity passed to the application-owned resource provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpResourceRequest {
    /// Installed service metadata.
    Service,
    /// Active workspace metadata.
    Workspace,
    /// Registered source metadata by opaque source identity.
    Source(Arc<str>),
    /// Registered model metadata by opaque model identity.
    Model(Arc<str>),
    /// Durable job event/result inspection by typed job identity.
    Job(JobId),
    /// Published artifact metadata by opaque artifact identity.
    Artifact(Arc<str>),
}

impl McpResourceRequest {
    /// Parses one stable resource URI without granting filesystem or generic network authority.
    ///
    /// # Errors
    ///
    /// Returns [`McpResourceError::InvalidUri`] when the URI is not one of the closed V1 forms.
    pub fn try_from_uri(uri: &str) -> Result<Self, McpResourceError> {
        if uri == SERVICE_URI {
            return Ok(Self::Service);
        }
        if uri == WORKSPACE_URI {
            return Ok(Self::Workspace);
        }
        if let Ok(job_id) = parse_job_resource_uri(uri) {
            return Ok(Self::Job(job_id));
        }
        parse_opaque_path(uri, "market-squawk://sources/")
            .map(Self::Source)
            .or_else(|| parse_opaque_path(uri, "market-squawk://models/").map(Self::Model))
            .or_else(|| parse_opaque_path(uri, "market-squawk://artifacts/").map(Self::Artifact))
            .ok_or(McpResourceError::InvalidUri)
    }
}

/// One bounded application-owned resource document.
#[derive(Clone)]
pub struct McpResourceDocument {
    value: Value,
    item_count: usize,
}

impl McpResourceDocument {
    /// Creates a nonempty logical resource document.
    ///
    /// # Errors
    ///
    /// Returns [`McpResourceError::InvalidDocument`] for a zero item count.
    pub fn try_new(value: Value, item_count: usize) -> Result<Self, McpResourceError> {
        if item_count == 0 {
            Err(McpResourceError::InvalidDocument)
        } else {
            Ok(Self { value, item_count })
        }
    }

    /// Structured document content.
    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Logical records represented by the document.
    #[must_use]
    pub const fn item_count(&self) -> usize {
        self.item_count
    }
}

impl fmt::Debug for McpResourceDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpResourceDocument")
            .field("value", &"[RESOURCE CONTENT REDACTED]")
            .field("item_count", &self.item_count)
            .finish()
    }
}

/// Application authority that resolves the closed V1 MCP resource namespace.
#[async_trait]
pub trait McpResourceProvider: fmt::Debug + Send + Sync + 'static {
    /// Reads one resource under the caller's bounded request lifecycle.
    async fn read(
        &self,
        request: McpResourceRequest,
        context: RequestContext,
    ) -> Result<McpResourceDocument, McpResourceError>;
}

/// Closed resource failure without provider payloads or ambient paths.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum McpResourceError {
    /// URI is outside the closed Market Squawk resource namespace.
    #[error("resource URI is invalid")]
    InvalidUri,
    /// Resource was not found under the authenticated workspace.
    #[error("resource was not found")]
    NotFound,
    /// Caller lacks authority for this resource.
    #[error("resource is not authorized")]
    Unauthorized,
    /// Resource content or logical count violated its contract.
    #[error("resource document is invalid")]
    InvalidDocument,
    /// Request was cancelled or exceeded its deadline.
    #[error("resource request did not complete")]
    Interrupted,
    /// Resource authority is temporarily unavailable.
    #[error("resource authority is unavailable")]
    Unavailable,
}

pub(crate) fn stable_resources() -> Arc<[Resource]> {
    [
        Resource::new(SERVICE_URI, "service")
            .with_title("Installed service")
            .with_description("Authenticated metadata for the installed Market Squawk service")
            .with_mime_type("application/json"),
        Resource::new(WORKSPACE_URI, "workspace")
            .with_title("Active workspace")
            .with_description("Authenticated metadata for the active Market Squawk workspace")
            .with_mime_type("application/json"),
    ]
    .into()
}

pub(crate) fn stable_resource_templates() -> Arc<[ResourceTemplate]> {
    [
        template(SOURCE_TEMPLATE, "source", "Registered source metadata"),
        template(MODEL_TEMPLATE, "model", "Registered model metadata"),
        template(
            JOB_RESOURCE_TEMPLATE,
            "job",
            "Durable job events and terminal result metadata",
        ),
        template(ARTIFACT_TEMPLATE, "artifact", "Published artifact metadata"),
    ]
    .into()
}

fn template(uri: &str, name: &str, description: &str) -> ResourceTemplate {
    ResourceTemplate::new(uri, name)
        .with_title(description)
        .with_description(description)
        .with_mime_type("application/json")
}

fn parse_opaque_path(uri: &str, prefix: &str) -> Option<Arc<str>> {
    uri.strip_prefix(prefix)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 1_024
                && !value.contains('/')
                && !value.chars().any(char::is_control)
        })
        .map(Arc::from)
}
