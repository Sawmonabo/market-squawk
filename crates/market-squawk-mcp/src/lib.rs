//! Bounded local stdio Model Context Protocol transport.
//!
//! The crate delegates MCP lifecycle and typed protocol handling to the pinned official Rust SDK,
//! while retaining owned framing, structural resource admission, output backpressure, audit, and
//! opaque artifact boundaries. Business-domain implementations live behind
//! [`market_squawk_services::ToolServices`].

mod audit;
mod framing;
#[cfg(feature = "fuzzing")]
mod fuzzing;
mod handler;
mod http;
mod isolation;
mod jobs;
mod limits;
mod protocol;
mod relay;
mod resources;
mod server;

pub use audit::{
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, LocalProcessIdentityClass, MutationAuditBundle,
    MutationAuditReservation,
};
#[cfg(feature = "fuzzing")]
pub use fuzzing::fuzz_decode_client_message;
pub use handler::{HandlerFactoryError, McpHandlerFactory};
pub use http::{
    AuthenticatedMcpClient, HttpMcpConfig, McpHttpAuthError, McpHttpAuthenticator,
    McpHttpConfigError, McpHttpService,
};
pub use jobs::{JOB_RESOURCE_TEMPLATE, JobResourceError, job_resource_uri, parse_job_resource_uri};
pub use limits::{McpLimitError, McpLimitSpec, McpLimits};
pub use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
pub use relay::{McpRelayError, McpStdioRelay};
pub use resources::{
    McpResourceDocument, McpResourceError, McpResourceProvider, McpResourceRequest,
};
pub use server::{McpServer, ServerError, ServerExit, validate_service_capabilities};
