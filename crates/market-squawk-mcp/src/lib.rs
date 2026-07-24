//! Bounded local stdio Model Context Protocol transport.
//!
//! The crate delegates MCP lifecycle and typed protocol handling to the pinned official Rust SDK,
//! while retaining owned framing, structural resource admission, output backpressure, audit, and
//! opaque artifact boundaries. Business-domain implementations live behind
//! [`market_squawk_services::ToolServices`].

mod audit;
mod framing;
mod isolation;
mod limits;
mod protocol;
mod server;

pub use audit::{
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, LocalProcessIdentityClass, MutationAuditBundle,
    MutationAuditReservation,
};
pub use limits::{McpLimitError, McpLimitSpec, McpLimits};
pub use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
pub use server::{McpServer, ServerError, ServerExit};
