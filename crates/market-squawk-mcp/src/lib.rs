//! Bounded local stdio Model Context Protocol transport.
//!
//! The crate delegates MCP lifecycle and typed protocol handling to the pinned official Rust SDK,
//! while retaining owned framing, structural resource admission, output backpressure, audit, and
//! opaque artifact boundaries. Business-domain implementations live behind
//! [`market_squawk_services::ToolServices`].

mod artifact;
mod audit;
mod framing;
mod isolation;
mod limits;
mod protocol;
mod server;

pub use artifact::{ArtifactError, ArtifactPublication, ArtifactReference, ArtifactRepository};
pub use audit::{
    AuditCompletion, AuditCompletionReservation, AuditError, AuditEvent, AuditOperation,
    AuditPhase, AuditResultClass, AuditSink, LocalProcessIdentityClass,
};
pub use limits::{McpLimitError, McpLimitSpec, McpLimits};
pub use server::{McpServer, ServerError, ServerExit};
