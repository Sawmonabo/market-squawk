//! Shared MCP audit and installed-client support.
//!
//! Shipping stdio clients relay into the authenticated [`crate::service::InstalledService`]
//! endpoint so native and MCP clients always use the same installed application authorities.
//! The superseded application-only server is intentionally absent.
//!
//! ```compile_fail
//! use market_squawk::mcp::McpServer;
//! ```

pub(crate) mod audit;
pub mod clients;
#[cfg(test)]
mod journal_worker;
#[cfg(test)]
mod services;

pub use audit::LocalAuditError;
