//! Transport-neutral contracts for the installed per-user Market Squawk service.
//!
//! The crate owns service/client identities, bounded request admission, native streaming input
//! tickets, and generation-aware event cursors without depending on application composition, MCP,
//! Tauri, or a financial domain engine.

mod contracts;

pub use contracts::*;
