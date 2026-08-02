//! Transport-neutral contracts for the installed per-user Market Squawk service.
//!
//! The crate owns service/client identities, bounded request admission, native streaming input
//! tickets, and generation-aware event cursors without depending on application composition, MCP,
//! Tauri, or a financial domain engine.

mod auth;
mod client;
mod contracts;
mod events;
mod input;
mod rendezvous;
mod replay;
mod router;
mod streaming;

pub use auth::*;
pub use client::*;
pub use contracts::*;
pub use events::*;
pub use input::*;
pub use rendezvous::*;
pub use replay::*;
pub use router::*;
pub use streaming::*;

#[cfg(test)]
mod tests;
