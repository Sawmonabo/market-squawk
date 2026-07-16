//! Local-first live market data engine.
//!
//! The latency-sensitive path is intentionally isolated from MCP and analytical consumers.

pub mod bot;
pub mod domain;
pub mod engine;
pub mod features;
pub mod mcp;
pub mod order_book;
pub mod quality;
pub mod replay;
pub mod risk;
pub mod source;
pub mod source_supervisor;

/// Platform journal compatibility facade retained for existing application imports.
pub mod journal {
    pub use market_squawk_platform::{JournalError, JournalReader, JournalWriter};
}

pub use domain::{MarketEvent, RawEnvelope};
pub use engine::{Engine, EngineSnapshot, SharedEngine};
pub use market_squawk_platform::{
    AppConfig as EngineConfig, JournalFileFormat, JournalSelectionError, LocalPaths as AppPaths,
};
