//! Local-first live market data engine.
//!
//! The latency-sensitive path is intentionally isolated from MCP and analytical consumers.

pub mod bot;
pub mod config;
pub mod domain;
pub mod engine;
pub mod features;
pub mod journal;
pub mod mcp;
pub mod order_book;
pub mod quality;
pub mod replay;
pub mod risk;
pub mod source;

pub use config::{AppPaths, EngineConfig, JournalFileFormat, JournalSelectionError};
pub use domain::{MarketEvent, RawEnvelope};
pub use engine::{Engine, EngineSnapshot, SharedEngine};
