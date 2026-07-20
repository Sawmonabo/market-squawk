//! Local-first Market Squawk application composition.
//!
//! Production live batches enter only [`live_runtime::LiveRuntimeComposition`]. The legacy local
//! event model is explicitly diagnostic and remains isolated from current execution authority.

pub mod bot;
pub mod diagnostic_engine;
mod domain;
pub mod features;
pub mod live_runtime;
mod live_source;
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

pub use diagnostic_engine::{
    DiagnosticEngine, DiagnosticEngineSnapshot, DiagnosticProductSnapshot, SharedDiagnosticEngine,
};
pub use domain::{
    BookChange as DiagnosticBookChange, MarketEvent as DiagnosticMarketEvent,
    PriceLevel as DiagnosticPriceLevel, RawEnvelope as DiagnosticRawEnvelope,
    Side as DiagnosticSide,
};
pub use live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};
pub use live_source::{
    ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError, ProductionSupervisorError,
};
pub use market_squawk_platform::{
    AppConfig, JournalFileFormat, JournalSelectionError, LocalPaths as AppPaths,
};
