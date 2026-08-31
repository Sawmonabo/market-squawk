//! Private production live-source composition.

mod composition;
mod direct;
pub(crate) mod display_market;
pub(crate) mod instruments;
mod kraken;
mod kraken_level3;
mod kraken_publication;
pub(crate) mod order_level;
mod provider;
#[cfg(feature = "release-evidence")]
mod release_support;
mod route_actor;
mod schwab_rest;
mod sink;
mod subscription_state;
mod supervisor;

pub use composition::{
    ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError,
};
pub(crate) use direct::try_build_product_metadata_set as coinbase_direct_publication_metadata;
pub use direct::{
    CoinbaseDirectLiveRuntime, CoinbaseDirectOutputFailure, CoinbaseDirectProductRuntimeError,
    CoinbaseDirectSupervisorError,
};
pub(crate) use kraken_level3::KrakenLevel3LiveRuntime;
pub use provider::ProductionSourceProvider;
#[cfg(feature = "release-evidence")]
pub(crate) use release_support::{CoinbaseReleaseEvidence, run_coinbase_release_evidence};
pub(crate) use schwab_rest::{
    SchwabRestQuoteCurrentBridge, SchwabRestQuoteCurrentEvidence, SchwabRestQuoteCurrentInstrument,
    SchwabRestQuoteCurrentPublication, SchwabRestQuoteCurrentRequest,
    SchwabRestQuoteCurrentSessionBridge, SchwabRestQuoteCurrentSessionInput,
    SchwabRestQuoteCurrentUnavailable,
};
pub use supervisor::ProductionSupervisorError;

#[cfg(test)]
mod tests;
