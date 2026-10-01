//! Private production live-source composition.

mod composition;
pub(crate) mod crypto_reference;
pub(crate) mod crypto_reference_transport;
mod direct;
pub(crate) mod display_market;
mod instruments;
mod kraken;
mod kraken_level3;
mod kraken_publication;
pub(crate) use sink::{AlpacaCapturedPublicationIngress, AlpacaCapturedPublicationReceiver};
pub(crate) mod order_level;
mod provider;
mod publication_admission;
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
pub(crate) use instruments::ProductionCatalogSelection;
pub(crate) use kraken_level3::{KrakenLevel3LiveRuntime, KrakenLevel3RuntimeError};
pub use provider::ProductionSourceProvider;
pub(crate) use provider::{ALPACA_IEX_LIVE_AUTHORITY_KEY, ALPACA_OPTIONS_LIVE_AUTHORITY_KEY};
#[cfg(feature = "release-evidence")]
pub(crate) use release_support::{CoinbaseReleaseEvidence, run_coinbase_release_evidence};
pub(crate) use schwab_rest::{
    SchwabQualifiedCurrent, SchwabRestQuoteCurrentBridge, SchwabRestQuoteCurrentEvidence,
    SchwabRestQuoteCurrentInstrument, SchwabRestQuoteCurrentPublication,
    SchwabRestQuoteCurrentRequest, SchwabRestQuoteCurrentSessionBridge,
    SchwabRestQuoteCurrentSessionInput, SchwabRestQuoteCurrentUnavailable,
    SchwabStreamerCurrentEvidence,
};
pub use supervisor::ProductionSupervisorError;

#[cfg(test)]
mod tests;
