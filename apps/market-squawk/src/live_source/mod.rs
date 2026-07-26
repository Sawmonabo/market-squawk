//! Private production live-source composition.

mod composition;
mod direct;
mod instruments;
mod kraken;
mod provider;
#[cfg(feature = "release-evidence")]
mod release_support;
mod route_actor;
mod sink;
mod subscription_state;
mod supervisor;

pub use composition::{
    ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError,
};
pub use direct::{
    CoinbaseDirectLiveRuntime, CoinbaseDirectOutputFailure, CoinbaseDirectProductRuntimeError,
    CoinbaseDirectSupervisorError,
};
pub use provider::ProductionSourceProvider;
#[cfg(feature = "release-evidence")]
pub(crate) use release_support::{CoinbaseReleaseEvidence, run_coinbase_release_evidence};
pub use supervisor::ProductionSupervisorError;

#[cfg(test)]
mod tests;
