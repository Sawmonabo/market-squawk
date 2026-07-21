//! Private production live-source composition.

mod composition;
mod instruments;
mod kraken;
mod provider;
mod route_actor;
mod sink;
mod subscription_state;
mod supervisor;

pub use composition::{
    ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError,
};
pub use provider::ProductionSourceProvider;
pub use supervisor::ProductionSupervisorError;

#[cfg(test)]
mod tests;
