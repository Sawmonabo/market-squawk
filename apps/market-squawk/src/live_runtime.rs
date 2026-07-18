//! Safe application ownership boundary for the production live runtime.
//!
//! Source supervisors start this composition before opening a feed, obtain a route-bound ingress
//! through the live crate's current-source lease handshake, and shut sources down before consuming
//! this owner. No app-local diagnostic event can be converted into production ingress.
//!
//! The exact compatibility deletion trigger is: production adapters emit receipt-validated
//! `CurrentDecodedProviderBatch` values after pre-feed binding, and application services consume
//! bounded production snapshots. Once both are integrated, `diagnostic_engine` and the
//! app-local event/book/quality path are deleted rather than promoted.

use market_squawk_live::{
    BoundShardIngress, LiveIngressBindError, LiveRouteConfig, LiveRuntime, LiveRuntimeConfig,
    LiveRuntimeHealthEvent, LiveRuntimeReplaceError, LiveRuntimeShutdown, LiveRuntimeStartError,
    LiveSnapshotReader, ShardId, ShardKey,
};
use market_squawk_sources::CurrentSourceAuthorityLease;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Complete application owner for production live state and its bounded lifecycle.
#[derive(Debug)]
pub struct LiveRuntimeComposition {
    runtime: LiveRuntime,
}

impl LiveRuntimeComposition {
    /// Starts every shard and its initial immutable snapshot before returning source ingress.
    pub async fn start(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        Ok(Self {
            runtime: LiveRuntime::start(config, routes).await?,
        })
    }

    /// Returns bounded authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.runtime.snapshots()
    }

    /// Performs the bounded pre-feed control handshake for one exact current source allocation.
    pub async fn bind_generation(
        &self,
        route: ShardKey,
        source: CurrentSourceAuthorityLease,
        cancellation: CancellationToken,
    ) -> Result<BoundShardIngress, LiveRuntimeCompositionError> {
        self.runtime
            .ingress()
            .bind_generation(route, source, cancellation)
            .await
            .map_err(LiveRuntimeCompositionError::Bind)
    }

    /// Returns the exact nonzero process-local runtime incarnation.
    pub const fn incarnation(&self) -> std::num::NonZeroU64 {
        self.runtime.incarnation()
    }

    /// Returns the checked conservative peak retained-memory model accepted at startup.
    pub const fn estimated_peak_bytes(&self) -> std::num::NonZeroU64 {
        self.runtime.estimated_peak_bytes()
    }

    /// Takes one bounded best-effort health mirror without waiting.
    pub fn try_next_health(&mut self) -> Option<LiveRuntimeHealthEvent> {
        self.runtime.try_next_health()
    }

    /// Takes one fair, coalesced snapshot-change hint keyed by its exact shard.
    pub fn try_next_snapshot_notification(&mut self) -> Option<ShardId> {
        self.runtime.try_next_snapshot_notification()
    }

    /// Fully shuts down the former incarnation before starting a clean replacement.
    pub async fn replace(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        Ok(Self {
            runtime: self.runtime.replace(config, routes).await?,
        })
    }

    /// Performs explicit bounded shutdown after source producers have stopped.
    pub async fn shutdown(self) -> Result<LiveRuntimeShutdown, LiveRuntimeCompositionError> {
        let outcome = self.runtime.shutdown().await;
        if outcome.is_complete() {
            Ok(outcome)
        } else {
            Err(LiveRuntimeCompositionError::IncompleteShutdown(outcome))
        }
    }
}

/// Safe composition startup or inspected shutdown failure.
#[derive(Debug, Error)]
pub enum LiveRuntimeCompositionError {
    #[error(transparent)]
    Start(#[from] LiveRuntimeStartError),
    #[error(transparent)]
    Bind(LiveIngressBindError),
    #[error(transparent)]
    Replace(#[from] LiveRuntimeReplaceError),
    #[error("production live runtime shutdown was incomplete")]
    IncompleteShutdown(LiveRuntimeShutdown),
}
