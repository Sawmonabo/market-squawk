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
    BoundShardIngress, LiveActionControlError, LiveActionHookReapReceipt, LiveIngressBindError,
    LiveRouteConfig, LiveRuntime, LiveRuntimeConfig, LiveRuntimeHealthEvent, LiveRuntimeIngress,
    LiveRuntimeReplaceError, LiveRuntimeShutdown, LiveRuntimeStartError, LiveSnapshotReader,
    PreparedLiveActionHookGroup, RouteActionHook, RouteQualifiedMarketExport, ShardId, ShardKey,
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

    /// Starts every shard only after one execution action hook is transferred for every route.
    ///
    /// # Errors
    ///
    /// Returns a typed startup error when routes, hooks, resource bounds, or actor startup are
    /// invalid. A failed startup does not leave detached live actors.
    pub async fn start_with_action_hooks(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        Ok(Self {
            runtime: LiveRuntime::start_with_action_hooks(config, routes, action_hooks).await?,
        })
    }

    /// Starts every shard with exact action hooks and bounded post-decision market exports.
    ///
    /// Export route validation and complete retained-memory accounting remain owned by the live
    /// runtime. Sender ownership transfers into live actors; this composition never owns or
    /// spawns a consumer for the independently returned receivers.
    ///
    /// # Errors
    ///
    /// Returns a typed startup error when routes, hooks, exports, resource bounds, or actor startup
    /// are invalid. A failed startup does not leave detached live actors.
    pub async fn start_with_action_hooks_and_qualified_market_exports(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        Ok(Self {
            runtime: LiveRuntime::start_with_action_hooks_and_qualified_market_exports(
                config,
                routes,
                action_hooks,
                qualified_market_exports,
            )
            .await?,
        })
    }

    /// Starts every shard with a bounded qualified-market export and no execution authority.
    ///
    /// This is the source-only runtime used by market display, research, and valuation consumers.
    /// It deliberately installs no strategy or execution action hook.
    pub async fn start_with_qualified_market_exports(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        Ok(Self {
            runtime: LiveRuntime::start_with_qualified_market_exports(
                config,
                routes,
                qualified_market_exports,
            )
            .await?,
        })
    }

    /// Returns bounded authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.runtime.snapshots()
    }

    /// Returns the bind-only ingress capability to the crate-private production source owner.
    ///
    /// This is not part of the public application API: CLI, MCP, diagnostic, and strategy code
    /// cannot obtain it. The production source composition uses it only to reserve every route
    /// before opening a provider connection and to complete the current-authority handshake.
    pub(crate) fn production_ingress(&self) -> LiveRuntimeIngress {
        self.runtime.ingress()
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

    /// Transfers one complete route-hook group into the running actors while it remains disabled.
    pub async fn prepare_action_hooks(
        &mut self,
        hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<PreparedLiveActionHookGroup, LiveRuntimeCompositionError> {
        self.runtime
            .prepare_action_hooks(hooks, cancellation)
            .await
            .map_err(LiveRuntimeCompositionError::ActionControl)
    }

    /// Removes the exact disabled dynamic hook group from every owning actor.
    pub async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<LiveActionHookReapReceipt, LiveRuntimeCompositionError> {
        self.runtime
            .reap_action_hooks(cancellation)
            .await
            .map_err(LiveRuntimeCompositionError::ActionControl)
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
    #[error(transparent)]
    ActionControl(LiveActionControlError),
    #[error("production live runtime shutdown was incomplete")]
    IncompleteShutdown(LiveRuntimeShutdown),
}
