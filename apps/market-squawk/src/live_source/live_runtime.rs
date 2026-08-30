//! Production live-runtime owner supporting the independently bounded research export plane.

use market_squawk_live::{
    LiveRouteConfig, LiveRuntime, LiveRuntimeConfig, LiveRuntimeExportPlan, LiveRuntimeIngress,
    LiveSnapshotReader, PreparedLiveActionHookGroup, RouteActionHook,
    RouteCommittedResearchMarketExport, RouteQualifiedMarketExport,
};
use tokio_util::sync::CancellationToken;

use crate::live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};

#[derive(Debug)]
pub(super) enum ProductionLiveRuntimeOwner {
    Standard(LiveRuntimeComposition),
    ResearchExports(LiveRuntime),
}

impl ProductionLiveRuntimeOwner {
    pub(super) const fn standard(runtime: LiveRuntimeComposition) -> Self {
        Self::Standard(runtime)
    }

    pub(super) async fn start_with_research_exports(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        qualified: Vec<RouteQualifiedMarketExport>,
        committed: Vec<RouteCommittedResearchMarketExport>,
    ) -> Result<Self, LiveRuntimeCompositionError> {
        let runtime = LiveRuntime::start_with_exports(
            config,
            routes,
            LiveRuntimeExportPlan::new(qualified, committed),
        )
        .await
        .map_err(LiveRuntimeCompositionError::Start)?;
        Ok(Self::ResearchExports(runtime))
    }

    pub(super) fn snapshots(&self) -> LiveSnapshotReader {
        match self {
            Self::Standard(runtime) => runtime.snapshots(),
            Self::ResearchExports(runtime) => runtime.snapshots(),
        }
    }

    pub(super) fn production_ingress(&self) -> LiveRuntimeIngress {
        match self {
            Self::Standard(runtime) => runtime.production_ingress(),
            Self::ResearchExports(runtime) => runtime.ingress(),
        }
    }

    pub(super) async fn prepare_action_hooks(
        &mut self,
        hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<PreparedLiveActionHookGroup, LiveRuntimeCompositionError> {
        match self {
            Self::Standard(runtime) => runtime.prepare_action_hooks(hooks, cancellation).await,
            Self::ResearchExports(runtime) => runtime
                .prepare_action_hooks(hooks, cancellation)
                .await
                .map_err(LiveRuntimeCompositionError::ActionControl),
        }
    }

    pub(super) async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_live::LiveActionHookReapReceipt, LiveRuntimeCompositionError> {
        match self {
            Self::Standard(runtime) => runtime.reap_action_hooks(cancellation).await,
            Self::ResearchExports(runtime) => runtime
                .reap_action_hooks(cancellation)
                .await
                .map_err(LiveRuntimeCompositionError::ActionControl),
        }
    }

    pub(super) async fn shutdown(self) -> Result<(), LiveRuntimeCompositionError> {
        match self {
            Self::Standard(runtime) => runtime.shutdown().await.map(|_outcome| ()),
            Self::ResearchExports(runtime) => {
                let outcome = runtime.shutdown().await;
                if outcome.is_complete() {
                    Ok(())
                } else {
                    Err(LiveRuntimeCompositionError::IncompleteShutdown(outcome))
                }
            }
        }
    }
}
