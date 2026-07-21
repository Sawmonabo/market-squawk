pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};

#[allow(
    dead_code,
    reason = "the consolidated action tests consume different parts of the shared fixture"
)]
#[path = "../support/current_source.rs"]
mod current_source;

#[path = "../action_boundary.rs"]
mod action_boundary;
#[path = "../action_runtime.rs"]
mod action_runtime;
#[path = "../runtime_rejection.rs"]
mod runtime_rejection;
