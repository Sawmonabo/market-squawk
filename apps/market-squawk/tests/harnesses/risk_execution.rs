pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};

#[allow(
    dead_code,
    reason = "the deterministic shared live-source fixture supports several focused cases"
)]
#[path = "../../../../crates/market-squawk-live/tests/support/current_source.rs"]
mod current_source;

#[path = "../risk.rs"]
mod risk;
#[path = "../risk_dispatch_pipeline.rs"]
mod risk_dispatch_pipeline;
