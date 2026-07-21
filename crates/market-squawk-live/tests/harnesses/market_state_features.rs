pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};

#[allow(
    dead_code,
    reason = "the consolidated market-state tests consume different parts of the shared fixture"
)]
#[path = "../support/current_source.rs"]
mod current_source;

#[path = "../book.rs"]
mod book;
#[path = "../book_properties.rs"]
mod book_properties;
#[path = "../conversion.rs"]
mod conversion;
#[path = "../cross_venue_features.rs"]
mod cross_venue_features;
#[path = "../feature_memory.rs"]
mod feature_memory;
#[path = "../feature_state.rs"]
mod feature_state;
#[path = "../overflow.rs"]
mod overflow;
#[path = "../sequence.rs"]
mod sequence;
#[path = "../sharding.rs"]
mod sharding;
#[path = "../state_machine.rs"]
mod state_machine;
