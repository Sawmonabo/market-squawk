//! Validated configuration and supervised ownership for the bounded live shard runtime.

use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;

#[path = "runtime/actor.rs"]
mod actor;
#[path = "runtime/admission.rs"]
mod admission;
#[path = "runtime/config.rs"]
mod config;
#[path = "runtime/lifecycle.rs"]
mod lifecycle;
#[path = "runtime/memory.rs"]
mod memory;

pub use admission::{
    BoundShardIngress, LiveIngressBindError, LiveIngressError, LiveRuntimeHealthEvent,
    LiveRuntimeHealthKind, LiveRuntimeIngress, RegistrationFailure,
};
pub use config::{
    LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput, MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT,
};
pub use lifecycle::{
    LiveRuntime, LiveRuntimeReplaceError, LiveRuntimeShutdown, LiveRuntimeStartError,
    ShardShutdownOutcome, ShardShutdownStatus,
};

fn system_timestamp() -> Result<Timestamp, ()> {
    let system = SystemTime::now();
    let nanos = match system.duration_since(UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    i64::try_from(nanos)
        .map(Timestamp::from_unix_nanos)
        .map_err(|_| ())
}
