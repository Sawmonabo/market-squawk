//! Validated configuration and supervised ownership for the bounded live shard runtime.

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

pub use config::{
    LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput,
};
