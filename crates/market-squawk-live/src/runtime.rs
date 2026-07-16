//! Validated configuration and supervised ownership for the bounded live shard runtime.

#[path = "runtime/config.rs"]
mod config;

pub use config::{
    LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput,
};
