//! Immutable compile-time dispatcher for capture benchmark backends.

#[cfg(all(
    capture_bench_backend = "standard",
    capture_bench_backend = "candidate"
))]
compile_error!("capture benchmark cannot compile standard and candidate backends together");

#[cfg(not(any(
    capture_bench_backend = "standard",
    capture_bench_backend = "candidate"
)))]
compile_error!("capture benchmark requires exactly one compile-time backend");

#[cfg(capture_bench_backend = "candidate")]
#[path = "backend/candidate.rs"]
mod selected;
#[cfg(capture_bench_backend = "standard")]
#[path = "backend/standard.rs"]
mod selected;

pub(crate) use selected::*;
