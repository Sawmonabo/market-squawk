//! Transport-neutral durable job lifecycle contracts.
//!
//! Jobs own lifecycle, progress, cancellation, and recovery. Domain services remain the immutable
//! authority for financial and analytical results; jobs retain only typed result references.

mod authority;
mod contracts;
mod process;
mod repository;
mod scheduler;

pub use authority::*;
pub use contracts::*;
pub use process::*;
pub use repository::*;
pub use scheduler::*;

#[cfg(test)]
mod tests;
