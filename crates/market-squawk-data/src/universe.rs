//! Point-in-time historical-universe construction from immutable membership evidence.

mod build;
mod canonical;
mod derivatives;
mod model;
mod retained;

pub use derivatives::{
    ContractRollEvidence, DerivativeBoundary, DerivativeCivilDate, DerivativeLifecycle,
    DerivativeLifecycleEvidence, DerivativeSelectionDecision, DerivativeUniverseSnapshot,
};
pub use model::{
    MAX_UNIVERSE_CANDIDATES, MAX_UNIVERSE_RETAINED_BYTES, UniverseConflictCounts,
    UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
