//! Bounded deterministic point-in-time selection over immutable research observations.

use std::time::Instant;

use tokio_util::sync::CancellationToken;

#[path = "pit/canonical.rs"]
mod canonical;
#[path = "pit/model.rs"]
mod model;
#[path = "pit/result.rs"]
mod result;
#[path = "pit/retained.rs"]
mod retained;
#[path = "pit/select.rs"]
mod select;

pub use model::{
    MAX_POINT_IN_TIME_CANDIDATES, MAX_POINT_IN_TIME_CONFLICTS, MAX_POINT_IN_TIME_FAMILIES,
    MAX_POINT_IN_TIME_RESULT_ROWS, MAX_POINT_IN_TIME_RETAINED_BYTES, ObservationFamilyKey,
    POINT_IN_TIME_IDENTITY_SCHEMA_VERSION, PointInTimeCandidate, PointInTimeLimits,
    PointInTimePolicy, PointInTimeRequest, PointInTimeRevisionMode,
};
pub use result::{
    PointInTimeConflict, PointInTimeConflictCounts, PointInTimeConflictReport, PointInTimeError,
    PointInTimeExclusion, PointInTimeExclusionCounts, PointInTimeExclusionReason,
    PointInTimeExclusionReasons, PointInTimeRecord, PointInTimeRevisionCounts,
    PointInTimeRevisionState, PointInTimeSelection,
};

/// Stateless asynchronous point-in-time selection service.
#[derive(Clone, Copy, Debug, Default)]
pub struct PointInTimeService;

impl PointInTimeService {
    /// Constructs the stateless selector service.
    pub const fn new() -> Self {
        Self
    }

    /// Selects immutable observations through the bounded deterministic pure kernel.
    ///
    /// The async boundary yields before CPU work so an already-ready cancellation can win. The
    /// internal borrowed kernel checks cancellation and deadline before work and after at most 64
    /// preparation, comparison, grouping, hashing, or materialization operations.
    pub async fn select<'a>(
        &self,
        request: &PointInTimeRequest,
        candidates: &'a [PointInTimeCandidate],
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<PointInTimeSelection<'a>, PointInTimeError<'a>> {
        tokio::task::yield_now().await;
        select::select(request, candidates, cancellation, deadline)
    }
}
