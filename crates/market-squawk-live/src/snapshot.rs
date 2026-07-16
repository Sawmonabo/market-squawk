//! Authority-free immutable live snapshot contracts and bounded reader configuration.

use std::num::{NonZeroU32, NonZeroUsize};

use thiserror::Error;

/// Hard bound aligned with one shard's preallocated route table.
pub(crate) const MAX_SNAPSHOT_ROUTES: usize = 64;
/// Hard bound aligned with Task 7 stream/status capacity per route.
pub(crate) const MAX_SNAPSHOT_STREAMS_PER_ROUTE: usize = 64;
/// Hard bound aligned with Task 7 stream/status capacity per route.
pub(crate) const MAX_SNAPSHOT_STATUSES_PER_ROUTE: usize = 64;
/// Hard bound aligned with the live book and decoder depth contract.
pub(crate) const MAX_SNAPSHOT_LEVELS_PER_SIDE: u32 = 10_000;
/// Hard upper bound for one immutable shard snapshot.
pub(crate) const MAX_SNAPSHOT_RETAINED_BYTES: u32 = 64 * 1024 * 1024;

/// Caller-selected snapshot output bounds validated before actor construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotLimits {
    maximum_routes: NonZeroUsize,
    maximum_streams_per_route: NonZeroUsize,
    maximum_statuses_per_route: NonZeroUsize,
    maximum_levels_per_side: NonZeroU32,
    maximum_retained_bytes: NonZeroU32,
}

impl SnapshotLimits {
    /// Constructs locally bounded snapshot dimensions.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above the corresponding live-plane hard limit.
    pub fn try_new(
        maximum_routes: usize,
        maximum_streams_per_route: usize,
        maximum_statuses_per_route: usize,
        maximum_levels_per_side: u32,
        maximum_retained_bytes: u32,
    ) -> Result<Self, SnapshotLimitsError> {
        Ok(Self {
            maximum_routes: checked_usize("maximum_routes", maximum_routes, MAX_SNAPSHOT_ROUTES)?,
            maximum_streams_per_route: checked_usize(
                "maximum_streams_per_route",
                maximum_streams_per_route,
                MAX_SNAPSHOT_STREAMS_PER_ROUTE,
            )?,
            maximum_statuses_per_route: checked_usize(
                "maximum_statuses_per_route",
                maximum_statuses_per_route,
                MAX_SNAPSHOT_STATUSES_PER_ROUTE,
            )?,
            maximum_levels_per_side: checked_u32(
                "maximum_levels_per_side",
                maximum_levels_per_side,
                MAX_SNAPSHOT_LEVELS_PER_SIDE,
            )?,
            maximum_retained_bytes: checked_u32(
                "maximum_retained_bytes",
                maximum_retained_bytes,
                MAX_SNAPSHOT_RETAINED_BYTES,
            )?,
        })
    }

    pub const fn maximum_routes(self) -> NonZeroUsize {
        self.maximum_routes
    }
    pub const fn maximum_streams_per_route(self) -> NonZeroUsize {
        self.maximum_streams_per_route
    }
    pub const fn maximum_statuses_per_route(self) -> NonZeroUsize {
        self.maximum_statuses_per_route
    }
    pub const fn maximum_levels_per_side(self) -> NonZeroU32 {
        self.maximum_levels_per_side
    }
    pub const fn maximum_retained_bytes(self) -> NonZeroU32 {
        self.maximum_retained_bytes
    }
}

fn checked_usize(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<NonZeroUsize, SnapshotLimitsError> {
    let value = NonZeroUsize::new(value).ok_or(SnapshotLimitsError::Zero { field })?;
    if value.get() > maximum {
        return Err(SnapshotLimitsError::ExceedsHardLimit {
            field,
            value: value.get() as u64,
            maximum: maximum as u64,
        });
    }
    Ok(value)
}

fn checked_u32(
    field: &'static str,
    value: u32,
    maximum: u32,
) -> Result<NonZeroU32, SnapshotLimitsError> {
    let value = NonZeroU32::new(value).ok_or(SnapshotLimitsError::Zero { field })?;
    if value.get() > maximum {
        return Err(SnapshotLimitsError::ExceedsHardLimit {
            field,
            value: u64::from(value.get()),
            maximum: u64::from(maximum),
        });
    }
    Ok(value)
}

/// Invalid public snapshot output bounds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SnapshotLimitsError {
    #[error("snapshot limit {field} must be nonzero")]
    Zero { field: &'static str },
    #[error("snapshot limit {field} value {value} exceeds hard maximum {maximum}")]
    ExceedsHardLimit {
        field: &'static str,
        value: u64,
        maximum: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::SnapshotLimits;

    #[test]
    fn limits_are_nonzero_and_locally_bounded() {
        assert!(SnapshotLimits::try_new(1, 1, 1, 1, 1).is_ok());
        assert!(SnapshotLimits::try_new(0, 1, 1, 1, 1).is_err());
        assert!(SnapshotLimits::try_new(1, 0, 1, 1, 1).is_err());
        assert!(SnapshotLimits::try_new(1, 1, 0, 1, 1).is_err());
        assert!(SnapshotLimits::try_new(1, 1, 1, 0, 1).is_err());
        assert!(SnapshotLimits::try_new(1, 1, 1, 1, 0).is_err());
    }
}
