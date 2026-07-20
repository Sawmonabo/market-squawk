//! One bounded venue/instrument route publication.

use serde::Serialize;

use super::{LiveFeatureSnapshot, SnapshotDimension, StatusSnapshot, StreamSnapshot};
use crate::ShardKey;

/// Bounded state for one venue/instrument owner.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSnapshot {
    pub(crate) route: ShardKey,
    pub(crate) streams: Box<[StreamSnapshot]>,
    pub(crate) statuses: Box<[StatusSnapshot]>,
    pub(crate) stream_dimension: SnapshotDimension,
    pub(crate) status_dimension: SnapshotDimension,
    pub(crate) features: LiveFeatureSnapshot,
}

impl RouteSnapshot {
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    pub fn streams(&self) -> &[StreamSnapshot] {
        &self.streams
    }

    pub fn statuses(&self) -> &[StatusSnapshot] {
        &self.statuses
    }

    pub const fn stream_dimension(&self) -> &SnapshotDimension {
        &self.stream_dimension
    }

    pub const fn status_dimension(&self) -> &SnapshotDimension {
        &self.status_dimension
    }

    pub const fn features(&self) -> &LiveFeatureSnapshot {
        &self.features
    }
}
