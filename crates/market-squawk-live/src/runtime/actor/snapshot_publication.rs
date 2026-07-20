//! Immutable bounded shard snapshot construction and publication.

use std::num::NonZeroU64;

use super::super::system_timestamp;
use super::{ActorError, ShardActor};
use crate::processor::{ProcessorSnapshotLimits, ProcessorSnapshotSeed};
use crate::snapshot::SnapshotBuildError;
use crate::{
    LiveFeatureSnapshot, RouteSnapshot, ShardLifecycleSnapshot, ShardSnapshot, SnapshotDimension,
};

impl ShardActor {
    pub(super) fn publish_snapshot(
        &mut self,
        lifecycle: ShardLifecycleSnapshot,
    ) -> Result<(), ActorError> {
        let next = self
            .snapshot_revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(SnapshotBuildError::RevisionExhausted)?;
        let evaluated_at = system_timestamp().map_err(|_| SnapshotBuildError::ClockRange)?;
        let mut route_keys = self.routes.keys().cloned().collect::<Vec<_>>();
        route_keys.sort_by(|left, right| {
            left.venue()
                .as_str()
                .cmp(right.venue().as_str())
                .then_with(|| left.instrument().cmp(&right.instrument()))
        });
        let available_routes = route_keys.len();
        let route_limit = self.snapshot_limits.maximum_routes().get();
        let mut routes = Vec::new();
        routes
            .try_reserve(route_limit.min(available_routes))
            .map_err(|_| ActorError::Allocation)?;
        let mut retained_bytes = std::mem::size_of::<ShardSnapshot>();
        for key in route_keys.into_iter().take(route_limit) {
            let remaining = usize::try_from(self.snapshot_limits.maximum_retained_bytes().get())
                .map_err(|_| SnapshotBuildError::RetainedSizeOverflow)?
                .checked_sub(retained_bytes)
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            let route_charge = std::mem::size_of::<RouteSnapshot>()
                .checked_add(key.venue().retained_bytes())
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            let minimum = std::mem::size_of::<ProcessorSnapshotSeed>()
                .checked_add(route_charge)
                .and_then(|value| value.checked_add(std::mem::size_of::<LiveFeatureSnapshot>()))
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            if remaining < minimum {
                break;
            }
            let owner = self.routes.get(&key).ok_or(ActorError::UnknownRoute)?;
            let processor_budget = remaining
                .checked_sub(route_charge)
                .and_then(|value| value.checked_sub(std::mem::size_of::<LiveFeatureSnapshot>()))
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            let seed = owner
                .processor
                .snapshot_seed(ProcessorSnapshotLimits::try_new(
                    self.snapshot_limits.maximum_streams_per_route().get(),
                    self.snapshot_limits.maximum_statuses_per_route().get(),
                    self.snapshot_limits.maximum_levels_per_side().get() as usize,
                    processor_budget,
                )?)?;
            let feature_budget = remaining
                .checked_sub(route_charge)
                .and_then(|value| value.checked_sub(seed.retained_bytes))
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?
                .min(self.maximum_feature_snapshot_bytes);
            let features = owner.features.build_snapshot(feature_budget)?;
            let candidate_retained_bytes = retained_bytes
                .checked_add(seed.retained_bytes)
                .and_then(|value| value.checked_add(route_charge))
                .and_then(|value| {
                    usize::try_from(features.retained_bytes())
                        .ok()
                        .and_then(|feature_bytes| value.checked_add(feature_bytes))
                })
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            if candidate_retained_bytes
                > self.snapshot_limits.maximum_retained_bytes().get() as usize
            {
                break;
            }
            retained_bytes = candidate_retained_bytes;
            routes.push(seed.into_route(key, features));
        }
        let route_dimension =
            SnapshotDimension::from_counts(available_routes, routes.len(), route_limit)?;
        let published_at = system_timestamp().map_err(|_| SnapshotBuildError::ClockRange)?;
        let snapshot = ShardSnapshot {
            routing_version: self.routing_version,
            shard_count: self.shard.count(),
            runtime_incarnation: self.runtime_incarnation,
            shard_id: self.shard,
            snapshot_revision: next,
            health_revision: self.health_revision,
            lifecycle,
            evaluated_at,
            published_at,
            routes: routes.into_boxed_slice(),
            route_dimension,
            retained_bytes: u64::try_from(retained_bytes)
                .map_err(|_| SnapshotBuildError::RetainedSizeOverflow)?,
        };
        self.publisher.publish(snapshot)?;
        let notification_drops = self.publisher.dropped_notifications();
        if notification_drops > self.observed_notification_drops {
            self.observed_notification_drops = notification_drops;
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(
                crate::runtime::LiveRuntimeHealthKind::SnapshotNotificationDropped,
                None,
            );
        }
        self.snapshot_revision = next;
        self.events_since_snapshot = 0;
        self.dirty = false;
        Ok(())
    }
}
