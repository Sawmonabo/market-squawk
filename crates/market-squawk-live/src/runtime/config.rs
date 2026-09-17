//! Checked runtime capacity and route ownership configuration.

use std::collections::HashSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::time::Duration;

use market_squawk_domain::InstrumentDefinition;
use market_squawk_sources::MAX_DECODED_EVENTS;
use thiserror::Error;

use crate::processor::MAX_STREAMS_PER_INSTRUMENT;
use crate::{
    DepthLimit, ShardCount, ShardKey, ShardRouter, ShardRoutingVersion, SnapshotLimits,
    SnapshotLimitsError,
};

const MAX_RUNTIME_SHARDS: usize = 64;
const MAX_MAILBOX_COMMANDS_PER_SHARD: usize = 1_000_000;
const MAX_ROUTES_PER_SHARD: usize = 64;
const MAX_SOURCES_PER_ROUTE: usize = 64;
const MAX_CONTROL_COMMANDS_PER_SHARD: usize = 65_536;
const MAX_HEALTH_EVENTS: usize = 65_536;
const MAX_SNAPSHOT_EVENT_TRIGGER: usize = 1_000_000;
const MAX_NONCE_CAPACITY: usize = 1_000_000;

#[path = "config/features.rs"]
mod features;

pub(crate) use features::LiveFeatureCapacity;

/// Maximum observations by which successful-batch-end publication can pass its trigger.
///
/// A decoded provider batch contains at most [`MAX_DECODED_EVENTS`] observations. Because the
/// scheduler does not publish an intermediate prefix of a successfully applied batch, a trigger
/// reached by the first observation in a maximum-size batch can be exceeded by every remaining
/// observation.
pub const MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT: usize = MAX_DECODED_EVENTS - 1;

/// Primitive configuration input checked into [`LiveRuntimeConfig`].
#[derive(Clone, Debug)]
pub struct LiveRuntimeConfigInput {
    pub routing_version: ShardRoutingVersion,
    pub shard_count: u16,
    pub mailbox_count_per_shard: usize,
    pub mailbox_bytes_per_shard: u32,
    pub maximum_message_bytes: u32,
    pub maximum_routes_per_shard: usize,
    pub maximum_sources_per_route: usize,
    /// Maximum independently keyed source/product/channel streams retained by one route owner.
    pub maximum_streams_per_route: usize,
    pub maximum_feature_window_observations_per_route: usize,
    pub maximum_feature_window_bytes_per_route: usize,
    pub maximum_feature_sets_per_route: usize,
    pub cross_venue_command_count: usize,
    pub cross_venue_command_bytes: u32,
    pub maximum_cross_venue_instruments: usize,
    pub maximum_venues_per_cross_venue_instrument: usize,
    pub maximum_feature_snapshot_bytes: u32,
    pub maximum_action_hook_bytes_per_route: usize,
    pub registration_control_capacity: usize,
    pub registration_deadline: Duration,
    pub health_event_capacity: usize,
    /// Accepted-observation count that triggers publication at successful current-batch end.
    ///
    /// This is a batch-end trigger, not an exact per-observation cadence. Publication occurs once
    /// the cumulative count reaches or exceeds this value and the current provider batch finishes
    /// successfully. The scheduler does not expose an intermediate prefix of that successful
    /// batch. The maximum overshoot is [`MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT`].
    pub snapshot_event_trigger: usize,
    pub snapshot_interval: Duration,
    pub snapshot_limits: SnapshotLimits,
    pub maximum_retained_snapshot_readers: u32,
    pub shutdown_deadline: Duration,
    /// Explicit configured ceiling for the checked conservative peak model.
    pub maximum_runtime_bytes: u64,
}

/// Fully checked runtime-wide capacity and lifecycle policy.
#[derive(Clone, Debug)]
pub struct LiveRuntimeConfig {
    routing_version: ShardRoutingVersion,
    shard_count: ShardCount,
    mailbox_count_per_shard: NonZeroUsize,
    mailbox_bytes_per_shard: NonZeroU32,
    maximum_message_bytes: NonZeroU32,
    maximum_routes_per_shard: NonZeroUsize,
    maximum_sources_per_route: NonZeroUsize,
    maximum_streams_per_route: NonZeroUsize,
    feature_capacity: LiveFeatureCapacity,
    registration_control_capacity: NonZeroUsize,
    registration_deadline: Duration,
    health_event_capacity: NonZeroUsize,
    snapshot_event_trigger: NonZeroUsize,
    snapshot_interval: Duration,
    snapshot_limits: SnapshotLimits,
    maximum_retained_snapshot_readers: NonZeroU32,
    shutdown_deadline: Duration,
    maximum_runtime_bytes: NonZeroU64,
}

impl LiveRuntimeConfig {
    /// Validates all runtime-wide primitives before any allocation or actor spawn.
    pub fn try_new(input: LiveRuntimeConfigInput) -> Result<Self, LiveRuntimeConfigError> {
        let shard_count = ShardCount::new(input.shard_count)?;
        checked_usize(
            "shard_count",
            usize::from(shard_count.get()),
            MAX_RUNTIME_SHARDS,
        )?;
        let mailbox_count_per_shard = checked_usize(
            "mailbox_count_per_shard",
            input.mailbox_count_per_shard,
            MAX_MAILBOX_COMMANDS_PER_SHARD,
        )?;
        let mailbox_bytes_per_shard =
            checked_permit_capacity("mailbox_bytes_per_shard", input.mailbox_bytes_per_shard)?;
        let maximum_message_bytes =
            checked_permit_capacity("maximum_message_bytes", input.maximum_message_bytes)?;
        if maximum_message_bytes > mailbox_bytes_per_shard {
            return Err(LiveRuntimeConfigError::MessageExceedsMailbox {
                message: maximum_message_bytes.get(),
                mailbox: mailbox_bytes_per_shard.get(),
            });
        }
        let maximum_retained_snapshot_readers = checked_permit_capacity(
            "maximum_retained_snapshot_readers",
            input.maximum_retained_snapshot_readers,
        )?;
        if maximum_retained_snapshot_readers.get() < u32::from(shard_count.get()) {
            return Err(LiveRuntimeConfigError::SnapshotReadersBelowShardCount {
                readers: maximum_retained_snapshot_readers.get(),
                shards: shard_count.get(),
            });
        }
        let maximum_sources_per_route = checked_usize(
            "maximum_sources_per_route",
            input.maximum_sources_per_route,
            MAX_SOURCES_PER_ROUTE,
        )?;
        let maximum_streams_per_route = checked_usize(
            "maximum_streams_per_route",
            input.maximum_streams_per_route,
            MAX_STREAMS_PER_INSTRUMENT,
        )?;
        if maximum_sources_per_route > maximum_streams_per_route {
            return Err(LiveRuntimeConfigError::SourcesExceedStreams {
                sources: maximum_sources_per_route.get(),
                streams: maximum_streams_per_route.get(),
            });
        }
        let feature_capacity = LiveFeatureCapacity::try_new(&input)?;
        let config = Self {
            routing_version: input.routing_version,
            shard_count,
            mailbox_count_per_shard,
            mailbox_bytes_per_shard,
            maximum_message_bytes,
            maximum_routes_per_shard: checked_usize(
                "maximum_routes_per_shard",
                input.maximum_routes_per_shard,
                MAX_ROUTES_PER_SHARD,
            )?,
            maximum_sources_per_route,
            maximum_streams_per_route,
            feature_capacity,
            registration_control_capacity: checked_usize(
                "registration_control_capacity",
                input.registration_control_capacity,
                MAX_CONTROL_COMMANDS_PER_SHARD,
            )?,
            registration_deadline: checked_duration(
                "registration_deadline",
                input.registration_deadline,
            )?,
            health_event_capacity: checked_usize(
                "health_event_capacity",
                input.health_event_capacity,
                MAX_HEALTH_EVENTS,
            )?,
            snapshot_event_trigger: checked_usize(
                "snapshot_event_trigger",
                input.snapshot_event_trigger,
                MAX_SNAPSHOT_EVENT_TRIGGER,
            )?,
            snapshot_interval: checked_duration("snapshot_interval", input.snapshot_interval)?,
            snapshot_limits: input.snapshot_limits,
            maximum_retained_snapshot_readers,
            shutdown_deadline: checked_duration("shutdown_deadline", input.shutdown_deadline)?,
            maximum_runtime_bytes: NonZeroU64::new(input.maximum_runtime_bytes).ok_or(
                LiveRuntimeConfigError::ZeroCapacity {
                    field: "maximum_runtime_bytes",
                },
            )?,
        };
        Ok(config)
    }

    /// Validates duplicate ownership, route definitions, and deterministic shard distribution.
    pub fn validate_routes(
        &self,
        routes: &[LiveRouteConfig],
    ) -> Result<(), LiveRuntimeConfigError> {
        let router = ShardRouter::v1(self.shard_count.get())?;
        let mut seen = HashSet::new();
        seen.try_reserve(routes.len())
            .map_err(|_| LiveRuntimeConfigError::Allocation)?;
        let mut counts = vec![0_usize; usize::from(self.shard_count.get())];
        for route in routes {
            let minimum_window_bytes = self
                .feature_capacity
                .minimum_window_bytes(route.depth().get())
                .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
            if self.maximum_feature_window_bytes_per_route().get() < minimum_window_bytes {
                return Err(
                    LiveRuntimeConfigError::FeatureWindowBytesBelowRetainedState {
                        bytes: self.maximum_feature_window_bytes_per_route().get(),
                        minimum: minimum_window_bytes,
                    },
                );
            }
            if !seen.insert(route.route.clone()) {
                return Err(LiveRuntimeConfigError::DuplicateRoute);
            }
            let shard = router.route(&route.route);
            let count = counts
                .get_mut(usize::from(shard.index()))
                .ok_or(LiveRuntimeConfigError::RouteOutsideShardSet)?;
            *count = count
                .checked_add(1)
                .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
            if *count > self.maximum_routes_per_shard.get() {
                return Err(LiveRuntimeConfigError::TooManyRoutesForShard {
                    shard: shard.index(),
                    count: *count,
                    maximum: self.maximum_routes_per_shard.get(),
                });
            }
        }
        Ok(())
    }

    /// Computes the checked conservative peak retained-memory model for these routes.
    ///
    /// # Errors
    ///
    /// Rejects invalid route partitioning, checked arithmetic overflow, or an estimate above the
    /// explicitly configured runtime ceiling.
    pub fn estimated_peak_bytes(
        &self,
        routes: &[LiveRouteConfig],
    ) -> Result<NonZeroU64, LiveRuntimeConfigError> {
        super::memory::estimate_peak_bytes(self, routes)
    }

    pub const fn routing_version(&self) -> ShardRoutingVersion {
        self.routing_version
    }
    pub const fn shard_count(&self) -> ShardCount {
        self.shard_count
    }
    pub const fn mailbox_count_per_shard(&self) -> NonZeroUsize {
        self.mailbox_count_per_shard
    }
    pub const fn mailbox_bytes_per_shard(&self) -> NonZeroU32 {
        self.mailbox_bytes_per_shard
    }
    pub const fn maximum_message_bytes(&self) -> NonZeroU32 {
        self.maximum_message_bytes
    }
    pub const fn maximum_routes_per_shard(&self) -> NonZeroUsize {
        self.maximum_routes_per_shard
    }
    pub const fn maximum_sources_per_route(&self) -> NonZeroUsize {
        self.maximum_sources_per_route
    }
    pub const fn maximum_streams_per_route(&self) -> NonZeroUsize {
        self.maximum_streams_per_route
    }
    pub(crate) const fn feature_capacity(&self) -> LiveFeatureCapacity {
        self.feature_capacity
    }
    pub const fn maximum_feature_window_observations_per_route(&self) -> NonZeroUsize {
        self.feature_capacity
            .maximum_feature_window_observations_per_route
    }
    pub const fn maximum_feature_window_bytes_per_route(&self) -> NonZeroUsize {
        self.feature_capacity.maximum_feature_window_bytes_per_route
    }
    pub const fn maximum_feature_sets_per_route(&self) -> NonZeroUsize {
        self.feature_capacity.maximum_feature_sets_per_route
    }
    pub const fn cross_venue_command_count(&self) -> NonZeroUsize {
        self.feature_capacity.cross_venue_command_count
    }
    pub const fn cross_venue_command_bytes(&self) -> NonZeroU32 {
        self.feature_capacity.cross_venue_command_bytes
    }
    pub const fn maximum_cross_venue_instruments(&self) -> NonZeroUsize {
        self.feature_capacity.maximum_cross_venue_instruments
    }
    pub const fn maximum_venues_per_cross_venue_instrument(&self) -> NonZeroUsize {
        self.feature_capacity
            .maximum_venues_per_cross_venue_instrument
    }
    pub const fn maximum_feature_snapshot_bytes(&self) -> NonZeroU32 {
        self.feature_capacity.maximum_feature_snapshot_bytes
    }
    /// Maximum bytes retained by an optional action hook for one route.
    ///
    /// Zero is valid for market-data-only runtimes that do not install action hooks.
    pub const fn maximum_action_hook_bytes_per_route(&self) -> usize {
        self.feature_capacity.maximum_action_hook_bytes_per_route
    }
    pub const fn registration_control_capacity(&self) -> NonZeroUsize {
        self.registration_control_capacity
    }
    pub const fn registration_deadline(&self) -> Duration {
        self.registration_deadline
    }
    pub const fn health_event_capacity(&self) -> NonZeroUsize {
        self.health_event_capacity
    }
    /// Returns the accepted-observation trigger for batch-end snapshot publication.
    ///
    /// The scheduler does not publish an intermediate prefix of a successfully applied provider
    /// batch. Publication therefore occurs at successful batch end after that batch reaches or
    /// crosses this trigger, with at most [`MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT`] additional
    /// observations.
    pub const fn snapshot_event_trigger(&self) -> NonZeroUsize {
        self.snapshot_event_trigger
    }
    pub const fn snapshot_interval(&self) -> Duration {
        self.snapshot_interval
    }
    pub const fn snapshot_limits(&self) -> SnapshotLimits {
        self.snapshot_limits
    }
    pub const fn maximum_retained_snapshot_readers(&self) -> NonZeroU32 {
        self.maximum_retained_snapshot_readers
    }
    pub const fn shutdown_deadline(&self) -> Duration {
        self.shutdown_deadline
    }
    pub const fn maximum_runtime_bytes(&self) -> NonZeroU64 {
        self.maximum_runtime_bytes
    }
}

/// Fully validated live-feature ownership capacities forwarded to shard actors as one unit.
/// Primitive route input checked into [`LiveRouteConfig`].
#[derive(Clone, Debug)]
pub struct LiveRouteConfigInput {
    pub route: ShardKey,
    pub definition: InstrumentDefinition,
    pub depth: DepthLimit,
    pub nonce_capacity: usize,
    pub nonce_reclaim_budget: usize,
    pub maximum_capability_lifetime: Duration,
}

/// One exact venue/instrument route and its preallocated processor bounds.
#[derive(Clone, Debug)]
pub struct LiveRouteConfig {
    route: ShardKey,
    definition: InstrumentDefinition,
    depth: DepthLimit,
    nonce_capacity: NonZeroUsize,
    nonce_reclaim_budget: NonZeroUsize,
    maximum_capability_lifetime: Duration,
}

impl LiveRouteConfig {
    /// Validates route/reference-master identity and processor bounds.
    pub fn try_new(input: LiveRouteConfigInput) -> Result<Self, LiveRuntimeConfigError> {
        if input.route.instrument() != input.definition.instrument_id() {
            return Err(LiveRuntimeConfigError::RouteInstrumentMismatch);
        }
        if !input
            .definition
            .venue_mappings()
            .iter()
            .any(|mapping| mapping.venue_id() == input.route.venue())
        {
            return Err(LiveRuntimeConfigError::RouteVenueMismatch);
        }
        Ok(Self {
            route: input.route,
            definition: input.definition,
            depth: input.depth,
            nonce_capacity: checked_usize(
                "nonce_capacity",
                input.nonce_capacity,
                MAX_NONCE_CAPACITY,
            )?,
            nonce_reclaim_budget: checked_usize(
                "nonce_reclaim_budget",
                input.nonce_reclaim_budget,
                MAX_NONCE_CAPACITY,
            )?,
            maximum_capability_lifetime: checked_duration(
                "maximum_capability_lifetime",
                input.maximum_capability_lifetime,
            )?,
        })
    }

    pub const fn route(&self) -> &ShardKey {
        &self.route
    }
    pub const fn definition(&self) -> &InstrumentDefinition {
        &self.definition
    }
    pub const fn depth(&self) -> DepthLimit {
        self.depth
    }
    pub const fn nonce_capacity(&self) -> NonZeroUsize {
        self.nonce_capacity
    }
    pub const fn nonce_reclaim_budget(&self) -> NonZeroUsize {
        self.nonce_reclaim_budget
    }
    pub const fn maximum_capability_lifetime(&self) -> Duration {
        self.maximum_capability_lifetime
    }
}

fn checked_usize(
    field: &'static str,
    value: usize,
    maximum: usize,
) -> Result<NonZeroUsize, LiveRuntimeConfigError> {
    let value = NonZeroUsize::new(value).ok_or(LiveRuntimeConfigError::ZeroCapacity { field })?;
    if value.get() > maximum {
        return Err(LiveRuntimeConfigError::CapacityExceedsHardLimit {
            field,
            value: value.get() as u64,
            maximum: maximum as u64,
        });
    }
    Ok(value)
}

fn checked_permit_capacity(
    field: &'static str,
    value: u32,
) -> Result<NonZeroU32, LiveRuntimeConfigError> {
    let value = NonZeroU32::new(value).ok_or(LiveRuntimeConfigError::ZeroCapacity { field })?;
    let permits = usize::try_from(value.get()).map_err(|_| {
        LiveRuntimeConfigError::CapacityExceedsTokioPermitLimit {
            field,
            value: value.get(),
            maximum: tokio::sync::Semaphore::MAX_PERMITS,
        }
    })?;
    if permits > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(LiveRuntimeConfigError::CapacityExceedsTokioPermitLimit {
            field,
            value: value.get(),
            maximum: tokio::sync::Semaphore::MAX_PERMITS,
        });
    }
    Ok(value)
}

fn checked_u32(
    field: &'static str,
    value: u32,
    maximum: u32,
) -> Result<NonZeroU32, LiveRuntimeConfigError> {
    let value = NonZeroU32::new(value).ok_or(LiveRuntimeConfigError::ZeroCapacity { field })?;
    if value.get() > maximum {
        return Err(LiveRuntimeConfigError::CapacityExceedsHardLimit {
            field,
            value: u64::from(value.get()),
            maximum: u64::from(maximum),
        });
    }
    Ok(value)
}

fn checked_duration(
    field: &'static str,
    value: Duration,
) -> Result<Duration, LiveRuntimeConfigError> {
    if value.is_zero() {
        Err(LiveRuntimeConfigError::ZeroDuration { field })
    } else {
        Ok(value)
    }
}

/// Invalid runtime capacity, route, memory, or lifecycle policy.
#[derive(Debug, Error)]
pub enum LiveRuntimeConfigError {
    #[error(transparent)]
    Routing(#[from] crate::ShardRoutingError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotLimitsError),
    #[error("runtime capacity {field} must be nonzero")]
    ZeroCapacity { field: &'static str },
    #[error("runtime duration {field} must be nonzero")]
    ZeroDuration { field: &'static str },
    #[error("runtime capacity {field} value {value} exceeds hard maximum {maximum}")]
    CapacityExceedsHardLimit {
        field: &'static str,
        value: u64,
        maximum: u64,
    },
    #[error("runtime capacity {field} value {value} exceeds Tokio permit maximum {maximum}")]
    CapacityExceedsTokioPermitLimit {
        field: &'static str,
        value: u32,
        maximum: usize,
    },
    #[error("maximum message bytes {message} exceeds mailbox bytes {mailbox}")]
    MessageExceedsMailbox { message: u32, mailbox: u32 },
    #[error("maximum sources per route {sources} exceeds maximum streams per route {streams}")]
    SourcesExceedStreams { sources: usize, streams: usize },
    #[error("feature window bytes {bytes} cannot retain bounded state minimum {minimum}")]
    FeatureWindowBytesBelowRetainedState { bytes: usize, minimum: usize },
    #[error("cross-venue state requires capacity for at least two venues per instrument")]
    CrossVenueRequiresTwoVenues,
    #[error("cross-venue command bytes {bytes} cannot retain one command minimum {minimum}")]
    CrossVenueCommandBytesBelowOne { bytes: u32, minimum: usize },
    #[error("maximum retained snapshot readers {readers} is below configured shard count {shards}")]
    SnapshotReadersBelowShardCount { readers: u32, shards: u16 },
    #[error("route instrument differs from its instrument definition")]
    RouteInstrumentMismatch,
    #[error("route venue is absent from its instrument definition")]
    RouteVenueMismatch,
    #[error("runtime route table contains a duplicate venue/instrument route")]
    DuplicateRoute,
    #[error("route partition resolved outside the configured shard set")]
    RouteOutsideShardSet,
    #[error("shard {shard} owns {count} routes, exceeding maximum {maximum}")]
    TooManyRoutesForShard {
        shard: u16,
        count: usize,
        maximum: usize,
    },
    #[error("runtime capacity arithmetic overflowed")]
    CapacityOverflow,
    #[error("runtime could not reserve bounded route validation state")]
    Allocation,
    #[error("conservative peak runtime bytes {estimated} exceed configured ceiling {ceiling}")]
    PeakMemoryExceedsCeiling { estimated: u64, ceiling: u64 },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{LiveRuntimeConfig, LiveRuntimeConfigInput, ShardRoutingVersion, SnapshotLimits};

    fn valid_input() -> Result<LiveRuntimeConfigInput, Box<dyn std::error::Error>> {
        Ok(LiveRuntimeConfigInput {
            routing_version: ShardRoutingVersion::V1,
            shard_count: 2,
            mailbox_count_per_shard: 64,
            mailbox_bytes_per_shard: 1_048_576,
            maximum_message_bytes: 262_144,
            maximum_routes_per_shard: 8,
            maximum_sources_per_route: 8,
            maximum_streams_per_route: 8,
            maximum_feature_window_observations_per_route: 8,
            maximum_feature_window_bytes_per_route: 1_048_576,
            maximum_feature_sets_per_route: 8,
            cross_venue_command_count: 8,
            cross_venue_command_bytes: 65_536,
            maximum_cross_venue_instruments: 8,
            maximum_venues_per_cross_venue_instrument: 2,
            maximum_feature_snapshot_bytes: 65_536,
            maximum_action_hook_bytes_per_route: 65_536,
            registration_control_capacity: 8,
            registration_deadline: Duration::from_secs(1),
            health_event_capacity: 64,
            snapshot_event_trigger: 128,
            snapshot_interval: Duration::from_millis(100),
            snapshot_limits: SnapshotLimits::try_new(8, 8, 8, 100, 1_048_576)?,
            maximum_retained_snapshot_readers: 4,
            shutdown_deadline: Duration::from_secs(5),
            maximum_runtime_bytes: 256 * 1024 * 1024,
        })
    }

    #[test]
    fn valid_input_preserves_all_capacity_contracts() -> Result<(), Box<dyn std::error::Error>> {
        let config = LiveRuntimeConfig::try_new(valid_input()?)?;
        assert_eq!(config.shard_count().get(), 2);
        assert_eq!(config.mailbox_bytes_per_shard().get(), 1_048_576);
        assert_eq!(config.maximum_message_bytes().get(), 262_144);
        assert_eq!(config.maximum_streams_per_route().get(), 8);
        assert_eq!(config.snapshot_event_trigger().get(), 128);
        assert_eq!(
            super::MAX_SNAPSHOT_EVENT_TRIGGER_OVERSHOOT,
            market_squawk_sources::MAX_DECODED_EVENTS - 1
        );
        assert_eq!(config.maximum_runtime_bytes().get(), 256 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn zero_capacities_and_durations_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = valid_input()?;
        input.shard_count = 0;
        assert!(LiveRuntimeConfig::try_new(input).is_err());

        let mut input = valid_input()?;
        input.registration_deadline = Duration::ZERO;
        assert!(LiveRuntimeConfig::try_new(input).is_err());

        let mut input = valid_input()?;
        input.shutdown_deadline = Duration::ZERO;
        assert!(LiveRuntimeConfig::try_new(input).is_err());
        Ok(())
    }

    #[test]
    fn message_limit_cannot_exceed_mailbox_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = valid_input()?;
        input.maximum_message_bytes = input.mailbox_bytes_per_shard + 1;
        assert!(LiveRuntimeConfig::try_new(input).is_err());
        Ok(())
    }

    #[test]
    fn every_admitted_source_has_capacity_for_at_least_one_stream()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut input = valid_input()?;
        input.maximum_sources_per_route = 3;
        input.maximum_streams_per_route = 2;
        assert!(matches!(
            LiveRuntimeConfig::try_new(input),
            Err(crate::LiveRuntimeConfigError::SourcesExceedStreams {
                sources: 3,
                streams: 2,
            })
        ));
        Ok(())
    }

    #[test]
    fn aggregate_snapshot_reader_budget_covers_every_shard_exactly()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut exact_input = valid_input()?;
        exact_input.maximum_retained_snapshot_readers = 2;
        let exact = LiveRuntimeConfig::try_new(exact_input)?;
        assert_eq!(exact.shard_count().get(), 2);
        assert_eq!(exact.maximum_retained_snapshot_readers().get(), 2);

        let mut below = valid_input()?;
        below.maximum_retained_snapshot_readers = 1;
        assert!(matches!(
            LiveRuntimeConfig::try_new(below),
            Err(
                crate::LiveRuntimeConfigError::SnapshotReadersBelowShardCount {
                    readers: 1,
                    shards: 2,
                }
            )
        ));
        Ok(())
    }
}
