//! Checked capacities for route and cross-venue live feature ownership.

use std::num::{NonZeroU32, NonZeroUsize};

use market_squawk_analytics::{
    MAX_CROSS_VENUE_OBSERVATIONS, MAX_ROLLING_OBSERVATIONS, MAX_ROLLING_RETAINED_BYTES,
};

use super::{LiveRuntimeConfigError, LiveRuntimeConfigInput, checked_u32, checked_usize};
use crate::processor::MAX_STREAMS_PER_INSTRUMENT;

const MAX_FEATURE_SETS_PER_ROUTE: usize = MAX_STREAMS_PER_INSTRUMENT;
const MAX_CROSS_VENUE_COMMANDS: usize = 65_536;
const MAX_CROSS_VENUE_COMMAND_BYTES: u32 = 64 * 1024 * 1024;
const MAX_CROSS_VENUE_INSTRUMENTS: usize = 64 * 64;
const MAX_FEATURE_SNAPSHOT_BYTES: u32 = 64 * 1024 * 1024;
const MAX_ACTION_HOOK_BYTES_PER_ROUTE: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveFeatureCapacity {
    pub(crate) maximum_feature_window_observations_per_route: NonZeroUsize,
    pub(crate) maximum_feature_window_bytes_per_route: NonZeroUsize,
    pub(crate) maximum_feature_sets_per_route: NonZeroUsize,
    pub(crate) cross_venue_command_count: NonZeroUsize,
    pub(crate) cross_venue_command_bytes: NonZeroU32,
    pub(crate) maximum_cross_venue_instruments: NonZeroUsize,
    pub(crate) maximum_venues_per_cross_venue_instrument: NonZeroUsize,
    pub(crate) maximum_feature_snapshot_bytes: NonZeroU32,
    pub(crate) maximum_action_hook_bytes_per_route: NonZeroUsize,
}

impl LiveFeatureCapacity {
    pub(super) fn try_new(input: &LiveRuntimeConfigInput) -> Result<Self, LiveRuntimeConfigError> {
        let maximum_feature_window_observations_per_route = checked_usize(
            "maximum_feature_window_observations_per_route",
            input.maximum_feature_window_observations_per_route,
            MAX_ROLLING_OBSERVATIONS,
        )?;
        let maximum_feature_window_bytes_per_route = checked_usize(
            "maximum_feature_window_bytes_per_route",
            input.maximum_feature_window_bytes_per_route,
            MAX_ROLLING_RETAINED_BYTES,
        )?;
        let maximum_feature_sets_per_route = checked_usize(
            "maximum_feature_sets_per_route",
            input.maximum_feature_sets_per_route,
            MAX_FEATURE_SETS_PER_ROUTE,
        )?;
        let minimum_window_bytes = minimum_window_bytes(
            maximum_feature_window_observations_per_route.get(),
            maximum_feature_sets_per_route.get(),
            0,
        )
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
        if maximum_feature_window_bytes_per_route.get() < minimum_window_bytes {
            return Err(
                LiveRuntimeConfigError::FeatureWindowBytesBelowRetainedState {
                    bytes: maximum_feature_window_bytes_per_route.get(),
                    minimum: minimum_window_bytes,
                },
            );
        }
        let maximum_venues_per_cross_venue_instrument = checked_usize(
            "maximum_venues_per_cross_venue_instrument",
            input.maximum_venues_per_cross_venue_instrument,
            MAX_CROSS_VENUE_OBSERVATIONS,
        )?;
        if maximum_venues_per_cross_venue_instrument.get() < 2 {
            return Err(LiveRuntimeConfigError::CrossVenueRequiresTwoVenues);
        }
        let cross_venue_command_bytes = checked_u32(
            "cross_venue_command_bytes",
            input.cross_venue_command_bytes,
            MAX_CROSS_VENUE_COMMAND_BYTES,
        )?;
        if usize::try_from(cross_venue_command_bytes.get())
            .map_err(|_| LiveRuntimeConfigError::CapacityOverflow)?
            < crate::cross_venue::runtime_command_bytes()
        {
            return Err(LiveRuntimeConfigError::CrossVenueCommandBytesBelowOne {
                bytes: cross_venue_command_bytes.get(),
                minimum: crate::cross_venue::runtime_command_bytes(),
            });
        }
        Ok(Self {
            maximum_feature_window_observations_per_route,
            maximum_feature_window_bytes_per_route,
            maximum_feature_sets_per_route,
            cross_venue_command_count: checked_usize(
                "cross_venue_command_count",
                input.cross_venue_command_count,
                MAX_CROSS_VENUE_COMMANDS,
            )?,
            cross_venue_command_bytes,
            maximum_cross_venue_instruments: checked_usize(
                "maximum_cross_venue_instruments",
                input.maximum_cross_venue_instruments,
                MAX_CROSS_VENUE_INSTRUMENTS,
            )?,
            maximum_venues_per_cross_venue_instrument,
            maximum_feature_snapshot_bytes: checked_u32(
                "maximum_feature_snapshot_bytes",
                input.maximum_feature_snapshot_bytes,
                MAX_FEATURE_SNAPSHOT_BYTES,
            )?,
            maximum_action_hook_bytes_per_route: checked_usize(
                "maximum_action_hook_bytes_per_route",
                input.maximum_action_hook_bytes_per_route,
                MAX_ACTION_HOOK_BYTES_PER_ROUTE,
            )?,
        })
    }

    pub(crate) fn minimum_window_bytes(self, depth: usize) -> Option<usize> {
        minimum_window_bytes(
            self.maximum_feature_window_observations_per_route.get(),
            self.maximum_feature_sets_per_route.get(),
            depth,
        )
    }
}

fn minimum_window_bytes(observations: usize, sets: usize, depth: usize) -> Option<usize> {
    let observation_bytes =
        observations.checked_mul(2_usize.checked_mul(std::mem::size_of::<
            Option<market_squawk_analytics::TradeFeatureView>,
        >())?)?;
    let depth_bytes = depth.checked_mul(
        2_usize
            .checked_mul(std::mem::size_of::<market_squawk_domain::BookLevel>())?
            .checked_add(
                2_usize
                    .checked_mul(std::mem::size_of::<market_squawk_analytics::PriceLevelView>())?,
            )?,
    )?;
    observation_bytes
        .checked_add(depth_bytes)?
        .checked_mul(sets)
}
