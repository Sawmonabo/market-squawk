//! Checked conservative peak-memory model for one complete runtime incarnation.

use std::mem::size_of;
use std::num::NonZeroU64;

use market_squawk_domain::BookLevel;

use super::{LiveRouteConfig, LiveRuntimeConfig, LiveRuntimeConfigError};

const ROUTE_FIXED_BYTES: u64 = 32 * 1024;
const SOURCE_STREAM_BYTES: u64 = 8 * 1024;
const NONCE_SLOT_BYTES: u64 = 192;
const BOOK_LEVEL_BYTES: u64 = size_of::<BookLevel>() as u64 + 64;
const ACTOR_FIXED_BYTES: u64 = 64 * 1024;
const CHANNEL_COMMAND_SLOT_BYTES: u64 = 128;
const CONTROL_SLOT_BYTES: u64 = 256;
const HEALTH_EVENT_BYTES: u64 = 512;
const SNAPSHOT_NOTIFICATION_BYTES: u64 = 256;

/// Conservative checked peak retained bytes for every configured runtime component.
pub(super) fn estimate_peak_bytes(
    config: &LiveRuntimeConfig,
    routes: &[LiveRouteConfig],
) -> Result<NonZeroU64, LiveRuntimeConfigError> {
    config.validate_routes(routes)?;
    let shards = u64::from(config.shard_count().get());
    let mut total = 0_u64;

    for route in routes {
        total = add(total, ROUTE_FIXED_BYTES)?;
        total = add(
            total,
            multiply(route.nonce_capacity().get() as u64, NONCE_SLOT_BYTES)?,
        )?;
        total = add(
            total,
            multiply(
                config.maximum_sources_per_route().get() as u64,
                SOURCE_STREAM_BYTES,
            )?,
        )?;
        let levels = multiply(route.depth().get() as u64, 2)?;
        total = add(total, multiply(levels, BOOK_LEVEL_BYTES)?)?;
    }

    let mailbox_per_shard = add(
        u64::from(config.mailbox_bytes_per_shard().get()),
        multiply(
            config.mailbox_count_per_shard().get() as u64,
            CHANNEL_COMMAND_SLOT_BYTES,
        )?,
    )?;
    total = add(total, multiply(shards, mailbox_per_shard)?)?;

    // A candidate plus its rollback journal may coexist with all bytes admitted by the semaphore.
    let candidate_and_rollback = multiply(u64::from(config.maximum_message_bytes().get()), 2)?;
    total = add(total, multiply(shards, candidate_and_rollback)?)?;

    let control_per_shard = multiply(
        config.registration_control_capacity().get() as u64,
        CONTROL_SLOT_BYTES,
    )?;
    total = add(total, multiply(shards, control_per_shard)?)?;

    let snapshot_bytes = u64::from(config.snapshot_limits().maximum_retained_bytes().get());
    // One under construction and one currently published per actor.
    total = add(total, multiply(multiply(shards, snapshot_bytes)?, 2)?)?;
    // The official retained-reader budget is runtime-wide, not multiplied by shard count.
    total = add(
        total,
        multiply(
            u64::from(config.maximum_retained_snapshot_readers().get()),
            snapshot_bytes,
        )?,
    )?;

    total = add(
        total,
        multiply(
            config.health_event_capacity().get() as u64,
            HEALTH_EVENT_BYTES,
        )?,
    )?;
    total = add(
        total,
        multiply(shards, ACTOR_FIXED_BYTES + SNAPSHOT_NOTIFICATION_BYTES)?,
    )?;

    if total > config.maximum_runtime_bytes().get() {
        return Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated: total,
            ceiling: config.maximum_runtime_bytes().get(),
        });
    }
    NonZeroU64::new(total).ok_or(LiveRuntimeConfigError::CapacityOverflow)
}

fn add(left: u64, right: u64) -> Result<u64, LiveRuntimeConfigError> {
    left.checked_add(right)
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)
}

fn multiply(left: u64, right: u64) -> Result<u64, LiveRuntimeConfigError> {
    left.checked_mul(right)
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)
}
