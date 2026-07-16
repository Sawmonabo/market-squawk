//! Checked conservative peak-memory model for one complete runtime incarnation.

use std::mem::size_of;
use std::num::NonZeroU64;

use market_squawk_domain::{BookLevel, SourceId, SourceIdentifier, VenueId};
use market_squawk_sources::ProviderBookLevel;

use super::{LiveRouteConfig, LiveRuntimeConfig, LiveRuntimeConfigError};

const ROUTE_FIXED_BYTES: u64 = 32 * 1024;
/// Admission and generation-registry ownership per distinct source.
const SOURCE_ADMISSION_BYTES: u64 = 8 * 1024;
const NONCE_SLOT_BYTES: u64 = 192;
/// Allocator and tree-node overhead added to one scaled price/quantity level.
const SCALED_BOOK_LEVEL_BYTES: u64 = size_of::<BookLevel>() as u64 + 64;
/// One exact provider level, two conservatively bounded lexeme allocations, and tree node.
///
/// `SourceIdentifier::MAX_LENGTH` is larger than the decoder's decimal-lexeme cap, so this charge
/// remains conservative without coupling the live crate to a private parser constant.
const EXACT_BOOK_LEVEL_BYTES: u64 =
    size_of::<ProviderBookLevel>() as u64 + (2 * SourceIdentifier::MAX_LENGTH) as u64 + 64;
/// Heap-owned authority cells and allocator slack retained by one stream/status allocation.
const STREAM_AUTHORITY_ALLOCATION_BYTES: u64 = 4 * 1024;
/// Maximum owned text behind source, venue, product, and channel identities in a stream key.
const STREAM_KEY_ALLOCATION_BYTES: u64 =
    (SourceId::MAX_LENGTH + VenueId::MAX_LENGTH + 2 * SourceIdentifier::MAX_LENGTH) as u64;
/// Hash-table node/slack for a stream entry and its status entry.
const STREAM_MAP_ALLOCATION_BYTES: u64 = 2 * 128;
const ACTOR_FIXED_BYTES: u64 = 64 * 1024;
const CHANNEL_COMMAND_SLOT_BYTES: u64 = 128;
const CONTROL_SLOT_BYTES: u64 = 256;
const HEALTH_EVENT_BYTES: u64 = 512;
const SNAPSHOT_NOTIFICATION_BYTES: u64 = 256;
/// Cloned route identity plus Vec/allocator slack retained while one actor sorts route ownership.
const SNAPSHOT_ROUTE_SORT_SCRATCH_BYTES: u64 =
    size_of::<crate::ShardKey>() as u64 + VenueId::MAX_LENGTH as u64 + 64;
/// Two references plus Vec/allocator slack retained while one route sorts stream entries.
const SNAPSHOT_STREAM_SORT_SCRATCH_BYTES: u64 = 64;
/// Status key reference/value tuple plus Vec/allocator slack retained during status sorting.
const SNAPSHOT_STATUS_SORT_SCRATCH_BYTES: u64 = 64;

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
                SOURCE_ADMISSION_BYTES,
            )?,
        )?;
        total = add(
            total,
            multiply(
                config.maximum_streams_per_route().get() as u64,
                persistent_stream_bytes(route.depth().get())?,
            )?,
        )?;
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
    // The official retained-reader budget is runtime-wide and weighted: a single-shard lease
    // consumes one permit, while `try_load_all` consumes one permit for every retained shard.
    // Therefore each permit can retain at most one per-shard publication and is not multiplied by
    // shard count a second time here.
    total = add(
        total,
        multiply(
            u64::from(config.maximum_retained_snapshot_readers().get()),
            snapshot_bytes,
        )?,
    )?;
    // Every shard may construct concurrently. Route-key scratch scales with configured routes;
    // one maximum-sized stream/status ordering workspace may coexist in each actor.
    total = add(
        total,
        multiply(routes.len() as u64, SNAPSHOT_ROUTE_SORT_SCRATCH_BYTES)?,
    )?;
    total = add(
        total,
        multiply(shards, per_actor_snapshot_sort_scratch(config)?)?,
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

fn per_actor_snapshot_sort_scratch(
    config: &LiveRuntimeConfig,
) -> Result<u64, LiveRuntimeConfigError> {
    multiply(
        config.maximum_streams_per_route().get() as u64,
        add(
            SNAPSHOT_STREAM_SORT_SCRATCH_BYTES,
            SNAPSHOT_STATUS_SORT_SCRATCH_BYTES,
        )?,
    )
}

fn persistent_stream_bytes(depth: usize) -> Result<u64, LiveRuntimeConfigError> {
    let levels = multiply(depth as u64, 2)?;
    let dual_book_level = add(SCALED_BOOK_LEVEL_BYTES, EXACT_BOOK_LEVEL_BYTES)?;
    let book_bytes = multiply(levels, dual_book_level)?;
    let inline = u64::try_from(crate::processor::persistent_stream_inline_bytes())
        .map_err(|_| LiveRuntimeConfigError::CapacityOverflow)?;
    // Charge owned identity text separately even though the inline String handles are already in
    // the structural size; this represents their maximum heap allocation, not duplicate objects.
    // The inline `size_of` keeps any future sequence/checksum/provenance growth in the model.
    add(
        add(
            add(inline, STREAM_KEY_ALLOCATION_BYTES)?,
            STREAM_MAP_ALLOCATION_BYTES,
        )?,
        add(STREAM_AUTHORITY_ALLOCATION_BYTES, book_bytes)?,
    )
}

fn add(left: u64, right: u64) -> Result<u64, LiveRuntimeConfigError> {
    left.checked_add(right)
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)
}

fn multiply(left: u64, right: u64) -> Result<u64, LiveRuntimeConfigError> {
    left.checked_mul(right)
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)
}

#[cfg(test)]
#[path = "tests/config_memory.rs"]
mod tests;
