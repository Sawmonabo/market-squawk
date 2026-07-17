//! Checked conservative peak-memory model for one complete runtime incarnation.

use std::mem::size_of;
use std::num::NonZeroU64;

use market_squawk_domain::{SourceId, SourceIdentifier, VenueId};

use super::{LiveRouteConfig, LiveRuntimeConfig, LiveRuntimeConfigError};
use crate::provider_book::{
    exact_level_arc_allocation_bytes, maximum_book_items_for_message, provider_book_buffer_bytes,
    shard_book_scratch_bytes,
};
use crate::{ShardRouter, ShardRoutingVersion};

const ROUTE_FIXED_BYTES: u64 = 32 * 1024;
/// Admission and generation-registry ownership per distinct source.
const SOURCE_ADMISSION_BYTES: u64 = 8 * 1024;
const NONCE_SLOT_BYTES: u64 = 192;
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

    // The byte semaphore already retains every admitted command and its decoded nested
    // allocations. Add only processing-owned storage here: one preallocated normalization scratch
    // per actor plus the maximum snapshot/delta transaction on each shard that owns a route.
    total = add(total, all_shard_book_processing_bytes(config, routes)?)?;

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
    let buffers =
        provider_book_buffer_bytes(depth).ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
    let active_exact_levels = multiply(depth as u64, 2)?;
    let active_exact = multiply(active_exact_levels, exact_level_arc_allocation_bytes())?;
    let book_bytes = add(buffers, active_exact)?;
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

/// Closed processing inventory for one shard at one route depth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BookProcessingPeak {
    pub(crate) maximum_message_bytes: u64,
    pub(crate) maximum_book_items: u64,
    pub(crate) shard_scratch_bytes: u64,
    pub(crate) candidate_exact_bytes: u64,
    pub(crate) snapshot_canonical_bytes: u64,
    pub(crate) delta_canonical_bytes: u64,
    pub(crate) snapshot_additional_bytes: u64,
    pub(crate) delta_additional_bytes: u64,
    pub(crate) additional_bytes: u64,
}

/// Derives the larger snapshot/delta processing peak for one shard.
///
/// The admitted command is deliberately excluded: its complete retained bytes are already charged
/// by `mailbox_bytes_per_shard`. Runtime book mutation has no rollback vectors or tree nodes: the
/// active fixed buffers remain published while the inactive fixed buffers and shard scratch are
/// reused. New exact-level `Arc` pointees coexist with the prior committed pointees. Canonical
/// domain vectors charge both their final allocations and the unsafe-free box-conversion overlap
/// used to normalize logical capacity.
pub(crate) fn book_processing_peak(
    maximum_message_bytes: u32,
    depth: usize,
) -> Result<BookProcessingPeak, LiveRuntimeConfigError> {
    let maximum_items = maximum_book_items_for_message(maximum_message_bytes);
    let maximum_items_u64 = maximum_items as u64;
    let retained_candidate_levels = multiply(depth as u64, 2)?.min(maximum_items_u64);
    let shard_scratch_bytes =
        shard_book_scratch_bytes(maximum_items).ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
    let candidate_exact_bytes = multiply(
        retained_candidate_levels,
        exact_level_arc_allocation_bytes(),
    )?;
    let snapshot_canonical_bytes =
        crate::processor::snapshot_canonical_vector_peak_bytes(depth, maximum_items)
            .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
    let delta_canonical_bytes = crate::processor::delta_canonical_vector_peak_bytes(maximum_items)
        .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
    let snapshot_additional_bytes = add(
        shard_scratch_bytes,
        add(candidate_exact_bytes, snapshot_canonical_bytes)?,
    )?;
    let delta_additional_bytes = add(
        shard_scratch_bytes,
        add(candidate_exact_bytes, delta_canonical_bytes)?,
    )?;
    Ok(BookProcessingPeak {
        maximum_message_bytes: u64::from(maximum_message_bytes),
        maximum_book_items: maximum_items_u64,
        shard_scratch_bytes,
        candidate_exact_bytes,
        snapshot_canonical_bytes,
        delta_canonical_bytes,
        snapshot_additional_bytes,
        delta_additional_bytes,
        additional_bytes: snapshot_additional_bytes.max(delta_additional_bytes),
    })
}

fn all_shard_book_processing_bytes(
    config: &LiveRuntimeConfig,
    routes: &[LiveRouteConfig],
) -> Result<u64, LiveRuntimeConfigError> {
    let shard_count = usize::from(config.shard_count().get());
    let maximum_message_bytes = config.maximum_message_bytes().get();
    let maximum_items = maximum_book_items_for_message(maximum_message_bytes);
    let scratch =
        shard_book_scratch_bytes(maximum_items).ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
    let mut total = multiply(shard_count as u64, scratch)?;
    let router = match config.routing_version() {
        ShardRoutingVersion::V1 => ShardRouter::v1(config.shard_count().get())?,
    };
    let mut maximum_depths = vec![0_usize; shard_count];
    for route in routes {
        let shard = router.route(route.route());
        let depth = maximum_depths
            .get_mut(usize::from(shard.index()))
            .ok_or(LiveRuntimeConfigError::RouteOutsideShardSet)?;
        *depth = (*depth).max(route.depth().get());
    }
    for depth in maximum_depths.into_iter().filter(|depth| *depth > 0) {
        let peak = book_processing_peak(maximum_message_bytes, depth)?;
        let transaction_only = peak
            .additional_bytes
            .checked_sub(peak.shard_scratch_bytes)
            .ok_or(LiveRuntimeConfigError::CapacityOverflow)?;
        total = add(total, transaction_only)?;
    }
    Ok(total)
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
