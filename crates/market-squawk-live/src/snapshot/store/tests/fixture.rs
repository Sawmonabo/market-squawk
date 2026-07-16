use std::num::NonZeroU64;

use market_squawk_domain::Timestamp;

use crate::{
    ShardCount, ShardId, ShardLifecycleSnapshot, ShardRoutingVersion, ShardSnapshot,
    SnapshotDimension,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub(super) fn snapshot(
    index: u16,
    count: u16,
    incarnation: u64,
    revision: u64,
) -> TestResult<ShardSnapshot> {
    snapshot_with_health(
        index,
        count,
        incarnation,
        revision,
        revision.saturating_mul(10),
    )
}

pub(super) fn snapshot_with_health(
    index: u16,
    count: u16,
    incarnation: u64,
    revision: u64,
    health_revision: u64,
) -> TestResult<ShardSnapshot> {
    let shard_count = ShardCount::new(count)?;
    let evaluated_at = i64::try_from(revision).unwrap_or(i64::MAX - 1);
    Ok(ShardSnapshot {
        routing_version: ShardRoutingVersion::V1,
        shard_count,
        runtime_incarnation: NonZeroU64::new(incarnation).ok_or("zero incarnation")?,
        shard_id: ShardId::new(index, count)?,
        snapshot_revision: NonZeroU64::new(revision).ok_or("zero revision")?,
        health_revision,
        lifecycle: if revision == 1 {
            ShardLifecycleSnapshot::Starting
        } else {
            ShardLifecycleSnapshot::Ready
        },
        evaluated_at: Timestamp::from_unix_nanos(evaluated_at),
        published_at: Timestamp::from_unix_nanos(evaluated_at + 1),
        routes: Box::new([]),
        route_dimension: SnapshotDimension::from_counts(
            usize::try_from(health_revision % 3)?,
            0,
            3,
        )?,
        retained_bytes: 1_000 + revision % 1_000,
    })
}

pub(super) fn initial(count: u16, incarnation: u64) -> TestResult<Vec<ShardSnapshot>> {
    (0..count)
        .map(|index| snapshot(index, count, incarnation, 1))
        .collect()
}
