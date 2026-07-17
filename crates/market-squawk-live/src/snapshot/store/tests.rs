#[path = "tests/fixture.rs"]
mod fixture;

use static_assertions::assert_not_impl_any;

use super::{SnapshotPublishError, create_snapshot_plane};
use crate::{
    LiveRuntimeSnapshotLease, LiveSnapshotLease, ShardId, ShardLifecycleSnapshot, SnapshotReadError,
};

use fixture::{TestResult, initial, snapshot, snapshot_with_health};

assert_not_impl_any!(LiveSnapshotLease: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(LiveRuntimeSnapshotLease: Clone, serde::Serialize, serde::de::DeserializeOwned);

#[test]
fn exact_successor_replaces_the_same_shard_cell_without_partial_identity_changes() -> TestResult {
    let bundle = create_snapshot_plane(initial(1, 7)?, 2)?;
    let publisher = bundle.publishers.first().ok_or("missing publisher")?;
    let shard = ShardId::new(0, 1)?;
    let old = bundle.reader.try_load(shard)?;

    publisher.publish(snapshot_with_health(0, 1, 7, 2, 22)?)?;

    let current = bundle.reader.try_load(shard)?;
    assert_eq!(old.snapshot().snapshot_revision().get(), 1);
    assert_eq!(old.snapshot().health_revision(), 10);
    assert_eq!(old.snapshot().lifecycle(), ShardLifecycleSnapshot::Starting);
    assert_eq!(old.snapshot().retained_bytes(), 1_001);
    assert_eq!(current.snapshot().snapshot_revision().get(), 2);
    assert_eq!(current.snapshot().health_revision(), 22);
    assert_eq!(
        current.snapshot().lifecycle(),
        ShardLifecycleSnapshot::Ready
    );
    assert_eq!(current.snapshot().retained_bytes(), 1_002);
    Ok(())
}

#[test]
fn revision_regression_skip_and_incarnation_or_shard_transplant_never_publish() -> TestResult {
    let bundle = create_snapshot_plane(initial(1, 11)?, 1)?;
    let publisher = bundle.publishers.first().ok_or("missing publisher")?;
    let shard = ShardId::new(0, 1)?;

    assert_eq!(
        publisher.publish(snapshot(0, 1, 11, 1)?),
        Err(SnapshotPublishError::NonSuccessorRevision {
            current: 1,
            proposed: 1,
        })
    );
    assert_eq!(
        publisher.publish(snapshot(0, 1, 11, 3)?),
        Err(SnapshotPublishError::NonSuccessorRevision {
            current: 1,
            proposed: 3,
        })
    );
    assert_eq!(
        publisher.publish(snapshot(0, 1, 12, 2)?),
        Err(SnapshotPublishError::IdentityTransplant)
    );
    assert_eq!(
        publisher.publish(snapshot(0, 2, 11, 2)?),
        Err(SnapshotPublishError::IdentityTransplant)
    );
    assert_eq!(
        bundle
            .reader
            .try_load(shard)?
            .snapshot()
            .snapshot_revision()
            .get(),
        1
    );
    Ok(())
}

#[test]
fn revision_exhaustion_is_fail_closed_and_preserves_the_last_publication() -> TestResult {
    let bundle = create_snapshot_plane(vec![snapshot(0, 1, 3, u64::MAX)?], 1)?;
    let publisher = bundle.publishers.first().ok_or("missing publisher")?;

    assert_eq!(
        publisher.publish(snapshot(0, 1, 3, u64::MAX)?),
        Err(SnapshotPublishError::RevisionExhausted)
    );
    assert_eq!(
        bundle
            .reader
            .try_load(ShardId::new(0, 1)?)?
            .snapshot()
            .snapshot_revision()
            .get(),
        u64::MAX
    );
    Ok(())
}

#[test]
fn one_permit_is_charged_per_retained_shard_generation_and_drop_restores_it() -> TestResult {
    let bundle = create_snapshot_plane(initial(2, 5)?, 2)?;
    let first = bundle.reader.try_load(ShardId::new(0, 2)?)?;
    let second = bundle.reader.try_load(ShardId::new(1, 2)?)?;
    let modeled_single = crate::snapshot::snapshot_reader_metadata_peak_bytes(2, 2)
        .ok_or("modeled single-reader metadata overflow")?;
    assert!(
        first
            .observed_metadata_bytes()
            .checked_add(second.observed_metadata_bytes())
            .ok_or("observed single-reader metadata overflow")?
            <= modeled_single
    );
    assert_eq!(
        bundle.reader.try_load(ShardId::new(0, 2)?).err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );

    drop(first);
    let replacement = bundle.reader.try_load(ShardId::new(0, 2)?)?;
    assert_eq!(replacement.snapshot().shard_id(), ShardId::new(0, 2)?);
    drop(second);
    drop(replacement);
    assert!(bundle.reader.try_load_all().is_ok());
    Ok(())
}

#[test]
fn aggregate_read_charges_every_retained_shard_generation() -> TestResult {
    let one_permit = create_snapshot_plane(initial(2, 8)?, 1)?;
    assert_eq!(
        one_permit.reader.try_load_all().err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );

    let two_permits = create_snapshot_plane(initial(2, 8)?, 2)?;
    let aggregate = two_permits.reader.try_load_all()?;
    assert_eq!(aggregate.snapshots().len(), 2);
    assert_eq!(
        two_permits.reader.try_load(ShardId::new(0, 2)?).err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );
    drop(aggregate);
    assert!(two_permits.reader.try_load(ShardId::new(0, 2)?).is_ok());
    Ok(())
}

#[test]
fn retained_old_generations_exhaust_readers_but_never_block_publication() -> TestResult {
    let bundle = create_snapshot_plane(initial(1, 9)?, 2)?;
    let publisher = bundle.publishers.first().ok_or("missing publisher")?;
    let shard = ShardId::new(0, 1)?;
    let revision_one = bundle.reader.try_load(shard)?;
    publisher.publish(snapshot(0, 1, 9, 2)?)?;
    let revision_two = bundle.reader.try_load(shard)?;

    publisher.publish(snapshot(0, 1, 9, 3)?)?;
    assert_eq!(
        bundle.reader.try_load(shard).err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );
    assert_eq!(revision_one.snapshot().snapshot_revision().get(), 1);
    assert_eq!(revision_two.snapshot().snapshot_revision().get(), 2);

    drop(revision_one);
    let latest = bundle.reader.try_load(shard)?;
    assert_eq!(latest.snapshot().snapshot_revision().get(), 3);
    Ok(())
}

#[test]
fn maximum_aggregate_readers_retain_distinct_all_shard_generations_while_all_shards_republish()
-> TestResult {
    let bundle = create_snapshot_plane(initial(2, 19)?, 4)?;
    let generation_one = bundle.reader.try_load_all()?;
    for (index, publisher) in bundle.publishers.iter().enumerate() {
        publisher.publish(snapshot(u16::try_from(index)?, 2, 19, 2)?)?;
    }
    let generation_two = bundle.reader.try_load_all()?;
    for (index, publisher) in bundle.publishers.iter().enumerate() {
        publisher.publish(snapshot(u16::try_from(index)?, 2, 19, 3)?)?;
    }

    assert_eq!(
        bundle.reader.try_load_all().err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );
    assert!(
        generation_one
            .revisions()
            .iter()
            .all(|revision| revision.snapshot_revision().get() == 1)
    );
    assert!(
        generation_two
            .revisions()
            .iter()
            .all(|revision| revision.snapshot_revision().get() == 2)
    );
    let observed_metadata = generation_one
        .observed_metadata_bytes()
        .and_then(|first| {
            generation_two
                .observed_metadata_bytes()
                .and_then(|second| first.checked_add(second))
        })
        .ok_or("observed aggregate metadata overflow")?;
    let modeled_metadata = crate::snapshot::snapshot_reader_metadata_peak_bytes(4, 2)
        .ok_or("modeled aggregate metadata overflow")?;
    assert!(observed_metadata <= modeled_metadata);
    drop(generation_one);
    let latest = bundle.reader.try_load_all()?;
    assert!(
        latest
            .revisions()
            .iter()
            .all(|revision| revision.snapshot_revision().get() == 3)
    );
    Ok(())
}

#[test]
fn notification_fullness_coalesces_hints_without_delaying_or_losing_latest_state() -> TestResult {
    let mut bundle = create_snapshot_plane(initial(1, 4)?, 1)?;
    let publisher = bundle.publishers.first().ok_or("missing publisher")?;
    let notifications = bundle
        .notifications
        .first_mut()
        .ok_or("missing notification receiver")?;
    let shard = ShardId::new(0, 1)?;

    publisher.publish(snapshot(0, 1, 4, 2)?)?;
    publisher.publish(snapshot(0, 1, 4, 3)?)?;
    publisher.publish(snapshot(0, 1, 4, 4)?)?;
    assert_eq!(publisher.dropped_notifications(), 2);
    assert_eq!(
        bundle
            .reader
            .try_load(shard)?
            .snapshot()
            .snapshot_revision()
            .get(),
        4
    );
    assert_eq!(notifications.try_recv(), Ok(()));
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    publisher.publish(snapshot(0, 1, 4, 5)?)?;
    assert_eq!(notifications.try_recv(), Ok(()));
    assert_eq!(publisher.dropped_notifications(), 2);
    Ok(())
}

#[test]
fn aggregate_snapshots_and_revision_vector_are_sorted_without_global_as_of() -> TestResult {
    let bundle = create_snapshot_plane(initial(3, 15)?, 3)?;
    bundle.publishers[2].publish(snapshot(2, 3, 15, 2)?)?;

    let lease = bundle.reader.try_load_all()?;
    let shard_ids = lease
        .snapshots()
        .map(|snapshot| snapshot.shard_id().index())
        .collect::<Vec<_>>();
    let revisions = lease
        .revisions()
        .iter()
        .map(|revision| {
            (
                revision.shard_id().index(),
                revision.snapshot_revision().get(),
                revision.evaluated_at(),
                revision.published_at(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(shard_ids, [0, 1, 2]);
    assert_eq!(revisions[0].0, 0);
    assert_eq!(revisions[1].0, 1);
    assert_eq!(revisions[2].0, 2);
    assert_eq!(revisions[2].1, 2);
    assert_ne!(revisions[0].2, revisions[2].2);
    assert_ne!(revisions[0].3, revisions[2].3);
    Ok(())
}

#[test]
fn close_rejects_new_reads_without_mutating_an_existing_lease() -> TestResult {
    let bundle = create_snapshot_plane(initial(1, 6)?, 1)?;
    let shard = ShardId::new(0, 1)?;
    let retained = bundle.reader.try_load(shard)?;

    bundle.reader.plane.close();

    assert_eq!(
        bundle.reader.try_load(shard).err(),
        Some(SnapshotReadError::Closed)
    );
    assert_eq!(
        bundle.reader.try_load_all().err(),
        Some(SnapshotReadError::Closed)
    );
    assert_eq!(retained.snapshot().snapshot_revision().get(), 1);
    Ok(())
}

#[test]
fn malformed_initial_shard_sets_are_rejected_before_any_reader_escapes() -> TestResult {
    assert_eq!(
        create_snapshot_plane(Vec::new(), 1).err(),
        Some(SnapshotReadError::UnknownShard)
    );
    assert_eq!(
        create_snapshot_plane(vec![snapshot(0, 2, 1, 1)?], 1).err(),
        Some(SnapshotReadError::UnknownShard)
    );
    assert_eq!(
        create_snapshot_plane(vec![snapshot(1, 2, 1, 1)?, snapshot(0, 2, 1, 1)?], 1,).err(),
        Some(SnapshotReadError::UnknownShard)
    );
    assert_eq!(
        create_snapshot_plane(initial(1, 1)?, 0).err(),
        Some(SnapshotReadError::ReaderLimitReached)
    );
    Ok(())
}
