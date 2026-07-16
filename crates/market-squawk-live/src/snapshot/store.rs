//! Crate-private ArcSwap publication plane and bounded official readers.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use tokio::sync::{Semaphore, mpsc};

use super::{
    LiveRuntimeSnapshotLease, LiveSnapshotLease, LiveSnapshotReader, ShardSnapshot,
    ShardSnapshotRevision, SnapshotReadError,
};
use crate::{ShardCount, ShardId, ShardRoutingVersion};
use thiserror::Error;

#[derive(Debug)]
struct SnapshotCell {
    shard: ShardId,
    value: ArcSwap<ShardSnapshot>,
}

/// Actor-owned publication handle. The value cell never crosses the crate boundary.
#[derive(Debug)]
pub(crate) struct SnapshotPublisher {
    cell: Arc<SnapshotCell>,
    routing_version: ShardRoutingVersion,
    shard_count: ShardCount,
    runtime_incarnation: std::num::NonZeroU64,
    notification: mpsc::Sender<()>,
    dropped_notifications: Arc<AtomicU64>,
}

impl SnapshotPublisher {
    pub(crate) fn publish(&self, snapshot: ShardSnapshot) -> Result<(), SnapshotPublishError> {
        if snapshot.shard_id() != self.cell.shard
            || snapshot.routing_version() != self.routing_version
            || snapshot.shard_count() != self.shard_count
            || snapshot.runtime_incarnation() != self.runtime_incarnation
        {
            return Err(SnapshotPublishError::IdentityTransplant);
        }
        let current = self.cell.value.load();
        let expected = current
            .snapshot_revision()
            .get()
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .ok_or(SnapshotPublishError::RevisionExhausted)?;
        if snapshot.snapshot_revision() != expected {
            return Err(SnapshotPublishError::NonSuccessorRevision {
                current: current.snapshot_revision().get(),
                proposed: snapshot.snapshot_revision().get(),
            });
        }
        self.cell.value.store(Arc::new(snapshot));
        if matches!(
            self.notification.try_send(()),
            Err(mpsc::error::TrySendError::Full(()))
        ) {
            self.dropped_notifications
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value.saturating_add(1))
                })
                .ok();
        }
        Ok(())
    }

    pub(crate) fn dropped_notifications(&self) -> u64 {
        self.dropped_notifications.load(Ordering::Relaxed)
    }
}

/// Fail-closed immutable publication rejection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SnapshotPublishError {
    #[error("snapshot routing, shard, or runtime incarnation identity was transplanted")]
    IdentityTransplant,
    #[error("snapshot revision exhausted")]
    RevisionExhausted,
    #[error("snapshot revision {proposed} is not the exact successor of {current}")]
    NonSuccessorRevision { current: u64, proposed: u64 },
}

/// Shared authority-free snapshot cells and one global retained-reader budget.
#[derive(Debug)]
pub(crate) struct SnapshotPlane {
    count: ShardCount,
    cells: Box<[Arc<SnapshotCell>]>,
    readers: Arc<Semaphore>,
    closed: AtomicBool,
}

/// Fully allocated publication plane returned before any actor is spawned.
#[derive(Debug)]
pub(crate) struct SnapshotPlaneBundle {
    pub(crate) publishers: Box<[SnapshotPublisher]>,
    pub(crate) reader: LiveSnapshotReader,
    pub(crate) notifications: Box<[mpsc::Receiver<()>]>,
}

impl SnapshotPlane {
    pub(crate) fn try_load(&self, shard: ShardId) -> Result<LiveSnapshotLease, SnapshotReadError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SnapshotReadError::Closed);
        }
        let cell = self.cell(shard)?;
        let permit =
            Arc::clone(&self.readers)
                .try_acquire_owned()
                .map_err(|error| match error {
                    tokio::sync::TryAcquireError::NoPermits => {
                        SnapshotReadError::ReaderLimitReached
                    }
                    tokio::sync::TryAcquireError::Closed => SnapshotReadError::Closed,
                })?;
        Ok(LiveSnapshotLease::new(cell.value.load_full(), permit))
    }

    pub(crate) fn try_load_all(&self) -> Result<LiveRuntimeSnapshotLease, SnapshotReadError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SnapshotReadError::Closed);
        }
        let permit_count = u32::from(self.count.get());
        let permit = Arc::clone(&self.readers)
            .try_acquire_many_owned(permit_count)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => SnapshotReadError::ReaderLimitReached,
                tokio::sync::TryAcquireError::Closed => SnapshotReadError::Closed,
            })?;
        let mut snapshots = Vec::new();
        snapshots
            .try_reserve_exact(self.cells.len())
            .map_err(|_| SnapshotReadError::ReaderLimitReached)?;
        let mut revisions = Vec::new();
        revisions
            .try_reserve_exact(self.cells.len())
            .map_err(|_| SnapshotReadError::ReaderLimitReached)?;
        for cell in &self.cells {
            let snapshot = cell.value.load_full();
            revisions.push(ShardSnapshotRevision {
                shard_id: snapshot.shard_id(),
                snapshot_revision: snapshot.snapshot_revision(),
                evaluated_at: snapshot.evaluated_at(),
                published_at: snapshot.published_at(),
            });
            snapshots.push(snapshot);
        }
        Ok(LiveRuntimeSnapshotLease::new(
            snapshots.into_boxed_slice(),
            revisions.into_boxed_slice(),
            permit,
        ))
    }

    fn cell(&self, shard: ShardId) -> Result<&Arc<SnapshotCell>, SnapshotReadError> {
        if shard.count() != self.count {
            return Err(SnapshotReadError::UnknownShard);
        }
        self.cells
            .get(usize::from(shard.index()))
            .ok_or(SnapshotReadError::UnknownShard)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.readers.close();
    }
}

/// Constructs all cells before actors start so readers never observe a missing shard.
pub(crate) fn create_snapshot_plane(
    initial: Vec<ShardSnapshot>,
    maximum_readers: u32,
) -> Result<SnapshotPlaneBundle, SnapshotReadError> {
    let first = initial.first().ok_or(SnapshotReadError::UnknownShard)?;
    let count = first.shard_count();
    if initial.len() != usize::from(count.get()) {
        return Err(SnapshotReadError::UnknownShard);
    }
    let readers =
        usize::try_from(maximum_readers).map_err(|_| SnapshotReadError::ReaderLimitReached)?;
    if readers == 0 || readers > Semaphore::MAX_PERMITS {
        return Err(SnapshotReadError::ReaderLimitReached);
    }
    let mut cells = Vec::new();
    let mut publishers = Vec::new();
    let mut receivers = Vec::new();
    cells
        .try_reserve_exact(initial.len())
        .map_err(|_| SnapshotReadError::ReaderLimitReached)?;
    publishers
        .try_reserve_exact(initial.len())
        .map_err(|_| SnapshotReadError::ReaderLimitReached)?;
    receivers
        .try_reserve_exact(initial.len())
        .map_err(|_| SnapshotReadError::ReaderLimitReached)?;
    for (index, snapshot) in initial.into_iter().enumerate() {
        if snapshot.shard_id().index()
            != u16::try_from(index).map_err(|_| SnapshotReadError::UnknownShard)?
            || snapshot.shard_count() != count
        {
            return Err(SnapshotReadError::UnknownShard);
        }
        let shard = snapshot.shard_id();
        let routing_version = snapshot.routing_version();
        let shard_count = snapshot.shard_count();
        let runtime_incarnation = snapshot.runtime_incarnation();
        let cell = Arc::new(SnapshotCell {
            shard,
            value: ArcSwap::from_pointee(snapshot),
        });
        let (notification, receiver) = mpsc::channel(1);
        publishers.push(SnapshotPublisher {
            cell: Arc::clone(&cell),
            routing_version,
            shard_count,
            runtime_incarnation,
            notification,
            dropped_notifications: Arc::new(AtomicU64::new(0)),
        });
        cells.push(cell);
        receivers.push(receiver);
    }
    let plane = Arc::new(SnapshotPlane {
        count,
        cells: cells.into_boxed_slice(),
        readers: Arc::new(Semaphore::new(readers)),
        closed: AtomicBool::new(false),
    });
    Ok(SnapshotPlaneBundle {
        publishers: publishers.into_boxed_slice(),
        reader: LiveSnapshotReader {
            plane: Arc::clone(&plane),
        },
        notifications: receivers.into_boxed_slice(),
    })
}

#[cfg(test)]
#[path = "store/tests.rs"]
mod tests;
