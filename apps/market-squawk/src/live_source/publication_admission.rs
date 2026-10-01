//! One bounded frame admission retained from transport receive through durable publication.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// Non-cloneable ownership of both an end-to-end frame admission and its raw queue slot.
#[derive(Debug)]
pub(super) struct PublicationReservation<T> {
    slot: mpsc::OwnedPermit<T>,
    frame: OwnedSemaphorePermit,
}

impl<T> PublicationReservation<T> {
    pub(super) async fn reserve(
        sender: mpsc::Sender<T>,
        frames: Arc<Semaphore>,
    ) -> Result<Self, ()> {
        let frame = tokio::select! {
            biased;
            () = sender.closed() => return Err(()),
            frame = frames.acquire_owned() => frame.map_err(|_| ())?,
        };
        let slot = sender.reserve_owned().await.map_err(|_| ())?;
        Ok(Self { slot, frame })
    }

    /// Sending consumes the queue slot; the frame admission must travel with its exact input.
    pub(super) fn into_parts(self) -> (mpsc::OwnedPermit<T>, OwnedSemaphorePermit) {
        (self.slot, self.frame)
    }
}

/// Waits for every previously admitted frame while its source still retains publication authority.
/// Callers must first stop receiving and release any unused receive reservation. The permit is
/// released before returning, so sibling Kraken owners sharing this budget cannot deadlock.
pub(super) async fn drain_admitted_publications<T>(
    sender: &mpsc::Sender<T>,
    frames: &Arc<Semaphore>,
    capacity: NonZeroUsize,
    deadline: Instant,
    forced: &CancellationToken,
) -> Result<(), PublicationDrainError> {
    let count =
        u32::try_from(capacity.get()).map_err(|_| PublicationDrainError::InvalidCapacity)?;
    let drained = tokio::select! {
        biased;
        () = forced.cancelled() => return Err(PublicationDrainError::Interrupted),
        () = sender.closed() => return Err(PublicationDrainError::WorkerClosed),
        () = tokio::time::sleep_until(deadline.into()) => return Err(PublicationDrainError::DeadlineElapsed),
        permit = frames.acquire_many(count) => permit.map_err(|_| PublicationDrainError::WorkerClosed)?,
    };
    // A failed worker can release its input permits while closing the receiver. Its actual
    // result remains owned by the publication supervisor and must still be joined by composition.
    let result = if forced.is_cancelled() {
        Err(PublicationDrainError::Interrupted)
    } else if sender.is_closed() {
        Err(PublicationDrainError::WorkerClosed)
    } else {
        Ok(())
    };
    drop(drained);
    result
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum PublicationDrainError {
    #[error("publication drain was forcibly interrupted")]
    Interrupted,
    #[error("publication drain exceeded its original shutdown deadline")]
    DeadlineElapsed,
    #[error("publication worker closed before its admitted work drained")]
    WorkerClosed,
    #[error("publication admission capacity cannot be represented")]
    InvalidCapacity,
    #[error("publication drain has no original shutdown deadline")]
    MissingDeadline,
    #[error("unsubmitted captured frames remain outside publication admission")]
    UnsubmittedFrames,
}
