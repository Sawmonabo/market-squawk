//! One bounded frame admission retained from transport receive through durable publication.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

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
