//! Bounded ownership for the Kraken durable-publication source handoff.

use std::{num::NonZeroUsize, sync::Arc};

use market_squawk_adapter_kraken::KrakenPendingPublication;
use market_squawk_domain::Timestamp;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use super::publication_admission::PublicationReservation;

/// Exact captured frame plus its one-use typed single-decode result.
#[derive(Debug)]
pub(super) struct KrakenCapturedPublicationInput {
    frame_admission: OwnedSemaphorePermit,
    pending: KrakenPendingPublication,
    observed_at: Timestamp,
}

impl KrakenCapturedPublicationInput {
    pub(super) fn into_parts(self) -> (KrakenPendingPublication, Timestamp, OwnedSemaphorePermit) {
        (self.pending, self.observed_at, self.frame_admission)
    }
}

/// Bounded source-side sender with asynchronous frame admission, installed by the
/// owning publication supervisor. Both Kraken channels share the admission budget.
#[derive(Clone, Debug)]
pub(super) struct KrakenCapturedPublicationIngress {
    sender: mpsc::Sender<KrakenCapturedPublicationInput>,
    frames: Arc<Semaphore>,
}

impl KrakenCapturedPublicationIngress {
    pub(super) fn try_channel(
        capacity: NonZeroUsize,
        frames: Arc<Semaphore>,
    ) -> (Self, KrakenCapturedPublicationReceiver) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (
            Self { sender, frames },
            KrakenCapturedPublicationReceiver { receiver },
        )
    }

    pub(super) fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub(super) async fn reserve(
        &self,
    ) -> Result<PublicationReservation<KrakenCapturedPublicationInput>, ()> {
        PublicationReservation::reserve(self.sender.clone(), Arc::clone(&self.frames)).await
    }

    pub(super) fn submit_reserved(
        &self,
        reservation: PublicationReservation<KrakenCapturedPublicationInput>,
        pending: KrakenPendingPublication,
        observed_at: Timestamp,
    ) -> Result<(), KrakenCapturedPublicationInput> {
        let (permit, frame_admission) = reservation.into_parts();
        let input = KrakenCapturedPublicationInput {
            frame_admission,
            pending,
            observed_at,
        };
        if self.sender.is_closed() {
            return Err(input);
        }
        drop(permit.send(input));
        Ok(())
    }
}

/// Sole bounded consumer transferred to the C2-C2b application rendezvous owner.
#[derive(Debug)]
pub(super) struct KrakenCapturedPublicationReceiver {
    receiver: mpsc::Receiver<KrakenCapturedPublicationInput>,
}

impl KrakenCapturedPublicationReceiver {
    pub(super) async fn recv(&mut self) -> Option<KrakenCapturedPublicationInput> {
        self.receiver.recv().await
    }

    pub(super) fn close(&mut self) {
        self.receiver.close();
    }

    pub(super) fn try_recv(
        &mut self,
    ) -> Result<KrakenCapturedPublicationInput, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}
