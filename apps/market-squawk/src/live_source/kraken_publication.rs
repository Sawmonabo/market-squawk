//! Bounded ownership for the Kraken durable-publication source handoff.

use std::num::NonZeroUsize;

use market_squawk_adapter_kraken::KrakenPendingPublication;
use market_squawk_domain::Timestamp;
use tokio::sync::mpsc;

/// Exact captured frame plus its one-use typed single-decode result.
#[derive(Debug)]
pub(super) struct KrakenCapturedPublicationInput {
    pending: KrakenPendingPublication,
    observed_at: Timestamp,
}

impl KrakenCapturedPublicationInput {
    pub(super) fn into_parts(self) -> (KrakenPendingPublication, Timestamp) {
        (self.pending, self.observed_at)
    }
}

/// Nonblocking source-side sender installed only by the owning application publication
/// supervisor. Absence is represented once by the common sink publication-ingress enum.
#[derive(Clone, Debug)]
pub(super) struct KrakenCapturedPublicationIngress {
    sender: mpsc::Sender<KrakenCapturedPublicationInput>,
}

impl KrakenCapturedPublicationIngress {
    pub(super) fn try_channel(capacity: NonZeroUsize) -> (Self, KrakenCapturedPublicationReceiver) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (
            Self { sender },
            KrakenCapturedPublicationReceiver { receiver },
        )
    }

    pub(super) fn try_submit(
        &self,
        pending: KrakenPendingPublication,
        observed_at: Timestamp,
    ) -> Result<(), KrakenCapturedPublicationInput> {
        let input = KrakenCapturedPublicationInput {
            pending,
            observed_at,
        };
        self.sender
            .try_send(input)
            .map_err(mpsc::error::TrySendError::into_inner)
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
