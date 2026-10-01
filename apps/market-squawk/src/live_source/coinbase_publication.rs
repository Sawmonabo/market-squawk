//! Bounded ownership for the public Coinbase durable-publication handoff.

use std::{num::NonZeroUsize, sync::Arc};

use market_squawk_adapter_coinbase::{
    CoinbaseMarketHandoff, CoinbaseMarketPublicationContext, CoinbaseMarketSealRejoin,
};
use market_squawk_domain::Timestamp;
use market_squawk_sources::ProviderCaptureSealRequest;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::live_source::publication_admission::PublicationReservation;

/// Closed live qualification result; raw custody is retained for either disposition.
#[derive(Clone, Copy, Debug)]
pub(in crate::live_source) enum CoinbaseCapturedPublicationDisposition {
    AwaitCommittedRows,
    FreshnessUnqualified,
}

/// Exact bounded Coinbase publication input transferred from one owning source generation.
#[derive(Debug)]
pub(in crate::live_source) enum CoinbaseCapturedPublicationInput {
    Public {
        disposition: CoinbaseCapturedPublicationDisposition,
        frame_admission: OwnedSemaphorePermit,
        rejoin: CoinbaseMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
    },
    Direct {
        handoff: CoinbaseMarketHandoff,
        context: CoinbaseMarketPublicationContext,
        observed_at: Timestamp,
    },
}

impl CoinbaseCapturedPublicationInput {
    pub(in crate::live_source) const fn observed_at(&self) -> Timestamp {
        match self {
            Self::Public { observed_at, .. } | Self::Direct { observed_at, .. } => *observed_at,
        }
    }
}

/// Bounded source-side sender with asynchronous public-frame admission, installed by
/// the owning publication supervisor. Direct submission remains synchronous.
#[derive(Clone, Debug)]
pub(in crate::live_source) struct CoinbaseCapturedPublicationIngress {
    sender: mpsc::Sender<CoinbaseCapturedPublicationInput>,
    frames: Arc<Semaphore>,
}

impl CoinbaseCapturedPublicationIngress {
    pub(in crate::live_source) fn try_channel(
        capacity: NonZeroUsize,
        frames: Arc<Semaphore>,
    ) -> (Self, CoinbaseCapturedPublicationReceiver) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (
            Self { sender, frames },
            CoinbaseCapturedPublicationReceiver { receiver },
        )
    }

    pub(in crate::live_source) fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub(in crate::live_source) async fn reserve(
        &self,
    ) -> Result<PublicationReservation<CoinbaseCapturedPublicationInput>, ()> {
        PublicationReservation::reserve(self.sender.clone(), Arc::clone(&self.frames)).await
    }

    pub(in crate::live_source) fn submit_reserved(
        &self,
        reservation: PublicationReservation<CoinbaseCapturedPublicationInput>,
        disposition: CoinbaseCapturedPublicationDisposition,
        rejoin: CoinbaseMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
    ) -> Result<(), CoinbaseCapturedPublicationInput> {
        let (permit, frame_admission) = reservation.into_parts();
        let input = CoinbaseCapturedPublicationInput::Public {
            disposition,
            frame_admission,
            rejoin,
            seal_request,
            observed_at,
        };
        if self.sender.is_closed() {
            return Err(input);
        }
        // The sole sink owns this queue's reservation; sending cannot fail for capacity.
        drop(permit.send(input));
        Ok(())
    }

    pub(in crate::live_source) fn try_submit_direct(
        &self,
        handoff: CoinbaseMarketHandoff,
        context: CoinbaseMarketPublicationContext,
        observed_at: Timestamp,
    ) -> Result<(), CoinbaseCapturedPublicationInput> {
        let input = CoinbaseCapturedPublicationInput::Direct {
            handoff,
            context,
            observed_at,
        };
        self.sender.try_send(input).map_err(|error| {
            let reason = match &error {
                mpsc::error::TrySendError::Full(_) => "full",
                mpsc::error::TrySendError::Closed(_) => "closed",
            };
            tracing::warn!(reason, "Coinbase durable publication queue rejected input");
            error.into_inner()
        })
    }
}

/// Sole bounded consumer transferred to the application-owned publication supervisor.
#[derive(Debug)]
pub(in crate::live_source) struct CoinbaseCapturedPublicationReceiver {
    receiver: mpsc::Receiver<CoinbaseCapturedPublicationInput>,
}

impl CoinbaseCapturedPublicationReceiver {
    pub(in crate::live_source) async fn recv(
        &mut self,
    ) -> Option<CoinbaseCapturedPublicationInput> {
        self.receiver.recv().await
    }

    pub(in crate::live_source) fn close(&mut self) {
        self.receiver.close();
    }

    pub(in crate::live_source) fn try_recv(
        &mut self,
    ) -> Result<CoinbaseCapturedPublicationInput, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}
