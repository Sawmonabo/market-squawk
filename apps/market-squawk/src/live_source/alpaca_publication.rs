//! Count-and-byte bounded captured Alpaca handoff. Permits remain held through custody and commit.
use crate::live_source::publication_admission::PublicationReservation;
use market_squawk_adapter_alpaca::AlpacaMarketSealRejoin;
use market_squawk_domain::Timestamp;
use market_squawk_sources::ProviderCaptureSealRequest;
use std::{num::NonZeroUsize, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

#[derive(Debug)]
pub(crate) struct AlpacaCapturedPublicationInput {
    pub(crate) rejoin: AlpacaMarketSealRejoin,
    pub(crate) seal_request: ProviderCaptureSealRequest,
    pub(crate) observed_at: Timestamp,
    pub(crate) _frame: OwnedSemaphorePermit,
    pub(crate) _bytes: AlpacaPublicationBytes,
}
/// Queued allocations use the exact byte semaphore. The sole cancelled producer may move its
/// already bounded working allocation through its reserved slot for raw-only sealing. That
/// allocation is not a second queue and no further frame can be received after this transfer.
#[derive(Debug)]
pub(crate) enum AlpacaPublicationBytes {
    Queued(OwnedSemaphorePermit),
    CancelledProducer { retained_bytes: usize },
}

impl AlpacaPublicationBytes {
    pub(crate) fn is_cancelled_producer(&self) -> bool {
        matches!(self, Self::CancelledProducer { .. })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        match self {
            Self::Queued(permit) => permit.num_permits(),
            Self::CancelledProducer { retained_bytes } => *retained_bytes,
        }
    }
}

#[derive(Debug)]
pub(super) struct PendingAlpacaPublication {
    rejoin: AlpacaMarketSealRejoin,
    seal_request: ProviderCaptureSealRequest,
    observed_at: Timestamp,
    retained_bytes: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct AlpacaCapturedPublicationIngress {
    sender: mpsc::Sender<AlpacaCapturedPublicationInput>,
    bytes: Arc<Semaphore>,
    frames: Arc<Semaphore>,
    maximum_bytes: usize,
}
#[derive(Debug)]
pub(crate) struct AlpacaCapturedPublicationReceiver {
    receiver: mpsc::Receiver<AlpacaCapturedPublicationInput>,
    bytes: Arc<Semaphore>,
    frames: Arc<Semaphore>,
}
#[derive(Debug)]
pub(crate) enum AlpacaPublicationQueueError {
    Closed,
    Bounds,
}
impl AlpacaCapturedPublicationIngress {
    pub(crate) fn try_channel(
        capacity: NonZeroUsize,
        maximum_bytes: usize,
    ) -> Result<(Self, AlpacaCapturedPublicationReceiver), AlpacaPublicationQueueError> {
        if maximum_bytes == 0
            || maximum_bytes > u32::MAX as usize
            || maximum_bytes > Semaphore::MAX_PERMITS
        {
            return Err(AlpacaPublicationQueueError::Bounds);
        }
        let (sender, receiver) = mpsc::channel(capacity.get());
        let bytes = Arc::new(Semaphore::new(maximum_bytes));
        let frames = Arc::new(Semaphore::new(capacity.get()));
        Ok((
            Self {
                sender,
                bytes: Arc::clone(&bytes),
                frames: Arc::clone(&frames),
                maximum_bytes,
            },
            AlpacaCapturedPublicationReceiver {
                receiver,
                bytes,
                frames,
            },
        ))
    }
    pub(super) async fn reserve(
        &self,
    ) -> Result<PublicationReservation<AlpacaCapturedPublicationInput>, AlpacaPublicationQueueError>
    {
        PublicationReservation::reserve(self.sender.clone(), Arc::clone(&self.frames))
            .await
            .map_err(|_| AlpacaPublicationQueueError::Closed)
    }

    pub(super) fn prepare(
        &self,
        rejoin: AlpacaMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
    ) -> Result<PendingAlpacaPublication, AlpacaPublicationQueueError> {
        let bytes = rejoin
            .retained_bytes()
            .map_err(|_| AlpacaPublicationQueueError::Bounds)?
            .checked_add(
                seal_request
                    .checked_plain_retained_bytes()
                    .map_err(|_| AlpacaPublicationQueueError::Bounds)?,
            )
            .and_then(|value| {
                value.checked_add(std::mem::size_of::<AlpacaCapturedPublicationInput>())
            })
            .and_then(|value| value.checked_add(std::mem::size_of::<PendingAlpacaPublication>()))
            .filter(|value| *value <= self.maximum_bytes)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(AlpacaPublicationQueueError::Bounds)?;
        Ok(PendingAlpacaPublication {
            rejoin,
            seal_request,
            observed_at,
            retained_bytes: bytes,
        })
    }

    pub(super) async fn reserve_bytes(
        &self,
        pending: &PendingAlpacaPublication,
    ) -> Result<OwnedSemaphorePermit, AlpacaPublicationQueueError> {
        tokio::select! { biased;
            () = self.sender.closed() => Err(AlpacaPublicationQueueError::Closed),
            permit = Arc::clone(&self.bytes).acquire_many_owned(pending.retained_bytes) =>
                permit.map_err(|_| AlpacaPublicationQueueError::Closed),
        }
    }

    pub(super) fn submit_reserved(
        &self,
        reservation: PublicationReservation<AlpacaCapturedPublicationInput>,
        pending: PendingAlpacaPublication,
        bytes: OwnedSemaphorePermit,
    ) -> Result<(), AlpacaPublicationQueueError> {
        self.submit(reservation, pending, AlpacaPublicationBytes::Queued(bytes))
    }

    /// Moves, rather than discards, the single producer allocation on forced cancellation.
    /// The worker suppresses canonical publication and retains raw custody before releasing it.
    pub(super) fn submit_cancelled(
        &self,
        reservation: PublicationReservation<AlpacaCapturedPublicationInput>,
        pending: PendingAlpacaPublication,
    ) -> Result<(), AlpacaPublicationQueueError> {
        let retained_bytes = pending.retained_bytes as usize;
        self.submit(
            reservation,
            pending,
            AlpacaPublicationBytes::CancelledProducer { retained_bytes },
        )
    }

    fn submit(
        &self,
        reservation: PublicationReservation<AlpacaCapturedPublicationInput>,
        pending: PendingAlpacaPublication,
        bytes: AlpacaPublicationBytes,
    ) -> Result<(), AlpacaPublicationQueueError> {
        if self.sender.is_closed() {
            return Err(AlpacaPublicationQueueError::Closed);
        }
        let (slot, frame) = reservation.into_parts();
        drop(slot.send(AlpacaCapturedPublicationInput {
            rejoin: pending.rejoin,
            seal_request: pending.seal_request,
            observed_at: pending.observed_at,
            _frame: frame,
            _bytes: bytes,
        }));
        Ok(())
    }
}
impl AlpacaCapturedPublicationReceiver {
    pub(crate) async fn recv(&mut self) -> Option<AlpacaCapturedPublicationInput> {
        self.receiver.recv().await
    }
    pub(crate) fn close_admission(&self) {
        // Wake blocked admission without rejecting the producer's already reserved queue slot.
        // Its cancellation hook transfers that bounded allocation for raw-only custody. The
        // sole supervisor then drops every sender; recv terminates after the final handoff.
        self.bytes.close();
        self.frames.close();
    }
}
