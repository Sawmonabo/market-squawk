//! Count-and-byte bounded captured Alpaca handoff. Permits remain held through custody and commit.
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
    pub(crate) _bytes: OwnedSemaphorePermit,
}
#[derive(Clone, Debug)]
pub(crate) struct AlpacaCapturedPublicationIngress {
    sender: mpsc::Sender<AlpacaCapturedPublicationInput>,
    bytes: Arc<Semaphore>,
    maximum_bytes: usize,
}
#[derive(Debug)]
pub(crate) struct AlpacaCapturedPublicationReceiver {
    receiver: mpsc::Receiver<AlpacaCapturedPublicationInput>,
}
#[derive(Debug)]
pub(crate) enum AlpacaPublicationQueueError {
    Full,
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
        Ok((
            Self {
                sender,
                bytes: Arc::new(Semaphore::new(maximum_bytes)),
                maximum_bytes,
            },
            AlpacaCapturedPublicationReceiver { receiver },
        ))
    }
    pub(crate) fn try_submit(
        &self,
        rejoin: AlpacaMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
    ) -> Result<(), AlpacaPublicationQueueError> {
        if self.sender.is_closed() {
            return Err(AlpacaPublicationQueueError::Closed);
        }
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
            .filter(|value| *value <= self.maximum_bytes)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(AlpacaPublicationQueueError::Bounds)?;
        let permit = Arc::clone(&self.bytes)
            .try_acquire_many_owned(bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => AlpacaPublicationQueueError::Closed,
                tokio::sync::TryAcquireError::NoPermits => AlpacaPublicationQueueError::Full,
            })?;
        self.sender
            .try_send(AlpacaCapturedPublicationInput {
                rejoin,
                seal_request,
                observed_at,
                _bytes: permit,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AlpacaPublicationQueueError::Full,
                mpsc::error::TrySendError::Closed(_) => AlpacaPublicationQueueError::Closed,
            })
    }
}
impl AlpacaCapturedPublicationReceiver {
    pub(crate) async fn recv(&mut self) -> Option<AlpacaCapturedPublicationInput> {
        self.receiver.recv().await
    }
    pub(crate) fn close(&mut self) {
        self.receiver.close();
    }
}
