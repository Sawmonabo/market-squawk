//! Bounded ownership for the public Coinbase durable-publication handoff.

use std::num::NonZeroUsize;

use market_squawk_adapter_coinbase::{
    CoinbaseMarketHandoff, CoinbaseMarketPublicationContext, CoinbaseMarketSealRejoin,
};
use market_squawk_domain::Timestamp;
use market_squawk_sources::ProviderCaptureSealRequest;
use tokio::sync::mpsc;

/// Exact bounded Coinbase publication input transferred from one owning source generation.
#[derive(Debug)]
pub(in crate::live_source) enum CoinbaseCapturedPublicationInput {
    Public {
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

/// Nonblocking source-side sender installed only by the owning application publication
/// supervisor. Absence is represented once by the common sink publication-ingress enum.
#[derive(Clone, Debug)]
pub(in crate::live_source) struct CoinbaseCapturedPublicationIngress {
    sender: mpsc::Sender<CoinbaseCapturedPublicationInput>,
}

impl CoinbaseCapturedPublicationIngress {
    pub(in crate::live_source) fn try_channel(
        capacity: NonZeroUsize,
    ) -> (Self, CoinbaseCapturedPublicationReceiver) {
        let (sender, receiver) = mpsc::channel(capacity.get());
        (
            Self { sender },
            CoinbaseCapturedPublicationReceiver { receiver },
        )
    }

    pub(in crate::live_source) fn try_submit(
        &self,
        rejoin: CoinbaseMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
    ) -> Result<(), CoinbaseCapturedPublicationInput> {
        let input = CoinbaseCapturedPublicationInput::Public {
            rejoin,
            seal_request,
            observed_at,
        };
        self.sender
            .try_send(input)
            .map_err(mpsc::error::TrySendError::into_inner)
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
        self.sender
            .try_send(input)
            .map_err(mpsc::error::TrySendError::into_inner)
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
