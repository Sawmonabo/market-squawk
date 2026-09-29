//! Retained Alpaca raw-custody and canonical-publication worker lifecycle.
use crate::application::{AlpacaMarketPublicationError, AlpacaPublicationRuntimeInput};
use crate::live_source::AlpacaCapturedPublicationReceiver;
use market_squawk_services::ServiceError;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct AlpacaPublicationRuntime {
    input: Arc<AlpacaPublicationRuntimeInput>,
    cancellation: CancellationToken,
    worker: Option<tokio::task::JoinHandle<Result<(), AlpacaPublicationRuntimeError>>>,
    result: Option<Result<(), ServiceError>>,
}
impl AlpacaPublicationRuntime {
    pub(crate) fn start(
        input: AlpacaPublicationRuntimeInput,
        mut receiver: AlpacaCapturedPublicationReceiver,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Self {
        let input = Arc::new(input);
        let owned = Arc::clone(&input);
        let stop = cancellation.clone();
        let worker = tokio::spawn(async move {
            let _cancel_on_exit = stop.clone().drop_guard();
            let mut failure = None;
            loop {
                let next = if stop.is_cancelled() {
                    owned.begin_shutdown();
                    receiver.close();
                    receiver.recv().await
                } else {
                    tokio::select! { biased;
                        ()=stop.cancelled()=> { owned.begin_shutdown();receiver.close();receiver.recv().await }
                        item=receiver.recv()=>item,
                    }
                };
                let Some(item) = next else {
                    break;
                };
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .ok_or(AlpacaPublicationRuntimeError::Bounds);
                let result = match deadline {
                    Ok(deadline) => owned
                        .publish(item.rejoin, item.seal_request, item.observed_at, deadline)
                        .await
                        .map_err(AlpacaPublicationRuntimeError::Publication),
                    Err(error) => Err(error),
                };
                // item._bytes remains owned until publication completes, including error paths.
                drop(item._bytes);
                if let Err(error) = result {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                    owned.begin_shutdown();
                    stop.cancel();
                    receiver.close();
                }
            }
            owned.begin_shutdown();
            owned.finish_shutdown().await;
            stop.cancel();
            failure.map_or(Ok(()), Err)
        });
        Self {
            input,
            cancellation,
            worker: Some(worker),
            result: None,
        }
    }
    pub(crate) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
    }
    pub(crate) fn begin_shutdown(&self) {
        self.input.begin_shutdown();
        self.cancellation.cancel();
    }
    pub(crate) async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        tokio::select! { biased;
            ()=cancellation.cancelled()=>Err(ServiceError::Cancelled),
            ()=tokio::time::sleep_until(deadline.into())=>Err(ServiceError::DeadlineExceeded),
            result=self.finish_retained_shutdown()=>result,
        }
    }
    pub(crate) async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let Some(result) = &self.result {
            return result.clone();
        }
        let result = match self.worker.as_mut() {
            Some(worker) => match worker.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => {
                    tracing::error!(%error,"Alpaca publication worker failed");
                    Err(ServiceError::Unavailable)
                }
                Err(error) => {
                    tracing::error!(%error,"Alpaca publication worker join failed");
                    Err(ServiceError::Unavailable)
                }
            },
            None => Err(ServiceError::Unavailable),
        };
        self.worker.take();
        self.result = Some(result.clone());
        result
    }
}
impl Drop for AlpacaPublicationRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaPublicationRuntimeError {
    #[error("Alpaca publication bounds are invalid")]
    Bounds,
    #[error(transparent)]
    Publication(#[from] AlpacaMarketPublicationError),
}
