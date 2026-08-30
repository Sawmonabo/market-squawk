//! Owned Kraken raw/committed publication rendezvous lifecycle.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use market_squawk_live::CommittedResearchMarketObservationReceiver;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    application::{
        CryptoCommittedRowIngress, CryptoMarketDurableRead, CryptoMarketDurableReadWriter,
        CryptoMarketPublicationAuthority, CryptoMarketPublicationError, CryptoPendingFrameIngress,
        CryptoPublicationRendezvousLimits, KrakenMarketApplicationOutcome,
    },
    provider_activation::KrakenMarketPublicationPackage,
};

use super::super::kraken_publication::{
    KrakenCapturedPublicationInput, KrakenCapturedPublicationReceiver,
};

#[derive(Debug)]
pub(super) struct KrakenPublicationSupervisor {
    cancellation: CancellationToken,
    expiry: Option<JoinHandle<()>>,
    raw: Option<JoinHandle<Result<(), KrakenPublicationSupervisorError>>>,
    committed: Vec<JoinHandle<Result<(), KrakenPublicationSupervisorError>>>,
    book: Option<Arc<CryptoMarketPublicationAuthority>>,
    trades: Option<Arc<CryptoMarketPublicationAuthority>>,
    durable_reads: [CryptoMarketDurableRead; 2],
}

impl KrakenPublicationSupervisor {
    #[allow(
        clippy::too_many_arguments,
        reason = "both exact source lanes and independently bounded handoffs remain explicit"
    )]
    pub(super) fn start(
        package: KrakenMarketPublicationPackage,
        mut book_raw: KrakenCapturedPublicationReceiver,
        mut trade_raw: KrakenCapturedPublicationReceiver,
        committed_rows: Vec<CommittedResearchMarketObservationReceiver>,
        maximum_inflight: NonZeroUsize,
        limits: CryptoPublicationRendezvousLimits,
        cancellation: CancellationToken,
    ) -> Result<Self, KrakenPublicationSupervisorError> {
        if cancellation.is_cancelled() {
            return Err(KrakenPublicationSupervisorError::Cancelled);
        }
        if committed_rows.is_empty() {
            return Err(KrakenPublicationSupervisorError::InvalidTopology);
        }
        let (pending, committed) =
            CryptoPendingFrameIngress::try_new(limits, cancellation.clone())?;
        let (book, trades) = package.into_parts();
        let (book_durable_writer, book_durable_read) = book.durable_read_capability();
        let (trade_durable_writer, trade_durable_read) = trades.durable_read_capability();

        let expiry_pending = pending.clone();
        let expiry = tokio::spawn(async move { expiry_pending.run_expiry_driver().await });

        let raw_cancellation = cancellation.clone();
        let raw_terminal = cancellation.clone();
        let raw_pending = pending.clone();
        let raw_book = Arc::clone(&book);
        let raw_trades = Arc::clone(&trades);
        let raw = tokio::spawn(async move {
            let outcome = run_raw_worker(
                &mut book_raw,
                &mut trade_raw,
                raw_pending,
                raw_book,
                raw_trades,
                maximum_inflight,
                limits,
                book_durable_writer,
                trade_durable_writer,
                raw_cancellation,
            )
            .await;
            raw_terminal.cancel();
            outcome
        });

        let mut committed_tasks = Vec::new();
        committed_tasks
            .try_reserve_exact(committed_rows.len())
            .map_err(|_| KrakenPublicationSupervisorError::Allocation)?;
        for mut receiver in committed_rows {
            let committed_ingress = committed.clone();
            let committed_cancellation = cancellation.clone();
            let committed_terminal = cancellation.clone();
            committed_tasks.push(tokio::spawn(async move {
                let outcome =
                    run_committed_worker(&mut receiver, committed_ingress, committed_cancellation)
                        .await;
                committed_terminal.cancel();
                outcome
            }));
        }

        Ok(Self {
            cancellation,
            expiry: Some(expiry),
            raw: Some(raw),
            committed: committed_tasks,
            book: Some(book),
            trades: Some(trades),
            durable_reads: [book_durable_read, trade_durable_read],
        })
    }

    pub(super) const fn durable_read_count(&self) -> usize {
        self.durable_reads.len()
    }

    pub(super) fn append_durable_reads(&self, destination: &mut Vec<CryptoMarketDurableRead>) {
        destination.extend(self.durable_reads.iter().cloned());
    }

    pub(super) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.expiry.as_ref().is_some_and(|task| !task.is_finished())
            && self.raw.as_ref().is_some_and(|task| !task.is_finished())
            && !self.committed.is_empty()
            && self.committed.iter().all(|task| !task.is_finished())
            && self.book.is_some()
            && self.trades.is_some()
    }

    pub(super) async fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<(), KrakenPublicationSupervisorError> {
        self.cancellation.cancel();
        let mut expiry = self
            .expiry
            .take()
            .ok_or(KrakenPublicationSupervisorError::PublicationWorkerOwnership)?;
        let mut raw = self
            .raw
            .take()
            .ok_or(KrakenPublicationSupervisorError::PublicationWorkerOwnership)?;
        let mut committed = std::mem::take(&mut self.committed);
        if committed.is_empty() {
            return Err(KrakenPublicationSupervisorError::PublicationWorkerOwnership);
        }
        let joined = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            (&mut expiry)
                .await
                .map_err(KrakenPublicationSupervisorError::Task)?;
            (&mut raw)
                .await
                .map_err(KrakenPublicationSupervisorError::Task)??;
            for task in &mut committed {
                task.await
                    .map_err(KrakenPublicationSupervisorError::Task)??;
            }
            Ok::<(), KrakenPublicationSupervisorError>(())
        })
        .await;
        match joined {
            Ok(outcome) => outcome?,
            Err(_elapsed) => {
                expiry.abort();
                raw.abort();
                for task in &committed {
                    task.abort();
                }
                let _expiry = expiry.await;
                let _raw = raw.await;
                for task in committed {
                    let _committed = task.await;
                }
                return Err(KrakenPublicationSupervisorError::ShutdownDeadline);
            }
        }
        // Dropping these exact-generation packages occurs only after all publication users join.
        self.book.take();
        self.trades.take();
        Ok(())
    }
}

impl Drop for KrakenPublicationSupervisor {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.expiry.as_ref() {
            task.abort();
        }
        if let Some(task) = self.raw.as_ref() {
            task.abort();
        }
        for task in &self.committed {
            task.abort();
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the sole raw worker multiplexes both exact Kraken source lanes"
)]
async fn run_raw_worker(
    book_receiver: &mut KrakenCapturedPublicationReceiver,
    trade_receiver: &mut KrakenCapturedPublicationReceiver,
    pending: CryptoPendingFrameIngress,
    book: Arc<CryptoMarketPublicationAuthority>,
    trades: Arc<CryptoMarketPublicationAuthority>,
    maximum_inflight: NonZeroUsize,
    limits: CryptoPublicationRendezvousLimits,
    book_durable_writer: CryptoMarketDurableReadWriter,
    trade_durable_writer: CryptoMarketDurableReadWriter,
    cancellation: CancellationToken,
) -> Result<(), KrakenPublicationSupervisorError> {
    let mut book_open = true;
    let mut trades_open = true;
    let mut inflight = FuturesUnordered::new();
    while book_open || trades_open || !inflight.is_empty() {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            outcome = inflight.next(), if !inflight.is_empty() => {
                if let Some(outcome) = outcome {
                    outcome?;
                }
            },
            input = book_receiver.recv(), if book_open && inflight.len() < maximum_inflight.get() => {
                match input {
                    Some(input) => inflight.push(publish_raw(
                        input,
                        pending.clone(),
                        Arc::clone(&book),
                        limits,
                        book_durable_writer.clone(),
                        cancellation.clone(),
                    )),
                    None => book_open = false,
                }
            },
            input = trade_receiver.recv(), if trades_open && inflight.len() < maximum_inflight.get() => {
                match input {
                    Some(input) => inflight.push(publish_raw(
                        input,
                        pending.clone(),
                        Arc::clone(&trades),
                        limits,
                        trade_durable_writer.clone(),
                        cancellation.clone(),
                    )),
                    None => trades_open = false,
                }
            },
        }
    }
    book_receiver.close();
    trade_receiver.close();
    while let Ok(_discarded) = book_receiver.try_recv() {}
    while let Ok(_discarded) = trade_receiver.try_recv() {}
    Ok(())
}

async fn publish_raw(
    input: KrakenCapturedPublicationInput,
    pending: CryptoPendingFrameIngress,
    authority: Arc<CryptoMarketPublicationAuthority>,
    limits: CryptoPublicationRendezvousLimits,
    durable_writer: CryptoMarketDurableReadWriter,
    cancellation: CancellationToken,
) -> Result<(), KrakenPublicationSupervisorError> {
    authority.validate_precommit()?;
    let deadline = Instant::now()
        .checked_add(limits.frame_timeout())
        .ok_or(KrakenPublicationSupervisorError::DeadlineRange)?;
    let (raw, observed_at) = input.into_parts();
    let publication = authority.publication();
    let outcome = publication
        .seal_kraken(
            raw,
            observed_at,
            authority.precommit_authority(),
            cancellation,
            deadline,
        )
        .await?;
    let KrakenMarketApplicationOutcome::CanonicalUnavailable(unavailable) = outcome else {
        return Ok(());
    };
    let material = unavailable.into_material();
    let idempotency = kraken_idempotency_key(&material);
    let outcome = pending
        .publish_when_committed(
            publication.as_ref(),
            material,
            authority.analytical_dataset().clone(),
            idempotency,
            observed_at,
            authority.precommit_authority(),
        )
        .await?;
    if let KrakenMarketApplicationOutcome::Published(receipt) = outcome {
        durable_writer.retain(receipt).await?;
    }
    Ok(())
}

async fn run_committed_worker(
    receiver: &mut CommittedResearchMarketObservationReceiver,
    ingress: CryptoCommittedRowIngress,
    cancellation: CancellationToken,
) -> Result<(), KrakenPublicationSupervisorError> {
    loop {
        let lease = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            lease = receiver.recv() => match lease {
                Some(lease) => lease,
                None => break,
            },
        };
        let wire_ordinal = lease.observation().wire_ordinal();
        let row_count = NonZeroUsize::new(lease.observation().row_count())
            .ok_or(KrakenPublicationSupervisorError::CommittedCoordinates)?;
        ingress.submit(wire_ordinal, row_count, lease).await?;
    }
    while let Ok(_discarded) = receiver.try_recv() {}
    Ok(())
}

fn kraken_idempotency_key(
    material: &market_squawk_adapter_kraken::KrakenSealedMarketPublicationMaterial,
) -> String {
    let evidence = material.evidence();
    let digest = evidence.raw_payload_digest();
    let mut key = String::with_capacity(28 + evidence.source_id().as_str().len() + 32 + 32 + 64);
    key.push_str("kraken-frame-v2-");
    key.push_str(evidence.source_id().as_str());
    key.push('-');
    append_hex(&mut key, evidence.connection_id());
    key.push('-');
    append_hex(&mut key, evidence.event_id());
    key.push('-');
    append_hex(&mut key, digest.bytes());
    key
}

fn append_hex<const N: usize>(output: &mut String, bytes: [u8; N]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

#[derive(Debug, Error)]
pub(super) enum KrakenPublicationSupervisorError {
    #[error("Kraken publication startup was cancelled")]
    Cancelled,
    #[error("Kraken publication worker ownership is unavailable")]
    PublicationWorkerOwnership,
    #[error("Kraken publication topology is invalid")]
    InvalidTopology,
    #[error("Kraken publication worker allocation failed")]
    Allocation,
    #[error("Kraken publication deadline cannot be represented")]
    DeadlineRange,
    #[error("Kraken committed observation coordinates are invalid")]
    CommittedCoordinates,
    #[error("Kraken publication worker exceeded its shutdown deadline")]
    ShutdownDeadline,
    #[error("Kraken publication worker task failed: {0}")]
    Task(tokio::task::JoinError),
    #[error(transparent)]
    Publication(#[from] CryptoMarketPublicationError),
    #[error(transparent)]
    Authority(#[from] crate::application::ResearchIngestCompositionError),
}
