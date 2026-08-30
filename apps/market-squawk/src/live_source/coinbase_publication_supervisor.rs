//! Owned Coinbase raw/committed publication rendezvous lifecycle.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use market_squawk_live::CommittedResearchMarketObservationReceiver;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::application::{
    CoinbaseMarketApplicationOutcome, CryptoCommittedRowIngress, CryptoMarketPublicationAuthority,
    CryptoMarketPublicationError, CryptoPendingFrameIngress, CryptoPublicationRendezvousLimits,
    MarketEventDurableReadWriter, MarketEventReadError,
};
use crate::provider_activation::CoinbaseMarketPublicationPackage;

use super::super::sink::{CoinbaseCapturedPublicationInput, CoinbaseCapturedPublicationReceiver};

/// Sole application-owned lifecycle for one Coinbase Advanced Trade public generation.
#[derive(Debug)]
pub(in crate::live_source) struct CoinbasePublicationSupervisor {
    cancellation: CancellationToken,
    expiry: Option<JoinHandle<()>>,
    raw: Option<JoinHandle<Result<(), CoinbasePublicationSupervisorError>>>,
    committed: Vec<JoinHandle<Result<(), CoinbasePublicationSupervisorError>>>,
    authority: Option<Arc<CryptoMarketPublicationAuthority>>,
}

impl CoinbasePublicationSupervisor {
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact source authority and independently bounded handoffs remain explicit"
    )]
    pub(in crate::live_source) fn start(
        package: CoinbaseMarketPublicationPackage,
        mut raw_frames: CoinbaseCapturedPublicationReceiver,
        committed_rows: Vec<CommittedResearchMarketObservationReceiver>,
        maximum_inflight: NonZeroUsize,
        limits: CryptoPublicationRendezvousLimits,
        cancellation: CancellationToken,
    ) -> Result<Self, CoinbasePublicationSupervisorError> {
        if cancellation.is_cancelled() {
            return Err(CoinbasePublicationSupervisorError::Cancelled);
        }
        if committed_rows.is_empty() {
            return Err(CoinbasePublicationSupervisorError::InvalidTopology);
        }
        let (authority, durable_writer) = package.into_parts();
        authority.validate_precommit()?;
        let (pending, committed) =
            CryptoPendingFrameIngress::try_new(limits, cancellation.clone())?;

        let mut committed_tasks = Vec::new();
        committed_tasks
            .try_reserve_exact(committed_rows.len())
            .map_err(|_| CoinbasePublicationSupervisorError::Allocation)?;

        let expiry_pending = pending.clone();
        let expiry = tokio::spawn(async move { expiry_pending.run_expiry_driver().await });

        let raw_cancellation = cancellation.clone();
        let raw_terminal = cancellation.clone();
        let raw_pending = pending.clone();
        let raw_authority = Arc::clone(&authority);
        let raw = tokio::spawn(async move {
            let outcome = run_raw_worker(
                &mut raw_frames,
                raw_pending,
                raw_authority,
                maximum_inflight,
                limits,
                durable_writer,
                raw_cancellation,
            )
            .await;
            raw_terminal.cancel();
            outcome
        });

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
            authority: Some(authority),
        })
    }

    pub(in crate::live_source) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self.expiry.as_ref().is_some_and(|task| !task.is_finished())
            && self.raw.as_ref().is_some_and(|task| !task.is_finished())
            && !self.committed.is_empty()
            && self.committed.iter().all(|task| !task.is_finished())
            && self.authority.is_some()
    }

    pub(in crate::live_source) async fn shutdown(
        mut self,
        deadline: Instant,
    ) -> Result<(), CoinbasePublicationSupervisorError> {
        self.cancellation.cancel();
        let mut expiry = self
            .expiry
            .take()
            .ok_or(CoinbasePublicationSupervisorError::PublicationWorkerOwnership)?;
        let mut raw = self
            .raw
            .take()
            .ok_or(CoinbasePublicationSupervisorError::PublicationWorkerOwnership)?;
        let mut committed = std::mem::take(&mut self.committed);
        if committed.is_empty() {
            return Err(CoinbasePublicationSupervisorError::PublicationWorkerOwnership);
        }
        let joined = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            (&mut expiry)
                .await
                .map_err(CoinbasePublicationSupervisorError::Task)?;
            (&mut raw)
                .await
                .map_err(CoinbasePublicationSupervisorError::Task)??;
            for task in &mut committed {
                task.await
                    .map_err(CoinbasePublicationSupervisorError::Task)??;
            }
            Ok::<(), CoinbasePublicationSupervisorError>(())
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
                return Err(CoinbasePublicationSupervisorError::ShutdownDeadline);
            }
        }
        self.authority.take();
        Ok(())
    }
}

impl Drop for CoinbasePublicationSupervisor {
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
    reason = "the sole raw worker retains every publication authority and bound explicitly"
)]
async fn run_raw_worker(
    receiver: &mut CoinbaseCapturedPublicationReceiver,
    pending: CryptoPendingFrameIngress,
    authority: Arc<CryptoMarketPublicationAuthority>,
    maximum_inflight: NonZeroUsize,
    limits: CryptoPublicationRendezvousLimits,
    durable_writer: MarketEventDurableReadWriter,
    cancellation: CancellationToken,
) -> Result<(), CoinbasePublicationSupervisorError> {
    let mut open = true;
    let mut inflight = FuturesUnordered::new();
    while open || !inflight.is_empty() {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            outcome = inflight.next(), if !inflight.is_empty() => {
                if let Some(outcome) = outcome {
                    outcome?;
                }
            },
            input = receiver.recv(), if open && inflight.len() < maximum_inflight.get() => {
                match input {
                    Some(input) => inflight.push(publish_raw(
                        input,
                        pending.clone(),
                        Arc::clone(&authority),
                        limits,
                        durable_writer.clone(),
                        cancellation.clone(),
                    )),
                    None => open = false,
                }
            },
        }
    }
    receiver.close();
    while let Ok(_discarded) = receiver.try_recv() {}
    Ok(())
}

async fn publish_raw(
    input: CoinbaseCapturedPublicationInput,
    pending: CryptoPendingFrameIngress,
    authority: Arc<CryptoMarketPublicationAuthority>,
    limits: CryptoPublicationRendezvousLimits,
    durable_writer: MarketEventDurableReadWriter,
    cancellation: CancellationToken,
) -> Result<(), CoinbasePublicationSupervisorError> {
    authority.validate_precommit()?;
    let publication = authority.publication();
    let outcome = match input {
        CoinbaseCapturedPublicationInput::Public {
            rejoin,
            seal_request,
            observed_at,
        } => {
            let deadline = Instant::now()
                .checked_add(limits.frame_timeout())
                .ok_or(CoinbasePublicationSupervisorError::DeadlineRange)?;
            let material = publication
                .seal_coinbase_public(
                    rejoin,
                    seal_request,
                    observed_at,
                    authority.precommit_authority(),
                    cancellation,
                    deadline,
                )
                .await?;
            let idempotency = coinbase_idempotency_key(&material)?;
            pending
                .publish_coinbase_when_committed(
                    publication.as_ref(),
                    material,
                    authority.analytical_dataset().clone(),
                    idempotency,
                    observed_at,
                    authority.precommit_authority(),
                )
                .await?
        }
        CoinbaseCapturedPublicationInput::Direct {
            handoff,
            context,
            observed_at,
        } => {
            let idempotency = coinbase_direct_idempotency_key(&handoff)?;
            pending
                .publish_coinbase_direct_when_committed(
                    publication.as_ref(),
                    handoff,
                    context,
                    authority.analytical_dataset().clone(),
                    idempotency,
                    observed_at,
                    authority.precommit_authority(),
                )
                .await?
        }
    };
    if let CoinbaseMarketApplicationOutcome::Published(receipt) = outcome {
        durable_writer.retain(receipt).await?;
    }
    Ok(())
}

fn coinbase_direct_idempotency_key(
    handoff: &market_squawk_adapter_coinbase::CoinbaseMarketHandoff,
) -> Result<String, CryptoMarketPublicationError> {
    let market_squawk_adapter_coinbase::CoinbaseMarketRawLineage::DirectInitial(lineage) =
        handoff.raw_lineage()
    else {
        return Err(CryptoMarketPublicationError::FamilyMismatch);
    };
    let snapshot = lineage.snapshot().receipt();
    let terminal = lineage
        .replay()
        .last()
        .ok_or(CryptoMarketPublicationError::FamilyMismatch)?;
    let source = handoff
        .typed_batch()
        .evidence()
        .binding()
        .source_id()
        .as_str();
    let generation = handoff
        .typed_batch()
        .evidence()
        .binding()
        .connection_generation()
        .get();
    let mut key = String::with_capacity(35 + source.len() + 20 + 64 + 20 + 64);
    key.push_str("coinbase-direct-response-event-v1-");
    key.push_str(source);
    key.push('-');
    key.push_str(&generation.to_string());
    key.push('-');
    append_hex(&mut key, snapshot.body_digest().bytes());
    key.push('-');
    key.push_str(&terminal.sequence().get().to_string());
    key.push('-');
    append_hex(
        &mut key,
        terminal.decoder_evidence().payload_digest().bytes(),
    );
    Ok(key)
}

async fn run_committed_worker(
    receiver: &mut CommittedResearchMarketObservationReceiver,
    ingress: CryptoCommittedRowIngress,
    cancellation: CancellationToken,
) -> Result<(), CoinbasePublicationSupervisorError> {
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
            .ok_or(CoinbasePublicationSupervisorError::CommittedCoordinates)?;
        ingress.submit(wire_ordinal, row_count, lease).await?;
    }
    while let Ok(_discarded) = receiver.try_recv() {}
    Ok(())
}

fn coinbase_idempotency_key(
    material: &market_squawk_adapter_coinbase::CoinbaseMarketSealRejoin,
) -> Result<String, CryptoMarketPublicationError> {
    let source = material.source_id().as_str();
    let generation = material.connection_generation().get();
    let frame = material.frame_id()?.get();
    let digest = material.raw_payload_digest();
    let mut key = String::with_capacity(31 + source.len() + 20 + 20 + 64);
    key.push_str("coinbase-public-frame-v1-");
    key.push_str(source);
    key.push('-');
    key.push_str(&generation.to_string());
    key.push('-');
    key.push_str(&frame.to_string());
    key.push('-');
    append_hex(&mut key, digest.bytes());
    Ok(key)
}

fn append_hex<const N: usize>(output: &mut String, bytes: [u8; N]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
}

#[derive(Debug, Error)]
pub(in crate::live_source) enum CoinbasePublicationSupervisorError {
    #[error("Coinbase publication startup was cancelled")]
    Cancelled,
    #[error("Coinbase publication worker ownership is unavailable")]
    PublicationWorkerOwnership,
    #[error("Coinbase publication topology is invalid")]
    InvalidTopology,
    #[error("Coinbase publication worker allocation failed")]
    Allocation,
    #[error("Coinbase publication deadline cannot be represented")]
    DeadlineRange,
    #[error("Coinbase committed observation coordinates are invalid")]
    CommittedCoordinates,
    #[error("Coinbase publication worker exceeded its shutdown deadline")]
    ShutdownDeadline,
    #[error("Coinbase publication worker task failed: {0}")]
    Task(tokio::task::JoinError),
    #[error(transparent)]
    Publication(#[from] CryptoMarketPublicationError),
    #[error(transparent)]
    DurableRead(#[from] MarketEventReadError),
    #[error(transparent)]
    Ingest(#[from] market_squawk_data::IngestError),
    #[error(transparent)]
    Authority(#[from] crate::application::ResearchIngestCompositionError),
}
