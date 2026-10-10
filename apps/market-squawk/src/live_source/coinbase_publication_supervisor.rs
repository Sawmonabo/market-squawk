//! Owned Coinbase raw/committed publication rendezvous lifecycle.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use market_squawk_adapter_coinbase::CoinbaseMarketNonPublicationReason;
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

use super::super::sink::{
    CoinbaseCapturedPublicationDisposition, CoinbaseCapturedPublicationInput,
    CoinbaseCapturedPublicationReceiver,
};

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
            match &outcome {
                Err(error) => trace_publication_worker_failure("raw", error),
                Ok(()) => tracing::info!(
                    worker = "raw",
                    cancelled = raw_terminal.is_cancelled(),
                    "Coinbase publication worker stopped"
                ),
            }
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
                match &outcome {
                    Err(error) => trace_publication_worker_failure("committed", error),
                    Ok(()) => tracing::info!(
                        worker = "committed",
                        cancelled = committed_terminal.is_cancelled(),
                        "Coinbase publication worker stopped"
                    ),
                }
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
        let mut failure =
            if self.expiry.is_none() || self.raw.is_none() || self.committed.is_empty() {
                Some(CoinbasePublicationSupervisorError::PublicationWorkerOwnership)
            } else {
                None
            };
        // Handles remain owned by self across every suspension. Cancelling this shutdown future
        // therefore still runs Drop's abort path for every task not yet joined.
        if tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.join_workers(&mut failure),
        )
        .await
        .is_err()
        {
            if failure.is_none() {
                failure = Some(CoinbasePublicationSupervisorError::ShutdownDeadline);
            }
            if let Some(task) = self.expiry.as_ref() {
                task.abort();
            }
            if let Some(task) = self.raw.as_ref() {
                task.abort();
            }
            for task in &self.committed {
                task.abort();
            }
            // Only still-owned handles remain. Never await a previously completed JoinHandle
            // again, and never replace the original worker failure with an abort result.
            self.join_workers(&mut failure).await;
        }
        self.authority.take();
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn join_workers(&mut self, failure: &mut Option<CoinbasePublicationSupervisorError>) {
        if let Some(task) = self.expiry.as_mut() {
            let outcome = task.await.map_err(CoinbasePublicationSupervisorError::Task);
            self.expiry.take();
            if failure.is_none() {
                *failure = outcome.err();
            }
        }
        if let Some(task) = self.raw.as_mut() {
            let outcome = task
                .await
                .map_err(CoinbasePublicationSupervisorError::Task)
                .and_then(|outcome| outcome);
            self.raw.take();
            if failure.is_none() {
                *failure = outcome.err();
            }
        }
        while let Some(task) = self.committed.first_mut() {
            let outcome = task
                .await
                .map_err(CoinbasePublicationSupervisorError::Task)
                .and_then(|outcome| outcome);
            // The await has completed; removing this original handle cannot detach a worker.
            drop(self.committed.remove(0));
            if failure.is_none() {
                *failure = outcome.err();
            }
        }
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
    let mut direct_head = None;
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
                    Some(CoinbaseCapturedPublicationInput::Direct { handoff, context, observed_at }) => {
                        // Only cold Direct publication is serial. Live admission never awaits this.
                        tokio::select! {
                            biased;
                            () = cancellation.cancelled() => break,
                            outcome = publish_direct(handoff, context, observed_at, &mut direct_head,
                                &pending, &authority, &durable_writer, &cancellation) => outcome?,
                        }
                    }
                    Some(input) => inflight.push(publish_raw(
                        input,
                        RawPublicationDiagnostic::queued(),
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

// Created at dequeue so Drop also distinguishes an unpolled future from an interrupted stage.
struct RawPublicationDiagnostic {
    queued_at: Instant,
    first_polled_at: Option<Instant>,
    stage_started_at: Instant,
    stage: &'static str,
    completed: bool,
}

impl RawPublicationDiagnostic {
    fn queued() -> Self {
        let queued_at = Instant::now();
        Self {
            queued_at,
            first_polled_at: None,
            stage_started_at: queued_at,
            stage: "queued",
            completed: false,
        }
    }

    fn enter_stage(&mut self, stage: &'static str) {
        self.stage = stage;
        self.stage_started_at = Instant::now();
    }
}

impl Drop for RawPublicationDiagnostic {
    fn drop(&mut self) {
        if !self.completed {
            tracing::warn!(
                stage = self.stage,
                polled = self.first_polled_at.is_some(),
                before_first_poll_ms = ?self.first_polled_at.map(|at| {
                    at.duration_since(self.queued_at).as_millis()
                }),
                total_elapsed_ms = %self.queued_at.elapsed().as_millis(),
                stage_elapsed_ms = %self.stage_started_at.elapsed().as_millis(),
                "Coinbase raw publication interrupted"
            );
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the dequeue diagnostic accompanies the existing exact publication inputs"
)]
async fn publish_raw(
    input: CoinbaseCapturedPublicationInput,
    mut diagnostic: RawPublicationDiagnostic,
    pending: CryptoPendingFrameIngress,
    authority: Arc<CryptoMarketPublicationAuthority>,
    limits: CryptoPublicationRendezvousLimits,
    durable_writer: MarketEventDurableReadWriter,
    cancellation: CancellationToken,
) -> Result<(), CoinbasePublicationSupervisorError> {
    diagnostic.first_polled_at = Some(Instant::now());
    diagnostic.enter_stage("authority");
    authority.validate_precommit()?;
    let publication = authority.publication();
    let (outcome, _frame_admission) = match input {
        CoinbaseCapturedPublicationInput::Public {
            disposition,
            frame_admission,
            rejoin,
            seal_request,
            observed_at,
        } => {
            let deadline = Instant::now()
                .checked_add(limits.frame_timeout())
                .ok_or(CoinbasePublicationSupervisorError::DeadlineRange)?;
            diagnostic.enter_stage("raw_seal");
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
            diagnostic.enter_stage("terminal_rows_or_canonical_publication");
            let outcome = match disposition {
                CoinbaseCapturedPublicationDisposition::AwaitCommittedRows => {
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
                CoinbaseCapturedPublicationDisposition::FreshnessUnqualified => {
                    CoinbaseMarketApplicationOutcome::SealedRaw(
                        material.into_sealed_raw(
                            CoinbaseMarketNonPublicationReason::CanonicalQualificationUnavailable,
                        ).map_err(CryptoMarketPublicationError::from)?,
                    )
                }
            };
            (outcome, frame_admission)
        }
        CoinbaseCapturedPublicationInput::Direct { .. } => {
            return Err(CoinbasePublicationSupervisorError::InvalidTopology);
        }
    };
    // Retain end-to-end admission until the durable read owner has retained this commit too.
    if let CoinbaseMarketApplicationOutcome::Published(receipt) = outcome {
        diagnostic.enter_stage("durable_receipt_retention");
        durable_writer.retain(receipt).await?;
    }
    diagnostic.completed = true;
    Ok(())
}

/// Compact evidence retained only after the exact physical/canonical publication commits.
/// The original snapshot is represented by its coordinate digest, never copied body bytes.
struct DirectCommittedHead {
    snapshot_coordinate: market_squawk_domain::EvidenceDigest,
    terminal: market_squawk_domain::SequenceNumber,
    decoder: market_squawk_sources::DecoderEvidence,
    physical_connection: [u8; 16],
    publication_digest: market_squawk_domain::EvidenceDigest,
}

#[allow(
    clippy::too_many_arguments,
    reason = "existing cold owner retains explicit authority, commit and cancellation ownership"
)]
async fn publish_direct(
    handoff: market_squawk_adapter_coinbase::CoinbaseMarketHandoff,
    mut context: market_squawk_adapter_coinbase::CoinbaseMarketPublicationContext,
    observed_at: market_squawk_domain::Timestamp,
    head: &mut Option<DirectCommittedHead>,
    pending: &CryptoPendingFrameIngress,
    authority: &CryptoMarketPublicationAuthority,
    durable_writer: &MarketEventDurableReadWriter,
    cancellation: &CancellationToken,
) -> Result<(), CoinbasePublicationSupervisorError> {
    use market_squawk_adapter_coinbase::{CoinbaseMarketContinuity, CoinbaseMarketRawLineage};
    authority.validate_precommit()?;
    if cancellation.is_cancelled() {
        return Err(CoinbasePublicationSupervisorError::Cancelled);
    }
    let decoder = handoff.typed_batch().evidence().clone();
    decoder
        .currentness_lease()
        .validate_current()
        .map_err(|_| CoinbasePublicationSupervisorError::CommittedCoordinates)?;
    let terminal =
        market_squawk_domain::SequenceNumber::new(handoff.evidence().continuity().terminal());
    let snapshot_coordinate = match handoff.raw_lineage() {
        CoinbaseMarketRawLineage::DirectInitial(lineage) => {
            if head.as_ref().is_some_and(|previous| {
                previous
                    .decoder
                    .binding()
                    .shares_allocation_with(decoder.binding())
            }) {
                return Err(CoinbasePublicationSupervisorError::CommittedCoordinates);
            }
            lineage.snapshot().receipt().coordinate_digest()
        }
        CoinbaseMarketRawLineage::DirectSuccessor(lineage) => {
            let previous = head
                .as_ref()
                .ok_or(CoinbasePublicationSupervisorError::CommittedCoordinates)?;
            let CoinbaseMarketContinuity::CapturedContiguous { predecessor, .. } =
                handoff.evidence().continuity()
            else {
                return Err(CoinbasePublicationSupervisorError::CommittedCoordinates);
            };
            if previous.terminal != predecessor
                || previous.snapshot_coordinate != lineage.snapshot().coordinate_digest()
                || previous.physical_connection != context.physical().connection_id()
                || previous.decoder.frame_id() != lineage.predecessor().frame_id()
                || previous.decoder.payload_digest() != lineage.predecessor().payload_digest()
                || previous.decoder.received_at() != lineage.predecessor().received_at()
                || !previous
                    .decoder
                    .binding()
                    .shares_allocation_with(decoder.binding())
                || !previous
                    .decoder
                    .currentness_lease()
                    .shares_authority_with(decoder.currentness_lease())
            {
                return Err(CoinbasePublicationSupervisorError::CommittedCoordinates);
            }
            context
                .bind_direct_predecessor(previous.publication_digest)
                .map_err(CryptoMarketPublicationError::from)?;
            previous.snapshot_coordinate
        }
        CoinbaseMarketRawLineage::AdvancedTrade(_) => {
            return Err(CoinbasePublicationSupervisorError::InvalidTopology);
        }
    };
    let physical_connection = context.physical().connection_id();
    let idempotency = coinbase_direct_idempotency_key(&handoff)?;
    let outcome = pending
        .publish_coinbase_direct_when_committed(
            authority.publication().as_ref(),
            handoff,
            context,
            authority.analytical_dataset().clone(),
            idempotency,
            observed_at,
            authority.precommit_authority(),
        )
        .await?;
    let CoinbaseMarketApplicationOutcome::Published(receipt) = outcome else {
        // Raw-only/abstention cannot become a committed predecessor for queued successors.
        return Err(CoinbasePublicationSupervisorError::CommittedCoordinates);
    };
    authority.validate_precommit()?;
    if cancellation.is_cancelled() {
        return Err(CoinbasePublicationSupervisorError::Cancelled);
    }
    let publication_digest = receipt.restart_selector().publication_digest();
    if !durable_writer.retain(receipt).await? {
        return Err(CoinbasePublicationSupervisorError::CommittedCoordinates);
    }
    decoder
        .currentness_lease()
        .validate_current()
        .map_err(|_| CoinbasePublicationSupervisorError::CommittedCoordinates)?;
    *head = Some(DirectCommittedHead {
        snapshot_coordinate,
        terminal,
        decoder,
        physical_connection,
        publication_digest,
    });
    Ok(())
}

fn coinbase_direct_idempotency_key(
    handoff: &market_squawk_adapter_coinbase::CoinbaseMarketHandoff,
) -> Result<String, CryptoMarketPublicationError> {
    let (snapshot, frames) = match handoff.raw_lineage() {
        market_squawk_adapter_coinbase::CoinbaseMarketRawLineage::DirectInitial(lineage) => {
            (lineage.snapshot().receipt(), lineage.replay())
        }
        market_squawk_adapter_coinbase::CoinbaseMarketRawLineage::DirectSuccessor(lineage) => {
            (lineage.snapshot(), lineage.frames())
        }
        _ => return Err(CryptoMarketPublicationError::FamilyMismatch),
    };
    let terminal = frames
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
    let snapshot_identity = match handoff.raw_lineage() {
        market_squawk_adapter_coinbase::CoinbaseMarketRawLineage::DirectInitial(_) => {
            snapshot.body_digest()
        }
        _ => snapshot.coordinate_digest(),
    };
    append_hex(&mut key, snapshot_identity.bytes());
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
        ingress.submit(lease).await?;
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

// Only code-owned variant names cross this diagnostic boundary, never provider material.
fn trace_publication_worker_failure(
    worker: &'static str,
    error: &CoinbasePublicationSupervisorError,
) {
    let category = match error {
        CoinbasePublicationSupervisorError::Cancelled => "cancelled",
        CoinbasePublicationSupervisorError::PublicationWorkerOwnership => "worker_ownership",
        CoinbasePublicationSupervisorError::InvalidTopology => "invalid_topology",
        CoinbasePublicationSupervisorError::Allocation => "allocation",
        CoinbasePublicationSupervisorError::DeadlineRange => "deadline_range",
        CoinbasePublicationSupervisorError::CommittedCoordinates => "committed_coordinates",
        CoinbasePublicationSupervisorError::ShutdownDeadline => "shutdown_deadline",
        CoinbasePublicationSupervisorError::Task(_) => "task",
        CoinbasePublicationSupervisorError::Publication(error) => match error {
            CryptoMarketPublicationError::AuthorityInvalid => "publication_authority_invalid",
            CryptoMarketPublicationError::FamilyMismatch => "publication_family_mismatch",
            CryptoMarketPublicationError::RendezvousUnavailable => {
                "publication_rendezvous_unavailable"
            }
            CryptoMarketPublicationError::Coinbase(error) => {
                // This closed adapter/common error contains only variants and numeric bounds.
                tracing::warn!(?error, "Coinbase canonical publication rejected evidence");
                "publication_coinbase"
            }
            CryptoMarketPublicationError::Kraken(_) => "publication_kraken",
            CryptoMarketPublicationError::Research(_) => "publication_research",
            CryptoMarketPublicationError::Ingest(error) => {
                // Display is closed domain context; never recursively log raw I/O/SQL/provider errors.
                let detail: &dyn std::fmt::Display = match error {
                    market_squawk_data::IngestError::Arrow(inner) => inner,
                    market_squawk_data::IngestError::Catalog(inner) => inner,
                    _ => error,
                };
                if let market_squawk_data::IngestError::Catalog(
                    market_squawk_data::CatalogError::Sqlite(rusqlite::Error::SqliteFailure(
                        code,
                        message,
                    )),
                ) = error
                {
                    // These parameterized writes report engine/constraint context, never bound values.
                    tracing::warn!(
                        ?code,
                        sqlite_message = message.as_deref(),
                        "Coinbase catalog write rejected"
                    );
                }
                tracing::warn!(%error, %detail, "Coinbase canonical ingestion failed");
                "publication_ingest"
            }
            CryptoMarketPublicationError::Capture(_) => "publication_capture",
            CryptoMarketPublicationError::RawCapture(_) => "publication_raw_capture",
            CryptoMarketPublicationError::Rights(_) => "publication_rights",
            CryptoMarketPublicationError::Service(_) => "publication_service",
            CryptoMarketPublicationError::MarketEventRead(_) => "publication_market_event_read",
        },
        CoinbasePublicationSupervisorError::DurableRead(_) => "durable_read",
        CoinbasePublicationSupervisorError::Ingest(_) => "ingest",
        CoinbasePublicationSupervisorError::Authority(_) => "authority",
    };
    tracing::warn!(worker, category, "Coinbase publication worker failed");
}
