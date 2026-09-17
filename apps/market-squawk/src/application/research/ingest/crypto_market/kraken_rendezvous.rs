//! Bounded one-use rendezvous between sealed crypto frames and committed live observations.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use market_squawk_adapter_coinbase::{
    CoinbaseMarketHandoff, CoinbaseMarketNonPublicationReason, CoinbaseMarketOmission,
    CoinbaseMarketOmissionReason, CoinbaseMarketPublicationContext,
    CoinbaseMarketQualificationOutcome, CoinbaseMarketRawLineage, CoinbaseMarketSealRejoin,
    CoinbaseQualifiedDirectReplayRow, CoinbaseQualifiedMarketPublication,
};
use market_squawk_adapter_kraken::{
    KrakenPublicationUnavailable, KrakenSealedMarketPublicationMaterial,
};
use market_squawk_domain::{ConnectionGeneration, EvidenceDigest, SourceId};
use market_squawk_live::CommittedResearchMarketObservationLease;
use market_squawk_sources::MAX_DECODED_EVENTS;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;

use super::{
    CoinbaseMarketApplicationOutcome, CryptoMarketPublicationClosure, CryptoMarketPublicationError,
    KrakenMarketApplicationOutcome, KrakenSealedRawCanonicalUnavailable,
};

/// Fixed memory and concurrency admission for one source-bound crypto rendezvous.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CryptoPublicationRendezvousLimits {
    maximum_pending_frames: NonZeroUsize,
    maximum_retained_bytes: NonZeroUsize,
    frame_timeout: Duration,
}

impl CryptoPublicationRendezvousLimits {
    pub(crate) const fn new(
        maximum_pending_frames: NonZeroUsize,
        maximum_retained_bytes: NonZeroUsize,
        frame_timeout: Duration,
    ) -> Self {
        Self {
            maximum_pending_frames,
            maximum_retained_bytes,
            frame_timeout,
        }
    }

    pub(crate) const fn frame_timeout(self) -> Duration {
        self.frame_timeout
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SourceObjectKey {
    source_id: SourceId,
    generation: ConnectionGeneration,
    coordinate: SourceObjectCoordinate,
    raw_payload_digest: EvidenceDigest,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SourceObjectCoordinate {
    TransportFrame(u64),
    HttpResponse(EvidenceDigest),
}

impl SourceObjectKey {
    fn from_material(material: &KrakenSealedMarketPublicationMaterial) -> Self {
        let evidence = material.evidence();
        Self {
            source_id: evidence.source_id().clone(),
            generation: evidence.connection_generation(),
            coordinate: SourceObjectCoordinate::TransportFrame(evidence.generation_frame_ordinal()),
            raw_payload_digest: evidence.raw_payload_digest(),
        }
    }

    fn from_coinbase(
        material: &CoinbaseMarketSealRejoin,
    ) -> Result<Self, CryptoMarketPublicationError> {
        Ok(Self {
            source_id: material.source_id().clone(),
            generation: material.connection_generation(),
            coordinate: SourceObjectCoordinate::TransportFrame(material.frame_id()?.get()),
            raw_payload_digest: material.raw_payload_digest(),
        })
    }

    fn direct(
        handoff: &CoinbaseMarketHandoff,
    ) -> Result<(Self, Self, usize), CryptoMarketPublicationError> {
        let CoinbaseMarketRawLineage::DirectInitial(lineage) = handoff.raw_lineage() else {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        };
        let decoder = handoff.typed_batch().evidence();
        let snapshot = lineage.snapshot().receipt();
        let terminal = lineage
            .replay()
            .last()
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        if terminal.decoder_evidence().frame_id() != decoder.frame_id()
            || terminal.decoder_evidence().payload_digest() != decoder.payload_digest()
            || !terminal
                .decoder_evidence()
                .binding()
                .shares_allocation_with(decoder.binding())
            || !snapshot.binding().shares_allocation_with(decoder.binding())
        {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        Ok((
            Self {
                source_id: snapshot.source_id().clone(),
                generation: snapshot.connection_generation(),
                coordinate: SourceObjectCoordinate::HttpResponse(snapshot.coordinate_digest()),
                raw_payload_digest: snapshot.body_digest(),
            },
            Self {
                source_id: decoder.binding().source_id().clone(),
                generation: decoder.binding().connection_generation(),
                coordinate: SourceObjectCoordinate::TransportFrame(decoder.frame_id().get()),
                raw_payload_digest: decoder.payload_digest(),
            },
            lineage.replay().len(),
        ))
    }

    fn from_lease(
        lease: &CommittedResearchMarketObservationLease,
    ) -> Result<Self, CryptoMarketPublicationError> {
        let observation = lease.observation();
        let binding = observation.qualification().binding();
        Ok(Self {
            source_id: binding.source_id().clone(),
            generation: observation.connection_generation(),
            coordinate: match observation.source_coordinate().evidence() {
                market_squawk_sources::CurrentObservationEvidence::TransportFrame(evidence) => {
                    SourceObjectCoordinate::TransportFrame(evidence.frame_id().get())
                }
                market_squawk_sources::CurrentObservationEvidence::HttpResponse(evidence) => {
                    SourceObjectCoordinate::HttpResponse(evidence.receipt().coordinate_digest())
                }
            },
            raw_payload_digest: binding.payload_digest(),
        })
    }
}

struct PendingRows {
    rows: Vec<Option<CommittedResearchMarketObservationLease>>,
    retained_bytes: usize,
}

#[derive(Default)]
struct State {
    rows: HashMap<SourceObjectKey, PendingRows>,
    sealed: HashMap<SourceObjectKey, usize>,
    deadlines: HashMap<SourceObjectKey, Instant>,
    retained_bytes: usize,
}

struct Core {
    limits: CryptoPublicationRendezvousLimits,
    state: Mutex<State>,
    changed: Notify,
    cancellation: CancellationToken,
}

/// One-use sealed-frame side. It alone returns the final durable or unavailable outcome.
#[derive(Clone)]
pub(crate) struct CryptoPendingFrameIngress {
    core: Arc<Core>,
}

/// One-use committed-row side drained from the exact instrument-owned research export.
#[derive(Clone)]
pub(crate) struct CryptoCommittedRowIngress {
    core: Arc<Core>,
}

impl CryptoPendingFrameIngress {
    pub(crate) fn try_new(
        limits: CryptoPublicationRendezvousLimits,
        cancellation: CancellationToken,
    ) -> Result<(Self, CryptoCommittedRowIngress), CryptoMarketPublicationError> {
        if limits.frame_timeout.is_zero() {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        let mut rows = HashMap::new();
        rows.try_reserve(limits.maximum_pending_frames.get())
            .map_err(|_| CryptoMarketPublicationError::RendezvousUnavailable)?;
        let mut sealed = HashMap::new();
        sealed
            .try_reserve(limits.maximum_pending_frames.get())
            .map_err(|_| CryptoMarketPublicationError::RendezvousUnavailable)?;
        let mut deadlines = HashMap::new();
        deadlines
            .try_reserve(limits.maximum_pending_frames.get())
            .map_err(|_| CryptoMarketPublicationError::RendezvousUnavailable)?;
        let core = Arc::new(Core {
            limits,
            state: Mutex::new(State {
                rows,
                sealed,
                deadlines,
                retained_bytes: 0,
            }),
            changed: Notify::new(),
            cancellation,
        });
        Ok((
            Self {
                core: Arc::clone(&core),
            },
            CryptoCommittedRowIngress { core },
        ))
    }

    /// Runs the sole deadline-eviction loop. The production supervisor owns and joins this
    /// future; the rendezvous never creates detached timeout tasks.
    pub(crate) async fn run_expiry_driver(&self) {
        loop {
            let next_deadline = {
                let state = self.core.state.lock().await;
                state.deadlines.values().copied().min()
            };
            match next_deadline {
                Some(deadline) => tokio::select! {
                    biased;
                    () = self.core.cancellation.cancelled() => break,
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        self.expire_due(Instant::now()).await;
                    }
                    () = self.core.changed.notified() => {}
                },
                None => tokio::select! {
                    biased;
                    () = self.core.cancellation.cancelled() => break,
                    () = self.core.changed.notified() => {}
                },
            }
        }
        self.clear_all().await;
    }

    async fn expire_due(&self, now: Instant) {
        let mut state = self.core.state.lock().await;
        while let Some(key) = state
            .deadlines
            .iter()
            .find_map(|(key, deadline)| (*deadline <= now).then_some(key.clone()))
        {
            if let Some(pending) = state.rows.remove(&key) {
                state.retained_bytes = state.retained_bytes.saturating_sub(pending.retained_bytes);
            }
            if let Some(retained_bytes) = state.sealed.remove(&key) {
                state.retained_bytes = state.retained_bytes.saturating_sub(retained_bytes);
            }
            state.deadlines.remove(&key);
        }
        drop(state);
        self.core.changed.notify_waiters();
    }

    async fn clear_all(&self) {
        let mut state = self.core.state.lock().await;
        *state = State::default();
        drop(state);
        self.core.changed.notify_waiters();
    }

    pub(crate) async fn publish_when_committed(
        &self,
        publication: &CryptoMarketPublicationClosure,
        material: KrakenSealedMarketPublicationMaterial,
        analytical_dataset: market_squawk_data::DatasetId,
        idempotency_key: String,
        observed_at: market_squawk_domain::Timestamp,
        precommit_authority: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
    ) -> Result<KrakenMarketApplicationOutcome, CryptoMarketPublicationError> {
        let key = SourceObjectKey::from_material(&material);
        let Some(retained_bytes) = material.conservative_retained_bytes() else {
            return Ok(unavailable(
                material,
                KrakenPublicationUnavailable::ApplicationBackpressure,
            ));
        };
        let Some(rows) = self
            .wait_for_rows(key, material.observations().len(), retained_bytes)
            .await
        else {
            return Ok(unavailable(
                material,
                KrakenPublicationUnavailable::ApplicationBackpressure,
            ));
        };
        let rows = rows
            .into_iter()
            .map(CommittedResearchMarketObservationLease::into_observation)
            .collect();
        publication
            .publish_kraken_joined(
                material,
                rows,
                analytical_dataset,
                idempotency_key,
                observed_at,
                precommit_authority,
                self.core.cancellation.clone(),
            )
            .await
    }

    pub(crate) async fn publish_coinbase_when_committed(
        &self,
        publication: &CryptoMarketPublicationClosure,
        material: CoinbaseMarketSealRejoin,
        analytical_dataset: market_squawk_data::DatasetId,
        idempotency_key: String,
        observed_at: market_squawk_domain::Timestamp,
        precommit_authority: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
    ) -> Result<CoinbaseMarketApplicationOutcome, CryptoMarketPublicationError> {
        let key = SourceObjectKey::from_coinbase(&material)?;
        let expected = material.expected_row_count();
        let Some(retained_bytes) = material.conservative_retained_bytes() else {
            return Ok(CoinbaseMarketApplicationOutcome::SealedRaw(
                material
                    .into_sealed_raw(CoinbaseMarketNonPublicationReason::ApplicationBackpressure)?,
            ));
        };
        let Some(rows) = self.wait_for_rows(key, expected, retained_bytes).await else {
            return Ok(CoinbaseMarketApplicationOutcome::SealedRaw(
                material
                    .into_sealed_raw(CoinbaseMarketNonPublicationReason::ApplicationBackpressure)?,
            ));
        };
        let rows = rows
            .into_iter()
            .map(CommittedResearchMarketObservationLease::into_observation)
            .collect();
        publication
            .publish_coinbase_public_joined(
                material,
                rows,
                analytical_dataset,
                idempotency_key,
                observed_at,
                precommit_authority,
                self.core.cancellation.clone(),
            )
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exact Direct raw graph, committed source objects, authority, and deadline remain explicit"
    )]
    pub(crate) async fn publish_coinbase_direct_when_committed(
        &self,
        publication: &CryptoMarketPublicationClosure,
        handoff: CoinbaseMarketHandoff,
        context: CoinbaseMarketPublicationContext,
        analytical_dataset: market_squawk_data::DatasetId,
        idempotency_key: String,
        observed_at: market_squawk_domain::Timestamp,
        precommit_authority: Arc<dyn market_squawk_data::IngestPrecommitAuthority>,
    ) -> Result<CoinbaseMarketApplicationOutcome, CryptoMarketPublicationError> {
        let (snapshot_key, terminal_key, replay_count) = SourceObjectKey::direct(&handoff)?;
        let retained_bytes = direct_retained_bytes(&handoff)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        let snapshot = self.wait_for_rows(snapshot_key, 1, retained_bytes).await;
        let terminal = self.wait_for_rows(terminal_key, 1, retained_bytes).await;
        let qualification = match (snapshot, terminal) {
            (Some(mut snapshot), Some(mut terminal)) => {
                let initial_snapshot = snapshot
                    .pop()
                    .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?
                    .into_observation()
                    .into_parts()
                    .event;
                let replay = terminal
                    .pop()
                    .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?
                    .into_observation()
                    .into_parts()
                    .event;
                let terminal_ordinal = replay_count
                    .checked_sub(1)
                    .and_then(|ordinal| u16::try_from(ordinal).ok())
                    .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
                let mut replay_omissions = Vec::new();
                replay_omissions
                    .try_reserve_exact(usize::from(terminal_ordinal))
                    .map_err(|_| CryptoMarketPublicationError::RendezvousUnavailable)?;
                for ordinal in 0..terminal_ordinal {
                    replay_omissions.push(CoinbaseMarketOmission::new(
                        ordinal,
                        CoinbaseMarketOmissionReason::UnsupportedCanonicalFamily,
                    ));
                }
                CoinbaseMarketQualificationOutcome::Qualified(
                    CoinbaseQualifiedMarketPublication::ExchangeDirect {
                        initial_snapshot,
                        replay_rows: vec![CoinbaseQualifiedDirectReplayRow::new(
                            terminal_ordinal,
                            replay,
                        )],
                        replay_omissions,
                    },
                )
            }
            _ => CoinbaseMarketQualificationOutcome::Unavailable(
                CoinbaseMarketNonPublicationReason::ApplicationBackpressure,
            ),
        };
        let deadline = Instant::now()
            .checked_add(self.core.limits.frame_timeout)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        publication
            .seal_and_publish_coinbase(
                handoff,
                context,
                qualification,
                analytical_dataset,
                idempotency_key,
                observed_at,
                precommit_authority,
                self.core.cancellation.clone(),
                deadline,
            )
            .await
    }

    async fn wait_for_rows(
        &self,
        key: SourceObjectKey,
        expected: usize,
        retained_bytes: usize,
    ) -> Option<Vec<CommittedResearchMarketObservationLease>> {
        let deadline = self.frame_deadline(&key).await?;
        if expected == 0 || !self.admit_material(&key, retained_bytes).await {
            self.discard(&key).await;
            return None;
        }
        loop {
            if self.core.cancellation.is_cancelled() || Instant::now() >= deadline {
                self.discard(&key).await;
                return None;
            }
            if let Some(rows) = self.take_complete_rows(&key, expected).await {
                return Some(rows);
            }
            let wake = self.core.changed.notified();
            tokio::select! {
                biased;
                () = self.core.cancellation.cancelled() => {}
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                () = wake => continue,
            }
        }
    }

    async fn frame_deadline(&self, key: &SourceObjectKey) -> Option<Instant> {
        if self.core.cancellation.is_cancelled() {
            return None;
        }
        let mut state = self.core.state.lock().await;
        if let Some(deadline) = state.deadlines.get(key) {
            return Some(*deadline);
        }
        if state.deadlines.len() >= self.core.limits.maximum_pending_frames.get() {
            return None;
        }
        let deadline = Instant::now().checked_add(self.core.limits.frame_timeout)?;
        state.deadlines.insert(key.clone(), deadline);
        drop(state);
        self.core.changed.notify_waiters();
        Some(deadline)
    }

    async fn take_complete_rows(
        &self,
        key: &SourceObjectKey,
        expected: usize,
    ) -> Option<Vec<CommittedResearchMarketObservationLease>> {
        let mut state = self.core.state.lock().await;
        let pending = state.rows.get(key)?;
        if pending.rows.len() != expected || pending.rows.iter().any(Option::is_none) {
            return None;
        }
        let pending = state.rows.remove(key)?;
        let sealed_bytes = state.sealed.remove(key).unwrap_or(0);
        state.deadlines.remove(key);
        state.retained_bytes = state
            .retained_bytes
            .saturating_sub(pending.retained_bytes)
            .saturating_sub(sealed_bytes);
        pending.rows.into_iter().collect()
    }

    async fn admit_material(&self, key: &SourceObjectKey, retained_bytes: usize) -> bool {
        let mut state = self.core.state.lock().await;
        if !state.deadlines.contains_key(key) || state.sealed.contains_key(key) {
            return false;
        }
        let Some(total) = state.retained_bytes.checked_add(retained_bytes) else {
            return false;
        };
        if total > self.core.limits.maximum_retained_bytes.get() {
            return false;
        }
        state.sealed.insert(key.clone(), retained_bytes);
        state.retained_bytes = total;
        true
    }

    async fn discard(&self, key: &SourceObjectKey) {
        let mut state = self.core.state.lock().await;
        if let Some(pending) = state.rows.remove(key) {
            state.retained_bytes = state.retained_bytes.saturating_sub(pending.retained_bytes);
        }
        if let Some(retained_bytes) = state.sealed.remove(key) {
            state.retained_bytes = state.retained_bytes.saturating_sub(retained_bytes);
        }
        state.deadlines.remove(key);
        drop(state);
        self.core.changed.notify_waiters();
    }
}

impl CryptoCommittedRowIngress {
    pub(crate) async fn submit(
        &self,
        wire_ordinal: usize,
        expected_row_count: NonZeroUsize,
        lease: CommittedResearchMarketObservationLease,
    ) -> Result<(), CryptoMarketPublicationError> {
        let row_count = expected_row_count.get();
        let observation = lease.observation();
        if self.core.cancellation.is_cancelled()
            || row_count > MAX_DECODED_EVENTS
            || wire_ordinal >= row_count
            || observation.wire_ordinal() != wire_ordinal
            || observation.row_count() != row_count
        {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        let key = SourceObjectKey::from_lease(&lease)?;
        let retained_bytes = lease.retained_bytes();
        let row_slot_bytes = std::mem::size_of::<Option<CommittedResearchMarketObservationLease>>()
            .checked_mul(row_count)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        let now = Instant::now();
        let mut state = self.core.state.lock().await;
        let deadline = match state.deadlines.get(&key).copied() {
            Some(deadline) => deadline,
            None => {
                if state.deadlines.len() >= self.core.limits.maximum_pending_frames.get() {
                    return Err(CryptoMarketPublicationError::RendezvousUnavailable);
                }
                let deadline = now
                    .checked_add(self.core.limits.frame_timeout)
                    .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
                state.deadlines.insert(key.clone(), deadline);
                deadline
            }
        };
        if now >= deadline {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        if !state.rows.contains_key(&key) {
            let total = state
                .retained_bytes
                .checked_add(row_slot_bytes)
                .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
            if total > self.core.limits.maximum_retained_bytes.get() {
                return Err(CryptoMarketPublicationError::RendezvousUnavailable);
            }
            let mut rows = Vec::new();
            rows.try_reserve_exact(row_count)
                .map_err(|_| CryptoMarketPublicationError::RendezvousUnavailable)?;
            rows.resize_with(row_count, || None);
            state.rows.insert(
                key.clone(),
                PendingRows {
                    rows,
                    retained_bytes: row_slot_bytes,
                },
            );
            state.retained_bytes = total;
        }
        let total = state
            .retained_bytes
            .checked_add(retained_bytes)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        if total > self.core.limits.maximum_retained_bytes.get() {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        let pending = state
            .rows
            .get_mut(&key)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        if pending.rows.len() != row_count || pending.rows[wire_ordinal].is_some() {
            return Err(CryptoMarketPublicationError::RendezvousUnavailable);
        }
        pending.rows[wire_ordinal] = Some(lease);
        pending.retained_bytes = pending
            .retained_bytes
            .checked_add(retained_bytes)
            .ok_or(CryptoMarketPublicationError::RendezvousUnavailable)?;
        state.retained_bytes = total;
        drop(state);
        self.core.changed.notify_waiters();
        Ok(())
    }
}

fn direct_retained_bytes(handoff: &CoinbaseMarketHandoff) -> Option<usize> {
    let CoinbaseMarketRawLineage::DirectInitial(lineage) = handoff.raw_lineage() else {
        return None;
    };
    let snapshot = lineage.snapshot().receipt();
    std::mem::size_of::<CoinbaseMarketHandoff>()
        .checked_add(handoff.typed_batch().retained_bytes().ok()?)?
        .checked_add(usize::try_from(snapshot.body_length()).ok()?)?
        .checked_add(snapshot.final_url().len())?
        .checked_add(snapshot.segments().len().checked_mul(std::mem::size_of::<
            market_squawk_sources::HttpResponseSegmentReceipt,
        >())?)?
        .checked_add(lineage.replay().len().checked_mul(std::mem::size_of::<
            market_squawk_adapter_coinbase::CoinbaseDirectReplayFrame,
        >())?)?
        .checked_add(lineage.replay().iter().try_fold(0_usize, |total, frame| {
            total.checked_add(frame.raw_payload().as_bytes().len())
        })?)
}

fn unavailable(
    material: KrakenSealedMarketPublicationMaterial,
    reason: KrakenPublicationUnavailable,
) -> KrakenMarketApplicationOutcome {
    KrakenMarketApplicationOutcome::CanonicalUnavailable(KrakenSealedRawCanonicalUnavailable {
        material,
        reason,
    })
}
