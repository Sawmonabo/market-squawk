//! Bounded acknowledged publication protocol for one Coinbase Direct product owner.
//!
//! The adapter callback is synchronous, while raw sealing and analytical publication are
//! asynchronous application work. This module provides the only allowed bridge: one bounded,
//! serialized actor command and a deadline-bound reply carrying the exact registry-minted current
//! lineage. The common sink prepares exact current authority before command admission; the actor
//! handler owns physical sealing, mandatory precommit publication, immutable generation commit,
//! and recovered-head continuity.

use std::{
    fmt::Write as _,
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::{
        Arc,
        mpsc::{self as blocking_mpsc, Receiver as BlockingReceiver, SyncSender},
    },
    time::{Duration, Instant},
};

use market_squawk_adapter_coinbase::{
    CoinbaseDirectConfig, CoinbaseMarketContinuity, CoinbaseMarketHandoff, CoinbaseMarketRawLineage,
};
use market_squawk_data::{
    DatasetId, DatasetManifestRef, IngestIdentity, IngestPrecommitAuthority, SourceOperation,
    provider_market_event_publication_digest,
};
use market_squawk_domain::{
    ConnectionGeneration, EvidenceDigest, InstrumentId, MarketEvent, ProviderProduct,
    SequenceNumber, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    CaptureAdmissionReceipt, CurrentProviderLineageHandoff, FrameId,
    ProviderEventMicrobatchMaterial, RawMarketFrame, SealedProviderPublicationBinding,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    ResearchService, application::ResearchRightsAuthority,
    live_source::composition::system_timestamp,
};

use super::canonical::try_build_initial_snapshot;

const ACKNOWLEDGEMENT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const COMMAND_CAPACITY: usize = 1;

/// Static product identity bound into one application publication actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseDirectPublicationKey {
    source_id: SourceId,
    product: ProviderProduct,
    instrument: InstrumentId,
    venue: VenueId,
}

impl CoinbaseDirectPublicationKey {
    pub(super) const fn new(
        source_id: SourceId,
        product: ProviderProduct,
        instrument: InstrumentId,
        venue: VenueId,
    ) -> Self {
        Self {
            source_id,
            product,
            instrument,
            venue,
        }
    }

    pub(super) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(super) const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    pub(super) const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub(super) const fn venue(&self) -> &VenueId {
        &self.venue
    }
}

/// Complete byte admission for one serialized callback command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseDirectPublicationActorLimits {
    retained_bytes: NonZeroU32,
}

impl CoinbaseDirectPublicationActorLimits {
    pub(super) const fn new(retained_bytes: NonZeroU32) -> Self {
        Self { retained_bytes }
    }

    pub(super) const fn retained_bytes(self) -> NonZeroU32 {
        self.retained_bytes
    }
}

/// Exact raw-capture admission paired with one adapter-retained sequenced frame.
#[derive(Debug)]
pub(in crate::live_source) struct CoinbaseDirectCapturedFrame {
    frame: RawMarketFrame,
    receipt: CaptureAdmissionReceipt,
}

impl CoinbaseDirectCapturedFrame {
    pub(super) const fn new(frame: RawMarketFrame, receipt: CaptureAdmissionReceipt) -> Self {
        Self { frame, receipt }
    }

    pub(in crate::live_source) const fn frame_id(&self) -> FrameId {
        self.frame.frame_id()
    }

    pub(in crate::live_source) fn wire_bytes(&self) -> usize {
        self.frame.payload().len()
    }

    pub(in crate::live_source) fn into_parts(self) -> (RawMarketFrame, CaptureAdmissionReceipt) {
        (self.frame, self.receipt)
    }
}

/// Consuming initial or successor callback request admitted to the sole product actor.
#[derive(Debug)]
pub(super) struct CoinbaseDirectPublicationRequest {
    handoff: CoinbaseMarketHandoff,
    prepared: CoinbaseDirectPreparedCurrentPublication,
    retained_bytes: NonZeroU32,
}

impl CoinbaseDirectPublicationRequest {
    pub(super) fn try_new(
        handoff: CoinbaseMarketHandoff,
        prepared: CoinbaseDirectPreparedCurrentPublication,
        retained_bytes: NonZeroU32,
    ) -> Result<Self, CoinbaseDirectPublicationRequestError> {
        match handoff.raw_lineage() {
            CoinbaseMarketRawLineage::DirectInitial(_)
            | CoinbaseMarketRawLineage::DirectSuccessor(_) => {}
            CoinbaseMarketRawLineage::AdvancedTrade(_) => {
                return Err(CoinbaseDirectPublicationRequestError::Profile);
            }
        }
        let decoder = handoff.typed_batch().evidence();
        let frame = prepared.current_lineage().frame_evidence();
        if !frame.binding().shares_allocation_with(decoder.binding())
            || frame.frame_id() != decoder.frame_id()
            || frame.received_at() != decoder.received_at()
            || frame.payload_digest() != decoder.payload_digest()
            || frame.decoder_rule() != decoder.decoder_rule()
        {
            return Err(CoinbaseDirectPublicationRequestError::CurrentEvidence);
        }
        Ok(Self {
            handoff,
            prepared,
            retained_bytes,
        })
    }

    pub(super) const fn handoff(&self) -> &CoinbaseMarketHandoff {
        &self.handoff
    }

    pub(super) const fn prepared(&self) -> &CoinbaseDirectPreparedCurrentPublication {
        &self.prepared
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        CoinbaseMarketHandoff,
        CoinbaseDirectPreparedCurrentPublication,
    ) {
        (self.handoff, self.prepared)
    }

    const fn retained_bytes(&self) -> NonZeroU32 {
        self.retained_bytes
    }
}

/// Request construction rejected an incomplete or inconsistent callback graph.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(in crate::live_source) enum CoinbaseDirectPublicationRequestError {
    #[error("Coinbase Direct publication request is not the authenticated Direct profile")]
    Profile,
    #[error("Coinbase Direct prepared current evidence does not match the terminal handoff")]
    CurrentEvidence,
}

/// Exact registry-qualified state transferred into the durable actor in the same callback.
///
/// The common sink constructs this only after consuming the terminal capture receipt through the
/// authoritative registry. The process-local precommit authority is cloned from that same current
/// observation before its noncloneable lineage is minted. The durable handler must pass the
/// authority to the mandatory provider-event publisher; it cannot substitute a manifest or a
/// caller-authored receipt.
#[derive(Debug)]
pub(in crate::live_source) struct CoinbaseDirectPreparedCurrentPublication {
    event_material: ProviderEventMicrobatchMaterial,
    canonical_events: Vec<MarketEvent>,
    current_lineage: CurrentProviderLineageHandoff,
    precommit_authority: Arc<dyn IngestPrecommitAuthority>,
}

impl CoinbaseDirectPreparedCurrentPublication {
    pub(in crate::live_source) fn try_new(
        event_material: ProviderEventMicrobatchMaterial,
        canonical_events: Vec<MarketEvent>,
        current_lineage: CurrentProviderLineageHandoff,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    ) -> Result<Self, CoinbaseDirectPublicationRequestError> {
        if canonical_events.is_empty() || event_material.records().is_empty() {
            return Err(CoinbaseDirectPublicationRequestError::CurrentEvidence);
        }
        current_lineage
            .validate_current()
            .map_err(|_| CoinbaseDirectPublicationRequestError::CurrentEvidence)?;
        precommit_authority
            .validate_precommit()
            .map_err(|_| CoinbaseDirectPublicationRequestError::CurrentEvidence)?;
        Ok(Self {
            event_material,
            canonical_events,
            current_lineage,
            precommit_authority,
        })
    }

    pub(super) const fn event_material(&self) -> &ProviderEventMicrobatchMaterial {
        &self.event_material
    }

    pub(super) fn canonical_events(&self) -> &[MarketEvent] {
        &self.canonical_events
    }

    pub(super) const fn current_lineage(&self) -> &CurrentProviderLineageHandoff {
        &self.current_lineage
    }

    pub(super) fn precommit_authority(&self) -> &Arc<dyn IngestPrecommitAuthority> {
        &self.precommit_authority
    }

    pub(super) fn into_parts(
        self,
    ) -> (
        ProviderEventMicrobatchMaterial,
        Vec<MarketEvent>,
        CurrentProviderLineageHandoff,
        Arc<dyn IngestPrecommitAuthority>,
    ) {
        (
            self.event_material,
            self.canonical_events,
            self.current_lineage,
            self.precommit_authority,
        )
    }
}

/// One application implementation of the complete current-to-durable operation.
///
/// Implementations must consume the request's prepared current state, pass its exact
/// `Arc<dyn IngestPrecommitAuthority>` to the mandatory provider-event publisher, retain the
/// committed generation, and only then return its exact `CurrentProviderLineageHandoff`.
/// Reconstructed lineage and detached publication tasks are invalid implementations.
pub(super) trait CoinbaseDirectPublicationHandler: Send + 'static {
    fn publish<'handler>(
        &'handler mut self,
        request: CoinbaseDirectPublicationRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        CurrentProviderLineageHandoff,
                        CoinbaseDirectPublicationFailure,
                    >,
                > + Send
                + 'handler,
        >,
    >;
}

/// Application-owned sealing and immutable-publication authority for one Direct product.
#[derive(Debug)]
pub(super) struct ProductionCoinbaseDirectPublicationHandler {
    research: Arc<ResearchService>,
    config: CoinbaseDirectConfig,
    analytical_dataset: DatasetId,
    registered_at: Timestamp,
    rights: ResearchRightsAuthority,
    latest: Option<CoinbaseDirectCommittedHead>,
}

impl ProductionCoinbaseDirectPublicationHandler {
    pub(super) fn try_new(
        research: Arc<ResearchService>,
        config: CoinbaseDirectConfig,
        registered_at: Timestamp,
        rights: ResearchRightsAuthority,
    ) -> Result<Self, CoinbaseDirectPublicationFailure> {
        if rights.source_id() != config.metadata().source_id() {
            return Err(CoinbaseDirectPublicationFailure::PublicationAdmission);
        }
        let dataset_name = format!(
            "coinbase-exchange-direct-{}-market-events-v1",
            config.product().as_source_identifier().as_str()
        );
        let analytical_dataset = DatasetId::try_from(dataset_name.as_str())
            .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
        Ok(Self {
            research,
            config,
            analytical_dataset,
            registered_at,
            rights,
            latest: None,
        })
    }

    async fn publish_inner(
        &mut self,
        request: CoinbaseDirectPublicationRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<CurrentProviderLineageHandoff, CoinbaseDirectPublicationFailure> {
        ensure_operation_live(&cancellation, deadline)?;
        let coordinates = CoinbaseDirectPublicationCoordinates::try_from_handoff(
            &self.config,
            request.handoff(),
        )?;
        self.validate_continuity(&coordinates)?;
        let (handoff, prepared) = request.into_parts();
        let (event_material, canonical_events, current_lineage, precommit_authority) =
            prepared.into_parts();
        current_lineage
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationFailure::CurrentAuthority)?;
        precommit_authority
            .validate_precommit()
            .map_err(|_error| CoinbaseDirectPublicationFailure::CurrentAuthority)?;

        let binding = match coordinates.kind {
            CoinbaseDirectPublicationKind::Initial => {
                let config = &self.config;
                let (pending, requests) = handoff
                    .into_exchange_direct_sealing_parts(
                        config,
                        event_material,
                        |row| try_build_initial_snapshot(config, row),
                        canonical_events,
                    )
                    .map_err(|_error| CoinbaseDirectPublicationFailure::CanonicalProjection)?;
                let (response_request, event_request) = requests.into_parts();
                let sealed_response = self
                    .research
                    .seal_provider_capture(response_request, &cancellation, deadline)
                    .await
                    .map_err(|_error| {
                        map_operation_failure(
                            &cancellation,
                            deadline,
                            CoinbaseDirectPublicationFailure::PhysicalSeal,
                        )
                    })?;
                let sealed_events = self
                    .research
                    .seal_provider_capture(event_request, &cancellation, deadline)
                    .await
                    .map_err(|_error| {
                        map_operation_failure(
                            &cancellation,
                            deadline,
                            CoinbaseDirectPublicationFailure::PhysicalSeal,
                        )
                    })?;
                pending
                    .try_rejoin(sealed_response, sealed_events)
                    .map(SealedProviderPublicationBinding::from)
                    .map_err(|_error| CoinbaseDirectPublicationFailure::PhysicalSeal)?
            }
            CoinbaseDirectPublicationKind::Successor => {
                let (pending, seal_request) = handoff
                    .into_exchange_direct_successor_sealing_parts(
                        &self.config,
                        event_material,
                        canonical_events,
                    )
                    .map_err(|_error| CoinbaseDirectPublicationFailure::CanonicalProjection)?;
                let sealed = self
                    .research
                    .seal_provider_capture(seal_request, &cancellation, deadline)
                    .await
                    .map_err(|_error| {
                        map_operation_failure(
                            &cancellation,
                            deadline,
                            CoinbaseDirectPublicationFailure::PhysicalSeal,
                        )
                    })?;
                pending
                    .try_rejoin(sealed)
                    .map(SealedProviderPublicationBinding::from)
                    .map_err(|_error| CoinbaseDirectPublicationFailure::PhysicalSeal)?
            }
        };
        ensure_operation_live(&cancellation, deadline)?;
        let publication_digest = provider_market_event_publication_digest(&binding)
            .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
        let observed_at = system_timestamp()
            .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
        let rights = self
            .rights
            .decision(publication_digest, observed_at)
            .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
        let identity = IngestIdentity::try_new(
            self.config.metadata().source_id().clone(),
            publication_digest,
            SourceOperation::Persist,
            direct_publication_idempotency_key(
                &self.config,
                &self.analytical_dataset,
                &coordinates,
                publication_digest,
            )?,
        )
        .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
        let reservation = self
            .research
            .analytical()
            .reserve_source_ingest(
                self.config.metadata(),
                self.registered_at,
                rights,
                &identity,
                &cancellation,
            )
            .await
            .map_err(|_error| {
                map_operation_failure(
                    &cancellation,
                    deadline,
                    CoinbaseDirectPublicationFailure::PublicationAdmission,
                )
            })?;
        ensure_operation_live(&cancellation, deadline)?;
        let committed = self
            .research
            .analytical()
            .ingest_provider_market_events(
                reservation,
                self.analytical_dataset.clone(),
                binding,
                cancellation.clone(),
                precommit_authority,
            )
            .await
            .map_err(|_error| {
                map_operation_failure(
                    &cancellation,
                    deadline,
                    CoinbaseDirectPublicationFailure::DurablePublication,
                )
            })?;
        ensure_operation_live(&cancellation, deadline)?;
        current_lineage
            .validate_current()
            .map_err(|_error| CoinbaseDirectPublicationFailure::CurrentAuthority)?;
        self.retain_committed_head(&coordinates, publication_digest, committed.manifest())?;
        Ok(current_lineage)
    }

    fn validate_continuity(
        &self,
        coordinates: &CoinbaseDirectPublicationCoordinates,
    ) -> Result<(), CoinbaseDirectPublicationFailure> {
        match (coordinates.kind, self.latest.as_ref()) {
            (CoinbaseDirectPublicationKind::Initial, Some(latest))
                if latest.session_id == coordinates.session_id
                    && latest.generation == coordinates.generation =>
            {
                Err(CoinbaseDirectPublicationFailure::Continuity)
            }
            (CoinbaseDirectPublicationKind::Initial, _) => Ok(()),
            (CoinbaseDirectPublicationKind::Successor, Some(latest))
                if latest.session_id == coordinates.session_id
                    && latest.generation == coordinates.generation
                    && coordinates.predecessor == Some(latest.terminal) =>
            {
                Ok(())
            }
            (CoinbaseDirectPublicationKind::Successor, _) => {
                Err(CoinbaseDirectPublicationFailure::Continuity)
            }
        }
    }

    fn retain_committed_head(
        &mut self,
        coordinates: &CoinbaseDirectPublicationCoordinates,
        publication_digest: EvidenceDigest,
        manifest: &DatasetManifestRef,
    ) -> Result<(), CoinbaseDirectPublicationFailure> {
        if manifest.dataset_id() != &self.analytical_dataset
            || self.latest.as_ref().is_some_and(|latest| {
                latest.manifest.manifest_version().checked_add(1)
                    != Some(manifest.manifest_version())
            })
        {
            return Err(CoinbaseDirectPublicationFailure::Continuity);
        }
        self.latest = Some(CoinbaseDirectCommittedHead {
            manifest: manifest.clone(),
            publication_digest,
            session_id: coordinates.session_id.clone(),
            generation: coordinates.generation,
            terminal: coordinates.terminal,
        });
        Ok(())
    }
}

impl CoinbaseDirectPublicationHandler for ProductionCoinbaseDirectPublicationHandler {
    fn publish<'handler>(
        &'handler mut self,
        request: CoinbaseDirectPublicationRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        CurrentProviderLineageHandoff,
                        CoinbaseDirectPublicationFailure,
                    >,
                > + Send
                + 'handler,
        >,
    > {
        Box::pin(self.publish_inner(request, cancellation, deadline))
    }
}

#[derive(Debug)]
struct CoinbaseDirectCommittedHead {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    session_id: SourceIdentifier,
    generation: ConnectionGeneration,
    terminal: SequenceNumber,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoinbaseDirectPublicationKind {
    Initial,
    Successor,
}

#[derive(Debug)]
struct CoinbaseDirectPublicationCoordinates {
    kind: CoinbaseDirectPublicationKind,
    session_id: SourceIdentifier,
    generation: ConnectionGeneration,
    predecessor: Option<SequenceNumber>,
    terminal: SequenceNumber,
}

impl CoinbaseDirectPublicationCoordinates {
    fn try_from_handoff(
        config: &CoinbaseDirectConfig,
        handoff: &CoinbaseMarketHandoff,
    ) -> Result<Self, CoinbaseDirectPublicationFailure> {
        if handoff.evidence().product() != config.product()
            || handoff.evidence().configured_instrument() != config.instrument()
            || handoff.evidence().venue() != config.venue()
        {
            return Err(CoinbaseDirectPublicationFailure::Continuity);
        }
        let (kind, predecessor, terminal) =
            match (handoff.raw_lineage(), handoff.evidence().continuity()) {
                (
                    CoinbaseMarketRawLineage::DirectInitial(_),
                    CoinbaseMarketContinuity::SnapshotContiguous { terminal, .. },
                ) => (CoinbaseDirectPublicationKind::Initial, None, terminal),
                (
                    CoinbaseMarketRawLineage::DirectSuccessor(_),
                    CoinbaseMarketContinuity::AcceptedContiguous {
                        predecessor,
                        terminal,
                    },
                ) => (
                    CoinbaseDirectPublicationKind::Successor,
                    Some(predecessor),
                    terminal,
                ),
                _ => return Err(CoinbaseDirectPublicationFailure::Continuity),
            };
        let binding = handoff.typed_batch().evidence().binding();
        Ok(Self {
            kind,
            session_id: binding.session_id().as_source_identifier().clone(),
            generation: binding.connection_generation(),
            predecessor,
            terminal,
        })
    }
}

fn direct_publication_idempotency_key(
    config: &CoinbaseDirectConfig,
    dataset: &DatasetId,
    coordinates: &CoinbaseDirectPublicationCoordinates,
    publication_digest: EvidenceDigest,
) -> Result<String, CoinbaseDirectPublicationFailure> {
    let mut digest_hex = String::with_capacity(64);
    for byte in publication_digest.bytes() {
        write!(&mut digest_hex, "{byte:02x}")
            .map_err(|_error| CoinbaseDirectPublicationFailure::PublicationAdmission)?;
    }
    Ok(format!(
        "coinbase-direct-v1:{}:{}:{}:{}:{}:{}",
        config.metadata().source_id().as_str(),
        dataset.as_str(),
        coordinates.session_id.as_str(),
        coordinates.generation.get(),
        coordinates.terminal.get(),
        digest_hex,
    ))
}

fn ensure_operation_live(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), CoinbaseDirectPublicationFailure> {
    if cancellation.is_cancelled() {
        Err(CoinbaseDirectPublicationFailure::Cancelled)
    } else if Instant::now() >= deadline {
        Err(CoinbaseDirectPublicationFailure::Deadline)
    } else {
        Ok(())
    }
}

fn map_operation_failure(
    cancellation: &CancellationToken,
    deadline: Instant,
    fallback: CoinbaseDirectPublicationFailure,
) -> CoinbaseDirectPublicationFailure {
    if cancellation.is_cancelled() {
        CoinbaseDirectPublicationFailure::Cancelled
    } else if Instant::now() >= deadline {
        CoinbaseDirectPublicationFailure::Deadline
    } else {
        fallback
    }
}

/// Durable operation failure returned by the application handler.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum CoinbaseDirectPublicationFailure {
    #[error("Coinbase Direct current registry qualification failed")]
    CurrentQualification,
    #[error("Coinbase Direct canonical current-event projection failed")]
    CanonicalProjection,
    #[error("Coinbase Direct raw publication material could not be sealed")]
    PhysicalSeal,
    #[error("Coinbase Direct event publication admission failed")]
    PublicationAdmission,
    #[error("Coinbase Direct immutable event generation could not be committed")]
    DurablePublication,
    #[error("Coinbase Direct publication continuity is inconsistent")]
    Continuity,
    #[error("Coinbase Direct current authority became stale")]
    CurrentAuthority,
    #[error("Coinbase Direct publication was cancelled")]
    Cancelled,
    #[error("Coinbase Direct publication exceeded its operation deadline")]
    Deadline,
}

/// Synchronous adapter-facing ingress for the bounded application actor.
#[derive(Clone, Debug)]
pub(super) struct CoinbaseDirectPublicationActorIngress {
    key: CoinbaseDirectPublicationKey,
    commands: mpsc::Sender<PublicationCommand>,
    command_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    cancellation: CancellationToken,
}

impl CoinbaseDirectPublicationActorIngress {
    pub(super) const fn key(&self) -> &CoinbaseDirectPublicationKey {
        &self.key
    }

    /// Enters one serialized operation and waits for its exact acknowledged lineage reply.
    ///
    /// This method intentionally blocks only the isolated Direct adapter worker. Root composition
    /// must not invoke the synchronous adapter loop on the same executor lane as the publication
    /// actor. Cancellation and the monotonic deadline bound both queue admission and reply wait.
    pub(super) fn try_publish(
        &self,
        request: CoinbaseDirectPublicationRequest,
        deadline: Instant,
    ) -> Result<CurrentProviderLineageHandoff, CoinbaseDirectPublicationIngressError> {
        if self.cancellation.is_cancelled() {
            return Err(CoinbaseDirectPublicationIngressError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(CoinbaseDirectPublicationIngressError::Deadline);
        }
        let byte_charge = request.retained_bytes().get();
        let command_permit = Arc::clone(&self.command_budget)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    CoinbaseDirectPublicationIngressError::CountSaturated
                }
                tokio::sync::TryAcquireError::Closed => {
                    CoinbaseDirectPublicationIngressError::ActorClosed
                }
            })?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(byte_charge)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    CoinbaseDirectPublicationIngressError::BytesSaturated
                }
                tokio::sync::TryAcquireError::Closed => {
                    CoinbaseDirectPublicationIngressError::ActorClosed
                }
            })?;
        let (reply, response) = blocking_mpsc::sync_channel(1);
        let operation_cancellation = self.cancellation.child_token();
        let command = PublicationCommand {
            request,
            deadline,
            cancellation: operation_cancellation.clone(),
            reply,
            _ticket: PublicationTicket {
                _command_permit: command_permit,
                _byte_permit: byte_permit,
            },
        };
        match self.commands.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_command)) => {
                return Err(CoinbaseDirectPublicationIngressError::CountSaturated);
            }
            Err(mpsc::error::TrySendError::Closed(_command)) => {
                return Err(CoinbaseDirectPublicationIngressError::ActorClosed);
            }
        }
        wait_for_acknowledgement(
            response,
            &self.cancellation,
            operation_cancellation,
            deadline,
        )
    }
}

fn wait_for_acknowledgement(
    response: BlockingReceiver<
        Result<CurrentProviderLineageHandoff, CoinbaseDirectPublicationFailure>,
    >,
    actor_cancellation: &CancellationToken,
    operation_cancellation: CancellationToken,
    deadline: Instant,
) -> Result<CurrentProviderLineageHandoff, CoinbaseDirectPublicationIngressError> {
    loop {
        if actor_cancellation.is_cancelled() {
            operation_cancellation.cancel();
            return Err(CoinbaseDirectPublicationIngressError::Cancelled);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            operation_cancellation.cancel();
            return Err(CoinbaseDirectPublicationIngressError::Deadline);
        }
        match response.recv_timeout(remaining.min(ACKNOWLEDGEMENT_POLL_INTERVAL)) {
            Ok(Ok(lineage)) => {
                lineage
                    .validate_current()
                    .map_err(|_| CoinbaseDirectPublicationIngressError::CurrentAuthority)?;
                return Ok(lineage);
            }
            Ok(Err(failure)) => {
                return Err(CoinbaseDirectPublicationIngressError::Publication(failure));
            }
            Err(blocking_mpsc::RecvTimeoutError::Timeout) => {}
            Err(blocking_mpsc::RecvTimeoutError::Disconnected) => {
                operation_cancellation.cancel();
                return Err(CoinbaseDirectPublicationIngressError::ReplyClosed);
            }
        }
    }
}

/// Bounded ingress or acknowledged-operation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum CoinbaseDirectPublicationIngressError {
    #[error("Coinbase Direct publication actor command capacity is full")]
    CountSaturated,
    #[error("Coinbase Direct publication actor byte capacity is full")]
    BytesSaturated,
    #[error("Coinbase Direct publication actor is closed")]
    ActorClosed,
    #[error("Coinbase Direct publication actor closed the acknowledgement reply")]
    ReplyClosed,
    #[error("Coinbase Direct publication operation was cancelled")]
    Cancelled,
    #[error("Coinbase Direct publication acknowledgement deadline elapsed")]
    Deadline,
    #[error("Coinbase Direct returned lineage is no longer current")]
    CurrentAuthority,
    #[error("Coinbase Direct publication failed: {0}")]
    Publication(#[source] CoinbaseDirectPublicationFailure),
}

#[derive(Debug)]
struct PublicationTicket {
    _command_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct PublicationCommand {
    request: CoinbaseDirectPublicationRequest,
    deadline: Instant,
    cancellation: CancellationToken,
    reply: SyncSender<Result<CurrentProviderLineageHandoff, CoinbaseDirectPublicationFailure>>,
    _ticket: PublicationTicket,
}

/// Sole serialized consumer for one Direct product's current and durable generations.
#[derive(Debug)]
pub(super) struct CoinbaseDirectPublicationActor {
    commands: mpsc::Receiver<PublicationCommand>,
    cancellation: CancellationToken,
}

/// Builds one count-and-byte-bounded product actor before provider network release.
pub(super) fn coinbase_direct_publication_actor_channel(
    key: CoinbaseDirectPublicationKey,
    limits: CoinbaseDirectPublicationActorLimits,
    cancellation: CancellationToken,
) -> (
    CoinbaseDirectPublicationActorIngress,
    CoinbaseDirectPublicationActor,
) {
    let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
    let command_budget = Arc::new(Semaphore::new(COMMAND_CAPACITY));
    let byte_budget = Arc::new(Semaphore::new(limits.retained_bytes().get() as usize));
    (
        CoinbaseDirectPublicationActorIngress {
            key,
            commands,
            command_budget,
            byte_budget,
            cancellation: cancellation.clone(),
        },
        CoinbaseDirectPublicationActor {
            commands: receiver,
            cancellation,
        },
    )
}

impl CoinbaseDirectPublicationActor {
    /// Runs the sole serialized handler until coordinated cancellation.
    pub(super) async fn run<H>(
        mut self,
        mut handler: H,
    ) -> Result<(), CoinbaseDirectPublicationActorRunError>
    where
        H: CoinbaseDirectPublicationHandler,
    {
        loop {
            let command = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => return Ok(()),
                command = self.commands.recv() => command
                    .ok_or(CoinbaseDirectPublicationActorRunError::CommandChannelClosed)?,
            };
            self.process(&mut handler, command).await;
        }
    }

    async fn process<H>(&self, handler: &mut H, command: PublicationCommand)
    where
        H: CoinbaseDirectPublicationHandler,
    {
        let PublicationCommand {
            request,
            deadline,
            cancellation,
            reply,
            _ticket,
        } = command;
        let deadline_at = tokio::time::Instant::from_std(deadline);
        let mut result = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => {
                Err(CoinbaseDirectPublicationFailure::Cancelled)
            }
            () = cancellation.cancelled() => {
                Err(CoinbaseDirectPublicationFailure::Cancelled)
            }
            () = tokio::time::sleep_until(deadline_at) => {
                cancellation.cancel();
                Err(CoinbaseDirectPublicationFailure::Deadline)
            }
            result = handler.publish(request, cancellation.clone(), deadline) => result,
        };
        if self.cancellation.is_cancelled() || cancellation.is_cancelled() {
            result = Err(CoinbaseDirectPublicationFailure::Cancelled);
        } else if Instant::now() >= deadline {
            cancellation.cancel();
            result = Err(CoinbaseDirectPublicationFailure::Deadline);
        } else if let Ok(lineage) = result.as_ref() {
            if lineage.validate_current().is_err() {
                result = Err(CoinbaseDirectPublicationFailure::CurrentAuthority);
            }
        }
        let _acknowledged_or_requester_gone = reply.try_send(result);
        drop(_ticket);
    }
}

/// Publication actor exited without coordinated cancellation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum CoinbaseDirectPublicationActorRunError {
    #[error("Coinbase Direct publication actor command channel closed unexpectedly")]
    CommandChannelClosed,
}
