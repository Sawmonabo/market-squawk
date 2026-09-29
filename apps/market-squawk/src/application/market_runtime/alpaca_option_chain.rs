//! Bounded demand ownership for original-reference-backed indicative option snapshots.
//!
//! One group-owned worker retains a demand after its caller disappears. Completed HTTP originals
//! enter atomic catalog custody before cancellation/currentness can prevent canonical publication.

use crate::{
    ResearchService,
    application::{
        AlpacaMarketPublicationClosure, AlpacaOptionMarketRestartReceipt,
        AlpacaOptionMarketRestartSelector, AlpacaPublicationRegistration,
        ProductionResearchIngestCoordinator, ResearchProviderRuntimeGeneration,
        ResearchRightsAuthority,
    },
    provider_activation::{
        AlpacaOptionChainRuntimeAuthority, MarketDataInstrumentBinding, MarketSubscriptionPriority,
        ProviderAccountPublicationAuthority,
    },
};
use market_squawk_adapter_alpaca::{
    AlpacaInstrumentMapping, AlpacaOptionChainContractAuthority,
    AlpacaOptionChainPublicationRequest, AlpacaOptionContractReferenceRequest,
    AlpacaOptionContractReferenceSet, AlpacaPendingOptionContractReferencePage,
};
use market_squawk_data::{
    DatasetId, IngestError, IngestPrecommitAuthority, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord, ProviderCaptureOriginalLease, ProviderCaptureOriginalReceipt,
};
use market_squawk_domain::{
    AssetClass, CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId, MarketDataReference,
    ProviderInstrumentId, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::HttpRequestBounds;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const MAX_QUEUED_DEMANDS: usize = 8;
const MAX_CONTRACTS: usize = 32_000;
const CUSTODY_TIMEOUT: Duration = Duration::from_secs(120);
const OPTION_DATASET: &str = "market_squawk.option_snapshots";

/// Application demand: canonical underlying and an explicit expiration window, never provider URLs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OptionChainDemand {
    underlying: InstrumentId,
    expiration_start: CalendarDate,
    expiration_end: CalendarDate,
    research_scope: Option<OptionResearchScope>,
}
impl OptionChainDemand {
    pub(crate) fn try_new(
        underlying: InstrumentId,
        expiration_start: CalendarDate,
        expiration_end: CalendarDate,
    ) -> Result<Self, OptionChainDemandError> {
        if expiration_start > expiration_end {
            return Err(OptionChainDemandError::InvalidDemand);
        }
        Ok(Self {
            underlying,
            expiration_start,
            expiration_end,
            research_scope: None,
        })
    }
    pub(crate) fn with_research_scope(
        mut self,
        origin: Timestamp,
        profile: String,
    ) -> Result<Self, OptionChainDemandError> {
        if origin.unix_nanos() <= 0
            || profile.len() != 64
            || !profile
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(OptionChainDemandError::InvalidDemand);
        }
        self.research_scope = Some(OptionResearchScope { origin, profile });
        Ok(self)
    }
    pub(crate) const fn underlying(&self) -> InstrumentId {
        self.underlying
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionResearchScope {
    origin: Timestamp,
    profile: String,
}

/// Successful demand always includes a typed read reopened from its exact immutable generation.
pub(crate) struct OptionChainDemandResult {
    restart: AlpacaOptionMarketRestartSelector,
    read: AlpacaOptionMarketRestartReceipt,
}
impl OptionChainDemandResult {
    pub(crate) const fn restart(&self) -> &AlpacaOptionMarketRestartSelector {
        &self.restart
    }
    pub(crate) const fn read(&self) -> &AlpacaOptionMarketRestartReceipt {
        &self.read
    }
}

#[derive(Clone)]
pub(crate) struct OptionChainDemandHandle {
    sender: mpsc::Sender<DemandMessage>,
    cancellation: CancellationToken,
}
impl fmt::Debug for OptionChainDemandHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OptionChainDemandHandle")
            .field("closed", &self.sender.is_closed())
            .finish()
    }
}
impl OptionChainDemandHandle {
    pub(crate) async fn acquire(
        &self,
        request: OptionChainDemand,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OptionChainDemandResult, OptionChainDemandError> {
        ensure_active(deadline, cancellation)?;
        if self.cancellation.is_cancelled() {
            return Err(OptionChainDemandError::Revoked);
        }
        let (reply, result) = oneshot::channel();
        // try_send bounds both retained requests and blocked caller futures.
        self.sender
            .try_send(DemandMessage {
                request,
                deadline,
                cancellation: cancellation.clone(),
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => OptionChainDemandError::Capacity,
                mpsc::error::TrySendError::Closed(_) => OptionChainDemandError::Revoked,
            })?;
        tokio::select! { biased;
            () = cancellation.cancelled() => Err(OptionChainDemandError::Cancelled),
            () = self.cancellation.cancelled() => Err(OptionChainDemandError::Revoked),
            () = tokio::time::sleep_until(deadline.into()) => Err(OptionChainDemandError::Deadline),
            result = result => result.map_err(|_| OptionChainDemandError::Revoked)?,
        }
    }
}
struct DemandMessage {
    request: OptionChainDemand,
    deadline: Instant,
    cancellation: CancellationToken,
    reply: oneshot::Sender<Result<OptionChainDemandResult, OptionChainDemandError>>,
}

/// Account group retains this join until acquisition, original custody and publication have drained.
pub(crate) struct AlpacaOptionChainRuntime {
    registration: Arc<AlpacaPublicationRegistration>,
    authority: Arc<AlpacaOptionChainRuntimeAuthority>,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
    handle: OptionChainDemandHandle,
}
impl AlpacaOptionChainRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start(
        authority: Arc<AlpacaOptionChainRuntimeAuthority>,
        generation: ResearchProviderRuntimeGeneration,
        coordinator: Arc<ProductionResearchIngestCoordinator>,
        research: Arc<ResearchService>,
        bounds: HttpRequestBounds,
        entitlement: EvidenceDigest,
        capability: EvidenceDigest,
        underlyings: Vec<MarketDataInstrumentBinding>,
        cancellation: CancellationToken,
        registration: AlpacaPublicationRegistration,
    ) -> Result<Self, OptionChainDemandError> {
        if cancellation.is_cancelled()
            || underlyings.is_empty()
            || underlyings.len() > 256
            || generation.metadata() != authority.metadata()
        {
            return Err(OptionChainDemandError::Authority);
        }
        let registration = Arc::new(registration);
        let (sender, receiver) = mpsc::channel(MAX_QUEUED_DEMANDS);
        let handle = OptionChainDemandHandle {
            sender,
            cancellation: cancellation.clone(),
        };
        let state = Worker {
            _registration: Arc::clone(&registration),
            authority: Arc::clone(&authority),
            generation,
            coordinator,
            research,
            bounds,
            entitlement,
            capability,
            underlyings,
            cancellation: cancellation.clone(),
        };
        let worker = tokio::spawn(state.run(receiver));
        Ok(Self {
            registration,
            authority,
            cancellation,
            worker: Some(worker),
            handle,
        })
    }
    pub(super) fn demand_handle(&self) -> OptionChainDemandHandle {
        self.handle.clone()
    }
    pub(super) fn begin_shutdown(&self) {
        self.registration.begin_shutdown();
        self.cancellation.cancel();
        self.authority.begin_revocation();
    }
    pub(super) fn is_healthy(&self) -> bool {
        !self.cancellation.is_cancelled()
            && self
                .worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished())
    }
    pub(super) async fn finish_shutdown_before(
        &mut self,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let Some(worker) = self.worker.as_mut() {
            tokio::time::timeout_at(deadline.into(), worker)
                .await
                .map_err(|_| ServiceError::Unavailable)?
                .map_err(|_| ServiceError::Unavailable)?;
            self.worker = None;
        }
        self.authority.revoke_and_drain().await;
        self.registration.finish_shutdown().await;
        Ok(())
    }
    pub(super) async fn finish_retained_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        if let Some(worker) = self.worker.as_mut() {
            worker.await.map_err(|_| ServiceError::Unavailable)?;
            self.worker = None;
        }
        self.authority.revoke_and_drain().await;
        self.registration.finish_shutdown().await;
        Ok(())
    }
}
impl Drop for AlpacaOptionChainRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct Worker {
    _registration: Arc<AlpacaPublicationRegistration>,
    authority: Arc<AlpacaOptionChainRuntimeAuthority>,
    generation: ResearchProviderRuntimeGeneration,
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    research: Arc<ResearchService>,
    bounds: HttpRequestBounds,
    entitlement: EvidenceDigest,
    capability: EvidenceDigest,
    underlyings: Vec<MarketDataInstrumentBinding>,
    cancellation: CancellationToken,
}
impl Worker {
    async fn run(self, mut receiver: mpsc::Receiver<DemandMessage>) {
        loop {
            let message = tokio::select! { biased;
                () = self.cancellation.cancelled() => break,
                message = receiver.recv() => match message { Some(message) => message, None => break },
            };
            let result = self
                .acquire_with_resume(&message.request, message.deadline, &message.cancellation)
                .await;
            let _ = message.reply.send(result);
        }
        receiver.close();
        while let Some(message) = receiver.recv().await {
            let _ = message.reply.send(Err(OptionChainDemandError::Revoked));
        }
    }
    async fn acquire_with_resume(
        &self,
        demand: &OptionChainDemand,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<OptionChainDemandResult, OptionChainDemandError> {
        ensure_active(deadline, caller)?;
        let data = self.research.analytical_service();
        let source = self.authority.metadata().source_id().clone();
        let pending = self
            .research
            .run_owned_research_io(deadline, caller, move |worker| {
                data.pending_provider_capture_original(&source, deadline, &worker)
            })
            .await
            .map_err(|_| OptionChainDemandError::Custody)?
            .map_err(|_| OptionChainDemandError::Custody)?;
        if let Some(original) = pending {
            let saved: OriginalContext = serde_json::from_slice(original.context())
                .map_err(|_| OptionChainDemandError::Custody)?;
            if saved.version != 1
                || saved.source != *self.authority.metadata().source_id()
                || saved.generation
                    != self
                        .generation
                        .generation_digest()
                        .map_err(|_| OptionChainDemandError::Authority)?
            {
                return Err(OptionChainDemandError::PendingOriginalDemand);
            }
            // Finish exactly the retained scope before admitting a different current demand.
            // Its original origin/profile/date window is never replaced by today's defaults.
            let restored = self.acquire(&saved.demand, deadline, caller).await?;
            if saved.demand == *demand {
                return Ok(restored);
            }
            ensure_active(deadline, caller)?;
        }
        self.acquire(demand, deadline, caller).await
    }
    async fn acquire(
        &self,
        demand: &OptionChainDemand,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<OptionChainDemandResult, OptionChainDemandError> {
        ensure_active(deadline, caller)?;
        let binding = self
            .underlyings
            .iter()
            .find(|binding| binding.instrument_id() == demand.underlying)
            .ok_or(OptionChainDemandError::InvalidDemand)?;
        let reference_request = AlpacaOptionContractReferenceRequest::try_new(
            binding.provisional_subscription_symbol().to_owned(),
            demand.expiration_start,
            demand.expiration_end,
        )
        .map_err(|_| OptionChainDemandError::InvalidDemand)?;
        // The owned worker keeps this operation alive if the waiting presentation disappears.
        let operation = self
            .coordinator
            .acquire_provider_publication_operation(
                &self.generation,
                self.cancellation.clone(),
                deadline,
            )
            .await
            .map_err(|error| match error {
                crate::application::ResearchIngestCompositionError::ShuttingDown
                | crate::application::ResearchIngestCompositionError::StaleRuntimeGeneration
                | crate::application::ResearchIngestCompositionError::RuntimeGenerationUnavailable => OptionChainDemandError::Revoked,
                _ => OptionChainDemandError::Authority,
            })?;
        let publication = AlpacaMarketPublicationClosure::try_new(
            Arc::clone(&self.research),
            operation.source().clone(),
            operation.rights().clone(),
            operation.source_registered_at(),
        )
        .map_err(|_| OptionChainDemandError::Authority)?;
        let lease = Arc::new(
            self.research
                .analytical()
                .acquire_provider_capture_original_lease(deadline, caller)
                .await
                .map_err(|_| OptionChainDemandError::Custody)?,
        );
        let (originals, original_receipts) = self
            .originals(
                demand,
                &reference_request,
                operation.rights(),
                Arc::clone(&lease),
                deadline,
                caller,
            )
            .await?;
        ensure_active(deadline, caller)?;
        let now = timestamp()?;
        let catalog = self.research.market_data_instruments();
        let underlying_record = catalog
            .latest(demand.underlying, deadline, caller)
            .map_err(|_| OptionChainDemandError::Identity)?
            .ok_or(OptionChainDemandError::Identity)?;
        let underlying = binding
            .publication_reference(&underlying_record, now)
            .map_err(|_| OptionChainDemandError::Identity)?;
        let mapping = AlpacaInstrumentMapping::try_new(
            underlying.source_symbol().as_str().to_owned(),
            underlying.instrument_id(),
            underlying.asset_class(),
        )
        .map_err(|_| OptionChainDemandError::Identity)?;
        // Existing catalog authority alone allocates canonical identities from original source proof.
        drop(lease);
        let originals = Arc::new(originals);
        let mut original_rights = Vec::new();
        original_rights
            .try_reserve_exact(original_receipts.len())
            .map_err(|_| OptionChainDemandError::Capacity)?;
        for original in &original_receipts {
            let received_at = original
                .capture()
                .pages()
                .first()
                .ok_or(OptionChainDemandError::Custody)?
                .received_at();
            original_rights.push(
                operation
                    .rights()
                    .decision(original.capture().observation_digest(), received_at)
                    .map_err(|_| OptionChainDemandError::Authority)?,
            );
        }
        let reference_precommit = Arc::new(ReferencePrecommit {
            account: self
                .authority
                .acquire_publication_authority(deadline, caller)
                .await
                .map_err(map_acquisition)?,
            publication: operation.precommit_authority(),
            catalog: catalog.clone(),
            records: vec![underlying_record.clone()],
            references: vec![underlying.clone()],
            deadline,
            cancellation: caller.clone(),
            lifecycle: self.cancellation.clone(),
        });
        self.research
            .publish_alpaca_option_references(
                market_squawk_data::AlpacaOptionReferenceAdmission {
                    source: operation.source().clone(),
                    rights: original_rights,
                    originals: original_receipts,
                    contracts: Arc::clone(&originals),
                    underlying: underlying_record.clone(),
                    underlying_asset_namespace: binding.native_identity()
                        .ok_or(OptionChainDemandError::Identity)?
                        .namespace.clone(),
                },
                reference_precommit,
                deadline,
                caller.clone(),
            )
            .await
            .map_err(|_| OptionChainDemandError::Identity)?;
        let mut records = vec![underlying_record];
        let mut references = vec![underlying.clone()];
        let mut contracts = Vec::new();
        for original in originals.contracts() {
            if contracts.len() == MAX_CONTRACTS {
                return Err(OptionChainDemandError::Capacity);
            }
            let (record, reference) = resolve_contract(
                &catalog,
                original.symbol(),
                original.occ_identity().as_str(),
                now,
                deadline,
                caller,
            )?;
            contracts
                .try_reserve(1)
                .map_err(|_| OptionChainDemandError::Capacity)?;
            records
                .try_reserve(1)
                .map_err(|_| OptionChainDemandError::Capacity)?;
            references
                .try_reserve(1)
                .map_err(|_| OptionChainDemandError::Capacity)?;
            contracts.push(
                AlpacaOptionChainContractAuthority::try_new(reference.clone(), original, now)
                    .map_err(|_| OptionChainDemandError::Identity)?,
            );
            records.push(record);
            references.push(reference);
        }
        let originals =
            Arc::try_unwrap(originals).map_err(|_| OptionChainDemandError::Authority)?;
        ensure_active(deadline, caller)?;
        let research = Arc::clone(&self.research);
        let capture = self
            .authority
            .acquire_complete_chain(&mapping, &reference_request, deadline, caller, move |request| {
                let research = Arc::clone(&research);
                async move {
                    let finish = CancellationToken::new();
                    research.seal_provider_capture(request, &finish, Instant::now() + CUSTODY_TIMEOUT)
                        .await.map(|_| ())
                        .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)
                }
            })
            .await
            .map_err(map_acquisition)?;
        let (rejoin, seal_request) = capture.into_parts();
        // Raw completed chain evidence seals even if account revocation races this continuation.
        let custody = CancellationToken::new();
        let custody_deadline = Instant::now() + CUSTODY_TIMEOUT;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &custody, custody_deadline)
            .await
            .map_err(|_| OptionChainDemandError::Custody)?;
        ensure_active(deadline, caller)?;
        let observed_at = timestamp()?;
        let request = AlpacaOptionChainPublicationRequest::try_new(
            underlying,
            contracts,
            originals,
            self.entitlement,
            self.capability,
            observed_at,
        )
        .map_err(|_| OptionChainDemandError::Identity)?;
        let binding = rejoin
            .try_rejoin(sealed, request)
            .and_then(|prepared| prepared.try_into_binding())
            .map_err(|_| OptionChainDemandError::Acquisition)?;
        let digest = binding.evidence_digest().evidence();
        let account = self
            .authority
            .acquire_publication_authority(deadline, caller)
            .await
            .map_err(map_acquisition)?;
        let precommit = Arc::new(ReferencePrecommit {
            account,
            publication: operation.precommit_authority(),
            catalog,
            records,
            references,
            deadline,
            cancellation: caller.clone(),
            lifecycle: self.cancellation.clone(),
        });
        let receipt = publication
            .publish_option_market(
                binding,
                dataset()?,
                format!("alpaca-option-{}", hex_digest(digest)),
                observed_at,
                precommit,
                operation.cancellation().clone(),
            )
            .await
            .map_err(|_| OptionChainDemandError::Publication)?;
        let restart = receipt.restart_selector().clone();
        let read = restart
            .reopen(&self.research, caller.clone())
            .await
            .map_err(|_| OptionChainDemandError::Read)?;
        Ok(OptionChainDemandResult { restart, read })
    }

    async fn originals(
        &self,
        demand: &OptionChainDemand,
        request: &AlpacaOptionContractReferenceRequest,
        rights: &ResearchRightsAuthority,
        lease: Arc<ProviderCaptureOriginalLease>,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<
        (
            AlpacaOptionContractReferenceSet,
            Vec<ProviderCaptureOriginalReceipt>,
        ),
        OptionChainDemandError,
    > {
        let data = self.research.analytical_service();
        let source = self.authority.metadata().source_id().clone();
        let pending = self
            .research
            .run_owned_research_io(deadline, caller, move |worker| {
                data.pending_provider_capture_original(&source, deadline, &worker)
            })
            .await
            .map_err(|_| OptionChainDemandError::Custody)?
            .map_err(|_| OptionChainDemandError::Custody)?;
        let (context, originals) = if let Some(first) = pending {
            let context: OriginalContext = serde_json::from_slice(first.context())
                .map_err(|_| OptionChainDemandError::Custody)?;
            if context.version != 1
                || context.generation
                    != self
                        .generation
                        .generation_digest()
                        .map_err(|_| OptionChainDemandError::Authority)?
                || context.demand != *demand
                || context.request != *request
                || context.source != *self.authority.metadata().source_id()
                || context.contexts.len() != usize::from(first.expected_count())
            {
                return Err(OptionChainDemandError::PendingOriginalDemand);
            }
            let data = self.research.analytical_service();
            let session = first.session();
            let count = first.expected_count();
            let originals = self
                .research
                .run_owned_research_io(deadline, caller, move |worker| {
                    let mut values = Vec::new();
                    values
                        .try_reserve_exact(usize::from(count))
                        .map_err(|_| OptionChainDemandError::Capacity)?;
                    for ordinal in 0..count {
                        values.push(
                            data.provider_capture_original(session, ordinal, deadline, &worker)
                                .map_err(|_| OptionChainDemandError::Custody)?
                                .ok_or(OptionChainDemandError::Custody)?,
                        );
                    }
                    Ok::<_, OptionChainDemandError>(values)
                })
                .await
                .map_err(|_| OptionChainDemandError::Custody)??;
            (context, originals)
        } else {
            let research = Arc::clone(&self.research);
            let page_lease = Arc::clone(&lease);
            let retained_pages = self
                .authority
                .acquire_option_contract_references(
                    request.clone(), self.bounds, deadline, caller,
                    move |page| {
                        let research = Arc::clone(&research);
                        let retained_lease = Arc::clone(&page_lease);
                        async move {
                            let store = research.provider_capture_store();
                            let finish = CancellationToken::new();
                            let custody_deadline = Instant::now() + CUSTODY_TIMEOUT;
                            research.run_owned_research_io(custody_deadline, &finish, move |worker| {
                                let _lease = retained_lease;
                                if worker.is_cancelled() || Instant::now() >= custody_deadline {
                                    return Err(market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial);
                                }
                                let context = page.context();
                                let (rejoin, request) = page.into_seal_parts()
                                    .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?;
                                let token = rejoin.into_original_token(request.seal(&store)
                                    .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?)
                                    .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?;
                                let context = String::from_utf8(context?)
                                    .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?;
                                let decoded_at = timestamp()
                                    .map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?;
                                Ok::<_, market_squawk_adapter_alpaca::AlpacaError>((context, decoded_at, token))
                            }).await.map_err(|_| market_squawk_adapter_alpaca::AlpacaError::CaptureMaterial)?
                        }
                    },
                )
                .await
                .map_err(map_acquisition)?;
            let contexts = retained_pages.iter().map(|(context, _, _)| context.clone()).collect();
            let context = OriginalContext {
                version: 1,
                demand: demand.clone(),
                source: self.authority.metadata().source_id().clone(),
                generation: self
                    .generation
                    .generation_digest()
                    .map_err(|_| OptionChainDemandError::Authority)?,
                request: request.clone(),
                contexts,
            };
            let encoded =
                serde_json::to_vec(&context).map_err(|_| OptionChainDemandError::Custody)?;
            let session =
                EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&encoded).into());
            let data = self.research.analytical_service();
            let store = self.research.provider_capture_store();
            let metadata = self.authority.metadata().clone();
            let rights = rights.clone();
            let target = dataset()?;
            let finish = CancellationToken::new();
            let custody_deadline = Instant::now() + CUSTODY_TIMEOUT;
            let retained_lease = Arc::clone(&lease);
            let originals = self
                .research
                .run_owned_research_io(custody_deadline, &finish, move |worker| {
                    let _lease = retained_lease;
                    let mut sealed = Vec::new();
                    sealed
                        .try_reserve_exact(retained_pages.len())
                        .map_err(|_| OptionChainDemandError::Capacity)?;
                    for (_, decoded_at, token) in retained_pages {
                        let capture = token.persisted_receipt().capture();
                        let received_at = capture
                            .pages()
                            .last()
                            .ok_or(OptionChainDemandError::Custody)?
                            .received_at();
                        let decision = rights
                            .decision(capture.observation_digest(), received_at)
                            .map_err(|_| OptionChainDemandError::Authority)?;
                        sealed.push((decoded_at, token, decision));
                    }
                    data.retain_option_contract_reference_originals(
                        &metadata,
                        session,
                        &target,
                        &encoded,
                        sealed,
                        &store,
                        custody_deadline,
                        &worker,
                    )
                    .map_err(|_| OptionChainDemandError::Custody)
                })
                .await
                .map_err(|_| OptionChainDemandError::Custody)??;
            (context, originals)
        };
        let proof = self
            .replay(context, originals.clone(), lease, deadline, caller)
            .await?;
        Ok((proof, originals))
    }
    async fn replay(
        &self,
        context: OriginalContext,
        originals: Vec<ProviderCaptureOriginalReceipt>,
        lease: Arc<ProviderCaptureOriginalLease>,
        deadline: Instant,
        caller: &CancellationToken,
    ) -> Result<AlpacaOptionContractReferenceSet, OptionChainDemandError> {
        let data = self.research.analytical_service();
        let store = self.research.provider_capture_store();
        self.research
            .run_owned_research_io(deadline, caller, move |worker| {
                let _lease = lease;
                if originals.len() != context.contexts.len() {
                    return Err(OptionChainDemandError::Custody);
                }
                let mut pages = Vec::new();
                pages
                    .try_reserve_exact(originals.len())
                    .map_err(|_| OptionChainDemandError::Capacity)?;
                for (original, bytes) in originals.iter().zip(context.contexts.iter()) {
                    let read = data
                        .reopen_provider_capture_original(original, &store, deadline, &worker)
                        .map_err(|_| OptionChainDemandError::Custody)?;
                    let page = AlpacaPendingOptionContractReferencePage::restore_original(
                        bytes.as_bytes(),
                        read.original().capture(),
                        read.records(),
                    )
                    .map_err(|_| OptionChainDemandError::Custody)?;
                    let (rejoin, seal) = page
                        .into_seal_parts()
                        .map_err(|_| OptionChainDemandError::Custody)?;
                    pages.push(
                        rejoin
                            .try_rejoin(
                                seal.seal(&store)
                                    .map_err(|_| OptionChainDemandError::Custody)?,
                            )
                            .map_err(|_| OptionChainDemandError::Custody)?,
                    );
                }
                AlpacaOptionContractReferenceSet::try_from_pages(pages)
                    .map_err(|_| OptionChainDemandError::Custody)
            })
            .await
            .map_err(|_| OptionChainDemandError::Custody)?
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalContext {
    version: u16,
    demand: OptionChainDemand,
    source: market_squawk_domain::SourceId,
    generation: EvidenceDigest,
    request: AlpacaOptionContractReferenceRequest,
    contexts: Vec<String>,
}

fn resolve_contract(
    catalog: &MarketDataInstrumentReadCapability,
    symbol: &str,
    occ_identity: &str,
    at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(MarketDataInstrumentRecord, MarketDataReference), OptionChainDemandError> {
    let search = catalog
        .search(occ_identity, 64, deadline, cancellation)
        .map_err(|_| OptionChainDemandError::Identity)?;
    if search.has_more() {
        return Err(OptionChainDemandError::Identity);
    }
    let mut accepted = None;
    for candidate in search.matches() {
        let record = candidate.record();
        if record.definition().asset_class() != AssetClass::Option {
            continue;
        }
        for identifier in record.definition().identifiers() {
            let Ok(binding) = MarketDataInstrumentBinding::try_from_assigned_identifier(
                MarketSubscriptionPriority::CurrentlyViewed,
                record.clone(),
                ProviderInstrumentId::try_from(symbol)
                    .map_err(|_| OptionChainDemandError::Identity)?,
                identifier.clone(),
            ) else {
                continue;
            };
            let reference = binding
                .publication_reference(record, at)
                .map_err(|_| OptionChainDemandError::Identity)?;
            if accepted.as_ref().is_some_and(
                |(previous, _): &(MarketDataInstrumentRecord, MarketDataReference)| {
                    previous != record
                },
            ) {
                return Err(OptionChainDemandError::Identity);
            }
            accepted = Some((record.clone(), reference));
        }
    }
    accepted.ok_or(OptionChainDemandError::Identity)
}

#[derive(Debug)]
struct ReferencePrecommit {
    account: ProviderAccountPublicationAuthority,
    publication: Arc<dyn IngestPrecommitAuthority>,
    catalog: MarketDataInstrumentReadCapability,
    records: Vec<MarketDataInstrumentRecord>,
    references: Vec<MarketDataReference>,
    deadline: Instant,
    cancellation: CancellationToken,
    lifecycle: CancellationToken,
}
impl ReferencePrecommit {
    fn validate(&self) -> Result<(), IngestError> {
        if self.lifecycle.is_cancelled()
            || self.cancellation.is_cancelled()
            || Instant::now() >= self.deadline
            || self.records.len() != self.references.len()
        {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        let now = timestamp().map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        for (record, reference) in self.records.iter().zip(&self.references) {
            if record.revision_digest() != reference.definition_digest() {
                return Err(IngestError::PublicationAuthorityRevoked);
            }
            reference
                .validate_definition_at(record.definition(), now)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
}
impl IngestPrecommitAuthority for ReferencePrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.validate()?;
        self.publication.validate_precommit()?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
    fn validate_catalog_precommit(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), IngestError> {
        self.validate()?;
        self.publication.validate_catalog_precommit(catalog)?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        for record in &self.records {
            self.catalog
                .require_current_in_catalog(catalog, record, self.deadline, &self.cancellation)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
}
fn dataset() -> Result<DatasetId, OptionChainDemandError> {
    DatasetId::try_from(OPTION_DATASET).map_err(|_| OptionChainDemandError::Authority)
}
fn timestamp() -> Result<Timestamp, OptionChainDemandError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OptionChainDemandError::Authority)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| OptionChainDemandError::Authority)?,
    ))
}
fn hex_digest(digest: EvidenceDigest) -> String {
    digest
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn ensure_active(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OptionChainDemandError> {
    if cancellation.is_cancelled() {
        Err(OptionChainDemandError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OptionChainDemandError::Deadline)
    } else {
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum OptionChainDemandError {
    #[error("option demand is invalid")]
    InvalidDemand,
    #[error("option demand capacity is exhausted")]
    Capacity,
    #[error("option demand was cancelled")]
    Cancelled,
    #[error("option demand deadline expired")]
    Deadline,
    #[error("option demand authority was revoked")]
    Revoked,
    #[error("option publication authority is invalid")]
    Authority,
    #[error("option provider permission is unavailable")]
    Permission,
    #[error("option acquisition failed")]
    Acquisition,
    #[error("original option evidence could not be retained or reopened")]
    Custody,
    #[error("a retained option request must be resumed before a different request")]
    PendingOriginalDemand,
    #[error("exact canonical option identity is missing or ambiguous")]
    Identity,
    #[error("option publication failed")]
    Publication,
    #[error("published option evidence could not be read")]
    Read,
}

fn map_acquisition(
    error: crate::provider_activation::AlpacaOptionChainRuntimeError,
) -> OptionChainDemandError {
    use crate::provider_activation::AlpacaOptionChainRuntimeError as R;
    use market_squawk_adapter_alpaca::AlpacaError as A;
    match error {
        R::Cancelled | R::Adapter(A::Cancelled) => OptionChainDemandError::Cancelled,
        R::Adapter(A::DeadlineExceeded) => OptionChainDemandError::Deadline,
        R::Unavailable | R::Adapter(A::Allocation | A::BodyTooLarge | A::SubscriptionLimit) => {
            OptionChainDemandError::Capacity
        }
        R::Revoked | R::Stale => OptionChainDemandError::Revoked,
        R::Adapter(A::InvalidAuthorization) => OptionChainDemandError::Permission,
        R::SourceBinding | R::Adapter(A::InvalidCredentials) => OptionChainDemandError::Authority,
        R::Adapter(A::Network) => OptionChainDemandError::Acquisition,
        R::Adapter(A::CaptureMaterial) => OptionChainDemandError::Custody,
        R::Adapter(_) => OptionChainDemandError::Identity,
    }
}
