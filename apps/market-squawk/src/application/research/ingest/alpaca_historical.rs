//! One generation-bound Alpaca historical source with bounded immutable click-plan admission.

use std::{
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_alpaca::{
    AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalEquityConfig,
    AlpacaHistoricalEquityDatasetPlan, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalEquityPreflightReceipt, AlpacaHistoricalEquitySource,
};
use market_squawk_data::{DatasetId, SourceOperation};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketDataInstrumentDefinition, ProviderInstrumentId,
    SourceIdentifier,
};
use market_squawk_sources::{
    AuthorizationMode, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch,
    ExtractionRequest, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    HttpRequestBounds, ProviderCaptureMaterial, ProviderCaptureSemanticBinding, SourceError,
    SourceMetadata, SourceMetadataProvider,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ManagedExtraction, ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    RegisteredExtractionSource, ResearchProviderAdmission, ResearchProviderPublicationLease,
    ResearchRevisionPlanError, ResearchRightsAuthority, invalid_capture_protocol,
};
use crate::{
    application::market_runtime::{
        AlpacaHistoricalCompositeCalendarAuthority, AlpacaHistoricalRuntimeCapability,
        MarketRuntimeGroupGeneration,
    },
    provider_activation::ProviderMarketAccount,
};

const MAXIMUM_ALPACA_HISTORICAL_PLANS: usize = 4_096;
const MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES: usize = 256 * 1024 * 1024;
const ALPACA_HISTORICAL_PROFILE: &str = "alpaca.basic-market-data.historical-v1";

/// Exact private parent of one installed Alpaca historical source generation.
///
/// The group generation prevents a same-account restart from reusing the preceding directory;
/// the binding digest covers the subordinate capability and immutable source authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AlpacaHistoricalParentGeneration {
    group_generation: MarketRuntimeGroupGeneration,
    binding_digest: EvidenceDigest,
}

impl AlpacaHistoricalParentGeneration {
    pub(crate) const fn group_generation(self) -> MarketRuntimeGroupGeneration {
        self.group_generation
    }

    pub(crate) const fn binding_digest(self) -> EvidenceDigest {
        self.binding_digest
    }

    fn try_from_runtime(
        runtime: &AlpacaHistoricalRuntimeCapability,
    ) -> Result<Self, AlpacaHistoricalSourceSlotError> {
        let group_generation = runtime.group_generation();
        let binding_digest = parent_binding_digest(
            runtime,
            runtime.historical_metadata(),
            runtime.historical_request_bounds(),
            runtime.historical_rights(),
        )
        .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidParent)?;
        if group_generation.digest().algorithm() != DigestAlgorithm::Sha256
            || group_generation.digest().bytes() == [0; 32]
            || binding_digest.bytes() == [0; 32]
        {
            return Err(AlpacaHistoricalSourceSlotError::InvalidParent);
        }
        Ok(Self {
            group_generation,
            binding_digest,
        })
    }

    #[cfg(test)]
    fn try_from_test_digests(
        group_digest: EvidenceDigest,
        binding_digest: EvidenceDigest,
    ) -> Result<Self, AlpacaHistoricalSourceSlotError> {
        let group_generation = MarketRuntimeGroupGeneration::try_from_expected_digest(group_digest)
            .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidParent)?;
        if binding_digest.algorithm() != DigestAlgorithm::Sha256
            || binding_digest.bytes() == [0; 32]
        {
            return Err(AlpacaHistoricalSourceSlotError::InvalidParent);
        }
        Ok(Self {
            group_generation,
            binding_digest,
        })
    }
}

/// Exact immutable plan coordinates minted by one validated directory mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AlpacaHistoricalAdmittedPlan {
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    parent_digest: EvidenceDigest,
    plan_digest: EvidenceDigest,
}

/// Narrow object-safe view retained by the specialized slot and exact-parent leases.
pub(crate) trait AlpacaHistoricalPlanAdmissionDirectory:
    Send + Sync + fmt::Debug + 'static
{
    fn admit_plan<'a>(
        &'a self,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<AlpacaHistoricalAdmittedPlan, AlpacaHistoricalPlanAdmissionError>>;

    fn validate_parent<'a>(
        &'a self,
        parent: AlpacaHistoricalParentGeneration,
        deadline: Instant,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AlpacaHistoricalSourceSlotError>>;

    fn validate_parent_now(
        &self,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), AlpacaHistoricalSourceSlotError>;

    fn validate_plan_now(
        &self,
        plan: &AlpacaHistoricalAdmittedPlan,
    ) -> Result<(), AlpacaHistoricalSourceSlotError>;
}

/// Exact-parent, exact-admission directory lease returned by the coordinator slot.
#[derive(Clone)]
pub(crate) struct AlpacaHistoricalPlanDirectoryLease {
    parent: AlpacaHistoricalParentGeneration,
    admission: ResearchProviderAdmission,
    directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory>,
}

impl AlpacaHistoricalPlanDirectoryLease {
    pub(crate) fn validate_exact_parent(
        &self,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        if self.parent != parent {
            return Err(AlpacaHistoricalSourceSlotError::StaleParent);
        }
        self.admission
            .ensure_live()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)
    }

    pub(crate) async fn admit_plan(
        &self,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalPlanReceipt, AlpacaHistoricalPlanAdmissionError> {
        self.validate_exact_parent(self.parent)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let publication = tokio::select! {
            biased;
            () = self.admission.cancellation().cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            result = self.admission.acquire_publication_lease() => {
                result.map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?
            }
        };
        let plan = tokio::select! {
            biased;
            () = self.admission.cancellation().cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            result = self.directory.admit_plan(
                preflight_plan,
                canonical_instrument,
                deadline,
                cancellation,
            ) => result?,
        };
        if plan.parent_digest != self.parent.binding_digest()
            || plan.plan_digest.algorithm() != DigestAlgorithm::Sha256
            || plan.plan_digest.bytes() == [0; 32]
        {
            return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
        }
        tokio::select! {
            biased;
            () = self.admission.cancellation().cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            result = self.directory.validate_parent(self.parent, deadline, cancellation) => {
                result.map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
            }
        }
        self.directory
            .validate_plan_now(&plan)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        publication
            .validate_precommit()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        self.validate_exact_parent(self.parent)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let receipt = AlpacaHistoricalPlanReceipt {
            parent: self.parent,
            plan,
            admission: self.admission.clone(),
            _private: (),
        };
        drop(publication);
        Ok(receipt)
    }
}

impl fmt::Debug for AlpacaHistoricalPlanDirectoryLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalPlanDirectoryLease")
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

/// Fully constructed, still-pending installation candidate.
pub(crate) struct PreparedAlpacaHistoricalSourceInstall {
    parent: AlpacaHistoricalParentGeneration,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
    admission: ResearchProviderAdmission,
    directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory>,
    source: Arc<dyn ManagedResearchExtractionSource>,
}

impl PreparedAlpacaHistoricalSourceInstall {
    fn try_new(
        parent: AlpacaHistoricalParentGeneration,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        admission: ResearchProviderAdmission,
        directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory>,
        source: Arc<dyn ManagedResearchExtractionSource>,
    ) -> Result<Self, AlpacaHistoricalSourceSlotError> {
        admission
            .ensure_pending()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidCandidate)?;
        if source.metadata() != &metadata || metadata.source_id() != rights.source_id() {
            return Err(AlpacaHistoricalSourceSlotError::InvalidCandidate);
        }
        Ok(Self {
            parent,
            metadata,
            rights,
            admission,
            directory,
            source,
        })
    }
}

#[derive(Clone)]
pub(super) struct AlpacaHistoricalStableSlot {
    parent: AlpacaHistoricalParentGeneration,
    directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory>,
    admission: ResearchProviderAdmission,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
}

pub(super) struct AlpacaHistoricalSlotCompletion {
    sender: watch::Sender<
        Option<Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError>>,
    >,
}

pub(super) struct AlpacaHistoricalDrainCompletion {
    sender: watch::Sender<Option<Result<(), AlpacaHistoricalSourceSlotError>>>,
}

impl AlpacaHistoricalDrainCompletion {
    fn new() -> Arc<Self> {
        let (sender, _receiver) = watch::channel(None);
        Arc::new(Self { sender })
    }

    fn complete(&self, result: Result<(), AlpacaHistoricalSourceSlotError>) {
        self.sender.send_replace(Some(result));
    }

    async fn wait(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        let mut receiver = self.sender.subscribe();
        loop {
            if let Some(result) = *receiver.borrow() {
                return result;
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(AlpacaHistoricalSourceSlotError::WaitCancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    return Err(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded);
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable);
                    }
                }
            }
        }
    }
}

impl AlpacaHistoricalSlotCompletion {
    fn new() -> Arc<Self> {
        let (sender, _receiver) = watch::channel(None);
        Arc::new(Self { sender })
    }

    fn complete(
        &self,
        result: Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError>,
    ) {
        self.sender.send_replace(Some(result));
    }

    async fn wait(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError> {
        let mut receiver = self.sender.subscribe();
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(AlpacaHistoricalSourceSlotError::WaitCancelled);
                }
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    return Err(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded);
                }
                changed = receiver.changed() => {
                    if changed.is_err() {
                        return Err(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable);
                    }
                }
            }
        }
    }
}

pub(super) enum AlpacaHistoricalSourceSlot {
    Absent,
    Installing {
        parent: AlpacaHistoricalParentGeneration,
        operation_id: Uuid,
        completion: Arc<AlpacaHistoricalSlotCompletion>,
        predecessor: Option<AlpacaHistoricalStableSlot>,
    },
    Active(AlpacaHistoricalStableSlot),
    Draining {
        stable: AlpacaHistoricalStableSlot,
        completion: Arc<AlpacaHistoricalDrainCompletion>,
    },
    Stopped(AlpacaHistoricalStableSlot),
    ReconciliationRequired(AlpacaHistoricalStableSlot),
}

impl AlpacaHistoricalSourceSlot {
    pub(super) const fn absent() -> Self {
        Self::Absent
    }
}

/// Sole non-cloneable mutation capability for the canonical Alpaca-history slot.
pub(crate) struct AlpacaHistoricalSourceMutationAuthority {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
}

impl AlpacaHistoricalSourceMutationAuthority {
    pub(super) fn new(coordinator: Arc<ProductionResearchIngestCoordinator>) -> Self {
        Self { coordinator }
    }

    pub(crate) async fn install_or_join_runtime(
        &self,
        runtime: AlpacaHistoricalRuntimeCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError> {
        runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleParent)?;
        let parent = AlpacaHistoricalParentGeneration::try_from_runtime(&runtime)?;
        let metadata = runtime.historical_metadata().clone();
        let rights = runtime.historical_rights().clone();
        let install_deadline = Instant::now()
            .checked_add(self.coordinator.limits.operation_duration)
            .ok_or(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded)?;
        self.install_or_join(
            parent,
            metadata.clone(),
            rights.clone(),
            deadline,
            cancellation,
            move |admission| async move {
                let (candidate_parent, directory, source) =
                    AlpacaHistoricalPlanDirectoryAuthority::try_new(
                        runtime,
                        install_deadline,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidCandidate)?;
                if candidate_parent != parent {
                    return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                }
                let directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory> =
                    Arc::new(directory);
                let source: Arc<dyn ManagedResearchExtractionSource> = Arc::new(source);
                PreparedAlpacaHistoricalSourceInstall::try_new(
                    parent, metadata, rights, admission, directory, source,
                )
            },
        )
        .await
    }

    #[cfg(test)]
    async fn install_or_join_for_test<F, Fut>(
        &self,
        parent: AlpacaHistoricalParentGeneration,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
        build: F,
    ) -> Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError>
    where
        F: FnOnce(ResearchProviderAdmission) -> Fut + Send + 'static,
        Fut: Future<
                Output = Result<
                    PreparedAlpacaHistoricalSourceInstall,
                    AlpacaHistoricalSourceSlotError,
                >,
            > + Send
            + 'static,
    {
        self.install_or_join(parent, metadata, rights, deadline, cancellation, build)
            .await
    }

    async fn install_or_join<F, Fut>(
        &self,
        parent: AlpacaHistoricalParentGeneration,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
        build: F,
    ) -> Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError>
    where
        F: FnOnce(ResearchProviderAdmission) -> Fut + Send + 'static,
        Fut: Future<
                Output = Result<
                    PreparedAlpacaHistoricalSourceInstall,
                    AlpacaHistoricalSourceSlotError,
                >,
            > + Send
            + 'static,
    {
        if Instant::now() >= deadline {
            return Err(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded);
        }
        let (completion, start) = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)?;
            if self.coordinator.lifecycle.shutdown_token().is_cancelled()
                || authority.registry.is_none()
            {
                return Err(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable);
            }
            match &authority.alpaca_historical {
                AlpacaHistoricalSourceSlot::Installing {
                    parent: active_parent,
                    completion,
                    ..
                } if *active_parent == parent => (Arc::clone(completion), None),
                AlpacaHistoricalSourceSlot::Active(stable) if stable.parent == parent => {
                    stable
                        .admission
                        .ensure_live()
                        .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?;
                    return Ok(AlpacaHistoricalPlanDirectoryLease {
                        parent,
                        admission: stable.admission.clone(),
                        directory: Arc::clone(&stable.directory),
                    });
                }
                AlpacaHistoricalSourceSlot::Absent
                | AlpacaHistoricalSourceSlot::Stopped(_)
                | AlpacaHistoricalSourceSlot::ReconciliationRequired(_) => {
                    let predecessor = match std::mem::replace(
                        &mut authority.alpaca_historical,
                        AlpacaHistoricalSourceSlot::Absent,
                    ) {
                        AlpacaHistoricalSourceSlot::Absent => None,
                        AlpacaHistoricalSourceSlot::Stopped(stable)
                        | AlpacaHistoricalSourceSlot::ReconciliationRequired(stable) => {
                            Some(stable)
                        }
                        _ => unreachable!("slot variant matched before replacement"),
                    };
                    if predecessor
                        .as_ref()
                        .is_some_and(|stable| !stable.admission.revocation_drained())
                    {
                        authority.alpaca_historical =
                            AlpacaHistoricalSourceSlot::ReconciliationRequired(
                                predecessor.expect("checked predecessor"),
                            );
                        return Err(AlpacaHistoricalSourceSlotError::DrainIncomplete);
                    }
                    let operation_id = Uuid::new_v4();
                    let completion = AlpacaHistoricalSlotCompletion::new();
                    authority.alpaca_historical = AlpacaHistoricalSourceSlot::Installing {
                        parent,
                        operation_id,
                        completion: Arc::clone(&completion),
                        predecessor,
                    };
                    (completion, Some(operation_id))
                }
                AlpacaHistoricalSourceSlot::Installing { .. }
                | AlpacaHistoricalSourceSlot::Active(_)
                | AlpacaHistoricalSourceSlot::Draining { .. } => {
                    return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                }
            }
        };

        if let Some(operation_id) = start {
            let admission =
                ResearchProviderAdmission::new_pending_for_parent_digest(parent.binding_digest())
                    .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidParent)?;
            let coordinator = Arc::clone(&self.coordinator);
            let completion_for_task = Arc::clone(&completion);
            tokio::spawn(async move {
                let candidate = build(admission).await;
                let result = match candidate {
                    Ok(candidate) => {
                        publish_candidate(
                            &coordinator,
                            parent,
                            operation_id,
                            metadata,
                            rights,
                            candidate,
                            Instant::now()
                                .checked_add(coordinator.limits.operation_duration)
                                .unwrap_or_else(Instant::now),
                        )
                        .await
                    }
                    Err(error) => {
                        restore_pre_registration_failure(&coordinator, parent, operation_id);
                        Err(error)
                    }
                };
                if result.is_err() {
                    restore_pre_registration_failure(&coordinator, parent, operation_id);
                }
                completion_for_task.complete(result);
            });
        }
        completion.wait(deadline, cancellation).await
    }

    /// Binds one receipt to the exact current coordinator state without acquiring a publication
    /// barrier.
    ///
    /// The returned non-cloneable token can acquire the receipt's publication barrier only after
    /// this coordinator lock has been released. The market registry retains its outer mutation
    /// serializer across both phases, establishing the sole lock order used by later consumers.
    pub(crate) fn validate_plan_receipt<'receipt>(
        &self,
        receipt: &'receipt AlpacaHistoricalPlanReceipt,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalValidatedPlan<'receipt>, AlpacaHistoricalSourceSlotError> {
        ensure_plan_receipt_wait_current(deadline, cancellation)?;
        let profile = canonical_alpaca_historical_profile()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)?;
        ensure_plan_receipt_wait_current(deadline, cancellation)?;
        let AlpacaHistoricalSourceSlot::Active(stable) = &authority.alpaca_historical else {
            return Err(AlpacaHistoricalSourceSlotError::StaleLease);
        };
        if stable.parent != receipt.parent
            || !stable.admission.matches(&receipt.admission)
            || receipt.plan.parent_digest != stable.parent.binding_digest()
            || receipt.plan.plan_digest.algorithm() != DigestAlgorithm::Sha256
            || receipt.plan.plan_digest.bytes() == [0; 32]
        {
            return Err(AlpacaHistoricalSourceSlotError::StaleLease);
        }
        stable
            .admission
            .ensure_live()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?;
        stable.directory.validate_parent_now(receipt.parent)?;
        stable.directory.validate_plan_now(&receipt.plan)?;
        let source = authority
            .sources
            .get(&profile)
            .ok_or(AlpacaHistoricalSourceSlotError::StaleLease)?;
        if !source.admission.matches(&receipt.admission)
            || !source.admission.matches(&stable.admission)
            || source.metadata != stable.metadata
            || source.rights != stable.rights
            || source.metadata.source_id() != source.registration.source_id()
            || source.metadata.revision() != source.registration.revision()
            || source.metadata.source_id() != source.rights.source_id()
        {
            return Err(AlpacaHistoricalSourceSlotError::StaleLease);
        }
        receipt
            .admission
            .ensure_live()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?;
        ensure_plan_receipt_wait_current(deadline, cancellation)?;
        let validated = AlpacaHistoricalValidatedPlan {
            plan: &receipt.plan,
            admission: receipt.admission.clone(),
        };
        drop(authority);
        Ok(validated)
    }

    #[cfg(test)]
    async fn drain_exact_for_test(
        &self,
        parent: AlpacaHistoricalParentGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        self.drain_exact(parent, deadline, cancellation).await
    }

    pub(crate) async fn drain_exact(
        &self,
        parent: AlpacaHistoricalParentGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        drain_slot(&self.coordinator, Some(parent), deadline, cancellation).await
    }
}

pub(super) async fn drain_before_registry_close(
    coordinator: &ProductionResearchIngestCoordinator,
    deadline: Instant,
) -> Result<(), AlpacaHistoricalSourceSlotError> {
    drain_slot(coordinator, None, deadline, &CancellationToken::new()).await
}

fn ensure_plan_receipt_wait_current(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalSourceSlotError> {
    if cancellation.is_cancelled() {
        Err(AlpacaHistoricalSourceSlotError::WaitCancelled)
    } else if Instant::now() >= deadline {
        Err(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded)
    } else {
        Ok(())
    }
}

async fn drain_slot(
    coordinator: &ProductionResearchIngestCoordinator,
    expected_parent: Option<AlpacaHistoricalParentGeneration>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalSourceSlotError> {
    loop {
        enum Action {
            Done,
            WaitInstall(Arc<AlpacaHistoricalSlotCompletion>),
            WaitDrain(Arc<AlpacaHistoricalDrainCompletion>),
            StartDrain(
                AlpacaHistoricalStableSlot,
                Arc<AlpacaHistoricalDrainCompletion>,
            ),
        }
        let action = {
            let mut authority = coordinator
                .authority
                .lock()
                .map_err(|_error| AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)?;
            match &authority.alpaca_historical {
                AlpacaHistoricalSourceSlot::Absent => Action::Done,
                AlpacaHistoricalSourceSlot::Installing {
                    parent, completion, ..
                } => {
                    if expected_parent.is_some_and(|expected| expected != *parent) {
                        return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                    }
                    Action::WaitInstall(Arc::clone(completion))
                }
                AlpacaHistoricalSourceSlot::Active(stable) => {
                    if expected_parent.is_some_and(|expected| expected != stable.parent) {
                        return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                    }
                    let stable = stable.clone();
                    stable.admission.revoke();
                    authority
                        .selections
                        .revoke_profile(&canonical_alpaca_historical_profile()?);
                    let completion = AlpacaHistoricalDrainCompletion::new();
                    authority.alpaca_historical = AlpacaHistoricalSourceSlot::Draining {
                        stable: stable.clone(),
                        completion: Arc::clone(&completion),
                    };
                    Action::StartDrain(stable, completion)
                }
                AlpacaHistoricalSourceSlot::Draining { stable, completion } => {
                    if expected_parent.is_some_and(|expected| expected != stable.parent) {
                        return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                    }
                    Action::WaitDrain(Arc::clone(completion))
                }
                AlpacaHistoricalSourceSlot::ReconciliationRequired(stable) => {
                    if expected_parent.is_some_and(|expected| expected != stable.parent) {
                        return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                    }
                    let stable = stable.clone();
                    stable.admission.revoke();
                    let completion = AlpacaHistoricalDrainCompletion::new();
                    authority.alpaca_historical = AlpacaHistoricalSourceSlot::Draining {
                        stable: stable.clone(),
                        completion: Arc::clone(&completion),
                    };
                    Action::StartDrain(stable, completion)
                }
                AlpacaHistoricalSourceSlot::Stopped(stable) => {
                    if expected_parent.is_some_and(|expected| expected != stable.parent) {
                        return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                    }
                    Action::Done
                }
            }
        };
        match action {
            Action::Done => return Ok(()),
            Action::WaitInstall(completion) => {
                match completion.wait(deadline, cancellation).await {
                    Err(
                        error @ (AlpacaHistoricalSourceSlotError::WaitCancelled
                        | AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded),
                    ) => return Err(error),
                    Ok(_) | Err(_) => {}
                }
            }
            Action::WaitDrain(completion) => return completion.wait(deadline, cancellation).await,
            Action::StartDrain(stable, completion) => {
                spawn_drain_worker(
                    Arc::clone(&coordinator.authority),
                    stable,
                    Arc::clone(&completion),
                    coordinator.limits.operation_duration,
                );
                return completion.wait(deadline, cancellation).await;
            }
        }
    }
}

fn spawn_drain_worker(
    authority: Arc<Mutex<super::CoordinatorAuthority>>,
    stable: AlpacaHistoricalStableSlot,
    completion: Arc<AlpacaHistoricalDrainCompletion>,
    operation_duration: std::time::Duration,
) {
    tokio::spawn(async move {
        let drained = match Instant::now().checked_add(operation_duration) {
            Some(worker_deadline) => {
                tokio::select! {
                    biased;
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(worker_deadline)) => {
                        Err(AlpacaHistoricalSourceSlotError::DrainIncomplete)
                    }
                    () = stable.admission.revoke_and_drain() => Ok(()),
                }
            }
            None => Err(AlpacaHistoricalSourceSlotError::DrainIncomplete),
        };
        let mut authority = authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let exact = matches!(
            &authority.alpaca_historical,
            AlpacaHistoricalSourceSlot::Draining {
                stable: current,
                completion: current_completion,
            } if current.parent == stable.parent
                && current.admission.matches(&stable.admission)
                && Arc::ptr_eq(current_completion, &completion)
        );
        let result = if !exact {
            Err(AlpacaHistoricalSourceSlotError::StaleParent)
        } else if let Err(error) = drained {
            authority.alpaca_historical =
                AlpacaHistoricalSourceSlot::ReconciliationRequired(stable);
            Err(error)
        } else {
            authority.alpaca_historical = AlpacaHistoricalSourceSlot::Stopped(stable);
            Ok(())
        };
        drop(authority);
        completion.complete(result);
    });
}

async fn publish_candidate(
    coordinator: &ProductionResearchIngestCoordinator,
    parent: AlpacaHistoricalParentGeneration,
    operation_id: Uuid,
    expected_metadata: SourceMetadata,
    expected_rights: ResearchRightsAuthority,
    candidate: PreparedAlpacaHistoricalSourceInstall,
    validation_deadline: Instant,
) -> Result<AlpacaHistoricalPlanDirectoryLease, AlpacaHistoricalSourceSlotError> {
    if candidate.parent != parent
        || candidate.metadata != expected_metadata
        || candidate.rights != expected_rights
        || candidate.source.metadata() != &candidate.metadata
    {
        restore_pre_registration_failure(coordinator, parent, operation_id);
        return Err(AlpacaHistoricalSourceSlotError::InvalidCandidate);
    }
    candidate
        .admission
        .ensure_pending()
        .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidCandidate)?;
    candidate.directory.validate_parent_now(parent)?;
    let validation_cancellation = CancellationToken::new();
    candidate
        .directory
        .validate_parent(parent, validation_deadline, &validation_cancellation)
        .await?;
    let profile = canonical_alpaca_historical_profile()?;
    let registered_at = super::system_timestamp()
        .map_err(|_error| AlpacaHistoricalSourceSlotError::TrustedTimeUnavailable)?;
    let stable = AlpacaHistoricalStableSlot {
        parent,
        directory: Arc::clone(&candidate.directory),
        admission: candidate.admission.clone(),
        metadata: candidate.metadata.clone(),
        rights: candidate.rights.clone(),
    };
    let post_registration_error = {
        enum PublicationOutcome {
            Active,
            Stopped(AlpacaHistoricalSourceSlotError),
        }

        let mut authority = coordinator
            .authority
            .lock()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)?;
        if coordinator.lifecycle.shutdown_token().is_cancelled() || authority.registry.is_none() {
            return Err(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable);
        }
        let predecessor = match &authority.alpaca_historical {
            AlpacaHistoricalSourceSlot::Installing {
                parent: current,
                operation_id: current_operation,
                predecessor,
                ..
            } if *current == parent && *current_operation == operation_id => predecessor.clone(),
            _ => return Err(AlpacaHistoricalSourceSlotError::StaleParent),
        };
        candidate.directory.validate_parent_now(parent)?;

        let outcome = {
            let CoordinatorAuthorityFields { registry, sources } =
                CoordinatorAuthorityFields::from(&mut *authority);
            match sources.get_mut(&profile) {
                None => {
                    if predecessor.is_some() {
                        return Err(AlpacaHistoricalSourceSlotError::InvalidCandidate);
                    }
                    let registration = registry
                        .register_or_resume_exact(candidate.metadata.clone(), registered_at)
                        .map_err(|_error| AlpacaHistoricalSourceSlotError::RegistryUnavailable)?;
                    let post_registration_error =
                        if coordinator.lifecycle.shutdown_token().is_cancelled() {
                            Some(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)
                        } else {
                            candidate.directory.validate_parent_now(parent).err()
                        };
                    if post_registration_error.is_some() {
                        candidate.admission.revoke();
                    } else {
                        candidate.admission.activate_pending();
                    }
                    sources.insert(
                        profile.clone(),
                        RegisteredExtractionSource {
                            source: Arc::clone(&candidate.source),
                            metadata: candidate.metadata.clone(),
                            registration: Box::new(registration),
                            rights: candidate.rights.clone(),
                            generation: None,
                            admission: candidate.admission.clone(),
                        },
                    );
                    post_registration_error
                        .map_or(PublicationOutcome::Active, PublicationOutcome::Stopped)
                }
                Some(current) => {
                    if predecessor.as_ref().is_none_or(|prior| {
                        !prior.admission.revocation_drained()
                            || !current.admission.matches(&prior.admission)
                    }) {
                        return Err(AlpacaHistoricalSourceSlotError::DrainIncomplete);
                    }
                    if current.metadata == candidate.metadata {
                        if coordinator.lifecycle.shutdown_token().is_cancelled() {
                            return Err(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable);
                        }
                        candidate.directory.validate_parent_now(parent)?;
                        candidate.admission.activate_pending();
                        current.source = Arc::clone(&candidate.source);
                        current.rights = candidate.rights.clone();
                        current.generation = None;
                        current.admission = candidate.admission.clone();
                        PublicationOutcome::Active
                    } else {
                        let registration = registry
                            .replace_metadata(
                                &current.registration,
                                candidate.metadata.clone(),
                                registered_at,
                            )
                            .map_err(|_error| {
                                AlpacaHistoricalSourceSlotError::RegistryUnavailable
                            })?;
                        let post_registration_error =
                            if coordinator.lifecycle.shutdown_token().is_cancelled() {
                                Some(AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)
                            } else {
                                candidate.directory.validate_parent_now(parent).err()
                            };
                        if post_registration_error.is_some() {
                            candidate.admission.revoke();
                        } else {
                            candidate.admission.activate_pending();
                        }
                        current.source = Arc::clone(&candidate.source);
                        current.metadata = candidate.metadata.clone();
                        current.registration = Box::new(registration);
                        current.rights = candidate.rights.clone();
                        current.generation = None;
                        current.admission = candidate.admission.clone();
                        post_registration_error
                            .map_or(PublicationOutcome::Active, PublicationOutcome::Stopped)
                    }
                }
            }
        };
        match outcome {
            PublicationOutcome::Active => {
                authority.alpaca_historical = AlpacaHistoricalSourceSlot::Active(stable.clone());
                None
            }
            PublicationOutcome::Stopped(error) => {
                authority.alpaca_historical = AlpacaHistoricalSourceSlot::Stopped(stable.clone());
                Some(error)
            }
        }
    };
    if let Some(error) = post_registration_error {
        candidate.admission.revoke_and_drain().await;
        return Err(error);
    }
    Ok(AlpacaHistoricalPlanDirectoryLease {
        parent,
        admission: stable.admission,
        directory: stable.directory,
    })
}

struct CoordinatorAuthorityFields<'a> {
    registry: &'a mut market_squawk_sources::AuthoritativeSourceRegistry,
    sources: &'a mut std::collections::BTreeMap<SourceIdentifier, RegisteredExtractionSource>,
}

impl<'a> From<&'a mut super::CoordinatorAuthority> for CoordinatorAuthorityFields<'a> {
    fn from(authority: &'a mut super::CoordinatorAuthority) -> Self {
        Self {
            registry: authority
                .registry
                .as_mut()
                .expect("registry checked before field split"),
            sources: &mut authority.sources,
        }
    }
}

fn restore_pre_registration_failure(
    coordinator: &ProductionResearchIngestCoordinator,
    parent: AlpacaHistoricalParentGeneration,
    operation_id: Uuid,
) {
    let Ok(mut authority) = coordinator.authority.lock() else {
        return;
    };
    let replacement = match &authority.alpaca_historical {
        AlpacaHistoricalSourceSlot::Installing {
            parent: current,
            operation_id: current_operation,
            predecessor,
            ..
        } if *current == parent && *current_operation == operation_id => {
            predecessor.clone().map_or(
                AlpacaHistoricalSourceSlot::Absent,
                AlpacaHistoricalSourceSlot::Stopped,
            )
        }
        _ => return,
    };
    authority.alpaca_historical = replacement;
}

pub(super) fn canonical_alpaca_historical_profile()
-> Result<SourceIdentifier, AlpacaHistoricalSourceSlotError> {
    SourceIdentifier::try_from(ALPACA_HISTORICAL_PROFILE)
        .map_err(|_error| AlpacaHistoricalSourceSlotError::InvalidParent)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AlpacaHistoricalSourceSlotError {
    #[error("the Alpaca historical parent is invalid")]
    InvalidParent,
    #[error("the Alpaca historical installation candidate is invalid")]
    InvalidCandidate,
    #[error("the requested Alpaca historical parent is stale")]
    StaleParent,
    #[error("the Alpaca historical directory lease is stale")]
    StaleLease,
    #[error("the Alpaca historical wait was cancelled")]
    WaitCancelled,
    #[error("the Alpaca historical wait deadline elapsed")]
    WaitDeadlineExceeded,
    #[error("the Alpaca historical coordinator is unavailable")]
    CoordinatorUnavailable,
    #[error("the Alpaca historical durable registry is unavailable")]
    RegistryUnavailable,
    #[error("the Alpaca historical predecessor has not drained")]
    DrainIncomplete,
    #[error("trusted time is unavailable")]
    TrustedTimeUnavailable,
}

/// Sole mutation authority for the immutable plan directory of one account generation.
///
/// This value is intentionally non-cloneable. It carries no credentials, provider-rate budget,
/// registry mutation, or source-replacement authority.
struct AlpacaHistoricalPlanDirectoryAuthority {
    inner: Arc<AlpacaHistoricalPlanDirectoryInner>,
}

/// Secret-free source handle intended for one later serialized registry installation.
///
/// The handle is intentionally non-cloneable. Discovery and extraction consume only retained raw
/// preflight bytes; its opaque runtime capability supplies currentness and is unusable after the
/// owner drains and clears the separate credential-bearing authority.
struct AlpacaHistoricalManagedSource {
    inner: Arc<AlpacaHistoricalPlanDirectoryInner>,
}

struct AlpacaHistoricalPlanDirectoryInner {
    metadata: SourceMetadata,
    request_bounds: HttpRequestBounds,
    parent_digest: EvidenceDigest,
    runtime: AlpacaHistoricalRuntimeCapability,
    admission: tokio::sync::Mutex<()>,
    plans: Mutex<Vec<Arc<AlpacaHistoricalPlanRecord>>>,
}

#[derive(Debug)]
struct AlpacaHistoricalPlanRecord {
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    parent_digest: EvidenceDigest,
    plan_digest: EvidenceDigest,
    config: AlpacaHistoricalEquityConfig,
    canonical_instrument: MarketDataInstrumentDefinition,
    preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
    bar_time_authority: Arc<AlpacaHistoricalCompositeCalendarAuthority>,
    retained_response_bytes: usize,
}

impl AlpacaHistoricalPlanRecord {
    fn same_identity(&self, other: &Self) -> bool {
        self.provider_dataset == other.provider_dataset
            && self.analytical_dataset == other.analytical_dataset
            && self.parent_digest == other.parent_digest
            && self.plan_digest == other.plan_digest
            && self.config == other.config
            && self.canonical_instrument == other.canonical_instrument
            && self.preflight.digest() == other.preflight.digest()
            && self.preflight.as_ref() == other.preflight.as_ref()
            && self.bar_time_authority.preflight_digest()
                == other.bar_time_authority.preflight_digest()
            && self.bar_time_authority.series_semantics()
                == other.bar_time_authority.series_semantics()
            && self.retained_response_bytes == other.retained_response_bytes
    }
}

/// Non-forgeable proof that one exact content-addressed plan was admitted.
pub(crate) struct AlpacaHistoricalPlanReceipt {
    parent: AlpacaHistoricalParentGeneration,
    plan: AlpacaHistoricalAdmittedPlan,
    admission: ResearchProviderAdmission,
    _private: (),
}

impl fmt::Debug for AlpacaHistoricalPlanReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalPlanReceipt")
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

impl AlpacaHistoricalPlanReceipt {
    /// Matches only the private parent group needed by the market-registry lock hierarchy.
    pub(crate) fn matches_group_generation(
        &self,
        group_generation: MarketRuntimeGroupGeneration,
    ) -> bool {
        self.parent.group_generation() == group_generation
    }
}

/// Coordinator-validated receipt state that does not retain a publication barrier.
///
/// This intermediate token exposes no plan coordinates. It is intentionally non-cloneable so the
/// caller must acquire the exact admission barrier or discard the validation result.
pub(crate) struct AlpacaHistoricalValidatedPlan<'receipt> {
    plan: &'receipt AlpacaHistoricalAdmittedPlan,
    admission: ResearchProviderAdmission,
}

impl<'receipt> AlpacaHistoricalValidatedPlan<'receipt> {
    /// Acquires the exact receipt admission after coordinator validation has released its lock.
    pub(crate) async fn authorize(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalAuthorizedPlan<'receipt>, AlpacaHistoricalSourceSlotError> {
        ensure_plan_receipt_wait_current(deadline, cancellation)?;
        let Self { plan, admission } = self;
        let publication = tokio::select! {
            biased;
            () = admission.cancellation().cancelled() => {
                return Err(AlpacaHistoricalSourceSlotError::StaleLease);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalSourceSlotError::WaitCancelled);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalSourceSlotError::WaitDeadlineExceeded);
            }
            result = admission.acquire_publication_lease() => {
                result.map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?
            }
        };
        publication
            .validate_precommit()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?;
        admission
            .ensure_live()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)?;
        ensure_plan_receipt_wait_current(deadline, cancellation)?;
        Ok(AlpacaHistoricalAuthorizedPlan { plan, publication })
    }
}

/// Exact current plan coordinates retained with the admission publication barrier.
///
/// This view is intentionally non-cloneable and is minted only by the coordinator's receipt
/// consumption path. Receipt storage alone therefore exposes no provider or analytical dataset
/// coordinates to later application waves.
pub(crate) struct AlpacaHistoricalAuthorizedPlan<'receipt> {
    plan: &'receipt AlpacaHistoricalAdmittedPlan,
    publication: ResearchProviderPublicationLease,
}

impl AlpacaHistoricalAuthorizedPlan<'_> {
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.plan.provider_dataset
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.plan.analytical_dataset
    }

    /// Revalidates the retained publication authority immediately before downstream commit.
    pub(crate) fn validate_current(&self) -> Result<(), AlpacaHistoricalSourceSlotError> {
        self.publication
            .validate_precommit()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleLease)
    }
}

impl AlpacaHistoricalPlanDirectoryAuthority {
    /// Constructs the sole source/directory pair for one exact active account generation.
    ///
    /// The dedicated coordinator slot decides whether the returned pending candidate may become
    /// the canonical profile; this constructor does not mint or retain slot mutation authority.
    async fn try_new(
        runtime: AlpacaHistoricalRuntimeCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            AlpacaHistoricalParentGeneration,
            Self,
            AlpacaHistoricalManagedSource,
        ),
        AlpacaHistoricalPlanAdmissionError,
    > {
        runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let metadata = runtime.historical_metadata().clone();
        let request_bounds = runtime.historical_request_bounds();
        let rights = runtime.historical_rights().clone();
        validate_parent_binding(&runtime, &metadata, &rights, request_bounds)?;
        AlpacaHistoricalEquityConfig::validate_parent_metadata(&metadata, request_bounds)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::ParentBindingMismatch)?;
        let parent = AlpacaHistoricalParentGeneration::try_from_runtime(&runtime)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::ParentBindingMismatch)?;
        let parent_digest = parent.binding_digest();
        let mut plans = Vec::new();
        plans
            .try_reserve_exact(MAXIMUM_ALPACA_HISTORICAL_PLANS)
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::CapacityUnavailable)?;
        let inner = Arc::new(AlpacaHistoricalPlanDirectoryInner {
            metadata,
            request_bounds,
            parent_digest,
            runtime,
            admission: tokio::sync::Mutex::new(()),
            plans: Mutex::new(plans),
        });
        Ok((
            parent,
            Self {
                inner: Arc::clone(&inner),
            },
            AlpacaHistoricalManagedSource { inner },
        ))
    }

    /// Admits or idempotently resolves one exact repository-owned instrument click plan.
    async fn admit_plan(
        &self,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<AlpacaHistoricalAdmittedPlan, AlpacaHistoricalPlanAdmissionError> {
        // A single generation owns this directory's mutation authority. Serializing the bounded
        // network preflight with publication prevents concurrent identical clicks from minting
        // different observation-time receipts before either immutable record becomes visible.
        let _admission = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable);
            }
            admission = self.inner.admission.lock() => admission,
        };
        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let timeframe = preflight_plan
            .timeframe()
            .provider_identifier()
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        if timeframe.as_str() != "1Day" {
            return Err(AlpacaHistoricalPlanAdmissionError::CalendarUnavailable);
        }
        AlpacaHistoricalEquitySource::validate_one_preflight_instrument(
            &self.inner.metadata,
            &preflight_plan,
            &canonical_instrument,
        )
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        {
            let plans = self
                .inner
                .plans
                .lock()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::DirectoryUnavailable)?;
            if let Some(existing) = plans
                .iter()
                .find(|record| record.preflight.plan() == &preflight_plan)
            {
                if existing.canonical_instrument == canonical_instrument {
                    return Ok(admitted_plan(existing));
                }
                return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
            }
            if plans.len() == MAXIMUM_ALPACA_HISTORICAL_PLANS {
                return Err(AlpacaHistoricalPlanAdmissionError::PlanCapacityExceeded);
            }
        }
        let provider_instrument_id =
            ProviderInstrumentId::try_from(preflight_plan.mapping().symbol().to_owned())
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        let preflight = self
            .inner
            .runtime
            .preflight_plan(
                preflight_plan,
                self.inner.request_bounds,
                deadline,
                cancellation,
            )
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::PreflightUnavailable)?;
        let bar_time_authority = self
            .inner
            .runtime
            .compose_returned_bar_calendar(
                &preflight,
                preflight.plan().mapping().instrument(),
                provider_instrument_id,
                self.inner.request_bounds,
                deadline,
                cancellation,
            )
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::CalendarUnavailable)?;
        let plan = AlpacaHistoricalEquityDatasetPlan::bind_preflight(
            preflight.plan().clone(),
            bar_time_authority.series_semantics().clone(),
        );
        let config = AlpacaHistoricalEquityConfig::try_bind_one_plan(
            self.inner.metadata.clone(),
            plan,
            self.inner.request_bounds,
        )
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        let provider_dataset = exactly_one_provider_dataset(&config)?;
        let analytical_identifier =
            AlpacaHistoricalEquitySource::one_plan_analytical_dataset_identifier(
                &config,
                &canonical_instrument,
            )
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidInstrumentAuthority)?;
        let analytical_dataset = DatasetId::try_from(analytical_identifier.as_str())
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
        let plan_digest = plan_binding_digest(
            self.inner.parent_digest,
            &config,
            &canonical_instrument,
            &provider_dataset,
            &analytical_dataset,
            preflight.digest(),
            bar_time_authority.preflight_digest(),
        )?;
        let retained_response_bytes = preflight
            .total_response_bytes()
            .checked_add(bar_time_authority.retained_response_bytes())
            .filter(|bytes| *bytes <= MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES)
            .ok_or(AlpacaHistoricalPlanAdmissionError::RetainedResponseCapacityExceeded)?;
        let candidate = Arc::new(AlpacaHistoricalPlanRecord {
            provider_dataset,
            analytical_dataset,
            parent_digest: self.inner.parent_digest,
            plan_digest,
            config,
            canonical_instrument,
            preflight,
            bar_time_authority,
            retained_response_bytes,
        });

        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        let record = {
            let mut plans = self
                .inner
                .plans
                .lock()
                .map_err(|_error| AlpacaHistoricalPlanAdmissionError::DirectoryUnavailable)?;
            let position = plans.binary_search_by(|record| {
                record
                    .provider_dataset
                    .as_str()
                    .cmp(candidate.provider_dataset.as_str())
            });
            match position {
                Ok(index) if plans[index].same_identity(candidate.as_ref()) => {
                    Arc::clone(&plans[index])
                }
                Ok(_index) => return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision),
                Err(_index)
                    if plans
                        .iter()
                        .any(|record| record.plan_digest == candidate.plan_digest) =>
                {
                    return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
                }
                Err(_index) if plans.len() == MAXIMUM_ALPACA_HISTORICAL_PLANS => {
                    return Err(AlpacaHistoricalPlanAdmissionError::PlanCapacityExceeded);
                }
                Err(index) => {
                    let retained = plans
                        .iter()
                        .try_fold(candidate.retained_response_bytes, |total, record| {
                            total.checked_add(record.retained_response_bytes)
                        });
                    if retained.is_none_or(|bytes| {
                        bytes > MAXIMUM_ALPACA_HISTORICAL_RETAINED_RESPONSE_BYTES
                    }) {
                        return Err(
                            AlpacaHistoricalPlanAdmissionError::RetainedResponseCapacityExceeded,
                        );
                    }
                    plans.insert(index, Arc::clone(&candidate));
                    candidate
                }
            }
        };
        self.inner
            .runtime
            .require_current(deadline, cancellation)
            .await
            .map_err(|_error| AlpacaHistoricalPlanAdmissionError::RuntimeUnavailable)?;
        Ok(admitted_plan(record.as_ref()))
    }
}

impl AlpacaHistoricalPlanAdmissionDirectory for AlpacaHistoricalPlanDirectoryAuthority {
    fn admit_plan<'a>(
        &'a self,
        preflight_plan: AlpacaHistoricalEquityPreflightPlan,
        canonical_instrument: MarketDataInstrumentDefinition,
        deadline: Instant,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<AlpacaHistoricalAdmittedPlan, AlpacaHistoricalPlanAdmissionError>>
    {
        Box::pin(AlpacaHistoricalPlanDirectoryAuthority::admit_plan(
            self,
            preflight_plan,
            canonical_instrument,
            deadline,
            cancellation,
        ))
    }

    fn validate_parent<'a>(
        &'a self,
        parent: AlpacaHistoricalParentGeneration,
        deadline: Instant,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AlpacaHistoricalSourceSlotError>> {
        Box::pin(async move {
            self.inner
                .runtime
                .require_current(deadline, cancellation)
                .await
                .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleParent)?;
            let current = AlpacaHistoricalParentGeneration::try_from_runtime(&self.inner.runtime)?;
            if current != parent || current.binding_digest() != self.inner.parent_digest {
                return Err(AlpacaHistoricalSourceSlotError::StaleParent);
            }
            Ok(())
        })
    }

    fn validate_parent_now(
        &self,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        self.inner
            .runtime
            .validate_current_now()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::StaleParent)?;
        let current = AlpacaHistoricalParentGeneration::try_from_runtime(&self.inner.runtime)?;
        if current != parent || current.binding_digest() != self.inner.parent_digest {
            return Err(AlpacaHistoricalSourceSlotError::StaleParent);
        }
        Ok(())
    }

    fn validate_plan_now(
        &self,
        plan: &AlpacaHistoricalAdmittedPlan,
    ) -> Result<(), AlpacaHistoricalSourceSlotError> {
        if plan.parent_digest != self.inner.parent_digest {
            return Err(AlpacaHistoricalSourceSlotError::StaleLease);
        }
        let plans = self
            .inner
            .plans
            .lock()
            .map_err(|_error| AlpacaHistoricalSourceSlotError::CoordinatorUnavailable)?;
        if plans.iter().any(|record| {
            record.provider_dataset == plan.provider_dataset
                && record.analytical_dataset == plan.analytical_dataset
                && record.parent_digest == plan.parent_digest
                && record.plan_digest == plan.plan_digest
        }) {
            Ok(())
        } else {
            Err(AlpacaHistoricalSourceSlotError::StaleLease)
        }
    }
}

impl fmt::Debug for AlpacaHistoricalPlanDirectoryAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalPlanDirectoryAuthority")
            .field("parent_digest", &self.inner.parent_digest)
            .finish_non_exhaustive()
    }
}

impl SourceMetadataProvider for AlpacaHistoricalManagedSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.inner.metadata
    }
}

impl ExtractionSource for AlpacaHistoricalManagedSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        let record = match self.inner.plan(request.dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority> =
            record.bar_time_authority.clone();
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            runtime
                .discover_plan(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let record = match self.inner.plan(request.object().dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority> =
            record.bar_time_authority.clone();
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            runtime
                .extract_plan(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await
        })
    }
}

impl ManagedResearchExtractionSource for AlpacaHistoricalManagedSource {
    fn extract_managed(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ManagedExtraction, ExtractionSourceError>> {
        let record = match self.inner.plan(request.object().dataset()) {
            Ok(record) => record,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let runtime = self.inner.runtime.clone();
        let bar_time_authority = Arc::clone(&record.bar_time_authority);
        let preflight = Arc::clone(&record.preflight);
        Box::pin(async move {
            let output = runtime
                .extract_plan_with_capture(
                    record.config.clone(),
                    record.canonical_instrument.clone(),
                    record.plan_digest,
                    bar_time_authority,
                    preflight,
                    authority,
                    request,
                    cancellation,
                )
                .await?;
            let (batch, bar_capture, calendar_capture, history_capture_semantic) =
                output.into_parts();
            bind_complete_market_history_capture_graph(
                batch,
                vec![bar_capture, calendar_capture],
                ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(
                    history_capture_semantic,
                ),
            )
        })
    }

    fn analytical_dataset(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<DatasetId, ResearchRevisionPlanError> {
        let record = self
            .inner
            .plan(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        let identifier = self
            .inner
            .runtime
            .analytical_dataset_for_plan(&record.config, &record.canonical_instrument, batch)
            .map_err(|_error| ResearchRevisionPlanError)?;
        let dataset =
            DatasetId::try_from(identifier.as_str()).map_err(|_error| ResearchRevisionPlanError)?;
        if dataset != record.analytical_dataset {
            return Err(ResearchRevisionPlanError);
        }
        Ok(dataset)
    }

    fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
        let record = self
            .inner
            .plan(batch.request().object().dataset())
            .map_err(|_error| ResearchRevisionPlanError)?;
        self.inner
            .runtime
            .revision_plan_for_plan(&record.config, batch)
            .map(Some)
            .map_err(|_error| ResearchRevisionPlanError)
    }
}

fn bind_complete_market_history_capture_graph(
    batch: ExtractionBatch,
    components: Vec<ProviderCaptureMaterial>,
    semantic_binding: ProviderCaptureSemanticBinding,
) -> Result<ManagedExtraction, ExtractionSourceError> {
    let dataset = batch.request().object().dataset().clone();
    let capture_material = ProviderCaptureMaterial::try_combine_request_graph_with_semantic(
        dataset,
        components,
        semantic_binding,
    )
    .map_err(|_error| invalid_capture_protocol())?;
    let batch = batch
        .try_bind_provider_capture(capture_material.receipt())
        .map_err(|_error| invalid_capture_protocol())?;
    Ok(ManagedExtraction {
        batch,
        company_identity: None,
        capture_material: Some(capture_material),
    })
}

impl AlpacaHistoricalPlanDirectoryInner {
    fn plan(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<Arc<AlpacaHistoricalPlanRecord>, ExtractionSourceError> {
        if self.runtime.is_revoked() {
            return Err(SourceError::SessionNotCurrent.into());
        }
        let plans = self
            .plans
            .lock()
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let index = plans
            .binary_search_by(|record| record.provider_dataset.as_str().cmp(dataset.as_str()))
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        Ok(Arc::clone(&plans[index]))
    }
}

impl fmt::Debug for AlpacaHistoricalManagedSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalManagedSource")
            .field("source_id", self.inner.metadata.source_id())
            .field("revision", self.inner.metadata.revision())
            .field("parent_digest", &self.inner.parent_digest)
            .field("credentials", &"[RUNTIME-OWNED; NOT RETAINED BY PLAN]")
            .field("revoked", &self.inner.runtime.is_revoked())
            .finish_non_exhaustive()
    }
}

/// Closed fail-safe outcomes for source construction and immutable plan admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AlpacaHistoricalPlanAdmissionError {
    #[error("Alpaca historical account runtime is unavailable or stale")]
    RuntimeUnavailable,
    #[error("Alpaca historical parent metadata is not bound to this account generation")]
    ParentBindingMismatch,
    #[error("the exact governed Alpaca calendar is unavailable")]
    CalendarUnavailable,
    #[error("the exact terminal Alpaca historical preflight is unavailable")]
    PreflightUnavailable,
    #[error("the Alpaca historical plan is invalid")]
    InvalidPlan,
    #[error("the canonical instrument/provider identity does not authorize this plan")]
    InvalidInstrumentAuthority,
    #[error("the fixed plan directory allocation is unavailable")]
    CapacityUnavailable,
    #[error("the fixed Alpaca historical plan directory is full")]
    PlanCapacityExceeded,
    #[error("retained exact historical responses exceed the directory byte budget")]
    RetainedResponseCapacityExceeded,
    #[error("a content-addressed plan identity collision was detected")]
    IdentityCollision,
    #[error("the plan directory is unavailable")]
    DirectoryUnavailable,
    #[error("canonical plan evidence could not be serialized")]
    Serialization,
}

fn validate_parent_binding(
    runtime: &AlpacaHistoricalRuntimeCapability,
    metadata: &SourceMetadata,
    rights: &ResearchRightsAuthority,
    request_bounds: HttpRequestBounds,
) -> Result<(), AlpacaHistoricalPlanAdmissionError> {
    let binding = runtime.account_binding();
    if binding.account() != ProviderMarketAccount::AlpacaBasic
        || runtime.surface_id().as_str() != ProviderMarketAccount::AlpacaBasic.surface_id()
        || runtime.onboarding_session_id().is_nil()
        || metadata.provider().as_str() != "alpaca-market-data"
        || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
        || metadata.authorization().basis().as_source_identifier() != binding.subject()
        || metadata.authorization().evidence().content_digest() != binding.verification_evidence()
        || metadata != runtime.historical_metadata()
        || request_bounds != runtime.historical_request_bounds()
        || rights != runtime.historical_rights()
        || metadata.source_id() != rights.source_id()
        || runtime.group_generation().digest().algorithm() != DigestAlgorithm::Sha256
        || runtime.group_generation().digest().bytes() == [0; 32]
    {
        return Err(AlpacaHistoricalPlanAdmissionError::ParentBindingMismatch);
    }
    Ok(())
}

fn exactly_one_provider_dataset(
    config: &AlpacaHistoricalEquityConfig,
) -> Result<SourceIdentifier, AlpacaHistoricalPlanAdmissionError> {
    let mut datasets = config.provider_dataset_identifiers();
    let dataset = datasets
        .next()
        .cloned()
        .ok_or(AlpacaHistoricalPlanAdmissionError::InvalidPlan)?;
    if datasets.next().is_some() {
        return Err(AlpacaHistoricalPlanAdmissionError::InvalidPlan);
    }
    Ok(dataset)
}

fn parent_binding_digest(
    runtime: &AlpacaHistoricalRuntimeCapability,
    metadata: &SourceMetadata,
    request_bounds: HttpRequestBounds,
    rights: &ResearchRightsAuthority,
) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
    validate_parent_binding(runtime, metadata, rights, request_bounds)?;
    parent_binding_digest_v2(AlpacaHistoricalParentDigestInput {
        group_digest: runtime.group_generation().digest(),
        surface_id: runtime.surface_id(),
        onboarding_session_id: runtime.onboarding_session_id(),
        credential_generation: runtime.credential_generation().get(),
        account_subject: runtime.account_binding().subject(),
        account_verification_evidence: runtime.account_binding().verification_evidence(),
        account_digest: runtime.account_digest(),
        public_configuration_digest: runtime.public_configuration_digest(),
        runtime_evidence_digest: runtime.runtime_evidence_digest(),
        trading_api_environment: runtime.trading_api_environment(),
        metadata,
        request_bounds,
        rights,
    })
}

struct AlpacaHistoricalParentDigestInput<'a> {
    group_digest: EvidenceDigest,
    surface_id: &'a SourceIdentifier,
    onboarding_session_id: Uuid,
    credential_generation: u64,
    account_subject: &'a SourceIdentifier,
    account_verification_evidence: EvidenceDigest,
    account_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    runtime_evidence_digest: EvidenceDigest,
    trading_api_environment: market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment,
    metadata: &'a SourceMetadata,
    request_bounds: HttpRequestBounds,
    rights: &'a ResearchRightsAuthority,
}

fn parent_binding_digest_v2(
    input: AlpacaHistoricalParentDigestInput<'_>,
) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
    let metadata = serde_json::to_vec(input.metadata)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let request_bounds = serde_json::to_vec(&input.request_bounds)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-parent-generation/v2\0");
    hash_evidence_digest(&mut digest, input.group_digest);
    hash_bytes(&mut digest, input.surface_id.as_str().as_bytes());
    digest.update(input.onboarding_session_id.as_bytes());
    digest.update(input.credential_generation.to_be_bytes());
    hash_bytes(&mut digest, input.account_subject.as_str().as_bytes());
    hash_evidence_digest(&mut digest, input.account_verification_evidence);
    hash_evidence_digest(&mut digest, input.account_digest);
    hash_evidence_digest(&mut digest, input.public_configuration_digest);
    hash_evidence_digest(&mut digest, input.runtime_evidence_digest);
    digest.update([match input.trading_api_environment {
        market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment::Live => 1,
        market_squawk_adapter_alpaca::AlpacaTradingApiEnvironment::Paper => 2,
    }]);
    hash_bytes(&mut digest, &metadata);
    hash_bytes(&mut digest, &request_bounds);
    hash_bytes(&mut digest, input.rights.source_id.as_str().as_bytes());
    hash_bytes(&mut digest, input.rights.basis.reference().as_bytes());
    hash_evidence_digest(&mut digest, input.rights.basis.digest());
    match input.rights.basis.root_identity_digest() {
        Some(root) => {
            digest.update([1]);
            hash_evidence_digest(&mut digest, root);
        }
        None => digest.update([0]),
    }
    hash_evidence_digest(&mut digest, input.rights.parent_authorization_evidence);
    hash_evidence_digest(&mut digest, input.rights.authorization_evidence);
    match input.rights.authorization_expires_at {
        Some(expires_at) => {
            digest.update([1]);
            digest.update(expires_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    match &input.rights.exact_subjects {
        Some(subjects) => {
            digest.update([1]);
            digest.update(
                u64::try_from(subjects.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            for subject in subjects {
                hash_bytes(&mut digest, subject.as_str().as_bytes());
            }
        }
        None => digest.update([0]),
    }
    digest.update(
        u64::try_from(input.rights.permitted_operations.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for operation in &input.rights.permitted_operations {
        digest.update([source_operation_tag(*operation)]);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_evidence_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

const fn source_operation_tag(operation: SourceOperation) -> u8 {
    match operation {
        SourceOperation::Retrieve => 1,
        SourceOperation::Display => 2,
        SourceOperation::Persist => 3,
        SourceOperation::Cache => 4,
        SourceOperation::Redistribute => 5,
        SourceOperation::Train => 6,
    }
}

fn plan_binding_digest(
    parent_digest: EvidenceDigest,
    config: &AlpacaHistoricalEquityConfig,
    canonical_instrument: &MarketDataInstrumentDefinition,
    provider_dataset: &SourceIdentifier,
    analytical_dataset: &DatasetId,
    preflight_digest: EvidenceDigest,
    authority_preflight_digest: EvidenceDigest,
) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
    if preflight_digest != authority_preflight_digest {
        return Err(AlpacaHistoricalPlanAdmissionError::IdentityCollision);
    }
    let metadata = serde_json::to_vec(config.metadata())
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let canonical_instrument = serde_json::to_vec(canonical_instrument)
        .map_err(|_error| AlpacaHistoricalPlanAdmissionError::Serialization)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/alpaca-historical-admitted-plan/v1\0");
    digest.update(parent_digest.bytes());
    hash_bytes(&mut digest, provider_dataset.as_str().as_bytes());
    hash_bytes(&mut digest, analytical_dataset.as_str().as_bytes());
    digest.update(preflight_digest.bytes());
    hash_bytes(&mut digest, &metadata);
    hash_bytes(&mut digest, &canonical_instrument);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

fn admitted_plan(record: &AlpacaHistoricalPlanRecord) -> AlpacaHistoricalAdmittedPlan {
    AlpacaHistoricalAdmittedPlan {
        provider_dataset: record.provider_dataset.clone(),
        analytical_dataset: record.analytical_dataset.clone(),
        parent_digest: record.parent_digest,
        plan_digest: record.plan_digest,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU16, NonZeroU32, NonZeroU64},
        str::FromStr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::future::BoxFuture;
    use market_squawk_adapter_alpaca::{
        AlpacaAdjustment, AlpacaHistoricalLookback, AlpacaInstrumentMapping, AlpacaTimeframe,
        AlpacaTradingApiEnvironment,
    };
    use market_squawk_data::{
        CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig, RightsBasis,
    };
    use market_squawk_domain::{
        AssetClass, AuthorizationBasis, Currency, DataQuality, EffectiveInterval,
        ExactPayloadEvidence, InstrumentId, MarketDataInstrumentDefinitionInput, MetadataRevision,
        RevisionBoundPayloadEvidence, SourceId, Timestamp, VenueId, VenueMapping, VenueSymbol,
    };
    use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
    use market_squawk_sources::{
        AuthorizationGrant, AuthorizationMode, AuthorizationSubjectResolutionError,
        AuthorizationSubjectResolver, BackoffPolicy, BudgetScope, DiscoveryBatch, DiscoveryRequest,
        ExtractionAuthority, ExtractionBatch, ExtractionRequest, ExtractionSource, FreshnessPolicy,
        HttpRequestBounds, ProviderBudgetPolicy,
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::ResearchService;
    use crate::application::{
        ProductionResearchIngestCoordinator, ResearchExtractionLimits, ResearchRightsAuthority,
    };

    #[tokio::test]
    async fn exact_parent_install_coalesces_and_successor_replaces_only_fresh_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let (coordinator, mutation) = test_coordinator()?;
        let mutation = Arc::new(mutation);
        let request_bounds = HttpRequestBounds::default();
        let metadata = fixture_metadata(21, request_bounds)?;
        let rights = fixture_rights(metadata.source_id().clone(), 24)?;
        AlpacaHistoricalEquityConfig::validate_parent_metadata(&metadata, request_bounds)?;
        assert_eq!(metadata.quality_ceiling(), DataQuality::Aggregated);
        assert!(
            rights
                .permitted_operations
                .contains(&SourceOperation::Train)
        );

        let first_parent = parent(11, &metadata, request_bounds, &rights)?;
        let successor_parent = parent(12, &metadata, request_bounds, &rights)?;
        assert_eq!(
            first_parent.binding_digest().bytes(),
            [
                192, 2, 149, 25, 219, 73, 42, 129, 14, 40, 213, 103, 83, 186, 136, 1, 169, 233,
                109, 183, 15, 147, 125, 148, 35, 203, 178, 37, 82, 143, 154, 94,
            ]
        );
        assert_ne!(
            first_parent.binding_digest(),
            parent(13, &metadata, request_bounds, &rights)?.binding_digest()
        );
        let changed_metadata = fixture_metadata(31, request_bounds)?;
        assert_ne!(
            first_parent.binding_digest(),
            parent_binding_digest_for_test(11, &changed_metadata, request_bounds, &rights)?
        );
        let changed_bounds = HttpRequestBounds::try_new(
            NonZeroU64::new(2_000_000_000).ok_or("zero connect timeout")?,
            NonZeroU64::new(5_000_000_000).ok_or("zero read timeout")?,
            NonZeroU64::new(10_000_000_000).ok_or("zero total timeout")?,
            1,
            NonZeroU64::new(8 * 1024 * 1024).ok_or("zero response bound")?,
        )?;
        assert_ne!(
            first_parent.binding_digest(),
            parent_binding_digest_for_test(11, &metadata, changed_bounds, &rights)?
        );
        let changed_rights = fixture_rights(metadata.source_id().clone(), 34)?;
        assert_ne!(
            first_parent.binding_digest(),
            parent_binding_digest_for_test(11, &metadata, request_bounds, &changed_rights)?
        );

        let install_count = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let first_cancel = CancellationToken::new();
        let first_waiter = {
            let mutation = Arc::clone(&mutation);
            let metadata = metadata.clone();
            let rights = rights.clone();
            let install_count = Arc::clone(&install_count);
            let first_cancel = first_cancel.clone();
            let candidate_metadata = metadata.clone();
            let candidate_rights = rights.clone();
            tokio::spawn(async move {
                mutation
                    .install_or_join_for_test(
                        first_parent,
                        metadata,
                        rights,
                        Instant::now() + Duration::from_secs(2),
                        &first_cancel,
                        move |admission| async move {
                            install_count.fetch_add(1, Ordering::SeqCst);
                            let _sent = started_tx.send(());
                            let _released = release_rx.await;
                            test_candidate(
                                first_parent,
                                candidate_metadata,
                                candidate_rights,
                                admission,
                            )
                        },
                    )
                    .await
            })
        };
        started_rx.await?;

        let joined = {
            let mutation = Arc::clone(&mutation);
            let metadata = metadata.clone();
            let rights = rights.clone();
            tokio::spawn(async move {
                mutation
                    .install_or_join_for_test(
                        first_parent,
                        metadata,
                        rights,
                        Instant::now() + Duration::from_secs(2),
                        &CancellationToken::new(),
                        |_admission| async {
                            panic!("an exact concurrent join must not construct a second source")
                        },
                    )
                    .await
            })
        };
        let stale = mutation
            .install_or_join_for_test(
                successor_parent,
                metadata.clone(),
                rights.clone(),
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
                |_admission| async { panic!("a stale parent must be refused before construction") },
            )
            .await;
        assert!(matches!(
            stale,
            Err(AlpacaHistoricalSourceSlotError::StaleParent)
        ));

        first_cancel.cancel();
        assert!(matches!(
            first_waiter.await?,
            Err(AlpacaHistoricalSourceSlotError::WaitCancelled)
        ));
        release_tx
            .send(())
            .map_err(|_| "installation release failed")?;
        let first_lease = joined.await??;
        assert_eq!(install_count.load(Ordering::SeqCst), 1);
        first_lease.validate_exact_parent(first_parent)?;
        let first_source = active_entry_snapshot(&coordinator, &first_lease)?;
        let (preflight_plan, canonical_instrument) = fixture_plan()?;
        let first_receipt = first_lease
            .admit_plan(
                preflight_plan.clone(),
                canonical_instrument.clone(),
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?;
        assert!(first_receipt.admission.matches(&first_lease.admission));
        assert!(first_receipt.matches_group_generation(first_parent.group_generation()));
        assert!(!first_receipt.matches_group_generation(successor_parent.group_generation()));
        let first_directory = Arc::downgrade(&first_lease.directory);
        drop(first_lease);
        let first_validated = mutation.validate_plan_receipt(
            &first_receipt,
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )?;
        let first_authorized = first_validated
            .authorize(
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?;
        let validated_before_drain = mutation.validate_plan_receipt(
            &first_receipt,
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )?;
        assert_eq!(
            first_authorized.provider_dataset().as_str(),
            "alpaca:test-history"
        );
        assert_eq!(
            first_authorized.analytical_dataset().as_str(),
            "alpaca.test-history"
        );

        let abandoned_drain_waiter = {
            let mutation = Arc::clone(&mutation);
            tokio::spawn(async move {
                mutation
                    .drain_exact_for_test(
                        first_parent,
                        Instant::now() + Duration::from_secs(2),
                        &CancellationToken::new(),
                    )
                    .await
            })
        };
        wait_for_slot_draining(&coordinator, first_parent).await?;
        abandoned_drain_waiter.abort();
        assert!(
            abandoned_drain_waiter
                .await
                .is_err_and(|error| error.is_cancelled())
        );
        assert_slot_draining(&coordinator, first_parent)?;
        drop(first_authorized);
        mutation
            .drain_exact_for_test(
                first_parent,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?;
        assert_slot_stopped(&coordinator, first_parent)?;
        assert!(matches!(
            validated_before_drain
                .authorize(
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await,
            Err(AlpacaHistoricalSourceSlotError::StaleLease)
        ));
        assert!(matches!(
            mutation.validate_plan_receipt(
                &first_receipt,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            ),
            Err(AlpacaHistoricalSourceSlotError::StaleLease)
        ));

        let successor_candidate_metadata = metadata.clone();
        let successor_candidate_rights = rights.clone();
        let successor_lease = mutation
            .install_or_join_for_test(
                successor_parent,
                metadata.clone(),
                rights.clone(),
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
                move |admission| async move {
                    test_candidate(
                        successor_parent,
                        successor_candidate_metadata,
                        successor_candidate_rights,
                        admission,
                    )
                },
            )
            .await?;
        successor_lease.validate_exact_parent(successor_parent)?;
        assert!(!first_receipt.admission.matches(&successor_lease.admission));
        assert!(!std::sync::Weak::ptr_eq(
            &first_directory,
            &Arc::downgrade(&successor_lease.directory)
        ));
        let successor_receipt = successor_lease
            .admit_plan(
                preflight_plan,
                canonical_instrument,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?;
        assert!(successor_receipt.matches_group_generation(successor_parent.group_generation()));
        let successor_source = active_entry_snapshot(&coordinator, &successor_lease)?;
        drop(successor_lease);
        let successor_validated = mutation.validate_plan_receipt(
            &successor_receipt,
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )?;
        let successor_authorized = successor_validated
            .authorize(
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            successor_authorized.provider_dataset().as_str(),
            "alpaca:test-history"
        );
        drop(successor_authorized);
        assert!(matches!(
            mutation.validate_plan_receipt(
                &first_receipt,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            ),
            Err(AlpacaHistoricalSourceSlotError::StaleLease)
        ));
        assert!(!std::sync::Weak::ptr_eq(
            &first_source.source,
            &successor_source.source
        ));
        assert_eq!(
            first_source.registration_owner,
            successor_source.registration_owner
        );
        assert_eq!(
            first_source.registration_source,
            successor_source.registration_source
        );
        assert_eq!(
            first_source.registration_revision,
            successor_source.registration_revision
        );
        Ok(())
    }

    fn test_coordinator() -> Result<
        (
            Arc<ProductionResearchIngestCoordinator>,
            AlpacaHistoricalSourceMutationAuthority,
        ),
        Box<dyn std::error::Error>,
    > {
        let directory = tempfile::tempdir()?.keep();
        let paths = LocalPaths::prepare(directory.join("market-squawk"))?;
        let research = Arc::new(ResearchService::initialize(
            &paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                CatalogLimit::new(16)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?);
        let registry = market_squawk_sources::AuthoritativeSourceRegistry::
            try_new_durable_with_authorization_subject_resolver(
                LocalAuthorityStateStore::try_open(
                    paths.control_root()?.root().join("source-authority"),
                )?,
                Arc::new(TestAuthorizationSubjectResolver {
                    evidence: exact_evidence(22).content_digest(),
                    record: SourceIdentifier::try_from("alpaca-test-credential-record")?,
                }),
            )?;
        let (coordinator, _generic, alpaca) =
            ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
                registry,
                research,
                ResearchExtractionLimits::try_new(
                    NonZeroU16::new(4).ok_or("zero discovery bound")?,
                    NonZeroU32::new(4).ok_or("zero record bound")?,
                    NonZeroU64::new(64 * 1024).ok_or("zero byte bound")?,
                    Duration::from_secs(2),
                    Duration::from_secs(2),
                )?,
                [],
            )?;
        Ok((coordinator, alpaca))
    }

    #[derive(Debug)]
    struct TestAuthorizationSubjectResolver {
        evidence: EvidenceDigest,
        record: SourceIdentifier,
    }

    impl AuthorizationSubjectResolver for TestAuthorizationSubjectResolver {
        fn resolve_subject_record(
            &self,
            mode: AuthorizationMode,
            evidence: EvidenceDigest,
        ) -> Result<SourceIdentifier, AuthorizationSubjectResolutionError> {
            if mode == AuthorizationMode::UserAuthorized && evidence == self.evidence {
                Ok(self.record.clone())
            } else {
                Err(AuthorizationSubjectResolutionError::EvidenceUnresolved)
            }
        }
    }

    fn parent(
        byte: u8,
        metadata: &SourceMetadata,
        request_bounds: HttpRequestBounds,
        rights: &ResearchRightsAuthority,
    ) -> Result<AlpacaHistoricalParentGeneration, Box<dyn std::error::Error>> {
        let group_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]);
        AlpacaHistoricalParentGeneration::try_from_test_digests(
            group_digest,
            parent_binding_digest_for_test(byte, metadata, request_bounds, rights)?,
        )
        .map_err(Into::into)
    }

    fn parent_binding_digest_for_test(
        group_byte: u8,
        metadata: &SourceMetadata,
        request_bounds: HttpRequestBounds,
        rights: &ResearchRightsAuthority,
    ) -> Result<EvidenceDigest, AlpacaHistoricalPlanAdmissionError> {
        let surface_id = SourceIdentifier::try_from("alpaca.basic-market-data")
            .map_err(|_| AlpacaHistoricalPlanAdmissionError::Serialization)?;
        let account_subject = SourceIdentifier::try_from("alpaca-market-data-principal-test")
            .map_err(|_| AlpacaHistoricalPlanAdmissionError::Serialization)?;
        parent_binding_digest_v2(AlpacaHistoricalParentDigestInput {
            group_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [group_byte; 32]),
            surface_id: &surface_id,
            onboarding_session_id: Uuid::from_u128(0x8f64_15bb_9811_48e0_a2c5_f5d5bc4ac829),
            credential_generation: 7,
            account_subject: &account_subject,
            account_verification_evidence: EvidenceDigest::new(DigestAlgorithm::Sha256, [22; 32]),
            account_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [41; 32]),
            public_configuration_digest: EvidenceDigest::new(DigestAlgorithm::Blake3, [42; 32]),
            runtime_evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [43; 32]),
            trading_api_environment: AlpacaTradingApiEnvironment::Paper,
            metadata,
            request_bounds,
            rights,
        })
    }

    fn fixture_metadata(
        revision_byte: u8,
        request_bounds: HttpRequestBounds,
    ) -> Result<SourceMetadata, Box<dyn std::error::Error>> {
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(SourceIdentifier::try_from(
                "alpaca-market-data-principal-test",
            )?),
            exact_evidence(22),
            effective,
        );
        let mapping = AlpacaInstrumentMapping::try_new(
            "AAPL".to_owned(),
            fixture_instrument_id()?,
            AssetClass::Equity,
        )?;
        Ok(AlpacaHistoricalEquityConfig::try_parent_metadata(
            SourceId::try_from("alpaca-basic-iex-history")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(format!(
                    "alpaca-basic-iex-history-v{revision_byte}"
                ))?),
                exact_evidence(revision_byte),
            ),
            authorization.clone(),
            exact_evidence(23),
            effective,
            vec![mapping],
            FreshnessPolicy::try_new(
                30_000_000_000,
                5_000_000_000,
                5_000_000_000,
                5_000_000_000,
                1_000_000_000,
            )?,
            ProviderBudgetPolicy::try_new(
                BudgetScope::for_authorization(
                    SourceIdentifier::try_from("alpaca-market-data")?,
                    &authorization,
                )?,
                NonZeroU32::new(150).ok_or("zero request budget")?,
                NonZeroU64::new(60_000_000_000).ok_or("zero request window")?,
                NonZeroU16::new(2).ok_or("zero request concurrency")?,
                BackoffPolicy::try_new(
                    NonZeroU64::new(1_000_000_000).ok_or("zero initial backoff")?,
                    NonZeroU64::new(60_000_000_000).ok_or("zero maximum backoff")?,
                    1_000,
                )?,
            )?,
            request_bounds,
        )?)
    }

    fn fixture_rights(
        source_id: SourceId,
        basis_byte: u8,
    ) -> Result<ResearchRightsAuthority, Box<dyn std::error::Error>> {
        Ok(ResearchRightsAuthority::try_new_source_wide(
            source_id,
            RightsBasis::reviewed_terms(
                "https://alpaca.markets/disclosures",
                EvidenceDigest::new(DigestAlgorithm::Sha256, [basis_byte; 32]),
            )?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [25; 32]),
            None,
            vec![
                SourceOperation::Retrieve,
                SourceOperation::Display,
                SourceOperation::Persist,
                SourceOperation::Train,
            ],
        )?)
    }

    fn test_candidate(
        parent: AlpacaHistoricalParentGeneration,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
        admission: ResearchProviderAdmission,
    ) -> Result<PreparedAlpacaHistoricalSourceInstall, AlpacaHistoricalSourceSlotError> {
        let directory: Arc<dyn AlpacaHistoricalPlanAdmissionDirectory> =
            Arc::new(TestPlanDirectory::try_new(parent)?);
        let source: Arc<dyn ManagedResearchExtractionSource> = Arc::new(TestManagedSource {
            metadata: metadata.clone(),
        });
        PreparedAlpacaHistoricalSourceInstall::try_new(
            parent, metadata, rights, admission, directory, source,
        )
    }

    struct ActiveEntrySnapshot {
        source: std::sync::Weak<dyn ManagedResearchExtractionSource>,
        registration_owner: usize,
        registration_source: SourceId,
        registration_revision: MetadataRevision,
    }

    fn active_entry_snapshot(
        coordinator: &ProductionResearchIngestCoordinator,
        lease: &AlpacaHistoricalPlanDirectoryLease,
    ) -> Result<ActiveEntrySnapshot, Box<dyn std::error::Error>> {
        let authority = coordinator
            .authority
            .lock()
            .map_err(|_| "coordinator poisoned")?;
        assert_eq!(authority.sources.len(), 1);
        let entry = authority
            .sources
            .get(&canonical_alpaca_historical_profile()?)
            .ok_or("installed source missing")?;
        assert!(entry.admission.matches(&lease.admission));
        Ok(ActiveEntrySnapshot {
            source: Arc::downgrade(&entry.source),
            registration_owner: std::ptr::from_ref(entry.registration.as_ref()) as usize,
            registration_source: entry.registration.source_id().clone(),
            registration_revision: entry.registration.revision().clone(),
        })
    }

    #[derive(Debug)]
    struct TestPlanDirectory {
        parent: AlpacaHistoricalParentGeneration,
        plan: AlpacaHistoricalAdmittedPlan,
        admitted: AtomicBool,
    }

    impl TestPlanDirectory {
        fn try_new(
            parent: AlpacaHistoricalParentGeneration,
        ) -> Result<Self, AlpacaHistoricalSourceSlotError> {
            let mut digest = Sha256::new();
            digest.update(b"market-squawk/alpaca-historical-test-plan/v1\0");
            digest.update(parent.binding_digest().bytes());
            Ok(Self {
                parent,
                plan: AlpacaHistoricalAdmittedPlan {
                    provider_dataset: SourceIdentifier::try_from("alpaca:test-history")
                        .map_err(|_| AlpacaHistoricalSourceSlotError::InvalidCandidate)?,
                    analytical_dataset: DatasetId::try_from("alpaca.test-history")
                        .map_err(|_| AlpacaHistoricalSourceSlotError::InvalidCandidate)?,
                    parent_digest: parent.binding_digest(),
                    plan_digest: EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        digest.finalize().into(),
                    ),
                },
                admitted: AtomicBool::new(false),
            })
        }
    }

    impl AlpacaHistoricalPlanAdmissionDirectory for TestPlanDirectory {
        fn admit_plan<'a>(
            &'a self,
            preflight_plan: AlpacaHistoricalEquityPreflightPlan,
            canonical_instrument: MarketDataInstrumentDefinition,
            deadline: Instant,
            cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<AlpacaHistoricalAdmittedPlan, AlpacaHistoricalPlanAdmissionError>>
        {
            Box::pin(async move {
                if cancellation.is_cancelled()
                    || Instant::now() >= deadline
                    || preflight_plan.mapping().instrument() != canonical_instrument.instrument_id()
                {
                    return Err(AlpacaHistoricalPlanAdmissionError::InvalidPlan);
                }
                self.admitted.store(true, Ordering::Release);
                Ok(self.plan.clone())
            })
        }

        fn validate_parent<'a>(
            &'a self,
            parent: AlpacaHistoricalParentGeneration,
            deadline: Instant,
            cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), AlpacaHistoricalSourceSlotError>> {
            Box::pin(async move {
                if cancellation.is_cancelled() || Instant::now() >= deadline {
                    return Err(AlpacaHistoricalSourceSlotError::StaleParent);
                }
                self.validate_parent_now(parent)
            })
        }

        fn validate_parent_now(
            &self,
            parent: AlpacaHistoricalParentGeneration,
        ) -> Result<(), AlpacaHistoricalSourceSlotError> {
            if self.parent == parent {
                Ok(())
            } else {
                Err(AlpacaHistoricalSourceSlotError::StaleParent)
            }
        }

        fn validate_plan_now(
            &self,
            plan: &AlpacaHistoricalAdmittedPlan,
        ) -> Result<(), AlpacaHistoricalSourceSlotError> {
            if self.admitted.load(Ordering::Acquire) && &self.plan == plan {
                Ok(())
            } else {
                Err(AlpacaHistoricalSourceSlotError::StaleLease)
            }
        }
    }

    fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        ))
    }

    fn fixture_instrument_id() -> Result<InstrumentId, Box<dyn std::error::Error>> {
        Ok(InstrumentId::from_str(
            "00000001-0002-0003-0004-000000000001",
        )?)
    }

    fn fixture_plan() -> Result<
        (
            AlpacaHistoricalEquityPreflightPlan,
            MarketDataInstrumentDefinition,
        ),
        Box<dyn std::error::Error>,
    > {
        let instrument_id = fixture_instrument_id()?;
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let mapping =
            AlpacaInstrumentMapping::try_new("AAPL".to_owned(), instrument_id, AssetClass::Equity)?;
        let plan = AlpacaHistoricalEquityPreflightPlan::try_new(
            mapping,
            AlpacaTimeframe::day(),
            Timestamp::from_unix_nanos(1_735_776_900_000_000_000),
            AlpacaHistoricalLookback::try_from_days(30)?,
            AlpacaAdjustment::All,
        )?;
        let definition =
            MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                instrument_id,
                reference_evidence: RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(SourceIdentifier::try_from(
                        "nasdaq-trader-symbol-directory-v1",
                    )?),
                    exact_evidence(51),
                ),
                effective_interval: effective,
                asset_class: AssetClass::Equity,
                display_name: None,
                quote_currency: Currency::try_from("USD")?,
                quote_currency_evidence: exact_evidence(52),
                venue_mappings: vec![VenueMapping::new(
                    VenueId::try_from("iex")?,
                    VenueSymbol::try_from("AAPL")?,
                )],
                provider_identities: Vec::new(),
                identifiers: Vec::new(),
            })?;
        Ok((plan, definition))
    }

    async fn wait_for_slot_draining(
        coordinator: &ProductionResearchIngestCoordinator,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for _attempt in 0..64 {
            if assert_slot_draining(coordinator, parent).is_ok() {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err("Alpaca historical drain did not enter Draining".into())
    }

    fn assert_slot_draining(
        coordinator: &ProductionResearchIngestCoordinator,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let authority = coordinator
            .authority
            .lock()
            .map_err(|_| "coordinator poisoned")?;
        if matches!(
            &authority.alpaca_historical,
            AlpacaHistoricalSourceSlot::Draining { stable, .. } if stable.parent == parent
        ) {
            Ok(())
        } else {
            Err("Alpaca historical slot is not Draining".into())
        }
    }

    fn assert_slot_stopped(
        coordinator: &ProductionResearchIngestCoordinator,
        parent: AlpacaHistoricalParentGeneration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let authority = coordinator
            .authority
            .lock()
            .map_err(|_| "coordinator poisoned")?;
        if matches!(
            &authority.alpaca_historical,
            AlpacaHistoricalSourceSlot::Stopped(stable) if stable.parent == parent
        ) {
            Ok(())
        } else {
            Err("Alpaca historical slot is not Stopped".into())
        }
    }

    #[derive(Debug)]
    struct TestManagedSource {
        metadata: SourceMetadata,
    }

    impl SourceMetadataProvider for TestManagedSource {
        fn metadata(&self) -> &SourceMetadata {
            &self.metadata
        }
    }

    impl ExtractionSource for TestManagedSource {
        fn discover(
            &self,
            _authority: ExtractionAuthority,
            _request: DiscoveryRequest,
            _cancellation: CancellationToken,
        ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
            Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
        }

        fn extract(
            &self,
            _authority: ExtractionAuthority,
            _request: ExtractionRequest,
            _cancellation: CancellationToken,
        ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
            Box::pin(async { Err(SourceError::InvalidProtocolState.into()) })
        }
    }

    impl ManagedResearchExtractionSource for TestManagedSource {
        fn revision_plan(
            &self,
            _batch: &ExtractionBatch,
        ) -> Result<Option<ExtractionRevisionPlan>, ResearchRevisionPlanError> {
            Ok(None)
        }
    }
}
