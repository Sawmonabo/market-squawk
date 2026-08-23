//! Revocable least-authority access to one active Alpaca account's historical-data prerequisites.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaHistoricalBarTimeAuthority, AlpacaHistoricalEquityConfig,
    AlpacaHistoricalEquityPreflightClient, AlpacaHistoricalEquityPreflightPlan,
    AlpacaHistoricalEquityPreflightReceipt, AlpacaHistoricalEquitySource,
    AlpacaTradingApiEnvironment,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketDataInstrumentDefinition, SourceIdentifier,
};
use market_squawk_platform::SecretGeneration;
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    CompleteMarketBarHistoryV1, DiscoveryBatch, DiscoveryRequest, ExtractionAuthority,
    ExtractionBatch, ExtractionRequest, ExtractionRevisionPlan, ExtractionSource,
    ExtractionSourceError, HttpRequestBounds, ProviderCaptureMaterial, ProviderRateDeclaration,
    SharedProviderBudget, SourceError, SourceMetadata,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::ResearchRightsAuthority;
use crate::provider_activation::{
    AlpacaBasicAccountActivation, ProviderAccountBinding, ProviderMarketAccount,
};

use super::{AlpacaHistoricalPublishedCleanupProof, MarketRuntimeGroupGeneration};

mod calendar;
pub(crate) use calendar::AlpacaHistoricalCompositeCalendarAuthority;

type CurrentnessFuture = Pin<Box<dyn Future<Output = bool> + Send + 'static>>;
type CurrentnessValidator = dyn Fn() -> CurrentnessFuture + Send + Sync + 'static;
type SynchronousCurrentnessValidator = dyn Fn() -> bool + Send + Sync + 'static;

/// Complete exact evidence returned by the rich Alpaca historical extraction boundary.
///
/// The application integration owner must durably seal both capture materials and bind both
/// sealed receipts to publication before the batch can enter canonical storage.
#[derive(Debug)]
pub(crate) struct AlpacaHistoricalExtractionWithCapture {
    batch: ExtractionBatch,
    bar_capture: ProviderCaptureMaterial,
    calendar_capture: ProviderCaptureMaterial,
    history_capture_semantic: CompleteMarketBarHistoryV1,
}

impl AlpacaHistoricalExtractionWithCapture {
    pub(crate) const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    pub(crate) const fn bar_capture(&self) -> &ProviderCaptureMaterial {
        &self.bar_capture
    }

    pub(crate) const fn calendar_capture(&self) -> &ProviderCaptureMaterial {
        &self.calendar_capture
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        ProviderCaptureMaterial,
        ProviderCaptureMaterial,
        CompleteMarketBarHistoryV1,
    ) {
        (
            self.batch,
            self.bar_capture,
            self.calendar_capture,
            self.history_capture_semantic,
        )
    }
}

/// Exact fail-closed outcome of a registry lookup for the active Alpaca historical authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AlpacaHistoricalLookupError {
    /// No Alpaca Basic account group is registered under its canonical surface.
    #[error("Alpaca historical authority is not configured")]
    NotConfigured,
    /// The exact group exists but one or more required live children are unhealthy or cancelled.
    #[error("Alpaca historical authority is inactive")]
    Inactive,
    /// Session, public configuration, credential generation, or onboarding currentness is stale.
    #[error("Alpaca historical authority is stale")]
    Stale,
    /// Registry mutation, shutdown, caller cancellation, or the deadline prevents a stable lease.
    #[error("Alpaca historical authority is transitioning")]
    Transitioning,
}

/// Registry-returned capability bound to one exact active Alpaca account generation.
///
/// It contains no endpoint, request builder, source registration, or onboarding mutation
/// authority. Credential and rate authority remain private until a later runtime-owned historical
/// transport consumes them.
#[derive(Clone)]
pub(crate) struct AlpacaHistoricalRuntimeCapability {
    inner: Arc<AlpacaHistoricalInner>,
}

impl AlpacaHistoricalRuntimeCapability {
    pub(crate) fn group_generation(&self) -> MarketRuntimeGroupGeneration {
        self.inner.group_generation
    }

    pub(crate) fn account_binding(&self) -> &ProviderAccountBinding {
        &self.inner.account_binding
    }

    pub(crate) fn surface_id(&self) -> &SourceIdentifier {
        &self.inner.surface_id
    }

    pub(crate) fn onboarding_session_id(&self) -> Uuid {
        self.inner.onboarding_session_id
    }

    pub(crate) fn credential_generation(&self) -> SecretGeneration {
        self.inner.credential_generation
    }

    pub(crate) fn account_digest(&self) -> EvidenceDigest {
        self.inner.account_digest
    }

    pub(crate) fn public_configuration_digest(&self) -> EvidenceDigest {
        self.inner.public_configuration_digest
    }

    pub(crate) fn runtime_evidence_digest(&self) -> EvidenceDigest {
        self.inner.runtime_evidence_digest
    }

    pub(crate) fn trading_api_environment(&self) -> AlpacaTradingApiEnvironment {
        self.inner.trading_api_environment
    }

    pub(crate) fn historical_metadata(&self) -> &SourceMetadata {
        &self.inner.historical_metadata
    }

    pub(crate) fn historical_request_bounds(&self) -> HttpRequestBounds {
        self.inner.historical_request_bounds
    }

    pub(crate) fn historical_rights(&self) -> &ResearchRightsAuthority {
        &self.inner.historical_rights
    }

    pub(crate) fn is_revoked(&self) -> bool {
        !self.inner.accepting.load(Ordering::Acquire) || self.inner.cancellation.is_cancelled()
    }

    pub(crate) async fn require_current(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), AlpacaHistoricalCapabilityError> {
        ensure_before(deadline, cancellation)?;
        let _operation = self.inner.admit()?;
        let currentness = Arc::clone(&self.inner.currentness);
        let current = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Revoked);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Cancelled);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(AlpacaHistoricalCapabilityError::DeadlineExceeded);
            }
            current = (currentness)() => current,
        };
        if !current {
            return Err(AlpacaHistoricalCapabilityError::Stale);
        }
        self.inner.ensure_usable()
    }

    /// Fetches one exact terminal historical-bar pagination graph while credentials and the
    /// account-colliding provider budget remain inside this admitted runtime operation.
    pub(crate) async fn preflight_plan(
        &self,
        plan: AlpacaHistoricalEquityPreflightPlan,
        request_bounds: HttpRequestBounds,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<AlpacaHistoricalEquityPreflightReceipt>, AlpacaHistoricalPlanOperationError>
    {
        ensure_before(deadline, cancellation)?;
        let _operation = self.inner.admit()?;
        self.validate_current(cancellation).await?;
        let (credentials, budget) = self.inner.historical_authority()?;
        let client = AlpacaHistoricalEquityPreflightClient::try_new(credentials, request_bounds)?;
        let receipt = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Revoked.into());
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Cancelled.into());
            }
            result = client.fetch(plan, &budget, deadline, cancellation) => result?,
        };
        drop(client);
        drop(budget);
        self.validate_current(cancellation).await?;
        Ok(receipt)
    }

    /// Delegates one discovery operation through the exact retained, credential-free preflight
    /// graph while the account-generation operation keeps revocation current.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact plan, canonical identity, calendar authority, and extraction authority stay explicit"
    )]
    pub(crate) async fn discover_plan(
        &self,
        config: AlpacaHistoricalEquityConfig,
        canonical_instrument: MarketDataInstrumentDefinition,
        bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority>,
        preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        let _operation = self.inner.admit().map_err(map_capability_error)?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        let source = AlpacaHistoricalEquitySource::try_from_preflight(
            config,
            vec![canonical_instrument],
            bar_time_authority,
            preflight,
        )
        .map_err(|_error| SourceError::InvalidProtocolState)?;
        let extracted = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                Err(SourceError::SessionNotCurrent.into())
            }
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            result = source.discover(authority, request, cancellation.clone()) => result,
        };
        drop(source);
        let batch = extracted?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        Ok(batch)
    }

    /// Keeps the legacy batch-only extraction surface fail-closed.
    ///
    /// Canonical publication is not admitted until the common integration lane consumes
    /// [`Self::extract_plan_with_capture`], seals both exact captures, and binds their receipts to
    /// the published generation. Returning a batch here would permit durable rows without their
    /// complete raw bar and calendar lineage.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact plan, canonical identity, calendar authority, and extraction authority stay explicit"
    )]
    pub(crate) async fn extract_plan(
        &self,
        config: AlpacaHistoricalEquityConfig,
        canonical_instrument: MarketDataInstrumentDefinition,
        bar_time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority>,
        preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<ExtractionBatch, ExtractionSourceError> {
        let _operation = self.inner.admit().map_err(map_capability_error)?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        drop((
            config,
            canonical_instrument,
            bar_time_authority,
            preflight,
            authority,
            request,
        ));
        Err(SourceError::InvalidProtocolState.into())
    }

    /// Extracts one exact historical page and returns both required raw-capture lineages while
    /// the account-generation operation and currentness checks remain held around the work.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact plan, canonical identity, calendar capture, and extraction authority stay explicit"
    )]
    pub(crate) async fn extract_plan_with_capture(
        &self,
        config: AlpacaHistoricalEquityConfig,
        canonical_instrument: MarketDataInstrumentDefinition,
        admitted_plan_digest: EvidenceDigest,
        bar_time_authority: Arc<AlpacaHistoricalCompositeCalendarAuthority>,
        preflight: Arc<AlpacaHistoricalEquityPreflightReceipt>,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<AlpacaHistoricalExtractionWithCapture, ExtractionSourceError> {
        let _operation = self.inner.admit().map_err(map_capability_error)?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        let bar_capture = preflight
            .provider_capture_material(&config)
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let calendar_capture = bar_time_authority
            .provider_capture_material(&config, &preflight)
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let canonical_instrument_json = serde_json::to_vec(&canonical_instrument)
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        let instrument_revision_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&canonical_instrument_json).into(),
        );
        let history_capture_semantic = bar_time_authority
            .history_capture_semantic(instrument_revision_digest, admitted_plan_digest)
            .map_err(|_error| SourceError::InvalidProtocolState)?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        let time_authority: Arc<dyn AlpacaHistoricalBarTimeAuthority> = bar_time_authority;
        let source = AlpacaHistoricalEquitySource::try_from_preflight(
            config,
            vec![canonical_instrument],
            time_authority,
            preflight,
        )
        .map_err(|_error| SourceError::InvalidProtocolState)?;
        let extracted = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                Err(SourceError::SessionNotCurrent.into())
            }
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            result = source.extract(authority, request, cancellation.clone()) => result,
        };
        drop(source);
        let batch = extracted?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        Ok(AlpacaHistoricalExtractionWithCapture {
            batch,
            bar_capture,
            calendar_capture,
            history_capture_semantic,
        })
    }

    /// Revalidates the revocable runtime around a pure, exact one-plan analytical mapping.
    pub(crate) fn analytical_dataset_for_plan(
        &self,
        config: &AlpacaHistoricalEquityConfig,
        canonical_instrument: &MarketDataInstrumentDefinition,
        batch: &ExtractionBatch,
    ) -> Result<SourceIdentifier, AlpacaHistoricalPlanOperationError> {
        let _operation = self.inner.admit()?;
        self.validate_current_now()?;
        let identifier =
            AlpacaHistoricalEquitySource::one_plan_analytical_dataset_identifier_for_batch(
                config,
                canonical_instrument,
                batch,
            )?;
        self.validate_current_now()?;
        Ok(identifier)
    }

    /// Revalidates the revocable runtime around the source-honest one-plan revision mapping.
    pub(crate) fn revision_plan_for_plan(
        &self,
        config: &AlpacaHistoricalEquityConfig,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, AlpacaHistoricalPlanOperationError> {
        let _operation = self.inner.admit()?;
        self.validate_current_now()?;
        let revisions = AlpacaHistoricalEquitySource::one_plan_revision_plan(config, batch)?;
        self.validate_current_now()?;
        Ok(revisions)
    }

    async fn validate_current(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), AlpacaHistoricalCapabilityError> {
        self.inner.ensure_usable()?;
        let currentness = Arc::clone(&self.inner.currentness);
        let current = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Revoked);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Cancelled);
            }
            current = (currentness)() => current,
        };
        if !current {
            return Err(AlpacaHistoricalCapabilityError::Stale);
        }
        self.inner.ensure_usable()
    }

    pub(crate) fn validate_current_now(&self) -> Result<(), AlpacaHistoricalCapabilityError> {
        self.inner.ensure_usable()?;
        if !(self.inner.synchronous_currentness)() {
            return Err(AlpacaHistoricalCapabilityError::Stale);
        }
        self.inner.ensure_usable()
    }
}

impl fmt::Debug for AlpacaHistoricalRuntimeCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaHistoricalRuntimeCapability")
            .field("surface_id", &self.inner.surface_id)
            .field("onboarding_session_id", &self.inner.onboarding_session_id)
            .field("credential_generation", &self.inner.credential_generation)
            .field("account", &self.inner.account_binding.account())
            .field("credentials", &"[REDACTED REVOCABLE ZEROIZING ARC]")
            .field("provider_rate", &"[SHARED PROCESS AUTHORITY]")
            .field("revoked", &self.is_revoked())
            .finish()
    }
}

/// Runtime-group owner that issues and revokes historical subordinate capabilities.
pub(super) struct AlpacaHistoricalCapabilityOwner {
    inner: Arc<AlpacaHistoricalInner>,
}

impl AlpacaHistoricalCapabilityOwner {
    pub(super) fn try_new(
        activation: &AlpacaBasicAccountActivation,
        group_generation: MarketRuntimeGroupGeneration,
        historical_metadata: SourceMetadata,
        historical_request_bounds: HttpRequestBounds,
        historical_rights: ResearchRightsAuthority,
        cancellation: CancellationToken,
    ) -> Result<Self, ServiceError> {
        let lease = activation.lease();
        let account_binding = activation.account_binding();
        let currentness: Arc<CurrentnessValidator> =
            Arc::new(activation.historical_currentness_validator());
        let synchronous_currentness: Arc<SynchronousCurrentnessValidator> =
            Arc::new(activation.historical_currentness_validator_now());
        let credential_generation = lease.generation().ok_or(ServiceError::Unavailable)?;
        let account_digest = lease.account_digest().ok_or(ServiceError::Unavailable)?;
        let public_configuration_digest = lease.public_configuration_digest();
        let runtime_evidence_digest = lease.runtime_evidence_digest();
        if cancellation.is_cancelled()
            || account_binding.account() != ProviderMarketAccount::AlpacaBasic
            || lease.surface_id().as_str() != ProviderMarketAccount::AlpacaBasic.surface_id()
            || lease.session_id().is_nil()
            || account_digest.algorithm() != DigestAlgorithm::Sha256
            || account_digest.bytes() == [0; 32]
            || public_configuration_digest.algorithm() != DigestAlgorithm::Sha256
            || public_configuration_digest.bytes() == [0; 32]
            || runtime_evidence_digest.bytes() == [0; 32]
            || lease.verification_evidence_digest() != Some(account_binding.verification_evidence())
            || historical_metadata.source_id() != historical_rights.source_id()
            || AlpacaHistoricalEquityConfig::validate_parent_metadata(
                &historical_metadata,
                historical_request_bounds,
            )
            .is_err()
        {
            return Err(ServiceError::Unavailable);
        }
        let provider_rate = activation.historical_provider_rate_authority();
        let provider_rate_declaration = ProviderRateDeclaration::try_for_authorization_subject(
            lease
                .provider_budget_policy()
                .cloned()
                .ok_or(ServiceError::Unavailable)?,
            account_binding.subject(),
        )
        .map_err(|_error| ServiceError::Unavailable)?;
        let historical_budget = provider_rate
            .register_budget(provider_rate_declaration)
            .map_err(|_error| ServiceError::Unavailable)?;
        Ok(Self {
            inner: Arc::new(AlpacaHistoricalInner {
                authority: Mutex::new(Some(AlpacaHistoricalAuthority {
                    credentials: activation.credentials(),
                    budget: historical_budget,
                })),
                account_binding: account_binding.clone(),
                surface_id: lease.surface_id().clone(),
                onboarding_session_id: lease.session_id(),
                credential_generation,
                account_digest,
                public_configuration_digest,
                runtime_evidence_digest,
                trading_api_environment: activation.trading_api_environment(),
                group_generation,
                historical_metadata,
                historical_request_bounds,
                historical_rights,
                currentness,
                synchronous_currentness,
                accepting: AtomicBool::new(true),
                cancellation,
                active: AtomicUsize::new(0),
                idle: Notify::new(),
            }),
        })
    }

    pub(super) fn issue(
        &self,
    ) -> Result<AlpacaHistoricalRuntimeCapability, AlpacaHistoricalCapabilityError> {
        self.inner.ensure_usable()?;
        Ok(AlpacaHistoricalRuntimeCapability {
            inner: Arc::clone(&self.inner),
        })
    }

    pub(super) fn begin_shutdown(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.cancellation.cancel();
    }

    /// Consumes authority that never crossed the account-group publication boundary.
    pub(super) async fn shutdown_unpublished(self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        while self.inner.active.load(Ordering::Acquire) != 0 {
            let notified = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
        self.inner.destroy_authority_once()
    }

    /// Destroys published credential/rate authority only after the exact B3 ownership barrier.
    pub(super) async fn finish_published_before(
        &mut self,
        parent_claim: Option<crate::application::AlpacaHistoricalParentGeneration>,
        proof: &AlpacaHistoricalPublishedCleanupProof,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        self.begin_shutdown();
        match (parent_claim, proof) {
            (Some(parent), AlpacaHistoricalPublishedCleanupProof::ExactDrain(receipt)) => receipt
                .validate_runtime_parent(parent, self.inner.group_generation)
                .map_err(|_error| ServiceError::Unavailable)?,
            (None, AlpacaHistoricalPublishedCleanupProof::NeverClaimed(proof))
                if proof.group_generation == self.inner.group_generation => {}
            (Some(_), AlpacaHistoricalPublishedCleanupProof::NeverClaimed(_))
            | (None, AlpacaHistoricalPublishedCleanupProof::ExactDrain(_))
            | (None, AlpacaHistoricalPublishedCleanupProof::NeverClaimed(_)) => {
                return Err(ServiceError::Unavailable);
            }
        }
        self.inner
            .wait_for_operations_before(deadline, cancellation)
            .await?;
        self.inner.destroy_authority_once()
    }

    pub(super) fn owns(&self, capability: &AlpacaHistoricalRuntimeCapability) -> bool {
        Arc::ptr_eq(&self.inner, &capability.inner)
    }
}

impl Drop for AlpacaHistoricalCapabilityOwner {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct AlpacaHistoricalInner {
    authority: Mutex<Option<AlpacaHistoricalAuthority>>,
    account_binding: ProviderAccountBinding,
    surface_id: SourceIdentifier,
    onboarding_session_id: Uuid,
    credential_generation: SecretGeneration,
    account_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    runtime_evidence_digest: EvidenceDigest,
    trading_api_environment: AlpacaTradingApiEnvironment,
    group_generation: MarketRuntimeGroupGeneration,
    historical_metadata: SourceMetadata,
    historical_request_bounds: HttpRequestBounds,
    historical_rights: ResearchRightsAuthority,
    currentness: Arc<CurrentnessValidator>,
    synchronous_currentness: Arc<SynchronousCurrentnessValidator>,
    accepting: AtomicBool,
    cancellation: CancellationToken,
    active: AtomicUsize,
    idle: Notify,
}

struct AlpacaHistoricalAuthority {
    credentials: Arc<AlpacaCredentials>,
    budget: SharedProviderBudget,
}

impl AlpacaHistoricalInner {
    fn ensure_usable(&self) -> Result<(), AlpacaHistoricalCapabilityError> {
        if !self.accepting.load(Ordering::Acquire) || self.cancellation.is_cancelled() {
            return Err(AlpacaHistoricalCapabilityError::Revoked);
        }
        let authority_available = self
            .authority
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some();
        if authority_available {
            Ok(())
        } else {
            Err(AlpacaHistoricalCapabilityError::Revoked)
        }
    }

    fn historical_authority(
        &self,
    ) -> Result<(Arc<AlpacaCredentials>, SharedProviderBudget), AlpacaHistoricalCapabilityError>
    {
        self.ensure_usable()?;
        self.authority
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|authority| (Arc::clone(&authority.credentials), authority.budget.clone()))
            .ok_or(AlpacaHistoricalCapabilityError::Revoked)
    }

    fn admit(
        self: &Arc<Self>,
    ) -> Result<AlpacaHistoricalOperation, AlpacaHistoricalCapabilityError> {
        self.ensure_usable()?;
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_active| AlpacaHistoricalCapabilityError::Revoked)?;
        if let Err(error) = self.ensure_usable() {
            self.finish_operation();
            return Err(error);
        }
        Ok(AlpacaHistoricalOperation {
            inner: Arc::clone(self),
        })
    }

    fn finish_operation(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_one();
        }
    }

    async fn wait_for_operations_before(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        while self.active.load(Ordering::Acquire) != 0 {
            let notified = self.idle.notified();
            if self.active.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    return Err(ServiceError::DeadlineExceeded);
                }
                () = notified => {}
            }
        }
        Ok(())
    }

    fn destroy_authority_once(&self) -> Result<(), ServiceError> {
        let authority = self
            .authority
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        // `None` is the retained terminal phase from an earlier successful attempt. Published
        // retries must not ask for or destroy this credential/rate authority a second time.
        drop(authority);
        Ok(())
    }
}

struct AlpacaHistoricalOperation {
    inner: Arc<AlpacaHistoricalInner>,
}

impl Drop for AlpacaHistoricalOperation {
    fn drop(&mut self) {
        self.inner.finish_operation();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum AlpacaHistoricalCapabilityError {
    #[error("Alpaca historical capability was revoked")]
    Revoked,
    #[error("Alpaca historical account activation is stale")]
    Stale,
    #[error("Alpaca historical capability operation was cancelled")]
    Cancelled,
    #[error("Alpaca historical capability operation exceeded its deadline")]
    DeadlineExceeded,
}

/// Exact failure from a non-network one-plan operation under the account runtime.
#[derive(Debug, thiserror::Error)]
pub(crate) enum AlpacaHistoricalPlanOperationError {
    #[error(transparent)]
    Capability(#[from] AlpacaHistoricalCapabilityError),
    #[error("Alpaca historical plan or batch is invalid")]
    Adapter(#[from] market_squawk_adapter_alpaca::AlpacaError),
}

const fn map_capability_error(error: AlpacaHistoricalCapabilityError) -> ExtractionSourceError {
    match error {
        AlpacaHistoricalCapabilityError::Cancelled => ExtractionSourceError::Cancelled,
        AlpacaHistoricalCapabilityError::DeadlineExceeded => {
            ExtractionSourceError::DeadlineExceeded
        }
        AlpacaHistoricalCapabilityError::Revoked | AlpacaHistoricalCapabilityError::Stale => {
            ExtractionSourceError::Source(SourceError::SessionNotCurrent)
        }
    }
}

fn ensure_before(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), AlpacaHistoricalCapabilityError> {
    if cancellation.is_cancelled() {
        Err(AlpacaHistoricalCapabilityError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(AlpacaHistoricalCapabilityError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
