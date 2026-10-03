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
    AlpacaHistoricalPendingExtractionSeal, AlpacaTradingApiEnvironment,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MarketDataInstrumentDefinition, SourceIdentifier,
};
use market_squawk_platform::SecretGeneration;
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    DiscoveryBatch, DiscoveryRequest, ExtractionAuthority, ExtractionBatch, ExtractionRequest,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError, HttpRequestBounds,
    ProviderCaptureSealRequest, ProviderRateDeclaration, SharedProviderBudget, SourceError,
    SourceMetadata,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::ResearchRightsAuthority;
use crate::provider_activation::{
    AlpacaBasicAccountActivation, ProviderAccountBinding, ProviderAccountRuntimeCurrentness,
    ProviderMarketAccount,
};

use super::MarketRuntimeGroupGeneration;

mod calendar;
mod corporate_actions;
pub(crate) use calendar::{
    AlpacaHistoricalCalendarError, AlpacaHistoricalCompositeCalendarAuthority,
};

type CurrentnessFuture = Pin<Box<dyn Future<Output = bool> + Send + 'static>>;
type CurrentnessValidator = dyn Fn() -> CurrentnessFuture + Send + Sync + 'static;
type SynchronousCurrentnessValidator = dyn Fn() -> bool + Send + Sync + 'static;

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

    /// Confirms that this capability belongs to a freshly minted physical runtime generation.
    ///
    /// The group-generation digest includes the runtime incarnation. Keeping this comparison on
    /// the opaque capability prevents the history coordinator from reconstructing runtime identity
    /// from looser account or configuration fields.
    pub(crate) fn is_fresh_generation_from(&self, retired: &Self) -> bool {
        !Arc::ptr_eq(&self.inner, &retired.inner)
            && self.inner.group_generation != retired.inner.group_generation
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

    pub(crate) fn calendar_metadata(&self) -> &SourceMetadata {
        &self.inner.calendar_metadata
    }

    pub(crate) fn calendar_rights(&self) -> &ResearchRightsAuthority {
        &self.inner.calendar_rights
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

    pub(crate) async fn wait_until_revoked(&self) {
        self.inner.cancellation.cancelled().await;
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
        identity: Arc<dyn market_squawk_sources::CurrentCatalogProviderIdentity>,
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
            vec![identity],
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
        identity: Arc<dyn market_squawk_sources::CurrentCatalogProviderIdentity>,
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
            identity,
        ));
        Err(SourceError::InvalidProtocolState.into())
    }

    /// Extracts one complete historical graph into an opaque adapter continuation and its seal
    /// request while retaining account-generation and currentness checks around the work.
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
        identity: Arc<dyn market_squawk_sources::CurrentCatalogProviderIdentity>,
    ) -> Result<
        (
            AlpacaHistoricalPendingExtractionSeal,
            ProviderCaptureSealRequest,
        ),
        ExtractionSourceError,
    > {
        let _operation = self.inner.admit().map_err(map_capability_error)?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
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
            .history_capture_semantic(
                instrument_revision_digest,
                admitted_plan_digest,
                identity.as_ref(),
                &preflight,
            )
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
            vec![identity],
        )
        .map_err(|_error| SourceError::InvalidProtocolState)?;
        let extracted = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                Err(SourceError::SessionNotCurrent.into())
            }
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            result = source.extract_for_sealing(
                authority, request, cancellation.clone(), calendar_capture, history_capture_semantic,
            ) => result,
        };
        drop(source);
        let pending = extracted?;
        self.validate_current(&cancellation)
            .await
            .map_err(map_capability_error)?;
        Ok(pending)
    }

    /// Revalidates the revocable runtime around a pure, exact one-plan analytical mapping.
    pub(crate) fn analytical_dataset_for_plan(
        &self,
        config: &AlpacaHistoricalEquityConfig,
        canonical_instrument: &MarketDataInstrumentDefinition,
        batch: &ExtractionBatch,
        identity: &dyn market_squawk_sources::CurrentCatalogProviderIdentity,
    ) -> Result<SourceIdentifier, AlpacaHistoricalPlanOperationError> {
        let _operation = self.inner.admit()?;
        self.validate_current_now()?;
        let identifier =
            AlpacaHistoricalEquitySource::one_plan_analytical_dataset_identifier_for_batch(
                config,
                canonical_instrument,
                batch,
                identity,
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

    /// Rejects revoked/expired pure batch work without replaying durable activation per row.
    /// Full currentness still gates extraction boundaries and every sealed-capture rejoin.
    pub(super) fn validate_normalization_at(
        &self,
        at: market_squawk_domain::Timestamp,
    ) -> Result<(), AlpacaHistoricalCapabilityError> {
        self.inner.ensure_usable()?;
        if !self
            .inner
            .account_currentness
            .retained_time_window_contains(at)
        {
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
        calendar_metadata: SourceMetadata,
        calendar_rights: ResearchRightsAuthority,
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
            || calendar_metadata.source_id() != calendar_rights.source_id()
            || calendar_metadata.authorization() != historical_metadata.authorization()
            || calendar_metadata.budget_policy() != historical_metadata.budget_policy()
            || calendar_metadata.coverage().domain()
                != market_squawk_sources::CoverageDomain::MarketCalendar
            || calendar_metadata.provider() != historical_metadata.provider()
            || market_squawk_adapter_alpaca::validate_alpaca_calendar_metadata(
                &calendar_metadata,
                historical_request_bounds,
                activation.trading_api_environment(),
            )
            .is_err()
            || calendar_metadata.capabilities().live()
            || !calendar_metadata.capabilities().extraction()
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
                calendar_metadata,
                calendar_rights,
                currentness,
                synchronous_currentness,
                account_currentness: activation.currentness(),
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

    /// Original coordinates remain available only to the owner retiring this exact allocation.
    pub(super) fn retirement_capability(&self) -> AlpacaHistoricalRuntimeCapability {
        AlpacaHistoricalRuntimeCapability {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Completes local operation cleanup after the group has drained this original history parent.
    pub(super) async fn finish_shutdown(&mut self) -> Result<(), ServiceError> {
        self.begin_shutdown();
        while self.inner.active.load(Ordering::Acquire) != 0 {
            let notified = self.inner.idle.notified();
            if self.inner.active.load(Ordering::Acquire) != 0 {
                notified.await;
            }
        }
        self.inner.clear_authority();
        Ok(())
    }

    pub(super) async fn shutdown(mut self) -> Result<(), ServiceError> {
        self.finish_shutdown().await
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
    calendar_metadata: SourceMetadata,
    calendar_rights: ResearchRightsAuthority,
    currentness: Arc<CurrentnessValidator>,
    synchronous_currentness: Arc<SynchronousCurrentnessValidator>,
    account_currentness: ProviderAccountRuntimeCurrentness,
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

    fn clear_authority(&self) {
        self.authority
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
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
