//! Production source lifecycle authority over live and research runtime owners.

use std::{
    num::NonZeroU64,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_domain::{
    ConnectionGeneration, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::source::{
    SourceAuthorizationState, SourceAvailabilityState, SourceDoctorEvidence, SourceLifecycleAction,
    SourceLifecycleAuthority, SourceLifecycleBlocker, SourceLifecycleCommand,
    SourceLifecycleCommandInput, SourceLifecycleDisposition, SourceLifecycleError,
    SourceLifecycleReceipt, SourceLifecycleReceiptInput, SourceLifecycleState,
    SourceLifecycleStatus, SourceLifecycleStatusInput, SourceRateBudgetState, SourceRightsEvidence,
    SourceStartEligibility,
};
use crate::application::{
    AccountMarketSurface, MarketProviderGroupLifecycleEvidence, MarketRuntimeGroupGeneration,
    MarketRuntimeRegistry, PreparedMarketProviderConfigurationRequest,
};
use crate::provider_activation::ProviderMarketAccount;
use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ProviderPortalActivationAuthority,
};

use super::{
    cli_provider,
    provider_activation_state::{
        DurableActivationRecipeState, DurableProviderActivationState,
        DurableProviderActivationStateError, DurableSourceLifecyclePhase,
        DurableSourceLifecycleRecord, DurableSourceLifecycleTransition,
    },
};

const COINBASE_PUBLIC_LIVE_SURFACE: &str = "coinbase.public-market-data";
const COINBASE_DIRECT_LIVE_SURFACE: &str = "coinbase.exchange-direct-market-data";
const KRAKEN_PUBLIC_LIVE_SURFACE: &str = "kraken.spot-public-market-data";

const LIVE_SURFACES: [&str; 6] = [
    COINBASE_PUBLIC_LIVE_SURFACE,
    COINBASE_DIRECT_LIVE_SURFACE,
    KRAKEN_PUBLIC_LIVE_SURFACE,
    ProviderMarketAccount::AlpacaBasic.surface_id(),
    ProviderMarketAccount::TradierBrokerage.surface_id(),
    ProviderMarketAccount::KrakenLevel3.surface_id(),
];
const PUBLIC_LIVE_SURFACES: [&str; 2] = [COINBASE_PUBLIC_LIVE_SURFACE, KRAKEN_PUBLIC_LIVE_SURFACE];

/// Bounded result of restoring every independently active live source.
#[derive(Debug)]
pub(crate) struct LiveSourceRestoreReport {
    restored: Vec<SourceIdentifier>,
    failures: Vec<LiveSourceRestoreFailure>,
}

impl LiveSourceRestoreReport {
    pub(crate) fn restored(&self) -> &[SourceIdentifier] {
        &self.restored
    }

    pub(crate) fn failures(&self) -> &[LiveSourceRestoreFailure] {
        &self.failures
    }
}

/// One provider-scoped startup restoration failure.
#[derive(Clone, Debug)]
pub(crate) struct LiveSourceRestoreFailure {
    provider: SourceIdentifier,
    error: SourceLifecycleError,
}

impl LiveSourceRestoreFailure {
    pub(crate) const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    pub(crate) const fn error(&self) -> SourceLifecycleError {
        self.error
    }
}

/// Single lifecycle authority injected into the Source application domain.
pub(crate) struct ProductionSourceLifecycleAuthority {
    paths: LocalPaths,
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<ProviderAdapterActivation>,
    portal: Arc<dyn ProviderPortalActivationAuthority>,
    durable: DurableProviderActivationState,
    live: Arc<MarketRuntimeRegistry>,
}

impl ProductionSourceLifecycleAuthority {
    /// Binds the existing runtime owners without constructing another source runtime.
    pub(crate) fn new(
        paths: LocalPaths,
        onboarding: Arc<ProviderOnboardingService>,
        activation: Arc<ProviderAdapterActivation>,
        portal: Arc<dyn ProviderPortalActivationAuthority>,
        durable: DurableProviderActivationState,
        live: Arc<MarketRuntimeRegistry>,
    ) -> Self {
        Self {
            paths,
            onboarding,
            activation,
            portal,
            durable,
            live,
        }
    }

    /// Restores every live source whose durable desired state is active.
    ///
    /// Installed-service shutdown deliberately stops process-owned sockets without changing the
    /// user's durable source choice. On the next service generation, this method re-establishes
    /// that exact source before the service publishes readiness. Provider, credential, budget, or
    /// network failures remain explicit lifecycle blockers and do not fabricate an active runtime.
    pub(crate) async fn restore_active_live_sources(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<LiveSourceRestoreReport, SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        let mut active = Vec::new();
        active
            .try_reserve_exact(LIVE_SURFACES.len())
            .map_err(|_error| SourceLifecycleError::Unavailable)?;
        let mut restored = Vec::new();
        restored
            .try_reserve_exact(LIVE_SURFACES.len())
            .map_err(|_error| SourceLifecycleError::Unavailable)?;
        let mut failures = Vec::new();
        failures
            .try_reserve_exact(LIVE_SURFACES.len())
            .map_err(|_error| SourceLifecycleError::Unavailable)?;
        for surface in LIVE_SURFACES {
            let provider = SourceIdentifier::try_from(surface)
                .map_err(|_error| SourceLifecycleError::InvalidResult)?;
            let record = match self.durable.source_lifecycle_record(surface) {
                Ok(record) => record,
                Err(error) => {
                    failures.push(LiveSourceRestoreFailure {
                        provider,
                        error: map_durable_error(error),
                    });
                    continue;
                }
            };
            match record.phase() {
                DurableSourceLifecyclePhase::Active => active.push((provider, record)),
                DurableSourceLifecyclePhase::Stopped
                    if PUBLIC_LIVE_SURFACES.contains(&surface)
                        && self.live.is_account_free_source_configured(&provider)
                        && record.revision() == NonZeroU64::MIN
                        && record.operation_id().is_none() =>
                {
                    active.push((provider, record));
                }
                DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
                    if PUBLIC_LIVE_SURFACES.contains(&surface)
                        && self.live.is_account_free_source_configured(&provider)
                        && record.operation_id()
                            == Some(&default_public_start_operation_id(&provider)?) =>
                {
                    let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                        provider: provider.clone(),
                        action: SourceLifecycleAction::Retry,
                        expected_state_revision: record.revision(),
                        expected_generation: None,
                        expected_runtime_generation_digest: None,
                        onboarding_session_id: None,
                        public_configuration_digest: None,
                        reason: Some(
                            SourceIdentifier::try_from("automatic-public-source-recovery")
                                .map_err(|_error| SourceLifecycleError::Internal)?,
                        ),
                        cancellation: cancellation.child_token(),
                        deadline,
                    })?;
                    match self.execute_owned(&command).await {
                        Ok(receipt) if receipt.fields().provider == provider => {
                            restored.push(provider)
                        }
                        Ok(_receipt) => failures.push(LiveSourceRestoreFailure {
                            provider,
                            error: SourceLifecycleError::InvalidResult,
                        }),
                        Err(error) => failures.push(LiveSourceRestoreFailure { provider, error }),
                    }
                }
                DurableSourceLifecyclePhase::Stopped
                | DurableSourceLifecyclePhase::Removed
                | DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired => {}
            }
        }

        for (provider, record) in active {
            ensure_status_live(cancellation, deadline)?;
            if record.phase() == DurableSourceLifecyclePhase::Stopped {
                let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                    provider: provider.clone(),
                    action: SourceLifecycleAction::Start,
                    expected_state_revision: record.revision(),
                    expected_generation: None,
                    expected_runtime_generation_digest: None,
                    onboarding_session_id: None,
                    public_configuration_digest: None,
                    reason: None,
                    cancellation: cancellation.child_token(),
                    deadline,
                })?;
                match self.execute_owned(&command).await {
                    Ok(receipt) if receipt.fields().provider == provider => restored.push(provider),
                    Ok(_receipt) => failures.push(LiveSourceRestoreFailure {
                        provider,
                        error: SourceLifecycleError::InvalidResult,
                    }),
                    Err(error) => failures.push(LiveSourceRestoreFailure { provider, error }),
                }
            } else if let Some(surface) = AccountMarketSurface::parse(provider.as_str()) {
                let request = match self.restored_account_group_request(surface, &provider, &record)
                {
                    Ok(request) => request,
                    Err(error) => {
                        failures.push(LiveSourceRestoreFailure { provider, error });
                        continue;
                    }
                };
                match self
                    .live
                    .start_account_group(request, deadline, cancellation)
                    .await
                    .map_err(map_live_error)
                {
                    Ok(evidence) => {
                        let group_generation = validate_account_group_evidence(request, &evidence)?;
                        match self
                            .live
                            .admit_account_group_reads(
                                request,
                                group_generation,
                                deadline,
                                cancellation,
                            )
                            .await
                            .map_err(map_live_error)
                        {
                            Ok(()) => restored.push(provider),
                            Err(error) => {
                                let error = match self
                                    .cleanup_account_group(request, group_generation)
                                    .await
                                {
                                    Ok(()) => error,
                                    Err(cleanup_error) => cleanup_error,
                                };
                                failures.push(LiveSourceRestoreFailure { provider, error });
                            }
                        }
                    }
                    Err(error) => failures.push(LiveSourceRestoreFailure { provider, error }),
                }
            } else {
                let session_id = match self.validate_restored_scalar_live_authority(
                    &provider,
                    record.session_id(),
                    record.public_configuration_digest(),
                ) {
                    Ok(session_id) => session_id,
                    Err(error) => {
                        failures.push(LiveSourceRestoreFailure { provider, error });
                        continue;
                    }
                };
                match self
                    .live
                    .start(&provider, session_id, deadline, cancellation)
                    .await
                    .map_err(map_live_error)
                {
                    Ok(evidence) if evidence.provider == provider => restored.push(provider),
                    Ok(_evidence) => failures.push(LiveSourceRestoreFailure {
                        provider,
                        error: SourceLifecycleError::InvalidResult,
                    }),
                    Err(error) => failures.push(LiveSourceRestoreFailure { provider, error }),
                }
            }
        }
        Ok(LiveSourceRestoreReport { restored, failures })
    }

    async fn execute_owned(
        &self,
        command: &SourceLifecycleCommand,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        ensure_live(command)?;
        let provider = command.provider().as_str().to_owned();
        let _mutation = self
            .durable
            .acquire_source_lifecycle(&provider)
            .await
            .map_err(map_durable_error)?;
        let command_digest = command_digest(command)?;
        let operation_id = operation_id(command_digest)?;
        let current = self
            .durable
            .source_lifecycle_record(&provider)
            .map_err(map_durable_error)?;
        let (target_session_id, target_public_configuration_digest) =
            self.lifecycle_transition_target(command, &current)?;
        self.preflight_runtime_lease(command, &current)?;
        let transition = self
            .durable
            .begin_source_lifecycle_transition(
                &provider,
                command.expected_state_revision(),
                operation_id.clone(),
                command_digest,
                matches!(
                    command.action(),
                    SourceLifecycleAction::Retry
                        | SourceLifecycleAction::Stop
                        | SourceLifecycleAction::Resynchronize
                        | SourceLifecycleAction::Remove
                ),
                target_session_id,
                target_public_configuration_digest,
            )
            .map_err(map_durable_error)?;
        if let DurableSourceLifecycleTransition::Replay(record) = transition {
            return self
                .receipt_for_current(
                    command,
                    operation_id,
                    SourceLifecycleDisposition::Replay,
                    &record,
                    None,
                )
                .await;
        }
        let transition_digest = transition.transition_digest();
        let prior_session_id = transition.record().session_id();
        let prior_public_configuration_digest = transition.record().public_configuration_digest();
        let prior_runtime_verification_receipt_digest =
            transition.record().runtime_verification_receipt_digest();
        let prior_credential_generation = transition.record().credential_generation();
        let prior_record = current;
        let result = if LIVE_SURFACES.contains(&provider.as_str()) {
            self.execute_live(
                command,
                prior_record.phase(),
                prior_session_id,
                prior_public_configuration_digest,
                prior_runtime_verification_receipt_digest,
                prior_credential_generation,
            )
            .await
        } else {
            self.execute_research(command, prior_session_id, prior_public_configuration_digest)
                .await
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error)
                if command.action() == SourceLifecycleAction::Verify
                    && command.provider().as_str()
                        == ProviderMarketAccount::AlpacaBasic.surface_id()
                    && doctor_attempt_had_no_onboarding_effect(error) =>
            {
                let _restored = self
                    .durable
                    .complete_source_lifecycle_no_effect(
                        &provider,
                        transition_digest,
                        &prior_record,
                    )
                    .map_err(map_durable_error)?;
                return Err(error);
            }
            Err(error) => {
                let _blocked = self
                    .durable
                    .require_source_lifecycle_reconciliation(&provider, transition_digest);
                return Err(error);
            }
        };
        if let Err(error) = ensure_live(command) {
            let cleanup = match outcome.account_group_read_admission {
                Some((request, group_generation)) => {
                    self.cleanup_account_group(request, group_generation).await
                }
                None => Ok(()),
            };
            let _blocked = self
                .durable
                .require_source_lifecycle_reconciliation(&provider, transition_digest);
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup_error) => cleanup_error,
            });
        }
        let record = match self.durable.complete_source_lifecycle_transition(
            &provider,
            transition_digest,
            outcome.phase,
            outcome.session_id,
            outcome.public_configuration_digest,
            outcome.runtime_verification_receipt_digest,
            outcome.credential_generation,
        ) {
            Ok(record) => record,
            Err(error) => {
                let cleanup = match outcome.account_group_read_admission {
                    Some((request, group_generation)) => {
                        self.cleanup_account_group(request, group_generation).await
                    }
                    None => Ok(()),
                };
                let _blocked = self
                    .durable
                    .require_source_lifecycle_reconciliation(&provider, transition_digest);
                return Err(match cleanup {
                    Ok(()) => map_durable_error(error),
                    Err(cleanup_error) => cleanup_error,
                });
            }
        };
        if let Some((request, group_generation)) = outcome.account_group_read_admission {
            if let Err(error) = self
                .live
                .admit_account_group_reads(
                    request,
                    group_generation,
                    command.deadline(),
                    command.cancellation(),
                )
                .await
            {
                let cleanup = self.cleanup_account_group(request, group_generation).await;
                let _blocked = self
                    .durable
                    .require_completed_source_lifecycle_reconciliation(
                        &provider,
                        transition_digest,
                    );
                return Err(match cleanup {
                    Ok(()) => map_live_error(error),
                    Err(cleanup_error) => cleanup_error,
                });
            }
        }
        self.receipt_for_current(
            command,
            operation_id,
            SourceLifecycleDisposition::Applied,
            &record,
            outcome.previous_generation,
        )
        .await
    }

    async fn status_owned(
        &self,
        provider: &SourceIdentifier,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: Instant,
    ) -> Result<SourceLifecycleStatus, SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        let _read = self
            .durable
            .acquire_source_lifecycle(provider.as_str())
            .await
            .map_err(map_durable_error)?;
        ensure_status_live(cancellation, deadline)?;
        let record = self
            .durable
            .source_lifecycle_record(provider.as_str())
            .map_err(map_durable_error)?;
        let (configuration_session_id, public_configuration_digest) =
            self.status_configuration_binding(provider, &record)?;
        let mut state = if record.phase() == DurableSourceLifecyclePhase::Applying {
            SourceLifecycleState::Blocked
        } else {
            map_phase(record.phase())
        };
        let mut blocker = if state == SourceLifecycleState::Blocked {
            Some(SourceLifecycleBlocker::Reconciliation)
        } else {
            None
        };
        let mut live = None;
        let mut account_group_generation = None;
        let mut research = None;
        if state == SourceLifecycleState::Active
            && let Some(surface) = AccountMarketSurface::parse(provider.as_str())
        {
            let request = account_group_request_from_record(surface, &record)?;
            match self
                .live
                .verify_account_group(request, deadline, cancellation)
                .await
            {
                Ok(Some(evidence)) => {
                    account_group_generation =
                        Some(validate_account_group_evidence(request, &evidence)?.digest());
                }
                Ok(None) | Err(market_squawk_services::ServiceError::Unavailable) => {
                    state = SourceLifecycleState::Blocked;
                    blocker = Some(SourceLifecycleBlocker::ProviderAvailability);
                }
                Err(error) => return Err(map_live_error(error)),
            }
        } else if state == SourceLifecycleState::Active
            && LIVE_SURFACES.contains(&provider.as_str())
        {
            match self.live.verify(provider, deadline, cancellation).await {
                Ok(Some(evidence)) => live = Some(evidence.generation),
                Ok(None) | Err(market_squawk_services::ServiceError::Unavailable) => {
                    state = SourceLifecycleState::Blocked;
                    blocker = Some(SourceLifecycleBlocker::ProviderAvailability);
                }
                Err(error) => return Err(map_live_error(error)),
            }
        } else if state == SourceLifecycleState::Active {
            match self.activation.research_runtime_generation(provider) {
                Ok(Some(generation)) => {
                    research = generation
                        .generation_digest()
                        .map(Some)
                        .map_err(|_| SourceLifecycleError::InvalidResult)?;
                }
                Ok(None) | Err(_) => {
                    state = SourceLifecycleState::Blocked;
                    blocker = Some(SourceLifecycleBlocker::ProviderAvailability);
                }
            }
        }
        ensure_status_live(cancellation, deadline)?;
        let observed_at = system_timestamp()?;
        let (doctor, start_eligibility) = self.doctor_status(provider, &record, state, observed_at);
        SourceLifecycleStatus::try_new(SourceLifecycleStatusInput {
            provider: provider.clone(),
            state_revision: record.revision(),
            state,
            configuration_session_id,
            current_generation: live,
            runtime_generation_digest: account_group_generation.or(research),
            public_configuration_digest,
            doctor,
            start_eligibility,
            blocker,
            observed_at,
        })
    }

    async fn execute_live(
        &self,
        command: &SourceLifecycleCommand,
        prior_phase: DurableSourceLifecyclePhase,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
        prior_runtime_verification_receipt_digest: Option<EvidenceDigest>,
        prior_credential_generation: Option<market_squawk_platform::SecretGeneration>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        if command.action() == SourceLifecycleAction::Verify
            && command.provider().as_str() == ProviderMarketAccount::AlpacaBasic.surface_id()
        {
            let session_id = prior_session_id.ok_or(SourceLifecycleError::InvalidRequest)?;
            let prior_request = if prior_phase == DurableSourceLifecyclePhase::Active {
                Some(account_group_request_from_values(
                    AccountMarketSurface::AlpacaBasic,
                    prior_session_id,
                    prior_public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                )?)
            } else {
                None
            };
            let lease = self
                .onboarding
                .verify_runtime_activation_target(session_id, command.cancellation().child_token())
                .await
                .map_err(map_onboarding_error)?;
            if lease.surface_id() != command.provider()
                || Some(lease.public_configuration_digest()) != prior_public_configuration_digest
            {
                return Err(SourceLifecycleError::Conflict);
            }
            if let Some(request) = prior_request {
                let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
                let cleanup = CancellationToken::new();
                self.live
                    .stop_account_group(request, None, deadline, &cleanup)
                    .await
                    .map_err(map_live_error)?;
            }
            return LifecycleOutcome::stopped_with_runtime_verification(&lease);
        }
        let supplied_lease = self.optional_exact_lease(command)?;
        let lease = match supplied_lease {
            Some(lease) => Some(lease),
            None if live_action_requires_current_lease(command.action()) => prior_session_id
                .and_then(|session_id| {
                    self.onboarding
                        .activation_lease(session_id)
                        .or_else(|_| self.onboarding.prepared_activation_lease(session_id))
                        .ok()
                }),
            None => None,
        };
        if live_action_requires_current_lease(command.action())
            && is_session_backed_live_surface(command.provider().as_str())
        {
            let lease = lease.as_ref().ok_or(SourceLifecycleError::Unauthorized)?;
            if lease.surface_id() != command.provider() {
                return Err(SourceLifecycleError::Conflict);
            }
            if command.action() != SourceLifecycleAction::Reconfigure
                && (prior_session_id.is_some() || prior_public_configuration_digest.is_some())
                && (prior_session_id != Some(lease.session_id())
                    || prior_public_configuration_digest
                        != Some(lease.public_configuration_digest()))
            {
                return Err(SourceLifecycleError::Conflict);
            }
        }
        let (session_id, public_configuration_digest) =
            if live_action_requires_current_lease(command.action()) {
                (
                    lease
                        .as_ref()
                        .map(|value| value.session_id())
                        .or(prior_session_id),
                    lease
                        .as_ref()
                        .map(|value| value.public_configuration_digest())
                        .or(prior_public_configuration_digest),
                )
            } else {
                (prior_session_id, prior_public_configuration_digest)
            };
        if let Some(surface) = AccountMarketSurface::parse(command.provider().as_str()) {
            let mut outcome = self
                .execute_account_group_live(
                    command,
                    surface,
                    prior_session_id,
                    prior_public_configuration_digest,
                    session_id,
                    public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                    lease.as_ref(),
                )
                .await?;
            if let Some(lease) = lease.as_ref() {
                outcome.bind_runtime_verification(lease)?;
            }
            return Ok(outcome);
        }
        match command.action() {
            SourceLifecycleAction::Start | SourceLifecycleAction::Retry => {
                let evidence = self
                    .live
                    .start(
                        command.provider(),
                        session_id,
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?;
                if command
                    .expected_generation()
                    .is_some_and(|expected| evidence.generation.get() <= expected.get())
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                Ok(LifecycleOutcome::active(
                    session_id,
                    public_configuration_digest,
                    None,
                ))
            }
            SourceLifecycleAction::Stop => {
                let previous = self
                    .live
                    .stop(
                        command.provider(),
                        command.expected_generation(),
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?;
                Ok(LifecycleOutcome::stopped(
                    previous,
                    session_id,
                    public_configuration_digest,
                ))
            }
            SourceLifecycleAction::Resynchronize | SourceLifecycleAction::Reconfigure => {
                let expected = match command.expected_generation() {
                    Some(expected) => expected,
                    None if command.action() == SourceLifecycleAction::Reconfigure => self
                        .live
                        .verify(
                            command.provider(),
                            command.deadline(),
                            command.cancellation(),
                        )
                        .await
                        .map_err(map_live_error)?
                        .map(|evidence| evidence.generation)
                        .ok_or(SourceLifecycleError::Unavailable)?,
                    None => return Err(SourceLifecycleError::InvalidRequest),
                };
                let (previous, _current) = self
                    .live
                    .resynchronize(
                        command.provider(),
                        expected,
                        session_id,
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?;
                Ok(LifecycleOutcome::active(
                    session_id,
                    public_configuration_digest,
                    Some(previous),
                ))
            }
            SourceLifecycleAction::Verify => {
                self.live
                    .verify(
                        command.provider(),
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?
                    .ok_or(SourceLifecycleError::Unavailable)?;
                Ok(LifecycleOutcome::active(
                    session_id,
                    public_configuration_digest,
                    None,
                ))
            }
            SourceLifecycleAction::Remove => {
                let previous = self
                    .live
                    .remove(
                        command.provider(),
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?;
                if let Some(session_id) = session_id {
                    self.portal
                        .cancel(session_id, command.cancellation().child_token())
                        .await
                        .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                }
                Ok(LifecycleOutcome::removed(previous))
            }
        }
    }

    async fn execute_account_group_live(
        &self,
        command: &SourceLifecycleCommand,
        surface: AccountMarketSurface,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        prior_runtime_verification_receipt_digest: Option<EvidenceDigest>,
        prior_credential_generation: Option<market_squawk_platform::SecretGeneration>,
        lease: Option<&crate::ProviderActivationLease>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        match command.action() {
            SourceLifecycleAction::Start | SourceLifecycleAction::Retry => {
                let request = account_group_request_from_binding(
                    surface,
                    session_id,
                    public_configuration_digest,
                    lease,
                )?;
                let evidence = self
                    .live
                    .start_account_group(request, command.deadline(), command.cancellation())
                    .await
                    .map_err(map_live_error)?;
                let group_generation = validate_account_group_evidence(request, &evidence)?;
                Ok(LifecycleOutcome::active_account(
                    session_id,
                    public_configuration_digest,
                    None,
                    request,
                    group_generation,
                ))
            }
            SourceLifecycleAction::Stop => {
                let request = account_group_request_from_values(
                    surface,
                    prior_session_id,
                    prior_public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                )?;
                self.stop_account_group_exact(command, request, false)
                    .await?;
                Ok(LifecycleOutcome::stopped_account(request))
            }
            SourceLifecycleAction::Resynchronize => {
                if command.expected_generation().is_some()
                    || command.expected_runtime_generation_digest().is_none()
                {
                    return Err(SourceLifecycleError::InvalidRequest);
                }
                let request = account_group_request_from_binding(
                    surface,
                    session_id,
                    public_configuration_digest,
                    lease,
                )?;
                let previous = self
                    .stop_account_group_exact(command, request, true)
                    .await?
                    .ok_or(SourceLifecycleError::Unavailable)?;
                let current = self
                    .live
                    .start_account_group(request, command.deadline(), command.cancellation())
                    .await
                    .map_err(map_live_error)?;
                let current = validate_account_group_evidence(request, &current)?;
                if current == previous {
                    let cleanup = self
                        .live
                        .stop_account_group(
                            request,
                            Some(current),
                            command.deadline(),
                            command.cancellation(),
                        )
                        .await
                        .map_err(|_error| SourceLifecycleError::ReconciliationRequired)?;
                    if cleanup != Some(current) {
                        return Err(SourceLifecycleError::ReconciliationRequired);
                    }
                    return Err(SourceLifecycleError::ReconciliationRequired);
                }
                Ok(LifecycleOutcome::active_account(
                    session_id,
                    public_configuration_digest,
                    None,
                    request,
                    current,
                ))
            }
            SourceLifecycleAction::Verify => {
                let request = account_group_request_from_binding(
                    surface,
                    session_id,
                    public_configuration_digest,
                    lease,
                )?;
                let evidence = self
                    .live
                    .verify_account_group(request, command.deadline(), command.cancellation())
                    .await
                    .map_err(map_live_error)?
                    .ok_or(SourceLifecycleError::Unavailable)?;
                validate_account_group_evidence(request, &evidence)?;
                Ok(LifecycleOutcome::active(
                    session_id,
                    public_configuration_digest,
                    None,
                ))
            }
            SourceLifecycleAction::Reconfigure => {
                let request = account_group_request_from_binding(
                    surface,
                    session_id,
                    public_configuration_digest,
                    lease,
                )?;
                if prior_session_id.is_some() || prior_public_configuration_digest.is_some() {
                    let prior_request = account_group_request_from_values(
                        surface,
                        prior_session_id,
                        prior_public_configuration_digest,
                        prior_runtime_verification_receipt_digest,
                        prior_credential_generation,
                    )?;
                    self.stop_account_group_exact(command, prior_request, false)
                        .await?;
                }
                let evidence = self
                    .live
                    .start_account_group(request, command.deadline(), command.cancellation())
                    .await
                    .map_err(map_live_error)?;
                let group_generation = validate_account_group_evidence(request, &evidence)?;
                Ok(LifecycleOutcome::active_account(
                    session_id,
                    public_configuration_digest,
                    None,
                    request,
                    group_generation,
                ))
            }
            SourceLifecycleAction::Remove => {
                self.live
                    .remove_account_group(surface, command.deadline(), command.cancellation())
                    .await
                    .map_err(map_live_error)?;
                if let Some(session_id) = prior_session_id {
                    self.portal
                        .cancel(session_id, command.cancellation().child_token())
                        .await
                        .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                }
                Ok(LifecycleOutcome::removed(None))
            }
        }
    }

    async fn stop_account_group_exact(
        &self,
        command: &SourceLifecycleCommand,
        request: PreparedMarketProviderConfigurationRequest,
        require_present: bool,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, SourceLifecycleError> {
        let expected = match self
            .live
            .verify_account_group(request, command.deadline(), command.cancellation())
            .await
        {
            Ok(Some(evidence)) => Some(validate_account_group_evidence(request, &evidence)?),
            Ok(None) => None,
            Err(market_squawk_services::ServiceError::Unavailable) if !require_present => None,
            Err(error) => return Err(map_live_error(error)),
        };
        if require_present && expected.is_none() {
            return Err(SourceLifecycleError::Unavailable);
        }
        if let Some(expected_digest) = command.expected_runtime_generation_digest()
            && expected.map(MarketRuntimeGroupGeneration::digest) != Some(expected_digest)
        {
            return Err(SourceLifecycleError::Conflict);
        }
        let stopped = self
            .live
            .stop_account_group(
                request,
                expected,
                command.deadline(),
                command.cancellation(),
            )
            .await
            .map_err(map_live_error)?;
        if expected.is_some() && stopped != expected {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(stopped)
    }

    async fn cleanup_account_group(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        expected: MarketRuntimeGroupGeneration,
    ) -> Result<(), SourceLifecycleError> {
        let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
        let cancellation = CancellationToken::new();
        match self
            .live
            .stop_account_group(request, Some(expected), deadline, &cancellation)
            .await
            .map_err(map_live_error)?
        {
            Some(stopped) if stopped != expected => Err(SourceLifecycleError::InvalidResult),
            Some(_) | None => Ok(()),
        }
    }

    async fn execute_research(
        &self,
        command: &SourceLifecycleCommand,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        if command.action() == SourceLifecycleAction::Resynchronize {
            return Err(SourceLifecycleError::InvalidRequest);
        }
        let profile = command.provider();
        let current = self
            .activation
            .research_runtime_generation(profile)
            .map_err(|_| SourceLifecycleError::Unavailable)?;
        match command.action() {
            SourceLifecycleAction::Start | SourceLifecycleAction::Reconfigure => {
                let lease = self.exact_lease(command)?;
                if command
                    .public_configuration_digest()
                    .is_some_and(|digest| digest != lease.public_configuration_digest())
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                if current
                    .as_ref()
                    .is_some_and(|runtime| runtime.session_id() != lease.session_id())
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                if current.is_none() {
                    cli_provider::resume_exact_research_provider(
                        &self.paths,
                        &self.onboarding,
                        &self.activation,
                        &self.durable,
                        profile.as_str(),
                        lease.session_id(),
                        command.cancellation().child_token(),
                    )
                    .await
                    .map_err(|_| SourceLifecycleError::Unavailable)?;
                }
                Ok(LifecycleOutcome::active(
                    Some(lease.session_id()),
                    Some(lease.public_configuration_digest()),
                    None,
                ))
            }
            SourceLifecycleAction::Retry => {
                let retained = self
                    .retained_recipe(profile.as_str())?
                    .ok_or(SourceLifecycleError::NotFound)?;
                if let Some(runtime) = current.as_ref() {
                    if runtime.session_id() != retained.session_id {
                        return Err(SourceLifecycleError::Conflict);
                    }
                } else {
                    cli_provider::resume_exact_research_provider(
                        &self.paths,
                        &self.onboarding,
                        &self.activation,
                        &self.durable,
                        profile.as_str(),
                        retained.session_id,
                        command.cancellation().child_token(),
                    )
                    .await
                    .map_err(|_| SourceLifecycleError::Unavailable)?;
                }
                let public_configuration_digest = self
                    .onboarding
                    .activation_lease(retained.session_id)
                    .ok()
                    .map(|lease| lease.public_configuration_digest())
                    .or(prior_public_configuration_digest);
                Ok(LifecycleOutcome::active(
                    Some(retained.session_id),
                    public_configuration_digest,
                    None,
                ))
            }
            SourceLifecycleAction::Stop => {
                if let Some(runtime) = current.as_ref() {
                    self.activation
                        .revoke_research_runtime(runtime)
                        .await
                        .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                }
                let retained = self.retained_recipe(profile.as_str())?;
                Ok(LifecycleOutcome::stopped(
                    None,
                    retained
                        .as_ref()
                        .map(|recipe| recipe.session_id)
                        .or(prior_session_id),
                    prior_public_configuration_digest,
                ))
            }
            SourceLifecycleAction::Verify => {
                let lease = self.exact_lease(command)?;
                let runtime = current.ok_or(SourceLifecycleError::Unavailable)?;
                if runtime.session_id() != lease.session_id()
                    || runtime.capability_digest() != lease.capability_digest()
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                Ok(LifecycleOutcome::active(
                    Some(lease.session_id()),
                    Some(lease.public_configuration_digest()),
                    None,
                ))
            }
            SourceLifecycleAction::Remove => {
                let session_id = current
                    .as_ref()
                    .map(|runtime| runtime.session_id())
                    .or_else(|| {
                        self.retained_recipe(profile.as_str())
                            .ok()
                            .flatten()
                            .map(|recipe| recipe.session_id)
                    })
                    .ok_or(SourceLifecycleError::NotFound)?;
                if let Some(runtime) = current.as_ref() {
                    self.activation
                        .revoke_research_runtime(runtime)
                        .await
                        .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                }
                self.portal
                    .cancel(session_id, command.cancellation().child_token())
                    .await
                    .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                Ok(LifecycleOutcome::removed(None))
            }
            SourceLifecycleAction::Resynchronize => Err(SourceLifecycleError::InvalidRequest),
        }
    }

    async fn receipt_for_current(
        &self,
        command: &SourceLifecycleCommand,
        operation_id: SourceIdentifier,
        disposition: SourceLifecycleDisposition,
        record: &DurableSourceLifecycleRecord,
        previous_generation: Option<ConnectionGeneration>,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        let observed_at = system_timestamp()?;
        let account_group_generation = if record.phase() == DurableSourceLifecyclePhase::Active
            && let Some(surface) = AccountMarketSurface::parse(command.provider().as_str())
        {
            let request = account_group_request_from_record(surface, record)?;
            let evidence = self
                .live
                .verify_account_group(request, command.deadline(), command.cancellation())
                .await
                .map_err(map_live_error)?
                .ok_or(SourceLifecycleError::Unavailable)?;
            Some(validate_account_group_evidence(request, &evidence)?)
        } else {
            None
        };
        let live = if AccountMarketSurface::parse(command.provider().as_str()).is_none()
            && LIVE_SURFACES.contains(&command.provider().as_str())
        {
            self.live
                .verify(
                    command.provider(),
                    command.deadline(),
                    command.cancellation(),
                )
                .await
                .map_err(map_live_error)?
        } else {
            None
        };
        let runtime_generation_digest = if let Some(generation) = account_group_generation {
            Some(generation.digest())
        } else if !LIVE_SURFACES.contains(&command.provider().as_str())
            && record.phase() == DurableSourceLifecyclePhase::Active
        {
            self.activation
                .research_runtime_generation(command.provider())
                .map_err(|_| SourceLifecycleError::Unavailable)?
                .ok_or(SourceLifecycleError::Unavailable)?
                .generation_digest()
                .map(Some)
                .map_err(|_| SourceLifecycleError::InvalidResult)?
        } else {
            None
        };
        let lease = record
            .session_id()
            .and_then(|session_id| {
                self.onboarding
                    .activation_lease(session_id)
                    .ok()
                    .or_else(|| {
                        (record.phase() == DurableSourceLifecyclePhase::Stopped)
                            .then(|| self.onboarding.prepared_activation_lease(session_id).ok())
                            .flatten()
                    })
            })
            .filter(|lease| {
                lease.surface_id() == command.provider()
                    && Some(lease.public_configuration_digest())
                        == record.public_configuration_digest()
            });
        let rights_evidence = lease
            .as_ref()
            .map(|lease| {
                SourceRightsEvidence::try_new(
                    SourceIdentifier::try_from(format!(
                        "source-rights-{}",
                        &lower_hex(&lease.rights_decision_digest().bytes())[..24]
                    ))
                    .map_err(|_| SourceLifecycleError::InvalidResult)?,
                    lease.rights_decision_digest(),
                    lease.authority_effective_at(),
                    lease.verification_expires_at(),
                )
            })
            .transpose()?;
        let state = map_phase(record.phase());
        let (doctor, start_eligibility) =
            self.doctor_status(command.provider(), record, state, observed_at);
        SourceLifecycleReceipt::try_new(SourceLifecycleReceiptInput {
            operation_id,
            provider: command.provider().clone(),
            action: command.action(),
            disposition,
            state,
            state_revision: record.revision(),
            previous_generation,
            current_generation: live.as_ref().map(|evidence| evidence.generation),
            runtime_generation_digest,
            coverage: live.as_ref().map(|evidence| evidence.coverage),
            integrity: live.as_ref().map(|evidence| evidence.integrity),
            quality: live.as_ref().map(|evidence| evidence.quality),
            rate_budget: SourceRateBudgetState::Indeterminate,
            authorization: if lease
                .as_ref()
                .is_some_and(|lease| lease.generation().is_some())
            {
                SourceAuthorizationState::Admitted
            } else if record.session_id().is_some() {
                SourceAuthorizationState::Blocked
            } else {
                SourceAuthorizationState::NotRequired
            },
            availability: match state {
                SourceLifecycleState::Removed => SourceAvailabilityState::Removed,
                SourceLifecycleState::Active
                    if live.is_some() || account_group_generation.is_some() =>
                {
                    SourceAvailabilityState::Available
                }
                SourceLifecycleState::Active => SourceAvailabilityState::Indeterminate,
                SourceLifecycleState::Starting
                | SourceLifecycleState::Resynchronizing
                | SourceLifecycleState::Blocked
                | SourceLifecycleState::Stopped => SourceAvailabilityState::Indeterminate,
            },
            rights_evidence,
            blocker: if state == SourceLifecycleState::Blocked {
                Some(SourceLifecycleBlocker::Reconciliation)
            } else {
                None
            },
            public_configuration_digest: record.public_configuration_digest(),
            configuration_session_id: record.session_id(),
            doctor,
            start_eligibility,
            observed_at,
        })
    }

    fn doctor_status(
        &self,
        provider: &SourceIdentifier,
        record: &DurableSourceLifecycleRecord,
        visible_state: SourceLifecycleState,
        observed_at: Timestamp,
    ) -> (Option<SourceDoctorEvidence>, SourceStartEligibility) {
        if provider.as_str() != ProviderMarketAccount::AlpacaBasic.surface_id() {
            return (None, SourceStartEligibility::NotApplicable);
        }
        if matches!(
            record.phase(),
            DurableSourceLifecyclePhase::Applying
                | DurableSourceLifecyclePhase::ReconciliationRequired
        ) {
            return (None, SourceStartEligibility::ReconciliationRequired);
        }
        let Some(session_id) = record.session_id() else {
            return (None, SourceStartEligibility::DoctorRequired);
        };
        let Some(public_configuration_digest) = record.public_configuration_digest() else {
            return (None, SourceStartEligibility::CredentialStale);
        };
        let Some(generation) = record.credential_generation() else {
            return (None, SourceStartEligibility::DoctorRequired);
        };
        let retained = self.onboarding.retained_runtime_verification_evidence(
            session_id,
            provider,
            public_configuration_digest,
            generation,
        );
        let Ok(retained) = retained else {
            return (None, SourceStartEligibility::DoctorRequired);
        };
        if Some(retained.evidence().evidence_digest())
            != record.runtime_verification_receipt_digest()
        {
            return (None, SourceStartEligibility::CredentialStale);
        }
        let Some(receipt) = retained.evidence().alpaca_paper_iex_receipt().cloned() else {
            return (None, SourceStartEligibility::DoctorRequired);
        };
        let evidence = match SourceDoctorEvidence::try_new(receipt, observed_at) {
            Ok(evidence) => evidence,
            Err(_) => return (None, SourceStartEligibility::CredentialStale),
        };
        let onboarding_ready = matches!(
            retained.onboarding_state(),
            market_squawk_sources::OnboardingState::RuntimeVerificationPending
                | market_squawk_sources::OnboardingState::ActiveScoped
                | market_squawk_sources::OnboardingState::RenewalRequired
        );
        let eligibility = if !onboarding_ready {
            SourceStartEligibility::CredentialStale
        } else if !evidence.current() {
            SourceStartEligibility::DoctorExpired
        } else if evidence.receipt().admits_source_start() {
            match visible_state {
                SourceLifecycleState::Active => SourceStartEligibility::AlreadyActive,
                SourceLifecycleState::Stopped => SourceStartEligibility::Eligible,
                SourceLifecycleState::Blocked => SourceStartEligibility::ProviderUnavailable,
                SourceLifecycleState::Starting
                | SourceLifecycleState::Resynchronizing
                | SourceLifecycleState::Removed => SourceStartEligibility::ReconciliationRequired,
            }
        } else {
            SourceStartEligibility::ProviderUnavailable
        };
        (Some(evidence), eligibility)
    }

    fn status_configuration_binding(
        &self,
        provider: &SourceIdentifier,
        record: &DurableSourceLifecycleRecord,
    ) -> Result<(Option<uuid::Uuid>, Option<EvidenceDigest>), SourceLifecycleError> {
        match (record.session_id(), record.public_configuration_digest()) {
            (Some(session_id), Some(public_configuration_digest)) => {
                Ok((Some(session_id), Some(public_configuration_digest)))
            }
            (None, None) if is_session_backed_live_surface(provider.as_str()) => self
                .onboarding
                .current_runtime_activation_target(provider)
                .map(|binding| match binding {
                    Some((session_id, digest)) => (Some(session_id), Some(digest)),
                    None => (None, None),
                })
                .map_err(map_onboarding_error),
            (None, None) => Ok((None, None)),
            _ => Err(SourceLifecycleError::InvalidResult),
        }
    }

    fn validate_restored_scalar_live_authority(
        &self,
        provider: &SourceIdentifier,
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
    ) -> Result<Option<uuid::Uuid>, SourceLifecycleError> {
        if !is_session_backed_live_surface(provider.as_str()) {
            return if session_id.is_none() && public_configuration_digest.is_none() {
                Ok(None)
            } else {
                Err(SourceLifecycleError::Conflict)
            };
        }
        let session_id = session_id.ok_or(SourceLifecycleError::Unauthorized)?;
        let lease = self
            .onboarding
            .activation_lease(session_id)
            .map_err(|_| SourceLifecycleError::Unauthorized)?;
        if lease.session_id() != session_id
            || lease.surface_id() != provider
            || Some(lease.public_configuration_digest()) != public_configuration_digest
        {
            return Err(SourceLifecycleError::Conflict);
        }
        Ok(Some(session_id))
    }

    fn restored_account_group_request(
        &self,
        surface: AccountMarketSurface,
        provider: &SourceIdentifier,
        record: &DurableSourceLifecycleRecord,
    ) -> Result<PreparedMarketProviderConfigurationRequest, SourceLifecycleError> {
        self.validate_restored_scalar_live_authority(
            provider,
            record.session_id(),
            record.public_configuration_digest(),
        )?;
        account_group_request_from_record(surface, record)
    }

    fn optional_exact_lease(
        &self,
        command: &SourceLifecycleCommand,
    ) -> Result<Option<crate::ProviderActivationLease>, SourceLifecycleError> {
        command
            .onboarding_session_id()
            .map(|session_id| {
                let lease = self
                    .onboarding
                    .activation_lease(session_id)
                    .or_else(|_| self.onboarding.prepared_activation_lease(session_id))
                    .map_err(|_| SourceLifecycleError::Unauthorized)?;
                if lease.surface_id() != command.provider()
                    || command
                        .public_configuration_digest()
                        .is_some_and(|digest| digest != lease.public_configuration_digest())
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                Ok(lease)
            })
            .transpose()
    }

    fn preflight_runtime_lease(
        &self,
        command: &SourceLifecycleCommand,
        current: &DurableSourceLifecycleRecord,
    ) -> Result<(), SourceLifecycleError> {
        if !is_session_backed_live_surface(command.provider().as_str())
            || matches!(
                command.action(),
                SourceLifecycleAction::Verify
                    | SourceLifecycleAction::Stop
                    | SourceLifecycleAction::Remove
            )
        {
            return Ok(());
        }
        let session_id = command
            .onboarding_session_id()
            .or(current.session_id())
            .ok_or(SourceLifecycleError::Unauthorized)?;
        let public_configuration_digest = command
            .public_configuration_digest()
            .or(current.public_configuration_digest())
            .ok_or(SourceLifecycleError::Unauthorized)?;
        let lease = self
            .onboarding
            .activation_lease(session_id)
            .or_else(|_| self.onboarding.prepared_activation_lease(session_id))
            .map_err(|_| SourceLifecycleError::Unauthorized)?;
        if lease.surface_id() != command.provider()
            || lease.public_configuration_digest() != public_configuration_digest
            || command.action() != SourceLifecycleAction::Reconfigure
                && (current.session_id().is_some()
                    || current.public_configuration_digest().is_some())
                && (current.session_id() != Some(lease.session_id())
                    || current.public_configuration_digest()
                        != Some(lease.public_configuration_digest()))
        {
            return Err(SourceLifecycleError::Conflict);
        }
        Ok(())
    }

    fn lifecycle_transition_target(
        &self,
        command: &SourceLifecycleCommand,
        current: &DurableSourceLifecycleRecord,
    ) -> Result<(Option<uuid::Uuid>, Option<EvidenceDigest>), SourceLifecycleError> {
        if command.action() != SourceLifecycleAction::Verify
            || !is_session_backed_live_surface(command.provider().as_str())
        {
            return Ok((
                command.onboarding_session_id().or(current.session_id()),
                command
                    .public_configuration_digest()
                    .or(current.public_configuration_digest()),
            ));
        }
        let session_id = match (current.session_id(), command.onboarding_session_id()) {
            (Some(current), Some(supplied)) if current != supplied => {
                return Err(SourceLifecycleError::Conflict);
            }
            (Some(current), _) => current,
            (None, Some(supplied)) => supplied,
            (None, None) => return Err(SourceLifecycleError::InvalidRequest),
        };
        let public_configuration_digest = self
            .onboarding
            .runtime_activation_target_public_configuration(session_id, command.provider())
            .map_err(map_onboarding_error)?;
        if command
            .public_configuration_digest()
            .is_some_and(|supplied| supplied != public_configuration_digest)
        {
            return Err(SourceLifecycleError::Conflict);
        }
        if current
            .public_configuration_digest()
            .is_some_and(|current| current != public_configuration_digest)
        {
            return Err(SourceLifecycleError::Conflict);
        }
        Ok((Some(session_id), Some(public_configuration_digest)))
    }

    fn exact_lease(
        &self,
        command: &SourceLifecycleCommand,
    ) -> Result<crate::ProviderActivationLease, SourceLifecycleError> {
        self.optional_exact_lease(command)?
            .ok_or(SourceLifecycleError::InvalidRequest)
    }

    fn retained_recipe(
        &self,
        surface_id: &str,
    ) -> Result<
        Option<super::provider_activation_state::DurableActivationRecipe>,
        SourceLifecycleError,
    > {
        match self
            .durable
            .load_recipe_for_lifecycle(surface_id)
            .map_err(map_durable_error)?
        {
            DurableActivationRecipeState::Desired(recipe) => Ok(Some(recipe)),
            DurableActivationRecipeState::Missing
            | DurableActivationRecipeState::Staged(_)
            | DurableActivationRecipeState::Cutover(_)
            | DurableActivationRecipeState::Quarantined(_) => Ok(None),
        }
    }
}

const fn doctor_attempt_had_no_onboarding_effect(error: SourceLifecycleError) -> bool {
    matches!(
        error,
        SourceLifecycleError::RateLimited
            | SourceLifecycleError::Cancelled
            | SourceLifecycleError::DeadlineExceeded
            | SourceLifecycleError::Unavailable
    )
}

fn account_group_request_from_values(
    surface: AccountMarketSurface,
    session_id: Option<uuid::Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    runtime_verification_receipt_digest: Option<EvidenceDigest>,
    credential_generation: Option<market_squawk_platform::SecretGeneration>,
) -> Result<PreparedMarketProviderConfigurationRequest, SourceLifecycleError> {
    PreparedMarketProviderConfigurationRequest::try_new(
        surface,
        session_id.ok_or(SourceLifecycleError::Unauthorized)?,
        public_configuration_digest.ok_or(SourceLifecycleError::Unauthorized)?,
        runtime_verification_receipt_digest.ok_or(SourceLifecycleError::Unauthorized)?,
        credential_generation.ok_or(SourceLifecycleError::Unauthorized)?,
    )
    .map_err(|_error| SourceLifecycleError::InvalidResult)
}

fn account_group_request_from_binding(
    surface: AccountMarketSurface,
    session_id: Option<uuid::Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    lease: Option<&crate::ProviderActivationLease>,
) -> Result<PreparedMarketProviderConfigurationRequest, SourceLifecycleError> {
    let lease = lease.ok_or(SourceLifecycleError::Unauthorized)?;
    if Some(lease.session_id()) != session_id
        || Some(lease.public_configuration_digest()) != public_configuration_digest
    {
        return Err(SourceLifecycleError::Conflict);
    }
    PreparedMarketProviderConfigurationRequest::try_new(
        surface,
        session_id.ok_or(SourceLifecycleError::Unauthorized)?,
        public_configuration_digest.ok_or(SourceLifecycleError::Unauthorized)?,
        lease.runtime_evidence_digest(),
        lease
            .generation()
            .ok_or(SourceLifecycleError::Unauthorized)?,
    )
    .map_err(|_error| SourceLifecycleError::InvalidResult)
}

fn account_group_request_from_record(
    surface: AccountMarketSurface,
    record: &DurableSourceLifecycleRecord,
) -> Result<PreparedMarketProviderConfigurationRequest, SourceLifecycleError> {
    account_group_request_from_values(
        surface,
        record.session_id(),
        record.public_configuration_digest(),
        record.runtime_verification_receipt_digest(),
        record.credential_generation(),
    )
}

fn validate_account_group_evidence(
    request: PreparedMarketProviderConfigurationRequest,
    evidence: &MarketProviderGroupLifecycleEvidence,
) -> Result<MarketRuntimeGroupGeneration, SourceLifecycleError> {
    let generation = evidence.group_generation();
    let digest = generation.digest();
    if evidence.surface_id().as_str() != request.surface().surface_id()
        || evidence.onboarding_session_id() != request.onboarding_session_id()
        || evidence.public_configuration_digest() != request.expected_public_configuration_digest()
        || evidence.runtime_verification_receipt_digest()
            != request.expected_runtime_verification_receipt_digest()
        || evidence.credential_generation() != request.expected_credential_generation()
        || digest.algorithm() != DigestAlgorithm::Sha256
        || digest.bytes() == [0; 32]
    {
        return Err(SourceLifecycleError::InvalidResult);
    }
    Ok(generation)
}

fn is_session_backed_live_surface(surface_id: &str) -> bool {
    surface_id == COINBASE_DIRECT_LIVE_SURFACE
        || ProviderMarketAccount::from_surface_id(surface_id).is_some()
}

const fn live_action_requires_current_lease(action: SourceLifecycleAction) -> bool {
    !matches!(
        action,
        SourceLifecycleAction::Stop | SourceLifecycleAction::Remove
    )
}

fn default_public_start_operation_id(
    provider: &SourceIdentifier,
) -> Result<SourceIdentifier, SourceLifecycleError> {
    let deadline = Instant::now()
        .checked_add(std::time::Duration::from_secs(1))
        .ok_or(SourceLifecycleError::Internal)?;
    let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
        provider: provider.clone(),
        action: SourceLifecycleAction::Start,
        expected_state_revision: NonZeroU64::MIN,
        expected_generation: None,
        expected_runtime_generation_digest: None,
        onboarding_session_id: None,
        public_configuration_digest: None,
        reason: None,
        cancellation: CancellationToken::new(),
        deadline,
    })?;
    operation_id(command_digest(&command)?)
}

impl std::fmt::Debug for ProductionSourceLifecycleAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionSourceLifecycleAuthority")
            .field("paths", &"[LOCAL CAPABILITIES]")
            .field("onboarding", &"[ONBOARDING AUTHORITY]")
            .field("activation", &"[ADAPTER AUTHORITY]")
            .field("durable", &"[DURABLE LIFECYCLE]")
            .field("live", &"[MARKET RUNTIME REGISTRY]")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SourceLifecycleAuthority for ProductionSourceLifecycleAuthority {
    fn active_source_count(&self) -> Result<usize, SourceLifecycleError> {
        let active_live = self.live.active_source_count().map_err(map_live_error)?;
        let active_research = self
            .activation
            .active_research_runtime_count()
            .map_err(|_error| SourceLifecycleError::Unavailable)?;
        active_live
            .checked_add(active_research)
            .ok_or(SourceLifecycleError::Unavailable)
    }

    async fn status(
        &self,
        provider: &SourceIdentifier,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: Instant,
    ) -> Result<SourceLifecycleStatus, SourceLifecycleError> {
        self.status_owned(provider, cancellation, deadline).await
    }

    async fn execute(
        &self,
        command: SourceLifecycleCommand,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        self.execute_owned(&command).await
    }
}

struct LifecycleOutcome {
    phase: DurableSourceLifecyclePhase,
    session_id: Option<uuid::Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    runtime_verification_receipt_digest: Option<EvidenceDigest>,
    credential_generation: Option<market_squawk_platform::SecretGeneration>,
    previous_generation: Option<ConnectionGeneration>,
    account_group_read_admission: Option<(
        PreparedMarketProviderConfigurationRequest,
        MarketRuntimeGroupGeneration,
    )>,
}

impl LifecycleOutcome {
    const fn active(
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        previous_generation: Option<ConnectionGeneration>,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Active,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            previous_generation,
            account_group_read_admission: None,
        }
    }

    const fn active_account(
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        previous_generation: Option<ConnectionGeneration>,
        request: PreparedMarketProviderConfigurationRequest,
        group_generation: MarketRuntimeGroupGeneration,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Active,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            previous_generation,
            account_group_read_admission: Some((request, group_generation)),
        }
    }

    const fn stopped(
        previous_generation: Option<ConnectionGeneration>,
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Stopped,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            previous_generation,
            account_group_read_admission: None,
        }
    }

    fn stopped_with_runtime_verification(
        lease: &crate::ProviderActivationLease,
    ) -> Result<Self, SourceLifecycleError> {
        let generation = lease
            .generation()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        if lease.runtime_evidence_digest().bytes() == [0; 32] {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(Self {
            phase: DurableSourceLifecyclePhase::Stopped,
            session_id: Some(lease.session_id()),
            public_configuration_digest: Some(lease.public_configuration_digest()),
            runtime_verification_receipt_digest: Some(lease.runtime_evidence_digest()),
            credential_generation: Some(generation),
            previous_generation: None,
            account_group_read_admission: None,
        })
    }

    const fn stopped_account(request: PreparedMarketProviderConfigurationRequest) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Stopped,
            session_id: Some(request.onboarding_session_id()),
            public_configuration_digest: Some(request.expected_public_configuration_digest()),
            runtime_verification_receipt_digest: Some(
                request.expected_runtime_verification_receipt_digest(),
            ),
            credential_generation: Some(request.expected_credential_generation()),
            previous_generation: None,
            account_group_read_admission: None,
        }
    }

    fn bind_runtime_verification(
        &mut self,
        lease: &crate::ProviderActivationLease,
    ) -> Result<(), SourceLifecycleError> {
        let generation = lease
            .generation()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        if self.session_id != Some(lease.session_id())
            || self.public_configuration_digest != Some(lease.public_configuration_digest())
            || lease.runtime_evidence_digest().bytes() == [0; 32]
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        self.runtime_verification_receipt_digest = Some(lease.runtime_evidence_digest());
        self.credential_generation = Some(generation);
        Ok(())
    }

    const fn removed(previous_generation: Option<ConnectionGeneration>) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Removed,
            session_id: None,
            public_configuration_digest: None,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            previous_generation,
            account_group_read_admission: None,
        }
    }
}

fn ensure_live(command: &SourceLifecycleCommand) -> Result<(), SourceLifecycleError> {
    if command.cancellation().is_cancelled() {
        Err(SourceLifecycleError::Cancelled)
    } else if Instant::now() >= command.deadline() {
        Err(SourceLifecycleError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn ensure_status_live(
    cancellation: &tokio_util::sync::CancellationToken,
    deadline: Instant,
) -> Result<(), SourceLifecycleError> {
    if cancellation.is_cancelled() {
        Err(SourceLifecycleError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SourceLifecycleError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn command_digest(
    command: &SourceLifecycleCommand,
) -> Result<EvidenceDigest, SourceLifecycleError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/source-lifecycle-command/v2\0");
    hash_field(&mut hasher, command.provider().as_str().as_bytes())?;
    hasher.update([action_code(command.action())]);
    hasher.update(command.expected_state_revision().get().to_be_bytes());
    match command.expected_generation() {
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.get().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    match command.expected_runtime_generation_digest() {
        Some(digest) => {
            hasher.update([1]);
            hasher.update(digest.bytes());
        }
        None => hasher.update([0]),
    }
    match command.onboarding_session_id() {
        Some(session_id) => {
            hasher.update([1]);
            hasher.update(session_id.as_bytes());
        }
        None => hasher.update([0]),
    }
    match command.public_configuration_digest() {
        Some(digest) => {
            hasher.update([1]);
            hasher.update(digest.bytes());
        }
        None => hasher.update([0]),
    }
    if let Some(reason) = command.reason() {
        hasher.update([1]);
        hash_field(&mut hasher, reason.as_str().as_bytes())?;
    } else {
        hasher.update([0]);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

const fn action_code(action: SourceLifecycleAction) -> u8 {
    match action {
        SourceLifecycleAction::Start => 1,
        SourceLifecycleAction::Stop => 2,
        SourceLifecycleAction::Retry => 3,
        SourceLifecycleAction::Resynchronize => 4,
        SourceLifecycleAction::Verify => 5,
        SourceLifecycleAction::Reconfigure => 6,
        SourceLifecycleAction::Remove => 7,
    }
}

fn operation_id(digest: EvidenceDigest) -> Result<SourceIdentifier, SourceLifecycleError> {
    SourceIdentifier::try_from(format!(
        "source-lifecycle-{}",
        &lower_hex(&digest.bytes())[..32]
    ))
    .map_err(|_| SourceLifecycleError::Internal)
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) -> Result<(), SourceLifecycleError> {
    let length = u64::try_from(value.len()).map_err(|_| SourceLifecycleError::Internal)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn system_timestamp() -> Result<Timestamp, SourceLifecycleError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SourceLifecycleError::Internal)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| SourceLifecycleError::Internal)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

const fn map_phase(phase: DurableSourceLifecyclePhase) -> SourceLifecycleState {
    match phase {
        DurableSourceLifecyclePhase::Applying => SourceLifecycleState::Starting,
        DurableSourceLifecyclePhase::Active => SourceLifecycleState::Active,
        DurableSourceLifecyclePhase::Stopped => SourceLifecycleState::Stopped,
        DurableSourceLifecyclePhase::Removed => SourceLifecycleState::Removed,
        DurableSourceLifecyclePhase::ReconciliationRequired => SourceLifecycleState::Blocked,
    }
}

fn map_durable_error(error: DurableProviderActivationStateError) -> SourceLifecycleError {
    match error {
        DurableProviderActivationStateError::UnknownSurface
        | DurableProviderActivationStateError::InvalidRecipe
        | DurableProviderActivationStateError::MissingEvidence
        | DurableProviderActivationStateError::Integrity
        | DurableProviderActivationStateError::InvalidLifecycle => {
            SourceLifecycleError::InvalidResult
        }
        DurableProviderActivationStateError::ResourceExhausted
        | DurableProviderActivationStateError::EvidenceReclamation(_)
        | DurableProviderActivationStateError::Store(_) => SourceLifecycleError::Internal,
        DurableProviderActivationStateError::StaleState => SourceLifecycleError::Conflict,
        DurableProviderActivationStateError::LifecycleReconciliationRequired => {
            SourceLifecycleError::ReconciliationRequired
        }
    }
}

const fn map_live_error(error: market_squawk_services::ServiceError) -> SourceLifecycleError {
    match error {
        market_squawk_services::ServiceError::InvalidRequest => SourceLifecycleError::Conflict,
        market_squawk_services::ServiceError::NotFound => SourceLifecycleError::NotFound,
        market_squawk_services::ServiceError::Unauthorized => SourceLifecycleError::Unauthorized,
        market_squawk_services::ServiceError::Cancelled => SourceLifecycleError::Cancelled,
        market_squawk_services::ServiceError::DeadlineExceeded => {
            SourceLifecycleError::DeadlineExceeded
        }
        market_squawk_services::ServiceError::Unavailable => SourceLifecycleError::Unavailable,
        market_squawk_services::ServiceError::ResourceExhausted
        | market_squawk_services::ServiceError::InvalidResult
        | market_squawk_services::ServiceError::Internal => SourceLifecycleError::Internal,
    }
}

fn map_onboarding_error(error: crate::ProviderOnboardingError) -> SourceLifecycleError {
    match error {
        crate::ProviderOnboardingError::OperationCancelled => SourceLifecycleError::Cancelled,
        crate::ProviderOnboardingError::ProbeDeadlineExceeded => {
            SourceLifecycleError::DeadlineExceeded
        }
        crate::ProviderOnboardingError::ProbeRateLimited => SourceLifecycleError::RateLimited,
        crate::ProviderOnboardingError::RightsBlocked => SourceLifecycleError::Unauthorized,
        crate::ProviderOnboardingError::ActivationExpired
        | crate::ProviderOnboardingError::ActivationUnavailable
        | crate::ProviderOnboardingError::CredentialRejected
        | crate::ProviderOnboardingError::ProbeUnavailable => SourceLifecycleError::Unavailable,
        crate::ProviderOnboardingError::InvalidRequest
        | crate::ProviderOnboardingError::InvalidSessionState
        | crate::ProviderOnboardingError::UnknownProfile => SourceLifecycleError::Conflict,
        _ => SourceLifecycleError::Internal,
    }
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
