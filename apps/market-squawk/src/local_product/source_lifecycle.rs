//! Production source lifecycle authority over live and research runtime owners.

use std::{
    future::Future,
    num::NonZeroU64,
    pin::Pin,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp};
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
    AccountGroupStopAcknowledgementReceipt, AccountGroupStopDurableProof,
    AccountGroupStopHistoryEvidence, AccountGroupStopKeyEvidence, AccountGroupStopReceipt,
    AccountGroupStopState, AccountGroupStopTicket, AccountMarketSurface,
    MarketProviderGroupLifecycleEvidence, MarketRuntimeGroupGeneration, MarketRuntimeRegistry,
    MarketSourceRuntimeGeneration, PreparedMarketProviderConfigurationRequest,
};
use crate::provider_activation::ProviderMarketAccount;
use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ProviderPortalActivationAuthority,
};

use super::{
    cli_provider,
    provider_activation_state::{
        DurableAccountHistoryClaim, DurableAccountShutdownKey, DurableActivationRecipeState,
        DurableAlpacaHistoricalParent, DurableProviderActivationState,
        DurableProviderActivationStateError, DurableSourceLifecycleCheckpoint,
        DurableSourceLifecycleIntent, DurableSourceLifecyclePhase, DurableSourceLifecycleRecord,
        DurableSourceLifecycleTransition, DurableSourceRuntimeGeneration,
        source_lifecycle_account_stop_proof_digest, source_lifecycle_runtime_absent_proof_digest,
    },
};

const COINBASE_PUBLIC_LIVE_SURFACE: &str = "coinbase.public-market-data";
const COINBASE_DIRECT_LIVE_SURFACE: &str = "coinbase.exchange-direct-market-data";
const KRAKEN_PUBLIC_LIVE_SURFACE: &str = "kraken.spot-public-market-data";

const LIVE_SURFACES: [&str; 5] = [
    COINBASE_PUBLIC_LIVE_SURFACE,
    COINBASE_DIRECT_LIVE_SURFACE,
    KRAKEN_PUBLIC_LIVE_SURFACE,
    ProviderMarketAccount::AlpacaBasic.surface_id(),
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
        let transition = if command.action() == SourceLifecycleAction::Retry {
            let expected_transition = current
                .pending_transition_digest()
                .ok_or(SourceLifecycleError::ReconciliationRequired)?;
            let expected_intent = current
                .pending_intent()
                .ok_or(SourceLifecycleError::ReconciliationRequired)?;
            self.durable.resume_source_lifecycle_transition(
                &provider,
                command.expected_state_revision(),
                expected_transition,
                expected_intent,
            )
        } else {
            let (target_session_id, target_public_configuration_digest) =
                self.lifecycle_transition_target(command, &current)?;
            self.preflight_runtime_lease(command, &current)?;
            self.durable.begin_source_lifecycle_transition(
                &provider,
                command.expected_state_revision(),
                operation_id.clone(),
                command_digest,
                durable_intent(command)?,
                target_session_id,
                target_public_configuration_digest,
                expected_durable_runtime_generation(command)?,
            )
        }
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
        let transition_digest = transition.transition_digest().map_err(map_durable_error)?;
        let transition_intent = transition
            .record()
            .pending_intent()
            .ok_or(SourceLifecycleError::ReconciliationRequired)?;
        let retained_operation_id = transition
            .record()
            .pending_operation_id()
            .cloned()
            .ok_or(SourceLifecycleError::ReconciliationRequired)?;
        if AccountMarketSurface::parse(&provider).is_some()
            && transition_intent == DurableSourceLifecycleIntent::Stop
        {
            let result = match account_stop_predecessor(transition.record()) {
                Ok(AccountStopPredecessor::AccountGroup(previous_generation)) => {
                    drive_account_group_predecessor(
                        &self.durable,
                        &self.live,
                        &provider,
                        transition_digest,
                        command.deadline(),
                        command.cancellation(),
                        AccountGroupPredecessorDriveBoundary::ThroughDurableAcknowledgement,
                    )
                    .await
                    .map(|record| {
                        (
                            record,
                            Some(MarketSourceRuntimeGeneration::Group(previous_generation)),
                        )
                    })
                }
                Ok(AccountStopPredecessor::StoppedRuntimeAbsent) => self
                    .durable
                    .complete_retained_account_stop_no_effect(&provider, transition_digest)
                    .map(|record| (record, None))
                    .map_err(map_durable_error),
                Ok(AccountStopPredecessor::DesiredActiveRuntimeAbsent) => {
                    drive_desired_active_absent_account_stop(
                        &self.durable,
                        &self.live,
                        &provider,
                        transition_digest,
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map(|record| (record, None))
                }
                Err(error) => Err(error),
            };
            let (record, previous_generation) = match result {
                Ok(result) => result,
                Err(error) => {
                    let _blocked = self
                        .durable
                        .require_source_lifecycle_reconciliation(&provider, transition_digest);
                    return Err(error);
                }
            };
            return self
                .receipt_for_current(
                    command,
                    retained_operation_id,
                    SourceLifecycleDisposition::Applied,
                    &record,
                    previous_generation,
                )
                .await;
        }
        if command.action() == SourceLifecycleAction::Retry {
            let _blocked = self
                .durable
                .require_source_lifecycle_reconciliation(&provider, transition_digest);
            return Err(SourceLifecycleError::ReconciliationRequired);
        }
        let (transition_target_session_id, transition_target_public_configuration_digest) = {
            let pending = transition
                .record()
                .pending_view()
                .ok_or(SourceLifecycleError::ReconciliationRequired)?;
            (
                pending.target().session_id(),
                pending.target().public_configuration_digest(),
            )
        };
        let prior_session_id = current.session_id();
        let prior_public_configuration_digest = current.public_configuration_digest();
        let prior_runtime_verification_receipt_digest =
            current.runtime_verification_receipt_digest();
        let prior_credential_generation = current.credential_generation();
        let prior_record = current;
        let result = if LIVE_SURFACES.contains(&provider.as_str()) {
            let execution: Pin<
                Box<
                    dyn Future<Output = Result<LifecycleOutcome, SourceLifecycleError>> + Send + '_,
                >,
            > = if command.action() == SourceLifecycleAction::Verify
                && command.provider().as_str() == ProviderMarketAccount::AlpacaBasic.surface_id()
            {
                Box::pin(self.execute_alpaca_verify(
                    command,
                    transition_digest,
                    transition_target_session_id,
                    transition_target_public_configuration_digest,
                    prior_record.phase(),
                    prior_session_id,
                    prior_public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                ))
            } else if command.action() == SourceLifecycleAction::Start
                && AccountMarketSurface::parse(command.provider().as_str()).is_some()
            {
                Box::pin(
                    self.execute_account_group_start(
                        command,
                        AccountMarketSurface::parse(command.provider().as_str())
                            .ok_or(SourceLifecycleError::InvalidRequest)?,
                        prior_session_id,
                        prior_public_configuration_digest,
                    ),
                )
            } else {
                Box::pin(self.execute_live(
                    command,
                    prior_session_id,
                    prior_public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                ))
            };
            execution.await
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
        match self.durable.complete_source_lifecycle_transition(
            &provider,
            transition_digest,
            transition_intent,
            outcome.phase,
            outcome.session_id,
            outcome.public_configuration_digest,
            outcome.runtime_verification_receipt_digest,
            outcome.credential_generation,
        ) {
            Ok(_record) => {}
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
        }
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
            if let Err(error) = self.durable.record_source_lifecycle_reads_admitted(
                &provider,
                transition_digest,
                transition_intent,
                DurableSourceRuntimeGeneration::AccountGroup(group_generation.digest()),
            ) {
                let cleanup = self.cleanup_account_group(request, group_generation).await;
                let _blocked = self
                    .durable
                    .require_completed_source_lifecycle_reconciliation(
                        &provider,
                        transition_digest,
                    );
                return Err(match cleanup {
                    Ok(()) => map_durable_error(error),
                    Err(cleanup_error) => cleanup_error,
                });
            }
        }
        let record = self
            .durable
            .confirm_source_lifecycle_transition(&provider, transition_digest, transition_intent)
            .map_err(map_durable_error)?;
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
            current_generation: live.and_then(MarketSourceRuntimeGeneration::connection_generation),
            runtime_generation_digest: account_group_generation
                .or_else(|| live.and_then(MarketSourceRuntimeGeneration::runtime_generation_digest))
                .or(research),
            public_configuration_digest,
            doctor,
            start_eligibility,
            blocker,
            observed_at,
        })
    }

    async fn execute_alpaca_verify(
        &self,
        command: &SourceLifecycleCommand,
        transition_digest: EvidenceDigest,
        target_session_id: Option<uuid::Uuid>,
        target_public_configuration_digest: Option<EvidenceDigest>,
        prior_phase: DurableSourceLifecyclePhase,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
        prior_runtime_verification_receipt_digest: Option<EvidenceDigest>,
        prior_credential_generation: Option<market_squawk_platform::SecretGeneration>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        if command.action() != SourceLifecycleAction::Verify
            || command.provider().as_str() != ProviderMarketAccount::AlpacaBasic.surface_id()
        {
            return Err(SourceLifecycleError::InvalidRequest);
        }
        let session_id = target_session_id.ok_or(SourceLifecycleError::InvalidRequest)?;
        let public_configuration_digest =
            target_public_configuration_digest.ok_or(SourceLifecycleError::InvalidRequest)?;
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
        let verification: Pin<
            Box<
                dyn Future<
                        Output = Result<
                            crate::ProviderActivationLease,
                            crate::ProviderOnboardingError,
                        >,
                    > + Send
                    + '_,
            >,
        > = Box::pin(
            self.onboarding
                .verify_runtime_activation_target(session_id, command.cancellation().child_token()),
        );
        let lease = verification.await.map_err(map_onboarding_error)?;
        if lease.surface_id() != command.provider()
            || lease.session_id() != session_id
            || lease.public_configuration_digest() != public_configuration_digest
        {
            return Err(SourceLifecycleError::Conflict);
        }
        let credential_generation = lease
            .generation()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        self.durable
            .bind_source_lifecycle_verification(
                command.provider().as_str(),
                transition_digest,
                DurableSourceLifecycleIntent::VerifyStop,
                lease.runtime_evidence_digest(),
                credential_generation,
            )
            .map_err(map_durable_error)?;
        if let Some(request) = prior_request {
            let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
            let cleanup = CancellationToken::new();
            self.live
                .stop_account_group(request, None, deadline, &cleanup)
                .await
                .map_err(map_live_error)?;
        }
        LifecycleOutcome::stopped_with_runtime_verification(&lease)
    }

    async fn execute_account_group_start(
        &self,
        command: &SourceLifecycleCommand,
        surface: AccountMarketSurface,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        if command.action() != SourceLifecycleAction::Start
            || command.provider().as_str() != surface.surface_id()
        {
            return Err(SourceLifecycleError::InvalidRequest);
        }
        let lease = match self.optional_exact_lease(command)? {
            Some(lease) => lease,
            None => prior_session_id
                .and_then(|session_id| {
                    self.onboarding
                        .activation_lease(session_id)
                        .or_else(|_| self.onboarding.prepared_activation_lease(session_id))
                        .ok()
                })
                .ok_or(SourceLifecycleError::Unauthorized)?,
        };
        if lease.surface_id() != command.provider()
            || (prior_session_id.is_some() || prior_public_configuration_digest.is_some())
                && (prior_session_id != Some(lease.session_id())
                    || prior_public_configuration_digest
                        != Some(lease.public_configuration_digest()))
        {
            return Err(SourceLifecycleError::Conflict);
        }
        let request = account_group_request_from_binding(
            surface,
            Some(lease.session_id()),
            Some(lease.public_configuration_digest()),
            Some(&lease),
        )?;
        let startup: Pin<
            Box<
                dyn Future<
                        Output = Result<
                            MarketProviderGroupLifecycleEvidence,
                            market_squawk_services::ServiceError,
                        >,
                    > + Send
                    + '_,
            >,
        > = Box::pin(self.live.start_account_group(
            request,
            command.deadline(),
            command.cancellation(),
        ));
        let evidence = startup.await.map_err(map_live_error)?;
        let group_generation = validate_account_group_evidence(request, &evidence)?;
        let mut outcome = LifecycleOutcome::active_account(
            Some(lease.session_id()),
            Some(lease.public_configuration_digest()),
            None,
            request,
            group_generation,
        );
        outcome.bind_runtime_verification(&lease)?;
        Ok(outcome)
    }

    async fn execute_live(
        &self,
        command: &SourceLifecycleCommand,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
        prior_runtime_verification_receipt_digest: Option<EvidenceDigest>,
        prior_credential_generation: Option<market_squawk_platform::SecretGeneration>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
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
                self.live
                    .start(
                        command.provider(),
                        session_id,
                        command.deadline(),
                        command.cancellation(),
                    )
                    .await
                    .map_err(map_live_error)?;
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
                        expected_market_runtime_generation(command)?,
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
                let expected = match expected_market_runtime_generation(command)? {
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
        stop_account_group_exact_live(
            &self.live,
            request,
            require_present,
            command.expected_runtime_generation_digest(),
            command.deadline(),
            command.cancellation(),
        )
        .await
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
        previous_generation: Option<MarketSourceRuntimeGeneration>,
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
        } else if let Some(generation) = live
            .as_ref()
            .and_then(|evidence| evidence.generation.runtime_generation_digest())
        {
            Some(generation)
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
            previous_generation: previous_generation
                .and_then(MarketSourceRuntimeGeneration::connection_generation),
            current_generation: live
                .as_ref()
                .and_then(|evidence| evidence.generation.connection_generation()),
            runtime_generation_digest,
            coverage: live.as_ref().and_then(|evidence| {
                evidence
                    .generation
                    .connection_generation()
                    .map(|_| evidence.coverage)
            }),
            integrity: live.as_ref().and_then(|evidence| {
                evidence
                    .generation
                    .connection_generation()
                    .map(|_| evidence.integrity)
            }),
            quality: live.as_ref().and_then(|evidence| {
                evidence
                    .generation
                    .connection_generation()
                    .map(|_| evidence.quality)
            }),
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
        if command.action() == SourceLifecycleAction::Reconfigure {
            return Ok((
                Some(
                    command
                        .onboarding_session_id()
                        .ok_or(SourceLifecycleError::InvalidRequest)?,
                ),
                Some(
                    command
                        .public_configuration_digest()
                        .ok_or(SourceLifecycleError::InvalidRequest)?,
                ),
            ));
        }
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

#[derive(Clone, Copy)]
enum AccountStopPredecessor {
    AccountGroup(MarketRuntimeGroupGeneration),
    StoppedRuntimeAbsent,
    DesiredActiveRuntimeAbsent,
}

fn account_stop_predecessor(
    record: &DurableSourceLifecycleRecord,
) -> Result<AccountStopPredecessor, SourceLifecycleError> {
    let pending = record
        .pending_view()
        .ok_or(SourceLifecycleError::ReconciliationRequired)?;
    if pending.intent() != DurableSourceLifecycleIntent::Stop {
        return Err(SourceLifecycleError::ReconciliationRequired);
    }
    let predecessor = pending.predecessor();
    match (predecessor.phase(), predecessor.runtime_generation()) {
        (
            DurableSourceLifecyclePhase::Active,
            Some(DurableSourceRuntimeGeneration::AccountGroup(digest)),
        ) if pending.expected_runtime_generation()
            == Some(DurableSourceRuntimeGeneration::AccountGroup(digest)) =>
        {
            MarketRuntimeGroupGeneration::try_from_expected_digest(digest)
                .map(AccountStopPredecessor::AccountGroup)
                .map_err(map_live_error)
        }
        (DurableSourceLifecyclePhase::Stopped, None)
            if pending.expected_runtime_generation().is_none() =>
        {
            Ok(AccountStopPredecessor::StoppedRuntimeAbsent)
        }
        (DurableSourceLifecyclePhase::Active, None)
            if pending.expected_runtime_generation().is_none() =>
        {
            Ok(AccountStopPredecessor::DesiredActiveRuntimeAbsent)
        }
        _ => Err(SourceLifecycleError::InvalidResult),
    }
}

async fn drive_desired_active_absent_account_stop(
    durable: &DurableProviderActivationState,
    live: &Arc<MarketRuntimeRegistry>,
    surface_id: &str,
    transition_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
    loop {
        let current = durable
            .source_lifecycle_record(surface_id)
            .map_err(map_durable_error)?;
        if !matches!(
            account_stop_predecessor(&current)?,
            AccountStopPredecessor::DesiredActiveRuntimeAbsent
        ) {
            return Err(SourceLifecycleError::InvalidResult);
        }
        let pending = current
            .pending_view()
            .ok_or(SourceLifecycleError::ReconciliationRequired)?;
        let predecessor = pending.predecessor();
        match pending.checkpoint() {
            DurableSourceLifecycleCheckpoint::Planned => {
                durable
                    .bind_source_lifecycle_verification(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        predecessor
                            .runtime_verification_receipt_digest()
                            .ok_or(SourceLifecycleError::InvalidResult)?,
                        predecessor
                            .credential_generation()
                            .ok_or(SourceLifecycleError::InvalidResult)?,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::VerificationBound => {
                let surface = AccountMarketSurface::parse(surface_id)
                    .ok_or(SourceLifecycleError::InvalidResult)?;
                let request = account_group_request_from_values(
                    surface,
                    predecessor.session_id(),
                    predecessor.public_configuration_digest(),
                    predecessor.runtime_verification_receipt_digest(),
                    predecessor.credential_generation(),
                )?;
                if !matches!(
                    live.account_group_stop_state(request, deadline, cancellation)
                        .await
                        .map_err(map_live_error)?,
                    AccountGroupStopState::Absent
                ) {
                    return Err(SourceLifecycleError::Conflict);
                }
                let proof = source_lifecycle_runtime_absent_proof_digest(transition_digest)
                    .map_err(map_durable_error)?;
                durable
                    .record_source_lifecycle_runtime_proven_absent(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        proof,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::RuntimeDrained => {
                let target = pending.target();
                durable
                    .complete_source_lifecycle_transition(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        target.phase(),
                        target.session_id(),
                        target.public_configuration_digest(),
                        target.runtime_verification_receipt_digest(),
                        target.credential_generation(),
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::TerminalPublished => {
                return durable
                    .confirm_source_lifecycle_transition(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                    )
                    .map_err(map_durable_error);
            }
            DurableSourceLifecycleCheckpoint::ShutdownKeyPersisted
            | DurableSourceLifecycleCheckpoint::AccountStopping
            | DurableSourceLifecycleCheckpoint::PortalCancelled
            | DurableSourceLifecycleCheckpoint::TombstoneAcknowledged
            | DurableSourceLifecycleCheckpoint::SuccessorStarted
            | DurableSourceLifecycleCheckpoint::SuccessorDurable
            | DurableSourceLifecycleCheckpoint::ReadsAdmitted => {
                return Err(SourceLifecycleError::InvalidResult);
            }
        }
    }
}

struct DurableAccountGroupStopContext {
    request: PreparedMarketProviderConfigurationRequest,
    expected_generation: MarketRuntimeGroupGeneration,
    shutdown_key: Option<DurableAccountShutdownKey>,
    checkpoint: DurableSourceLifecycleCheckpoint,
}

fn durable_account_group_stop_context(
    surface_id: &str,
    record: &DurableSourceLifecycleRecord,
) -> Result<DurableAccountGroupStopContext, SourceLifecycleError> {
    let pending = record
        .pending_view()
        .ok_or(SourceLifecycleError::ReconciliationRequired)?;
    if pending.intent() != DurableSourceLifecycleIntent::Stop {
        return Err(SourceLifecycleError::ReconciliationRequired);
    }
    let predecessor = pending.predecessor();
    if predecessor.phase() != DurableSourceLifecyclePhase::Active {
        return Err(SourceLifecycleError::InvalidResult);
    }
    let predecessor_generation = match predecessor.runtime_generation() {
        Some(DurableSourceRuntimeGeneration::AccountGroup(digest)) => digest,
        _ => return Err(SourceLifecycleError::InvalidResult),
    };
    if pending.expected_runtime_generation()
        != Some(DurableSourceRuntimeGeneration::AccountGroup(
            predecessor_generation,
        ))
    {
        return Err(SourceLifecycleError::InvalidResult);
    }
    let surface =
        AccountMarketSurface::parse(surface_id).ok_or(SourceLifecycleError::InvalidResult)?;
    let request = account_group_request_from_values(
        surface,
        predecessor.session_id(),
        predecessor.public_configuration_digest(),
        predecessor.runtime_verification_receipt_digest(),
        predecessor.credential_generation(),
    )?;
    let expected_generation =
        MarketRuntimeGroupGeneration::try_from_expected_digest(predecessor_generation)
            .map_err(map_live_error)?;
    Ok(DurableAccountGroupStopContext {
        request,
        expected_generation,
        shutdown_key: pending.shutdown_key(),
        checkpoint: pending.checkpoint(),
    })
}

fn durable_account_shutdown_key_from_market(
    evidence: AccountGroupStopKeyEvidence,
) -> Result<DurableAccountShutdownKey, SourceLifecycleError> {
    let history = match evidence.history() {
        AccountGroupStopHistoryEvidence::AlpacaNeverClaimed => {
            DurableAccountHistoryClaim::AlpacaNeverClaimed
        }
        AccountGroupStopHistoryEvidence::Alpaca {
            parent_group_generation,
            parent_binding_digest,
        } => DurableAccountHistoryClaim::Alpaca(
            DurableAlpacaHistoricalParent::try_new(parent_group_generation, parent_binding_digest)
                .map_err(map_durable_error)?,
        ),
        AccountGroupStopHistoryEvidence::NeverApplicable => {
            DurableAccountHistoryClaim::NeverApplicable
        }
    };
    DurableAccountShutdownKey::try_new(
        evidence.registry_incarnation(),
        evidence.surface(),
        evidence.onboarding_session_id(),
        evidence.public_configuration_digest(),
        evidence.runtime_verification_receipt_digest(),
        evidence.credential_generation(),
        evidence.group_generation(),
        history,
    )
    .map_err(map_durable_error)
}

fn market_account_stop_key_from_durable(
    key: DurableAccountShutdownKey,
) -> Result<AccountGroupStopKeyEvidence, SourceLifecycleError> {
    let history = match key.history_claim() {
        DurableAccountHistoryClaim::AlpacaNeverClaimed => {
            AccountGroupStopHistoryEvidence::AlpacaNeverClaimed
        }
        DurableAccountHistoryClaim::Alpaca(parent) => AccountGroupStopHistoryEvidence::Alpaca {
            parent_group_generation: parent.group_generation(),
            parent_binding_digest: parent.binding_digest(),
        },
        DurableAccountHistoryClaim::NeverApplicable => {
            AccountGroupStopHistoryEvidence::NeverApplicable
        }
    };
    AccountGroupStopKeyEvidence::try_new(
        key.registry_incarnation(),
        key.surface_id(),
        key.onboarding_session_id(),
        key.public_configuration_digest(),
        key.runtime_verification_receipt_digest(),
        key.credential_generation(),
        key.group_generation(),
        history,
    )
    .map_err(map_live_error)
}

async fn exact_account_group_stop_ticket(
    live: &Arc<MarketRuntimeRegistry>,
    request: PreparedMarketProviderConfigurationRequest,
    expected_generation: MarketRuntimeGroupGeneration,
    shutdown_key: DurableAccountShutdownKey,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<AccountGroupStopTicket>, SourceLifecycleError> {
    let Some(prepared) = live
        .prepare_account_group_stop(request, Some(expected_generation), deadline, cancellation)
        .await
        .map_err(map_live_error)?
    else {
        return Ok(None);
    };
    if durable_account_shutdown_key_from_market(prepared.key_evidence().map_err(map_live_error)?)?
        != shutdown_key
    {
        return Err(SourceLifecycleError::Conflict);
    }
    let ticket = live
        .commit_prepared_account_group_stop(prepared, deadline, cancellation)
        .await
        .map_err(map_live_error)?;
    if durable_account_shutdown_key_from_market(ticket.key_evidence().map_err(map_live_error)?)?
        != shutdown_key
    {
        return Err(SourceLifecycleError::InvalidResult);
    }
    Ok(Some(ticket))
}

async fn exact_account_group_stop_receipt(
    live: &Arc<MarketRuntimeRegistry>,
    request: PreparedMarketProviderConfigurationRequest,
    expected_generation: MarketRuntimeGroupGeneration,
    shutdown_key: DurableAccountShutdownKey,
    terminal_proof_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(AccountGroupStopReceipt, AccountGroupStopDurableProof), SourceLifecycleError> {
    let receipt = match exact_account_group_stop_ticket(
        live,
        request,
        expected_generation,
        shutdown_key,
        deadline,
        cancellation,
    )
    .await?
    {
        Some(ticket) => live
            .join_account_group_stop(&ticket, deadline, cancellation)
            .await
            .map_err(map_live_error)?,
        None => live
            .reacquire_acknowledged_account_group_stop_receipt(
                market_account_stop_key_from_durable(shutdown_key)?,
                terminal_proof_digest,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_live_error)?,
    };
    if durable_account_shutdown_key_from_market(receipt.key_evidence().map_err(map_live_error)?)?
        != shutdown_key
    {
        return Err(SourceLifecycleError::InvalidResult);
    }
    let durable_proof = receipt
        .bind_durable_proof(terminal_proof_digest)
        .map_err(map_live_error)?;
    Ok((receipt, durable_proof))
}

#[derive(Clone, Copy)]
enum AccountGroupPredecessorDriveBoundary {
    ThroughDurableAcknowledgement,
    #[cfg(test)]
    ReturnAfterRegistryAcknowledgement,
}

fn record_account_group_tombstone_acknowledged(
    durable: &DurableProviderActivationState,
    surface_id: &str,
    transition_digest: EvidenceDigest,
    shutdown_key: DurableAccountShutdownKey,
    terminal_proof_digest: EvidenceDigest,
    acknowledgement: AccountGroupStopAcknowledgementReceipt,
) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
    let _disposition = acknowledgement
        .authorize_checkpoint(
            market_account_stop_key_from_durable(shutdown_key)?,
            terminal_proof_digest,
        )
        .map_err(map_live_error)?;
    durable
        .record_source_lifecycle_tombstone_acknowledged(
            surface_id,
            transition_digest,
            DurableSourceLifecycleIntent::Stop,
            shutdown_key,
            terminal_proof_digest,
        )
        .map_err(map_durable_error)
}

async fn drive_account_group_predecessor(
    durable: &DurableProviderActivationState,
    live: &Arc<MarketRuntimeRegistry>,
    surface_id: &str,
    transition_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
    _boundary: AccountGroupPredecessorDriveBoundary,
) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
    loop {
        let current = durable
            .source_lifecycle_record(surface_id)
            .map_err(map_durable_error)?;
        let context = durable_account_group_stop_context(surface_id, &current)?;
        match context.checkpoint {
            DurableSourceLifecycleCheckpoint::Planned => {
                let pending = current
                    .pending_view()
                    .ok_or(SourceLifecycleError::ReconciliationRequired)?;
                let predecessor = pending.predecessor();
                durable
                    .bind_source_lifecycle_verification(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        predecessor
                            .runtime_verification_receipt_digest()
                            .ok_or(SourceLifecycleError::InvalidResult)?,
                        predecessor
                            .credential_generation()
                            .ok_or(SourceLifecycleError::InvalidResult)?,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::VerificationBound => {
                let prepared = live
                    .prepare_account_group_stop(
                        context.request,
                        Some(context.expected_generation),
                        deadline,
                        cancellation,
                    )
                    .await
                    .map_err(map_live_error)?
                    .ok_or(SourceLifecycleError::Unavailable)?;
                let shutdown_key = durable_account_shutdown_key_from_market(
                    prepared.key_evidence().map_err(map_live_error)?,
                )?;
                durable
                    .bind_source_lifecycle_shutdown_key(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        shutdown_key,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::ShutdownKeyPersisted => {
                let shutdown_key = context
                    .shutdown_key
                    .ok_or(SourceLifecycleError::InvalidResult)?;
                exact_account_group_stop_ticket(
                    live,
                    context.request,
                    context.expected_generation,
                    shutdown_key,
                    deadline,
                    cancellation,
                )
                .await?
                .ok_or(SourceLifecycleError::Unavailable)?;
                durable
                    .record_source_lifecycle_account_stopping(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        shutdown_key,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::AccountStopping => {
                let shutdown_key = context
                    .shutdown_key
                    .ok_or(SourceLifecycleError::InvalidResult)?;
                let ticket = exact_account_group_stop_ticket(
                    live,
                    context.request,
                    context.expected_generation,
                    shutdown_key,
                    deadline,
                    cancellation,
                )
                .await?
                .ok_or(SourceLifecycleError::Unavailable)?;
                let receipt = live
                    .join_account_group_stop(&ticket, deadline, cancellation)
                    .await
                    .map_err(map_live_error)?;
                if durable_account_shutdown_key_from_market(
                    receipt.key_evidence().map_err(map_live_error)?,
                )? != shutdown_key
                {
                    return Err(SourceLifecycleError::InvalidResult);
                }
                let terminal_proof_digest =
                    source_lifecycle_account_stop_proof_digest(transition_digest, shutdown_key)
                        .map_err(map_durable_error)?;
                receipt
                    .bind_durable_proof(terminal_proof_digest)
                    .map_err(map_live_error)?;
                durable
                    .record_source_lifecycle_runtime_drained(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        shutdown_key,
                        terminal_proof_digest,
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::RuntimeDrained => {
                let pending = current
                    .pending_view()
                    .ok_or(SourceLifecycleError::ReconciliationRequired)?;
                let target = pending.target();
                durable
                    .complete_source_lifecycle_transition(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                        target.phase(),
                        target.session_id(),
                        target.public_configuration_digest(),
                        target.runtime_verification_receipt_digest(),
                        target.credential_generation(),
                    )
                    .map_err(map_durable_error)?;
            }
            DurableSourceLifecycleCheckpoint::TerminalPublished => {
                let shutdown_key = context
                    .shutdown_key
                    .ok_or(SourceLifecycleError::InvalidResult)?;
                let terminal_proof_digest = current
                    .pending_terminal_proof_digest()
                    .ok_or(SourceLifecycleError::InvalidResult)?;
                let (receipt, durable_proof) = exact_account_group_stop_receipt(
                    live,
                    context.request,
                    context.expected_generation,
                    shutdown_key,
                    terminal_proof_digest,
                    deadline,
                    cancellation,
                )
                .await?;
                let acknowledgement = live
                    .acknowledge_account_group_stop(
                        &receipt,
                        &durable_proof,
                        deadline,
                        cancellation,
                    )
                    .await
                    .map_err(map_live_error)?;
                #[cfg(test)]
                if matches!(
                    _boundary,
                    AccountGroupPredecessorDriveBoundary::ReturnAfterRegistryAcknowledgement
                ) {
                    return Err(SourceLifecycleError::ReconciliationRequired);
                }
                record_account_group_tombstone_acknowledged(
                    durable,
                    surface_id,
                    transition_digest,
                    shutdown_key,
                    terminal_proof_digest,
                    acknowledgement,
                )?;
            }
            DurableSourceLifecycleCheckpoint::TombstoneAcknowledged => {
                return durable
                    .confirm_source_lifecycle_transition(
                        surface_id,
                        transition_digest,
                        DurableSourceLifecycleIntent::Stop,
                    )
                    .map_err(map_durable_error);
            }
            DurableSourceLifecycleCheckpoint::PortalCancelled
            | DurableSourceLifecycleCheckpoint::SuccessorStarted
            | DurableSourceLifecycleCheckpoint::SuccessorDurable
            | DurableSourceLifecycleCheckpoint::ReadsAdmitted => {
                return Err(SourceLifecycleError::InvalidResult);
            }
        }
    }
}

async fn stop_account_group_exact_live(
    live: &Arc<MarketRuntimeRegistry>,
    request: PreparedMarketProviderConfigurationRequest,
    require_present: bool,
    expected_runtime_generation_digest: Option<EvidenceDigest>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<MarketRuntimeGroupGeneration>, SourceLifecycleError> {
    let expected = match live
        .account_group_stop_state(request, deadline, cancellation)
        .await
        .map_err(map_live_error)?
    {
        AccountGroupStopState::Absent => None,
        AccountGroupStopState::Active(evidence) => {
            Some(validate_account_group_evidence(request, &evidence)?)
        }
        AccountGroupStopState::Stopping(generation) => Some(generation),
    };
    if require_present && expected.is_none() {
        return Err(SourceLifecycleError::Unavailable);
    }
    if let Some(expected_digest) = expected_runtime_generation_digest
        && expected.map(MarketRuntimeGroupGeneration::digest) != Some(expected_digest)
    {
        return Err(SourceLifecycleError::Conflict);
    }
    let stopped = live
        .stop_account_group(request, expected, deadline, cancellation)
        .await
        .map_err(map_live_error)?;
    if expected.is_some() && stopped != expected {
        return Err(SourceLifecycleError::InvalidResult);
    }
    Ok(stopped)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        time::{Duration, Instant},
    };

    use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
    use market_squawk_platform::{AppConfig, ConfigOverrides, ConfigSources, SecretGeneration};
    use market_squawk_services::ServiceError;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::application::{MarketRuntimeRegistry, active_shutdown_fixture};

    #[tokio::test]
    async fn source_lifecycle_stop_deadline_retains_owner_and_retry_completes_exact_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (historical, parent, successor_parent, publication) = active_shutdown_fixture().await?;
        let request = PreparedMarketProviderConfigurationRequest::try_new(
            AccountMarketSurface::AlpacaBasic,
            uuid::Uuid::new_v4(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [71; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [72; 32]),
            SecretGeneration::new(73)?,
        )?;
        let generation = parent.group_generation();
        let (registry, probe) =
            MarketRuntimeRegistry::shutdown_fixture(historical, parent, request)?;
        let durable_temporary = tempfile::tempdir()?;
        let durable = DurableProviderActivationState::new(durable_temporary.path().to_path_buf());
        let composition_temporary = tempfile::tempdir()?;
        let composition = crate::LocalProduct::try_new(AppConfig::load(ConfigSources::new(
            None,
            &BTreeMap::<OsString, OsString>::new(),
            ConfigOverrides {
                data_dir: Some(composition_temporary.path().join("data")),
                ..ConfigOverrides::default()
            },
        ))?)?;
        let authority = ProductionSourceLifecycleAuthority::new(
            composition.paths().clone(),
            composition.provider_onboarding(),
            composition.provider_activation(),
            composition.provider_portal_activation(),
            durable.clone(),
            Arc::clone(&registry),
        );
        let durable_generation =
            DurableSourceRuntimeGeneration::account_group(generation.digest())?;
        let start = durable.begin_source_lifecycle_transition(
            request.surface().surface_id(),
            NonZeroU64::MIN,
            SourceIdentifier::try_from("durable-account-start")?,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
            DurableSourceLifecycleIntent::Start,
            Some(request.onboarding_session_id()),
            Some(request.expected_public_configuration_digest()),
            None,
        )?;
        let start_digest = start.transition_digest()?;
        durable.bind_source_lifecycle_verification(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
            request.expected_runtime_verification_receipt_digest(),
            request.expected_credential_generation(),
        )?;
        durable.bind_source_lifecycle_target_generation(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
            durable_generation,
        )?;
        durable.record_source_lifecycle_successor_durable(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
            durable_generation,
        )?;
        durable.complete_source_lifecycle_transition(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
            DurableSourceLifecyclePhase::Active,
            Some(request.onboarding_session_id()),
            Some(request.expected_public_configuration_digest()),
            Some(request.expected_runtime_verification_receipt_digest()),
            Some(request.expected_credential_generation()),
        )?;
        durable.record_source_lifecycle_reads_admitted(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
            durable_generation,
        )?;
        let active = durable.confirm_source_lifecycle_transition(
            request.surface().surface_id(),
            start_digest,
            DurableSourceLifecycleIntent::Start,
        )?;
        assert!(probe.reads_are_admitted());
        assert!(
            registry
                .verify_account_group(
                    request,
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await?
                .is_some()
        );

        let short_cancellation = CancellationToken::new();
        let stop_command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider: SourceIdentifier::try_from(request.surface().surface_id())?,
            action: SourceLifecycleAction::Stop,
            expected_state_revision: active.revision(),
            expected_generation: None,
            expected_runtime_generation_digest: Some(generation.digest()),
            onboarding_session_id: None,
            public_configuration_digest: None,
            reason: Some(SourceIdentifier::try_from("durable-account-stop")?),
            cancellation: short_cancellation,
            deadline: Instant::now() + Duration::from_millis(100),
        })?;
        let stop_operation_id = operation_id(command_digest(&stop_command)?)?;
        let short = authority.execute(stop_command).await;
        assert!(matches!(short, Err(SourceLifecycleError::DeadlineExceeded)));
        let interrupted = durable.source_lifecycle_record(request.surface().surface_id())?;
        let stop_digest = interrupted
            .pending_transition_digest()
            .ok_or(ServiceError::NotFound)?;
        assert_eq!(
            interrupted.pending_checkpoint(),
            Some(
                crate::local_product::provider_activation_state::DurableSourceLifecycleCheckpoint::AccountStopping
            )
        );
        assert!(interrupted.pending_shutdown_key().is_some());
        assert!(registry.is_exact_account_stopping_for_test(request).await);
        assert_eq!(registry.active_source_count()?, 0);
        assert!(probe.credentials_are_owned());
        assert_eq!(probe.credential_destructions(), 0);
        assert_eq!(probe.display_destructions(), 0);
        assert!(!probe.reads_are_admitted());
        assert!(!probe.try_readmit());

        assert_eq!(
            interrupted.phase(),
            DurableSourceLifecyclePhase::ReconciliationRequired
        );
        assert_eq!(
            interrupted
                .pending_operation_id()
                .map(SourceIdentifier::as_str),
            Some(stop_operation_id.as_str())
        );
        drop(durable);
        let durable = DurableProviderActivationState::new(durable_temporary.path().to_path_buf());
        drop(authority);
        let authority = ProductionSourceLifecycleAuthority::new(
            composition.paths().clone(),
            composition.provider_onboarding(),
            composition.provider_activation(),
            composition.provider_portal_activation(),
            durable.clone(),
            Arc::clone(&registry),
        );
        let reconciliation = durable.source_lifecycle_record(request.surface().surface_id())?;
        let resumed = durable.resume_source_lifecycle_transition(
            request.surface().surface_id(),
            reconciliation.revision(),
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
        )?;
        assert_eq!(
            resumed
                .record()
                .pending_operation_id()
                .map(|value| value.as_str()),
            Some(stop_operation_id.as_str())
        );
        assert_eq!(
            resumed.record().pending_intent(),
            Some(DurableSourceLifecycleIntent::Stop)
        );
        let successor_generation = successor_parent.group_generation();
        assert_ne!(successor_generation, generation);
        assert!(matches!(
            registry
                .admit_shutdown_fixture_successor(
                    successor_parent,
                    request,
                    Instant::now() + Duration::from_secs(2),
                )
                .await,
            Err(ServiceError::ResourceExhausted)
        ));

        let join_cancellation = CancellationToken::new();
        let shutdown_key = resumed
            .record()
            .pending_shutdown_key()
            .ok_or(ServiceError::NotFound)?;
        assert_eq!(shutdown_key.group_generation(), generation.digest());
        let first_ticket = exact_account_group_stop_ticket(
            &registry,
            request,
            generation,
            shutdown_key,
            Instant::now() + Duration::from_secs(2),
            &join_cancellation,
        )
        .await?
        .ok_or(ServiceError::NotFound)?;
        let second_ticket = exact_account_group_stop_ticket(
            &registry,
            request,
            generation,
            shutdown_key,
            Instant::now() + Duration::from_secs(2),
            &join_cancellation,
        )
        .await?
        .ok_or(ServiceError::NotFound)?;
        drop(publication);
        let acknowledgement_gap = drive_account_group_predecessor(
            &durable,
            &registry,
            request.surface().surface_id(),
            stop_digest,
            Instant::now() + Duration::from_secs(2),
            &join_cancellation,
            AccountGroupPredecessorDriveBoundary::ReturnAfterRegistryAcknowledgement,
        )
        .await;
        assert!(matches!(
            acknowledgement_gap,
            Err(SourceLifecycleError::ReconciliationRequired)
        ));
        let acknowledgement_gap =
            durable.source_lifecycle_record(request.surface().surface_id())?;
        assert_eq!(
            acknowledgement_gap.pending_checkpoint(),
            Some(DurableSourceLifecycleCheckpoint::TerminalPublished)
        );
        let terminal_proof_digest = acknowledgement_gap
            .pending_terminal_proof_digest()
            .ok_or(ServiceError::NotFound)?;
        assert!(!registry.is_exact_account_stopping_for_test(request).await);

        let join_deadline = Instant::now() + Duration::from_secs(2);
        let (first_receipt, second_receipt) = tokio::join!(
            registry.join_account_group_stop(&first_ticket, join_deadline, &join_cancellation),
            registry.join_account_group_stop(&second_ticket, join_deadline, &join_cancellation),
        );
        let first_receipt = first_receipt?;
        let second_receipt = second_receipt?;
        let first_durable_proof = first_receipt.bind_durable_proof(terminal_proof_digest)?;
        let second_durable_proof = second_receipt.bind_durable_proof(terminal_proof_digest)?;
        let acknowledge_deadline = Instant::now() + Duration::from_secs(2);
        let (first_acknowledgement, second_acknowledgement) = tokio::join!(
            registry.acknowledge_account_group_stop(
                &first_receipt,
                &first_durable_proof,
                acknowledge_deadline,
                &join_cancellation,
            ),
            registry.acknowledge_account_group_stop(
                &second_receipt,
                &second_durable_proof,
                acknowledge_deadline,
                &join_cancellation,
            ),
        );
        let _first_acknowledgement = first_acknowledgement?;
        let _second_acknowledgement = second_acknowledgement?;

        let exact_key_evidence = market_account_stop_key_from_durable(shutdown_key)?;
        let wrong_proof = EvidenceDigest::new(DigestAlgorithm::Sha256, [77; 32]);
        assert!(matches!(
            registry
                .reacquire_acknowledged_account_group_stop_receipt(
                    exact_key_evidence,
                    wrong_proof,
                    Instant::now() + Duration::from_secs(2),
                    &join_cancellation,
                )
                .await,
            Err(ServiceError::NotFound)
        ));
        let old_incarnation_key = AccountGroupStopKeyEvidence::try_new(
            uuid::Uuid::new_v4(),
            exact_key_evidence.surface(),
            exact_key_evidence.onboarding_session_id(),
            exact_key_evidence.public_configuration_digest(),
            exact_key_evidence.runtime_verification_receipt_digest(),
            exact_key_evidence.credential_generation(),
            exact_key_evidence.group_generation(),
            exact_key_evidence.history(),
        )?;
        assert!(matches!(
            registry
                .reacquire_acknowledged_account_group_stop_receipt(
                    old_incarnation_key,
                    terminal_proof_digest,
                    Instant::now() + Duration::from_secs(2),
                    &join_cancellation,
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        let wrong_key = AccountGroupStopKeyEvidence::try_new(
            exact_key_evidence.registry_incarnation(),
            exact_key_evidence.surface(),
            exact_key_evidence.onboarding_session_id(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [78; 32]),
            exact_key_evidence.runtime_verification_receipt_digest(),
            exact_key_evidence.credential_generation(),
            exact_key_evidence.group_generation(),
            exact_key_evidence.history(),
        )?;
        assert!(matches!(
            registry
                .reacquire_acknowledged_account_group_stop_receipt(
                    wrong_key,
                    terminal_proof_digest,
                    Instant::now() + Duration::from_secs(2),
                    &join_cancellation,
                )
                .await,
            Err(ServiceError::NotFound)
        ));

        let reconciliation = durable
            .require_source_lifecycle_reconciliation(request.surface().surface_id(), stop_digest)?;
        let resumed = durable.resume_source_lifecycle_transition(
            request.surface().surface_id(),
            reconciliation.revision(),
            stop_digest,
            DurableSourceLifecycleIntent::Stop,
        )?;
        assert_eq!(
            resumed
                .record()
                .pending_operation_id()
                .map(|value| value.as_str()),
            Some(stop_operation_id.as_str())
        );
        assert_eq!(
            resumed
                .record()
                .pending_view()
                .and_then(|pending| pending.predecessor().runtime_generation()),
            Some(durable_generation)
        );
        let retry = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider: SourceIdentifier::try_from(request.surface().surface_id())?,
            action: SourceLifecycleAction::Retry,
            expected_state_revision: resumed.record().revision(),
            expected_generation: None,
            expected_runtime_generation_digest: None,
            onboarding_session_id: None,
            public_configuration_digest: None,
            reason: Some(SourceIdentifier::try_from("retry-durable-account-stop")?),
            cancellation: join_cancellation.child_token(),
            deadline: Instant::now() + Duration::from_secs(2),
        })?;
        let completed_receipt = authority.execute(retry).await.map_err(|error| {
            std::io::Error::other(format!("public Retry failed before completion: {error:?}"))
        })?;
        assert_eq!(completed_receipt.fields().operation_id, stop_operation_id);
        assert_eq!(
            completed_receipt.fields().action,
            SourceLifecycleAction::Retry
        );
        assert_eq!(
            completed_receipt.fields().state,
            SourceLifecycleState::Stopped
        );
        let completed = durable.source_lifecycle_record(request.surface().surface_id())?;
        assert_eq!(completed.phase(), DurableSourceLifecyclePhase::Stopped);
        assert_eq!(
            completed.operation_id().map(|value| value.as_str()),
            Some(stop_operation_id.as_str())
        );
        assert!(completed.pending_checkpoint().is_none());
        assert!(!registry.is_exact_account_stopping_for_test(request).await);
        assert!(!probe.credentials_are_owned());
        assert_eq!(probe.credential_destructions(), 1);
        assert_eq!(probe.display_destructions(), 1);
        assert!(!probe.try_readmit());

        let no_effect_stop = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider: SourceIdentifier::try_from(request.surface().surface_id())?,
            action: SourceLifecycleAction::Stop,
            expected_state_revision: completed.revision(),
            expected_generation: None,
            expected_runtime_generation_digest: None,
            onboarding_session_id: None,
            public_configuration_digest: None,
            reason: Some(SourceIdentifier::try_from("stop-already-stopped-account")?),
            cancellation: CancellationToken::new(),
            deadline: Instant::now() + Duration::from_secs(2),
        })?;
        let no_effect_receipt = authority.execute(no_effect_stop).await.map_err(|error| {
            std::io::Error::other(format!("public Stop from Stopped failed: {error:?}"))
        })?;
        assert_eq!(
            no_effect_receipt.fields().state,
            SourceLifecycleState::Stopped
        );
        assert_eq!(
            durable
                .source_lifecycle_record(request.surface().surface_id())?
                .phase(),
            DurableSourceLifecyclePhase::Stopped
        );

        let retained_no_effect_stop =
            SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                provider: SourceIdentifier::try_from(request.surface().surface_id())?,
                action: SourceLifecycleAction::Stop,
                expected_state_revision: no_effect_receipt.fields().state_revision,
                expected_generation: None,
                expected_runtime_generation_digest: None,
                onboarding_session_id: None,
                public_configuration_digest: None,
                reason: Some(SourceIdentifier::try_from("resume-stopped-account-stop")?),
                cancellation: CancellationToken::new(),
                deadline: Instant::now() + Duration::from_secs(2),
            })?;
        let retained_no_effect_operation = operation_id(command_digest(&retained_no_effect_stop)?)?;
        let retained_no_effect = durable.begin_source_lifecycle_transition(
            request.surface().surface_id(),
            retained_no_effect_stop.expected_state_revision(),
            retained_no_effect_operation.clone(),
            command_digest(&retained_no_effect_stop)?,
            DurableSourceLifecycleIntent::Stop,
            Some(request.onboarding_session_id()),
            Some(request.expected_public_configuration_digest()),
            None,
        )?;
        let retained_no_effect_digest = retained_no_effect.transition_digest()?;
        let retained_no_effect = durable.require_source_lifecycle_reconciliation(
            request.surface().surface_id(),
            retained_no_effect_digest,
        )?;
        drop(authority);
        drop(durable);
        let durable = DurableProviderActivationState::new(durable_temporary.path().to_path_buf());
        let authority = ProductionSourceLifecycleAuthority::new(
            composition.paths().clone(),
            composition.provider_onboarding(),
            composition.provider_activation(),
            composition.provider_portal_activation(),
            durable.clone(),
            Arc::clone(&registry),
        );
        let retained_no_effect_retry =
            SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                provider: SourceIdentifier::try_from(request.surface().surface_id())?,
                action: SourceLifecycleAction::Retry,
                expected_state_revision: retained_no_effect.revision(),
                expected_generation: None,
                expected_runtime_generation_digest: None,
                onboarding_session_id: None,
                public_configuration_digest: None,
                reason: Some(SourceIdentifier::try_from("retry-stopped-account-stop")?),
                cancellation: CancellationToken::new(),
                deadline: Instant::now() + Duration::from_secs(2),
            })?;
        let retained_no_effect_receipt = authority
            .execute(retained_no_effect_retry)
            .await
            .map_err(|error| {
                std::io::Error::other(format!(
                    "public Retry from retained Stopped Stop failed: {error:?}"
                ))
            })?;
        assert_eq!(
            retained_no_effect_receipt.fields().operation_id,
            retained_no_effect_operation
        );
        assert_eq!(
            retained_no_effect_receipt.fields().action,
            SourceLifecycleAction::Retry
        );
        assert_eq!(
            retained_no_effect_receipt.fields().state,
            SourceLifecycleState::Stopped
        );
        let retained_no_effect_completed =
            durable.source_lifecycle_record(request.surface().surface_id())?;
        assert_eq!(
            retained_no_effect_completed.phase(),
            DurableSourceLifecyclePhase::Stopped
        );
        assert_eq!(
            retained_no_effect_completed
                .operation_id()
                .map(SourceIdentifier::as_str),
            Some(retained_no_effect_operation.as_str())
        );

        let successor_probe = registry
            .admit_shutdown_fixture_successor(
                successor_parent,
                request,
                Instant::now() + Duration::from_secs(2),
            )
            .await?;
        assert!(successor_probe.reads_are_admitted());
        let successor_evidence = registry
            .verify_account_group(
                request,
                Instant::now() + Duration::from_secs(2),
                &CancellationToken::new(),
            )
            .await?
            .ok_or(ServiceError::Unavailable)?;
        assert_eq!(successor_evidence.group_generation(), successor_generation);
        assert!(matches!(
            registry
                .stop_account_group(
                    request,
                    Some(generation),
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        assert!(matches!(
            registry
                .reacquire_acknowledged_account_group_stop_receipt(
                    exact_key_evidence,
                    terminal_proof_digest,
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        assert!(matches!(
            registry
                .acknowledge_account_group_stop(
                    &first_receipt,
                    &first_durable_proof,
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await,
            Err(ServiceError::InvalidRequest)
        ));
        assert_eq!(
            registry
                .verify_account_group(
                    request,
                    Instant::now() + Duration::from_secs(2),
                    &CancellationToken::new(),
                )
                .await?
                .ok_or(ServiceError::Unavailable)?
                .group_generation(),
            successor_generation
        );
        assert!(!probe.try_readmit());

        let drop_publication = registry
            .hold_shutdown_fixture_historical_publication(
                successor_parent,
                request,
                Instant::now() + Duration::from_secs(2),
            )
            .await?;
        assert!(drop_publication.validate_precommit());
        drop(authority);
        drop(registry);
        assert!(!drop_publication.validate_precommit());
        assert!(
            composition
                .application()
                .shutdown(Instant::now() + Duration::from_secs(5))
                .await
                .is_complete()
        );
        Ok(())
    }
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
        let execution: Pin<
            Box<
                dyn Future<Output = Result<SourceLifecycleReceipt, SourceLifecycleError>>
                    + Send
                    + '_,
            >,
        > = Box::pin(self.execute_owned(&command));
        execution.await
    }
}

struct LifecycleOutcome {
    phase: DurableSourceLifecyclePhase,
    session_id: Option<uuid::Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    runtime_verification_receipt_digest: Option<EvidenceDigest>,
    credential_generation: Option<market_squawk_platform::SecretGeneration>,
    runtime_generation: Option<DurableSourceRuntimeGeneration>,
    previous_generation: Option<MarketSourceRuntimeGeneration>,
    account_group_read_admission: Option<(
        PreparedMarketProviderConfigurationRequest,
        MarketRuntimeGroupGeneration,
    )>,
}

impl LifecycleOutcome {
    const fn active(
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        previous_generation: Option<MarketSourceRuntimeGeneration>,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Active,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: None,
            previous_generation,
            account_group_read_admission: None,
        }
    }

    const fn active_account(
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        previous_generation: Option<MarketSourceRuntimeGeneration>,
        request: PreparedMarketProviderConfigurationRequest,
        group_generation: MarketRuntimeGroupGeneration,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Active,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: Some(DurableSourceRuntimeGeneration::AccountGroup(
                group_generation.digest(),
            )),
            previous_generation,
            account_group_read_admission: Some((request, group_generation)),
        }
    }

    const fn stopped(
        previous_generation: Option<MarketSourceRuntimeGeneration>,
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
    ) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Stopped,
            session_id,
            public_configuration_digest,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: None,
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
            runtime_generation: None,
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
            runtime_generation: None,
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

    const fn removed(previous_generation: Option<MarketSourceRuntimeGeneration>) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Removed,
            session_id: None,
            public_configuration_digest: None,
            runtime_verification_receipt_digest: None,
            credential_generation: None,
            runtime_generation: None,
            previous_generation,
            account_group_read_admission: None,
        }
    }
}

fn durable_intent(
    command: &SourceLifecycleCommand,
) -> Result<DurableSourceLifecycleIntent, SourceLifecycleError> {
    Ok(match command.action() {
        SourceLifecycleAction::Start => DurableSourceLifecycleIntent::Start,
        SourceLifecycleAction::Stop => DurableSourceLifecycleIntent::Stop,
        SourceLifecycleAction::Resynchronize => DurableSourceLifecycleIntent::Resynchronize,
        SourceLifecycleAction::Verify
            if command.provider().as_str() == ProviderMarketAccount::AlpacaBasic.surface_id() =>
        {
            DurableSourceLifecycleIntent::VerifyStop
        }
        SourceLifecycleAction::Verify => DurableSourceLifecycleIntent::Verify,
        SourceLifecycleAction::Reconfigure => DurableSourceLifecycleIntent::Reconfigure,
        SourceLifecycleAction::Remove => DurableSourceLifecycleIntent::Remove,
        SourceLifecycleAction::Retry => return Err(SourceLifecycleError::InvalidRequest),
    })
}

fn expected_durable_runtime_generation(
    command: &SourceLifecycleCommand,
) -> Result<Option<DurableSourceRuntimeGeneration>, SourceLifecycleError> {
    match (
        command.expected_generation(),
        command.expected_runtime_generation_digest(),
    ) {
        (Some(generation), None) => NonZeroU64::new(generation.get())
            .map(DurableSourceRuntimeGeneration::Scalar)
            .map(Some)
            .ok_or(SourceLifecycleError::InvalidRequest),
        (None, Some(digest)) => {
            let generation = if AccountMarketSurface::parse(command.provider().as_str()).is_some() {
                DurableSourceRuntimeGeneration::account_group(digest)
            } else {
                DurableSourceRuntimeGeneration::non_account_digest(digest)
            };
            generation.map(Some).map_err(map_durable_error)
        }
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(SourceLifecycleError::InvalidRequest),
    }
}

fn expected_market_runtime_generation(
    command: &SourceLifecycleCommand,
) -> Result<Option<MarketSourceRuntimeGeneration>, SourceLifecycleError> {
    match (
        command.expected_generation(),
        command.expected_runtime_generation_digest(),
    ) {
        (Some(generation), None) => Ok(Some(MarketSourceRuntimeGeneration::Scalar(generation))),
        (None, Some(digest)) => MarketRuntimeGroupGeneration::try_from_expected_digest(digest)
            .map(MarketSourceRuntimeGeneration::Group)
            .map(Some)
            .map_err(|_| SourceLifecycleError::InvalidRequest),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(SourceLifecycleError::InvalidRequest),
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
