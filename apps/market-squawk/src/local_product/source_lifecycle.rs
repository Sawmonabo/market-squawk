//! Production source lifecycle authority over live and research runtime owners.

mod credential_access;
mod display_history;
mod reconnect;

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
    AccountMarketSurface, MarketProviderGroupLifecycleEvidence, MarketRuntimeGroupGeneration,
    MarketRuntimeRegistry, MarketSourceRuntimeGeneration, PreparedAccountStop,
    PreparedMarketProviderConfigurationRequest,
};
use crate::provider_activation::{FredPointInTimeReadCapability, ProviderMarketAccount};
use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ResearchService,
};

use super::{
    cli_provider,
    provider_activation_state::{
        AccountAllocationCoordinates, AccountLifecycleAction, AccountStopDisposition,
        DurableActivationRecipeState, DurableProviderActivationState,
        DurableProviderActivationStateError, DurableSourceLifecyclePhase,
        DurableSourceLifecycleRecord, DurableSourceLifecycleTransition, PendingAccountLifecycle,
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
    ProviderMarketAccount::KrakenLevel3.surface_id(),
    ProviderMarketAccount::SchwabMarketData.surface_id(),
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
    credential_access: credential_access::CredentialRuntimeAccess,
    paths: LocalPaths,
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<ProviderAdapterActivation>,
    portal: Arc<dyn ProviderPortalActivationAuthority>,
    durable: DurableProviderActivationState,
    research: Arc<ResearchService>,
    live: Arc<MarketRuntimeRegistry>,
    calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
    display_history: crate::application::SourceActionPreparationCapability,
    display_history_requests: display_history::StarterHistorySender,
}

impl ProductionSourceLifecycleAuthority {
    /// Binds the existing runtime owners without constructing another source runtime.
    pub(crate) fn new(
        paths: LocalPaths,
        onboarding: Arc<ProviderOnboardingService>,
        activation: Arc<ProviderAdapterActivation>,
        portal: Arc<dyn ProviderPortalActivationAuthority>,
        durable: DurableProviderActivationState,
        research: Arc<ResearchService>,
        live: Arc<MarketRuntimeRegistry>,
        calendars: crate::application::market_calendar::CompletedMarketSessionReadCapability,
        display_history: crate::application::SourceActionPreparationCapability,
        display_history_requests: display_history::StarterHistorySender,
    ) -> Self {
        Self {
            credential_access: credential_access::CredentialRuntimeAccess::default(),
            paths,
            onboarding,
            activation,
            portal,
            durable,
            research,
            live,
            calendars,
            display_history,
            display_history_requests,
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
            let mut record = match self.durable.source_lifecycle_record(surface) {
                Ok(record) => record,
                Err(error) => {
                    failures.push(LiveSourceRestoreFailure {
                        provider,
                        error: map_durable_error(error),
                    });
                    continue;
                }
            };
            if let Some(account_surface) = AccountMarketSurface::parse(surface)
                && record.account().is_some_and(|pending| !pending.finished)
            {
                let _gate = self
                    .lifecycle_gate_before(surface, deadline, cancellation)
                    .await?;
                match self
                    .continue_account_transition(
                        record,
                        account_surface,
                        deadline,
                        cancellation,
                        true,
                        account_surface == AccountMarketSurface::AlpacaBasic,
                    )
                    .await
                {
                    Ok(completed) => record = completed,
                    Err(error) => {
                        failures.push(LiveSourceRestoreFailure { provider, error });
                        continue;
                    }
                }
            }
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
                let command = if surface == AccountMarketSurface::AlpacaBasic {
                    // Retry owns fresh doctor verification when a saved proof expired while
                    // the application was stopped. Saved keys alone never authorize a runtime.
                    let (Some(_session), Some(_configuration)) =
                        (record.session_id(), record.public_configuration_digest())
                    else {
                        failures.push(LiveSourceRestoreFailure {
                            provider,
                            error: SourceLifecycleError::InvalidResult,
                        });
                        continue;
                    };
                    saved_source_retry_command(
                        provider.clone(),
                        record.revision(),
                        "automatic-alpaca-source-recovery",
                        deadline,
                        cancellation.child_token(),
                    )?
                } else {
                    let request =
                        match self.restored_account_group_request(surface, &provider, &record) {
                            Ok(request) => request,
                            Err(error) => {
                                failures.push(LiveSourceRestoreFailure { provider, error });
                                continue;
                            }
                        };
                    SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                        provider: provider.clone(),
                        action: SourceLifecycleAction::Start,
                        expected_state_revision: record.revision(),
                        expected_generation: None,
                        expected_runtime_generation_digest: None,
                        onboarding_session_id: Some(request.onboarding_session_id()),
                        public_configuration_digest: Some(
                            request.expected_public_configuration_digest(),
                        ),
                        reason: None,
                        cancellation: cancellation.child_token(),
                        deadline,
                    })?
                };
                match self.execute_owned(&command).await {
                    Ok(_) => restored.push(provider),
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
        self.credential_access.ensure_resumed()?;
        let provider = command.provider().as_str().to_owned();
        let _mutation = self
            .lifecycle_gate_before(&provider, command.deadline(), command.cancellation())
            .await?;
        self.credential_access.ensure_resumed()?;
        let command_digest = command_digest(command)?;
        let operation_id = operation_id(command_digest)?;
        let current = self
            .durable
            .source_lifecycle_record(&provider)
            .map_err(map_durable_error)?;
        if let Some(surface) = AccountMarketSurface::parse(&provider) {
            if command.action() != SourceLifecycleAction::Verify {
                return self
                    .execute_account_transition(
                        command,
                        surface,
                        current,
                        command_digest,
                        operation_id,
                        None,
                    )
                    .await;
            }
            if current.account().is_some_and(|pending| !pending.finished) {
                return Err(SourceLifecycleError::ReconciliationRequired);
            }
        }
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
            let execution: Pin<
                Box<
                    dyn Future<Output = Result<LifecycleOutcome, SourceLifecycleError>> + Send + '_,
                >,
            > = if command.action() == SourceLifecycleAction::Verify
                && command.provider().as_str() == ProviderMarketAccount::AlpacaBasic.surface_id()
            {
                Box::pin(self.execute_alpaca_verify(
                    command,
                    prior_record.phase(),
                    prior_session_id,
                    prior_public_configuration_digest,
                    prior_runtime_verification_receipt_digest,
                    prior_credential_generation,
                ))
            } else {
                Box::pin(self.execute_live(
                    command,
                    prior_session_id,
                    prior_public_configuration_digest,
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
            let _blocked = self
                .durable
                .require_source_lifecycle_reconciliation(&provider, transition_digest);
            return Err(error);
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
                let _blocked = self
                    .durable
                    .require_source_lifecycle_reconciliation(&provider, transition_digest);
                return Err(map_durable_error(error));
            }
        };
        if outcome.phase == DurableSourceLifecyclePhase::Active
            && matches!(
                command.action(),
                SourceLifecycleAction::Start
                    | SourceLifecycleAction::Retry
                    | SourceLifecycleAction::Reconfigure
            )
            && matches!(
                provider.as_str(),
                "treasury.fiscal-data" | "treasury.daily-rates-xml"
            )
        {
            // The exact lifecycle transition is durable before ordinary callable-recipe
            // admission resumes. Reuse the existing retained portal task; its acknowledgement
            // means publication is pending, while Macro readiness still requires completion.
            let session_id = record
                .session_id()
                .ok_or(SourceLifecycleError::InvalidResult)?;
            self.portal
                .resume_research_publication(session_id, command.cancellation().child_token())
                .await
                .map_err(|_| SourceLifecycleError::Unavailable)?;
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

    async fn lifecycle_gate_before(
        &self,
        provider: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, SourceLifecycleError> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(SourceLifecycleError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => Err(SourceLifecycleError::DeadlineExceeded),
            result = self.durable.acquire_source_lifecycle(provider) => result.map_err(map_durable_error),
        }
    }

    /// Real OAuth callback joins the same durable owner; it cannot wait while holding OAuth's
    /// session lock for a lifecycle start that may itself need that session lock.
    pub(super) async fn drain_schwab_oauth(
        &self,
        session: uuid::Uuid,
        current_receipt: Option<market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt>,
        purpose: crate::provider_onboarding::SchwabOAuthMarketDrainPurpose,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let _gate = self
            .durable
            .try_acquire_source_lifecycle()
            .map_err(map_durable_error)?;
        let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
        let surface = AccountMarketSurface::SchwabMarketData;
        if matches!(
            purpose,
            crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::CredentialLock
        ) {
            if self
                .durable
                .source_lifecycle_record(surface.surface_id())
                .map_err(map_durable_error)?
                .account()
                .is_some_and(|pending| !pending.finished)
            {
                return Err(SourceLifecycleError::ReconciliationRequired);
            }
            self.live
                .prepare_schwab_oauth_stop(session, current_receipt, deadline, cancellation)
                .await
                .map_err(map_live_error)?;
            return self
                .suspend_live_credentials(surface.surface_id(), deadline, cancellation)
                .await;
        }
        let action = match purpose {
            crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::ProcessShutdown => {
                AccountLifecycleAction::OAuthProcessShutdown
            }
            crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::Unlink => {
                AccountLifecycleAction::OAuthUnlink
            }
            crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::CredentialReplacement => {
                AccountLifecycleAction::OAuthCredentialReplacement
            }
            crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::CredentialLock => {
                return Err(SourceLifecycleError::Internal);
            }
        };
        let prepared = self
            .live
            .prepare_schwab_oauth_stop(session, current_receipt, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        let current = self
            .durable
            .source_lifecycle_record(surface.surface_id())
            .map_err(map_durable_error)?;
        let record = if let Some(pending) = current.account().filter(|pending| !pending.finished) {
            if !matches!(
                pending.action,
                AccountLifecycleAction::OAuthProcessShutdown
                    | AccountLifecycleAction::OAuthUnlink
                    | AccountLifecycleAction::OAuthCredentialReplacement
            ) || (pending.action != action
                && action != AccountLifecycleAction::OAuthProcessShutdown)
                || pending
                    .predecessor
                    .as_ref()
                    .is_some_and(|original| original.session_id() != session)
            {
                return Err(SourceLifecycleError::ReconciliationRequired);
            }
            current
        } else {
            let Some((request, generation)) = prepared.predecessor() else {
                return Ok(());
            };
            let mut hash = Sha256::new();
            hash.update(b"market-squawk/source-lifecycle-oauth-drain/v1\0");
            hash.update(session.as_bytes());
            hash.update(generation.digest().bytes());
            hash.update(current.revision().get().to_be_bytes());
            hash.update([match purpose {
                crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::ProcessShutdown => 1,
                crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::Unlink => 2,
                crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::CredentialReplacement => 3,
                crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::CredentialLock => {
                    return Err(SourceLifecycleError::Internal);
                }
            }]);
            let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
            let pending = PendingAccountLifecycle {
                action,
                predecessor: Some(AccountAllocationCoordinates::from_observed(
                    request, generation,
                )),
                disposition: AccountStopDisposition::AwaitingRuntime,
                target_session_id: Some(request.onboarding_session_id()),
                target_configuration_sha256: Some(lower_hex(
                    &request.expected_public_configuration_digest().bytes(),
                )),
                successor: None,
                retired_successor: None,
                successor_retirement: None,
                finished: false,
                // Only explicit process shutdown keeps the original desired Active choice.
                oauth_restore_active: purpose
                    == crate::provider_onboarding::SchwabOAuthMarketDrainPurpose::ProcessShutdown
                    && current.phase() == DurableSourceLifecyclePhase::Active,
            };
            self.durable
                .begin_account_lifecycle(
                    surface.surface_id(),
                    &current,
                    operation_id(digest)?,
                    digest,
                    pending,
                )
                .map_err(map_durable_error)?
        };
        let record = self
            .finish_account_predecessor(&record, surface, Some(prepared), deadline, cancellation)
            .await?;
        self.continue_account_transition(record, surface, deadline, cancellation, false, false)
            .await?;
        Ok(())
    }

    async fn drain_pending_account_transitions(
        &self,
        deadline: Instant,
    ) -> Result<(), SourceLifecycleError> {
        let cancellation = CancellationToken::new();
        let gate = tokio::time::timeout_at(
            deadline.into(),
            self.durable
                .acquire_source_lifecycle(ProviderMarketAccount::AlpacaBasic.surface_id()),
        )
        .await
        .map_err(|_| SourceLifecycleError::DeadlineExceeded)?
        .map_err(map_durable_error)?;
        let mut failure = None;
        for surface in [
            AccountMarketSurface::AlpacaBasic,
            AccountMarketSurface::KrakenLevel3,
            AccountMarketSurface::SchwabMarketData,
        ] {
            let current = self
                .durable
                .source_lifecycle_record(surface.surface_id())
                .map_err(map_durable_error)?;
            if current.account().is_some_and(|pending| !pending.finished) {
                if let Err(error) = self
                    .continue_account_transition(
                        current,
                        surface,
                        deadline,
                        &cancellation,
                        false,
                        false,
                    )
                    .await
                {
                    failure.get_or_insert(error);
                }
            }
        }
        drop(gate);
        failure.map_or(Ok(()), Err)
    }

    /// Caller holds the sole lifecycle gate. Pending intent is resumed, never replaced by Retry.
    async fn execute_account_transition(
        &self,
        command: &SourceLifecycleCommand,
        surface: AccountMarketSurface,
        current: DurableSourceLifecycleRecord,
        digest: EvidenceDigest,
        operation: SourceIdentifier,
        expected_predecessor: Option<(
            PreparedMarketProviderConfigurationRequest,
            MarketRuntimeGroupGeneration,
        )>,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        let provider = surface.surface_id();
        if current.operation_id() == Some(&operation)
            && current.command_digest() == Some(digest)
            && current.account().is_some_and(|pending| pending.finished)
        {
            return self
                .receipt_for_current(
                    command,
                    operation,
                    SourceLifecycleDisposition::Replay,
                    &current,
                    None,
                )
                .await;
        }
        let cancel_pending = matches!(
            command.action(),
            SourceLifecycleAction::Stop | SourceLifecycleAction::Remove
        ) && current.account().is_some_and(|pending| {
            !pending.finished
                && matches!(
                    pending.action,
                    AccountLifecycleAction::Start
                        | AccountLifecycleAction::Retry
                        | AccountLifecycleAction::Resynchronize
                        | AccountLifecycleAction::Reconfigure
                        | AccountLifecycleAction::Verify
                )
        });
        let current = if cancel_pending {
            // Fence and persist Stop/Remove before retiring either allocation, so recovery
            // cannot resume the superseded activation after an interrupted drain.
            if command.expected_state_revision() != current.revision()
                || command.expected_generation().is_some()
            {
                return Err(SourceLifecycleError::Conflict);
            }
            let observed = self
                .live
                .prepare_account_stop(surface, command.deadline(), command.cancellation())
                .await
                .map_err(map_live_error)?;
            if command
                .expected_runtime_generation_digest()
                .is_some_and(|expected| {
                    observed
                        .predecessor()
                        .map(|(_, generation)| generation.digest())
                        != Some(expected)
                })
            {
                return Err(SourceLifecycleError::Conflict);
            }
            let mut pending = current
                .account()
                .cloned()
                .ok_or(SourceLifecycleError::InvalidResult)?;
            pending.action = if command.action() == SourceLifecycleAction::Remove {
                AccountLifecycleAction::Remove
            } else {
                AccountLifecycleAction::Stop
            };
            self.durable
                .begin_account_lifecycle(provider, &current, operation.clone(), digest, pending)
                .map_err(map_durable_error)?
        } else {
            current
        };
        let mut record = if current.account().is_some_and(|pending| !pending.finished) {
            if (current.operation_id() != Some(&operation)
                || current.command_digest() != Some(digest))
                && (command.action() != SourceLifecycleAction::Retry
                    || command.expected_state_revision() != current.revision()
                    || command.onboarding_session_id().is_some_and(|session| {
                        Some(session)
                            != current
                                .account()
                                .and_then(|pending| pending.target_session_id)
                    })
                    || command.public_configuration_digest().is_some_and(|digest| {
                        current
                            .account()
                            .and_then(|pending| pending.target_configuration().ok().flatten())
                            != Some(digest)
                    }))
            {
                return Err(SourceLifecycleError::ReconciliationRequired);
            }
            current
        } else {
            if command.expected_state_revision() != current.revision() {
                return Err(SourceLifecycleError::Conflict);
            }
            self.preflight_runtime_lease(command, &current)?;
            let (session, configuration) = self.lifecycle_transition_target(command, &current)?;
            let prepared = self
                .live
                .prepare_account_stop(surface, command.deadline(), command.cancellation())
                .await
                .map_err(map_live_error)?;
            let observed = prepared.predecessor();
            // Automatic recovery retains its exact runtime CAS internally: the public Retry
            // command addresses saved intent and deliberately accepts no runtime coordinates.
            if expected_predecessor.is_some_and(|expected| observed != Some(expected)) {
                return Err(SourceLifecycleError::Conflict);
            }
            if let Some((actual, _)) = observed {
                if current.runtime_verification_receipt_digest().is_some()
                    && actual != account_group_request_from_record(surface, &current)?
                {
                    return Err(SourceLifecycleError::Conflict);
                }
            }
            if command.expected_generation().is_some()
                || command
                    .expected_runtime_generation_digest()
                    .is_some_and(|expected| {
                        observed.map(|(_, generation)| generation.digest()) != Some(expected)
                    })
                || command.action() == SourceLifecycleAction::Resynchronize && observed.is_none()
            {
                return Err(SourceLifecycleError::Conflict);
            }
            let action = match command.action() {
                SourceLifecycleAction::Start => AccountLifecycleAction::Start,
                SourceLifecycleAction::Stop => AccountLifecycleAction::Stop,
                SourceLifecycleAction::Retry => AccountLifecycleAction::Retry,
                SourceLifecycleAction::Resynchronize => AccountLifecycleAction::Resynchronize,
                SourceLifecycleAction::Reconfigure => AccountLifecycleAction::Reconfigure,
                SourceLifecycleAction::Remove => AccountLifecycleAction::Remove,
                SourceLifecycleAction::Verify => return Err(SourceLifecycleError::InvalidRequest),
            };
            let reuse = if action == AccountLifecycleAction::Start {
                if let Some((request, _)) = observed {
                    Some(request.onboarding_session_id()) == session
                        && Some(request.expected_public_configuration_digest()) == configuration
                        && self
                            .live
                            .verify_account_group(
                                request,
                                command.deadline(),
                                command.cancellation(),
                            )
                            .await
                            .map_err(map_live_error)?
                            .is_some()
                } else {
                    false
                }
            } else {
                false
            };
            let predecessor =
                (!reuse)
                    .then_some(observed)
                    .flatten()
                    .map(|(request, generation)| {
                        AccountAllocationCoordinates::from_observed(request, generation)
                    });
            let pending = PendingAccountLifecycle {
                action,
                disposition: if predecessor.is_some() {
                    AccountStopDisposition::AwaitingRuntime
                } else {
                    AccountStopDisposition::NoPredecessor
                },
                predecessor,
                target_session_id: session,
                target_configuration_sha256: configuration.map(|value| lower_hex(&value.bytes())),
                successor: reuse
                    .then_some(observed)
                    .flatten()
                    .map(|(request, generation)| {
                        AccountAllocationCoordinates::from_observed(request, generation)
                    }),
                retired_successor: None,
                successor_retirement: None,
                finished: false,
                oauth_restore_active: false,
            };
            let record = self
                .durable
                .begin_account_lifecycle(provider, &current, operation, digest, pending)
                .map_err(map_durable_error)?;
            // Preparation has never revoked anything. Its exact coordinates are now durable.
            if !reuse {
                self.finish_account_predecessor(
                    &record,
                    surface,
                    Some(prepared),
                    command.deadline(),
                    command.cancellation(),
                )
                .await?
            } else {
                record
            }
        };
        record = self
            .continue_account_transition(
                record,
                surface,
                command.deadline(),
                command.cancellation(),
                true,
                command.action() == SourceLifecycleAction::Retry,
            )
            .await?;
        let operation = record
            .operation_id()
            .cloned()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        self.receipt_for_current(
            command,
            operation,
            SourceLifecycleDisposition::Applied,
            &record,
            None,
        )
        .await
    }

    async fn finish_account_predecessor(
        &self,
        record: &DurableSourceLifecycleRecord,
        surface: AccountMarketSurface,
        preparation: Option<PreparedAccountStop>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
        let pending = record
            .account()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        if pending.disposition != AccountStopDisposition::AwaitingRuntime {
            if pending.disposition == AccountStopDisposition::NoPredecessor {
                if let Some(prepared) = preparation {
                    if prepared.predecessor().is_some()
                        || self
                            .live
                            .consume_account_stop(prepared, deadline, cancellation)
                            .await
                            .map_err(map_live_error)?
                            .is_some()
                    {
                        return Err(SourceLifecycleError::ReconciliationRequired);
                    }
                }
            }
            return Ok(record.clone());
        }
        let prepared = match preparation {
            Some(prepared) => prepared,
            None => self
                .live
                .prepare_account_stop(surface, deadline, cancellation)
                .await
                .map_err(map_live_error)?,
        };
        let (request, generation) = prepared
            .predecessor()
            .ok_or(SourceLifecycleError::ReconciliationRequired)?;
        if !pending
            .predecessor
            .as_ref()
            .is_some_and(|original| original.matches(request, generation))
        {
            return Err(SourceLifecycleError::ReconciliationRequired);
        }
        let receipt = self
            .live
            .consume_account_stop(prepared, deadline, cancellation)
            .await
            .map_err(map_live_error)?
            .ok_or(SourceLifecycleError::InvalidResult)?;
        let mut committed = None;
        self.live
            .acknowledge_account_group_stop(
                &receipt,
                |receipt| {
                    committed = Some(
                        self.durable
                            .acknowledge_account_predecessor(surface.surface_id(), record, receipt)
                            .map_err(|_| market_squawk_services::ServiceError::Unavailable)?,
                    );
                    Ok(())
                },
                deadline,
                cancellation,
            )
            .await
            .map_err(map_live_error)?;
        committed.ok_or(SourceLifecycleError::InvalidResult)
    }

    async fn continue_account_transition(
        &self,
        record: DurableSourceLifecycleRecord,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
        start_successor: bool,
        renew_expired_doctor: bool,
    ) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        let mut record = self
            .finish_account_predecessor(&record, surface, None, deadline, cancellation)
            .await?;
        let mut pending = record
            .account()
            .cloned()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        if pending.finished {
            return Ok(record);
        }
        match pending.action {
            AccountLifecycleAction::Stop
            | AccountLifecycleAction::Remove
            | AccountLifecycleAction::OAuthProcessShutdown
            | AccountLifecycleAction::OAuthUnlink
            | AccountLifecycleAction::OAuthCredentialReplacement => {
                if matches!(
                    pending.action,
                    AccountLifecycleAction::Stop | AccountLifecycleAction::Remove
                ) {
                    record = self
                        .drain_account_successor(record, surface, deadline, cancellation)
                        .await?;
                    pending = record
                        .account()
                        .cloned()
                        .ok_or(SourceLifecycleError::InvalidResult)?;
                }
                if pending.action == AccountLifecycleAction::Remove {
                    let sessions = [
                        record.session_id(),
                        pending.target_session_id,
                        pending
                            .predecessor
                            .as_ref()
                            .map(AccountAllocationCoordinates::session_id),
                    ]
                    .into_iter()
                    .flatten()
                    .collect::<std::collections::BTreeSet<_>>();
                    for session in sessions {
                        self.portal
                            .cancel(session, cancellation.child_token())
                            .await
                            .map_err(|_| SourceLifecycleError::ReconciliationRequired)?;
                    }
                }
                pending.finished = true;
                let phase = if pending.action == AccountLifecycleAction::Remove {
                    DurableSourceLifecyclePhase::Removed
                } else if pending.oauth_restore_active {
                    DurableSourceLifecyclePhase::Active
                } else {
                    DurableSourceLifecyclePhase::Stopped
                };
                return self
                    .durable
                    .update_account_lifecycle(surface.surface_id(), &record, pending, phase, None)
                    .map_err(map_durable_error);
            }
            AccountLifecycleAction::Start
            | AccountLifecycleAction::Retry
            | AccountLifecycleAction::Resynchronize
            | AccountLifecycleAction::Reconfigure => {}
            AccountLifecycleAction::Verify => {
                let session = pending
                    .target_session_id
                    .ok_or(SourceLifecycleError::Unauthorized)?;
                let configuration = pending
                    .target_configuration()
                    .map_err(map_durable_error)?
                    .ok_or(SourceLifecycleError::Unauthorized)?;
                let lease = self
                    .onboarding
                    .activation_lease(session)
                    .or_else(|_| self.onboarding.prepared_activation_lease(session))
                    .map_err(|_| SourceLifecycleError::Unauthorized)?;
                let request = account_group_request_from_binding(
                    surface,
                    Some(session),
                    Some(configuration),
                    Some(&lease),
                )?;
                pending.finished = true;
                return self
                    .durable
                    .update_account_lifecycle(
                        surface.surface_id(),
                        &record,
                        pending,
                        DurableSourceLifecyclePhase::Stopped,
                        Some(request),
                    )
                    .map_err(map_durable_error);
            }
        }
        if !start_successor {
            // Preserve the user's original successor intent while joining any allocation already
            // constructed by it. Shutdown never starts another provider runtime.
            return self
                .drain_account_successor(record, surface, deadline, cancellation)
                .await;
        }
        let session = pending
            .target_session_id
            .ok_or(SourceLifecycleError::Unauthorized)?;
        let configuration = pending
            .target_configuration()
            .map_err(map_durable_error)?
            .ok_or(SourceLifecycleError::Unauthorized)?;
        let lease = if renew_expired_doctor && surface == AccountMarketSurface::AlpacaBasic {
            self.alpaca_retry_admission(&record, session, configuration)?
        } else {
            Some(
                self.onboarding
                    .activation_lease(session)
                    .or_else(|_| self.onboarding.prepared_activation_lease(session))
                    .map_err(|_| SourceLifecycleError::Unauthorized)?,
            )
        };
        let lease = match lease {
            Some(lease) => lease,
            None => {
                // Retry retains the saved target and successor intent. Only a fresh doctor
                // can replace expired authority; configuration and generation remain bound.
                let generation = record
                    .credential_generation()
                    .ok_or(SourceLifecycleError::Unauthorized)?;
                ensure_status_live(cancellation, deadline)?;
                let verification_cancellation = cancellation.child_token();
                let _verification_guard = verification_cancellation.clone().drop_guard();
                let renewed = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(SourceLifecycleError::Cancelled),
                    () = tokio::time::sleep_until(deadline.into()) => return Err(SourceLifecycleError::DeadlineExceeded),
                    result = self.onboarding.verify_runtime_activation_target(session, verification_cancellation) => {
                        result.map_err(map_onboarding_error)?
                    }
                };
                ensure_status_live(cancellation, deadline)?;
                if renewed.generation() != Some(generation) {
                    return Err(SourceLifecycleError::Conflict);
                }
                renewed
            }
        };
        let request = account_group_request_from_binding(
            surface,
            Some(session),
            Some(configuration),
            Some(&lease),
        )?;
        // A previous successor can have become unhealthy while its caller was cancelled. It too
        // must be captured, persisted, joined and acknowledged before a fresh start is admitted.
        let observed = self
            .live
            .prepare_account_stop(surface, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        if let Some((actual, generation)) = observed.predecessor() {
            if pending
                .successor
                .as_ref()
                .is_some_and(|prior| !prior.matches(actual, generation))
            {
                return Err(SourceLifecycleError::ReconciliationRequired);
            }
            // Renewal can supersede a partially published successor's doctor receipt. Retire
            // that original allocation through the same target and saved-successor checks;
            // never ask it to impersonate the fresh request or discard its join receipt.
            if actual != request
                || !matches!(
                    self.live
                        .verify_account_group(actual, deadline, cancellation)
                        .await,
                    Ok(Some(_))
                )
            {
                record = self
                    .drain_account_successor(record, surface, deadline, cancellation)
                    .await?;
                pending = record
                    .account()
                    .cloned()
                    .ok_or(SourceLifecycleError::InvalidResult)?;
            }
        } else if pending.successor.is_some() {
            return Err(SourceLifecycleError::ReconciliationRequired);
        }
        let evidence = self
            .live
            .start_account_group(request, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        let generation = validate_account_group_evidence(request, &evidence)?;
        if pending
            .predecessor
            .as_ref()
            .is_some_and(|original| original.matches(request, generation))
        {
            return Err(SourceLifecycleError::ReconciliationRequired);
        }
        pending.successor = Some(AccountAllocationCoordinates::from_observed(
            request, generation,
        ));
        // Persist exact successor identity before opening reads. If admission is cancelled, the
        // pending record and registry continue to own this same allocation.
        record = self
            .durable
            .update_account_lifecycle(
                surface.surface_id(),
                &record,
                pending.clone(),
                DurableSourceLifecyclePhase::Applying,
                Some(request),
            )
            .map_err(map_durable_error)?;
        self.live
            .admit_account_group_reads(request, generation, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        if surface == AccountMarketSurface::AlpacaBasic {
            // Authorized source activation publishes the original calendar before local paper
            // preparation can read it. A failure retains Applying and this exact successor for
            // Retry; opening a read-only paper dialog never acquires provider data.
            let calendar = self.calendars
                .preflight_current_session(deadline, cancellation.clone())
                .await
                .map_err(|error| {
                    use crate::application::market_calendar::CompletedMarketSessionError;
                    tracing::warn!(%error, stage = "alpaca_calendar_publication", "source activation calendar unavailable");
                    match error {
                        CompletedMarketSessionError::Cancelled => SourceLifecycleError::Cancelled,
                        CompletedMarketSessionError::DeadlineExceeded => SourceLifecycleError::DeadlineExceeded,
                        CompletedMarketSessionError::InvalidRequest
                        | CompletedMarketSessionError::InvalidEvidence => SourceLifecycleError::InvalidResult,
                        CompletedMarketSessionError::ResourceBoundExceeded
                        | CompletedMarketSessionError::Unavailable => SourceLifecycleError::Unavailable,
                    }
                })?;
            if calendar.is_none() {
                tracing::warn!(
                    stage = "alpaca_calendar_publication",
                    "source activation calendar unavailable"
                );
                return Err(SourceLifecycleError::Unavailable);
            }
            ensure_status_live(cancellation, deadline)?;
        }
        pending.finished = true;
        let active = self
            .durable
            .update_account_lifecycle(
                surface.surface_id(),
                &record,
                pending,
                DurableSourceLifecyclePhase::Active,
                Some(request),
            )
            .map_err(map_durable_error)?;
        if surface == AccountMarketSurface::AlpacaBasic {
            let admitted = match self
                .live
                .current_alpaca_calendar_runtime(deadline, cancellation)
                .await
            {
                Ok(runtime) => self.admit_display_history(runtime, deadline),
                Err(_) if cancellation.is_cancelled() => {
                    Err(market_squawk_services::ServiceError::Cancelled)
                }
                Err(_) if Instant::now() >= deadline => {
                    Err(market_squawk_services::ServiceError::DeadlineExceeded)
                }
                Err(_) => Err(market_squawk_services::ServiceError::Unavailable),
            };
            if let Err(error) = admitted {
                // History is a display prerequisite, not connection authority. Its failure
                // must not undo the exact healthy source persisted above.
                tracing::warn!(%error, provider = surface.surface_id(),
                    "starter market history preparation is unavailable");
                if matches!(
                    error,
                    market_squawk_services::ServiceError::Cancelled
                        | market_squawk_services::ServiceError::DeadlineExceeded
                ) {
                    return Err(map_live_error(error));
                }
            }
        }
        Ok(active)
    }

    async fn drain_account_successor(
        &self,
        record: DurableSourceLifecycleRecord,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DurableSourceLifecycleRecord, SourceLifecycleError> {
        let mut pending = record
            .account()
            .cloned()
            .ok_or(SourceLifecycleError::InvalidResult)?;
        let prepared = self
            .live
            .prepare_account_stop(surface, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        let Some((request, generation)) = prepared.predecessor() else {
            return if pending.successor.is_none() {
                Ok(record)
            } else {
                Err(SourceLifecycleError::ReconciliationRequired)
            };
        };
        if Some(request.onboarding_session_id()) != pending.target_session_id
            || Some(request.expected_public_configuration_digest())
                != pending.target_configuration().map_err(map_durable_error)?
            || pending
                .successor
                .as_ref()
                .is_some_and(|prior| !prior.matches(request, generation))
        {
            return Err(SourceLifecycleError::ReconciliationRequired);
        }
        pending.successor = Some(AccountAllocationCoordinates::from_observed(
            request, generation,
        ));
        let record = self
            .durable
            .update_account_lifecycle(
                surface.surface_id(),
                &record,
                pending.clone(),
                record.phase(),
                None,
            )
            .map_err(map_durable_error)?;
        let receipt = self
            .live
            .consume_account_stop(prepared, deadline, cancellation)
            .await
            .map_err(map_live_error)?
            .ok_or(SourceLifecycleError::InvalidResult)?;
        let mut committed = None;
        self.live
            .acknowledge_account_group_stop(
                &receipt,
                |receipt| {
                    if !pending.successor.as_ref().is_some_and(|original| {
                        original.matches(receipt.request(), receipt.generation())
                    }) {
                        return Err(market_squawk_services::ServiceError::InvalidResult);
                    }
                    pending.retired_successor = pending.successor.take();
                    pending.successor_retirement = Some(AccountStopDisposition::GracefullyDrained);
                    committed = Some(
                        self.durable
                            .update_account_lifecycle(
                                surface.surface_id(),
                                &record,
                                pending,
                                record.phase(),
                                None,
                            )
                            .map_err(|_| market_squawk_services::ServiceError::Unavailable)?,
                    );
                    Ok(())
                },
                deadline,
                cancellation,
            )
            .await
            .map_err(map_live_error)?;
        committed.ok_or(SourceLifecycleError::InvalidResult)
    }

    async fn status_owned(
        &self,
        provider: &SourceIdentifier,
        cancellation: &tokio_util::sync::CancellationToken,
        deadline: Instant,
    ) -> Result<SourceLifecycleStatus, SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        // The lock owner already retains the durable gate while suspended. Serialize this read
        // against suspension/resume, then use that held authority instead of awaiting it again.
        let _operation = self
            .credential_access
            .operation_before(deadline, cancellation)
            .await?;
        let _read = if self.credential_access.owns_mutation()? {
            None
        } else {
            Some(
                self.lifecycle_gate_before(provider.as_str(), deadline, cancellation)
                    .await?,
            )
        };
        ensure_status_live(cancellation, deadline)?;
        let record = match self.durable.source_lifecycle_record(provider.as_str()) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(provider = %provider.as_str(), %error, "source lifecycle state is unavailable");
                ensure_status_live(cancellation, deadline)?;
                return SourceLifecycleStatus::try_new(SourceLifecycleStatusInput {
                    provider: provider.clone(),
                    state_revision: NonZeroU64::MIN,
                    state: SourceLifecycleState::Blocked,
                    configuration_session_id: None,
                    current_generation: None,
                    runtime_generation_digest: None,
                    public_configuration_digest: None,
                    doctor: None,
                    start_eligibility: if provider.as_str()
                        == ProviderMarketAccount::AlpacaBasic.surface_id()
                    {
                        SourceStartEligibility::ReconciliationRequired
                    } else {
                        SourceStartEligibility::NotApplicable
                    },
                    blocker: Some(SourceLifecycleBlocker::Reconciliation),
                    observed_at: system_timestamp()?,
                });
            }
        };
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
                    let digest = generation
                        .generation_digest()
                        .map_err(|_| SourceLifecycleError::InvalidResult)?;
                    if self.research_selection_available(provider, digest) {
                        research = Some(digest);
                    } else {
                        state = SourceLifecycleState::Blocked;
                        blocker = Some(SourceLifecycleBlocker::Reconciliation);
                    }
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
            || Some(lease.public_configuration_digest()) != prior_public_configuration_digest
        {
            return Err(SourceLifecycleError::Conflict);
        }
        if let Some(request) = prior_request {
            let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
            let cleanup = CancellationToken::new();
            let prepared = self
                .live
                .prepare_account_stop(request.surface(), deadline, &cleanup)
                .await
                .map_err(map_live_error)?;
            if prepared
                .predecessor()
                .is_some_and(|(actual, _)| actual != request)
            {
                return Err(SourceLifecycleError::Conflict);
            }
            let predecessor = prepared.predecessor().map(|(actual, generation)| {
                AccountAllocationCoordinates::from_observed(actual, generation)
            });
            let record = self
                .durable
                .source_lifecycle_record(command.provider().as_str())
                .map_err(map_durable_error)?;
            let pending = PendingAccountLifecycle {
                action: AccountLifecycleAction::Verify,
                disposition: if predecessor.is_some() {
                    AccountStopDisposition::AwaitingRuntime
                } else {
                    AccountStopDisposition::NoPredecessor
                },
                predecessor,
                target_session_id: Some(lease.session_id()),
                target_configuration_sha256: Some(lower_hex(
                    &lease.public_configuration_digest().bytes(),
                )),
                successor: None,
                retired_successor: None,
                successor_retirement: None,
                finished: false,
                oauth_restore_active: false,
            };
            let record = self
                .durable
                .attach_account_lifecycle(command.provider().as_str(), &record, pending)
                .map_err(map_durable_error)?;
            self.finish_account_predecessor(
                &record,
                request.surface(),
                Some(prepared),
                deadline,
                &cleanup,
            )
            .await?;
        }
        LifecycleOutcome::stopped_with_runtime_verification(&lease)
    }

    async fn execute_live(
        &self,
        command: &SourceLifecycleCommand,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
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
                })
                .or_else(|| {
                    is_session_backed_live_surface(command.provider().as_str())
                        .then(|| {
                            self.onboarding
                                .current_runtime_activation_target(command.provider())
                                .ok()
                                .flatten()
                                .and_then(|(session_id, digest)| {
                                    self.onboarding.activation_lease(session_id).ok().filter(
                                        |lease| {
                                            lease.surface_id() == command.provider()
                                                && lease.public_configuration_digest() == digest
                                        },
                                    )
                                })
                        })
                        .flatten()
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
                .verify_account_group_live(
                    command,
                    surface,
                    session_id,
                    public_configuration_digest,
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

    async fn verify_account_group_live(
        &self,
        command: &SourceLifecycleCommand,
        surface: AccountMarketSurface,
        session_id: Option<uuid::Uuid>,
        public_configuration_digest: Option<EvidenceDigest>,
        lease: Option<&crate::ProviderActivationLease>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        if command.action() != SourceLifecycleAction::Verify {
            return Err(SourceLifecycleError::InvalidRequest);
        }
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
                        command.deadline(),
                    )
                    .await
                    .map_err(|_| SourceLifecycleError::Unavailable)?;
                } else if matches!(profile.as_str(), "eia.api-v2" | "census.data-api") {
                    cli_provider::publish_activated_macro_data(
                        &self.activation,
                        &lease,
                        command.cancellation().child_token(),
                        command.deadline(),
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
                    if matches!(profile.as_str(), "eia.api-v2" | "census.data-api") {
                        let lease = self
                            .onboarding
                            .activation_lease(retained.session_id)
                            .map_err(|_| SourceLifecycleError::Unavailable)?;
                        cli_provider::publish_activated_macro_data(
                            &self.activation,
                            &lease,
                            command.cancellation().child_token(),
                            command.deadline(),
                        )
                        .await
                        .map_err(|_| SourceLifecycleError::Unavailable)?;
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
                        command.deadline(),
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

    /// A callable adapter cannot make a malformed or mismatched saved product selection healthy.
    fn research_selection_available(
        &self,
        provider: &SourceIdentifier,
        runtime_digest: EvidenceDigest,
    ) -> bool {
        let surface = provider.as_str();
        if surface != market_squawk_sources::FRED_ALFRED_API_SURFACE_ID
            && surface != "treasury.fiscal-data"
            && surface != "treasury.daily-rates-xml"
        {
            return true;
        }
        let Ok(DurableActivationRecipeState::Desired(recipe)) = self.durable.load_recipe(surface)
        else {
            return false;
        };
        if recipe.runtime_generation_digest != runtime_digest {
            return false;
        }
        if surface == market_squawk_sources::FRED_ALFRED_API_SURFACE_ID {
            let Ok(dataset) = cli_provider::fred_dashboard_provider_dataset(&self.durable) else {
                return false;
            };
            return FredPointInTimeReadCapability::try_new(
                self.research.analytical_reader(),
                dataset,
            )
            .is_ok();
        }
        if surface == "treasury.fiscal-data" {
            return cli_provider::treasury_fiscal_release_query(&self.durable)
                .is_ok_and(|(_, expected)| expected == runtime_digest);
        }
        cli_provider::treasury_daily_rate_all_history_datasets(&self.durable)
            .is_ok_and(|(_, expected)| expected == runtime_digest)
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
            (Some(session_id), None) if !is_session_backed_live_surface(provider.as_str()) => self
                .onboarding
                .runtime_activation_target_public_configuration(session_id, provider)
                .map(|digest| (Some(session_id), Some(digest)))
                .map_err(map_onboarding_error),
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
        let lease = if command.action() == SourceLifecycleAction::Retry
            && command.provider().as_str() == ProviderMarketAccount::AlpacaBasic.surface_id()
        {
            match self.alpaca_retry_admission(current, session_id, public_configuration_digest)? {
                Some(lease) => lease,
                // Only renewal work is admitted. The durable account transition precedes the
                // fresh doctor and ordinary runtime admission in continuation.
                None => return Ok(()),
            }
        } else {
            self.onboarding
                .activation_lease(session_id)
                .or_else(|_| self.onboarding.prepared_activation_lease(session_id))
                .map_err(|_| SourceLifecycleError::Unauthorized)?
        };
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

    /// Returns a current exact lease, or admits only renewal of the retained Alpaca doctor.
    /// A missing lease never admits runtime start; continuation must obtain fresh authority.
    fn alpaca_retry_admission(
        &self,
        record: &DurableSourceLifecycleRecord,
        session_id: uuid::Uuid,
        configuration: EvidenceDigest,
    ) -> Result<Option<crate::ProviderActivationLease>, SourceLifecycleError> {
        if record.session_id() != Some(session_id)
            || record.public_configuration_digest() != Some(configuration)
        {
            return Err(SourceLifecycleError::Conflict);
        }
        let provider = SourceIdentifier::try_from(ProviderMarketAccount::AlpacaBasic.surface_id())
            .map_err(|_| SourceLifecycleError::InvalidResult)?;
        let current_configuration = self
            .onboarding
            .runtime_activation_target_public_configuration(session_id, &provider)
            .map_err(map_onboarding_error)?;
        if current_configuration != configuration {
            return Err(SourceLifecycleError::Conflict);
        }
        let generation = record
            .credential_generation()
            .ok_or(SourceLifecycleError::Unauthorized)?;
        let retained = self
            .onboarding
            .retained_runtime_verification_evidence(
                session_id,
                &provider,
                configuration,
                generation,
            )
            .map_err(map_onboarding_error)?;
        let doctor = retained
            .evidence()
            .alpaca_paper_iex_receipt()
            .ok_or(SourceLifecycleError::Unauthorized)?;
        if doctor.surface_id() != &provider
            || doctor.session_identifier().as_str() != session_id.hyphenated().to_string()
            || doctor.public_configuration_digest() != configuration
            || doctor.generation() != generation
        {
            return Err(SourceLifecycleError::Conflict);
        }
        let lease = self
            .onboarding
            .activation_lease(session_id)
            .or_else(|error| {
                // RenewalRequired rejects scoped authority before the expiry check. Preserve that
                // result for classification against the exact retained state, not a prepared target.
                if matches!(
                    error,
                    crate::ProviderOnboardingError::ActivationExpired
                        | crate::ProviderOnboardingError::InvalidSessionState
                ) {
                    Err(error)
                } else {
                    self.onboarding.prepared_activation_lease(session_id)
                }
            });
        match lease {
            Ok(lease) => {
                if lease.surface_id() != &provider
                    || lease.session_id() != session_id
                    || lease.public_configuration_digest() != configuration
                    || lease.generation() != Some(generation)
                {
                    return Err(SourceLifecycleError::Conflict);
                }
                Ok(Some(lease))
            }
            Err(crate::ProviderOnboardingError::ActivationExpired) => Ok(None),
            Err(crate::ProviderOnboardingError::InvalidSessionState)
                if retained.onboarding_state()
                    == market_squawk_sources::OnboardingState::RenewalRequired =>
            {
                Ok(None)
            }
            Err(_) => Err(SourceLifecycleError::Unauthorized),
        }
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

fn saved_source_retry_command(
    provider: SourceIdentifier,
    expected_state_revision: NonZeroU64,
    reason: &str,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<SourceLifecycleCommand, SourceLifecycleError> {
    SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
        provider,
        action: SourceLifecycleAction::Retry,
        expected_state_revision,
        expected_generation: None,
        expected_runtime_generation_digest: None,
        onboarding_session_id: None,
        public_configuration_digest: None,
        reason: Some(
            SourceIdentifier::try_from(reason).map_err(|_| SourceLifecycleError::Internal)?,
        ),
        cancellation,
        deadline,
    })
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
    PUBLIC_LIVE_SURFACES.contains(&surface_id)
        || surface_id == COINBASE_DIRECT_LIVE_SURFACE
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
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), SourceLifecycleError> {
        self.credential_access
            .release_for_shutdown(deadline)
            .await?;
        self.drain_pending_account_transitions(deadline).await
    }

    async fn suspend_credential_runtimes(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        self.suspend_credentials_owned(deadline, cancellation).await
    }

    async fn resume_credential_runtimes(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        self.resume_credentials_owned(deadline, cancellation).await
    }

    fn supports(&self, provider: &SourceIdentifier) -> bool {
        DurableProviderActivationState::supports_source_lifecycle(provider.as_str())
    }

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
    previous_generation: Option<MarketSourceRuntimeGeneration>,
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
            previous_generation,
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
            previous_generation,
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
        })
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
            previous_generation,
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_saved_source_retry_preserves_command_contract_and_control()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = SourceIdentifier::try_from(ProviderMarketAccount::AlpacaBasic.surface_id())?;
        let revision = NonZeroU64::new(7).ok_or("nonzero test revision")?;
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        for reason in [
            "automatic-alpaca-source-recovery",
            "alpaca-doctor-proof-expired",
        ] {
            let cancellation = CancellationToken::new();
            let command = saved_source_retry_command(
                provider.clone(),
                revision,
                reason,
                deadline,
                cancellation.clone(),
            )?;
            assert_eq!(command.provider(), &provider);
            assert_eq!(command.action(), SourceLifecycleAction::Retry);
            assert_eq!(command.expected_state_revision(), revision);
            assert_eq!(command.expected_generation(), None);
            assert_eq!(command.expected_runtime_generation_digest(), None);
            assert_eq!(command.onboarding_session_id(), None);
            assert_eq!(command.public_configuration_digest(), None);
            assert_eq!(command.reason().map(SourceIdentifier::as_str), Some(reason));
            assert_eq!(command.deadline(), deadline);
            assert!(!command.cancellation().is_cancelled());
            cancellation.cancel();
            assert!(command.cancellation().is_cancelled());
            assert_eq!(ensure_live(&command), Err(SourceLifecycleError::Cancelled));
        }
        assert_eq!(
            saved_source_retry_command(
                provider,
                revision,
                "alpaca-doctor-proof-expired",
                Instant::now(),
                CancellationToken::new(),
            )
            .unwrap_err(),
            SourceLifecycleError::DeadlineExceeded,
        );
        Ok(())
    }
}
