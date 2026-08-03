//! Sole production source lifecycle authority over existing live and research owners.

use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use market_squawk_domain::{
    ConnectionGeneration, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use sha2::{Digest as _, Sha256};

use crate::application::PaperSourceLifecycleControl;
use crate::application::source::{
    SourceAuthorizationState, SourceAvailabilityState, SourceLifecycleAction,
    SourceLifecycleAuthority, SourceLifecycleBlocker, SourceLifecycleCommand,
    SourceLifecycleDisposition, SourceLifecycleError, SourceLifecycleReceipt,
    SourceLifecycleReceiptInput, SourceLifecycleState, SourceLifecycleStatus,
    SourceLifecycleStatusInput, SourceRateBudgetState, SourceRightsEvidence,
};
use crate::{
    ProviderAdapterActivation, ProviderOnboardingService, ProviderPortalActivationAuthority,
};

use super::{
    cli_provider,
    provider_activation_state::{
        DurableActivationRecipeState, DurableProviderActivationState,
        DurableProviderActivationStateError, DurableSourceLifecyclePhase,
        DurableSourceLifecycleRecord, DurableSourceLifecycleTransition,
        SERIALIZED_RESEARCH_SURFACES,
    },
};

const LIVE_SURFACES: [&str; 3] = [
    "coinbase.public-market-data",
    "coinbase.exchange-direct-market-data",
    "kraken.spot-public-market-data",
];

/// Single lifecycle authority injected into the Source application domain.
pub(crate) struct ProductionSourceLifecycleAuthority {
    paths: LocalPaths,
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<ProviderAdapterActivation>,
    portal: Arc<dyn ProviderPortalActivationAuthority>,
    durable: DurableProviderActivationState,
    live: PaperSourceLifecycleControl,
}

impl ProductionSourceLifecycleAuthority {
    /// Binds the existing runtime owners without constructing another source runtime.
    pub(crate) fn new(
        paths: LocalPaths,
        onboarding: Arc<ProviderOnboardingService>,
        activation: Arc<ProviderAdapterActivation>,
        portal: Arc<dyn ProviderPortalActivationAuthority>,
        durable: DurableProviderActivationState,
        live: PaperSourceLifecycleControl,
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
        let result = if LIVE_SURFACES.contains(&provider.as_str()) {
            self.execute_live(command, prior_session_id, prior_public_configuration_digest)
                .await
        } else {
            self.execute_research(command, prior_session_id, prior_public_configuration_digest)
                .await
        };
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => {
                let _blocked = self
                    .durable
                    .require_source_lifecycle_reconciliation(&provider, transition_digest);
                return Err(error);
            }
        };
        ensure_live(command)?;
        let record = self
            .durable
            .complete_source_lifecycle_transition(
                &provider,
                transition_digest,
                outcome.phase,
                outcome.session_id,
                outcome.public_configuration_digest,
            )
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
        let mut research = None;
        if state == SourceLifecycleState::Active && LIVE_SURFACES.contains(&provider.as_str()) {
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
        SourceLifecycleStatus::try_new(SourceLifecycleStatusInput {
            provider: provider.clone(),
            state_revision: record.revision(),
            state,
            current_generation: live,
            runtime_generation_digest: research,
            public_configuration_digest: record.public_configuration_digest(),
            blocker,
            observed_at: system_timestamp()?,
        })
    }

    async fn execute_live(
        &self,
        command: &SourceLifecycleCommand,
        prior_session_id: Option<uuid::Uuid>,
        prior_public_configuration_digest: Option<EvidenceDigest>,
    ) -> Result<LifecycleOutcome, SourceLifecycleError> {
        let lease = match self.optional_exact_lease(command)? {
            Some(lease) => Some(lease),
            None => prior_session_id
                .and_then(|session_id| self.onboarding.activation_lease(session_id).ok()),
        };
        let session_id = lease
            .as_ref()
            .map(|value| value.session_id())
            .or(prior_session_id);
        let public_configuration_digest = lease
            .as_ref()
            .map(|value| value.public_configuration_digest())
            .or(prior_public_configuration_digest);
        if command.provider().as_str() == "coinbase.exchange-direct-market-data"
            && matches!(
                command.action(),
                SourceLifecycleAction::Start
                    | SourceLifecycleAction::Verify
                    | SourceLifecycleAction::Reconfigure
            )
            && lease.is_none()
        {
            return Err(SourceLifecycleError::Unauthorized);
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
        let live = if LIVE_SURFACES.contains(&command.provider().as_str()) {
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
        let runtime_generation_digest = if !LIVE_SURFACES.contains(&command.provider().as_str())
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
            .and_then(|session_id| self.onboarding.activation_lease(session_id).ok());
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
                SourceLifecycleState::Active if live.is_some() => {
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
            observed_at,
        })
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

impl std::fmt::Debug for ProductionSourceLifecycleAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProductionSourceLifecycleAuthority")
            .field("paths", &"[LOCAL CAPABILITIES]")
            .field("onboarding", &"[ONBOARDING AUTHORITY]")
            .field("activation", &"[ADAPTER AUTHORITY]")
            .field("durable", &"[DURABLE LIFECYCLE]")
            .field("live", &"[PAPER LIVE OWNER]")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SourceLifecycleAuthority for ProductionSourceLifecycleAuthority {
    fn active_source_count(&self) -> Result<usize, SourceLifecycleError> {
        let mut active_live = 0_usize;
        for surface in LIVE_SURFACES {
            if self
                .durable
                .source_lifecycle_record(surface)
                .map_err(map_durable_error)?
                .phase()
                == DurableSourceLifecyclePhase::Active
            {
                active_live = active_live
                    .checked_add(1)
                    .ok_or(SourceLifecycleError::Unavailable)?;
            }
        }
        let observed_live = self.live.active_source_count().map_err(map_live_error)?;
        if observed_live != active_live {
            return Err(SourceLifecycleError::Unavailable);
        }

        let mut active_research = 0_usize;
        for surface in SERIALIZED_RESEARCH_SURFACES {
            let provider = SourceIdentifier::try_from(surface)
                .map_err(|_| SourceLifecycleError::InvalidResult)?;
            let durable_active = self
                .durable
                .source_lifecycle_record(surface)
                .map_err(map_durable_error)?
                .phase()
                == DurableSourceLifecyclePhase::Active;
            let runtime_active = self
                .activation
                .research_runtime_generation(&provider)
                .map_err(|_error| SourceLifecycleError::Unavailable)?
                .is_some();
            if durable_active != runtime_active {
                return Err(SourceLifecycleError::Unavailable);
            }
            if runtime_active {
                active_research = active_research
                    .checked_add(1)
                    .ok_or(SourceLifecycleError::Unavailable)?;
            }
        }
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
    previous_generation: Option<ConnectionGeneration>,
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
            previous_generation,
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
            previous_generation,
        }
    }

    const fn removed(previous_generation: Option<ConnectionGeneration>) -> Self {
        Self {
            phase: DurableSourceLifecyclePhase::Removed,
            session_id: None,
            public_configuration_digest: None,
            previous_generation,
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
    hasher.update(b"market-squawk/source-lifecycle-command/v1\0");
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

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
