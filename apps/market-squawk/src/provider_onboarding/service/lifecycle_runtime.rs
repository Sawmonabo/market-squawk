//! Durable renewal, expiry, cutover cleanup, and startup reconciliation.

use std::{sync::Arc, time::Instant};

use market_squawk_data::CatalogLimit;
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretInteractionPolicy, SecretKey,
    SecretOperationControl, SecretReconciliationObservation,
};
use market_squawk_sources::{
    AuthorityVerification, CredentialGenerationState, LocalDeletionOutcome, OnboardingEvent,
    OnboardingState, ProfileReleaseState, RemoteRevocationOutcome, SecretStoreClearOutcome,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    OnboardingSessionView, ProviderOnboardingError, ProviderOnboardingService,
    SECRET_OPERATION_DURATION, SESSION_DURATION, await_blocking_secret_operation, event_digest,
    session_view, system_timestamp, wall_deadline,
};

impl ProviderOnboardingService {
    /// Replays one exact durable session, closes safe refresh recovery, and returns status.
    pub fn resume(
        &self,
        session_id: Uuid,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let mut resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let mut profile = self.profile_for(&resumed)?;
        let observed_at = system_timestamp()?;
        let capability_is_current = profile.capability().revision()
            == resumed.lifecycle().capability_revision()
            && profile.capability().content_digest() == resumed.lifecycle().capability_digest();
        if !capability_is_current && resumed.lifecycle().state() != OnboardingState::RefreshRequired
        {
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::RefreshRequired {
                    evidence_digest: event_digest(
                        b"capability-refresh-required",
                        session_id,
                        resumed.lifecycle().candidate_generation(),
                    ),
                },
            )?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        } else if profile.release_state() == ProfileReleaseState::RefreshRequired
            && resumed.lifecycle().state() == OnboardingState::AnonymousAvailable
        {
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::RefreshRequired {
                    evidence_digest: event_digest(
                        b"refresh-required",
                        session_id,
                        resumed.lifecycle().candidate_generation(),
                    ),
                },
            )?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        }
        if capability_is_current
            && let Some(generation) = resumed.lifecycle().candidate_generation()
            && resumed.lifecycle().generation_state(generation)
                == Some(CredentialGenerationState::StorePlanned)
            && let Some(plan) = resumed
                .lifecycle()
                .generation_store_plan(generation)
                .cloned()
        {
            let key = SecretKey::try_new(
                "provider-onboarding",
                &format!("{}.{}", profile.id(), session_id.simple()),
            )?;
            let deadline = Instant::now()
                .checked_add(SECRET_OPERATION_DURATION)
                .ok_or(ProviderOnboardingError::Clock)?;
            let control = SecretOperationControl::try_new(
                format!("provider-startup-reconcile-{session_id}"),
                deadline,
                0,
                SecretInteractionPolicy::Forbid,
                SecretCancellation::new(),
            )?;
            let event = match self.secrets.inspect_planned(&key, &plan, &control) {
                Ok(SecretReconciliationObservation::Absent) => {
                    OnboardingEvent::SecretStoreCleared {
                        generation,
                        reference: plan.target().clone(),
                        outcome: SecretStoreClearOutcome::Absent,
                    }
                }
                Ok(
                    SecretReconciliationObservation::PresentUnverified
                    | SecretReconciliationObservation::Matches
                    | SecretReconciliationObservation::Mismatch,
                )
                | Err(_) => OnboardingEvent::SecretStoreReconciliationRequired {
                    generation,
                    evidence_digest: event_digest(
                        b"startup-secret-store-reconciliation",
                        session_id,
                        Some(generation),
                    ),
                },
            };
            self.append(resumed.reservation(), resumed.next_sequence(), event)?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        }
        if capability_is_current
            && resumed.lifecycle().state() == OnboardingState::ActiveScoped
            && let Some(generation) = resumed.lifecycle().active_generation()
            && let Some(expires_at) = resumed
                .lifecycle()
                .generation_verification(generation)
                .and_then(AuthorityVerification::expires_at)
            && expires_at <= observed_at
        {
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::RenewalRequired {
                    generation,
                    expires_at,
                    evidence_digest: event_digest(
                        b"credential-renewal-required",
                        session_id,
                        Some(generation),
                    ),
                },
            )?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        }
        if capability_is_current
            && resumed.lifecycle().state() == OnboardingState::RotationPending
            && resumed
                .lifecycle()
                .rotation_deadline_at()
                .is_some_and(|deadline| deadline <= observed_at)
        {
            let generation = resumed
                .lifecycle()
                .candidate_generation()
                .ok_or(ProviderOnboardingError::InvalidSessionState)?;
            let generation_state = resumed
                .lifecycle()
                .generation_state(generation)
                .ok_or(ProviderOnboardingError::InvalidSessionState)?;
            let event = match generation_state {
                CredentialGenerationState::Reserved => {
                    OnboardingEvent::CandidateCancelledNoEffect {
                        generation,
                        evidence_digest: event_digest(
                            b"rotation-deadline-cancelled-no-effect",
                            session_id,
                            Some(generation),
                        ),
                    }
                }
                CredentialGenerationState::StorePlanned
                | CredentialGenerationState::StoreReconciliationRequired => {
                    OnboardingEvent::SecretStoreReconciliationRequired {
                        generation,
                        evidence_digest: event_digest(
                            b"rotation-deadline-store-reconciliation",
                            session_id,
                            Some(generation),
                        ),
                    }
                }
                CredentialGenerationState::StoredUnverified
                | CredentialGenerationState::VerifiedLeastPrivilege => {
                    OnboardingEvent::CleanupRequired {
                        generation: Some(generation),
                        evidence_digest: event_digest(
                            b"rotation-deadline-cleanup-required",
                            session_id,
                            Some(generation),
                        ),
                    }
                }
                CredentialGenerationState::ActiveScoped
                | CredentialGenerationState::SupersededRetained
                | CredentialGenerationState::Retired
                | CredentialGenerationState::Tombstoned
                | CredentialGenerationState::AbandonedNoEffect
                | CredentialGenerationState::CleanupRequired => {
                    return Err(ProviderOnboardingError::InvalidSessionState);
                }
            };
            self.append(resumed.reservation(), resumed.next_sequence(), event)?;
            resumed = self.catalog.resume_provider_onboarding(session_id)?;
            profile = self.profile_for(&resumed)?;
        }
        Ok(session_view(profile, &resumed))
    }

    /// Starts one bounded replacement operation while retaining the exact active generation.
    pub async fn begin_renewal(
        &self,
        session_id: Uuid,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let _activation = self.activation.lock().await;
        let _current = self.resume(session_id)?;
        let resumed = self.catalog.resume_provider_onboarding(session_id)?;
        let profile = self.current_profile_for(&resumed)?;
        if !profile.capability().lifecycle_support().rotation()
            || !matches!(
                resumed.lifecycle().state(),
                OnboardingState::ActiveScoped | OnboardingState::RenewalRequired
            )
            || resumed.lifecycle().active_generation().is_none()
            || resumed.lifecycle().candidate_generation().is_some()
        {
            return Err(ProviderOnboardingError::RenewalUnavailable);
        }
        let candidate_generation = resumed
            .lifecycle()
            .next_generation()
            .map_err(|_| ProviderOnboardingError::InvalidSessionState)?;
        let operation_id = Uuid::new_v4();
        self.append(
            resumed.reservation(),
            resumed.next_sequence(),
            OnboardingEvent::BeginRotation {
                candidate_generation,
                operation_owner: Some(SourceIdentifier::try_from(format!(
                    "provider-renewal-{operation_id}"
                ))?),
                deadline_at: Some(wall_deadline(SESSION_DURATION)?),
                retry_budget: 0,
            },
        )?;
        self.resume(session_id)
    }

    pub(super) fn reconcile_startup(
        &self,
        limit: CatalogLimit,
    ) -> Result<(), ProviderOnboardingError> {
        let sessions = self.catalog.current_provider_onboarding_sessions(limit)?;
        for session in sessions {
            let _view = self.resume(session.reservation().session_id())?;
        }
        Ok(())
    }

    /// Reconciles and retires every non-active credential generation for one current session.
    pub async fn reconcile_cleanup(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<OnboardingSessionView, ProviderOnboardingError> {
        let _activation = self.activation.lock().await;
        self.cleanup_superseded_unlocked(session_id, cancellation)
            .await?;
        self.resume(session_id)
    }

    async fn cleanup_superseded_unlocked(
        &self,
        session_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), ProviderOnboardingError> {
        loop {
            let resumed = self.catalog.resume_provider_onboarding(session_id)?;
            let profile = self.current_profile_for(&resumed)?;
            let lifecycle = resumed.lifecycle();
            let target = lifecycle
                .generation_states()
                .find_map(|(generation, state)| {
                    if lifecycle.active_generation() != Some(generation)
                        && matches!(
                            state,
                            CredentialGenerationState::SupersededRetained
                                | CredentialGenerationState::CleanupRequired
                                | CredentialGenerationState::Retired
                        )
                    {
                        Some((generation, state))
                    } else {
                        None
                    }
                });
            let Some((generation, state)) = target else {
                return Ok(());
            };
            if state == CredentialGenerationState::Retired {
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::Tombstone { generation },
                )?;
                continue;
            }
            match lifecycle.generation_remote_revocation(generation) {
                None if profile.capability().lifecycle_support().remote_revocation() => {
                    return Err(ProviderOnboardingError::RemoteReconciliationRequired);
                }
                None => {
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::RemoteRevocation {
                            generation,
                            outcome: RemoteRevocationOutcome::Unsupported,
                            evidence_digest: event_digest(
                                b"remote-revocation-unsupported",
                                session_id,
                                Some(generation),
                            ),
                        },
                    )?;
                    continue;
                }
                Some(RemoteRevocationOutcome::Failed | RemoteRevocationOutcome::Indeterminate) => {
                    return Err(ProviderOnboardingError::RemoteReconciliationRequired);
                }
                Some(
                    RemoteRevocationOutcome::Confirmed
                    | RemoteRevocationOutcome::NotFound
                    | RemoteRevocationOutcome::Unsupported,
                ) => {}
            }
            if !matches!(
                lifecycle.generation_local_deletion(generation),
                Some(LocalDeletionOutcome::Deleted | LocalDeletionOutcome::NotFound)
            ) {
                let reference = lifecycle
                    .generation_cleanup_reference(generation)
                    .cloned()
                    .ok_or(ProviderOnboardingError::SecretCleanupUnavailable)?;
                let secrets = Arc::clone(&self.secrets);
                let outcome = await_blocking_secret_operation(
                    Arc::clone(&self.secret_operations),
                    cancellation.clone(),
                    move |operation| {
                        let deadline = Instant::now()
                            .checked_add(SECRET_OPERATION_DURATION)
                            .ok_or(ProviderOnboardingError::Clock)?;
                        let control = SecretOperationControl::try_new(
                            format!("provider-cleanup-{session_id}-{}", generation.get()),
                            deadline,
                            0,
                            SecretInteractionPolicy::AllowPlatformPrompt,
                            operation,
                        )?;
                        match secrets.delete(&reference, &control) {
                            Ok(()) => Ok(LocalDeletionOutcome::Deleted),
                            Err(LocalSecretStoreError::NotFound) => {
                                Ok(LocalDeletionOutcome::NotFound)
                            }
                            Err(LocalSecretStoreError::IndeterminateCompletion) => {
                                Ok(LocalDeletionOutcome::Indeterminate)
                            }
                            Err(
                                LocalSecretStoreError::OperationCancelled
                                | LocalSecretStoreError::UserCancelled,
                            ) => Err(ProviderOnboardingError::OperationCancelled),
                            Err(_error) => Ok(LocalDeletionOutcome::Failed),
                        }
                    },
                )
                .await?;
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::LocalDeletion {
                        generation,
                        outcome,
                    },
                )?;
                if matches!(
                    outcome,
                    LocalDeletionOutcome::Failed | LocalDeletionOutcome::Indeterminate
                ) {
                    return Err(ProviderOnboardingError::SecretCleanupUnavailable);
                }
                continue;
            }
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::Retire { generation },
            )?;
        }
    }
}
