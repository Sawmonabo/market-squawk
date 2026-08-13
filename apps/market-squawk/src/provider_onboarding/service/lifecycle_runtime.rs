//! Durable renewal, expiry, cutover cleanup, and startup reconciliation.

use std::{sync::Arc, time::Instant};

use market_squawk_data::CatalogLimit;
use market_squawk_domain::SourceIdentifier;
use market_squawk_platform::{
    LocalSecretStoreError, SecretCancellation, SecretDeletionDisposition, SecretInteractionPolicy,
    SecretKey, SecretOperationControl, SecretReconciliationObservation,
};
use market_squawk_sources::{
    AuthorityVerification, CredentialGenerationState, LifecycleSupport, LocalDeletionOutcome,
    OnboardingEvent, OnboardingState, ProfileReleaseState, RemoteRevocationOutcome,
    SecretStoreClearOutcome,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    OnboardingSessionView, ProviderOnboardingError, ProviderOnboardingService,
    ProviderRuntimeStartupAdmissions, SECRET_OPERATION_DURATION, SESSION_DURATION,
    await_blocking_secret_operation, event_digest, session_view, system_timestamp, wall_deadline,
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
            && resumed.lifecycle().active_generation().is_none()
            && !matches!(
                resumed.lifecycle().state(),
                OnboardingState::Blocked
                    | OnboardingState::CleanupRequired
                    | OnboardingState::RevocationUnconfirmed
                    | OnboardingState::IndeterminateRemoteState
            )
            && resumed.reservation().deadline_at() <= observed_at
        {
            let event = match resumed.lifecycle().candidate_generation() {
                Some(generation)
                    if resumed.lifecycle().generation_state(generation)
                        == Some(CredentialGenerationState::Reserved) =>
                {
                    OnboardingEvent::CandidateCancelledNoEffect {
                        generation,
                        evidence_digest: event_digest(
                            b"initial-session-expired-no-effect",
                            session_id,
                            Some(generation),
                        ),
                    }
                }
                Some(generation) => OnboardingEvent::CleanupRequired {
                    generation: Some(generation),
                    evidence_digest: event_digest(
                        b"initial-session-expired-cleanup",
                        session_id,
                        Some(generation),
                    ),
                },
                None => OnboardingEvent::Cancelled {
                    evidence_digest: event_digest(b"initial-session-expired", session_id, None),
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
                .generation_alpaca_paper_iex_doctor_receipt(generation)
                .map(|receipt| receipt.exclusive_expires_at())
                .or_else(|| {
                    resumed
                        .lifecycle()
                        .generation_verification(generation)
                        .and_then(AuthorityVerification::expires_at)
                })
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
        runtime_admissions: &ProviderRuntimeStartupAdmissions,
    ) -> Result<(), ProviderOnboardingError> {
        let mut after = None;
        loop {
            let session_ids = self
                .catalog
                .provider_onboarding_session_ids_after(after, limit)?;
            if session_ids.is_empty() {
                return Ok(());
            }
            for session_id in &session_ids {
                let resumed = self.catalog.resume_provider_onboarding(*session_id)?;
                let profile = self.profiles.get(resumed.lifecycle().surface_id().as_str());
                let exact_capability = profile.and_then(|profile| {
                    profile.capability_at(
                        resumed.lifecycle().capability_revision(),
                        resumed.lifecycle().capability_digest(),
                    )
                });
                let public_configuration_valid = profile.is_some_and(|profile| {
                    super::validate_recovered_public_configuration(
                        profile,
                        resumed.public_configuration(),
                    )
                    .is_ok()
                });
                let exact_lifecycle_support =
                    exact_capability.map(|capability| capability.lifecycle_support());
                let current_runtime_admitted = runtime_admissions
                    .admits(resumed.lifecycle().surface_id(), *session_id)
                    && profile
                        .zip(exact_capability)
                        .is_some_and(|(profile, capability)| {
                            public_configuration_valid
                                && capability == profile.capability()
                                && matches!(
                                    profile.release_state(),
                                    ProfileReleaseState::Available
                                        | ProfileReleaseState::RightsLimited
                                )
                        });

                if profile.is_some() && exact_capability.is_some() && public_configuration_valid {
                    let _view = self.resume(*session_id)?;
                }

                let resumed = self.catalog.resume_provider_onboarding(*session_id)?;
                let lifecycle = resumed.lifecycle();
                let secret_authority_retained =
                    lifecycle.generation_states().any(|(generation, state)| {
                        !matches!(
                            state,
                            CredentialGenerationState::Retired
                                | CredentialGenerationState::Tombstoned
                                | CredentialGenerationState::AbandonedNoEffect
                        ) && lifecycle.generation_cleanup_reference(generation).is_some()
                    });
                let authority_recognized =
                    profile.is_some() && exact_capability.is_some() && public_configuration_valid;
                if (!current_runtime_admitted && secret_authority_retained) || !authority_recognized
                {
                    self.quarantine_startup_authority(&resumed)?;
                }
                self.cleanup_startup_unattended(*session_id, exact_lifecycle_support)?;
            }
            after = session_ids.last().copied();
        }
    }

    fn quarantine_startup_authority(
        &self,
        resumed: &market_squawk_data::ResumedProviderOnboarding,
    ) -> Result<(), ProviderOnboardingError> {
        let lifecycle = resumed.lifecycle();
        let quarantine_established = matches!(
            lifecycle.state(),
            OnboardingState::Blocked | OnboardingState::CleanupRequired
        ) && lifecycle.active_generation().is_none()
            && lifecycle.candidate_generation().is_none()
            && lifecycle.generation_states().all(|(_generation, state)| {
                matches!(
                    state,
                    CredentialGenerationState::CleanupRequired
                        | CredentialGenerationState::Retired
                        | CredentialGenerationState::Tombstoned
                        | CredentialGenerationState::AbandonedNoEffect
                )
            });
        if quarantine_established {
            return Ok(());
        }
        self.append(
            resumed.reservation(),
            resumed.next_sequence(),
            OnboardingEvent::ActivationQuarantined {
                evidence_digest: event_digest(
                    b"startup-activation-quarantined",
                    resumed.reservation().session_id(),
                    lifecycle.candidate_generation(),
                ),
            },
        )
    }

    fn cleanup_startup_unattended(
        &self,
        session_id: Uuid,
        lifecycle_support: Option<LifecycleSupport>,
    ) -> Result<(), ProviderOnboardingError> {
        loop {
            let resumed = self.catalog.resume_provider_onboarding(session_id)?;
            let lifecycle = resumed.lifecycle();
            let target = lifecycle.generation_states().find(|(generation, state)| {
                lifecycle.active_generation() != Some(*generation)
                    && matches!(
                        state,
                        CredentialGenerationState::Reserved
                            | CredentialGenerationState::SupersededRetained
                            | CredentialGenerationState::CleanupRequired
                            | CredentialGenerationState::Retired
                    )
            });
            let Some((generation, state)) = target else {
                if lifecycle.active_generation().is_none()
                    && lifecycle.state() == OnboardingState::Blocked
                    && !lifecycle.cancellation_recorded()
                    && resumed.reservation().deadline_at() <= system_timestamp()?
                {
                    self.append(
                        resumed.reservation(),
                        resumed.next_sequence(),
                        OnboardingEvent::Cancelled {
                            evidence_digest: event_digest(
                                b"startup-expired-cleanup-complete",
                                session_id,
                                None,
                            ),
                        },
                    )?;
                    continue;
                }
                return Ok(());
            };
            if state == CredentialGenerationState::Reserved {
                if lifecycle.candidate_generation() != Some(generation) {
                    return Ok(());
                }
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::CandidateCancelledNoEffect {
                        generation,
                        evidence_digest: event_digest(
                            b"startup-cancelled-no-effect",
                            session_id,
                            Some(generation),
                        ),
                    },
                )?;
                continue;
            }
            if state == CredentialGenerationState::Retired {
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::Tombstone { generation },
                )?;
                continue;
            }
            let deadline = Instant::now()
                .checked_add(SECRET_OPERATION_DURATION)
                .ok_or(ProviderOnboardingError::Clock)?;
            let control = SecretOperationControl::try_new(
                format!("provider-startup-cleanup-{session_id}-{}", generation.get()),
                deadline,
                0,
                SecretInteractionPolicy::Forbid,
                SecretCancellation::new(),
            )?;
            if let Some(plan) = lifecycle.generation_store_plan(generation).cloned()
                && lifecycle.generation_reference(generation).is_none()
            {
                let key = SecretKey::try_new(
                    "provider-onboarding",
                    &format!("{}.{}", lifecycle.surface_id(), session_id.simple()),
                )?;
                let outcome = match self.secrets.delete_planned(&key, &plan, &control) {
                    Ok(SecretDeletionDisposition::Deleted) => SecretStoreClearOutcome::Deleted,
                    Ok(SecretDeletionDisposition::AlreadyAbsent) => SecretStoreClearOutcome::Absent,
                    Err(_) => return Ok(()),
                };
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::SecretStoreCleared {
                        generation,
                        reference: plan.target().clone(),
                        outcome,
                    },
                )?;
                continue;
            }
            if lifecycle_support.is_none()
                && !matches!(
                    lifecycle.generation_local_deletion(generation),
                    Some(LocalDeletionOutcome::Deleted | LocalDeletionOutcome::NotFound)
                )
            {
                let Some(reference) = lifecycle.generation_cleanup_reference(generation) else {
                    return Ok(());
                };
                let outcome = match self.secrets.delete(reference, &control) {
                    Ok(()) => LocalDeletionOutcome::Deleted,
                    Err(LocalSecretStoreError::NotFound) => LocalDeletionOutcome::NotFound,
                    Err(_) => return Ok(()),
                };
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::LocalDeletion {
                        generation,
                        outcome,
                    },
                )?;
                continue;
            }
            if lifecycle.generation_remote_revocation(generation).is_none() {
                let Some(lifecycle_support) = lifecycle_support else {
                    return Ok(());
                };
                if lifecycle_support.remote_revocation() {
                    return Ok(());
                }
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::RemoteRevocation {
                        generation,
                        outcome: RemoteRevocationOutcome::Unsupported,
                        evidence_digest: event_digest(
                            b"startup-remote-revocation-unsupported",
                            session_id,
                            Some(generation),
                        ),
                    },
                )?;
                continue;
            }
            if startup_remote_cleanup_unresolved(lifecycle.generation_remote_revocation(generation))
            {
                return Ok(());
            }
            if !matches!(
                lifecycle.generation_local_deletion(generation),
                Some(LocalDeletionOutcome::Deleted | LocalDeletionOutcome::NotFound)
            ) {
                let Some(reference) = lifecycle.generation_cleanup_reference(generation) else {
                    return Ok(());
                };
                let outcome = match self.secrets.delete(reference, &control) {
                    Ok(()) => LocalDeletionOutcome::Deleted,
                    Err(LocalSecretStoreError::NotFound) => LocalDeletionOutcome::NotFound,
                    Err(_) => return Ok(()),
                };
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::LocalDeletion {
                        generation,
                        outcome,
                    },
                )?;
                continue;
            }
            self.append(
                resumed.reservation(),
                resumed.next_sequence(),
                OnboardingEvent::Retire { generation },
            )?;
        }
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

    pub(super) async fn cleanup_superseded_unlocked(
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
            if let Some(plan) = lifecycle.generation_store_plan(generation).cloned()
                && lifecycle.generation_reference(generation).is_none()
            {
                let key = SecretKey::try_new(
                    "provider-onboarding",
                    &format!("{}.{}", profile.id(), session_id.simple()),
                )?;
                let secrets = Arc::clone(&self.secrets);
                let target = plan.target().clone();
                let outcome = await_blocking_secret_operation(
                    Arc::clone(&self.secret_operations),
                    cancellation.clone(),
                    move |operation| {
                        let deadline = Instant::now()
                            .checked_add(SECRET_OPERATION_DURATION)
                            .ok_or(ProviderOnboardingError::Clock)?;
                        let control = SecretOperationControl::try_new(
                            format!("provider-planned-cleanup-{session_id}-{}", generation.get()),
                            deadline,
                            0,
                            SecretInteractionPolicy::AllowPlatformPrompt,
                            operation,
                        )?;
                        match secrets.delete_planned(&key, &plan, &control) {
                            Ok(SecretDeletionDisposition::Deleted) => {
                                Ok(SecretStoreClearOutcome::Deleted)
                            }
                            Ok(SecretDeletionDisposition::AlreadyAbsent) => {
                                Ok(SecretStoreClearOutcome::Absent)
                            }
                            Err(failure) => Err(failure.into_error().into()),
                        }
                    },
                )
                .await?;
                self.append(
                    resumed.reservation(),
                    resumed.next_sequence(),
                    OnboardingEvent::SecretStoreCleared {
                        generation,
                        reference: target,
                        outcome,
                    },
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

const fn startup_remote_cleanup_unresolved(outcome: Option<RemoteRevocationOutcome>) -> bool {
    matches!(
        outcome,
        Some(RemoteRevocationOutcome::Failed | RemoteRevocationOutcome::Indeterminate)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_unresolved_remote_revocation_stops_cleanup() {
        for outcome in [
            RemoteRevocationOutcome::Failed,
            RemoteRevocationOutcome::Indeterminate,
        ] {
            assert!(startup_remote_cleanup_unresolved(Some(outcome)));
        }
        assert!(!startup_remote_cleanup_unresolved(Some(
            RemoteRevocationOutcome::Confirmed
        )));
    }
}
