//! Finite public reference preparation outside the shared lifecycle mutation authority.

use super::super::startup::{ProductStartupTasks, PublicPreparationExclusion};
use super::*;

impl ProductionSourceLifecycleAuthority {
    pub(crate) fn bind_public_startup_tasks(
        self: &Arc<Self>,
        owner: Weak<ProductStartupTasks>,
    ) -> Result<(), SourceLifecycleError> {
        let retained = owner.upgrade().ok_or(SourceLifecycleError::Unavailable)?;
        retained.bind_public_lifecycle(Arc::downgrade(self))?;
        self.public_startup_tasks
            .set(owner)
            .map_err(|_| SourceLifecycleError::Conflict)
    }

    pub(super) async fn exclude_public_preparation(
        &self,
        provider: Option<&SourceIdentifier>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<PublicPreparationExclusion>, SourceLifecycleError> {
        match self.public_startup_tasks.get().and_then(Weak::upgrade) {
            Some(owner) => owner
                .exclude_public_preparation(provider, deadline, cancellation)
                .await
                .map(Some)
                .map_err(map_live_error),
            None => Ok(None),
        }
    }

    /// The retained startup owner calls this once for one saved public source intent.
    pub(crate) async fn prepare_configured_public_source(
        &self,
        provider: SourceIdentifier,
        cancellation: CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let deadline = public_operation_deadline()?;
        let (original, session, configuration) = {
            self.credential_access.ensure_resumed()?;
            let _gate = self
                .lifecycle_gate_before(provider.as_str(), deadline, &cancellation)
                .await?;
            self.credential_access.ensure_resumed()?;
            let original = self
                .durable
                .source_lifecycle_record(provider.as_str())
                .map_err(map_durable_error)?;
            if !self.live.is_account_free_source_configured(&provider)
                || !public_source_desired(&provider, &original)?
            {
                return Ok(());
            }
            let target = match (
                original.session_id(),
                original.public_configuration_digest(),
            ) {
                (Some(session), Some(configuration)) => (session, configuration),
                (None, None) if original.phase() == DurableSourceLifecyclePhase::Stopped => {
                    match self
                        .onboarding
                        .current_runtime_activation_target(&provider)
                        .map_err(map_onboarding_error)?
                    {
                        Some(target) => target,
                        None => {
                            let view = self
                                .onboarding
                                .start_deferred(
                                    crate::StartOnboardingRequest::try_new(
                                        provider.as_str(),
                                        None,
                                        None,
                                    )
                                    .map_err(map_onboarding_error)?,
                                )
                                .map_err(map_onboarding_error)?;
                            let configuration = self
                                .onboarding
                                .runtime_activation_target_public_configuration(
                                    view.session_id(),
                                    &provider,
                                )
                                .map_err(map_onboarding_error)?;
                            (view.session_id(), configuration)
                        }
                    }
                }
                _ => return Err(SourceLifecycleError::InvalidResult),
            };
            (original, target.0, target.1)
        };
        // Activation may perform the anonymous doctor request. No shared mutation guard is held.
        let activation_cancellation = cancellation.child_token();
        let activation = self
            .onboarding
            .activate(session, activation_cancellation.clone());
        tokio::pin!(activation);
        let lease = tokio::select! { biased;
            () = cancellation.cancelled() => {
                activation_cancellation.cancel();
                let _drained = activation.await;
                return Err(SourceLifecycleError::Cancelled);
            },
            () = tokio::time::sleep_until(deadline.into()) => {
                activation_cancellation.cancel();
                let _drained = activation.await;
                return Err(SourceLifecycleError::DeadlineExceeded);
            },
            result = &mut activation => result.map_err(|error| {
                tracing::warn!(provider = provider.as_str(), %error, "public source onboarding activation failed");
                map_onboarding_error(error)
            })?,
        };
        if lease.session_id() != session
            || lease.surface_id() != &provider
            || lease.public_configuration_digest() != configuration
            || lease.generation().is_some()
        {
            return Err(SourceLifecycleError::Conflict);
        }
        loop {
            // Each policy readmission is a new bounded operation, preserving the original intent.
            let deadline = public_operation_deadline()?;
            self.require_public_preparation_current(
                &provider,
                &original,
                session,
                configuration,
                deadline,
                &cancellation,
            )
            .await?;
            match self
                .live
                .prepare_public_start(&provider, session, deadline, &cancellation)
                .await
                .map_err(map_live_error)?
            {
                PublicMarketStartPreparation::Deferred { not_before } => {
                    if not_before <= Instant::now() {
                        return Err(SourceLifecycleError::Unavailable);
                    }
                    tokio::select! { biased;
                        () = cancellation.cancelled() => return Err(SourceLifecycleError::Cancelled),
                        () = tokio::time::sleep_until(not_before.into()) => {},
                    }
                }
                PublicMarketStartPreparation::Ready(prepared) => {
                    let command = if original.phase() == DurableSourceLifecyclePhase::Stopped {
                        SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
                            provider: provider.clone(),
                            action: SourceLifecycleAction::Start,
                            expected_state_revision: original.revision(),
                            expected_generation: None,
                            expected_runtime_generation_digest: None,
                            onboarding_session_id: Some(session),
                            public_configuration_digest: Some(configuration),
                            reason: None,
                            cancellation: cancellation.child_token(),
                            deadline,
                        })?
                    } else {
                        saved_source_retry_command(
                            provider.clone(),
                            original.revision(),
                            "automatic-public-source-recovery",
                            deadline,
                            cancellation.child_token(),
                        )?
                    };
                    self.execute_prepared_owned(&command, Some(&original), Some(prepared))
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    async fn require_public_preparation_current(
        &self,
        provider: &SourceIdentifier,
        original: &DurableSourceLifecycleRecord,
        session: uuid::Uuid,
        configuration: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        self.credential_access.ensure_resumed()?;
        let _gate = self
            .lifecycle_gate_before(provider.as_str(), deadline, cancellation)
            .await?;
        self.credential_access.ensure_resumed()?;
        let current = self
            .durable
            .source_lifecycle_record(provider.as_str())
            .map_err(map_durable_error)?;
        if &current != original
            || self
                .onboarding
                .runtime_activation_target_public_configuration(session, provider)
                .map_err(map_onboarding_error)?
                != configuration
        {
            return Err(SourceLifecycleError::Conflict);
        }
        Ok(())
    }

    pub(crate) async fn execute_public_command_owned(
        &self,
        command: &SourceLifecycleCommand,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError> {
        let (expected, prepared) = self.prepare_public_command(command).await?;
        self.execute_prepared_owned(command, expected.as_ref(), prepared)
            .await
    }

    pub(super) async fn drain_uncommitted_public(
        &self,
        provider: &SourceIdentifier,
    ) -> Result<(), SourceLifecycleError> {
        let deadline = self.live.cleanup_deadline().map_err(map_live_error)?;
        self.live
            .stop(provider, None, deadline, &CancellationToken::new())
            .await
            .map_err(map_live_error)?;
        Ok(())
    }

    // Keep public preparation's full runtime state out of account-command dispatch.
    #[inline(never)]
    pub(super) fn prepare_public_command<'a>(
        &'a self,
        command: &'a SourceLifecycleCommand,
    ) -> Pin<
        Box<
            impl Future<
                Output = Result<
                    (
                        Option<DurableSourceLifecycleRecord>,
                        Option<Box<PreparedPublicMarketStart>>,
                    ),
                    SourceLifecycleError,
                >,
            > + Send
            + 'a,
        >,
    > {
        Box::pin(self.prepare_public_command_inner(command))
    }

    async fn prepare_public_command_inner(
        &self,
        command: &SourceLifecycleCommand,
    ) -> Result<
        (
            Option<DurableSourceLifecycleRecord>,
            Option<Box<PreparedPublicMarketStart>>,
        ),
        SourceLifecycleError,
    > {
        if !PUBLIC_LIVE_SURFACES.contains(&command.provider().as_str())
            || !public_action_prepares(command.action())
        {
            return Ok((None, None));
        }
        let (original, session, configuration) = {
            ensure_live(command)?;
            self.credential_access.ensure_resumed()?;
            let _gate = self
                .lifecycle_gate_before(
                    command.provider().as_str(),
                    command.deadline(),
                    command.cancellation(),
                )
                .await?;
            let original = self
                .durable
                .source_lifecycle_record(command.provider().as_str())
                .map_err(map_durable_error)?;
            if original.revision() != command.expected_state_revision() {
                return Err(SourceLifecycleError::Conflict);
            }
            self.preflight_runtime_lease(command, &original)?;
            let (session, configuration) = self.lifecycle_transition_target(command, &original)?;
            (
                original,
                session.ok_or(SourceLifecycleError::Unauthorized)?,
                configuration.ok_or(SourceLifecycleError::Unauthorized)?,
            )
        };
        loop {
            self.require_public_preparation_current(
                command.provider(),
                &original,
                session,
                configuration,
                command.deadline(),
                command.cancellation(),
            )
            .await?;
            match self
                .live
                .prepare_public_start(
                    command.provider(),
                    session,
                    command.deadline(),
                    command.cancellation(),
                )
                .await
                .map_err(map_live_error)?
            {
                PublicMarketStartPreparation::Ready(prepared) => {
                    return Ok((Some(original), Some(prepared)));
                }
                PublicMarketStartPreparation::Deferred { not_before } => {
                    if not_before <= Instant::now() {
                        return Err(SourceLifecycleError::Unavailable);
                    }
                    tokio::select! { biased;
                        () = command.cancellation().cancelled() => return Err(SourceLifecycleError::Cancelled),
                        () = tokio::time::sleep_until(command.deadline().into()) => return Err(SourceLifecycleError::DeadlineExceeded),
                        () = tokio::time::sleep_until(not_before.into()) => {},
                    }
                }
            }
        }
    }
}

fn public_operation_deadline() -> Result<Instant, SourceLifecycleError> {
    Instant::now()
        .checked_add(super::super::LOCAL_RECOVERY_TIMEOUT)
        .ok_or(SourceLifecycleError::Internal)
}

fn public_source_desired(
    provider: &SourceIdentifier,
    record: &DurableSourceLifecycleRecord,
) -> Result<bool, SourceLifecycleError> {
    Ok(match record.phase() {
        DurableSourceLifecyclePhase::Active => true,
        DurableSourceLifecyclePhase::Stopped => {
            record.revision() == NonZeroU64::MIN && record.operation_id().is_none()
        }
        DurableSourceLifecyclePhase::Applying
        | DurableSourceLifecyclePhase::ReconciliationRequired => {
            is_default_public_recovery(provider, record)?
        }
        DurableSourceLifecyclePhase::Removed => false,
    })
}

pub(super) fn public_action_prepares(action: SourceLifecycleAction) -> bool {
    matches!(
        action,
        SourceLifecycleAction::Start
            | SourceLifecycleAction::Retry
            | SourceLifecycleAction::Resynchronize
            | SourceLifecycleAction::Reconfigure
    )
}
