//! Reversible credential suspension; saved source choices and catalog reads remain available.

use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
pub(super) struct CredentialRuntimeAccess {
    suspended: AtomicBool,
    resume_pending: AtomicBool,
    closing: AtomicBool,
    operation: tokio::sync::Mutex<()>,
    mutation: Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>,
}

impl CredentialRuntimeAccess {
    pub(super) async fn operation_before(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<tokio::sync::MutexGuard<'_, ()>, SourceLifecycleError> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(SourceLifecycleError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => Err(SourceLifecycleError::DeadlineExceeded),
            operation = self.operation.lock() => Ok(operation),
        }
    }

    pub(super) fn owns_mutation(&self) -> Result<bool, SourceLifecycleError> {
        Ok(self
            .mutation
            .lock()
            .map_err(|_| SourceLifecycleError::Internal)?
            .is_some())
    }

    pub(super) fn ensure_resumed(&self) -> Result<(), SourceLifecycleError> {
        if self.suspended.load(Ordering::Acquire) {
            Err(SourceLifecycleError::Unavailable)
        } else {
            Ok(())
        }
    }

    pub(super) async fn release_for_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), SourceLifecycleError> {
        self.closing.store(true, Ordering::Release);
        self.suspended.store(true, Ordering::Release);
        let _operation = tokio::time::timeout_at(deadline.into(), self.operation.lock())
            .await
            .map_err(|_| SourceLifecycleError::DeadlineExceeded)?;
        self.mutation
            .lock()
            .map_err(|_| SourceLifecycleError::Internal)?
            .take();
        Ok(())
    }
}

impl ProductionSourceLifecycleAuthority {
    pub(super) async fn suspend_credentials_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let _operation = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(SourceLifecycleError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(SourceLifecycleError::DeadlineExceeded),
            operation = self.credential_access.operation.lock() => operation,
        };
        if self.credential_access.closing.load(Ordering::Acquire) {
            return Err(SourceLifecycleError::Unavailable);
        }
        self.credential_access
            .resume_pending
            .store(true, Ordering::Release);
        self.credential_access
            .suspended
            .store(true, Ordering::Release);
        let has_gate = self
            .credential_access
            .mutation
            .lock()
            .map_err(|_| SourceLifecycleError::Internal)?
            .is_some();
        if !has_gate {
            let gate = self
                .lifecycle_gate_before(
                    ProviderMarketAccount::AlpacaBasic.surface_id(),
                    deadline,
                    cancellation,
                )
                .await?;
            *self
                .credential_access
                .mutation
                .lock()
                .map_err(|_| SourceLifecycleError::Internal)? = Some(gate);
        }

        // Keep the existing activation gate even on failure: another Lock retries retained drains,
        // while no concurrent source or portal activation can create a replacement with secrets.
        let mut failure = None;
        for surface_id in LIVE_SURFACES {
            if PUBLIC_LIVE_SURFACES.contains(&surface_id) {
                continue;
            }
            let result = self
                .suspend_live_credentials(surface_id, deadline, cancellation)
                .await;
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        if self
            .activation
            .suspend_credential_research_runtimes(deadline, cancellation)
            .await
            .is_err()
        {
            failure.get_or_insert(SourceLifecycleError::Unavailable);
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) async fn suspend_live_credentials(
        &self,
        surface_id: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        let provider = SourceIdentifier::try_from(surface_id)
            .map_err(|_| SourceLifecycleError::InvalidResult)?;
        let Some(surface) = AccountMarketSurface::parse(surface_id) else {
            self.live
                .stop(&provider, None, deadline, cancellation)
                .await
                .map_err(map_live_error)?;
            return Ok(());
        };
        let mut record = self
            .durable
            .source_lifecycle_record(surface_id)
            .map_err(map_durable_error)?;
        if record.account().is_some_and(|pending| !pending.finished) {
            record = self
                .continue_account_transition(record, surface, deadline, cancellation, false, false)
                .await?;
        }
        let prepared = self
            .live
            .prepare_account_stop(surface, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        let predecessor = prepared.predecessor();
        let receipt = self
            .live
            .consume_account_stop(prepared, deadline, cancellation)
            .await
            .map_err(map_live_error)?;
        if let Some(receipt) = receipt {
            self.live
                .acknowledge_account_group_stop(
                    &receipt,
                    |receipt| {
                        if predecessor != Some((receipt.request(), receipt.generation())) {
                            return Err(market_squawk_services::ServiceError::InvalidResult);
                        }
                        let current = self
                            .durable
                            .source_lifecycle_record(surface_id)
                            .map_err(|_| market_squawk_services::ServiceError::Unavailable)?;
                        if current.revision() != record.revision()
                            || current.command_digest() != record.command_digest()
                            || current.phase() != record.phase()
                            || current.session_id() != record.session_id()
                            || current.public_configuration_digest()
                                != record.public_configuration_digest()
                            || current.runtime_verification_receipt_digest()
                                != record.runtime_verification_receipt_digest()
                            || current.credential_generation() != record.credential_generation()
                        {
                            return Err(market_squawk_services::ServiceError::Unavailable);
                        }
                        // This is process suspension, not a change to the user's durable desired state.
                        Ok(())
                    },
                    deadline,
                    cancellation,
                )
                .await
                .map_err(map_live_error)?;
        } else if predecessor.is_some() {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(())
    }

    pub(super) async fn resume_credentials_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let _operation = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(SourceLifecycleError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(SourceLifecycleError::DeadlineExceeded),
            operation = self.credential_access.operation.lock() => operation,
        };
        if self.credential_access.closing.load(Ordering::Acquire) {
            return Err(SourceLifecycleError::Unavailable);
        }
        if !self.credential_access.suspended.load(Ordering::Acquire)
            && !self
                .credential_access
                .resume_pending
                .load(Ordering::Acquire)
        {
            return Ok(());
        }
        if self
            .onboarding
            .credential_access_status()
            .map_err(map_onboarding_error)?
            .access
            != market_squawk_platform::SecretAccessState::Ready
        {
            return Err(SourceLifecycleError::Unavailable);
        }
        self.credential_access
            .resume_pending
            .store(true, Ordering::Release);
        self.credential_access
            .mutation
            .lock()
            .map_err(|_| SourceLifecycleError::Internal)?
            .take();
        self.credential_access
            .suspended
            .store(false, Ordering::Release);

        // Reopening OAuth while the source gate is retained would strand its next drain if this
        // waiter were interrupted. Release first, then restore through the existing portal owner.
        let portal = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(SourceLifecycleError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(SourceLifecycleError::DeadlineExceeded),
            result = self.portal.resume_credential_access() => result,
        };
        portal.map_err(|error| match error {
            crate::ProviderPortalActivationError::Cancelled => SourceLifecycleError::Cancelled,
            crate::ProviderPortalActivationError::DeadlineExceeded => {
                SourceLifecycleError::DeadlineExceeded
            }
            _ => SourceLifecycleError::Unavailable,
        })?;
        self.restore_ready_research_sources_owned(deadline, cancellation)
            .await?;
        let report = self
            .restore_active_live_sources(deadline, cancellation)
            .await?;
        ensure_status_live(cancellation, deadline)?;
        for item in report.failures() {
            if matches!(
                item.error(),
                SourceLifecycleError::Cancelled | SourceLifecycleError::DeadlineExceeded
            ) {
                return Err(item.error());
            }
            tracing::warn!(provider = %item.provider().as_str(), error = %item.error(),
                "credential access restored; live connection requires recovery");
        }
        self.credential_access
            .resume_pending
            .store(false, Ordering::Release);
        Ok(())
    }

    /// Startup restores only desired, absent research runtimes when saved access is ready.
    pub(crate) async fn restore_ready_research_sources(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        let _operation = self
            .credential_access
            .operation_before(deadline, cancellation)
            .await?;
        self.credential_access.ensure_resumed()?;
        self.restore_ready_research_sources_owned(deadline, cancellation)
            .await
    }

    async fn restore_ready_research_sources_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceLifecycleError> {
        ensure_status_live(cancellation, deadline)?;
        if self
            .onboarding
            .credential_access_status()
            .map_err(map_onboarding_error)?
            .access
            != market_squawk_platform::SecretAccessState::Ready
        {
            return Ok(());
        }
        for surface_id in super::super::provider_activation_state::RESTORABLE_RESEARCH_SURFACES {
            let provider = SourceIdentifier::try_from(surface_id)
                .map_err(|_| SourceLifecycleError::InvalidResult)?;
            let gate = self
                .lifecycle_gate_before(surface_id, deadline, cancellation)
                .await?;
            let result = async {
                let record = self
                    .durable
                    .source_lifecycle_record(surface_id)
                    .map_err(map_durable_error)?;
                if record.phase() != DurableSourceLifecyclePhase::Active
                    || self
                        .activation
                        .research_runtime_generation(&provider)
                        .map_err(|_| SourceLifecycleError::Unavailable)?
                        .is_some()
                {
                    return Ok(());
                }
                let Some(recipe) = self.retained_recipe(surface_id)? else {
                    return Ok(());
                };
                cli_provider::resume_exact_research_provider(
                    &self.paths,
                    &self.onboarding,
                    &self.activation,
                    &self.durable,
                    surface_id,
                    recipe.session_id,
                    cancellation.child_token(),
                    deadline,
                )
                .await
                .map_err(|error| match error {
                    cli_provider::CliProviderActivationError::Cancelled => {
                        SourceLifecycleError::Cancelled
                    }
                    cli_provider::CliProviderActivationError::Onboarding(error) => {
                        map_onboarding_error(error)
                    }
                    _ => SourceLifecycleError::Unavailable,
                })?;
                Ok::<_, SourceLifecycleError>(())
            }
            .await;
            drop(gate);
            ensure_status_live(cancellation, deadline)?;
            if let Err(error) = result {
                if matches!(
                    error,
                    SourceLifecycleError::Cancelled | SourceLifecycleError::DeadlineExceeded
                ) {
                    return Err(error);
                }
                tracing::warn!(provider = %provider.as_str(), %error,
                    "credential access restored; research connection requires recovery");
            }
        }
        Ok(())
    }
}
