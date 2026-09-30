//! Disconnect recovery retains the existing durable transition and credential renewal owners.

use super::*;
use crate::application::AccountMarketRuntimeReconnect;
use crate::provider_onboarding::{ProviderPortalActivationError, SchwabOAuthLifecycleAction};
use market_squawk_services::ServiceError;

#[async_trait]
impl AccountMarketRuntimeReconnect for ProductionSourceLifecycleAuthority {
    async fn reconnect_public(
        &self,
        provider: SourceIdentifier,
        session: uuid::Uuid,
        incarnation: uuid::Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError> {
        if !PUBLIC_LIVE_SURFACES.contains(&provider.as_str()) {
            return Err(ServiceError::InvalidRequest);
        }
        // Reuse the installed application's existing local recovery operation bound.
        let deadline = Instant::now()
            .checked_add(super::super::LOCAL_RECOVERY_TIMEOUT)
            .ok_or(ServiceError::Unavailable)?;
        let (original, delay) = {
            let _gate = self
                .lifecycle_gate_before(provider.as_str(), deadline, &cancellation)
                .await
                .map_err(reconnect_error)?;
            let original = self
                .durable
                .source_lifecycle_record(provider.as_str())
                .map_err(|error| reconnect_error(map_durable_error(error)))?;
            if original.phase() != DurableSourceLifecyclePhase::Active
                || original.session_id() != Some(session)
            {
                return Ok(());
            }
            let Some(delay) = self
                .live
                .prepare_public_recovery(&provider, session, incarnation, deadline, &cancellation)
                .await?
            else {
                return Ok(());
            };
            (original, delay)
        };
        // No authority lock spans the delay. A user stop/removal/replacement wins via the
        // original durable revision CAS when execute_owned reacquires the lifecycle gate.
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            () = tokio::time::sleep(delay) => {}
        }
        let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider,
            action: SourceLifecycleAction::Retry,
            expected_state_revision: original.revision(),
            expected_generation: None,
            expected_runtime_generation_digest: None,
            onboarding_session_id: None,
            public_configuration_digest: None,
            reason: Some(
                SourceIdentifier::try_from("public-catalog-selection-stale")
                    .map_err(|_| ServiceError::Internal)?,
            ),
            cancellation,
            deadline,
        })
        .map_err(reconnect_error)?;
        // One successor attempt follows the exact stale terminal. Existing reference preparation
        // physically reopens unchanged originals, selects current catalog authority, and rebuilds
        // routes. It does not write a replacement revision for an unchanged accepted identity.
        self.execute_owned(&command)
            .await
            .map_err(reconnect_error)?;
        Ok(())
    }

    async fn has_pending(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<bool, ServiceError> {
        let surface = AccountMarketSurface::SchwabMarketData;
        let _gate = self
            .lifecycle_gate_before(surface.surface_id(), deadline, &cancellation)
            .await
            .map_err(reconnect_error)?;
        let record = match self.durable.source_lifecycle_record(surface.surface_id()) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(surface = surface.surface_id(), %error, "account reconnect state is unavailable; source remains disabled");
                return Ok(false);
            }
        };
        Ok(is_pending_reconnect(&record))
    }

    async fn resume_pending(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError> {
        let surface = AccountMarketSurface::SchwabMarketData;
        let original = {
            let _gate = self
                .lifecycle_gate_before(surface.surface_id(), deadline, &cancellation)
                .await
                .map_err(reconnect_error)?;
            let record = self
                .durable
                .source_lifecycle_record(surface.surface_id())
                .map_err(|error| reconnect_error(map_durable_error(error)))?;
            if !is_pending_reconnect(&record) {
                return Ok(());
            }
            record
        };
        let session = original
            .account()
            .and_then(|pending| pending.target_session_id)
            .ok_or(ServiceError::InvalidResult)?;
        self.continue_schwab_reconnect_authority(session, deadline, &cancellation)
            .await?;
        let _gate = self
            .lifecycle_gate_before(surface.surface_id(), deadline, &cancellation)
            .await
            .map_err(reconnect_error)?;
        let record = self
            .durable
            .source_lifecycle_record(surface.surface_id())
            .map_err(|error| reconnect_error(map_durable_error(error)))?;
        if record != original {
            return Ok(());
        }
        // Equality includes revision, action, original predecessor, target, partial successor,
        // credential and verification receipt. Resume this exact record after renewal.
        self.continue_account_transition(record, surface, deadline, &cancellation, true, false)
            .await
            .map_err(reconnect_error)?;
        Ok(())
    }

    async fn reconnect(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        generation: MarketRuntimeGroupGeneration,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError> {
        let surface = AccountMarketSurface::SchwabMarketData;
        if request.surface() != surface {
            return Err(ServiceError::InvalidRequest);
        }
        let original = {
            let _gate = self
                .lifecycle_gate_before(surface.surface_id(), deadline, &cancellation)
                .await
                .map_err(reconnect_error)?;
            let record = self
                .durable
                .source_lifecycle_record(surface.surface_id())
                .map_err(|error| reconnect_error(map_durable_error(error)))?;
            if record.account().is_some_and(|pending| !pending.finished)
                || record.phase() != DurableSourceLifecyclePhase::Active
                || record.session_id() != Some(request.onboarding_session_id())
                || record.public_configuration_digest()
                    != Some(request.expected_public_configuration_digest())
                || record.runtime_verification_receipt_digest()
                    != Some(request.expected_runtime_verification_receipt_digest())
                || record.credential_generation() != Some(request.expected_credential_generation())
            {
                return Ok(());
            }
            record
        };
        self.continue_schwab_reconnect_authority(
            request.onboarding_session_id(),
            deadline,
            &cancellation,
        )
        .await?;
        let _gate = self
            .lifecycle_gate_before(surface.surface_id(), deadline, &cancellation)
            .await
            .map_err(reconnect_error)?;
        let record = self
            .durable
            .source_lifecycle_record(surface.surface_id())
            .map_err(|error| reconnect_error(map_durable_error(error)))?;
        if record != original {
            // A concurrent stop, removal or replacement wins; automatic recovery cannot undo it.
            return Ok(());
        }
        let command = SourceLifecycleCommand::try_new(SourceLifecycleCommandInput {
            provider: SourceIdentifier::try_from(surface.surface_id())
                .map_err(|_| ServiceError::Internal)?,
            action: SourceLifecycleAction::Resynchronize,
            expected_state_revision: record.revision(),
            expected_generation: None,
            expected_runtime_generation_digest: Some(generation.digest()),
            onboarding_session_id: None,
            public_configuration_digest: None,
            reason: Some(
                SourceIdentifier::try_from("schwab-disconnected-generation")
                    .map_err(|_| ServiceError::Internal)?,
            ),
            cancellation,
            deadline,
        })
        .map_err(reconnect_error)?;
        let digest = command_digest(&command).map_err(reconnect_error)?;
        let operation = operation_id(digest).map_err(reconnect_error)?;
        // The existing transition observes the predecessor before durable CAS, joins it,
        // acknowledges its real receipt, and derives the successor from the current doctor lease.
        self.execute_account_transition(&command, surface, record, digest, operation)
            .await
            .map_err(reconnect_error)?;
        Ok(())
    }
}

impl ProductionSourceLifecycleAuthority {
    async fn continue_schwab_reconnect_authority(
        &self,
        session: uuid::Uuid,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        ensure_status_live(cancellation, deadline).map_err(reconnect_error)?;
        // OAuth may call the original lifecycle drain. Never hold that gate while invoking
        // Continue. The portal retains and coalesces its own doctor task, including deferred
        // exclusive expiry; this call grants no bypass around the successor's current lease.
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => Err(ServiceError::DeadlineExceeded),
            result = self.portal.schwab_oauth(session, SchwabOAuthLifecycleAction::Continue, cancellation.child_token()) => {
                result.map(|_| ()).map_err(|error| match error {
                    ProviderPortalActivationError::Cancelled => ServiceError::Cancelled,
                    ProviderPortalActivationError::DeadlineExceeded => ServiceError::DeadlineExceeded,
                    ProviderPortalActivationError::InvalidRequest => ServiceError::InvalidRequest,
                    ProviderPortalActivationError::Internal => ServiceError::Internal,
                    ProviderPortalActivationError::Unavailable | ProviderPortalActivationError::StateUnavailable => ServiceError::Unavailable,
                })
            }
        }
    }
}

fn reconnect_error(error: SourceLifecycleError) -> ServiceError {
    match error {
        SourceLifecycleError::Cancelled => ServiceError::Cancelled,
        SourceLifecycleError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        SourceLifecycleError::InvalidRequest => ServiceError::InvalidRequest,
        SourceLifecycleError::InvalidResult => ServiceError::InvalidResult,
        SourceLifecycleError::Unauthorized => ServiceError::Unauthorized,
        SourceLifecycleError::Internal => ServiceError::Internal,
        _ => ServiceError::Unavailable,
    }
}

fn is_pending_reconnect(record: &DurableSourceLifecycleRecord) -> bool {
    record.account().is_some_and(|pending| {
        !pending.finished
            && matches!(
                pending.action,
                AccountLifecycleAction::Retry | AccountLifecycleAction::Resynchronize
            )
    })
}
