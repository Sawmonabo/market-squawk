//! Bounded source-native MarketHours demand through the existing account and research owners.
mod metadata;

use super::{ProviderAdapterActivation, SchwabMarketDataAccountActivation};
use crate::application::{ResearchProviderRuntimeGeneration, ResearchRightsAuthority};
use chrono::NaiveDate;
use market_squawk_adapter_schwab::{
    AccessTokenAdmission, MarketId, ParseBounds, RequestAdmission, RestExecutionOutcome,
    RestTransportBounds, SchwabRestExecutor, SchwabTransportTelemetry, build_market_hours_request,
};
use market_squawk_data::CommittedDataset;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetReservationDecision, ProviderRateDeclaration,
    apply_http_retry_after,
};
use sha2::{Digest as _, Sha256};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

impl ProviderAdapterActivation {
    /// Reopens original native request semantics after the common store verifies its physical
    /// binding. This is a historical read and neither acquires nor advances account authority.
    pub(crate) async fn verify_market_session_context_reference(
        &self,
        market: MarketId,
        date: NaiveDate,
        manifest: &market_squawk_data::DatasetManifestRef,
        binding_digest: EvidenceDigest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        check_operation(deadline, cancellation)?;
        let request = build_market_hours_request(
            vec![market],
            Some(date),
            RequestAdmission::new(
                NonZeroUsize::new(16 * 1024).ok_or(ServiceError::Internal)?,
                NonZeroUsize::new(5).ok_or(ServiceError::Internal)?,
            ),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let evidence = self
            .research
            .provider_capture_binding_evidence(manifest, binding_digest, deadline, cancellation)
            .await
            .map_err(|_| {
                check_operation(deadline, cancellation)
                    .err()
                    .unwrap_or(ServiceError::InvalidResult)
            })?;
        if evidence.binding_digest() != binding_digest
            || evidence.capture().source_id().as_str() != metadata::SOURCE
            || evidence.capture().dataset().as_str() != metadata::DATASET
        {
            return Err(ServiceError::InvalidResult);
        }
        let bytes = evidence
            .native_lineage()
            .batch_sidecar_semantic_payload()
            .filter(|bytes| bytes.len() <= 1024 * 1024)
            .ok_or(ServiceError::InvalidResult)?;
        // The original native encoder retains the exact request URL in its digest-bound sidecar.
        // Decode only those source fields this read must compare; the common binding validates
        // all retained bytes and physical claims before this source-specific comparison.
        #[derive(serde::Deserialize)]
        struct NativeRequest {
            version: u16,
            family: String,
            request_url: String,
        }
        let native: NativeRequest =
            serde_json::from_slice(bytes).map_err(|_| ServiceError::InvalidResult)?;
        if native.version != 1
            || native.family != metadata::DATASET
            || native.request_url != request.url()
        {
            return Err(ServiceError::InvalidResult);
        }
        check_operation(deadline, cancellation)
    }

    /// Publishes returned native products/dates as an independent MarketCalendar generation.
    /// The caller chooses an actual civil date and product set; omitted products are never closed.
    /// The actual account demand gate serializes registration through revocation and drain.
    pub(crate) async fn publish_schwab_market_hours(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        markets: Vec<MarketId>,
        date: NaiveDate,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), ServiceError> {
        check_operation(deadline, &cancellation)?;
        let _demand = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            guard = activation.market_hours_demand().lock() => guard,
        };
        let nonzero = |value| NonZeroUsize::new(value).ok_or(ServiceError::Internal);
        let request = build_market_hours_request(
            markets,
            Some(date),
            RequestAdmission::new(nonzero(16 * 1024)?, nonzero(5)?),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        activation
            .require_current()
            .await
            .map_err(|_| ServiceError::Unauthorized)?;
        let source = metadata::metadata(activation)?;
        source
            .network_policy()
            .authorize(request.url())
            .map_err(|_| ServiceError::Unauthorized)?;
        let lease = activation.lease();
        let dataset =
            SourceIdentifier::try_from(metadata::DATASET).map_err(|_| ServiceError::Internal)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/schwab-market-hours-rights/v1\0");
        hash.update(lease.rights_decision_digest().bytes());
        hash.update(source.source_id().as_str().as_bytes());
        hash.update(dataset.as_str().as_bytes());
        let rights = ResearchRightsAuthority::try_new_scoped(
            source.source_id().clone(),
            super::provider_research_rights_basis(lease).map_err(|_| ServiceError::Unauthorized)?,
            lease.rights_decision_digest(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            lease.verification_expires_at(),
            vec![dataset],
            super::lease_research_operations(lease),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(metadata::PROFILE).map_err(|_| ServiceError::Internal)?,
            lease.session_id(),
            lease.capability_revision(),
            lease.capability_digest(),
            lease.generation(),
            lease.secret_reference().cloned(),
            lease.authority_effective_at(),
            source,
            rights.clone(),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        {
            let account = activation
                .currentness()
                .try_acquire_publication_authority()
                .map_err(|_| ServiceError::Unauthorized)?;
            account
                .require_current()
                .map_err(|_| ServiceError::Unauthorized)?;
            self.research_mutation
                .register_provider_publication_generation(generation.clone(), rights)
                .map_err(|_| ServiceError::Unavailable)?;
        }
        let result = self
            .acquire_and_publish_schwab_market_hours(
                activation,
                &generation,
                request,
                deadline,
                &cancellation,
            )
            .await;
        // Drain only after transport, finite physical sealing and supervised canonical work settle.
        let drained = self
            .research_mutation
            .revoke_provider_generation(generation.profile(), &generation)
            .await;
        match (result, drained) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(_)) => Err(ServiceError::Unavailable),
            (Ok(committed), Ok(())) => Ok(committed),
        }
    }

    async fn acquire_and_publish_schwab_market_hours(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        generation: &ResearchProviderRuntimeGeneration,
        request: market_squawk_adapter_schwab::ReadOnlyRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), ServiceError> {
        let nonzero = |value| NonZeroUsize::new(value).ok_or(ServiceError::Internal);
        let bounds = RestTransportBounds::try_new(
            Duration::from_secs(5),
            Duration::from_secs(15),
            Duration::from_secs(20),
            nonzero(4 * 1024 * 1024)?,
            nonzero(64)?,
            nonzero(64 * 1024)?,
        )
        .map_err(|_| ServiceError::Internal)?;
        let executor = SchwabRestExecutor::try_production(
            bounds,
            ParseBounds::new(
                nonzero(4 * 1024 * 1024)?,
                nonzero(8 * 1024)?,
                nonzero(256 * 1024)?,
                nonzero(64)?,
                512,
                512 * 1024,
            ),
            AccessTokenAdmission::new(nonzero(16 * 1024)?, Duration::from_secs(60)),
            SchwabTransportTelemetry::default(),
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let (token, epoch) = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            attempt = activation.acquire_publication_attempt() => attempt.map_err(|_| ServiceError::Unauthorized)?,
        };
        let oauth = epoch.receipt();
        let authority = self
            .research_mutation
            .bind_schwab_market_hours_publication_package(
                generation,
                activation.oauth_receipt_currentness(),
                oauth,
            )
            .map_err(|_| ServiceError::Unauthorized)?;
        let policy = activation
            .lease()
            .provider_budget_policy()
            .cloned()
            .ok_or(ServiceError::Unauthorized)?;
        if policy.weighted_window_count() != 0 {
            return Err(ServiceError::Unauthorized);
        }
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            policy,
            activation.account_binding().subject(),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        let budget = self
            .provider_rate
            .register_budget(declaration)
            .map_err(|_| ServiceError::Unavailable)?;
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(value) => value,
            _ => return Err(ServiceError::Unavailable),
        };
        check_operation(deadline, cancellation)?;
        epoch
            .validate_current(oauth)
            .map_err(|_| ServiceError::Unauthorized)?;
        if !activation.currentness().is_active_now() {
            return Err(ServiceError::Unauthorized);
        }
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(value) => value,
            _ => return Err(ServiceError::Unavailable),
        };
        let transport_cancel = authority.cancellation().child_token();
        let outcome = {
            let run = executor.execute(&request, &token, transport_cancel.clone());
            tokio::pin!(run);
            tokio::select! {
                biased;
                () = cancellation.cancelled() => { transport_cancel.cancel(); (&mut run).await }
                () = tokio::time::sleep_until(deadline.into()) => { transport_cancel.cancel(); (&mut run).await }
                result = &mut run => result,
            }.map_err(|_| if cancellation.is_cancelled() { ServiceError::Cancelled } else if Instant::now() >= deadline { ServiceError::DeadlineExceeded } else { ServiceError::Unavailable })?
        };
        drop(token);
        let receipt = match &outcome {
            RestExecutionOutcome::Accepted(response) => response.capture().receipt(),
            RestExecutionOutcome::ProviderRejected(capture)
            | RestExecutionOutcome::InvalidPayload { capture, .. } => capture.receipt(),
            _ => return Err(ServiceError::InvalidResult),
        };
        // Native transport observations settle request enforcement, without granting learned capacity.
        let rate_ok = if receipt.status() == 429 {
            backoff_recorded(apply_http_retry_after(
                &budget,
                receipt
                    .headers()
                    .iter()
                    .find(|h| h.name() == "retry-after")
                    .map(|h| h.value()),
                0,
            ))
        } else if (200..=299).contains(&receipt.status()) {
            budget.record_success().is_ok()
        } else {
            backoff_recorded(budget.apply_refusal(0))
        };
        let cleanup_deadline = Instant::now()
            .checked_add(Duration::from_secs(20))
            .ok_or(ServiceError::Internal)?;
        let sealed = authority.seal_outcome(outcome, cleanup_deadline).await;
        permit.release();
        let sealed = sealed
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::InvalidResult)?;
        check_operation(deadline, cancellation)?;
        if !rate_ok {
            return Err(ServiceError::Unavailable);
        }
        epoch
            .validate_current(oauth)
            .map_err(|_| ServiceError::Unauthorized)?;
        let account = activation
            .currentness()
            .try_acquire_publication_authority()
            .map_err(|_| ServiceError::Unauthorized)?;
        account
            .require_current()
            .map_err(|_| ServiceError::Unauthorized)?;
        authority
            .publish(
                sealed,
                epoch,
                account,
                std::num::NonZeroU64::new(32 * 1024 * 1024).ok_or(ServiceError::Internal)?,
                cancellation.child_token(),
                deadline,
            )
            .await
            .map(|receipt| (receipt.committed, receipt.binding_digest))
            .map_err(|_| {
                if cancellation.is_cancelled() {
                    ServiceError::Cancelled
                } else if Instant::now() >= deadline {
                    ServiceError::DeadlineExceeded
                } else {
                    ServiceError::Unavailable
                }
            })
    }
}
fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn backoff_recorded(decision: market_squawk_sources::BudgetDecision) -> bool {
    match decision {
        market_squawk_sources::BudgetDecision::WaitUntil(_) => true,
        market_squawk_sources::BudgetDecision::Unavailable(_) => false,
        market_squawk_sources::BudgetDecision::Ready(permit) => {
            permit.release();
            false
        }
    }
}
