//! Actual bounded Instruments acquisition before canonical identity and quote startup.

mod metadata;

use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ParseBounds, RequestAdmission, RestExecutionOutcome, RestTransportBounds,
    SchwabRestExecutor, SchwabTransportTelemetry, build_instrument_by_cusip_request,
};
use market_squawk_data::{ListingReferenceRecord, MarketDataInstrumentRecord, OfficialIssuerInstrumentReference};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetReservationDecision, ProviderRateDeclaration,
    apply_http_retry_after,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{ProviderAdapterActivation, SchwabMarketDataAccountActivation};
use crate::application::{ResearchProviderRuntimeGeneration, ResearchRightsAuthority};

impl ProviderAdapterActivation {
    /// Acquires one exact selected issuer reference and commits it through the existing catalog
    /// identity owner. The account is borrowed so the same sole owner can later start quotes.
    #[allow(
        clippy::too_many_arguments,
        reason = "each source and publication authority stays explicit"
    )]
    pub(crate) async fn publish_schwab_instrument_reference(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        issuer: &OfficialIssuerInstrumentReference,
        official_listing: ListingReferenceRecord,
        expected_current: Option<MarketDataInstrumentRecord>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
        check_operation(deadline, &cancellation)?;
        issuer.validate_listing(&official_listing, metadata::timestamp()?).map_err(|_| ServiceError::Unavailable)?;
        activation
            .require_current()
            .await
            .map_err(|_| ServiceError::Unauthorized)?;
        let source = metadata::metadata(activation)?;
        // A restart of the same live doctor/lease reuses the catalog-minted reference. Re-fetching
        // would mint another revision and break the unchanged exact quote generation unnecessarily.
        if let Some(expected) = expected_current.as_ref() {
            let at = metadata::timestamp()?;
            let definition = expected.definition();
            let identities: Vec<_> = definition
                .provider_identities()
                .iter()
                .filter(|identity| {
                    identity.source_id() == source.source_id()
                        && identity.metadata_revision() == source.revision()
                        && identity.provider_instrument_id().as_str() == issuer.symbol().as_str()
                        && definition.provider_identity_at(
                            identity.source_id(),
                            identity.provider_instrument_id(),
                            at,
                        ) == Some(*identity)
                })
                .collect();
            let effective = definition.effective_interval();
            if identities.len() == 1
                && expected.published_at() <= at
                && effective.starts_at() <= at
                && effective.ends_at().is_none_or(|end| at < end)
                && definition.quote_currency() == issuer.quote_currency()
                && definition.quote_currency_evidence() == issuer.currency_evidence()
                && definition.identifiers().iter().any(|record| matches!(record.identifier(), market_squawk_domain::ExternalIdentifier::Cusip(cusip) if cusip == issuer.cusip()))
                && self.research.market_data_instruments().latest(definition.instrument_id(), deadline, &cancellation)
                    .map_err(|_| ServiceError::Unavailable)?.as_ref() == Some(expected)
            {
                return Ok(expected.clone());
            }
        }

        let lease = activation.lease();
        let dataset =
            SourceIdentifier::try_from(metadata::DATASET).map_err(|_| ServiceError::Internal)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/schwab-instruments-rights/v1\0");
        hash.update(lease.rights_decision_digest().bytes());
        hash.update(source.source_id().as_str().as_bytes());
        hash.update(dataset.as_str().as_bytes());
        let rights = ResearchRightsAuthority::try_new_scoped(
            source.source_id().clone(),
            super::provider_research_rights_basis(lease).map_err(|_| ServiceError::Unauthorized)?,
            lease.rights_decision_digest(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            lease
                .verification_expires_at()
                .ok_or(ServiceError::Unauthorized)?,
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
            let guard = activation
                .currentness()
                .try_acquire_publication_authority()
                .map_err(|_| ServiceError::Unauthorized)?;
            guard
                .require_current()
                .map_err(|_| ServiceError::Unauthorized)?;
            self.research_mutation
                .register_provider_publication_generation(generation.clone(), rights)
                .map_err(|_| ServiceError::Unavailable)?;
        }
        let result = self
            .acquire_and_publish_schwab_instrument_reference(
                activation,
                issuer,
                official_listing,
                expected_current,
                &generation,
                deadline,
                &cancellation,
            )
            .await;
        // Native acquisition and the supervised commit have finished before releasing this
        // bounded family slot. Exact metadata history stays durable for restore and read lineage.
        let drained = self
            .research_mutation
            .revoke_provider_generation(generation.profile(), &generation)
            .await;
        match (result, drained) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(_)) => Err(ServiceError::Unavailable),
            (Ok(record), Ok(())) => Ok(record),
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "bounded acquisition retains exact source and caller coordinates"
    )]
    async fn acquire_and_publish_schwab_instrument_reference(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        issuer: &OfficialIssuerInstrumentReference,
        official_listing: ListingReferenceRecord,
        expected_current: Option<MarketDataInstrumentRecord>,
        generation: &ResearchProviderRuntimeGeneration,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, ServiceError> {
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
        let request = build_instrument_by_cusip_request(
            issuer.cusip().as_str(),
            RequestAdmission::new(nonzero(16 * 1024)?, NonZeroUsize::MIN),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let (token, epoch) = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            attempt = activation.acquire_publication_attempt() => attempt.map_err(|_| ServiceError::Unauthorized)?,
        };
        let receipt = epoch.receipt();
        let authority = self
            .research
            .acquire_schwab_instrument_reference_publication(
                generation,
                activation.doctor_receipt().clone(),
                activation.oauth_receipt_currentness(),
                receipt,
                deadline,
                cancellation.child_token(),
            )
            .await
            .map_err(|_| ServiceError::Unauthorized)?;
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(
            activation
                .lease()
                .provider_budget_policy()
                .cloned()
                .ok_or(ServiceError::Unauthorized)?,
            activation.account_binding().subject(),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        let budget = self
            .provider_rate
            .register_budget(declaration)
            .map_err(|_| ServiceError::Unavailable)?;
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            _ => return Err(ServiceError::Unavailable),
        };
        check_operation(deadline, cancellation)?;
        epoch
            .validate_current(receipt)
            .map_err(|_| ServiceError::Unauthorized)?;
        if !activation.currentness().is_active_now() {
            return Err(ServiceError::Unauthorized);
        }
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
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
        }.map_err(|_| if cancellation.is_cancelled() { ServiceError::Cancelled }
            else if Instant::now() >= deadline { ServiceError::DeadlineExceeded } else { ServiceError::Unavailable })?
        };
        drop(token);
        let receipt = match &outcome {
            RestExecutionOutcome::Accepted(response) => response.capture().receipt(),
            RestExecutionOutcome::ProviderRejected(capture)
            | RestExecutionOutcome::InvalidPayload { capture, .. } => capture.receipt(),
            _ => return Err(ServiceError::InvalidResult),
        };
        let rate_ok = if receipt.status() == 429 {
            backoff_recorded(apply_http_retry_after(
                &budget,
                receipt
                    .headers()
                    .iter()
                    .find(|header| header.name() == "retry-after")
                    .map(|header| header.value()),
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
        let reference = authority
            .seal_detail_outcome(outcome, issuer.cusip(), cleanup_deadline)
            .await;
        permit.release();
        let reference = reference.map_err(|_| ServiceError::Unavailable)?;
        check_operation(deadline, cancellation)?;
        if !rate_ok {
            return Err(ServiceError::Unavailable);
        }
        authority
            .publish_instrument_reference(
                reference,
                issuer,
                official_listing,
                expected_current,
                epoch,
                activation.currentness(),
                deadline,
                cancellation.child_token(),
            )
            .await
            .map_err(|_| ServiceError::Unavailable)
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
