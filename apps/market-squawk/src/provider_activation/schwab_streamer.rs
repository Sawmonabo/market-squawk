//! Real read-only UserPreference bootstrap on the same staged or active account and budget.
use super::{ProviderAdapterActivation, SchwabMarketDataAccountActivation};
use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ParseBounds, ReadOnlyRequest, RequestAdmission, RestExecutionOutcome,
    RestTransportBounds, SchwabRestExecutor, SchwabTransportTelemetry,
    SchwabUserPreferenceEvidence,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetReservationDecision, ProviderRateDeclaration,
    apply_http_retry_after,
};
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

impl ProviderAdapterActivation {
    /// Acquires dynamic zeroizing socket coordinates; a doctor hash cannot reconstruct them.
    pub(crate) async fn acquire_schwab_streamer_bootstrap(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SchwabUserPreferenceEvidence, ServiceError> {
        let nz = |value| NonZeroUsize::new(value).ok_or(ServiceError::Internal);
        let request =
            ReadOnlyRequest::user_preference(RequestAdmission::new(nz(16 * 1024)?, nz(1)?))
                .map_err(|_| ServiceError::Internal)?;
        let executor = SchwabRestExecutor::try_production(
            RestTransportBounds::try_new(
                Duration::from_secs(5),
                Duration::from_secs(15),
                Duration::from_secs(20),
                nz(4 * 1024 * 1024)?,
                nz(64)?,
                nz(64 * 1024)?,
            )
            .map_err(|_| ServiceError::Internal)?,
            ParseBounds::new(
                nz(4 * 1024 * 1024)?,
                nz(8192)?,
                nz(256 * 1024)?,
                nz(64)?,
                512,
                512 * 1024,
            ),
            AccessTokenAdmission::new(nz(16 * 1024)?, Duration::from_secs(60)),
            SchwabTransportTelemetry::default(),
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let (token, epoch) = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            value = activation.acquire_runtime_publication_attempt() => value.map_err(|_| ServiceError::Unauthorized)?,
        };
        let oauth = epoch.receipt();
        let policy = activation
            .lease()
            .provider_budget_policy()
            .cloned()
            .ok_or(ServiceError::Unauthorized)?;
        if policy.weighted_window_count() != 0 {
            return Err(ServiceError::Unauthorized);
        }
        let budget = self
            .provider_rate
            .register_budget(
                ProviderRateDeclaration::try_for_authorization_subject(
                    policy,
                    activation.account_binding().subject(),
                )
                .map_err(|_| ServiceError::Unauthorized)?,
            )
            .map_err(|_| ServiceError::Unavailable)?;
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(value) => value,
            _ => return Err(ServiceError::Unavailable),
        };
        epoch
            .validate_current(oauth)
            .map_err(|_| ServiceError::Unauthorized)?;
        if !activation.runtime_currentness().is_current_now() {
            return Err(ServiceError::Unauthorized);
        }
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(value) => value,
            _ => return Err(ServiceError::Unavailable),
        };
        let transport_cancel = cancellation.child_token();
        let outcome = {
            let operation = executor.execute(&request, &token, transport_cancel.clone());
            tokio::pin!(operation);
            tokio::select! {
                biased;
                () = cancellation.cancelled() => { transport_cancel.cancel(); (&mut operation).await }
                () = tokio::time::sleep_until(deadline.into()) => { transport_cancel.cancel(); (&mut operation).await }
                outcome = &mut operation => outcome,
            }.map_err(|_| ServiceError::Unavailable)?
        };
        drop(token);
        let receipt = match &outcome {
            RestExecutionOutcome::AcceptedUserPreference(value) => value.receipt(),
            RestExecutionOutcome::UserPreferenceRejected(receipt)
            | RestExecutionOutcome::InvalidUserPreference { receipt, .. } => receipt,
            _ => return Err(ServiceError::InvalidResult),
        };
        let rate_ok = if receipt.status() == 429 {
            matches!(
                apply_http_retry_after(
                    &budget,
                    receipt
                        .headers()
                        .iter()
                        .find(|header| header.name() == "retry-after")
                        .map(|header| header.value()),
                    0
                ),
                market_squawk_sources::BudgetDecision::WaitUntil(_)
            )
        } else if (200..=299).contains(&receipt.status()) {
            budget.record_success().is_ok()
        } else {
            matches!(
                budget.apply_refusal(0),
                market_squawk_sources::BudgetDecision::WaitUntil(_)
            )
        };
        permit.release();
        // Native UserPreference deliberately retains only a receipt and zeroizing bootstrap, not
        // unrelated account data. It is not persisted in the raw market-data capture store.
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        epoch
            .validate_current(oauth)
            .map_err(|_| ServiceError::Unauthorized)?;
        let RestExecutionOutcome::AcceptedUserPreference(provider) = outcome else {
            return Err(ServiceError::Unavailable);
        };
        if !rate_ok
            || !activation.runtime_currentness().is_current_now()
            || provider.receipt().credential_authority() != oauth.credential_authority()
            || provider.bootstrap().value().market_data_principal_sha256()
                != activation
                    .doctor_receipt()
                    .market_data_principal_sha256()
                    .bytes()
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(provider)
    }
}

#[derive(Debug)]
pub(crate) struct PreparedSchwabStreamerMarketRuntimeStart {
    pub(crate) activation: std::sync::Arc<SchwabMarketDataAccountActivation>,
    pub(crate) generation: crate::application::ResearchProviderRuntimeGeneration,
    pub(crate) publication: crate::application::SchwabStreamerPublicationPackage,
    pub(crate) bootstrap: SchwabUserPreferenceEvidence,
    pub(crate) bindings: Vec<(
        super::SchwabQuoteReferenceBinding,
        Option<super::MarketReferenceIdentityApprovalV1>,
    )>,
    pub(crate) display_bindings: Vec<super::MarketDataInstrumentBinding>,
    pub(crate) venue: market_squawk_domain::VenueId,
    pub(crate) canonical: market_squawk_data::MarketDataInstrumentReadCapability,
    pub(crate) listing: Option<market_squawk_data::ListingReferenceReadCapability>,
    pub(crate) nasdaq_generation: Option<market_squawk_data::ListingReferenceGenerationReceipt>,
    pub(crate) provider_rate: market_squawk_sources::ProviderRateAuthority,
}
impl ProviderAdapterActivation {
    #[allow(
        clippy::too_many_arguments,
        reason = "same account and original canonical/listing authorities remain explicit"
    )]
    pub(crate) async fn prepare_schwab_streamer_market_runtime_start(
        &self,
        activation: std::sync::Arc<SchwabMarketDataAccountActivation>,
        instruments: Vec<super::SchwabQuoteReferenceBinding>,
        display_bindings: Vec<super::MarketDataInstrumentBinding>,
        approvals: Vec<super::MarketReferenceIdentityApprovalV1>,
        canonical: market_squawk_data::MarketDataInstrumentReadCapability,
        listing: Option<market_squawk_data::ListingReferenceReadCapability>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<super::PreparedSchwabMarketRuntimeStart, ServiceError> {
        let bootstrap = self
            .acquire_schwab_streamer_bootstrap(&activation, deadline, &cancellation)
            .await?;
        let generation = self
            .register_schwab_streamer_generation(
                &activation,
                &instruments,
                bootstrap.bootstrap().value(),
            )
            .await?;
        let prepared = async {
            let now = super::schwab::system_timestamp().map_err(|_| ServiceError::Unavailable)?;
            let nasdaq_generation = super::schwab::selected_nasdaq_generation(&instruments)
                .map_err(|_| ServiceError::Unavailable)?;
            if let Some(expected) = &nasdaq_generation {
                if listing
                    .as_ref()
                    .ok_or(ServiceError::Unavailable)?
                    .current(deadline, &cancellation)
                    .map_err(|_| ServiceError::Unavailable)?
                    .as_ref()
                    != Some(expected)
                {
                    return Err(ServiceError::Unavailable);
                }
            }
            let bindings = super::schwab::exact_schwab_quote_bindings(
                instruments,
                approvals,
                generation.metadata(),
                nasdaq_generation.as_ref(),
                now,
            )
            .map_err(|_| ServiceError::Unavailable)?;
            super::schwab::validate_schwab_display_bindings(&bindings, &display_bindings, now)
                .map_err(|_| ServiceError::Unavailable)?;
            for (binding, _) in &bindings {
                if canonical
                    .latest(binding.instrument_id(), deadline, &cancellation)
                    .map_err(|_| ServiceError::Unavailable)?
                    .as_ref()
                    != Some(binding.canonical_record())
                {
                    return Err(ServiceError::Unavailable);
                }
            }
            let oauth = activation
                .runtime_oauth_receipt()
                .await
                .map_err(|_| ServiceError::Unauthorized)?;
            let publication = self
                .research_mutation
                .bind_schwab_streamer_publication_package(
                    &generation,
                    activation.doctor_receipt().clone(),
                    activation.oauth_receipt_currentness(),
                    oauth,
                )
                .map_err(|_| ServiceError::Unauthorized)?;
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            if Instant::now() >= deadline {
                return Err(ServiceError::DeadlineExceeded);
            }
            Ok(super::PreparedSchwabMarketRuntimeStart::Streamer(
                PreparedSchwabStreamerMarketRuntimeStart {
                    activation,
                    generation: generation.clone(),
                    publication,
                    bootstrap,
                    bindings,
                    display_bindings,
                    venue: market_squawk_domain::VenueId::try_from("schwab")
                        .map_err(|_| ServiceError::Internal)?,
                    canonical,
                    listing,
                    nasdaq_generation,
                    provider_rate: self.provider_rate.clone(),
                },
            ))
        }
        .await;
        if prepared.is_err() {
            self.research_mutation
                .revoke_provider_generation(generation.profile(), &generation)
                .await
                .map_err(|_| ServiceError::Unavailable)?;
        }
        prepared
    }
}
