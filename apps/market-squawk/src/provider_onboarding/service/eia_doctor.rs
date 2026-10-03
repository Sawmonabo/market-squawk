//! Real, bounded EIA credential verification through the existing shared probe budget.

use market_squawk_adapter_eia::{
    EiaDataPage, EiaDatasetContract, EiaDatasetContractInput, EiaError, EiaMetadataRequest,
    EiaParseLimits, eia_api_endpoint_rules, parse_facet_metadata, parse_route_metadata,
};
use market_squawk_sources::{EndpointPolicy, HttpRequestBounds};

use super::*;
use crate::provider_activation::eia_configuration::{
    electricity_price_descriptors, electricity_price_fields, electricity_price_query,
};

impl ProviderOnboardingService {
    pub(super) async fn run_eia_credential_doctor(
        &self,
        profile: &ProviderOnboardingProfile,
        secret: &SecretValue,
        cancellation: CancellationToken,
    ) -> Result<CredentialProbeEvidence, ProviderOnboardingError> {
        if profile.id() != "eia.api-v2"
            || profile.capability().revision().get() != 1
            || profile.probe().transport() != ProbeTransport::HttpGet
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        validate_secret_shape(profile, secret)?;
        let deadline = Instant::now()
            .checked_add(rate_runtime::PROBE_OPERATION_DURATION)
            .ok_or(ProviderOnboardingError::Clock)?;
        let observe = async {
            let query = electricity_price_query("2024-01".to_owned(), "2025-12".to_owned())
                .map_err(map_eia_probe_error)?;
            let policy = EndpointPolicy::try_from_api_rules(
                eia_api_endpoint_rules(&query)?,
                HttpRequestBounds::default(),
            )?;
            let limits = EiaParseLimits::production_defaults();
            let subject = ProviderRateDeclaration::governed_provider_subject(
                profile
                    .capability()
                    .rate_policy()
                    .enforcement_policy()
                    .ok_or(ProviderOnboardingError::InvalidProfile)?
                    .scope()
                    .as_source_identifier(),
            )
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
            let mut retained_bytes = 0_usize;
            let mut evidence = Sha256::new();
            evidence.update(b"market-squawk/eia-onboarding-metadata-price-doctor/v1\0");
            evidence.update(profile.capability().content_digest().bytes());
            evidence.update(profile.rights_decision_digest().bytes());
            evidence.update(profile.capability().rate_policy().evidence_digest().bytes());
            evidence.update(query.identity().bytes());
            let route_request = EiaMetadataRequest::route(query.route().clone());
            let safe = route_request.secret_free().map_err(map_eia_probe_error)?;
            let body = self
                .eia_probe_response(
                    profile,
                    secret,
                    &subject,
                    &policy,
                    safe.secret_free_url(),
                    deadline,
                    &cancellation,
                )
                .await?;
            add_eia_probe_bytes(&mut retained_bytes, body.len())?;
            let metadata = parse_route_metadata(&body, &route_request, system_timestamp()?, limits)
                .map_err(map_eia_probe_error)?;
            evidence.update(metadata.receipt().retained_payload_digest().bytes());
            drop(body);
            let mut facets = Vec::new();
            for facet in query.facets() {
                let request =
                    EiaMetadataRequest::facet(query.route().clone(), facet.facet().clone());
                let safe = request.secret_free().map_err(map_eia_probe_error)?;
                let body = self
                    .eia_probe_response(
                        profile,
                        secret,
                        &subject,
                        &policy,
                        safe.secret_free_url(),
                        deadline,
                        &cancellation,
                    )
                    .await?;
                add_eia_probe_bytes(&mut retained_bytes, body.len())?;
                let catalog = parse_facet_metadata(&body, &request, system_timestamp()?, limits)
                    .map_err(map_eia_probe_error)?;
                evidence.update(catalog.receipt().retained_payload_digest().bytes());
                facets.push(catalog);
            }
            let contract = EiaDatasetContract::try_new(EiaDatasetContractInput {
                metadata,
                query: query.clone(),
                fields: electricity_price_fields().map_err(map_eia_probe_error)?,
                facet_catalogs: facets,
                descriptor_fields: electricity_price_descriptors().map_err(map_eia_probe_error)?,
                clock_fields: Vec::new(),
            })
            .map_err(map_eia_probe_error)?;
            let request = query.page(0);
            let safe = request.secret_free().map_err(map_eia_probe_error)?;
            let body = self
                .eia_probe_response(
                    profile,
                    secret,
                    &subject,
                    &policy,
                    safe.secret_free_url(),
                    deadline,
                    &cancellation,
                )
                .await?;
            add_eia_probe_bytes(&mut retained_bytes, body.len())?;
            let page = EiaDataPage::parse(&body, request, &contract, system_timestamp()?, limits)
                .map_err(map_eia_probe_error)?;
            let receipt = page.receipt();
            // The verification recipe has exactly 24 monthly US residential price observations.
            if receipt.offset() != 0
                || receipt.total() != 24
                || receipt.returned_rows() != 24
                || receipt.observation_count() != 24
                || receipt.missing_observation_count() != 0
                || page.api_version() != contract.metadata().api_version()
            {
                return Err(ProviderOnboardingError::ProbeUnavailable);
            }
            evidence.update(contract.schema_digest().bytes());
            evidence.update(receipt.retained_payload_digest().bytes());
            evidence.update(receipt.received_at().unix_nanos().to_be_bytes());
            Ok(CredentialProbeEvidence {
                response_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    evidence.finalize().into(),
                ),
                account_digest: None,
            })
        };
        let evidence = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ProviderOnboardingError::OperationCancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) =>
                Err(ProviderOnboardingError::ProbeDeadlineExceeded),
            result = observe => result,
        }?;
        if cancellation.is_cancelled() {
            return Err(ProviderOnboardingError::OperationCancelled);
        }
        if Instant::now() >= deadline {
            return Err(ProviderOnboardingError::ProbeDeadlineExceeded);
        }
        Ok(evidence)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact probe policy, authority and operation bounds stay explicit"
    )]
    async fn eia_probe_response(
        &self,
        profile: &ProviderOnboardingProfile,
        secret: &SecretValue,
        subject: &SourceIdentifier,
        policy: &EndpointPolicy,
        safe_url: &reqwest::Url,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ProviderOnboardingError> {
        let mut target = safe_url.clone();
        target
            .query_pairs_mut()
            .append_pair("api_key", secret.expose_secret());
        if !policy
            .authorize_request(target.as_str())?
            .contains_sensitive_query()
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        let mut permit = self
            .probe_rates
            .acquire(
                profile,
                profile.capability().rate_policy(),
                Some(subject),
                cancellation.clone(),
            )
            .await?;
        permit.deadline = permit.deadline.min(deadline);
        let response = self
            .collect_probe_response(
                self.client.get(target),
                policy,
                &mut permit,
                true,
                false,
                cancellation.clone(),
            )
            .await?;
        permit.record_success()?;
        Ok(response)
    }
}

fn add_eia_probe_bytes(total: &mut usize, bytes: usize) -> Result<(), ProviderOnboardingError> {
    *total = total
        .checked_add(bytes)
        .ok_or(ProviderOnboardingError::ProbeUnavailable)?;
    if *total > MAX_PROBE_BODY_BYTES {
        return Err(ProviderOnboardingError::ProbeUnavailable);
    }
    Ok(())
}

fn map_eia_probe_error(error: EiaError) -> ProviderOnboardingError {
    // EiaError contains only closed reasons and numeric limits; never provider bodies or URLs.
    tracing::warn!(error = ?error, "bounded energy-data credential verification failed");
    ProviderOnboardingError::ProbeUnavailable
}
