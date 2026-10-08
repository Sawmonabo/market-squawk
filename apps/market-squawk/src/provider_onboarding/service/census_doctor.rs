//! Fixed Census credential verification using the adapter's shared ACS doctor contract.

use market_squawk_adapter_census::{
    CENSUS_DOCTOR_MAX_RESPONSE_BYTES, CENSUS_DOCTOR_TIMEOUT, census_doctor_query,
    validate_census_doctor_response,
};

use super::*;

impl ProviderOnboardingService {
    pub(super) async fn run_census_credential_doctor(
        &self,
        profile: &ProviderOnboardingProfile,
        secret: &SecretValue,
        cancellation: CancellationToken,
    ) -> Result<CredentialProbeEvidence, ProviderOnboardingError> {
        if profile.id() != "census.data-api"
            || profile.capability().revision().get() != 1
            || profile.probe().transport() != ProbeTransport::HttpGet
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        validate_secret_shape(profile, secret)?;
        let query = census_doctor_query().map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let policy = profile
            .probe()
            .endpoint_policy()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let mut target = reqwest::Url::parse(query.redacted_url())
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        target
            .query_pairs_mut()
            .append_pair("key", secret.expose_secret());
        if !policy
            .authorize_request(target.as_str())?
            .contains_sensitive_query()
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        let subject = ProviderRateDeclaration::governed_provider_subject(
            profile
                .rate_policy()
                .enforcement_policy()
                .ok_or(ProviderOnboardingError::InvalidProfile)?
                .scope()
                .as_source_identifier(),
        )
        .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let deadline = Instant::now()
            .checked_add(rate_runtime::PROBE_OPERATION_DURATION)
            .ok_or(ProviderOnboardingError::Clock)?;
        let observe = async {
            let mut permit = self
                .probe_rates
                .acquire(
                    profile,
                    profile.rate_policy(),
                    Some(&subject),
                    cancellation.clone(),
                )
                .await?;
            let request_deadline = Instant::now()
                .checked_add(CENSUS_DOCTOR_TIMEOUT)
                .ok_or(ProviderOnboardingError::Clock)?;
            permit.deadline = permit.deadline.min(deadline).min(request_deadline);
            let response = self
                .collect_probe_response_bounded(
                    self.client.get(target),
                    policy,
                    &mut permit,
                    true,
                    false,
                    cancellation.clone(),
                    CENSUS_DOCTOR_MAX_RESPONSE_BYTES,
                )
                .await?;
            validate_census_doctor_response(&response)
                .map_err(|_| ProviderOnboardingError::ProbeUnavailable)?;
            let mut evidence = Sha256::new();
            evidence.update(b"market-squawk/census-onboarding-acs-population-doctor/v1\0");
            evidence.update(profile.capability().content_digest().bytes());
            evidence.update(profile.rights_decision_digest().bytes());
            evidence.update(profile.capability().rate_policy().evidence_digest().bytes());
            evidence.update(query.request_digest());
            evidence.update(Sha256::digest(&response));
            permit.record_success()?;
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
}
