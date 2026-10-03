//! Bounded credential verification; exact metadata and capture admission remains in the BEA source.

use market_squawk_adapter_bea::{
    BeaError, BeaMetadataRecords, BeaParseLimits, BeaQuery, parse_metadata_page,
};
use zeroize::Zeroizing;

use super::*;

const MAXIMUM_BEA_CATALOG_RECORDS: usize = 256;

impl ProviderOnboardingService {
    pub(super) async fn run_bea_credential_doctor(
        &self,
        profile: &ProviderOnboardingProfile,
        secret: &SecretValue,
        cancellation: CancellationToken,
    ) -> Result<CredentialProbeEvidence, ProviderOnboardingError> {
        if profile.id() != "bea.api-data" || profile.probe().transport() != ProbeTransport::HttpGet
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
        let user_id = BeaUserId::try_new(secret.expose_secret().to_owned())
            .map_err(|_| ProviderOnboardingError::InvalidSecretShape)?;
        let request = BeaQuery::dataset_list()
            .and_then(|query| query.single_page(None))
            .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let policy = profile
            .probe()
            .endpoint_policy()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let endpoint = profile
            .probe()
            .endpoint()
            .ok_or(ProviderOnboardingError::InvalidProfile)?;
        let mut target =
            reqwest::Url::parse(endpoint).map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        target
            .query_pairs_mut()
            .append_pair("Method", "GetDatasetList")
            .append_pair("ResultFormat", "JSON")
            .append_pair("UserID", secret.expose_secret());
        if !policy
            .authorize_request(target.as_str())?
            .contains_sensitive_query()
        {
            return Err(ProviderOnboardingError::InvalidProfile);
        }
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
        // Metadata rows also pass through the adapter's shared page-receipt row bound.
        let limits = BeaParseLimits::try_new(
            MAXIMUM_BEA_CATALOG_RECORDS,
            MAXIMUM_BEA_CATALOG_RECORDS,
            MAX_PROBE_BODY_BYTES,
            8 * 1024,
            1,
            1,
        )
        .map_err(|_| ProviderOnboardingError::InvalidProfile)?;
        let deadline = Instant::now()
            .checked_add(rate_runtime::PROBE_OPERATION_DURATION)
            .ok_or(ProviderOnboardingError::Clock)?;
        let mut permit = self
            .probe_rates
            .acquire(
                profile,
                profile.capability().rate_policy(),
                Some(&subject),
                cancellation.clone(),
            )
            .await?;
        permit.deadline = permit.deadline.min(deadline);
        let body = Zeroizing::new(
            self.collect_probe_response(
                self.client.get(target),
                policy,
                &mut permit,
                true,
                false,
                cancellation.clone(),
            )
            .await
            .map_err(|error| {
                let classification = match &error {
                    ProviderOnboardingError::ProbeUnavailable => "transport_or_response_rejected",
                    ProviderOnboardingError::CredentialRejected => "credential_rejected",
                    ProviderOnboardingError::ProbeRateLimited => "rate_limited",
                    ProviderOnboardingError::ProbeDeadlineExceeded => "deadline_exceeded",
                    ProviderOnboardingError::OperationCancelled => "cancelled",
                    _ => "probe_admission_rejected",
                };
                tracing::warn!(
                    stage = "http_response",
                    classification,
                    "bounded regional-data credential verification failed"
                );
                error
            })?,
        );
        // The existing adapter validates and redacts the exact echoed UserID before decoding.
        // No original response bytes or credential-bearing URL leave this transient operation.
        let page = parse_metadata_page(&body, &request, &user_id, limits)
            .map_err(map_bea_metadata_probe_error)?;
        let BeaMetadataRecords::Datasets(datasets) = page.records() else {
            tracing::warn!(
                stage = "dataset_selection",
                classification = "unexpected_metadata_kind",
                "bounded regional-data credential verification failed"
            );
            return Err(ProviderOnboardingError::ProbeUnavailable);
        };
        if !datasets
            .iter()
            .any(|dataset| dataset.identity().as_str() == "Regional")
        {
            tracing::warn!(
                stage = "dataset_selection",
                classification = "regional_dataset_absent",
                "bounded regional-data credential verification failed"
            );
            return Err(ProviderOnboardingError::ProbeUnavailable);
        }
        if cancellation.is_cancelled() {
            return Err(ProviderOnboardingError::OperationCancelled);
        }
        if Instant::now() >= deadline {
            return Err(ProviderOnboardingError::ProbeDeadlineExceeded);
        }
        let mut evidence = Sha256::new();
        evidence.update(b"market-squawk/bea-onboarding-regional-metadata-doctor/v1\0");
        evidence.update(profile.capability().content_digest().bytes());
        evidence.update(profile.rights_decision_digest().bytes());
        evidence.update(profile.capability().rate_policy().evidence_digest().bytes());
        evidence.update(request.request_digest());
        evidence.update(page.receipt().upstream_response_digest());
        evidence.update(page.receipt().response_digest());
        permit.record_success()?;
        Ok(CredentialProbeEvidence {
            response_digest: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                evidence.finalize().into(),
            ),
            account_digest: None,
        })
    }
}

fn map_bea_metadata_probe_error(error: BeaError) -> ProviderOnboardingError {
    // Never format the error: provider descriptions may include arbitrary response text.
    let classification = match &error {
        BeaError::InvalidCredential => "invalid_credential",
        BeaError::InvalidRequest => "invalid_request",
        BeaError::InvalidLimit => "invalid_limit",
        BeaError::BodyTooLarge => "body_limit",
        BeaError::RowLimitExceeded => "row_limit",
        BeaError::StringLimitExceeded => "string_limit",
        BeaError::Allocation => "allocation",
        BeaError::SanitizationCancelled => "cancelled",
        BeaError::SanitizationDeadlineExceeded => "deadline_exceeded",
        BeaError::SanitizationClockUnavailable => "clock_unavailable",
        BeaError::InvalidJson => "invalid_json",
        BeaError::InvalidField(_) => "invalid_protocol_field",
        BeaError::RequestEchoMismatch => "request_echo_mismatch",
        BeaError::Provider(_) => "provider_error",
        BeaError::FilteredParameterValuesUnsupported => "filtered_values_unsupported",
        BeaError::InvalidDecimal => "invalid_decimal",
        BeaError::InvalidTimePeriod => "invalid_time_period",
        BeaError::InvalidRevision => "invalid_revision",
    };
    let provider_code = match &error {
        BeaError::Provider(provider) => Some(provider.code()),
        _ => None,
    };
    tracing::warn!(
        stage = "metadata_parse",
        classification,
        provider_code,
        "bounded regional-data credential verification failed"
    );
    ProviderOnboardingError::ProbeUnavailable
}
