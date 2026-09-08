//! Credentialed, non-persistent provider inspection behind the Source application contract.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use market_squawk_adapter_fred::{FredSource, FredSourceError};
use market_squawk_domain::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    AuthorizationMode, CoverageDomain, ExtractionSourceError, HistoricalCapability, SourceClass,
    SourceError,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::application::{
    EphemeralSourceInspectionAuthority, EphemeralSourceInspectionRequest,
    EphemeralSourceInspectionResult,
};
use crate::{ProviderAdapterActivationError, ProviderOnboardingError};

use super::{
    CliProviderActivationError, FRED_SURFACE, ProviderResearchActivationService, ProviderSurface,
    default_unrate_dashboard_dataset, fred_budget, fred_network_policy, metadata, require_surface,
    update_digest,
};

const INSPECTION_EVIDENCE_DOMAIN: &[u8] = b"market-squawk:fred-ephemeral-inspection-authority:v1\0";

#[async_trait]
impl EphemeralSourceInspectionAuthority for ProviderResearchActivationService {
    async fn inspect(
        &self,
        request: EphemeralSourceInspectionRequest,
    ) -> Result<EphemeralSourceInspectionResult, ServiceError> {
        self.tasks
            .require_admission()
            .map_err(|_error| ServiceError::Unavailable)?;
        if request.provider().as_str() != FRED_SURFACE {
            return Err(ServiceError::InvalidRequest);
        }
        if request.cancellation().is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        let cancellation = request.cancellation().clone();
        let session_id = request.onboarding_session_id();
        let lease = self
            .onboarding
            .prepare_runtime_activation_target(session_id, cancellation.child_token())
            .await
            .map_err(map_onboarding_error)?;
        require_surface(&lease, ProviderSurface::Exact(FRED_SURFACE))
            .map_err(|_error| ServiceError::Unauthorized)?;

        let dataset = request.dataset_identifier().clone();
        let dashboard_dataset = default_unrate_dashboard_dataset().map_err(map_cli_error)?;
        if dataset != dashboard_dataset {
            return Err(ServiceError::InvalidRequest);
        }
        FredSource::series_identifier(&dataset).map_err(|_error| ServiceError::InvalidRequest)?;

        let evidence = inspection_evidence(
            &lease,
            request.provider(),
            &dataset,
            request.page_index(),
            request.max_records().get(),
        );
        let effective = EffectiveInterval::new(
            lease.authority_effective_at(),
            lease.verification_expires_at(),
        )
        .map_err(|_error| ServiceError::Unauthorized)?;
        let source_metadata = metadata(
            &lease,
            evidence,
            "fred",
            "fred",
            SourceClass::OfficialAgency,
            CoverageDomain::Macroeconomic,
            AuthorizationMode::UserAuthorized,
            HistoricalCapability::RevisionPreserving,
            effective,
            fred_network_policy().map_err(map_cli_error)?,
            fred_budget(&lease).map_err(map_cli_error)?,
        )
        .map_err(map_cli_error)?;
        let page = self
            .activation
            .inspect_fred_ephemeral(
                lease,
                source_metadata,
                dataset.clone(),
                request.page_index(),
                request.max_records(),
                request.max_bytes(),
                wall_deadline(request.deadline())?,
                cancellation,
            )
            .await
            .map_err(map_activation_error)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(page.canonical_payloads().len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for payload in page.canonical_payloads() {
            let observation: Value =
                serde_json::from_slice(payload).map_err(|_error| ServiceError::InvalidResult)?;
            if !observation.is_object() {
                return Err(ServiceError::InvalidResult);
            }
            observations.push(observation);
        }
        if observations.len() > usize::from(request.max_records().get()) {
            return Err(ServiceError::InvalidResult);
        }
        Ok(EphemeralSourceInspectionResult::new(
            request.provider().clone(),
            session_id,
            dataset,
            page.object_id().clone(),
            request.page_index(),
            page.page_evidence().clone(),
            page.received_at(),
            observations,
        ))
    }
}

fn inspection_evidence(
    lease: &crate::ProviderActivationLease,
    provider: &SourceIdentifier,
    dataset: &SourceIdentifier,
    page_index: u16,
    max_records: u16,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(INSPECTION_EVIDENCE_DOMAIN);
    update_digest(&mut hasher, lease.capability_digest());
    update_digest(&mut hasher, lease.public_configuration_digest());
    update_digest(&mut hasher, lease.rights_decision_digest());
    hasher.update(lease.session_id().as_bytes());
    hasher.update(
        lease
            .generation()
            .map_or(0_u64, |generation| generation.get())
            .to_be_bytes(),
    );
    hash_identifier(&mut hasher, provider);
    hash_identifier(&mut hasher, dataset);
    hasher.update(page_index.to_be_bytes());
    hasher.update(max_records.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn hash_identifier(hasher: &mut Sha256, identifier: &SourceIdentifier) {
    let bytes = identifier.as_str().as_bytes();
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn wall_deadline(deadline: Instant) -> Result<Timestamp, ServiceError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(ServiceError::DeadlineExceeded)?;
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| ServiceError::Unavailable)?;
    let deadline_nanos = wall
        .as_nanos()
        .checked_add(remaining.as_nanos())
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(deadline_nanos))
}

fn map_cli_error(error: CliProviderActivationError) -> ServiceError {
    match error {
        CliProviderActivationError::Cancelled => ServiceError::Cancelled,
        CliProviderActivationError::InvalidRequest
        | CliProviderActivationError::InvalidRights
        | CliProviderActivationError::InvalidMetadata
        | CliProviderActivationError::ProviderConfiguration
        | CliProviderActivationError::SurfaceMismatch
        | CliProviderActivationError::ConfirmationRequired => ServiceError::InvalidRequest,
        CliProviderActivationError::Onboarding(error) => map_onboarding_error(error),
        CliProviderActivationError::Activation(error) => map_activation_error(error),
        CliProviderActivationError::StateUnavailable
        | CliProviderActivationError::InputUnavailable => ServiceError::Unavailable,
    }
}

fn map_onboarding_error(error: ProviderOnboardingError) -> ServiceError {
    match error {
        ProviderOnboardingError::OperationCancelled => ServiceError::Cancelled,
        ProviderOnboardingError::ProbeDeadlineExceeded => ServiceError::DeadlineExceeded,
        ProviderOnboardingError::InvalidRequest
        | ProviderOnboardingError::InvalidSecretShape
        | ProviderOnboardingError::UnknownProfile => ServiceError::InvalidRequest,
        ProviderOnboardingError::CredentialRejected
        | ProviderOnboardingError::RightsBlocked
        | ProviderOnboardingError::EvidenceRefreshRequired
        | ProviderOnboardingError::ActivationUnavailable
        | ProviderOnboardingError::ActivationExpired
        | ProviderOnboardingError::InvalidSessionState
        | ProviderOnboardingError::SecretImportUnavailable
        | ProviderOnboardingError::RenewalUnavailable => ServiceError::Unauthorized,
        _ => ServiceError::Unavailable,
    }
}

fn map_activation_error(error: ProviderAdapterActivationError) -> ServiceError {
    match error {
        ProviderAdapterActivationError::Cancelled => ServiceError::Cancelled,
        ProviderAdapterActivationError::Onboarding(error) => map_onboarding_error(error),
        ProviderAdapterActivationError::SurfaceMismatch
        | ProviderAdapterActivationError::SourceBinding
        | ProviderAdapterActivationError::InvalidRights
        | ProviderAdapterActivationError::ExtractionContract(_) => ServiceError::InvalidRequest,
        ProviderAdapterActivationError::ExplicitResumeRequired => ServiceError::Unauthorized,
        ProviderAdapterActivationError::Fred(error) => map_fred_error(error),
        ProviderAdapterActivationError::ExtractionSource(error) => map_extraction_error(error),
        _ => ServiceError::Unavailable,
    }
}

fn map_fred_error(error: FredSourceError) -> ServiceError {
    match error {
        FredSourceError::Cancelled => ServiceError::Cancelled,
        FredSourceError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        FredSourceError::InvalidApiKey => ServiceError::Unauthorized,
        FredSourceError::InvalidDataset => ServiceError::InvalidRequest,
        FredSourceError::BodyTooLarge { .. } => ServiceError::ResourceExhausted,
        FredSourceError::Network
        | FredSourceError::Protocol
        | FredSourceError::InvalidConfiguration
        | FredSourceError::RevisionAuthority(_) => ServiceError::Unavailable,
    }
}

fn map_extraction_error(error: ExtractionSourceError) -> ServiceError {
    match error {
        ExtractionSourceError::Cancelled => ServiceError::Cancelled,
        ExtractionSourceError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ExtractionSourceError::Contract(_) => ServiceError::ResourceExhausted,
        ExtractionSourceError::Source(SourceError::Unauthorized) => ServiceError::Unauthorized,
        ExtractionSourceError::Source(_) | ExtractionSourceError::Authority(_) => {
            ServiceError::Unavailable
        }
    }
}
