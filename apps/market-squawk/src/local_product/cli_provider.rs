//! Closed, evidence-bound CLI activation for supported research providers.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cap_fs_ext::DirExt as _;
use cap_std::fs::Dir;
use market_squawk_adapter_bls::{BlsAccessTier, BlsRequestPlan, BlsSeriesMetadata};
use market_squawk_adapter_fred::{
    FredOperation, FredOwnerAuthorizationEvidence, FredRightsArtifact, FredRightsPolicy,
    FredSeriesRightsGrant, FredTermsDocumentBytes, FredTermsDocumentRole,
    MAX_FRED_TERMS_DOCUMENT_BYTES, Sha256Digest, fred_series_endpoint_rule,
};
use market_squawk_adapter_sec::{
    RawEvidenceStore, SecParserLimits, SecRepresentationLimits, SecRepresentationRegistry,
};
use market_squawk_adapter_treasury::{TreasuryFiscalQuery, TreasurySourceConfig};
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ProviderIdentityRegistry, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    BoundedInput, LocalPaths, LocalSecretStoreError, UserAuthorizedInputRoot,
};
use market_squawk_sources::{
    ApiEndpointRule, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    BudgetWindowSemantics, CoverageDomain, EndpointPolicy, FreshnessPolicy, HistoricalCapability,
    HttpRequestBounds, NetworkAccessPolicy, PathScope, ProviderBudgetPolicy, ProviderBudgetWindow,
    QueryParameterRule, QuerySensitivity, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    BlsAdapterActivation, FredAdapterActivation, ProviderActivationLease,
    ProviderActivationOutcome, ProviderAdapterActivation, ProviderAdapterActivationError,
    ProviderAdapterActivationRequest, ProviderOnboardingError, ProviderOnboardingService,
    ProviderPortalActivationAuthority, ProviderPortalActivationError,
    ProviderPortalActivationRequest, ProviderPortalActivationView, SecAdapterActivation,
    TreasuryAdapterActivation,
};

use super::LocalProduct;
use super::provider_activation_state::{
    DurableActivationQuarantineReason, DurableActivationRecipeState,
    DurableProviderActivationState, RESTORABLE_RESEARCH_SURFACES,
};

const REQUEST_SCHEMA_VERSION: u16 = 2;
const REQUEST_MAXIMUM_BYTES: u64 = 1024 * 1024;
const BLS_SERIES_METADATA_MAXIMUM_BYTES: u64 = 4 * 1024;
const FRED_RIGHTS_ARTIFACT_MAXIMUM_BYTES: u64 = 256 * 1024;
const FRED_AUTHORIZATION_MAXIMUM_BYTES: u64 = 256 * 1024;
const MAXIMUM_BLS_SERIES: usize = 1_000;
const MAXIMUM_FRED_GRANTS: usize = 256;
const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;
const DAY_NANOS: u64 = 86_400 * SECOND_NANOS;
const SEC_SURFACE: &str = "sec.edgar-public";
const BLS_PUBLIC_SURFACE: &str = "bls.v1-unregistered";
const BLS_REGISTERED_SURFACE: &str = "bls.v2-registered";
const TREASURY_XML_SURFACE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_SURFACE: &str = "treasury.fiscal-data";
const FRED_SURFACE: &str = "fred-alfred.api-v1-v2";

/// Shared application authority behind local portal adapter activation and durable restart.
#[derive(Clone)]
pub(super) struct ProviderResearchActivationService {
    paths: LocalPaths,
    onboarding: Arc<ProviderOnboardingService>,
    activation: Arc<ProviderAdapterActivation>,
    state: DurableProviderActivationState,
}

impl ProviderResearchActivationService {
    pub(super) fn new(
        paths: LocalPaths,
        onboarding: Arc<ProviderOnboardingService>,
        activation: Arc<ProviderAdapterActivation>,
        state: DurableProviderActivationState,
    ) -> Self {
        Self {
            paths,
            onboarding,
            activation,
            state,
        }
    }

    async fn activate_from_portal(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, CliProviderActivationError> {
        if cancellation.is_cancelled() {
            return Err(CliProviderActivationError::Cancelled);
        }
        let (provider, evidence) = portal_provider_request(session_id, request)?;
        let lease = match self.onboarding.activation_lease(session_id) {
            Ok(lease) => lease,
            Err(ProviderOnboardingError::ActivationUnavailable) => self
                .onboarding
                .activate(session_id, cancellation.clone())
                .await
                .map_err(CliProviderActivationError::Onboarding)?,
            Err(error) => return Err(CliProviderActivationError::Onboarding(error)),
        };
        require_surface(&lease, provider.surface())?;
        let surface_id = lease.surface_id().as_str().to_owned();
        let _activation_guard = self
            .state
            .acquire_activation(&surface_id)
            .await
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        let request = ActivationRequest {
            schema_version: REQUEST_SCHEMA_VERSION,
            session_id,
            provider,
        };
        let request_bytes = serde_json::to_vec(&request)
            .map_err(|_error| CliProviderActivationError::InvalidRequest)?;
        if request_bytes.is_empty()
            || u64::try_from(request_bytes.len())
                .map_or(true, |length| length > REQUEST_MAXIMUM_BYTES)
        {
            return Err(CliProviderActivationError::InvalidRequest);
        }
        let activation =
            build_research_activation(&self.paths, &lease, &request_bytes, request, &evidence)?;
        evidence.persist(&self.state)?;
        let candidate_digest = self
            .state
            .recipe_digest(&surface_id, session_id, &request_bytes, &evidence.digests())
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        if self
            .activation
            .is_research_profile_active(lease.surface_id())
            .map_err(CliProviderActivationError::Activation)?
        {
            if self
                .state
                .desired_recipe_matches(&surface_id, candidate_digest)
                .map_err(|_error| CliProviderActivationError::StateUnavailable)?
            {
                return Ok(ProviderPortalActivationView::from_lease(
                    lease.surface_id().clone(),
                    &lease,
                ));
            }
            return Err(CliProviderActivationError::ProviderConfiguration);
        }
        let published_digest = self
            .state
            .persist_recipe(&surface_id, session_id, &request_bytes, &evidence.digests())
            .map_err(|_error| CliProviderActivationError::StateUnavailable)?;
        if published_digest != candidate_digest {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        let outcome = match self
            .activation
            .activate_ready_profile(session_id, activation, cancellation)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                if !self
                    .state
                    .quarantine_recipe_if_current(
                        &surface_id,
                        candidate_digest,
                        DurableActivationQuarantineReason::AdapterRejected,
                    )
                    .map_err(|_error| CliProviderActivationError::StateUnavailable)?
                {
                    return Err(CliProviderActivationError::StateUnavailable);
                }
                return Err(CliProviderActivationError::Activation(error));
            }
        };
        let ProviderActivationOutcome::Research(activated) = outcome else {
            if !self
                .state
                .quarantine_recipe_if_current(
                    &surface_id,
                    candidate_digest,
                    DurableActivationQuarantineReason::AdapterRejected,
                )
                .map_err(|_error| CliProviderActivationError::StateUnavailable)?
            {
                return Err(CliProviderActivationError::StateUnavailable);
            }
            return Err(CliProviderActivationError::ProviderConfiguration);
        };
        Ok(ProviderPortalActivationView::from_lease(
            activated.profile().clone(),
            activated.lease(),
        ))
    }
}

#[async_trait]
impl ProviderPortalActivationAuthority for ProviderResearchActivationService {
    async fn activate(
        &self,
        session_id: Uuid,
        request: ProviderPortalActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderPortalActivationView, ProviderPortalActivationError> {
        self.activate_from_portal(session_id, request, cancellation)
            .await
            .map_err(map_portal_activation_error)
    }
}

/// Activates one already-onboarded research provider from a closed, no-follow request.
///
/// The request never carries credential bytes or caller-made rights evidence. Provider-specific
/// series metadata and any FRED grant files are read beneath the request's retained input-root
/// capability. Persistence rights come only from the active code-owned onboarding lease.
pub(super) async fn activate_research_provider(
    product: &LocalProduct,
    request_path: &Path,
    confirm: bool,
    cancellation: CancellationToken,
) -> Result<Value, CliProviderActivationError> {
    if !confirm {
        return Err(CliProviderActivationError::ConfirmationRequired);
    }
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    let (root, input, request) = read_request(request_path)?;
    let onboarding = product.provider_onboarding();
    let lease = onboarding
        .activation_lease(request.session_id)
        .map_err(CliProviderActivationError::Onboarding)?;
    require_surface(&lease, request.provider.surface())?;
    let surface_id = lease.surface_id().as_str().to_owned();
    let _activation_guard = product
        .provider_activation_state()
        .acquire_activation(&surface_id)
        .await
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let evidence = LoadedActivationEvidence::from_user(&root, &request)?;
    let session_id = request.session_id;
    let activation = build_research_activation(
        product.paths(),
        &lease,
        input.as_bytes(),
        request,
        &evidence,
    )?;
    if cancellation.is_cancelled() {
        return Err(CliProviderActivationError::Cancelled);
    }
    evidence.persist(product.provider_activation_state())?;
    let candidate_digest = product
        .provider_activation_state()
        .recipe_digest(
            &surface_id,
            session_id,
            input.as_bytes(),
            &evidence.digests(),
        )
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    if product
        .provider_activation()
        .is_research_profile_active(lease.surface_id())
        .map_err(CliProviderActivationError::Activation)?
    {
        if product
            .provider_activation_state()
            .desired_recipe_matches(&surface_id, candidate_digest)
            .map_err(|_| CliProviderActivationError::StateUnavailable)?
        {
            return Ok(activation_result(lease.surface_id(), &lease));
        }
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let published_digest = product
        .provider_activation_state()
        .persist_recipe(
            &surface_id,
            session_id,
            input.as_bytes(),
            &evidence.digests(),
        )
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    if published_digest != candidate_digest {
        return Err(CliProviderActivationError::StateUnavailable);
    }
    let outcome = match product
        .provider_activation()
        .activate_ready_profile(session_id, activation, cancellation)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            if !product
                .provider_activation_state()
                .quarantine_recipe_if_current(
                    &surface_id,
                    candidate_digest,
                    DurableActivationQuarantineReason::AdapterRejected,
                )
                .map_err(|_| CliProviderActivationError::StateUnavailable)?
            {
                return Err(CliProviderActivationError::StateUnavailable);
            }
            return Err(CliProviderActivationError::Activation(error));
        }
    };
    let ProviderActivationOutcome::Research(activated) = outcome else {
        if !product
            .provider_activation_state()
            .quarantine_recipe_if_current(
                &surface_id,
                candidate_digest,
                DurableActivationQuarantineReason::AdapterRejected,
            )
            .map_err(|_| CliProviderActivationError::StateUnavailable)?
        {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        return Err(CliProviderActivationError::ProviderConfiguration);
    };
    Ok(activation_result(activated.profile(), activated.lease()))
}

pub(super) fn restore_research_providers(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
) {
    for surface_id in RESTORABLE_RESEARCH_SURFACES {
        let recipe = match state.load_recipe(surface_id) {
            Ok(DurableActivationRecipeState::Missing) => continue,
            Ok(DurableActivationRecipeState::Quarantined(quarantine)) => {
                enforce_recovery_quarantine(
                    onboarding,
                    surface_id,
                    quarantine.session_id,
                    quarantine.reason,
                    quarantine.state_digest,
                );
                continue;
            }
            Ok(DurableActivationRecipeState::Desired(recipe)) => recipe,
            Err(_error) => {
                quarantine_failed_recovery(
                    onboarding,
                    state,
                    surface_id,
                    None,
                    DurableActivationQuarantineReason::StateInvalid,
                );
                continue;
            }
        };
        let session_id = recipe.session_id;
        let restored = restore_research_provider(
            paths,
            onboarding,
            activation_authority,
            state,
            surface_id,
            recipe,
        );
        match restored {
            Ok(ResearchProviderRecovery::Restored) => {}
            Ok(ResearchProviderRecovery::ResumeRequired) => {
                tracing::warn!(
                    surface_id,
                    "provider activation remains disabled until an explicit user resume"
                );
            }
            Err(error) => {
                quarantine_failed_recovery(
                    onboarding,
                    state,
                    surface_id,
                    Some(session_id),
                    recovery_quarantine_reason(&error),
                );
            }
        }
    }
}

enum ResearchProviderRecovery {
    Restored,
    ResumeRequired,
}

fn restore_research_provider(
    paths: &LocalPaths,
    onboarding: &crate::ProviderOnboardingService,
    activation_authority: &crate::ProviderAdapterActivation,
    state: &DurableProviderActivationState,
    surface_id: &str,
    recipe: super::provider_activation_state::DurableActivationRecipe,
) -> Result<ResearchProviderRecovery, CliProviderActivationError> {
    if recipe.request_bytes.len()
        > usize::try_from(REQUEST_MAXIMUM_BYTES)
            .map_err(|_| CliProviderActivationError::InvalidRequest)?
    {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let request = decode_request(&recipe.request_bytes)?;
    if request.session_id != recipe.session_id {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let lease = onboarding
        .activation_lease(recipe.session_id)
        .map_err(CliProviderActivationError::Onboarding)?;
    if lease.surface_id().as_str() != surface_id {
        return Err(CliProviderActivationError::SurfaceMismatch);
    }
    require_surface(&lease, request.provider.surface())?;
    let evidence = LoadedActivationEvidence::from_durable(state, &request)?;
    if evidence.digests() != recipe.evidence_digests {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    let adapter =
        build_research_activation(paths, &lease, &recipe.request_bytes, request, &evidence)?;
    let outcome = match activation_authority.restore_active_profile(recipe.session_id, adapter) {
        Ok(outcome) => outcome,
        Err(error) if recovery_requires_explicit_resume(&error) => {
            return Ok(ResearchProviderRecovery::ResumeRequired);
        }
        Err(error) => return Err(CliProviderActivationError::Activation(error)),
    };
    if !matches!(outcome, ProviderActivationOutcome::Research(_)) {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    Ok(ResearchProviderRecovery::Restored)
}

fn recovery_requires_explicit_resume(error: &ProviderAdapterActivationError) -> bool {
    matches!(
        error,
        ProviderAdapterActivationError::ExplicitResumeRequired
            | ProviderAdapterActivationError::Onboarding(
                ProviderOnboardingError::SecretOperationUnavailable
                    | ProviderOnboardingError::OperationCancelled
            )
    ) || matches!(
        error,
        ProviderAdapterActivationError::Onboarding(ProviderOnboardingError::SecretStore(
            LocalSecretStoreError::ProviderUnavailable
                | LocalSecretStoreError::SessionUnavailable
                | LocalSecretStoreError::Locked
                | LocalSecretStoreError::InteractionRequired
                | LocalSecretStoreError::UserCancelled
                | LocalSecretStoreError::OperationCancelled
                | LocalSecretStoreError::DeadlineExceeded
        ))
    )
}

fn quarantine_failed_recovery(
    onboarding: &crate::ProviderOnboardingService,
    state: &DurableProviderActivationState,
    surface_id: &str,
    session_id: Option<Uuid>,
    reason: DurableActivationQuarantineReason,
) {
    match state.quarantine_recipe(surface_id, reason) {
        Ok(state_digest) => {
            enforce_recovery_quarantine(onboarding, surface_id, session_id, reason, state_digest);
        }
        Err(_error) => {
            tracing::error!(
                surface_id,
                reason = ?reason,
                "provider activation recovery failed closed; durable quarantine could not be recorded"
            );
        }
    }
}

fn enforce_recovery_quarantine(
    onboarding: &crate::ProviderOnboardingService,
    surface_id: &str,
    session_id: Option<Uuid>,
    reason: DurableActivationQuarantineReason,
    state_digest: EvidenceDigest,
) {
    if let Some(session_id) = session_id
        && onboarding
            .invalidate_activation_recipe(session_id, state_digest)
            .is_err()
    {
        tracing::warn!(
            surface_id,
            reason = ?reason,
            "provider activation recipe is quarantined but its onboarding session could not be blocked"
        );
        return;
    }
    tracing::warn!(
        surface_id,
        reason = ?reason,
        "provider activation recipe is quarantined; re-onboarding is required"
    );
}

fn recovery_quarantine_reason(
    error: &CliProviderActivationError,
) -> DurableActivationQuarantineReason {
    match error {
        CliProviderActivationError::Onboarding(_) => {
            DurableActivationQuarantineReason::AuthorityInvalidated
        }
        CliProviderActivationError::Activation(_) => {
            DurableActivationQuarantineReason::AdapterRejected
        }
        CliProviderActivationError::StateUnavailable
        | CliProviderActivationError::InputUnavailable => {
            DurableActivationQuarantineReason::StateInvalid
        }
        CliProviderActivationError::ConfirmationRequired
        | CliProviderActivationError::InvalidRequest
        | CliProviderActivationError::SurfaceMismatch
        | CliProviderActivationError::InvalidRights
        | CliProviderActivationError::InvalidMetadata
        | CliProviderActivationError::ProviderConfiguration
        | CliProviderActivationError::Cancelled => {
            DurableActivationQuarantineReason::RequestSuperseded
        }
    }
}

fn build_research_activation(
    paths: &LocalPaths,
    lease: &ProviderActivationLease,
    request_bytes: &[u8],
    request: ActivationRequest,
    evidence: &LoadedActivationEvidence,
) -> Result<ProviderAdapterActivationRequest, CliProviderActivationError> {
    let activation_evidence = activation_evidence(request_bytes, lease);
    let metadata_effective =
        EffectiveInterval::new(lease.issued_at(), lease.verification_expires_at())
            .map_err(|_| CliProviderActivationError::InvalidRights)?;
    let activation = match request.provider {
        ProviderRequest::Sec => {
            let metadata = metadata(
                lease,
                activation_evidence,
                "sec",
                "us-sec-edgar",
                SourceClass::RegulatoryFiling,
                CoverageDomain::RegulatoryFilings,
                AuthorizationMode::PublicInterface,
                HistoricalCapability::RevisionPreserving,
                metadata_effective,
                sec_network_policy()?,
                simple_budget("us-sec-edgar", 8, SECOND_NANOS, 4, None)?,
            )?;
            let (raw_store, representations) = sec_state(paths, activation_evidence)?;
            ProviderAdapterActivationRequest::Sec(SecAdapterActivation::new(
                metadata,
                raw_store,
                representations,
                ProviderIdentityRegistry::new(),
                SecParserLimits::production_defaults(),
            ))
        }
        ProviderRequest::Bls {
            series_metadata,
            start_year,
            end_year,
        } => {
            let tier = bls_tier(lease)?;
            let series = bls_series(evidence, &series_metadata, activation_evidence)?;
            let series_ids = series
                .iter()
                .map(|metadata| metadata.series_id().to_owned())
                .collect();
            let plan = BlsRequestPlan::try_new(tier, series_ids, start_year, end_year)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let endpoint = match tier {
                BlsAccessTier::PublicV1 => "https://api.bls.gov/publicAPI/v1/timeseries/data/",
                BlsAccessTier::RegisteredV2 => "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            };
            let authorization_mode = match tier {
                BlsAccessTier::PublicV1 => AuthorizationMode::PublicInterface,
                BlsAccessTier::RegisteredV2 => AuthorizationMode::UserAuthorized,
            };
            let metadata = metadata(
                lease,
                activation_evidence,
                "bls",
                "us-bls",
                SourceClass::OfficialAgency,
                CoverageDomain::Macroeconomic,
                authorization_mode,
                HistoricalCapability::Historical,
                metadata_effective,
                exact_endpoint_policy(endpoint, 16 * 1024 * 1024)?,
                bls_budget(lease, authorization_mode, plan.limits().daily_queries())?,
            )?;
            ProviderAdapterActivationRequest::Bls(BlsAdapterActivation::new(
                metadata, series, start_year, end_year,
            ))
        }
        ProviderRequest::TreasuryFiscal {
            first_record_date,
            last_record_date,
            page_size,
        } => {
            let page_size = NonZeroU16::new(page_size)
                .ok_or(CliProviderActivationError::ProviderConfiguration)?;
            let query = TreasuryFiscalQuery::average_interest_rates_v2(
                first_record_date,
                last_record_date,
                page_size,
            )
            .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let config = TreasurySourceConfig::average_interest_rates(query);
            let metadata =
                treasury_metadata(lease, activation_evidence, metadata_effective, &config)?;
            ProviderAdapterActivationRequest::Treasury(TreasuryAdapterActivation::new(
                metadata, config,
            ))
        }
        ProviderRequest::TreasuryDailyRates { year } => {
            let config = TreasurySourceConfig::daily_par_yield_curve(year)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            let metadata =
                treasury_metadata(lease, activation_evidence, metadata_effective, &config)?;
            ProviderAdapterActivationRequest::Treasury(TreasuryAdapterActivation::new(
                metadata, config,
            ))
        }
        ProviderRequest::FredAlfred { configuration } => {
            let FredProviderRequest {
                rights_artifact,
                api_terms,
                services_legal_terms,
                privacy_policy,
                grants,
            } = *configuration;
            let policy = fred_policy(
                evidence,
                &rights_artifact,
                &api_terms,
                &services_legal_terms,
                &privacy_policy,
                grants,
            )?;
            let metadata = metadata(
                lease,
                activation_evidence,
                "fred",
                "fred",
                SourceClass::OfficialAgency,
                CoverageDomain::Macroeconomic,
                AuthorizationMode::UserAuthorized,
                HistoricalCapability::RevisionPreserving,
                metadata_effective,
                fred_network_policy()?,
                simple_budget(
                    "fred",
                    120,
                    MINUTE_NANOS,
                    2,
                    Some(authorization_subject(lease)?),
                )?,
            )?;
            ProviderAdapterActivationRequest::Fred(FredAdapterActivation::new(metadata, policy))
        }
    };
    Ok(activation)
}

fn activation_result(profile: &SourceIdentifier, lease: &ProviderActivationLease) -> Value {
    json!({
        "profile": profile.as_str(),
        "sessionId": lease.session_id().to_string(),
        "capabilityRevision": lease.capability_revision().get(),
        "capabilityEvidence": lease.capability_digest(),
        "rightsDecisionEvidence": lease.rights_decision_digest(),
        "persistenceRightsEvidence": lease.persistence_evidence(),
        "publicConfigurationEvidence": lease.public_configuration_digest(),
        "credentialGeneration": lease.generation().map(|generation| generation.get()),
        "verificationExpiresAtUnixNanos": lease
            .verification_expires_at()
            .map(Timestamp::unix_nanos),
        "issuedAtUnixNanos": lease.issued_at().unix_nanos(),
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ActivationRequest {
    schema_version: u16,
    session_id: Uuid,
    provider: ProviderRequest,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum ProviderRequest {
    Sec,
    Bls {
        series_metadata: Vec<ExactInputReference>,
        start_year: u16,
        end_year: u16,
    },
    TreasuryFiscal {
        first_record_date: CalendarDate,
        last_record_date: CalendarDate,
        page_size: u16,
    },
    TreasuryDailyRates {
        year: u16,
    },
    FredAlfred {
        configuration: Box<FredProviderRequest>,
    },
}

impl ProviderRequest {
    const fn surface(&self) -> ProviderSurface {
        match self {
            Self::Sec => ProviderSurface::Exact(SEC_SURFACE),
            Self::Bls { .. } => ProviderSurface::Either(BLS_PUBLIC_SURFACE, BLS_REGISTERED_SURFACE),
            Self::TreasuryFiscal { .. } => ProviderSurface::Exact(TREASURY_FISCAL_SURFACE),
            Self::TreasuryDailyRates { .. } => ProviderSurface::Exact(TREASURY_XML_SURFACE),
            Self::FredAlfred { .. } => ProviderSurface::Exact(FRED_SURFACE),
        }
    }
}

fn portal_provider_request(
    session_id: Uuid,
    request: ProviderPortalActivationRequest,
) -> Result<(ProviderRequest, LoadedActivationEvidence), CliProviderActivationError> {
    match request {
        ProviderPortalActivationRequest::Sec => Ok((
            ProviderRequest::Sec,
            LoadedActivationEvidence {
                objects: BTreeMap::new(),
            },
        )),
        ProviderPortalActivationRequest::TreasuryFiscal {
            first_record_date,
            last_record_date,
            page_size,
        } => Ok((
            ProviderRequest::TreasuryFiscal {
                first_record_date,
                last_record_date,
                page_size,
            },
            LoadedActivationEvidence {
                objects: BTreeMap::new(),
            },
        )),
        ProviderPortalActivationRequest::Bls {
            series,
            start_year,
            end_year,
        } => {
            if series.is_empty() || series.len() > MAXIMUM_BLS_SERIES {
                return Err(CliProviderActivationError::ProviderConfiguration);
            }
            let authorization = SourceIdentifier::try_from(format!("portal-session-{session_id}"))
                .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?;
            let mut objects = BTreeMap::new();
            let mut references = Vec::new();
            for (index, input) in series.into_iter().enumerate() {
                let metadata = BlsSeriesMetadata::from_verified_input(input, authorization.clone())
                    .map_err(|_error| CliProviderActivationError::ProviderConfiguration)?;
                let digest = metadata.evidence().content_digest();
                let sha256 = lower_hex(&digest.bytes());
                let reference = ExactInputReference {
                    path: PathBuf::from(format!("portal-series-{index}.json")),
                    sha256,
                };
                insert_evidence(
                    &mut objects,
                    &reference,
                    ExactActivationInput {
                        bytes: Arc::from(metadata.exact_payload()),
                        digest,
                    },
                )?;
                references.push(reference);
            }
            Ok((
                ProviderRequest::Bls {
                    series_metadata: references,
                    start_year,
                    end_year,
                },
                LoadedActivationEvidence { objects },
            ))
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FredProviderRequest {
    rights_artifact: ExactInputReference,
    api_terms: ExactInputReference,
    services_legal_terms: ExactInputReference,
    privacy_policy: ExactInputReference,
    grants: Vec<FredGrantRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FredGrantRequest {
    series: SourceIdentifier,
    owner: SourceIdentifier,
    authorization_url: String,
    authorization_document: ExactInputReference,
    operations: Vec<FredOperation>,
    effective_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExactInputReference {
    path: PathBuf,
    sha256: String,
}

#[derive(Clone)]
struct ExactActivationInput {
    bytes: Arc<[u8]>,
    digest: EvidenceDigest,
}

impl ExactActivationInput {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

struct LoadedActivationEvidence {
    objects: BTreeMap<String, ExactActivationInput>,
}

impl LoadedActivationEvidence {
    fn from_user(
        root: &UserAuthorizedInputRoot,
        request: &ActivationRequest,
    ) -> Result<Self, CliProviderActivationError> {
        let mut objects = BTreeMap::new();
        for bounded in evidence_references(request)? {
            let input = read_exact_input(root, bounded.reference, bounded.maximum_bytes)?;
            let exact = ExactActivationInput {
                bytes: Arc::from(input.as_bytes()),
                digest: input.digest(),
            };
            insert_evidence(&mut objects, bounded.reference, exact)?;
        }
        Ok(Self { objects })
    }

    fn from_durable(
        state: &DurableProviderActivationState,
        request: &ActivationRequest,
    ) -> Result<Self, CliProviderActivationError> {
        let mut objects = BTreeMap::new();
        for bounded in evidence_references(request)? {
            let input = state
                .load_evidence(&bounded.reference.sha256, bounded.maximum_bytes)
                .map_err(|_| CliProviderActivationError::StateUnavailable)?;
            let exact = ExactActivationInput {
                bytes: Arc::from(input.as_bytes()),
                digest: input.digest(),
            };
            insert_evidence(&mut objects, bounded.reference, exact)?;
        }
        Ok(Self { objects })
    }

    fn read(
        &self,
        reference: &ExactInputReference,
        maximum_bytes: u64,
    ) -> Result<ExactActivationInput, CliProviderActivationError> {
        let expected = validate_reference_digest(reference)?;
        let input = self
            .objects
            .get(&reference.sha256)
            .ok_or(CliProviderActivationError::InputUnavailable)?;
        let length = u64::try_from(input.as_bytes().len())
            .map_err(|_| CliProviderActivationError::InputUnavailable)?;
        if length > maximum_bytes
            || input.digest().algorithm() != DigestAlgorithm::Sha256
            || input.digest().bytes() != expected.bytes()
        {
            return Err(CliProviderActivationError::InputUnavailable);
        }
        Ok(input.clone())
    }

    fn persist(
        &self,
        state: &DurableProviderActivationState,
    ) -> Result<(), CliProviderActivationError> {
        for (digest, input) in &self.objects {
            state
                .persist_evidence(digest, input.as_bytes())
                .map_err(|_| CliProviderActivationError::StateUnavailable)?;
        }
        Ok(())
    }

    fn digests(&self) -> Vec<String> {
        self.objects.keys().cloned().collect()
    }
}

#[derive(Clone, Copy)]
struct BoundedExactReference<'a> {
    reference: &'a ExactInputReference,
    maximum_bytes: u64,
}

enum ProviderSurface {
    Exact(&'static str),
    Either(&'static str, &'static str),
}

fn evidence_references(
    request: &ActivationRequest,
) -> Result<Vec<BoundedExactReference<'_>>, CliProviderActivationError> {
    let mut references = Vec::new();
    match &request.provider {
        ProviderRequest::Sec
        | ProviderRequest::TreasuryFiscal { .. }
        | ProviderRequest::TreasuryDailyRates { .. } => {}
        ProviderRequest::Bls {
            series_metadata, ..
        } => {
            if series_metadata.is_empty() || series_metadata.len() > MAXIMUM_BLS_SERIES {
                return Err(CliProviderActivationError::ProviderConfiguration);
            }
            references.extend(
                series_metadata
                    .iter()
                    .map(|reference| BoundedExactReference {
                        reference,
                        maximum_bytes: BLS_SERIES_METADATA_MAXIMUM_BYTES,
                    }),
            );
        }
        ProviderRequest::FredAlfred { configuration } => {
            if configuration.grants.len() > MAXIMUM_FRED_GRANTS {
                return Err(CliProviderActivationError::InvalidRights);
            }
            let terms_maximum = u64::try_from(MAX_FRED_TERMS_DOCUMENT_BYTES)
                .map_err(|_| CliProviderActivationError::InvalidRights)?;
            references.extend([
                BoundedExactReference {
                    reference: &configuration.rights_artifact,
                    maximum_bytes: FRED_RIGHTS_ARTIFACT_MAXIMUM_BYTES,
                },
                BoundedExactReference {
                    reference: &configuration.api_terms,
                    maximum_bytes: terms_maximum,
                },
                BoundedExactReference {
                    reference: &configuration.services_legal_terms,
                    maximum_bytes: terms_maximum,
                },
                BoundedExactReference {
                    reference: &configuration.privacy_policy,
                    maximum_bytes: terms_maximum,
                },
            ]);
            references.extend(
                configuration
                    .grants
                    .iter()
                    .map(|grant| BoundedExactReference {
                        reference: &grant.authorization_document,
                        maximum_bytes: FRED_AUTHORIZATION_MAXIMUM_BYTES,
                    }),
            );
        }
    }
    Ok(references)
}

fn insert_evidence(
    objects: &mut BTreeMap<String, ExactActivationInput>,
    reference: &ExactInputReference,
    input: ExactActivationInput,
) -> Result<(), CliProviderActivationError> {
    let expected = validate_reference_digest(reference)?;
    if input.digest().algorithm() != DigestAlgorithm::Sha256
        || input.digest().bytes() != expected.bytes()
    {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    if let Some(existing) = objects.get(&reference.sha256) {
        if existing.as_bytes() != input.as_bytes() || existing.digest() != input.digest() {
            return Err(CliProviderActivationError::InvalidRequest);
        }
        return Ok(());
    }
    objects.insert(reference.sha256.clone(), input);
    Ok(())
}

fn validate_reference_digest(
    reference: &ExactInputReference,
) -> Result<Sha256Digest, CliProviderActivationError> {
    Sha256Digest::from_lower_hex(&reference.sha256)
        .map_err(|_| CliProviderActivationError::InvalidRequest)
}

fn read_request(
    path: &Path,
) -> Result<(UserAuthorizedInputRoot, BoundedInput, ActivationRequest), CliProviderActivationError>
{
    let absolute =
        std::path::absolute(path).map_err(|_| CliProviderActivationError::InputUnavailable)?;
    if absolute
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(CliProviderActivationError::InputUnavailable);
    }
    let parent = absolute
        .parent()
        .ok_or(CliProviderActivationError::InputUnavailable)?;
    let name = absolute
        .file_name()
        .ok_or(CliProviderActivationError::InputUnavailable)?;
    let root = UserAuthorizedInputRoot::open(parent)
        .map_err(|_| CliProviderActivationError::InputUnavailable)?;
    let input = read_input(&root, Path::new(name), REQUEST_MAXIMUM_BYTES)?;
    let request = decode_request(input.as_bytes())?;
    Ok((root, input, request))
}

fn decode_request(bytes: &[u8]) -> Result<ActivationRequest, CliProviderActivationError> {
    let request: ActivationRequest =
        serde_json::from_slice(bytes).map_err(|_| CliProviderActivationError::InvalidRequest)?;
    if request.schema_version != REQUEST_SCHEMA_VERSION {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    Ok(request)
}

fn read_input(
    root: &UserAuthorizedInputRoot,
    reference: &Path,
    maximum_bytes: u64,
) -> Result<BoundedInput, CliProviderActivationError> {
    root.resolve(reference)
        .and_then(|input| input.open_bounded(maximum_bytes))
        .and_then(|input| input.read_bounded())
        .map_err(|_| CliProviderActivationError::InputUnavailable)
}

fn read_exact_input(
    root: &UserAuthorizedInputRoot,
    reference: &ExactInputReference,
    maximum_bytes: u64,
) -> Result<BoundedInput, CliProviderActivationError> {
    let input = read_input(root, &reference.path, maximum_bytes)?;
    let expected = validate_reference_digest(reference)?;
    if input.digest().algorithm() != DigestAlgorithm::Sha256
        || input.digest().bytes() != expected.bytes()
    {
        return Err(CliProviderActivationError::InvalidRequest);
    }
    Ok(input)
}

fn require_surface(
    lease: &ProviderActivationLease,
    expected: ProviderSurface,
) -> Result<(), CliProviderActivationError> {
    let actual = lease.surface_id().as_str();
    let matches = match expected {
        ProviderSurface::Exact(expected) => actual == expected,
        ProviderSurface::Either(first, second) => actual == first || actual == second,
    };
    if matches {
        Ok(())
    } else {
        Err(CliProviderActivationError::SurfaceMismatch)
    }
}

fn activation_evidence(input: &[u8], lease: &ProviderActivationLease) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk:cli-provider-activation:v1");
    hasher.update([0]);
    hasher.update(input);
    hasher.update([1]);
    update_digest(&mut hasher, lease.capability_digest());
    hasher.update([2]);
    update_digest(&mut hasher, lease.public_configuration_digest());
    hasher.update([3]);
    update_digest(&mut hasher, lease.rights_decision_digest());
    hasher.update([4]);
    hasher.update(lease.session_id().as_bytes());
    hasher.update([5]);
    hasher.update(
        lease
            .generation()
            .map_or(0_u64, |generation| generation.get())
            .to_be_bytes(),
    );
    hasher.update([6]);
    hasher.update(
        lease
            .verification_expires_at()
            .map_or(i64::MIN, Timestamp::unix_nanos)
            .to_be_bytes(),
    );
    EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into())
}

fn update_digest(hasher: &mut Sha256, digest: EvidenceDigest) {
    hasher.update(match digest.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    hasher.update(digest.bytes());
}

fn source_id(
    provider: &str,
    evidence: EvidenceDigest,
) -> Result<SourceId, CliProviderActivationError> {
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidRequest)?;
    SourceId::try_from(format!("{provider}-{short}"))
        .map_err(|_| CliProviderActivationError::InvalidRequest)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every parameter is an independent source-metadata authority dimension"
)]
fn metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    source_tag: &str,
    provider: &str,
    source_class: SourceClass,
    coverage_domain: CoverageDomain,
    authorization_mode: AuthorizationMode,
    historical: HistoricalCapability,
    effective: EffectiveInterval,
    network: EndpointPolicy,
    budget: ProviderBudgetPolicy,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let source_id = source_id(source_tag, evidence)?;
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::InvalidMetadata)?;
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!("activation-{short}"))
            .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    );
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let basis = match authorization_mode {
        AuthorizationMode::PublicInterface => {
            SourceIdentifier::try_from("official-public-interface")
                .map_err(|_| CliProviderActivationError::InvalidMetadata)?
        }
        AuthorizationMode::UserAuthorized => authorization_subject(lease)?,
        AuthorizationMode::Licensed | AuthorizationMode::UserOwnedLocal => {
            return Err(CliProviderActivationError::InvalidMetadata);
        }
    };
    let exact = ExactPayloadEvidence::from_content_digest(evidence);
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, exact.clone()),
        source_class,
        provider,
        AuthorizationGrant::new(
            authorization_mode,
            AuthorizationBasis::new(basis),
            exact.clone(),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            exact,
            effective,
            coverage_domain,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Allowlisted(network),
        FreshnessPolicy::try_new(
            MINUTE_NANOS,
            MINUTE_NANOS,
            DAY_NANOS,
            DAY_NANOS,
            SECOND_NANOS,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            historical,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn authorization_subject(
    lease: &ProviderActivationLease,
) -> Result<SourceIdentifier, CliProviderActivationError> {
    SourceIdentifier::try_from(format!("provider-session-{}", lease.session_id().simple()))
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn exact_endpoint_policy(
    endpoint: &str,
    response_bytes: u64,
) -> Result<EndpointPolicy, CliProviderActivationError> {
    let rule = ApiEndpointRule::try_new(endpoint, PathScope::Exact, Vec::new(), 1, 1)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(response_bytes)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn request_bounds(response_bytes: u64) -> Result<HttpRequestBounds, CliProviderActivationError> {
    HttpRequestBounds::try_new(
        nonzero_u64(5 * SECOND_NANOS)?,
        nonzero_u64(30 * SECOND_NANOS)?,
        nonzero_u64(45 * SECOND_NANOS)?,
        0,
        nonzero_u64(response_bytes)?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn simple_budget(
    provider: &str,
    requests: u32,
    window_nanos: u64,
    concurrency: u16,
    authorization_account: Option<SourceIdentifier>,
) -> Result<ProviderBudgetPolicy, CliProviderActivationError> {
    let provider = SourceIdentifier::try_from(provider)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let scope = match authorization_account {
        Some(account) => BudgetScope::with_authorization_account(provider, account),
        None => BudgetScope::new(provider),
    };
    ProviderBudgetPolicy::try_new(
        scope,
        NonZeroU32::new(requests).ok_or(CliProviderActivationError::InvalidMetadata)?,
        nonzero_u64(window_nanos)?,
        NonZeroU16::new(concurrency).ok_or(CliProviderActivationError::InvalidMetadata)?,
        backoff()?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn backoff() -> Result<BackoffPolicy, CliProviderActivationError> {
    BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, CliProviderActivationError> {
    NonZeroU64::new(value).ok_or(CliProviderActivationError::InvalidMetadata)
}

fn bls_tier(lease: &ProviderActivationLease) -> Result<BlsAccessTier, CliProviderActivationError> {
    match lease.surface_id().as_str() {
        BLS_PUBLIC_SURFACE => Ok(BlsAccessTier::PublicV1),
        BLS_REGISTERED_SURFACE => Ok(BlsAccessTier::RegisteredV2),
        _ => Err(CliProviderActivationError::SurfaceMismatch),
    }
}

fn bls_series(
    inputs: &LoadedActivationEvidence,
    references: &[ExactInputReference],
    evidence: EvidenceDigest,
) -> Result<Vec<BlsSeriesMetadata>, CliProviderActivationError> {
    if references.is_empty() || references.len() > MAXIMUM_BLS_SERIES {
        return Err(CliProviderActivationError::ProviderConfiguration);
    }
    let digest = lower_hex(&evidence.bytes());
    let short = digest
        .get(..24)
        .ok_or(CliProviderActivationError::ProviderConfiguration)?;
    let authorization = SourceIdentifier::try_from(format!("bls-series-review-{short}"))
        .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
    references
        .iter()
        .map(|reference| {
            let input = inputs.read(reference, BLS_SERIES_METADATA_MAXIMUM_BYTES)?;
            BlsSeriesMetadata::parse_exact(
                Bytes::copy_from_slice(input.as_bytes()),
                ExactPayloadEvidence::from_content_digest(input.digest()),
                authorization.clone(),
            )
            .map_err(|_| CliProviderActivationError::ProviderConfiguration)
        })
        .collect()
}

fn bls_budget(
    lease: &ProviderActivationLease,
    mode: AuthorizationMode,
    daily_queries: u16,
) -> Result<ProviderBudgetPolicy, CliProviderActivationError> {
    let provider = SourceIdentifier::try_from("us-bls")
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let account = match mode {
        AuthorizationMode::PublicInterface => None,
        AuthorizationMode::UserAuthorized => Some(authorization_subject(lease)?),
        AuthorizationMode::Licensed | AuthorizationMode::UserOwnedLocal => {
            return Err(CliProviderActivationError::InvalidMetadata);
        }
    };
    let scope = match account {
        Some(account) => BudgetScope::with_authorization_account(provider, account),
        None => BudgetScope::new(provider),
    };
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(50).ok_or(CliProviderActivationError::InvalidMetadata)?,
            nonzero_u64(10 * SECOND_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(u32::from(daily_queries))
                .ok_or(CliProviderActivationError::InvalidMetadata)?,
            nonzero_u64(DAY_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
    ];
    ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &windows,
        NonZeroU16::new(2).ok_or(CliProviderActivationError::InvalidMetadata)?,
        backoff()?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn sec_network_policy() -> Result<EndpointPolicy, CliProviderActivationError> {
    let rules = [
        ("https://data.sec.gov/submissions", PathScope::Descendants),
        (
            "https://data.sec.gov/api/xbrl/companyfacts",
            PathScope::Descendants,
        ),
        (
            "https://www.sec.gov/Archives/edgar/data",
            PathScope::Descendants,
        ),
    ]
    .into_iter()
    .map(|(endpoint, scope)| {
        ApiEndpointRule::try_new(endpoint, scope, Vec::new(), 1, 1)
            .map_err(|_| CliProviderActivationError::InvalidMetadata)
    })
    .collect::<Result<Vec<_>, _>>()?;
    EndpointPolicy::try_from_api_rules(rules, request_bounds(64 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn sec_state(
    paths: &LocalPaths,
    evidence: EvidenceDigest,
) -> Result<(RawEvidenceStore, SecRepresentationRegistry), CliProviderActivationError> {
    let control = paths
        .control_root()
        .map_err(|_| CliProviderActivationError::StateUnavailable)?
        .try_clone_directory()
        .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    let digest = lower_hex(&evidence.bytes());
    let base = open_or_create(
        control,
        &["sources", "provider-adapters", "sec", digest.as_str()],
    )?;
    let raw = open_or_create(
        base.try_clone()
            .map_err(|_| CliProviderActivationError::StateUnavailable)?,
        &["raw"],
    )?;
    let representations = open_or_create(base, &["representations"])?;
    let registry = SecRepresentationRegistry::open(
        representations,
        SecRepresentationLimits::production_defaults(),
    )
    .map_err(|_| CliProviderActivationError::StateUnavailable)?;
    Ok((RawEvidenceStore::new(raw), registry))
}

fn open_or_create(
    mut directory: Dir,
    components: &[&str],
) -> Result<Dir, CliProviderActivationError> {
    for component in components {
        directory = match directory.open_dir_nofollow(*component) {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match directory.create_dir(*component) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(_) => return Err(CliProviderActivationError::StateUnavailable),
                }
                directory
                    .open_dir_nofollow(*component)
                    .map_err(|_| CliProviderActivationError::StateUnavailable)?
            }
            Err(_) => return Err(CliProviderActivationError::StateUnavailable),
        };
    }
    Ok(directory)
}

fn treasury_metadata(
    lease: &ProviderActivationLease,
    evidence: EvidenceDigest,
    effective: EffectiveInterval,
    config: &TreasurySourceConfig,
) -> Result<SourceMetadata, CliProviderActivationError> {
    let rule = match config {
        TreasurySourceConfig::AverageInterestRates(query) => {
            let page = query
                .page(1)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            ApiEndpointRule::try_new(
                without_query(page.url())?,
                PathScope::Exact,
                query_rules(&[
                    ("fields", 1_024),
                    ("filter", 512),
                    ("sort", 128),
                    ("page[number]", 20),
                    ("page[size]", 5),
                ])?,
                5,
                4_096,
            )
        }
        TreasurySourceConfig::DailyParYieldCurve { profile, year } => {
            let page = profile
                .page(*year, 0)
                .map_err(|_| CliProviderActivationError::ProviderConfiguration)?;
            ApiEndpointRule::try_new(
                without_query(page.url())?,
                PathScope::Exact,
                query_rules(&[("data", 64), ("field_tdr_date_value", 4)])?,
                2,
                256,
            )
        }
    }
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let network = EndpointPolicy::try_from_api_rules(vec![rule], request_bounds(64 * 1024 * 1024)?)
        .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    metadata(
        lease,
        evidence,
        "treasury",
        "us-treasury",
        SourceClass::OfficialAgency,
        CoverageDomain::Macroeconomic,
        AuthorizationMode::PublicInterface,
        HistoricalCapability::Historical,
        effective,
        network,
        simple_budget("us-treasury", 100, MINUTE_NANOS, 2, None)?,
    )
}

fn query_rules(
    rules: &[(&str, u16)],
) -> Result<Vec<QueryParameterRule>, CliProviderActivationError> {
    rules
        .iter()
        .map(|(key, maximum)| {
            QueryParameterRule::try_new(
                SourceIdentifier::try_from(*key)
                    .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
                *maximum,
                false,
                QuerySensitivity::Public,
            )
            .map_err(|_| CliProviderActivationError::InvalidMetadata)
        })
        .collect()
}

fn without_query(url: &str) -> Result<&str, CliProviderActivationError> {
    url.split('?')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or(CliProviderActivationError::InvalidMetadata)
}

fn fred_network_policy() -> Result<EndpointPolicy, CliProviderActivationError> {
    let rules = [
        ("api_key", QuerySensitivity::Secret, 32),
        ("series_id", QuerySensitivity::Public, 120),
        ("realtime_start", QuerySensitivity::Public, 10),
        ("realtime_end", QuerySensitivity::Public, 10),
        ("limit", QuerySensitivity::Public, 6),
        ("offset", QuerySensitivity::Public, 20),
        ("sort_order", QuerySensitivity::Public, 4),
        ("order_by", QuerySensitivity::Public, 32),
        ("output_type", QuerySensitivity::Public, 1),
        ("file_type", QuerySensitivity::Public, 4),
    ]
    .into_iter()
    .map(|(key, sensitivity, maximum)| {
        QueryParameterRule::try_new(
            SourceIdentifier::try_from(key)
                .map_err(|_| CliProviderActivationError::InvalidMetadata)?,
            maximum,
            false,
            sensitivity,
        )
        .map_err(|_| CliProviderActivationError::InvalidMetadata)
    })
    .collect::<Result<Vec<_>, _>>()?;
    let observations = ApiEndpointRule::try_new(
        "https://api.stlouisfed.org/fred/series/observations",
        PathScope::Exact,
        rules,
        10,
        1_024,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    let series =
        fred_series_endpoint_rule().map_err(|_| CliProviderActivationError::InvalidMetadata)?;
    EndpointPolicy::try_from_api_rules(
        vec![observations, series],
        request_bounds(64 * 1024 * 1024)?,
    )
    .map_err(|_| CliProviderActivationError::InvalidMetadata)
}

fn fred_policy(
    inputs: &LoadedActivationEvidence,
    artifact_reference: &ExactInputReference,
    api_terms_reference: &ExactInputReference,
    services_reference: &ExactInputReference,
    privacy_reference: &ExactInputReference,
    grants: Vec<FredGrantRequest>,
) -> Result<FredRightsPolicy, CliProviderActivationError> {
    if grants.len() > MAXIMUM_FRED_GRANTS {
        return Err(CliProviderActivationError::InvalidRights);
    }
    let artifact = inputs.read(artifact_reference, FRED_RIGHTS_ARTIFACT_MAXIMUM_BYTES)?;
    let api_terms = inputs.read(
        api_terms_reference,
        u64::try_from(MAX_FRED_TERMS_DOCUMENT_BYTES)
            .map_err(|_| CliProviderActivationError::InvalidRights)?,
    )?;
    let services = inputs.read(
        services_reference,
        u64::try_from(MAX_FRED_TERMS_DOCUMENT_BYTES)
            .map_err(|_| CliProviderActivationError::InvalidRights)?,
    )?;
    let privacy = inputs.read(
        privacy_reference,
        u64::try_from(MAX_FRED_TERMS_DOCUMENT_BYTES)
            .map_err(|_| CliProviderActivationError::InvalidRights)?,
    )?;
    let documents = [
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::ApiTerms, api_terms.as_bytes()),
        FredTermsDocumentBytes::try_new(
            FredTermsDocumentRole::FredServicesLegalTerms,
            services.as_bytes(),
        ),
        FredTermsDocumentBytes::try_new(FredTermsDocumentRole::PrivacyPolicy, privacy.as_bytes()),
    ]
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .map_err(|_| CliProviderActivationError::InvalidRights)?;
    let artifact = FredRightsArtifact::parse(artifact.as_bytes(), &documents)
        .map_err(|_| CliProviderActivationError::InvalidRights)?;
    let terms_digest = artifact.terms_evidence().bundle_digest();
    let grants = grants
        .into_iter()
        .map(|grant| {
            let authorization = inputs.read(
                &grant.authorization_document,
                FRED_AUTHORIZATION_MAXIMUM_BYTES,
            )?;
            let authorization_digest = Sha256Digest::from_bytes(authorization.digest().bytes());
            let evidence = FredOwnerAuthorizationEvidence::try_new(
                grant.authorization_url,
                authorization_digest,
                authorization.as_bytes().len(),
                authorization.as_bytes(),
            )
            .map_err(|_| CliProviderActivationError::InvalidRights)?;
            FredSeriesRightsGrant::try_new(
                grant.series,
                grant.owner,
                evidence,
                terms_digest,
                grant.operations,
                Timestamp::from_unix_nanos(grant.effective_at_unix_nanos),
                Timestamp::from_unix_nanos(grant.expires_at_unix_nanos),
            )
            .map_err(|_| CliProviderActivationError::InvalidRights)
        })
        .collect::<Result<Vec<_>, _>>()?;
    FredRightsPolicy::try_new(artifact.terms_evidence().clone(), grants)
        .map_err(|_| CliProviderActivationError::InvalidRights)
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn map_portal_activation_error(error: CliProviderActivationError) -> ProviderPortalActivationError {
    match error {
        CliProviderActivationError::Cancelled => ProviderPortalActivationError::Cancelled,
        CliProviderActivationError::StateUnavailable
        | CliProviderActivationError::InputUnavailable => {
            ProviderPortalActivationError::StateUnavailable
        }
        CliProviderActivationError::InvalidRequest
        | CliProviderActivationError::InvalidRights
        | CliProviderActivationError::InvalidMetadata
        | CliProviderActivationError::ProviderConfiguration
        | CliProviderActivationError::SurfaceMismatch
        | CliProviderActivationError::ConfirmationRequired => {
            ProviderPortalActivationError::InvalidRequest
        }
        CliProviderActivationError::Onboarding(_) | CliProviderActivationError::Activation(_) => {
            ProviderPortalActivationError::Unavailable
        }
    }
}

/// Closed provider-activation failure without path, secret, or response-body disclosure.
#[derive(Debug, Error)]
pub enum CliProviderActivationError {
    #[error("provider activation requires explicit confirmation")]
    ConfirmationRequired,
    #[error("provider activation input is unavailable")]
    InputUnavailable,
    #[error("provider activation request is invalid")]
    InvalidRequest,
    #[error("provider activation request does not match the active profile")]
    SurfaceMismatch,
    #[error("provider activation rights evidence is invalid")]
    InvalidRights,
    #[error("provider activation metadata is invalid")]
    InvalidMetadata,
    #[error("provider activation configuration is invalid")]
    ProviderConfiguration,
    #[error("provider activation state is unavailable")]
    StateUnavailable,
    #[error("provider activation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Onboarding(crate::ProviderOnboardingError),
    #[error(transparent)]
    Activation(crate::ProviderAdapterActivationError),
}
