//! Bounded application-owned Schwab market-data entitlement doctor.
//!
//! This module composes four authorities without taking ownership away from them: the protected
//! OAuth market authority, the shared provider-rate authority, the sole physical raw sealer, and
//! the bounded provider-native probe executor. Every REST and Streamer family must return exact
//! attempted evidence. A missing, unsealed, or family-ambiguous result fails closed rather than
//! being presented as unavailable.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_schwab::{
    MarketDataService, SchwabOAuthAuthorityReceipt, SchwabObservedCapabilityFamily,
};
use market_squawk_data::IngestError;
use market_squawk_domain::{
    CoverageDelay, DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::SecretGeneration;
use market_squawk_sources::{
    ProviderCapabilityRevision, ProviderCaptureSealRequest, RuntimeCapabilityDisposition,
    SCHWAB_MARKET_DATA_SURFACE_ID, SchwabMarketDataDoctorObservation,
    SchwabMarketDataDoctorReceiptInput, SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily,
    SchwabMarketDataFamilyEvidence, SchwabUserPreferenceDoctorEvidence,
    SealedProviderCaptureMaterial,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::schwab_oauth_runtime::SchwabOAuthMarketAuthority;
use crate::research_service::{ResearchService, ResearchServiceError};

const MAX_RETRY_AFTER_BYTES: usize = 256;
const USER_PREFERENCE_ENDPOINT_CONTRACT: &[u8] =
    b"market-squawk/schwab-user-preference-read-only/v1";
const FAMILY_OBSERVATION_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/schwab-market-doctor-family-observation/v1";
const FAMILY_DISPOSITION_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/schwab-market-doctor-family-disposition/v1";
const SETUP_REQUIRED_DIGEST_DOMAIN: &[u8] = b"market-squawk/schwab-market-doctor-setup-required/v1";

const REST_FAMILIES: [SchwabMarketDataFamily; 7] = [
    SchwabMarketDataFamily::Quotes,
    SchwabMarketDataFamily::PriceHistory,
    SchwabMarketDataFamily::OptionChains,
    SchwabMarketDataFamily::ExpirationChains,
    SchwabMarketDataFamily::Movers,
    SchwabMarketDataFamily::MarketHours,
    SchwabMarketDataFamily::Instruments,
];

const STREAMER_FAMILIES: [SchwabMarketDataFamily; 12] = [
    SchwabMarketDataFamily::LevelOneEquities,
    SchwabMarketDataFamily::LevelOneOptions,
    SchwabMarketDataFamily::LevelOneFutures,
    SchwabMarketDataFamily::LevelOneFuturesOptions,
    SchwabMarketDataFamily::LevelOneForex,
    SchwabMarketDataFamily::NyseBook,
    SchwabMarketDataFamily::NasdaqBook,
    SchwabMarketDataFamily::OptionsBook,
    SchwabMarketDataFamily::ChartEquity,
    SchwabMarketDataFamily::ChartFutures,
    SchwabMarketDataFamily::ScreenerEquity,
    SchwabMarketDataFamily::ScreenerOption,
];

pub(crate) type DoctorFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SchwabMarketDataDoctorError>> + Send + 'a>>;

/// Exact durable authority that a successful doctor receipt must retain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchwabMarketDoctorAuthorityBinding {
    surface_id: SourceIdentifier,
    session_id: Uuid,
    application_credential_generation: SecretGeneration,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    public_configuration_digest: EvidenceDigest,
    rights_decision_digest: EvidenceDigest,
    rate_policy_digest: EvidenceDigest,
    predecessor_digest: Option<EvidenceDigest>,
}

impl SchwabMarketDoctorAuthorityBinding {
    #[allow(
        clippy::too_many_arguments,
        reason = "every independent receipt authority remains explicit"
    )]
    pub(crate) fn try_new(
        surface_id: SourceIdentifier,
        session_id: Uuid,
        application_credential_generation: SecretGeneration,
        capability_revision: ProviderCapabilityRevision,
        capability_digest: EvidenceDigest,
        public_configuration_digest: EvidenceDigest,
        rights_decision_digest: EvidenceDigest,
        rate_policy_digest: EvidenceDigest,
        predecessor_digest: Option<EvidenceDigest>,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        if surface_id.as_str() != SCHWAB_MARKET_DATA_SURFACE_ID || session_id.is_nil() {
            return Err(SchwabMarketDataDoctorError::InvalidAuthority);
        }
        for digest in [
            capability_digest,
            public_configuration_digest,
            rights_decision_digest,
            rate_policy_digest,
        ] {
            require_digest(digest)?;
        }
        if let Some(digest) = predecessor_digest {
            require_digest(digest)?;
        }
        Ok(Self {
            surface_id,
            session_id,
            application_credential_generation,
            capability_revision,
            capability_digest,
            public_configuration_digest,
            rights_decision_digest,
            rate_policy_digest,
            predecessor_digest,
        })
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub(crate) const fn rate_policy_digest(&self) -> EvidenceDigest {
        self.rate_policy_digest
    }
}

/// One exact provider-rate admission scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "transport", content = "family", rename_all = "snake_case")]
pub(crate) enum SchwabMarketDoctorProbeScope {
    UserPreference,
    Rest(SchwabMarketDataFamily),
    Streamer(SchwabMarketDataFamily),
}

/// Exact response fact fed back into the shared adaptive rate authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchwabMarketDoctorRateObservation {
    scope: SchwabMarketDoctorProbeScope,
    status: SchwabMarketDoctorProbeStatus,
    retry_after: Option<Box<[u8]>>,
    observed_at: Timestamp,
}

impl SchwabMarketDoctorRateObservation {
    pub(crate) fn try_new(
        scope: SchwabMarketDoctorProbeScope,
        status: SchwabMarketDoctorProbeStatus,
        retry_after: Option<Vec<u8>>,
        observed_at: Timestamp,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        if retry_after.as_ref().is_some_and(|value| {
            value.is_empty()
                || value.len() > MAX_RETRY_AFTER_BYTES
                || value.contains(&b'\r')
                || value.contains(&b'\n')
        }) {
            return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
        }
        Ok(Self {
            scope,
            status,
            retry_after: retry_after.map(Vec::into_boxed_slice),
            observed_at,
        })
    }

    pub(crate) const fn scope(&self) -> SchwabMarketDoctorProbeScope {
        self.scope
    }

    pub(crate) const fn status(&self) -> SchwabMarketDoctorProbeStatus {
        self.status
    }

    pub(crate) fn retry_after(&self) -> Option<&[u8]> {
        self.retry_after.as_deref()
    }

    pub(crate) const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// Closed provider response status across REST and Streamer probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "transport", content = "status", rename_all = "snake_case")]
pub(crate) enum SchwabMarketDoctorProbeStatus {
    Http(u16),
    Streamer(i64),
}

impl SchwabMarketDoctorProbeStatus {
    const fn accepted(self) -> bool {
        match self {
            Self::Http(status) => status >= 200 && status <= 299,
            Self::Streamer(code) => code == 0,
        }
    }
}

/// One checked shared-rate permit. Implementations wrap the existing durable provider budget.
pub(crate) trait SchwabMarketDoctorRatePermit: fmt::Debug + Send {
    fn rate_policy_digest(&self) -> EvidenceDigest;

    fn commit_dispatch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, ()>;

    fn observe<'a>(
        &'a mut self,
        observation: &'a SchwabMarketDoctorRateObservation,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, ()>;
}

/// Sole shared adaptive rate authority used before every provider attempt.
pub(crate) trait SchwabMarketDoctorRateAuthority: fmt::Debug + Send + Sync {
    fn acquire<'a>(
        &'a self,
        scope: SchwabMarketDoctorProbeScope,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, Box<dyn SchwabMarketDoctorRatePermit>>;
}

/// Application-owned consuming physical sealer.
pub(crate) trait SchwabMarketDoctorCaptureSealer: fmt::Debug + Send + Sync {
    fn seal<'a>(
        &'a self,
        request: ProviderCaptureSealRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SealedProviderCaptureMaterial>;
}

impl SchwabMarketDoctorCaptureSealer for ResearchService {
    fn seal<'a>(
        &'a self,
        request: ProviderCaptureSealRequest,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SealedProviderCaptureMaterial> {
        Box::pin(async move {
            self.seal_provider_capture(request, &cancellation, deadline)
                .await
                .map_err(map_research_seal_error)
        })
    }
}

/// A successful User Preference bootstrap or an exact setup-required response.
pub(crate) enum SchwabMarketDoctorUserPreferenceOutcome {
    Available(SchwabMarketDoctorUserPreferenceAvailable),
    SetupRequired(SchwabMarketDoctorSetupRequiredEvidence),
}

/// Minimum User Preference evidence. The provider body remains discarded by the adapter.
pub(crate) struct SchwabMarketDoctorUserPreferenceAvailable {
    token_generation: u64,
    evidence: SchwabUserPreferenceDoctorEvidence,
    rate_observation: SchwabMarketDoctorRateObservation,
}

impl SchwabMarketDoctorUserPreferenceAvailable {
    pub(crate) fn try_new(
        token_generation: u64,
        evidence: SchwabUserPreferenceDoctorEvidence,
        rate_observation: SchwabMarketDoctorRateObservation,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        validate_user_preference(&evidence)?;
        if token_generation == 0
            || rate_observation.scope() != SchwabMarketDoctorProbeScope::UserPreference
            || rate_observation.status()
                != SchwabMarketDoctorProbeStatus::Http(evidence.status_code)
            || rate_observation.observed_at() != evidence.received_at
        {
            return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
        }
        Ok(Self {
            token_generation,
            evidence,
            rate_observation,
        })
    }
}

/// Exact failed bootstrap evidence; it grants no runtime authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchwabMarketDoctorSetupRequiredEvidence {
    token_generation: u64,
    request_sha256: EvidenceDigest,
    response_sha256: EvidenceDigest,
    status_code: u16,
    response_bytes: u64,
    observed_at: Timestamp,
    reason_sha256: EvidenceDigest,
    rate_observation: SchwabMarketDoctorRateObservation,
}

impl SchwabMarketDoctorSetupRequiredEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "the exact failed bootstrap receipt remains explicit"
    )]
    pub(crate) fn try_new(
        token_generation: u64,
        request_sha256: EvidenceDigest,
        response_sha256: EvidenceDigest,
        status_code: u16,
        response_bytes: u64,
        observed_at: Timestamp,
        reason_sha256: EvidenceDigest,
        rate_observation: SchwabMarketDoctorRateObservation,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        for digest in [request_sha256, response_sha256, reason_sha256] {
            require_digest(digest)?;
        }
        if token_generation == 0
            || !(100..=599).contains(&status_code)
            || rate_observation.scope() != SchwabMarketDoctorProbeScope::UserPreference
            || rate_observation.status() != SchwabMarketDoctorProbeStatus::Http(status_code)
            || rate_observation.observed_at() != observed_at
        {
            return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
        }
        Ok(Self {
            token_generation,
            request_sha256,
            response_sha256,
            status_code,
            response_bytes,
            observed_at,
            reason_sha256,
            rate_observation,
        })
    }

    fn seal_identity(&self) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
        digest_serialized(SETUP_REQUIRED_DIGEST_DOMAIN, self)
    }
}

/// Primitive exact evidence returned only after the provider probe crossed physical sealing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchwabMarketDoctorFamilyProbeEvidence {
    family: SchwabMarketDataFamily,
    disposition: RuntimeCapabilityDisposition,
    token_generation: u64,
    status: SchwabMarketDoctorProbeStatus,
    request_sha256: EvidenceDigest,
    response_sha256: EvidenceDigest,
    raw_payload_sha256: EvidenceDigest,
    sealed_capture_receipt_sha256: EvidenceDigest,
    service_observation_sha256: EvidenceDigest,
    requested_items: u64,
    returned_items: u64,
    missing_items: u64,
    unexpected_items: u64,
    provider_records: u64,
    response_bytes: u64,
    latency_nanos: u64,
    observed_at: Timestamp,
    service: Option<Box<str>>,
    quote_delay: Option<CoverageDelay>,
    declared_limitation_sha256: Option<EvidenceDigest>,
    rate_observation: SchwabMarketDoctorRateObservation,
}

/// Constructor input kept separate so no partially valid family evidence can escape.
pub(crate) struct SchwabMarketDoctorFamilyProbeInput {
    pub family: SchwabMarketDataFamily,
    pub disposition: RuntimeCapabilityDisposition,
    pub token_generation: u64,
    pub status: SchwabMarketDoctorProbeStatus,
    pub request_sha256: EvidenceDigest,
    pub response_sha256: EvidenceDigest,
    pub raw_payload_sha256: EvidenceDigest,
    pub sealed_capture_receipt_sha256: EvidenceDigest,
    pub service_observation_sha256: EvidenceDigest,
    pub requested_items: u64,
    pub returned_items: u64,
    pub missing_items: u64,
    pub unexpected_items: u64,
    pub provider_records: u64,
    pub response_bytes: u64,
    pub latency_nanos: u64,
    pub observed_at: Timestamp,
    pub service: Option<Box<str>>,
    pub quote_delay: Option<CoverageDelay>,
    pub declared_limitation_sha256: Option<EvidenceDigest>,
    pub rate_observation: SchwabMarketDoctorRateObservation,
}

impl SchwabMarketDoctorFamilyProbeEvidence {
    pub(crate) fn try_new(
        input: SchwabMarketDoctorFamilyProbeInput,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        for digest in [
            input.request_sha256,
            input.response_sha256,
            input.raw_payload_sha256,
            input.sealed_capture_receipt_sha256,
            input.service_observation_sha256,
        ] {
            require_digest(digest)?;
        }
        if let Some(digest) = input.declared_limitation_sha256 {
            require_digest(digest)?;
        }
        let expected_service = streamer_service(input.family).map(MarketDataService::as_str);
        if input.token_generation == 0
            || input.requested_items == 0
            || input
                .returned_items
                .checked_add(input.missing_items)
                .is_none_or(|total| total != input.requested_items)
            || input.rate_observation.scope() != probe_scope(input.family)?
            || input.rate_observation.status() != input.status
            || input.rate_observation.observed_at() != input.observed_at
            || input.service.as_deref() != expected_service
            || matches!(input.quote_delay, Some(CoverageDelay::Delayed(0)))
            || input.quote_delay.is_some()
                && (input.family != SchwabMarketDataFamily::Quotes
                    || !input.status.accepted()
                    || input.returned_items == 0
                    || input.provider_records == 0)
            || matches!(input.disposition, RuntimeCapabilityDisposition::NotProbed)
        {
            return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
        }
        match input.disposition {
            RuntimeCapabilityDisposition::Available
                if input.status.accepted()
                    && input.returned_items > 0
                    && input.provider_records > 0
                    && input.response_bytes > 0
                    && input.missing_items == 0
                    && input.unexpected_items == 0 => {}
            RuntimeCapabilityDisposition::Degraded
                if input.status.accepted()
                    && input.returned_items > 0
                    && input.provider_records > 0
                    && input.response_bytes > 0
                    && (input.missing_items > 0
                        || input.unexpected_items > 0
                        || input.declared_limitation_sha256.is_some()) => {}
            RuntimeCapabilityDisposition::Unavailable
                if !input.status.accepted()
                    || input.returned_items == 0
                    || input.provider_records == 0 => {}
            _ => return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence),
        }
        Ok(Self {
            family: input.family,
            disposition: input.disposition,
            token_generation: input.token_generation,
            status: input.status,
            request_sha256: input.request_sha256,
            response_sha256: input.response_sha256,
            raw_payload_sha256: input.raw_payload_sha256,
            sealed_capture_receipt_sha256: input.sealed_capture_receipt_sha256,
            service_observation_sha256: input.service_observation_sha256,
            requested_items: input.requested_items,
            returned_items: input.returned_items,
            missing_items: input.missing_items,
            unexpected_items: input.unexpected_items,
            provider_records: input.provider_records,
            response_bytes: input.response_bytes,
            latency_nanos: input.latency_nanos,
            observed_at: input.observed_at,
            service: input.service,
            quote_delay: input.quote_delay,
            declared_limitation_sha256: input.declared_limitation_sha256,
            rate_observation: input.rate_observation,
        })
    }

    pub(crate) const fn family(&self) -> SchwabMarketDataFamily {
        self.family
    }

    pub(crate) const fn quote_delay(&self) -> Option<CoverageDelay> {
        self.quote_delay
    }

    fn observation_sha256(&self) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
        digest_serialized(FAMILY_OBSERVATION_DIGEST_DOMAIN, self)
    }
}

/// Bounded provider-native probes. The production implementation owns exact requests,
/// subscriptions, and adapter executors but receives neither rate nor physical-seal authority.
pub(crate) trait SchwabMarketDoctorProbeExecutor: fmt::Debug + Send + Sync {
    fn probe_contract_digest(&self) -> EvidenceDigest;

    fn user_preference<'a>(
        &'a self,
        authority: &'a SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorUserPreferenceOutcome>;

    fn rest<'a>(
        &'a self,
        family: SchwabMarketDataFamily,
        authority: &'a SchwabOAuthMarketAuthority,
        sealer: &'a dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorFamilyProbeEvidence>;

    fn streamer<'a>(
        &'a self,
        family: SchwabMarketDataFamily,
        authority: &'a SchwabOAuthMarketAuthority,
        sealer: &'a dyn SchwabMarketDoctorCaptureSealer,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> DoctorFuture<'a, SchwabMarketDoctorFamilyProbeEvidence>;
}

/// Successful doctor result retaining the complete typed receipt and exact detailed probes.
pub(crate) struct SchwabMarketDataDoctorRun {
    receipt: SchwabMarketDataDoctorReceiptV1,
    probe_contract_digest: EvidenceDigest,
    user_preference_rate_observation: SchwabMarketDoctorRateObservation,
    families: Box<[SchwabMarketDoctorFamilyProbeEvidence]>,
}

impl SchwabMarketDataDoctorRun {
    pub(crate) const fn receipt(&self) -> &SchwabMarketDataDoctorReceiptV1 {
        &self.receipt
    }

    pub(crate) const fn observation(&self) -> &SchwabMarketDataDoctorObservation {
        self.receipt.observation()
    }

    pub(crate) fn families(&self) -> &[SchwabMarketDoctorFamilyProbeEvidence] {
        &self.families
    }

    pub(crate) const fn user_preference_rate_observation(
        &self,
    ) -> &SchwabMarketDoctorRateObservation {
        &self.user_preference_rate_observation
    }
}

impl fmt::Debug for SchwabMarketDataDoctorRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabMarketDataDoctorRun")
            .field("receipt", &self.receipt.receipt_sha256())
            .field("probe_contract", &self.probe_contract_digest)
            .field(
                "user_preference_rate_observation",
                &self.user_preference_rate_observation,
            )
            .field("family_count", &self.families.len())
            .finish()
    }
}

/// Doctor terminal: either a complete receipt or exact setup-required evidence.
#[derive(Debug)]
pub(crate) enum SchwabMarketDataDoctorOutcome {
    Observed(SchwabMarketDataDoctorRun),
    SetupRequired(SchwabMarketDoctorSetupRequiredEvidence),
}

/// Sole application doctor orchestrator.
pub(crate) struct SchwabMarketDataDoctorExecutor {
    rate: Arc<dyn SchwabMarketDoctorRateAuthority>,
    sealer: Arc<dyn SchwabMarketDoctorCaptureSealer>,
    probes: Arc<dyn SchwabMarketDoctorProbeExecutor>,
}

impl SchwabMarketDataDoctorExecutor {
    pub(crate) fn try_new(
        rate: Arc<dyn SchwabMarketDoctorRateAuthority>,
        sealer: Arc<dyn SchwabMarketDoctorCaptureSealer>,
        probes: Arc<dyn SchwabMarketDoctorProbeExecutor>,
    ) -> Result<Self, SchwabMarketDataDoctorError> {
        require_digest(probes.probe_contract_digest())?;
        Ok(Self {
            rate,
            sealer,
            probes,
        })
    }

    pub(crate) async fn run(
        &self,
        binding: SchwabMarketDoctorAuthorityBinding,
        authority: SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDataDoctorOutcome, SchwabMarketDataDoctorError> {
        ensure_active(&cancellation, deadline)?;
        if binding.session_id() != authority.session_id() {
            return Err(SchwabMarketDataDoctorError::InvalidAuthority);
        }
        let oauth = await_bounded(authority.current_receipt(), &cancellation, deadline)
            .await?
            .map_err(|_| SchwabMarketDataDoctorError::AuthorityUnavailable)?;

        let mut preference_permit = self
            .acquire_rate(
                SchwabMarketDoctorProbeScope::UserPreference,
                &binding,
                &cancellation,
                deadline,
            )
            .await?;
        await_bounded(
            preference_permit.commit_dispatch(&cancellation, deadline),
            &cancellation,
            deadline,
        )
        .await??;
        let preference = await_bounded(
            self.probes
                .user_preference(&authority, cancellation.child_token(), deadline),
            &cancellation,
            deadline,
        )
        .await??;
        let (user_preference, user_preference_rate_observation) = match preference {
            SchwabMarketDoctorUserPreferenceOutcome::Available(available) => {
                if available.token_generation != oauth.generation().get() {
                    return Err(SchwabMarketDataDoctorError::AuthorityChanged);
                }
                await_bounded(
                    preference_permit.observe(&available.rate_observation, &cancellation, deadline),
                    &cancellation,
                    deadline,
                )
                .await??;
                (available.evidence, available.rate_observation)
            }
            SchwabMarketDoctorUserPreferenceOutcome::SetupRequired(evidence) => {
                if evidence.token_generation != oauth.generation().get() {
                    return Err(SchwabMarketDataDoctorError::AuthorityChanged);
                }
                await_bounded(
                    preference_permit.observe(&evidence.rate_observation, &cancellation, deadline),
                    &cancellation,
                    deadline,
                )
                .await??;
                let _ = evidence.seal_identity()?;
                return Ok(SchwabMarketDataDoctorOutcome::SetupRequired(evidence));
            }
        };

        let probe_contract_digest = self.probes.probe_contract_digest();
        let mut detailed = Vec::new();
        let mut families = Vec::new();
        detailed
            .try_reserve_exact(REST_FAMILIES.len() + STREAMER_FAMILIES.len())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?;
        families
            .try_reserve_exact(REST_FAMILIES.len() + STREAMER_FAMILIES.len())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?;

        for family in REST_FAMILIES {
            let evidence = self
                .run_family(
                    family,
                    false,
                    &binding,
                    &authority,
                    oauth,
                    &cancellation,
                    deadline,
                )
                .await?;
            families.push(family_receipt_evidence(
                &binding,
                probe_contract_digest,
                &evidence,
            )?);
            detailed.push(evidence);
        }
        for family in STREAMER_FAMILIES {
            let evidence = self
                .run_family(
                    family,
                    true,
                    &binding,
                    &authority,
                    oauth,
                    &cancellation,
                    deadline,
                )
                .await?;
            families.push(family_receipt_evidence(
                &binding,
                probe_contract_digest,
                &evidence,
            )?);
            detailed.push(evidence);
        }

        let current = await_bounded(authority.current_receipt(), &cancellation, deadline)
            .await?
            .map_err(|_| SchwabMarketDataDoctorError::AuthorityUnavailable)?;
        if current != oauth {
            return Err(SchwabMarketDataDoctorError::AuthorityChanged);
        }
        let completed_at = system_timestamp()?;
        let access_issued_at = timestamp_from_seconds(oauth.access_issued_at_unix_seconds())?;
        let access_expires_at = timestamp_from_seconds(oauth.access_expires_at_unix_seconds())?;
        let refresh_authorized_at =
            timestamp_from_seconds(oauth.refresh_authorized_at_unix_seconds())?;
        let refresh_expires_at = timestamp_from_seconds(oauth.refresh_expires_at_unix_seconds())?;
        let observation = SchwabMarketDataDoctorObservation {
            provider_observation_origin:
                SchwabMarketDataDoctorObservation::provider_observed_origin()
                    .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeEvidence)?,
            access_token_generation: oauth.generation().get(),
            access_issued_at,
            access_expires_at,
            refresh_authorized_at,
            refresh_expires_at,
            user_preference,
            quote_delay: detailed
                .iter()
                .find(|evidence| evidence.family() == SchwabMarketDataFamily::Quotes)
                .and_then(SchwabMarketDoctorFamilyProbeEvidence::quote_delay),
            families: families.into_boxed_slice(),
            completed_at,
        };
        let maximum_expiry = completed_at
            .unix_nanos()
            .checked_add(SchwabMarketDataDoctorReceiptV1::VALIDITY_NANOS)
            .ok_or(SchwabMarketDataDoctorError::Clock)?
            .min(access_expires_at.unix_nanos())
            .min(refresh_expires_at.unix_nanos());
        let receipt =
            SchwabMarketDataDoctorReceiptV1::try_new(SchwabMarketDataDoctorReceiptInput {
                surface_id: binding.surface_id,
                session_identifier: SourceIdentifier::try_from(binding.session_id.to_string())
                    .map_err(|_| SchwabMarketDataDoctorError::InvalidAuthority)?,
                application_credential_generation: binding.application_credential_generation,
                capability_revision: binding.capability_revision,
                capability_digest: binding.capability_digest,
                public_configuration_digest: binding.public_configuration_digest,
                rights_decision_digest: binding.rights_decision_digest,
                rate_policy_digest: binding.rate_policy_digest,
                data_quality: DataQuality::DirectUnverified,
                observation,
                exclusive_expires_at: Timestamp::from_unix_nanos(maximum_expiry),
                predecessor_digest: binding.predecessor_digest,
            })
            .map_err(|_| SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
        Ok(SchwabMarketDataDoctorOutcome::Observed(
            SchwabMarketDataDoctorRun {
                receipt,
                probe_contract_digest,
                user_preference_rate_observation,
                families: detailed.into_boxed_slice(),
            },
        ))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "authority, family, deadline, and cancellation stay explicit"
    )]
    async fn run_family(
        &self,
        family: SchwabMarketDataFamily,
        streamer: bool,
        binding: &SchwabMarketDoctorAuthorityBinding,
        authority: &SchwabOAuthMarketAuthority,
        oauth: SchwabOAuthAuthorityReceipt,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketDoctorFamilyProbeEvidence, SchwabMarketDataDoctorError> {
        let scope = probe_scope(family)?;
        if streamer != matches!(scope, SchwabMarketDoctorProbeScope::Streamer(_)) {
            return Err(SchwabMarketDataDoctorError::InvalidProbeContract);
        }
        let mut permit = self
            .acquire_rate(scope, binding, cancellation, deadline)
            .await?;
        await_bounded(
            permit.commit_dispatch(cancellation, deadline),
            cancellation,
            deadline,
        )
        .await??;
        let operation = if streamer {
            self.probes.streamer(
                family,
                authority,
                self.sealer.as_ref(),
                cancellation.child_token(),
                deadline,
            )
        } else {
            self.probes.rest(
                family,
                authority,
                self.sealer.as_ref(),
                cancellation.child_token(),
                deadline,
            )
        };
        let evidence = await_bounded(operation, cancellation, deadline).await??;
        if evidence.family() != family || evidence.token_generation != oauth.generation().get() {
            return Err(SchwabMarketDataDoctorError::AuthorityChanged);
        }
        await_bounded(
            permit.observe(&evidence.rate_observation, cancellation, deadline),
            cancellation,
            deadline,
        )
        .await??;
        Ok(evidence)
    }

    async fn acquire_rate(
        &self,
        scope: SchwabMarketDoctorProbeScope,
        binding: &SchwabMarketDoctorAuthorityBinding,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<Box<dyn SchwabMarketDoctorRatePermit>, SchwabMarketDataDoctorError> {
        let permit = await_bounded(
            self.rate
                .acquire(scope, cancellation.child_token(), deadline),
            cancellation,
            deadline,
        )
        .await??;
        if permit.rate_policy_digest() != binding.rate_policy_digest() {
            return Err(SchwabMarketDataDoctorError::InvalidRateAuthority);
        }
        Ok(permit)
    }
}

impl fmt::Debug for SchwabMarketDataDoctorExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabMarketDataDoctorExecutor")
            .field("rate", &self.rate)
            .field("sealer", &self.sealer)
            .field("probes", &self.probes)
            .finish()
    }
}

fn family_receipt_evidence(
    binding: &SchwabMarketDoctorAuthorityBinding,
    probe_contract_digest: EvidenceDigest,
    evidence: &SchwabMarketDoctorFamilyProbeEvidence,
) -> Result<SchwabMarketDataFamilyEvidence, SchwabMarketDataDoctorError> {
    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct DispositionMaterial<'a> {
        surface_id: &'a SourceIdentifier,
        session_id: Uuid,
        application_credential_generation: u64,
        capability_revision: u64,
        capability_digest: EvidenceDigest,
        public_configuration_digest: EvidenceDigest,
        rights_decision_digest: EvidenceDigest,
        rate_policy_digest: EvidenceDigest,
        probe_contract_digest: EvidenceDigest,
        observation_sha256: EvidenceDigest,
        family: SchwabMarketDataFamily,
        disposition: RuntimeCapabilityDisposition,
    }
    let observation_sha256 = evidence.observation_sha256()?;
    let material = DispositionMaterial {
        surface_id: &binding.surface_id,
        session_id: binding.session_id,
        application_credential_generation: binding.application_credential_generation.get(),
        capability_revision: binding.capability_revision.get(),
        capability_digest: binding.capability_digest,
        public_configuration_digest: binding.public_configuration_digest,
        rights_decision_digest: binding.rights_decision_digest,
        rate_policy_digest: binding.rate_policy_digest,
        probe_contract_digest,
        observation_sha256,
        family: evidence.family,
        disposition: evidence.disposition,
    };
    Ok(SchwabMarketDataFamilyEvidence {
        family: evidence.family,
        disposition: evidence.disposition,
        disposition_evidence_sha256: digest_serialized(
            FAMILY_DISPOSITION_DIGEST_DOMAIN,
            &material,
        )?,
        observation_sha256: Some(observation_sha256),
        observed_at: Some(evidence.observed_at),
    })
}

fn validate_user_preference(
    evidence: &SchwabUserPreferenceDoctorEvidence,
) -> Result<(), SchwabMarketDataDoctorError> {
    for digest in [
        evidence.endpoint_contract_sha256,
        evidence.request_sha256,
        evidence.response_sha256,
        evidence.market_data_principal_sha256,
        evidence.streamer_bootstrap_sha256,
    ] {
        require_digest(digest)?;
    }
    if let Some(digest) = evidence.market_data_offer_sha256 {
        require_digest(digest)?;
    }
    if evidence.endpoint_contract_sha256 != sha256(USER_PREFERENCE_ENDPOINT_CONTRACT)
        || !(200..=299).contains(&evidence.status_code)
        || evidence.response_bytes == 0
    {
        return Err(SchwabMarketDataDoctorError::InvalidProbeEvidence);
    }
    Ok(())
}

/// Exact code-owned contract digest expected on the minimum read-only bootstrap probe.
pub(crate) fn user_preference_endpoint_contract_sha256() -> EvidenceDigest {
    sha256(USER_PREFERENCE_ENDPOINT_CONTRACT)
}

fn probe_scope(
    family: SchwabMarketDataFamily,
) -> Result<SchwabMarketDoctorProbeScope, SchwabMarketDataDoctorError> {
    if REST_FAMILIES.contains(&family) {
        Ok(SchwabMarketDoctorProbeScope::Rest(family))
    } else if STREAMER_FAMILIES.contains(&family) {
        Ok(SchwabMarketDoctorProbeScope::Streamer(family))
    } else {
        Err(SchwabMarketDataDoctorError::InvalidProbeContract)
    }
}

/// Maps the receipt family onto the current adapter success-evidence family.
pub(crate) const fn adapter_rest_family(
    family: SchwabMarketDataFamily,
) -> Option<SchwabObservedCapabilityFamily> {
    match family {
        SchwabMarketDataFamily::Quotes => Some(SchwabObservedCapabilityFamily::Quotes),
        SchwabMarketDataFamily::PriceHistory => {
            Some(SchwabObservedCapabilityFamily::DailyPriceHistory)
        }
        SchwabMarketDataFamily::OptionChains => Some(SchwabObservedCapabilityFamily::OptionChain),
        SchwabMarketDataFamily::ExpirationChains => {
            Some(SchwabObservedCapabilityFamily::ExpirationChain)
        }
        SchwabMarketDataFamily::Movers => Some(SchwabObservedCapabilityFamily::Movers),
        SchwabMarketDataFamily::MarketHours => Some(SchwabObservedCapabilityFamily::MarketHours),
        SchwabMarketDataFamily::Instruments => Some(SchwabObservedCapabilityFamily::Instruments),
        _ => None,
    }
}

/// Maps the receipt family onto the exact selected Streamer service.
pub(crate) const fn streamer_service(family: SchwabMarketDataFamily) -> Option<MarketDataService> {
    match family {
        SchwabMarketDataFamily::LevelOneEquities => Some(MarketDataService::LevelOneEquities),
        SchwabMarketDataFamily::LevelOneOptions => Some(MarketDataService::LevelOneOptions),
        SchwabMarketDataFamily::LevelOneFutures => Some(MarketDataService::LevelOneFutures),
        SchwabMarketDataFamily::LevelOneFuturesOptions => {
            Some(MarketDataService::LevelOneFuturesOptions)
        }
        SchwabMarketDataFamily::LevelOneForex => Some(MarketDataService::LevelOneForex),
        SchwabMarketDataFamily::NyseBook => Some(MarketDataService::NyseBook),
        SchwabMarketDataFamily::NasdaqBook => Some(MarketDataService::NasdaqBook),
        SchwabMarketDataFamily::OptionsBook => Some(MarketDataService::OptionsBook),
        SchwabMarketDataFamily::ChartEquity => Some(MarketDataService::ChartEquity),
        SchwabMarketDataFamily::ChartFutures => Some(MarketDataService::ChartFutures),
        SchwabMarketDataFamily::ScreenerEquity => Some(MarketDataService::ScreenerEquity),
        SchwabMarketDataFamily::ScreenerOption => Some(MarketDataService::ScreenerOption),
        _ => None,
    }
}

async fn await_bounded<T, F>(
    operation: F,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, SchwabMarketDataDoctorError>
where
    F: Future<Output = T>,
{
    ensure_active(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabMarketDataDoctorError::Cancelled),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(SchwabMarketDataDoctorError::Deadline)
        }
        result = operation => Ok(result),
    }
}

fn ensure_active(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SchwabMarketDataDoctorError> {
    if cancellation.is_cancelled() {
        Err(SchwabMarketDataDoctorError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SchwabMarketDataDoctorError::Deadline)
    } else {
        Ok(())
    }
}

fn system_timestamp() -> Result<Timestamp, SchwabMarketDataDoctorError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SchwabMarketDataDoctorError::Clock)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| SchwabMarketDataDoctorError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn timestamp_from_seconds(seconds: u64) -> Result<Timestamp, SchwabMarketDataDoctorError> {
    let nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabMarketDataDoctorError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn digest_serialized(
    domain: &[u8],
    value: &impl Serialize,
) -> Result<EvidenceDigest, SchwabMarketDataDoctorError> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| SchwabMarketDataDoctorError::InvalidProbeEvidence)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(
        u64::try_from(bytes.len())
            .map_err(|_| SchwabMarketDataDoctorError::ResourceLimit)?
            .to_be_bytes(),
    );
    hasher.update(bytes);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn require_digest(digest: EvidenceDigest) -> Result<(), SchwabMarketDataDoctorError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(SchwabMarketDataDoctorError::InvalidProbeEvidence)
    } else {
        Ok(())
    }
}

fn map_research_seal_error(error: ResearchServiceError) -> SchwabMarketDataDoctorError {
    match error {
        ResearchServiceError::Ingest(IngestError::Cancelled) => {
            SchwabMarketDataDoctorError::Cancelled
        }
        ResearchServiceError::Ingest(IngestError::DeadlineExceeded) => {
            SchwabMarketDataDoctorError::Deadline
        }
        ResearchServiceError::ProviderCaptureSealWorkerUnavailable
        | ResearchServiceError::ProviderCaptureStore(_)
        | ResearchServiceError::Ingest(_) => SchwabMarketDataDoctorError::InvalidProbeEvidence,
        ResearchServiceError::Path(_)
        | ResearchServiceError::Catalog(_)
        | ResearchServiceError::Manifest(_)
        | ResearchServiceError::ProviderOnboarding(_)
        | ResearchServiceError::Dataset(_)
        | ResearchServiceError::IngestAuthorityMismatch
        | ResearchServiceError::Rights(_)
        | ResearchServiceError::IdentityOverflow => {
            SchwabMarketDataDoctorError::InvalidProbeEvidence
        }
    }
}

/// Closed secret-free doctor failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SchwabMarketDataDoctorError {
    #[error("the Schwab doctor authority binding is invalid")]
    InvalidAuthority,
    #[error("the active Schwab OAuth market authority is unavailable")]
    AuthorityUnavailable,
    #[error("the Schwab OAuth authority changed during the doctor run")]
    AuthorityChanged,
    #[error("the Schwab doctor probe contract is invalid")]
    InvalidProbeContract,
    #[error("the Schwab doctor provider evidence is incomplete or inconsistent")]
    InvalidProbeEvidence,
    #[error("the Schwab doctor rate authority does not match the onboarding policy")]
    InvalidRateAuthority,
    #[error("the Schwab doctor operation was cancelled")]
    Cancelled,
    #[error("the Schwab doctor deadline elapsed")]
    Deadline,
    #[error("the Schwab doctor clock is unavailable")]
    Clock,
    #[error("the Schwab doctor evidence exceeded its resource bound")]
    ResourceLimit,
}
