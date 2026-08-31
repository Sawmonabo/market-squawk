//! Provider-local activation and exact shared-rate contracts.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, NetworkAccessPolicy, ProviderBudgetPolicy,
    ProviderBudgetWindow, ProviderCaptureTerminalDisposition, ProviderRateDeclaration,
    SealedProviderCaptureSetReceipt, SourceMetadata,
};
use sha2::{Digest as _, Sha256};

use crate::chunks::limits_for;
use crate::{
    BlsAccessTier, BlsCredentialRejoin, BlsDoctorReadiness, BlsDoctorReport, BlsRequestLimits,
    BlsSourceError,
};

const SECOND_NANOS: u64 = 1_000_000_000;
const TEN_SECONDS_NANOS: u64 = 10 * SECOND_NANOS;
const DAY_NANOS: u64 = 86_400 * SECOND_NANOS;
const DOCUMENTED_REQUESTS_PER_TEN_SECONDS: u16 = 50;
const MAXIMUM_BACKOFF_NANOS: u64 = 60 * SECOND_NANOS;
/// Maximum time one physically sealed successful doctor may admit BLS production work.
pub const BLS_DOCTOR_ACTIVATION_TTL_NANOS: i64 = 86_400_000_000_000;

/// Private in-process capability tying the only credential owner to doctor and activation.
pub(crate) struct BlsRuntimeInstanceCapability {
    _private: (),
}

impl BlsRuntimeInstanceCapability {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { _private: () })
    }
}

impl std::fmt::Debug for BlsRuntimeInstanceCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlsRuntimeInstanceCapability([PRIVATE])")
    }
}

/// Builds the one code-owned BLS budget profile used by source metadata and durable admission.
///
/// Public v1 collides on the normalized official endpoint authority. Registered v2 collides on a
/// stable governed-provider subject because BLS supplies no verified immutable account identity.
/// Both tiers enforce one request/second, concurrency one, the tier-specific daily ceiling, and
/// deterministic one-to-sixty-second refusal backoff.
pub fn bls_application_provider_budget(
    tier: BlsAccessTier,
) -> Result<ProviderBudgetPolicy, BlsSourceError> {
    let limits = limits_for(tier);
    let provider =
        SourceIdentifier::try_from("us-bls").map_err(|_| BlsSourceError::InvalidConfiguration)?;
    let scope = match tier {
        BlsAccessTier::PublicV1 => BudgetScope::new(provider),
        BlsAccessTier::RegisteredV2 => {
            let subject = ProviderRateDeclaration::governed_provider_subject(&provider)
                .map_err(|_| BlsSourceError::InvalidConfiguration)?;
            BudgetScope::with_authorization_account(provider, subject)
        }
    };
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(u32::from(limits.enforced_requests_per_second()))
                .ok_or(BlsSourceError::InvalidConfiguration)?,
            NonZeroU64::new(SECOND_NANOS).ok_or(BlsSourceError::InvalidConfiguration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| BlsSourceError::InvalidConfiguration)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(u32::from(limits.daily_queries()))
                .ok_or(BlsSourceError::InvalidConfiguration)?,
            NonZeroU64::new(DAY_NANOS).ok_or(BlsSourceError::InvalidConfiguration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| BlsSourceError::InvalidConfiguration)?,
    ];
    ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &windows,
        NonZeroU16::new(1).ok_or(BlsSourceError::InvalidConfiguration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(SECOND_NANOS).ok_or(BlsSourceError::InvalidConfiguration)?,
            NonZeroU64::new(MAXIMUM_BACKOFF_NANOS).ok_or(BlsSourceError::InvalidConfiguration)?,
            0,
        )
        .map_err(|_| BlsSourceError::InvalidConfiguration)?,
    )
    .map_err(|_| BlsSourceError::InvalidConfiguration)
}

/// Builds the exact product-wide declaration for already constructed BLS source metadata.
pub fn bls_provider_rate_declaration(
    metadata: &SourceMetadata,
    tier: BlsAccessTier,
) -> Result<BlsProviderRateDeclaration, BlsSourceError> {
    BlsProviderRateDeclaration::try_from_metadata(metadata, tier, limits_for(tier))
}

/// Exact BLS provider facts and Market Squawk limits registered with the sole durable authority.
///
/// `shared_rate_declaration` is the canonical product-wide declaration. This wrapper retains the
/// provider/application distinction and the invariants that every started request—including a
/// doctor or failed response—uses the same crash-safe allocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsProviderRateDeclaration {
    tier: BlsAccessTier,
    limits: BlsRequestLimits,
    authorization_subject: Option<SourceIdentifier>,
    shared_rate_declaration: ProviderRateDeclaration,
}

impl BlsProviderRateDeclaration {
    pub(crate) fn try_from_metadata(
        metadata: &SourceMetadata,
        tier: BlsAccessTier,
        limits: BlsRequestLimits,
    ) -> Result<Self, BlsSourceError> {
        let policy = metadata
            .budget_policy()
            .ok_or(BlsSourceError::InvalidMetadata)?;
        let expected_policy =
            bls_application_provider_budget(tier).map_err(|_| BlsSourceError::InvalidMetadata)?;
        let provider =
            SourceIdentifier::try_from("us-bls").map_err(|_| BlsSourceError::InvalidMetadata)?;
        let short = policy.window(0).ok_or(BlsSourceError::InvalidMetadata)?;
        let daily = policy.window(1).ok_or(BlsSourceError::InvalidMetadata)?;
        if policy != &expected_policy
            || policy.scope().as_source_identifier() != &provider
            || policy.window_count() != 2
            || short.requests_per_window() != u32::from(limits.enforced_requests_per_second())
            || short.window_nanos() != SECOND_NANOS
            || short.semantics() != BudgetWindowSemantics::Sliding
            || daily.requests_per_window() != u32::from(limits.daily_queries())
            || daily.window_nanos() != DAY_NANOS
            || daily.semantics() != BudgetWindowSemantics::Sliding
            || policy.max_concurrent() != 1
            || policy.backoff().delay_nanos(0, 0) != SECOND_NANOS
            || policy.backoff().delay_nanos(0, 10_000) != SECOND_NANOS
            || policy.backoff().maximum_nanos() != MAXIMUM_BACKOFF_NANOS
        {
            return Err(BlsSourceError::InvalidMetadata);
        }

        let (authorization_subject, shared_rate_declaration) = match tier {
            BlsAccessTier::PublicV1 => {
                if policy.scope().authorization_account().is_some() {
                    return Err(BlsSourceError::InvalidMetadata);
                }
                let NetworkAccessPolicy::Allowlisted(endpoints) = metadata.network_policy() else {
                    return Err(BlsSourceError::InvalidMetadata);
                };
                let declaration =
                    ProviderRateDeclaration::try_for_endpoint(policy.clone(), endpoints)
                        .map_err(|_| BlsSourceError::InvalidMetadata)?;
                (None, declaration)
            }
            BlsAccessTier::RegisteredV2 => {
                if policy.scope().authorization_account().is_none() {
                    return Err(BlsSourceError::InvalidMetadata);
                }
                // BLS supplies no verified immutable account ID. All local registered-v2 keys
                // therefore collide on one stable code-owned provider subject rather than secret
                // bytes, credential generations, or caller labels.
                let subject = ProviderRateDeclaration::governed_provider_subject(&provider)
                    .map_err(|_| BlsSourceError::InvalidMetadata)?;
                let declaration = ProviderRateDeclaration::try_for_authorization_subject(
                    policy.clone(),
                    &subject,
                )
                .map_err(|_| BlsSourceError::InvalidMetadata)?;
                (Some(subject), declaration)
            }
        };
        shared_rate_declaration
            .validate()
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        Ok(Self {
            tier,
            limits,
            authorization_subject,
            shared_rate_declaration,
        })
    }

    /// Returns the exact public-v1 or registered-v2 allocation.
    pub const fn tier(&self) -> BlsAccessTier {
        self.tier
    }

    /// Returns the provider-published burst ceiling.
    pub const fn documented_requests_per_ten_seconds(&self) -> u16 {
        DOCUMENTED_REQUESTS_PER_TEN_SECONDS
    }

    /// Returns the provider-published burst-window width.
    pub const fn documented_burst_window_nanos(&self) -> u64 {
        TEN_SECONDS_NANOS
    }

    /// Returns the provider-published daily request ceiling.
    pub const fn documented_requests_per_day(&self) -> u16 {
        self.limits.documented_daily_queries()
    }

    /// Returns Market Squawk's conservative one-second attempt ceiling.
    pub const fn application_requests_per_second(&self) -> u16 {
        self.limits.enforced_requests_per_second()
    }

    /// Returns Market Squawk's conservative daily attempt ceiling.
    pub const fn application_requests_per_day(&self) -> u16 {
        self.limits.daily_queries()
    }

    /// Returns the only admitted concurrent request count.
    pub const fn maximum_in_flight(&self) -> u16 {
        1
    }

    /// Returns the stable non-secret subject for registered-v2, or `None` for public v1.
    pub const fn authorization_subject(&self) -> Option<&SourceIdentifier> {
        self.authorization_subject.as_ref()
    }

    /// Returns the exact declaration application composition must register and recover.
    pub const fn shared_rate_declaration(&self) -> &ProviderRateDeclaration {
        &self.shared_rate_declaration
    }

    /// Returns the canonical shared declaration identity.
    pub const fn declaration_digest(&self) -> EvidenceDigest {
        self.shared_rate_declaration.declaration_digest()
    }

    /// Confirms that provider refusals begin at one second and cap at sixty seconds.
    pub const fn maximum_backoff_nanos(&self) -> u64 {
        MAXIMUM_BACKOFF_NANOS
    }

    /// Confirms every started request consumes quota, independent of its outcome.
    pub const fn counts_all_started_attempts(&self) -> bool {
        true
    }

    /// Confirms activation requires the shared crash-safe provider-rate authority.
    pub const fn persistent_shared_authority_required(&self) -> bool {
        true
    }

    pub(crate) const fn limits(&self) -> BlsRequestLimits {
        self.limits
    }
}

/// Provider-local requirements passed to shared application composition.
///
/// This plan does not prove that data exists or that an analytical generation has been committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlsActivationPlan {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    provider_dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    credential_rejoin: BlsCredentialRejoin,
    rate: BlsProviderRateDeclaration,
    plan_digest: EvidenceDigest,
}

impl BlsActivationPlan {
    pub(crate) fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        provider_dataset: SourceIdentifier,
        analytical_dataset: SourceIdentifier,
        credential_rejoin: BlsCredentialRejoin,
        rate: BlsProviderRateDeclaration,
    ) -> Result<Self, BlsSourceError> {
        let mut plan = Self {
            source_id,
            metadata_revision,
            provider_dataset,
            analytical_dataset,
            credential_rejoin,
            rate,
            plan_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
        };
        plan.plan_digest = plan.compute_digest()?;
        plan.validate()?;
        Ok(plan)
    }

    /// Returns the exact registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the immutable metadata revision required at activation.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the provider request-plan dataset.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the storage-safe canonical analytical dataset root may reserve.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the explicit no-credential marker or registered credential-generation coordinate.
    pub const fn credential_rejoin(&self) -> BlsCredentialRejoin {
        self.credential_rejoin
    }

    /// Returns the exact durable declaration shared composition must register and recover.
    pub const fn rate(&self) -> &BlsProviderRateDeclaration {
        &self.rate
    }

    /// Returns the domain-separated identity of the complete static activation recipe.
    pub const fn plan_digest(&self) -> EvidenceDigest {
        self.plan_digest
    }

    /// Recomputes the provider-local recipe before any admission decision.
    pub fn validate(&self) -> Result<(), BlsSourceError> {
        self.rate
            .shared_rate_declaration()
            .validate()
            .map_err(|_| BlsSourceError::InvalidMetadata)?;
        let provider_prefix = match self.rate.tier() {
            BlsAccessTier::PublicV1 => "bls:timeseries:public-v1:",
            BlsAccessTier::RegisteredV2 => "bls:timeseries:registered-v2:",
        };
        let expected_analytical =
            crate::BlsSource::analytical_dataset_identifier(&self.provider_dataset)?;
        if self.source_id.as_str().is_empty()
            || self
                .metadata_revision
                .as_source_identifier()
                .as_str()
                .is_empty()
            || !self.provider_dataset.as_str().starts_with(provider_prefix)
            || self.analytical_dataset != expected_analytical
            || !credential_rejoin_matches_tier(self.credential_rejoin, self.rate.tier())
            || self.rate.declaration_digest().bytes() == [0; 32]
            || !self.rate.persistent_shared_authority_required()
            || !self.rate.counts_all_started_attempts()
            || self.plan_digest != self.compute_digest()?
        {
            return Err(BlsSourceError::InvalidMetadata);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EvidenceDigest, BlsSourceError> {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/bls-activation-plan/v3\0");
        hash_contract_field(&mut digest, self.source_id.as_str().as_bytes())?;
        hash_contract_field(
            &mut digest,
            self.metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_contract_field(&mut digest, self.provider_dataset.as_str().as_bytes())?;
        hash_contract_field(&mut digest, self.analytical_dataset.as_str().as_bytes())?;
        hash_contract_digest(&mut digest, self.rate.declaration_digest());
        hash_credential_rejoin(&mut digest, self.credential_rejoin);
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }
}

/// Non-serializable, in-process BLS readiness admission bound to one actual sealed doctor.
///
/// This candidate grants only bounded provider-operation admission. It cannot commit a dataset,
/// assign revisions, advance a checkpoint, survive restart, or mint a query/PIT receipt.
#[derive(Debug)]
pub struct BlsActivationCandidate {
    plan: BlsActivationPlan,
    doctor: BlsDoctorReport,
    sealed_doctor_capture: SealedProviderCaptureSetReceipt,
    activated_at: Timestamp,
    expires_at: Timestamp,
    candidate_digest: EvidenceDigest,
    runtime_instance: Arc<BlsRuntimeInstanceCapability>,
}

impl BlsActivationCandidate {
    /// Admits a current successful doctor only after its exact raw response is physically sealed.
    pub(crate) fn try_new(
        plan: BlsActivationPlan,
        doctor: BlsDoctorReport,
        sealed_doctor_capture: SealedProviderCaptureSetReceipt,
        activated_at: Timestamp,
        runtime_instance: Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<Self, BlsSourceError> {
        plan.validate()?;
        validate_doctor_capture(&plan, &doctor, &sealed_doctor_capture, &runtime_instance)?;
        let expires_at = doctor
            .locally_available_at()
            .checked_add_nanos(BLS_DOCTOR_ACTIVATION_TTL_NANOS)
            .map_err(|_| BlsSourceError::InvalidPublication)?;
        if activated_at < doctor.locally_available_at() || activated_at >= expires_at {
            return Err(BlsSourceError::InvalidPublication);
        }
        let candidate_digest = activation_candidate_digest(
            plan.plan_digest(),
            doctor.report_digest(),
            sealed_doctor_capture.receipt_digest(),
            activated_at,
            expires_at,
        )?;
        Ok(Self {
            plan,
            doctor,
            sealed_doctor_capture,
            activated_at,
            expires_at,
            candidate_digest,
            runtime_instance,
        })
    }

    /// Reopens the full plan/report/physical-receipt binding at one trusted operation clock.
    pub(crate) fn validate(
        &self,
        expected_plan: &BlsActivationPlan,
        operation_at: Timestamp,
        expected_runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
    ) -> Result<(), BlsSourceError> {
        self.plan.validate()?;
        validate_doctor_capture(
            &self.plan,
            &self.doctor,
            &self.sealed_doctor_capture,
            expected_runtime_instance,
        )?;
        let expected_digest = activation_candidate_digest(
            self.plan.plan_digest(),
            self.doctor.report_digest(),
            self.sealed_doctor_capture.receipt_digest(),
            self.activated_at,
            self.expires_at,
        )?;
        if &self.plan != expected_plan
            || operation_at < self.activated_at
            || operation_at >= self.expires_at
            || !Arc::ptr_eq(&self.runtime_instance, expected_runtime_instance)
            || expected_digest != self.candidate_digest
        {
            return Err(BlsSourceError::InvalidPublication);
        }
        Ok(())
    }

    /// Returns the exact source/configuration/rate plan admitted by the doctor.
    pub const fn plan(&self) -> &BlsActivationPlan {
        &self.plan
    }

    /// Returns the redacted successful doctor retained by this in-process admission.
    pub const fn doctor_report(&self) -> &BlsDoctorReport {
        &self.doctor
    }

    /// Returns the actual immutable doctor receipt retained by this in-process admission.
    pub const fn sealed_doctor_capture(&self) -> &SealedProviderCaptureSetReceipt {
        &self.sealed_doctor_capture
    }

    /// Returns when this source admitted the already sealed successful doctor in-process.
    pub const fn activated_at(&self) -> Timestamp {
        self.activated_at
    }

    /// Returns the exclusive end of this doctor-backed production window.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the non-authoritative identity of this in-process readiness admission.
    pub const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }

    pub(crate) fn runtime_instance(&self) -> &Arc<BlsRuntimeInstanceCapability> {
        &self.runtime_instance
    }
}

fn validate_doctor_capture(
    plan: &BlsActivationPlan,
    doctor: &BlsDoctorReport,
    sealed: &SealedProviderCaptureSetReceipt,
    runtime_instance: &Arc<BlsRuntimeInstanceCapability>,
) -> Result<(), BlsSourceError> {
    doctor.validate()?;
    let capture = sealed.capture();
    let page = capture
        .pages()
        .first()
        .ok_or(BlsSourceError::InvalidPublication)?;
    if doctor.readiness() != BlsDoctorReadiness::Available
        || doctor.source_id() != plan.source_id()
        || doctor.metadata_revision() != plan.metadata_revision()
        || doctor.tier() != plan.rate().tier()
        || doctor.dataset() != plan.provider_dataset()
        || doctor.returned_series() != 1
        || doctor.returned_observations() == 0
        || doctor.observed_values() != doctor.returned_observations()
        || doctor.missing_values() != 0
        || doctor.provider_messages() != 0
        || doctor.credential_rejoin() != plan.credential_rejoin()
        || doctor.provider_rate_declaration_digest() != plan.rate().declaration_digest()
        || doctor.limits() != plan.rate().limits()
        || capture.source_id() != plan.source_id()
        || capture.metadata_revision() != plan.metadata_revision()
        || capture.dataset() != plan.provider_dataset()
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.pages().len() != 1
        || !capture.request_graph_components().is_empty()
        || capture.request_set_identity() != doctor.request_set_identity()
        || capture.content_digest() != doctor.capture_content_digest()
        || capture.observation_digest() != doctor.capture_observation_digest()
        || capture.total_body_bytes() != doctor.response_bytes()
        || page.ordinal() != 0
        || page.http_status() != 200
        || page.received_at() != doctor.locally_available_at()
        || page.body_digest() != doctor.response_content_digest()
        || sealed.receipt_digest().bytes() == [0; 32]
        || !doctor.matches_runtime_instance(runtime_instance)
    {
        return Err(BlsSourceError::InvalidPublication);
    }
    Ok(())
}

fn credential_rejoin_matches_tier(
    credential_rejoin: BlsCredentialRejoin,
    tier: BlsAccessTier,
) -> bool {
    matches!(
        (credential_rejoin, tier),
        (
            BlsCredentialRejoin::PublicNoCredential,
            BlsAccessTier::PublicV1
        ) | (
            BlsCredentialRejoin::RegisteredGeneration(_),
            BlsAccessTier::RegisteredV2
        )
    )
}

fn hash_credential_rejoin(digest: &mut Sha256, value: BlsCredentialRejoin) {
    match value {
        BlsCredentialRejoin::PublicNoCredential => digest.update(b"public-no-credential"),
        BlsCredentialRejoin::RegisteredGeneration(generation) => {
            digest.update(b"registered-generation");
            hash_contract_digest(digest, generation);
        }
    }
}

fn activation_candidate_digest(
    plan: EvidenceDigest,
    doctor: EvidenceDigest,
    sealed: EvidenceDigest,
    activated_at: Timestamp,
    expires_at: Timestamp,
) -> Result<EvidenceDigest, BlsSourceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/bls-activation-candidate/v1\0");
    for value in [plan, doctor, sealed] {
        hash_contract_digest(&mut digest, value);
    }
    digest.update(activated_at.unix_nanos().to_be_bytes());
    digest.update(expires_at.unix_nanos().to_be_bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_contract_digest(digest: &mut Sha256, value: EvidenceDigest) {
    digest.update(match value.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(value.bytes());
}

fn hash_contract_field(digest: &mut Sha256, value: &[u8]) -> Result<(), BlsSourceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| BlsSourceError::InvalidPublication)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}
