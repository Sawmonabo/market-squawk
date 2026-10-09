//! Generation-bound publication of callable research-provider adapters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt;
use market_squawk_data::{DatasetId, IngestError, IngestPrecommitAuthority, SourceOperation};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{
    ProviderCapabilityRevision, RuntimeVerificationEvidence, SEC_EDGAR_PROFILE_ID,
    SEC_EDGAR_SOURCE_ID, SourceMetadata, SourceMetadataProvider,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::schwab_market::{
    SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET, SchwabMarketPublicationClosure,
    SchwabMarketPublicationError, SchwabRestQuoteGenerationAuthority,
};
use super::{
    CryptoMarketPublicationClosure, ManagedResearchExtractionSource, MarketEventDurableRead,
    MarketEventDurableReadWriter, MarketEventPointInTimeSelector,
    ProductionResearchIngestCoordinator, ResearchIngestCompositionError, ResearchRightsAuthority,
};
use crate::provider_onboarding::SchwabOAuthReceiptCurrentness;

/// Exact non-secret generation identity for one callable research-provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResearchProviderRuntimeGeneration {
    profile: SourceIdentifier,
    session_id: Uuid,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    credential_generation: Option<SecretGeneration>,
    secret_reference: Option<SecretRef>,
    authority_effective_at: Timestamp,
    runtime_verification: Option<RuntimeVerificationEvidence>,
    metadata: SourceMetadata,
    rights: ResearchRightsAuthority,
}

/// Exact Schwab REST quote publication generation and its sole neutral durable-read channel.
///
/// The generation authority and writer are consumed by the provider runtime while the paired read
/// is installed into the provider-neutral selector registry. Keeping all three in one package
/// prevents a runtime from combining publication and point-in-time capabilities from different
/// source generations.
#[derive(Debug)]
pub(crate) struct SchwabRestQuotePublicationPackage {
    generation: Arc<SchwabRestQuoteGenerationAuthority>,
    durable_writer: MarketEventDurableReadWriter,
    durable_read: MarketEventDurableRead,
}

impl SchwabRestQuotePublicationPackage {
    pub(crate) const fn durable_read(&self) -> &MarketEventDurableRead {
        &self.durable_read
    }

    pub(crate) fn into_runtime_parts(
        self,
    ) -> (
        Arc<SchwabRestQuoteGenerationAuthority>,
        MarketEventDurableReadWriter,
    ) {
        (self.generation, self.durable_writer)
    }
}

#[derive(Debug)]
pub(crate) struct SchwabStreamerPublicationPackage {
    pub(crate) authority: Arc<super::schwab_market::SchwabStreamerGenerationAuthority>,
    pub(crate) durable_writer: MarketEventDurableReadWriter,
    pub(crate) durable_read: MarketEventDurableRead,
}

impl ResearchProviderRuntimeGeneration {
    /// Binds one adapter candidate to onboarding, secret, metadata, and rights authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "runtime authority dimensions remain explicit in one validated constructor"
    )]
    pub fn try_new(
        profile: SourceIdentifier,
        session_id: Uuid,
        capability_revision: ProviderCapabilityRevision,
        capability_digest: EvidenceDigest,
        credential_generation: Option<SecretGeneration>,
        secret_reference: Option<SecretRef>,
        authority_effective_at: Timestamp,
        metadata: SourceMetadata,
        rights: ResearchRightsAuthority,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let secret_binding_valid = match (credential_generation, secret_reference.as_ref()) {
            (None, None) => true,
            (Some(generation), Some(reference)) => reference.generation() == generation,
            (None, Some(_)) | (Some(_), None) => false,
        };
        if profile.as_str().is_empty()
            || session_id.is_nil()
            || capability_digest.bytes() == [0; 32]
            || !secret_binding_valid
            || metadata.source_id() != &rights.source_id
            || !metadata.is_effective_at(authority_effective_at)
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        Ok(Self {
            profile,
            session_id,
            capability_revision,
            capability_digest,
            credential_generation,
            secret_reference,
            authority_effective_at,
            runtime_verification: None,
            metadata,
            rights,
        })
    }

    /// Retains the typed doctor already admitted by this exact activation lease. Digest-only
    /// verification cannot authorize same-credential renewal and keeps its existing identity.
    pub(crate) fn with_runtime_verification(
        mut self,
        lease: &crate::ProviderActivationLease,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let Some(evidence) = lease.runtime_verification_evidence() else {
            return Ok(self);
        };
        if evidence.verified_at().is_none() {
            return Ok(self);
        }
        if self.session_id != lease.session_id()
            || self.capability_revision != lease.capability_revision()
            || self.capability_digest != lease.capability_digest()
            || self.credential_generation != lease.generation()
            || self.secret_reference.as_ref() != lease.secret_reference()
            || self.authority_effective_at != lease.authority_effective_at()
            || self.rights.parent_authorization_evidence != lease.rights_decision_digest()
            || evidence.exclusive_expires_at() != lease.verification_expires_at()
            || !evidence.admits_activation_at(lease.issued_at())
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        self.runtime_verification = Some(evidence.clone());
        Ok(self)
    }

    /// Returns the profile/surface identity selecting the runtime slot.
    pub const fn profile(&self) -> &SourceIdentifier {
        &self.profile
    }

    /// Returns the durable onboarding session.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the exact capability revision.
    pub const fn capability_revision(&self) -> ProviderCapabilityRevision {
        self.capability_revision
    }

    /// Returns the exact canonical capability evidence.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.capability_digest
    }

    /// Returns the exact credential generation, when this surface uses one.
    pub const fn credential_generation(&self) -> Option<SecretGeneration> {
        self.credential_generation
    }

    /// Returns the opaque exact-generation secret reference, when credential-backed.
    pub const fn secret_reference(&self) -> Option<&SecretRef> {
        self.secret_reference.as_ref()
    }

    /// Returns the exact source metadata retained by this adapter.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }

    /// Returns the durable instant from which this exact generation is effective.
    pub const fn authority_effective_at(&self) -> Timestamp {
        self.authority_effective_at
    }

    /// Returns the exact admitted rights decision bound into this generation.
    pub const fn rights_authorization_evidence(&self) -> EvidenceDigest {
        self.rights.authorization_evidence
    }

    /// Returns the parent onboarding rights decision that admitted this subordinate authority.
    pub const fn parent_rights_authorization_evidence(&self) -> EvidenceDigest {
        self.rights.parent_authorization_evidence
    }

    /// Returns the finite subordinate authority expiry, when this generation is time-bounded.
    pub const fn rights_authorization_expires_at(&self) -> Option<Timestamp> {
        self.rights.authorization_expires_at
    }

    /// Returns the exact provider subjects, or `None` for a source-wide authority.
    pub const fn rights_exact_subjects(
        &self,
    ) -> Option<&std::collections::BTreeSet<SourceIdentifier>> {
        self.rights.exact_subjects.as_ref()
    }

    /// Returns whether the subordinate authority admits one operation.
    pub fn rights_admits(&self, operation: SourceOperation) -> bool {
        self.rights.permitted_operations.contains(&operation)
    }

    /// Returns the stable callable slot shared by legitimate generations of one provider source.
    pub fn slot_identity_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeSlotWire<'a> {
            profile: &'a SourceIdentifier,
            source_id: &'a market_squawk_domain::SourceId,
        }

        digest_runtime_wire(
            b"market-squawk/research-provider-runtime-slot/v1\0",
            &RuntimeSlotWire {
                profile: &self.profile,
                source_id: self.metadata.source_id(),
            },
        )
    }

    /// Returns a canonical digest of every non-secret exact-generation authority dimension.
    pub fn generation_digest(&self) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        #[derive(Serialize)]
        #[serde(deny_unknown_fields)]
        struct RuntimeGenerationWire<'a> {
            slot_identity_digest: EvidenceDigest,
            profile: &'a SourceIdentifier,
            session_id: Uuid,
            capability_revision: ProviderCapabilityRevision,
            capability_digest: EvidenceDigest,
            credential_generation: Option<SecretGeneration>,
            secret_reference: Option<&'a SecretRef>,
            authority_effective_at: Timestamp,
            runtime_verification_digest: Option<EvidenceDigest>,
            metadata: &'a SourceMetadata,
            rights_source_id: &'a market_squawk_domain::SourceId,
            rights_basis_reference: &'a str,
            rights_basis_digest: EvidenceDigest,
            rights_root_identity_digest: Option<EvidenceDigest>,
            rights_parent_authorization_evidence: EvidenceDigest,
            rights_authorization_evidence: EvidenceDigest,
            rights_authorization_expires_at: Option<market_squawk_domain::Timestamp>,
            rights_contract_digest: EvidenceDigest,
        }

        digest_runtime_wire(
            b"market-squawk/research-provider-runtime-generation/v3\0",
            &RuntimeGenerationWire {
                slot_identity_digest: self.slot_identity_digest()?,
                profile: &self.profile,
                session_id: self.session_id,
                capability_revision: self.capability_revision,
                capability_digest: self.capability_digest,
                credential_generation: self.credential_generation,
                secret_reference: self.secret_reference.as_ref(),
                authority_effective_at: self.authority_effective_at,
                runtime_verification_digest: self
                    .runtime_verification
                    .as_ref()
                    .map(RuntimeVerificationEvidence::evidence_digest),
                metadata: &self.metadata,
                rights_source_id: &self.rights.source_id,
                rights_basis_reference: self.rights.basis.reference(),
                rights_basis_digest: self.rights.basis.digest(),
                rights_root_identity_digest: self.rights.basis.root_identity_digest(),
                rights_parent_authorization_evidence: self.rights.parent_authorization_evidence,
                rights_authorization_evidence: self.rights.authorization_evidence,
                rights_authorization_expires_at: self.rights.authorization_expires_at,
                rights_contract_digest: rights_contract_digest(&self.rights),
            },
        )
    }

    /// Reconstructs acquisition identity only; this value cannot register or reactivate a runtime.
    /// The caller must compare the result with the digest sealed into the original custody context.
    pub(crate) fn retained_option_generation_digest(
        &self,
        metadata: SourceMetadata,
        renewal: &market_squawk_sources::AlpacaDoctorRenewalChain,
    ) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
        let current = RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(Box::new(
            renewal.current().clone(),
        ));
        if self.runtime_verification.as_ref() != Some(&current)
            || self.metadata.source_id() != metadata.source_id()
            || self.session_id.to_string() != renewal.original().session_identifier().as_str()
            || self.credential_generation != Some(renewal.original().generation())
            || self.capability_revision != renewal.original().capability_revision()
            || self.capability_digest != renewal.original().capability_digest()
            || self.rights.parent_authorization_evidence
                != renewal.original().rights_decision_digest()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let mut original = self.clone();
        original.authority_effective_at = renewal.original().verified_at();
        original.runtime_verification =
            Some(RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(
                Box::new(renewal.original().clone()),
            ));
        original.metadata = metadata;
        if !original
            .metadata
            .is_effective_at(original.authority_effective_at)
            || (self.generation_digest()? != original.generation_digest()?
                && !self.is_same_credential_option_renewal_of(&original))
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        original.generation_digest()
    }

    fn is_same_credential_option_renewal_of(&self, original: &Self) -> bool {
        self.profile == original.profile
            && self.session_id == original.session_id
            && self.credential_generation == original.credential_generation
            && self.secret_reference == original.secret_reference
            && self.capability_revision == original.capability_revision
            && self.capability_digest == original.capability_digest
            && self.rights.parent_authorization_evidence
                == original.rights.parent_authorization_evidence
            && self.rights.authorization_evidence == original.rights.authorization_evidence
            && self.rights.basis == original.rights.basis
            && self.rights.permitted_operations == original.rights.permitted_operations
            && self.rights.exact_subjects == original.rights.exact_subjects
            && self.has_renewed_runtime_verification(original)
    }

    fn is_exact_successor_of(
        &self,
        expected: &Self,
    ) -> Result<bool, ResearchIngestCompositionError> {
        if self.profile != expected.profile
            || self.metadata.source_id() != expected.metadata.source_id()
            || self.slot_identity_digest()? != expected.slot_identity_digest()?
            || self.generation_digest()? == expected.generation_digest()?
            || self.authority_effective_at <= expected.authority_effective_at
            || self.capability_revision < expected.capability_revision
        {
            return Ok(false);
        }
        if self.session_id != expected.session_id {
            return Ok(true);
        }
        // Renewal preserves credentials while replacing provider-observed verification. The
        // current lease proves the durable doctor chain, including any intervening receipts;
        // this slot may have been unused during those renewals. Registration still requires
        // the exact predecessor's revocation to be fully drained.
        if self.credential_generation.is_some()
            && self.credential_generation == expected.credential_generation
            && self.secret_reference == expected.secret_reference
            && self.capability_revision == expected.capability_revision
            && self.capability_digest == expected.capability_digest
            && self.rights.parent_authorization_evidence
                == expected.rights.parent_authorization_evidence
            && self.rights.authorization_evidence == expected.rights.authorization_evidence
            && self.rights.basis == expected.rights.basis
            && self.rights.permitted_operations == expected.rights.permitted_operations
            && self.rights.exact_subjects == expected.rights.exact_subjects
            && self.has_renewed_runtime_verification(expected)
        {
            return Ok(true);
        }
        Ok(self.capability_revision == expected.capability_revision
            && self.capability_digest == expected.capability_digest
            && match (
                expected.credential_generation,
                self.credential_generation,
                expected.secret_reference.as_ref(),
                self.secret_reference.as_ref(),
            ) {
                (
                    Some(prior),
                    Some(candidate),
                    Some(prior_reference),
                    Some(candidate_reference),
                ) => {
                    prior
                        .get()
                        .checked_add(1)
                        .is_some_and(|next| next == candidate.get())
                        && prior_reference != candidate_reference
                }
                _ => false,
            })
    }

    fn has_renewed_runtime_verification(&self, expected: &Self) -> bool {
        let (Some(current), Some(prior)) = (
            self.runtime_verification.as_ref(),
            expected.runtime_verification.as_ref(),
        ) else {
            return false;
        };
        if current.verified_at() <= prior.verified_at()
            || current.exclusive_expires_at() < prior.exclusive_expires_at()
            || !current.is_activation_ready()
            || !prior.is_activation_ready()
        {
            return false;
        }
        match (current, prior) {
            (
                RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(current),
                RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(prior),
            ) => {
                current.surface_id() == prior.surface_id()
                    && current.session_identifier() == prior.session_identifier()
                    && current.generation() == prior.generation()
                    && current.realm() == prior.realm()
                    && current.market_data_principal_sha256()
                        == prior.market_data_principal_sha256()
                    && current.capability_revision() == prior.capability_revision()
                    && current.capability_digest() == prior.capability_digest()
                    && current.public_configuration_digest() == prior.public_configuration_digest()
                    && current.rights_decision_digest() == prior.rights_decision_digest()
                    && current.rate_policy_digest() == prior.rate_policy_digest()
                    && current.doctor_revision() == prior.doctor_revision()
                    && current.doctor_contract_digest() == prior.doctor_contract_digest()
            }
            _ => false,
        }
    }
}

fn rights_contract_digest(rights: &ResearchRightsAuthority) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/research-rights-contract/v1\0");
    digest.update(rights.parent_authorization_evidence.bytes());
    digest.update(rights.authorization_evidence.bytes());
    match rights.authorization_expires_at {
        Some(expires_at) => {
            digest.update([1]);
            digest.update(expires_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    match &rights.exact_subjects {
        Some(subjects) => {
            digest.update([1]);
            digest.update(
                u32::try_from(subjects.len())
                    .unwrap_or(u32::MAX)
                    .to_be_bytes(),
            );
            for subject in subjects {
                update_digest_part(&mut digest, subject.as_str().as_bytes());
            }
        }
        None => digest.update([0]),
    }
    digest.update(
        u32::try_from(rights.permitted_operations.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    for operation in &rights.permitted_operations {
        digest.update([source_operation_tag(*operation)]);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn update_digest_part(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

const fn source_operation_tag(operation: SourceOperation) -> u8 {
    match operation {
        SourceOperation::Retrieve => 1,
        SourceOperation::Display => 2,
        SourceOperation::Persist => 3,
        SourceOperation::Cache => 4,
        SourceOperation::Redistribute => 5,
        SourceOperation::Train => 6,
    }
}

fn digest_runtime_wire<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<EvidenceDigest, ResearchIngestCompositionError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

/// Per-generation, process-local authority checked before and during every provider request.
#[derive(Clone, Debug)]
pub(super) struct ResearchProviderAdmission {
    generation_digest: Option<EvidenceDigest>,
    source_id: Option<SourceId>,
    metadata_revision: Option<MetadataRevision>,
    state: Arc<ResearchProviderAdmissionState>,
    cancellation: CancellationToken,
}

const ADMISSION_PENDING: u8 = 0;
const ADMISSION_ACTIVE: u8 = 1;
const ADMISSION_REVOKING: u8 = 2;
const ADMISSION_DRAINED: u8 = 3;

#[derive(Debug)]
struct ResearchProviderAdmissionState {
    phase: AtomicU8,
    publication_barrier: Arc<RwLock<()>>,
}

/// Exact-generation lease retained across the durable research publication boundary.
pub(super) struct ResearchProviderPublicationLease {
    admission: ResearchProviderAdmission,
    _publication: OwnedRwLockReadGuard<()>,
}

impl std::fmt::Debug for ResearchProviderPublicationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderPublicationLease")
            .field("generation_digest", &self.admission.generation_digest)
            .finish_non_exhaustive()
    }
}

impl ResearchProviderPublicationLease {
    pub(super) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()
    }
}

impl IngestPrecommitAuthority for ResearchProviderPublicationLease {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        ResearchProviderPublicationLease::validate_precommit(self)
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)
    }
}

/// Coordinator-owned exact-generation authority spanning specialized provider network, raw seal,
/// and final analytical commit without exposing the registry or publication lease separately.
pub(crate) struct ResearchProviderPublicationOperation {
    generation: ResearchProviderRuntimeGeneration,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
    publication: Arc<ResearchProviderPublicationLease>,
    cancellation: CancellationToken,
    cancellation_cause: Arc<AtomicU8>,
    upstream_cancellation: [CancellationToken; 3],
    watcher: JoinHandle<()>,
}

/// The first signal observed by the existing publication-operation watcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ProviderPublicationCancellationCause {
    Caller = 1,
    Shutdown = 2,
    Revoked = 3,
    Deadline = 4,
}

fn spawn_publication_cancellation_watcher(
    signal: CancellationToken,
    cause: Arc<AtomicU8>,
    caller: CancellationToken,
    shutdown: CancellationToken,
    revoked: CancellationToken,
    deadline: Instant,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let observed = tokio::select! {
            biased;
            () = caller.cancelled() => ProviderPublicationCancellationCause::Caller,
            () = shutdown.cancelled() => ProviderPublicationCancellationCause::Shutdown,
            () = revoked.cancelled() => ProviderPublicationCancellationCause::Revoked,
            () = tokio::time::sleep_until(deadline.into()) => ProviderPublicationCancellationCause::Deadline,
        };
        // Publish the reason before waking any consumer of the operation token.
        cause.store(observed as u8, Ordering::Release);
        signal.cancel();
    })
}

/// Application-minted, exact-generation authority for one crypto canonical-publication lane.
///
/// All fields remain private so callers can retain and use the authority but cannot substitute a
/// source, dataset, rights grant, publication lease, or research service.
pub(crate) struct CryptoMarketPublicationAuthority {
    operation: ResearchProviderPublicationOperation,
    publication: Arc<CryptoMarketPublicationClosure>,
    analytical_dataset: DatasetId,
    precommit: Arc<dyn IngestPrecommitAuthority>,
}

impl CryptoMarketPublicationAuthority {
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        self.operation.generation()
    }

    pub(crate) fn publication(&self) -> Arc<CryptoMarketPublicationClosure> {
        self.publication.clone()
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) fn precommit_authority(&self) -> Arc<dyn IngestPrecommitAuthority> {
        self.precommit.clone()
    }

    /// Mints the sole source- and dataset-bound durable-read handoff for this runtime generation.
    pub(crate) fn durable_read_capability(
        &self,
    ) -> (MarketEventDurableReadWriter, MarketEventDurableRead) {
        let point_in_time = self
            .publication
            .point_in_time_selector(self.analytical_dataset.clone());
        MarketEventDurableRead::channel(point_in_time)
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        self.operation.cancellation()
    }

    pub(crate) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        self.operation.validate_precommit()
    }
}

impl std::fmt::Debug for CryptoMarketPublicationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CryptoMarketPublicationAuthority")
            .field("generation", self.operation.generation())
            .field("analytical_dataset", &self.analytical_dataset)
            .finish_non_exhaustive()
    }
}

impl ResearchProviderPublicationOperation {
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    pub(crate) const fn source(&self) -> &SourceMetadata {
        &self.source
    }

    pub(crate) const fn rights(&self) -> &ResearchRightsAuthority {
        &self.rights
    }

    pub(crate) const fn source_registered_at(&self) -> Timestamp {
        self.source_registered_at
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn cancellation_cause(&self) -> Option<ProviderPublicationCancellationCause> {
        match self.cancellation_cause.load(Ordering::Acquire) {
            1 => Some(ProviderPublicationCancellationCause::Caller),
            2 => Some(ProviderPublicationCancellationCause::Shutdown),
            3 => Some(ProviderPublicationCancellationCause::Revoked),
            4 => Some(ProviderPublicationCancellationCause::Deadline),
            _ => None,
        }
    }

    /// A concurrent source stop/revocation always vetoes local-timeout recovery.
    pub(crate) fn has_local_deadline_failure(&self) -> bool {
        self.cancellation_cause() == Some(ProviderPublicationCancellationCause::Deadline)
            && !self
                .upstream_cancellation
                .iter()
                .any(CancellationToken::is_cancelled)
    }

    pub(crate) fn precommit_authority(&self) -> Arc<dyn IngestPrecommitAuthority> {
        self.publication.clone()
    }

    pub(crate) fn validate_precommit(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.publication.validate_precommit()
    }
}

impl Drop for ResearchProviderPublicationOperation {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.watcher.abort();
    }
}

impl std::fmt::Debug for ResearchProviderPublicationOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderPublicationOperation")
            .field("generation", &self.generation)
            .field("source_id", self.source.source_id())
            .finish_non_exhaustive()
    }
}

impl ResearchProviderAdmission {
    pub(super) fn new(
        generation: Option<&ResearchProviderRuntimeGeneration>,
    ) -> Result<Self, ResearchIngestCompositionError> {
        Self::with_phase(generation, ADMISSION_ACTIVE)
    }

    fn new_pending(
        generation: &ResearchProviderRuntimeGeneration,
    ) -> Result<Self, ResearchIngestCompositionError> {
        Self::with_phase(Some(generation), ADMISSION_PENDING)
    }

    /// Creates one pending admission for a sealed, non-generic parent generation.
    ///
    /// Alpaca history is subordinate to a market-runtime group rather than to a generic research
    /// provider generation. Keeping this constructor digest-only prevents that private parent from
    /// changing the generic generation wire identity or successor protocol.
    pub(super) fn new_pending_for_parent_digest(
        parent_digest: EvidenceDigest,
    ) -> Result<Self, ResearchIngestCompositionError> {
        if parent_digest.algorithm() != DigestAlgorithm::Sha256 || parent_digest.bytes() == [0; 32]
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        Ok(Self {
            generation_digest: Some(parent_digest),
            source_id: None,
            metadata_revision: None,
            state: Arc::new(ResearchProviderAdmissionState {
                phase: AtomicU8::new(ADMISSION_PENDING),
                publication_barrier: Arc::new(RwLock::new(())),
            }),
            cancellation: CancellationToken::new(),
        })
    }

    fn with_phase(
        generation: Option<&ResearchProviderRuntimeGeneration>,
        phase: u8,
    ) -> Result<Self, ResearchIngestCompositionError> {
        let (generation_digest, source_id, metadata_revision) = match generation {
            Some(generation) => (
                Some(generation.generation_digest()?),
                Some(generation.metadata().source_id().clone()),
                Some(generation.metadata().revision().clone()),
            ),
            None => (None, None, None),
        };
        Ok(Self {
            generation_digest,
            source_id,
            metadata_revision,
            state: Arc::new(ResearchProviderAdmissionState {
                phase: AtomicU8::new(phase),
                publication_barrier: Arc::new(RwLock::new(())),
            }),
            cancellation: CancellationToken::new(),
        })
    }

    pub(super) fn ensure_live(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled()
            || self.state.phase.load(Ordering::Acquire) != ADMISSION_ACTIVE
        {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        } else {
            Ok(())
        }
    }

    /// Returns whether this live admission was minted for the exact source, metadata revision,
    /// and complete non-secret runtime generation supplied by the caller.
    pub(super) fn admits_generation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
    ) -> Result<bool, ResearchIngestCompositionError> {
        self.ensure_live()?;
        let metadata = generation.metadata();
        Ok(
            self.generation_digest == Some(generation.generation_digest()?)
                && self.source_id.as_ref() == Some(metadata.source_id())
                && self.metadata_revision.as_ref() == Some(metadata.revision())
                && metadata.source_id() == &generation.rights.source_id
                && metadata.is_effective_at(generation.authority_effective_at()),
        )
    }

    pub(super) fn matches(&self, other: &Self) -> bool {
        self.generation_digest == other.generation_digest
            && self.source_id == other.source_id
            && self.metadata_revision == other.metadata_revision
            && Arc::ptr_eq(&self.state, &other.state)
    }

    pub(super) fn revoke(&self) {
        self.begin_revocation();
    }

    fn begin_revocation(&self) {
        let mut phase = self.state.phase.load(Ordering::Acquire);
        while matches!(phase, ADMISSION_PENDING | ADMISSION_ACTIVE) {
            match self.state.phase.compare_exchange(
                phase,
                ADMISSION_REVOKING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => phase = current,
            }
        }
        self.cancellation.cancel();
    }

    pub(super) fn ensure_pending(&self) -> Result<(), ResearchIngestCompositionError> {
        if self.cancellation.is_cancelled()
            || self.state.phase.load(Ordering::Acquire) != ADMISSION_PENDING
        {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        } else {
            Ok(())
        }
    }

    pub(super) fn activate_pending(&self) {
        debug_assert!(!self.cancellation.is_cancelled());
        let prior = self.state.phase.swap(ADMISSION_ACTIVE, Ordering::AcqRel);
        debug_assert_eq!(prior, ADMISSION_PENDING);
    }

    pub(super) async fn acquire_publication_lease(
        &self,
    ) -> Result<ResearchProviderPublicationLease, ResearchIngestCompositionError> {
        self.ensure_live()?;
        let publication = Arc::clone(&self.state.publication_barrier)
            .read_owned()
            .await;
        self.ensure_live()?;
        Ok(ResearchProviderPublicationLease {
            admission: self.clone(),
            _publication: publication,
        })
    }

    pub(super) fn revoke_if_idle(&self) -> bool {
        self.begin_revocation();
        let Ok(_publication) = self.state.publication_barrier.try_write() else {
            return false;
        };
        self.state.phase.store(ADMISSION_DRAINED, Ordering::Release);
        true
    }

    pub(super) async fn revoke_and_drain(&self) {
        self.begin_revocation();
        let publication = Arc::clone(&self.state.publication_barrier)
            .write_owned()
            .await;
        self.state.phase.store(ADMISSION_DRAINED, Ordering::Release);
        drop(publication);
    }

    pub(super) fn revocation_drained(&self) -> bool {
        self.state.phase.load(Ordering::Acquire) == ADMISSION_DRAINED
    }

    pub(super) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// One callable Schwab generation bound to both generic provider and exact OAuth currentness.
struct SchwabCompositeMarketRuntimeAdmission {
    generation_digest: EvidenceDigest,
    admission: ResearchProviderAdmission,
    oauth: SchwabOAuthReceiptCurrentness,
    oauth_receipt: SchwabOAuthAuthorityReceipt,
}

#[cfg(test)]
pub(super) fn test_schwab_composite_market_runtime_admission(
    generation: &ResearchProviderRuntimeGeneration,
    oauth: SchwabOAuthReceiptCurrentness,
    oauth_receipt: SchwabOAuthAuthorityReceipt,
) -> Result<
    Arc<dyn super::schwab_market::SchwabMarketRuntimeAdmission>,
    ResearchIngestCompositionError,
> {
    validate_schwab_oauth_binding(generation, &oauth, oauth_receipt)?;
    let generation_digest = generation.generation_digest()?;
    Ok(Arc::new(SchwabCompositeMarketRuntimeAdmission {
        generation_digest,
        admission: ResearchProviderAdmission::new(Some(generation))?,
        oauth,
        oauth_receipt,
    }))
}

fn validate_schwab_oauth_binding(
    generation: &ResearchProviderRuntimeGeneration,
    oauth: &SchwabOAuthReceiptCurrentness,
    receipt: SchwabOAuthAuthorityReceipt,
) -> Result<(), ResearchIngestCompositionError> {
    let reference = generation
        .secret_reference()
        .ok_or(ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
    let credential = market_squawk_adapter_schwab::SchwabCredentialAuthorityBinding::try_from_application_credential(reference)
        .map_err(|_| ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
    if oauth.session_id() != generation.session_id()
        || generation.credential_generation() != Some(reference.generation())
        || receipt.credential_authority() != credential
    {
        return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
    }
    oauth
        .validate_current_receipt(receipt)
        .map_err(|_| ResearchIngestCompositionError::StaleRuntimeGeneration)
}

impl SchwabCompositeMarketRuntimeAdmission {
    fn ensure_exact_current(&self) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()?;
        if self.admission.generation_digest != Some(self.generation_digest) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.oauth
            .validate_current_authorization(self.oauth_receipt)
            .map_err(|_error| ResearchIngestCompositionError::StaleRuntimeGeneration)?;
        self.admission.ensure_live()
    }
}

impl super::schwab_market::SchwabMarketRuntimeAdmission for SchwabCompositeMarketRuntimeAdmission {
    fn generation_digest(&self) -> Option<EvidenceDigest> {
        self.ensure_exact_current()
            .ok()
            .map(|()| self.generation_digest)
    }

    fn ensure_live(&self) -> Result<(), ResearchIngestCompositionError> {
        self.ensure_exact_current()
    }

    fn validate_oauth_current(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.admission.ensure_live()?;
        if receipt.credential_authority() != self.oauth_receipt.credential_authority()
            || receipt.authorization_generation() != self.oauth_receipt.authorization_generation()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.oauth
            .validate_current_receipt(receipt)
            .map_err(|_error| ResearchIngestCompositionError::StaleRuntimeGeneration)?;
        self.ensure_exact_current()
    }

    fn cancellation(&self) -> &CancellationToken {
        self.admission.cancellation()
    }

    fn acquire_publication_lease(
        &self,
    ) -> BoxFuture<'_, Result<ResearchProviderPublicationLease, ResearchIngestCompositionError>>
    {
        Box::pin(async move {
            self.ensure_exact_current()?;
            let lease = self.admission.acquire_publication_lease().await?;
            self.ensure_exact_current()?;
            Ok(lease)
        })
    }

    fn revoke(&self) {
        self.admission.revoke();
    }

    fn revoke_and_drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.admission.revoke_and_drain().await;
        })
    }

    fn revocation_drained(&self) -> bool {
        self.admission.revocation_drained()
    }
}

impl std::fmt::Debug for SchwabCompositeMarketRuntimeAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabCompositeMarketRuntimeAdmission")
            .field("generation_digest", &self.generation_digest)
            .field("oauth", &"[SECRET-FREE RECEIPT VALIDATION]")
            .field("oauth_generation", &self.oauth_receipt.generation().get())
            .finish_non_exhaustive()
    }
}

/// Fully constructed replacement held outside the callable runtime until exact finalization.
struct PreparedResearchProviderReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_capability: Option<super::RegisteredSourceCapability>,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl PreparedResearchProviderReplacement {
    /// Revokes and drains only the token-bound predecessor retained by this transaction.
    async fn revoke_predecessor(&mut self) -> Result<(), ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            let current = authority
                .sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(&self.expected)
                || current.metadata != self.expected.metadata
                || current.rights != self.expected.rights
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            let admission = current.admission.clone();
            authority.selections.revoke_profile(&self.profile);
            admission
        };
        // Revocation has taken effect even if draining later fails or is cancelled.
        self.coordinator
            .research
            .application_changes()
            .record(market_squawk_services::ServiceDomain::Source);
        admission.revoke_and_drain().await;
        super::treasury::drain_generation_replay(&self.coordinator, &self.expected).await?;
        Ok(())
    }

    /// Restores the exact predecessor retained by this token without publishing the candidate.
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let authority = &mut *authority;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let replacement_admission = {
            let current = authority
                .sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(&self.expected)
                || current.metadata != self.expected.metadata
                || current.rights != self.expected.rights
                || current.registration.source_id() != current.metadata.source_id()
                || current.registration.revision() != current.metadata.revision()
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            if current.admission.revocation_drained() {
                Some(ResearchProviderAdmission::new(Some(&self.expected))?)
            } else {
                current.admission.ensure_live()?;
                None
            }
        };
        self.candidate_admission.revoke();
        if let Some(admission) = replacement_admission {
            let current = authority
                .sources
                .get_mut(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            current.admission = admission;
        }
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    /// Transfers the validated candidate into a still-non-callable committed capability.
    fn commit(
        &mut self,
    ) -> Result<CommittedResearchProviderReplacement, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if !current.admission.revocation_drained() {
            return Err(ResearchIngestCompositionError::RuntimeGenerationStillCallable);
        }
        drop(authority);
        let candidate_capability = self
            .candidate_capability
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        self.completed = true;
        Ok(CommittedResearchProviderReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile: self.profile.clone(),
            token: self.token,
            expected: self.expected.clone(),
            candidate: self.candidate.clone(),
            candidate_capability: Some(candidate_capability),
            candidate_admission: self.candidate_admission.clone(),
            completed: false,
        })
    }
}

impl std::fmt::Debug for PreparedResearchProviderReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedResearchProviderReplacement")
            .field("profile", &self.profile)
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

impl Drop for PreparedResearchProviderReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        let Ok(mut authority) = self.coordinator.authority.lock() else {
            tracing::error!(
                profile = self.profile.as_str(),
                "provider replacement admission could not be released"
            );
            return;
        };
        if authority.pending_replacements.get(&self.profile) == Some(&self.token) {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Token-bound candidate retained pending until higher-level durable authority is exact.
struct CommittedResearchProviderReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_capability: Option<super::RegisteredSourceCapability>,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl CommittedResearchProviderReplacement {
    /// Cancels the pending candidate and re-admits the exact retained predecessor.
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor_admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        current.admission = predecessor_admission;
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    /// Publishes and activates the candidate after higher-level durable authority is exact.
    fn finalize(
        &mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let super::CoordinatorAuthority {
            registry,
            sources,
            publication_sources: _,
            pending_replacements,
            selections: _,
            alpaca_historical: _,
            filing_taxonomy_sources: _,
        } = &mut *authority;
        let current = sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&self.expected)
            || current.metadata != self.expected.metadata
            || current.rights != self.expected.rights
            || current.registration.source_id() != current.metadata.source_id()
            || current.registration.revision() != current.metadata.revision()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        // Preserve the original capability if timestamp or metadata publication fails, while
        // still rejecting a missing candidate before changing the registered metadata.
        self.candidate_capability
            .as_ref()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        let replacement_registration = if current.metadata == self.candidate.metadata {
            None
        } else {
            let registered_at = super::system_timestamp()
                .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
            Some(
                registry
                    .as_mut()
                    .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                    .replace_metadata(
                        &current.registration,
                        self.candidate.metadata.clone(),
                        registered_at,
                    )?,
            )
        };
        // No await or candidate mutation separates the presence check from this exact move.
        let candidate_capability = self
            .candidate_capability
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        let super::RegisteredSourceCapability { erased, typed } = candidate_capability;
        current.source = erased;
        current.typed_capability = typed;
        current.metadata = self.candidate.metadata.clone();
        if let Some(registration) = replacement_registration {
            current.registration = Box::new(registration);
        }
        current.rights = self.candidate.rights.clone();
        current.generation = Some(self.candidate.clone());
        current.admission = self.candidate_admission.clone();
        let removed = pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.candidate_admission.activate_pending();
        self.completed = true;
        Ok(self.candidate.clone())
    }
}

impl std::fmt::Debug for CommittedResearchProviderReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommittedResearchProviderReplacement")
            .field("profile", &self.profile)
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

impl Drop for CommittedResearchProviderReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        let Ok(mut authority) = self.coordinator.authority.lock() else {
            tracing::error!(
                profile = self.profile.as_str(),
                "committed provider replacement could not be failed closed"
            );
            return;
        };
        if authority.pending_replacements.get(&self.profile) == Some(&self.token) {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Pending exact-generation replacement for a specialized publication-only provider.
struct PreparedResearchProviderPublicationReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_rights: ResearchRightsAuthority,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl PreparedResearchProviderPublicationReplacement {
    async fn revoke_predecessor(&mut self) -> Result<(), ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor = {
            let authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if self.coordinator.lifecycle.shutdown_token().is_cancelled()
                || authority.pending_replacements.get(&self.profile) != Some(&self.token)
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            let current = authority
                .publication_sources
                .get(&self.profile)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation != self.expected
                || current.metadata != *self.expected.metadata()
                || current.rights != self.expected.rights
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            current.admission.clone()
        };
        self.coordinator
            .research
            .application_changes()
            .record(market_squawk_services::ServiceDomain::Source);
        predecessor.revoke_and_drain().await;
        Ok(())
    }

    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected || current.metadata != *self.expected.metadata() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        if current.admission.revocation_drained() {
            current.admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        } else {
            current.admission.ensure_live()?;
        }
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    fn commit(
        &mut self,
    ) -> Result<CommittedResearchProviderPublicationReplacement, ResearchIngestCompositionError>
    {
        self.candidate_admission.ensure_pending()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected
            || current.metadata != *self.expected.metadata()
            || !current.admission.revocation_drained()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        drop(authority);
        self.completed = true;
        Ok(CommittedResearchProviderPublicationReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile: self.profile.clone(),
            token: self.token,
            expected: self.expected.clone(),
            candidate: self.candidate.clone(),
            candidate_rights: self.candidate_rights.clone(),
            candidate_admission: self.candidate_admission.clone(),
            completed: false,
        })
    }
}

impl Drop for PreparedResearchProviderPublicationReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        if let Ok(mut authority) = self.coordinator.authority.lock()
            && authority.pending_replacements.get(&self.profile) == Some(&self.token)
        {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Committed, still-non-callable specialized provider candidate.
struct CommittedResearchProviderPublicationReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    profile: SourceIdentifier,
    token: Uuid,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    candidate_rights: ResearchRightsAuthority,
    candidate_admission: ResearchProviderAdmission,
    completed: bool,
}

impl CommittedResearchProviderPublicationReplacement {
    fn rollback(
        mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let predecessor_admission = ResearchProviderAdmission::new(Some(&self.expected))?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let current = authority
            .publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected || !current.admission.revocation_drained() {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        self.candidate_admission.revoke();
        current.admission = predecessor_admission;
        let removed = authority.pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.completed = true;
        Ok(self.expected.clone())
    }

    fn finalize(
        &mut self,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.candidate_admission.ensure_pending()?;
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        if !self.candidate.metadata().is_effective_at(registered_at) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.pending_replacements.get(&self.profile) != Some(&self.token) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        let super::CoordinatorAuthority {
            registry,
            sources: _,
            publication_sources,
            pending_replacements,
            selections: _,
            alpaca_historical: _,
            filing_taxonomy_sources: _,
        } = &mut *authority;
        let current = publication_sources
            .get_mut(&self.profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != self.expected
            || !current.admission.revocation_drained()
            || current.registration.source_id() != self.expected.metadata().source_id()
            || current.registration.revision() != self.expected.metadata().revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if current.metadata != *self.candidate.metadata() {
            current.registration = Box::new(
                registry
                    .as_mut()
                    .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                    .replace_metadata(
                        current.registration.as_ref(),
                        self.candidate.metadata().clone(),
                        registered_at,
                    )?,
            );
            current.registered_at = registered_at;
        }
        current.metadata = self.candidate.metadata().clone();
        current.rights = self.candidate_rights.clone();
        current.generation = self.candidate.clone();
        current.admission = self.candidate_admission.clone();
        let removed = pending_replacements.remove(&self.profile);
        debug_assert_eq!(removed, Some(self.token));
        self.candidate_admission.activate_pending();
        self.completed = true;
        Ok(self.candidate.clone())
    }
}

impl Drop for CommittedResearchProviderPublicationReplacement {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.candidate_admission.revoke();
        if let Ok(mut authority) = self.coordinator.authority.lock()
            && authority.pending_replacements.get(&self.profile) == Some(&self.token)
        {
            let _removed = authority.pending_replacements.remove(&self.profile);
        }
    }
}

/// Non-cloneable mutation authority minted with one exact production coordinator.
///
/// The coordinator itself exposes only read-only provider-generation inspection. Every provider
/// source-map mutation requires this value, which is moved into the application-owned adapter
/// activation boundary at composition.
pub(crate) struct ResearchProviderRuntimeMutationAuthority {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
}

/// Unforgeable proof that SEC live-fund composition came from the locked coordinator registry.
pub(super) struct SecLiveFundCoordinatorSeal {
    _private: (),
}

/// Opaque token-bound replacement whose transitions are available only through its minting
/// [`ResearchProviderRuntimeMutationAuthority`].
pub(crate) struct ResearchProviderRuntimeReplacement {
    coordinator: Arc<ProductionResearchIngestCoordinator>,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    state: Option<ResearchProviderRuntimeReplacementState>,
}

enum ResearchProviderRuntimeReplacementState {
    Prepared(PreparedResearchProviderReplacement),
    Committed(CommittedResearchProviderReplacement),
    PreparedPublication(PreparedResearchProviderPublicationReplacement),
    CommittedPublication(CommittedResearchProviderPublicationReplacement),
}

impl ResearchProviderRuntimeMutationAuthority {
    /// Includes revoked entries: revocation alone can still retain a credential-bearing adapter.
    pub(crate) fn retained_credential_generations(
        &self,
    ) -> Result<Vec<ResearchProviderRuntimeGeneration>, ResearchIngestCompositionError> {
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if !authority.pending_replacements.is_empty() {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        Ok(authority
            .sources
            .values()
            .filter_map(|source| source.generation.as_ref())
            .chain(
                authority
                    .publication_sources
                    .values()
                    .map(|source| &source.generation),
            )
            .filter(|generation| generation.secret_reference().is_some())
            .cloned()
            .collect())
    }

    /// Releases drained process adapters while retaining their durable catalog history.
    pub(crate) fn release_suspended_provider_generation(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority
            .pending_replacements
            .contains_key(expected.profile())
        {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let super::CoordinatorAuthority {
            registry,
            sources,
            publication_sources,
            ..
        } = &mut *authority;
        let registration = if let Some(current) = sources.get(expected.profile()) {
            if current.generation.as_ref() != Some(expected)
                || !current.admission.revocation_drained()
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            let registry_owners = match &current.typed_capability {
                super::RegisteredTypedSourceCapability::None => 1,
                super::RegisteredTypedSourceCapability::BoardFullHistory(_)
                | super::RegisteredTypedSourceCapability::TreasuryAllHistory(_)
                | super::RegisteredTypedSourceCapability::BeaRegional(_) => 2,
            };
            if Arc::strong_count(&current.source) != registry_owners {
                return Err(ResearchIngestCompositionError::AuthorityUnavailable);
            }
            current.registration.as_ref()
        } else if let Some(current) = publication_sources.get(expected.profile()) {
            if &current.generation != expected || !current.admission.revocation_drained() {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.registration.as_ref()
        } else {
            return Err(ResearchIngestCompositionError::RuntimeGenerationUnavailable);
        };
        registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .release_process_registration_exact(registration)?;
        drop(sources.remove(expected.profile()));
        drop(publication_sources.remove(expected.profile()));
        Ok(())
    }

    pub(super) fn new(coordinator: Arc<ProductionResearchIngestCoordinator>) -> Self {
        Self { coordinator }
    }

    fn require_bound(
        &self,
        transaction: &ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        if Arc::ptr_eq(&self.coordinator, &transaction.coordinator) {
            Ok(())
        } else {
            Err(ResearchIngestCompositionError::StaleRuntimeGeneration)
        }
    }

    /// Atomically registers one exact SEC source and composes its sole live-fund authority.
    ///
    /// No callable coordinator entry is published until registry registration, extraction
    /// authority, and application bridge composition all succeed. A post-registration failure
    /// releases only that exact process registration while preserving its clean-resumable durable
    /// metadata history.
    pub(crate) fn register_sec_live_fund_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_sec::SecEdgarSource>,
        rights: ResearchRightsAuthority,
        identity_authority_source_id: market_squawk_domain::SourceId,
    ) -> Result<super::sec_live::SecLiveFundSource, super::sec_live::SecLiveFundApplicationError>
    {
        let metadata =
            market_squawk_sources::SourceMetadataProvider::metadata(source.as_ref()).clone();
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.profile().as_str() != SEC_EDGAR_PROFILE_ID
            || metadata.source_id().as_str() != SEC_EDGAR_SOURCE_ID
            || metadata != *generation.metadata()
            || rights != generation.rights
            || rights.source_id() != metadata.source_id()
            || identity_authority_source_id != *metadata.source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration.into());
        }
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        let admission = ResearchProviderAdmission::new(Some(&generation))?;
        let generation_digest = generation.generation_digest()?;
        let source_erased: Arc<dyn ManagedResearchExtractionSource> = source.clone();
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown.into());
        }
        if authority.sources.contains_key(generation.profile()) {
            return Err(ResearchIngestCompositionError::DuplicateProfile.into());
        }
        // Complete filings retain the independent publishers of their taxonomy components.
        // Admit those descriptor-bound revisions before SEC can publish the mixed-source graph.
        // Repeated SEC activation reuses the exact handles and preserves source revocation.
        {
            let super::CoordinatorAuthority {
                registry,
                filing_taxonomy_sources,
                ..
            } = &mut *authority;
            let registry = registry
                .as_mut()
                .ok_or(ResearchIngestCompositionError::ShuttingDown)?;
            let dependency_count = market_squawk_sources::FILING_TAXONOMY_SOURCE_AUTHORITIES
                .len()
                .saturating_sub(1);
            filing_taxonomy_sources
                .try_reserve_exact(dependency_count.saturating_sub(filing_taxonomy_sources.len()))
                .map_err(|_| ResearchIngestCompositionError::AuthorityUnavailable)?;
            for publisher in market_squawk_sources::FILING_TAXONOMY_SOURCE_AUTHORITIES {
                if publisher.source_id() == SEC_EDGAR_SOURCE_ID {
                    continue;
                }
                let dependency = publisher
                    .dependency_source_metadata()
                    .map_err(|_| ResearchIngestCompositionError::InvalidRuntimeGeneration)?;
                if let Some(existing) = filing_taxonomy_sources
                    .iter()
                    .find(|registered| registered.source_id() == dependency.source_id())
                {
                    if registry
                        .validate_registered(existing, registered_at)
                        .map_err(ResearchIngestCompositionError::from)?
                        != &dependency
                    {
                        return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration.into());
                    }
                } else {
                    filing_taxonomy_sources.push(
                        registry
                            .register_or_resume_exact(dependency, registered_at)
                            .map_err(ResearchIngestCompositionError::from)?,
                    );
                }
            }
        }
        let (registration, operation) = {
            let registry = authority
                .registry
                .as_mut()
                .ok_or(ResearchIngestCompositionError::ShuttingDown)?;
            let registration = registry
                .register_or_resume_exact(metadata.clone(), registered_at)
                .map_err(ResearchIngestCompositionError::from)?;
            let composition = (|| {
                let extraction = registry
                    .extraction_authority(&registration, source.as_ref())
                    .map_err(ResearchIngestCompositionError::from)?;
                let operation = super::sec_live::SecLiveFundSource::from_coordinator(
                    SecLiveFundCoordinatorSeal { _private: () },
                    Arc::clone(&source),
                    extraction,
                    generation.clone(),
                    admission.clone(),
                    rights.clone(),
                    Arc::clone(&self.coordinator.research),
                    identity_authority_source_id,
                )?;
                admission.ensure_live()?;
                if admission.generation_digest != Some(generation_digest)
                    || source_erased.metadata() != &metadata
                {
                    return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration.into());
                }
                Ok(operation)
            })();
            match composition {
                Ok(operation) => (registration, operation),
                Err(error) => {
                    registry
                        .release_process_registration_exact(&registration)
                        .map_err(ResearchIngestCompositionError::from)?;
                    return Err(error);
                }
            }
        };
        authority.sources.insert(
            generation.profile().clone(),
            super::RegisteredExtractionSource {
                source: source_erased,
                typed_capability: super::RegisteredTypedSourceCapability::None,
                metadata,
                registration: Box::new(registration),
                rights,
                generation: Some(generation),
                admission,
            },
        );
        Ok(operation)
    }

    /// Atomically binds one exact registered Schwab publication generation to its sole REST quote
    /// publisher and provider-neutral durable-read channel.
    #[allow(
        clippy::too_many_arguments,
        reason = "generation, OAuth, analytical dataset, and operation lifetime remain explicit"
    )]
    pub(crate) fn bind_schwab_rest_quote_publication_package(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        oauth: SchwabOAuthReceiptCurrentness,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
        analytical_dataset: DatasetId,
        operation_timeout: Duration,
    ) -> Result<SchwabRestQuotePublicationPackage, SchwabMarketPublicationError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.profile().as_str() != market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
            || oauth.session_id() != generation.session_id()
            || analytical_dataset.as_str() != SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET
            || operation_timeout.is_zero()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let package = {
            let closure = self.bind_schwab_publication_closure(generation, oauth, oauth_receipt)?;
            let generation_authority =
                closure.bind_rest_quote_sink(operation_timeout, analytical_dataset.clone())?;
            let point_in_time = MarketEventPointInTimeSelector::new(
                Arc::clone(&self.coordinator.research),
                analytical_dataset,
                generation.metadata().source_id().clone(),
            );
            let (durable_writer, durable_read) = MarketEventDurableRead::channel(point_in_time);
            SchwabRestQuotePublicationPackage {
                generation: generation_authority,
                durable_writer,
                durable_read,
            }
        };
        Ok(package)
    }

    /// Binds the exact registered MarketCalendar family, original OAuth receipt.
    /// The returned owner seals native outcomes and publishes through ordinary immutable research manifests.
    #[allow(
        clippy::too_many_arguments,
        reason = "generation and original OAuth remain explicit"
    )]
    pub(crate) fn bind_schwab_market_hours_publication_package(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        oauth: SchwabOAuthReceiptCurrentness,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<
        super::schwab_market::SchwabMarketHoursGenerationAuthority,
        SchwabMarketPublicationError,
    > {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.profile().as_str() != super::schwab_market::SCHWAB_MARKET_HOURS_PROFILE
            || generation.metadata().source_id().as_str()
                != super::schwab_market::SCHWAB_MARKET_HOURS_SOURCE
            || generation.metadata().coverage().domain()
                != market_squawk_sources::CoverageDomain::MarketCalendar
            || oauth.session_id() != generation.session_id()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let closure = self.bind_schwab_publication_closure(generation, oauth, oauth_receipt)?;
        super::schwab_market::SchwabMarketHoursGenerationAuthority::try_new(closure)
    }

    fn bind_schwab_publication_closure(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        oauth: SchwabOAuthReceiptCurrentness,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<Arc<SchwabMarketPublicationClosure>, SchwabMarketPublicationError> {
        validate_schwab_oauth_binding(generation, &oauth, oauth_receipt)?;
        let generation_digest = generation.generation_digest()?;
        let authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown.into());
        }
        let current = authority
            .publication_sources
            .get(generation.profile())
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != *generation
            || current.metadata != *generation.metadata()
            || current.rights != generation.rights
            || current.registration.source_id() != generation.metadata().source_id()
            || current.registration.revision() != generation.metadata().revision()
            || current.admission.generation_digest != Some(generation_digest)
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration.into());
        }
        current.admission.ensure_live()?;
        oauth
            .validate_current_receipt(oauth_receipt)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let admission = Arc::new(SchwabCompositeMarketRuntimeAdmission {
            generation_digest,
            admission: current.admission.clone(),
            oauth,
            oauth_receipt,
        });
        admission.ensure_exact_current()?;
        let closure = Arc::new(SchwabMarketPublicationClosure::try_new(
            Arc::clone(&self.coordinator.research),
            generation.clone(),
            current.rights.clone(),
            admission,
        )?);
        Ok(closure)
    }

    pub(crate) fn bind_schwab_streamer_publication_package(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        oauth: SchwabOAuthReceiptCurrentness,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<SchwabStreamerPublicationPackage, SchwabMarketPublicationError> {
        if generation.profile().as_str() != super::schwab_market::SCHWAB_STREAMER_PROFILE
            || generation.metadata().source_id().as_str()
                != super::schwab_market::SCHWAB_STREAMER_SOURCE
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let closure = self.bind_schwab_publication_closure(generation, oauth, oauth_receipt)?;
        let authority =
            Arc::new(super::schwab_market::SchwabStreamerGenerationAuthority::try_new(closure)?);
        let selector = MarketEventPointInTimeSelector::new(
            Arc::clone(&self.coordinator.research),
            DatasetId::try_from(SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET)
                .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?,
            generation.metadata().source_id().clone(),
        );
        let (durable_writer, durable_read) = MarketEventDurableRead::channel(selector);
        Ok(SchwabStreamerPublicationPackage {
            authority,
            durable_writer,
            durable_read,
        })
    }

    /// Registers one provider adapter bound to an exact onboarding/runtime generation.
    pub(crate) fn register_provider_publication_generation(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || generation.metadata().source_id() != rights.source_id()
            || rights != generation.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let profile = generation.profile().clone();
        let registered_at = super::system_timestamp()
            .map_err(|_error| ResearchIngestCompositionError::TrustedTimeUnavailable)?;
        if !generation.metadata().is_effective_at(registered_at) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
            || authority.sources.contains_key(&profile)
            || authority.pending_replacements.contains_key(&profile)
        {
            return Err(ResearchIngestCompositionError::DuplicateProfile);
        }
        let super::CoordinatorAuthority {
            registry,
            sources: _,
            publication_sources,
            pending_replacements: _,
            selections: _,
            alpaca_historical: _,
            filing_taxonomy_sources: _,
        } = &mut *authority;
        if let Some(current) = publication_sources.get_mut(&profile) {
            if current.generation == generation {
                if current.metadata != *generation.metadata()
                    || current.rights != rights
                    || current.registration.source_id() != generation.metadata().source_id()
                    || current.registration.revision() != generation.metadata().revision()
                    || (!current.admission.revocation_drained()
                        && current.admission.ensure_live().is_err())
                {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                if current.admission.revocation_drained() {
                    current.admission = ResearchProviderAdmission::new(Some(&generation))?;
                }
                return Ok(generation);
            }
            if !current.admission.revocation_drained()
                || !generation.is_exact_successor_of(&current.generation)?
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            if current.metadata != *generation.metadata() {
                current.registration = Box::new(
                    registry
                        .as_mut()
                        .ok_or(ResearchIngestCompositionError::ShuttingDown)?
                        .replace_metadata(
                            current.registration.as_ref(),
                            generation.metadata().clone(),
                            registered_at,
                        )?,
                );
            }
            current.metadata = generation.metadata().clone();
            current.registered_at = registered_at;
            current.rights = rights;
            current.generation = generation.clone();
            current.admission = ResearchProviderAdmission::new(Some(&generation))?;
            return Ok(generation);
        }
        let registration = registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .register_or_resume_exact(generation.metadata().clone(), registered_at)?;
        let admission = ResearchProviderAdmission::new(Some(&generation))?;
        publication_sources.insert(
            profile,
            super::RegisteredPublicationSource {
                metadata: generation.metadata().clone(),
                registered_at,
                registration: Box::new(registration),
                rights,
                generation: generation.clone(),
                admission,
            },
        );
        Ok(generation)
    }

    /// Registers one provider adapter bound to an exact onboarding/runtime generation.
    pub(crate) fn register_provider_source<S>(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        if source.metadata() != generation.metadata()
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        self.coordinator.register_source_inner(
            generation.profile().clone(),
            source,
            rights,
            Some(generation),
        )
    }

    /// Retains the same Board allocation for dashboard and source-owned complete-file operations.
    pub(crate) fn register_board_provider_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_federal_reserve::BoardSource>,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError> {
        if source.metadata() != generation.metadata()
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        self.coordinator.register_source_capability_inner(
            generation.profile().clone(),
            source.metadata().clone(),
            super::RegisteredSourceCapability::board(source),
            rights,
            Some(generation),
        )
    }

    /// Registers the same BEA allocation for neutral discovery and sealed-token ingestion.
    pub(crate) fn register_bea_provider_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: super::BeaRegisteredSource,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError> {
        if source.metadata() != generation.metadata()
            || source.generation() != &generation
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        self.coordinator.register_source_capability_inner(
            generation.profile().clone(),
            source.metadata().clone(),
            super::RegisteredSourceCapability::bea(source),
            rights,
            Some(generation),
        )
    }

    /// Registers one Treasury adapter while retaining its exact typed allocation beside the
    /// erased extraction source.
    pub(crate) fn register_treasury_provider_source(
        &self,
        generation: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_treasury::TreasurySource>,
        rights: ResearchRightsAuthority,
    ) -> Result<(), ResearchIngestCompositionError> {
        if source.metadata() != generation.metadata()
            || rights != generation.rights
            || &rights.source_id != source.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let metadata = source.metadata().clone();
        self.coordinator.register_source_capability_inner(
            generation.profile().clone(),
            metadata,
            super::RegisteredSourceCapability::treasury(source),
            rights,
            Some(generation),
        )
    }
}

impl ProductionResearchIngestCoordinator {
    /// Acquires the sole non-forgeable crypto canonical-publication authority for one exact active
    /// runtime generation.
    pub(crate) async fn acquire_crypto_market_publication_authority(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        caller: CancellationToken,
        deadline: Instant,
        analytical_dataset: DatasetId,
    ) -> Result<CryptoMarketPublicationAuthority, ResearchIngestCompositionError> {
        if analytical_dataset.as_str() != "market_squawk.market_events" {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let operation = self
            .acquire_provider_publication_operation(generation, caller, deadline)
            .await?;
        let publication = Arc::new(
            CryptoMarketPublicationClosure::try_new(
                self.research.clone(),
                operation.source().clone(),
                operation.rights().clone(),
                operation.source_registered_at(),
            )
            .map_err(|_error| ResearchIngestCompositionError::InvalidRuntimeGeneration)?,
        );
        let precommit = operation.precommit_authority();
        let authority = CryptoMarketPublicationAuthority {
            operation,
            publication,
            analytical_dataset,
            precommit,
        };
        authority.validate_precommit()?;
        Ok(authority)
    }

    /// Acquires one exact specialized-provider admission and retains its cancellation and
    /// publication lease across network, raw sealing, and final commit.
    pub(crate) async fn acquire_provider_publication_operation(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        caller: CancellationToken,
        deadline: Instant,
    ) -> Result<ResearchProviderPublicationOperation, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() || caller.is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let generation_digest = generation.generation_digest()?;
        let (source, rights, source_registered_at, admission) = {
            let authority = self
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            let current = authority
                .publication_sources
                .get(generation.profile())
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation != *generation
                || current.metadata != *generation.metadata()
                || current.rights != generation.rights
                || current.registration.source_id() != generation.metadata().source_id()
                || current.registration.revision() != generation.metadata().revision()
                || current.admission.generation_digest != Some(generation_digest)
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.ensure_live()?;
            (
                current.metadata.clone(),
                current.rights.clone(),
                current.registered_at,
                current.admission.clone(),
            )
        };
        let cancellation = CancellationToken::new();
        let shutdown = self.lifecycle.shutdown_token().clone();
        let revoked = admission.cancellation().clone();
        let cancellation_cause = Arc::new(AtomicU8::new(0));
        let watcher = spawn_publication_cancellation_watcher(
            cancellation.clone(),
            Arc::clone(&cancellation_cause),
            caller.clone(),
            shutdown.clone(),
            revoked.clone(),
            deadline,
        );
        let lease = admission.acquire_publication_lease();
        tokio::pin!(lease);
        let publication = tokio::select! {
            biased;
            () = caller.cancelled() => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            () = shutdown.cancelled() => Err(ResearchIngestCompositionError::ShuttingDown),
            () = revoked.cancelled() => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            () = tokio::time::sleep_until(deadline.into()) => Err(ResearchIngestCompositionError::StaleRuntimeGeneration),
            result = lease.as_mut() => result,
        };
        let publication = match publication {
            Ok(publication) => Arc::new(publication),
            Err(error) => {
                cancellation.cancel();
                watcher.abort();
                return Err(error);
            }
        };
        let operation = ResearchProviderPublicationOperation {
            generation: generation.clone(),
            source,
            rights,
            source_registered_at,
            publication,
            cancellation,
            cancellation_cause,
            upstream_cancellation: [caller, shutdown, revoked],
            watcher,
        };
        operation.validate_precommit()?;
        Ok(operation)
    }

    /// Returns one coherent, nonblocking count of callable provider runtime generations.
    pub fn active_provider_runtime_count(&self) -> Result<usize, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .try_lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let extraction = authority
            .sources
            .values()
            .try_fold(0_usize, |count, source| {
                if source.generation.is_some() && source.admission.ensure_live().is_ok() {
                    count
                        .checked_add(1)
                        .ok_or(ResearchIngestCompositionError::AuthorityUnavailable)
                } else {
                    Ok(count)
                }
            })?;
        authority
            .publication_sources
            .values()
            .try_fold(extraction, |count, source| {
                if source.admission.ensure_live().is_ok() {
                    count
                        .checked_add(1)
                        .ok_or(ResearchIngestCompositionError::AuthorityUnavailable)
                } else {
                    Ok(count)
                }
            })
    }

    /// Returns the exact callable generation currently published for one provider profile.
    pub fn provider_runtime_generation(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<ResearchProviderRuntimeGeneration>, ResearchIngestCompositionError> {
        if self.lifecycle.shutdown_token().is_cancelled() {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if let Some(source) = authority.sources.get(profile) {
            if source.admission.ensure_live().is_err() {
                return Ok(None);
            }
            return source
                .generation
                .clone()
                .map(Some)
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable);
        }
        let Some(source) = authority.publication_sources.get(profile) else {
            return Ok(None);
        };
        if source.admission.ensure_live().is_err() {
            return Ok(None);
        }
        Ok(Some(source.generation.clone()))
    }
}

impl ResearchProviderRuntimeMutationAuthority {
    /// Prepares an exact replacement for a specialized provider that publishes but does not expose
    /// generic extraction authority.
    pub(crate) fn prepare_provider_publication_replacement(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || !candidate.is_exact_successor_of(&expected)?
            || rights != candidate.rights
            || rights.source_id() != candidate.metadata().source_id()
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_admission = ResearchProviderAdmission::new_pending(&candidate)?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if authority.registry.is_none() || authority.sources.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        if authority.pending_replacements.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let current = authority
            .publication_sources
            .get(&profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation != expected
            || current.metadata != *expected.metadata()
            || current.rights != expected.rights
            || current.admission.ensure_live().is_err()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        authority
            .pending_replacements
            .insert(profile.clone(), token);
        let prepared = PreparedResearchProviderPublicationReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile,
            token,
            expected: expected.clone(),
            candidate: candidate.clone(),
            candidate_rights: rights,
            candidate_admission,
            completed: false,
        };
        Ok(ResearchProviderRuntimeReplacement {
            coordinator: Arc::clone(&self.coordinator),
            expected,
            candidate,
            state: Some(ResearchProviderRuntimeReplacementState::PreparedPublication(prepared)),
        })
    }

    /// Prepares an exact expected-old to exact-new adapter replacement without publishing it.
    pub(crate) fn prepare_provider_replacement<S>(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError>
    where
        S: ManagedResearchExtractionSource,
    {
        self.prepare_provider_replacement_capability(
            expected,
            candidate,
            super::RegisteredSourceCapability::erased(source),
            rights,
        )
    }

    /// Prepares an exact Treasury successor while retaining the candidate's one typed allocation.
    pub(crate) fn prepare_treasury_provider_replacement(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: Arc<market_squawk_adapter_treasury::TreasurySource>,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        self.prepare_provider_replacement_capability(
            expected,
            candidate,
            super::RegisteredSourceCapability::treasury(source),
            rights,
        )
    }

    /// Preserves the exact BEA successor's typed allocation in the existing replacement owner.
    pub(crate) fn prepare_bea_provider_replacement(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: super::BeaRegisteredSource,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        if source.generation() != &candidate {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        self.prepare_provider_replacement_capability(
            expected,
            candidate,
            super::RegisteredSourceCapability::bea(source),
            rights,
        )
    }

    fn prepare_provider_replacement_capability(
        &self,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        candidate_capability: super::RegisteredSourceCapability,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ResearchIngestCompositionError> {
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || !candidate.is_exact_successor_of(&expected)?
            || candidate_capability.erased.metadata() != candidate.metadata()
            || rights != candidate.rights
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        let profile = candidate.profile().clone();
        let token = Uuid::new_v4();
        let candidate_admission = ResearchProviderAdmission::new_pending(&candidate)?;
        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        if self.coordinator.lifecycle.shutdown_token().is_cancelled()
            || authority.registry.is_none()
        {
            return Err(ResearchIngestCompositionError::ShuttingDown);
        }
        if authority.pending_replacements.contains_key(&profile) {
            return Err(ResearchIngestCompositionError::ReplacementInProgress);
        }
        let current = authority
            .sources
            .get(&profile)
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(&expected)
            || current.metadata != expected.metadata
            || current.rights != expected.rights
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        if !current
            .typed_capability
            .same_kind(&candidate_capability.typed)
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeReplacement);
        }
        authority
            .pending_replacements
            .insert(profile.clone(), token);
        let prepared = PreparedResearchProviderReplacement {
            coordinator: Arc::clone(&self.coordinator),
            profile,
            token,
            expected: expected.clone(),
            candidate: candidate.clone(),
            candidate_capability: Some(candidate_capability),
            candidate_admission,
            completed: false,
        };
        Ok(ResearchProviderRuntimeReplacement {
            coordinator: Arc::clone(&self.coordinator),
            expected,
            candidate,
            state: Some(ResearchProviderRuntimeReplacementState::Prepared(prepared)),
        })
    }

    pub(crate) async fn revoke_predecessor(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        match transaction.state.as_mut() {
            Some(ResearchProviderRuntimeReplacementState::Prepared(prepared)) => {
                prepared.revoke_predecessor().await
            }
            Some(ResearchProviderRuntimeReplacementState::PreparedPublication(prepared)) => {
                prepared.revoke_predecessor().await
            }
            Some(
                ResearchProviderRuntimeReplacementState::Committed(_)
                | ResearchProviderRuntimeReplacementState::CommittedPublication(_),
            )
            | None => Err(ResearchIngestCompositionError::InvalidRuntimeReplacement),
        }
    }

    pub(crate) fn commit(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<(), ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        let state = transaction
            .state
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?;
        match state {
            ResearchProviderRuntimeReplacementState::Prepared(mut prepared) => {
                match prepared.commit() {
                    Ok(committed) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::Committed(committed),
                        );
                        Ok(())
                    }
                    Err(error) => {
                        transaction.state =
                            Some(ResearchProviderRuntimeReplacementState::Prepared(prepared));
                        Err(error)
                    }
                }
            }
            ResearchProviderRuntimeReplacementState::Committed(committed) => {
                transaction.state = Some(ResearchProviderRuntimeReplacementState::Committed(
                    committed,
                ));
                Ok(())
            }
            ResearchProviderRuntimeReplacementState::PreparedPublication(mut prepared) => {
                match prepared.commit() {
                    Ok(committed) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::CommittedPublication(
                                committed,
                            ),
                        );
                        Ok(())
                    }
                    Err(error) => {
                        transaction.state = Some(
                            ResearchProviderRuntimeReplacementState::PreparedPublication(prepared),
                        );
                        Err(error)
                    }
                }
            }
            ResearchProviderRuntimeReplacementState::CommittedPublication(committed) => {
                transaction.state =
                    Some(ResearchProviderRuntimeReplacementState::CommittedPublication(committed));
                Ok(())
            }
        }
    }

    pub(crate) fn rollback(
        &self,
        mut transaction: ResearchProviderRuntimeReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.require_bound(&transaction)?;
        match transaction
            .state
            .take()
            .ok_or(ResearchIngestCompositionError::InvalidRuntimeReplacement)?
        {
            ResearchProviderRuntimeReplacementState::Prepared(prepared) => prepared.rollback(),
            ResearchProviderRuntimeReplacementState::Committed(committed) => committed.rollback(),
            ResearchProviderRuntimeReplacementState::PreparedPublication(prepared) => {
                prepared.rollback()
            }
            ResearchProviderRuntimeReplacementState::CommittedPublication(committed) => {
                committed.rollback()
            }
        }
    }

    pub(crate) fn finalize(
        &self,
        transaction: &mut ResearchProviderRuntimeReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ResearchIngestCompositionError> {
        self.require_bound(transaction)?;
        match transaction.state.as_mut() {
            Some(ResearchProviderRuntimeReplacementState::Committed(committed)) => {
                committed.finalize()
            }
            Some(ResearchProviderRuntimeReplacementState::CommittedPublication(committed)) => {
                committed.finalize()
            }
            Some(
                ResearchProviderRuntimeReplacementState::Prepared(_)
                | ResearchProviderRuntimeReplacementState::PreparedPublication(_),
            )
            | None => Err(ResearchIngestCompositionError::InvalidRuntimeReplacement),
        }
    }

    /// Drains and releases one exact SEC generation without durably revoking its source history.
    pub(crate) async fn revoke_sec_provider_generation_and_release(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        if expected.profile().as_str() != SEC_EDGAR_PROFILE_ID
            || expected.metadata().source_id().as_str() != SEC_EDGAR_SOURCE_ID
        {
            return Err(ResearchIngestCompositionError::InvalidRuntimeGeneration);
        }
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            let current = authority
                .sources
                .get(expected.profile())
                .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
            if current.generation.as_ref() != Some(expected)
                || current.metadata != *expected.metadata()
                || current.registration.source_id() != expected.metadata().source_id()
                || current.registration.revision() != expected.metadata().revision()
            {
                return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
            }
            current.admission.revoke();
            let admission = current.admission.clone();
            authority.selections.revoke_profile(expected.profile());
            admission
        };
        // Revocation has taken effect even if draining later fails or is cancelled.
        self.coordinator
            .research
            .application_changes()
            .record(market_squawk_services::ServiceDomain::Source);
        admission.revoke_and_drain().await;

        let mut authority = self
            .coordinator
            .authority
            .lock()
            .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
        let super::CoordinatorAuthority {
            registry,
            sources,
            publication_sources: _,
            pending_replacements: _,
            selections: _,
            alpaca_historical: _,
            filing_taxonomy_sources: _,
        } = &mut *authority;
        let current = sources
            .get(expected.profile())
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if current.generation.as_ref() != Some(expected)
            || !current.admission.matches(&admission)
            || !current.admission.revocation_drained()
            || current.metadata != *expected.metadata()
            || current.registration.source_id() != expected.metadata().source_id()
            || current.registration.revision() != expected.metadata().revision()
        {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        registry
            .as_mut()
            .ok_or(ResearchIngestCompositionError::ShuttingDown)?
            .release_process_registration_exact(current.registration.as_ref())?;
        let removed = sources
            .remove(expected.profile())
            .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
        if removed.generation.as_ref() != Some(expected) || !removed.admission.matches(&admission) {
            return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
        }
        Ok(())
    }

    /// Revokes exactly one callable generation and every retained receipt minted from it.
    pub(crate) async fn revoke_provider_generation(
        &self,
        profile: &SourceIdentifier,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ResearchIngestCompositionError> {
        let admission = {
            let mut authority = self
                .coordinator
                .authority
                .lock()
                .map_err(|_error| ResearchIngestCompositionError::AuthorityUnavailable)?;
            if let Some(current) = authority.publication_sources.get(profile) {
                if &current.generation != expected {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                current.admission.revoke();
                current.admission.clone()
            } else {
                let current = authority
                    .sources
                    .get(profile)
                    .ok_or(ResearchIngestCompositionError::RuntimeGenerationUnavailable)?;
                if current.generation.as_ref() != Some(expected) {
                    return Err(ResearchIngestCompositionError::StaleRuntimeGeneration);
                }
                current.admission.revoke();
                let admission = current.admission.clone();
                authority.selections.revoke_profile(profile);
                admission
            }
        };
        // Revocation has taken effect even if draining later fails or is cancelled.
        self.coordinator
            .research
            .application_changes()
            .record(market_squawk_services::ServiceDomain::Source);
        admission.revoke_and_drain().await;
        super::treasury::drain_generation_replay(&self.coordinator, expected).await?;
        Ok(())
    }
}

impl ResearchProviderRuntimeReplacement {
    pub(crate) const fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        &self.expected
    }
}

impl std::fmt::Debug for ResearchProviderRuntimeMutationAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderRuntimeMutationAuthority")
            .field("coordinator", &"[SEALED]")
            .finish()
    }
}

impl std::fmt::Debug for ResearchProviderRuntimeReplacement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResearchProviderRuntimeReplacement")
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::path::Path;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use bytes::Bytes;
    use market_squawk_adapter_schwab::{
        AccessTokenAdmission, CallbackOutcome, OAuthCallback, ProtectedSchwabOAuthAuthority,
        ProviderIdentifier, QuoteField, QuoteRequest, RequestAdmission, ResponseHeaderEvidence,
        RestExecutionOutcome, RestTransportBounds, SchwabHttpWire, SchwabHttpWireRequest,
        SchwabHttpWireResponse, SchwabOAuthAuthorityConfiguration, SchwabOAuthInteraction,
        SchwabOAuthSecretPolicy, SchwabOAuthWire, SchwabOAuthWireError, SchwabOAuthWireRequest,
        SchwabOAuthWireResponse, SchwabRestExecutor, SchwabTransportTelemetry,
        TransientAccessToken,
    };
    use market_squawk_data::{
        CatalogConfig, CatalogResultLimits, ObjectStoreConfig, RightsBasis, SourceOperation,
    };
    use market_squawk_domain::{
        AssetClass, AssignmentVerification, AuthorizationBasis, ChecksumCapability, CoverageDelay,
        Currency, DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval,
        EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
        ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
        InstrumentId, IntegrityRule, MetadataRevision, ProviderChannel, ProviderIdentityEvidence,
        ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId, ProviderProduct,
        RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability,
        SnapshotApplicability, SourceId, SourceIdentifier, Ticker, Timestamp, VenueId,
    };
    use market_squawk_platform::{
        EncryptedFileSecretStore, LocalPaths, SecretCancellation, SecretGeneration,
        SecretInteractionPolicy, SecretKey, SecretOperationControl, SecretRef, SecretStore,
        SecretValue,
    };
    use market_squawk_sources::{
        AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
        ChecksumValidationProfile, CoverageTopology, EndpointPolicy, FreshnessPolicy,
        HistoricalCapability, InstrumentCoverage, LiveCoverageDeclaration, LiveCoverageRule,
        LiveProtocolProfile, NetworkAccessPolicy, ProviderBudgetPolicy, ProviderCapabilityRevision,
        ProviderNumericPolicy, SCHWAB_MARKET_DATA_SURFACE_ID, SemanticInterpretationProfile,
        SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
        SourceMetadataInput, SourceProtocolProfile,
    };

    use crate::ResearchService;
    use crate::application::market_runtime::{
        SchwabRestQuoteInstrumentBinding, SchwabRestQuoteProducer, SchwabRestQuoteSealFirstSink,
        SchwabRestQuoteSourceEvidence,
    };
    use crate::live_source::{
        SchwabQualifiedCurrent, SchwabRestQuoteCurrentBridge, SchwabRestQuoteCurrentPublication,
        SchwabRestQuoteCurrentRequest, SchwabRestQuoteCurrentUnavailable,
    };
    use crate::provider_activation::{
        MarketInstrumentReferenceBinding, MarketSubscriptionPriority,
        SchwabMarketDataAccountActivation,
    };
    use crate::provider_onboarding::SchwabOAuthMarketAuthority;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[tokio::test]
    async fn exact_generation_revocation_drains_the_publication_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let session = Uuid::new_v4();
        let (oauth, _, reference) =
            scripted_market_authority(directory.path().join("renewal"), session, 1_800, 60, 0)
                .await?;
        let (_, epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&oauth).await?;
        epoch.validate_current(epoch.receipt())?;
        let source = SourceId::try_from("schwab-trader-api")?;
        let metadata = quote_metadata(
            source.clone(),
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            ProviderProduct::new(SourceIdentifier::try_from("schwab-rest")?),
            ProviderChannel::new(SourceIdentifier::try_from("schwab-rest-quotes")?),
        )?;
        let rights = ResearchRightsAuthority::try_new_source_wide(
            source,
            RightsBasis::reviewed_terms("https://developer.schwab.com/terms", digest(33))?,
            digest(32),
            None,
            vec![SourceOperation::Persist],
        )?;
        let prior = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(SCHWAB_MARKET_DATA_SURFACE_ID)?,
            session,
            ProviderCapabilityRevision::new(1)?,
            digest(31),
            Some(reference.generation()),
            Some(reference),
            timestamp_seconds(epoch.receipt().access_issued_at_unix_seconds())?,
            metadata,
            rights,
        )?;
        assert!(prior.runtime_verification.is_none());
        let mut renewed = prior.clone();
        renewed.authority_effective_at = prior
            .authority_effective_at
            .checked_add_nanos(1_000_000_000)?;
        assert!(!renewed.is_exact_successor_of(&prior)?);
        renewed.session_id = Uuid::new_v4();
        assert!(renewed.is_exact_successor_of(&prior)?);
        assert!(!prior.is_exact_successor_of(&renewed)?);

        for expected in [
            ProviderPublicationCancellationCause::Deadline,
            ProviderPublicationCancellationCause::Caller,
            ProviderPublicationCancellationCause::Shutdown,
            ProviderPublicationCancellationCause::Revoked,
        ] {
            let admission = ResearchProviderAdmission::new(Some(&prior))?;
            let publication = Arc::new(admission.acquire_publication_lease().await?);
            let caller = CancellationToken::new();
            let shutdown = CancellationToken::new();
            let revoked = admission.cancellation().clone();
            match expected {
                ProviderPublicationCancellationCause::Caller => caller.cancel(),
                ProviderPublicationCancellationCause::Shutdown => shutdown.cancel(),
                ProviderPublicationCancellationCause::Revoked => admission.revoke(),
                ProviderPublicationCancellationCause::Deadline => {}
            }
            let cancellation = CancellationToken::new();
            let cancellation_cause = Arc::new(AtomicU8::new(0));
            // The deadline is ready for every case: explicit signals must win this race.
            let watcher = spawn_publication_cancellation_watcher(
                cancellation.clone(),
                Arc::clone(&cancellation_cause),
                caller.clone(),
                shutdown.clone(),
                revoked.clone(),
                Instant::now(),
            );
            let operation = ResearchProviderPublicationOperation {
                generation: prior.clone(),
                source: prior.metadata.clone(),
                rights: prior.rights.clone(),
                source_registered_at: prior.authority_effective_at,
                publication,
                cancellation,
                cancellation_cause,
                upstream_cancellation: [caller.clone(), shutdown.clone(), revoked.clone()],
                watcher,
            };
            tokio::time::timeout(Duration::from_secs(1), operation.cancellation().cancelled())
                .await?;
            assert_eq!(operation.cancellation_cause(), Some(expected));
            assert_eq!(
                operation.has_local_deadline_failure(),
                expected == ProviderPublicationCancellationCause::Deadline
            );
            if expected == ProviderPublicationCancellationCause::Deadline {
                assert!(!caller.is_cancelled());
                assert!(!shutdown.is_cancelled());
                assert!(admission.ensure_live().is_ok());
            }
            let revoking = admission.clone();
            let mut drain = tokio::spawn(async move {
                revoking.revoke_and_drain().await;
            });
            revoked.cancelled().await;
            assert!(!operation.has_local_deadline_failure());
            assert!(operation.publication.validate_precommit().is_err());
            // A timed-out cleanup waiter must retain the original publication lease/join.
            assert!(
                tokio::time::timeout(Duration::ZERO, &mut drain)
                    .await
                    .is_err()
            );
            assert!(!admission.revocation_drained());
            drop(operation);
            tokio::time::timeout(Duration::from_secs(1), drain).await??;
            assert!(admission.revocation_drained());
            // Recovery can mint a new admission for the same configured provider authority.
            let restored = ResearchProviderAdmission::new(Some(&prior))?;
            assert!(restored.admits_generation(&prior)?);
            assert!(
                restored
                    .acquire_publication_lease()
                    .await?
                    .validate_precommit()
                    .is_ok()
            );
        }
        let successor = ResearchProviderAdmission::new(Some(&renewed))?;
        assert!(successor.admits_generation(&renewed)?);
        assert!(!successor.admits_generation(&prior)?);
        assert!(
            successor
                .acquire_publication_lease()
                .await?
                .validate_precommit()
                .is_ok()
        );
        Ok(())
    }

    #[tokio::test]
    async fn schwab_quote_attempt_rejects_revoked_epoch_before_current_qualification() -> TestResult
    {
        let directory = tempfile::tempdir()?;
        let session_id = Uuid::new_v4();
        let (oauth, wire, secret_reference) = scripted_market_authority(
            directory.path().join("stable-oauth"),
            session_id,
            1_800,
            60,
            0,
        )
        .await?;
        let (token, epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&oauth).await?;
        let oauth_receipt = epoch.receipt();
        let (durable, evidence, binding, _, research) = quote_publication_fixture(
            directory.path(),
            session_id,
            secret_reference,
            oauth.clone(),
            oauth_receipt,
        )?;
        let generation = market_squawk_domain::ConnectionGeneration::new(1)?;
        // The real bridge records health after the sink entered publication. Selecting with
        // that earlier sink timestamp rejects this otherwise valid, newly qualified response.
        let (current, display) =
            quote_current_bridge(&research, evidence.metadata(), &binding, session_id).await?;
        let current = Arc::new(current);
        let positive_sink =
            SchwabRestQuoteSealFirstSink::new(Arc::clone(&durable), current.clone());
        let source_at = timestamp_seconds(oauth_receipt.access_issued_at_unix_seconds())?;
        let completed =
            executed_quote(token, oauth_receipt.access_issued_at_unix_seconds()).await?;
        let original_digest = completed.capture().receipt().body_sha256();
        let receipt = SchwabRestQuoteProducer::publish_test_completed_response(
            &positive_sink,
            completed,
            evidence.clone(),
            vec![binding.clone()],
            epoch,
            generation,
        )
        .await?;
        assert_eq!(receipt.published(), 1);
        let snapshots = display
            .snapshots_for_instrument(
                binding.instrument_id(),
                NonZeroUsize::MIN,
                crate::live_source::display_market::DisplayMarketReadTime::LatestDisplay,
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .await?;
        let quote = snapshots
            .first()
            .and_then(|snapshot| snapshot.quote())
            .ok_or("qualified quote did not reach the display")?;
        assert_eq!(
            quote.observation().provenance().source_at(),
            Some(source_at)
        );
        assert_eq!(
            quote.observation().provenance().payload_digest().bytes(),
            original_digest
        );
        drop(snapshots);
        drop(positive_sink);
        Arc::try_unwrap(current)
            .map_err(|_| "current bridge remains borrowed")?
            .shutdown()
            .await
            .map_err(|error| format!("current bridge shutdown failed: {error:?}"))?;

        let (token, epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&oauth).await?;
        let bridge_calls = Arc::new(AtomicUsize::new(0));
        let sink = SchwabRestQuoteSealFirstSink::new(
            Arc::clone(&durable),
            Arc::new(CountingUnavailableCurrentBridge(Arc::clone(&bridge_calls))),
        );
        let completed =
            executed_quote(token, oauth_receipt.access_issued_at_unix_seconds()).await?;
        oauth.revoke_test_authority();
        assert!(epoch.validate_current(epoch.receipt()).is_err());
        assert!(
            SchwabRestQuoteProducer::publish_test_completed_response(
                &sink,
                completed,
                evidence,
                vec![binding],
                epoch,
                generation,
            )
            .await
            .is_err()
        );
        assert_eq!(bridge_calls.load(Ordering::SeqCst), 0);
        assert_eq!(wire.exchange_count(), 1);
        assert!(matches!(
            durable.latest_source_health()?,
            Some(super::super::schwab_market::SchwabRestQuoteSourceHealthOutcome::PostSealPublicationUnavailable {
                sealed_receipt_digest: Some(_),
                ..
            })
        ));

        let (rotating, rotating_wire, rotating_reference) = scripted_market_authority(
            directory.path().join("rotating-oauth"),
            session_id,
            30,
            300,
            0,
        )
        .await?;
        let original_rotation_receipt = rotating.current_receipt().await?;
        let (rotating_durable, rotating_evidence, rotating_binding, rotating_generation, _) =
            quote_publication_fixture(
                &directory.path().join("rotating-publication"),
                session_id,
                rotating_reference,
                rotating.clone(),
                original_rotation_receipt,
            )?;
        let rotating_admission = test_schwab_composite_market_runtime_admission(
            &rotating_generation,
            rotating.receipt_currentness(),
            original_rotation_receipt,
        )?;
        let original_generation_digest = rotating_generation.generation_digest()?;
        let (rotated_token, rotated_epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&rotating).await?;
        assert_eq!(rotating_wire.exchange_count(), 2);
        assert_eq!(rotated_epoch.receipt().generation().get(), 2);
        assert_eq!(
            rotated_token.generation(),
            rotated_epoch.receipt().generation()
        );
        assert_eq!(original_rotation_receipt.generation().get(), 1);
        assert_eq!(
            original_rotation_receipt.authorization_generation(),
            rotated_epoch.receipt().authorization_generation()
        );
        assert!(
            rotating
                .receipt_currentness()
                .validate_current_receipt(original_rotation_receipt)
                .is_err()
        );
        rotated_epoch.validate_current(rotated_epoch.receipt())?;
        rotating_admission.ensure_live()?;
        rotating_admission.validate_oauth_current(rotated_epoch.receipt())?;
        assert!(
            rotating_admission
                .validate_oauth_current(original_rotation_receipt)
                .is_err()
        );
        assert_eq!(
            rotating_admission.generation_digest(),
            Some(original_generation_digest)
        );
        assert!(rotating_generation.runtime_verification.is_none());

        let rotating_bridge_calls = Arc::new(AtomicUsize::new(0));
        let rotating_sink = SchwabRestQuoteSealFirstSink::new(
            Arc::clone(&rotating_durable),
            Arc::new(CountingUnavailableCurrentBridge(Arc::clone(
                &rotating_bridge_calls,
            ))),
        );
        let rotated_response = executed_quote(
            rotated_token,
            rotated_epoch.receipt().access_issued_at_unix_seconds(),
        )
        .await?;
        let _ = SchwabRestQuoteProducer::publish_test_completed_response(
            &rotating_sink,
            rotated_response,
            rotating_evidence,
            vec![rotating_binding],
            rotated_epoch,
            generation,
        )
        .await;
        assert_eq!(
            rotating_bridge_calls.load(Ordering::SeqCst),
            1,
            "same-grant refresh was rejected before current quote qualification"
        );

        // Restored access expiry must reach the sole writer's refresh path before requests. Pure receipt inspection remains read-only and rejects the expired epoch.
        let (expired, expired_wire, _) = scripted_market_authority(
            directory.path().join("expired-bootstrap"),
            session_id,
            30,
            60,
            60,
        )
        .await?;
        let prior = expired.issued_receipt();
        assert!(expired.current_receipt().await.is_err());
        assert_eq!(expired_wire.exchange_count(), 1);
        expired_wire.refresh_release.try_acquire()?.forget();
        let mut bootstrap = Box::pin(expired.prepare_test_bootstrap());
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                () = expired_wire.refresh_started.notified() => Ok(()),
                _result = &mut bootstrap => Err("bootstrap completed before held refresh"),
            }
        })
        .await??;
        // The response waiter may time out while the retained operation still owns rotation.
        assert!(
            tokio::time::timeout(Duration::ZERO, &mut bootstrap)
                .await
                .is_err()
        );
        assert_eq!(expired_wire.exchange_count(), 2);
        assert!(
            expired
                .receipt_currentness()
                .validate_current_receipt(prior)
                .is_err()
        );
        expired_wire.refresh_release.add_permits(1);
        let refreshed = tokio::time::timeout(Duration::from_secs(5), bootstrap).await??;
        assert_eq!(refreshed.generation().get(), 2);
        assert_eq!(expired.current_receipt().await?, refreshed);
        assert_eq!(expired.prepare_test_bootstrap().await?, refreshed);
        assert_eq!(
            expired_wire.exchange_count(),
            2,
            "current bootstrap exchanged again"
        );
        assert!(
            expired
                .receipt_currentness()
                .validate_current_receipt(prior)
                .is_err()
        );
        let (_, current_epoch) =
            SchwabMarketDataAccountActivation::acquire_test_publication_attempt(&expired).await?;
        assert_eq!(current_epoch.receipt(), refreshed);
        current_epoch.validate_current(refreshed)?;
        assert_eq!(expired_wire.exchange_count(), 2);
        Ok(())
    }

    async fn quote_current_bridge(
        research: &ResearchService,
        metadata: &SourceMetadata,
        binding: &SchwabRestQuoteInstrumentBinding,
        session_id: Uuid,
    ) -> Result<
        (
            crate::live_source::SchwabRestQuoteCurrentSessionBridge,
            crate::live_source::display_market::DisplayMarketDirectory,
        ),
        Box<dyn std::error::Error>,
    > {
        use crate::live_source::display_market::{
            DisplayMarketActorLimits, DisplayMarketDirectory, DisplayMarketKey,
            DisplayMarketReadAdmission,
        };
        use market_squawk_platform::{
            CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureWriterPolicy,
            MemoryCaptureSink, initialize_capture_process_infrastructure, raw_capture_channel,
            spawn_capture_writer,
        };
        use market_squawk_sources::{
            AuthoritativeSourceRegistry, ProviderNativeIdentityRequest, SessionId,
        };

        let at = Timestamp::from_unix_nanos(i64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        )?);
        let mut registry =
            AuthoritativeSourceRegistry::try_new_ephemeral_with_authorization_subject_resolver_for_diagnostics(
                Arc::new(QuoteAuthorizationSubject),
            )?
            .with_provider_identity_authority(Arc::new(research.market_data_instruments()))?;
        let registered = registry.register(metadata.clone(), at)?;
        let reference = binding.binding();
        let venue = VenueId::try_from("schwab")?;
        registry.record_provider_identities(
            &registered,
            &[ProviderNativeIdentityRequest {
                namespace: reference.provider_identity().source_id().clone(),
                provider_instrument_id: reference
                    .provider_identity()
                    .provider_instrument_id()
                    .clone(),
                instrument: binding.instrument_id(),
                venue: venue.clone(),
                venue_symbol: market_squawk_domain::VenueSymbol::try_from(
                    binding.provider_symbol(),
                )?,
                knowledge_at: reference.canonical_record().published_at(),
                effective_at: reference.canonical_record().published_at(),
            }],
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )?;
        let session_id = SourceIdentifier::try_from(session_id.to_string())?;
        let session =
            registry.begin_next_session(&registered, SessionId::new(session_id.clone()), at)?;
        let generation = session.generation();
        let capabilities = registry.take_capture_generation_capabilities(&session)?;
        let health = registry.take_current_health_reporter(&session)?;
        let frames = registry.take_raw_frame_factory(&session)?;
        let process = initialize_capture_process_infrastructure(
            CaptureProcessInfrastructureLimits::new(nonzero(1024 * 1024)),
        )?;
        let (capture, control, writer) = raw_capture_channel(
            &process,
            CaptureChannelLimits::new(nonzero(8), nonzero(16 * 1024 * 1024)),
            capabilities,
        )?;
        let writer = spawn_capture_writer(
            writer,
            MemoryCaptureSink::try_new(nonzero(64), nonzero(16 * 1024 * 1024))?,
            CaptureWriterPolicy::default(),
        )?;
        let display = DisplayMarketDirectory::try_new(
            NonZeroUsize::MIN,
            CancellationToken::new(),
            market_squawk_runtime::ApplicationChanges::default(),
        )?;
        let mut current = crate::live_source::SchwabRestQuoteCurrentSessionInput::new(
            registry,
            session,
            frames,
            capture,
            control,
            writer,
            health,
            display.clone(),
            Vec::new(),
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(5),
        );
        current
            .activate_capture_initial()
            .map_err(|error| format!("current capture activation failed: {error:?}"))?;
        let bytes = NonZeroU32::new(1024 * 1024).ok_or("invalid display bound")?;
        let _monitor = current
            .register_display_route(
                DisplayMarketKey::try_new(
                    metadata.source_id(),
                    &venue,
                    binding.instrument_id(),
                    generation,
                )?,
                DisplayMarketActorLimits::try_new(
                    nonzero(8),
                    bytes,
                    bytes,
                    nonzero(8),
                    bytes,
                    bytes,
                )?,
                DisplayMarketReadAdmission::open(),
                &CancellationToken::new(),
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map_err(|error| format!("current display registration failed: {error:?}"))?;
        let instrument = crate::live_source::SchwabRestQuoteCurrentInstrument::try_new(
            ProviderIdentifier::try_new(binding.provider_symbol().to_owned())?,
            SourceIdentifier::try_from(binding.provider_symbol())?,
            binding.instrument_id(),
            reference.quote_reference(at)?,
        )
        .map_err(|error| format!("current instrument binding failed: {error:?}"))?;
        let bridge = crate::live_source::SchwabRestQuoteCurrentSessionBridge::try_new(
            current,
            metadata,
            &session_id,
            generation,
            &venue,
            &[instrument],
        )
        .await
        .map_err(|error| format!("current bridge construction failed: {error:?}"))?;
        Ok((bridge, display))
    }

    #[derive(Debug)]
    struct QuoteAuthorizationSubject;

    impl market_squawk_sources::AuthorizationSubjectResolver for QuoteAuthorizationSubject {
        fn resolve_subject_record(
            &self,
            mode: AuthorizationMode,
            evidence: EvidenceDigest,
        ) -> Result<SourceIdentifier, market_squawk_sources::AuthorizationSubjectResolutionError>
        {
            if mode == AuthorizationMode::UserAuthorized && evidence == digest(2) {
                SourceIdentifier::try_from("schwab-test-account").map_err(|_| {
                    market_squawk_sources::AuthorizationSubjectResolutionError::EvidenceUnresolved
                })
            } else {
                Err(market_squawk_sources::AuthorizationSubjectResolutionError::EvidenceUnresolved)
            }
        }
    }

    #[derive(Debug)]
    struct CountingUnavailableCurrentBridge(Arc<AtomicUsize>);

    impl SchwabRestQuoteCurrentBridge for CountingUnavailableCurrentBridge {
        fn qualify_current(
            &self,
            _request: SchwabRestQuoteCurrentRequest<'_>,
        ) -> Result<SchwabQualifiedCurrent, SchwabRestQuoteCurrentUnavailable> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth)
        }

        fn publish_qualified(
            &self,
            _qualified: SchwabQualifiedCurrent,
            _deadline: Instant,
        ) -> SchwabRestQuoteCurrentPublication {
            self.0.fetch_add(1, Ordering::SeqCst);
            SchwabRestQuoteCurrentPublication::Unavailable(
                SchwabRestQuoteCurrentUnavailable::AuthorityOrHealth,
            )
        }
    }

    #[derive(Debug)]
    struct ScriptedSchwabOAuthWire {
        initial_lifetime_seconds: u64,
        exchanges: AtomicUsize,
        refresh_started: tokio::sync::Notify,
        refresh_release: tokio::sync::Semaphore,
    }

    impl ScriptedSchwabOAuthWire {
        fn exchange_count(&self) -> usize {
            self.exchanges.load(Ordering::SeqCst)
        }
    }

    impl SchwabOAuthWire for ScriptedSchwabOAuthWire {
        fn exchange(
            &self,
            _request: SchwabOAuthWireRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                let attempt = self.exchanges.fetch_add(1, Ordering::SeqCst);
                if attempt == 1 {
                    self.refresh_started.notify_one();
                    self.refresh_release
                        .acquire()
                        .await
                        .map_err(|_| SchwabOAuthWireError::Protocol)?
                        .forget();
                }
                let body = match attempt {
                    0 => format!(
                        r#"{{"access_token":"initial-access","refresh_token":"initial-refresh","token_type":"Bearer","expires_in":{},"scope":"market-data"}}"#,
                        self.initial_lifetime_seconds
                    )
                    .into_bytes(),
                    1 => br#"{"access_token":"rotated-access","refresh_token":"rotated-refresh","token_type":"Bearer","expires_in":1800,"scope":"market-data"}"#
                        .to_vec(),
                    _ => return Err(SchwabOAuthWireError::Protocol),
                };
                SchwabOAuthWireResponse::try_new(200, body, nonzero(4 * 1024))
            })
        }
    }

    async fn scripted_market_authority(
        root: impl AsRef<Path>,
        session_id: Uuid,
        initial_lifetime_seconds: u64,
        refresh_early_seconds: u64,
        issued_seconds_ago: u64,
    ) -> Result<
        (
            SchwabOAuthMarketAuthority,
            Arc<ScriptedSchwabOAuthWire>,
            SecretRef,
        ),
        Box<dyn std::error::Error>,
    > {
        let root = root.as_ref();
        let secrets = Arc::new(EncryptedFileSecretStore::try_open(
            root.join("secrets"),
            SecretValue::new("schwab publication attempt test unlock".to_owned())?,
        )?);
        let control = SecretOperationControl::try_new(
            "schwab-publication-attempt-test",
            Instant::now() + Duration::from_secs(30),
            0,
            SecretInteractionPolicy::Forbid,
            SecretCancellation::new(),
        )?;
        let application_credential = secrets.create(
            &SecretKey::try_new("market-squawk.schwab", "test-application")?,
            SecretGeneration::new(1)?,
            SecretValue::new(
                r#"{"version":1,"app_key":"test-app-key","app_secret":"test-app-secret"}"#
                    .to_owned(),
            )?,
            &control,
        )?;
        let wire = Arc::new(ScriptedSchwabOAuthWire {
            initial_lifetime_seconds,
            exchanges: AtomicUsize::new(0),
            refresh_started: tokio::sync::Notify::new(),
            refresh_release: tokio::sync::Semaphore::new(1),
        });
        let authority = Arc::new(
            ProtectedSchwabOAuthAuthority::try_open(
                root.join("authority"),
                SchwabOAuthAuthorityConfiguration::try_new(
                    secrets.clone(),
                    wire.clone(),
                    application_credential.clone(),
                    SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)?,
                    parse_bounds(),
                    AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
                    refresh_early_seconds,
                )?,
            )
            .await?,
        );
        let callback = match OAuthCallback::parse(
            "https://127.0.0.1:8182/?code=one-time&state=publication-attempt",
            "publication-attempt",
            RequestAdmission::new(nonzero(4 * 1024), NonZeroUsize::MIN),
        )? {
            CallbackOutcome::Authorized(callback) => callback,
            CallbackOutcome::Denied { .. } => return Err("test callback was denied".into()),
        };
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .checked_sub(issued_seconds_ago)
            .ok_or("fixture issuance underflow")?;
        let receipt = authority
            .complete_authorization(&callback, issued_at, SchwabOAuthInteraction::Background)
            .await?;
        let authority = if issued_seconds_ago > 0 {
            drop(authority);
            Arc::new(
                ProtectedSchwabOAuthAuthority::try_open(
                    root.join("authority"),
                    SchwabOAuthAuthorityConfiguration::try_new(
                        secrets,
                        wire.clone(),
                        application_credential.clone(),
                        SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)?,
                        parse_bounds(),
                        AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
                        refresh_early_seconds,
                    )?,
                )
                .await?,
            )
        } else {
            authority
        };
        Ok((
            SchwabOAuthMarketAuthority::from_test_authority(
                session_id,
                receipt,
                Arc::clone(&authority),
            ),
            wire,
            application_credential,
        ))
    }

    #[derive(Debug)]
    struct QuoteHttpWire(Mutex<Option<SchwabHttpWireResponse>>);

    impl SchwabHttpWire for QuoteHttpWire {
        fn get<'a>(
            &'a self,
            _request: SchwabHttpWireRequest<'a>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            SchwabHttpWireResponse,
                            market_squawk_adapter_schwab::SchwabTransportError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.0
                    .lock()
                    .map_err(|_poisoned| {
                        market_squawk_adapter_schwab::SchwabTransportError::Protocol
                    })?
                    .take()
                    .ok_or(market_squawk_adapter_schwab::SchwabTransportError::Protocol)
            })
        }
    }

    async fn executed_quote(
        token: TransientAccessToken,
        source_seconds: u64,
    ) -> Result<market_squawk_adapter_schwab::ExecutedRestResponse, Box<dyn std::error::Error>>
    {
        let request = QuoteRequest::try_new(
            vec![ProviderIdentifier::try_new("AAPL".to_owned())?],
            vec![QuoteField::Quote],
            None,
            RequestAdmission::new(nonzero(4 * 1024), NonZeroUsize::MIN),
        )?;
        let body = Bytes::from(format!(
            r#"{{"AAPL":{{"assetMainType":"EQUITY","realtime":true,"quote":{{"bidPrice":100.12,"askPrice":100.13,"bidSize":2,"askSize":3,"quoteTime":{}}}}}}}"#,
            source_seconds.saturating_mul(1_000)
        ));
        let bounds = RestTransportBounds::try_new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(2),
            nonzero(64 * 1024),
            nonzero(8),
            nonzero(2 * 1024),
        )?;
        let response = SchwabHttpWireResponse::try_new(
            200,
            request.request().url().to_owned(),
            Some(u64::try_from(body.len())?),
            vec![ResponseHeaderEvidence::try_new(
                "content-type".to_owned(),
                b"application/json".to_vec(),
            )?],
            body,
            bounds,
        )?;
        let executor = SchwabRestExecutor::try_new(
            Arc::new(QuoteHttpWire(Mutex::new(Some(response)))),
            bounds,
            parse_bounds(),
            AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
            SchwabTransportTelemetry::default(),
        )?;
        match executor
            .execute(request.request(), &token, CancellationToken::new())
            .await?
        {
            RestExecutionOutcome::Accepted(response) => Ok(response),
            _ => Err("mock quote response was not accepted".into()),
        }
    }

    fn quote_publication_fixture(
        root: &Path,
        session_id: Uuid,
        secret_reference: SecretRef,
        oauth: SchwabOAuthMarketAuthority,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<
        (
            Arc<super::super::schwab_market::SchwabRestQuoteGenerationAuthority>,
            SchwabRestQuoteSourceEvidence,
            SchwabRestQuoteInstrumentBinding,
            ResearchProviderRuntimeGeneration,
            Arc<ResearchService>,
        ),
        Box<dyn std::error::Error>,
    > {
        let instrument_id = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
        let source_id = SourceId::try_from("schwab-trader-api")?;
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
        let product = ProviderProduct::new(SourceIdentifier::try_from("schwab-rest")?);
        let channel = ProviderChannel::new(SourceIdentifier::try_from("schwab-rest-quotes")?);
        let metadata = quote_metadata(
            source_id.clone(),
            instrument_id,
            effective,
            product.clone(),
            channel.clone(),
        )?;
        let capability_digest = digest(31);
        let parent_rights = digest(32);
        let rights = ResearchRightsAuthority::try_new_scoped(
            source_id,
            RightsBasis::reviewed_terms("https://developer.schwab.com/terms", digest(33))?,
            parent_rights,
            digest(34),
            None,
            vec![SourceIdentifier::try_from("schwab-rest-quotes-aapl")?],
            vec![SourceOperation::Persist],
        )?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(SCHWAB_MARKET_DATA_SURFACE_ID)?,
            session_id,
            ProviderCapabilityRevision::new(1)?,
            capability_digest,
            Some(secret_reference.generation()),
            Some(secret_reference),
            timestamp_seconds(oauth_receipt.access_issued_at_unix_seconds())?,
            metadata.clone(),
            rights.clone(),
        )?;
        let paths = LocalPaths::prepare(root.join("research"))?;
        let research = Arc::new(ResearchService::open_or_initialize(
            &paths,
            CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                market_squawk_data::CatalogLimit::new(64)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?,
            8,
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?,
        )?);
        let binding = quote_binding(
            &research,
            metadata.source_id().clone(),
            instrument_id,
            effective,
        )?;
        let durable = super::super::schwab_market::SchwabRestQuoteGenerationAuthority::bind_test_rest_quote_sink(
            Arc::clone(&research),
            generation.clone(),
            rights,
            oauth,
            oauth_receipt,
            Duration::from_secs(5),
        )?;
        let evidence =
            SchwabRestQuoteSourceEvidence::try_new(metadata, VenueId::try_from("schwab")?)?;
        Ok((durable, evidence, binding, generation, research))
    }

    fn quote_metadata(
        source_id: SourceId,
        instrument_id: InstrumentId,
        effective: EffectiveInterval,
        product: ProviderProduct,
        channel: ProviderChannel,
    ) -> Result<SourceMetadata, Box<dyn std::error::Error>> {
        let provider = SourceIdentifier::try_from("schwab-trader-api")?;
        let authorization = AuthorizationGrant::new(
            AuthorizationMode::UserAuthorized,
            AuthorizationBasis::new(SourceIdentifier::try_from("schwab-test-account")?),
            ExactPayloadEvidence::from_content_digest(digest(2)),
            effective,
        );
        let live = LiveCoverageDeclaration::try_new(
            product,
            channel,
            vec![LiveCoverageRule::try_new(
                market_squawk_domain::LiveEventClass::Quote,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: rule("schwab-rest-quote-snapshot")?,
                },
            )?],
        )?;
        let budget = ProviderBudgetPolicy::try_new(
            BudgetScope::for_authorization(provider.clone(), &authorization)?,
            NonZeroU32::new(20).ok_or("invalid request budget")?,
            NonZeroU64::new(15 * 60 * 1_000_000_000).ok_or("invalid budget window")?,
            NonZeroU16::new(1).ok_or("invalid concurrency budget")?,
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).ok_or("invalid backoff")?,
                NonZeroU64::new(60_000_000_000).ok_or("invalid backoff cap")?,
                1_000,
            )?,
        )?;
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            source_id,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("schwab-rest-quote-v1")?),
                ExactPayloadEvidence::from_content_digest(digest(3)),
            ),
            SourceClass::Broker,
            provider,
            authorization,
            SourceCoverage::try_instrument(
                ExactPayloadEvidence::from_content_digest(digest(4)),
                effective,
                vec![AssetClass::Index],
                CoverageTopology::single_venue(VenueId::try_from("schwab")?),
                InstrumentCoverage::enumerated(vec![instrument_id])?,
                Some(live),
                CoverageDelay::Unknown,
                DeliveryEvidence::AuthorizedBroker,
            )?,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
                "https://api.schwabapi.com/marketdata/v1/quotes",
            ])?),
            FreshnessPolicy::try_new(
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                60_000_000_000,
                1_000_000_000,
            )?,
            Some(budget),
            SourceCapabilities::new(
                true,
                true,
                SequenceCapability::Unsupported,
                ChecksumCapability::Unsupported,
                HistoricalCapability::None,
                true,
            ),
            SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
                rule("schwab-rest-decoder")?,
                SemanticInterpretationProfile::new(
                    rule("schwab-rest-aggressor")?,
                    rule("schwab-rest-auction")?,
                    rule("schwab-rest-status")?,
                    rule("schwab-rest-corporate-action")?,
                ),
                rule("schwab-rest-timestamp")?,
                SequenceValidationProfile::Unsupported {
                    rule: rule("schwab-rest-no-sequence")?,
                },
                ChecksumValidationProfile::Unsupported {
                    rule: rule("schwab-rest-no-checksum")?,
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ))),
        ))?)
    }

    fn quote_binding(
        research: &ResearchService,
        source_id: SourceId,
        instrument_id: InstrumentId,
        effective: EffectiveInterval,
    ) -> Result<SchwabRestQuoteInstrumentBinding, Box<dyn std::error::Error>> {
        let provider_identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id,
            source_id: SourceId::try_from("schwab-trader-api-instruments")?,
            provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
            evidence: ProviderIdentityEvidence::from_content_digest(digest(10)),
            source_timestamp: Some(Timestamp::from_unix_nanos(0)),
            observed_at: Timestamp::from_unix_nanos(1),
            metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                "schwab-provider-identity-v1",
            )?),
            validity: effective,
            supersedes: None,
        });
        let identifier = ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
            identifier: ExternalIdentifier::Ticker(Ticker::try_from("AAPL")?),
            assignment_verification: AssignmentVerification::VerifiedAssigned,
            source_id: SourceId::try_from("reference-master")?,
            source_evidence: ExactPayloadEvidence::from_content_digest(digest(11)),
            source_timestamp: Some(Timestamp::from_unix_nanos(0)),
            observed_at: Timestamp::from_unix_nanos(1),
            validity: effective,
            rights_policy: IdentifierRightsPolicyReference::new(
                SourceIdentifier::try_from("reference-personal-use-v1")?,
                IdentifierEntitlement::LicensedInternalUse,
                SourceIdentifier::try_from("https://example.test/reference")?,
            ),
        });
        let definition = market_squawk_domain::MarketDataInstrumentDefinition::try_new(
            market_squawk_domain::MarketDataInstrumentDefinitionInput {
                instrument_id,
                reference_evidence: RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(SourceIdentifier::try_from("test-schwab-reference")?),
                    ExactPayloadEvidence::from_content_digest(digest(12)),
                ),
                effective_interval: effective,
                asset_class: AssetClass::Index,
                display_name: None,
                quote_currency: Currency::try_from("USD")?,
                quote_currency_evidence: ExactPayloadEvidence::from_content_digest(digest(13)),
                venue_mappings: Vec::new(),
                provider_identities: vec![provider_identity.clone()],
                identifiers: vec![identifier.clone()],
            },
        )?;
        research
            .market_data_instrument_synchronization()
            .synchronize(
                market_squawk_data::MarketDataInstrumentSynchronization::try_new(
                    vec![definition],
                    1,
                )?,
                std::time::Instant::now() + Duration::from_secs(5),
                &CancellationToken::new(),
            )?;
        let record = research
            .market_data_instruments()
            .latest(
                instrument_id,
                std::time::Instant::now() + Duration::from_secs(5),
                &CancellationToken::new(),
            )?
            .ok_or("missing canonical test reference")?;
        let at = record.published_at();
        SchwabRestQuoteInstrumentBinding::try_new(
            crate::provider_activation::SchwabQuoteReferenceBinding::try_new(
                record,
                provider_identity,
                MarketInstrumentReferenceBinding::AssignedExternalIdentifier(identifier),
                MarketSubscriptionPriority::CurrentlyViewed,
                at,
            )?,
            &source_id,
        )
        .map_err(Into::into)
    }

    fn parse_bounds() -> market_squawk_adapter_schwab::ParseBounds {
        market_squawk_adapter_schwab::ParseBounds::new(
            nonzero(64 * 1024),
            nonzero(64),
            nonzero(2_048),
            nonzero(16),
            32,
            8 * 1024,
        )
    }

    fn timestamp_seconds(seconds: u64) -> Result<Timestamp, Box<dyn std::error::Error>> {
        Ok(Timestamp::from_unix_nanos(i64::try_from(
            seconds.checked_mul(1_000_000_000).ok_or("clock overflow")?,
        )?))
    }

    fn rule(value: &str) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
        Ok(IntegrityRule::new(
            SourceIdentifier::try_from(value)?,
            RuleVersion::new(1)?,
        ))
    }

    fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }

    fn nonzero(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
    }
}
